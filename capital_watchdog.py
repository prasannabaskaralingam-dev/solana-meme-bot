"""
Capital Watchdog — Surveillance de la santé du CAPITAL (pas du bot).

Détecte et alerte quand :
  ⚠️ Une position n'a pas été vérifiée depuis > 10s (gap de monitoring)
  🚨 Un token chute sans que le bot réagisse (chute libre non détectée)
  💀 Capital à risque sans surveillance (WS down + polling en retard)

Le watchdog tourne toutes les 5s et envoie des alertes Telegram IMMÉDIATEMENT.
Si le gap dépasse 30s → VENTE D'URGENCE automatique (force sell).
"""

import time
import logging
from dataclasses import dataclass, field
from typing import Dict, Optional, Callable, List

logger = logging.getLogger(__name__)


# ============================================================
# CONFIGURATION
# ============================================================

@dataclass
class WatchdogConfig:
    """Seuils d'alerte du watchdog"""

    # Seuils de gap de monitoring (secondes)
    warn_gap_seconds: float = 10.0       # ⚠️ Alerte si position non vérifiée > 10s
    critical_gap_seconds: float = 20.0   # 🚨 Alerte critique > 20s
    emergency_gap_seconds: float = 30.0  # 💀 Vente d'urgence > 30s

    # Seuils de chute libre (% par seconde)
    freefall_pct_per_sec: float = 1.0    # 🚨 Si le token chute > 1%/sec = chute libre

    # Capital à risque
    max_capital_at_risk_pct: float = 50.0  # 💀 Si > 50% du capital est non-surveillé

    # Cooldown entre alertes (éviter le spam)
    alert_cooldown_seconds: float = 30.0   # Pas plus d'1 alerte par position par 30s

    # Intervalle du watchdog
    check_interval_seconds: float = 5.0    # Le watchdog tourne toutes les 5s


# ============================================================
# POSITION HEALTH TRACKER
# ============================================================

@dataclass
class PositionHealth:
    """État de santé d'une position surveillée"""
    token_address: str
    token_symbol: str
    strategy: str
    amount_sol: float
    entry_price: float

    # Timestamps de dernière vérification
    last_check_time: float = 0.0          # Dernière fois qu'on a vérifié le prix
    last_price: float = 0.0               # Dernier prix connu
    last_price_time: float = 0.0          # Quand ce prix a été obtenu

    # Historique de prix pour détecter la chute libre
    price_history: List = field(default_factory=list)  # [(timestamp, price), ...]

    # Alertes
    last_alert_time: float = 0.0          # Dernière alerte envoyée
    alert_level: str = "ok"               # ok, warn, critical, emergency
    consecutive_gaps: int = 0             # Nombre de gaps consécutifs


# ============================================================
# CAPITAL WATCHDOG
# ============================================================

class CapitalWatchdog:
    """
    Watchdog de surveillance du capital.
    Vérifie que CHAQUE position est activement surveillée.
    Alerte immédiatement si un gap de monitoring est détecté.
    """

    def __init__(self, config: Optional[WatchdogConfig] = None):
        self.config = config or WatchdogConfig()
        self.positions: Dict[str, PositionHealth] = {}
        self._stats = {
            "warnings_sent": 0,
            "critical_alerts": 0,
            "emergency_sells": 0,
            "total_checks": 0,
            "max_gap_seen": 0.0,
        }
        self._alert_callback: Optional[Callable] = None
        self._emergency_sell_callback: Optional[Callable] = None
        logger.info(f"[Watchdog] Initialisé: warn={self.config.warn_gap_seconds}s, "
                    f"critical={self.config.critical_gap_seconds}s, "
                    f"emergency={self.config.emergency_gap_seconds}s")

    # ----------------------------------------------------------
    # CALLBACKS
    # ----------------------------------------------------------

    def set_alert_callback(self, callback: Callable):
        """Définir le callback pour envoyer des alertes Telegram"""
        self._alert_callback = callback

    def set_emergency_sell_callback(self, callback: Callable):
        """Définir le callback pour vente d'urgence"""
        self._emergency_sell_callback = callback

    # ----------------------------------------------------------
    # GESTION DES POSITIONS
    # ----------------------------------------------------------

    def register_position(self, token_address: str, token_symbol: str,
                          strategy: str, amount_sol: float, entry_price: float):
        """Enregistrer une position à surveiller"""
        now = time.time()
        self.positions[token_address] = PositionHealth(
            token_address=token_address,
            token_symbol=token_symbol,
            strategy=strategy,
            amount_sol=amount_sol,
            entry_price=entry_price,
            last_check_time=now,
            last_price=entry_price,
            last_price_time=now,
        )
        logger.info(f"[Watchdog] Position enregistrée: {token_symbol} ({strategy})")

    def unregister_position(self, token_address: str):
        """Retirer une position (vendue)"""
        if token_address in self.positions:
            sym = self.positions[token_address].token_symbol
            del self.positions[token_address]
            logger.info(f"[Watchdog] Position retirée: {sym}")

    def heartbeat(self, token_address: str, current_price: float):
        """
        Signal de vie : appelé à chaque fois qu'un prix est vérifié pour une position.
        C'est le "ping" qui dit "cette position est activement surveillée".
        """
        if token_address not in self.positions:
            return

        pos = self.positions[token_address]
        now = time.time()

        # Mettre à jour le timestamp de dernière vérification
        pos.last_check_time = now
        pos.last_price = current_price
        pos.last_price_time = now
        pos.consecutive_gaps = 0

        # Historique de prix (garder les 30 dernières secondes)
        pos.price_history.append((now, current_price))
        cutoff = now - 30
        pos.price_history = [(t, p) for t, p in pos.price_history if t > cutoff]

        # ─── FIX MÉMOIRE 1 — Limite absolue de 60 entrées ────────────
        if len(pos.price_history) > 60:
            pos.price_history = pos.price_history[-60:]
        # ──────────────────────────────────────────────────────────

        # ─── FIX MÉMOIRE 2 — Détection token mort ────────────────
        # Si 10 prix consécutifs = 0 → token mort depuis ~5 min
        zero_prices = [p for t, p in pos.price_history if p <= 0]
        if len(zero_prices) >= 10:
            if not getattr(pos, 'is_dead', False):
                pos.is_dead = True
                logger.warning(
                    f"[Memory] Token mort détecté : "
                    f"{pos.token_symbol} — "
                    f"prix=0 depuis {len(zero_prices) * 30}s"
                )
        # ──────────────────────────────────────────────────────────

        # Si on était en alerte, on repasse en OK
        if pos.alert_level != "ok":
            logger.info(f"[Watchdog] ✅ {pos.token_symbol} de retour sous surveillance")
            pos.alert_level = "ok"

    # ----------------------------------------------------------
    # CHECK PRINCIPAL (appelé toutes les 5s)
    # ----------------------------------------------------------

    def check(self) -> List[dict]:
        """
        Vérifier la santé de toutes les positions.
        Retourne une liste d'alertes à envoyer.
        """
        self._stats["total_checks"] += 1
        now = time.time()
        alerts = []

        for token_addr, pos in list(self.positions.items()):
            gap = now - pos.last_check_time

            # Mettre à jour le max gap vu
            if gap > self._stats["max_gap_seen"]:
                self._stats["max_gap_seen"] = gap

            # Vérifier le cooldown d'alerte
            time_since_last_alert = now - pos.last_alert_time
            can_alert = time_since_last_alert >= self.config.alert_cooldown_seconds

            # === EMERGENCY (> 30s sans vérification) ===
            if gap >= self.config.emergency_gap_seconds:
                pos.alert_level = "emergency"
                pos.consecutive_gaps += 1
                if can_alert:
                    alert = {
                        "level": "emergency",
                        "token_address": token_addr,
                        "token_symbol": pos.token_symbol,
                        "strategy": pos.strategy,
                        "gap_seconds": gap,
                        "amount_sol": pos.amount_sol,
                        "message": (
                            f"💀 URGENCE — {pos.token_symbol} NON SURVEILLÉ "
                            f"depuis {gap:.0f}s!\n"
                            f"Capital à risque: {pos.amount_sol:.4f} SOL\n"
                            f"→ VENTE D'URGENCE DÉCLENCHÉE"
                        ),
                        "action": "emergency_sell",
                    }
                    alerts.append(alert)
                    pos.last_alert_time = now
                    self._stats["emergency_sells"] += 1
                    logger.critical(f"[Watchdog] 💀 EMERGENCY SELL: {pos.token_symbol} "
                                    f"(gap={gap:.0f}s)")

            # === CRITICAL (> 20s sans vérification) ===
            elif gap >= self.config.critical_gap_seconds:
                pos.alert_level = "critical"
                pos.consecutive_gaps += 1
                if can_alert:
                    alert = {
                        "level": "critical",
                        "token_address": token_addr,
                        "token_symbol": pos.token_symbol,
                        "strategy": pos.strategy,
                        "gap_seconds": gap,
                        "amount_sol": pos.amount_sol,
                        "message": (
                            f"🚨 CRITIQUE — {pos.token_symbol} non vérifié "
                            f"depuis {gap:.0f}s!\n"
                            f"Capital exposé: {pos.amount_sol:.4f} SOL\n"
                            f"Prochain seuil: vente d'urgence dans "
                            f"{self.config.emergency_gap_seconds - gap:.0f}s"
                        ),
                        "action": "alert_only",
                    }
                    alerts.append(alert)
                    pos.last_alert_time = now
                    self._stats["critical_alerts"] += 1
                    logger.warning(f"[Watchdog] 🚨 CRITICAL: {pos.token_symbol} "
                                   f"(gap={gap:.0f}s)")

            # === WARNING (> 10s sans vérification) ===
            elif gap >= self.config.warn_gap_seconds:
                pos.alert_level = "warn"
                pos.consecutive_gaps += 1
                if can_alert:
                    alert = {
                        "level": "warn",
                        "token_address": token_addr,
                        "token_symbol": pos.token_symbol,
                        "strategy": pos.strategy,
                        "gap_seconds": gap,
                        "amount_sol": pos.amount_sol,
                        "message": (
                            f"⚠️ {pos.token_symbol} non vérifié "
                            f"depuis {gap:.0f}s\n"
                            f"Monitoring potentiellement en panne"
                        ),
                        "action": "alert_only",
                    }
                    alerts.append(alert)
                    pos.last_alert_time = now
                    self._stats["warnings_sent"] += 1

            # === CHUTE LIBRE (prix en baisse rapide) ===
            if len(pos.price_history) >= 2:
                freefall = self._detect_freefall(pos)
                if freefall and can_alert:
                    alert = {
                        "level": "critical",
                        "token_address": token_addr,
                        "token_symbol": pos.token_symbol,
                        "strategy": pos.strategy,
                        "gap_seconds": gap,
                        "amount_sol": pos.amount_sol,
                        "message": (
                            f"📉 CHUTE LIBRE — {pos.token_symbol} "
                            f"perd {freefall['drop_pct']:.1f}% en {freefall['duration']:.0f}s!\n"
                            f"Prix: {pos.last_price:.8f} → en train de s'effondrer"
                        ),
                        "action": "alert_only",
                    }
                    alerts.append(alert)
                    pos.last_alert_time = now

        # === CAPITAL GLOBAL À RISQUE ===
        total_capital = sum(p.amount_sol for p in self.positions.values())
        unmonitored_capital = sum(
            p.amount_sol for p in self.positions.values()
            if p.alert_level in ("critical", "emergency")
        )
        if total_capital > 0:
            risk_pct = (unmonitored_capital / total_capital) * 100
            if risk_pct >= self.config.max_capital_at_risk_pct:
                alerts.append({
                    "level": "emergency",
                    "token_address": "GLOBAL",
                    "token_symbol": "PORTFOLIO",
                    "strategy": "all",
                    "gap_seconds": 0,
                    "amount_sol": unmonitored_capital,
                    "message": (
                        f"💀 {risk_pct:.0f}% DU CAPITAL NON SURVEILLÉ!\n"
                        f"{unmonitored_capital:.4f} SOL à risque sur "
                        f"{total_capital:.4f} SOL total"
                    ),
                    "action": "alert_only",
                })

        return alerts

    # ----------------------------------------------------------
    # DÉTECTION CHUTE LIBRE
    # ----------------------------------------------------------

    def _detect_freefall(self, pos: PositionHealth) -> Optional[dict]:
        """Détecter si un token est en chute libre (baisse rapide continue)"""
        if len(pos.price_history) < 3:
            return None

        # Prendre les 10 dernières secondes
        now = time.time()
        recent = [(t, p) for t, p in pos.price_history if t > now - 10]
        if len(recent) < 2:
            return None

        first_time, first_price = recent[0]
        last_time, last_price = recent[-1]

        if first_price <= 0 or last_price <= 0:
            return None

        duration = last_time - first_time
        if duration < 2:  # Besoin d'au moins 2s de données
            return None

        drop_pct = ((last_price - first_price) / first_price) * 100
        drop_per_sec = abs(drop_pct) / duration if duration > 0 else 0

        # Chute libre = baisse > 1%/sec pendant > 3s
        if drop_pct < 0 and drop_per_sec >= self.config.freefall_pct_per_sec and duration >= 3:
            return {
                "drop_pct": drop_pct,
                "drop_per_sec": drop_per_sec,
                "duration": duration,
            }

        return None

    # ----------------------------------------------------------
    # STATS & STATUS
    # ----------------------------------------------------------

    def get_stats(self) -> dict:
        """Retourner les stats du watchdog"""
        now = time.time()
        positions_status = []
        for addr, pos in self.positions.items():
            gap = now - pos.last_check_time
            positions_status.append({
                "symbol": pos.token_symbol,
                "strategy": pos.strategy,
                "gap_seconds": gap,
                "alert_level": pos.alert_level,
                "amount_sol": pos.amount_sol,
            })

        return {
            **self._stats,
            "active_positions": len(self.positions),
            "positions_status": positions_status,
        }

    def get_health_summary(self) -> str:
        """Résumé de santé pour affichage Telegram"""
        now = time.time()
        if not self.positions:
            return "🟢 Aucune position à surveiller"

        lines = []
        all_ok = True
        for addr, pos in self.positions.items():
            gap = now - pos.last_check_time
            if gap < self.config.warn_gap_seconds:
                emoji = "🟢"
            elif gap < self.config.critical_gap_seconds:
                emoji = "⚠️"
                all_ok = False
            elif gap < self.config.emergency_gap_seconds:
                emoji = "🚨"
                all_ok = False
            else:
                emoji = "💀"
                all_ok = False
            lines.append(f"  {emoji} {pos.token_symbol}: vérifié il y a {gap:.0f}s")

        header = "🟢 Capital sous contrôle" if all_ok else "⚠️ Gaps de monitoring détectés"
        return header + "\n" + "\n".join(lines)
