"""
CircuitBreaker — Module centralisé de gestion des sorties.

3 règles de sortie actives :
  RÈGLE 1 — Time Stop : > 15 min sans +20% → sortie
  RÈGLE 2 — SL Universel : -25% depuis l'achat → sortie TOUJOURS
  RÈGLE 3 — Trailing Stop : dès +15% atteint → SL à -10% du max
  BONUS  — Take Profit : +20% → vente immédiate

Usage:
  cb = CircuitBreaker()              # 1 fois au démarrage
  pos = cb.open_position(...)        # à chaque achat
  action = cb.check(addr, price)     # dans ta boucle de prix (3s)
"""

import time
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


# ============================================================
# CONFIGURATION
# ============================================================

@dataclass
class CBConfig:
    """Configuration du CircuitBreaker"""

    # RÈGLE 1 — Time Stop
    time_stop_enabled: bool = True
    time_stop_minutes: float = 15.0       # Max 15 min
    time_stop_min_profit: float = 20.0    # Sortie si pas +20% après 15 min

    # RÈGLE 2 — SL Universel
    stop_loss_pct: float = -25.0          # -25% = sortie TOUJOURS

    # RÈGLE 3 — Trailing Stop
    trailing_enabled: bool = True
    trailing_activation_pct: float = 15.0  # Dès +15% atteint
    trailing_stop_pct: float = 10.0        # SL à -10% du max
    # Paliers pour moonshots
    trailing_tight_pct: float = 8.0        # -8% si ATH > +50%
    trailing_ultra_tight_pct: float = 6.0  # -6% si ATH > +100%

    # RÈGLE 4 — Momentum Stop (DÉSACTIVÉ - redondant avec Trailing)
    momentum_stop_enabled: bool = False
    momentum_stop_drop_pct: float = 15.0   # Chute de 15% depuis ATH
    momentum_stop_min_pump: float = 5.0    # Le token doit avoir pumpé au moins +5%

    # Take Profit (bonus)
    take_profit_pct: float = 20.0          # +20% = vente immédiate


# ============================================================
# POSITION TRACKÉE PAR LE CIRCUIT BREAKER
# ============================================================

@dataclass
class CBPosition:
    """Position suivie par le CircuitBreaker"""
    token_address: str
    token_symbol: str
    entry_price: float
    entry_time: float          # timestamp Unix
    highest_price: float = 0.0  # ATH depuis l'achat
    current_price: float = 0.0
    trailing_activated: bool = False

    @property
    def age_minutes(self) -> float:
        """Durée de la position en minutes"""
        return (time.time() - self.entry_time) / 60.0

    @property
    def pnl_pct(self) -> float:
        """PnL en % depuis l'achat"""
        if self.entry_price <= 0:
            return 0.0
        return ((self.current_price - self.entry_price) / self.entry_price) * 100.0

    @property
    def pnl_at_high(self) -> float:
        """PnL au plus haut atteint"""
        if self.entry_price <= 0:
            return 0.0
        return ((self.highest_price - self.entry_price) / self.entry_price) * 100.0

    @property
    def drop_from_high_pct(self) -> float:
        """Chute en % depuis le plus haut"""
        if self.highest_price <= 0:
            return 0.0
        return ((self.current_price - self.highest_price) / self.highest_price) * 100.0


# ============================================================
# ACTIONS DE SORTIE
# ============================================================

@dataclass
class CBAction:
    """Résultat d'un check du CircuitBreaker"""
    should_sell: bool
    reason: str
    rule: str           # "time_stop", "stop_loss", "trailing", "momentum_stop", "take_profit", "hold"
    pnl_pct: float
    age_minutes: float
    highest_pnl: float  # Le max PnL atteint

    def __bool__(self) -> bool:
        return self.should_sell


# ============================================================
# CIRCUIT BREAKER
# ============================================================

class CircuitBreaker:
    """
    Gestionnaire centralisé de toutes les règles de sortie.
    
    Usage:
        cb = CircuitBreaker()
        pos = cb.open_position("TOKEN_ADDR", "TOKEN", entry_price=0.001)
        
        # Dans la boucle de prix (toutes les 15s) :
        action = cb.check("TOKEN_ADDR", current_price=0.0015)
        if action:
            execute_sell(reason=action.reason)
    """

    def __init__(self, config: Optional[CBConfig] = None):
        self.config = config or CBConfig()
        self.positions: Dict[str, CBPosition] = {}
        self._stats = {
            "time_stop_triggered": 0,
            "stop_loss_triggered": 0,
            "trailing_triggered": 0,
            "momentum_stop_triggered": 0,
            "take_profit_triggered": 0,
            "total_checks": 0,
        }
        logger.info("[CircuitBreaker] Initialisé avec config: "
                    f"TS={self.config.time_stop_minutes}min, "
                    f"SL={self.config.stop_loss_pct}%, "
                    f"Trailing=+{self.config.trailing_activation_pct}%/-{self.config.trailing_stop_pct}%, "
                    f"TP=+{self.config.take_profit_pct}%")

    # ----------------------------------------------------------
    # GESTION DES POSITIONS
    # ----------------------------------------------------------

    def open_position(self, token_address: str, token_symbol: str,
                      entry_price: float, entry_time: Optional[float] = None) -> CBPosition:
        """Enregistrer une nouvelle position à surveiller"""
        pos = CBPosition(
            token_address=token_address,
            token_symbol=token_symbol,
            entry_price=entry_price,
            entry_time=entry_time or time.time(),
            highest_price=entry_price,
            current_price=entry_price,
        )
        self.positions[token_address] = pos
        logger.info(f"[CircuitBreaker] Position ouverte: {token_symbol} @ ${entry_price:.8f}")
        return pos

    def close_position(self, token_address: str) -> Optional[CBPosition]:
        """Retirer une position du suivi"""
        pos = self.positions.pop(token_address, None)
        if pos:
            logger.info(f"[CircuitBreaker] Position fermée: {pos.token_symbol} "
                        f"(PnL: {pos.pnl_pct:+.1f}%, durée: {pos.age_minutes:.0f}min)")
        return pos

    def get_position(self, token_address: str) -> Optional[CBPosition]:
        """Récupérer une position"""
        return self.positions.get(token_address)

    # ----------------------------------------------------------
    # CHECK PRINCIPAL — Appeler dans la boucle de prix
    # ----------------------------------------------------------

    def check(self, token_address: str, current_price: float) -> CBAction:
        """
        Vérifier toutes les règles de sortie pour une position.
        
        Retourne un CBAction:
          - action.should_sell = True → VENDRE
          - action.should_sell = False → HOLD
          - action.reason = message explicatif
          - action.rule = identifiant de la règle déclenchée
        """
        pos = self.positions.get(token_address)
        if not pos:
            return CBAction(
                should_sell=False, reason="Position inconnue",
                rule="unknown", pnl_pct=0, age_minutes=0, highest_pnl=0
            )

        # Mettre à jour le prix et l'ATH
        pos.current_price = current_price
        if current_price > pos.highest_price:
            pos.highest_price = current_price

        self._stats["total_checks"] += 1

        # ============================================================
        # VÉRIFICATION DES RÈGLES (ordre de priorité)
        # ============================================================

        # 🎯 TAKE PROFIT — +20% = vente immédiate
        if pos.pnl_pct >= self.config.take_profit_pct:
            self._stats["take_profit_triggered"] += 1
            return CBAction(
                should_sell=True,
                reason=f"🎯 Take Profit ({pos.pnl_pct:+.1f}% ≥ +{self.config.take_profit_pct}%)",
                rule="take_profit",
                pnl_pct=pos.pnl_pct,
                age_minutes=pos.age_minutes,
                highest_pnl=pos.pnl_at_high,
            )

        # 🛑 RÈGLE 2 — SL Universel (-25% → sortie TOUJOURS)
        if pos.pnl_pct <= self.config.stop_loss_pct:
            self._stats["stop_loss_triggered"] += 1
            return CBAction(
                should_sell=True,
                reason=f"🛑 SL Universel ({pos.pnl_pct:.1f}% ≤ {self.config.stop_loss_pct}%)",
                rule="stop_loss",
                pnl_pct=pos.pnl_pct,
                age_minutes=pos.age_minutes,
                highest_pnl=pos.pnl_at_high,
            )

        # 📉 RÈGLE 3 — Trailing Stop (dès +15% → SL à -10% du max)
        if self.config.trailing_enabled:
            action = self._check_trailing(pos)
            if action and action.should_sell:
                self._stats["trailing_triggered"] += 1
                return action

        # ⏰ RÈGLE 1 — Time Stop (> 15 min sans +20%)
        if self.config.time_stop_enabled:
            action = self._check_time_stop(pos)
            if action and action.should_sell:
                self._stats["time_stop_triggered"] += 1
                return action

        # 📉 RÈGLE 4 — Momentum Stop (ATH -15% + token avait pumpé)
        if self.config.momentum_stop_enabled:
            action = self._check_momentum_stop(pos)
            if action and action.should_sell:
                self._stats["momentum_stop_triggered"] += 1
                return action

        # ✅ HOLD — Aucune règle déclenchée
        return CBAction(
            should_sell=False,
            reason="Hold",
            rule="hold",
            pnl_pct=pos.pnl_pct,
            age_minutes=pos.age_minutes,
            highest_pnl=pos.pnl_at_high,
        )

    # ----------------------------------------------------------
    # RÈGLES INDIVIDUELLES
    # ----------------------------------------------------------

    def _check_trailing(self, pos: CBPosition) -> Optional[CBAction]:
        """RÈGLE 3 — Trailing Stop dynamique"""
        # Le trailing s'active seulement si le prix a atteint +15% depuis l'entrée
        if pos.pnl_at_high < self.config.trailing_activation_pct:
            return None

        pos.trailing_activated = True

        # Déterminer le % de trailing selon le niveau de profit atteint
        if pos.pnl_at_high >= 100:
            # Moonshot (+100%+) → trailing ultra serré à -6%
            trailing_pct = self.config.trailing_ultra_tight_pct
        elif pos.pnl_at_high >= 50:
            # Gros pump (+50%+) → trailing serré à -8%
            trailing_pct = self.config.trailing_tight_pct
        else:
            # Pump normal (+15% à +50%) → trailing standard à -10%
            trailing_pct = self.config.trailing_stop_pct

        # Vérifier si le prix a chuté de X% depuis l'ATH
        if pos.drop_from_high_pct <= -trailing_pct:
            return CBAction(
                should_sell=True,
                reason=(f"📉 Trailing Stop (chute {pos.drop_from_high_pct:.1f}% depuis ATH, "
                        f"seuil: -{trailing_pct}%)"),
                rule="trailing",
                pnl_pct=pos.pnl_pct,
                age_minutes=pos.age_minutes,
                highest_pnl=pos.pnl_at_high,
            )
        return None

    def _check_time_stop(self, pos: CBPosition) -> Optional[CBAction]:
        """RÈGLE 1 — Time Stop (> 15 min sans +20%)"""
        if pos.age_minutes < self.config.time_stop_minutes:
            return None

        # Si le PnL est sous le seuil (+20%), sortie forcée
        if pos.pnl_pct < self.config.time_stop_min_profit:
            return CBAction(
                should_sell=True,
                reason=(f"⏰ Time Stop ({pos.age_minutes:.0f} min, "
                        f"PnL {pos.pnl_pct:+.1f}% < +{self.config.time_stop_min_profit}%)"),
                rule="time_stop",
                pnl_pct=pos.pnl_pct,
                age_minutes=pos.age_minutes,
                highest_pnl=pos.pnl_at_high,
            )
        return None

    def _check_momentum_stop(self, pos: CBPosition) -> Optional[CBAction]:
        """RÈGLE 4 — Momentum Stop (prix sous ATH -15% + avait pumpé)"""
        # Le token doit avoir eu un pump minimum (+5% depuis l'entrée)
        if pos.pnl_at_high < self.config.momentum_stop_min_pump:
            return None

        # Vérifier la chute depuis l'ATH
        if pos.drop_from_high_pct <= -self.config.momentum_stop_drop_pct:
            return CBAction(
                should_sell=True,
                reason=(f"📉 Momentum Stop (ATH {pos.pnl_at_high:+.1f}%, "
                        f"chute {pos.drop_from_high_pct:.1f}% depuis ATH)"),
                rule="momentum_stop",
                pnl_pct=pos.pnl_pct,
                age_minutes=pos.age_minutes,
                highest_pnl=pos.pnl_at_high,
            )
        return None

    # ----------------------------------------------------------
    # UTILITAIRES
    # ----------------------------------------------------------

    def get_stats(self) -> dict:
        """Statistiques du CircuitBreaker"""
        return {
            **self._stats,
            "active_positions": len(self.positions),
            "trailing_active": sum(1 for p in self.positions.values() if p.trailing_activated),
        }

    def get_position_status(self, token_address: str) -> Optional[dict]:
        """Status détaillé d'une position"""
        pos = self.positions.get(token_address)
        if not pos:
            return None
        return {
            "symbol": pos.token_symbol,
            "pnl_pct": pos.pnl_pct,
            "age_minutes": pos.age_minutes,
            "highest_pnl": pos.pnl_at_high,
            "drop_from_high": pos.drop_from_high_pct,
            "trailing_activated": pos.trailing_activated,
            "time_remaining": max(0, self.config.time_stop_minutes - pos.age_minutes),
        }

    def sync_from_existing_positions(self, positions: list):
        """
        Synchroniser le CircuitBreaker avec des positions existantes.
        Utile au redémarrage du bot.
        
        positions: liste de dicts avec token_address, token_symbol, 
                   entry_price_usd, entry_time, highest_price, current_price
        """
        for p in positions:
            entry_time = p.get("entry_time")
            if isinstance(entry_time, str):
                try:
                    entry_time = datetime.fromisoformat(entry_time).timestamp()
                except (ValueError, TypeError):
                    entry_time = time.time()

            cb_pos = CBPosition(
                token_address=p["token_address"],
                token_symbol=p.get("token_symbol", "???"),
                entry_price=p.get("entry_price_usd", 0),
                entry_time=entry_time or time.time(),
                highest_price=p.get("highest_price", p.get("entry_price_usd", 0)),
                current_price=p.get("current_price", p.get("entry_price_usd", 0)),
            )
            self.positions[p["token_address"]] = cb_pos

        logger.info(f"[CircuitBreaker] Synchronisé {len(positions)} positions existantes")

    def update_config(self, **kwargs):
        """Mettre à jour la config dynamiquement"""
        for key, value in kwargs.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
                logger.info(f"[CircuitBreaker] Config mise à jour: {key} = {value}")
