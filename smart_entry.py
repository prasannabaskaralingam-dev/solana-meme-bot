"""
Module Smart Entry - Confirmation Multi-Timeframe & Volume Spike Detection
Évite les faux signaux en vérifiant la tendance sur plusieurs timeframes
et en détectant les vrais spikes de volume avant d'acheter.
"""

import time
import logging
from typing import Optional, Tuple
from dataclasses import dataclass, field
from collections import deque

logger = logging.getLogger(__name__)


@dataclass
class TokenSnapshot:
    """Snapshot d'un token à un instant T"""
    address: str
    timestamp: float
    price_usd: float
    volume_5m: float  # Volume estimé sur 5min
    buys_5m: int
    sells_5m: int
    liquidity_usd: float
    market_cap: float
    price_change_5m: float
    price_change_1h: float


@dataclass
class EntrySignal:
    """Signal d'entrée validé par multi-confirmation"""
    address: str
    token_name: str
    token_symbol: str
    confidence: float  # 0.0 à 1.0
    reasons: list = field(default_factory=list)
    strategy: str = "momentum"  # "sniper", "momentum", "volume_spike"
    volume_multiplier: float = 1.0  # Combien de fois le volume normal


class SmartEntryEngine:
    """
    Moteur d'entrée intelligent.
    
    Principes:
    1. Double-check: un token doit passer 2 analyses espacées de 30-60s
    2. Volume spike: détecter quand le volume explose vs la moyenne
    3. Momentum confirmé: le prix doit monter entre les 2 checks
    4. Buyer dominance: ratio acheteurs/vendeurs croissant
    """

    def __init__(self):
        # Historique des snapshots par token (max 5 par token)
        self.token_history: dict[str, deque] = {}
        # Tokens en "watchlist" (premier check passé, en attente de confirmation)
        self.watchlist: dict[str, dict] = {}
        # Tokens confirmés prêts à acheter
        self.confirmed_entries: dict[str, EntrySignal] = {}
        # Volume moyen par token (pour détecter les spikes)
        self.volume_baselines: dict[str, float] = {}
        # Timestamp du dernier nettoyage
        self._last_cleanup = time.time()

    def first_check(self, analysis: dict) -> Tuple[bool, str]:
        """
        Premier check d'un token. Si prometteur, l'ajouter à la watchlist
        pour confirmation dans 30-60s.
        
        Returns: (ajouté_à_watchlist, raison)
        """
        address = analysis["address"]
        now = time.time()

        # Déjà en watchlist ?
        if address in self.watchlist:
            return False, "Déjà en watchlist"

        # Déjà confirmé ?
        if address in self.confirmed_entries:
            return False, "Déjà confirmé"

        # Critères pour la watchlist (plus souples que l'achat final)
        score, reasons = self._score_for_watchlist(analysis)

        if score >= 3:
            # Ajouter à la watchlist avec le snapshot actuel
            snapshot = TokenSnapshot(
                address=address,
                timestamp=now,
                price_usd=float(analysis.get("price_usd", 0) or 0),
                volume_5m=self._estimate_volume_5m(analysis),
                buys_5m=analysis.get("buys_5m", 0),
                sells_5m=analysis.get("sells_5m", 0),
                liquidity_usd=analysis.get("liquidity_usd", 0),
                market_cap=analysis.get("market_cap", 0),
                price_change_5m=analysis.get("price_change_5m", 0),
                price_change_1h=analysis.get("price_change_1h", 0),
            )

            self.watchlist[address] = {
                "snapshot": snapshot,
                "analysis": analysis,
                "score": score,
                "reasons": reasons,
                "added_at": now,
            }

            # Stocker dans l'historique
            if address not in self.token_history:
                self.token_history[address] = deque(maxlen=5)
            self.token_history[address].append(snapshot)

            logger.info(f"[SMART] Watchlist +1: {analysis.get('name', address[:12])} "
                       f"(score={score}, raisons: {', '.join(reasons[:3])})")
            return True, f"Watchlist (score {score})"

        return False, f"Score insuffisant ({score}/3)"

    def confirm_check(self, analysis: dict) -> Optional[EntrySignal]:
        """
        Deuxième check d'un token en watchlist.
        Compare avec le premier snapshot pour confirmer le momentum.
        
        Returns: EntrySignal si confirmé, None sinon
        """
        address = analysis["address"]
        now = time.time()

        if address not in self.watchlist:
            return None

        watchlist_entry = self.watchlist[address]
        first_snapshot = watchlist_entry["snapshot"]
        time_elapsed = now - first_snapshot.timestamp

        # Trop tôt (< 20s) ou trop tard (> 120s)
        if time_elapsed < 20:
            return None  # Pas encore le moment de confirmer
        if time_elapsed > 120:
            # Expiré, retirer de la watchlist
            del self.watchlist[address]
            logger.info(f"[SMART] Watchlist expiré: {analysis.get('name', address[:12])}")
            return None

        # Comparer avec le premier snapshot
        current_price = float(analysis.get("price_usd", 0) or 0)
        first_price = first_snapshot.price_usd

        if first_price <= 0 or current_price <= 0:
            del self.watchlist[address]
            return None

        # === CRITÈRES DE CONFIRMATION ===
        confirmation_score = 0
        confirmation_reasons = []

        # 1. Le prix doit être stable ou en hausse (pas en chute)
        price_change_since_first = ((current_price - first_price) / first_price) * 100
        if price_change_since_first >= 0:
            confirmation_score += 1
            if price_change_since_first >= 2:
                confirmation_score += 1
                confirmation_reasons.append(f"📈 Prix +{price_change_since_first:.1f}% depuis 1er check")
        else:
            if price_change_since_first < -5:
                # Chute significative = annuler
                del self.watchlist[address]
                logger.info(f"[SMART] Confirmation FAIL (chute {price_change_since_first:.1f}%): "
                           f"{analysis.get('name', address[:12])}")
                return None

        # 2. Volume en hausse ou stable
        current_volume = self._estimate_volume_5m(analysis)
        first_volume = first_snapshot.volume_5m
        if first_volume > 0:
            volume_change = (current_volume - first_volume) / first_volume
            if volume_change >= 0:
                confirmation_score += 1
                if volume_change >= 0.5:  # +50% de volume
                    confirmation_score += 1
                    confirmation_reasons.append(f"📊 Volume +{volume_change*100:.0f}%")

        # 3. Ratio buy/sell toujours favorable
        current_buys = analysis.get("buys_5m", 0)
        current_sells = analysis.get("sells_5m", 0)
        current_ratio = current_buys / max(current_sells, 1)
        if current_ratio >= 1.5:
            confirmation_score += 1
            confirmation_reasons.append(f"🛒 Ratio B/S: {current_ratio:.1f}x")

        # 4. Volume spike detection
        volume_multiplier = self._detect_volume_spike(address, current_volume)
        if volume_multiplier >= 2.0:
            confirmation_score += 2
            confirmation_reasons.append(f"🔥 Volume spike x{volume_multiplier:.1f}")

        # 5. Liquidité stable ou en hausse (pas de rug pull en cours)
        current_liq = analysis.get("liquidity_usd", 0)
        if current_liq >= first_snapshot.liquidity_usd * 0.9:  # Max -10% de liq
            confirmation_score += 1
        else:
            # Liquidité en chute = danger
            del self.watchlist[address]
            logger.info(f"[SMART] Confirmation FAIL (liq en chute): {analysis.get('name', address[:12])}")
            return None

        # 6. Nombre d'acheteurs en hausse
        if current_buys > first_snapshot.buys_5m:
            confirmation_score += 1
            confirmation_reasons.append(f"👥 Acheteurs: {first_snapshot.buys_5m} → {current_buys}")

        # === DÉCISION ===
        # Score minimum de 4 pour confirmer
        if confirmation_score >= 4:
            # Déterminer la stratégie - SNIPER ONLY MODE
            age = analysis.get("age_hours", 99)
            if age and age <= 1:
                strategy = "sniper"
            elif volume_multiplier >= 3.0:
                strategy = "sniper"  # Traité comme sniper (volume spike sur token frais)
            else:
                # Rejeter les tokens > 1h (pas de momentum)
                if age and age > 1:
                    del self.watchlist[address]
                    return None
                strategy = "sniper"

            # Calculer la confiance (0-1)
            confidence = min(confirmation_score / 8.0, 1.0)

            signal = EntrySignal(
                address=address,
                token_name=analysis.get("name", "Unknown"),
                token_symbol=analysis.get("symbol", "???"),
                confidence=confidence,
                reasons=watchlist_entry["reasons"] + confirmation_reasons,
                strategy=strategy,
                volume_multiplier=volume_multiplier,
            )

            # Retirer de la watchlist, ajouter aux confirmés
            del self.watchlist[address]
            self.confirmed_entries[address] = signal

            # Stocker le snapshot
            snapshot = TokenSnapshot(
                address=address,
                timestamp=now,
                price_usd=current_price,
                volume_5m=current_volume,
                buys_5m=current_buys,
                sells_5m=current_sells,
                liquidity_usd=current_liq,
                market_cap=analysis.get("market_cap", 0),
                price_change_5m=analysis.get("price_change_5m", 0),
                price_change_1h=analysis.get("price_change_1h", 0),
            )
            if address not in self.token_history:
                self.token_history[address] = deque(maxlen=5)
            self.token_history[address].append(snapshot)

            logger.info(f"[SMART] ✅ CONFIRMÉ: {signal.token_name} "
                       f"(confiance={confidence:.0%}, stratégie={strategy}, "
                       f"volume x{volume_multiplier:.1f})")
            return signal

        else:
            # Pas assez de confirmation, garder en watchlist pour un prochain check
            # (sera expiré après 120s)
            logger.info(f"[SMART] Confirmation partielle ({confirmation_score}/4): "
                       f"{analysis.get('name', address[:12])}")
            return None

    def consume_signal(self, address: str) -> Optional[EntrySignal]:
        """Consommer un signal confirmé (après achat)"""
        return self.confirmed_entries.pop(address, None)

    def get_watchlist_tokens(self) -> list[str]:
        """Retourner les adresses en watchlist (pour re-check)"""
        now = time.time()
        ready = []
        for addr, entry in list(self.watchlist.items()):
            elapsed = now - entry["snapshot"].timestamp
            if elapsed >= 25:  # Prêt pour confirmation (>25s)
                ready.append(addr)
            elif elapsed > 120:  # Expiré
                del self.watchlist[addr]
        return ready

    def cleanup(self):
        """Nettoyer les entrées expirées"""
        now = time.time()
        if now - self._last_cleanup < 60:
            return

        self._last_cleanup = now

        # Nettoyer la watchlist (>120s)
        expired = [addr for addr, entry in self.watchlist.items()
                   if now - entry["snapshot"].timestamp > 120]
        for addr in expired:
            del self.watchlist[addr]

        # Nettoyer les signaux confirmés non consommés (>300s)
        expired_signals = [addr for addr, sig in self.confirmed_entries.items()
                          if addr in self.token_history and
                          self.token_history[addr][-1].timestamp < now - 300]
        for addr in expired_signals:
            del self.confirmed_entries[addr]

        # Limiter l'historique
        if len(self.token_history) > 200:
            # Garder les 100 plus récents
            sorted_tokens = sorted(self.token_history.items(),
                                   key=lambda x: x[1][-1].timestamp if x[1] else 0,
                                   reverse=True)
            self.token_history = dict(sorted_tokens[:100])

        if expired or expired_signals:
            logger.info(f"[SMART] Cleanup: {len(expired)} watchlist expirés, "
                       f"{len(expired_signals)} signaux expirés")

    def _score_for_watchlist(self, analysis: dict) -> Tuple[int, list]:
        """Scorer un token pour l'ajout à la watchlist"""
        score = 0
        reasons = []

        # Prix en hausse sur 5min
        p5m = analysis.get("price_change_5m", 0)
        if p5m >= 5:
            score += 1
            reasons.append(f"📈 +{p5m:.0f}% (5m)")
        if p5m >= 15:
            score += 1

        # Prix en hausse sur 1h
        p1h = analysis.get("price_change_1h", 0)
        if p1h >= 20:
            score += 1
            reasons.append(f"📈 +{p1h:.0f}% (1h)")

        # Volume significatif
        vol = analysis.get("volume_24h", 0)
        if vol >= 10000:
            score += 1
            reasons.append(f"💰 Vol: ${vol:,.0f}")

        # Beaucoup d'achats
        buys = analysis.get("buys_5m", 0)
        if buys >= 10:
            score += 1
            reasons.append(f"🛒 {buys} achats/5m")
        if buys >= 30:
            score += 1

        # Ratio favorable
        ratio = analysis.get("buy_sell_ratio_5m", 0)
        if ratio >= 2:
            score += 1
            reasons.append(f"⚡ Ratio {ratio:.1f}x")

        # Token frais - SNIPER ONLY: rejeter si > 1h
        age = analysis.get("age_hours")
        if age and age > 1:
            return 0, []  # Trop vieux pour le mode sniper
        if age and age <= 1:
            score += 2  # Bonus fort pour les tokens très frais
            reasons.append(f"🆕 {age:.1f}h")

        return score, reasons

    def _estimate_volume_5m(self, analysis: dict) -> float:
        """Estimer le volume sur 5 minutes à partir des données disponibles"""
        # DexScreener donne le volume 24h, on estime le 5min
        vol_24h = analysis.get("volume_24h", 0)
        buys_5m = analysis.get("buys_5m", 0)
        sells_5m = analysis.get("sells_5m", 0)
        total_txns_5m = buys_5m + sells_5m

        # Estimation: volume_5m ≈ (volume_24h / 288) * facteur_activité
        # 288 = nombre de périodes de 5min dans 24h
        base_vol = vol_24h / 288 if vol_24h > 0 else 0

        # Ajuster par l'activité récente
        if total_txns_5m > 20:
            return base_vol * 2  # Activité élevée
        elif total_txns_5m > 10:
            return base_vol * 1.5
        return base_vol

    def _detect_volume_spike(self, address: str, current_volume: float) -> float:
        """
        Détecter un spike de volume par rapport à la baseline.
        Returns: multiplicateur (1.0 = normal, 3.0 = 3x le volume habituel)
        """
        if address not in self.token_history or len(self.token_history[address]) < 2:
            # Pas assez d'historique, utiliser la baseline globale
            if address in self.volume_baselines:
                baseline = self.volume_baselines[address]
                if baseline > 0:
                    return current_volume / baseline
            return 1.0

        # Calculer la moyenne des volumes précédents
        history = self.token_history[address]
        prev_volumes = [s.volume_5m for s in history if s.volume_5m > 0]

        if not prev_volumes:
            return 1.0

        avg_volume = sum(prev_volumes) / len(prev_volumes)
        self.volume_baselines[address] = avg_volume

        if avg_volume <= 0:
            return 1.0

        return current_volume / avg_volume

    def get_stats(self) -> dict:
        """Statistiques du moteur smart entry"""
        return {
            "watchlist_size": len(self.watchlist),
            "confirmed_pending": len(self.confirmed_entries),
            "tokens_tracked": len(self.token_history),
            "volume_baselines": len(self.volume_baselines),
        }
