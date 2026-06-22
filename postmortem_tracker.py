"""
Postmortem Tracker — Autopsie de chaque trade terminé.

Pour chaque trade fermé, ce module :
1. Enregistre les conditions exactes à l'entrée et à la sortie
2. Suit le prix du token APRÈS la vente (5min, 15min, 30min, 1h)
3. Calcule le "vrai max" qu'on aurait pu capturer
4. Détermine si la sortie était optimale ou non
5. Identifie les patterns récurrents (erreurs systématiques)
6. Génère des recommandations pour ajuster les paramètres

Usage:
    from postmortem_tracker import PostmortemTracker
    
    tracker = PostmortemTracker()
    
    # Quand un trade est fermé:
    tracker.record_exit(
        token_address="...",
        token_symbol="TOKEN",
        token_name="Token Name",
        strategy="sniper",
        entry_time="2026-06-22T14:00:00",
        entry_price=0.001,
        exit_price=0.0012,
        highest_price=0.0015,
        exit_pnl_pct=20.0,
        exit_reason="🎯 Take Profit",
        amount_sol=0.05,
        hold_duration_min=8.5,
        liquidity_at_entry=15000,
        liquidity_at_exit=12000,
        volume_at_entry=50000,
        volume_at_exit=30000,
    )
    
    # Le tracker vérifie automatiquement le prix post-vente
    completed = tracker.run_pending_checks()
    
    # Rapport complet
    report = tracker.get_postmortem_report()
"""

import json
import os
import time
import logging
import requests
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Tuple

logger = logging.getLogger(__name__)

# Répertoire de données persistantes
DATA_DIR = os.environ.get("PERSISTENT_DATA_DIR", os.path.dirname(os.path.abspath(__file__)))


@dataclass
class TradePostmortem:
    """Autopsie complète d'un trade terminé"""
    # Identité
    token_address: str
    token_symbol: str
    token_name: str
    strategy: str

    # Entrée
    entry_time: str
    entry_price: float
    liquidity_at_entry: float = 0.0
    volume_at_entry: float = 0.0

    # Pendant le hold
    highest_price: float = 0.0
    ath_pnl_pct: float = 0.0  # PnL au plus haut
    time_to_ath_min: float = 0.0  # Temps entre entrée et ATH

    # Sortie
    exit_time: str = ""
    exit_price: float = 0.0
    exit_pnl_pct: float = 0.0
    exit_reason: str = ""
    hold_duration_min: float = 0.0
    amount_sol: float = 0.0
    liquidity_at_exit: float = 0.0
    volume_at_exit: float = 0.0

    # Post-vente (rempli après les checks)
    price_5min_after: float = 0.0
    price_15min_after: float = 0.0
    price_30min_after: float = 0.0
    price_1h_after: float = 0.0
    pnl_5min_after: float = 0.0   # PnL si on avait gardé 5 min de plus
    pnl_15min_after: float = 0.0
    pnl_30min_after: float = 0.0
    pnl_1h_after: float = 0.0
    post_exit_max_price: float = 0.0  # Prix max atteint après la vente
    post_exit_max_pnl: float = 0.0    # PnL max qu'on aurait eu
    post_exit_min_price: float = 0.0  # Prix min après la vente
    post_exit_min_pnl: float = 0.0    # Pire PnL post-vente

    # Analyse
    missed_profit_pct: float = 0.0  # Profit raté (post_max - exit_pnl)
    verdict: str = ""  # "perfect", "good", "hold_longer", "sell_earlier", "neutral"
    analysis_complete: bool = False
    analysis_timestamp: str = ""

    # Contexte (pour identifier les patterns)
    token_age_hours: float = 0.0
    buys_5min_at_entry: int = 0
    sells_5min_at_entry: int = 0


@dataclass
class PendingCheck:
    """Check post-vente en attente"""
    token_address: str
    token_symbol: str
    entry_price: float
    exit_price: float
    exit_pnl_pct: float
    exit_time: float  # timestamp
    checks_done: List[str] = field(default_factory=list)  # ["5min", "15min", ...]
    prices_collected: Dict[str, float] = field(default_factory=dict)
    max_price_seen: float = 0.0
    min_price_seen: float = 999999.0


class PostmortemTracker:
    """
    Tracker post-mortem pour analyser chaque trade après sa clôture.
    
    Fonctionnement:
    1. record_exit() → enregistre le trade et planifie les checks post-vente
    2. run_pending_checks() → vérifie le prix à T+5, T+15, T+30, T+60
    3. Quand tous les checks sont faits → analyse complète + verdict
    """

    def __init__(self):
        self.postmortems: List[TradePostmortem] = []
        self.pending_checks: List[PendingCheck] = []
        self._load()

    # ================================================================
    # ENREGISTREMENT
    # ================================================================

    def record_exit(
        self,
        token_address: str,
        token_symbol: str,
        token_name: str,
        strategy: str,
        entry_time: str,
        entry_price: float,
        exit_price: float,
        highest_price: float,
        exit_pnl_pct: float,
        exit_reason: str,
        amount_sol: float,
        hold_duration_min: float = 0.0,
        liquidity_at_entry: float = 0.0,
        liquidity_at_exit: float = 0.0,
        volume_at_entry: float = 0.0,
        volume_at_exit: float = 0.0,
        token_age_hours: float = 0.0,
        buys_5min_at_entry: int = 0,
        sells_5min_at_entry: int = 0,
    ):
        """Enregistrer un trade terminé et planifier les checks post-vente"""

        # Calculer le hold duration si pas fourni
        if hold_duration_min == 0.0 and entry_time:
            try:
                entry_dt = datetime.fromisoformat(entry_time)
                hold_duration_min = (datetime.utcnow() - entry_dt).total_seconds() / 60.0
            except (ValueError, TypeError):
                pass

        # Calculer l'ATH PnL
        ath_pnl_pct = 0.0
        time_to_ath_min = 0.0
        if highest_price > 0 and entry_price > 0:
            ath_pnl_pct = ((highest_price - entry_price) / entry_price) * 100

        # Créer le postmortem
        pm = TradePostmortem(
            token_address=token_address,
            token_symbol=token_symbol,
            token_name=token_name,
            strategy=strategy,
            entry_time=entry_time,
            entry_price=entry_price,
            highest_price=highest_price,
            ath_pnl_pct=ath_pnl_pct,
            time_to_ath_min=time_to_ath_min,
            exit_time=datetime.utcnow().isoformat(),
            exit_price=exit_price,
            exit_pnl_pct=exit_pnl_pct,
            exit_reason=exit_reason,
            hold_duration_min=hold_duration_min,
            amount_sol=amount_sol,
            liquidity_at_entry=liquidity_at_entry,
            liquidity_at_exit=liquidity_at_exit,
            volume_at_entry=volume_at_entry,
            volume_at_exit=volume_at_exit,
            token_age_hours=token_age_hours,
            buys_5min_at_entry=buys_5min_at_entry,
            sells_5min_at_entry=sells_5min_at_entry,
        )
        self.postmortems.append(pm)

        # Planifier les checks post-vente
        pending = PendingCheck(
            token_address=token_address,
            token_symbol=token_symbol,
            entry_price=entry_price,
            exit_price=exit_price,
            exit_pnl_pct=exit_pnl_pct,
            exit_time=time.time(),
            max_price_seen=exit_price,
            min_price_seen=exit_price,
        )
        self.pending_checks.append(pending)

        self._save()
        logger.info(f"📋 Postmortem enregistré: {token_symbol} | PnL: {exit_pnl_pct:+.1f}% | "
                    f"Raison: {exit_reason} | Hold: {hold_duration_min:.0f}min")

    # ================================================================
    # CHECKS POST-VENTE
    # ================================================================

    CHECK_SCHEDULE = [
        ("5min", 5 * 60),
        ("15min", 15 * 60),
        ("30min", 30 * 60),
        ("1h", 60 * 60),
    ]

    def run_pending_checks(self) -> List[Dict]:
        """
        Exécuter les checks post-vente en attente.
        Retourne la liste des postmortems complétés lors de cet appel.
        """
        completed = []
        now = time.time()

        for pending in self.pending_checks[:]:
            elapsed = now - pending.exit_time

            for check_name, check_delay in self.CHECK_SCHEDULE:
                if check_name in pending.checks_done:
                    continue

                if elapsed >= check_delay:
                    # Récupérer le prix actuel
                    current_price = self._get_current_price(pending.token_address)
                    if current_price > 0:
                        pending.prices_collected[check_name] = current_price
                        pending.checks_done.append(check_name)

                        # Tracker max/min post-vente
                        if current_price > pending.max_price_seen:
                            pending.max_price_seen = current_price
                        if current_price < pending.min_price_seen:
                            pending.min_price_seen = current_price

                        logger.info(f"📊 Postmortem check {check_name}: "
                                    f"{pending.token_symbol} = ${current_price:.8f}")

            # Tous les checks faits ?
            if len(pending.checks_done) >= len(self.CHECK_SCHEDULE):
                # Finaliser l'analyse
                result = self._finalize_postmortem(pending)
                if result:
                    completed.append(result)
                self.pending_checks.remove(pending)

            # Timeout: si > 2h sans compléter, abandonner
            elif elapsed > 7200:
                # Finaliser avec ce qu'on a
                result = self._finalize_postmortem(pending)
                if result:
                    completed.append(result)
                self.pending_checks.remove(pending)

        if completed:
            self._save()

        return completed

    def _finalize_postmortem(self, pending: PendingCheck) -> Optional[Dict]:
        """Finaliser l'analyse post-mortem d'un trade"""

        # Trouver le postmortem correspondant
        pm = None
        for p in reversed(self.postmortems):
            if p.token_address == pending.token_address and not p.analysis_complete:
                pm = p
                break

        if not pm:
            return None

        entry_price = pending.entry_price

        # Remplir les prix post-vente
        if "5min" in pending.prices_collected:
            pm.price_5min_after = pending.prices_collected["5min"]
            pm.pnl_5min_after = ((pm.price_5min_after - entry_price) / entry_price) * 100 if entry_price > 0 else 0

        if "15min" in pending.prices_collected:
            pm.price_15min_after = pending.prices_collected["15min"]
            pm.pnl_15min_after = ((pm.price_15min_after - entry_price) / entry_price) * 100 if entry_price > 0 else 0

        if "30min" in pending.prices_collected:
            pm.price_30min_after = pending.prices_collected["30min"]
            pm.pnl_30min_after = ((pm.price_30min_after - entry_price) / entry_price) * 100 if entry_price > 0 else 0

        if "1h" in pending.prices_collected:
            pm.price_1h_after = pending.prices_collected["1h"]
            pm.pnl_1h_after = ((pm.price_1h_after - entry_price) / entry_price) * 100 if entry_price > 0 else 0

        # Max et min post-vente
        pm.post_exit_max_price = pending.max_price_seen
        pm.post_exit_min_price = pending.min_price_seen
        if entry_price > 0:
            pm.post_exit_max_pnl = ((pending.max_price_seen - entry_price) / entry_price) * 100
            pm.post_exit_min_pnl = ((pending.min_price_seen - entry_price) / entry_price) * 100

        # Profit raté
        pm.missed_profit_pct = max(0, pm.post_exit_max_pnl - pm.exit_pnl_pct)

        # Verdict
        pm.verdict = self._determine_verdict(pm)
        pm.analysis_complete = True
        pm.analysis_timestamp = datetime.utcnow().isoformat()

        return {
            "token_symbol": pm.token_symbol,
            "exit_pnl": pm.exit_pnl_pct,
            "pnl_1h_after": pm.pnl_1h_after,
            "post_max": pm.post_exit_max_pnl,
            "missed": pm.missed_profit_pct,
            "verdict": pm.verdict,
            "hold_duration": pm.hold_duration_min,
            "exit_reason": pm.exit_reason,
        }

    def _determine_verdict(self, pm: TradePostmortem) -> str:
        """
        Déterminer le verdict du trade:
        - "perfect": vente au bon moment (le token crash après)
        - "good": vente correcte (le token ne monte pas beaucoup plus)
        - "hold_longer": on aurait dû garder (le token continue à monter)
        - "sell_earlier": on aurait dû vendre plus tôt (le token avait un ATH plus haut)
        - "neutral": pas de conclusion claire
        """

        # Si le token a crashé après la vente → vente parfaite
        if pm.pnl_1h_after < pm.exit_pnl_pct - 10:
            return "perfect"

        # Si le token n'a pas bougé significativement → bonne vente
        if abs(pm.pnl_1h_after - pm.exit_pnl_pct) < 10:
            return "good"

        # Si le token a continué à monter significativement → hold longer
        if pm.post_exit_max_pnl > pm.exit_pnl_pct + 20:
            return "hold_longer"

        # Si l'ATH pendant le hold était bien plus haut que le prix de vente
        if pm.ath_pnl_pct > pm.exit_pnl_pct + 30:
            return "sell_earlier"

        # Si le token a légèrement monté → bonne vente quand même
        if pm.pnl_1h_after > pm.exit_pnl_pct + 5:
            return "hold_longer"

        return "neutral"

    # ================================================================
    # RAPPORT & INSIGHTS
    # ================================================================

    def get_postmortem_report(self) -> Dict:
        """Générer un rapport complet de tous les postmortems"""

        completed = [pm for pm in self.postmortems if pm.analysis_complete]

        if not completed:
            return {
                "status": "no_data",
                "total_trades": len(self.postmortems),
                "pending": len(self.pending_checks),
            }

        # Statistiques globales
        total = len(completed)
        avg_hold = sum(pm.hold_duration_min for pm in completed) / total
        avg_pnl = sum(pm.exit_pnl_pct for pm in completed) / total
        avg_ath = sum(pm.ath_pnl_pct for pm in completed) / total
        avg_missed = sum(pm.missed_profit_pct for pm in completed) / total
        avg_post_1h = sum(pm.pnl_1h_after for pm in completed) / total

        # Verdicts
        verdicts = {}
        for pm in completed:
            verdicts[pm.verdict] = verdicts.get(pm.verdict, 0) + 1

        # Par raison de sortie
        by_exit_reason = {}
        for pm in completed:
            # Simplifier la raison
            reason_key = self._simplify_reason(pm.exit_reason)
            if reason_key not in by_exit_reason:
                by_exit_reason[reason_key] = {
                    "count": 0, "avg_pnl": 0, "avg_missed": 0,
                    "avg_hold": 0, "verdicts": {}
                }
            stats = by_exit_reason[reason_key]
            stats["count"] += 1
            stats["avg_pnl"] += pm.exit_pnl_pct
            stats["avg_missed"] += pm.missed_profit_pct
            stats["avg_hold"] += pm.hold_duration_min
            stats["verdicts"][pm.verdict] = stats["verdicts"].get(pm.verdict, 0) + 1

        # Moyenner
        for reason, stats in by_exit_reason.items():
            n = stats["count"]
            stats["avg_pnl"] /= n
            stats["avg_missed"] /= n
            stats["avg_hold"] /= n

        # Tokens qui ont le plus monté après la vente (regrets)
        biggest_misses = sorted(completed, key=lambda pm: pm.missed_profit_pct, reverse=True)[:5]

        # Meilleures ventes (token crash après)
        best_exits = sorted(completed, key=lambda pm: pm.exit_pnl_pct - pm.pnl_1h_after, reverse=True)[:5]

        # Recommandations
        recommendations = self._generate_recommendations(completed)

        return {
            "status": "ok",
            "total_analyzed": total,
            "pending": len(self.pending_checks),
            "avg_hold_min": avg_hold,
            "avg_pnl": avg_pnl,
            "avg_ath_during_hold": avg_ath,
            "avg_missed_profit": avg_missed,
            "avg_pnl_1h_after": avg_post_1h,
            "verdicts": verdicts,
            "by_exit_reason": by_exit_reason,
            "biggest_misses": [
                {"symbol": pm.token_symbol, "exit_pnl": pm.exit_pnl_pct,
                 "max_after": pm.post_exit_max_pnl, "missed": pm.missed_profit_pct}
                for pm in biggest_misses
            ],
            "best_exits": [
                {"symbol": pm.token_symbol, "exit_pnl": pm.exit_pnl_pct,
                 "pnl_1h_after": pm.pnl_1h_after}
                for pm in best_exits
            ],
            "recommendations": recommendations,
        }

    def get_recent_postmortems(self, n: int = 5) -> List[TradePostmortem]:
        """Retourner les N derniers postmortems complétés"""
        completed = [pm for pm in self.postmortems if pm.analysis_complete]
        return completed[-n:]

    def get_trade_postmortem(self, token_address: str) -> Optional[TradePostmortem]:
        """Récupérer le postmortem d'un trade spécifique"""
        for pm in reversed(self.postmortems):
            if pm.token_address == token_address:
                return pm
        return None

    # ================================================================
    # RECOMMANDATIONS AUTOMATIQUES
    # ================================================================

    def _generate_recommendations(self, completed: List[TradePostmortem]) -> List[str]:
        """Générer des recommandations basées sur les données postmortem"""
        recs = []

        if len(completed) < 5:
            return ["Pas assez de données (min 5 trades analysés)"]

        # 1. Analyser si le TP est trop bas
        hold_longer_count = sum(1 for pm in completed if pm.verdict == "hold_longer")
        hold_longer_pct = (hold_longer_count / len(completed)) * 100
        if hold_longer_pct > 40:
            avg_max_post = sum(pm.post_exit_max_pnl for pm in completed
                              if pm.verdict == "hold_longer") / max(hold_longer_count, 1)
            recs.append(f"⬆️ TP trop bas: {hold_longer_pct:.0f}% des trades continuent après. "
                        f"Max moyen post-vente: +{avg_max_post:.0f}%. Augmenter le TP.")

        # 2. Analyser si le Time Stop est trop agressif
        time_stop_trades = [pm for pm in completed if "Time Stop" in pm.exit_reason]
        if time_stop_trades:
            ts_that_pumped = [pm for pm in time_stop_trades if pm.post_exit_max_pnl > 20]
            if len(ts_that_pumped) > len(time_stop_trades) * 0.3:
                recs.append(f"⏰ Time Stop trop agressif: {len(ts_that_pumped)}/{len(time_stop_trades)} "
                            f"tokens ont pumpé après. Augmenter le délai.")

        # 3. Analyser si le SL est bien calibré
        sl_trades = [pm for pm in completed if "Stop Loss" in pm.exit_reason]
        if sl_trades:
            sl_that_recovered = [pm for pm in sl_trades if pm.pnl_1h_after > pm.exit_pnl_pct + 10]
            if len(sl_that_recovered) > len(sl_trades) * 0.2:
                recs.append(f"🛑 SL trop serré: {len(sl_that_recovered)}/{len(sl_trades)} "
                            f"tokens ont récupéré après le SL. Élargir légèrement.")
            else:
                recs.append(f"✅ SL bien calibré: {len(sl_trades) - len(sl_that_recovered)}/{len(sl_trades)} "
                            f"tokens ont continué à chuter après le SL.")

        # 4. Analyser le trailing stop
        trailing_trades = [pm for pm in completed if "Trailing" in pm.exit_reason]
        if trailing_trades:
            avg_exit_trailing = sum(pm.exit_pnl_pct for pm in trailing_trades) / len(trailing_trades)
            avg_post_trailing = sum(pm.pnl_1h_after for pm in trailing_trades) / len(trailing_trades)
            if avg_post_trailing > avg_exit_trailing + 10:
                recs.append(f"📉 Trailing trop serré: vente moy à +{avg_exit_trailing:.0f}%, "
                            f"1h après: +{avg_post_trailing:.0f}%. Élargir le trailing.")
            else:
                recs.append(f"✅ Trailing bien calibré: vente moy à +{avg_exit_trailing:.0f}%, "
                            f"1h après: +{avg_post_trailing:.0f}%.")

        # 5. Analyser le hold moyen
        avg_hold = sum(pm.hold_duration_min for pm in completed) / len(completed)
        profitable = [pm for pm in completed if pm.exit_pnl_pct > 0]
        if profitable:
            avg_hold_winners = sum(pm.hold_duration_min for pm in profitable) / len(profitable)
            recs.append(f"⏱️ Hold moyen gagnants: {avg_hold_winners:.0f}min vs global: {avg_hold:.0f}min")

        # 6. Meilleure stratégie
        by_strategy = {}
        for pm in completed:
            s = pm.strategy
            if s not in by_strategy:
                by_strategy[s] = {"count": 0, "total_pnl": 0, "wins": 0}
            by_strategy[s]["count"] += 1
            by_strategy[s]["total_pnl"] += pm.exit_pnl_pct
            if pm.exit_pnl_pct > 0:
                by_strategy[s]["wins"] += 1

        for strat, data in by_strategy.items():
            wr = (data["wins"] / data["count"]) * 100
            avg = data["total_pnl"] / data["count"]
            recs.append(f"📊 {strat}: WR {wr:.0f}%, PnL moy: {avg:+.1f}% ({data['count']} trades)")

        return recs

    # ================================================================
    # PATTERNS & ERREURS SYSTÉMATIQUES
    # ================================================================

    def identify_patterns(self) -> Dict:
        """Identifier les erreurs systématiques dans le trading"""
        completed = [pm for pm in self.postmortems if pm.analysis_complete]

        if len(completed) < 10:
            return {"status": "need_more_data", "min_required": 10}

        patterns = {
            "selling_too_early": False,
            "selling_too_late": False,
            "time_stop_too_aggressive": False,
            "sl_too_tight": False,
            "best_hold_duration": 0,
            "best_exit_reason": "",
            "worst_exit_reason": "",
        }

        # Pattern: vente trop tôt (> 50% des trades continuent après)
        continued_up = sum(1 for pm in completed if pm.pnl_1h_after > pm.exit_pnl_pct + 5)
        if continued_up > len(completed) * 0.5:
            patterns["selling_too_early"] = True

        # Pattern: vente trop tard (ATH moyen >> prix de vente)
        avg_ath_vs_exit = sum(pm.ath_pnl_pct - pm.exit_pnl_pct for pm in completed) / len(completed)
        if avg_ath_vs_exit > 20:
            patterns["selling_too_late"] = True

        # Meilleure durée de hold (celle qui donne le meilleur PnL moyen)
        by_duration = {}
        for pm in completed:
            bucket = int(pm.hold_duration_min // 5) * 5  # Par tranches de 5 min
            if bucket not in by_duration:
                by_duration[bucket] = []
            by_duration[bucket].append(pm.exit_pnl_pct)

        if by_duration:
            best_bucket = max(by_duration.items(),
                              key=lambda x: sum(x[1]) / len(x[1]) if x[1] else -999)
            patterns["best_hold_duration"] = best_bucket[0]

        # Meilleure/pire raison de sortie
        by_reason = {}
        for pm in completed:
            reason = self._simplify_reason(pm.exit_reason)
            if reason not in by_reason:
                by_reason[reason] = []
            by_reason[reason].append(pm.exit_pnl_pct)

        if by_reason:
            best_reason = max(by_reason.items(),
                              key=lambda x: sum(x[1]) / len(x[1]) if x[1] else -999)
            worst_reason = min(by_reason.items(),
                               key=lambda x: sum(x[1]) / len(x[1]) if x[1] else 999)
            patterns["best_exit_reason"] = best_reason[0]
            patterns["worst_exit_reason"] = worst_reason[0]

        return patterns

    # ================================================================
    # UTILITAIRES
    # ================================================================

    def _simplify_reason(self, reason: str) -> str:
        """Simplifier la raison de sortie pour le groupement"""
        if "Take Profit" in reason:
            return "Take Profit"
        elif "Stop Loss" in reason:
            return "Stop Loss"
        elif "Trailing" in reason:
            return "Trailing Stop"
        elif "Time Stop" in reason:
            return "Time Stop"
        elif "Momentum Stop" in reason:
            return "Momentum Stop"
        elif "LP" in reason or "Liquidity" in reason:
            return "LP Emergency"
        else:
            return "Autre"

    def _get_current_price(self, token_address: str) -> float:
        """Récupérer le prix actuel d'un token via DexScreener"""
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                pairs = data.get("pairs", [])
                if pairs:
                    # Prendre la paire avec le plus de liquidité
                    best_pair = max(pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
                    price = float(best_pair.get("priceUsd", 0) or 0)
                    return price
        except Exception as e:
            logger.error(f"Erreur prix postmortem {token_address}: {e}")
        return 0.0

    # ================================================================
    # PERSISTANCE
    # ================================================================

    def _save(self):
        """Sauvegarder les données sur disque"""
        filepath = os.path.join(DATA_DIR, "postmortem_data.json")
        data = {
            "postmortems": [asdict(pm) for pm in self.postmortems[-200:]],  # Garder les 200 derniers
            "pending_checks": [
                {
                    "token_address": p.token_address,
                    "token_symbol": p.token_symbol,
                    "entry_price": p.entry_price,
                    "exit_price": p.exit_price,
                    "exit_pnl_pct": p.exit_pnl_pct,
                    "exit_time": p.exit_time,
                    "checks_done": p.checks_done,
                    "prices_collected": p.prices_collected,
                    "max_price_seen": p.max_price_seen,
                    "min_price_seen": p.min_price_seen,
                }
                for p in self.pending_checks
            ],
        }
        try:
            with open(filepath, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Erreur sauvegarde postmortem: {e}")

    def _load(self):
        """Charger les données depuis le disque"""
        filepath = os.path.join(DATA_DIR, "postmortem_data.json")
        if not os.path.exists(filepath):
            return

        try:
            with open(filepath, "r") as f:
                data = json.load(f)

            # Charger les postmortems
            for pm_data in data.get("postmortems", []):
                pm = TradePostmortem(**pm_data)
                self.postmortems.append(pm)

            # Charger les pending checks
            for pc_data in data.get("pending_checks", []):
                pc = PendingCheck(
                    token_address=pc_data["token_address"],
                    token_symbol=pc_data["token_symbol"],
                    entry_price=pc_data["entry_price"],
                    exit_price=pc_data["exit_price"],
                    exit_pnl_pct=pc_data["exit_pnl_pct"],
                    exit_time=pc_data["exit_time"],
                    checks_done=pc_data.get("checks_done", []),
                    prices_collected=pc_data.get("prices_collected", {}),
                    max_price_seen=pc_data.get("max_price_seen", 0),
                    min_price_seen=pc_data.get("min_price_seen", 999999),
                )
                self.pending_checks.append(pc)

            logger.info(f"📋 Postmortem chargé: {len(self.postmortems)} trades, "
                        f"{len(self.pending_checks)} en attente")
        except Exception as e:
            logger.error(f"Erreur chargement postmortem: {e}")
