"""
Post-Trade Analyzer - Analyse ce qui se passe APRÈS chaque trade
Répond à 3 questions critiques:
1. À quel % le token atteignait son VRAI maximum ?
2. Combien de temps après l'achat le pump se produit ?
3. Ce qui se passe APRÈS la vente (continuation ou rechute ?)
"""

import json
import os
import time
import logging
import threading
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict

import httpx

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("PERSISTENT_DATA_DIR", BASE_DIR)


@dataclass
class TradeAnalysis:
    """Analyse complète d'un trade (avant, pendant, après)"""
    token_address: str
    token_symbol: str
    token_name: str
    strategy: str

    # Données d'entrée
    entry_time: str = ""
    entry_price: float = 0.0
    amount_sol: float = 0.0

    # Données de sortie
    exit_time: str = ""
    exit_price: float = 0.0
    exit_pnl_pct: float = 0.0
    exit_reason: str = ""

    # === ANALYSE PENDANT LE HOLD ===
    # Prix le plus haut atteint pendant le hold
    highest_price_during_hold: float = 0.0
    highest_pnl_during_hold: float = 0.0  # % max atteint
    time_to_peak_minutes: float = 0.0  # Temps entre achat et ATH

    # === ANALYSE POST-VENTE (5min, 15min, 30min, 1h après) ===
    price_5min_after: float = 0.0
    price_15min_after: float = 0.0
    price_30min_after: float = 0.0
    price_1h_after: float = 0.0

    pnl_5min_after: float = 0.0   # % par rapport au prix d'entrée
    pnl_15min_after: float = 0.0
    pnl_30min_after: float = 0.0
    pnl_1h_after: float = 0.0

    # Verdict post-trade
    missed_profit_pct: float = 0.0  # Combien on a "raté" (ATH - exit)
    post_exit_max_pct: float = 0.0  # Max atteint APRÈS la vente
    would_have_been_better: str = ""  # "hold", "sell_earlier", "perfect"

    # Statut du suivi
    tracking_complete: bool = False
    checks_done: int = 0


class PostTradeAnalyzer:
    """
    Analyse post-trade: suit les tokens APRÈS la vente pour optimiser la stratégie.
    """

    def __init__(self):
        self.analyses: List[TradeAnalysis] = []
        self.pending_checks: Dict[str, dict] = {}  # Tokens à re-checker après vente
        self.data_file = os.path.join(DATA_DIR, "post_trade_analysis.json")
        self._load()
        self._lock = threading.Lock()

    def _load(self):
        """Charger l'historique d'analyses"""
        if os.path.exists(self.data_file):
            try:
                with open(self.data_file, "r") as f:
                    data = json.load(f)
                self.analyses = [TradeAnalysis(**d) for d in data.get("analyses", [])]
                self.pending_checks = data.get("pending_checks", {})
                logger.info(f"📈 Post-Trade Analyzer: {len(self.analyses)} analyses chargées, "
                            f"{len(self.pending_checks)} en attente")
            except Exception as e:
                logger.error(f"Erreur chargement post_trade_analysis: {e}")
                self.analyses = []
                self.pending_checks = {}

    def _save(self):
        """Sauvegarder les analyses"""
        try:
            data = {
                "analyses": [asdict(a) for a in self.analyses[-200:]],  # Garder les 200 dernières
                "pending_checks": self.pending_checks,
            }
            with open(self.data_file, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Erreur sauvegarde post_trade_analysis: {e}")

    def record_trade_exit(self, token_address: str, token_symbol: str, token_name: str,
                          strategy: str, entry_time: str, entry_price: float,
                          exit_price: float, exit_pnl_pct: float, exit_reason: str,
                          highest_price: float, amount_sol: float):
        """
        Enregistrer une vente et programmer le suivi post-trade.
        Appelé quand une position est fermée.
        """
        with self._lock:
            now = datetime.utcnow()

            # Calculer le temps jusqu'au peak
            try:
                entry_dt = datetime.fromisoformat(entry_time)
                hold_duration = (now - entry_dt).total_seconds() / 60.0
            except (ValueError, TypeError):
                hold_duration = 0

            # PnL au plus haut
            highest_pnl = ((highest_price - entry_price) / entry_price * 100
                           if entry_price > 0 else 0)

            # Profit raté (ATH vs exit)
            missed = highest_pnl - exit_pnl_pct if highest_pnl > exit_pnl_pct else 0

            analysis = TradeAnalysis(
                token_address=token_address,
                token_symbol=token_symbol,
                token_name=token_name,
                strategy=strategy,
                entry_time=entry_time,
                entry_price=entry_price,
                amount_sol=amount_sol,
                exit_time=now.isoformat(),
                exit_price=exit_price,
                exit_pnl_pct=exit_pnl_pct,
                exit_reason=exit_reason,
                highest_price_during_hold=highest_price,
                highest_pnl_during_hold=highest_pnl,
                time_to_peak_minutes=hold_duration,  # Approximation
                missed_profit_pct=missed,
            )

            self.analyses.append(analysis)

            # Programmer les checks post-vente (5min, 15min, 30min, 1h)
            self.pending_checks[token_address] = {
                "analysis_index": len(self.analyses) - 1,
                "entry_price": entry_price,
                "exit_time": now.isoformat(),
                "checks": {
                    "5min": {"due": (now + timedelta(minutes=5)).isoformat(), "done": False},
                    "15min": {"due": (now + timedelta(minutes=15)).isoformat(), "done": False},
                    "30min": {"due": (now + timedelta(minutes=30)).isoformat(), "done": False},
                    "1h": {"due": (now + timedelta(hours=1)).isoformat(), "done": False},
                }
            }

            self._save()
            logger.info(f"📊 Post-Trade: tracking {token_symbol} "
                        f"(vendu à {exit_pnl_pct:+.1f}%, ATH était {highest_pnl:+.1f}%)")

    def run_pending_checks(self) -> List[dict]:
        """
        Exécuter les checks post-vente en attente.
        Retourne les analyses complétées pour notification.
        Appelé toutes les 60s par le job scheduler.
        """
        completed = []
        now = datetime.utcnow()
        tokens_to_remove = []

        with self._lock:
            for token_address, check_data in list(self.pending_checks.items()):
                idx = check_data["analysis_index"]
                if idx >= len(self.analyses):
                    tokens_to_remove.append(token_address)
                    continue

                analysis = self.analyses[idx]
                entry_price = check_data["entry_price"]
                all_done = True

                for period, period_data in check_data["checks"].items():
                    if period_data["done"]:
                        continue

                    # Vérifier si c'est le moment
                    due_time = datetime.fromisoformat(period_data["due"])
                    if now < due_time:
                        all_done = False
                        continue

                    # Récupérer le prix actuel
                    current_price = self._fetch_current_price(token_address)
                    if current_price is None or current_price <= 0:
                        all_done = False
                        continue

                    # Calculer le PnL par rapport au prix d'entrée
                    pnl_vs_entry = ((current_price - entry_price) / entry_price * 100
                                    if entry_price > 0 else 0)

                    # Enregistrer selon la période
                    if period == "5min":
                        analysis.price_5min_after = current_price
                        analysis.pnl_5min_after = pnl_vs_entry
                    elif period == "15min":
                        analysis.price_15min_after = current_price
                        analysis.pnl_15min_after = pnl_vs_entry
                    elif period == "30min":
                        analysis.price_30min_after = current_price
                        analysis.pnl_30min_after = pnl_vs_entry
                    elif period == "1h":
                        analysis.price_1h_after = current_price
                        analysis.pnl_1h_after = pnl_vs_entry

                    period_data["done"] = True
                    analysis.checks_done += 1

                    # Mettre à jour le max post-exit
                    if pnl_vs_entry > analysis.post_exit_max_pct:
                        analysis.post_exit_max_pct = pnl_vs_entry

                    logger.info(f"📊 Post-check {period}: {analysis.token_symbol} "
                                f"= {pnl_vs_entry:+.1f}% (vendu à {analysis.exit_pnl_pct:+.1f}%)")

                # Vérifier si tous les checks sont faits
                if all_done or all(c["done"] for c in check_data["checks"].values()):
                    analysis.tracking_complete = True
                    # Déterminer le verdict
                    analysis.would_have_been_better = self._determine_verdict(analysis)
                    tokens_to_remove.append(token_address)
                    completed.append({
                        "token_symbol": analysis.token_symbol,
                        "token_name": analysis.token_name,
                        "exit_pnl": analysis.exit_pnl_pct,
                        "post_max": analysis.post_exit_max_pct,
                        "pnl_1h_after": analysis.pnl_1h_after,
                        "verdict": analysis.would_have_been_better,
                        "missed": analysis.missed_profit_pct,
                    })

            # Nettoyer les checks terminés
            for token in tokens_to_remove:
                if token in self.pending_checks:
                    del self.pending_checks[token]

            if completed or tokens_to_remove:
                self._save()

        return completed

    def _determine_verdict(self, analysis: TradeAnalysis) -> str:
        """Déterminer si la vente était optimale"""
        exit_pnl = analysis.exit_pnl_pct
        post_max = analysis.post_exit_max_pct
        pnl_1h = analysis.pnl_1h_after

        # Le token a continué à monter significativement après la vente
        if post_max > exit_pnl + 20:
            return "hold"  # On aurait dû garder plus longtemps

        # Le token a chuté après la vente → bonne décision
        if pnl_1h < exit_pnl - 10:
            return "perfect"  # Vente parfaite

        # Le token est resté stable
        if abs(pnl_1h - exit_pnl) < 5:
            return "neutral"  # Pas de différence significative

        # On a vendu trop tard (le prix était déjà en chute)
        if exit_pnl < 0 and analysis.highest_pnl_during_hold > 10:
            return "sell_earlier"  # On aurait dû vendre plus tôt

        return "good"  # Vente correcte

    def _fetch_current_price(self, token_address: str) -> Optional[float]:
        """Récupérer le prix actuel d'un token via DexScreener"""
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
            with httpx.Client(timeout=10) as client:
                resp = client.get(url)
                if resp.status_code == 200:
                    data = resp.json()
                    pairs = data.get("pairs", [])
                    if pairs:
                        return float(pairs[0].get("priceUsd", 0) or 0)
        except Exception as e:
            logger.debug(f"Erreur fetch price post-trade {token_address}: {e}")
        return None

    def get_insights(self) -> dict:
        """
        Générer des insights à partir de toutes les analyses complétées.
        Retourne des recommandations pour optimiser TP/SL/Time Stop.
        """
        completed = [a for a in self.analyses if a.tracking_complete]
        if not completed:
            return {"status": "no_data", "message": "Pas encore assez de données"}

        # Stats globales
        total = len(completed)
        missed_profits = [a.missed_profit_pct for a in completed if a.missed_profit_pct > 0]
        avg_missed = sum(missed_profits) / len(missed_profits) if missed_profits else 0

        # Verdicts
        verdicts = {}
        for a in completed:
            v = a.would_have_been_better
            verdicts[v] = verdicts.get(v, 0) + 1

        # Timing du pump
        peak_times = [a.time_to_peak_minutes for a in completed if a.highest_pnl_during_hold > 5]
        avg_peak_time = sum(peak_times) / len(peak_times) if peak_times else 0

        # ATH moyen pendant le hold
        ath_pnls = [a.highest_pnl_during_hold for a in completed]
        avg_ath = sum(ath_pnls) / len(ath_pnls) if ath_pnls else 0

        # Post-exit: le token continue ou rechute ?
        continued_up = sum(1 for a in completed if a.post_exit_max_pct > a.exit_pnl_pct + 10)
        crashed_after = sum(1 for a in completed if a.pnl_1h_after < a.exit_pnl_pct - 20)

        # Recommandations
        recommendations = []

        # TP trop bas ?
        if avg_missed > 15 and verdicts.get("hold", 0) > total * 0.3:
            recommendations.append(
                f"📈 TP probablement trop bas: tu rates en moyenne {avg_missed:.0f}% de profit. "
                f"Suggestion: augmenter TP à +{min(avg_ath * 0.7, 100):.0f}%"
            )

        # TP correct ?
        if verdicts.get("perfect", 0) + verdicts.get("good", 0) > total * 0.6:
            recommendations.append(
                "✅ Ton TP est bien calibré: la majorité des tokens rechutent après ta vente."
            )

        # Time Stop trop court ?
        if avg_peak_time > 20 and verdicts.get("sell_earlier", 0) < total * 0.2:
            recommendations.append(
                f"⏰ Le pump moyen arrive après {avg_peak_time:.0f} min. "
                f"Ton Time Stop de 15 min est peut-être trop agressif."
            )

        # Time Stop parfait ?
        if avg_peak_time <= 15:
            recommendations.append(
                f"✅ Time Stop bien calibré: le pump moyen arrive à {avg_peak_time:.0f} min."
            )

        return {
            "status": "ok",
            "total_analyzed": total,
            "avg_ath_during_hold": avg_ath,
            "avg_missed_profit": avg_missed,
            "avg_time_to_peak": avg_peak_time,
            "continued_up_after_sell": continued_up,
            "crashed_after_sell": crashed_after,
            "verdicts": verdicts,
            "recommendations": recommendations,
            "pct_hold_too_short": verdicts.get("hold", 0) / total * 100 if total > 0 else 0,
            "pct_perfect_exit": (verdicts.get("perfect", 0) + verdicts.get("good", 0)) / total * 100 if total > 0 else 0,
        }

    def get_recent_analyses(self, limit: int = 5) -> List[TradeAnalysis]:
        """Retourner les analyses récentes complétées"""
        completed = [a for a in self.analyses if a.tracking_complete]
        return completed[-limit:]
