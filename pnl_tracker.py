"""
Module PnL Tracker & Dynamic Position Sizing
- Suivi détaillé des performances par stratégie
- Position sizing adaptatif basé sur la confiance et le win rate
- Rapport PnL quotidien/hebdomadaire
- Détection des meilleures/pires stratégies pour auto-optimisation
"""

import os
import json
import time
import logging
from typing import Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Répertoire persistant
DATA_DIR = os.environ.get("PERSISTENT_DATA_DIR", os.path.dirname(os.path.abspath(__file__)))


@dataclass
class TradeResult:
    """Résultat détaillé d'un trade (achat + vente)"""
    token_address: str
    token_symbol: str
    strategy: str  # "sniper", "momentum", "volume_spike", "copy_trade"
    entry_time: str
    exit_time: str
    amount_sol: float
    pnl_pct: float
    pnl_sol: float  # PnL en SOL (estimé)
    exit_reason: str  # "take_profit", "trailing_stop", "stop_loss", "manual"
    hold_duration_min: float  # Durée en minutes
    confidence: float = 0.0  # Score de confiance à l'entrée (0-1)
    volume_spike: float = 1.0  # Multiplicateur de volume à l'entrée


@dataclass
class StrategyStats:
    """Statistiques par stratégie"""
    strategy: str
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    total_pnl_pct: float = 0.0
    total_pnl_sol: float = 0.0
    best_trade_pct: float = 0.0
    worst_trade_pct: float = 0.0
    avg_hold_minutes: float = 0.0
    avg_confidence: float = 0.0

    @property
    def win_rate(self) -> float:
        return (self.wins / max(self.total_trades, 1)) * 100

    @property
    def avg_pnl_pct(self) -> float:
        return self.total_pnl_pct / max(self.total_trades, 1)

    @property
    def profit_factor(self) -> float:
        """Ratio gains/pertes (>1 = profitable)"""
        # Approximation
        if self.losses == 0:
            return float('inf') if self.wins > 0 else 0
        return self.wins / max(self.losses, 1)


class PnLTracker:
    """
    Tracker de PnL avancé avec:
    - Historique détaillé par trade
    - Stats par stratégie
    - Position sizing dynamique
    - Rapports périodiques
    """

    PNL_FILE = os.path.join(DATA_DIR, "pnl_history.json")
    DAILY_FILE = os.path.join(DATA_DIR, "daily_pnl.json")

    def __init__(self):
        self.trade_results: list[TradeResult] = []
        self.daily_pnl: dict[str, dict] = {}  # "2024-01-15" -> {pnl_sol, trades, wins}
        self._load()

    def _load(self):
        """Charger l'historique PnL"""
        if os.path.exists(self.PNL_FILE):
            try:
                with open(self.PNL_FILE, "r") as f:
                    data = json.load(f)
                self.trade_results = [TradeResult(**t) for t in data]
            except Exception as e:
                logger.error(f"[PNL] Erreur chargement: {e}")
                self.trade_results = []

        if os.path.exists(self.DAILY_FILE):
            try:
                with open(self.DAILY_FILE, "r") as f:
                    self.daily_pnl = json.load(f)
            except Exception:
                self.daily_pnl = {}

    def _save(self):
        """Sauvegarder l'historique"""
        try:
            data = []
            for t in self.trade_results[-500:]:  # Garder les 500 derniers
                data.append({
                    "token_address": t.token_address,
                    "token_symbol": t.token_symbol,
                    "strategy": t.strategy,
                    "entry_time": t.entry_time,
                    "exit_time": t.exit_time,
                    "amount_sol": t.amount_sol,
                    "pnl_pct": t.pnl_pct,
                    "pnl_sol": t.pnl_sol,
                    "exit_reason": t.exit_reason,
                    "hold_duration_min": t.hold_duration_min,
                    "confidence": t.confidence,
                    "volume_spike": t.volume_spike,
                })
            with open(self.PNL_FILE, "w") as f:
                json.dump(data, f, indent=2)
            with open(self.DAILY_FILE, "w") as f:
                json.dump(self.daily_pnl, f, indent=2)
        except Exception as e:
            logger.error(f"[PNL] Erreur sauvegarde: {e}")

    def record_trade(self, token_address: str, token_symbol: str, strategy: str,
                     entry_time: str, amount_sol: float, pnl_pct: float,
                     exit_reason: str, confidence: float = 0.0,
                     volume_spike: float = 1.0):
        """Enregistrer un trade terminé"""
        now = datetime.utcnow()
        exit_time = now.isoformat()

        # Calculer la durée
        try:
            entry_dt = datetime.fromisoformat(entry_time)
            hold_duration = (now - entry_dt).total_seconds() / 60
        except:
            hold_duration = 0

        # Estimer le PnL en SOL
        pnl_sol = amount_sol * (pnl_pct / 100)

        # Classifier la raison de sortie
        if "take profit" in exit_reason.lower() or "tp" in exit_reason.lower():
            exit_cat = "take_profit"
        elif "trailing" in exit_reason.lower():
            exit_cat = "trailing_stop"
        elif "stop loss" in exit_reason.lower() or "sl" in exit_reason.lower():
            exit_cat = "stop_loss"
        else:
            exit_cat = "other"

        result = TradeResult(
            token_address=token_address,
            token_symbol=token_symbol,
            strategy=strategy,
            entry_time=entry_time,
            exit_time=exit_time,
            amount_sol=amount_sol,
            pnl_pct=pnl_pct,
            pnl_sol=pnl_sol,
            exit_reason=exit_cat,
            hold_duration_min=hold_duration,
            confidence=confidence,
            volume_spike=volume_spike,
        )
        self.trade_results.append(result)

        # Mettre à jour le PnL quotidien
        today = now.strftime("%Y-%m-%d")
        if today not in self.daily_pnl:
            self.daily_pnl[today] = {"pnl_sol": 0, "pnl_pct_sum": 0, "trades": 0, "wins": 0}
        self.daily_pnl[today]["pnl_sol"] += pnl_sol
        self.daily_pnl[today]["pnl_pct_sum"] += pnl_pct
        self.daily_pnl[today]["trades"] += 1
        if pnl_pct > 0:
            self.daily_pnl[today]["wins"] += 1

        self._save()
        logger.info(f"[PNL] Trade enregistré: {token_symbol} {strategy} "
                   f"PnL: {pnl_pct:+.1f}% ({pnl_sol:+.4f} SOL) "
                   f"durée: {hold_duration:.0f}min")

    def get_strategy_stats(self) -> dict[str, StrategyStats]:
        """Obtenir les stats par stratégie"""
        stats: dict[str, StrategyStats] = {}

        for trade in self.trade_results:
            strat = trade.strategy
            if strat not in stats:
                stats[strat] = StrategyStats(strategy=strat)

            s = stats[strat]
            s.total_trades += 1
            s.total_pnl_pct += trade.pnl_pct
            s.total_pnl_sol += trade.pnl_sol
            s.avg_hold_minutes += trade.hold_duration_min
            s.avg_confidence += trade.confidence

            if trade.pnl_pct > 0:
                s.wins += 1
            else:
                s.losses += 1

            if trade.pnl_pct > s.best_trade_pct:
                s.best_trade_pct = trade.pnl_pct
            if trade.pnl_pct < s.worst_trade_pct:
                s.worst_trade_pct = trade.pnl_pct

        # Finaliser les moyennes
        for s in stats.values():
            if s.total_trades > 0:
                s.avg_hold_minutes /= s.total_trades
                s.avg_confidence /= s.total_trades

        return stats

    def get_exit_reason_stats(self) -> dict[str, dict]:
        """Stats par raison de sortie"""
        reasons = {}
        for trade in self.trade_results:
            r = trade.exit_reason
            if r not in reasons:
                reasons[r] = {"count": 0, "total_pnl": 0, "wins": 0}
            reasons[r]["count"] += 1
            reasons[r]["total_pnl"] += trade.pnl_pct
            if trade.pnl_pct > 0:
                reasons[r]["wins"] += 1
        return reasons

    def get_daily_summary(self, days: int = 7) -> list[dict]:
        """Résumé des derniers N jours"""
        today = datetime.utcnow().date()
        summaries = []
        for i in range(days):
            date = (today - timedelta(days=i)).strftime("%Y-%m-%d")
            if date in self.daily_pnl:
                day_data = self.daily_pnl[date]
                summaries.append({
                    "date": date,
                    "pnl_sol": day_data["pnl_sol"],
                    "trades": day_data["trades"],
                    "wins": day_data["wins"],
                    "win_rate": (day_data["wins"] / max(day_data["trades"], 1)) * 100,
                })
            else:
                summaries.append({
                    "date": date,
                    "pnl_sol": 0,
                    "trades": 0,
                    "wins": 0,
                    "win_rate": 0,
                })
        return summaries

    def purge_strategies(self, strategies_to_remove: list[str]):
        """Supprimer tous les trades d'une ou plusieurs stratégies"""
        before = len(self.trade_results)
        self.trade_results = [
            t for t in self.trade_results
            if t.strategy not in strategies_to_remove
        ]
        removed = before - len(self.trade_results)
        if removed > 0:
            self._save()
            logger.info(f"[PNL] Purgé {removed} trades (stratégies: {strategies_to_remove})")
        return removed

    def get_total_pnl(self) -> dict:
        """PnL total depuis le début"""
        total_sol = sum(t.pnl_sol for t in self.trade_results)
        total_trades = len(self.trade_results)
        wins = sum(1 for t in self.trade_results if t.pnl_pct > 0)

        return {
            "total_pnl_sol": total_sol,
            "total_trades": total_trades,
            "wins": wins,
            "losses": total_trades - wins,
            "win_rate": (wins / max(total_trades, 1)) * 100,
            "avg_pnl_pct": sum(t.pnl_pct for t in self.trade_results) / max(total_trades, 1),
            "best_trade": max((t.pnl_pct for t in self.trade_results), default=0),
            "worst_trade": min((t.pnl_pct for t in self.trade_results), default=0),
        }


class DynamicPositionSizer:
    """
    Position sizing dynamique basé sur:
    1. Score de confiance du signal (Smart Entry)
    2. Win rate de la stratégie
    3. Streak actuelle (réduire après pertes consécutives)
    4. Solde disponible
    """

    def __init__(self, base_size_sol: float = 0.05, min_size_sol: float = 0.02,
                 max_size_sol: float = 0.15):
        self.base_size = base_size_sol
        self.min_size = min_size_sol
        self.max_size = max_size_sol
        self.consecutive_losses = 0
        self.consecutive_wins = 0

    def calculate_size(self, strategy: str, confidence: float,
                       strategy_stats: Optional[StrategyStats] = None,
                       balance_sol: float = 1.0) -> float:
        """
        Calculer la taille de position optimale.
        
        Facteurs:
        - Confiance élevée → taille plus grande
        - Win rate élevé → taille plus grande
        - Pertes consécutives → réduire la taille (protéger le capital)
        - Solde faible → réduire proportionnellement
        """
        size = self.base_size

        # 1. Facteur confiance (0.5x à 1.5x)
        if confidence > 0:
            confidence_multiplier = 0.5 + confidence  # 0.5 à 1.5
            size *= confidence_multiplier

        # 2. Facteur win rate de la stratégie (0.7x à 1.3x)
        if strategy_stats and strategy_stats.total_trades >= 5:
            wr = strategy_stats.win_rate
            if wr >= 60:
                size *= 1.3  # Stratégie qui gagne souvent → plus gros
            elif wr >= 50:
                size *= 1.1
            elif wr < 40:
                size *= 0.7  # Stratégie qui perd → plus petit
            elif wr < 30:
                size *= 0.5  # Très mauvais → minimum

        # 3. Facteur streak (protection anti-tilt)
        if self.consecutive_losses >= 3:
            # Réduire de 20% par perte consécutive au-delà de 3
            loss_factor = max(0.4, 1.0 - (self.consecutive_losses - 2) * 0.2)
            size *= loss_factor
            logger.info(f"[SIZING] Réduction tilt: {self.consecutive_losses} pertes → x{loss_factor:.1f}")
        elif self.consecutive_wins >= 3:
            # Légère augmentation après série gagnante (max +30%)
            win_factor = min(1.3, 1.0 + (self.consecutive_wins - 2) * 0.1)
            size *= win_factor

        # 4. Facteur solde (ne jamais risquer plus de 15% du solde)
        max_risk = balance_sol * 0.15
        if size > max_risk:
            size = max_risk

        # Bornes min/max
        size = max(self.min_size, min(self.max_size, size))

        # Arrondir à 0.01 SOL
        size = round(size, 2)

        logger.info(f"[SIZING] {strategy} conf={confidence:.0%} → {size} SOL "
                   f"(base={self.base_size}, streak: W{self.consecutive_wins}/L{self.consecutive_losses})")
        return size

    def record_result(self, won: bool):
        """Enregistrer le résultat d'un trade pour la streak"""
        if won:
            self.consecutive_wins += 1
            self.consecutive_losses = 0
        else:
            self.consecutive_losses += 1
            self.consecutive_wins = 0

    def get_info(self) -> dict:
        """Info sur le sizing actuel"""
        return {
            "base_size": self.base_size,
            "min_size": self.min_size,
            "max_size": self.max_size,
            "consecutive_wins": self.consecutive_wins,
            "consecutive_losses": self.consecutive_losses,
        }
