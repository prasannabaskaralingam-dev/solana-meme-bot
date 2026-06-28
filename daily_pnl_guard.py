"""
Daily PnL Guard — Circuit Breaker Global.

Pause automatique du trading si:
  - Perte journalière > -0.05 SOL
  - 3 Stop Loss consécutifs

Usage:
    guard = DailyPnLGuard()
    guard.record_loss(pnl_sol=-0.01)
    if guard.is_paused():
        # Ne pas trader
"""

import time
import logging
from dataclasses import dataclass, field
from typing import List

logger = logging.getLogger(__name__)


@dataclass
class DailyPnLGuardConfig:
    """Configuration du Daily PnL Guard"""
    max_daily_loss_sol: float = -0.15       # Pause si perte > 0.15 SOL/jour (3 trades)
    max_consecutive_sl: int = 3             # Pause après 3 SL consécutifs
    pause_duration_minutes: float = 60.0    # Durée de la pause (1h)
    reset_hour_utc: int = 0                 # Reset à minuit UTC


class DailyPnLGuard:
    """
    Circuit Breaker Global — protège contre les mauvaises journées.
    """

    def __init__(self, config: DailyPnLGuardConfig = None):
        self.config = config or DailyPnLGuardConfig()
        self._daily_pnl_sol: float = 0.0
        self._consecutive_sl: int = 0
        self._trades_today: List[dict] = []
        self._paused_until: float = 0.0
        self._pause_reason: str = ""
        self._last_reset_day: int = 0
        self._total_pauses: int = 0

        logger.info(f"[DailyPnLGuard] Initialisé: max_loss={self.config.max_daily_loss_sol} SOL, "
                    f"max_consecutive_sl={self.config.max_consecutive_sl}, "
                    f"pause={self.config.pause_duration_minutes}min")

    def _check_daily_reset(self):
        """Reset les compteurs à minuit UTC"""
        import datetime
        now = datetime.datetime.utcnow()
        today = now.toordinal()
        if today != self._last_reset_day:
            self._last_reset_day = today
            self._daily_pnl_sol = 0.0
            self._consecutive_sl = 0
            self._trades_today = []
            logger.info("[DailyPnLGuard] Reset journalier effectué")

    def record_trade(self, pnl_sol: float, is_stop_loss: bool = False):
        """
        Enregistrer un trade terminé.
        
        Args:
            pnl_sol: PnL en SOL (négatif = perte)
            is_stop_loss: True si la sortie était un SL
        """
        self._check_daily_reset()

        self._daily_pnl_sol += pnl_sol
        self._trades_today.append({
            "pnl_sol": pnl_sol,
            "is_sl": is_stop_loss,
            "time": time.time(),
        })

        # Compteur SL consécutifs
        if is_stop_loss:
            self._consecutive_sl += 1
        else:
            self._consecutive_sl = 0

        # Vérifier si on doit pauser
        if self._daily_pnl_sol <= self.config.max_daily_loss_sol:
            self._pause(f"Perte journalière {self._daily_pnl_sol:.4f} SOL "
                       f"≤ {self.config.max_daily_loss_sol} SOL")

        elif self._consecutive_sl >= self.config.max_consecutive_sl:
            self._pause(f"{self._consecutive_sl} SL consécutifs")

    def _pause(self, reason: str):
        """Activer la pause"""
        self._paused_until = time.time() + (self.config.pause_duration_minutes * 60)
        self._pause_reason = reason
        self._total_pauses += 1
        logger.warning(f"\n{'='*50}\n⛔ DAILY PnL GUARD — PAUSE ACTIVÉE\n"
                      f"Raison: {reason}\n"
                      f"Durée: {self.config.pause_duration_minutes} min\n{'='*50}")

    def is_paused(self) -> bool:
        """Vérifier si le trading est en pause"""
        self._check_daily_reset()
        if time.time() < self._paused_until:
            return True
        return False

    def get_pause_reason(self) -> str:
        """Raison de la pause actuelle"""
        if self.is_paused():
            remaining = (self._paused_until - time.time()) / 60
            return f"⛔ {self._pause_reason} (reprise dans {remaining:.0f}min)"
        return ""

    def get_stats(self) -> dict:
        """Stats du guard"""
        self._check_daily_reset()
        return {
            "daily_pnl_sol": self._daily_pnl_sol,
            "trades_today": len(self._trades_today),
            "consecutive_sl": self._consecutive_sl,
            "is_paused": self.is_paused(),
            "pause_reason": self.get_pause_reason(),
            "total_pauses": self._total_pauses,
            "max_daily_loss": self.config.max_daily_loss_sol,
            "max_consecutive_sl": self.config.max_consecutive_sl,
        }

    def force_unpause(self):
        """Forcer la reprise (commande admin)"""
        self._paused_until = 0
        self._pause_reason = ""
        self._consecutive_sl = 0
        logger.info("[DailyPnLGuard] Pause forcée levée par admin")
