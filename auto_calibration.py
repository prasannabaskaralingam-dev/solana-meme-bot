"""
AUTO-CALIBRATION DES PARAMÈTRES
================================
Analyse les post-mortems SQLite et ajuste automatiquement
les paramètres TP/SL/Trailing/TimeStop dans state.json.

Lancé quotidiennement à 21h UTC (1h après le résumé quotidien).
"""

import sqlite3
import json
import os
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# ─── Limites de sécurité pour l'auto-calibration ─────────────
LIMITS = {
    "tp_pct": {
        "min": 15.0,
        "max": 60.0,
        "default": 20.0
    },
    "time_stop_sniper": {
        "min": 8,
        "max": 30,
        "default": 20
    },
    "time_stop_recovered": {
        "min": 15,
        "max": 60,
        "default": 30
    },
    "trailing_activation": {
        "min": 10.0,
        "max": 40.0,
        "default": 20.0
    },
    "trailing_sl": {
        "min": 8.0,
        "max": 25.0,
        "default": 15.0
    }
}

MIN_TRADES_FOR_CALIBRATION = 20  # Minimum de trades pour calibrer


def clamp(value, min_val, max_val):
    """Limite une valeur entre min et max."""
    return max(min_val, min(max_val, value))


def analyze_postmortems(data_dir: str) -> dict:
    """
    Analyse les post-mortems SQLite des 7 derniers jours
    et retourne les paramètres optimaux calculés.
    """
    db_path = os.path.join(data_dir, "postmortem.db")

    try:
        if not os.path.exists(db_path):
            return {"status": "no_database", "trades_analyzed": 0}

        conn = sqlite3.connect(db_path)
        c = conn.cursor()

        # Vérifier si la table existe
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='postmortem'")
        if not c.fetchone():
            conn.close()
            return {"status": "no_table", "trades_analyzed": 0}

        # Tous les post-mortems des 7 derniers jours
        c.execute("""
            SELECT
                reason_exit,
                pnl_at_exit,
                max_pnl_after,
                missed_gain,
                tracking_duration_min,
                strategy
            FROM postmortem
            WHERE created_at >= datetime('now', '-7 days')
        """)
        rows = c.fetchall()
        conn.close()

        if len(rows) < MIN_TRADES_FOR_CALIBRATION:
            return {
                "status": "insufficient_data",
                "trades_analyzed": len(rows),
                "min_required": MIN_TRADES_FOR_CALIBRATION
            }

        # ─── Analyse par raison de sortie ────────────────────
        take_profit_trades = [r for r in rows if "take_profit" in (r[0] or "").lower() or "tp" in (r[0] or "").lower()]
        trailing_trades = [r for r in rows if "trailing" in (r[0] or "").lower()]
        time_stop_trades = [r for r in rows if "time_stop" in (r[0] or "").lower() or "time" in (r[0] or "").lower()]

        results = {
            "status": "ok",
            "trades_analyzed": len(rows),
            "adjustments": {}
        }

        # ─── TP : si gain manqué moyen > 25% → augmenter TP ─
        if take_profit_trades:
            missed_values = [r[3] for r in take_profit_trades if r[3] is not None]
            exit_values = [r[1] for r in take_profit_trades if r[1] is not None]

            if missed_values:
                avg_missed = sum(missed_values) / len(missed_values)
                avg_exit = sum(exit_values) / len(exit_values) if exit_values else 20

                if avg_missed > 25:
                    new_tp = clamp(
                        avg_exit + (avg_missed * 0.3),
                        LIMITS["tp_pct"]["min"],
                        LIMITS["tp_pct"]["max"]
                    )
                    results["adjustments"]["tp_pct"] = {
                        "reason": f"Gain manqué moy +{avg_missed:.1f}% → TP trop tôt",
                        "new_value": round(new_tp, 1),
                        "direction": "↑ augmenté"
                    }
                elif avg_missed < 5 and avg_exit > 15:
                    results["adjustments"]["tp_pct"] = {
                        "reason": f"Gain manqué faible +{avg_missed:.1f}% → TP optimal",
                        "new_value": None,
                        "direction": "✅ inchangé"
                    }

        # ─── Time Stop : si trades Time Stop ont missed > 20% ─
        if time_stop_trades:
            missed_ts = [r[3] for r in time_stop_trades if r[3] is not None]
            if missed_ts:
                avg_missed_ts = sum(missed_ts) / len(missed_ts)

                if avg_missed_ts > 20:
                    results["adjustments"]["time_stop_sniper"] = {
                        "reason": f"Gain manqué après TS moy +{avg_missed_ts:.1f}%",
                        "new_value": clamp(22, LIMITS["time_stop_sniper"]["min"], LIMITS["time_stop_sniper"]["max"]),
                        "direction": "↑ augmenté"
                    }
                elif avg_missed_ts < 3:
                    results["adjustments"]["time_stop_sniper"] = {
                        "reason": "Time Stop bien calibré",
                        "new_value": None,
                        "direction": "✅ inchangé"
                    }

        # ─── Trailing : si missed > 30% après trailing ───────
        if trailing_trades:
            missed_trail = [r[3] for r in trailing_trades if r[3] is not None]
            if missed_trail:
                avg_missed_trail = sum(missed_trail) / len(missed_trail)

                if avg_missed_trail > 30:
                    results["adjustments"]["trailing_sl"] = {
                        "reason": f"Gain manqué après trailing +{avg_missed_trail:.1f}%",
                        "new_value": clamp(
                            18.0,
                            LIMITS["trailing_sl"]["min"],
                            LIMITS["trailing_sl"]["max"]
                        ),
                        "direction": "↑ élargi"
                    }

        return results

    except Exception as e:
        logger.error(f"[AutoCalib] Erreur analyse: {e}")
        return {"status": "error", "message": str(e)}


def apply_adjustments(adjustments: dict, state: dict) -> dict:
    """
    Applique les nouveaux paramètres dans le state dict.
    Retourne les changements effectués.
    """
    changes = {}

    for param, data in adjustments.items():
        new_value = data.get("new_value")
        if new_value is None:
            continue

        old_value = state.get(param)
        state[param] = new_value
        changes[param] = {
            "old": old_value,
            "new": new_value,
            "direction": data.get("direction", ""),
            "reason": data.get("reason", "")
        }

    if changes:
        logger.info(f"[AutoCalib] {len(changes)} paramètres ajustés")

    return changes
