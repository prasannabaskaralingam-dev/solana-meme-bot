"""
latency_metrics.py — Métriques de latence pipeline SNIPER
Enregistre les 8 timestamps du pipeline complet (détection → vente confirmée)
et calcule les deltas automatiquement.
"""

import os
import time
import sqlite3
import threading
import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

# ─── Configuration ───────────────────────────────────────────────────────────
_DATA_DIR = os.environ.get("PERSISTENT_DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(_DATA_DIR, "latency_metrics.db")
_db_lock = threading.Lock()


# ─── Init DB ─────────────────────────────────────────────────────────────────
def init_latency_db():
    """Crée la table latency_metrics si elle n'existe pas."""
    with _db_lock:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS latency_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id TEXT UNIQUE,
                token_address TEXT NOT NULL,
                token_symbol TEXT,
                strategy TEXT DEFAULT 'sniper',
                t0_ws_detection REAL,
                t1_queue_entry REAL,
                t2_gates_complete REAL,
                t3_buy_called REAL,
                t4_buy_confirmed REAL,
                t5_sell_signal REAL,
                t6_sell_called REAL,
                t7_sell_confirmed REAL,
                delta_detection_to_buy_ms REAL,
                delta_buy_to_sell_signal_ms REAL,
                delta_sell_signal_to_confirmed_ms REAL,
                delta_total_buy_to_sell_ms REAL,
                sell_source TEXT,
                pnl_pct REAL,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_latency_token
            ON latency_metrics(token_address)
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_latency_created
            ON latency_metrics(created_at DESC)
        """)
        conn.commit()
        conn.close()
    logger.info(f"[LatencyMetrics] DB initialisée: {DB_PATH}")


# ─── Tracker en mémoire (un par trade) ──────────────────────────────────────
@dataclass
class LatencyTracker:
    """Accumule les timestamps d'un trade en cours."""
    token_address: str
    token_symbol: str = ""
    strategy: str = "sniper"
    trade_id: str = ""
    t0_ws_detection: float = 0.0
    t1_queue_entry: float = 0.0
    t2_gates_complete: float = 0.0
    t3_buy_called: float = 0.0
    t4_buy_confirmed: float = 0.0
    t5_sell_signal: float = 0.0
    t6_sell_called: float = 0.0
    t7_sell_confirmed: float = 0.0
    sell_source: str = ""
    pnl_pct: float = 0.0


# ─── Registre global des trackers actifs ─────────────────────────────────────
_active_trackers: dict = {}  # {token_address: LatencyTracker}
_trackers_lock = threading.Lock()


def start_tracking(token_address: str, t0_ws_detection: float, t1_queue_entry: float = 0.0) -> LatencyTracker:
    """Démarre le tracking de latence pour un nouveau token."""
    tracker = LatencyTracker(
        token_address=token_address,
        t0_ws_detection=t0_ws_detection,
        t1_queue_entry=t1_queue_entry or t0_ws_detection,
        trade_id=f"{token_address[:8]}_{int(t0_ws_detection)}",
    )
    with _trackers_lock:
        _active_trackers[token_address] = tracker
    return tracker


def get_tracker(token_address: str) -> Optional[LatencyTracker]:
    """Récupère le tracker actif pour un token."""
    with _trackers_lock:
        return _active_trackers.get(token_address)


def record_gates_complete(token_address: str):
    """Enregistre t2 = passage complet des Gates."""
    tracker = get_tracker(token_address)
    if tracker:
        tracker.t2_gates_complete = time.time()


def record_buy_called(token_address: str):
    """Enregistre t3 = appel execute_buy()."""
    tracker = get_tracker(token_address)
    if tracker:
        tracker.t3_buy_called = time.time()


def record_buy_confirmed(token_address: str, token_symbol: str = "", tx_sig: str = ""):
    """Enregistre t4 = confirmation on-chain de l'achat."""
    tracker = get_tracker(token_address)
    if tracker:
        tracker.t4_buy_confirmed = time.time()
        if token_symbol:
            tracker.token_symbol = token_symbol
        if tx_sig:
            tracker.trade_id = f"{token_address[:8]}_{tx_sig[:8]}"


def record_sell_signal(token_address: str, source: str = ""):
    """Enregistre t5 = détection du signal de vente (SL/TP/etc)."""
    tracker = get_tracker(token_address)
    if tracker:
        tracker.t5_sell_signal = time.time()
        if source:
            tracker.sell_source = source


def record_sell_called(token_address: str):
    """Enregistre t6 = appel execute_sell()."""
    tracker = get_tracker(token_address)
    if tracker:
        tracker.t6_sell_called = time.time()


def record_sell_confirmed(token_address: str, pnl_pct: float = 0.0):
    """Enregistre t7 = confirmation on-chain de la vente, puis persiste en DB."""
    tracker = get_tracker(token_address)
    if tracker:
        tracker.t7_sell_confirmed = time.time()
        tracker.pnl_pct = pnl_pct
        # Persister en DB dans un thread séparé pour ne pas bloquer
        _persist_tracker(tracker)
        # Nettoyer le tracker actif
        with _trackers_lock:
            _active_trackers.pop(token_address, None)


def _persist_tracker(tracker: LatencyTracker):
    """Calcule les deltas et persiste le tracker en DB."""
    # Calcul des deltas (en ms)
    delta_detection_to_buy = None
    delta_buy_to_sell_signal = None
    delta_sell_signal_to_confirmed = None
    delta_total_buy_to_sell = None

    if tracker.t4_buy_confirmed > 0 and tracker.t0_ws_detection > 0:
        delta_detection_to_buy = (tracker.t4_buy_confirmed - tracker.t0_ws_detection) * 1000

    if tracker.t5_sell_signal > 0 and tracker.t4_buy_confirmed > 0:
        delta_buy_to_sell_signal = (tracker.t5_sell_signal - tracker.t4_buy_confirmed) * 1000

    if tracker.t7_sell_confirmed > 0 and tracker.t5_sell_signal > 0:
        delta_sell_signal_to_confirmed = (tracker.t7_sell_confirmed - tracker.t5_sell_signal) * 1000

    if tracker.t7_sell_confirmed > 0 and tracker.t4_buy_confirmed > 0:
        delta_total_buy_to_sell = (tracker.t7_sell_confirmed - tracker.t4_buy_confirmed) * 1000

    try:
        with _db_lock:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("""
                INSERT OR REPLACE INTO latency_metrics (
                    trade_id, token_address, token_symbol, strategy,
                    t0_ws_detection, t1_queue_entry, t2_gates_complete,
                    t3_buy_called, t4_buy_confirmed,
                    t5_sell_signal, t6_sell_called, t7_sell_confirmed,
                    delta_detection_to_buy_ms, delta_buy_to_sell_signal_ms,
                    delta_sell_signal_to_confirmed_ms, delta_total_buy_to_sell_ms,
                    sell_source, pnl_pct
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                tracker.trade_id,
                tracker.token_address,
                tracker.token_symbol,
                tracker.strategy,
                tracker.t0_ws_detection,
                tracker.t1_queue_entry,
                tracker.t2_gates_complete,
                tracker.t3_buy_called,
                tracker.t4_buy_confirmed,
                tracker.t5_sell_signal,
                tracker.t6_sell_called,
                tracker.t7_sell_confirmed,
                delta_detection_to_buy,
                delta_buy_to_sell_signal,
                delta_sell_signal_to_confirmed,
                delta_total_buy_to_sell,
                tracker.sell_source,
                tracker.pnl_pct,
            ))
            conn.commit()
            conn.close()
        logger.info(
            f"[LatencyMetrics] ✅ Trade {tracker.token_symbol} persisté | "
            f"détection→achat={delta_detection_to_buy:.0f}ms | "
            f"achat→signal_vente={delta_buy_to_sell_signal:.0f}ms | "
            f"signal→confirmé={delta_sell_signal_to_confirmed:.0f}ms | "
            f"total={delta_total_buy_to_sell:.0f}ms"
            if all(x is not None for x in [delta_detection_to_buy, delta_buy_to_sell_signal,
                                            delta_sell_signal_to_confirmed, delta_total_buy_to_sell])
            else f"[LatencyMetrics] ✅ Trade {tracker.token_symbol} persisté (données partielles)"
        )
    except Exception as e:
        logger.error(f"[LatencyMetrics] Erreur persistance: {e}")


# ─── Requêtes pour /latency ──────────────────────────────────────────────────
def get_latency_stats(limit: int = 20) -> dict:
    """Retourne les statistiques agrégées sur les N derniers trades."""
    try:
        with _db_lock:
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            c.execute("""
                SELECT * FROM latency_metrics
                ORDER BY created_at DESC
                LIMIT ?
            """, (limit,))
            rows = [dict(r) for r in c.fetchall()]
            conn.close()

        if not rows:
            return {"count": 0, "trades": [], "stats": {}}

        # Calcul des stats agrégées par maillon
        stats = {}
        delta_fields = [
            ("delta_detection_to_buy_ms", "Détection → Achat"),
            ("delta_buy_to_sell_signal_ms", "Achat → Signal vente"),
            ("delta_sell_signal_to_confirmed_ms", "Signal → Vente confirmée"),
            ("delta_total_buy_to_sell_ms", "Total achat → vente"),
        ]
        for field_name, label in delta_fields:
            values = [r[field_name] for r in rows if r[field_name] is not None and r[field_name] > 0]
            if values:
                stats[label] = {
                    "avg_ms": sum(values) / len(values),
                    "min_ms": min(values),
                    "max_ms": max(values),
                    "count": len(values),
                }

        return {
            "count": len(rows),
            "trades": rows[:5],  # 5 derniers pour le détail
            "stats": stats,
        }
    except Exception as e:
        logger.error(f"[LatencyMetrics] Erreur requête stats: {e}")
        return {"count": 0, "trades": [], "stats": {}, "error": str(e)}


def get_active_trackers_count() -> int:
    """Nombre de trackers actifs en mémoire."""
    with _trackers_lock:
        return len(_active_trackers)


def format_latency_telegram(stats: dict) -> str:
    """Formate les stats de latence pour Telegram."""
    if stats["count"] == 0:
        return "📊 *Latence Pipeline*\n\nAucun trade enregistré."

    msg = f"📊 *Latence Pipeline* ({stats['count']} trades)\n\n"

    for label, data in stats.get("stats", {}).items():
        avg = data["avg_ms"]
        # Formater en secondes si > 1000ms
        if avg > 60000:
            msg += f"*{label}*\n"
            msg += f"  Moy: {avg/1000:.1f}s | Min: {data['min_ms']/1000:.1f}s | Max: {data['max_ms']/1000:.1f}s\n\n"
        elif avg > 1000:
            msg += f"*{label}*\n"
            msg += f"  Moy: {avg/1000:.2f}s | Min: {data['min_ms']/1000:.2f}s | Max: {data['max_ms']/1000:.2f}s\n\n"
        else:
            msg += f"*{label}*\n"
            msg += f"  Moy: {avg:.0f}ms | Min: {data['min_ms']:.0f}ms | Max: {data['max_ms']:.0f}ms\n\n"

    # Dernier trade détaillé
    if stats.get("trades"):
        last = stats["trades"][0]
        msg += f"─── Dernier trade ───\n"
        msg += f"🪙 {last.get('token_symbol', '?')}\n"
        if last.get("delta_detection_to_buy_ms"):
            msg += f"⚡ Détection→Achat: {last['delta_detection_to_buy_ms']:.0f}ms\n"
        if last.get("delta_buy_to_sell_signal_ms"):
            val = last['delta_buy_to_sell_signal_ms']
            msg += f"📡 Achat→Signal: {val/1000:.1f}s\n" if val > 1000 else f"📡 Achat→Signal: {val:.0f}ms\n"
        if last.get("delta_sell_signal_to_confirmed_ms"):
            msg += f"💨 Signal→Vente: {last['delta_sell_signal_to_confirmed_ms']:.0f}ms\n"
        if last.get("sell_source"):
            msg += f"🎯 Source: {last['sell_source']}\n"
        if last.get("pnl_pct"):
            msg += f"📈 PnL: {last['pnl_pct']:+.1f}%\n"

    msg += f"\n🔄 Trackers actifs: {get_active_trackers_count()}"
    return msg
