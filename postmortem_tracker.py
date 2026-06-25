import threading
import time
import sqlite3
import requests
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

DB_PATH = "postmortem.db"

# ─── FIX MÉMOIRE — Limite globale : max 5 threads post-mortem simultanés ───
# Évite l'accumulation de threads en RAM
MAX_POSTMORTEM_THREADS = 5
_postmortem_semaphore = threading.Semaphore(MAX_POSTMORTEM_THREADS)
# ────────────────────────────────────────────────────────────────────────────


def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS postmortem (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_address TEXT,
            token_symbol TEXT,
            strategy TEXT,
            reason_exit TEXT,
            pnl_at_exit REAL,
            price_at_exit REAL,
            max_price_after REAL,
            max_pnl_after REAL,
            missed_gain REAL,
            tracking_duration_min INTEGER,
            exit_time TEXT,
            created_at TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS postmortem_ticks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_address TEXT,
            exit_time TEXT,
            elapsed_sec INTEGER,
            price REAL,
            pnl_vs_entry REAL,
            recorded_at TEXT
        )
    """)
    conn.commit()
    conn.close()


def _get_price(token_address: str, helius_api_key: str) -> float:
    """Récupère le prix via DexScreener (fallback fiable)."""
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
        r = requests.get(url, timeout=5)
        pairs = r.json().get("pairs", [])
        if pairs:
            return float(pairs[0].get("priceUsd", 0) or 0)
        return 0.0
    except Exception:
        return 0.0


def track_postmortem(
    trade_record: dict,
    entry_price_usd: float,
    helius_api_key: str,
    telegram_bot_token: str,
    telegram_chat_id: str,
    duration_min: int = 30,
    interval_sec: int = 30
):
    """
    Tracker post-mortem — surveille le prix 30 min après la vente.
    VERSION AVEC FIX MÉMOIRE : ticks limités, cleanup automatique.
    """
    token_address = trade_record.get("token_address", "")
    token_symbol = trade_record.get("token", "???")
    exit_time = trade_record.get("timestamp", "")
    pnl_at_exit = trade_record.get("pnl_pct", 0.0)

    if entry_price_usd <= 0:
        return

    price_at_exit = entry_price_usd * (1 + pnl_at_exit / 100)
    max_price = price_at_exit
    start = time.time()
    total_sec = duration_min * 60

    # ─── FIX MÉMOIRE — Limite ticks en mémoire ───────────────
    MAX_TICKS_IN_MEMORY = 100
    ticks_count = 0
    # ──────────────────────────────────────────────────────────

    logger.info(f"[Postmortem] Start {token_symbol} for {duration_min}min")

    while time.time() - start < total_sec:
        time.sleep(interval_sec)
        elapsed = int(time.time() - start)

        # Récupérer le prix
        current_price = _get_price(token_address, helius_api_key)
        if current_price <= 0:
            continue

        if current_price > max_price:
            max_price = current_price

        pnl_vs_entry = (
            (current_price - entry_price_usd) / entry_price_usd * 100
            if entry_price_usd > 0 else 0
        )

        # ─── FIX MÉMOIRE — Écrire en DB, pas en mémoire ──────
        # On écrit directement en SQLite sans accumuler en RAM
        if ticks_count < MAX_TICKS_IN_MEMORY:
            try:
                conn = sqlite3.connect(DB_PATH)
                conn.execute("""
                    INSERT INTO postmortem_ticks
                    (token_address, exit_time, elapsed_sec,
                     price, pnl_vs_entry, recorded_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    token_address, exit_time, elapsed,
                    current_price, pnl_vs_entry,
                    datetime.utcnow().isoformat()
                ))
                conn.commit()
                conn.close()
                ticks_count += 1
            except Exception as e:
                logger.error(f"[Postmortem] DB error: {e}")
        # ──────────────────────────────────────────────────────

    # Calcul final
    max_pnl_after = (
        (max_price - entry_price_usd) / entry_price_usd * 100
        if entry_price_usd > 0 else 0
    )
    missed_gain = max_pnl_after - pnl_at_exit

    # Sauvegarder le résumé
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""
            INSERT INTO postmortem
            (token_address, token_symbol, strategy, reason_exit,
             pnl_at_exit, price_at_exit, max_price_after,
             max_pnl_after, missed_gain, tracking_duration_min,
             exit_time, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            token_address, token_symbol,
            trade_record.get("strategy", "?"),
            trade_record.get("reason", "?"),
            pnl_at_exit, price_at_exit,
            max_price, max_pnl_after,
            missed_gain, duration_min,
            exit_time, datetime.utcnow().isoformat()
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"[Postmortem] DB summary error: {e}")

    # Rapport Telegram
    emoji = "✅" if missed_gain < 10 else (
            "⚠️" if missed_gain < 30 else "🔥")

    msg = (
        f"📊 *Post-Mortem — {token_symbol}*\n\n"
        f"🚪 Sorti à       : `{pnl_at_exit:+.1f}%`\n"
        f"🚀 Max après     : `{max_pnl_after:+.1f}%`\n"
        f"{emoji} Gain manqué : `{missed_gain:+.1f}%`\n\n"
        f"📋 Raison sortie : {trade_record.get('reason', '?')}\n"
        f"⏱ Tracking      : {duration_min} min\n"
        f"🏷 Token         : `{token_address[:8]}...`"
    )

    try:
        requests.post(
            f"https://api.telegram.org/bot{telegram_bot_token}/sendMessage",
            json={
                "chat_id": telegram_chat_id,
                "text": msg,
                "parse_mode": "Markdown"
            },
            timeout=5
        )
    except Exception as e:
        logger.error(f"[Postmortem] Telegram error: {e}")

    logger.info(
        f"[Postmortem] Done {token_symbol} | "
        f"Manqué: {missed_gain:+.1f}%"
    )


def _track_postmortem_safe(
    trade_record: dict,
    entry_price_usd: float,
    helius_api_key: str,
    telegram_bot_token: str,
    telegram_chat_id: str
):
    """
    Wrapper avec sémaphore pour limiter les threads simultanés.
    Si 5 threads tournent déjà, attend qu'un se libère.
    """
    with _postmortem_semaphore:
        track_postmortem(
            trade_record=trade_record,
            entry_price_usd=entry_price_usd,
            helius_api_key=helius_api_key,
            telegram_bot_token=telegram_bot_token,
            telegram_chat_id=telegram_chat_id
        )


def start_postmortem_thread(
    trade_record: dict,
    entry_price_usd: float,
    helius_api_key: str,
    telegram_bot_token: str,
    telegram_chat_id: str
):
    """
    Lance le tracker dans un thread séparé — non bloquant.
    VERSION AVEC FIX MÉMOIRE : sémaphore limite à 5 threads max.
    """
    t = threading.Thread(
        target=_track_postmortem_safe,
        args=(
            trade_record,
            entry_price_usd,
            helius_api_key,
            telegram_bot_token,
            telegram_chat_id
        ),
        daemon=True
    )
    t.start()
