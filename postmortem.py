import sqlite3
import os
from datetime import datetime

DB_PATH = "/data/solana-bot/postmortem.db"

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            token_address TEXT,
            token_symbol TEXT,
            strategy TEXT,
            entry_price REAL,
            exit_price REAL,
            pnl_pct REAL,
            duration_min INTEGER,
            exit_reason TEXT,
            market_cap_entry REAL,
            sol_amount REAL,
            dry_run INTEGER DEFAULT 0
        )
    ''')
    # Migration: ajouter colonne dry_run si elle n'existe pas
    try:
        c.execute("ALTER TABLE trades ADD COLUMN dry_run INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass  # Colonne existe déjà
    conn.commit()
    conn.close()

def record_trade(
    token_address,
    token_symbol,
    strategy,
    entry_price,
    exit_price,
    pnl_pct,
    duration_min,
    exit_reason,
    market_cap_entry=None,
    sol_amount=None,
    dry_run=False
):
    init_db()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        INSERT INTO trades (
            timestamp, token_address, token_symbol,
            strategy, entry_price, exit_price,
            pnl_pct, duration_min, exit_reason,
            market_cap_entry, sol_amount, dry_run
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    ''', (
        datetime.utcnow().isoformat(),
        token_address,
        token_symbol,
        strategy,
        entry_price,
        exit_price,
        pnl_pct,
        duration_min,
        exit_reason,
        market_cap_entry,
        sol_amount,
        1 if dry_run else 0
    ))
    conn.commit()
    conn.close()

def get_today_trades():
    init_db()
    today = datetime.utcnow().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        SELECT token_symbol, strategy, pnl_pct,
               duration_min, exit_reason, dry_run
        FROM trades
        WHERE timestamp LIKE ?
        ORDER BY timestamp DESC
    ''', (f"{today}%",))
    rows = c.fetchall()
    conn.close()
    return rows

def get_stats():
    init_db()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        SELECT
            COUNT(*) as total,
            ROUND(AVG(pnl_pct), 2) as avg_pnl,
            ROUND(SUM(CASE WHEN pnl_pct > 0
                  THEN 1 ELSE 0 END) * 100.0
                  / COUNT(*), 1) as winrate,
            exit_reason,
            COUNT(*) as count
        FROM trades
        GROUP BY exit_reason
    ''')
    rows = c.fetchall()
    conn.close()
    return rows
