"""
Configuration du Bot Telegram - Solana Meme Coin Tracker
"""
import os

# ============================================================
# TELEGRAM - Token via variable d'environnement (sécurisé)
# Sur Render : configuré dans Environment Variables
# En local : remplacer directement ou exporter la variable
# ============================================================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "VOTRE_TOKEN_ICI")

# ============================================================
# API ENDPOINTS (Gratuits - Aucune clé requise)
# ============================================================
DEXSCREENER_BASE_URL = "https://api.dexscreener.com"

# Endpoints DexScreener
ENDPOINTS = {
    "token_profiles_latest": "/token-profiles/latest/v1",
    "token_boosts_latest": "/token-boosts/latest/v1",
    "token_boosts_top": "/token-boosts/top/v1",
    "trending_metas": "/metas/trending/v1",
    "search": "/latest/dex/search",
    "pairs": "/latest/dex/pairs/solana",
    "tokens": "/tokens/v1/solana",
}

# ============================================================
# PARAMÈTRES DU BOT
# ============================================================

# Intervalle de polling en secondes (60 = 1 requête/min, safe)
POLLING_INTERVAL = 45

# Filtres pour les meme coins
FILTERS = {
    "min_liquidity_usd": 1000,       # Liquidité minimum en USD
    "min_volume_24h": 500,           # Volume 24h minimum
    "min_market_cap": 1000,          # Market cap minimum
    "max_market_cap": 10_000_000,    # Market cap maximum (filtre les gros)
    "min_price_change_5m": 10,       # % hausse min sur 5 min pour alerte pump
    "min_price_change_1h": 30,       # % hausse min sur 1h pour alerte pump
    "max_token_age_hours": 24,       # Âge max du token (nouveaux tokens)
    "min_buys_5m": 5,                # Nombre min d'achats sur 5 min
}

# Nombre max d'alertes par cycle pour éviter le spam
MAX_ALERTS_PER_CYCLE = 5

# Chain ID pour Solana
CHAIN_ID = "solana"
