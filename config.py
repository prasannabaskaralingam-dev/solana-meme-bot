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

# Filtres pour les meme coins (optimisés anti-rug)
FILTERS = {
    "min_liquidity_usd": 5000,       # Liquidité minimum en USD (élevé = moins de scams)
    "min_volume_24h": 5000,          # Volume 24h minimum (filtre les tokens morts)
    "min_market_cap": 10_000,        # Market cap minimum ($10k = token réel)
    "max_market_cap": 5_000_000,     # Market cap maximum (on veut les early)
    "min_price_change_5m": 5,        # % hausse min sur 5 min (détection précoce)
    "min_price_change_1h": 20,       # % hausse min sur 1h pour momentum
    "max_token_age_hours": 12,       # Âge max du token (plus frais = mieux)
    "min_buys_5m": 10,               # Nombre min d'achats sur 5 min (activité réelle)
}

# Nombre max d'alertes par cycle pour éviter le spam
MAX_ALERTS_PER_CYCLE = 5

# Chain ID pour Solana
CHAIN_ID = "solana"
