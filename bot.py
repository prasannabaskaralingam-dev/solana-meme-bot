"""
Bot Telegram - Solana Meme Coin Tracker
Détecte les nouveaux meme coins, pumps et opportunités sur Solana.
100% gratuit - Utilise l'API DexScreener (sans clé API).
"""

import asyncio
import logging
import time
import json
import os
from datetime import datetime, timezone

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)
from telegram.constants import ParseMode

from config import TELEGRAM_BOT_TOKEN, POLLING_INTERVAL, MAX_ALERTS_PER_CYCLE, FILTERS
from dexscreener_api import DexScreenerAPI

# Configuration du logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# Stockage des données
DATA_FILE = "bot_data.json"


def load_data() -> dict:
    """Charger les données persistantes"""
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            return json.load(f)
    return {"subscribers": [], "seen_tokens": [], "watchlist": {}}


def save_data(data: dict):
    """Sauvegarder les données"""
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)


# Initialisation
api = DexScreenerAPI()
bot_data = load_data()


# ============================================================
# COMMANDES DU BOT
# ============================================================


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /start - Inscription et bienvenue"""
    chat_id = update.effective_chat.id

    if chat_id not in bot_data["subscribers"]:
        bot_data["subscribers"].append(chat_id)
        save_data(bot_data)

    welcome_msg = """
🚀 *Solana Meme Coin Tracker* 🚀

Bienvenue ! Ce bot surveille les meme coins sur Solana en temps réel.

*Fonctionnalités :*
• 🆕 Détection de nouveaux tokens
• 📈 Alertes pump (hausse rapide)
• 🔥 Tokens trending / boostés
• 📊 Analyse détaillée de tokens
• 🎯 Narratives trending

*Commandes :*
/start - Démarrer et s'abonner
/stop - Se désabonner des alertes
/scan - Scanner maintenant
/trending - Tokens trending
/metas - Narratives populaires
/search `<nom>` - Rechercher un token
/analyze `<adresse>` - Analyser un token
/watchlist - Voir votre watchlist
/add `<adresse>` - Ajouter à la watchlist
/remove `<adresse>` - Retirer de la watchlist
/filters - Voir les filtres actifs
/help - Aide

_Les alertes automatiques sont envoyées toutes les ~45 secondes._
"""
    await update.message.reply_text(welcome_msg, parse_mode=ParseMode.MARKDOWN)


async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /stop - Désabonnement"""
    chat_id = update.effective_chat.id
    if chat_id in bot_data["subscribers"]:
        bot_data["subscribers"].remove(chat_id)
        save_data(bot_data)
        await update.message.reply_text("❌ Vous êtes désabonné des alertes automatiques.")
    else:
        await update.message.reply_text("Vous n'étiez pas abonné.")


async def scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /scan - Scanner manuellement"""
    await update.message.reply_text("🔍 Scan en cours...")

    results = await scan_for_gems()

    if results:
        for msg in results[:5]:
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
    else:
        await update.message.reply_text("😴 Aucun nouveau gem détecté pour le moment. Réessayez plus tard.")


async def trending_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /trending - Tokens boostés/trending"""
    await update.message.reply_text("🔥 Récupération des tokens trending...")

    boosts = api.get_boosted_tokens()

    if not boosts:
        await update.message.reply_text("Aucun token trending trouvé.")
        return

    msg = "🔥 *Top Tokens Boostés (Solana)* 🔥\n\n"
    for i, token in enumerate(boosts[:10], 1):
        addr = token.get("tokenAddress", "")[:8]
        url = token.get("url", "")
        description = (token.get("description") or "")[:50]
        amount = token.get("amount", token.get("totalAmount", 0))
        msg += f"{i}. `{token.get('tokenAddress', '')[:12]}...`\n"
        if description:
            msg += f"   📝 {description}\n"
        if amount:
            msg += f"   🚀 Boost: {amount}\n"
        if url:
            msg += f"   🔗 [Voir]({url})\n"
        msg += "\n"

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)


async def metas_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /metas - Narratives trending"""
    await update.message.reply_text("🎯 Récupération des narratives...")

    metas = api.get_trending_metas()

    if not metas:
        await update.message.reply_text("Aucune narrative trending trouvée.")
        return

    msg = "🎯 *Narratives Trending* 🎯\n\n"
    for i, meta in enumerate(metas[:10], 1):
        name = meta.get("name", "Unknown")
        mc = meta.get("marketCap", 0)
        volume = meta.get("volume", 0)
        token_count = meta.get("tokenCount", 0)
        mc_change = (meta.get("marketCapChange") or {}).get("h1", 0)

        msg += f"{i}. *{name}*\n"
        msg += f"   💰 MC: ${mc:,.0f} | Vol: ${volume:,.0f}\n"
        msg += f"   📊 {token_count} tokens | MC 1h: {mc_change:+.1f}%\n\n"

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /search <query> - Rechercher un token"""
    if not context.args:
        await update.message.reply_text("Usage: /search <nom ou symbole>\nExemple: /search BONK")
        return

    query = " ".join(context.args)
    await update.message.reply_text(f"🔍 Recherche de '{query}'...")

    pairs = api.search_pairs(query)

    if not pairs:
        await update.message.reply_text(f"Aucun résultat pour '{query}' sur Solana.")
        return

    msg = f"🔍 *Résultats pour '{query}'* (Solana)\n\n"
    for i, pair in enumerate(pairs[:5], 1):
        base = pair.get("baseToken", {})
        name = base.get("name", "Unknown")
        symbol = base.get("symbol", "???")
        price = pair.get("priceUsd", "N/A")
        liq = (pair.get("liquidity") or {}).get("usd", 0)
        mc = pair.get("marketCap") or pair.get("fdv", 0)
        change_1h = (pair.get("priceChange") or {}).get("h1", 0)
        change_24h = (pair.get("priceChange") or {}).get("h24", 0)
        url = pair.get("url", "")

        msg += f"{i}. *{name}* (${symbol})\n"
        msg += f"   💵 Prix: ${price}\n"
        msg += f"   💧 Liquidité: ${liq:,.0f}\n"
        msg += f"   📊 MC: ${mc:,.0f}\n" if mc else ""
        msg += f"   📈 1h: {change_1h:+.1f}% | 24h: {change_24h:+.1f}%\n"
        if url:
            msg += f"   🔗 [DexScreener]({url})\n"
        msg += "\n"

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)


async def analyze_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /analyze <adresse> - Analyse détaillée d'un token"""
    if not context.args:
        await update.message.reply_text(
            "Usage: /analyze <adresse_du_token>\n"
            "Exemple: /analyze DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"
        )
        return

    address = context.args[0]
    await update.message.reply_text(f"📊 Analyse en cours...")

    analysis = api.analyze_token(address)

    if not analysis:
        await update.message.reply_text("❌ Token non trouvé ou erreur API.")
        return

    is_gem, reasons = api.is_potential_gem(analysis)

    msg = format_analysis_message(analysis, is_gem, reasons)
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)


async def watchlist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /watchlist - Voir la watchlist"""
    chat_id = str(update.effective_chat.id)
    watchlist = bot_data.get("watchlist", {}).get(chat_id, [])

    if not watchlist:
        await update.message.reply_text(
            "📋 Votre watchlist est vide.\n"
            "Ajoutez des tokens avec /add <adresse>"
        )
        return

    msg = "📋 *Votre Watchlist*\n\n"
    for i, addr in enumerate(watchlist, 1):
        analysis = api.analyze_token(addr)
        if analysis:
            msg += f"{i}. *{analysis['name']}* (${analysis['symbol']})\n"
            msg += f"   💵 ${analysis['price_usd']} | "
            msg += f"1h: {analysis['price_change_1h']:+.1f}%\n"
        else:
            msg += f"{i}. `{addr[:12]}...` (données indisponibles)\n"
        msg += "\n"
        await asyncio.sleep(1.5)  # Rate limit

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def add_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /add <adresse> - Ajouter à la watchlist"""
    if not context.args:
        await update.message.reply_text("Usage: /add <adresse_du_token>")
        return

    chat_id = str(update.effective_chat.id)
    address = context.args[0]

    if "watchlist" not in bot_data:
        bot_data["watchlist"] = {}
    if chat_id not in bot_data["watchlist"]:
        bot_data["watchlist"][chat_id] = []

    if address in bot_data["watchlist"][chat_id]:
        await update.message.reply_text("Ce token est déjà dans votre watchlist.")
        return

    if len(bot_data["watchlist"][chat_id]) >= 20:
        await update.message.reply_text("❌ Watchlist pleine (max 20 tokens).")
        return

    bot_data["watchlist"][chat_id].append(address)
    save_data(bot_data)
    await update.message.reply_text(f"✅ Token ajouté à votre watchlist !\n`{address}`", parse_mode=ParseMode.MARKDOWN)


async def remove_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /remove <adresse> - Retirer de la watchlist"""
    if not context.args:
        await update.message.reply_text("Usage: /remove <adresse_du_token>")
        return

    chat_id = str(update.effective_chat.id)
    address = context.args[0]

    if chat_id in bot_data.get("watchlist", {}) and address in bot_data["watchlist"][chat_id]:
        bot_data["watchlist"][chat_id].remove(address)
        save_data(bot_data)
        await update.message.reply_text("✅ Token retiré de votre watchlist.")
    else:
        await update.message.reply_text("Ce token n'est pas dans votre watchlist.")


async def filters_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /filters - Afficher les filtres actifs"""
    msg = "⚙️ *Filtres actifs*\n\n"
    msg += f"💧 Liquidité min: ${FILTERS['min_liquidity_usd']:,}\n"
    msg += f"💰 Volume 24h min: ${FILTERS['min_volume_24h']:,}\n"
    msg += f"📊 Market cap: ${FILTERS['min_market_cap']:,} - ${FILTERS['max_market_cap']:,}\n"
    msg += f"🚀 Pump 5min: ≥{FILTERS['min_price_change_5m']}%\n"
    msg += f"📈 Pump 1h: ≥{FILTERS['min_price_change_1h']}%\n"
    msg += f"🆕 Âge max: {FILTERS['max_token_age_hours']}h\n"
    msg += f"🛒 Achats min (5min): {FILTERS['min_buys_5m']}\n"
    msg += f"\n⏱ Intervalle scan: {POLLING_INTERVAL}s"
    msg += "\n\n_Modifiez config.py pour ajuster les filtres._"

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /help"""
    help_msg = """
📖 *Aide - Solana Meme Coin Tracker*

*Commandes principales :*
/scan - Lancer un scan immédiat
/trending - Voir les tokens trending
/metas - Narratives populaires du moment
/search <nom> - Rechercher un token
/analyze <adresse> - Analyse complète

*Watchlist :*
/watchlist - Voir vos tokens suivis
/add <adresse> - Ajouter un token
/remove <adresse> - Retirer un token

*Paramètres :*
/filters - Voir les filtres d'alerte
/stop - Désactiver les alertes auto

*Comment ça marche ?*
Le bot scanne DexScreener toutes les ~45s pour :
1. Détecter les nouveaux tokens Solana
2. Identifier les pumps rapides
3. Repérer les tokens boostés
4. Suivre les narratives trending

Quand un token passe les filtres, vous recevez une alerte automatique.

⚠️ _Ce bot est un outil d'information. DYOR (Do Your Own Research). Ne jamais investir plus que ce que vous pouvez perdre._
"""
    await update.message.reply_text(help_msg, parse_mode=ParseMode.MARKDOWN)


# ============================================================
# FONCTIONS UTILITAIRES
# ============================================================


def format_analysis_message(analysis: dict, is_gem: bool, reasons: list) -> str:
    """Formater un message d'analyse de token"""
    gem_indicator = "💎 POTENTIEL GEM" if is_gem else "📊 Analyse"

    msg = f"{'='*20}\n"
    msg += f"{gem_indicator}\n"
    msg += f"{'='*20}\n\n"
    msg += f"*{analysis['name']}* (${analysis['symbol']})\n\n"
    msg += f"💵 *Prix:* ${analysis['price_usd']}\n"
    msg += f"💧 *Liquidité:* ${analysis['liquidity_usd']:,.0f}\n"
    msg += f"💰 *Volume 24h:* ${analysis['volume_24h']:,.0f}\n"

    if analysis['market_cap']:
        msg += f"📊 *Market Cap:* ${analysis['market_cap']:,.0f}\n"

    msg += f"\n📈 *Variations:*\n"
    msg += f"   5min: {analysis['price_change_5m']:+.1f}%\n"
    msg += f"   1h: {analysis['price_change_1h']:+.1f}%\n"
    msg += f"   24h: {analysis['price_change_24h']:+.1f}%\n"

    msg += f"\n🛒 *Transactions (5min):*\n"
    msg += f"   Achats: {analysis['buys_5m']} | Ventes: {analysis['sells_5m']}\n"
    msg += f"   Ratio: {analysis['buy_sell_ratio_5m']:.1f}x\n"

    if analysis['age_hours']:
        msg += f"\n⏰ *Âge:* {analysis['age_hours']:.1f} heures\n"

    msg += f"\n🏦 *DEX:* {analysis['dex']}\n"

    if reasons:
        msg += f"\n{'🟢' if is_gem else '🔵'} *Signaux:*\n"
        for reason in reasons:
            msg += f"   {reason}\n"

    msg += f"\n🔗 [Voir sur DexScreener]({analysis['dexscreener_url']})\n"
    msg += f"📋 `{analysis['address']}`"

    return msg


def format_alert_message(analysis: dict, reasons: list) -> str:
    """Formater un message d'alerte compact"""
    msg = f"🚨 *ALERTE MEME COIN* 🚨\n\n"
    msg += f"*{analysis['name']}* (${analysis['symbol']})\n\n"
    msg += f"💵 Prix: ${analysis['price_usd']}\n"
    msg += f"📊 MC: ${analysis['market_cap']:,.0f}\n" if analysis['market_cap'] else ""
    msg += f"💧 Liq: ${analysis['liquidity_usd']:,.0f}\n"
    msg += f"📈 5min: {analysis['price_change_5m']:+.1f}% | 1h: {analysis['price_change_1h']:+.1f}%\n"
    msg += f"🛒 Achats 5min: {analysis['buys_5m']} (ratio {analysis['buy_sell_ratio_5m']:.1f}x)\n"

    if analysis['age_hours'] and analysis['age_hours'] <= 24:
        msg += f"🆕 Créé il y a {analysis['age_hours']:.1f}h\n"

    msg += f"\n*Signaux:*\n"
    for reason in reasons:
        msg += f"  {reason}\n"

    msg += f"\n🔗 [DexScreener]({analysis['dexscreener_url']})\n"
    msg += f"📋 `{analysis['address']}`"

    return msg


# ============================================================
# SCAN AUTOMATIQUE
# ============================================================


async def scan_for_gems() -> list:
    """Scanner pour trouver des gems et retourner les messages d'alerte"""
    messages = []

    try:
        # Récupérer les nouveaux tokens
        new_tokens = api.find_new_meme_coins()
        logger.info(f"Scan: {len(new_tokens)} tokens trouvés")

        alerts_sent = 0

        for token_data in new_tokens:
            if alerts_sent >= MAX_ALERTS_PER_CYCLE:
                break

            address = token_data["address"]

            # Vérifier si déjà vu récemment
            if address in bot_data.get("seen_tokens", []):
                continue

            # Analyser le token
            await asyncio.sleep(1.5)  # Rate limit
            analysis = api.analyze_token(address)

            if not analysis:
                continue

            # Évaluer le potentiel
            is_gem, reasons = api.is_potential_gem(analysis)

            if is_gem:
                msg = format_alert_message(analysis, reasons)
                messages.append(msg)
                alerts_sent += 1

                # Marquer comme vu
                if "seen_tokens" not in bot_data:
                    bot_data["seen_tokens"] = []
                bot_data["seen_tokens"].append(address)

                # Garder seulement les 500 derniers pour ne pas surcharger
                if len(bot_data["seen_tokens"]) > 500:
                    bot_data["seen_tokens"] = bot_data["seen_tokens"][-500:]

        save_data(bot_data)

    except Exception as e:
        logger.error(f"Erreur lors du scan: {e}")

    return messages


async def auto_scan_job(context: ContextTypes.DEFAULT_TYPE):
    """Job automatique de scan périodique"""
    messages = await scan_for_gems()

    if messages and bot_data["subscribers"]:
        for chat_id in bot_data["subscribers"]:
            for msg in messages:
                try:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=msg,
                        parse_mode=ParseMode.MARKDOWN,
                        disable_web_page_preview=True,
                    )
                    await asyncio.sleep(0.5)
                except Exception as e:
                    logger.error(f"Erreur envoi à {chat_id}: {e}")


# ============================================================
# MAIN
# ============================================================


def main():
    """Démarrer le bot"""
    if TELEGRAM_BOT_TOKEN == "VOTRE_TOKEN_ICI":
        print("=" * 50)
        print("⚠️  ERREUR: Token Telegram non configuré !")
        print("")
        print("1. Ouvrez Telegram et cherchez @BotFather")
        print("2. Envoyez /newbot et suivez les instructions")
        print("3. Copiez le token reçu")
        print("4. Collez-le dans config.py à la ligne TELEGRAM_BOT_TOKEN")
        print("=" * 50)
        return

    print("🚀 Démarrage du Solana Meme Coin Tracker...")
    print(f"⏱  Intervalle de scan: {POLLING_INTERVAL}s")
    print(f"📊 Filtres: MC ${FILTERS['min_market_cap']:,}-${FILTERS['max_market_cap']:,}")
    print(f"💧 Liquidité min: ${FILTERS['min_liquidity_usd']:,}")
    print("")

    # Créer l'application
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Ajouter les commandes
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stop", stop))
    app.add_handler(CommandHandler("scan", scan_command))
    app.add_handler(CommandHandler("trending", trending_command))
    app.add_handler(CommandHandler("metas", metas_command))
    app.add_handler(CommandHandler("search", search_command))
    app.add_handler(CommandHandler("analyze", analyze_command))
    app.add_handler(CommandHandler("watchlist", watchlist_command))
    app.add_handler(CommandHandler("add", add_command))
    app.add_handler(CommandHandler("remove", remove_command))
    app.add_handler(CommandHandler("filters", filters_command))
    app.add_handler(CommandHandler("help", help_command))

    # Ajouter le job de scan automatique
    job_queue = app.job_queue
    job_queue.run_repeating(
        auto_scan_job,
        interval=POLLING_INTERVAL,
        first=10,  # Premier scan après 10 secondes
    )

    print("✅ Bot démarré ! En attente de messages...")
    print("   Appuyez sur Ctrl+C pour arrêter.")

    # Démarrer le bot
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
