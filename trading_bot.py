"""
Bot Telegram V2 - Solana Meme Coin TRADING Bot
Combine le monitoring (alertes) avec le trading automatique (achat/vente).
"""

import asyncio
import logging
import time
import json
import os
from datetime import datetime, timezone

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)
from telegram.constants import ParseMode

from config import TELEGRAM_BOT_TOKEN, POLLING_INTERVAL, MAX_ALERTS_PER_CYCLE, FILTERS
from dexscreener_api import DexScreenerAPI
from trader import (
    TradingConfig, WalletManager, JupiterSwap,
    PositionManager, TradingEngine, Position
)
from token_security import TokenSecurityChecker
from price_monitor import PriceMonitor
from copy_trading import CopyTradingEngine, CopyTradeSignal
from smart_entry import SmartEntryEngine, EntrySignal
from pnl_tracker import PnLTracker, DynamicPositionSizer
from correlation_filter import CorrelationFilter
from liquidity_guard import LiquidityGuard
from post_trade_analyzer import PostTradeAnalyzer
from postmortem_tracker import init_db as init_postmortem_db, start_postmortem_thread
from circuit_breaker import CircuitBreaker, CBConfig
from capital_watchdog import CapitalWatchdog, WatchdogConfig
from daily_pnl_guard import DailyPnLGuard, DailyPnLGuardConfig

# Logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ============================================================
# INITIALISATION
# ============================================================

# Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("PERSISTENT_DATA_DIR", BASE_DIR)
os.makedirs(DATA_DIR, exist_ok=True)
TRADING_CONFIG_FILE = os.path.join(DATA_DIR, "trading_config.json")


def load_trading_config() -> TradingConfig:
    """Charger la config de trading"""
    config = TradingConfig()
    # Helius RPC (gratuit, 1M credits/mois, 10 req/s) - fallback sur public RPC
    config.rpc_url = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
    if os.path.exists(TRADING_CONFIG_FILE):
        with open(TRADING_CONFIG_FILE, "r") as f:
            data = json.load(f)
        for key, value in data.items():
            if hasattr(config, key):
                setattr(config, key, value)
    return config


def save_trading_config(config: TradingConfig):
    """Sauvegarder la config"""
    data = {
        "max_budget_sol": config.max_budget_sol,
        "position_size_sol": config.position_size_sol,
        "max_open_positions": config.max_open_positions,
        "sniper_enabled": config.sniper_enabled,
        "sniper_position_sol": config.sniper_position_sol,
        "take_profit_pct": config.take_profit_pct,
        "stop_loss_pct": config.stop_loss_pct,
        "trailing_stop_pct": config.trailing_stop_pct,
        "trailing_activation_pct": config.trailing_activation_pct,
        "time_stop_enabled": config.time_stop_enabled,
        "time_stop_minutes": config.time_stop_minutes,
        "time_stop_min_profit": config.time_stop_min_profit,
        # momentum_stop DÉSACTIVÉ (ne plus sauvegarder)
        "slippage_bps": config.slippage_bps,
    }
    with open(TRADING_CONFIG_FILE, "w") as f:
        json.dump(data, f, indent=2)


# Initialisation globale
trading_config = load_trading_config()
api = DexScreenerAPI()
wallet = WalletManager(trading_config.rpc_url)
swap_engine: JupiterSwap = None
positions = PositionManager()
trading_engine: TradingEngine = None
security_checker: TokenSecurityChecker = None
price_monitor: PriceMonitor = None
copy_trader: CopyTradingEngine = None
smart_entry: SmartEntryEngine = None
pnl_tracker: PnLTracker = None
position_sizer: DynamicPositionSizer = None
correlation_filter: CorrelationFilter = None
liquidity_guard: LiquidityGuard = None
post_trade_analyzer: PostTradeAnalyzer = None
circuit_breaker: CircuitBreaker = None
capital_watchdog: CapitalWatchdog = None
daily_pnl_guard: DailyPnLGuard = None
auto_trading_enabled = False
subscribers = []

# Déduplication: tokens déjà analysés/rejetés (évite de re-checker les mêmes)
seen_tokens: set = set()  # Tokens déjà achetés ou rejetés
MAX_SEEN_TOKENS = 500  # Limite pour éviter une fuite mémoire

# Blacklist post-SL: tokens qui ont déclenché un Stop Loss (ne pas racheter)
SL_BLACKLIST_DURATION = 3600  # 1 heure de blacklist après un SL
BLACKLIST_FILE = os.path.join(DATA_DIR, "sl_blacklist.json")

def _load_blacklist() -> dict:
    """Charger la blacklist depuis le disque (supprimer les entrées expirées)"""
    if os.path.exists(BLACKLIST_FILE):
        try:
            with open(BLACKLIST_FILE, "r") as f:
                data = json.load(f)
            # Nettoyer les entrées expirées
            now = time.time()
            return {k: v for k, v in data.items() if v > now}
        except:
            pass
    return {}

def _save_blacklist():
    """Sauvegarder la blacklist sur disque"""
    try:
        with open(BLACKLIST_FILE, "w") as f:
            json.dump(sl_blacklist, f)
    except:
        pass

sl_blacklist: dict = _load_blacklist()  # {token_address: timestamp_expiry}

# Charger les subscribers
SUBS_FILE = os.path.join(DATA_DIR, "bot_data.json")
if os.path.exists(SUBS_FILE):
    with open(SUBS_FILE, "r") as f:
        bot_data = json.load(f)
    subscribers = bot_data.get("subscribers", [])


def init_trading():
    """Initialiser le moteur de trading (après import du wallet)"""
    global swap_engine, trading_engine, security_checker, price_monitor, copy_trader, smart_entry, pnl_tracker, position_sizer, correlation_filter, liquidity_guard, post_trade_analyzer, circuit_breaker, capital_watchdog, daily_pnl_guard
    swap_engine = JupiterSwap(wallet, trading_config)
    trading_engine = TradingEngine(trading_config, wallet, swap_engine, positions)
    security_checker = TokenSecurityChecker(rpc_url=trading_config.rpc_url)
    price_monitor = PriceMonitor(on_price_update=on_realtime_price_update)
    # Copy Trading DÉSACTIVÉ (performances négatives)
    copy_trader = None
    smart_entry = SmartEntryEngine()
    pnl_tracker = PnLTracker()
    # Purger les trades MOMENTUM et COPY_TRADE de l'historique
    purged = pnl_tracker.purge_strategies(["momentum", "copy_trade"])
    if purged:
        logger.info(f"🧹 Purgé {purged} trades (momentum + copy_trade) de l'historique PnL")
    position_sizer = DynamicPositionSizer(
        base_size_sol=trading_config.position_size_sol,
        min_size_sol=0.02,
        max_size_sol=0.15
    )

    # Importer l'historique existant dans le PnL Tracker (migration)
    if not pnl_tracker.trade_results and trading_engine.trade_history:
        imported = 0
        for trade in trading_engine.trade_history:
            if trade.get("type") == "SELL":
                pnl_tracker.record_trade(
                    token_address=trade.get("token_address", ""),
                    token_symbol=trade.get("token", "???"),
                    strategy=trade.get("strategy", "momentum"),
                    entry_time=trade.get("timestamp", ""),
                    amount_sol=trade.get("amount_sol_invested", 0.05),
                    pnl_pct=trade.get("pnl_pct", 0),
                    exit_reason=trade.get("reason", "unknown"),
                )
                imported += 1
        if imported:
            logger.info(f"📥 PnL Tracker: {imported} trades historiques importés")

    logger.info("✅ Security checker (RugCheck + on-chain) initialisé")
    logger.info("🔌 PriceMonitor WebSocket initialisé")
    logger.info("📋 Copy Trading: DÉSACTIVÉ")
    logger.info("🧠 Smart Entry Engine initialisé (SNIPER ONLY - tokens < 1h)")
    logger.info(f"💰 Position Sizer: {trading_config.position_size_sol} SOL (dynamique 0.02-0.15)")
    # Filtre anti-corrélation
    correlation_filter = CorrelationFilter(data_dir=DATA_DIR)
    correlation_filter.sync_with_positions(positions.get_open_positions())
    logger.info(f"🎯 Filtre corrélation: {len(correlation_filter.active_narratives)} positions trackées")

    logger.info(f"📊 PnL Tracker: {len(pnl_tracker.trade_results)} trades chargés")

    # Liquidity Guard (3 protections)
    liquidity_guard = LiquidityGuard()
    # Enregistrer les positions existantes pour le monitoring LP
    for pos in positions.get_open_positions():
        # Récupérer la liquidité actuelle pour le monitoring
        liq = liquidity_guard._fetch_liquidity(pos.token_address)
        if liq is not None and liq > 0:
            liquidity_guard.register_position(pos.token_address, liq)
    # Valider que toutes les positions ont un SL actif
    issues = liquidity_guard.validate_positions_on_startup(positions.positions, trading_config)
    if issues:
        logger.warning(f"⚠️ SL Guard: {len(issues)} problèmes corrigés au démarrage")
        for issue in issues:
            logger.warning(f"  {issue}")
    logger.info(f"💧 Liquidity Guard: {len(liquidity_guard.snapshots)} positions monitorées")

    # Post-Trade Analyzer
    post_trade_analyzer = PostTradeAnalyzer()
    logger.info(f"📊 Post-Trade Analyzer: {len(post_trade_analyzer.analyses)} analyses, "
                f"{len(post_trade_analyzer.pending_checks)} en suivi")

    # Postmortem Tracker (SQLite)
    init_postmortem_db()
    logger.info("📋 Postmortem Tracker DB initialisée")

    # CircuitBreaker centralisé (4 règles de sortie)
    circuit_breaker = CircuitBreaker(CBConfig(
        time_stop_minutes=trading_config.time_stop_minutes,
        time_stop_min_profit=trading_config.time_stop_min_profit,
        stop_loss_pct=trading_config.stop_loss_pct,
        trailing_activation_pct=trading_config.trailing_activation_pct,
        trailing_stop_pct=trading_config.trailing_stop_pct,
        take_profit_pct=trading_config.take_profit_pct,
        momentum_stop_enabled=False,  # DÉSACTIVÉ
    ))
    # Synchroniser les positions existantes
    existing_positions = []
    for addr, pos in positions.positions.items():
        existing_positions.append({
            "token_address": addr,
            "token_symbol": pos.token_symbol,
            "entry_price_usd": pos.entry_price_usd,
            "entry_time": pos.entry_time,
            "highest_price": pos.highest_price,
            "current_price": pos.current_price,
            "strategy": pos.strategy,
        })
    if existing_positions:
        circuit_breaker.sync_from_existing_positions(existing_positions)
    logger.info(f"🔌 CircuitBreaker: {len(circuit_breaker.positions)} positions synchronisées")

    # Capital Watchdog (surveillance de la santé du capital)
    capital_watchdog = CapitalWatchdog(WatchdogConfig(
        warn_gap_seconds=10.0,
        critical_gap_seconds=20.0,
        emergency_gap_seconds=30.0,
    ))
    # Enregistrer les positions existantes
    for addr, pos in positions.positions.items():
        capital_watchdog.register_position(
            token_address=addr,
            token_symbol=pos.token_symbol,
            strategy=pos.strategy,
            amount_sol=pos.amount_sol_invested,
            entry_price=pos.entry_price_usd,
        )
    logger.info(f"🛡️ Capital Watchdog: {len(capital_watchdog.positions)} positions surveillées")

    # Daily PnL Guard (circuit breaker global)
    daily_pnl_guard = DailyPnLGuard(DailyPnLGuardConfig(
        max_daily_loss_sol=-0.05,
        max_consecutive_sl=3,
        pause_duration_minutes=60.0,
    ))
    logger.info("⛔ Daily PnL Guard: actif (pause après -0.05 SOL/jour ou 3 SL consécutifs)")


# ============================================================
# WEBSOCKET PRICE MONITOR - CALLBACKS
# ============================================================

async def on_realtime_price_update(token_address: str, price_sol: float, change_pct: float):
    """
    Callback appelé en temps réel quand le prix d'un token change.
    Vérifie instantanément le TP/SL.
    """
    global sl_blacklist

    if not trading_engine:
        return

    if token_address not in positions.positions:
        return

    pos = positions.positions[token_address]

    # Convertir le prix SOL en USD (approximation)
    # On utilise le ratio entry_price / (amount_sol / amount_tokens) pour estimer
    # Mais plus simple: utiliser le prix SOL actuel depuis le wallet
    sol_price_usd = 73.0  # Approximation, sera mis à jour
    try:
        balance_sol = wallet.get_sol_balance()
        if balance_sol > 0:
            # On ne peut pas facilement obtenir le prix USD du SOL ici
            # Utiliser le prix SOL depuis DexScreener serait trop lent
            # On utilise le ratio de prix pour calculer le PnL
            pass
    except:
        pass

    # Calculer le prix USD à partir du prix SOL
    current_price_usd = price_sol * sol_price_usd

    # Mettre à jour la position
    if current_price_usd > 0:
        positions.update_position(token_address, current_price_usd)

    # Heartbeat watchdog (cette position est surveillée)
    if capital_watchdog:
        capital_watchdog.heartbeat(token_address, current_price_usd)

    # Vérifier TP/SL via CircuitBreaker centralisé (SEUL chemin de décision)
    if not circuit_breaker:
        return
    cb_action = circuit_breaker.check(token_address, current_price_usd)
    if cb_action.should_sell:
        reason = cb_action.reason
        result = trading_engine.execute_sell(pos, reason)
        if result:
            # Enregistrer dans le PnL tracker
            if pnl_tracker:
                pnl_tracker.record_trade(
                    token_address=pos.token_address,
                    token_symbol=pos.token_symbol,
                    strategy=pos.strategy,
                    entry_time=pos.entry_time,
                    amount_sol=pos.amount_sol_invested,
                    pnl_pct=pos.pnl_pct,
                    exit_reason=reason,
                )
            # Mettre à jour le position sizer (streak)
            if position_sizer:
                position_sizer.record_result(pos.pnl_pct > 0)
            # Daily PnL Guard
            if daily_pnl_guard:
                pnl_sol = pos.amount_sol_invested * (pos.pnl_pct / 100.0)
                daily_pnl_guard.record_trade(pnl_sol, is_stop_loss=(reason and "SL" in reason))

            # Retirer du filtre de corrélation
            if correlation_filter:
                correlation_filter.unregister_position(pos.token_address)

            # Retirer du monitoring LP
            if liquidity_guard:
                liquidity_guard.unregister_position(pos.token_address)

            # Retirer du CircuitBreaker
            if circuit_breaker:
                circuit_breaker.close_position(pos.token_address)
                if capital_watchdog:
                    capital_watchdog.unregister_position(pos.token_address)

            # Enregistrer pour analyse post-trade
            if post_trade_analyzer:
                post_trade_analyzer.record_trade_exit(
                    token_address=pos.token_address,
                    token_symbol=pos.token_symbol,
                    token_name=pos.token_name,
                    strategy=pos.strategy,
                    entry_time=pos.entry_time,
                    entry_price=pos.entry_price_usd,
                    exit_price=pos.current_price,
                    exit_pnl_pct=pos.pnl_pct,
                    exit_reason=reason,
                    highest_price=pos.highest_price,
                    amount_sol=pos.amount_sol_invested,
                )

            # Postmortem Tracker (thread dédié 30min)
            if result and pos.entry_price_usd > 0:
                try:
                    helius_key = os.environ.get("HELIUS_API_KEY", "")
                    tg_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
                    tg_chat = str(subscribers[0]) if subscribers else ""
                    start_postmortem_thread(
                        trade_record=result,
                        entry_price_usd=pos.entry_price_usd,
                        helius_api_key=helius_key,
                        telegram_bot_token=tg_token,
                        telegram_chat_id=tg_chat,
                    )
                except Exception as e:
                    logger.error(f"Erreur postmortem thread: {e}")

            # Blacklister si SL
            if pos.pnl_pct < 0:
                sl_blacklist[pos.token_address] = time.time() + SL_BLACKLIST_DURATION
                _save_blacklist()
                seen_tokens.add(pos.token_address)
                logger.info(f"🚫 Blacklisté après SL (WS): {pos.token_name}")

            # Retirer du monitoring WS
            if price_monitor:
                await price_monitor.remove_token(token_address)

            # Notifier (on ne peut pas envoyer de message Telegram depuis ici
            # car on n'a pas le context. On log et le polling job notifiera)
            logger.info(f"⚡ VENTE WS INSTANTANÉE: {pos.token_name} PnL: {pos.pnl_pct:+.1f}% - {reason}")


async def start_price_monitor_for_positions():
    """Démarrer le monitoring WS pour toutes les positions ouvertes"""
    if not price_monitor:
        return

    await price_monitor.start()

    for addr in list(positions.positions.keys()):
        try:
            success = await price_monitor.add_token(addr)
            if success:
                logger.info(f"🔌 WS monitoring actif: {addr[:12]}...")
        except Exception as e:
            logger.error(f"Erreur ajout WS pour {addr[:12]}: {e}")


# ============================================================
# COMMANDES TELEGRAM - WALLET
# ============================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /start"""
    chat_id = update.effective_chat.id
    if chat_id not in subscribers:
        subscribers.append(chat_id)

    msg = """
🤖 *Solana Trading Bot* 🤖

Bot de trading automatique pour meme coins Solana.

*📊 Monitoring :*
/scan - Scanner les meme coins
/trending - Tokens trending
/metas - Narratives populaires

*💰 Trading :*
/wallet - Voir votre wallet
/import\_wallet `<clé_privée>` - Importer wallet
/balance - Voir le solde
/positions - Positions ouvertes
/history - Historique des trades
/stats - Statistiques
/pnl - Rapport PnL détaillé (par stratégie)
/diversity - Diversification portefeuille

*⚙️ Contrôle :*
/auto\\_on - Activer le trading auto
/auto\\_off - Désactiver le trading auto
/set\\_tp `<pct>` - Modifier Take Profit
/set\\_sl `<pct>` - Modifier Stop Loss
/set\\_trailing - Voir/modifier trailing stop
/set\\_timestop - Time stop (sortie forcée)
/set\\_size `<sol>` - Modifier taille position
/config - Voir la configuration
/sell\\_all - Vendre toutes les positions

*📋 Copy Trading :* DÉSACTIVÉ

*🔍 Manuel :*
/buy `<adresse>` - Acheter un token
/sell `<adresse>` - Vendre un token

⚠️ _DYOR - Ne tradez que ce que vous pouvez perdre._
"""
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def wallet_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /wallet - Afficher ou créer le wallet"""
    if wallet.keypair:
        balance = wallet.get_sol_balance()
        msg = f"💰 *Votre Wallet*\n\n"
        msg += f"📋 Adresse:\n`{wallet.public_key}`\n\n"
        msg += f"💵 Solde: *{balance:.4f} SOL*\n"
        msg += f"\n🔗 [Voir sur Solscan](https://solscan.io/account/{wallet.public_key})"
        msg += f"\n\n💡 Envoyez des SOL à cette adresse pour commencer à trader."
    else:
        # Créer un nouveau wallet automatiquement
        pub_key = wallet.load_or_create_wallet()
        init_trading()
        msg = f"✅ *Nouveau Wallet créé !*\n\n"
        msg += f"📋 Adresse:\n`{pub_key}`\n\n"
        msg += f"💵 Solde: *0.0000 SOL*\n\n"
        msg += f"📤 Envoyez des SOL à cette adresse depuis Phantom, Backpack ou Binance.\n\n"
        msg += f"⚠️ Ce wallet est DÉDIÉ au trading. Ne mettez que ce que vous pouvez perdre.\n\n"
        msg += f"Ou importez un wallet existant: /import\\_wallet `<clé_privée>`"
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def import_wallet_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /import_wallet <private_key>"""
    if not context.args:
        await update.message.reply_text(
            "Usage: /import\\_wallet `<clé_privée_base58>`\n\n"
            "⚠️ Envoyez votre clé privée en base58.\n"
            "Utilisez un wallet DÉDIÉ, jamais votre wallet principal !",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    private_key = context.args[0]

    # Supprimer le message contenant la clé privée (sécurité)
    try:
        await update.message.delete()
    except:
        pass

    try:
        pub_key = wallet.import_wallet(private_key)
        init_trading()
        balance = wallet.get_sol_balance()
        msg = f"✅ *Wallet importé avec succès !*\n\n"
        msg += f"📋 Adresse: `{pub_key}`\n"
        msg += f"💵 Solde: *{balance:.4f} SOL*\n\n"
        msg += "Le bot peut maintenant trader. Activez avec /auto\\_on"
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        await update.message.reply_text(f"❌ Erreur: {e}")


async def balance_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /balance"""
    if not wallet.keypair:
        await update.message.reply_text("❌ Aucun wallet. Utilisez /import\\_wallet", parse_mode=ParseMode.MARKDOWN)
        return

    balance = wallet.get_sol_balance()
    invested = positions.total_invested()
    available = balance

    msg = f"💰 *Solde du Wallet*\n\n"
    msg += f"💵 SOL disponible: *{available:.4f} SOL*\n"
    msg += f"📊 SOL investi: *{invested:.4f} SOL*\n"
    msg += f"📈 Positions ouvertes: *{positions.count_positions()}*\n"
    msg += f"\n💼 Budget max: {trading_config.max_budget_sol} SOL"
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


# ============================================================
# COMMANDES TELEGRAM - TRADING
# ============================================================

async def auto_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Activer le trading automatique"""
    global auto_trading_enabled

    if not wallet.keypair:
        await update.message.reply_text("❌ Importez d'abord un wallet avec /import\\_wallet", parse_mode=ParseMode.MARKDOWN)
        return

    if not trading_engine:
        init_trading()

    auto_trading_enabled = True
    balance = wallet.get_sol_balance()
    msg = f"✅ *Trading automatique ACTIVÉ*\n\n"
    msg += f"💵 Solde: {balance:.4f} SOL\n"
    msg += f"📊 Stratégie:\n"
    msg += f"  • Recovered Only: ✅ ({trading_config.sniper_position_sol} SOL/trade)\n"
    msg += f"  • Sniper: ❌ Désactivé\n"
    msg += f"🎯 TP: +{trading_config.take_profit_pct}% | SL: {trading_config.stop_loss_pct}%\n"
    msg += f"\n⚠️ Le bot surveille et protège le capital automatiquement !"
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def auto_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Désactiver le trading automatique"""
    global auto_trading_enabled
    auto_trading_enabled = False
    await update.message.reply_text("🛑 *Trading automatique DÉSACTIVÉ*\n\nLe bot continue de scanner mais n'achètera plus.", parse_mode=ParseMode.MARKDOWN)


async def positions_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /positions - Voir les positions ouvertes"""
    open_positions = positions.get_open_positions()

    if not open_positions:
        await update.message.reply_text("📋 Aucune position ouverte.")
        return

    msg = "📊 *Positions Ouvertes*\n\n"
    total_pnl = 0

    for i, pos in enumerate(open_positions, 1):
        emoji = "🟢" if pos.pnl_pct >= 0 else "🔴"
        msg += f"{i}. {emoji} *{pos.token_name}* (${pos.token_symbol})\n"
        msg += f"   💵 Investi: {pos.amount_sol_invested} SOL\n"
        msg += f"   📈 PnL: {pos.pnl_pct:+.1f}%\n"
        msg += f"   🏷 Stratégie: {pos.strategy}\n"
        msg += f"   ⏰ Depuis: {pos.entry_time[:16]}\n\n"
        total_pnl += pos.pnl_pct

    msg += f"━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📊 PnL moyen: {total_pnl / len(open_positions):+.1f}%"
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def history_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /history - Historique des trades"""
    if not trading_engine or not trading_engine.trade_history:
        await update.message.reply_text("📋 Aucun trade effectué.")
        return

    msg = "📜 *Historique des Trades* (derniers 10)\n\n"
    for trade in trading_engine.trade_history[-10:]:
        if trade["type"] == "BUY":
            msg += f"🟢 ACHAT {trade['token']} - {trade['amount_sol']} SOL\n"
        else:
            pnl = trade.get('pnl_pct', 0)
            emoji = "✅" if pnl > 0 else "❌"
            msg += f"{emoji} VENTE {trade['token']} - PnL: {pnl:+.1f}%\n"
            msg += f"   Raison: {trade.get('reason', 'N/A')}\n"
        msg += f"   📅 {trade['timestamp'][:16]}\n\n"

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /stats - Statistiques détaillées"""
    if not trading_engine:
        await update.message.reply_text("❌ Trading non initialisé. Importez un wallet d'abord.")
        return

    stats = trading_engine.get_stats()
    msg = f"📊 *Statistiques de Trading*\n\n"
    msg += f"💵 Solde: {stats['balance_sol']:.4f} SOL\n"
    msg += f"📈 Positions ouvertes: {stats['open_positions']}\n\n"
    msg += f"*Trades:*\n"
    msg += f"  Total: {stats['total_trades']}\n"
    msg += f"  ✅ Wins: {stats['wins']} | ❌ Losses: {stats['losses']}\n"
    msg += f"  🎯 Win Rate: {stats['win_rate']:.1f}%\n"
    msg += f"  📊 PnL moyen: {stats['avg_pnl_pct']:+.1f}%\n"
    msg += f"\n💼 Total investi: {stats['total_invested_sol']:.2f} SOL\n"

    # Stats PnL Tracker détaillées
    if pnl_tracker and pnl_tracker.trade_results:
        total = pnl_tracker.get_total_pnl()
        msg += f"\n*📈 PnL Total:*\n"
        msg += f"  💰 {total['total_pnl_sol']:+.4f} SOL\n"
        msg += f"  🎯 Best: {total['best_trade']:+.1f}% | Worst: {total['worst_trade']:+.1f}%\n"

        # Stats par stratégie
        strat_stats = pnl_tracker.get_strategy_stats()
        if strat_stats:
            msg += f"\n*🎮 Par Stratégie:*\n"
            for name, s in sorted(strat_stats.items(), key=lambda x: x[1].total_pnl_sol, reverse=True):
                emoji = "🟢" if s.total_pnl_sol >= 0 else "🔴"
                msg += f"  {emoji} {name}: WR {s.win_rate:.0f}% | PnL {s.total_pnl_sol:+.3f} SOL ({s.total_trades} trades)\n"

        # PnL quotidien (3 derniers jours)
        daily = pnl_tracker.get_daily_summary(3)
        active_days = [d for d in daily if d['trades'] > 0]
        if active_days:
            msg += f"\n*📅 Derniers jours:*\n"
            for d in active_days:
                day_emoji = "🟢" if d['pnl_sol'] >= 0 else "🔴"
                msg += f"  {day_emoji} {d['date']}: {d['pnl_sol']:+.4f} SOL ({d['trades']} trades, WR {d['win_rate']:.0f}%)\n"

    # Position Sizer info
    if position_sizer:
        sizer_info = position_sizer.get_info()
        streak = ""
        if sizer_info['consecutive_wins'] >= 2:
            streak = f"🔥 Série: {sizer_info['consecutive_wins']}W"
        elif sizer_info['consecutive_losses'] >= 2:
            streak = f"⚠️ Série: {sizer_info['consecutive_losses']}L"
        if streak:
            msg += f"\n{streak}"

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def config_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /config - Voir la configuration"""
    msg = f"⚙️ *Configuration Trading*\n\n"
    msg += f"*Budget:*\n"
    msg += f"  Max: {trading_config.max_budget_sol} SOL\n"
    msg += f"  Taille position: {trading_config.position_size_sol} SOL\n"
    msg += f"  Max positions: {trading_config.max_open_positions}\n\n"
    msg += f"*Sniper:* {'✅ Activé' if trading_config.sniper_enabled else '❌ Désactivé'}\n"
    msg += f"  Montant: {trading_config.sniper_position_sol} SOL\n"
    msg += f"  Liq min: ${trading_config.sniper_min_liquidity}\n"
    msg += f"  MC max: ${trading_config.sniper_max_mc:,}\n\n"

    msg += f"*4 RÈGLES DE SORTIE:*\n\n"
    ts_status = '✅' if trading_config.time_stop_enabled else '❌'
    msg += f"  {ts_status} *RÈGLE 1 — Time Stop*\n"
    msg += f"     > {trading_config.time_stop_minutes:.0f} min sans +{trading_config.time_stop_min_profit:.0f}% → sortie\n\n"
    msg += f"  ✅ *RÈGLE 2 — SL Universel*\n"
    msg += f"     {trading_config.stop_loss_pct:.0f}% depuis l'achat → sortie TOUJOURS\n\n"
    msg += f"  ✅ *RÈGLE 3 — Trailing Stop*\n"
    msg += f"     Dès +{trading_config.trailing_activation_pct:.0f}% atteint → SL à -{trading_config.trailing_stop_pct:.0f}% du max\n\n"
    ms_status = '✅' if trading_config.momentum_stop_enabled else '❌'
    msg += f"  {ms_status} *RÈGLE 4 — Momentum Stop*\n"
    msg += f"     Prix sous ATH -{trading_config.momentum_stop_drop_pct:.0f}% + volume chute → sortie\n\n"
    msg += f"  🎯 TP: +{trading_config.take_profit_pct}% | ⚡ Slippage: {trading_config.slippage_bps/100}%\n\n"
    msg += f"*Auto Trading:* {'🟢 ACTIF' if auto_trading_enabled else '🔴 INACTIF'}"
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def set_tp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /set_tp <pct>"""
    if not context.args:
        await update.message.reply_text("Usage: /set\\_tp 50 (pour +50%)", parse_mode=ParseMode.MARKDOWN)
        return
    try:
        tp = float(context.args[0])
        trading_config.take_profit_pct = tp
        save_trading_config(trading_config)
        if circuit_breaker:
            circuit_breaker.update_config(take_profit_pct=tp)
        await update.message.reply_text(f"✅ Take Profit mis à jour: +{tp}%")
    except ValueError:
        await update.message.reply_text("❌ Valeur invalide")


async def set_sl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /set_sl <pct>"""
    if not context.args:
        await update.message.reply_text("Usage: /set\\_sl 30 (pour -30%)", parse_mode=ParseMode.MARKDOWN)
        return
    try:
        sl = float(context.args[0])
        trading_config.stop_loss_pct = -abs(sl)
        save_trading_config(trading_config)
        if circuit_breaker:
            circuit_breaker.update_config(stop_loss_pct=-abs(sl))
        await update.message.reply_text(f"✅ Stop Loss mis à jour: {trading_config.stop_loss_pct}%")
    except ValueError:
        await update.message.reply_text("❌ Valeur invalide")


async def set_trailing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /set_trailing <activation_pct> - Configurer le trailing stop"""
    if not context.args:
        msg = "📉 *Trailing Stop Dynamique*\n\n"
        msg += f"Activation: dès +{trading_config.trailing_activation_pct}% de profit\n\n"
        msg += "*Paliers automatiques:*\n"
        msg += "• Profit +5% à +10% → trailing 12%\n"
        msg += "• Profit +10% à +20% → trailing 10%\n"
        msg += "• Profit +20% à +50% → trailing 8%\n"
        msg += "• Profit +50% à +100% → trailing 6%\n"
        msg += "• Profit > +100% → trailing 5%\n\n"
        msg += "Usage: /set\\_trailing 5 (activer dès +5%)"
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        return
    try:
        activation = float(context.args[0])
        if activation < 1 or activation > 50:
            await update.message.reply_text("❌ Valeur entre 1 et 50%")
            return
        trading_config.trailing_activation_pct = activation
        save_trading_config(trading_config)
        if circuit_breaker:
            circuit_breaker.update_config(trailing_activation_pct=activation)
        await update.message.reply_text(
            f"✅ Trailing Stop mis à jour!\n"
            f"📉 Activation: dès +{activation}% de profit\n"
            f"Les paliers dynamiques s'appliquent automatiquement."
        )
    except ValueError:
        await update.message.reply_text("❌ Valeur invalide")


async def set_timestop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /set_timestop - Configurer le time stop"""
    if not context.args:
        ts_status = '✅ Actif' if trading_config.time_stop_enabled else '❌ Désactivé'
        msg = f"⏰ *Time Stop*\n\n"
        msg += f"Statut: {ts_status}\n"
        msg += f"Durée: {trading_config.time_stop_minutes:.0f} minutes\n"
        msg += f"Seuil: +{trading_config.time_stop_min_profit}% de profit\n\n"
        msg += "*Règle:* Si un trade dure > {:.0f} min ".format(trading_config.time_stop_minutes)
        msg += f"et PnL < +{trading_config.time_stop_min_profit}% \u2192 VENTE FORC\u00c9E\n\n"
        msg += "*Commandes:*\n"
        msg += "/set\\_timestop off \u2014 D\u00e9sactiver\n"
        msg += "/set\\_timestop on \u2014 Activer\n"
        msg += "/set\\_timestop 15 \u2014 D\u00e9lai en minutes\n"
        msg += "/set\\_timestop 15 5 \u2014 D\u00e9lai + seuil profit"
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        return

    arg = context.args[0].lower()
    if arg == "off":
        trading_config.time_stop_enabled = False
        save_trading_config(trading_config)
        await update.message.reply_text("❌ Time Stop d\u00e9sactiv\u00e9")
        return
    elif arg == "on":
        trading_config.time_stop_enabled = True
        save_trading_config(trading_config)
        await update.message.reply_text(
            f"✅ Time Stop activ\u00e9 ({trading_config.time_stop_minutes:.0f} min, seuil +{trading_config.time_stop_min_profit}%)"
        )
        return

    try:
        minutes = float(arg)
        if minutes < 5 or minutes > 120:
            await update.message.reply_text("❌ Valeur entre 5 et 120 minutes")
            return
        trading_config.time_stop_minutes = minutes
        trading_config.time_stop_enabled = True

        # Optionnel: seuil de profit minimum
        if len(context.args) >= 2:
            min_profit = float(context.args[1])
            if 0 <= min_profit <= 50:
                trading_config.time_stop_min_profit = min_profit

        save_trading_config(trading_config)
        if circuit_breaker:
            circuit_breaker.update_config(
                time_stop_minutes=trading_config.time_stop_minutes,
                time_stop_min_profit=trading_config.time_stop_min_profit,
                time_stop_enabled=trading_config.time_stop_enabled,
            )
        await update.message.reply_text(
            f"✅ Time Stop mis à jour!\n"
            f"⏰ Délai: {trading_config.time_stop_minutes:.0f} min\n"
            f"🎯 Seuil: +{trading_config.time_stop_min_profit}% (en dessous = vente forcée)"
        )
    except ValueError:
        await update.message.reply_text("❌ Valeur invalide. Usage: /set\\_timestop 15 5", parse_mode=ParseMode.MARKDOWN)


async def set_size(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /set_size <sol>"""
    if not context.args:
        await update.message.reply_text("Usage: /set\\_size 0.5 (pour 0.5 SOL par trade)", parse_mode=ParseMode.MARKDOWN)
        return
    try:
        size = float(context.args[0])
        trading_config.position_size_sol = size
        trading_config.sniper_position_sol = size
        save_trading_config(trading_config)
        await update.message.reply_text(f"✅ Taille de position: {size} SOL")
    except ValueError:
        await update.message.reply_text("❌ Valeur invalide")


async def buy_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /buy <adresse> - Achat manuel"""
    if not wallet.keypair or not trading_engine:
        await update.message.reply_text("❌ Wallet non configuré.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /buy `<adresse_token>`", parse_mode=ParseMode.MARKDOWN)
        return

    token_address = context.args[0]
    await update.message.reply_text(f"🔄 Achat en cours de `{token_address[:12]}...`", parse_mode=ParseMode.MARKDOWN)

    # Analyser le token
    analysis = api.analyze_token(token_address)
    if not analysis:
        await update.message.reply_text("❌ Token non trouvé.")
        return

    # Exécuter l'achat
    result = trading_engine.execute_buy(analysis, "manual")
    if result:
        # Enregistrer dans le CircuitBreaker
        if circuit_breaker:
            entry_price = float(analysis.get("price_usd", 0) or 0)
            circuit_breaker.open_position(
                token_address=token_address,
                token_symbol=analysis.get("symbol", "???"),
                entry_price=entry_price,
            )
        if capital_watchdog:
            capital_watchdog.register_position(
                token_address=token_address,
                token_symbol=analysis.get("symbol", "???"),
                strategy="manual",
                amount_sol=result['amount_sol'],
                entry_price=float(analysis.get("price_usd", 0) or 0),
            )
        msg = f"✅ *Achat réussi !*\n\n"
        msg += f"🪙 {analysis['name']} (${analysis['symbol']})\n"
        msg += f"💵 Montant: {result['amount_sol']} SOL\n"
        msg += f"📋 TX: `{result['tx_signature'][:20]}...`\n"
        msg += f"🔗 [Voir TX](https://solscan.io/tx/{result['tx_signature']})"
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
    else:
        await update.message.reply_text("❌ Achat échoué. Vérifiez le solde et les logs.")


async def sell_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /sell <adresse> - Vente manuelle"""
    if not wallet.keypair or not trading_engine:
        await update.message.reply_text("❌ Wallet non configuré.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /sell `<adresse_token>`", parse_mode=ParseMode.MARKDOWN)
        return

    token_address = context.args[0]
    position = positions.get_position(token_address)
    if not position:
        await update.message.reply_text("❌ Aucune position ouverte pour ce token.")
        return

    await update.message.reply_text(f"🔄 Vente en cours de {position.token_name}...")

    result = trading_engine.execute_sell(position, "Vente manuelle")
    if result:
        # Retirer du CircuitBreaker
        if circuit_breaker:
            circuit_breaker.close_position(token_address)
            if capital_watchdog:
                capital_watchdog.unregister_position(token_address)
        msg = f"✅ *Vente réussie !*\n\n"
        msg += f"🪙 {position.token_name} (${position.token_symbol})\n"
        msg += f"📈 PnL: {result['pnl_pct']:+.1f}%\n"
        msg += f"📋 TX: `{result['tx_signature'][:20]}...`\n"
        msg += f"🔗 [Voir TX](https://solscan.io/tx/{result['tx_signature']})"
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN, disable_web_page_preview=True)
    else:
        await update.message.reply_text("❌ Vente échouée.")


async def sell_all_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /sell_all - Vendre TOUS les tokens du wallet (scan blockchain)"""
    if not trading_engine or not wallet.keypair:
        await update.message.reply_text("❌ Trading non initialisé.")
        return

    await update.message.reply_text("🔄 Scan du wallet et vente de tous les tokens en cours...")

    # Scanner TOUS les tokens réels dans le wallet
    IGNORE_MINTS = {
        "So11111111111111111111111111111111111111112",   # Wrapped SOL
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", # USDC
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
    }

    all_tokens = wallet.get_all_token_balances()
    tokens_to_sell = [t for t in all_tokens if t["mint"] not in IGNORE_MINTS]

    if not tokens_to_sell:
        await update.message.reply_text("📋 Aucun token à vendre dans le wallet.")
        return

    await update.message.reply_text(f"💰 {len(tokens_to_sell)} tokens trouvés. Vente en cours...")

    sold = 0
    failed = 0
    for token_info in tokens_to_sell:
        mint = token_info["mint"]
        raw_amount = token_info["raw_amount"]
        try:
            # Vendre via Jupiter
            tx_sig = swap_engine.sell_token(mint, raw_amount)
            if tx_sig:
                sold += 1
                # Retirer du CircuitBreaker
                if circuit_breaker:
                    circuit_breaker.close_position(mint)
                    if capital_watchdog:
                        capital_watchdog.unregister_position(mint)
                # Fermer la position si elle existe dans le tracking
                if mint in positions.positions:
                    pos = positions.positions[mint]
                    trading_engine.trade_history.append({
                        "type": "SELL",
                        "reason": "Sell All",
                        "token": pos.token_symbol,
                        "token_address": mint,
                        "pnl_pct": pos.pnl_pct,
                        "amount_sol_invested": pos.amount_sol_invested,
                        "tx_signature": tx_sig,
                        "timestamp": datetime.utcnow().isoformat(),
                    })
                    positions.close_position(mint)
                else:
                    trading_engine.trade_history.append({
                        "type": "SELL",
                        "reason": "Sell All (orphan)",
                        "token": mint[:8],
                        "token_address": mint,
                        "pnl_pct": 0,
                        "amount_sol_invested": 0.05,
                        "tx_signature": tx_sig,
                        "timestamp": datetime.utcnow().isoformat(),
                    })
                trading_engine._save_history()
            else:
                failed += 1
        except Exception as e:
            logger.error(f"Erreur vente {mint[:12]}...: {e}")
            failed += 1
        await asyncio.sleep(3)  # Pause entre les ventes

    msg = f"✅ *Vente terminée*\n\n"
    msg += f"💰 Vendus: {sold}/{len(tokens_to_sell)}\n"
    if failed:
        msg += f"❌ Échoués: {failed}\n"
    balance = wallet.get_sol_balance()
    msg += f"\n💵 Nouveau solde: {balance:.4f} SOL"
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


# ============================================================
# COPY TRADING - CALLBACKS & COMMANDES
# ============================================================

async def on_copy_trade_signal(signal: CopyTradeSignal):
    """
    Callback appelé quand un smart wallet fait un trade.
    Copie automatiquement l'achat si les conditions sont remplies.
    """
    global seen_tokens, sl_blacklist

    if not auto_trading_enabled or not trading_engine:
        return

    # On ne copie que les achats (pas les ventes - le TP/SL gère les ventes)
    if signal.action != "buy":
        logger.info(f"[COPY] Signal SELL ignoré (TP/SL gère les ventes): {signal.token_symbol}")
        return

    token_mint = signal.token_mint

    # Vérifier si déjà en position ou blacklisté
    if token_mint in positions.positions:
        logger.info(f"[COPY] Déjà en position sur {signal.token_symbol}, skip")
        return
    if token_mint in sl_blacklist and time.time() < sl_blacklist[token_mint]:
        logger.info(f"[COPY] Token blacklisté (SL): {signal.token_symbol}, skip")
        return
    if token_mint in seen_tokens:
        return

    # Vérification sécurité rapide
    if security_checker:
        is_safe, sec_reason = security_checker.quick_check(token_mint)
        if not is_safe:
            logger.info(f"[COPY] Token rejeté (sécurité): {signal.token_symbol} - {sec_reason}")
            seen_tokens.add(token_mint)
            return

    # Analyser le token
    analysis = api.analyze_token(token_mint)
    if not analysis:
        logger.warning(f"[COPY] Impossible d'analyser {signal.token_symbol}")
        return

    # Exécuter l'achat (copy trade)
    result = trading_engine.execute_buy(analysis, "copy_trade")
    if result:
        seen_tokens.add(token_mint)
        # Enregistrer dans le CircuitBreaker
        if circuit_breaker:
            entry_price = float(analysis.get("price_usd", 0) or 0)
            circuit_breaker.open_position(
                token_address=token_mint,
                token_symbol=analysis.get("symbol", signal.token_symbol),
                entry_price=entry_price,
            )
        if capital_watchdog:
            capital_watchdog.register_position(
                token_address=token_mint,
                token_symbol=analysis.get("symbol", signal.token_symbol),
                strategy="copy_trade",
                amount_sol=result['amount_sol'],
                entry_price=float(analysis.get("price_usd", 0) or 0),
            )
        # Notifier via Telegram
        msg = f"📋 *COPY TRADE*\n\n"
        msg += f"👤 Wallet: {signal.wallet_label}\n"
        msg += f"🪙 {analysis['name']} (${analysis['symbol']})\n"
        msg += f"💵 {result['amount_sol']} SOL\n"
        msg += f"📊 MC: ${analysis.get('market_cap', 0):,.0f}\n"
        msg += f"💧 Liq: ${analysis.get('liquidity_usd', 0):,.0f}\n"
        msg += f"🔗 [TX](https://solscan.io/tx/{result['tx_signature']})\n"
        msg += f"👁 [Wallet source](https://solscan.io/tx/{signal.tx_signature})"
        for chat_id in subscribers:
            try:
                from telegram import Bot
                bot = Bot(token=TELEGRAM_BOT_TOKEN)
                await bot.send_message(
                    chat_id=chat_id, text=msg,
                    parse_mode=ParseMode.MARKDOWN,
                    disable_web_page_preview=True
                )
            except Exception as e:
                logger.error(f"Erreur notification copy trade: {e}")

        # Ajouter au monitoring WebSocket
        if price_monitor:
            try:
                await price_monitor.add_token(token_mint)
            except:
                pass

        logger.info(f"✅ COPY TRADE exécuté: {signal.token_symbol} via {signal.wallet_label}")
    else:
        logger.warning(f"❌ COPY TRADE échoué: {signal.token_symbol}")


async def copy_wallets_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /copy_wallets - Voir les wallets suivis"""
    if not copy_trader:
        await update.message.reply_text("❌ Copy Trading DÉSACTIVÉ (performances négatives).")
        return

    wallets = copy_trader.smart_wallets
    if not wallets:
        await update.message.reply_text("📋 Aucun wallet suivi. Ajoutez-en avec /copy\\_add")
        return

    msg = "📋 *Smart Wallets Suivis*\n\n"
    for i, w in enumerate(wallets, 1):
        status = "✅" if w.active else "❌"
        msg += f"{i}. {status} *{w.label}*\n"
        msg += f"   📍 `{w.address[:12]}...{w.address[-6:]}`\n"
        msg += f"   📊 WR: {w.win_rate:.0f}% | ROI: {w.avg_roi:.0f}%\n"
        msg += f"   📋 Trades copiés: {w.trades_copied}\n\n"

    msg += f"\n💡 Commandes: /copy\\_add, /copy\\_remove, /copy\\_history"
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def copy_add_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /copy_add <adresse> [label] - Ajouter un wallet à suivre"""
    if not copy_trader:
        await update.message.reply_text("❌ Copy Trading DÉSACTIVÉ (performances négatives).")
        return

    if not context.args:
        await update.message.reply_text(
            "Usage: /copy\\_add `<adresse_wallet>` `[label]`\n\n"
            "Exemple: /copy\\_add 5Q544...4j1 TopTrader",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    address = context.args[0]
    label = " ".join(context.args[1:]) if len(context.args) > 1 else f"Wallet {len(copy_trader.smart_wallets) + 1}"

    if len(address) < 32:
        await update.message.reply_text("❌ Adresse invalide (trop courte)")
        return

    success = copy_trader.add_wallet(address, label)
    if success:
        msg = f"✅ *Wallet ajouté !*\n\n"
        msg += f"👤 Label: {label}\n"
        msg += f"📍 `{address}`\n\n"
        msg += f"Le bot copiera automatiquement ses achats de meme coins."
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        # Redémarrer le monitoring pour inclure le nouveau wallet
        if copy_trader.running:
            await copy_trader.stop_monitoring()
            await copy_trader.start_monitoring()
    else:
        await update.message.reply_text("❌ Ce wallet est déjà dans la liste.")


async def copy_remove_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /copy_remove <adresse> - Retirer un wallet"""
    if not copy_trader:
        await update.message.reply_text("❌ Copy Trading DÉSACTIVÉ (performances négatives).")
        return

    if not context.args:
        await update.message.reply_text(
            "Usage: /copy\\_remove `<adresse_wallet>`",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    address = context.args[0]
    success = copy_trader.remove_wallet(address)
    if success:
        await update.message.reply_text(f"✅ Wallet retiré: `{address[:12]}...`", parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text("❌ Wallet non trouvé dans la liste.")


async def copy_history_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /copy_history - Historique des copy trades"""
    if not copy_trader:
        await update.message.reply_text("❌ Copy Trading DÉSACTIVÉ (performances négatives).")
        return

    history = copy_trader.copy_history
    if not history:
        await update.message.reply_text("📋 Aucun copy trade effectué.")
        return

    msg = "📋 *Historique Copy Trades* (derniers 10)\n\n"
    for trade in history[-10:]:
        emoji = "🟢" if trade["action"] == "buy" else "🔴"
        msg += f"{emoji} {trade['action'].upper()} {trade['token']}\n"
        msg += f"   👤 {trade['wallet']}\n"
        msg += f"   💵 {trade['amount_sol']:.4f} SOL\n"
        msg += f"   📅 {trade['timestamp'][:16]}\n\n"

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


# ============================================================
# CAPITAL WATCHDOG — SURVEILLANCE SANTÉ DU CAPITAL (5s)
# ============================================================

async def watchdog_check_job(context: ContextTypes.DEFAULT_TYPE):
    """
    Job watchdog (5s): vérifie que CHAQUE position est activement surveillée.
    Alerte immédiatement si un gap de monitoring est détecté.
    TOURNE TOUJOURS, même si auto_trading est off (protège le capital).
    """
    if not capital_watchdog:
        return

    # Vérifier la santé de toutes les positions
    alerts = capital_watchdog.check()

    if not alerts:
        return

    for alert in alerts:
        # Envoyer l'alerte Telegram
        level_emoji = {
            "warn": "⚠️",
            "critical": "🚨",
            "emergency": "💀",
        }.get(alert["level"], "❓")

        msg = f"{level_emoji} *WATCHDOG CAPITAL*\n\n"
        msg += alert["message"]

        for chat_id in subscribers:
            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=msg,
                    parse_mode=ParseMode.MARKDOWN,
                )
            except:
                pass

        # VENTE D'URGENCE si emergency
        if alert["action"] == "emergency_sell" and alert["token_address"] != "GLOBAL":
            token_addr = alert["token_address"]
            if token_addr in positions.positions:
                pos = positions.positions[token_addr]
                try:
                    result = trading_engine.execute_sell(
                        pos, f"💀 VENTE D'URGENCE WATCHDOG (non surveillé {alert['gap_seconds']:.0f}s)"
                    )
                    if result:
                        if pnl_tracker:
                            pnl_tracker.record_trade(
                                token_address=pos.token_address,
                                token_symbol=pos.token_symbol,
                                strategy=pos.strategy,
                                entry_time=pos.entry_time,
                                amount_sol=pos.amount_sol_invested,
                                pnl_pct=pos.pnl_pct,
                                exit_reason="WATCHDOG_EMERGENCY",
                            )
                        if circuit_breaker:
                            circuit_breaker.close_position(token_addr)
                        if capital_watchdog:
                            capital_watchdog.unregister_position(token_addr)
                        if price_monitor:
                            try:
                                await price_monitor.remove_token(token_addr)
                            except:
                                pass
                        # Notifier la vente d'urgence
                        sell_msg = f"💀 *VENTE D'URGENCE*\n\n"
                        sell_msg += f"🪙 {pos.token_name} (${pos.token_symbol})\n"
                        sell_msg += f"📈 PnL: {pos.pnl_pct:+.1f}%\n"
                        sell_msg += f"📝 Raison: Non surveillé depuis {alert['gap_seconds']:.0f}s\n"
                        sell_msg += f"🔗 [TX](https://solscan.io/tx/{result['tx_signature']})"
                        for chat_id in subscribers:
                            try:
                                await context.bot.send_message(
                                    chat_id=chat_id,
                                    text=sell_msg,
                                    parse_mode=ParseMode.MARKDOWN,
                                    disable_web_page_preview=True,
                                )
                            except:
                                pass
                except Exception as e:
                    logger.error(f"[Watchdog] Erreur vente d'urgence {token_addr[:12]}: {e}")


async def watchdog_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /watchdog - Afficher la santé du capital"""
    if not capital_watchdog:
        await update.message.reply_text("❌ Capital Watchdog non initialisé.")
        return

    stats = capital_watchdog.get_stats()
    summary = capital_watchdog.get_health_summary()

    msg = "🛡️ *Capital Watchdog*\n\n"
    msg += f"{summary}\n\n"
    msg += f"*Stats:*\n"
    msg += f"  🔍 Checks: {stats['total_checks']}\n"
    msg += f"  ⚠️ Warnings: {stats['warnings_sent']}\n"
    msg += f"  🚨 Critiques: {stats['critical_alerts']}\n"
    msg += f"  💀 Ventes urgence: {stats['emergency_sells']}\n"
    msg += f"  ⏱️ Max gap vu: {stats['max_gap_seen']:.1f}s\n"

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


# ============================================================
# SNIPER MONITOR — POLLING 3s (UNIQUEMENT positions SNIPER)
# ============================================================

async def sniper_monitor_job(context: ContextTypes.DEFAULT_TYPE):
    """
    Job ultra-rapide (3s): vérifier UNIQUEMENT les positions SNIPER.
    Le SL universel (-25%) doit être réactif sur les tokens frais.
    TOURNE TOUJOURS, même si auto_trading est off (protège le capital).
    """
    if not trading_engine or not circuit_breaker:
        return

    sniper_positions = [p for p in positions.get_open_positions() if p.strategy == "sniper"]
    if not sniper_positions:
        return

    for pos in sniper_positions:
        try:
            # Récupérer le prix actuel via DexScreener
            analysis = api.analyze_token(pos.token_address)
            current_price = float(analysis.get("price_usd", 0) or 0) if analysis else 0

            # Si prix indisponible, utiliser le dernier prix connu
            # cb.check() est TOUJOURS appelé (Time Stop doit agir même sans prix frais)
            if current_price <= 0:
                current_price = pos.current_price  # dernier prix connu
                if current_price <= 0:
                    current_price = pos.entry_price_usd * 0.01  # assumer -99% si jamais vu
                logger.warning(f"[SNIPER 3s] Prix indisponible pour {pos.token_symbol}, "
                              f"utilise fallback=${current_price:.8f}")
            else:
                # Mettre à jour la position avec le prix frais
                positions.update_position(pos.token_address, current_price)

            # Heartbeat watchdog (cette position est surveillée)
            if capital_watchdog:
                capital_watchdog.heartbeat(pos.token_address, current_price)

            # Vérifier via CircuitBreaker centralisé
            cb_action = circuit_breaker.check(pos.token_address, current_price)

            if cb_action.should_sell:
                # Partial TP: vendre 50% seulement, garder la position ouverte
                is_partial = cb_action.rule == "partial_take_profit"
                sell_pct = 50.0 if is_partial else 100.0

                result = trading_engine.execute_sell(pos, cb_action.reason, sell_pct=sell_pct)
                if result:
                    if is_partial:
                        # Partial TP: ne PAS fermer la position, le trailing gère la suite
                        logger.info(f"[SNIPER 3s] Partial TP 50% exécuté pour {pos.token_symbol}")
                    else:
                        # Vente totale: fermer tout
                        if pnl_tracker:
                            pnl_tracker.record_trade(
                                token_address=pos.token_address,
                                token_symbol=pos.token_symbol,
                                strategy=pos.strategy,
                                entry_time=pos.entry_time,
                                amount_sol=pos.amount_sol_invested,
                                pnl_pct=pos.pnl_pct,
                                exit_reason=cb_action.reason,
                            )
                        if position_sizer:
                            position_sizer.record_result(pos.pnl_pct > 0)
                        # Daily PnL Guard
                        if daily_pnl_guard:
                            pnl_sol = pos.amount_sol_invested * (pos.pnl_pct / 100.0)
                            daily_pnl_guard.record_trade(pnl_sol, is_stop_loss=(cb_action.rule == "stop_loss"))
                        if correlation_filter:
                            correlation_filter.unregister_position(pos.token_address)
                        if liquidity_guard:
                            liquidity_guard.unregister_position(pos.token_address)
                        if circuit_breaker:
                            circuit_breaker.close_position(pos.token_address)
                            if capital_watchdog:
                                capital_watchdog.unregister_position(pos.token_address)
                        if post_trade_analyzer:
                            post_trade_analyzer.record_trade_exit(
                                token_address=pos.token_address,
                                token_symbol=pos.token_symbol,
                                token_name=pos.token_name,
                                strategy=pos.strategy,
                                entry_time=pos.entry_time,
                                entry_price=pos.entry_price_usd,
                                exit_price=pos.current_price,
                                exit_pnl_pct=pos.pnl_pct,
                                exit_reason=cb_action.reason,
                                highest_price=pos.highest_price,
                                amount_sol=pos.amount_sol_invested,
                            )
                        # Blacklister si perte
                        if pos.pnl_pct < 0:
                            sl_blacklist[pos.token_address] = time.time() + SL_BLACKLIST_DURATION
                            _save_blacklist()
                            seen_tokens.add(pos.token_address)
                    # Retirer du WS seulement si vente totale
                    if not is_partial and price_monitor:
                        try:
                            await price_monitor.remove_token(pos.token_address)
                        except:
                            pass
                    # Notifier
                    pnl_emoji = '✅' if pos.pnl_pct > 0 else '❌'
                    sell_type = "PARTIAL TP 50%" if is_partial else "VENTE SNIPER (3s)"
                    msg = f"{pnl_emoji} *{sell_type}*\n\n"
                    msg += f"🪙 {pos.token_name} (${pos.token_symbol})\n"
                    msg += f"📈 PnL: {pos.pnl_pct:+.1f}%\n"
                    msg += f"📝 Raison: {cb_action.reason}\n"
                    if is_partial:
                        msg += f"📊 50% vendu, 50% court avec trailing\n"
                    msg += f"🔗 [TX](https://solscan.io/tx/{result['tx_signature']})"
                    for chat_id in subscribers:
                        try:
                            await context.bot.send_message(
                                chat_id=chat_id, text=msg,
                                parse_mode=ParseMode.MARKDOWN,
                                disable_web_page_preview=True
                            )
                        except:
                            pass

            await asyncio.sleep(0.5)  # Rate limit DexScreener
        except Exception as e:
            logger.error(f"Erreur sniper_monitor {pos.token_address[:12]}: {e}")


# ============================================================
# SCAN AUTOMATIQUE + TRADING
# ============================================================

async def position_monitor_job(context: ContextTypes.DEFAULT_TYPE):
    """Job backup: vérifier les positions toutes les 15s (non-sniper + LP + post-trade)"""
    if not trading_engine:
        return

    try:
        await check_positions(context)
    except Exception as e:
        logger.error(f"Erreur position_monitor_job: {e}")

    # === MONITORING LP (Protection anti-rug post-achat) ===
    try:
        if liquidity_guard and liquidity_guard.snapshots:
            lp_actions = liquidity_guard.check_all_positions()
            for action in lp_actions:
                token_addr = action["token"]
                pos = positions.get_position(token_addr)
                if not pos:
                    continue

                if action["type"] == "emergency_sell":
                    # VENTE D'URGENCE - LP effondrée (rug pull en cours)
                    logger.warning(f"🚨 VENTE D'URGENCE LP: {pos.token_name}")
                    # Utiliser le retry avec slippage progressif
                    raw_amount = int(pos.amount_tokens)
                    tx_sig = liquidity_guard.emergency_sell_with_retry(
                        swap_engine, pos.token_address, raw_amount
                    )
                    if tx_sig:
                        positions.close_position(token_addr)
                        if correlation_filter:
                            correlation_filter.unregister_position(token_addr)
                        liquidity_guard.unregister_position(token_addr)
                        # Blacklister le token 24h
                        sl_blacklist[token_addr] = time.time() + 86400
                        _save_blacklist()
                        # Notifier
                        msg = f"🚨 *VENTE D'URGENCE (Rug Pull)*\n\n"
                        msg += f"🪙 {pos.token_name} (${pos.token_symbol})\n"
                        msg += f"💧 LP effondrée: {action['drop_pct']:.0f}%\n"
                        msg += f"💰 ${action['entry_liq']:.0f} \u2192 ${action['current_liq']:.0f}\n"
                        msg += f"🔗 [TX](https://solscan.io/tx/{tx_sig})"
                        for chat_id in subscribers:
                            try:
                                await context.bot.send_message(
                                    chat_id=chat_id, text=msg,
                                    parse_mode=ParseMode.MARKDOWN,
                                    disable_web_page_preview=True
                                )
                            except:
                                pass
                    else:
                        # Échec de vente - notifier quand même
                        msg = f"🚨 *ALERTE: Impossible de vendre (illiquide)*\n\n"
                        msg += f"🪙 {pos.token_name} (${pos.token_symbol})\n"
                        msg += f"💧 LP: ${action['current_liq']:.0f}\n"
                        msg += f"⚠️ Token potentiellement bloqué"
                        for chat_id in subscribers:
                            try:
                                await context.bot.send_message(
                                    chat_id=chat_id, text=msg,
                                    parse_mode=ParseMode.MARKDOWN
                                )
                            except:
                                pass

                elif action["type"] == "alert":
                    # Alerte LP en chute (pas encore urgence)
                    msg = f"⚠️ *ALERTE LP*\n\n"
                    msg += f"🪙 {pos.token_name} (${pos.token_symbol})\n"
                    msg += f"💧 Liquidité en chute: -{action['drop_pct']:.0f}%\n"
                    msg += f"💰 ${action['entry_liq']:.0f} \u2192 ${action['current_liq']:.0f}\n"
                    msg += f"👀 Surveillance renforcée..."
                    for chat_id in subscribers:
                        try:
                            await context.bot.send_message(
                                chat_id=chat_id, text=msg,
                                parse_mode=ParseMode.MARKDOWN
                            )
                        except:
                            pass
    except Exception as e:
        logger.error(f"Erreur LP monitor: {e}")

    # === POST-TRADE ANALYSIS (checks post-vente) ===
    try:
        if post_trade_analyzer and post_trade_analyzer.pending_checks:
            completed = post_trade_analyzer.run_pending_checks()
            for result in completed:
                # Notifier quand une analyse post-trade est complète
                verdict_emoji = {
                    "hold": "📈 Tu aurais dû garder",
                    "perfect": "✅ Vente parfaite",
                    "good": "👍 Bonne vente",
                    "neutral": "➖ Neutre",
                    "sell_earlier": "⚠️ Vendre plus tôt",
                }.get(result["verdict"], "❓")

                msg = f"📊 *POST-TRADE: {result['token_symbol']}*\n\n"
                msg += f"Vendu à: {result['exit_pnl']:+.1f}%\n"
                msg += f"1h après: {result['pnl_1h_after']:+.1f}%\n"
                if result['post_max'] > result['exit_pnl']:
                    msg += f"Max post-vente: {result['post_max']:+.1f}%\n"
                if result['missed'] > 0:
                    msg += f"Profit raté: {result['missed']:.0f}%\n"
                msg += f"\n{verdict_emoji}"

                for chat_id in subscribers:
                    try:
                        await context.bot.send_message(
                            chat_id=chat_id, text=msg,
                            parse_mode=ParseMode.MARKDOWN
                        )
                    except:
                        pass
    except Exception as e:
        logger.error(f"Erreur post-trade analysis: {e}")


async def auto_trading_job(context: ContextTypes.DEFAULT_TYPE):
    """Job automatique: scan pour nouvelles opportunités (toutes les 45s)"""
    global auto_trading_enabled

    if not auto_trading_enabled or not trading_engine:
        return

    try:
        await scan_and_trade(context)
    except Exception as e:
        logger.error(f"Erreur auto_trading_job: {e}")


async def check_positions(context: ContextTypes.DEFAULT_TYPE):
    """Vérifier les positions ouvertes pour TP/SL (exclut SNIPER, géré par job 3s)"""
    non_sniper = [p for p in positions.get_open_positions() if p.strategy != "sniper"]
    for pos in non_sniper:
        try:
            # Mettre à jour le prix
            analysis = api.analyze_token(pos.token_address)
            current_price = float(analysis.get("price_usd", 0) or 0) if analysis else 0

            # Si prix indisponible, utiliser le dernier prix connu
            # cb.check() est TOUJOURS appelé (Time Stop doit agir même sans prix frais)
            if current_price <= 0:
                current_price = pos.current_price  # dernier prix connu
                if current_price <= 0:
                    current_price = pos.entry_price_usd * 0.01  # assumer -99% si jamais vu
                logger.warning(f"[CHECK 15s] Prix indisponible pour {pos.token_symbol}, "
                              f"utilise fallback=${current_price:.8f}")
            else:
                positions.update_position(pos.token_address, current_price)

            # Heartbeat watchdog
            if capital_watchdog:
                capital_watchdog.heartbeat(pos.token_address, current_price)

            # Vérifier si on doit vendre via CircuitBreaker centralisé (SEUL chemin de décision)
            if not circuit_breaker:
                continue
            cb_action = circuit_breaker.check(pos.token_address, current_price)
            if cb_action.should_sell:
                reason = cb_action.reason
                result = trading_engine.execute_sell(pos, reason)
                if result:
                    # Enregistrer dans le PnL tracker
                    if pnl_tracker:
                        pnl_tracker.record_trade(
                            token_address=pos.token_address,
                            token_symbol=pos.token_symbol,
                            strategy=pos.strategy,
                            entry_time=pos.entry_time,
                            amount_sol=pos.amount_sol_invested,
                            pnl_pct=pos.pnl_pct,
                            exit_reason=reason,
                        )
                    # Mettre à jour le position sizer (streak)
                    if position_sizer:
                        position_sizer.record_result(pos.pnl_pct > 0)
                    # Daily PnL Guard
                    if daily_pnl_guard:
                        pnl_sol = pos.amount_sol_invested * (pos.pnl_pct / 100.0)
                        daily_pnl_guard.record_trade(pnl_sol, is_stop_loss=(cb_action.rule == "stop_loss"))

                    # Retirer du filtre de corrélation
                    if correlation_filter:
                        correlation_filter.unregister_position(pos.token_address)

                    # Retirer du monitoring LP
                    if liquidity_guard:
                        liquidity_guard.unregister_position(pos.token_address)

                    # Retirer du CircuitBreaker
                    if circuit_breaker:
                        circuit_breaker.close_position(pos.token_address)
                        if capital_watchdog:
                            capital_watchdog.unregister_position(pos.token_address)

                    # Enregistrer pour analyse post-trade
                    if post_trade_analyzer:
                        post_trade_analyzer.record_trade_exit(
                            token_address=pos.token_address,
                            token_symbol=pos.token_symbol,
                            token_name=pos.token_name,
                            strategy=pos.strategy,
                            entry_time=pos.entry_time,
                            entry_price=pos.entry_price_usd,
                            exit_price=pos.current_price,
                            exit_pnl_pct=pos.pnl_pct,
                            exit_reason=reason,
                            highest_price=pos.highest_price,
                            amount_sol=pos.amount_sol_invested,
                        )

                    # Postmortem Tracker (thread dédié 30min)
                    if result and pos.entry_price_usd > 0:
                        try:
                            helius_key = os.environ.get("HELIUS_API_KEY", "")
                            tg_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
                            tg_chat = str(subscribers[0]) if subscribers else ""
                            start_postmortem_thread(
                                trade_record=result,
                                entry_price_usd=pos.entry_price_usd,
                                helius_api_key=helius_key,
                                telegram_bot_token=tg_token,
                                telegram_chat_id=tg_chat,
                            )
                        except Exception as e:
                            logger.error(f"Erreur postmortem thread: {e}")

                    # Si c'est un Stop Loss, blacklister le token
                    if pos.pnl_pct < 0:
                        sl_blacklist[pos.token_address] = time.time() + SL_BLACKLIST_DURATION
                        _save_blacklist()
                        seen_tokens.add(pos.token_address)
                        logger.info(f"🚫 Blacklisté après SL: {pos.token_name} (1h)")

                    # Détecter un RUG PULL (-80%+) et blacklister le créateur
                    if pos.pnl_pct < -80 and security_checker:
                        logger.warning(f"🚨 RUG PULL DÉTECTÉ: {pos.token_name} ({pos.pnl_pct:.0f}%)")
                        # Récupérer le créateur depuis le cache RugCheck
                        cached_report = security_checker._cache.get(pos.token_address)
                        if cached_report:
                            # Invalider le cache pour ce token
                            del security_checker._cache[pos.token_address]
                        # Blacklister le token pour 24h
                        sl_blacklist[pos.token_address] = time.time() + 86400
                        _save_blacklist()

                    # Notifier
                    pnl_emoji = '✅' if pos.pnl_pct > 0 else '❌'
                    msg = f"{pnl_emoji} *VENTE AUTO*\n\n"
                    msg += f"🪙 {pos.token_name} (${pos.token_symbol})\n"
                    msg += f"📈 PnL: {pos.pnl_pct:+.1f}% ({pos.amount_sol_invested * pos.pnl_pct / 100:+.4f} SOL)\n"
                    msg += f"📝 Raison: {reason}\n"
                    msg += f"🔗 [TX](https://solscan.io/tx/{result['tx_signature']})"
                    for chat_id in subscribers:
                        try:
                            await context.bot.send_message(
                                chat_id=chat_id, text=msg,
                                parse_mode=ParseMode.MARKDOWN,
                                disable_web_page_preview=True
                            )
                        except:
                            pass

            await asyncio.sleep(1.5)  # Rate limit
        except Exception as e:
            logger.error(f"Erreur check position {pos.token_address}: {e}")


async def scan_and_trade(context: ContextTypes.DEFAULT_TYPE):
    """Scanner et trader automatiquement avec Smart Entry (double-check)"""
    # SNIPER DÉSACTIVÉ — plus de nouveaux achats auto
    # Le monitoring des positions existantes reste actif (sniper_monitor_job 3s)
    return

    # Daily PnL Guard: ne pas trader si en pause
    if daily_pnl_guard and daily_pnl_guard.is_paused():
        logger.info(f"[SCAN] Trading en pause: {daily_pnl_guard.get_pause_reason()}")
        return
    try:
        # === PHASE 1: Confirmer les tokens en watchlist (prioritaire) ===
        if smart_entry:
            ready_tokens = smart_entry.get_watchlist_tokens()
            for address in ready_tokens:
                if address in seen_tokens or address in positions.positions:
                    smart_entry.consume_signal(address)
                    continue

                await asyncio.sleep(1.5)
                analysis = api.analyze_token(address)
                if not analysis:
                    continue

                # Tenter la confirmation
                signal = smart_entry.confirm_check(analysis)
                if signal:
                    # Calculer la taille dynamique
                    if position_sizer:
                        strat_stats = pnl_tracker.get_strategy_stats() if pnl_tracker else {}
                        s_stats = strat_stats.get(signal.strategy)
                        balance = trading_engine.wallet.get_sol_balance()
                        dyn_size = position_sizer.calculate_size(
                            strategy=signal.strategy,
                            confidence=signal.confidence,
                            strategy_stats=s_stats,
                            balance_sol=balance
                        )
                        # Override temporaire de la taille de position
                        original_size = trading_config.position_size_sol
                        trading_config.position_size_sol = dyn_size
                        trading_config.sniper_position_sol = dyn_size

                    # 💧 Vérifier la liquidité AVANT l'achat (protection illiquidité)
                    if liquidity_guard:
                        liq_usd = analysis.get("liquidity_usd", 0)
                        can_buy_liq, liq_reason = liquidity_guard.can_buy(
                            address, liq_usd, strategy=signal.strategy
                        )
                        if not can_buy_liq:
                            logger.info(f"{liq_reason} - Skip {signal.token_symbol}")
                            smart_entry.consume_signal(address)
                            seen_tokens.add(address)
                            if position_sizer:
                                trading_config.position_size_sol = original_size
                                trading_config.sniper_position_sol = original_size
                            continue

                    # Vérifier la corrélation avant d'acheter
                    if correlation_filter:
                        can_buy, corr_reason = correlation_filter.can_buy(
                            token_address=address,
                            token_name=signal.token_name,
                            token_symbol=signal.token_symbol,
                            age_hours=analysis.get("age_hours")
                        )
                        if not can_buy:
                            logger.info(f"{corr_reason} - Skip {signal.token_symbol}")
                            smart_entry.consume_signal(address)
                            seen_tokens.add(address)
                            # Restaurer la taille
                            if position_sizer:
                                trading_config.position_size_sol = original_size
                                trading_config.sniper_position_sol = original_size
                            continue

                    # Signal confirmé ! Exécuter l'achat
                    result = trading_engine.execute_buy(analysis, signal.strategy)

                    # Restaurer la taille originale
                    if position_sizer:
                        trading_config.position_size_sol = original_size
                        trading_config.sniper_position_sol = original_size
                    if result:
                        smart_entry.consume_signal(address)
                        seen_tokens.add(address)

                        # Enregistrer dans le CircuitBreaker
                        if circuit_breaker:
                            entry_price = float(analysis.get("price_usd", 0) or 0)
                            circuit_breaker.open_position(
                                token_address=address,
                                token_symbol=signal.token_symbol,
                                entry_price=entry_price,
                            )
                        if capital_watchdog:
                            capital_watchdog.register_position(
                                token_address=address,
                                token_symbol=signal.token_symbol,
                                strategy=signal.strategy,
                                amount_sol=result['amount_sol'],
                                entry_price=float(analysis.get("price_usd", 0) or 0),
                            )

                        # Enregistrer dans le filtre de corrélation
                        if correlation_filter:
                            correlation_filter.register_position(
                                token_address=address,
                                token_name=signal.token_name,
                                token_symbol=signal.token_symbol,
                                age_hours=analysis.get("age_hours")
                            )

                        # Enregistrer la liquidité pour monitoring post-achat
                        if liquidity_guard:
                            liq_usd = analysis.get("liquidity_usd", 0)
                            if liq_usd > 0:
                                liquidity_guard.register_position(address, liq_usd)

                        # Notification enrichie
                        sec_info = ""
                        if security_checker:
                            sr = security_checker._cache.get(address)
                            if sr:
                                sec_info = f"\n🛡 Sécurité: {sr.risk_level} (score {sr.risk_score})"

                        confidence_bar = "█" * int(signal.confidence * 5) + "░" * (5 - int(signal.confidence * 5))
                        msg = f"🧠 *SMART ENTRY* ({signal.strategy})\n\n"
                        msg += f"🪙 {signal.token_name} (${signal.token_symbol})\n"
                        msg += f"💵 {result['amount_sol']} SOL\n"
                        msg += f"📊 MC: ${analysis.get('market_cap', 0):,.0f}\n"
                        msg += f"💧 Liq: ${analysis.get('liquidity_usd', 0):,.0f}\n"
                        msg += f"🎯 Confiance: [{confidence_bar}] {signal.confidence:.0%}\n"
                        if signal.volume_multiplier >= 2.0:
                            msg += f"🔥 Volume spike: x{signal.volume_multiplier:.1f}\n"
                        msg += f"📈 5m: {analysis['price_change_5m']:+.1f}% | 1h: {analysis['price_change_1h']:+.1f}%"
                        msg += sec_info
                        msg += f"\n\n✅ *Confirmé après double-check*"
                        msg += f"\n🔗 [TX](https://solscan.io/tx/{result['tx_signature']})"

                        for chat_id in subscribers:
                            try:
                                await context.bot.send_message(
                                    chat_id=chat_id, text=msg,
                                    parse_mode=ParseMode.MARKDOWN,
                                    disable_web_page_preview=True
                                )
                            except:
                                pass

                        await asyncio.sleep(trading_config.cooldown_seconds)
                        if price_monitor:
                            try:
                                await price_monitor.add_token(address)
                            except Exception as e:
                                logger.error(f"Erreur ajout WS monitoring: {e}")

            # Nettoyage périodique
            smart_entry.cleanup()

        # === PHASE 2: Scanner les nouveaux tokens (premier check) ===
        new_tokens = api.find_new_meme_coins()

        for token_data in new_tokens[:10]:
            address = token_data["address"]

            # Déjà vu/rejeté ?
            if address in seen_tokens:
                continue

            # Blacklisté après un Stop Loss ?
            if address in sl_blacklist:
                if time.time() < sl_blacklist[address]:
                    continue
                else:
                    del sl_blacklist[address]

            # Déjà en position ?
            if address in positions.positions:
                seen_tokens.add(address)
                continue

            # Analyser
            await asyncio.sleep(1.5)
            analysis = api.analyze_token(address)
            if not analysis:
                continue

            # 🛡️ FILTRE ANTI-RUG RENFORCÉ (avant toute décision d'achat)
            if security_checker:
                # Déterminer la stratégie pour adapter la sévérité
                token_age = analysis.get('age_hours', 999)
                check_strategy = "sniper" if token_age < 1 else "momentum"
                is_safe, security_reason = security_checker.quick_check(address, strategy=check_strategy)
                if not is_safe:
                    logger.info(f"🚫 REJETÉ (anti-rug): {analysis.get('name', address[:12])} - {security_reason}")
                    seen_tokens.add(address)
                    if len(seen_tokens) > MAX_SEEN_TOKENS:
                        seen_tokens.clear()
                    continue
                logger.info(f"✅ Token sûr: {analysis.get('name', address[:12])} - {security_reason}")

            # 🧠 SMART ENTRY: Premier check (ajouter à la watchlist si prometteur)
            if smart_entry:
                added, watch_reason = smart_entry.first_check(analysis)
                if added:
                    # Token ajouté à la watchlist, sera confirmé au prochain cycle
                    logger.info(f"🧠 Watchlist: {analysis.get('name', address[:12])} - {watch_reason}")
                    continue

            # Fallback: stratégies classiques (si smart_entry n'est pas actif ou score trop bas)
            # Stratégie Sniper (tokens très frais < 1h)
            should_snipe, reason = trading_engine.should_snipe(analysis)
            if should_snipe:
                # Vérifier la liquidité avant achat
                if liquidity_guard:
                    liq_usd = analysis.get("liquidity_usd", 0)
                    can_buy_liq, liq_reason = liquidity_guard.can_buy(address, liq_usd, strategy="sniper")
                    if not can_buy_liq:
                        logger.info(f"{liq_reason} - Skip {analysis.get('name', address[:12])}")
                        seen_tokens.add(address)
                        continue
                result = trading_engine.execute_buy(analysis, "sniper")
                if result:
                    # Enregistrer dans le CircuitBreaker
                    if circuit_breaker:
                        entry_price = float(analysis.get("price_usd", 0) or 0)
                        circuit_breaker.open_position(
                            token_address=address,
                            token_symbol=analysis.get("symbol", "???"),
                            entry_price=entry_price,
                        )
                    if capital_watchdog:
                        capital_watchdog.register_position(
                            token_address=address,
                            token_symbol=analysis.get("symbol", "???"),
                            strategy="sniper",
                            amount_sol=result['amount_sol'],
                            entry_price=float(analysis.get("price_usd", 0) or 0),
                        )
                    # Enregistrer LP pour monitoring
                    if liquidity_guard:
                        liq_usd = analysis.get("liquidity_usd", 0)
                        if liq_usd > 0:
                            liquidity_guard.register_position(address, liq_usd)
                    sec_info = ""
                    if security_checker:
                        sr = security_checker._cache.get(address)
                        if sr:
                            sec_info = f"\n🛡 Sécurité: {sr.risk_level} (score {sr.risk_score})"
                    msg = f"🎯 *SNIPE AUTO*\n\n"
                    msg += f"🪙 {analysis['name']} (${analysis['symbol']})\n"
                    msg += f"💵 {result['amount_sol']} SOL\n"
                    msg += f"📊 MC: ${analysis.get('market_cap', 0):,.0f}\n"
                    msg += f"💧 Liq: ${analysis.get('liquidity_usd', 0):,.0f}"
                    msg += sec_info
                    msg += f"\n🔗 [TX](https://solscan.io/tx/{result['tx_signature']})"
                    for chat_id in subscribers:
                        try:
                            await context.bot.send_message(
                                chat_id=chat_id, text=msg,
                                parse_mode=ParseMode.MARKDOWN,
                                disable_web_page_preview=True
                            )
                        except:
                            pass
                    await asyncio.sleep(trading_config.cooldown_seconds)
                    if price_monitor:
                        try:
                            await price_monitor.add_token(address)
                        except Exception as e:
                            logger.error(f"Erreur ajout WS monitoring: {e}")
                continue

            # Stratégie Momentum - DÉSACTIVÉE (mode Sniper Only)
            # Les tokens > 1h sont déjà filtrés par le Smart Entry
            pass

    except Exception as e:
        logger.error(f"Erreur scan_and_trade: {e}")


# ============================================================
# COMMANDE PNL TRACKER
# ============================================================

async def pnl_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /pnl - Rapport PnL détaillé"""
    if not pnl_tracker:
        await update.message.reply_text("❌ PnL Tracker non initialisé.")
        return

    if not pnl_tracker.trade_results:
        await update.message.reply_text("📊 Aucun trade terminé pour le moment.")
        return

    total = pnl_tracker.get_total_pnl()
    msg = f"💰 *Rapport PnL Détaillé*\n\n"
    msg += f"🎯 *Résumé Global:*\n"
    pnl_emoji = "🟢" if total['total_pnl_sol'] >= 0 else "🔴"
    msg += f"  {pnl_emoji} PnL total: {total['total_pnl_sol']:+.4f} SOL\n"
    msg += f"  📊 Trades: {total['total_trades']} (✅{total['wins']} / ❌{total['losses']})\n"
    msg += f"  🎯 Win Rate: {total['win_rate']:.1f}%\n"
    msg += f"  📈 PnL moyen: {total['avg_pnl_pct']:+.1f}%\n"
    msg += f"  🚀 Meilleur: {total['best_trade']:+.1f}%\n"
    msg += f"  💥 Pire: {total['worst_trade']:+.1f}%\n"

    # Par stratégie
    strat_stats = pnl_tracker.get_strategy_stats()
    if strat_stats:
        msg += f"\n🎮 *Performance par Stratégie:*\n"
        for name, s in sorted(strat_stats.items(), key=lambda x: x[1].total_pnl_sol, reverse=True):
            emoji = "🟢" if s.total_pnl_sol >= 0 else "🔴"
            msg += f"\n  {emoji} *{name.upper()}*\n"
            msg += f"    Trades: {s.total_trades} | WR: {s.win_rate:.0f}%\n"
            msg += f"    PnL: {s.total_pnl_sol:+.4f} SOL (moy: {s.avg_pnl_pct:+.1f}%)\n"
            msg += f"    Best: {s.best_trade_pct:+.1f}% | Worst: {s.worst_trade_pct:+.1f}%\n"
            msg += f"    Durée moy: {s.avg_hold_minutes:.0f} min\n"

    # Par raison de sortie
    exit_stats = pnl_tracker.get_exit_reason_stats()
    if exit_stats:
        msg += f"\n🚪 *Par Raison de Sortie:*\n"
        reason_labels = {
            "take_profit": "✅ Take Profit",
            "trailing_stop": "📉 Trailing Stop",
            "stop_loss": "❌ Stop Loss",
            "other": "📝 Autre",
        }
        for reason, data in exit_stats.items():
            label = reason_labels.get(reason, reason)
            wr = (data['wins'] / max(data['count'], 1)) * 100
            avg_pnl = data['total_pnl'] / max(data['count'], 1)
            msg += f"  {label}: {data['count']}x (PnL moy: {avg_pnl:+.1f}%)\n"

    # PnL quotidien (7 jours)
    daily = pnl_tracker.get_daily_summary(7)
    active_days = [d for d in daily if d['trades'] > 0]
    if active_days:
        msg += f"\n📅 *PnL Quotidien (7j):*\n"
        for d in active_days:
            bar = "█" * min(int(abs(d['pnl_sol']) * 20), 5)
            day_emoji = "🟢" if d['pnl_sol'] >= 0 else "🔴"
            msg += f"  {day_emoji} {d['date'][-5:]}: {d['pnl_sol']:+.4f} SOL {bar} ({d['trades']}t)\n"

    # Position Sizer
    if position_sizer:
        info = position_sizer.get_info()
        msg += f"\n💰 *Position Sizing:*\n"
        msg += f"  Base: {info['base_size']} SOL | Range: {info['min_size']}-{info['max_size']} SOL\n"
        if info['consecutive_wins'] >= 2:
            msg += f"  🔥 Série gagnante: {info['consecutive_wins']} (↑ taille)\n"
        elif info['consecutive_losses'] >= 2:
            msg += f"  ⚠️ Série perdante: {info['consecutive_losses']} (↓ taille)\n"

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def diversity_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /diversity - Voir la diversification du portefeuille"""
    if not correlation_filter:
        await update.message.reply_text("❌ Filtre corrélation non initialisé.")
        return

    # Synchroniser avec les positions actuelles
    correlation_filter.sync_with_positions(positions.get_open_positions())

    # Générer le rapport
    msg = correlation_filter.get_status_message()

    # Ajouter les commandes
    msg += "\n*Commandes:*\n"
    msg += "/set\\_corr off \u2014 Désactiver le filtre\n"
    msg += "/set\\_corr on \u2014 Activer le filtre\n"
    msg += "/set\\_corr 3 \u2014 Max 3 tokens par narratif"

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def set_corr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /set_corr - Configurer le filtre anti-corrélation"""
    if not correlation_filter:
        await update.message.reply_text("❌ Filtre corrélation non initialisé.")
        return

    if not context.args:
        await diversity_cmd(update, context)
        return

    arg = context.args[0].lower()
    if arg == "off":
        correlation_filter.config.enabled = False
        await update.message.reply_text("❌ Filtre anti-corrélation désactivé")
    elif arg == "on":
        correlation_filter.config.enabled = True
        await update.message.reply_text("✅ Filtre anti-corrélation activé")
    else:
        try:
            max_per = int(arg)
            if 1 <= max_per <= 5:
                correlation_filter.config.max_per_narrative = max_per
                await update.message.reply_text(
                    f"✅ Max tokens par narratif: {max_per}"
                )
            else:
                await update.message.reply_text("❌ Valeur entre 1 et 5")
        except ValueError:
            await update.message.reply_text("❌ Usage: /set\\_corr on|off|<1-5>", parse_mode=ParseMode.MARKDOWN)


async def insights_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /insights - Analyse post-trade et recommandations"""
    if not post_trade_analyzer:
        await update.message.reply_text("❌ Post-Trade Analyzer non initialisé.")
        return

    insights = post_trade_analyzer.get_insights()

    if insights["status"] == "no_data":
        pending = len(post_trade_analyzer.pending_checks)
        total = len(post_trade_analyzer.analyses)
        msg = "📊 *Post-Trade Insights*\n\n"
        msg += "Pas encore assez de données complètes.\n\n"
        msg += f"Trades enregistrés: {total}\n"
        msg += f"En cours de suivi: {pending}\n\n"
        msg += "Les insights seront disponibles après que les premiers\n"
        msg += "trades aient été suivis pendant 1h post-vente."
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
        return

    msg = "📊 *POST-TRADE INSIGHTS*\n\n"

    msg += f"🎯 *Résumé ({insights['total_analyzed']} trades analysés):*\n"
    msg += f"  ATH moyen pendant hold: +{insights['avg_ath_during_hold']:.0f}%\n"
    msg += f"  Profit raté moyen: {insights['avg_missed_profit']:.0f}%\n"
    msg += f"  Temps moyen avant pump: {insights['avg_time_to_peak']:.0f} min\n\n"

    msg += f"📈 *Après la vente:*\n"
    msg += f"  Token continue à monter: {insights['continued_up_after_sell']}x\n"
    msg += f"  Token crash après vente: {insights['crashed_after_sell']}x\n\n"

    msg += f"🏆 *Verdicts:*\n"
    verdicts = insights['verdicts']
    verdict_labels = {
        "hold": "📈 Garder plus longtemps",
        "perfect": "✅ Vente parfaite",
        "good": "👍 Bonne vente",
        "neutral": "➖ Neutre",
        "sell_earlier": "⚠️ Vendre plus tôt",
    }
    for v, count in sorted(verdicts.items(), key=lambda x: x[1], reverse=True):
        label = verdict_labels.get(v, v)
        msg += f"  {label}: {count}x\n"

    msg += f"\n🎯 Ventes parfaites/bonnes: {insights['pct_perfect_exit']:.0f}%\n"
    msg += f"📈 Hold trop court: {insights['pct_hold_too_short']:.0f}%\n\n"

    # Recommandations
    if insights['recommendations']:
        msg += "*💡 Recommandations:*\n"
        for rec in insights['recommendations']:
            msg += f"  {rec}\n"

    # Derniers trades analysés
    recent = post_trade_analyzer.get_recent_analyses(3)
    if recent:
        msg += "\n*🔬 Derniers trades analysés:*\n"
        for a in recent:
            v_emoji = {"hold": "📈", "perfect": "✅", "good": "👍", "neutral": "➖", "sell_earlier": "⚠️"}.get(a.would_have_been_better, "❓")
            msg += f"  {v_emoji} {a.token_symbol}: vendu {a.exit_pnl_pct:+.1f}% | 1h après: {a.pnl_1h_after:+.1f}%\n"

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def cb_stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /cb_stats - Stats du CircuitBreaker"""
    if not circuit_breaker:
        await update.message.reply_text("❌ CircuitBreaker non initialisé.")
        return

    stats = circuit_breaker.get_stats()
    msg = "🔌 *CircuitBreaker Stats*\n\n"
    msg += f"📊 Checks effectués: {stats['total_checks']}\n"
    msg += f"🟢 Positions actives: {stats['active_positions']}\n"
    msg += f"📈 Trailing actif: {stats['trailing_active']}\n\n"
    msg += "*Déclenchements:*\n"
    msg += f"  ⏰ Time Stop: {stats['time_stop_triggered']}\n"
    msg += f"  🛑 Stop Loss: {stats['stop_loss_triggered']}\n"
    msg += f"  📉 Trailing: {stats['trailing_triggered']}\n"
    msg += f"  🎯 Take Profit: {stats['take_profit_triggered']}\n\n"

    # Status des positions actives
    if circuit_breaker.positions:
        msg += "*Positions actives:*\n"
        for addr, pos in circuit_breaker.positions.items():
            status = circuit_breaker.get_position_status(addr)
            if status:
                emoji = "🟢" if status['pnl_pct'] >= 0 else "🔴"
                trail = " 📉" if status['trailing_activated'] else ""
                msg += f"  {emoji} {status['symbol']}: {status['pnl_pct']:+.1f}%{trail}\n"
                msg += f"     ATH: {status['highest_pnl']:+.1f}% | Age: {status['age_minutes']:.0f}min\n"

    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


# ============================================================
# HEARTBEAT + STATUS UNIFIÉ + UNPAUSE
# ============================================================

async def heartbeat_job(context: ContextTypes.DEFAULT_TYPE):
    """Heartbeat Telegram toutes les 30 min — confirme que le bot est actif"""
    open_pos = len(positions.get_open_positions()) if positions else 0
    cb_checks = circuit_breaker._stats["total_checks"] if circuit_breaker else 0
    
    # Daily PnL Guard status
    guard_status = ""
    if daily_pnl_guard:
        stats = daily_pnl_guard.get_stats()
        if stats["is_paused"]:
            guard_status = f"\n⛔ {stats['pause_reason']}"
        else:
            guard_status = f"\n💰 PnL jour: {stats['daily_pnl_sol']:+.4f} SOL ({stats['trades_today']} trades)"
    
    msg = f"💚 *Bot Actif*\n"
    msg += f"📊 Positions: {open_pos}\n"
    msg += f"🔍 Checks CB: {cb_checks}\n"
    msg += guard_status
    
    for chat_id in subscribers:
        try:
            await context.bot.send_message(
                chat_id=chat_id, text=msg,
                parse_mode=ParseMode.MARKDOWN
            )
        except:
            pass


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/status — Vue unifiée de tout le système"""
    msg = "📊 *STATUS COMPLET*\n\n"
    
    # 1. Solde
    if wallet:
        sol_balance = wallet.get_sol_balance()
        msg += f"💰 *Solde:* {sol_balance:.4f} SOL\n\n"
    
    # 2. Positions ouvertes
    open_pos = positions.get_open_positions() if positions else []
    msg += f"🟢 *Positions ouvertes:* {len(open_pos)}\n"
    for pos in open_pos:
        emoji = '🟢' if pos.pnl_pct >= 0 else '🔴'
        msg += f"  {emoji} {pos.token_symbol}: {pos.pnl_pct:+.1f}% ({pos.age_minutes:.0f}min)\n"
    msg += "\n"
    
    # 3. CircuitBreaker
    if circuit_breaker:
        stats = circuit_breaker.get_stats()
        msg += f"🔌 *CircuitBreaker:*\n"
        msg += f"  Checks: {stats['total_checks']} | "
        msg += f"SL: {stats['stop_loss_triggered']} | "
        msg += f"TP: {stats['take_profit_triggered']} | "
        msg += f"Trail: {stats['trailing_triggered']} | "
        msg += f"TS: {stats['time_stop_triggered']}\n\n"
    
    # 4. Daily PnL Guard
    if daily_pnl_guard:
        guard = daily_pnl_guard.get_stats()
        status_emoji = '⛔' if guard['is_paused'] else '✅'
        msg += f"{status_emoji} *Daily Guard:*\n"
        msg += f"  PnL jour: {guard['daily_pnl_sol']:+.4f} SOL\n"
        msg += f"  Trades: {guard['trades_today']} | SL consécutifs: {guard['consecutive_sl']}/{guard['max_consecutive_sl']}\n"
        if guard['is_paused']:
            msg += f"  {guard['pause_reason']}\n"
        msg += "\n"
    
    # 5. Watchdog
    if capital_watchdog:
        wd_stats = capital_watchdog.get_stats()
        msg += f"🛡️ *Watchdog:*\n"
        msg += f"  Checks: {wd_stats.get('total_checks', 0)} | "
        msg += f"Max gap: {wd_stats.get('max_gap_seen', 0):.1f}s\n\n"
    
    # 6. Config active
    msg += f"⚙️ *Config:*\n"
    msg += f"  TP: +{trading_config.take_profit_pct}% | SL: {trading_config.stop_loss_pct}%\n"
    msg += f"  Trailing: dès +20% / -{circuit_breaker.config.trailing_stop_pct}%\n" if circuit_breaker else ""
    msg += f"  Time Stop: {trading_config.time_stop_minutes}min (sniper) / 30min (recovered)\n"
    msg += f"  Partial TP: 50% au TP, 50% trailing\n"
    
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def unpause_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/unpause — Forcer la reprise du trading après une pause Daily Guard"""
    if daily_pnl_guard:
        daily_pnl_guard.force_unpause()
        await update.message.reply_text("✅ Pause levée. Trading repris.")
    else:
        await update.message.reply_text("❌ Daily PnL Guard non initialisé.")


# ============================================================
# COMMANDES MONITORING (reprises du bot v1)
# ============================================================

async def scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /scan"""
    await update.message.reply_text("🔍 Scan en cours...")
    new_tokens = api.find_new_meme_coins()
    count = 0
    for token_data in new_tokens[:5]:
        analysis = api.analyze_token(token_data["address"])
        if analysis:
            is_gem, reasons = api.is_potential_gem(analysis)
            if is_gem:
                msg = f"💎 *{analysis['name']}* (${analysis['symbol']})\n"
                msg += f"💵 ${analysis['price_usd']} | MC: ${analysis.get('market_cap', 0):,.0f}\n"
                msg += f"📈 5m: {analysis['price_change_5m']:+.1f}% | 1h: {analysis['price_change_1h']:+.1f}%\n"
                msg += f"💧 Liq: ${analysis['liquidity_usd']:,.0f}\n"
                for r in reasons:
                    msg += f"  {r}\n"
                msg += f"\n📋 `{analysis['address']}`"
                await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
                count += 1
        await asyncio.sleep(1.5)
    if count == 0:
        await update.message.reply_text("😴 Aucun gem détecté pour le moment.")


async def trending_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /trending"""
    boosts = api.get_boosted_tokens()
    if not boosts:
        await update.message.reply_text("Aucun token trending.")
        return
    msg = "🔥 *Top Tokens Boostés (Solana)*\n\n"
    for i, token in enumerate(boosts[:10], 1):
        msg += f"{i}. `{token.get('tokenAddress', '')[:16]}...`\n"
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def metas_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /metas"""
    metas = api.get_trending_metas()
    if not metas:
        await update.message.reply_text("Aucune narrative trending.")
        return
    msg = "🎯 *Narratives Trending*\n\n"
    for i, meta in enumerate(metas[:10], 1):
        name = meta.get("name", "Unknown")
        mc = meta.get("marketCap", 0)
        msg += f"{i}. *{name}* - MC: ${mc:,.0f}\n"
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


# ============================================================
# MAIN
# ============================================================

def main():
    """Démarrer le bot de trading"""
    # Fix: Créer un event loop explicite pour éviter RuntimeError sur certains environnements
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    if TELEGRAM_BOT_TOKEN == "VOTRE_TOKEN_ICI":
        print("⚠️  Token Telegram non configuré dans config.py !")
        return

    # Charger le wallet (depuis env var WALLET_PRIVATE_KEY ou fichier)
    env_key = os.environ.get("WALLET_PRIVATE_KEY", "").strip()
    print(f"[DEBUG] WALLET_PRIVATE_KEY env var present: {bool(env_key)}, length: {len(env_key)}")
    print(f"[DEBUG] Wallet file exists: {os.path.exists(WalletManager.WALLET_FILE)}")
    if env_key:
        try:
            pub_key = wallet.import_wallet(env_key)
            init_trading()
            print(f"💰 Wallet chargé depuis env: {pub_key}")
            balance = wallet.get_sol_balance()
            print(f"💰 Solde: {balance} SOL")
            # Auto-activer le trading si wallet présent
            global auto_trading_enabled
            auto_trading_enabled = True
            print("🚀 Trading automatique ACTIVÉ")
        except Exception as e:
            print(f"[ERROR] Impossible de charger wallet depuis env: {e}")
    elif os.path.exists(WalletManager.WALLET_FILE):
        wallet.load_or_create_wallet()
        init_trading()
        print(f"💰 Wallet chargé depuis fichier: {wallet.public_key}")
        auto_trading_enabled = True
        print("🚀 Trading automatique ACTIVÉ")
    else:
        print("⚠️  Aucun wallet configuré. Utilisez /wallet ou /import_wallet")

    # Nettoyer les positions fantômes (positions sans tokens réels)
    if wallet.keypair and positions.count_positions() > 0:
        print("🔍 Vérification des positions existantes...")
        to_remove = []
        for addr, pos in positions.positions.items():
            _, raw_bal = wallet.get_token_balance(addr)
            if raw_bal <= 0:
                to_remove.append(addr)
                print(f"  ❌ Position fantôme supprimée: {pos.token_name}")
        for addr in to_remove:
            positions.close_position(addr)
        if to_remove:
            print(f"  🧹 {len(to_remove)} positions fantômes nettoyées")

    # Scanner le wallet pour récupérer les positions orphelines
    # (tokens achetés mais perdus du tracking après un redémarrage)
    if wallet.keypair:
        print("🔍 Scan du wallet pour positions orphelines...")
        try:
            all_tokens = wallet.get_all_token_balances()
            # Tokens connus à ignorer (stablecoins, wrapped SOL, etc.)
            IGNORE_MINTS = {
                "So11111111111111111111111111111111111111112",   # Wrapped SOL
                "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", # USDC
                "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
            }
            orphans_found = 0
            for token_info in all_tokens:
                mint = token_info["mint"]
                # Ignorer les tokens déjà suivis ou connus
                if mint in IGNORE_MINTS:
                    continue
                if mint in positions.positions:
                    continue
                # C'est un token orphelin ! Récupérer ses infos
                try:
                    analysis = api.analyze_token(mint)
                    if analysis:
                        token_name = analysis.get("name", "Unknown")
                        token_symbol = analysis.get("symbol", "???")
                        price_usd = float(analysis.get("price_usd", 0) or 0)
                        # Filtre qualité Recovered: vérifier volume et liquidité
                        volume_24h = float(analysis.get("volume_24h", 0) or 0)
                        liquidity = float(analysis.get("liquidity_usd", 0) or 0)
                        buy_sell_ratio = float(analysis.get("buy_sell_ratio", 1.0) or 1.0)
                        # Minimum: $500 volume 24h ET $1000 liquidité
                        if volume_24h < 500:
                            print(f"  ⚠️ Skip {token_symbol}: volume trop faible (${volume_24h:.0f} < $500)")
                            continue
                        if liquidity < 1000:
                            print(f"  ⚠️ Skip {token_symbol}: liquidité trop faible (${liquidity:.0f} < $1000)")
                            continue
                    else:
                        token_name = f"Token {mint[:8]}..."
                        token_symbol = "???"
                        price_usd = 0
                    # Estimer le SOL investi (on utilise 0.05 par défaut)
                    positions.open_position(
                        token_address=mint,
                        token_name=token_name,
                        token_symbol=token_symbol,
                        entry_price=price_usd,  # On utilise le prix actuel comme référence
                        amount_sol=0.05,  # Estimation
                        amount_tokens=token_info["ui_amount"],
                        strategy="recovered",
                    )
                    orphans_found += 1
                    print(f"  ✅ Position récupérée: {token_name} ({token_symbol}) - {token_info['ui_amount']:.0f} tokens")
                except Exception as e:
                    print(f"  ⚠️ Erreur récupération {mint[:12]}...: {e}")
            if orphans_found:
                print(f"  📦 {orphans_found} positions orphelines récupérées !")
            else:
                print("  ✅ Aucune position orpheline")
        except Exception as e:
            print(f"  ⚠️ Erreur scan wallet: {e}")

    print("🤖 Démarrage du Solana Trading Bot...")
    print(f"⏱  Intervalle: {POLLING_INTERVAL}s")
    print(f"🎯 TP: +{trading_config.take_profit_pct}% | SL: {trading_config.stop_loss_pct}% | Trailing: dès +{trading_config.trailing_activation_pct}%")
    ts_info = f"{trading_config.time_stop_minutes:.0f}min" if trading_config.time_stop_enabled else "OFF"
    print(f"⏰ Time Stop: {ts_info} (seuil: +{trading_config.time_stop_min_profit}%)")
    print(f"💰 Budget max: {trading_config.max_budget_sol} SOL")
    print(f"🌐 Jupiter API: {trading_config.jupiter_api_url}")

    # Créer l'application Telegram
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Commandes
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("wallet", wallet_cmd))
    app.add_handler(CommandHandler("import_wallet", import_wallet_cmd))
    app.add_handler(CommandHandler("balance", balance_cmd))
    app.add_handler(CommandHandler("positions", positions_cmd))
    app.add_handler(CommandHandler("history", history_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("config", config_cmd))
    app.add_handler(CommandHandler("auto_on", auto_on))
    app.add_handler(CommandHandler("auto_off", auto_off))
    app.add_handler(CommandHandler("set_tp", set_tp))
    app.add_handler(CommandHandler("set_sl", set_sl))
    app.add_handler(CommandHandler("set_trailing", set_trailing))
    app.add_handler(CommandHandler("set_timestop", set_timestop))
    app.add_handler(CommandHandler("set_size", set_size))
    app.add_handler(CommandHandler("buy", buy_cmd))
    app.add_handler(CommandHandler("sell", sell_cmd))
    app.add_handler(CommandHandler("sell_all", sell_all_cmd))
    app.add_handler(CommandHandler("scan", scan_command))
    app.add_handler(CommandHandler("trending", trending_command))
    app.add_handler(CommandHandler("metas", metas_command))
    app.add_handler(CommandHandler("copy_wallets", copy_wallets_cmd))
    app.add_handler(CommandHandler("copy_add", copy_add_cmd))
    app.add_handler(CommandHandler("copy_remove", copy_remove_cmd))
    app.add_handler(CommandHandler("copy_history", copy_history_cmd))
    app.add_handler(CommandHandler("pnl", pnl_cmd))
    app.add_handler(CommandHandler("diversity", diversity_cmd))
    app.add_handler(CommandHandler("set_corr", set_corr))
    app.add_handler(CommandHandler("insights", insights_cmd))
    app.add_handler(CommandHandler("cb_stats", cb_stats_cmd))
    app.add_handler(CommandHandler("watchdog", watchdog_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("unpause", unpause_cmd))

    # Job SNIPER ultra-rapide (3s) - SL/TP réactif pour positions sniper
    job_queue = app.job_queue
    job_queue.run_repeating(sniper_monitor_job, interval=3, first=5)

    # Job Capital Watchdog (5s) - surveille que le capital est sous contrôle
    job_queue.run_repeating(watchdog_check_job, interval=5, first=8)

    # Job backup pour LP monitoring + post-trade analysis (15s)
    job_queue.run_repeating(position_monitor_job, interval=15, first=10)

    # Job de scan pour nouvelles opportunités (toutes les 45s)
    job_queue.run_repeating(auto_trading_job, interval=POLLING_INTERVAL, first=20)

    # Heartbeat Telegram (30min) - confirme que le bot est actif
    job_queue.run_repeating(heartbeat_job, interval=1800, first=60)

    # Démarrer le WebSocket PriceMonitor pour les positions existantes
    # (s'exécute en arrière-plan dans l'event loop)
    async def post_init(application):
        """Callback post-init pour démarrer le WebSocket et le copy trading"""
        try:
            await start_price_monitor_for_positions()
            print("🔌 WebSocket PriceMonitor démarré !")
        except Exception as e:
            print(f"⚠️ Erreur démarrage WebSocket (fallback polling actif): {e}")
        # Démarrer le copy trading
        if copy_trader and auto_trading_enabled:
            try:
                await copy_trader.start_monitoring()
                print(f"📋 Copy Trading démarré: {len(copy_trader.get_active_wallets())} wallets suivis")
            except Exception as e:
                print(f"⚠️ Erreur démarrage copy trading (polling fallback actif): {e}")

    app.post_init = post_init

    print("✅ Bot de trading démarré !")
    print("🔌 WebSocket: monitoring temps réel actif (TP/SL instantané)")
    print("⏱  Polling 15s: backup pour tokens sans pool PumpSwap")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
