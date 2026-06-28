"""
Bot Telegram V2 - Solana Meme Coin TRADING Bot
Combine le monitoring (alertes) avec le trading automatique (achat/vente).
"""

import asyncio
import logging
import logging.handlers
import time
import json
import os
from datetime import datetime, timezone, time as dt_time

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
from token_filter import TokenFilter
from onchain_scorer import OnchainScorer
from auto_calibration import analyze_postmortems, apply_adjustments
from helius_websocket import HeliusWebSocket
import requests

# ─── FIX MÉMOIRE 5 — Rotation des logs (max 10MB, 3 fichiers) ───
def _setup_logging():
    """Configure les logs avec rotation automatique."""
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    )

    # Handler fichier avec rotation : max 10MB, garde 3 fichiers
    file_handler = logging.handlers.RotatingFileHandler(
        'trading_bot.log',
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=3
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    # Handler console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

_setup_logging()
logger = logging.getLogger(__name__)
# ─────────────────────────────────────────────────────────────────

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


# === PERSISTANCE DE L'ÉTAT (survit aux redémarrages Render) ===
STATE_FILE = os.path.join(DATA_DIR, "state.json")
TRADES_LOG_FILE = os.path.join(DATA_DIR, "trades.json")

# Valeurs par défaut pour state.json (source de vérité)
_STATE_DEFAULTS = {
    "auto_trading": True,
    "sl_pct": -25.0,
    "tp_pct": 20.0,
    "trailing_activation": 20.0,
    "trailing_sl": -15.0,
    "time_stop_sniper": 20,
    "time_stop_recovered": 30,
    "position_sizing": {
        "base": 0.05,
        "min": 0.02,
        "max": 0.15,
    },
    "streak": {
        "consecutive_wins": 0,
        "consecutive_losses": 0,
    },
    "blacklist": {},
    "last_updated": "",
}


def _load_state() -> dict:
    """
    Charger l'état persistant depuis state.json.
    - Si fichier absent → créer avec défauts
    - Si clé manquante → utiliser valeur par défaut
    - Ne jamais crasher
    """
    state = {}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
        except (json.JSONDecodeError, IOError, ValueError):
            logger.warning("[STATE] state.json corrompu, utilisation des défauts")
            state = {}

    # Remplir les clés manquantes avec les défauts
    for key, default_val in _STATE_DEFAULTS.items():
        if key not in state:
            state[key] = default_val
        elif isinstance(default_val, dict):
            # Merge profond pour les sous-dicts (position_sizing, streak, blacklist)
            for sub_key, sub_default in default_val.items():
                if sub_key not in state[key]:
                    state[key][sub_key] = sub_default

    # Nettoyer la blacklist (supprimer les entrées expirées)
    now = time.time()
    if "blacklist" in state and isinstance(state["blacklist"], dict):
        state["blacklist"] = {
            addr: entry for addr, entry in state["blacklist"].items()
            if isinstance(entry, dict) and entry.get("banned_until", 0) > now
        }

    return state


def _save_state():
    """
    Sauvegarder l'état complet immédiatement après chaque modification.
    Toutes les variables critiques sont capturées ici.
    """
    from datetime import datetime, timezone

    # Capturer le sizing et la streak depuis position_sizer (si initialisé)
    sizing_data = {"base": 0.05, "min": 0.02, "max": 0.15}
    streak_data = {"consecutive_wins": 0, "consecutive_losses": 0}
    if position_sizer:
        info = position_sizer.get_info()
        sizing_data = {
            "base": info["base_size"],
            "min": info["min_size"],
            "max": info["max_size"],
        }
        streak_data = {
            "consecutive_wins": info["consecutive_wins"],
            "consecutive_losses": info["consecutive_losses"],
        }

    # Construire la blacklist au format enrichi
    blacklist_data = {}
    for addr, expiry in sl_blacklist.items():
        if expiry > time.time():
            blacklist_data[addr] = {
                "banned_until": expiry,
                "reason": "SL déclenché",
            }

    state = {
        "auto_trading": auto_trading_enabled,
        "sl_pct": trading_config.stop_loss_pct,
        "tp_pct": trading_config.take_profit_pct,
        "trailing_activation": trading_config.trailing_activation_pct,
        "trailing_sl": -trading_config.trailing_stop_pct,  # Stocké en négatif
        "time_stop_sniper": trading_config.time_stop_minutes,
        "time_stop_recovered": 30,  # Fixe dans CBConfig
        "position_sizing": sizing_data,
        "streak": streak_data,
        "blacklist": blacklist_data,
        "last_updated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }

    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except IOError as e:
        logger.error(f"[STATE] Erreur sauvegarde state.json: {e}")


def _safe_symbol(symbol, token_address: str) -> str:
    """
    Garantir que symbol n'est JAMAIS None/vide.
    Si symbol = None ou vide → utiliser adresse courte comme fallback.
    Ex: 'UNK_7xKp'
    """
    if symbol and str(symbol).strip() and str(symbol).strip() != "None":
        return str(symbol).strip()
    # Fallback: préfixe UNK_ + 4 premiers caractères de l'adresse
    short_addr = token_address[:4] if token_address else "????"
    return f"UNK_{short_addr}"


def _log_trade(token_address: str, token_symbol: str, strategy: str, side: str,
               pnl_pct: float = 0.0, reason: str = "", price: float = 0.0,
               amount_sol: float = 0.0):
    """
    RÈGLE 5: Logger chaque trade dans trades.json (source de vérité post-mortem).
    Format structuré append-only.
    """
    from datetime import datetime, timezone

    trade_entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "token": token_symbol,
        "token_address": token_address,
        "strategy": strategy,
        "side": side,
        "pnl_pct": round(pnl_pct, 2),
        "reason": reason,
        "price": price,
        "amount_sol": round(amount_sol, 6),
    }

    try:
        trades = []
        if os.path.exists(TRADES_LOG_FILE):
            with open(TRADES_LOG_FILE, "r") as f:
                trades = json.load(f)
        trades.append(trade_entry)
        # Garder les 500 derniers trades max (éviter fichier trop gros)
        if len(trades) > 500:
            trades = trades[-500:]
        with open(TRADES_LOG_FILE, "w") as f:
            json.dump(trades, f, indent=2)
    except (IOError, json.JSONDecodeError) as e:
        logger.error(f"[TRADES] Erreur écriture trades.json: {e}")


# ─── FIX 2: Vérifier prix avant achat ────────────────────────────────────────
def verify_price_before_buy(
    token_address: str,
    token_symbol: str,
    max_attempts: int = 3,
    wait_seconds: float = 2.0
) -> tuple:
    """
    Vérifie que le token a un prix réel AVANT d'acheter.
    Retourne (True, price) si OK, (False, 0.0) si skip.

    Évite les Time Stop à 0% causés par
    les tokens non encore indexés sur DexScreener.
    """
    for attempt in range(max_attempts):
        try:
            url = (
                f"https://api.dexscreener.com/latest/"
                f"dex/tokens/{token_address}"
            )
            r = requests.get(url, timeout=3)
            pairs = r.json().get("pairs", [])

            if pairs:
                price = float(pairs[0].get("priceUsd", 0) or 0)
                if price > 0:
                    logger.info(
                        f"[PrixCheck] ✅ {token_symbol} "
                        f"prix OK à l'essai {attempt + 1}: ${price:.8f}"
                    )
                    record_prix_ok(token_symbol)
                    return True, price

            logger.info(
                f"[PrixCheck] Prix=0 essai {attempt + 1}/{max_attempts} "
                f"— attente {wait_seconds}s"
            )
            time.sleep(wait_seconds)

        except Exception as e:
            logger.warning(f"[PrixCheck] Erreur essai {attempt + 1}: {e}")
            time.sleep(wait_seconds)

    logger.warning(
        f"[PrixCheck] ⛔ SKIP {token_symbol} "
        f"— prix=0 après {max_attempts} essais ({max_attempts * wait_seconds:.0f}s)"
    )
    record_prix_skip(token_symbol)
    return False, 0.0
# ─────────────────────────────────────────────────────────────────────────────


# ─── FIX 3: max_positions dynamique ─────────────────────────────────────────
def get_dynamic_max_positions(
    sol_balance: float = None,
    position_size_sol: float = 0.05
) -> int:
    """
    Calcule le nombre maximum de positions simultanées
    basé sur le solde SOL disponible (via RPC si non fourni).

    Logique : floor(solde / taille_position)
    Minimum garanti : 1
    Maximum garanti : 50 (sécurité)

    Exemples :
      0.10 SOL → max 2 positions
      0.50 SOL → max 10 positions
      1.43 SOL → max 28 positions
    """
    import math

    # Récupérer le solde réel via RPC si non fourni
    if sol_balance is None:
        try:
            sol_balance = wallet.get_sol_balance()
        except Exception:
            sol_balance = 0.0

    if position_size_sol <= 0:
        return 1

    # Calcul dynamique : floor(solde / taille_position)
    max_pos = math.floor(sol_balance / position_size_sol)

    # Minimum 1, maximum 50 (sécurité)
    result = max(1, min(max_pos, 50))

    logger.info(
        f"[MaxPos] Dynamique: {sol_balance:.4f} SOL / "
        f"{position_size_sol} = {result} positions (min=1, max=50)"
    )
    return result
# ─────────────────────────────────────────────────────────────────────────────


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
token_filter: TokenFilter = None
onchain_scorer: OnchainScorer = None
helius_ws: HeliusWebSocket = None
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


# Queue de nouveaux tokens détectés par le WebSocket (consommée par ws_token_processor_job)
from collections import deque
_ws_new_token_queue: deque = deque(maxlen=50)  # FIFO, max 50 tokens en attente

# Tracking pipeline silencieux
_ws_queue_first_push: float = 0.0  # timestamp du premier token poussé (0 = jamais)
_bot_start_time: float = time.time()  # timestamp de démarrage du bot
_pipeline_silent_alert_sent: bool = False  # éviter de spammer l'alerte


def init_trading():
    """Initialiser le moteur de trading (après import du wallet)"""
    global swap_engine, trading_engine, security_checker, price_monitor, copy_trader, smart_entry, pnl_tracker, position_sizer, correlation_filter, liquidity_guard, post_trade_analyzer, circuit_breaker, capital_watchdog, daily_pnl_guard, token_filter
    swap_engine = JupiterSwap(wallet, trading_config)
    trading_engine = TradingEngine(trading_config, wallet, swap_engine, positions)
    security_checker = TokenSecurityChecker(rpc_url=trading_config.rpc_url)
    price_monitor = PriceMonitor(
        on_price_update=on_realtime_price_update,
        on_fallback_change=on_ws_fallback_change,
    )
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
    # Token Filter on-chain (mint authority + LP burned + deployer)
    token_filter = TokenFilter(rpc_url=trading_config.rpc_url)
    logger.info("🔍 Token Filter on-chain initialisé (mint/LP/deployer)")

    # Onchain Scorer — scoring direct RPC (remplace RugCheck comme source primaire)
    global onchain_scorer
    onchain_scorer = OnchainScorer(rpc_url=trading_config.rpc_url)
    logger.info("⚡ Onchain Scorer initialisé (score maison 0-100, ~150ms)")

    # Filtre anti-corrélation (RENFORCÉ avec deployer on-chain)
    correlation_filter = CorrelationFilter(data_dir=DATA_DIR, token_filter=token_filter)
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

    # Helius WebSocket — prix temps réel depuis la blockchain
    global helius_ws
    helius_api_key = os.environ.get("HELIUS_API_KEY", "")
    if helius_api_key:
        async def _ws_on_price_update(token_address: str, price_usd: float):
            """Callback Helius WS: met à jour position + cb.check()"""
            pos = positions.get_position(token_address)
            if pos and price_usd > 0:
                positions.update_position(token_address, price_usd)
                if capital_watchdog:
                    capital_watchdog.heartbeat(token_address, price_usd)
                # cb.check() PRIORITÉ ABSOLUE
                if circuit_breaker:
                    cb_action = circuit_breaker.check(token_address, price_usd)
                    if cb_action and cb_action.should_sell:
                        _ws_notification_queue.append({
                            "token_address": token_address,
                            "action": cb_action,
                            "price": price_usd,
                        })

        async def _ws_on_new_token(token_address: str, price_usd: float):
            """Callback Helius WS: nouveau token détecté sur pump.fun → queue pour analyse"""
            # Skip si déjà vu, en position, ou blacklisté
            if token_address in seen_tokens:
                return
            if token_address in positions.positions:
                return
            if token_address in sl_blacklist and time.time() < sl_blacklist[token_address]:
                return
            # Ajouter à la queue pour traitement par ws_token_processor_job
            global _ws_queue_first_push, _pipeline_silent_alert_sent
            _ws_new_token_queue.append({
                "address": token_address,
                "price_usd": price_usd,
                "timestamp": time.time(),
                "source": "websocket",
            })
            if _ws_queue_first_push == 0.0:
                _ws_queue_first_push = time.time()
                _pipeline_silent_alert_sent = False  # reset alerte
            logger.info(f"[WS-QUEUE] +1 token {token_address[:8]}... (queue={len(_ws_new_token_queue)})")
            seen_tokens.add(token_address)  # Marquer comme vu immédiatement (dédup)
            if len(seen_tokens) > MAX_SEEN_TOKENS:
                seen_tokens.clear()

        helius_ws = HeliusWebSocket(
            api_key=helius_api_key,
            on_price_update=_ws_on_price_update,
            on_new_token=_ws_on_new_token,
        )
        # Enregistrer les positions existantes pour surveillance
        for addr in positions.positions:
            helius_ws.watch_token(addr)
        logger.info(f"⚡ Helius WebSocket initialisé ({len(positions.positions)} tokens surveillés)")
    else:
        logger.warning("⚠️ HELIUS_API_KEY manquante — WebSocket désactivé, polling uniquement")


# ============================================================
# WEBSOCKET PRICE MONITOR - CALLBACKS
# ============================================================

# Queue de notifications Telegram (le WS callback n'a pas accès au context)
_ws_notification_queue: list = []

# Queue de nouveaux tokens détectés par le WebSocket (déplacée avant init_trading)
# _ws_new_token_queue est définie au début du fichier (avant init_trading)

# Prix SOL/USD caché (mis à jour périodiquement par le polling job)
_cached_sol_price_usd: float = 150.0  # Défaut conservateur


def update_sol_price(price_usd: float):
    """Mettre à jour le prix SOL/USD caché (appelé par le polling job)"""
    global _cached_sol_price_usd
    if price_usd > 0:
        _cached_sol_price_usd = price_usd


async def on_realtime_price_update(token_address: str, price_sol: float, change_pct: float):
    """
    Callback Helius WebSocket — PRIORITÉ ABSOLUE.
    
    Appelé en temps réel quand le prix d'un token change.
    Pipeline: prix reçu → cb.check() EN PREMIER → SL -25% avant tout.
    
    Compatible avec:
      - circuit_breaker (cb.check)
      - capital_watchdog (.heartbeat)
      - postmortem_tracker
      - pnl_tracker
      - correlation_filter
    """
    global sl_blacklist

    if not trading_engine or not circuit_breaker:
        return

    if token_address not in positions.positions:
        return

    pos = positions.positions[token_address]

    # Convertir prix SOL → USD
    current_price_usd = price_sol * _cached_sol_price_usd

    # RÈGLE 2: Si prix indisponible → fallback last_known_price
    # cb.check() est TOUJOURS appelé (Time Stop doit agir même sans prix frais)
    if current_price_usd <= 0:
        current_price_usd = pos.current_price  # Dernier prix connu
        if current_price_usd <= 0:
            current_price_usd = pos.entry_price_usd * 0.01  # Assumer -99% si jamais vu
        logger.warning(f"[⚡ WS] Prix indisponible pour {pos.token_symbol}, "
                      f"fallback=${current_price_usd:.8f}")
    else:
        # Prix valide → mettre à jour la position (high watermark, PnL)
        positions.update_position(token_address, current_price_usd)

    # Heartbeat watchdog (cette position est surveillée activement)
    if capital_watchdog:
        capital_watchdog.heartbeat(token_address, current_price_usd)

    # ━━━ PRIORITÉ ABSOLUE: cb.check() → SL -25% avant tout ━━━
    # JAMAIS skipé, même si le prix est un fallback (Time Stop fonctionne toujours)
    cb_action = circuit_breaker.check(token_address, current_price_usd)

    if not cb_action.should_sell:
        return  # Pas d'action, on sort immédiatement

    # === VENTE DÉCLENCHÉE ===
    reason = cb_action.reason
    is_partial = cb_action.rule == "partial_take_profit"
    sell_pct = 50.0 if is_partial else 100.0

    result = trading_engine.execute_sell(pos, reason, sell_pct=sell_pct)

    if not result:
        # Vente échouée — le fallback polling réessaiera
        logger.warning(f"⚡ WS vente échouée: {pos.token_symbol} ({reason})")
        return

    if is_partial:
        # Partial TP: 50% vendu, trailing gère la suite
        logger.info(f"⚡ WS PARTIAL TP: {pos.token_symbol} +{pos.pnl_pct:.1f}%")
        _ws_notification_queue.append({
            "type": "partial_tp",
            "symbol": pos.token_symbol,
            "name": pos.token_name,
            "pnl_pct": pos.pnl_pct,
            "reason": reason,
            "tx": result.get("tx_signature", ""),
        })
        return

    # === VENTE TOTALE: cleanup complet ===

    # PnL Tracker
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

    # Position Sizer (streak)
    if position_sizer:
        position_sizer.record_result(pos.pnl_pct > 0)
        _save_state()  # Persister streak immédiatement

    # Daily PnL Guard
    if daily_pnl_guard:
        pnl_sol = pos.amount_sol_invested * (pos.pnl_pct / 100.0)
        daily_pnl_guard.record_trade(pnl_sol, is_stop_loss=(cb_action.rule == "stop_loss"))

    # Corrélation
    if correlation_filter:
        correlation_filter.unregister_position(pos.token_address)

    # Liquidity Guard
    if liquidity_guard:
        liquidity_guard.unregister_position(pos.token_address)

    # CircuitBreaker + Watchdog
    circuit_breaker.close_position(pos.token_address)
    if capital_watchdog:
        capital_watchdog.unregister_position(pos.token_address)

    # Post-Trade Analyzer
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
    if pos.entry_price_usd > 0:
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

    # RÈGLE 5: Logger le trade dans trades.json
    _log_trade(
        token_address=pos.token_address,
        token_symbol=pos.token_symbol,
        strategy=pos.strategy,
        side="SELL",
        pnl_pct=pos.pnl_pct,
        reason=reason,
        price=current_price_usd,
        amount_sol=pos.amount_sol_invested,
    )

    # RÈGLE 3: Blacklister si perte (SL déclenché)
    if pos.pnl_pct < 0:
        sl_blacklist[pos.token_address] = time.time() + SL_BLACKLIST_DURATION
        _save_blacklist()
        seen_tokens.add(pos.token_address)
        _save_state()  # Persister blacklist dans state.json

    # Retirer du monitoring WS
    if price_monitor:
        await price_monitor.remove_token(token_address)
    if helius_ws:
        helius_ws.unwatch_token(token_address)

    # Notification Telegram (via queue, sera envoyée par le polling job)
    _ws_notification_queue.append({
        "type": "sell",
        "symbol": pos.token_symbol,
        "name": pos.token_name,
        "pnl_pct": pos.pnl_pct,
        "reason": reason,
        "amount_sol": pos.amount_sol_invested,
        "tx": result.get("tx_signature", ""),
    })

    logger.info(f"⚡ VENTE WS INSTANTANÉE: {pos.token_name} PnL: {pos.pnl_pct:+.1f}% - {reason}")


async def on_ws_fallback_change(is_fallback: bool, reason: str):
    """
    Callback quand le WebSocket bascule entre connecté et fallback.
    Ajoute une notification à la queue Telegram.
    """
    if is_fallback:
        logger.warning(f"⚠️ FALLBACK ACTIVÉ: {reason}")
        _ws_notification_queue.append({
            "type": "fallback_on",
            "reason": reason,
        })
    else:
        logger.info(f"✅ WebSocket RECONNECTÉ: {reason}")
        _ws_notification_queue.append({
            "type": "fallback_off",
            "reason": reason,
        })


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
    _save_state()  # Persister l'état pour survivre aux redémarrages
    balance = wallet.get_sol_balance()
    msg = f"✅ *Trading automatique ACTIVÉ*\n\n"
    msg += f"💵 Solde: {balance:.4f} SOL\n"
    msg += f"📊 Stratégie:\n"
    msg += f"  • Sniper: ✅ ({trading_config.sniper_position_sol} SOL/trade)\n"
    msg += f"  • Recovered: ✅\n"
    msg += f"🎯 TP: +{trading_config.take_profit_pct}% | SL: {trading_config.stop_loss_pct}%\n"
    msg += f"\n⚠️ Le bot surveille et protège le capital automatiquement !"
    await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)


async def auto_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Désactiver le trading automatique"""
    global auto_trading_enabled
    auto_trading_enabled = False
    _save_state()  # Persister l'état pour survivre aux redémarrages
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
        _save_state()  # Persistance immédiate
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
        _save_state()  # Persistance immédiate
        if circuit_breaker:
            circuit_breaker.update_config(stop_loss_pct=-abs(sl))
        await update.message.reply_text(f"✅ Stop Loss mis à jour: {trading_config.stop_loss_pct}%")
    except ValueError:
        await update.message.reply_text("❌ Valeur invalide")


async def set_slippage_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /set_slippage <pct> - Modifier le slippage (en %)"""
    if not context.args:
        current_bps = trading_config.slippage_bps if hasattr(trading_config, 'slippage_bps') else trading_engine.config.slippage_bps
        current_pct = current_bps / 100
        await update.message.reply_text(
            f"💧 *Slippage actuel:* {current_pct:.0f}%\n\n"
            f"Usage: /set\\_slippage 50 (pour 50%)\n"
            f"Normal: 5-15% | Forcer vente: 50-80%",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    try:
        pct = float(context.args[0])
        if pct < 1 or pct > 99:
            await update.message.reply_text("❌ Valeur entre 1 et 99%")
            return
        bps = int(pct * 100)  # Convertir % en basis points
        trading_engine.config.slippage_bps = bps
        await update.message.reply_text(f"✅ Slippage mis à jour: {pct:.0f}% ({bps} bps)")
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
        _save_state()  # Persistance immédiate
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
        _save_state()  # Persistance immédiate
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
        # Mettre à jour le position_sizer (base durable, pas l'override temporaire)
        if position_sizer:
            position_sizer.base_size = size
        _save_state()  # Persistance immédiate
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
        sym = _safe_symbol(analysis.get("symbol"), token_address)
        # RÈGLE 5: Logger le BUY
        _log_trade(
            token_address=token_address,
            token_symbol=sym,
            strategy="manual",
            side="BUY",
            price=float(analysis.get("price_usd", 0) or 0),
            amount_sol=result['amount_sol'],
        )
        # Enregistrer dans le CircuitBreaker
        if circuit_breaker:
            entry_price = float(analysis.get("price_usd", 0) or 0)
            circuit_breaker.open_position(
                token_address=token_address,
                token_symbol=sym,
                entry_price=entry_price,
            )
        if capital_watchdog:
            capital_watchdog.register_position(
                token_address=token_address,
                token_symbol=sym,
                strategy="manual",
                amount_sol=result['amount_sol'],
                entry_price=float(analysis.get("price_usd", 0) or 0),
            )
        # Helius WebSocket: surveiller le nouveau token
        if helius_ws:
            helius_ws.watch_token(token_address)
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
        # RÈGLE 5: Logger le SELL
        _log_trade(
            token_address=token_address,
            token_symbol=position.token_symbol,
            strategy=position.strategy,
            side="SELL",
            pnl_pct=result.get('pnl_pct', 0),
            reason="Vente manuelle",
            price=position.current_price,
            amount_sol=position.amount_sol_invested,
        )
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


async def close_position_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/close_position <symbol> - Fermer une position dans le tracker SANS vendre (token mort)"""
    if not context.args:
        await update.message.reply_text(
            "❓ Usage: /close_position <symbol>\n"
            "Ex: /close_position PUMPNOTFUN\n\n"
            "Ferme la position dans le tracker sans essayer de vendre (pour les tokens morts)."
        )
        return

    symbol = context.args[0].upper()

    # Trouver la position par symbole
    target_pos = None
    target_addr = None
    for addr, pos in positions.positions.items():
        if pos.token_symbol.upper() == symbol:
            target_pos = pos
            target_addr = addr
            break

    if not target_pos:
        await update.message.reply_text(f"❌ Position '{symbol}' non trouvée.")
        return

    # Fermer dans tous les systèmes
    pnl = target_pos.pnl_pct
    try:
        # Enregistrer comme perte dans l'historique
        if pnl_tracker:
            pnl_tracker.record_trade(
                token_address=target_addr,
                token_symbol=target_pos.token_symbol,
                strategy=target_pos.strategy,
                pnl_pct=-100.0,  # Considéré comme perte totale
                pnl_sol=-(target_pos.amount_sol_invested),
                reason="Token mort (close_position)",
                duration_minutes=(datetime.now() - datetime.fromisoformat(target_pos.entry_time)).total_seconds() / 60
            )
        # Fermer dans le CircuitBreaker
        if circuit_breaker:
            circuit_breaker.close_position(target_addr)
        # Fermer dans le Watchdog
        if capital_watchdog:
            capital_watchdog.unregister_position(target_addr)
        # Retirer du WebSocket
        try:
            if price_monitor:
                asyncio.create_task(price_monitor.remove_token(target_addr))
            if helius_ws:
                helius_ws.unwatch_token(target_addr)
        except:
            pass
        # Fermer la position dans le tracker
        positions.close_position(target_addr)
        # Blacklister le token
        sl_blacklist[target_addr] = time.time() + 86400  # 24h
        _save_blacklist()

        msg = f"🗑️ *Position fermée (sans vente)*\n\n"
        msg += f"🪙 {target_pos.token_name} (${target_pos.token_symbol})\n"
        msg += f"📉 PnL final: {pnl:+.1f}% (enregistré comme -100%)\n"
        msg += f"💰 Perte: -{target_pos.amount_sol_invested:.4f} SOL\n"
        msg += f"🚫 Blacklisté 24h"
        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        await update.message.reply_text(f"❌ Erreur: {e}")


async def kill_zombies_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/kill_zombies - Fermer TOUTES les positions zombies (> 60 min ET PnL = 0%)"""
    if not positions.positions:
        await update.message.reply_text("✅ Aucune position ouverte.")
        return

    now = datetime.now(timezone.utc)
    zombies_killed = []
    addrs_to_kill = []

    for addr, pos in list(positions.positions.items()):
        try:
            entry_time = datetime.fromisoformat(pos.entry_time)
            if entry_time.tzinfo is None:
                entry_time = entry_time.replace(tzinfo=timezone.utc)
            age_minutes = (now - entry_time).total_seconds() / 60
        except:
            age_minutes = 9999  # Si entry_time invalide, considérer comme zombie

        # Zombie = > 60 min ET PnL figé à 0% (ou prix = entry)
        is_zombie = (age_minutes > 60 and abs(pos.pnl_pct) < 0.1)
        if is_zombie:
            addrs_to_kill.append((addr, pos, age_minutes))

    if not addrs_to_kill:
        await update.message.reply_text("✅ Aucune position zombie détectée.")
        return

    for addr, pos, age_min in addrs_to_kill:
        try:
            # Tenter la vente réelle
            sold = False
            if trading_engine:
                try:
                    result = trading_engine.execute_sell(addr, percentage=100)
                    if result:
                        sold = True
                except:
                    pass

            # Fermer dans tous les systèmes
            if pnl_tracker:
                pnl_tracker.record_trade(
                    token_address=addr,
                    token_symbol=pos.token_symbol,
                    strategy=pos.strategy,
                    pnl_pct=-100.0,
                    pnl_sol=-(pos.amount_sol_invested),
                    reason="Zombie killé (/kill_zombies)",
                    duration_minutes=age_min
                )
            if circuit_breaker:
                circuit_breaker.close_position(addr)
            if capital_watchdog:
                capital_watchdog.unregister_position(addr)
            if price_monitor:
                try:
                    asyncio.create_task(price_monitor.remove_token(addr))
                except:
                    pass
                if helius_ws:
                    helius_ws.unwatch_token(addr)
            positions.close_position(addr)
            sl_blacklist[addr] = time.time() + 86400
            _save_blacklist()

            zombies_killed.append(
                f"  💀 {pos.token_symbol} ({age_min:.0f}min) {'VENDU' if sold else 'FERMÉ'}"
            )
        except Exception as e:
            zombies_killed.append(f"  ❌ {pos.token_symbol}: erreur {e}")

    _save_state()
    msg = f"💀 *ZOMBIE KILLER*\n\n"
    msg += f"Positions fermées: {len(zombies_killed)}\n\n"
    msg += "\n".join(zombies_killed)
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

    # Vérification sécurité rapide (RPC direct, 150ms)
    import os
    _helius_key = os.environ.get("HELIUS_API_KEY", "")
    if _helius_key:
        from rpc_security_check import check_token_security
        rpc_sec = await check_token_security(token_mint, _helius_key)
        if not rpc_sec.is_safe:
            logger.info(f"[COPY] Token rejeté (Gate3 RPC): {signal.token_symbol} Score={rpc_sec.score}")
            seen_tokens.add(token_mint)
            return
    elif security_checker:
        is_safe, sec_reason = security_checker.quick_check(token_mint)
        if not is_safe:
            logger.info(f"[COPY] Token rejeté (RugCheck fallback): {signal.token_symbol} - {sec_reason}")
            seen_tokens.add(token_mint)
            return

    # Analyser le token
    analysis = api.analyze_token(token_mint)
    if not analysis:
        logger.warning(f"[COPY] Impossible d'analyser {signal.token_symbol}")
        return

    # ─── FIX PRIX ZÉRO ─────────────────────────────────────────
    _sym_copy = _safe_symbol(analysis.get("symbol") or signal.token_symbol, token_mint)
    ok, verified_price = verify_price_before_buy(
        token_address=token_mint,
        token_symbol=_sym_copy
    )
    if not ok:
        logger.info(f"[Skip] {_sym_copy} prix non disponible (copy_trade)")
        seen_tokens.add(token_mint)
        return
    # ────────────────────────────────────────────────────────────

    # Exécuter l'achat (copy trade)
    result = trading_engine.execute_buy(analysis, "copy_trade")
    if result:
        sym = _safe_symbol(analysis.get("symbol") or signal.token_symbol, token_mint)
        seen_tokens.add(token_mint)
        # Enregistrer dans le CircuitBreaker
        if circuit_breaker:
            entry_price = float(analysis.get("price_usd", 0) or 0)
            circuit_breaker.open_position(
                token_address=token_mint,
                token_symbol=sym,
                entry_price=entry_price,
            )
        if capital_watchdog:
            capital_watchdog.register_position(
                token_address=token_mint,
                token_symbol=sym,
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
                        if helius_ws:
                            helius_ws.unwatch_token(token_addr)
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

# Compteur d'échecs de vente par token (si 3 échecs → fermer la position)
sell_fail_counter: dict = {}


async def _flush_ws_notifications(context: ContextTypes.DEFAULT_TYPE):
    """
    Envoyer les notifications Telegram accumulées par le WS callback.
    Appelé par le polling job (qui a accès au context Telegram).
    """
    global _ws_notification_queue
    if not _ws_notification_queue:
        return

    # Copier et vider la queue
    notifications = _ws_notification_queue[:]
    _ws_notification_queue = []

    for notif in notifications:
        try:
            msg = ""
            if notif["type"] == "sell":
                pnl_emoji = '✅' if notif['pnl_pct'] > 0 else '❌'
                msg = f"{pnl_emoji} *VENTE WS INSTANTANÉE*\n\n"
                msg += f"🪙 {notif['name']} (${notif['symbol']})\n"
                msg += f"📈 PnL: {notif['pnl_pct']:+.1f}%\n"
                msg += f"📝 Raison: {notif['reason']}\n"
                msg += f"⚡ Source: Helius WebSocket (temps réel)\n"
                if notif.get('tx'):
                    msg += f"🔗 [TX](https://solscan.io/tx/{notif['tx']})"

            elif notif["type"] == "partial_tp":
                msg = f"✅ *PARTIAL TP 50% (WS)*\n\n"
                msg += f"🪙 {notif['name']} (${notif['symbol']})\n"
                msg += f"📈 PnL: {notif['pnl_pct']:+.1f}%\n"
                msg += f"📊 50% vendu, 50% court avec trailing\n"
                msg += f"⚡ Source: Helius WebSocket\n"
                if notif.get('tx'):
                    msg += f"🔗 [TX](https://solscan.io/tx/{notif['tx']})"

            elif notif["type"] == "fallback_on":
                # Notification désactivée (log only)
                logger.info(f"[WS] Fallback activé: {notif['reason']}")

            elif notif["type"] == "fallback_off":
                # Notification désactivée (log only)
                logger.info(f"[WS] WebSocket reconnecté: {notif['reason']}")

            if msg:
                for chat_id in subscribers:
                    try:
                        await context.bot.send_message(
                            chat_id=chat_id, text=msg,
                            parse_mode=ParseMode.MARKDOWN,
                            disable_web_page_preview=True
                        )
                    except:
                        pass
        except Exception as e:
            logger.error(f"Erreur flush notification WS: {e}")

async def sniper_monitor_job(context: ContextTypes.DEFAULT_TYPE):
    """
    Job FALLBACK (3s): vérifie les positions SNIPER UNIQUEMENT quand:
      - Le WebSocket est DOWN (fallback mode)
      - Un token n'est pas monitoré via WS (pool non supporté)
    
    Si le WS est connecté et le token est monitoré, ce job ne fait RIEN
    (le WS callback gère déjà le cb.check en temps réel).
    
    TOURNE TOUJOURS, même si auto_trading est off (protège le capital).
    """
    global sell_fail_counter
    if not trading_engine or not circuit_breaker:
        return

    # === ENVOYER LES NOTIFICATIONS WS EN ATTENTE ===
    await _flush_ws_notifications(context)

    sniper_positions = [p for p in positions.get_open_positions() if p.strategy == "sniper"]
    if not sniper_positions:
        return

    # Déterminer quels tokens ont besoin du polling
    ws_connected = price_monitor and price_monitor.is_connected
    ws_monitored = set(price_monitor.monitored_pools.keys()) if price_monitor else set()

    for pos in sniper_positions:
        try:
            # SKIP si le WS gère déjà ce token (pas besoin de polling)
            if ws_connected and pos.token_address in ws_monitored:
                # Juste vérifier le Time Stop (pas besoin de prix frais)
                cb_action = circuit_breaker.check(pos.token_address, pos.current_price)
                if cb_action.should_sell and cb_action.rule == "time_stop":
                    # Time Stop ne dépend pas du prix, on peut le déclencher ici
                    pass  # Sera géré ci-dessous
                else:
                    continue  # WS gère le reste

            # Récupérer le prix : Helius WS cache > DexScreener fallback
            current_price = 0.0
            if helius_ws and helius_ws.is_connected:
                current_price = helius_ws.get_price(pos.token_address)
            
            # Fallback DexScreener si Helius n'a pas de prix
            if current_price <= 0:
                analysis = api.analyze_token(pos.token_address)
                current_price = float(analysis.get("price_usd", 0) or 0) if analysis else 0
                # Mettre à jour le prix SOL/USD caché
                if analysis:
                    sol_price = analysis.get("sol_price_usd", 0)
                    if not sol_price:
                        price_native = float(analysis.get("price_native", 0) or 0)
                        price_usd_val = float(analysis.get("price_usd", 0) or 0)
                        if price_native > 0 and price_usd_val > 0:
                            sol_price = price_usd_val / price_native
                    if sol_price and sol_price > 50:
                        update_sol_price(sol_price)

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

            # ─── FIX MÉMOIRE — Token mort ─────────────────
            if capital_watchdog and pos.token_address in capital_watchdog.positions:
                wd_pos = capital_watchdog.positions[pos.token_address]
                if getattr(wd_pos, 'is_dead', False):
                    logger.warning(
                        f"[Zombie] Fermeture forcée "
                        f"{pos.token_symbol} — token mort"
                    )
                    result = trading_engine.execute_sell(
                        pos,
                        "💀 Token mort — prix=0 depuis 5min"
                    )
                    if result:
                        _log_trade(
                            token_address=pos.token_address,
                            token_symbol=pos.token_symbol,
                            strategy=pos.strategy,
                            side="SELL",
                            pnl_pct=-100.0,
                            reason="Token mort (prix=0 depuis 5min)",
                            price=0,
                            amount_sol=pos.amount_sol_invested,
                        )
                        if pnl_tracker:
                            pnl_tracker.record_trade(
                                token_address=pos.token_address,
                                token_symbol=pos.token_symbol,
                                strategy=pos.strategy,
                                entry_time=pos.entry_time,
                                amount_sol=pos.amount_sol_invested,
                                pnl_pct=-100.0,
                                exit_reason="Token mort (prix=0)",
                            )
                        if position_sizer:
                            position_sizer.record_result(False)
                            _save_state()
                        circuit_breaker.close_position(pos.token_address)
                        capital_watchdog.unregister_position(pos.token_address)
                        if price_monitor:
                            try:
                                await price_monitor.remove_token(pos.token_address)
                            except:
                                pass
                        if helius_ws:
                            helius_ws.unwatch_token(pos.token_address)
                        positions.close_position(pos.token_address)
                        sl_blacklist[pos.token_address] = time.time() + 86400
                        _save_blacklist()
                    continue
            # ───────────────────────────────────────────────

            # Vérifier via CircuitBreaker centralisé
            cb_action = circuit_breaker.check(pos.token_address, current_price)

            if cb_action.should_sell:
                # Partial TP: vendre 50% seulement, garder la position ouverte
                is_partial = cb_action.rule == "partial_take_profit"
                sell_pct = 50.0 if is_partial else 100.0

                result = trading_engine.execute_sell(pos, cb_action.reason, sell_pct=sell_pct)
                if not result:
                    # Vente échouée — incrémenter le compteur
                    sell_fail_counter[pos.token_address] = sell_fail_counter.get(pos.token_address, 0) + 1
                    fails = sell_fail_counter[pos.token_address]
                    logger.warning(f"[SNIPER 3s] Vente échouée pour {pos.token_symbol} ({fails}/3)")
                    if fails >= 3:
                        # Token mort — fermer la position sans vente
                        logger.warning(f"[SNIPER 3s] 🗑️ {pos.token_symbol}: 3 échecs de vente → fermeture forcée")
                        # RÈGLE 5: Logger le SELL forcé
                        _log_trade(
                            token_address=pos.token_address,
                            token_symbol=pos.token_symbol,
                            strategy=pos.strategy,
                            side="SELL",
                            pnl_pct=-100.0,
                            reason="Token mort (3 échecs vente)",
                            price=0,
                            amount_sol=pos.amount_sol_invested,
                        )
                        if pnl_tracker:
                            pnl_tracker.record_trade(
                                token_address=pos.token_address,
                                token_symbol=pos.token_symbol,
                                strategy=pos.strategy,
                                entry_time=pos.entry_time,
                                amount_sol=pos.amount_sol_invested,
                                pnl_pct=-100.0,
                                exit_reason="Token mort (3 échecs vente)",
                            )
                        if daily_pnl_guard:
                            daily_pnl_guard.record_trade(-pos.amount_sol_invested, is_stop_loss=True)
                        circuit_breaker.close_position(pos.token_address)
                        if capital_watchdog:
                            capital_watchdog.unregister_position(pos.token_address)
                        if price_monitor:
                            try:
                                await price_monitor.remove_token(pos.token_address)
                            except:
                                pass
                        if helius_ws:
                            helius_ws.unwatch_token(pos.token_address)
                        positions.close_position(pos.token_address)
                        sl_blacklist[pos.token_address] = time.time() + 86400  # 24h
                        _save_blacklist()
                        del sell_fail_counter[pos.token_address]
                        # Notifier
                        dead_msg = f"🗑️ *TOKEN MORT*\n\n"
                        dead_msg += f"🪙 {pos.token_name} (${pos.token_symbol})\n"
                        dead_msg += f"📉 3 tentatives de vente échouées\n"
                        dead_msg += f"💰 Perte: -{pos.amount_sol_invested:.4f} SOL\n"
                        dead_msg += f"🚫 Blacklisté 24h"
                        for chat_id in subscribers:
                            try:
                                await context.bot.send_message(
                                    chat_id=chat_id, text=dead_msg,
                                    parse_mode=ParseMode.MARKDOWN
                                )
                            except:
                                pass
                    continue
                if result:
                    # RÈGLE 5: Logger le SELL
                    _log_trade(
                        token_address=pos.token_address,
                        token_symbol=pos.token_symbol,
                        strategy=pos.strategy,
                        side="SELL",
                        pnl_pct=pos.pnl_pct,
                        reason=cb_action.reason,
                        price=current_price,
                        amount_sol=pos.amount_sol_invested,
                    )
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
                        _save_state()  # Persister streak + blacklist
                    # Retirer du WS seulement si vente totale
                    if not is_partial and price_monitor:
                        try:
                            await price_monitor.remove_token(pos.token_address)
                        except:
                            pass
                    if not is_partial and helius_ws:
                        helius_ws.unwatch_token(pos.token_address)
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

                # Post-trade notification désactivée (log only)
                logger.info(f"[PostTrade] {result['token_symbol']}: vendu {result['exit_pnl']:+.1f}% | 1h après: {result['pnl_1h_after']:+.1f}% | verdict: {result['verdict']}")
    except Exception as e:
        logger.error(f"Erreur post-trade analysis: {e}")


async def postmortem_analysis_job(context: ContextTypes.DEFAULT_TYPE):
    """
    Job d'analyse automatique des post-mortems (toutes les 6h).
    Agrège les résultats, détecte les patterns, et suggère des ajustements.
    """
    if not post_trade_analyzer:
        return

    try:
        insights = post_trade_analyzer.get_insights()
        if insights["status"] == "no_data":
            return

        total = insights["total_analyzed"]
        if total < 3:
            return  # Pas assez de données pour un rapport

        # Générer le rapport
        verdicts = insights.get("verdicts", {})
        hold_pct = insights.get("pct_hold_too_short", 0)
        perfect_pct = insights.get("pct_perfect_exit", 0)
        avg_missed = insights.get("avg_missed_profit", 0)
        avg_peak_time = insights.get("avg_time_to_peak", 0)
        avg_ath = insights.get("avg_ath_during_hold", 0)
        continued_up = insights.get("continued_up_after_sell", 0)
        crashed_after = insights.get("crashed_after_sell", 0)

        # Déterminer l'état global
        if perfect_pct >= 60:
            status_emoji = "✅"
            status_text = "Stratégie bien calibrée"
        elif hold_pct >= 40:
            status_emoji = "⚠️"
            status_text = "Tu vends trop tôt"
        else:
            status_emoji = "ℹ️"
            status_text = "Résultats mixtes"

        msg = f"📊 *RAPPORT POST-MORTEM AUTO*\n"
        msg += f"{status_emoji} {status_text}\n\n"
        msg += f"📊 Trades analysés: {total}\n"
        msg += f"✅ Ventes parfaites: {perfect_pct:.0f}%\n"
        msg += f"📈 Vendus trop tôt: {hold_pct:.0f}%\n"
        msg += f"💰 Profit moyen raté: {avg_missed:+.1f}%\n"
        msg += f"🚀 ATH moyen pendant hold: +{avg_ath:.0f}%\n"
        msg += f"⏱ Temps moyen au peak: {avg_peak_time:.0f} min\n\n"

        msg += f"*Après la vente:*\n"
        msg += f"  📈 A continué à monter: {continued_up}/{total}\n"
        msg += f"  📉 A crashé: {crashed_after}/{total}\n\n"

        # Recommandations auto
        recommendations = insights.get("recommendations", [])
        if recommendations:
            msg += "*💡 Recommandations:*\n"
            for rec in recommendations[:3]:
                msg += f"  {rec}\n"
            msg += "\n"

        # Suggestions d'ajustement concrètes
        suggestions = []
        if avg_missed > 20 and hold_pct > 35:
            new_tp = min(int(avg_ath * 0.6), 50)
            if new_tp > trading_config.take_profit_pct:
                suggestions.append(f"🎯 TP: {trading_config.take_profit_pct:.0f}% → {new_tp}%")
        if avg_peak_time > 25 and trading_config.time_stop_minutes < avg_peak_time:
            new_ts = min(int(avg_peak_time * 1.2), 45)
            suggestions.append(f"⏰ Time Stop: {trading_config.time_stop_minutes}min → {new_ts}min")
        if crashed_after > total * 0.7:
            suggestions.append("✅ SL/TP actuels sont bons (70%+ crash après vente)")

        if suggestions:
            msg += "*🔧 Ajustements suggérés:*\n"
            for s in suggestions:
                msg += f"  {s}\n"
            msg += "\nℹ️ Utilise /set\_tp, /set\_sl, /set\_timestop pour appliquer"

        # Envoyer le rapport
        for chat_id in subscribers:
            try:
                await context.bot.send_message(
                    chat_id=chat_id, text=msg,
                    parse_mode=ParseMode.MARKDOWN
                )
            except:
                pass

        logger.info(f"[POSTMORTEM] Rapport auto envoyé: {total} trades analysés")

    except Exception as e:
        logger.error(f"Erreur postmortem_analysis_job: {e}")


# ============================================================
# DAILY SUMMARY JOB (20h UTC)
# ============================================================

async def daily_summary_job(context: ContextTypes.DEFAULT_TYPE):
    """
    Job quotidien — DÉSACTIVÉ (log only).
    Résumé disponible via /today.
    """
    logger.info("[DailySummary] Job désactivé — utiliser /today")
    return
    # --- Code original désactivé ci-dessous ---
    if not pnl_tracker or not subscribers:
        return

    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Récupérer les trades du jour depuis trades.json
        trades_today = []
        if os.path.exists(TRADES_LOG_FILE):
            try:
                with open(TRADES_LOG_FILE, "r") as f:
                    all_trades = json.load(f)
                trades_today = [
                    t for t in all_trades
                    if t.get("timestamp", "").startswith(today)
                ]
            except Exception:
                pass

        total_trades = len(trades_today)
        sells = [t for t in trades_today if t.get("side") == "SELL"]
        buys = [t for t in trades_today if t.get("side") == "BUY"]
        wins = [t for t in sells if t.get("pnl_pct", 0) > 0]
        losses = [t for t in sells if t.get("pnl_pct", 0) <= 0]
        total_pnl_pct = sum(t.get("pnl_pct", 0) for t in sells)
        avg_pnl = total_pnl_pct / len(sells) if sells else 0

        best = max(sells, key=lambda t: t.get("pnl_pct", 0), default=None)
        worst = min(sells, key=lambda t: t.get("pnl_pct", 0), default=None)

        best_str = f"+{best['pnl_pct']:.1f}% ({best.get('token', '?')})" if best else "—"
        worst_str = f"{worst['pnl_pct']:.1f}% ({worst.get('token', '?')})" if worst else "—"

        # Post-mortems du jour (gains manqués)
        missed_avg = 0
        try:
            import sqlite3
            conn = sqlite3.connect(os.path.join(DATA_DIR, "postmortem.db"))
            c = conn.cursor()
            c.execute("""
                SELECT AVG(missed_gain), COUNT(*)
                FROM postmortem
                WHERE exit_time LIKE ?
            """, (f"{today}%",))
            row = c.fetchone()
            conn.close()
            if row and row[0]:
                missed_avg = row[0]
        except Exception:
            pass

        # Recommandation
        recommandation = ""
        if missed_avg > 30:
            recommandation = (
                f"\n\u2699\ufe0f *Recommandation :*\n"
                f"Gain moyen manqu\u00e9 : `+{missed_avg:.1f}%`\n"
                f"\u2192 TP trop t\u00f4t \u2014 envisager Trailing plus large"
            )
        elif missed_avg < 5 and len(sells) > 3:
            recommandation = (
                f"\n\u2699\ufe0f *Recommandation :*\n"
                f"Gain manqu\u00e9 faible : `+{missed_avg:.1f}%`\n"
                f"\u2192 Param\u00e8tres bien calibr\u00e9s \u2705"
            )

        # Solde actuel
        solde_sol = 0.0
        try:
            solde_sol = trading_engine.wallet.get_sol_balance()
        except Exception:
            pass

        # Message Telegram
        emoji_jour = "\u2705" if avg_pnl > 0 else "\ud83d\udd34"

        msg = (
            f"\ud83d\udcca *R\u00e9sum\u00e9 du {today}*\n\n"
            f"{emoji_jour} *PnL moyen :* `{avg_pnl:+.1f}%`\n"
            f"\ud83d\udcc8 *Trades :* `{total_trades}` "
            f"(\ud83d\udcb0{len(buys)} achats / \ud83d\udcb8{len(sells)} ventes)\n"
            f"\ud83c\udfaf *Win Rate :* `{len(wins)}/{len(sells)}` "
            f"({(len(wins)/len(sells)*100) if sells else 0:.0f}%)\n"
            f"\ud83d\ude80 *Meilleur :* `{best_str}`\n"
            f"\ud83d\udca5 *Pire :* `{worst_str}`\n"
            f"\ud83d\udcb0 *Solde :* `{solde_sol:.4f} SOL`\n"
            f"{recommandation}\n\n"
            f"\ud83d\udd50 Prochain r\u00e9sum\u00e9 demain \u00e0 20h UTC"
        )

        for chat_id in subscribers:
            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=msg,
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception as e:
                logger.error(f"[DailySummary] Telegram error: {e}")

        logger.info(f"[DailySummary] R\u00e9sum\u00e9 envoy\u00e9 \u2014 {total_trades} trades")

    except Exception as e:
        logger.error(f"[DailySummary] Erreur: {e}")


# ============================================================
# CRITICAL MONITOR JOB (60s)
# ============================================================

# Anti-spam : une alerte par type par heure
CRITICAL_SL_THRESHOLD = -20.0
CRITICAL_ZOMBIE_MINUTES = 60
CRITICAL_RAM_PCT = 80
CRITICAL_GAP_SECONDS = 30
_last_alert_time = {}


def _can_alert(alert_type: str, cooldown_seconds: int = 3600) -> bool:
    """\u00c9vite le spam d'alertes du m\u00eame type."""
    now = time.time()
    last = _last_alert_time.get(alert_type, 0)
    if now - last > cooldown_seconds:
        _last_alert_time[alert_type] = now
        return True
    return False


async def _send_critical_alert(context, alert_type: str, message: str):
    """Envoie une alerte critique sur Telegram (cooldown 1h par type)."""
    if not _can_alert(alert_type):
        return
    for chat_id in subscribers:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception as e:
            logger.error(f"[CriticalAlert] Telegram error: {e}")


async def critical_monitor_job(context: ContextTypes.DEFAULT_TYPE):
    """
    Job toutes les 60s \u2014 v\u00e9rifie les 4 conditions critiques.
    N'envoie une alerte QUE si condition critique atteinte.
    """
    try:
        # CRITIQUE 1 \u2014 SL > -20%
        if positions:
            for pos in positions.get_open_positions():
                pnl = getattr(pos, 'pnl_pct', 0) or 0
                if pnl <= CRITICAL_SL_THRESHOLD:
                    symbol = getattr(pos, 'token_symbol', pos.token_address[:8])
                    await _send_critical_alert(
                        context,
                        alert_type=f"sl_{pos.token_address[:8]}",
                        message=(
                            f"\ud83d\udea8 *ALERTE SL CRITIQUE*\n\n"
                            f"\ud83c\udff7 Token : `{symbol}`\n"
                            f"\ud83d\udcc9 PnL : `{pnl:.1f}%`\n"
                            f"\u26a0\ufe0f SL universel -25% imminent\n"
                            f"\ud83d\udd50 {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC"
                        )
                    )

        # CRITIQUE 2 \u2014 Zombie > 60 min
        if positions:
            for pos in positions.get_open_positions():
                entry_time = getattr(pos, 'entry_time', None)
                if entry_time:
                    try:
                        if isinstance(entry_time, str):
                            entry = datetime.fromisoformat(entry_time)
                        else:
                            entry = entry_time
                        if entry.tzinfo is None:
                            entry = entry.replace(tzinfo=timezone.utc)
                        age_min = (datetime.now(timezone.utc) - entry).total_seconds() / 60

                        if age_min > CRITICAL_ZOMBIE_MINUTES:
                            symbol = getattr(pos, 'token_symbol', pos.token_address[:8])
                            pnl = getattr(pos, 'pnl_pct', 0) or 0
                            await _send_critical_alert(
                                context,
                                alert_type=f"zombie_{pos.token_address[:8]}",
                                message=(
                                    f"\ud83e\udddf *POSITION ZOMBIE D\u00c9TECT\u00c9E*\n\n"
                                    f"\ud83c\udff7 Token : `{symbol}`\n"
                                    f"\u23f1 \u00c2ge : `{age_min:.0f} min`\n"
                                    f"\ud83d\udcca PnL : `{pnl:+.1f}%`\n"
                                    f"\u2192 Utilise /kill\\_zombies pour fermer"
                                )
                            )
                    except Exception:
                        pass

        # CRITIQUE 3 \u2014 RAM > 80%
        try:
            import psutil
            ram_pct = psutil.virtual_memory().percent
            if ram_pct > CRITICAL_RAM_PCT:
                await _send_critical_alert(
                    context,
                    alert_type="ram_critical",
                    message=(
                        f"\u26a0\ufe0f *RAM CRITIQUE*\n\n"
                        f"\ud83d\udcbe Utilisation : `{ram_pct:.1f}%`\n"
                        f"\ud83d\udd34 Risque de crash OOM\n"
                        f"\u2192 Render peut red\u00e9marrer\n"
                        f"\u2192 auto\\_trading sera relu depuis state.json"
                    )
                )
        except ImportError:
            pass

        # CRITIQUE 4 \u2014 Bot mort (gap watchdog)
        if capital_watchdog and hasattr(capital_watchdog, 'positions'):
            for token_addr, wpos in capital_watchdog.positions.items():
                last_check = getattr(wpos, 'last_check_time', 0) or 0
                if last_check > 0:
                    gap = time.time() - last_check
                    if gap > CRITICAL_GAP_SECONDS:
                        symbol = getattr(wpos, 'token_symbol', token_addr[:8])
                        await _send_critical_alert(
                            context,
                            alert_type=f"dead_{token_addr[:8]}",
                            message=(
                                f"\ud83d\udc80 *BOT AVEUGLE D\u00c9TECT\u00c9*\n\n"
                                f"\ud83c\udff7 Token : `{symbol}`\n"
                                f"\u23f1 Gap : `{gap:.0f}s` sans v\u00e9rification\n"
                                f"\ud83d\udea8 Position non surveill\u00e9e"
                            )
                        )

        # CRITIQUE 5 — Gate 1 DexScreener dégradé (>50% échec sur 10 min)
        if api:
            stats_10m = api.get_gate1_stats_window(600)  # 10 minutes
            if stats_10m["total"] >= 5 and stats_10m["failure_rate"] > 50:
                await _send_critical_alert(
                    context,
                    alert_type="gate1_degraded",
                    message=(
                        f"⚠️ *GATE 1 DÉGRADÉ*\n\n"
                        f"❌ {stats_10m['failures']}/{stats_10m['total']} tokens rejetés (None)\n"
                        f"📉 Taux échec : `{stats_10m['failure_rate']:.0f}%` (10 min)\n"
                        f"⚠️ Possible rate limit DexScreener\n"
                        f"🕐 {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC"
                    )
                )

            # Alerte rate limit immédiate (429 détecté)
            if api._rate_limit_paused_until > time.time():
                remaining = api._rate_limit_paused_until - time.time()
                await _send_critical_alert(
                    context,
                    alert_type="gate1_ratelimit",
                    message=(
                        f"🚨 *RATE LIMIT DEXSCREENER*\n\n"
                        f"🛑 HTTP 429 reçu\n"
                        f"⏸ Bot ralenti — attente `{remaining:.0f}s`\n"
                        f"🕐 {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC"
                    )
                )

    except Exception as e:
        logger.error(f"[CriticalMonitor] Erreur: {e}")


# ============================================================
# AUTO-CALIBRATION JOB (21h UTC)
# ============================================================

async def auto_calibration_job(context: ContextTypes.DEFAULT_TYPE):
    """
    Job quotidien \u00e0 21h UTC.
    Analyse les post-mortems et ajuste les param\u00e8tres automatiquement.
    """
    logger.info("[AutoCalib] D\u00e9marrage analyse post-mortems...")

    try:
        # Analyser les donn\u00e9es
        analysis = analyze_postmortems(DATA_DIR)

        if analysis["status"] == "insufficient_data":
            msg = (
                f"\ud83d\udcca *Auto-Calibration*\n\n"
                f"\u23f3 Donn\u00e9es insuffisantes\n"
                f"Trades analys\u00e9s : `{analysis['trades_analyzed']}`\n"
                f"Minimum requis  : `{analysis['min_required']}`\n\n"
                f"\u2192 Calibration disponible dans "
                f"`{analysis['min_required'] - analysis['trades_analyzed']}` trades"
            )
            # Notification Telegram désactivée (log only)
            return

        if analysis["status"] in ("error", "no_database", "no_table"):
            logger.info(f"[AutoCalib] Pas de donn\u00e9es: {analysis['status']}")
            return

        adjustments = analysis.get("adjustments", {})

        # Appliquer les changements dans state.json
        state = _load_state()
        changes = apply_adjustments(adjustments, state)

        if changes:
            # Sauvegarder state.json avec les nouveaux param\u00e8tres
            for param, data in changes.items():
                state[param] = data["new"]
            state["last_updated"] = datetime.now(timezone.utc).isoformat()
            try:
                with open(STATE_FILE, "w") as f:
                    json.dump(state, f, indent=2)
            except IOError:
                pass

            # Appliquer au circuit_breaker en m\u00e9moire
            if circuit_breaker:
                cb_updates = {}
                if "tp_pct" in changes:
                    cb_updates["take_profit_pct"] = changes["tp_pct"]["new"]
                if "trailing_sl" in changes:
                    cb_updates["trailing_sl_pct"] = changes["trailing_sl"]["new"]
                if "time_stop_sniper" in changes:
                    cb_updates["time_stop_sniper_min"] = changes["time_stop_sniper"]["new"]
                if cb_updates:
                    circuit_breaker.update_config(**cb_updates)

        # Construire le rapport Telegram
        lines = [
            f"\ud83e\udd16 *Auto-Calibration \u2014 {datetime.now(timezone.utc).strftime('%Y-%m-%d')}*\n",
            f"\ud83d\udcca Trades analys\u00e9s : `{analysis['trades_analyzed']}`\n"
        ]

        if changes:
            lines.append("*Param\u00e8tres ajust\u00e9s :*\n")
            for param, data in changes.items():
                lines.append(
                    f"{data['direction']} `{param}`\n"
                    f"   `{data['old']}` \u2192 `{data['new']}`\n"
                    f"   _{data['reason']}_\n"
                )
        else:
            lines.append("\u2705 *Tous les param\u00e8tres sont optimaux*\n")
            lines.append("Aucun ajustement n\u00e9cessaire\n")

        lines.append(f"\n\ud83d\udd50 Prochaine calibration demain \u00e0 21h UTC")

        msg = "\n".join(lines)

        # Notification Telegram désactivée (log only)

        logger.info(f"[AutoCalib] Termin\u00e9 \u2014 {len(changes)} changements")

    except Exception as e:
        logger.error(f"[AutoCalib] Erreur globale: {e}")


async def ws_token_processor_job(context: ContextTypes.DEFAULT_TYPE):
    """
    Job (5s): consomme les tokens détectés par le WebSocket Helius
    et les passe dans le pipeline complet (OnchainScorer → filtres → execute_buy).
    Avantage: détection en ~500ms vs 45s (polling DexScreener).
    """
    global auto_trading_enabled

    if not auto_trading_enabled or not trading_engine:
        return

    # Daily PnL Guard: ne pas trader si en pause
    if daily_pnl_guard and daily_pnl_guard.is_paused():
        return

    # Couvre-feu 3h-7h UTC
    current_hour_utc = datetime.now(timezone.utc).hour
    if 3 <= current_hour_utc < 7:
        return

    # Traiter max 3 tokens par cycle (éviter de bloquer le bot)
    processed = 0
    if _ws_new_token_queue:
        logger.info(f"[WS-PROC] 📥 Queue: {len(_ws_new_token_queue)} tokens à traiter")
    while _ws_new_token_queue and processed < 3:
        token_data = _ws_new_token_queue.popleft()
        address = token_data["address"]
        ws_price = token_data["price_usd"]
        t_detected = token_data["timestamp"]
        processed += 1

        try:
            # Retry metadata
            retry_count = token_data.get("retry_count", 0)
            first_seen = token_data.get("first_seen", t_detected)

            # Skip si trop vieux (> 60s depuis first_seen = plus pertinent pour snipe)
            age_s = time.time() - first_seen
            if age_s > 60:
                if retry_count > 0:
                    logger.info(f"[WS-PROC] [Gate1] REJETÉ {address[:8]}... trop vieux ({age_s:.0f}s) après {retry_count} tentatives")
                else:
                    logger.info(f"[WS-PROC] Skip {address[:8]}... (trop vieux: {age_s:.0f}s)")
                continue

            # Déjà en position ?
            if address in positions.positions:
                continue

            # ─── GATE 1: Analyse DexScreener (prix, MC, liquidité) ───
            await asyncio.sleep(0.5)  # Rate limit DexScreener
            analysis = api.analyze_token(address)
            if not analysis:
                if retry_count >= 2:
                    # 3 tentatives (0, 1, 2) épuisées → rejeter définitivement
                    logger.info(f"[WS-PROC] [Gate1] REJETÉ après 3 tentatives DexScreener - "
                               f"token non indexé {address[:8]}...")
                    continue
                # Re-queuer avec retry_count incrémenté (sera traité ~10s plus tard)
                _ws_new_token_queue.append({
                    "address": address,
                    "price_usd": ws_price,
                    "timestamp": time.time(),  # Reset timestamp pour le délai
                    "first_seen": first_seen,  # Conserver le timestamp original
                    "retry_count": retry_count + 1,
                    "source": token_data.get("source", "websocket"),  # Conserver source
                })
                logger.info(f"[WS-PROC] [Gate1] Re-queue {address[:8]}... "
                           f"(tentative {retry_count + 1}/3, retry dans ~10s)")
                continue

            token_name = analysis.get('name', address[:12])
            latency_ms = (time.time() - t_detected) * 1000
            logger.info(f"[WS-PROC] 📡 Analyse {token_name} (latence {latency_ms:.0f}ms)")

            # ─── GATE 2: MC minimum (seuil réduit pour WS pump.fun) ───
            mc = analysis.get("market_cap", 0) or 0
            _source = token_data.get("source", "")
            min_mc = 2_000 if _source == "websocket" else trading_config.sniper_min_mc
            if mc < min_mc:
                logger.info(f"🚫 REJETÉ WS (MC trop faible): {token_name} "
                           f"MC=${mc:,.0f} < ${min_mc:,.0f}")
                continue

            # ─── GATE 3: OnchainScorer (LP exemptée pour pump.fun) ───
            oc_score = None
            if onchain_scorer:
                oc_score = await onchain_scorer.score_token(address, source="pump.fun")
                if not oc_score.safe:
                    logger.info(f"🚫 REJETÉ WS (onchain score={oc_score.score}): "
                               f"{token_name} — {oc_score.summary}")
                    continue
                logger.info(f"✅ WS Onchain OK (score={oc_score.score}): {token_name} — {oc_score.summary}")
            else:
                if token_filter:
                    mint_ok, mint_reason = await token_filter.quick_mint_check(address)
                    if not mint_ok:
                        logger.info(f"🚫 REJETÉ WS (on-chain): {token_name} - {mint_reason}")
                        continue

            # ─── GATE 4: RPC Security Check ───
            if not onchain_scorer or (onchain_scorer and oc_score and oc_score.errors):
                from rpc_security_check import check_token_security
                _helius_key = os.environ.get("HELIUS_API_KEY", "")
                if _helius_key:
                    rpc_sec = await check_token_security(address, _helius_key)
                    if not rpc_sec.is_safe:
                        logger.info(f"🚫 REJETÉ WS (Gate RPC): {token_name} "
                                   f"Score={rpc_sec.score} - {', '.join(rpc_sec.reasons)}")
                        continue

            # ─── GATE 5: LP Burned ───
            if token_filter and not (onchain_scorer and oc_score and oc_score.lp_burned):
                pair_addr = analysis.get('pair_address', '')
                if pair_addr:
                    tf_result = await token_filter.check(
                        address, require_lp_burned=True, pair_address=pair_addr
                    )
                    if not tf_result.is_safe:
                        logger.info(f"🚫 REJETÉ WS (LP check): {token_name} - {tf_result.rejection_reason}")
                        continue

            # ─── GATE 6: Liquidité (désactivé pour WS pump.fun - bonding curve = toujours liquide) ───
            if liquidity_guard and _source != "websocket":
                liq_usd = analysis.get("liquidity_usd", 0)
                can_buy_liq, liq_reason = liquidity_guard.can_buy(address, liq_usd, strategy="sniper")
                if not can_buy_liq:
                    logger.info(f"{liq_reason} - Skip WS {token_name}")
                    continue

            # ─── GATE 7: Vérification prix non-zéro ───
            _sym = _safe_symbol(analysis.get("symbol"), address)
            ok, verified_price = verify_price_before_buy(
                token_address=address,
                token_symbol=_sym
            )
            if not ok:
                logger.info(f"[WS-PROC] Skip {_sym} prix non disponible")
                continue

            # ─── SMART ENTRY: ajouter à la watchlist si actif ───
            if smart_entry:
                added, watch_reason = smart_entry.first_check(analysis)
                if added:
                    logger.info(f"🧠 WS → Watchlist: {token_name} - {watch_reason}")
                    continue

            # ─── ACHAT: Sniper direct ───
            should_snipe, reason = trading_engine.should_snipe(analysis)
            if should_snipe:
                result = trading_engine.execute_buy(analysis, "sniper")
                if result:
                    sym = _safe_symbol(analysis.get("symbol"), address)
                    _log_trade(
                        token_address=address,
                        token_symbol=sym,
                        strategy="sniper",
                        side="BUY",
                        price=float(analysis.get("price_usd", 0) or 0),
                        amount_sol=result['amount_sol'],
                    )
                    if circuit_breaker:
                        entry_price = float(analysis.get("price_usd", 0) or 0)
                        circuit_breaker.open_position(
                            token_address=address,
                            token_symbol=sym,
                            entry_price=entry_price,
                        )
                    if capital_watchdog:
                        capital_watchdog.register_position(
                            token_address=address,
                            token_symbol=sym,
                            strategy="sniper",
                            amount_sol=result['amount_sol'],
                            entry_price=float(analysis.get("price_usd", 0) or 0),
                        )
                    if liquidity_guard:
                        liq_usd = analysis.get("liquidity_usd", 0)
                        if liq_usd > 0:
                            liquidity_guard.register_position(address, liq_usd)
                    if helius_ws:
                        helius_ws.watch_token(address)

                    # Notification Telegram
                    total_latency = (time.time() - t_detected) * 1000
                    msg = f"⚡ *SNIPE WS* (détection {total_latency:.0f}ms)\n\n"
                    msg += f"🪙 {analysis['name']} (${analysis.get('symbol', '???')})\n"
                    msg += f"💵 {result['amount_sol']} SOL\n"
                    msg += f"📊 MC: ${analysis.get('market_cap', 0):,.0f}\n"
                    msg += f"💧 Liq: ${analysis.get('liquidity_usd', 0):,.0f}\n"
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
                    logger.info(f"⚡ SNIPE WS EXÉCUTÉ: {sym} | {result['amount_sol']} SOL | latence {total_latency:.0f}ms")
                    await asyncio.sleep(trading_config.cooldown_seconds)
            else:
                logger.debug(f"[WS-PROC] {token_name} passé tous les filtres mais should_snipe=False")

        except Exception as e:
            logger.error(f"[WS-PROC] Erreur traitement {address[:8]}...: {e}")


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
                    # RÈGLE 5: Logger le SELL
                    _log_trade(
                        token_address=pos.token_address,
                        token_symbol=pos.token_symbol,
                        strategy=pos.strategy,
                        side="SELL",
                        pnl_pct=pos.pnl_pct,
                        reason=reason,
                        price=current_price,
                        amount_sol=pos.amount_sol_invested,
                    )
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

                    _save_state()  # Persister streak + blacklist immédiatement

                    # Détecter un RUG PULL (-80%+) et blacklister le créateur
                    if pos.pnl_pct < -80 and security_checker:
                        logger.warning(f"🚨 RUG PULL DÉTECTÉ: {pos.token_name} ({pos.pnl_pct:.0f}%)")
                        # Récupérer le créateur depuis le cache RugCheck
                        cached_report = security_checker._cache.get(pos.token_address)
                        if cached_report:
                            # Invalider le cache pour ce token
                            del security_checker._cache[pos.token_address]
                        # NOUVEAU: Blacklister le DEPLOYER (pas juste le token)
                        if correlation_filter:
                            deployer_rug = correlation_filter.get_deployer_for_token(pos.token_address)
                            if deployer_rug:
                                correlation_filter.blacklist_deployer(
                                    deployer_rug, reason=f"Rug pull {pos.token_symbol} ({pos.pnl_pct:.0f}%)"
                                )
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
    # ─── COUVRE-FEU 3h-7h UTC (heures creuses pump.fun = rugs nocturnes) ───
    current_hour_utc = datetime.now(timezone.utc).hour
    if 3 <= current_hour_utc < 7:
        return  # Skip silencieusement, pas de log spam
    # ─────────────────────────────────────────────────────────────────

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

                # 🚨 GATE MC MINIMUM RECOVERED $50K (token établi requis)
                mc = analysis.get("market_cap", 0) or 0
                if mc < trading_config.recovered_min_mc:
                    logger.info(f"🚫 REJETÉ (MC trop faible RECOVERED): {analysis.get('name', address[:12])} "
                               f"MC=${mc:,.0f} < ${trading_config.recovered_min_mc:,.0f}")
                    smart_entry.consume_signal(address)
                    seen_tokens.add(address)
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

                    # Vérifier la corrélation avant d'acheter (RENFORCÉ: deployer on-chain)
                    if correlation_filter:
                        # Récupérer le deployer depuis token_filter si disponible
                        deployer_addr = None
                        if token_filter:
                            deployer_addr = token_filter.get_deployer(address)
                        can_buy, corr_reason = correlation_filter.can_buy(
                            token_address=address,
                            token_name=signal.token_name,
                            token_symbol=signal.token_symbol,
                            deployer=deployer_addr,
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

                    # ─── FIX PRIX ZÉRO ─────────────────────────────────────────
                    _sym = _safe_symbol(signal.token_symbol or analysis.get("symbol"), address)
                    ok, verified_price = verify_price_before_buy(
                        token_address=address,
                        token_symbol=_sym
                    )
                    if not ok:
                        logger.info(f"[Skip] {_sym} prix non disponible")
                        smart_entry.consume_signal(address)
                        seen_tokens.add(address)
                        if position_sizer:
                            trading_config.position_size_sol = original_size
                            trading_config.sniper_position_sol = original_size
                        continue
                    # ────────────────────────────────────────────────────────

                    # Signal confirmé ! Exécuter l'achat
                    result = trading_engine.execute_buy(analysis, signal.strategy)

                    # Restaurer la taille originale
                    if position_sizer:
                        trading_config.position_size_sol = original_size
                        trading_config.sniper_position_sol = original_size
                    if result:
                        smart_entry.consume_signal(address)
                        seen_tokens.add(address)

                        # RÈGLE 5: Logger le BUY
                        sym = _safe_symbol(signal.token_symbol or analysis.get("symbol"), address)
                        _log_trade(
                            token_address=address,
                            token_symbol=sym,
                            strategy=signal.strategy,
                            side="BUY",
                            price=float(analysis.get("price_usd", 0) or 0),
                            amount_sol=result['amount_sol'],
                        )

                        # Enregistrer dans le CircuitBreaker
                        if circuit_breaker:
                            entry_price = float(analysis.get("price_usd", 0) or 0)
                            circuit_breaker.open_position(
                                token_address=address,
                                token_symbol=sym,
                                entry_price=entry_price,
                            )
                        if capital_watchdog:
                            capital_watchdog.register_position(
                                token_address=address,
                                token_symbol=sym,
                                strategy=signal.strategy,
                                amount_sol=result['amount_sol'],
                                entry_price=float(analysis.get("price_usd", 0) or 0),
                            )

                        # Enregistrer dans le filtre de corrélation (avec deployer on-chain)
                        if correlation_filter:
                            deployer_for_reg = None
                            if token_filter:
                                deployer_for_reg = token_filter.get_deployer(address)
                            correlation_filter.register_position(
                                token_address=address,
                                token_name=signal.token_name,
                                token_symbol=signal.token_symbol,
                                deployer=deployer_for_reg,
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
                        # Helius WebSocket: surveiller le nouveau token
                        if helius_ws:
                            helius_ws.watch_token(address)

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

            # 🚨 GATE MC MINIMUM SNIPER $5K — AVANT TOUT (bloque les nano-rugs)
            mc = analysis.get("market_cap", 0) or 0
            if mc < trading_config.sniper_min_mc:
                logger.info(f"🚫 REJETÉ (MC trop faible SNIPER): {analysis.get('name', address[:12])} "
                           f"MC=${mc:,.0f} < ${trading_config.sniper_min_mc:,.0f}")
                seen_tokens.add(address)
                if len(seen_tokens) > MAX_SEEN_TOKENS:
                    seen_tokens.clear()
                continue

            # ⚡ SCORING ON-CHAIN DIRECT (remplace RugCheck comme source primaire, ~150ms)
            if onchain_scorer:
                oc_score = await onchain_scorer.score_token(address)
                if not oc_score.safe:
                    logger.info(f"🚫 REJETÉ (onchain score={oc_score.score}): "
                               f"{analysis.get('name', address[:12])} — {oc_score.summary}")
                    seen_tokens.add(address)
                    if len(seen_tokens) > MAX_SEEN_TOKENS:
                        seen_tokens.clear()
                    continue
                logger.info(f"✅ Onchain OK (score={oc_score.score}): "
                           f"{analysis.get('name', address[:12])} — {oc_score.summary}")
            else:
                # Fallback: anciens filtres si onchain_scorer indisponible
                if token_filter:
                    mint_ok, mint_reason = await token_filter.quick_mint_check(address)
                    if not mint_ok:
                        logger.info(f"🚫 REJETÉ (on-chain): {analysis.get('name', address[:12])} - {mint_reason}")
                        seen_tokens.add(address)
                        if len(seen_tokens) > MAX_SEEN_TOKENS:
                            seen_tokens.clear()
                        continue

            # 🛡️ GATE 3 — RPC Security Check (remplace RugCheck, 150ms vs 2-5s)
            if not onchain_scorer or (onchain_scorer and oc_score.errors):
                from rpc_security_check import check_token_security
                import os
                _helius_key = os.environ.get("HELIUS_API_KEY", "")
                if _helius_key:
                    rpc_sec = await check_token_security(address, _helius_key)
                    if not rpc_sec.is_safe:
                        logger.info(f"🚫 REJETÉ (Gate3 RPC): {analysis.get('name', address[:12])} "
                                   f"Score={rpc_sec.score} - {', '.join(rpc_sec.reasons)}")
                        seen_tokens.add(address)
                        if len(seen_tokens) > MAX_SEEN_TOKENS:
                            seen_tokens.clear()
                        continue
                    logger.info(f"✅ Gate3 RPC OK (score={rpc_sec.score}): {analysis.get('name', address[:12])}")
                elif security_checker:
                    # Fallback RugCheck si pas de clé Helius
                    token_age = analysis.get('age_hours', 999)
                    check_strategy = "sniper" if token_age < 1 else "momentum"
                    is_safe, security_reason = security_checker.quick_check(address, strategy=check_strategy)
                    if not is_safe:
                        logger.info(f"🚫 REJETÉ (RugCheck fallback): {analysis.get('name', address[:12])} - {security_reason}")
                        seen_tokens.add(address)
                        if len(seen_tokens) > MAX_SEEN_TOKENS:
                            seen_tokens.clear()
                        continue

            # 🔥 FILTRE LP BURNED (vérification on-chain complète si pas déjà confirmé par scorer)
            if token_filter and not (onchain_scorer and oc_score.lp_burned):
                pair_addr = analysis.get('pair_address', '')
                if pair_addr:
                    tf_result = await token_filter.check(
                        address, require_lp_burned=True, pair_address=pair_addr
                    )
                    if not tf_result.is_safe:
                        logger.info(f"🚫 REJETÉ (LP check): {analysis.get('name', address[:12])} - {tf_result.rejection_reason}")
                        seen_tokens.add(address)
                        if len(seen_tokens) > MAX_SEEN_TOKENS:
                            seen_tokens.clear()
                        continue

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
                # ─── FIX PRIX ZÉRO ─────────────────────────────────────────
                _sym_snipe = _safe_symbol(analysis.get("symbol"), address)
                ok, verified_price = verify_price_before_buy(
                    token_address=address,
                    token_symbol=_sym_snipe
                )
                if not ok:
                    logger.info(f"[Skip] {_sym_snipe} prix non disponible")
                    seen_tokens.add(address)
                    continue
                # ────────────────────────────────────────────────────────

                result = trading_engine.execute_buy(analysis, "sniper")
                if result:
                    sym = _safe_symbol(analysis.get("symbol"), address)
                    # RÈGLE 5: Logger le BUY
                    _log_trade(
                        token_address=address,
                        token_symbol=sym,
                        strategy="sniper",
                        side="BUY",
                        price=float(analysis.get("price_usd", 0) or 0),
                        amount_sol=result['amount_sol'],
                    )
                    # Enregistrer dans le CircuitBreaker
                    if circuit_breaker:
                        entry_price = float(analysis.get("price_usd", 0) or 0)
                        circuit_breaker.open_position(
                            token_address=address,
                            token_symbol=sym,
                            entry_price=entry_price,
                        )
                    if capital_watchdog:
                        capital_watchdog.register_position(
                            token_address=address,
                            token_symbol=sym,
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
                    # Helius WebSocket: surveiller le nouveau token
                    if helius_ws:
                        helius_ws.watch_token(address)
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


async def today_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Commande /today - PnL du jour depuis postmortem.db (SQLite persistant)"""
    from datetime import datetime, timezone
    from postmortem import get_today_trades

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # ─── Source 1 : postmortem.db (trades fermés) ─────────────────
    try:
        db_trades = get_today_trades()  # [(symbol, strategy, pnl_pct, duration_min, exit_reason)]
    except Exception as e:
        logger.error(f"[/today] Erreur lecture postmortem.db: {e}")
        db_trades = []

    # ─── Source 2 : trades.json (achats du jour) ──────────────────
    buys = []
    try:
        if os.path.exists(TRADES_LOG_FILE):
            with open(TRADES_LOG_FILE, "r") as f:
                all_trades = json.load(f)
            buys = [t for t in all_trades
                    if t.get("timestamp", "").startswith(today_str)
                    and t.get("side") == "BUY"]
    except Exception:
        pass

    if not db_trades and not buys:
        await update.message.reply_text(
            f"📊 *PnL du {today_str}*\n\nAucun trade aujourd'hui.",
            parse_mode=ParseMode.MARKDOWN)
        return

    # ─── Calculs depuis postmortem.db ─────────────────────────────
    total_sells = len(db_trades)
    wins = [t for t in db_trades if t[2] and t[2] > 0]
    losses = [t for t in db_trades if t[2] and t[2] < 0]
    pnl_values = [t[2] for t in db_trades if t[2] is not None]

    avg_pnl = sum(pnl_values) / max(len(pnl_values), 1)
    best = max(pnl_values, default=0)
    worst = min(pnl_values, default=0)
    win_rate = (len(wins) / max(total_sells, 1)) * 100

    pnl_emoji = "🟢" if avg_pnl >= 0 else "🔴"
    msg = f"📅 *PnL du jour \u2014 {today_str}*\n\n"
    msg += f"{pnl_emoji} *PnL moyen: {avg_pnl:+.1f}%*\n\n"
    msg += f"📊 Trades: {len(buys)} achats / {total_sells} ventes\n"
    msg += f"🎯 Win Rate: {win_rate:.0f}% (✅{len(wins)} / ❌{len(losses)})\n"
    msg += f"🚀 Meilleur: {best:+.1f}%\n"
    msg += f"💥 Pire: {worst:+.1f}%\n"

    # Détail des ventes (max 15)
    if db_trades:
        msg += f"\n📝 *Détail des ventes:*\n"
        for t in db_trades[:15]:
            symbol, strategy, pnl_pct, duration, reason = t
            pnl_pct = pnl_pct or 0
            emoji = "✅" if pnl_pct > 0 else "❌"
            dur_str = f"{duration}m" if duration else "?"
            reason_short = (reason or "")[:20]
            msg += f"  {emoji} {symbol} {pnl_pct:+.1f}% ({reason_short}) {dur_str}\n"

    # Achats du jour (max 10)
    if buys:
        msg += f"\n🛍 *Achats du jour:*\n"
        for t in buys[-10:]:
            time_str = t.get("timestamp", "")[11:16]
            msg += f"  🔵 `{time_str}` {t.get('token', '?')} ({t.get('strategy', '?')}) {t.get('amount_sol', 0):.4f} SOL\n"

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
    """Heartbeat Telegram toutes les 30 min — DÉSACTIVÉ (log only)"""
    open_pos = len(positions.get_open_positions()) if positions else 0
    cb_checks = circuit_breaker._stats["total_checks"] if circuit_breaker else 0
    logger.info(f"[Heartbeat] Bot actif | Positions: {open_pos} | CB checks: {cb_checks}")
    # Notification Telegram désactivée pour réduire le bruit
    return


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/status — Vue unifiée de tout le système"""
    try:
        msg = "📊 *STATUS COMPLET*\n\n"

        # 1. Solde
        try:
            if wallet:
                sol_balance = wallet.get_sol_balance()
                msg += f"💰 *Solde:* {sol_balance:.4f} SOL\n\n"
        except Exception:
            msg += "💰 *Solde:* indisponible\n\n"

        # 2. Positions ouvertes
        try:
            open_pos = positions.get_open_positions() if positions else []
            msg += f"🟢 *Positions ouvertes:* {len(open_pos)}\n"
            for pos in open_pos:
                emoji = '🟢' if pos.pnl_pct >= 0 else '🔴'
                # Calculer l'âge depuis entry_time (str ISO)
                try:
                    from datetime import datetime
                    entry_dt = datetime.fromisoformat(pos.entry_time)
                    age_min = (datetime.now() - entry_dt).total_seconds() / 60
                except Exception:
                    age_min = 0
                msg += f"  {emoji} {pos.token_symbol}: {pos.pnl_pct:+.1f}% ({age_min:.0f}min)\n"
            msg += "\n"
        except Exception as e:
            msg += f"🟢 *Positions:* erreur ({e})\n\n"

        # 3. CircuitBreaker
        try:
            if circuit_breaker:
                stats = circuit_breaker.get_stats()
                msg += f"🔌 *CircuitBreaker:*\n"
                msg += f"  Checks: {stats['total_checks']} | "
                msg += f"SL: {stats['stop_loss_triggered']} | "
                msg += f"TP: {stats['take_profit_triggered']} | "
                msg += f"Trail: {stats['trailing_triggered']} | "
                msg += f"TS: {stats['time_stop_triggered']}\n\n"
        except Exception as e:
            msg += f"🔌 *CB:* erreur ({e})\n\n"

        # 4. Daily PnL Guard
        try:
            if daily_pnl_guard:
                guard = daily_pnl_guard.get_stats()
                status_emoji = '⛔' if guard['is_paused'] else '✅'
                msg += f"{status_emoji} *Daily Guard:*\n"
                msg += f"  PnL jour: {guard['daily_pnl_sol']:+.4f} SOL\n"
                msg += f"  Trades: {guard['trades_today']} | SL consécutifs: {guard['consecutive_sl']}/{guard['max_consecutive_sl']}\n"
                if guard['is_paused']:
                    msg += f"  {guard['pause_reason']}\n"
                msg += "\n"
        except Exception as e:
            msg += f"*Daily Guard:* erreur ({e})\n\n"

        # 5. Watchdog
        try:
            if capital_watchdog:
                wd_stats = capital_watchdog.get_stats()
                msg += f"🛡️ *Watchdog:*\n"
                msg += f"  Checks: {wd_stats.get('total_checks', 0)} | "
                msg += f"Max gap: {wd_stats.get('max_gap_seen', 0):.1f}s\n\n"
        except Exception as e:
            msg += f"🛡️ *Watchdog:* erreur ({e})\n\n"

        # 6. Config active
        try:
            msg += f"⚙️ *Config:*\n"
            msg += f"  TP: +{trading_config.take_profit_pct}% | SL: {trading_config.stop_loss_pct}%\n"
            if circuit_breaker:
                msg += f"  Trailing: activation +{circuit_breaker.config.trailing_activation_pct}% | SL -{circuit_breaker.config.trailing_stop_pct}% du max\n"
            msg += f"  Time Stop: {trading_config.time_stop_minutes}min (sniper) / 30min (recovered)\n"
            msg += f"  Partial TP: 50% au TP, 50% trailing\n"
        except Exception:
            pass

        await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        await update.message.reply_text(f"❌ Erreur /status: {e}")


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
# COMMANDES /verify ET /health_check + JOB HEALTH
# ============================================================

# Compteurs globaux pour /verify
_prix_check_skips = 0
_prix_check_ok = 0


def record_prix_skip(token_symbol: str):
    """Appeler quand verify_price_before_buy() retourne False."""
    global _prix_check_skips
    _prix_check_skips += 1


def record_prix_ok(token_symbol: str):
    """Appeler quand verify_price_before_buy() retourne True."""
    global _prix_check_ok
    _prix_check_ok += 1


async def verify_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/verify — État réel de tous les fixes en production."""
    lines = ["🔍 *Vérification des fixes*\n"]

    # ─── FIX 1 : Helius WebSocket
    if helius_ws and helius_ws.is_connected:
        watched = len(helius_ws._watched_tokens) if hasattr(helius_ws, '_watched_tokens') else 0
        lines.append(
            f"✅ *Helius WebSocket* : actif\n"
            f"   → tokens surveillés : `{watched}`"
        )
    elif helius_ws and not helius_ws.is_connected:
        lines.append(
            f"⚠️ *Helius WebSocket* : déconnecté\n"
            f"   → reconnexion automatique en cours\n"
            f"   → fallback DexScreener actif"
        )
    else:
        helius_key = os.environ.get("HELIUS_API_KEY", "")
        if not helius_key:
            lines.append(
                f"❌ *Helius WebSocket* : inactif\n"
                f"   → HELIUS\_API\_KEY manquante"
            )
        else:
            lines.append(
                f"❌ *Helius WebSocket* : non démarré\n"
                f"   → helius\_ws.start() pas appelé"
            )

    lines.append("")

    # ─── FIX 2 : Prix check avant achat
    total_prix_checks = _prix_check_skips + _prix_check_ok
    if total_prix_checks > 0:
        lines.append(
            f"✅ *Prix check avant achat* : actif\n"
            f"   → skips (prix=0) : `{_prix_check_skips}` tokens\n"
            f"   → achats OK (prix>0) : `{_prix_check_ok}` tokens"
        )
    else:
        lines.append(
            f"⚠️ *Prix check avant achat* : aucun trade depuis démarrage\n"
            f"   → fonction présente, pas encore utilisée"
        )

    lines.append("")

    # ─── FIX 3 : Max positions dynamique
    if wallet and wallet.keypair:
        sol_balance = wallet.get_sol_balance()
        pos_size = trading_config.sniper_position_sol or 0.05
        max_pos = get_dynamic_max_positions(
            sol_balance=sol_balance,
            position_size_sol=pos_size
        )
        current_pos = positions.count_positions()
        lines.append(
            f"✅ *Max positions dynamique* : actif\n"
            f"   → solde RPC : `{sol_balance:.4f} SOL`\n"
            f"   → taille position : `{pos_size} SOL`\n"
            f"   → formule : floor({sol_balance:.4f} / {pos_size}) = `{max_pos}`\n"
            f"   → limites : min=1, max=50\n"
            f"   → ouvertes : `{current_pos}/{max_pos}`"
        )
    else:
        lines.append(f"❌ *Max positions dynamique* : wallet non initialisé")

    lines.append("")

    # ─── Auto-calibration
    try:
        import sqlite3
        db_path = os.path.join(DATA_DIR, "postmortem.db")
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM postmortem")
        count = c.fetchone()[0]
        conn.close()
        if count >= 20:
            lines.append(
                f"✅ *Auto-calibration* : active\n"
                f"   → post-mortems : `{count}`\n"
                f"   → calibration tourne à 21h UTC"
            )
        else:
            lines.append(
                f"⏳ *Auto-calibration* : en attente\n"
                f"   → post-mortems : `{count}/20` minimum"
            )
    except Exception:
        lines.append(f"❌ *Auto-calibration* : DB postmortem absente")

    lines.append("")

    # ─── Blacklist post-SL
    active_blacklist = len([
        v for v in sl_blacklist.values()
        if v > time.time()
    ]) if sl_blacklist else 0
    lines.append(
        f"✅ *Blacklist post-SL* : active\n"
        f"   → tokens blacklistés : `{active_blacklist}`\n"
        f"   → durée : `1h` après chaque SL"
    )

    lines.append("")

    # ─── CircuitBreaker
    if circuit_breaker:
        stats = circuit_breaker.get_stats()
        lines.append(
            f"✅ *CircuitBreaker* : actif\n"
            f"   → checks : `{stats.get('total_checks', 0)}`\n"
            f"   → SL : `{stats.get('stop_loss_triggered', 0)}` | "
            f"TP : `{stats.get('take_profit_triggered', 0)}`\n"
            f"   → Time Stop : `{stats.get('time_stop_triggered', 0)}`"
        )
    else:
        lines.append(f"❌ *CircuitBreaker* : non initialisé")

    await update.message.reply_text(
        "\n".join(lines),
        parse_mode=ParseMode.MARKDOWN
    )


async def health_check_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/health_check — Santé complète du bot."""
    import psutil
    now = datetime.now(timezone.utc).strftime("%H:%M UTC")
    lines = [f"🏥 *Health Check — {now}*\n"]

    # RAM
    try:
        process = psutil.Process(os.getpid())
        ram_mb = process.memory_info().rss / 1024 / 1024
        ram_pct = psutil.virtual_memory().percent
        ram_emoji = "✅" if ram_mb < 800 else ("⚠️" if ram_mb < 1500 else "🚨")
        lines.append(
            f"{ram_emoji} *RAM* : `{ram_mb:.0f}MB` utilisés "
            f"(`{ram_pct:.0f}%` système)"
        )
    except Exception:
        lines.append(f"⚠️ *RAM* : impossible à mesurer")

    # Trading ON/OFF
    trading_emoji = "✅" if auto_trading_enabled else "🔴"
    lines.append(
        f"{trading_emoji} *Trading* : "
        f"{'ON' if auto_trading_enabled else 'OFF'}"
    )

    # Solde
    if wallet and wallet.keypair:
        sol = wallet.get_sol_balance()
        lines.append(f"💰 *Solde* : `{sol:.4f} SOL`")
    else:
        lines.append(f"❌ *Wallet* : non initialisé")

    # Positions
    open_pos = positions.count_positions()
    pos_list = positions.get_open_positions()
    if open_pos == 0:
        lines.append(f"📊 *Positions* : aucune ouverte")
    else:
        pos_str = ""
        for p in pos_list:
            try:
                entry_str = p.entry_time
                if "Z" in entry_str:
                    entry_str = entry_str.replace("Z", "+00:00")
                elif "+" not in entry_str and entry_str[-1] != "Z":
                    entry_str = entry_str + "+00:00"
                entry = datetime.fromisoformat(entry_str)
                if entry.tzinfo is None:
                    entry = entry.replace(tzinfo=timezone.utc)
                age_min = (datetime.now(timezone.utc) - entry).total_seconds() / 60
            except Exception:
                age_min = 0
            zombie_warning = " ⚠️ ZOMBIE" if age_min > 60 and p.pnl_pct == 0.0 else ""
            pos_str += f"\n   → {p.token_symbol} `{p.pnl_pct:+.1f}%` ({age_min:.0f}min){zombie_warning}"
        lines.append(f"📊 *Positions* : `{open_pos}` ouvertes{pos_str}")

    # Watchdog
    if capital_watchdog:
        max_gap = max(
            (time.time() - getattr(p, 'last_check_time', 0)
             for p in capital_watchdog.positions.values()
             if getattr(p, 'last_check_time', 0) > 0),
            default=0
        )
        gap_emoji = "✅" if max_gap < 10 else ("⚠️" if max_gap < 30 else "🚨")
        lines.append(f"{gap_emoji} *Watchdog* : max gap `{max_gap:.0f}s`")
    else:
        lines.append(f"❌ *Watchdog* : non initialisé")

    # Daily Guard
    if daily_pnl_guard:
        paused = getattr(daily_pnl_guard, 'is_paused', False)
        guard_emoji = "🔴" if paused else "✅"
        lines.append(f"{guard_emoji} *Daily Guard* : {'PAUSE' if paused else 'actif'}")
    else:
        lines.append(f"⚠️ *Daily Guard* : non initialisé")

    # WebSocket
    if helius_ws:
        ws_status = "connecté" if helius_ws.is_connected else "déconnecté"
        ws_emoji = "✅" if helius_ws.is_connected else "⚠️"
        lines.append(f"{ws_emoji} *WebSocket* : {ws_status}")
    else:
        lines.append(f"❌ *WebSocket* : non démarré")

    # CB
    if circuit_breaker:
        stats = circuit_breaker.get_stats()
        lines.append(
            f"✅ *CB* : `{stats.get('total_checks', 0)}` checks "
            f"| TS: `{stats.get('time_stop_triggered', 0)}`"
        )

    # Gate 1 DexScreener
    if api:
        g1 = api.get_gate1_stats()
        g1_total = g1['total']
        g1_ok = g1['successes']
        g1_rate = g1['success_rate']
        g1_emoji = "✅" if g1_rate >= 80 else ("⚠️" if g1_rate >= 50 else "🚨")
        lines.append(
            f"{g1_emoji} *Gate1* : `{g1_ok}` succès / `{g1_total}` appels "
            f"(`{g1_rate:.0f}%` dernière heure)"
        )
        if api._rate_limit_paused_until > time.time():
            remaining = api._rate_limit_paused_until - time.time()
            lines.append(f"🚨 *Rate Limit* : pause active (`{remaining:.0f}s` restantes)")

    lines.append(f"\n💡 `/verify` pour l'état détaillé des fixes")

    await update.message.reply_text(
        "\n".join(lines),
        parse_mode=ParseMode.MARKDOWN
    )


async def hourly_health_check_job(context):
    """
    Job automatique toutes les heures.
    Silencieux si tout va bien, alerte Telegram si problème.
    """
    issues = []

    # RAM trop haute ?
    try:
        import psutil
        process = psutil.Process(os.getpid())
        ram_mb = process.memory_info().rss / 1024 / 1024
        if ram_mb > 1500:
            issues.append(f"🚨 RAM critique : {ram_mb:.0f}MB")
    except Exception:
        pass

    # Zombie détectée ?
    for pos in positions.get_open_positions():
        try:
            entry_str = pos.entry_time
            if "Z" in entry_str:
                entry_str = entry_str.replace("Z", "+00:00")
            elif "+" not in entry_str:
                entry_str = entry_str + "+00:00"
            entry = datetime.fromisoformat(entry_str)
            if entry.tzinfo is None:
                entry = entry.replace(tzinfo=timezone.utc)
            age_min = (datetime.now(timezone.utc) - entry).total_seconds() / 60
            if age_min > 60 and pos.pnl_pct == 0.0:
                issues.append(
                    f"🧟 Zombie : {pos.token_symbol} "
                    f"({age_min:.0f}min, PnL={pos.pnl_pct:.1f}%)"
                )
        except Exception:
            pass

    # WebSocket déconnecté ?
    if helius_ws and not helius_ws.is_connected:
        issues.append("⚠️ WebSocket Helius déconnecté")

    # Trading OFF involontairement ?
    if not auto_trading_enabled:
        issues.append("🔴 Trading AUTO est OFF")

    # Pipeline silencieux ? (30min sans token dans la queue)
    global _pipeline_silent_alert_sent
    uptime_s = time.time() - _bot_start_time
    if uptime_s > 1800 and _ws_queue_first_push == 0.0 and not _pipeline_silent_alert_sent:
        issues.append("⚠️ PIPELINE SILENCIEUX\n   30min sans token dans la queue\n   Vérifie _ws_on_new_token")
        _pipeline_silent_alert_sent = True

    # Si tout va bien → silence (pas de spam)
    if not issues:
        return

    # Si problèmes → alerte immédiate
    now = datetime.now(timezone.utc).strftime("%H:%M UTC")
    msg = f"⚠️ *Health Check — {now}*\n\n"
    msg += "\n".join(issues)
    msg += "\n\n💡 Tape `/health_check` pour le détail"

    for chat_id in subscribers:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=msg,
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception:
            pass


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
        except Exception as e:
            print(f"[ERROR] Impossible de charger wallet depuis env: {e}")
    elif os.path.exists(WalletManager.WALLET_FILE):
        wallet.load_or_create_wallet()
        init_trading()
        print(f"💰 Wallet chargé depuis fichier: {wallet.public_key}")
    else:
        print("⚠️  Aucun wallet configuré. Utilisez /wallet ou /import_wallet")

    # === RESTAURER L'ÉTAT PERSISTANT (state.json) ===
    global auto_trading_enabled, sl_blacklist
    saved_state = _load_state()

    # 1. Auto trading
    auto_trading_enabled = saved_state.get("auto_trading", wallet.keypair is not None)

    # 2. Paramètres de trading (restaurer dans trading_config + circuit_breaker)
    trading_config.stop_loss_pct = saved_state.get("sl_pct", -25.0)
    trading_config.take_profit_pct = saved_state.get("tp_pct", 20.0)
    trading_config.trailing_activation_pct = saved_state.get("trailing_activation", 20.0)
    trading_config.trailing_stop_pct = abs(saved_state.get("trailing_sl", -15.0))
    trading_config.time_stop_minutes = saved_state.get("time_stop_sniper", 20)
    if circuit_breaker:
        circuit_breaker.update_config(
            stop_loss_pct=trading_config.stop_loss_pct,
            take_profit_pct=trading_config.take_profit_pct,
            trailing_activation_pct=trading_config.trailing_activation_pct,
            trailing_stop_pct=trading_config.trailing_stop_pct,
            time_stop_minutes=trading_config.time_stop_minutes,
        )

    # 3. Position sizing et streak
    sizing = saved_state.get("position_sizing", {})
    streak = saved_state.get("streak", {})
    if position_sizer:
        position_sizer.base_size = sizing.get("base", 0.05)
        position_sizer.min_size = sizing.get("min", 0.02)
        position_sizer.max_size = sizing.get("max", 0.15)
        position_sizer.consecutive_wins = streak.get("consecutive_wins", 0)
        position_sizer.consecutive_losses = streak.get("consecutive_losses", 0)

    # 4. Blacklist (format enrichi → convertir en {addr: expiry} pour compatibilité)
    blacklist_raw = saved_state.get("blacklist", {})
    sl_blacklist = {}
    for addr, entry in blacklist_raw.items():
        if isinstance(entry, dict):
            sl_blacklist[addr] = entry.get("banned_until", 0)
        elif isinstance(entry, (int, float)):
            sl_blacklist[addr] = entry  # Ancien format

    print(f"💾 État restauré depuis state.json:")
    print(f"   Trading: {'ON' if auto_trading_enabled else 'OFF'}")
    print(f"   SL: {trading_config.stop_loss_pct}% | TP: +{trading_config.take_profit_pct}%")
    print(f"   Trailing: activation +{trading_config.trailing_activation_pct}%, SL -{trading_config.trailing_stop_pct}%")
    print(f"   Time Stop: {trading_config.time_stop_minutes:.0f}min sniper / {saved_state.get('time_stop_recovered', 30)}min recovered")
    print(f"   Streak: W{streak.get('consecutive_wins', 0)} / L{streak.get('consecutive_losses', 0)}")
    print(f"   Blacklist: {len(sl_blacklist)} tokens")
    print(f"   Dernière MAJ: {saved_state.get('last_updated', 'jamais')}")

    # Si premier démarrage (state.json vide ou absent), sauvegarder les défauts
    if not os.path.exists(STATE_FILE):
        _save_state()
        print("🚀 Premier démarrage: state.json créé avec valeurs par défaut")

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
                        token_symbol = _safe_symbol(analysis.get("symbol"), mint)
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
                    # Skip les tokens sans prix (non indexés = probablement morts)
                    if price_usd <= 0:
                        print(f"  ⚠️ Skip {token_symbol} ({mint[:12]}): prix=0 (token non indexé/mort)")
                        continue
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
    app.add_handler(CommandHandler("set_slippage", set_slippage_cmd))
    app.add_handler(CommandHandler("set_trailing", set_trailing))
    app.add_handler(CommandHandler("set_timestop", set_timestop))
    app.add_handler(CommandHandler("set_size", set_size))
    app.add_handler(CommandHandler("buy", buy_cmd))
    app.add_handler(CommandHandler("sell", sell_cmd))
    app.add_handler(CommandHandler("sell_all", sell_all_cmd))
    app.add_handler(CommandHandler("close_position", close_position_cmd))
    app.add_handler(CommandHandler("kill_zombies", kill_zombies_cmd))
    app.add_handler(CommandHandler("scan", scan_command))
    app.add_handler(CommandHandler("trending", trending_command))
    app.add_handler(CommandHandler("metas", metas_command))
    app.add_handler(CommandHandler("copy_wallets", copy_wallets_cmd))
    app.add_handler(CommandHandler("copy_add", copy_add_cmd))
    app.add_handler(CommandHandler("copy_remove", copy_remove_cmd))
    app.add_handler(CommandHandler("copy_history", copy_history_cmd))
    app.add_handler(CommandHandler("pnl", pnl_cmd))
    app.add_handler(CommandHandler("today", today_cmd))
    app.add_handler(CommandHandler("diversity", diversity_cmd))
    app.add_handler(CommandHandler("set_corr", set_corr))
    app.add_handler(CommandHandler("insights", insights_cmd))
    app.add_handler(CommandHandler("cb_stats", cb_stats_cmd))
    app.add_handler(CommandHandler("watchdog", watchdog_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("unpause", unpause_cmd))
    app.add_handler(CommandHandler("resume", unpause_cmd))
    app.add_handler(CommandHandler("verify", verify_cmd))
    app.add_handler(CommandHandler("health_check", health_check_cmd))

    # Job SNIPER FALLBACK (3s) - polling DexScreener seulement si WS down
    job_queue = app.job_queue
    job_queue.run_repeating(sniper_monitor_job, interval=3, first=5)

    # Job Capital Watchdog (5s) - surveille que le capital est sous contrôle
    job_queue.run_repeating(watchdog_check_job, interval=5, first=8)

    # Job backup pour LP monitoring + post-trade analysis (15s)
    job_queue.run_repeating(position_monitor_job, interval=15, first=10)

    # Job WebSocket: traite les tokens détectés par Helius WS (5s)
    job_queue.run_repeating(ws_token_processor_job, interval=5, first=15)

    # Job de scan pour nouvelles opportunités (toutes les 45s) - FALLBACK DexScreener
    job_queue.run_repeating(auto_trading_job, interval=POLLING_INTERVAL, first=20)

    # Heartbeat Telegram (30min) - confirme que le bot est actif
    job_queue.run_repeating(heartbeat_job, interval=1800, first=60)

    # Job Post-Mortem Analysis (6h) - rapport automatique d'insights
    job_queue.run_repeating(postmortem_analysis_job, interval=21600, first=300)

    # Alertes critiques (60s) - SL imminent, zombie, RAM, bot mort
    job_queue.run_repeating(critical_monitor_job, interval=60, first=30)

    # Résumé quotidien à 20h UTC
    job_queue.run_daily(
        daily_summary_job,
        time=dt_time(hour=20, minute=0, tzinfo=timezone.utc),
        name="daily_summary"
    )

    # Auto-calibration quotidienne à 21h UTC (1h après le résumé)
    job_queue.run_daily(
        auto_calibration_job,
        time=dt_time(hour=21, minute=0, tzinfo=timezone.utc),
        name="auto_calibration"
    )

    # Health check automatique toutes les heures (silencieux si tout OK)
    job_queue.run_repeating(
        hourly_health_check_job,
        interval=3600,
        first=300,
        name="hourly_health"
    )

    # ─── FIX 1: Helper Helius WebSocket ─────────────────────────────────
    async def start_helius_websocket(application):
        """
        Lance le Helius WebSocket en arrière-plan.
        Doit être appelé APRÈS init_trading().
        Compatible avec python-telegram-bot v20+
        """
        if not helius_ws:
            logger.warning("[WSS] helius_ws non initialisé — skip")
            return

        logger.info("[WSS] Démarrage Helius WebSocket...")
        asyncio.create_task(helius_ws.start())
        logger.info("[WSS] ✅ Task WebSocket lancée")
    # ─────────────────────────────────────────────────────────────────────

    # Démarrer le WebSocket PriceMonitor pour les positions existantes
    # (s'exécute en arrière-plan dans l'event loop)
    async def post_init(application):
        """Callback post-init pour démarrer le WebSocket et le copy trading"""
        # 💀 PURGE ZOMBIE AU DÉMARRAGE: fermer les positions mortes qui survivent aux reboots
        try:
            now = datetime.now(timezone.utc)
            time_stop_min = trading_config.time_stop_minutes if hasattr(trading_config, 'time_stop_minutes') else 20
            zombies_purged = 0
            for addr in list(positions.positions.keys()):
                pos = positions.positions.get(addr)
                if not pos:
                    continue
                try:
                    entry_time = datetime.fromisoformat(pos.entry_time)
                    if entry_time.tzinfo is None:
                        entry_time = entry_time.replace(tzinfo=timezone.utc)
                    age_minutes = (now - entry_time).total_seconds() / 60
                except:
                    age_minutes = 9999

                # Zombie = age > time_stop + 5 min ET PnL figé à 0%
                if age_minutes > (time_stop_min + 5) and abs(pos.pnl_pct) < 0.1:
                    print(f"💀 ZOMBIE PURGE: {pos.token_symbol} ({age_minutes:.0f}min, PnL={pos.pnl_pct:.1f}%)")
                    # Tenter la vente
                    if trading_engine:
                        try:
                            trading_engine.execute_sell(addr, percentage=100)
                        except:
                            pass
                    if circuit_breaker:
                        circuit_breaker.close_position(addr)
                    if capital_watchdog:
                        capital_watchdog.unregister_position(addr)
                    positions.close_position(addr)
                    sl_blacklist[addr] = time.time() + 86400
                    _save_blacklist()
                    zombies_purged += 1

            if zombies_purged > 0:
                _save_state()
                print(f"💀 {zombies_purged} position(s) zombie purgée(s) au démarrage")
        except Exception as e:
            print(f"⚠️ Erreur purge zombies: {e}")

        try:
            await start_price_monitor_for_positions()
            print("🔌 WebSocket PriceMonitor démarré !")
        except Exception as e:
            print(f"⚠️ Erreur démarrage WebSocket (fallback polling actif): {e}")

        # Lancer Helius WebSocket via helper dédié
        await start_helius_websocket(application)

        # Démarrer le copy trading
        if copy_trader and auto_trading_enabled:
            try:
                await copy_trader.start_monitoring()
                print(f"📋 Copy Trading démarré: {len(copy_trader.get_active_wallets())} wallets suivis")
            except Exception as e:
                print(f"⚠️ Erreur démarrage copy trading (polling fallback actif): {e}")

        # RÈGLE 4: Alerte Telegram au redémarrage — DÉSACTIVÉE (log only)
        if subscribers:
            try:
                balance = wallet.get_sol_balance() if wallet.keypair else 0
                n_positions = positions.count_positions()
                logger.info(f"[Restart] Bot redémarré | Capital: {balance:.4f} SOL | Positions: {n_positions}")
                # Notification Telegram désactivée pour réduire le bruit
            except Exception as e:
                logger.error(f"Erreur alerte redémarrage: {e}")

    app.post_init = post_init

    print("✅ Bot de trading démarré !")
    print("🔌 Helius WebSocket: source PRIMAIRE de prix (temps réel)")
    print("🔄 Polling DexScreener 3s: FALLBACK si WS down")
    print("⚡ Pipeline: prix WS → cb.check() → SL -25% instantané")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
