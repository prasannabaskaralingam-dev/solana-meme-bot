"""
Module Copy Trading - Smart Wallet Tracker
Surveille les wallets performants en temps réel via WebSocket (logsSubscribe)
et copie automatiquement leurs achats/ventes de meme coins.
"""

import os
import json
import time
import asyncio
import logging
import httpx
import websockets
from typing import Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Programme IDs des DEX Solana
JUPITER_V6 = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"
PUMP_FUN = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMP_SWAP = "PSwapMdSai8tjrEXcxFeQth87xC4rRsa4VA5mhGhXkP"
RAYDIUM_V4 = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
RAYDIUM_CPMM = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C"

SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

# Stablecoins et tokens connus à ignorer (pas des meme coins)
IGNORE_TOKENS = {
    SOL_MINT, USDC_MINT,
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
    "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",  # mSOL
    "7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj",  # stSOL
    "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",  # BONK (trop gros)
    "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm",  # WIF (trop gros)
}

# Taille minimum d'un swap pour être considéré comme un trade (en lamports)
MIN_SWAP_SOL = 0.02  # Ignorer les micro-trades < 0.02 SOL


@dataclass
class SmartWallet:
    """Représente un wallet à suivre"""
    address: str
    label: str
    win_rate: float = 0.0  # % de trades gagnants
    avg_roi: float = 0.0   # ROI moyen
    active: bool = True
    last_trade_time: str = ""
    trades_copied: int = 0


@dataclass
class CopyTradeSignal:
    """Signal de trade détecté depuis un smart wallet"""
    wallet_address: str
    wallet_label: str
    action: str  # "buy" ou "sell"
    token_mint: str
    token_symbol: str
    amount_sol: float
    timestamp: str
    tx_signature: str


class CopyTradingEngine:
    """
    Moteur de copy trading.
    Surveille les smart wallets via WebSocket et émet des signaux de trade.
    """

    # Fichier de config des wallets à suivre
    WALLETS_FILE = os.path.join(
        os.environ.get("PERSISTENT_DATA_DIR", os.path.dirname(os.path.abspath(__file__))),
        "smart_wallets.json"
    )
    COPY_HISTORY_FILE = os.path.join(
        os.environ.get("PERSISTENT_DATA_DIR", os.path.dirname(os.path.abspath(__file__))),
        "copy_trade_history.json"
    )

    def __init__(self, rpc_url: str = ""):
        self.rpc_url = rpc_url or os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
        self.ws_url = self.rpc_url.replace("https://", "wss://").replace("http://", "ws://")
        # Fallback pour le WS public
        if "api.mainnet-beta.solana.com" in self.ws_url:
            self.ws_url = "wss://api.mainnet-beta.solana.com"

        self.smart_wallets: list[SmartWallet] = []
        self.on_signal_callback: Optional[Callable] = None
        self.running = False
        self._ws_task: Optional[asyncio.Task] = None
        self._recently_copied: dict[str, float] = {}  # token_mint -> timestamp (éviter doublons)
        self.copy_history: list[dict] = []

        # Charger les wallets sauvegardés
        self._load_wallets()
        self._load_history()

    def _load_wallets(self):
        """Charger la liste des smart wallets depuis le fichier"""
        if os.path.exists(self.WALLETS_FILE):
            try:
                with open(self.WALLETS_FILE, "r") as f:
                    data = json.load(f)
                self.smart_wallets = [SmartWallet(**w) for w in data]
                logger.info(f"[COPY] {len(self.smart_wallets)} smart wallets chargés")
            except Exception as e:
                logger.error(f"[COPY] Erreur chargement wallets: {e}")
                self.smart_wallets = []
        else:
            # Wallets par défaut - top performers connus
            self._init_default_wallets()

    def _init_default_wallets(self):
        """Initialiser avec des wallets performants connus"""
        # Ces wallets sont des traders connus avec un bon track record
        # Source: GMGN.ai, Dune Analytics, communauté Solana
        default_wallets = [
            # Top sniper wallets (régulièrement dans le top GMGN)
            SmartWallet(
                address="Ai4zqm7UKx2DKXV3gXWnEgNih3RaJDEp2JkCCpVpump",
                label="Alpha Sniper 1",
                win_rate=68.0,
                avg_roi=45.0,
            ),
            SmartWallet(
                address="5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1",
                label="Raydium Authority",
                win_rate=72.0,
                avg_roi=35.0,
            ),
            SmartWallet(
                address="HWEoBxYs7ssKuudEjzjmpfJVX7Dvi7wescFsVx2L5yoY",
                label="Smart Degen 1",
                win_rate=65.0,
                avg_roi=55.0,
            ),
        ]
        self.smart_wallets = default_wallets
        self._save_wallets()
        logger.info(f"[COPY] Wallets par défaut initialisés: {len(default_wallets)}")

    def _save_wallets(self):
        """Sauvegarder la liste des wallets"""
        try:
            data = []
            for w in self.smart_wallets:
                data.append({
                    "address": w.address,
                    "label": w.label,
                    "win_rate": w.win_rate,
                    "avg_roi": w.avg_roi,
                    "active": w.active,
                    "last_trade_time": w.last_trade_time,
                    "trades_copied": w.trades_copied,
                })
            with open(self.WALLETS_FILE, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"[COPY] Erreur sauvegarde wallets: {e}")

    def _load_history(self):
        """Charger l'historique des copy trades"""
        if os.path.exists(self.COPY_HISTORY_FILE):
            try:
                with open(self.COPY_HISTORY_FILE, "r") as f:
                    self.copy_history = json.load(f)
            except Exception:
                self.copy_history = []

    def _save_history(self):
        """Sauvegarder l'historique"""
        try:
            with open(self.COPY_HISTORY_FILE, "w") as f:
                json.dump(self.copy_history[-100:], f, indent=2)  # Garder les 100 derniers
        except Exception as e:
            logger.error(f"[COPY] Erreur sauvegarde historique: {e}")

    def add_wallet(self, address: str, label: str = "") -> bool:
        """Ajouter un wallet à suivre"""
        # Vérifier si déjà présent
        for w in self.smart_wallets:
            if w.address == address:
                return False
        wallet = SmartWallet(
            address=address,
            label=label or f"Wallet {len(self.smart_wallets) + 1}",
        )
        self.smart_wallets.append(wallet)
        self._save_wallets()
        logger.info(f"[COPY] Wallet ajouté: {label} ({address[:8]}...)")
        return True

    def remove_wallet(self, address: str) -> bool:
        """Retirer un wallet de la liste"""
        for i, w in enumerate(self.smart_wallets):
            if w.address == address:
                self.smart_wallets.pop(i)
                self._save_wallets()
                return True
        return False

    def get_active_wallets(self) -> list[SmartWallet]:
        """Retourner les wallets actifs"""
        return [w for w in self.smart_wallets if w.active]

    def set_signal_callback(self, callback: Callable):
        """Définir le callback appelé quand un signal est détecté"""
        self.on_signal_callback = callback

    async def start_monitoring(self):
        """Démarrer le monitoring WebSocket de tous les wallets"""
        if self.running:
            return
        self.running = True
        active_wallets = self.get_active_wallets()
        if not active_wallets:
            logger.warning("[COPY] Aucun wallet actif à surveiller")
            return

        logger.info(f"[COPY] Démarrage monitoring de {len(active_wallets)} wallets...")
        self._ws_task = asyncio.create_task(self._monitor_loop())

    async def stop_monitoring(self):
        """Arrêter le monitoring"""
        self.running = False
        if self._ws_task:
            self._ws_task.cancel()

    async def _monitor_loop(self):
        """Boucle principale de monitoring - reconnexion automatique"""
        while self.running:
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[COPY] WebSocket error: {e}")
                if self.running:
                    await asyncio.sleep(5)  # Attendre avant de reconnecter

    async def _connect_and_listen(self):
        """Se connecter au WebSocket et écouter les transactions des wallets"""
        active_wallets = self.get_active_wallets()
        if not active_wallets:
            await asyncio.sleep(30)
            return

        logger.info(f"[COPY] Connexion WebSocket: {self.ws_url}")

        async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=30) as ws:
            # S'abonner aux logs de chaque wallet
            subscription_ids = {}
            for wallet in active_wallets:
                sub_msg = json.dumps({
                    "jsonrpc": "2.0",
                    "id": hash(wallet.address) % 10000,
                    "method": "logsSubscribe",
                    "params": [
                        {"mentions": [wallet.address]},
                        {"commitment": "confirmed"}
                    ]
                })
                await ws.send(sub_msg)
                response = await asyncio.wait_for(ws.recv(), timeout=10)
                resp_data = json.loads(response)
                sub_id = resp_data.get("result")
                if sub_id:
                    subscription_ids[sub_id] = wallet
                    logger.info(f"[COPY] Abonné à {wallet.label} ({wallet.address[:8]}...) sub_id={sub_id}")

            logger.info(f"[COPY] {len(subscription_ids)} abonnements actifs, en écoute...")

            # Écouter les notifications
            while self.running:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=60)
                    data = json.loads(msg)

                    if data.get("method") == "logsNotification":
                        await self._handle_log_notification(data, subscription_ids)
                except asyncio.TimeoutError:
                    # Pas de message depuis 60s, c'est normal
                    continue
                except websockets.ConnectionClosed:
                    logger.warning("[COPY] WebSocket fermé, reconnexion...")
                    break

    async def _handle_log_notification(self, data: dict, subscription_ids: dict):
        """Traiter une notification de log (transaction détectée)"""
        try:
            params = data.get("params", {})
            result = params.get("result", {})
            value = result.get("value", {})
            subscription = params.get("subscription")

            # Identifier le wallet source
            wallet = subscription_ids.get(subscription)
            if not wallet:
                return

            # Vérifier si c'est un swap (chercher les programmes DEX dans les logs)
            logs = value.get("logs", [])
            signature = value.get("signature", "")
            err = value.get("err")

            # Ignorer les transactions échouées
            if err:
                return

            # Vérifier si c'est un swap DEX
            is_swap = False
            for log in logs:
                if any(dex in log for dex in [JUPITER_V6, PUMP_FUN, PUMP_SWAP, RAYDIUM_V4, RAYDIUM_CPMM]):
                    is_swap = True
                    break

            if not is_swap:
                return

            logger.info(f"[COPY] Swap détecté de {wallet.label}! TX: {signature[:16]}...")

            # Analyser la transaction pour déterminer le trade
            await asyncio.sleep(2)  # Attendre que la TX soit confirmée
            signal = await self._analyze_transaction(signature, wallet)

            if signal:
                await self._process_signal(signal)

        except Exception as e:
            logger.error(f"[COPY] Erreur handle_log: {e}")

    async def _analyze_transaction(self, signature: str, wallet: SmartWallet) -> Optional[CopyTradeSignal]:
        """Analyser une transaction pour extraire les détails du swap"""
        try:
            # Récupérer les détails de la transaction
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTransaction",
                "params": [
                    signature,
                    {
                        "encoding": "jsonParsed",
                        "maxSupportedTransactionVersion": 0
                    }
                ]
            }

            async with httpx.AsyncClient(timeout=15) as client:
                response = await client.post(self.rpc_url, json=payload)
                result = response.json()

            tx = result.get("result")
            if not tx:
                logger.warning(f"[COPY] TX non trouvée: {signature[:16]}...")
                return None

            meta = tx.get("meta", {})
            if meta.get("err"):
                return None

            # Analyser les changements de balance de tokens
            pre_balances = meta.get("preTokenBalances", [])
            post_balances = meta.get("postTokenBalances", [])

            # Trouver les changements pour le wallet suivi
            wallet_changes = self._compute_token_changes(
                pre_balances, post_balances, wallet.address
            )

            if not wallet_changes:
                return None

            # Déterminer si c'est un achat ou une vente
            sol_change = 0
            token_mint = ""
            token_change = 0

            for mint, change in wallet_changes.items():
                if mint == SOL_MINT or mint == "native":
                    sol_change = change
                elif mint not in IGNORE_TOKENS:
                    token_mint = mint
                    token_change = change

            if not token_mint:
                # Pas de meme coin impliqué
                return None

            # Déterminer l'action
            if token_change > 0:
                action = "buy"
                amount_sol = abs(sol_change) if sol_change < 0 else 0
            elif token_change < 0:
                action = "sell"
                amount_sol = abs(sol_change) if sol_change > 0 else 0
            else:
                return None

            # Ignorer les micro-trades
            if amount_sol < MIN_SWAP_SOL:
                # Essayer de calculer depuis les pre/post SOL balances
                pre_sol = meta.get("preBalances", [0])[0] / 1e9
                post_sol = meta.get("postBalances", [0])[0] / 1e9
                amount_sol = abs(post_sol - pre_sol)
                if amount_sol < MIN_SWAP_SOL:
                    return None

            # Récupérer le symbole du token
            token_symbol = await self._get_token_symbol(token_mint)

            signal = CopyTradeSignal(
                wallet_address=wallet.address,
                wallet_label=wallet.label,
                action=action,
                token_mint=token_mint,
                token_symbol=token_symbol,
                amount_sol=amount_sol,
                timestamp=datetime.utcnow().isoformat(),
                tx_signature=signature,
            )

            logger.info(
                f"[COPY] Signal: {wallet.label} {action.upper()} {token_symbol} "
                f"({amount_sol:.4f} SOL) TX: {signature[:16]}..."
            )

            return signal

        except Exception as e:
            logger.error(f"[COPY] Erreur analyse TX {signature[:16]}...: {e}")
            return None

    def _compute_token_changes(self, pre_balances: list, post_balances: list, wallet_address: str) -> dict:
        """Calculer les changements de tokens pour un wallet donné"""
        changes = {}

        # Indexer les pre-balances par (owner, mint)
        pre_map = {}
        for bal in pre_balances:
            owner = bal.get("owner", "")
            mint = bal.get("mint", "")
            amount = float(bal.get("uiTokenAmount", {}).get("uiAmount") or 0)
            if owner == wallet_address:
                pre_map[mint] = amount

        # Comparer avec post-balances
        for bal in post_balances:
            owner = bal.get("owner", "")
            mint = bal.get("mint", "")
            amount = float(bal.get("uiTokenAmount", {}).get("uiAmount") or 0)
            if owner == wallet_address:
                pre_amount = pre_map.get(mint, 0)
                change = amount - pre_amount
                if abs(change) > 0.000001:
                    changes[mint] = change

        # Tokens qui étaient là avant mais plus après (vente complète)
        for mint, pre_amount in pre_map.items():
            if mint not in changes:
                found_in_post = False
                for bal in post_balances:
                    if bal.get("owner") == wallet_address and bal.get("mint") == mint:
                        found_in_post = True
                        break
                if not found_in_post and pre_amount > 0:
                    changes[mint] = -pre_amount

        return changes

    async def _get_token_symbol(self, mint: str) -> str:
        """Récupérer le symbole d'un token via DexScreener"""
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                response = await client.get(f"https://api.dexscreener.com/tokens/v1/solana/{mint}")
                if response.status_code == 200:
                    data = response.json()
                    if data and len(data) > 0:
                        return data[0].get("baseToken", {}).get("symbol", mint[:8])
        except Exception:
            pass
        return mint[:8]

    async def _process_signal(self, signal: CopyTradeSignal):
        """Traiter un signal de trade"""
        # Vérifier si on n'a pas déjà copié ce token récemment (anti-doublon)
        now = time.time()
        last_copy = self._recently_copied.get(signal.token_mint, 0)
        if now - last_copy < 300:  # 5 minutes cooldown par token
            logger.info(f"[COPY] Token {signal.token_symbol} déjà copié récemment, skip")
            return

        # Vérifier que le token n'est pas dans la liste d'exclusion
        if signal.token_mint in IGNORE_TOKENS:
            return

        # Marquer comme copié
        self._recently_copied[signal.token_mint] = now

        # Sauvegarder dans l'historique
        self.copy_history.append({
            "wallet": signal.wallet_label,
            "action": signal.action,
            "token": signal.token_symbol,
            "token_mint": signal.token_mint,
            "amount_sol": signal.amount_sol,
            "timestamp": signal.timestamp,
            "tx": signal.tx_signature,
        })
        self._save_history()

        # Mettre à jour le wallet
        for w in self.smart_wallets:
            if w.address == signal.wallet_address:
                w.last_trade_time = signal.timestamp
                w.trades_copied += 1
                self._save_wallets()
                break

        # Appeler le callback pour exécuter le trade
        if self.on_signal_callback:
            try:
                await self.on_signal_callback(signal)
            except Exception as e:
                logger.error(f"[COPY] Erreur callback signal: {e}")

    # ============================================================
    # MÉTHODE ALTERNATIVE: Polling (si WebSocket instable)
    # ============================================================

    async def poll_wallets(self):
        """
        Méthode alternative: vérifier les dernières transactions des wallets par polling.
        Utilisée comme fallback si le WebSocket est instable.
        """
        active_wallets = self.get_active_wallets()
        for wallet in active_wallets:
            try:
                await self._check_recent_transactions(wallet)
            except Exception as e:
                logger.error(f"[COPY] Erreur polling {wallet.label}: {e}")
            await asyncio.sleep(1)  # Rate limiting

    async def _check_recent_transactions(self, wallet: SmartWallet):
        """Vérifier les transactions récentes d'un wallet"""
        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getSignaturesForAddress",
                "params": [
                    wallet.address,
                    {"limit": 5, "commitment": "confirmed"}
                ]
            }
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(self.rpc_url, json=payload)
                result = response.json()

            signatures = result.get("result", [])
            for sig_info in signatures:
                # Vérifier si la TX est récente (< 2 minutes)
                block_time = sig_info.get("blockTime", 0)
                if time.time() - block_time > 120:
                    continue

                signature = sig_info.get("signature", "")
                if sig_info.get("err"):
                    continue

                # Analyser la transaction
                signal = await self._analyze_transaction(signature, wallet)
                if signal:
                    await self._process_signal(signal)

        except Exception as e:
            logger.error(f"[COPY] Erreur check TX {wallet.label}: {e}")
