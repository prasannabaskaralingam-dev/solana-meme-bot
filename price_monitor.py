"""
Price Monitor WebSocket - Monitoring en temps réel des prix via Helius WebSocket.
Surveille les pools PumpSwap/Raydium pour détecter TP/SL instantanément.
"""

import asyncio
import base64
import json
import struct
import logging
import time
import os
from typing import Optional, Callable, Dict
from dataclasses import dataclass

import base58
import requests
import websockets

logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION
# ============================================================

# Helius WebSocket (gratuit, 2 connexions max)
HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY", "")
RPC_URL = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
WS_URL = os.environ.get("SOLANA_WS_URL", "")

# Si pas de WS_URL défini, dériver du RPC_URL ou utiliser Helius
if not WS_URL:
    if HELIUS_API_KEY:
        WS_URL = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    elif "helius" in RPC_URL:
        # Extraire l'API key du RPC URL
        WS_URL = RPC_URL.replace("https://", "wss://")
    else:
        WS_URL = "wss://api.mainnet-beta.solana.com"

SOL_MINT = "So11111111111111111111111111111111111111112"

# DexScreener API pour trouver les pools
DEXSCREENER_API = "https://api.dexscreener.com/latest/dex/tokens"


# ============================================================
# POOL STRUCTURES
# ============================================================

@dataclass
class PoolInfo:
    """Info d'un pool surveillé"""
    pool_address: str
    base_vault: str
    quote_vault: str
    base_mint: str
    quote_mint: str
    base_decimals: int
    quote_decimals: int
    token_address: str  # Le mint du token (pas SOL)
    latest_price_sol: float = 0.0
    last_update: float = 0.0


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def fetch_pair_address(token_mint: str) -> Optional[dict]:
    """Trouver l'adresse du pool pour un token via DexScreener"""
    try:
        url = f"{DEXSCREENER_API}/{token_mint}"
        resp = requests.get(url, timeout=8)
        resp.raise_for_status()
        data = resp.json()
        pairs = data.get("pairs", [])

        # Chercher un pool PumpSwap ou Raydium sur Solana
        for pair in pairs:
            chain = pair.get("chainId", "").lower()
            if chain != "solana":
                continue
            dex_id = pair.get("dexId", "").lower()
            if "pump" in dex_id or "raydium" in dex_id:
                return {
                    "pair_address": pair.get("pairAddress"),
                    "dex_id": dex_id,
                    "price_usd": float(pair.get("priceUsd", 0) or 0),
                    "price_native": float(pair.get("priceNative", 0) or 0),
                }
        return None
    except Exception as e:
        logger.error(f"Erreur fetch_pair_address pour {token_mint}: {e}")
        return None


def get_account_info(address: str) -> Optional[dict]:
    """Récupérer les données d'un compte Solana"""
    try:
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getAccountInfo",
            "params": [address, {"encoding": "base64"}]
        }
        resp = requests.post(RPC_URL, json=payload, timeout=5)
        resp.raise_for_status()
        result = resp.json().get("result", {}).get("value")
        return result
    except Exception as e:
        logger.error(f"Erreur getAccountInfo {address}: {e}")
        return None


def get_spl_decimals(mint_addr: str) -> Optional[int]:
    """Récupérer le nombre de décimales d'un token SPL"""
    if mint_addr == SOL_MINT:
        return 9
    try:
        result = get_account_info(mint_addr)
        if not result or not result.get("data"):
            return None
        raw = base64.b64decode(result["data"][0])
        decimals = raw[44]  # u8 at offset 44 for SPL mint account
        return decimals
    except Exception as e:
        logger.error(f"Erreur get_spl_decimals {mint_addr}: {e}")
        return None


def parse_pumpswap_pool(b64_data: str) -> Optional[dict]:
    """Parser les données d'un pool PumpSwap"""
    try:
        raw = base64.b64decode(b64_data)
        # PumpSwap Pool struct:
        # 8 bytes discriminator
        # 1 byte pool_bump
        # 2 bytes index
        # 32 bytes creator
        # 32 bytes base_mint
        # 32 bytes quote_mint
        # 32 bytes lp_mint
        # 32 bytes pool_base_token_account
        # 32 bytes pool_quote_token_account
        offset = 8 + 1 + 2 + 32  # skip discriminator, bump, index, creator
        base_mint = base58.b58encode(raw[offset:offset+32]).decode()
        offset += 32
        quote_mint = base58.b58encode(raw[offset:offset+32]).decode()
        offset += 32
        # skip lp_mint
        offset += 32
        base_vault = base58.b58encode(raw[offset:offset+32]).decode()
        offset += 32
        quote_vault = base58.b58encode(raw[offset:offset+32]).decode()

        return {
            "base_mint": base_mint,
            "quote_mint": quote_mint,
            "base_vault": base_vault,
            "quote_vault": quote_vault,
        }
    except Exception as e:
        logger.error(f"Erreur parse_pumpswap_pool: {e}")
        return None


def parse_token_account_amount(b64_data: str) -> Optional[int]:
    """Extraire le montant d'un token account"""
    try:
        raw = base64.b64decode(b64_data)
        amount = struct.unpack_from("<Q", raw, 64)[0]
        return amount
    except Exception as e:
        logger.error(f"Erreur parse_token_account_amount: {e}")
        return None


def calculate_price_sol(base_amount: int, quote_amount: int,
                        base_decimals: int, quote_decimals: int,
                        base_mint: str) -> Optional[float]:
    """Calculer le prix en SOL d'un token à partir des réserves du pool"""
    if base_amount == 0 or quote_amount == 0:
        return None

    if base_mint == SOL_MINT:
        # base = SOL, quote = token → price = base_amount / quote_amount (ajusté)
        price = (base_amount * (10 ** quote_decimals)) / (quote_amount * (10 ** base_decimals))
    else:
        # base = token, quote = SOL → price = quote_amount / base_amount (ajusté)
        price = (quote_amount * (10 ** base_decimals)) / (base_amount * (10 ** quote_decimals))

    return price


# ============================================================
# PRICE MONITOR CLASS
# ============================================================

class PriceMonitor:
    """
    Moniteur de prix en temps réel via WebSocket.
    Surveille les pools des positions ouvertes et déclenche des callbacks
    quand le prix change (pour TP/SL instantané).
    """

    def __init__(self, on_price_update: Callable = None):
        """
        Args:
            on_price_update: async callback(token_address, price_sol, price_change_pct)
        """
        self.on_price_update = on_price_update
        self.monitored_pools: Dict[str, PoolInfo] = {}  # token_address → PoolInfo
        self.subscription_map: Dict[int, tuple] = {}  # sub_id → (token_address, "base"|"quote")
        self.vault_amounts: Dict[str, int] = {}  # vault_address → amount
        self._ws = None
        self._running = False
        self._reconnect_delay = 1
        self._task = None

    async def start(self):
        """Démarrer le monitoring WebSocket en arrière-plan"""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._ws_loop())
        logger.info("🔌 PriceMonitor WebSocket démarré")

    async def stop(self):
        """Arrêter le monitoring"""
        self._running = False
        if self._ws:
            await self._ws.close()
        if self._task:
            self._task.cancel()
        logger.info("🔌 PriceMonitor WebSocket arrêté")

    async def add_token(self, token_address: str) -> bool:
        """
        Ajouter un token à surveiller.
        Trouve le pool, récupère les vaults, et s'abonne aux changements.
        """
        if token_address in self.monitored_pools:
            return True  # Déjà surveillé

        # 1. Trouver le pool via DexScreener
        pair_info = fetch_pair_address(token_address)
        if not pair_info or not pair_info.get("pair_address"):
            logger.warning(f"⚠️ Pas de pool trouvé pour {token_address}")
            return False

        pool_address = pair_info["pair_address"]
        dex_id = pair_info.get("dex_id", "")

        # 2. Récupérer les données du pool
        pool_data = get_account_info(pool_address)
        if not pool_data or not pool_data.get("data"):
            logger.warning(f"⚠️ Impossible de lire le pool {pool_address}")
            return False

        # 3. Parser le pool (PumpSwap)
        if "pump" in dex_id:
            parsed = parse_pumpswap_pool(pool_data["data"][0])
        else:
            # Pour Raydium, on utilise une approche simplifiée via le prix DexScreener
            # et le polling rapide (pas de WS pour Raydium dans cette version)
            logger.info(f"ℹ️ Pool Raydium détecté pour {token_address}, monitoring via polling")
            return False

        if not parsed:
            return False

        # 4. Récupérer les décimales
        base_dec = get_spl_decimals(parsed["base_mint"])
        quote_dec = get_spl_decimals(parsed["quote_mint"])
        if base_dec is None or quote_dec is None:
            logger.warning(f"⚠️ Impossible de récupérer les décimales pour {token_address}")
            return False

        # 5. Créer le PoolInfo
        pool_info = PoolInfo(
            pool_address=pool_address,
            base_vault=parsed["base_vault"],
            quote_vault=parsed["quote_vault"],
            base_mint=parsed["base_mint"],
            quote_mint=parsed["quote_mint"],
            base_decimals=base_dec,
            quote_decimals=quote_dec,
            token_address=token_address,
        )

        # 6. Récupérer les montants initiaux des vaults
        for vault_addr in [pool_info.base_vault, pool_info.quote_vault]:
            vault_data = get_account_info(vault_addr)
            if vault_data and vault_data.get("data"):
                amount = parse_token_account_amount(vault_data["data"][0])
                if amount is not None:
                    self.vault_amounts[vault_addr] = amount

        # Calculer le prix initial
        base_amt = self.vault_amounts.get(pool_info.base_vault, 0)
        quote_amt = self.vault_amounts.get(pool_info.quote_vault, 0)
        if base_amt and quote_amt:
            price = calculate_price_sol(base_amt, quote_amt,
                                        pool_info.base_decimals, pool_info.quote_decimals,
                                        pool_info.base_mint)
            if price:
                pool_info.latest_price_sol = price
                pool_info.last_update = time.time()

        self.monitored_pools[token_address] = pool_info
        logger.info(f"✅ Monitoring WS ajouté: {token_address} (pool: {pool_address[:8]}...)")

        # 7. S'abonner via WebSocket si connecté
        if self._ws and self._ws.open:
            await self._subscribe_pool(pool_info)

        return True

    async def remove_token(self, token_address: str):
        """Retirer un token du monitoring"""
        if token_address in self.monitored_pools:
            pool_info = self.monitored_pools.pop(token_address)
            # Nettoyer les vault amounts
            self.vault_amounts.pop(pool_info.base_vault, None)
            self.vault_amounts.pop(pool_info.quote_vault, None)
            logger.info(f"🗑️ Monitoring WS retiré: {token_address}")
            # Note: on ne peut pas unsubscribe facilement, mais c'est OK
            # les notifications seront juste ignorées

    async def _subscribe_pool(self, pool_info: PoolInfo):
        """S'abonner aux changements des vaults d'un pool"""
        if not self._ws or not self._ws.open:
            return

        # Subscribe base vault
        msg_id_base = hash(pool_info.base_vault) % 100000
        await self._ws.send(json.dumps({
            "jsonrpc": "2.0",
            "id": msg_id_base,
            "method": "accountSubscribe",
            "params": [pool_info.base_vault, {"encoding": "base64", "commitment": "confirmed"}]
        }))

        # Subscribe quote vault
        msg_id_quote = hash(pool_info.quote_vault) % 100000 + 100000
        await self._ws.send(json.dumps({
            "jsonrpc": "2.0",
            "id": msg_id_quote,
            "method": "accountSubscribe",
            "params": [pool_info.quote_vault, {"encoding": "base64", "commitment": "confirmed"}]
        }))

        # Store pending subscriptions (will be mapped when confirmation arrives)
        self._pending_subs = getattr(self, '_pending_subs', {})
        self._pending_subs[msg_id_base] = (pool_info.token_address, "base", pool_info.base_vault)
        self._pending_subs[msg_id_quote] = (pool_info.token_address, "quote", pool_info.quote_vault)

    async def _ws_loop(self):
        """Boucle principale WebSocket avec reconnexion automatique"""
        while self._running:
            try:
                async with websockets.connect(WS_URL, ping_interval=30, ping_timeout=10) as ws:
                    self._ws = ws
                    self._reconnect_delay = 1
                    logger.info(f"🔌 WebSocket connecté: {WS_URL[:50]}...")

                    # Re-souscrire à tous les pools existants
                    self._pending_subs = {}
                    for pool_info in self.monitored_pools.values():
                        await self._subscribe_pool(pool_info)

                    # Boucle de réception
                    while self._running:
                        try:
                            raw_msg = await asyncio.wait_for(ws.recv(), timeout=60)
                            msg = json.loads(raw_msg)
                            await self._handle_message(msg)
                        except asyncio.TimeoutError:
                            # Pas de message depuis 60s, envoyer un ping
                            continue
                        except websockets.ConnectionClosed:
                            logger.warning("🔌 WebSocket déconnecté")
                            break

            except Exception as e:
                logger.error(f"🔌 Erreur WebSocket: {e}")

            if self._running:
                logger.info(f"🔄 Reconnexion dans {self._reconnect_delay}s...")
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, 30)

    async def _handle_message(self, msg: dict):
        """Traiter un message WebSocket"""
        # Confirmation de souscription
        if "result" in msg and "id" in msg:
            msg_id = msg["id"]
            sub_id = msg["result"]
            pending = getattr(self, '_pending_subs', {})
            if msg_id in pending:
                token_addr, vault_type, vault_addr = pending.pop(msg_id)
                self.subscription_map[sub_id] = (token_addr, vault_type, vault_addr)
            return

        # Notification de changement de compte
        if msg.get("method") == "accountNotification":
            params = msg.get("params", {})
            sub_id = params.get("subscription")
            if sub_id not in self.subscription_map:
                return

            token_addr, vault_type, vault_addr = self.subscription_map[sub_id]
            value = params.get("result", {}).get("value", {})
            data = value.get("data")

            if not data or not data[0]:
                return

            # Parser le montant du token account
            amount = parse_token_account_amount(data[0])
            if amount is None:
                return

            # Mettre à jour le montant
            self.vault_amounts[vault_addr] = amount

            # Recalculer le prix
            if token_addr in self.monitored_pools:
                pool_info = self.monitored_pools[token_addr]
                base_amt = self.vault_amounts.get(pool_info.base_vault, 0)
                quote_amt = self.vault_amounts.get(pool_info.quote_vault, 0)

                if base_amt and quote_amt:
                    new_price = calculate_price_sol(
                        base_amt, quote_amt,
                        pool_info.base_decimals, pool_info.quote_decimals,
                        pool_info.base_mint
                    )

                    if new_price and new_price > 0:
                        old_price = pool_info.latest_price_sol
                        pool_info.latest_price_sol = new_price
                        pool_info.last_update = time.time()

                        # Calculer le changement
                        if old_price > 0:
                            change_pct = ((new_price - old_price) / old_price) * 100
                        else:
                            change_pct = 0

                        # Appeler le callback
                        if self.on_price_update and abs(change_pct) > 0.1:
                            try:
                                await self.on_price_update(token_addr, new_price, change_pct)
                            except Exception as e:
                                logger.error(f"Erreur callback price_update: {e}")

    def get_price(self, token_address: str) -> Optional[float]:
        """Récupérer le dernier prix connu d'un token (en SOL)"""
        pool_info = self.monitored_pools.get(token_address)
        if pool_info and pool_info.latest_price_sol > 0:
            return pool_info.latest_price_sol
        return None

    def get_all_prices(self) -> Dict[str, float]:
        """Récupérer tous les prix connus"""
        return {
            addr: info.latest_price_sol
            for addr, info in self.monitored_pools.items()
            if info.latest_price_sol > 0
        }

    @property
    def is_connected(self) -> bool:
        """Vérifier si le WebSocket est connecté"""
        return self._ws is not None and self._ws.open

    @property
    def monitored_count(self) -> int:
        """Nombre de tokens surveillés"""
        return len(self.monitored_pools)
