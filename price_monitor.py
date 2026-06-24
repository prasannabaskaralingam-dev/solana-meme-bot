"""
Price Monitor WebSocket — Helius WebSocket comme source PRIMAIRE de prix.

Architecture:
  1. CONNEXION: wss://mainnet.helius-rpc.com?api-key=HELIUS_API_KEY
  2. ABONNEMENT: accountSubscribe sur les vaults (base + quote) de chaque pool
  3. ÉVÉNEMENT: accountNotification → recalcul prix → callback immédiat
  4. HEARTBEAT: ping toutes les 30s, détection déconnexion
  5. FALLBACK: si WS down > 5s → signale au bot de basculer en polling
  6. UNSUBSCRIBE: accountUnsubscribe propre quand position fermée

Supporte:
  - PumpSwap: parsing direct du pool account → vaults → prix
  - Raydium AMM V4: parsing des vaults via offsets connus → prix
"""

import asyncio
import base64
import json
import struct
import logging
import time
import os
from typing import Optional, Callable, Dict, Set
from dataclasses import dataclass, field
from enum import Enum

import base58
import requests
import websockets

logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION
# ============================================================

HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY", "")
RPC_URL = os.environ.get("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")

# Endpoint WebSocket Helius (PRIMAIRE)
WS_URL = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}" if HELIUS_API_KEY else ""

# Fallback: dériver du RPC si pas de clé Helius
if not WS_URL:
    if "helius" in RPC_URL:
        WS_URL = RPC_URL.replace("https://", "wss://").replace("http://", "ws://")
    else:
        WS_URL = "wss://api.mainnet-beta.solana.com"

SOL_MINT = "So11111111111111111111111111111111111111112"
DEXSCREENER_API = "https://api.dexscreener.com/latest/dex/tokens"

# Paramètres WebSocket
WS_HEARTBEAT_INTERVAL = 30      # Ping toutes les 30s
WS_RECV_TIMEOUT = 45            # Timeout réception (doit être > heartbeat)
WS_RECONNECT_MAX_DELAY = 15     # Max delay entre reconnexions
WS_FALLBACK_THRESHOLD = 5       # Secondes sans WS avant de signaler fallback


# ============================================================
# ENUMS & DATACLASSES
# ============================================================

class MonitorStatus(Enum):
    """État du monitoring"""
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    FALLBACK = "fallback"
    STOPPED = "stopped"


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
    token_address: str
    dex_type: str = "pumpswap"
    latest_price_sol: float = 0.0
    last_update: float = 0.0
    # Subscription IDs pour unsubscribe propre
    base_sub_id: Optional[int] = None
    quote_sub_id: Optional[int] = None


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
        decimals = raw[44]
        return decimals
    except Exception as e:
        logger.error(f"Erreur get_spl_decimals {mint_addr}: {e}")
        return None


def parse_pumpswap_pool(b64_data: str) -> Optional[dict]:
    """Parser les données d'un pool PumpSwap"""
    try:
        raw = base64.b64decode(b64_data)
        offset = 8 + 1 + 2 + 32  # discriminator + bump + index + creator
        base_mint = base58.b58encode(raw[offset:offset+32]).decode()
        offset += 32
        quote_mint = base58.b58encode(raw[offset:offset+32]).decode()
        offset += 32
        offset += 32  # skip lp_mint
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


def parse_raydium_amm_pool(b64_data: str) -> Optional[dict]:
    """
    Parser les données d'un pool Raydium AMM V4.
    Offsets: coin_mint@432, pc_mint@464, coin_vault@496, pc_vault@528
    """
    try:
        raw = base64.b64decode(b64_data)
        if len(raw) < 560:
            logger.warning(f"Raydium pool data trop court: {len(raw)} bytes")
            return None

        coin_mint = base58.b58encode(raw[432:464]).decode()
        pc_mint = base58.b58encode(raw[464:496]).decode()
        coin_vault = base58.b58encode(raw[496:528]).decode()
        pc_vault = base58.b58encode(raw[528:560]).decode()

        return {
            "base_mint": coin_mint,
            "quote_mint": pc_mint,
            "base_vault": coin_vault,
            "quote_vault": pc_vault,
        }
    except Exception as e:
        logger.error(f"Erreur parse_raydium_amm_pool: {e}")
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
        price = (base_amount * (10 ** quote_decimals)) / (quote_amount * (10 ** base_decimals))
    else:
        price = (quote_amount * (10 ** base_decimals)) / (base_amount * (10 ** quote_decimals))

    return price


# ============================================================
# PRICE MONITOR CLASS
# ============================================================

class PriceMonitor:
    """
    Moniteur de prix en temps réel via Helius WebSocket.
    
    PRIORITÉ ABSOLUE: Prix reçu → cb.check() → SL -25% avant tout.
    
    Features:
      - Connexion Helius WebSocket avec heartbeat 30s
      - Reconnexion automatique avec backoff exponentiel
      - accountSubscribe sur les vaults (base + quote)
      - accountUnsubscribe propre quand position fermée
      - Signalement fallback si WS down > 5s
      - Callback immédiat sur changement de prix
    """

    def __init__(self, on_price_update: Callable = None,
                 on_fallback_change: Callable = None):
        """
        Args:
            on_price_update: async callback(token_address, price_sol, change_pct)
                             Appelé à CHAQUE changement de prix significatif.
            on_fallback_change: async callback(is_fallback: bool, reason: str)
                                Appelé quand le mode bascule WS↔fallback.
        """
        self.on_price_update = on_price_update
        self.on_fallback_change = on_fallback_change

        # Pools surveillés
        self.monitored_pools: Dict[str, PoolInfo] = {}  # token_address → PoolInfo
        self.subscription_map: Dict[int, tuple] = {}    # sub_id → (token_address, "base"|"quote", vault_addr)
        self.vault_amounts: Dict[str, int] = {}         # vault_address → amount

        # Pending subscriptions (msg_id → info)
        self._pending_subs: Dict[int, tuple] = {}

        # WebSocket state
        self._ws = None
        self._running = False
        self._reconnect_delay = 1
        self._task = None
        self._last_msg_time: float = 0.0
        self._status = MonitorStatus.STOPPED
        self._was_fallback = False

        # Stats
        self._stats = {
            "messages_received": 0,
            "price_updates_sent": 0,
            "reconnections": 0,
            "last_connected": 0.0,
            "fallback_activations": 0,
        }

    # ============================================================
    # PUBLIC API
    # ============================================================

    async def start(self):
        """Démarrer le monitoring WebSocket en arrière-plan"""
        if self._running:
            return
        self._running = True
        self._status = MonitorStatus.RECONNECTING
        self._task = asyncio.create_task(self._ws_loop())
        logger.info(f"🔌 PriceMonitor démarré (Helius WS: {WS_URL[:50]}...)")

    async def stop(self):
        """Arrêter le monitoring"""
        self._running = False
        self._status = MonitorStatus.STOPPED
        if self._ws:
            try:
                await self._ws.close()
            except:
                pass
        if self._task:
            self._task.cancel()
        logger.info("🔌 PriceMonitor arrêté")

    async def add_token(self, token_address: str) -> bool:
        """
        Ajouter un token à surveiller.
        Trouve le pool, récupère les vaults, et s'abonne aux changements.
        """
        if token_address in self.monitored_pools:
            return True

        # 1. Trouver le pool via DexScreener
        pair_info = fetch_pair_address(token_address)
        if not pair_info or not pair_info.get("pair_address"):
            logger.warning(f"⚠️ Pas de pool trouvé pour {token_address[:12]}")
            return False

        pool_address = pair_info["pair_address"]
        dex_id = pair_info.get("dex_id", "")

        # 2. Récupérer les données du pool
        pool_data = get_account_info(pool_address)
        if not pool_data or not pool_data.get("data"):
            logger.warning(f"⚠️ Impossible de lire le pool {pool_address[:12]}")
            return False

        # 3. Parser le pool selon le DEX
        parsed = None
        dex_type = "unknown"

        if "pump" in dex_id:
            parsed = parse_pumpswap_pool(pool_data["data"][0])
            dex_type = "pumpswap"
        elif "raydium" in dex_id:
            parsed = parse_raydium_amm_pool(pool_data["data"][0])
            dex_type = "raydium"
        else:
            logger.info(f"ℹ️ DEX non supporté pour WS: {dex_id} ({token_address[:12]})")
            return False

        if not parsed:
            logger.warning(f"⚠️ Impossible de parser le pool {dex_type} pour {token_address[:12]}")
            return False

        # 4. Récupérer les décimales
        base_dec = get_spl_decimals(parsed["base_mint"])
        quote_dec = get_spl_decimals(parsed["quote_mint"])
        if base_dec is None or quote_dec is None:
            logger.warning(f"⚠️ Impossible de récupérer les décimales pour {token_address[:12]}")
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
            dex_type=dex_type,
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
        logger.info(f"✅ WS monitoring: {token_address[:12]}... "
                    f"(pool: {pool_address[:8]}..., dex: {dex_type})")

        # 7. S'abonner via WebSocket si connecté
        if self._ws and not self._ws.closed:
            await self._subscribe_pool(pool_info)

        return True

    async def remove_token(self, token_address: str):
        """
        Retirer un token du monitoring.
        Envoie accountUnsubscribe propre au serveur.
        """
        if token_address not in self.monitored_pools:
            return

        pool_info = self.monitored_pools.pop(token_address)

        # Unsubscribe propre via WebSocket
        if self._ws and not self._ws.closed:
            await self._unsubscribe_pool(pool_info)

        # Nettoyer les vault amounts
        self.vault_amounts.pop(pool_info.base_vault, None)
        self.vault_amounts.pop(pool_info.quote_vault, None)

        # Nettoyer la subscription_map
        to_remove = [sid for sid, info in self.subscription_map.items()
                     if info[0] == token_address]
        for sid in to_remove:
            del self.subscription_map[sid]

        logger.info(f"🗑️ WS monitoring retiré: {token_address[:12]}... ({pool_info.dex_type})")

    # ============================================================
    # WEBSOCKET CORE
    # ============================================================

    async def _ws_loop(self):
        """Boucle principale WebSocket avec reconnexion automatique et heartbeat 30s"""
        while self._running:
            try:
                async with websockets.connect(
                    WS_URL,
                    ping_interval=WS_HEARTBEAT_INTERVAL,
                    ping_timeout=10,
                    close_timeout=5,
                    max_size=2**20,  # 1MB max message
                ) as ws:
                    self._ws = ws
                    self._reconnect_delay = 1
                    self._last_msg_time = time.time()
                    self._status = MonitorStatus.CONNECTED
                    self._stats["last_connected"] = time.time()
                    self._stats["reconnections"] += 1

                    logger.info(f"🔌 Helius WebSocket CONNECTÉ ({len(self.monitored_pools)} pools)")

                    # Si on revient de fallback, notifier
                    if self._was_fallback:
                        self._was_fallback = False
                        if self.on_fallback_change:
                            try:
                                await self.on_fallback_change(False, "WebSocket reconnecté")
                            except:
                                pass

                    # Re-souscrire à tous les pools existants
                    self._pending_subs = {}
                    for pool_info in self.monitored_pools.values():
                        await self._subscribe_pool(pool_info)

                    # Boucle de réception
                    while self._running:
                        try:
                            raw_msg = await asyncio.wait_for(ws.recv(), timeout=WS_RECV_TIMEOUT)
                            self._last_msg_time = time.time()
                            self._stats["messages_received"] += 1
                            msg = json.loads(raw_msg)
                            await self._handle_message(msg)
                        except asyncio.TimeoutError:
                            # Pas de message depuis WS_RECV_TIMEOUT — vérifier la connexion
                            # Le ping_interval de websockets gère le keepalive
                            elapsed = time.time() - self._last_msg_time
                            if elapsed > WS_RECV_TIMEOUT * 2:
                                logger.warning(f"🔌 Pas de message depuis {elapsed:.0f}s, reconnexion...")
                                break
                            continue
                        except websockets.ConnectionClosed as e:
                            logger.warning(f"🔌 WebSocket déconnecté: {e.code} {e.reason}")
                            break

            except (OSError, websockets.InvalidURI, websockets.InvalidHandshake) as e:
                logger.error(f"🔌 Erreur connexion WebSocket: {e}")
            except Exception as e:
                logger.error(f"🔌 Erreur WebSocket inattendue: {e}")

            # Déconnecté — signaler le fallback
            self._ws = None
            if self._running:
                self._status = MonitorStatus.FALLBACK
                if not self._was_fallback:
                    self._was_fallback = True
                    self._stats["fallback_activations"] += 1
                    if self.on_fallback_change:
                        try:
                            await self.on_fallback_change(True, f"WebSocket down, reconnexion dans {self._reconnect_delay}s")
                        except:
                            pass

                logger.info(f"🔄 Reconnexion dans {self._reconnect_delay}s...")
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, WS_RECONNECT_MAX_DELAY)

    async def _subscribe_pool(self, pool_info: PoolInfo):
        """S'abonner aux changements des vaults d'un pool"""
        if not self._ws or self._ws.closed:
            return

        # Subscribe base vault
        msg_id_base = abs(hash(pool_info.base_vault + "base")) % 1000000
        try:
            await self._ws.send(json.dumps({
                "jsonrpc": "2.0",
                "id": msg_id_base,
                "method": "accountSubscribe",
                "params": [pool_info.base_vault, {"encoding": "base64", "commitment": "confirmed"}]
            }))
            self._pending_subs[msg_id_base] = (pool_info.token_address, "base", pool_info.base_vault)
        except Exception as e:
            logger.error(f"Erreur subscribe base vault: {e}")

        # Subscribe quote vault
        msg_id_quote = abs(hash(pool_info.quote_vault + "quote")) % 1000000
        try:
            await self._ws.send(json.dumps({
                "jsonrpc": "2.0",
                "id": msg_id_quote,
                "method": "accountSubscribe",
                "params": [pool_info.quote_vault, {"encoding": "base64", "commitment": "confirmed"}]
            }))
            self._pending_subs[msg_id_quote] = (pool_info.token_address, "quote", pool_info.quote_vault)
        except Exception as e:
            logger.error(f"Erreur subscribe quote vault: {e}")

    async def _unsubscribe_pool(self, pool_info: PoolInfo):
        """Envoyer accountUnsubscribe pour les vaults d'un pool"""
        if not self._ws or self._ws.closed:
            return

        for sub_id in [pool_info.base_sub_id, pool_info.quote_sub_id]:
            if sub_id is not None:
                try:
                    await self._ws.send(json.dumps({
                        "jsonrpc": "2.0",
                        "id": abs(hash(f"unsub_{sub_id}")) % 1000000,
                        "method": "accountUnsubscribe",
                        "params": [sub_id]
                    }))
                except Exception as e:
                    logger.error(f"Erreur unsubscribe {sub_id}: {e}")

    # ============================================================
    # MESSAGE HANDLING
    # ============================================================

    async def _handle_message(self, msg: dict):
        """Traiter un message WebSocket — PRIORITÉ: prix → cb.check()"""

        # Confirmation de souscription
        if "result" in msg and "id" in msg:
            msg_id = msg["id"]
            sub_id = msg["result"]

            if msg_id in self._pending_subs:
                token_addr, vault_type, vault_addr = self._pending_subs.pop(msg_id)
                self.subscription_map[sub_id] = (token_addr, vault_type, vault_addr)

                # Stocker le sub_id dans le PoolInfo pour unsubscribe
                if token_addr in self.monitored_pools:
                    pool = self.monitored_pools[token_addr]
                    if vault_type == "base":
                        pool.base_sub_id = sub_id
                    else:
                        pool.quote_sub_id = sub_id
            return

        # Notification de changement de compte (ÉVÉNEMENT PRIX)
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

            # Recalculer le prix IMMÉDIATEMENT
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

                        # CALLBACK IMMÉDIAT — pas de seuil minimum
                        # Le CircuitBreaker décide, pas nous
                        if self.on_price_update and (abs(change_pct) > 0.05 or old_price == 0):
                            try:
                                self._stats["price_updates_sent"] += 1
                                await self.on_price_update(token_addr, new_price, change_pct)
                            except Exception as e:
                                logger.error(f"Erreur callback price_update: {e}")

    # ============================================================
    # PUBLIC GETTERS
    # ============================================================

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

    def get_ws_stats(self) -> dict:
        """Statistiques complètes du WebSocket monitoring"""
        pumpswap_count = sum(1 for p in self.monitored_pools.values() if p.dex_type == "pumpswap")
        raydium_count = sum(1 for p in self.monitored_pools.values() if p.dex_type == "raydium")

        # Calculer l'uptime
        now = time.time()
        last_connected = self._stats.get("last_connected", 0)
        uptime = now - last_connected if self._status == MonitorStatus.CONNECTED else 0

        return {
            "status": self._status.value,
            "connected": self.is_connected,
            "total_monitored": len(self.monitored_pools),
            "pumpswap_pools": pumpswap_count,
            "raydium_pools": raydium_count,
            "subscriptions": len(self.subscription_map),
            "messages_received": self._stats["messages_received"],
            "price_updates_sent": self._stats["price_updates_sent"],
            "reconnections": self._stats["reconnections"],
            "fallback_activations": self._stats["fallback_activations"],
            "uptime_seconds": round(uptime),
            "last_msg_age_seconds": round(now - self._last_msg_time) if self._last_msg_time else -1,
        }

    @property
    def is_connected(self) -> bool:
        """Vérifier si le WebSocket est connecté"""
        return self._ws is not None and not self._ws.closed

    @property
    def is_fallback(self) -> bool:
        """Vérifier si on est en mode fallback (WS down)"""
        if not self._running:
            return True
        if not self.is_connected:
            return True
        # Vérifier le temps depuis le dernier message
        if self._last_msg_time > 0:
            elapsed = time.time() - self._last_msg_time
            return elapsed > WS_FALLBACK_THRESHOLD
        return False

    @property
    def status(self) -> MonitorStatus:
        """État actuel du monitoring"""
        return self._status

    @property
    def monitored_count(self) -> int:
        """Nombre de tokens surveillés"""
        return len(self.monitored_pools)
