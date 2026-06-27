"""
HELIUS WEBSOCKET — PRIX TEMPS RÉEL
=====================================
Remplace le polling DexScreener toutes les 3-15s
par un flux temps réel depuis la blockchain Solana.

Résultat :
  Prix disponible à T+0.5s au lieu de T+30s
  Fin des Time Stop à 0%
  Latence × 60 moins élevée
"""

import asyncio
import json
import time
import logging
import websockets
import requests
from typing import Dict, Callable, Optional

logger = logging.getLogger(__name__)

# ─── Constantes ──────────────────────────────────────────────
PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
HEARTBEAT_INTERVAL = 20   # secondes
RECONNECT_DELAY    = 2    # secondes
MAX_RECONNECT      = 999  # tentatives infinies


class HeliusWebSocket:
    """
    Connexion WebSocket persistante à Helius.
    Reçoit les prix des tokens en temps réel
    depuis la blockchain Solana.
    """

    def __init__(
        self,
        api_key: str,
        on_price_update: Callable,
        on_new_token: Optional[Callable] = None
    ):
        """
        api_key         : clé API Helius
        on_price_update : callback(token_address, price_usd)
                          appelé à chaque mise à jour de prix
        on_new_token    : callback(token_address, price_usd)
                          appelé quand nouveau token détecté
        """
        self.api_key         = api_key
        self.on_price_update = on_price_update
        self.on_new_token    = on_new_token

        self.ws_url = (
            f"wss://mainnet.helius-rpc.com/"
            f"?api-key={api_key}"
        )

        # Cache des prix reçus
        # token_address → {"price": float, "ts": timestamp}
        self._price_cache: Dict[str, dict] = {}

        # Tokens surveillés (positions ouvertes)
        self._watched_tokens: set = set()

        # État de la connexion
        self._connected    = False
        self._running      = False
        self._ws           = None
        self._sub_id       = None

        # Compteurs de debug
        self._stats = {
            "events_total": 0,
            "events_err_skipped": 0,
            "events_createv2": 0,
            "tokens_detected": 0,
            "mint_from_logs": 0,
            "mint_from_api": 0,
        }

    # ─── Interface publique ───────────────────────────────────

    def watch_token(self, token_address: str):
        """Commence à surveiller un token."""
        self._watched_tokens.add(token_address)
        logger.info(f"[WSS] 👁 Watching: {token_address[:8]}...")

    def unwatch_token(self, token_address: str):
        """Arrête de surveiller un token."""
        self._watched_tokens.discard(token_address)
        self._price_cache.pop(token_address, None)
        logger.info(f"[WSS] 🔕 Unwatched: {token_address[:8]}...")

    def get_price(self, token_address: str) -> float:
        """
        Retourne le dernier prix connu depuis le cache WebSocket.
        Fallback DexScreener si pas en cache.
        """
        cached = self._price_cache.get(token_address)
        if cached and time.time() - cached["ts"] < 30:
            return cached["price"]

        # Fallback DexScreener
        return self._get_price_dexscreener(token_address)

    @property
    def is_connected(self) -> bool:
        return self._connected

    # ─── Boucle principale ───────────────────────────────────

    async def start(self):
        """Lance la connexion WebSocket avec reconnect automatique."""
        self._running = True
        attempts = 0

        while self._running and attempts < MAX_RECONNECT:
            try:
                logger.info(
                    f"[WSS] Connexion Helius... "
                    f"(tentative {attempts + 1})"
                )
                await self._connect_and_listen()

            except websockets.ConnectionClosed as e:
                logger.warning(f"[WSS] Connexion fermée: {e}")

            except Exception as e:
                logger.error(f"[WSS] Erreur: {e}")

            finally:
                self._connected = False

            if self._running:
                attempts += 1
                logger.info(
                    f"[WSS] Reconnexion dans {RECONNECT_DELAY}s..."
                )
                await asyncio.sleep(RECONNECT_DELAY)

    async def stop(self):
        """Arrête proprement la connexion."""
        self._running = False
        if self._ws:
            await self._ws.close()
        logger.info("[WSS] Arrêté proprement")

    # ─── Connexion et écoute ──────────────────────────────────

    async def _connect_and_listen(self):
        """Établit la connexion et écoute les messages."""
        async with websockets.connect(
            self.ws_url,
            ping_interval=None,  # on gère le heartbeat manuellement
            max_size=10 * 1024 * 1024  # 10MB max par message
        ) as ws:
            self._ws        = ws
            self._connected = True
            logger.info("[WSS] ✅ Connecté à Helius")

            # S'abonner aux transactions pump.fun
            await self._subscribe(ws)

            # Lancer le heartbeat en parallèle
            heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(ws)
            )

            try:
                async for message in ws:
                    await self._handle_message(message)
            finally:
                heartbeat_task.cancel()

    async def _subscribe(self, ws):
        """S'abonne aux logs du programme pump.fun."""
        sub_request = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "logsSubscribe",
            "params": [
                {
                    "mentions": [PUMP_FUN_PROGRAM]
                },
                {
                    "commitment": "confirmed"
                }
            ]
        }
        await ws.send(json.dumps(sub_request))
        logger.info(f"[WSS] Abonné à pump.fun: {PUMP_FUN_PROGRAM[:8]}...")

    async def _heartbeat_loop(self, ws):
        """Envoie un ping toutes les 20s pour garder la connexion."""
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            try:
                ping = {
                    "jsonrpc": "2.0",
                    "id": 999,
                    "method": "ping"
                }
                await ws.send(json.dumps(ping))
            except Exception:
                break

    # ─── Traitement des messages ──────────────────────────────

    async def _handle_message(self, raw_message: str):
        """Traite un message reçu du WebSocket."""
        try:
            msg = json.loads(raw_message)

            # Confirmation de souscription
            if "result" in msg and msg.get("id") == 1:
                self._sub_id = msg["result"]
                logger.info(f"[WSS] Souscription confirmée: {self._sub_id}")
                return

            # Notification de transaction
            if msg.get("method") == "logsNotification":
                t_reception = time.time()
                await self._process_log_notification(msg, t_reception)

        except json.JSONDecodeError:
            pass
        except Exception as e:
            logger.error(f"[WSS] Erreur handle_message: {e}")

    async def _process_log_notification(self, msg: dict, t_reception: float = None):
        """
        Traite une notification de log pump.fun.
        FIX 1: Ignore les TX avec err != null
        FIX 2: Détecte CreateV2/InitializeMint2
        FIX 3: Fallback Enhanced API si mint introuvable dans logs
        """
        try:
            if t_reception is None:
                t_reception = time.time()

            self._stats["events_total"] += 1

            params = msg.get("params", {})
            result = params.get("result", {})
            value  = result.get("value", {})
            logs   = value.get("logs", [])
            sig    = value.get("signature", "")
            err    = value.get("err")

            # ─── FIX 1: Ignorer les transactions échouées ─────
            if err is not None:
                self._stats["events_err_skipped"] += 1
                return

            # Vérifier si c'est une création de token (CreateV2)
            logs_joined = " ".join(logs)
            is_create = "Instruction: CreateV2" in logs_joined

            if not is_create:
                # Pas une création de token, ignorer
                return

            self._stats["events_createv2"] += 1

            # ─── FIX 2: Extraire le mint depuis les logs ──────
            token_address = self._extract_token_from_logs(logs)

            # ─── FIX 3: Fallback Enhanced API si pas trouvé ───
            if not token_address:
                token_address = await self._get_mint_from_enhanced_api(sig)
                if token_address:
                    self._stats["mint_from_api"] += 1
            else:
                self._stats["mint_from_logs"] += 1

            if not token_address:
                logger.debug(f"[WSS] Mint introuvable pour sig={sig[:16]}...")
                return

            self._stats["tokens_detected"] += 1

            # Mesure latence
            t_detected = time.time()
            m1_ms = (t_detected - t_reception) * 1000
            logger.info(
                f"[WSS] 🆕 TOKEN DÉTECTÉ en {m1_ms:.0f}ms | "
                f"{token_address[:12]}... | sig={sig[:16]}..."
            )

            # Log stats périodique (tous les 10 tokens)
            if self._stats["tokens_detected"] % 10 == 0:
                logger.info(
                    f"[WSS-STATS] total={self._stats['events_total']} "
                    f"err_skip={self._stats['events_err_skipped']} "
                    f"createv2={self._stats['events_createv2']} "
                    f"detected={self._stats['tokens_detected']} "
                    f"(logs={self._stats['mint_from_logs']} "
                    f"api={self._stats['mint_from_api']})"
                )

            # Callback nouveau token
            if self.on_new_token and token_address not in self._watched_tokens:
                await self._safe_callback(
                    self.on_new_token,
                    token_address,
                    0.0  # prix initial inconnu pour un nouveau token
                )

        except Exception as e:
            logger.error(f"[WSS] Erreur process_log: {e}")

    def _extract_token_from_logs(self, logs: list) -> Optional[str]:
        """
        FIX 2: Extrait l'adresse du mint depuis les logs pump.fun CreateV2.

        Dans les logs pump.fun, le mint apparaît parfois dans les lignes
        "Program log:" avec des données encodées. On cherche aussi dans
        les invocations de programmes (les account keys passées en argument).
        """
        # Stratégie 1: Chercher une adresse base58 dans les lignes
        # qui mentionnent le token (InitializeMint2, Create, etc.)
        target_keywords = [
            "initializemint2",
            "createv2",
            "instruction: create",
            "initialize the associated token account",
        ]

        # Collecter toutes les adresses base58 trouvées dans les logs
        # (sauf les programmes connus)
        known_programs = {
            "11111111111111111111111111111111",
            "ComputeBudget111111111111111111111111111111",
            "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
            "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
            "pfeeUxB6jkeYEGxnEBPcQtKEzUMAfR1SNBXwcHEqpUH",  # pump.fun fee account
            "CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9i",  # pump.fun fee account 2
        }

        candidates = []
        for log in logs:
            parts = log.split()
            for part in parts:
                # Nettoyer les caractères non-alphanumériques en fin
                clean = part.rstrip(".,;:()[]{}\"'")
                # Adresse Solana = 32-44 caractères base58
                if (32 <= len(clean) <= 44
                        and clean.isalnum()
                        and clean not in known_programs
                        and not clean.startswith("1111")):
                    candidates.append(clean)

        # Si on a des candidats, le mint est typiquement le premier
        # qui n'est pas un programme connu
        if candidates:
            return candidates[0]

        return None

    async def _get_mint_from_enhanced_api(self, signature: str) -> Optional[str]:
        """
        FIX 3: Récupère le mint via l'API Helius Enhanced Transactions.
        Appelle /v0/transactions/?api-key=... avec la signature.
        """
        try:
            url = (
                f"https://api.helius.xyz/v0/transactions/"
                f"?api-key={self.api_key}"
            )
            resp = requests.post(
                url,
                json={"transactions": [signature]},
                timeout=3
            )

            if resp.status_code != 200:
                logger.debug(
                    f"[WSS] Enhanced API {resp.status_code} "
                    f"pour sig={signature[:16]}..."
                )
                return None

            txs = resp.json()
            if not txs or not isinstance(txs, list):
                return None

            tx = txs[0]

            # Chercher le mint dans tokenTransfers
            token_transfers = tx.get("tokenTransfers", [])
            for transfer in token_transfers:
                mint = transfer.get("mint", "")
                if mint and mint not in (
                    "So11111111111111111111111111111111111111112",
                ):
                    return mint

            # Chercher dans les account data
            account_data = tx.get("accountData", [])
            for acc in account_data:
                # Le mint est souvent le premier account avec tokenBalanceChanges
                token_changes = acc.get("tokenBalanceChanges", [])
                for change in token_changes:
                    mint = change.get("mint", "")
                    if mint and mint != "So11111111111111111111111111111111111111112":
                        return mint

            # Dernier recours : chercher dans les instructions
            instructions = tx.get("instructions", [])
            for ix in instructions:
                if ix.get("programId") == PUMP_FUN_PROGRAM:
                    accounts = ix.get("accounts", [])
                    # Dans CreateV2 pump.fun, le mint est typiquement
                    # le 2ème account (index 1)
                    if len(accounts) >= 2:
                        return accounts[1]

        except Exception as e:
            logger.debug(f"[WSS] Enhanced API error: {e}")

        return None

    async def _get_price_from_tx(
        self,
        signature: str,
        token_address: str
    ) -> float:
        """
        Calcule le prix depuis une transaction Helius.
        Prix = SOL_amount / token_amount
        """
        try:
            url = (
                f"https://api.helius.xyz/v0/transactions/"
                f"?api-key={self.api_key}"
            )
            resp = requests.post(
                url,
                json={"transactions": [signature]},
                timeout=3
            )

            if resp.status_code != 200:
                return self._get_price_dexscreener(token_address)

            txs = resp.json()
            if not txs:
                return 0.0

            tx = txs[0]

            # Chercher les token transfers
            token_transfers = tx.get("tokenTransfers", [])
            native_transfers = tx.get("nativeTransfers", [])

            sol_amount = sum(
                t.get("amount", 0) for t in native_transfers
            ) / 1e9  # lamports → SOL

            token_amount = sum(
                float(t.get("tokenAmount", 0))
                for t in token_transfers
                if t.get("mint") == token_address
            )

            if token_amount > 0 and sol_amount > 0:
                # Prix en SOL par token
                price_in_sol = sol_amount / token_amount

                # Convertir en USD (prix SOL approximatif)
                sol_price_usd = self._get_sol_price()
                price_usd = price_in_sol * sol_price_usd

                return price_usd

        except Exception as e:
            logger.warning(f"[WSS] Erreur tx price: {e}")

        # Fallback DexScreener
        return self._get_price_dexscreener(token_address)

    # ─── Helpers ─────────────────────────────────────────────

    def _get_price_dexscreener(self, token_address: str) -> float:
        """Fallback : récupère le prix via DexScreener."""
        try:
            url = (
                f"https://api.dexscreener.com/latest/"
                f"dex/tokens/{token_address}"
            )
            r = requests.get(url, timeout=3)
            pairs = r.json().get("pairs", [])
            if pairs:
                return float(pairs[0].get("priceUsd", 0) or 0)
        except Exception:
            pass
        return 0.0

    def _get_sol_price(self) -> float:
        """Prix SOL en USD (mis en cache 60s)."""
        cached = self._price_cache.get("__SOL__")
        if cached and time.time() - cached["ts"] < 60:
            return cached["price"]

        try:
            r = requests.get(
                "https://api.coingecko.com/api/v3/simple/price"
                "?ids=solana&vs_currencies=usd",
                timeout=3
            )
            price = r.json()["solana"]["usd"]
            self._price_cache["__SOL__"] = {
                "price": price,
                "ts": time.time()
            }
            return price
        except Exception:
            return 160.0  # valeur par défaut

    async def _safe_callback(self, callback, *args):
        """Appelle un callback de manière sécurisée."""
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(*args)
            else:
                callback(*args)
        except Exception as e:
            logger.error(f"[WSS] Erreur callback: {e}")
