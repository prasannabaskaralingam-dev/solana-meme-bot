"""
Token Filter — Vérifications on-chain RAPIDES et INDÉPENDANTES de RugCheck.

3 vérifications critiques avant chaque achat :
  1. Mint Authority : DOIT être révoquée (sinon le dev peut mint ∞ tokens)
  2. LP Burned/Locked : La liquidité DOIT être brûlée ou lockée (sinon rug pull)
  3. Deployer : Récupérer l'adresse du créateur pour la corrélation inter-positions

Ces checks sont ON-CHAIN (RPC direct) et ne dépendent PAS de RugCheck.
Ils servent de DOUBLE-CHECK rapide en plus de token_security.py.

Usage:
    tf = TokenFilter(rpc_url="https://...")
    result = await tf.check(token_address)
    if not result.is_safe:
        reject(result.rejection_reason)
"""

import asyncio
import logging
import struct
import time
from dataclasses import dataclass
from typing import Optional, Tuple

import httpx
import base58

logger = logging.getLogger(__name__)


# ============================================================
# CONFIGURATION
# ============================================================

# Adresses de burn connues sur Solana
BURN_ADDRESSES = {
    "1nc1nerator11111111111111111111111111111111",
    "11111111111111111111111111111111",
    "1111111111111111111111111111111111111111111",
    "1111111QLbz7JHiBTspS962RLKV8GndWFwiEaqKM",
}

# Programmes de lock LP connus
LP_LOCK_PROGRAMS = {
    "2r5VekMNiWPzi1pWwvJczrdPaZnJG59u91unSrTunwJg",  # Uncx (Solana)
    "Lock7kBijGCQLEFAmXcengzXKA88iDNQPriQ7TbgJHsN",  # Team.Finance
    "FLockLhTEBLgMFPDXnfnMVSKb4LHVnpxgXKsAADGfxY",  # Fluxbeam Lock
}

# Programme PumpSwap (pour détecter les pools pump.fun)
PUMPSWAP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMP_FUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

# Raydium AMM Program
RAYDIUM_AMM_PROGRAM = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
RAYDIUM_CPMM_PROGRAM = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"


# ============================================================
# RESULT DATACLASS
# ============================================================

@dataclass
class TokenFilterResult:
    """Résultat du filtre on-chain"""
    token_address: str
    is_safe: bool
    rejection_reason: str = ""

    # Détails des checks
    mint_authority_revoked: bool = False
    freeze_authority_revoked: bool = False
    lp_burned: bool = False
    lp_locked: bool = False
    deployer_address: Optional[str] = None

    # Métadonnées
    check_duration_ms: float = 0.0
    supply: Optional[int] = None
    decimals: Optional[int] = None

    @property
    def safety_summary(self) -> str:
        """Résumé court pour les logs"""
        parts = []
        parts.append(f"Mint:{'✅' if self.mint_authority_revoked else '❌'}")
        parts.append(f"Freeze:{'✅' if self.freeze_authority_revoked else '❌'}")
        if self.lp_burned:
            parts.append("LP:🔥")
        elif self.lp_locked:
            parts.append("LP:🔒")
        else:
            parts.append("LP:⚠️")
        if self.deployer_address:
            parts.append(f"Dev:{self.deployer_address[:8]}")
        return " | ".join(parts)


# ============================================================
# TOKEN FILTER CLASS
# ============================================================

class TokenFilter:
    """
    Filtre on-chain rapide — vérifie mint authority, LP, et deployer
    directement via RPC sans dépendre d'APIs tierces.
    
    Temps de réponse cible: < 500ms (2 appels RPC parallèles).
    """

    def __init__(self, rpc_url: str = "https://api.mainnet-beta.solana.com"):
        self.rpc_url = rpc_url
        self._client = httpx.AsyncClient(timeout=8)
        # Cache pour éviter de re-vérifier
        self._cache: dict[str, TokenFilterResult] = {}
        self._cache_ttl = 600  # 10 minutes

    async def check(self, token_address: str, require_lp_burned: bool = True,
                    pair_address: Optional[str] = None) -> TokenFilterResult:
        """
        Vérification complète on-chain d'un token.
        
        Args:
            token_address: Mint address du token
            require_lp_burned: Si True, rejette si LP non burned (mode sniper strict)
            pair_address: Adresse du pool (optionnel, pour vérifier LP)
            
        Returns:
            TokenFilterResult avec is_safe et détails
        """
        # Check cache
        if token_address in self._cache:
            cached = self._cache[token_address]
            if time.time() - cached.check_duration_ms < self._cache_ttl:
                return cached

        start = time.time()
        result = TokenFilterResult(token_address=token_address, is_safe=True)

        try:
            # === CHECK 1: Mint & Freeze Authority (on-chain direct) ===
            mint_ok = await self._check_mint_authority(token_address, result)

            # === CHECK 2: Deployer (premier signataire de la tx de création) ===
            await self._get_deployer(token_address, result)

            # === CHECK 3: LP Burned/Locked ===
            if pair_address:
                await self._check_lp_status(pair_address, result)

            # === DÉCISION FINALE ===
            if not result.mint_authority_revoked:
                result.is_safe = False
                result.rejection_reason = "❌ Mint Authority ACTIVE (dev peut mint ∞ tokens)"
            elif not result.freeze_authority_revoked:
                result.is_safe = False
                result.rejection_reason = "❌ Freeze Authority ACTIVE (dev peut bloquer tes tokens)"
            elif require_lp_burned and pair_address and not result.lp_burned and not result.lp_locked:
                result.is_safe = False
                result.rejection_reason = "❌ LP non burned/locked (rug pull possible)"

        except Exception as e:
            logger.error(f"[TokenFilter] Erreur check {token_address[:12]}: {e}")
            # En cas d'erreur, on rejette par sécurité
            result.is_safe = False
            result.rejection_reason = f"❌ Erreur vérification on-chain: {e}"

        elapsed = (time.time() - start) * 1000
        result.check_duration_ms = elapsed

        # Cache le résultat
        self._cache[token_address] = result

        # Log
        status = "✅ SAFE" if result.is_safe else f"🚫 REJETÉ"
        logger.info(f"[TokenFilter] {status} {token_address[:12]}... "
                    f"({elapsed:.0f}ms) — {result.safety_summary}")

        return result

    # ----------------------------------------------------------
    # CHECK 1: MINT & FREEZE AUTHORITY
    # ----------------------------------------------------------

    async def _check_mint_authority(self, token_address: str, result: TokenFilterResult) -> bool:
        """Vérifier si mint/freeze authority sont révoquées via getAccountInfo jsonParsed"""
        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getAccountInfo",
                "params": [token_address, {"encoding": "jsonParsed"}]
            }
            resp = await self._client.post(self.rpc_url, json=payload)
            data = resp.json()

            value = data.get("result", {}).get("value")
            if not value:
                return False

            parsed = value.get("data", {}).get("parsed", {})
            info = parsed.get("info", {})

            # Mint authority
            mint_auth = info.get("mintAuthority")
            result.mint_authority_revoked = (mint_auth is None or mint_auth == "")

            # Freeze authority
            freeze_auth = info.get("freezeAuthority")
            result.freeze_authority_revoked = (freeze_auth is None or freeze_auth == "")

            # Supply et decimals (bonus info)
            result.supply = int(info.get("supply", 0))
            result.decimals = info.get("decimals")

            return result.mint_authority_revoked

        except Exception as e:
            logger.error(f"[TokenFilter] Erreur check mint authority: {e}")
            return False

    # ----------------------------------------------------------
    # CHECK 2: DEPLOYER (CRÉATEUR DU TOKEN)
    # ----------------------------------------------------------

    async def _get_deployer(self, token_address: str, result: TokenFilterResult):
        """
        Récupérer l'adresse du deployer via getSignaturesForAddress.
        Le deployer est le premier signataire de la première transaction du mint.
        """
        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getSignaturesForAddress",
                "params": [
                    token_address,
                    {"limit": 1, "before": None}
                ]
            }
            resp = await self._client.post(self.rpc_url, json=payload)
            data = resp.json()

            signatures = data.get("result", [])
            if not signatures:
                return

            # Récupérer la première transaction (la plus ancienne)
            # Note: getSignaturesForAddress retourne du plus récent au plus ancien
            # On prend la dernière signature disponible
            # Pour obtenir la PREMIÈRE tx, on fait un appel avec "until" = None et limit élevé
            # Mais c'est trop lent. Alternative: on utilise la signature la plus ancienne retournée.
            
            # Méthode rapide: récupérer les dernières signatures et prendre la plus ancienne
            payload2 = {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "getSignaturesForAddress",
                "params": [
                    token_address,
                    {"limit": 1000}  # Max 1000, on prend la dernière
                ]
            }
            resp2 = await self._client.post(self.rpc_url, json=payload2)
            data2 = resp2.json()
            all_sigs = data2.get("result", [])

            if not all_sigs:
                return

            # La dernière dans la liste = la plus ancienne (première tx du token)
            oldest_sig = all_sigs[-1].get("signature")
            if not oldest_sig:
                return

            # Récupérer la transaction pour trouver le signataire
            payload3 = {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "getTransaction",
                "params": [
                    oldest_sig,
                    {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}
                ]
            }
            resp3 = await self._client.post(self.rpc_url, json=payload3)
            data3 = resp3.json()
            tx = data3.get("result")

            if tx:
                # Le premier signataire = le deployer
                account_keys = tx.get("transaction", {}).get("message", {}).get("accountKeys", [])
                if account_keys:
                    # accountKeys[0] est toujours le fee payer / signataire principal
                    first_key = account_keys[0]
                    if isinstance(first_key, dict):
                        result.deployer_address = first_key.get("pubkey")
                    else:
                        result.deployer_address = first_key

        except Exception as e:
            logger.warning(f"[TokenFilter] Erreur récupération deployer: {e}")

    # ----------------------------------------------------------
    # CHECK 3: LP STATUS (BURNED / LOCKED)
    # ----------------------------------------------------------

    async def _check_lp_status(self, pair_address: str, result: TokenFilterResult):
        """
        Vérifier si la LP est burned ou locked.
        
        Méthode: Récupérer le LP mint du pool, puis vérifier si le supply
        est détenu par une burn address ou un programme de lock.
        """
        try:
            # Récupérer les données du pool pour trouver le LP mint
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getAccountInfo",
                "params": [pair_address, {"encoding": "jsonParsed"}]
            }
            resp = await self._client.post(self.rpc_url, json=payload)
            data = resp.json()
            value = data.get("result", {}).get("value")

            if not value:
                return

            # Pour les pools PumpSwap, la LP est automatiquement burned (100%)
            owner = value.get("owner", "")
            if owner == PUMPSWAP_PROGRAM or "pump" in owner.lower():
                result.lp_burned = True
                return

            # Pour Raydium: on doit trouver le LP mint et vérifier ses holders
            # Le LP mint est dans les données du pool
            account_data = value.get("data", {})
            if isinstance(account_data, list) and len(account_data) >= 1:
                # Données binaires — parser le LP mint
                lp_mint = await self._extract_raydium_lp_mint(pair_address)
                if lp_mint:
                    burned = await self._check_lp_mint_burned(lp_mint)
                    if burned:
                        result.lp_burned = True
                    else:
                        locked = await self._check_lp_mint_locked(lp_mint)
                        if locked:
                            result.lp_locked = True

        except Exception as e:
            logger.warning(f"[TokenFilter] Erreur check LP status: {e}")

    async def _extract_raydium_lp_mint(self, pair_address: str) -> Optional[str]:
        """Extraire le LP mint d'un pool Raydium"""
        try:
            # Raydium AMM V4 pool layout:
            # Le LP mint est à l'offset 400 (32 bytes) dans les données du pool
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getAccountInfo",
                "params": [pair_address, {"encoding": "base64"}]
            }
            resp = await self._client.post(self.rpc_url, json=payload)
            data = resp.json()
            value = data.get("result", {}).get("value")
            if not value or not value.get("data"):
                return None

            import base64 as b64
            raw = b64.b64decode(value["data"][0])

            # Raydium AMM V4: LP mint at offset 400
            if len(raw) > 432:
                lp_mint_bytes = raw[400:432]
                lp_mint = base58.b58encode(lp_mint_bytes).decode()
                return lp_mint

            return None
        except Exception as e:
            logger.warning(f"[TokenFilter] Erreur extraction LP mint: {e}")
            return None

    async def _check_lp_mint_burned(self, lp_mint: str) -> bool:
        """Vérifier si le supply du LP mint est détenu par une burn address"""
        try:
            # Vérifier le supply total et les largest accounts
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTokenLargestAccounts",
                "params": [lp_mint]
            }
            resp = await self._client.post(self.rpc_url, json=payload)
            data = resp.json()
            accounts = data.get("result", {}).get("value", [])

            if not accounts:
                return False

            # Récupérer le total supply
            total_amount = sum(int(a.get("amount", 0)) for a in accounts)
            if total_amount == 0:
                return True  # Supply = 0 → considéré comme burned

            # Vérifier si les plus gros holders sont des burn addresses
            burned_amount = 0
            for account in accounts:
                account_addr = account.get("address", "")
                amount = int(account.get("amount", 0))

                # Vérifier si ce token account appartient à une burn address
                owner = await self._get_token_account_owner(account_addr)
                if owner in BURN_ADDRESSES:
                    burned_amount += amount

            # Si > 95% est burned, considérer comme LP burned
            if total_amount > 0 and burned_amount / total_amount >= 0.95:
                return True

            return False

        except Exception as e:
            logger.warning(f"[TokenFilter] Erreur check LP burned: {e}")
            return False

    async def _check_lp_mint_locked(self, lp_mint: str) -> bool:
        """Vérifier si le LP mint est détenu par un programme de lock"""
        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTokenLargestAccounts",
                "params": [lp_mint]
            }
            resp = await self._client.post(self.rpc_url, json=payload)
            data = resp.json()
            accounts = data.get("result", {}).get("value", [])

            if not accounts:
                return False

            total_amount = sum(int(a.get("amount", 0)) for a in accounts)
            locked_amount = 0

            for account in accounts:
                account_addr = account.get("address", "")
                amount = int(account.get("amount", 0))

                owner = await self._get_token_account_owner(account_addr)
                if owner in LP_LOCK_PROGRAMS:
                    locked_amount += amount

            # Si > 80% est locked, considérer comme LP locked
            if total_amount > 0 and locked_amount / total_amount >= 0.80:
                return True

            return False

        except Exception as e:
            logger.warning(f"[TokenFilter] Erreur check LP locked: {e}")
            return False

    async def _get_token_account_owner(self, token_account_address: str) -> str:
        """Récupérer le owner d'un token account"""
        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getAccountInfo",
                "params": [token_account_address, {"encoding": "jsonParsed"}]
            }
            resp = await self._client.post(self.rpc_url, json=payload)
            data = resp.json()
            value = data.get("result", {}).get("value")
            if not value:
                return ""

            parsed = value.get("data", {}).get("parsed", {})
            info = parsed.get("info", {})
            return info.get("owner", "")

        except Exception:
            return ""

    # ----------------------------------------------------------
    # UTILITAIRES
    # ----------------------------------------------------------

    async def quick_mint_check(self, token_address: str) -> Tuple[bool, str]:
        """
        Check ultra-rapide (1 seul appel RPC): mint + freeze authority.
        Retourne (is_safe, reason).
        Utilisé comme PREMIER filtre avant même RugCheck.
        """
        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getAccountInfo",
                "params": [token_address, {"encoding": "jsonParsed"}]
            }
            resp = await self._client.post(self.rpc_url, json=payload)
            data = resp.json()

            value = data.get("result", {}).get("value")
            if not value:
                return False, "Token introuvable on-chain"

            parsed = value.get("data", {}).get("parsed", {})
            info = parsed.get("info", {})

            mint_auth = info.get("mintAuthority")
            freeze_auth = info.get("freezeAuthority")

            mint_revoked = (mint_auth is None or mint_auth == "")
            freeze_revoked = (freeze_auth is None or freeze_auth == "")

            if not mint_revoked:
                return False, f"❌ Mint Authority ACTIVE ({mint_auth[:12]}...)"
            if not freeze_revoked:
                return False, f"❌ Freeze Authority ACTIVE ({freeze_auth[:12]}...)"

            return True, "✅ Mint OFF + Freeze OFF"

        except Exception as e:
            logger.error(f"[TokenFilter] Erreur quick_mint_check: {e}")
            return False, f"Erreur RPC: {e}"

    def get_deployer(self, token_address: str) -> Optional[str]:
        """Récupérer le deployer depuis le cache"""
        cached = self._cache.get(token_address)
        if cached:
            return cached.deployer_address
        return None

    def clear_cache(self):
        """Vider le cache"""
        self._cache.clear()

    async def close(self):
        """Fermer le client HTTP"""
        await self._client.aclose()
