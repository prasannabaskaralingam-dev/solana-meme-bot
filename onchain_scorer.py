"""
ONCHAIN SCORER — Scoring on-chain direct via Helius RPC.
=========================================================

Remplace RugCheck comme source PRIMAIRE de scoring.
RugCheck devient le FALLBACK (si RPC échoue).

3 vérifications en parallèle (~150ms total):
  1. getMintInfo → mint/freeze révoqué ?
  2. getTokenLargestAccounts → top holder < 20% ?
  3. LP token envoyé à burn address → LP brûlée ?

Score maison 0-100 (plus bas = plus sûr):
  - 0-29  = SAFE ✅
  - 30-59 = MEDIUM ⚠️
  - 60+   = DANGER 🚨

Timeout max: 500ms par appel RPC.
"""

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Optional, List

import httpx

logger = logging.getLogger(__name__)

# ─── Configuration ────────────────────────────────────────────
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "")
HELIUS_RPC = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}" if HELIUS_API_KEY else ""

# Timeout max par appel RPC
RPC_TIMEOUT = 0.5  # 500ms

# Adresses de burn connues
BURN_ADDRESSES = {
    "1nc1nerator11111111111111111111111111111111",
    "11111111111111111111111111111111",
    "1111111111111111111111111111111111111111111",
    "1111111QLbz7JHiBTspS962RLKV8GndWFwiEaqKM",
}

# Pools connues (à exclure du calcul holders)
KNOWN_POOLS = {
    "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1",  # Raydium
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",  # Raydium V4
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",  # Raydium CPMM
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",   # Orca
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",   # Jupiter
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",   # PumpSwap
}

# Seuils
MAX_TOP_HOLDER_PCT = 20.0  # Top holder < 20%


# ─── Result Dataclass ─────────────────────────────────────────

@dataclass
class OnchainScore:
    """Résultat du scoring on-chain"""
    token_address: str
    score: int = 100  # 0-100 (0 = safe)
    safe: bool = False

    # Détails des checks
    mint_revoked: bool = False
    freeze_revoked: bool = False
    lp_burned: bool = False
    holders_ok: bool = False
    top_holder_pct: float = 0.0

    # Métadonnées
    duration_ms: float = 0.0
    source: str = "onchain"  # "onchain" ou "rugcheck_fallback"
    errors: List[str] = field(default_factory=list)

    @property
    def risk_level(self) -> str:
        if self.score < 30:
            return "Good"
        elif self.score < 60:
            return "Medium"
        else:
            return "Danger"

    @property
    def summary(self) -> str:
        parts = []
        parts.append(f"Score:{self.score}")
        parts.append(f"Mint:{'✅' if self.mint_revoked else '❌'}")
        parts.append(f"Freeze:{'✅' if self.freeze_revoked else '❌'}")
        parts.append(f"LP:{'🔥' if self.lp_burned else '⚠️'}")
        parts.append(f"Top:{self.top_holder_pct:.0f}%")
        return " | ".join(parts)


# ─── Onchain Scorer Class ────────────────────────────────────

class OnchainScorer:
    """
    Scoring on-chain direct via Helius RPC.
    Remplace RugCheck comme source primaire.
    ~150ms total (3 appels en parallèle).
    """

    def __init__(self, rpc_url: Optional[str] = None):
        self.rpc_url = rpc_url or HELIUS_RPC or "https://api.mainnet-beta.solana.com"
        self._client = httpx.AsyncClient(timeout=RPC_TIMEOUT)
        # Cache (5 min)
        self._cache: dict[str, OnchainScore] = {}
        self._cache_ts: dict[str, float] = {}
        self._cache_ttl = 300  # 5 minutes

    async def score_token(self, token_address: str, source: str = "") -> OnchainScore:
        """
        Score un token directement on-chain.
        3 vérifications en parallèle, timeout 500ms chacune.
        
        Args:
            token_address: Adresse du token à scorer
            source: Origine de la détection ("pump.fun" = exempte LP de la pénalité)
        
        Returns:
            OnchainScore avec score 0-100 et safe=True si < 30
        """
        # Check cache
        if token_address in self._cache:
            age = time.time() - self._cache_ts.get(token_address, 0)
            if age < self._cache_ttl:
                return self._cache[token_address]

        start = time.time()
        result = OnchainScore(token_address=token_address)

        # Lancer les 3 checks en parallèle
        tasks = [
            self._check_mint_info(token_address, result),
            self._check_top_holders(token_address, result),
            self._check_lp_burned(token_address, result),
        ]

        await asyncio.gather(*tasks, return_exceptions=True)

        # Calculer le score maison (0-100)
        # Pour les tokens pump.fun, la LP n'est JAMAIS burned au lancement
        # (bonding curve interne) → on exempte la pénalité LP
        is_pumpfun = source.lower() in ("pump.fun", "pumpfun", "ws_pumpfun")
        score = 0
        if not result.mint_revoked:
            score += 40
        if not result.lp_burned and not is_pumpfun:
            score += 40  # Pénalité LP uniquement pour les tokens NON pump.fun
        if not result.holders_ok:
            score += 20

        result.score = score
        result.safe = score < 30
        if is_pumpfun:
            result.source = "onchain_pumpfun"  # Traçabilité
        result.duration_ms = (time.time() - start) * 1000

        # Cache
        self._cache[token_address] = result
        self._cache_ts[token_address] = time.time()

        logger.info(
            f"[OnchainScorer] {token_address[:12]}... "
            f"score={score} ({'✅ SAFE' if result.safe else '🚫 DANGER'}) "
            f"({result.duration_ms:.0f}ms) — {result.summary}"
        )

        return result

    # ─── CHECK 1: Mint Authority ──────────────────────────────

    async def _check_mint_info(self, token_address: str, result: OnchainScore):
        """
        getMintInfo → mint/freeze authority révoqué ?
        Si mintAuthority = None → révoqué ✅
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
                result.errors.append("mint_info: token introuvable")
                return

            parsed = value.get("data", {}).get("parsed", {})
            info = parsed.get("info", {})

            mint_auth = info.get("mintAuthority")
            freeze_auth = info.get("freezeAuthority")

            result.mint_revoked = (mint_auth is None or mint_auth == "")
            result.freeze_revoked = (freeze_auth is None or freeze_auth == "")

        except httpx.TimeoutException:
            result.errors.append("mint_info: timeout >500ms")
            logger.warning(f"[OnchainScorer] getMintInfo timeout {token_address[:12]}")
        except Exception as e:
            result.errors.append(f"mint_info: {e}")

    # ─── CHECK 2: Top Holders ─────────────────────────────────

    async def _check_top_holders(self, token_address: str, result: OnchainScore):
        """
        getTokenLargestAccounts → top holder < 20% ?
        Exclut les pools et burn addresses du calcul.
        """
        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "getTokenLargestAccounts",
                "params": [token_address]
            }
            resp = await self._client.post(self.rpc_url, json=payload)
            data = resp.json()

            accounts = data.get("result", {}).get("value", [])
            if not accounts:
                result.errors.append("holders: aucun account trouvé")
                return

            # Calculer le total (tous les accounts)
            total_amount = sum(int(a.get("amount", 0)) for a in accounts)
            if total_amount == 0:
                result.errors.append("holders: supply = 0")
                return

            # Identifier les holders réels (exclure pools et burn)
            real_holders = []
            for account in accounts:
                addr = account.get("address", "")
                amount = int(account.get("amount", 0))

                # Vérifier si c'est un pool ou burn (via owner check rapide)
                owner = await self._get_owner_fast(addr)
                if owner in KNOWN_POOLS or owner in BURN_ADDRESSES or addr in BURN_ADDRESSES:
                    continue

                real_holders.append(amount)

            if not real_holders:
                # Tous les holders sont des pools/burn → token OK
                result.holders_ok = True
                result.top_holder_pct = 0.0
                return

            # Top holder en % du total
            top_amount = max(real_holders)
            result.top_holder_pct = (top_amount / total_amount) * 100
            result.holders_ok = result.top_holder_pct < MAX_TOP_HOLDER_PCT

        except httpx.TimeoutException:
            result.errors.append("holders: timeout >500ms")
            logger.warning(f"[OnchainScorer] getTokenLargestAccounts timeout {token_address[:12]}")
        except Exception as e:
            result.errors.append(f"holders: {e}")

    # ─── CHECK 3: LP Burned ───────────────────────────────────

    async def _check_lp_burned(self, token_address: str, result: OnchainScore):
        """
        Vérifier si le LP token est envoyé à une burn address.
        Méthode: getTokenAccountsByOwner(burn_address, {mint: token})
        """
        try:
            # Vérifier les burn addresses connues
            for burn_addr in list(BURN_ADDRESSES)[:2]:  # Check les 2 principales
                payload = {
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "getTokenAccountsByOwner",
                    "params": [
                        burn_addr,
                        {"mint": token_address},
                        {"encoding": "jsonParsed"}
                    ]
                }
                try:
                    resp = await self._client.post(self.rpc_url, json=payload)
                    data = resp.json()
                    accounts = data.get("result", {}).get("value", [])
                    if accounts:
                        result.lp_burned = True
                        return
                except httpx.TimeoutException:
                    continue

            # Si aucun LP trouvé chez les burn addresses,
            # vérifier si c'est un token PumpSwap (LP auto-burned)
            # PumpSwap tokens ont leur LP burned par design
            payload_check = {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "getSignaturesForAddress",
                "params": [token_address, {"limit": 5}]
            }
            try:
                resp = await self._client.post(self.rpc_url, json=payload_check)
                data = resp.json()
                sigs = data.get("result", [])
                # Si le token a été créé via PumpSwap, la LP est auto-burned
                # Heuristique: vérifier si le programme PumpSwap est dans les logs
                for sig_info in sigs:
                    memo = sig_info.get("memo", "") or ""
                    if "pump" in memo.lower():
                        result.lp_burned = True
                        return
            except httpx.TimeoutException:
                pass

        except httpx.TimeoutException:
            result.errors.append("lp_burned: timeout >500ms")
            logger.warning(f"[OnchainScorer] LP check timeout {token_address[:12]}")
        except Exception as e:
            result.errors.append(f"lp_burned: {e}")

    # ─── Utilitaires ──────────────────────────────────────────

    async def _get_owner_fast(self, token_account: str) -> str:
        """Récupérer le owner d'un token account (timeout 500ms)"""
        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 99,
                "method": "getAccountInfo",
                "params": [token_account, {"encoding": "jsonParsed"}]
            }
            resp = await self._client.post(self.rpc_url, json=payload)
            data = resp.json()
            value = data.get("result", {}).get("value")
            if not value:
                return ""
            parsed = value.get("data", {}).get("parsed", {})
            return parsed.get("info", {}).get("owner", "")
        except Exception:
            return ""

    def clear_cache(self):
        """Vider le cache"""
        self._cache.clear()
        self._cache_ts.clear()

    async def close(self):
        """Fermer le client HTTP"""
        await self._client.aclose()
