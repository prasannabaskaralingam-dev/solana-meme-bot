"""
BONDING CURVE MODULE — Lecture directe de la bonding curve pump.fun via RPC Helius.

Fonctions :
- get_bonding_curve_data(token_address, helius_api_key) → données de la bonding curve
- is_safe_bonding_curve(token_address, helius_api_key) → vérification sécurité (mint/freeze)

Utilise getAccountInfo RPC pour lire le PDA de la bonding curve pump.fun.
Latence : ~100-200ms (1 appel RPC).
"""

import struct
import logging
import time
import aiohttp
import asyncio
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# ─── Constantes pump.fun ─────────────────────────────────────────────────────
PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMP_CURVE_SEED = b"bonding-curve"

# Discriminator = sha256("account:BondingCurve")[:8]
PUMP_CURVE_STATE_SIGNATURE = bytes([0x17, 0xB7, 0xF8, 0x37, 0x60, 0xD8, 0xAC, 0x60])

# Offsets dans le buffer du compte bonding curve (après le discriminator de 8 bytes)
# Layout: discriminator(8) + virtualTokenReserves(8) + virtualSolReserves(8) +
#          realTokenReserves(8) + realSolReserves(8) + tokenTotalSupply(8) + complete(1)
OFFSET_VIRTUAL_TOKEN_RESERVES = 8
OFFSET_VIRTUAL_SOL_RESERVES = 16
OFFSET_REAL_TOKEN_RESERVES = 24
OFFSET_REAL_SOL_RESERVES = 32
OFFSET_TOKEN_TOTAL_SUPPLY = 40
OFFSET_COMPLETE = 48

# Pump.fun token decimals
TOKEN_DECIMALS = 6
LAMPORTS_PER_SOL = 1_000_000_000

# Supply totale fixe pump.fun = 1 milliard tokens
TOTAL_SUPPLY = 1_000_000_000

# Token Program ID
TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"


@dataclass
class BondingCurveData:
    """Données de la bonding curve pump.fun"""
    token_address: str
    virtual_token_reserves: int  # en unités brutes (avec decimals)
    virtual_sol_reserves: int    # en lamports
    real_token_reserves: int
    real_sol_reserves: int
    token_total_supply: int
    complete: bool               # True = migré vers Raydium
    price_sol: float             # prix en SOL par token
    price_usd: float             # prix en USD (estimé avec SOL price)
    market_cap_usd: float        # MC estimé en USD
    liquidity_sol: float         # réserve SOL réelle
    bonding_progress_pct: float  # % de progression de la bonding curve
    fetch_latency_ms: float      # latence de l'appel RPC


@dataclass
class SafetyCheck:
    """Résultat du check de sécurité"""
    is_safe: bool
    mint_revoked: bool
    freeze_revoked: bool
    score: int  # 0 = safe, 20 = mint non revoked, 40 = freeze non revoked


def _derive_bonding_curve_address(token_mint: str) -> str:
    """
    Dérive l'adresse PDA de la bonding curve à partir du token mint.
    PDA = findProgramAddress(["bonding-curve", mint_pubkey], PUMP_PROGRAM_ID)
    """
    from solders.pubkey import Pubkey

    program_id = Pubkey.from_string(PUMP_PROGRAM_ID)
    mint_pubkey = Pubkey.from_string(token_mint)

    # Derive PDA
    pda, _bump = Pubkey.find_program_address(
        [PUMP_CURVE_SEED, bytes(mint_pubkey)],
        program_id
    )
    return str(pda)


def _parse_bonding_curve_data(data: bytes, token_address: str, sol_price_usd: float, latency_ms: float) -> Optional[BondingCurveData]:
    """Parse les données brutes du compte bonding curve."""
    if len(data) < 49:  # 8 + 8*5 + 1
        logger.warning(f"[BC] Données trop courtes: {len(data)} bytes")
        return None

    # Vérifier le discriminator
    discriminator = data[:8]
    if discriminator != PUMP_CURVE_STATE_SIGNATURE:
        logger.warning(f"[BC] Discriminator invalide pour {token_address[:8]}...")
        return None

    # Lire les champs (little-endian uint64)
    virtual_token_reserves = struct.unpack_from('<Q', data, OFFSET_VIRTUAL_TOKEN_RESERVES)[0]
    virtual_sol_reserves = struct.unpack_from('<Q', data, OFFSET_VIRTUAL_SOL_RESERVES)[0]
    real_token_reserves = struct.unpack_from('<Q', data, OFFSET_REAL_TOKEN_RESERVES)[0]
    real_sol_reserves = struct.unpack_from('<Q', data, OFFSET_REAL_SOL_RESERVES)[0]
    token_total_supply = struct.unpack_from('<Q', data, OFFSET_TOKEN_TOTAL_SUPPLY)[0]
    complete = data[OFFSET_COMPLETE] != 0

    # Calculer le prix en SOL
    # prix = (virtual_sol_reserves / LAMPORTS_PER_SOL) / (virtual_token_reserves / 10^TOKEN_DECIMALS)
    if virtual_token_reserves == 0:
        return None

    price_sol = (virtual_sol_reserves / LAMPORTS_PER_SOL) / (virtual_token_reserves / (10 ** TOKEN_DECIMALS))
    price_usd = price_sol * sol_price_usd

    # MC = prix × supply totale
    market_cap_usd = price_usd * TOTAL_SUPPLY

    # Liquidité SOL réelle
    liquidity_sol = real_sol_reserves / LAMPORTS_PER_SOL

    # Progression bonding curve
    # Initial real_token_reserves = 793_100_000_000_000 (793.1M tokens avec 6 decimals)
    INITIAL_REAL_TOKEN_RESERVES = 793_100_000_000_000
    if real_token_reserves >= INITIAL_REAL_TOKEN_RESERVES:
        bonding_progress_pct = 0.0
    else:
        bonding_progress_pct = (1 - (real_token_reserves / INITIAL_REAL_TOKEN_RESERVES)) * 100

    return BondingCurveData(
        token_address=token_address,
        virtual_token_reserves=virtual_token_reserves,
        virtual_sol_reserves=virtual_sol_reserves,
        real_token_reserves=real_token_reserves,
        real_sol_reserves=real_sol_reserves,
        token_total_supply=token_total_supply,
        complete=complete,
        price_sol=price_sol,
        price_usd=price_usd,
        market_cap_usd=market_cap_usd,
        liquidity_sol=liquidity_sol,
        bonding_progress_pct=bonding_progress_pct,
        fetch_latency_ms=latency_ms,
    )


async def get_bonding_curve_data(
    token_address: str,
    helius_api_key: str,
    sol_price_usd: float = 150.0,
) -> Optional[BondingCurveData]:
    """
    Récupère les données de la bonding curve pump.fun via RPC Helius.

    Args:
        token_address: Adresse du token mint
        helius_api_key: Clé API Helius
        sol_price_usd: Prix actuel de SOL en USD (pour estimer le MC)

    Returns:
        BondingCurveData ou None si erreur/token non pump.fun
    """
    start = time.time()

    try:
        # Dériver l'adresse de la bonding curve
        curve_address = _derive_bonding_curve_address(token_address)

        # Appel RPC getAccountInfo
        url = f"https://mainnet.helius-rpc.com/?api-key={helius_api_key}"
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [
                curve_address,
                {"encoding": "base64", "commitment": "confirmed"}
            ]
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    logger.warning(f"[BC] RPC HTTP {resp.status} pour {token_address[:8]}...")
                    return None

                result = await resp.json()

        latency_ms = (time.time() - start) * 1000

        # Vérifier la réponse
        if "error" in result:
            logger.warning(f"[BC] RPC error: {result['error']}")
            return None

        account_info = result.get("result", {}).get("value")
        if account_info is None:
            # Compte n'existe pas → pas un token pump.fun ou pas encore créé
            logger.debug(f"[BC] Bonding curve non trouvée pour {token_address[:8]}...")
            return None

        # Décoder les données base64
        import base64
        data_b64 = account_info.get("data", [None])[0]
        if not data_b64:
            return None

        data = base64.b64decode(data_b64)

        # Parser les données
        return _parse_bonding_curve_data(data, token_address, sol_price_usd, latency_ms)

    except asyncio.TimeoutError:
        logger.error(f"[BC] ⏱️ TIMEOUT 10s RPC pour {token_address[:8]}...")
        return None
    except Exception as e:
        logger.error(f"[BC] Erreur get_bonding_curve_data({token_address[:8]}...): {e}")
        return None


async def is_safe_bonding_curve(
    token_address: str,
    helius_api_key: str,
) -> SafetyCheck:
    """
    Vérifie la sécurité du token via RPC :
    - Mint Authority révoquée
    - Freeze Authority révoquée

    Args:
        token_address: Adresse du token mint
        helius_api_key: Clé API Helius

    Returns:
        SafetyCheck avec is_safe, mint_revoked, freeze_revoked, score
    """
    try:
        url = f"https://mainnet.helius-rpc.com/?api-key={helius_api_key}"
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [
                token_address,
                {"encoding": "jsonParsed", "commitment": "confirmed"}
            ]
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return SafetyCheck(is_safe=False, mint_revoked=False, freeze_revoked=False, score=100)

                result = await resp.json()

        account_info = result.get("result", {}).get("value")
        if account_info is None:
            return SafetyCheck(is_safe=False, mint_revoked=False, freeze_revoked=False, score=100)

        # Parse le mint account (jsonParsed)
        parsed = account_info.get("data", {}).get("parsed", {})
        info = parsed.get("info", {})

        mint_authority = info.get("mintAuthority")
        freeze_authority = info.get("freezeAuthority")

        mint_revoked = mint_authority is None
        freeze_revoked = freeze_authority is None

        # Score : 0 = parfait, +20 par problème
        score = 0
        if not mint_revoked:
            score += 20
        if not freeze_revoked:
            score += 20

        is_safe = score == 0  # Safe si mint ET freeze révoqués

        return SafetyCheck(
            is_safe=is_safe,
            mint_revoked=mint_revoked,
            freeze_revoked=freeze_revoked,
            score=score,
        )

    except asyncio.TimeoutError:
        logger.error(f"[BC] ⏱️ TIMEOUT 10s RPC safety pour {token_address[:8]}...")
        return SafetyCheck(is_safe=False, mint_revoked=False, freeze_revoked=False, score=100)
    except Exception as e:
        logger.error(f"[BC] Erreur is_safe_bonding_curve({token_address[:8]}...): {e}")
        return SafetyCheck(is_safe=False, mint_revoked=False, freeze_revoked=False, score=100)


async def get_sol_price_usd() -> float:
    """Récupère le prix actuel de SOL en USD via CoinGecko (fallback: $150)."""
    try:
        url = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=3)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("solana", {}).get("usd", 150.0)
    except Exception:
        pass
    return 150.0
