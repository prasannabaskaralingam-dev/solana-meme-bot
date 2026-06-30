"""
PRICE RESOLVER — Bascule automatique de source de prix.

Pour les tokens encore sur bonding curve (pump.fun) :
  → Lecture directe RPC Helius (get_bonding_curve_data) — latence ~150ms

Pour les tokens migrés sur Raydium :
  → DexScreener API (fallback) — latence ~500-3000ms

Utilisé par :
  - autonomous_guardian.py (_sniper_check)
  - trading_bot.py (sniper_monitor_job)

Robustesse (analyse Inversion) :
  - Double échec : alerte CRITICAL + compteur + fallback dernier prix connu
  - État intermédiaire : essaie DexScreener même si pas connu comme migré
  - TTL cache : prix caché utilisable comme fallback (max 10s)
  - Changement d'état : cache invalidé dès détection migration via RPC
"""

import os
import time
import logging
import asyncio
from typing import Optional, Tuple
from dataclasses import dataclass

from bonding_curve import get_bonding_curve_data

logger = logging.getLogger(__name__)

# ─── Cache d'état BC ─────────────────────────────────────────────────────────
# Format: {token_address: {"complete": bool, "last_check": float, "price_usd": float, "price_ts": float}}
_bc_state_cache: dict = {}
BC_CACHE_TTL = 30  # Re-vérifier l'état BC toutes les 30s (migration est rare)
PRICE_CACHE_TTL = 10  # Prix caché valide max 10s (pour fallback double échec)

# ─── Compteur d'échecs consécutifs ────────────────────────────────────────────
# Format: {token_address: {"count": int, "first_failure": float}}
_failure_tracker: dict = {}
FAILURE_CRITICAL_THRESHOLD = 5  # Alerte CRITICAL après 5 échecs consécutifs (15s)


@dataclass
class PriceResult:
    """Résultat de la résolution de prix."""
    price_usd: float
    source: str  # "bonding_curve_rpc", "dexscreener", "helius_ws_cache", "cached_fallback", "last_known_fallback"
    latency_ms: float
    is_complete: bool  # True = token migré vers Raydium
    emergency_sell: bool = False  # True = 5 échecs consécutifs, vente d'urgence requise


async def get_realtime_price(
    token_address: str,
    helius_api_key: str,
    sol_price_usd: float = 150.0,
    helius_ws=None,
    dexscreener_api=None,
) -> PriceResult:
    """
    Résout le prix d'un token en basculant automatiquement entre :
    1. RPC Helius (bonding curve) — si token encore sur BC
    2. DexScreener API — si token migré ou BC indisponible
    3. Cache prix récent — si double échec (max 10s)
    4. Fallback prix=0 — si tout échoue (le caller gère)

    Args:
        token_address: Adresse du token
        helius_api_key: Clé API Helius
        sol_price_usd: Prix SOL actuel en USD
        helius_ws: Instance HeliusWebSocket (pour le cache, optionnel)
        dexscreener_api: Instance DexScreenerAPI (optionnel)

    Returns:
        PriceResult avec prix, source, latence et état migration
    """
    start = time.time()

    # ─── Étape 1: Vérifier le cache d'état BC ───
    cached = _bc_state_cache.get(token_address)
    is_known_migrated = False
    if cached and (time.time() - cached["last_check"]) < BC_CACHE_TTL:
        is_known_migrated = cached["complete"]

    # ─── Étape 2: Si pas connu comme migré → essayer RPC Helius (bonding curve) ───
    if not is_known_migrated and helius_api_key:
        try:
            bc_data = await get_bonding_curve_data(
                token_address=token_address,
                helius_api_key=helius_api_key,
                sol_price_usd=sol_price_usd,
            )
            if bc_data is not None:
                latency = (time.time() - start) * 1000
                # Mettre à jour le cache
                _bc_state_cache[token_address] = {
                    "complete": bc_data.complete,
                    "last_check": time.time(),
                    "price_usd": bc_data.price_usd,
                    "price_ts": time.time(),
                }
                if not bc_data.complete and bc_data.price_usd > 0:
                    # Token encore sur bonding curve — prix RPC direct
                    _reset_failure_tracker(token_address)
                    return PriceResult(
                        price_usd=bc_data.price_usd,
                        source="bonding_curve_rpc",
                        latency_ms=latency,
                        is_complete=False,
                    )
                else:
                    # Token migré — on le sait maintenant, bascule DexScreener
                    is_known_migrated = True
                    logger.info(
                        f"[PriceResolver] {token_address[:8]}... migré Raydium "
                        f"(détecté via RPC, bascule DexScreener)"
                    )
        except Exception as e:
            logger.debug(f"[PriceResolver] BC RPC error pour {token_address[:8]}: {e}")

    # ─── Étape 3: HeliusWS cache (temps réel si alimenté) ───
    if helius_ws:
        try:
            if hasattr(helius_ws, '_price_cache') and helius_ws.is_connected:
                cached_price = helius_ws._price_cache.get(token_address)
                if cached_price and time.time() - cached_price["ts"] < 30:
                    latency = (time.time() - start) * 1000
                    _reset_failure_tracker(token_address)
                    return PriceResult(
                        price_usd=cached_price["price"],
                        source="helius_ws_cache",
                        latency_ms=latency,
                        is_complete=is_known_migrated,
                    )
        except Exception:
            pass

    # ─── Étape 4: DexScreener API (fonctionne pour BC ET migrés) ───
    if dexscreener_api:
        try:
            analysis = dexscreener_api.analyze_token(token_address)
            if analysis:
                price = float(analysis.get("price_usd", 0) or 0)
                if price > 0:
                    latency = (time.time() - start) * 1000
                    # Mettre à jour le cache
                    _bc_state_cache[token_address] = {
                        "complete": is_known_migrated or True,
                        "last_check": time.time(),
                        "price_usd": price,
                        "price_ts": time.time(),
                    }
                    _reset_failure_tracker(token_address)
                    return PriceResult(
                        price_usd=price,
                        source="dexscreener",
                        latency_ms=latency,
                        is_complete=is_known_migrated,
                    )
        except Exception as e:
            logger.debug(f"[PriceResolver] DexScreener error pour {token_address[:8]}: {e}")

    # ─── Étape 5: DexScreener direct via HeliusWS helper ───
    if helius_ws:
        try:
            if hasattr(helius_ws, '_get_price_dexscreener'):
                price = helius_ws._get_price_dexscreener(token_address)
                if price and price > 0:
                    latency = (time.time() - start) * 1000
                    _bc_state_cache[token_address] = {
                        "complete": is_known_migrated,
                        "last_check": time.time(),
                        "price_usd": price,
                        "price_ts": time.time(),
                    }
                    _reset_failure_tracker(token_address)
                    return PriceResult(
                        price_usd=price,
                        source="dexscreener",
                        latency_ms=latency,
                        is_complete=is_known_migrated,
                    )
        except Exception:
            pass

    # ─── Étape 6: DOUBLE ÉCHEC — Fallback cache prix récent (max 10s) ───
    should_emergency_sell = _record_failure(token_address)
    latency = (time.time() - start) * 1000

    if cached and cached.get("price_usd", 0) > 0:
        cache_age = time.time() - cached.get("price_ts", 0)
        if cache_age < PRICE_CACHE_TTL:
            logger.warning(
                f"[PriceResolver] ⚠️ DOUBLE ÉCHEC {token_address[:8]}... — "
                f"utilise cache prix (âge={cache_age:.1f}s, prix=${cached['price_usd']:.8f})"
            )
            return PriceResult(
                price_usd=cached["price_usd"],
                source="cached_fallback",
                latency_ms=latency,
                is_complete=is_known_migrated,
                emergency_sell=should_emergency_sell,
            )

    # ─── Étape 7: ÉCHEC TOTAL — prix indisponible ───
    logger.warning(
        f"[PriceResolver] ⚠️ ÉCHEC TOTAL {token_address[:8]}... — "
        f"aucune source de prix disponible (RPC+DexScreener down)"
    )
    return PriceResult(
        price_usd=0.0,
        source="fallback",
        latency_ms=latency,
        is_complete=is_known_migrated,
        emergency_sell=should_emergency_sell,
    )


def _record_failure(token_address: str) -> bool:
    """Enregistre un échec. Retourne True si seuil CRITICAL atteint (vente d'urgence requise)."""
    now = time.time()
    tracker = _failure_tracker.get(token_address)
    if tracker is None:
        _failure_tracker[token_address] = {"count": 1, "first_failure": now}
        return False
    else:
        tracker["count"] += 1
        if tracker["count"] >= FAILURE_CRITICAL_THRESHOLD:
            duration = now - tracker["first_failure"]
            logger.critical(
                f"[PriceResolver] 🚨 ALERTE CRITIQUE: {token_address[:8]}... — "
                f"{tracker['count']} échecs consécutifs en {duration:.0f}s — "
                f"AUCUNE source de prix ne répond! VENTE D'URGENCE REQUISE."
            )
            # Reset après alerte pour ne pas spammer
            tracker["count"] = 0
            tracker["first_failure"] = now
            return True
        return False


def _reset_failure_tracker(token_address: str):
    """Reset le compteur d'échecs après un succès."""
    _failure_tracker.pop(token_address, None)


def invalidate_cache(token_address: str):
    """Invalider le cache pour un token (ex: après vente)."""
    _bc_state_cache.pop(token_address, None)
    _failure_tracker.pop(token_address, None)


def get_cache_stats() -> dict:
    """Stats du cache pour monitoring."""
    now = time.time()
    total = len(_bc_state_cache)
    bc_count = sum(1 for v in _bc_state_cache.values() if not v["complete"])
    migrated_count = sum(1 for v in _bc_state_cache.values() if v["complete"])
    stale = sum(1 for v in _bc_state_cache.values() if now - v["last_check"] > BC_CACHE_TTL)
    failures = sum(1 for v in _failure_tracker.values() if v["count"] > 0)
    return {
        "total_cached": total,
        "bonding_curve": bc_count,
        "migrated": migrated_count,
        "stale": stale,
        "active_failures": failures,
    }
