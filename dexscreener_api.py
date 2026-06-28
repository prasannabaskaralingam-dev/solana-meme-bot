"""
Module d'interaction avec l'API DexScreener (100% gratuit, sans clé API)
"""

import time
import logging
import requests
from typing import Optional
from collections import deque
from config import DEXSCREENER_BASE_URL, ENDPOINTS, CHAIN_ID, FILTERS

logger = logging.getLogger(__name__)


class DexScreenerAPI:
    """Client pour l'API DexScreener (gratuit, rate limit 60 req/min)"""

    def __init__(self):
        self.base_url = DEXSCREENER_BASE_URL
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "SolanaMemeBot/1.0"
        })
        self.last_request_time = 0
        self.min_interval = 1.0  # 1 seconde entre les requêtes (safe)

        # ─── Monitoring Gate 1 ───────────────────────────────────────
        self._gate1_calls: deque = deque()       # timestamps de tous les appels analyze_token
        self._gate1_successes: deque = deque()   # timestamps des succès
        self._gate1_failures: deque = deque()    # timestamps des échecs
        self._rate_limit_paused_until: float = 0.0  # timestamp fin de pause rate limit
        self._last_rate_limit_alert: float = 0.0    # cooldown alerte rate limit
        self._last_degraded_alert: float = 0.0      # cooldown alerte dégradé

    def _rate_limit(self):
        """Respecter le rate limit de l'API"""
        # Si en pause rate limit, attendre
        now = time.time()
        if now < self._rate_limit_paused_until:
            wait = self._rate_limit_paused_until - now
            logger.warning(f"[DexScreener] Rate limit pause active, attente {wait:.0f}s")
            time.sleep(wait)

        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request_time = time.time()

    def _cleanup_old_entries(self, window_seconds: float = 3600.0):
        """Nettoyer les entrées de plus d'une heure dans les compteurs"""
        cutoff = time.time() - window_seconds
        while self._gate1_calls and self._gate1_calls[0] < cutoff:
            self._gate1_calls.popleft()
        while self._gate1_successes and self._gate1_successes[0] < cutoff:
            self._gate1_successes.popleft()
        while self._gate1_failures and self._gate1_failures[0] < cutoff:
            self._gate1_failures.popleft()

    def get_gate1_stats(self) -> dict:
        """Retourner les statistiques Gate 1 (dernière heure)"""
        self._cleanup_old_entries()
        total = len(self._gate1_calls)
        successes = len(self._gate1_successes)
        failures = len(self._gate1_failures)
        return {
            "total": total,
            "successes": successes,
            "failures": failures,
            "success_rate": (successes / total * 100) if total > 0 else 100.0,
        }

    def get_gate1_stats_window(self, window_seconds: float = 600.0) -> dict:
        """Retourner les statistiques Gate 1 sur une fenêtre glissante (défaut 10 min)"""
        cutoff = time.time() - window_seconds
        total = sum(1 for t in self._gate1_calls if t >= cutoff)
        failures = sum(1 for t in self._gate1_failures if t >= cutoff)
        successes = total - failures
        return {
            "total": total,
            "successes": successes,
            "failures": failures,
            "failure_rate": (failures / total * 100) if total > 0 else 0.0,
        }

    def _get(self, endpoint: str, params: Optional[dict] = None) -> tuple:
        """
        Effectuer une requête GET avec gestion d'erreurs.
        Retourne (data, status_code) pour permettre la détection de rate limit.
        """
        self._rate_limit()
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.get(url, params=params, timeout=10)

            # Détection rate limit (429)
            if response.status_code == 429:
                logger.error(
                    f"[DexScreener] 🚨 RATE LIMIT 429 reçu ! "
                    f"Headers: {dict(response.headers)}"
                )
                # Activer la pause automatique de 60s
                self._rate_limit_paused_until = time.time() + 60.0
                return None, 429

            # Autres erreurs HTTP
            if response.status_code != 200:
                logger.warning(
                    f"[DexScreener] HTTP {response.status_code} pour {endpoint} "
                    f"Headers: {dict(response.headers)}"
                )
                return None, response.status_code

            return response.json(), 200

        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur API DexScreener: {e}")
            return None, 0

    def _get_simple(self, endpoint: str, params: Optional[dict] = None) -> Optional[dict]:
        """Wrapper simple pour les appels non-analyze_token (compatibilité)"""
        data, _status = self._get(endpoint, params)
        return data

    def get_latest_token_profiles(self) -> list:
        """Récupérer les derniers profils de tokens (nouveaux listings)"""
        data = self._get_simple(ENDPOINTS["token_profiles_latest"])
        if not data:
            return []
        # Filtrer uniquement Solana
        return [t for t in data if t.get("chainId") == CHAIN_ID]

    def get_boosted_tokens(self) -> list:
        """Récupérer les tokens les plus boostés (trending)"""
        data = self._get_simple(ENDPOINTS["token_boosts_top"])
        if not data:
            return []
        return [t for t in data if t.get("chainId") == CHAIN_ID]

    def get_latest_boosts(self) -> list:
        """Récupérer les derniers tokens boostés"""
        data = self._get_simple(ENDPOINTS["token_boosts_latest"])
        if not data:
            return []
        return [t for t in data if t.get("chainId") == CHAIN_ID]

    def get_trending_metas(self) -> list:
        """Récupérer les narratives/metas trending"""
        data = self._get_simple(ENDPOINTS["trending_metas"])
        if not data:
            return []
        return data

    def search_pairs(self, query: str) -> list:
        """Rechercher des paires par nom/symbole"""
        data = self._get_simple(ENDPOINTS["search"], params={"q": query})
        if not data or "pairs" not in data:
            return []
        # Filtrer Solana uniquement
        return [p for p in data["pairs"] if p.get("chainId") == CHAIN_ID]

    def get_token_info(self, token_addresses: list) -> list:
        """Récupérer les infos détaillées de tokens (max 30 adresses)"""
        if not token_addresses:
            return []
        addresses = ",".join(token_addresses[:30])
        endpoint = f"{ENDPOINTS['tokens']}/{addresses}"
        data = self._get_simple(endpoint)
        if not data:
            return []
        return data if isinstance(data, list) else data.get("pairs", [])

    def get_pair_info(self, pair_address: str) -> Optional[dict]:
        """Récupérer les infos d'une paire spécifique"""
        endpoint = f"{ENDPOINTS['pairs']}/{pair_address}"
        data = self._get_simple(endpoint)
        if not data or "pairs" not in data:
            return None
        pairs = data["pairs"]
        return pairs[0] if pairs else None

    def find_new_meme_coins(self) -> list:
        """
        Trouver les nouveaux meme coins prometteurs sur Solana.
        Combine plusieurs sources pour un résultat complet.
        """
        results = []
        seen_addresses = set()

        # 1. Tokens récemment listés sur DexScreener
        profiles = self.get_latest_token_profiles()
        for profile in profiles:
            addr = profile.get("tokenAddress", "")
            if addr and addr not in seen_addresses:
                seen_addresses.add(addr)
                results.append({
                    "address": addr,
                    "source": "new_listing",
                    "url": profile.get("url", ""),
                    "description": profile.get("description", ""),
                    "icon": profile.get("icon", ""),
                })

        # 2. Tokens boostés (payés pour être mis en avant = activité)
        boosts = self.get_boosted_tokens()
        for boost in boosts:
            addr = boost.get("tokenAddress", "")
            if addr and addr not in seen_addresses:
                seen_addresses.add(addr)
                results.append({
                    "address": addr,
                    "source": "boosted",
                    "url": boost.get("url", ""),
                    "description": boost.get("description", ""),
                    "icon": boost.get("icon", ""),
                    "boost_amount": boost.get("amount", 0),
                })

        return results

    def analyze_token(self, token_address: str) -> Optional[dict]:
        """
        Analyser un token en détail : prix, volume, liquidité, etc.
        Avec monitoring Gate 1 : compteurs succès/échec et détection rate limit.
        """
        now = time.time()
        self._gate1_calls.append(now)

        # Appel API avec status code
        pairs_data, status_code = self._get(
            f"{ENDPOINTS['tokens']}/{token_address}"
        )

        # Traitement de l'échec
        if not pairs_data:
            self._gate1_failures.append(now)
            if status_code == 429:
                logger.error(
                    f"[Gate1] ❌ RATE LIMIT 429 pour {token_address[:8]}... "
                    f"Pause 60s activée"
                )
            elif status_code > 0:
                logger.info(
                    f"[Gate1] ❌ HTTP {status_code} pour {token_address[:8]}... "
                    f"(token non indexé ou erreur)"
                )
            else:
                logger.info(
                    f"[Gate1] ❌ Timeout/Erreur réseau pour {token_address[:8]}..."
                )
            return None

        pairs = pairs_data if isinstance(pairs_data, list) else pairs_data.get("pairs", [])
        if not pairs:
            self._gate1_failures.append(now)
            logger.info(f"[Gate1] ❌ Aucune paire trouvée pour {token_address[:8]}...")
            return None

        # Succès
        self._gate1_successes.append(now)

        # Prendre la paire avec le plus de liquidité
        best_pair = max(pairs, key=lambda p: (p.get("liquidity") or {}).get("usd", 0))

        liquidity = (best_pair.get("liquidity") or {}).get("usd", 0)
        volume_24h = (best_pair.get("volume") or {}).get("h24", 0)
        market_cap = best_pair.get("marketCap", 0) or best_pair.get("fdv", 0)
        price_change_5m = (best_pair.get("priceChange") or {}).get("m5", 0)
        price_change_1h = (best_pair.get("priceChange") or {}).get("h1", 0)
        price_change_24h = (best_pair.get("priceChange") or {}).get("h24", 0)

        txns = best_pair.get("txns") or {}
        buys_5m = (txns.get("m5") or {}).get("buys", 0)
        sells_5m = (txns.get("m5") or {}).get("sells", 0)
        buys_1h = (txns.get("h1") or {}).get("buys", 0)
        sells_1h = (txns.get("h1") or {}).get("sells", 0)

        created_at = best_pair.get("pairCreatedAt")
        age_hours = None
        if created_at:
            age_hours = (time.time() * 1000 - created_at) / (1000 * 3600)

        return {
            "address": token_address,
            "name": (best_pair.get("baseToken") or {}).get("name", "Unknown"),
            "symbol": (best_pair.get("baseToken") or {}).get("symbol", "???"),
            "price_usd": best_pair.get("priceUsd", "0"),
            "liquidity_usd": liquidity,
            "volume_24h": volume_24h,
            "market_cap": market_cap,
            "price_change_5m": price_change_5m,
            "price_change_1h": price_change_1h,
            "price_change_24h": price_change_24h,
            "buys_5m": buys_5m,
            "sells_5m": sells_5m,
            "buys_1h": buys_1h,
            "sells_1h": sells_1h,
            "buy_sell_ratio_5m": buys_5m / max(sells_5m, 1),
            "age_hours": age_hours,
            "dex": best_pair.get("dexId", "unknown"),
            "pair_address": best_pair.get("pairAddress", ""),
            "url": best_pair.get("url", ""),
            "dexscreener_url": f"https://dexscreener.com/solana/{best_pair.get('pairAddress', '')}",
        }

    def is_potential_gem(self, analysis: dict) -> tuple:
        """
        Évaluer si un token est un potentiel gem basé sur les filtres améliorés.
        Scoring pondéré pour privilégier les tokens avec momentum réel.
        Retourne (bool, list_of_reasons)
        """
        if not analysis:
            return False, []

        reasons = []
        score = 0

        # === FILTRES ELIMINATOIRES ===

        # Liquidité minimum (augmentée pour éviter les scams)
        if analysis["liquidity_usd"] < FILTERS["min_liquidity_usd"]:
            return False, ["Liquidité trop faible"]

        # Market cap
        mc = analysis["market_cap"]
        if mc and mc < FILTERS["min_market_cap"]:
            return False, ["Market cap trop faible"]
        if mc and mc > FILTERS["max_market_cap"]:
            return False, ["Market cap trop élevé"]

        # NOUVEAU: Rejeter si le prix baisse sur 5min (on ne catch pas un couteau)
        if analysis["price_change_5m"] < -10:
            return False, ["Token en chute libre (-10% sur 5min)"]

        # NOUVEAU: Rejeter si ratio sell > buy (distribution en cours)
        if analysis["buy_sell_ratio_5m"] < 0.5:
            return False, ["Plus de vendeurs que d'acheteurs"]

        # === SCORING PONDÉRÉ ===

        # Pump sur 5 min (signal fort si modéré, danger si trop fort)
        pump_5m = analysis["price_change_5m"]
        if 5 <= pump_5m <= 50:
            reasons.append(f"🚀 Pump sain +{pump_5m:.1f}% en 5min")
            score += 3
        elif pump_5m > 50:
            reasons.append(f"⚠️ Pump extrême +{pump_5m:.1f}% (risque de retrace)")
            score += 1  # Moins de points car risque de retrace

        # Pump sur 1h
        pump_1h = analysis["price_change_1h"]
        if 20 <= pump_1h <= 200:
            reasons.append(f"📈 Hausse +{pump_1h:.1f}% en 1h")
            score += 2

        # Volume élevé (signal de liquidité réelle)
        if analysis["volume_24h"] >= FILTERS["min_volume_24h"]:
            reasons.append(f"💰 Volume 24h: ${analysis['volume_24h']:,.0f}")
            score += 1
        if analysis["volume_24h"] >= 50_000:
            score += 1  # Bonus pour gros volume

        # Beaucoup d'achats (signal d'intérêt réel)
        buys = analysis["buys_5m"]
        if buys >= FILTERS["min_buys_5m"]:
            reasons.append(f"🛒 {buys} achats en 5min")
            score += 1
        if buys >= 50:
            reasons.append(f"🔥 {buys} achats en 5min (FOMO)")
            score += 2  # Beaucoup d'acheteurs = signal fort

        # Ratio buy/sell favorable (signal clé)
        ratio = analysis["buy_sell_ratio_5m"]
        if ratio >= 3:
            reasons.append(f"⚡ Ratio achat/vente: {ratio:.1f}x (très bullish)")
            score += 3
        elif ratio >= 1.5:
            reasons.append(f"⚡ Ratio achat/vente: {ratio:.1f}x")
            score += 1

        # Token récent (les nouveaux tokens ont plus de potentiel)
        age = analysis["age_hours"]
        if age and age <= 1:
            reasons.append(f"🆕 Créé il y a {age:.1f}h (très frais)")
            score += 3
        elif age and age <= 6:
            reasons.append(f"🆕 Créé il y a {age:.1f}h")
            score += 1

        # Seuil: score minimum de 4 ET au moins 2 raisons
        is_gem = score >= 4 and len(reasons) >= 2
        return is_gem, reasons
