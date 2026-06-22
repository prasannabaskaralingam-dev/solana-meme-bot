"""
Module d'interaction avec l'API DexScreener (100% gratuit, sans clé API)
"""

import time
import logging
import requests
from typing import Optional
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

    def _rate_limit(self):
        """Respecter le rate limit de l'API"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request_time = time.time()

    def _get(self, endpoint: str, params: Optional[dict] = None) -> Optional[dict]:
        """Effectuer une requête GET avec gestion d'erreurs"""
        self._rate_limit()
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur API DexScreener: {e}")
            return None

    def get_latest_token_profiles(self) -> list:
        """Récupérer les derniers profils de tokens (nouveaux listings)"""
        data = self._get(ENDPOINTS["token_profiles_latest"])
        if not data:
            return []
        # Filtrer uniquement Solana
        return [t for t in data if t.get("chainId") == CHAIN_ID]

    def get_boosted_tokens(self) -> list:
        """Récupérer les tokens les plus boostés (trending)"""
        data = self._get(ENDPOINTS["token_boosts_top"])
        if not data:
            return []
        return [t for t in data if t.get("chainId") == CHAIN_ID]

    def get_latest_boosts(self) -> list:
        """Récupérer les derniers tokens boostés"""
        data = self._get(ENDPOINTS["token_boosts_latest"])
        if not data:
            return []
        return [t for t in data if t.get("chainId") == CHAIN_ID]

    def get_trending_metas(self) -> list:
        """Récupérer les narratives/metas trending"""
        data = self._get(ENDPOINTS["trending_metas"])
        if not data:
            return []
        return data

    def search_pairs(self, query: str) -> list:
        """Rechercher des paires par nom/symbole"""
        data = self._get(ENDPOINTS["search"], params={"q": query})
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
        data = self._get(endpoint)
        if not data:
            return []
        return data if isinstance(data, list) else data.get("pairs", [])

    def get_pair_info(self, pair_address: str) -> Optional[dict]:
        """Récupérer les infos d'une paire spécifique"""
        endpoint = f"{ENDPOINTS['pairs']}/{pair_address}"
        data = self._get(endpoint)
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
        """
        pairs = self.get_token_info([token_address])
        if not pairs:
            return None

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
