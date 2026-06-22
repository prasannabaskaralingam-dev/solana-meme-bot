"""
Module de sécurité anti-rug pour tokens Solana.
Utilise RugCheck API (gratuit) + vérifications on-chain.
"""

import time
import logging
import httpx
from typing import Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SecurityReport:
    """Résultat de l'analyse de sécurité d'un token"""
    token_address: str
    is_safe: bool
    risk_score: int  # 0-1000 (0 = safe, 1000 = scam)
    risk_level: str  # "Good", "Medium", "High", "Critical"
    reasons: list  # Liste des raisons de rejet/acceptation
    mint_authority_revoked: bool
    freeze_authority_revoked: bool
    lp_locked: bool
    top_holder_pct: float  # % du top holder
    insider_networks: int  # Nombre de réseaux d'insiders détectés


class TokenSecurityChecker:
    """
    Vérificateur de sécurité des tokens.
    Combine RugCheck API + vérifications RPC on-chain.
    """

    RUGCHECK_API = "https://api.rugcheck.xyz"

    # Seuils de sécurité
    MAX_RISK_SCORE = 400  # Au-dessus = rejeté (High Risk)
    MAX_TOP_HOLDER_PCT = 30.0  # Si un holder a > 30% = danger
    MIN_LP_LOCKED_PCT = 50.0  # LP doit être lockée à > 50%

    def __init__(self, rpc_url: str = "https://api.mainnet-beta.solana.com"):
        self.rpc_url = rpc_url
        self.client = httpx.Client(timeout=12)
        # Cache pour éviter de re-checker le même token
        self._cache: dict[str, SecurityReport] = {}
        self._cache_ttl = 300  # 5 minutes
        self._cache_timestamps: dict[str, float] = {}

    def check_token(self, token_address: str) -> SecurityReport:
        """
        Vérification complète de sécurité d'un token.
        Retourne un SecurityReport avec toutes les infos.
        """
        # Vérifier le cache
        if token_address in self._cache:
            if time.time() - self._cache_timestamps[token_address] < self._cache_ttl:
                return self._cache[token_address]

        reasons = []
        risk_score = 0
        mint_revoked = False
        freeze_revoked = False
        lp_locked = False
        top_holder_pct = 0.0
        insider_networks = 0

        # 1. RugCheck API (principal)
        rugcheck_data = self._get_rugcheck_report(token_address)
        if rugcheck_data:
            # Score de risque RugCheck
            risks = rugcheck_data.get("risks", [])
            for risk in risks:
                risk_name = risk.get("name", "")
                risk_level = risk.get("level", "")
                risk_score_item = risk.get("score", 0)
                risk_score += risk_score_item

                # Vérifier les risques critiques
                if "Mint Authority" in risk_name and "still enabled" in risk.get("description", "").lower():
                    reasons.append(f"⚠️ Mint Authority active: {risk_name}")
                elif "Freeze Authority" in risk_name:
                    reasons.append(f"⚠️ Freeze Authority: {risk_name}")
                elif "Low Liquidity" in risk_name:
                    reasons.append(f"⚠️ Liquidité faible")
                elif "Insider" in risk_name or "insider" in risk_name:
                    insider_networks += 1
                    reasons.append(f"🚨 Insider détecté: {risk_name}")
                elif risk_level == "danger" or risk_score_item >= 100:
                    reasons.append(f"🚨 {risk_name}")

            # Vérifier mint/freeze authority depuis RugCheck
            token_meta = rugcheck_data.get("tokenMeta", {})
            if token_meta:
                mint_revoked = token_meta.get("mintAuthority") is None or token_meta.get("mintAuthority") == ""
                freeze_revoked = token_meta.get("freezeAuthority") is None or token_meta.get("freezeAuthority") == ""

            # Vérifier les top holders
            top_holders = rugcheck_data.get("topHolders", [])
            if top_holders:
                # Exclure les pools de liquidité et les burn addresses
                KNOWN_POOLS = {"5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1"}  # Raydium
                BURN_ADDRESSES = {"1nc1nerator11111111111111111111111111111111"}
                for holder in top_holders:
                    holder_addr = holder.get("address", "")
                    holder_pct = holder.get("pct", 0)
                    if holder_addr in KNOWN_POOLS or holder_addr in BURN_ADDRESSES:
                        continue
                    if holder_pct > top_holder_pct:
                        top_holder_pct = holder_pct

                if top_holder_pct > self.MAX_TOP_HOLDER_PCT:
                    reasons.append(f"🚨 Top holder: {top_holder_pct:.1f}% (max {self.MAX_TOP_HOLDER_PCT}%)")
                    risk_score += 200

            # Vérifier LP lockée
            markets = rugcheck_data.get("markets", [])
            for market in markets:
                lp_locked_pct = market.get("lp", {}).get("lpLockedPct", 0)
                if lp_locked_pct >= self.MIN_LP_LOCKED_PCT:
                    lp_locked = True
                    reasons.append(f"✅ LP lockée: {lp_locked_pct:.0f}%")
                    break

            if not lp_locked and markets:
                reasons.append("⚠️ LP non lockée")
                risk_score += 100

        else:
            # Pas de données RugCheck - faire les vérifications on-chain
            logger.warning(f"RugCheck indisponible pour {token_address[:12]}...")
            mint_revoked, freeze_revoked = self._check_authorities_onchain(token_address)
            if not mint_revoked:
                reasons.append("⚠️ Mint Authority active (on-chain)")
                risk_score += 200
            if not freeze_revoked:
                reasons.append("⚠️ Freeze Authority active (on-chain)")
                risk_score += 150

        # Bonus de sécurité
        if mint_revoked:
            reasons.append("✅ Mint Authority révoquée")
        if freeze_revoked:
            reasons.append("✅ Freeze Authority révoquée")

        # Déterminer le niveau de risque
        if risk_score <= 100:
            risk_level = "Good"
        elif risk_score <= 400:
            risk_level = "Medium"
        elif risk_score <= 700:
            risk_level = "High"
        else:
            risk_level = "Critical"

        # Décision finale
        is_safe = (
            risk_score <= self.MAX_RISK_SCORE and
            top_holder_pct <= self.MAX_TOP_HOLDER_PCT and
            insider_networks == 0
        )

        # Si mint authority est active ET pas de LP lock = très dangereux
        if not mint_revoked and not lp_locked:
            is_safe = False
            if "Mint + LP non lockée" not in str(reasons):
                reasons.append("🚨 DANGER: Mint active + LP non lockée = rug probable")

        report = SecurityReport(
            token_address=token_address,
            is_safe=is_safe,
            risk_score=risk_score,
            risk_level=risk_level,
            reasons=reasons,
            mint_authority_revoked=mint_revoked,
            freeze_authority_revoked=freeze_revoked,
            lp_locked=lp_locked,
            top_holder_pct=top_holder_pct,
            insider_networks=insider_networks,
        )

        # Mettre en cache
        self._cache[token_address] = report
        self._cache_timestamps[token_address] = time.time()

        logger.info(
            f"Security check {token_address[:12]}...: "
            f"score={risk_score}, level={risk_level}, safe={is_safe}"
        )

        return report

    def _get_rugcheck_report(self, token_address: str) -> Optional[dict]:
        """Récupérer le rapport RugCheck (gratuit, pas besoin de clé API)"""
        try:
            url = f"{self.RUGCHECK_API}/v1/tokens/{token_address}/report"
            response = self.client.get(url)
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"RugCheck API status {response.status_code} pour {token_address[:12]}")
                return None
        except Exception as e:
            logger.error(f"Erreur RugCheck API: {e}")
            return None

    def _get_rugcheck_summary(self, token_address: str) -> Optional[dict]:
        """Récupérer le résumé rapide RugCheck (plus léger)"""
        try:
            url = f"{self.RUGCHECK_API}/v1/tokens/{token_address}/report/summary"
            response = self.client.get(url)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            logger.error(f"Erreur RugCheck summary: {e}")
            return None

    def _check_authorities_onchain(self, token_address: str) -> Tuple[bool, bool]:
        """Vérifier mint/freeze authority directement on-chain via RPC"""
        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getAccountInfo",
                "params": [
                    token_address,
                    {"encoding": "jsonParsed"}
                ]
            }
            response = httpx.post(self.rpc_url, json=payload, timeout=10)
            result = response.json()
            account_data = result.get("result", {}).get("value", {})
            if not account_data:
                return False, False

            parsed = account_data.get("data", {}).get("parsed", {})
            info = parsed.get("info", {})

            mint_authority = info.get("mintAuthority")
            freeze_authority = info.get("freezeAuthority")

            mint_revoked = mint_authority is None or mint_authority == ""
            freeze_revoked = freeze_authority is None or freeze_authority == ""

            return mint_revoked, freeze_revoked
        except Exception as e:
            logger.error(f"Erreur check authorities on-chain: {e}")
            return False, False

    def quick_check(self, token_address: str) -> Tuple[bool, str]:
        """
        Vérification rapide - retourne (is_safe, reason).
        Utilisé comme gate avant l'achat.
        """
        report = self.check_token(token_address)
        if report.is_safe:
            return True, f"✅ Score: {report.risk_score} ({report.risk_level})"
        else:
            # Trouver la raison principale de rejet
            danger_reasons = [r for r in report.reasons if "🚨" in r or "⚠️" in r]
            main_reason = danger_reasons[0] if danger_reasons else f"Score trop élevé: {report.risk_score}"
            return False, main_reason
