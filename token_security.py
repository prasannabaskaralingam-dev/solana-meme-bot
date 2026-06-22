"""
Module de sécurité anti-rug RENFORCÉ pour tokens Solana.
Utilise RugCheck API + vérifications on-chain + heuristiques avancées.

Objectif: ÉLIMINER les -98% (rug pulls) avant l'achat.
"""

import time
import logging
import httpx
from typing import Optional, Tuple, List
from dataclasses import dataclass, field

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
    lp_burned: bool
    top_holder_pct: float  # % du top holder
    insider_networks: int  # Nombre de réseaux d'insiders détectés
    holder_count: int = 0
    creator_holding_pct: float = 0.0
    is_honeypot: bool = False


class TokenSecurityChecker:
    """
    Vérificateur de sécurité des tokens RENFORCÉ.
    Combine RugCheck API + vérifications RPC on-chain + heuristiques.
    
    Stratégie anti-rug en 3 couches:
    1. RugCheck API (score global + risques identifiés)
    2. Vérifications on-chain (authorities, holders, LP)
    3. Heuristiques comportementales (patterns de scam)
    """

    RUGCHECK_API = "https://api.rugcheck.xyz"

    # === SEUILS DE SÉCURITÉ RENFORCÉS ===
    
    # Score RugCheck maximum autorisé
    MAX_RISK_SCORE = 350  # Réduit de 400 à 350 (plus strict)
    
    # Concentration des holders
    MAX_TOP_HOLDER_PCT = 25.0  # Réduit de 30% à 25%
    MAX_CREATOR_HOLDING_PCT = 15.0  # Le créateur ne doit pas garder > 15%
    MAX_TOP5_COMBINED_PCT = 50.0  # Top 5 holders combinés < 50%
    
    # Liquidité
    MIN_LP_LOCKED_PCT = 50.0  # LP doit être lockée à > 50%
    MIN_LIQUIDITY_USD = 5000  # Minimum $5000 de liquidité
    
    # Holders
    MIN_HOLDERS_SNIPER = 30  # Minimum 30 holders pour stratégie sniper
    MIN_HOLDERS_MOMENTUM = 50  # Minimum 50 holders pour momentum
    
    # Insider networks
    MAX_INSIDER_NETWORKS = 0  # ZÉRO tolérance pour les insiders
    
    # Blacklist de patterns de noms (scam fréquents)
    SCAM_NAME_PATTERNS = [
        "airdrop", "free", "claim", "reward",
        "official", "verified", "v2", "migration",
    ]

    # Adresses connues (pools, burn, etc.)
    KNOWN_POOLS = {
        "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1",  # Raydium
        "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",  # Raydium V4
        "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",  # Raydium CPMM
        "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",   # Orca Whirlpool
        "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",   # Jupiter
    }
    BURN_ADDRESSES = {
        "1nc1nerator11111111111111111111111111111111",
        "11111111111111111111111111111111",
    }

    def __init__(self, rpc_url: str = "https://api.mainnet-beta.solana.com"):
        self.rpc_url = rpc_url
        self.client = httpx.Client(timeout=12)
        # Cache pour éviter de re-checker le même token
        self._cache: dict[str, SecurityReport] = {}
        self._cache_ttl = 300  # 5 minutes
        self._cache_timestamps: dict[str, float] = {}
        # Blacklist de créateurs connus comme scammers
        self._scammer_creators: set = set()

    def check_token(self, token_address: str, strategy: str = "momentum") -> SecurityReport:
        """
        Vérification complète de sécurité d'un token.
        Plus strict pour la stratégie 'sniper' (tokens très jeunes).
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
        lp_burned = False
        top_holder_pct = 0.0
        creator_holding_pct = 0.0
        insider_networks = 0
        holder_count = 0
        is_honeypot = False
        top5_combined = 0.0

        # === COUCHE 1: RugCheck API ===
        rugcheck_data = self._get_rugcheck_report(token_address)
        if rugcheck_data:
            # Score de risque RugCheck
            risks = rugcheck_data.get("risks", [])
            for risk in risks:
                risk_name = risk.get("name", "")
                risk_level = risk.get("level", "")
                risk_score_item = risk.get("score", 0)
                risk_description = risk.get("description", "").lower()
                risk_score += risk_score_item

                # Vérifier les risques critiques
                if "mint authority" in risk_name.lower():
                    if "still enabled" in risk_description or "not revoked" in risk_description:
                        reasons.append(f"🚨 MINT AUTHORITY ACTIVE")
                        risk_score += 150  # Pénalité supplémentaire
                elif "freeze authority" in risk_name.lower():
                    if "still enabled" in risk_description or "not revoked" in risk_description:
                        reasons.append(f"🚨 FREEZE AUTHORITY ACTIVE")
                        risk_score += 100
                elif "low liquidity" in risk_name.lower():
                    reasons.append(f"⚠️ Liquidité faible")
                elif "insider" in risk_name.lower() or "insider" in risk_description:
                    insider_networks += 1
                    reasons.append(f"🚨 Insider détecté: {risk_name}")
                elif "honeypot" in risk_name.lower() or "honeypot" in risk_description:
                    is_honeypot = True
                    reasons.append(f"🚨 HONEYPOT DÉTECTÉ")
                    risk_score += 500
                elif "copycat" in risk_name.lower() or "copy" in risk_description:
                    reasons.append(f"⚠️ Token copié/clone")
                    risk_score += 100
                elif risk_level == "danger" or risk_score_item >= 100:
                    reasons.append(f"🚨 {risk_name}")

            # Vérifier mint/freeze authority depuis RugCheck
            token_meta = rugcheck_data.get("tokenMeta", {})
            if token_meta:
                mint_auth = token_meta.get("mintAuthority")
                freeze_auth = token_meta.get("freezeAuthority")
                mint_revoked = mint_auth is None or mint_auth == "" or mint_auth == "null"
                freeze_revoked = freeze_auth is None or freeze_auth == "" or freeze_auth == "null"

            # === COUCHE 2: Analyse des holders (RENFORCÉE) ===
            top_holders = rugcheck_data.get("topHolders", [])
            if top_holders:
                real_holders = []
                for holder in top_holders:
                    holder_addr = holder.get("address", "")
                    holder_pct = holder.get("pct", 0)
                    holder_is_insider = holder.get("isInsider", False)
                    
                    # Exclure les pools et burn addresses
                    if holder_addr in self.KNOWN_POOLS or holder_addr in self.BURN_ADDRESSES:
                        continue
                    
                    real_holders.append({
                        "address": holder_addr,
                        "pct": holder_pct,
                        "is_insider": holder_is_insider
                    })
                    
                    if holder_is_insider:
                        insider_networks += 1

                # Top holder
                if real_holders:
                    top_holder_pct = real_holders[0]["pct"]
                    
                    # Top 5 combinés
                    top5_combined = sum(h["pct"] for h in real_holders[:5])
                    
                    # Vérifier le créateur (souvent le premier holder non-pool)
                    creator = rugcheck_data.get("creator")
                    if creator:
                        for h in real_holders:
                            if h["address"] == creator:
                                creator_holding_pct = h["pct"]
                                break

                # Nombre de holders (estimation)
                holder_count = len(top_holders)
                # RugCheck peut aussi fournir le nombre total
                total_holders = rugcheck_data.get("totalHolders", 0)
                if total_holders > 0:
                    holder_count = total_holders

                # === CHECKS DE CONCENTRATION ===
                if top_holder_pct > self.MAX_TOP_HOLDER_PCT:
                    reasons.append(f"🚨 Top holder: {top_holder_pct:.1f}% (max {self.MAX_TOP_HOLDER_PCT}%)")
                    risk_score += 200

                if top5_combined > self.MAX_TOP5_COMBINED_PCT:
                    reasons.append(f"🚨 Top 5 holders: {top5_combined:.1f}% (max {self.MAX_TOP5_COMBINED_PCT}%)")
                    risk_score += 150

                if creator_holding_pct > self.MAX_CREATOR_HOLDING_PCT:
                    reasons.append(f"🚨 Créateur garde: {creator_holding_pct:.1f}% (max {self.MAX_CREATOR_HOLDING_PCT}%)")
                    risk_score += 200

            # === COUCHE 3: Vérification LP ===
            markets = rugcheck_data.get("markets", [])
            for market in markets:
                lp_info = market.get("lp", {})
                lp_locked_pct = lp_info.get("lpLockedPct", 0)
                lp_burned_pct = lp_info.get("lpBurnedPct", 0)
                
                if lp_burned_pct >= 90:
                    lp_burned = True
                    lp_locked = True  # Burned = mieux que locked
                    reasons.append(f"✅ LP BURNED: {lp_burned_pct:.0f}%")
                    break
                elif lp_locked_pct >= self.MIN_LP_LOCKED_PCT:
                    lp_locked = True
                    reasons.append(f"✅ LP lockée: {lp_locked_pct:.0f}%")
                    break

            if not lp_locked and not lp_burned and markets:
                reasons.append("🚨 LP NON LOCKÉE/BURNED - Rug possible à tout moment")
                risk_score += 200  # Augmenté de 100 à 200

            # Vérifier le créateur dans la blacklist
            creator = rugcheck_data.get("creator", "")
            if creator and creator in self._scammer_creators:
                reasons.append(f"🚨 CRÉATEUR BLACKLISTÉ (scammer connu)")
                risk_score += 500
                is_honeypot = True

        else:
            # Pas de données RugCheck - vérifications on-chain uniquement
            logger.warning(f"RugCheck indisponible pour {token_address[:12]}...")
            mint_revoked, freeze_revoked = self._check_authorities_onchain(token_address)
            if not mint_revoked:
                reasons.append("🚨 Mint Authority active (on-chain)")
                risk_score += 250
            if not freeze_revoked:
                reasons.append("🚨 Freeze Authority active (on-chain)")
                risk_score += 200
            # Sans RugCheck, on est plus conservateur
            risk_score += 100
            reasons.append("⚠️ RugCheck indisponible - vérification limitée")

        # === BONUS DE SÉCURITÉ ===
        if mint_revoked:
            reasons.append("✅ Mint Authority révoquée")
        if freeze_revoked:
            reasons.append("✅ Freeze Authority révoquée")
        if mint_revoked and freeze_revoked and (lp_locked or lp_burned):
            # Token très sûr: réduction du score
            risk_score = max(0, risk_score - 100)
            reasons.append("✅ Triple sécurité: Mint OFF + Freeze OFF + LP protégée")

        # === DÉTERMINER LE NIVEAU DE RISQUE ===
        if risk_score <= 100:
            risk_level = "Good"
        elif risk_score <= 350:
            risk_level = "Medium"
        elif risk_score <= 600:
            risk_level = "High"
        else:
            risk_level = "Critical"

        # === DÉCISION FINALE (STRICTE) ===
        is_safe = True
        rejection_reasons = []

        # 1. Score trop élevé
        if risk_score > self.MAX_RISK_SCORE:
            is_safe = False
            rejection_reasons.append(f"Score {risk_score} > {self.MAX_RISK_SCORE}")

        # 2. Honeypot détecté
        if is_honeypot:
            is_safe = False
            rejection_reasons.append("Honeypot")

        # 3. Insiders
        if insider_networks > self.MAX_INSIDER_NETWORKS:
            is_safe = False
            rejection_reasons.append(f"{insider_networks} insider(s)")

        # 4. Top holder trop concentré
        if top_holder_pct > self.MAX_TOP_HOLDER_PCT:
            is_safe = False
            rejection_reasons.append(f"Top holder {top_holder_pct:.1f}%")

        # 5. Créateur garde trop
        if creator_holding_pct > self.MAX_CREATOR_HOLDING_PCT:
            is_safe = False
            rejection_reasons.append(f"Créateur {creator_holding_pct:.1f}%")

        # 6. RÈGLE CRITIQUE: Mint active + LP non protégée = REJET ABSOLU
        if not mint_revoked and not lp_locked and not lp_burned:
            is_safe = False
            rejection_reasons.append("Mint active + LP non protégée")

        # 7. Freeze authority active = peut bloquer tes tokens
        if not freeze_revoked:
            is_safe = False
            rejection_reasons.append("Freeze authority active")

        # 8. RÈGLE SNIPER: plus strict pour les tokens très jeunes
        if strategy == "sniper":
            # Pour les snipes, on exige LP burned (pas juste locked)
            if not lp_burned and not mint_revoked:
                is_safe = False
                rejection_reasons.append("Sniper: LP non burned + Mint active")
            # Minimum de holders plus strict
            if holder_count > 0 and holder_count < self.MIN_HOLDERS_SNIPER:
                is_safe = False
                rejection_reasons.append(f"Sniper: {holder_count} holders < {self.MIN_HOLDERS_SNIPER}")

        # 9. Top 5 holders trop concentrés
        if top5_combined > self.MAX_TOP5_COMBINED_PCT:
            is_safe = False
            rejection_reasons.append(f"Top5 = {top5_combined:.1f}%")

        # Log les raisons de rejet
        if not is_safe and rejection_reasons:
            logger.info(f"🚫 REJETÉ {token_address[:12]}: {', '.join(rejection_reasons)}")

        report = SecurityReport(
            token_address=token_address,
            is_safe=is_safe,
            risk_score=risk_score,
            risk_level=risk_level,
            reasons=reasons,
            mint_authority_revoked=mint_revoked,
            freeze_authority_revoked=freeze_revoked,
            lp_locked=lp_locked,
            lp_burned=lp_burned,
            top_holder_pct=top_holder_pct,
            insider_networks=insider_networks,
            holder_count=holder_count,
            creator_holding_pct=creator_holding_pct,
            is_honeypot=is_honeypot,
        )

        # Mettre en cache
        self._cache[token_address] = report
        self._cache_timestamps[token_address] = time.time()

        logger.info(
            f"Security check {token_address[:12]}...: "
            f"score={risk_score}, level={risk_level}, safe={is_safe}, "
            f"mint={'OFF' if mint_revoked else 'ON'}, "
            f"freeze={'OFF' if freeze_revoked else 'ON'}, "
            f"lp={'BURNED' if lp_burned else ('LOCKED' if lp_locked else 'OPEN')}"
        )

        return report

    def blacklist_creator(self, creator_address: str):
        """Ajouter un créateur à la blacklist (après un rug pull détecté)"""
        self._scammer_creators.add(creator_address)
        logger.info(f"🚫 Créateur blacklisté: {creator_address[:12]}...")

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

    def quick_check(self, token_address: str, strategy: str = "momentum") -> Tuple[bool, str]:
        """
        Vérification rapide - retourne (is_safe, reason).
        Utilisé comme gate avant l'achat.
        """
        report = self.check_token(token_address, strategy=strategy)
        if report.is_safe:
            safety_details = []
            if report.lp_burned:
                safety_details.append("LP burned")
            elif report.lp_locked:
                safety_details.append("LP locked")
            if report.mint_authority_revoked:
                safety_details.append("Mint OFF")
            detail_str = " | ".join(safety_details) if safety_details else ""
            return True, f"✅ Score: {report.risk_score} ({report.risk_level}) {detail_str}"
        else:
            # Trouver la raison principale de rejet
            danger_reasons = [r for r in report.reasons if "🚨" in r]
            if not danger_reasons:
                danger_reasons = [r for r in report.reasons if "⚠️" in r]
            main_reason = danger_reasons[0] if danger_reasons else f"Score trop élevé: {report.risk_score}"
            return False, main_reason

    def get_safety_summary(self, token_address: str) -> str:
        """Résumé court pour les notifications Telegram"""
        if token_address not in self._cache:
            return ""
        report = self._cache[token_address]
        
        parts = []
        parts.append(f"Score: {report.risk_score}")
        
        if report.lp_burned:
            parts.append("LP🔥")
        elif report.lp_locked:
            parts.append("LP🔒")
        else:
            parts.append("LP⚠️")
        
        if report.mint_authority_revoked:
            parts.append("Mint✅")
        else:
            parts.append("Mint❌")
            
        if report.freeze_authority_revoked:
            parts.append("Freeze✅")
        else:
            parts.append("Freeze❌")
        
        if report.top_holder_pct > 0:
            parts.append(f"Top:{report.top_holder_pct:.0f}%")
            
        return " | ".join(parts)
