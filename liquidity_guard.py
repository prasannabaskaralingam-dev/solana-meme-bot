"""
Module de Protection Liquidité - 3 couches de sécurité:

1. LP MONITOR POST-ACHAT: Surveille la liquidité après l'achat.
   Si la LP chute de > 50% → vente d'urgence immédiate (rug pull en cours)

2. SL GARANTI: Vérifie que chaque position a un SL actif au démarrage.
   Force un SL sur les positions orphelines/récupérées.

3. ILLIQUIDITY GUARD: Vérifie la liquidité AVANT l'achat (min $3000).
   Si vente impossible (token illiquide) → retry avec slippage progressif.
"""

import time
import logging
import httpx
from typing import Optional, Tuple, Dict, List
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

DEXSCREENER_API = "https://api.dexscreener.com/latest/dex/tokens"


@dataclass
class LiquiditySnapshot:
    """Snapshot de la liquidité d'un token au moment de l'achat"""
    token_address: str
    entry_liquidity_usd: float  # Liquidité au moment de l'achat
    current_liquidity_usd: float  # Dernière liquidité connue
    last_check: float  # Timestamp du dernier check
    alert_sent: bool = False  # Alerte déjà envoyée
    emergency_triggered: bool = False  # Vente d'urgence déjà déclenchée


class LiquidityGuard:
    """
    Gardien de liquidité - Protège contre les rug pulls et tokens illiquides.
    
    3 protections:
    1. Pré-achat: vérifier que le token a assez de liquidité pour sortir
    2. Post-achat: monitoring continu de la LP, vente d'urgence si chute
    3. Vente: retry avec slippage progressif si le token est devenu illiquide
    """

    # === SEUILS ===
    MIN_LIQUIDITY_BUY = 3000       # $3000 minimum pour acheter
    MIN_LIQUIDITY_SNIPER = 5000    # $5000 pour les snipes (plus strict)
    LP_DROP_ALERT_PCT = 40         # Alerte si LP chute de 40%
    LP_DROP_EMERGENCY_PCT = 60     # Vente d'urgence si LP chute de 60%
    LP_CHECK_INTERVAL = 30         # Vérifier la LP toutes les 30s
    
    # Slippage progressif pour vente d'urgence
    EMERGENCY_SLIPPAGE_LEVELS = [
        1000,   # 10% - premier essai
        2000,   # 20% - deuxième essai
        3500,   # 35% - troisième essai
        5000,   # 50% - dernier essai (accepter grosse perte)
    ]

    def __init__(self):
        self.snapshots: Dict[str, LiquiditySnapshot] = {}
        self._http = httpx.Client(timeout=10)

    # ================================================================
    # PROTECTION 1: Vérification pré-achat
    # ================================================================

    def can_buy(self, token_address: str, liquidity_usd: float, 
                strategy: str = "momentum") -> Tuple[bool, str]:
        """
        Vérifier si un token a assez de liquidité pour être acheté.
        Retourne (can_buy, reason).
        """
        min_liq = self.MIN_LIQUIDITY_SNIPER if strategy == "sniper" else self.MIN_LIQUIDITY_BUY

        if liquidity_usd < min_liq:
            return False, f"💧 Liquidité insuffisante: ${liquidity_usd:.0f} < ${min_liq} (risque illiquide)"

        # Vérifier le ratio liquidité/position
        # Si la liquidité est trop faible par rapport à la taille du trade,
        # le slippage sera énorme à la vente
        # Règle: liquidité doit être > 50x la taille du trade (~0.05 SOL ≈ $8)
        # $3000 / $8 = 375x → OK pour 0.05 SOL
        # Mais si quelqu'un trade 0.15 SOL ($24), il faut $1200 minimum
        # On vérifie ça dans le flow d'achat avec le montant réel

        return True, f"✅ Liquidité OK: ${liquidity_usd:.0f}"

    def can_exit(self, token_address: str) -> Tuple[bool, float, str]:
        """
        Vérifier si on peut sortir d'une position (liquidité suffisante).
        Retourne (can_exit, current_liquidity, reason).
        """
        current_liq = self._fetch_liquidity(token_address)
        if current_liq is None:
            return True, 0, "⚠️ Impossible de vérifier la liquidité (on tente quand même)"

        if current_liq < 500:
            return False, current_liq, f"🚨 Token ILLIQUIDE: ${current_liq:.0f} de liquidité"
        elif current_liq < 1500:
            return True, current_liq, f"⚠️ Liquidité faible: ${current_liq:.0f} (slippage élevé probable)"
        else:
            return True, current_liq, f"✅ Liquidité OK: ${current_liq:.0f}"

    # ================================================================
    # PROTECTION 2: Monitoring LP post-achat
    # ================================================================

    def register_position(self, token_address: str, liquidity_usd: float):
        """Enregistrer la liquidité au moment de l'achat pour monitoring"""
        self.snapshots[token_address] = LiquiditySnapshot(
            token_address=token_address,
            entry_liquidity_usd=liquidity_usd,
            current_liquidity_usd=liquidity_usd,
            last_check=time.time(),
        )
        logger.info(f"💧 LP Monitor: {token_address[:12]}... enregistré (LP: ${liquidity_usd:.0f})")

    def unregister_position(self, token_address: str):
        """Retirer un token du monitoring LP (après vente)"""
        if token_address in self.snapshots:
            del self.snapshots[token_address]

    def check_all_positions(self) -> List[Dict]:
        """
        Vérifier la liquidité de toutes les positions ouvertes.
        Retourne une liste d'alertes/actions à prendre.
        
        Actions possibles:
        - {"type": "alert", "token": ..., "drop_pct": ..., "msg": ...}
        - {"type": "emergency_sell", "token": ..., "drop_pct": ..., "msg": ...}
        """
        actions = []
        now = time.time()

        for token_addr, snapshot in list(self.snapshots.items()):
            # Respecter l'intervalle de check
            if now - snapshot.last_check < self.LP_CHECK_INTERVAL:
                continue

            # Fetch la liquidité actuelle
            current_liq = self._fetch_liquidity(token_addr)
            if current_liq is None:
                continue

            snapshot.current_liquidity_usd = current_liq
            snapshot.last_check = now

            # Calculer la chute de LP
            if snapshot.entry_liquidity_usd > 0:
                drop_pct = ((snapshot.entry_liquidity_usd - current_liq) / snapshot.entry_liquidity_usd) * 100
            else:
                drop_pct = 0

            # === VENTE D'URGENCE: LP a chuté de > 60% ===
            if drop_pct >= self.LP_DROP_EMERGENCY_PCT and not snapshot.emergency_triggered:
                snapshot.emergency_triggered = True
                actions.append({
                    "type": "emergency_sell",
                    "token": token_addr,
                    "drop_pct": drop_pct,
                    "current_liq": current_liq,
                    "entry_liq": snapshot.entry_liquidity_usd,
                    "msg": f"🚨 VENTE D'URGENCE - LP effondrée de {drop_pct:.0f}%! "
                           f"(${snapshot.entry_liquidity_usd:.0f} → ${current_liq:.0f})"
                })
                logger.warning(
                    f"🚨 EMERGENCY SELL {token_addr[:12]}: "
                    f"LP -{drop_pct:.0f}% (${snapshot.entry_liquidity_usd:.0f} → ${current_liq:.0f})"
                )

            # === ALERTE: LP a chuté de > 40% ===
            elif drop_pct >= self.LP_DROP_ALERT_PCT and not snapshot.alert_sent:
                snapshot.alert_sent = True
                actions.append({
                    "type": "alert",
                    "token": token_addr,
                    "drop_pct": drop_pct,
                    "current_liq": current_liq,
                    "entry_liq": snapshot.entry_liquidity_usd,
                    "msg": f"⚠️ ALERTE LP - Liquidité en chute de {drop_pct:.0f}%! "
                           f"(${snapshot.entry_liquidity_usd:.0f} → ${current_liq:.0f})"
                })
                logger.warning(
                    f"⚠️ LP ALERT {token_addr[:12]}: "
                    f"LP -{drop_pct:.0f}% (${snapshot.entry_liquidity_usd:.0f} → ${current_liq:.0f})"
                )

        return actions

    # ================================================================
    # PROTECTION 3: Vente avec retry et slippage progressif
    # ================================================================

    def emergency_sell_with_retry(self, swap_engine, token_mint: str, 
                                   raw_amount: int) -> Optional[str]:
        """
        Tenter de vendre un token avec slippage progressif.
        Si la première tentative échoue (token illiquide), on augmente le slippage.
        
        Retourne le tx_signature si succès, None si échec total.
        """
        original_slippage = swap_engine.config.slippage_bps
        sol_mint = "So11111111111111111111111111111111111111112"

        for i, slippage in enumerate(self.EMERGENCY_SLIPPAGE_LEVELS):
            try:
                logger.info(
                    f"🔄 Tentative vente {i+1}/{len(self.EMERGENCY_SLIPPAGE_LEVELS)} "
                    f"- Slippage: {slippage/100}%"
                )

                # Modifier temporairement le slippage
                swap_engine.config.slippage_bps = slippage

                # Obtenir un devis
                quote = swap_engine.get_quote(token_mint, sol_mint, raw_amount)
                if not quote:
                    logger.warning(f"  ❌ Pas de devis (tentative {i+1})")
                    time.sleep(2)
                    continue

                out_amount = int(quote.get("outAmount", 0))
                sol_received = out_amount / 1_000_000_000
                logger.info(f"  📊 Devis: → {sol_received:.4f} SOL (slippage {slippage/100}%)")

                # Exécuter le swap
                tx_sig = swap_engine.execute_swap(quote)
                if tx_sig:
                    confirmed = swap_engine.confirm_transaction(tx_sig, timeout=30)
                    if confirmed:
                        logger.info(f"  ✅ Vente d'urgence réussie! TX: {tx_sig}")
                        swap_engine.config.slippage_bps = original_slippage
                        return tx_sig
                    else:
                        logger.warning(f"  ⚠️ TX envoyée mais non confirmée: {tx_sig}")
                        swap_engine.config.slippage_bps = original_slippage
                        return tx_sig

                logger.warning(f"  ❌ Swap échoué (tentative {i+1})")
                time.sleep(3)

            except Exception as e:
                logger.error(f"  ❌ Erreur tentative {i+1}: {e}")
                time.sleep(2)

        # Restaurer le slippage original
        swap_engine.config.slippage_bps = original_slippage
        logger.error(f"🚨 ÉCHEC TOTAL: Impossible de vendre {token_mint[:12]}... après {len(self.EMERGENCY_SLIPPAGE_LEVELS)} tentatives")
        return None

    # ================================================================
    # PROTECTION BONUS: SL garanti au démarrage
    # ================================================================

    def validate_positions_on_startup(self, positions: dict, config) -> List[str]:
        """
        Vérifier que toutes les positions ouvertes ont un SL actif.
        Retourne la liste des tokens qui n'avaient pas de SL (maintenant forcé).
        
        En pratique, le SL est global (config.stop_loss_pct), donc cette fonction
        vérifie que:
        1. Le SL est activé (pas à 0 ou désactivé)
        2. Chaque position a un entry_price valide (sinon impossible de calculer le SL)
        3. Les positions sans prix d'entrée sont marquées pour vente immédiate
        """
        issues = []

        # Vérifier que le SL global est actif
        if config.stop_loss_pct >= 0:
            logger.warning("⚠️ Stop Loss désactivé ou positif! Forçage à -30%")
            config.stop_loss_pct = -30.0
            issues.append("SL global forcé à -30%")

        # Vérifier chaque position
        for token_addr, position in positions.items():
            # Position sans prix d'entrée = impossible de calculer SL
            if position.entry_price_usd <= 0:
                issues.append(f"⚠️ {position.token_name}: prix d'entrée invalide ({position.entry_price_usd})")
                logger.warning(
                    f"Position {position.token_name} sans prix d'entrée valide - "
                    f"sera vendue au prochain check"
                )

            # Position sans entry_time = impossible de calculer Time Stop
            if not position.entry_time:
                issues.append(f"⚠️ {position.token_name}: pas de timestamp d'entrée")
                # Forcer un timestamp (maintenant - 1h pour déclencher le time stop)
                from datetime import datetime, timedelta
                position.entry_time = (datetime.utcnow() - timedelta(hours=1)).isoformat()
                logger.warning(
                    f"Position {position.token_name} sans timestamp - "
                    f"forcé à -1h (time stop va se déclencher)"
                )

        return issues

    # ================================================================
    # HELPERS
    # ================================================================

    def _fetch_liquidity(self, token_address: str) -> Optional[float]:
        """Récupérer la liquidité actuelle d'un token via DexScreener"""
        try:
            url = f"{DEXSCREENER_API}/{token_address}"
            response = self._http.get(url)
            if response.status_code != 200:
                return None

            data = response.json()
            pairs = data.get("pairs", [])
            if not pairs:
                return 0.0  # Aucun pool = liquidité 0

            # Prendre le pool le plus liquide
            best_liq = 0.0
            for pair in pairs:
                if pair.get("chainId", "").lower() != "solana":
                    continue
                liq = pair.get("liquidity", {}).get("usd", 0) or 0
                if liq > best_liq:
                    best_liq = liq

            return best_liq

        except Exception as e:
            logger.error(f"Erreur fetch liquidité {token_address[:12]}: {e}")
            return None

    def get_status(self) -> str:
        """Retourner un résumé du statut du monitoring LP"""
        if not self.snapshots:
            return "Aucune position monitorée"

        lines = []
        for addr, snap in self.snapshots.items():
            if snap.entry_liquidity_usd > 0:
                change = ((snap.current_liquidity_usd - snap.entry_liquidity_usd) 
                         / snap.entry_liquidity_usd) * 100
            else:
                change = 0

            status_emoji = "✅" if change > -20 else ("⚠️" if change > -50 else "🚨")
            lines.append(
                f"  {status_emoji} {addr[:8]}... "
                f"${snap.current_liquidity_usd:.0f} ({change:+.0f}%)"
            )

        return "\n".join(lines)
