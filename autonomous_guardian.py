"""
autonomous_guardian.py — R5 SÉPARATION ABSOLUE
═══════════════════════════════════════════════

Boucle de protection autonome qui tourne INDÉPENDAMMENT de Telegram.
Si Telegram meurt, ce module continue de protéger le capital.

Responsabilités :
  1. SL/TP/CB via polling (fallback si WS down) — toutes les 3s
  2. Capital Watchdog (vente d'urgence si position non surveillée) — toutes les 5s
  3. Position Monitor non-sniper + LP Guard — toutes les 15s
  4. Flush des notifications WS (exécute les ventes en queue) — toutes les 3s

Notifications :
  - Toutes les notifications sont mises dans une queue centralisée
  - Un flush best-effort envoie via Telegram SI disponible
  - Si Telegram est mort → les notifications sont loguées (pas perdues)
"""

import asyncio
import logging
import time
import requests
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
# NOTIFICATION QUEUE — Centralisée, indépendante de Telegram
# ═══════════════════════════════════════════════════════════════

_notification_queue: list = []
_telegram_bot_token: str = ""
_telegram_subscribers: list = []


def configure_notifications(bot_token: str, subscribers: list):
    """Configurer les paramètres Telegram pour les notifications best-effort."""
    global _telegram_bot_token, _telegram_subscribers
    _telegram_bot_token = bot_token
    _telegram_subscribers = subscribers


def queue_notification(msg: str):
    """Ajouter une notification à la queue (thread-safe pour asyncio)."""
    global _notification_queue
    _notification_queue.append({
        "text": msg,
        "timestamp": time.time(),
    })
    # Limiter la queue à 100 messages max
    if len(_notification_queue) > 100:
        _notification_queue = _notification_queue[-50:]


def flush_notifications():
    """
    Envoyer les notifications en attente via Telegram (best-effort).
    Utilise requests.post directement (pas le Bot python-telegram-bot)
    pour être 100% indépendant du thread Telegram.
    """
    global _notification_queue
    if not _notification_queue or not _telegram_bot_token or not _telegram_subscribers:
        return
    # Copier et vider
    to_send = _notification_queue[:]
    _notification_queue = []
    url = f"https://api.telegram.org/bot{_telegram_bot_token}/sendMessage"
    for notif in to_send:
        for chat_id in _telegram_subscribers:
            try:
                requests.post(url, json={
                    "chat_id": chat_id,
                    "text": notif["text"],
                    "parse_mode": "Markdown",
                    "disable_web_page_preview": True,
                }, timeout=5)
            except Exception as e:
                logger.debug(f"[Guardian] Notification TG échouée (best-effort): {e}")


# ═══════════════════════════════════════════════════════════════
# AUTONOMOUS GUARDIAN LOOP
# ═══════════════════════════════════════════════════════════════

class AutonomousGuardian:
    """
    Boucle de protection autonome — R5 compliant.
    Tourne dans sa propre asyncio task, indépendante de Telegram.
    """

    def __init__(
        self,
        positions,
        trading_engine,
        circuit_breaker,
        capital_watchdog,
        price_monitor,
        helius_ws,
        api,  # DexScreenerAPI
        pnl_tracker,
        position_sizer,
        daily_pnl_guard,
        correlation_filter,
        liquidity_guard,
        post_trade_analyzer,
        sl_blacklist: dict,
        save_blacklist_fn,
        save_state_fn,
        log_trade_fn,
        update_sol_price_fn,
        ws_notification_queue: list,
        trading_config,
    ):
        self.positions = positions
        self.trading_engine = trading_engine
        self.circuit_breaker = circuit_breaker
        self.capital_watchdog = capital_watchdog
        self.price_monitor = price_monitor
        self.helius_ws = helius_ws
        self.api = api
        self.pnl_tracker = pnl_tracker
        self.position_sizer = position_sizer
        self.daily_pnl_guard = daily_pnl_guard
        self.correlation_filter = correlation_filter
        self.liquidity_guard = liquidity_guard
        self.post_trade_analyzer = post_trade_analyzer
        self.sl_blacklist = sl_blacklist
        self._save_blacklist = save_blacklist_fn
        self._save_state = save_state_fn
        self._log_trade = log_trade_fn
        self._update_sol_price = update_sol_price_fn
        self._ws_notification_queue = ws_notification_queue
        self.trading_config = trading_config
        # Compteurs pour les intervalles différents
        self._tick_count = 0
        self._sell_fail_counter: dict = {}
        self._running = False
        self._task = None
        # Stats
        self.stats = {
            "ticks": 0,
            "sells_executed": 0,
            "emergency_sells": 0,
            "errors": 0,
            "last_tick": 0,
        }

    async def start(self):
        """Démarrer la boucle de protection autonome."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._guardian_loop())
        logger.info("🛡️ [Guardian] Boucle de protection autonome DÉMARRÉE (R5)")

    async def stop(self):
        """Arrêter la boucle proprement."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("🛡️ [Guardian] Boucle de protection ARRÊTÉE")

    async def _guardian_loop(self):
        """
        Boucle principale — tourne toutes les 3s.
        Intervalles :
          - 3s : sniper SL/TP/CB + flush WS queue
          - 6s : watchdog capital (chaque 2 ticks)
          - 15s : position monitor non-sniper (chaque 5 ticks)
          - 30s : flush notifications Telegram (chaque 10 ticks)
        """
        logger.info("🛡️ [Guardian] Loop started — interval=3s")
        while self._running:
            try:
                self._tick_count += 1
                self.stats["ticks"] = self._tick_count
                self.stats["last_tick"] = time.time()

                # ━━━ CHAQUE TICK (3s) : Sniper SL/TP/CB + WS Queue ━━━
                await self._process_ws_sell_queue()
                await self._sniper_check()

                # ━━━ CHAQUE 2 TICKS (6s) : Watchdog Capital ━━━
                if self._tick_count % 2 == 0:
                    await self._watchdog_check()

                # ━━━ CHAQUE 5 TICKS (15s) : Position Monitor ━━━
                if self._tick_count % 5 == 0:
                    await self._position_monitor_check()

                # ━━━ CHAQUE 10 TICKS (30s) : Flush Notifications ━━━
                if self._tick_count % 10 == 0:
                    flush_notifications()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.stats["errors"] += 1
                logger.error(f"🛡️ [Guardian] Erreur tick #{self._tick_count}: {e}")

            await asyncio.sleep(3)

    # ─────────────────────────────────────────────────────────
    # PROCESS WS SELL QUEUE — Exécute les ventes en attente du HeliusWS
    # ─────────────────────────────────────────────────────────
    async def _process_ws_sell_queue(self):
        """
        Traite les items de la queue WS qui ont un 'action' (cb_action).
        FIX du bug: ces items n'avaient pas de 'type' et n'étaient jamais traités.
        """
        if not self._ws_notification_queue:
            return
        # Séparer les items 'action' (ventes à exécuter) des notifications
        to_process = []
        remaining = []
        for item in self._ws_notification_queue:
            if "action" in item and "token_address" in item:
                to_process.append(item)
            else:
                remaining.append(item)
        # Remettre les notifications normales dans la queue
        self._ws_notification_queue.clear()
        self._ws_notification_queue.extend(remaining)
        # Exécuter les ventes
        for item in to_process:
            token_address = item["token_address"]
            cb_action = item["action"]
            price = item.get("price", 0)
            if token_address not in self.positions.positions:
                continue
            pos = self.positions.positions[token_address]
            try:
                is_partial = cb_action.rule == "partial_take_profit"
                sell_pct = 50.0 if is_partial else 100.0
                result = self.trading_engine.execute_sell(pos, cb_action.reason, sell_pct=sell_pct)
                if result:
                    self.stats["sells_executed"] += 1
                    self._log_trade(
                        token_address=pos.token_address,
                        token_symbol=pos.token_symbol,
                        strategy=pos.strategy,
                        side="SELL",
                        pnl_pct=pos.pnl_pct,
                        reason=cb_action.reason,
                        price=price,
                        amount_sol=pos.amount_sol_invested,
                    )
                    if not is_partial:
                        self._cleanup_after_sell(pos, cb_action.reason)
                    # Notification
                    pnl_emoji = '✅' if pos.pnl_pct > 0 else '❌'
                    source = "Partial TP 50%" if is_partial else "VENTE WS"
                    sim = "🧪 [SIMULATION] " if self.trading_engine.config.dry_run else ""
                    msg = f"{sim}{pnl_emoji} *{source}*\n\n"
                    msg += f"🪙 {pos.token_name} (${pos.token_symbol})\n"
                    msg += f"📈 PnL: {pos.pnl_pct:+.1f}%\n"
                    msg += f"📝 Raison: {cb_action.reason}\n"
                    msg += f"⚡ Source: Guardian autonome"
                    if result.get("tx_signature"):
                        msg += f"\n🔗 [TX](https://solscan.io/tx/{result['tx_signature']})"
                    queue_notification(msg)
            except Exception as e:
                logger.error(f"🛡️ [Guardian] Erreur vente WS queue {token_address[:12]}: {e}")

    # ─────────────────────────────────────────────────────────
    # SNIPER CHECK — SL/TP/CB pour positions sniper (3s)
    # ─────────────────────────────────────────────────────────
    async def _sniper_check(self):
        """Vérifier les positions sniper via polling (fallback si WS down)."""
        if not self.trading_engine or not self.circuit_breaker:
            return
        sniper_positions = [p for p in self.positions.get_open_positions() if p.strategy == "sniper"]
        if not sniper_positions:
            return
        ws_connected = self.price_monitor and self.price_monitor.is_connected
        ws_monitored = set(self.price_monitor.monitored_pools.keys()) if self.price_monitor else set()
        for pos in sniper_positions:
            try:
                # SKIP si le WS gère déjà ce token
                if ws_connected and pos.token_address in ws_monitored:
                    cb_action = self.circuit_breaker.check(pos.token_address, pos.current_price)
                    if cb_action.should_sell and cb_action.rule == "time_stop":
                        pass  # Time Stop ne dépend pas du prix
                    else:
                        continue
                # Récupérer le prix
                current_price = 0.0
                if self.helius_ws and self.helius_ws.is_connected:
                    current_price = self.helius_ws.get_price(pos.token_address)
                if current_price <= 0:
                    analysis = self.api.analyze_token(pos.token_address)
                    current_price = float(analysis.get("price_usd", 0) or 0) if analysis else 0
                    if analysis:
                        sol_price = analysis.get("sol_price_usd", 0)
                        if not sol_price:
                            price_native = float(analysis.get("price_native", 0) or 0)
                            price_usd_val = float(analysis.get("price_usd", 0) or 0)
                            if price_native > 0 and price_usd_val > 0:
                                sol_price = price_usd_val / price_native
                        if sol_price and sol_price > 50:
                            self._update_sol_price(sol_price)
                if current_price <= 0:
                    current_price = pos.current_price
                    if current_price <= 0:
                        current_price = pos.entry_price_usd * 0.01
                else:
                    self.positions.update_position(pos.token_address, current_price)
                # Heartbeat watchdog
                if self.capital_watchdog:
                    self.capital_watchdog.heartbeat(pos.token_address, current_price)
                # Token mort check
                if self.capital_watchdog and pos.token_address in self.capital_watchdog.positions:
                    wd_pos = self.capital_watchdog.positions[pos.token_address]
                    if getattr(wd_pos, 'is_dead', False):
                        logger.warning(f"[Guardian] Zombie forcé: {pos.token_symbol}")
                        result = self.trading_engine.execute_sell(pos, "💀 Token mort — prix=0 depuis 5min")
                        if result:
                            self.stats["sells_executed"] += 1
                            self._log_trade(
                                token_address=pos.token_address,
                                token_symbol=pos.token_symbol,
                                strategy=pos.strategy,
                                side="SELL", pnl_pct=-100.0,
                                reason="Token mort (prix=0 depuis 5min)",
                                price=0, amount_sol=pos.amount_sol_invested,
                            )
                            self._cleanup_after_sell(pos, "Token mort")
                            queue_notification(f"💀 *TOKEN MORT*\n🪙 {pos.token_name} (${pos.token_symbol})")
                        continue
                # CircuitBreaker check
                cb_action = self.circuit_breaker.check(pos.token_address, current_price)
                if cb_action.should_sell:
                    is_partial = cb_action.rule == "partial_take_profit"
                    sell_pct = 50.0 if is_partial else 100.0
                    result = self.trading_engine.execute_sell(pos, cb_action.reason, sell_pct=sell_pct)
                    if not result:
                        self._sell_fail_counter[pos.token_address] = self._sell_fail_counter.get(pos.token_address, 0) + 1
                        fails = self._sell_fail_counter[pos.token_address]
                        if fails >= 3:
                            self._force_close_dead_token(pos)
                        continue
                    self.stats["sells_executed"] += 1
                    logger.info(f"⚡ [SOURCE: Guardian-Sniper] Vente: {pos.token_symbol} "
                               f"PnL={pos.pnl_pct:+.1f}% | {cb_action.reason}")
                    self._log_trade(
                        token_address=pos.token_address,
                        token_symbol=pos.token_symbol,
                        strategy=pos.strategy,
                        side="SELL", pnl_pct=pos.pnl_pct,
                        reason=cb_action.reason,
                        price=current_price, amount_sol=pos.amount_sol_invested,
                        source="Guardian-Sniper",
                    )
                    if not is_partial:
                        self._cleanup_after_sell(pos, cb_action.reason)
                    # Notification
                    pnl_emoji = '✅' if pos.pnl_pct > 0 else '❌'
                    action_type = "PARTIAL TP 50%" if is_partial else "VENTE"
                    msg = f"{pnl_emoji} *{action_type} (Guardian)*\n\n"
                    msg += f"🪙 {pos.token_name} (${pos.token_symbol})\n"
                    msg += f"📈 PnL: {pos.pnl_pct:+.1f}%\n"
                    msg += f"📝 Raison: {cb_action.reason}"
                    if result.get("tx_signature"):
                        msg += f"\n🔗 [TX](https://solscan.io/tx/{result['tx_signature']})"
                    queue_notification(msg)
            except Exception as e:
                logger.error(f"🛡️ [Guardian] Erreur sniper {pos.token_symbol}: {e}")

    # ─────────────────────────────────────────────────────────
    # WATCHDOG CHECK — Vente d'urgence si position non surveillée (6s)
    # ─────────────────────────────────────────────────────────
    async def _watchdog_check(self):
        """Vérifier que chaque position est activement surveillée."""
        if not self.capital_watchdog:
            return
        alerts = self.capital_watchdog.check()
        if not alerts:
            return
        for alert in alerts:
            level_emoji = {"warn": "⚠️", "critical": "🚨", "emergency": "💀"}.get(alert["level"], "❓")
            msg = f"{level_emoji} *WATCHDOG CAPITAL*\n\n{alert['message']}"
            queue_notification(msg)
            # VENTE D'URGENCE si emergency
            if alert["action"] == "emergency_sell" and alert["token_address"] != "GLOBAL":
                token_addr = alert["token_address"]
                if token_addr in self.positions.positions:
                    pos = self.positions.positions[token_addr]
                    try:
                        result = self.trading_engine.execute_sell(
                            pos, f"💀 VENTE D'URGENCE WATCHDOG (non surveillé {alert['gap_seconds']:.0f}s)"
                        )
                        if result:
                            self.stats["emergency_sells"] += 1
                            self.stats["sells_executed"] += 1
                            self._cleanup_after_sell(pos, "Watchdog emergency")
                            sell_msg = f"💀 *VENTE D'URGENCE*\n\n"
                            sell_msg += f"🪙 {pos.token_name} (${pos.token_symbol})\n"
                            sell_msg += f"📈 PnL: {pos.pnl_pct:+.1f}%\n"
                            sell_msg += f"📝 Non surveillé depuis {alert['gap_seconds']:.0f}s"
                            if result.get("tx_signature"):
                                sell_msg += f"\n🔗 [TX](https://solscan.io/tx/{result['tx_signature']})"
                            queue_notification(sell_msg)
                    except Exception as e:
                        logger.error(f"[Guardian] Erreur vente urgence {token_addr[:12]}: {e}")

    # ─────────────────────────────────────────────────────────
    # POSITION MONITOR — Non-sniper + LP Guard (15s)
    # ─────────────────────────────────────────────────────────
    async def _position_monitor_check(self):
        """Vérifier les positions non-sniper + LP Guard."""
        if not self.trading_engine:
            return
        # Non-sniper positions
        non_sniper = [p for p in self.positions.get_open_positions() if p.strategy != "sniper"]
        for pos in non_sniper:
            try:
                analysis = self.api.analyze_token(pos.token_address)
                current_price = float(analysis.get("price_usd", 0) or 0) if analysis else 0
                if current_price <= 0:
                    current_price = pos.current_price
                    if current_price <= 0:
                        current_price = pos.entry_price_usd * 0.01
                else:
                    self.positions.update_position(pos.token_address, current_price)
                if self.capital_watchdog:
                    self.capital_watchdog.heartbeat(pos.token_address, current_price)
                if not self.circuit_breaker:
                    continue
                cb_action = self.circuit_breaker.check(pos.token_address, current_price)
                if cb_action.should_sell:
                    result = self.trading_engine.execute_sell(pos, cb_action.reason)
                    if result:
                        self.stats["sells_executed"] += 1
                        self._log_trade(
                            token_address=pos.token_address,
                            token_symbol=pos.token_symbol,
                            strategy=pos.strategy,
                            side="SELL", pnl_pct=pos.pnl_pct,
                            reason=cb_action.reason,
                            price=current_price, amount_sol=pos.amount_sol_invested,
                        )
                        self._cleanup_after_sell(pos, cb_action.reason)
                        pnl_emoji = '✅' if pos.pnl_pct > 0 else '❌'
                        sim = "🧪 [SIMULATION] " if self.trading_engine.config.dry_run else ""
                        msg = f"{sim}{pnl_emoji} *VENTE (Guardian)*\n\n"
                        msg += f"🪙 {pos.token_name} (${pos.token_symbol})\n"
                        msg += f"📈 PnL: {pos.pnl_pct:+.1f}%\n"
                        msg += f"📝 Raison: {cb_action.reason}"
                        queue_notification(msg)
            except Exception as e:
                logger.error(f"[Guardian] Erreur position {pos.token_symbol}: {e}")
        # LP Guard (anti-rug)
        await self._lp_guard_check()

    async def _lp_guard_check(self):
        """Vérifier la liquidité des positions (anti-rug pull)."""
        if not self.liquidity_guard or not self.liquidity_guard.snapshots:
            return
        try:
            from trader import JupiterSwap
            lp_actions = self.liquidity_guard.check_all_positions()
            for action in lp_actions:
                token_addr = action["token"]
                pos = self.positions.get_position(token_addr)
                if not pos:
                    continue
                if action["type"] == "emergency_sell":
                    logger.warning(f"🚨 [Guardian] VENTE D'URGENCE LP: {pos.token_name}")
                    raw_amount = int(pos.amount_tokens)
                    tx_sig = self.liquidity_guard.emergency_sell_with_retry(
                        self.trading_engine.swap_engine, pos.token_address, raw_amount
                    )
                    if tx_sig:
                        self.stats["emergency_sells"] += 1
                        self.positions.close_position(token_addr)
                        if self.correlation_filter:
                            self.correlation_filter.unregister_position(token_addr)
                        self.liquidity_guard.unregister_position(token_addr)
                        self.sl_blacklist[token_addr] = time.time() + 86400
                        self._save_blacklist()
                        msg = f"🚨 *VENTE D'URGENCE (Rug Pull)*\n\n"
                        msg += f"🪙 {pos.token_name} (${pos.token_symbol})\n"
                        msg += f"💧 LP effondrée: {action['drop_pct']:.0f}%\n"
                        msg += f"🔗 [TX](https://solscan.io/tx/{tx_sig})"
                        queue_notification(msg)
        except Exception as e:
            logger.error(f"[Guardian] Erreur LP Guard: {e}")

    # ─────────────────────────────────────────────────────────
    # HELPERS
    # ─────────────────────────────────────────────────────────
    def _cleanup_after_sell(self, pos, reason: str):
        """Cleanup complet après une vente totale."""
        try:
            if self.pnl_tracker:
                self.pnl_tracker.record_trade(
                    token_address=pos.token_address,
                    token_symbol=pos.token_symbol,
                    strategy=pos.strategy,
                    entry_time=pos.entry_time,
                    amount_sol=pos.amount_sol_invested,
                    pnl_pct=pos.pnl_pct,
                    exit_reason=reason,
                )
            if self.position_sizer:
                self.position_sizer.record_result(pos.pnl_pct > 0)
                self._save_state()
            if self.daily_pnl_guard:
                pnl_sol = pos.amount_sol_invested * (pos.pnl_pct / 100.0)
                is_sl = "stop_loss" in reason.lower() or "sl" in reason.lower()
                self.daily_pnl_guard.record_trade(pnl_sol, is_stop_loss=is_sl)
            if self.correlation_filter:
                self.correlation_filter.unregister_position(pos.token_address)
            if self.liquidity_guard:
                self.liquidity_guard.unregister_position(pos.token_address)
            if self.circuit_breaker:
                self.circuit_breaker.close_position(pos.token_address)
            if self.capital_watchdog:
                self.capital_watchdog.unregister_position(pos.token_address)
            if self.post_trade_analyzer:
                self.post_trade_analyzer.record_trade_exit(
                    token_address=pos.token_address,
                    token_symbol=pos.token_symbol,
                    token_name=pos.token_name,
                    strategy=pos.strategy,
                    entry_time=pos.entry_time,
                    entry_price=pos.entry_price_usd,
                    exit_price=pos.current_price,
                    exit_pnl_pct=pos.pnl_pct,
                    exit_reason=reason,
                    highest_price=pos.highest_price,
                    amount_sol=pos.amount_sol_invested,
                )
            if self.price_monitor:
                try:
                    asyncio.create_task(self.price_monitor.remove_token(pos.token_address))
                except Exception:
                    pass
            if self.helius_ws:
                self.helius_ws.unwatch_token(pos.token_address)
            # Blacklister si perte
            if pos.pnl_pct < 0:
                self.sl_blacklist[pos.token_address] = time.time() + 86400
                self._save_blacklist()
            # Fermer la position
            self.positions.close_position(pos.token_address)
            # Reset fail counter
            self._sell_fail_counter.pop(pos.token_address, None)
        except Exception as e:
            logger.error(f"[Guardian] Erreur cleanup {pos.token_address[:12]}: {e}")

    def _force_close_dead_token(self, pos):
        """Fermer une position morte (3 échecs de vente)."""
        logger.warning(f"[Guardian] 🗑️ {pos.token_symbol}: 3 échecs → fermeture forcée")
        self._log_trade(
            token_address=pos.token_address,
            token_symbol=pos.token_symbol,
            strategy=pos.strategy,
            side="SELL", pnl_pct=-100.0,
            reason="Token mort (3 échecs vente)",
            price=0, amount_sol=pos.amount_sol_invested,
        )
        self._cleanup_after_sell(pos, "Token mort (3 échecs vente)")
        queue_notification(
            f"🗑️ *TOKEN MORT*\n🪙 {pos.token_name} (${pos.token_symbol})\n"
            f"📉 3 tentatives de vente échouées\n💰 Perte: -{pos.amount_sol_invested:.4f} SOL"
        )
