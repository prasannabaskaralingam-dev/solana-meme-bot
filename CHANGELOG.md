# CHANGELOG — Solana Meme Bot

Chaque entrée inclut une commande `grep` de vérification pour détecter les régressions futures.

---

## 30 juin 2025

### FIX: HeliusWS exécute la vente directement (commit `8509efd`)

Le callback `_ws_on_price_update` mettait les signaux en queue avec `action: cb_action`. Le Guardian traitait la queue toutes les 3s. Pendant ce délai, le prix pouvait chuter de -25% à -94%. Le fix fait exécuter la vente directement dans le callback HeliusWS (temps réel).

```bash
# Vérif: vente directe dans _ws_on_price_update
grep -n "SOURCE: HeliusWS" trading_bot.py
grep -n "execute_sell" trading_bot.py | grep "_ws_on_price_update" -A5
```

### FIX: Traçabilité SOURCE dans les logs et trades.json

Ajout du champ `source` dans `_log_trade()` pour identifier quel composant déclenche chaque vente : HeliusWS, PriceMonitor, Guardian-Sniper, ou Guardian-Poll.

```bash
# Vérif: paramètre source dans _log_trade
grep -n "source=" trading_bot.py | grep "_log_trade"
grep -n "SOURCE: HeliusWS\|SOURCE: PriceMonitor\|SOURCE: Guardian" trading_bot.py
grep -n "SOURCE: Guardian-Sniper" autonomous_guardian.py
```

### FIX: Gate 2 BC réduit de $80K à $40K (commit `e552a23`)

Les tokens bonding curve ont un MC max de $60-70K avant migration Raydium. Gate 2 à $80K bloquait 100% des tokens BC. Réduit à $40K pour le pipeline BC uniquement.

```bash
# Vérif: Gate 2 BC à $40K
grep -n "40_000\|40000" trading_bot.py | grep -i "mc\|gate\|min"
```

### DOC: RULES.md mis à jour (R1 Shannon, R2 Entropie, R6 E2E)

Ajout de 3 précisions aux règles existantes concernant la vérification des formats de jonction, la réévaluation des intervalles, et la validation du chemin emprunté.

```bash
# Vérif: ajouts R1, R2, R6
grep -n "FORMAT.*message\|format.*attendu" RULES.md
grep -n "intervalles.*surveillance\|réévaluer" RULES.md
grep -n "CHEMIN.*emprunté\|Goodhart" RULES.md
```

---

## 29 juin 2025

### FIX: Guardian R5 indépendant de Telegram

Le Guardian tourne dans une asyncio task séparée. Si Telegram meurt, le Guardian continue de protéger le capital (SL/TP/CB toutes les 3s).

```bash
# Vérif: Guardian autonome
grep -n "class.*Guardian" autonomous_guardian.py
grep -n "asyncio.sleep(3)" autonomous_guardian.py
grep -n "DÉMARRÉE\|Loop started" autonomous_guardian.py
```

### FIX: drop_pending_updates=True

Évite que le bot traite des commandes Telegram obsolètes au redémarrage (qui pouvaient déclencher des actions non voulues).

```bash
# Vérif: drop_pending_updates
grep -n "drop_pending_updates" trading_bot.py
```

---

## 28 juin 2025

### FIX: Timeouts partout (DexScreener 10s, RPC 10s, execute_buy/sell 30s)

Ajout de timeouts explicites sur tous les appels réseau pour éviter les blocages infinis qui empêchaient le Guardian de tourner.

```bash
# Vérif: timeouts
grep -rn "timeout=" trading_bot.py | grep -c "timeout"
grep -rn "timeout=" trader.py | grep -c "timeout"
grep -rn "wait_for.*timeout\|asyncio.wait_for" trading_bot.py
```

---

## 27 juin 2025

### FIX: sniper_monitor_job interval=3s (maintenant dans Guardian)

Surveillance des positions sniper toutes les 3s avec cb.check() pour détecter SL/TP/TS rapidement. Initialement dans `job_queue.run_repeating(sniper_monitor_job, interval=3)`, maintenant migré dans `AutonomousGuardian._sniper_check()` (appelé à chaque tick de 3s).

```bash
# Vérif: interval 3s dans Guardian
grep -n "sleep(3)" autonomous_guardian.py
grep -n "interval=3s" autonomous_guardian.py
grep -n "_sniper_check" autonomous_guardian.py
```

### FIX: SL Universel -25% en première position dans cb.check()

Le Stop Loss universel est la PREMIÈRE règle vérifiée dans `CircuitBreaker.check()`, avant Time Stop, Trailing Stop, et Take Profit. Garantit que le SL est toujours appliqué en priorité absolue.

```bash
# Vérif: SL universel en première position
grep -n "RÈGLE 1.*SL Universel" circuit_breaker.py
grep -n "stop_loss_pct.*=.*-25" circuit_breaker.py
sed -n '220,235p' circuit_breaker.py
```

---

## Script de vérification rapide (anti-régression)

```bash
#!/bin/bash
# Exécuter depuis /opt/solana-meme-bot/
echo "=== VÉRIFICATION ANTI-RÉGRESSION ==="
echo ""
echo "1. Guardian 3s:"
grep -c "asyncio.sleep(3)" autonomous_guardian.py && echo "  ✅ OK" || echo "  ❌ RÉGRESSION"
echo ""
echo "2. SL Universel -25% (première règle):"
grep -c "RÈGLE 1.*SL Universel" circuit_breaker.py && echo "  ✅ OK" || echo "  ❌ RÉGRESSION"
echo ""
echo "3. HeliusWS vente directe:"
grep -c "SOURCE: HeliusWS" trading_bot.py && echo "  ✅ OK" || echo "  ❌ RÉGRESSION"
echo ""
echo "4. drop_pending_updates:"
grep -c "drop_pending_updates" trading_bot.py && echo "  ✅ OK" || echo "  ❌ RÉGRESSION"
echo ""
echo "5. Guardian autonome:"
grep -c "class.*Guardian" autonomous_guardian.py && echo "  ✅ OK" || echo "  ❌ RÉGRESSION"
echo ""
echo "6. Gate 2 BC $40K:"
grep -c "40_000" trading_bot.py && echo "  ✅ OK" || echo "  ❌ RÉGRESSION"
```
