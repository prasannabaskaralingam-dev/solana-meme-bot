═══════════════════════════════════════════
RULES.md — RÈGLES OBLIGATOIRES AVANT TOUT
═══════════════════════════════════════════

RÈGLE 0 — ENVIRONNEMENT AVANT LE CODE
Avant de toucher au moindre fichier Python :

1. ps aux
   → Lister TOUS les processus actifs
   
2. systemctl list-units --user
   → Lister services utilisateur cachés
   
3. ss -tnp | grep 443
   → Identifier tout processus connecté
     à Telegram ou services externes
     
4. Si conflit détecté → résoudre AVANT
   de modifier le code
   
→ Ne jamais chercher un bug dans le code
  avant d'avoir vérifié l'environnement

───────────────────────────────────────────

RÈGLE 1 — SHANNON AVANT DE CODER
Pour chaque nouveau composant :

LIMITE   → Qu'est-ce qui peut bloquer ?
ENTRÉE   → Quelles données peuvent manquer ?
SOURCE   → Quel service externe peut tomber ?

→ Si une SOURCE tombe, le bot doit
  continuer à fonctionner seul

───────────────────────────────────────────

RÈGLE 2 — ENTROPIE (30 JOURS)
Avant chaque déploiement, demande-toi :

→ Qu'est-ce qui casse dans 30 jours ?
→ Comment je le détecte ?
→ Quelle maintenance minimum ?

Exemples à ne jamais répéter :
- Timeout manquant → freeze 311s
- Watchdog dans Telegram → surveillance morte
- OpenClaw fantôme → 409 Conflict 2h perdues

───────────────────────────────────────────

RÈGLE 3 — HORMOZI (MAILLON FAIBLE)
→ Coder le composant le plus dangereux
  EN PREMIER
→ Ce qui coûte du capital = priorité maximale
→ Ordre obligatoire :
  1. SL/TP/CB (protection capitale)
  2. Surveillance positions
  3. Trading
  4. Notifications Telegram

───────────────────────────────────────────

RÈGLE 4 — TIMEOUTS PARTOUT
Tout appel externe = timeout obligatoire :

DexScreener HTTP    → 10s
Bonding Curve RPC   → 10s
OnchainScorer RPC   → 500ms
RPC Security Check  → 500ms
execute_buy()       → 30s
execute_sell()      → 30s

→ Zéro appel externe sans timeout
→ Timeout manquant = bug critique

───────────────────────────────────────────

RÈGLE 5 — SÉPARATION ABSOLUE
Telegram = notifications uniquement

JAMAIS dans Telegram :
- Watchdog Capital
- Surveillance SL/TP/CB
- Vente d'urgence
- Logique de protection

TOUJOURS dans le trader principal :
- SL/TP/CB actifs même si Telegram mort
- Watchdog Capital indépendant
- Bot 100% autonome sans Telegram

───────────────────────────────────────────

RÈGLE 6 — TEST E2E OBLIGATOIRE
Après chaque déploiement :

1. Injection TEST_TOKEN_123 dans queue
2. Gate 1-7 logs [TEST] ✅/❌
3. execute_buy() DRY RUN
4. CircuitBreaker + capital_watchdog
5. execute_sell() TEST_TP +15%
6. record_trade() + /history + /today
7. Couper Telegram → vérifier SL/TP actifs
8. Redémarrer Telegram → pas de 409

Si un seul ❌ → corriger avant de
confirmer tâche terminée

═══════════════════════════════════════════
