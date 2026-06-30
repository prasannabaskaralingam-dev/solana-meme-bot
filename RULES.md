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

AJOUT : Quand deux systèmes/composants
communiquent entre eux (ex: callback A
envoie un message, fonction B le lit),
vérifier explicitement que le FORMAT
du message envoyé par l'un correspond
EXACTEMENT au format ATTENDU par
l'autre (ex: clés du dictionnaire/JSON
identiques). Tester ce point de jonction
AVANT de tester le résultat final global.

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

AJOUT : Une fréquence/intervalle de vérification
(ex: polling) qui était négligeable pour
un cas d'usage (ex: trades longs) peut
devenir critique si le cas d'usage change
(ex: trades courts) sans que personne
n'ajuste le réglage. Toujours réévaluer
les intervalles de surveillance quand la
durée typique d'une opération change.

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

AJOUT : Un test ne doit jamais valider seulement
le RÉSULTAT final (ex: 'vente exécutée'
= ✅) sans aussi valider le CHEMIN emprunté
(ex: quel composant exact a déclenché
l'action, et en combien de temps). Un
résultat correct obtenu par un chemin de
secours lent peut masquer un chemin
principal cassé (loi de Goodhart).

AJOUT — SIGNAL ORPHELIN :
Pour tout NOUVEAU signal/log/alerte
introduit par un fix ou un nouveau
composant (ex: log 'CRITICAL'), vérifier
explicitement et documenter :
(1) où ce signal est envoyé (Telegram
visible ? logs serveur seulement, donc
invisible ?)
(2) quelle ACTION concrète il déclenche
automatiquement.
Ne jamais supposer qu'un log critique
implique une réponse automatique — un
signal qui existe mais que personne
n'exploite (ni alerte visible, ni action)
est une régression silencieuse classique,
car la checklist R6 fixe ne couvre que
les chemins déjà connus au moment où elle
a été écrite, pas les nouveaux signaux
créés par un nouveau composant.

───────────────────────────────────────────

RÈGLE 7 — INVERSION (PAR COMPOSANT)
Qu'est-ce qui ferait ÉCHOUER ce composant
précisément ? Cherche les scénarios de
panne (double échec, état intermédiaire,
cache obsolète, changement d'état pendant
une opération) au lieu de vérifier
seulement le fonctionnement nominal.

S'applique de façon fractale : au système
entier ET à chaque composant individuel
zoomé — pas seulement à l'architecture
globale.

Exemples de questions Inversion :
- Double échec : que se passe-t-il si
  TOUTES les sources de données tombent ?
- État intermédiaire : existe-t-il un
  moment où le composant n'est ni dans
  l'état A ni dans l'état B ?
- Cache obsolète : un prix/état caché
  peut-il devenir dangereux si non
  rafraîchi ?
- Changement d'état : si l'état change
  PENDANT une opération, le composant
  le détecte-t-il ?

───────────────────────────────────────────

ORDRE D'APPLICATION DES RÈGLES :

1. Shannon (R1) — limites, entrées, sources
2. Entropie (R2) — dégradation 30 jours
3. Hormozi (R3) — maillon faible en premier
4. First Principles (R4/R5) — timeouts,
   séparation
5. Inversion (R7) — par composant, fractale
6. Changelog — avant tout déploiement

═══════════════════════════════════════════
