# SOLANA BOT — MAJOR FIXES & NEXT ACTIONS
## Dernière mise à jour : 2026-07-15

---

## ÉTAT ACTUEL

- **Mode** : DRY RUN actif (0.0493 SOL protégé)
- **Serveur** : Hetzner `/opt/solana-meme-bot/`
- **Repo** : `prasannabaskaralingam-dev/solana-meme-bot`
- **Problème actuel** : 0 trade depuis 15 jours
- **Cause racine identifiée** : onchain_scorer rejette TOUS les tokens pump.fun

---

## FIXES DÉJÀ APPLIQUÉS ✅

### Fix 1 — websockets v16.0 (30 juin 2026)
- `.closed` supprimé → crash toutes les 3s → SL aveugle → pertes -94%
- **Fix** : `_ws_is_open()` avec `websockets.protocol.State`
- **Fichier** : `price_monitor.py`

### Fix 2 — Gates 4 et 5 fantômes (5 juillet 2026)
- `top_holder_pct`, `unique_holders`, `volume_5m_usd` absents de `BondingCurveData`
- `hasattr()` retournait valeurs fictives → gates ne filtraient rien
- **Fix Gate 4** : Supprimée
- **Fix Gate 5** : Remplacée par `real_sol_reserves > 1 SOL`
- **Fichier** : `trading_bot.py` lignes ~3333-3354

### Fix 3 — R9 Dépendances figées
- `requirements.txt` : 44 librairies figées en `==`
- `UPGRADE.md` : procédure 5 étapes
- Alerte Telegram si 3 crashs en < 30s

---

## FIX EN COURS 🔧 — PRIORITÉ ABSOLUE

### Fix 4 — onchain_scorer.py : score impossible à passer

**Problème identifié (Shannon) :**
```
Ligne 155: score = 0
Ligne 156: score += 40   ← TOUJOURS appliqué
Ligne 157: if not lp_burned and not is_pumpfun:
Ligne 158:     score += 40  (ignoré pour pump.fun ✅)
Ligne 159: score += 20   ← TOUJOURS appliqué
Ligne 162: result.safe = score < 30

→ Score minimum pour tout token = 60
→ Seuil safe = score < 30
→ IMPOSSIBLE à atteindre
→ 100% des tokens rejetés
```

**Prochaine étape avant fix :**
```bash
sed -n '145,165p' /opt/solana-meme-bot/onchain_scorer.py
```
→ Comprendre ce que représentent les +40 et +20
→ Ensuite modifier le seuil ou la logique

**Fix probable :**
- Soit relever le seuil `score < 30` à `score < 65` pour tokens BC
- Soit revoir ce que les +40 et +20 représentent vraiment

---

## FIXES PLANIFIÉS (après Fix 4)

### Fix 5 — SmartEntry sur pipeline BC
- SmartEntry existe mais seulement sur pipeline DexScreener
- Pipeline BC achète immédiatement sans double vérification
- **Fix** : détecter → attendre 45s → reconfirmer → acheter
- **Fichier** : `trading_bot.py` fonction `ws_token_processor_job`

### Fix 6 — Signal 1 : Vitesse de progression BC
- Ajouter timestamp création token via Helius RPC
- Critère : `age_token < 10 minutes` dans Gate 5
- Logger l'âge en DRY RUN pour valider le seuil

### Fix 7 — Signal 2 : Volume réel 5 premières minutes
- Appel Helius `getSignaturesForAddress`
- Calculer volume réel en USD
- Logger en DRY RUN — seuil à déterminer par les données

### Fix 8 — Signal 3 : Wallets acheteurs distincts
- Appel Helius `getTokenLargestAccounts`
- Logger en DRY RUN — seuil à déterminer par les données

---

## ARCHITECTURE GATES (état actuel)

```
Gate 2 → MC > $40K (bonding curve)
Gate 3 → onchain_scorer (BLOQUE TOUT — Fix 4 en cours)
Gate 4 → SUPPRIMÉE (était fantôme)
Gate 5 → bonding_progress_pct > 40% ET real_sol_reserves > 1 SOL
Gate 6 → virtual_sol_reserves > 10 SOL
```

---

## ANALYSE FIRST PRINCIPLES — 4 SIGNAUX

| Signal | Statut | Fix |
|--------|--------|-----|
| 1 — Vitesse BC < 10 min | Analysé | Fix 6 |
| 2 — Volume réel 5 min | Analysé | Fix 7 |
| 3 — Wallets uniques | Analysé | Fix 8 |
| 4 — Momentum confirmé 2x | Analysé | Fix 5 (SmartEntry BC) |

**Pattern backtesting (7 juillet) :**
- LP:⚠️ ne prédit pas le rug
- Vrais gagnants : volume > $184K en 18h
- Critère discriminant = volume + momentum, pas LP burned

---

## WORKFLOW DE TRAVAIL

```
SHELLFISH → grep -n "mot clé" fichier.py  (trouver ligne)
GITHUB    → éditer + committer
HETZNER   → cd /opt/solana-meme-bot && git pull && systemctl restart solana-bot

RÈGLE : toujours taper cd /opt/solana-meme-bot en premier dans Shellfish
```

---

## COMMANDES UTILES

```bash
# État du bot
systemctl status solana-bot

# Logs récents
journalctl -u solana-bot --since "30 seconds ago"

# Tokens rejetés
grep "REJETÉ" /opt/solana-meme-bot/trading_bot.log | tail -20

# Vérifier une ligne de code
sed -n '145,165p' /opt/solana-meme-bot/onchain_scorer.py

# Après modification GitHub
cd /opt/solana-meme-bot && git pull && systemctl restart solana-bot
```

---

## BILAN HISTORIQUE

| Pipeline | Trades | WR | PnL moyen |
|----------|--------|----|-----------|
| BC direct | 40 | 0% | -51.4% |
| DexScreener + SmartEntry | 7 | 29% | -36.7% |
| **TOTAL** | **500** | **0.8%** | **-7.3%** |

**Gagnants** : Ansem's Army (+70.5%) et NIGGA (+51.2%) — pipeline DexScreener uniquement.
