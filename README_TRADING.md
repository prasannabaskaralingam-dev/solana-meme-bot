# 🤖 Solana Meme Coin Trading Bot

Un bot Telegram complet pour scanner, sniper et trader automatiquement les meme coins sur Solana.

## ⚠️ AVERTISSEMENT DE SÉCURITÉ (TRÈS IMPORTANT)

**LE TRADING DE MEME COINS EST EXTRÊMEMENT RISQUÉ. VOUS POUVEZ PERDRE 100% DE VOTRE CAPITAL EN QUELQUES SECONDES (RUG PULLS).**

1. **N'utilisez JAMAIS votre wallet principal (Phantom/Backpack) !**
2. **Créez un NOUVEAU wallet dédié uniquement à ce bot.**
3. **Ne déposez QUE ce que vous êtes prêt à perdre totalement.**
4. **La clé privée de ce wallet sera stockée sur le serveur (Render/Koyeb).**

---

## 🛠 Fonctionnalités

### 1. Stratégie Sniper (Risque Élevé)
- Détecte les nouveaux tokens lancés sur Solana (via DexScreener).
- Vérifie la liquidité minimum (>$5,000) et le Market Cap maximum (<$100,000).
- Achète automatiquement une petite position dès le lancement.

### 2. Stratégie Momentum (Risque Modéré)
- Surveille les tokens existants.
- Achète automatiquement si le token fait +15% en 5min ou +40% en 1h.
- Vérifie le volume (>$10k) et la pression acheteuse (ratio buy/sell > 3.0).

### 3. Gestion du Risque Automatique
- **Take Profit (TP)** : Vend automatiquement à +50% (configurable).
- **Stop Loss (SL)** : Vend automatiquement à -30% (configurable).
- **Trailing Stop** : Sécurise les gains (-20% depuis le plus haut).

---

## 🚀 Comment l'utiliser

### 1. Mettre à jour le bot sur Render
Poussez ces nouveaux fichiers sur votre dépôt GitHub :
- `trader.py` (Moteur de trading)
- `trading_bot.py` (Nouveau bot Telegram)
- `requirements.txt` (Mise à jour avec `solders`, `solana`)
- `Procfile` (Modifié pour lancer `trading_bot.py`)

### 2. Configurer le Wallet
Dans Telegram, envoyez :
`/wallet`
Le bot va générer un nouveau wallet Solana. Il vous donnera une adresse publique.
Envoyez des SOL sur cette adresse depuis votre Phantom ou Binance.

*(Optionnel : Si vous avez déjà créé un wallet vide sur Phantom, exportez la clé privée et envoyez `/import_wallet <clé_privée>` au bot).*

### 3. Configurer la Stratégie
Vérifiez la configuration avec `/config`.
Ajustez selon vos besoins :
- `/set_size 0.2` (Miser 0.2 SOL par trade)
- `/set_tp 100` (Take profit à +100%)
- `/set_sl 50` (Stop loss à -50%)

### 4. Lancer le Trading
Envoyez :
`/auto_on`

Le bot va maintenant scanner le marché toutes les 45 secondes et exécuter des trades automatiquement !

---

## 📋 Commandes Disponibles

### Trading Auto
- `/auto_on` : Activer le trading automatique
- `/auto_off` : Désactiver
- `/config` : Voir la configuration actuelle

### Wallet & Positions
- `/wallet` : Voir l'adresse du wallet
- `/balance` : Voir le solde en SOL
- `/positions` : Voir les trades en cours et le PnL
- `/history` : Historique des trades
- `/stats` : Statistiques (Win rate, PnL moyen)

### Trading Manuel
- `/buy <adresse>` : Acheter un token manuellement
- `/sell <adresse>` : Vendre un token manuellement
- `/sell_all` : Vendre TOUTES les positions ouvertes (bouton panique)
