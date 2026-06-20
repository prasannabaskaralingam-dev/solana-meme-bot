# Solana Meme Coin Tracker (Bot Telegram)

Un bot Telegram en Python pour monitorer les nouveaux meme coins, les pumps et les tokens trending sur la blockchain **Solana**.

Ce bot utilise l'API **DexScreener** (100% gratuite, sans clé API nécessaire) pour analyser les données on-chain en temps réel.

## 🌟 Fonctionnalités

- **Détection de Nouveaux Tokens** : Scanne les tokens récemment listés ou boostés sur Solana.
- **Analyse Automatique** : Filtre les tokens selon la liquidité, le market cap, le volume et l'évolution du prix.
- **Alertes "Pump"** : Vous notifie quand un token montre des signes de forte hausse (+10% en 5min, etc.).
- **Watchlist** : Ajoutez vos propres tokens pour les surveiller.
- **Trending & Narratives** : Consultez les tokens les plus boostés et les "metas" (narratives) du moment.
- **100% Gratuit** : Aucune API payante requise (DexScreener public API).

## 🚀 Installation Locale (Test)

1. Clonez ce dossier ou téléchargez les fichiers.
2. Installez les dépendances :
   ```bash
   pip install -r requirements.txt
   ```
3. Obtenez un token Telegram :
   - Ouvrez Telegram et cherchez `@BotFather`.
   - Envoyez `/newbot` et suivez les instructions.
   - Copiez le token fourni (ex: `123456789:ABCdefGHIjklmnoPQRstuvwxyz`).
4. Éditez `config.py` et remplacez `"VOTRE_TOKEN_ICI"` par votre token.
5. Lancez le bot :
   ```bash
   python bot.py
   ```
6. Allez sur Telegram, trouvez votre bot et envoyez `/start`.

## ⚙️ Configuration des Filtres

Ouvrez `config.py` pour ajuster les filtres d'alerte :

```python
FILTERS = {
    "min_liquidity_usd": 1000,       # Liquidité minimum ($)
    "min_volume_24h": 500,           # Volume 24h minimum ($)
    "min_market_cap": 1000,          # MC minimum
    "max_market_cap": 10_000_000,    # MC maximum (pour éviter le spam des gros tokens)
    "min_price_change_5m": 10,       # Hausse minimum sur 5min (%)
    "min_price_change_1h": 30,       # Hausse minimum sur 1h (%)
    "max_token_age_hours": 24,       # Âge maximum du token
    "min_buys_5m": 5,                # Achats minimum sur 5min
}
```

## ☁️ Déploiement Gratuit (24/7)

Pour que votre bot tourne 24h/24 sans laisser votre PC allumé, voici 2 méthodes 100% gratuites :

### Méthode 1 : Render.com (Le plus simple)

1. Créez un compte gratuit sur [Render.com](https://render.com).
2. Poussez votre code sur un dépôt GitHub privé.
3. Sur Render, cliquez sur **New > Web Service**.
4. Connectez votre dépôt GitHub.
5. Paramètres :
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `python bot.py`
6. Cliquez sur **Create Web Service**. Le bot tournera en permanence !

### Méthode 2 : Oracle Cloud Free Tier (VPS Gratuit à vie)

1. Créez un compte sur [Oracle Cloud](https://www.oracle.com/cloud/free/).
2. Créez une instance "Compute" (VM.Standard.E2.1.Micro, gratuit).
3. Connectez-vous en SSH à la machine (Ubuntu).
4. Installez Python et clonez le code :
   ```bash
   sudo apt update && sudo apt install python3-pip git screen -y
   git clone <votre_repo_github>
   cd solana-meme-bot
   pip3 install -r requirements.txt
   ```
5. Lancez le bot en arrière-plan avec `screen` :
   ```bash
   screen -S bot
   python3 bot.py
   ```
   *(Faites `Ctrl+A` puis `D` pour détacher l'écran et fermer le terminal, le bot continuera de tourner).*

## ⚠️ Avertissement (DYOR)

Ce bot est fourni à titre informatif. Les "Meme Coins" sont extrêmement volatils et risqués (risques de rug pull, honeypot, etc.). **Ne tradez jamais de l'argent que vous ne pouvez pas vous permettre de perdre.**
