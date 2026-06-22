# Helius WebSocket - Notes

## Endpoint
- `wss://mainnet.helius-rpc.com/?api-key=<API_KEY>`

## Méthodes utiles

### transactionSubscribe (Helius extension)
- Filtre par `accountInclude` (jusqu'à 50,000 adresses)
- Retourne les transactions en temps réel
- Commitment: processed, confirmed, finalized
- Encoding: jsonParsed
- Peut voir les token balances (preTokenBalances, postTokenBalances)

### accountSubscribe (standard Solana)
- Subscribe à un compte spécifique
- Notification quand lamports ou data changent
- Utile pour monitorer un token account

## Stratégie pour le bot
Pour monitorer les prix des positions SNIPER en temps réel :
1. Utiliser `transactionSubscribe` avec les pool addresses (Raydium/Pump)
2. OU utiliser `accountSubscribe` sur les pool accounts pour détecter les swaps
3. Calculer le prix à partir des balances de la pool

## Ping
- Envoyer un ping toutes les 30 secondes pour garder la connexion ouverte
