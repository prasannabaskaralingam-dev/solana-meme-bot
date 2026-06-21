# Recherche - Trading Bot Solana

## Jupiter Swap API (gratuit avec rate limits)
- Quote: GET https://api.jup.ag/swap/v1/quote?inputMint=...&outputMint=...&amount=...&slippageBps=50
- Swap: POST https://api.jup.ag/swap/v1/swap (body: quoteResponse + userPublicKey)
- Retourne une transaction sérialisée à signer et envoyer
- Nécessite une API key (gratuite via developers.jup.ag)

## Flux de swap en Python:
1. Obtenir un quote via /quote
2. Obtenir la transaction sérialisée via /swap
3. Décoder la transaction (base64 -> VersionedTransaction)
4. Signer avec la keypair du wallet
5. Encoder et envoyer via RPC sendTransaction

## Code Python pour signer:
```python
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
import base64

raw_tx = VersionedTransaction.from_bytes(base64.b64decode(swap_transaction))
signature = keypair.sign_message(bytes(raw_tx.message))
signed_tx = VersionedTransaction.populate(raw_tx.message, [signature])
encoded_tx = base64.b64encode(bytes(signed_tx)).decode('utf-8')
```

## RPC Gratuits:
- Helius: free tier (gratuit, inscription sur helius.dev)
- QuickNode: free tier
- Solana public: https://api.mainnet-beta.solana.com (rate limited)

## Packages Python nécessaires:
- solders (pour keypair, transaction)
- solana (pour RPC client)
- requests/httpx (pour Jupiter API)
- base64, base58
