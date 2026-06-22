"""
Module de Trading - Solana Meme Coin Sniper & Momentum Bot
Exécute des achats/ventes automatiques via Jupiter Swap API.
"""

import os
import json
import time
import base64
import base58
import logging
import httpx
from typing import Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime

from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from postmortem_tracker import start_postmortem_thread, init_db as init_postmortem_db

logger = logging.getLogger(__name__)

# Répertoire de base (même dossier que le script)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Répertoire persistant (Render Disk ou fallback local)
# Si PERSISTENT_DATA_DIR est défini, on l'utilise pour stocker les données
# Sinon on utilise le répertoire du script (non persistant sur Render)
DATA_DIR = os.environ.get("PERSISTENT_DATA_DIR", BASE_DIR)
os.makedirs(DATA_DIR, exist_ok=True)


# ============================================================
# CONFIGURATION TRADING
# ============================================================

@dataclass
class TradingConfig:
    """Configuration de la stratégie de trading"""

    # Budget et position
    max_budget_sol: float = 6.5          # ~1000 CHF en SOL (ajuster selon le cours)
    position_size_sol: float = 0.05      # Taille d'une position (SOL par trade)
    max_open_positions: int = 10         # Nombre max de positions ouvertes

    # Stratégie Sniper (nouveaux tokens)
    sniper_enabled: bool = True
    sniper_position_sol: float = 0.05    # Montant par snipe
    sniper_min_liquidity: float = 5000   # Liquidité min pour sniper ($)
    sniper_max_mc: float = 100_000       # MC max pour sniper ($)


    # Take Profit & Stop Loss
    take_profit_pct: float = 20.0        # Vendre quand +20%
    stop_loss_pct: float = -25.0         # RÈGLE 2: SL universel -25% (évite les -100%)
    trailing_stop_pct: float = 10.0      # RÈGLE 3: SL à -10% du max
    trailing_activation_pct: float = 15.0 # RÈGLE 3: Trailing dès +15% atteint

    # Time Stop (sortie forcée si pas de profit après X minutes)
    time_stop_enabled: bool = True       # RÈGLE 1: Time Stop actif
    time_stop_minutes: float = 15.0      # RÈGLE 1: 15 min max
    time_stop_min_profit: float = 20.0   # RÈGLE 1: sortie si pas +20% après 15 min

    # Momentum Stop (DÉSACTIVÉ - redondant avec Trailing Stop)
    momentum_stop_enabled: bool = False
    momentum_stop_drop_pct: float = 15.0  # Chute depuis ATH local (%)
    momentum_stop_volume_drop: float = 50.0  # Volume doit chuter de 50%+

    # Sécurité
    slippage_bps: int = 500              # 5% slippage max (meme coins = volatile)
    max_retries: int = 3                 # Nombre de tentatives par transaction
    cooldown_seconds: int = 10           # Attente entre les trades

    # RPC & API
    rpc_url: str = ""
    # lite-api.jup.ag est GRATUIT (pas besoin d'API key)
    jupiter_api_url: str = "https://lite-api.jup.ag"


@dataclass
class Position:
    """Représente une position ouverte"""
    token_address: str
    token_name: str
    token_symbol: str
    entry_price_usd: float
    amount_sol_invested: float
    amount_tokens: float
    entry_time: str
    highest_price: float = 0.0
    current_price: float = 0.0
    pnl_pct: float = 0.0
    strategy: str = "sniper"  # Stratégie unique: sniper

    def to_dict(self) -> dict:
        return {
            "token_address": self.token_address,
            "token_name": self.token_name,
            "token_symbol": self.token_symbol,
            "entry_price_usd": self.entry_price_usd,
            "amount_sol_invested": self.amount_sol_invested,
            "amount_tokens": self.amount_tokens,
            "entry_time": self.entry_time,
            "highest_price": self.highest_price,
            "current_price": self.current_price,
            "pnl_pct": self.pnl_pct,
            "strategy": self.strategy,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Position":
        return cls(**data)


# ============================================================
# WALLET MANAGER
# ============================================================

class WalletManager:
    """Gestion du wallet Solana (keypair)"""

    WALLET_FILE = os.path.join(DATA_DIR, "wallet.json")
    SOL_MINT = "So11111111111111111111111111111111111111112"

    def __init__(self, rpc_url: str):
        self.rpc_url = rpc_url or "https://api.mainnet-beta.solana.com"
        self.keypair: Optional[Keypair] = None
        self.public_key: str = ""

    def load_or_create_wallet(self) -> str:
        """Charger un wallet existant ou en créer un nouveau"""
        # Priorité 1: Variable d'environnement (persistée sur Render)
        env_key = os.environ.get("WALLET_PRIVATE_KEY", "").strip()
        if env_key:
            try:
                return self.import_wallet(env_key)
            except Exception as e:
                logger.error(f"Erreur chargement wallet depuis env: {e}")
        # Priorité 2: Fichier local
        if os.path.exists(self.WALLET_FILE):
            return self._load_wallet()
        else:
            return self._create_wallet()

    def import_wallet(self, private_key: str) -> str:
        """Importer un wallet depuis une clé privée (base58)"""
        try:
            key_bytes = base58.b58decode(private_key)
            self.keypair = Keypair.from_bytes(key_bytes)
            self.public_key = str(self.keypair.pubkey())
            self._save_wallet()
            logger.info(f"Wallet importé: {self.public_key}")
            return self.public_key
        except Exception as e:
            logger.error(f"Erreur import wallet: {e}")
            raise ValueError(f"Clé privée invalide: {e}")

    def _create_wallet(self) -> str:
        """Créer un nouveau wallet"""
        self.keypair = Keypair()
        self.public_key = str(self.keypair.pubkey())
        self._save_wallet()
        logger.info(f"Nouveau wallet créé: {self.public_key}")
        return self.public_key

    def _load_wallet(self) -> str:
        """Charger le wallet depuis le fichier"""
        with open(self.WALLET_FILE, "r") as f:
            data = json.load(f)
        key_bytes = bytes(data["keypair"])
        self.keypair = Keypair.from_bytes(key_bytes)
        self.public_key = str(self.keypair.pubkey())
        logger.info(f"Wallet chargé: {self.public_key}")
        return self.public_key

    def _save_wallet(self):
        """Sauvegarder le wallet (chiffré en production !)"""
        data = {
            "keypair": list(bytes(self.keypair)),
            "public_key": self.public_key,
        }
        with open(self.WALLET_FILE, "w") as f:
            json.dump(data, f)

    def get_sol_balance(self) -> float:
        """Récupérer le solde SOL du wallet"""
        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getBalance",
                "params": [self.public_key]
            }
            response = httpx.post(self.rpc_url, json=payload, timeout=10)
            result = response.json()
            lamports = result.get("result", {}).get("value", 0)
            return lamports / 1_000_000_000  # Lamports -> SOL
        except Exception as e:
            logger.error(f"Erreur getBalance: {e}")
            return 0.0

    def get_token_balance(self, token_mint: str) -> Tuple[float, int]:
        """Récupérer le solde d'un token SPL - retourne (uiAmount, rawAmount)"""
        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTokenAccountsByOwner",
                "params": [
                    self.public_key,
                    {"mint": token_mint},
                    {"encoding": "jsonParsed"}
                ]
            }
            response = httpx.post(self.rpc_url, json=payload, timeout=10)
            result = response.json()
            accounts = result.get("result", {}).get("value", [])
            if not accounts:
                return 0.0, 0
            token_amount = accounts[0]["account"]["data"]["parsed"]["info"]["tokenAmount"]
            ui_amount = float(token_amount.get("uiAmount") or 0)
            raw_amount = int(token_amount.get("amount", "0"))
            return ui_amount, raw_amount
        except Exception as e:
            logger.error(f"Erreur getTokenBalance: {e}")
            return 0.0, 0

    def get_all_token_balances(self) -> list[dict]:
        """Scanner tous les tokens détenus dans le wallet (SPL + Token-2022)"""
        all_tokens = []
        # Scanner les deux programmes de tokens
        programs = [
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",   # SPL Token
            "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",   # Token-2022
        ]
        for program_id in programs:
            try:
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTokenAccountsByOwner",
                    "params": [
                        self.public_key,
                        {"programId": program_id},
                        {"encoding": "jsonParsed"}
                    ]
                }
                response = httpx.post(self.rpc_url, json=payload, timeout=15)
                result = response.json()
                accounts = result.get("result", {}).get("value", [])
                for account in accounts:
                    info = account["account"]["data"]["parsed"]["info"]
                    mint = info["mint"]
                    token_amount = info["tokenAmount"]
                    ui_amount = float(token_amount.get("uiAmount") or 0)
                    raw_amount = int(token_amount.get("amount", "0"))
                    if raw_amount > 0:
                        all_tokens.append({
                            "mint": mint,
                            "ui_amount": ui_amount,
                            "raw_amount": raw_amount,
                            "decimals": token_amount.get("decimals", 6),
                        })
            except Exception as e:
                logger.error(f"Erreur scan tokens ({program_id[:8]}...): {e}")
        logger.info(f"Wallet scan: {len(all_tokens)} tokens avec solde > 0")
        return all_tokens

    def export_private_key(self) -> str:
        """Exporter la clé privée en base58 (ATTENTION: sensible !)"""
        if self.keypair:
            return base58.b58encode(bytes(self.keypair)).decode()
        return ""


# ============================================================
# JUPITER SWAP ENGINE
# ============================================================

class JupiterSwap:
    """Moteur de swap via Jupiter API (lite-api.jup.ag = gratuit, pas de clé API)"""

    def __init__(self, wallet: WalletManager, config: TradingConfig):
        self.wallet = wallet
        self.config = config
        self.base_url = config.jupiter_api_url
        self.client = httpx.Client(timeout=30)

    def get_quote(self, input_mint: str, output_mint: str, amount: int) -> Optional[dict]:
        """Obtenir un devis de swap via Jupiter"""
        try:
            url = f"{self.base_url}/swap/v1/quote"
            params = {
                "inputMint": input_mint,
                "outputMint": output_mint,
                "amount": str(amount),
                "slippageBps": self.config.slippage_bps,
                "restrictIntermediateTokens": "true",
            }
            logger.info(f"[JUPITER] GET quote: {input_mint[:8]}... -> {output_mint[:8]}... amount={amount}")
            response = self.client.get(url, params=params)
            if response.status_code == 200:
                quote = response.json()
                logger.info(f"[JUPITER] Quote OK: outAmount={quote.get('outAmount', '?')}")
                return quote
            else:
                logger.error(f"[JUPITER] Quote error {response.status_code}: {response.text[:200]}")
                return None
        except Exception as e:
            logger.error(f"[JUPITER] Erreur get_quote: {e}")
            return None

    def execute_swap(self, quote_response: dict) -> Optional[str]:
        """Exécuter un swap à partir d'un devis - MÉTHODE CORRIGÉE"""
        try:
            # 1. Obtenir la transaction sérialisée depuis Jupiter
            url = f"{self.base_url}/swap/v1/swap"
            payload = {
                "quoteResponse": quote_response,
                "userPublicKey": self.wallet.public_key,
                "dynamicComputeUnitLimit": True,
                "dynamicSlippage": True,
                "prioritizationFeeLamports": {
                    "priorityLevelWithMaxLamports": {
                        "maxLamports": 1000000,  # 0.001 SOL max priority fee
                        "priorityLevel": "veryHigh"
                    }
                }
            }
            logger.info(f"[JUPITER] POST swap request...")
            response = self.client.post(url, json=payload)
            if response.status_code != 200:
                logger.error(f"[JUPITER] Swap API error {response.status_code}: {response.text[:300]}")
                return None

            swap_data = response.json()
            swap_transaction_b64 = swap_data.get("swapTransaction")
            if not swap_transaction_b64:
                logger.error(f"[JUPITER] Pas de swapTransaction dans la réponse: {swap_data}")
                return None

            logger.info(f"[JUPITER] Got swapTransaction, signing...")

            # 2. Décoder la transaction
            swap_transaction_bytes = base64.b64decode(swap_transaction_b64)
            raw_transaction = VersionedTransaction.from_bytes(swap_transaction_bytes)

            # 3. Signer la transaction - MÉTHODE CORRECTE (comme l'exemple officiel Jupiter)
            # Trouver l'index du wallet dans les account_keys
            account_keys = raw_transaction.message.account_keys
            wallet_pubkey = self.wallet.keypair.pubkey()
            wallet_index = None
            for i, key in enumerate(account_keys):
                if key == wallet_pubkey:
                    wallet_index = i
                    break

            if wallet_index is None:
                logger.error(f"[JUPITER] Wallet pubkey not found in transaction account_keys!")
                return None

            # Créer la liste des signers en remplaçant la signature placeholder
            signers = list(raw_transaction.signatures)
            signers[wallet_index] = self.wallet.keypair
            signed_transaction = VersionedTransaction(raw_transaction.message, signers)

            logger.info(f"[JUPITER] Transaction signed, sending to RPC...")

            # 4. Envoyer la transaction signée via RPC
            tx_signature = self._send_transaction(signed_transaction)
            return tx_signature

        except Exception as e:
            logger.error(f"[JUPITER] Erreur execute_swap: {e}", exc_info=True)
            return None

    def _send_transaction(self, signed_tx: VersionedTransaction) -> Optional[str]:
        """Envoyer une transaction signée au réseau Solana"""
        # Encoder en base64 pour l'envoi RPC
        encoded_tx = base64.b64encode(bytes(signed_tx)).decode("utf-8")

        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [
                encoded_tx,
                {
                    "skipPreflight": True,
                    "preflightCommitment": "confirmed",
                    "encoding": "base64",
                    "maxRetries": 5,
                }
            ]
        }
        try:
            response = httpx.post(self.wallet.rpc_url, json=payload, timeout=30)
            result = response.json()
            if "result" in result:
                tx_sig = result["result"]
                logger.info(f"[JUPITER] ✅ Transaction envoyée: {tx_sig}")
                return tx_sig
            else:
                error = result.get("error", {})
                logger.error(f"[JUPITER] ❌ Erreur RPC sendTransaction: {json.dumps(error)}")
                return None
        except Exception as e:
            logger.error(f"[JUPITER] Erreur send_transaction: {e}")
            return None

    def confirm_transaction(self, tx_signature: str, timeout: int = 30) -> bool:
        """Attendre la confirmation d'une transaction"""
        start = time.time()
        while time.time() - start < timeout:
            try:
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getSignatureStatuses",
                    "params": [[tx_signature], {"searchTransactionHistory": True}]
                }
                response = httpx.post(self.wallet.rpc_url, json=payload, timeout=10)
                result = response.json()
                statuses = result.get("result", {}).get("value", [])
                if statuses and statuses[0]:
                    status = statuses[0]
                    if status.get("confirmationStatus") in ["confirmed", "finalized"]:
                        if status.get("err") is None:
                            logger.info(f"[JUPITER] ✅ TX confirmée: {tx_signature}")
                            return True
                        else:
                            logger.error(f"[JUPITER] ❌ TX échouée: {status.get('err')}")
                            return False
            except Exception as e:
                logger.error(f"[JUPITER] Erreur confirm: {e}")
            time.sleep(2)
        logger.warning(f"[JUPITER] ⏰ Timeout confirmation TX: {tx_signature}")
        return False

    def buy_token(self, token_mint: str, amount_sol: float) -> Optional[str]:
        """Acheter un token avec du SOL"""
        amount_lamports = int(amount_sol * 1_000_000_000)
        sol_mint = WalletManager.SOL_MINT

        logger.info(f"[BUY] {amount_sol} SOL -> {token_mint[:16]}...")

        # Obtenir le devis
        quote = self.get_quote(sol_mint, token_mint, amount_lamports)
        if not quote:
            logger.error("[BUY] Impossible d'obtenir un devis")
            return None

        out_amount = quote.get("outAmount", "0")
        logger.info(f"[BUY] Devis: {amount_sol} SOL -> {out_amount} tokens")

        # Exécuter le swap
        tx_sig = self.execute_swap(quote)
        if tx_sig:
            logger.info(f"[BUY] ✅ Achat envoyé! TX: {tx_sig}")
            # Vérifier la confirmation
            confirmed = self.confirm_transaction(tx_sig, timeout=30)
            if confirmed:
                logger.info(f"[BUY] ✅ Achat CONFIRMÉ!")
                return tx_sig
            else:
                logger.warning(f"[BUY] ⚠️ TX envoyée mais non confirmée: {tx_sig}")
                return tx_sig  # On retourne quand même la signature
        return None

    def sell_token(self, token_mint: str, raw_amount: int) -> Optional[str]:
        """Vendre un token contre du SOL (utilise raw_amount = unités atomiques)"""
        sol_mint = WalletManager.SOL_MINT

        logger.info(f"[SELL] {raw_amount} raw tokens {token_mint[:16]}... -> SOL")

        # Obtenir le devis
        quote = self.get_quote(token_mint, sol_mint, raw_amount)
        if not quote:
            logger.error("[SELL] Impossible d'obtenir un devis")
            return None

        out_amount = int(quote.get("outAmount", 0))
        sol_received = out_amount / 1_000_000_000
        logger.info(f"[SELL] Devis: -> {sol_received:.4f} SOL")

        # Exécuter le swap
        tx_sig = self.execute_swap(quote)
        if tx_sig:
            logger.info(f"[SELL] ✅ Vente envoyée! TX: {tx_sig}")
            confirmed = self.confirm_transaction(tx_sig, timeout=30)
            if confirmed:
                logger.info(f"[SELL] ✅ Vente CONFIRMÉE!")
            return tx_sig
        return None


# ============================================================
# POSITION MANAGER
# ============================================================

class PositionManager:
    """Gestion des positions ouvertes"""

    POSITIONS_FILE = os.path.join(DATA_DIR, "positions.json")

    def __init__(self):
        self.positions: dict[str, Position] = {}
        self._load_positions()

    def _load_positions(self):
        """Charger les positions depuis le fichier"""
        if os.path.exists(self.POSITIONS_FILE):
            with open(self.POSITIONS_FILE, "r") as f:
                data = json.load(f)
            for addr, pos_data in data.items():
                self.positions[addr] = Position.from_dict(pos_data)

    def _save_positions(self):
        """Sauvegarder les positions"""
        data = {addr: pos.to_dict() for addr, pos in self.positions.items()}
        with open(self.POSITIONS_FILE, "w") as f:
            json.dump(data, f, indent=2)

    def open_position(self, token_address: str, token_name: str, token_symbol: str,
                      entry_price: float, amount_sol: float, amount_tokens: float,
                      strategy: str = "momentum") -> Position:
        """Ouvrir une nouvelle position"""
        position = Position(
            token_address=token_address,
            token_name=token_name,
            token_symbol=token_symbol,
            entry_price_usd=entry_price,
            amount_sol_invested=amount_sol,
            amount_tokens=amount_tokens,
            entry_time=datetime.utcnow().isoformat(),
            highest_price=entry_price,
            current_price=entry_price,
            pnl_pct=0.0,
            strategy=strategy,
        )
        self.positions[token_address] = position
        self._save_positions()
        logger.info(f"Position ouverte: {token_name} ({strategy}) - {amount_sol} SOL")
        return position

    def close_position(self, token_address: str) -> Optional[Position]:
        """Fermer une position"""
        if token_address in self.positions:
            position = self.positions.pop(token_address)
            self._save_positions()
            logger.info(f"Position fermée: {position.token_name} - PnL: {position.pnl_pct:.1f}%")
            return position
        return None

    def update_position(self, token_address: str, current_price: float):
        """Mettre à jour le prix d'une position"""
        if token_address in self.positions:
            pos = self.positions[token_address]
            pos.current_price = current_price
            if current_price > pos.highest_price:
                pos.highest_price = current_price
            if pos.entry_price_usd > 0:
                pos.pnl_pct = ((current_price - pos.entry_price_usd) / pos.entry_price_usd) * 100
            self._save_positions()

    def get_open_positions(self) -> list[Position]:
        """Retourner toutes les positions ouvertes"""
        return list(self.positions.values())

    def get_position(self, token_address: str) -> Optional[Position]:
        """Récupérer une position spécifique"""
        return self.positions.get(token_address)

    def count_positions(self) -> int:
        """Nombre de positions ouvertes"""
        return len(self.positions)

    def total_invested(self) -> float:
        """Total SOL investi"""
        return sum(p.amount_sol_invested for p in self.positions.values())


# ============================================================
# TRADING ENGINE (Stratégies)
# ============================================================

class TradingEngine:
    """Moteur de trading - Exécute les stratégies Sniper et Momentum"""

    def __init__(self, config: TradingConfig, wallet: WalletManager,
                 swap: JupiterSwap, positions: PositionManager):
        self.config = config
        self.wallet = wallet
        self.swap = swap
        self.positions = positions
        self.last_trade_time = 0
        self.trade_history: list[dict] = []
        self._load_history()
        init_postmortem_db()

    def _load_history(self):
        """Charger l'historique des trades"""
        history_file = os.path.join(DATA_DIR, "trade_history.json")
        if os.path.exists(history_file):
            with open(history_file, "r") as f:
                self.trade_history = json.load(f)

    def _save_history(self):
        """Sauvegarder l'historique"""
        history_file = os.path.join(DATA_DIR, "trade_history.json")
        with open(history_file, "w") as f:
            json.dump(self.trade_history, f, indent=2)

    def can_trade(self) -> Tuple[bool, str]:
        """Vérifier si on peut trader"""
        # Cooldown
        elapsed = time.time() - self.last_trade_time
        if elapsed < self.config.cooldown_seconds:
            return False, f"Cooldown ({self.config.cooldown_seconds - elapsed:.0f}s restantes)"

        # Max positions
        if self.positions.count_positions() >= self.config.max_open_positions:
            return False, f"Max positions atteint ({self.config.max_open_positions})"

        # Budget
        total_invested = self.positions.total_invested()
        if total_invested >= self.config.max_budget_sol:
            return False, f"Budget max atteint ({total_invested:.2f}/{self.config.max_budget_sol} SOL)"

        # Solde
        balance = self.wallet.get_sol_balance()
        if balance < 0.01:  # Garder un minimum pour les frais
            return False, f"Solde insuffisant ({balance:.4f} SOL)"

        return True, "OK"

    def should_snipe(self, analysis: dict) -> Tuple[bool, str]:
        """Déterminer si on doit sniper un token"""
        if not self.config.sniper_enabled:
            return False, "Sniper désactivé"

        # Déjà en position
        if analysis["address"] in self.positions.positions:
            return False, "Déjà en position"

        # Vérifier liquidité
        if analysis["liquidity_usd"] < self.config.sniper_min_liquidity:
            return False, "Liquidité trop faible"

        # Vérifier MC
        mc = analysis.get("market_cap", 0)
        if mc and mc > self.config.sniper_max_mc:
            return False, "MC trop élevé pour snipe"

        # Token récent (< 1h)
        age = analysis.get("age_hours")
        if age and age > 1:
            return False, "Token trop vieux pour snipe"

        # Ratio buy/sell favorable
        if analysis.get("buy_sell_ratio_5m", 0) < 2:
            return False, "Ratio buy/sell insuffisant"

        return True, "✅ Snipe validé"


    def should_sell(self, position: Position) -> Tuple[bool, str]:
        """Déterminer si on doit vendre une position"""
        pnl = position.pnl_pct

        # Take Profit (fixe)
        if pnl >= self.config.take_profit_pct:
            return True, f"🎯 Take Profit atteint ({pnl:.1f}%)"

        # Stop Loss (fixe - filet de sécurité)
        if pnl <= self.config.stop_loss_pct:
            return True, f"🛑 Stop Loss déclenché ({pnl:.1f}%)"

        # Trailing Stop Dynamique à paliers
        # Plus le profit est élevé, plus le trailing se resserre
        if position.highest_price > 0 and position.current_price > 0:
            # Calculer le PnL au plus haut (profit max atteint)
            pnl_at_high = ((position.highest_price - position.entry_price_usd)
                           / position.entry_price_usd) * 100 if position.entry_price_usd > 0 else 0

            # Le trailing s'active dès que le profit a atteint le seuil d'activation
            if pnl_at_high >= self.config.trailing_activation_pct:
                # Calculer le trailing stop dynamique selon le palier de profit
                trailing_pct = self._get_dynamic_trailing(pnl_at_high)

                # Chute depuis le plus haut
                drop_from_high = ((position.current_price - position.highest_price)
                                  / position.highest_price) * 100

                if drop_from_high <= -trailing_pct:
                    # Calculer le PnL réel au moment de la vente
                    return True, (f"📉 Trailing Stop (chute {drop_from_high:.1f}% depuis ATH, "
                                  f"seuil: -{trailing_pct:.0f}%, profit max: +{pnl_at_high:.0f}%)")

        # Time Stop : sortie forcée si le trade dure trop longtemps sans profit suffisant
        if self.config.time_stop_enabled:
            try:
                entry_dt = datetime.fromisoformat(position.entry_time)
                elapsed_minutes = (datetime.utcnow() - entry_dt).total_seconds() / 60.0

                if elapsed_minutes >= self.config.time_stop_minutes:
                    # Vérifier si le profit est en dessous du seuil minimum
                    if pnl < self.config.time_stop_min_profit:
                        return True, (f"⏰ Time Stop ({elapsed_minutes:.0f} min, "
                                      f"PnL {pnl:+.1f}% < +{self.config.time_stop_min_profit:.0f}%)")
            except (ValueError, TypeError):
                pass  # Si entry_time est invalide, on ignore le time stop

        # RÈGLE 4 - Momentum Stop: prix sous ATH -15% ET volume en chute
        if self.config.momentum_stop_enabled:
            if position.highest_price > 0 and position.entry_price_usd > 0:
                # Calculer la chute depuis l'ATH local
                drop_from_ath = ((position.current_price - position.highest_price)
                                 / position.highest_price) * 100

                # Si le prix a chuté de plus de 15% depuis l'ATH local
                if drop_from_ath <= -self.config.momentum_stop_drop_pct:
                    # Vérifier que le token a eu un pump (ATH > entry +5% au moins)
                    pnl_at_ath = ((position.highest_price - position.entry_price_usd)
                                  / position.entry_price_usd) * 100
                    if pnl_at_ath >= 5.0:
                        # Le token a pumpé puis est retombé = momentum mort
                        return True, (f"📉 Momentum Stop (chute {drop_from_ath:.1f}% depuis ATH, "
                                      f"ATH était +{pnl_at_ath:.0f}%, token mort)")

        return False, "Hold"

    def _get_dynamic_trailing(self, pnl_at_high: float) -> float:
        """
        RÈGLE 3 - Trailing Stop simplifié:
        Dès +15% atteint → SL remonte à -10% du max
        Capture l'essentiel du pump sans couper trop tôt.
        
        Pour les moonshots, on resserre légèrement:
        - Profit +15% à +50%  → trailing de 10%
        - Profit +50% à +100% → trailing de 8%
        - Profit > +100%      → trailing de 6%
        """
        if pnl_at_high >= 100:
            return 6.0
        elif pnl_at_high >= 50:
            return 8.0
        else:
            return self.config.trailing_stop_pct  # 10% par défaut

    def execute_buy(self, analysis: dict, strategy: str) -> Optional[dict]:
        """Exécuter un achat"""
        can, reason = self.can_trade()
        if not can:
            logger.info(f"Trade refusé: {reason}")
            return None

        # Taille de position (stratégie sniper uniquement)
        amount_sol = self.config.sniper_position_sol

        # Vérifier le solde
        balance = self.wallet.get_sol_balance()
        if balance < amount_sol + 0.005:  # +0.005 pour les frais
            logger.warning(f"Solde insuffisant: {balance:.4f} SOL < {amount_sol + 0.005}")
            return None

        # Exécuter l'achat
        token_mint = analysis["address"]
        tx_sig = self.swap.buy_token(token_mint, amount_sol)

        if tx_sig:
            self.last_trade_time = time.time()

            # Estimer les tokens reçus (approximation basée sur le prix)
            price_usd = float(analysis.get("price_usd", 0) or 0)

            # Ouvrir la position
            position = self.positions.open_position(
                token_address=token_mint,
                token_name=analysis.get("name", "Unknown"),
                token_symbol=analysis.get("symbol", "???"),
                entry_price=price_usd,
                amount_sol=amount_sol,
                amount_tokens=0,  # Sera mis à jour après vérification on-chain
                strategy=strategy,
            )

            # Enregistrer dans l'historique
            trade_record = {
                "type": "BUY",
                "strategy": strategy,
                "token": analysis.get("symbol", "???"),
                "token_address": token_mint,
                "amount_sol": amount_sol,
                "price_usd": price_usd,
                "tx_signature": tx_sig,
                "timestamp": datetime.utcnow().isoformat(),
            }
            self.trade_history.append(trade_record)
            self._save_history()

            return trade_record
        return None

    def execute_sell(self, position: Position, reason: str) -> Optional[dict]:
        """Exécuter une vente"""
        token_mint = position.token_address

        # Récupérer le solde réel du token (ui_amount ET raw_amount)
        ui_amount, raw_amount = self.wallet.get_token_balance(token_mint)
        if raw_amount <= 0:
            logger.warning(f"Pas de tokens à vendre pour {position.token_name}")
            self.positions.close_position(token_mint)
            return None

        logger.info(f"[SELL] Token balance: ui={ui_amount}, raw={raw_amount}")

        # Utiliser le raw_amount directement (unités atomiques correctes)
        tx_sig = self.swap.sell_token(token_mint, raw_amount)

        if tx_sig:
            self.last_trade_time = time.time()

            # Fermer la position
            closed_position = self.positions.close_position(token_mint)

            # Enregistrer
            trade_record = {
                "type": "SELL",
                "reason": reason,
                "strategy": position.strategy,
                "token": position.token_symbol,
                "token_address": token_mint,
                "pnl_pct": position.pnl_pct,
                "amount_sol_invested": position.amount_sol_invested,
                "tx_signature": tx_sig,
                "timestamp": datetime.utcnow().isoformat(),
            }
            self.trade_history.append(trade_record)
            self._save_history()

            # Postmortem Tracker — thread dédié 30min post-vente
            try:
                start_postmortem_thread(
                    trade_record=trade_record,
                    entry_price_usd=position.entry_price_usd,
                    helius_api_key=os.environ.get("HELIUS_API_KEY", ""),
                    telegram_bot_token=os.environ.get("TELEGRAM_BOT_TOKEN", ""),
                    telegram_chat_id=os.environ.get("TELEGRAM_CHAT_ID", ""),
                )
            except Exception as e:
                logger.error(f"Postmortem thread error: {e}")

            return trade_record
        return None

    def get_stats(self) -> dict:
        """Obtenir les statistiques de trading"""
        total_trades = len(self.trade_history)
        buys = [t for t in self.trade_history if t["type"] == "BUY"]
        sells = [t for t in self.trade_history if t["type"] == "SELL"]
        wins = [s for s in sells if s.get("pnl_pct", 0) > 0]
        losses = [s for s in sells if s.get("pnl_pct", 0) <= 0]

        total_invested = sum(b.get("amount_sol", 0) for b in buys)
        total_pnl = sum(s.get("pnl_pct", 0) for s in sells)

        return {
            "total_trades": total_trades,
            "buys": len(buys),
            "sells": len(sells),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": (len(wins) / max(len(sells), 1)) * 100,
            "total_invested_sol": total_invested,
            "avg_pnl_pct": total_pnl / max(len(sells), 1),
            "open_positions": self.positions.count_positions(),
            "balance_sol": self.wallet.get_sol_balance(),
        }
