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

logger = logging.getLogger(__name__)


# ============================================================
# CONFIGURATION TRADING
# ============================================================

@dataclass
class TradingConfig:
    """Configuration de la stratégie de trading"""

    # Budget et position
    max_budget_sol: float = 6.5          # ~1000 CHF en SOL (ajuster selon le cours)
    position_size_sol: float = 0.3       # Taille d'une position (SOL par trade)
    max_open_positions: int = 10         # Nombre max de positions ouvertes

    # Stratégie Sniper (nouveaux tokens)
    sniper_enabled: bool = True
    sniper_position_sol: float = 0.2     # Montant par snipe (plus petit = moins risqué)
    sniper_min_liquidity: float = 5000   # Liquidité min pour sniper ($)
    sniper_max_mc: float = 100_000       # MC max pour sniper ($)

    # Stratégie Momentum (tokens en pump)
    momentum_enabled: bool = True
    momentum_position_sol: float = 0.3   # Montant par trade momentum
    momentum_min_pump_5m: float = 15     # % hausse min sur 5min
    momentum_min_pump_1h: float = 40     # % hausse min sur 1h
    momentum_min_volume: float = 10_000  # Volume 24h min ($)
    momentum_min_buys_ratio: float = 3.0 # Ratio achats/ventes min

    # Take Profit & Stop Loss
    take_profit_pct: float = 50.0        # Vendre quand +50%
    stop_loss_pct: float = -30.0         # Vendre quand -30%
    trailing_stop_pct: float = 20.0      # Trailing stop de 20% depuis le plus haut

    # Sécurité
    slippage_bps: int = 500              # 5% slippage max (meme coins = volatile)
    max_retries: int = 3                 # Nombre de tentatives par transaction
    cooldown_seconds: int = 10           # Attente entre les trades

    # RPC & API
    rpc_url: str = ""
    jupiter_api_url: str = "https://api.jup.ag"


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
    strategy: str = "momentum"  # "momentum" ou "sniper"

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

    WALLET_FILE = "wallet.json"
    SOL_MINT = "So11111111111111111111111111111111111111112"

    def __init__(self, rpc_url: str):
        self.rpc_url = rpc_url or "https://api.mainnet-beta.solana.com"
        self.keypair: Optional[Keypair] = None
        self.public_key: str = ""

    def load_or_create_wallet(self) -> str:
        """Charger un wallet existant ou en créer un nouveau"""
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

    def get_token_balance(self, token_mint: str) -> float:
        """Récupérer le solde d'un token SPL"""
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
                return 0.0
            token_amount = accounts[0]["account"]["data"]["parsed"]["info"]["tokenAmount"]
            return float(token_amount["uiAmount"] or 0)
        except Exception as e:
            logger.error(f"Erreur getTokenBalance: {e}")
            return 0.0

    def export_private_key(self) -> str:
        """Exporter la clé privée en base58 (ATTENTION: sensible !)"""
        if self.keypair:
            return base58.b58encode(bytes(self.keypair)).decode()
        return ""


# ============================================================
# JUPITER SWAP ENGINE
# ============================================================

class JupiterSwap:
    """Moteur de swap via Jupiter API"""

    def __init__(self, wallet: WalletManager, config: TradingConfig):
        self.wallet = wallet
        self.config = config
        self.base_url = config.jupiter_api_url
        self.client = httpx.Client(timeout=30)

    def get_quote(self, input_mint: str, output_mint: str, amount_lamports: int) -> Optional[dict]:
        """Obtenir un devis de swap via Jupiter"""
        try:
            url = f"{self.base_url}/swap/v1/quote"
            params = {
                "inputMint": input_mint,
                "outputMint": output_mint,
                "amount": str(amount_lamports),
                "slippageBps": self.config.slippage_bps,
                "restrictIntermediateTokens": "true",
            }
            response = self.client.get(url, params=params)
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Quote error {response.status_code}: {response.text}")
                return None
        except Exception as e:
            logger.error(f"Erreur get_quote: {e}")
            return None

    def execute_swap(self, quote_response: dict) -> Optional[str]:
        """Exécuter un swap à partir d'un devis"""
        try:
            # 1. Obtenir la transaction sérialisée
            url = f"{self.base_url}/swap/v1/swap"
            payload = {
                "quoteResponse": quote_response,
                "userPublicKey": self.wallet.public_key,
                "dynamicComputeUnitLimit": True,
                "dynamicSlippage": True,
                "prioritizationFeeLamports": {
                    "priorityLevelWithMaxLamports": {
                        "maxLamports": 500000,  # 0.0005 SOL max priority fee
                        "priorityLevel": "high"
                    }
                }
            }
            response = self.client.post(url, json=payload)
            if response.status_code != 200:
                logger.error(f"Swap API error: {response.text}")
                return None

            swap_data = response.json()
            swap_transaction = swap_data.get("swapTransaction")
            if not swap_transaction:
                logger.error(f"Pas de swapTransaction dans la réponse")
                return None

            # 2. Décoder et signer la transaction
            raw_tx = VersionedTransaction.from_bytes(base64.b64decode(swap_transaction))
            signature = self.wallet.keypair.sign_message(bytes(raw_tx.message))
            signed_tx = VersionedTransaction.populate(raw_tx.message, [signature])
            encoded_tx = base64.b64encode(bytes(signed_tx)).decode("utf-8")

            # 3. Envoyer la transaction via RPC
            tx_signature = self._send_transaction(encoded_tx)
            return tx_signature

        except Exception as e:
            logger.error(f"Erreur execute_swap: {e}")
            return None

    def _send_transaction(self, encoded_tx: str) -> Optional[str]:
        """Envoyer une transaction signée au réseau Solana"""
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
                    "maxRetries": 3,
                }
            ]
        }
        try:
            response = httpx.post(self.wallet.rpc_url, json=payload, timeout=30)
            result = response.json()
            if "result" in result:
                tx_sig = result["result"]
                logger.info(f"Transaction envoyée: {tx_sig}")
                return tx_sig
            else:
                error = result.get("error", {})
                logger.error(f"Erreur RPC: {error}")
                return None
        except Exception as e:
            logger.error(f"Erreur send_transaction: {e}")
            return None

    def buy_token(self, token_mint: str, amount_sol: float) -> Optional[str]:
        """Acheter un token avec du SOL"""
        amount_lamports = int(amount_sol * 1_000_000_000)
        sol_mint = WalletManager.SOL_MINT

        logger.info(f"Achat: {amount_sol} SOL -> {token_mint[:12]}...")

        # Obtenir le devis
        quote = self.get_quote(sol_mint, token_mint, amount_lamports)
        if not quote:
            logger.error("Impossible d'obtenir un devis pour l'achat")
            return None

        out_amount = int(quote.get("outAmount", 0))
        logger.info(f"Devis: {amount_sol} SOL -> {out_amount} tokens")

        # Exécuter le swap
        tx_sig = self.execute_swap(quote)
        if tx_sig:
            logger.info(f"✅ Achat réussi! TX: {tx_sig}")
        return tx_sig

    def sell_token(self, token_mint: str, amount_tokens: int) -> Optional[str]:
        """Vendre un token contre du SOL"""
        sol_mint = WalletManager.SOL_MINT

        logger.info(f"Vente: {amount_tokens} tokens {token_mint[:12]}... -> SOL")

        # Obtenir le devis
        quote = self.get_quote(token_mint, sol_mint, amount_tokens)
        if not quote:
            logger.error("Impossible d'obtenir un devis pour la vente")
            return None

        out_amount = int(quote.get("outAmount", 0))
        sol_received = out_amount / 1_000_000_000
        logger.info(f"Devis: {amount_tokens} tokens -> {sol_received:.4f} SOL")

        # Exécuter le swap
        tx_sig = self.execute_swap(quote)
        if tx_sig:
            logger.info(f"✅ Vente réussie! TX: {tx_sig}")
        return tx_sig


# ============================================================
# POSITION MANAGER
# ============================================================

class PositionManager:
    """Gestion des positions ouvertes"""

    POSITIONS_FILE = "positions.json"

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

    def _load_history(self):
        """Charger l'historique des trades"""
        if os.path.exists("trade_history.json"):
            with open("trade_history.json", "r") as f:
                self.trade_history = json.load(f)

    def _save_history(self):
        """Sauvegarder l'historique"""
        with open("trade_history.json", "w") as f:
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

    def should_buy_momentum(self, analysis: dict) -> Tuple[bool, str]:
        """Déterminer si on doit acheter en momentum"""
        if not self.config.momentum_enabled:
            return False, "Momentum désactivé"

        # Déjà en position
        if analysis["address"] in self.positions.positions:
            return False, "Déjà en position"

        # Vérifier le pump
        pump_5m = analysis.get("price_change_5m", 0)
        pump_1h = analysis.get("price_change_1h", 0)

        has_pump = (pump_5m >= self.config.momentum_min_pump_5m or
                    pump_1h >= self.config.momentum_min_pump_1h)
        if not has_pump:
            return False, "Pas assez de pump"

        # Volume
        if analysis.get("volume_24h", 0) < self.config.momentum_min_volume:
            return False, "Volume insuffisant"

        # Ratio buy/sell
        if analysis.get("buy_sell_ratio_5m", 0) < self.config.momentum_min_buys_ratio:
            return False, "Ratio buy/sell insuffisant"

        # Liquidité minimum
        if analysis.get("liquidity_usd", 0) < 5000:
            return False, "Liquidité trop faible"

        return True, "✅ Momentum validé"

    def should_sell(self, position: Position) -> Tuple[bool, str]:
        """Déterminer si on doit vendre une position"""
        pnl = position.pnl_pct

        # Take Profit
        if pnl >= self.config.take_profit_pct:
            return True, f"🎯 Take Profit atteint ({pnl:.1f}%)"

        # Stop Loss
        if pnl <= self.config.stop_loss_pct:
            return True, f"🛑 Stop Loss déclenché ({pnl:.1f}%)"

        # Trailing Stop
        if position.highest_price > 0 and position.current_price > 0:
            drop_from_high = ((position.current_price - position.highest_price)
                              / position.highest_price) * 100
            if position.pnl_pct > 10 and drop_from_high <= -self.config.trailing_stop_pct:
                return True, f"📉 Trailing Stop ({drop_from_high:.1f}% depuis le plus haut)"

        return False, "Hold"

    def execute_buy(self, analysis: dict, strategy: str) -> Optional[dict]:
        """Exécuter un achat"""
        can, reason = self.can_trade()
        if not can:
            logger.info(f"Trade refusé: {reason}")
            return None

        # Déterminer la taille de position
        if strategy == "sniper":
            amount_sol = self.config.sniper_position_sol
        else:
            amount_sol = self.config.momentum_position_sol

        # Vérifier le solde
        balance = self.wallet.get_sol_balance()
        if balance < amount_sol + 0.01:  # +0.01 pour les frais
            logger.warning(f"Solde insuffisant: {balance:.4f} SOL < {amount_sol + 0.01}")
            return None

        # Exécuter l'achat
        token_mint = analysis["address"]
        tx_sig = self.swap.buy_token(token_mint, amount_sol)

        if tx_sig:
            self.last_trade_time = time.time()

            # Estimer les tokens reçus (approximation basée sur le prix)
            price_usd = float(analysis.get("price_usd", 0) or 0)
            # Note: le montant exact sera vérifié après confirmation
            estimated_tokens = 0  # Sera mis à jour après confirmation

            # Ouvrir la position
            position = self.positions.open_position(
                token_address=token_mint,
                token_name=analysis.get("name", "Unknown"),
                token_symbol=analysis.get("symbol", "???"),
                entry_price=price_usd,
                amount_sol=amount_sol,
                amount_tokens=estimated_tokens,
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

        # Récupérer le solde réel du token
        token_balance = self.wallet.get_token_balance(token_mint)
        if token_balance <= 0:
            logger.warning(f"Pas de tokens à vendre pour {position.token_name}")
            self.positions.close_position(token_mint)
            return None

        # Convertir en unités atomiques (approximation - les décimales varient)
        # Pour la plupart des meme coins sur Solana: 6 ou 9 décimales
        amount_atomic = int(token_balance * 1_000_000)  # Assumant 6 décimales

        tx_sig = self.swap.sell_token(token_mint, amount_atomic)

        if tx_sig:
            self.last_trade_time = time.time()

            # Fermer la position
            closed_position = self.positions.close_position(token_mint)

            # Enregistrer
            trade_record = {
                "type": "SELL",
                "reason": reason,
                "token": position.token_symbol,
                "token_address": token_mint,
                "pnl_pct": position.pnl_pct,
                "amount_sol_invested": position.amount_sol_invested,
                "tx_signature": tx_sig,
                "timestamp": datetime.utcnow().isoformat(),
            }
            self.trade_history.append(trade_record)
            self._save_history()

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
