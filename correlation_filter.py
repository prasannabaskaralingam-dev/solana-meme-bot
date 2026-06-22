"""
Correlation Filter - Éviter les tokens corrélés pour diversifier le portefeuille.

Détecte les tokens du même narratif/catégorie et limite l'exposition par groupe.
Exemples: pas 3 "dog coins" en même temps, pas 3 "AI coins", etc.

Méthodes de détection:
1. Analyse du nom/symbole (mots-clés narratifs)
2. Même développeur (même deployer address)
3. Même pool de liquidité source
4. Corrélation temporelle (tokens lancés en même temps = même dev)
"""

import re
import time
import json
import os
import logging
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger(__name__)

# ============================================================
# NARRATIVES & KEYWORDS
# ============================================================

# Catégories de narratifs avec mots-clés associés
NARRATIVE_KEYWORDS = {
    "dog": [
        "dog", "doge", "shib", "inu", "puppy", "pup", "woof", "bark",
        "corgi", "husky", "retriever", "bulldog", "poodle", "mutt",
        "bonk", "floki", "cheems", "dingo", "hound"
    ],
    "cat": [
        "cat", "kitty", "kitten", "meow", "nyan", "purr", "feline",
        "tabby", "whiskers", "paws", "mew", "popcat"
    ],
    "frog": [
        "frog", "pepe", "kek", "ribbit", "toad", "croak", "tadpole",
        "froggy", "pepecoin"
    ],
    "ai": [
        "ai", "gpt", "neural", "brain", "intelligence", "machine",
        "robot", "bot", "algo", "quantum", "compute", "llm",
        "openai", "anthropic", "deepseek"
    ],
    "elon": [
        "elon", "musk", "tesla", "spacex", "mars", "doge", "x ",
        "twitter", "grok", "neuralink", "boring"
    ],
    "trump": [
        "trump", "maga", "donald", "potus", "republican", "patriot",
        "freedom", "america first", "47"
    ],
    "anime": [
        "anime", "waifu", "senpai", "kawaii", "manga", "otaku",
        "naruto", "goku", "luffy", "chan", "san", "sama"
    ],
    "food": [
        "pizza", "burger", "sushi", "taco", "food", "eat", "cook",
        "chef", "hungry", "yummy", "delicious", "meat", "steak"
    ],
    "money": [
        "rich", "money", "cash", "dollar", "billion", "million",
        "gold", "diamond", "lambo", "moon", "pump", "wagmi"
    ],
    "gaming": [
        "game", "play", "gamer", "esport", "pixel", "arcade",
        "quest", "level", "boss", "npc", "rpg", "mmorpg"
    ],
    "celebrity": [
        "drake", "kanye", "beyonce", "taylor", "swift", "bieber",
        "rihanna", "eminem", "snoop", "50cent", "diddy"
    ],
    "solana_meta": [
        "sol", "solana", "raydium", "jupiter", "pump", "fun",
        "bonk", "jito", "marinade", "orca"
    ],
}

# Mots-clés qui indiquent un "wrapper" ou clone d'un token existant
CLONE_INDICATORS = [
    "2.0", "v2", "new", "real", "official", "original", "classic",
    "inu", "swap", "finance", "protocol", "dao", "labs"
]


@dataclass
class TokenNarrative:
    """Narratif détecté pour un token"""
    token_address: str
    token_name: str
    token_symbol: str
    narratives: List[str]  # Liste des narratifs détectés
    deployer: Optional[str] = None
    launch_time: Optional[str] = None
    confidence: float = 0.0  # 0-1, confiance dans la détection


@dataclass
class CorrelationConfig:
    """Configuration du filtre de corrélation"""
    enabled: bool = True
    max_per_narrative: int = 2          # Max 2 tokens du même narratif
    max_same_deployer: int = 1          # Max 1 token du même dev
    time_correlation_minutes: int = 5   # Tokens lancés à < 5min = même dev probable
    max_same_time_cluster: int = 1      # Max 1 token d'un cluster temporel


class CorrelationFilter:
    """
    Filtre anti-corrélation pour diversifier le portefeuille.
    
    Empêche d'acheter trop de tokens du même narratif, du même dev,
    ou lancés au même moment (probable même dev/scam).
    """

    def __init__(self, data_dir: str = "/var/data/solana-bot"):
        self.config = CorrelationConfig()
        self.data_dir = data_dir
        self.data_file = os.path.join(data_dir, "correlation_data.json")
        
        # Cache des narratifs détectés pour les positions ouvertes
        self.active_narratives: Dict[str, TokenNarrative] = {}
        
        # Historique des deployers vus
        self.deployer_cache: Dict[str, str] = {}  # token_address -> deployer
        
        # Clusters temporels (tokens lancés en même temps)
        self.time_clusters: Dict[str, List[str]] = {}  # cluster_id -> [token_addresses]
        
        self._load_data()

    def _load_data(self):
        """Charger les données persistées"""
        try:
            if os.path.exists(self.data_file):
                with open(self.data_file, "r") as f:
                    data = json.load(f)
                self.deployer_cache = data.get("deployer_cache", {})
        except Exception as e:
            logger.error(f"Erreur chargement correlation data: {e}")

    def _save_data(self):
        """Sauvegarder les données"""
        try:
            os.makedirs(self.data_dir, exist_ok=True)
            data = {
                "deployer_cache": self.deployer_cache,
                "last_updated": datetime.utcnow().isoformat(),
            }
            with open(self.data_file, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Erreur sauvegarde correlation data: {e}")

    def detect_narratives(self, token_name: str, token_symbol: str) -> List[str]:
        """
        Détecter les narratifs d'un token à partir de son nom et symbole.
        
        Returns:
            Liste des narratifs détectés (ex: ["dog", "elon"])
        """
        detected = []
        text = f"{token_name} {token_symbol}".lower()
        
        for narrative, keywords in NARRATIVE_KEYWORDS.items():
            for keyword in keywords:
                # Vérifier si le mot-clé est dans le nom/symbole
                if keyword.lower() in text:
                    if narrative not in detected:
                        detected.append(narrative)
                    break  # Un seul match suffit par narratif
        
        return detected

    def detect_name_similarity(self, name1: str, name2: str) -> float:
        """
        Calculer la similarité entre deux noms de tokens.
        Retourne un score entre 0 et 1.
        """
        # Normaliser
        n1 = re.sub(r'[^a-z0-9]', '', name1.lower())
        n2 = re.sub(r'[^a-z0-9]', '', name2.lower())
        
        if not n1 or not n2:
            return 0.0
        
        # Vérifier si l'un contient l'autre
        if n1 in n2 or n2 in n1:
            return 0.8
        
        # Calculer les n-grams communs (bigrams)
        def get_bigrams(s):
            return set(s[i:i+2] for i in range(len(s)-1))
        
        bg1 = get_bigrams(n1)
        bg2 = get_bigrams(n2)
        
        if not bg1 or not bg2:
            return 0.0
        
        intersection = bg1 & bg2
        union = bg1 | bg2
        
        return len(intersection) / len(union) if union else 0.0

    def check_time_correlation(self, token_age_hours: float, 
                                existing_positions: List[dict]) -> Tuple[bool, str]:
        """
        Vérifier si un token a été lancé en même temps qu'une position existante.
        Tokens lancés à < 5 min d'intervalle = probable même dev (batch launch).
        """
        if not existing_positions:
            return False, ""
        
        threshold_hours = self.config.time_correlation_minutes / 60.0
        
        for pos in existing_positions:
            pos_age = pos.get("age_hours", 0)
            if pos_age and token_age_hours:
                time_diff = abs(token_age_hours - pos_age)
                if time_diff < threshold_hours:
                    return True, (f"Token lancé à < {self.config.time_correlation_minutes} min "
                                  f"d'un token en position ({pos.get('name', '???')})")
        
        return False, ""

    def register_position(self, token_address: str, token_name: str, 
                          token_symbol: str, deployer: Optional[str] = None,
                          age_hours: Optional[float] = None):
        """
        Enregistrer un token comme position active pour le tracking de corrélation.
        Appelé quand un achat est effectué.
        """
        narratives = self.detect_narratives(token_name, token_symbol)
        
        self.active_narratives[token_address] = TokenNarrative(
            token_address=token_address,
            token_name=token_name,
            token_symbol=token_symbol,
            narratives=narratives,
            deployer=deployer,
            launch_time=datetime.utcnow().isoformat(),
            confidence=0.8 if narratives else 0.3,
        )
        
        if deployer:
            self.deployer_cache[token_address] = deployer
            self._save_data()
        
        logger.info(f"📊 Corrélation enregistrée: {token_symbol} → narratifs: {narratives}")

    def unregister_position(self, token_address: str):
        """Retirer un token quand la position est fermée"""
        if token_address in self.active_narratives:
            del self.active_narratives[token_address]

    def sync_with_positions(self, open_positions: List):
        """
        Synchroniser le filtre avec les positions réellement ouvertes.
        Appelé au démarrage pour reconstruire l'état.
        """
        # Nettoyer les positions qui ne sont plus ouvertes
        active_addresses = {pos.token_address for pos in open_positions}
        to_remove = [addr for addr in self.active_narratives if addr not in active_addresses]
        for addr in to_remove:
            del self.active_narratives[addr]
        
        # Ajouter les positions ouvertes qui ne sont pas encore trackées
        for pos in open_positions:
            if pos.token_address not in self.active_narratives:
                self.register_position(
                    pos.token_address, pos.token_name, pos.token_symbol
                )

    def can_buy(self, token_address: str, token_name: str, token_symbol: str,
                deployer: Optional[str] = None, age_hours: Optional[float] = None) -> Tuple[bool, str]:
        """
        Vérifier si on peut acheter ce token sans créer trop de corrélation.
        
        Returns:
            (can_buy, reason) - True si OK, False avec la raison si bloqué
        """
        if not self.config.enabled:
            return True, "Filtre corrélation désactivé"

        # 1. Détecter les narratifs du nouveau token
        new_narratives = self.detect_narratives(token_name, token_symbol)
        
        # 2. Compter les positions par narratif
        narrative_counts: Dict[str, int] = {}
        for tn in self.active_narratives.values():
            for narr in tn.narratives:
                narrative_counts[narr] = narrative_counts.get(narr, 0) + 1
        
        # 3. Vérifier si un narratif dépasse la limite
        for narr in new_narratives:
            current_count = narrative_counts.get(narr, 0)
            if current_count >= self.config.max_per_narrative:
                # Lister les tokens existants dans ce narratif
                existing = [tn.token_symbol for tn in self.active_narratives.values() 
                           if narr in tn.narratives]
                return False, (f"🚫 Corrélation: max {self.config.max_per_narrative} tokens "
                              f"'{narr}' atteint (existants: {', '.join(existing)})")
        
        # 4. Vérifier le même deployer
        if deployer and deployer != "unknown":
            same_deployer_count = sum(
                1 for addr, dep in self.deployer_cache.items()
                if dep == deployer and addr in self.active_narratives
            )
            if same_deployer_count >= self.config.max_same_deployer:
                return False, f"🚫 Corrélation: même dev détecté ({deployer[:12]}...)"
        
        # 5. Vérifier la similarité de nom avec les positions existantes
        for tn in self.active_narratives.values():
            similarity = self.detect_name_similarity(token_name, tn.token_name)
            if similarity >= 0.7:
                return False, (f"🚫 Corrélation: nom trop similaire à "
                              f"{tn.token_symbol} (similarité: {similarity:.0%})")
        
        # 6. Vérifier la corrélation temporelle (tokens lancés en même temps)
        if age_hours is not None and age_hours < 1:  # Seulement pour les tokens récents
            for tn in self.active_narratives.values():
                if tn.launch_time:
                    try:
                        tn_launch = datetime.fromisoformat(tn.launch_time)
                        minutes_since = (datetime.utcnow() - tn_launch).total_seconds() / 60
                        # Si le token existant a été acheté récemment ET le nouveau est aussi récent
                        if minutes_since < 30 and age_hours < 0.5:
                            # Vérifier s'ils partagent des narratifs
                            shared = set(new_narratives) & set(tn.narratives)
                            if shared:
                                return False, (f"🚫 Corrélation temporelle: {token_symbol} et "
                                              f"{tn.token_symbol} lancés en même temps, "
                                              f"narratif commun: {', '.join(shared)}")
                    except (ValueError, TypeError):
                        pass
        
        return True, "✅ Diversification OK"

    def get_portfolio_diversity(self) -> Dict[str, any]:
        """
        Obtenir un rapport de diversification du portefeuille.
        """
        narrative_counts: Dict[str, List[str]] = {}
        
        for tn in self.active_narratives.values():
            if not tn.narratives:
                narrative_counts.setdefault("non_classé", []).append(tn.token_symbol)
            for narr in tn.narratives:
                narrative_counts.setdefault(narr, []).append(tn.token_symbol)
        
        total_positions = len(self.active_narratives)
        unique_narratives = len(narrative_counts)
        
        # Score de diversification (0-100)
        if total_positions <= 1:
            diversity_score = 100
        else:
            # Pénaliser la concentration
            max_concentration = max(len(tokens) for tokens in narrative_counts.values()) if narrative_counts else 0
            diversity_score = max(0, 100 - (max_concentration / total_positions * 100))
        
        return {
            "total_positions": total_positions,
            "unique_narratives": unique_narratives,
            "narrative_breakdown": {k: v for k, v in sorted(narrative_counts.items(), 
                                                             key=lambda x: -len(x[1]))},
            "diversity_score": round(diversity_score),
            "max_per_narrative": self.config.max_per_narrative,
        }

    def get_status_message(self) -> str:
        """Générer un message de statut pour Telegram"""
        diversity = self.get_portfolio_diversity()
        
        msg = f"🎯 *Filtre Anti-Corrélation*\n\n"
        msg += f"Statut: {'✅ Actif' if self.config.enabled else '❌ Désactivé'}\n"
        msg += f"Max par narratif: {self.config.max_per_narrative}\n"
        msg += f"Score diversification: {diversity['diversity_score']}/100\n\n"
        
        if diversity["narrative_breakdown"]:
            msg += "*Répartition actuelle:*\n"
            for narrative, tokens in diversity["narrative_breakdown"].items():
                count = len(tokens)
                bar = "█" * count + "░" * (self.config.max_per_narrative - count)
                msg += f"  {narrative}: [{bar}] {', '.join(tokens)}\n"
        else:
            msg += "_Aucune position ouverte_\n"
        
        return msg
