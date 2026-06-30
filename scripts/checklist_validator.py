#!/usr/bin/env python3
"""
checklist_validator.py
═══════════════════════════════════════════════════════════════════
Validateur de la checklist R0-R8 (framework Shannon / Entropie /
Hormozi / First Principles / Inversion) pour le bot Solana.

À lancer EN FIN DE CHAQUE TÂCHE Manus, avant de déclarer "terminé".

Usage :
    # Mode interactif (saisie au clavier)
    python3 checklist_validator.py

    # Mode fichier (rapport déjà rempli en JSON)
    python3 checklist_validator.py --report rapport.json

    # Vérification automatique du non-régression changelog (R7)
    python3 checklist_validator.py --report rapport.json \
        --changelog /data/solana-bot/CHANGELOG.md

Format attendu du JSON (--report) :
{
  "R0":  {"status": "✅", "note": "ps aux/systemctl/ss -tnp vérifiés"},
  "R1":  {"status": "✅", "note": "exhaustivité des états vérifiée: ..."},
  "R1b": {"status": "N-A", "note": "aucune jonction entre composants"},
  "R2":  {"status": "✅", "note": "intervalle polling réévalué"},
  "R3":  {"status": "✅", "note": "maillon faible = HeliusWS callback"},
  "R4":  {"status": "N-A", "note": "architecture non remise en cause"},
  "R5":  {"status": "✅", "note": "double échec + cache obsolète testés"},
  "R6":  {"status": "✅", "note": "chemin validé, signal CRITICAL -> Telegram + auto-pause"},
  "R7":  {"status": "✅", "note": "grep record_trade OK, grep init_db OK"}
}
"""

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

VALID_STATUSES = {"✅", "❌", "N-A"}

CHECKLIST_DEFINITION = {
    "R0":  "Environnement vérifié (ps aux / systemctl / ss -tnp)",
    "R1":  "Shannon — exhaustivité des états vérifiée (chaque état a un chemin défini)",
    "R1b": "Shannon — format des messages entre composants vérifié (clés JSON/dict identiques)",
    "R2":  "Entropie — intervalles de surveillance réévalués si pertinent",
    "R3":  "Hormozi — maillon le plus faible traité en premier",
    "R4":  "First Principles — architecture remise en question si pertinent",
    "R5":  "Inversion — scénarios d'échec testés (double échec, état intermédiaire, cache obsolète)",
    "R6":  "Test E2E + signal orphelin (chemin emprunté validé + destination/action de tout nouveau signal)",
    "R7":  "Changelog — fixes précédents non régressés (vérifié via grep)",
}

# Règle R0 : ces items ne peuvent JAMAIS être ❌ sans bloquer la tâche.
BLOCKING_ITEMS = set(CHECKLIST_DEFINITION.keys())


@dataclass
class ItemResult:
    code: str
    label: str
    status: str
    note: str

    @property
    def is_blocking_failure(self) -> bool:
        return self.status == "❌"

    @property
    def is_unjustified(self) -> bool:
        # ✅ et N-A doivent toujours être accompagnés d'une justification
        return self.status in {"✅", "N-A"} and not self.note.strip()


def load_report_from_file(path: str) -> dict:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return data


def load_report_interactive() -> dict:
    print("═" * 70)
    print("CHECKLIST R0-R8 — saisie interactive")
    print("═" * 70)
    report = {}
    for code, label in CHECKLIST_DEFINITION.items():
        print(f"\n[{code}] {label}")
        while True:
            status = input("  Statut (✅ / ❌ / N-A) : ").strip()
            if status in VALID_STATUSES:
                break
            print(f"  -> invalide. Choix possibles : {', '.join(VALID_STATUSES)}")
        note = input("  Justification courte : ").strip()
        report[code] = {"status": status, "note": note}
    return report


def grep_changelog(changelog_path: str) -> list[str]:
    """
    Aide R7 : extrait les commandes de vérification (grep ...) listées
    dans le CHANGELOG.md, exécute chacune, et retourne les fixes
    introuvables dans le code (régression détectée).
    """
    import re
    import subprocess

    path = Path(changelog_path)
    if not path.exists():
        return [f"CHANGELOG introuvable : {changelog_path}"]

    text = path.read_text(encoding="utf-8")
    # Cherche les lignes du type : `grep "xxx" fichier.py` dans le changelog
    grep_cmds = re.findall(r"`(grep[^`]+)`", text)

    failures = []
    for cmd in grep_cmds:
        try:
            result = subprocess.run(
                cmd, shell=True, capture_output=True, text=True, timeout=10
            )
            if result.returncode != 0:
                failures.append(f"RÉGRESSION probable — commande sans résultat : {cmd}")
        except Exception as e:
            failures.append(f"Erreur d'exécution pour '{cmd}': {e}")

    return failures


def validate(report: dict) -> tuple[list[ItemResult], bool]:
    results = []
    all_ok = True

    for code, label in CHECKLIST_DEFINITION.items():
        entry = report.get(code)
        if entry is None:
            results.append(ItemResult(code, label, "❌", "MANQUANT dans le rapport"))
            all_ok = False
            continue

        status = entry.get("status", "")
        note = entry.get("note", "")

        if status not in VALID_STATUSES:
            results.append(ItemResult(code, label, "❌", f"statut invalide: '{status}'"))
            all_ok = False
            continue

        item = ItemResult(code, label, status, note)
        results.append(item)

        if item.is_blocking_failure and code in BLOCKING_ITEMS:
            all_ok = False
        if item.is_unjustified:
            all_ok = False

    return results, all_ok


def print_report(results: list[ItemResult], all_ok: bool, changelog_failures: list[str] | None):
    print("\n" + "═" * 70)
    print("RAPPORT CHECKLIST R0-R8")
    print("═" * 70)
    for r in results:
        flag = ""
        if r.is_blocking_failure:
            flag = "  <-- BLOQUANT"
        elif r.is_unjustified:
            flag = "  <-- JUSTIFICATION MANQUANTE"
        print(f"[{r.code:>3}] {r.status}  {r.label}{flag}")
        if r.note:
            print(f"       └─ {r.note}")

    if changelog_failures:
        print("\n" + "─" * 70)
        print("VÉRIFICATION AUTOMATIQUE CHANGELOG (R7)")
        print("─" * 70)
        for f in changelog_failures:
            print(f"  ❌ {f}")
        all_ok = all_ok and len(changelog_failures) == 0

    print("\n" + "═" * 70)
    if all_ok:
        print("✅ CHECKLIST COMPLÈTE — la tâche peut être déclarée TERMINÉE")
    else:
        print("❌ CHECKLIST INCOMPLÈTE — NE PAS déclarer la tâche terminée")
        print("   (corriger les items bloquants ou les justifications manquantes)")
    print("═" * 70)


def main():
    parser = argparse.ArgumentParser(description="Validateur checklist R0-R8")
    parser.add_argument("--report", help="Chemin vers un rapport JSON déjà rempli")
    parser.add_argument("--changelog", help="Chemin vers CHANGELOG.md pour vérif R7 automatique")
    args = parser.parse_args()

    if args.report:
        report = load_report_from_file(args.report)
    else:
        report = load_report_interactive()

    results, all_ok = validate(report)

    changelog_failures = None
    if args.changelog:
        changelog_failures = grep_changelog(args.changelog)

    print_report(results, all_ok, changelog_failures)

    # Code de sortie non-zéro si checklist incomplète -> utilisable en CI/script
    final_ok = all_ok and not (changelog_failures and len(changelog_failures) > 0)
    sys.exit(0 if final_ok else 1)


if __name__ == "__main__":
    main()
