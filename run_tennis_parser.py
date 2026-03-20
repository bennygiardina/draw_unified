#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

DEFAULT_ATP_SCRIPT = "update_atp_matches_csv.py"
DEFAULT_WTA_SCRIPT = "update_wta_matches_csv.py"


def normalize_tour(tour: str) -> str:
    t = (tour or "").strip().lower()
    if t not in {"atp", "wta"}:
        raise ValueError("--tour deve essere 'atp' oppure 'wta'")
    return t


def resolve_script_path(script_name: str) -> Path:
    here = Path(__file__).resolve().parent
    candidate = here / script_name
    if candidate.exists():
        return candidate
    return Path(script_name).expanduser().resolve()


def run_subprocess(cmd: list[str]) -> int:
    proc = subprocess.run(cmd)
    return proc.returncode


def main() -> int:
    parser = argparse.ArgumentParser(description="Dispatcher unico per parser ATP/WTA.")
    parser.add_argument("--tour", required=True, help="atp oppure wta")
    parser.add_argument("--output", default="matches_format.csv", help="CSV di output")

    # ATP
    parser.add_argument("--tournament-url", default="", help="URL torneo ATP")
    parser.add_argument("--draw-page", default="", help="URL draw ATP")
    parser.add_argument("--results-page", default="", help="URL results ATP")
    parser.add_argument("--tournament-id", default="", help="ID torneo ATP")
    parser.add_argument("--year", default="", help="Anno torneo ATP")

    # WTA
    parser.add_argument("--pdf-url", default="", help="URL PDF WTA")

    # common
    parser.add_argument("--watch", action="store_true", help="Modalità watch")
    parser.add_argument("--interval", type=int, default=1800, help="Intervallo in secondi")
    parser.add_argument("--run-tests", action="store_true", help="Esegue i test dello script selezionato")

    # explicit paths
    parser.add_argument("--atp-script", default=DEFAULT_ATP_SCRIPT, help="Path script ATP")
    parser.add_argument("--wta-script", default=DEFAULT_WTA_SCRIPT, help="Path script WTA")

    args = parser.parse_args()
    tour = normalize_tour(args.tour)

    if tour == "atp":
        script_path = resolve_script_path(args.atp_script)
        cmd = [sys.executable, str(script_path), "--output", args.output]

        if args.tournament_url:
            cmd += ["--tournament-url", args.tournament_url]
        if args.draw_page:
            cmd += ["--draw-page", args.draw_page]
        if args.results_page:
            cmd += ["--results-page", args.results_page]
        if args.tournament_id:
            cmd += ["--tournament-id", args.tournament_id]
        if args.year:
            cmd += ["--year", str(args.year)]
        if args.watch:
            cmd += ["--watch"]
        if args.interval:
            cmd += ["--interval", str(args.interval)]
        if args.run_tests:
            cmd += ["--run-tests"]

    else:
        script_path = resolve_script_path(args.wta_script)
        cmd = [sys.executable, str(script_path), "--output", args.output]

        if args.pdf_url:
            cmd += ["--pdf-url", args.pdf_url]
        if args.watch:
            cmd += ["--watch"]
        if args.interval:
            cmd += ["--interval", str(args.interval)]
        if args.run_tests:
            cmd += ["--run-tests"]

    if not script_path.exists():
        print(f"ERRORE: script non trovato: {script_path}", file=sys.stderr)
        return 2

    return run_subprocess(cmd)


if __name__ == "__main__":
    raise SystemExit(main())
