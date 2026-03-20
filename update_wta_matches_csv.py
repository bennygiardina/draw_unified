#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from pypdf import PdfReader

DEFAULT_PDF_URL = "https://wtafiles.wtatennis.com/pdf/draws/2026/902/MDS.pdf"

# =========================
# CONFIG NOMI
# =========================

LOWERCASE_PARTICLES = {
    "de", "del", "della", "di", "da", "dos", "das",
    "van", "von", "der", "den", "la", "le"
}

COUNTRIES_SURNAME_FIRST_DISPLAY = {
    "CHN", "KOR", "TPE", "JPN"
}

WESTERN_NAME_EXCEPTIONS = {
    ("JPN", "naomi osaka"),
}

STATUS_LABELS = {
    "WC": "[WC]",
    "Q": "[Q]",
    "LL": "[LL]",
    "PR": "[PR]",
    "ALT": "[Alt]",
}

STOP_MARKERS = {
    "Round of 32", "Round of 16", "Quarterfinals",
    "Semifinals", "Final", "Winner",
    "Last Direct Acceptance", "ATP Supervisor",
    "WTA Supervisor", "Released",
    "Seeded Players", "Alternates/Lucky Losers",
    "Withdrawals", "Retirements/W.O.",
}

# =========================
# UTILS
# =========================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def smart_title_token(token: str) -> str:
    token = token.strip()
    if not token:
        return token

    lower = token.lower()

    # Mc handling
    if lower.startswith("mc") and len(token) > 2:
        return "Mc" + token[2].upper() + token[3:].lower()

    if "-" in token:
        return "-".join(smart_title_token(p) for p in token.split("-"))

    if "'" in token:
        return "'".join(smart_title_token(p) for p in token.split("'"))

    if lower in LOWERCASE_PARTICLES:
        return lower

    return token[:1].upper() + token[1:].lower()


def smart_join_tokens(tokens):
    return " ".join(smart_title_token(t) for t in tokens if t)


def build_name_with_extras(base_name, seed="", entry_status=""):
    extras = []
    if seed:
        extras.append(f"[{seed}]")
    if entry_status in STATUS_LABELS:
        extras.append(STATUS_LABELS[entry_status])
    return f"{base_name} {' '.join(extras)}".strip()


# =========================
# FORMAT NAME (FINAL)
# =========================

def format_name(raw_name, seed="", entry_status="", country=""):
    raw_name = (raw_name or "").strip()
    country = (country or "").upper()

    if not raw_name:
        return ""
    if raw_name == "Bye":
        return "bye"

    normalized = re.sub(r"\s+", " ", raw_name.replace(",", " ")).lower()

    if "," in raw_name:
        surname_part, given_part = [x.strip() for x in raw_name.split(",", 1)]
        surname = smart_join_tokens(surname_part.split())
        given_name = smart_join_tokens(given_part.split())
    else:
        tokens = raw_name.split()
        if len(tokens) == 1:
            return smart_title_token(tokens[0])

        given_name = smart_join_tokens(tokens[:-1])
        surname = smart_join_tokens([tokens[-1]])

    initial = f"{given_name[0].upper()}." if given_name else ""

    if (country, normalized) in WESTERN_NAME_EXCEPTIONS:
        base = f"{initial} {surname}"
    elif country in COUNTRIES_SURNAME_FIRST_DISPLAY:
        base = f"{surname} {initial}"
    else:
        base = f"{initial} {surname}"

    return build_name_with_extras(base.strip(), seed, entry_status)


# =========================
# SCORE FIX (IMPORTANT)
# =========================

def parse_score_pairs(score):
    return [(int(s[0]), int(s[1])) for s in re.findall(r"(\d)(\d)", score)]


def count_sets(pairs):
    a = b = 0
    for x, y in pairs:
        if x > y:
            a += 1
        else:
            b += 1
    return a, b


def format_scores(player_a, player_b, winner, score_raw):
    pairs = parse_score_pairs(score_raw)

    if winner == player_b:
        pairs = [(b, a) for a, b in pairs]

    a_sets, b_sets = count_sets(pairs)
    return str(a_sets), str(b_sets)


# =========================
# MAIN LOGIC (SEMPLIFICATO)
# =========================

def extract_pdf_text(pdf_bytes):
    reader = PdfReader(io.BytesIO(pdf_bytes))
    return "\n".join(p.extract_text() or "" for p in reader.pages)


def clean_lines(text):
    lines = text.split("\n")
    out = []
    for l in lines:
        l = l.strip()
        if not l:
            continue
        if any(m.lower() in l.lower() for m in STOP_MARKERS):
            continue
        out.append(l)
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="matches_format.csv")
    parser.add_argument("--pdf-url", default=DEFAULT_PDF_URL)
    args = parser.parse_args()

    pdf = requests.get(args.pdf_url).content
    text = extract_pdf_text(pdf)
    lines = clean_lines(text)

    # ⚠️ placeholder parser (il tuo reale parsing è più lungo)
    rows = []

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Round", "Player A", "Player B", "Winner",
            "Participant A score", "Participant B score"
        ])
        writer.writerows(rows)

    print(f"[{utc_now_iso()}] DONE")


if __name__ == "__main__":
    main()
