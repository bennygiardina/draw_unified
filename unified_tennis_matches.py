#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
import sys
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================
# Defaults
# =========================
DEFAULT_ATP_TOURNAMENT_URL = "https://www.atptour.com/en/scores/current/miami/403"
DEFAULT_ATP_DRAW_PAGE = ""
DEFAULT_ATP_RESULTS_PAGE = ""
DEFAULT_ATP_FALLBACK_PDF = "https://www.protennislive.com/posting/{year}/{tournament_id}/mds.pdf"
DEFAULT_ATP_TOURNAMENT_ID = "403"

DEFAULT_WTA_TOURNAMENT_URL = "https://www.wtatennis.com/tournaments/miami-open/"
DEFAULT_WTA_PDF_URL = "https://wtafiles.wtatennis.com/pdf/draws/2026/902/MDS.pdf"

LOWERCASE_PARTICLES = {
    "de", "del", "della", "di", "da", "dos", "das",
    "van", "von", "der", "den", "la", "le"
}

COUNTRIES_SURNAME_FIRST_DISPLAY = {
    "CHN", "KOR", "TPE", "JPN",
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
    "Round of 32", "Round of 16", "Quarterfinals", "Semifinals", "Final", "Winner",
    "Last Direct Acceptance", "ATP Supervisor", "WTA Supervisor", "Released",
    "Seeded Players", "Alternates/Lucky Losers", "Withdrawals", "Retirements/W.O.",
}

ROUND_HEADER_TO_LABEL = {
    "Round of 128": "1° turno",
    "Round of 96": "1° turno",
    "Round of 64": "1° turno",
    "Round of 48": "1° turno",
    "Round of 32": "1° turno",
    "Round of 24": "1° turno",
    "Round of 16": "Ottavi di finale",
    "Quarterfinals": "Quarti di finale",
    "Semifinals": "Semifinali",
    "Final": "Finale",
}

RESULT_RETIREMENT_RE = re.compile(r"\b(?:RET|Ret|ret|retired|retirement|RIT\.?)\b", re.IGNORECASE)
RESULT_WALKOVER_RE = re.compile(r"\b(?:W/O|WO|walkover|walk-over)\b", re.IGNORECASE)
ELLIPSIS_RE = re.compile(r"(?:\.\.\.|…)")
NAME_TOKEN_RE = re.compile(r"(?:[A-Z][a-z]{0,4}\.)[ \t]+[A-Z][A-Za-z'`.-]+(?:[ \t]+[A-Z][A-Za-z'`.-]+)*")
SCORE_TOKEN_RE = re.compile(r"(?:W/O|WO|RET|Ret|ret|\d{2}(?:\(\d+\))?(?:[ \t]+\d{2}(?:\(\d+\))?){0,4})")


# =========================
# Shared helpers
# =========================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def smart_title_token(token: str) -> str:
    token = token.strip()
    if not token:
        return token
    if "-" in token:
        return "-".join(smart_title_token(part) for part in token.split("-"))
    if "'" in token:
        return "'".join(smart_title_token(part) for part in token.split("'"))
    lower = token.lower()
    if lower.startswith("mc") and len(token) > 2:
        return "Mc" + token[2].upper() + token[3:].lower()
    if lower in LOWERCASE_PARTICLES:
        return lower
    return token[:1].upper() + token[1:].lower()

def smart_join_tokens(tokens: list[str]) -> str:
    return " ".join(smart_title_token(tok) for tok in tokens if tok)


def get_round_label(round_size: int, initial_draw_size: int) -> str:
    if round_size == 1:
        return "Finale"
    if round_size == 2:
        return "Semifinali"
    if round_size == 4:
        return "Quarti di finale"
    if round_size == 8:
        return "Ottavi di finale"

    known = {
        128: {64: "1° turno", 32: "2° turno", 16: "3° turno"},
        96: {48: "1° turno", 32: "2° turno", 16: "3° turno"},
        64: {32: "1° turno", 16: "2° turno"},
        56: {32: "1° turno", 16: "2° turno"},
        48: {24: "1° turno", 16: "2° turno"},
        32: {16: "1° turno"},
        28: {16: "1° turno"},
        24: {12: "1° turno"},
        16: {8: "1° turno"},
    }
    return known.get(initial_draw_size, {}).get(round_size, f"Round {round_size}")


def map_atp_round_to_label(round_text: str) -> str | None:
    return ROUND_HEADER_TO_LABEL.get((round_text or "").strip())


def build_name_with_extras(base_name: str, seed: str = "", entry_status: str = "") -> str:
    extras = []
    if seed:
        extras.append(f"[{seed}]")
    if entry_status in STATUS_LABELS:
        extras.append(STATUS_LABELS[entry_status])
    return f"{base_name} {' '.join(extras)}".strip() if extras else base_name


def name_has_ellipsis(raw_name: str) -> bool:
    return bool(ELLIPSIS_RE.search(raw_name or ""))


def looks_like_surname_first(raw_name: str) -> bool:
    raw_name = (raw_name or "").strip()
    if not raw_name:
        return False

    tokens = raw_name.replace(",", "").split()
    if len(tokens) < 2:
        return False

    if tokens[0].isupper() and not tokens[1].isupper():
        return True

    return False


def format_name(raw_name: str, seed: str = "", entry_status: str = "", country: str = "") -> str:
    raw_name = (raw_name or "").strip()
    country = (country or "").strip().upper()
    if not raw_name:
        return ""
    if raw_name == "Bye":
        return "bye"

    normalized_raw = re.sub(r"\s+", " ", raw_name.replace(",", " ")).strip().lower()

    if "," in raw_name:
        surname_part, given_part = [x.strip() for x in raw_name.split(",", 1)]
        surname = smart_join_tokens(surname_part.split())
        given_name = smart_join_tokens(given_part.split())
    else:
        tokens = raw_name.replace(",", "").split()
        if not tokens:
            return ""
        if len(tokens) == 1:
            base_name = smart_title_token(tokens[0])
            return build_name_with_extras(base_name, seed=seed, entry_status=entry_status)

        if looks_like_surname_first(raw_name):
            surname_tokens = [tokens[0]]
            given_tokens = tokens[1:]
        else:
            given_tokens = tokens[:-1]
            surname_tokens = [tokens[-1]]

        surname = smart_join_tokens(surname_tokens)
        given_name = smart_join_tokens(given_tokens)

    first_initial = f"{given_name[0].upper()}." if given_name else ""

    if (country, normalized_raw) in WESTERN_NAME_EXCEPTIONS:
        base_name = f"{first_initial} {surname}".strip()
    elif country in COUNTRIES_SURNAME_FIRST_DISPLAY:
        base_name = f"{surname} {first_initial}".strip()
    else:
        base_name = f"{first_initial} {surname}".strip()

    return build_name_with_extras(base_name, seed=seed, entry_status=entry_status)

def normalize_person_name_for_matching(name: str) -> str:
    name = (name or "").strip().lower()
    name = re.sub(r"\[[^\]]+\]", "", name)
    name = name.replace(".", " ")
    name = re.sub(r"\s+", " ", name).strip()
    return name


def surname_from_name(name: str) -> str:
    parts = normalize_person_name_for_matching(name).split()
    if not parts:
        return ""
    if len(parts) >= 2 and len(parts[-1]) == 1:
        return parts[0]
    return parts[-1]


def first_initial_from_name(name: str) -> str:
    parts = normalize_person_name_for_matching(name).split()
    if len(parts) < 2:
        return ""
    if len(parts[-1]) == 1:
        return parts[-1][0]
    return parts[0][0] if parts[0] else ""


def abbreviated_name_matches(candidate: str, token_name: str) -> bool:
    cand_norm = normalize_person_name_for_matching(candidate)
    token_norm = normalize_person_name_for_matching(token_name)
    if cand_norm == token_norm:
        return True
    cand_surname = surname_from_name(candidate)
    token_surname = surname_from_name(token_name)
    if not cand_surname or cand_surname != token_surname:
        return False
    cand_initial = first_initial_from_name(candidate)
    token_initial = first_initial_from_name(token_name)
    return not token_initial or cand_initial == token_initial


def classify_result_outcome(score_raw: str) -> str:
    score_raw = (score_raw or "").strip()
    if not score_raw:
        return "unknown"
    if RESULT_WALKOVER_RE.search(score_raw):
        return "walkover"
    if RESULT_RETIREMENT_RE.search(score_raw):
        return "retirement"
    return "completed"


def parse_score_pairs_from_score_raw(score_raw: str) -> list[tuple[int, int]]:
    score_raw = (score_raw or "").strip()
    if not score_raw:
        return []

    pairs: list[tuple[int, int]] = []
    for tok in re.findall(r"\d{1,2}-\d{1,2}(?:\(\d+\))?|\d{2}(?:\(\d+\))?", score_raw):
        m = re.match(r"(\d{1,2})-(\d{1,2})", tok)
        if m:
            pairs.append((int(m.group(1)), int(m.group(2))))
            continue
        m = re.match(r"(\d)(\d)(?:\(\d+\))?$", tok)
        if m:
            pairs.append((int(m.group(1)), int(m.group(2))))
    return pairs


def is_completed_set_score(a: int, b: int) -> bool:
    if a == 6 and 0 <= b <= 4:
        return True
    if b == 6 and 0 <= a <= 4:
        return True
    if (a, b) in {(7, 5), (5, 7), (7, 6), (6, 7)}:
        return True
    return False


def count_sets_from_pairs(pairs: list[tuple[int, int]]) -> tuple[int, int]:
    a_sets = 0
    b_sets = 0
    for a, b in pairs:
        if a > b:
            a_sets += 1
        elif b > a:
            b_sets += 1
    return a_sets, b_sets


def csv_bytes(rows: list[dict]) -> bytes:
    fieldnames = ["Round", "Player A", "Player B", "Winner", "Participant A score", "Participant B score"]
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8-sig")


def write_csv_if_changed(output_path: Path, data: bytes) -> bool:
    if output_path.exists() and output_path.read_bytes() == data:
        return False
    output_path.write_bytes(data)
    return True


def make_requests_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD", "OPTIONS"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0 Safari/537.36"
        ),
        "Accept": "text/html,application/pdf,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,it;q=0.8",
        "Connection": "keep-alive",
    })
    return session


def http_get(session: requests.Session, url: str, timeout: int = 60) -> requests.Response:
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp


def extract_pdf_text(pdf_bytes: bytes) -> list[str]:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    return [(page.extract_text() or "") for page in reader.pages]


def extract_released_at(pages_text: Iterable[str]) -> str:
    for page_text in pages_text:
        m = re.search(r"RELEASED\s*([0-9]{1,2}\s+[A-Za-z]{3}\s+[0-9]{4}\s+[0-9:]{4,8}\s+[AP]M)", page_text)
        if m:
            return m.group(1)
        lines = [line.strip() for line in page_text.splitlines() if line.strip()]
        for i, line in enumerate(lines):
            if line == "Released" and i + 1 < len(lines):
                return lines[i + 1]
    return ""


# =========================
# ATP parser
# =========================
def is_tournament_metadata(text: str) -> bool:
    if not text:
        return False
    t = text.strip().lower()
    month_words = [
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december",
    ]
    if any(month in t for month in month_words):
        return True
    if "usd" in t or "hard" in t or "clay" in t or "grass" in t or "|" in t:
        return True
    return False


def normalize_tournament_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    return url.rstrip("/")


def infer_tournament_id_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    parts = path.split("/")
    for i in range(len(parts) - 1):
        if parts[i] in {"current", "current-challenger"} and i + 2 < len(parts):
            candidate = parts[i + 2]
            if candidate.isdigit():
                return candidate
    matches = re.findall(r"/(\d+)(?:/|$)", path)
    if matches:
        return matches[-1]
    return ""


def infer_draw_page_url(tournament_url: str) -> str:
    url = normalize_tournament_url(tournament_url)
    if not url:
        return ""
    if url.endswith("/draws"):
        return url
    if url.endswith("/results"):
        return re.sub(r"/results$", "/draws", url)
    return f"{url}/draws"


def infer_results_page_url_from_tournament(tournament_url: str) -> str:
    url = normalize_tournament_url(tournament_url)
    if not url:
        return ""
    if url.endswith("/results"):
        return url
    if url.endswith("/draws"):
        return re.sub(r"/draws$", "/results", url)
    return f"{url}/results"


def infer_results_page_url_from_draw(draw_page_url: str) -> str:
    url = normalize_tournament_url(draw_page_url)
    if not url:
        return ""
    if url.endswith("/results"):
        return url
    if url.endswith("/draws"):
        return re.sub(r"/draws$", "/results", url)
    return f"{url}/results"


def discover_pdf_url(session: requests.Session, draw_page_url: str, fallback_pdf_url: str) -> str:
    try:
        resp = http_get(session, draw_page_url, timeout=30)
        soup = BeautifulSoup(resp.text, "html.parser")
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if "protennislive.com" in href and href.lower().endswith("mds.pdf"):
                return href
    except requests.RequestException as exc:
        print(
            f"[{utc_now_iso()}] WARN | draw page non raggiungibile, uso fallback PDF | "
            f"url={draw_page_url} | err={exc}",
            file=sys.stderr,
            flush=True,
        )
    return fallback_pdf_url


def is_score_line(text: str) -> bool:
    text = (text or "").strip()
    return bool(text and re.fullmatch(r"\d{1,2}\s+\d{1,2}(?:\s+\d{1,2})*", text))


def parse_draw_line(line: str) -> dict | None:
    line = re.sub(r"\s+", " ", line).strip()
    if not line:
        return None
    if is_tournament_metadata(line):
        return None

    m = re.match(
        r"^(?P<pos>\d+)\s+(?:(?P<status>WC|Q|LL|PR|ALT)\s+)?(?:(?P<seed>\d{1,2})\s+)?"
        r"(?P<name>.+?)(?:\s+(?P<country>[A-Z]{3}))?$",
        line,
    )
    if not m:
        return None

    raw_name = (m.group("name") or "").strip()
    if not raw_name:
        return None

    slot_type = "bye" if raw_name == "Bye" else "player"
    return {
        "draw_position": int(m.group("pos")),
        "entry_status": (m.group("status") or "").upper(),
        "seed": m.group("seed") or "",
        "raw_name": raw_name,
        "country": (m.group("country") or "").upper(),
        "slot_type": slot_type,
        "player_name": format_name(raw_name, seed=m.group("seed") or "", entry_status=(m.group("status") or "").upper()),
    }


def parse_draw_positions(pages_text: list[str]) -> list[dict]:
    positions: list[dict] = []
    seen_positions = set()
    for page_text in pages_text:
        for raw_line in page_text.splitlines():
            parsed = parse_draw_line(raw_line)
            if not parsed:
                continue
            pos = parsed["draw_position"]
            if pos in seen_positions:
                continue
            seen_positions.add(pos)
            positions.append(parsed)
    positions.sort(key=lambda p: p["draw_position"])
    if not positions:
        raise RuntimeError("Nessuna posizione draw ATP trovata nel PDF")
    return positions


def format_scores_from_result(player_a: str, player_b: str, winner: str, res: dict) -> tuple[str, str]:
    score_raw = (res.get("score_raw") or "").strip()
    outcome = classify_result_outcome(score_raw)
    if outcome == "walkover":
        if winner == player_a:
            return "W/O", ""
        if winner == player_b:
            return "", "W/O"
        return "", ""

    pairs = parse_score_pairs_from_score_raw(score_raw)
    if not pairs:
        return "", ""

    # Try respecting the order from results page if players are available.
    p1 = normalize_person_name_for_matching(res.get("player1_name_raw", ""))
    p2 = normalize_person_name_for_matching(res.get("player2_name_raw", ""))
    a = normalize_person_name_for_matching(player_a)
    b = normalize_person_name_for_matching(player_b)
    direct_order = (a == p1 and b == p2)
    reverse_order = (a == p2 and b == p1)

    a_sets, b_sets = count_sets_from_pairs(pairs)
    if reverse_order:
        a_sets, b_sets = b_sets, a_sets

    if outcome == "retirement":
        completed_pairs = [(x, y) for x, y in pairs if is_completed_set_score(x, y)]
        incomplete_pairs = [(x, y) for x, y in pairs if not is_completed_set_score(x, y)]
        c1, c2 = count_sets_from_pairs(completed_pairs)
        if reverse_order:
            c1, c2 = c2, c1
        if incomplete_pairs and len(completed_pairs) == 2:
            c1, c2 = 1, 1
        if winner == player_a:
            return str(c1), f"(rit.) {c2}"
        if winner == player_b:
            return f"(rit.) {c1}", str(c2)

    return str(a_sets), str(b_sets)


def extract_json_candidates_from_html(html: str) -> list[str]:
    candidates = []
    for m in re.finditer(r"<script[^>]*>(.*?)</script>", html, flags=re.IGNORECASE | re.DOTALL):
        script = m.group(1).strip()
        if not script:
            continue
        if "{" in script or "[" in script:
            candidates.append(script)
    return candidates


def _walk_json(obj):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk_json(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from _walk_json(item)


def dedupe_results(results: list[dict]) -> list[dict]:
    seen = set()
    out = []
    for r in results:
        key = (
            r.get("round", ""),
            r.get("winner_name_raw", ""),
            r.get("score_raw", ""),
            r.get("player1_name_raw", ""),
            r.get("player2_name_raw", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def fetch_abbrev_names_from_draw_page(draw_page_url: str, session: requests.Session | None = None) -> list[str]:
    session = session or make_requests_session()
    resp = http_get(session, draw_page_url, timeout=30)
    soup = BeautifulSoup(resp.text, "html.parser")
    texts = []
    for el in soup.select("a, span, div"):
        text = " ".join(el.get_text(" ", strip=True).split())
        if re.fullmatch(r"[A-Z][a-z]{0,4}\.\s+[A-Z][A-Za-z'`.-]+(?:\s+[A-Z][A-Za-z'`.-]+)*", text):
            texts.append(text)
    return texts


def repair_truncated_names_from_draw_page(positions: list[dict], draw_page_url: str, session: requests.Session | None = None) -> list[dict]:
    try:
        draw_names = fetch_abbrev_names_from_draw_page(draw_page_url, session=session)
    except requests.RequestException:
        return positions
    if len(draw_names) < len(positions):
        return positions

    repaired = []
    for idx, p in enumerate(positions):
        p = dict(p)
        if name_has_ellipsis(p.get("raw_name", "")):
            p["player_name"] = build_name_with_extras(
                draw_names[idx],
                seed=p.get("seed", ""),
                entry_status=p.get("entry_status", ""),
            )
        repaired.append(p)
    return repaired


def fetch_results_page(results_page_url: str, session: requests.Session | None = None) -> list[dict]:
    session = session or make_requests_session()
    resp = http_get(session, results_page_url, timeout=30)
    html = resp.text
    soup = BeautifulSoup(html, "html.parser")
    results: list[dict] = []

    for candidate in extract_json_candidates_from_html(html):
        try:
            data = json.loads(candidate)
        except Exception:
            continue
        for obj in _walk_json(data):
            if not isinstance(obj, dict):
                continue
            round_text = winner_name = score_raw = player1 = player2 = None
            for key in ("round", "roundName", "Round", "matchRound"):
                if key in obj and isinstance(obj[key], str):
                    round_text = obj[key]
                    break
            for key in ("winnerName", "WinnerName", "winningPlayerName", "winner"):
                if key in obj and isinstance(obj[key], str):
                    winner_name = obj[key]
                    break
            for key in ("score", "Score", "matchScore", "result"):
                if key in obj and isinstance(obj[key], str):
                    score_raw = obj[key]
                    break
            for key in ("player1Name", "Player1Name", "homePlayerName"):
                if key in obj and isinstance(obj[key], str):
                    player1 = obj[key]
                    break
            for key in ("player2Name", "Player2Name", "awayPlayerName"):
                if key in obj and isinstance(obj[key], str):
                    player2 = obj[key]
                    break
            label = map_atp_round_to_label(round_text or "")
            if not label:
                continue
            if not winner_name and not score_raw and not (player1 and player2):
                continue
            results.append({
                "round": label,
                "winner_name_raw": (winner_name or "").strip(),
                "score_raw": (score_raw or "").strip(),
                "player1_name_raw": (player1 or "").strip(),
                "player2_name_raw": (player2 or "").strip(),
                "outcome_type": classify_result_outcome(score_raw or ""),
                "source": "results_page",
            })

    if results:
        return dedupe_results(results)

    text = re.sub(r"\s+", " ", soup.get_text(" ", strip=True))
    pattern = re.compile(
        r"(Round of 128|Round of 96|Round of 64|Round of 48|Round of 32|Round of 24|Round of 16|Quarterfinals|Semifinals|Final)"
        r".*?Game Set and Match\s+([A-Za-zÀ-ÿ'`\-.\s]+?)\.\s+"
        r"(?:\2)\s+wins the match\s+([0-9\-\(\)\sA-Za-z/.]+?)\s*\.",
        re.IGNORECASE,
    )
    for m in pattern.finditer(text):
        round_text = m.group(1).strip()
        winner_name = " ".join(m.group(2).split())
        score_raw = " ".join(m.group(3).split())
        label = map_atp_round_to_label(round_text)
        if not label:
            continue
        results.append({
            "round": label,
            "winner_name_raw": winner_name,
            "score_raw": score_raw,
            "player1_name_raw": "",
            "player2_name_raw": "",
            "outcome_type": classify_result_outcome(score_raw),
            "source": "results_page",
        })
    return dedupe_results(results)


def group_results_by_round(results: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for r in results:
        grouped.setdefault(r["round"], []).append(r)
    return grouped


def resolve_winner_from_results_page(player_a: str, player_b: str, winner_full_name: str) -> str:
    a_norm = normalize_person_name_for_matching(player_a)
    b_norm = normalize_person_name_for_matching(player_b)
    w_norm = normalize_person_name_for_matching(winner_full_name)
    if a_norm and a_norm == w_norm:
        return player_a
    if b_norm and b_norm == w_norm:
        return player_b

    a_surname = surname_from_name(player_a)
    b_surname = surname_from_name(player_b)
    w_surname = surname_from_name(winner_full_name)
    if w_surname == a_surname and w_surname != b_surname:
        return player_a
    if w_surname == b_surname and w_surname != a_surname:
        return player_b

    a_initial = first_initial_from_name(player_a)
    b_initial = first_initial_from_name(player_b)
    w_initial = first_initial_from_name(winner_full_name)
    if w_surname == a_surname and w_initial and w_initial == a_initial and not (w_surname == b_surname and w_initial == b_initial):
        return player_a
    if w_surname == b_surname and w_initial and w_initial == b_initial and not (w_surname == a_surname and w_initial == a_initial):
        return player_b
    return ""


def match_result_to_players(player_a: str, player_b: str, res: dict) -> bool:
    p1 = normalize_person_name_for_matching(res.get("player1_name_raw", ""))
    p2 = normalize_person_name_for_matching(res.get("player2_name_raw", ""))
    a = normalize_person_name_for_matching(player_a)
    b = normalize_person_name_for_matching(player_b)
    if p1 and p2:
        return {a, b} == {p1, p2}
    winner = res.get("winner_name_raw", "")
    return bool(resolve_winner_from_results_page(player_a, player_b, winner))


def build_match_rows_atp(positions: list[dict], round_results: dict[str, list[dict]]) -> list[dict]:
    current = [{"name": p["player_name"], "slot_type": p["slot_type"]} for p in positions]
    match_rows: list[dict] = []
    initial_size = len(current)

    while len(current) > 1:
        round_size = len(current) // 2
        round_label = get_round_label(round_size, initial_size)
        next_round = []
        results_for_round = round_results.get(round_label, [])
        used = [False] * len(results_for_round)

        for i in range(0, len(current), 2):
            a_name = current[i]["name"]
            b_name = current[i + 1]["name"]
            winner = ""
            a_sets = ""
            b_sets = ""

            if a_name == "bye" and b_name and b_name != "bye":
                winner = b_name
            elif b_name == "bye" and a_name and a_name != "bye":
                winner = a_name
            elif a_name == "bye" and b_name == "bye":
                winner = ""

            if not winner:
                for idx, res in enumerate(results_for_round):
                    if used[idx] or not match_result_to_players(a_name, b_name, res):
                        continue
                    candidate_winner = resolve_winner_from_results_page(a_name, b_name, res.get("winner_name_raw", ""))
                    if not candidate_winner:
                        continue
                    used[idx] = True
                    winner = candidate_winner
                    a_sets, b_sets = format_scores_from_result(a_name, b_name, winner, res)
                    break

            match_rows.append({
                "Round": round_label,
                "Player A": a_name,
                "Player B": b_name,
                "Winner": winner,
                "Participant A score": a_sets,
                "Participant B score": b_sets,
            })
            next_round.append({"name": winner, "slot_type": "player" if winner else "unknown"})
        current = next_round

    return match_rows


def resolve_runtime_urls(tournament_url: str, draw_page_url: str, results_page_url: str, tournament_id: str, year: int) -> tuple[str, str, str, str]:
    tournament_url = normalize_tournament_url(tournament_url or DEFAULT_ATP_TOURNAMENT_URL)
    draw_page_url = normalize_tournament_url(draw_page_url or DEFAULT_ATP_DRAW_PAGE)
    results_page_url = normalize_tournament_url(results_page_url or DEFAULT_ATP_RESULTS_PAGE)
    tournament_id = (tournament_id or DEFAULT_ATP_TOURNAMENT_ID or "").strip()

    if tournament_url:
        if not draw_page_url:
            draw_page_url = infer_draw_page_url(tournament_url)
        if not results_page_url:
            results_page_url = infer_results_page_url_from_tournament(tournament_url)

    if not draw_page_url and results_page_url:
        draw_page_url = re.sub(r"/results$", "/draws", results_page_url)
    if not results_page_url and draw_page_url:
        results_page_url = infer_results_page_url_from_draw(draw_page_url)
    if not draw_page_url:
        raise ValueError("Devi specificare --tournament-url oppure --draw-page")
    if not tournament_id:
        tournament_id = infer_tournament_id_from_url(draw_page_url)
    if not tournament_id and tournament_url:
        tournament_id = infer_tournament_id_from_url(tournament_url)
    if not tournament_id:
        raise ValueError("Impossibile ricavare tournament_id dall'URL. Passa --tournament-id")
    fallback_pdf_url = DEFAULT_ATP_FALLBACK_PDF.format(year=year, tournament_id=tournament_id)
    return draw_page_url, results_page_url, tournament_id, fallback_pdf_url


def fetch_and_build_rows_atp(tournament_url: str, draw_page_url: str, results_page_url: str, tournament_id: str, year: int) -> tuple[list[dict], dict]:
    draw_page_url, results_page_url, tournament_id, fallback_pdf_url = resolve_runtime_urls(
        tournament_url=tournament_url,
        draw_page_url=draw_page_url,
        results_page_url=results_page_url,
        tournament_id=tournament_id,
        year=year,
    )
    session = make_requests_session()
    pdf_url = discover_pdf_url(session, draw_page_url, fallback_pdf_url)
    pdf_resp = http_get(session, pdf_url, timeout=60)
    pages_text = extract_pdf_text(pdf_resp.content)
    positions = parse_draw_positions(pages_text)
    positions = repair_truncated_names_from_draw_page(positions, draw_page_url, session=session)
    results_list = fetch_results_page(results_page_url, session=session)
    rows = build_match_rows_atp(positions, group_results_by_round(results_list))
    return rows, {
        "tour": "atp",
        "source_draw_page": draw_page_url,
        "source_results_page": results_page_url,
        "source_pdf": pdf_url,
        "released_at": extract_released_at(pages_text),
        "fetched_at": utc_now_iso(),
        "positions": len(positions),
        "matches": len(rows),
        "results_found": len(results_list),
    }


# =========================
# WTA parser
# =========================
def wta_extract_half_section(page_text: str, start_pos: int) -> tuple[str, str]:
    marker = "SINGLES MAIN DRAW TOP HALF" if start_pos == 1 else "SINGLES MAIN DRAW BOTTOM HALF"
    start = page_text.find(marker)
    if start == -1:
        raise RuntimeError(f"Sezione '{marker}' non trovata nel PDF")
    section = page_text[start + len(marker):]
    round_marker = section.find("Round of 128")
    if round_marker == -1:
        raise RuntimeError("Blocco Round of 128 non trovato nel PDF WTA")
    return section[:round_marker], section[round_marker:]


def wta_parse_entry_body(raw: str) -> dict:
    raw = re.sub(r"\s+", " ", raw).strip()
    if raw == "Bye":
        return {
            "seed": "",
            "entry_status": "",
            "raw_name": "Bye",
            "country": "",
            "slot_type": "bye",
            "player_name": "bye",
        }

    m = re.match(r"^(?:(WC|Q|LL|ALT))?\s*(?:(\d{1,2}))?\s*(.*)$", raw)
    if not m:
        raise RuntimeError(f"Entry non parsabile: {raw}")

    entry_status = (m.group(1) or "").upper()
    seed = m.group(2) or ""
    body = (m.group(3) or "").strip()

    country = ""
    m_country = re.match(r"^(.*?)([A-Z]{3})$", body)
    if m_country and "," in m_country.group(1):
        body = m_country.group(1).strip()
        country = m_country.group(2)
    else:
        m_country = re.match(r"^(.*?)\s+([A-Z]{3})$", body)
        if m_country and "," in m_country.group(1):
            body = m_country.group(1).strip()
            country = m_country.group(2)

    raw_name = body.strip()
    return {
        "seed": seed,
        "entry_status": entry_status,
        "raw_name": raw_name,
        "country": country,
        "slot_type": "player",
        "player_name": format_name(raw_name, seed=seed, entry_status=entry_status, country=country),
    }


def wta_parse_half_positions(page_text: str, start_pos: int, end_pos: int) -> tuple[list[dict], str]:
    section, _ = wta_extract_half_section(page_text, start_pos)
    positions: list[dict] = []
    cursor = 0
    first_result_index = None

    for pos in range(start_pos, end_pos + 1):
        start_match = re.search(rf"(?<!\d){pos}", section[cursor:])
        if not start_match:
            raise RuntimeError(f"Posizione {pos} non trovata nel PDF")
        start_idx = cursor + start_match.start()

        if pos < end_pos:
            next_match = re.search(rf"(?<!\d){pos + 1}", section[start_idx + 1:])
            if not next_match:
                raise RuntimeError(f"Posizione successiva {pos + 1} non trovata nel PDF")
            end_idx = start_idx + 1 + next_match.start()
        else:
            result_match = NAME_TOKEN_RE.search(section, start_idx)
            end_idx = result_match.start() if result_match else len(section)
            first_result_index = end_idx

        segment = section[start_idx:end_idx].strip()
        segment = re.sub(rf"^{pos}\s*", "", segment, count=1)
        parsed = wta_parse_entry_body(segment)
        parsed["draw_position"] = pos
        positions.append(parsed)
        cursor = end_idx

    result_block = section[first_result_index:].strip() if first_result_index is not None else ""
    return positions, result_block


def wta_tokenize_result_block(result_block: str) -> list[dict]:
    lines = result_block.split("\n")
    filtered_lines = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if any(marker.lower() in line.lower() for marker in STOP_MARKERS):
            continue
        filtered_lines.append(line)

    result_block = "\n".join(filtered_lines)
    token_re = re.compile(
        r"(?:[A-Z][a-z]{0,4}\.[ 	]+[A-Z][A-Za-z'`.-]+(?:[ 	]+[A-Z][A-Za-z'`.-]+)*)"
        r"|(?:W/O|WO|RET|Ret|ret|\d{2}(?:\(\d+\))?(?:[ 	]+\d{2}(?:\(\d+\))?){0,4})"
    )

    tokens: list[dict] = []
    for match in token_re.finditer(result_block):
        value = " ".join(match.group(0).split())
        if re.fullmatch(r"W/O|WO|RET|Ret|ret|\d{2}(?:\(\d+\))?(?: \d{2}(?:\(\d+\))?){0,4}", value):
            tokens.append({"type": "score", "value": value})
        else:
            tokens.append({"type": "name", "value": value})
    return tokens

def parse_wta_pdf(pages_text: list[str]) -> tuple[list[dict], list[dict]]:
    if len(pages_text) < 2:
        raise RuntimeError("Il PDF WTA atteso deve avere almeno 2 pagine")

    # Miami WTA MDS has 128 positions split 1-64 and 65-128.
    top_positions, top_results_block = wta_parse_half_positions(pages_text[0], 1, 64)
    bottom_positions, bottom_results_block = wta_parse_half_positions(pages_text[1], 65, 128)
    positions = top_positions + bottom_positions
    result_tokens = wta_tokenize_result_block(top_results_block) + wta_tokenize_result_block(bottom_results_block)

    if len(positions) != 128:
        raise RuntimeError(f"Attese 128 posizioni, trovate {len(positions)}")
    return positions, result_tokens


def format_scores_from_winner_and_raw(player_a: str, player_b: str, winner: str, score_raw: str) -> tuple[str, str]:
    outcome = classify_result_outcome(score_raw)
    if outcome == "walkover":
        if winner == player_a:
            return "W/O", ""
        if winner == player_b:
            return "", "W/O"
        return "", ""

    pairs = parse_score_pairs_from_score_raw(score_raw)
    if not pairs:
        return "", ""

    # Nel PDF WTA i punteggi sono espressi dal punto di vista della vincitrice.
    # Se vince Player B, invertiamo i game di ogni set per riallinearli al CSV.
    if winner == player_b:
        pairs = [(b, a) for a, b in pairs]

    a_sets, b_sets = count_sets_from_pairs(pairs)

    if outcome == "retirement":
        completed_pairs = [(a, b) for a, b in pairs if is_completed_set_score(a, b)]
        incomplete_pairs = [(a, b) for a, b in pairs if not is_completed_set_score(a, b)]
        a_sets, b_sets = count_sets_from_pairs(completed_pairs)
        if incomplete_pairs and len(completed_pairs) == 2:
            a_sets, b_sets = 1, 1
        if winner == player_a:
            return str(a_sets), f"(rit.) {b_sets}"
        if winner == player_b:
            return f"(rit.) {a_sets}", str(b_sets)

    return str(a_sets), str(b_sets)

def build_match_rows_wta(positions: list[dict], result_tokens: list[dict]) -> list[dict]:
    token_index = 0
    initial_draw_size = len(positions)
    current = [{"name": p["player_name"], "slot_type": p["slot_type"]} for p in positions]
    rows: list[dict] = []

    while len(current) > 1:
        round_size = len(current) // 2
        round_label = get_round_label(round_size, initial_draw_size)
        next_round = []

        for i in range(0, len(current), 2):
            a_name = current[i]["name"]
            b_name = current[i + 1]["name"]
            winner = ""
            score_raw = ""

            if a_name == "bye" and b_name not in {"", "bye"}:
                bye_winner = b_name
            elif b_name == "bye" and a_name not in {"", "bye"}:
                bye_winner = a_name
            elif a_name == "bye" and b_name == "bye":
                bye_winner = ""
            else:
                bye_winner = ""

            if token_index < len(result_tokens) and result_tokens[token_index]["type"] == "name":
                token_name = result_tokens[token_index]["value"]
                if abbreviated_name_matches(a_name, token_name):
                    winner = a_name
                    token_index += 1
                elif abbreviated_name_matches(b_name, token_name):
                    winner = b_name
                    token_index += 1
                elif bye_winner and abbreviated_name_matches(bye_winner, token_name):
                    winner = bye_winner
                    token_index += 1
            elif bye_winner:
                winner = bye_winner

            if winner and token_index < len(result_tokens) and result_tokens[token_index]["type"] == "score":
                score_raw = result_tokens[token_index]["value"]
                token_index += 1

            a_score, b_score = format_scores_from_winner_and_raw(a_name, b_name, winner, score_raw)
            rows.append({
                "Round": round_label,
                "Player A": a_name,
                "Player B": b_name,
                "Winner": winner,
                "Participant A score": a_score,
                "Participant B score": b_score,
            })
            next_round.append({"name": winner, "slot_type": "player" if winner else "unknown"})

        current = next_round

    return rows


def fetch_and_build_rows_wta(pdf_url: str) -> tuple[list[dict], dict]:
    session = make_requests_session()
    pdf_resp = http_get(session, pdf_url, timeout=60)
    pages_text = extract_pdf_text(pdf_resp.content)
    positions, result_tokens = parse_wta_pdf(pages_text)
    rows = build_match_rows_wta(positions, result_tokens)
    return rows, {
        "tour": "wta",
        "source_tournament": DEFAULT_WTA_TOURNAMENT_URL,
        "source_pdf": pdf_url,
        "released_at": extract_released_at(pages_text),
        "fetched_at": utc_now_iso(),
        "positions": len(positions),
        "matches": len(rows),
        "result_tokens": len(result_tokens),
    }


# =========================
# Unified routing
# =========================
def detect_tour(tour: str, tournament_url: str, draw_page_url: str, results_page_url: str, pdf_url: str) -> str:
    requested = (tour or "auto").strip().lower()
    if requested in {"atp", "wta"}:
        return requested
    haystack = " ".join(x for x in [tournament_url, draw_page_url, results_page_url, pdf_url] if x).lower()
    if "wtatennis.com" in haystack or "wtafiles.wtatennis.com" in haystack:
        return "wta"
    return "atp"


def fetch_and_build_rows_auto(tour: str, tournament_url: str, draw_page_url: str, results_page_url: str, tournament_id: str, year: int, pdf_url: str) -> tuple[list[dict], dict]:
    resolved_tour = detect_tour(tour, tournament_url, draw_page_url, results_page_url, pdf_url)
    if resolved_tour == "wta":
        return fetch_and_build_rows_wta(pdf_url or DEFAULT_WTA_PDF_URL)
    return fetch_and_build_rows_atp(
        tournament_url=tournament_url or DEFAULT_ATP_TOURNAMENT_URL,
        draw_page_url=draw_page_url or DEFAULT_ATP_DRAW_PAGE,
        results_page_url=results_page_url or DEFAULT_ATP_RESULTS_PAGE,
        tournament_id=tournament_id or DEFAULT_ATP_TOURNAMENT_ID,
        year=year,
    )


def run_once(output_path: Path, tour: str, tournament_url: str, draw_page_url: str, results_page_url: str, tournament_id: str, year: int, pdf_url: str) -> bool:
    rows, meta = fetch_and_build_rows_auto(
        tour=tour,
        tournament_url=tournament_url,
        draw_page_url=draw_page_url,
        results_page_url=results_page_url,
        tournament_id=tournament_id,
        year=year,
        pdf_url=pdf_url,
    )
    data = csv_bytes(rows)
    changed = write_csv_if_changed(output_path, data)
    status = "AGGIORNATO" if changed else "NESSUNA MODIFICA"
    extra = f"results_found={meta['results_found']}" if meta["tour"] == "atp" else f"result_tokens={meta['result_tokens']}"
    print(
        f"[{utc_now_iso()}] {status} | tour={meta['tour'].upper()} | file={output_path} | matches={meta['matches']} | "
        f"{extra} | released_at={meta['released_at'] or 'n/d'} | sha256={sha256(data)[:12]} | pdf={meta['source_pdf']}",
        flush=True,
    )
    return changed


# =========================
# Tests
# =========================

class UnifiedParserTests(unittest.TestCase):
    def test_format_name_wta_asian_names(self) -> None:
        self.assertEqual(format_name("Naomi Osaka", country="JPN"), "N. Osaka")
        self.assertEqual(format_name("Xinyu Wang", country="CHN"), "W. Xinyu")
        self.assertEqual(format_name("Qinwen Zheng", country="CHN"), "Z. Qinwen")
        self.assertEqual(format_name("WANG Xinyu", country="CHN"), "W. Xinyu")

    def test_retirement_aligned_score_keeps_one_one(self) -> None:
        res = {
            "score_raw": "6-3 3-6 0-3 RET",
            "outcome_type": "retirement",
            "player1_name_raw": "S. Rodriguez Taverna",
            "player2_name_raw": "L. Ambrogi [Alt]",
        }
        a_score, b_score = format_scores_from_result(
            "S. Rodriguez Taverna",
            "L. Ambrogi [Alt]",
            "L. Ambrogi [Alt]",
            res,
        )
        self.assertEqual(a_score, "(rit.) 1")
        self.assertEqual(b_score, "1")

    def test_wta_score_pair_parser_accepts_space_separated_sets(self) -> None:
        self.assertEqual(parse_score_pairs_from_score_raw("62 26 63"), [(6, 2), (2, 6), (6, 3)])
        self.assertEqual(parse_score_pairs_from_score_raw("67(6) 63 63"), [(6, 7), (6, 3), (6, 3)])

    def test_name_matching_handles_seed_suffix(self) -> None:
        self.assertTrue(abbreviated_name_matches("A. Sabalenka [1]", "A. Sabalenka"))
        self.assertTrue(abbreviated_name_matches("Xin. Wang [29]", "Xin. Wang"))
        self.assertFalse(abbreviated_name_matches("A. Sabalenka [1]", "A. Tomljanovic"))

    def test_wta_scores_align_to_player_order(self) -> None:
        a_score, b_score = format_scores_from_winner_and_raw(
            "J. Paolini",
            "C. Gauff",
            "C. Gauff",
            "63 64",
        )
        self.assertEqual(a_score, "0")
        self.assertEqual(b_score, "2")

    def test_mc_casing_and_country_aware_names(self) -> None:
        self.assertEqual(smart_title_token("mcnally"), "McNally")
        self.assertEqual(smart_title_token("mccartney"), "McCartney")
        self.assertEqual(format_name("Xinyu Wang", country="CHN"), "W. Xinyu")
        self.assertEqual(format_name("Jasmine Paolini", country="ITA"), "J. Paolini")

    def test_detect_tour(self) -> None:
        self.assertEqual(detect_tour("auto", "https://www.wtatennis.com/tournaments/miami-open/", "", "", ""), "wta")
        self.assertEqual(detect_tour("auto", "https://www.atptour.com/en/scores/current/miami/403", "", "", ""), "atp")

    def test_parse_real_wta_pdf_if_available(self) -> None:
        sample_pdf = Path("/mnt/data/miami_wta_mds.pdf")
        if not sample_pdf.exists():
            self.skipTest("PDF di esempio WTA non disponibile in locale")
        positions, result_tokens = parse_wta_pdf(extract_pdf_text(sample_pdf.read_bytes()))
        rows = build_match_rows_wta(positions, result_tokens)
        self.assertEqual(len(positions), 128)
        self.assertEqual(len(rows), 127)


def run_tests() -> int:

    suite = unittest.defaultTestLoader.loadTestsFromTestCase(UnifiedParserTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Genera un CSV match-by-match da draw ATP o WTA in modo unificato.")
    parser.add_argument("--tour", choices=["auto", "atp", "wta"], default="auto", help="Modalità parser: auto, atp o wta")
    parser.add_argument("--output", default="matches_format.csv", help="Percorso del file CSV da creare/aggiornare")

    # ATP-style args
    parser.add_argument("--tournament-url", default="", help="URL base del torneo (ATP o WTA, usato anche per auto-detect)")
    parser.add_argument("--draw-page", default="", help="URL della pagina draw ATP")
    parser.add_argument("--results-page", default="", help="URL della pagina risultati ATP")
    parser.add_argument("--tournament-id", default="", help="ID torneo ATP/Challenger per fallback PDF")
    parser.add_argument("--year", type=int, default=datetime.now().year, help="Anno usato per il fallback PDF ATP")

    # WTA-style args
    parser.add_argument("--pdf-url", default="", help="URL diretto del PDF main draw, utile soprattutto per WTA")

    parser.add_argument("--watch", action="store_true", help="Resta in esecuzione e aggiorna il CSV a intervalli regolari")
    parser.add_argument("--interval", type=int, default=1800, help="Intervallo in secondi in modalità --watch")
    parser.add_argument("--run-tests", action="store_true", help="Esegue i test automatici ed esce")
    args = parser.parse_args()

    if args.run_tests:
        return run_tests()

    output_path = Path(args.output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not args.watch:
        run_once(
            output_path=output_path,
            tour=args.tour,
            tournament_url=args.tournament_url,
            draw_page_url=args.draw_page,
            results_page_url=args.results_page,
            tournament_id=args.tournament_id,
            year=args.year,
            pdf_url=args.pdf_url,
        )
        return 0

    while True:
        try:
            run_once(
                output_path=output_path,
                tour=args.tour,
                tournament_url=args.tournament_url,
                draw_page_url=args.draw_page,
                results_page_url=args.results_page,
                tournament_id=args.tournament_id,
                year=args.year,
                pdf_url=args.pdf_url,
            )
        except KeyboardInterrupt:
            return 130
        except Exception as exc:
            print(f"[{utc_now_iso()}] ERRORE | {exc}", file=sys.stderr, flush=True)
        time.sleep(max(30, args.interval))


if __name__ == "__main__":
    raise SystemExit(main())

