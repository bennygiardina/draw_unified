#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import re
import sys
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pypdf import PdfReader

DEFAULT_PDF_URL = "https://wtafiles.wtatennis.com/pdf/draws/2026/902/MDS.pdf"
DEFAULT_TOURNAMENT_URL = "https://www.wtatennis.com/tournaments/miami-open/"

LOWERCASE_PARTICLES = {
    "de", "del", "della", "di", "da", "dos", "das",
    "van", "von", "der", "den", "la", "le",
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

RESULT_RETIREMENT_RE = re.compile(r"\b(?:RET|Ret|ret|retired|retirement|RIT\.?)\b", re.IGNORECASE)
RESULT_WALKOVER_RE = re.compile(r"\b(?:W/O|WO|walkover|walk-over)\b", re.IGNORECASE)

# Supporta sia "A. Sabalenka" sia "Zheng Q."
NAME_TOKEN_RE = re.compile(
    r"(?:"
    r"(?:[A-Z][a-z]{0,40}\.)[ \t]+[A-Z][A-Za-z'`.-]+(?:[ \t]+[A-Z][A-Za-z'`.-]+)*"   # A. Sabalenka
    r"|"
    r"[A-Z][A-Za-z'`.-]+(?:[ \t]+[A-Z][A-Za-z'`.-]+)*[ \t]+(?:[A-Z][a-z]{0,40}\.)"   # Zheng Q.
    r")"
)

SCORE_TOKEN_RE = re.compile(
    r"(?:W/O|WO|RET|Ret|ret|\d{2}(?:\(\d+\))?(?:[ \t]+\d{2}(?:\(\d+\))?){0,4})"
)


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

    # Fix McDonald / McNally / McCartney
    if lower.startswith("mc") and len(token) > 2:
        return "Mc" + token[2].upper() + token[3:].lower()

    if lower in LOWERCASE_PARTICLES:
        return lower

    return token[:1].upper() + token[1:].lower()


def smart_join_tokens(tokens: list[str]) -> str:
    return " ".join(smart_title_token(tok) for tok in tokens if tok)


def build_name_with_extras(base_name: str, seed: str = "", entry_status: str = "") -> str:
    extras = []
    if seed:
        extras.append(f"[{seed}]")
    if entry_status in STATUS_LABELS:
        extras.append(STATUS_LABELS[entry_status])
    return f"{base_name} {' '.join(extras)}".strip() if extras else base_name


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

        # Nei PDF WTA arrivano normalmente in ordine occidentale: Nome Cognome
        given_name = smart_join_tokens(tokens[:-1])
        surname = smart_join_tokens([tokens[-1]])

    first_initial = f"{given_name[0].upper()}." if given_name else ""

    if (country, normalized_raw) in WESTERN_NAME_EXCEPTIONS:
        base_name = f"{first_initial} {surname}".strip()
    elif country in COUNTRIES_SURNAME_FIRST_DISPLAY:
        base_name = f"{surname} {first_initial}".strip()
    else:
        base_name = f"{first_initial} {surname}".strip()

    return build_name_with_extras(base_name, seed=seed, entry_status=entry_status)


def normalize_person_name_for_matching(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r"\[[^\]]+\]", "", name)
    name = name.replace(".", " ")
    name = re.sub(r"\s+", " ", name).strip().lower()
    return name


def split_display_name_parts(name: str) -> tuple[str, str]:
    """
    Restituisce (surname, first_initial) supportando:
    - A. Sabalenka
    - Zheng Q.
    """
    norm = normalize_person_name_for_matching(name)
    parts = norm.split()
    if not parts:
        return "", ""

    if len(parts) == 1:
        return parts[0], ""

    # Caso occidentale: "a sabalenka"
    if len(parts[0]) == 1:
        return parts[-1], parts[0]

    # Caso asiatico display: "zheng q"
    if len(parts[-1]) == 1:
        return parts[0], parts[-1]

    # Fallback
    return parts[-1], parts[0][0] if parts[0] else ""


def surname_from_name(name: str) -> str:
    surname, _ = split_display_name_parts(name)
    return surname


def first_initial_from_name(name: str) -> str:
    _, initial = split_display_name_parts(name)
    return initial


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

    # Nel PDF WTA i set sono scritti dal punto di vista della vincitrice.
    # Riallineiamo a Player A / Player B.
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


def extract_released_at(pages_text: list[str]) -> str:
    for text in pages_text:
        m = re.search(r"RELEASED\s*([0-9]{1,2}\s+[A-Za-z]{3}\s+[0-9]{4}\s+[0-9:]{4,8}\s+[AP]M)", text)
        if m:
            return m.group(1)
    return ""


def extract_half_section(page_text: str, start_pos: int) -> tuple[str, str]:
    marker = "SINGLES MAIN DRAW TOP HALF" if start_pos == 1 else "SINGLES MAIN DRAW BOTTOM HALF"
    start = page_text.find(marker)
    if start == -1:
        raise RuntimeError(f"Sezione '{marker}' non trovata nel PDF")
    section = page_text[start + len(marker):]
    round_marker = section.find("Round of 128")
    if round_marker == -1:
        raise RuntimeError("Blocco Round of 128 non trovato nel PDF WTA")
    return section[:round_marker], section[round_marker:]


def parse_entry_body(raw: str) -> dict:
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


def parse_half_positions(page_text: str, start_pos: int, end_pos: int) -> tuple[list[dict], str]:
    section, _ = extract_half_section(page_text, start_pos)
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
        parsed = parse_entry_body(segment)
        parsed["draw_position"] = pos
        positions.append(parsed)
        cursor = end_idx

    result_block = section[first_result_index:].strip() if first_result_index is not None else ""
    return positions, result_block


def clean_result_block(result_block: str) -> str:
    lines = result_block.split("\n")
    filtered_lines: list[str] = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if any(marker.lower() in line.lower() for marker in STOP_MARKERS):
            continue
        filtered_lines.append(line)
    return "\n".join(filtered_lines)


def tokenize_result_block(result_block: str) -> list[dict]:
    result_block = clean_result_block(result_block)

    tokens: list[dict] = []

    # Tokenizza riga per riga per non fondere nomi consecutivi
    for line in result_block.splitlines():
        line = line.strip()
        if not line:
            continue

        i = 0
        while i < len(line):
            m_name = NAME_TOKEN_RE.match(line, i)
            if m_name:
                tokens.append({"type": "name", "value": " ".join(m_name.group(0).split())})
                i = m_name.end()
                continue

            m_score = SCORE_TOKEN_RE.match(line, i)
            if m_score:
                tokens.append({"type": "score", "value": " ".join(m_score.group(0).split())})
                i = m_score.end()
                continue

            i += 1

    return tokens


def parse_wta_pdf(pages_text: list[str]) -> tuple[list[dict], list[dict]]:
    if len(pages_text) < 2:
        raise RuntimeError("Il PDF WTA atteso deve avere almeno 2 pagine")

    top_positions, top_results_block = parse_half_positions(pages_text[0], 1, 64)
    bottom_positions, bottom_results_block = parse_half_positions(pages_text[1], 65, 128)

    positions = top_positions + bottom_positions
    result_tokens = tokenize_result_block(top_results_block) + tokenize_result_block(bottom_results_block)

    if len(positions) != 128:
        raise RuntimeError(f"Attese 128 posizioni, trovate {len(positions)}")
    return positions, result_tokens


def build_match_rows_from_result_tokens(positions: list[dict], result_tokens: list[dict]) -> list[dict]:
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


def fetch_and_build_rows(pdf_url: str) -> tuple[list[dict], dict]:
    session = make_requests_session()
    pdf_resp = http_get(session, pdf_url, timeout=60)
    pages_text = extract_pdf_text(pdf_resp.content)
    positions, result_tokens = parse_wta_pdf(pages_text)
    rows = build_match_rows_from_result_tokens(positions, result_tokens)
    meta = {
        "source_pdf": pdf_url,
        "released_at": extract_released_at(pages_text),
        "fetched_at": utc_now_iso(),
        "positions": len(positions),
        "matches": len(rows),
        "result_tokens": len(result_tokens),
    }
    return rows, meta


def run_once(output_path: Path, pdf_url: str) -> bool:
    rows, meta = fetch_and_build_rows(pdf_url)
    data = csv_bytes(rows)
    changed = write_csv_if_changed(output_path, data)
    status = "AGGIORNATO" if changed else "NESSUNA MODIFICA"
    print(
        f"[{utc_now_iso()}] {status} | file={output_path} | matches={meta['matches']} | "
        f"result_tokens={meta['result_tokens']} | released_at={meta['released_at'] or 'n/d'} | "
        f"sha256={sha256(data)[:12]} | pdf={meta['source_pdf']}",
        flush=True,
    )
    return changed


class WtaPdfTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.sample_pdf = Path("/mnt/data/miami_wta_mds.pdf")
        if cls.sample_pdf.exists():
            cls.pages_text = extract_pdf_text(cls.sample_pdf.read_bytes())
        else:
            cls.pages_text = []

    def test_score_pair_parser_accepts_space_separated_sets(self) -> None:
        self.assertEqual(parse_score_pairs_from_score_raw("62 26 63"), [(6, 2), (2, 6), (6, 3)])
        self.assertEqual(parse_score_pairs_from_score_raw("67(6) 63 63"), [(6, 7), (6, 3), (6, 3)])

    def test_name_matching_handles_seed_suffix(self) -> None:
        self.assertTrue(abbreviated_name_matches("A. Sabalenka [1]", "A. Sabalenka"))
        self.assertTrue(abbreviated_name_matches("Wang X. [29]", "Wang X."))
        self.assertFalse(abbreviated_name_matches("A. Sabalenka [1]", "A. Tomljanovic"))

    def test_format_name_asian_rules_final(self) -> None:
        self.assertEqual(format_name("Qinwen Zheng", country="CHN"), "Zheng Q.")
        self.assertEqual(format_name("Xinyu Wang", country="CHN"), "Wang X.")
        self.assertEqual(format_name("Moyuka Uchijima", country="JPN"), "Uchijima M.")
        self.assertEqual(format_name("Naomi Osaka", country="JPN"), "N. Osaka")
        self.assertEqual(format_name("Cody Wong", country="HKG"), "C. Wong")

    def test_mc_names(self) -> None:
        self.assertEqual(smart_title_token("mccartney"), "McCartney")
        self.assertEqual(smart_title_token("mcnally"), "McNally")
        self.assertEqual(smart_title_token("mcdonald"), "McDonald")

    def test_scores_are_aligned_to_player_a_and_b(self) -> None:
        a_score, b_score = format_scores_from_winner_and_raw(
            "J. Paolini",
            "C. Gauff",
            "C. Gauff",
            "63 64",
        )
        self.assertEqual(a_score, "0")
        self.assertEqual(b_score, "2")

    def test_parse_real_pdf_if_available(self) -> None:
        if not self.pages_text:
            self.skipTest("PDF di esempio non disponibile in locale")

        positions, result_tokens = parse_wta_pdf(self.pages_text)
        rows = build_match_rows_from_result_tokens(positions, result_tokens)

        self.assertEqual(len(positions), 128)
        self.assertEqual(len(rows), 127)

        first_real_match = next(r for r in rows if r["Player A"] == "A. Li" and r["Player B"] == "K. Birrell [Q]")
        self.assertEqual(first_real_match["Winner"], "A. Li")
        self.assertEqual(first_real_match["Participant A score"], "2")
        self.assertEqual(first_real_match["Participant B score"], "1")

        round64_match = next(r for r in rows if r["Player A"] == "M. Andreeva [8]" and r["Player B"] == "M. Kessler")
        self.assertEqual(round64_match["Winner"], "M. Andreeva [8]")
        self.assertEqual(round64_match["Participant A score"], "2")
        self.assertEqual(round64_match["Participant B score"], "1")


def run_tests() -> int:
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(WtaPdfTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Genera un CSV match-by-match dal PDF draw ufficiale WTA.")
    parser.add_argument("--output", default="matches_format.csv", help="Percorso del file CSV da creare/aggiornare")
    parser.add_argument("--pdf-url", default=DEFAULT_PDF_URL, help="URL del PDF WTA del main draw")
    parser.add_argument("--watch", action="store_true", help="Resta in esecuzione e aggiorna il CSV a intervalli regolari")
    parser.add_argument("--interval", type=int, default=1800, help="Intervallo in secondi in modalità --watch")
    parser.add_argument("--run-tests", action="store_true", help="Esegue i test automatici ed esce")
    args = parser.parse_args()

    if args.run_tests:
        return run_tests()

    output_path = Path(args.output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not args.watch:
        run_once(output_path, args.pdf_url)
        return 0

    while True:
        try:
            run_once(output_path, args.pdf_url)
        except KeyboardInterrupt:
            return 130
        except Exception as exc:
            print(f"[{utc_now_iso()}] ERRORE | {exc}", file=sys.stderr, flush=True)
        time.sleep(max(30, args.interval))


if __name__ == "__main__":
    raise SystemExit(main())
