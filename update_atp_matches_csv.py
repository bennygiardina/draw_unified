#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import json
import re
import sys
from pathlib import Path

import requests
from bs4 import BeautifulSoup


THIS_FILE = Path(__file__).resolve()
ORIGINAL_MODULE_PATH = THIS_FILE.with_name("update_atp_matches_csv.py")

if not ORIGINAL_MODULE_PATH.exists():
    raise SystemExit(
        "Non trovo il file originale 'update_atp_matches_csv.py' nella stessa cartella. "
        "Metti questo script accanto all'originale oppure rinomina l'originale in modo da poterlo importare."
    )

spec = importlib.util.spec_from_file_location("_orig_update_atp_matches_csv", ORIGINAL_MODULE_PATH)
if spec is None or spec.loader is None:
    raise SystemExit("Impossibile caricare il modulo originale update_atp_matches_csv.py")

_orig = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = _orig
spec.loader.exec_module(_orig)


ROUND_HEADER_TO_CANONICAL = {
    "Round of 128": "R128",
    "Round of 96": "R96",
    "Round of 64": "R64",
    "Round of 48": "R48",
    "Round of 32": "R32",
    "Round of 24": "R24",
    "Round of 16": "R16",
    "Quarterfinals": "QF",
    "Quarter-Finals": "QF",
    "Semifinals": "SF",
    "Semi-Finals": "SF",
    "Final": "F",
    "Second Round": "R64",
    "Third Round": "R32",
    "Fourth Round": "R16",
}

CANONICAL_ROUND_TO_LABEL = {
    "R128": "1° turno",
    "R96": "1° turno",
    "R64": "2° turno",
    "R48": "2° turno",
    "R32": "3° turno",
    "R24": "3° turno",
    "R16": "Ottavi di finale",
    "QF": "Quarti di finale",
    "SF": "Semifinali",
    "F": "Finale",
}


def normalize_spaces(text: str | None) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def canonical_round_to_label(canonical_round: str) -> str:
    return CANONICAL_ROUND_TO_LABEL.get(canonical_round, canonical_round)



def map_atp_round_to_canonical(round_text: str) -> str | None:
    rt = normalize_spaces(round_text)
    if not rt:
        return None

    for header, canonical in ROUND_HEADER_TO_CANONICAL.items():
        if rt == header or rt.startswith(f"{header} ") or rt.startswith(f"{header} -"):
            return canonical

    m = re.match(
        r"^(Round of \d+|Quarterfinals|Quarter-Finals|Semifinals|Semi-Finals|Final|Second Round|Third Round|Fourth Round)(?:\b|\s*-)",
        rt,
        re.IGNORECASE,
    )
    if m:
        matched = normalize_spaces(m.group(1))
        for header, canonical in ROUND_HEADER_TO_CANONICAL.items():
            if matched.lower() == header.lower():
                return canonical

    lower = rt.lower().replace("-", " ")
    aliases = {
        "first round": "R128",
        "second round": "R64",
        "third round": "R32",
        "fourth round": "R16",
        "quarter finals": "QF",
        "quarterfinal": "QF",
        "quarterfinals": "QF",
        "semi finals": "SF",
        "semifinal": "SF",
        "semifinals": "SF",
        "finals": "F",
        "championship": "F",
    }
    return aliases.get(lower)



def _looks_like_player_name_line(line: str) -> bool:
    line = normalize_spaces(line)
    if not line:
        return False
    if line.startswith("Image:") or line.startswith("Ump:") or line.startswith("Winner:"):
        return False
    if line in {"H2H", "Stats", "Print", "Refresh"}:
        return False
    if map_atp_round_to_canonical(line):
        return False
    if re.match(r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun),", line):
        return False
    if re.match(r"^Day\s*\(", line):
        return False
    if re.match(r"^\d{4}\.\d{2}\.\d{2}$", line):
        return False
    if re.fullmatch(r"\d{1,2}(?:[:.]\d{2}){0,2}", line):
        return False
    if re.fullmatch(r"\d{1,2}", line):
        return False
    if re.fullmatch(r"\d{1,2}\s+\d{1,2}", line):
        return False
    return bool(re.search(r"[A-Za-zÀ-ÿ]", line))



def _clean_results_player_name(line: str) -> str:
    line = normalize_spaces(line)
    return re.sub(r"\s*\(([^)]*)\)\s*$", "", line).strip()



def _is_numeric_score_line(line: str) -> bool:
    return bool(re.fullmatch(r"\d{1,2}(?:\s+\d{1,2})*", normalize_spaces(line)))



def _leading_score_number(line: str) -> int | None:
    m = re.match(r"^(\d{1,2})", normalize_spaces(line))
    return int(m.group(1)) if m else None



def _extract_sets_won_from_block(score1_lines: list[str], score2_lines: list[str]) -> tuple[int, int]:
    a_sets = 0
    b_sets = 0
    for left, right in zip(score1_lines, score2_lines):
        a = _leading_score_number(left)
        b = _leading_score_number(right)
        if a is None or b is None:
            continue
        if a > b:
            a_sets += 1
        elif b > a:
            b_sets += 1
    return a_sets, b_sets



def _parse_results_blocks_from_lines(lines: list[str]) -> list[dict]:
    results: list[dict] = []
    i = 0

    while i < len(lines):
        line = normalize_spaces(lines[i])
        canonical_round = map_atp_round_to_canonical(line)
        if not canonical_round:
            i += 1
            continue

        j = i + 1
        block: list[str] = []
        while j < len(lines):
            nxt = normalize_spaces(lines[j])
            if map_atp_round_to_canonical(nxt) or re.match(r"^####\s+", nxt):
                break
            block.append(nxt)
            j += 1

        player_lines: list[tuple[int, str]] = []
        for idx, item in enumerate(block):
            cleaned = _clean_results_player_name(item)
            lowered = cleaned.lower()
            if not _looks_like_player_name_line(cleaned):
                continue
            if cleaned.startswith("Game Set and Match ") or " wins the match " in cleaned:
                continue
            if cleaned.startswith("Winner:"):
                continue
            if any(
                fragment in lowered
                for fragment in ["serve fault", "match point", "break point", "set point", "ace", "double fault"]
            ):
                continue
            player_lines.append((idx, cleaned))

        players: list[str] = []
        player_indexes: list[int] = []
        seen: set[str] = set()
        for idx, name in player_lines:
            if name and name not in seen:
                seen.add(name)
                players.append(name)
                player_indexes.append(idx)
            if len(players) == 2:
                break

        winner_name = ""
        score_raw = ""
        explicit_a_sets = None
        explicit_b_sets = None

        for item in block:
            m = re.search(
                r"Game Set and Match\s+([A-Za-zÀ-ÿ'`\-.\s]+?)\.\s+.*?wins the match\s+([0-9\-\(\)\sA-Za-z/.]+?)\s*\.?$",
                item,
                re.IGNORECASE,
            )
            if m:
                winner_name = normalize_spaces(m.group(1))
                score_raw = normalize_spaces(m.group(2))
                break

            m = re.search(
                r"Winner:\s+([A-Za-zÀ-ÿ'`\-.\s]+?)(?:\s+by\s+(Walkover))?$",
                item,
                re.IGNORECASE,
            )
            if m:
                winner_name = normalize_spaces(m.group(1))
                if m.group(2):
                    score_raw = "W/O"
                break

        if len(players) == 2 and (not score_raw or not winner_name):
            idx1, idx2 = player_indexes[0], player_indexes[1]
            score1_lines = [x for x in block[idx1 + 1:idx2] if _is_numeric_score_line(x)]

            tail_end = len(block)
            for stop_idx in range(idx2 + 1, len(block)):
                if (
                    block[stop_idx].startswith("Ump:")
                    or block[stop_idx].startswith("Winner:")
                    or block[stop_idx].startswith("Game Set and Match")
                ):
                    tail_end = stop_idx
                    break

            score2_lines = [x for x in block[idx2 + 1:tail_end] if _is_numeric_score_line(x)]
            if score1_lines and score2_lines:
                explicit_a_sets, explicit_b_sets = _extract_sets_won_from_block(score1_lines, score2_lines)
                if explicit_a_sets or explicit_b_sets:
                    if not winner_name:
                        if explicit_a_sets > explicit_b_sets:
                            winner_name = players[0]
                        elif explicit_b_sets > explicit_a_sets:
                            winner_name = players[1]
                    if not score_raw:
                        score_raw = f"{explicit_a_sets}-{explicit_b_sets}"

        if players or winner_name or score_raw:
            result = {
                "round": canonical_round_to_label(canonical_round),
                "winner_name_raw": winner_name,
                "score_raw": score_raw,
                "player1_name_raw": players[0] if len(players) > 0 else "",
                "player2_name_raw": players[1] if len(players) > 1 else "",
                "outcome_type": _orig.classify_result_outcome(score_raw),
                "source": "results_page_text",
            }
            if explicit_a_sets is not None and explicit_b_sets is not None:
                result["player1_sets_won"] = explicit_a_sets
                result["player2_sets_won"] = explicit_b_sets
            results.append(result)

        i = j

    return results



def fetch_results_page(results_page_url: str, session: requests.Session | None = None) -> list[dict]:
    session = session or _orig.make_requests_session()
    resp = _orig.http_get(session, results_page_url, timeout=30)

    html = resp.text
    soup = BeautifulSoup(html, "html.parser")
    results: list[dict] = []

    for candidate in _orig.extract_json_candidates_from_html(html):
        try:
            data = json.loads(candidate)
        except Exception:
            continue

        for obj in _orig._walk_json(data):
            if not isinstance(obj, dict):
                continue

            round_text = winner_name = score_raw = player1 = player2 = None

            for key in ("round", "roundName", "Round", "matchRound"):
                if isinstance(obj.get(key), str):
                    round_text = obj[key]
                    break

            for key in ("winnerName", "WinnerName", "winningPlayerName", "winner"):
                if isinstance(obj.get(key), str):
                    winner_name = obj[key]
                    break

            for key in ("score", "Score", "matchScore", "result"):
                if isinstance(obj.get(key), str):
                    score_raw = obj[key]
                    break

            for key in ("player1Name", "Player1Name", "homePlayerName"):
                if isinstance(obj.get(key), str):
                    player1 = obj[key]
                    break

            for key in ("player2Name", "Player2Name", "awayPlayerName"):
                if isinstance(obj.get(key), str):
                    player2 = obj[key]
                    break

            canonical_round = map_atp_round_to_canonical(round_text or "")
            if not canonical_round:
                continue
            if not winner_name and not score_raw and not (player1 and player2):
                continue

            results.append(
                {
                    "round": canonical_round_to_label(canonical_round),
                    "winner_name_raw": (winner_name or "").strip(),
                    "score_raw": (score_raw or "").strip(),
                    "player1_name_raw": (player1 or "").strip(),
                    "player2_name_raw": (player2 or "").strip(),
                    "outcome_type": _orig.classify_result_outcome(score_raw or ""),
                    "source": "results_page_json",
                }
            )

    text_compact = re.sub(r"\s+", " ", soup.get_text(" ", strip=True))
    pattern = re.compile(
        r"(Round of 128|Round of 96|Round of 64|Round of 48|Round of 32|Round of 24|Round of 16|Quarterfinals|Semifinals|Final)"
        r".*?Game Set and Match\s+([A-Za-zÀ-ÿ'`\-.\s]+?)\.\s+"
        r"(?:\2)\s+wins the match\s+([0-9\-\(\)\sA-Za-z/.]+?)\s*\.",
        re.IGNORECASE,
    )

    for match in pattern.finditer(text_compact):
        round_text = match.group(1).strip()
        winner_name = normalize_spaces(match.group(2))
        score_raw = normalize_spaces(match.group(3))

        canonical_round = map_atp_round_to_canonical(round_text)
        if not canonical_round:
            continue

        results.append(
            {
                "round": canonical_round_to_label(canonical_round),
                "winner_name_raw": winner_name,
                "score_raw": score_raw,
                "player1_name_raw": "",
                "player2_name_raw": "",
                "outcome_type": _orig.classify_result_outcome(score_raw),
                "source": "results_page_regex",
            }
        )

    lines = [normalize_spaces(s) for s in soup.stripped_strings]
    results.extend(_parse_results_blocks_from_lines(lines))

    return _orig.dedupe_results(results)


# Monkey patch del parser originale
_orig.fetch_results_page = fetch_results_page


def main() -> int:
    return _orig.main()


if __name__ == "__main__":
    raise SystemExit(main())

