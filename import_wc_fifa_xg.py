"""
FEATURE-008 (extension, 2026-07-07): pull OFFICIAL FIFA per-match xG from the
FIFA Training Centre's match report hub -- a second, more authoritative
external xG source alongside TheStatsAPI, for POST-HOC COMPARISON ONLY.

HARD CONSTRAINT (unchanged from import_wc_match_xg.py): the model's own xG is
the source of truth for anything the model does. This never writes to
soccer_wc_matches or any core-workflow table -- only to soccer_wc_external_xg,
tagged source='fifa_official' (distinct from 'thestatsapi'), so the two
external sources sit side by side without being confused with each other or
with the model's own numbers.

Source: FIFA publishes one "Post Match Summary Report" (PMSR) PDF per finished
match on two hub pages (group stage / knockout stage), e.g.:
  https://www.fifatrainingcentre.com/en/fifa-world-cup-2026/match-report-hub.php
  https://www.fifatrainingcentre.com/en/fifa-world-cup-2026/match-report-hub-knockout-stage.php
There is no API, so this scrapes the hub pages for the actual PDF hrefs --
filenames are NOT a reliable naming pattern (separators vary between space and
hyphen, and revised reports carry a "-V2"/"POST-V2" suffix; confirmed live
2026-07-07), so team/date/score identity is taken entirely from each PDF's own
text, never guessed from its filename.

Report layout (confirmed against 2 real reports, one decided in regulation,
one on penalties): page 1 always has "<Home> <home_score> - <away_score>
<Away>" and the kickoff date in plain text (FULL team names, not the 3-letter
code used only in the filename); the goals shown are ALWAYS the 90' regulation
score even for a penalty-shootout match. A later page (page 3 in both samples,
but not assumed fixed -- the first 8 pages are searched) has a "Match Summary
- Key Statistics" table with a "<home_xg> xG (Expected Goals) <away_xg>" line.

Requires ``pdfplumber`` (not otherwise a project dependency): pip install pdfplumber

Usage:
    python import_wc_fifa_xg.py                  # fetch hub pages, pull all new reports
    python import_wc_fifa_xg.py --dry-run
    python import_wc_fifa_xg.py --limit 5         # first N reports only (smoke test)
"""

import argparse
import io
import re
import sqlite3
import time
from datetime import datetime, timezone

import pdfplumber
import requests

from core.sports_db import DATABASE_PATH, upsert_wc_external_xg

HUB_URLS = [
    "https://www.fifatrainingcentre.com/en/fifa-world-cup-2026/match-report-hub.php",
    "https://www.fifatrainingcentre.com/en/fifa-world-cup-2026/match-report-hub-knockout-stage.php",
]
BASE_URL = "https://www.fifatrainingcentre.com"
SOURCE = "fifa_official"
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
}
# Politeness delay between PDF downloads -- a public site, not an API.
REQUEST_DELAY_SECONDS = 0.5

# FIFA report full-team-name spellings -> canonical soccer_wc_teams.name, for
# the handful that differ. If a run logs "NO MATCH" for a spelling not listed
# here, add it.
FIFA_TEAM_ALIASES = {
    "korea republic": "South Korea",
    "ir iran": "Iran",
    "czech republic": "Czechia",
    "ivory coast": "Côte d'Ivoire",
    "bosnia and herzegovina": "Bosnia & Herzegovina",
    "congo dr": "DR Congo",
    "dr congo": "DR Congo",
    "turkiye": "Türkiye",
    "turkey": "Türkiye",
    "united states": "USA",
    "cabo verde": "Cape Verde",
}

SCORE_LINE_RE = re.compile(r'^(.+?)\s+(\d+)\s*-\s*(\d+)\s+(.+)$')
DATE_RE = re.compile(r'(\d{1,2} [A-Za-z]+ \d{4})')
XG_RE = re.compile(r'([\d.]+)\s+xG \(Expected Goals\)\s+([\d.]+)')


def parse_args():
    ap = argparse.ArgumentParser(
        description="Import OFFICIAL FIFA per-match xG from the match report hub (PDFs).")
    ap.add_argument("--limit", type=int, help="Only process the first N reports (smoke test).")
    ap.add_argument("--dry-run", action="store_true",
                    help="Parse and report without writing to the database.")
    return ap.parse_args()


def normalize_team(name):
    if not name:
        return name
    key = name.strip().lower()
    return FIFA_TEAM_ALIASES.get(key, name.strip())


def fetch_hub_pdf_urls():
    """All PMSR PDF URLs linked from both hub pages (group + knockout)."""
    urls = []
    for hub in HUB_URLS:
        resp = requests.get(hub, headers=REQUEST_HEADERS, timeout=30)
        resp.raise_for_status()
        hrefs = sorted(set(re.findall(r'href="([^"]+\.pdf)"', resp.text)))
        urls.extend(BASE_URL + h if h.startswith("/") else h for h in hrefs)
    return urls


def parse_page1(text):
    """Pure parse of page 1's raw text -> dict(home, away, home_score,
    away_score, date), or None if the layout doesn't match (e.g. not actually
    a PMSR report)."""
    lines = (text or "").splitlines()
    if not lines:
        return None
    score_m = SCORE_LINE_RE.match(lines[0].strip())
    date_m = DATE_RE.search(text)
    if not score_m or not date_m:
        return None
    home, home_score, away_score, away = score_m.groups()
    try:
        report_date = datetime.strptime(date_m.group(1), "%d %B %Y").date()
    except ValueError:
        return None
    return {
        "home": normalize_team(home), "away": normalize_team(away),
        "home_score": int(home_score), "away_score": int(away_score),
        "date": report_date,
    }


def parse_xg(text):
    """(home_xg, away_xg) from a page's raw text containing the "Match
    Summary - Key Statistics" xG line, or None if this page doesn't have it."""
    m = XG_RE.search(text or "")
    return (float(m.group(1)), float(m.group(2))) if m else None


def parse_report(pdf_bytes):
    """Return dict(home, away, home_score, away_score, date, home_xg, away_xg),
    or None if the report doesn't have the expected page-1 / xG layout."""
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        report = parse_page1(pdf.pages[0].extract_text() or "")
        if report is None:
            return None
        for page in pdf.pages[:8]:
            xg = parse_xg(page.extract_text() or "")
            if xg:
                report["home_xg"], report["away_xg"] = xg
                return report
    return None


def our_matches(conn):
    """All finished matches, for team-pair matching."""
    return conn.execute("""
        SELECT m.match_id, m.match_date, m.home_score, m.away_score,
               m.extra_time_home_score, m.extra_time_away_score,
               h.name AS home, a.name AS away
        FROM soccer_wc_matches m
        JOIN soccer_wc_teams h ON h.team_id = m.home_team_id
        JOIN soccer_wc_teams a ON a.team_id = m.away_team_id
        WHERE m.home_score IS NOT NULL
    """).fetchall()


def final_score(match):
    """The score FIFA's page 1 actually shows: extra-time-inclusive goals for
    a tie that went to ET (penalties aren't goals, so a shootout-decided tie
    still shows its regulation score -- confirmed against a real report)."""
    if match["extra_time_home_score"] is not None:
        return match["extra_time_home_score"], match["extra_time_away_score"]
    return match["home_score"], match["away_score"]


def already_fetched(conn, match_id):
    return conn.execute(
        "SELECT 1 FROM soccer_wc_external_xg WHERE match_id = ? AND source = ?",
        (match_id, SOURCE)
    ).fetchone() is not None


def find_match(matches, report):
    """The one finished match whose team pair matches this report -- an
    UNORDERED-pair match, since FIFA's home/away in the report doesn't
    necessarily line up with our own nominal home/away for a neutral-venue
    game."""
    pair = {report["home"].lower(), report["away"].lower()}
    candidates = [m for m in matches if {m["home"].lower(), m["away"].lower()} == pair]
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]

    def date_key(m):
        try:
            md = datetime.fromisoformat(str(m["match_date"])[:10]).date()
            return abs((md - report["date"]).days)
        except ValueError:
            return 10_000
    return min(candidates, key=date_key)


def scores_agree(match, report):
    """Sanity check (orientation-aware): a mismatch means this report was
    paired to the wrong match, so it should NOT be stored. Compares against
    final_score (extra-time-inclusive when applicable), not the bare
    regulation home_score/away_score -- see final_score's docstring."""
    home_final, away_final = final_score(match)
    if match["home"].lower() == report["home"].lower():
        return (home_final, away_final) == (report["home_score"], report["away_score"])
    return (home_final, away_final) == (report["away_score"], report["home_score"])


def oriented_xg(match, report):
    """(home_xg, away_xg) in OUR match's home/away orientation."""
    if match["home"].lower() == report["home"].lower():
        return report["home_xg"], report["away_xg"]
    return report["away_xg"], report["home_xg"]


def main():
    args = parse_args()
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row

    matches = our_matches(conn)
    pdf_urls = fetch_hub_pdf_urls()
    if args.limit:
        pdf_urls = pdf_urls[:args.limit]
    print(f"{len(pdf_urls)} PMSR reports found on the hub pages.")

    matched = unmatched = mismatched = already_had = parse_failed = 0
    for url in pdf_urls:
        try:
            resp = requests.get(url, headers=REQUEST_HEADERS, timeout=60)
            resp.raise_for_status()
        except requests.RequestException as exc:
            print(f"  FETCH FAILED: {url} ({exc})")
            continue

        report = parse_report(resp.content)
        time.sleep(REQUEST_DELAY_SECONDS)
        if report is None:
            parse_failed += 1
            print(f"  PARSE FAILED: {url}")
            continue

        match = find_match(matches, report)
        if match is None:
            unmatched += 1
            print(f"  NO MATCH: {report['home']} v {report['away']} ({report['date']})")
            continue
        if already_fetched(conn, match["match_id"]):
            already_had += 1
            continue
        if not scores_agree(match, report):
            mismatched += 1
            print(f"  SCORE MISMATCH, skipped: {report['home']} v {report['away']} report "
                  f"{report['home_score']}-{report['away_score']} vs stored "
                  f"{match['home']} {match['home_score']}-{match['away_score']} {match['away']}")
            continue

        home_xg, away_xg = oriented_xg(match, report)
        matched += 1
        print(f"  {match['home']} v {match['away']}: xG {home_xg:.2f} - {away_xg:.2f}")
        if not args.dry_run:
            upsert_wc_external_xg(
                match_id=match["match_id"], source=SOURCE,
                home_xg=home_xg, away_xg=away_xg,
                fetched_at=datetime.now(timezone.utc).isoformat())

    conn.close()
    print(f"\n=== FIFA OFFICIAL xG IMPORT ({'dry-run' if args.dry_run else 'stored'}) ===")
    print(f"Matched {matched}  already-had {already_had}  unmatched {unmatched}  "
          f"score-mismatch {mismatched}  parse-failed {parse_failed}")


if __name__ == "__main__":
    main()
