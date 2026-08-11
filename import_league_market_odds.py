"""
Import a league's opening- and closing-line 1X2 odds from football-data.co.uk (free)
into soccer_market_odds -- sharp books (Pinnacle, Betfair Exchange) for CLV, plus
Bet365 as a soft/retail-book baseline for comparison. Generalized 2026-08-10 (multi-
league expansion, renamed from import_serie_a_market_odds.py) -- every per-league
detail (football-data.co.uk CSV code, team-name map) now comes from --league via
core/leagues.py's LEAGUES registry and core/team_name_maps.py, instead of being
hardcoded to Serie A.

Coverage per season/book varies by source-side data completeness and is NOT assumed
to match Serie A's -- see Serie A's own documented gaps below as an example of what
to check per new league (this file only tracks Serie A's; a new league's equivalent
gaps should be checked and documented the same way before trusting its numbers):
  2023-24 (Serie A): Pinnacle and Bet365 complete (380/380); no Betfair Exchange
           column that season.
  2024-25 (Serie A): Pinnacle, Betfair Exchange, and Bet365 all complete (380/380).
  2025-26 (Serie A): Pinnacle has a large gap from 2026-01-15 onward (source-side
           data lag, not a real unavailability) -- 198/380 closing, 200/380 opening;
           Betfair Exchange is complete through March 2026, mostly complete after
           (359/380 opening, 360/380 closing); Bet365 is complete (380/380).

Odds are devigged (overround removed) via the standard multiplicative method:
p_fair = (1/odds) / sum((1/odds) across home+draw+away).

Conflict-safe writes: soccer_market_odds has no natural upsert (add_soccer_market_odds
is a plain insert; clear_soccer_market_odds + reinsert is the existing "upsert"
pattern for this table). Before clearing, this script fetches what's currently stored
for the same (league, season, source, line_type) and diffs it against the freshly-
downloaded CSV data, logging a one-line summary (rows unchanged vs. rows that would
change) -- a real correction from the source should be a visible, reviewed event, not
silently absorbed into a routine re-run.

Usage:
    python import_league_market_odds.py --league "Serie A" --season 2025
    python import_league_market_odds.py --league "Premier League" --season 2024
"""

import argparse
import csv
import io
import sqlite3
import urllib.request

from core.sports_db import (
    DATABASE_PATH,
    clear_soccer_market_odds,
    add_soccer_market_odds,
)
from core.leagues import LEAGUES, FOOTBALLDATACOUK_SEASON_CODE
from core.team_name_maps import canonical_team_name

FD_URL = "https://www.football-data.co.uk/mmz4281/{code}/{league_code}.csv"

# (source label, opening column names, closing column names)
SOURCES = [
    ("Pinnacle", ("PSH", "PSD", "PSA"), ("PSCH", "PSCD", "PSCA")),
    ("Betfair Exchange", ("BFEH", "BFED", "BFEA"), ("BFECH", "BFECD", "BFECA")),
    ("Bet365", ("B365H", "B365D", "B365A"), ("B365CH", "B365CD", "B365CA")),
]


def fetch_csv_rows(league, season_code):
    league_code = LEAGUES[league]["footballdatacouk_code"]
    if league_code is None:
        raise SystemExit(f"'{league}' has no footballdatacouk_code configured in core/leagues.py.")
    url = FD_URL.format(code=season_code, league_code=league_code)
    with urllib.request.urlopen(url) as resp:
        text = resp.read().decode("utf-8-sig")
    return list(csv.DictReader(io.StringIO(text)))


def devig(home_odds, draw_odds, away_odds):
    """Multiplicative devig: p_fair = (1/odds) / sum(1/odds)."""
    raw_h, raw_d, raw_a = 1 / home_odds, 1 / draw_odds, 1 / away_odds
    total = raw_h + raw_d + raw_a
    return raw_h / total, raw_d / total, raw_a / total


def get_team_id(cur, league, name):
    cur.execute("SELECT team_id FROM soccer_teams WHERE league = ? AND name = ?", (league, name))
    row = cur.fetchone()
    return row[0] if row else None


def get_match_id(cur, league, season, home_team_id, away_team_id):
    cur.execute(
        """SELECT match_id FROM soccer_matches
           WHERE league = ? AND season = ? AND home_team_id = ? AND away_team_id = ?""",
        (league, season, home_team_id, away_team_id)
    )
    row = cur.fetchone()
    return row[0] if row else None


def load_existing_odds(cur, league, season, source_label, line_type):
    """{match_id: (home_odds, draw_odds, away_odds)} currently stored -- the "before"
    side of the conflict-safe diff (see module docstring)."""
    cur.execute(
        """SELECT match_id, home_odds, draw_odds, away_odds FROM soccer_market_odds
           WHERE league = ? AND source = ? AND line_type = ?
             AND match_id IN (SELECT match_id FROM soccer_matches WHERE league = ? AND season = ?)""",
        (league, source_label, line_type, league, season),
    )
    return {row[0]: (row[1], row[2], row[3]) for row in cur.fetchall()}


def import_line_type(conn, cur, league, rows, season, source_label, line_type, hcol, dcol, acol):
    existing = load_existing_odds(cur, league, season, source_label, line_type)

    inserted = 0
    unchanged = 0
    changed = 0
    skipped_no_odds = 0
    skipped_no_match = 0
    skipped_unmapped_team = 0
    new_by_match = {}

    for r in rows:
        home_name = canonical_team_name(league, r["HomeTeam"])
        away_name = canonical_team_name(league, r["AwayTeam"])

        h_odds, d_odds, a_odds = r.get(hcol), r.get(dcol), r.get(acol)
        if not h_odds or not d_odds or not a_odds:
            skipped_no_odds += 1
            continue

        home_id = get_team_id(cur, league, home_name)
        away_id = get_team_id(cur, league, away_name)
        if home_id is None or away_id is None:
            skipped_unmapped_team += 1
            continue

        match_id = get_match_id(cur, league, season, home_id, away_id)
        if match_id is None:
            skipped_no_match += 1
            continue

        h_odds, d_odds, a_odds = float(h_odds), float(d_odds), float(a_odds)
        new_by_match[match_id] = (h_odds, d_odds, a_odds)

        if existing.get(match_id) == (h_odds, d_odds, a_odds):
            unchanged += 1
        else:
            if match_id in existing:
                print(f"  [{source_label}/{line_type}] match_id={match_id}: "
                      f"{existing[match_id]} -> {(h_odds, d_odds, a_odds)}")
            changed += 1

    # Delete-then-reinsert is the only upsert this table supports (see module
    # docstring) -- the diff above is purely for visibility, done before the write.
    clear_soccer_market_odds(league, season, source_label, line_type, conn=conn)
    for match_id, (h_odds, d_odds, a_odds) in new_by_match.items():
        p_h, p_d, p_a = devig(h_odds, d_odds, a_odds)
        add_soccer_market_odds(
            match_id=match_id, league=league, source=source_label, line_type=line_type,
            home_odds=h_odds, draw_odds=d_odds, away_odds=a_odds,
            p_home_fair=p_h, p_draw_fair=p_d, p_away_fair=p_a,
            conn=conn,
        )
        inserted += 1

    print(f"[{source_label} / {line_type}] {league} season={season}: inserted={inserted} "
          f"(unchanged={unchanged} changed={changed}) "
          f"skipped_no_odds={skipped_no_odds} skipped_no_match={skipped_no_match} "
          f"skipped_unmapped_team={skipped_unmapped_team}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--league", default="Serie A", choices=sorted(LEAGUES),
                        help="League name, must be a key in core/leagues.py's LEAGUES registry.")
    parser.add_argument("--season", type=int, required=True,
                        help="Our DB's season label, e.g. 2025 for 2025-26")
    args = parser.parse_args()

    season_code = FOOTBALLDATACOUK_SEASON_CODE.get(args.season)
    if season_code is None:
        raise SystemExit(f"No football-data.co.uk season code mapped for season={args.season}")

    rows = fetch_csv_rows(args.league, season_code)

    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()

    for source_label, opening_cols, closing_cols in SOURCES:
        import_line_type(conn, cur, args.league, rows, args.season, source_label, "opening", *opening_cols)
        import_line_type(conn, cur, args.league, rows, args.season, source_label, "closing", *closing_cols)

    conn.close()


if __name__ == "__main__":
    main()
