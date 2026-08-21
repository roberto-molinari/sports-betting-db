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

Odds are devigged (overround removed) via the power method (2026-08-20, replacing
the earlier proportional method -- see devig()'s own docstring for why: proportional
devig leaves a real favorite-longshot bias in p_*_fair, measured at calibration slope
1.154 against realized outcomes, BUGS.md BUG-009). A bad/degenerate row (overround
<= 1.0 -- no real book prices this way; a data glitch, not a market to correct) falls
back to the old proportional formula for that row only, and is appended to
DEVIG_FALLBACK_LOG_PATH so these stay visible across runs instead of scrolling off
in stdout.

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
from datetime import datetime, timezone
from pathlib import Path

from core.sports_db import (
    DATABASE_PATH,
    clear_soccer_market_odds,
    add_soccer_market_odds,
)
from core.leagues import LEAGUES, FOOTBALLDATACOUK_SEASON_CODE
from core.team_name_maps import canonical_team_name

FD_URL = "https://www.football-data.co.uk/mmz4281/{code}/{league_code}.csv"

# Durable, git-tracked record of devig()'s proportional-fallback rows (see that
# function's docstring) -- these should be rare (a real book's overround is always
# >1), so a running append-only log across every import run is more useful than a
# print buried in one run's stdout, for noticing if they start happening often.
DEVIG_FALLBACK_LOG_PATH = Path(__file__).parent / "devig_overround_warnings.log"

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


def log_devig_fallback(home_odds, draw_odds, away_odds, overround, context=""):
    """Append one line to DEVIG_FALLBACK_LOG_PATH -- see that constant's comment."""
    ts = datetime.now(timezone.utc).isoformat()
    with open(DEVIG_FALLBACK_LOG_PATH, "a") as f:
        f.write(f"{ts}  overround={overround:.4f} <= 1.0, used proportional fallback  "
                f"odds=({home_odds}, {draw_odds}, {away_odds})  {context}\n")


def _proportional_devig(raw):
    total = sum(raw)
    return tuple(r / total for r in raw)


def devig(home_odds, draw_odds, away_odds, context=""):
    """Power-method devig (2026-08-20, replacing the old proportional method):
    solve for the single exponent k > 0 such that sum((1/odds_i)^k) = 1, then
    p_fair_i = (1/odds_i)^k. Unlike proportional devig (p_i = r_i / sum(r_i), which
    shrinks every outcome by the SAME percentage), raising each r_i = 1/odds_i to a
    power k > 1 shrinks a LONGSHOT's small r_i proportionally more than a favorite's
    large r_i -- which is the correction actually needed: proportional devig was
    measured leaving a real favorite-longshot bias in p_*_fair (calibration slope
    1.154 against realized outcomes, i.e. longshots win even less often than its
    fair-p implies -- BUGS.md BUG-009, 2026-08-20).

    A real book's overround (sum of raw implied probabilities r_h+r_d+r_a) is always
    > 1 -- that IS the vig. sum(r_i^k) is strictly decreasing in k (each r_i is in
    (0, 1)), running from sum(r_i^0)=3 down to 0 as k -> infinity, so there is
    exactly one root and it's found by ordinary bisection starting from k=1 (where
    sum(r_i^1) = the overround, > 1) and doubling the upper bound until the sum
    drops below 1.

    If overround <= 1.0 (a data glitch -- no real sportsbook prices a 3-way market
    at or under fair value), a root would still exist mathematically but at k <= 1,
    which would INFLATE every probability's spread instead of correcting it -- the
    opposite of what devig is for. That row falls back to the old proportional
    formula instead, and is logged to DEVIG_FALLBACK_LOG_PATH (see log_devig_fallback)
    so these are visible across runs, not just this one's stdout."""
    raw = (1 / home_odds, 1 / draw_odds, 1 / away_odds)
    overround = sum(raw)
    if overround <= 1.0:
        log_devig_fallback(home_odds, draw_odds, away_odds, overround, context)
        return _proportional_devig(raw)

    def f(k):
        return sum(r ** k for r in raw)

    lo, hi = 1.0, 2.0
    while f(hi) > 1.0:
        hi *= 2
        if hi > 1e6:   # pathological input guard -- should be unreachable given the overround check above
            return _proportional_devig(raw)

    for _ in range(100):   # ample for float64 precision via bisection
        mid = (lo + hi) / 2
        if f(mid) > 1.0:
            lo = mid
        else:
            hi = mid
        if hi - lo < 1e-12:
            break

    k = (lo + hi) / 2
    return tuple(r ** k for r in raw)


def get_team_id(cur, league, name):
    """league is unused for the lookup itself (kept for call-site symmetry): a team's
    soccer_teams.league is its CURRENT division (FEATURE-019), so scoping by it would
    fail to resolve a team that's since been promoted/relegated out of `league` for a
    historical-season import -- name is globally unique (ensure_soccer_team), so an
    unscoped lookup is the correct one. Same bug/fix as import_league_betting_odds.py's
    load_team_map, found live 2026-08-20."""
    cur.execute("SELECT team_id FROM soccer_teams WHERE name = ?", (name,))
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
        p_h, p_d, p_a = devig(h_odds, d_odds, a_odds,
                              context=f"league={league} season={season} source={source_label} "
                                      f"line_type={line_type} match_id={match_id}")
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
