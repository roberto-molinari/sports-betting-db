"""
Export every GRADED club-league pick to a flat JSON array, for a static
front-end (no server, no build step) to slice by league/market/time-window
and compute ROI entirely client-side (2026-08-26 -- interactive ROI report
page, built in a separate repo that hosts the site).

Scope decisions (locked in with the user before building this):
  - GRADED PICKS ONLY (soccer_club_league_picks.result IS NOT NULL) -- this
    is a historical-performance report, not a live picks preview. A pending
    pick simply doesn't appear until its match is graded.
  - FULL regenerate every run, no incremental/append mode -- at a few
    hundred KB even for multiple seasons (measured: ~185 bytes/pick raw,
    ~26 bytes/pick gzipped), re-querying everything each time is negligible
    cost and has zero drift risk. Never edit the output file by hand.
  - date is the ET+buffer MATCHDAY (core.matchday.matchday_for_match), not
    the raw UTC kickoff -- consistent with every other tool in this repo
    (generate_club_league_card.py, club_league_scorecard.py, matchday_
    summary.py all group by this same boundary).
  - profit is PRECOMPUTED here (via club_league_scorecard._pick_profit, the
    same function the scorecard tool itself uses) rather than shipping raw
    odds for the client to convert -- one tested source of truth for the
    american-odds math, not reimplemented in JS.

Cadence: run this once daily, after club_league_scorecard.py has graded that
day's picks -- not wired into the scorecard tool itself (separate concern,
matching this repo's "one tool, one job" convention). Then copy the output
file into the site repo and commit/deploy there (manual for now; no
cross-repo automation exists yet).

Usage:
    python export_club_league_picks_json.py
    python export_club_league_picks_json.py --output web_export/club_league_picks.json
"""
import argparse
import json
import sqlite3
from pathlib import Path

from core.sports_db import DATABASE_PATH
from core.matchday import matchday_for_match
from club_league_scorecard import _pick_profit

DEFAULT_OUTPUT = Path(__file__).parent / "web_export" / "club_league_picks.json"


def market_for_side(side):
    return "totals" if side.startswith(("OVER", "UNDER")) else "1x2"


def graded_picks(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT p.league, m.match_date, ht.name, at.name, p.side, p.odds,
               p.result, p.method
        FROM soccer_club_league_picks p
        JOIN soccer_matches m ON m.match_id = p.match_id
        JOIN soccer_teams ht ON ht.team_id = m.home_team_id
        JOIN soccer_teams at ON at.team_id = m.away_team_id
        WHERE p.result IS NOT NULL
        ORDER BY m.match_date
    """)
    return cur.fetchall()


def build_export(rows):
    """rows -> [{"date","league","home","away","market","side","odds",
    "result","profit","method"}, ...], one dict per graded pick."""
    export = []
    for league, match_date, home, away, side, odds, result, method in rows:
        export.append({
            "date": str(matchday_for_match(match_date)),
            "league": league,
            "home": home,
            "away": away,
            "market": market_for_side(side),
            "side": side,
            "odds": odds,
            "result": result,
            "profit": round(_pick_profit(odds, result), 4),
            "method": method,
        })
    return export


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT,
                        help=f"Output JSON file path (default: {DEFAULT_OUTPUT}).")
    return parser.parse_args()


def main():
    args = parse_args()
    conn = sqlite3.connect(DATABASE_PATH)
    rows = graded_picks(conn)
    conn.close()

    export = build_export(rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(export, separators=(",", ":"))
    args.output.write_text(text)

    leagues = sorted({p["league"] for p in export})
    dates = sorted({p["date"] for p in export})
    print(f"Wrote {len(export)} graded pick(s)")
    print(f"  file: {args.output}")
    print(f"  leagues: {', '.join(leagues) if leagues else '(none)'}")
    print(f"  date range: {dates[0]} to {dates[-1]}" if dates else "  date range: (no data)")
    print(f"  file size: {len(text.encode())} bytes")


if __name__ == "__main__":
    main()
