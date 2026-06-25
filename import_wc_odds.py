"""
Import World Cup 2026 betting odds from a simple CSV.

The CSV is produced by transcribing a sportsbook screenshot (via the AI
assistant) into one row per match.  Required header columns:

    date, home, away, home_ml, draw_ml, away_ml, total, over_odds, under_odds

Optional columns (knockout ties only):

    home_adv, away_adv

- date           : match date (YYYY-MM-DD, optionally with time) — used to
                   disambiguate when a fixture is ambiguous; may be blank.
- home, away     : national team names (matched to soccer_wc_teams.name).
- *_ml           : 1X2 American moneylines.
- total          : the over/under goals line the book posted (e.g. 2.5, 3.5).
- over/under_odds : American odds for that total.
- home/away_adv  : 2-way "to advance" American odds (knockout rounds; blank for
                   group games or books that don't post the market).

Odds are American by default.  Pass --decimal if the CSV holds decimal odds.

Rows are written to soccer_wc_odds via an upsert keyed on (match_id,
sportsbook), so re-running updates existing rows rather than duplicating.

Usage:
    python import_wc_odds.py path/to/odds.csv --sportsbook DraftKings
    python import_wc_odds.py path/to/odds.csv --decimal
"""

import argparse
import csv
import sqlite3
import sys
from datetime import datetime, timezone

from core.sports_db import DATABASE_PATH, upsert_wc_odds
from import_serie_a_odds import decimal_to_american


# Map common alternate spellings to the canonical soccer_wc_teams.name.
# Extend as needed once squads are imported and exact names are known.
WC_TEAM_ALIASES = {
    "usa": "United States",
    "united states of america": "United States",
    "south korea": "Korea Republic",
    "iran": "IR Iran",
    "ivory coast": "Côte d'Ivoire",
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Import World Cup 2026 odds from a transcribed sportsbook CSV.")
    parser.add_argument("files", nargs="+", help="CSV file(s) to import.")
    parser.add_argument("--sportsbook", default="DraftKings",
                        help="Sportsbook name stored with each odds row.")
    parser.add_argument("--decimal", action="store_true",
                        help="Treat odds columns as decimal odds and convert to American.")
    parser.add_argument("--odds-date",
                        help="Timestamp recorded for these odds (ISO). Defaults to now (UTC).")
    return parser.parse_args()


def normalize(name):
    return (name or "").strip().lower()


def load_team_map(conn):
    """Return a dict of normalized team name -> team_id from soccer_wc_teams."""
    cur = conn.cursor()
    cur.execute("SELECT team_id, name FROM soccer_wc_teams")
    return {normalize(name): team_id for team_id, name in cur.fetchall()}


def resolve_team(name, team_map):
    """Resolve a CSV team name to a team_id, applying aliases. None if unknown."""
    key = normalize(name)
    if key in team_map:
        return team_map[key]
    alias = WC_TEAM_ALIASES.get(key)
    if alias and normalize(alias) in team_map:
        return team_map[normalize(alias)]
    return None


def load_match_index(conn):
    """Return (home_id, away_id) -> list of (match_id, match_date)."""
    cur = conn.cursor()
    cur.execute("SELECT match_id, home_team_id, away_team_id, match_date FROM soccer_wc_matches")
    index = {}
    for match_id, home_id, away_id, match_date in cur.fetchall():
        index.setdefault((home_id, away_id), []).append((match_id, match_date))
    return index


def find_match(index, home_id, away_id, target_date):
    """Find the match_id for a fixture, using the date to break ties."""
    candidates = index.get((home_id, away_id))
    if not candidates:
        return None
    if len(candidates) == 1 or not target_date:
        return candidates[0][0]
    # Multiple fixtures with the same pairing: pick the closest date.
    def date_key(item):
        try:
            md = datetime.fromisoformat(str(item[1])[:10])
            td = datetime.fromisoformat(str(target_date)[:10])
            return abs((md - td).days)
        except ValueError:
            return 10_000
    return min(candidates, key=date_key)[0]


def parse_odds(value, as_decimal):
    text = (value or "").strip()
    if not text:
        return None
    try:
        num = float(text)
    except ValueError:
        return None
    return decimal_to_american(num) if as_decimal else num


def main():
    args = parse_args()
    odds_date = args.odds_date or datetime.now(timezone.utc).isoformat()

    conn = sqlite3.connect(DATABASE_PATH)
    team_map = load_team_map(conn)
    match_index = load_match_index(conn)
    conn.close()

    if not team_map:
        sys.exit("No teams in soccer_wc_teams — import squads/teams first.")

    inserted = updated = 0
    unmatched_team = []
    unmatched_match = []

    for path in args.files:
        with open(path, newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                home_id = resolve_team(row.get("home"), team_map)
                away_id = resolve_team(row.get("away"), team_map)
                if home_id is None or away_id is None:
                    unmatched_team.append((row.get("home"), row.get("away")))
                    continue

                match_id = find_match(match_index, home_id, away_id, row.get("date"))
                if match_id is None:
                    unmatched_match.append((row.get("home"), row.get("away"), row.get("date")))
                    continue

                # Decide insert vs update for reporting (upsert is idempotent).
                conn = sqlite3.connect(DATABASE_PATH)
                exists = conn.execute(
                    "SELECT 1 FROM soccer_wc_odds WHERE match_id = ? AND sportsbook = ?",
                    (match_id, args.sportsbook)
                ).fetchone() is not None
                conn.close()

                upsert_wc_odds(
                    match_id=match_id,
                    sportsbook=args.sportsbook,
                    odds_date=odds_date,
                    home_moneyline=parse_odds(row.get("home_ml"), args.decimal),
                    draw_moneyline=parse_odds(row.get("draw_ml"), args.decimal),
                    away_moneyline=parse_odds(row.get("away_ml"), args.decimal),
                    over_under=parse_odds(row.get("total"), as_decimal=False),
                    over_odds=parse_odds(row.get("over_odds"), args.decimal),
                    under_odds=parse_odds(row.get("under_odds"), args.decimal),
                    home_advance_ml=parse_odds(row.get("home_adv"), args.decimal),
                    away_advance_ml=parse_odds(row.get("away_adv"), args.decimal),
                )
                if exists:
                    updated += 1
                else:
                    inserted += 1

    print(f"Sportsbook: {args.sportsbook}")
    print(f"Inserted: {inserted}  Updated: {updated}")
    if unmatched_team:
        print(f"Unmatched team names ({len(unmatched_team)}):")
        for home, away in unmatched_team:
            print(f"  {home!r} vs {away!r}")
    if unmatched_match:
        print(f"No fixture found ({len(unmatched_match)}):")
        for home, away, date in unmatched_match:
            print(f"  {home} vs {away} ({date})")


if __name__ == "__main__":
    main()
