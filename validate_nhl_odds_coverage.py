"""
NHL odds coverage audit.

Usage:
  python validate_nhl_odds_coverage.py --season 2025
  python validate_nhl_odds_coverage.py  # defaults to latest season in nhl_matches
"""

import argparse
import sqlite3

from core.sports_db import DATABASE_PATH


def latest_season(conn):
    cur = conn.cursor()
    cur.execute("SELECT MAX(season) FROM nhl_matches")
    row = cur.fetchone()
    return row[0] if row and row[0] is not None else None


def pct(part, whole):
    if not whole:
        return 0.0
    return round(100.0 * part / whole, 1)


def main():
    parser = argparse.ArgumentParser(description="Audit NHL odds coverage for a season.")
    parser.add_argument(
        "--season",
        type=int,
        default=None,
        help="Season start year (e.g. 2025 for 2025-26). Defaults to latest season in nhl_matches.",
    )
    args = parser.parse_args()

    conn = sqlite3.connect(DATABASE_PATH)
    try:
        season = args.season if args.season is not None else latest_season(conn)
        if season is None:
            raise SystemExit("No NHL matches found in database.")

        cur = conn.cursor()

        cur.execute(
            """
            WITH per_match AS (
              SELECT m.match_id,
                     MAX(CASE WHEN o.match_id IS NOT NULL THEN 1 ELSE 0 END) AS has_any_odds,
                     MAX(CASE WHEN o.home_moneyline IS NOT NULL AND o.away_moneyline IS NOT NULL THEN 1 ELSE 0 END) AS has_full_ml,
                     MAX(CASE WHEN (o.home_moneyline IS NOT NULL) <> (o.away_moneyline IS NOT NULL) THEN 1 ELSE 0 END) AS has_one_sided_ml,
                     MAX(CASE WHEN o.over_under IS NOT NULL THEN 1 ELSE 0 END) AS has_ou_line,
                     MAX(CASE WHEN o.over_under IS NOT NULL AND o.over_odds IS NOT NULL AND o.under_odds IS NOT NULL THEN 1 ELSE 0 END) AS has_full_ou,
                     MAX(CASE WHEN o.spread_home IS NOT NULL AND o.spread_away IS NOT NULL THEN 1 ELSE 0 END) AS has_spread_line,
                     MAX(CASE WHEN o.spread_home IS NOT NULL AND o.spread_away IS NOT NULL
                                   AND o.spread_home_odds IS NOT NULL AND o.spread_away_odds IS NOT NULL THEN 1 ELSE 0 END) AS has_full_spread
              FROM nhl_matches m
              LEFT JOIN nhl_betting_odds o ON o.match_id = m.match_id
              WHERE m.season = ?
                AND m.match_status = 'completed'
              GROUP BY m.match_id
            )
            SELECT COUNT(*) AS completed_games,
                   SUM(has_any_odds) AS with_any_odds,
                   SUM(has_full_ml) AS with_full_moneyline,
                   SUM(has_one_sided_ml) AS with_one_sided_moneyline,
                   SUM(has_ou_line) AS with_ou_line,
                   SUM(has_full_ou) AS with_full_ou,
                   SUM(has_spread_line) AS with_spread_line,
                   SUM(has_full_spread) AS with_full_spread
            FROM per_match
            """,
            (season,),
        )
        row = cur.fetchone()

        completed_games = row[0] or 0
        with_any_odds = row[1] or 0
        with_full_ml = row[2] or 0
        with_one_sided_ml = row[3] or 0
        with_ou_line = row[4] or 0
        with_full_ou = row[5] or 0
        with_spread_line = row[6] or 0
        with_full_spread = row[7] or 0

        print(f"NHL Odds Coverage Audit - season {season}")
        print("=" * 48)
        print(f"Completed games:             {completed_games}")
        print(f"With any odds:              {with_any_odds} ({pct(with_any_odds, completed_games)}%)")
        print(f"With full moneyline:        {with_full_ml} ({pct(with_full_ml, completed_games)}%)")
        print(f"With one-sided moneyline:   {with_one_sided_ml} ({pct(with_one_sided_ml, completed_games)}%)")
        print(f"With O/U line:              {with_ou_line} ({pct(with_ou_line, completed_games)}%)")
        print(f"With full O/U:              {with_full_ou} ({pct(with_full_ou, completed_games)}%)")
        print(f"With spread line:           {with_spread_line} ({pct(with_spread_line, completed_games)}%)")
        print(f"With full spread:           {with_full_spread} ({pct(with_full_spread, completed_games)}%)")
        print(f"Missing any odds:           {completed_games - with_any_odds}")

        cur.execute(
            """
            SELECT o.sportsbook, COUNT(*) AS rows
            FROM nhl_betting_odds o
            JOIN nhl_matches m ON m.match_id = o.match_id
            WHERE m.season = ?
            GROUP BY o.sportsbook
            ORDER BY rows DESC, o.sportsbook
            """,
            (season,),
        )
        books = cur.fetchall()
        if books:
            print("\nRows by sportsbook:")
            for book, rows_count in books:
                print(f"  {book}: {rows_count}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
