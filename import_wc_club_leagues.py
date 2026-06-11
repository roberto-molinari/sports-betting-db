"""
Backfill soccer_wc_players.club_league — the player's club competition name.

The squad import stores each player's club (id + name) but not the club's
league, so the league-quality factor in compute_wc_team_strength.py is currently
inert (every player defaults to the same factor). This resolves each distinct
club's primary competition once (GET /teams/{id} -> primary_competition.name)
and writes it to all of that club's players.

Cost: ~1 request per distinct club (~429).

Usage:
    python import_wc_club_leagues.py --dry-run     # show what would be set
    python import_wc_club_leagues.py               # apply
"""

import argparse
import sqlite3
import sys
from collections import Counter

from core.sports_db import DATABASE_PATH
from core.thestatsapi import Client, TheStatsAPIError


def parse_args():
    parser = argparse.ArgumentParser(description="Backfill club_league for WC players.")
    parser.add_argument("--api-key", help="Override the THE_STATS_API_API_KEY env var.")
    parser.add_argument("--max-requests", type=int, default=800,
                        help="Abort if more than this many API requests are issued.")
    parser.add_argument("--only-missing", action="store_true",
                        help="Only resolve clubs that don't already have a league set.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Resolve and report without writing.")
    return parser.parse_args()


def distinct_clubs(conn, only_missing):
    sql = """SELECT api_club_id, MIN(club) AS club, COUNT(*) AS n
             FROM soccer_wc_players
             WHERE api_club_id IS NOT NULL"""
    if only_missing:
        sql += " AND (club_league IS NULL OR club_league = '')"
    sql += " GROUP BY api_club_id ORDER BY n DESC"
    return conn.execute(sql).fetchall()


def main():
    args = parse_args()
    try:
        client = Client(api_key=args.api_key, max_requests=args.max_requests)
    except TheStatsAPIError as exc:
        sys.exit(str(exc))

    conn = sqlite3.connect(DATABASE_PATH)
    clubs = distinct_clubs(conn, args.only_missing)
    if not clubs:
        conn.close()
        print("No clubs to resolve.")
        return

    print(f"Resolving leagues for {len(clubs)} clubs...")
    league_counts = Counter()
    resolved = unresolved = 0
    try:
        for i, (club_id, club, n) in enumerate(clubs, 1):
            team = client.get_data(f"teams/{club_id}")
            league = ((team or {}).get("primary_competition") or {}).get("name")
            if league:
                league_counts[league] += 1
                resolved += 1
                if not args.dry_run:
                    conn.execute(
                        "UPDATE soccer_wc_players SET club_league = ? WHERE api_club_id = ?",
                        (league, club_id)
                    )
                    conn.commit()
            else:
                unresolved += 1
            if i % 50 == 0:
                print(f"  ...{i}/{len(clubs)} clubs, {client.requests_made} requests", flush=True)
    except TheStatsAPIError as exc:
        print(f"\nAborted: {exc}")
    finally:
        conn.close()

    print(f"\n{'(dry-run) ' if args.dry_run else ''}Resolved {resolved} clubs, "
          f"{unresolved} unresolved. API requests: {client.requests_made}")
    print("\nLeagues found (club count):")
    for league, c in league_counts.most_common():
        print(f"  {c:>3}  {league}")


if __name__ == "__main__":
    main()
