"""
Repair broken club_ga_per90 values caused by TheStatsAPI returning an
unreliable `matches_played` for some clubs (mostly non-European leagues).

A too-low matches_played inflates club_ga_per90 (goals_against / matches_played)
to impossible values (e.g. Al Ahly 4.0). We detect the symptom — a stored
club_ga_per90 at/above a threshold — and recompute it with a robust games
denominator that does NOT trust matches_played:

    games = max(matches_played, round(max_club_player_minutes / 90))
    club_ga_per90 = goals_against / games

A regular starter plays nearly every match, so the busiest squad player's
minutes/90 is a reliable floor on games played. This "un-breaks" the value into
the sane range; it is not exact (the API's goals_against can also be off for
these leagues — see WC notes), which is acceptable for v1.

Only the flagged clubs are re-fetched (cheap), so this costs ~3 requests/club.

Usage:
    python fix_wc_club_defense.py --dry-run      # show proposed changes
    python fix_wc_club_defense.py                # apply
    python fix_wc_club_defense.py --threshold 2.0
"""

import argparse
import sqlite3
import sys

from core.sports_db import DATABASE_PATH
from core.thestatsapi import Client, TheStatsAPIError
from import_wc_player_stats import club_meta


def parse_args():
    parser = argparse.ArgumentParser(description="Fix inflated club_ga_per90 from bad matches_played.")
    parser.add_argument("--threshold", type=float, default=2.2,
                        help="Flag clubs whose stored club_ga_per90 >= this (default 2.2).")
    parser.add_argument("--api-key", help="Override the THE_STATS_API_API_KEY env var.")
    parser.add_argument("--max-requests", type=int, default=300,
                        help="Abort if more than this many API requests are issued.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show proposed changes without writing.")
    return parser.parse_args()


def find_suspect_clubs(conn, threshold):
    """Return [(api_club_id, club, stored_ga, max_minutes)] above the threshold."""
    return conn.execute(
        """SELECT p.api_club_id, MIN(p.club) AS club,
                  MAX(s.club_ga_per90) AS stored_ga,
                  MAX(s.minutes_played) AS max_minutes
           FROM soccer_wc_players p
           JOIN soccer_wc_player_stats s ON s.player_id = p.player_id
           WHERE s.club_ga_per90 IS NOT NULL AND p.api_club_id IS NOT NULL
           GROUP BY p.api_club_id
           HAVING MAX(s.club_ga_per90) >= ?
           ORDER BY stored_ga DESC""",
        (threshold,)
    ).fetchall()


def main():
    args = parse_args()
    try:
        client = Client(api_key=args.api_key, max_requests=args.max_requests)
    except TheStatsAPIError as exc:
        sys.exit(str(exc))

    conn = sqlite3.connect(DATABASE_PATH)
    suspects = find_suspect_clubs(conn, args.threshold)
    if not suspects:
        conn.close()
        print(f"No clubs with club_ga_per90 >= {args.threshold}. Nothing to fix.")
        return

    print(f"{'CLUB':<22}{'OLD GA':>7}{'GAMES':>7}{'NEW GA':>8}  (gf/ga/mp from API)")
    meta_cache = {}
    fixed = 0
    try:
        for club_id, club, stored_ga, max_minutes in suspects:
            meta = club_meta(client, club_id, meta_cache)
            if not meta or not meta.get("season_id"):
                print(f"{club:<22}  (could not resolve season — skipped)")
                continue
            stats = client.get_data(f"teams/{club_id}/stats", {"season_id": meta["season_id"]}) or {}
            ga = stats.get("goals_against")
            mp = stats.get("matches_played") or 0
            if ga is None:
                print(f"{club:<22}  (no goals_against — skipped)")
                continue
            games = max(mp, round((max_minutes or 0) / 90), 1)
            new_ga = ga / games
            print(f"{club:<22}{stored_ga:>7.2f}{games:>7}{new_ga:>8.2f}  "
                  f"(gf={stats.get('goals_for')}/ga={ga}/mp={mp})")
            if not args.dry_run:
                conn.execute(
                    """UPDATE soccer_wc_player_stats
                       SET club_ga_per90 = ?
                       WHERE player_id IN (
                           SELECT player_id FROM soccer_wc_players WHERE api_club_id = ?)""",
                    (new_ga, club_id)
                )
                conn.commit()
                fixed += 1
    except TheStatsAPIError as exc:
        print(f"\nAborted: {exc}")
    finally:
        conn.close()

    print(f"\n{'(dry-run) ' if args.dry_run else ''}Clubs fixed: {fixed}/{len(suspects)}  "
          f"API requests used: {client.requests_made}")
    if not args.dry_run:
        print("Re-run compute_wc_team_strength.py --persist to refresh team lambdas.")


if __name__ == "__main__":
    main()
