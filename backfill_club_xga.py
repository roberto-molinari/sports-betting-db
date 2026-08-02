"""
Backfill soccer_player_stats.club_xga_per90 (expected goals allowed by a player's
team in a given match) for existing rows. No new API calls needed -- a team's xG
allowed in a match is just the sum of the OPPOSING team's players' already-imported
xg for that same match, mirroring exactly how club_ga_per90 is already derived from
our own match scores (see import_club_player_stats.py) rather than pulled from the
API, which has no expected-goals-against field at all (checked 2026-08-02).

Usage:
    python backfill_club_xga.py --season 2024
    python backfill_club_xga.py --season 2023 --season 2024 --season 2025
"""

import argparse
import sqlite3

from core.sports_db import DATABASE_PATH


def compute_match_team_xg(conn, season):
    """{(match_id, venue): summed team xG} for all rows in `season`."""
    cur = conn.cursor()
    cur.execute("""
        SELECT match_id, venue, SUM(xg)
        FROM soccer_player_stats
        WHERE season = ? AND match_id IS NOT NULL AND venue IS NOT NULL
        GROUP BY match_id, venue
    """, (season,))
    return {(mid, venue): xg or 0.0 for mid, venue, xg in cur.fetchall()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, action="append", required=True,
                        help="Season to backfill (repeatable).")
    args = parser.parse_args()

    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()

    for season in args.season:
        team_xg = compute_match_team_xg(conn, season)
        opponent_venue = {"home": "away", "away": "home"}

        cur.execute("""
            SELECT stat_id, match_id, venue FROM soccer_player_stats
            WHERE season = ? AND match_id IS NOT NULL AND venue IS NOT NULL
        """, (season,))
        rows = cur.fetchall()

        updated = 0
        for stat_id, match_id, venue in rows:
            opp_xg = team_xg.get((match_id, opponent_venue[venue]))
            if opp_xg is None:
                continue
            cur.execute(
                "UPDATE soccer_player_stats SET club_xga_per90 = ? WHERE stat_id = ?",
                (opp_xg, stat_id)
            )
            updated += 1
        conn.commit()
        print(f"season={season}: {updated}/{len(rows)} rows updated")

    conn.close()


if __name__ == "__main__":
    main()
