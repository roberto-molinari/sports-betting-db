"""
One-time backfill: fetch halftime scores from football-data.org for all
completed soccer_matches that currently have NULL halftime_home_score.

Usage:
    python backfill_halftime.py <api_key>
    python backfill_halftime.py a816e87178fb49ff9197795f16b3df59
"""

import sys
import time
import sqlite3
import requests
from sports_db import DATABASE_PATH

API_BASE = "https://api.football-data.org/v4"
COMPETITION = "SA"


def fetch_season(api_key: str, season: int) -> list:
    url = f"{API_BASE}/competitions/{COMPETITION}/matches?season={season}"
    resp = requests.get(url, headers={"X-Auth-Token": api_key})
    resp.raise_for_status()
    return resp.json().get("matches", [])


def backfill(api_key: str):
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()

    # Find seasons that have at least one match with NULL halftime
    cur.execute("""
        SELECT DISTINCT season FROM soccer_matches
        WHERE match_status = 'completed'
          AND halftime_home_score IS NULL
        ORDER BY season
    """)
    seasons = [r[0] for r in cur.fetchall()]

    if not seasons:
        print("No matches need halftime backfill.")
        conn.close()
        return

    print(f"Seasons needing backfill: {seasons}")

    total_updated = 0

    for i, season in enumerate(seasons):
        if i > 0:
            print("  (waiting 6 s for rate limit...)")
            time.sleep(6)

        print(f"\nFetching season {season}...")
        matches = fetch_season(api_key, season)

        # Build lookup: (home_team_name, away_team_name, date) -> halftime scores
        ht_lookup: dict = {}
        for m in matches:
            if m.get("status") != "FINISHED":
                continue
            score = m.get("score", {})
            ht = score.get("halfTime", {})
            hhs = ht.get("home")
            has_ = ht.get("away")
            if hhs is None or has_ is None:
                continue
            home = m["homeTeam"]["name"]
            away = m["awayTeam"]["name"]
            date = m["utcDate"][:10]
            ht_lookup[(home, away, date)] = (hhs, has_)

        # Fetch rows needing update for this season
        cur.execute("""
            SELECT sm.match_id, sm.match_date,
                   ht.name AS home_name, at.name AS away_name
            FROM soccer_matches sm
            JOIN soccer_teams ht ON ht.team_id = sm.home_team_id
            JOIN soccer_teams at ON at.team_id = sm.away_team_id
            WHERE sm.season = ?
              AND sm.match_status = 'completed'
              AND sm.halftime_home_score IS NULL
        """, (season,))
        rows = cur.fetchall()

        updated = 0
        not_found = 0
        for match_id, match_date, home_name, away_name in rows:
            date_str = match_date[:10]
            key = (home_name, away_name, date_str)
            if key in ht_lookup:
                hhs, has_ = ht_lookup[key]
                cur.execute("""
                    UPDATE soccer_matches
                    SET halftime_home_score = ?, halftime_away_score = ?
                    WHERE match_id = ?
                """, (hhs, has_, match_id))
                updated += 1
            else:
                not_found += 1

        conn.commit()
        print(f"  Season {season}: {updated} updated, {not_found} not found in API response")
        total_updated += updated

    print(f"\nBackfill complete. Total rows updated: {total_updated}")

    # Summary
    cur.execute("""
        SELECT COUNT(*) FROM soccer_matches
        WHERE match_status = 'completed' AND halftime_home_score IS NULL
    """)
    remaining = cur.fetchone()[0]
    print(f"Remaining NULL halftime rows: {remaining}")
    conn.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python backfill_halftime.py <api_key>")
        sys.exit(1)
    backfill(sys.argv[1])
