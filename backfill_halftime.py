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
from datetime import datetime, timedelta
from sports_db import DATABASE_PATH

API_BASE = "https://api.football-data.org/v4"
COMPETITION = "SA"

# Map football-data.org team names → canonical DB names
TEAM_NAME_MAP = {
    "FC Internazionale Milano": "Inter",
    "Inter Milan": "Inter",
    "Juventus FC": "Juventus",
    "SSC Napoli": "Napoli",
    "SS Lazio": "Lazio",
    "Atalanta BC": "Atalanta",
    "ACF Fiorentina": "Fiorentina",
    "Hellas Verona FC": "Hellas Verona",
    "Bologna FC 1909": "Bologna",
    "Empoli FC": "Empoli",
    "Torino FC": "Torino",
    "Udinese Calcio": "Udinese",
    "US Sassuolo Calcio": "Sassuolo",
    "US Cremonese": "Cremonese",
    "US Lecce": "Lecce",
    "US Salernitana 1919": "Salernitana",
    "Spezia Calcio": "Spezia",
    "AC Monza": "Monza",
    "Cagliari Calcio": "Cagliari",
    "Genoa CFC": "Genoa",
    "Frosinone Calcio": "Frosinone",
    "AC Milan": "Milan",
    "AS Roma": "Roma",
    "ACF Fiorentina": "Fiorentina",
    "Venezia FC": "Venezia",
    "Parma Calcio 1913": "Parma",
    "Como 1907": "Como",
}


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

        # Build lookup: (home_team_name, away_team_name) -> list of (date, hhs, has_)
        # Using a list because theoretically two fixtures could exist, but in practice one per season
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
            home = TEAM_NAME_MAP.get(m["homeTeam"]["name"], m["homeTeam"]["name"])
            away = TEAM_NAME_MAP.get(m["awayTeam"]["name"], m["awayTeam"]["name"])
            date = m["utcDate"][:10]
            ht_lookup.setdefault((home, away), []).append((date, hhs, has_))

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
            db_date = datetime.strptime(match_date[:10], "%Y-%m-%d")
            home_norm = TEAM_NAME_MAP.get(home_name, home_name)
            away_norm = TEAM_NAME_MAP.get(away_name, away_name)
            candidates = ht_lookup.get((home_norm, away_norm), [])
            # Match within ±3 days to handle matchday date vs actual kickoff date skew
            matched = None
            for api_date_str, hhs, has_ in candidates:
                api_date = datetime.strptime(api_date_str, "%Y-%m-%d")
                if abs((api_date - db_date).days) <= 3:
                    matched = (hhs, has_)
                    break
            if matched:
                hhs, has_ = matched
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
