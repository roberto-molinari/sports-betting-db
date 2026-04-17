"""
Weekly Serie A Results Updater
==============================
Syncs the local database with football-data.org for a given Serie A season:
  - Inserts any fixtures not yet in the database
  - Scores (full-time + half-time) are written on insert for FINISHED matches
  - Updates existing 'scheduled' records whose matches are now FINISHED
  - Adds any new teams encountered (e.g. newly promoted clubs)

Usage:
    python update_serie_a_results.py <api_key>
    python update_serie_a_results.py <api_key> --season 2024   # sync 2024-25 season

Requirements:
    - football-data.org API key (free tier: https://www.football-data.org/client/register)
    - Free tier supports current season + 1 past season; rate limit: 10 req/min
    - Only 2 API calls are made per run (teams + matches), well within the free tier limit

Schedule (macOS cron example – run every Monday at 8am):
    0 8 * * 1 cd /Users/robertomolinari/code/sports-betting-db && \
              source .venv/bin/activate && \
              python update_serie_a_results.py YOUR_API_KEY >> update_log.txt 2>&1
"""

import sqlite3
import requests
import time
import argparse
from datetime import datetime

DB_PATH = 'sports_betting.db'
API_BASE = 'https://api.football-data.org/v4'
SERIE_A_CODE = 'SA'
RATE_LIMIT_DELAY = 6  # seconds between requests (free tier: 10 req/min)


def current_season_year():
    """Return the start year of the current Serie A season (e.g. 2025 for 2025-26)."""
    now = datetime.now()
    return now.year if now.month >= 7 else now.year - 1


def season_from_date(utc_date_str):
    """Derive the DB season year from a UTC date string like '2025-09-14T18:45:00Z'."""
    year = int(utc_date_str[:4])
    month = int(utc_date_str[5:7])
    return year if month >= 7 else year - 1


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def load_team_map(conn):
    """Return {team_name: team_id} for all Serie A teams in the DB."""
    cur = conn.cursor()
    cur.execute("SELECT name, team_id FROM soccer_teams WHERE league = 'Serie A'")
    return dict(cur.fetchall())


def ensure_team(conn, team_map, name):
    """Return the DB team_id for *name*, inserting a new row if needed."""
    if name in team_map:
        return team_map[name]
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO soccer_teams (name, league, country) VALUES (?, 'Serie A', 'Italy')",
        (name,)
    )
    conn.commit()
    team_id = cur.lastrowid
    team_map[name] = team_id
    print(f"  + New team added: {name} (id={team_id})")
    return team_id


def load_existing_matches(conn):
    """
    Return a dict (home_team_id, away_team_id, date_str) -> (match_id, match_status)
    for all Serie A matches already in the DB.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT match_id, home_team_id, away_team_id,
               DATE(match_date), match_status
        FROM soccer_matches
    """)
    rows = cur.fetchall()
    return {(r[1], r[2], r[3]): (r[0], r[4]) for r in rows}


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def fetch_season_data(api_key, season):
    """
    Fetch teams and all matches for a Serie A season.
    Returns (teams_list, matches_list) from the API.
    Makes 2 API calls with a rate-limit delay between them.
    """
    headers = {'X-Auth-Token': api_key}
    params = {'season': season}

    print(f"  Fetching teams for season {season}...", end=' ', flush=True)
    r = requests.get(f"{API_BASE}/competitions/{SERIE_A_CODE}/teams",
                     headers=headers, params=params, timeout=15)
    r.raise_for_status()
    teams = r.json().get('teams', [])
    print(f"{len(teams)} teams")
    time.sleep(RATE_LIMIT_DELAY)

    print(f"  Fetching matches for season {season}...", end=' ', flush=True)
    r = requests.get(f"{API_BASE}/competitions/{SERIE_A_CODE}/matches",
                     headers=headers, params=params, timeout=15)
    r.raise_for_status()
    matches = r.json().get('matches', [])
    print(f"{len(matches)} matches")

    return teams, matches


# ---------------------------------------------------------------------------
# Main sync logic
# ---------------------------------------------------------------------------

def sync_season(api_key, season):
    conn = sqlite3.connect(DB_PATH)
    try:
        team_map = load_team_map(conn)
        existing = load_existing_matches(conn)

        _, api_matches = fetch_season_data(api_key, season)

        inserted = 0
        updated  = 0
        skipped  = 0

        cur = conn.cursor()

        for m in api_matches:
            home_name  = m['homeTeam']['name']
            away_name  = m['awayTeam']['name']
            utc_date   = m['utcDate']        # e.g. "2025-09-14T18:45:00Z"
            date_str   = utc_date[:10]       # "2025-09-14"
            api_status = m['status']         # FINISHED / SCHEDULED / TIMED / etc.

            home_id = ensure_team(conn, team_map, home_name)
            away_id = ensure_team(conn, team_map, away_name)

            key = (home_id, away_id, date_str)

            if key in existing:
                match_id, db_status = existing[key]
                if api_status == 'FINISHED' and db_status != 'completed':
                    score   = m.get('score', {})
                    ft      = score.get('fullTime', {})
                    ht      = score.get('halfTime', {})
                    hs, as_ = ft.get('home'), ft.get('away')
                    hhs, has_ = ht.get('home'), ht.get('away')
                    if hs is not None and as_ is not None:
                        cur.execute("""
                            UPDATE soccer_matches
                            SET home_score = ?, away_score = ?,
                                halftime_home_score = ?, halftime_away_score = ?,
                                match_status = 'completed'
                            WHERE match_id = ?
                        """, (hs, as_, hhs, has_, match_id))
                        print(f"  Updated  [{match_id}] {home_name} {hs}-{as_} {away_name}")
                        updated += 1
                    else:
                        skipped += 1
                else:
                    skipped += 1
            else:
                # New fixture — insert it
                db_status    = 'completed' if api_status == 'FINISHED' else 'scheduled'
                match_season = season_from_date(utc_date)

                cur.execute("""
                    INSERT INTO soccer_matches
                        (league, season, home_team_id, away_team_id,
                         match_date, match_status)
                    VALUES ('Serie A', ?, ?, ?, ?, ?)
                """, (match_season, home_id, away_id, utc_date, db_status))
                match_id = cur.lastrowid

                score_str = ''
                if api_status == 'FINISHED':
                    score   = m.get('score', {})
                    ft      = score.get('fullTime', {})
                    ht      = score.get('halfTime', {})
                    hs, as_ = ft.get('home'), ft.get('away')
                    hhs, has_ = ht.get('home'), ht.get('away')
                    if hs is not None and as_ is not None:
                        cur.execute("""
                            UPDATE soccer_matches
                            SET home_score = ?, away_score = ?,
                                halftime_home_score = ?, halftime_away_score = ?
                            WHERE match_id = ?
                        """, (hs, as_, hhs, has_, match_id))
                        score_str = f" [{hs}-{as_}]"

                print(f"  Inserted [{match_id}] {date_str}  {home_name} vs {away_name}{score_str}")
                existing[key] = (match_id, db_status)
                inserted += 1

        conn.commit()
        print(f"\nSeason {season}: {inserted} inserted, {updated} updated, {skipped} unchanged.")

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Sync Serie A fixtures and results from football-data.org.'
    )
    parser.add_argument('api_key', help='Your football-data.org API key')
    parser.add_argument(
        '--season', type=int, default=None,
        metavar='YYYY',
        help='Season start year to sync (default: current season)'
    )
    args = parser.parse_args()

    season = args.season or current_season_year()
    print(f"=== Serie A Sync  {datetime.now().strftime('%Y-%m-%d %H:%M')}  season={season} ===\n")
    sync_season(args.api_key, season)
    print("\nDone.")


if __name__ == '__main__':
    main()
