"""
api-football.com Serie A Historical Importer
=============================================
Fetches historical Serie A seasons from api-football.com (v3) and inserts
completed matches into the local database.

Usage:
    python import_api_football_history.py <api_key>
    python import_api_football_history.py <api_key> --seasons 2018 2019 2020

    Default: imports seasons 2013 through 2022 (years not yet in the database).
    The database already has seasons 2023, 2024, and 2025.

Getting an API key:
    1. Register (free) at https://dashboard.api-football.com/register
    2. The free tier allows 100 requests/day — more than enough for this import.

Notes:
    - 1 request per season; 10 seasons = 10 requests total
    - Only FINISHED matches are inserted (status: FT, AET, PEN)
    - Duplicate matches already in the DB are skipped safely
    - Halftime scores are included for all available fixtures
    - Serie A league ID on api-football.com: 135
"""

import sqlite3
import requests
import time
import argparse

DB_PATH = 'sports_betting.db'
API_BASE = 'https://v3.football.api-sports.io'
SERIE_A_LEAGUE_ID = 135
FINISHED_STATUSES = {'FT', 'AET', 'PEN'}

# Seasons already present in the database — skip these by default
EXISTING_SEASONS = {2023, 2024, 2025}


# ---------------------------------------------------------------------------
# DB helpers  (mirrors the pattern in update_serie_a_results.py)
# ---------------------------------------------------------------------------

def season_from_date(date_str):
    """Derive the DB season year from a date string like '2014-09-20T18:45:00+00:00'."""
    year  = int(date_str[:4])
    month = int(date_str[5:7])
    return year if month >= 7 else year - 1


def load_team_map(conn):
    """Return {team_name: team_id} for all Serie A teams already in the DB."""
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


def load_existing_match_keys(conn):
    """
    Return a set of (home_team_id, away_team_id, date_str) for all
    Serie A matches already in the DB, used for duplicate detection.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT home_team_id, away_team_id, DATE(match_date)
        FROM soccer_matches
        WHERE league = 'Serie A'
    """)
    return {(r[0], r[1], r[2]) for r in cur.fetchall()}


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def fetch_season_fixtures(api_key, season):
    """Fetch all fixtures for a Serie A season from api-football.com v3."""
    headers = {'x-apisports-key': api_key}
    params  = {'league': SERIE_A_LEAGUE_ID, 'season': season}

    print(f"  Fetching season {season}-{str(season + 1)[-2:]}...", end=' ', flush=True)
    r = requests.get(f"{API_BASE}/fixtures", headers=headers, params=params, timeout=30)
    r.raise_for_status()

    data = r.json()
    errors = data.get('errors')
    if errors and (isinstance(errors, dict) and errors) or (isinstance(errors, list) and errors):
        raise RuntimeError(f"API returned errors: {errors}")

    fixtures = data.get('response', [])
    print(f"{len(fixtures)} fixtures received")
    return fixtures


# ---------------------------------------------------------------------------
# Import logic
# ---------------------------------------------------------------------------

def import_season(conn, api_key, season):
    """Fetch and insert all completed fixtures for a single season."""
    if season in EXISTING_SEASONS:
        print(f"  Season {season} already in database — skipping.")
        return 0

    team_map = load_team_map(conn)
    existing = load_existing_match_keys(conn)

    fixtures = fetch_season_fixtures(api_key, season)

    inserted        = 0
    skipped_status  = 0
    skipped_dupe    = 0
    skipped_no_score = 0
    cur = conn.cursor()

    for f in fixtures:
        status = f['fixture']['status']['short']
        if status not in FINISHED_STATUSES:
            skipped_status += 1
            continue

        home_name = f['teams']['home']['name']
        away_name = f['teams']['away']['name']
        date_iso  = f['fixture']['date']   # e.g. "2014-09-20T18:45:00+00:00"
        date_str  = date_iso[:10]          # "2014-09-20"

        ft  = f['score']['fulltime']
        ht  = f['score']['halftime']
        hs, as_ = ft.get('home'), ft.get('away')
        hhs, has_ = ht.get('home'), ht.get('away')

        if hs is None or as_ is None:
            skipped_no_score += 1
            continue

        home_id = ensure_team(conn, team_map, home_name)
        away_id = ensure_team(conn, team_map, away_name)
        key = (home_id, away_id, date_str)

        if key in existing:
            skipped_dupe += 1
            continue

        match_season = season_from_date(date_iso)

        cur.execute("""
            INSERT INTO soccer_matches
                (league, season, home_team_id, away_team_id, match_date,
                 home_score, away_score,
                 halftime_home_score, halftime_away_score,
                 match_status)
            VALUES ('Serie A', ?, ?, ?, ?, ?, ?, ?, ?, 'completed')
        """, (match_season, home_id, away_id, date_iso,
              hs, as_, hhs, has_))

        existing.add(key)
        inserted += 1

    conn.commit()

    detail = []
    if skipped_status:  detail.append(f"{skipped_status} not finished")
    if skipped_dupe:    detail.append(f"{skipped_dupe} already in DB")
    if skipped_no_score: detail.append(f"{skipped_no_score} missing score")
    detail_str = f" ({', '.join(detail)})" if detail else ""
    print(f"  → {inserted} inserted{detail_str}")
    return inserted


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Import historical Serie A match data from api-football.com'
    )
    parser.add_argument('api_key', help='Your api-football.com API key')
    parser.add_argument(
        '--seasons', nargs='+', type=int,
        default=list(range(2013, 2023)),
        help='Season start years to import (default: 2013–2022)'
    )
    parser.add_argument(
        '--delay', type=float, default=1.5,
        help='Seconds between API requests (default: 1.5)'
    )
    args = parser.parse_args()

    # Warn if any requested seasons are already in the DB
    overlap = set(args.seasons) & EXISTING_SEASONS
    if overlap:
        print(f"Note: seasons {sorted(overlap)} are already in the DB and will be skipped.")

    conn = sqlite3.connect(DB_PATH)
    try:
        total = 0
        seasons = args.seasons
        for i, season in enumerate(seasons):
            print(f"\n[{i + 1}/{len(seasons)}] Season {season}-{str(season + 1)[-2:]}:")
            inserted = import_season(conn, args.api_key, season)
            total += inserted
            if i < len(seasons) - 1:
                time.sleep(args.delay)

        print(f"\n{'=' * 50}")
        print(f"Import complete. Total matches inserted: {total}")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
