"""
Unified Serie A Results Updater
===============================
Syncs the local database with the latest Serie A match results for a given season.
Supports two data sources:
  - 'api': Fetches data from football-data.org (default, requires API key).
  - 'csv': Downloads and processes the latest CSV from football-data.co.uk (no key needed).

API Usage (Default):
    # Update current season from API
    python update_serie_a_results.py <api_key>

    # Update specific season from API
    python update_serie_a_results.py <api_key> --season 2024

CSV Usage (Fallback):
    # Update current season from CSV
    python update_serie_a_results.py --source csv

Requirements for API usage:
    - football-data.org API key (free tier: https://www.football-data.org/client/register)
    - Free tier supports current season + 1 past season; rate limit: 10 req/min.

Schedule (macOS cron example – run every Monday at 8am using API):
    0 8 * * 1 cd /Users/robertomolinari/code/sports-betting-db && \
              source .venv/bin/activate && \
              python update_serie_a_results.py YOUR_API_KEY >> update_log.txt 2>&1
"""

import sqlite3
import requests
import time
import argparse
import csv
from datetime import datetime

DB_PATH = 'sports_betting.db'

# --- API constants ---
API_BASE = 'https://api.football-data.org/v4'
SERIE_A_CODE = 'SA'
RATE_LIMIT_DELAY = 6  # seconds between requests (free tier: 10 req/min)

# --- CSV constants ---
CSV_URL_TEMPLATE = "https://www.football-data.co.uk/mmz4281/{season_short}/I1.csv"

# --- Team Name Mappings ---

# Maps football-data.org long team names → canonical DB names
API_TEAM_NAME_MAP = {
    'FC Internazionale Milano': 'Inter',
    'Inter Milan': 'Inter',
    'Juventus FC': 'Juventus',
    'SSC Napoli': 'Napoli',
    'SS Lazio': 'Lazio',
    'Atalanta BC': 'Atalanta',
    'ACF Fiorentina': 'Fiorentina',
    'Hellas Verona FC': 'Hellas Verona',
    'Bologna FC 1909': 'Bologna',
    'Empoli FC': 'Empoli',
    'Torino FC': 'Torino',
    'Udinese Calcio': 'Udinese',
    'US Sassuolo Calcio': 'Sassuolo',
    'US Cremonese': 'Cremonese',
    'US Lecce': 'Lecce',
    'US Salernitana 1919': 'Salernitana',
    'Spezia Calcio': 'Spezia',
    'AC Monza': 'Monza',
}

# Maps football-data.co.uk CSV team names -> canonical DB names
CSV_TEAM_NAME_MAP = {
    'Atalanta': 'Atalanta',
    'Bologna': 'Bologna',
    'Cagliari': 'Cagliari Calcio',
    'Como': 'Como 1907',
    'Cremonese': 'Cremonese',
    'Empoli': 'Empoli FC',
    'Fiorentina': 'Fiorentina',
    'Genoa': 'Genoa CFC',
    'Inter': 'Inter',
    'Juventus': 'Juventus',
    'Lazio': 'Lazio',
    'Lecce': 'Lecce',
    'Milan': 'AC Milan',
    'Napoli': 'Napoli',
    'Parma': 'Parma Calcio 1913',
    'Pisa': 'AC Pisa 1909',
    'Roma': 'AS Roma',
    'Sassuolo': 'Sassuolo',
    'Torino': 'Torino',
    'Udinese': 'Udinese',
    'Verona': 'Hellas Verona',
}


def current_season_year():
    """Return the start year of the current Serie A season (e.g. 2025 for 2025-26)."""
    now = datetime.now()
    return now.year if now.month >= 7 else now.year - 1


def season_from_date(utc_date_str):
# ...existing code...
    year = int(utc_date_str[:4])
    month = int(utc_date_str[5:7])
    return year if month >= 7 else year - 1


def parse_int_or_none(value):
    """Safely parse a string to an int, returning None if invalid."""
    s = (value or '').strip()
    return int(s) if s.isdigit() else None

# ...existing code...
# DB helpers
# ---------------------------------------------------------------------------

def load_team_map(conn):
# ...existing code...
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


def load_existing_matches(conn, season):
    """
    Return a dict (home_team_id, away_team_id) -> list of (match_id, date_str, match_status)
    for a given Serie A season. Fuzzy date matching is used later to handle date discrepancies.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT match_id, home_team_id, away_team_id,
               DATE(match_date), match_status
        FROM soccer_matches
        WHERE league = 'Serie A' AND season = ?
    """, (season,))
    rows = cur.fetchall()
    result = {}
    for match_id, home_id, away_id, date_str, status in rows:
        result.setdefault((home_id, away_id), []).append((match_id, date_str, status))
    return result


# ---------------------------------------------------------------------------
# API Sync Logic
# ---------------------------------------------------------------------------

def fetch_season_data(api_key, season):
# ...existing code...
    headers = {'X-Auth-Token': api_key}
    params = {'season': season}

    print(f"  Fetching teams for season {season}...", end=' ', flush=True)
# ...existing code...
    r = requests.get(f"{API_BASE}/competitions/{SERIE_A_CODE}/teams",
                     headers=headers, params=params, timeout=15)
    if r.status_code == 400:
        # Some API plans/endpoints reject season on /teams; retry without it.
        r = requests.get(f"{API_BASE}/competitions/{SERIE_A_CODE}/teams",
                         headers=headers, timeout=15)
    r.raise_for_status()
# ...existing code...
    print(f"{len(teams)} teams")
    time.sleep(RATE_LIMIT_DELAY)

    print(f"  Fetching matches for season {season}...", end=' ', flush=True)
# ...existing code...
    r = requests.get(f"{API_BASE}/competitions/{SERIE_A_CODE}/matches",
                     headers=headers, params=params, timeout=15)
    r.raise_for_status()
    matches = r.json().get('matches', [])
    print(f"{len(matches)} matches")

    return teams, matches


def sync_from_api(api_key, season):
    """Syncs a given season using the football-data.org API."""
    conn = sqlite3.connect(DB_PATH)
    try:
        team_map = load_team_map(conn)
        existing = load_existing_matches(conn, season)

        _, api_matches = fetch_season_data(api_key, season)

        inserted = 0
        updated = 0
        skipped = 0

        cur = conn.cursor()

        for m in api_matches:
            home_name = API_TEAM_NAME_MAP.get(m['homeTeam']['name'], m['homeTeam']['name'])
            away_name = API_TEAM_NAME_MAP.get(m['awayTeam']['name'], m['awayTeam']['name'])
            utc_date = m['utcDate']  # e.g. "2025-09-14T18:45:00Z"
            date_str = utc_date[:10]  # "2025-09-14"
            api_status = m['status']  # FINISHED / SCHEDULED / TIMED / etc.

            home_id = ensure_team(conn, team_map, home_name)
            away_id = ensure_team(conn, team_map, away_name)

            # Fuzzy match: same teams, date within ±3 days
            api_date = datetime.strptime(date_str, '%Y-%m-%d')
            existing_entry = None
            for match_id, db_date_str, db_status in existing.get((home_id, away_id), []):
                db_date = datetime.strptime(db_date_str, '%Y-%m-%d')
                if abs((api_date - db_date).days) <= 3:
                    existing_entry = (match_id, db_status)
                    break

            if existing_entry is not None:
                match_id, db_status = existing_entry
                if api_status == 'FINISHED' and db_status != 'completed':
                    score = m.get('score', {})
                    ft = score.get('fullTime', {})
                    ht = score.get('halfTime', {})
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
                db_status = 'completed' if api_status == 'FINISHED' else 'scheduled'
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
                    score = m.get('score', {})
                    ft = score.get('fullTime', {})
                    ht = score.get('halfTime', {})
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
                existing.setdefault((home_id, away_id), []).append((match_id, date_str, db_status))
                inserted += 1

        conn.commit()
        print(f"\nSeason {season}: {inserted} inserted, {updated} updated, {skipped} unchanged.")

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CSV Sync Logic
# ---------------------------------------------------------------------------

def get_csv_url(season):
    """Construct the football-data.co.uk URL for a given season start year."""
    season_end_short = str(season + 1)[-2:]
    season_short = f"{str(season)[-2:]}{season_end_short}"
    return CSV_URL_TEMPLATE.format(season_short=season_short)


def download_csv(url):
    """Download CSV content from a URL, returning a list of decoded lines."""
    print(f"  Downloading latest CSV from {url}...")
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    print(f"  Success: {len(r.content)} bytes downloaded.")
    return r.content.decode('utf-8').splitlines()


def sync_from_csv(season):
    """Syncs a given season using the football-data.co.uk CSV."""
    url = get_csv_url(season)
    try:
        csv_lines = download_csv(url)
    except requests.exceptions.RequestException as e:
        print(f"  Error: Failed to download CSV. {e}")
        return

    conn = sqlite3.connect(DB_PATH)
    try:
        team_map = load_team_map(conn)
        existing = load_existing_matches(conn, season)
        csv_reader = csv.DictReader(csv_lines)

        updated = 0
        skipped = 0
        not_found = 0

        cur = conn.cursor()

        for row in csv_reader:
            hs = parse_int_or_none(row.get('FTHG'))
            aws = parse_int_or_none(row.get('FTAG'))
            if hs is None or aws is None:
                continue  # Skip rows without a final score

            hhs = parse_int_or_none(row.get('HTHG'))
            has = parse_int_or_none(row.get('HTAG'))

            try:
                date = datetime.strptime(row['Date'].strip(), '%d/%m/%Y').date()
            except (ValueError, KeyError):
                continue # Skip rows with invalid dates

            home_name = CSV_TEAM_NAME_MAP.get(row['HomeTeam'].strip(), row['HomeTeam'].strip())
            away_name = CSV_TEAM_NAME_MAP.get(row['AwayTeam'].strip(), row['AwayTeam'].strip())
            home_id = team_map.get(home_name)
            away_id = team_map.get(away_name)

            if not home_id or not away_id:
                not_found += 1
                continue

            # Fuzzy match: same teams, date within ±3 days
            existing_entry = None
            for match_id, db_date_str, db_status in existing.get((home_id, away_id), []):
                db_date = datetime.strptime(db_date_str, '%Y-%m-%d').date()
                if abs((date - db_date).days) <= 3:
                    existing_entry = (match_id, db_status)
                    break

            if existing_entry:
                match_id, db_status = existing_entry
                if db_status != 'completed':
                    cur.execute("""
                        UPDATE soccer_matches
                        SET home_score = ?, away_score = ?,
                            halftime_home_score = ?, halftime_away_score = ?,
                            match_status = 'completed'
                        WHERE match_id = ?
                    """, (hs, aws, hhs, has, match_id))
                    print(f"  Updated  [{match_id}] {home_name} {hs}-{aws} {away_name}")
                    updated += 1
                else:
                    skipped += 1
            else:
                not_found += 1

        conn.commit()
        print(f"\nSeason {season}: {updated} updated, {skipped} unchanged, {not_found} not found in DB.")

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Sync Serie A fixtures and results from football-data.org API or football-data.co.uk CSV."
    )
    parser.add_argument(
        '--source', type=str, choices=['api', 'csv'], default='api',
        help="Data source: 'api' (default) or 'csv'."
    )
    parser.add_argument(
        '--season', type=int, default=None,
        metavar='YYYY',
        help='Season start year to sync (default: current season).'
    )
    parser.add_argument(
        'api_key', nargs='?', default=None,
        help='Your football-data.org API key (required for --source api).'
    )
    args = parser.parse_args()

    if args.source == 'api' and not args.api_key:
        parser.error("--source 'api' requires an api_key.")

    season = args.season or current_season_year()
    print(f"=== Serie A Sync  {datetime.now().strftime('%Y-%m-%d %H:%M')}  season={season}  source={args.source} ===\n")

    if args.source == 'api':
        sync_from_api(args.api_key, season)
    elif args.source == 'csv':
        sync_from_csv(season)

    print("\nDone.")


if __name__ == '__main__':
    main()
