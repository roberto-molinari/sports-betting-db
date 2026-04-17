#!/usr/bin/env python3
"""
Import Serie A match results from football-data.co.uk CSV files into sports_betting.db.

Sources:
  - I1_2324.csv : 2023-24 season (we only import games from Apr 8 2024 onwards)
  - I1.csv      : 2024-25 season (full season)

Also fixes existing DB issue: 2025-26 season matches were stored with season=2024,
corrects them to season=2025.
"""

import sqlite3
import csv
from datetime import datetime

DB_PATH = "sports_betting.db"

# Maps football-data.co.uk short team names -> canonical DB team name
# Canonical names are the ones already used in the 2025-26 season data
TEAM_NAME_MAP = {
    "Atalanta":    "Atalanta BC",
    "Bologna":     "Bologna FC 1909",
    "Cagliari":    "Cagliari Calcio",
    "Como":        "Como 1907",
    "Empoli":      "Empoli FC",
    "Fiorentina":  "ACF Fiorentina",
    "Frosinone":   "Frosinone Calcio",
    "Genoa":       "Genoa CFC",
    "Inter":       "FC Internazionale Milano",
    "Juventus":    "Juventus FC",
    "Lazio":       "SS Lazio",
    "Lecce":       "US Lecce",
    "Milan":       "AC Milan",
    "Monza":       "AC Monza",
    "Napoli":      "SSC Napoli",
    "Parma":       "Parma Calcio 1913",
    "Roma":        "AS Roma",
    "Salernitana": "US Salernitana 1919",
    "Sassuolo":    "US Sassuolo Calcio",
    "Torino":      "Torino FC",
    "Udinese":     "Udinese Calcio",
    "Venezia":     "Venezia FC",
    "Verona":      "Hellas Verona FC",
    "Cremonese":   "US Cremonese",
    "Spezia":      "Spezia Calcio",
    "Salernitana": "US Salernitana 1919",
}

# Teams that need to be created if not present
NEW_TEAMS = [
    "Empoli FC",
    "Frosinone Calcio",
    "AC Monza",
    "US Salernitana 1919",
    "Venezia FC",
    "Spezia Calcio",
]

def get_canonical_name(csv_name):
    return TEAM_NAME_MAP.get(csv_name, csv_name)

def load_csv(filename):
    with open(filename, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        return list(reader), reader.fieldnames

def fix_existing_season(cur):
    """Fix 2025-26 matches that were incorrectly stored as season=2024."""
    cur.execute("""
        UPDATE matches
        SET season = 2025
        WHERE league = 'Serie A'
          AND season = 2024
          AND match_date >= '2025-08-01'
    """)
    count = cur.rowcount
    print(f"  Fixed season: updated {count} existing 2025-26 matches from season=2024 to season=2025")

def ensure_teams(cur):
    """Add any missing Serie A teams."""
    for name in NEW_TEAMS:
        cur.execute("SELECT team_id FROM teams WHERE name = ?", (name,))
        if not cur.fetchone():
            cur.execute("""
                INSERT INTO teams (name, sport, league, country)
                VALUES (?, 'Soccer', 'Serie A', 'Italy')
            """, (name,))
            print(f"  Created team: {name}")

def get_team_id(cur, canonical_name):
    cur.execute("SELECT team_id FROM teams WHERE name = ?", (canonical_name,))
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Team not found in DB: '{canonical_name}'")
    return row[0]

def existing_games_set(cur):
    """Return a set of (date_str, home_team_id, away_team_id) for quick duplicate check."""
    cur.execute("""
        SELECT DATE(match_date), home_team_id, away_team_id
        FROM matches WHERE league = 'Serie A'
    """)
    return set((r[0], r[1], r[2]) for r in cur.fetchall())

def import_rows(cur, rows, season, min_date=None, existing=None):
    inserted = skipped_dup = skipped_date = 0

    for row in rows:
        date_str = row.get('Date', '').strip()
        home_csv = row.get('HomeTeam', '').strip()
        away_csv = row.get('AwayTeam', '').strip()
        fthg = row.get('FTHG', '').strip()
        ftag = row.get('FTAG', '').strip()

        if not date_str or not home_csv or not away_csv:
            continue

        match_date = datetime.strptime(date_str, '%d/%m/%Y')

        if min_date and match_date < min_date:
            skipped_date += 1
            continue

        time_str = row.get('Time', '').strip()
        if time_str:
            try:
                dt = datetime.strptime(f"{date_str} {time_str}", '%d/%m/%Y %H:%M')
            except ValueError:
                dt = match_date
        else:
            dt = match_date

        iso_date = dt.strftime('%Y-%m-%dT%H:%M:%S')
        date_only = match_date.strftime('%Y-%m-%d')

        home_name = get_canonical_name(home_csv)
        away_name = get_canonical_name(away_csv)

        home_id = get_team_id(cur, home_name)
        away_id = get_team_id(cur, away_name)

        # Duplicate check
        if existing is not None and (date_only, home_id, away_id) in existing:
            skipped_dup += 1
            continue

        home_score = int(fthg) if fthg.isdigit() else None
        away_score = int(ftag) if ftag.isdigit() else None

        cur.execute("""
            INSERT INTO matches
                (sport, league, season, home_team_id, away_team_id, match_date,
                 home_score, away_score, match_status)
            VALUES ('Soccer', 'Serie A', ?, ?, ?, ?, ?, ?, 'completed')
        """, (season, home_id, away_id, iso_date, home_score, away_score))

        if existing is not None:
            existing.add((date_only, home_id, away_id))
        inserted += 1

    return inserted, skipped_dup, skipped_date

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    print("=== Serie A CSV Import ===\n")

    print("Step 1: Fix existing 2025-26 season label...")
    fix_existing_season(cur)

    print("\nStep 2: Ensure all teams exist...")
    ensure_teams(cur)

    # Build existing games set after fix so checks are correct
    existing = existing_games_set(cur)
    print(f"  Existing Serie A games in DB: {len(existing)}")

    print("\nStep 3: Import I1_2324.csv (2023-24 season, Apr 8 2024 onwards)...")
    rows_2324, _ = load_csv('I1_2324.csv')
    min_date = datetime(2024, 4, 8)
    ins, dup, skipped = import_rows(cur, rows_2324, season=2023, min_date=min_date, existing=existing)
    print(f"  Inserted: {ins}, Duplicates skipped: {dup}, Before cutoff: {skipped}")

    print("\nStep 4: Import I1.csv (2024-25 season, full season)...")
    rows_2425, _ = load_csv('I1.csv')
    ins, dup, skipped = import_rows(cur, rows_2425, season=2024, existing=existing)
    print(f"  Inserted: {ins}, Duplicates skipped: {dup}")

    conn.commit()

    print("\n=== Verification ===")
    cur.execute("""
        SELECT season, COUNT(*), MIN(DATE(match_date)), MAX(DATE(match_date))
        FROM matches WHERE league='Serie A'
        GROUP BY season ORDER BY season
    """)
    print(f"  {'Season':<8} {'Games':>6}  {'First':<12} {'Last':<12}")
    for r in cur.fetchall():
        print(f"  {r[0]:<8} {r[1]:>6}  {r[2]:<12} {r[3]:<12}")

    cur.execute("SELECT COUNT(*) FROM matches WHERE league='Serie A'")
    total = cur.fetchone()[0]
    print(f"\n  Total Serie A matches in DB: {total}")

    conn.close()
    print("\nDone.")

if __name__ == "__main__":
    main()
