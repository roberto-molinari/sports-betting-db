"""
Import Serie A betting odds from football-data.co.uk CSV files.

Loads the Bet365 1X2 market plus the 2.5-goal total and Asian handicap
fields into soccer_betting_odds, matching rows to existing soccer_matches.

Default file mapping:
  - I1_2324.csv -> season 2023
  - I1.csv      -> season 2024
  - I1_2526.csv -> season 2025 (preferred)
  - I2.csv      -> season 2025 (fallback)
"""

import argparse
import csv
import sqlite3
from datetime import datetime
from pathlib import Path

from sports_db import DATABASE_PATH, ensure_soccer_betting_odds_schema
from update_serie_a_results import CSV_TEAM_NAME_MAP


DEFAULT_IMPORTS = [
    ("I1_2324.csv", 2023),
    ("I1.csv", 2024),
    ("I1_2526.csv", 2025),
]

FALLBACK_IMPORTS = [
    ("I2.csv", 2025),
]


def parse_args():
    parser = argparse.ArgumentParser(description="Import Serie A odds from local CSV files.")
    parser.add_argument(
        "files",
        nargs="*",
        help="Optional CSV files to import. If omitted, uses the default local season files.",
    )
    parser.add_argument(
        "--season",
        type=int,
        help="Force all provided files to match against this DB season.",
    )
    parser.add_argument(
        "--sportsbook",
        default="Bet365",
        help="Sportsbook name to store in soccer_betting_odds.",
    )
    parser.add_argument(
        "--insert-missing",
        action="store_true",
        help="Insert missing soccer_matches records from CSV rows that include a result (FTHG/FTAG).",
    )
    return parser.parse_args()


def decimal_to_american(decimal_odds):
    """Convert decimal odds to American moneyline, preserving decimals."""
    if decimal_odds is None or decimal_odds <= 1.0:
        return None
    if decimal_odds >= 2.0:
        return (decimal_odds - 1.0) * 100.0
    return -100.0 / (decimal_odds - 1.0)


def parse_float_or_none(value):
    text = (value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_match_datetime(row):
    date_text = (row.get("Date") or "").strip()
    if not date_text:
        return None

    for fmt in ("%d/%m/%Y", "%d/%m/%y"):
        try:
            match_date = datetime.strptime(date_text, fmt)
            break
        except ValueError:
            match_date = None
    if match_date is None:
        return None

    time_text = (row.get("Time") or "").strip()
    if not time_text:
        return match_date

    for fmt in ("%H:%M", "%H:%M:%S"):
        try:
            parsed_time = datetime.strptime(time_text, fmt).time()
            return datetime.combine(match_date.date(), parsed_time)
        except ValueError:
            continue
    return match_date


def canonical_team_name(csv_name):
    csv_name = (csv_name or "").strip()
    return CSV_TEAM_NAME_MAP.get(csv_name, csv_name)


def load_team_map(conn):
    cur = conn.cursor()
    cur.execute("SELECT name, team_id FROM soccer_teams WHERE league = 'Serie A'")
    return dict(cur.fetchall())


def load_match_index(conn, season):
    """Return (home_id, away_id) -> [(match_id, match_date)] for fuzzy date matching."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT match_id, home_team_id, away_team_id, DATE(match_date)
        FROM soccer_matches
        WHERE league = 'Serie A' AND season = ?
        """,
        (season,),
    )

    index = {}
    for match_id, home_id, away_id, match_date in cur.fetchall():
        index.setdefault((home_id, away_id), []).append((match_id, match_date))
    return index


def find_match_id(match_index, home_id, away_id, target_date):
    candidates = match_index.get((home_id, away_id), [])
    for match_id, db_date_text in candidates:
        db_date = datetime.strptime(db_date_text, "%Y-%m-%d").date()
        if abs((target_date - db_date).days) <= 3:
            return match_id
    return None


def upsert_odds(cur, match_id, sportsbook, odds_date, values):
    cur.execute(
        "SELECT odds_id FROM soccer_betting_odds WHERE match_id = ? AND sportsbook = ?",
        (match_id, sportsbook),
    )
    existing = cur.fetchone()

    if existing:
        cur.execute(
            """
            UPDATE soccer_betting_odds
            SET odds_date = ?,
                home_moneyline = ?,
                draw_moneyline = ?,
                away_moneyline = ?,
                spread_home = ?,
                spread_away = ?,
                spread_home_odds = ?,
                spread_away_odds = ?,
                over_under = ?,
                over_odds = ?,
                under_odds = ?,
                notes = ?
            WHERE odds_id = ?
            """,
            (
                odds_date,
                values["home_moneyline"],
                values["draw_moneyline"],
                values["away_moneyline"],
                values["spread_home"],
                values["spread_away"],
                values["spread_home_odds"],
                values["spread_away_odds"],
                values["over_under"],
                values["over_odds"],
                values["under_odds"],
                values["notes"],
                existing[0],
            ),
        )
        return "updated"

    cur.execute(
        """
        INSERT INTO soccer_betting_odds (
            match_id, sportsbook, odds_date,
            home_moneyline, draw_moneyline, away_moneyline,
            spread_home, spread_away, spread_home_odds, spread_away_odds,
            over_under, over_odds, under_odds, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            match_id,
            sportsbook,
            odds_date,
            values["home_moneyline"],
            values["draw_moneyline"],
            values["away_moneyline"],
            values["spread_home"],
            values["spread_away"],
            values["spread_home_odds"],
            values["spread_away_odds"],
            values["over_under"],
            values["over_odds"],
            values["under_odds"],
            values["notes"],
        ),
    )
    return "inserted"


def insert_match(cur, home_id, away_id, match_dt, season, row):
    """Insert a soccer_matches record from a CSV row. Returns the new match_id."""
    from update_serie_a_results import parse_int_or_none as _parse_int
    hs  = _parse_int(row.get('FTHG'))
    aws = _parse_int(row.get('FTAG'))
    hhs = _parse_int(row.get('HTHG'))
    has = _parse_int(row.get('HTAG'))
    status = 'completed' if hs is not None and aws is not None else 'scheduled'
    cur.execute(
        """
        INSERT INTO soccer_matches
            (league, season, home_team_id, away_team_id, match_date,
             home_score, away_score, halftime_home_score, halftime_away_score, match_status)
        VALUES ('Serie A', ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (season, home_id, away_id, match_dt.isoformat(),
         hs, aws, hhs, has, status),
    )
    return cur.lastrowid


def import_csv(conn, csv_path: Path, season: int, sportsbook: str = "Bet365",
               insert_missing: bool = False) -> dict:
    team_map = load_team_map(conn)
    match_index = load_match_index(conn, season)
    cur = conn.cursor()

    inserted = 0
    updated = 0
    no_match = 0
    unknown_team = 0
    skipped = 0
    matches_created = 0

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            match_dt = parse_match_datetime(row)
            if match_dt is None:
                skipped += 1
                continue

            home_name = canonical_team_name(row.get("HomeTeam"))
            away_name = canonical_team_name(row.get("AwayTeam"))
            home_id = team_map.get(home_name)
            away_id = team_map.get(away_name)

            if not home_id or not away_id:
                unknown_team += 1
                continue

            match_id = find_match_id(match_index, home_id, away_id, match_dt.date())
            if match_id is None:
                if insert_missing:
                    match_id = insert_match(cur, home_id, away_id, match_dt, season, row)
                    # Keep index up to date so duplicate rows don't create duplicates
                    match_index.setdefault((home_id, away_id), []).append(
                        (match_id, match_dt.strftime("%Y-%m-%d"))
                    )
                    matches_created += 1
                else:
                    no_match += 1
                    continue

            home_decimal = parse_float_or_none(row.get("B365H"))
            draw_decimal = parse_float_or_none(row.get("B365D"))
            away_decimal = parse_float_or_none(row.get("B365A"))

            ah_line = parse_float_or_none(row.get("AHh"))
            over_odds = parse_float_or_none(row.get("B365>2.5"))
            under_odds = parse_float_or_none(row.get("B365<2.5"))
            spread_home_odds = parse_float_or_none(row.get("B365AHH"))
            spread_away_odds = parse_float_or_none(row.get("B365AHA"))

            values = {
                "home_moneyline": decimal_to_american(home_decimal),
                "draw_moneyline": decimal_to_american(draw_decimal),
                "away_moneyline": decimal_to_american(away_decimal),
                "spread_home": ah_line,
                "spread_away": -ah_line if ah_line is not None else None,
                "spread_home_odds": decimal_to_american(spread_home_odds),
                "spread_away_odds": decimal_to_american(spread_away_odds),
                "over_under": 2.5 if over_odds is not None or under_odds is not None else None,
                "over_odds": decimal_to_american(over_odds),
                "under_odds": decimal_to_american(under_odds),
                "notes": f"Imported from {csv_path.name}",
            }

            action = upsert_odds(cur, match_id, sportsbook, match_dt.isoformat(), values)
            if action == "inserted":
                inserted += 1
            else:
                updated += 1

    conn.commit()
    return {
        "inserted": inserted,
        "updated": updated,
        "no_match": no_match,
        "unknown_team": unknown_team,
        "skipped": skipped,
        "matches_created": matches_created,
    }


def resolve_default_files(base_dir):
    resolved = []
    for file_name, season in DEFAULT_IMPORTS:
        path = base_dir / file_name
        if path.exists():
            resolved.append((path, season))
    for file_name, season in FALLBACK_IMPORTS:
        if any(existing_season == season for _, existing_season in resolved):
            continue
        path = base_dir / file_name
        if path.exists():
            resolved.append((path, season))
    return resolved


def main():
    args = parse_args()
    base_dir = Path(__file__).parent

    if args.files:
        imports = [(base_dir / file_name, args.season) for file_name in args.files]
    else:
        imports = resolve_default_files(base_dir)

    if not imports:
        raise SystemExit("No CSV files found to import.")

    if any(season is None for _, season in imports):
        raise SystemExit("--season is required when importing custom file paths.")

    conn = sqlite3.connect(DATABASE_PATH)
    try:
        ensure_soccer_betting_odds_schema(conn)
        for csv_path, season in imports:
            stats = import_csv(conn, csv_path, season, args.sportsbook,
                               insert_missing=args.insert_missing)
            print(
                f"{csv_path.name} (season {season}): "
                f"{stats['inserted']} inserted, {stats['updated']} updated, "
                f"{stats['no_match']} unmatched, {stats['unknown_team']} unknown-team, "
                f"{stats['skipped']} skipped"
                + (f", {stats['matches_created']} match records created" if stats['matches_created'] else "")
            )
    finally:
        conn.close()


if __name__ == "__main__":
    main()