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
import io
import os
import sqlite3
from datetime import datetime
from pathlib import Path
import requests

from core.sports_db import DATABASE_PATH, ensure_soccer_betting_odds_schema
from update_serie_a_results import CSV_TEAM_NAME_MAP


ODDS_API_URL = "https://api.the-odds-api.com/v4/sports/soccer_italy_serie_a/odds"
ODDS_API_DEFAULT_BOOK = "Pinnacle"



def parse_args():
    parser = argparse.ArgumentParser(description="Import Serie A odds from the configured source or from local CSV files.")
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download the latest Serie A odds CSV for the given season(s) from football-data.co.uk."
    )
    parser.add_argument(
        "--season",
        type=int,
        nargs="+",
        help="Season(s) to import (e.g., 2024 for 2024-25). If omitted, uses local CSV files."
    )
    parser.add_argument(
        "files",
        nargs="*",
        help="Optional local CSV files to import. Ignored if --download is used."
    )
    parser.add_argument(
        "--sportsbook",
        default="Bet365",
        help="Preferred sportsbook to store in soccer_betting_odds."
    )
    parser.add_argument(
        "--insert-missing",
        action="store_true",
        help="Insert missing soccer_matches records from CSV rows that include a result (FTHG/FTAG)."
    )
    parser.add_argument(
        "--future-only",
        action="store_true",
        help="Import odds only for matches on/after today (useful for next matchday updates)."
    )
    return parser.parse_args()


def season_to_code(season):
    """Convert season start year to football-data code (e.g., 2025 -> '2526')."""
    if season < 1900 or season > 3000:
        return None
    return f"{season % 100:02d}{(season + 1) % 100:02d}"


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
    db_override_map = {
        'Atalanta BC': 'Atalanta',
        'Bologna': 'Bologna',
        'Fiorentina': 'Fiorentina',
        'Inter': 'Inter',
        'Inter Milan': 'Inter',
        'Hellas Verona': 'Hellas Verona',
    }
    if csv_name in db_override_map:
        return db_override_map[csv_name]

    # Extend with legacy normalization for robustness
    legacy_map = {
        'Milan': 'AC Milan',
        'Inter': 'FC Internazionale Milano',
        'Internazionale': 'FC Internazionale Milano',
        'Inter Milan': 'FC Internazionale Milano',
        'Juventus': 'Juventus FC',
        'Roma': 'AS Roma',
        'Napoli': 'SSC Napoli',
        'Lazio': 'SS Lazio',
        'Atalanta': 'Atalanta BC',
        'Fiorentina': 'ACF Fiorentina',
        'Bologna': 'Bologna FC 1909',
        'Torino': 'Torino FC',
        'Genoa': 'Genoa CFC',
        'Cagliari': 'Cagliari Calcio',
        'Udinese': 'Udinese Calcio',
        'Verona': 'Hellas Verona FC',
        'Hellas Verona': 'Hellas Verona FC',
        'Sassuolo': 'US Sassuolo Calcio',
        'Lecce': 'US Lecce',
        'Parma': 'Parma Calcio 1913',
        'Como': 'Como 1907',
        'Empoli': 'Empoli FC',
        'Monza': 'AC Monza',
        'Salernitana': 'US Salernitana 1919',
        'Spezia': 'Spezia Calcio',
        'Cremonese': 'US Cremonese',
        'Sampdoria': 'UC Sampdorla',
        'Venezia': 'Venezia FC',
        'Pisa': 'AC Pisa 1909',
    }
    return CSV_TEAM_NAME_MAP.get(csv_name, legacy_map.get(csv_name, csv_name))


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


def parse_iso_datetime(value):
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


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
               insert_missing: bool = False, future_only: bool = False) -> dict:
    team_map = load_team_map(conn)
    match_index = load_match_index(conn, season)
    cur = conn.cursor()

    inserted = 0
    updated = 0
    no_match = 0
    unknown_team = 0
    skipped = 0
    skipped_past = 0
    matches_created = 0

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            match_dt = parse_match_datetime(row)
            if match_dt is None:
                skipped += 1
                continue

            if future_only and match_dt.date() < datetime.now().date():
                skipped_past += 1
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
        "skipped_past": skipped_past,
        "matches_created": matches_created,
    }



def download_serie_a_csv(season_code):
    url = f"https://www.football-data.co.uk/mmz4281/{season_code}/I1.csv"
    print(f"Downloading Serie A odds for season {season_code}...")
    print(f"URL: {url}")
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        payload = response.text

        # Fail fast if the endpoint returns an HTML page instead of CSV data.
        sample = payload[:200].lstrip().lower()
        if sample.startswith("<html") or "<title>football betting" in sample:
            print("Error downloading: response was HTML, not CSV. Please retry later.")
            return None

        first_line = payload.splitlines()[0] if payload.splitlines() else ""
        if "Date" not in first_line or "HomeTeam" not in first_line or "AwayTeam" not in first_line:
            print("Error downloading: CSV header not recognized.")
            return None

        return payload
    except requests.exceptions.RequestException as e:
        print(f"Error downloading: {e}")
        return None


def fetch_odds_api_events():
    api_key = os.getenv("THE_ODDS_API_KEY")
    if not api_key:
        raise SystemExit("THE_ODDS_API_KEY is not set.")

    params = {
        "apiKey": api_key,
        "regions": "eu",
        "markets": "h2h,spreads,totals",
        "oddsFormat": "american",
        "dateFormat": "iso",
    }
    response = requests.get(ODDS_API_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def choose_bookmaker(bookmakers, preferred_sportsbook):
    if not bookmakers:
        return None

    preferred_lower = (preferred_sportsbook or "").strip().lower()
    for bookmaker in bookmakers:
        if bookmaker.get("title", "").strip().lower() == preferred_lower:
            return bookmaker

    if preferred_lower == "bet365":
        for bookmaker in bookmakers:
            if bookmaker.get("title") == ODDS_API_DEFAULT_BOOK:
                return bookmaker

    return bookmakers[0]


def find_market(bookmaker, market_key):
    for market in bookmaker.get("markets", []):
        if market.get("key") == market_key:
            return market
    return None


def import_odds_api(conn, season, preferred_sportsbook):
    team_map = load_team_map(conn)
    match_index = load_match_index(conn, season)
    cur = conn.cursor()

    inserted = 0
    updated = 0
    no_match = 0
    unknown_team = 0
    skipped = 0
    bookmaker_fallbacks = 0
    effective_sportsbook = preferred_sportsbook
    if preferred_sportsbook == "Bet365":
        effective_sportsbook = ODDS_API_DEFAULT_BOOK

    events = fetch_odds_api_events()
    today = datetime.now().date()

    for event in events:
        match_dt = parse_iso_datetime(event.get("commence_time"))
        if match_dt is None:
            skipped += 1
            continue

        if match_dt.date() < today:
            skipped += 1
            continue

        home_name = canonical_team_name(event.get("home_team"))
        away_name = canonical_team_name(event.get("away_team"))
        home_id = team_map.get(home_name)
        away_id = team_map.get(away_name)

        if not home_id or not away_id:
            unknown_team += 1
            continue

        match_id = find_match_id(match_index, home_id, away_id, match_dt.date())
        if match_id is None:
            no_match += 1
            continue

        bookmaker = choose_bookmaker(event.get("bookmakers", []), effective_sportsbook)
        if bookmaker is None:
            skipped += 1
            continue

        if bookmaker.get("title") != effective_sportsbook:
            bookmaker_fallbacks += 1

        h2h_market = find_market(bookmaker, "h2h")
        totals_market = find_market(bookmaker, "totals")
        spreads_market = find_market(bookmaker, "spreads")

        home_moneyline = None
        draw_moneyline = None
        away_moneyline = None
        if h2h_market:
            for outcome in h2h_market.get("outcomes", []):
                name = canonical_team_name(outcome.get("name"))
                price = outcome.get("price")
                if name == home_name:
                    home_moneyline = price
                elif name == away_name:
                    away_moneyline = price
                elif (outcome.get("name") or "").strip().lower() == "draw":
                    draw_moneyline = price

        over_under = None
        over_odds = None
        under_odds = None
        if totals_market:
            for outcome in totals_market.get("outcomes", []):
                point = outcome.get("point")
                if point is not None:
                    over_under = point
                name = (outcome.get("name") or "").strip().lower()
                if name == "over":
                    over_odds = outcome.get("price")
                elif name == "under":
                    under_odds = outcome.get("price")

        spread_home = None
        spread_away = None
        spread_home_odds = None
        spread_away_odds = None
        if spreads_market:
            for outcome in spreads_market.get("outcomes", []):
                name = canonical_team_name(outcome.get("name"))
                point = outcome.get("point")
                price = outcome.get("price")
                if name == home_name:
                    spread_home = point
                    spread_home_odds = price
                elif name == away_name:
                    spread_away = point
                    spread_away_odds = price

        values = {
            "home_moneyline": home_moneyline,
            "draw_moneyline": draw_moneyline,
            "away_moneyline": away_moneyline,
            "spread_home": spread_home,
            "spread_away": spread_away,
            "spread_home_odds": spread_home_odds,
            "spread_away_odds": spread_away_odds,
            "over_under": over_under,
            "over_odds": over_odds,
            "under_odds": under_odds,
            "notes": "Imported from The Odds API",
        }

        action = upsert_odds(cur, match_id, bookmaker.get("title"), match_dt.isoformat(), values)
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
        "bookmaker_fallbacks": bookmaker_fallbacks,
    }

def import_csv_text(conn, csv_text, season, sportsbook="Bet365", insert_missing=False,
                    future_only=False):
    # Use StringIO to treat text as file
    csv_file = io.StringIO(csv_text)
    # Use the same import_csv logic, but with a file-like object
    team_map = load_team_map(conn)
    match_index = load_match_index(conn, season)
    cur = conn.cursor()

    inserted = 0
    updated = 0
    no_match = 0
    unknown_team = 0
    skipped = 0
    skipped_past = 0
    matches_created = 0

    reader = csv.DictReader(csv_file)
    for row in reader:
        match_dt = parse_match_datetime(row)
        if match_dt is None:
            skipped += 1
            continue

        if future_only and match_dt.date() < datetime.now().date():
            skipped_past += 1
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
            "notes": f"Imported from football-data.co.uk (season {season})",
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
        "skipped_past": skipped_past,
        "matches_created": matches_created,
    }


def main():
    args = parse_args()
    base_dir = Path(__file__).parent

    if args.future_only and not args.files:
        if not args.season or len(args.season) != 1:
            raise SystemExit("--future-only requires exactly one --season value.")

        conn = sqlite3.connect(DATABASE_PATH)
        try:
            ensure_soccer_betting_odds_schema(conn)
            stats = import_odds_api(conn, args.season[0], args.sportsbook)
            print(
                f"The Odds API (season {args.season[0]}): "
                f"{stats['inserted']} inserted, {stats['updated']} updated, "
                f"{stats['no_match']} unmatched, {stats['unknown_team']} unknown-team, "
                f"{stats['skipped']} skipped"
                + (f", {stats['bookmaker_fallbacks']} bookmaker fallbacks" if stats.get('bookmaker_fallbacks') else "")
            )
        finally:
            conn.close()
        return

    if args.download:
        if not args.season:
            print("You must specify --season (e.g., --season 2024) when using --download.")
            return
        conn = sqlite3.connect(DATABASE_PATH)
        try:
            ensure_soccer_betting_odds_schema(conn)
            for season in args.season:
                season_code = season_to_code(season)
                if not season_code:
                    print(f"Invalid season value: {season}")
                    continue
                csv_text = download_serie_a_csv(season_code)
                if not csv_text:
                    print(f"Failed to download odds for season {season}")
                    continue
                stats = import_csv_text(
                    conn,
                    csv_text,
                    season,
                    args.sportsbook,
                    insert_missing=args.insert_missing,
                    future_only=args.future_only,
                )
                print(
                    f"football-data.co.uk (season {season}): "
                    f"{stats['inserted']} inserted, {stats['updated']} updated, "
                    f"{stats['no_match']} unmatched, {stats['unknown_team']} unknown-team, "
                    f"{stats['skipped']} skipped"
                    + (f", {stats['skipped_past']} past rows skipped" if stats.get('skipped_past') else "")
                    + (f", {stats['matches_created']} match records created" if stats['matches_created'] else "")
                )
        finally:
            conn.close()
        return

    # Local file import (default)
    if args.files:
        imports = [(base_dir / file_name, args.season[0] if args.season else None) for file_name in args.files]
    else:
        print("No files specified and --download not used. Exiting.")
        return

    for csv_path, season in imports:
        if not season:
            print(f"Season must be specified for file {csv_path}. Use --season <year>.")
            continue
        if not csv_path.exists():
            print(f"File not found: {csv_path}")
            continue
        conn = sqlite3.connect(DATABASE_PATH)
        try:
            ensure_soccer_betting_odds_schema(conn)
            stats = import_csv(
                conn,
                csv_path,
                season,
                args.sportsbook,
                insert_missing=args.insert_missing,
                future_only=args.future_only,
            )
            print(
                f"{csv_path.name} (season {season}): "
                f"{stats['inserted']} inserted, {stats['updated']} updated, "
                f"{stats['no_match']} unmatched, {stats['unknown_team']} unknown-team, "
                f"{stats['skipped']} skipped"
                + (f", {stats['skipped_past']} past rows skipped" if stats.get('skipped_past') else "")
                + (f", {stats['matches_created']} match records created" if stats['matches_created'] else "")
            )
        finally:
            conn.close()


if __name__ == "__main__":
    main()