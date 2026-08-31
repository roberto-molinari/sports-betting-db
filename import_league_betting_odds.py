"""
Import a league's betting odds from football-data.co.uk CSV files (historical/
backtest) or The Odds API (live/current-week), into soccer_betting_odds.

Loads the Bet365 1X2 market plus the 2.5-goal total and Asian handicap fields,
matching rows to existing soccer_matches. Generalized 2026-08-10 (multi-league
expansion, renamed from import_serie_a_odds.py) -- every per-league detail
(football-data.co.uk CSV code, The Odds API sport key) now comes from --league via
core/leagues.py's LEAGUES registry instead of being hardcoded to Serie A.

Default file mapping (football-data.co.uk CSV code varies by league, see
core/leagues.py):
  - {code}_2324.csv -> season 2023
  - {code}.csv      -> season 2024
  - {code}_2526.csv -> season 2025 (preferred)
  - {code}2.csv     -> season 2025 (fallback)
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
from core.leagues import LEAGUES
from core.team_name_maps import canonical_team_name


ODDS_API_DEFAULT_BOOK = "Pinnacle"
FIND_MATCH_DATE_TOLERANCE_DAYS = 3



def parse_args():
    parser = argparse.ArgumentParser(description="Import a league's odds from the configured source or from local CSV files.")
    parser.add_argument("--league", default="Serie A", choices=sorted(LEAGUES),
                        help="League name, must be a key in core/leagues.py's LEAGUES registry.")
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download the odds CSV for the given season(s) from football-data.co.uk AND import it "
             "in the same run (not download-only -- use this instead of a local CSV file)."
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
        "--insert-missing-matches",
        dest="insert_missing",
        action="store_true",
        help="Insert missing soccer_matches records from CSV rows that include a result (FTHG/FTAG). "
             "The odds themselves are always inserted regardless of this flag -- this only controls "
             "whether a not-yet-ingested fixture gets created from the CSV row."
    )
    parser.add_argument(
        "--future-only",
        action="store_true",
        help="Import odds only for matches on/after today (useful for next matchday updates)."
    )
    args = parser.parse_args()

    if not args.download and not args.files and not args.future_only:
        parser.print_help()
        parser.exit(2, "\nerror: provide local CSV files, or use --download, or use --future-only "
                        "(live odds from The Odds API).\n")

    if args.download and args.files:
        parser.error("--download cannot be used together with local CSV files.")

    if args.future_only and (not args.season or len(args.season) != 1):
        parser.error("--future-only requires exactly one --season value.")

    if args.download and args.future_only and not args.files:
        parser.error("--download and --future-only can't be combined without local CSV files -- "
                      "they're two different sources (football-data.co.uk vs. The Odds API) and "
                      "--future-only alone would silently take over, ignoring --download. Use "
                      "--future-only on its own for live odds, or --download on its own for a "
                      "full season's historical odds.")

    return args


def season_to_code(season):
    """Convert season start year to football-data.co.uk code (e.g., 2025 -> '2526')."""
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


def load_team_map(conn, league):
    """{name: team_id} for every team, globally -- NOT filtered by league. A team's
    soccer_teams.league is its CURRENT division (see FEATURE-019); a historical-season
    odds import needs to resolve teams that have since been promoted/relegated out of
    `league` entirely (e.g. importing Premier League 2022-23 odds must still resolve
    Leicester City/West Ham/Southampton/Wolves, all since relegated). name is globally
    unique by design (ensure_soccer_team), so an unscoped lookup is safe and correct --
    scoping by current league silently dropped 140/380 matches' odds when first found
    live 2026-08-20 backfilling Premier League 2022-23."""
    cur = conn.cursor()
    cur.execute("SELECT name, team_id FROM soccer_teams")
    return dict(cur.fetchall())


def load_match_index(conn, league, season):
    """Return (home_id, away_id) -> [(match_id, match_date)] for fuzzy date matching."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT match_id, home_team_id, away_team_id, DATE(match_date)
        FROM soccer_matches
        WHERE league = ? AND season = ?
        """,
        (league, season),
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
        if abs((target_date - db_date).days) <= FIND_MATCH_DATE_TOLERANCE_DAYS:
            return match_id
    return None


def upsert_odds(cur, match_id, sportsbook, odds_date, values):
    """Insert, or update-and-log-what-changed if a row for this match/sportsbook
    already exists (conflict-safe write -- see module docstring for the principle
    this follows, same as import_league_matches.py)."""
    cur.execute(
        """SELECT odds_id, home_moneyline, draw_moneyline, away_moneyline,
                  spread_home, spread_away, spread_home_odds, spread_away_odds,
                  over_under, over_odds, under_odds
           FROM soccer_betting_odds WHERE match_id = ? AND sportsbook = ?""",
        (match_id, sportsbook),
    )
    existing = cur.fetchone()

    if existing:
        odds_id = existing[0]
        old_fields = dict(zip(
            ["home_moneyline", "draw_moneyline", "away_moneyline", "spread_home",
             "spread_away", "spread_home_odds", "spread_away_odds", "over_under",
             "over_odds", "under_odds"],
            existing[1:],
        ))
        changed = [(k, old_fields[k], values[k]) for k in old_fields
                   if old_fields[k] != values[k]]
        if not changed:
            return "unchanged"
        for field, old, new in changed:
            print(f"  updated match_id={match_id} sportsbook={sportsbook}: "
                  f"{field} {old!r} -> {new!r}")
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
                odds_id,
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


def insert_match(cur, league, home_id, away_id, match_dt, season, row):
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (league, season, home_id, away_id, match_dt.isoformat(),
         hs, aws, hhs, has, status),
    )
    return cur.lastrowid


def _import_rows(conn, league, reader, season, sportsbook, insert_missing, future_only, source_label):
    team_map = load_team_map(conn, league)
    match_index = load_match_index(conn, league, season)
    cur = conn.cursor()

    inserted = updated = unchanged = no_match = unknown_team = skipped = skipped_past = matches_created = 0

    for row in reader:
        match_dt = parse_match_datetime(row)
        if match_dt is None:
            skipped += 1
            continue

        if future_only and match_dt.date() < datetime.now().date():
            skipped_past += 1
            continue

        home_name = canonical_team_name(league, row.get("HomeTeam"))
        away_name = canonical_team_name(league, row.get("AwayTeam"))
        home_id = team_map.get(home_name)
        away_id = team_map.get(away_name)

        if not home_id or not away_id:
            unknown_team += 1
            print(f"  UNKNOWN TEAM: {league} {row.get('HomeTeam')!r} vs {row.get('AwayTeam')!r} "
                  f"on {match_dt.date()} -- {'HomeTeam' if not home_id else 'AwayTeam'} name "
                  f"{'and AwayTeam name ' if not home_id and not away_id else ''}"
                  f"not in core/team_name_maps.py and not an exact soccer_teams match. "
                  f"This match's odds were NOT imported -- add the missing mapping and re-run.")
            continue

        match_id = find_match_id(match_index, home_id, away_id, match_dt.date())
        if match_id is None:
            if insert_missing:
                match_id = insert_match(cur, league, home_id, away_id, match_dt, season, row)
                match_index.setdefault((home_id, away_id), []).append(
                    (match_id, match_dt.strftime("%Y-%m-%d"))
                )
                matches_created += 1
            else:
                no_match += 1
                print(f"  NO MATCH: {league} {home_name} vs {away_name} on {match_dt.date()} -- "
                      f"both teams resolved but no soccer_matches row found within "
                      f"{FIND_MATCH_DATE_TOLERANCE_DAYS} days. This match's odds were NOT "
                      f"imported -- either the fixture hasn't been ingested yet "
                      f"(import_league_matches.py), or the two sources disagree on which "
                      f"team is home/away for this fixture (see BUGS.md's duplicate-fixture "
                      f"entry, 2026-08-23) -- verify before assuming this is a simple gap.")
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
            "notes": f"Imported from {source_label}",
        }

        action = upsert_odds(cur, match_id, sportsbook, match_dt.isoformat(), values)
        if action == "inserted":
            inserted += 1
        elif action == "updated":
            updated += 1
        else:
            unchanged += 1

    conn.commit()
    return {
        "inserted": inserted, "updated": updated, "unchanged": unchanged,
        "no_match": no_match, "unknown_team": unknown_team, "skipped": skipped,
        "skipped_past": skipped_past, "matches_created": matches_created,
    }


def import_csv(conn, league, csv_path: Path, season: int, sportsbook: str = "Bet365",
               insert_missing: bool = False, future_only: bool = False) -> dict:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return _import_rows(conn, league, reader, season, sportsbook, insert_missing,
                            future_only, csv_path.name)


def import_csv_text(conn, league, csv_text, season, sportsbook="Bet365", insert_missing=False,
                    future_only=False):
    reader = csv.DictReader(io.StringIO(csv_text))
    return _import_rows(conn, league, reader, season, sportsbook, insert_missing,
                        future_only, f"football-data.co.uk (season {season})")


def download_league_csv(league, season_code):
    code = LEAGUES[league]["footballdatacouk_code"]
    if code is None:
        raise SystemExit(f"'{league}' has no footballdatacouk_code configured in core/leagues.py.")
    url = f"https://www.football-data.co.uk/mmz4281/{season_code}/{code}.csv"
    print(f"Downloading {league} odds for season {season_code}...")
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


def fetch_odds_api_events(league):
    sport_key = LEAGUES[league]["odds_api_sport_key"]
    if sport_key is None:
        raise SystemExit(f"'{league}' has no odds_api_sport_key configured in core/leagues.py.")
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
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    response = requests.get(url, params=params, timeout=30)
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


def import_odds_api(conn, league, season, preferred_sportsbook):
    team_map = load_team_map(conn, league)
    match_index = load_match_index(conn, league, season)
    cur = conn.cursor()

    inserted = updated = unchanged = no_match = unknown_team = skipped = bookmaker_fallbacks = 0
    effective_sportsbook = preferred_sportsbook
    if preferred_sportsbook == "Bet365":
        effective_sportsbook = ODDS_API_DEFAULT_BOOK

    events = fetch_odds_api_events(league)
    today = datetime.now().date()

    for event in events:
        match_dt = parse_iso_datetime(event.get("commence_time"))
        if match_dt is None:
            skipped += 1
            continue

        if match_dt.date() < today:
            skipped += 1
            continue

        home_name = canonical_team_name(league, event.get("home_team"))
        away_name = canonical_team_name(league, event.get("away_team"))
        home_id = team_map.get(home_name)
        away_id = team_map.get(away_name)

        if not home_id or not away_id:
            unknown_team += 1
            print(f"  UNKNOWN TEAM: {league} {event.get('home_team')!r} vs {event.get('away_team')!r} "
                  f"on {match_dt.date()} -- The Odds API's team name isn't in "
                  f"core/team_name_maps.py and doesn't exactly match soccer_teams. This "
                  f"match's odds were NOT imported -- add the missing mapping and re-run.")
            continue

        match_id = find_match_id(match_index, home_id, away_id, match_dt.date())
        if match_id is None:
            no_match += 1
            print(f"  NO MATCH: {league} {home_name} vs {away_name} on {match_dt.date()} -- "
                  f"both teams resolved but no soccer_matches row found within "
                  f"{FIND_MATCH_DATE_TOLERANCE_DAYS} days. This match's odds were NOT "
                  f"imported -- either the fixture hasn't been ingested yet "
                  f"(import_league_matches.py), or the two sources disagree on which team "
                  f"is home/away for this fixture (see BUGS.md's duplicate-fixture entry, "
                  f"2026-08-23) -- verify before assuming this is a simple gap.")
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
                name = canonical_team_name(league, outcome.get("name"))
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
                name = canonical_team_name(league, outcome.get("name"))
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
        elif action == "updated":
            updated += 1
        else:
            unchanged += 1

    conn.commit()
    return {
        "inserted": inserted, "updated": updated, "unchanged": unchanged,
        "no_match": no_match, "unknown_team": unknown_team, "skipped": skipped,
        "bookmaker_fallbacks": bookmaker_fallbacks,
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
            stats = import_odds_api(conn, args.league, args.season[0], args.sportsbook)
            print(
                f"The Odds API ({args.league}, season {args.season[0]}): "
                f"{stats['inserted']} inserted, {stats['updated']} updated, "
                f"{stats['unchanged']} unchanged, "
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
                csv_text = download_league_csv(args.league, season_code)
                if not csv_text:
                    print(f"Failed to download odds for season {season}")
                    continue
                stats = import_csv_text(
                    conn,
                    args.league,
                    csv_text,
                    season,
                    args.sportsbook,
                    insert_missing=args.insert_missing,
                    future_only=args.future_only,
                )
                print(
                    f"football-data.co.uk ({args.league}, season {season}): "
                    f"{stats['inserted']} inserted, {stats['updated']} updated, "
                    f"{stats['unchanged']} unchanged, "
                    f"{stats['no_match']} unmatched, {stats['unknown_team']} unknown-team, "
                    f"{stats['skipped']} skipped"
                    + (f", {stats['skipped_past']} past rows skipped" if stats.get('skipped_past') else "")
                    + (f", {stats['matches_created']} match records created" if stats['matches_created'] else "")
                )
        finally:
            conn.close()
        return

    # Local file import
    imports = [(base_dir / file_name, args.season[0] if args.season else None) for file_name in args.files]

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
                args.league,
                csv_path,
                season,
                args.sportsbook,
                insert_missing=args.insert_missing,
                future_only=args.future_only,
            )
            print(
                f"{csv_path.name} ({args.league}, season {season}): "
                f"{stats['inserted']} inserted, {stats['updated']} updated, "
                f"{stats['unchanged']} unchanged, "
                f"{stats['no_match']} unmatched, {stats['unknown_team']} unknown-team, "
                f"{stats['skipped']} skipped"
                + (f", {stats['skipped_past']} past rows skipped" if stats.get('skipped_past') else "")
                + (f", {stats['matches_created']} match records created" if stats['matches_created'] else "")
            )
        finally:
            conn.close()


if __name__ == "__main__":
    main()
