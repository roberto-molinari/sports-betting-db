"""
Import NHL betting odds into nhl_betting_odds.

Sources:
  --future-only (no file)   Fetches upcoming game odds from The Odds API.
                            Reads THE_ODDS_API_KEY from environment.
  <file> [--season YYYY]    Imports from a local CSV (e.g. the Kaggle export).

No --download mode exists for NHL because there is no free recurring source
for historical NHL odds comparable to football-data.co.uk.
"""

import argparse
import csv
import os
import sqlite3
from datetime import datetime
from pathlib import Path

import requests

from sports_db import DATABASE_PATH

ODDS_API_URL = "https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds"
ODDS_API_DEFAULT_BOOK = "DraftKings"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Import NHL betting odds from The Odds API or a local CSV."
    )
    parser.add_argument(
        "files",
        nargs="*",
        help="Local CSV file(s) to import. If omitted with --future-only, uses The Odds API.",
    )
    parser.add_argument(
        "--season",
        type=int,
        help="Season start year (e.g. 2025 for 2025-26). Required for local CSV imports.",
    )
    parser.add_argument(
        "--sportsbook",
        default=ODDS_API_DEFAULT_BOOK,
        help=f"Preferred sportsbook name (default: {ODDS_API_DEFAULT_BOOK}).",
    )
    parser.add_argument(
        "--future-only",
        action="store_true",
        help=(
            "Import odds only for games on/after today. "
            "Without a file argument, fetches live from The Odds API."
        ),
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def load_team_map(conn):
    cur = conn.cursor()
    cur.execute("SELECT name, team_id FROM nhl_teams")
    return dict(cur.fetchall())


def load_match_index(conn, season=None):
    """Return (home_id, away_id) -> [(match_id, date_str)] for fuzzy matching."""
    cur = conn.cursor()
    if season:
        cur.execute(
            "SELECT match_id, home_team_id, away_team_id, DATE(match_date) FROM nhl_matches WHERE season = ?",
            (season,),
        )
    else:
        cur.execute(
            "SELECT match_id, home_team_id, away_team_id, DATE(match_date) FROM nhl_matches"
        )
    index = {}
    for match_id, home_id, away_id, date_str in cur.fetchall():
        index.setdefault((home_id, away_id), []).append((match_id, date_str))
    return index


def find_match_id(match_index, home_id, away_id, target_date, tolerance_days=3):
    for match_id, db_date_str in match_index.get((home_id, away_id), []):
        db_date = datetime.strptime(db_date_str, "%Y-%m-%d").date()
        if abs((target_date - db_date).days) <= tolerance_days:
            return match_id
    return None


def upsert_odds(cur, match_id, sportsbook, odds_date, values):
    cur.execute(
        "SELECT odds_id FROM nhl_betting_odds WHERE match_id = ? AND sportsbook = ?",
        (match_id, sportsbook),
    )
    existing = cur.fetchone()
    if existing:
        cur.execute(
            """
            UPDATE nhl_betting_odds
            SET odds_date = ?,
                home_moneyline = ?, away_moneyline = ?,
                spread_home = ?, spread_away = ?,
                spread_home_odds = ?, spread_away_odds = ?,
                over_under = ?, over_odds = ?, under_odds = ?,
                notes = ?
            WHERE odds_id = ?
            """,
            (
                odds_date,
                values["home_moneyline"], values["away_moneyline"],
                values["spread_home"], values["spread_away"],
                values["spread_home_odds"], values["spread_away_odds"],
                values["over_under"], values["over_odds"], values["under_odds"],
                values["notes"],
                existing[0],
            ),
        )
        return "updated"

    cur.execute(
        """
        INSERT INTO nhl_betting_odds (
            match_id, sportsbook, odds_date,
            home_moneyline, away_moneyline,
            spread_home, spread_away,
            spread_home_odds, spread_away_odds,
            over_under, over_odds, under_odds,
            notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            match_id, sportsbook, odds_date,
            values["home_moneyline"], values["away_moneyline"],
            values["spread_home"], values["spread_away"],
            values["spread_home_odds"], values["spread_away_odds"],
            values["over_under"], values["over_odds"], values["under_odds"],
            values["notes"],
        ),
    )
    return "inserted"


# ---------------------------------------------------------------------------
# Odds API path
# ---------------------------------------------------------------------------

def fetch_odds_api_events():
    api_key = os.getenv("THE_ODDS_API_KEY")
    if not api_key:
        raise SystemExit("THE_ODDS_API_KEY is not set.")
    params = {
        "apiKey": api_key,
        "regions": "us",
        "markets": "h2h,spreads,totals",
        "oddsFormat": "american",
        "dateFormat": "iso",
    }
    resp = requests.get(ODDS_API_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def choose_bookmaker(bookmakers, preferred):
    if not bookmakers:
        return None
    preferred_lower = (preferred or "").lower()
    for b in bookmakers:
        if b.get("title", "").lower() == preferred_lower:
            return b
    return bookmakers[0]


def find_market(bookmaker, key):
    for m in bookmaker.get("markets", []):
        if m.get("key") == key:
            return m
    return None


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


def import_odds_api(conn, preferred_sportsbook, season=None):
    team_map = load_team_map(conn)
    match_index = load_match_index(conn, season)
    cur = conn.cursor()

    inserted = updated = no_match = unknown_team = skipped = fallbacks = 0
    today = datetime.now().date()
    events = fetch_odds_api_events()

    for event in events:
        match_dt = parse_iso_datetime(event.get("commence_time"))
        if match_dt is None or match_dt.date() < today:
            skipped += 1
            continue

        home_name = event.get("home_team", "").strip()
        away_name = event.get("away_team", "").strip()
        home_id = team_map.get(home_name)
        away_id = team_map.get(away_name)

        if not home_id or not away_id:
            unknown_team += 1
            print(f"  Unknown team(s): {home_name!r} vs {away_name!r}")
            continue

        match_id = find_match_id(match_index, home_id, away_id, match_dt.date())
        if match_id is None:
            no_match += 1
            continue

        bookmaker = choose_bookmaker(event.get("bookmakers", []), preferred_sportsbook)
        if bookmaker is None:
            skipped += 1
            continue
        if bookmaker.get("title") != preferred_sportsbook:
            fallbacks += 1

        h2h = find_market(bookmaker, "h2h")
        spreads = find_market(bookmaker, "spreads")
        totals = find_market(bookmaker, "totals")

        home_ml = away_ml = None
        if h2h:
            for o in h2h.get("outcomes", []):
                if o.get("name") == home_name:
                    home_ml = o.get("price")
                elif o.get("name") == away_name:
                    away_ml = o.get("price")

        over_under = over_odds = under_odds = None
        if totals:
            for o in totals.get("outcomes", []):
                if o.get("point") is not None:
                    over_under = o["point"]
                nm = (o.get("name") or "").lower()
                if nm == "over":
                    over_odds = o.get("price")
                elif nm == "under":
                    under_odds = o.get("price")

        spread_home = spread_away = spread_home_odds = spread_away_odds = None
        if spreads:
            for o in spreads.get("outcomes", []):
                if o.get("name") == home_name:
                    spread_home = o.get("point")
                    spread_home_odds = o.get("price")
                elif o.get("name") == away_name:
                    spread_away = o.get("point")
                    spread_away_odds = o.get("price")

        values = {
            "home_moneyline": home_ml,
            "away_moneyline": away_ml,
            "spread_home": spread_home,
            "spread_away": spread_away,
            "spread_home_odds": spread_home_odds,
            "spread_away_odds": spread_away_odds,
            "over_under": over_under,
            "over_odds": over_odds,
            "under_odds": under_odds,
            "notes": "Imported from The Odds API",
        }

        action = upsert_odds(cur, match_id, bookmaker["title"], match_dt.isoformat(), values)
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
        "fallbacks": fallbacks,
    }


# ---------------------------------------------------------------------------
# Local CSV path (Kaggle export format)
# ---------------------------------------------------------------------------

def parse_float_or_none(value):
    text = (value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def import_csv(conn, csv_path, season, sportsbook, future_only=False):
    team_map = load_team_map(conn)
    match_index = load_match_index(conn, season)
    cur = conn.cursor()

    inserted = updated = no_match = unknown_team = skipped = skipped_past = 0
    today = datetime.now().date()

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            match_dt = parse_iso_datetime(row.get("match_date"))
            if match_dt is None:
                skipped += 1
                continue

            if future_only and match_dt.date() < today:
                skipped_past += 1
                continue

            home_name = (row.get("home_team") or "").strip()
            away_name = (row.get("away_team") or "").strip()
            home_id = team_map.get(home_name)
            away_id = team_map.get(away_name)

            if not home_id or not away_id:
                unknown_team += 1
                continue

            match_id = find_match_id(match_index, home_id, away_id, match_dt.date())
            if match_id is None:
                no_match += 1
                continue

            row_sportsbook = (row.get("sportsbook") or "").strip() or sportsbook
            odds_date = (row.get("odds_date") or match_dt.isoformat()).strip()

            values = {
                "home_moneyline": parse_float_or_none(row.get("home_moneyline")),
                "away_moneyline": parse_float_or_none(row.get("away_moneyline")),
                "spread_home": parse_float_or_none(row.get("spread_home")),
                "spread_away": parse_float_or_none(row.get("spread_away")),
                "spread_home_odds": parse_float_or_none(row.get("spread_home_odds")),
                "spread_away_odds": parse_float_or_none(row.get("spread_away_odds")),
                "over_under": parse_float_or_none(row.get("over_under")),
                "over_odds": parse_float_or_none(row.get("over_odds")),
                "under_odds": parse_float_or_none(row.get("under_odds")),
                "notes": (row.get("notes") or f"Imported from {csv_path.name}").strip(),
            }

            action = upsert_odds(cur, match_id, row_sportsbook, odds_date, values)
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
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    if args.future_only and not args.files:
        conn = sqlite3.connect(DATABASE_PATH)
        try:
            stats = import_odds_api(conn, args.sportsbook, season=args.season)
            print(
                f"The Odds API (NHL): "
                f"{stats['inserted']} inserted, {stats['updated']} updated, "
                f"{stats['no_match']} unmatched, {stats['unknown_team']} unknown-team, "
                f"{stats['skipped']} skipped"
                + (f", {stats['fallbacks']} bookmaker fallbacks" if stats["fallbacks"] else "")
            )
        finally:
            conn.close()
        return

    if not args.files:
        raise SystemExit(
            "Provide a CSV file to import, or use --future-only to fetch live odds from The Odds API."
        )

    for file_name in args.files:
        csv_path = Path(file_name)
        if not csv_path.exists():
            print(f"File not found: {csv_path}")
            continue
        if not args.season:
            print(f"--season is required for CSV import ({csv_path.name}).")
            continue

        conn = sqlite3.connect(DATABASE_PATH)
        try:
            stats = import_csv(
                conn, csv_path, args.season, args.sportsbook,
                future_only=args.future_only,
            )
            print(
                f"{csv_path.name} (season {args.season}): "
                f"{stats['inserted']} inserted, {stats['updated']} updated, "
                f"{stats['no_match']} unmatched, {stats['unknown_team']} unknown-team, "
                f"{stats['skipped']} skipped"
                + (f", {stats['skipped_past']} past rows skipped" if stats.get("skipped_past") else "")
            )
        finally:
            conn.close()


if __name__ == "__main__":
    main()
