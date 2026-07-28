"""
Import Serie A opening- and closing-line 1X2 odds from football-data.co.uk (free)
into soccer_market_odds -- sharp books (Pinnacle, Betfair Exchange) for CLV, plus
Bet365 as a soft/retail-book baseline for comparison.

Coverage per season (checked 2026-07-27), opening/closing identical shape:
  2023-24: Pinnacle and Bet365 complete (380/380); no Betfair Exchange column
           that season.
  2024-25: Pinnacle, Betfair Exchange, and Bet365 all complete (380/380).
  2025-26: Pinnacle has a large gap from 2026-01-15 onward (source-side data lag,
           not a real unavailability) -- 198/380 closing, 200/380 opening;
           Betfair Exchange is complete through March 2026, mostly complete after
           (359/380 opening, 360/380 closing); Bet365 is complete (380/380).

Odds are devigged (overround removed) via the standard multiplicative method:
p_fair = (1/odds) / sum((1/odds) across home+draw+away).

Usage:
    python import_serie_a_market_odds.py --season 2023
    python import_serie_a_market_odds.py --season 2024
    python import_serie_a_market_odds.py --season 2025
"""

import argparse
import csv
import io
import sqlite3
import urllib.request

from core.sports_db import (
    DATABASE_PATH,
    clear_soccer_market_odds,
    add_soccer_market_odds,
)

LEAGUE = "Serie A"

# football-data.co.uk season code, e.g. 2023 (our DB's season label) -> "2324"
FD_SEASON_CODE = {2022: "2223", 2023: "2324", 2024: "2425", 2025: "2526"}

FD_URL = "https://www.football-data.co.uk/mmz4281/{code}/I1.csv"

# football-data.co.uk short name -> our DB's soccer_teams.name (Serie A only;
# union of every team appearing in the 2023-24/2024-25/2025-26 files).
TEAM_NAME_MAP = {
    "Atalanta": "Atalanta",
    "Bologna": "Bologna",
    "Cagliari": "Cagliari Calcio",
    "Como": "Como 1907",
    "Cremonese": "Cremonese",
    "Empoli": "Empoli",
    "Fiorentina": "Fiorentina",
    "Frosinone": "Frosinone Calcio",
    "Genoa": "Genoa CFC",
    "Inter": "Inter",
    "Juventus": "Juventus",
    "Lazio": "Lazio",
    "Lecce": "Lecce",
    "Milan": "AC Milan",
    "Monza": "Monza",
    "Napoli": "Napoli",
    "Parma": "Parma Calcio 1913",
    "Pisa": "AC Pisa 1909",
    "Roma": "AS Roma",
    "Salernitana": "Salernitana",
    "Sassuolo": "Sassuolo",
    "Torino": "Torino",
    "Udinese": "Udinese",
    "Venezia": "Venezia FC",
    "Verona": "Hellas Verona",
}

# (source label, opening column names, closing column names)
SOURCES = [
    ("Pinnacle", ("PSH", "PSD", "PSA"), ("PSCH", "PSCD", "PSCA")),
    ("Betfair Exchange", ("BFEH", "BFED", "BFEA"), ("BFECH", "BFECD", "BFECA")),
    ("Bet365", ("B365H", "B365D", "B365A"), ("B365CH", "B365CD", "B365CA")),
]


def fetch_csv_rows(season_code):
    url = FD_URL.format(code=season_code)
    with urllib.request.urlopen(url) as resp:
        text = resp.read().decode("utf-8-sig")
    return list(csv.DictReader(io.StringIO(text)))


def devig(home_odds, draw_odds, away_odds):
    """Multiplicative devig: p_fair = (1/odds) / sum(1/odds)."""
    raw_h, raw_d, raw_a = 1 / home_odds, 1 / draw_odds, 1 / away_odds
    total = raw_h + raw_d + raw_a
    return raw_h / total, raw_d / total, raw_a / total


def get_team_id(cur, name):
    cur.execute("SELECT team_id FROM soccer_teams WHERE league = ? AND name = ?", (LEAGUE, name))
    row = cur.fetchone()
    return row[0] if row else None


def get_match_id(cur, season, home_team_id, away_team_id):
    cur.execute(
        """SELECT match_id FROM soccer_matches
           WHERE league = ? AND season = ? AND home_team_id = ? AND away_team_id = ?""",
        (LEAGUE, season, home_team_id, away_team_id)
    )
    row = cur.fetchone()
    return row[0] if row else None


def import_line_type(conn, cur, rows, season, source_label, line_type, hcol, dcol, acol):
    clear_soccer_market_odds(LEAGUE, season, source_label, line_type, conn=conn)

    inserted = 0
    skipped_no_odds = 0
    skipped_no_match = 0
    skipped_unmapped_team = 0

    for r in rows:
        home_fd, away_fd = r["HomeTeam"], r["AwayTeam"]
        home_name = TEAM_NAME_MAP.get(home_fd)
        away_name = TEAM_NAME_MAP.get(away_fd)
        if home_name is None or away_name is None:
            skipped_unmapped_team += 1
            continue

        h_odds, d_odds, a_odds = r.get(hcol), r.get(dcol), r.get(acol)
        if not h_odds or not d_odds or not a_odds:
            skipped_no_odds += 1
            continue

        home_id = get_team_id(cur, home_name)
        away_id = get_team_id(cur, away_name)
        if home_id is None or away_id is None:
            skipped_unmapped_team += 1
            continue

        match_id = get_match_id(cur, season, home_id, away_id)
        if match_id is None:
            skipped_no_match += 1
            continue

        h_odds, d_odds, a_odds = float(h_odds), float(d_odds), float(a_odds)
        p_h, p_d, p_a = devig(h_odds, d_odds, a_odds)

        add_soccer_market_odds(
            match_id=match_id, league=LEAGUE, source=source_label, line_type=line_type,
            home_odds=h_odds, draw_odds=d_odds, away_odds=a_odds,
            p_home_fair=p_h, p_draw_fair=p_d, p_away_fair=p_a,
            conn=conn,
        )
        inserted += 1

    print(f"[{source_label} / {line_type}] season={season}: inserted={inserted} "
          f"skipped_no_odds={skipped_no_odds} skipped_no_match={skipped_no_match} "
          f"skipped_unmapped_team={skipped_unmapped_team}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, required=True,
                        help="Our DB's season label, e.g. 2025 for 2025-26")
    args = parser.parse_args()

    season_code = FD_SEASON_CODE.get(args.season)
    if season_code is None:
        raise SystemExit(f"No football-data.co.uk season code mapped for season={args.season}")

    rows = fetch_csv_rows(season_code)

    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()

    for source_label, opening_cols, closing_cols in SOURCES:
        import_line_type(conn, cur, rows, args.season, source_label, "opening", *opening_cols)
        import_line_type(conn, cur, rows, args.season, source_label, "closing", *closing_cols)

    conn.close()


if __name__ == "__main__":
    main()
