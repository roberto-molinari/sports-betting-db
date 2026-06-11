"""
Import World Cup 2026 fixtures + odds directly from Bovada's public feed.

Bovada renders its sportsbook via JavaScript, but the page is backed by a JSON
coupon endpoint that lists every posted match with 1X2 (3-way moneyline), the
goal total line, and over/under prices.  This script pulls that feed, seeds any
missing fixtures into soccer_wc_matches, and upserts the odds into
soccer_wc_odds — replacing the manual screenshot->CSV step for Bovada.

It is safe to re-run: fixtures are keyed on (home, away, date) and odds are
upserted on (match_id, sportsbook), so refreshing before a matchday just updates
prices in place.

Only the main "Game Lines" markets are read (3W-1X2 and 2W-OU, Regulation Time).
Bovada uses nominal home/away for these neutral-venue games; we store that
orientation consistently across fixtures and odds so grading stays aligned.

Usage:
    python import_wc_bovada.py                       # fetch live feed + load
    python import_wc_bovada.py --dry-run             # show what would change
    python import_wc_bovada.py --json feed.json      # load from a saved feed
    python import_wc_bovada.py --save-csv odds.csv   # also write a reviewable CSV
"""

import argparse
import csv
import json
import sqlite3
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests

from core.sports_db import (
    DATABASE_PATH,
    ensure_wc_match,
    upsert_wc_odds,
)

FEED_URL = (
    "https://www.bovada.lv/services/sports/event/coupon/events/A/description/"
    "soccer/fifa-world-cup/fifa-world-cup-matches"
    "?marketFilterId=def&preMatchOnly=true&eventsLimit=200&lang=en"
)
# match_date is stored in UTC (canonical). This tz is used only to render the
# Eastern matchday into the review CSV, since matchdays are reckoned in Eastern
# (the US broadcast/posting frame); the card does the UTC->Eastern bucketing at
# query time. ZoneInfo is DST-aware (June-July games resolve to EDT, UTC-4).
TOURNAMENT_TZ = ZoneInfo("America/New_York")

FEED_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "X-Channel": "desktop",
}

# Bovada spellings -> canonical soccer_wc_teams.name.
BOVADA_TEAM_ALIASES = {
    "Curacao": "Curaçao",
    "Ivory Coast": "Côte d'Ivoire",
    "Turkey": "Türkiye",
}

CSV_FIELDS = ["date", "home", "away", "home_ml", "draw_ml", "away_ml",
              "total", "over_odds", "under_odds"]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load World Cup fixtures + odds from Bovada's feed.")
    parser.add_argument("--json", help="Read the feed from a saved JSON file "
                                        "instead of fetching it live.")
    parser.add_argument("--sportsbook", default="Bovada",
                        help="Sportsbook label stored with each odds row.")
    parser.add_argument("--save-csv", help="Also write the parsed odds to this CSV.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and report without writing to the database.")
    return parser.parse_args()


def fetch_feed():
    resp = requests.get(FEED_URL, headers=FEED_HEADERS, timeout=40)
    resp.raise_for_status()
    return resp.json()


def canonical(name):
    return BOVADA_TEAM_ALIASES.get(name, name)


def american(value):
    """Normalize a Bovada american-odds string ('EVEN', '+118', '-143')."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.upper() == "EVEN":
        return 100.0
    try:
        return float(text)
    except ValueError:
        return None


def parse_event(ev):
    """Pull (date, home, away, mls, total, ou_odds) from one Bovada event.

    Returns None if the event lacks usable home/away competitors.
    """
    comps = {c.get("home"): c for c in ev.get("competitors", [])}
    home = comps.get(True)
    away = comps.get(False)
    if not home or not away:
        return None

    start = ev.get("startTime")
    utc = datetime.fromtimestamp(start / 1000, tz=timezone.utc) if start else None
    # match_date is stored in UTC (canonical); the card buckets by the Eastern
    # calendar day at query time. The Eastern date is kept only for the CSV so a
    # human reviewer sees the matchday the way it's reckoned for posting.
    eastern = utc.astimezone(TOURNAMENT_TZ) if utc else None

    home_ml = draw_ml = away_ml = None
    total = over_odds = under_odds = None
    for dg in ev.get("displayGroups", []):
        for m in dg.get("markets", []):
            key = m.get("key")
            if key == "3W-1X2":
                for o in m.get("outcomes", []):
                    price = american((o.get("price") or {}).get("american"))
                    t = o.get("type")
                    if t == "H":
                        home_ml = price
                    elif t == "D":
                        draw_ml = price
                    elif t == "A":
                        away_ml = price
            elif key == "2W-OU":
                for o in m.get("outcomes", []):
                    price_obj = o.get("price") or {}
                    price = american(price_obj.get("american"))
                    line = price_obj.get("handicap")
                    if o.get("type") == "O":
                        over_odds = price
                        total = float(line) if line not in (None, "") else total
                    elif o.get("type") == "U":
                        under_odds = price

    return {
        "datetime": utc,
        "date": eastern.date().isoformat() if eastern else "",
        "match_date": utc.strftime("%Y-%m-%d %H:%M:%S") if utc else None,
        "home": canonical(home.get("name")),
        "away": canonical(away.get("name")),
        "home_ml": home_ml,
        "draw_ml": draw_ml,
        "away_ml": away_ml,
        "total": total,
        "over_odds": over_odds,
        "under_odds": under_odds,
    }


def load_team_map(conn):
    return {name.lower(): tid
            for tid, name in conn.execute("SELECT team_id, name FROM soccer_wc_teams")}


def main():
    args = parse_args()

    if args.json:
        with open(args.json, encoding="utf-8") as fh:
            feed = json.load(fh)
    else:
        try:
            feed = fetch_feed()
        except requests.RequestException as exc:
            sys.exit(f"Failed to fetch Bovada feed: {exc}")

    events = []
    for block in feed:
        events.extend(block.get("events", []))
    rows = [r for r in (parse_event(ev) for ev in events) if r]
    print(f"Parsed {len(rows)} matches from the Bovada feed.")

    conn = sqlite3.connect(DATABASE_PATH)
    team_map = load_team_map(conn)
    conn.close()
    if not team_map:
        sys.exit("No teams in soccer_wc_teams — import squads/teams first.")

    odds_date = datetime.now(timezone.utc).isoformat()
    fixtures_made = odds_written = 0
    unmatched = []

    for r in rows:
        home_id = team_map.get(r["home"].lower())
        away_id = team_map.get(r["away"].lower())
        if home_id is None or away_id is None:
            unmatched.append((r["home"], r["away"]))
            continue
        if args.dry_run:
            continue
        match_id = ensure_wc_match(r["match_date"], home_id, away_id, stage="Group")
        fixtures_made += 1
        upsert_wc_odds(
            match_id=match_id,
            sportsbook=args.sportsbook,
            odds_date=odds_date,
            home_moneyline=r["home_ml"],
            draw_moneyline=r["draw_ml"],
            away_moneyline=r["away_ml"],
            over_under=r["total"],
            over_odds=r["over_odds"],
            under_odds=r["under_odds"],
        )
        odds_written += 1

    if args.save_csv:
        with open(args.save_csv, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
            writer.writeheader()
            for r in rows:
                writer.writerow({k: r.get(k) for k in CSV_FIELDS})
        print(f"Wrote {len(rows)} rows to {args.save_csv}")

    prefix = "(dry-run) " if args.dry_run else ""
    print(f"{prefix}Sportsbook: {args.sportsbook}")
    print(f"{prefix}Fixtures ensured: {fixtures_made}  Odds rows upserted: {odds_written}")
    if unmatched:
        print(f"Unmatched team names ({len(unmatched)}) — add to BOVADA_TEAM_ALIASES:")
        for home, away in unmatched:
            print(f"  {home!r} vs {away!r}")


if __name__ == "__main__":
    main()
