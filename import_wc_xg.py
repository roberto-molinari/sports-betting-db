"""
Optional, heavy pass: upgrade the attack signal from goals/90 to REAL xG/90.

TheStatsAPI exposes expected goals only per match, not per season, so this
aggregates each WC player's club xG across that club's whole season:

  for each club with WC players:
    list the club's finished league matches
    for each match: pull matches/{id}/player-stats, sum shooting.expected_goals
                    and minutes_played for our players
  xg_per90 = total_xg / total_minutes * 90

The computed xg / xg_per90 are written back into soccer_wc_player_stats (other
fields are preserved). compute_wc_team_strength.py automatically prefers xg_per90
over goals/90 once it is present, so re-running strength after this pass upgrades
every team that got real xG.

This is expensive (~10k+ requests, tens of minutes) — run it after the cheap
goals/90 import, e.g. overnight. The request cap and counter still apply.

Usage:
    python import_wc_xg.py                      # all players
    python import_wc_xg.py --team Brazil        # one team
    python import_wc_xg.py --dry-run --team Brazil
"""

import argparse
import sqlite3
import sys
from collections import defaultdict

from core.sports_db import DATABASE_PATH, upsert_wc_player_stats
from core.thestatsapi import Client, TheStatsAPIError
from import_wc_player_stats import load_players, club_meta


def parse_args():
    parser = argparse.ArgumentParser(description="Aggregate real per-match xG into season xG/90.")
    parser.add_argument("--team", help="Only process players on this national team.")
    parser.add_argument("--limit", type=int, help="Process at most N players.")
    parser.add_argument("--season", type=int, default=2025,
                        help="Season label to update in soccer_wc_player_stats (default 2025).")
    parser.add_argument("--api-key", help="Override the THE_STATS_API_API_KEY env var.")
    parser.add_argument("--max-requests", type=int, default=15000,
                        help="Abort if more than this many API requests are issued (default 15000).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Aggregate and report without writing to the database.")
    return parser.parse_args()


def club_finished_matches(client, club_id, meta, match_cache):
    """Return the club's finished matches for the season (filtered to the club)."""
    key = (club_id, meta["season_id"])
    if key in match_cache:
        return match_cache[key]
    all_matches = list(client.paginate(
        "matches", {"competition_id": meta["comp_id"], "season_id": meta["season_id"]}))
    matches = [m for m in all_matches
               if m.get("status") == "finished"
               and club_id in (m["home_team"]["id"], m["away_team"]["id"])]
    match_cache[key] = matches
    return matches


def main():
    args = parse_args()
    try:
        client = Client(api_key=args.api_key, max_requests=args.max_requests)
    except TheStatsAPIError as exc:
        sys.exit(str(exc))

    conn = sqlite3.connect(DATABASE_PATH)
    players = load_players(conn, team=args.team, limit=args.limit)
    conn.close()
    if not players:
        sys.exit("No players with api_player_id — run import_wc_squads.py first.")

    # Group players by club so each club's matches are pulled once.
    by_club = defaultdict(list)
    for row in players:
        player_id, name, club, api_player_id, api_club_id, team_name = row
        if api_club_id and api_player_id:
            by_club[api_club_id].append(
                {"player_id": player_id, "pid": api_player_id, "team": team_name})

    meta_cache, match_cache, ps_cache = {}, {}, {}
    agg = defaultdict(lambda: {"xg": 0.0, "min": 0})
    n_clubs = len(by_club)
    n_updated = 0

    try:
        for idx, (club_id, club_players) in enumerate(by_club.items(), 1):
            meta = club_meta(client, club_id, meta_cache)
            if not meta or not meta.get("season_id"):
                continue
            pids = {p["pid"] for p in club_players}
            matches = club_finished_matches(client, club_id, meta, match_cache)
            for m in matches:
                if m["id"] in ps_cache:
                    rows = ps_cache[m["id"]]
                else:
                    rows = client.get_data(f"matches/{m['id']}/player-stats") or []
                    ps_cache[m["id"]] = rows
                for r in rows:
                    if r.get("player_id") in pids:
                        a = agg[r["player_id"]]
                        a["xg"] += (r.get("shooting") or {}).get("expected_goals") or 0
                        a["min"] += r.get("minutes_played") or 0
            print(f"  club {idx}/{n_clubs} ({len(matches)} matches), "
                  f"{client.requests_made} requests", flush=True)

            if not args.dry_run:
                for p in club_players:
                    a = agg[p["pid"]]
                    if a["min"] > 0:
                        upsert_wc_player_stats(
                            p["player_id"], season=args.season,
                            xg=a["xg"], xg_per90=a["xg"] / a["min"] * 90,
                            source="thestatsapi xg-agg")
                        n_updated += 1
    except TheStatsAPIError as exc:
        print(f"\nAborted: {exc}")

    with_xg = sum(1 for a in agg.values() if a["min"] > 0)
    print(f"\n=== xG AGGREGATION ({'dry-run' if args.dry_run else 'stored'}) ===")
    print(f"Clubs: {n_clubs}   players with real xG: {with_xg}   updated rows: {n_updated}")
    print(f"API requests used: {client.requests_made}")


if __name__ == "__main__":
    main()
