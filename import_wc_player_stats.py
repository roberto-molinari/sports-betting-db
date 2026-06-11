"""
Pull current-season CLUB stats for every imported World Cup player and store
them in soccer_wc_player_stats, then print a data-coverage report.

For each player (from soccer_wc_players, populated by import_wc_squads.py):
  - resolve the player's club -> club's competition -> current season,
  - fetch player season stats (minutes, goals, assists, expected_goals -> xg_per90),
  - fetch the club team's season stats (goals_against / matches_played ->
    club_ga_per90, the defensive signal — the API exposes goals against but not
    xGA at team level, so club_xga_per90 is left null and the strength model
    falls back to club_ga_per90).

The coverage report is the day-1 gate: it shows how many players resolved usable
xG and club-defense numbers, and which national teams fall short (those will use
the FIFA-ranking fallback in compute_wc_team_strength.py).

API key is read from THE_STATS_API_API_KEY (or pass --api-key).

Usage:
    python import_wc_player_stats.py
    python import_wc_player_stats.py --team Brazil --limit 5 --dry-run   # validation
"""

import argparse
import sqlite3
import sys
from collections import defaultdict

from core.sports_db import DATABASE_PATH, upsert_wc_player_stats
from core.thestatsapi import Client, TheStatsAPIError

# Minimum share of a squad with usable xG for the team to use stat-based strength.
TEAM_COVERAGE_THRESHOLD = 0.5


def parse_args():
    parser = argparse.ArgumentParser(description="Pull WC players' club stats from TheStatsAPI.")
    parser.add_argument("--team", help="Only process players on this national team (by name).")
    parser.add_argument("--limit", type=int, help="Process at most N players (validation run).")
    parser.add_argument("--season", type=int, default=2025,
                        help="Season label stored in soccer_wc_player_stats (default: 2025).")
    parser.add_argument("--api-key", help="Override the THE_STATS_API_API_KEY env var.")
    parser.add_argument("--max-requests", type=int, default=3000,
                        help="Abort if more than this many API requests are issued "
                             "(guards the quota; default 3000).")
    parser.add_argument("--resume", action="store_true",
                        help="Skip players that already have stats for --season "
                             "(resume an interrupted run; only fetch the remainder).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and report without writing to the database.")
    return parser.parse_args()


def load_players(conn, team=None, limit=None, resume_season=None):
    cur = conn.cursor()
    sql = """SELECT p.player_id, p.name, p.club, p.api_player_id, p.api_club_id,
                    t.name AS team_name
             FROM soccer_wc_players p
             JOIN soccer_wc_teams t ON t.team_id = p.team_id
             WHERE p.api_player_id IS NOT NULL"""
    params = []
    if team:
        sql += " AND t.name = ?"
        params.append(team)
    if resume_season is not None:
        # Skip players that already have a stats row for this season — lets an
        # interrupted run resume and only fetch the remainder.
        sql += """ AND NOT EXISTS (
                       SELECT 1 FROM soccer_wc_player_stats s
                       WHERE s.player_id = p.player_id AND s.season IS ?)"""
        params.append(resume_season)
    sql += " ORDER BY t.name, p.name"
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    cur.execute(sql, params)
    return cur.fetchall()


def club_meta(client, api_club_id, cache):
    """Return {comp_id, season_id, has_player_stats, xg_available} for a club, cached."""
    if api_club_id in cache:
        return cache[api_club_id]
    meta = None
    team = client.get_data(f"teams/{api_club_id}")
    comp = (team or {}).get("primary_competition") or {}
    comp_id = comp.get("id")
    if comp_id:
        detail = client.get_data(f"competitions/{comp_id}") or {}
        meta = {
            "comp_id": comp_id,
            "comp_name": detail.get("name") or comp.get("name"),
            "season_id": detail.get("current_season_id"),
            "has_player_stats": detail.get("has_player_stats"),
            "xg_available": detail.get("xg_available"),
        }
    cache[api_club_id] = meta
    return meta


def club_defense(client, api_club_id, season_id, cache):
    """Return club_ga_per90 (goals against / matches played) for a club, cached."""
    if api_club_id in cache:
        return cache[api_club_id]
    value = None
    stats = client.get_data(f"teams/{api_club_id}/stats", {"season_id": season_id})
    if stats:
        ga = stats.get("goals_against")
        mp = stats.get("matches_played")
        if ga is not None and mp:
            value = ga / mp
    cache[api_club_id] = value
    return value


def fetch_player_line(client, api_player_id, meta):
    """Return a stats dict for storage, or None if no usable player stats.

    The season stats response nests scoring under a `scoring` block and does NOT
    expose expected goals (xG exists only at the per-match level). We therefore
    populate goals/assists/minutes here and leave xg/xg_per90 to the optional
    per-match xG pass; the strength model derives attack from goals/90 until then.
    """
    if not meta or not meta.get("season_id") or not meta.get("has_player_stats"):
        return None
    stats = client.get_data(
        f"players/{api_player_id}/stats",
        {"season_id": meta["season_id"], "competition_id": meta["comp_id"]},
    )
    if not stats:
        return None
    scoring = stats.get("scoring") or {}
    minutes = stats.get("minutes_played")
    goals = scoring.get("goals")
    return {
        "minutes_played": minutes,
        "goals": goals,
        "assists": scoring.get("assists"),
        "goals_per90": (goals / minutes * 90) if (goals is not None and minutes) else None,
        "xg": None,        # not available at season level
        "xg_per90": None,
    }


def main():
    args = parse_args()
    try:
        client = Client(api_key=args.api_key, max_requests=args.max_requests)
    except TheStatsAPIError as exc:
        sys.exit(str(exc))

    conn = sqlite3.connect(DATABASE_PATH)
    players = load_players(conn, team=args.team, limit=args.limit,
                           resume_season=args.season if args.resume else None)
    conn.close()
    if not players:
        if args.resume:
            print("Nothing to resume — all players already have stats for "
                  f"season {args.season}.")
            return
        sys.exit("No players with api_player_id — run import_wc_squads.py first.")

    meta_cache, def_cache = {}, {}
    per_team = defaultdict(lambda: {"squad": 0, "att": 0, "ga": 0})
    n_stored = n_no_club = n_processed = 0

    try:
        for player_id, name, club, api_player_id, api_club_id, team_name in players:
            per_team[team_name]["squad"] += 1
            n_processed += 1
            if not api_club_id:
                n_no_club += 1
                continue

            meta = club_meta(client, api_club_id, meta_cache)
            line = fetch_player_line(client, api_player_id, meta)
            club_ga = club_defense(client, api_club_id, meta["season_id"], def_cache) if meta and meta.get("season_id") else None

            if line and line.get("goals_per90") is not None:
                per_team[team_name]["att"] += 1
            if club_ga is not None:
                per_team[team_name]["ga"] += 1

            if not args.dry_run and (line or club_ga is not None):
                line = line or {}
                upsert_wc_player_stats(
                    player_id, season=args.season,
                    minutes_played=line.get("minutes_played"),
                    xg=line.get("xg"), xg_per90=line.get("xg_per90"),
                    goals=line.get("goals"), assists=line.get("assists"),
                    club_ga_per90=club_ga, source="thestatsapi",
                )
                n_stored += 1

            if n_processed % 50 == 0:
                print(f"  ...{n_processed}/{len(players)} players, "
                      f"{client.requests_made} requests", flush=True)
    except TheStatsAPIError as exc:
        print(f"\nAborted after {n_processed}/{len(players)} players: {exc}")

    # ── Coverage report ──────────────────────────────────────────────────────
    total = n_processed or len(players)
    with_att = sum(t["att"] for t in per_team.values())
    with_ga = sum(t["ga"] for t in per_team.values())
    print(f"\n=== COVERAGE ({'dry-run' if args.dry_run else 'stored'}) ===")
    print(f"Players processed: {total}   stored: {n_stored}   no club: {n_no_club}")
    print(f"API requests used: {client.requests_made}")
    print(f"With goals/90 (attack): {with_att} ({with_att/total:.0%})   "
          f"with club GA (defense): {with_ga} ({with_ga/total:.0%})")
    print(f"\n{'TEAM':<24} {'SQUAD':>5} {'ATT':>4} {'GA':>4}  FLAG")
    for team_name in sorted(per_team):
        t = per_team[team_name]
        share = t["att"] / t["squad"] if t["squad"] else 0
        flag = "" if share >= TEAM_COVERAGE_THRESHOLD else "LOW -> FIFA fallback"
        print(f"{team_name:<24} {t['squad']:>5} {t['att']:>4} {t['ga']:>4}  {flag}")


if __name__ == "__main__":
    main()
