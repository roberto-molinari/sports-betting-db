"""
FEATURE-011 prototype: pull current-season stats for imported club-league players
(soccer_players) and store them in soccer_player_stats.

Simpler than the WC version (import_wc_player_stats.py): a club-league player's team
IS their club, so there's no club-of-national-team indirection -- every player in scope
shares the same competition/season context, and club-level defense is one API call per
TEAM (not per player).

Field shape matches soccer_wc_player_stats intentionally so compute_wc_team_strength.py's
aggregation logic is reusable with minimal changes. As in the WC import, xG isn't
available at the season-stats level (only per-match), so xg/xg_per90 stay null here and
the strength computation falls back to goals/90 -- same known limitation, not new.

Usage:
    python import_club_player_stats.py --limit-teams 3   # matches the squad prototype subset
    python import_club_player_stats.py --team "AC Milan" --limit 5 --dry-run
"""

import argparse
import sqlite3
import sys

from core.sports_db import DATABASE_PATH, upsert_player_stats
from core.thestatsapi import Client, TheStatsAPIError
from import_club_squads import resolve_competition, normalize_team_name, load_db_teams


def parse_args():
    parser = argparse.ArgumentParser(description="Pull club-league players' season stats from TheStatsAPI.")
    parser.add_argument("--league", default="Serie A")
    parser.add_argument("--season", type=int, default=2025,
                        help="Season used to select the DB team set (default: 2025).")
    parser.add_argument("--stats-season", type=int, default=2025,
                        help="Season label stored in soccer_player_stats (default: 2025).")
    parser.add_argument("--search", default=None)
    parser.add_argument("--country", default=None)
    parser.add_argument("--competition-id")
    parser.add_argument("--team", help="Only process players on this DB team name.")
    parser.add_argument("--limit-teams", type=int,
                        help="Only process the first N teams (validation run).")
    parser.add_argument("--limit", type=int, help="Only process the first N players overall.")
    parser.add_argument("--api-key")
    parser.add_argument("--max-requests", type=int, default=500)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def load_players(team_ids, limit=None):
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    placeholders = ",".join("?" * len(team_ids))
    sql = f"""SELECT player_id, team_id, name, api_player_id
              FROM soccer_players WHERE team_id IN ({placeholders}) AND api_player_id IS NOT NULL
              ORDER BY team_id, name"""
    params = list(team_ids)
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return rows


def club_defense(client, api_team_id, season_id):
    """Return club_ga_per90 (goals against / matches played), or None."""
    stats = client.get_data(f"teams/{api_team_id}/stats", {"season_id": season_id})
    if not stats:
        return None
    ga = stats.get("goals_against")
    mp = stats.get("matches_played")
    return (ga / mp) if (ga is not None and mp) else None


def player_line(client, api_player_id, season_id, comp_id):
    stats = client.get_data(f"players/{api_player_id}/stats",
                            {"season_id": season_id, "competition_id": comp_id})
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
    }


def main():
    args = parse_args()
    search = args.search or args.league
    try:
        client = Client(api_key=args.api_key, max_requests=args.max_requests)
    except TheStatsAPIError as exc:
        sys.exit(str(exc))

    db_teams = load_db_teams(args.league, args.season)
    if args.team:
        db_teams = {tid: name for tid, name in db_teams.items() if name == args.team}
        if not db_teams:
            sys.exit(f"No DB team named {args.team!r} in {args.league} season {args.season}.")

    try:
        comp = resolve_competition(client, args.competition_id, search, args.country)
        season_id = comp.get("current_season_id")
        comp_id = comp["id"]
        print(f"Competition: {comp_id}  {comp['name']}  season={season_id}")

        api_teams = list(client.paginate("teams", {"competition_id": comp_id, "season_id": season_id}))
        api_by_norm = {normalize_team_name(t["name"]): t for t in api_teams}
        team_api_id = {}
        for tid, name in db_teams.items():
            api_team = api_by_norm.get(normalize_team_name(name))
            if api_team:
                team_api_id[tid] = api_team["id"]
        team_ids = list(team_api_id)
        if args.limit_teams:
            team_ids = team_ids[:args.limit_teams]
        print(f"Teams in scope: {len(team_ids)}")

        # One club-defense lookup per team (cached), not per player.
        defense_cache = {}
        for tid in team_ids:
            defense_cache[tid] = club_defense(client, team_api_id[tid], season_id)

        players = load_players(team_ids, limit=args.limit)
        print(f"Players in scope: {len(players)}\n")

        n_stored = n_with_minutes = n_processed = 0
        per_team = {}
        for player_id, team_id, name, api_player_id in players:
            n_processed += 1
            line = player_line(client, api_player_id, season_id, comp_id)
            club_ga = defense_cache.get(team_id)

            has_minutes = bool(line and line.get("minutes_played"))
            if has_minutes:
                n_with_minutes += 1
            per_team.setdefault(team_id, {"squad": 0, "with_minutes": 0})
            per_team[team_id]["squad"] += 1
            if has_minutes:
                per_team[team_id]["with_minutes"] += 1

            if not args.dry_run and (line or club_ga is not None):
                line = line or {}
                upsert_player_stats(
                    player_id, season=args.stats_season,
                    minutes_played=line.get("minutes_played"),
                    goals=line.get("goals"), assists=line.get("assists"),
                    club_ga_per90=club_ga, source="thestatsapi",
                )
                n_stored += 1

            if n_processed % 25 == 0:
                print(f"  ...{n_processed}/{len(players)} players, "
                      f"{client.requests_made} requests", flush=True)
    except TheStatsAPIError as exc:
        print(f"\nAborted after {n_processed}/{len(players)} players: {exc}")

    print(f"\n=== COVERAGE ({'dry-run' if args.dry_run else 'stored'}) ===")
    print(f"Players processed: {n_processed}   stored: {n_stored}   "
          f"with minutes: {n_with_minutes} ({n_with_minutes/n_processed:.0%})" if n_processed else "No players processed.")
    print(f"API requests used: {client.requests_made}")


if __name__ == "__main__":
    main()
