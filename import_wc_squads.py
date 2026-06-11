"""
Import World Cup 2026 squads from TheStatsAPI into soccer_wc_teams / soccer_wc_players.

Resolves the World Cup competition, lists its national teams, and pulls each
team's squad. Player rows carry the API ids (api_player_id, api_club_id) and the
player's current club, which import_wc_player_stats.py then uses to fetch club stats.

The API key is read from THE_STATS_API_API_KEY (or pass --api-key).

Usage:
    python import_wc_squads.py                       # auto-resolve the WC competition
    python import_wc_squads.py --competition-id comp_1234
    python import_wc_squads.py --limit-teams 1 --dry-run   # validation run
"""

import argparse
import sys

from core.sports_db import ensure_wc_team, add_wc_player
from core.thestatsapi import Client, TheStatsAPIError
from core.fifa_rankings import get_fifa_ranking


def parse_args():
    parser = argparse.ArgumentParser(description="Import World Cup 2026 squads from TheStatsAPI.")
    parser.add_argument("--competition-id",
                        help="Use this competition id directly instead of searching.")
    parser.add_argument("--search", default="World Cup",
                        help="Competition name search term (default: 'World Cup').")
    parser.add_argument("--limit-teams", type=int,
                        help="Only import the first N teams (for a quick validation run).")
    parser.add_argument("--api-key", help="Override the THE_STATS_API_API_KEY env var.")
    parser.add_argument("--max-requests", type=int, default=1000,
                        help="Abort if more than this many API requests are issued "
                             "(guards the quota; default 1000).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and report without writing to the database.")
    return parser.parse_args()


def resolve_competition(client, competition_id, search):
    """Return the World Cup competition `data` dict, or exit with guidance."""
    if competition_id:
        comp = client.get_data(f"competitions/{competition_id}")
        if not comp:
            sys.exit(f"Competition {competition_id!r} not found.")
        return comp

    candidates = [
        c for c in client.paginate("competitions", {"search": search})
        if "world cup" in c["name"].lower()
    ]
    # Prefer a senior men's World Cup with an active season (skip youth/women's if named).
    preferred = [c for c in candidates if c.get("current_season_id")
                 and not any(w in c["name"].lower() for w in ("women", "u-", "u17", "u20", "u23"))]
    pool = preferred or candidates

    if not pool:
        sys.exit(f"No competition matched search {search!r}. Try --competition-id.")
    if len(pool) > 1:
        print("Multiple matching competitions — re-run with --competition-id:")
        for c in pool:
            print(f"  {c['id']}  {c['name']} ({c.get('country')})  "
                  f"season={c.get('current_season_id')}")
        sys.exit(1)
    # Single match: fetch full detail (for current_season_id).
    return client.get_data(f"competitions/{pool[0]['id']}")


def main():
    args = parse_args()
    try:
        client = Client(api_key=args.api_key, max_requests=args.max_requests)
    except TheStatsAPIError as exc:
        sys.exit(str(exc))

    try:
        run(client, args)
    except TheStatsAPIError as exc:
        print(f"\nAborted: {exc}")
    finally:
        print(f"API requests used: {client.requests_made}")


def run(client, args):
    comp = resolve_competition(client, args.competition_id, args.search)
    season_id = comp.get("current_season_id")
    print(f"Competition: {comp['id']}  {comp['name']} ({comp.get('country')})")
    print(f"Season: {season_id}   xg_available={comp.get('xg_available')}  "
          f"has_player_stats={comp.get('has_player_stats')}")
    if not season_id:
        sys.exit("Competition has no current_season_id — squads not available yet.")

    teams = list(client.paginate("teams",
                                 {"competition_id": comp["id"], "season_id": season_id}))
    if args.limit_teams:
        teams = teams[:args.limit_teams]
    print(f"Teams found: {len(teams)}\n")

    total_players = 0
    squad_sizes = []
    for team in teams:
        # National-team squads come from /teams/{id}/players (a single page);
        # /players?team_id= only indexes players by their club.
        squad = client.get_data(f"teams/{team['id']}/players") or []
        squad_sizes.append(len(squad))
        flag = " (no current_team)" if any(not p.get("current_team") for p in squad) else ""
        print(f"  {team['name']:<24} {len(squad):>2} players{flag}")

        if args.dry_run:
            total_players += len(squad)
            continue

        team_db_id = ensure_wc_team(team["name"], api_team_id=team["id"],
                                    fifa_ranking=get_fifa_ranking(team["name"]))
        for p in squad:
            club = p.get("current_team") or {}
            add_wc_player(
                team_db_id, p["name"],
                position=p.get("position"),
                club=club.get("name"),
                api_player_id=p["id"],
                api_club_id=club.get("id"),
            )
            total_players += 1

    avg = (sum(squad_sizes) / len(squad_sizes)) if squad_sizes else 0
    print(f"\n{'(dry-run) ' if args.dry_run else ''}"
          f"Teams: {len(teams)}  Players: {total_players}  Avg squad: {avg:.1f}")


if __name__ == "__main__":
    main()
