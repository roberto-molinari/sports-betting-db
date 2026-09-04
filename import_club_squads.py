"""
FEATURE-011 prototype: import a club league's current squads from TheStatsAPI into
soccer_players. Unlike the World Cup import (import_wc_squads.py), a club-league "team"
IS the club -- so this matches directly against the existing soccer_teams table instead
of creating a separate team table, and there's no club-of-national-team indirection.

Usage:
    python import_club_squads.py                          # Serie A, season 2025
    python import_club_squads.py --league "Serie A" --country Italy
    python import_club_squads.py --limit-teams 3 --dry-run # validation run
"""

import argparse
import re
import sqlite3
import sys

from core.leagues import LEAGUES
from core.sports_db import DATABASE_PATH, add_player
from core.thestatsapi import Client, TheStatsAPIError


def parse_args():
    parser = argparse.ArgumentParser(description="Import club-league squads from TheStatsAPI.")
    parser.add_argument("--league", default="Serie A",
                        help="League name as stored in soccer_teams.league (default: Serie A).")
    parser.add_argument("--season", type=int, default=2025,
                        help="Season used to select the current team set from soccer_matches.")
    parser.add_argument("--search", default=None,
                        help="TheStatsAPI competition search term (default: --league value).")
    parser.add_argument("--country", default=None,
                        help="Filter competition candidates by country (disambiguates "
                             "leagues that share a name, e.g. Serie A Italy vs Brazil).")
    parser.add_argument("--competition-id",
                        help="Use this competition id directly instead of searching.")
    parser.add_argument("--limit-teams", type=int,
                        help="Only import the first N matched teams (for a quick validation run).")
    parser.add_argument("--api-key", help="Override the THE_STATS_API_API_KEY env var.")
    parser.add_argument("--max-requests", type=int, default=200,
                        help="Abort if more than this many API requests are issued "
                             "(guards the quota; default 200).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and report without writing to the database.")
    return parser.parse_args()


def resolve_competition(client, competition_id, search, country):
    if competition_id:
        comp = client.get_data(f"competitions/{competition_id}")
        if not comp:
            sys.exit(f"Competition {competition_id!r} not found.")
        return comp

    candidates = list(client.paginate("competitions", {"search": search}))
    if country:
        candidates = [c for c in candidates if (c.get("country") or "").lower() == country.lower()]

    if not candidates:
        sys.exit(f"No competition matched search {search!r} (country={country!r}). "
                  f"Try --competition-id.")
    if len(candidates) > 1:
        print("Multiple matching competitions -- re-run with --competition-id:")
        for c in candidates:
            print(f"  {c['id']}  {c['name']} ({c.get('country')})")
        sys.exit(1)
    return client.get_data(f"competitions/{candidates[0]['id']}")


def pick_season_id(seasons, season):
    """Pure matching logic behind resolve_season_id, split out for testing: find the
    season dict whose start_year equals OUR `season` convention. Returns None (not an
    error) if not found -- the caller decides how to fail."""
    for s in seasons:
        if s.get("start_year") == season:
            return s["id"]
    return None


def resolve_season_id(client, competition_id, season):
    """Resolve TheStatsAPI's season id for OUR `season` convention (a season's
    start_year, e.g. 2024 = "2024-25") via competitions/{id}/seasons.

    Deliberately NOT comp['current_season_id'] -- that's always the API's CURRENT
    season regardless of which `--season` was requested. A real bug found backfilling
    season=2024 while the API's current season was 2025-26: current_season_id would
    have silently resolved to 2025-26's matches, mislabeling that season's data as
    2024 in our DB (272/380 team-pairing "matches" were actually 2025-26 fixtures
    coincidentally resolving against a mixed team set -- caught before any data was
    written, by noticing relegated 2024-25 teams like Empoli/Monza/Venezia FC showing
    as unmatched against what should have been their own season's team list)."""
    seasons = client.get_data(f"competitions/{competition_id}/seasons") or []
    season_id = pick_season_id(seasons, season)
    if season_id is None:
        available = ", ".join(f"{s.get('year')}={s.get('start_year')}" for s in seasons)
        sys.exit(f"No season with start_year={season} for competition {competition_id}. "
                  f"Available: {available or '(none)'}")
    return season_id


# Strips common Italian-club prefixes/suffixes so "AC Milan" / "Cagliari Calcio" /
# "Como 1907" match the API's bare "Milan" / "Cagliari" / "Como". Verified against
# TheStatsAPI's actual Serie A team list (all 20/20 matched cleanly) before writing this.
_PREFIX_RE = re.compile(r"^(AC|AS|US|ACF)\s+", re.IGNORECASE)
_SUFFIX_RE = re.compile(r"\s+(Calcio|CFC|FC)\b", re.IGNORECASE)
_YEAR_RE = re.compile(r"\s+\d{4}$")


def normalize_team_name(name):
    n = _PREFIX_RE.sub("", name)
    n = _SUFFIX_RE.sub("", n)
    n = _YEAR_RE.sub("", n)
    return n.strip().lower()


def load_db_teams(league, season):
    """Return {team_id: name} for the teams that played in league/season."""
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT t.team_id, t.name FROM soccer_teams t
        JOIN soccer_matches m ON m.home_team_id = t.team_id OR m.away_team_id = t.team_id
        WHERE m.league = ? AND m.season = ?
        ORDER BY t.name
    """, (league, season))
    rows = cur.fetchall()
    conn.close()
    return {tid: name for tid, name in rows}


def match_teams_to_api(db_teams, api_teams):
    """Match {db_team_id: name} against a list of API team dicts via normalize_team_name.
    Returns (matched, unmatched_db, unmatched_api) where matched is
    [(db_team_id, db_name, api_team_dict), ...]. Shared by squad import and match-id
    mapping so both report the same match/unmatch story."""
    api_by_norm = {normalize_team_name(t["name"]): t for t in api_teams}
    matched, unmatched_db = [], []
    for tid, name in db_teams.items():
        api_team = api_by_norm.get(normalize_team_name(name))
        if api_team:
            matched.append((tid, name, api_team))
        else:
            unmatched_db.append(name)
    matched_api_names = {m[2]["name"] for m in matched}
    unmatched_api = [t["name"] for t in api_teams if t["name"] not in matched_api_names]
    return matched, unmatched_db, unmatched_api


def main():
    args = parse_args()
    search = args.search or args.league
    # FEATURE-019 (2026-08-19): default --competition-id from core/leagues.py's
    # registry when the caller didn't pass one -- a bare name search is
    # genuinely ambiguous for common league names (confirmed live: "Serie A",
    # "Premier League", "Bundesliga", "Ligue 1", "Championship" all match
    # multiple competitions, e.g. "Bundesliga" also matches "Austrian
    # Bundesliga"), so every squad refresh for those leagues failed and
    # required manual disambiguation. Explicit --competition-id still wins if
    # passed. Every league in the registry (Serie A included as of
    # 2026-09-04, BUGS.md) now has a real thestatsapi_competition_id.
    if args.competition_id is None:
        args.competition_id = LEAGUES.get(args.league, {}).get("thestatsapi_competition_id")
    try:
        client = Client(api_key=args.api_key, max_requests=args.max_requests)
    except TheStatsAPIError as exc:
        sys.exit(str(exc))

    db_teams = load_db_teams(args.league, args.season)
    if not db_teams:
        sys.exit(f"No teams found in soccer_teams for league={args.league!r} season={args.season}. "
                  f"Import matches first.")
    print(f"DB teams ({args.league}, season {args.season}): {len(db_teams)}")

    try:
        comp = resolve_competition(client, args.competition_id, search, args.country)
        season_id = resolve_season_id(client, comp["id"], args.season)
        print(f"Competition: {comp['id']}  {comp['name']} ({comp.get('country')})  "
              f"season={season_id}  has_player_stats={comp.get('has_player_stats')}  "
              f"xg_available={comp.get('xg_available')}")

        api_teams = list(client.paginate("teams", {"competition_id": comp["id"], "season_id": season_id}))
        print(f"API teams: {len(api_teams)}\n")

        matched, unmatched_db, unmatched_api = match_teams_to_api(db_teams, api_teams)

        print(f"Matched: {len(matched)}/{len(db_teams)}")
        if unmatched_db:
            print(f"UNMATCHED (in DB, not found via API): {unmatched_db}")
        if unmatched_api:
            print(f"UNMATCHED (API team with no DB match): {unmatched_api}")
        print()

        if args.limit_teams:
            matched = matched[:args.limit_teams]

        total_players = 0
        for tid, db_name, api_team in matched:
            squad = client.get_data(f"teams/{api_team['id']}/players") or []
            print(f"  {db_name:<22} <- {api_team['name']:<22} {len(squad):>2} players")
            if args.dry_run:
                total_players += len(squad)
                continue
            for p in squad:
                add_player(tid, p["name"], position=p.get("position"), api_player_id=p["id"])
                total_players += 1

        print(f"\n{'(dry-run) ' if args.dry_run else ''}Teams processed: {len(matched)}  "
              f"Players: {total_players}")
    except TheStatsAPIError as exc:
        print(f"\nAborted: {exc}")
    finally:
        print(f"API requests used: {client.requests_made}")


if __name__ == "__main__":
    main()
