"""
Import club-league PER-MATCH player stats and lineups from TheStatsAPI.

Rewritten from the season-total prototype (players/{id}/stats) to the per-match
approach agreed in FEATURE-011_REQUIREMENTS.md (Persistence): a completed season and
an in-progress season are stored identically, one row per player per match. This also
gets real xG (unavailable at the season-stats level) and real starting-lineup history
(Scenario 0), and removes the need for a separate per-team defense API call -- team
goals-against comes from our own soccer_matches, not the API.

Steps per league/season:
  1. Resolve the competition, match our DB teams to API teams (import_club_squads).
  2. Match our soccer_matches rows to the API's match ids by (home team, away team,
     date) -- stored on soccer_matches.api_match_id so re-runs don't re-resolve.
  3. For each match in scope: call matches/{id}/player-stats AND matches/{id}/lineups.
     Players not already in soccer_players are added on the fly (a historical match
     can reference a player who has since transferred off the current roster).
  4. venue (home/away) and season are set from our own match record; club_ga_per90 is
     the player's team's actual goals conceded in THAT match, from soccer_matches --
     not an API call.

Backfill depth is asymmetric by design (see FEATURE-011_REQUIREMENTS.md): player-stats
defaults to 3 seasons of history, lineups to 1 -- rosters turn over enough that older
lineups aren't a useful "who starts next" signal.

Usage:
    python import_club_player_stats.py --limit-matches 3 --dry-run   # validation
    python import_club_player_stats.py --season 2025                # stats + lineups
    python import_club_player_stats.py --season 2025 --skip-lineups # stats only
"""

import argparse
import sqlite3
import sys

from core.sports_db import (
    DATABASE_PATH,
    add_player,
    add_player_match_stats,
    add_player_match_lineup,
    set_match_api_id,
)
from core.thestatsapi import Client, TheStatsAPIError
from import_club_squads import resolve_competition, resolve_season_id, match_teams_to_api, load_db_teams


def parse_args():
    parser = argparse.ArgumentParser(description="Import club-league per-match player stats + lineups.")
    parser.add_argument("--league", default="Serie A")
    parser.add_argument("--season", type=int, default=2025,
                        help="Season used to select DB matches and label stats rows (default: 2025).")
    parser.add_argument("--search", default=None)
    parser.add_argument("--country", default=None)
    parser.add_argument("--competition-id")
    parser.add_argument("--team", help="Only process matches involving this DB team name.")
    parser.add_argument("--limit-matches", type=int, help="Only process the first N matches (validation run).")
    parser.add_argument("--skip-lineups", action="store_true",
                        help="Only pull player-stats, not lineups (e.g. for older backfill seasons).")
    parser.add_argument("--api-key")
    parser.add_argument("--max-requests", type=int, default=2000)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def load_db_matches(league, season, team_name=None):
    """Return rows for league/season matches, including any already-resolved api_match_id."""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    sql = """SELECT m.match_id, m.home_team_id, m.away_team_id, m.match_date, m.api_match_id,
                    m.home_score, m.away_score,
                    ht.name AS home_name, at.name AS away_name
             FROM soccer_matches m
             JOIN soccer_teams ht ON ht.team_id = m.home_team_id
             JOIN soccer_teams at ON at.team_id = m.away_team_id
             WHERE m.league = ? AND m.season = ?"""
    params = [league, season]
    if team_name:
        sql += " AND (ht.name = ? OR at.name = ?)"
        params.extend([team_name, team_name])
    sql += " ORDER BY m.match_date"
    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return rows


def resolve_match_api_ids(client, comp_id, season_id, db_matches, team_api_id, conn, dry_run=False):
    """Fill in api_match_id for any db_matches row missing it, by (home team, away
    team) against the API's match list -- NOT date. A mismatch was found between our
    soccer_matches dates and TheStatsAPI's utc_date for ~13% of Serie A 2025 matches
    (e.g. Cagliari-Como is 2026-03-07 in the API, 2026-03-08 in our DB); checked
    whether it was a timezone issue -- it isn't (the offsets scatter in both
    directions and cluster in the fixture-congested March-April window, which a
    timezone bug wouldn't produce; most likely some matches were rescheduled and the
    two sources disagree on which date to report). Matching on team pairing alone
    sidesteps the cause entirely: in a double round-robin league each (home, away)
    ordered pair occurs exactly once per season, so there's no ambiguity to resolve
    with a date anyway. Returns (n_resolved, n_unresolved).

    dry_run=True still computes and returns the resolution (for an honest preview) but
    does NOT call set_match_api_id -- this used to write regardless of --dry-run, a
    real bug caught when a season_id resolution mistake (see resolve_season_id) got
    persisted by a "dry" run before the mistake was noticed. Returns
    (n_resolved, n_unresolved, resolution) -- resolution is {match_id: api_match_id}
    for everything resolved THIS call (whether or not persisted), so a caller can
    preview the full downstream pipeline without a second API call or DB round trip."""
    needing = [m for m in db_matches if not m["api_match_id"]]
    if not needing:
        return 0, 0, {}

    api_matches = list(client.paginate("matches", {"competition_id": comp_id, "season_id": season_id}))
    resolution = match_db_matches_to_api_by_team_pairing(needing, team_api_id, api_matches)

    if not dry_run:
        for match_id, api_id in resolution.items():
            set_match_api_id(match_id, api_id, conn=conn)

    return len(resolution), len(needing) - len(resolution), resolution


def match_db_matches_to_api_by_team_pairing(db_matches, team_api_id, api_matches):
    """Pure matching logic behind resolve_match_api_ids, split out for testing without
    a live client: given DB match rows, a {db_team_id: api_team_id} map, and an
    already-fetched API match list, return {match_id: api_match_id} for every DB match
    whose (home, away) team pairing resolves to exactly one API match. A DB match
    whose teams don't both have a known api_team_id, or whose pairing isn't found in
    api_matches, is simply absent from the result (not an error)."""
    by_key = {}
    for am in api_matches:
        by_key[(am["home_team"]["id"], am["away_team"]["id"])] = am["id"]

    result = {}
    for m in db_matches:
        home_api = team_api_id.get(m["home_team_id"])
        away_api = team_api_id.get(m["away_team_id"])
        if not home_api or not away_api:
            continue
        api_id = by_key.get((home_api, away_api))
        if api_id:
            result[m["match_id"]] = api_id
    return result


def import_match(client, match_row, api_match_id, team_api_id_reverse, season,
                 skip_lineups, dry_run, conn):
    """Pull + store player-stats (always) and lineups (unless skipped) for one match.
    Returns (n_stats_rows, n_lineup_rows)."""
    home_id, away_id = match_row["home_team_id"], match_row["away_team_id"]

    stats_rows = client.get_data(f"matches/{api_match_id}/player-stats") or []

    # Club xGA = the OPPOSING team's total xG in this match -- the API has no
    # expected-goals-against field (checked 2026-08-02, no such field anywhere in the
    # player-stats payload), so this is derived the same way club_ga_per90 already is:
    # locally, from data already being pulled, not a separate API call. Needs a first
    # pass over all rows (both teams) before any row can be written, since a player's
    # xGA depends on their opponent's full-match total.
    team_xg = {home_id: 0.0, away_id: 0.0}
    for row in stats_rows:
        team_id = team_api_id_reverse.get(row.get("team_id"))
        if team_id in team_xg:
            xg = (row.get("shooting") or {}).get("expected_goals")
            if xg is not None:
                team_xg[team_id] += xg
    opponent_xg = {home_id: team_xg[away_id], away_id: team_xg[home_id]}

    n_stats = 0
    for row in stats_rows:
        api_team_id = row.get("team_id")
        team_id = team_api_id_reverse.get(api_team_id)
        if team_id is None:
            continue  # player on a team we couldn't resolve -- skip rather than guess
        venue = "home" if team_id == home_id else ("away" if team_id == away_id else None)

        # Club defense = the player's team's ACTUAL goals conceded in this match, from
        # our own soccer_matches -- not an API call (see module docstring). A single
        # match's goals-conceded doubles as its own "per-90" rate.
        if venue == "home":
            club_ga = match_row["away_score"]
        elif venue == "away":
            club_ga = match_row["home_score"]
        else:
            club_ga = None
        club_xga = opponent_xg.get(team_id)

        if not dry_run:
            player_id = add_player(team_id, row["player_name"], position=row.get("position"),
                                   api_player_id=row["player_id"], conn=conn)
            shooting = row.get("shooting") or {}
            add_player_match_stats(
                player_id, match_row["match_id"], season=season, venue=venue,
                minutes_played=row.get("minutes_played"),
                xg=shooting.get("expected_goals"),
                xg_per90=(shooting.get("expected_goals") / row["minutes_played"] * 90
                          if shooting.get("expected_goals") is not None and row.get("minutes_played") else None),
                goals=shooting.get("goals"),
                assists=(row.get("passing") or {}).get("assists"),
                club_ga_per90=club_ga,
                club_xga_per90=club_xga,
                source="thestatsapi",
            )
        n_stats += 1

    n_lineups = 0
    if not skip_lineups:
        lineup_data = client.get_data(f"matches/{api_match_id}/lineups")
        if lineup_data:
            for side, team_id in (("home", home_id), ("away", away_id)):
                side_data = lineup_data.get(side) or {}
                formation = side_data.get("formation")
                for started, players in ((True, side_data.get("starting_xi") or []),
                                         (False, side_data.get("substitutes") or [])):
                    for p in players:
                        if not dry_run:
                            player_id = add_player(team_id, p["name"], position=p.get("position"),
                                                   api_player_id=p["id"], conn=conn)
                            add_player_match_lineup(player_id, match_row["match_id"], team_id,
                                                    started, position=p.get("position"),
                                                    formation=formation, conn=conn)
                        n_lineups += 1

    return n_stats, n_lineups


def main():
    args = parse_args()
    search = args.search or args.league
    try:
        client = Client(api_key=args.api_key, max_requests=args.max_requests)
    except TheStatsAPIError as exc:
        sys.exit(str(exc))

    conn = sqlite3.connect(DATABASE_PATH)
    db_teams = load_db_teams(args.league, args.season)
    db_matches = load_db_matches(args.league, args.season, team_name=args.team)
    if not db_matches:
        sys.exit(f"No matches found for {args.league} season {args.season}.")
    print(f"DB matches in scope: {len(db_matches)}")

    try:
        comp = resolve_competition(client, args.competition_id, search, args.country)
        comp_id = comp["id"]
        season_id = resolve_season_id(client, comp_id, args.season)
        print(f"Competition: {comp_id}  {comp['name']}  season={season_id}")

        api_teams = list(client.paginate("teams", {"competition_id": comp_id, "season_id": season_id}))
        matched, unmatched_db, unmatched_api = match_teams_to_api(db_teams, api_teams)
        if unmatched_db:
            print(f"UNMATCHED DB teams (skipping their matches): {unmatched_db}")
        team_api_id = {tid: api_team["id"] for tid, _, api_team in matched}
        team_api_id_reverse = {v: k for k, v in team_api_id.items()}

        resolved, unresolved, new_resolution = resolve_match_api_ids(
            client, comp_id, season_id, db_matches, team_api_id, conn, dry_run=args.dry_run)
        print(f"Match-id mapping: {resolved} newly resolved, {unresolved} unresolved this run")

        # Merge already-persisted api_match_id with this run's resolution in memory --
        # works identically for a real run (both sources agree) and a dry run (nothing
        # was persisted, so this is the only place the new resolution is visible).
        matches = [dict(m, api_match_id=m["api_match_id"] or new_resolution.get(m["match_id"]))
                   for m in db_matches]
        matches = [m for m in matches if m["api_match_id"]]
        if args.limit_matches:
            matches = matches[:args.limit_matches]
        print(f"Matches to process: {len(matches)}\n")

        total_stats = total_lineups = 0
        for i, m in enumerate(matches, 1):
            n_stats, n_lineups = import_match(client, m, m["api_match_id"], team_api_id_reverse,
                                              args.season, args.skip_lineups, args.dry_run, conn)
            total_stats += n_stats
            total_lineups += n_lineups
            print(f"  {m['home_name']:<22} vs {m['away_name']:<22} "
                  f"stats={n_stats:>2} lineups={n_lineups:>2}")
            if i % 25 == 0:
                print(f"  ...{i}/{len(matches)} matches, {client.requests_made} requests", flush=True)

        print(f"\n{'(dry-run) ' if args.dry_run else ''}Matches processed: {len(matches)}  "
              f"stat rows: {total_stats}  lineup rows: {total_lineups}")
    except TheStatsAPIError as exc:
        print(f"\nAborted: {exc}")
    finally:
        conn.close()
        print(f"API requests used: {client.requests_made}")


if __name__ == "__main__":
    main()
