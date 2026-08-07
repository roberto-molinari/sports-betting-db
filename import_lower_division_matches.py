"""
Import a lower-division league's completed matches from TheStatsAPI into soccer_matches
-- the missing first step for pulling player-level history for teams/players whose prior
season was in a division we've never tracked before (e.g. a newly-promoted Serie A
team's Serie B season). See BUG-010 (cross-league player-history design, 2026-08-03).

Serie A's own soccer_matches rows come from football-data.co.uk CSVs (import_serie_a_
odds.py / update_serie_a_results.py), not TheStatsAPI -- there was previously no "pull
matches from TheStatsAPI" path for any league. This script is that path, scoped to
lower divisions where we don't need betting odds (we don't bet on Serie B): once matches
exist here, the EXISTING league-agnostic pipeline (import_club_squads.py,
import_club_player_stats.py) works unmodified against them via --league/--competition-id.

Only imports FINISHED matches -- an unplayed fixture has no player-stats to eventually
pull, so there's nothing useful in it for this purpose. Idempotent: an already-imported
match (matched by TheStatsAPI's own match id, stored on api_match_id) is skipped, not
duplicated. Deliberately NOT (league, season, home_team, away_team) -- unlike Serie A's
plain double round-robin, a lower division can have a promotion PLAYOFF bracket after
the regular season where the same two teams meet again with the same venue (found
2026-08-03 backfilling Serie B 2024: e.g. Cremonese/Spezia met in the regular season
AND in the playoff final, same home/away; a team-pairing dedup key silently drops the
second, real, distinct match -- 9 playoff matches lost this way before the fix,
including Cremonese's actual promotion-deciding games).

Team resolution: TheStatsAPI's own team names are used directly (no normalize_team_name
needed, unlike the CSV-sourced scripts -- both this script and the existing Serie A
club-league imports draw from the SAME API, so naming is already consistent across
seasons/divisions, e.g. "Cremonese" here matches the "Cremonese" row already in
soccer_teams from their Serie A seasons; ensure_soccer_team's exact-name lookup reuses
that row rather than creating a duplicate). A genuinely new club (never seen in any
division we've imported) gets a fresh soccer_teams row tagged with THIS division's name
-- see BUG-010's note on soccer_teams.name being globally UNIQUE and `league` not being
authoritative per season; a future promotion of such a club may need that tag updated.

Usage:
    python import_lower_division_matches.py --league "Serie B" --competition-id comp_5450 --season 2024
    python import_lower_division_matches.py --league "Serie B" --competition-id comp_5450 --season 2023 --dry-run
"""

import argparse
import sqlite3
import sys

from core.sports_db import DATABASE_PATH, ensure_soccer_team, add_soccer_match, \
    set_match_api_id, update_soccer_match_result
from core.thestatsapi import Client, TheStatsAPIError
from import_club_squads import resolve_competition, resolve_season_id


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--league", required=True,
                        help="League name to store on soccer_matches/soccer_teams, "
                             "e.g. 'Serie B'.")
    parser.add_argument("--season", type=int, required=True,
                        help="Season start_year, e.g. 2024 = the 2024-25 season.")
    parser.add_argument("--search", default=None,
                        help="TheStatsAPI competition search term (default: --league value).")
    parser.add_argument("--country", default=None)
    parser.add_argument("--competition-id")
    parser.add_argument("--api-key")
    parser.add_argument("--max-requests", type=int, default=200)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def find_existing_match_id(conn, api_match_id):
    cur = conn.cursor()
    cur.execute("SELECT match_id FROM soccer_matches WHERE api_match_id = ?", (api_match_id,))
    row = cur.fetchone()
    return row[0] if row else None


def main():
    args = parse_args()
    search = args.search or args.league
    try:
        client = Client(api_key=args.api_key, max_requests=args.max_requests)
    except TheStatsAPIError as exc:
        sys.exit(str(exc))

    conn = sqlite3.connect(DATABASE_PATH)
    team_id_by_api_id = {}

    try:
        comp = resolve_competition(client, args.competition_id, search, args.country)
        comp_id = comp["id"]
        season_id = resolve_season_id(client, comp_id, args.season)
        print(f"Competition: {comp_id}  {comp['name']} ({comp.get('country')})  "
              f"season={season_id}")

        api_matches = list(client.paginate("matches", {"competition_id": comp_id,
                                                        "season_id": season_id}))
        finished = [m for m in api_matches if m.get("status") == "finished"]
        print(f"API matches: {len(api_matches)}  finished: {len(finished)}")

        created = skipped = 0
        for m in finished:
            home_api, away_api = m["home_team"], m["away_team"]
            for api_team in (home_api, away_api):
                if api_team["id"] not in team_id_by_api_id:
                    team_id_by_api_id[api_team["id"]] = ensure_soccer_team(
                        api_team["name"], args.league, comp.get("country"))
            home_id = team_id_by_api_id[home_api["id"]]
            away_id = team_id_by_api_id[away_api["id"]]

            existing_id = find_existing_match_id(conn, m["id"])
            if existing_id:
                skipped += 1
                continue

            score = m.get("score") or {}
            home_score, away_score = score.get("home"), score.get("away")
            if args.dry_run:
                created += 1
                continue

            match_id = add_soccer_match(args.league, args.season, home_id, away_id,
                                        m["utc_date"], status="scheduled")
            set_match_api_id(match_id, m["id"], conn=conn)
            if home_score is not None and away_score is not None:
                update_soccer_match_result(match_id, home_score, away_score)
            created += 1

        print(f"\n{'(dry-run) ' if args.dry_run else ''}Matches created: {created}  "
              f"already present (skipped): {skipped}")
    except TheStatsAPIError as exc:
        print(f"\nAborted: {exc}")
    finally:
        conn.close()
        print(f"API requests used: {client.requests_made}")


if __name__ == "__main__":
    main()
