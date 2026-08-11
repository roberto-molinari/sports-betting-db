"""
Import a league's matches from TheStatsAPI into the soccer_matches database table --
the canonical team/match ingestion path for every league except Serie A (which stays
on football-data.org until the tracked fast-follow migration -- see BUGS.md).

--league is the only required identifier; every other per-source detail (competition
id, whether odds are tracked) is looked up from core/leagues.py's LEAGUES registry.

Scheduled vs finished-only: a league with a configured odds source (core.leagues.
has_odds_source) imports ALL matches, since live picks need scheduled fixtures to
exist ahead of kickoff. A league without one (feeder divisions) stays finished-only
-- an unplayed lower-division fixture has no player-stats to pull yet.

Re-sync is minimal-API-call by design: only makes a call at all if some already-
imported match is 'scheduled' with a kickoff RESYNC_LOOKBACK_HOURS+ in the past (a
real "might have finished by now" candidate); one bulk paginated call then covers
the whole league/season.

Conflict-safe writes: for a match that already exists (matched by TheStatsAPI's own
id), every fetched field is compared against what's stored. Identical -> no write.
Different -> logged (old value vs. new) but not applied unless --allow-overwrite --
a source correcting itself after the fact should be a visible, reviewed event.

Match dedup is by THESTATSAPI'S OWN match id, not (league, season, home, away) --
a division's promotion playoff bracket can rematch the same two teams at the same
venue after the regular season, which a team-pairing key would wrongly collapse
into one match (real data loss, found 2026-08-03 backfilling Serie B).

Team resolution: TheStatsAPI's own team names are used as-is (no normalizing needed
-- squads/player-stats draw from the same API, so naming is already consistent
across seasons/divisions). ensure_soccer_team() reuses a club's existing row across
a division change by name; see core/sports_db.py's country-scoped collision check
for the cross-country case that doesn't cover.

Usage:
    python import_league_matches.py --league "Premier League" --season 2024
    python import_league_matches.py --league "Serie B" --season 2024 --dry-run
    python import_league_matches.py --league "Premier League" --season 2025 --allow-overwrite
"""

import argparse
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

from core.leagues import LEAGUES, has_odds_source
from core.sports_db import DATABASE_PATH, ensure_soccer_team, add_soccer_match, \
    set_thestatsapi_match_id, update_soccer_match_result
from core.thestatsapi import Client, TheStatsAPIError
from import_club_squads import resolve_season_id

RESYNC_LOOKBACK_HOURS = 4


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--league", required=True, choices=sorted(LEAGUES),
                        help="League name, must be a key in core/leagues.py's LEAGUES registry.")
    parser.add_argument("--season", type=int, required=True,
                        help="Season start_year, e.g. 2024 = the 2024-25 season.")
    parser.add_argument("--api-key", help="Override the THE_STATS_API_API_KEY env var.")
    parser.add_argument("--max-requests", type=int, default=200,
                        help="Abort if more than this many TheStatsAPI requests are issued "
                             "(guards the quota; default 200).")
    parser.add_argument("--allow-overwrite", action="store_true",
                        help="Apply flagged data conflicts (see module docstring) instead of only reporting them.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would be created/changed without writing to the database.")
    return parser.parse_args()


def find_existing_match(conn, thestatsapi_match_id):
    """Return (match_id, home_score, away_score, match_date, match_status) for this
    TheStatsAPI match id if we already have it, else None."""
    cur = conn.cursor()
    cur.execute(
        """SELECT match_id, home_score, away_score, match_date, match_status
           FROM soccer_matches WHERE thestatsapi_match_id = ?""",
        (thestatsapi_match_id,))
    return cur.fetchone()


def compute_conflicts(existing, api_status, home_score, away_score, api_date):
    """Compare an existing (match_id, home_score, away_score, match_date,
    match_status) row against freshly-fetched API values; return a list of
    (field, old_value, new_value) tuples for anything that actually differs (see
    module docstring's "Conflict-safe writes"). Empty list means identical -- the
    common case on a routine re-run."""
    _, db_home, db_away, db_date, db_status = existing
    diffs = []
    if db_status != api_status:
        diffs.append(("match_status", db_status, api_status))
    if api_status == "completed" and (db_home != home_score or db_away != away_score):
        diffs.append(("score", f"{db_home}-{db_away}", f"{home_score}-{away_score}"))
    if db_date != api_date:
        diffs.append(("match_date", db_date, api_date))
    return diffs


def any_match_due_for_resync(conn, league, season):
    """True if any already-imported match in this league/season is still 'scheduled'
    with a kickoff RESYNC_LOOKBACK_HOURS+ in the past -- the trigger for making an API
    call at all on a re-run (see module docstring)."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=RESYNC_LOOKBACK_HOURS)).isoformat()
    cur = conn.cursor()
    cur.execute(
        """SELECT 1 FROM soccer_matches
           WHERE league = ? AND season = ? AND match_status = 'scheduled' AND match_date <= ?
           LIMIT 1""",
        (league, season, cutoff))
    return cur.fetchone() is not None


def main():
    args = parse_args()
    entry = LEAGUES[args.league]
    comp_id = entry["thestatsapi_competition_id"]
    if comp_id is None:
        sys.exit(f"'{args.league}' has no thestatsapi_competition_id configured in core/leagues.py "
                 f"(it's still sourced from football-data.org -- see BUGS.md's Serie A fast-follow entry).")
    import_all_statuses = has_odds_source(args.league)

    conn = sqlite3.connect(DATABASE_PATH)

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM soccer_matches WHERE league = ? AND season = ?",
                (args.league, args.season))
    already_has_matches = cur.fetchone()[0] > 0
    if already_has_matches and import_all_statuses and not any_match_due_for_resync(conn, args.league, args.season):
        print(f"{args.league} {args.season}: no scheduled match is {RESYNC_LOOKBACK_HOURS}+ hours past "
              f"kickoff yet -- nothing could have changed, skipping API call.")
        conn.close()
        return

    try:
        client = Client(api_key=args.api_key, max_requests=args.max_requests)
    except TheStatsAPIError as exc:
        sys.exit(str(exc))

    team_id_by_api_id = {}
    try:
        season_id = resolve_season_id(client, comp_id, args.season)
        print(f"Competition: {comp_id}  {args.league} ({entry['country']})  season={season_id}")

        api_matches = list(client.paginate("matches", {"competition_id": comp_id, "season_id": season_id}))
        wanted = api_matches if import_all_statuses else [m for m in api_matches if m.get("status") == "finished"]
        print(f"API matches: {len(api_matches)}  imported-scope "
              f"({'all statuses' if import_all_statuses else 'finished only'}): {len(wanted)}")

        created = unchanged = conflicts = applied = 0
        for m in wanted:
            home_api, away_api = m["home_team"], m["away_team"]
            for api_team in (home_api, away_api):
                if api_team["id"] not in team_id_by_api_id:
                    team_id_by_api_id[api_team["id"]] = ensure_soccer_team(
                        api_team["name"], args.league, entry["country"])
            home_id = team_id_by_api_id[home_api["id"]]
            away_id = team_id_by_api_id[away_api["id"]]

            score = m.get("score") or {}
            home_score, away_score = score.get("home"), score.get("away")
            api_status = "completed" if m.get("status") == "finished" else "scheduled"

            existing = find_existing_match(conn, m["id"])
            if existing is None:
                if args.dry_run:
                    created += 1
                    continue
                match_id = add_soccer_match(args.league, args.season, home_id, away_id,
                                            m["utc_date"], status="scheduled")
                set_thestatsapi_match_id(match_id, m["id"], conn=conn)
                if home_score is not None and away_score is not None:
                    update_soccer_match_result(match_id, home_score, away_score)
                created += 1
                continue

            match_id = existing[0]
            diffs = compute_conflicts(existing, api_status, home_score, away_score, m["utc_date"])

            if not diffs:
                unchanged += 1
                continue

            conflicts += 1
            action = "applying (--allow-overwrite)" if args.allow_overwrite else "NOT applied, use --allow-overwrite"
            for field, old, new in diffs:
                print(f"  CONFLICT match_id={match_id} ({args.league} {home_api['name']} vs "
                      f"{away_api['name']}): {field} stored={old!r} api={new!r} -- {action}")
            if args.allow_overwrite and not args.dry_run:
                if home_score is not None and away_score is not None:
                    update_soccer_match_result(match_id, home_score, away_score)
                applied += 1

        applied_note = f"  applied={applied}" if args.allow_overwrite else ""
        print(f"\n{'(dry-run) ' if args.dry_run else ''}created={created}  unchanged={unchanged}  "
              f"conflicts={conflicts}{applied_note}")
    except TheStatsAPIError as exc:
        print(f"\nAborted: {exc}")
    finally:
        conn.close()
        print(f"API requests used: {client.requests_made}")


if __name__ == "__main__":
    main()
