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

A still-SCHEDULED match getting its first real status/score (is_routine_completion())
is NOT logged as a conflict -- that's the expected, everyday outcome of a refresh, not
a disagreement -- though it's still gated behind --allow-overwrite the same as any
other write, and counted in the run summary (results_recorded=N). Anything else that
differs (a score changing on an ALREADY-completed match -- a source correcting itself
after the fact -- or any match_date change -- a postponement) IS logged loudly
(CONFLICT) and not applied unless --allow-overwrite: that genuinely deserves a human's
attention. (2026-08-24, BUGS.md: this split was added after the routine case's
all-caps CONFLICT logging read as an error to a first-time user.)

Match dedup is by THESTATSAPI'S OWN match id, not (league, season, home, away) --
a division's promotion playoff bracket can rematch the same two teams at the same
venue after the regular season, which a team-pairing key would wrongly collapse
into one match (real data loss, found 2026-08-03 backfilling Serie B).

Duplicate-fixture detection (2026-08-23, BUGS.md): a NEW thestatsapi_match_id for
a team pairing we already have a DIFFERENT match id for, within
DUPLICATE_FIXTURE_TOLERANCE_DAYS, is flagged loudly and NOT imported -- the source
itself can serve two conflicting records for one real match (same competition/
season/matchday/kickoff instant, reversed home/away). This is deliberately not
auto-resolved (see find_conflicting_pairing()'s docstring for the real incident
this came from): which record is correct isn't decidable from our data alone, and
getting HOME/AWAY backwards silently corrupts every pick posted off that match.

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
    set_thestatsapi_match_id, update_soccer_match_result, update_soccer_match_date
from core.thestatsapi import Client, TheStatsAPIError
from import_club_squads import resolve_season_id

RESYNC_LOOKBACK_HOURS = 4
DUPLICATE_FIXTURE_TOLERANCE_DAYS = 1


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


def _dates_equal(db_date, api_date):
    """Compare two ISO-ish timestamp strings by actual instant, not literal text --
    found live 2026-09-04 migrating Serie A: its rows (previously sourced from
    football-data.org, various historical ingestion passes) store match_date in a
    handful of formatting variants ("...T18:45:00Z", "...T18:45:00+00:00",
    "...T00:00:00Z" for date-only rows) that don't match TheStatsAPI's own
    ("...T18:45:00.000Z") byte-for-byte despite being the identical instant --
    every one of Serie A's 60 currently-stamped matches falsely flagged as a
    match_date CONFLICT on this script's very first real run for that league.
    Falls back to a literal string compare if either side doesn't parse (never
    silently treats a genuinely malformed date as equal)."""
    if db_date == api_date:
        return True
    try:
        return datetime.fromisoformat(db_date) == datetime.fromisoformat(api_date)
    except ValueError:
        return False


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
    if not _dates_equal(db_date, api_date):
        diffs.append(("match_date", db_date, api_date))
    return diffs


def is_routine_completion(existing, diffs):
    """True when a diff set is nothing more than a still-scheduled match
    getting its FIRST real result -- the normal, expected outcome of a
    refresh, not a source disagreeing with itself. False for anything that
    actually deserves a human's attention: a score changing on a match that
    was ALREADY completed (a correction), or a match_date changing at all
    (a postponement) -- see the module docstring's "Conflict-safe writes".

    Added 2026-08-24 (BUGS.md) after CONFLICT-labeled, all-caps console
    output for this routine case read as an error to a first-time user, who
    reasonably asked why an EXPECTED event was being shouted about at all."""
    _, _, _, _, db_status = existing
    diff_fields = {field for field, _, _ in diffs}
    return db_status == "scheduled" and diff_fields <= {"match_status", "score"}


def find_conflicting_pairing(conn, league, season, home_id, away_id, api_match_id, api_date):
    """A DIFFERENT thestatsapi_match_id for the SAME two teams within
    DUPLICATE_FIXTURE_TOLERANCE_DAYS of this one -- two teams playing each other
    twice within a single day is never legitimate (unlike a real home-leg/away-leg
    pair, which is always weeks or months apart), so this can only be a genuine
    source-data duplicate, not a rematch (see test_find_existing_match_does_not_
    match_on_team_pairing_alone for why team-pairing alone is otherwise NOT used
    as a dedup key).

    Found live 2026-08-23: TheStatsAPI served two conflicting records for the same
    real Ligue 1 matchday-1 Rennes/PSG fixture -- same competition/season/matchday/
    kickoff instant, reversed home/away, one wrongly flagged is_neutral=true. Our
    ingestion had silently kept the wrong one (PSG home) until a downstream odds-
    import mismatch surfaced it days later -- this check catches it at import time
    instead. Returns the conflicting (match_id, thestatsapi_match_id, home_team_id,
    away_team_id, match_date, match_status) row, or None."""
    api_dt = datetime.fromisoformat(api_date.replace("Z", "+00:00"))
    window_start = (api_dt - timedelta(days=DUPLICATE_FIXTURE_TOLERANCE_DAYS)).isoformat()
    window_end = (api_dt + timedelta(days=DUPLICATE_FIXTURE_TOLERANCE_DAYS)).isoformat()
    cur = conn.cursor()
    cur.execute(
        """SELECT match_id, thestatsapi_match_id, home_team_id, away_team_id, match_date, match_status
           FROM soccer_matches
           WHERE league = ? AND season = ?
             AND ((home_team_id = ? AND away_team_id = ?) OR (home_team_id = ? AND away_team_id = ?))
             AND thestatsapi_match_id IS NOT NULL AND thestatsapi_match_id != ?
             AND match_date >= ? AND match_date <= ?""",
        (league, season, home_id, away_id, away_id, home_id, api_match_id, window_start, window_end))
    return cur.fetchone()


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
        sys.exit(f"'{args.league}' has no thestatsapi_competition_id configured in core/leagues.py.")
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

        created = unchanged = conflicts = applied = duplicate_fixtures = results_recorded = 0
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
                dup = find_conflicting_pairing(conn, args.league, args.season, home_id, away_id,
                                               m["id"], m["utc_date"])
                if dup is not None:
                    duplicate_fixtures += 1
                    dup_match_id, dup_api_id, dup_home_id, dup_away_id, dup_date, dup_status = dup
                    dup_orientation = "same" if (dup_home_id, dup_away_id) == (home_id, away_id) else "REVERSED"
                    print(f"  DUPLICATE FIXTURE: {args.league} {home_api['name']} vs {away_api['name']} "
                          f"on {m['utc_date']} (api_id={m['id']}, is_neutral={m.get('is_neutral')}) "
                          f"conflicts with existing match_id={dup_match_id} (api_id={dup_api_id}, "
                          f"date={dup_date}, status={dup_status}, home/away {dup_orientation}) -- "
                          f"the source is serving two records for what looks like ONE real match. "
                          f"NOT imported -- verify externally which record is correct (e.g. actual "
                          f"venue) before touching either row; see BUGS.md.")
                    continue
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

            if is_routine_completion(existing, diffs):
                # Expected outcome of a refresh, not a disagreement -- no
                # per-match noise (BUGS.md, 2026-08-24). Still counted below.
                results_recorded += 1
                if args.allow_overwrite and not args.dry_run:
                    if home_score is not None and away_score is not None:
                        update_soccer_match_result(match_id, home_score, away_score)
                    applied += 1
                continue

            conflicts += 1
            action = "applying (--allow-overwrite)" if args.allow_overwrite else "NOT applied, use --allow-overwrite"
            for field, old, new in diffs:
                print(f"  CONFLICT match_id={match_id} ({args.league} {home_api['name']} vs "
                      f"{away_api['name']}): {field} stored={old!r} api={new!r} -- {action}")
            if args.allow_overwrite and not args.dry_run:
                diff_fields = {field for field, _, _ in diffs}
                if "score" in diff_fields and home_score is not None and away_score is not None:
                    update_soccer_match_result(match_id, home_score, away_score)
                if "match_date" in diff_fields:
                    update_soccer_match_date(match_id, m["utc_date"])
                applied += 1

        applied_note = f"  applied={applied}" if args.allow_overwrite else ""
        print(f"\n{'(dry-run) ' if args.dry_run else ''}created={created}  unchanged={unchanged}  "
              f"results_recorded={results_recorded}  conflicts={conflicts}  "
              f"duplicate_fixtures={duplicate_fixtures}{applied_note}")
        if results_recorded and not args.allow_overwrite:
            print(f"  {results_recorded} match(es) finished and have a real score available -- "
                  f"rerun with --allow-overwrite to record it (not a conflict, just not yet applied).")
        if duplicate_fixtures:
            print(f"  {duplicate_fixtures} DUPLICATE FIXTURE(S) found and skipped -- see details above, "
                  f"resolve manually before they can block a real odds import (BUGS.md).")
    except TheStatsAPIError as exc:
        print(f"\nAborted: {exc}")
    finally:
        conn.close()
        print(f"API requests used: {client.requests_made}")


if __name__ == "__main__":
    main()
