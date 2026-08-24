"""
FEATURE-019: season-start kickoff sequence, run once per season across every
tracked division (5 top-flight leagues + their 5 feeders, core.leagues.LEAGUES).

For each division, in order:
  1. Check the new season resolves at all on its data source. A division whose
     season isn't published yet is skipped cleanly (not an error) -- re-running
     this script later just naturally picks it up once it's live.
  2. Sync league membership: fetch the season's real team list and compare it
     against soccer_teams.league for each team. A mismatch is a genuine
     promotion/relegation -- logged plainly and applied. This is the ONLY place
     in the codebase allowed to change a team's league label (see BUGS.md
     FEATURE-019 -- ensure_soccer_team() itself deliberately never does this,
     to avoid an order-dependent correctness risk).
  3. Import that season's fixtures, via the existing scripts
     (import_league_matches.py for the 8 TheStatsAPI divisions,
     update_serie_a_results.py for Serie A).
  4. Refresh squads (import_club_squads.py) -- transfer windows close right
     around season start, so last season's rosters are meaningfully stale.
  5. List any teams whose odds data didn't match this division's team names
     (a real, recurring need -- newly promoted teams often appear under a
     slightly different name in the odds sources). Reported, not auto-fixed.
  6. Check that this division's odds sources are actually returning data yet.

After every division is processed, one final pass: any team whose CURRENT
stored league belongs to a division that WAS processed this run, but who
wasn't seen in ANY division's team list this run, gets marked with the
"dropped out of everything we track" sentinel -- distinguishing "genuinely
not promoted/relegated into anything we follow" from "we just haven't
checked yet" (a division skipped in step 1 leaves its teams untouched, not
incorrectly marked unknown).

Usage:
    python season_kickoff.py --season 2026
    python season_kickoff.py --season 2026 --skip-squads --skip-fixtures
"""
import argparse
import os
import subprocess
import sys

from core.leagues import LEAGUES, has_odds_source
from core.sports_db import DATABASE_PATH, ensure_soccer_team
from core.team_name_maps import canonical_team_name
from core.thestatsapi import Client
from import_club_squads import resolve_season_id
import import_league_betting_odds as ilbo
import update_serie_a_results as usr

SENTINEL_LEAGUE = "(unknown - not seen this season)"


def season_team_list(client, conn, league, season):
    """Return [(name, ...)] for every team in `league` this season, from the
    real data source -- TheStatsAPI's teams endpoint for the 8 divisions that
    have one, football-data.org's teams endpoint for Serie A (the one division
    still on that source). Returns None (not an empty list) if the season
    doesn't resolve at all yet -- the caller's signal to skip this division
    cleanly rather than treating "nothing published yet" as "zero teams"."""
    cfg = LEAGUES[league]
    if cfg["thestatsapi_competition_id"] is None:
        api_key = os.getenv("FOOTBALL_DATA_API_KEY")
        if not api_key:
            print(f"  [{league}] FOOTBALL_DATA_API_KEY not set, skipping.")
            return None
        try:
            teams, _ = usr.fetch_season_data(api_key, season)
        except Exception as e:
            print(f"  [{league}] season {season} not available yet ({e}), skipping.")
            return None
        return [usr.API_TEAM_NAME_MAP.get(t["name"], t["name"]) for t in teams]

    try:
        season_id = resolve_season_id(client, cfg["thestatsapi_competition_id"], season)
    except SystemExit as e:
        print(f"  [{league}] season {season} not available yet ({e}), skipping.")
        return None
    api_teams = list(client.paginate(
        "teams", {"competition_id": cfg["thestatsapi_competition_id"], "season_id": season_id}))
    return [t["name"] for t in api_teams]


def sync_league_membership(conn, league, names):
    """Diff `names` (this season's real team list for `league`) against
    soccer_teams, applying and logging any promotion/relegation. Returns the
    set of team_ids confirmed to be in `league` this season."""
    cur = conn.cursor()
    seen_ids = set()
    for name in names:
        cur.execute("SELECT team_id, league FROM soccer_teams WHERE name = ?", (name,))
        row = cur.fetchone()
        if row is None:
            team_id = ensure_soccer_team(name, league, LEAGUES[league]["country"])
            print(f"  + new team: {name} -> {league}")
        else:
            team_id, stored_league = row
            if stored_league != league:
                print(f"  {name}: {stored_league} -> {league}")
                cur.execute("UPDATE soccer_teams SET league = ? WHERE team_id = ?", (league, team_id))
                conn.commit()
        seen_ids.add(team_id)
    return seen_ids


def import_fixtures(league, season, allow_overwrite=False):
    """allow_overwrite only affects the import_league_matches.py path (the 4
    newer leagues) -- it threads through to that script's own --allow-overwrite
    flag, applying a detected scheduled->completed/score conflict instead of
    only reporting it (see import_league_matches.py's module docstring).
    update_serie_a_results.py (Serie A's own path) has no such gate -- it always
    applies fetched results directly, so this parameter is a no-op there.
    Default False preserves this function's original report-only behavior for
    existing callers (e.g. season_kickoff.py's own bootstrap run, where a human
    reviewing conflicts before they're applied is the safer default); pass True
    for a caller like club_league_scorecard.py's refresh step, where applying a
    newly-completed match's real score is the entire point of "refresh" -- see
    BUGS.md's entry on picks going ungraded because this was missed."""
    if LEAGUES[league]["thestatsapi_competition_id"] is None:
        subprocess.run([sys.executable, "update_serie_a_results.py", "--season", str(season)],
                       check=True)
    else:
        cmd = [sys.executable, "import_league_matches.py", "--league", league, "--season", str(season)]
        if allow_overwrite:
            cmd.append("--allow-overwrite")
        subprocess.run(cmd, check=True)


# Serie A has no thestatsapi_competition_id in core/leagues.py (deliberately --
# it stays on football-data.org for MATCHES), but its SQUAD data still comes
# from TheStatsAPI under a real id that a bare name search can't disambiguate
# ("Serie A" also matches "LigaPro Serie A (Ecuador)"). Confirmed live
# 2026-08-19; not in the shared registry since that field's meaning there is
# specifically about the match-import source, and overloading it would make
# import_league_matches.py wrongly skip the football-data.org path for
# Serie A's matches.
SERIE_A_SQUAD_COMPETITION_ID = "comp_5840"


def refresh_squads(league, season):
    cmd = [sys.executable, "import_club_squads.py", "--league", league, "--season", str(season)]
    if league == "Serie A":
        cmd += ["--competition-id", SERIE_A_SQUAD_COMPETITION_ID]
    subprocess.run(cmd, check=True)


def list_odds_name_gaps(conn, league):
    """Reuses import_league_betting_odds.py's own team-matching logic (no
    duplication) to report which teams in this season's Odds API events
    didn't match our canonical names -- read-only, doesn't import anything."""
    if not has_odds_source(league) or LEAGUES[league]["odds_api_sport_key"] is None:
        return []
    if not os.getenv("THE_ODDS_API_KEY"):
        print(f"  [{league}] THE_ODDS_API_KEY not set, skipping name-gap check.")
        return []
    team_map = ilbo.load_team_map(conn, league)
    try:
        events = ilbo.fetch_odds_api_events(league)
    except SystemExit as e:
        print(f"  [{league}] odds check failed: {e}")
        return []
    gaps = []
    for event in events:
        for raw in (event.get("home_team"), event.get("away_team")):
            canonical = canonical_team_name(league, raw)
            if team_map.get(canonical) is None:
                gaps.append(raw)
    return sorted(set(gaps))


def check_odds_source_live(league):
    """Cheap availability check -- does The Odds API return any events for
    this division's current season yet? Not a full odds import."""
    if not has_odds_source(league) or LEAGUES[league]["odds_api_sport_key"] is None:
        print(f"  [{league}] no odds source configured (feeder division).")
        return
    if not os.getenv("THE_ODDS_API_KEY"):
        print(f"  [{league}] THE_ODDS_API_KEY not set, cannot check.")
        return
    try:
        events = ilbo.fetch_odds_api_events(league)
        print(f"  [{league}] odds source live: {len(events)} events available.")
    except SystemExit as e:
        print(f"  [{league}] odds source NOT available: {e}")


def mark_unknown_teams(conn, processed_leagues, all_seen_ids):
    """Final pass, after every division this run: a team whose CURRENT stored
    league is one of the divisions we actually processed this run, but who
    wasn't in ANY division's team list this run, has dropped out of
    everything we track -- mark it with the sentinel rather than leaving a
    stale real league name in place. A team under a league we DIDN'T process
    this run (e.g. skipped in step 1) is left untouched -- no new information
    about them, so no change."""
    if not processed_leagues:
        return
    cur = conn.cursor()
    placeholders = ",".join("?" * len(processed_leagues))
    cur.execute(f"SELECT team_id, name, league FROM soccer_teams WHERE league IN ({placeholders})",
               list(processed_leagues))
    for team_id, name, league in cur.fetchall():
        if team_id not in all_seen_ids:
            print(f"  {name}: {league} -> {SENTINEL_LEAGUE} (not seen in any tracked division)")
            cur.execute("UPDATE soccer_teams SET league = ? WHERE team_id = ?",
                       (SENTINEL_LEAGUE, team_id))
    conn.commit()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--skip-fixtures", action="store_true", help="Skip step 3 (fixture import).")
    parser.add_argument("--skip-squads", action="store_true", help="Skip step 4 (squad refresh).")
    args = parser.parse_args()

    import sqlite3
    conn = sqlite3.connect(DATABASE_PATH)
    client = Client()

    processed_leagues = []
    all_seen_ids = set()

    for league in LEAGUES:
        print(f"\n=== {league} (season {args.season}) ===")
        names = season_team_list(client, conn, league, args.season)
        if names is None:
            continue
        processed_leagues.append(league)

        print(f"-- step 2: league membership ({len(names)} teams) --")
        seen_ids = sync_league_membership(conn, league, names)
        all_seen_ids |= seen_ids

        if not args.skip_fixtures:
            print("-- step 3: fixtures --")
            import_fixtures(league, args.season)

        if not args.skip_squads:
            print("-- step 4: squads --")
            refresh_squads(league, args.season)

        print("-- step 5: odds name-gap check --")
        gaps = list_odds_name_gaps(conn, league)
        if gaps:
            print(f"  {len(gaps)} unmatched team name(s): {', '.join(gaps)}")
        else:
            print("  no gaps found.")

        print("-- step 6: odds-source availability --")
        check_odds_source_live(league)

    print("\n=== final pass: teams dropped from everything we track ===")
    mark_unknown_teams(conn, processed_leagues, all_seen_ids)

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
