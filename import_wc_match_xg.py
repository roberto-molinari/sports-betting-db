"""
FEATURE-008: pull external (TheStatsAPI) per-match xG for POST-HOC COMPARISON ONLY.

HARD CONSTRAINT (user, 2026-06-28): the model's own xG
(soccer_wc_player_stats.xg_per90) is the source of truth for anything the model
does. External xG must NEVER be written to soccer_wc_matches or any other
core-workflow table -- it's stored in its own table, soccer_wc_external_xg, used
only for after-the-fact "was this a good pick or bad variance?" comparison
(e.g. against BUG-004's over-skew finding).

Feasibility confirmed live 2026-06-28: TheStatsAPI carries this competition as
comp_6107 ("FIFA World Cup"), season sn_118868, xg_available=True. Per-team match
xG = sum matches/{id}/player-stats -> shooting.expected_goals, grouped by team.

Scope defaults to SURVIVORS ONLY -- matches involving a team that has reached
R16 or later. This is a live DB query (stage IN R16/QF/SF/Final), not a hardcoded
team list, so it grows automatically as the bracket progresses and trivially
extends to the WHOLE tournament with --scope all once it's over (e.g. for a
full post-tournament calibration postmortem).

Usage:
    python import_wc_match_xg.py                  # survivors only (default)
    python import_wc_match_xg.py --scope all       # every finished match
    python import_wc_match_xg.py --team Brazil     # further restrict to one team
    python import_wc_match_xg.py --dry-run --team Brazil   # validate before a full pull
"""

import argparse
import sqlite3
import sys
from datetime import datetime, timezone

from core.sports_db import DATABASE_PATH, upsert_wc_external_xg
from core.thestatsapi import Client, TheStatsAPIError

COMPETITION_ID = "comp_6107"
SEASON_ID = "sn_118868"
SOURCE = "thestatsapi"

# Reaching any of these stages means the team is (or was) still alive past the
# Round of 32 -- the "still relevant to a future pick" cutoff for the default scope.
SURVIVOR_STAGES = ("R16", "QF", "SF", "Final")


def parse_args():
    ap = argparse.ArgumentParser(
        description="Import external (TheStatsAPI) per-match xG for comparison only.")
    ap.add_argument("--scope", choices=("survivors", "all"), default="survivors",
                    help="'survivors' (default): only matches involving a team that has "
                         "reached R16 or later. 'all': every finished match (full "
                         "post-tournament postmortem).")
    ap.add_argument("--team", help="Further restrict to one team's matches.")
    ap.add_argument("--api-key", help="Override the THE_STATS_API_API_KEY env var.")
    ap.add_argument("--max-requests", type=int, default=200,
                    help="Abort if more than this many API requests are issued (default 200).")
    ap.add_argument("--dry-run", action="store_true",
                    help="Fetch and report without writing to the database.")
    return ap.parse_args()


def normalize(name):
    return (name or "").strip().lower()


def survivor_team_names(conn):
    """Teams that have appeared in any match at R16 or later -- live-derived, not
    hardcoded, so it grows on its own as the bracket progresses."""
    placeholders = ",".join("?" for _ in SURVIVOR_STAGES)
    rows = conn.execute(f"""
        SELECT DISTINCT t.name FROM soccer_wc_matches m
        JOIN soccer_wc_teams t ON t.team_id IN (m.home_team_id, m.away_team_id)
        WHERE m.stage IN ({placeholders})
    """, SURVIVOR_STAGES).fetchall()
    return {r[0] for r in rows}


def our_matches(conn, scope, team_filter):
    """Our finished matches to fetch external xG for, per --scope/--team."""
    rows = conn.execute("""
        SELECT m.match_id, m.match_date, m.home_team_id, m.away_team_id,
               h.name AS home, a.name AS away
        FROM soccer_wc_matches m
        JOIN soccer_wc_teams h ON h.team_id = m.home_team_id
        JOIN soccer_wc_teams a ON a.team_id = m.away_team_id
        WHERE m.home_score IS NOT NULL
        ORDER BY m.match_date
    """).fetchall()
    if scope == "survivors":
        survivors = survivor_team_names(conn)
        rows = [r for r in rows if r["home"] in survivors or r["away"] in survivors]
    if team_filter:
        rows = [r for r in rows if team_filter in (r["home"], r["away"])]
    return rows


def already_fetched(conn, match_id):
    return conn.execute(
        "SELECT 1 FROM soccer_wc_external_xg WHERE match_id = ? AND source = ?",
        (match_id, SOURCE)
    ).fetchone() is not None


def fetch_comp_matches(client):
    """All finished comp_6107/sn_118868 matches, keyed by normalized (home, away)
    team-name pairs (a competition can have multiple meetings, so each key maps to
    a list)."""
    index = {}
    for m in client.paginate(
            "matches", {"competition_id": COMPETITION_ID, "season_id": SEASON_ID}):
        if m.get("status") != "finished":
            continue
        key = (normalize(m["home_team"]["name"]), normalize(m["away_team"]["name"]))
        index.setdefault(key, []).append(m)
    return index


def find_comp_match(index, home_name, away_name, our_date):
    """Best comp_6107 match for our fixture: exact normalized team-name pair,
    closest kickoff date breaks ties (mirrors import_wc_odds.find_match)."""
    candidates = index.get((normalize(home_name), normalize(away_name)))
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]

    def date_key(m):
        try:
            md = datetime.fromisoformat(str(m.get("date"))[:10])
            td = datetime.fromisoformat(str(our_date)[:10])
            return abs((md - td).days)
        except (ValueError, TypeError):
            return 10_000
    return min(candidates, key=date_key)


def match_team_xg(rows, home_team_id, away_team_id):
    """Sum shooting.expected_goals by team from a matches/{id}/player-stats payload."""
    xg = {home_team_id: 0.0, away_team_id: 0.0}
    for r in rows:
        tid = r.get("team_id")
        if tid in xg:
            xg[tid] += (r.get("shooting") or {}).get("expected_goals") or 0
    return xg[home_team_id], xg[away_team_id]


def main():
    args = parse_args()
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row

    matches = our_matches(conn, args.scope, args.team)
    pending = [m for m in matches if not already_fetched(conn, m["match_id"])]
    print(f"{len(matches)} matches in scope ({args.scope}"
          f"{', team=' + args.team if args.team else ''}); "
          f"{len(pending)} not yet fetched.")
    if not pending:
        conn.close()
        return

    try:
        client = Client(api_key=args.api_key, max_requests=args.max_requests)
    except TheStatsAPIError as exc:
        conn.close()
        sys.exit(str(exc))

    matched = unmatched = 0
    try:
        comp_index = fetch_comp_matches(client)
        for m in pending:
            comp_match = find_comp_match(comp_index, m["home"], m["away"], m["match_date"])
            if not comp_match:
                unmatched += 1
                print(f"  NO MATCH: {m['home']} v {m['away']} ({m['match_date']})")
                continue
            rows = client.get_data(f"matches/{comp_match['id']}/player-stats") or []
            home_xg, away_xg = match_team_xg(
                rows, comp_match["home_team"]["id"], comp_match["away_team"]["id"])
            matched += 1
            print(f"  {m['home']} v {m['away']}: xG {home_xg:.2f} - {away_xg:.2f}")
            if not args.dry_run:
                upsert_wc_external_xg(
                    match_id=m["match_id"], source=SOURCE,
                    home_xg=home_xg, away_xg=away_xg,
                    fetched_at=datetime.now(timezone.utc).isoformat())
    except TheStatsAPIError as exc:
        print(f"\nAborted: {exc}")
    finally:
        conn.close()

    print(f"\n=== EXTERNAL xG IMPORT ({'dry-run' if args.dry_run else 'stored'}) ===")
    print(f"Matched {matched}/{len(pending)}  unmatched {unmatched}")
    print(f"API requests used: {client.requests_made}")


if __name__ == "__main__":
    main()
