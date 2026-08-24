"""
Grade stored club-league picks (soccer_club_league_picks) once their match has
a final score.

Reuses core.grading.grade_pick() -- the same market-agnostic settlement
function the World Cup tracker uses (update_wc_results.py) -- so club-league
picks are scored identically, and totals pushes are handled the same way.

Does NOT refresh match results itself -- that's a separate concern (see
club_league_scorecard.py, which runs the real results-refresh scripts before
grading). This script only grades picks against whatever scores are already
in soccer_matches.

Usage:
    python grade_club_league_picks.py --matchday-date 2026-08-22
    python grade_club_league_picks.py --matchday-date 2026-08-20 2026-08-22
    python grade_club_league_picks.py --all   # every ungraded, completed pick
"""
import argparse
import sqlite3

from core.sports_db import DATABASE_PATH, set_club_league_pick_result
from core.grading import grade_pick
from core.matchday import matchday_utc_window, matchday_range_utc_window, format_db_timestamp


def grade_picks_in_window(conn, start_utc=None, end_utc=None, dry_run=False):
    """Grade every ungraded pick whose match is completed, optionally
    restricted to the half-open UTC window [start_utc, end_utc). Returns
    (graded_count, still_pending_count, graded_details) -- pending is
    ungraded picks in scope whose match has no final score yet, useful for
    telling the caller "not everything could be graded yet" rather than
    silently under-reporting. graded_details is
    [(league, side, odds, result, method, home, away), ...] for whatever was
    (or, in dry-run, would be) graded this call -- lets a caller preview a
    scorecard (including which specific match a pick was on, e.g. for a
    "biggest winner" summary) without anything being persisted.

    dry_run: compute results as normal but skip set_club_league_pick_result()
    -- nothing written to soccer_club_league_picks.result. Match results
    themselves (soccer_matches) are a separate concern, untouched either way;
    this only gates the grading write."""
    cur = conn.cursor()
    window_clause, window_params = "", ()
    if start_utc is not None:
        window_clause = " AND m.match_date >= ? AND m.match_date < ?"
        window_params = (format_db_timestamp(start_utc), format_db_timestamp(end_utc))

    cur.execute(f"""
        SELECT p.pick_id, p.league, p.side, p.odds, p.method, m.home_score, m.away_score,
               ht.name, at.name
        FROM soccer_club_league_picks p
        JOIN soccer_matches m ON m.match_id = p.match_id
        JOIN soccer_teams ht ON ht.team_id = m.home_team_id
        JOIN soccer_teams at ON at.team_id = m.away_team_id
        WHERE p.result IS NULL
          AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
          {window_clause}
    """, window_params)
    rows = cur.fetchall()

    graded = 0
    graded_details = []
    for pick_id, league, side, odds, method, hs, as_, home, away in rows:
        outcome = {"regulation_home": hs, "regulation_away": as_}
        result = grade_pick(side, outcome)
        if not dry_run:
            set_club_league_pick_result(pick_id, result, conn=conn)
        graded_details.append((league, side, odds, result, method, home, away))
        graded += 1
    if not dry_run:
        conn.commit()

    cur.execute(f"""
        SELECT COUNT(*)
        FROM soccer_club_league_picks p
        JOIN soccer_matches m ON m.match_id = p.match_id
        WHERE p.result IS NULL
          AND (m.home_score IS NULL OR m.away_score IS NULL)
          {window_clause}
    """, window_params)
    pending = cur.fetchone()[0]

    return graded, pending, graded_details


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--matchday-date", nargs="+", metavar="YYYY-MM-DD",
                       help="One matchday, or two (a start and end date) to cover every "
                            "matchday in between, inclusive (core.matchday's ET+buffer day "
                            "boundary).")
    group.add_argument("--all", action="store_true", help="Grade every ungraded, completed pick, no date filter.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute results without writing them to soccer_club_league_picks.")
    args = parser.parse_args()
    if args.matchday_date and len(args.matchday_date) not in (1, 2):
        parser.error("--matchday-date takes 1 or 2 dates")
    return args


def main():
    args = parse_args()
    conn = sqlite3.connect(DATABASE_PATH)

    if args.all:
        start_utc = end_utc = None
    elif len(args.matchday_date) == 1:
        start_utc, end_utc = matchday_utc_window(args.matchday_date[0])
    else:
        start_utc, end_utc = matchday_range_utc_window(*args.matchday_date)

    graded, pending, _ = grade_picks_in_window(conn, start_utc, end_utc, dry_run=args.dry_run)
    conn.close()

    print(f"{'Would grade' if args.dry_run else 'Graded'} {graded} pick(s)."
          f"{'  (dry-run, nothing written)' if args.dry_run else ''}")
    if pending:
        print(f"{pending} pick(s) still ungraded -- their match has no final score yet.")


if __name__ == "__main__":
    main()
