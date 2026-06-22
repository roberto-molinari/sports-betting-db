"""
Record World Cup 2026 match results and grade stored picks.

Two ways to supply results:
  - A CSV file with columns: date, home, away, home_score, away_score
  - A single match on the command line: --home --away --home-score --away-score

After a match is marked completed, every stored pick for it is graded
(win / loss / push) so confidence-vs-outcome can be reviewed. Use --grade-only
to (re)grade all already-completed matches without entering new scores.

Usage:
    python update_wc_results.py results.csv
    python update_wc_results.py --home Brazil --away Serbia --home-score 2 --away-score 0
    python update_wc_results.py --grade-only
"""

import argparse
import csv
import sqlite3
import sys

from core.sports_db import (
    DATABASE_PATH,
    update_wc_match_result,
    set_wc_pick_result,
    set_wc_override_result,
)
from import_wc_odds import load_team_map, load_match_index, resolve_team, find_match


def parse_args():
    parser = argparse.ArgumentParser(description="Record WC results and grade picks.")
    parser.add_argument("files", nargs="*", help="Results CSV file(s).")
    parser.add_argument("--home", help="Home team name (single-match mode).")
    parser.add_argument("--away", help="Away team name (single-match mode).")
    parser.add_argument("--home-score", type=int, help="Home goals (single-match mode).")
    parser.add_argument("--away-score", type=int, help="Away goals (single-match mode).")
    parser.add_argument("--date", help="Match date to disambiguate (single-match mode).")
    parser.add_argument("--grade-only", action="store_true",
                        help="Re-grade picks for all completed matches; enter no new scores.")
    return parser.parse_args()


def grade_pick(side, home_score, away_score):
    """Return 'win' / 'loss' / 'push' for a pick side given the final score."""
    total = home_score + away_score
    if side == "HOME":
        return "win" if home_score > away_score else "loss"
    if side == "AWAY":
        return "win" if away_score > home_score else "loss"
    if side == "DRAW":
        return "win" if home_score == away_score else "loss"
    if side.startswith("OVER ") or side.startswith("UNDER "):
        label, line_text = side.split(" ", 1)
        line = float(line_text)
        if total == line:
            return "push"
        over = total > line
        return "win" if (over == (label == "OVER")) else "loss"
    raise ValueError(f"Unknown pick side: {side!r}")


def grade_match_picks(conn, match_id, home_score, away_score):
    """Grade every stored pick for a completed match. Returns count graded."""
    cur = conn.cursor()
    cur.execute("SELECT pick_id, side FROM soccer_wc_picks WHERE match_id = ?", (match_id,))
    graded = 0
    for pick_id, side in cur.fetchall():
        set_wc_pick_result(pick_id, grade_pick(side, home_score, away_score))
        graded += 1
    return graded


def grade_match_overrides(conn, match_id, home_score, away_score):
    """Grade every user override for a completed match on its user_side. Returns count."""
    cur = conn.cursor()
    cur.execute("SELECT override_id, user_side FROM soccer_wc_pick_overrides WHERE match_id = ?",
                (match_id,))
    graded = 0
    for override_id, side in cur.fetchall():
        set_wc_override_result(override_id, grade_pick(side, home_score, away_score))
        graded += 1
    return graded


def record_result(conn, match_id, home_score, away_score):
    update_wc_match_result(match_id, home_score, away_score)
    n = grade_match_picks(conn, match_id, home_score, away_score)
    grade_match_overrides(conn, match_id, home_score, away_score)
    return n


def main():
    args = parse_args()
    conn = sqlite3.connect(DATABASE_PATH)

    if args.grade_only:
        cur = conn.cursor()
        cur.execute("""SELECT match_id, home_score, away_score FROM soccer_wc_matches
                       WHERE match_status = 'completed'
                         AND home_score IS NOT NULL AND away_score IS NOT NULL""")
        total = ov = 0
        for match_id, hs, as_ in cur.fetchall():
            total += grade_match_picks(conn, match_id, hs, as_)
            ov += grade_match_overrides(conn, match_id, hs, as_)
        conn.close()
        print(f"Re-graded {total} picks and {ov} overrides across completed matches.")
        return

    team_map = load_team_map(conn)
    match_index = load_match_index(conn)
    if not team_map:
        conn.close()
        sys.exit("No teams in soccer_wc_teams — import squads/teams first.")

    recorded = 0
    graded = 0
    unmatched = []

    def handle(home, away, hs, as_, date):
        nonlocal recorded, graded
        home_id = resolve_team(home, team_map)
        away_id = resolve_team(away, team_map)
        match_id = (find_match(match_index, home_id, away_id, date)
                    if home_id and away_id else None)
        if match_id is None:
            unmatched.append((home, away, date))
            return
        graded += record_result(conn, match_id, hs, as_)
        recorded += 1

    if args.files:
        for path in args.files:
            with open(path, newline="", encoding="utf-8-sig") as fh:
                for row in csv.DictReader(fh):
                    handle(row.get("home"), row.get("away"),
                           int(row["home_score"]), int(row["away_score"]),
                           row.get("date"))
    elif args.home and args.away and args.home_score is not None and args.away_score is not None:
        handle(args.home, args.away, args.home_score, args.away_score, args.date)
    else:
        conn.close()
        sys.exit("Provide a results CSV, single-match flags, or --grade-only.")

    conn.close()
    print(f"Recorded {recorded} result(s); graded {graded} pick(s).")
    if unmatched:
        print(f"No fixture found ({len(unmatched)}):")
        for home, away, date in unmatched:
            print(f"  {home} vs {away} ({date})")


if __name__ == "__main__":
    main()
