"""
List every league with matches on a given matchday (or range of matchdays) --
useful for deciding which leagues to run generate_club_league_card.py /
club_league_scorecard.py for on a given day
(docs/PRE-AND-POST-MATCHDAY-EXPERIENCE.md, 2026-08-21).

Read-only, no side effects. Shows every league with a real soccer_matches row
in the window, not just the 5 pick-generating leagues (core.leagues.
has_odds_source) -- a feeder division (Serie B, Championship, etc.) is
flagged as such since it has no odds source and generate_club_league_card.py
can't produce picks for it, but its matches are still shown.

Usage:
    python matchday_summary.py --matchday-date 2026-08-22
    python matchday_summary.py --matchday-date 2026-08-20 2026-08-22
"""
import argparse
import sqlite3

from core.sports_db import DATABASE_PATH
from core.matchday import (matchday_utc_window, matchday_range_utc_window, format_db_timestamp,
                          matchday_for_match, et_kickoff_time)
from core.leagues import has_odds_source


def matches_in_window(conn, start_utc, end_utc):
    """Includes has_odds -- whether soccer_betting_odds has any row for this
    match yet. generate_club_league_card.py INNER JOINs that table (it needs
    a price to compute EV), so a match with zero odds rows will never
    produce a pick even though it's a real, scheduled match -- found live
    2026-08-21 comparing this tool's count against the card generator's for
    the same league/date and seeing a real, unexplained-looking mismatch."""
    cur = conn.cursor()
    cur.execute("""
        SELECT m.league, m.match_date, ht.name, at.name,
               EXISTS(SELECT 1 FROM soccer_betting_odds o WHERE o.match_id = m.match_id)
        FROM soccer_matches m
        JOIN soccer_teams ht ON ht.team_id = m.home_team_id
        JOIN soccer_teams at ON at.team_id = m.away_team_id
        WHERE m.match_date >= ? AND m.match_date < ?
        ORDER BY m.league, m.match_date
    """, (format_db_timestamp(start_utc), format_db_timestamp(end_utc)))
    return cur.fetchall()


def summarize(rows):
    """{league: [(matchday_date, match_date, kickoff_et, home, away, has_odds), ...]}
    -- matchday_date is the ET+buffer day each match actually belongs to
    (core.matchday), not just its UTC calendar date, so a match near the
    boundary is labeled consistently with how pick generation/grading would
    treat it. kickoff_et is the actual local kickoff time (e.g. '2:30 PM ET'),
    for display only -- not what matchday_date is derived from."""
    by_league = {}
    for league, match_date, home, away, has_odds in rows:
        by_league.setdefault(league, []).append(
            (matchday_for_match(match_date), match_date, et_kickoff_time(match_date),
             home, away, bool(has_odds)))
    return by_league


def print_summary(by_league):
    if not by_league:
        print("No matches found in this window.")
        return
    total = sum(len(v) for v in by_league.values())
    print(f"{len(by_league)} league(s), {total} match(es) total\n")
    for league in sorted(by_league):
        matches = by_league[league]
        no_odds = sum(1 for *_, has_odds in matches if not has_odds)
        pickable = "" if has_odds_source(league) else "  (feeder division -- no odds source, no picks)"
        print(f"{league} ({len(matches)} match{'es' if len(matches) != 1 else ''}){pickable}")
        for matchday_date, match_date, kickoff_et, home, away, has_odds in matches:
            odds_note = "" if has_odds else "  (no odds yet -- won't produce a pick)"
            print(f"  {matchday_date}  {kickoff_et:>10}  {home} vs {away}{odds_note}")
        if no_odds and has_odds_source(league):
            print(f"  -> {no_odds}/{len(matches)} match{'es' if no_odds != 1 else ''} still missing odds")
        print()


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--matchday-date", nargs="+", required=True, metavar="YYYY-MM-DD",
                        help="One matchday, or two (a start and end date) to cover every "
                             "matchday in between, inclusive (core.matchday's ET+buffer day "
                             "boundary).")
    args = parser.parse_args()
    if len(args.matchday_date) not in (1, 2):
        parser.error("--matchday-date takes 1 or 2 dates")
    return args


def main():
    args = parse_args()
    if len(args.matchday_date) == 1:
        start_utc, end_utc = matchday_utc_window(args.matchday_date[0])
    else:
        start_utc, end_utc = matchday_range_utc_window(*args.matchday_date)

    conn = sqlite3.connect(DATABASE_PATH)
    rows = matches_in_window(conn, start_utc, end_utc)
    conn.close()

    print_summary(summarize(rows))


if __name__ == "__main__":
    main()
