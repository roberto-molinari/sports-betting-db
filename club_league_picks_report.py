"""
List every stored club-league pick for a matchday (or range) -- pick, odds,
model probability, EV, and stars for each; plus result and per-pick ROI for
any pick whose match has already concluded (docs/PRE-AND-POST-MATCHDAY-
EXPERIENCE.md, 2026-08-21). No existing tool did this: generate_club_league_
card.py only RECOMPUTES fresh picks for one league (never reads what was
actually stored, no result/ROI); grade_club_league_picks.py only writes
results, doesn't display anything; club_league_scorecard.py only shows
AGGREGATED per-league stats, not each individual pick.

Read-only, no side effects -- doesn't refresh results or grade anything (use
club_league_scorecard.py for that). Shows whatever is currently in
soccer_club_league_picks/soccer_matches: a concluded match whose pick hasn't
been graded yet just shows as PENDING, same as an unplayed one.

Usage:
    python club_league_picks_report.py --matchday-date 2026-08-22
    python club_league_picks_report.py --matchday-date 2026-08-20 2026-08-22
"""
import argparse
import sqlite3

from core.sports_db import DATABASE_PATH
from core.matchday import matchday_utc_window, matchday_range_utc_window, format_db_timestamp
from club_league_scorecard import _pick_profit


def picks_in_window(conn, start_utc, end_utc):
    cur = conn.cursor()
    cur.execute("""
        SELECT p.league, m.match_date, ht.name, at.name, p.side, p.odds,
               p.model_prob, p.ev, p.stars, p.result, p.method
        FROM soccer_club_league_picks p
        JOIN soccer_matches m ON m.match_id = p.match_id
        JOIN soccer_teams ht ON ht.team_id = m.home_team_id
        JOIN soccer_teams at ON at.team_id = m.away_team_id
        WHERE m.match_date >= ? AND m.match_date < ?
        ORDER BY p.league, m.match_date
    """, (format_db_timestamp(start_utc), format_db_timestamp(end_utc)))
    return cur.fetchall()


def group_by_league(rows):
    """{league: [pick_dict, ...]} -- each pick_dict adds "profit"/"roi" when
    result is not None (concluded + graded), else both are None (still
    pending, whether or not the match itself has finished -- club_league_
    scorecard.py is what actually grades)."""
    by_league = {}
    for league, match_date, home, away, side, odds, model_prob, ev, stars, result, method in rows:
        profit = _pick_profit(odds, result) if result is not None else None
        by_league.setdefault(league, []).append({
            "match_date": match_date, "home": home, "away": away, "side": side,
            "odds": odds, "model_prob": model_prob, "ev": ev, "stars": stars,
            "result": result, "method": method, "profit": profit,
        })
    return by_league


def print_report(label, by_league):
    total = sum(len(v) for v in by_league.values())
    print(f"=== PICKS: {label} ===")
    if not by_league:
        print("No picks found in this window.")
        return
    pending = sum(1 for picks in by_league.values() for p in picks if p["result"] is None)
    print(f"{total} pick(s), {total - pending} graded, {pending} pending\n")

    for league in sorted(by_league):
        picks = by_league[league]
        print(f"{league} ({len(picks)} pick{'s' if len(picks) != 1 else ''})")
        for p in picks:
            date_label = p["match_date"][:10]
            core = (f"  {date_label}  {p['home']} vs {p['away']} | {p['side']} ({p['odds']:+.0f}) | "
                    f"model p={p['model_prob']:.3f} EV={p['ev']:+.1%} {'*' * p['stars']}")
            if p["result"] is None:
                print(f"{core} | PENDING")
            else:
                print(f"{core} | {p['result'].upper()} | profit={p['profit']:+.2f}u")
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
        label = f"matchday {args.matchday_date[0]}"
    else:
        start_utc, end_utc = matchday_range_utc_window(*args.matchday_date)
        label = f"{args.matchday_date[0]} to {args.matchday_date[1]}"

    conn = sqlite3.connect(DATABASE_PATH)
    rows = picks_in_window(conn, start_utc, end_utc)
    conn.close()

    print_report(label, group_by_league(rows))


if __name__ == "__main__":
    main()
