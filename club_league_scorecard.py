"""
One tool for post-matchday reporting (docs/PRE-AND-POST-MATCHDAY-EXPERIENCE.md,
2026-08-21): refreshes match results, grades any newly-completed picks, and
prints a win/loss + ROI scorecard -- per league and pooled overall -- for a
single matchday or a day range.

Steps, in order:
  1. Refresh results for every league with matches in the window, reusing
     season_kickoff.py's import_fixtures() (import_league_matches.py for
     every league now -- Serie A migrated onto this same pipeline
     2026-09-04, BUGS.md) -- a refresh failure for one league is logged and
     skipped, not fatal to the rest.
     Called with allow_overwrite=True (2026-08-23, BUGS.md) -- a scheduled
     match transitioning to completed with a real score is exactly what
     "refresh" means here, so it must actually be APPLIED, not just detected
     and reported (import_league_matches.py's default, for a human-reviewed
     one-off run, is report-only).
  2. Grade any picks that are now gradeable (grade_club_league_picks.py).
  3. Report: win/loss/push counts and ROI, per league and pooled overall,
     for every GRADED pick (soccer_club_league_picks.result IS NOT NULL) in
     the window -- not a fresh model recompute, the actual picks that were
     posted (FEATURE-016's stored-picks contract). Also reports the day's
     "biggest winner" -- the single highest-profit winning pick across every
     league (profit == ROI here since every pick is a flat 1-unit stake).
     --post-friendly prints a copy-pasteable version of the same report
     instead of the detailed table (mirrors generate_club_league_card.py's
     own --post-friendly for the picks side).

A push (a totals pick landing exactly on an integer line) is excluded from
the ROI staked/profit denominator, same convention backtest_from_predictions.
py's grade_totals() already uses -- reported as its own count, not folded
into wins or losses.

Usage:
    python club_league_scorecard.py --matchday-date 2026-08-22
    python club_league_scorecard.py --matchday-date 2026-08-20 2026-08-22
    python club_league_scorecard.py --matchday-date 2026-08-22 --skip-refresh
    python club_league_scorecard.py --matchday-date 2026-08-22 --post-friendly
"""
import argparse
import sqlite3

from core.sports_db import DATABASE_PATH
from core.matchday import matchday_utc_window, matchday_range_utc_window, format_db_timestamp
from core.poisson_model import american_to_decimal
from grade_club_league_picks import grade_picks_in_window
from generate_club_league_card import _pick_emoji
from season_kickoff import import_fixtures


def leagues_with_matches_in_window(conn, start_utc, end_utc):
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT league, season FROM soccer_matches
        WHERE match_date >= ? AND match_date < ?
    """, (format_db_timestamp(start_utc), format_db_timestamp(end_utc)))
    return cur.fetchall()


def refresh_results(conn, start_utc, end_utc):
    pairs = leagues_with_matches_in_window(conn, start_utc, end_utc)
    for league, season in pairs:
        print(f"Refreshing results: {league} season {season}...")
        try:
            import_fixtures(league, season, allow_overwrite=True)
        except Exception as exc:
            print(f"  WARNING: refresh failed for {league} season {season}: {exc}")


def _pick_profit(odds, result):
    """Profit on a 1-unit stake: american_to_decimal(odds) - 1 on a win, -1 on
    a loss, 0 (and not staked at all) on a push -- shared by _pick_stats() and
    the biggest-winner lookup so the two can never disagree on the math."""
    if result == "win":
        return american_to_decimal(odds) - 1
    if result == "push":
        return 0.0
    return -1.0


def _pick_stats(picks):
    """picks: list of (odds, result). Returns wins/losses/pushes/staked/profit/roi."""
    wins = losses = pushes = 0
    staked = profit = 0.0
    for odds, result in picks:
        if result == "push":
            pushes += 1
            continue
        staked += 1.0
        if result == "win":
            wins += 1
        else:
            losses += 1
        profit += _pick_profit(odds, result)
    roi = profit / staked if staked else 0.0
    return {"wins": wins, "losses": losses, "pushes": pushes,
            "staked": staked, "profit": profit, "roi": roi}


def _biggest_winner(picks):
    """The single WINNING pick with the highest profit (== highest ROI, since
    every pick is a flat 1-unit stake) among a list of full pick records --
    the day's headline result. None if nothing won in this window (a real,
    reportable state, not an error)."""
    wins = [p for p in picks if p["result"] == "win"]
    if not wins:
        return None
    return max(wins, key=lambda p: p["profit"])


def scorecard(conn, start_utc, end_utc, extra_picks=None):
    """{"by_league": {league: stats}, "overall": stats, "biggest_winner": pick
    or None, "methods_seen": set} for every GRADED pick in [start_utc, end_utc).
    "overall" is true pooled ROI (sum of profit / sum of staked), not an
    average of per-league ROI ratios. "biggest_winner" is a dict (league, side,
    odds, home, away, profit) for the single highest-profit win across every
    league in the window, or None if nothing won.

    extra_picks: optional [(league, side, odds, result, method, home, away), ...]
    to merge in alongside what's already persisted -- for a --dry-run preview,
    where grading was computed but never written, so the DB query alone
    wouldn't see them. Only pass this in dry-run mode; in a real run the
    grading step already persisted these, so the DB query already includes
    them and passing them again would double-count."""
    cur = conn.cursor()
    cur.execute("""
        SELECT p.league, p.side, p.odds, p.result, p.method, ht.name, at.name
        FROM soccer_club_league_picks p
        JOIN soccer_matches m ON m.match_id = p.match_id
        JOIN soccer_teams ht ON ht.team_id = m.home_team_id
        JOIN soccer_teams at ON at.team_id = m.away_team_id
        WHERE p.result IS NOT NULL
          AND m.match_date >= ? AND m.match_date < ?
    """, (format_db_timestamp(start_utc), format_db_timestamp(end_utc)))
    rows = cur.fetchall()

    by_league_picks = {}
    all_picks = []
    all_picks_full = []
    methods_seen = set()
    for league, side, odds, result, method, home, away in list(rows) + list(extra_picks or []):
        by_league_picks.setdefault(league, []).append((odds, result))
        all_picks.append((odds, result))
        all_picks_full.append({"league": league, "side": side, "odds": odds, "result": result,
                               "home": home, "away": away, "profit": _pick_profit(odds, result)})
        methods_seen.add(method)

    by_league = {league: _pick_stats(picks) for league, picks in sorted(by_league_picks.items())}
    overall = _pick_stats(all_picks)
    return {"by_league": by_league, "overall": overall,
            "biggest_winner": _biggest_winner(all_picks_full), "methods_seen": methods_seen}


def _biggest_winner_line(bw):
    """Plain-text description of a biggest_winner pick record, shared by both
    print_scorecard() and print_scorecard_post_friendly()."""
    return (f"{bw['league']}: {bw['home']} vs {bw['away']} -- {bw['side']} ({bw['odds']:+.0f}) "
            f"-> profit {bw['profit']:+.2f}u (ROI {bw['profit']:+.1%})")


def print_scorecard(label, card):
    print(f"\n=== SCORECARD: {label} ===")
    if not card["by_league"]:
        print("No graded picks in this window.")
        return
    for league, s in card["by_league"].items():
        print(f"  {league:<16} {s['wins']}-{s['losses']}-{s['pushes']} (W-L-Push)  "
              f"staked={s['staked']:.0f}u  profit={s['profit']:+.2f}u  ROI={s['roi']:+.1%}")
    o = card["overall"]
    print(f"  {'OVERALL':<16} {o['wins']}-{o['losses']}-{o['pushes']} (W-L-Push)  "
          f"staked={o['staked']:.0f}u  profit={o['profit']:+.2f}u  ROI={o['roi']:+.1%}")

    if card["biggest_winner"]:
        print(f"  Biggest winner: {_biggest_winner_line(card['biggest_winner'])}")
    else:
        print("  Biggest winner: none -- no picks won in this window.")

    methods = sorted(m for m in card["methods_seen"] if m is not None)
    unknown = None in card["methods_seen"]
    if len(methods) > 1 or (methods and unknown):
        print(f"  WARNING: picks in this window span multiple model versions "
              f"({', '.join(methods) or 'none'}{', pre-tracking (unknown)' if unknown else ''}) "
              f"-- ROI above blends them together, not a clean comparison.")
    elif methods:
        print(f"  Model version: {methods[0]}")


def print_scorecard_post_friendly(label, card):
    """One copy-pasteable results post: per-league record + ROI, then the
    day's biggest winner -- mirrors generate_club_league_card.py's
    print_post_friendly() for the picks side (docs/PRE-AND-POST-MATCHDAY-
    EXPERIENCE.md, 2026-08-21), no raw odds/staked/profit-per-league detail."""
    print(f"\U0001F4CA {label} Results")
    if not card["by_league"]:
        print("No graded picks in this window.")
        return
    for league, s in card["by_league"].items():
        print(f"{league}: {s['wins']}-{s['losses']}-{s['pushes']} | ROI {s['roi']:+.1%}")
    o = card["overall"]
    print(f"Overall: {o['wins']}-{o['losses']}-{o['pushes']} | ROI {o['roi']:+.1%}")

    bw = card["biggest_winner"]
    if bw:
        print(f"\U0001F3C6 Biggest winner: {bw['home']} vs {bw['away']} "
              f"{_pick_emoji(bw['side'])} {bw['side']} ({bw['odds']:+.0f}) "
              f"+{bw['profit']:.2f}u")
    else:
        print("No winning picks today.")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--matchday-date", nargs="+", required=True, metavar="YYYY-MM-DD",
                        help="One matchday, or two (a start and end date) to cover every "
                             "matchday in between, inclusive (core.matchday's ET+buffer day "
                             "boundary).")
    parser.add_argument("--skip-refresh", action="store_true",
                        help="Skip step 1 (results refresh) -- grade/report against whatever "
                             "scores are already in the database.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview the scorecard without writing pick results to "
                             "soccer_club_league_picks. Results refresh (soccer_matches) still "
                             "runs normally unless --skip-refresh is also given -- that's real "
                             "data ingestion, not scoring, so dry-run doesn't touch it.")
    parser.add_argument("--post-friendly", action="store_true",
                        help="Print a copy-pasteable results summary (per-league record + ROI, "
                             "plus the day's biggest winner) instead of the detailed scorecard.")
    args = parser.parse_args()
    if len(args.matchday_date) not in (1, 2):
        parser.error("--matchday-date takes 1 or 2 dates")
    return args


def main():
    args = parse_args()
    conn = sqlite3.connect(DATABASE_PATH)

    if len(args.matchday_date) == 1:
        start_utc, end_utc = matchday_utc_window(args.matchday_date[0])
        label = f"matchday {args.matchday_date[0]}"
    else:
        start_utc, end_utc = matchday_range_utc_window(*args.matchday_date)
        label = f"{args.matchday_date[0]} to {args.matchday_date[1]}"

    if not args.skip_refresh:
        print("-- step 1: refresh results --")
        refresh_results(conn, start_utc, end_utc)

    print("\n-- step 2: grade picks --")
    graded, pending, graded_details = grade_picks_in_window(conn, start_utc, end_utc, dry_run=args.dry_run)
    print(f"{'Would grade' if args.dry_run else 'Graded'} {graded} pick(s)."
          f"{'  (dry-run, nothing written)' if args.dry_run else ''}")
    if pending:
        print(f"{pending} pick(s) still ungraded -- their match has no final score yet.")

    card = scorecard(conn, start_utc, end_utc, extra_picks=graded_details if args.dry_run else None)
    full_label = f"{label}{'  [DRY RUN -- not persisted]' if args.dry_run else ''}"
    if args.post_friendly:
        print_scorecard_post_friendly(full_label, card)
    else:
        print_scorecard(full_label, card)

    conn.close()


if __name__ == "__main__":
    main()
