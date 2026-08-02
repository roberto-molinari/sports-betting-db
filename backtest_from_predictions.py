"""
ROI backtest sourced from ALREADY-COMPUTED soccer_model_predictions rows, rather than
recomputed on the fly like backtest.py. Lets any backfilled method (poisson_v3,
poisson_v4, ...) be graded against real results directly, with no train/test split
needed -- a full-season backfill (e.g. backfill_player_blend_predictions.py or
backfill_soccer_model_predictions.py) is already point-in-time correct for every
match on its own, so the whole season is a valid test set.

Staking odds come from soccer_betting_odds, filtered to a single --sportsbook
(default Bet365 -- the soft-book reference the Success Criteria's ROI bar is defined
against, FEATURE-011_REQUIREMENTS.md; see FEATURE-011_BUILD_TRACKER.md's loose thread
on this, found 2026-08-01). Each match has exactly one soccer_betting_odds row, but
which book varies by match (Serie A season 2025: 350 Bet365, 10 Pinnacle, 20 "User
Book") -- unfiltered, the backtest silently staked some matches against a sharp book
or an unidentified source instead of the soft book the criteria means. Matches without
a Bet365 row are excluded, not substituted with another book.

Usage:
    python backtest_from_predictions.py --method poisson_v4 --season 2025
    python backtest_from_predictions.py --method poisson_v3 --season 2025 --ev-threshold 0.05
"""

import argparse
import sqlite3

from core.sports_db import DATABASE_PATH
from core.poisson_model import american_to_decimal, compute_ev

DEFAULT_SPORTSBOOK = "Bet365"


def load_predictions(conn, league, season, method, sportsbook=DEFAULT_SPORTSBOOK):
    cur = conn.cursor()
    cur.execute("""
        SELECT mp.p_home, mp.p_draw, mp.p_away,
               o.home_moneyline, o.draw_moneyline, o.away_moneyline,
               m.home_score, m.away_score
        FROM soccer_model_predictions mp
        JOIN soccer_matches m ON m.match_id = mp.match_id
        JOIN soccer_betting_odds o ON o.match_id = mp.match_id AND o.sportsbook = ?
        WHERE mp.league = ? AND mp.method = ? AND m.season = ?
              AND m.home_score IS NOT NULL
    """, (sportsbook, league, method, season))
    return cur.fetchall()


def run(conn, league, season, method, ev_threshold, sportsbook=DEFAULT_SPORTSBOOK):
    rows = load_predictions(conn, league, season, method, sportsbook=sportsbook)
    total_staked = total_profit = 0.0
    bets = wins = 0
    by_side = {"home": [0, 0, 0.0], "draw": [0, 0, 0.0], "away": [0, 0, 0.0]}

    for (p_home, p_draw, p_away, ml_home, ml_draw, ml_away, hs, as_) in rows:
        actual = "home" if hs > as_ else ("away" if as_ > hs else "draw")
        # EV computed here against the filtered sportsbook's own moneyline, not
        # read from soccer_model_predictions.ev_home/ev_draw/ev_away -- those were
        # stored against whatever book happened to be joined at prediction time,
        # not guaranteed to be `sportsbook`.
        for side, p_model, ml in (("home", p_home, ml_home), ("draw", p_draw, ml_draw), ("away", p_away, ml_away)):
            if p_model is None or ml is None:
                continue
            ev = compute_ev(p_model, ml)
            if ev <= ev_threshold:
                continue
            stake = 1.0
            won = (side == actual)
            profit = stake * (american_to_decimal(ml) - 1) if won else -stake
            total_staked += stake
            total_profit += profit
            bets += 1
            by_side[side][0] += 1
            by_side[side][2] += profit
            if won:
                wins += 1
                by_side[side][1] += 1

    roi = total_profit / total_staked if total_staked else 0.0
    print(f"\n{method} | {league} season {season} | vs {sportsbook} | "
          f"EV threshold {ev_threshold:+.1%} | {len(rows)} graded matches")
    if bets:
        print(f"  Bets: {bets}  Wins: {wins}  Win rate: {wins/bets:.1%}")
    else:
        print("  No bets placed")
    print(f"  Total staked: ${total_staked:.2f}  Profit: ${total_profit:+.2f}  ROI: {roi:+.1%}")
    print("\n  By side:")
    for side, (n, w, p) in by_side.items():
        side_roi = p / n if n else 0.0
        print(f"    {side:>5}  n={n:<4} wins={w:<4} profit=${p:>+8.2f}  ROI={side_roi:+.1%}")
    return roi, bets


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--league", default="Serie A")
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--method", required=True)
    parser.add_argument("--ev-threshold", type=float, default=0.0)
    parser.add_argument("--sportsbook", default=DEFAULT_SPORTSBOOK,
                        help=f"Stake against this soccer_betting_odds sportsbook only "
                             f"(default: {DEFAULT_SPORTSBOOK}).")
    args = parser.parse_args()

    conn = sqlite3.connect(DATABASE_PATH)
    run(conn, args.league, args.season, args.method, args.ev_threshold, sportsbook=args.sportsbook)
    conn.close()


if __name__ == "__main__":
    main()
