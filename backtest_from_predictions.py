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
from core.pick_guardrails import guardrail_reasons
from generate_club_league_card import CLUB_LEAGUE_MIN_PICK_PROBABILITY

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


def load_totals_predictions(conn, league, season, method, sportsbook=DEFAULT_SPORTSBOOK):
    """Same shape as load_predictions but for the totals (over/under) market --
    p_over/p_under and over_odds/under_odds, plus the line itself and actual total
    goals (needed since 'over' only means something relative to o.over_under)."""
    cur = conn.cursor()
    cur.execute("""
        SELECT mp.p_over, mp.p_under, o.over_odds, o.under_odds, o.over_under,
               m.home_score, m.away_score
        FROM soccer_model_predictions mp
        JOIN soccer_matches m ON m.match_id = mp.match_id
        JOIN soccer_betting_odds o ON o.match_id = mp.match_id AND o.sportsbook = ?
        WHERE mp.league = ? AND mp.method = ? AND m.season = ?
              AND m.home_score IS NOT NULL AND o.over_under IS NOT NULL
    """, (sportsbook, league, method, season))
    return cur.fetchall()


def grade_1x2(conn, league, season, method, ev_threshold, sportsbook=DEFAULT_SPORTSBOOK,
              guardrail_floor=None):
    """Core 1X2 grading logic, returning the full stats dict (not just roi/bets) --
    factored out of run() so a caller pooling across many leagues/seasons (e.g.
    model_metrics_report.py's all-up view) can sum true staked/profit dollars
    directly instead of reconstructing them from a printed ROI ratio. run() itself
    is now a thin wrapper: print + return (roi, bets), unchanged for existing
    callers.

    guardrail_floor: None (default) grades every EV-positive candidate, unchanged
    from before this parameter existed -- the raw model ROI. Pass a probability
    (e.g. CLUB_LEAGUE_MIN_PICK_PROBABILITY) to additionally reject any candidate
    below that floor, via the SAME guardrail_reasons() check generate_club_league_
    card.py applies to real picks -- this is "what would ROI look like for what
    the live card generator would actually have surfaced," not a different
    metric. No cap check: the live card generator itself only applies the floor
    for club leagues (see its own module docstring), so this mirrors that, not a
    simplification."""
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
            if guardrail_floor is not None and guardrail_reasons(p_model, None, guardrail_floor):
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

    return {"n_graded": len(rows), "staked": total_staked, "profit": total_profit,
            "bets": bets, "wins": wins, "by_side": by_side}


def print_grading_report(stats, label):
    roi = stats["profit"] / stats["staked"] if stats["staked"] else 0.0
    print(f"\n{label}")
    if stats["bets"]:
        print(f"  Bets: {stats['bets']}  Wins: {stats['wins']}  Win rate: {stats['wins']/stats['bets']:.1%}")
    else:
        print("  No bets placed")
    print(f"  Total staked: ${stats['staked']:.2f}  Profit: ${stats['profit']:+.2f}  ROI: {roi:+.1%}")
    print("\n  By side:")
    for side, (n, w, p) in stats["by_side"].items():
        side_roi = p / n if n else 0.0
        print(f"    {side:>5}  n={n:<4} wins={w:<4} profit=${p:>+8.2f}  ROI={side_roi:+.1%}")


def run(conn, league, season, method, ev_threshold, sportsbook=DEFAULT_SPORTSBOOK, guardrail_floor=None):
    stats = grade_1x2(conn, league, season, method, ev_threshold, sportsbook=sportsbook,
                      guardrail_floor=guardrail_floor)
    guardrail_note = f" | guardrail floor {guardrail_floor:g}" if guardrail_floor is not None else ""
    label = (f"{method} | {league} season {season} | vs {sportsbook} | "
             f"EV threshold {ev_threshold:+.1%}{guardrail_note} | {stats['n_graded']} graded matches")
    print_grading_report(stats, label)
    roi = stats["profit"] / stats["staked"] if stats["staked"] else 0.0
    return roi, stats["bets"]


def grade_totals(conn, league, season, method, ev_threshold, sportsbook=DEFAULT_SPORTSBOOK,
                 guardrail_floor=None):
    """Core totals (over/under) grading logic -- same role as grade_1x2() for the
    O/U market, factored out of run_totals() for the same pooling reason.
    guardrail_floor: see grade_1x2's docstring -- the live card generator applies
    the same floor to OVER/UNDER candidates as it does to HOME/DRAW/AWAY
    (build_candidates() screens both through one guardrail_reasons() call), so
    this mirrors that rather than treating totals as ungated."""
    rows = load_totals_predictions(conn, league, season, method, sportsbook=sportsbook)
    total_staked = total_profit = 0.0
    bets = wins = 0
    by_side = {"over": [0, 0, 0.0], "under": [0, 0, 0.0]}

    for (p_over, p_under, over_odds, under_odds, line, hs, as_) in rows:
        total_goals = hs + as_
        # A push (total == line) can't happen at a .5 line (every observed line is
        # 2.5), but guard it anyway rather than assume -- a push is neither a win
        # nor a loss and shouldn't be staked as either.
        if total_goals == line:
            continue
        actual = "over" if total_goals > line else "under"
        for side, p_model, odds in (("over", p_over, over_odds), ("under", p_under, under_odds)):
            if p_model is None or odds is None:
                continue
            ev = compute_ev(p_model, odds)
            if ev <= ev_threshold:
                continue
            if guardrail_floor is not None and guardrail_reasons(p_model, None, guardrail_floor):
                continue
            stake = 1.0
            won = (side == actual)
            profit = stake * (american_to_decimal(odds) - 1) if won else -stake
            total_staked += stake
            total_profit += profit
            bets += 1
            by_side[side][0] += 1
            by_side[side][2] += profit
            if won:
                wins += 1
                by_side[side][1] += 1

    return {"n_graded": len(rows), "staked": total_staked, "profit": total_profit,
            "bets": bets, "wins": wins, "by_side": by_side}


def run_totals(conn, league, season, method, ev_threshold, sportsbook=DEFAULT_SPORTSBOOK, guardrail_floor=None):
    """Totals (over/under) market ROI -- kept as a SEPARATE report/return value from
    run()'s 1X2 numbers, not pooled together, since they're different markets and
    every existing ROI reference point in BUGS.md/model_metrics_report.py is 1X2-only.
    2026-08-07: added alongside generate_club_league_card.py's over/under support
    -- the model already computed p_over/p_under and it was already stored, but
    nothing graded it, so there was no way to tell if those picks were any good."""
    stats = grade_totals(conn, league, season, method, ev_threshold, sportsbook=sportsbook,
                         guardrail_floor=guardrail_floor)
    guardrail_note = f" | guardrail floor {guardrail_floor:g}" if guardrail_floor is not None else ""
    label = (f"{method} | {league} season {season} | vs {sportsbook} | TOTALS | "
             f"EV threshold {ev_threshold:+.1%}{guardrail_note} | {stats['n_graded']} graded matches")
    print_grading_report(stats, label)
    roi = stats["profit"] / stats["staked"] if stats["staked"] else 0.0
    return roi, stats["bets"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--league", default="Serie A")
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--method", required=True)
    parser.add_argument("--ev-threshold", type=float, default=0.0)
    parser.add_argument("--sportsbook", default=DEFAULT_SPORTSBOOK,
                        help=f"Stake against this soccer_betting_odds sportsbook only "
                             f"(default: {DEFAULT_SPORTSBOOK}).")
    parser.add_argument("--market", choices=["1x2", "totals", "both"], default="1x2",
                        help="Which market to grade (default: 1x2, unchanged from before "
                             "totals support existed). 'both' prints two separate reports "
                             "-- totals ROI is never pooled into the 1x2 numbers.")
    parser.add_argument("--guardrail", action="store_true",
                        help="Additionally reject any candidate below "
                             "CLUB_LEAGUE_MIN_PICK_PROBABILITY (generate_club_league_card.py's "
                             "real, shipped guardrail floor) -- 'what would ROI look like for "
                             "what the live card generator actually surfaces', not just every "
                             "raw EV-positive prediction. Default off: unchanged raw-model ROI.")
    args = parser.parse_args()
    guardrail_floor = CLUB_LEAGUE_MIN_PICK_PROBABILITY if args.guardrail else None

    conn = sqlite3.connect(DATABASE_PATH)
    if args.market in ("1x2", "both"):
        run(conn, args.league, args.season, args.method, args.ev_threshold, sportsbook=args.sportsbook,
            guardrail_floor=guardrail_floor)
    if args.market in ("totals", "both"):
        run_totals(conn, args.league, args.season, args.method, args.ev_threshold, sportsbook=args.sportsbook,
                  guardrail_floor=guardrail_floor)
    conn.close()


if __name__ == "__main__":
    main()
