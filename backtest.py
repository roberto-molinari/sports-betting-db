"""
Backtest: Poisson Model vs 2025 Serie A Odds
=============================================
Evaluates the Poisson model against every completed 2025-season Serie A match
that has moneyline odds, using a strict chronological split:

  Train window  : first 60% of odds-covered matches (model observes but is not tested here)
  Test window   : last 40% of odds-covered matches

For each match in the test window the model:
    1. Computes P(home win), P(draw), and P(away win) using only results BEFORE that match.
    2. Calculates EV for all three 1X2 moneylines.
  3. Decides whether to "bet" based on an EV threshold (default: EV > 0).
  4. Records outcome.

Reported metrics
----------------
  - ROI        : total profit / total staked (only counting bet matches)
  - Win rate   : fraction of bets that won
  - Calibration: compare model probability buckets to actual win rates
  - Bet coverage: how many matches triggered at least one bet

Usage
-----
    python backtest.py                  # default EV threshold = 0.0
    python backtest.py --ev-threshold 0.05   # only bet when EV > 5%
"""

import argparse
import sqlite3
from collections import defaultdict

from core.poisson_model import analyse_match
from core.sports_db import DATABASE_PATH


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_test_matches(conn, season: int = 2025, test_fraction: float = 0.4) -> tuple:
    """
    Load all completed Serie A matches from the given season that have odds,
    sorted chronologically.  Returns (train_matches, test_matches).
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT
            m.match_id,
            m.match_date,
            ht.name AS home_name,
            at.name AS away_name,
            m.home_team_id,
            m.away_team_id,
            m.home_score,
            m.away_score,
            o.home_moneyline,
            o.draw_moneyline,
            o.away_moneyline
        FROM soccer_matches m
        JOIN soccer_teams ht ON m.home_team_id = ht.team_id
        JOIN soccer_teams at ON m.away_team_id = at.team_id
        JOIN soccer_betting_odds o ON m.match_id = o.match_id
        WHERE m.league = 'Serie A'
          AND m.season = ?
          AND m.home_score IS NOT NULL
          AND o.home_moneyline IS NOT NULL
                    AND o.draw_moneyline IS NOT NULL
          AND o.away_moneyline IS NOT NULL
        ORDER BY m.match_date ASC
    """, (season,))

    rows = cur.fetchall()
    keys = ["match_id", "match_date", "home_name", "away_name",
            "home_team_id", "away_team_id", "home_score", "away_score",
            "home_moneyline", "draw_moneyline", "away_moneyline"]
    matches = [dict(zip(keys, r)) for r in rows]

    split = int(len(matches) * (1 - test_fraction))
    return matches[:split], matches[split:]


# ---------------------------------------------------------------------------
# Calibration helper
# ---------------------------------------------------------------------------

def calibration_report(predictions: list[dict]) -> str:
    """
    Group predictions into probability buckets and show actual win rate per bucket.
    predictions: list of {"model_prob": float, "won": bool}
    """
    buckets = defaultdict(lambda: {"n": 0, "wins": 0, "sum_prob": 0.0})
    for p in predictions:
        bucket = round(p["model_prob"] * 10) / 10  # round to nearest 0.1
        buckets[bucket]["n"] += 1
        buckets[bucket]["sum_prob"] += p["model_prob"]
        if p["won"]:
            buckets[bucket]["wins"] += 1

    lines = [f"  {'Bucket':>8}  {'N':>5}  {'Avg model p':>12}  {'Actual win%':>12}"]
    lines.append("  " + "-" * 46)
    for bucket in sorted(buckets):
        b = buckets[bucket]
        avg_p  = b["sum_prob"] / b["n"]
        actual = b["wins"] / b["n"]
        lines.append(
            f"  {bucket:>8.1f}  {b['n']:>5}  {avg_p:>12.3f}  {actual:>12.3f}"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main backtest loop
# ---------------------------------------------------------------------------

def run_backtest(ev_threshold: float = 0.0, season: int = 2025,
                 test_fraction: float = 0.4, verbose: bool = False,
                 sides: set = None):
    if sides is None:
        sides = {"home", "draw", "away"}

    conn = sqlite3.connect(DATABASE_PATH)
    train_matches, test_matches = load_test_matches(conn, season, test_fraction)

    print(f"\nSeason {season} | Train: {len(train_matches)} matches | "
          f"Test: {len(test_matches)} matches | EV threshold: {ev_threshold:+.2%}")
    print("=" * 65)

    total_staked  = 0.0
    total_profit  = 0.0
    bets_placed   = 0
    bets_won      = 0
    no_bet_count  = 0
    by_side = {
        "home": {"bets": 0, "wins": 0, "profit": 0.0},
        "draw": {"bets": 0, "wins": 0, "profit": 0.0},
        "away": {"bets": 0, "wins": 0, "profit": 0.0},
    }

    # For calibration we track all model predictions (bet or not)
    all_predictions = []

    for m in test_matches:
        try:
            result = analyse_match(
                home_team_id=m["home_team_id"],
                away_team_id=m["away_team_id"],
                match_date=m["match_date"],
                home_moneyline=m["home_moneyline"],
                draw_moneyline=m["draw_moneyline"],
                away_moneyline=m["away_moneyline"],
                conn=conn,
            )
        except Exception as e:
            print(f"  SKIP {m['home_name']} vs {m['away_name']}: {e}")
            continue

        actual_home_win = m["home_score"] > m["away_score"]
        actual_draw = m["home_score"] == m["away_score"]
        actual_away_win = m["away_score"] > m["home_score"]

        # Track all model predictions for calibration
        all_predictions.append({"model_prob": result["p_home"], "won": actual_home_win})
        all_predictions.append({"model_prob": result["p_draw"], "won": actual_draw})
        all_predictions.append({"model_prob": result["p_away"], "won": actual_away_win})

        ev_home = result.get("ev_home", -999)
        ev_draw = result.get("ev_draw", -999)
        ev_away = result.get("ev_away", -999)

        bet_made = False

        # Home bet
        if "home" in sides and ev_home > ev_threshold:
            stake  = 1.0
            profit = stake * (result["decimal_home"] - 1) if actual_home_win else -stake
            total_staked += stake
            total_profit += profit
            bets_placed  += 1
            by_side["home"]["bets"] += 1
            by_side["home"]["profit"] += profit
            if actual_home_win:
                bets_won += 1
                by_side["home"]["wins"] += 1
            bet_made = True

            if verbose:
                tag = "WIN" if actual_home_win else "loss"
                print(f"  [{tag}] HOME bet  {m['home_name']:20s} vs {m['away_name']:20s} "
                      f"score={m['home_score']}-{m['away_score']}  "
                      f"EV={ev_home:+.3f}  p_model={result['p_home']:.3f}  "
                      f"p_implied={result['implied_home']:.3f}")

        # Draw bet
        if "draw" in sides and ev_draw > ev_threshold:
            stake  = 1.0
            profit = stake * (result["decimal_draw"] - 1) if actual_draw else -stake
            total_staked += stake
            total_profit += profit
            bets_placed  += 1
            by_side["draw"]["bets"] += 1
            by_side["draw"]["profit"] += profit
            if actual_draw:
                bets_won += 1
                by_side["draw"]["wins"] += 1
            bet_made = True

            if verbose:
                tag = "WIN" if actual_draw else "loss"
                print(f"  [{tag}] DRAW bet  {m['home_name']:20s} vs {m['away_name']:20s} "
                      f"score={m['home_score']}-{m['away_score']}  "
                      f"EV={ev_draw:+.3f}  p_model={result['p_draw']:.3f}  "
                      f"p_implied={result['implied_draw']:.3f}")

        # Away bet
        if "away" in sides and ev_away > ev_threshold:
            stake  = 1.0
            profit = stake * (result["decimal_away"] - 1) if actual_away_win else -stake
            total_staked += stake
            total_profit += profit
            bets_placed  += 1
            by_side["away"]["bets"] += 1
            by_side["away"]["profit"] += profit
            if actual_away_win:
                bets_won += 1
                by_side["away"]["wins"] += 1
            bet_made = True

            if verbose:
                tag = "WIN" if actual_away_win else "loss"
                print(f"  [{tag}] AWAY bet  {m['home_name']:20s} vs {m['away_name']:20s} "
                      f"score={m['home_score']}-{m['away_score']}  "
                      f"EV={ev_away:+.3f}  p_model={result['p_away']:.3f}  "
                      f"p_implied={result['implied_away']:.3f}")

        if not bet_made:
            no_bet_count += 1

    conn.close()

    # ---- Summary ----
    roi = total_profit / total_staked if total_staked > 0 else 0.0
    win_rate = bets_won / bets_placed if bets_placed > 0 else 0.0

    print(f"\n{'─'*65}")
    print(f"  Matches evaluated   : {len(test_matches)}")
    print(f"  Bets placed         : {bets_placed}  ({no_bet_count} matches skipped)")
    print(f"  Bets won            : {bets_won}")
    print(f"  Win rate            : {win_rate:.1%}")
    print(f"  Total staked        : ${total_staked:.2f}")
    print(f"  Total profit/loss   : ${total_profit:+.2f}")
    print(f"  ROI                 : {roi:+.1%}")

    print(f"\n--- By Bet Type ---")
    print(f"  {'Side':>6}  {'Bets':>5}  {'Wins':>5}  {'Win rate':>9}  {'Profit':>9}  {'ROI':>8}")
    print("  " + "-" * 52)
    for side in ("home", "draw", "away"):
        stats = by_side[side]
        side_bets = stats["bets"]
        side_wins = stats["wins"]
        side_profit = stats["profit"]
        side_win_rate = side_wins / side_bets if side_bets else 0.0
        side_roi = side_profit / side_bets if side_bets else 0.0
        print(f"  {side:>6}  {side_bets:>5}  {side_wins:>5}  {side_win_rate:>9.1%}  ${side_profit:>+8.2f}  {side_roi:>+7.1%}")

    print(f"\n--- Calibration (all {len(all_predictions)} side predictions) ---")
    print(calibration_report(all_predictions))
    print()

    return {"roi": roi, "bets": bets_placed, "win_rate": win_rate, "profit": total_profit}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backtest Poisson model against Serie A odds")
    parser.add_argument("--ev-threshold", type=float, default=0.0,
                        help="Minimum EV to place a bet (default: 0.0)")
    parser.add_argument("--season", type=int, default=2025,
                        help="Season to backtest (default: 2025)")
    parser.add_argument("--test-fraction", type=float, default=0.4,
                        help="Fraction of matches used as test set (default: 0.4)")
    parser.add_argument("--verbose", action="store_true",
                        help="Print each individual bet")
    parser.add_argument(
        "--sides",
        default="home,draw,away",
        help="Comma-separated sides to bet: home,draw,away (default: all)",
    )
    args = parser.parse_args()

    run_backtest(
        ev_threshold=args.ev_threshold,
        season=args.season,
        test_fraction=args.test_fraction,
        verbose=args.verbose,
        sides=set(args.sides.split(",")),
    )
