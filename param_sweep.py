"""
Parameter sweep over SHRINKAGE_K and RECENCY_DECAY.
Runs the full backtest for each combination and prints a summary table sorted by ROI.
"""

import sqlite3
import itertools
from core.sports_db import DATABASE_PATH
import core.poisson_model as pm
from backtest import load_test_matches

# ---------------------------------------------------------------------------
# Grid
# ---------------------------------------------------------------------------

SHRINKAGE_K_VALUES = [0, 1, 2, 5, 10]
RECENCY_DECAY_VALUES = [0.75, 0.85, 0.90, 0.95, 1.0]
EV_THRESHOLD = 0.0   # bet whenever model shows any edge
SEASON = "2025"
TEST_FRACTION = 0.4

# ---------------------------------------------------------------------------
# Single-run evaluator (mirrors backtest.py logic but parameterised)
# ---------------------------------------------------------------------------

def run_one(conn, test_matches, shrinkage_k: float, decay: float) -> dict:
    bets = 0
    wins = 0
    profit = 0.0

    # Calibration buckets: key = floor(p * 10) / 10
    cal_model = {}
    cal_actual = {}
    cal_n = {}

    for m in test_matches:
        home_id    = m["home_team_id"]
        away_id    = m["away_team_id"]
        match_date = m["match_date"]
        home_ml    = m["home_moneyline"]
        away_ml    = m["away_moneyline"]
        home_score = m["home_score"]
        away_score = m["away_score"]

        if home_ml is None or away_ml is None:
            continue
        if home_score is None or away_score is None:
            continue

        league_avgs = pm.get_league_averages(conn, "Serie A", [SEASON])
        home_ratings = pm.get_team_ratings(conn, home_id, match_date, decay=decay)
        away_ratings = pm.get_team_ratings(conn, away_id, match_date, decay=decay)

        try:
            lH, lA = pm.estimate_lambdas(home_ratings, away_ratings, league_avgs,
                                          shrinkage_k=shrinkage_k)
        except Exception:
            continue

        grid = pm.scoreline_grid(lH, lA)
        probs = pm.outcome_probs(grid)

        actual_home_win = 1 if home_score > away_score else 0
        actual_away_win = 1 if away_score > home_score else 0

        for side, p_model, ml, actual_win in [
            ("home", probs["p_home"], home_ml, actual_home_win),
            ("away", probs["p_away"], away_ml, actual_away_win),
        ]:
            ev = pm.compute_ev(p_model, ml)
            dec = pm.american_to_decimal(ml)

            # calibration (all predictions regardless of EV)
            bucket = round(int(p_model * 10) / 10, 1)
            cal_model[bucket]  = cal_model.get(bucket, 0.0) + p_model
            cal_actual[bucket] = cal_actual.get(bucket, 0) + actual_win
            cal_n[bucket]      = cal_n.get(bucket, 0) + 1

            if ev >= EV_THRESHOLD:
                bets += 1
                profit += (dec - 1) if actual_win else -1
                if actual_win:
                    wins += 1

    roi = profit / bets if bets > 0 else float("nan")

    # Mean absolute calibration error (weighted by bucket size)
    mae_num = 0.0
    mae_den = 0
    for bucket, n in cal_n.items():
        avg_model  = cal_model[bucket] / n
        avg_actual = cal_actual[bucket] / n
        mae_num += abs(avg_model - avg_actual) * n
        mae_den += n
    mae = mae_num / mae_den if mae_den > 0 else float("nan")

    return {"bets": bets, "wins": wins, "roi": roi, "mae": mae}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row

    _, test_matches = load_test_matches(conn, season=SEASON, test_fraction=TEST_FRACTION)
    test_matches = [dict(m) for m in test_matches]

    print(f"Test set: {len(test_matches)} matches | EV threshold: {EV_THRESHOLD*100:.0f}%\n")

    results = []
    combos = list(itertools.product(SHRINKAGE_K_VALUES, RECENCY_DECAY_VALUES))
    for shrinkage_k, decay in combos:
        r = run_one(conn, test_matches, shrinkage_k, decay)
        results.append({
            "k": shrinkage_k,
            "decay": decay,
            **r,
        })

    conn.close()

    # Sort by ROI descending
    results.sort(key=lambda x: x["roi"], reverse=True)

    header = f"{'k':>4}  {'decay':>6}  {'bets':>5}  {'wins':>5}  {'ROI':>8}  {'CalibMAE':>10}"
    print(header)
    print("-" * len(header))
    for r in results:
        roi_str = f"{r['roi']*100:+.1f}%"
        mae_str = f"{r['mae']*100:.1f}%"
        print(f"{r['k']:>4}  {r['decay']:>6.2f}  {r['bets']:>5}  {r['wins']:>5}  {roi_str:>8}  {mae_str:>10}")
