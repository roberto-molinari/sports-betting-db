"""
EXPERIMENTAL, not shipped -- step 3 of the ROI-improvement investigation (BUGS.md,
BUG-009, 2026-08-07 addendum): post-hoc recalibration of a model's OUTPUT
probabilities, instead of another upstream input change. Builds a correction curve
from the measured gap between the model's own probability and the sharp book's
(Betfair Exchange closing) fair probability, bucketed by the MODEL'S OWN probability
(not the market's -- this needs to work without knowing the market price first, the
same way the real model would at prediction time), then linearly interpolates between
bucket midpoints so the correction doesn't jump at bucket edges.

Reads an EXISTING stored method's predictions (e.g. poisson_v4_stretch130) and market
data -- no strength.compute() involved, this is a pure post-processing step on
probabilities already sitting in soccer_model_predictions.

Fit/evaluate split, not a single in-sample number: a correction curve fit and
evaluated on the SAME season's own data is close to circular (it's shaped by exactly
the data it's then "improving") -- this script always fits on one season and applies
to the OTHER, plus (separately, clearly labeled) an in-sample pooled fit for
comparison, so the honest out-of-sample number is never confused with the optimistic
in-sample one.

Usage:
    python3 recalibrate_output.py --source-method poisson_v4_stretch130 --league "Serie A"
        (writes poisson_v4_stretch130_recal_ho2024, _recal_ho2025, and
         _recal_insample under the same league)
"""
import argparse
import sqlite3
from datetime import datetime, timezone

from core.sports_db import DATABASE_PATH, clear_soccer_model_predictions, add_soccer_model_prediction

LEAGUE_DEFAULT = "Serie A"
SHARP_SOURCE = "Betfair Exchange"
SEASONS = [2024, 2025]
BUCKET_MIDPOINTS = [0.075, 0.20, 0.30, 0.40, 0.50, 0.775]  # midpoints of the 6 standard buckets
BUCKET_EDGES = [0.00, 0.15, 0.25, 0.35, 0.45, 0.55, 1.01]


def load_rows(conn, league, season, method):
    cur = conn.cursor()
    cur.execute("""
        SELECT mp.match_id, mp.p_home, mp.p_draw, mp.p_away,
               mp.lambda_home, mp.lambda_away,
               o.home_moneyline, o.draw_moneyline, o.away_moneyline,
               o.over_under, o.over_odds, o.under_odds,
               mo.p_home_fair, mo.p_away_fair
        FROM soccer_model_predictions mp
        JOIN soccer_matches m ON m.match_id = mp.match_id
        JOIN soccer_betting_odds o ON o.match_id = mp.match_id AND o.sportsbook = 'Bet365'
        LEFT JOIN soccer_market_odds mo ON mo.match_id = mp.match_id
                                        AND mo.source = ? AND mo.line_type = 'closing'
        WHERE mp.league = ? AND mp.method = ? AND m.season = ? AND m.home_score IS NOT NULL
    """, (SHARP_SOURCE, league, method, season))
    cols = ["match_id", "p_home", "p_draw", "p_away", "lambda_home", "lambda_away",
            "home_moneyline", "draw_moneyline", "away_moneyline",
            "over_under", "over_odds", "under_odds", "mkt_home", "mkt_away"]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def fit_curve(rows, field, mkt_field):
    """Bucket by the model's OWN probability, mean(market - model) per bucket,
    return a function that linearly interpolates the correction between bucket
    midpoints (flat-extrapolated past the first/last midpoint)."""
    buckets = {mid: [] for mid in BUCKET_MIDPOINTS}
    for r in rows:
        p, mkt = r[field], r[mkt_field]
        if p is None or mkt is None:
            continue
        for i in range(len(BUCKET_EDGES) - 1):
            if BUCKET_EDGES[i] <= p < BUCKET_EDGES[i + 1]:
                buckets[BUCKET_MIDPOINTS[i]].append(mkt - p)
                break
    points = [(mid, (sum(v) / len(v) if v else 0.0)) for mid, v in buckets.items()]
    xs = [x for x, _ in points]
    ys = [y for _, y in points]

    def correction(p):
        if p <= xs[0]:
            return ys[0]
        if p >= xs[-1]:
            return ys[-1]
        for i in range(len(xs) - 1):
            if xs[i] <= p <= xs[i + 1]:
                frac = (p - xs[i]) / (xs[i + 1] - xs[i])
                return ys[i] + frac * (ys[i + 1] - ys[i])
        return 0.0

    return correction, points


def apply_correction(p_home, p_draw, p_away, home_curve, away_curve):
    ch, ca = home_curve(p_home), away_curve(p_away)
    new_home = max(0.001, p_home + ch)
    new_away = max(0.001, p_away + ca)
    new_draw = max(0.001, 1.0 - new_home - new_away)
    total = new_home + new_draw + new_away
    return new_home / total, new_draw / total, new_away / total


def write_method(conn, league, rows_by_season, method, home_curve, away_curve):
    generated_at = datetime.now(timezone.utc).isoformat()
    inserted = 0
    for season, rows in rows_by_season.items():
        clear_soccer_model_predictions(league, season, method, conn=conn)
        for r in rows:
            p_h, p_d, p_a = apply_correction(r["p_home"], r["p_draw"], r["p_away"], home_curve, away_curve)
            add_soccer_model_prediction(
                match_id=r["match_id"], league=league,
                match_date=_match_date(conn, r["match_id"]),
                generated_at=generated_at, method=method,
                lambda_home=r["lambda_home"], lambda_away=r["lambda_away"],
                p_home=p_h, p_draw=p_d, p_away=p_a,
                over_under_line=r["over_under"], p_over=None, p_under=None,
                home_moneyline=r["home_moneyline"], draw_moneyline=r["draw_moneyline"],
                away_moneyline=r["away_moneyline"], over_odds=r["over_odds"], under_odds=r["under_odds"],
                ev_home=None, ev_draw=None, ev_away=None, ev_over=None, ev_under=None,
                conn=conn,
            )
            inserted += 1
    return inserted


_match_date_cache = {}


def _match_date(conn, match_id):
    if match_id not in _match_date_cache:
        cur = conn.cursor()
        cur.execute("SELECT match_date FROM soccer_matches WHERE match_id = ?", (match_id,))
        _match_date_cache[match_id] = cur.fetchone()[0]
    return _match_date_cache[match_id]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source-method", required=True)
    parser.add_argument("--league", default=LEAGUE_DEFAULT)
    args = parser.parse_args()

    conn = sqlite3.connect(DATABASE_PATH)
    rows_by_season = {s: load_rows(conn, args.league, s, args.source_method) for s in SEASONS}

    # Out-of-sample: fit on one season, apply to the OTHER only.
    for train_season, test_season in ((2024, 2025), (2025, 2024)):
        home_curve, home_points = fit_curve(rows_by_season[train_season], "p_home", "mkt_home")
        away_curve, away_points = fit_curve(rows_by_season[train_season], "p_away", "mkt_away")
        method = f"{args.source_method}_recal_ho{test_season}"
        n = write_method(conn, args.league, {test_season: rows_by_season[test_season]}, method, home_curve, away_curve)
        print(f"{method}: fit on {train_season}, applied to held-out {test_season} ({n} rows)")
        print(f"  home correction curve (bucket midpoint -> mkt-model): {[(round(x,3), round(y,4)) for x,y in home_points]}")
        print(f"  away correction curve (bucket midpoint -> mkt-model): {[(round(x,3), round(y,4)) for x,y in away_points]}")

    # In-sample (pooled fit, applied to both seasons) -- clearly a more optimistic
    # number, kept separate so it's never mistaken for the honest out-of-sample one.
    pooled_rows = rows_by_season[2024] + rows_by_season[2025]
    home_curve, home_points = fit_curve(pooled_rows, "p_home", "mkt_home")
    away_curve, away_points = fit_curve(pooled_rows, "p_away", "mkt_away")
    method = f"{args.source_method}_recal_insample"
    n = write_method(conn, args.league, rows_by_season, method, home_curve, away_curve)
    print(f"\n{method}: fit on BOTH seasons pooled, applied to BOTH (in-sample, optimistic) ({n} rows)")
    print(f"  home correction curve: {[(round(x,3), round(y,4)) for x,y in home_points]}")
    print(f"  away correction curve: {[(round(x,3), round(y,4)) for x,y in away_points]}")

    conn.close()


if __name__ == "__main__":
    main()
