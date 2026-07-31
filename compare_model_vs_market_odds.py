"""
Compare the model's 1X2 probabilities (soccer_model_predictions, method='poisson_v2')
against market implied probabilities (soccer_market_odds) for Serie A -- sharp books
(Pinnacle, Betfair Exchange) and a soft/retail book (Bet365) baseline. Closing lines
by default; pass line_type='opening' to compare against opening lines instead.

Per-season comparison plan (coverage-driven, see import_serie_a_market_odds.py):
  Sharp:
    2023-24: model vs Pinnacle only (no Betfair Exchange data that season).
    2024-25: model vs Pinnacle, and separately model vs Betfair Exchange.
    2025-26: model vs Betfair Exchange only (Pinnacle has a large post-01/2026 gap).
  Soft:
    2023-24 / 2024-25 / 2025-26: model vs Bet365 (complete all three seasons).

Usage:
    python compare_model_vs_market_odds.py
"""

import argparse
import sqlite3
import statistics as st

from core.sports_db import DATABASE_PATH

LEAGUE = "Serie A"
METHOD = "poisson_v3"
LINE_TYPE = "closing"

SHARP_SEASON_SOURCES = {
    2023: ["Pinnacle"],
    2024: ["Pinnacle", "Betfair Exchange"],
    2025: ["Betfair Exchange"],
}

SOFT_SEASON_SOURCES = {
    2023: ["Bet365"],
    2024: ["Bet365"],
    2025: ["Bet365"],
}


def fetch_pairs(conn, season, source, line_type=LINE_TYPE, method=METHOD):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT mp.p_home, mp.p_draw, mp.p_away,
               mo.p_home_fair, mo.p_draw_fair, mo.p_away_fair
        FROM soccer_model_predictions mp
        JOIN soccer_market_odds mo ON mo.match_id = mp.match_id
                                   AND mo.source = ? AND mo.line_type = ?
        JOIN soccer_matches m ON m.match_id = mp.match_id
        WHERE mp.league = ? AND mp.method = ? AND m.season = ?
        """,
        (source, line_type, LEAGUE, method, season)
    )
    return cur.fetchall()


def favored(p_home, p_draw, p_away):
    return max([("home", p_home), ("draw", p_draw), ("away", p_away)], key=lambda t: t[1])[0]


def summarize(pairs):
    n = len(pairs)
    diffs = {"home": [], "draw": [], "away": []}
    signed = {"home": [], "draw": [], "away": []}
    agree = 0

    for p_h, p_d, p_a, m_h, m_d, m_a in pairs:
        diffs["home"].append(abs(p_h - m_h))
        diffs["draw"].append(abs(p_d - m_d))
        diffs["away"].append(abs(p_a - m_a))
        signed["home"].append(p_h - m_h)
        signed["draw"].append(p_d - m_d)
        signed["away"].append(p_a - m_a)
        if favored(p_h, p_d, p_a) == favored(m_h, m_d, m_a):
            agree += 1

    return {
        "n": n,
        "mean_abs_diff": {k: st.mean(v) for k, v in diffs.items()},
        "mean_signed_diff": {k: st.mean(v) for k, v in signed.items()},
        "max_abs_diff": {k: max(v) for k, v in diffs.items()},
        "favored_agree_rate": agree / n if n else float("nan"),
    }


def run_table(conn, title, season_sources, line_type, method=METHOD):
    print(f"\n===== {title} =====")
    for season in sorted(season_sources):
        for source in season_sources[season]:
            pairs = fetch_pairs(conn, season, source, line_type=line_type, method=method)
            if not pairs:
                print(f"season={season} vs {source}: no overlapping rows")
                continue
            s = summarize(pairs)
            print(f"\nseason={season} vs {source}  (n={s['n']})")
            for side in ("home", "draw", "away"):
                print(f"  {side:5s}  mean_abs_diff={s['mean_abs_diff'][side]:.4f}  "
                      f"mean_signed_diff(model-market)={s['mean_signed_diff'][side]:+.4f}  "
                      f"max_abs_diff={s['max_abs_diff'][side]:.4f}")
            print(f"  favored-side agreement: {s['favored_agree_rate']*100:.1f}%")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--line-type", choices=["opening", "closing"], default=LINE_TYPE)
    parser.add_argument("--method", default=METHOD,
                        help=f"soccer_model_predictions.method to compare (default: {METHOD})")
    parser.add_argument("--seasons", type=int, nargs="+",
                        help="Restrict to these seasons only (default: all configured).")
    args = parser.parse_args()

    sharp = SHARP_SEASON_SOURCES
    soft = SOFT_SEASON_SOURCES
    if args.seasons:
        sharp = {s: v for s, v in SHARP_SEASON_SOURCES.items() if s in args.seasons}
        soft = {s: v for s, v in SOFT_SEASON_SOURCES.items() if s in args.seasons}

    conn = sqlite3.connect(DATABASE_PATH)
    run_table(conn, f"Table 1: {args.method} vs sharp books ({args.line_type})", sharp, args.line_type, args.method)
    run_table(conn, f"Table 2: {args.method} vs soft book ({args.line_type})", soft, args.line_type, args.method)
    conn.close()


if __name__ == "__main__":
    main()
