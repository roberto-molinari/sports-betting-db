"""
Compare the model's 1X2 probabilities (soccer_model_predictions) against market
implied probabilities (soccer_market_odds) for a league -- sharp books (Pinnacle,
Betfair Exchange) and a soft/retail book (Bet365) baseline. Closing lines by default;
pass line_type='opening' to compare against opening lines instead. Generalized
2026-08-10 (multi-league expansion) via --league; SHARP_SEASON_SOURCES/
SOFT_SEASON_SOURCES are now per-league, keyed the same way core/leagues.py's LEAGUES
registry is.

Per-season comparison plan is COVERAGE-DRIVEN, not assumed -- each league's entry
below reflects what import_league_market_odds.py actually found when it ran (its
per-source/line_type skipped_no_odds counts), not a copy of Serie A's numbers. Serie
A's own documented gaps (see import_league_market_odds.py's docstring):
  Sharp:
    2023-24: model vs Pinnacle only (no Betfair Exchange data that season).
    2024-25: model vs Pinnacle, and separately model vs Betfair Exchange.
    2025-26: model vs Betfair Exchange only (Pinnacle has a large post-01/2026 gap).
  Soft:
    2023-24 / 2024-25 / 2025-26: model vs Bet365 (complete all three seasons).

Usage:
    python compare_model_vs_market_odds.py --league "Serie A"
    python compare_model_vs_market_odds.py --league "Premier League"
"""

import argparse
import sqlite3
import statistics as st

from core.sports_db import DATABASE_PATH
from core.leagues import LEAGUES

METHOD = "poisson_v3"
LINE_TYPE = "closing"

# Checked 2026-08-10 from import_league_market_odds.py's own real per-source
# skipped_no_odds counts on the 2024/2025 seasons actually imported -- the SAME
# pattern as Serie A shows up in all 4 new leagues: 2024 fully complete on every
# source; 2025 has a large Pinnacle gap (source-side data lag on the current
# in-progress season, not real unavailability -- PL 210/380, Bundesliga 150/306,
# La Liga 189/380, Ligue 1 153/306 present) and a small Betfair Exchange gap (92-95%
# complete), so 2025 excludes Pinnacle the same way Serie A's own 2025-26 entry does.
SHARP_SEASON_SOURCES = {
    "Serie A": {
        2023: ["Pinnacle"],
        2024: ["Pinnacle", "Betfair Exchange"],
        2025: ["Betfair Exchange"],
    },
    "Premier League": {2024: ["Pinnacle", "Betfair Exchange"], 2025: ["Betfair Exchange"]},
    "Bundesliga": {2024: ["Pinnacle", "Betfair Exchange"], 2025: ["Betfair Exchange"]},
    "La Liga": {2024: ["Pinnacle", "Betfair Exchange"], 2025: ["Betfair Exchange"]},
    "Ligue 1": {2024: ["Pinnacle", "Betfair Exchange"], 2025: ["Betfair Exchange"]},
}

SOFT_SEASON_SOURCES = {
    "Serie A": {2023: ["Bet365"], 2024: ["Bet365"], 2025: ["Bet365"]},
    "Premier League": {2024: ["Bet365"], 2025: ["Bet365"]},
    "Bundesliga": {2024: ["Bet365"], 2025: ["Bet365"]},
    "La Liga": {2024: ["Bet365"], 2025: ["Bet365"]},
    "Ligue 1": {2024: ["Bet365"], 2025: ["Bet365"]},
}


def fetch_pairs(conn, league, season, source, line_type=LINE_TYPE, method=METHOD):
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
        (source, line_type, league, method, season)
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


def run_table(conn, league, title, season_sources, line_type, method=METHOD):
    print(f"\n===== {title} =====")
    for season in sorted(season_sources):
        for source in season_sources[season]:
            pairs = fetch_pairs(conn, league, season, source, line_type=line_type, method=method)
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
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--league", default="Serie A", choices=sorted(LEAGUES),
                        help="League name, must be a key in core/leagues.py's LEAGUES registry.")
    parser.add_argument("--line-type", choices=["opening", "closing"], default=LINE_TYPE)
    parser.add_argument("--method", default=METHOD,
                        help=f"soccer_model_predictions.method to compare (default: {METHOD})")
    parser.add_argument("--seasons", type=int, nargs="+",
                        help="Restrict to these seasons only (default: all configured).")
    args = parser.parse_args()

    if args.league not in SHARP_SEASON_SOURCES:
        raise SystemExit(f"No SHARP_SEASON_SOURCES/SOFT_SEASON_SOURCES entry for '{args.league}' yet -- "
                         f"check its football-data.co.uk coverage per season/book and add one "
                         f"(see module docstring; don't copy another league's blind).")

    sharp = SHARP_SEASON_SOURCES[args.league]
    soft = SOFT_SEASON_SOURCES[args.league]
    if args.seasons:
        sharp = {s: v for s, v in sharp.items() if s in args.seasons}
        soft = {s: v for s, v in soft.items() if s in args.seasons}

    conn = sqlite3.connect(DATABASE_PATH)
    run_table(conn, args.league, f"Table 1: {args.league} {args.method} vs sharp books ({args.line_type})",
             sharp, args.line_type, args.method)
    run_table(conn, args.league, f"Table 2: {args.league} {args.method} vs soft book ({args.line_type})",
             soft, args.line_type, args.method)
    conn.close()


if __name__ == "__main__":
    main()
