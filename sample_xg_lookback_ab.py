"""
Sample-based A/B for team-xG lookback structure (decay / opponent-adjust)
without a full-season backfill.

Picks a stratified random sample of finished matches that already have
poisson_v4 predictions + Bet365 odds (and preferably Betfair closing),
recomputes each matchday twice (baseline defaults vs variant flags), and
prints:

  - mean |Δ p_home| and mean signed Δ p_home (variant - baseline)
  - agreement of baseline recompute vs stored poisson_v4 (sanity)
  - home-bet subset (EV>0, optional floor): calib / ROI baseline vs variant
  - gap_bf on home bets when Betfair is present

Point-in-time: same roster_as_of_date + compute() path as
backfill_player_blend_predictions.py.

Usage:
    python3 sample_xg_lookback_ab.py
    python3 sample_xg_lookback_ab.py --n 150 --seed 7
    python3 sample_xg_lookback_ab.py --n 80 --opp-adjust --decay 0.85
    python3 sample_xg_lookback_ab.py --league "Bundesliga" --n 60
"""

from __future__ import annotations

import argparse
import random
import sqlite3
import time
from collections import defaultdict
from itertools import groupby
from statistics import mean

from core.sports_db import DATABASE_PATH
from core.poisson_model import analyse_match_wc
from core.pick_guardrails import guardrail_reasons
from generate_club_league_card import CLUB_LEAGUE_MIN_PICK_PROBABILITY
import compute_club_player_strength as strength
from diagnose_home_bet_calibration import home_bet_row, summarize_bets, format_summary_line

DEFAULT_METHOD = "poisson_v4"
DEFAULT_SPORTSBOOK = "Bet365"
DEFAULT_SHARP = "Betfair Exchange"
DEFAULT_SEASONS = (2024, 2025)


def load_candidate_matches(conn, method, seasons, sportsbook, sharp_source, league=None):
    placeholders = ",".join("?" * len(seasons))
    params = [sportsbook, sharp_source, method, *seasons]
    league_sql = ""
    if league:
        league_sql = " AND mp.league = ?"
        params.append(league)
    cur = conn.cursor()
    cur.execute(f"""
        SELECT mp.match_id, mp.league, m.season, mp.match_date,
               m.home_team_id, m.away_team_id,
               m.home_score, m.away_score,
               mp.p_home AS stored_p_home,
               mp.lambda_home AS stored_lh, mp.lambda_away AS stored_la,
               o.home_moneyline, o.draw_moneyline, o.away_moneyline,
               o.over_under, o.over_odds, o.under_odds,
               mo.p_home_fair
        FROM soccer_model_predictions mp
        JOIN soccer_matches m ON m.match_id = mp.match_id
        JOIN soccer_betting_odds o
          ON o.match_id = mp.match_id AND o.sportsbook = ?
        LEFT JOIN soccer_market_odds mo
          ON mo.match_id = mp.match_id
         AND mo.source = ?
         AND mo.line_type = 'closing'
        WHERE mp.method = ?
          AND m.home_score IS NOT NULL
          AND m.season IN ({placeholders})
          {league_sql}
        ORDER BY mp.league, m.season, mp.match_date, mp.match_id
    """, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def stratified_sample(rows, n, seed):
    """Sample up to n rows, as even as possible across (league, season)."""
    if n <= 0 or n >= len(rows):
        return list(rows)
    by_key = defaultdict(list)
    for r in rows:
        by_key[(r["league"], r["season"])].append(r)
    keys = sorted(by_key)
    rng = random.Random(seed)
    for k in keys:
        rng.shuffle(by_key[k])

    # round-robin take until n
    out = []
    idx = {k: 0 for k in keys}
    while len(out) < n:
        progressed = False
        for k in keys:
            i = idx[k]
            if i < len(by_key[k]):
                out.append(by_key[k][i])
                idx[k] = i + 1
                progressed = True
                if len(out) >= n:
                    break
        if not progressed:
            break
    out.sort(key=lambda r: (r["league"], r["season"], r["match_date"], r["match_id"]))
    return out


def load_team_ids(conn, league, season):
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT t.team_id FROM soccer_teams t
        JOIN soccer_matches m ON m.home_team_id = t.team_id OR m.away_team_id = t.team_id
        WHERE m.league = ? AND m.season = ?
    """, (league, season))
    return [r[0] for r in cur.fetchall()]


def price_row(home, away, row):
    avg_home, avg_away = home["avg_home"], home["avg_away"]
    return analyse_match_wc(
        lambda_home_attack=home["lambda_attack_home_blend"],
        lambda_away_attack=away["lambda_attack_away_blend"],
        lambda_home_defense=home["lambda_defense_home_blend"],
        lambda_away_defense=away["lambda_defense_away_blend"],
        home_moneyline=row["home_moneyline"],
        draw_moneyline=row["draw_moneyline"],
        away_moneyline=row["away_moneyline"],
        ou_line=row["over_under"],
        over_odds=row["over_odds"],
        under_odds=row["under_odds"],
        baseline=avg_home,
        home_advantage=1.0,
        away_advantage=avg_home / avg_away if avg_away else 1.0,
    )


def home_bets_from_priced(priced_rows, floor):
    bets = []
    for pr in priced_rows:
        row = pr["row"]
        base = home_bet_row(
            p_home=pr["p_home"],
            home_ml=row["home_moneyline"],
            home_score=row["home_score"],
            away_score=row["away_score"],
            lambda_home=pr["lambda_H"],
            lambda_away=pr["lambda_A"],
            p_home_fair=row["p_home_fair"],
            floor=floor,
        )
        if base is None:
            continue
        base["league"] = row["league"]
        base["match_id"] = row["match_id"]
        bets.append(base)
    return bets


def run_variant(conn, sample, label, compute_kw, team_ids_cache, caches):
    """Recompute sample under one compute() config. Returns list of priced dicts."""
    # group by league, season, match_date for one compute per matchday
    sample_sorted = sorted(
        sample, key=lambda r: (r["league"], r["season"], r["match_date"], r["match_id"])
    )
    priced = []
    t0 = time.time()
    for (league, season, match_date), date_rows in groupby(
        sample_sorted, key=lambda r: (r["league"], r["season"], r["match_date"])
    ):
        date_rows = list(date_rows)
        key = (league, season)
        if key not in team_ids_cache:
            team_ids_cache[key] = load_team_ids(conn, league, season)
        team_ids = team_ids_cache[key]
        if label not in caches:
            caches[label] = {}
        cache = caches[label]
        roster_ids_by_team = {
            tid: strength.roster_as_of_date(conn, tid, season, match_date)
            for tid in team_ids
        }
        results = strength.compute(
            conn, team_ids, league, season, match_date,
            current_roster_ids_by_team=roster_ids_by_team,
            cache=cache,
            **compute_kw,
        )
        for row in date_rows:
            home = results[row["home_team_id"]]
            away = results[row["away_team_id"]]
            result = price_row(home, away, row)
            priced.append({
                "row": row,
                "p_home": result["p_home"],
                "p_draw": result["p_draw"],
                "p_away": result["p_away"],
                "lambda_H": result["lambda_H"],
                "lambda_A": result["lambda_A"],
                "away_att_team": away["lambda_attack_team_away"],
                "home_att_team": home["lambda_attack_team_home"],
            })
    elapsed = time.time() - t0
    return priced, elapsed


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--n", type=int, default=120,
                        help="Sample size (stratified by league×season). Default 120.")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--league", default=None)
    parser.add_argument("--season", dest="seasons", type=int, action="append")
    parser.add_argument("--method", default=DEFAULT_METHOD,
                        help="Stored method used to pick the sample universe.")
    parser.add_argument("--sportsbook", default=DEFAULT_SPORTSBOOK)
    parser.add_argument("--sharp-source", default=DEFAULT_SHARP)
    parser.add_argument("--opp-adjust", action="store_true", default=True,
                        help="Variant: xg_opponent_adjust=True (default on).")
    parser.add_argument("--no-opp-adjust", dest="opp_adjust", action="store_false")
    parser.add_argument("--decay", type=float, default=None,
                        help="Variant: xg_window_decay (default: leave at shipped 1.0).")
    parser.add_argument("--guardrail", dest="guardrail", action="store_true", default=True)
    parser.add_argument("--no-guardrail", dest="guardrail", action="store_false")
    args = parser.parse_args()
    seasons = tuple(args.seasons) if args.seasons else DEFAULT_SEASONS
    floor = CLUB_LEAGUE_MIN_PICK_PROBABILITY if args.guardrail else None

    conn = sqlite3.connect(DATABASE_PATH)
    universe = load_candidate_matches(
        conn, args.method, seasons, args.sportsbook, args.sharp_source, args.league,
    )
    if not universe:
        print("No candidate matches.")
        conn.close()
        return

    sample = stratified_sample(universe, args.n, args.seed)
    n_days = len({(r["league"], r["season"], r["match_date"]) for r in sample})
    by_ls = defaultdict(int)
    for r in sample:
        by_ls[(r["league"], r["season"])] += 1

    variant_kw = {}
    if args.opp_adjust:
        variant_kw["xg_opponent_adjust"] = True
    if args.decay is not None:
        variant_kw["xg_window_decay"] = args.decay
    if not variant_kw:
        print("Nothing to A/B (both opp-adjust off and no --decay). Enable a variant.")
        conn.close()
        return

    print(f"universe={len(universe)}  sample={len(sample)}  unique_matchdays={n_days}")
    print(f"seasons={list(seasons)}  league={args.league or 'ALL'}  seed={args.seed}")
    print(f"variant kwargs: {variant_kw}")
    print(f"home-bet floor: {floor if floor is not None else 'none'}")
    print("sample by league×season:")
    for k in sorted(by_ls):
        print(f"  {k[0]} {k[1]}: {by_ls[k]}")
    print()

    team_ids_cache = {}
    caches = {}

    print("Recomputing BASELINE (shipped defaults)...")
    base_priced, base_t = run_variant(conn, sample, "baseline", {}, team_ids_cache, caches)
    print(f"  done in {base_t:.1f}s")

    print("Recomputing VARIANT...")
    var_priced, var_t = run_variant(conn, sample, "variant", variant_kw, team_ids_cache, caches)
    print(f"  done in {var_t:.1f}s")
    conn.close()

    # index variant by match_id
    var_by_id = {p["row"]["match_id"]: p for p in var_priced}
    pairs = []
    for b in base_priced:
        mid = b["row"]["match_id"]
        v = var_by_id[mid]
        pairs.append((b, v))

    # sanity vs stored
    store_drifts = [abs(b["p_home"] - b["row"]["stored_p_home"]) for b, _ in pairs]
    print()
    print("=" * 72)
    print("SANITY: baseline recompute vs stored poisson_v4 p_home")
    print("=" * 72)
    print(f"  mean |Δ|={mean(store_drifts):.4f}  max|Δ|={max(store_drifts):.4f}  "
          f"n_exact(Δ<1e-6)={sum(1 for d in store_drifts if d < 1e-6)}/{len(store_drifts)}")

    d_p = [v["p_home"] - b["p_home"] for b, v in pairs]
    d_lh = [v["lambda_H"] - b["lambda_H"] for b, v in pairs]
    d_la = [v["lambda_A"] - b["lambda_A"] for b, v in pairs]
    d_aa = [v["away_att_team"] - b["away_att_team"] for b, v in pairs]

    print()
    print("=" * 72)
    print("ALL SAMPLED MATCHES  (variant − baseline)")
    print("=" * 72)
    print(f"  n={len(pairs)}")
    print(f"  mean Δ p_home={mean(d_p):+.4f}  mean |Δ p_home|={mean(abs(x) for x in d_p):.4f}")
    print(f"  mean Δ λ_H={mean(d_lh):+.3f}  mean Δ λ_A={mean(d_la):+.3f}  "
          f"mean Δ away_att_team={mean(d_aa):+.3f}")
    # how often p_home moves toward Betfair when both exist
    toward = []
    for b, v in pairs:
        bf = b["row"]["p_home_fair"]
        if bf is None:
            continue
        db = abs(b["p_home"] - bf)
        dv = abs(v["p_home"] - bf)
        toward.append(dv - db)  # negative = closer to Betfair
    if toward:
        print(f"  vs Betfair |p-bf|: mean Δ={mean(toward):+.4f}  "
              f"(neg=variant closer)  improved={sum(1 for x in toward if x < -1e-6)}/"
              f"{len(toward)}  worsened={sum(1 for x in toward if x > 1e-6)}/{len(toward)}")

    # by league
    print("\n  by league (mean Δ p_home, mean |Δ|, share closer to bf):")
    by_lg = defaultdict(list)
    for b, v in pairs:
        by_lg[b["row"]["league"]].append((b, v))
    for lg in sorted(by_lg):
        ps = by_lg[lg]
        dp = [v["p_home"] - b["p_home"] for b, v in ps]
        tw = []
        for b, v in ps:
            bf = b["row"]["p_home_fair"]
            if bf is None:
                continue
            tw.append(abs(v["p_home"] - bf) - abs(b["p_home"] - bf))
        closer = sum(1 for x in tw if x < -1e-6) if tw else 0
        print(f"    {lg:<16} n={len(ps):3d}  Δp={mean(dp):+.4f}  |Δp|={mean(abs(x) for x in dp):.4f}"
              + (f"  closer_bf={closer}/{len(tw)}" if tw else ""))

    # home bets
    print()
    print("=" * 72)
    print("HOME-BET SUBSET on this sample (EV>0, same floor as diagnose tool)")
    print("=" * 72)
    base_hb = home_bets_from_priced(base_priced, floor)
    var_hb = home_bets_from_priced(var_priced, floor)
    print(format_summary_line("baseline home bets", summarize_bets(base_hb)))
    print(format_summary_line("variant  home bets", summarize_bets(var_hb)))

    # paired: matches that are home bets under BOTH
    base_hb_ids = {b["match_id"]: b for b in base_hb}
    var_hb_ids = {b["match_id"]: b for b in var_hb}
    both = sorted(set(base_hb_ids) & set(var_hb_ids))
    only_base = sorted(set(base_hb_ids) - set(var_hb_ids))
    only_var = sorted(set(var_hb_ids) - set(base_hb_ids))
    print(f"\n  home-bet set: both={len(both)}  only_baseline={len(only_base)}  "
          f"only_variant={len(only_var)}")
    if both:
        b_both = [base_hb_ids[i] for i in both]
        v_both = [var_hb_ids[i] for i in both]
        print(format_summary_line("paired baseline", summarize_bets(b_both)))
        print(format_summary_line("paired variant ", summarize_bets(v_both)))
        d_gap = []
        for i in both:
            bb, vv = base_hb_ids[i], var_hb_ids[i]
            if bb.get("gap_bf") is not None and vv.get("gap_bf") is not None:
                d_gap.append(vv["gap_bf"] - bb["gap_bf"])
        if d_gap:
            print(f"  paired mean Δ gap_bf (var-base)={mean(d_gap):+.4f}  "
                  f"(neg=less overconfident vs Betfair)")

    print()
    print("Note: this is a SAMPLE, not full-population ROI. Use for direction /")
    print("effect size before committing to a multi-hour full backfill.")


if __name__ == "__main__":
    main()
