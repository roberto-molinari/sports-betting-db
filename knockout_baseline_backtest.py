"""
BUG-004 (reopened): backtest candidate knockout-stage goal-level corrections against
the complete Round of 32 (16 games), before wiring a fix into generate_wc_card.py.

totals_calibration.py showed R32's mean projected total (2.901) sits ~18% above the
mean actual (2.375) -- the OPPOSITE direction from the group stage, which slightly
UNDER-projects (2.776 vs 2.986). A single global WC_BASELINE change can't fix both at
once, so this tests a STAGE-SCOPED multiplicative scale instead: applied only when
stage != "Group", multiplied into the same home_advantage/away_advantage slot
analyse_match_wc already accepts (so it scales lambda_H and lambda_A uniformly,
without touching the shared WC_BASELINE or core/poisson_model.py at all).

For each candidate scale s, re-derives every graded R32 match's full candidate set
(current team strength, stored odds) and reports:
  - calibration: mean signed gap / mean |gap| between s*projected and actual totals
  - selection outcome: re-running FEATURE-009's locked thresholds (0.60/0.60) on the
    scaled lambdas -- W-L-P and units, same discipline as the FEATURE-009 backtest.

s=1.00 is today's baseline (no correction). s=0.819 is the exact value that zeroes
out R32's mean signed gap (2.375 / 2.901).

Usage:
    python knockout_baseline_backtest.py                # sweep
    python knockout_baseline_backtest.py --scale 0.85    # one setting, per-game detail
"""

import argparse
import sqlite3

from core.sports_db import DATABASE_PATH, get_latest_wc_strength
from core.poisson_model import (
    analyse_match_wc, advance_probs, american_to_implied_prob, compute_ev,
)
from core.wc_host_advantage import host_advantage
from core.grading import grade_pick, advancing_side
from compute_wc_team_strength import compute_bench_indices
from generate_wc_card import VALUE_MODE_MIN_PROBABILITY, PREDICTION_MODE_MIN_IMPLIED_PROBABILITY
from feature009_backtest import select_two_step, units


def parse_args():
    ap = argparse.ArgumentParser(description="Backtest BUG-004 knockout goal-level scale.")
    ap.add_argument("--scale", type=float, help="Single scale factor to test in detail.")
    ap.add_argument("--stage", default="R32", help="Stage to backtest against (default R32).")
    return ap.parse_args()


def load_games(conn, stage_filter, scale):
    """Same shape as feature009_backtest.load_games, but multiplies `scale` into
    both sides' venue-advantage factor for non-Group stages (BUG-004 knockout fix)."""
    matches = conn.execute("""
        SELECT m.match_id, m.stage, m.home_team_id, m.away_team_id,
               h.name AS home, a.name AS away,
               m.home_score, m.away_score,
               m.extra_time_home_score, m.extra_time_away_score,
               m.shootout_home_score, m.shootout_away_score
        FROM soccer_wc_matches m
        JOIN soccer_wc_teams h ON h.team_id = m.home_team_id
        JOIN soccer_wc_teams a ON a.team_id = m.away_team_id
        JOIN soccer_wc_picks p ON p.match_id = m.match_id
        WHERE p.result IS NOT NULL AND m.stage = ?
        GROUP BY m.match_id
    """, (stage_filter,)).fetchall()

    bench = compute_bench_indices(conn)
    games = []
    for (mid, stage, hid, aid, home, away, hs, as_, eth, eta, sh, sa) in matches:
        odds = conn.execute("""
            SELECT * FROM soccer_wc_odds WHERE match_id = ?
            ORDER BY CASE sportsbook WHEN 'Bovada' THEN 0 ELSE 1 END LIMIT 1
        """, (mid,)).fetchone()
        cols = [d[0] for d in conn.execute("SELECT * FROM soccer_wc_odds LIMIT 1").description]
        o = dict(zip(cols, odds))

        h_strength = get_latest_wc_strength(hid, conn=conn)
        a_strength = get_latest_wc_strength(aid, conn=conn)
        if h_strength is None or a_strength is None:
            continue
        h_att, h_def = h_strength
        a_att, a_def = a_strength
        level = scale if stage != "Group" else 1.0
        home_adv = host_advantage(home, stage) * level
        away_adv = host_advantage(away, stage) * level

        r = analyse_match_wc(
            h_att, a_att, h_def, a_def,
            home_moneyline=o["home_moneyline"], draw_moneyline=o["draw_moneyline"],
            away_moneyline=o["away_moneyline"], ou_line=o["over_under"],
            over_odds=o["over_odds"], under_odds=o["under_odds"],
            home_advantage=home_adv, away_advantage=away_adv)

        line = o["over_under"]
        ll = f"{line:g}" if line is not None else ""
        cands = [
            ("HOME", o["home_moneyline"], r.get("p_home"), r.get("ev_home")),
            ("DRAW", o["draw_moneyline"], r.get("p_draw"), r.get("ev_draw")),
            ("AWAY", o["away_moneyline"], r.get("p_away"), r.get("ev_away")),
            (f"OVER {ll}", o["over_odds"], r.get("p_over"), r.get("ev_over")),
            (f"UNDER {ll}", o["under_odds"], r.get("p_under"), r.get("ev_under")),
        ]
        if o["home_advance_ml"] is not None and o["away_advance_ml"] is not None:
            adv = advance_probs(r["lambda_H"], r["lambda_A"],
                                 bench_index_home=bench.get(hid, 0.0),
                                 bench_index_away=bench.get(aid, 0.0))
            cands.append(("HOME ADVANCE", o["home_advance_ml"], adv["p_home_advance"],
                          compute_ev(adv["p_home_advance"], o["home_advance_ml"])))
            cands.append(("AWAY ADVANCE", o["away_advance_ml"], adv["p_away_advance"],
                          compute_ev(adv["p_away_advance"], o["away_advance_ml"])))

        advanced = advancing_side(hs, as_, eth, eta, sh, sa) if stage != "Group" else None
        outcome = {"regulation_home": hs, "regulation_away": as_, "advanced": advanced}

        candidates = []
        for side, odds_val, prob, ev in cands:
            if odds_val is None or prob is None or ev is None:
                continue
            try:
                result = grade_pick(side, outcome)
            except ValueError:
                continue   # e.g. ADVANCE side but no decided tie (shouldn't happen for graded rows)
            candidates.append({
                "side": side, "odds": odds_val, "prob": prob, "ev": ev,
                "implied": american_to_implied_prob(odds_val), "result": result,
            })
        if candidates:
            games.append({
                "match_id": mid, "home": home, "away": away, "candidates": candidates,
                "proj_total": r["lambda_H"] + r["lambda_A"], "actual_total": hs + as_,
            })
    return games


def calibration(games):
    gaps = [g["proj_total"] - g["actual_total"] for g in games]
    n = len(gaps)
    signed = sum(gaps) / n
    mae = sum(abs(x) for x in gaps) / n
    return signed, mae


def selection_report(games):
    total_n = total_w = total_l = total_pu = 0
    total_u = 0.0
    for g in games:
        chosen, mode = select_two_step(g["candidates"], VALUE_MODE_MIN_PROBABILITY,
                                       PREDICTION_MODE_MIN_IMPLIED_PROBABILITY)
        u = units(chosen["odds"], chosen["result"])
        total_n += 1
        total_u += u
        if chosen["result"] == "win":
            total_w += 1
        elif chosen["result"] == "loss":
            total_l += 1
        else:
            total_pu += 1
    return total_n, total_w, total_l, total_pu, total_u


def main():
    args = parse_args()
    conn = sqlite3.connect(DATABASE_PATH)

    if args.scale:
        games = load_games(conn, args.stage, args.scale)
        signed, mae = calibration(games)
        n, w, loss_n, pu, u = selection_report(games)
        print(f"=== scale={args.scale:g}  stage={args.stage} ===")
        print(f"Calibration: mean signed gap {signed:+.2f}  mean |gap| {mae:.2f}")
        print(f"Selection:   n={n}  {w}-{loss_n}-{pu}  {u:+.2f}u")
        print()
        for g in games:
            chosen, mode = select_two_step(g["candidates"], VALUE_MODE_MIN_PROBABILITY,
                                           PREDICTION_MODE_MIN_IMPLIED_PROBABILITY)
            u = units(chosen["odds"], chosen["result"])
            print(f"  {g['home']} v {g['away']:<22} proj {g['proj_total']:.2f} actual "
                  f"{g['actual_total']}  -> [{mode:<10}] {chosen['side']:<13} "
                  f"@ {chosen['odds']:+.0f} -> {chosen['result']:<5} ({u:+.2f}u)")
        conn.close()
        return

    print(f"{'SCALE':>6}{'SIGNED GAP':>12}{'MAE':>7}{'W':>4}{'L':>4}{'P':>4}{'UNITS':>9}")
    for s in (1.00, 0.95, 0.90, 0.85, 0.819, 0.80, 0.75):
        games = load_games(conn, args.stage, s)
        signed, mae = calibration(games)
        n, w, loss_n, pu, u = selection_report(games)
        print(f"{s:>6.3f}{signed:>+12.2f}{mae:>7.2f}{w:>4}{loss_n:>4}{pu:>4}{u:>+9.2f}")
    conn.close()


if __name__ == "__main__":
    main()
