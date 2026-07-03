"""
FEATURE-009 threshold backtest: sweep the two-step selection's bars against every
graded pick to date, before wiring the logic into generate_wc_card.py.

For every graded match we recompute the FULL candidate set (current team strength,
the match's stored odds) and grade each candidate against the actual result via
core.grading.grade_pick — so we know, for every candidate on every match, not just
what the model picked historically but what EVERY side would have scored.

The two-step selection being tested:
  Step 1 (value mode)      — among candidates with model_prob >= B1 that also clear
                              the existing ratio/advance-edge guardrails, take the
                              highest-EV one if it's positive EV. Stop.
  Step 2 (prediction mode) — only if step 1 finds nothing: among ALL candidates with
                              market-IMPLIED prob >= B2, take the best payout (not
                              model EV — we've stopped trusting the model here).
  Fallback                 — neither step finds a qualifying candidate: highest
                              model-probability side (today's existing safety net).

Usage:
    python feature009_backtest.py                  # full grid sweep
    python feature009_backtest.py --b1 0.55 --b2 0.60   # one setting, per-game detail
"""

import argparse
import sqlite3

from core.sports_db import DATABASE_PATH, get_latest_wc_strength
from core.poisson_model import (
    analyse_match_wc, advance_probs, american_to_implied_prob,
    american_to_decimal, compute_ev,
)
from core.grading import grade_pick, advancing_side
from compute_wc_team_strength import compute_bench_indices
import generate_wc_card as gwc

# Existing guardrail constants, reused as-is for step 1's market-agreement check.
MAX_UNDERDOG_MARKET_DISAGREEMENT = gwc.MAX_UNDERDOG_MARKET_DISAGREEMENT
MAX_ADVANCE_ABSOLUTE_DISAGREEMENT = gwc.MAX_ADVANCE_ABSOLUTE_DISAGREEMENT


def parse_args():
    ap = argparse.ArgumentParser(description="Backtest FEATURE-009 two-step selection bars.")
    ap.add_argument("--b1", type=float, help="Single step-1 probability bar (skip grid, show detail).")
    ap.add_argument("--b2", type=float, help="Single step-2 implied-probability bar.")
    return ap.parse_args()


def load_games(conn):
    """One row per graded match: its full candidate set (odds/model_prob/implied/ev)
    plus each candidate's actual grade against the real result."""
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
        WHERE p.result IS NOT NULL
        GROUP BY m.match_id
    """).fetchall()

    bench = compute_bench_indices(conn)
    games = []
    for (mid, stage, hid, aid, home, away, hs, as_,
         eth, eta, sh, sa) in matches:
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
        home_adv = gwc.HOST_HOME_ADVANTAGE if home in gwc.HOST_NATIONS else 1.0
        away_adv = gwc.HOST_HOME_ADVANTAGE if away in gwc.HOST_NATIONS else 1.0

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
            games.append({"match_id": mid, "stage": stage, "home": home, "away": away,
                          "candidates": candidates})
    return games


def units(odds, result):
    if result == "win":
        return american_to_decimal(odds) - 1
    if result == "loss":
        return -1.0
    return 0.0


def select_two_step(candidates, b1, b2):
    """Return (chosen candidate dict, mode) for one game under bars (b1, b2)."""
    step1 = []
    for c in candidates:
        if c["prob"] < b1:
            continue
        if c["implied"] and c["prob"] >= MAX_UNDERDOG_MARKET_DISAGREEMENT * c["implied"]:
            continue
        if ("ADVANCE" in c["side"] and c["implied"] and c["implied"] < 0.5
                and c["prob"] - c["implied"] >= MAX_ADVANCE_ABSOLUTE_DISAGREEMENT):
            continue
        if c["ev"] > 0:
            step1.append(c)
    if step1:
        return max(step1, key=lambda c: c["ev"]), "value"

    step2 = [c for c in candidates if c["implied"] and c["implied"] >= b2]
    if step2:
        return max(step2, key=lambda c: american_to_decimal(c["odds"])), "prediction"

    return max(candidates, key=lambda c: c["prob"]), "fallback"


def simulate(games, b1, b2):
    modes = {"value": [], "prediction": [], "fallback": []}
    for g in games:
        chosen, mode = select_two_step(g["candidates"], b1, b2)
        u = units(chosen["odds"], chosen["result"])
        modes[mode].append((g, chosen, u))
    return modes


def report(modes):
    total_u = 0.0
    total_n = 0
    total_w = 0
    total_loss = 0
    for mode in ("value", "prediction", "fallback"):
        rows = modes[mode]
        n = len(rows)
        if n == 0:
            continue
        w = sum(1 for _, c, _ in rows if c["result"] == "win")
        loss_n = sum(1 for _, c, _ in rows if c["result"] == "loss")
        pu = sum(1 for _, c, _ in rows if c["result"] == "push")
        u = sum(x for _, _, x in rows)
        total_u += u
        total_n += n
        total_w += w
        total_loss += loss_n
        hit = 100 * w / (w + loss_n) if (w + loss_n) else 0.0
        print(f"    {mode:<11} n={n:<3} {w}-{loss_n}-{pu}  hit {hit:5.1f}%  {u:+.2f}u")
    total_hit = 100 * total_w / (total_w + total_loss) if (total_w + total_loss) else 0.0
    print(f"    {'TOTAL':<11} n={total_n:<3} {total_w}-{total_loss}  hit {total_hit:5.1f}%  {total_u:+.2f}u")
    return total_n, total_u


def main():
    args = parse_args()
    conn = sqlite3.connect(DATABASE_PATH)
    games = load_games(conn)
    conn.close()
    print(f"Loaded {len(games)} graded games.\n")

    if args.b1 and args.b2:
        print(f"=== b1={args.b1:g}  b2={args.b2:g} (all {len(games)} games) ===")
        modes = simulate(games, args.b1, args.b2)
        report(modes)

        stages = sorted({g["stage"] for g in games}, key=lambda s: [g["stage"] for g in games].index(s))
        if len(stages) > 1:
            print("\nBy stage:")
            for s in stages:
                sgames = [g for g in games if g["stage"] == s]
                print(f"  -- {s} ({len(sgames)} games) --")
                smodes = simulate(sgames, args.b1, args.b2)
                report(smodes)

        print(f"\nRobustness — neighboring cells around b1={args.b1:g}, b2={args.b2:g}:")
        neighborhood = [round(args.b1 + d, 2) for d in (-0.05, 0, 0.05)]
        b2_neighborhood = [round(args.b2 + d, 2) for d in (-0.05, 0, 0.05)]
        print(f"{'B1':>6}{'B2':>6}{'total u':>10}")
        for nb1 in neighborhood:
            for nb2 in b2_neighborhood:
                nmodes = simulate(games, nb1, nb2)
                nu = sum(x for mode in nmodes.values() for _, _, x in mode)
                marker = "  <-- chosen" if (nb1 == args.b1 and nb2 == args.b2) else ""
                print(f"{nb1:>6.2f}{nb2:>6.2f}{nu:>10.2f}{marker}")

        print()
        for mode in ("value", "prediction", "fallback"):
            for g, c, u in modes[mode]:
                print(f"  [{mode:<10}] {g['home']} v {g['away']:<22} {c['side']:<13} "
                      f"@ {c['odds']:+.0f}  model {c['prob']:.3f}  imp {c['implied']:.3f}  "
                      f"-> {c['result']:<5} ({u:+.2f}u)")
        return

    b1_grid = [0.40, 0.45, 0.50, 0.55, 0.60, 0.65]
    b2_grid = [0.55, 0.60, 0.65, 0.70, 0.75]
    print(f"{'B1':>6}{'B2':>6}{'value n':>10}{'pred n':>9}{'fall n':>8}{'total u':>10}")
    for b1 in b1_grid:
        for b2 in b2_grid:
            modes = simulate(games, b1, b2)
            nv, nu = len(modes["value"]), sum(x for _, _, x in modes["value"])
            npd, pu = len(modes["prediction"]), sum(x for _, _, x in modes["prediction"])
            nf, fu = len(modes["fallback"]), sum(x for _, _, x in modes["fallback"])
            total_u = nu + pu + fu
            print(f"{b1:>6.2f}{b2:>6.2f}{nv:>10}{npd:>9}{nf:>8}{total_u:>10.2f}")


if __name__ == "__main__":
    main()
