"""
Per-match knockout pick review: ROI, selection mode, and the model's goals proxy
vs. official FIFA xG vs. actual, side by side.

For every graded knockout pick (Round of 32 / Round of 16 / Quarterfinal) this shows:
  - the match, final score (with ET/PK where applicable), and the actual stored pick
    with its ROI
  - the pick's selection mode -- "legacy" for picks made before FEATURE-009 shipped
    (2026-07-03, plain single-highest-EV selection, no value/prediction/fallback
    tiers existed yet), else the real stored value/prediction/fallback label
  - the top candidates from that SAME mode's pool (FEATURE-010's mode_breakdown,
    or the raw top-EV list for legacy picks) -- so the alternatives shown are what
    was actually in play for that tier, not just an unfiltered EV ranking dominated
    by guardrail-excluded longshots
  - each team's model proxy goals for the match (analyse_match_wc's lambda_H/lambda_A,
    the same per-match quantity proxy_goals_calibration.py sums across a season),
    the official FIFA xG (soccer_wc_external_xg, source='fifa_official') where on
    file, and the gaps proxy-vs-actual and proxy-vs-FIFA-xG for both teams

Uses CURRENT team strength / odds / bench data -- same convention as
proxy_goals_calibration.py / proxy_defense_calibration.py -- not a historical
snapshot (neither is retained), so a pick's displayed alternatives can differ
slightly from what was actually in play at generation time if the book has since
moved a line. The pick's own stored odds/model_prob/result/ROI are always exact.

Usage:
    python knockout_pick_review.py                  # every graded knockout stage
    python knockout_pick_review.py --stage R16
"""

import argparse
import sqlite3

from core.sports_db import DATABASE_PATH, get_latest_wc_strength
from core.poisson_model import (
    analyse_match_wc, advance_probs, american_to_implied_prob, compute_ev,
)
from core.wc_host_advantage import host_advantage
from core.wc_knockout_scale import knockout_goal_scale
from compute_wc_team_strength import compute_bench_indices
from generate_wc_card import select_pick, mode_breakdown

FIFA_XG_SOURCE = "fifa_official"


def parse_args():
    ap = argparse.ArgumentParser(description="Per-match knockout pick review.")
    ap.add_argument("--stage", choices=["R32", "R16", "QF", "SF", "F"],
                    help="Limit to one knockout stage (default: all, in match order).")
    return ap.parse_args()


def score_str(m):
    s = f"{m['home_score']}-{m['away_score']}"
    if m["extra_time_home_score"] is not None:
        s += f" (ET {m['extra_time_home_score']}-{m['extra_time_away_score']})"
    if m["shootout_home_score"] is not None:
        s += f" (PK {m['shootout_home_score']}-{m['shootout_away_score']})"
    return s


def profit(odds, result):
    if result == "push":
        return 0.0
    if result == "loss":
        return -1.0
    return odds / 100.0 if odds > 0 else 100.0 / abs(odds)


def roi_summary(picks):
    """(n, wins, losses, pushes, total_profit, roi_pct) for a list of pick rows.
    Pushes are excluded from the staked total (no capital at risk)."""
    n = len(picks)
    wins = sum(1 for p in picks if p["result"] == "win")
    losses = sum(1 for p in picks if p["result"] == "loss")
    pushes = sum(1 for p in picks if p["result"] == "push")
    total_profit = sum(profit(p["odds"], p["result"]) for p in picks)
    staked = n - pushes
    roi = total_profit / staked * 100 if staked else 0.0
    return n, wins, losses, pushes, total_profit, roi


def print_roi_line(label, picks):
    n, wins, losses, pushes, total_profit, roi = roi_summary(picks)
    print(f"{label}: n={n}  W-L-P {wins}-{losses}-{pushes}  "
          f"profit={total_profit:+.2f}u  ROI={roi:+.1f}%")


def price_match(m, bench_indices):
    """Return (priced, lambda_H, lambda_A) for a match row using CURRENT team
    strength/odds -- mirrors generate_wc_card.best_pick_for_match's candidate
    construction, duplicated here (not imported) so this stays a read-only
    diagnostic with no dependency on the live pipeline's DB writes."""
    home_strength = get_latest_wc_strength(m["home_team_id"])
    away_strength = get_latest_wc_strength(m["away_team_id"])
    if home_strength is None or away_strength is None:
        return None, None, None
    h_att, h_def = home_strength
    a_att, a_def = away_strength

    level = knockout_goal_scale(m["stage"])
    home_adv = host_advantage(m["home"], m["stage"]) * level
    away_adv = host_advantage(m["away"], m["stage"]) * level

    r = analyse_match_wc(
        lambda_home_attack=h_att, lambda_away_attack=a_att,
        lambda_home_defense=h_def, lambda_away_defense=a_def,
        home_moneyline=m["home_moneyline"], draw_moneyline=m["draw_moneyline"],
        away_moneyline=m["away_moneyline"], ou_line=m["over_under"],
        over_odds=m["over_odds"], under_odds=m["under_odds"],
        home_advantage=home_adv, away_advantage=away_adv,
    )
    line_label = f"{m['over_under']:g}" if m["over_under"] is not None else ""
    candidates = [
        ("HOME", m["home_moneyline"], r.get("p_home"), r.get("ev_home")),
        ("DRAW", m["draw_moneyline"], r.get("p_draw"), r.get("ev_draw")),
        ("AWAY", m["away_moneyline"], r.get("p_away"), r.get("ev_away")),
        (f"OVER {line_label}", m["over_odds"], r.get("p_over"), r.get("ev_over")),
        (f"UNDER {line_label}", m["under_odds"], r.get("p_under"), r.get("ev_under")),
    ]
    if m["home_advance_ml"] is not None and m["away_advance_ml"] is not None:
        adv = advance_probs(
            r["lambda_H"], r["lambda_A"],
            bench_index_home=bench_indices.get(m["home_team_id"], 0.0),
            bench_index_away=bench_indices.get(m["away_team_id"], 0.0))
        candidates.append(("HOME ADVANCE", m["home_advance_ml"], adv["p_home_advance"],
                            compute_ev(adv["p_home_advance"], m["home_advance_ml"])))
        candidates.append(("AWAY ADVANCE", m["away_advance_ml"], adv["p_away_advance"],
                            compute_ev(adv["p_away_advance"], m["away_advance_ml"])))

    priced = [
        {"side": side, "odds": odds, "prob": prob, "ev": ev,
         "implied": american_to_implied_prob(odds)}
        for side, odds, prob, ev in candidates
        if ev is not None and odds is not None and prob is not None
    ]
    return priced, r["lambda_H"], r["lambda_A"]


def pool_for_mode(priced, stored_mode, top_n=3):
    """The candidates 'in play' for the mode that actually produced this pick.
    Legacy (pre-FEATURE-009) picks used plain highest-EV selection -- mode_breakdown's
    own top_ev list (no guardrail/probability filter) is the correct analogue."""
    select_pick(priced)   # side effect: sets excluded_by on every candidate
    breakdown = mode_breakdown(priced, top_n=top_n)
    return breakdown[stored_mode if stored_mode in breakdown else "top_ev"]


def fetch_picks(conn, stage=None):
    where = "m.stage IN ('R32','R16','QF','SF','F') AND p.result IS NOT NULL"
    params = ()
    if stage:
        where = "m.stage = ? AND p.result IS NOT NULL"
        params = (stage,)
    return conn.execute(f"""
        SELECT p.pick_id, p.match_id, p.side, p.odds, p.model_prob, p.result,
               p.selection_mode, m.stage, m.match_date, m.home_team_id, m.away_team_id,
               ht.name AS home, at.name AS away,
               m.home_score, m.away_score, m.extra_time_home_score, m.extra_time_away_score,
               m.shootout_home_score, m.shootout_away_score,
               o.home_moneyline, o.draw_moneyline, o.away_moneyline,
               o.over_under, o.over_odds, o.under_odds,
               o.home_advance_ml, o.away_advance_ml
        FROM soccer_wc_picks p
        JOIN soccer_wc_matches m ON p.match_id = m.match_id
        JOIN soccer_wc_teams ht ON m.home_team_id = ht.team_id
        JOIN soccer_wc_teams at ON m.away_team_id = at.team_id
        JOIN soccer_wc_odds o ON o.match_id = m.match_id AND o.sportsbook = 'Bovada'
        WHERE {where}
        ORDER BY m.match_date
    """, params).fetchall()


def main():
    args = parse_args()
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row

    bench_indices = compute_bench_indices(conn)
    picks = fetch_picks(conn, args.stage)

    current_stage = None
    stage_picks = []
    for p in picks:
        if p["stage"] != current_stage:
            if current_stage is not None:
                print_roi_line(f"\n{current_stage} TOTAL", stage_picks)
            current_stage = p["stage"]
            stage_picks = []
            print(f"\n{'=' * 22} {current_stage} {'=' * 22}")
        stage_picks.append(p)

        fifa_xg = conn.execute(
            "SELECT home_xg, away_xg FROM soccer_wc_external_xg "
            "WHERE match_id = ? AND source = ?", (p["match_id"], FIFA_XG_SOURCE)
        ).fetchone()
        home_fifa_xg, away_fifa_xg = fifa_xg if fifa_xg else (None, None)

        priced, lambda_h, lambda_a = price_match(p, bench_indices)

        pl = profit(p["odds"], p["result"])
        date_str = p["match_date"][:10]
        stored_mode = p["selection_mode"] or "legacy"
        print(f"\n[{p['stage']}] {date_str}  {p['home']} v {p['away']}  "
              f"FINAL {score_str(p)}  (match {p['match_id']})")
        if p['result'] == 'win':
            print(f"  PICK [{stored_mode.upper()}]: {p['side']} @ {p['odds']:+.0f}  "
                  f"model_prob={p['model_prob']:.1%}  result=\033[32m{p['result'].upper()}\033[0m  "
                  f"P/L={pl:+.2f}u  ROI={pl * 100:+.1f}%")
        else:
            print(f"  PICK [{stored_mode.upper()}]: {p['side']} @ {p['odds']:+.0f}  "
                  f"model_prob={p['model_prob']:.1%}  result=\033[31m{p['result'].upper()}\033[0m  "
                  f"P/L={pl:+.2f}u  ROI={pl * 100:+.1f}%")

        if priced is None:
            print("  (no current team strength -- can't reconstruct candidates)")
        else:
            top = pool_for_mode(priced, stored_mode)
            label = "top-EV" if stored_mode == "legacy" else stored_mode
            print(f"  Top {len(top)} candidates in the {label} pool "
                  f"(reconstructed from current odds/strength, approximate):")
            for i, c in enumerate(top, 1):
                flag = (" <= PICKED" if c["side"] == p["side"]
                        and abs(c["odds"] - p["odds"]) < 1e-6 else "")
                print(f"    {i}. {c['side']:<14} @ {c['odds']:>+7.0f}  "
                      f"model={c['prob']:.1%}  implied={c['implied']:.1%}  "
                      f"EV={c['ev']:+.1%}{flag}")

        def cell(v, signed=False):
            if v is None:
                return f"{'n/a':>14}"
            return f"{v:{'+' if signed else ''}.2f}".rjust(14)

        home_gap_actual = lambda_h - p["home_score"] if lambda_h is not None else None
        away_gap_actual = lambda_a - p["away_score"] if lambda_a is not None else None
        home_gap_fifa = (lambda_h - home_fifa_xg
                          if lambda_h is not None and home_fifa_xg is not None else None)
        away_gap_fifa = (lambda_a - away_fifa_xg
                          if lambda_a is not None and away_fifa_xg is not None else None)

        print(f"  {'GOALS PROXY':<16}{p['home']:>14}{p['away']:>14}")
        print(f"  {'model proxy':<16}{cell(lambda_h)}{cell(lambda_a)}")
        print(f"  {'actual (90m)':<16}{p['home_score']:>14}{p['away_score']:>14}")
        print(f"  {'FIFA xG':<16}{cell(home_fifa_xg)}{cell(away_fifa_xg)}")
        print(f"  {'gap vs actual':<16}{cell(home_gap_actual, signed=True)}{cell(away_gap_actual, signed=True)}")
        print(f"  {'gap vs FIFA xG':<16}{cell(home_gap_fifa, signed=True)}{cell(away_gap_fifa, signed=True)}")

    if current_stage is not None:
        print_roi_line(f"\n{current_stage} TOTAL", stage_picks)

    if args.stage is None and picks:
        print(f"\n{'=' * 22} OVERALL {'=' * 22}")
        print_roi_line("ALL KNOCKOUT STAGES TOTAL", picks)

    conn.close()


if __name__ == "__main__":
    main()
