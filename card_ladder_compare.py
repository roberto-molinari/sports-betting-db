"""Side-by-side: today's card on the STATIC main line vs the FULL LADDER.

Both columns use the identical card pipeline (1X2 + totals candidates, run through the
real select_pick guardrails). The ONLY difference is the totals candidate set:
  - AS-IS : over/under at the single stored main line.
  - LADDER: over/under at every posted line (splits priced as quarter-bets).
This isolates exactly what FEATURE-003 would change.
"""
import sqlite3
from core.sports_db import DATABASE_PATH, get_latest_wc_strength
from core.poisson_model import (
    scoreline_grid, outcome_probs, totals_probs, compute_ev, compute_ev_totals,
    american_to_implied_prob, ev_to_stars, WC_BASELINE, WC_MAX_GOALS,
)
from generate_wc_card import select_pick
from core.wc_host_advantage import host_advantage
from core.wc_knockout_scale import knockout_goal_scale
from price_ladders import LADDERS  # match_id -> (name, {label: (lines, over, under)})

MAIN = {  # match_id -> (home_ml, draw_ml, away_ml, line, over_odds, under_odds)
    45: (-650, 700, 1500, 3.25, -105, -115),
    46: (-525, 575, 1300, 3.0, -112, -108),
    47: (575, 330, -206, 2.75, -118, -102),
    48: (-190, 295, 575, 2.25, -115, -105),
}


def grid_for(conn, mid):
    r = conn.execute("""SELECT m.home_team_id, m.away_team_id, m.stage, ht.name h, at.name a
        FROM soccer_wc_matches m JOIN soccer_wc_teams ht ON ht.team_id=m.home_team_id
        JOIN soccer_wc_teams at ON at.team_id=m.away_team_id
        WHERE m.match_id=?""", (mid,)).fetchone()
    h_att, h_def = get_latest_wc_strength(r["home_team_id"], conn=conn)
    a_att, a_def = get_latest_wc_strength(r["away_team_id"], conn=conn)
    level = knockout_goal_scale(r["stage"])
    home_adv = host_advantage(r["h"], r["stage"]) * level
    away_adv = host_advantage(r["a"], r["stage"]) * level
    lh = max(h_att * (a_def / WC_BASELINE) * home_adv, 0.1)
    la = max(a_att * (h_def / WC_BASELINE) * away_adv, 0.1)
    return r["h"], r["a"], scoreline_grid(lh, la, max_goals=WC_MAX_GOALS)


def cand(side, odds, prob, ev):
    return {"side": side, "odds": odds, "prob": prob, "ev": ev,
            "implied": american_to_implied_prob(odds)}


def moneyline_cands(grid, home_ml, draw_ml, away_ml):
    p = outcome_probs(grid)
    return [
        cand("HOME", home_ml, p["p_home"], compute_ev(p["p_home"], home_ml)),
        cand("DRAW", draw_ml, p["p_draw"], compute_ev(p["p_draw"], draw_ml)),
        cand("AWAY", away_ml, p["p_away"], compute_ev(p["p_away"], away_ml)),
    ]


def total_cand(grid, label, lines, over_odds, under_odds):
    """One over and one under candidate for a (possibly split) line, push-aware."""
    eos, eus, pos, pus = [], [], [], []
    for ln in lines:
        t = totals_probs(grid, ln)
        po, pu = t["p_over"], t["p_under"]
        eos.append(compute_ev_totals(po, pu, over_odds))
        eus.append(compute_ev_totals(pu, po, under_odds))
        pos.append(po)
        pus.append(pu)
    n = len(lines)
    return [cand(f"OVER {label}", over_odds, sum(pos)/n, sum(eos)/n),
            cand(f"UNDER {label}", under_odds, sum(pus)/n, sum(eus)/n)]


def fmt(pick):
    g = ""
    if pick.get("fallback"):
        g = " (fallback)"
    elif pick.get("demoted"):
        g = " (guardrail)"
    return (f"{pick['side']:<11} @{pick['odds']:+.0f}  EV {pick['ev']:+6.1%}  "
            f"p{pick['prob']:.3f}  {'⭐'*ev_to_stars(pick['ev'])}{g}")


def main():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    for mid in (45, 46, 47, 48):
        h, a, grid = grid_for(conn, mid)
        hml, dml, aml, line, oo, uo = MAIN[mid]
        ml = moneyline_cands(grid, hml, dml, aml)

        # AS-IS: 1X2 + over/under at the single main line.
        asis = ml + total_cand(grid, f"{line:g}", (line,), oo, uo)
        asis_pick = select_pick([dict(c) for c in asis])

        # LADDER: 1X2 + over/under at every posted line.
        ladder_cands = list(ml)
        for label, (lines, lo, lu) in LADDERS[mid][1].items():
            ladder_cands += total_cand(grid, label, lines, lo, lu)
        ladder_pick = select_pick([dict(c) for c in ladder_cands])

        same = "  (same pick)" if asis_pick["side"] == ladder_pick["side"] else "  <<< CHANGED"
        print(f"\n=== {h} vs {a} ==={same}")
        print(f"  AS-IS : {fmt(asis_pick)}")
        print(f"  LADDER: {fmt(ladder_pick)}")
    conn.close()


if __name__ == "__main__":
    main()
