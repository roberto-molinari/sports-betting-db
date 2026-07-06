"""One-off: price every posted O/U total line for the Jun 23 slate against the model.

Manual stand-in for FEATURE-003. For each match, build the model scoreline grid from
stored strengths (same lambda shape as generate_wc_card), then for each book line compute
push-aware EV on over and under and report the best line per game.

Split lines ("2, 2.5") are QUARTER bets: half the stake on each component line at the same
quoted odds, so EV = mean of the two legs (each priced exactly, push-aware on whole lines).
"""
import sqlite3
from core.sports_db import DATABASE_PATH, get_latest_wc_strength
from core.poisson_model import (
    scoreline_grid, totals_probs, compute_ev_totals, american_to_implied_prob,
)
from core.poisson_model import WC_BASELINE, WC_MAX_GOALS
from core.wc_host_advantage import host_advantage
from core.wc_knockout_scale import knockout_goal_scale

# label -> (component_lines, over_american, under_american).
# Single line -> one component; split/quarter line -> two components.
LADDERS = {
    45: ("Portugal", {  # vs Uzbekistan
        "1.5": ((1.5,), -650, 435), "2,2.5": ((2.0, 2.5), -300, 235),
        "2.5": ((2.5,), -210, 169), "2.5,3": ((2.5, 3.0), -175, 143),
        "3.0": ((3.0,), -136, 113), "3.5": ((3.5,), 122, -148),
        "3.5,4": ((3.5, 4.0), 156, -190), "4.0": ((4.0,), 220, -275),
        "4,4.5": ((4.0, 4.5), 255, -320),
    }),
    46: ("England", {  # vs Ghana
        "2.0": ((2.0,), -410, 305), "2,2.5": ((2.0, 2.5), -250, 200),
        "2.5": ((2.5,), -180, 147), "2.5,3": ((2.5, 3.0), -145, 120),
        "3,3.5": ((3.0, 3.5), 115, -140), "3.5": ((3.5,), 140, -170),
        "3.5,4": ((3.5, 4.0), 183, -225), "4.0": ((4.0,), 265, -340),
        "4.5": ((4.5,), 400, -575), "5.5": ((5.5,), 850, -1700),
    }),
    47: ("Panama", {  # vs Croatia
        "0.5": ((0.5,), -1750, 850), "1.5,2": ((1.5, 2.0), -400, 300),
        "2.0": ((2.0,), -320, 255), "2,2.5": ((2.0, 2.5), -206, 167),
        "2.5": ((2.5,), -148, 122), "3.0": ((3.0,), 112, -134),
        "3,3.5": ((3.0, 3.5), 140, -170), "3.5": ((3.5,), 167, -206),
        "3.5,4": ((3.5, 4.0), 220, -275),
    }),
    48: ("Colombia", {  # vs DR Congo
        "0.5": ((0.5,), -1250, 750), "1,1.5": ((1.0, 1.5), -430, 320),
        "1.5": ((1.5,), -270, 215), "1.5,2": ((1.5, 2.0), -217, 175),
        "2.0": ((2.0,), -168, 138), "2.5": ((2.5,), 114, -138),
        "2.5,3": ((2.5, 3.0), 150, -183), "3.0": ((3.0,), 215, -270),
        "3,3.5": ((3.0, 3.5), 255, -320),
    }),
}


def ev_for_line(grid, lines, over_odds, under_odds):
    """Return (ev_over, ev_under, mean_p_over, mean_p_under) for a single or split line."""
    eos, eus, pos, pus = [], [], [], []
    for line in lines:
        t = totals_probs(grid, line)
        po, pu = t["p_over"], t["p_under"]
        eos.append(compute_ev_totals(po, pu, over_odds))
        eus.append(compute_ev_totals(pu, po, under_odds))
        pos.append(po)
        pus.append(pu)
    n = len(lines)
    return sum(eos) / n, sum(eus) / n, sum(pos) / n, sum(pus) / n


def model_grid(conn, match_id):
    r = conn.execute("""SELECT m.home_team_id, m.away_team_id, m.stage, ht.name h, at.name a
        FROM soccer_wc_matches m JOIN soccer_wc_teams ht ON ht.team_id=m.home_team_id
        JOIN soccer_wc_teams at ON at.team_id=m.away_team_id
        WHERE m.match_id=?""", (match_id,)).fetchone()
    h_att, h_def = get_latest_wc_strength(r["home_team_id"], conn=conn)
    a_att, a_def = get_latest_wc_strength(r["away_team_id"], conn=conn)
    level = knockout_goal_scale(r["stage"])
    home_adv = host_advantage(r["h"], r["stage"]) * level
    away_adv = host_advantage(r["a"], r["stage"]) * level
    lambda_H = max(h_att * (a_def / WC_BASELINE) * home_adv, 0.1)
    lambda_A = max(a_att * (h_def / WC_BASELINE) * away_adv, 0.1)
    grid = scoreline_grid(lambda_H, lambda_A, max_goals=WC_MAX_GOALS)
    return r["h"], r["a"], lambda_H, lambda_A, grid


def main():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    for match_id, (_, ladder) in LADDERS.items():
        h, a, lh, la, grid = model_grid(conn, match_id)
        print(f"\n=== {h} vs {a}  (model λ {lh:.2f}/{la:.2f}, total {lh+la:.2f}) ===")
        print(f"{'line':>7} {'mP(o)':>7} {'mP(u)':>7} | {'O odds':>7} {'EV over':>8} "
              f"| {'U odds':>7} {'EV und':>8}")
        best = None
        for label, (lines, oo, uo) in ladder.items():
            ev_o, ev_u, po, pu = ev_for_line(grid, lines, oo, uo)
            if ev_o >= ev_u:
                cand = (label, "OVER", ev_o, oo, po)
            else:
                cand = (label, "UNDER", ev_u, uo, pu)
            if best is None or cand[2] > best[2]:
                best = cand
            print(f"{label:>7} {po:>7.3f} {pu:>7.3f} | {oo:>+7.0f} {ev_o:>+8.1%} "
                  f"| {uo:>+7.0f} {ev_u:>+8.1%}")
        blabel, bs, bev, bodds, bprob = best
        imp = american_to_implied_prob(bodds)
        ratio = bprob / imp if imp else float("inf")
        flag = ""
        if bprob < 0.25:
            flag += "  [BELOW 0.25 FLOOR]"
        if ratio >= 2.0:
            flag += "  [>2x MARKET CAP]"
        print(f"  -> BEST: {bs} {blabel} @ {bodds:+.0f}  EV {bev:+.1%}  "
              f"(model {bprob:.3f} vs imp {imp:.3f}, {ratio:.2f}x){flag}")
    conn.close()


if __name__ == "__main__":
    main()
