"""
Track the real-world impact of the FIFA-rank blend (BUG-005, v7 strengths).

For every v7-era match (from FIRST_BLEND_DATE on), re-derive the pick the model
WOULD have made on the pre-blend v6 strengths and compare it to the v7 pick the
blend actually produces. The interesting rows are the ones where the blend
*changed* the pick: only there can the blend help or hurt. For graded matches it
settles both the v6 and v7 sides and reports the unit delta the blend caused.

This is the data that tells us whether FIFA_BLEND_WEIGHT=0.2 is earning its keep:
trim it, leave it, or raise it — judged on results, not one card.

Usage:
    python blend_impact.py                 # all graded v7-era matches
    python blend_impact.py --date 2026-06-19
"""

import argparse
import sqlite3

from core.sports_db import DATABASE_PATH
from core.poisson_model import (analyse_match_wc, american_to_implied_prob,
                                american_to_decimal)
from generate_wc_card import select_pick, HOST_NATIONS, HOST_HOME_ADVANTAGE, EASTERN_SQL_OFFSET
from update_wc_results import grade_pick

# v7 (FIFA blend) was persisted 2026-06-19; earlier slates ran on v6, so the blend
# could not have changed them. Counterfactuals before this date are meaningless.
FIRST_BLEND_DATE = "2026-06-19"
MARKET_BOOK = "Bovada"   # the book the card prices against (Consensus rows are reference lines)


def strength(conn, team_id, blended):
    """Latest lambdas for a team. blended=True -> v7 (newest row); False -> the
    newest NON-v7 row (the v6 the blend replaced)."""
    if blended:
        sql = ("SELECT lambda_attack, lambda_defense FROM soccer_wc_team_strength "
               "WHERE team_id = ? ORDER BY strength_id DESC LIMIT 1")
    else:
        sql = ("SELECT lambda_attack, lambda_defense FROM soccer_wc_team_strength "
               "WHERE team_id = ? AND (notes IS NULL OR notes NOT LIKE 'v7:%') "
               "ORDER BY strength_id DESC LIMIT 1")
    row = conn.execute(sql, (team_id,)).fetchone()
    return (row[0], row[1]) if row else None


def pick_on(match, h_att, h_def, a_att, a_def):
    """The model's best pick for a match given a specific pair of strengths —
    mirrors generate_wc_card.best_pick_for_match's candidate build + select_pick."""
    home_adv = HOST_HOME_ADVANTAGE if match["home"] in HOST_NATIONS else 1.0
    away_adv = HOST_HOME_ADVANTAGE if match["away"] in HOST_NATIONS else 1.0
    r = analyse_match_wc(
        lambda_home_attack=h_att, lambda_away_attack=a_att,
        lambda_home_defense=h_def, lambda_away_defense=a_def,
        home_moneyline=match["home_moneyline"], draw_moneyline=match["draw_moneyline"],
        away_moneyline=match["away_moneyline"], ou_line=match["over_under"],
        over_odds=match["over_odds"], under_odds=match["under_odds"],
        home_advantage=home_adv, away_advantage=away_adv)
    line = match["over_under"]
    lbl = f"{line:g}" if line is not None else ""
    cands = [
        ("HOME", match["home_moneyline"], r.get("p_home"), r.get("ev_home")),
        ("DRAW", match["draw_moneyline"], r.get("p_draw"), r.get("ev_draw")),
        ("AWAY", match["away_moneyline"], r.get("p_away"), r.get("ev_away")),
        (f"OVER {lbl}", match["over_odds"], r.get("p_over"), r.get("ev_over")),
        (f"UNDER {lbl}", match["under_odds"], r.get("p_under"), r.get("ev_under")),
    ]
    priced = [
        {"side": s, "odds": o, "prob": p, "ev": ev, "implied": american_to_implied_prob(o)}
        for s, o, p, ev in cands if ev is not None and o is not None and p is not None
    ]
    return select_pick(priced) if priced else None


def settle(side, odds, hs, as_):
    """Units won/lost on a 1u flat stake, or None if the match isn't graded."""
    if hs is None or as_ is None:
        return None
    res = grade_pick(side, hs, as_)
    return american_to_decimal(odds) - 1 if res == "win" else (-1.0 if res == "loss" else 0.0)


def fetch(conn, date_filter):
    cur = conn.cursor()
    sql = """
        SELECT m.match_id, m.match_date, h.name AS home, a.name AS away,
               m.home_team_id, m.away_team_id, m.home_score, m.away_score,
               o.home_moneyline, o.draw_moneyline, o.away_moneyline,
               o.over_under, o.over_odds, o.under_odds
        FROM soccer_wc_matches m
        JOIN soccer_wc_teams h ON h.team_id = m.home_team_id
        JOIN soccer_wc_teams a ON a.team_id = m.away_team_id
        JOIN soccer_wc_odds  o ON o.match_id = m.match_id AND o.sportsbook = ?
        WHERE date(m.match_date, ?) >= date(?)
    """
    params = [MARKET_BOOK, EASTERN_SQL_OFFSET, FIRST_BLEND_DATE]
    if date_filter:
        sql += " AND date(m.match_date, ?) = date(?)"
        params += [EASTERN_SQL_OFFSET, date_filter]
    sql += " ORDER BY m.match_date"
    return cur.execute(sql, params).fetchall()


def main():
    ap = argparse.ArgumentParser(description="Track FIFA-blend pick changes vs the v6 counterfactual.")
    ap.add_argument("--date", help="Limit to one matchday (YYYY-MM-DD).")
    args = ap.parse_args()

    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    matches = fetch(conn, args.date)

    print(f"{'MATCH':<26}{'v6 PICK':<13}{'v7 PICK':<13}{'SCORE':<7}{'v6':>6}{'v7':>6}{'Δ':>7}")
    graded = pending = 0
    v6_tot = v7_tot = 0.0
    for m in matches:
        v6s_h = strength(conn, m["home_team_id"], False)
        v6s_a = strength(conn, m["away_team_id"], False)
        v7s_h = strength(conn, m["home_team_id"], True)
        v7s_a = strength(conn, m["away_team_id"], True)
        if not all((v6s_h, v6s_a, v7s_h, v7s_a)):
            continue
        p6 = pick_on(m, v6s_h[0], v6s_h[1], v6s_a[0], v6s_a[1])
        p7 = pick_on(m, v7s_h[0], v7s_h[1], v7s_a[0], v7s_a[1])
        if not p6 or not p7 or p6["side"] == p7["side"]:
            continue   # blend didn't change the pick -> nothing to attribute
        u6 = settle(p6["side"], p6["odds"], m["home_score"], m["away_score"])
        if u6 is None:        # not yet graded -> it's an upcoming change, count only
            pending += 1
            continue
        u7 = settle(p7["side"], p7["odds"], m["home_score"], m["away_score"])
        graded += 1
        v6_tot += u6
        v7_tot += u7
        name = f"{m['home']} v {m['away']}"
        print(f"{name[:25]:<26}{p6['side']:<13}{p7['side']:<13}"
              f"{m['home_score']}-{m['away_score']:<5}{u6:>6.2f}{u7:>6.2f}{u7 - u6:>+7.2f}")

    print("-" * 78)
    print(f"Blend changed {graded + pending} pick(s): {graded} graded, {pending} upcoming.")
    if graded:
        print(f"v6 counterfactual: {v6_tot:+.2f}u | v7 actual: {v7_tot:+.2f}u | "
              f"blend delta: {v7_tot - v6_tot:+.2f}u")
    conn.close()


if __name__ == "__main__":
    main()
