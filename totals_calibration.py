"""
Score the model's projected MATCH TOTAL (lambda_H + lambda_A) against the actual
regulation (90') total goals, split by stage — a re-check of BUG-004 (the model's
over-skew / LEVEL bias on totals) now that real knockout results exist.

BUG-004 was originally diagnosed via EV skew (OVER avg EV +6.8% vs UNDER -14.4%
across the group stage) before results existed to check directly. This script
does the direct check: for every finished match, project the total goals using
CURRENT team strength (same lambdas analyse_match_wc prices from) and compare to
what actually happened, split by stage so a knockout-specific re-emergence is
visible even if the group-stage number looks contained by now.

Usage:
    python totals_calibration.py                  # all finished matches, by stage
    python totals_calibration.py --stage R32       # one stage only
"""

import argparse
import sqlite3

from core.sports_db import DATABASE_PATH, get_latest_wc_strength
from core.poisson_model import WC_BASELINE
from core.wc_host_advantage import host_advantage
from core.wc_knockout_scale import knockout_goal_scale


def parse_args():
    ap = argparse.ArgumentParser(
        description="Compare the model's projected match total to actual goals, by stage.")
    ap.add_argument("--stage", help="Restrict to one stage (e.g. Group, R32).")
    return ap.parse_args()


def build_rows(conn, stage_filter=None):
    matches = conn.execute(
        """SELECT m.match_id, m.match_date, m.stage, m.home_score, m.away_score,
                  h.name AS home, a.name AS away, m.home_team_id, m.away_team_id
           FROM soccer_wc_matches m
           JOIN soccer_wc_teams h ON h.team_id = m.home_team_id
           JOIN soccer_wc_teams a ON a.team_id = m.away_team_id
           WHERE m.home_score IS NOT NULL
           ORDER BY m.match_date""").fetchall()

    rows = []
    for mid, date, stage, hs, as_, home, away, hid, aid in matches:
        if stage_filter and stage != stage_filter:
            continue
        h_strength = get_latest_wc_strength(hid, conn=conn)
        a_strength = get_latest_wc_strength(aid, conn=conn)
        if h_strength is None or a_strength is None:
            continue
        h_att, h_def = h_strength
        a_att, a_def = a_strength
        level = knockout_goal_scale(stage)
        home_adv = host_advantage(home, stage) * level
        away_adv = host_advantage(away, stage) * level
        lam_h = max(h_att * (a_def / WC_BASELINE) * home_adv, 0.1)
        lam_a = max(a_att * (h_def / WC_BASELINE) * away_adv, 0.1)
        proj = lam_h + lam_a
        actual = hs + as_
        rows.append({
            "match_id": mid, "date": str(date)[:10], "stage": stage,
            "home": home, "away": away, "proj": proj, "actual": actual,
            "gap": proj - actual,
        })
    return rows


def summarize(rows):
    n = len(rows)
    bias = sum(r["gap"] for r in rows) / n
    mae = sum(abs(r["gap"]) for r in rows) / n
    over = sum(1 for r in rows if r["gap"] > 0.25)
    under = sum(1 for r in rows if r["gap"] < -0.25)
    flat = n - over - under
    return n, bias, mae, over, under, flat


def main():
    args = parse_args()
    conn = sqlite3.connect(DATABASE_PATH)
    rows = build_rows(conn, args.stage)
    conn.close()

    if not rows:
        print("No matches found for that filter.")
        return

    hdr = f"{'DATE':<11}{'STAGE':<7}{'MATCH':<28}{'PROJ':>7}{'ACTUAL':>8}{'GAP':>8}"
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        game = f"{r['home']} v {r['away']}"
        print(f"{r['date']:<11}{r['stage']:<7}{game:<28}{r['proj']:>7.2f}"
              f"{r['actual']:>8d}{r['gap']:>+8.2f}")

    print("-" * len(hdr))
    n, bias, mae, over, under, flat = summarize(rows)
    print(f"{n} matches · mean signed gap {bias:+.2f} (+ = model over-projects total goals) "
          f"· mean |gap| {mae:.2f}")
    print(f"  over-projected (gap > +0.25): {over}   under-projected (gap < -0.25): {under}   "
          f"roughly on target: {flat}")

    # per-stage breakdown so a knockout-specific shift is visible even if the
    # all-matches number looks contained.
    stages = sorted({r["stage"] for r in rows}, key=lambda s: [r["stage"] for r in rows].index(s))
    if len(stages) > 1:
        print()
        print("By stage:")
        for s in stages:
            srows = [r for r in rows if r["stage"] == s]
            sn, sbias, smae, sover, sunder, sflat = summarize(srows)
            print(f"  {s:<7} {sn:>3} matches · mean signed gap {sbias:+.2f} · mean |gap| {smae:.2f} "
                  f"· over {sover} / under {sunder} / flat {sflat}")


if __name__ == "__main__":
    main()
