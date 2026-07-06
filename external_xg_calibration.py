"""
FEATURE-008 comparison view: model-projected total vs. external (TheStatsAPI) xG vs.
actual result, per match -- the "good pick, bad variance" vs "genuine mispricing"
diagnostic external xG was built for (see BUGS.md FEATURE-008).

Three numbers per match:
  - proj    : model's projected total, lambda_H + lambda_A (current team strength,
              current host-advantage + BUG-004 knockout scale -- same basis as
              totals_calibration.py, so the two are directly comparable).
  - xg      : external xG total (home_xg + away_xg from soccer_wc_external_xg) --
              a process-based read of the match, immune to finishing variance.
  - actual  : goals actually scored.

Two gaps answer different questions:
  - model vs xG     -> is the MODEL's read of the underlying game wrong (a real
                       calibration issue), independent of how the match finished?
  - xG vs actual    -> did the RESULT diverge from the run of play (variance /
                       finishing), regardless of whether the model's read was any good?

Usage:
    python external_xg_calibration.py                 # all matches with external xG on file
    python external_xg_calibration.py --stage R16
"""

import argparse
import sqlite3

from core.sports_db import DATABASE_PATH, get_latest_wc_strength
from core.poisson_model import analyse_match_wc
from core.wc_host_advantage import host_advantage
from core.wc_knockout_scale import knockout_goal_scale


def parse_args():
    ap = argparse.ArgumentParser(
        description="Compare model projection vs external xG vs actual result.")
    ap.add_argument("--stage", help="Restrict to one stage (e.g. Group, R32, R16).")
    return ap.parse_args()


def build_rows(conn, stage_filter=None):
    matches = conn.execute("""
        SELECT m.match_id, m.match_date, m.stage, m.home_score, m.away_score,
               h.name AS home, a.name AS away, m.home_team_id, m.away_team_id,
               x.home_xg, x.away_xg
        FROM soccer_wc_matches m
        JOIN soccer_wc_teams h ON h.team_id = m.home_team_id
        JOIN soccer_wc_teams a ON a.team_id = m.away_team_id
        JOIN soccer_wc_external_xg x ON x.match_id = m.match_id
        WHERE m.home_score IS NOT NULL
        ORDER BY m.match_date
    """).fetchall()

    rows = []
    for mid, date, stage, hs, as_, home, away, hid, aid, home_xg, away_xg in matches:
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
        r = analyse_match_wc(h_att, a_att, h_def, a_def,
                             home_advantage=home_adv, away_advantage=away_adv)
        proj = r["lambda_H"] + r["lambda_A"]
        xg_total = (home_xg or 0) + (away_xg or 0)
        actual = hs + as_
        rows.append({
            "match_id": mid, "date": str(date)[:10], "stage": stage,
            "home": home, "away": away,
            "proj": proj, "xg": xg_total, "actual": actual,
            "model_vs_xg": proj - xg_total,
            "xg_vs_actual": xg_total - actual,
            "model_vs_actual": proj - actual,
        })
    return rows


def summarize(rows, key):
    n = len(rows)
    vals = [r[key] for r in rows]
    signed = sum(vals) / n
    mae = sum(abs(v) for v in vals) / n
    return n, signed, mae


def main():
    args = parse_args()
    conn = sqlite3.connect(DATABASE_PATH)
    rows = build_rows(conn, args.stage)
    conn.close()

    if not rows:
        print("No matches with external xG on file for that filter (run import_wc_match_xg.py).")
        return

    hdr = (f"{'DATE':<11}{'STAGE':<7}{'MATCH':<28}{'PROJ':>7}{'XG':>7}{'ACTUAL':>7}"
           f"{'MvXG':>8}{'XGvA':>8}")
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        game = f"{r['home']} v {r['away']}"
        print(f"{r['date']:<11}{r['stage']:<7}{game:<28}{r['proj']:>7.2f}{r['xg']:>7.2f}"
              f"{r['actual']:>7d}{r['model_vs_xg']:>+8.2f}{r['xg_vs_actual']:>+8.2f}")

    print("-" * len(hdr))
    n, mvx_signed, mvx_mae = summarize(rows, "model_vs_xg")
    _, xva_signed, xva_mae = summarize(rows, "xg_vs_actual")
    _, mva_signed, mva_mae = summarize(rows, "model_vs_actual")
    print(f"{n} matches with external xG on file")
    print(f"  model vs xG      : mean signed {mvx_signed:+.2f}  mean |gap| {mvx_mae:.2f}  "
          f"(+ = model over-projects vs. the run of play)")
    print(f"  xG vs actual     : mean signed {xva_signed:+.2f}  mean |gap| {xva_mae:.2f}  "
          f"(+ = the game underperformed its own process -- variance/finishing)")
    print(f"  model vs actual  : mean signed {mva_signed:+.2f}  mean |gap| {mva_mae:.2f}  "
          f"(the totals_calibration.py / BUG-004 number, for reference)")

    stages = sorted({r["stage"] for r in rows}, key=lambda s: [r["stage"] for r in rows].index(s))
    if len(stages) > 1:
        print("\nBy stage:")
        for s in stages:
            srows = [r for r in rows if r["stage"] == s]
            sn, smvx, smvx_mae = summarize(srows, "model_vs_xg")
            _, sxva, sxva_mae = summarize(srows, "xg_vs_actual")
            print(f"  {s:<7} {sn:>3} matches · model-vs-xG {smvx:+.2f} (|{smvx_mae:.2f}|) · "
                  f"xG-vs-actual {sxva:+.2f} (|{sxva_mae:.2f}|)")


if __name__ == "__main__":
    main()
