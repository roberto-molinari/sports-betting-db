"""
Score the model's expected-goals PROXY against ACTUAL goals scored, per team.

The model has no true xG; its per-match projected goals ("proxy") is the same
opponent- and venue-adjusted rate analyse_match_wc builds:

    proj_goals(T, match) = T_attack * (opponent_defense / WC_BASELINE) * host_boost

summed over every finished match team T played. That total is compared to T's
actual regulation (90') goals (soccer_wc_matches.home_score/away_score — the model's
90' basis; extra-time / shootout goals are excluded), AND to T's own official FIFA xG
total (soccer_wc_external_xg, source='fifa_official' -- FEATURE-008) where on file, so
the proxy is checked against both the final result and the underlying process. Sorted
by smallest |gap| (vs actual), so the best-calibrated attacks come first — a
diagnostic for where the goals-based strength model retrodicts reality and where it
does not (BUG-005 / BUG-001 / pins).

Usage:
    python proxy_goals_calibration.py
    python proxy_goals_calibration.py --team Mexico   # highlight + rank + summary
"""

import argparse
import sqlite3
import sys

from core.sports_db import DATABASE_PATH, get_latest_wc_strength
from core.poisson_model import WC_BASELINE
from core.wc_host_advantage import host_advantage
from core.wc_knockout_scale import knockout_goal_scale

HIGHLIGHT = "\033[1;32m"   # bold green
RESET = "\033[0m"
# Must match SOURCE in import_wc_fifa_xg.py. Not imported from there directly
# to avoid pulling this lightweight calibration script's import chain through
# that script's pdfplumber/requests (web-scraping) dependencies.
FIFA_XG_SOURCE = "fifa_official"


def parse_args():
    ap = argparse.ArgumentParser(
        description="Score the model's expected-goals proxy against actual goals scored.")
    ap.add_argument("--team", help="Highlight this team's row and print its rank/summary.")
    return ap.parse_args()


def ordinal(n):
    if 11 <= n % 100 <= 13:
        return f"{n}th"
    suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def latest_method(conn, team_id):
    row = conn.execute(
        "SELECT method FROM soccer_wc_team_strength WHERE team_id = ? "
        "ORDER BY computed_at DESC LIMIT 1", (team_id,)).fetchone()
    return row[0] if row else None


def build_rows(conn):
    teams = {tid: (name, fifa) for tid, name, fifa in conn.execute(
        "SELECT team_id, name, fifa_ranking FROM soccer_wc_teams")}
    strength = {tid: get_latest_wc_strength(tid, conn=conn) for tid in teams}

    matches = conn.execute(
        "SELECT match_id, home_team_id, away_team_id, home_score, away_score, stage "
        "FROM soccer_wc_matches WHERE home_score IS NOT NULL").fetchall()
    fifa_xg = {mid: (h_xg, a_xg) for mid, h_xg, a_xg in conn.execute(
        "SELECT match_id, home_xg, away_xg FROM soccer_wc_external_xg WHERE source = ?",
        (FIFA_XG_SOURCE,))}

    agg = {tid: {"proxy": 0.0, "actual": 0, "gp": 0, "fifa_xg": 0.0, "gp_xg": 0}
           for tid in teams}
    for mid, h, a, hs, as_, stage in matches:
        if strength.get(h) is None or strength.get(a) is None:
            continue
        h_att, h_def = strength[h]
        a_att, a_def = strength[a]
        h_names, a_names = teams[h][0], teams[a][0]
        level = knockout_goal_scale(stage)
        h_boost = host_advantage(h_names, stage) * level
        a_boost = host_advantage(a_names, stage) * level
        # home team's proxy: its attack vs the away defense, its own venue boost
        agg[h]["proxy"] += max(h_att * (a_def / WC_BASELINE) * h_boost, 0.1)
        agg[a]["proxy"] += max(a_att * (h_def / WC_BASELINE) * a_boost, 0.1)
        agg[h]["actual"] += hs
        agg[a]["actual"] += as_
        agg[h]["gp"] += 1
        agg[a]["gp"] += 1
        xg = fifa_xg.get(mid)
        if xg is not None:
            h_xg, a_xg = xg
            agg[h]["fifa_xg"] += h_xg      # each team's OWN official xG (its attacking output)
            agg[a]["fifa_xg"] += a_xg
            agg[h]["gp_xg"] += 1
            agg[a]["gp_xg"] += 1

    rows = []
    for tid, (name, fifa) in teams.items():
        if agg[tid]["gp"] == 0 or strength.get(tid) is None:
            continue
        att, dfn = strength[tid]
        proxy, actual = agg[tid]["proxy"], agg[tid]["actual"]
        gp_xg, fifa_xg_total = agg[tid]["gp_xg"], agg[tid]["fifa_xg"]
        rows.append({
            "team": name, "att": att, "def": dfn,
            "proxy": proxy, "actual": actual, "gap": proxy - actual,
            "gp": agg[tid]["gp"],
            "fifa_xg": fifa_xg_total if gp_xg else None, "gp_xg": gp_xg,
            "xg_gap": (proxy - fifa_xg_total) if gp_xg else None,
            "method": latest_method(conn, tid), "fifa": fifa,
        })
    rows.sort(key=lambda r: abs(r["gap"]))
    return rows


METHOD_LABEL = {"fifa_ranking": "FIFA-pin", "player_aggregation": "blend(0.2)"}


def main():
    args = parse_args()
    conn = sqlite3.connect(DATABASE_PATH)
    rows = build_rows(conn)
    conn.close()

    color = sys.stdout.isatty()
    target_idx = None
    if args.team:
        for i, r in enumerate(rows):
            if r["team"].lower() == args.team.lower():
                target_idx = i
                break
        if target_idx is None:
            print(f"WARNING: no calibration row for team {args.team!r} "
                  f"(not enough finished matches, or unknown team name).\n")

    hdr = (f"{'TEAM':<22}{'ATT λ':>7}{'DEF λ':>7}{'PROXY xG':>10}{'ACTUAL':>8}{'GAP':>8}"
           f"{'FIFA xG':>9}{'vs FIFA':>9}{'GP':>4}  {'METHOD':<11}{'RANK':>5}")
    print(hdr)
    print("-" * len(hdr))
    for i, r in enumerate(rows):
        method = METHOD_LABEL.get(r["method"], r["method"] or "?")
        fifa_xg_col = f"{r['fifa_xg']:>9.2f}" if r["fifa_xg"] is not None else f"{'-':>9}"
        xg_gap_col = f"{r['xg_gap']:>+9.2f}" if r["xg_gap"] is not None else f"{'-':>9}"
        line = (f"{r['team']:<22}{r['att']:>7.2f}{r['def']:>7.2f}{r['proxy']:>10.2f}"
                f"{r['actual']:>8d}{r['gap']:>+8.2f}{fifa_xg_col}{xg_gap_col}{r['gp']:>4d}  "
                f"{method:<11}{r['fifa'] if r['fifa'] else '-':>5}")
        if i == target_idx and color:
            line = f"{HIGHLIGHT}{line}{RESET}"
        print(line)

    n = len(rows)
    mae = sum(abs(r["gap"]) for r in rows) / n
    bias = sum(r["gap"] for r in rows) / n
    print("-" * len(hdr))
    print(f"{n} teams · mean |gap| {mae:.2f} · mean signed gap {bias:+.2f} "
          f"(+ = model over-projects goals)")
    xg_rows = [r for r in rows if r["xg_gap"] is not None]
    if xg_rows:
        xg_mae = sum(abs(r["xg_gap"]) for r in xg_rows) / len(xg_rows)
        xg_bias = sum(r["xg_gap"] for r in xg_rows) / len(xg_rows)
        print(f"{len(xg_rows)} teams w/ official FIFA xG on file · mean |gap vs FIFA xG| "
              f"{xg_mae:.2f} · mean signed {xg_bias:+.2f} (+ = model over-projects vs. the "
              f"official process, independent of how the games actually finished)")

    if target_idx is not None:
        r = rows[target_idx]
        method = METHOD_LABEL.get(r["method"], r["method"] or "?")
        fifa_xg_str = f"{r['fifa_xg']:.2f}" if r["fifa_xg"] is not None else "n/a"
        xg_gap_str = f"{r['xg_gap']:+.2f}" if r["xg_gap"] is not None else "n/a"
        print()
        print(f"=== {r['team']} ===")
        print(f"Position: {ordinal(target_idx + 1)} out of {n} (ranked by |gap|, best-calibrated first)")
        print(f"  ATT λ {r['att']:.2f}  DEF λ {r['def']:.2f}  PROXY xG {r['proxy']:.2f}  "
              f"ACTUAL {r['actual']}  GAP {r['gap']:+.2f}  GP {r['gp']}  "
              f"METHOD {method}  FIFA rank #{r['fifa'] if r['fifa'] else '-'}")
        print(f"  FIFA xG {fifa_xg_str} ({r['gp_xg']} games on file)  vs FIFA xG {xg_gap_str}")


if __name__ == "__main__":
    main()
