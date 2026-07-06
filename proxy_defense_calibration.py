"""
Score the model's DEFENSE proxy against ACTUAL goals allowed, per team.

The mirror of proxy_goals_calibration.py. For each team T and each finished match,
T's projected goals-AGAINST is the opponent's projected goals — the same lambda
analyse_match_wc builds for the other side:

    ga_proxy(T, match) = opponent_attack * (T_defense / WC_BASELINE) * opponent_host_boost

summed over every finished match T played, compared to T's actual regulation (90')
goals allowed (the opponent's home_score/away_score). Sorted by smallest |gap|, so
the best-calibrated DEFENSES come first. This is where the Mexico pin (defense fix)
and BUG-001 (club-concede overstates national leakiness) should show up.

Usage:
    python proxy_defense_calibration.py
    python proxy_defense_calibration.py --team Mexico   # highlight + rank + summary
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


def parse_args():
    ap = argparse.ArgumentParser(
        description="Score the model's defense proxy against actual goals allowed.")
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
        "SELECT home_team_id, away_team_id, home_score, away_score, stage "
        "FROM soccer_wc_matches WHERE home_score IS NOT NULL").fetchall()

    agg = {tid: {"ga_proxy": 0.0, "allowed": 0, "gp": 0} for tid in teams}
    for h, a, hs, as_, stage in matches:
        if strength.get(h) is None or strength.get(a) is None:
            continue
        h_att, h_def = strength[h]
        a_att, a_def = strength[a]
        level = knockout_goal_scale(stage)
        h_host = host_advantage(teams[h][0], stage) * level
        a_host = host_advantage(teams[a][0], stage) * level
        # T's goals-against = opponent's projected goals (opponent attack vs T defense,
        # with the OPPONENT's venue boost). Mirrors analyse_match_wc's two lambdas.
        agg[h]["ga_proxy"] += max(a_att * (h_def / WC_BASELINE) * a_host, 0.1)  # H concedes A's goals
        agg[a]["ga_proxy"] += max(h_att * (a_def / WC_BASELINE) * h_host, 0.1)  # A concedes H's goals
        agg[h]["allowed"] += as_
        agg[a]["allowed"] += hs
        agg[h]["gp"] += 1
        agg[a]["gp"] += 1

    rows = []
    for tid, (name, fifa) in teams.items():
        if agg[tid]["gp"] == 0 or strength.get(tid) is None:
            continue
        att, dfn = strength[tid]
        ga_proxy, allowed = agg[tid]["ga_proxy"], agg[tid]["allowed"]
        rows.append({
            "team": name, "att": att, "def": dfn,
            "ga_proxy": ga_proxy, "allowed": allowed, "gap": ga_proxy - allowed,
            "gp": agg[tid]["gp"],
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

    hdr = (f"{'TEAM':<22}{'ATT λ':>7}{'DEF λ':>7}{'GA PROXY':>10}{'ALLOWED':>9}"
           f"{'GAP':>8}{'GP':>4}  {'METHOD':<11}{'FIFA':>5}")
    print(hdr)
    print("-" * len(hdr))
    for i, r in enumerate(rows):
        method = METHOD_LABEL.get(r["method"], r["method"] or "?")
        line = (f"{r['team']:<22}{r['att']:>7.2f}{r['def']:>7.2f}{r['ga_proxy']:>10.2f}"
                f"{r['allowed']:>9d}{r['gap']:>+8.2f}{r['gp']:>4d}  {method:<11}"
                f"{r['fifa'] if r['fifa'] else '-':>5}")
        if i == target_idx and color:
            line = f"{HIGHLIGHT}{line}{RESET}"
        print(line)

    n = len(rows)
    mae = sum(abs(r["gap"]) for r in rows) / n
    bias = sum(r["gap"] for r in rows) / n
    print("-" * len(hdr))
    print(f"{n} teams · mean |gap| {mae:.2f} · mean signed gap {bias:+.2f} "
          f"(+ = model over-projects goals-against, i.e. rates the defense leakier than reality)")

    if target_idx is not None:
        r = rows[target_idx]
        method = METHOD_LABEL.get(r["method"], r["method"] or "?")
        print()
        print(f"=== {r['team']} ===")
        print(f"Position: {ordinal(target_idx + 1)} out of {n} (ranked by |gap|, best-calibrated first)")
        print(f"  ATT λ {r['att']:.2f}  DEF λ {r['def']:.2f}  GA PROXY {r['ga_proxy']:.2f}  "
              f"ALLOWED {r['allowed']}  GAP {r['gap']:+.2f}  GP {r['gp']}  "
              f"METHOD {method}  FIFA #{r['fifa'] if r['fifa'] else '-'}")


if __name__ == "__main__":
    main()
