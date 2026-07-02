"""
Score the model's expected-goals PROXY against ACTUAL goals scored, per team.

The model has no true xG; its per-match projected goals ("proxy") is the same
opponent- and venue-adjusted rate analyse_match_wc builds:

    proj_goals(T, match) = T_attack * (opponent_defense / WC_BASELINE) * host_boost

summed over every finished match team T played. That total is compared to T's
actual regulation (90') goals (soccer_wc_matches.home_score/away_score — the model's
90' basis; extra-time / shootout goals are excluded). Sorted by smallest |gap|, so
the best-calibrated attacks come first — a diagnostic for where the goals-based
strength model retrodicts reality and where it does not (BUG-005 / BUG-001 / pins).

Usage:
    python proxy_goals_calibration.py
"""

import sqlite3

from core.sports_db import DATABASE_PATH, get_latest_wc_strength
from core.poisson_model import WC_BASELINE
from generate_wc_card import HOST_NATIONS, HOST_HOME_ADVANTAGE


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
        "SELECT home_team_id, away_team_id, home_score, away_score "
        "FROM soccer_wc_matches WHERE home_score IS NOT NULL").fetchall()

    agg = {tid: {"proxy": 0.0, "actual": 0, "gp": 0} for tid in teams}
    for h, a, hs, as_ in matches:
        if strength.get(h) is None or strength.get(a) is None:
            continue
        h_att, h_def = strength[h]
        a_att, a_def = strength[a]
        h_names, a_names = teams[h][0], teams[a][0]
        h_boost = HOST_HOME_ADVANTAGE if h_names in HOST_NATIONS else 1.0
        a_boost = HOST_HOME_ADVANTAGE if a_names in HOST_NATIONS else 1.0
        # home team's proxy: its attack vs the away defense, its own venue boost
        agg[h]["proxy"] += max(h_att * (a_def / WC_BASELINE) * h_boost, 0.1)
        agg[a]["proxy"] += max(a_att * (h_def / WC_BASELINE) * a_boost, 0.1)
        agg[h]["actual"] += hs
        agg[a]["actual"] += as_
        agg[h]["gp"] += 1
        agg[a]["gp"] += 1

    rows = []
    for tid, (name, fifa) in teams.items():
        if agg[tid]["gp"] == 0 or strength.get(tid) is None:
            continue
        att, dfn = strength[tid]
        proxy, actual = agg[tid]["proxy"], agg[tid]["actual"]
        rows.append({
            "team": name, "att": att, "def": dfn,
            "proxy": proxy, "actual": actual, "gap": proxy - actual,
            "gp": agg[tid]["gp"],
            "method": latest_method(conn, tid), "fifa": fifa,
        })
    rows.sort(key=lambda r: abs(r["gap"]))
    return rows


METHOD_LABEL = {"fifa_ranking": "FIFA-pin", "player_aggregation": "blend(0.2)"}


def main():
    conn = sqlite3.connect(DATABASE_PATH)
    rows = build_rows(conn)
    conn.close()

    hdr = (f"{'TEAM':<22}{'ATT λ':>7}{'DEF λ':>7}{'PROXY xG':>10}{'ACTUAL':>8}"
           f"{'GAP':>8}{'GP':>4}  {'METHOD':<11}{'FIFA':>5}")
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        method = METHOD_LABEL.get(r["method"], r["method"] or "?")
        print(f"{r['team']:<22}{r['att']:>7.2f}{r['def']:>7.2f}{r['proxy']:>10.2f}"
              f"{r['actual']:>8d}{r['gap']:>+8.2f}{r['gp']:>4d}  {method:<11}"
              f"{r['fifa'] if r['fifa'] else '-':>5}")

    n = len(rows)
    mae = sum(abs(r["gap"]) for r in rows) / n
    bias = sum(r["gap"] for r in rows) / n
    print("-" * len(hdr))
    print(f"{n} teams · mean |gap| {mae:.2f} · mean signed gap {bias:+.2f} "
          f"(+ = model over-projects goals)")


if __name__ == "__main__":
    main()
