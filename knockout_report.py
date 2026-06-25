"""
Knockout analysis — what the captured ET/PK data enables (FEATURE-002 reqs 13-15).

Three views, each consumable for later-round pricing:
  1. Model calibration by path : did ADVANCE picks win near their modeled probability,
     sliced by how the tie was decided (regulation / extra time / shootout)?
  2. Team ET/PK trends         : per-team shootout W/L and extra-time goals for/against.
  3. Player penalty conversion : per-player shootout goals / attempts.

Usage:
    python knockout_report.py
"""

import sqlite3
from collections import defaultdict

from core.sports_db import DATABASE_PATH
from core.grading import advancing_side


def advance_calibration(conn):
    """Return (overall, by_path) where each is {n, mean_model_prob, win_rate}.

    Uses the latest stored ADVANCE pick per knockout match that has been graded.
    """
    rows = conn.execute("""
        SELECT m.decided_by, p.model_prob, p.result
        FROM soccer_wc_picks p
        JOIN soccer_wc_matches m ON m.match_id = p.match_id
        WHERE p.side LIKE '%ADVANCE%' AND p.result IS NOT NULL
          AND p.pick_id = (SELECT MAX(pick_id) FROM soccer_wc_picks
                           WHERE match_id = m.match_id AND side LIKE '%ADVANCE%')
    """).fetchall()

    def summarise(items):
        n = len(items)
        if not n:
            return {"n": 0, "mean_model_prob": None, "win_rate": None}
        return {
            "n": n,
            "mean_model_prob": sum(mp for mp, _ in items) / n,
            "win_rate": sum(1 for _, res in items if res == "win") / n,
        }

    by_path = defaultdict(list)
    allitems = []
    for decided_by, model_prob, result in rows:
        by_path[decided_by or "?"].append((model_prob, result))
        allitems.append((model_prob, result))
    return summarise(allitems), {k: summarise(v) for k, v in by_path.items()}


def team_shootout_records(conn):
    """Return {team_id: [wins, losses]} over shootout-decided ties."""
    rows = conn.execute("""
        SELECT home_team_id, away_team_id, home_score, away_score,
               extra_time_home_score, extra_time_away_score,
               shootout_home_score, shootout_away_score
        FROM soccer_wc_matches WHERE decided_by = 'shootout'
    """).fetchall()
    records = defaultdict(lambda: [0, 0])   # team_id -> [wins, losses]
    for home, away, rh, ra, eth, eta, sh, sa in rows:
        winner = advancing_side(rh, ra, eth, eta, sh, sa)
        if winner == "HOME":
            records[home][0] += 1
            records[away][1] += 1
        elif winner == "AWAY":
            records[away][0] += 1
            records[home][1] += 1
    return dict(records)


def team_et_goals(conn):
    """Return {team_id: [goals_for, goals_against]} in extra time."""
    goals = defaultdict(lambda: [0, 0])
    rows = conn.execute("""
        SELECT g.team_id, m.home_team_id, m.away_team_id
        FROM soccer_extra_time_goals g
        JOIN soccer_wc_matches m ON m.match_id = g.match_id
    """).fetchall()
    for scorer, home, away in rows:
        opponent = away if scorer == home else home
        goals[scorer][0] += 1
        goals[opponent][1] += 1
    return dict(goals)


def player_penalty_conversion(conn):
    """Return list of (player_name, goals, attempts) ordered by attempts then goals."""
    return conn.execute("""
        SELECT player_name,
               SUM(CASE WHEN result = 'goal' THEN 1 ELSE 0 END) AS goals,
               COUNT(*) AS attempts
        FROM soccer_penalty_kicks
        WHERE player_name IS NOT NULL
        GROUP BY player_name
        ORDER BY attempts DESC, goals DESC
    """).fetchall()


def _fmt_cal(label, s):
    if not s["n"]:
        return f"  {label:<12} n=0"
    return (f"  {label:<12} n={s['n']:<3} model {s['mean_model_prob']:.3f} "
            f"vs actual {s['win_rate']:.3f}")


def main():
    conn = sqlite3.connect(DATABASE_PATH)
    team_names = {tid: name for tid, name in
                  conn.execute("SELECT team_id, name FROM soccer_wc_teams")}

    overall, by_path = advance_calibration(conn)
    print("=== ADVANCE-PICK CALIBRATION BY PATH ===")
    if not overall["n"]:
        print("  (no graded ADVANCE picks yet)")
    else:
        print(_fmt_cal("overall", overall))
        for path in ("regulation", "extra_time", "shootout"):
            if path in by_path:
                print(_fmt_cal(path, by_path[path]))

    print("\n=== TEAM SHOOTOUT RECORDS ===")
    so = team_shootout_records(conn)
    if not so:
        print("  (no shootouts yet)")
    for tid, (w, ll) in sorted(so.items(), key=lambda kv: -kv[1][0]):
        print(f"  {team_names.get(tid, tid):<22} {w}-{ll}")

    print("\n=== TEAM EXTRA-TIME GOALS (for-against) ===")
    et = team_et_goals(conn)
    if not et:
        print("  (no extra-time goals yet)")
    for tid, (gf, ga) in sorted(et.items(), key=lambda kv: -(kv[1][0] - kv[1][1])):
        print(f"  {team_names.get(tid, tid):<22} {gf}-{ga}")

    print("\n=== PLAYER PENALTY CONVERSION ===")
    pens = player_penalty_conversion(conn)
    if not pens:
        print("  (no shootout kicks yet)")
    for name, goals, attempts in pens:
        print(f"  {name:<22} {goals}/{attempts}")

    conn.close()


if __name__ == "__main__":
    main()
