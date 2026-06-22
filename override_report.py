"""
Head-to-head: the model's picks vs the user's tagged deviations.

On every game where the user overrode the model (soccer_wc_pick_overrides), this
settles BOTH sides and reports whether going with instinct beat the model — overall
and broken down by reason category. The category breakdown is the point: a category
that consistently beats the model (e.g. "form") is the evidence to build that signal
INTO the model, so the edge becomes systematic and no human is needed in the loop.

Usage:
    python override_report.py
"""

import sqlite3
from collections import defaultdict

from core.sports_db import DATABASE_PATH
from core.poisson_model import american_to_decimal


def units(odds, result):
    if result == "win":
        return american_to_decimal(odds) - 1
    if result == "loss":
        return -1.0
    return 0.0   # push


def load(conn):
    return conn.execute("""
        SELECT o.override_id, h.name || ' v ' || a.name AS match,
               o.user_side, o.user_odds, o.result AS user_res, o.category, o.reason,
               p.side AS model_side, p.odds AS model_odds, p.result AS model_res
        FROM soccer_wc_pick_overrides o
        JOIN soccer_wc_matches m ON o.match_id = m.match_id
        JOIN soccer_wc_teams h ON m.home_team_id = h.team_id
        JOIN soccer_wc_teams a ON m.away_team_id = a.team_id
        LEFT JOIN soccer_wc_picks p ON p.pick_id =
            (SELECT MAX(pick_id) FROM soccer_wc_picks WHERE match_id = m.match_id)
        ORDER BY o.created_at""").fetchall()


def main():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    rows = load(conn)
    conn.close()
    if not rows:
        print("No overrides recorded yet. Log one with record_override.py.")
        return

    graded = [r for r in rows if r["user_res"] and r["model_res"]]
    pending = [r for r in rows if not (r["user_res"] and r["model_res"])]

    print("=== MODEL vs YOUR DEVIATIONS (graded) ===")
    m_tot = u_tot = 0.0
    cat = defaultdict(lambda: [0, 0.0, 0.0])   # n, model_u, user_u
    if graded:
        print(f"{'match':<22}{'model':<13}{'you':<13}{'cat':<9}{'mΔu':>7}{'uΔu':>7}{'edge':>7}")
        for r in graded:
            mu = units(r["model_odds"], r["model_res"])
            uu = units(r["user_odds"], r["user_res"])
            m_tot += mu
            u_tot += uu
            c = r["category"] or "—"
            cat[c][0] += 1
            cat[c][1] += mu
            cat[c][2] += uu
            mlab = f"{r['model_side']}·{r['model_res'][0].upper()}"
            ulab = f"{r['user_side']}·{r['user_res'][0].upper()}"
            print(f"{r['match'][:21]:<22}{mlab:<13}{ulab:<13}{c[:8]:<9}"
                  f"{mu:>+7.2f}{uu:>+7.2f}{uu - mu:>+7.2f}")
        print("-" * 78)
        print(f"{'TOTAL ('+str(len(graded))+' games)':<57}{m_tot:>+7.2f}{u_tot:>+7.2f}{u_tot - m_tot:>+7.2f}")
        verdict = ("your reads ADD value" if u_tot > m_tot
                   else "your reads COST value" if u_tot < m_tot else "even")
        print(f"\nNet edge of deviating: {u_tot - m_tot:+.2f}u  ->  {verdict}")

        print("\n=== by reason category (where to look for a systematic signal) ===")
        print(f"{'category':<12}{'n':>3}{'model u':>9}{'your u':>9}{'edge':>8}")
        for c, (n, mu, uu) in sorted(cat.items(), key=lambda kv: kv[1][2] - kv[1][1], reverse=True):
            print(f"{c:<12}{n:>3}{mu:>+9.2f}{uu:>+9.2f}{uu - mu:>+8.2f}")
    else:
        print("(none graded yet)")

    if pending:
        print(f"\n=== pending ({len(pending)}) ===")
        for r in pending:
            print(f"  {r['match']}: model {r['model_side']} | you {r['user_side']} "
                  f"[{r['category'] or '—'}] — {r['reason']}")

    print("\n=== reasons (the raw material for model improvements) ===")
    for r in graded:
        res = "✓" if r["user_res"] == "win" else ("✗" if r["user_res"] == "loss" else "P")
        beat = "beat" if units(r["user_odds"], r["user_res"]) > units(r["model_odds"], r["model_res"]) else "lost to"
        print(f"  [{r['category'] or '—'}] {r['match']}: {res} ({beat} model) — {r['reason']}")


if __name__ == "__main__":
    main()
