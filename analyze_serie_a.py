"""
Serie A Match Analysis
======================
Analyses Serie A match data across all available seasons.

Usage:
    python analyze_serie_a.py
    python analyze_serie_a.py --season 2024   # single season

Sections:
    1. Overview
    2. Home / Away / Draw rates
    3. Goals
    4. Halftime → Fulltime result correlation
    5. Both Teams To Score (BTTS)
    6. Per-team: attack, defence, home/away form
"""

import sqlite3
import argparse
from pathlib import Path

DB_PATH = Path(__file__).parent / "sports_betting.db"

SEP  = "=" * 70
SEP2 = "-" * 70


def pct(n, d):
    return f"{100 * n / d:.1f}%" if d else "n/a"


def section(title):
    print(f"\n{SEP}\n  {title}\n{SEP2}")


def run(season_filter=None):
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    # Base WHERE clause
    where = "m.league = 'Serie A' AND m.match_status = 'completed' AND m.home_score IS NOT NULL"
    if season_filter:
        where += f" AND m.season = {season_filter}"

    def q(sql, params=()):
        cur.execute(sql, params)
        return cur.fetchall()

    # ── 1. Overview ────────────────────────────────────────────────────────────
    section("1. OVERVIEW")

    rows = q(f"""
        SELECT m.season,
               COUNT(*) as matches,
               ROUND(AVG(m.home_score + m.away_score), 2) as avg_goals
        FROM soccer_matches m
        WHERE {where}
        GROUP BY m.season ORDER BY m.season
    """)

    print(f"  {'Season':<10} {'Matches':>8}  {'Avg goals/game':>14}")
    print(f"  {'-'*10} {'-'*8}  {'-'*14}")
    total_matches = 0
    for season, matches, avg in rows:
        label = f"{season}-{str(season+1)[-2:]}"
        print(f"  {label:<10} {matches:>8}  {avg:>14.2f}")
        total_matches += matches
    print(f"\n  Total matches analysed: {total_matches}")

    # ── 2. Result distribution ─────────────────────────────────────────────────
    section("2. HOME / AWAY / DRAW RATES")

    rows = q(f"""
        SELECT
            SUM(CASE WHEN home_score > away_score THEN 1 ELSE 0 END)  as home_wins,
            SUM(CASE WHEN home_score = away_score THEN 1 ELSE 0 END)  as draws,
            SUM(CASE WHEN home_score < away_score THEN 1 ELSE 0 END)  as away_wins,
            COUNT(*) as total
        FROM soccer_matches m
        WHERE {where}
    """)
    hw, d, aw, tot = rows[0]
    print(f"  Home wins : {hw:>5}  ({pct(hw, tot)})")
    print(f"  Draws     : {d:>5}  ({pct(d, tot)})")
    print(f"  Away wins : {aw:>5}  ({pct(aw, tot)})")

    if not season_filter:
        print(f"\n  By season:")
        rows = q(f"""
            SELECT season,
                SUM(CASE WHEN home_score > away_score THEN 1 ELSE 0 END),
                SUM(CASE WHEN home_score = away_score THEN 1 ELSE 0 END),
                SUM(CASE WHEN home_score < away_score THEN 1 ELSE 0 END),
                COUNT(*)
            FROM soccer_matches m
            WHERE {where}
            GROUP BY season ORDER BY season
        """)
        print(f"  {'Season':<10} {'H-win':>6} {'Draw':>6} {'A-win':>6}")
        print(f"  {'-'*10} {'-'*6} {'-'*6} {'-'*6}")
        for season, hw, d, aw, tot in rows:
            label = f"{season}-{str(season+1)[-2:]}"
            print(f"  {label:<10} {pct(hw,tot):>6} {pct(d,tot):>6} {pct(aw,tot):>6}")

    # ── 3. Goals ───────────────────────────────────────────────────────────────
    section("3. GOALS")

    rows = q(f"""
        SELECT
            ROUND(AVG(home_score + away_score), 3)             as avg_total,
            ROUND(AVG(home_score), 3)                          as avg_home,
            ROUND(AVG(away_score), 3)                          as avg_away,
            ROUND(AVG(halftime_home_score + halftime_away_score), 3) as avg_ht,
            SUM(CASE WHEN home_score + away_score > 2.5 THEN 1 ELSE 0 END) as over25,
            SUM(CASE WHEN home_score + away_score > 1.5 THEN 1 ELSE 0 END) as over15,
            SUM(CASE WHEN home_score + away_score > 3.5 THEN 1 ELSE 0 END) as over35,
            COUNT(*) as total
        FROM soccer_matches m
        WHERE {where}
    """)
    avg_total, avg_home, avg_away, avg_ht, over25, over15, over35, tot = rows[0]

    print(f"  Avg goals/game          : {avg_total:.3f}")
    print(f"    Home team avg         : {avg_home:.3f}")
    print(f"    Away team avg         : {avg_away:.3f}")
    print(f"  Avg HT goals/game       : {avg_ht:.3f}  (FT avg {avg_total:.3f} → {100*avg_ht/avg_total:.1f}% scored by HT)" if avg_ht and avg_total else "")
    print(f"\n  Over 1.5 goals          : {pct(over15, tot)}")
    print(f"  Over 2.5 goals          : {pct(over25, tot)}")
    print(f"  Over 3.5 goals          : {pct(over35, tot)}")

    print(f"\n  Score frequency (top 10):")
    rows = q(f"""
        SELECT home_score || '-' || away_score as scoreline, COUNT(*) as n
        FROM soccer_matches m
        WHERE {where}
        GROUP BY scoreline ORDER BY n DESC LIMIT 10
    """)
    for scoreline, n in rows:
        print(f"    {scoreline:<6} {n:>5}  ({pct(n, tot)})")

    # ── 4. HT → FT correlation ─────────────────────────────────────────────────
    section("4. HALFTIME → FULLTIME RESULT CORRELATION")

    print("  (only matches where halftime score is available)\n")
    rows = q(f"""
        SELECT
            CASE
                WHEN halftime_home_score > halftime_away_score THEN 'H'
                WHEN halftime_home_score = halftime_away_score THEN 'D'
                ELSE 'A'
            END as ht_result,
            CASE
                WHEN home_score > away_score THEN 'H'
                WHEN home_score = away_score THEN 'D'
                ELSE 'A'
            END as ft_result,
            COUNT(*) as n
        FROM soccer_matches m
        WHERE {where} AND halftime_home_score IS NOT NULL
        GROUP BY ht_result, ft_result
        ORDER BY ht_result, ft_result
    """)

    # Pivot the data
    from collections import defaultdict
    pivot = defaultdict(dict)
    ht_totals = defaultdict(int)
    for ht, ft, n in rows:
        pivot[ht][ft] = n
        ht_totals[ht] += n

    labels = {'H': 'Home leading', 'D': 'Level', 'A': 'Away leading'}
    print(f"  {'HT result':<16}  {'→ H win':>8}  {'→ Draw':>8}  {'→ A win':>8}  {'Total':>7}")
    print(f"  {'-'*16}  {'-'*8}  {'-'*8}  {'-'*8}  {'-'*7}")
    for ht in ['H', 'D', 'A']:
        tot_ht = ht_totals[ht]
        hw = pivot[ht].get('H', 0)
        dw = pivot[ht].get('D', 0)
        aw = pivot[ht].get('A', 0)
        print(f"  {labels[ht]:<16}  {pct(hw,tot_ht):>8}  {pct(dw,tot_ht):>8}  {pct(aw,tot_ht):>8}  {tot_ht:>7}")

    # ── 5. BTTS ────────────────────────────────────────────────────────────────
    section("5. BOTH TEAMS TO SCORE (BTTS)")

    rows = q(f"""
        SELECT
            SUM(CASE WHEN home_score > 0 AND away_score > 0 THEN 1 ELSE 0 END) as btts,
            SUM(CASE WHEN home_score = 0 OR  away_score = 0 THEN 1 ELSE 0 END) as no_btts,
            SUM(CASE WHEN home_score = 0 THEN 1 ELSE 0 END) as home_blank,
            SUM(CASE WHEN away_score = 0 THEN 1 ELSE 0 END) as away_blank,
            COUNT(*) as total
        FROM soccer_matches m
        WHERE {where}
    """)
    btts, no_btts, home_blank, away_blank, tot = rows[0]
    print(f"  BTTS (yes)              : {btts:>5}  ({pct(btts, tot)})")
    print(f"  BTTS (no)               : {no_btts:>5}  ({pct(no_btts, tot)})")
    print(f"    Home team blank       : {home_blank:>5}  ({pct(home_blank, tot)})")
    print(f"    Away team blank       : {away_blank:>5}  ({pct(away_blank, tot)})")

    # ── 6. Per-team ────────────────────────────────────────────────────────────
    section("6. PER-TEAM STATS  (minimum 20 home or away games)")

    rows = q(f"""
        SELECT
            t.name,
            -- home
            COUNT(CASE WHEN m.home_team_id = t.team_id THEN 1 END)                       as h_played,
            SUM(CASE WHEN m.home_team_id = t.team_id THEN m.home_score ELSE 0 END)       as h_gf,
            SUM(CASE WHEN m.home_team_id = t.team_id THEN m.away_score ELSE 0 END)       as h_ga,
            SUM(CASE WHEN m.home_team_id = t.team_id AND m.home_score > m.away_score
                     THEN 1 ELSE 0 END)                                                   as h_wins,
            -- away
            COUNT(CASE WHEN m.away_team_id = t.team_id THEN 1 END)                       as a_played,
            SUM(CASE WHEN m.away_team_id = t.team_id THEN m.away_score ELSE 0 END)       as a_gf,
            SUM(CASE WHEN m.away_team_id = t.team_id THEN m.home_score ELSE 0 END)       as a_ga,
            SUM(CASE WHEN m.away_team_id = t.team_id AND m.away_score > m.home_score
                     THEN 1 ELSE 0 END)                                                   as a_wins
        FROM soccer_teams t
        JOIN soccer_matches m
          ON (m.home_team_id = t.team_id OR m.away_team_id = t.team_id)
        WHERE {where.replace('m.', 'm.')}
        GROUP BY t.team_id
        HAVING h_played + a_played >= 20
        ORDER BY (h_gf + a_gf) * 1.0 / (h_played + a_played) DESC
    """)

    print(f"\n  {'Team':<22} {'Pld':>4} {'GF':>4} {'GA':>4} {'GD':>4}  {'H-win%':>7}  {'A-win%':>7}")
    print(f"  {'-'*22} {'-'*4} {'-'*4} {'-'*4} {'-'*4}  {'-'*7}  {'-'*7}")
    for (name, hp, hgf, hga, hw, ap, agf, aga, aw) in rows:
        pld = hp + ap
        gf  = hgf + agf
        ga  = hga + aga
        gd  = gf - ga
        print(f"  {name:<22} {pld:>4} {gf:>4} {ga:>4} {gd:>+4}  {pct(hw, hp) if hp else 'n/a':>7}  {pct(aw, ap) if ap else 'n/a':>7}")

    print()
    conn.close()


def main():
    parser = argparse.ArgumentParser(description='Serie A match analysis')
    parser.add_argument('--season', type=int, default=None,
                        help='Filter to a single season start year (e.g. 2024)')
    args = parser.parse_args()
    run(season_filter=args.season)


if __name__ == '__main__':
    main()
