#!/usr/bin/env python3
"""View current database contents."""

import sqlite3
from sports_db import DATABASE_PATH


def print_section(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}\n")


def main():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # ── Summary ───────────────────────────────────────────────────────────────
    print_section("DATABASE SUMMARY")

    for table, label in [
        ('soccer_teams',       'Soccer teams'),
        ('nhl_teams',          'NHL teams'),
        ('soccer_matches',     'Soccer matches'),
        ('nhl_matches',        'NHL matches'),
        ('soccer_betting_odds','Soccer betting odds'),
        ('nhl_betting_odds',   'NHL betting odds'),
    ]:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"  {label:<25}: {cur.fetchone()[0]}")

    # ── Soccer teams ──────────────────────────────────────────────────────────
    print_section("SOCCER TEAMS")
    cur.execute("SELECT team_id, name, league FROM soccer_teams ORDER BY league, name")
    for row in cur.fetchall():
        print(f"  {row['team_id']:3d} | {row['name']:30s} | {row['league']}")

    # ── NHL teams ─────────────────────────────────────────────────────────────
    print_section("NHL TEAMS")
    cur.execute("SELECT team_id, name FROM nhl_teams ORDER BY name")
    for row in cur.fetchall():
        print(f"  {row['team_id']:3d} | {row['name']}")

    # ── Recent soccer matches ─────────────────────────────────────────────────
    print_section("SOCCER MATCHES (most recent 20)")
    cur.execute("""
        SELECT sm.match_id, h.name AS home, a.name AS away,
               sm.home_score, sm.away_score,
               sm.halftime_home_score, sm.halftime_away_score,
               sm.match_date, sm.match_status, sm.league
        FROM soccer_matches sm
        JOIN soccer_teams h ON sm.home_team_id = h.team_id
        JOIN soccer_teams a ON sm.away_team_id = a.team_id
        ORDER BY sm.match_date DESC
        LIMIT 20
    """)
    for m in cur.fetchall():
        score = f"{m['home_score']}-{m['away_score']}" if m['home_score'] is not None else "TBD"
        ht = f"HT:{m['halftime_home_score']}-{m['halftime_away_score']}" if m['halftime_home_score'] is not None else ""
        date = (m['match_date'] or '')[:10]
        print(f"  {date} | {m['home']:25s} vs {m['away']:25s} | {score:5s} {ht:10s} | {m['match_status']}")

    # ── Recent NHL matches ────────────────────────────────────────────────────
    print_section("NHL MATCHES (most recent 20)")
    cur.execute("""
        SELECT nm.match_id, h.name AS home, a.name AS away,
               nm.home_score, nm.away_score, nm.match_date, nm.match_status
        FROM nhl_matches nm
        JOIN nhl_teams h ON nm.home_team_id = h.team_id
        JOIN nhl_teams a ON nm.away_team_id = a.team_id
        ORDER BY nm.match_date DESC
        LIMIT 20
    """)
    for m in cur.fetchall():
        score = f"{m['home_score']}-{m['away_score']}" if m['home_score'] is not None else "TBD"
        date = (m['match_date'] or '')[:10]
        print(f"  {date} | {m['home']:25s} vs {m['away']:25s} | {score:5s} | {m['match_status']}")

    # ── Soccer betting odds (sample) ──────────────────────────────────────────
    print_section("SOCCER BETTING ODDS (sample 5)")
    cur.execute("""
        SELECT bo.odds_id, h.name AS home, a.name AS away,
               bo.sportsbook, bo.home_moneyline, bo.away_moneyline, bo.over_under
        FROM soccer_betting_odds bo
        JOIN soccer_matches sm ON bo.match_id = sm.match_id
        JOIN soccer_teams h   ON sm.home_team_id = h.team_id
        JOIN soccer_teams a   ON sm.away_team_id = a.team_id
        LIMIT 5
    """)
    for row in cur.fetchall():
        print(f"  {row['sportsbook']:15s} | {row['home']:25s} vs {row['away']:25s}")
        print(f"    ML: {row['home_moneyline']:6.0f} / {row['away_moneyline']:6.0f} | O/U: {row['over_under']}")

    # ── NHL betting odds (sample) ─────────────────────────────────────────────
    print_section("NHL BETTING ODDS (sample 5)")
    cur.execute("""
        SELECT bo.odds_id, h.name AS home, a.name AS away,
               bo.sportsbook, bo.home_moneyline, bo.away_moneyline,
               bo.spread_home, bo.over_under
        FROM nhl_betting_odds bo
        JOIN nhl_matches nm ON bo.match_id = nm.match_id
        JOIN nhl_teams h    ON nm.home_team_id = h.team_id
        JOIN nhl_teams a    ON nm.away_team_id = a.team_id
        LIMIT 5
    """)
    for row in cur.fetchall():
        print(f"  {row['sportsbook']:15s} | {row['home']:25s} vs {row['away']:25s}")
        print(f"    ML: {row['home_moneyline']:6.0f} / {row['away_moneyline']:6.0f} | Spread: {row['spread_home']:5.1f} | O/U: {row['over_under']}")

    conn.close()


if __name__ == "__main__":
    main()
