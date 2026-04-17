"""
quickstart.py — Overview of the sports-betting-db database.

Demonstrates how to query the sport-specific tables introduced in the
schema migration. Run with:

    python quickstart.py
"""

import sqlite3
from sports_db import DATABASE_PATH


def print_header(title: str):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print('=' * 60)


def summary(conn: sqlite3.Connection):
    print_header("Database summary")
    cur = conn.cursor()
    for table in [
        'soccer_teams', 'nhl_teams',
        'soccer_matches', 'nhl_matches',
        'soccer_betting_odds', 'nhl_betting_odds',
    ]:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"  {table:<25} {cur.fetchone()[0]:>6} rows")


def recent_soccer_matches(conn: sqlite3.Connection, n: int = 5):
    print_header(f"Recent {n} completed Serie A matches (with halftime)")
    cur = conn.cursor()
    cur.execute("""
        SELECT m.match_date,
               ht.name  AS home,
               at.name  AS away,
               m.home_score, m.away_score,
               m.halftime_home_score, m.halftime_away_score
        FROM   soccer_matches m
        JOIN   soccer_teams ht ON ht.team_id = m.home_team_id
        JOIN   soccer_teams at ON at.team_id = m.away_team_id
        WHERE  m.match_status = 'completed'
        ORDER  BY m.match_date DESC
        LIMIT  ?
    """, (n,))
    for date, home, away, hs, as_, hhs, has_ in cur.fetchall():
        ht_str = f"  (HT {hhs}-{has_})" if hhs is not None else ""
        print(f"  {date[:10]}  {home:<25} {hs}-{as_}  {away}{ht_str}")


def recent_nhl_matches(conn: sqlite3.Connection, n: int = 5):
    print_header(f"Recent {n} completed NHL matches")
    cur = conn.cursor()
    cur.execute("""
        SELECT m.match_date,
               ht.name  AS home,
               at.name  AS away,
               m.home_score, m.away_score
        FROM   nhl_matches m
        JOIN   nhl_teams ht ON ht.team_id = m.home_team_id
        JOIN   nhl_teams at ON at.team_id = m.away_team_id
        WHERE  m.match_status = 'completed'
        ORDER  BY m.match_date DESC
        LIMIT  ?
    """, (n,))
    for date, home, away, hs, as_ in cur.fetchall():
        print(f"  {date[:10]}  {home:<30} {hs}-{as_}  {away}")


def sample_nhl_odds(conn: sqlite3.Connection, n: int = 5):
    print_header(f"Sample NHL betting odds ({n} rows)")
    cur = conn.cursor()
    cur.execute("""
        SELECT m.match_date,
               ht.name  AS home,
               at.name  AS away,
               o.sportsbook,
               o.home_moneyline, o.away_moneyline,
               o.over_under
        FROM   nhl_betting_odds o
        JOIN   nhl_matches m  ON m.match_id  = o.match_id
        JOIN   nhl_teams ht   ON ht.team_id  = m.home_team_id
        JOIN   nhl_teams at   ON at.team_id  = m.away_team_id
        ORDER  BY m.match_date DESC
        LIMIT  ?
    """, (n,))
    for date, home, away, book, hml, aml, ou in cur.fetchall():
        print(f"  {date[:10]}  {home:<26} vs {away:<26}  "
              f"{book:<12} ML {int(hml):+d}/{int(aml):+d}  O/U {ou}")


def halftime_coverage(conn: sqlite3.Connection):
    print_header("Halftime score coverage (soccer_matches)")
    cur = conn.cursor()
    cur.execute("""
        SELECT season,
               COUNT(*) AS total,
               SUM(CASE WHEN halftime_home_score IS NOT NULL THEN 1 ELSE 0 END) AS with_ht
        FROM   soccer_matches
        WHERE  match_status = 'completed'
        GROUP  BY season
        ORDER  BY season
    """)
    for season, total, with_ht in cur.fetchall():
        pct = 100 * with_ht // total if total else 0
        print(f"  Season {season}: {with_ht}/{total} ({pct}%)")


def main():
    conn = sqlite3.connect(DATABASE_PATH)
    try:
        summary(conn)
        recent_soccer_matches(conn)
        recent_nhl_matches(conn)
        sample_nhl_odds(conn)
        halftime_coverage(conn)
    finally:
        conn.close()
    print()


if __name__ == "__main__":
    main()
