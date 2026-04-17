import sqlite3

conn = sqlite3.connect("sports_betting.db")
c = conn.cursor()

print("=== SCHEMA: soccer_matches ===")
c.execute("PRAGMA table_info(soccer_matches)")
for row in c.fetchall():
    print(f"  {row[1]:25s} {row[2]}")

print("\n=== SCHEMA: soccer_teams ===")
c.execute("PRAGMA table_info(soccer_teams)")
for row in c.fetchall():
    print(f"  {row[1]:25s} {row[2]}")

print("\n=== SAMPLE SERIE A MATCH (with team names) ===")
c.execute("""
    SELECT sm.match_id, sm.match_date, sm.season, sm.league,
           ht.name as home_team, at.name as away_team,
           sm.home_score, sm.away_score,
           sm.halftime_home_score, sm.halftime_away_score,
           sm.match_status
    FROM soccer_matches sm
    JOIN soccer_teams ht ON sm.home_team_id = ht.team_id
    JOIN soccer_teams at ON sm.away_team_id = at.team_id
    ORDER BY sm.match_date DESC
    LIMIT 3
""")
rows = c.fetchall()
if rows:
    cols = [d[0] for d in c.description]
    print("  " + " | ".join(cols))
    for row in rows:
        print("  " + " | ".join(str(x) for x in row))

print("\n=== COVERAGE ===")
c.execute("SELECT COUNT(*) FROM soccer_matches")
print(f"  Total Serie A games:  {c.fetchone()[0]}")

c.execute("SELECT MIN(match_date), MAX(match_date) FROM soccer_matches")
mn, mx = c.fetchone()
print(f"  Date range:           {mn}  to  {mx}")

c.execute("SELECT season, COUNT(*) FROM soccer_matches GROUP BY season ORDER BY season")
for row in c.fetchall():
    print(f"  Season {row[0]}:           {row[1]} games")

print("\n=== SCORE COMPLETENESS ===")
c.execute("SELECT COUNT(*) FROM soccer_matches WHERE home_score IS NULL")
print(f"  Missing home_score:          {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM soccer_matches WHERE away_score IS NULL")
print(f"  Missing away_score:          {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM soccer_matches WHERE home_score=0 AND away_score=0")
print(f"  0-0 scores:                  {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM soccer_matches WHERE halftime_home_score IS NOT NULL")
print(f"  With halftime scores:        {c.fetchone()[0]}")

print("\n=== SCORE DISTRIBUTION (sanity check) ===")
c.execute("""
    SELECT home_score + away_score as total, COUNT(*) as games
    FROM soccer_matches WHERE home_score IS NOT NULL
    GROUP BY total ORDER BY total
""")
for row in c.fetchall():
    print(f"  Total goals={row[0]:2d}: {row[1]} games")

print("\n=== TEAM COVERAGE ===")
c.execute("""
    SELECT COUNT(DISTINCT team_id) FROM (
        SELECT home_team_id as team_id FROM soccer_matches
        UNION
        SELECT away_team_id FROM soccer_matches
    )
""")
print(f"  Distinct teams:       {c.fetchone()[0]}  (Serie A has 20)")

print("\n=== ALL TEAMS IN SERIE A DATA ===")
c.execute("""
    SELECT DISTINCT t.name FROM soccer_teams t
    WHERE t.team_id IN (
        SELECT home_team_id FROM soccer_matches
        UNION
        SELECT away_team_id FROM soccer_matches
    )
    ORDER BY t.name
""")
teams = [r[0] for r in c.fetchall()]
for i in range(0, len(teams), 3):
    print("  " + "  |  ".join(teams[i:i+3]))

print("\n=== DUPLICATE CHECK ===")
c.execute("""
    SELECT COUNT(*) FROM (
        SELECT home_team_id, away_team_id, DATE(match_date), COUNT(*) as c
        FROM soccer_matches
        GROUP BY home_team_id, away_team_id, DATE(match_date)
        HAVING c > 1
    )
""")
print(f"  Duplicate game combos: {c.fetchone()[0]}")

conn.close()
