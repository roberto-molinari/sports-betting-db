import sqlite3

conn = sqlite3.connect("sports_betting.db")
c = conn.cursor()

print("=== SCHEMA: what data exists per match ===")
c.execute("PRAGMA table_info(matches)")
for row in c.fetchall():
    print(f"  {row[1]:20s} {row[2]}")

print("\n=== TEAMS table (for name lookup) ===")
c.execute("PRAGMA table_info(teams)")
for row in c.fetchall():
    print(f"  {row[1]:20s} {row[2]}")

print("\n=== SAMPLE SERIE A MATCH (with team names) ===")
c.execute("""
    SELECT m.match_id, m.match_date, m.season, m.league,
           ht.name as home_team, at.name as away_team,
           m.home_score, m.away_score, m.match_status
    FROM matches m
    JOIN teams ht ON m.home_team_id = ht.team_id
    JOIN teams at ON m.away_team_id = at.team_id
    WHERE m.league = 'Serie A'
    ORDER BY m.match_date DESC
    LIMIT 3
""")
rows = c.fetchall()
if rows:
    cols = [d[0] for d in c.description]
    print("  " + " | ".join(cols))
    for row in rows:
        print("  " + " | ".join(str(x) for x in row))

print("\n=== COVERAGE ===")
c.execute("SELECT COUNT(*) FROM matches WHERE league='Serie A'")
print(f"  Total Serie A games:  {c.fetchone()[0]}")

c.execute("SELECT MIN(match_date), MAX(match_date) FROM matches WHERE league='Serie A'")
mn, mx = c.fetchone()
print(f"  Date range:           {mn}  to  {mx}")

c.execute("SELECT season, COUNT(*) FROM matches WHERE league='Serie A' GROUP BY season ORDER BY season")
for row in c.fetchall():
    print(f"  Season {row[0]}:           {row[1]} games")

print("\n=== SCORE COMPLETENESS ===")
c.execute("SELECT COUNT(*) FROM matches WHERE league='Serie A' AND home_score IS NULL")
print(f"  Missing home_score:   {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM matches WHERE league='Serie A' AND away_score IS NULL")
print(f"  Missing away_score:   {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM matches WHERE league='Serie A' AND home_score=0 AND away_score=0")
print(f"  0-0 scores:           {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM matches WHERE league='Serie A' AND (home_score < 0 OR away_score < 0)")
print(f"  Negative scores:      {c.fetchone()[0]}")

print("\n=== SCORE DISTRIBUTION (sanity check) ===")
c.execute("""
    SELECT home_score + away_score as total, COUNT(*) as games
    FROM matches WHERE league='Serie A' AND home_score IS NOT NULL
    GROUP BY total ORDER BY total
""")
for row in c.fetchall():
    print(f"  Total goals={row[0]:2d}: {row[1]} games")

print("\n=== TEAM COVERAGE ===")
c.execute("""
    SELECT COUNT(DISTINCT team_id) FROM (
        SELECT home_team_id as team_id FROM matches WHERE league='Serie A'
        UNION
        SELECT away_team_id FROM matches WHERE league='Serie A'
    )
""")
print(f"  Distinct teams:       {c.fetchone()[0]}  (Serie A has 20)")

print("\n=== ALL TEAMS IN SERIE A DATA ===")
c.execute("""
    SELECT DISTINCT t.name FROM teams t
    WHERE t.team_id IN (
        SELECT home_team_id FROM matches WHERE league='Serie A'
        UNION
        SELECT away_team_id FROM matches WHERE league='Serie A'
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
        FROM matches WHERE league='Serie A'
        GROUP BY home_team_id, away_team_id, DATE(match_date)
        HAVING c > 1
    )
""")
print(f"  Duplicate game combos: {c.fetchone()[0]}")

print("\n=== MATCH STATUS BREAKDOWN ===")
c.execute("SELECT match_status, COUNT(*) FROM matches WHERE league='Serie A' GROUP BY match_status")
for row in c.fetchall():
    print(f"  {row[0]:20s}: {row[1]}")

print("\n=== GAMES PER MONTH (gap detection) ===")
c.execute("""
    SELECT strftime('%Y-%m', match_date) as month, COUNT(*) as games
    FROM matches WHERE league='Serie A'
    GROUP BY month ORDER BY month
""")
for row in c.fetchall():
    print(f"  {row[0]}: {row[1]:4d} games")

conn.close()
