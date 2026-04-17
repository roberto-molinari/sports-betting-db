import sqlite3

conn = sqlite3.connect("sports_betting.db")
c = conn.cursor()

print("=== SCHEMA: nhl_matches ===")
c.execute("PRAGMA table_info(nhl_matches)")
for row in c.fetchall():
    print(f"  {row[1]:20s} {row[2]}")

print("\n=== SCHEMA: nhl_teams ===")
c.execute("PRAGMA table_info(nhl_teams)")
for row in c.fetchall():
    print(f"  {row[1]:20s} {row[2]}")

print("\n=== SAMPLE NHL MATCH (with team names) ===")
c.execute("""
    SELECT nm.match_id, nm.match_date, nm.season,
           ht.name as home_team, at.name as away_team,
           nm.home_score, nm.away_score, nm.match_status
    FROM nhl_matches nm
    JOIN nhl_teams ht ON nm.home_team_id = ht.team_id
    JOIN nhl_teams at ON nm.away_team_id = at.team_id
    ORDER BY nm.match_date DESC
    LIMIT 3
""")
rows = c.fetchall()
if rows:
    cols = [d[0] for d in c.description]
    print("  " + " | ".join(cols))
    for row in rows:
        print("  " + " | ".join(str(x) for x in row))

print("\n=== COVERAGE ===")
c.execute("SELECT COUNT(*) FROM nhl_matches")
print(f"  Total NHL games:      {c.fetchone()[0]}")

c.execute("SELECT MIN(match_date), MAX(match_date) FROM nhl_matches")
mn, mx = c.fetchone()
print(f"  Date range:           {mn}  to  {mx}")

c.execute("SELECT season, COUNT(*) FROM nhl_matches GROUP BY season ORDER BY season")
for row in c.fetchall():
    print(f"  Season {row[0]}:           {row[1]} games")

print("\n=== SCORE COMPLETENESS ===")
c.execute("SELECT COUNT(*) FROM nhl_matches WHERE home_score IS NULL")
print(f"  Missing home_score:   {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM nhl_matches WHERE away_score IS NULL")
print(f"  Missing away_score:   {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM nhl_matches WHERE home_score=0 AND away_score=0")
print(f"  0-0 scores:           {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM nhl_matches WHERE (home_score > 10 OR away_score > 10)")
print(f"  Scores > 10 goals:    {c.fetchone()[0]}")

print("\n=== SCORE DISTRIBUTION (sanity check) ===")
c.execute("""
    SELECT home_score + away_score as total, COUNT(*) as games
    FROM nhl_matches WHERE home_score IS NOT NULL
    GROUP BY total ORDER BY total
""")
for row in c.fetchall():
    print(f"  Total goals={row[0]:2d}: {row[1]} games")

print("\n=== TEAM COVERAGE ===")
c.execute("""
    SELECT COUNT(DISTINCT team_id) FROM (
        SELECT home_team_id as team_id FROM nhl_matches
        UNION
        SELECT away_team_id FROM nhl_matches
    )
""")
print(f"  Distinct teams:       {c.fetchone()[0]}  (NHL has 32)")

print("\n=== ALL TEAMS IN NHL DATA ===")
c.execute("""
    SELECT DISTINCT t.name FROM nhl_teams t
    WHERE t.team_id IN (
        SELECT home_team_id FROM nhl_matches
        UNION
        SELECT away_team_id FROM nhl_matches
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
        FROM nhl_matches
        GROUP BY home_team_id, away_team_id, DATE(match_date)
        HAVING c > 1
    )
""")
print(f"  Duplicate game combos: {c.fetchone()[0]}")

print("\n=== MATCH STATUS BREAKDOWN ===")
c.execute("SELECT match_status, COUNT(*) FROM nhl_matches GROUP BY match_status")
for row in c.fetchall():
    print(f"  {row[0]:20s}: {row[1]}")

print("\n=== GAMES PER MONTH (gap detection) ===")
c.execute("""
    SELECT strftime('%Y-%m', match_date) as month, COUNT(*) as games
    FROM nhl_matches
    GROUP BY month ORDER BY month
""")
for row in c.fetchall():
    print(f"  {row[0]}: {row[1]:4d} games")

conn.close()
