#!/usr/bin/env python3
"""Check current odds in database"""
import sqlite3
from sports_db import DATABASE_PATH

conn = sqlite3.connect(DATABASE_PATH)
cursor = conn.cursor()

cursor.execute('SELECT COUNT(*) FROM betting_odds')
total = cursor.fetchone()[0]
print(f'Total betting_odds records: {total}')

cursor.execute('''
    SELECT COUNT(DISTINCT bo.match_id) 
    FROM betting_odds bo 
    JOIN matches m ON bo.match_id = m.match_id 
    WHERE m.league = "Serie A"
''')
serie_a_with_odds = cursor.fetchone()[0]
print(f'Serie A matches with odds: {serie_a_with_odds}')

cursor.execute('SELECT sportsbook, COUNT(*) FROM betting_odds GROUP BY sportsbook')
print('\nOdds by sportsbook:')
for row in cursor.fetchall():
    print(f'  {row[0]}: {row[1]} records')

# Check if import worked
cursor.execute('''
    SELECT m.match_id, h.name, a.name, bo.home_moneyline
    FROM betting_odds bo
    JOIN matches m ON bo.match_id = m.match_id
    JOIN teams h ON m.home_team_id = h.team_id
    JOIN teams a ON m.away_team_id = a.team_id
    WHERE m.league = "Serie A"
    LIMIT 5
''')
print('\nSample Serie A matches with odds:')
for row in cursor.fetchall():
    print(f'  Match {row[0]}: {row[1]} vs {row[2]} (ML: {row[3]})')

conn.close()
