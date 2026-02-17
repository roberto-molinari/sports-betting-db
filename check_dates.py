import sqlite3
conn = sqlite3.connect('sports_betting.db')
cursor = conn.cursor()
cursor.execute('SELECT MIN(match_date), MAX(match_date) FROM matches WHERE league = "Serie A"')
result = cursor.fetchone()
print(f'Database Serie A matches: {result[0]} to {result[1]}')
