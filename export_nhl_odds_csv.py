import sqlite3
import csv
from sports_db import DATABASE_PATH

conn = sqlite3.connect(DATABASE_PATH)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

query = '''
    SELECT
        nm.match_id,
        nm.season,
        h.name  as home_team,
        a.name  as away_team,
        nm.match_date,
        nm.match_status,
        nm.home_score,
        nm.away_score,
        bo.sportsbook,
        bo.odds_date,
        bo.home_moneyline,
        bo.away_moneyline,
        bo.spread_home,
        bo.spread_home_odds,
        bo.spread_away,
        bo.spread_away_odds,
        bo.over_under,
        bo.over_odds,
        bo.under_odds,
        bo.notes
    FROM nhl_matches nm
    JOIN nhl_teams h  ON nm.home_team_id = h.team_id
    JOIN nhl_teams a  ON nm.away_team_id = a.team_id
    JOIN nhl_betting_odds bo ON nm.match_id = bo.match_id
    ORDER BY nm.match_date ASC
'''

cursor.execute(query)
rows = cursor.fetchall()
conn.close()

if not rows:
    print("No NHL games with odds found in database")
    exit(1)

columns = rows[0].keys()
output_file = 'nhl_games_with_odds.csv'
with open(output_file, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=columns)
    writer.writeheader()
    for row in rows:
        writer.writerow(dict(row))

print(f"Created {output_file}")
print(f"Total records: {len(rows)}")
print(f"Columns: {', '.join(columns)}")
