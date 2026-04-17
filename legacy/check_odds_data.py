import sqlite3
from sports_db import DATABASE_PATH

conn = sqlite3.connect(DATABASE_PATH)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

# Check what columns exist and have data
query = '''
    SELECT 
        COUNT(*) as total_records,
        COUNT(spread_home_odds) as spread_home_odds_count,
        COUNT(spread_away_odds) as spread_away_odds_count,
        COUNT(over_odds) as over_odds_count,
        COUNT(under_odds) as under_odds_count
    FROM betting_odds
'''

cursor.execute(query)
result = cursor.fetchone()

print("Betting Odds Data Population:")
print(f"Total records: {result['total_records']}")
print(f"Spread home odds: {result['spread_home_odds_count']} records ({100*result['spread_home_odds_count']/result['total_records']:.1f}%)")
print(f"Spread away odds: {result['spread_away_odds_count']} records ({100*result['spread_away_odds_count']/result['total_records']:.1f}%)")
print(f"Over odds: {result['over_odds_count']} records ({100*result['over_odds_count']/result['total_records']:.1f}%)")
print(f"Under odds: {result['under_odds_count']} records ({100*result['under_odds_count']/result['total_records']:.1f}%)")

# Show a sample record
print("\nSample record:")
query2 = '''
    SELECT 
        m.match_id,
        h.name as home_team,
        a.name as away_team,
        bo.spread_home,
        bo.spread_home_odds,
        bo.spread_away,
        bo.spread_away_odds,
        bo.over_under,
        bo.over_odds,
        bo.under_odds
    FROM betting_odds bo
    JOIN matches m ON bo.match_id = m.match_id
    JOIN teams h ON m.home_team_id = h.team_id
    JOIN teams a ON m.away_team_id = a.team_id
    LIMIT 1
'''

cursor.execute(query2)
sample = cursor.fetchone()
if sample:
    print(f"Match: {sample['home_team']} vs {sample['away_team']}")
    print(f"  Home spread: {sample['spread_home']} @ {sample['spread_home_odds']}")
    print(f"  Away spread: {sample['spread_away']} @ {sample['spread_away_odds']}")
    print(f"  Over/Under: {sample['over_under']} (O: {sample['over_odds']}, U: {sample['under_odds']})")
else:
    print("No records found")

conn.close()
