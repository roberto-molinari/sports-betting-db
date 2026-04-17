import sqlite3
from sports_db import DATABASE_PATH

conn = sqlite3.connect(DATABASE_PATH)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

query = '''
    SELECT
        nm.match_id,
        h.name  as home_team,
        a.name  as away_team,
        nm.home_score,
        nm.away_score,
        bo.home_moneyline,
        bo.away_moneyline,
        nm.match_date
    FROM nhl_matches nm
    JOIN nhl_teams h  ON nm.home_team_id = h.team_id
    JOIN nhl_teams a  ON nm.away_team_id = a.team_id
    JOIN nhl_betting_odds bo ON nm.match_id = bo.match_id
    WHERE nm.match_status = 'completed'
    ORDER BY nm.match_date ASC
'''

cursor.execute(query)
matches = cursor.fetchall()
conn.close()


def calculate_payout(bet_amount, moneyline):
    if moneyline < 0:
        return bet_amount * (100 / abs(moneyline))
    else:
        return bet_amount * (moneyline / 100)


total_wagered = 0
total_profit = 0
wins = 0
losses = 0
win_details = []

for match in matches:
    home_ml = match['home_moneyline']
    away_ml = match['away_moneyline']
    home_score = match['home_score']
    away_score = match['away_score']

    if home_ml < away_ml:
        favorite_ml = home_ml
        favorite_team = match['home_team']
        favorite_won = home_score > away_score
    else:
        favorite_ml = away_ml
        favorite_team = match['away_team']
        favorite_won = away_score > home_score

    total_wagered += 100

    if favorite_won:
        payout = calculate_payout(100, favorite_ml)
        profit = payout - 100
        total_profit += profit
        wins += 1
        outcome = "WIN"
    else:
        profit = -100
        total_profit += profit
        losses += 1
        outcome = "LOSS"

    win_details.append({
        'date': match['match_date'],
        'matchup': f"{match['home_team']} vs {match['away_team']}",
        'favorite': f"{favorite_team} ({favorite_ml})",
        'score': f"{home_score}-{away_score}",
        'outcome': outcome,
        'profit': profit
    })

print("=" * 70)
print("NHL MONEYLINE FAVORITE BETTING ANALYSIS")
print("=" * 70)
print(f"\nBet Amount Per Game: $100")
print(f"Total Games Played: {len(matches)}")
print(f"Total Amount Wagered: ${total_wagered:,.2f}")
print(f"\nWins: {wins} ({100*wins/len(matches):.1f}%)")
print(f"Losses: {losses} ({100*losses/len(matches):.1f}%)")
print(f"\nTotal Profit/Loss: ${total_profit:,.2f}")
print(f"ROI: {100*total_profit/total_wagered:.2f}%")

if total_profit > 0:
    print(f"\nYou would have MADE ${total_profit:,.2f}")
else:
    print(f"\nYou would have LOST ${abs(total_profit):,.2f}")

print("\n" + "=" * 70)
print("RECENT GAMES (Last 10):")
print("=" * 70)
for detail in win_details[-10:]:
    status = "+" if detail['outcome'] == "WIN" else "-"
    profit_str = f"+${detail['profit']:.2f}" if detail['profit'] > 0 else f"-${abs(detail['profit']):.2f}"
    print(f"{status} {detail['date'][:10]} | {detail['matchup']:40} | {detail['outcome']:4} | {profit_str:>10}")
