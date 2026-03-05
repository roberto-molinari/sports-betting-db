"""
Betting Analysis Queries - Analyze hockey match outcomes and betting performance
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime

DATABASE_PATH = Path(__file__).parent / "sports_betting.db"


def run_analysis():
    """Run comprehensive betting analysis."""
    conn = sqlite3.connect(DATABASE_PATH)
    
    print("=" * 80)
    print("NHL BETTING ANALYSIS - 2024-2025 SEASONS")
    print("=" * 80)
    
    # 1. Basic dataset overview
    print("\n1️⃣  DATASET OVERVIEW")
    print("-" * 80)
    
    result = pd.read_sql('''
        SELECT 
            COUNT(*) as total_games,
            COUNT(DISTINCT DATE(m.match_date)) as unique_dates,
            MIN(m.match_date) as earliest_game,
            MAX(m.match_date) as latest_game
        FROM matches m
        WHERE m.sport = 'Hockey'
    ''', conn)
    
    print(f"Total games: {result['total_games'].iloc[0]:,}")
    print(f"Unique dates: {result['unique_dates'].iloc[0]}")
    print(f"Date range: {result['earliest_game'].iloc[0][:10]} to {result['latest_game'].iloc[0][:10]}")
    
    # 2. Betting odds coverage
    print("\n2️⃣  BETTING ODDS COVERAGE")
    print("-" * 80)
    
    result = pd.read_sql('''
        SELECT 
            COUNT(DISTINCT m.match_id) as games_with_odds,
            COUNT(DISTINCT m.match_id) * 100.0 / (SELECT COUNT(*) FROM matches WHERE sport = 'Hockey') as coverage_pct
        FROM matches m
        WHERE m.sport = 'Hockey'
        AND EXISTS (SELECT 1 FROM betting_odds bo WHERE bo.match_id = m.match_id)
    ''', conn)
    
    print(f"Games with odds: {result['games_with_odds'].iloc[0]:,}")
    print(f"Coverage: {result['coverage_pct'].iloc[0]:.1f}%")
    
    # 3. Home/Away performance
    print("\n3️⃣  HOME vs AWAY PERFORMANCE")
    print("-" * 80)
    
    result = pd.read_sql('''
        SELECT 
            'HOME' as location,
            COUNT(*) as games,
            SUM(CASE WHEN home_score > away_score THEN 1 ELSE 0 END) as wins,
            ROUND(SUM(CASE WHEN home_score > away_score THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as win_pct
        FROM matches
        WHERE sport = 'Hockey' AND home_score IS NOT NULL
        
        UNION ALL
        
        SELECT 
            'AWAY' as location,
            COUNT(*) as games,
            SUM(CASE WHEN away_score > home_score THEN 1 ELSE 0 END) as wins,
            ROUND(SUM(CASE WHEN away_score > home_score THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as win_pct
        FROM matches
        WHERE sport = 'Hockey' AND home_score IS NOT NULL
    ''', conn)
    
    print(result.to_string(index=False))
    
    # 4. Moneyline favorites vs underdogs
    print("\n4️⃣  MONEYLINE ANALYSIS - FAVORITES vs UNDERDOGS")
    print("-" * 80)
    
    result = pd.read_sql('''
        WITH odds_games AS (
            SELECT 
                m.match_id,
                m.home_score,
                m.away_score,
                bo.home_moneyline,
                bo.away_moneyline,
                CASE WHEN bo.home_moneyline < bo.away_moneyline THEN 'HOME' ELSE 'AWAY' END as favorite,
                CASE WHEN (bo.home_moneyline < bo.away_moneyline AND m.home_score > m.away_score)
                     OR (bo.home_moneyline >= bo.away_moneyline AND m.away_score > m.home_score)
                THEN 1 ELSE 0 END as favorite_won
            FROM matches m
            JOIN betting_odds bo ON m.match_id = bo.match_id
            WHERE m.sport = 'Hockey' AND m.home_score IS NOT NULL
        )
        SELECT 
            'FAVORITES' as bet_type,
            COUNT(*) as games,
            SUM(favorite_won) as wins,
            ROUND(SUM(favorite_won) * 100.0 / COUNT(*), 1) as win_pct
        FROM odds_games
        
        UNION ALL
        
        SELECT 
            'UNDERDOGS' as bet_type,
            COUNT(*) as games,
            COUNT(*) - SUM(favorite_won) as wins,
            ROUND((COUNT(*) - SUM(favorite_won)) * 100.0 / COUNT(*), 1) as win_pct
        FROM odds_games
    ''', conn)
    
    print(result.to_string(index=False))
    
    # 5. Spread covering analysis
    print("\n5️⃣  SPREAD COVERING ANALYSIS")
    print("-" * 80)
    
    result = pd.read_sql('''
        WITH spread_games AS (
            SELECT 
                m.match_id,
                m.home_score,
                m.away_score,
                bo.spread_home,
                CASE WHEN m.home_score - m.away_score > bo.spread_home THEN 1
                     WHEN m.home_score - m.away_score <= bo.spread_home THEN 0
                     ELSE NULL END as home_covered
            FROM matches m
            JOIN betting_odds bo ON m.match_id = bo.match_id
            WHERE m.sport = 'Hockey' AND m.home_score IS NOT NULL AND bo.spread_home IS NOT NULL
        )
        SELECT 
            'HOME COVERS' as outcome,
            SUM(CASE WHEN home_covered = 1 THEN 1 ELSE 0 END) as count,
            ROUND(SUM(CASE WHEN home_covered = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as pct
        FROM spread_games
        
        UNION ALL
        
        SELECT 
            'AWAY COVERS',
            SUM(CASE WHEN home_covered = 0 THEN 1 ELSE 0 END),
            ROUND(SUM(CASE WHEN home_covered = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)
        FROM spread_games
    ''', conn)
    
    print(result.to_string(index=False))
    
    # 6. Over/Under analysis
    print("\n6️⃣  OVER/UNDER ANALYSIS")
    print("-" * 80)
    
    result = pd.read_sql('''
        WITH ou_games AS (
            SELECT 
                m.match_id,
                m.home_score + m.away_score as total_points,
                bo.over_under,
                CASE WHEN (m.home_score + m.away_score) > bo.over_under THEN 'OVER'
                     WHEN (m.home_score + m.away_score) < bo.over_under THEN 'UNDER'
                     ELSE 'PUSH' END as result
            FROM matches m
            JOIN betting_odds bo ON m.match_id = bo.match_id
            WHERE m.sport = 'Hockey' AND m.home_score IS NOT NULL AND bo.over_under IS NOT NULL
        )
        SELECT 
            result,
            COUNT(*) as games,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM ou_games), 1) as pct
        FROM ou_games
        GROUP BY result
        ORDER BY games DESC
    ''', conn)
    
    print(result.to_string(index=False))
    
    # 7. Best performing teams (moneyline favorites)
    print("\n7️⃣  TOP 10 TEAMS - MONEYLINE FAVORITE PERFORMANCE")
    print("-" * 80)
    
    result = pd.read_sql('''
        WITH favorite_games AS (
            SELECT 
                CASE WHEN bo.home_moneyline < bo.away_moneyline THEN t_home.name
                     ELSE t_away.name END as team,
                CASE WHEN (bo.home_moneyline < bo.away_moneyline AND m.home_score > m.away_score)
                     OR (bo.home_moneyline >= bo.away_moneyline AND m.away_score > m.home_score)
                THEN 1 ELSE 0 END as won
            FROM matches m
            JOIN betting_odds bo ON m.match_id = bo.match_id
            JOIN teams t_home ON m.home_team_id = t_home.team_id
            JOIN teams t_away ON m.away_team_id = t_away.team_id
            WHERE m.sport = 'Hockey' AND m.home_score IS NOT NULL
        )
        SELECT 
            team,
            COUNT(*) as favorite_bets,
            SUM(won) as wins,
            ROUND(SUM(won) * 100.0 / COUNT(*), 1) as win_pct
        FROM favorite_games
        GROUP BY team
        HAVING COUNT(*) >= 5
        ORDER BY win_pct DESC
        LIMIT 10
    ''', conn)
    
    print(result.to_string(index=False))
    
    # 8. Worst performing teams (moneyline favorites)
    print("\n8️⃣  BOTTOM 10 TEAMS - MONEYLINE FAVORITE UNDERPERFORMANCE")
    print("-" * 80)
    
    result = pd.read_sql('''
        WITH favorite_games AS (
            SELECT 
                CASE WHEN bo.home_moneyline < bo.away_moneyline THEN t_home.name
                     ELSE t_away.name END as team,
                CASE WHEN (bo.home_moneyline < bo.away_moneyline AND m.home_score > m.away_score)
                     OR (bo.home_moneyline >= bo.away_moneyline AND m.away_score > m.home_score)
                THEN 1 ELSE 0 END as won
            FROM matches m
            JOIN betting_odds bo ON m.match_id = bo.match_id
            JOIN teams t_home ON m.home_team_id = t_home.team_id
            JOIN teams t_away ON m.away_team_id = t_away.team_id
            WHERE m.sport = 'Hockey' AND m.home_score IS NOT NULL
        )
        SELECT 
            team,
            COUNT(*) as favorite_bets,
            SUM(won) as wins,
            ROUND(SUM(won) * 100.0 / COUNT(*), 1) as win_pct
        FROM favorite_games
        GROUP BY team
        HAVING COUNT(*) >= 5
        ORDER BY win_pct ASC
        LIMIT 10
    ''', conn)
    
    print(result.to_string(index=False))
    
    # 9. Best recent trends (last 30 games)
    print("\n9️⃣  RECENT TRENDS - LAST 30 GAMES")
    print("-" * 80)
    
    result = pd.read_sql('''
        WITH recent_games AS (
            SELECT 
                t_home.name,
                m.home_score,
                m.away_score,
                bo.home_moneyline
            FROM matches m
            JOIN betting_odds bo ON m.match_id = bo.match_id
            JOIN teams t_home ON m.home_team_id = t_home.team_id
            WHERE m.sport = 'Hockey' AND m.home_score IS NOT NULL
            ORDER BY m.match_date DESC
            LIMIT 30
        )
        SELECT 
            name as team,
            COUNT(*) as games,
            SUM(CASE WHEN home_score > away_score THEN 1 ELSE 0 END) as wins,
            ROUND(SUM(CASE WHEN home_score > away_score THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as win_pct
        FROM recent_games
        GROUP BY name
        ORDER BY win_pct DESC
        LIMIT 10
    ''', conn)
    
    print(result.to_string(index=False))
    
    # 10. Biggest upsets (underdog wins)
    print("\n🔟 BIGGEST UPSETS - UNDERDOG WINS")
    print("-" * 80)
    
    result = pd.read_sql('''
        SELECT 
            CASE WHEN m.home_score > m.away_score THEN t_home.name ELSE t_away.name END as winner,
            CASE WHEN m.home_score > m.away_score THEN t_away.name ELSE t_home.name END as loser,
            m.home_score || '-' || m.away_score as score,
            ROUND(CASE WHEN m.away_score > m.home_score THEN bo.away_moneyline ELSE bo.home_moneyline END, 0) as underdog_odds
        FROM matches m
        JOIN betting_odds bo ON m.match_id = bo.match_id
        JOIN teams t_home ON m.home_team_id = t_home.team_id
        JOIN teams t_away ON m.away_team_id = t_away.team_id
        WHERE m.sport = 'Hockey' AND m.home_score IS NOT NULL
        AND ((m.away_score > m.home_score AND bo.home_moneyline < bo.away_moneyline)
             OR (m.home_score > m.away_score AND bo.away_moneyline < bo.home_moneyline))
        ORDER BY underdog_odds DESC
        LIMIT 10
    ''', conn)
    
    print(result.to_string(index=False))
    
    print("\n" + "=" * 80)
    print("END OF REPORT")
    print("=" * 80)
    
    conn.close()


if __name__ == "__main__":
    run_analysis()
