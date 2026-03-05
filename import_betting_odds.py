"""
Import betting odds from nhl_data_extensive_last_two_years.csv into the betting_odds table.
Maps ESPN odds data to matches in the database.
"""

import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime
from sports_db import DATABASE_PATH


def import_betting_odds_from_csv(csv_path, sportsbook="ESPN"):
    """
    Import betting odds from CSV file into betting_odds table.
    
    Args:
        csv_path: Path to nhl_data_extensive_last_two_years.csv
        sportsbook: Name of sportsbook (default: ESPN)
    
    Returns:
        Tuple of (inserted_count, skipped_count, error_count)
    """
    # Load CSV
    print(f"Loading CSV from {csv_path}...")
    df = pd.read_csv(csv_path, index_col=0)
    df['date'] = pd.to_datetime(df['date'])
    
    # Connect to database
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    # Group by game_id to get unique games (each game has 2 rows - home and away)
    print(f"Processing {len(df)} rows ({df['game_id'].nunique()} unique games)...")
    
    inserted = 0
    skipped = 0
    errors = 0
    
    for game_id, game_group in df.groupby('game_id'):
        try:
            # Get one row per game (both rows have same odds)
            game_row = game_group.iloc[0]
            
            # Get betting info
            game_date = str(game_row['date'])  # Convert pandas Timestamp to string
            spread = game_row['spread']
            over_under = game_row['over_under']
            favorite_moneyline = game_row['favorite_moneyline']
            
            # Find match_id in database
            # Match by game_id from ESPN (store as text in notes)
            cursor.execute('''
                SELECT match_id FROM matches 
                WHERE sport = 'NHL' 
                AND DATETIME(match_date) >= DATETIME(?)
                AND DATETIME(match_date) <= DATETIME(?, '+12 hours')
                LIMIT 1
            ''', (game_date, game_date))
            
            result = cursor.fetchone()
            if not result:
                # Try matching by team names if available
                home_team = game_group[game_group['is_home'] == 1].iloc[0]
                away_team = game_group[game_group['is_home'] == 0].iloc[0]
                
                home_name = home_team['team_name']
                away_name = away_team['team_name']
                
                cursor.execute('''
                    SELECT m.match_id 
                    FROM matches m
                    JOIN teams t_home ON m.home_team_id = t_home.team_id
                    JOIN teams t_away ON m.away_team_id = t_away.team_id
                    WHERE t_home.name = ? 
                    AND t_away.name = ?
                    AND DATE(m.match_date) = DATE(?)
                ''', (home_name, away_name, game_date))
                
                result = cursor.fetchone()
            
            if not result:
                skipped += 1
                print(f"  ⚠ Skipped game_id {game_id}: no matching match in database")
                continue
            
            match_id = result[0]
            
            # Determine home/away moneyline
            # favorite_moneyline is for the favored team
            home_row = game_group[game_group['is_home'] == 1].iloc[0]
            away_row = game_group[game_group['is_home'] == 0].iloc[0]
            
            # In American odds:
            # - Favorite has negative odds (e.g., -110)
            # - Underdog has positive odds (e.g., +110)
            # If favorite_moneyline is negative, it's for the favorite team
            if favorite_moneyline < 0:
                # Favorite is either home or away
                # Spread sign indicates: negative spread = home team favored
                if spread < 0:
                    # Home is favored
                    home_moneyline = favorite_moneyline
                    away_moneyline = -100 / (favorite_moneyline / 100)  # Approximate underdog odds
                else:
                    # Away is favored
                    away_moneyline = favorite_moneyline
                    home_moneyline = -100 / (favorite_moneyline / 100)
            else:
                # Shouldn't happen with standard sportsbook odds
                home_moneyline = favorite_moneyline
                away_moneyline = favorite_moneyline
            
            # Determine spread direction (home team spread)
            spread_home = spread  # Negative means home is favored
            spread_away = -spread  # For away team
            
            # Insert betting odds
            cursor.execute('''
                INSERT INTO betting_odds 
                (match_id, sportsbook, odds_date, home_moneyline, away_moneyline, 
                 spread_home, spread_away, over_under, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                match_id, 
                sportsbook, 
                game_date, 
                float(home_moneyline),
                float(away_moneyline),
                float(spread_home),
                float(spread_away),
                float(over_under),
                f"ESPN game_id: {game_id}"
            ))
            
            inserted += 1
            if inserted % 500 == 0:
                print(f"  Inserted {inserted} odds...")
        
        except Exception as e:
            errors += 1
            print(f"  ✗ Error processing game_id {game_id}: {e}")
            continue
    
    conn.commit()
    conn.close()
    
    print(f"\n✓ Import complete:")
    print(f"  Inserted: {inserted}")
    print(f"  Skipped: {skipped}")
    print(f"  Errors: {errors}")
    
    return inserted, skipped, errors


if __name__ == "__main__":
    csv_path = Path(__file__).parent / "nhl_data_extensive_last_two_years.csv"
    
    if not csv_path.exists():
        print(f"Error: {csv_path} not found")
        exit(1)
    
    import_betting_odds_from_csv(str(csv_path))
