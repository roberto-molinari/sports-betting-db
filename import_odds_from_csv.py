#!/usr/bin/env python3
"""
Import Historical Betting Odds from Football-Data.co.uk
Downloads Serie A CSV files with historical odds and imports them
"""

import requests
import csv
import io
from datetime import datetime
from sports_db import add_betting_odds, get_team_id
import sqlite3
from sports_db import DATABASE_PATH


def download_serie_a_odds(season):
    """
    Download Serie A odds CSV from Football-Data.co.uk
    Season format: '2425' for 2024-25 season, '2324' for 2023-24, etc.
    """
    # URL pattern: http://www.football-data.co.uk/mmz4281/2425/I1.csv
    # I1 = Serie A (Italy Division 1)
    url = f"http://www.football-data.co.uk/mmz4281/{season}/I1.csv"
    
    print(f"Downloading Serie A odds for season {season}...")
    print(f"URL: {url}")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error downloading: {e}")
        return None


def normalize_team_name(name):
    """Normalize team names to match database entries."""
    # Map Football-Data.co.uk names to our database names
    name_mapping = {
        'AC Milan': 'AC Milan',
        'Inter': 'FC Internazionale Milano',
        'Inter Milan': 'FC Internazionale Milano',
        'Internazionale': 'FC Internazionale Milano',
        'Juventus': 'Juventus FC',
        'AS Roma': 'AS Roma',
        'Roma': 'AS Roma',
        'Napoli': 'SSC Napoli',
        'Lazio': 'SS Lazio',
        'Atalanta': 'Atalanta BC',
        'Fiorentina': 'ACF Fiorentina',
        'Bologna': 'Bologna FC 1909',
        'Torino': 'Torino FC',
        'Genoa': 'Genoa CFC',
        'Cagliari': 'Cagliari Calcio',
        'Udinese': 'Udinese Calcio',
        'Verona': 'Hellas Verona FC',
        'Hellas Verona': 'Hellas Verona FC',
        'Sassuolo': 'US Sassuolo Calcio',
        'Lecce': 'US Lecce',
        'Parma': 'Parma Calcio 1913',
        'Como': 'Como 1907',
        'Empoli': 'Empoli FC',
        'Monza': 'AC Monza',
        'Salernitana': 'US Salernitana 1919',
        'Spezia': 'Spezia Calcio',
        'Cremonese': 'US Cremonese',
        'Sampdoria': 'UC Sampdorla',
        'Venezia': 'Venezia FC',
        'Pisa': 'AC Pisa 1909',
    }
    
    return name_mapping.get(name, name)


def find_match_id(home_team, away_team, match_date):
    """Find match_id in database by teams and date."""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    # Get team IDs
    cursor.execute("SELECT team_id FROM teams WHERE name = ?", (home_team,))
    home_result = cursor.fetchone()
    if not home_result:
        conn.close()
        return None
    home_id = home_result[0]
    
    cursor.execute("SELECT team_id FROM teams WHERE name = ?", (away_team,))
    away_result = cursor.fetchone()
    if not away_result:
        conn.close()
        return None
    away_id = away_result[0]
    
    # Find match by teams and approximate date (within 2 days)
    cursor.execute("""
        SELECT match_id FROM matches 
        WHERE home_team_id = ? 
        AND away_team_id = ?
        AND date(match_date) = date(?)
    """, (home_id, away_id, match_date))
    
    result = cursor.fetchone()
    conn.close()
    
    return result[0] if result else None


def import_odds_from_csv(csv_text, sportsbook='Bet365'):
    """
    Import odds from CSV text.
    Default to Bet365 odds (B365H, B365D, B365A columns)
    """
    if not csv_text:
        print("No CSV data to import")
        return
    
    # Parse CSV
    csv_file = io.StringIO(csv_text)
    reader = csv.DictReader(csv_file)
    
    imported = 0
    skipped = 0
    errors = []
    
    for row in reader:
        try:
            # Skip rows with missing data
            if not row.get('Date') or not row.get('HomeTeam') or not row.get('AwayTeam'):
                continue
            
            # Parse date (format: DD/MM/YY or DD/MM/YYYY)
            date_str = row['Date'].strip()
            try:
                # Try DD/MM/YY format first
                match_date = datetime.strptime(date_str, '%d/%m/%y')
            except:
                try:
                    # Try DD/MM/YYYY format
                    match_date = datetime.strptime(date_str, '%d/%m/%Y')
                except:
                    print(f"Could not parse date: {date_str}")
                    skipped += 1
                    continue
            
            # Normalize team names
            home_team = normalize_team_name(row['HomeTeam'].strip())
            away_team = normalize_team_name(row['AwayTeam'].strip())
            
            # Find match in database
            match_id = find_match_id(home_team, away_team, match_date.isoformat())
            
            if not match_id:
                skipped += 1
                continue
            
            # Get Bet365 odds (B365H = Home, B365D = Draw, B365A = Away)
            # Convert decimal odds to American moneyline
            try:
                home_decimal = float(row.get('B365H', 0))
                away_decimal = float(row.get('B365A', 0))
                
                # Convert decimal to American odds
                if home_decimal > 0:
                    home_ml = (home_decimal - 1) * 100 if home_decimal >= 2 else -100 / (home_decimal - 1)
                else:
                    home_ml = 0
                
                if away_decimal > 0:
                    away_ml = (away_decimal - 1) * 100 if away_decimal >= 2 else -100 / (away_decimal - 1)
                else:
                    away_ml = 0
                
                # Add odds to database
                add_betting_odds(
                    match_id=match_id,
                    sportsbook=sportsbook,
                    odds_date=match_date.isoformat(),
                    home_moneyline=home_ml,
                    away_moneyline=away_ml,
                    notes=f'Imported from Football-Data.co.uk'
                )
                
                imported += 1
                
            except (ValueError, KeyError) as e:
                errors.append(f"Error processing odds for {home_team} vs {away_team}: {e}")
                skipped += 1
                
        except Exception as e:
            errors.append(f"Error processing row: {e}")
            skipped += 1
    
    print(f"\n✓ Import complete!")
    print(f"  Imported: {imported} matches with odds")
    print(f"  Skipped: {skipped} matches")
    
    if errors:
        print(f"\nErrors encountered: {len(errors)}")
        for error in errors[:10]:  # Show first 10 errors
            print(f"  - {error}")


def main():
    print("="*70)
    print("  IMPORT HISTORICAL BETTING ODDS")
    print("  Source: Football-Data.co.uk")
    print("="*70)
    
    # Current season is 2024-25
    # Format: '2425' means 2024-25 season
    seasons = ['2425', '2324', '2223']  # Last 3 seasons
    
    print("\nAvailable seasons:")
    print("  2425 = 2024-25 season (current)")
    print("  2324 = 2023-24 season")
    print("  2223 = 2022-23 season")
    print("  etc.")
    
    print("\nWhich season(s) to import? (comma-separated or 'all')")
    choice = input("Enter seasons (e.g., '2425' or '2425,2324' or 'all'): ").strip()
    
    if choice.lower() == 'all':
        seasons_to_import = seasons
    else:
        seasons_to_import = [s.strip() for s in choice.split(',')]
    
    for season in seasons_to_import:
        print(f"\n{'-'*70}")
        csv_data = download_serie_a_odds(season)
        if csv_data:
            import_odds_from_csv(csv_data, sportsbook='Bet365')
        else:
            print(f"Failed to download season {season}")
    
    print("\n" + "="*70)
    print("  Import complete! Run analysis to see results.")
    print("  Command: python test_analysis.py")
    print("="*70)


if __name__ == "__main__":
    main()
