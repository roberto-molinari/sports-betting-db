#!/usr/bin/env python3
"""
Import Real Betting Odds from Football-Data.co.uk CSV File
Download the CSV manually and this script will import it
"""

import csv
from datetime import datetime
from sports_db import add_betting_odds
import sqlite3
from sports_db import DATABASE_PATH


def normalize_team_name(name):
    """Normalize team names to match database entries."""
    name_mapping = {
        # Short names from CSV
        'Milan': 'AC Milan',
        'Inter': 'FC Internazionale Milano',
        'Internazionale': 'FC Internazionale Milano',
        'Juventus': 'Juventus FC',
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
        'Sassuolo': 'US Sassuolo Calcio',
        'Lecce': 'US Lecce',
        'Parma': 'Parma Calcio 1913',
        'Como': 'Como 1907',
        'Empoli': 'Empoli FC',
        'Monza': 'AC Monza',
        'Cremonese': 'US Cremonese',
        'Pisa': 'AC Pisa 1909',
        
        # Full names for direct matching
        'AC Milan': 'AC Milan',
        'AS Roma': 'AS Roma',
        'SSC Napoli': 'SSC Napoli',
        'SS Lazio': 'SS Lazio',
        'ACF Fiorentina': 'ACF Fiorentina',
        'Hellas Verona': 'Hellas Verona FC',
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
    
    # Find match by teams and date
    cursor.execute("""
        SELECT match_id FROM matches 
        WHERE home_team_id = ? 
        AND away_team_id = ?
        AND date(match_date) = date(?)
    """, (home_id, away_id, match_date))
    
    result = cursor.fetchone()
    conn.close()
    
    return result[0] if result else None


def decimal_to_american(decimal_odds):
    """Convert decimal odds to American moneyline."""
    if decimal_odds >= 2.0:
        return (decimal_odds - 1) * 100
    else:
        return -100 / (decimal_odds - 1)


def import_csv_file(filename):
    """Import odds from a downloaded CSV file."""
    print(f"Reading {filename}...")
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            imported = 0
            skipped = 0
            no_match = 0
            
            for row in reader:
                try:
                    # Skip empty rows
                    if not row.get('Date') or not row.get('HomeTeam'):
                        continue
                    
                    # Parse date
                    date_str = row['Date'].strip()
                    try:
                        match_date = datetime.strptime(date_str, '%d/%m/%Y')
                    except:
                        try:
                            match_date = datetime.strptime(date_str, '%d/%m/%y')
                        except:
                            skipped += 1
                            continue
                    
                    # Normalize team names
                    home_team = normalize_team_name(row['HomeTeam'].strip())
                    away_team = normalize_team_name(row['AwayTeam'].strip())
                    
                    # Find match
                    match_id = find_match_id(home_team, away_team, match_date.isoformat())
                    
                    if not match_id:
                        no_match += 1
                        print(f"  No match found: {home_team} vs {away_team} on {date_str}")
                        continue
                    
                    # Get Bet365 odds
                    home_decimal = float(row.get('B365H', 0))
                    away_decimal = float(row.get('B365A', 0))
                    
                    if home_decimal == 0 or away_decimal == 0:
                        skipped += 1
                        continue
                    
                    # Convert to American odds
                    home_ml = decimal_to_american(home_decimal)
                    away_ml = decimal_to_american(away_decimal)
                    
                    # Get over/under if available
                    over_under = None
                    try:
                        over_under = float(row.get('BbAv>2.5', 0))
                        if over_under == 0:
                            over_under = None
                    except:
                        pass
                    
                    # Add odds
                    add_betting_odds(
                        match_id=match_id,
                        sportsbook='Bet365',
                        odds_date=match_date.isoformat(),
                        home_moneyline=home_ml,
                        away_moneyline=away_ml,
                        over_under=over_under,
                        notes='Imported from Football-Data.co.uk'
                    )
                    
                    imported += 1
                    
                except Exception as e:
                    skipped += 1
                    print(f"  Error: {e}")
            
            print(f"\n✓ Import complete!")
            print(f"  Imported: {imported} matches with odds")
            print(f"  No match found in DB: {no_match}")
            print(f"  Skipped: {skipped}")
            
    except FileNotFoundError:
        print(f"❌ File not found: {filename}")
        print("\nPlease download the CSV file first:")
        print("  1. Go to http://www.football-data.co.uk/italym.php")
        print("  2. Click on a season (e.g., 2024-25)")
        print("  3. Save the CSV file to this folder")
        print("  4. Run this script again")


def main():
    print("="*70)
    print("  IMPORT REAL BETTING ODDS FROM CSV")
    print("  Source: Football-Data.co.uk")
    print("="*70)
    
    print("\nSTEP 1: Download CSV file")
    print("-" * 70)
    print("Go to: http://www.football-data.co.uk/italym.php")
    print("\nClick on a season to download, for example:")
    print("  • 2024-25: http://www.football-data.co.uk/mmz4281/2425/I1.csv")
    print("  • 2023-24: http://www.football-data.co.uk/mmz4281/2324/I1.csv")
    print("  • 2022-23: http://www.football-data.co.uk/mmz4281/2223/I1.csv")
    print("\nSave the CSV file to this folder.")
    
    print("\n" + "="*70)
    print("STEP 2: Import the CSV file")
    print("-" * 70)
    
    filename = input("\nEnter CSV filename (e.g., I1.csv): ").strip()
    
    if not filename:
        print("No filename provided")
        return
    
    import_csv_file(filename)
    
    print("\n" + "="*70)
    print("  Run analysis to see results: python test_analysis.py")
    print("="*70)


if __name__ == "__main__":
    main()
