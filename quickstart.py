#!/usr/bin/env python3
"""
Quick Start Script for Sports Betting Database
Run this to initialize and test the system
"""

from sports_db import init_database, get_all_teams, get_matches_by_league_and_date
from data_collector import SportDataCollector
from betting_analyzer import BettingAnalyzer
from datetime import datetime, timedelta


def print_header(title):
    """Print formatted section header."""
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}\n")


def main():
    """Run quick start setup and demo."""
    
    print_header("SPORTS BETTING DATABASE - QUICK START")
    
    # Step 1: Initialize database
    print("Step 1: Initializing database...")
    init_database()
    print("✓ Database initialized\n")
    
    # Step 2: Add sample data
    print("Step 2: Adding sample data...")
    collector = SportDataCollector()
    collector.add_sample_data()
    print("✓ Sample data added\n")
    
    # Step 3: Verify data
    print("Step 3: Verifying data...\n")
    
    # Show Serie A teams
    serie_a_teams = get_all_teams(league='Serie A')
    print(f"Serie A Teams ({len(serie_a_teams)}):")
    for team in serie_a_teams:
        print(f"  • {team['name']}")
    
    # Show NHL teams
    nhl_teams = get_all_teams(league='NHL')
    print(f"\nNHL Teams ({len(nhl_teams)}):")
    for team in nhl_teams:
        print(f"  • {team['name']}")
    
    # Show recent matches
    print_header("Recent Matches")
    
    end_date = datetime.now().isoformat()
    start_date = (datetime.now() - timedelta(days=90)).isoformat()
    
    matches = get_matches_by_league_and_date('Serie A', start_date, end_date)
    if matches:
        print("Recent Serie A Matches:")
        for match in matches[-3:]:
            score = f"{match['home_score']}-{match['away_score']}" if match['home_score'] is not None else "TBD"
            print(f"  • {match['home_team_name']} vs {match['away_team_name']} - {score}")
    
    matches = get_matches_by_league_and_date('NHL', start_date, end_date)
    if matches:
        print("\nRecent NHL Matches:")
        for match in matches[-3:]:
            score = f"{match['home_score']}-{match['away_score']}" if match['home_score'] is not None else "TBD"
            print(f"  • {match['home_team_name']} vs {match['away_team_name']} - {score}")
    
    # Step 4: Run analysis
    print_header("Running Analysis")
    
    analyzer = BettingAnalyzer()
    
    # Serie A analysis
    print("Serie A Analysis:")
    print("-" * 70)
    ml_analysis = analyzer.analyze_moneyline_accuracy('Serie A', 'Soccer', days=365)
    if 'total_games' in ml_analysis:
        print(f"Total matches analyzed: {ml_analysis['total_games']}")
        if ml_analysis['total_games'] > 0:
            print(f"Favorite win rate: {ml_analysis['favorite_win_rate']}")
            print(f"Upsets: {ml_analysis['upset_count']}")
    else:
        print(ml_analysis.get('status', 'Analysis unavailable'))
    
    # NHL analysis
    print("\nNHL Analysis:")
    print("-" * 70)
    ml_analysis = analyzer.analyze_moneyline_accuracy('NHL', 'Hockey', days=365)
    if 'total_games' in ml_analysis:
        print(f"Total matches analyzed: {ml_analysis['total_games']}")
        if ml_analysis['total_games'] > 0:
            print(f"Favorite win rate: {ml_analysis['favorite_win_rate']}")
            print(f"Upsets: {ml_analysis['upset_count']}")
    else:
        print(ml_analysis.get('status', 'Analysis unavailable'))
    
    # Step 5: Next steps
    print_header("Next Steps")
    
    print("""
1. POPULATE WITH REAL DATA:
   
   For Serie A (Soccer):
   - Register at https://www.football-data.org/ for free API key
   - Update data_collector.py with your API key
   - Run: collector.collect_serie_a_data(season=2024)
   
   For NHL (Hockey):
   - NHL API is free at https://statsapi.web.nhl.com/api/v1
   - Run: collector.collect_nhl_data(season=2024)

2. ADD HISTORICAL BETTING ODDS:
   
   - Get data from The Odds API (theoddsapi.com)
   - Or manually import from Covers.com or Sports-Reference
   - Use add_betting_odds() to populate the database

3. RUN ANALYSIS:
   
   analyzer = BettingAnalyzer()
   analyzer.analyze_moneyline_accuracy('Serie A', 'Soccer', days=365)
   analyzer.analyze_spread_covering('NHL', 'Hockey', days=365)
   analyzer.identify_line_movement_opportunities('Serie A', days=30)

4. EXPORT RESULTS:
   
   - Use SQL queries to extract specific data
   - Generate CSV reports for further analysis
   - Create visualizations with pandas/matplotlib

For detailed documentation, see: README.md
    """)
    
    print_header("Setup Complete!")
    print("Database is ready for use. See README for API usage and examples.\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error during setup: {e}")
        print("\nTroubleshooting:")
        print("1. Ensure all files are in the same directory")
        print("2. Check Python version (3.8+)")
        print("3. Verify no database file is locked")
        import traceback
        traceback.print_exc()
