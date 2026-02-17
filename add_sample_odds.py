#!/usr/bin/env python3
"""
Add Sample Betting Odds to Existing Matches
This adds realistic mock odds to your Serie A matches for testing
"""

import sqlite3
import random
from sports_db import DATABASE_PATH, add_betting_odds


def add_sample_odds_to_matches(limit=100):
    """Add sample betting odds to completed Serie A matches."""
    
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    # Get completed Serie A matches without odds
    cursor.execute("""
        SELECT m.match_id, m.home_score, m.away_score, m.match_date
        FROM matches m
        LEFT JOIN betting_odds bo ON m.match_id = bo.match_id
        WHERE m.league = 'Serie A'
        AND m.match_status = 'completed'
        AND m.home_score IS NOT NULL
        AND bo.odds_id IS NULL
        LIMIT ?
    """, (limit,))
    
    matches = cursor.fetchall()
    conn.close()
    
    if not matches:
        print("No matches found without odds")
        return
    
    print(f"Adding sample odds to {len(matches)} matches...")
    
    added = 0
    for match_id, home_score, away_score, match_date in matches:
        # Generate realistic odds based on actual result
        # Favorites typically have negative moneylines (-120 to -200)
        # Underdogs have positive moneylines (+100 to +180)
        
        score_diff = home_score - away_score
        
        if score_diff > 0:
            # Home won - make home favorite
            home_ml = random.randint(-200, -110)
            away_ml = random.randint(100, 180)
            spread = random.choice([-0.5, -1.0, -1.5])
        elif score_diff < 0:
            # Away won - make away favorite
            home_ml = random.randint(100, 180)
            away_ml = random.randint(-200, -110)
            spread = random.choice([0.5, 1.0, 1.5])
        else:
            # Draw - even odds
            home_ml = random.randint(-120, -100)
            away_ml = random.randint(-120, -100)
            spread = 0.0
        
        # Total goals for over/under
        total_goals = home_score + away_score
        over_under = random.choice([2.5, 3.0, 3.5])
        
        # Add odds
        add_betting_odds(
            match_id=match_id,
            sportsbook='Sample-Data',
            odds_date=match_date,
            home_moneyline=home_ml,
            away_moneyline=away_ml,
            spread_home=spread,
            over_under=over_under,
            notes='Generated sample odds for testing'
        )
        
        added += 1
    
    print(f"✓ Added sample odds to {added} matches")
    print(f"\nNow run: python test_analysis.py")


def main():
    print("="*70)
    print("  ADD SAMPLE BETTING ODDS")
    print("="*70)
    
    print("\nThis will add realistic mock betting odds to your Serie A matches")
    print("for testing the analysis features.")
    
    print("\nHow many matches to add odds for?")
    print("  Recommended: 100-200 for good analysis")
    
    choice = input("\nEnter number of matches (or 'all'): ").strip()
    
    if choice.lower() == 'all':
        limit = 999999
    else:
        try:
            limit = int(choice)
        except:
            limit = 100
            print(f"Invalid input, using default: {limit}")
    
    add_sample_odds_to_matches(limit)
    
    print("\n" + "="*70)
    print("  Sample odds added!")
    print("="*70)


if __name__ == "__main__":
    main()
