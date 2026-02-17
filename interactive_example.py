#!/usr/bin/env python3
"""
Interactive example - copy/paste these commands into Python REPL
Or run this script directly
"""

# Import the analyzer
from betting_analyzer import BettingAnalyzer

# Create analyzer instance
analyzer = BettingAnalyzer()

# Example 1: Check all Serie A moneyline bets
print("Serie A Moneyline Analysis:")
result = analyzer.analyze_moneyline_accuracy('Serie A', 'Soccer', days=365)
for match in result['recent_matches']:
    print(f"  {match['match']}: {match['score']} - {match['result']}")

print(f"\nFavorite win rate: {result['favorite_win_rate']}")
print(f"Total upsets: {result['upset_count']}\n")

# Example 2: Check NHL spread covering
print("NHL Spread Analysis:")
result = analyzer.analyze_spread_covering('NHL', 'Hockey', days=365)
for match in result['recent_results']:
    print(f"  {match['match']}: {match['score']}, Spread: {match['spread']}, Result: {match['cover']}")

# Example 3: Generate full report
print("\n" + "="*70)
analyzer.generate_summary_report("Serie A", "Soccer")
