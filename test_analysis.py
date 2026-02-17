#!/usr/bin/env python3
"""
Test Analysis Features with Sample Data
Run this to explore the betting analysis capabilities
"""

from betting_analyzer import BettingAnalyzer
from sports_db import get_all_teams, get_matches_by_league_and_date
from datetime import datetime, timedelta
import json


def print_section(title):
    """Print formatted section header."""
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}\n")


def main():
    analyzer = BettingAnalyzer()
    
    print_section("TESTING BETTING ANALYSIS WITH SAMPLE DATA")
    
    # Test 1: Moneyline Analysis for Serie A
    print("TEST 1: Moneyline Analysis - Serie A")
    print("-" * 70)
    result = analyzer.analyze_moneyline_accuracy(league='Serie A', sport='Soccer', days=365)
    print(json.dumps(result, indent=2))
    
    # Test 2: Moneyline Analysis for NHL
    print_section("TEST 2: Moneyline Analysis - NHL")
    print("-" * 70)
    result = analyzer.analyze_moneyline_accuracy(league='NHL', sport='Hockey', days=365)
    print(json.dumps(result, indent=2))
    
    # Test 3: Spread Analysis for Serie A
    print_section("TEST 3: Spread Covering Analysis - Serie A")
    print("-" * 70)
    result = analyzer.analyze_spread_covering(league='Serie A', sport='Soccer', days=365)
    print(json.dumps(result, indent=2))
    
    # Test 4: Spread Analysis for NHL
    print_section("TEST 4: Spread Covering Analysis - NHL")
    print("-" * 70)
    result = analyzer.analyze_spread_covering(league='NHL', sport='Hockey', days=365)
    print(json.dumps(result, indent=2))
    
    # Test 5: Line Movement Analysis
    print_section("TEST 5: Line Movement Opportunities - Serie A")
    print("-" * 70)
    result = analyzer.identify_line_movement_opportunities(league='Serie A', days=30)
    print(json.dumps(result, indent=2))
    
    # Test 6: Comprehensive Summary Reports
    print_section("TEST 6: Comprehensive Summary Report - Serie A")
    analyzer.generate_summary_report("Serie A", "Soccer")
    
    print_section("TEST 7: Comprehensive Summary Report - NHL")
    analyzer.generate_summary_report("NHL", "Hockey")
    
    # Test 8: All sports combined
    print_section("TEST 8: All Sports Combined Analysis")
    print("Moneyline Analysis (All Leagues):")
    print("-" * 70)
    result = analyzer.analyze_moneyline_accuracy(days=365)
    print(json.dumps(result, indent=2))
    
    print("\n" + "="*70)
    print("  TESTING COMPLETE!")
    print("="*70)
    print("\n✓ All analysis features tested successfully")
    print("\nTo add more sample data and test further:")
    print("  1. Edit data_collector.py add_sample_data() to add more matches")
    print("  2. Re-run: python quickstart.py")
    print("  3. Run this test again: python test_analysis.py")


if __name__ == "__main__":
    main()
