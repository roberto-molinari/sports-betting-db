#!/usr/bin/env python3
"""
Import Serie A Data from Football-Data.org
"""

from data_collector import SportDataCollector
import sys


def main():
    print("="*70)
    print("  SERIE A DATA IMPORT")
    print("="*70)
    
    # Get API key from user
    print("\nPaste your Football-Data.org API key below:")
    api_key = input("API Key: ").strip()
    
    if not api_key or api_key == "":
        print("❌ No API key provided")
        return
    
    print("\nStarting data collection...")
    print("-"*70)
    
    # Create collector and fetch data
    collector = SportDataCollector()
    success = collector.collect_serie_a_data(api_key=api_key, season=2024)
    
    if success:
        print("\n" + "="*70)
        print("  ✓ SUCCESS! Serie A data imported")
        print("="*70)
        print("\nNext steps:")
        print("  1. Run analysis: python test_analysis.py")
        print("  2. Add betting odds manually using add_betting_odds()")
        print("  3. Or use The Odds API for historical odds data")
    else:
        print("\n❌ Import failed. Check your API key and try again.")


if __name__ == "__main__":
    main()
