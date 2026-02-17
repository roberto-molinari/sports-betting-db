#!/usr/bin/env python3
"""Debug the CSV import process"""
import csv
from datetime import datetime
import sqlite3
from sports_db import DATABASE_PATH

filename = input("Enter CSV filename: ").strip()

try:
    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        print("\nFirst 5 rows from CSV:")
        print("="*70)
        
        for i, row in enumerate(reader):
            if i >= 5:
                break
            
            print(f"\nRow {i+1}:")
            print(f"  Date: {row.get('Date')}")
            print(f"  Home: {row.get('HomeTeam')}")
            print(f"  Away: {row.get('AwayTeam')}")
            print(f"  B365H: {row.get('B365H')}")
            print(f"  B365A: {row.get('B365A')}")
        
        print("\n" + "="*70)
        print("Team names in database:")
        print("="*70)
        
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM teams WHERE league = 'Serie A' ORDER BY name")
        teams = cursor.fetchall()
        
        for team in teams:
            print(f"  • {team[0]}")
        
        conn.close()
        
except FileNotFoundError:
    print(f"File not found: {filename}")
