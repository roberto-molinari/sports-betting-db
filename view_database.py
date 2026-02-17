#!/usr/bin/env python3
"""
View current database contents
"""

import sqlite3
from sports_db import DATABASE_PATH


def print_section(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}\n")


def main():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    print_section("CURRENT DATABASE CONTENTS")
    
    # Teams
    cursor.execute("SELECT * FROM teams ORDER BY league, name")
    teams = cursor.fetchall()
    print(f"📋 TEAMS ({len(teams)} total)")
    print("-" * 70)
    for team in teams:
        print(f"  ID: {team['team_id']:3d} | {team['name']:30s} | {team['league']:10s} | {team['sport']}")
    
    # Matches
    cursor.execute("""
        SELECT m.match_id, h.name as home, a.name as away, 
               m.home_score, m.away_score, m.match_date, m.match_status, m.league
        FROM matches m
        JOIN teams h ON m.home_team_id = h.team_id
        JOIN teams a ON m.away_team_id = a.team_id
        ORDER BY m.match_date DESC
    """)
    matches = cursor.fetchall()
    print_section(f"⚽ MATCHES ({len(matches)} total)")
    for match in matches:
        score = f"{match['home_score']}-{match['away_score']}" if match['home_score'] is not None else "TBD"
        date = match['match_date'][:10] if match['match_date'] else "N/A"
        print(f"  [{match['league']:8s}] {date} | {match['home']:25s} vs {match['away']:25s} | {score:5s} | {match['match_status']}")
    
    # Betting odds
    cursor.execute("""
        SELECT bo.odds_id, h.name as home, a.name as away,
               bo.sportsbook, bo.home_moneyline, bo.away_moneyline, bo.spread_home, bo.over_under
        FROM betting_odds bo
        JOIN matches m ON bo.match_id = m.match_id
        JOIN teams h ON m.home_team_id = h.team_id
        JOIN teams a ON m.away_team_id = a.team_id
    """)
    odds = cursor.fetchall()
    print_section(f"💰 BETTING ODDS ({len(odds)} total)")
    for odd in odds:
        print(f"  {odd['sportsbook']:15s} | {odd['home']:25s} vs {odd['away']:25s}")
        print(f"    ML: {odd['home_moneyline']:6.0f} / {odd['away_moneyline']:6.0f} | Spread: {odd['spread_home']:5.1f} | O/U: {odd['over_under']:4.1f}")
    
    # Summary
    print_section("SUMMARY")
    cursor.execute("SELECT league, sport, COUNT(*) as count FROM teams GROUP BY league, sport")
    league_counts = cursor.fetchall()
    for row in league_counts:
        print(f"  {row['league']:15s} ({row['sport']:10s}): {row['count']} teams")
    
    cursor.execute("SELECT league, COUNT(*) as count FROM matches GROUP BY league")
    match_counts = cursor.fetchall()
    print(f"\nMatches by league:")
    for row in match_counts:
        print(f"  {row['league']:15s}: {row['count']} matches")
    
    conn.close()


if __name__ == "__main__":
    main()
