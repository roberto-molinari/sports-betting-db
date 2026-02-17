"""
Sports Betting Database - Schema and Core Functions
Supports Serie A (Soccer) and NHL (Hockey) historical data with betting odds
"""

import sqlite3
import os
from datetime import datetime
from pathlib import Path


DATABASE_PATH = Path(__file__).parent / "sports_betting.db"


def init_database():
    """Initialize the sports betting database with all necessary tables."""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    # Teams table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS teams (
            team_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            sport TEXT NOT NULL,
            league TEXT NOT NULL,
            country TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Matches table (for both soccer and hockey)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS matches (
            match_id INTEGER PRIMARY KEY,
            sport TEXT NOT NULL,
            league TEXT NOT NULL,
            season INTEGER,
            home_team_id INTEGER NOT NULL,
            away_team_id INTEGER NOT NULL,
            match_date TIMESTAMP NOT NULL,
            home_score INTEGER,
            away_score INTEGER,
            match_status TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
            FOREIGN KEY (away_team_id) REFERENCES teams(team_id)
        )
    ''')
    
    # Betting odds table - stores opening lines from sportsbooks
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS betting_odds (
            odds_id INTEGER PRIMARY KEY,
            match_id INTEGER NOT NULL,
            sportsbook TEXT NOT NULL,
            odds_date TIMESTAMP NOT NULL,
            home_moneyline REAL,
            away_moneyline REAL,
            spread_home REAL,
            spread_away REAL,
            spread_home_odds REAL,
            spread_away_odds REAL,
            over_under REAL,
            over_odds REAL,
            under_odds REAL,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES matches(match_id)
        )
    ''')
    
    # Match results summary (for easy analysis)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS match_outcomes (
            outcome_id INTEGER PRIMARY KEY,
            match_id INTEGER NOT NULL UNIQUE,
            home_team_id INTEGER NOT NULL,
            away_team_id INTEGER NOT NULL,
            home_score INTEGER NOT NULL,
            away_score INTEGER NOT NULL,
            result TEXT NOT NULL,
            home_covered_spread BOOLEAN,
            away_covered_spread BOOLEAN,
            total_points INTEGER,
            over_under_result TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES matches(match_id),
            FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
            FOREIGN KEY (away_team_id) REFERENCES teams(team_id)
        )
    ''')
    
    # Betting analysis table - for tracking patterns and inefficiencies
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS betting_analysis (
            analysis_id INTEGER PRIMARY KEY,
            match_id INTEGER NOT NULL,
            analysis_type TEXT NOT NULL,
            opening_favorite REAL,
            closing_line_movement REAL,
            actual_result TEXT,
            prediction_correct BOOLEAN,
            roi_potential REAL,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES matches(match_id)
        )
    ''')
    
    # Create indexes for faster queries
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_match_date ON matches(match_date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_league ON matches(league)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sport ON matches(sport)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sportsbook ON betting_odds(sportsbook)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_team_name ON teams(name)')
    
    conn.commit()
    conn.close()
    print(f"Database initialized at {DATABASE_PATH}")


def add_team(name, sport, league, country=None):
    """Add a team to the database."""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO teams (name, sport, league, country)
            VALUES (?, ?, ?, ?)
        ''', (name, sport, league, country))
        conn.commit()
        team_id = cursor.lastrowid
        print(f"Added team: {name} (ID: {team_id})")
        return team_id
    except sqlite3.IntegrityError:
        print(f"Team {name} already exists")
        cursor.execute('SELECT team_id FROM teams WHERE name = ?', (name,))
        return cursor.fetchone()[0]
    finally:
        conn.close()


def add_match(sport, league, season, home_team_id, away_team_id, match_date, status="scheduled"):
    """Add a match to the database."""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO matches (sport, league, season, home_team_id, away_team_id, match_date, match_status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (sport, league, season, home_team_id, away_team_id, match_date, status))
        conn.commit()
        match_id = cursor.lastrowid
        return match_id
    finally:
        conn.close()


def add_betting_odds(match_id, sportsbook, odds_date, home_moneyline=None, away_moneyline=None,
                     spread_home=None, spread_away=None, over_under=None, notes=None):
    """Add betting odds for a match."""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO betting_odds 
            (match_id, sportsbook, odds_date, home_moneyline, away_moneyline, 
             spread_home, spread_away, over_under, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (match_id, sportsbook, odds_date, home_moneyline, away_moneyline,
              spread_home, spread_away, over_under, notes))
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()


def update_match_result(match_id, home_score, away_score):
    """Update match with final result."""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            UPDATE matches 
            SET home_score = ?, away_score = ?, match_status = 'completed'
            WHERE match_id = ?
        ''', (home_score, away_score, match_id))
        conn.commit()
    finally:
        conn.close()


def get_matches_by_league_and_date(league, start_date, end_date):
    """Retrieve matches for a league within a date range."""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT m.*, 
               h.name as home_team_name, 
               a.name as away_team_name
        FROM matches m
        JOIN teams h ON m.home_team_id = h.team_id
        JOIN teams a ON m.away_team_id = a.team_id
        WHERE m.league = ? AND m.match_date BETWEEN ? AND ?
        ORDER BY m.match_date
    ''', (league, start_date, end_date))
    
    matches = cursor.fetchall()
    conn.close()
    return matches


def get_betting_odds_for_match(match_id):
    """Retrieve all betting odds for a specific match."""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT * FROM betting_odds
        WHERE match_id = ?
        ORDER BY odds_date
    ''', (match_id,))
    
    odds = cursor.fetchall()
    conn.close()
    return odds


def get_team_id(team_name):
    """Get team ID by name."""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    cursor.execute('SELECT team_id FROM teams WHERE name = ?', (team_name,))
    result = cursor.fetchone()
    conn.close()
    
    return result[0] if result else None


def get_all_teams(sport=None, league=None):
    """Get all teams, optionally filtered by sport or league."""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    if sport and league:
        cursor.execute('SELECT * FROM teams WHERE sport = ? AND league = ?', (sport, league))
    elif sport:
        cursor.execute('SELECT * FROM teams WHERE sport = ?', (sport,))
    elif league:
        cursor.execute('SELECT * FROM teams WHERE league = ?', (league,))
    else:
        cursor.execute('SELECT * FROM teams')
    
    teams = cursor.fetchall()
    conn.close()
    return teams


if __name__ == "__main__":
    init_database()
    print("Database setup complete!")
