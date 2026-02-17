"""
Betting Analysis Module
Analyzes historical data to identify market inefficiencies and betting opportunities
"""

import sqlite3
from datetime import datetime, timedelta
from sports_db import DATABASE_PATH
from collections import defaultdict
import statistics


class BettingAnalyzer:
    """Analyzes betting data and match results to find opportunities."""
    
    def __init__(self):
        self.db_path = DATABASE_PATH
    
    def get_connection(self):
        """Get database connection with row factory."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def analyze_moneyline_accuracy(self, league=None, sport=None, days=365):
        """
        Analyze how accurate opening moneylines were at predicting outcomes.
        Returns teams/leagues where favorites underperformed or underdogs overperformed.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        start_date = (datetime.now() - timedelta(days=days)).isoformat()
        
        query = '''
            SELECT 
                m.match_id,
                h.name as home_team,
                a.name as away_team,
                m.home_score,
                m.away_score,
                bo.home_moneyline,
                bo.away_moneyline,
                m.match_date
            FROM matches m
            JOIN teams h ON m.home_team_id = h.team_id
            JOIN teams a ON m.away_team_id = a.team_id
            JOIN betting_odds bo ON m.match_id = bo.match_id
            WHERE m.match_status = 'completed'
            AND m.match_date >= ?
        '''
        
        params = [start_date]
        
        if league:
            query += ' AND m.league = ?'
            params.append(league)
        
        if sport:
            query += ' AND m.sport = ?'
            params.append(sport)
        
        query += ' ORDER BY m.match_date DESC'
        
        cursor.execute(query, params)
        matches = cursor.fetchall()
        conn.close()
        
        if not matches:
            return {"status": "No data available", "matches": 0}
        
        # Analyze moneyline accuracy
        favorite_wins = 0
        favorite_losses = 0
        underdog_wins = 0
        underdog_losses = 0
        
        results = []
        
        for match in matches:
            home_ml = match['home_moneyline']
            away_ml = match['away_moneyline']
            home_score = match['home_score']
            away_score = match['away_score']
            
            # Determine which side was favored (negative is favorite)
            if home_ml < away_ml:
                # Home is favorite
                if home_score > away_score:
                    favorite_wins += 1
                    result = "Favorite ✓"
                else:
                    favorite_losses += 1
                    result = "Upset ⚠"
            else:
                # Away is favorite
                if away_score > home_score:
                    favorite_wins += 1
                    result = "Favorite ✓"
                else:
                    favorite_losses += 1
                    result = "Upset ⚠"
            
            results.append({
                "match": f"{match['home_team']} vs {match['away_team']}",
                "date": match['match_date'],
                "score": f"{home_score}-{away_score}",
                "home_ml": home_ml,
                "away_ml": away_ml,
                "result": result
            })
        
        total_games = favorite_wins + favorite_losses
        win_rate = (favorite_wins / total_games * 100) if total_games > 0 else 0
        
        return {
            "period_days": days,
            "total_games": total_games,
            "favorite_wins": favorite_wins,
            "favorite_losses": favorite_losses,
            "favorite_win_rate": f"{win_rate:.1f}%",
            "upset_count": favorite_losses,
            "recent_matches": results[-10:]  # Last 10 matches
        }
    
    def analyze_spread_covering(self, league=None, sport=None, days=365):
        """
        Analyze spread covering patterns.
        Identifies teams/matchups where favorites consistently beat/miss spreads.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        start_date = (datetime.now() - timedelta(days=days)).isoformat()
        
        query = '''
            SELECT 
                m.match_id,
                h.name as home_team,
                a.name as away_team,
                m.home_score,
                m.away_score,
                bo.spread_home,
                bo.spread_away,
                m.match_date
            FROM matches m
            JOIN teams h ON m.home_team_id = h.team_id
            JOIN teams a ON m.away_team_id = a.team_id
            JOIN betting_odds bo ON m.match_id = bo.match_id
            WHERE m.match_status = 'completed'
            AND bo.spread_home IS NOT NULL
            AND m.match_date >= ?
        '''
        
        params = [start_date]
        
        if league:
            query += ' AND m.league = ?'
            params.append(league)
        
        if sport:
            query += ' AND m.sport = ?'
            params.append(sport)
        
        cursor.execute(query, params)
        matches = cursor.fetchall()
        conn.close()
        
        if not matches:
            return {"status": "No spread data available"}
        
        # Analyze spread covering
        home_covers = 0
        away_covers = 0
        home_fails = 0
        away_fails = 0
        
        spread_results = []
        
        for match in matches:
            home_score = match['home_score']
            away_score = match['away_score']
            spread_home = match['spread_home']
            
            # Point differential
            point_diff = home_score - away_score
            
            # Check if home team covered
            if spread_home >= 0:
                # Home is favored
                if point_diff > abs(spread_home):
                    home_covers += 1
                    cover = "Home Covers ✓"
                elif point_diff < abs(spread_home):
                    away_covers += 1
                    cover = "Away Covers ✓"
                else:
                    cover = "Push"
            else:
                # Away is favored
                if point_diff < spread_home:
                    away_covers += 1
                    cover = "Away Covers ✓"
                elif point_diff > abs(spread_home):
                    home_covers += 1
                    cover = "Home Covers ✓"
                else:
                    cover = "Push"
            
            spread_results.append({
                "match": f"{match['home_team']} vs {match['away_team']}",
                "date": match['match_date'],
                "score": f"{home_score}-{away_score}",
                "spread": spread_home,
                "differential": point_diff,
                "cover": cover
            })
        
        total_covers = home_covers + away_covers
        
        return {
            "period_days": days,
            "total_games": len(matches),
            "home_covers": home_covers,
            "away_covers": away_covers,
            "pushes": len(matches) - home_covers - away_covers,
            "recent_results": spread_results[-10:]
        }
    
    def identify_line_movement_opportunities(self, league=None, days=30):
        """
        Identify games where opening lines moved significantly.
        Large moves may indicate sharp money or public betting patterns.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        start_date = (datetime.now() - timedelta(days=days)).isoformat()
        
        query = '''
            SELECT 
                m.match_id,
                h.name as home_team,
                a.name as away_team,
                bo.sportsbook,
                bo.home_moneyline,
                bo.odds_date
            FROM matches m
            JOIN teams h ON m.home_team_id = h.team_id
            JOIN teams a ON m.away_team_id = a.team_id
            JOIN betting_odds bo ON m.match_id = bo.match_id
            WHERE m.match_date >= ?
            AND bo.home_moneyline IS NOT NULL
        '''
        
        params = [start_date]
        
        if league:
            query += ' AND m.league = ?'
            params.append(league)
        
        query += ' ORDER BY m.match_id, bo.odds_date'
        
        cursor.execute(query, params)
        odds_history = cursor.fetchall()
        conn.close()
        
        # Group by match and analyze movement
        match_movements = defaultdict(lambda: {'lines': [], 'match_info': {}})
        
        for odd in odds_history:
            match_id = odd['match_id']
            match_movements[match_id]['lines'].append({
                'sportsbook': odd['sportsbook'],
                'ml': odd['home_moneyline'],
                'time': odd['odds_date']
            })
            if not match_movements[match_id]['match_info']:
                match_movements[match_id]['match_info'] = {
                    'home': odd['home_team'],
                    'away': odd['away_team']
                }
        
        significant_moves = []
        
        for match_id, data in match_movements.items():
            if len(data['lines']) >= 2:
                first_line = data['lines'][0]['ml']
                last_line = data['lines'][-1]['ml']
                movement = last_line - first_line
                
                # Flag if movement >= 20 cents (0.20)
                if abs(movement) >= 0.20:
                    significant_moves.append({
                        'match': f"{data['match_info']['home']} vs {data['match_info']['away']}",
                        'opening': first_line,
                        'closing': last_line,
                        'movement': f"{movement:+.2f}",
                        'direction': 'Home favored more' if movement < 0 else 'Away favored more'
                    })
        
        return {
            "period_days": days,
            "significant_moves": significant_moves,
            "moves_count": len(significant_moves)
        }
    
    def generate_summary_report(self, league, sport):
        """Generate comprehensive betting analysis summary."""
        print(f"\n{'='*60}")
        print(f"BETTING ANALYSIS REPORT - {league} {sport}")
        print(f"{'='*60}\n")
        
        # Moneyline accuracy
        print("📊 MONEYLINE ANALYSIS (Last 365 days)")
        print("-" * 60)
        ml_analysis = self.analyze_moneyline_accuracy(league=league, sport=sport, days=365)
        if 'total_games' in ml_analysis:
            print(f"Total games: {ml_analysis['total_games']}")
            print(f"Favorite win rate: {ml_analysis['favorite_win_rate']}")
            print(f"Upsets: {ml_analysis['upset_count']}")
            print()
        
        # Spread covering
        print("📈 SPREAD ANALYSIS (Last 365 days)")
        print("-" * 60)
        spread_analysis = self.analyze_spread_covering(league=league, sport=sport, days=365)
        if 'total_games' in spread_analysis:
            print(f"Total games with spreads: {spread_analysis['total_games']}")
            print(f"Home covers: {spread_analysis['home_covers']}")
            print(f"Away covers: {spread_analysis['away_covers']}")
            print()
        
        # Line movement
        print("💹 RECENT LINE MOVEMENT (Last 30 days)")
        print("-" * 60)
        movement_analysis = self.identify_line_movement_opportunities(league=league, days=30)
        print(f"Significant moves (>0.20): {movement_analysis['moves_count']}")
        
        return {
            'moneyline': ml_analysis,
            'spreads': spread_analysis,
            'line_movement': movement_analysis
        }


if __name__ == "__main__":
    analyzer = BettingAnalyzer()
    
    # Example analysis
    print("Betting Analysis Examples\n")
    
    # Analyze Serie A
    analyzer.generate_summary_report("Serie A", "Soccer")
    
    # Analyze NHL
    analyzer.generate_summary_report("NHL", "Hockey")
