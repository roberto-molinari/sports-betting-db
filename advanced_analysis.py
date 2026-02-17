#!/usr/bin/env python3
"""
Advanced Betting Analysis
Explores which teams beat the odds, favorite value, and upset patterns
"""

import sqlite3
from collections import defaultdict
from sports_db import DATABASE_PATH
import statistics


class AdvancedBettingAnalyzer:
    """Advanced analysis of betting patterns and team performance against odds."""
    
    def __init__(self):
        self.db_path = DATABASE_PATH
    
    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def analyze_team_performance_vs_odds(self, league='Serie A'):
        """Analyze which teams beat the odds most consistently."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        query = '''
            SELECT 
                h.name as home_team,
                a.name as away_team,
                m.home_score,
                m.away_score,
                bo.home_moneyline,
                bo.away_moneyline
            FROM matches m
            JOIN teams h ON m.home_team_id = h.team_id
            JOIN teams a ON m.away_team_id = a.team_id
            JOIN betting_odds bo ON m.match_id = bo.match_id
            WHERE m.league = ?
            AND m.match_status = 'completed'
            AND m.home_score IS NOT NULL
        '''
        
        cursor.execute(query, (league,))
        matches = cursor.fetchall()
        conn.close()
        
        # Analyze performance by team
        team_stats = defaultdict(lambda: {
            'home_wins': 0,
            'home_losses': 0,
            'home_as_favorite': 0,
            'home_favorite_wins': 0,
            'away_wins': 0,
            'away_losses': 0,
            'away_as_favorite': 0,
            'away_favorite_wins': 0,
            'home_upset_wins': 0,
            'away_upset_wins': 0,
        })
        
        for match in matches:
            home = match['home_team']
            away = match['away_team']
            home_score = match['home_score']
            away_score = match['away_score']
            home_ml = match['home_moneyline']
            away_ml = match['away_moneyline']
            
            # Determine favorites
            home_is_favorite = home_ml < away_ml
            
            # Home team results
            if home_score > away_score:
                team_stats[home]['home_wins'] += 1
                if home_is_favorite:
                    team_stats[home]['home_favorite_wins'] += 1
                else:
                    team_stats[home]['home_upset_wins'] += 1
            else:
                team_stats[home]['home_losses'] += 1
            
            if home_is_favorite:
                team_stats[home]['home_as_favorite'] += 1
            
            # Away team results
            if away_score > home_score:
                team_stats[away]['away_wins'] += 1
                if not home_is_favorite:
                    team_stats[away]['away_favorite_wins'] += 1
                else:
                    team_stats[away]['away_upset_wins'] += 1
            else:
                team_stats[away]['away_losses'] += 1
            
            if not home_is_favorite:
                team_stats[away]['away_as_favorite'] += 1
        
        # Calculate win rates
        results = []
        for team, stats in team_stats.items():
            total_games = stats['home_wins'] + stats['home_losses'] + stats['away_wins'] + stats['away_losses']
            total_wins = stats['home_wins'] + stats['away_wins']
            total_upsets = stats['home_upset_wins'] + stats['away_upset_wins']
            
            # Favorite performance
            total_as_favorite = stats['home_as_favorite'] + stats['away_as_favorite']
            if total_as_favorite > 0:
                favorite_wr = (stats['home_favorite_wins'] + stats['away_favorite_wins']) / total_as_favorite
            else:
                favorite_wr = 0
            
            # Underdog performance
            total_as_underdog = total_games - total_as_favorite
            if total_as_underdog > 0:
                underdog_wr = total_upsets / total_as_underdog
            else:
                underdog_wr = 0
            
            results.append({
                'team': team,
                'games': total_games,
                'wins': total_wins,
                'overall_wr': total_wins / total_games if total_games > 0 else 0,
                'favorite_wr': favorite_wr,
                'underdog_wr': underdog_wr,
                'upset_wins': total_upsets,
                'home_record': f"{stats['home_wins']}-{stats['home_losses']}",
                'away_record': f"{stats['away_wins']}-{stats['away_losses']}",
            })
        
        # Sort by upset wins
        results.sort(key=lambda x: x['upset_wins'], reverse=True)
        return results
    
    def analyze_favorite_value(self, league='Serie A'):
        """Analyze if favorites are overvalued or undervalued."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        query = '''
            SELECT 
                bo.home_moneyline,
                bo.away_moneyline,
                m.home_score,
                m.away_score
            FROM matches m
            JOIN betting_odds bo ON m.match_id = bo.match_id
            WHERE m.league = ?
            AND m.match_status = 'completed'
            AND m.home_score IS NOT NULL
        '''
        
        cursor.execute(query, (league,))
        matches = cursor.fetchall()
        conn.close()
        
        # Group by favorite odds
        odds_buckets = defaultdict(lambda: {
            'wins': 0,
            'losses': 0,
        })
        
        for match in matches:
            home_ml = match['home_moneyline']
            away_ml = match['away_moneyline']
            home_score = match['home_score']
            away_score = match['away_score']
            
            # Determine favorite and bucket
            if abs(home_ml) < abs(away_ml):
                # Home is favorite
                favorite_ml = home_ml
                favorite_won = home_score > away_score
            else:
                # Away is favorite
                favorite_ml = away_ml
                favorite_won = away_score > home_score
            
            # Round to nearest 50 for bucketing
            bucket = round(abs(favorite_ml) / 50) * 50
            
            if favorite_won:
                odds_buckets[bucket]['wins'] += 1
            else:
                odds_buckets[bucket]['losses'] += 1
        
        # Calculate win rates by odds level
        results = []
        for odds, stats in sorted(odds_buckets.items()):
            total = stats['wins'] + stats['losses']
            wr = stats['wins'] / total if total > 0 else 0
            
            # Calculate implied probability from moneyline
            # For negative odds: implied% = abs(ML) / (abs(ML) + 100)
            if odds > 0:
                implied = 100 / (odds + 100)
            else:
                implied = abs(odds) / (abs(odds) + 100)
            
            results.append({
                'odds_range': f"-{odds} to -{odds+50}",
                'implied_prob': implied,
                'actual_wr': wr,
                'games': total,
                'wins': stats['wins'],
                'losses': stats['losses'],
                'value': 'GOOD' if wr > implied else 'POOR'
            })
        
        return results
    
    def analyze_upset_patterns(self, league='Serie A'):
        """Analyze what predicts upsets."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        query = '''
            SELECT 
                h.name as home_team,
                a.name as away_team,
                m.home_score,
                m.away_score,
                bo.home_moneyline,
                bo.away_moneyline
            FROM matches m
            JOIN teams h ON m.home_team_id = h.team_id
            JOIN teams a ON m.away_team_id = a.team_id
            JOIN betting_odds bo ON m.match_id = bo.match_id
            WHERE m.league = ?
            AND m.match_status = 'completed'
            AND m.home_score IS NOT NULL
        '''
        
        cursor.execute(query, (league,))
        matches = cursor.fetchall()
        conn.close()
        
        upsets = []
        favorites = []
        
        for match in matches:
            home_ml = match['home_moneyline']
            away_ml = match['away_moneyline']
            home_score = match['home_score']
            away_score = match['away_score']
            
            # Determine favorite and result
            if home_ml < away_ml:
                # Home is favorite
                favorite_ml = home_ml
                is_upset = away_score > home_score
                upset_odds = abs(away_ml)
            else:
                # Away is favorite
                favorite_ml = away_ml
                is_upset = home_score > away_score
                upset_odds = abs(home_ml)
            
            if is_upset:
                upsets.append({
                    'home': match['home_team'],
                    'away': match['away_team'],
                    'score': f"{home_score}-{away_score}",
                    'favorite_odds': abs(favorite_ml),
                    'upset_odds': upset_odds,
                    'odds_ratio': upset_odds / abs(favorite_ml) if favorite_ml != 0 else 0,
                })
            
            favorites.append({
                'is_upset': is_upset,
                'favorite_odds': abs(favorite_ml),
                'score_diff': abs(home_score - away_score),
            })
        
        # Summary statistics
        upset_count = len(upsets)
        total_matches = len(favorites)
        upset_rate = upset_count / total_matches if total_matches > 0 else 0
        
        # Average odds of upsets
        avg_favorite_odds = statistics.mean([u['favorite_odds'] for u in upsets]) if upsets else 0
        avg_upset_odds = statistics.mean([u['upset_odds'] for u in upsets]) if upsets else 0
        
        # Upsets where underdog was heavy (>300 odds)
        heavy_upsets = len([u for u in upsets if u['upset_odds'] > 300])
        
        return {
            'total_matches': total_matches,
            'upsets': upset_count,
            'upset_rate': upset_rate,
            'heavy_upsets': heavy_upsets,
            'avg_favorite_odds': avg_favorite_odds,
            'avg_upset_odds': avg_upset_odds,
            'biggest_upsets': sorted(upsets, key=lambda x: x['upset_odds'], reverse=True)[:5],
        }


def main():
    print("="*80)
    print("  ADVANCED BETTING ANALYSIS - SERIE A")
    print("="*80)
    
    analyzer = AdvancedBettingAnalyzer()
    
    # Analysis 1: Team performance vs odds
    print("\n📊 WHICH TEAMS BEAT THE ODDS?\n")
    print("-" * 80)
    teams = analyzer.analyze_team_performance_vs_odds('Serie A')
    
    print(f"{'Team':<30} {'Games':>6} {'W-L':>8} {'Overall':>8} {'As Fav':>8} {'As Dog':>8}")
    print("-" * 80)
    
    for team in teams[:15]:  # Top 15
        print(f"{team['team']:<30} {team['games']:>6} "
              f"{team['wins']}-{team['games']-team['wins']:>5} "
              f"{team['overall_wr']:.1%}{'':<3} "
              f"{team['favorite_wr']:.1%}{'':<3} "
              f"{team['underdog_wr']:.1%}")
    
    print("\n💡 KEY INSIGHTS:")
    best_overall = max(teams, key=lambda x: x['overall_wr'])
    best_favorite = max(teams, key=lambda x: x['favorite_wr'])
    best_underdog = max(teams, key=lambda x: x['underdog_wr'])
    
    print(f"  • Best overall: {best_overall['team']} ({best_overall['overall_wr']:.1%})")
    print(f"  • Best as favorite: {best_favorite['team']} ({best_favorite['favorite_wr']:.1%} win rate)")
    print(f"  • Best as underdog: {best_underdog['team']} ({best_underdog['underdog_wr']:.1%} upset wins)")
    
    # Analysis 2: Favorite value
    print("\n" + "="*80)
    print("  ARE FAVORITES OVERVALUED OR UNDERVALUED?")
    print("="*80 + "\n")
    print("-" * 80)
    
    fav_analysis = analyzer.analyze_favorite_value('Serie A')
    
    print(f"{'Odds Range':<20} {'Implied %':>12} {'Actual %':>12} {'Games':>8} {'Value':>8}")
    print("-" * 80)
    
    for odds in fav_analysis:
        print(f"{odds['odds_range']:<20} {odds['implied_prob']:.1%}{'':<8} "
              f"{odds['actual_wr']:.1%}{'':<8} {odds['games']:>8} {odds['value']:>8}")
    
    print("\n💡 INTERPRETATION:")
    print("  If Actual % > Implied %: Favorites are UNDERVALUED (good bet)")
    print("  If Actual % < Implied %: Favorites are OVERVALUED (bad bet)")
    
    # Analysis 3: Upset patterns
    print("\n" + "="*80)
    print("  UPSET PATTERNS")
    print("="*80 + "\n")
    
    upset_data = analyzer.analyze_upset_patterns('Serie A')
    
    print(f"Total matches: {upset_data['total_matches']}")
    print(f"Total upsets: {upset_data['upsets']} ({upset_data['upset_rate']:.1%})")
    print(f"Heavy upsets (>300 odds): {upset_data['heavy_upsets']}")
    print(f"\nAverage odds of favorites in upsets: {upset_data['avg_favorite_odds']:.0f}")
    print(f"Average odds of underdogs who pulled upset: {upset_data['avg_upset_odds']:.0f}")
    
    print("\n🏆 BIGGEST UPSETS:\n")
    print("-" * 80)
    for i, upset in enumerate(upset_data['biggest_upsets'], 1):
        print(f"{i}. {upset['home']} vs {upset['away']} ({upset['score']})")
        print(f"   Favorite: {upset['favorite_odds']:.0f} | Underdog: {upset['upset_odds']:.0f}")
    
    print("\n" + "="*80)
    print("  ANALYSIS COMPLETE")
    print("="*80)


if __name__ == "__main__":
    main()
