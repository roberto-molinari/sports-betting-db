"""
Betting Analysis Module
Analyzes historical data to identify market inefficiencies and betting opportunities.
Uses sport-specific tables: soccer_* or nhl_*
"""

import sqlite3
from datetime import datetime, timedelta
from core.sports_db import DATABASE_PATH
from collections import defaultdict
import statistics


class BettingAnalyzer:
    """Analyzes betting data and match results to find opportunities."""

    def __init__(self):
        self.db_path = DATABASE_PATH

    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _tables(sport):
        """Return (matches_tbl, teams_tbl, odds_tbl) for the given sport string."""
        if (sport or '').lower() in ('soccer', 'football'):
            return 'soccer_matches', 'soccer_teams', 'soccer_betting_odds'
        return 'nhl_matches', 'nhl_teams', 'nhl_betting_odds'

    def analyze_moneyline_accuracy(self, sport='hockey', league=None, days=365):
        """
        Analyze how accurate opening moneylines were at predicting outcomes.
        sport: 'soccer' or 'hockey'
        league: optional filter (e.g. 'Serie A'); only applies to soccer
        """
        mt, tt, ot = self._tables(sport)
        conn = self.get_connection()
        cursor = conn.cursor()

        start_date = (datetime.now() - timedelta(days=days)).isoformat()

        query = f'''
            SELECT
                m.match_id,
                h.name as home_team,
                a.name as away_team,
                m.home_score,
                m.away_score,
                bo.home_moneyline,
                bo.away_moneyline,
                m.match_date
            FROM {mt} m
            JOIN {tt} h ON m.home_team_id = h.team_id
            JOIN {tt} a ON m.away_team_id = a.team_id
            JOIN {ot} bo ON m.match_id = bo.match_id
            WHERE m.match_status = 'completed'
            AND bo.home_moneyline IS NOT NULL
            AND bo.away_moneyline IS NOT NULL
            AND m.match_date >= ?
        '''
        params = [start_date]

        if league and mt == 'soccer_matches':
            query += ' AND m.league = ?'
            params.append(league)

        query += ' ORDER BY m.match_date DESC'
        cursor.execute(query, params)
        matches = cursor.fetchall()
        conn.close()

        if not matches:
            return {"status": "No data available", "matches": 0}

        favorite_wins = 0
        favorite_losses = 0
        results = []

        for match in matches:
            home_ml = match['home_moneyline']
            away_ml = match['away_moneyline']
            home_score = match['home_score']
            away_score = match['away_score']

            if home_ml < away_ml:
                if home_score > away_score:
                    favorite_wins += 1
                    result = "Favorite"
                else:
                    favorite_losses += 1
                    result = "Upset"
            else:
                if away_score > home_score:
                    favorite_wins += 1
                    result = "Favorite"
                else:
                    favorite_losses += 1
                    result = "Upset"

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
            "recent_matches": results[-10:]
        }

    def analyze_spread_covering(self, sport='hockey', league=None, days=365):
        """Analyze spread covering patterns."""
        mt, tt, ot = self._tables(sport)
        conn = self.get_connection()
        cursor = conn.cursor()

        start_date = (datetime.now() - timedelta(days=days)).isoformat()

        query = f'''
            SELECT
                m.match_id,
                h.name as home_team,
                a.name as away_team,
                m.home_score,
                m.away_score,
                bo.spread_home,
                bo.spread_away,
                m.match_date
            FROM {mt} m
            JOIN {tt} h ON m.home_team_id = h.team_id
            JOIN {tt} a ON m.away_team_id = a.team_id
            JOIN {ot} bo ON m.match_id = bo.match_id
            WHERE m.match_status = 'completed'
            AND bo.spread_home IS NOT NULL
            AND m.match_date >= ?
        '''
        params = [start_date]

        if league and mt == 'soccer_matches':
            query += ' AND m.league = ?'
            params.append(league)

        cursor.execute(query, params)
        matches = cursor.fetchall()
        conn.close()

        if not matches:
            return {"status": "No spread data available"}

        home_covers = 0
        away_covers = 0
        spread_results = []

        for match in matches:
            home_score = match['home_score']
            away_score = match['away_score']
            spread_home = match['spread_home']
            point_diff = home_score - away_score

            if spread_home >= 0:
                if point_diff > abs(spread_home):
                    home_covers += 1
                    cover = "Home Covers"
                elif point_diff < abs(spread_home):
                    away_covers += 1
                    cover = "Away Covers"
                else:
                    cover = "Push"
            else:
                if point_diff < spread_home:
                    away_covers += 1
                    cover = "Away Covers"
                elif point_diff > abs(spread_home):
                    home_covers += 1
                    cover = "Home Covers"
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

        return {
            "period_days": days,
            "total_games": len(matches),
            "home_covers": home_covers,
            "away_covers": away_covers,
            "pushes": len(matches) - home_covers - away_covers,
            "recent_results": spread_results[-10:]
        }

    def identify_line_movement_opportunities(self, sport='hockey', league=None, days=30):
        """Identify games where opening lines moved significantly."""
        mt, tt, ot = self._tables(sport)
        conn = self.get_connection()
        cursor = conn.cursor()

        start_date = (datetime.now() - timedelta(days=days)).isoformat()

        query = f'''
            SELECT
                m.match_id,
                h.name as home_team,
                a.name as away_team,
                bo.sportsbook,
                bo.home_moneyline,
                bo.odds_date
            FROM {mt} m
            JOIN {tt} h ON m.home_team_id = h.team_id
            JOIN {tt} a ON m.away_team_id = a.team_id
            JOIN {ot} bo ON m.match_id = bo.match_id
            WHERE m.match_date >= ?
            AND bo.home_moneyline IS NOT NULL
        '''
        params = [start_date]

        if league and mt == 'soccer_matches':
            query += ' AND m.league = ?'
            params.append(league)

        query += ' ORDER BY m.match_id, bo.odds_date'
        cursor.execute(query, params)
        odds_history = cursor.fetchall()
        conn.close()

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

    def analyze_team_performance_vs_odds(self, sport='soccer', league=None):
        """Analyze which teams beat the odds most consistently."""
        mt, tt, ot = self._tables(sport)
        conn = self.get_connection()
        cursor = conn.cursor()

        where = (
            "WHERE m.match_status = 'completed' "
            "AND m.home_score IS NOT NULL "
            "AND bo.home_moneyline IS NOT NULL "
            "AND bo.away_moneyline IS NOT NULL"
        )
        params = []
        if league and mt == 'soccer_matches':
            where += ' AND m.league = ?'
            params.append(league)

        query = f'''
            SELECT
                h.name as home_team,
                a.name as away_team,
                m.home_score,
                m.away_score,
                bo.home_moneyline,
                bo.away_moneyline
            FROM {mt} m
            JOIN {tt} h ON m.home_team_id = h.team_id
            JOIN {tt} a ON m.away_team_id = a.team_id
            JOIN {ot} bo ON m.match_id = bo.match_id
            {where}
        '''

        cursor.execute(query, params)
        matches = cursor.fetchall()
        conn.close()

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

            home_is_favorite = home_ml < away_ml

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

        results = []
        for team, stats in team_stats.items():
            total_games = stats['home_wins'] + stats['home_losses'] + stats['away_wins'] + stats['away_losses']
            total_wins = stats['home_wins'] + stats['away_wins']
            total_upsets = stats['home_upset_wins'] + stats['away_upset_wins']

            total_as_favorite = stats['home_as_favorite'] + stats['away_as_favorite']
            if total_as_favorite > 0:
                favorite_wr = (stats['home_favorite_wins'] + stats['away_favorite_wins']) / total_as_favorite
            else:
                favorite_wr = 0

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

        results.sort(key=lambda x: x['upset_wins'], reverse=True)
        return results

    def analyze_favorite_value(self, sport='soccer', league=None):
        """Analyze if favorites are overvalued or undervalued."""
        mt, _, ot = self._tables(sport)
        conn = self.get_connection()
        cursor = conn.cursor()

        where = (
            "WHERE m.match_status = 'completed' "
            "AND m.home_score IS NOT NULL "
            "AND bo.home_moneyline IS NOT NULL "
            "AND bo.away_moneyline IS NOT NULL"
        )
        params = []
        if league and mt == 'soccer_matches':
            where += ' AND m.league = ?'
            params.append(league)

        query = f'''
            SELECT
                bo.home_moneyline,
                bo.away_moneyline,
                m.home_score,
                m.away_score
            FROM {mt} m
            JOIN {ot} bo ON m.match_id = bo.match_id
            {where}
        '''

        cursor.execute(query, params)
        matches = cursor.fetchall()
        conn.close()

        odds_buckets = defaultdict(lambda: {'wins': 0, 'losses': 0})

        for match in matches:
            home_ml = match['home_moneyline']
            away_ml = match['away_moneyline']
            home_score = match['home_score']
            away_score = match['away_score']

            if abs(home_ml) < abs(away_ml):
                favorite_ml = home_ml
                favorite_won = home_score > away_score
            else:
                favorite_ml = away_ml
                favorite_won = away_score > home_score

            bucket = round(abs(favorite_ml) / 50) * 50
            if favorite_won:
                odds_buckets[bucket]['wins'] += 1
            else:
                odds_buckets[bucket]['losses'] += 1

        results = []
        for odds, stats in sorted(odds_buckets.items()):
            total = stats['wins'] + stats['losses']
            wr = stats['wins'] / total if total > 0 else 0

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

    def analyze_upset_patterns(self, sport='soccer', league=None):
        """Analyze what predicts upsets."""
        mt, tt, ot = self._tables(sport)
        conn = self.get_connection()
        cursor = conn.cursor()

        where = (
            "WHERE m.match_status = 'completed' "
            "AND m.home_score IS NOT NULL "
            "AND bo.home_moneyline IS NOT NULL "
            "AND bo.away_moneyline IS NOT NULL"
        )
        params = []
        if league and mt == 'soccer_matches':
            where += ' AND m.league = ?'
            params.append(league)

        query = f'''
            SELECT
                h.name as home_team,
                a.name as away_team,
                m.home_score,
                m.away_score,
                bo.home_moneyline,
                bo.away_moneyline
            FROM {mt} m
            JOIN {tt} h ON m.home_team_id = h.team_id
            JOIN {tt} a ON m.away_team_id = a.team_id
            JOIN {ot} bo ON m.match_id = bo.match_id
            {where}
        '''

        cursor.execute(query, params)
        matches = cursor.fetchall()
        conn.close()

        upsets = []
        favorites = []

        for match in matches:
            home_ml = match['home_moneyline']
            away_ml = match['away_moneyline']
            home_score = match['home_score']
            away_score = match['away_score']

            if home_ml < away_ml:
                favorite_ml = home_ml
                is_upset = away_score > home_score
                upset_odds = abs(away_ml)
            else:
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

            favorites.append({'is_upset': is_upset, 'favorite_odds': abs(favorite_ml)})

        upset_count = len(upsets)
        total_matches = len(favorites)
        upset_rate = upset_count / total_matches if total_matches > 0 else 0

        avg_favorite_odds = statistics.mean([u['favorite_odds'] for u in upsets]) if upsets else 0
        avg_upset_odds = statistics.mean([u['upset_odds'] for u in upsets]) if upsets else 0
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

    def generate_advanced_report(self, sport='soccer', league='Serie A'):
        """Generate advanced report previously provided by analyze_serie_a_advanced.py."""
        label = league or sport.upper()
        print("=" * 80)
        print(f"  ADVANCED BETTING ANALYSIS - {label}")
        print("=" * 80)

        print("\nWHICH TEAMS BEAT THE ODDS?\n")
        print("-" * 80)
        teams = self.analyze_team_performance_vs_odds(sport=sport, league=league)

        print(f"{'Team':<30} {'Games':>6} {'W-L':>8} {'Overall':>8} {'As Fav':>8} {'As Dog':>8}")
        print("-" * 80)
        for team in teams[:15]:
            print(f"{team['team']:<30} {team['games']:>6} "
                  f"{team['wins']}-{team['games']-team['wins']:>5} "
                  f"{team['overall_wr']:.1%}{'':<3} "
                  f"{team['favorite_wr']:.1%}{'':<3} "
                  f"{team['underdog_wr']:.1%}")

        print("\n" + "=" * 80)
        print("  ARE FAVORITES OVERVALUED OR UNDERVALUED?")
        print("=" * 80 + "\n")
        print("-" * 80)

        fav_analysis = self.analyze_favorite_value(sport=sport, league=league)
        print(f"{'Odds Range':<20} {'Implied %':>12} {'Actual %':>12} {'Games':>8} {'Value':>8}")
        print("-" * 80)
        for odds in fav_analysis:
            print(f"{odds['odds_range']:<20} {odds['implied_prob']:.1%}{'':<8} "
                  f"{odds['actual_wr']:.1%}{'':<8} {odds['games']:>8} {odds['value']:>8}")

        print("\n" + "=" * 80)
        print("  UPSET PATTERNS")
        print("=" * 80 + "\n")
        upset_data = self.analyze_upset_patterns(sport=sport, league=league)

        print(f"Total matches: {upset_data['total_matches']}")
        print(f"Total upsets: {upset_data['upsets']} ({upset_data['upset_rate']:.1%})")
        print(f"Heavy upsets (>300 odds): {upset_data['heavy_upsets']}")
        print(f"\nAverage odds of favorites in upsets: {upset_data['avg_favorite_odds']:.0f}")
        print(f"Average odds of underdogs who pulled upset: {upset_data['avg_upset_odds']:.0f}")

        print("\nBIGGEST UPSETS:\n")
        print("-" * 80)
        for i, upset in enumerate(upset_data['biggest_upsets'], 1):
            print(f"{i}. {upset['home']} vs {upset['away']} ({upset['score']})")
            print(f"   Favorite: {upset['favorite_odds']:.0f} | Underdog: {upset['upset_odds']:.0f}")

        print("\n" + "=" * 80)
        print("  ANALYSIS COMPLETE")
        print("=" * 80)

    def generate_summary_report(self, sport, league=None):
        """Generate comprehensive betting analysis summary."""
        label = league or sport.upper()
        print(f"\n{'='*60}")
        print(f"BETTING ANALYSIS REPORT - {label}")
        print(f"{'='*60}\n")

        print("MONEYLINE ANALYSIS (Last 365 days)")
        print("-" * 60)
        ml = self.analyze_moneyline_accuracy(sport=sport, league=league, days=365)
        if 'total_games' in ml:
            print(f"Total games: {ml['total_games']}")
            print(f"Favorite win rate: {ml['favorite_win_rate']}")
            print(f"Upsets: {ml['upset_count']}")
        print()

        print("SPREAD ANALYSIS (Last 365 days)")
        print("-" * 60)
        sp = self.analyze_spread_covering(sport=sport, league=league, days=365)
        if 'total_games' in sp:
            print(f"Total games with spreads: {sp['total_games']}")
            print(f"Home covers: {sp['home_covers']}")
            print(f"Away covers: {sp['away_covers']}")
        print()

        print("RECENT LINE MOVEMENT (Last 30 days)")
        print("-" * 60)
        lm = self.identify_line_movement_opportunities(sport=sport, league=league, days=30)
        print(f"Significant moves (>0.20): {lm['moves_count']}")

        return {'moneyline': ml, 'spreads': sp, 'line_movement': lm}


if __name__ == "__main__":
    analyzer = BettingAnalyzer()

    analyzer.generate_summary_report("soccer", league="Serie A")
    analyzer.generate_summary_report("hockey")
