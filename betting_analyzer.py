"""
Betting Analysis Module
Analyzes historical data to identify market inefficiencies and betting opportunities.
Uses sport-specific tables: soccer_* or nhl_*
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
