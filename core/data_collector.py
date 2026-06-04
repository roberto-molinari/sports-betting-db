"""
Sports Data Collection Module
Fetches historical match data and odds from free/public APIs
"""

import requests
import time
from core.sports_db import (
    ensure_soccer_team, add_soccer_match, update_soccer_match_result,
    init_database
)
from core.nhl_results_sync import sync_many_nhl_seasons, sync_nhl_results


class SportDataCollector:
    """Collects sports data from various APIs."""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'SportsBettingDB/1.0'
        })
    
    def collect_serie_a_data(self, api_key, season=2024):
        """
        Collect Serie A soccer data using Football-Data.org API.
        Note: Free tier has limited historical data (current + 1 past season).
        """
        print(f"Collecting Serie A data for season {season}...")
        
        # Initialize database
        init_database()
        
        try:
            base_url = "https://api.football-data.org/v4"
            headers = {
                "X-Auth-Token": api_key
            }
            
            # Serie A competition code is SA (or ID 2019)
            serie_a_code = "SA"
            
            # Get Serie A teams
            print("Fetching Serie A teams...")
            teams_url = f"{base_url}/competitions/{serie_a_code}/teams"
            response = requests.get(teams_url, headers=headers)
            response.raise_for_status()
            teams_data = response.json()
            
            # Rate limiting: wait 6 seconds (10 requests/min = 1 request per 6 seconds)
            time.sleep(6)
            
            team_mapping = {}
            for team in teams_data['teams']:
                team_id = ensure_soccer_team(
                    name=team['name'],
                    league='Serie A',
                    country='Italy'
                )
                team_mapping[team['id']] = team_id
                print(f"  Added: {team['name']}")
            
            print(f"\n✓ Added {len(team_mapping)} Serie A teams")
            
            # Get matches
            print("\nFetching Serie A matches (rate limiting: 6 sec delay)...")
            matches_url = f"{base_url}/competitions/{serie_a_code}/matches"
            response = requests.get(matches_url, headers=headers)
            response.raise_for_status()
            matches_data = response.json()
            
            # Rate limiting applied
            time.sleep(1)  # Small delay after final request
            
            matches_added = 0
            for match in matches_data['matches']:
                # Skip if teams not in our mapping
                home_api_id = match['homeTeam']['id']
                away_api_id = match['awayTeam']['id']
                
                if home_api_id not in team_mapping or away_api_id not in team_mapping:
                    continue
                
                # Determine status
                status = 'completed' if match['status'] == 'FINISHED' else 'scheduled'
                
                # Add match
                match_id = add_soccer_match(
                    league='Serie A',
                    season=season,
                    home_team_id=team_mapping[home_api_id],
                    away_team_id=team_mapping[away_api_id],
                    match_date=match['utcDate'],
                    status=status
                )
                
                # Update result if finished
                if status == 'completed' and match['score']['fullTime']['home'] is not None:
                    ht = match['score'].get('halfTime', {})
                    update_soccer_match_result(
                        match_id,
                        home_score=match['score']['fullTime']['home'],
                        away_score=match['score']['fullTime']['away'],
                        halftime_home=ht.get('home'),
                        halftime_away=ht.get('away')
                    )
                
                matches_added += 1
            
            print(f"✓ Added {matches_added} Serie A matches")
            print("\n⚠️  Note: Football-Data.org does not provide betting odds.")
            print("   You'll need to add odds manually or use The Odds API.")
            
            return True
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                print("❌ API Key Error: Invalid or expired API key")
                print("   Get a new key at: https://www.football-data.org/client/register")
            else:
                print(f"❌ HTTP Error: {e}")
            return False
        except Exception as e:
            print(f"❌ Error collecting Serie A data: {e}")
            return False
    
    def collect_nhl_data(self, season=2024):
        """
        Collect NHL hockey data for a season.
        Uses shared sync logic also used by update_nhl_results.py.
        """
        print(f"Collecting NHL data for season {season}...")
        try:
            stats = sync_nhl_results(
                season,
                completed_only=False,
                initialize_db=True,
                verbose=True,
            )
            print(f"✓ Synced {stats['games_written']} unique NHL games")
            print(f"✓ Updated {stats['results_updated']} completed game results")
            print(
                "\n⚠️  Note: NHL API provides results/schedule data only; "
                "run import_nhl_odds.py separately for odds."
            )
            return True

        except Exception as e:
            print(f"Error collecting NHL data: {e}")
            return False
    
    def collect_nhl_historical_data(self, seasons=None):
        """
        Collect and insert NHL game results from historical seasons.
        By default fetches the past 2 complete seasons (2023-2024 and 2024-2025).
        
        Args:
            seasons (list): List of season strings in YYYYYYYY format
                           (e.g., ['20232024', '20242025']).
                           If None, defaults to past 2 seasons.
        
        Returns:
            bool: True if successful, False otherwise.
        """
        if seasons is None:
            # Default: fetch last 2 complete seasons
            # Current date is March 2026, so past 2 complete seasons are
            # 2023-2024 and 2024-2025
            seasons = ['20232024', '20242025']
        
        print(f"\nCollecting historical NHL data for seasons: {seasons}")

        try:
            season_years = [int(s[:4]) for s in seasons]
            stats = sync_many_nhl_seasons(
                season_years,
                completed_only=True,
                initialize_db=True,
                verbose=True,
            )
            print(f"\n✓ Seasons synced: {stats['seasons']}")
            print(f"✓ Completed games written: {stats['completed_written']}")
            print(f"✓ Results updated: {stats['results_updated']}")
            return True
        
        except Exception as e:
            print(f"❌ Error collecting historical NHL data: {e}")
            return False

if __name__ == "__main__":
    print("SportDataCollector — import and call collect_serie_a_data() or collect_nhl_historical_data().")
    print("See quickstart.py for usage examples.")
