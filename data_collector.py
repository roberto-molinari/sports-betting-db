"""
Sports Data Collection Module
Fetches historical match data and odds from free/public APIs
"""

import requests
import json
import time
from datetime import datetime, timedelta
from sports_db import (
    ensure_soccer_team, add_soccer_match, update_soccer_match_result,
    ensure_nhl_team, get_nhl_team_id, add_nhl_match, update_nhl_match_result,
    init_database
)

# optional NHL helper library for newer API endpoints
try:
    from nhlpy import NHLClient
except ImportError:  # package not installed, we'll fall back to raw HTTP
    NHLClient = None


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
        Collect NHL hockey data using the `nhlpy` helper package if available.
        The wrapper hides the details of the underlying NHL endpoints and
        provides higher‑level helpers for teams, schedules, stats, etc.

        If `nhlpy` isn't installed we fall back to the original direct HTTP
        requests against `statsapi.web.nhl.com` (which may be subject to DNS
        issues as you've experienced).
        """
        print(f"Collecting NHL data for season {season}...")

        # Make sure the database exists
        init_database()

        nhl_teams = {}
        try:
            if NHLClient is not None:
                # use nhlpy for all calls
                client = NHLClient()

                print("Fetching NHL teams via nhlpy...")
                teams_list = client.teams.teams()

                # teams_list is a simple list of team dicts
                for t in teams_list:
                    # name, abbreviation, maybe country
                    country = t.get("country", "USA/Canada")
                    team_id = ensure_nhl_team(
                        name=t.get("name", ""),
                        country=country,
                    )
                    # store by abbreviation so we can look teams up later
                    nhl_teams[t.get("abbr")] = team_id
                    print(f"  Added: {t.get('name')} ({t.get('abbr')})")

                print(f"\n✓ Added {len(nhl_teams)} NHL teams to database")

                # optionally pull schedule/games for the season; nhlpy exposes
                # several helper methods, e.g. calendar_schedule() for a given
                # date or team_season_schedule(team_abbr, season_str).
                # We'll fetch the full season schedule for each team and insert
                # matches/results where available, similar to the old code.

                season_str = f"{season}{season+1:04d}"
                print(f"Fetching season schedules ({season_str})...")

                games_added = 0
                for abbr, team_db_id in nhl_teams.items():
                    sched = client.schedule.team_season_schedule(
                        team_abbr=abbr, season=season_str
                    )

                    # sched is a dict containing a 'games' list
                    for game in sched.get("games", []):
                        home = game.get("homeTeam")
                        away = game.get("awayTeam")
                        # map abbreviation to our database id
                        home_id = nhl_teams.get(home.get("abbr"))
                        away_id = nhl_teams.get(away.get("abbr"))
                        if not home_id or not away_id:
                            continue

                        status = "completed" if game.get("gameState") in [
                            "FINAL", "LIVE"
                        ] else "scheduled"

                        match_date = game.get("startTimeUTC")
                        match_id = add_nhl_match(
                            season=season,
                            home_team_id=home_id,
                            away_team_id=away_id,
                            match_date=match_date,
                            status=status,
                        )

                        # update result if finished
                        if status == "completed":
                            home_score = home.get("score", 0)
                            away_score = away.get("score", 0)
                            update_nhl_match_result(
                                match_id, home_score=home_score, away_score=away_score
                            )

                        games_added += 1

                print(f"✓ Added {games_added} NHL games from schedules")
                print(
                    "\n⚠️  Note: nhlpy (and the NHL API) still do not provide "
                    "betting odds.  You'll need a separate source for lines."
                )

                return True

            else:
                # fallback to raw HTTP call if nhlpy not installed
                print("nhlpy not available; falling back to direct NHL API")
                base_url = "https://statsapi.web.nhl.com/api/v1"

                teams_url = f"{base_url}/teams"
                response = requests.get(teams_url)
                response.raise_for_status()
                teams_data = response.json()

                for team in teams_data["teams"]:
                    if team.get("active"):
                        team_id = ensure_nhl_team(
                            name=team.get("name"),
                            country="USA/Canada",
                        )
                        nhl_teams[team["id"]] = team_id

                print(f"Added {len(nhl_teams)} NHL teams to database")
                print("⚠️  Note: NHL API provides game results but limited betting odds.")
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
        init_database()
        
        try:
            if NHLClient is None:
                print("⚠️  nhlpy not available; cannot collect historical data")
                return False
            
            client = NHLClient()
            
            # Fetch all teams once (reuse across seasons)
            print("Fetching NHL teams...")
            teams_list = client.teams.teams()
            nhl_teams_by_abbr = {}
            
            for t in teams_list:
                abbr = t.get('abbr')
                name = t.get('name', '')
                country = t.get('country', 'USA/Canada')
                
                # Try to get existing team or add new one
                team_id = get_nhl_team_id(name)
                if team_id is None:
                    team_id = ensure_nhl_team(
                        name=name,
                        country=country
                    )
                    print(f"  Added: {name}")
                else:
                    print(f"  Found existing: {name}")
                
                nhl_teams_by_abbr[abbr] = team_id
            
            print(f"✓ Total {len(nhl_teams_by_abbr)} NHL teams in database\n")
            
            # For each season, fetch all team schedules
            total_games_added = 0
            for season_str in seasons:
                print(f"Fetching schedules for season {season_str}...")
                season_int = int(season_str[:4])
                games_this_season = 0
                
                # Fetch schedule for each team
                for abbr, team_id in nhl_teams_by_abbr.items():
                    try:
                        sched = client.schedule.team_season_schedule(
                            team_abbr=abbr, season=season_str
                        )
                        
                        for game in sched.get('games', []):
                            # Only insert completed games with results
                            game_state = game.get('gameState', '')
                            if game_state not in ['FINAL', 'OFF']:
                                # Skip unfinished games
                                continue
                            
                            home_team = game.get('homeTeam', {})
                            away_team = game.get('awayTeam', {})
                            
                            home_abbr = home_team.get('abbrev')
                            away_abbr = away_team.get('abbrev')
                            
                            home_id = nhl_teams_by_abbr.get(home_abbr)
                            away_id = nhl_teams_by_abbr.get(away_abbr)
                            
                            if not home_id or not away_id:
                                continue
                            
                            match_date = game.get('startTimeUTC')
                            home_score = home_team.get('score')
                            away_score = away_team.get('score')
                            
                            # Skip if scores are missing
                            if home_score is None or away_score is None:
                                continue
                            
                            try:
                                match_id = add_nhl_match(
                                    season=season_int,
                                    home_team_id=home_id,
                                    away_team_id=away_id,
                                    match_date=match_date,
                                    status='completed'
                                )
                                
                                update_nhl_match_result(
                                    match_id,
                                    home_score=home_score,
                                    away_score=away_score
                                )
                                
                                games_this_season += 1
                            except Exception as e:
                                # Handle duplicate matches gracefully
                                if 'UNIQUE constraint failed' in str(e):
                                    pass  # Already in database
                                else:
                                    print(f"  Error adding match: {e}")
                    
                    except Exception as e:
                        print(f"  Error fetching schedule for {abbr}: {e}")
                
                print(f"  ✓ Added {games_this_season} games for season {season_str}")
                total_games_added += games_this_season
            
            print(f"\n✓ Total games added: {total_games_added}")
            return True
        
        except Exception as e:
            print(f"❌ Error collecting historical NHL data: {e}")
            return False

if __name__ == "__main__":
    print("SportDataCollector — import and call collect_serie_a_data() or collect_nhl_historical_data().")
    print("See quickstart.py for usage examples.")
