"""
Sports Data Collection Module
Fetches historical match data and odds from free/public APIs
"""

import requests
import json
import time
from datetime import datetime, timedelta
from sports_db import (
    add_team, add_match, add_betting_odds, update_match_result,
    get_team_id, init_database
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
                team_id = add_team(
                    name=team['name'],
                    sport='Soccer',
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
                match_id = add_match(
                    sport='Soccer',
                    league='Serie A',
                    season=season,
                    home_team_id=team_mapping[home_api_id],
                    away_team_id=team_mapping[away_api_id],
                    match_date=match['utcDate'],
                    status=status
                )
                
                # Update result if finished
                if status == 'completed' and match['score']['fullTime']['home'] is not None:
                    update_match_result(
                        match_id,
                        home_score=match['score']['fullTime']['home'],
                        away_score=match['score']['fullTime']['away']
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
                    team_id = add_team(
                        name=t.get("name", ""),
                        sport="Hockey",
                        league="NHL",
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
                        match_id = add_match(
                            sport="Hockey",
                            league="NHL",
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
                            update_match_result(
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
                        team_id = add_team(
                            name=team.get("name"),
                            sport="Hockey",
                            league="NHL",
                            country="USA/Canada",
                        )
                        nhl_teams[team["id"]] = team_id

                print(f"Added {len(nhl_teams)} NHL teams to database")
                print("⚠️  Note: NHL API provides game results but limited betting odds.")
                return True

        except Exception as e:
            print(f"Error collecting NHL data: {e}")
            return False
    
    def add_sample_data(self):
        """
        Add sample data for demonstration and testing.
        This shows how to structure and populate the database.
        """
        print("\nAdding sample data for demonstration...")
        
        init_database()
        
        # Add Serie A teams
        serie_a_teams = [
            ('AC Milan', 'Soccer', 'Serie A', 'Italy'),
            ('Inter Milan', 'Soccer', 'Serie A', 'Italy'),
            ('Juventus', 'Soccer', 'Serie A', 'Italy'),
            ('AS Roma', 'Soccer', 'Serie A', 'Italy'),
            ('Napoli', 'Soccer', 'Serie A', 'Italy'),
            ('Lazio', 'Soccer', 'Serie A', 'Italy'),
            ('Atalanta', 'Soccer', 'Serie A', 'Italy'),
            ('Fiorentina', 'Soccer', 'Serie A', 'Italy'),
        ]
        
        serie_a_team_ids = {}
        for name, sport, league, country in serie_a_teams:
            team_id = add_team(name, sport, league, country)
            serie_a_team_ids[name] = team_id
        
        # Add NHL teams
        nhl_teams = [
            ('New York Rangers', 'Hockey', 'NHL', 'USA'),
            ('Boston Bruins', 'Hockey', 'NHL', 'USA'),
            ('Toronto Maple Leafs', 'Hockey', 'NHL', 'Canada'),
            ('Montreal Canadiens', 'Hockey', 'NHL', 'Canada'),
            ('Pittsburgh Penguins', 'Hockey', 'NHL', 'USA'),
            ('Philadelphia Flyers', 'Hockey', 'NHL', 'USA'),
            ('New Jersey Devils', 'Hockey', 'NHL', 'USA'),
            ('Washington Capitals', 'Hockey', 'NHL', 'USA'),
        ]
        
        nhl_team_ids = {}
        for name, sport, league, country in nhl_teams:
            team_id = add_team(name, sport, league, country)
            nhl_team_ids[name] = team_id
        
        # Sample Serie A match
        match_date = (datetime.now() - timedelta(days=30)).isoformat()
        match_id = add_match(
            'Soccer', 'Serie A', 2024,
            serie_a_team_ids['AC Milan'],
            serie_a_team_ids['Inter Milan'],
            match_date,
            status='completed'
        )
        
        # Add betting odds
        add_betting_odds(
            match_id=match_id,
            sportsbook='DraftKings',
            odds_date=match_date,
            home_moneyline=-120,
            away_moneyline=100,
            spread_home=-1.5,
            over_under=2.5,
            notes='Opening lines'
        )
        
        # Update with result
        update_match_result(match_id, home_score=2, away_score=1)
        
        # Sample NHL match
        match_date_nhl = (datetime.now() - timedelta(days=15)).isoformat()
        match_id_nhl = add_match(
            'Hockey', 'NHL', 2024,
            nhl_team_ids['New York Rangers'],
            nhl_team_ids['Boston Bruins'],
            match_date_nhl,
            status='completed'
        )
        
        add_betting_odds(
            match_id=match_id_nhl,
            sportsbook='FanDuel',
            odds_date=match_date_nhl,
            home_moneyline=-150,
            away_moneyline=130,
            spread_home=-1.5,
            over_under=6.5,
            notes='Opening lines'
        )
        
        update_match_result(match_id_nhl, home_score=3, away_score=2)
        
        print("✓ Sample data added successfully")


if __name__ == "__main__":
    collector = SportDataCollector()
    
    print("Sports Betting Database - Data Collection")
    print("=" * 50)
    
    # Add sample data for testing
    collector.add_sample_data()
    
    print("\n" + "=" * 50)
    print("To import real historical data:")
    print("\n1. For Serie A soccer:")
    print("   - Register at football-data.org for free API key")
    print("   - Or use api-football.com")
    print("\n2. For NHL hockey:")
    print("   - API available at statsapi.web.nhl.com (free, no auth)")
    print("\n3. For betting odds:")
    print("   - The Odds API (https://theoddsapi.com/)")
    print("   - Sports-Reference historical database")
    print("   - Covers.com (requires web scraping)")
