# Sports Betting Database System

A comprehensive system for tracking and analyzing Serie A (soccer) and NHL (hockey) match results with historical betting odds to identify market inefficiencies and betting opportunities.

## System Overview

This system consists of three main components:

1. **sports_db.py** - Database schema and core functions
2. **data_collector.py** - Data collection from sports APIs
3. **betting_analyzer.py** - Analysis and opportunity identification

## Database Schema

### Tables

- **teams**: All teams with metadata (sport, league, country)
- **matches**: Match/game records with results
- **betting_odds**: Historical opening lines from sportsbooks
- **match_outcomes**: Summarized match results for analysis
- **betting_analysis**: Patterns and inefficiencies

### Key Relationships

```
teams
  ├── matches (home_team_id, away_team_id)
  │   ├── betting_odds
  │   └── match_outcomes
  └── betting_analysis
```

## Getting Started

### 1. Initial Setup

```python
from sports_db import init_database

# Initialize the database
init_database()
```

This creates a SQLite database file: `sports_betting.db`

### 2. Add Sample Data

```python
from data_collector import SportDataCollector

collector = SportDataCollector()
collector.add_sample_data()
```

This adds sample Serie A and NHL teams and matches for testing.

### 3. Import Historical Data

#### Option A: Use Football-Data.org (Serie A)

1. Register for free at https://www.football-data.org/
2. Get your free API key
3. Use in data_collector.py:

```python
# Add to data_collector.py
headers = {
    "X-Auth-Token": "YOUR_API_KEY"
}
```

#### Option B: NHL API (Free, No Auth Required)

The repository now supports the `nhl-api-py` package (imported as `nhlpy`),
which wraps the official NHL web endpoints and handles rate‑limits and
pagination for you. Install it in your virtual environment:

```bash
pip install nhl-api-py
```

Once the package is available the collector will automatically use it when
fetching teams and schedules.  Usage remains the same:

```python
collector = SportDataCollector()
collector.collect_nhl_data(season=2024)
```

If `nhlpy` is not installed the code falls back to the older `requests`
implementation against `statsapi.web.nhl.com`.

##### Load Historical NHL Game Results

To build a database of past NHL games with scores (e.g., for analysis and backtesting),
use the dedicated historical data collection method:

```python
from data_collector import SportDataCollector

collector = SportDataCollector()

# Load the last 2 complete seasons (2023-2024 and 2024-2025)
# This inserts ~5,800 completed games with final scores
collector.collect_nhl_historical_data()

# Or specify custom seasons
collector.collect_nhl_historical_data(seasons=['20232024', '20242025', '20212022'])
```

**Note:** 
- Requires `nhl-api-py` (install with: `pip install nhl-api-py`)
- Seasons are specified in `YYYYYYYY` format (e.g., `20242025` for 2024–2025)
- Only completed games with final scores are inserted into the `matches` table
- Existing games are skipped gracefully, so you can safely re-run the method

#### Option C: Manual Data Import

You can also manually import historical data using CSV:

```python
from sports_db import add_team, add_match, add_betting_odds, update_match_result
import csv

# Read from CSV and populate database
with open('historical_matches.csv') as f:
    reader = csv.DictReader(f)
    for row in reader:
        # Parse and add to database
        pass
```

## Database Operations

### Adding Data

```python
from sports_db import add_team, add_match, add_betting_odds, update_match_result

# Add teams
home_team_id = add_team('AC Milan', 'Soccer', 'Serie A', 'Italy')
away_team_id = add_team('Inter Milan', 'Soccer', 'Serie A', 'Italy')

# Add match
match_id = add_match(
    sport='Soccer',
    league='Serie A',
    season=2024,
    home_team_id=home_team_id,
    away_team_id=away_team_id,
    match_date='2024-02-01T20:00:00',
    status='scheduled'
)

# Add betting odds
add_betting_odds(
    match_id=match_id,
    sportsbook='DraftKings',
    odds_date='2024-01-31T12:00:00',
    home_moneyline=-120,
    away_moneyline=100,
    spread_home=-1.5,
    over_under=2.5,
    notes='Opening lines'
)

# Update with final result
update_match_result(match_id, home_score=2, away_score=1)
```

### Querying Data

```python
from sports_db import get_matches_by_league_and_date, get_betting_odds_for_match, get_all_teams

# Get all Serie A teams
teams = get_all_teams(league='Serie A')

# Get matches within date range
matches = get_matches_by_league_and_date(
    league='Serie A',
    start_date='2024-01-01T00:00:00',
    end_date='2024-12-31T23:59:59'
)

# Get betting odds for specific match
odds = get_betting_odds_for_match(match_id=123)
```

## Betting Analysis

### Moneyline Accuracy

Analyze how often favorites won vs underdogs pulled upsets:

```python
from betting_analyzer import BettingAnalyzer

analyzer = BettingAnalyzer()
results = analyzer.analyze_moneyline_accuracy(
    league='Serie A',
    sport='Soccer',
    days=365  # Last 365 days
)

# Results include:
# - total_games: Number of matches analyzed
# - favorite_wins: Times favorite won
# - favorite_win_rate: Percentage
# - upset_count: Underdog wins
```

### Spread Covering Analysis

Identify if favorites consistently beat or miss their spreads:

```python
results = analyzer.analyze_spread_covering(
    league='NHL',
    sport='Hockey',
    days=365
)

# Results include:
# - home_covers: Times home team covered
# - away_covers: Times away team covered
# - pushes: Exact spread results
```

### Line Movement Opportunities

Find games where opening lines moved significantly:

```python
results = analyzer.identify_line_movement_opportunities(
    league='Serie A',
    days=30  # Last 30 days
)

# Identifies moves of 0.20+ (significant sharp money movement)
```

### Generate Summary Report

```python
analyzer.generate_summary_report('Serie A', 'Soccer')
analyzer.generate_summary_report('NHL', 'Hockey')
```

## Data Sources for Historical Betting Odds

Since free APIs have limited historical odds data, here are recommended sources:

### Free/Affordable Options:

1. **The Odds API** (https://theoddsapi.com/)
   - Current odds for multiple sportsbooks
   - Historical data in paid tier
   - ~$49/month for historical access

2. **Sports-Reference.com**
   - Historical scores and some line data
   - Web scraping allowed
   - Free

3. **Covers.com**
   - Historical opening/closing lines
   - Requires web scraping
   - Free access

### Data Structure for Import

When importing betting data, use this format:

```json
{
    "match_id": 123,
    "sportsbook": "DraftKings",
    "odds_date": "2024-01-31T10:00:00",
    "home_moneyline": -120,
    "away_moneyline": 100,
    "spread_home": -1.5,
    "spread_away": 1.5,
    "over_under": 2.5,
    "notes": "Opening lines"
}
```

## Analysis Examples

### Find Teams That Outperform Spreads

```python
from betting_analyzer import BettingAnalyzer

analyzer = BettingAnalyzer()

# Check which teams consistently cover (beat their spreads)
spreads = analyzer.analyze_spread_covering('Serie A', 'Soccer', days=365)

# A team that covers >55% of the time is a potential edge
```

### Identify Upset Trends

```python
# Moneyline analysis shows upset frequency
ml_results = analyzer.analyze_moneyline_accuracy('NHL', 'Hockey', days=365)

# If underdogs win >45% of the time, there's value in underdog betting
```

### Track Line Movement

```python
# Lines that move >0.20 (20 cents) typically indicate sharp action
movements = analyzer.identify_line_movement_opportunities('Serie A', days=30)

# Moves away from public betting patterns may indicate value
```

## Database Maintenance

### Backup Database

```bash
# In PowerShell
Copy-Item sports_betting.db sports_betting_backup.db
```

### Export Data to CSV

```python
import sqlite3
import csv

conn = sqlite3.connect('sports_betting.db')
cursor = conn.cursor()

cursor.execute('SELECT * FROM matches WHERE league = ?', ('Serie A',))
matches = cursor.fetchall()

with open('serie_a_matches.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerows(matches)

conn.close()
```

### Clean Database (Reset for Testing)

```python
import os
from sports_db import DATABASE_PATH

os.remove(DATABASE_PATH)
# Then re-run init_database()
```

## Tips for Building a Successful Analysis

### Data Quality
- Ensure all matches have corresponding odds entries
- Use consistent date/time formats (ISO 8601)
- Validate team names for consistency
- Document data sources in 'notes' field

### Analysis Best Practices
- Always analyze at least 50+ games for statistical significance
- Compare against betting market baseline (typically -110 moneyline)
- Look for patterns in specific matchups or conditions
- Track before/after implementing strategies

### Identifying Market Inefficiencies
1. **Closing line value (CLV)**: If your picks consistently beat closing odds vs opening odds
2. **Betting percentages**: When public heavily favors one side, opposite may have value
3. **Home/Away patterns**: Some teams perform very differently at home vs away
4. **Spread covering**: Teams that consistently over/under-perform their spread

## Next Steps

1. **Populate with real data**: Connect to APIs or import historical data
2. **Run analysis queries**: Identify patterns in the data
3. **Track performance**: Document your betting strategy results over time
4. **Refine models**: Use analysis results to improve predictions
5. **Backtest strategies**: Test strategies on historical data before live betting

## Support & Troubleshooting

### Database Locks
If you get "database is locked" error:
```python
# Ensure no other processes have the database open
# Restart Python kernel/terminal
```

### API Rate Limits
If hitting rate limits on free APIs:
- Add delays between requests
- Consider upgrading to paid tier
- Use web scraping as fallback

### Missing Data
If certain fields are NULL:
- Check data source availability
- Some sportsbooks don't offer all bet types
- Fill in manually if available elsewhere

---

**Last Updated**: February 2025
**Database Version**: 1.0
