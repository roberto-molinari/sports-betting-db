# Sports Betting Database System

A comprehensive system for tracking and analyzing Serie A (soccer) and NHL (hockey) match results with historical betting odds to identify market inefficiencies and betting opportunities.

## Current Data Status (June 2026)

This snapshot reflects the latest verification queries run against `sports_betting.db`.

### Full Database Coverage (All Seasons Present)

- Serie A match data: seasons **2022-2023 to 2025-2026** (`soccer_matches`, league='Serie A'), **1521** total matches
- Serie A odds data: seasons **2022-2023 to 2025-2026** (`soccer_betting_odds` joined to Serie A matches), **1520** odds rows covering **1520** distinct matches
- NHL game data: seasons **2023-2024 to 2025-2026** (`nhl_matches`), **4424** total games
- NHL odds data: seasons **2023-2024 to 2025-2026** (`nhl_betting_odds` joined to `nhl_matches`), **2547** odds rows covering **2547** distinct games


## System Overview

This system now has two layers:

1. **core/** - Shared database, collection, sync, and modeling code
2. **root scripts** - Runnable entrypoints for imports, analysis, validation, and maintenance

## Database Schema

### Tables

- **soccer_teams**: Serie A team metadata (`team_id`, `name`, `league`, `country`)
- **soccer_matches**: Serie A fixtures/results (`season`, teams, scores, halftime scores, `match_status`)
- **soccer_betting_odds**: Soccer odds history (1X2, spread, totals, sportsbook/time metadata)
- **nhl_teams**: NHL team metadata (`team_id`, `name`, `country`)
- **nhl_matches**: NHL games/results (`season`, teams, scores, `match_status`)
- **nhl_betting_odds**: NHL odds history (moneyline, spread, totals, sportsbook/time metadata)

### Key Relationships

```
soccer_teams
  └── soccer_matches (home_team_id, away_team_id)
      └── soccer_betting_odds (match_id)

nhl_teams
  └── nhl_matches (home_team_id, away_team_id)
      └── nhl_betting_odds (match_id)
```

## Getting Started

### 1. Initial Setup

```python
from core.sports_db import init_database

# Initialize the database
init_database()
```

This creates a SQLite database file: `sports_betting.db`

### 2. Ingest Data

Use the update/import scripts in Section 3 to populate the database with
current Serie A and NHL results/odds data.

### 3. Updating Serie A and NHL Data

#### For Serie A Data Updates - Use the Unified Serie A Updater

The script `update_serie_a_results.py` is the primary way to keep your Serie A match data current. It can use two different data sources, specified with the `--source` flag.

What it updates in `sports_betting.db`:
- Final score (`home_score`, `away_score`)
- Halftime score (`halftime_home_score`, `halftime_away_score`) when the source provides it
- Match status (`scheduled` -> `completed`)

It is safe to re-run. If a match is already marked `completed` but is still missing halftime scores, the updater will now backfill those fields instead of skipping the row.

**1. API Mode (Default)**

This is the recommended method for automated, recurring updates. It fetches data directly from the `football-data.org` API.

- **Requirements**: A free API key from [football-data.org](https://www.football-data.org/client/register).
- **Usage**:
  ```bash
  # Activate the virtual environment first
  source .venv/bin/activate

  # Option A: pass key explicitly
  python update_serie_a_results.py YOUR_API_KEY

  # Option B: read key from env var
  export FOOTBALL_DATA_API_KEY=YOUR_API_KEY

  # Update the current season's results
  python update_serie_a_results.py

  # Update a specific season (e.g., 2024-25)
  python update_serie_a_results.py --season 2024
  ```
- **Automation**: You can schedule this to run automatically (e.g., via cron) to keep your database fresh.

**2. CSV Mode (Fallback)**

This method provides a reliable, key-free way to update results by downloading the latest season data as a CSV file from `football-data.co.uk`. It's a great manual fallback if the API is unavailable.

- **Note**: The CSV feed can lag behind the actual match completion time. If a just-finished match is still missing scores after a CSV sync, re-run later or use another verified source for a one-off manual repair.

- **Requirements**: None (no API key needed).
- **Usage**:
  ```bash
  # Activate the virtual environment first
  source .venv/bin/activate

  # Update the current season's results from CSV
  python update_serie_a_results.py --source csv

  # Update a specific season from CSV
  python update_serie_a_results.py --source csv --season 2024
  ```

#### Quick Verification

After an update run, you can verify that completed matches have both final and halftime scores:

```bash
sqlite3 sports_betting.db <<'SQL'
.headers on
.mode column
SELECT COUNT(*) AS completed_missing_scores
FROM soccer_matches
WHERE league = 'Serie A'
  AND match_status = 'completed'
  AND (
    home_score IS NULL OR away_score IS NULL OR
    halftime_home_score IS NULL OR halftime_away_score IS NULL
  );
SQL
```

If that query returns `0`, there are no completed Serie A matches missing score fields.

#### For NHL Data Updates - Use NHL API (Free, No Auth Required)

For routine NHL results refreshes, use the unified updater script:

```bash
source .venv/bin/activate

# Sync current NHL season
python update_nhl_results.py

# Sync a specific season (e.g. 2025-26)
python update_nhl_results.py --season 2025

# Historical/backfill mode: write completed games only
python update_nhl_results.py --season 2024 --completed-only
```

This updates `nhl_matches` with the latest schedule state and final scores.
NHL betting odds are still imported separately via `import_nhl_odds.py`.

### NHL Maintenance Scripts (Recommended Workflow)

Use these scripts together when refreshing NHL data:

```bash
source .venv/bin/activate

# 1) Sync NHL schedule + results
python update_nhl_results.py --season 2025

# 2) Import NHL odds from a source file or API
python import_nhl_odds.py --season 2025 <odds_file.csv>

# 3) Validate odds coverage progress
python validate_nhl_odds_coverage.py --season 2025
```

Script roles:
- `update_nhl_results.py`: user-facing unified updater for NHL fixtures and final scores.
- `import_nhl_odds.py`: imports NHL odds from The Odds API (`--future-only`) or local CSV input.
- `validate_nhl_odds_coverage.py`: tracks progress to full odds coverage by market type.
- `core/nhl_results_sync.py`: shared internal sync logic used by the updater and collector modules.

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
from core.data_collector import SportDataCollector

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


## Database Operations

### Adding Data

```python
from core.sports_db import (
    ensure_soccer_team,
    add_soccer_match,
    add_soccer_betting_odds,
    update_soccer_match_result,
)

# Add teams
home_team_id = ensure_soccer_team('AC Milan', 'Serie A', 'Italy')
away_team_id = ensure_soccer_team('Inter', 'Serie A', 'Italy')

# Add match
match_id = add_soccer_match(
    league='Serie A',
    season=2024,
    home_team_id=home_team_id,
    away_team_id=away_team_id,
    match_date='2024-02-01T20:00:00',
    status='scheduled',
)

# Add betting odds
add_soccer_betting_odds(
    match_id=match_id,
    sportsbook='DraftKings',
    odds_date='2024-01-31T12:00:00',
    home_moneyline=-120,
    draw_moneyline=240,
    away_moneyline=100,
    spread_home=-1.5,
    spread_away=1.5,
    over_under=2.5,
    notes='Opening lines'
)

# Update with final result
update_soccer_match_result(
    match_id,
    home_score=2,
    away_score=1,
    halftime_home=1,
    halftime_away=0,
)
```

### Querying Data

```python
from core.sports_db import get_soccer_matches, get_nhl_matches

# Get completed Serie A matches for a season
serie_a_matches = get_soccer_matches(league='Serie A', season=2025, status='completed')

# Get completed NHL games for a season
nhl_games = get_nhl_matches(season=2025, status='completed')
```

## Betting Analysis

This repository is primarily script-driven for analysis workflows. Prefer running
the scripts below directly instead of using inline Python snippets.

```bash
source .venv/bin/activate
```

### Core Analysis Scripts

- `python analyze_serie_a.py`
  - Full multi-section Serie A analysis across available seasons.
- `python analyze_serie_a.py --season 2025`
  - Single-season Serie A analysis.
- `python analyze_nhl_betting.py`
  - NHL betting/coverage analysis report (moneyline, spread, totals, team-level summaries).
- `python analyze_serie_a_advanced.py`
  - Additional deeper-dive analysis queries.

### Strategy Evaluation Scripts

- `python backtest.py`
  - Backtesting framework for strategy performance.
- `python param_sweep.py`
  - Parameter sweep for strategy tuning.
- `python calculate_nhl_moneyline_roi.py --side favorite`
  - ROI view for NHL favorite-oriented approaches.
- `python calculate_nhl_moneyline_roi.py --side underdog`
  - ROI view for NHL underdog-oriented approaches.
- `python calculate_nhl_moneyline_roi.py --side both`
  - Combined ROI view for both favorite and underdog strategies.
- `python inspect_away_bets.py`
  - Investigation helper for away-bet behavior.

### Data Quality Validation for Analysis Inputs

- `python validate_serie_a_matches.py`
- `python validate_nhl_matches.py`
- `python validate_nhl_odds_coverage.py --season 2025`

These validation scripts are useful pre-analysis checks to confirm coverage and
avoid misleading outputs from partial datasets.

## Data Sources for Historical Betting Odds

### Current Source (NHL Data)

**Kaggle: NHL Historical Game Data**
- Source: https://www.kaggle.com/datasets/jonathanncoletti/nhl-historical-game-data
- File: `data/csv/nhl_data_extensive_last_two_years.csv`
- Contains: 2+ seasons of NHL games with ESPN betting odds (moneylines, spreads, over/under)
- Imported via: `import_nhl_odds.py` (local CSV mode)
- Note: Includes spread values and favorite moneyline, but NOT spread odds or over/under odds (these would need to be sourced separately)

### Additional Data Sources for Reference

Since free APIs have limited historical odds data, here are other recommended sources:

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

**Last Updated**: June 2026
**Database Version**: 1.0


