# Architecture - Sports Betting Database

## Purpose
Provide a concise technical overview of the repository: main components,
storage schema, data flow, external integrations (notably NHL data), and
how to run the collector to build a 2-year NHL historical dataset.

## High-level Components

- `sports_db.py` — SQLite schema and helper functions for CRUD operations.
  - Core tables: `teams`, `matches`, `betting_odds`, `match_outcomes`, `betting_analysis`.
- `data_collector.py` — Ingests sports data and betting lines.
  - Integrates with `nhlpy` (installed as `nhl-api-py`) for NHL web endpoints.
  - Falls back to direct `requests` calls against `statsapi.web.nhl.com` when `nhlpy` isn't available.
  - Includes `collect_nhl_data()` and `collect_nhl_historical_data()` helpers.
- `betting_analyzer.py` / `advanced_analysis.py` — Analysis modules to find
  moneyline/spread inefficiencies and generate reports.
- Utility scripts: `quickstart.py`, `interactive_example.py`, `view_database.py`,
  CSV import helpers (manual import paths), and small test scripts.

## Database Schema (summary)

- `teams`:
  - `team_id` (PK), `name`, `sport`, `league`, `country`, `created_at`
- `matches`:
  - `match_id` (PK), `sport`, `league`, `season`, `home_team_id`, `away_team_id`,
    `match_date`, `home_score`, `away_score`, `match_status`, `created_at`
- `betting_odds`:
  - Stores betting lines per `match_id`, `sportsbook`, `odds_date`, moneyline/spread/OU fields
- `match_outcomes`, `betting_analysis`:
  - Summaries and precomputed analysis artifacts

Refer to `sports_db.py` for full DDL.

## Data Flow

1. Initialize DB: call `init_database()` (creates `sports_betting.db`).
2. Ingest teams:
   - `collector.add_sample_data()` (local quick testing), or
   - `collector.collect_nhl_data()` (uses `nhlpy` or HTTP fallback) to populate `teams`.
3. Ingest matches & results:
   - `collect_nhl_historical_data(seasons=[...])` fetches per-team season schedules
     (via `nhlpy.client.schedule.team_season_schedule`) and inserts completed games into `matches`.
4. (Separate) Ingest betting odds:
   - Odds are not provided by `nhlpy` or the NHL web API — use paid sources (The Odds API), web scraping sources, or CSV import via provided helpers.
5. Analyze:
   - Use `betting_analyzer.py` and `advanced_analysis.py` to query tables and compute insights.

## NHL Integration Details

- Preferred client: `nhlpy` (package name: `nhl-api-py`, imported as `nhlpy`).
  - Lightweight `NHLClient` with modules: `teams`, `schedule`, `standings`, `stats`, etc.
  - Example: `client = NHLClient(); teams = client.teams.teams()` returns a list of teams.
  - Season format: use `YYYYYYYY` strings (e.g., `20242025`).
- Fallback: direct HTTP to `https://statsapi.web.nhl.com/api/v1` (older endpoints).
- Note: neither `nhlpy` nor the NHL APIs provide betting lines; you'll need a separate odds source.

## Running / Commands

- Install runtime deps in the project venv:

```bash
/your/venv/path/bin/pip install requests nhl-api-py
```

- Initialize DB:

```bash
/your/venv/path/bin/python -c "from sports_db import init_database; init_database()"
```

- Add sample data (quick check):

```bash
/your/venv/path/bin/python -c "from data_collector import SportDataCollector; SportDataCollector().add_sample_data()"
```

- Build 2-year NHL historical results (example):

```bash
/your/venv/path/bin/python - <<'PY'
from data_collector import SportDataCollector
c = SportDataCollector()
c.collect_nhl_historical_data()  # defaults to last 2 seasons
PY
```

- Verify data via `view_database.py` or run `quickstart.py` (it contains commented instructions to load historical NHL data).

## Operational Notes & Recommendations

- Idempotency: `collect_nhl_historical_data()` attempts to avoid duplicates; re-runs are safe.
- Rate limiting & retries: `nhlpy` handles standard web endpoints; for large imports consider adding exponential backoff for failures and partial resume checkpoints.
- Odds ingestion: design a separate pipeline to fetch and normalize odds (timestamped lines), and link rows to `matches` by `match_date` + teams.
- Tests & CI: add a small smoke test that initializes the DB, inserts a couple of sample rows, and asserts counts.
- Add a `requirements.txt` or `pyproject.toml` for reproducible installs.

## Next steps (suggested)

- Add `requirements.txt` with `requests` and `nhl-api-py`.
- Add a small script to import betting odds from a CSV and reconcile with `matches`.
- Implement caching or local manifest of downloaded schedules to avoid repeated network traffic.

---

If you want, I can: 
- create `requirements.txt`, or
- add a small `scripts/import_odds.py` prototype, or
- expand this doc with a diagram (Mermaid) and concrete schema DDL excerpts.

Which would you like next?