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
4. Ingest betting odds:
   - **Current source:** Kaggle NHL Historical Game Data (https://www.kaggle.com/datasets/jonathanncoletti/nhl-historical-game-data)
     - File: `nhl_data_extensive_last_two_years.csv` (ESPN betting data)
     - Import via: `import_betting_odds.py`
   - **Note:** Kaggle dataset contains moneylines and spread values, but NOT spread odds or over/under odds.
   - **Alternative sources:** The Odds API (paid), web scraping (Covers.com, Sports-Reference.com)
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

## Computed Fields: Spread Odds Estimation

### Background

The primary NHL odds source (Kaggle ESPN dataset) provides moneylines, spread values (±1.5),
and over/under totals, but does **not** include spread odds (`spread_home_odds`,
`spread_away_odds`) or over/under odds (`over_odds`, `under_odds`). These fields are left
NULL after the initial import via `import_betting_odds.py`.

### Approach: Poisson Goal Model

Spread odds are estimated using a Poisson goal model, implemented in `compute_spread_odds.py`.
The approach derives each team's expected goals (λ) from the moneyline and O/U already in the
database, then uses the joint Poisson distribution to compute the probability that the favorite
wins by 2 or more goals (i.e., covers the -1.5 spread).

**Step 1 — Moneyline → fair win probability**

American odds are converted to raw implied probabilities, then normalized to remove sportsbook
vig:

- Negative odds: `p_raw = |ml| / (|ml| + 100)`
- Positive odds: `p_raw = 100 / (ml + 100)`
- Fair probability: `p_home_fair = p_home_raw / (p_home_raw + p_away_raw)`

**Step 2 — Solve for λ_home and λ_away**

Two constraints are used:
1. `λ_home + λ_away = over_under`
2. `P(home wins | Poisson(λ_home), Poisson(λ_away)) = p_home_fair`

A binary search on the split fraction `s` sets `λ_home = s × O/U` and finds the value where
the modeled home win probability matches the implied fair probability. Win probability is
computed by summing the joint Poisson mass over all score pairs where `i > j` (truncated at
15 goals per team).

**Step 3 — Compute P(favorite covers -1.5)**

The favorite is identified from the `spread_home` sign:
- `spread_home < 0` → home team is favored → compute `P(home - away ≥ 2)`
- `spread_home > 0` → away team is favored → compute `P(away - home ≥ 2)`

Over/under odds are also computed: `P(home_goals + away_goals > ou_line)`.

**Step 4 — Apply vig and convert to American odds**

A standard -110/-110 sportsbook margin (~4.76% overround) is applied before converting to
American odds format for storage.

### Assumptions and Limitations

- Goals are modeled as independent Poisson processes, which is a good but imperfect
  approximation for hockey (scoring rate is not perfectly stationary within a game).
- Moneylines include overtime and shootout outcomes; the Poisson model strictly models
  regulation. This introduces a small systematic bias — OT/shootout results are treated as
  if they were regulation wins/losses.
- The spread is assumed to be ±1.5 for all computed records. Games with non-standard spreads
  are handled by the same formula but noted as less reliable.
- Computed odds are approximations, not market prices. They should be treated as a baseline
  for analysis, not as actionable betting lines.

### How to Run

```bash
python compute_spread_odds.py
```

This updates all NHL `betting_odds` rows where `spread_home IS NOT NULL` and
`spread_home_odds IS NULL`. It is safe to re-run (idempotent — skips already-populated rows).