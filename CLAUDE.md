# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

**70+ scripts live in the repo root.** Before running one you haven't used before,
check `TOOLS.md` — it sorts every root script into "you'll actually run this" vs.
internal/one-off/experimental, so picking one by a plausible-sounding name doesn't
end in a confusing surprise.

## Commands

```bash
source .venv/bin/activate         # always activate first
pip install -r requirements-dev.txt   # one-time: pytest + ruff
```

**Tests** (pytest, config in `pyproject.toml`):
```bash
pytest                              # fast unit tests (default) — no real DB needed
pytest -v                           # list every test name
pytest tests/test_poisson_model.py  # a single file
pytest tests/test_poisson_model.py::test_name   # a single test
pytest -m data_integrity            # ONLY the live-DB data-quality checks (opt-in, needs sports_betting.db)
pytest -m ""                        # everything: unit + data-integrity together
```
Two kinds of test: `test_poisson_model.py`/`test_db_smoke.py`/`test_compute_club_player_strength.py`/etc.
run against a throwaway temp-file DB (see `tests/conftest.py`'s `db_path`/`conn` fixtures) and check
*code* correctness. `test_data_integrity.py` opens the real `sports_betting.db` read-only and checks
*data* quality (dupes, referential integrity, missing scores) — it's marked opt-in and skipped by
default so the everyday run doesn't depend on the data file. A failing data-integrity test means the
data needs fixing, not the code.

**Lint** (ruff, config in `pyproject.toml` — conservative rule set `E4,E7,E9,F`, `legacy/` excluded):
```bash
ruff check .          # report problems
ruff check . --fix    # auto-fix the safe ones
```
Not wired into a hook or CI — run manually.

**Common workflows** (all scripts take `--league`/`--season` flags, not hardcoded):
```bash
# Backfill club-league model predictions for a season (writes soccer_model_predictions)
python3 backfill_player_blend_predictions.py --league "Serie A" --season 2025 --method poisson_v4

# Metrics report (Brier/bias/ROI) for a stored method — --note is REQUIRED whenever any
# other flag is passed (only the zero-arg invocation skips it and prints console-only)
python3 model_metrics_report.py --method poisson_v4 --note "why this run"

# Generate a live matchday card (one pick per match)
python3 generate_club_league_card.py --league "Serie A" --matchday-date 2026-08-20

# World Cup: squads/stats -> team strength -> per-matchday odds import -> card -> grade
python3 import_wc_squads.py && python3 import_wc_player_stats.py
python3 compute_wc_team_strength.py --print   # inspect before persisting
python3 compute_wc_team_strength.py --persist
python3 import_wc_odds.py odds.csv --sportsbook DraftKings
python3 generate_wc_card.py --date 2026-06-11
python3 update_wc_results.py results.csv
```
# Any sports_betting.db query
To query the database, always run sqlite3 /Users/robertomolinari/code/sports-betting-db/sports_betting.db \"SELECT ...\" directly — never write a Python script to run queries.

## Architecture

**Single SQLite file** (`sports_betting.db`), no ORM — `core/sports_db.py` owns the schema
(`init_database()`) and every CRUD helper; scripts call those helpers rather than writing raw SQL
against tables directly. Three largely-independent product areas share this one file and this one
`core/` layer:

1. **Club-league model** (`soccer_*` tables) — the actively-developed system. Serie A plus four
   more top-flight leagues (Premier League, Bundesliga, La Liga, Ligue 1), each with a feeder
   division for cross-league promotion/call-up history (Serie B, Championship, 2. Bundesliga,
   LaLiga 2, Ligue 2). Method name `poisson_v4` (`compute_club_player_strength.py` +
   `core/poisson_model.py`) is the live default; `poisson_v3` (team-level only, no player blend)
   is the earlier baseline still kept for comparison.
2. **World Cup 2026 picks** (`soccer_wc_*` tables) — reuses the same probability -> EV -> pick
   framework but derives team strength from squad players' *club* stats blended with a FIFA-rank
   fallback (`compute_wc_team_strength.py`). **The picks-posting/scoring UI lives in a separate
   repo (`serie-a-bets-tracker`)** — this repo is the data/model backend only; see
   `WC_TRACKER_INTEGRATION_BRIEF.md`.
3. **NHL** (`nhl_*` tables) — earlier/less-active system, moneyline+spread+totals tracking via
   `nhlpy`/`update_nhl_results.py`.

### Club-league model pipeline

Full plain-English walkthrough: `MODEL_PIPELINE_OVERVIEW.md`. Short version — for each team,
independently for attack and defense:

- A **player-level number**: for every player whose most recent appearance was for that team
  (no lineup projection — see `load_team_players`'s docstring), their last N appearances
  (season- and league-blind, xG or goals), minutes/position-weighted and shrunk toward the
  league-position average. A cross-league goal-rate adjustment
  (`PLAYER_RATING_CROSS_LEAGUE_GOAL_ADJUSTMENT`) gates/scales games from leagues without a
  calibrated goal-rate relationship (e.g. Serie B).
- A **team-level number**: the pre-existing system, last-10-home/away-matches, blended
  actual-goals/xG (`TEAM_RATING_XG_V_GOALS_BLEND`, currently pure xG).
- Blended per team via a trust score (`player_trust_score`/`resolve_blend_weight`) built from
  roster continuity (churn between two adjacent, non-overlapping N-match windows) and current-
  squad minutes coverage — a stable squad gets pulled toward pure team-level; a squad with heavy
  turnover or many thin-minutes unknowns leans player-level.
- Both team-level and player-level attack/defense ratings get an optional "spread-stretch"
  recentering (`TEAM_RATING_XG_SPREAD_STRETCH_ATTACK/_DEFENSE`,
  `PLAYER_RATING_SPREAD_STRETCH_ATTACK/_DEFENSE` — four independent constants, split 2026-08-12)
  to counter dispersion compression relative to raw goals-based ratings — see BUG-009/BUG-010 in
  `BUGS.md` for why and how each was calibrated.
- The resulting `lambda_home`/`lambda_away` feed the same Poisson scoreline grid `poisson_v3`
  already used (`core/poisson_model.py`) — nothing downstream (EV, pick selection) differs
  between versions.

`core/leagues.py` is the single per-league config registry (external API/CSV identifiers,
feeder-division links) — ingestion scripts read from it instead of hardcoding per-league values.
`core/team_name_maps.py` similarly centralizes the football-data.co.uk-name -> canonical-name
maps needed to join odds data onto TheStatsAPI-sourced teams.

**Point-in-time correctness is a load-bearing invariant, not a nice-to-have.** Every backfill/
backtest script computes each match's prediction using only data that existed strictly before
that match (team ratings, roster-as-of-date, league averages) — mirroring `analyse_match()`'s
no-lookahead discipline. When touching `compute_club_player_strength.py` or the backfill scripts,
preserve this; a "current roster" or "latest stats" shortcut that's fine for a live card is a
lookahead bug in a backtest.

### Tracking conventions

- **`BUGS.md`** (large, append-only) is the canonical log of bugs/features investigated, fixed, or
  still open — new findings (even mid-investigation) get appended there, not left only in chat.
  Each entry documents root cause, fix, and validation numbers so the reasoning survives past the
  session that found it.
- **`MODEL_TUNING_PARAMETERS.md`** documents every tunable constant's current value and the
  calibration rationale behind it (a sweep, an empirical study — never a guessed number).
- **`MODEL_VERSION_LOG.md`** is a running, append-only summary of each shipped
  `soccer_model_predictions.method` tag's net impact (Brier/bias/ROI, before vs. after) — one
  entry per version, so "what did version X actually do to the numbers" doesn't require digging
  back through `BUGS.md`. Update it whenever a new method tag ships, alongside its `BUGS.md` entry.
- **Calibration sweep discipline**: when tuning a constant, sweep real candidate values against
  bias (`compare_model_vs_market_odds.py`)/Brier/ROI (`model_metrics_report.py`,
  `backtest_from_predictions.py`), and pick the largest value that stays inside the bias target
  (±0.01-0.02) rather than stopping at the first value tried — find the real ceiling.
- **Verify plumbing changes are true no-ops**: when refactoring (e.g. splitting a shared constant
  into per-side constants, adding a cache), verify behavior is unchanged at default values via a
  row-level diff of `soccer_model_predictions` before/after, not just "tests still pass."
- `model_snapshots/` holds timestamped, `--note`'d output from `model_metrics_report.py` — a
  running history of real metrics per method/config, not just the latest.
