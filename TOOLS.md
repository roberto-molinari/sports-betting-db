# TOOLS.md — what's actually in the root directory

70 Python files live in the repo root. Most of them are not things you run day
to day — they're one-off investigations, tuning sweeps, or building blocks
another script calls. This file exists so "which script do I run" is a lookup,
not a guess. See `CLAUDE.md` for architecture/commands.

Convention below: **bold name** = you'll actually type this. Everything else
is grouped so you can still find it, without it competing for attention.

---

## Club-league model — day to day

The active system (Serie A + Premier League + Bundesliga + La Liga + Ligue 1).

**Generating a card, in order:**
1. `matchday_summary.py --matchday-date <date>` — see what's on the slate and
   which leagues are still missing odds for that date.
2. If a league is missing odds: `import_league_betting_odds.py --league
   "<League>" --season <year> --future-only` — pulls live current-week odds
   from The Odds API and inserts/updates just the upcoming matches (`<year>`
   is the season the date falls in, e.g. 2025 for the 2025-26 season). This
   is the live-odds path — different from `--download`, which pulls
   historical/backtest CSVs from football-data.co.uk instead.
3. `generate_club_league_card.py --league "<League>" --matchday-date <date>`
   — generate the card now that odds exist. Skipping step 2 when odds are
   missing just means fewer/no picks come out, not an error.

- **`matchday_summary.py`** — what's on the slate for a date/range, which
  leagues have odds yet. Run this first (step 1 above).
- **`import_league_betting_odds.py`** — pull live odds ahead of a card
  (step 2 above; `--future-only`), or backfill historical odds for
  backtesting (`--download --season <year>`, no `--future-only`).
- **`generate_club_league_card.py`** — generate today's picks for one league
  (`--post-friendly` for the copy-paste format, `--dry-run` to preview).
  Step 3 above.
- **`club_league_picks_report.py`** — list every stored pick for a matchday/
  range, all leagues: pick/odds/EV/stars, plus result/ROI once graded.
  Read-only — doesn't refresh or grade anything.
- **`club_league_scorecard.py`** — refresh results + grade + report, for a
  matchday or range. This is how you grade picks. `--post-friendly` prints a
  copy-pasteable results summary (per-league record/ROI + the day's biggest
  winner) instead of the detailed table.
- **`model_metrics_report.py`** — Brier/bias/ROI report for a model version,
  optionally persisted to `model_snapshots/`.
- **`export_club_league_picks_json.py`** — run once daily after
  `club_league_scorecard.py` grades that day's picks: regenerates
  `web_export/club_league_picks.json`, the flat per-pick data file behind
  the interactive ROI report page (site lives in a separate repo — copy the
  output there and deploy). Graded picks only, full history every run.

## Club-league model — setup & periodic ingestion

Not daily-loop tools. Three sub-groups, because "run less often" was hiding a
real difference: some of these you trigger yourself, some already run
automatically inside tools from the section above.

**You run these yourself, on their own trigger:**
- **`season_kickoff.py`** — once per season: bootstrap teams/squads/matches
  across every tracked division.
- **`import_club_squads.py`** / **`import_club_player_stats.py`** — after a
  transfer window or roster change, to refresh the player data the model
  blends in.
- `import_league_betting_odds.py` — see the day-to-day section above (step 2)
  for the live-odds command; this is also where you'd reach for the
  `--download` (historical CSV) form for backtest work.

**Already run for you — use these directly only for a manual backfill or fix:**
- `import_league_matches.py` — `season_kickoff.py`'s bootstrap and
  `club_league_scorecard.py`'s refresh step both call this already. Run it
  yourself only to backfill a league/season that hasn't been bootstrapped, or
  to force a resync outside the normal flow.
- `update_serie_a_results.py` — same idea, Serie A-specific (its results sync
  isn't on the TheStatsAPI pipeline yet, see `BUGS.md`).

**Model-work only, not routine ops** (needed when validating a new method or
doing a calibration pass, not for posting today's picks):
- `backfill_player_blend_predictions.py` / `backfill_soccer_model_predictions.py`
  — backfill `soccer_model_predictions` for a full season under a given method.
- `import_league_market_odds.py` — sharp-book (Pinnacle/Betfair) opening +
  closing lines into `soccer_market_odds`, for CLV/bias checks.

## Club-league model — tuning, sweeps, one-off investigations

Not part of any regular workflow. Each exists to answer one specific past
question (usually named in the docstring, cross-referenced to `BUGS.md`).
Skim the docstring before running — several are stale sweeps kept for
reference, not maintained against schema changes.

`compare_model_vs_market_odds.py` (bias/CLV diagnostic against a sharp book;
the tool behind most `BUGS.md` calibration findings — reusable, but you reach
for it during tuning, not as part of posting picks), `backfill_with_xg_stretch.py`,
`backfill_club_xga.py`, `param_sweep.py`,
`diagnose_home_bet_calibration.py`, `sample_xg_lookback_ab.py`,
`oracle_roster_blend_test.py` (marked EXPERIMENTAL, not shipped),
`recalibrate_output.py` (marked EXPERIMENTAL, not shipped), `blend_impact.py`,
`inspect_away_bets.py`, `backtest.py` (superseded by
`backtest_from_predictions.py`, which grades stored predictions instead of
recomputing them), `analyze_serie_a.py`, `analyze_serie_a_advanced.py`,
`validate_serie_a_matches.py`, `betting_analyzer.py`, `backtest_from_predictions.py`
(also a library — several tools import its `run_totals`/grading helpers).

## Library code that happens to be a runnable script

**`compute_club_player_strength.py`** is the actual player/team-strength model
— `core/poisson_model.py`'s partner. Seven other scripts `import` it directly.
Treat it as core logic, not a one-off, even though it lives in root and has a
docstring that still says "FEATURE-011 prototype."

`grade_club_league_picks.py` — grading only, no refresh; the piece
`club_league_scorecard.py` calls internally. Not meant to be run on its own.

---

## World Cup 2026

Picks-posting/scoring UI lives in the separate `serie-a-bets-tracker` repo —
this repo is the data/model backend only (see `WC_TRACKER_INTEGRATION_BRIEF.md`).

**Setup / per-matchday:**
- `import_wc_squads.py` / `import_wc_player_stats.py` — squads + club stats.
- `compute_wc_team_strength.py` — aggregate into per-team strength (`--print`
  to inspect before `--persist`).
- `import_wc_odds.py` — odds from a transcribed-screenshot CSV.
- `import_wc_bovada.py` — odds directly from Bovada's public JSON feed.
- **`generate_wc_card.py`** — the value card for a match window.
- **`update_wc_results.py`** — record results + grade picks.
- `record_knockout.py` — record a full knockout tie (90'/ET/PK) + grade.
- `record_override.py` / `override_report.py` — log and later review a
  deliberate human deviation from the model's pick.

**Data-quality / xG plumbing:** `import_wc_club_leagues.py`,
`import_wc_fifa_xg.py`, `import_wc_match_xg.py`, `import_wc_xg.py`,
`fix_wc_club_defense.py` — all POST-HOC (the model's own numbers are never
derived from these; see `import_wc_match_xg.py`'s "HARD CONSTRAINT" docstring).

**Analysis / calibration / one-off charts:** `roi_history.py`,
`kelly_backtest.py`, `knockout_baseline_backtest.py`, `knockout_pick_review.py`,
`knockout_report.py`, `totals_calibration.py`, `proxy_defense_calibration.py`,
`proxy_goals_calibration.py`, `external_xg_calibration.py`,
`generate_scoreline_heatmap.py`, `generate_r32_xg_gap_chart.py`,
`generate_feature009_chart.py`, `feature009_backtest.py`, `price_ladders.py`
(one-off, docstring says so), `card_ladder_compare.py`.

---

## NHL

Earlier/less-active system (see `CLAUDE.md`).

- `update_nhl_results.py` — sync matches/results for a season.
- `import_nhl_odds.py` — moneyline/spread/totals odds.
- `calculate_nhl_moneyline_roi.py`, `analyze_nhl_betting.py`,
  `validate_nhl_odds_coverage.py`, `validate_nhl_matches.py`,
  `export_nhl_odds_csv.py` — analysis/validation, run ad hoc.

---

## Misc

- `quickstart.py` — a guided tour of the schema/tables. Good first read if
  you're new to this repo.
- `view_database.py` — quick ad hoc table dump (per the top-level `CLAUDE.md`
  rule, prefer `sqlite3 sports_betting.db "SELECT ..."` directly for real
  queries).
