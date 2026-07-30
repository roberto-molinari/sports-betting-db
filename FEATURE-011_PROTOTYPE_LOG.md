# FEATURE-011 Prototype — Goals, Non-Goals, and Results Log

**STATUS: CLOSED, 2026-07-29.** The prototype (3-team Serie A subset: Milan, Pisa, Roma)
fully answered what it set out to answer — all 5 goals below are scored. Production build
work is no longer tracked here; see **`FEATURE-011_BUILD_TRACKER.md`** for what's done,
in progress, and left before the Aug 15 release. This file is now a historical record —
don't append new status here.

Ran: 2026-07-28 to 2026-07-29. Companion: `FEATURE-011_REQUIREMENTS.md` (source of truth
for scope/design).

---

## Goals — all scored

| # | Goal | Result |
|---|------|--------|
| 1 | Data source feasibility | **WORKED.** TheStatsAPI has real per-player Serie A data (position, minutes, goals, assists). 76% of the 75-player sample had usable minutes/goals. No xG at season-stats level (same gap as WC data) — later fully resolved by switching to the per-match endpoint, which does have real xG. |
| 2 | Team-name matching | **WORKED.** All 20/20 Serie A teams matched via one normalization pass (strip AC/AS/US prefixes, Calcio/CFC suffixes, founding-year suffixes). Confirmed **not reusable** for other leagues (see cross-league spot check below). |
| 3 | Blend mechanism | **WORKED — headline result.** AC Pisa 1909 (thin Serie A history, one of BUG-009's worst-mispriced teams): team-level-only attack rating was an implausible 0.684; player-based was 1.147; the blend visibly pulled toward the more plausible number. Milan/Roma (established squads) showed small, unremarkable gaps — the expected pattern. This is the core evidence the mechanism does what it's meant to do. |
| 4 | Persistence shape | **WORKED.** All three new tables round-tripped a real `--persist` run; latest-blend retrieval preserves both pre-blend components and a basis label. |
| 5 | Cost / request-volume | **WORKED, corrected number.** Initial finding (~48 min/league refresh) was based on a stale ~12 req/min comment. A live rate-limit-header check confirmed the real limit: **120 req/min (Starter tier)**. Real weekly-refresh cost is a few minutes per league, not a latency risk. |

---

## Non-Goals (explicitly out of scope for the prototype)

- Scenario 0 (lineup-history baseline) — later pulled into v1 scope and built for real during the production rework (`matches/{id}/lineups`); no longer a non-goal.
- Scenario 2 (player-unavailable override), Scenario 3 (contributor reporting), Scenario 9 (coach-change override) — not built. Still open; tracked in the build tracker.
- Scenario 4 (home/away split calculation) — not built; data is captured (`venue` column) but the split calculation is deferred by design, not a prototype gap.
- Cadence (automated weekly refresh/validation/rollback) — not built.
- Scenario 6 (multi-league config) — Serie A was hardcoded.
- Scenarios 7 & 8 (league-strength self-bootstrapping, cold-start profiles) — reuses WC's static `LEAGUE_FACTORS` as-is.
- Success Criteria (signed-bias + ROI backtest) — not run.
- Output (pick generation, card format, decision-trail logging) — not built.

---

## Cross-league spot check (2026-07-29) — Premier League, Bundesliga, LaLiga, Ligue 1

Quick API-side check (no DB team data existed yet for these leagues) before committing to
the Aug 15 rollout.

- **Data coverage: no blocker.** All 4 confirmed `has_player_stats=True`, `xg_available=True`. Match-level `player-stats` endpoint confirmed working outside Serie A too.
- **Competition search-term gotcha (real, concrete):** searching `"La Liga"` returns nothing — the API's actual name is `LaLiga` (no space). Needs a per-league search-term/competition-id config, not a shared "search the league name" default.
- **Team-name matching: confirmed real, not hypothetical, and NOT reusable from Serie A.** Bundesliga (numeric prefixes, umlauts, abbreviated names), LaLiga (accents, alternate names like Athletic Club = Athletic Bilbao), Ligue 1 (official vs. common names, e.g. PSG), Premier League (cleanest, but still truncation like "Wolverhampton"). Each league needs its own pass.
- **Unexplained, still unresolved:** `/teams` returned 19 (Bundesliga) / 21 (Ligue 1) teams against a stated `total_teams: 18` for both. Not investigated. Carried forward to the build tracker.

---

## Reference: build history during the prototype (for context only)

- 2026-07-28 — schema added (`soccer_players`, `soccer_player_stats` season-total, `soccer_player_team_strength`); `import_club_squads.py` and `import_club_player_stats.py` (season-total version) built and run for real on 3 teams (75 players, 87 requests); `compute_club_player_strength.py` built, reusing WC's player-aggregation and shrinkage logic with a team-level blend replacing the FIFA-rank blend.
- 2026-07-29 — per-match storage rework began (this is where prototype work ends and production build begins — see `FEATURE-011_BUILD_TRACKER.md` for everything from this point forward).
