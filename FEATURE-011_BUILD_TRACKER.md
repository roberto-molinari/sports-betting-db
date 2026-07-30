# FEATURE-011 Build Tracker — Real System

Tracks the actual mid-August production build (not the prototype — see
`FEATURE-011_PROTOTYPE_LOG.md`, closed 2026-07-29, for the feasibility work that preceded
this). Companion to `FEATURE-011_REQUIREMENTS.md` (design/scope source of truth). Update
task status in place as work lands; this doc reflects *current* state, not a chronological
log — put dated narrative detail in commit messages, not here.

**Target: mid-August 2026 release.** v1 scope = Serie A + Premier League, Bundesliga,
LaLiga, Ligue 1. Serie A must clear Success Criteria (signed bias + ROI) before European
rollout starts — see Success Criteria task below, it's a hard gate, not a formality.

**Status legend:** DONE / IN PROGRESS / NOT STARTED / NEEDS YOUR INPUT

---

## Needs your input right now

Nothing blocking right now. Next up per the build order is task 4 (per-league naming) or
task 5 (Success Criteria validation, now unblocked) — say which to start on.

---

## Task list (agreed build order)

| # | Task | Status | Notes |
|---|------|--------|-------|
| 1 | Per-match schema + pipeline rework (Serie A) | **DONE** (2026-07-29) | `soccer_player_stats` reworked to per-match grain, `soccer_player_match_lineups` added, `api_match_id` mapping solved (team-pairing, not date). Validated end-to-end on 3 teams (Milan/Pisa/Roma, 380/380 matches mapped). 31 new tests added, full suite (285) green. |
| 2 | Blend-weight resolution table | **DONE** (2026-07-30) | `resolve_blend_weight(team_id, league, component, season)` in `compute_club_player_strength.py`. Per-team default = `data_coverage_score * roster_change_score` (product, both required), converted to this file's `w` convention; league-wide override (`LEAGUE_WEIGHT_OVERRIDES`, empty by default) takes precedence per component. Formula in `FEATURE-011_REQUIREMENTS.md` (Blend). 7 new tests, full suite (292) green. Not yet validated against real data — current DB only has 2025 (season) player-stats, so `last_season = season-1` has nothing to compute from yet and everything correctly falls back to pure team-level; real validation happens once 2025-26 data exists as "last season" for an actual 2026-27 computation. |
| 3 | Scale per-match import to full 20-team Serie A | **DONE** (2026-07-30) | All 380 matches imported (17683 stat rows, 17703 lineup rows, 762 API requests, well under the confirmed 120 req/min pace). Found and fixed a real bug this surfaced: `load_team_players()` was attributing a player's stats by their CURRENT team (`soccer_players.team_id`) instead of who they actually played for in each match — invisible with the 3-team subset (no mid-season transfers among them), but real once the full league's transfer activity was in view (29 players / 463 rows misattributed, e.g. Sebastiano Luperto's 23 Cagliari matches were being folded into Cremonese, leaving Cagliari's aggregate missing him). Fixed to attribute by match-time team (venue + `soccer_matches`); all 20 teams now land at the expected ~37-38k total squad-minutes ballpark. 1 new regression test + 5 existing tests updated (needed `venue` to resolve team attribution, which they hadn't needed before); full suite (293) green. |
| 4 | Per-league team-name matching + competition config: Premier League, Bundesliga, LaLiga, Ligue 1 | NOT STARTED | Confirmed real, per-league work (not reusable from Serie A) — see prototype log's cross-league spot check. Also need per-league competition search-term/id config (`LaLiga` gotcha). **Bigger dependency found 2026-07-30: `soccer_matches` has ZERO rows for any of these 4 leagues, and the only base match/team/result collector (`core/data_collector.py`) is hardcoded to Serie A via football-data.org — there's no generic multi-league importer yet.** This predates FEATURE-011 (it's the foundational team-level system's own data) but blocks this task: real chain is base match-data collection (not yet scoped anywhere) → this task's competition/team-name matching → the already-built import/compute pipeline (reusable as-is). Nothing else in this list depends on task 4 — it only blocks scaling to Europe, which can't start before task 5 clears anyway. |
| 5 | Success Criteria validation (Serie A) | NOT STARTED | Signed bias vs. sharp closing lines within ±0.01-0.02, **hard** +ROI requirement (harder bar than the team-level fix ever cleared). Hasn't been run at all yet. Hard gate — Europe rollout can't start until this clears. Depends on task 2. |
| 6 | Cadence automation (weekly refresh, staged validation, auto-promote/rollback) | NOT STARTED | Currently a one-off manual compute (`--print`/`--persist`), same shape as `compute_wc_team_strength.py`. Needs task 2 finished to run unattended. |
| 7 | Output (pick generation, card format, decision-trail logging) | NOT STARTED | Reuses `generate_wc_card.py`'s display layer; selection algorithm (guardrails/EV logic) is a separate decision, not assumed to be WC's tuned version. |
| 8 | Remaining scenarios: Scenario 2 (unavailable-player override), Scenario 3 (contributor reporting), Scenario 9 (coach-change override) | NOT STARTED | Lower priority — none of these are believed likely to threaten the Aug 15 date. Scenario 9 flagged explicitly (2026-07-30) as a real near-term concern — many Serie A (and likely other) teams had offseason manager changes — but deliberately kept OUT of the blend-weight formula (task 2); it's a separate trigger on the same "how much to trust team-level history" question. Don't forget it. |

## Loose threads (bugs/wrinkles, not full tasks)

- Pisa's per-match import: 1835 stat rows vs. 1834 lineup rows for the same 38 matches — one-row mismatch, not investigated.
- Bundesliga/Ligue 1 `/teams` list returns 19/21 teams against a stated `total_teams: 18` for both — not investigated, flag before relying on either league's team list during task 4.
- Fiorentina vs Atalanta (match_id 373, 2026-05-24) has lineups but zero player-stats — confirmed via a direct API call that `matches/{id}/player-stats` genuinely returns nothing for this match (not an import bug on our end, a source-data gap). Explains why Atalanta/Fiorentina's total squad-minutes came in slightly lower than the other 18 teams after task 3. Not investigated further; low priority (1 match out of 380).
