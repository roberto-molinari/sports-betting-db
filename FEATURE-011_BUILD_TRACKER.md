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

## STATUS AS OF 2026-08-02 (end of session) — READ THIS FIRST

### The problem

Earlier versions of the probability generation model relied solely on team-level data as inputs. This reliance on team-level data makes generating reliable and reasonable probabilities harder because of both a smaller set of data points at the team level, and also the potential for underlying trends to go unseen. So we began building a player-level-driven version of the model (v4). In order to increase the reliability and accuracy of the new v4 model, we chose tracking (but explicitly not replicating) the implied probabilities from sharp sportsbooks as one of the Success Criteria. Implied probabilities from sharp sportsbooks are one of the best available references for what a well-calibrated probability looks like.

But when we first applied the tracking, to measure the gap between the v4 model's probabilities and the implied probabilities from sharp sportsbooks, the gaps were much larger (for both v3 and v4) than we were willing to accept, hence the investigation to narrow these gaps.

Starting point — signed bias vs. Betfair Exchange closing lines, season 2025-26, before any of the fixes below:

| Model | home | away | Target |
|---|---|---|---|
| poisson_v3 (existing team-level model) | -0.0252 | +0.0309 | ±0.01-0.02 |
| poisson_v4 (new player-driven model, as first built) | -0.0388 | +0.0632 | ±0.01-0.02 |

Both were already outside the target band, and the new player-driven model was worse than the one it was meant to improve on.


### The investigation

Fixing this took several rounds, and they were not equally important — worth being
explicit about which ones actually moved the numbers versus which were necessary cleanup
that didn't:

The first real finding was that the gap had **nothing to do with player data at all**.
Forcing every team to 0% player influence (pure team-level) reproduced almost the exact
same bad bias as the real player-blended model — proof the regression lived in shared
pipeline code, not in the new player-level logic. The actual cause: the club-league blend
pipeline had been built by reusing a component from the World Cup system's which had neutral-venue math
(`analyse_match_wc`), removing any home-field-advantage mechanism, plus a
team-rating lookback window (`n=25`) that didn't match the proven system's own window
(`n=10`). Restoring both got the new model back to *parity* with the old one — necessary,
but on its own it only matched the existing (still imperfect) team-level bias, it didn't
clear the target.

Chasing the residual bias further led to the actual breakthrough. The team-level rating
system estimates a team's scoring strength from its last 10 matches' *actual* goals —
a small, high-variance sample where a single hot or cold scoring stretch (found concretely
via Atalanta, whose last-10-away-games average was 2.5 goals against a true full-season
average of 2.2) swings the number a lot. The fix: derive team-level ratings from **xG
(expected goals) and xGA (expected goals against) instead of actual goals/goals-conceded**
— the same variance-reduction idea already used for player-level attack. xGA didn't
already exist as data, but it didn't need new collection either: a team's expected goals
allowed in a match is just the sum of the *opposing* team's players' already-imported xG
that match, so it was fully derivable from data already on hand. This was built as a
parallel, comparison-only path (`get_team_xg_ratings`, `--team-metric xg`) that never
touches `core.poisson_model` or poisson_v3 at all, specifically so poisson_v3 stays
available as a clean reference throughout. **This was the change that actually cleared
the bar** — by a wide margin over everything else combined.

One more consistency fix landed along the way (player-level defense switched from actual
goals-conceded to xGA, matching attack's existing xG preference) — worth doing on
principle, confirmed to be bias-*neutral* in the numbers. Included for completeness, not
because it moved the needle.

### The result

Signed bias vs. Betfair Exchange closing lines, before (restored-parity model, team
ratings from actual goals) vs. after (team ratings from xG/xGA):

| Season | Side | Before (goals-based) | After (xG-based) | Target |
|---|---|---|---|---|
| 2024-25 | home | -0.0127 | **-0.0051** | ±0.01-0.02 |
| 2024-25 | away | +0.0302 | **+0.0055** | ±0.01-0.02 |
| 2025-26 | home | -0.0224 | **-0.0073** | ±0.01-0.02 |
| 2025-26 | away | +0.0294 | **+0.0117** | ±0.01-0.02 |

Both sides, both seasons, now land inside the target band — the first time either bar
has cleared in this whole investigation. Absolute calibration and favored-side agreement
improved alongside the signed numbers (not just a cancellation artifact). Full 302-test
suite green throughout; poisson_v3 and `core.poisson_model` untouched.

**2026-08-02, later same day — ROI checked, still fails; bias fix adopted as the real
default anyway.** Ran ROI for `poisson_v4_teamxg` vs. Bet365 (the Success Criteria's
soft-book definition): negative at every threshold, both seasons (2024-25: -14.9% /
-20.0% / -21.9%; 2025-26: -9.5% / -5.9% / -2.0%). Does not clear the ROI bar. Decision:
ship the bias fix now regardless — `team_metric` (and `attack_metric`/`defense_metric`)
default flipped from "goals" to "xg" in `compute_club_player_strength.py`, so
`poisson_v4` itself (no special flag needed) now uses xG/xGA team ratings. 5 new tests
added for `get_team_xg_ratings`/`team_level_lambda`'s metric switch (had zero dedicated
coverage before this). Full suite: 307 passed. ROI stays open as its own follow-up,
deliberately not blocking this commit — see task 5 status below.

**Decision needed when picking this back up:** investigate the ROI miss. Unlike the bias
investigation, we don't yet have a lead theory for this one.

---

## Task list (agreed build order)

| # | Task | Status | Notes |
|---|------|--------|-------|
| 1 | Per-match schema + pipeline rework (Serie A) | **DONE** (2026-07-29) | `soccer_player_stats` reworked to per-match grain, `soccer_player_match_lineups` added, `api_match_id` mapping solved (team-pairing, not date). Validated end-to-end on 3 teams (Milan/Pisa/Roma, 380/380 matches mapped). 31 new tests added, full suite (285) green. |
| 2 | Blend-weight resolution table | **DONE** (2026-07-30) | `resolve_blend_weight(team_id, league, component, season)` in `compute_club_player_strength.py`. Per-team default = `data_coverage_score * roster_change_score` (product, both required), converted to this file's `w` convention; league-wide override (`LEAGUE_WEIGHT_OVERRIDES`, empty by default) takes precedence per component. Formula in `FEATURE-011_REQUIREMENTS.md` (Blend). 7 new tests, full suite (292) green. Not yet validated against real data — current DB only has 2025 (season) player-stats, so `last_season = season-1` has nothing to compute from yet and everything correctly falls back to pure team-level; real validation happens once 2025-26 data exists as "last season" for an actual 2026-27 computation. |
| 3 | Scale per-match import to full 20-team Serie A | **DONE** (2026-07-30) | All 380 matches imported (17683 stat rows, 17703 lineup rows, 762 API requests, well under the confirmed 120 req/min pace). Found and fixed a real bug this surfaced: `load_team_players()` was attributing a player's stats by their CURRENT team (`soccer_players.team_id`) instead of who they actually played for in each match — invisible with the 3-team subset (no mid-season transfers among them), but real once the full league's transfer activity was in view (29 players / 463 rows misattributed, e.g. Sebastiano Luperto's 23 Cagliari matches were being folded into Cremonese, leaving Cagliari's aggregate missing him). Fixed to attribute by match-time team (venue + `soccer_matches`); all 20 teams now land at the expected ~37-38k total squad-minutes ballpark. 1 new regression test + 5 existing tests updated (needed `venue` to resolve team attribution, which they hadn't needed before); full suite (293) green. |
| 4 | Per-league team-name matching + competition config: Premier League, Bundesliga, LaLiga, Ligue 1 | NOT STARTED | Confirmed real, per-league work (not reusable from Serie A) — see prototype log's cross-league spot check. Also need per-league competition search-term/id config (`LaLiga` gotcha). **Bigger dependency found 2026-07-30: `soccer_matches` has ZERO rows for any of these 4 leagues, and the only base match/team/result collector (`core/data_collector.py`) is hardcoded to Serie A via football-data.org — there's no generic multi-league importer yet.** This predates FEATURE-011 (it's the foundational team-level system's own data) but blocks this task: real chain is base match-data collection (not yet scoped anywhere) → this task's competition/team-name matching → the already-built import/compute pipeline (reusable as-is). Nothing else in this list depends on task 4 — it only blocks scaling to Europe, which can't start before task 5 clears anyway. |
| 5 | Success Criteria validation (Serie A) | **BIAS CLEARED, ROI NOT CLEARED** (2026-08-02) | Built `backfill_player_blend_predictions.py` (point-in-time correct: match-derived `squad_as_of_date` instead of the mutable `soccer_players.team_id`, `before_date` threaded through `load_team_players`/`get_league_averages`/blend-weight so no lookahead within the season being backtested — same discipline as BUG-008). Backfilled 2024-25 player-match data first (`resolve_season_id` bug found+fixed along the way: competition resolution was always using TheStatsAPI's CURRENT season regardless of `--season` requested — caught before it corrupted anything, see commit). Results, `poisson_v4` vs `poisson_v3`, season 2025: **signed bias WORSE** (home -0.025→-0.039, away +0.031→+0.063 vs Betfair — further from the ±0.01-0.02 target, not closer) despite better absolute calibration (mean_abs_diff ~0.11→~0.07) and favored-side agreement (74%→86%). **ROI: +1.3% at EV>0%** (vs poisson_v3's -3.9%, matches BUG-009's documented number) but **not robust** — flips to -5.1% at EV>5%, -2.0% at EV>10%. **2026-08-01: root cause confirmed, not player data** — re-ran with weight forced to 100% team-level for every team (`--weight-attack 1 --weight-defense 1`) and got nearly the same bias (home -0.035, away +0.067). The bug is in `team_level_lambda()`/`analyse_match_wc()` reuse from the World Cup (neutral-venue) pipeline, which has no home-field-advantage mechanism at all — not player-lambda dilution as originally guessed. See fix discussion. **Verdict: does not clear either bar as currently built.** Hard gate — Europe rollout (task 4) shouldn't start until this does. **2026-08-01: Success Criteria itself firmed up** (see `FEATURE-011_REQUIREMENTS.md`) after a post-break deep-dive found the as-written criteria was satisfiable by a broken model — a pooled (non-split) bias number and a single-threshold ROI check both would have shown poisson_v4 passing despite the split/multi-threshold view showing it's worse than v3 on every dimension. Criteria now explicitly bans pooling and requires ROI robustness across EV>0/5/10%, and defines ROI's EV against the soft book (Bet365), not the sharp book. Numbers above still stand (they were already measured the stricter way) — nothing to rerun, but see new loose thread on `soccer_betting_odds` book-mixing below before this ROI number is fully trustworthy under the new definition. **2026-08-02: bias bar CLEARED.** Root cause of the remaining bias was noisy small-sample team ratings (last-10-matches of ACTUAL goals) — fixed by deriving team-level attack/defense from xG/xGA instead (`get_team_xg_ratings`, derived free from already-imported player xG, no new data collection). Signed bias vs Betfair now inside ±0.01-0.02 both sides, both seasons (2024-25: home -0.0051, away +0.0055; 2025-26: home -0.0073, away +0.0117) — see STATUS block above for the full writeup. Adopted as the real `poisson_v4` default (not just a comparison flag). **ROI checked against this version, still fails** (negative at every EV threshold, both seasons) — open, no lead theory yet, next task. |
| 6 | Cadence automation (weekly refresh, staged validation, auto-promote/rollback) | NOT STARTED | Currently a one-off manual compute (`--print`/`--persist`), same shape as `compute_wc_team_strength.py`. Needs task 2 finished to run unattended. |
| 7 | Output (pick generation, card format, decision-trail logging) | NOT STARTED | Reuses `generate_wc_card.py`'s display layer; selection algorithm (guardrails/EV logic) is a separate decision, not assumed to be WC's tuned version. |
| 8 | Remaining scenarios: Scenario 2 (unavailable-player override), Scenario 3 (contributor reporting), Scenario 9 (coach-change override) | NOT STARTED | Lower priority — none of these are believed likely to threaten the Aug 15 date. Scenario 9 flagged explicitly (2026-07-30) as a real near-term concern — many Serie A (and likely other) teams had offseason manager changes — but deliberately kept OUT of the blend-weight formula (task 2); it's a separate trigger on the same "how much to trust team-level history" question. Don't forget it. |

## Loose threads (bugs/wrinkles, not full tasks)

- ~~`backtest_from_predictions.py` stakes against whatever `soccer_betting_odds` row exists per
  match, unfiltered by sportsbook.~~ **FIXED 2026-08-01**: now filters to `--sportsbook`
  (default Bet365, matching the Success Criteria's ROI definition) and recomputes EV inline
  against that book's own moneyline instead of trusting `soccer_model_predictions.ev_home` etc.
  (those were stored against whatever book the prediction backfill happened to join, not
  guaranteed Bet365 either). n drops from 380 to 350 matches (the 10 Pinnacle + 20 "User Book"
  matches are now correctly excluded rather than silently mis-staked). Corrected ROI, vs Bet365:
  poisson_v3 = -7.7%/-7.8%/-4.2% at EV>0/5/10%; poisson_v4 (post home/away fix) =
  -14.5%/-15.6%/-14.5% — worse than v3 at every threshold. Not investigated further yet (bias
  is the current priority). Separately: Bovada is NOT in `soccer_betting_odds` for Serie A at
  all — that book only appears in the World Cup system (`soccer_wc_odds`), a different table.
- Pisa's per-match import: 1835 stat rows vs. 1834 lineup rows for the same 38 matches — one-row mismatch, not investigated.
- Bundesliga/Ligue 1 `/teams` list returns 19/21 teams against a stated `total_teams: 18` for both — not investigated, flag before relying on either league's team list during task 4.
- Fiorentina vs Atalanta (match_id 373, 2026-05-24) has lineups but zero player-stats — confirmed via a direct API call that `matches/{id}/player-stats` genuinely returns nothing for this match (not an import bug on our end, a source-data gap). Explains why Atalanta/Fiorentina's total squad-minutes came in slightly lower than the other 18 teams after task 3. Not investigated further; low priority (1 match out of 380).
