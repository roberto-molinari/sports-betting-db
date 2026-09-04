# Known Issues / Bug Log

A running log of known model/data issues that are understood but deferred.
Newest first. When fixing one, update its **Status** (and remove from the active
set once shipped + verified). Format is deliberately lightweight.

Severity: **high** (materially wrong picks across many teams) ·
**medium** (distorts some teams/matches) · **low** (cosmetic / rare).

---

## BUG-025 follow-on, deliverable 1 of 2 — Serie A -> TheStatsAPI migration, step 1: stamp `thestatsapi_match_id` onto existing rows — **DONE 2026-09-04**

Per BUG-025's tracked follow-on (Serie A must migrate onto TheStatsAPI before
it can get the same non-2.5 totals validation the other 4 leagues will).
Backed up `sports_betting.db` first (`db_backups/`, gitignored) given this
writes to real historical data.

**Surprise found before writing anything:** assumed (from `core/leagues.py`'s
comment, "Serie A ... stays on football-data.org ... even though its SQUAD
data does come from TheStatsAPI under a different id") that Serie A's
`soccer_matches` rows had NO `thestatsapi_match_id` at all. Actually: seasons
2022-2025 (1,521 rows) already had one, apparently stamped by some earlier
process this session has no record of. Only season 2026 (the current
in-progress one, 380 rows) was genuinely unstamped.

**Independently re-verified all 1,521 already-stamped historical rows anyway**
(team-pairing + closest-date match against a fresh TheStatsAPI pull, plus a
score check for every completed match) rather than trusting them blind:
1,520 agreed exactly. **The one disagreement was a real wrong stamp**, not
noise -- match_id 6997 (Spezia v Hellas Verona, 2022-23 season) was stamped
to `mt_405804585` (2023-03-05, 0-0, a regular-season match), but our row's
own stored date/score (2023-06-11, 1-3) matches a DIFFERENT TheStatsAPI
match, `mt_012108033` (`stage_name: "final"`, matchday 29) -- the two teams
met twice that season with the same orientation, apparently a genuine
relegation-decider replay (this is also the "extra" 381st fixture noted in
season 2022's count). Corrected the stamp on match_id 6997 directly.

**Migration script:** `migrate_serie_a_thestatsapi_ids.py` (new) --
team-name map (9 Serie A teams need one, e.g. "AC Milan"->"Milan", the rest
match exactly) + closest-date match within a tolerance (Serie A's stored
dates drift up to 2 days from TheStatsAPI's real kickoff in a handful of
2025-26 matches -- a football-data.org quirk, every case checked was
score-identical) + a hard score check before stamping any completed match
(never silently trusts a date/team match alone). Defaults to dry-run;
`--apply` required to write, and refuses to write at all if it finds a score
mismatch or a reused TheStatsAPI id anywhere in the run. Ran `--apply`:
stamped 60 of season 2026's 60 currently-published fixtures; the other 320
found no confident match, which is expected, not a bug -- TheStatsAPI
publishes a season's fixtures progressively (confirmed live: Premier League/
Bundesliga/La Liga/Ligue 1 all show the exact same partial-current-season
pattern today, in both TheStatsAPI's own data and our existing DB rows).

**Deliberately NOT done here:** `core/leagues.py`'s `thestatsapi_competition_id`
for Serie A is still `None`. Registering it now, before this step is
verified stable, would be the actual risk -- `import_league_matches.py`'s
dedup is keyed purely on `thestatsapi_match_id`
(`find_existing_match()`), so any run for Serie A before every row is safely
stamped would treat unstamped rows as brand new and insert duplicates. That
registration + removing `season_kickoff.py`'s Serie A guardrail + retiring
`update_serie_a_results.py` is deliverable 2, tracked separately, not
executed yet.

## BUG-025 — `generate_club_league_card.py` silently skipped the totals market for any posted line other than 2.5 — real, current +EV opportunities never even got evaluated — **REVERTED 2026-09-01 pending real validation (see below); diagnosis and finding still stand**

- **Type:** missing market coverage (not a wrong number, a market never priced
  at all) · **Severity:** medium (live opportunities silently dropped, no
  error anywhere to notice by) · **Found:** 2026-09-01, investigating why
  Ligue 1's PSG v Monaco card came back with no picks and no guardrail-log
  detail at all -- traced to the match's posted total being 3.25, not 2.5.

**Root cause.** The 2.5-only filter (`abs(float(ou_line) - 2.5) > 1e-9` ->
`ou_line = None`, added when this script was first written 2026-08-07) was
never a deliberate market-scope decision -- it matched reality at the time:
Bet365's historical CSV data (football-data.co.uk, the only totals source
that existed then) posts a total of exactly 2.5 for 100% of rows across
every league/season checked (BUGS.md, 2026-08-07 totals-backtesting entry).
Once live odds (The Odds API) started flowing through `--future-only`, real
lines vary a lot -- checked live across all 5 leagues' matches since
2026-08-01: only ~34% of posted lines are 2.5 (69/200); the rest range
1.75-4.0. Every one of those non-2.5 matches had its totals market silently
skipped, with no log line indicating anything was excluded (a candidate that
was never built doesn't show up in the GUARDRAIL LOG, which only lists
candidates that existed and got excluded) -- this is also what made the
"why did PSG v Monaco come back empty" investigation confusing in the first
place: nothing was wrong with the 1X2 candidates that were shown, the
missing UNDER 3.25 candidate (p=0.653, EV +32.6%) never existed to begin
with.

**Initial fix (2026-09-01), since reverted.** Removed the 2.5-only filter --
`analyse_match_wc()`'s totals math (`totals_probs()` over the Poisson grid)
already handles any line correctly, and `core.grading.grade_pick()` already
parses an arbitrary line back out of the `"OVER <line>"/"UNDER <line>"` side
string (including integer-line pushes) -- neither the pricing math nor the
grading path was ever the limiting factor, only this one filter.
`build_candidates()` labeled totals candidates with whatever line was
actually posted (`format_ou_line()`: `3.0 -> "3"`, `3.25 -> "3.25"`) instead
of a hardcoded `"OVER 2.5"`/`"UNDER 2.5"`. Verified live: PSG v Monaco (line
3.25) surfaced `UNDER 3.25 | odds +103 | model p 0.653 | EV +32.6%`.

**Open validation gap.** Every piece of backtested evidence for the totals
market's edge (BUGS.md, 2026-08-07: +8.2% ROI 2025 / +0.5% 2024) was measured
ONLY at line 2.5 -- that's the only line that exists anywhere in the
historical Bet365 CSV data used for backtesting, across every league/season.
The initial fix meant the live card would generate real picks at lines the
model's totals edge had never been validated against (e.g. 3.25, 1.75).

**Early look at that question (2026-09-01).** soccer_model_predictions had
zero season-2026 rows (the backfill script had only ever been run for closed
historical seasons) -- backfilled all 5 leagues (poisson_v4_4, live-shipped
defaults) to get real predictions against this season's actual results so
far, then graded totals via backtest_from_predictions.py against Pinnacle
(not Bet365 -- the live Odds API import path stores this season's non-2.5-
line odds under Pinnacle/"User Book", zero under Bet365). Pooled across all
5 leagues, EV>0%, split by whether the posted line was 2.5:

| Line | Bets | Win rate | ROI |
|---|---|---|---|
| == 2.5 | 34 | 58.8% | **+17.5%** |
| != 2.5 | 42 | 45.2% | **-12.5%** |
| pooled | 76 | 51.3% | +0.9% |

Directionally exactly the concern above -- the 2.5 line still looks good,
non-2.5 lines look bad, split roughly breakeven pooled. **Not remotely
conclusive**: this is 3-4 weeks of one in-progress season, per-league n is
tiny (Bundesliga: 7 bets, one swing result moves ROI ±50%+).

**Found a much better path to real validation, same session:** TheStatsAPI's
`/matches/{match_id}/odds` endpoint (already the source for teams/matches/
squads/player-stats in 4 of 5 leagues) returns a FULL totals ladder per
match -- every half-goal line (0.5 through 7.5+) simultaneously, Bet365 odds,
confirmed live against real completed matches from last season. That means a
real multi-line, two-season historical backtest is possible: ~2,755
completed matches (Premier League/Bundesliga/La Liga/Ligue 1, seasons 2024+
2025, all already carry a thestatsapi_match_id) at one API call each, well
within the 120 req/min rate limit (~23 minutes total). Serie A is excluded --
it's still on the football-data.org pipeline with no thestatsapi_match_id.

**Decision (2026-09-01): REVERTED the fix.** Generating real live picks
against untested lines wasn't worth it given a real path to actually
validating this exists and is cheap to execute. Reverted
`generate_club_league_card.py`/its tests to the original 2.5-only behavior
byte-for-byte (confirmed via `git diff`) -- the live card is back to exactly
how it's behaved for the last few weeks. Two follow-on deliverables tracked,
each its own commit:
  1. Migrate Serie A onto the TheStatsAPI pipeline (the long-tracked
     fast-follow from the original multi-league expansion plan) so all 5
     leagues, not 4, can eventually get this validation and any other
     TheStatsAPI-only capability.
  2. Pull the TheStatsAPI odds ladder for the ~2,755 historical matches,
     build a real multi-line totals backtest, and only THEN re-enable
     non-2.5 lines in the live card if the numbers support it.

## BUG-024 — A source-side match reschedule (match_date CONFLICT) was detected but had no write path at all, even under `--allow-overwrite` — silently dropped a real, live Premier League match from the card window — **FIXED 2026-08-31**

- **Type:** missing write path (a whole conflict category was detectable but
  not actionable) · **Severity:** medium (a real match invisible to the live
  card, not a data-quality nuisance) · **Found:** 2026-08-31, user reported
  "there's a Premier League match today (Aston Villa v. Arsenal) but the
  model thinks there are zero matches."

**Root cause.** `soccer_matches` had match_id 17242 (Aston Villa v Arsenal)
dated `2026-08-29T14:00:00.000Z`. TheStatsAPI's real record is
`2026-08-31T19:00:00.000Z` (confirmed against the actual kickoff via web
search) — the source rescheduled the match at some point after our original
import. `import_league_matches.py`'s conflict detection caught this
correctly every run (`CONFLICT ... match_date stored=... api=...`), but the
apply branch under `--allow-overwrite` only ever called
`update_soccer_match_result()` — which writes score/status, not date — so a
match_date-only conflict (no score diff, since the match hadn't been played)
matched neither of that function's score-not-None guards and nothing was
ever written. The run still printed "applying" and counted `applied=1`, a
false positive that made the conflict look resolved when it silently
wasn't. Because `generate_club_league_card.py`/`matchday_summary.py` both
filter matches by date window, the match simply vanished from "today" with
no error anywhere.

**Fix.** Added `update_soccer_match_date()` to `core/sports_db.py` (mirrors
`update_soccer_match_result()`'s pattern). `import_league_matches.py`'s
conflict-apply branch now checks which fields actually differ and calls the
right updater per field (score via `update_soccer_match_result`, date via
`update_soccer_match_date`) instead of assuming every conflict is a score
correction. New test locks in that a match_date conflict actually persists.
Verified live: re-ran `--league "Premier League" --season 2026
--allow-overwrite`, match_id 17242's stored date is now
`2026-08-31T19:00:00.000Z`, and it now appears in `matchday_summary.py`'s
output for today.

**Sweep for other affected matches (same day, all leagues):** ran
`import_league_matches.py` (dry, no `--allow-overwrite`) for every
TheStatsAPI-sourced league/feeder division (Premier League, Bundesliga, La
Liga, Ligue 1, Serie B, Championship, 2. Bundesliga, LaLiga 2, Ligue 2) plus
`update_serie_a_results.py` (Serie A's separate football-data.org pipeline)
— **zero other conflicts found**. Aston Villa v Arsenal was the only match
affected.

## BUG-023 — `import_league_betting_odds.py --download --future-only` silently ignored `--download`, running the live-odds path instead with no warning — **FIXED 2026-08-28**

- **Type:** argument-validation gap (silent precedence, not an error) ·
  **Severity:** low (would surprise a user expecting the download path to
  run, no data corruption) · **Found:** 2026-08-28, while answering a user
  question about how `--download` and `--future-only` differ -- traced
  `main()`'s branch order and found `--future-only` (with no local files)
  returns early unconditionally, before `args.download` is ever checked.

**Fix.** `parse_args()` now rejects `--download` + `--future-only` together
(without local CSV files) outright, explaining they're two different
sources (football-data.co.uk vs. The Odds API) and pointing at using each on
its own. `--future-only` combined with local CSV files is unaffected (that's
a real, different code path -- a future-only filter over a local file's
rows) and still works as before.

## BUG-022 — `import_league_betting_odds.py --future-only` rejected as invalid unless `--download` or a CSV file was also given, even though `--future-only` is its own valid mode — **FIXED 2026-08-28**

- **Type:** argument-validation bug (upfront check didn't know about a real
  mode) · **Severity:** low (blocked a legit command, no data impact) ·
  **Found:** 2026-08-28, user ran `--league "Serie A" --future-only --season
  2026` (per TOOLS.md's own documented live-odds step) and hit `error:
  provide local CSV files or use --download.` — a real command failing
  exactly as documented.

**Root cause.** `parse_args()`'s upfront validation only recognized two
input modes (`--download` or local CSV `files`) and rejected everything
else, without knowing `--future-only` triggers a third, independent mode
(`import_odds_api()`, live odds from The Odds API) that needs neither. The
error message compounded this by suggesting `--download`, which pulls
historical football-data.co.uk CSVs, not live current-week odds — the wrong
fix even if followed.

**Fix.** `parse_args()` now accepts `--future-only` as satisfying the
input-mode requirement, and moved the existing "`--future-only` needs
exactly one `--season`" check (previously only enforced deep in `main()`)
up next to it so it fails fast with a clear message. Verified live:
`--league "Serie A" --future-only --season 2026` now runs and pulled 20 real
Pinnacle/1xBet odds rows for that weekend's matches.

## BUG-021 — Routine match completions logged as loud, all-caps `CONFLICT` -- read as an error by a first-time user for something entirely expected — **FIXED 2026-08-24**

- **Type:** UX / logging design (alarming language for a non-event) · **Severity:**
  low (no data or behavior was wrong, just confusing output) · **Found:**
  2026-08-24, user ran `club_league_scorecard.py --dry-run`, saw `CONFLICT`
  lines for two ordinary Ligue 1 matches that had simply finished, and asked
  "before fixing anything, tell me what went wrong" -- correctly suspecting a
  regression that, on inspection, wasn't one (see BUG-019 for the other half
  of that same report, which WAS a real, expected recurrence).

**Root cause.** `import_league_matches.py`'s conflict-safe-write logging
treated every detected diff the same way — printed as `CONFLICT ... stored=X
api=Y`, regardless of whether the diff was a genuine source disagreement (a
score changing on an ALREADY-completed match; a match_date changing —
a postponement) or just a still-`scheduled` match getting its real result for
the first time, which is the normal, everyday outcome of a refresh and not a
disagreement about anything.

**Fix.** New `is_routine_completion(existing, diffs)`: true only when the
diff set is exactly `{match_status, score}` AND the match was previously
`scheduled` (never `completed` before). Routine completions print nothing —
still applied under `--allow-overwrite` exactly as before, just silently —
and are counted in the run summary as `results_recorded=N` instead of
`conflicts=N`. A genuine conflict (correction or postponement) still prints
the loud `CONFLICT` line, unchanged. 4 new tests lock in the classification
boundary (first-time score, already-completed correction, postponement with
and without an accompanying first-time score).

## BUG-020 — `club_league_scorecard.py`'s refresh step detected newly-completed matches but never applied their scores, leaving real picks stuck ungraded — **FIXED 2026-08-23**

- **Type:** bug (missing flag threading) · **Severity:** medium (every league
  except Serie A silently failed to grade on schedule; discovered via 15
  picks reported "still ungraded" a full day after their matches finished) ·
  **Found:** 2026-08-23, user ran `grade_club_league_picks.py` directly (not
  the scorecard tool) and got 0/15 graded; tracing why led here.

**Root cause.** `club_league_scorecard.py`'s `refresh_results()` calls
`season_kickoff.import_fixtures()`, which shells out to
`import_league_matches.py` -- a script whose conflict-safe-write design
(see BUG-021 above) requires `--allow-overwrite` to actually WRITE a
detected `scheduled -> completed` transition, by default only reporting it.
`import_fixtures()` never passed that flag through, so every scorecard
"refresh" correctly fetched fresh results but silently discarded them.
Serie A's own sync path (`update_serie_a_results.py`) has no such gate — it
always applies — which is why only the 4 newer leagues were affected.

**Fix.** Threaded an `allow_overwrite` parameter through `import_fixtures()`
(default `False`, preserving `season_kickoff.py`'s own report-only bootstrap
behavior); `club_league_scorecard.py`'s call site now passes
`allow_overwrite=True`, since applying a newly-completed match's real score
is the entire point of "refresh." Confirmed idempotent and safe to call on
every run (only ever writes `home_score`/`away_score`; a no-op re-run is
counted `unchanged`). 4 new tests, including a regression test on
`refresh_results()` itself asserting the exact call arguments.

## BUG-019 — TheStatsAPI served two conflicting records for the same real Ligue 1 fixture (reversed home/away); silently ingested the wrong one — **FIXED + DETECTION ADDED 2026-08-23**

- **Type:** data quality (source-provider duplicate) · **Severity:** high (would
  have posted a HOME pick for a team that was actually AWAY) · **Found:**
  2026-08-23, when a live odds re-import reported the Rennes/PSG matchday-1
  fixture as "1 unmatched" after the user re-ran `import_league_betting_odds.py`.

**Root cause.** TheStatsAPI's `matches` endpoint returned TWO records for one
real fixture (Ligue 1 2026-27, matchday 1, same kickoff instant): `mt_022917220`
(Paris Saint-Germain home, wrongly flagged `is_neutral: true`) and
`mt_466109840` (Stade Rennais home, `is_neutral: false`, never previously
ingested). Our earlier backfill had picked up the bad one — `match_id 17397`
was stored with PSG as home. `import_league_betting_odds.py`'s `find_match_id()`
looks up matches by exact `(home_id, away_id)`, so The Odds API's (correctly
labeled) event couldn't match our (mislabeled) row and silently dropped as
`no_match` — the only visible symptom, hours away from the root cause.

**Verified externally** before touching anything: the match is played at
Roazhon Park (Stade Rennais's home ground), confirming Rennes is genuinely
home, PSG away — [ESPN's own gameId URL](https://www.espn.com/soccer/match/_/gameId/401876487/stade-rennais-paris-saint-germain)
encodes it as `stade-rennais-paris-saint-germain`.

**Fix.**
1. Corrected `soccer_matches.match_id=17397` directly: swapped
   `home_team_id`/`away_team_id`, repointed `thestatsapi_match_id` to the
   correct record (`mt_466109840`). Confirmed zero downstream rows (odds,
   predictions, picks) referenced the mislabeled row yet, so no further cleanup
   needed.
2. **Deliberately did NOT** make `find_match_id()` tolerant of a reversed
   home/away pair — that would have silently matched odds to a mislabeled
   match and posted a backwards pick (`generate_club_league_card.py`'s output
   format is literally `HOME | AWAY`). The fix belongs in the data, not the
   lookup.
3. Added real, permanent guardrails so this class of problem surfaces at
   import time instead of days later via a confusing downstream symptom:
   - `import_league_matches.py`: `find_conflicting_pairing()` — a NEW
     `thestatsapi_match_id` for a team pairing we already have a DIFFERENT
     match id for, within 1 day, is flagged loudly (`DUPLICATE FIXTURE`, both
     records' details printed) and NOT imported. Confirmed live: re-running
     against Ligue 1 2026 now catches the stale `mt_022917220` record on every
     future sync instead of silently ignoring it.
   - `import_league_betting_odds.py`: `no_match`/`unknown_team` counts (both
     the CSV and live-Odds-API paths) now print the specific team names/date
     that failed to resolve, instead of only a silent aggregate count.

## BUG-018 — Backfill scripts insert one prediction row PER `soccer_betting_odds` row, so a multi-book match gets duplicate predictions, double-counting it in every downstream metric — **FIXED + DATA CLEANED 2026-08-20**

- **Type:** bug (bare join, wrong cardinality assumption) · **Severity:** medium
  (30 Serie A 2025 matches double-counted in every Brier/ROI query, across the
  live tag AND all 19 kept-for-comparison method tags) · **Found:** 2026-08-20,
  during the BUG-017 re-backfill audit (Serie A 2025 inserted 410 rows for 380
  matches).

**Root cause.** `backfill_player_blend_predictions.py`/`backfill_soccer_model_
predictions.py` selected matches via a bare `JOIN soccer_betting_odds`, assuming
one odds row per match (an assumption `backtest_from_predictions.py`'s docstring
even stated outright). It went stale once Bet365 coverage was completed over
matches that already had Pinnacle/"User Book" rows: 30 Serie A 2025 matches carry
two books, so the backfill loop processed them twice — two prediction rows with
IDENTICAL probabilities (same matchday `compute()` result) but different stored
ev/moneyline metadata. `generate_club_league_card.py` had the same bare join: a
doubly-priced upcoming match was processed once per book, the second pass
silently replacing the first's stored picks (FEATURE-016) with picks priced off
whichever odds row sorted last.

**Fix (all three call sites, same subquery):** join through
`o.odds_id = (SELECT ... ORDER BY (sportsbook='Bet365') DESC, odds_date DESC,
odds_id DESC LIMIT 1)` — one odds row per match, preferring Bet365 (the soft-book
reference every ROI criterion is defined against), newest otherwise.
`backtest_from_predictions.py` needed no query change (its sportsbook filter
already guarantees one row per match); its stale docstring claim was corrected.

**Data cleanup:** Serie A 2025 `poisson_v4_4` re-backfilled with the fixed query
(410 -> 380 rows). The 19 historical method tags were DEDUPED in place (570 rows
deleted, keeping the first row per match/method) rather than re-run — re-running
them under 2026-08-20's data would have changed them (BUG-017), destroying their
value as historical records; the duplicate rows' probabilities were verified
identical in every case first, so the deletion is lossless (only unused stored
ev/odds metadata differed; every backtest recomputes EV from soccer_betting_odds
directly). New data-integrity test pins the invariant
(`test_no_duplicate_model_prediction_rows`); full suite green (478 unit + 14
data-integrity).

---

## BUG-017 — Importing new historical data silently stales already-backfilled `soccer_model_predictions` rows — **FOUND + REMEDIATED (re-backfill) 2026-08-20; no structural guard yet**

- **Type:** process/data-freshness gap · **Severity:** high for any analysis run on
  the stale rows (Serie A 2022 p_home shifted by up to **0.43** between the stale
  and fresh computation) · **Found:** 2026-08-20, while validating the newly
  imported 2022-2023 history (all 5 leagues + feeders back to season 2022).

**Finding.** `soccer_model_predictions` rows are point-in-time correct *with
respect to the data present when the backfill ran* — nothing marks them stale when
a LATER import adds data those predictions would have used. The 2026-08-20 history
import (2022+2023 for Premier League/Bundesliga/La Liga/Ligue 1, plus feeder
divisions and Serie A player/odds gaps back to 2022) changed the inputs of 12
already-backfilled league-seasons: the season-blind last-10 player/team windows of
early-2024 matchdays now reach into the newly imported 2023 seasons; Serie A
2022/2023 (backfilled 2026-08-18 from partial data) changed outright. Confirmed by
row-level recomputation: leagues backfilled AFTER the import reproduce to ~1e-16;
Serie A 2022 (backfilled before it) differs up to 0.43 in p_home.

**Remediation (done 2026-08-20):** re-ran `backfill_player_blend_predictions.py
--method poisson_v4_4` for the 12 stale league-seasons (Serie A 2022-2025 + the
four other leagues' 2024/2025). `generated_at` is the tell for staleness: any
prediction row older than the latest data import is suspect. The refresh didn't
just restore consistency — it measurably IMPROVED the 2024/2025 predictions
(deeper history now behind their early-season windows): calibration slope
0.989 -> 1.020, corr-with-Pinnacle 0.830 -> 0.840, home-Brier 0.2141 -> 0.2125
on the same n=3,030 sample.

**Open (structural guard, not built):** nothing prevents a recurrence — options
when it next matters: a `data_version`/import-timestamp check in the metrics
scripts (warn when predictions predate the newest relevant import), or folding
"re-backfill all method tags" into the season-kickoff checklist (FEATURE-019).

---

## WATCH — 2022 is a cold-start burn-in season: first ~3 months badly overconfident, and no shipped lever fixes it (investigated 2026-08-20, no code change)

The 2022 season is the earliest data in the DB, so its opening months have no
prior history behind the season-blind windows at all — a condition no other
backtest season has. Measured on `poisson_v4_4` vs Pinnacle closing:
first-3-months-of-2022 calibration slope **0.385** with model logit sd 0.961 vs
market 0.858 (the model sprays MORE spread than the market on a fraction of the
information); rest of 2022 recovers to slope 0.888; 2023 onward is normal.

Isolation probes (in-memory recomputation of the early-2022 slice, no DB writes):
team-level shrinkage `TEAM_RATING_PULL_TOWARD_AVERAGE_MATCHES` k=5 improves it
only marginally (slope 0.450 -> 0.488, Brier 0.6470 -> 0.6446 3-class), k=10 is
worse; disabling the player stretch, the team xG stretch, or both barely moves it;
pure team-level (`w=1.0`) is WORSE (slope 0.337) — the player blend genuinely
helps here. Conclusion: the badness is an information deficit inherent to the
boundary of the dataset, not a mis-set constant — same lesson as BUG-009's
re-diagnosis: no output-side constant can substitute for missing information.

**Practical consequence — handled in the metrics tool (2026-08-20, later):**
`model_metrics_report.py` now excludes matches before **2022-11-01** from EVERY
metric it reports (Brier, bias, ROI, both markets) via `METRICS_MIN_MATCH_DATE`,
making 2022 a deliberate partial season — the report only ever grades predictions
built off real prior history, and its header states the scope. This is a
reporting scope, not a model change: backfills still cover full seasons, and
`backtest_from_predictions.py`'s own CLI still grades whole seasons (its
`min_match_date` parameter defaults to None; the report passes the cutoff
explicitly, same pattern in `compare_model_vs_market_odds.fetch_pairs`). No
model-side fix shipped: a `k_eff = window_size - n` "window-fill" shrink was
considered (exact no-op on full windows) but the flat-k probe caps its plausible
gain at ~0.04 slope — real fixes would need external pre-2022 history that
doesn't exist in the DB.

---

## BUG-016 — Matchday grouping used the exact match_date timestamp, not calendar date: a later same-day kickoff's rating computation could see an earlier same-day match's already-finished result — **FIXED + SHIPPED 2026-08-15 as `poisson_v4_1_1`**

- **Type:** bug (correctness, not just perf) · **Severity:** high on individual
  matches (majority affected in leagues with dense same-day scheduling, shifts
  up to 6 points of win probability), **negligible net effect measured
  pooled/per-league** (see below — the leak is noise-like per match, not a
  directional bias, so it washes out in aggregate). **Status:** fixed and
  re-backfilled as `poisson_v4_1_1`; this is now the reference baseline for
  ongoing BUG-012 Stage 2 work (superseding `poisson_v4_1` for that purpose).

**Found while investigating a performance question** (why BUG-012 Stage 2 sweeps
take so long): `backfill_player_blend_predictions.py` and its 3 siblings
(`backfill_with_xg_stretch.py`, `generate_club_league_card.py`,
`oracle_roster_blend_test.py`) all process matches "one matchday at a time" via
`itertools.groupby(rows, key=lambda r: r["match_date"])` — grouping by the
*exact* `match_date` string, not the calendar date. For TheStatsAPI-sourced
leagues, `match_date` carries a full kickoff timestamp, so two matches on the
same Saturday at 15:00 and 17:30 land in *different* groups, each triggering
its own full-league `compute()` call with its own group key as `before_date`.

**Two consequences, one perf, one correctness:**
1. **Perf (the original motivation):** the "group same-date matches to avoid
   redundant recomputation" optimization barely fires for these leagues.
   Pooled across the 5 leagues x 2 seasons used in BUG-012's sweeps: 2,535
   `compute()` calls today vs. 1,159 if grouped by calendar date — a ~2.2x
   cut confirmed by timing (`compute()` measured at ~0.25s/call for a 20-team
   league).
2. **Correctness (found while verifying the perf fix is a true no-op — it
   isn't):** `compute()`'s `before_date` argument gates every SQL query
   pulling historical data via `match_date < before_date`, string-compared.
   A later same-day match used its own late kickoff timestamp as `before_date`
   — so an *earlier* same-day match (different teams, already finished, real
   score already in the database by the time the later match is being
   "predicted" in a backfill) satisfies `match_date < before_date` and gets
   pulled into the shared league-wide baseline (`avg_home`/`avg_away`, league
   attack/defense means) that every team's rating gets recentered against for
   that `compute()` call. On a day with several matches, that's a real chunk
   of the round's own results leaking into that same round's own predictions.

**Confirmed empirically** — in-memory comparison (no DB writes) of predictions
under the old exact-timestamp grouping vs. calendar-date grouping, same code
otherwise, Stage 1 near-no-op recency defaults so decay isn't a variable:

| League 2025 | matches | identical | differ | max diff | median diff (of those that differ) |
|---|---|---|---|---|---|
| Bundesliga | 306 | 81 (26%) | 225 (74%) | 0.060 | 0.0006 |
| La Liga | 380 | 130 (34%) | 250 (66%) | 0.021 | 0.0022 |
| Serie A | 410 | 206 (50%) | 174 (42%) | 0.024 | 0.0029 |

The differing-match count lines up almost exactly with "every match except the
first one of its calendar day" per league — consistent with the leakage
mechanism above, not a fluke. Not a rare edge case: most Bundesliga matches in
this sample were affected.

**Fix implemented:** added `match_calendar_date(match_date)` (truncates to
`YYYY-MM-DD`) and `matches_on_date(rows, date)` to `compute_club_player_
strength.py`, and switched all 4 call sites above from `itertools.groupby` on
the exact timestamp to grouping by calendar date, passing the bare date
string (not any single match's own timestamp) as `before_date`. A bare date
is a strict string-prefix of any same-day full timestamp, so `match_date <
before_date` now excludes *every* match on that calendar day uniformly
(matches the existing live-CLI convention, `compute()`'s own `__main__` already
used `date.today().isoformat()` with no time component). Tests added in
`tests/test_compute_club_player_strength.py`; full suite green, ruff clean.

**Shipped as `poisson_v4_1_1`** — re-backfilled all 5 leagues x 2024/2025 with
the identical config `poisson_v4_1` used (no other flags changed), the only
difference being the corrected grouping. Real before/after, pooled and per
league (Brier / ROI @0/5/10% EV, Bet365):

| | Brier | ROI @0/5/10% |
|---|---|---|
| ALL-UP old (`poisson_v4_1`) | 0.6033 (n=3533) | -7.4 / -7.2 / -8.6% |
| ALL-UP new (`poisson_v4_1_1`) | 0.6031 (n=3533) | -7.6 / -7.2 / -8.8% |
| Serie A | 0.6073→0.6072 | -9.6/-8.9/-9.0% → -9.6/-8.9/-9.8% |
| Premier League | 0.6112→0.6109 | +3.2/+3.9/+2.6% → +2.4/+4.2/+3.1% |
| Bundesliga | 0.6066→0.6064 | -17.8/-18.2/-17.0% → -18.3/-18.1/-17.1% |
| La Liga | 0.5922→0.5919 | -12.2/-13.1/-16.0% → -11.7/-13.4/-17.1% |
| Ligue 1 | 0.5987→0.5987 | -1.2/-1.4/-4.8% → -1.5/-1.2/-3.8% |

Every league moved by <0.001 Brier and roughly ±1.5pp ROI at most, with no
consistent direction across leagues or EV thresholds. **Interpretation:** the
per-match leakage confirmed above is real (up to 6pp on individual matches),
but it pushes each affected match's prediction up or down depending on
whether that day's earlier same-day results happened to be high- or
low-scoring — noise, not a systematic bias — so across ~3,500 matches it
washes out almost entirely. Still worth fixing: a specific live card
generated on a day with an earlier same-day kickoff can get a meaningfully
wrong number for that one match, even though it doesn't move the season-long
scorecard. Given the negligible pooled/per-league shift, the BUG-012 Stage 2
sweep (6 candidates run against the old, leaky `poisson_v4_1` baseline) was
judged not worth re-running from scratch — the corrected baseline is close
enough that the existing sweep's candidate ranking almost certainly holds.

**Not yet done:**
- **User's follow-up idea, not yet designed or implemented:** calendar-date
  grouping stops same-day leakage, but doesn't stop a match played a day or
  two off the "core" round (e.g. a Friday match ahead of a Saturday/Sunday
  round) from counting as history for that round's Saturday/Sunday matches,
  even though it's arguably the same round, not genuinely "earlier." User's
  framing: the model should arguably look back to the *previous matchday*
  (round), not just the previous *calendar day*. Worth a real design pass (how
  is "matchday/round" even defined when there's no explicit round number in
  the schema?) rather than folding in ad hoc.

## BUG-015 — Bundesliga's guardrail ROI (~-19%) is far worse than every other club league despite a normal Brier score — **INVESTIGATED 2026-08-14, no fix; most likely real variance in a small (2-season) sample**

- **Type:** investigation (no code change) · **Status:** concluded for now, not
  actionable. Found while running BUG-012's Stage 2 calibration sweep: every
  half_life/cutoff/shape candidate tried (exponential and linear decay, flat
  weighting, half-lives 15-120d, cutoffs 45-270d) underperformed the `poisson_v4_1`
  baseline on Bundesliga 2024+2025, prompting "is Bundesliga just bad, independent
  of BUG-012" — confirmed yes, by a wide margin (`poisson_v4_1`, guardrail EV
  0/5/10%, Bet365):

  | League | Brier | ROI @0/5/10% |
  |---|---|---|
  | Serie A | 0.6073 | -5.3 / -6.4 / -7.9% |
  | Premier League | 0.6112 | +6.0 / +8.0 / +7.4% |
  | Bundesliga | 0.6066 | **-19.0 / -19.0 / -17.1%** |
  | La Liga | 0.5922 | -8.1 / -8.7 / -9.4% |
  | Ligue 1 | 0.5987 | -0.2 / -1.0 / -4.4% |

  Bundesliga's Brier is fine (comparable to or better than Serie A/Premier
  League) -- this isn't a probability-calibration problem in the aggregate.
  ROI is uniquely bad, and unlike every other league, **every one of the three
  bet sides loses money** (home -27.6%, draw -22.0%, away -9.8%, guardrail
  pooled) -- every other league has at least one clearly profitable side.

  **Ruled out, one at a time (each independently checked, no smoking gun in
  any of them):**
  - Odds coverage: 306/308 matches per season have Bet365 odds (2 missing =
    postponed/incomplete, same pattern other leagues would show).
  - Odds format/overround: Bet365's average bookmaker margin is ~1.055 for
    all 5 leagues, Bundesliga included -- no scaling/format bug.
  - Odds distribution shape: mean/sd of implied home/draw/away probabilities,
    checked against BOTH Bet365 and Betfair Exchange closing lines, are
    essentially identical across all 5 leagues -- Bundesliga's odds aren't
    unusually spread out, concentrated, or shifted.
  - Player/team blend mix: Bundesliga runs 75.1% team-level / 24.9%
    player-level (via `resolve_blend_weight`, point-in-time correct,
    `roster_as_of_date`-driven, same computation the real backfill uses) --
    right in the middle of the pack (70.9%-75.8% across all 5 leagues).
    Premier League actually leans slightly MORE team-level and has the best
    ROI, ruling out "too player-reliant" as the story.
  - Team-name mapping: every one of the 20 real top-flight Bundesliga teams
    has complete Bet365 odds coverage (68/68 or the full season count) -- no
    silently-dropped or misattributed team. **Found one real, harmless gap**
    while checking: `core/team_name_maps.py`'s Bundesliga dict is missing
    entries for SC Paderborn 07 / SV 07 Elversberg (the two 2. Bundesliga
    opponents from that season's top-flight relegation playoff -- 1. FC
    Heidenheim vs. Elversberg 2024-25, VfL Wolfsburg vs. Paderborn 2025-26).
    Confirmed harmless: those 4 matches have zero `soccer_betting_odds` rows
    and zero `soccer_model_predictions` rows, so they never enter the graded
    backtest sample at all. Worth a one-line fix later so it doesn't bite if
    relegation-playoff odds are ever ingested -- not done now, out of scope.
  - Unicode/encoding: verified via raw byte inspection (`hex(name)`) that
    every diacritic-bearing Bundesliga team name (Köln, München, Nürnberg,
    Saarbrücken, Düsseldorf, Preußen Münster) is stored as correct,
    precomposed (NFC) UTF-8 -- no mojibake, no NFC/NFD mismatch. No duplicate
    team rows anywhere in `soccer_teams` for Bundesliga/2. Bundesliga. Also
    checked (prompted by the "1." prefix on club names like "1. FC Köln,"
    itself a genuine German naming convention, not a data artifact --
    historically means "first" sports club founded under that name/city):
    `import_club_squads.py`'s `normalize_team_name()` is a regex built
    specifically for Italian club-name conventions (strips "AC"/"AS"/"US"
    prefixes, "Calcio"/"CFC"/"FC" suffixes, trailing year numbers) and,
    applied to German names, does mangle them in unintended ways ("1. FC
    Köln" -> "1. köln", dropping "FC" from the *middle* because "1." creates
    the leading-whitespace pattern the suffix regex looks for; "VfL Bochum
    1848" -> "vfl bochum", dropping the year). This was already flagged as a
    known risk in the multi-league expansion plan ("confirmed NOT to
    generalize past Serie A") but never revisited. Checked for real damage:
    across all Bundesliga + 2. Bundesliga team names, **zero collisions**
    (no two distinct real clubs fold to the same mangled string), so it
    isn't silently merging any two teams' squad/player data. Still flagged
    as fragile tech debt worth a real fix later, just not the cause here.

  **The "does the model overrate win probability" check needed the right
  scope to show anything.** Checked model win-probability vs actual outcome
  across EVERY match: Bundesliga's gap (+0.0104 pooled) wasn't unusual --
  Premier League's was actually larger (+0.0147) despite having the best ROI
  of any league. Re-checked restricted to ONLY the matches where a bet
  actually cleared EV>0 + the guardrail floor (`CLUB_LEAGUE_MIN_PICK_
  PROBABILITY`) -- this is the metric that actually discriminates:

  | League | bets | model's avg win prob (selected) | actual win rate (selected) | gap |
  |---|---|---|---|---|
  | Serie A | 552 | 49.2% | 38.4% | +0.107 |
  | Premier League | 543 | 51.9% | 43.5% | +0.085 |
  | Bundesliga | 459 | 50.9% | 36.4% | **+0.146** |
  | La Liga | 570 | 50.5% | 39.5% | +0.110 |
  | Ligue 1 | 493 | 54.4% | 44.2% | +0.102 |

  Bundesliga's selected-bet gap is the largest of all 5 leagues, and **the
  gap ranking exactly matches the ROI ranking** across every league (worst to
  best: Bundesliga > La Liga > Serie A > Ligue 1 > Premier League, both
  metrics, same order). Confirms the earlier all-matches check was just
  measuring the wrong population -- most of a league's matches never become
  bets, so their calibration doesn't drive ROI.

  **Per-Bundesliga-team breakdown** (same selected-bets-only gap, by team)
  showed the pattern holds broadly but isn't confined to promoted/weak teams:
  VfL Bochum 1848, 1. FC Union Berlin, FC St. Pauli, Hamburger SV, and 1. FC
  Köln have the largest gaps and worst ROI (-52% to -74%), but Bayer 04
  Leverkusen (n=34 bets, the 2023-24 Bundesliga CHAMPION, not remotely a weak
  or promoted club) also shows a large gap (+0.206) and bad ROI (-24.2%) --
  ruling out "only thin-history promoted teams" as the full story. Spot-
  checking Leverkusen's actual bet log found 12 of its 19 losing bets were
  DRAWS (not defeats), several at high model confidence (p=0.65-0.88) --
  prompted checking draw-probability calibration specifically, but this did
  NOT discriminate Bundesliga either: restricted to selected bets, Bundesliga's
  draw-underestimation (-0.0375) was close to Serie A's (-0.0365) and Premier
  League's (-0.0354) -- every league underestimates draws by a similar amount
  on its selected bets, so this is a real but leaguewide (not Bundesliga-
  specific) pattern, not the differentiator.

  **Dispersion (spread of the model's own probabilities) also ruled out**,
  checked both across all matches and restricted to selected bets, against
  BOTH Bet365 and Betfair Exchange. Across all matches, the model's own
  p_home is consistently more spread out than either book in EVERY league
  (not Bundesliga-specific), and the size of that model-vs-market dispersion
  gap doesn't track ROI (Ligue 1's gap is the largest of all 5 leagues despite
  a much better ROI than Bundesliga's). Restricted to selected bets, sd is
  essentially identical across every league for the model AND both books
  (0.16-0.17 across the board) -- no outlier at all once scoped correctly.

  **The decisive check: is it just the model, or is the market wrong too?**
  On the same selected-bet matches, compared model/Bet365/Betfair's own gap
  vs the actual outcome:

  | League | model gap | Bet365 gap | Betfair (sharp) gap |
  |---|---|---|---|
  | Serie A | +0.107 | +0.022 | -0.007 |
  | Premier League | +0.085 | -0.014 | -0.033 |
  | Bundesliga | +0.146 | +0.062 | **+0.039** |
  | La Liga | +0.110 | +0.018 | -0.001 |
  | Ligue 1 | +0.102 | +0.001 | -0.023 |

  Bundesliga is the ONLY league where even the SHARP book (Betfair Exchange,
  a real-money efficient market) is overconfident on these specific matches --
  every other league's sharp-book gap is at or below zero. And critically,
  the model's OWN incremental error beyond what the sharp market already got
  wrong (model gap minus Betfair gap) is actually the SMALLEST of all 5
  leagues for Bundesliga (+0.107, vs +0.111 to +0.125 elsewhere) -- the model
  isn't uniquely bad at reading Bundesliga relative to the sharp market. What's
  different is that the sharp market itself got surprised more often on
  exactly these Bundesliga matches than on any other league's selected bets.

  **Conclusion: most likely genuine variance in a small sample, not a
  discoverable pipeline bug.** Exhausted the checkable candidates (odds
  coverage/format/distribution, blend mix, team-name mapping/encoding, draw
  calibration, dispersion) with nothing found strong enough to explain the
  gap, and the one signal that DOES discriminate Bundesliga (the selected-bet
  win-probability gap) shows up even in the SHARP market's own numbers on the
  same matches, not just the model's -- consistent with "this specific
  612-match, 2-season window of Bundesliga results happened to run more
  upset-heavy than even Betfair Exchange priced for," not "our pipeline is
  broken for Bundesliga." No code changes made. Revisit once more Bundesliga
  seasons accumulate -- 2 seasons/612 matches is a small sample for separating
  real mispricing from variance, and this conclusion could look wrong with
  more data.

---

## BUG-014 — Spread-stretch recentering (additive) has no floor: can silently produce a negative attack/defense rating, masked by an unrelated hardcoded final-lambda clamp — **FIXED 2026-08-14 (shipped as poisson_v4_1)**

- **Type:** bug (correctness + missing test coverage) · **Status:** in progress.
  Found 2026-08-14, same walkthrough as BUG-013, investigating the negative
  `home_attack_player`/`home_attack_blend` values (-0.24) the user spotted for
  1. FC Köln vs. SC Freiburg (2025-08-31, Bundesliga). User: "some formula problem
  in whatever computes `*_attack_player` and `*_attack_blend` (and maybe the
  defense variants too) that allowed a negative value without returning/flagging
  an error. Also missing tests on this one."

**Root cause.** The player-level spread-stretch recentering in `compute()`
(`compute_club_player_strength.py`, ~line 1328):
```
r["ra"] = attack_mean + (r["ra"] - attack_mean) * player_spread_stretch_attack
```
has no protection against overshooting past zero when a team's raw pre-stretch
rate sits far enough below the league mean. Confirmed concretely: Köln's raw
player attack rate as of 2025-08-31 was 0.0807 vs. a league `attack_mean` of
0.1911; with `PLAYER_RATING_SPREAD_STRETCH_ATTACK = 2.0` (locked in earlier this
session), `0.1911 + (0.0807 - 0.1911) * 2.0 = -0.0296` — negative. This propagates
through the `avg_home/attack_mean` unit conversion into a negative
`lambda_attack_player_home`, and because Köln's blend weight was `w=0.0` for this
match (fully player-level — see BUG-012's walkthrough example, same match), it
passes straight through into a negative `home_attack_blend` too. Nothing in
`compute()` or the blend path notices or flags this.

**Where it's currently masked, not fixed.** The negative value isn't caught until
`analyse_match_wc`'s hardcoded floor (`core/poisson_model.py:556-557`):
```
lambda_H = max(lambda_H, 0.1)
lambda_A = max(lambda_A, 0.1)
```
— which clamps the FINAL combined lambda (attack × opponent-defense / baseline),
not the broken attack/defense component itself. This means: (a) a genuinely
negative intermediate rating exists silently, with no error/warning anywhere in
the pipeline; (b) the clamp forces an arbitrary flat 0.1, which has no
relationship to how weak the team's attack actually is — indistinguishable
between "genuinely weak" and "broken/negative" — and 0.1 expected goals is not a
realistic value for any real match. Confirmed in production: **exactly 3 of
~3,548 `poisson_v4` predictions across all 10 currently-backfilled league-seasons
hit this floor, ALL in Bundesliga 2025, ALL involving 1. FC Köln** (home attack
once, away attack twice): RB Leipzig vs. Köln (2025-09-20, actual 3-1), VfL
Wolfsburg vs. Köln (2025-09-13, actual 3-3), Köln vs. SC Freiburg (2025-08-31,
actual 4-1) — the last of these was already sitting in the Bundesliga bucket-2
outlier list (agg_delta 3.43) found the day before.

**Defense side doesn't currently manifest this** — `PLAYER_RATING_SPREAD_STRETCH_
DEFENSE` is presently locked at `1.0` (a true no-op, per this session's earlier
calibration work), so no defense rating can be pushed past its mean at all right
now. Confirmed live: scanning all Bundesliga 2025 teams for this same date, only
Köln's ATTACK goes negative post-stretch; no team's defense does. But the formula
itself has the identical, unguarded vulnerability and would trigger the same way
the moment `PLAYER_RATING_SPREAD_STRETCH_DEFENSE` is ever recalibrated away from
1.0 — this is latent, not defense-specific-safe by design.

**Missing test coverage (user's own framing):** no test asserts that a
player-level (or team-level) attack/defense rating stays non-negative after the
spread-stretch transform, and nothing catches a negative rating being silently
absorbed by the unrelated `analyse_match_wc` final-lambda floor instead of being
caught at its actual source.

**Design resolved 2026-08-14: switch additive stretch to multiplicative.**
Discussed and rejected clamping the output at an arbitrary floor (0.0 or
otherwise) — a clamped value doesn't represent anything the model actually
believes about the team, it's just where broken math happened to land before
going negative; the model's real (pre-stretch) belief for Köln was 0.0807, not
whatever floor got chosen. Landed on a different fix entirely: additive
recentering (`mean + (raw - mean) * factor`) is the textbook way to increase
dispersion around a fixed mean, which is genuinely what the stretch is FOR
(compression correction) — the bug is that this technique assumes the quantity
can range over the whole real line, when a goal-scoring rate has a hard floor at
zero. **This project already learned this exact lesson once before**, in a
neighboring computation (see `compute()`'s player-level home/away unit
conversion, BUG-009 2026-08-09: "keep the raw sample's RATIO spread... previously
additive, an unexplained asymmetry with defense's own multiplicative form...
ratio is correct: it can't drive lambda negative the way a flat shift can") — it
just never got carried over to the spread-stretch step specifically. Fix:
`stretched = mean * (raw / mean) ** factor` — below-mean values shrink toward
(never past) zero, above-mean values grow, exact no-op at raw == mean, only
reaches exactly 0 if raw itself is already 0.

**Scan found this same additive defect in 4 places, not 2** (searched the whole
repo for the pattern): `team_level_lambda`'s `xg_spread_stretch_attack/_defense`
(compute_club_player_strength.py), `compute()`'s
`player_spread_stretch_attack/_defense` (2 sites, same file), and
`compute_wc_team_strength.py`'s attack/defense normalization (same shape,
different parameterization — targets a specific standard deviation rather than a
fixed factor). **World Cup explicitly OUT of scope for this fix** — separate
product area, own tuning history; left as a documented follow-up, not touched.
(`backfill_with_xg_stretch.py` also has a copy, but it's a standalone sweep tool,
not production — not touched either.)

**Staged rollout, explicitly NOT all at once** (user's call — wants to measure
impact one call site at a time, not switch every site to multiplicative in one
shot): built one shared function, `spread_around_mean(raw, mean, factor, mode)`
(compute_club_player_strength.py, right before `raw_team_strength`), with
`mode="additive"` (today's formula, byte-identical) and `mode="multiplicative"`
(the fix). Unit-tested directly (7 new tests: additive matches the old inline
formula exactly, additive can still go negative — documents the known defect,
not a desired behavior — multiplicative can't, no-op at raw==mean, only reaches
0 at raw==0, None/non-positive-mean handling, unknown-mode rejection).

**Stage 1 (DONE 2026-08-14): wire all 3 club-league call sites through the
shared function in `mode="additive"` — verified true no-op.** All 423
previously-passing tests (including the exact-value assertions on the stretch
formula, which would catch any drift) still pass unchanged. The two
negative-value regression tests
(`test_compute_player_spread_stretch_cannot_push_attack_negative`,
`test_team_level_lambda_stretch_cannot_push_attack_negative`) are marked
`@pytest.mark.xfail(strict=True)` for now, with a reason string pointing back
here — expected, since no call site has switched to multiplicative yet; removing
each marker is literally the acceptance criterion for that site's own stage.

**Stage 2 (DONE 2026-08-14): all 4 club-league call sites now `mode=
"multiplicative"`.** Switched player-level attack first (the site that
originally surfaced this bug via Köln), then player-level defense (currently a
true no-op in production either way — `PLAYER_RATING_SPREAD_STRETCH_DEFENSE=
1.0` — but switched for consistency and to protect it whenever that constant
is ever tuned away from 1.0; added its own negative-value regression test,
`test_compute_player_spread_stretch_cannot_push_defense_negative` — first
draft of that test accidentally passed even under additive mode because TeamB's
300 raw minutes didn't clear the 300-WEIGHTED-minutes threshold at DEF's 0.8
position weight (240 < 300) to even join the league mean at all, making the
stretch a guaranteed no-op regardless of formula; fixed by giving TeamB 375
raw minutes, re-confirmed genuinely red before the site switch), then
team-level (attack+defense share one code path in `team_level_lambda`, both
switch together). All existing tests hardcoding the additive formula's exact
output (`test_team_level_lambda_stretch_recenters_on_league_mean`,
`test_team_level_lambda_attack_and_defense_stretch_apply_independently`,
`test_compute_wires_xg_spread_stretch_through_to_team_level_lambda`) updated
to the multiplicative formula's expected values — legitimate updates, not
loosened assertions, since the underlying formula genuinely changed shape.
Suite: 426 passed, 0 xfailed, ruff clean.

**Validation — Bundesliga 2025 (full additive-vs-multiplicative comparison,
isolated from BUG-013 since both sides of this specific comparison already
had that fix applied equally):** 1X2 Brier 0.5942→0.5927, Totals Brier
0.5274→0.5150, bias stayed within target both ways. ROI: 1X2 roughly flat
(within ~1pp either direction across EV thresholds — noise-level for one
league-season); **Totals ROI improved at every threshold** (EV>0%: -23.7%→
-22.5%; EV>5%: -24.9%→-20.3%; EV>10%: -24.4%→-18.1%). No metric moved
meaningfully the wrong way.

**Shipped as `poisson_v4_1`** (kept as its own method tag rather than
overwriting `poisson_v4`, so the pre-fix baseline stays comparable — see
"Versioning" note below) — backfilled for all 5 leagues × both seasons
(3,534 rows, matching `poisson_v4`'s row counts exactly). ALL-UP pooled
(all leagues/seasons/markets): Brier 0.5591→0.5572, ROI roughly flat
(@0%: -6.6%→-6.7%; @5%: -6.9%→-6.3%; @10%: -6.6%→-6.8%) — small net
positive, diluted at full pool since most leagues aren't touched much by
BUG-013 specifically, but BUG-014's fix applies uniformly everywhere.

**Real per-league signal found post-backfill (guardrail mode, TOTALS market,
pooled across both seasons per league) — ROI improving monotonically with EV
threshold, the shape you'd expect from a model whose own confidence tracks
real edge rather than noise:**
  - **Serie A**: -0.2% → +0.5% → +6.4% (EV>0/5/10%) — flips positive
  - **Premier League**: -3.9% → -3.6% → -0.8% — right shape, still negative
  - **Ligue 1**: -10.3% → -9.8% → -7.9% — right shape, still clearly negative
  - Bundesliga and La Liga do NOT show this shape yet (not monotonic).

**Versioning note (2026-08-14):** these fixes (BUG-013 + BUG-014) are tagged
`poisson_v4_1`, not `poisson_v4` (not overwritten) and not `poisson_v5`.
Rationale: `v3→v4` was a structural mechanism change (the whole player-level
blend system); this stays the same architecture with corrected data/formula,
matching how prior fixes (spread-stretch calibration, cross-league adjustment
work) stayed under `v4`. Reserved `v5` for BUG-012 (calendar-time windowing),
which replaces the underlying recency mechanism itself — a comparable
structural shift to what earned v4 its own number. `v4_1` costs ~1MB in a
105MB database (266 bytes/row × 3,534 rows) — negligible, kept alongside
`v4` rather than overwriting so the pre-fix baseline stays available for
comparison. `model_metrics_report.py` and other tools still default to
`poisson_v4` (`DEFAULT_METHOD` in 4 files: `model_metrics_report.py`,
`backfill_player_blend_predictions.py`, `sample_xg_lookback_ab.py`,
`diagnose_home_bet_calibration.py`) — must pass `--method poisson_v4_1`
explicitly until/unless `v4_1` is promoted (edit that one constant in each,
or just re-backfill under the `poisson_v4` name directly once fully satisfied).
**Note: `generate_club_league_card.py` (the LIVE card generator) does not read
`method` tags at all — it calls `compute()` directly with whatever's in the
current code, so every league already reflects today's fixes in live picks
regardless of any `method`-tag versioning discussion above; the tags only
affect backtest/comparison tooling.**

---

## BUG-013 — `PLAYER_RATING_CROSS_LEAGUE_GOAL_ADJUSTMENT` has no entries for the 4 new leagues' feeder divisions — a promoted team's real recent history gets almost entirely discarded — **FIXED 2026-08-14**

- **Type:** bug (missing calibration data + missing test coverage) · **Status:**
  FIXED same day. Found 2026-08-14, same investigation as BUG-014, walking through
  why 1. FC Köln's player-level attack rating was so far below the league mean
  that BUG-014's spread-stretch pushed it negative.

**Fix.** Measured all four factors the same way Serie B's `0.663` was measured
(not guessed): players with ≥300 minutes in BOTH the top-flight league and its
feeder division (any season), own goals/90 in each, pooled by minutes, ratio =
top-flight rate / feeder rate. Sample sizes (152-196 qualifying players per pair)
are larger than Serie B's original 82-player measurement, and all four factors
land in a tight, plausible band consistent with Serie B's own 0.663 — a real
cross-check that the methodology itself is sound:
  - Bundesliga / 2. Bundesliga: 152 players, 0.1143/0.1906 = **0.5999**
  - Premier League / Championship: 162 players, 0.0931/0.1402 = **0.6643**
  - La Liga / LaLiga 2: 196 players, 0.0842/0.1294 = **0.6512**
  - Ligue 1 / Ligue 2: 159 players, 0.1016/0.1372 = **0.7408**

Added to `PLAYER_RATING_CROSS_LEAGUE_GOAL_ADJUSTMENT` with the same rationale
comment style as Serie B's entry. Added a generic regression test,
`test_every_registered_feeder_division_has_a_cross_league_adjustment_entry`
(tests/test_compute_club_player_strength.py) — asserts every league with a
`lower_division` set in `core.leagues.LEAGUES` has a matching entry here; keyed
off the leagues registry, not hardcoded to today's 4 divisions, so it also
catches this class of gap for any future league added the same way. Confirmed
red against the pre-fix code (caught the exact 4 missing divisions), green after.

**Validated the fix resolves BUG-014's known real-world trigger.** Re-ran
Köln's player-level attack computation for all 3 matches that had hit the
downstream 0.1 floor (see BUG-014) — none go negative anymore post-stretch:
Köln vs. Freiburg (2025-08-31): -0.0296 → **+0.0061**; Wolfsburg vs. Köln
(2025-09-13) and Leipzig vs. Köln (2025-09-20) both similarly resolved. Also
confirmed no OTHER team in Bundesliga 2025 goes negative post-stretch as of
these dates. Köln's effective attack-rating sample size (`aw`, the weighted
minutes behind the rating) also jumped from 1,156.8 to 6,162.4 — over 5x more
real data now counted, since previously-excluded `2. Bundesliga` games are back
in (scaled by 0.5999).

**Not yet done: re-backfill production.** `soccer_model_predictions` for the 4
new leagues' 2025 seasons still reflects the OLD (buggy) factors — this fix only
changes the constant + tests, not the stored predictions. Re-backfill + real
before/after Brier/bias/ROI check still pending, deferred while BUG-014's own
fix is worked (no point re-backfilling twice).

**Open anomaly found validating this fix, NOT resolved by it — Hamburger SV.**
Hamburger SV (also promoted from `2. Bundesliga` this season, same timing as
Köln) appears ~17 times in the Bundesliga "model expects ~2 goals" bucket-2
outlier table from 2026-08-13's diagnostic session, several with large
predicted-vs-actual deltas — genuinely bad predictions for this team across
2025. The natural hypothesis was that BUG-013 (this bug) explained it the same
way it explained Köln's. Checked directly: it doesn't. HSV's raw attack rating
barely moved from this fix (0.1385 → 0.1005, and its `ratio_to_mean` stayed
comfortably away from BUG-014's overshoot boundary both before and after,
0.725 → 0.627) — unlike Köln, whose rating was much more exposed. Root cause:
HSV's squad already had substantial genuine non-`2. Bundesliga` data even
before this fix — most notably Nicolai Remberg, who carries 38.8% of the
team's ENTIRE weighted attack contribution alone (871 real minutes at MID
weight, from a loan spell at Holstein Kiel, an actual Bundesliga club, last
season — nothing to do with HSV's own second-division history). So this fix
was never going to move HSV's number much; whatever is actually causing HSV's
poor 2025 predictions is a still-undiagnosed, separate issue. Flagged here so
it isn't lost — worth its own diagnostic pass later (same style as the
Bundesliga bucket-2 drill-down), not assumed solved by BUG-013 or BUG-014.

**Root cause.** `PLAYER_RATING_CROSS_LEAGUE_GOAL_ADJUSTMENT` (in
`compute_club_player_strength.py`) currently has entries for only `Serie A, Serie
B, Premier League, Bundesliga, La Liga, Ligue 1` — none of the four new feeder
divisions added during the multi-league expansion (`2. Bundesliga`,
`Championship`, `LaLiga 2`, `Ligue 2`) have an entry at all, not even a
placeholder. Per `load_team_players`'s own documented behavior, a game played in a
league with NO factor entry is excluded ENTIRELY from that player's rating — not
scaled, not discounted, just dropped, from both the attack/defense numerator AND
the minutes denominator.

**Concrete evidence.** Marvin Schwäbe (Köln's GK, promoted from `2. Bundesliga`
this season): of his last 10 tracked appearances before 2025-08-31, **9 were in
`2. Bundesliga` and got excluded entirely**, leaving exactly **1 real game** (Aug
24 2025, 90 minutes, 0 goals) to represent his entire attacking-rating input. This
is not an isolated case — it plausibly affects most of Köln's promoted squad, and
by the same mechanism presumably any newly-promoted team in the other 3 new
leagues (Championship→Premier League, LaLiga 2→La Liga, Ligue 2→Ligue 1) — not yet
checked for those. A roster reduced to single-match, mostly-scoreless samples is
exactly what drove Köln's raw team-wide attack rate down to 0.0807 against a
league mean of 0.1911, which is what BUG-014's unguarded stretch then pushed
negative.

**Note this isn't quite the same gap the multi-league expansion plan already
flagged.** That plan explicitly logged the 4 new TOP-FLIGHT leagues'
cross-league-adjustment values as "an assumption to validate empirically later"
(defaulted to `1.0`, peer top-5 leagues) — those entries exist. The gap found here
is different and was apparently never flagged: the FEEDER divisions themselves
have no entry whatsoever, so promotion/call-up history from them (the exact
mechanism this constant exists to support, per BUG-010) is being silently thrown
away rather than scaled/gated deliberately.

**Missing test coverage (user's own framing):** nothing currently asserts that
every feeder division actually referenced by a team's real promotion history has
a calibrated (or explicitly gated) entry, and nothing catches a player's
rating-window sample size collapsing to as little as one match because of a
missing entry.

**Not yet resolved — real calibration work, not a guess.** Serie B's existing
`0.663` factor was empirically measured from players' own goal-scoring rate
specifically (see that constant's own comment) — the 4 missing feeder divisions
need the same real measurement, per this project's calibration-sweep discipline,
not a placeholder `1.0` assumed by analogy. Scope still open: measure real
factors for `2. Bundesliga`, `Championship`, `LaLiga 2`, `Ligue 2`, add entries,
then re-run BUG-014's Köln example to see whether the negative-rating trigger
disappears on its own once the sample size problem is fixed.

---

## BUG-012 — Player-recency windows are count-based ("last N appearances"), not calendar-based, so a player out for months can still count as fully "recent" — **STAGE 1 DONE 2026-08-14; STAGE 2 SHIPPED 2026-08-15 as `poisson_v4_2`; ROOT CAUSE #3 SHIPPED 2026-08-15 as `poisson_v4_3`; ROOT CAUSE #4 SHIPPED 2026-08-17 as `poisson_v4_4` (continuous coverage ramp, after two rejected intermediate designs)**

- **Type:** bug (design agreed, implementation deferred) · **Status:** open. Found
  2026-08-13 while walking through `player_trust_score`'s `data_coverage_score`
  calculation for 1. FC Köln (promoted from Bundesliga 2) ahead of its 2025-08-31
  match vs. SC Freiburg, at the user's request, step by step. User: "this opens the
  door for the model to reach back weeks/months/years to fill up the player's
  minutes, which defeats the purpose of the 'recent 10 games' window... this is
  absolutely a bug."

**Root cause (confirmed in three places now, one worse than the others):**

1. **`players_aggregated_recent_minutes()`** (used only inside `player_trust_score`,
   for `data_coverage_score` and the `joined_minutes` half of `roster_change_score`)
   takes a player's own last `window_size` (10) appearances, *any team*, with
   **zero calendar bound** — ordered purely by recency rank, no date cutoff at all.
   A player returning from a long injury layoff would have their "last 10" reach
   back however far necessary, treated identically to a player who played those
   same 10 games in the last 10 weeks. Confirmed live: for Köln vs. Freiburg
   (2025-08-31), several qualifying players' minutes came from BEFORE the
   promotion (i.e. a different club, a different division, many months earlier) —
   e.g. Ísak Bergmann Jóhannesson: 889 of 889 minutes counted came from a different
   team; only 80 were at Köln itself.
2. **`load_team_players()`** (the actual player-level attack/defense rating engine —
   NOT just the trust score) has the *same underlying flaw in milder form*: line
   ~429, `window = games[:window_size]`, decayed by **rank** (`w = decay ** rank`
   at line ~436), not by elapsed calendar time. A player's 9th-most-recent game
   gets the same weight (`decay**9`) whether that game was 3 weeks old (dense
   recent schedule) or 8 months old (return from injury/thin schedule) — rank says
   nothing about actual staleness. Confirmed this is a real, separate instance of
   the same class of bug, not just "the same bug twice" — `load_team_players`
   already has a *different*, narrower fix from 2026-08-11 (candidate players must
   have appeared in the TEAM's own last `window_size` matches to be considered at
   all — a team-recency gate), but that gate doesn't bound how far back a
   *qualifying* player's own rate-computation window can still reach. **Covers
   defense too, not just attack, with no separate fix needed:** the same `w =
   decay ** rank` value computed once per game in this loop is applied to BOTH the
   attack accumulators (`attack_num`/`attack_den`) AND the defense accumulators
   (`ga_num`/`ga_den`, `xga_num`/`xga_den`) — one shared per-game weight, not two
   independent ones (confirmed 2026-08-14) — so fixing this one weight fixes both
   sides of the rating at once.
3. **The team-attribution gate itself is also count-based, not calendar-based**
   (found 2026-08-14, walking through why a specific Köln player's attack rating
   looked wrong). `load_team_players`'s candidate-narrowing step (the 2026-08-11
   fix referenced above) considers a match "recent" if it falls within a TEAM's
   own last `window_size` (10) matches — a count, not a date range. Confirmed live:
   Köln's own last-10-matches window (as of 2025-08-31) spans **2025-03-15 to
   2025-08-24 — over 5 months** — because Köln just came up from `2. Bundesliga`,
   so 9 of those 10 "recent" matches are actually from months earlier. A player
   whose only appearance for Köln was that March match, and who hasn't played
   since, would still register as "recent enough" to be attributed to Köln under
   this gate, with no calendar check at all. Same root design flaw as #1/#2 above
   (match-count standing in for calendar time), just at the attribution step
   rather than the minutes/rating step — folded into this entry rather than
   tracked separately, since the fix is the same fix.

**Architectural direction agreed 2026-08-14 (in addition to the 2026-08-13
design):** rather than fixing these three call sites independently and risking a
fourth, undiscovered one, build **one centralized function** — "how much does
player X count for team Y, as of date D (with a given lookback window)" — that
owns ALL of the calendar-time-decay logic in one place: team attribution/
candidacy, minutes weighting (`player_trust_score`), and rating-window weighting
(`load_team_players`) all call through it rather than each reimplementing their
own version of "recent." User: "it's sounding like we need a centralized...
function that takes a player, a team, and a date... and figures out how much that
player matters to the team... so there's one place to implement the calendar
based time decay and it's used uniformly across all scenarios." This centralized-
function idea should shape how the staged implementation plan below gets built —
worth revisiting the plan's step 1 (shared time-decay helper) as "build this
function first, then migrate all three call sites onto it" rather than three
separate patches.

**Design agreed with user (2026-08-13), not yet built:**

- Replace **rank-based decay** (`decay ** rank`, position in an ordered list) with
  **calendar-time-based decay** (a function of actual elapsed days/weeks since the
  match), applied consistently everywhere a "recent window" is computed — both
  `load_team_players` (replacing today's rank decay) and `player_trust_score`
  (replacing today's flat, undecayed minute sums). Rationale (user): "calendar
  time is a singular universal concept whereas matches introduce a bunch of
  variability that clearly has already caused problems" — directly analogous to
  this file's BUG-010 "season-blind" precedent (continuous, date-driven logic
  beats anything keyed to discrete boundaries/counts).
- **Shape: exponential decay with a hard floor** — smooth exponential falloff by
  elapsed time, forced to exactly 0 past some cutoff (a real number TBD, user's
  working intuition: "any stats that occurred 3mo from the current match date are
  probably useless"). Explicitly NOT season-aware: no season-label check, no
  special-case discontinuity at a season boundary — the earlier illustrative
  example's apparent "steep drop" across an 8-week gap was confirmed to be nothing
  more than the natural shape of one continuous decay curve evaluated at two
  distant elapsed-time points, not a separate rule. A hard cutoff also lets the
  underlying SQL query stop pulling games past that point at all, rather than
  fetching a full last-10 and discarding/near-zero-weighting the stale ones.
- **`player_trust_score`'s totals must decay consistently, both sides of BOTH
  ratios — not just `data_coverage_score`'s.** If `players_aggregated_recent_
  minutes`'s numerator becomes time-decayed, `team_aggregated_recent_roster_
  minutes` (the `team_total_minutes` denominator) must decay the same way too, or
  the `min(ratio, 1.0)` cap stops meaning "fully covered." This isn't only
  `data_coverage_score`'s concern: `roster_change_score`'s `departed_minutes` and
  `joined_minutes` are built from these SAME two functions
  (`team_aggregated_recent_roster_minutes` and `players_aggregated_recent_minutes`
  respectively) — so decaying both functions once fixes `data_coverage_score` AND
  `roster_change_score` together, no separate mechanism needed for the second one
  (confirmed 2026-08-13, walking through `roster_change_score` step by step after
  `data_coverage_score`).
- **`PLAYER_RATING_MIN_MINUTES_RECENT_WINDOW` (currently 300, a literal minutes
  count) needs re-examination once minutes are decay-weighted** — 300 raw minutes
  from last week and 300 raw minutes from 10 weeks ago currently qualify a player
  identically; once decayed, "300" stops being a real minutes count and the
  qualifying bar likely needs its own recalibration (user: "I'm not sure the 300
  minute bar still makes sense... presumably there's a different number that
  needs to serve the same purpose").

**Staged implementation plan (agreed, deferred until picked back up):**

1. **Structural refactor first, as a verified near-no-op.** Build one shared
   time-decay helper, wire it into both `load_team_players` (replacing rank decay)
   and `player_trust_score` (replacing flat sums), using TDD (write the tests for
   the desired decay behavior first, per user's explicit request, then implement
   against them). Choose a starting decay parameter close to today's shipped
   rank-decay behavior and verify via row-level diff of `soccer_model_predictions`
   before/after (this project's standard "verify plumbing changes are true
   no-ops" discipline), NOT just "tests pass."
2. **Calibration sweep second, as a real model-behavior change.** Because this
   touches `load_team_players` (the rating engine feeding `lambda_home`/
   `lambda_away` for every prediction), the actual decay half-life/cutoff constant
   needs the same bias/Brier/ROI sweep discipline as every other tuned constant in
   `MODEL_TUNING_PARAMETERS.md` — picked from real backtest evidence, not the
   number that felt reasonable in design conversation.
3. Recalibrate/redefine the minutes-qualifying threshold (replacing the current
   300-minute literal bar) once decayed minutes are in place, on the same
   evidence-based footing.

**Stage 1 (DONE 2026-08-14): built the centralized function, wired it into all
three call sites, verified structurally near-no-op.** Built via TDD (tests
first, confirmed red, then implemented): `calendar_recency_weight(match_date,
before_date, half_life_days, cutoff_days)` (compute_club_player_strength.py,
right before raw_team_strength -- same placement convention as BUG-014's
`spread_around_mean`). Exponential decay by elapsed CALENDAR days (not
rank/position in a list), with a hard floor: exactly 0.0 once
`elapsed_days > cutoff_days`, 1.0 at `elapsed_days == 0`, halving every
`half_life_days` below the cutoff. Two matches on the same calendar day with
different kickoff times (a real case -- an early and a late kickoff on the
same matchday) truncate to `elapsed_days == 0`, not an error -- only a
genuinely negative gap (a real lookahead) raises. `match_date`/`before_date`
are parsed via `str(x)[:10]` before `date.fromisoformat` -- found live
backfilling Bundesliga that `soccer_matches.match_date` carries a full
timestamp for some data sources (`'2025-08-22T18:30:00.000Z'`) and a plain
`'YYYY-MM-DD'` for others; adopted the same truncate-then-parse convention
already used elsewhere in this codebase (e.g. `import_wc_match_xg.py`) rather
than inventing a new one. 9 unit tests cover the decay shape, the hard-cutoff
boundary, same-day handling, the mixed-timestamp-format parsing, and the
negative-gap rejection.

Wired into all three root-cause call sites from the design above:
- `load_team_players`'s per-game weight (`w = decay ** rank`, rank = position in
  the top-`window_size` list) replaced by `calendar_recency_weight(g["match_date"],
  before_date, ...)` -- an actual elapsed-time weight, applied to both the
  attack and defense accumulators (they already shared one per-game weight, see
  root-cause #2 above, so one change fixes both sides).
- `team_aggregated_recent_roster_minutes` and `players_aggregated_recent_minutes`
  (player_trust_score's two aggregation functions) -- both changed from a flat
  SQL `SUM(minutes_played)` to a per-game `calendar_recency_weight`-weighted sum
  computed in Python (had to pull `match_date` alongside `minutes_played` per
  row instead of letting SQL aggregate it). Both decay the same way, per the
  design note above (`roster_change_score`'s ratio compares them against each
  other, so they must move together).
- Root cause #3 (the team-attribution/candidate-narrowing gate, still a match
  COUNT not a calendar bound) is deliberately **not** touched in Stage 1 -- the
  design's own staged plan only calls for the shared time-decay helper wired
  into `load_team_players` and `player_trust_score` at this stage; the
  attribution gate is folded into Stage 2's real cutoff-day tightening instead
  (a hard `cutoff_days` is what lets that gate become calendar-bound instead of
  count-bound, per the design note: "a hard cutoff also lets the underlying SQL
  query stop pulling games past that point at all").

`PLAYER_RATING_RECENCY_HALF_LIFE_DAYS`/`PLAYER_RATING_RECENCY_CUTOFF_DAYS`
(new constants, replacing the now-dead `PLAYER_RATING_PAST_MATCH_WINDOW_DECAY`,
which no call site used anymore -- deleted rather than left as backwards-compat
cruft, and swapped for the two new constants in `model_metrics_report.py`'s
auto-recorded `KNOB_NAMES` list) both default to `1.0e12` days -- deliberately
absurd, so `0.5 ** (elapsed_days / half_life_days)` is indistinguishable from
1.0 to double-precision float error for any realistic elapsed gap. This is
Stage 1's actual no-op mechanism, not just "a small number": even a literal
multi-century elapsed_days value rounds to a weight of 1.0 - O(1e-9) or
smaller.

**Verified near-no-op two ways:**
1. Full test suite (435 tests) required updating ~13 existing assertions from
   exact `==` float/dict equality to `pytest.approx(...)` -- the previous
   rank-based `decay=1.0` was a mathematically EXACT no-op (`1.0 ** rank ==
   1.0` for every rank), so old tests could assert exact sums; calendar decay
   at even the most extreme near-no-op setting is still a real (if
   ~1e-9-to-1e-11-scale) floating-point computation, not an exact identity.
   Four tests seeding a player at exactly the 300-minute
   `PLAYER_RATING_MIN_ATTACK_WEIGHTED_MINUTES_TO_HAVE_OWN_RATING` boundary had
   to move to 400 minutes -- any nonzero decay strictly below 1.0, applied to a
   value sitting exactly ON a `>=` gate, tips it just under, dropping the
   player from having an own rating entirely (a genuine edge-case discovery,
   not just a tolerance issue -- documented as vanishingly unlikely in real
   production data, since real weighted-minutes sums essentially never land on
   an exact integer boundary once real decay curves are involved, but real for
   a hand-constructed boundary-exact test fixture).
2. Row-level diff of real production data: re-backfilled Bundesliga 2025 under
   a scratch method tag (`poisson_v4_1_stage1_calendar_decay`, since deleted)
   with today's code (calendar decay wired in) and diffed all 306 matches'
   `p_home`/`p_draw`/`p_away` against the existing `poisson_v4_1` rows
   (pre-Stage-1 code). Zero rows differ by more than 1e-6; the actual max
   deviation across all 306 matches x 3 outcomes was ~2.7e-11 -- floating-point
   noise, not a behavior change. This project's "verify plumbing changes are
   true no-ops" discipline (row diff, not just "tests pass") -- same standard
   BUG-014's Stage 1 was held to.

**Stage 2, shipped 2026-08-15 as `poisson_v4_2`.** Swept exponential-shape
candidates (half_life/cutoff pairs, ratio 1.5x per the user's own "half-life
felt too fast relative to the cutoff" feedback) pooled across all 5 leagues x
2024/2025 -- 6 candidates tried (60/90, 80/120, 120/180, 150/225, 200/300,
300/450) against the `poisson_v4_1_1` baseline (BUG-016-corrected). Picked
half_life=120d (~4mo) / cutoff=180d (~6mo): among the best EV>10% ROI gains of
any candidate (+1.8pp pooled) for a modest Brier cost, and easy to remember.
Real impact vs. `poisson_v4_1_1`, pooled and per-league (Brier / ROI @0/5/10%
EV, Bet365):

| | Brier | ROI @0/5/10% |
|---|---|---|
| ALL-UP old | 0.6031 (n=3533) | -7.6 / -7.2 / -8.8% |
| ALL-UP new | 0.6037 (n=3533) | -7.3 / -8.1 / -7.0% |
| Serie A | 0.6072→**0.6059** | -9.4/-9.0/-6.9% (was -9.6/-8.9/-9.8%) |
| Premier League | 0.6109→0.6107 | +6.1/+6.0/+4.7% (was +2.4/+4.2/+3.1%) |
| Bundesliga | 0.6064→0.6087 (worse) | -17.0/-19.8/-17.7% (was -18.3/-18.1/-17.1%) |
| La Liga | 0.5919→0.5916 | -12.7/-14.2/-12.8% (was -11.7/-13.4/-17.1%) |
| Ligue 1 | 0.5987→0.6019 (worse) | -5.0/-5.4/-4.7% (was -1.5/-1.2/-3.8%) |

Mixed per-league, not a clean win everywhere: Serie A/Premier League/La Liga
improve, Bundesliga and Ligue 1 get worse. Bundesliga's degradation isn't
surprising (BUG-015: already a known-noisy 2-season sample). **Ligue 1's
degradation was investigated in depth** (user pushed back correctly on an
initial hand-wavy "early season = thinner data" explanation, since every
league uses the identical window/decay/schedule and that alone can't explain
one league/season standing out) -- traced to a real, verified mechanism: it's
concentrated almost entirely in Aug/Sep 2025 (108 of 611 matches, but ~88% of
the whole season's Brier degradation), and the proximate cause isn't the
player-rating math itself (checked: no player's last-10-appearance window
actually drops a game to the 180-day cutoff for the worst-affected teams) but
`player_trust_score`'s blend weight collapsing hard toward team-level right at
a new season's start -- e.g. RC Lens's `weight_attack` went from 0.0 (fully
trust player-level) to 0.41-0.54 across its worst-affected matches, because
the "current squad minutes coverage" signal (same half_life/cutoff) has much
less to work with right when a season has barely begun, and RC Lens's
team-level rating happens to underrate them relative to their real
(player-level) strength this season. Not really "Ligue-1-specific" as a root
cause -- every league's early season hits the same starvation, it just landed
harder on these particular teams' player-vs-team gap. This is a direct preview
of, and motivation for, the still-open **root cause #3** below (the
count-based team-attribution gate) -- v4_3's unified weighted-minutes design
is the more targeted fix for this exact interaction, not further half-life/
cutoff tuning.

**Follow-on fix found and shipped alongside v4_2, same day:** `roster_as_of_
date`'s fallback path (used only for a team's literal first match of a season,
before any current-season match exists yet -- reaches into the PREVIOUS
season by design) called `team_aggregated_recent_roster_minutes` with no way
to override half_life/cutoff, so it silently inherited whatever the module
default was. Found while re-validating the test suite after promoting real
Stage 2 values to the module defaults (28 tests failed, one of which exposed
this). Fixed by hardcoding that one internal call to near-no-op regardless of
the module default -- the fallback's whole purpose is reaching back as far as
needed, independent of the rating-decay cutoff. Traced the actual timeline:
both `poisson_v4_1_1` and `poisson_v4_2`'s backfills ran BEFORE the module
defaults were promoted from Stage 1's near-infinite values to the real 120/180
-- so this bug was dormant during both runs (the un-parameterized fallback
call was already using near-infinite values at that time, coincidentally
identical to what the fix now hardcodes) and neither already-shipped version's
numbers needed re-running. The fix matters going forward: every future
backfill now permanently has the real 120/180 as its module default, so
without this fix, `roster_as_of_date`'s fallback would have started silently
picking up real decay on every future run's first-match-of-season predictions.

Promoted to shipped module defaults (`PLAYER_RATING_RECENCY_HALF_LIFE_DAYS`/
`_CUTOFF_DAYS` = 120.0/180.0) -- live picks (`generate_club_league_card.py`)
reflect this now, not just backfill/backtest tooling.

**Root cause #3, shipped 2026-08-15 as `poisson_v4_3`.** The count-based
team-attribution/candidate-narrowing gate in `load_team_players` (a player had
to have appeared in the team's own last `window_size` matches by COUNT to be
considered at all) converted to calendar-based: a new constant,
`PLAYER_RATING_MIN_TEAM_WEIGHTED_MINUTES_TO_BE_A_CANDIDATE = 10.0`, gates on
calendar-decayed weighted minutes at the player's actual attributed team
(summed across every appearance within `cutoff_days`), checked against the
team they're really attributed to -- not just "cleared some team's floor"
(see the constant's own comment for why that scoping matters: a thin debut at
a new team isn't rescued by a big history at the old one). Deliberately low
floor (10 minutes, per the user's own reasoning): this is just a gate, not a
calibration target -- `apply_shrinkage` already pulls thin samples toward the
positional prior regardless, so the gate only needs to guard against literal
single-minute-cameo data-artifact noise, not protect against thin-but-real
data. Old gate was fixture-density-sensitive (a team's own "last window_size
matches" stretches or shrinks in calendar time depending on how densely
they've played -- a dense cup stretch could exclude a player out just 6-7
weeks; a sparse schedule could still include one who hasn't played in
months); new gate doesn't care how many matches the team has played since,
only real elapsed time. TDD (7 new tests replacing the old count-based test),
full suite green (455 tests), ruff clean.

**Impact vs. `poisson_v4_2` (real backfill, 5 leagues x 2024/2025 + Serie A
2022/2023):** pooled Brier 0.6037→0.6036 (negligible), ROI within ~0.1-0.6pp
at every threshold, no consistent direction, every league near-flat. Same
profile as BUG-016: a real, individually-meaningful fix (fixes a genuine
fixture-density bug) whose pooled effect is small because it only changes
which players qualify for a relatively narrow set of matches. Promoted
immediately -- pure code fix, no tunable constant to separately promote,
live picks reflect it now.

**Investigated and explicitly did NOT fix Ligue 1's v4_2 weakness (important
correction to this entry's own earlier framing).** v4_3 only touches
`load_team_players` (the RATING computation). The actual mechanism behind
RC Lens/Lille/Nice/Le Havre's `weight_attack` collapse lives in a completely
different function, `player_trust_score` (the BLEND-WEIGHT computation) --
confirmed empirically: RC Lens's `weight_attack` is bit-for-bit unchanged by
v4_3. Digging into `player_trust_score` found a real, deeper issue: its
"prior roster reference" is anchored not to the real `before_date` but to
`team_prior_window_cutoff_date`'s output -- a purely count-based boundary (no
calendar awareness, no decay) that, early in a season, can reach back nearly
a full year (confirmed for RC Lens: `prior_cutoff` = 2025-03-30, itself 152
days before the real 2025-08-29 evaluation date, with the actual roster data
reaching toward October 2024). `team_aggregated_recent_roster_minutes`'s own
decay is then computed relative to that stale `prior_cutoff`, not the real
date -- so simply calendar-izing that function's OWN count limit (its
`LIMIT n`) wouldn't fix anything, since it's being fed the wrong reference
point to begin with. Proposed fix (prototyped, not yet built): drop
`team_prior_window_cutoff_date` and the two-adjacent-count-windows design
entirely -- decay already lets a single calendar window express "how recent"
smoothly, so the second window (originally built to compensate for
count-based windows having no granularity) is no longer needed. Compare
current roster directly against one calendar-decayed recent-minutes window,
anchored to the real `before_date`.

**Prototype validated for churn-signal correctness** (methodical check,
per the user's explicit request, using the CORRECT point-in-time roster via
`roster_as_of_date` -- see the correction below) across RC Lens + 4 other real
Ligue 1 teams, both early- and mid-season: departed-player counts look
real and plausible (7-19 depending on team), and at mid-season the new design
correctly converges to zero detected churn (roster has caught up with
itself), cleaner than the old mechanism, which still shows small nonzero
"leftover" churn readings months later (a residue of the stale-window bug).
**But it moves trust the WRONG direction for the original goal**: at
season-start, the corrected mechanism gives LOWER trust (MORE team-level
weight) than the current buggy one, not higher -- RC Lens goes from 0.4763
(current, buggy) to 0.2945 (prototype). This is backwards from what would
help Ligue 1's calibration. Why: a point-in-time roster (what backtesting
uses) can only register players who've ALREADY PLAYED for the team this
season -- by construction it can never detect a "joined" player who hasn't
debuted yet, so 100% of the churn signal comes from the "departed" side. The
old, bloated, stale reference window diluted real departures inside a huge
pool of ancient data; the tighter, correct window surfaces MORE of that
departure signal cleanly -- more technically correct, but reads as more
churn, not less. **Not built into production** -- real architectural
improvement worth having on its own merits (removes a genuine staleness bug,
cleaner mid-season behavior), but doesn't rescue Ligue 1's calibration, so
not urgent. Logged here as a scoped, ready-to-build follow-up if/when wanted.

**Correction found along the way:** the ad-hoc diagnostic scripts used
earlier in this investigation (the ones that produced the original
`weight_attack` numbers cited above in this entry, e.g. "0.000 -> 0.414")
had a real methodology bug -- they called `resolve_blend_weight`/`compute()`
without passing `current_roster_ids_by_team`, so they silently used
`current_roster_player_ids` (today's LIVE roster, as of whenever the script
ran) instead of the point-in-time roster (`roster_as_of_date`) the real
backfill actually uses. **The shipped `poisson_v4_1_1`/`poisson_v4_2`
predictions themselves are unaffected** -- `backfill_player_blend_
predictions.py` always correctly passes the point-in-time roster -- only the
follow-up diagnostic scripts used the wrong signal. Corrected real numbers
for the originally-flagged matches (baseline = near-no-op decay, v4_2 = real
shipped settings, both with the correct point-in-time roster):

| Team | Date | baseline | v4_2 (corrected) |
|---|---|---|---|
| RC Lens | 2025-08-29 | 0.0000 | 0.5237 |
| RC Lens | 2025-09-20 | 0.0019 | 0.5985 |
| Lille | 2025-09-28 | 0.0000 | 0.5513 |
| Nice | 2025-08-31 | 0.3205 | 0.6794 |
| Nice | 2025-10-29 | 0.1695 | 0.4866 |
| Le Havre | 2025-08-31 | 0.4021 | 0.7126 |

**Magnitude check, done properly (not just eyeballed):** is a +0.3-to-+0.6
shift, or a resulting weight in the 0.5-0.7 range, actually unusual? Two
checks, both real data:
1. Shift size for a TYPICAL (not cherry-picked) team's early match, across
   all 5 leagues, one match per team: mean/median shifts of +0.02 to +0.04
   everywhere, Ligue 1 unremarkable among them (mean +0.044, max +0.107) --
   the 6 flagged matches (+0.31 to +0.60) are genuine outliers, not
   "what early season normally looks like."
2. Absolute weight_attack VALUE at a genuinely comparable point (each Ligue 1
   team's 2nd graded match of the season, same `roster_as_of_date` branch):
   league range 0.62-0.91, mean 0.80. RC Lens (0.619) is the LOWEST of all 18
   teams -- i.e. the model trusts player-level data MORE for RC Lens than for
   any other Ligue 1 team at this point in the season, not less. Lille/Nice/
   Le Havre are also bottom-third, not top. **The flagged teams are not
   outliers toward excessive team-level trust -- if anything the opposite.**
   Reinforces the read that this is real season-to-season variance in which
   teams the (normally-behaving) team-level fallback happens to fit well,
   not a mechanism defect -- same flavor of conclusion as BUG-015.

**Root cause #4 SHIPPED 2026-08-17 as `poisson_v4_4`, after building and real-
data-testing THREE designs, two of which were rejected on real evidence before
landing on the one that shipped.** The scoped-but-not-built prototype above
(single calendar-decayed window, still churn-gated) was picked back up, built
for real, and put through this project's actual calibration-sweep discipline
(real backfill + Brier/bias/ROI, not intuition) -- which is exactly what
caught both rejected designs before either could ship.

**Attempt 1 — single calendar-decayed window (still churn-gated), matches the
prototype above:** replaced the two-adjacent-count-windows design with ONE
window (team's own last `window` matches, anchored directly to the real
`before_date`) -- fixes the wrong-clock bug (BUG-012 root cause #3 section
above) with no second reference date left to get wrong. Real backfill (5
leagues x 2024/2025 + Serie A 2022/2023) showed a genuine, uniform Brier
regression: pooled 0.6036 -> 0.6071 (+0.0035), and EVERY league got worse, not
a mixed bag (Serie A +0.0006, Premier League +0.0019, Bundesliga +0.0034, La
Liga +0.0057, Ligue 1 +0.0060) -- a materially different, non-noise-like
profile from every other fix this session. Mechanism, confirmed by comparing
weight_attack under the old vs. new mechanism across every 2025 team-match in
all 5 leagues (not just the flagged teams): mean weight_attack shifted +0.09
to +0.14 toward team-level, affecting 66-73% of all team-matches, uniformly
across leagues. Root cause: "current roster" (built from recent match
appearances, via `roster_as_of_date`) and "team's own last `window` matches"
(the new single reference window) are nearly the SAME underlying signal, so
genuine roster churn became almost structurally undetectable -- the OLD
design's stale, much-older reference window had, by accident, been providing
the temporal separation needed to detect churn at all. **Not shipped.**

**Attempt 2 — coverage-only, binary cutoff:** given churn detection had now
caused three separate bugs (the original season-anchor staleness, the
two-window anchor bug, and this uniform-suppression failure) and the
poisson_v4_4 A/B above showed even the OLD mechanism's already-team-heavy
blend (~80% team-level on average) was still extracting real value from its
~20% player-level minority share, the churn factor was dropped entirely --
trust becomes pure data coverage (how much of the current squad's minutes
belong to players with a real recent track record), no roster-change
comparison at all. Real backfill showed a DIFFERENT problem: the existing
binary >=300min qualify/disqualify cutoff (`PLAYER_RATING_MIN_MINUTES_
RECENT_WINDOW`) is trivially cleared by nearly any real roster player, so
once it was the SOLE gate (not multiplied against a churn factor anymore),
coverage almost never failed to saturate near 1.0. Mean weight_attack
collapsed to ~0.07-0.11 (nearly ALL player-level trust) in every league
uniformly -- an even more extreme, equally uniform swing in the opposite
direction. Pooled Brier: 0.6036 -> 0.6052 (still worse), but per-league this
version was genuinely mixed rather than uniform: Bundesliga/La Liga/Ligue 1
improved (these are exactly the leagues `poisson_v4_2` had flagged as hurt by
leaning MORE team-level), while Serie A/Premier League (the two largest
leagues in the pool) got meaningfully worse, dragging the pooled number
negative. Squad rotation depth (32-40 distinct players/team) and player-stats
completeness (99.7-100%) were checked and ruled out as the explanation --
neither correlates with which leagues improved. **Not shipped**, but this
result is what pointed at the real fix: since the blend weight barely
differed by league (all ~90% player-level) yet outcomes still diverged by
league, the split isn't about HOW MUCH player data gets used -- the coverage
score's SHAPE (a flat, trivially-cleared cutoff) just wasn't discriminating
between squads at all.

**Shipped design — coverage-only, continuous ramp:** replaced the binary
cutoff with a smooth per-player confidence multiplier, `min(minutes /
PLAYER_RATING_COVERAGE_SATURATION_MINUTES, 1.0)`, applied to each player's own
tracked minutes before summing into the coverage score -- a thin player is
discounted TWICE (both raw minutes and confidence are small), so a squad of
mostly fringe/rotation players lands meaningfully below full trust even though
every individual technically "has some data." The constant itself (renamed
from `PLAYER_RATING_MIN_MINUTES_RECENT_WINDOW`) was calibrated via a real
7-candidate sweep (400/500/700/900/1200/1500/2000, 5 leagues x 2024/2025,
pooled against `poisson_v4_3`) rather than picked from the first value tried:

| Saturation | Brier | Δ | Home bias | Draw bias | Away bias | ROI@0% | ROI@5% | ROI@10% |
|---|---|---|---|---|---|---|---|---|
| Baseline | 0.6036 | — | +0.003 | -0.016 | +0.013 | -7.2% | -7.5% | -7.1% |
| 400 | 0.6075 | +0.0038 | -0.008 | -0.016 | +0.024 | -11.1% | -12.1% | -11.6% |
| 500 | 0.6057 | +0.0021 | -0.008 | -0.016 | +0.023 | -10.5% | -11.5% | -11.5% |
| 700 | 0.6003 | -0.0033 | -0.006 | -0.015 | +0.021 | -9.5% | -8.9% | -9.7% |
| 900 | 0.5966 | -0.0071 | -0.004 | -0.015 | +0.019 | -9.4% | -9.3% | -7.4% |
| **1200** | **0.5964** | **-0.0072** | **-0.002** | -0.015 | +0.017 | -8.0% | -6.1% | -5.8% |
| 1500 | 0.5977 | -0.0060 | -0.001 | -0.015 | +0.016 | -6.4% | -7.6% | -6.8% |
| 2000 | 0.5998 | -0.0038 | +0.000 | -0.015 | +0.015 | -7.4% | -6.5% | -6.2% |

1200 is a genuine local minimum, not an early plateau -- Brier improves
monotonically from 400 through 1200, then gets WORSE again at 1500/2000,
confirming the ceiling rather than assuming it. At 1200, ALL FIVE leagues
improved on Brier (unlike either rejected design), and home bias moved
closest to zero among the strong performers. Draw bias barely moves across
the whole sweep (-0.015/-0.016 everywhere) -- this parameter doesn't touch
draw calibration, only the attack/defense player-vs-team blend.

Raw ROI at 1200 looked like a clean win at the 5%/10% EV thresholds, but
**checking with the same guardrail `generate_club_league_card.py` applies to
real picks** (`CLUB_LEAGUE_MIN_PICK_PROBABILITY=0.25`) moderated that: baseline
guardrail ROI -5.0%/-4.8%/-3.1% vs. 1200's -5.6%/-4.2%/-3.5% -- one threshold
improves, two get slightly worse, all small moves, net roughly neutral rather
than a real gain. **The ROI case for shipping is a wash, not a win; the case
for shipping rests on Brier and bias, both of which improved for real.**

Notably, 1200 exceeds the theoretical maximum minutes a player can accumulate
in one `window` (10 games x ~90 min = 900) -- no individual player's
confidence ever actually reaches 1.0 at this setting (tops out around
900/1200 = 0.75), but the aggregate team-level score, summed across a whole
roster, still reaches 1.0 for well-tracked squads. Left as the real,
validated value rather than artificially capped at 900 to "look right."

Shipped as `poisson_v4_4`: full production backfill (5 leagues x 2024/2025 +
Serie A 2022/2023) vs. `poisson_v4_3` -- see MODEL_VERSION_LOG.md for the
final numbers.

---

## FEATURE-017 — All-up metrics report across every league/season/market; renamed model_snapshot.py — **SHIPPED 2026-08-11**

- **Type:** enhancement · **Status:** SHIPPED 2026-08-11. Logged/built same day —
  user asked "if i want to see all up... brier, roi, bias... across all leagues,
  seasons and markets, how do i do that?" after FEATURE-015 shipped; the honest
  answer at the time was "there isn't a command for that, run it per league and
  combine by hand." Built as the new default report instead of staying a manual
  step. Also folds in the user's separate ask to rename `model_snapshot.py` —
  "an odd and not very useful name" — to `model_metrics_report.py`.

**What shipped.** `model_metrics_report.py` (renamed from `model_snapshot.py`) now
has two modes: the ORIGINAL single-league deep-dive report (compression-bucket
table included) still available via `--league "X"`, unchanged in output; and a NEW
default report (no `--league` needed) covering every league/season the model has
real `soccer_model_predictions` rows for. Leagues and seasons are discovered live
from the database (`discover_leagues()`/`discover_seasons()`), not a hardcoded
list, so a newly-added league is picked up automatically without a code change.

Three views, all built from the same pooling primitives so every number in the
report is computed the same way regardless of scope:
- **ALL-UP** — every league x every season x every MARKET, genuinely pooled into
  three numbers (Brier, Bias, ROI) via `pooled_brier_across_markets()`/
  `pooled_roi_across_markets()`.
- **BY MARKET** — pooled across leagues, split by season, one market at a time
  (`pooled_brier()`/`pooled_roi()` with `totals=True/False`).
- **BY LEAGUE** — pooled across seasons, split by season, both markets shown
  side by side, not blended.

**Design correction, 2026-08-11 (same day): the first version of this feature kept
1X2 and totals separate even in the ALL-UP section** — reasoning that this codebase
has a repeatedly-stated "never pool across markets" rule (`run_totals()`'s own
docstring). **User corrected this directly**, looking at the shipped output: "this
section should have three numbers: brier, bias, roi -- across *all* leagues,
markets, seasons." Rebuilt ALL-UP to actually blend markets for Brier and ROI:
  - **Brier** is legitimately combinable: a 3-class (1X2) and 2-class (totals)
    Brier score differ in their NAIVE baseline (~0.667 vs ~0.5) but land on the
    SAME [0, 2] error scale regardless of class count, so an n-weighted average
    across both answers a real question (mean squared probability error per
    graded prediction, regardless of market).
  - **ROI** is legitimately combinable: every bet stakes $1 regardless of market,
    so summing profit/staked across both is an exact portfolio-level return, not
    an approximation.
  - **Bias** is the one metric that genuinely can't be pooled across markets --
    totals has no sharp-book O/U data to blend WITH (FEATURE-015), so ALL-UP's
    bias number is inherently 1X2-only; reported as such inline rather than
    silently presented as if it covered both markets.
The original "never pool across markets" rule still holds everywhere else in the
report (BY MARKET and BY LEAGUE keep the two markets as separate numbers) --
it was specifically the ALL-UP section's job to be the true single-number rollup,
which the first version failed to actually deliver.

**Pooling correctness, not just convenience:**
- Brier: n-weighted sum of per-(league, season[, market]) scores -- exact, since
  Brier is itself a mean of squared errors (associative with computing on raw
  concatenated data).
- Bias: concatenates raw model-vs-market pairs across every (league, season) and
  calls `compare_model_vs_market_odds.summarize()` ONCE, rather than averaging
  pre-computed per-group summaries -- averaging would be wrong for `summarize()`'s
  non-linear stats (`max_abs_diff`, `favored_agree_rate`); only the mean happens to
  come out right that way, so this avoids a subtle latent bug rather than just
  being tidier.
- ROI: `backtest_from_predictions.py` refactored to extract `grade_1x2()`/
  `grade_totals()` -- the same core grading logic `run()`/`run_totals()` already
  had, just returning the full stats dict (staked, profit, bets, wins, by_side)
  instead of only printing it and returning `(roi, bets)`. `run()`/`run_totals()`
  themselves are now thin wrappers (print + return the same 2-tuple as before,
  verified byte-identical output pre/post-refactor) -- true pooled ROI is sum of
  profit / sum of staked across every group (leagues, seasons, and for ALL-UP,
  markets too), not an average of pre-computed ratios (which would be wrong
  whenever stakes differ across groups).

**A real finding the tool surfaced immediately:** pooling totals ROI across all 5
leagues flips it negative (-6.8% at EV>0%) despite Serie A alone being positive at
every threshold (the highlighted result from FEATURE-015/`docs/STATUS-08-08-2026.md`)
-- Bundesliga's totals ROI is deeply negative (-19% to -20%) and dominates the
pooled number. Exactly the kind of masking this file has warned about since
BUG-009 (a pooled-only number hiding a real per-group effect) -- now visible at a
glance because the BY LEAGUE breakdown sits right below the ALL-UP number instead
of requiring 5 separate manual runs to notice.

**Tests:** `tests/test_model_metrics_report.py` (renamed from
`test_model_snapshot.py`) gained 8 new tests covering `discover_leagues()`/
`discover_seasons()` (including method/league scoping), `pooled_brier()`/
`pooled_roi()` (real end-to-end checks against `backtest_from_predictions`,
confirming true dollar-pooling rather than ratio-averaging), and
`pooled_brier_across_markets()`/`pooled_roi_across_markets()` (one match seeded
with a perfect 1X2 call and a worst-case totals call, confirming the blended
number is the genuine average of both, not either market silently shadowing the
other). Verified live: the default (no-args except `--note`) report runs cleanly
across all 5 leagues, and every number cross-checks exactly against
FEATURE-014/015's previously-recorded values (e.g. ALL-UP ROI @ EV>0% staked/
profit sums exactly match 1X2's + totals' previously-recorded figures). Full
suite: 380 passed, 0 failures.

**2026-08-11 addendum: console-only preview mode, no persistence.** While
discussing whether these persisted files have real ongoing utility (see FEATURE-018
below), user asked for a quick-look path that doesn't add to the permanent record:
running `model_metrics_report.py` with LITERALLY ZERO arguments (not even `--note`)
prints the all-up report and returns without writing anything under
`model_snapshots/`. Passing any flag at all, even `--note` by itself, falls through
to the normal persisted behavior unchanged -- the trigger is strictly `len(sys.argv)
== 1`, not "was --note omitted." 2 new tests (`test_no_args_prints_a_report_and_
writes_no_file`, `test_note_only_still_persists_a_file`) confirm both the no-file
guarantee and that the persisted path isn't accidentally weakened. 15 tests total
in `tests/test_model_metrics_report.py` now; full suite still 380 passed.

---

## FEATURE-018 — Metrics history belongs in a database table, not flat files in `model_snapshots/`

- **Type:** enhancement / tech debt · **Status:** PROPOSED, not started. Logged
  2026-08-11 — user, looking at the growing `model_snapshots/` directory: "i am
  skeptical about the utility [of committing these files]. if there is utility, it
  feels like the data belongs in a database and not these individual files."

**Why a snapshot mechanism exists at all (confirmed, not assumed):**
`clear_soccer_model_predictions()` (`core/sports_db.py`) DELETES and re-inserts
every `soccer_model_predictions` row for a given (league, season, method) on every
backfill re-run -- so a method name like `poisson_v4` gets its underlying
predictions destructively overwritten in place the next time anyone re-backfills
after a model tweak. There is currently no other way to recover what the OLD
numbers were once that happens; the committed text files under `model_snapshots/`
are the only record. This is a real, load-bearing reason to persist SOMETHING --
the open question is only the storage form.

**Contrast with `roi_history.py`** (the World Cup side): it computes ROI history
on the fly, directly from `soccer_wc_picks`, with no separate snapshot storage at
all. That works there specifically because WC picks are effectively append-only
once graded -- it would NOT work for club-league metrics, where the source data
(`soccer_model_predictions`) is itself mutated in place per the paragraph above.

**Problem with the current file-based approach:**
- Unbounded growth -- every run adds a file, nothing is ever removed (16 files
  already committed before this session; 6 more from this session alone). Now
  that the all-up report needs zero flags to run, generating one is even easier,
  which will likely accelerate growth.
- Every file repeats the full 17-line "Committed model constants" dump verbatim,
  pure duplication across files that could be one shared row/blob per run instead.
- Not queryable -- "how has Serie A's ROI at EV>0% moved across the last 10
  `poisson_v4` runs" currently means manually opening and diffing several text
  files; a database table makes it one SQL query.
- No policy yet for what's worth keeping -- in practice this session has already
  been informally curating (deleting scratch/smoke-test runs before each commit,
  keeping only ones tied to a real BUGS.md finding), which isn't written down
  anywhere and relies on remembering to do it every time.

**Proposed shape (not built, not fully scoped):** a new `model_metrics_history`
table -- one row per persisted report run: timestamp, method, scope (league/
season/market, or "all-up"), the free-text note, the committed constants
(serialized, e.g. JSON), and the actual Brier/bias/ROI numbers. Still strictly
append-only, same historical-record property the files have today, just
structured instead of flat text. The 16+ already-committed files would stay as
legacy/orphaned history rather than get migrated into the new table.

**Deliberately not started:** user said "leave [the files] for now" -- this is
tracked so the idea isn't lost, not scoped as next-up work. The console-only
preview mode above (2026-08-11) already reduces file growth for casual/exploratory
runs without waiting on this larger change.

---

## FEATURE-014 — Multi-league expansion: Premier League, Bundesliga, La Liga, Ligue 1 — **SHIPPED 2026-08-10**

- **Type:** enhancement (major) · **Status:** SHIPPED 2026-08-10. Logged/built same
  session — the "next big item" after FEATURE-011/BUG-009/BUG-011, since sample-size
  breadth (only Serie A had real data) was repeatedly the limiting factor behind
  several open questions above (e.g. BUG-009's 2-season ROI signal too noisy to trust
  either way).

**What shipped.** All 4 leagues built together (not staged), on `core/leagues.py` — a
new central registry (10 leagues incl. feeder divisions Championship/2. Bundesliga/
LaLiga 2/Ligue 2, needed for BUG-010's promotion/call-up cross-league continuity)
replacing scattered per-script constants. Teams/matches for the 4 new leagues come
from TheStatsAPI (`import_league_matches.py`, generalized+renamed from
`import_lower_division_matches.py`) — Serie A stays on football-data.org for now, see
fast-follow below. Odds come from the same two sources Serie A already used
(football-data.co.uk for historical, The Odds API for live), via
`import_league_betting_odds.py`/`import_league_market_odds.py` (generalized+renamed
from the `import_serie_a_*` scripts) plus a new `core/team_name_maps.py` (one
football-data.co.uk-name -> canonical-name map per league, ~76 teams). All three
ingestion paths are conflict-safe (compare fetched vs. stored, log real differences,
require `--allow-overwrite` to apply — never silent overwrite). Also: renamed
`api_match_id` -> `thestatsapi_match_id` (`soccer_matches` column, now that a second
source is in play for the same tables); added `idx_player_stats_season` (missing
index directly on BUG-011's hot path, found while scale-checking this expansion);
fixed `ensure_soccer_team`'s cross-country name-collision gap (the `soccer_teams.name`
global-uniqueness that BUG-010 depends on was never checked against a DIFFERENT
country's same-named club — newly plausible going from 1 country to 5). Full design
discussion/decisions: see the approved plan this was built from (referenced in
session; not persisted as a repo file). Data backfilled: teams+matches+squads+player-
stats for all 4 new top-flight leagues plus their feeder divisions, 2024+2025 seasons
(~2,860 matches, ~1,860 with odds); `PLAYER_RATING_CROSS_LEAGUE_GOAL_ADJUSTMENT` given
a `1.0` entry for each new league (peer top-5 leagues, matching Serie A's own default
— **an assumption, not empirically derived** like Serie B's real `0.663`; flagged for
later validation, not proven-correct).

**Two real bugs found and fixed while validating (not present before this session,
both only exposed by having a genuinely NEW league's first tracked season):**
1. `core.poisson_model.get_league_averages()` — a thin match-history window early in
   a brand-new league's first season can average to exactly `0.0` goals on one side by
   chance (hit for real backfilling Premier League 2024: the 2nd distinct kickoff time
   processed had exactly one prior completed match, 1-0, making `avg_away=0.0`).
   Downstream code divides by both `avg_home` and `avg_away` as separate baselines, so
   this was a `ZeroDivisionError`, not just a noisy estimate — Serie A never hit this
   because it always has 100+ matches of history (the function's own window size) by
   the time any backfill runs. Fixed: fall back to the same default used for a fully
   empty window (`{"avg_home": 1.3, "avg_away": 1.1}`) whenever the computed average is
   `<= 0` on either side, instead of trusting a degenerate small sample. New regression
   test (`test_get_league_averages_falls_back_when_window_averages_to_exact_zero`);
   one pre-existing test's synthetic fixture happened to also hit this exact edge case
   (all 3 of its matches had `away_score=0`) and needed its scores adjusted to keep
   testing what it actually meant to test (decay weighting, not the zero-guard).
2. `model_snapshot.py`'s `compression_bucket_table()` and pooled-bias section both
   called `compare_model_vs_market_odds.fetch_pairs()` without the new `league`
   positional argument — a call site missed when that function was generalized (this
   session) from a hardcoded-Serie-A signature to `--league`-driven. Caught immediately
   (`TypeError` on the first new-league snapshot run) since every league now exercises
   this path; fixed by threading `league` through both call sites. No behavior change
   for Serie A (verified via a regression snapshot run before/after).
3. `model_snapshot.py` named its output file `{timestamp}_{method}.txt` -- no league
   in the filename, since every prior run had always been Serie A. Looping over the 4
   new leagues (same method, same second) produced 4 identical filenames; each
   overwrote the last, silently destroying 3 of the 4 result files on disk (only
   Ligue 1's survived -- the console output was still correct, just not what got
   persisted). Found while generating the table below for a second, clean pass across
   all 5 leagues. Fixed: filename is now `{timestamp}_{league}_{method}.txt`.

**Validation results (`poisson_v4`, both seasons, vs. Betfair Exchange closing /
Bet365) — full snapshots in `model_snapshots/20260810_193101_{league}_poisson_v4.txt`
(one file per league; regenerated cleanly after bug 3 above was found+fixed):**

| League | Brier (2024/2025) | Bias home/away 2024 | Bias home/away 2025 | ROI EV0/5/10% 2024 | ROI EV0/5/10% 2025 |
|---|---|---|---|---|---|
| Serie A | 0.591 / 0.613 | -0.004 / +0.012 | -0.012 / +0.017 | -11.0/-16.4/-19.1% | -5.3/-4.4/-7.3% |
| Premier League | 0.596 / 0.634 | -0.024 / +0.025 | -0.011 / +0.011 | +8.2/+13.0/+14.4% | -6.3/-6.3/-5.4% |
| Bundesliga | 0.661 / 0.594 | -0.003 / +0.007 | -0.012 / +0.018 | -9.7/-12.9/-12.1% | -22.4/-30.3/-37.4% |
| La Liga | 0.625 / 0.589 | +0.012 / -0.001 | -0.000 / +0.019 | -12.4/-13.1/-18.4% | -10.3/-15.1/-13.7% |
| Ligue 1 | 0.620 / 0.602 | -0.010 / +0.036 | +0.016 / -0.005 | +3.7/+1.3/+3.7% | -16.1/-12.2/-13.9% |

Serie A row included for reference (its own fresh run, same batch, same shipped code
-- confirms nothing regressed for the original league while generalizing). Brier
scores all comfortably beat the ~0.667 naive baseline and sit in the same range across
all 5 leagues — no sign of a league-specific data problem (a bad team-name mapping or
a missing cross-league adjustment would show up as an outlier here, and none of the 4
new leagues does, relative to Serie A's own baseline). Bias is mostly inside or near
the documented +/-0.01-0.02 target, Ligue 1 2024's away-side +0.036 the one clear
outlier worth a second look once more seasons exist. **ROI is negative at most
thresholds for most leagues/seasons, Serie A included** (Bundesliga and La Liga
worst) — this matches BUG-010's already-documented, already-
open finding that `poisson_v4` has negative ROI at every threshold despite clearing
the bias bar, **not a new problem introduced by this expansion**. Net read: the
pipeline generalized correctly (no new distinct failure mode per league), and the 4
new leagues inherit the exact same known, already-tracked model weakness Serie A has
(BUG-010) rather than surfacing a fresh one.

**Fast-follow, explicitly tracked, not yet started:** migrate Serie A onto the same
TheStatsAPI-sourced ingestion pipeline as the 4 new leagues (`import_league_matches.py`
instead of `update_serie_a_results.py`), so all 5 leagues share one data source for
teams/matches instead of Serie A being the odd one out on football-data.org. Requires
a one-time reconciliation: match Serie A's existing `soccer_matches` rows (no
`thestatsapi_match_id` today) against TheStatsAPI's own matches for the same league/
season by date+team name, stamp the id on, validate via dry-run diff before committing
anything, then retire `update_serie_a_results.py`. Deferred out of this session's scope
specifically to avoid risking already-in-production Serie A data on the same pass as
new-league buildout — user's explicit call.

---

## FEATURE-015 — `model_snapshot.py` only reports the 1X2 market; wire in totals (over/under) — **Brier + ROI SHIPPED 2026-08-11, bias explicitly deferred**

- **Type:** enhancement · **Status:** Brier and ROI done; bias still has no data
  source to check against (see below) — not a wiring gap, a real data gap, tracked
  separately below rather than left implicit. Logged 2026-08-10 while generating
  FEATURE-014's 5-league validation snapshots (user asked whether those bias/ROI
  numbers covered every market the model supports; they didn't). Built 2026-08-11
  per the user's own follow-up ("I want to do FEATURE-015 next").

**What shipped.** `model_snapshot.py` now has two new sections alongside the
existing 1X2 ones: `totals_brier_score()` (same sum-of-squared-errors shape as the
1X2 Brier function, scored on `p_over`/`p_under` vs. actual total-goals-over/under
the line stored on the prediction row itself; pushes and rows with no totals
prediction are excluded, matching `run_totals()`'s own push handling) and a totals
ROI section that calls the already-existing `backtest_from_predictions.run_totals()`
per season/EV-threshold. Both are printed as their own sections, never pooled with
the 1X2 numbers (matching `run_totals()`'s own documented convention). A third
section, `## Bias, TOTALS/over-under: not available`, makes the gap explicit in
every snapshot's output instead of silently omitting it — `soccer_market_odds` (the
sharp-book reference every existing bias check runs against) has no O/U columns at
all, so there's currently no sharp-book totals line to compare against; adding one
is out of scope here, tracked as its own open question, not assumed away.

New test file `tests/test_model_snapshot.py` (5 tests) covers `totals_brier_score()`
directly: perfect/worst-case scoring, push exclusion, missing-prediction exclusion,
pooling across matches. Verified live across all 5 leagues (`model_snapshots/
20260811_115505_*_poisson_v4.txt` + Ligue 1's `_115506_`) — totals Brier lands close
to the ~0.5 naive baseline for every league (0.506-0.518 pooled), no outliers; totals
ROI is genuinely positive at every EV threshold for Serie A (+5.5/+9.4/+19.0% at
EV>0/5/10%, 2025), matching the pattern already flagged in `docs/STATUS-08-08-2026.md`
as "the one place in the whole model where [ROI] is monotonically positive."

**Open, not scoped further:** whether to start ingesting a sharp-book totals odds
source (so a real bias check becomes possible), and if so, which one and at what
cost/effort — deliberately left as a future decision, not defaulted into this pass.

---

## FEATURE-016 — `generate_club_league_card.py` doesn't persist its picks anywhere, so they can't be scored later — **SHIPPED 2026-08-19**

- **Type:** enhancement · **Status:** SHIPPED 2026-08-19. Logged 2026-08-10 —
  noticed while answering a question about what `generate_club_league_card.py` writes
  to the database (answer: nothing at all, confirmed by reading the script).

**Problem.** `generate_club_league_card.py` reads matches/odds, computes picks
(`compute()` + `analyse_match_wc()` + guardrails), and prints the card straight to
stdout — no `INSERT`, no `conn.commit()`, nothing. Once the terminal output is gone,
there's no record of what the model actually picked for a given matchday, so there's
no way to later check whether those specific picks won or lost.

**Contrast.** `generate_wc_card.py` already solves exactly this for the World Cup
side: its own docstring says it "stores the pick in `soccer_wc_picks` for later
scoring" (and has a `--no-store`-style flag to print without storing, implying
storing is the default). `soccer_wc_picks` (`core/sports_db.py`) is a small table —
`match_id`, `generated_at`, `side`, `odds`, `model_prob`, `ev`, `stars`, `result`,
`selection_mode` — that this club-league side has no equivalent of. This is also the
literal problem FEATURE-012 (pick/probability lineage) is scoped to help with
generally, so this could plausibly be built together with, or as groundwork for,
FEATURE-012 rather than as a one-off table.

**Not urgent, but not new either** — flagged now, but per the user: the project
already has a running "add this later" list, this file's own PROPOSED/not-started
entries (FEATURE-012, FEATURE-013, FEATURE-015 above, and this one), plus
`docs/STATUS-08-08-2026.md`'s Feature Backlog section (more markets, line shopping,
Kelly staking) which explicitly defers to this file as the canonical source. This is
another item for that same list, not something scoped to build immediately.

**Shipped 2026-08-19, ahead of an upcoming weekend where the club-league card was
about to actually get used.** Built as its own small table rather than folded into
FEATURE-012 (still not started) — that stayed the right call once actually built:
FEATURE-012's lineage/traceability scope is broader than just "can this get graded
later." New table `soccer_club_league_picks` (`core/sports_db.py`) — `pick_id`,
`match_id`, `league` (denormalized, matching every other multi-league table),
`generated_at`, `side` (1X2 or totals — one match can produce up to
`MAX_PICKS_PER_MATCH` picks across different markets, unlike `soccer_wc_picks`'
one-pick-per-match shape), `odds`, `model_prob`, `ev`, `stars`, `result`. Two new
functions: `replace_club_league_picks_for_match()` (deletes+reinserts a WHOLE
match's ungraded picks in one call, not per-pick — a per-pick delete would wipe out
a same-match pick just inserted a moment earlier) and
`set_club_league_pick_result()`. `generate_club_league_card.py` stores by default,
new `--dry-run` flag to print without storing (same flag/behavior as
`generate_wc_card.py`). 7 new tests (`tests/test_db_smoke.py`,
`tests/test_generate_club_league_card.py`): multi-pick storage, replace-supersedes-
ungraded, replace-preserves-graded, grade round-trip, store-by-default, dry-run,
re-run-doesn't-duplicate. Full suite green (472 passed). Verified end-to-end against
the real database (a past matchday with real odds, since no future fixtures were
loaded yet) before being reset to empty for real use.

---

## BUG-011 — `compute_club_player_strength.compute()` redundantly recomputes last-season aggregates on every matchday during a backfill, making full-season backfills slow — **FIXED 2026-08-08**

- **Type:** performance (no correctness impact) · **Severity:** low (cosmetic/rare —
  slows backfill/backtest runs, doesn't affect any stored prediction) · **Status:**
  FIXED 2026-08-08. Logged 2026-08-07 while running FEATURE-011 Follow-up B's
  post-implementation bias/ROI validation (see FEATURE-011 entry below).

**Context.** `backfill_player_blend_predictions.py` processes a season one matchday
(exact `match_date`) at a time and calls `compute()` fresh for each — profiled at
~1.6s/call, ~318 distinct match_dates for a 380-match Serie A season (kickoff times
vary within a "matchday", so it's not ~38 calls), i.e. several minutes per season.

**Root cause (confirmed via `cProfile`).** 1.36s of each 1.6s `compute()` call is
inside `player_trust_score()` (`compute_club_player_strength.py:429`), called 40x per
matchday (20 teams x 2 blend components). Each call re-runs `team_roster_minutes()`
and `player_season_minutes()` (`compute_club_player_strength.py:347-386`) as fresh SQL
aggregates over **last season's** data. But both functions' inputs (`team_id`,
`season - 1`) are fixed for the entire backfill run — last season's data can't change
while backtesting the CURRENT season — so the same aggregate gets recomputed from
scratch ~12,700+ times per season (318 matchdays x 40 calls) when it only needs
computing once per team.

**Fix.** Added an optional `cache` dict threaded through `compute()` ->
`resolve_blend_weight()` -> `player_trust_score()`, via a small `_memoized(cache, key,
fn)` helper. `cache=None` (the default everywhere) is a plain passthrough — identical
behavior to before this fix, safe for one-off/live calls. A caller looping over many
matchdays (a backfill/backtest script) creates one `cache = {}` before the loop and
passes it to every `compute()` call; `team_roster_minutes(team_id, last_season)` and
`player_season_minutes(last_season)` then get computed once each and reused for the
rest of the run instead of ~12,700+ times. Wired into the three scripts that loop over
a full season: `backfill_player_blend_predictions.py`, `backfill_with_xg_stretch.py`,
`oracle_roster_blend_test.py`. Not wired into `generate_club_league_card.py` (a live,
single-card run — negligible benefit, not the target of this fix).

**Verified.** New test
(`test_player_trust_score_cache_avoids_recomputing_last_season_aggregates`) confirms
the underlying aggregates are called exactly once per team across repeated calls
sharing a cache, and that cached vs. uncached results are identical. Measured
end-to-end on a real Serie A season backfill (380 matches, 2025): **625s -> 248s
(~2.5x faster)**. Full `test_compute_club_player_strength.py` suite still 44 passed /
1 pre-existing (unrelated, documented-as-expected-to-fail) failure, same as before
this change — no behavior change for any existing caller.

**2026-08-12 addendum: same class of bug found and fixed one level down, in
`get_team_xg_ratings()`.** Profiled a live backfill again after the multi-league
expansion made runs slow enough to matter (~30min for a full metrics validation
pass) — `get_team_xg_ratings` (queries a team's own recent xG/xGA rows) had NO team
filter in its SQL at all, the exact same bug class as `load_team_players` (see
BUG-010's 2026-08-11/12 entry) — it pulled matches for every team in the league on
every call, filtering in Python afterward. Fixed by rewriting its two queries
(`venue_rows("home", ...)` / `venue_rows("away", ...)`) to filter by `team_id` in
SQL directly with `ORDER BY match_date DESC LIMIT n`. Also found and fixed a
redundant double-call of the same function for the same `(team_id, before_date,
league, n)` between `league_xg_field_means` and `team_level_lambda` — memoized with
the same `_memoized(cache, key, fn)` helper this bug's original fix introduced.
**Verified byte-identical**: backed up `soccer_model_predictions` before the change,
re-ran a full backfill after, diffed every prediction field (`p_home`, `p_draw`,
`p_away`, `lambda_home`, `lambda_away`) row-by-row — exact match, confirming this is
pure performance work with zero behavior change. Measured: 2.25x faster (one
league-season backfill, 5:11 -> 2:18) — short of an initial ~8-15x profile-based
estimate, owned as an overpromise rather than defended; the real bottleneck besides
this fix is CPU-bound `compute()` work itself, not further redundant queries.

---

## BUG-010 — poisson_v4 (player-level xG blend, FEATURE-011) generates wildly overconfident home-win probabilities for underdog home teams, driving negative ROI despite clearing the bias criterion

- **Type:** model calibration (tail behavior) · **Severity:** high (this is the concrete
  lead behind poisson_v4/poisson_v4_teamxg's negative ROI at every EV threshold, both
  seasons tested) · **Status:** PARTIALLY FIXED — same underlying phenomenon as
  BUG-009 (see that entry's dollar-impact quantification for why this stays worth
  revisiting). THREE real contributing mechanisms found and fixed 2026-08-11/12
  (two in the trust-score window that gates player-vs-team-level blending, one
  cross-league defense-gating asymmetry), one real calibration lever shipped
  (`PLAYER_RATING_SPREAD_STRETCH_ATTACK=2.0`), and every remaining candidate in
  this investigation's own hypothesis list has now been tested and correctly left
  off (player/team defense stretch, opponent-adjust — see 2026-08-19 below). No
  further untested candidate remains here; next progress needs a structurally
  different idea, same as BUG-009. Logged 2026-08-02, see progress notes below.

**Context.** FEATURE-011's player-level blend (`compute_club_player_strength.py`,
method `poisson_v4`/`poisson_v4_teamxg`) cleared the Model Calibration success
criterion (signed bias vs. sharp closing lines, ±0.01-0.02) after switching team-level
ratings from noisy last-10-matches actual goals to xG/xGA. But the ROI success
criterion (`backtest_from_predictions.py` vs. Bet365) still fails at every EV threshold,
both seasons (2024-25: -14.9%/-20.0%/-21.9% at EV>0/5/10%; 2025-26: -9.5%/-5.9%/-2.0%),
and gets *worse* as the threshold tightens in 2024-25 -- the "noise not edge" signature
(see BUG-009's ROI note for the same pattern found there).

**Finding.** Broke the ROI backtest down by side (home/draw/away) and by matchday count
at each threshold. Home is the worst-performing side in both seasons at every
threshold (2024-25: -30.4%/-35.3%/-33.9%; 2025-26: -17.6%/-18.2%/-13.9%) and the number
of home bets clearing EV>10% is large relative to the pool -- 102 of 350 home-side rows
in 2025-26 (29%). Pulling the raw `p_home` vs. Bet365-implied-probability for those
EV>10% bets: they are almost all **big underdogs** (moneylines +300 to +650, i.e.
market-implied ~15-25% win probability) where the model assigns `p_home` of 28-47% --
roughly double the market's read. Median `model_p - implied_p` gap among EV>10% home
bets in 2025-26 is +0.077.

**Not the same shape as the aggregate bias.** Averaged across all 350 home-side rows,
the model's mean `p_home` (0.409) is actually *below* Bet365's mean implied `p_home`
(0.445) -- consistent with BUG-009's known aggregate home-underestimation. So this isn't
"the model is systematically better at pricing home teams than the market" broadly; it's
a small subset of matches -- weak/underdog home teams specifically -- where the model's
home lambda estimate spikes hard in the other direction, and those spikes are exactly
what clears the EV bar and gets bet.

**Leading candidates (not yet checked):** thin/mismatched current-squad data for
weaker clubs skewing `load_team_players`'s player-level blend for the home side; the
xG-based team rating (`get_team_xg_ratings`) being unstable for bottom-table teams with
fewer minutes/matches in the lookback window (small-sample xG variance, same class of
issue as the actual-goals last-10 window this replaced); or a blend-weight resolution
issue that under-trusts team-level (stabilizing) signal specifically for weak teams.

**Next step:** pull the specific matches behind the largest home EV blowups (e.g. the
`ev=+1.7`, `model_p=0.418` vs `implied_p=0.154` case in 2025-26) and trace which
component (player blend vs. team xG rating vs. blend weight) is producing the inflated
home lambda for that team/date.

**2026-08-11/12 progress: ONE real contributing mechanism found and fixed --
Status stays OPEN, this is not the root cause, just a confirmed contributor.**
Revisited BUG-010 after the multi-league expansion (FEATURE-014) gave 5x the data to
check the pattern against. Doing exactly what the "Next step" above asked -- pulling
the actual worst home-underdog blowups and tracing the blend components -- found:

`player_trust_score()`'s "prior roster" reference was hard-coded to a literal
`season - 1` DATABASE LABEL, not an actual date lookback. Any league with nothing
under that specific label (a newly-added league's first tracked season) collapsed
EVERY team's trust to a hard 0.0 -- confirmed directly: Holstein Kiel hosting FC
Bayern München (2024-09-14, `model_p_home=0.410` vs market-implied `0.091`) had
BOTH teams at exactly `trust=0.000`, not because either team's player data was bad,
but because the database had no rows under the literal season label "2023" for
either. 17 of the 20 worst home-underdog blowups checked showed this same exact
signature (both teams' trust hard-zero).

Fixing this took more than the season-label swap -- two more real bugs surfaced
along the way, all part of the same fix:
- The replacement window (team's own last N matches, season-blind) initially still
  overlapped with "current roster" (this season's own matches are always inside
  "the last N matches"), so a genuinely NEW signing could never register as
  "joined" -- only departures were detectable. Fixed by anchoring the prior-roster
  window to the season's own start date (`league_season_start_date()`), not
  `before_date` -- two genuinely non-overlapping periods to compare.
- `PLAYER_RATING_MIN_MINUTES_RECENT_WINDOW` (900.0) was calibrated for the OLD
  definition (a whole season's minutes, thousands available) but the new window
  caps at 900 minutes MAXIMUM (10 games x 90 min) -- checked directly against 241
  real roster slots across 10 Serie A teams: only 1 player (0.4%) could ever clear
  900 under the new definition, so `data_coverage_score` was ~always 0 regardless
  of real roster quality. Recalibrated to 300 (data-driven, not fitted -- ~48% of
  real roster slots clear it; no natural gap/step exists in the real distribution,
  it's a smooth curve, so this is a reasoned choice, not a discovered constant).
- `load_team_players()` (pre-existing, untouched until now) had NO team/player
  filter in its SQL at all -- pulled the entire `soccer_player_stats` table on
  every single call, filtering in Python afterward. Harmless when this was written
  against a much smaller dataset; became the dominant backfill cost (15 of 23s in
  a live profile) once this session's multi-league expansion + a new 2023-season
  backfill (added specifically to give the fixed trust mechanism real historical
  depth) grew the table past what it was designed for. Fixed by narrowing to a
  candidate player pool (who's appeared in the requested teams' own last N
  matches) before the full history fetch -- 2.6x faster (781ms -> 298ms per
  `compute()` call), and arguably MORE correct, not just faster: a player who
  hasn't appeared in longer than the team's own recent match window isn't
  plausibly part of what the team's doing now, so excluding them is right, not a
  lossy shortcut.

Also renamed the core concepts throughout (`squad` -> `current_roster`,
`recent_roster` -> `aggregated_recent_roster`) for clarity, per direct user
request while working through the design.

**Validation, on the actual case that found this (not just the aggregate ROI number
alone):** Holstein Kiel vs Bayern's `model_p_home` moved from 0.410 to 0.333 (market-implied:
0.091) -- real, meaningful, but not resolved. Across the worst 15 home-underdog
blowups league-wide, combined EV severity dropped ~11% (35.4 -> 31.5) and the count
of extreme cases (EV>2.5) dropped from 7 to 5. All-up Brier improved 0.5619 ->
0.5487. **ROI is still negative at every threshold, and the general weak-home-vs-
elite-away overconfidence pattern still shows up** (new worst case: Burnley hosting
Manchester City, `model_p=0.247` vs `implied=0.053`) -- reduced, not eliminated.
Full numbers: `model_snapshots/20260811_202910_all_leagues_poisson_v4.txt`.

**Conclusion: this was a real, confirmed contributing mechanism, not THE root
cause.** Status stays OPEN. Next step, same shape as before: pull the NEW worst
blowup cases (Burnley/Man City is the current #1) and trace which component is
still producing the inflated home probability now that the trust-score cliff is
fixed -- the original "leading candidates" list above (xG small-sample instability
for weak teams, a still-imperfect blend-weight resolution) is still the live
hypothesis set.

**2026-08-12 progress: a SECOND, distinct mechanism found and fixed in the same
trust-score window -- still not THE root cause, Status stays OPEN.** Traced the
new worst case named above (Burnley hosting Manchester City, 2026-04-22).

**Finding.** Both teams' `resolve_blend_weight` had collapsed to near-zero team
weight (Burnley: `weight_team=0.019`, i.e. 98% pure player-level; Man City:
`0.253`) -- NOT because of the season-label bug (already fixed), but because
`player_trust_score`'s "prior roster" reference was anchored to `season_start_date`
(2026-08-11's own fix), computed once and reused for EVERY matchday for the rest
of the season. This match is 2026-04-22 -- 8+ months after Burnley's summer
transfer window. By then Burnley's team-level rating (`TEAM_PAST_MATCH_WINDOW_SIZE
=10`) was built entirely from real games WITH the current roster (a solid, current
signal on a struggling promoted side: weak home attack 1.098, leaky home defense
1.898), but the trust score still compared today's roster against the stale August
snapshot, measured churn at 0.98, and threw the team-level signal away almost
entirely in favor of the player-level read -- which rated Burnley's home attack
higher (1.321) and, critically, their home defense as LEAKING LESS (1.451 vs
team-level's 1.898). With near-zero team weight, the blend took the optimistic
player-level numbers almost unchanged, driving `p_home` to 0.247 against a
market-implied 0.053-0.063.

**Root cause: the reference window was computed once but compared against a
window (`team_level_lambda`'s own last-10-matches rating) that moves every
matchday -- an illogical mismatch once you notice it.** User's diagnosis,
confirmed without needing further evidence. Fixed by replacing the
`season_start_date` anchor with `team_prior_window_cutoff_date()`: the "prior
roster" is now the team's own `window` matches immediately preceding its CURRENT
`window`-match window (the same one `team_level_lambda`'s rating is built from) --
two adjacent, non-overlapping periods, both anchored to `before_date`, that
genuinely shift forward every matchday. A squad overhaul now reads as "new" right
after it happens and stops being flagged once enough same-roster matches exist to
fill its own window, instead of staying flagged for an entire season.
`player_trust_score`/`resolve_blend_weight` no longer take a `season_start_date`
parameter at all (removed, along with the now-dead `league_season_start_date()`);
`resolve_blend_weight` gained a `window` passthrough so `compute()`'s own
`player_window_size` stays the SAME horizon the churn check uses, per
`player_trust_score`'s own documented invariant.

**Validation, on the case that found it:** Burnley/Man City's `model_p_home`
moved from 0.247 to 0.108 (market-implied: ~0.06) -- the gap shrank from +0.184 to
+0.049, and the match dropped out of the top-15 worst blowups entirely. Across the
worst 15 home-underdog blowups league-wide (same method as 2026-08-11's check,
re-run fresh since the ranking shifts as the worst cases change), combined EV
severity dropped ~24% (31.5 -> 23.9) and extreme cases (EV>2.5) dropped from 5 to
2. Aggregate home bias, the direct symptom this bug family targets, is
essentially eliminated: `home=-0.0060` -> `home=+0.0000` (all-up, vs Betfair
Exchange). **Mixed on the other two numbers, reported honestly rather than
cherry-picked:** all-up Brier ticked UP slightly (0.5487 -> 0.5586, n=7066) and
ROI stayed flat/mixed across thresholds (EV>0%: -7.5%->-7.7%; EV>5%:
-8.2%->-8.0%; EV>10%: -6.7%->-7.5%). A small Brier regression alongside a clear
bias improvement is plausible (more probability mass now sits nearer 0.5 for
teams whose churn genuinely does carry real signal, trading some sharpness for
correctness) but not yet explained instead of assumed. Full numbers:
`model_snapshots/20260812_103003_all_leagues_poisson_v4.txt`.

**2026-08-12: corrected an inaccurate claim used repeatedly in this file --
"ROI is too noisy to trust at this sample size" is FALSE for the pooled all-up
number itself.** Computed the real statistics instead of assuming: at n=6988
individual bets (ALL-UP, EV>0%, both markets pooled), per-bet stdev is 1.40,
giving a standard error of 1.67% on the mean -- the current -7.7% ROI has a 95%
CI of [-11.0%, -4.4%], entirely below zero (t=-4.61). That is a statistically
robust, trustworthy finding: the model does NOT currently have a betting edge,
and this is not an artifact of insufficient data. A power calculation (80%
power, 95% confidence, sigma=1.4) shows why: detecting an effect as large as the
current -7.7% needs only ~2,600 bets, well within what we already have; only
detecting something much SMALLER -- e.g. a modest 2% edge -- would need ~38,000
bets (~10+ more seasons at current volume). What genuinely IS underpowered is a
narrower claim this file has sometimes conflated with the above: comparing two
MODEL VERSIONS against each other on a small delta (e.g. this entry's own
-7.5%->-7.7% shift after the 2026-08-12 fix, well within the ±1.67% SE) --
that specific before/after comparison can't be read confidently, but "is the
model currently profitable" can. Caveat: this treats bets as independent, which
is generous (bets share correlated risk via the same underlying model bias
across a league/team), so the true noise floor is likely somewhat worse than
these numbers suggest -- not corrected for here.

**Conclusion: a second real, confirmed contributing mechanism inside the SAME
trust-score window, not THE root cause.** Status stays OPEN. Both fixes so far
have targeted the player-vs-team blend weight specifically; the original "leading
candidates" (xG small-sample instability for weak teams, a still-imperfect
blend-weight resolution beyond just the window boundary) remain the live
hypothesis set. Next step: pull the current worst blowup case fresh (re-run the
same top-15 query against `soccer_model_predictions`/`soccer_market_odds` -- the
ranking has shifted now that Burnley/Man City is gone) and trace it the same way.

**2026-08-12 progress: a THIRD mechanism found and fixed (cross-league defense
gating asymmetry), plus a structural compression issue found and a fix mechanism
built (not yet calibrated) -- Status stays OPEN.** Traced the next blowup case
(Real Oviedo hosting Real Madrid) the same way as the two prior fixes -- corrected
a methodology slip along the way (only 2 teams were passed to `compute()` instead
of the full league, which `attack_mean`/`defense_mean` need; corrected result:
`p_home` 0.385 -> 0.349, still far from the market).

**Finding 1 (fixed): `PLAYER_RATING_CROSS_LEAGUE_GOAL_ADJUSTMENT` gated/scaled
ATTACK only.** This constant exists to exclude/scale games from leagues whose goal
rate isn't calibrated against the primary set (currently only Serie B, factor
0.663, empirically derived) so they don't distort a team's rating. The gate was
only ever applied on the attack side of `load_team_players()`'s per-game loop --
defense accumulated minutes/goals-against from EVERY league a player appeared in,
uncalibrated or not, unscaled. Fixed to gate (include/exclude) both attack and
defense symmetrically -- a game from an uncalibrated league is now excluded from
both accumulators, not just attack. The numeric 0.663 scaling factor itself stays
attack-only (not applied to defense) since it was derived from an attack-specific
self-comparison study with no validated defensive meaning -- reusing it for
defense without evidence would be exactly the kind of guessed-not-measured
constant this project avoids; extending it to defense is future work, pending its
own analysis.

**Finding 2 (fix built, not yet calibrated): player-level attack/defense ratings
are structurally compressed vs. team-level xG ratings, mirroring BUG-009's
already-fixed team-level compression issue one level down.** Continuing to dig on
the same Oviedo/Real Madrid match ("what else pops up as incorrect") found
player-level CV (coefficient of variation, stdev/mean -- the same diagnostic
BUG-009 used) is consistently lower than the corresponding team-level xG CV,
measured on La Liga 2025: player-level away-attack CV=0.157 vs team-level xG
away-attack CV=0.246; player-level home-defense CV=0.082 vs team-level xG
home-defense CV=0.200 (defense more compressed than attack, and more severely than
at the team level). Verifying whether team-level itself already has this
attack/defense asymmetry (it did: goals/xG CV ratio ~1.64 for defense vs ~1.39 for
attack, La Liga 2025) motivated **splitting the existing shared
`TEAM_RATING_XG_SPREAD_STRETCH` into `TEAM_RATING_XG_SPREAD_STRETCH_ATTACK`/
`_DEFENSE`** (both start at the prior shared value, 1.3 -- unchanged-behavior
starting point, team defense not yet separately recalibrated) and **building an
analogous new player-level mechanism**, `PLAYER_RATING_SPREAD_STRETCH_ATTACK`/
`_DEFENSE`, recentering each team's raw player-level attack/defense rate around
the league mean before the home/away unit conversion (`stretched = mean + (raw -
mean) * factor`, same formula as the team-level version). Shipped first as pure
plumbing (both new player-level constants at 1.0, a true no-op) and verified
byte-identical via the same before/after row-level prediction diff used for the
BUG-011 performance fix above.

**Player-level attack calibration (2026-08-12, 5 leagues, season 2025, all-up
pooled): swept 1.0/1.3/1.6/2.0/2.5/3.0.** Bias vs Betfair Exchange never breached
the +/-0.01-0.02 target even at 3.0 (away bias only reached +0.0154) -- no hard
ceiling from that criterion. But Brier degraded monotonically across the range
(0.5689 at 1.0 -> 0.5728 at 3.0) while ROI improvement plateaued after 2.0 (all
three EV thresholds sat in a tight -9.3% to -10.1% band from 2.0 through 3.0, vs
-11.8%/-11.1%/-10.5% at 1.0) -- past 2.0, further stretch bought steady Brier cost
for no further ROI gain. **Shipped `PLAYER_RATING_SPREAD_STRETCH_ATTACK = 2.0`**
(the value capturing nearly all the ROI improvement while Brier degradation is
still modest, +0.001 vs baseline) -- production `poisson_v4` re-backfilled across
all 10 league-seasons with this default. `PLAYER_RATING_SPREAD_STRETCH_DEFENSE`
still 1.0 (not yet calibrated), then `TEAM_RATING_XG_SPREAD_STRETCH_DEFENSE`
after that, per the same discipline.

**Conclusion: a third real, confirmed contributing mechanism (defense gating),
plus a real structural issue (player-level compression) with a fix mechanism now
partially calibrated -- neither is confirmed as THE root cause.** Status stays
OPEN. Next: calibrate `player_spread_stretch_defense`, then
`team_xg_spread_stretch_defense`, then re-trace the current worst blowup case
fresh with all fixes applied.

**2026-08-12: `player_spread_stretch_defense` swept (1.0/1.5/2.0/2.5/3.0, 5
leagues, season 2025, attack held at its locked 2.0) -- NOT calibrated away from
1.0 (no-op), on stronger evidence than "ROI didn't move."** Three independent
checks all point the same direction: (1) aggregate ROI is flat-to-worse at every
candidate above 1.0, not just noisy-flat; (2) aggregate Brier degrades
monotonically across the whole range (0.5699 -> 0.5730), the same shape as the
attack sweep's Brier cost, but attack bought real ROI improvement for that cost
and defense didn't; (3) a targeted check against the exact population this bug
exists to fix -- the current top-15 worst home-win-overconfidence blowups -- found
13 of 15 got MORE overconfident (`p_home` higher) as defense stretch increased
from 1.0 to 3.0, not less, including Atlético Madrid (an elite defense the
player-level signal already rates as roughly average, made more extreme by the
stretch).

Investigated WHY defense behaves differently from attack given both showed real
compression (the original motivation) -- one hypothesis (defense's raw
player-level signal correlates more weakly with the team-level xG reference than
attack's does, so stretching it amplifies more noise) was tested directly and
**did not hold up**: Pearson r between raw player-level rating and team-level xG
rating, averaged across all 5 leagues, is 0.722 for attack vs 0.699 for
defense -- both moderate, not meaningfully different. **The attack/defense
asymmetry in this sweep's results is real (three consistent, independent
signals) but its root cause is NOT understood -- flagged here as an open
question rather than closed with an unverified explanation**, since the first
attempted explanation didn't survive being checked against real data. Worth
revisiting if a future investigation needs it; not blocking -- the mechanism
(`PLAYER_RATING_SPREAD_STRETCH_DEFENSE`) stays in the codebase, fully built and
tested, just left at its no-op default pending better evidence to move it.

**2026-08-12: `team_xg_spread_stretch_defense` swept fast/cheap (2-league pilot
first, then a targeted 5-league confirmation of just the leading candidate --
NOT a full 5-league x N-candidate sweep, per explicit direction to fail fast on a
dead end rather than spend the same hours again) -- NOT calibrated away from its
inherited 1.3.** Pilot (Serie A + Premier League only, n=1580): 1.0/1.6/2.3/2.6
showed a real, monotonic-ish ROI improvement (-5.9%/-3.4%/-3.8% at 1.0 up to
-3.6%/-0.1%/+0.8% at 2.3) with away bias approaching but not yet breaching the
target at 2.3 (+0.0190). Confirming 2.3 at full 5-league scale changed the
picture: away bias +0.0195 AND draw bias -0.0181 -- both now sitting right at the
+/-0.02 ceiling, not comfortably inside it the way the pilot suggested. Brier
cost was also far steeper than any other stretch calibrated this session
(0.5656 -> 0.5955, +0.0299 -- roughly 10x the player-attack stretch's Brier
cost for its whole tested range). The ROI deltas (+0.3%/+0.2%/+2.1% at
EV>0/5/10%, n~2200-3600) don't clear this same file's own established
significance bar (SE~2.4-3.0% at this n) -- this is exactly the underpowered
"version-to-version delta" case flagged in the 2026-08-12 ROI-noise correction
above, not the well-powered "is there an edge" case. **Conclusion: no real
signal survives scrutiny here (Brier and bias both argue against moving, ROI
doesn't clear its own noise floor) -- left at 1.3.** Total cost: ~15 backfill
runs (2-league pilot x 4 values + 5-league confirmation x 2 values) vs. ~50 for
a full blind sweep -- the fail-fast approach caught this before spending the
same hours the player-defense sweep did.

**Net result of the whole four-constant spread-stretch effort (2026-08-12):**
only ONE of the four constants actually moved --
`PLAYER_RATING_SPREAD_STRETCH_ATTACK` (1.0 -> 2.0); the other three
(`TEAM_RATING_XG_SPREAD_STRETCH_ATTACK`=1.3, `_DEFENSE`=1.3,
`PLAYER_RATING_SPREAD_STRETCH_DEFENSE`=1.0) are unchanged from their pre-session
values, each now backed by real tested-and-rejected evidence rather than being
untested assumptions. The one real change: Brier flat (0.5689->0.5699, +0.001),
bias unchanged inside target, ROI ~2 points better at every EV threshold
(-11.8%/-11.1%/-10.5% -> -9.7%/-9.5%/-9.5%) -- a modest, plateau-shaped
improvement (credible because it's the stable endpoint of a 6-point monotonic-ish
sweep, not because any single before/after delta is independently significant).
**BUG-010 is NOT resolved by this** -- aggregate ROI is still solidly negative
(the real, statistically-confirmed finding from the 2026-08-12 correction above).
This is one more modest, partial contributor stacked on the trust-score-window
and cross-league-gating fixes already made, plus two now-disconfirmed hypotheses
(player-defense stretch, team-defense stretch) that no longer need re-testing by
a future investigation, plus a real, separate correctness bug fixed along the way
(defense cross-league gating). Status stays OPEN.

**2026-08-12 addendum: home-bet calibration diagnostic shipped
(`diagnose_home_bet_calibration.py`).** ROI investigation clarified that the
failure is not mean bias vs Betfair but **selection-conditional overconfidence
on home sides that clear EV>0** (optional floor=0.25). Tool has two modes: (1)
fast slice report on stored predictions — calib = model_p − realized wr, by
league/season/month/gap_bf/λ-diff/team; (2) `--deep-dive N` point-in-time
recompute of player/team/blend λ components (same path as
`backfill_player_blend_predictions.py`) for the N largest model−Betfair home
gaps. First live read on current `poisson_v4` (floor on, 5 leagues × 2024–25):

- Home EV+ floor bets: n=1330, ROI −8.3%, **calib +0.124** (model 0.557 vs wr 0.433).
- **gap_bf ≤ 0.05**: calib ~0, ROI **+6.3%**. **gap_bf > 0.15**: calib **+0.202**.
  Disagreement with sharp ranks error, not edge.
- Bundesliga home-bet calib **+0.199** / ROI −28%; Ligue 1 least bad (+0.078 / +8% ROI).
- Deep-dive top-12 by gap_bf: mean p 0.679 vs wr 0.167; recomputes match stored
  rows (Δ=0). Cohort is **team-weight heavy** (mean home/away w≈0.80–0.83), not
  pure-player trust collapse. Mean (player−team) home_att ≈ −0.09 (mixed);
  away_def player is **lower** than team on average (player rates elite away
  defenses tighter) — so the inflated p_home on this tail is often already in
  **team-level λs** (and/or matchup math), not only a player-blend blow-up.
  Counterexamples still exist (e.g. Levante/Madrid, Southampton/Man Utd with
  low home w_att and player attack >> team).

**2026-08-12 follow-up: Pattern A/B classifier + control cohort in the same
tool.** `--deep-dive N` now labels each recompute (A = home team-weight ≥0.85;
B = home team-weight ≤0.40 and player home-att − team ≥0.15; else MIXED) and,
by default, also recomputes N low-|gap_bf| controls (`--control 0` to skip).
First live n=8 / n=8:

- Overconfident tail: **A=5, B=2, MIXED=1**. Pattern A mean p=0.76 vs wr=0.20;
  away **team** attack mean only **0.86** (crushed visitor attack in team form).
  Pattern B: home att player−team **+0.53**, w≈0.18.
- Controls (mean gap_bf≈0, wr=0.50≈model 0.51): **no B**; A=4 MIXED=4; away team
  attack **0.99** (not collapsed like bad-tail A). So high team weight alone is
  not the bug — bad A cases look like **extreme team matchup tilt** (especially
  dead away attack), not merely “used team blend.” Status stays OPEN.

**2026-08-12 follow-up: structural team-xG lookback (not another stretch sweep).**
Diagnosis: live v4 team form is a **flat last-10 sum-of-player-xG mean** with
**no opponent adjustment**; goals-path decay exists but is 1.0 and unused under
pure xG. Shipped wire-up (defaults **unchanged** so stored poisson_v4 is a
no-op):

- `TEAM_RATING_XG_WINDOW_DECAY` (default 1.0) + `get_team_xg_ratings(..., decay=)`
- `TEAM_RATING_XG_OPPONENT_ADJUST` (default False): point-in-time scale each past
  match’s xG/xGA by opponent’s **raw** defense/attack as of that match date
  (two-pass league means; no circular adjust-on-adjust).
- Threaded through `league_xg_field_means` / `team_level_lambda` / `compute`
  (`xg_window_decay=`, `xg_opponent_adjust=`).

Unit tests cover decay weighting, SOS boost vs stingy defense, and default =
legacy mean. First live case A/B on Pattern A examples (full `compute`, not
production default): **mixed** — e.g. Milan–Roma gap_bf improved with opp_adj
(+0.245→+0.181) while Auxerre–St-Étienne and Freiburg–Bremen did not; City
away attack correctly rose under SOS but home p gap worsened. Takeaway: the
plumbing is the right layer to attack, but opponent-adjust alone is not a
free lunch — needs method-tagged season backfill + home-bet calib / bias /
ROI before flipping defaults.

**2026-08-19: the deferred full backfill run — validated, NOT shipped.** Added a
`--xg-opponent-adjust` CLI flag to `backfill_player_blend_predictions.py` (didn't
exist before; this candidate had never actually been backfilled) and ran it
across the full standard scope (5 leagues x 2024/2025 + Serie A 2022/2023) vs.
`poisson_v4_4` baseline, Brier/bias as the primary signal per this session's
established discipline (see BUG-009: ROI is a weak validation signal here).

Pooled: Brier worse (0.5964→0.5980), draw/away bias both widen, away breaches
the ±0.02 target (+0.017→+0.024). Per league, genuinely split rather than a
uniform win or loss: Bundesliga improves clearly (Brier 0.5979→0.5930, guardrail
ROI roughly halves its loss), Premier League improves slightly; Serie A and
Ligue 1 both get meaningfully worse (Serie A Brier 0.6013→0.6069, guardrail ROI
-3.7%→-11.0% at EV>0%); La Liga near-flat. Same shape as every BUG-009 candidate
tested this session (constant-family fixes: real in some slices, costly in
others, net negative on Brier/bias pooled) — **not promoted to default.** Flag
stays in the codebase (`--xg-opponent-adjust`, `TEAM_RATING_XG_OPPONENT_ADJUST`
default `False`), tested, available if a future investigation wants to build on
it, same treatment as BUG-009's team-credibility de-shrink.

**This was the last unvalidated candidate in this bug's investigation.** BUG-010
and BUG-009 are very likely the same underlying compression phenomenon studied
from two angles (BUG-010 found it via "overconfident home underdogs," BUG-009
via pooled bias buckets) — they share the exact same tunable constants
(`PLAYER_RATING_SPREAD_STRETCH_ATTACK/_DEFENSE`, `TEAM_RATING_XG_SPREAD_STRETCH_
ATTACK/_DEFENSE`) and every fix attempted in either investigation shows the same
trade-off shape. **Status: PARTIALLY FIXED, same as BUG-009** — three real
mechanisms found and fixed (trust-score window x2, cross-league defense gating),
one real calibration lever shipped (`PLAYER_RATING_SPREAD_STRETCH_ATTACK=2.0`),
several candidates tested and correctly left at their no-op defaults with real
evidence behind that choice (player/team defense stretch, opponent-adjust). No
further untested candidate remains in this investigation's own hypothesis list.
See BUG-009 for the current best estimate of the dollar value in fixing the
shared underlying problem, and for why "no further lever found" is a reason to
change approach on the next attempt, not a reason to deprioritize it.

---

## FEATURE-011 — Player-level lambda model for club leagues

- **Type:** enhancement (major) · **Status:** PROPOSED, not started. Logged 2026-07-27.
  **Priority: highest** of three ideas logged together this session (user's explicit
  ranking: FEATURE-011 > FEATURE-012 > FEATURE-013).

**Problem.** Today's club-league model (`core/poisson_model.py`) computes team attack/
defense ratings purely from that team's own recent match results (goals scored/
conceded). Two structural weaknesses follow from that: **(1) cold start** — at the start
of a season a team has little or no current-season history, and the roster itself may
have changed (transfers, or for a promoted side, no top-flight history at all), so
team-level ratings are least reliable exactly when they're most needed. **(2) lag** —
team-level scoring stats only reflect a real change (a key player's absence, a tactical
shift) after it's shown up across several matches worth of results; the model is
reactive, never proactive.

**Proposal.** Build a player-level lambda model — instead of, or alongside, the
team-level one — aggregating individual player attacking/defensive contributions
(weighted by minutes/position) into a team lambda for club matches, similar in spirit to
how the World Cup side already builds squad-level λ from player stats
(`compute_wc_team_strength.py`). Two direct benefits: works at season-start since it's
built from players' established performance rather than this-team's-this-season
results, and can react immediately to a player-availability announcement by removing
that player's contribution at the moment it's known, rather than waiting for the gap to
surface in team-level results.

**Related, not overlapping.** FEATURE-001 already does something adjacent for the World
Cup — a what-if diagnostic that excludes one player from an already-player-level squad
λ. This proposal is different in kind: making player-level data the *primary*
lambda-construction mechanism for club leagues, which have no player-level modeling at
all today (no club-league equivalent of `soccer_wc_players`/`soccer_wc_player_stats`).

**Open questions (unresolved, for scoping later).** Which player-level stats to use
(club minutes/goals/assists, via a similar pipeline to the WC's `club_meta()`/
TheStatsAPI pull?); how to weight position/role; whether to run fully in parallel with
team-level and compare, or blend the two.

**2026-08-05: two follow-up asks logged while explaining the as-built pipeline
(`MODEL_PIPELINE_OVERVIEW.md`) during the BUG-009 MD20-28 investigation. Not started;
come back to before calling model changes done for this feature.**

- **Follow-up A — roster-aware player pool (`MODEL_PIPELINE_OVERVIEW.md` section 1).**
  Today, "which players count for a team" is derived purely from season-to-date match
  PARTICIPATION (`soccer_player_stats` rows), with no concept of the current/expected
  roster. Nothing removes a departed player's stats once they've left (their pre-
  departure minutes stay baked into the team's player-level number for the rest of the
  season), and a new arrival contributes ~nothing until they've accumulated enough
  minutes to escape heavy positional shrinkage. Ask: estimate (or look up) the actual
  roster for the matchday being predicted, and only draw player-level contributions
  from players actually on it at that point in time -- closer to Requirements'
  Scenario 0/8 (starting-lineup baseline, new-player-no-history handling), neither of
  which is built yet.
- **Follow-up B — recency-windowed player-level data (`MODEL_PIPELINE_OVERVIEW.md`
  section 2).** Today the player-level number is a FLAT sum over the entire current
  season, no rolling window, no recency decay -- unlike the team-level number, which
  already uses a last-N-matches window with an (currently off) recency-decay knob
  (`RECENT_N=10`, `core.poisson_model.get_team_ratings`). Ask: give the player-level
  aggregation the same shape -- a window of the last M games (M = RECENT_N=10,
  matching team-level, not a separately-tuned constant) with more recent games weighted
  higher, instead of an unweighted season-to-date sum.

Both are directly motivated by (not yet confirmed as the root cause of) the MD20-28
ROI anomaly above -- a mid-season roster change is invisible to the player-level number
under today's implementation of either dimension.

**2026-08-08: Follow-up A payoff estimate -- worth a cheap fix, not worth a heavy one.
Not started; come back to when a low-cost roster-signal source (transfers/injuries/
suspensions) is convenient to wire in.**

Before investing in a real roster-aware system, ran a coverage check plus a
ceiling-payoff estimate to see whether the effort is justified at all. A hard
dependency on confirmed starting lineups was ruled out up front (user: don't want the
model gated on lineup availability), so the real-world version of this fix would have
to come from lower-latency signals available well before kickoff (transfer/signing
announcements, injury/suspension lists) -- not a same-day lineup feed.

*Coverage check* (`load_team_players` vs actual match participants, minutes-weighted,
10 matches / 20 team-checks): confirmed the roster-blindness is real, concentrated
around transfer-window debuts and long-benched players suddenly starting (e.g. new
signings contributing 0% to their team's rating in their debut match).

*Ceiling estimate* (`oracle_roster_blend_test.py`, `poisson_v4_oracleroster50`,
`blend_frac=0.5` -- **hindsight test**, blends each player's rate 50% of the way from
today's trailing-window rate toward a rate computed from that exact match's own actual
outcome; NOT a realistic implementation, and the blend is inconsistently applied
between player categories -- previously-invisible players get their FULL oracle rate
at half weight rather than a half-blended rate, so this run overstates a true "halfway"
result, skewing toward the high end of "partial improvement." Treated as a generous
ceiling, not a precise 50% figure):

| metric | baseline (`poisson_v4`) | oracle-50 ceiling | Δ |
|---|---|---|---|
| Brier, 2024 | 0.5948 | 0.5825 | -0.0123 |
| Brier, 2025 | 0.6169 | 0.6080 | -0.0089 |
| Brier, pooled | 0.6058 | 0.5953 | **-0.0105 (~1.7% relative)** |
| ROI vs Bet365, 2024 @EV+0% | -9.2% | -6.8% | +2.4pp |
| ROI vs Bet365, 2024 @EV+5% | -12.6% | -4.8% | +7.8pp |
| ROI vs Bet365, 2024 @EV+10% | -19.9% | -4.8% | +15.1pp |
| ROI vs Bet365, 2025 @EV+0% | -9.8% | -9.4% | +0.4pp |
| ROI vs Bet365, 2025 @EV+5% | -6.6% | -7.1% | -0.5pp |
| ROI vs Bet365, 2025 @EV+10% | -8.5% | -3.3% | +5.2pp |

Bias vs Betfair Exchange closing (home/away split) moved inconsistently -- 2024 away
bias improved (+0.0099 -> -0.0020) but 2024 home bias flipped sign (-0.0048 ->
+0.0076) and 2025 draw bias worsened (-0.0046 -> -0.0083). No clean convergence toward
zero the way Brier/ROI moved.

**Conclusion.** Brier improves modestly and consistently across both seasons (~1.7%
relative). ROI improves substantially in 2024 (especially at higher EV thresholds) but
barely moves in 2025, and even at this generous ceiling **ROI never crosses into
profitable territory in either season**. Roster awareness looks like a real, moderate
contributor -- not a fix that alone would flip the model profitable, and not one that
justifies a heavy build (a real lineup-prediction system) on this evidence alone. A
cheap version scoped to the cases the coverage check actually found (transfer-window
debuts, long-benched-then-starting players) via existing external signals -- not
same-day lineups -- is worth doing when convenient; a heavier system is not justified
by this ceiling. Worth revisiting once a low-cost data source for those signals is
identified.

**2026-08-11: open design question -- how much (if at all) should roster changes
impact player trust / blend?** Logged while investigating BUG-010's root cause,
explicitly deferred by the user ("a conversation for another time"), not scoped or
started. Current `player_trust_score()` requires BOTH good data coverage on the
current squad AND meaningful roster change since last season before leaning on
player-level data at all (a stable, well-tracked squad gets zero credit for good
player data, on the theory that team-level would already agree with it). User's
pushback: if the player-level data is good and the algorithm computing team-level
IMPACT from it is trusted, why not generically bias toward player-level over
team-level regardless of roster churn -- never necessarily 100% player (team
performance is still what ultimately matters), but the roster-change gate feels
like it's withholding a signal that should help even when the roster hasn't
turned over. Not resolved either way; needs its own scoping conversation, separate
from BUG-010's fix.

---

## FEATURE-012 — Pick/probability lineage and traceability

- **Type:** enhancement / tooling · **Status:** PROPOSED, not started. Logged 2026-07-27.
  **Priority: second** of three ideas logged together this session.

**Problem.** As the model accumulates changes over time (parameter tuning, fixes like
BUG-008/BUG-009, new features), there's no way to trace a specific historical pick or
stored probability back to exactly which model version/constants/inputs produced it.
Makes post-event analysis and debugging harder as the model matures — e.g. "was this
pick's EV computed before or after the BUG-008 fix?" currently requires manually
cross-referencing dates against this file rather than being answerable from the data.

**Proposal.** Build a lineage view tracing pick → model probability → the specific
factors/formula/constants/inputs that produced it. Some groundwork already exists
piecemeal (`soccer_model_predictions.method` tags the model version;
`soccer_wc_picks.selection_mode` tags which selection rule fired) but there's no unified
view tying a pick back through every contributing factor (team ratings used, league
averages used, shrinkage/decay constants in effect, odds snapshot used) in one place.

**Related.** Would make bugs like BUG-009 easier to retroactively diagnose (e.g. "show
every 2025-26 pick where the away-side bias plausibly changed the selected side") and
would turn the poisson_v1/poisson_v2-style before/after comparison pattern (built ad hoc
for BUG-008) into a reusable, general mechanism instead of a one-off script each time.

**Open questions.** What granularity to store (full input snapshot per pick vs.
reconstructible-on-demand from method + timestamp + constants-at-that-time); whether
tunable constants (`SHRINKAGE_K`, `RECENCY_DECAY`, etc.) need their own versioned/
timestamped table rather than living as module constants whose history only this file
documents.

---

**2026-08-07: over/under (totals) market backtesting added -- `backtest_from_
predictions.py` could not grade totals bets at all until now.** The model already
computed `p_over`/`p_under` (`analyse_match_wc`'s Poisson grid gives this for free)
and `soccer_model_predictions` already stored them, but nothing checked whether
those picks actually won -- so there was no way to validate the totals market the
way 1X2 has been validated all along. Added `run_totals()` (same EV-threshold/ROI
shape as the existing `run()`, but a SEPARATE report -- never pooled into the 1X2
staked/profit numbers, since every existing ROI reference point in this file is
1X2-only) plus a `--market {1x2,totals,both}` flag. `soccer_market_odds` (the
sharp-book reference used for every bias check in this file) has no totals fields
at all, and Bet365's `over_under` line is 100% 2.5 across both Serie A seasons
(1490/1490 rows) -- no sharp-book totals bias check is possible with current data,
and no multi-line complexity exists to handle either.

First real numbers (vs Bet365, EV>0%, `poisson_v4_stretch130`):

| Season | Totals ROI | 1X2 ROI (for contrast) |
|---|---|---|
| 2025 | **+8.2%** (n=273, 56.0% win rate) | -9.8% |
| 2024 | +0.5% (n=318, 53.1% win rate) | -9.2% |

Genuinely positive in 2025, roughly breakeven in 2024 -- both a clear improvement
over 1X2's negative ROI in the same seasons, and directionally what the user
predicted when proposing this work ("won't change the negative roi... but may add
positive roi to offset that... even if a little"). 2 seasons is still a real
sample-size caveat (same discipline as every other finding in this file) but this
is a promising, mostly-unexplored market -- worth continuing to track as more
seasons/leagues are added, per the session's broader shift toward more data
(more markets, more leagues) over further squeezing the current 2-season 1X2
dataset.

`generate_club_league_card.py` already surfaces OVER/UNDER 2.5 candidates through
the same floor guardrail as 1X2 (shipped in the same session, before this
backtesting gap was noticed) -- this closes the loop so those live picks now have
a real validation history behind them, not just an untested code path.

---

## FEATURE-013 — Incorporate additional external factors (fatigue/rest days, coaching changes, weather) — analysis first

- **Type:** enhancement · **Status:** PROPOSED, not started. Logged 2026-07-27.
  **Priority: third (lowest)** of three ideas logged together this session, per user.

**Problem.** Home vs. away is currently the only external/contextual factor the model
incorporates (via separate home/away team ratings). Other, less-frequent but
potentially impactful factors are entirely absent: fixture congestion/rest days between
matches (e.g. 3 days' rest vs. 6), recent coaching changes (a "new manager bounce" is a
commonly cited effect), weather, etc.

**Proposal (deliberately staged).** Before building any data collection or model
change, run an analysis on Serie A's last few seasons to check whether these factors
actually correlate with results in a way the model would benefit from — don't invest in
collecting/integrating a factor without first confirming a real trend exists. Only build
the data-collection + model-integration work for factors that show a genuine, non-noise
effect.

**Note.** Same "verify before building" discipline already used elsewhere in this
project — see `HYPOTHESES.md`'s away-heavy-underdog investigation, which was tested and
found **false** rather than assumed true.

---

## BUG-009 — Model compresses extreme mismatches toward a coin flip (favorite underrated, underdog overrated); pooled home/away bias fixed, compression partially addressed; re-diagnosed 2026-08-20 (residual is regression dilution, not miscalibration) + market-probability floor **SHIPPED 2026-08-20**

- **Severity:** high (touches every match; drives negative backtest ROI specifically on
  the model's most confident bets, not a cosmetic footnote) · **Status:** PARTIALLY
  FIXED, real remaining value, worth continued investment. Two independent causes
  confirmed and both safely addressed (real shipped fixes below); a
  further-improving candidate exists but is intentionally not promoted (see
  2026-08-18/19).

- **Quantified dollar impact of the residual (2026-08-19, current model,
  `poisson_v4_4`, 5 leagues x 2024/2025, EV>0% picks vs. Bet365):** the segment where
  the market prices the picked side below 25% loses -10.9% ROI (1,779 bets, -194
  profit units) vs. -4.4% ROI (2,036 bets, -88.6 profit units) for everything else.
  If that segment performed as well as the rest, total portfolio loss over this
  sample would shrink from -282.6 units to about -166.6 — **a ~41% reduction in
  total losses, real money, not a rounding error.** The portfolio would still be net
  negative even at that ceiling (the non-compressed segment is *also* currently
  unprofitable, a separate/broader issue) — this is a meaningful partial fix, not a
  path to overall profitability by itself. A closing-line-value check on the same
  split shows the compressed segment drifting slightly AWAY from the model's picks
  by closing (-0.0019) while the rest drifts slightly TOWARD them (+0.0022) —
  small in magnitude but directionally consistent with a real, fixable defect
  specific to this segment, not just generic noise. **Re-run and update this number
  whenever a candidate fix is tested or shipped**, so the headline stays current
  rather than going stale like the rest of this entry did before the 2026-08-19
  cleanup.

  The constant-tuning family of fixes (shrinkage `k_minutes`, spread stretch, the
  team-credibility de-shrink) is exhausted — two dedicated searches this session
  found no further improvement inside it (see below). That's a reason to change
  *approach* on the next attempt, not a reason to deprioritize the problem — the
  value case above stands regardless of which specific lever gets there.

**Finding.** Model probabilities vs. sharp closing lines (Pinnacle, Betfair Exchange)
showed two related but distinct problems:

1. **Pooled home/away signed bias** (model underrated home, overrated away, averaged
   across all matches) — **fixed 2026-07-27**: `get_league_averages()` gained a
   recency window (`LEAGUE_AVG_WINDOW=100`, ~10 matchdays) instead of an unweighted
   all-history blend. Confirmed across seasons/books; residual bias shrank but didn't
   fully close on its own (~-0.02 to -0.03 home / +0.03-0.04 away remained).
2. **Spread compression on extreme mismatches** (found 2026-08-04, invisible to the
   pooled metric above): bucketing model p(home) by market implied probability shows
   the model pulls big favorites down and big underdogs up, growing with mismatch
   size. Two additive causes, both from FEATURE-011's 2026-08-02 work: (a) switching
   team-level ratings from goals to xG (xG has genuinely less natural spread than
   goals — a property of the data, not a bug), (b) the player-level blend layer on
   top of that.

**Fixes shipped from this investigation:**
- `TEAM_RATING_XG_V_GOALS_BLEND` param added (2026-08-05), tunable goals/xG mix —
  default unchanged (pure xG) since ROI didn't track the calibration gain cleanly.
- `CLUB_LEAGUE_MIN_PICK_PROBABILITY=0.25` floor guardrail ported from the World Cup
  pipeline into `core.pick_guardrails`, wired into live picks (2026-08-07) — real
  ROI improvement in most slices, though its marginal value shrank once the xG-stretch
  fix below also shipped (overlapping symptoms).
- Player-level attack recentering switched additive→multiplicative (2026-08-09) — a
  real correctness fix (additive could drive a rating negative); improved compression
  in every bucket/season, Brier ~0.6% better pooled. Shipped on correctness/calibration
  grounds despite season-inconsistent ROI (see recurring lesson below).
- `TEAM_RATING_XG_SPREAD_STRETCH_ATTACK/_DEFENSE=1.3` shipped (2026-08-07) — flat
  multiplicative re-widening of team-level xG ratings around the league mean, partially
  restoring spread lost to pure-xG's lower variance. Real compression/Brier
  improvement; ROI improved in *both* seasons at this value (unusual for this
  investigation). 1.66 (the literal goals/xG stdev ratio) was tested and overcorrects
  (breaches the bias target) — 1.3 is the calibrated safe ceiling.

**Recurring lesson: ROI does not reliably track calibration improvements** at the
1-2 season sample sizes available here — several well-evidenced calibration fixes
(xG/goals blend sweep, additive→multiplicative, post-hoc recalibration) moved ROI in
*opposite* directions across the two backtest seasons, or not at all. Brier/bias, not
ROI, has been the primary signal for shipping decisions since.

**Dead end, tried and dropped (2026-08-07):** a post-hoc output-recalibration curve
(`recalibrate_output.py`) tightened calibration in-sample but made ROI worse in both
seasons and didn't generalize out-of-sample (fit on one season, applied to the other,
flipped the bias sign). Not pursued further.

**2026-08-18/19 follow-up session — three more angles, all real, none a clean fix, and
ROI itself found to be a weaker validation signal than treated so far:**

- **Isolated the player-blend layer's specific mechanism.** `apply_shrinkage()`'s
  `k_minutes` (currently 900 — the minutes-based half-trust point pulling every
  player toward their position's league average) is a real, isolated contributor to
  the compression bug; lowering it toward ~100-150 nearly zeroed the favorite/underdog
  compression in a small-sample probe. But a full grid sweep of `k_minutes` ×
  `PLAYER_RATING_SPREAD_STRETCH_ATTACK` (12 combinations, full 5-league backfill)
  found a clean, monotonic Pareto trade-off: every combination that meaningfully
  improves compression also degrades pooled Brier and reopens the away-bias target.
  The current shipped values (k=900, stretch=2.0) are Pareto-best on Brier — no free
  lunch inside this constant family.
- **Built and shipped, opt-in only: team-specific credibility de-shrink.**
  `apply_shrinkage()` now retains each player's shrink weight (previously discarded);
  `team_credibility()` aggregates it per team the same way `raw_team_strength()`
  weights the value itself; `compute()` gained `player_use_team_credibility_deshrink`
  (default `False`) to replace the flat stretch with a team-specific linear correction
  sized to that team's own actual shrinkage, instead of one constant for every team.
  Nearly matches the constant grid's best compression result (-0.053→-0.012 favorite
  delta) at ~60% of the Brier cost — a real improvement to the trade-off, not an
  escape from it. Full backfill: pooled ROI modestly better, but Brier worse in
  **every single league**, and per-league ROI is genuinely mixed (Bundesliga roughly
  halves its loss; La Liga gets worse across the board). **Kept off by default** —
  real, tested (10 new unit tests, full suite green), documented, not promoted.
- **Found ROI itself is close to uninformative for validating any of this.** The
  model's positive-EV picks are worse-calibrated (higher Brier) than its whole-season
  average, and get *more* overconfident as the EV threshold rises — true for baseline
  AND team-deshrink, worse for team-deshrink. A closing-line-value check (does the
  sharp market move toward the model's picks between opening and closing?) showed
  **zero average movement** (essentially a 50/50 coin flip toward vs. away) at every
  EV threshold — the model's "positive EV" picks carry no detectable real information
  edge; EV-threshold selection on a noisy model mechanically selects for overconfident
  noise, not skill. **ROI comparisons throughout this file's history should be read
  with real skepticism** — Brier/bias (unbiased, no selection filter) remain the
  trustworthy signal.
- **Checked the team-level side for the same class of fix — ruled out, not just
  deprioritized.** `TEAM_RATING_PULL_TOWARD_AVERAGE_MATCHES=0` (team-level shrinkage
  is already fully disabled). There's no team-level shrinkage to de-shrink, so the
  player-level mechanism doesn't transfer. Checked whether the flat
  `TEAM_RATING_XG_SPREAD_STRETCH` constant should instead vary by team or league:
  per-team xG/goals variance ratio is dominated by sampling noise (individual team
  ratios swing from -3.5 to +3.7 at realistic sample sizes); per-league ratios are
  more stable within a season but swing 0.11-0.34 across seasons — larger than the
  spread between leagues in any one season. **Neither is a stable enough signal to
  calibrate against** — the current flat constant is a reasonably defensible choice.
  Closed, no further lever identified.

**Where this leaves things:** two independently-diagnosed causes (team-level xG
spread, player-blend shrinkage), both understood, both partially and safely addressed
(xG-stretch, additive→multiplicative, guardrail all shipped; team-credibility
de-shrink built and available but off by default). No further lever found at either
the player or team level that improves compression without a real calibration cost —
but the ~41% total-loss-reduction ceiling quantified above means this stays worth
continued investment, not something to shelve. The specific thing that's exhausted is
the constant-tuning family (shrinkage/stretch/credibility-weighting); the next
attempt needs a structurally different idea, not another sweep inside that family.

**2026-08-20 re-diagnosis — the residual "compression" is regression dilution
(an information deficit), not miscalibration; there was never anything left for an
output transform to fix. Plus a validated betting-layer fix.**

- **The model is already calibrated at the extremes.** Logistic regression of the
  home-win outcome on logit(model p_home), `poisson_v4_4`, all matches with a
  Pinnacle closing line (n=3,030): **calibration slope = 0.989** (1.0 = perfect;
  <1 = overconfident, >1 = underconfident), intercept -0.037. When the model says
  30%, it happens ~30% of the time — including in the tail buckets. There is no
  residual compression *relative to reality* to stretch away.
- **The compression only exists relative to the market, and its size is exactly
  what noise geometry predicts.** On the logit scale: model sd 0.831, Pinnacle
  closing sd 0.860, correlation 0.830. Regressing model on market therefore gives
  slope corr x sd_model/sd_market ~= 0.80 — i.e. in market-p buckets the model
  looks ~20% "compressed" even though it is perfectly calibrated. That is textbook
  errors-in-variables/regression dilution: the market has information the model
  lacks (lineups, injuries, rest, transfers), so conditioning on an extreme market
  price selects matches where the model's noisier estimate is less extreme.
- **This is why the constant-tuning family had to fail — confirmed on the stored
  sweep methods** (same regression, same n=3,030 sample): `bug009_shrinkage_k150`
  and `bug009_team_deshrink` raise logit sd to ~1.06-1.08 ("compression fixed" in
  market buckets) but collapse the calibration slope to ~0.78 (overconfident) and
  worsen Brier, while corr with the market barely moves (0.83 -> 0.83-0.85). A
  monotone output transform can add spread but cannot add information; the Pareto
  wall found on 2026-08-18/19 is a mathematical necessity, not bad luck. **The
  right metrics for future attempts on this bug are calibration slope (keep ~1.0)
  and logit-correlation with the sharp closing line (raise it) — not "compression
  in market-p buckets," which any harmful stretch can fake.**
- **Checked and ruled out: Poisson-grid truncation.** `outcome_probs()` sums the
  MAX_GOALS=6 grid without renormalizing, so p_H+p_D+p_A < 1; but the deficit is
  ~0.01% typical and ~1% worst-case in the most lopsided matches. A one-line
  renormalization is harmless and correct, but it is not this bug.
- **The dollar leak has a separate, fixable cause: the pick guardrail floors on
  MODEL probability only** (`guardrail_reasons`), so the losing segment — market
  prices the picked side under 25% but the model says more — passes it by
  construction. Those bets are exactly where (a) EV>0 selection on a corr-0.83
  model harvests estimation error (winner's-curse: a huge model-vs-market gap is
  far more likely noise than edge), and (b) the market yardstick itself is biased:
  `import_league_market_odds.py` de-vigs proportionally, which leaves the
  favorite-longshot bias in `p_*_fair` (measured market calibration slope 1.154 —
  longshots win even less often than fair-p implies). Long-odds "edges" are
  double-counterfeit.
- **Validated fix, betting layer (simulation on stored `poisson_v4_4` predictions
  vs Bet365 closing, best-EV-per-match, EV>0, 3,913 matches):** add a market-side
  floor to the guardrail — skip any pick whose market fair probability is below
  ~0.30. Baseline: 3,585 bets, -11.4% ROI, -409.6 units. Floor 0.25: -122.5 units.
  **Floor 0.30: 1,640 bets, -4.5% ROI, -73.4 units (82% of total loss removed).**
  Floor 0.35: -29.0 units. Improves every season individually (2023: -19.0% ->
  -2.8% at 0.30, +5.4% at 0.35; 2024: -7.0% -> -2.8%; 2025: -14.2% -> -6.3%) and
  4 of 5 leagues at 0.30 (Ligue 1 slightly worse, small sample). This beats the
  ~41% headline ceiling because dropping the segment entirely is better than
  making it perform "as well as the rest." Blending model p toward the book's own
  fair p on the logit scale before EV (w_model 0.3-0.7) was also tested and barely
  helps on its own — the prior it shrinks toward is itself longshot-biased; fix
  the de-vig (power/logit method instead of proportional) before revisiting that.
- **The only genuine fix for the dilution itself is more information per rating
  (raise corr), not more spread:** the team xG window is a hard last-10 home-only
  (resp. away-only) sample with flat weights (`TEAM_PAST_MATCH_WINDOW_SIZE=10`,
  `TEAM_RATING_XG_WINDOW_DECAY=1.0` — the decay knob exists, unused); widening to
  ~20-30 matches with a decay half-life around 8-10, and/or pooling home+away
  with an explicit home-advantage factor, roughly doubles the effective sample
  per rating. Validate against calibration slope + corr-with-closing-line, per
  above. Lineup/availability data (FEATURE-001's club-league analogue) is the
  single largest information gap vs. the closing line, but is a bigger project.

**Market floor SHIPPED 2026-08-20** — `CLUB_LEAGUE_MIN_MARKET_PROBABILITY = 0.32`
(vig-inclusive single-side implied, `1/decimal`; equals the fair-p 0.30 the
diagnosis validated once Bet365's ~5% 1X2 overround is added back), as a new
`market_floor` check in `core.pick_guardrails.guardrail_reasons`/`guardrail_excess`,
wired into `generate_club_league_card.py` and `backtest_from_predictions.py`'s
`--guardrail` flag (which now mirrors both live floors). WC pipeline untouched
(never swept there). Calibration sweep on the implied scale (0.25-0.40, stored
`poisson_v4_4` vs Bet365 closing, best-EV-per-match, no model floor): every season
improves monotonically up to ~0.30-0.32, flattening into thin-sample noise beyond —
0.32 per the "largest value inside the target" discipline, anchored to the
independently-diagnosed fair-0.30 mechanism rather than the noisy ROI tail.
**Marginal validation on the live card's actual policy** (top-2-EV-per-match, model
floor 0.25 already applied — the shipped baseline): total 3-season loss
-205.8 -> -78.9 units (-6.3% -> -4.6% ROI), with 2023 -10.7% -> -4.1%,
2025 -9.9% -> -6.5%, 2024 flat in absolute units (-20.6 -> -20.7, ROI -1.4% ->
-2.8% on half the bets). Honest caveat: under `backtest_from_predictions.py`'s
stake-EVERY-EV-positive-side policy the marginal effect is smaller (total loss
-203 -> -115 units, pooled per-bet ROI roughly flat, and 2024's removed segment
was mildly positive) — the floor's value concentrates exactly where the card
concentrates, on the highest-EV (largest model-vs-market gap) candidates, which
is what the winner's-curse mechanism predicts. Unit tests added (guardrail
market-floor cases + card wiring test); full suite green (478 passed).

**2026-08-20 (later): out-of-sample confirmation on the newly imported
2022/2023 history.** The 2026-08-20 data import (all 5 leagues back to season
2022) provided league-seasons neither the re-diagnosis nor the market floor had
ever seen. On 2023x4 (the four non-Serie-A leagues' 2023 seasons, which have
2022 history behind them): calibration slope **1.065**, corr-with-Pinnacle
0.817, home-Brier 0.216 — matching the calibration-era numbers (0.989/0.830/
0.214), so the shipped constants are NOT overfit to 2024/2025 and no retuning
is indicated. The market floor also held out-of-sample: on the new
league-seasons pooled (best-EV-per-match vs Bet365 closing, model floor
applied), floor 0.32 cut the total loss -160.4 -> -62.5 units (2023x4 strongly
better, -126.0 -> -38.9; 2022 slightly worse per-bet but smaller in absolute
units, -34.4 -> -23.5 — and 2022 is the cold-start burn-in season, see the
WATCH entry). 2022's own degraded calibration (slope 0.66) is entirely the
cold-start artifact, not evidence against the constants — see the 2026-08-20
WATCH entry for the decomposition and probes.

---

## DESIGN-002 — League-average baseline blends all prior seasons equally, with no season-scoping or recency weighting

- **Status:** OPEN, not yet decided. Logged 2026-07-27, surfaced while deciding whether
  `param_sweep.py` should route through `analyse_match()` (BUG-008 follow-up: promoting
  `shrinkage_k`/`decay` to overrides on the single entry point).

**The scenario, not just the code.** Imagine Serie A's scoring environment genuinely shifts
over a few years — say a stricter (or looser) offside/VAR interpretation phases in, or the
league's competitive balance changes because the promoted-from-Serie-B teams in one season are
weaker defensively than in another. League-wide average goals/game drifts as a result — this
kind of drift is a real, observed pattern in most soccer leagues over multi-year windows, not a
hypothetical. Right now `get_league_averages()` (via `analyse_match()`) computes its baseline
from **every** prior match in the league, with no season boundary and no weighting — a goal
scored in 2022-23 counts exactly as much as one scored last week. If the current season's
scoring rate has genuinely moved away from the 2022-24 average, the baseline lags that shift.

**Why it concentrates in exactly the matches where it matters most.** Team-level ratings use
shrinkage (`_shrink`) that falls back hard to the league-average baseline when a team has few
same-season games — which is precisely true for **every team, every season, in the first few
matchdays**. So a stale multi-season baseline has its largest effect exactly when the model is
already leaning most heavily on it: the opening weeks of each season, before current-season
form has accumulated enough to dominate the shrinkage-weighted rating. A season-wide scoring
drift would show up there as a systematic, one-directional bias in early-season Over/Under and
moneyline probabilities (e.g. persistently over- or under-predicting total goals) that
gradually self-corrects as the season's own match count grows — a pattern that would be easy to
miss match-by-match but should be visible in aggregate calibration checks restricted to
early-season windows.

**Two candidate fixes, each with a real cost — not yet chosen.**
1. **Hard season-scoping** (baseline = current season only, or current + last N seasons).
   Removes the stale-environment risk directly, but makes the *cold-start* problem worse for
   **every** season's opening weeks, not just the first one — the same issue already true for
   2022 (the earliest season on record, with zero prior history) would recur every August: with
   less same-season data to fall back on, shrinkage would default toward the hardcoded 1.3/1.1
   constants more often, which is arguably a worse approximation than a slightly-stale
   multi-season blend.
2. **Recency-weighted blend across season boundaries** (user's suggestion) — decay older
   seasons' contribution to the baseline the same way `RECENCY_DECAY` already decays older
   matches *within* a team's rating history, rather than a hard cutoff. Avoids the cold-start
   cliff of option 1, but adds a new tunable constant and more surface area, and doesn't fully
   solve cold-start either (a brand-new season still has zero same-season weight at kickoff).

**Not fixed.** Deliberately left as the status quo (uniform blend, no scoping) for now — see
BUG-008's fix for why: this affects `analyse_match()` directly, the shared entry point every
caller (live picks, backfills, `param_sweep.py`) now goes through, so whichever way this is
decided should apply uniformly rather than as a caller-specific override.

**2026-08-07: MD20-28's bottom6 ROI collapse traced back to THIS bug's compression finding,
localized to team-level xG/xGA specifically -- and a candidate fix tested (not shipped) with
better results than any prior compression fix.** Picked back up while digging into
FEATURE-011 Follow-up B's post-implementation validation (below) -- the season-blind window
didn't move MD20-28's bottom6 collapse at all (expected: roster staleness was already ruled
out as the driver, 2026-08-05 above), so dug into WHY the collapse concentrates there.

- **Not a pricing-window effect.** Checked whether sharp books or the model re-price
  bottom6-vs-other matchups differently in MD20-28 specifically (four candidate mechanisms:
  sharp lowering/raising odds more in-window, model doing the same) -- all four ruled out.
  The model-vs-sharp gap on the bottom6 side is ~flat all season (+7.8pp MD1-9, +6.9pp
  MD10-19, +7.4pp MD20-28, +5.2pp MD29-38, both seasons pooled, n=328 bottom6-vs-other
  matches). MD20-28 isn't even the largest gap.
- **What IS different: fixture composition + variance.** MD20-28 simply contains more
  extreme lopsided bottom6-vs-strong-team fixtures than other windows -- 21.8% of its
  bottom6 matchups are sharp-book sub-10% longshots, vs 14.5-15.5% in MD1-9/MD10-19. A
  roughly constant ABSOLUTE probability-overrating bias does much more damage applied to a
  5%-vs-12% mismatch than a 25%-vs-32% one (payout convexity near the tail), so the same
  bias produces outsized EV numbers specifically where these fixtures cluster.
- **Isolated to team-level, not player-level, and not a generic "model can't use xG"
  issue.** Sweeping `team_xg_v_goals_blend` (team-level xG-vs-goals) 1.0->0.0 shrinks the
  bottom6-vs-sharp gap by 44% (6.9pp->3.9pp), monotonically. Sweeping
  `attack_xg_v_goals_source`/`defense_xg_v_goals_source` (player-level) the same way only
  shrinks it 7% (6.9pp->6.4pp). Both fully de-xG'd together: 3.4pp remains -- so xG-reliance
  (mostly team-level) explains roughly half the gap, not all of it.
- **Confirmed this IS the already-known compression property (BUG-009 diagnosis #2 above),
  not a new bug -- magnitude matches almost exactly.** Directly measured, on this specific
  bottom6-vs-other match set (n=328, point-in-time correct): the opponent-minus-bottom6
  ATTACK RATING gap is 45.3% smaller under xG than under goals (+0.483 goals/match vs
  +0.264 xG/match) -- essentially the same size as the 44% probability-gap reduction found
  above. No separate mechanism needed to explain the size of the effect.
- **Code verified clean -- team-level xG/xGA really is just "sum of player xG," and the sum
  is implemented correctly.** `get_team_xg_ratings`' SQL groups by `(match_id, venue)` (all
  of one team's players in one match), and `backfill_club_xga.py`'s
  `compute_match_team_xg` does the same for the opponent side, with the
  home/away-opponent lookup verified correct (not swapped). Added test coverage that didn't
  exist before: `tests/test_backfill_club_xga.py` (new file, 4 tests -- multi-player sum,
  NULL-teammate handling, home/away tracked separately, end-to-end own-vs-opponent
  assignment) and 2 new multi-player-sum tests in `tests/test_compute_club_player_strength.py`
  (`test_get_team_xg_ratings_sums_all_of_a_teams_players_not_just_one`,
  `test_get_team_xg_ratings_sum_ignores_teammates_with_no_xg_data`) -- all existing
  `get_team_xg_ratings` tests only ever used one player per team per match, never actually
  exercising the SUM. **Not yet committed.**
- **Candidate fix tested (ad hoc script, not committed, not shipped): stretch team-level xG
  ratings' spread back toward goals-level dispersion, instead of blending xG away.**
  `stretched = league_xg_mean + (raw_xg - league_xg_mean) * factor`, applied to each of
  `get_team_xg_ratings`' four fields before the existing shrink/blend pipeline (recenters on
  the league's OWN xG mean per `before_date`, not the goals-based `avg_home`/`avg_away`).
  Swept factor 1.0 (no-op) / 1.3 / 1.66 (the empirically-measured goals-stdev/xG-stdev
  ratio) / 2.0, full pipeline, both seasons separately:

  | Factor | bottom6 gap | 2024 ROI @0/5/10% | 2025 ROI @0/5/10% | 2024/2025 away bias |
  |---|---|---|---|---|
  | 1.0 (today) | +6.9pp | -13.4%/-14.8%/-23.9% | -8.1%/-6.1%/-4.4% | +0.0056/+0.0140 |
  | 1.3 | +5.0pp | -9.2%/-12.6%/-19.9% | -9.8%/-6.6%/-8.5% | +0.0099/+0.0177 |
  | 1.66 | +2.9pp | -8.0%/-8.4%/-13.1% | -9.3%/-5.9%/-4.9% | +0.0153/**+0.0229** |
  | 2.0 | +1.2pp | -7.9%/-5.8%/-4.7% | -5.5%/-4.3%/-3.5% | **+0.0203**/**+0.0280** |

  Bottom6 gap shrinks monotonically as designed. **ROI improves in BOTH seasons as the
  factor increases -- notably different from every other compression fix tried in this
  file (blend, additive->multiplicative below) which showed one season winning while the
  other lost.** But away-side pooled bias grows with the stretch and **breaches the
  +/-0.01-0.02 Model Calibration target at factor=1.66 in 2025 and at factor=2.0 in both
  seasons** -- the literal "restore full goals-level spread" value (1.66) already
  overcorrects, plausibly because goals-based ratings are noisier/wider partly from
  small-sample variance, not purely truer signal (the original reason xG replaced goals as
  the team-level default). factor~1.3 looks like the practical safe ceiling (inside target
  both seasons, real if partial ROI gain); the true bias-breach boundary sits somewhere
  between 1.3 and 1.66, not yet pinned down.

**Next steps (not started):** (a) finer sweep between factor=1.3 and 1.66 to find the actual
bias-breach boundary rather than the coarse grid above; (b) check whether combining a modest
stretch with a partial `team_xg_v_goals_blend` reduction (rather than either lever alone)
reaches a better bias/ROI tradeoff than either does by itself; (c) commit the two new test
files/additions above (currently sitting uncommitted) once this thread is done, independent
of whether the stretch fix itself ships; (d) if a safe factor is chosen, promote it from ad
hoc script into a real, named, documented constant (`MODEL_TUNING_PARAMETERS.md` +
`compute_club_player_strength.py`) with a temporary rollout toggle, same pattern as every
other lever in this file -- not shipped as a silent default without that.

**2026-08-07: combined stretch+blend tested -- negative result, dropped.** Ran
factor=1.3 stretch together with `team_xg_v_goals_blend=0.5` (a 50/50 xG/goals blend),
both seasons, via `backfill_with_xg_stretch.py`. Worse than stretch=1.3 alone on both
bias and ROI in 2024, and roughly similar-to-worse in 2025 -- directly contradicts next
step (b) above's premise that combining might reach a better tradeoff than either lever
alone. Decision: drop the blend lever entirely, keep stretch=1.3 as the sole core-tuning
candidate going forward.

**2026-08-07: step 3 of the ROI-improvement plan (output recalibration) tested --
negative result, not pursued further.** Built `recalibrate_output.py`: a post-hoc
correction curve from bucketed (model's own p -> mean market-minus-model gap),
linearly interpolated between 6 bucket midpoints, applied per side on top of the
stretch=1.3 baseline. Fit/apply split three ways -- fit 2024/apply 2025, fit 2025/apply
2024, and fit-both/apply-both (labeled optimistic, closest to circular):

| Variant | Brier (pooled) | Bias home/away | ROI @0/5/10% |
|---|---|---|---|
| stretch=1.3 alone, 2024 | 0.5948 | -0.0048/+0.0099 | -9.2%/-12.6%/-19.9% |
| stretch=1.3 alone, 2025 | 0.6169 | -0.0107/+0.0153 | -9.8%/-6.6%/-8.5% |
| recal, in-sample, 2024 | 0.5964 | +0.0047/-0.0033 | -14.8%/-17.6%/-24.5% (worse) |
| recal, in-sample, 2025 | 0.6158 | -0.0028/+0.0033 | -9.1%/-12.1%/-10.5% (worse) |
| recal, fit 2024->apply 2025 | 0.6156 | -0.0080/+0.0087 | -7.4%/-10.6%/-10.7% (mixed) |
| recal, fit 2025->apply 2024 | 0.6006 | **+0.0113** (sign flip) | **-20.3%/-23.0%/-23.7%** (much worse) |

Two findings: (1) even the best-case in-sample fit tightens calibration (bias shrinks
toward zero, Brier roughly flat) but makes ROI WORSE in both seasons at nearly every
threshold -- pulling probabilities toward the sharp book's price shrinks the model's
own apparent edge (EV is measured against a book that tracks the sharp price closely),
so the correction filters out exactly the disagreements that were driving bets, and
what's left over isn't good signal. (2) it doesn't generalize out-of-sample -- fit on
2025/applied to 2024 flips the home-side bias sign and craters ROI to worse than doing
nothing, because a 6-bucket curve fit on ~350-380 games is too noisy a residual
correction to trust on held-out data. Same overfitting-with-2-seasons risk the user
flagged (from a different angle) when considering a per-season stretch factor --
confirmed here empirically rather than just as a hypothesis. **Conclusion: this
specific mechanism (bucketed post-hoc correction toward market price) is a dead end,
not just underpowered -- not pursued further without a fundamentally different design
or much more data.**

**2026-08-07: step 4, guardrails (BUG-003 pattern: `MIN_PICK_PROBABILITY` floor +
`MAX_UNDERDOG_MARKET_DISAGREEMENT` cap) re-tested on top of stretch=1.3, rather than the
2026-08-05 `poisson_v4_priorblend` baseline they were originally validated on.** Same
methodology (floor/cap sweep, EV>0%, both seasons):

| Guardrail | 2024 ROI @0% | 2025 ROI @0% |
|---|---|---|
| none (stretch=1.3 alone) | -9.2% | -9.8% |
| floor=0.25 only | +1.6% | -9.6% |
| floor=0.25 + cap=1.75 (2026-08-05's validated value) | +1.4% | -8.8% |
| floor=0.25 + cap=2.00 | +0.4% | -7.7% |

Floor=0.25 alone is still a large win in 2024 (-9.2% -> +1.6%) but is now close to
**inert in 2025** (-9.8% -> -9.6%), a much weaker/less season-consistent effect than the
2026-08-05 result on the pre-stretch baseline (which roughly halved the loss in BOTH
seasons). The cap is now close to inert too -- sweeping it 1.25 through "none" barely
moves 2025 (-7.7% to -12.8%, non-monotonic) and doesn't reproduce the clean
both-seasons convergence found on `priorblend`. Reading: the guardrail and the
xG-stretch fix are correcting overlapping symptoms (both target overconfident-longshot
overpricing), so on top of stretch=1.3 there's measurably less left for the guardrail
to catch -- most of its 2026-08-05 value has already been captured by the core-tuning
fix. floor=0.35-0.50 alone reach both-seasons-positive ROI in this sweep, but sample
sizes there shrink to 73-167 bets/season -- too small to trust over 2 seasons, flagged
as a likely overfit rather than a real finding, not a candidate. **Not yet decided
whether to ship floor=0.25 given its now-asymmetric benefit, tune further, or treat
guardrails as lower-priority now that core tuning captured most of the value they were
meant to add.**

**2026-08-07: dug into WHY floor=0.25 fixes 2024 but not 2025 -- found a real, unexplained
2025-specific anomaly, not a season-specific longshot mechanism.** The excluded (p<0.25)
group itself explains most of the asymmetry: 2024's sub-0.25 picks won half as often as
the model expected (11.2% win rate vs ~20% mean model p, n=134, ~2.5 sigma shortfall --
a real deviation) while 2025's sub-0.25 picks (14.3% vs ~20%, n=98, ~1.4 sigma) were
within normal variance. So 2024's longshots were unusually bad; 2025's weren't unusual
at all -- floor=0.25 mostly "fixes" a bad 2024 longshot patch, not a general problem.

The bigger finding is in the group the floor does NOT touch (p>=0.25 draw/away picks,
mean p 0.30-0.45 -- not longshots): 2024 was solidly profitable there (draw +7.0%,
away +16.5%) while 2025 was broadly weak across the board (draw -17.3%, away +1.7%).
Checked two clean hypotheses against REALIZED results (not model/market prices) and
ruled both out: (1) top6-at-home win rate was nearly identical both seasons (63.2% 2024
vs 60.5% 2025), so it isn't "top6 teams got stronger"; (2) league-wide draw rate was
also close (28.4% vs 26.1%), not a big enough shift to explain draw picks losing
uniformly across every implied-probability bucket in 2025 (-21% to -24% in every
bucket, vs 2024's uneven pattern where most buckets were strongly positive).

Isolated one specific, real anomaly instead: **away picks made against a top6 home
opponent** (n=40, mean model p=34.4%, market implied 23.0% -- a real, moderate edge in
both years) won only 4 times in 2025 (10.0% win rate, -42.2% ROI) against a model
expectation of ~13.8 wins -- a ~3.2 sigma shortfall, not just noise. The 2024 equivalent
(n=32) landed almost exactly on the model's own expectation (10 wins vs 11.6 expected,
well within variance). The pattern isn't concentrated in top6-vs-top6 matchups
specifically (both that subset and top6-vs-weaker lose at a similar rate), isn't
confined to one matchday window (spans MD1-MD34), and touches every one of 2025's top6
clubs as the home side. **Root cause not identified** -- ruled out roster churn and a
general home-record shift; open possibilities are (a) 2025's top6 sides got genuinely
stronger in a way the model's rolling-window stats lag behind (a cold-start-style
mechanism, but for established teams improving mid-tenure rather than promoted/departed
players), or (b) a real but sample-limited (n=40) single-season anomaly that a 3rd
season of data would clarify. **Logged as an open finding, not pursued further for
now** -- flagged as a candidate thread once a 3rd season or additional leagues give more
data to check it against, per the "sample size is the real constraint right now"
read (2026-08-07).

**2026-08-07: shipped -- xG-stretch=1.3 and the floor guardrail are now real, wired
code, not ad hoc scripts.** Resolves next-steps (c) and (d) from the sweep above:

- `TEAM_RATING_XG_SPREAD_STRETCH = 1.3` is a real constant in
  `compute_club_player_strength.py` (`MODEL_TUNING_PARAMETERS.md`), wired through
  `team_level_lambda`/`compute()` via a `league_xg_means` snapshot (new
  `league_xg_field_means`, computed once per `compute()` call across all of
  `team_ids`, not once per team). 1.0 reproduces the exact pre-2026-08-07 shape for
  comparison; the blend lever (`TEAM_RATING_XG_V_GOALS_BLEND`) stays at its existing
  default (1.0) per the combined-test negative result above -- stretch only, blend
  untouched.
- The BUG-003 floor/cap guardrail check was extracted from `generate_wc_card.py`
  into a new shared, tested module, `core.pick_guardrails` (`guardrail_reasons`/
  `guardrail_excess`, taking plain `(prob, implied, floor, cap)` rather than a
  card-specific dict) -- a pure refactor of the WC tool (all 35 of its existing
  tests still pass unchanged) so a floor/cap fix or re-tune can no longer silently
  drift between the two systems, which is exactly how the club-league pipeline went
  without ANY guardrail for weeks in the first place.
- Discovered while wiring the live path: `generate_serie_a_card.py` (the actual
  live pick generator) was never on the FEATURE-011 player-blend pipeline at all --
  it called `core.poisson_model.analyse_match()` (the old, pure-goals, team-only
  model), so nothing from FEATURE-011/BUG-009/BUG-010 had ever reached a live pick,
  independent of anything found this week. Rather than patch that script, and given
  the project is about to add more leagues (making a hardcoded Serie A-specific
  tool the wrong shape anyway), it's replaced by `generate_club_league_card.py`
  (`--league`-parameterized, defaults to Serie A since that's the only league with
  data today) on the real `compute()`/`analyse_match_wc()` pipeline, applying
  `CLUB_LEAGUE_MIN_PICK_PROBABILITY = 0.25` via the new shared guardrail module. No
  cap shipped for club leagues -- the 2026-08-07 sweep above found it close to
  inert on top of the stretch fix, so shipping one would add a knob with no
  demonstrated value. `generate_wc_card.py` is untouched beyond the guardrail
  extraction (WC and club leagues stay separate tools -- different markets, ADVANCE
  and group/knockout structure vs. a season fixture list -- deliberately not
  unified).
- New/updated test coverage: `tests/test_pick_guardrails.py` (new, the shared
  module in isolation), `tests/test_generate_club_league_card.py` (new, including
  an end-to-end guardrail-exclusion case), plus `team_level_lambda`/`compute()`
  stretch-wiring tests in `tests/test_compute_club_player_strength.py`. Full suite
  green except the one pre-existing, deliberately-red `test_attack_recentering_
  should_scale_by_ratio_to_league_average_like_defense_does` (unrelated, predates
  this work).

---

## BUG-008 — `get_league_averages()` has no date cutoff: league-average baseline leaks future-season data into historical backfills — **FIXED 2026-07-26**

- **Severity:** low-medium (systematic but likely small; invisible during live single-season
  pick generation, only surfaces when the model is run retroactively against a DB that already
  holds later seasons) · **Status:** FIXED 2026-07-26.

**Symptom.** `analyse_match()` calls `get_league_averages(conn, league)` with no `seasons`
argument, so it defaults to averaging home/away goals over **every** `soccer_matches` row for
that league currently in the DB — no `match_date <` cutoff. `get_team_ratings()` correctly
restricts to matches strictly before the match being analyzed, but the league-average scaling
baseline does not. During normal WC live use this was invisible (no future-season data existed
yet in the DB). It becomes a real lookahead leak once the DB holds multiple completed seasons
and the model is run retroactively: predicting an August 2025 fixture pulls in goal-scoring
data from as late as May 2026.

**Confirmed while backfilling.** The just-shipped `soccer_model_predictions` backfill for
Serie A 2025-26 (380 rows, see `backfill_soccer_model_predictions.py`) used this same
unfiltered league average for every match, including the season-opening fixtures — so those
probabilities blend "what was knowable at the time" (team ratings, correctly cutoff) with "the
full-season average including future results" (league baseline, not cutoff). Likely a small
numeric effect season-to-season (league scoring rates are fairly stable), but it's a genuine,
quantifiable bias, not a hypothetical one.

**Scope if the backfill is extended to 2022/2023/2024.** Same defect applies to any season
backfilled while later seasons already sit in the DB, and compounds as more seasons accumulate
— a 2022 backfill run today would average in 2023/2024/2025 results too, none of which existed
yet in August 2022.

**Fix.** Added an optional `before_date` parameter to `get_league_averages()` (same
`match_date <` semantics `get_team_ratings()` already used) and wired `analyse_match()` to
always pass its own `match_date` through. Backward compatible — omitting `before_date` keeps
the old unfiltered behavior for any caller not yet updated. `backtest.py` goes through
`analyse_match()` so it's fixed for free.

**`param_sweep.py` follow-up — fixed by routing through `analyse_match()`, not by patching its
own call (2026-07-27).** It had the same leak independently (`get_league_averages()` called
directly with an un-cutoff `[SEASON]`-scoped call, bypassing `analyse_match()` entirely) because
it needed to vary `shrinkage_k`/`decay` per sweep iteration, which `analyse_match()` didn't
expose. Rather than patch that one call site (leaving two independent implementations of the
same pipeline, which is how this bug existed in two places to begin with), `analyse_match()`
gained optional `shrinkage_k`/`decay` override params (default to the existing module constants
— zero behavior change for every other caller) and `param_sweep.py`'s `run_one()` was rewritten
to call `analyse_match()` directly, consuming its returned `p_home`/`p_away`/`ev_home`/`ev_away`
instead of re-deriving them from `estimate_lambdas`/`scoreline_grid`/`outcome_probs` by hand.
This also drops the season-2025-only restriction on the league-average baseline during
tuning — see **DESIGN-002** for why blending prior seasons (matching what live picks/backfills
already do) was chosen over preserving that restriction, and for the open question of whether
the baseline should be season-scoped or recency-weighted at all.

**Re-running the sweep post-fix changes the ranking.** With the same test split (152 matches,
season 2025, 40% holdout), the currently-live constants (`SHRINKAGE_K=0`, `RECENCY_DECAY=1.0`)
now rank below the new top combo: `k=2, decay=0.95` → +25.4% ROI / 5.8% calib. MAE vs. the
live combo's +19.0% / 6.6%. Not acted on — updating the live constants is a separate decision
from fixing the measurement pipeline, and wasn't requested.

**Tests.** `tests/test_poisson_model.py`: `test_get_league_averages_before_date_cutoff`
(unit, proves the cutoff and that omitting it reproduces the old behavior) and
`test_analyse_match_league_avg_excludes_future_matches` (integration, proves a later
high-scoring match added to the DB doesn't change an earlier match's `analyse_match()` result).

**Quantified impact (Serie A 2025-26, 380 matches, `poisson_v1` vs `poisson_v2`).** Small, as
expected — the leak here is only within a single season (2025-26 is the newest season in the
DB, so `poisson_v1`'s baseline included the same season's own later-in-season results, not a
whole extra season): mean abs diff `p_home` 0.0025, `p_draw` 0.0011, `p_away` 0.0018, `p_over`
0.0024 (max diffs all < 0.012). EV shifts a bit more (mean abs diff ~0.006-0.007, max ~0.04)
since it amplifies small probability changes against fixed odds. **Practical effect: the
model-favored side changed on 2/380 matches**, and **an EV sign flipped on 2/380** (both
`ev_away`, both crossing from slightly negative to slightly positive — Juventus/Parma
2025-08-24 and Cagliari/Milan 2026-01-02; neither was ever an actual stored pick, since Serie A
has no picks table, see the note above this bug). No `ev_home` sign flips. Confirms the bug was
real but low-severity for a single-season backfill; the concern flagged above — that it
compounds across multiple accumulated seasons — is still open for the 2023-24/2024-25 backfill,
which will be diffed the same way once run.

---

## WATCH — Knockout ROI shortfall: selection-threshold backtest + variance decomposition

- **Type:** analysis / watch, no action · **Status:** OPEN, revisit post-tournament with the
  full sample. Prompted 2026-07-08 by the observation that R16 ROI is still slightly negative
  (-0.9%, 8 picks) even after FEATURE-009's two-step modes replaced R32's plain-best-EV legacy
  selection (R32 -15.7%, though the direct comparison is muddied since R32 never actually ran
  the two-step framework).

**Backtest 1 — prediction-mode floor (`PREDICTION_MODE_MIN_IMPLIED_PROBABILITY`, B2), swept
50/55/60/65/70% against R32+R16 (24 games), B1 held at 60%.** Smooth, monotonic: 60% is at or
near the best setting tested (combined ROI +28.2% vs +12.4%/+1.0% at 50/55%, +24.1%/+22.9% at
65/70%). Mechanism: lowering B2 lets prediction mode reach for progressively more marginal
("barely favored," near-50%-implied) candidates, which pay better but are less reliable —
in this sample, more of those reaches lost than won. **No change indicated.**

**Backtest 2 — value-mode floor (`VALUE_MODE_MIN_PROBABILITY`, B1), same sweep, B2 held at
60%.** Less clean. Combined R32+R16 is non-monotonic (a dip at 55%), but **R16 alone is
genuinely monotonic** (60%→-0.9%, 55%→+6.3%, 50%→+18.7% ROI) — the only period the two-step
framework has actually been live, so it's a real test, not a hypothetical retrofit onto R32's
legacy picks. Traced to source: the entire R16 curve is produced by exactly **4 game-level
swaps** — Mexico/England and Portugal/Spain flip at the 55% step (net +0.58u, one is a wash),
then Paraguay/France (**+2.32u**, a real winner previously excluded from value mode by the
model-probability floor alone — the model's own EV read was correct, but 53.6% < 60% forced
a fall-through to prediction mode, which grabbed the wrong side) and Argentina/Egypt
(**-1.33u**, the opposite: a winner-under-prediction that turns into a loser-under-value) flip
at the 50% step. Two large, opposite-signed swings netting positive twice in a row is not
strong independent evidence — it's closer to a coin flip landing the same way twice.

**Why no change was made despite the monotonic R16 shape — tested directly, not just
reasoned about.** Decomposed the knockout total-goals gap into "model vs FIFA's own
real-time xG" (the addressable part, if our process-read were off) vs. "FIFA xG vs actual"
(the unavoidable part — pure in-game variance), using `soccer_wc_external_xg`:
```
              model vs actual   model vs FIFA xG   FIFA xG vs actual
R32 (16 gm)      MAE 0.61            MAE 0.66            MAE 0.54
R16 (8 gm)       MAE 1.62            MAE 0.78            MAE 1.48
```
The model's read of the underlying process (vs. FIFA's own measured xG) barely degrades from
R32 to R16 (0.66 → 0.78) — **the model itself hasn't gotten worse.** What blows up is
actual-score volatility: **even FIFA's own shot-by-shot, real-time xG measurement misses the
final score by MAE 1.48** in R16 — nearly as badly as the model misses it (1.62). If the best
possible in-game process measurement can't predict the scoreline much better than we can,
no selection-threshold tweak reaches that gap; it isn't sitting in "which candidate we picked,"
it's sitting in the games themselves. Compounding this for R16 specifically: 5 of 8 picks were
on the to-advance market (not totals, so a different variance channel — who wins the tie, not
goal count), and one of those five (Switzerland/Colombia) was decided by a **penalty
shootout** — about as close to a structural coin flip as the sport has.

**Conclusion: hold both floors at 60% through the semifinal.** Not just "sample too small" —
the FIFA-xG decomposition is independent evidence that the R16 shortfall looks like inherent
knockout-stage variance (including a shootout), not a fixable systematic bias to tune toward.
Revisit with the full tournament sample (ideally next tournament's early rounds too) post-2026;
Paraguay/France is worth a specific look then as the one concrete "model was right, floor
excluded it" case.

---

## BUG-007 — FC Bayern München players pulled from DFB Pokal (cup) instead of Bundesliga — **FIXED 2026-07-08**

- **Severity:** high (16 players across 10 national teams, including 2 live QF teams) ·
  **Status:** FIXED 2026-07-08. Discovered during the Haaland/Norway attack-dilution analysis
  when the user's own Google check of Harry Kane's Bundesliga minutes (~2300) didn't match the
  ~537 minutes stored in the DB.

**Root cause (confirmed live against TheStatsAPI).** `import_wc_player_stats.py`'s `club_meta()`
trusts each club's `primary_competition` field to pick which league to pull season stats from.
For FC Bayern München specifically (`api_club_id tm_98299`), TheStatsAPI's `teams/tm_98299`
endpoint incorrectly returns **DFB Pokal** (`comp_3620`, the German Cup) as the primary
competition instead of **Bundesliga** (`comp_4643`) — verified by checking Borussia Dortmund
and RB Leipzig (`tm_51366` etc.), both of which correctly return Bundesliga. This is an
upstream data error specific to Bayern's team profile in the API, not a bug in our import
logic (which reasonably trusts the field). Every Bayern player in every WC squad silently got
a tiny-sample cup stat line instead of a near-full domestic season.

**Affected players (16, across 10 national teams) — old (wrong) vs. new (correct Bundesliga,
season `sn_5789634`) minutes/goals/assists:**
```
Player               Team          OLD min/g/a        NEW min/g/a
Konrad Laimer         Austria       373 / 0 / 0        1997 / 3 / 9
Alphonso Davies       Canada        105 / 0 / 0        534 / 1 / 3
Luis Díaz             Colombia      538 / 3 / 2        2450 / 15 / 14
Josip Stanišić        Croatia       437 / 0 / 1        1855 / 2 / 3
Harry Kane            England       537 / 10 / 0       2382 / 36 / 5
Dayot Upamecano       France        450 / 0 / 0        1797 / 1 / 1
Michael Olise         France        522 / 2 / 1        2317 / 15 / 19
Aleksandar Pavlović   Germany       448 / 0 / 0        1461 / 3 / 1
Jamal Musiala         Germany       181 / 0 / 1        685 / 3 / 4
Jonathan Tah          Germany       540 / 0 / 0        2024 / 2 / 1
Joshua Kimmich        Germany       540 / 0 / 2        2280 / 2 / 8
Leon Goretzka         Germany       120 / 0 / 1        1954 / 5 / 3
Manuel Neuer          Germany       270 / 0 / 0        1860 / 0 / 0
Hiroki Ito            Japan         13 / 0 / 0         932 / 1 / 2
Nicolas Jackson       Senegal       8 / 0 / 0           1037 / 8 / 1
Kim Min-jae           South Korea   111 / 0 / 0        1622 / 1 / 1
```
`club_league` for all 16 also corrected from "DFB Pokal" (league_factor 0.95) to "Bundesliga"
(0.97) — a minor secondary effect versus the minutes/goals correction. Defense-side fields
(`club_xga_per90`/`club_ga_per90`) were already NULL for all 16 (not populated at all, not
just wrong), so this fix is attack-side only; left out of scope rather than expanding the fix
unprompted.

**Directly relevant to two live QF teams: England (Kane) and France (Upamecano, Olise).**
Also affects Austria/Canada/Colombia/Croatia/Germany/Japan/Senegal/South Korea, all already
eliminated — corrected for data-integrity/postmortem accuracy, not because it changes any
live pick.

**Fix applied (attack side, v13):** `soccer_wc_player_stats` rows updated in place with the
correct values (a factual data correction, not a modeling choice — no "old version" preserved
at this layer, but the wrong values are fully documented above). `soccer_wc_team_strength` is
versioned (no unique constraint on `team_id`; every `--persist` inserts rather than
overwrites), so a new version was persisted on top of the existing v1–v12 history — **all
prior versions remain queryable** for a postmortem comparison of old- vs. new-data-based λ.
Because the model normalizes attack values across the whole 48-team field, persisting after
this fix produced a new version row for all 48 teams, not just the 16 players'/10 teams'
directly affected — expected, and the other teams' λ shift is negligible.

**Completion (defense side, v14) — `club_ga_per90` was ALSO silently null for all 16 Bayern
players, same root cause.** `club_defense()` fetches team-level stats via
`teams/{api_club_id}/stats` using whatever `season_id` `club_meta()` resolved — the broken
DFB-Pokal season for Bayern. Confirmed live: `teams/tm_98299/stats` under the DFB-Pokal
season returns **no data at all** (`None`), while under the correct Bundesliga season it
returns `matches_played=34, goals_against=36`. So Bayern's defenders/GK weren't just missing
attack signal — their defense signal was silently **zero-weighted** (null, not wrong-but-present)
in every team's aggregate. Backfilled `club_ga_per90 = 36/34 = 1.0588` for all 16 players and
persisted **v14** ("backfilled Bayern Munich players' club_ga_per90 (BUG-007 completion)").
Effect on the one live QF team this touches (Dayot Upamecano, France defender): France DEF
1.0651 → 1.0626 — negligible, since one moderate (near-team-average) defensive data point
added to an otherwise-full squad aggregate barely moves the mean. `club_xga_per90` remains
null for every player tournament-wide — that's the separate, deliberate DESIGN-001 gap (no
real xG-based defense metric was ever built), not part of this bug.

---

## FEATURE-008 (extension) — Official FIFA xG as a second external source — **SHIPPED 2026-07-07**

- **Type:** feature / analysis · **Status:** SHIPPED 2026-07-07. Requested by user, who
  pointed to FIFA's own match-report hub as a more authoritative xG source than
  TheStatsAPI.

**What it does.** `import_wc_fifa_xg.py` scrapes FIFA's two match-report hub pages
(group + knockout stage), downloads each match's official "Post Match Summary Report"
PDF, and extracts team-level xG from the "Match Summary - Key Statistics" page (found
via search, not a hardcoded page index) using `pdfplumber`. Stored in the SAME
`soccer_wc_external_xg` table as the TheStatsAPI pull, keyed by `source='fifa_official'`
— no schema change, the two sources sit side by side per match. Same hard constraint as
the original FEATURE-008: never mixed with the model's own xG or any core-workflow
table. **Live run 2026-07-07: 94/94 matched and stored** (all finished matches to date;
no `--scope survivors` restriction here since the hub pages only ever list finished
matches anyway).

**Two real bugs caught and fixed during the live run, not just theoretical:**
1. Team-name spelling mismatches (Korea Republic, IR Iran, Czech Republic, Ivory Coast,
   Bosnia and Herzegovina, Cabo Verde, Congo DR, Türkiye/Turkey) — `FIFA_TEAM_ALIASES`.
2. **FIFA's page-1 score is the extra-time-INCLUSIVE final score for a tie decided in
   ET, not the bare 90' score** (penalties aren't goals, so a shootout-decided tie's
   page 1 still shows the 90' score — only ET differs). Caught live: Belgium 2-2
   Senegal (ET winner 3-2) reported as "Belgium 3 - 2 Senegal", which tripped the
   score-sanity-check against our stored 90' score (2-2) until `final_score()` was
   added to compare against `extra_time_home_score`/`extra_time_away_score` when
   present. A test (`test_scores_agree_uses_extra_time_inclusive_score`) locks this in.

**Implementation.** Filenames on the hub pages are NOT a reliable naming pattern
(separators vary space/hyphen, revised reports carry a `-V2`/`POST-V2` suffix) so team/
date/score identity comes entirely from each PDF's own text (page 1), never guessed
from the filename. Matching is by unordered team-pair (FIFA's listed order doesn't
necessarily match our nominal home/away for a neutral-venue game), same pattern as
`import_wc_match_xg.py`. New dependency: `pdfplumber` (not yet in a formal
requirements file — this repo has none; installed ad hoc). 18 new tests in
`tests/test_import_wc_fifa_xg.py` covering all the pure parsing/matching/scoring logic;
the hub-scraping + PDF-download path is untested by design (same convention as this
repo's other live-network import scripts).

**Bonus deliverable.** `FIFA_MATCH_REPORT_DATA_SURVEY.md` catalogs everything else in
these reports beyond xG (per-player passing networks, pressing maps, physical data,
goalkeeping detail, etc.) — not implemented, just scoped for later. Flags that almost
all of it is **national-team, per-match data**, which is the direct answer to the
club-stats-don't-transfer problem behind DESIGN-001/BUG-001/BUG-005, and that the
Phases-of-Play / Defensive-Pressure sections could quantify team "style" (press vs
block, transition-heavy vs possession-heavy) — a dimension the Poisson model has no
concept of today.

---

## FEATURE-010 — Per-match "close calls" candidate breakdown — **SHIPPED 2026-07-06**

- **Type:** diagnostic / visibility · **Status:** SHIPPED 2026-07-06. Requested by user
  after noticing two straight days of conservative (prediction/fallback mode) knockout
  picks, wanting fast visibility into what else was close without changing any pick.

**What it does.** `generate_wc_card.py` now prints a `CANDIDATE BREAKDOWN` section per
match: the top 3 candidates for EACH of FEATURE-009's three modes (value/prediction/
fallback), ranked by that mode's own rule (EV / payout / model prob respectively) —
purely informational, never affects the actual selected pick. A BUG-003-guardrail-
excluded candidate is still shown in the value list, tagged `near_miss`, if it missed
clearing the guardrail by `CLOSE_CALL_TOLERANCE` (0.02 / 200bps) or less — e.g. a cap
ratio of 2.01x instead of 2.0x. Candidates that miss by more stay excluded from the
diagnostic too.

**Implementation.** `guardrail_excess(c)` (worst/largest excess across any guardrails
tripped — a candidate must clear ALL of them, so the hardest one to fix determines how
close it really is) and `mode_breakdown(priced, top_n=3)` in `generate_wc_card.py`, both
pure functions reusing `select_pick`'s `excluded_by` annotations. 8 new tests in
`tests/test_generate_wc_card.py` covering the excess calculation, near-miss tagging,
far-miss exclusion, per-mode ranking, and the top-N cap.

**Update 2026-07-06 — added a `TOP EV` list (raw EV, no filter at all).** Requested the
same day: the VALUE list is still filtered to prob>=0.60 & EV>0, so a high-EV candidate
sitting just below that bar (or guardrail-excluded) never appeared anywhere marked as
"actually the best EV on paper." `top_ev` in `mode_breakdown` sorts ALL candidates by raw
EV with zero filtering, and `why_not_value(c)` annotates each with a plain-English reason
it isn't a value pick — a BUG-003 guardrail reason if one fired, else "below the value
bar" or "EV not positive." This is the direct answer to "why isn't the model finding value
here?" — e.g. 2026-07-06's Portugal/Spain card: top EV was Portugal-to-advance at +25.7%,
annotated `advance-edge` (the model over-rating a knockout-mismatch dog, per BUG-003's
2026-06-29 update) — confirming the demotion was principled, not the model missing an
obvious pick. 5 new tests.

---

## BUG-006 — Host venue-advantage boost applied without checking the match is actually domestic — **FIXED 2026-07-04**

- **Severity:** medium (affects only host-nation knockout matches, but flips the selected
  market/side when it fires) · **Status:** FIXED 2026-07-04.

**Symptom.** `HOST_HOME_ADVANTAGE` (1.20x attack multiplier) was applied to any match where
a host nation (USA/Mexico/Canada) appeared, purely by team identity — with no check on
whether that specific match is actually played in that team's own country. This is true by
tournament design for the GROUP STAGE (each host's group games are guaranteed domestic), but
knockout-round stadium assignment is independent of host status, and a host can end up
playing a knockout match in one of the other co-host countries.

**Caught live 2026-07-04** reviewing the newly generated Canada v Morocco R16 card: Canada
was still getting the boost despite this match being played in the USA. Quantified: with the
(wrong) boost, Canada's attack λ = 1.55 → Over 2.5 priced at model 63.7%/+41.4% EV, selected
via value mode. Without it, λ = 1.29 → model 58.2%/+29.2% EV — under the 60% value-mode bar
entirely, changing the selected pick from OVER 2.5 to AWAY ADVANCE (Morocco).

**Retroactive finding.** Checking history surfaced the same defect had already fired: Canada's
R32 match (vs South Africa, 2026-06-28) was confirmed (user) to have been played in Los
Angeles, USA — not Canada — so the ORIGINAL stored pick for that match (pick_id 87, HOME
ADVANCE/South Africa, lost) was computed with an erroneous boost on Canada's lambda. Left as
locked history (already graded); noted here for calibration awareness, not corrected
retroactively.

**Fix.** New single source of truth, `core/wc_host_advantage.py`, replacing two duplicate
constant definitions (`generate_wc_card.py`, `generate_scoreline_heatmap.py`) and nine
call sites across the codebase that each reimplemented the same
`HOST_HOME_ADVANTAGE if team in HOST_NATIONS else 1.0` ternary. Two tiers, since a host's
domestic run has to be tracked by hand (no venue/stadium column exists on
`soccer_wc_matches` to derive this automatically):
- `HOST_NATIONS` — currently-active hosts, boosted regardless of stage (as of 2026-07-04:
  `{"USA", "Mexico"}`).
- `GROUP_STAGE_HOST_NATIONS` — retired hosts, boosted only for the stage(s) confirmed
  domestic (as of 2026-07-04: `{"Canada"}`, Group stage only — their R32 game was NOT
  domestic per the finding above, so Group-only is the correct scope, not an oversight).
- `host_advantage(team_name, stage)` — the one function every consumer now calls.

**Planned next move (not yet executed, per user 2026-07-04):** once Mexico's confirmed-domestic
run ends (they have Group + R32 + R16 matches in Mexico, including their R16 match vs England
on 2026-07-05), retire Mexico from `HOST_NATIONS` into a new stage-scoped set covering all
three of those stages (name TBD at that time — flagged so we don't repeat the Round-of-32-vs-
Round-of-16 naming slip almost made here).

**Files touched.** New: `core/wc_host_advantage.py`, `tests/test_wc_host_advantage.py`.
Updated to source from it: `generate_wc_card.py`, `generate_scoreline_heatmap.py`,
`card_ladder_compare.py`, `price_ladders.py`, `blend_impact.py`, `feature009_backtest.py`,
`proxy_goals_calibration.py`, `proxy_defense_calibration.py`, `totals_calibration.py`.

---

## FEATURE-009 — Codify the two-step "best pick" selection (never pass) — **SHIPPED 2026-07-03**

- **Type:** core selection redesign · **Status:** IMPLEMENTED (user 2026-06-29: "it's not ok to say
  'pass'… codify this rather than have it be a discussion every time and me overriding").

**Implemented (2026-07-03).** `select_pick` in `generate_wc_card.py` now runs the two-step decision
below instead of pure EV-with-guardrails. Bars are named, tunable module constants (not the ad hoc
"B1"/"B2" used during backtesting):
- `VALUE_MODE_MIN_PROBABILITY = 0.60` — step-1 "realistic probability" bar (item 1 below).
- `PREDICTION_MODE_MIN_IMPLIED_PROBABILITY = 0.60` — step-2 implied-prob bar (item 2 below).

**Backtest that set the bars (`feature009_backtest.py`, not the 72-game group-stage-only scope
originally planned — extended to all 85 graded picks to date, group + R32).** Full `b1 x b2` grid
sweep vs. the actual historical system (EV-only, no two-step: 40-40-5, +6.03u on these 85 games):
- Best cell: **B1=0.60, B2=0.60 → +14.10u**, 70.7% hit rate (58-24-3), all three modes independently
  profitable (value n=26 +8.05u, prediction n=37 +3.56u, fallback n=22 +2.48u).
- Stage split (light stress test before locking): Group (72 games) +11.24u/69.6% hit; R32 (13 games)
  +2.86u/76.9% hit — edge holds in both, not a group-stage-only artifact.
- Robustness (3x3 neighboring cells): all profitable (range +3.65u to +14.10u), but 0.60/0.60 is a
  clear local peak rather than a flat plateau — neighbors cluster ~8-12u. Read as "somewhere in
  0.55-0.60 for both bars" being the durable signal; the exact peak may be somewhat sample-specific
  at n=85. Revisit once more knockout results land.

**Open design decisions — resolved.**
1. Step-1 bar: **0.60** (backtested; see above).
2. Step-2 bar: **0.60** (backtested; see above).
3. Staking tie-in (FEATURE-005): **NOT adopted** — user decision 2026-07-03: same stake size
   regardless of mode. Every mode is still just "the best pick we can name," not a partial bet.

**Tracking added.** `soccer_wc_picks.selection_mode` (TEXT: `value` | `prediction` | `fallback`) —
migration `ensure_wc_picks_schema` in `core/sports_db.py`, populated by `generate_wc_card.py`, so
per-mode performance can be reviewed as more results land (the backtest's per-mode breakdown above
was the first pass; this makes it an ongoing, queryable signal instead of a one-off computation).

**Design retained from the original spec below**, now shipped as described.

**Why.** The system's mandate is **one best pick per match, no abstention**. Today `select_pick`
only ever optimizes **EV** (with a floor + cap), so on games where the model finds no honest value
(e.g. contaminated knockouts like SA/Canada) it still posts a thin/mirage EV pick, and the *only*
correction is a hand-override. That's a recurring manual tax and it isn't reproducible. "Pass" /
"stake ~0" is **not** an acceptable answer — the model must always name a best pick.

**The design (user's two-step spec, 2026-06-29).**
- **Step 1 — value mode (trust the model).** Look at EV only and find a **good** pick = good payout
  **with** a good + *realistic* probability. If found, take it and stop.
- **Step 2 — prediction mode (only if step 1 finds nothing good).** Stop trusting the model; use the
  **market-implied** probabilities to pick the most-probable outcome, taking the **best payout among
  the genuinely-likely** ones. (Using *implied* prob is deliberate: step 1 fails precisely where the
  model is unreliable, so the fallback defers to the market.)

**Original open design decisions (historical — resolved above with 0.60/0.60).**
1. **Step-1 "realistic probability" bar.** Must reject mirages (SA-advance, model 0.346) AND thin
   contaminated value (SA/Canada Over 2, model 0.518) — a ~0.55 model-prob bar does both; ~0.40 lets
   Over 2 through (and it lost). Grounded in calibration: model is reliable only in **p 0.45–0.70**.
   Likely = a **higher** prob floor than today's 0.25 + tighter market-agreement than the 2× cap.
2. **Step-2 probability-vs-payout balance.** Naive "best payout among probable" can land back on the
   coin-flip loser (Over 2 −148 beats Canada-win −152 on payout). Needs a **high implied-prob bar
   (~0.60)** so "best payout" is chosen only among genuine favorites → Canada-win (won), not Over 2.
3. **Staking tie-in (natural, optional).** Step-1 picks = real **value bets** (staked); step-2 picks
   = **predictions** (post them, ~0/min stake). Formalizes the user's "stake ~0" intuition without
   abstaining. Ties to **FEATURE-005**.

**Validation done (SA/Canada, the motivating case).** With sane bars (step-1 prob ~0.55, step-2
implied ~0.60): step 1 rejects all (SA-advance mirage + thin Over 2) → step 2 → **Canada win 90'
−152 → WON**. The algorithm produces a winning, credible pick where today's EV-only logic posted a
loser. Naive bars do NOT work — hence the open decisions above.

**What it touches.** `select_pick` in `generate_wc_card.py` (the two-step logic), reusing the
existing `excluded_by`/guardrail-log machinery; possibly a `selection_mode` (value/prediction) tag on
`soccer_wc_picks`; the FEATURE-005 stake field if the staking tie-in is adopted. **Before coding:
backtest the bars on the 72-game group stage** (compute-only, like the blend test) — show, per bar
setting, the value-vs-prediction split and resulting P&L + hit-rate — so thresholds are tuned on data.

---

## FEATURE-008 — External-source xG ingestion for comparison ONLY (separate table)

- **Type:** feature / analysis · **Status:** BUILT 2026-07-06, scoped to survivors — not yet
  run live against the API (needs a `--dry-run --team <one team>` smoke test first, same
  convention as this repo's other TheStatsAPI import scripts). Feasibility confirmed + key
  constraint set with user 2026-06-28.

**Update 2026-07-06 — built, scoped to survivors by default.** `import_wc_match_xg.py` +
`soccer_wc_external_xg` table + `upsert_wc_external_xg` shipped. Default `--scope survivors`
only pulls matches involving a team that has reached R16 or later (currently: Brazil, Canada,
England, France, Mexico, Morocco, Norway, Paraguay — 32 of 90 finished matches) — the live-DB-
derived set of teams whose games can still inform a future pick, not a hardcoded list. Pass
`--scope all` for the full tournament (identical code path, just skips the survivor filter) —
that's the intended mode for the post-tournament postmortem this was requested for. Tests cover
all the pure logic (name matching, date tie-breaking, xG aggregation, scope filtering);
the live API call path is untested by design, matching this repo's convention for
TheStatsAPI-calling scripts (validate with a small `--dry-run` pull, not a mock).

**Why.** Results are the primary eval; external xG is a **secondary diagnostic** for the
over-skew / calibration review (separates "good pick, bad variance" from genuine mispricing).
Feasibility confirmed live: **TheStatsAPI already carries it** — `comp_6107` "FIFA World Cup",
`xg_available=True`, season `sn_118868`, 72 finished matches. Per-team-per-game xG = sum
`matches/{id}/player-stats → shooting.expected_goals` by team. Cross-checked vs the user's source
on Colombia/Portugal: API 1.64/0.91 vs user 1.70/0.93 — agrees. No new dependency (existing
`core/thestatsapi.Client`, key configured). ESPN not needed.

**HARD CONSTRAINT (user, 2026-06-28) — do NOT confuse our xG with external xG.** The project's
purpose is our OWN analytics; **the model's own xG (`soccer_wc_player_stats.xg_per90` from the
per-match pass) is the source of truth.** External-sourced xG must **never** be written to
`soccer_wc_matches` or any core-workflow table. Store it in a **new, clearly-labeled table**
(e.g. `soccer_wc_external_xg`: `match_id`, `source`, `home_xg`, `away_xg`, `fetched_at`) used
**only** for post-hoc comparison/learning, so the two sources are never mixed.

**Scope.** `import_wc_match_xg.py`: map our matches → comp_6107 matches (team names + date, like
`import_wc_odds.py`), pull team xG, write the new comparison table. ~72 requests (~6 min). Then
"model expected total vs external xG total" is a join, kept entirely out of the core tables.

---

## FEATURE-007 — Dynamic odds refresh + realized-line capture (close the staleness gap)

- **Type:** feature · **Status:** BACKLOG. Requested by user 2026-06-26 as the actionable
  sibling of **ROI-STALENESS** (the caveat) and overlaps **FEATURE-003** (ladder ingestion).

**Why.** Odds are captured once (CSV import at generation/capture time) and then go stale. By
bet time the line can move a **lot**, with no fast path to refresh the stored line near post/bet
time or to record the actual fill — so the model record and realized ROI drift apart, sometimes
by more than a little per bet. Live evidence (2026-06-26, all stored vs realized within hours):
- **NZ/Belgium Over 2.5** — stored **−122 @ line 2.5**, realized **−290 @ line 2.75** (kept bet;
  large *adverse* move).
- **Egypt/Iran Over 2** — stored **−108**, realized **+112** (override #5; move *in our favor*,
  EV +16.3% → +25.5%).
  Opposite directions the same day — the per-bet swing is real and two-sided.

**What it touches (options, pick at build time).**
- A quick **odds-refresh path** — single-match or slate re-import run close to post time so the
  stored line ≈ the line actually bet (shrinks the gap at the source).
- **Realized/closing-line capture** per pick — the `closing_odds`/`actual_odds` column from
  ROI-STALENESS → compute ROI both ways (model-line vs realized-line / CLV).
- Optionally **re-price the card against fresh odds** right before posting.
- Naturally pairs with FEATURE-003's ladder table (a fresh-odds path wants somewhere to put the
  ladder anyway).

**Note.** The override tracker already captures real `user_odds`, so scrutinized/overridden picks
are honest today; this feature generalizes that to *all* picks and to a pre-post refresh.

**Update 2026-07-08 — QF backlog review; confirmed BACKLOG, post-tournament.** Directly
demonstrated this session: a Switzerland/Colombia totals quote was entered by overwriting the
existing Bovada row in place (rather than capturing it as a separate realized-line snapshot),
which silently lost the original 2.0 line and corrupted a downstream backtest until caught and
fixed by hand. Real evidence the gap this feature closes is live, not theoretical. **Decision
(user): leave as-is for the remainder of this tournament** — build after the World Cup
completes, not mid-tournament.

---

## ROI-STALENESS — Stored odds go stale; reported ROI is an optimistic ceiling (CLV)

- **Type:** measurement caveat / accounting · **Status:** ACKNOWLEDGED, no action today
  (deliberate). Surfaced by user 2026-06-26. Related to **FEATURE-003** (line movement).

**Why.** Odds are stored at card-generation/capture time, but the line moves before the bet
is actually placed. Scoring the next day uses the **stored** odds, so a winning pick is
credited at the price we captured, not the price a bettor realistically got. Example (2026-06-26):
NZ/Belgium Over 2.5 is stored at −122, but the total moved to 3.75, so the real Over-2.5 price
is far worse (~−250); a win would be scored at the inflated −122.

**The correction to "it comes out in the wash."** It does **not** cleanly wash for a model with
genuine edge. Lines tend to move **toward** the model's number (the value gets bet away; the
closing line is sharper), so stored (earlier) odds are **systematically better** than realized
odds → reported ROI is biased **upward**, modestly but consistently (this is closing-line value).
Magnitude is usually small per game, occasionally large (big total moves like NZ/Belgium). So
**treat the model's reported ROI as an optimistic ceiling, not the realized figure** — paradoxically
the better the model, the larger the overstatement.

**Decision today.** Leave odds as-is (consistent with the 2026-06-26 "don't update the last two
group-stage days' odds" call) and carry the caveat. No code.

**Cheap mitigations (future, if we want honest realized ROI).**
- Capture odds as close to **post/bet time** as possible to shrink the gap (discipline, not code).
- At grading, record the **actual closing/bet line** per pick — a `closing_odds`/`actual_odds`
  column on `soccer_wc_picks` → compute ROI **both ways** (model-line vs realized-line, i.e. a
  CLV column). The real fix; ongoing per-pick data entry.
- Note: the **override tracker already stores real `user_odds`**, so picks you actually scrutinize
  are already honest — the bias mainly affects picks taken **as-is** without re-checking the line.

---

## FEATURE-006 — Suppress moneyline picks that bet *against* a FIFA-override-pinned team

- **Type:** feature / selection guardrail · **Status:** BACKLOG — **LOWERED PRIORITY 2026-07-06**.
  Scope/design still stands (below) if ever revisited, but of the 7 fully FIFA-pinned teams
  (`method='fifa_ranking'`) only 3 remain alive at R16 (Mexico, Belgium, Egypt) — too few
  remaining games to justify building this now. Also note the feature as scoped only catches
  a **full** pin, not a per-component blend override (e.g. Algeria/Switzerland,
  `method='player_aggregation'`), so the applicable set is narrower than "any FIFA-adjusted
  team." Revisit at the start of a future tournament instead. Related to **BUG-005** (the
  pins it keys off).

**Why.** A pinned team's λ is a **hand-set FIFA-rank anchor** (`method = 'fifa_ranking'`), the
model's least-reliable input, and the pin tends to **under-correct** (the market rates the team
*above* the rank pin — the very reason the raw stats looked broken). So a 1X2 pick that needs the
pinned team to **underperform** (opponent ML, or draw) rests on the model's weakest leg. **Totals
are robust** — they route through the goal *sum*, not the win-split, and the BUG-005 error
(attack under-rated) pushes the total *up*, which *helps* an over. Same model, sturdier leg.

**Live case (2026-06-26).** Egypt/Iran: model #1 = **Iran ML +260 (3★, +39.1%)**, resting entirely
on Egypt's pinned rating; hand-overridden to the model's own #2, **Over 2 −108 (+16.3%)** — logged
as override #5 [robustness]. This rule would surface that pick **through the model** instead of by
hand, exactly as the v8 Egypt pin already self-demoted the NZ moneyline (see BUG-005).

**Update 2026-07-08 — QF backlog review; confirmed leave as-is.** Belgium is now the only
fully FIFA-pinned team left alive (Mexico, Egypt eliminated) — Spain/Belgium is the sole match
this would touch, and if built it would specifically exclude a Spain ML (and draw) pick,
leaving only Belgium ML or totals eligible. **Decision (user): leave as-is, re-evaluate at the
start of a future tournament** — consistent with the 2026-07-06 lowered-priority call, not
worth building for one remaining match.

**Scope (checked against the code 2026-06-26 — relatively easy, ~40–60 lines + 1–2 tests, no
schema change / no migration).**
- **Detection** trivial: `method = 'fifa_ranking'` on the latest strength row (6 teams currently
  pinned). Add `is_wc_strength_pinned(team_id, conn)` in `core/sports_db.py` (~6 lines; avoids
  touching `get_latest_wc_strength`'s signature + its callers).
- **Hook** in `best_pick_for_match`: compute `home_pinned`/`away_pinned` from the strengths it
  already fetches (~3 lines).
- **Rule** in `select_pick`: reuse the existing **BUG-003 exclusion machinery** (`excluded_by` /
  reasons / demoted-log) to drop the **against-pin 1X2 candidates**; selection falls through to the
  next eligible pick automatically (~10 lines). The guardrail log line comes free.
- **Test**: seed a `method='fifa_ranking'` game with an ML dog as top EV + a +EV total; assert the
  total is selected and the ML is logged excluded. Egypt/Iran is a ready-made fixture.

**Design choice (recommended):** exclude only the picks that bet **against** the pinned team
(opponent ML + draw); **keep totals and backing the pinned team itself** eligible. Surgical and
matches the reasoning (the pin under-corrects → betting *against* it is the fragile bet; backing it
is fine). Alternatives — blunt "exclude all 1X2 in pinned games" (over-suppresses) or an EV haircut
(adds a tuning constant; needs ~58%+ to flip Egypt/Iran) — rejected for v1. Fully logged, so grading
measures whether suppressed picks would've won (same as BUG-003).

---

## REFACTOR-001 — Generalize schema for multi-tournament reuse (drop `wc_`)

- **Type:** refactor / tech-debt · **Status:** BACKLOG — **post-deadline**, deliberate one-shot.
  Surfaced 2026-06-25 during FEATURE-002 design.

**Why.** The `soccer_wc_*` tables are World-Cup-specific in name, but the schema (matches,
penalties, ET goals, picks, odds) is tournament-agnostic and reusable (Champions League, Euros…).
FEATURE-002's two new tables were named generically (`soccer_penalty_kicks`,
`soccer_extra_time_goals`) as a seed, leaving a temporary generic-vs-`wc_` naming mix.

**What it takes (why deferred).** `ALTER TABLE ... RENAME` on all 8 `soccer_wc_*` tables (data
preserved) + every reference across `core/sports_db.py`, ~6 scripts, and all tests + renaming
~30 `*_wc_*` CRUD helpers + **coordinating a breaking change with the external serie-a-bets-tracker
repo** that reads these tables. Real cross-repo risk; not to be bolted onto a deadline feature.

**Scope when done.** Rename all tables/helpers off `wc_` **and** introduce generic
match/team/player parent tables (the part that actually unlocks cross-tournament reuse — the new
event tables currently still FK to `soccer_wc_*` parents in this DB).

---

## FEATURE-005 — Confidence-weighted staking plan (stake field + stake_for_stars)

- **Type:** feature · **Status:** BACKLOG — **LOWEST priority** (build after FEATURE-003/-002,
  and only if -004 doesn't take precedence). Confirmed low-priority with user 2026-06-25.

**Why.** Everything is currently **flat 1u** — both the model record (`soccer_wc_picks`, no
stake column) and the override tracker. There's no way to stake by confidence or to record a
partial stake. Prompted 2026-06-25 when the user wanted to play **0.5u** on a low-conviction
1★ pick (Tunisia/Netherlands Under 2.5) — which the schema can't represent.

**What the data already says (54 graded picks, 2026-06-25):** star tier *does* carry signal,
but **opposite to the 1★ instinct** — 1★ is mildly **+** (n=14, +4.6% ROI/bet), 2★ is **−**
(n=7, −14.5%, small-sample noise), 3★ carries the book (n=33, **+19.3%**). So a sound plan
stakes **UP on 3★**, not down on 1★. A naive "stake = stars" weighting backtests ~14% ROI vs
flat ~11% (the best, biggest tier gets the most weight). Re-run this backtest before committing
a plan — samples are still modest. Sizing *down* on low-confidence picks is still defensible as
**variance management** (Kelly-style: smaller/under-certain edge → smaller stake), independent
of EV sign — that's the user's Tunisia rationale.

**What it touches:** a `stake` column on `soccer_wc_picks` (+ override table), a
`stake_for_stars(stars)` plan function (default the backtested shape), card output shows stake,
and `roi_history` / grading weight units by stake. Small build.

**Noted manual instances (no stake field yet — fold into realized-ROI accounting):**
- 2026-06-25 Tunisia/Netherlands **Under 2.5 @ -102 staked 0.5u** (model record holds it flat 1u).

---

## FEATURE-004 — Dead-rubber / motivation flag (demote win-picks on clinched teams)

- **Type:** feature candidate · **Status:** CLOSED 2026-07-06 — test window (group stage) is
  over; no more dead rubbers can occur (every knockout match is win-or-go-home by definition).
  Final tally: 2 tagged `motivation` overrides, **2-0** (both won). A real, consistent pattern,
  but too small a sample and now structurally out of runway to justify building the flag this
  tournament. Revisit at the start of a future tournament's final group matchday if the
  pattern is worth testing again from scratch.

**Why.** The model is structurally blind to **stakes/motivation**. It prices a clinched
(already-advanced) team the same in a meaningless final group game as in a must-win, so it
will confidently back an **unmotivated** side's win line. First live case (Jun 24): model
picked **Canada +250** to win at Switzerland — but Canada had already advanced (nothing to
play for) while **Switzerland still needed the result**. Switzerland won 2-1. The tagged
`motivation` override (pivot to **Over 2**, same high-scoring model thesis but not dependent
on the unmotivated team *winning*) cashed +0.85u while the model's Canada ML lost (+1.85u edge).

**The signal (well-defined, generalizable).** In a final group game, if a team has clinched
(or is eliminated) and its opponent still has something to play for, **demote/avoid that team's
win line** — and (the override's insight) the goals market is often the better expression: a
motivated side pressing an indifferent one tends to *raise* the total, not lower it.

**What it would touch:** a per-match "stakes" input (clinched / eliminated / alive — needs a
standings + scenario source, or a manual flag), and a `select_pick` rule that demotes a
moneyline pick when the backed team is in a dead rubber vs a live opponent.

**Why WATCH not BUILD.** n=1. BUT the **final group matchday is exactly when dead rubbers
appear en masse**, so the `motivation` override category should accumulate fast over the next
few days. If it keeps beating the model, that's the empirical case to build the flag. Until
then, handle case-by-case via tagged `motivation` overrides (don't hand-fade silently —
[[let-the-model-ride]] — log it so the pattern is measured).

---

## FEATURE-003 — Price every posted O/U total line, not just the stored one

- **Type:** feature · **Status:** BACKLOG — **post-tournament**. Originally QUEUED
  ("build first", target 2026-06-24/25) but never actually built; superseded in practice by
  manual ladder pricing on request all tournament. Re-scoped to post-World-Cup 2026-07-08
  during a QF backlog review (no user demand for it materialized once FEATURE-002 shipped and
  knockout odds narrowed to fewer, simpler markets).

**Why.** Books post a **ladder** of total lines per game (e.g. Portugal O/U 2.5 / 3.0 / 3.5 /
4.0, all bettable at different prices). The card prices only the **single stored line**, so
real EV on a different line is invisible. The model already computes the **full scoreline
grid** → P(over/under) at *every* line for free (`totals_probs`); only the odds-feeding side
is missing. Demonstrated 2026-06-23 on the Portugal game: the model's fair-odds ladder showed
big edges sitting on lines we never priced.

**What it touches:**
- **schema** — a small `soccer_wc_total_lines` table (`match_id`, `sportsbook`, `line`,
  `over_odds`, `under_odds`) holding the ladder; keeps 1X2 / primary odds clean and sidesteps
  the "which odds row owns the moneyline" mess.
- `import_wc_odds.py` (or a sibling) — load the ladder.
- `generate_wc_card.py` — `best_pick_for_match` builds candidates from 1X2 **+ over/under at
  every stored line**, then `select_pick` **once** across all of them. **Also kills the
  latent duplicate-pick JOIN bug** in `fetch_matches` (multiple odds rows → duplicate picks)
  by collapsing to one candidate set per match.
- tests.

**Effort:** moderate, ~a few hours. **Order:** (1) this, then (2) FEATURE-002 (to-advance),
per user. Interim: ladders priced **manually** on request (give the book's O/U ladder, model
returns best-EV line) until shipped.

---

## KNOCKOUT-PRICING — confirm 90-minute markets at the knockout transition

- **Type:** watch / reminder · **Status:** CLOSED 2026-07-06 — trigger fired and passed clean.
  Every R32/R16 card since 2026-06-28 has correctly separated 90-minute markets from the
  to-advance market (FEATURE-002); no mis-pricing observed. No further action.
- **Trigger:** group stage ends ~2026-06-27; the first knockout card is the check.

**What.** `analyse_match_wc` prices **90-minute** 1X2 + totals. Knockout games can't end
in a draw (extra time + penalties), so books post BOTH a **90-minute** line (with Draw)
and a separate **"to advance"** market. DRAW picks and O/U totals still settle correctly
**only if we keep ingesting the 90-minute lines.** On the first knockout card, confirm
`import_wc_odds` is loading 90-min markets — do **not** feed a to-advance price into the
card as if it were 90-min (it would mis-price the draw and the moneylines). This is the
*defensive* check only; betting the to-advance market itself is a separate build — see
**FEATURE-002**.

---

## FEATURE-002 — "To advance" market for knockout ties

- **Type:** feature · **Status:** SHIPPED 2026-06-29 (stale "IN PROGRESS" label corrected
  2026-07-06). Live in every knockout card since: `advance_probs`, `home_advance_ml`/
  `away_advance_ml` odds capture, ADVANCE candidates + guardrails in `select_pick`, graded via
  `advancing_side`/`grade_pick`. **Requirements** → [FEATURE-002_TO_ADVANCE.md](FEATURE-002_TO_ADVANCE.md).
  Decisions: real ET/PK model with a proxy bench nudge, team+player-level ET/PK data capture,
  manual results entry, reusable market-agnostic grader in `core/` for the social/ROI tracker.

**Why.** Knockout ties resolve via extra time + penalties, so "which team advances" is its
own 2-way market (the marquee knockout line) that the 90-min model can't price today.
Distinct from KNOCKOUT-PRICING (that's the defensive "keep ingesting 90-min lines" check;
this *adds* a new bettable market). **Not blocking:** the card still works in the knockouts
on 90-min 1X2 + totals — to-advance is additive value, not a prerequisite.

**What it touches:**
- `core/poisson_model.py` — `advance_probs(λ_H, λ_A)` = `P(win 90) + P(draw 90)·[P(win ET) +
  P(draw ET)·0.5]`; ET reuses `scoreline_grid` at λ scaled to 30 min; penalties ~50/50 (v1).
- **schema** — store 2-way to-advance odds (e.g. `home_advance_ml`/`away_advance_ml` on
  `soccer_wc_odds`) and an **"advanced"** result field on `soccer_wc_matches` (the 90-min
  score doesn't capture a penalty-shootout winner).
- `import_wc_odds.py` — parse the to-advance odds.
- `generate_wc_card.py` — add ADVANCE candidates (floor/cap guardrails apply).
- `update_wc_results.py` — grade ADVANCE picks against who advanced.

**Effort:** moderate, multi-file, ~a few hours. Buildable before Jun 28 if started this week.
**Decision needed:** build it (bet to-advance) vs. stick to 90-min markets only in knockouts.

---

## w-VALIDATION — calibration snapshot at group-stage end (BUG-005)

- **Type:** analysis task · **Status:** ✅ DONE (run 2026-06-28, full 72-pick group stage).
  **Result:** model well-calibrated in the meat (p 0.45–0.70 ≈ actual) but still over-rates dogs
  (0.30–0.45 bucket predicts 38%, delivers 28%; model-rated dogs above market won at 27.8% =
  market's 26.9%, not model's 36.7%). P&L contained (dogs +0.05u; blend +4.78u). Full discussion +
  the re-prioritization decision under **BUG-005 → Update 2026-06-28**.

**Why now-or-never.** The FIFA blend only does anything on strong-vs-weak FIFA-gap games,
which concentrate in the **group stage** and dry up in the knockouts (the field clusters,
gaps narrow, the blend goes quiet). So the `w`-tuning sample is effectively closed once the
group stage ends — we will **not** get an empirical `w` from this tournament; it stays a
principled prior. **Do the one measurement that's meaningful and time-boxed:** a
**calibration** check (not a unit backtest — too noisy on ~37 longshot games). Of the
group-stage games where the model rated a dog well above market (CIV-type), did they lose at
roughly the blended probability's implied rate? That validates the blend's *probabilities*,
which is variance-robust where P&L is not. `blend_impact.py` tracks pick-change deltas until
then.

---

## FEATURE-001 — Player-availability "what-if" (squad λ impact of an absence)

- **Type:** enhancement (not a bug) · **Status:** BACKLOG — **post-tournament**. Originally
  PROPOSED 2026-06-20, never built. Re-scoped to post-World-Cup 2026-07-08 during a QF backlog
  review — no specific injury/suspension case has forced the issue since Jun 19, and building
  a squad-availability model mid-knockout-stage carries more risk than value this late.
- **Prompted by:** the Jun 19 USA game — USA put up only ~1.08 xG (1.43 combined) with
  **Pulisic injured**, so the Over 2.5 loss and the low total partly reflect a missing
  creator the model never accounted for.

**The gap.** Team λ are computed from the **full squad pool** (`compute_wc_team_strength.py`),
assuming everyone is available. There is no concept of who is actually fit/selected for a
given match, so an injury or suspension to a key player is invisible to the card.

**Two builds (start small):**

| Option | What | Effort | New data? |
|---|---|---|---|
| **A — what-if diagnostic** (recommended first) | `whatif_player_out.py --team USA --out "Pulisic"`: exclude the player from `raw_team_strength`, re-derive *that one team's* raw attack/defense, re-apply the **same field normalization + FIFA blend** the rest of the field used, print the λ delta and (optionally) re-price the team's next match. | ~1hr | none — only a player name; per-player stats already stored |
| **B — availability baked into the card** | mark players out before card-gen so λ reflect the available XI. Makes λ **per-match** instead of per-tournament-version → touches the strength-storage model. | larger | needs an availability source (manual "out" flags easiest; API feed harder) |

**Caveat the tool must address — and the fix.** λ weight each player by *club minutes ×
position*, so any one player is only ~5–8% of a 23-man squad's weight. A naïve exclusion
therefore **understates** a marquee absence — the model has no notion that one creator is
disproportionately important to the *national* side. Mitigation (user's call, and sound
because exclusion is already a deliberate "this is a real loss" judgment): an **importance
multiplier** `M` (tunable, default > 1) that amplifies the excluded player's effect beyond
his raw squad-share — applied either to his contribution weight before removal or to the
resulting λ delta. You'd only ever exclude a player you consider a meaningful loss, so
over-counting their absence is acceptable and likely *more* realistic than the flat share.

**Recommendation.** Build **A with the multiplier** when wanted — cheap, no new pipeline,
immediately useful for pre-match "key player ruled out → fade/adjust?" calls. Defer **B**
until A shows absences move the needle enough to justify per-match λ.

---

## DESIGN-001 — Model runs on goals, not xG/xGA (deliberate v1 choice)

- **Status:** BY DESIGN for v1 (recorded 2026-06-14). **TODO:** add real xG (attack)
  and xGA (defense) when feasible. Not a bug — a known simplification.

**What.** No player row currently carries `xg_per90` or `club_xga_per90`
(0 of ~1,124). The strength model therefore runs entirely on the cruder proxies:
- **attack = club goals/90** (the per-player fallback in `compute_wc_team_strength.py`),
- **defense = club goals-against/90** (`club_ga_per90`).

**Why it's a choice, not a failure.** Both gaps are documented at the source:
- *xGA (defense):* TheStatsAPI exposes team **goals against** but **not xGA at team
  level**, so `club_xga_per90` is intentionally left null and the model falls back to
  `club_ga_per90` — see `import_wc_player_stats.py` (`club_defense`, module docstring).
- *xG (attack):* the season endpoint **doesn't expose expected goals** (xG exists only
  per-match), so `xg/xg_per90` are left null pending an **optional per-match xG pass**
  that has not been run; attack derives from goals/90 until then — see
  `import_wc_player_stats.py` (`fetch_player_line`).

**Why it matters (the cost of the proxy).** goals/90 rewards finishers and goals-
against/90 is blind to defensive *identity*, so finisher-heavy squads get inflated
attack and defense-first sides (e.g. Ecuador) get under-rated defense. This is the
same mechanism as **BUG-001** (club-concede ≠ national defense) and contributes to
big market disagreements on individual picks (e.g. CIV +255 read as a coin-flip).

**TODO (future).** (1) Run a per-match xG aggregation pass to populate `xg_per90`
for attack. (2) Source team-level xGA (different feed/scrape) to replace the
goals-against proxy for defense. Until then, treat large model-vs-market gaps that
hinge on attack/defense *level* with skepticism.

---

## BUG-005 — Club goals/90 over-credits the attack of talent-rich, mid-ranked teams

- **Severity:** medium · **Status:** PARTIALLY MITIGATED — FIFA blend w=0.2 (v7,
  2026-06-19) + targeted FIFA overrides for the worst cases (Egypt, CIV; v8,
  2026-06-21). Root metric issue (goals-not-xG) still open.
- **Discovered:** 2026-06-14, reviewing the Jun 14 card (Côte d'Ivoire vs Ecuador;
  model gave CIV win +78% EV — a +255 market dog read as a ~50% favorite).

**Update 2026-06-28 — group-stage calibration (w-VALIDATION) result + re-prioritization.**
Across all 72 graded group-stage picks the model is well-calibrated in the meat (p 0.45–0.70 ≈
actual) but **still over-rates dogs**: the 0.30–0.45 bucket predicts 38% / delivers 28%, and
model-rated dogs above market won at **27.8% — the market's 26.9%, NOT the model's 36.7%**. So
**w=0.2 did not fix the dog over-rating** (BUG-005 persists at the probability level). BUT the
**P&L is contained**: those dogs went **+0.05u (break-even)** and the blend added **+4.78u**
(`blend_impact.py`), vs the early-tournament −2.45u bleed — the floor/cap guardrails + blend
neutralized the damage even though the bias remains. Big-underdog MLs (+150+) finished 4–13 /
−1.00u; strip them and the model ran **+14.6% ROI** (vs +9.8% overall).
**Re-prioritization decision: do NOT pull a deeper fix forward to now.** The over-rating is a
strong-vs-weak-FIFA-gap effect that concentrates in the **group stage, which is now over**; the
knockout field has clustered, so the blend goes quiet and the leak largely disappears. With the
P&L already contained, a deeper dog-ML suppression (tighter BUG-003 cap / dog-specific blend /
**FEATURE-006**) is the **top model improvement for the NEXT group-stage cycle**, not a today task
— and re-tuning the engine right before a live knockout card adds risk for little remaining
group-stage upside.

**Symptom.** Teams whose squads are full of genuine club *scorers* but who are only
mid-tier *internationally* get an inflated `lambda_attack`. CIV is the standout:
the stats model ranks their attack ~2nd in the field (1.67, ≈Germany tier) while
they sit at **FIFA #34**. That produces λ_H 1.68 vs Ecuador (#23) 1.13 → model
P(CIV) ≈ 50% vs market-implied ~28%, a 22-point disagreement on a well-bet match.

**What it is NOT (levers already ruled out):**
- *Not weak-league inflation.* CIV's goals are overwhelmingly legit top-5: Bundesliga
  17 (Diomande 12), Ligue 1 10 (Wahi 7), LaLiga 8, Serie A 5, PL 4. Those carry
  league factor ≥0.85; the Pro League/Saudi chunks are already discounted hard by
  `ATTACK_LEAGUE_EXPONENT` (1.5). So **raising the BUG-002 league discount won't fix
  this** — the scoring is real top-flight output.
- *Not small-sample noise.* Empirical-Bayes shrinkage (`apply_shrinkage`, K=900 min)
  already pulls hot low-minute rates toward the positional prior (e.g. Fofana's
  3-in-295 is regressed).
- *Not a pipeline bug.* The aggregation is doing exactly what it's designed to.

**Root cause (structural).** The model treats club **goals/90 as if it transfers 1:1
to international scoring.** It doesn't: a national side's output against organized
international defenses is systematically lower than the minutes/position-weighted
average of its players' club rates (scoring is not additive across a squad, roles
change, opposition is tougher). Nothing calibrates a high club rate toward the team's
actual international standing. Two sub-causes feed it:
1. **goals, not xG** — hot finishing seasons aren't regressed to chances created
   (ties to **DESIGN-001**; the per-match xG pass is the metric-level fix).
2. **no club→international calibration** — the stats rating and the team's
   international results (FIFA/Elo) are never blended; the model trusts club output
   outright.

**Candidate fixes (discuss before building — see also the menu reasoning in chat):**
| Fix | Targets | Effort | Moves CIV? | Risk |
|---|---|---|---|---|
| **Measure first** — log model-xG vs ESPN-xG over several games | is it *really* high? | tiny | informs | none; delays |
| **xG pass** (DESIGN-001) — real xG/90 replaces/blends goals/90 | finishing variance | high | partially | won't fix transfer |
| **Blend attack toward FIFA/field-rank**: `λ = w·stats + (1−w)·rank_implied` | club→intl transfer | medium | **strongly, targeted** | over-blend → just re-predicts the ranking, kills value edges |
| **Compress spread** (lower `ATTACK_LAMBDA_SD`) | extreme ratings | one line | yes, but nerfs Germany/NL too | blunt, untargeted |
| **Targeted override** (CIV + similar AFCON sides), like `FIFA_OVERRIDES` | one match now | tiny | yes | band-aid, subjective |

**Recommendation.** The principled pair is the **FIFA/field-rank blend** (highest-
leverage, targets the actual cause) plus the **xG pass** (fixes the metric). But the
blend carries the system's central tension — blend too hard and the model just
re-predicts FIFA rank, destroying the value-vs-market edge that is the entire point.
So `w` must be tuned against **results**, not guessed, and **"measure first" gates any
engine change** — one match against a market we already distrust on totals is not
enough signal. **Interim handling:** for affected matches prefer totals (robust to the
attack/defense misallocation — see the CIV Over 1.5 sensitivity check) or skip the
moneyline; flag the team rather than trusting the raw edge.

**Targeted overrides shipped (2026-06-21) — Egypt + CIV pinned to FIFA rank (v8).**
The flat w=0.2 blend is too light for the worst mismatches, so the two clearest cases
were added to `FIFA_OVERRIDES` (100% FIFA, like the existing South Korea/Belgium pins):
- **CIV** — club scorers inflated attack to ~Germany tier (1.59) for a #34 side; override
  drops it to 1.24/1.46.
- **Egypt** — Salah's output diluted across an Egyptian-league squad left attack at 1.01
  (below baseline) for a #32 favorite; override lifts it to 1.29/1.41.
Effect verified on the Jun 21 NZ/Egypt card: Egypt 0.447→0.550 (vs market ~0.60), which
dropped NZ's win prob below the 0.25 floor so the model self-demoted the +57% NZ moneyline
and selected Under 2.25 — the correction surfaced the sound pick *through the model*, no
hand-override. Trade-off: an override discards ALL player signal for that team (a blunt rank
anchor); justified only when the aggregate is too contaminated to trust. `blend_impact.py`
now measures blend+overrides vs the pinned v6 baseline.

**Mitigation shipped (2026-06-19) — FIFA blend, `FIFA_BLEND_WEIGHT = 0.2`.** Stat-based
teams now blend `λ = 0.8·stats + 0.2·fifa_fallback(rank)` on BOTH attack and defense
(overrides/thin-coverage fallbacks already 100% FIFA, untouched). Both inputs are
WC_BASELINE-centered so the field mean stays at baseline — it only redistributes
strength. Surfaced on the Jun 19 card: model rated **Scotland (#33) attack 1.48 ABOVE
Morocco (#11) 1.41** — the inversion this bug predicts. Blend trims Scotland to 1.44 and
lifts Morocco to 1.47/def 1.21. Weight kept deliberately LOW: the opposite failure
exists too (club stats correctly capture Haaland-type talent FIFA rank lags — Norway #39
att 1.87, only trimmed to 1.72), and over-blending re-predicts the ranking and kills the
value edge. Persisted as v7 (`notes LIKE 'v7:%'`); v6 rows retained, revert = delete v7.
**Still open:** w=0.2 is a calibration nudge, not a cure — it trims EV on over-rated dogs
but does not remove them (Scotland still posted at +36% / 3★). Tune `w` against results
over the tournament; the goals→xG metric fix (DESIGN-001) is the orthogonal other half.

**Update 2026-07-08 — QF backlog review: Morocco validated, Norway investigated.**
Per-match `model proxy / actual / FIFA xG` comparison (`knockout_pick_review.py`) across
Morocco's and Norway's R32+R16 games:
```
Morocco   R32 (v Netherlands)  proxy 1.05  actual 1  FIFA xG 1.30  gap-actual +0.05  gap-FIFA -0.25
          R16 (v Canada)       proxy 1.47  actual 3  FIFA xG 0.85  gap-actual -1.53  gap-FIFA +0.62
Norway    R32 (v Côte d'Ivoire) proxy 1.58  actual 2  FIFA xG 2.30  gap-actual -0.42  gap-FIFA -0.72
          R16 (v Brazil)        proxy 1.19  actual 2  FIFA xG 1.41  gap-actual -0.81  gap-FIFA -0.22
```
**Morocco: no adjustment worthwhile (user call).** The blend's lift looks fine against FIFA xG
in R32 (-0.25 gap); the R16 scoreline gap is explained by the already-logged note above (the
0-3 final overstated the real chance-quality gap per FIFA xG and the user's own viewing) —
not a model problem. **Decision: leave as-is.**

**Norway: tested the "Haaland is watered down by teammates more than other stars" theory —
supported, but via a different channel than first stated (corrected twice, see below).**
User's hypothesis: Norway's proxy under-shoots because Haaland is a clear outlier surrounded
by a weaker "cluster" of teammates, dragging the team average down relative to other elite
scorers (Messi, Mbappé, Kane) whose teams have more depth near their level. Tested against
`compute_wc_team_strength.py`'s actual aggregation — a weighted average
`raw_attack = Σ(weight_i × rate_i × league_factor_i^1.5) / Σ(weight_i)`, weight_i = minutes ×
position-weight — using each star's post-shrinkage attack_rate:
```
                 star rate   numerator share   team raw_attack   others_avg (rest of squad)
Haaland (NOR)      0.736        42.4%              0.261              0.177
Mbappé  (FRA)      0.759        33.9%              0.291              0.221
Messi   (ARG)      0.695         8.2%              0.253              0.248
Kane    (ENG)      0.911         6.8%              0.286              0.273
```
(First pass mistakenly reported a "weight share" of 15%/13%/3%/2.1% — an invalid ratio, a
league-factor-adjusted numerator-style quantity divided by an un-adjusted weight denominator
that don't actually correspond to the same terms in the formula. **`numerator share` above is
the corrected, meaningful figure**: exactly what fraction of the team's final goal-proxy value
is attributable to that player's own goals, since both share the same denominator.)

**Correct reading:** Haaland is NOT diluted in a credit-share sense — at 42.4% he carries the
*largest* share of any of the four stars, well above Mbappé's 33.9% and far above Messi's/
Kane's (8.2%/6.8%, mostly because those two rows are partial-season club samples — Messi 1243
MLS minutes, Kane only 537 minutes labeled "DFB Pokal" rather than his full Bundesliga season,
likely a real TheStatsAPI coverage gap worth re-pulling if this is revisited). And yet Norway's
team number (0.261) still sits below France's (0.291) despite Haaland's bigger share — which
is only possible because Norway's **others_avg (0.177) is the lowest of the four**, well below
France's 0.221 and further still below Argentina's/England's 0.248/0.273. So the mechanism is
exactly the "no second/third player close to Haaland" effect the user described — it just
shows up as a weak supporting-cast average dragging the linear blend down, not as Haaland's own
number being proportionally diluted (he's actually over-carrying, not under-credited). Real,
structural property of a flat weighted-average aggregation (any team with one clear outlier and
thin depth behind him hits this ceiling) — not noise. **Still no fix queued** — consistent with
this tournament's standing discipline against retuning the engine mid-knockout-stage on a
single-team finding; revisit (e.g. a depth-weighted or top-N aggregation instead of a flat
squad average) only if a future tournament shows the same pattern across multiple thin-depth,
one-star teams.

**Blend impact tracker — is w=0.2 earning its keep?** `python blend_impact.py` re-derives
the pick the v6 (pre-blend) strengths WOULD have made and compares to the v7 pick on every
graded match from 2026-06-19 on, attributing the unit delta only where the blend *changed*
the pick (the only place it can help/hurt). Judge `w` on this cumulative delta, not one card.

| Slate | Game (only blend-changed) | v6 pick → result | v7 pick → result | Δ units |
|---|---|---|---|---|
| Jun 19 | USA v Australia (2-0) | HOME ✓ (+0.74) | OVER 2.5 ✗ (−1.00) | **−1.74** |
| | **cumulative** | | | **−1.74u (1 graded)** |

Early and noisy (1 game): the blend's first swing backfired — it flipped a winning USA
moneyline to a losing Over 2.5. This is exactly the Haaland-side cost flagged at design time
(blend cools a strong favorite toward market). Do NOT move `w` off one slate; let the table grow.

---

## BUG-003 — EV on big-longshot moneylines is unreliable (noise amplification)

- **Severity:** medium · **Status:** MITIGATED 2026-06-17 (floor + cap shipped);
  the underlying dog over-rating is still open (BUG-005 / DESIGN-001 territory)
- **Symptom.** A small absolute probability error on a big underdog produces a
  large EV%, so longshot moneylines keep topping the card on fake edges.
- **Evidence.** Qatar +1100: model p 0.119 vs market 0.083 — a **3.6-point**
  edge — yields +42.7% EV. At +1100, model p of 0.10 already gives +20% EV; the
  model cannot reliably distinguish 8% from 12% on a dead-last team. Same trap as
  Mexico/South Africa +700 (model 0.187 vs 0.125 → +49.8%, which then LOST).
- **Proposed fix (card-level, low risk).** A selection guardrail: don't take a
  moneyline pick below a model-probability floor (~0.15), or require a minimum
  *absolute* edge (model_p − implied_p ≥ ~0.04), not just EV%. Demote to the
  next-best qualifying side (no abstention). Does not touch the lambdas.

**Update 2026-06-17 — noise amplification has been the book's single biggest leak
(~1 week in).** Through 19 graded picks (Jun 11–16), big-underdog moneylines (+150
or longer) are **1W-5L for −2.45u** — CIV +255 the lone winner; Korea +180, Bosnia
+360, Netherlands/Japan +255, Haiti +475 and Jordan +725 all lost. Strip them out
and the rest of the model is **+3.16u** (vs +0.71u overall): the longshots alone
turn a clearly-profitable model into a coin flip. The shipped guardrail (uniform
`MIN_PICK_PROBABILITY = 0.25`) does NOT catch them — it only demotes picks the model
itself rates sub-floor, whereas these are dogs the model *confidently over-rates*
(Jordan: model p 0.353 vs market 0.121, ~3×, +191% "EV", cleared the 0.25 floor,
Austria won 3-1). The error is directional (model under-rates the favorite → over-
rates the dog), so the fix is a **market-disagreement check on top of** the floor,
not a replacement for it.

**Update 2026-06-17 (cont.) — cap shipped.** Added `MAX_UNDERDOG_MARKET_DISAGREEMENT
= 2.0`: a candidate is demoted when its model prob is >= 2x the market's implied prob
(only an underdog can trip it — a favorite can't be 2x its own high implied). A
K-sweep over the 20 graded picks showed the *ratio* does **not** cleanly separate
winners from losers (losers span 1.13x-2.91x, and the lone ML winner CIV +255 sits
at 1.78x, mid-pack), so K is set on **principle** ("twice the market's price"), not
fit — and the cap is justified as a **soundness filter** (don't *post* a pick whose
EV rests on a probability the model can't support), **not** by sample P&L (it adds
~0 on the sample, since the demoted matches were largely unwinnable by +EV logic
anyway). At K=2.0 it fires only on the egregious case (Jordan 2.91x) with **zero
false positives**; Bosnia (1.86x), Haiti (1.58x), CIV (1.78x) and ordinary
near-market dogs (Korea 1.19x, NL/Japan 1.13x) are all left alone. Implemented as a
pure `select_pick()` in `generate_wc_card.py`: floor and cap are evaluated
**independently** on every candidate (each exclusion logs which check(s) fired, with
the math), a pick must clear **both**, and when nothing clears both the fallback is
the **highest-model-probability** side (not highest EV — that would hand it back to
the longshot). A `GUARDRAIL LOG` section prints every demotion. This is *mitigation*;
the root over-rating of dogs is unchanged (BUG-005 / DESIGN-001).

**Update 2026-06-29 — the 2× cap is structurally the WRONG guardrail for the to-advance market.**
Two knockout days running, a dog **to-advance** mirage slipped the `MAX_UNDERDOG_MARKET_DISAGREEMENT
= 2.0` cap that *would* have caught the same team's 90-min win pick:
- **Paraguay to advance (Jun 29):** model 0.377 vs market 0.190 = **1.98×** (cleared) — yet the
  *absolute* edge is **+18.7 pts**, LARGER than Paraguay's 90-min WIN (model 0.269 vs 0.121 =
  **2.22×**, correctly capped, +14.8 pts). The card posted Paraguay-advance at **3★**.
- **South Africa to advance (Jun 28):** model 0.346 vs market 0.278 = **1.24×** (cleared), lost.
**Root cause:** advance probabilities compress toward 0.5 (the draw→ET→PK path inflates the dog),
so the *ratio* shrinks while the *absolute* over-rating stays large — the multiplicative cap can't
see it. **SHIPPED 2026-06-29 — `MAX_ADVANCE_ABSOLUTE_DISAGREEMENT = 0.07` in `generate_wc_card.py`.**
An **absolute-points** guardrail on underdog ADVANCE candidates (market implied < 0.5): demote when
`model_prob − market_implied ≥ 0.07`, reusing the existing BUG-003 `excluded_by` / demoted-log
machinery (logged like the floor/cap). Threshold set to **0.07** (user chose the aggressive end) to
catch both knockout mirages — Paraguay (+18.7 pts) and yesterday's SA (+6.8 pts) — on the principle
that the club-stats inputs make mismatches look closer than they are, so a dog's *advance* number is
inflated past any realistic edge. Verified live on the Jun 29 card: Paraguay-advance (model 0.375 vs
market 0.190) demoted → Germany/Paraguay self-resolves to Under 2.5 (1★), no hand-override. Tests:
`test_select_pick_advance_edge_*` (demotes over-rated dog, keeps small-gap dog, ignores favorite,
advance-only). Only targets the **underdog** side (a favorite's model advance prob sits *below*
market here, so it never trips). FEATURE-009's step-1 bar still supersedes this later as the general
fix; this is the durable interim.

**First live result (2026-06-29, n=1) — demoted a WINNER.** The suppressed Paraguay-to-advance
(+425) *did* advance (1-1, won pens 4-3), so it would have returned +4.25u; the model instead
recorded Under 2.5 (+1.15 win), scoreboard unhurt but upside forgone. Expected soundness-filter
behaviour (trade occasional upside to not post unsupportable picks — cf. SA-advance Jun 28, posted
and LOST). Tally so far: 1 suppressed winner (Paraguay) vs 1 posted loser (SA, pre-guardrail) — a
wash at n=2; keep watching, do not retune. NOTE: suppressed advance picks are only in the card-gen
log, not stored with a result, so this tally is **manual** — auto-grading demoted picks is a small
follow-up if we want the guardrail's net effect measured automatically.

---

## BUG-004 — Over-skew: model expects more total goals than the market (LEVEL bias)

- **Severity:** medium · **Status:** FIXED 2026-07-05 (knockout-scoped correction; group
  stage was never actually biased once the two stages were compared directly — see below)
- **Symptom.** Across all 72 games, OVER has avg EV **+6.8%** (+EV in 48/72) vs
  UNDER **−14.4%** (+EV in only 17/72). The model's expected total goals sits
  above the market's on essentially every game — a *level* bias, distinct from
  any team being mis-rated. (Draws are well-calibrated: model avg p 0.226 vs
  market 0.230; they just rarely win the one-best-pick race.)
- **Important:** this is a LEVEL issue (the whole slate's goal expectation), NOT
  the BUG-002 distribution issue — the two were originally conflated. The BUG-002
  fix (attack exponent + spread normalization) is a pure redistribution and left
  the over-skew unchanged (+6.8% → +7.0%), confirming they are separate.
- **It may be a real edge, not an error.** Model ~2.7 goals/game vs market ~2.5
  could mean the model correctly sees WC group games as higher-scoring. Matchday-1
  Overs went 1-1 — far too few to tell.
- **Do NOT fix yet.** Let results accumulate; only adjust the baseline / totals
  level once results show the higher goal expectation is wrong, not right.
  Candidate lever if so: lower WC_BASELINE or a totals-specific level shift.
- **Update 2026-06-15 — dispersion checked and ruled out as the lever.** Tested
  whether the totals miss is a *spread* problem (model under-dispersing →
  under-calling blowouts) fixable via `ATTACK_LAMBDA_SD`. It is not: SD of the
  model's per-game totals = **0.40** vs SD of the market's O/U lines = **0.41**
  (72 games) — the model is *not* under-dispersed, so widening the dial would
  over-disperse. `ATTACK_LAMBDA_SD` confirmed a pure *dispersion* knob (slate-avg
  total moves only +0.01 going 0.41→0.55), orthogonal to the *level*
  (`WC_BASELINE`). The model-minus-market gap is a LEVEL effect concentrated on
  low-total games (+0.64 at line 2.0, +0.33 at 2.5, ~0 at 3.0+), and Spain/Cape
  Verde — the elite-attack-vs-leaky-defense corner we'd have widened *for* — went
  0-0. **Decision: leave `ATTACK_LAMBDA_SD` at 0.41.** Any future totals fix
  targets the LEVEL, pending results to confirm the over-skew is error vs edge.

**Update 2026-07-03 — REOPENED. Direct model-vs-actual check (not EV-vs-market) shows the
over-skew has resurfaced sharply in the KNOCKOUT stage, with a much cleaner signal than the
group stage ever had.** Prompted by the Jul 3 Colombia/Ghana card: their calibration gaps
looked like a Colombia/Ghana-specific problem at first (`proxy_goals_calibration.py` /
`proxy_defense_calibration.py`, both teams badly over-projected on attack AND defense), but a
compounding-error check showed the SAME pattern in all 6 of Colombia's/Ghana's group games
(wins and the one loss alike) — meaning it isn't specific to these two teams. That prompted a
new tool, `totals_calibration.py` (projected total λ_H+λ_A vs actual regulation total, by
stage):
```
  Group    72 matches · mean signed gap -0.21 · mean |gap| 1.51 · over 34 / under 29 / flat 9
  R32      13 matches · mean signed gap +0.43 · mean |gap| 0.68 · over 7 / under 1 / flat 5
```
Group stage (using fully-revised current lambdas) is noisy and roughly balanced. **R32 is
different in kind, not just degree**: tighter errors (|gap| less than half the group stage's)
with a decisive lean — **7 over-projected vs. 1 under-projected**. That's a systematic level
shift, not scatter. Consistent with the knockout field being a stronger, more defensively
organized set of teams than the model's shared baseline currently assumes.
**Still only 13 games — real but not final evidence, same discipline as every other change
today.** Candidate lever (per the original 2026-06-13 note, still valid): **lower
`WC_BASELINE`** — in the current normalization, decreasing baseline shifts every team's attack
lambda down additively (the defense/baseline ratio in `analyse_match_wc` is invariant to
baseline, since defense values are constructed as `raw · baseline/mean_raw`), so this is a
clean, single-parameter lever for the LEVEL, not a per-team patch. **Not applied yet** — this
is a reopened watch item pending more knockout results, the same way the original bug waited
for group-stage results before either fixing or dismissing it.

**Update 2026-07-05 — FIXED, knockout-scoped, `WC_BASELINE` ruled out.** Round of 32 finished
(16/16 graded); rerunning `totals_calibration.py` on the complete set sharpened the signal
further, and — critically — showed the two stages need corrections in OPPOSITE directions:
```
  Group  72 matches · mean proj 2.776 · mean actual 2.986 · ratio 1.076 (model UNDER-projects ~7.6%)
  R32    16 matches · mean proj 2.901 · mean actual 2.375 · ratio 0.819 (model OVER-projects ~18.1%)
```
This rules out the originally-proposed `WC_BASELINE` lever outright: a single global constant
cannot fix a bias that runs one way in Group and the opposite way in R32. Built a new tool,
`knockout_baseline_backtest.py`, to size and validate a **stage-scoped** multiplier instead —
applied only when `stage != "Group"`, so it can't retroactively disturb group-stage calibration
(moot in any case: the group stage is finished, no more games to price). Swept candidate scales
against the 16 R32 games, re-running FEATURE-009's locked selection (0.60/0.60) on each:
```
 SCALE  SIGNED GAP   MAE   W  L  P   UNITS
 1.000       +0.53  0.73  12  4  0   +2.92   (today's baseline, no correction)
 0.900       +0.24  0.65  12  4  0   +2.92
 0.850       +0.09  0.62  14  2  0   +5.22   <- shipped
 0.819       +0.00  0.62  14  2  0   +5.77   (exact calibration-zeroing value)
 0.800       -0.05  0.62  14  2  0   +5.77
 0.750       -0.20  0.62  12  3  1   +4.99   (overcorrects)
```
0.819 — derived purely from the calibration ratio above, with zero reference to betting
outcomes — landed at the units-optimal point in the sweep, which is a good sign this isn't
overfit to 16 games. **Shipped the more conservative 0.85** (user's choice) rather than the
exact value: nearly all of the improvement (14-2-0, +5.22u vs. 1.00's 12-4-0, +2.92u) with a
smaller departure from baseline. Per-game, the correction is genuinely mixed, not a clean
sweep — worth remembering when reviewing future results: 3 picks flipped loss→win
(Germany/Paraguay, Netherlands/Morocco, USA/Bosnia — all cases of an inflated
OVER/HOME-favorite confidence the correction properly discounted), 1 flipped win→loss
(Belgium/Senegal — a real 4-goal knockout game the correction now underrates).

**Implementation.** `core/wc_knockout_scale.py` (new, single source of truth):
`KNOCKOUT_GOAL_SCALE = 0.85`, `knockout_goal_scale(stage)` returns 1.0 for Group else the
scale. Multiplies into the SAME `home_advantage`/`away_advantage` slot `host_advantage()`
(BUG-006) already occupies in `analyse_match_wc` — no change to `core/poisson_model.py`,
`WC_BASELINE`, or any team-strength data. Because it scales both teams' λ directly, it affects
moneyline and to-advance pricing too, not only totals. Wired into every file that already
composed `host_advantage()` (same 9 consumers as BUG-006, plus the new
`knockout_baseline_backtest.py`). Tests: `tests/test_wc_knockout_scale.py`,
`tests/test_generate_wc_card.py::test_knockout_stage_scales_projected_lambda_down`.

**Applied starting 2026-07-05** (R16: Canada/Morocco, Paraguay/France) — no retroactive
regrading of already-settled R32 picks (locked history, same convention as every other fix
this tournament).

**Update 2026-07-08 — QF backlog review; leave the 0.85 scale as-is.** Raised as a live risk
for the Spain/Belgium QF: Belgium is the one named case (Belgium/Senegal, R32) where the flat
0.85 knockout scale underrated a genuine high-scoring game, and Belgium has since put up 4
goals against USA in R16 too — a plausible pattern, not just one data point. **Decision (user):
do not special-case Belgium mid-tournament** — leave `KNOCKOUT_GOAL_SCALE` at 0.85 for the
Spain/Belgium QF and re-evaluate whether high-scoring teams need per-team handling only after
the tournament completes (same one-shot-fix discipline as every other engine change this
tournament — one team's two data points isn't enough to justify a targeted correction now).

## BUG-002 — Weak-league forwards inflate attack lambda

- **Severity:** medium-high · **Status:** FIXED 2026-06-13 (v5)
- **Symptom.** Teams whose forwards play in weak leagues got an over-rated attack
  even after the league-factor discount (goals/90 in a soft league overstates
  international scoring more than the single factor captures) — over-optimistic
  underdog ML picks (Czechia, Bosnia, Haiti rated well above their FIFA-field).
- **Evidence.** Matchday 1: Czechia ML (lost, Korea won 2-1) and Bosnia ML (1-1
  draw) were both weak-league-attack-inflated underdogs that failed to win.
- **Fix shipped (v5).** Attack uses `league_factor ** ATTACK_LEAGUE_EXPONENT`
  (1.5) so weak-league scoring is discounted harder while top leagues (factor
  1.0) are untouched; paired with **mean-AND-spread** attack normalization
  (`ATTACK_LAMBDA_SD`) so the discount can't blow out the spread / re-inflate the
  elites. Full 48-team table re-validated: Czechia/Bosnia/Haiti down, elites held,
  Brazil ~2.3. Exponent 1.5 (not 2.0) chosen to spare mid-tier leagues (Liga MX).
- **Note:** this fix is a *redistribution*; it does NOT address the Over-skew
  (that's the separate BUG-004, a level bias).

---

## BUG-001 — Goalkeeper club-concede rate is a poor proxy for team defense

- **Severity:** medium
- **Status:** PARTIALLY FIXED 2026-06-15 (league markup softened — see update
  below); the core proxy issue remains OPEN
- **Discovered:** 2026-06-11, reviewing the Jun 12 card (Canada vs Bosnia &
  Herzegovina; model gave Bosnia ML +86% EV, an obvious stretch).

**Symptom.** Teams whose goalkeepers/defenders play for weak-league or
defensively-poor clubs get an inflated `lambda_defense` (look far leakier than
they are). Canada is the standout: model rates them ~38% to beat Bosnia at home
vs a market-implied ~56%.

**Two compounding root causes:**

1. **Wrong keeper by minutes.** Defense aggregation weights keepers by *club*
   minutes. Canada's most-played club keeper is Owen Goodman (Barnsley, League
   One — 3rd-tier, factor 0.50), so a backup dominates the GK signal. Their
   *actual* starter (confirmed via web search) is **Maxime Crépeau**. Minutes ≠
   national-team role, and for keepers exactly one plays.
2. **Club-concede ≠ keeper/national quality (the deeper issue).** A keeper's
   club-concede rate measures his *club's* defense, not his ability or his
   national side's. Crépeau's club (Orlando City) had a record-bad 2025 defense
   (~72 conceded, ~2.1/game). Even with the *correct* starter and *correct* data,
   Canada's defense barely improves (1.76 → 1.63) and Bosnia ML stays +72% EV —
   because a busy keeper behind a bad club back line is not a bad keeper. The
   league-factor *division* amplifies it further (2.1 ÷ 0.62 MLS = 3.4).

**Why deferring is safe.** Verified that "fixing the data" (Crépeau as starter at
his real Orlando rate) does NOT rescue the pick — so this is not a quick data
patch. It needs a design change, not a hotfix.

**Proposed fix (for a future session).** A per-team **keeper-quality override**:
web-search each team's starting keeper and assign a quality tier (elite / good /
average / weak) from reputation + recent international form, and use *that* for
the GK contribution instead of the club-concede proxy. This is the keeper analog
of the existing team-level `FIFA_OVERRIDES`. ~48 manual judgments; reliable.
Smaller alternative to evaluate first: a gentler league adjustment for **defense**
(the ÷ league_factor over-amplifies high concede rates in weak leagues — affects
keepers *and* defenders, one formula change, but needs whole-table re-validation).

**Update 2026-06-15 — shipped the gentle league adjustment (partial fix).** Added
`DEFENSE_LEAGUE_EXPONENT = 0.5`: club concede rates are now marked up by
`÷ league_factor**0.5` instead of the full `÷ league_factor`, applying only part
of the markup. Diagnosed first (the division most distorts teams with weak
defensive leagues, factor 0.50–0.68), then persisted as **v6**. Effect — pulls in
the over-rated-leaky tail while leaving strong-league defenses and all attacks
untouched (field mean stays at baseline; pure redistribution): Qatar 2.19→1.82,
New Zealand 1.82→1.65, Jordan 1.80→1.55, Iraq 1.74→1.51, Canada 1.76→1.65. This
reproduces, across the whole field, the ~1.65 endpoint the manual Canada data-fix
reached. **Still OPEN:** the deeper issues — club-concede ≠ national defense, and
wrong-keeper-by-minutes — are untouched, so the **keeper-quality override (option
A)** remains the follow-up for the residual distortion.

**Update 2026-06-28 — bucket scoped; FIFA anchor + global blend both tested and REJECTED.**
The first live knockout (South Africa/Canada R32) surfaced BUG-001: Canada's λ_def **1.60**
inflated South Africa to **0.346 to advance** (vs market 0.278). Findings:
- **Bucket (who else is affected):** decent national teams whose players are in weaker leagues, so
  club-concede over-states national leakiness — **Canada (1.60), Australia (1.44), Mexico (1.35),
  USA (1.32 borderline)**. Genuinely weak teams (Ghana, Bosnia, Cape Verde) are *correctly* leaky
  (FIFA agrees) — not in the bucket.
- **FIFA anchor is insufficient.** Pinning Canada to FIFA #31 only yields def **1.39** (still
  leaky); SA-advance drops 0.41→0.37, nowhere near market 0.278. Canada's FIFA rank is itself too
  low to capture how good the market/reality rates it.
- **Global split-weight blend tested & REJECTED.** A defense-heavy blend (w_attack 0.2, w_defense
  0.5–0.6) *improves* goal-MAE — defense fit improves monotonically toward pure-FIFA, **confirming
  the defense proxy is the broken part** — BUT **destroys betting P&L: −11.6u (w_d=0.5) / −12.1u
  (w_d=0.6)** over the 72 graded group games (re-priced; baseline +6.44u → −5.15u). It flips
  longshot-value *winners* (Ecuador +360, Türkiye +245, Saudi/Uruguay draw +340) to chalk. A
  textbook **results-vs-fit divergence** — the better-fitting model loses money; do NOT ship on
  goal-MAE.
- **Conclusion:** the fix must be **TARGETED, not global** — per-team **keeper-quality tiers** (the
  documented proper fix) or **manual value overrides** for the ~4 bucket teams — which repair the
  broken defenses *without* globally shifting all defenses and killing the longshot value carrying
  the P&L. Interim: handle **case-by-case** (pass / override per game), as done for SA/Canada
  (passed the to-advance; took the push-protected Over instead).

**Update 2026-06-30 — Mexico pinned to FIFA #17 (v10), results-driven.** Group form (3 clean
sheets, incl. vs a decent South Korea) confirmed the club-concede-derived defense (blend 1.354,
≈field-avg) was too leaky vs FIFA pedigree (1.133); user pinned Mexico to FIFA. Effect on
Mexico/Ecuador R32: def 1.354→1.133 suppresses Ecuador 1.30→1.08, so Mexico-advance *rises*
0.666→0.690 (the leaky-D bug had been masking, not inflating, Mexico's edge) and total eases
3.31→2.95. **Known imperfection (documented in the override reason):** the full pin also drops the
attack 1.69→1.57, which the group goals (6 in 3) do NOT support — a **defense-only / per-component
blend** is the cleaner fix (FEATURE candidate; the global split-weight version was already rejected
on P&L, so per-team is the targeted path). Revisit Mexico's pin when per-component weights exist.

**Related / also worth a look:**
- The `matches_played` denominator is unreliable from TheStatsAPI for some clubs
  (e.g. Orlando stored as ga=44/mp=15 → 2.93 vs real ~2.1). `fix_wc_club_defense.py`
  can't always repair it (needs a squad player with full-season club minutes).
- Host **defensive** advantage is not modeled — hosts get an attack boost
  (`HOST_HOME_ADVANTAGE`) but no defensive edge at home. Adding opponent-attack
  suppression for host home games is a clean general add (helps modestly).

**Watch item 2026-07-06 — CLOSED (Paraguay eliminated, R16).** Paraguay's disruptive/
physical style may have been underrated by club-stat defense proxies (same root cause
as this bug, different symptom): user's eye-test on two straight results, Germany
(R32, lost) and France (R16, a 0-1 "slog"), had BOTH visibly slowed down by Paraguay's
defensive, physical approach — a *tactical* effect a club-aggregated `club_ga_per90`
proxy has no way to see (it measures average defensive output across a squad's club
seasons, not a specific national-team game-plan). Paraguay lost the France game and is
now eliminated, so this stays a 2-data-point anecdote with no further evidence coming
this tournament — not actionable, but the underlying blind spot (proxy can't see
game-plan-driven defense) is real and may resurface with a different team.

**Note 2026-07-06 — Canada/Morocco (R16) scoreline overstated the actual gap.** Final
was 0-3, but user's own viewing had it roughly even on chances (xG 0.84 Canada vs 0.82
Morocco) until Morocco took over in the second half. The model's pick that day (Morocco
to advance, 59.9% model prob) was NOT a lopsided call — it read this correctly as
close to a coin flip, and the blowout scoreline is second-half variance/execution, not
a model miss. Recorded so a future calibration pass doesn't misread this result as
"model badly underrated Morocco" from the scoreline alone.

**Note 2026-07-06 — Paraguay/France (R16) scoreline UNDERSOLD the actual gap (opposite
direction from Canada/Morocco, same day).** Final was 0-1, but external xG had France
1.45 vs Paraguay 0.13 — a near-total territorial/chance-quality mismatch that the tight
scoreline completely hides. The Over 2.5 pick's underlying thesis (France would
eventually create enough to break the game open) was well-supported by the actual run
of play; the loss was a finishing/variance issue (France just didn't convert), not a
sign the model's/market's read of the game was wrong. Two games, same day, opposite
score-vs-process divergence — a clean illustration of why single-game results (and
even the model's own goals-based proxy, not true xG — see DESIGN-001) can diverge
sharply from the underlying quality of play in either direction.

---

## FEATURE-019 — Season-start kickoff sequence (league-membership sync, squad refresh, name-map/odds checks), run once per season — **SHIPPED 2026-08-19**

- **Type:** enhancement / tooling · **Status:** SHIPPED 2026-08-19. New
  `season_kickoff.py`, run once per season across all 10 tracked divisions.

**Problem.** A team's row in `soccer_teams` carries a `league` column meant to answer
"what league is this team in right now" — but nothing in the codebase ever updates it
after the team is first created.  So if/when a team is relegated/promoted the value in 
the league column gets out of date.  There is other code that consumes the league and 
expects it to be correct, for example - `import_league_betting_odds.py`'s `load_team_map()` filters
`soccer_teams WHERE league = ?`, so a promoted team silently becomes invisible to odds
matching, every season, for as long as this project runs.

**Two independent pipelines, two different bugs — both in scope.**
1. The 8 TheStatsAPI-sourced divisions (Premier League/Bundesliga/La Liga/Ligue 1 +
   their feeders) go through `core.sports_db.ensure_soccer_team()` — the stale-label
   bug described above. Silent, not loud: a promotion just quietly stops updating.
2. Serie A/Serie B go through `update_serie_a_results.py`'s OWN, separate
   `ensure_team()`/`load_team_map()` (does not call `core.sports_db.ensure_soccer_team`
   at all). Worse failure mode: `load_team_map()` only ever loads teams matching
   `WHERE league = 'Serie A'` once at start, cached in a plain dict; a team promoted
   FROM Serie B that already exists in `soccer_teams` (just under the wrong league)
   won't be in that cache, so `ensure_team()` will attempt a fresh INSERT with the
   same `name` — which `soccer_teams.name`'s UNIQUE constraint will reject with an
   uncaught `sqlite3.IntegrityError`, crashing the whole sync. (Not hit today only
   because the specific promoted team we pulled, Frosinone Calcio, happened to be new
   to our tracking rather than a previously-known relegated team — this is a
   real, live landmine for a future season, not already proven safe.)

**Proposed shape.** A new script, run once at the start of each season, doing six
things per division, in order:

1. **Check the new season is actually available.** Some leagues publish their
   schedule later than others — Bundesliga's 2026-27 season doesn't start until
   Aug 28, over a week after the others. Because of that, the script needs to be
   able to be run multiple times and only add new information (like a new league)
   that has published its schedule.

2. **Update any team that changed leagues.** For every team in the new season,
   compare it to what league we currently have on file for them. If it changed,
   that's a promotion or relegation — log it plainly and update our record; if it's
   a team we've never seen before, just add it.

3. **Pull in the full season's match schedule**, using the import scripts that already
   do this — no new fixture-pulling logic needed here.

4. **Refresh every team's roster.** Transfer windows close right around this time,
   so last season's rosters are stale. The model needs current rosters to make good
   picks, so re-pull them once this season's teams and matches are in place.

5. **List any teams whose odds data didn't match.** Every season, some newly
   promoted teams show up under a slightly different name in the odds data (e.g.
   "Inter Milan" instead of our "Inter"), so their odds silently fail to import.
   This step doesn't fix that automatically — it just logs a clear list of who
   didn't match, so someone can fix the names on purpose instead of discovering the
   gap by accident later.

6. **Confirm each league's odds source is actually live**, not just configured
   correctly, before assuming odds import will work. Cheap check, and it would have
   flagged Bundesliga's not-yet-started season from a second angle.

**What this deliberately does NOT do:** it doesn't change any other code's behavior
(the fix stays isolated to this one script); it doesn't fix the name-mismatch gaps
found in step 5, only lists them; and it doesn't add a new "seasons" table to the
database — season stays a plain year number.

**New automatic check to add:** compare every team's stored league against the
league of their own most recent match, and flag any mismatch. This is a simple
standing alarm — it would have caught today's Hull City-style problem immediately,
and keeps catching similar mistakes in the future even if this new script has a
gap somewhere. Runs against the real database, opt-in, same pattern as the
project's existing data-quality checks. A brand-new team with no matches yet isn't
treated as broken.

**Decisions (resolved 2026-08-19, ready for implementation):**

1. **Serie A/B's separate, older team-tracking code gets fixed too** — rewired onto
   the same safe, shared pattern as the other 8 divisions, removing the live crash
   risk (not left as a known risk for next season).
2. **Resolved: use TheStatsAPI's dedicated `teams` endpoint**
   (`client.paginate("teams", {"competition_id": ..., "season_id": ...})`), not the
   match schedule. This endpoint already exists and is already used by two other
   scripts in this codebase (`import_club_squads.py`, `import_club_player_stats.py`)
   — not a new integration. Verified live for Premier League 2026-27: returns all 20
   teams correctly, Hull City and Coventry City included. More direct and
   authoritative than inferring team membership from fixtures, at the same cost (one
   extra API call per division, already an established, cheap pattern here).
3. **A team that drops out of everything we track gets an explicit sentinel value**
   — `league = "(unknown - not seen this season)"` — instead of a silently stale
   real league name. Two implementation notes that follow from this: (a) this can
   only be determined after ALL ten divisions are processed for the season (a team
   missing from Serie A alone isn't "unknown" until it's also missing from Serie B,
   Championship, etc.) — one extra pass at the end, not a per-division check; (b) any
   other code that looks up `soccer_teams.league` in `core/leagues.py`'s registry
   needs to tolerate this value without erroring — to be verified during
   implementation, not assumed safe.
4. **First-run cleanup confirmed as normal behavior** — the five already-mislabeled
   teams in the live DB get fixed automatically the first time this script runs, no
   separate one-off migration needed.
>>> YES
**Shipped 2026-08-19.** New `season_kickoff.py`, implementing all six steps against
`core.leagues.LEAGUES`' 10 tracked divisions:

- **Decision 1 done:** `update_serie_a_results.py`'s own separate `ensure_team()`/
  `load_team_map()` now delegates to `core.sports_db.ensure_soccer_team()` instead of
  a raw INSERT, removing the live crash risk (verified directly: a team that already
  exists under a different, stale league no longer raises `sqlite3.IntegrityError`).
  `load_team_map()` also dropped its `WHERE league = 'Serie A'` filter, since `name`
  is already the real unique key.
- **Decision 2 done:** team lists come from TheStatsAPI's `teams` endpoint (8
  divisions) or football-data.org's `/teams` endpoint (Serie A only) — not derived
  from fixtures.
- **Decision 3 done:** a team not seen in any of the 10 divisions this run gets
  `league = "(unknown - not seen this season)"`, applied only after all divisions are
  processed, and only to teams whose *current* label belongs to a division that was
  actually checked this run (a skipped division's teams are left untouched, not
  incorrectly marked unknown).
- **New data-integrity test** (`tests/test_data_integrity.py`,
  `test_team_league_matches_their_most_recent_match`): compares every team's stored
  league against their most recent match. One real refinement found while building
  it — a division with zero matches in the *current* season yet (e.g. Serie B, since
  `import_league_matches.py` only imports finished matches for divisions without an
  odds source, and no 2026-27 Serie B match has been played yet) will always look
  "stale" by this check even when correctly labeled, since there's no fresher match to
  compare against. Exempted: only leagues with at least one current-season match get
  the strict check; the rest are skipped as "can't validate yet," not "known good."

**Verified end-to-end against the real database, not just tests.** First run
surfaced far more staleness than the 5 teams found by hand while pulling fixtures —
**22 teams**, including relegated teams stale in the *other* direction too (Sampdoria,
Southampton, etc. still showing their old top-flight label). A second run (full
pipeline, fixtures included) fixed the rest and left **~40 additional teams**
correctly marked with the new sentinel — mostly teams from 2022-2025 seasons that
have since dropped below the two tiers this project tracks per country, a real
first-time "true-up" given nothing has ever reconciled these labels before. The new
data-integrity test passes cleanly on the resulting database.

**Not done in this pass, left for a follow-up run:** step 4 (squad refresh) was run
with `--skip-squads` during testing to keep iteration fast; run separately right
after, surfacing one more real bug worth recording here.

**2026-08-19, same day: squad refresh run for real, found and fixed a second
recurring problem.** `import_club_squads.py` resolves its TheStatsAPI competition by
a bare name search when `--competition-id` isn't passed — genuinely ambiguous for
common league names, confirmed live: "Serie A" also matches "LigaPro Serie A
(Ecuador)"; "Bundesliga" also matches "Austrian Bundesliga"; "Premier League" matches
six different competitions. 5 of 10 divisions (Serie A, Premier League, Bundesliga,
Ligue 1, Championship) failed outright on first attempt. Since `season_kickoff.py`'s
own step 4 calls this exact script without `--competition-id`, every future
season-kickoff run would have silently hit the same failure. Fixed: `import_club_
squads.py` now defaults `--competition-id` from `core/leagues.py`'s registry when the
caller doesn't pass one explicitly (8 of 10 divisions already have a registered id);
`season_kickoff.py` special-cases Serie A with its own discovered id
(`comp_5840`) since squad data comes from TheStatsAPI even though matches
deliberately don't (staying on football-data.org) — not added to the shared registry
field, since that field's meaning there is specifically about the match-import
source and overloading it would make `import_league_matches.py` wrongly skip the
football-data.org path for Serie A's matches.

**Final squad-refresh result: 9 of 10 divisions done.** Serie B is the one exception
— genuinely zero matches recorded for the 2026-27 season yet (its season hasn't
started producing results; `import_club_squads.py` requires at least one match on
record to know which teams to pull), same underlying data-availability shape as the
Bundesliga-fixtures and Serie-B-matches cases already noted above, not a bug. Will
resolve itself on a future re-run once Serie B's season is underway.

Step 5 (name-map gaps) surfaced real gaps as designed (Inter Milan, Atalanta BC,
Bayer Leverkusen, Alavés, Athletic Bilbao, Le Mans FC, Paris Saint Germain, and
others) — reported, not fixed, per the feature's explicit scope.
