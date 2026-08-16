# Model Version Impact Log

One entry per shipped `soccer_model_predictions.method` tag — a running,
append-only summary of what changed and its measured net impact, so "what did
version X actually do to the numbers" never requires digging back through
`BUGS.md`'s per-bug write-ups. `BUGS.md` stays the source of truth for root
cause / fix detail per bug; this file is the version-level rollup across
whichever bug(s) a given tag bundles. Newest first.

Note: a new method tag does NOT by itself mean live picks changed —
`generate_club_league_card.py` calls `compute()` directly with whatever's in
the current code, with no method-tag dependency at all. A tag only affects
backtest/comparison tooling until its constants are promoted to be the
module's shipped defaults (see each entry below for whether that happened).

---

## poisson_v4_3 — shipped 2026-08-15

**Bundles:** BUG-012 root cause #3 only -- the count-based team-attribution/
candidate-narrowing gate in `load_team_players` converted to calendar-based
(a player now qualifies via calendar-decayed weighted minutes at their team,
not "was in the team's last N matches by count" -- fixes a real fixture-
density sensitivity). New constant `PLAYER_RATING_MIN_TEAM_WEIGHTED_MINUTES_
TO_BE_A_CANDIDATE = 10.0`. Pure code fix, no tunable constant to promote
separately -- live picks reflect it immediately. Full detail: BUGS.md BUG-012.

**Impact (vs. `poisson_v4_2`):** pooled Brier 0.6037→0.6036 (negligible); ROI
within ~0.1-0.6pp at every threshold, no consistent direction; every league
near-flat (largest single-league Brier move was La Liga's -0.0002). Same
profile as `poisson_v4_1_1` (BUG-016): a real, individually-meaningful
correctness fix whose pooled effect is small because it only changes a
relatively narrow set of matches (players caught by the old fixture-density
trap specifically).

**Explicitly investigated and did NOT fix Ligue 1's `poisson_v4_2` weakness**
(this was the original motivation for prioritizing this work) -- confirmed
empirically that RC Lens's `weight_attack` is bit-for-bit unchanged by this
fix, since v4_3 only touches the rating computation
(`load_team_players`), not the blend-weight computation (`player_trust_
score`) that actually drives that number. Found a real, deeper issue in
`player_trust_score` while investigating (its "prior roster reference" is
anchored to a stale, purely count-based date that can reach back nearly a
year early in a season) -- prototyped a fix, validated the churn signal
stays correct, but found it would move trust the WRONG direction for Ligue
1's specific case (more team-level weight, not less). Not built. Full
detail, including the magnitude-check that shows the flagged teams are
NOT actually outliers in absolute weight_attack (RC Lens has the LOWEST
team-level weight of any Ligue 1 team at a comparable point in the season):
BUGS.md BUG-012.

**Net read:** ships a real, worth-keeping correctness fix; the Ligue 1
question remains open but is now well-understood, and the "small-sample
season-to-season variance" read (same conclusion as BUG-015) is more strongly
supported after this investigation than before it.

## poisson_v4_2 — shipped 2026-08-15

**Bundles:** BUG-012 Stage 2 (real calendar-time recency values: half_life=120d,
cutoff=180d, exponential shape — replacing Stage 1's near-no-op placeholders)
+ a follow-on fix to `roster_as_of_date`'s fallback path (found the same day,
re-validating tests after promoting these defaults — see BUG-012 in BUGS.md
for why it didn't require re-running this backfill). Picked from a 6-candidate
sweep pooled across all 5 leagues x 2 seasons against the `poisson_v4_1_1`
baseline. Constants promoted to shipped module defaults immediately — live
picks (`generate_club_league_card.py`) reflect this now.

**Impact (vs. `poisson_v4_1_1`):**
- ALL-UP pooled: Brier 0.6031→0.6037 (small degradation); ROI @10% EV
  -8.8%→-7.0% (the headline gain this candidate was picked for), @0/5% mixed.
- Per league, genuinely mixed, not a clean win everywhere:
  - Serie A: Brier improves (0.6072→0.6059), ROI@10% -9.8%→-6.9% (+2.9pp) —
    clear win.
  - Premier League: Brier flat, ROI better at every threshold (+3.1%→+4.7%
    @10%) — clear win.
  - La Liga: Brier flat/better, ROI@10% -17.1%→-12.8% (+4.3pp) but worse at
    0%/5% — net positive.
  - Bundesliga: worse across the board (Brier +0.0023) — consistent with
    BUG-015 (already a known-noisy 2-season sample for this league).
  - Ligue 1: worse across the board, the single biggest Brier hit of any
    league (+0.0032) — investigated in depth, see BUG-012 in BUGS.md. Traced
    to `player_trust_score`'s blend weight collapsing toward team-level right
    at the 2025-26 season's start (concentrated in Aug/Sep 2025, ~88% of the
    league's whole-season degradation), not a player-rating data problem.

**Net read:** real ROI gain on the model's highest-confidence bets, at a small
aggregate Brier cost, landing unevenly across leagues (3 improve, 2 don't).
The Ligue 1/Bundesliga weakness is a genuine, understood side effect of the
trust-score mechanism getting starved of roster-coverage data right at a
season's start — not a data or plumbing bug — and is exactly what **BUG-012's
still-open root cause #3 (v4_3)** is designed to address next.

## poisson_v4_1_1 — shipped 2026-08-15

**Bundles:** BUG-016 only (matchday grouping fixed from exact `match_date`
timestamp to calendar date — a later same-day kickoff could previously see an
earlier same-day match's already-finished result leak into the shared
league-wide baseline used to compute its own prediction). Grouping fix
promoted to the 4 affected scripts immediately (`backfill_player_blend_
predictions.py`, `backfill_with_xg_stretch.py`, `generate_club_league_card.py`,
`oracle_roster_blend_test.py`) — live picks already reflect it. Same config as
`poisson_v4_1` otherwise (no other flags/constants changed). Full detail:
`BUGS.md` BUG-016.

**Impact:**
- ALL-UP pooled: Brier 0.6033→0.6031 (negligible); ROI within ~0.2-0.4pp at
  every EV threshold, no consistent direction.
- Per league: every league moved <0.001 Brier and roughly ±1.5pp ROI at most,
  mixed direction (Premier League ROI@10% +0.5pp, La Liga -1.1pp, others
  near-flat).
- Per-match effect is real and much larger (up to 6pp win-probability shift,
  confirmed on individual matches in leagues with dense same-day scheduling —
  see BUG-016) but washes out at the pooled/per-league level since the leak is
  noise-like (direction depends on that day's earlier results), not a
  systematic bias.

**Net read:** correctness fix worth keeping (protects individual live-card
predictions on multi-kickoff days), but not a lever that moves aggregate
Brier/ROI — don't expect this tag to explain a season-level metrics change.
Now the reference baseline for ongoing BUG-012 Stage 2 work.

## poisson_v4_1 — shipped 2026-08-14

**Bundles:** BUG-013 (missing `PLAYER_RATING_CROSS_LEAGUE_GOAL_ADJUSTMENT`
entries for the 4 new leagues' feeder divisions) + BUG-014 (spread-stretch
recentering switched from additive to multiplicative, can no longer go
negative). Both fixes' constants were promoted to shipped defaults
immediately (not gated behind the tag) — live picks already reflected both
fixes the same day, backfilled to `poisson_v4_1` for backtest comparability
against the `poisson_v4` baseline. Full detail: `BUGS.md` BUG-013/BUG-014.

**Impact:**
- ALL-UP pooled (every league/season/market): Brier 0.5591→0.5572 (small
  improvement); ROI roughly flat (@0%: -6.6%→-6.7%; @5%: -6.9%→-6.3%; @10%:
  -6.6%→-6.8%) — small net positive, diluted at full pool since BUG-013 only
  touches promoted/thin-history teams, not the whole field.
- Real signal, TOTALS market, guardrail mode, pooled across seasons per
  league (where the fix actually shows up clearly):
  - Serie A: -0.2% → +0.5% → +6.4% (EV 0/5/10%) — flips positive, right shape
  - Premier League: -3.9% → -3.6% → -0.8% — right shape, still negative
  - Ligue 1: -10.3% → -9.8% → -7.9% — right shape, still negative
  - Bundesliga, La Liga: no clean shape yet (see BUG-015 -- Bundesliga's lack
    of signal here now reads as more evidence its whole 2-season sample is
    just noisy, not that the fix failed to help there)
- Isolated Bundesliga 2025 test (BUG-014 alone, BUG-013 controlled out):
  Totals Brier 0.5274→0.5150; Totals ROI improved at every EV threshold
  (-23.7%→-22.5% / -24.9%→-20.3% / -24.4%→-18.1%). 1X2 roughly flat.

**Net read:** real, measurable improvement in the TOTALS market for 3 of 5
leagues; negligible-to-flat elsewhere; nothing moved meaningfully the wrong
way.
