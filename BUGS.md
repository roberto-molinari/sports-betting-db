# Known Issues / Bug Log

A running log of known model/data issues that are understood but deferred.
Newest first. When fixing one, update its **Status** (and remove from the active
set once shipped + verified). Format is deliberately lightweight.

Severity: **high** (materially wrong picks across many teams) ·
**medium** (distorts some teams/matches) · **low** (cosmetic / rare).

---

## BUG-011 — `compute_club_player_strength.compute()` redundantly recomputes last-season aggregates on every matchday during a backfill, making full-season backfills slow

- **Type:** performance (no correctness impact) · **Severity:** low (cosmetic/rare —
  slows backfill/backtest runs, doesn't affect any stored prediction) · **Status:**
  OPEN, not yet fixed. Logged 2026-08-07 while running FEATURE-011 Follow-up B's
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

**Fix (not yet done):** memoize `player_season_minutes(conn, last_season)` (one dict,
reused for the whole run) and `team_roster_minutes(conn, team_id, last_season)`
(one dict per team, reused for the whole run) at the top of a backfill/backtest loop,
or add simple caching inside `player_trust_score`/`resolve_blend_weight` keyed on
`(team_id, season)`. No behavior change expected — same queries, same results, just
not re-run on every matchday.

---

## BUG-010 — poisson_v4 (player-level xG blend, FEATURE-011) generates wildly overconfident home-win probabilities for underdog home teams, driving negative ROI despite clearing the bias criterion

- **Type:** model calibration (tail behavior) · **Severity:** high (this is the concrete
  lead behind poisson_v4/poisson_v4_teamxg's negative ROI at every EV threshold, both
  seasons tested) · **Status:** OPEN, root cause not yet located. Logged 2026-08-02.

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

## BUG-009 — Model systematically undervalues home teams / overvalues away teams (confirmed via CLV + ROI; one contributing cause found + partially fixed; SECOND cause found 2026-08-04: spread compression on large mismatches, worsened by FEATURE-011's xG team-metric switch AND player-level blend)

- **Severity:** high (touches every match, every season; a material driver of negative
  backtest ROI, not a cosmetic calibration footnote) · **Status:** OPEN. Goal agreed
  2026-07-27. Two contributing causes now confirmed: (1) stale league-average baseline,
  partially fixed 2026-07-27; (2) spread compression on large mismatches, found
  2026-08-04 while investigating FEATURE-011's promoted-team ROI failure -- root cause
  identified (see below), not yet fixed. `SHRINKAGE_K` (the prior "next candidate") is
  RULED OUT -- confirmed at 0 in `core/poisson_model.py`, i.e. already a no-op.

**Finding.** Comparing the model's 1X2 probabilities (`soccer_model_predictions`,
`poisson_v2`) against sharp closing lines (Pinnacle, Betfair Exchange) and a soft book
(Bet365), at both opening and closing, across three full Serie A seasons (2023-24,
2024-25, 2025-26 -- `soccer_market_odds`, `compare_model_vs_market_odds.py`): the model
shows a consistent, same-direction bias in 2024-25 and 2025-26 -- mean signed diff on
home win probability is negative (model reads home teams as less likely to win than the
market, roughly -0.035 to -0.05 depending on season/source) and positive on away
(roughly +0.04 to +0.05). Present at **both** opening and closing lines (near-identical
numbers) and consistent across **both** sharp and soft books -- so it isn't a
late-line-movement artifact or one book's pricing quirk.

**Confirmed costly, not just a calibration curiosity.** Full-season backtest ROI
(`backtest.py`, all bets clearing EV threshold) is negative in all three seasons
(-15.1% / -12.7% / -5.2% at EV>0%) and does **not** improve monotonically as the EV
threshold is raised (2024: -12.7% → -14.9% → -14.6% at 0/5/10%) -- the miscalibration
isn't confined to the low-confidence tail, it's present in the model's most confident
bets too. By-side breakdown: away bets are the single worst-performing bucket in most
seasons (2023: -33.2% ROI on 164 away bets), consistent with the model overvaluing away
win probability and generating false-positive-EV away bets.

**Goal (agreed 2026-07-27).** Not "match the market exactly" -- a model that exactly
reproduces market probability has zero edge by construction, and some model/closing gap
is structurally irreducible (closing lines price in late information -- injury/lineup/
money -- the model can't have at generation time). The target is the **signed** bias
specifically, not raw distance: get mean signed diff on home and away to within roughly
±0.01-0.02 of zero, leaving the unbiased/idiosyncratic spread alone (that's where
genuine edge, if any, should come from). Full-season ROI is the validation check for
this goal over a multi-season horizon, not the tuning target itself -- ROI is noisy
(raising the EV threshold didn't reliably move it here), a lesson already learned once
in this log (see the knockout-ROI **WATCH** entry's "coin flip" finding).

**Diagnosis #1, confirmed: stale league-average baseline (partial cause).**
`get_league_averages()`'s `avg_home`/`avg_away` blended all history with no recency
weighting (DESIGN-002), and `avg_home` enters `estimate_lambdas()` as a bare divisor
(`lambda_H = h_att * (a_def / avg_h)`) that -- with `SHRINKAGE_K` at 0, i.e. shrinkage
fully disabled -- was one of the only live channels through which the league average
affected a prediction. Confirmed directly: per-season `avg_home - avg_away` (the
league's home-scoring edge) is ~0.257-0.258 in 2022-23 and ~0.118-0.126 in 2024-25 --
home advantage genuinely roughly halved -- while the old unweighted all-history blend
sat at 0.190, overstating today's home advantage by ~0.06-0.07 goals of edge.

**Fix shipped 2026-07-27 (partial).** `get_league_averages()` gained `window`/`decay`
params (see `core/poisson_model.py`); `analyse_match()` exposes them as
`league_avg_window`/`league_avg_decay` overrides, same single-entry-point pattern as
BUG-008's fix. Tested window sizes (10/20/30/40 matchdays, ~100-400 matches) and decay
values directly against the signed-bias metric on 2025-26 vs. Betfair closing: **all
converge on the same plateau** (signed home ≈ -0.025, signed away ≈ +0.031) regardless
of whether a short window, a long window, or continuous decay is used -- confirming this
mechanism has a hard ceiling, not a tuning problem. Shipped the simplest option that
reaches the plateau: `LEAGUE_AVG_WINDOW = 100` (~10 matchdays), `LEAGUE_AVG_DECAY = 1.0`
(off -- adds no benefit over the window alone). New model version `poisson_v3`,
backfilled to `soccer_model_predictions` for all three seasons. Tests: `test_get_league_
averages_window_limits_to_recent_matches`, `test_get_league_averages_default_window_is_
100`, `test_get_league_averages_decay_weights_recent_matches_more` (`tests/test_poisson_
model.py`).

**Result, validated across all seasons/books, not just the diagnosis slice:**

| | signed home | signed away | favored agree |
|---|---|---|---|
| 2023 vs Pinnacle | +0.0109 → +0.0093 | +0.0074 → +0.0064 | 78.2% → 77.9% |
| 2024 vs Pinnacle | -0.0353 → -0.0175 | +0.0520 → +0.0374 | 74.5% → 77.4% |
| 2024 vs Betfair | -0.0361 → -0.0183 | +0.0531 → +0.0385 | 73.9% → 76.8% |
| 2025 vs Betfair | -0.0423 → -0.0252 | +0.0396 → +0.0309 | 70.2% → 73.8% |

Full-season backtest ROI (validation check, not the tuning target): 2023 -15.1% →
-9.1%, 2024 -12.7% → -12.3% (barely moved), 2025 -5.2% → -3.9%. Still negative
everywhere, consistent with a partial fix -- no surprises between the CLV and ROI reads.

**Still open (as of the 2026-07-27 fix).** A real residual bias remains (~-0.02 to -0.03
signed home, ~+0.03-0.04 signed away in 2024/2025), well outside the ±0.01-0.02 target.
2023 was already near-zero before this fix and stayed there -- worth noting the bias may
not be uniform across seasons/eras, another thread to pull on.

**Diagnosis #2, confirmed 2026-08-04: spread compression on large mismatches (the
"residual bias" root cause) -- discovered investigating why FEATURE-011's poisson_v4
fails the ROI success criterion despite passing Model Calibration.** The signed home/
away split (above) only measures average bias pooled across ALL matches -- it can't see
a bias that changes SIGN and MAGNITUDE with how lopsided a match is, which is exactly
what's happening. Bucketing model p_home vs. Betfair Exchange closing p_home by the
market's own implied probability (`poisson_v4_priorblend`, Serie A 2025-26, n=359):

| Market p(home) | poisson_v3 (2026-07-27 fix) | pureteamxg (xG team-level, no player blend) | poisson_v4_priorblend (full v4) |
|---|---|---|---|
| 0.00-0.15 | +0.013 | +0.069 | +0.108 |
| 0.15-0.25 | -0.011 | +0.055 | +0.094 |
| 0.25-0.35 | +0.010 | +0.039 | +0.051 |
| 0.35-0.45 | -0.020 | -0.017 | -0.014 |
| 0.45-0.55 | -0.062 | -0.032 | -0.045 |
| 0.55-1.00 | -0.056 | -0.071 | -0.125 |

The model systematically pulls extreme probabilities toward the middle relative to the
market -- overrating big underdogs AND underrating big favorites, growing with the size
of the mismatch. NOT specific to any one team (confirmed identical in matches between
two "team-dominated" teams, i.e. resolve_blend_weight >= 0.5 on both sides, vs. matches
involving a "player-dominated" team -- ruling out compute_club_player_strength.py's
K_SHRINK_MINUTES/positional-shrinkage pipeline as the SOLE cause, since the pattern is
present even when neither side touches it).

Decomposed by re-running the identical bucket check on `poisson_v3` (goals-based team-
level, no player blend at all -- this session's pre-2026-08-02 model) vs. a pure-team-
level xG variant (`--weight-attack 1 --weight-defense 1 --team-metric xg`, isolates the
team-rating source with zero player-blend contribution) vs. the full `poisson_v4_
priorblend`: **two separate, additive causes**, both introduced/discovered via
FEATURE-011's 2026-08-02 work --
1. **Switching the team-level rating from actual goals to xG (`team_metric="xg"`,
   shipped 2026-08-02, the change that CLEARED the Model Calibration success
   criterion)** is itself the larger contributor at the underdog end -- alone adds
   +0.056 on top of poisson_v3's small residual (+0.013 -> +0.069 at the biggest-
   underdog bucket). This is a real tradeoff, not a wash: the xG switch fixed the
   POOLED signed home/away bias (verified, see the 2026-07-27... no, 2026-08-02 result
   table in FEATURE-011_BUILD_TRACKER.md) while introducing a mismatch-size-dependent
   bias that the pooled signed-bias metric structurally cannot see.
2. **The player-level blend layer on top adds a second, separate contribution** -- going
   from pureteamxg to full priorblend adds another +0.039 at the underdog end and a
   much larger -0.054 at the favorite end. Exact mechanism within the player-blend
   layer (K_SHRINK_MINUTES=900 positional shrinkage is the leading suspect -- it
   shrinks essentially every player's rate toward the position-wide average, which
   would compress team-quality spread league-wide, not just for thin-sample players)
   not yet isolated further.

**Why this matters for FEATURE-011's ROI failure specifically:** promoted teams
(Cremonese/Sassuolo/Pisa) are simply the most extreme underdogs in the league most
often, so they're where this shows up most acutely (e.g. Cremonese's own attack/defense
ratings are close to league-average, not inflated -- see FEATURE-011_BUILD_TRACKER.md;
even a hypothetical "both teams exactly league-average" matchup already gets ~40% model
home-win probability from the baseline/home-advantage structure alone). The promoted-
team cold-start trust-score bug (this file's earlier note, fixed 2026-08-03) was real
and independently worth fixing, but it is a SECONDARY contributor to the ROI failure --
this spread-compression pattern is the dominant one, and it's league-wide.

**Not yet fixed (as of 2026-08-04).** Candidates: (a) revert `team_metric` default to
"goals" (undoes the xG-switch's ~half of the compression, but reopens some of the
pooled signed home/away bias it fixed -- a real tradeoff to make deliberately, not
accidentally); (b) reduce or retune `K_SHRINK_MINUTES` and re-check both this bucket
table AND the pooled signed-bias metric together, since BUG-009's original fix and this
one are now coupled; (c) investigate whether get_team_xg_ratings' xG data source itself
has less spread than actual goals by construction (a known general property of xG
models) vs. something league/window-specific fixable with tuning.

**2026-08-05: (a) built and validated -- `team_xg_weight` param added to
`team_level_lambda`/`compute()` (0.0=pure goals, 1.0=pure xG/DEFAULT/no-op, blends in
between; replaces the old binary `team_metric` string, exact behavior preserved at both
boundaries). Full-pipeline sweep (`poisson_v4_priorblend` family, Serie A 2025-26, vs.
Betfair closing): bucket compression improves monotonically as weight drops toward 0.5
(underdog end +0.108→+0.083, favorite end -0.125→-0.113) while pooled signed-bias stays
within the ±0.01-0.02 target at every value tested down to 0.25 (-0.0090 at 1.0 down to
-0.0195 at 0.25). ROI, however, did NOT track the calibration improvement -- ROI @0/5/
10% across alpha=1.0/0.75/0.5/0.25 zigzags with no monotonic trend (0.75 is the WORST of
the four at EV>10%, -15.5%; 0.25 is the BEST at EV>0%, -8.1%) -- same "ROI is a noisy
single-season validation check, not a tuning target" lesson as the original 2026-07-27
fix. Not shipped as a new default pending more validation; parameter exists, default
unchanged (1.0).

**2026-08-05: found a MUCH larger, validated ROI lever -- the existing WC "noise
amplification on longshots" guardrails (BUG-003, `generate_wc_card.py`:
`MIN_PICK_PROBABILITY=0.25` floor, `MAX_UNDERDOG_MARKET_DISAGREEMENT=2.0` ratio cap) had
never been ported to the club-league pipeline at all -- every poisson_v4 backtest all
week included picks a WC-style guardrail would have silently rejected.** Retroactively
applying floor+cap to `poisson_v4_priorblend`'s existing positive-EV picks (no model
change, pure bet-selection filter -- pooled signed bias is mathematically unaffected,
confirmed directly: 2025 home -0.0090/away +0.0125, 2024 home -0.0050/away +0.0055, both
comfortably within target either way):

| Guardrail | 2025 ROI @0/5/10% | 2024 ROI @0/5/10% |
|---|---|---|
| none | -9.0% / -7.5% / -7.6% | -15.6% / -20.7% / -21.2% |
| floor=0.25 only | -8.0% / -5.9% / -2.4% | -6.7% / -10.7% / -8.8% |
| floor=0.25 + cap=2.0 (WC's value) | -6.2% / -3.6% / **+0.6%** | -8.4% / -13.0% / -11.7% |

Floor alone is a large, consistent win in BOTH seasons (roughly halves the loss every
time). The WC's cap=2.0 helps in 2025 but hurts relative to floor-alone in 2024 -- its
marginal value isn't season-robust. Swept cap directly against both seasons (floor held
at 0.25): **cap=1.75 is the point where the two seasons' ROI curves converge**
(2024 -5.8% / 2025 -5.7% at EV>0%, vs. cap=2.0's -8.4%/-6.2% -- a much tighter agreement
than any other cap value tested, a stronger robustness signal than picking whichever
single point scores best on one season) and beats cap=2.0 in EVERY cell of a 2-season x
3-threshold grid (6/6). 2 seasons x ~300-330 bets is still a real sample-size caveat --
"best-supported value in the data available," not proven. Not yet built into the
pipeline as a real gate (tested via an ad hoc script only); WC's `select_pick()`/
guardrail pattern (`generate_wc_card.py`) is the template to port if/when shipped.

**Guardrail diagnostic breakdown (2026-08-05, floor=0.25/cap=1.75, both seasons pooled,
EV>0%, one dimension at a time -- not re-tuned per slice, just checking where the
already-chosen values' benefit concentrates):**

| Dimension | Baseline ROI | With guardrail |
|---|---|---|
| Home picks | -23.4% | -11.6% |
| Draw picks | -6.9% | -7.9% (guardrail ~inert on draws, as expected -- cap can't fire on a draw the way it does on a 1X2 underdog) |
| Away picks | -7.4% | +2.6% |
| Early season (MD 1-19) | -7.7% | -0.5% |
| **Late season (MD 20-38)** | **-17.2%** | **-11.3%** |
| Favorite picks (implied>=0.5) | +19.5% (n=26, too small to trust) | +19.5% (n=26, guardrail never fires here) |
| Underdog picks | -13.3% | -6.9% |
| Top-half team backed | **+15.2%** | +8.4% (guardrail COSTS profit here -- cuts some winners along with losers) |
| Bottom-half team backed | -26.7% | -13.3% |
| Excluded-by-guardrail picks (own perf) | -25.1% ROI, 12.4% win rate | n/a (this IS the excluded group) |
| Included/kept picks (own perf) | -5.8% ROI, 28.5% win rate | n/a (this IS the included group) |

Two findings worth carrying forward: (1) excluded picks have less than half the win
rate of included picks (12.4% vs 28.5%) -- the guardrail is demonstrably cutting real
bad bets, not just shrinking the sample for luck; backing top-half teams was ALREADY
profitable pre-guardrail and the guardrail shaves some of that off, a real (if smaller)
cost against the larger gain elsewhere. (2) **Late season is the worse bucket, both
before AND after the guardrail fix -- the opposite of the "cold start" intuition that
motivated most of this week's work (promoted-team trust score, prior-season blend).**
If cold-start/thin-early-season-data were the dominant driver, early season should be
worse. It isn't. This suggests a SEPARATE, not-yet-diagnosed systemic issue in how the
model weighs accumulated in-season data as a season progresses -- next thread to pull,
distinct from everything fixed/found this week. Small-sample caveat applies (2 seasons,
~450-480 picks per early/late bucket) but the direction is clear enough to prioritize.

**2026-08-05: late-season anomaly diagnosed -- NOT the spread-compression bug, NOT
general model miscalibration; a real market-information gap that peaks specifically in
MD20-28, concentrated in bottom-table (and top-table) teams.** Three checks against the
"cold start gets worse as the season goes on" intuition, ruled out cleanly:

1. **Spread-compression bug does not get worse late season -- if anything it's
   slightly smaller in every bucket** (e.g. biggest-underdog bucket +0.112 early vs
   +0.096 late, biggest-favorite bucket -0.139 early vs -0.106 late; market's own
   implied-probability extremity is flat, 0.175 early vs 0.178 late). Ruled out the
   "more current-season minutes accumulated -> less shrinkage -> bigger mismatches ->
   compression bites harder" hypothesis directly (also confirmed shrinkage weight
   `mins/(mins+900)` does climb steadily across the season, 0.17 at MD5 to 0.58-0.59 by
   MD38, as expected -- the mechanism is real, it just isn't the explanation here).
2. **The model's own calibration against REALIZED outcomes (Brier score, all games,
   not just picks) is BETTER late season, not worse** (0.615 early -> 0.590 late,
   pooled both seasons) -- and the MARKET improves similarly (0.586 -> 0.556). Per
   `[[backtest-vs-realized-outcomes]]`-style discipline (check against reality, not
   just the market): this rules out "the model's probabilities get less accurate as
   the season goes on" as the cause -- they don't.
3. **What DOES get worse: the specific subset of games the model actually bets on**
   (positive-EV picks vs Bet365, pre-guardrail). Overconfidence (mean model pick
   probability minus actual win rate) on this subset rises from +0.072 (MD1-19) to
   +0.088 (MD20-38), and by finer matchday chunk is sharply non-monotonic -- MD1-9
   +0.066, MD10-19 +0.077, **MD20-28 +0.104**, MD29-38 +0.071 (partial recovery). So
   this isn't "everything gets worse as the season wears on" -- it's a **spike
   concentrated in the MD20-28 window specifically**, not a smooth late-season drift.

**MD20-28 breakdown by team's final-table position (both seasons pooled, n=236
positive-EV picks in that window):**

| Group | n | ROI | Win rate | Mean model p |
|---|---|---|---|---|
| Top-6 (final table) backed | 21 | -37.2% | 23.8% | 0.424 |
| Mid-table backed | 74 | +9.8% | 29.7% | 0.309 |
| **Bottom-6 (final table) backed** | 76 | **-62.5%** | **10.5%** | 0.304 |
| Draw / no team attribution | 65 | -21.5% | 18.5% | 0.259 |

Bottom-6 is the standout: 76 picks the model liked at an average 30% probability won
only 10.5% of the time -- a much larger gap than anywhere else in the whole
investigation this week. Top-6 is bad too (though n=21 is thin). Mid-table, by
contrast, is fine (+9.8% ROI) in the exact same window -- so this isn't a pure
matchday-number effect either, it's specific to teams with something riding on the
run-in (relegation fight or European-spot race).

**Leading hypothesis (not yet tested further): the Serie A winter transfer window.**
Italy's window closes late January / early February, which lines up almost exactly
with MD20-22 -- right at the front edge of the MD20-28 spike. A relegation-battle
squad reshaped by January signings (or a key departure) has very little current-season
minutes on the new pieces, so the model's rolling stats are describing a roster that
partially no longer exists on the pitch -- compounded by motivation/manager-change
effects (relegation four-point-plans, a new manager bounce) that no stats-based signal
captures at all. This is a genuinely different mechanism from anything fixed this week
(cold-start trust score is about *promoted* teams at *season start*; this is
*established* teams reshaped *mid-season*) and from the compression bug (ruled out
above). **Not yet fixed or further isolated** -- next steps, if pursued: (a) check
whether bottom-6 team's MD20-28 losing picks cluster around actual transfer activity
(new-signing-heavy lineups) vs. squad-stable relegation teams that still lose value,
which would separate "roster-churn blind spot" from "pure motivation/desperation
effect" as the driver; (b) same table-position x window breakdown on a 3rd season if/
when one is backfilled, to check this isn't a 2-season coincidence.

**2026-08-05: (a) tested directly -- roster-churn/departure NOT the driver, cleanly
ruled out.** Flagged every bottom-6/top-6 team with a "significant departure" (a
player with >=500 minutes before MD20 who then plays zero minutes for that team from
MD20 on -- sold, released, or long-injured; the model can't tell which, but either way
their stale rate keeps counting per `MODEL_PIPELINE_OVERVIEW.md` section 1's gap) and
split the MD20-28 picks on it:

| Group | Has significant departure | No departure |
|---|---|---|
| Bottom-6 backed | n=39, roi=-62.0%, win 12.8% | n=37, roi=-63.0%, win 8.1% |
| Top-6 backed | n=4, roi=-18.8%, win 25.0% | n=17, roi=-41.6%, win 23.5% |
| Mid-table backed | n=49, roi=+20.4%, win 34.7% | n=25, roi=-10.9%, win 20.0% |

Within bottom-6 -- the group where the anomaly actually lives -- ROI and win rate are
statistically indistinguishable with or without a squad-stable-vs-churned split
(-62.0% vs -63.0%). Squad-stable relegation teams (Lecce 2024, Empoli 2024, Hellas
Verona 2025, Cremonese 2025, Genoa 2025 -- no player crossing the 500-minute
departure bar) collapse just as hard as the churned ones (Cagliari, Venezia, Monza,
Parma 2024; Pisa, Fiorentina, Lecce 2025). **This rules out Follow-up A's stale-
departed-player mechanism as the driver of THIS specific pattern** -- the roster-
awareness feature ask still stands on its own general merit (section 1 of
`MODEL_PIPELINE_OVERVIEW.md` is still a real gap), but building it would not be
expected to fix the MD20-28 bottom-6 collapse specifically. Points the remaining
explanation back toward something common to EVERY bottom-6 team in that window
regardless of squad continuity -- relegation-fight motivation/effort-level swings
(FEATURE-004's "dead rubber" idea is the inverse case; FEATURE-013 already proposed
"additional external factors" as a category) rather than a data-staleness bug. Not
further isolated; may be genuinely outside what a stats-only model can capture.

**2026-08-05: separate thread -- additive-vs-multiplicative inconsistency in the
player-level ATTACK recentering, tested and found to shrink compression but NOT
reliably improve ROI (same disconnect as the `team_xg_weight` sweep).** Spotted while
discussing whether the model treats goal-scoring as linear (user's hypothesis): today,
`compute_club_player_strength.compute()` recenters player-level attack with a flat
additive shift (`avg_home + (r["ra"] - attack_mean)`) but recenters defense with a
multiplicative ratio (`r["rd"] * (avg_away / defense_mean)`) -- an unexplained
asymmetry between the two components in the SAME function, and structurally exactly
the "a fixed delta means the same thing everywhere" pattern the user was asking about
(the team-level system itself, `lambda_H = h_att * (a_def / avg_h)`, is already
multiplicative throughout, so this asymmetry is specific to the player-blend layer).

Tested making attack multiplicative too (ad hoc script, not committed). Compression
bucket table (2025-26, n=329, vs Betfair closing) improved in every bucket (e.g.
biggest-underdog end +0.109->+0.084, biggest-favorite end -0.128->-0.106) with pooled
signed bias essentially unchanged (-0.0104 -> -0.0102, both already within target).
**ROI, however, is season-inconsistent -- the same "calibration improves, ROI doesn't
track it" pattern already seen with `team_xg_weight`:**

| | 2024 @0/5/10% EV | 2025 @0/5/10% EV | Pooled @0/5/10% EV |
|---|---|---|---|
| additive (today) | -15.6% / -20.7% / -21.2% | -9.0% / -7.5% / -7.6% | -12.4% / -14.1% / -14.3% |
| multiplicative | -14.7% / -18.7% / -19.6% | -16.0% / -9.4% / -9.7% | -15.3% / -14.0% / -14.5% |

Multiplicative is a clear, consistent win in 2024 (better at all 3 thresholds) and a
clear, consistent loss in 2025 (worse at all 3 thresholds) -- pooled, it's close to a
wash. Not shipped as a default. Consistent with this week's recurring lesson: ROI is a
noisy single-season signal that does not reliably move with calibration improvements
-- the compression-bucket result is real evidence this fixes something, but on its
own isn't sufficient to justify shipping given the ROI picture is a coin flip across
the two seasons available. Candidate for revisiting alongside `team_xg_weight` if/when
a 3rd season of backtest data exists.

**2026-08-05: sanity-checked the ROI success criterion's own benchmark validity --
does a sharp closing line actually beat Bet365, i.e. is chasing agreement with it a
meaningful target at all?** Ran the identical backtest methodology used all week
(find bets where a "prediction" disagrees enough with Bet365's price to clear an EV
threshold, then check real-money ROI against Bet365's payouts) but substituted the
SHARP BOOK'S OWN closing fair-probability in place of the model's:

| Source used as "prediction" | 2024 @0% EV | 2025 @0% EV | Pooled @0/5/10% EV |
|---|---|---|---|
| **Betfair Exchange** | **+6.1%** (n=243) | **+13.8%** (n=197) | **+9.6%** / +27.8% / -1.2% |
| Pinnacle | -11.6% (n=250) | +8.5% (n=108) | -5.6% / +4.6% / +7.7% |

**Betfair Exchange -- the source this entire week's bias/compression diagnostics have
used as ground truth -- validates cleanly:** positive ROI in BOTH seasons
individually and pooled, at the largest/most-robust threshold (EV>0%, n=440). This
confirms the ROI success criterion is chasing a real, profitable target, not an
arbitrary one. **Pinnacle does NOT validate as cleanly** -- pooled negative at EV>0%
(the only threshold with a robust sample, n=358), only positive at higher thresholds
where n drops to 29-45 (thin, noisy). Practical implication: the model's persistent
negative ROI this week (-5% to -21% across variants/seasons) should be read against
Betfair Exchange's own achievable ceiling (+9.6% at EV>0%, and even that gets noisy
past EV>5%, n=109-123) -- "beat zero" and "beat +9.6%" are different bars, and even
the sharp benchmark itself doesn't hold up at every threshold/source combination
tested. Not a criticism of the criterion itself (Betfair Exchange holds up fine), but
useful context for calibrating how much ROI improvement is realistically achievable.

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
