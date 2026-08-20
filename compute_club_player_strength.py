"""
FEATURE-011 prototype: compute player-based attack/defense lambdas for club-league
teams and blend them against the EXISTING team-level lambda system, per the agreed
Player-Level Strength Estimation design (FEATURE-011_REQUIREMENTS.md):

    lambda = (1 - w) * player_lambda + w * team_lambda      (independently for
                                                               attack and defense)

Reuses compute_wc_team_strength.py's aggregation approach (position-weighted rates,
minutes-based shrinkage toward positional priors) but drops the WC-specific FIFA-rank
fallback/blend entirely -- the "other side" of the blend here is the EXISTING
club-league team-level lambda (core.poisson_model.get_team_ratings), not FIFA rank.

Still out of scope (see FEATURE-011_BUILD_TRACKER.md for the current task list):
  - single scalar attack/defense per team, no home/away split (Scenario 4 deferred)
  - no league-quality factor applied: all players in scope play in the SAME league
    (Serie A), so LEAGUE_FACTORS would be a no-op here. It becomes relevant once a
    cross-league transfer scenario (Scenario 7) is in play.

Usage:
    python compute_club_player_strength.py --league "Serie A" --team "AC Milan"
    python compute_club_player_strength.py --league "Serie A" --limit-teams 3 --persist
"""

import argparse
import sqlite3
from datetime import date, timedelta
from statistics import mean

from core.sports_db import DATABASE_PATH, set_player_team_strength
from core.poisson_model import (
    get_team_ratings, get_league_averages,
    TEAM_RATING_MIN_MATCHES_TO_TRUST_TEAM_RATING_OVER_LEAGUE_AVERAGE,
    TEAM_RATING_PULL_TOWARD_AVERAGE_MATCHES,
    TEAM_PAST_MATCH_WINDOW_SIZE,
    _shrink,
)

# Same weights/rationale as compute_wc_team_strength.py (BUG-001/BUG-002 family):
# forwards carry attack, defenders/keepers carry defense, midfield contributes to both.
PLAYER_RATING_POSITION_ATTACK_WEIGHTS = {"FWD": 1.0, "MID": 0.6, "DEF": 0.2, "GK": 0.0}
PLAYER_RATING_POSITION_DEFENSE_WEIGHTS = {"GK": 1.0, "DEF": 0.8, "MID": 0.3, "FWD": 0.1}

# Same half-trust point as the WC system -- ~10 matches, set from how football works,
# not fit to this league.
PLAYER_RATING_MINUTES_TO_HALF_TRUST_OWN_RATE_OVER_LEAGUE_AVERAGE = 900.0

# Player-level rolling window (2026-08-06, MODEL_TUNING_PARAMETERS.md/BUGS.md FEATURE-
# 011 Follow-up B) -- replaces the old flat season-to-date sum load_team_players used
# to compute. Mirrors TEAM_PAST_MATCH_WINDOW_SIZE's shape, one level down (player
# instead of team): a player's attack/defense rate is built from their last N
# appearances, recency-weighted (see PLAYER_RATING_RECENCY_HALF_LIFE_DAYS/_CUTOFF_DAYS
# below -- BUG-012, 2026-08-14, replaced this window's original rank-based decay
# constant with calendar-time decay), SEASON-BLIND -- the window reaches back across a
# season boundary (and across a team/league change, following the PLAYER not the
# roster slot) the same way it reaches back across a matchday, with no special-cased
# "prior season" discount layered on top. This replaces the old
# blend_prior_season_attack/PRIOR_SEASON_DISCOUNT mechanism entirely (retired the same
# day this was added) -- that mechanism was a cruder, season-boundary-shaped version of
# exactly this same idea. Starting value matches TEAM_PAST_MATCH_WINDOW_SIZE (10 games)
# -- not yet independently tuned.
PLAYER_RATING_PAST_MATCH_WINDOW_SIZE = 10

# BUG-012 (2026-08-14) Stage 1: calendar-time-based recency, replacing the
# rank/count-based windowing above project-wide -- load_team_players' decay**rank
# (a match's weight depended on its POSITION in a top-N list, not its actual age)
# and player_trust_score's flat last-N-appearance minute sums (no decay at all).
# Both let a player out for months still read as fully "recent" just by being
# inside the top-N, which is the bug (see BUGS.md). Stage 1 shipped near-no-op
# defaults (~2.7 billion years) as a verified structural no-op. Stage 2
# (2026-08-15, shipped as poisson_v4_2) tightened these to real, swept values --
# half_life=120d (~4mo), cutoff=180d (~6mo), exponential shape -- picked from a
# multi-league sweep pooled across all 5 leagues x 2 seasons (60/90, 80/120,
# 120/180, 150/225, 200/300, 300/450 all tried; 120/180 gave the best EV>10%
# ROI improvement of any candidate, +1.8pp pooled, for a modest Brier cost).
# Real per-league impact (vs poisson_v4_1_1, the BUG-016-corrected baseline):
# net positive for Serie A/Premier League/La Liga, net negative for Bundesliga
# (already a known-noisy 2-season sample, BUG-015) and Ligue 1 (traced to the
# trust-score/blend-weight mechanism below getting starved of roster-coverage
# data right at a new season's start -- see BUG-012 in BUGS.md). Full sweep +
# validation detail: BUGS.md BUG-012, MODEL_VERSION_LOG.md poisson_v4_2 entry.
PLAYER_RATING_RECENCY_HALF_LIFE_DAYS = 120.0
PLAYER_RATING_RECENCY_CUTOFF_DAYS = 180.0

# BUG-009 proposed fix (2026-08-19): team-specific linear de-shrink, replacing
# the flat multiplicative player_spread_stretch_attack/_defense correction
# with a per-team credibility factor derived from apply_shrinkage's own
# per-player shrink weights (see apply_shrinkage's and team_credibility's
# docstrings for the mechanism). Motivation: the flat stretch applies the
# SAME correction to every team regardless of how much shrinkage that team's
# specific roster actually absorbed -- a settled, high-minutes squad barely
# gets shrunk and needs almost no correction, while a thin/rotation squad
# gets shrunk hard and needs a much bigger one. A chat-session probe
# (2026-08-18/19, 200 lopsided matches) found this nearly zeroes the
# favorite/underdog compression bias (-0.053 -> -0.012) at the SAME k_minutes
# as the shipped baseline (no shrinkage change at all), at roughly 60% of the
# bulk-calibration cost (Brier/pooled bias) that an equivalent flat-stretch
# fix required -- a real improvement to the trade-off, not a full escape from
# it (see BUGS.md BUG-009). Off by default -- a genuinely different code
# path, not just a new constant value, so it ships as an explicit opt-in
# toggle pending a full backfill sweep before it could become the default.
PLAYER_RATING_USE_TEAM_CREDIBILITY_DESHRINK = False

# Floor on the per-team credibility factor before dividing by it in the
# team-credibility de-shrink above, so a team with almost no credible player
# data doesn't get wildly extrapolated -- same "must not diverge to something
# absurd" discipline BUG-014 established for the old additive spread-stretch
# bug. Probed at 0.15 in chat (2026-08-19); not independently swept.
PLAYER_RATING_TEAM_CREDIBILITY_FLOOR = 0.15


def calendar_recency_weight(match_date, before_date, half_life_days=PLAYER_RATING_RECENCY_HALF_LIFE_DAYS,
                            cutoff_days=PLAYER_RATING_RECENCY_CUTOFF_DAYS, shape="exponential"):
    """How much a match played on `match_date` still counts as of `before_date`
    (BUG-012) -- the one centralized function every calendar-time-decay call
    site routes through, replacing each one's own count/rank-based "recency"
    proxy (see the constants above for which call sites and why).

    Hard floor common to both shapes: exactly 0.0 once elapsed_days >
    cutoff_days (a real cutoff a caller can use to stop counting a match at
    all, not just near-zero-weight it).

    shape="exponential" (default): 1.0 at elapsed_days == 0, halving every
    half_life_days below the cutoff -- decays fastest right away, then tapers.

    shape="linear" (added 2026-08-14, Stage 2 calibration -- explored because
    exponential's early-days drop-off felt too aggressive relative to a
    several-month cutoff): a straight ramp from 1.0 at elapsed_days == 0 down
    to 0.0 exactly AT elapsed_days == cutoff_days -- `half_life_days` is unused
    in this shape, cutoff_days alone defines the whole ramp.

    shape="flat" (added 2026-08-14, Stage 2 calibration -- isolates the "count-
    window -> calendar-window" swap from adding any decay curve at all): 1.0
    for any match within cutoff_days, no decay whatsoever inside that boundary
    -- the SAME flatness the old count-based `window_size` mechanism shipped
    with (its own decay constant was 1.0, a no-op), just bounded by calendar
    days instead of by game count. `half_life_days` is unused, same as linear.

    match_date, before_date: ISO date strings, plain ('YYYY-MM-DD') or a full
    timestamp ('YYYY-MM-DDTHH:MM:SS.sssZ') -- soccer_matches.match_date carries
    either depending on data source, so only the first 10 characters are parsed
    (same truncate-then-fromisoformat convention already used elsewhere in this
    codebase, e.g. import_wc_match_xg.py). before_date must not be earlier than
    match_date's own CALENDAR DAY -- every caller already enforces
    `match_date < before_date` at the full-timestamp SQL level (the project's
    no-lookahead discipline), but two matches on the same calendar day with
    different kickoff times (a real, common case -- an early and a late kickoff
    on the same matchday) truncate to elapsed_days == 0, not an error: that
    match is exactly as "recent" as it gets, weight 1.0. Only a genuinely
    NEGATIVE gap (before_date's calendar day earlier than match_date's) signals
    a real caller bug -- a lookahead -- and raises rather than silently
    returning a meaningless weight."""
    elapsed_days = (date.fromisoformat(str(before_date)[:10]) - date.fromisoformat(str(match_date)[:10])).days
    if elapsed_days < 0:
        raise ValueError(
            f"calendar_recency_weight: before_date ({before_date}) must not be "
            f"earlier than match_date's calendar day ({match_date})"
        )
    if elapsed_days > cutoff_days:
        return 0.0
    if shape == "exponential":
        return 0.5 ** (elapsed_days / half_life_days)
    if shape == "linear":
        return 1.0 - elapsed_days / cutoff_days
    if shape == "flat":
        return 1.0
    raise ValueError(f"calendar_recency_weight: unknown shape {shape!r}")


def match_calendar_date(match_date):
    """Calendar-date portion ('YYYY-MM-DD') of a match_date value -- same
    truncate-then-parse convention as calendar_recency_weight, factored out
    so callers grouping matches by day don't each reimplement `str(x)[:10]`."""
    return str(match_date)[:10]


def matches_on_date(rows, date):
    """rows (soccer_matches-shaped, from a query ordered by match_date) whose
    calendar date equals `date` ('YYYY-MM-DD'), regardless of kickoff time-of-
    day -- BUG-016, 2026-08-15. Backfill/card scripts previously grouped by
    the exact match_date string (itertools.groupby), which for full-timestamp
    leagues meant two same-day matches at different kickoff times landed in
    separate groups and triggered two redundant full-league compute() calls
    instead of one -- up to a ~2x blowup in some leagues. Grouping by calendar
    date instead restores one compute() call per real matchday. Pass `date`
    itself (not a same-day match's own timestamp) as compute()'s before_date:
    every match_date < before_date comparison in this codebase already
    happens on ISO strings, and a bare 'YYYY-MM-DD' is a strict-prefix, so it
    correctly excludes EVERY match that calendar day (even an earlier same-
    day kickoff) rather than the old per-timestamp grouping's asymmetric
    behavior, where an earlier same-day match's already-finished result could
    leak into a later same-day match's prediction. Matches the live-card
    convention `compute()`'s own __main__ already uses (date.today().
    isoformat(), no time component)."""
    return [r for r in rows if match_calendar_date(r["match_date"]) == date]

# Split 2026-08-06 (MODEL_TUNING_PARAMETERS.md) -- these used to be one constant each
# (MIN_ATTACK_WEIGHT/MIN_DEFENSE_WEIGHT) doing double duty: gating both whether a team
# gets its OWN player-based rating at all, and separately whether that team's raw
# number counts toward the league-wide average other teams get recentered against.
# Same starting value for both of each pair (300.0, lower than WC's 1000 -- a
# single-league squad has fewer thin-coverage players diluting the pool than a WC
# squad does) -- no behavior change from the split itself, just decoupled so either
# can be tuned independently later.
PLAYER_RATING_MIN_ATTACK_WEIGHTED_MINUTES_TO_HAVE_OWN_RATING = 300.0
PLAYER_RATING_MIN_ATTACK_WEIGHTED_MINUTES_TO_JOIN_LEAGUE_AVERAGE = 300.0
PLAYER_RATING_MIN_DEFENSE_WEIGHTED_MINUTES_TO_HAVE_OWN_RATING = 300.0
PLAYER_RATING_MIN_DEFENSE_WEIGHTED_MINUTES_TO_JOIN_LEAGUE_AVERAGE = 300.0

# BUG-012 root cause #3 (2026-08-15, v4_3): the candidate-narrowing gate in
# load_team_players used to be count-based ("was this player in the team's own
# last window_size matches") -- fixture-density-sensitive, since "last window_size
# matches" stretches or shrinks in calendar time depending on how densely a team's
# been playing (a dense cup stretch could exclude a player out just 6-7 weeks;
# a sparse early-season schedule could still include one who hasn't played in
# months). Replaced with a calendar-bound check: a player clears the gate for a
# team if their calendar_recency_weight-weighted minutes AT THAT TEAM specifically
# (summed across every qualifying appearance within cutoff_days) meet this floor.
# Deliberately low (per the user's own reasoning, 2026-08-15): this is JUST a
# gate, not a calibration target -- a player who clears it with a thin sample
# still gets pulled toward the positional prior by apply_shrinkage, and toward
# zero further still by calendar_recency_weight if they're old-but-inside-cutoff,
# so the gate doesn't need to protect against thin/stale data itself, only against
# literal single-minute-cameo data-artifact noise. 10.0 ~= a genuine late
# substitute appearance, not swept/calibrated (no meaningful calibration target
# exists for "how thin is too thin to even try").
PLAYER_RATING_MIN_TEAM_WEIGHTED_MINUTES_TO_BE_A_CANDIDATE = 10.0

# Blend-weight resolution (FEATURE-011_REQUIREMENTS.md, Blend, resolved 2026-07-30;
# window made season-blind 2026-08-11, BUG-010; coverage-only redesign 2026-08-16,
# BUG-012 root cause #4 v2). Minutes a player needs, across their own last
# PLAYER_RATING_PAST_MATCH_WINDOW_SIZE appearances (recency, not career totals),
# before they count as a FULLY-confident individual signal for the data-coverage
# score below -- the point a per-player confidence ramp saturates to 1.0, not a
# binary qualify/disqualify cutoff anymore (see player_trust_score). A player below
# this contributes partial credit (their own minutes scaled by minutes/this value),
# not zero.
#
# Renamed and repurposed 2026-08-16 (was PLAYER_RATING_MIN_MINUTES_RECENT_WINDOW,
# a binary >= cutoff at 300.0): real backfill validation found that once
# player_trust_score dropped its churn factor entirely (coverage-only, see BUGS.md
# BUG-012), a binary cutoff this low meant nearly every real roster player cleared
# it and counted at FULL value regardless of whether they had 305 or 3000 minutes --
# mean weight_attack collapsed to ~0.07-0.11 (nearly all player-level trust) in
# EVERY league uniformly, since coverage essentially never failed to saturate.
# A continuous ramp fixes that by making a thin player's contribution shrink
# TWICE over (both their raw minutes and their confidence multiplier are small),
# so a squad of mostly fringe/rotation players lands meaningfully below full
# trust even though every individual technically "has some data."
#
# Value calibrated 2026-08-17 via a real 7-candidate sweep (400/500/700/900/
# 1200/1500/2000, 5 leagues x 2024/2025, pooled Brier/bias/ROI against
# poisson_v4_3 -- see BUGS.md BUG-012 and MODEL_VERSION_LOG.md poisson_v4_4 for
# full numbers). 1200 is a genuine local minimum, not a plateau stopped at
# early: Brier improves monotonically from 400 up through 1200 (0.6075 ->
# 0.5964), then gets WORSE again at 1500/1200 (0.5977) and 2000 (0.5998) --
# confirmed by sweeping past the first "good enough" value, not assumed.
# Notably exceeds the theoretical max a player can accumulate in one `window`
# (10 games x ~90 min = 900) -- no individual player's confidence ever
# actually reaches 1.0 at this setting (it tops out around 900/1200 = 0.75),
# but the aggregate team-level score (summed across the whole roster) still
# reaches 1.0 for well-tracked squads. This is unusual but was left as-is
# once the real numbers supported it, rather than artificially capped at 900
# to "look right" -- see the note above the constant's own docstring
# reference in player_trust_score for the mechanism.
PLAYER_RATING_COVERAGE_SATURATION_MINUTES = 1200.0

# {league: {"attack": w, "defense": w}} -- a coarse override that forces EVERY team in
# that league to the same weight for that component, taking precedence over each
# team's computed default. `w` is this file's usual blend convention (0=pure player,
# 1=pure team) -- same as blend()/soccer_player_team_strength.weight_attack. Empty by
# default; add entries only with a documented reason, same convention as WC's
# FIFA_BLEND_OVERRIDES.
PLAYER_RATING_LEAGUE_WIDE_BLEND_WEIGHT_OVERRIDE = {}

# Cross-league attack-rate conversion for prior_season_attack_rate/blend_prior_season_
# attack (2026-08-03, FEATURE-011/BUG-010's cross-league player-history design) -- a
# goal in a weaker division doesn't predict the same Serie A output, so a prior-
# season rate sourced from a different league is scaled by this factor before being
# blended in. Deliberately GOALS-based, not xG: TheStatsAPI's xG coverage is real for
# Serie A and Championship, but Serie B/2. Bundesliga/Ligue 2/LaLiga 2 all return
# expected_goals=0.0 on every single shot (checked directly, incl. players with
# multiple shots and real goals) despite the competition metadata's xg_available=true
# claiming otherwise -- goals is the one signal available uniformly.
#
# Serie B's 0.663 is EMPIRICALLY measured (not guessed, per this project's "verify
# before building" discipline): 82 players with >=300 minutes in BOTH Serie A and
# Serie B (any season) compared to THEMSELVES across leagues -- 0.0956 goals/90 in
# Serie A vs 0.1442 in Serie B, pooled by minutes. Cross-checked against
# compute_wc_team_strength.py's separate, subjective LEAGUE_FACTORS table, which
# already had "Serie B": 0.60 for a different purpose (predicting INTERNATIONAL
# scoring from club form) -- close agreement, a reasonable sanity check though not
# the same question. A league with NO entry here is excluded from the prior-season
# blend entirely, never assumed equal to Serie A (see player_prior_season_attack_rate).
#
# 2026-08-10 (multi-league expansion): Premier League/Bundesliga/La Liga/Ligue 1
# added at 1.0 -- an ASSUMPTION (peer top-5 leagues, same default as Serie A's own
# entry), not an empirical derivation like Serie B's 0.663 above.
#
# 2026-08-12 (BUG-010 continued): the GATE (exclude a league with no entry here,
# rather than assume Serie-A-equivalent) now also applies to defense in
# load_team_players -- a promoted team's defense rating was being built almost
# entirely from unadjusted feeder-league form. The numeric factor itself still
# only scales attack (it's an attack-specific measurement); defense from a
# league that DOES have an entry is included unscaled. See load_team_players'
# docstring for the full mechanism.
#
# BUG-013 (2026-08-14): the four new leagues' feeder divisions (Championship,
# 2. Bundesliga, LaLiga 2, Ligue 2) had NO entry at all -- not even a 1.0
# placeholder -- so every game a promoted team's players had played in their
# feeder division was excluded ENTIRELY (not scaled, dropped), collapsing some
# players' rating input to a single-match sample. Found via 1. FC Koeln
# (promoted from 2. Bundesliga), whose player-level attack rating went so low it
# tripped BUG-014's unguarded spread-stretch negative. Fixed the same way Serie
# B's 0.663 was measured (not guessed): players with >=300 minutes in BOTH the
# top-flight league and its feeder division (any season), own goals/90 in each,
# pooled by minutes, ratio = top-flight rate / feeder rate. All four land in a
# tight, plausible band consistent with Serie B's own 0.663 -- a reasonable
# cross-check that the methodology itself is sound:
#   Bundesliga/2. Bundesliga:      152 qualifying players, 0.1143/0.1906 = 0.5999
#   Premier League/Championship:   162 qualifying players, 0.0931/0.1402 = 0.6643
#   La Liga/LaLiga 2:              196 qualifying players, 0.0842/0.1294 = 0.6512
#   Ligue 1/Ligue 2:               159 qualifying players, 0.1016/0.1372 = 0.7408
# (sample sizes here are larger than Serie B's original 82-player measurement.)
# See test_every_registered_feeder_division_has_a_cross_league_adjustment_entry
# in tests/test_compute_club_player_strength.py -- a generic regression test
# (keyed off core.leagues.LEAGUES' lower_division field, not hardcoded to these
# 4) that would have caught this gap immediately and catches it for any future
# league added the same way.
PLAYER_RATING_CROSS_LEAGUE_GOAL_ADJUSTMENT = {
    "Serie A": 1.0,
    "Serie B": 0.663,
    "Premier League": 1.0,
    "Bundesliga": 1.0,
    "La Liga": 1.0,
    "Ligue 1": 1.0,
    "Championship": 0.6643,
    "2. Bundesliga": 0.5999,
    "LaLiga 2": 0.6512,
    "Ligue 2": 0.7408,
}

# Team-level attack/defense blend between actual-goals-based and xG-based sources
# (2026-08-02/2026-08-05, BUG-009's mismatch-size-compression diagnosis). 0.0=pure
# goals (matches poisson_v3 exactly), 1.0=pure xG (DEFAULT, a true no-op matching the
# fix that cleared the Model Calibration success criterion -- a small sample of actual
# goals proved too noisy), values in between blend the two. Promoted from a bare
# function default to a named constant 2026-08-06 (MODEL_TUNING_PARAMETERS.md) so it's
# discoverable alongside the other tunable knobs instead of buried in two function
# signatures. See team_level_lambda's docstring for the full derivation.
TEAM_RATING_XG_V_GOALS_BLEND = 1.0

# Spreads team-level xG ratings' cross-team dispersion back out, toward (but not all
# the way to) actual-goals-level dispersion (2026-08-07, BUG-009's compression
# finding continued: xG has LESS team-to-team spread than actual goals by
# construction, which compresses win probabilities toward a coin flip specifically
# on the biggest favorites/underdogs -- worst in matches like a bottom-table team
# hosting a top-table one). For each of get_team_xg_ratings' four fields
# (home/away attack/defense), recenters on that field's own league-wide mean at the
# same before_date, then scales the deviation from that mean by the relevant factor
# below:
#     stretched = league_mean + (raw - league_mean) * factor
# 1.0 is an exact no-op (today's pre-2026-08-07 shape). The original 2026-08-07 sweep
# (1.0/1.3/1.66/2.0, ad hoc, both Serie A seasons) tested ONE shared factor across all
# four fields: bottom6-vs-other probability gap shrinks monotonically and ROI improves
# in BOTH seasons as the factor increases -- notably different from every other
# compression fix tried (blend, this constant's sibling above) which always showed one
# season winning while the other lost -- but pooled away-side bias grows with the
# stretch and breaches the +/-0.01-0.02 Model Calibration target at 1.66 in one season
# and at 2.0 in both. 1.3 was the largest value tested that stays inside the target in
# both seasons, hence that shared default. See BUGS.md, BUG-009, 2026-08-07 addendum
# for the full sweep and the combined-with-blend test (negative result -- don't
# combine the two).
#
# Split into separate attack/defense constants 2026-08-12 (BUG-010 continued):
# measuring xG-vs-actual-goals dispersion separately by side shows defense is MORE
# compressed than attack (CV ratio goals/xG averaged ~1.64 for defense vs ~1.39 for
# attack, across home/away, La Liga 2025 -- a single shared factor was calibrated on
# attack-shaped data and applied uniformly, under-correcting defense). Both start at
# the prior shared value (1.3) -- an unchanged-behavior starting point, not yet
# re-calibrated separately; that needs its own sweep, same discipline as the
# original 1.0/1.3/1.66/2.0 sweep, before either value should move.
TEAM_RATING_XG_SPREAD_STRETCH_ATTACK = 1.3
TEAM_RATING_XG_SPREAD_STRETCH_DEFENSE = 1.3

# Team-xG lookback structure (2026-08-12, BUG-010 home-bet / Pattern A work).
# Defaults keep today's shipped behavior (flat last-N mean, no schedule adjust).
# Live poisson_v4 still uses these defaults until a method-tagged backfill proves
# the structural change on home-bet calib / gap_bf / bias — do NOT flip production
# defaults from a single case study.
#
# TEAM_RATING_XG_WINDOW_DECAY: recency weights on get_team_xg_ratings, same
# convention as core.poisson_model.get_team_ratings (most recent weight 1.0, then
# decay, decay^2, ...). 1.0 = plain average (shipped). Note: goals-path
# TEAM_PAST_MATCH_WINDOW_DECAY is separate and already 1.0; v4 is pure xG so only
# THIS constant matters for team form until team_xg_v_goals_blend leaves 1.0.
TEAM_RATING_XG_WINDOW_DECAY = 1.0
#
# TEAM_RATING_XG_OPPONENT_ADJUST: when True, each past match's team xG / xGA is
# scaled by opponent quality *as of that past match date* (point-in-time raw
# opponent rating, never the post-adjust rating — avoids circularity):
#   attack_adj = xG_for * (league_mean_opp_defense / opp_defense)
#   defense_adj = xGA    * (league_mean_opp_attack  / opp_attack)
# So 0.4 xG at a stingy defense counts for more than 0.4 xG at a sieve. False
# (default) = raw xG averages, today's behavior.
TEAM_RATING_XG_OPPONENT_ADJUST = False

# Player-level counterpart to the team-level stretch above (2026-08-12, BUG-010
# continued): player-level attack/defense ratings show the SAME kind of compression
# relative to team-level xG ratings (measured on La Liga 2025: player-level away-
# attack CV=0.157 vs team-level xG away-attack CV=0.246; player-level home-defense
# CV=0.082 vs team-level xG home-defense CV=0.200 -- defense again more compressed
# than attack, and more severely than at the team level). Applied the same way, in
# compute(), recentering each team's raw player-level ra/rd around the league's own
# attack_mean/defense_mean before the home/away unit conversion (a pure linear
# rescale, so it doesn't touch relative dispersion -- the stretch has to happen
# before that step, not after).
#
# ATTACK calibrated 2026-08-12 (5 leagues, season 2025, all-up pooled): swept
# 1.0/1.3/1.6/2.0/2.5/3.0. Bias vs Betfair Exchange never breached the +/-0.01-0.02
# target even at 3.0 (away bias only reached +0.0154) -- no hard ceiling from that
# criterion. But Brier degraded monotonically across the whole range (0.5689 at 1.0
# -> 0.5728 at 3.0) while ROI improvement plateaued after 2.0 (all three EV
# thresholds sat in a tight -9.3% to -10.1% band from 2.0 through 3.0, vs
# -11.8%/-11.1%/-10.5% at 1.0) -- past 2.0, further stretch bought steady Brier
# cost for no further ROI gain. 2.0 shipped as the value that captures nearly all
# the ROI improvement while Brier degradation is still modest (+0.001 vs baseline).
#
# DEFENSE not yet calibrated -- still 1.0 (true no-op) pending its own sweep, same
# discipline, with attack held at its now-locked 2.0.
PLAYER_RATING_SPREAD_STRETCH_ATTACK = 2.0
PLAYER_RATING_SPREAD_STRETCH_DEFENSE = 1.0

_MID_CODES = {"m", "mf", "cm", "dm", "am", "cdm", "cam", "rm", "lm", "mid"}
_DEF_CODES = {"d", "df", "cb", "lb", "rb", "wb", "rwb", "lwb", "def"}
_FWD_CODES = {"f", "fw", "fwd", "st", "cf", "ss", "rw", "lw"}


def normalize_position(pos):
    if not pos:
        return None
    p = pos.strip().lower()
    if "goal" in p or p in {"gk", "g"}:
        return "GK"
    if "midfield" in p or "winger" in p or p in _MID_CODES:
        return "MID"
    if "back" in p or "defen" in p or p in _DEF_CODES:
        return "DEF"
    if "forward" in p or "striker" in p or "attack" in p or p in _FWD_CODES:
        return "FWD"
    return None


def load_team_players(conn, team_ids, before_date, attack_xg_v_goals_source="xg",
                      defense_xg_v_goals_source="xga",
                      window_size=PLAYER_RATING_PAST_MATCH_WINDOW_SIZE,
                      half_life_days=PLAYER_RATING_RECENCY_HALF_LIFE_DAYS,
                      cutoff_days=PLAYER_RATING_RECENCY_CUTOFF_DAYS,
                      league_strength=None, min_date=None):
    """Build one per-player-per-TEAM rating entry from each player's own last
    `window_size` appearances (2026-08-06, FEATURE-011 Follow-up B) -- replaces the
    old flat season-to-date sum (see MODEL_PIPELINE_OVERVIEW.md's original section 2,
    now out of date) and the separate blend_prior_season_attack/PRIOR_SEASON_DISCOUNT
    mechanism it needed to compensate for thin-current-season players (both retired
    the same day this landed).

    before_date: ISO date string, restricting to matches strictly before it (BUG-008
    no-lookahead discipline -- required now, not optional, since without a window
    bound at all a season-blind query has no other stopping point).

    SEASON-BLIND: the window reaches back across a season boundary the same way it
    reaches back across a matchday -- no special casing, no separate discount layered
    on top of decay. It also follows the PLAYER across a team or league change (their
    last N appearances, wherever they happened), not just the current team's own
    matches -- so a just-transferred player isn't cold-started back to zero the way a
    team-scoped window would leave them.

    ROSTER MEMBERSHIP: since "current season roster" no longer bounds anything, a
    player is attributed to whichever team their SINGLE most recent appearance
    (strictly before before_date) was for -- simple, well-defined, and season-blind,
    without attempting Follow-up A's fuller roster/lineup-projection scope (still
    unbuilt; see BUGS.md FEATURE-011). A player whose most recent appearance wasn't
    for a team in `team_ids` doesn't appear in the result at all (e.g. they left the
    league, or their most recent team isn't one being computed for right now) --
    NOR does a player whose calendar-decayed weighted minutes at their attributed
    team (within cutoff_days) fall below PLAYER_RATING_MIN_TEAM_WEIGHTED_MINUTES_
    TO_BE_A_CANDIDATE (2026-08-11, performance; converted from count-based to
    calendar-based 2026-08-15, BUG-012 root cause #3/v4_3 -- see the candidate-
    narrowing step below): they haven't featured recently enough, in real elapsed
    time, to still be part of what the team's actually been doing.

    min_date: optional ISO date lower bound -- when given, the window additionally
    can't reach earlier than this (e.g. a season's start date), for A/B-comparing a
    season-scoped window against the season-blind default. None (default) means truly
    season-blind: reach back as far as necessary to fill the window.

    Rates are computed from decay-weighted summed totals (goals/xg over minutes), not
    by averaging each match's own per-90 rate -- same "sum before rate" reasoning as
    before (a single sub appearance of a few minutes would otherwise produce a wildly
    noisy per-match rate that a simple average wouldn't smooth out), now with each
    game additionally weighted by calendar_recency_weight(match_date, before_date) --
    an actual elapsed-time decay (BUG-012, 2026-08-14), not decay**rank (a game's
    position in the top-`window_size` list) as this used to work: two games equally
    "recent" by rank could be months apart in reality (e.g. a team just promoted, or
    a player back from injury), which decay**rank couldn't tell apart. `half_life_days`/
    `cutoff_days` default to PLAYER_RATING_RECENCY_HALF_LIFE_DAYS/_CUTOFF_DAYS, set
    now the real, shipped Stage 2 values (v4_2, 2026-08-15 -- half_life=120d,
    cutoff=180d). The candidate-narrowing gate below (v4_3, same day) is ALSO
    calendar-bound now, via these same parameters -- see PLAYER_RATING_MIN_TEAM_
    WEIGHTED_MINUTES_TO_BE_A_CANDIDATE's own comment for why that gate was
    converted from the old count-based "team's last window_size matches" rule.

    attack_xg_v_goals_source: "xg" (default) uses real xG when at least one game in
    the window has it, falling back to goals for every game in the window otherwise
    (never mixing units within one player's rate) -- matching
    compute_wc_team_strength.py's fallback. "goals" always uses actual goals.

    defense_xg_v_goals_source: "xga" (default) uses club_xga_per90 (the opposing
    team's xG that match) when a game has it, falling back to club_ga_per90 (actual
    goals conceded) otherwise. "actual" forces club_ga_per90 always.

    Cross-league adjustment (`league_strength`, defaults to
    PLAYER_RATING_CROSS_LEAGUE_GOAL_ADJUSTMENT): a game played in a league with NO
    factor entry is excluded entirely -- from BOTH attack and defense (goal/xg
    numerator, goals-conceded numerator, and the minutes denominator for that one
    game), never assumed Serie-A-equivalent. Found live 2026-08-12 (BUG-010,
    Real Oviedo hosting Real Madrid): before this, the exclusion only applied to
    attack, so a promoted team's defense rating was built almost entirely from
    UNADJUSTED feeder-league form (e.g. one player: attack_minutes=90 -- their
    real top-flight debut only -- vs defense_minutes=891, nearly the whole window,
    from Segunda División) while attack correctly stayed thin and got shrunk
    toward the league average. The GATE now applies symmetrically. The numeric
    `factor` itself, however, still only SCALES attack -- Serie B's real `0.663`
    was empirically measured from players' own goal-SCORING rate specifically
    (see this constant's own comment) and has no established meaning for goals
    conceded; naively reusing it as a defense multiplier would be a guess dressed
    as a calibration, not an actual fix, so a calibrated league's games are
    included on defense UNSCALED (effectively factor=1.0) until a real
    defense-specific measurement exists. `attack_minutes` and `defense_minutes`
    can still differ even post-fix -- not from league-gating anymore, but from a
    game missing its own `club_ga_per90`/`club_xga_per90` value while still
    having usable attack data -- so both are still returned separately
    (decay-weighted, NOT raw minutes) rather than one shared `minutes` field.

    Each game is attributed to the team the player actually played for IN THAT MATCH
    (derived from venue + soccer_matches.home/away_team_id), NOT soccer_players.team_id
    (their most-recently-seen team, per add_player) -- confirmed necessary for real
    once this ran across the full 20-team Serie A (29 players, 463 rows misattributed
    when this used soccer_players.team_id).
    """
    league_strength = PLAYER_RATING_CROSS_LEAGUE_GOAL_ADJUSTMENT if league_strength is None else league_strength
    cur = conn.cursor()

    # Narrow to a candidate player pool BEFORE the full history fetch (2026-08-11,
    # performance -- found while validating BUG-010: this query used to have no
    # team/player filter at all, pulling the ENTIRE soccer_player_stats table on
    # every single call, which became the dominant backfill cost once the
    # multi-league expansion grew that table well past what this was originally
    # written against, 15+ of ~23 seconds per 30 compute() calls in a live
    # profile). Calendar-bound now (2026-08-15, BUG-012 root cause #3/v4_3) --
    # was "team's own last window_size matches" (count-based, fixture-density-
    # sensitive: a team squeezing many matches into a short calendar span could
    # exclude a player who's still genuinely recent in real elapsed time; a sparse
    # early-season schedule could still reach back months). Everything within
    # cutoff_days of before_date is a cheap, wide candidate net for this query's
    # own performance purpose only -- the ACTUAL gate (weighted minutes at the
    # player's attributed team) is applied below, once each candidate's real
    # current team is known.
    team_placeholders = ",".join("?" * len(team_ids))
    team_id_set = set(team_ids)
    params = [before_date] + list(team_ids) + list(team_ids)
    date_filter = ""
    # cutoff_days can be an intentionally huge near-no-op sentinel (Stage 1
    # back-compat, other tests/callers' explicit near-no-op overrides) --
    # timedelta can't represent that many days, so skip the SQL-level lower bound
    # in that regime; calendar_recency_weight's own 0.0-past-cutoff floor still
    # applies below regardless, this only affects whether the DB query itself is
    # narrowed early.
    if cutoff_days < 1_000_000:
        cutoff_date = (date.fromisoformat(str(before_date)[:10]) - timedelta(days=cutoff_days)).isoformat()
        date_filter = " AND match_date >= ?"
        params.append(cutoff_date)
    cur.execute(f"""
        SELECT match_id, home_team_id, away_team_id, match_date FROM soccer_matches
        WHERE match_date < ?{date_filter}
          AND (home_team_id IN ({team_placeholders}) OR away_team_id IN ({team_placeholders}))
    """, params)
    candidate_match_ids = set()
    match_date_by_id = {}
    for match_id, home_id, away_id, match_date_val in cur.fetchall():
        if home_id in team_id_set or away_id in team_id_set:
            candidate_match_ids.add(match_id)
            match_date_by_id[match_id] = match_date_val
    if not candidate_match_ids:
        return {tid: [] for tid in team_ids}

    match_placeholders = ",".join("?" * len(candidate_match_ids))
    cur.execute(f"""
        SELECT DISTINCT player_id FROM soccer_player_stats
        WHERE match_id IN ({match_placeholders}) AND venue IS NOT NULL
    """, list(candidate_match_ids))
    candidate_player_ids = [row[0] for row in cur.fetchall()]
    if not candidate_player_ids:
        return {tid: [] for tid in team_ids}

    # The real gate (BUG-012 root cause #3/v4_3): each candidate's calendar-decayed
    # weighted minutes AT EACH of team_ids specifically, summed across every
    # qualifying (within-cutoff) appearance for that team -- checked below against
    # the team the player is actually attributed to, not against whichever team's
    # floor they happen to clear (a thin debut at a new team isn't "rescued" by a
    # big history at the old one -- see PLAYER_RATING_MIN_TEAM_WEIGHTED_MINUTES_
    # TO_BE_A_CANDIDATE's own comment).
    cur.execute(f"""
        SELECT s.player_id, s.match_id, s.minutes_played, s.venue,
               m.home_team_id, m.away_team_id
        FROM soccer_player_stats s
        JOIN soccer_matches m ON m.match_id = s.match_id
        WHERE s.match_id IN ({match_placeholders}) AND s.venue IS NOT NULL
    """, list(candidate_match_ids))
    weighted_minutes_at_team = {}
    for player_id, match_id, minutes, venue, home_id, away_id in cur.fetchall():
        match_team_id = home_id if venue == "home" else away_id
        if match_team_id not in team_id_set:
            continue
        w = calendar_recency_weight(match_date_by_id[match_id], before_date, half_life_days, cutoff_days)
        key = (match_team_id, player_id)
        weighted_minutes_at_team[key] = weighted_minutes_at_team.get(key, 0.0) + w * (minutes or 0)

    player_placeholders = ",".join("?" * len(candidate_player_ids))
    sql = f"""
        SELECT s.player_id, p.position, s.minutes_played, s.goals, s.xg,
               s.club_ga_per90, s.club_xga_per90,
               s.venue, m.home_team_id, m.away_team_id, m.match_date, m.league
        FROM soccer_player_stats s
        JOIN soccer_players p ON p.player_id = s.player_id
        JOIN soccer_matches m ON m.match_id = s.match_id
        WHERE s.match_id IS NOT NULL AND s.venue IS NOT NULL AND m.match_date < ?
          AND s.player_id IN ({player_placeholders})
    """
    params = [before_date] + candidate_player_ids
    if min_date is not None:
        sql += " AND m.match_date >= ?"
        params.append(min_date)
    sql += " ORDER BY s.player_id, m.match_date DESC"
    cur.execute(sql, params)

    by_player = {}
    for player_id, position, minutes, goals, xg, club_ga90, club_xga90, venue, home_id, away_id, match_date, m_league in cur.fetchall():
        match_team_id = home_id if venue == "home" else away_id
        by_player.setdefault(player_id, []).append({
            "pos": normalize_position(position), "minutes": minutes or 0, "goals": goals or 0,
            "xg": xg, "club_ga90": club_ga90, "club_xga90": club_xga90,
            "team_id": match_team_id, "match_date": match_date, "league": m_league,
        })

    by_team = {tid: [] for tid in team_ids}
    for player_id, games in by_player.items():
        # SQL already ORDER BY match_date DESC per player -- games[0] is most recent.
        current_team = games[0]["team_id"]
        if current_team not in team_id_set:
            continue
        # The actual candidate gate (BUG-012 root cause #3/v4_3): checked against
        # weighted minutes at THIS specific (attributed) team, not just "cleared
        # some team's floor" -- see weighted_minutes_at_team's own comment above.
        if weighted_minutes_at_team.get((current_team, player_id), 0.0) < PLAYER_RATING_MIN_TEAM_WEIGHTED_MINUTES_TO_BE_A_CANDIDATE:
            continue
        window = games[:window_size]
        has_xg = attack_xg_v_goals_source == "xg" and any(g["xg"] is not None for g in window)

        attack_num = attack_den = 0.0
        ga_num = ga_den = 0.0
        xga_num = xga_den = 0.0
        for g in window:
            w = calendar_recency_weight(g["match_date"], before_date, half_life_days, cutoff_days)
            if w == 0.0:
                continue
            factor = league_strength.get(g["league"])
            if factor is None:
                # BUG-010, 2026-08-12: this gate now excludes the game from BOTH
                # sides, not just attack -- see load_team_players' docstring for why
                # a defense-side equivalent of `factor` isn't applied even for
                # leagues that DO have one (Serie A/Serie B): 0.663 was measured
                # from players' own goal-SCORING rate specifically and has no
                # established meaning for goals conceded, so scaling defense by it
                # would be an unvalidated guess, not a calibration. The gate itself
                # (never assume Serie-A-equivalent for an uncalibrated league) is
                # the part that generalizes to defense; the numeric factor is not.
                continue
            # When has_xg (ANY game in the window has real xg), a game WITHOUT
            # its own xg contributes 0, not its goals -- never mix units within
            # one player's rate. Only fall back to goals per-game when the whole
            # window has no xg at all (has_xg False).
            goal_val = (g["xg"] or 0) if has_xg else g["goals"]
            attack_num += w * factor * (goal_val or 0)
            attack_den += w * g["minutes"]
            if g["minutes"]:
                if g["club_ga90"] is not None:
                    ga_num += w * g["minutes"] * g["club_ga90"]
                    ga_den += w * g["minutes"]
                if g["club_xga90"] is not None:
                    xga_num += w * g["minutes"] * g["club_xga90"]
                    xga_den += w * g["minutes"]

        if attack_den <= 0:
            continue

        attack_rate = attack_num / attack_den * 90
        if defense_xg_v_goals_source == "xga" and xga_den > 0:
            club_ga_per90, defense_minutes = xga_num / xga_den, xga_den
        elif ga_den > 0:
            club_ga_per90, defense_minutes = ga_num / ga_den, ga_den
        else:
            club_ga_per90, defense_minutes = None, 0.0

        by_team[current_team].append({
            "player_id": player_id,
            "pos": games[0]["pos"],
            "attack_rate": attack_rate,
            "attack_minutes": attack_den,
            "club_ga_per90": club_ga_per90,
            "defense_minutes": defense_minutes,
        })
    return by_team


def positional_priors(by_team, field, weight_field):
    num, den = {}, {}
    for players in by_team.values():
        for p in players:
            pos, val, mins = p["pos"], p.get(field), p.get(weight_field)
            if pos and val is not None and mins:
                num[pos] = num.get(pos, 0.0) + mins * val
                den[pos] = den.get(pos, 0.0) + mins
    return {pos: num[pos] / den[pos] for pos in num}


# field -> which decay-weighted minutes field measures its credibility for shrinkage/
# aggregation weighting. Both fields are always present now that load_team_players
# itself is windowed/decayed (2026-08-06) -- no more raw-vs-combined-minutes distinction
# to fall back between (that distinction existed only to compensate for the old flat
# season-to-date sum via blend_prior_season_attack, retired the same day this landed).
# attack_minutes and defense_minutes can still legitimately differ per player (a game
# in an uncalibrated league is excluded from attack_minutes but not defense_minutes --
# see load_team_players).
SHRINKAGE_WEIGHT_FIELD = {"attack_rate": "attack_minutes", "club_ga_per90": "defense_minutes"}

# field -> the key apply_shrinkage stores that player's shrink weight under
# (BUG-009 team-credibility de-shrink, 2026-08-19) -- see apply_shrinkage's
# docstring and team_credibility() below.
SHRINKAGE_WEIGHT_OUTPUT_FIELD = {"attack_rate": "_shrink_weight_attack", "club_ga_per90": "_shrink_weight_defense"}


def apply_shrinkage(by_team, k_minutes=PLAYER_RATING_MINUTES_TO_HALF_TRUST_OWN_RATE_OVER_LEAGUE_AVERAGE):
    """Shrinks each player's attack_rate/club_ga_per90 toward their position's
    league-wide average -- `w*val + (1-w)*prior`, where `w = mins/(mins+k_minutes)`
    is how much a player's own rate counts vs. the position prior at their current
    credibility (decayed minutes).

    Also stores that per-player `w` under SHRINKAGE_WEIGHT_OUTPUT_FIELD (BUG-009,
    2026-08-19) -- previously computed and discarded. team_credibility() below
    aggregates it into one team-level credibility score, the input to the
    team-specific de-shrink (PLAYER_RATING_USE_TEAM_CREDIBILITY_DESHRINK) that
    replaces the flat, one-constant-for-every-team spread_around_mean stretch.
    Storing it here (rather than recomputing from mins/k_minutes downstream)
    keeps a single source of truth for "how much was this specific player
    actually shrunk," including whatever k_minutes value this call used."""
    for field in ("attack_rate", "club_ga_per90"):
        weight_field = SHRINKAGE_WEIGHT_FIELD[field]
        output_field = SHRINKAGE_WEIGHT_OUTPUT_FIELD[field]
        prior = positional_priors(by_team, field, weight_field=weight_field)
        for players in by_team.values():
            for p in players:
                pos, val = p["pos"], p.get(field)
                mins = p.get(weight_field)
                if pos in prior and val is not None and mins:
                    w = mins / (mins + k_minutes)
                    p[output_field] = w
                    p[field] = w * val + (1 - w) * prior[pos]


def team_credibility(players, field, position_weights):
    """Credibility-weighted average of a team's players' shrink weights (from
    apply_shrinkage, SHRINKAGE_WEIGHT_OUTPUT_FIELD) for one field ("attack_rate"
    or "club_ga_per90") -- weighted the SAME way raw_team_strength() weights
    that field's VALUE for team aggregation (minutes * position weight), so the
    credibility score reflects each player's actual contribution to the team's
    raw rate, not a flat per-player average.

    Returns None if no player has both a shrink weight and a positive
    aggregation weight (mirrors raw_team_strength's own "not enough signal"
    fallback shape)."""
    weight_field = SHRINKAGE_WEIGHT_FIELD[field]
    output_field = SHRINKAGE_WEIGHT_OUTPUT_FIELD[field]
    num = den = 0.0
    for p in players:
        pos = p["pos"]
        sw = p.get(output_field)
        mins = p.get(weight_field)
        if pos is None or sw is None or not mins:
            continue
        w = mins * position_weights.get(pos, 0.0)
        if w <= 0:
            continue
        num += w * sw
        den += w
    return (num / den) if den > 0 else None


def spread_around_mean(raw, mean, factor, mode):
    """Single shared implementation for "stretch raw's distance from mean by
    factor" -- BUG-014 (2026-08-14): every call site used to duplicate this
    formula inline (team_level_lambda's xg_spread_stretch_attack/_defense,
    compute()'s player_spread_stretch_attack/_defense), and all shared the same
    defect: additive mode has no floor and can push a rate-like quantity (which
    can't be negative) past zero on an extreme input. Centralizing it here means
    the fix (switching a call site to multiplicative) happens in exactly one
    place per caller, not by hunting down every inline copy.

    mode="additive" (today's shipped behavior, a straight port of the old inline
    formula, not yet a bug fix on its own): `mean + (raw - mean) * factor`. Can
    go negative -- kept only so wiring every call site through this function is
    a verified no-op before any call site's actual behavior changes.

    mode="multiplicative" (the fix, rolled out one call site at a time to
    measure impact rather than all at once): `mean * (raw / mean) ** factor`.
    Structurally cannot go negative -- raw/mean stays positive whenever raw and
    mean are both positive, and any power of a positive number is positive.
    Only reaches exactly 0 if raw itself is already 0.

    raw/mean of None, or mean <= 0, returns raw unchanged (matches the None-
    guards every existing call site already had before this refactor)."""
    if raw is None or mean is None or mean <= 0:
        return raw
    if mode == "additive":
        return mean + (raw - mean) * factor
    if mode == "multiplicative":
        return mean * (raw / mean) ** factor
    raise ValueError(f"spread_around_mean: unknown mode {mode!r}")


def raw_team_strength(players):
    a_num = a_w = d_num = d_w = 0.0
    for p in players:
        pos = p["pos"]
        if pos is None:
            continue
        if p["attack_rate"] is not None:
            w = p["attack_minutes"] * PLAYER_RATING_POSITION_ATTACK_WEIGHTS.get(pos, 0.0)
            if w > 0:
                a_num += w * p["attack_rate"]
                a_w += w
        if p["club_ga_per90"] is not None:
            w = p["defense_minutes"] * PLAYER_RATING_POSITION_DEFENSE_WEIGHTS.get(pos, 0.0)
            if w > 0:
                d_num += w * p["club_ga_per90"]
                d_w += w
    raw_attack = (a_num / a_w) if a_w > 0 else None
    raw_defense = (d_num / d_w) if d_w > 0 else None
    return raw_attack, a_w, raw_defense, d_w


def player_season_minutes(conn, season):
    """{player_id: total minutes played in `season`, summed across ALL teams/matches.

    Deliberately NOT team-scoped -- a summer transfer's minutes at their PREVIOUS club
    count in full toward their data-coverage signal (no cross-club discount), which is
    the entire point of tracking player identity across a transfer (api_player_id)."""
    cur = conn.cursor()
    cur.execute("""
        SELECT player_id, SUM(minutes_played)
        FROM soccer_player_stats
        WHERE season = ? AND match_id IS NOT NULL
        GROUP BY player_id
    """, (season,))
    return {pid: mins or 0 for pid, mins in cur.fetchall()}


def team_roster_minutes(conn, team_id, season, before_date=None):
    """{player_id: minutes played AT team_id specifically in `season`} -- team-scoped,
    since this answers "how much of team_id's OWN production is this player part of,"
    not the player's overall total (that's player_season_minutes).

    before_date: optional ISO date string, restricting to matches strictly before it.
    Used two ways: with no before_date, this gives a fully-completed season's final
    roster (safe -- the whole season is in the past by construction). WITH a
    before_date, it gives a point-in-time-correct read of the CURRENT season's roster
    so far, for backtesting (see roster_as_of_date) -- no lookahead, since it only
    counts matches that had already happened."""
    cur = conn.cursor()
    sql = """
        SELECT s.player_id, SUM(s.minutes_played)
        FROM soccer_player_stats s
        JOIN soccer_matches m ON m.match_id = s.match_id
        WHERE s.season = ? AND s.match_id IS NOT NULL AND s.venue IS NOT NULL
          AND ((s.venue = 'home' AND m.home_team_id = ?)
               OR (s.venue = 'away' AND m.away_team_id = ?))
    """
    params = [season, team_id, team_id]
    if before_date is not None:
        sql += " AND m.match_date < ?"
        params.append(before_date)
    sql += " GROUP BY s.player_id"
    cur.execute(sql, params)
    return {pid: mins or 0 for pid, mins in cur.fetchall()}


def team_aggregated_recent_roster_minutes(conn, team_id, before_date, n=PLAYER_RATING_PAST_MATCH_WINDOW_SIZE,
                                          half_life_days=PLAYER_RATING_RECENCY_HALF_LIFE_DAYS,
                                          cutoff_days=PLAYER_RATING_RECENCY_CUTOFF_DAYS):
    """{player_id: calendar_recency_weight-decayed minutes played AT team_id,
    across team_id's own last `n` matches (season-blind, strictly before
    before_date)} -- the season-blind replacement (BUG-010, 2026-08-11) for
    team_roster_minutes(team_id, season - 1) as player_trust_score's coverage
    denominator (2026-08-16: no longer a churn "prior roster" reference, just a
    normalizing scale -- see player_trust_score's own docstring). Same window
    size/style as load_team_players' own rating window (not a separately-tuned
    constant) -- the point is measuring coverage over the SAME horizon the
    team-level and player-level ratings themselves already use, not an
    arbitrary different one.

    Each match's minutes are weighted by calendar_recency_weight(match_date,
    before_date) (BUG-012, 2026-08-14) rather than summed flat -- a match from
    5 months ago no longer counts the same as one from last week just because
    both fall inside the same last-`n`-matches window. half_life_days/
    cutoff_days default to the Stage 1 near-no-op constants (see their own
    comment); a weight of exactly 0.0 (past cutoff_days) drops that match's
    minutes from the sum entirely, same shape as load_team_players' own
    per-game gate.

    Team-scoped, same reasoning as team_roster_minutes: answers "how much of
    team_id's own recent production is this player part of.\""""
    cur = conn.cursor()
    cur.execute("""
        SELECT match_id, match_date FROM soccer_matches
        WHERE match_date < ? AND (home_team_id = ? OR away_team_id = ?)
        ORDER BY match_date DESC LIMIT ?
    """, (before_date, team_id, team_id, n))
    rows = cur.fetchall()
    if not rows:
        return {}
    match_date_by_id = dict(rows)
    match_ids = list(match_date_by_id)
    placeholders = ",".join("?" * len(match_ids))
    cur.execute(f"""
        SELECT s.match_id, s.player_id, s.minutes_played
        FROM soccer_player_stats s
        JOIN soccer_matches m ON m.match_id = s.match_id
        WHERE s.match_id IN ({placeholders}) AND s.venue IS NOT NULL
          AND ((s.venue = 'home' AND m.home_team_id = ?)
               OR (s.venue = 'away' AND m.away_team_id = ?))
    """, match_ids + [team_id, team_id])
    result = {}
    for match_id, player_id, minutes in cur.fetchall():
        w = calendar_recency_weight(match_date_by_id[match_id], before_date, half_life_days, cutoff_days)
        if w == 0.0:
            continue
        result[player_id] = result.get(player_id, 0.0) + w * (minutes or 0)
    return result


def players_aggregated_recent_minutes(conn, player_ids, before_date, n=PLAYER_RATING_PAST_MATCH_WINDOW_SIZE,
                                      half_life_days=PLAYER_RATING_RECENCY_HALF_LIFE_DAYS,
                                      cutoff_days=PLAYER_RATING_RECENCY_CUTOFF_DAYS):
    """{player_id: calendar_recency_weight-decayed minutes across THAT player's own
    last `n` appearances, ANY team/league (season-blind, strictly before
    before_date)} -- the season-blind replacement (BUG-010, 2026-08-11) for
    player_season_minutes(season) as player_trust_score's team-agnostic
    per-player signal. Deliberately NOT team-scoped, same reasoning as
    player_season_minutes: a just-transferred player's minutes at their
    previous club count in full toward their data-coverage signal. Only
    computes for the given player_ids (not every player in the DB), for
    efficiency -- callers already know which players they need this for.

    Each match's minutes are weighted by calendar_recency_weight(match_date,
    before_date) (BUG-012, 2026-08-14) rather than summed flat -- same
    reasoning and half_life_days/cutoff_days defaults as
    team_aggregated_recent_roster_minutes above (the two must decay
    consistently, since player_trust_score's coverage ratio compares them
    against each other)."""
    if not player_ids:
        return {}
    placeholders = ",".join("?" * len(player_ids))
    cur = conn.cursor()
    cur.execute(f"""
        SELECT s.player_id, m.match_date, s.minutes_played
        FROM soccer_player_stats s
        JOIN soccer_matches m ON m.match_id = s.match_id
        WHERE s.player_id IN ({placeholders}) AND s.match_id IS NOT NULL AND s.venue IS NOT NULL
          AND m.match_date < ?
        ORDER BY s.player_id, m.match_date DESC
    """, list(player_ids) + [before_date])
    by_player = {}
    for player_id, match_date, minutes in cur.fetchall():
        by_player.setdefault(player_id, []).append((match_date, minutes or 0))
    result = {}
    for pid, games in by_player.items():
        window = games[:n]
        result[pid] = sum(
            calendar_recency_weight(match_date, before_date, half_life_days, cutoff_days) * minutes
            for match_date, minutes in window
        )
    return result


def current_roster_player_ids(conn, team_id):
    """Players whose most-recently-seen team (soccer_players.team_id, see add_player's
    api_player_id-first identity resolution) is team_id right now -- the best available
    read of "who's on the roster today," not scoped to any particular season.

    LIVE use only. This field only ever holds the single latest known team, so it
    can't answer "who was on the roster as of a PAST date" -- for backtesting, use
    roster_as_of_date instead (derived from real match appearances, so it's actually
    point-in-time correct)."""
    cur = conn.cursor()
    cur.execute("SELECT player_id FROM soccer_players WHERE team_id = ?", (team_id,))
    return {row[0] for row in cur.fetchall()}


def roster_as_of_date(conn, team_id, season, before_date):
    """Point-in-time "who's on this roster" for BACKTESTING a past season, where
    current_roster_player_ids can't be trusted (soccer_players.team_id reflects TODAY's
    state, not what was true at before_date). Derived from real match appearances:

    1. Players who've played for team_id in `season`'s own matches so far (before
       before_date) -- the season's own transfer activity, as it becomes visible.
    2. If none yet (very early in the season, before any match has revealed summer
       transfer activity), falls back to team_id's last PLAYER_RATING_PAST_MATCH_
       WINDOW_SIZE matches, season-blind (BUG-010, 2026-08-11 -- was `season - 1`,
       same literal-season-label fragility as player_trust_score's own fix) -- an
       honest approximation: with no historical squad-list snapshot available, the
       first matchday or two assumes roster continuity until match evidence says
       otherwise. This understates day-one churn; a documented, bounded limitation
       (a season has ~38 matchdays; this affects the first one or two), not a
       lookahead leak.

    NOT for live use -- current_roster_player_ids is the better signal there (updated
    from a squad-list pull before the season begins, so it knows about a transfer
    before a ball is kicked; this only knows once the player actually plays)."""
    this_season = team_roster_minutes(conn, team_id, season, before_date=before_date)
    if this_season:
        return set(this_season.keys())
    # Deliberately near-no-op half_life/cutoff here (2026-08-15, BUG-012 follow-up):
    # this fallback's whole purpose is reaching back into the PREVIOUS season when
    # the current one has no matches yet -- exactly the moment a real, tightened
    # cutoff would otherwise return an empty roster (found while re-validating
    # tests after Stage 2 shipped real values). An empty roster here reads to
    # player_trust_score as "we know nothing about the current squad," collapsing
    # trust toward team-level right when this function's whole job is to say
    # "assume roster continuity." This call intentionally does NOT inherit the
    # shipped PLAYER_RATING_RECENCY_HALF_LIFE_DAYS/_CUTOFF_DAYS defaults.
    return set(team_aggregated_recent_roster_minutes(
        conn, team_id, before_date, half_life_days=1.0e12, cutoff_days=1.0e12).keys())


def _memoized(cache, key, fn):
    """cache=None (default everywhere) is a plain passthrough -- fn() runs every call,
    identical to pre-BUG-011 behavior. Pass a dict (created once by the caller, e.g. at
    the top of a backfill loop, and reused across calls) to memoize fn()'s result under
    key instead."""
    if cache is None:
        return fn()
    if key not in cache:
        cache[key] = fn()
    return cache[key]


def player_trust_score(conn, team_id, before_date, current_roster_ids=None, cache=None,
                       window=PLAYER_RATING_PAST_MATCH_WINDOW_SIZE,
                       half_life_days=PLAYER_RATING_RECENCY_HALF_LIFE_DAYS,
                       cutoff_days=PLAYER_RATING_RECENCY_CUTOFF_DAYS):
    """1.0 = fully trust the player-level lambda for this team; 0.0 = fully trust
    team-level. Coverage-only, continuous (2026-08-16, BUG-012 root cause #4 v3):
    of the CURRENT squad's tracked recent-window minutes, the fraction belonging
    to well-tracked players -- where "well-tracked" is now a smooth confidence
    ramp, not a binary cutoff. Each player's own last-`window`-appearance minutes
    (recency, not career totals -- team-agnostic, see
    players_aggregated_recent_minutes) get scaled by
    min(minutes / PLAYER_RATING_COVERAGE_SATURATION_MINUTES, 1.0) before being
    counted -- a thin player contributes little (both their raw minutes AND
    their confidence multiplier are small), a genuine regular contributes close
    to their full minutes. The denominator (team_total_minutes) is team_id's own
    last `window` matches' total decayed minutes -- not a churn reference, just
    a normalizing scale for "how much of the team's typical playing-time volume
    is covered by players we individually trust."

    Went through THREE prior designs before landing here, each replaced for a
    real, validated reason (full history: BUGS.md BUG-012):
    (1) churn-gated via a season-start anchor, then (2) via two-adjacent-count-
        windows -- both went stale in different ways (BUG-010; see
        MODEL_VERSION_LOG.md).
    (3) churn-gated via a single calendar-decayed window -- architecturally
        correct (no more wrong-clock bug), but real backfill (poisson_v4_4,
        2026-08-16) showed it systematically suppressed player-level trust
        across ALL 5 leagues uniformly, not just edge cases, because "current
        roster" and "team's own last `window` matches" are nearly the SAME
        underlying signal, so churn became almost undetectable by construction.
    (4) coverage-only with a BINARY per-player cutoff (drop churn entirely,
        keep the existing >=300min qualify/disqualify test) -- real backfill
        showed this swung the opposite way: mean weight_attack collapsed to
        ~0.07-0.11 (nearly ALL player-level trust) in every league uniformly,
        because a 300-minute bar is trivially cleared by nearly any real
        roster player, so coverage almost never failed to saturate near 1.0.
        Helped 3 leagues' Brier (Bundesliga/La Liga/Ligue 1) but hurt the two
        largest (Serie A/Premier League) -- pooled net negative.
    This version (a continuous ramp instead of a binary cutoff, still no churn
    factor) is meant to restore real team-to-team variation in the coverage
    score without reintroducing a churn mechanism, which has now caused three
    separate bugs and one broad regression across its various forms.

    Removing churn (as of design (4) above) is itself empirically supported,
    not just a simplification for its own sake: the poisson_v4_4 churn-vs-
    no-churn A/B (same lambdas, only the blend weight differed) showed that
    even the OLD churn-gated mechanism's already-team-heavy blend (~80%
    team-level on average) was still getting real value from its ~20%
    player-level minority share -- shifting further toward team-level hurt
    broadly. That implies coverage-driven trust (blend in player data whenever
    we have enough of it, regardless of whether the roster looks "stable") is
    closer to correct than the original churn-gated premise
    (FEATURE-011_REQUIREMENTS.md, Blend) that a stable, well-tracked squad has
    nothing to gain from the player signal.

    Season-blind (2026-08-11, BUG-010): the coverage denominator is team_id's
    own last `window` matches (team_aggregated_recent_roster_minutes), reaching
    back across a season boundary the same way load_team_players' rating window
    already does -- NOT a literal "last calendar season" lookup. Same window
    size as load_team_players' default (PLAYER_RATING_PAST_MATCH_WINDOW_SIZE),
    deliberately not a separately-tuned constant: this compares coverage over
    the SAME horizon the ratings themselves are computed over, not an arbitrary
    different one.

    current_roster_ids: optional override for "who's on the roster right now" --
    defaults to current_roster_player_ids() (the live signal). Backtesting a PAST
    season must pass a point-in-time squad instead (e.g. from roster_as_of_date), since
    the live default has no history.

    half_life_days, cutoff_days: passed through to both aggregation functions
    above (BUG-012, 2026-08-14).

    cache: optional dict (BUG-011) memoizing team_aggregated_recent_roster_minutes/
    players_aggregated_recent_minutes within a single compute() call (this function is called
    once per component -- attack, defense -- with identical inputs, so caching still
    avoids a duplicate query pair per team per matchday). The window shifts every
    matchday by design (matching load_team_players' own point-in-time-correct
    cost), so this doesn't cache ACROSS matchdays -- only within one compute() call
    at the same before_date. None (default) is a plain passthrough.

    NOTE: this is the INVERSE of the `w` convention used everywhere else in this file
    (blend(), soccer_player_team_strength.weight_attack/weight_defense -- there, 1.0
    means team-level). The inversion happens in exactly one place, resolve_blend_weight
    below, specifically so it isn't scattered across call sites.

    No recent-window history for team_id at all (e.g. backfill not run far back
    enough yet, or no matches exist before before_date at all) -> 0.0 (caller
    falls back fully to team-level; not a crash)."""
    aggregated_recent_roster_minutes = _memoized(
        cache, ("team_aggregated_recent_roster_minutes", team_id, before_date, window, half_life_days, cutoff_days),
        lambda: team_aggregated_recent_roster_minutes(conn, team_id, before_date, n=window,
                                                       half_life_days=half_life_days, cutoff_days=cutoff_days))
    team_total_minutes = sum(aggregated_recent_roster_minutes.values())
    if team_total_minutes <= 0:
        return 0.0

    current_roster = current_roster_ids if current_roster_ids is not None else current_roster_player_ids(conn, team_id)
    if not current_roster:
        return 0.0

    current_roster_minutes = _memoized(
        cache, ("players_aggregated_recent_minutes", team_id, before_date, window, half_life_days, cutoff_days),
        lambda: players_aggregated_recent_minutes(conn, current_roster, before_date, n=window,
                                                  half_life_days=half_life_days, cutoff_days=cutoff_days))

    coverage_minutes = sum(
        m * min(m / PLAYER_RATING_COVERAGE_SATURATION_MINUTES, 1.0)
        for m in (current_roster_minutes.get(p, 0) for p in current_roster)
    )
    return min(coverage_minutes / team_total_minutes, 1.0)


def resolve_blend_weight(conn, team_id, league, component, before_date,
                         current_roster_ids=None, cache=None,
                         window=PLAYER_RATING_PAST_MATCH_WINDOW_SIZE,
                         half_life_days=PLAYER_RATING_RECENCY_HALF_LIFE_DAYS,
                         cutoff_days=PLAYER_RATING_RECENCY_CUTOFF_DAYS):
    """Default per-team weight (`w`; 0=pure player, 1=pure team), with a league-wide
    override taking precedence per component (FEATURE-011_REQUIREMENTS.md, Blend).
    before_date: passed straight through to player_trust_score -- season-blind
    (BUG-010, 2026-08-11/12), no season label or separate season-start anchor
    involved. current_roster_ids: see player_trust_score -- pass a point-in-time
    squad (e.g. from roster_as_of_date) when backtesting a past season. cache: see
    player_trust_score (BUG-011). window: see player_trust_score -- compute()
    threads its own player_window_size through here so the coverage denominator
    stays pinned to the SAME horizon as the ratings it's gating, per
    player_trust_score's own docstring (not independently tunable from a call
    site). half_life_days,
    cutoff_days: passed straight through to player_trust_score (BUG-012,
    2026-08-14) -- compute() threads its own player_recency_half_life_days/
    player_recency_cutoff_days through here too, so the trust-score computation
    decays on the SAME calendar-time basis as load_team_players' rating itself,
    not an independently-set one."""
    override = PLAYER_RATING_LEAGUE_WIDE_BLEND_WEIGHT_OVERRIDE.get(league, {}).get(component)
    if override is not None:
        return override
    return 1.0 - player_trust_score(conn, team_id, before_date,
                                    current_roster_ids=current_roster_ids, cache=cache,
                                    window=window, half_life_days=half_life_days,
                                    cutoff_days=cutoff_days)


def _decay_weighted_mean(values, decay):
    """values[0] is most recent. decay=1.0 => plain mean. Skips None entries."""
    total_v = 0.0
    total_w = 0.0
    k = 0
    for v in values:
        if v is None:
            continue
        w = decay ** k
        total_v += v * w
        total_w += w
        k += 1
    if total_w <= 0:
        return None
    return total_v / total_w


def get_team_xg_ratings(conn, team_id, before_date, n=TEAM_PAST_MATCH_WINDOW_SIZE, league="Serie A",
                        decay=None, opponent_adjust=None, league_raw_means=None, cache=None):
    """xG-based counterpart to core.poisson_model.get_team_ratings -- same shape
    (home/away_attack, home/away_defense, home/away_n), derived from
    soccer_player_stats (this project's own xG/xGA data) instead of soccer_matches'
    actual scores.

    Lives entirely in this file, not core.poisson_model, so poisson_v3 is untouched;
    this is the team-form data source for THIS file's team_level_lambda / poisson_v4.

    A team's xG in a match = sum of that team's players' xg that match. A team's xGA
    in a match = the opposing team's xG that match -- already stored per player row
    as club_xga_per90 (backfill_club_xga.py), constant across a team's rows for a
    given match, so MAX() is a safe way to pull one copy of it per (match, venue).

    Coverage caveat: only reaches as far back as player-level stats exist (seasons
    2023+ as of 2026-08-02) -- soccer_matches itself goes back to season 2022, so an
    early-season match's N-game lookback can hit a real, silent gap before that.

    Team-filtered directly in SQL, not in Python (2026-08-12, performance).

    decay (default TEAM_RATING_XG_WINDOW_DECAY): recency weights, most-recent-first.
    1.0 preserves the historical flat last-N mean.

    opponent_adjust (default TEAM_RATING_XG_OPPONENT_ADJUST): schedule-strength
    correction using the opponent's RAW (unadjusted) xG rating as of each past
    match_date — point-in-time, no lookahead. league_raw_means supplies the
    league-mean baselines for the four fields; if omitted, baselines fall back to
    the mean of the opponents actually faced in this team's window (still
    well-defined for unit tests / single-team calls). Opponent ratings always use
    opponent_adjust=False so this cannot recurse into a circular definition.
    """
    if decay is None:
        decay = TEAM_RATING_XG_WINDOW_DECAY
    if opponent_adjust is None:
        opponent_adjust = TEAM_RATING_XG_OPPONENT_ADJUST

    cache_key = ("get_team_xg_ratings", team_id, before_date, league, n, decay,
                 bool(opponent_adjust),
                 None if league_raw_means is None else
                 tuple(league_raw_means.get(f) for f in
                       ("home_attack", "home_defense", "away_attack", "away_defense")))

    def _compute():
        cur = conn.cursor()

        # home rows: we are home_team_id, opponent is away_team_id
        # away rows: we are away_team_id, opponent is home_team_id
        def venue_rows(venue, team_col, opp_col):
            cur.execute(f"""
                SELECT m.match_id, m.match_date, m.{opp_col} AS opp_id,
                       SUM(s.xg) AS team_xg, MAX(s.club_xga_per90) AS team_xga
                FROM soccer_player_stats s
                JOIN soccer_matches m ON m.match_id = s.match_id
                WHERE m.league = ? AND s.venue = ? AND m.{team_col} = ?
                  AND m.match_date < ?
                GROUP BY s.match_id
                ORDER BY m.match_date DESC
                LIMIT ?
            """, (league, venue, team_id, before_date, n))
            return cur.fetchall()

        home_rows = venue_rows("home", "home_team_id", "away_team_id")
        away_rows = venue_rows("away", "away_team_id", "home_team_id")

        if not opponent_adjust:
            return {
                "home_attack":  _decay_weighted_mean([r[3] for r in home_rows], decay),
                "home_defense": _decay_weighted_mean([r[4] for r in home_rows], decay),
                "away_attack":  _decay_weighted_mean([r[3] for r in away_rows], decay),
                "away_defense": _decay_weighted_mean([r[4] for r in away_rows], decay),
                "home_n": len(home_rows), "away_n": len(away_rows),
            }

        # Opponent quality as of each past match: RAW ratings only (no adjust).
        def opp_raw(opp_id, match_date):
            return get_team_xg_ratings(
                conn, opp_id, match_date, n=n, league=league,
                decay=decay, opponent_adjust=False, cache=cache,
            )

        # Baselines: prefer league-wide raw means at the rating date; else mean of
        # opponents faced in-window (per field).
        def baseline(field, collected):
            if league_raw_means is not None and league_raw_means.get(field) is not None:
                return league_raw_means[field]
            vals = [v for v in collected if v is not None and v > 0]
            return (sum(vals) / len(vals)) if vals else None

        def adjust_side(rows, opp_def_field, opp_att_field):
            """rows: (match_id, date, opp_id, xg, xga). Attack uses opp defense;
            defense (xGA) uses opp attack."""
            att_vals, def_vals = [], []
            opp_defs, opp_atts = [], []
            for _mid, mdate, opp_id, xg, xga in rows:
                o = opp_raw(opp_id, mdate)
                od = o.get(opp_def_field)
                oa = o.get(opp_att_field)
                opp_defs.append(od)
                opp_atts.append(oa)
                att_vals.append((xg, od))
                def_vals.append((xga, oa))

            b_def = baseline(opp_def_field, opp_defs)
            b_att = baseline(opp_att_field, opp_atts)

            def scale(pairs, base):
                out = []
                for actual, opp_q in pairs:
                    if actual is None:
                        out.append(None)
                        continue
                    if base is None or opp_q is None or opp_q <= 1e-9:
                        out.append(actual)
                    else:
                        out.append(actual * (base / opp_q))
                return out

            return (
                _decay_weighted_mean(scale(att_vals, b_def), decay),
                _decay_weighted_mean(scale(def_vals, b_att), decay),
            )

        # Home: faced away-side opponent => opp's away_defense / away_attack
        h_att, h_def = adjust_side(home_rows, "away_defense", "away_attack")
        # Away: faced home-side opponent => opp's home_defense / home_attack
        a_att, a_def = adjust_side(away_rows, "home_defense", "home_attack")
        return {
            "home_attack": h_att, "home_defense": h_def,
            "away_attack": a_att, "away_defense": a_def,
            "home_n": len(home_rows), "away_n": len(away_rows),
        }

    return _memoized(cache, cache_key, _compute)


def league_xg_field_means(conn, team_ids, before_date, league="Serie A", n=TEAM_PAST_MATCH_WINDOW_SIZE,
                          cache=None, decay=None, opponent_adjust=None):
    """League-wide mean of each of get_team_xg_ratings' four fields across
    team_ids, at the same (league, before_date, n, decay, opponent_adjust) every
    team_ids member will be rated at -- the recentering point
    TEAM_RATING_XG_SPREAD_STRETCH_ATTACK/_DEFENSE stretch around. Depends only on
    those rating knobs + the team set, not on which team is currently being rated,
    so compute() calls this ONCE per call and passes the result to every team's
    team_level_lambda call.

    When opponent_adjust is on, this is a two-pass mean: first the RAW (unadjusted)
    league means (used as schedule baselines inside each team's adjusted rating),
    then the mean of the adjusted ratings themselves (what stretch recenters on).

    cache: optional dict (BUG-011 pattern) shared with get_team_xg_ratings /
    team_level_lambda. None is a plain passthrough."""
    if decay is None:
        decay = TEAM_RATING_XG_WINDOW_DECAY
    if opponent_adjust is None:
        opponent_adjust = TEAM_RATING_XG_OPPONENT_ADJUST

    fields = ("home_attack", "home_defense", "away_attack", "away_defense")

    def mean_of(get_ratings):
        sums = {f: 0.0 for f in fields}
        counts = {f: 0 for f in fields}
        for tid in team_ids:
            ratings = get_ratings(tid)
            for f in fields:
                if ratings[f] is not None:
                    sums[f] += ratings[f]
                    counts[f] += 1
        return {f: (sums[f] / counts[f] if counts[f] else None) for f in fields}

    raw_means = mean_of(
        lambda tid: get_team_xg_ratings(
            conn, tid, before_date, n=n, league=league,
            decay=decay, opponent_adjust=False, cache=cache,
        )
    )
    if not opponent_adjust:
        return raw_means

    return mean_of(
        lambda tid: get_team_xg_ratings(
            conn, tid, before_date, n=n, league=league,
            decay=decay, opponent_adjust=True, league_raw_means=raw_means, cache=cache,
        )
    )


def team_level_lambda(conn, team_id, league, before_date, avg_home, avg_away, n=TEAM_PAST_MATCH_WINDOW_SIZE,
                      team_xg_v_goals_blend=TEAM_RATING_XG_V_GOALS_BLEND,
                      xg_spread_stretch_attack=TEAM_RATING_XG_SPREAD_STRETCH_ATTACK,
                      xg_spread_stretch_defense=TEAM_RATING_XG_SPREAD_STRETCH_DEFENSE,
                      league_xg_means=None, cache=None,
                      xg_window_decay=None, xg_opponent_adjust=None,
                      league_raw_xg_means=None):
    """Home/away-split intrinsic attack/defense for a team from the EXISTING team-level
    system, mirroring estimate_lambdas()'s own fallback/shrink logic exactly
    (TEAM_RATING_MIN_MATCHES_TO_TRUST_TEAM_RATING_OVER_LEAGUE_AVERAGE/TEAM_RATING_PULL_TOWARD_AVERAGE_MATCHES, and now TEAM_PAST_MATCH_WINDOW_SIZE -- the old n=25 default here didn't match
    analyse_match()'s actual n=TEAM_PAST_MATCH_WINDOW_SIZE=10 window, a real behavioral mismatch found
    2026-08-01 while debugging the away-side bias, separate from the home/away-collapse
    bug fixed earlier the same day) -- this RESTORES that mechanism rather than
    approximating it, since it's what already correctly encodes home-field advantage
    (2026-08-01: averaging home/away into one scalar here had silently dropped that,
    independent of any player blending -- see FEATURE-011_BUILD_TRACKER.md task 5).

    Defense values are measured in the OPPONENT's scoring units, same convention
    estimate_lambdas() uses (home_defense shrinks toward avg_away, away_defense
    toward avg_home) -- "goals conceded" is fundamentally the opponent's attack.

    Returns (home_attack, away_attack, home_defense, away_defense) -- always defined
    (falls back to the relevant league average, never None), same guarantee
    estimate_lambdas() provides; callers no longer need a None/cold-start check.

    team_xg_v_goals_blend (0.0=pure goals, 1.0=pure xG, DEFAULT 1.0): blends the RAW goals-
    based rating (core.poisson_model.get_team_ratings, actual match scores, exactly
    what poisson_v3 uses) with the RAW xG-based rating (get_team_xg_ratings above,
    this file's own xG/xGA derivation) before the shrink-to-fallback step below.
    1.0 is a true no-op, exactly reproducing this function's behavior since
    2026-08-02 (the fix that cleared the Model Calibration success criterion, using
    pure xG because a small sample of ACTUAL goals is noisy -- see the Atalanta case
    in FEATURE-011_BUILD_TRACKER.md); 0.0 exactly reproduces the pre-2026-08-02
    goals-only behavior. Values in between were added 2026-08-05 (BUG-009's
    2026-08-04 diagnosis): pure xG has much LESS team-to-team spread than pure goals
    (measured directly on 2025-26 home attack ratings: xG stdev=0.304 vs goals
    stdev=0.505, same mean ~1.27, with zero shrinkage involved on either side --a
    property of the xG data itself, not a bug) -- the dominant cause of a mismatch-
    size-dependent bias (model probabilities compressed toward a coin flip for big
    favorites/underdogs) invisible to the pooled signed-bias metric BUG-009's
    ±0.01-0.02 target uses. 0.5 tested (ad hoc script, not committed) as a reasonable
    middle ground: pooled signed home bias -0.0108 (near the ±0.01-0.02 target,
    pure-xG's -0.0009 is better there) while cutting the mismatch-bucket compression
    roughly in half vs. pure xG. This function never touches core.poisson_model
    either way, so poisson_v3 is unaffected by this default regardless.

    home_n/away_n (the TEAM_RATING_MIN_MATCHES_TO_TRUST_TEAM_RATING_OVER_LEAGUE_AVERAGE gate below) use whichever single source is
    fetched when team_xg_v_goals_blend is exactly 0.0 or 1.0 (preserving exact behavior at
    those boundaries even though get_team_ratings/get_team_xg_ratings can have
    slightly different coverage -- see get_team_xg_ratings' coverage caveat); for a
    genuine blend (0 < team_xg_v_goals_blend < 1) uses the MIN of the two sources' counts,
    a conservative choice (don't fully trust a blend unless BOTH sources have enough
    matches).

    xg_spread_stretch_attack/xg_spread_stretch_defense (DEFAULTS
    TEAM_RATING_XG_SPREAD_STRETCH_ATTACK/_DEFENSE -- see those constants' comment
    for the full derivation, including why they're separate): re-spreads the RAW
    xG ratings around the league's own mean before they enter the blend above,
    attack fields scaled by the attack factor and defense fields by the defense
    factor. Only takes effect when league_xg_means is also given (the per-field
    league averages needed to recenter on) -- this function only rates ONE team,
    so it can't compute a league-wide mean itself without an extra full-league
    query; compute() calculates it once per call and passes it down to every team
    rather than paying that cost per team. Callers that don't pass league_xg_means
    (e.g. this function's own unit tests) get the exact pre-2026-08-07 behavior
    regardless of either stretch value -- it's a silent no-op without its
    companion, by design, so isolated tests of the blend lever don't also need to
    fake up a league snapshot.

    cache: optional dict (BUG-011 pattern) -- see league_xg_field_means' docstring.
    compute() calls league_xg_field_means for the WHOLE league right before
    calling this function once per team, so without a shared cache this team's
    own get_team_xg_ratings gets queried a second, redundant time here. None
    (default) is a plain passthrough, identical to pre-2026-08-12 behavior."""
    if xg_window_decay is None:
        xg_window_decay = TEAM_RATING_XG_WINDOW_DECAY
    if xg_opponent_adjust is None:
        xg_opponent_adjust = TEAM_RATING_XG_OPPONENT_ADJUST

    goals_ratings = (get_team_ratings(conn, team_id, before_date, n=n, league=league, decay=1.0)
                     if team_xg_v_goals_blend < 1.0 else None)
    # get_team_xg_ratings memoizes itself via cache; do not double-wrap with a
    # shorter key that would ignore decay/opponent_adjust.
    xg_ratings = (get_team_xg_ratings(
                      conn, team_id, before_date, n=n, league=league,
                      decay=xg_window_decay, opponent_adjust=xg_opponent_adjust,
                      league_raw_means=league_raw_xg_means, cache=cache)
                  if team_xg_v_goals_blend > 0.0 else None)
    if (xg_ratings is not None and league_xg_means is not None
            and (xg_spread_stretch_attack != 1.0 or xg_spread_stretch_defense != 1.0)):
        xg_ratings = dict(xg_ratings)
        for field in ("home_attack", "home_defense", "away_attack", "away_defense"):
            v, m = xg_ratings[field], league_xg_means.get(field)
            if v is not None and m is not None:
                factor = xg_spread_stretch_attack if "attack" in field else xg_spread_stretch_defense
                xg_ratings[field] = spread_around_mean(v, m, factor, mode="multiplicative")

    def blend(field, n_field):
        g = goals_ratings[field] if goals_ratings is not None else None
        x = xg_ratings[field] if xg_ratings is not None else None
        if x is None:
            value = g
        elif g is None:
            value = x
        else:
            value = (1 - team_xg_v_goals_blend) * g + team_xg_v_goals_blend * x
        if goals_ratings is not None and xg_ratings is not None:
            n_matches = min(goals_ratings[n_field], xg_ratings[n_field])
        elif goals_ratings is not None:
            n_matches = goals_ratings[n_field]
        else:
            n_matches = xg_ratings[n_field]
        return value, n_matches

    def resolved(value, n_matches, fallback_avg):
        if value is not None and n_matches >= TEAM_RATING_MIN_MATCHES_TO_TRUST_TEAM_RATING_OVER_LEAGUE_AVERAGE:
            return _shrink(value, fallback_avg, n_matches, TEAM_RATING_PULL_TOWARD_AVERAGE_MATCHES)
        return fallback_avg

    home_attack_raw, home_n = blend("home_attack", "home_n")
    away_attack_raw, away_n = blend("away_attack", "away_n")
    home_defense_raw, _ = blend("home_defense", "home_n")
    away_defense_raw, _ = blend("away_defense", "away_n")

    home_attack  = resolved(home_attack_raw, home_n, avg_home)
    away_attack  = resolved(away_attack_raw, away_n, avg_away)
    home_defense = resolved(home_defense_raw, home_n, avg_away)
    away_defense = resolved(away_defense_raw, away_n, avg_home)
    return home_attack, away_attack, home_defense, away_defense


def compute(conn, team_ids, league, season, before_date, w_attack=None, w_defense=None,
           current_roster_ids_by_team=None, attack_xg_v_goals_source="xg", defense_xg_v_goals_source="xga",
           team_xg_v_goals_blend=TEAM_RATING_XG_V_GOALS_BLEND,
           xg_spread_stretch_attack=TEAM_RATING_XG_SPREAD_STRETCH_ATTACK,
           xg_spread_stretch_defense=TEAM_RATING_XG_SPREAD_STRETCH_DEFENSE,
           player_spread_stretch_attack=PLAYER_RATING_SPREAD_STRETCH_ATTACK,
           player_spread_stretch_defense=PLAYER_RATING_SPREAD_STRETCH_DEFENSE,
           player_window_size=PLAYER_RATING_PAST_MATCH_WINDOW_SIZE,
           player_recency_half_life_days=PLAYER_RATING_RECENCY_HALF_LIFE_DAYS,
           player_recency_cutoff_days=PLAYER_RATING_RECENCY_CUTOFF_DAYS,
           player_window_min_date=None, cache=None,
           xg_window_decay=None, xg_opponent_adjust=None,
           player_shrinkage_k_minutes=PLAYER_RATING_MINUTES_TO_HALF_TRUST_OWN_RATE_OVER_LEAGUE_AVERAGE,
           player_use_team_credibility_deshrink=PLAYER_RATING_USE_TEAM_CREDIBILITY_DESHRINK,
           player_team_credibility_floor=PLAYER_RATING_TEAM_CREDIBILITY_FLOOR):
    """w_attack/w_defense: force this weight for EVERY team, bypassing per-team
    resolution -- a manual debugging/comparison override, not the normal path. Leave
    as None (default) to use resolve_blend_weight() per team, per component.

    attack_xg_v_goals_source, defense_xg_v_goals_source: passed through to load_team_players -- see that
    function's docstring.

    team_xg_v_goals_blend: passed through to team_level_lambda -- 1.0 (default) is a true
    no-op (pure xG, today's shipped behavior since 2026-08-02); 0.0 is pure goals
    (matches poisson_v3 exactly); values in between blend the two -- see that
    function's docstring (BUG-009's mismatch-size-compression diagnosis).

    xg_spread_stretch_attack/xg_spread_stretch_defense: passed through to
    team_level_lambda, along with a league_xg_means snapshot computed ONCE per
    compute() call (via league_xg_field_means) across all of team_ids -- see
    TEAM_RATING_XG_SPREAD_STRETCH_ATTACK/_DEFENSE's comment for the full
    derivation, including why attack and defense are separate constants (2026-08-12).
    1.3/1.3 (defaults) match the original shared-factor default; pass 1.0/1.0 to
    reproduce the exact pre-2026-08-07 shape. Skipped entirely (no snapshot query)
    when team_xg_v_goals_blend is exactly 0.0 -- xG never enters the rating at that
    boundary, so there's nothing to stretch.

    player_spread_stretch_attack/player_spread_stretch_defense: the SAME kind of
    re-spread as the xg_spread_stretch pair above, one level down -- applied to
    raw[tid]["ra"]/["rd"] (the per-team player-level attack/defense rate, BEFORE
    the avg_home/attack_mean unit-conversion step, which is a pure linear rescale
    that doesn't touch relative dispersion) around attack_mean/defense_mean, the
    same league-wide means already computed below for that conversion. See
    PLAYER_RATING_SPREAD_STRETCH_ATTACK/_DEFENSE's comment for the compression
    measurement that motivated this (2026-08-12, BUG-010 continued). 1.0/1.0
    (defaults) are a true no-op -- unlike the team-level pair, these have no
    calibrated non-1.0 value yet.

    player_window_size: passed through to load_team_players -- see that function's
    docstring. Not yet independently tuned (starting value matches the team-level
    system's own TEAM_PAST_MATCH_WINDOW_SIZE).

    player_recency_half_life_days, player_recency_cutoff_days: passed through to
    BOTH load_team_players (as half_life_days/cutoff_days, the rating computation)
    AND resolve_blend_weight/player_trust_score (the trust-score computation
    deciding how much to lean on that rating) -- BUG-012, 2026-08-14, see those
    functions' docstrings and PLAYER_RATING_RECENCY_HALF_LIFE_DAYS/_CUTOFF_DAYS'
    own comment. One shared value for both, not two independently-tunable ones --
    the design's whole point (BUGS.md) is a single centralized notion of recency
    used uniformly everywhere, not a rating-side decay and a trust-side decay that
    could drift apart. Defaults are Stage 1's near-no-op values, not yet calibrated.

    player_window_min_date: passed through to load_team_players as `min_date` --
    comparison/validation only (e.g. pass a season's start date to reproduce a
    season-SCOPED window, for A/B-checking against the season-blind default of None).
    Not used by any real caller; exists purely so the season-blind design decision
    (2026-08-06, replacing blend_prior_season_attack/PRIOR_SEASON_DISCOUNT) can be
    validated against bias/ROI the same way team_xg_v_goals_blend's rollout was.

    current_roster_ids_by_team: optional {team_id: set(player_id)} override for the
    blend-weight "current squad" signal, passed through to resolve_blend_weight per
    team -- pass this (built from roster_as_of_date per team) when backtesting a past
    season; leave None for live use. before_date is threaded through
    load_team_players and get_league_averages too, for the same reason: computing a
    PAST match's lambdas must only see data that existed before that match (live use,
    where before_date is always "now" or later than all data on hand, is unaffected
    either way).

    cache: optional dict (BUG-011), passed through to resolve_blend_weight/
    player_trust_score to memoize their last-season aggregates across repeated
    compute() calls -- e.g. a backfill/backtest script looping over a season's
    matchdays can create one dict before the loop and pass it every call, turning
    ~12,700+ redundant aggregate recomputations per season into one per team. None
    (default) is a no-op -- identical behavior to before BUG-011's fix.

    player_shrinkage_k_minutes: passed through to apply_shrinkage as k_minutes --
    the "half-trust point" (in decayed minutes) at which a player's own attack/
    defense rate counts as much as their position's league-wide average, before
    that point the average dominates. Default is the shipped
    PLAYER_RATING_MINUTES_TO_HALF_TRUST_OWN_RATE_OVER_LEAGUE_AVERAGE (900 -- a
    heavy pull toward the mean for most players). BUG-009 diagnosis (2026-08-18):
    this shrinkage is a real, isolated contributor to the favorite/underdog
    spread-compression bug -- lowering it toward ~100 nearly zeroes the
    favorite-side underrating in a 200-match probe (-0.053 -> -0.006), though the
    underdog side only partially improves (a second, separate cause -- the
    team-level xG rating switch -- still contributes there). Exists as a tunable
    parameter so a real multi-league sweep can validate a new default against
    Brier/bias/ROI together, not just this one compression metric.

    player_use_team_credibility_deshrink (default PLAYER_RATING_USE_TEAM_
    CREDIBILITY_DESHRINK, False -- opt-in only): when True, REPLACES the flat
    player_spread_stretch_attack/_defense correction with a per-team linear
    de-shrink sized to exactly undo that team's OWN aggregate shrinkage
    (team_credibility(), using the per-player weights apply_shrinkage stores).
    player_spread_stretch_attack/_defense are ignored in this mode (this is a
    replacement for that mechanism, not an addition to it). See
    PLAYER_RATING_USE_TEAM_CREDIBILITY_DESHRINK's comment for the validated
    trade-off (BUG-009, 2026-08-19) and player_team_credibility_floor for the
    divide-by-near-zero guard."""
    by_team = load_team_players(conn, team_ids, before_date,
                                attack_xg_v_goals_source=attack_xg_v_goals_source,
                                defense_xg_v_goals_source=defense_xg_v_goals_source,
                                window_size=player_window_size,
                                half_life_days=player_recency_half_life_days,
                                cutoff_days=player_recency_cutoff_days,
                                min_date=player_window_min_date)
    apply_shrinkage(by_team, k_minutes=player_shrinkage_k_minutes)

    if xg_window_decay is None:
        xg_window_decay = TEAM_RATING_XG_WINDOW_DECAY
    if xg_opponent_adjust is None:
        xg_opponent_adjust = TEAM_RATING_XG_OPPONENT_ADJUST

    # Stretch recentering and opponent-adjust baselines both need league-wide
    # snapshots. When opponent_adjust is on we always build them (two-pass);
    # otherwise only when stretch is active (historical path).
    need_league_xg = (
        team_xg_v_goals_blend > 0.0
        and (
            xg_opponent_adjust
            or xg_spread_stretch_attack != 1.0
            or xg_spread_stretch_defense != 1.0
        )
    )
    league_raw_xg_means = None
    league_xg_means = None
    if need_league_xg:
        # Raw means: always available for opponent_adjust baselines.
        league_raw_xg_means = league_xg_field_means(
            conn, team_ids, before_date, league=league, cache=cache,
            decay=xg_window_decay, opponent_adjust=False,
        )
        if xg_opponent_adjust:
            league_xg_means = league_xg_field_means(
                conn, team_ids, before_date, league=league, cache=cache,
                decay=xg_window_decay, opponent_adjust=True,
            )
        else:
            league_xg_means = league_raw_xg_means

    raw = {}
    for tid, players in by_team.items():
        ra, aw, rd, dw = raw_team_strength(players)
        raw[tid] = {"ra": ra, "aw": aw, "rd": rd, "dw": dw}

    # No `seasons=` filter, matching estimate_lambdas()'s own call to this function --
    # the 100-match rolling window (LEAGUE_AVG_GOALS_PER_GAME_WINDOW_SIZE) is meant to smooth across a
    # season boundary; restricting to the current season alone starves it early in a
    # new season (2026-08-01: a single-match sample can even average to exactly 0.0
    # goals, causing a ZeroDivisionError downstream once avg_home/avg_away are used
    # as separate normalization baselines instead of one pooled number).
    avgs = get_league_averages(conn, league=league, before_date=before_date)
    avg_home, avg_away = avgs["avg_home"], avgs["avg_away"]

    attack_vals = [r["ra"] for r in raw.values() if r["ra"] is not None
                   and r["aw"] >= PLAYER_RATING_MIN_ATTACK_WEIGHTED_MINUTES_TO_JOIN_LEAGUE_AVERAGE]
    defense_vals = [r["rd"] for r in raw.values() if r["rd"] is not None
                     and r["dw"] >= PLAYER_RATING_MIN_DEFENSE_WEIGHTED_MINUTES_TO_JOIN_LEAGUE_AVERAGE]
    attack_mean = mean(attack_vals) if attack_vals else None
    defense_mean = mean(defense_vals) if defense_vals else None

    # Player-level counterpart to xg_spread_stretch_attack/_defense above (2026-08-12,
    # BUG-010 continued) -- MUST happen here, before the avg_home/attack_mean unit
    # conversion below, since that conversion is a pure linear rescale (multiply by a
    # constant) that doesn't change relative dispersion at all; stretching after it
    # would be a no-op in effect. Recenters each team's raw player-level rate around
    # the SAME league mean the unit conversion itself uses, so the mean is preserved.
    if player_use_team_credibility_deshrink:
        # BUG-009 team-specific de-shrink (2026-08-19): REPLACES the flat
        # stretch below with a per-team linear correction sized to exactly
        # undo THAT team's own aggregate shrinkage, instead of one constant
        # applied identically to every team. See apply_shrinkage/
        # team_credibility's docstrings for the mechanism.
        if attack_mean is not None:
            for tid, players in by_team.items():
                r = raw[tid]
                if r["ra"] is None:
                    continue
                w_team = team_credibility(players, "attack_rate", PLAYER_RATING_POSITION_ATTACK_WEIGHTS)
                if w_team is not None:
                    w_team = max(w_team, player_team_credibility_floor)
                    r["ra"] = attack_mean + (r["ra"] - attack_mean) / w_team
        if defense_mean is not None:
            for tid, players in by_team.items():
                r = raw[tid]
                if r["rd"] is None:
                    continue
                w_team = team_credibility(players, "club_ga_per90", PLAYER_RATING_POSITION_DEFENSE_WEIGHTS)
                if w_team is not None:
                    w_team = max(w_team, player_team_credibility_floor)
                    r["rd"] = defense_mean + (r["rd"] - defense_mean) / w_team
    else:
        # Player-level counterpart to xg_spread_stretch_attack/_defense above (2026-08-12,
        # BUG-010 continued) -- MUST happen here, before the avg_home/attack_mean unit
        # conversion below, since that conversion is a pure linear rescale (multiply by a
        # constant) that doesn't change relative dispersion at all; stretching after it
        # would be a no-op in effect. Recenters each team's raw player-level rate around
        # the SAME league mean the unit conversion itself uses, so the mean is preserved.
        if player_spread_stretch_attack != 1.0 and attack_mean is not None:
            for r in raw.values():
                if r["ra"] is not None:
                    r["ra"] = spread_around_mean(r["ra"], attack_mean, player_spread_stretch_attack, mode="multiplicative")
        if player_spread_stretch_defense != 1.0 and defense_mean is not None:
            for r in raw.values():
                if r["rd"] is not None:
                    r["rd"] = spread_around_mean(r["rd"], defense_mean, player_spread_stretch_defense, mode="multiplicative")

    results = {}
    for tid, players in by_team.items():
        r = raw[tid]
        # attack_mean/defense_mean > 0, not just "is not None" (found live, BUG-012
        # Stage 2 calibration sweep, 2026-08-14): a tight recency cutoff can leave
        # so few qualifying teams early in a window that the league-wide average
        # itself lands on EXACTLY 0.0 (e.g. the only qualifying team's own raw
        # rate happened to be 0), which used to reach the avg_home/attack_mean
        # division below and raise ZeroDivisionError. A mean of exactly 0.0 carries
        # no real dimensional information to normalize against (same reasoning as
        # every other "not enough real signal yet" gate in this function) -- treat
        # it the same as no mean at all and fall back to team-level.
        has_attack = (r["ra"] is not None
                      and r["aw"] >= PLAYER_RATING_MIN_ATTACK_WEIGHTED_MINUTES_TO_HAVE_OWN_RATING
                      and attack_mean is not None and attack_mean > 0)
        has_defense = (r["rd"] is not None
                       and r["dw"] >= PLAYER_RATING_MIN_DEFENSE_WEIGHTED_MINUTES_TO_HAVE_OWN_RATING
                       and defense_mean is not None and defense_mean > 0)

        # No fixed target spread (unlike WC's ATTACK_LAMBDA_SD) -- the single-league
        # sample here is too small to set one responsibly. Just re-center to baseline
        # and keep the raw sample's RATIO spread (BUG-009, 2026-08-09 fix -- previously
        # additive, an unexplained asymmetry with defense's own multiplicative form
        # below; see BUGS.md for why ratio is correct: it can't drive lambda negative
        # the way a flat shift can, and it matches the "relative attacking strength"
        # interpretation the team-level system already uses throughout,
        # `lambda_H = h_att * (a_def / avg_h)`). Player data still has no home/away
        # split of its own (Scenario 4, deferred) -- the same recentered ratio is
        # re-based onto the home and away league averages separately here, purely so
        # it's dimensionally consistent with the team-level home/away split it's
        # blended against below (same "opponent-scored units" convention for defense).
        la_player_home = (r["ra"] * (avg_home / attack_mean)) if has_attack else None
        la_player_away = (r["ra"] * (avg_away / attack_mean)) if has_attack else None
        ld_player_home = (r["rd"] * (avg_away / defense_mean)) if has_defense else None
        ld_player_away = (r["rd"] * (avg_home / defense_mean)) if has_defense else None

        team_home_attack, team_away_attack, team_home_defense, team_away_defense = \
            team_level_lambda(conn, tid, league, before_date, avg_home, avg_away,
                              team_xg_v_goals_blend=team_xg_v_goals_blend,
                              xg_spread_stretch_attack=xg_spread_stretch_attack,
                              xg_spread_stretch_defense=xg_spread_stretch_defense,
                              league_xg_means=league_xg_means, cache=cache,
                              xg_window_decay=xg_window_decay,
                              xg_opponent_adjust=xg_opponent_adjust,
                              league_raw_xg_means=league_raw_xg_means)

        def blend(player_val, team_val, w):
            # team_val is always defined now (team_level_lambda falls back to the
            # league average itself), so the only real branch is missing player data.
            if player_val is None:
                return team_val, 1.0
            return (1 - w) * player_val + w * team_val, w

        roster_ids = current_roster_ids_by_team.get(tid) if current_roster_ids_by_team is not None else None
        w_att = w_attack if w_attack is not None else resolve_blend_weight(
            conn, tid, league, "attack", before_date,
            current_roster_ids=roster_ids, cache=cache, window=player_window_size,
            half_life_days=player_recency_half_life_days, cutoff_days=player_recency_cutoff_days)
        w_def = w_defense if w_defense is not None else resolve_blend_weight(
            conn, tid, league, "defense", before_date,
            current_roster_ids=roster_ids, cache=cache, window=player_window_size,
            half_life_days=player_recency_half_life_days, cutoff_days=player_recency_cutoff_days)
        attack_home_blend, w_a_used = blend(la_player_home, team_home_attack, w_att)
        attack_away_blend, _ = blend(la_player_away, team_away_attack, w_att)
        defense_home_blend, w_d_used = blend(ld_player_home, team_home_defense, w_def)
        defense_away_blend, _ = blend(ld_player_away, team_away_defense, w_def)

        if w_a_used == 0.0 and w_d_used == 0.0:
            basis = "player"
        elif w_a_used == 1.0 and w_d_used == 1.0:
            basis = "team"
        else:
            basis = f"mix(w_att={w_a_used:g},w_def={w_d_used:g})"

        results[tid] = {
            "lambda_attack_player_home": la_player_home, "lambda_attack_player_away": la_player_away,
            "lambda_defense_player_home": ld_player_home, "lambda_defense_player_away": ld_player_away,
            "lambda_attack_team_home": team_home_attack, "lambda_attack_team_away": team_away_attack,
            "lambda_defense_team_home": team_home_defense, "lambda_defense_team_away": team_away_defense,
            "lambda_attack_home_blend": attack_home_blend, "lambda_attack_away_blend": attack_away_blend,
            "lambda_defense_home_blend": defense_home_blend, "lambda_defense_away_blend": defense_away_blend,
            "weight_attack": w_a_used, "weight_defense": w_d_used, "basis": basis,
            "avg_home": avg_home, "avg_away": avg_away,
        }
    return results


def parse_args():
    parser = argparse.ArgumentParser(description="Compute + blend club-league player-based lambdas.")
    parser.add_argument("--league", default="Serie A")
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--team", help="Only compute for this DB team name.")
    parser.add_argument("--limit-teams", type=int, help="Only the first N teams (by team_id).")
    parser.add_argument("--weight-attack", type=float, default=None,
                        help="Force this attack weight for ALL teams (0=pure player, 1=pure "
                             "team), bypassing per-team resolution. Debugging/comparison only.")
    parser.add_argument("--weight-defense", type=float, default=None)
    parser.add_argument("--persist", action="store_true", help="Store results in soccer_player_team_strength.")
    return parser.parse_args()


def main():
    args = parse_args()
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT t.team_id, t.name FROM soccer_teams t
        JOIN soccer_matches m ON m.home_team_id = t.team_id OR m.away_team_id = t.team_id
        WHERE m.league = ? AND m.season = ?
        ORDER BY t.name
    """, (args.league, args.season))
    all_teams = cur.fetchall()
    if args.team:
        all_teams = [(tid, name) for tid, name in all_teams if name == args.team]
    if args.limit_teams:
        all_teams = all_teams[:args.limit_teams]
    team_ids = [tid for tid, _ in all_teams]
    names = dict(all_teams)

    if not team_ids:
        print("No matching teams.")
        return

    before_date = date.today().isoformat()
    results = compute(conn, team_ids, args.league, args.season, before_date,
                      args.weight_attack, args.weight_defense)

    print(f"{'TEAM':<20} {'BLEND-ATT(H/A)':>17} {'BLEND-DEF(H/A)':>17}  BASIS")
    for tid in team_ids:
        r = results[tid]
        def fmt(v):
            return f"{v:6.3f}" if v is not None else f"{'--':>6}"
        print(f"{names[tid]:<20} "
              f"{fmt(r['lambda_attack_home_blend'])}/{fmt(r['lambda_attack_away_blend'])}   "
              f"{fmt(r['lambda_defense_home_blend'])}/{fmt(r['lambda_defense_away_blend'])}  {r['basis']}")

        if args.persist:
            # soccer_player_team_strength doesn't have home/away-split columns yet
            # (nothing downstream reads this table so far -- tasks 6/7, not built).
            # Persisting the home-side values only, as a known simplification.
            set_player_team_strength(
                tid, args.league,
                lambda_attack_player=r["lambda_attack_player_home"],
                lambda_defense_player=r["lambda_defense_player_home"],
                lambda_attack_team=r["lambda_attack_team_home"],
                lambda_defense_team=r["lambda_defense_team_home"],
                lambda_attack_blend=r["lambda_attack_home_blend"],
                lambda_defense_blend=r["lambda_defense_home_blend"],
                weight_attack=r["weight_attack"], weight_defense=r["weight_defense"],
                basis=r["basis"], notes="prototype v1 (home-side only; see FEATURE-011_BUILD_TRACKER.md)",
                conn=conn,
            )
    conn.close()


if __name__ == "__main__":
    main()
