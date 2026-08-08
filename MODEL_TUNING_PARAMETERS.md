# Model Tuning Parameters

Inventory of every tunable "knob" in the club-league prediction pipeline (`core/
poisson_model.py`, `compute_club_player_strength.py`) -- both module-level constants
and function-level params that behave like knobs (defaults meant to be overridden for
comparison/debugging, e.g. `attack_xg_v_goals_source`). Values are deliberately left
out of this doc; see the source for current values, since the point here is naming and
documentation, not a snapshot of settings that will drift.

Compiled 2026-08-06 while scoping BUG-009's windowing/decay work, and renamed/
reorganized the same day as part of that work (FEATURE-011 Follow-up B) -- see
BUGS.md, FEATURE-011 entry, and `MODEL_PIPELINE_OVERVIEW.md`. Each entry notes where
it lives and what documentation already exists at that location, so this doc can
point back to the source rather than duplicate it.

**How to use this doc:** each entry name matches a real constant or param in the
source -- use it to find a knob by what it does, or to check what a name you found in
the code actually means, without having to trace through the surrounding logic.

---

## Team-level rating (`core/poisson_model.py`)

### `TEAM_PAST_MATCH_WINDOW_SIZE`
How many of a team's most recent matches count toward its own attack/defense rating —
home and away are tracked separately (last N home matches, last N away matches, not
combined). *Docs: 2-line comment above the constant.*

### `TEAM_PAST_MATCH_WINDOW_DECAY`
Within that window, how much less each older match counts toward the rating (1.0 =
every match in the window counts equally; below 1.0, older matches count for less).
*Docs: 4-line comment with a worked numeric example.*

### `TEAM_RATING_PULL_TOWARD_AVERAGE_MATCHES`
How hard a team's rating gets pulled toward the league average when it doesn't have
much history yet. Zero means no pull at all — the team's own (possibly thin) data is
used as-is once it clears TEAM_RATING_MIN_MATCHES_TO_TRUST_TEAM_RATING_OVER_LEAGUE_AVERAGE. The value
is "how many equivalent average-weighted matches to blend in to affect a team's rating."
*Docs: 4-line comment with the blend formula spelled out.*

### `TEAM_RATING_MIN_MATCHES_TO_TRUST_TEAM_RATING_OVER_LEAGUE_AVERAGE`
Below this many matches, skip the team's own rating entirely and use the league
average instead. *Docs: 1-line comment.*

---

## League-wide baseline (`core/poisson_model.py`)

### `LEAGUE_AVG_GOALS_PER_GAME_WINDOW_SIZE`
How many of the league's most recent matches (across every team) set the "average
goals per game" baseline that team- and player-level ratings get scaled against.
*Docs: full paragraph — explains the BUG-009 history and why this value was chosen.*

### `LEAGUE_AVG_GOALS_PER_GAME_WINDOW_DECAY`
Same idea as `TEAM_PAST_MATCH_WINDOW_DECAY`, but for the league-wide baseline instead
of one team's own rating. *Docs: covered in the same paragraph as
`LEAGUE_AVG_GOALS_PER_GAME_WINDOW_SIZE`.*

---

## Player-level aggregation (`compute_club_player_strength.py`)

### `PLAYER_RATING_MINUTES_TO_HALF_TRUST_OWN_RATE_OVER_LEAGUE_AVERAGE`
How many minutes of playing time a player needs before their own stat rate is trusted
over the position-wide average rate. A low-minutes player gets pulled hard toward the
position average; a high-minutes player barely moves. *Docs: 1-line comment ("same
half-trust point as the WC system"). No equivalent window/decay knob exists yet for
player-level data at all — this is the gap the current windowing work is meant to
close.*

### `PLAYER_RATING_MIN_ATTACK_WEIGHTED_MINUTES_TO_HAVE_OWN_RATING`
The minimum position-weighted attacking minutes a team's player pool needs before the team
gets its own player-based attack rating at all. Below this, the team falls back entirely 
to the team-level attack number for blending — no player signal contributes to this team's 
attack side.

### `PLAYER_RATING_MIN_ATTACK_WEIGHTED_MINUTES_TO_JOIN_LEAGUE_AVERAGE`
The minimum position-weighted attacking minutes a team needs before its own raw
attack number is included in the league-wide average that every team's attack
rating gets recentered against. A team can clear one gate without clearing the
other now that they're independent.

### `PLAYER_RATING_MIN_DEFENSE_WEIGHTED_MINUTES_TO_HAVE_OWN_RATING`
The minimum position-weighted defensive minutes a team's player pool needs 
before the team gets its own player-based defense rating at all. Below this, 
the team falls back entirely to the team-level defense number.

### `PLAYER_RATING_MIN_DEFENSE_WEIGHTED_MINUTES_TO_JOIN_LEAGUE_AVERAGE`
The minimum position-weighted defensive minutes a team needs before its own raw
defense number is included in the league-wide average every team's defense rating 
gets recentered against.

### `PLAYER_RATING_POSITION_ATTACK_WEIGHTS` / `PLAYER_RATING_POSITION_DEFENSE_WEIGHTS`
How much each position (forward/midfielder/defender/keeper) counts toward a team's
attack number vs. its defense number — e.g. forwards count fully toward attack and
barely at all toward defense. *Docs: 2-line comment citing the World Cup system this
was carried over from.*

---

## Player-level blend weight (how much to trust player data vs. team data)

### `PLAYER_RATING_MIN_MINUTES_FROM_PRIOR_SEASON`
How many minutes (in the most recently completed season) a player needs before they
count as real evidence in the trust-score's data-coverage calculation. It answers
the question of how proven this player was BEFORE this season started, for the
trust-score's roster-continuity check. *Docs: 1-line comment.*

### `PLAYER_RATING_LEAGUE_WIDE_BLEND_WEIGHT_OVERRIDE`
Lets you force every team in a given league to a single, fixed player/team blend
weight, bypassing the normal per-team calculation entirely. Currently empty/unused.
*Docs: multi-line comment explaining intent and the convention for adding an entry.*

---

## Cross-league / prior-season blending

### `PLAYER_RATING_CROSS_LEAGUE_GOAL_ADJUSTMENT`
Converts a player's goal-scoring rate from a weaker league into a Serie-A-equivalent
rate, so a goal in a lower division doesn't count the same as a Serie A goal. Applied
per-game (not per-season-block) inside `load_team_players`'s rolling window, to the
ATTACK side only (BUG-010: a defense-side equivalent would need its own calibration)
-- so a player whose window spans a league change gets each individual game scaled by
that game's own league factor, correctly blending stints of different lengths in
different leagues. A game in a league with no factor entry (uncalibrated) is excluded
entirely from the attack calculation -- both goals/xg and minutes for that game --
rather than assumed Serie-A-equivalent; this can make a player's attack-side window
smaller than their defense-side window. *Docs: long comment — explains this was
empirically measured (not guessed) and how; see also `load_team_players`'s docstring
for the per-game mechanics.*

---

## Runtime toggles (function params, not module constants)

These don't live in either file's "constants" block, so they're easy to miss even
when scanning for tunable values.

### `TEAM_RATING_XG_V_GOALS_BLEND` (`team_level_lambda` / `compute()`)
Blends a team's rating between actual-goals-based and xG-based sources — one end is
pure actual goals, the other end is pure xG, with a genuine blend in between. *Docs:
long docstring (added alongside this parameter).*

### `TEAM_RATING_XG_SPREAD_STRETCH` (`team_level_lambda` / `compute()`)
Spreads team-level xG ratings' cross-team dispersion back out toward (not all the way
to) actual-goals-level dispersion, recentered on the league's own xG mean — xG has
less team-to-team spread than actual goals by construction, which compresses win
probabilities toward a coin flip on the biggest mismatches (BUG-009). 1.0 is a no-op;
the shipped default (1.3) is the largest factor tested that stays inside the Model
Calibration bias target in both seasons. Only takes effect when `compute()` calls it
(it supplies the league-wide means via `league_xg_field_means`) — calling
`team_level_lambda` directly without that snapshot is always a no-op regardless of
this value. *Docs: long docstring on the constant itself plus `team_level_lambda`'s
own docstring; see BUGS.md, BUG-009, 2026-08-07 addendum for the sweep.*

### `PLAYER_RATING_PAST_MATCH_WINDOW_SIZE`
How many of a player's most recent appearances (for whichever team they were actually
playing for at the time, ignoring season boundaries) count toward their current
attack/defense rate. Mirrors `TEAM_PAST_MATCH_WINDOW_SIZE`'s shape exactly, one level
down (player instead of team).

### `PLAYER_RATING_PAST_MATCH_WINDOW_DECAY`
Within that window, how much less each older appearance counts (1.0 = flat average
across the window, same convention as every other `_DECAY` constant here).

### `attack_xg_v_goals_source` / `defense_xg_v_goals_source` (`load_team_players`)
Whether player-level attack/defense numbers are built from xG-based or goals-based
per-player stats. *Docs: docstring paragraph.*
> Implemented as two separate params (one per side) rather than the single
> `PLAYER_RATING_XG_V_GOALS_SOURCE` name originally proposed, since attack and defense
> already used different underlying fields (`xg` vs `club_xga_per90`) and can be set
> independently — see `load_team_players`'s signature.

