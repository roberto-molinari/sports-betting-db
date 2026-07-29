# FEATURE-011 — Player-Level Lambda Model — Requirements

**"v1 scope" throughout this doc means the mid-August release** (Serie A + Premier League,
Bundesliga, LaLiga, Ligue 1 — see Constraints and Preferences), not the prototype. The
prototype (`FEATURE-011_PROTOTYPE_LOG.md`) deliberately skipped things to move fast; a
prototype non-goal is NOT automatically a v1 exclusion — each needs its own call.

## Context

The club (for footbal and hockey) model currently relies on team-level data to enable
calculating attack and defense lambdas that feed into a Poisson probability calculation.

Analysis has shown that this approach has hit limits on providing reliable and accurate
probabilities for the Serie A (and likely the next set of leagues we want to add) seasons
in the past.  In order to improve the reliabilty and accuracy of the models probability
estimations, we need to be able to go a level deeper into player level statistics to form
data-driven (at the player level) inputs that feed into team-level attack/defense lambda outputs.

This is the next natural step after tuning the team-level data, starting with football before
we extend to hockey.

For more context, see the FEATURE-011 entry and BUG-009 entry in BUGS.md for more details.

---

## Core Requirement

Add player-level attack/defense lambda generation to the Poisson model as an alternative to
the existing team-level generation, with the generation method (team-level vs. player-level)
selectable **per league**. This changes how `lambda_home`/`lambda_away` are produced; it does
not change the downstream probability/EV/pick logic already in `analyse_match()`.

**Fallback behavior:**
- A team with insufficient player-level data (e.g. partial roster coverage) falls back to
  the existing team-level lambda calculation for that team.
- As leagues/teams are added over time, the system uses whatever player-level data is
  available for them; a league or team without enough of it is just an instance of the
  insufficient-data case above, not a separate failure mode.

**v1 scope:** build and validate the player-level model for Serie A first, using the same
kind of closing-line/sharp-book validation used for BUG-009 (see BUGS.md), before extending
to European leagues. Target date for all of it — Serie A build, validation/tuning, and
European league rollout — is **August 15**, so the new system is generating picks for all
major European leagues starting on matchday 1.

**Versioning:** ships as a new `soccer_model_predictions.method` value (e.g. `poisson_v4`)
that runs side-by-side against the current team-level method (`poisson_v3`), for direct
before/after comparison, consistent with how the BUG-009 fix was validated.

---

## Scenarios

Scenario 0 - the system generates a starting lineup baseline for each team (across leagues), using starting
lineup history as the baseline (i.e. the players that have started in the past are most
likely to start in the future).  Since this is across leagues, teams that are newly relegated/promoted leverage
starting lineup history from their prior league.

Scenario 1 - using the starting lineup for each team, generate a team level attack and team level defense lambda from the 
per-player stats of the current roster.  The team level lambdas are generated automatically and quickly

Scenario 2 - ability to re-calculate team level attack and team level defense lambdas if/when
a player is marked as unavailable by manual input (for now).  Automate the selection of the replacement
player using a mix of starting lineup history (when available), minutes played at the positon and 
overall player strenght (attack + defense) before generating the team level numbers.  Be clear in the output on which
replacements were automatically made with an option to change/override after automation completes.

Scenario 3 - provide reports (on demand) for what player(s) are top and bottom contributors
to team level attack and team level defense lambdas

Scenario 4 - take into account player performance at home v. away, to enable team level lambdas
to change (as needed) when the team is home v. away

Scenario 5 - support for updating player level data on a weekly basis to incorporate player
level data changes after each match day completes

Scenario 6 - ability to add (via configuration) support for all major european and south american football leagues/players, 
then asian, african and north american leagues, as well as lesser leagues over time

Scenario 7 - a new player is acquired by a team the model is estimating a probability for.  The new player has 
historical stats from prior club/league play (either in the same league or another one).  The model includes
the players historical stats when calculating the players new team attack/defense lambdas.  The model maintains
and updates (as needed) a league factor that enables players going from weaker leauges to stronger leagues
to be dialed down on attack/defense and vice versa for playser going from stronger to weaker.

Scenario 8 - a new player is acquired by a team the model is estimating a proboabilty for.  The new player has
no historical stats.  The model has a profile for the position/age and uses that profile to inform the players
stats until enough data is collected (by weekly matchday updates) to improve the specific players stats in the 
model.

Scenario 9 - support for modifying the team level attack/defense lambdas based on a head coach change by manual input (for now).  
The attack/defense lambdas can change (from a head coach change) due to any combination of (1) style differences for the new head coach v. the old head coach (2) formation changes for the new head coach v. the old head coach or (3) player selection differences betweent the
old head coach and the new head coach

---

## Player-Level Strength Estimation

**Two lambda-generation methods, selectable per league** (Core Requirement):
- **Team-level** — the existing system, unchanged, re-used and maintained for leagues
  that continue using it.
- **Player-level** — built on top of the World Cup 2026 player-aggregation system
  (`compute_wc_team_strength.py`): position-weighted per-player rates, minutes-based
  shrinkage toward positional priors, and league-quality factors (separate attack/defense
  exponents), normalized to a baseline. Initial data configuration uses the same data the
  WC system uses; the source is swappable at the config level, not the code level.

**Blend (replaces the WC system's FIFA-rank fallback/blend):**
- The WC system blends stats-based lambdas toward a FIFA-rank estimate when player-data
  coverage is thin, and falls back to FIFA rank entirely below a coverage threshold. Club
  leagues have no FIFA-rank equivalent, so the "other side" of the blend is the **existing
  team-level lambda** for that same team (consistent with the Core Requirement's fallback
  behavior for insufficient player data).
- This is a genuine weighted blend, not a binary switch:
  `lambda = (1 - w) * player_lambda + w * team_lambda`, with `w` in `[0, 1]`.
- The blend is computed **independently for attack and defense** — reusing the WC system's
  existing per-team, per-component override mechanism (`FIFA_BLEND_WEIGHT` /
  `FIFA_BLEND_OVERRIDES`), just re-pointed at team-level lambdas instead of FIFA rank.
- **Weight resolution:** a per-team weight is the default. Setting a league-wide weight for
  a given component (attack or defense) overrides every team's weight for that component in
  that league. <!-- How the default per-team weight is derived (e.g. from data-coverage
  thresholds, mirroring MIN_ATTACK_WEIGHT/MIN_DEFENSE_WEIGHT) is an implementation detail,
  not resolved here. -->

**Reporting:** every team's basis must be visible on demand — purely player-based (`w=0`),
purely team-based (`w=1`, e.g. insufficient player data), or a mix (`0<w<1`) — reusing the
WC system's existing basis/method-tagging pattern (`soccer_wc_team_strength.method`:
`player_aggregation` vs `fifa_ranking`) rather than building new reporting infrastructure.
This is closely related to Scenario 3's contributor reporting and may end up as the same
feature.

---

## Data Sources

**Provider:** TheStatsAPI, same as the World Cup system (Player-Level Strength Estimation).
Confirmed via the prototype (see `FEATURE-011_PROTOTYPE_LOG.md`) for Serie A and spot-checked
for the four August-15 leagues: Premier League, Bundesliga, LaLiga, and Ligue 1 all have
`has_player_stats=True` and `xg_available=True`.

**Endpoints, and which is primary:**
- `competitions` (resolve) — **cannot assume the display league name is searchable.**
  Confirmed: searching `"La Liga"` returns nothing; the API's actual name is `LaLiga` (no
  space). Needs a per-league search-term/competition-id config, not a shared "search the
  league name" default.
- `teams` (list, per competition+season) — for matching against `soccer_teams`.
- `matches` (list, per competition+season) — source of the API's match ids.
- **`matches/{id}/player-stats` — the primary stats source**, not `players/{id}/stats`. One
  call returns both teams' full matchday rosters with per-match minutes/goals/assists/**xG**
  (unavailable at the season-stats level — a known gap in both this system and the WC
  system's current data). This also removes the need for per-team defense API calls — team
  goals-against comes from our own `soccer_matches` table, not the API.
- **`matches/{id}/lineups` — real starting-lineup history, replacing the minutes-played
  proxy (Scenario 0).** Verified for a finished Serie A match: `starting_xi` vs
  `substitutes`, per player position/jersey number, plus formation. For *completed* matches
  this is real ground truth, not a proxy. For *upcoming* matches, confirmed lineups aren't
  available until ~1hr before kickoff — after picks need to be generated — so a projection
  step is still required; this endpoint just gives that projection real historical start/bench
  records to work from instead of raw season minutes.
  **Backfill depth is shorter than stats: 1 season, not 3.** Rosters turn over enough across
  seasons that older lineups aren't a useful signal for "who starts next" — unlike
  `player-stats`, where more seasons of scoring history still helps. This asymmetry cuts the
  backfill meaningfully (see Cost below) and needs to be reflected in Persistence: the two
  per-match tables don't share the same historical depth.

**Team-name matching is per-league work, not reusable.** Confirmed via the cross-league spot
check: Serie A's normalization (strip `AC/AS/US` prefixes, `Calcio`/`CFC` suffixes, year
suffixes) does not transfer. Bundesliga has numeric prefixes and umlauts (`1. FC Köln`,
`FC Bayern München`), LaLiga has accents and non-obvious alternate names (`Athletic Club` =
"Athletic Bilbao" elsewhere), Ligue 1's official names diverge from common short names
(`Olympique de Marseille` vs. "Marseille"). Same shape of work as the football-data.co.uk
odds import's hand-verified `TEAM_NAME_MAP` — expect one per league, not a shared function.

**Verified vs. still open:**
- Verified against real API responses: Serie A (full prototype run), Premier League
  (match-stats endpoint spot check only).
- Not yet spot-checked at the match-stats level: Bundesliga, LaLiga, Ligue 1 (only
  competition-level flags confirmed for these three).
- Unexplained, not yet safe to automate on: Bundesliga's and Ligue 1's `/teams` list
  returned 19 and 21 teams respectively, against a stated `total_teams: 18` for both.

**Cost:** the key already has the $50/mo Starter tier (120 req/min) — confirmed via a live
rate-limit-header check on 2026-07-29, not just the plan name. (An earlier version of this
doc, and a stale code comment in `core/thestatsapi.py`, claimed only ~12 req/min observed in
practice; that didn't hold up under a direct check and has been corrected.) Ongoing weekly
cost (both `player-stats` and `lineups` per match) is ~320-480 requests/week across all 16
target leagues combined — at 120/min that's a few minutes total, comfortably inside the
5-minute-per-league batch latency target with real margin, not just barely clearing it.

**Historical backfill, recalculated for the asymmetric depth above:** for just the five
August-15 leagues (Serie A, Premier League, Bundesliga, LaLiga, Ligue 1) — 3 seasons of
`player-stats` (~5,256 matches) + 1 season of `lineups` (~1,752 matches) + squad/competition
overhead ≈ **~7,150 requests total**, or **~1 hour** at the confirmed 120/min. The eventual
16-league rollout is proportionally larger (order of a few hours) but still one-time, not
recurring, and no tier upgrade is needed for either.

---

## Cadence

**Refresh frequency:** at least once a week; exact day/time to be decided later.

**Automation:** the routine weekly player-data refresh is fully automated end to end —
staged, validated, and auto-promoted to active with no human involvement, as long as
validation passes.

**Staging (applies to the incoming player data itself, not just computed lambdas):**
- Each weekly refresh writes to a staged copy rather than overwriting the existing/active
  player data directly.
- Validation checks both directions: the raw ingested data (completeness — no missing
  teams/rows) and the resulting computed values (no negative/NaN lambdas, no implausible
  week-over-week swings).
- On a pass, the staged data auto-promotes to active. On a failure, promotion is blocked
  and the prior active data remains in use.
- Manual rollback to the prior active state must be possible even after a refresh has
  auto-promoted (e.g. a bad refresh passes validation but is caught after the fact).

**Manual overrides persist across refreshes:**
- A player marked unavailable (Scenario 2) or a coach change (Scenario 9) stays in effect
  until manually cleared — a weekly refresh does not reset these flags.
- **New requirement:** a weekly report listing every player currently marked unavailable,
  so stale/forgotten manual flags stay visible rather than silently persisting unnoticed.

---

## Persistence

**Storage grain: per-match, not per-season.** A completed season and an in-progress season
are stored identically — one row per player per match played. "Season stats" (or any
rolling window) become a query (aggregate match rows over a date range), not a separately
stored format. This resolves the backfill-integration question raised when the prototype's
per-match-vs-per-season decision was made: there is no second format to reconcile between
historical seasons and the current in-progress one, since both use the same rows. It also
means the same import code handles backfill (a large historical match-id list) and the
ongoing weekly refresh (a small one) — no separate backfill path.

**Tables — prototype versions exist, one needs reworking:**
- `soccer_players` (player_id, team_id, name, position, api_player_id) — reused as-is.
- `soccer_player_stats` — **needs rework before this goes past prototype.** Currently one
  row per player per *season* (an upserted running total, sourced from `players/{id}/stats`).
  Needs to become one row per player per *match* (sourced from `matches/{id}/player-stats`),
  which also means `match_id` becomes a required column and `xg`/`xg_per90` go from
  always-null to populated. **v1 must also add a `venue` column (`home`/`away`)**, populated
  from the match record at insert time — cheap now since it's already per-match, and it's
  the one piece of this table that can't be cheaply backfilled later if v1 ships without it
  (see Scenario 4 in Out of Scope for v1: the split *calculation* is deferred, but the data
  it would need is captured from day one).
- `soccer_player_team_strength` — reused as-is; already stores the player-based lambda, the
  team-level lambda it was blended against, the blend result, the weights used, and a basis
  label, satisfying the reporting requirement from Player-Level Strength Estimation.

**New table: `soccer_player_match_lineups`** (Scenario 0, v1 scope) — one row per player per
match from `matches/{id}/lineups`: `player_id`, `match_id`, `team_id`, `started` (bool,
`starting_xi` vs `substitutes`), `position`, `formation` (team-level, repeated per row or
stored once per team/match — TBD at implementation). This is the real data the "who's likely
to start next" projection aggregates over, replacing the minutes-played proxy. Same per-match
grain as `soccer_player_stats`, so backfill/refresh share the same match-id-mapping and
import-code-reuse story below.

**Backfill depth differs between the two per-match tables — not a bug, an intentional
asymmetry.** `soccer_player_stats` backfills 3 seasons (scoring history stays useful further
back). `soccer_player_match_lineups` backfills 1 season only (rosters turn over enough that
older lineups aren't a useful signal for projecting who starts next). Any code that walks
"all per-match player data for a team" needs to expect these two tables to cover different
date ranges, not assume symmetric coverage.

**New, not yet designed: match-id mapping.** `soccer_matches` doesn't store the API's match
id. Needs either a new `api_match_id` column or a separate mapping table, resolved by
team+date matching — the same approach already proven for team names, applied to matches.

**New, not yet designed: the blend-weight table.** Player-Level Strength Estimation
describes a per-team default weight with a league-wide override that takes precedence, set
independently for attack and defense. The prototype used one fixed CLI weight (0.5/0.5) for
every team — a real weight-resolution table (team_id + league + component -> weight, with
override precedence) is required before the automated weekly compute (Cadence) can run
unattended, and before Success Criteria validation means anything (a bias/ROI result is only
as meaningful as the weight logic that produced it).

---

## Output

**Display format:** reuse `generate_wc_card.py`'s presentation layer — ranked picks, a
decision/diagnostic log, a close-calls breakdown, and a social-post-ready block. The
underlying **selection algorithm** (guardrails, thresholds, EV logic that actually picks a
side) is a separate decision from the display format — not assumed to be the WC system's
tuned guardrails/two-step mode, which were fit to World Cup conditions specifically.

**Decision-trail logging:** the selection algorithm's reasoning (why a candidate was
excluded, which mode fired, close calls) is written to **literal log files** for
debugging/post-hoc analysis, not a DB table. v1 scope is FEATURE-011's own pick generation
only — other pick-generation paths (WC card, live Serie A picks) can move to the same
approach later; that's explicitly out of scope here.

**Persistence (distinct from the debug logs above):** final picks and all underlying
probabilities must still be persisted to the database, following the existing
`soccer_model_predictions` pattern — the debug logs are a supplement, not a replacement.

**Multi-league view:** a combined view across all supported leagues by default, filterable
by league. Filtering by generation method (team-level/player-level/mix) is not needed here.

**Basis reporting** (which teams are player-based/mixed/team-based, per Player-Level
Strength Estimation): a separate report, not part of this pick output.

---

## Out of Scope for v1

- **Home/away split CALCULATION for player-level lambdas** (Scenario 4) — v1 computes a
  single scalar attack/defense per team; whether splitting by venue actually improves the
  probabilities is unknown and cheap to test post-launch once real bias/ROI data exists.
  **Not deferred: the underlying data.** `soccer_player_stats` stores a `venue` (home/away)
  column from day one (see Persistence) specifically so this decision can be made later from
  already-captured data, without a backfill. Starting without the split is reversible;
  starting without the data isn't.
- **Automated replacement-player mechanism details beyond the agreed shape** (Scenario 2) —
  the "flag unavailable, auto-select top-ranked replacement, allow override" behavior is
  in scope; the ranking algorithm's internals are an implementation detail, not v1
  requirements.
- **League-strength-factor self-bootstrapping** (Scenario 7) and **position/age cold-start
  profiles** (Scenario 8) — mechanisms deferred to implementation. V1 may reuse the WC
  system's static `LEAGUE_FACTORS` table as a stand-in where a cross-league transfer needs
  one.
- **Top/bottom contributor reporting** (Scenario 3) beyond the basis label already stored in
  `soccer_player_team_strength` — a dedicated report is separate follow-on work.
- **Pick generation, card output, and decision-trail logging** (Output) — v1 stops at
  computing and storing lambdas; nothing downstream of that is built yet.
- **League coverage beyond Serie A + the four August-15 leagues** (Premier League,
  Bundesliga, LaLiga, Ligue 1) — Eredivisie/Primeira Liga (Aug 30) onward are later rollout
  phases per Constraints, not v1.
- **Multi-source data-provider configurability** — v1 is TheStatsAPI only. Per-league
  provider swapping stays a config-level future capability (per Player-Level Strength
  Estimation), not something built or tested now.

---

## Success Criteria

**Signed bias target:** same target and measurement approach as the team-level tuning
(BUG-009) — signed bias vs. sharp-book closing lines within **±0.01-0.02** (not raw/absolute
distance), using the existing no-mixing-within-a-season rule for book selection (Pinnacle-only
or Betfair-only depending on a season's data completeness, never both within the same season).

**ROI:** positive ROI on full-season backtesting is a **hard requirement**, not just a
directional/validation-only check — a harder bar than the team-level work cleared. (For
context: after BUG-009's fix, ROI stayed net-negative across all three backtested seasons even
though signed bias measurably improved — this is a materially higher bar than what's been hit
so far.)

**Gating:** Serie A must clear both bars above before European-league work begins. Serie A is
targeted to be built, validated, and cleared several days before August 15, so the European
leagues have room to build and clear the same gate and still land by August 15. The same
clear-before-proceeding gate applies to each subsequent rollout phase (South America, then
Asia/Africa/North America, per the Constraints rollout order).

---

## Constraints and Preferences

<!-- Cost, licensing, league coverage timing (Serie A now, major European leagues
+ other geos within ~1 week), and anything else that shapes the design before we
get there. -->

**Cost**
- *total* across all data/compute/storage - $100/mo to start.
**League coverage timing**
- *by August 15* Serie A, Premier League, Bundesliga, La Liga, Ligue 1
- *by August 30* Eredivisie, Primeira Liga
- *by September 15* Belgian Pro League, Super Lig, Czech First, Austrian Bundesliga
- *by September 30* Serie A (Brazil), Liga Profesional (Argentina), LigaPro (Ecuador), Categoría Primera A (Colombia), Primera División (Uruguay)
**Licensing**
- any data used must be able to be used commercially without per-request fees or limitations (i.e. fixed cost is ok within the budget, but no scaling costs per requests/users)
**Latency:**
- *Batch latency* (Scenario 1 — generating lambdas for a full slate of matches): must
  complete within 5 minutes per league supported (i.e. 5 minute max per league, so 3 leagues can take up to 15 minutes).  This enables iteration (as needed) on pick-generation as new information comes in.
- *Interactive latency* (Scenarios 2 & 3 — marking a player unavailable and getting the
  automated replacement + recalculated lambda, or pulling an on-demand contributor
  report): must be completed under 10 seconds to use in the moment, not batch-job speed.
