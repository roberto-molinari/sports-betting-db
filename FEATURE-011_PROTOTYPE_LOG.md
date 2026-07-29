# FEATURE-011 Prototype — Goals, Non-Goals, and Results Log

Durable log for the first player-level-lambda prototype (Serie A). Companion to
`FEATURE-011_REQUIREMENTS.md` — this tracks what the *prototype specifically* set out to
prove, what it deliberately left out, and (once run) what actually happened. Update the
Status/Result lines as work lands; don't rewrite history — append notes with a date instead.

Started: 2026-07-28

---

## STATUS AS OF 2026-07-28 (end of session) — READ THIS FIRST

**Where things stand:** the prototype is done and all 5 goals are scored (below) — the
core mechanism works, including the headline result on AC Pisa 1909 (see Goal 3). Nothing
is broken or half-finished; everything built so far runs cleanly.

**One open decision is waiting on you, not yet acted on:**

While reviewing Goal 5's finding (weekly refresh would take ~48 min against a 5-min target),
you asked whether `matches/{match_id}/player-stats` (a bulk, per-match endpoint) could
replace the current per-player calls. **I tested it against real data and confirmed it
works well** — one call per match returns both teams' full rosters (52 players for the
Milan-Cagliari test) with real per-match xG, which we don't currently have at all. It would
cut the weekly request count from ~580 to ~10-15 per league, comfortably clearing the
latency target without needing a paid API tier, AND it also eliminates the separate
per-team defense calls (team goals-against is already in our own `soccer_matches` table).

**The catch:** adopting it is an architecture change, not a drop-in swap. `soccer_player_stats`
currently stores one row per player *per season* (a running total). The match endpoint gives
per-*match* data, so this would mean switching to one row per player *per match* and
aggregating ourselves — a better fit for the weekly-cadence requirement, but real rework of
the schema and both import scripts, plus a new problem to solve (matching our
`soccer_matches.match_id` to the API's match IDs, likely via home/away team + date, similar
to how team-name matching was already solved).

**Decision needed from you tomorrow:** rework `soccer_player_stats` + the import scripts
around the match-level endpoint now, or leave the current season-total version as the
prototype's answer and log the match-level approach as a strong, verified follow-up. Nothing
else is blocked on this — it only affects Cadence/Persistence design, not whether the
prototype's core findings (Goals 1-4) hold up.

**Everything below this point was already true before this decision came up** — the prototype
results, non-goals, and running log are unchanged and still accurate.

---

## STATUS AS OF 2026-07-29 — supersedes the 07-28 status block above

The per-match-vs-per-season decision above is **resolved**: per-match won. Since then,
`FEATURE-011_REQUIREMENTS.md`'s Data Sources, Persistence, and Out of Scope for v1 sections
have all been written (and are internally consistent with each other and with this log) —
see that doc for the actual design. Nothing in this log needed correcting as a result; the
requirements doc is now the source of truth for scope, this log stays the source of truth
for what was actually tested/verified.

**Also resolved via a cross-league spot check** (see the dedicated section below): data
coverage confirmed for all four August-15 leagues; two concrete per-league risks confirmed
real (competition-name search, e.g. `LaLiga` not `"La Liga"`; team-name matching, which
doesn't transfer from Serie A's normalization to any of the other three).

**Current risk list** (supersedes the risk ranking given earlier in conversation — several
items shifted since then):

1. **Timeline / Success Criteria validation still hasn't started, and now covers more scope
   than originally planned.** Going through Out of Scope for v1 item-by-item pulled Scenario 0
   (real starting-lineup history, via `matches/{id}/lineups`) INTO v1 — building it properly
   turned out to cost about the same as the fake proxy version, so it's the right call, but
   it's real work that wasn't accounted for before. The underlying issue is unchanged: the
   bias/ROI gate is hard (the team-level fix never cleared ROI), and now there's slightly more
   to build before that validation can even start.
2. **The per-match storage rework (`soccer_player_stats` + new `soccer_player_match_lineups`
   table) is designed but not built.** Decision made, code not written. Still blocks Cadence's
   automated refresh from running for real.
3. **Per-league naming/matching work is real, confirmed for 4 leagues, still unscoped for
   effort.** Bundesliga (umlauts, numeric prefixes), LaLiga (accents, non-obvious alternate
   names like Athletic Club/Bilbao), Ligue 1 (official vs. common names) each need their own
   pass — nobody's estimated how long.
4. **The blend-weight resolution table (per-team default + league override) is still
   undesigned**, beyond being named as a requirement in Persistence. The prototype's flat
   0.5/0.5 weight isn't real logic.
5. **Validation is still thin — one team (Pisa), one league.** Unchanged since the prototype;
   nothing since has added more evidence at scale.

Unexplained, not yet investigated: Bundesliga/Ligue 1 `/teams` list returning 19/21 vs. a
stated `total_teams: 18` for both — still open, flagged in Data Sources too.

**Correction to Goal 5 (below): the ~12 req/min figure was stale/wrong.** A live
rate-limit-header check on the actual key (not the old code comment) confirmed **120
req/min (Starter tier)**, not ~12. Goal 5's "~48 minutes for a full-league weekly refresh,
conflicts with the 5-minute target" finding does not hold at the real rate — even the
original per-player approach (~580 requests) would have cleared 5 minutes at 120/min
(~4.8 min). The move to match-level endpoints (`player-stats`/`lineups`) is still the right
call for the xG and real-lineup-data wins, just not because of an urgent latency problem —
that part of the original justification was overstated. Goal 5 should be read as
**WORKED, with a corrected number** (real cost is a few minutes/week, not a risk), not
PARTIAL. Left the original text below unedited per this file's convention — this note is
the correction of record.

---

## Goals — RESULTS (2026-07-28, 3-team subset: AC Milan, AC Pisa 1909, AS Roma)

1. **Data source feasibility** — **WORKED**. TheStatsAPI's Italian Serie A competition
   (`comp_5840`, season `sn_3061436`) has real club-team player data: position, minutes,
   goals, assists, plus advanced stats (shots, passing, duels) not even used yet. Club-level
   goals-for/against with final standings also available. 76% of the 75-player sample had
   usable minutes/goals. Same known gap as the WC data: no xG at season-stats level (only
   per-match), so attack rate falls back to goals/90, consistent with how
   `compute_wc_team_strength.py` already handles this.

2. **Team-name matching** — **WORKED**. All 20/20 current Serie A teams matched TheStatsAPI's
   team list cleanly via one normalization pass (strip `AC/AS/US` prefixes, `Calcio`/`CFC`
   suffixes, trailing founding-year digits). No manual name-mapping table needed, unlike the
   football-data.co.uk odds import which needed one.

3. **Blend mechanism** — **WORKED**, and the result is directly on-point for why this feature
   exists. For **AC Pisa 1909** (newly promoted, thin Serie A history — one of the exact teams
   named in BUG-009's diagnosis as worst-mispriced under team-level-only), the team-level
   system alone gives an implausibly low attack rating (0.684 goals/match) purely from a small,
   noisy sample. The player-based rating (1.147) is far closer to a believable range, and the
   50/50 blend (0.916) visibly pulls the final number toward it. Milan and Roma (established
   squads with long team-level history) show smaller, unremarkable gaps between the two
   sources, which is the expected/healthy pattern — the mechanism moves the most where
   team-level history is weakest, not uniformly.

4. **Persistence shape** — **WORKED**. All three new tables round-tripped a real `--persist`
   run; `get_latest_player_team_strength()` retrieves the stored blend correctly, including
   the basis label and both pre-blend components (so nothing about "how we got this number" is
   lost).

5. **Cost / request-volume reality check** — **PARTIAL — surfaced a real problem, not just a
   cost number.** The 3-team run (squads + full player stats) used ~87 API requests. Linearly
   extrapolated to all 20 Serie A teams: **~580 requests**. At the API's observed pacing
   (~12 requests/60s), a full-league weekly refresh is **~48 minutes**, not the 5-minute-per-
   league batch latency target already agreed in Constraints and Preferences. This is a
   concrete, specific conflict for the current key/tier — worth resolving explicitly (higher
   API tier, smarter batching/caching, or revisiting the latency target) before Cadence's
   automation gets built for real, not just left as a vague risk note.

---

## Non-Goals (explicitly out of scope for this prototype — logged so they aren't mistaken
for "done" later; each maps to a specific Scenario or section)

- **Scenario 0** (lineup-history baseline) — not built; uses the WC system's crude "top-11
  by club minutes" proxy instead of real starting-lineup history.
- **Scenario 2** (player marked unavailable, automated replacement) — not built.
- **Scenario 4** (home/away split for player-level lambdas) — not built; a single scalar
  attack/defense per team, mirroring the WC system, not a home/away-aware pair.
- **Scenario 9** (head-coach-change override) — not built.
- **Cadence** (weekly staged/automated refresh, validation, auto-promote, manual rollback) —
  not built; this is a one-off manual compute, same shape as
  `compute_wc_team_strength.py --print`.
- **Scenario 6** (multi-league config) — Serie A is hardcoded for this prototype.
- **Scenarios 7 & 8** (league-strength-factor self-bootstrapping, cold-start position/age
  profiles) — not built; reuses the WC system's existing static `LEAGUE_FACTORS` table
  as-is rather than a maintained/updating factor.
- **Scenario 3** (top/bottom contributor reporting) — not built.
- **Success Criteria** (signed-bias vs. sharp closing lines, +ROI backtest) — not run; this
  prototype stops at "does the mechanism produce sane numbers," not "is it good enough to
  ship."
- **Output** (pick generation, card format, decision-trail logging) — not built; this
  prototype only goes as far as computing and storing lambdas.

---

## Cross-league spot check (2026-07-29) — Premier League, Bundesliga, LaLiga, Ligue 1

Quick, cheap verification pass before committing to the August 15 rollout, per the
cross-league confidence question. No DB team data exists yet for these leagues (only
Serie A), so this checks API-side coverage and naming risk, not an actual match rate.

**Data coverage — good news, no blocker.** All 4 confirmed `has_player_stats=True` and
`xg_available=True`, matching Serie A: Premier League (`comp_3039`, 20 teams), Bundesliga
(`comp_4643`), LaLiga (`comp_8814`, 20 teams), Ligue 1 (`comp_0256`). Match-level
`player-stats` endpoint also confirmed working outside Serie A (tested on a Premier League
match — real per-match data, xG field present and populated).

**Competition search-term gotcha (new, concrete).** Searching `"La Liga"` (the common
display name) returns **zero results** — the API's actual competition name is `LaLiga`
(no space). An automated pipeline that searches by human-readable league name would
silently fail for Spain. Needs a per-league search-term (or direct competition-id) config,
not a "search the league name" assumption.

**Team-name matching — the risk is confirmed real, not hypothetical.** Serie A's
normalization (strip `AC/AS/US` prefixes, `Calcio`/`CFC` suffixes, year suffixes) will NOT
transfer as-is:
  - **Bundesliga:** numeric prefixes (`1. FC Köln`, `1. FSV Mainz 05`), umlauts
    (`FC Bayern München`), abbreviated names (`Borussia M'gladbach`).
  - **LaLiga:** accented characters (`Atlético Madrid`, `Deportivo Alavés`), and a
    non-obvious alternate common name (`Athletic Club` — commonly known elsewhere as
    "Athletic Bilbao").
  - **Ligue 1:** full official names vs. common short names (`Olympique Lyonnais` vs.
    "Lyon", `Olympique de Marseille` vs. "Marseille", `Paris Saint-Germain` vs. "PSG").
  - **Premier League:** cleanest of the four, but still has truncation (`Wolverhampton`
    vs. "Wolverhampton Wanderers").
  Each league likely needs its own matching pass, same as the football-data.co.uk odds
  import needed a hand-verified `TEAM_NAME_MAP` — not one shared normalizer.

**Unexplained wrinkle, worth a look before relying on it:** the `/teams` list returned
**19** teams for Bundesliga and **21** for Ligue 1, though each competition's own metadata
says `total_teams: 18`. Not yet investigated — could be a relegation-playoff artifact or a
season-boundary overlap in how the API scopes "current season." Flagging so it isn't
silently trusted during a real import.

**Net read:** the mechanism and data coverage look solid across leagues — no reason to
believe the core approach is Serie-A-specific. The two things that need real per-league
work before Aug 15, now confirmed rather than assumed: (1) competition resolution can't
assume the display name is searchable, (2) team-name matching needs a per-league pass, not
reused Italian-specific logic.

---

## Log / Notes

- **2026-07-28** — Schema added (`soccer_players`, `soccer_player_stats`,
  `soccer_player_team_strength`) and applied via `init_database()`. Confirmed via TheStatsAPI:
  Italian Serie A competition `comp_5840`, current season `sn_3061436`, 20 teams, both
  `has_player_stats` and `xg_available` true.
- **2026-07-28** — Built and validated `import_club_squads.py` (name-matching normalization,
  20/20 clean match) and `import_club_player_stats.py` (one club-defense call per team instead
  of per player, since a club-league player's team IS their club — simpler than the WC
  import's club-of-national-team indirection). Ran for real (not dry-run) on 3 teams: Milan,
  Pisa, Roma (75 players, 87 total requests).
- **2026-07-28** — Built `compute_club_player_strength.py`: player aggregation reused from
  `compute_wc_team_strength.py` (position weights, minutes-based shrinkage), FIFA-rank
  fallback/blend replaced with a blend against `core.poisson_model.get_team_ratings`'s
  existing team-level output, per the agreed Player-Level Strength Estimation design. Ran and
  persisted for the 3-team subset — see scored goals above for results.
- **All 5 goals resolved** (4 WORKED, 1 PARTIAL surfacing a real latency conflict). Prototype
  scope is complete for what it set out to prove. Next decision is the user's: scale the
  import to the full 20-team league, resolve the request-volume/latency conflict found in
  Goal 5, or move on to writing the Data Sources / Persistence sections of
  `FEATURE-011_REQUIREMENTS.md` from what was learned here.
