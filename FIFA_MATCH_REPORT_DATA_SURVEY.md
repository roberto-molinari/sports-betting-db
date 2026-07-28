# FIFA Match Report Data Survey

What's actually inside FIFA's official "Post Match Summary Report" (PMSR) PDFs,
beyond the xG figure `import_wc_fifa_xg.py` currently extracts. Written after
reading one full report end-to-end (USA 4-1 Paraguay, Group D, 52 pages) plus
spot-checks of a couple of others. Purpose: a reference for scoping future
features, not a spec — nothing here is implemented except the xG pull.

**Source & access.** Two hub pages list one PDF per finished match (group
stage / knockout stage — see `import_wc_fifa_xg.py`'s docstring for URLs).
Reports run ~52-53 pages, are genuinely text-bearing (not scanned images), and
parse cleanly with `pdfplumber`. Only page 1 (score/teams/date) and the xG
line (page 3 in every report checked so far) are parsed today.

**Why this matters more than it might first appear.** Almost everything below
is **national-team, per-match data** — not club-league stats aggregated up
with a FIFA-rank blend. That's the exact gap behind this project's oldest,
most-argued-over open items:

- **DESIGN-001 / BUG-001 / BUG-005** — the whole saga of club goals/90 not
  transferring to international performance (Côte d'Ivoire, Egypt, Switzerland
  overrides, the FIFA blend, etc.) is a proxy problem *because* we had no
  direct national-team signal. This report **is** that signal — real
  national-team xG, shots, pressures, distances — for every match played so
  far. It doesn't have pre-tournament data (so it can't replace the squad-based
  cold-start), but for **in-tournament recalibration** it's a fundamentally
  better input than club stats.
- **Style is currently invisible to the model.** `analyse_match_wc` is a pure
  goals-in/goals-out Poisson model; it has no notion of a team's *style*
  (press-heavy vs low-block, possession-heavy vs transition). The Phases of
  Play / Defensive Pressure sections below quantify exactly the kind of thing
  that came up qualitatively this session (e.g. "Paraguay plays a highly
  defensive style that slows possession-heavy teams down") — currently a
  post-hoc narrative, potentially a real feature.
- **FEATURE-002's ET/PK proxy bench nudge** is currently a coarse squad-depth
  proxy; goalkeeper save%/shot-stopping data here is a more direct input.
- **FEATURE-001 (player-availability what-if)** could use the individual
  offers/receptions/progression data as an importance signal independent of
  club stats.

None of this is scoped or committed to — it's what's available if any of
those ever get revisited.

---

## What's in a report

### 1. Cover (page 1)
Score, group/round + match number, date, kickoff time, venue. This is the only
page today's script parses for team identity/date/score.

### 2. Match Summary — Teams
Full lineups (starting XI + subs) with shirt numbers, positions, sub in/out
minutes, goal-scoring minutes; formation; a small "distribution in the final
third" possession-over-time strip chart.

### 3. Match Summary — Key Statistics (team level)
The team-vs-team summary table — this is where xG lives. Full row list:
Possession %, Goals, **xG (Expected Goals)**, Attempts at Goal (on target),
Total Passes (complete), Pass Completion %, Completed Line Breaks, Defensive
Line Breaks, Receptions in the Final Third, Crosses, Ball Progressions,
Defensive Pressures Applied (Direct Pressures), Forced Turnovers, Second
Balls, Total Distance Covered, Zone 4 (20-25 km/h) distance.

### 4. Phases of Play (team level, % of time/actions)
**In possession:** Build Up Unopposed/Opposed, Progression, Final Third, Long
Ball, Attacking Transition, Counter Attack, Set Piece.
**Out of possession:** High/Mid/Low Press, High/Mid/Low Block, Recovery,
Defensive Transition, Counter-press.
This is the cleanest available **style fingerprint** per team per match.

### 5. In-Possession Line Height & Team Length (team level, meters)
Average defensive-to-attacking line height and team width, split by phase
(Build Up Low, Build Up Mid, Final Third).

### 6. Line Breaks
Team summary (attempted/completed by "units" — 4/3/2, i.e. how many defensive
lines were broken at once) **and** a full per-player table: attempted/
completed/completion %, split by zone, by direction (through/around/over), and
by distribution type (pass/cross/ball progression).

### 7. Passing Networks
Full player-to-player pass-count matrix (who passed to whom, how often) plus
the top-5 passing combinations by share of team passes. Enough to reconstruct
a team's passing structure/reliance on specific channels.

### 8. Attempts at Goal
Shot map (approximate pitch location per attempt) + outcome summary (goal / on
target / off target / blocked / incomplete) + a full chronological shot log
(minute, player, outcome, body part, delivery type — pass/cross/loose
ball/corner/ball progression).

### 9. Crosses (Open Play)
Cross map + pitch-zone breakdown + delivery type (inswing/outswing/driven/
lofted/cutback/push) + per-player cross counts by type.

### 10. Offering to Receive
Team totals (offers made/received, split by pitch third) + per-player offers
made/received/% completed, with "inside shape" vs "outside shape" heatmaps.

### 11. Movement to Receive
A movement-type taxonomy (In Front, In Between, Out to In, In to Out, In
Behind) broken down by possession phase and pitch third, plus top-ranked
player per movement type. This is essentially off-ball movement profiling.

### 12. Defensive Actions
Team totals (forced turnovers, possession regained, interceptions, tackles,
"possession actions per defensive action") + blocks by type (pass/shot/cross/
clearance) + possession-contest win counts (physical/aerial) + a spatial map
of every forced-turnover and possession-regain event + per-player possession
regains.

### 13. Defensive Line Height & Team Length (out of possession)
Same idea as #5 but for High Block/Press, Mid Block, Low Block phases.

### 14. Defensive Pressure
Team totals (total/direct pressures, avg pressure duration, forced turnovers,
ball recovery time, pressing-direction inside/outside) + a full spatial map of
every individual pressure event (tagged shown-outside/inside/neutral/from
behind) + the top individual presser.

### 15. Goalkeeping (four sub-sections)
- **Involvement** — total involvements + a 5-minute-binned timeline.
- **Distribution** — kick-from-feet / kick-from-hands / throws, each
  complete/incomplete with direction maps; a "play onto/into/around/through/
  beyond" taxonomy for keeper line-breaking passes.
- **Goal Prevention** — attempts faced, save %, intervention type (deflect &
  retain / save & deflect / save & retain / no-save-attempt / save-attempt),
  intervention body part, shot-location map.
- **Aerial Control** — claims/punches/tipped-palmed (complete/incomplete),
  crosses faced with a map, delivery types faced.

### 16. Set Plays
Totals for free kicks (direct/indirect, on/off target), penalties, corners
(delivery side x type: direct-to-area/short/edge-of-box; style: inswing/
outswing/driven/lofted), throw-ins.

### 17. Individual Data (per player, both teams)
- **In Possession — Distributions**: passes attempted/completed/%, switches
  of play, crosses, line breaks, ball progressions, take-ons, step-ins,
  attempts at goal, goals.
- **In Possession — Offers & Receptions**: offers made/received per player,
  by movement type.
- **Out of Possession**: tackles made/won, blocks, interceptions, pressing
  direct/indirect, aerial/physical duels won, possession contests won,
  clearances, loose-ball receptions, possession regains/interrupted.
- **Physical Data**: total distance, distance in 5 speed zones (0-7, 7-15,
  15-20, 20-25, 25+ km/h), high-speed-run count, sprint count, top speed.

---

## Practical notes for anyone building on this later

- **Page indices aren't fixed.** The Key Statistics page was page 3 in every
  report checked (including one decided on penalties, which has extra pages
  elsewhere for shootout detail) — `import_wc_fifa_xg.py` searches the first 8
  pages for the xG line rather than hardcoding an index; any future parser for
  another section should do the same.
- **Team-name spellings differ from ours** in the usual handful of cases
  (Korea Republic, IR Iran, Czech Republic, Ivory Coast, Bosnia and
  Herzegovina, Cabo Verde, Congo DR, Türkiye/Turkey) — see
  `FIFA_TEAM_ALIASES` in `import_wc_fifa_xg.py`.
- **The page-1 score is NOT always the 90' regulation score.** For a tie
  decided in extra time, it's the extra-time-inclusive final score (confirmed
  live: Belgium 2-2 Senegal, ET winner 3-2, page 1 reads "Belgium 3 - 2
  Senegal"). Penalties don't count as goals, so a shootout-decided tie's page
  1 still shows the 90' score. `final_score()` in `import_wc_fifa_xg.py`
  already handles this for the xG import's score-sanity-check; anything new
  reading page 1 needs the same handling.
- **No pre-tournament data.** Everything here starts once the tournament does
  — it's an in-tournament recalibration signal, not a squad cold-start
  replacement.
