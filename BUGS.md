# Known Issues / Bug Log

A running log of known model/data issues that are understood but deferred.
Newest first. When fixing one, update its **Status** (and remove from the active
set once shipped + verified). Format is deliberately lightweight.

Severity: **high** (materially wrong picks across many teams) ·
**medium** (distorts some teams/matches) · **low** (cosmetic / rare).

---

## KNOCKOUT-PRICING — confirm 90-minute markets at the knockout transition

- **Type:** watch / reminder · **Status:** PENDING (fires ~2026-06-28, Round of 32)
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

- **Type:** feature · **Status:** PROPOSED — needed before knockouts (~2026-06-28) **if we
  want to bet to-advance**; not blocking otherwise.

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

- **Type:** analysis task · **Status:** PENDING (run when group stage completes ~2026-06-27)

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

- **Type:** enhancement (not a bug) · **Status:** PROPOSED (2026-06-20) · not built
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

## BUG-004 — Over-skew: model expects more total goals than the market (LEVEL bias)

- **Severity:** medium · **Status:** OPEN — and possibly *not* a bug (2026-06-13)
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

**Related / also worth a look:**
- The `matches_played` denominator is unreliable from TheStatsAPI for some clubs
  (e.g. Orlando stored as ga=44/mp=15 → 2.93 vs real ~2.1). `fix_wc_club_defense.py`
  can't always repair it (needs a squad player with full-season club minutes).
- Host **defensive** advantage is not modeled — hosts get an attack boost
  (`HOST_HOME_ADVANTAGE`) but no defensive edge at home. Adding opponent-attack
  suppression for host home games is a clean general add (helps modestly).
