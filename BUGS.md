# Known Issues / Bug Log

A running log of known model/data issues that are understood but deferred.
Newest first. When fixing one, update its **Status** (and remove from the active
set once shipped + verified). Format is deliberately lightweight.

Severity: **high** (materially wrong picks across many teams) ·
**medium** (distorts some teams/matches) · **low** (cosmetic / rare).

---

## FEATURE-009 — Codify the two-step "best pick" selection (never pass) — **TOP OF BACKLOG**

- **Type:** core selection redesign · **Status:** TOP PRIORITY — build next session the user has
  time (user 2026-06-29: "it's not ok to say 'pass'… codify this rather than have it be a discussion
  every time and me overriding"). Design **not yet locked** — user "doesn't agree with everything"
  discussed; thresholds + step-2 rule still to be settled before coding.

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

**Open design decisions (must settle with user before building — these determine whether it works).**
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

- **Type:** feature / analysis · **Status:** BACKLOG (build after the results-based review).
  Feasibility confirmed + key constraint set with user 2026-06-28.

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

- **Type:** feature / selection guardrail · **Status:** BACKLOG — **scoped & ready to build**
  (2026-06-26). Related to **BUG-005** (the pins it keys off). Logged in lieu of building now.

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

- **Type:** feature candidate · **Status:** WATCH — strong qualitative case, n=1 result.
  **Test window is NOW (final group matchday, ~Jun 25-27)** when dead rubbers cluster.

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

- **Type:** feature · **Status:** QUEUED — **build first** (ahead of FEATURE-002), target
  2026-06-24/25. Confirmed with user 2026-06-23.

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

- **Type:** feature · **Status:** IN PROGRESS — **promoted to build FIRST** (user wants
  to-advance live for Round of 32, ~2026-06-28; multi-line O/U FEATURE-003 demoted to nice-to-have,
  2026-06-25). **Requirements locked** → see [FEATURE-002_TO_ADVANCE.md](FEATURE-002_TO_ADVANCE.md).
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
