# FEATURE-002 — "To Advance" Market for Knockout Ties

**Status:** requirements locked 2026-06-25 · design next · **hard deadline ~2026-06-28 (Round of 32)**

## Context & goal

Knockout ties (R32 → Final) can't end level: if tied after 90', they go to extra time
(2×15'), then a penalty shootout. Books post a **2-way "to advance"** market (which team wins
the tie) *in addition to* the 90' 1X2 (incl. Draw) and 90' O/U. v1 of the system was scoped to
1X2 + O/U only ([WC2026_REQUIREMENTS.md](WC2026_REQUIREMENTS.md)), so to-advance is a genuinely
new market. Goal: price it, bet it, grade it, and capture enough structured data to analyze
ET/PK performance (model **and** teams/players) and feed that back into later-round pricing.

---

## MUST-HAVE requirements

### Pricing
1. For each knockout match, produce **P(home advances)** / **P(away advances)** (2-way, sums to
   1), modeling **90' regulation + extra time + penalty shootout**.
2. **Additive, not a replacement:** the knockout card must still price **90' 1X2 (incl. Draw)**
   and **90' O/U** exactly as today. To-advance is a new candidate alongside them.
   (See KNOCKOUT-PRICING in [BUGS.md](BUGS.md): keep ingesting *90'* lines for 1X2/O-U.)
3. To-advance is priced **only for knockout-stage** matches (`stage` ≠ group).
4. **ET/PK bench nudge (proxy, v1):** subs matter more in ET/PK, so teams with a stronger
   **bench** get a small edge in the ET/PK windows. v1 derives bench as *squad minus the
   projected top-11* (by the existing minutes×position weighting — no new lineup input), computes
   a bench λ, and applies a tunable nudge to the ET/PK component. Nudge weight is a constant we
   tune; defaults conservative.

### Odds ingestion
5. Ingest and store the book's **2-way to-advance prices** (home-advance / away-advance) from a
   transcribed-screenshot CSV (same flow as other odds, new columns), without disturbing the
   existing 1X2/O-U odds for the same match.

### Selection
6. The single-best-pick selector considers **ADVANCE candidates alongside** 1X2 + totals, with
   the **existing floor/cap guardrails** applied unchanged.
7. Persist advance picks (side, odds, model prob, EV, stars) for scoring, like other picks.

### Results capture — full path (team + player level)
8. Per knockout match, capture the **complete path**, not a binary advanced-flag:
   - **90' regulation score** (grades 1X2 + O/U — which settle on 90' only),
   - whether **ET** was played + **ET score**,
   - whether it went to **PKs** + **shootout score**,
   - **`decided_by`**: regulation / ET / pens,
   - **advancing team**.
9. **Player-level detail:**
   - **ET goals:** scorer + minute for each goal in extra time.
   - **Penalty shootout:** each kick — team, player, order, result (goal / miss / saved).
10. Entry is **manual** (screenshots and/or typed) — automation explicitly out of scope for now.

### Grading
11. Grade **1X2 + O/U on the 90' score**; grade **ADVANCE on the advancing team** (independent of
    the 90' result — a team can draw/lose 90' and still advance).
12. **Reusable, market-agnostic grading:** the grade/score logic for *all* markets (1X2, O/U,
    to-advance) lives in a **`core/` module importable by the external social/ROI tracker repo**
    (which must score to-advance + 1X2 together) — not buried in `update_wc_results.py`.
    (See [WC_TRACKER_INTEGRATION_BRIEF.md](WC_TRACKER_INTEGRATION_BRIEF.md).)

### Analysis / reporting (what the captured data must enable)
13. **Model calibration by path** — model P(advance) vs outcome, sliceable by how the tie was
    decided (did ET/PK ties calibrate, not just regulation wins?).
14. **Team ET/PK trends** — shootout W/L, ET GF/GA per team — consumable as a signal in
    later-round pricing.
15. **Player PK analytics** — per-player penalty conversion, taker order (enabled by req. 9).

---

## Decisions locked (2026-06-25)

| # | Decision | Choice |
|---|---|---|
| A | ET/PK model fidelity | Real ET-grid + pens, **with proxy bench nudge in v1** (req. 4) |
| B | Results-entry UX | Manual (screenshots/typed); no automation now |
| C | To-advance odds source | Transcribed-screenshot CSV, new columns |
| D | ET/PK data granularity | **Team + player-level** (reqs. 8–9) |
| E | Penalty model | Slight-favorite edge (tunable); not pure 50/50 — finalize in design |

---

## Scope sequencing & deadline protection

Both granularity (D) and bench nudge (A) are the *richer* options → more scope against a 3-day
deadline. Build in **betting-critical-path order** so a slip never sinks the launch:

1. **Critical path (must work to bet R32 on Jun 28):** advance pricing (even simple) → ingest
   advance odds → select + persist advance picks → capture advancing team → grade advance picks.
2. **Pricing quality:** proxy bench nudge (req. 4).
3. **Results richness:** player-level ET goals + shootout detail (req. 9) — this is *post-match
   entry*, so it never blocks placing the bet.

**If time gets tight, de-scope in this order (and only this order):** player-level detail →
team-level (still grade correctly); bench nudge → simple ET/PK model. **Never** de-scope the
core advance pricing/odds/selection/advance-grading path.

---

## Open design questions (for the design phase)

- ET model: `scoreline_grid` at λ scaled to 30' — exact scaling + how the bench nudge enters.
- Penalty model: shootout win prob = 0.5 + small favorite edge — size/source of the edge.
- Schema shape: new columns on `soccer_wc_odds`/`soccer_wc_matches` vs new tables for path +
  player events (lean: small new tables for penalties/ET-goals; path fields on matches).
- Where the reusable grader lives in `core/` and its interface (pick side + outcome record).
- How bench λ is computed/stored (on-the-fly vs persisted alongside team strength).

---

# DESIGN

Design choices below resolve the open questions above. Reviewer: approve this section before
any code. Built in the critical-path order from "Scope sequencing" — each numbered build step
is independently testable.

## 1. Data model (all changes additive)

> **Naming convention (Q9):** full words, no acronyms, in all new identifiers — `extra_time`,
> `shootout`, `penalty_kick`, `regulation` (not `et`/`pk`/`reg`). Self-documenting and consistent.

**`soccer_wc_matches` — add path columns** (existing `home_score`/`away_score` are redefined to
mean the **90' regulation** score, which is what 1X2 + O/U already grade on, so existing grading
is unchanged):
```
extra_time_home_score  INTEGER  -- cumulative score at end of extra time (NULL if no ET)
extra_time_away_score  INTEGER
shootout_home_score    INTEGER  -- shootout tally, e.g. 4 (NULL if no shootout)
shootout_away_score    INTEGER
decided_by             TEXT     -- 'regulation' | 'extra_time' | 'shootout' (NULL for group games)
```
> ✅ Q (decided_by as TEXT vs integer + lookup table): TEXT is right — overkill to add a table
> for a fixed 3-value set with no attributes, and it matches the existing convention
> (`match_status` is already a TEXT enum with no lookup table). We'll add a CHECK constraint for
> integrity.
> ✅ Q (do we store winners here? advanced_team_id seems odd): Good catch — today the winner is
> *derived* from the score, never stored. So **advanced_team_id is dropped.** "Who advanced" is
> fully derivable from the path fields above (compare extra_time_score; if tied, shootout_score) via a pure
> `advancing_side(...)` helper in `core/grading.py` — consistent with "derive winners from
> scores," no redundant column. `decided_by` is kept (a primary analysis axis per req. 13, and a
> data-entry guard) even though it too is derivable.
Extra-time-only goals (for analysis) derive as `extra_time_score - regulation_score`.

**`soccer_wc_odds` — add to-advance (2-way) market:**
```
home_advance_ml REAL      -- American odds, team advances (NULL on non-knockout / no market)
away_advance_ml REAL
```

**New table `soccer_penalty_kicks`** (player-level shootout detail, req. 9):
> ✅ Q (drop "wc" for cross-tournament reuse): Agreed — renamed to **`soccer_penalty_kicks`**
> (helper `add_penalty_kick`). Caveat: its `match_id`/`team_id`/`player_id` still reference the
> `soccer_wc_*` tables *in this DB*; true multi-tournament reuse also needs generic
> match/team/player parents (a bigger refactor, out of scope here). The generic name + generic id
> columns are the cheap first step toward that.
> ✅ Q (remove "wc" from ALL table names now — easier to do them all?): **No — defer.** What it
> would take: `ALTER TABLE ... RENAME` on all 8 `soccer_wc_*` tables (preserving data), update
> every reference across `core/sports_db.py` + ~6 scripts + all tests, rename the ~30 `*_wc_*`
> CRUD helpers for consistency, *and* coordinate a **breaking change with the external
> serie-a-bets-tracker repo** that reads these tables — a half-day-plus of mechanical work with
> real cross-repo risk, 3 days from a hard deadline. **Decision:** keep the two new tables generic
> (seed for reuse), accept a temporary generic-vs-`wc_` mix, and do the *full* generalization
> (rename all + generic match/team/player parents — what actually unlocks reuse) as a dedicated
> post-deadline refactor → **REFACTOR-001** in BUGS.md.
```
penalty_kick_id INTEGER PK · match_id FK · team_id FK · player_id FK NULL · player_name TEXT
kick_order INTEGER · result TEXT ('goal'|'miss'|'saved') · created_at
```
`player_id` nullable with a `player_name` fallback so entry never blocks on squad-name matching.

**New table `soccer_extra_time_goals`** (player-level ET scorers, req. 9):
> ✅ Q (same "wc" rename): Agreed — **`soccer_extra_time_goals`** (helper `add_extra_time_goal`),
> same FK caveat as `soccer_penalty_kicks` above.
> ✅ Q (acronyms vs full names — be consistent): Agreed — **full words, no acronyms** convention
> (stated at the top of §1). Applied throughout: `extra_time`/`shootout`/`penalty_kick`/
> `regulation`, never `et`/`pk`/`reg`. So this table is `soccer_extra_time_goals` and its id is
> `extra_time_goal_id` (not `et_goal_id`).
```
extra_time_goal_id INTEGER PK · match_id FK · team_id FK · player_id FK NULL · player_name TEXT
minute INTEGER · created_at
```

CRUD helpers in `core/sports_db.py` next to the existing WC ones: `set_wc_match_advance_result`
(path fields only — reg/ET/PK scores + decided_by; **no** advanced-team column, it's derived),
`add_penalty_kick`, `add_extra_time_goal`, and extend `upsert_wc_odds` with the two advance params.

## 2. Pricing — `advance_probs` (new, in `core/poisson_model.py`)

Two-way tie winner = regulation, else extra time, else shootout:
```
P(home advances) = p_home_90
                 + p_draw_90 * [ p_home_et + p_draw_et * P(home wins shootout) ]
P(away advances) = 1 - P(home advances)
```
- **Regulation** `p_*_90`: existing `outcome_probs(scoreline_grid(λ_H, λ_A))` — reuse as-is.
- **Extra time** (30' = ⅓ of 90'): `scoreline_grid(λ_ET_H, λ_ET_A)` where
  `λ_ET = λ_90 * EXTRA_TIME_LAMBDA_FRACTION * bench_mult`. → `p_home_et`, `p_draw_et`.
- **Shootout**: `P(home wins SO) = clamp(0.5 + SHOOTOUT_BENCH_WEIGHT*Δbench
  + SHOOTOUT_FAVORITE_WEIGHT*Δstr, *SHOOTOUT_PROB_BOUNDS)`.

**Bench nudge (proxy, req. 4).** A new `bench_strength(team_id)` in
`compute_wc_team_strength.py` reuses the existing minutes×position weighting: rank the squad,
take the top-11 weights as the proxy XI, aggregate the **remaining** players into a bench λ via
the same normalize path. Field-center across the knockout teams → `bench_index_team`. Then:
- ET: `bench_mult_team = 1 + EXTRA_TIME_BENCH_WEIGHT * bench_index_team` (stronger bench →
  slightly higher ET scoring).
- SO: `Δbench = bench_index_home - bench_index_away`; `Δstr` from the 90' win-prob gap (the
  "slight favorite" edge, decision E).

> ✅ Q (self-documenting constant names): Done — final names + conservative defaults (all tunable):

**Constants:**
- `EXTRA_TIME_LAMBDA_FRACTION = 1/3`    — ET scoring rate vs 90' (30 of 90 minutes)
- `EXTRA_TIME_BENCH_WEIGHT    = 0.10`   — how much a stronger bench lifts ET scoring
- `SHOOTOUT_BENCH_WEIGHT      = 0.10`   — bench tilt on shootout win prob
- `SHOOTOUT_FAVORITE_WEIGHT   = 0.05`   — slight favorite edge in a shootout
- `SHOOTOUT_PROB_BOUNDS = (0.40, 0.60)` — keeps a shootout near coin-flip

Defaults are deliberately small — the nudge and favorite edge *tilt*, they don't dominate. Tune
against captured ET/PK results over the knockouts.

`analyse_match_wc` gains `lambda_home`/`lambda_away` in its return (additive) so the card can
pass the already-derived λ into `advance_probs` without recomputing.

## 3. Reusable market-agnostic grader — new `core/grading.py`

A **pure** function (stdlib only, no DB) so the social/ROI tracker repo can vendor/import it
(req. 12):
```python
def grade_pick(side: str, outcome: dict) -> str:   # 'win' | 'loss' | 'push'
    # outcome = {"regulation_home": int, "regulation_away": int, "advanced": "HOME"|"AWAY"|None}
```
Side grammar (superset of today's): `HOME` / `DRAW` / `AWAY` and `OVER x` / `UNDER x` grade on
`regulation_*` (unchanged logic); **`HOME ADVANCE` / `AWAY ADVANCE`** grade on `outcome["advanced"]`.
`update_wc_results.py` and `record_override.py` import from here instead of the local copy; the
existing `grade_pick(side, home, away)` is replaced by the dict form (callers + tests updated).
Display: `display_pick` renders `HOME ADVANCE` → "<home> to advance".

## 4. Card / import / results integration

- **`generate_wc_card.py`** — for knockout matches with advance odds present, add two ADVANCE
  candidates (`HOME ADVANCE` / `AWAY ADVANCE`) from `advance_probs`, fed through the **unchanged**
  `select_pick` (floor/cap apply as-is). Group-stage matches are untouched (no advance odds).
> ✅ Q (no advance odds ⇒ system runs exactly as today?): **Yes, exactly.** The presence of
> `home_advance_ml`/`away_advance_ml` is the **sole trigger** — no advance odds → `advance_probs`
> is never called, no ET/PK code runs, and the candidate set + card are identical to today. Holds
> for every group game *and* any knockout game whose advance odds we haven't ingested yet. This is
> the key safety invariant, covered by a test (fixture without advance odds → no ADVANCE
> candidate, unchanged pick).
- **`import_wc_odds.py`** — two optional CSV columns `home_adv`, `away_adv`; pass through.
- **Results entry** — new `record_knockout.py` for the rich path: 90' score + ET/PK scores +
  decided_by + advanced team, plus penalty-kick and ET-goal events. Keeps the simple
  `update_wc_results.py` group-stage flow intact. Both share the `core/grading.py` grader.

## 5. Tests (written alongside each step)

- `advance_probs`: symmetric λ + equal bench → 0.5/0.5; stronger side advances more; raising a
  team's bench shifts ET + shootout its way; sums to 1; pure-regulation favorite ≈ its 90' win
  prob plus draw share.
- `core/grading.py`: ADVANCE win/loss; **all existing 1X2/O-U grade cases still pass** (refactor
  regression guard); unknown side raises.
- schema smoke: new columns + tables create; advance-result + PK + ET-goal round-trips.
- card e2e: knockout fixture + advance odds → ADVANCE considered and guardrails applied; a
  group fixture yields no ADVANCE candidate.

## 6. Build order (critical path first; each step ships green)
> ✅ Q (stop for sign-off at each step): Agreed — **checkpoint protocol:** after each numbered
> step I stop, show the diff + passing tests, and wait for your explicit sign-off before starting
> the next. No batching steps together.
1. **Schema + CRUD** (matches path cols, odds advance cols, PK/ET tables) + smoke tests.
2. **`core/grading.py`** refactor (pure grader incl. ADVANCE) + move callers + regression tests.
3. **`advance_probs`** (simple: ET + 50/50-ish shootout, bench nudge **off**) + tests.
4. **Card + import** wire-up: ingest advance odds, surface ADVANCE picks, persist + grade.
   → *At end of step 4 we can bet R32 on Jun 28.*
5. **Bench nudge** (proxy `bench_strength` + turn on `W_*`) + tests. *(pricing quality)*
6. **`record_knockout.py`** player-level PK/ET capture + analysis queries. *(results richness)*

If time is tight, steps 5–6 slip per the de-scope rule; steps 1–4 are the non-negotiable core.
