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
