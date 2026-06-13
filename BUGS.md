# Known Issues / Bug Log

A running log of known model/data issues that are understood but deferred.
Newest first. When fixing one, update its **Status** (and remove from the active
set once shipped + verified). Format is deliberately lightweight.

Severity: **high** (materially wrong picks across many teams) ·
**medium** (distorts some teams/matches) · **low** (cosmetic / rare).

---

## BUG-003 — EV on big-longshot moneylines is unreliable (noise amplification)

- **Severity:** medium · **Status:** OPEN (2026-06-13)
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

## BUG-002 — Weak-league forwards inflate attack lambda

- **Severity:** medium-high · **Status:** OPEN (2026-06-13)
- **Symptom.** Teams whose forwards play in weak leagues get an over-rated attack
  even after the league-factor discount (goals/90 in a soft league overstates
  international scoring more than the factor captures). Drives over-optimistic
  underdog ML picks and a systematic Over skew.
- **Evidence.**
  - Per-team: Czechia, Bosnia, Haiti rated too high (model ranks well above
    FIFA-field). Bosnia attack 1.44 / Haiti 1.23 built on Romanian/Austrian/
    Czech/Hungarian-league forwards.
  - **Aggregate over-skew (all 72 games):** OVER avg EV **+6.8%** (+EV in 48/72)
    vs UNDER avg EV **−14.4%** (+EV in only 17/72). The model expects more goals
    than the market across the board. (Draws, by contrast, are well-calibrated:
    model avg p 0.226 vs market 0.230 — they just rarely win the one-best-pick
    race, crowded out by inflated Overs and longshots.)
  - **Matchday 1 results (1-3 record):** all three losses were on known issues —
    Czechia ML (lost, Korea won 2-1) and Bosnia ML (1-1 draw) were both
    weak-league-attack-inflated underdogs; Mexico Over 2.5 lost (2-0). The lone
    win was USA Over 2 (4-1), the Over with a *legit* (top-league) attack. Tiny
    sample, but directionally consistent with the diagnosis.
- **Proposed fix (model-level, NEEDS validation — do in a calm session, not
  reactively on a few games).** Options to evaluate: steeper attack-specific
  discount for non-top leagues; cap/shrink individual weak-league goals/90 harder
  toward the positional prior. Re-validate the full 48-team table after any change.

---

## BUG-001 — Goalkeeper club-concede rate is a poor proxy for team defense

- **Severity:** medium
- **Status:** OPEN (deferred 2026-06-11; documented for a calm session)
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

**Related / also worth a look:**
- The `matches_played` denominator is unreliable from TheStatsAPI for some clubs
  (e.g. Orlando stored as ga=44/mp=15 → 2.93 vs real ~2.1). `fix_wc_club_defense.py`
  can't always repair it (needs a squad player with full-season club minutes).
- Host **defensive** advantage is not modeled — hosts get an attack boost
  (`HOST_HOME_ADVANTAGE`) but no defensive edge at home. Adding opponent-attack
  suppression for host home games is a clean general add (helps modestly).
