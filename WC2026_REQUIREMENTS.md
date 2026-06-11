# World Cup 2026 — Betting Pick System Requirements

## Context

An existing Serie A betting analytics system generates value picks using a Poisson model
that compares model-derived probabilities to market-implied probabilities. This document
captures the requirements for extending that system to cover the 2026 FIFA World Cup
(June 11 – July 19, 2026).

The World Cup presents different data characteristics than Serie A: national teams play
far fewer matches, history is sparse, squads change significantly between tournaments,
and the relevance of historical results to future performance is lower. As a result,
the specific method used to generate probabilities may necessarily differ from the
Serie A approach, even though the analytical framework is the same.

---

## Core Requirement

Generate one best pick per match for upcoming World Cup games, using the same
analytical **framework** as the existing Serie A system: produce own probability
estimates, compare to market-implied probabilities, and select the pick with the
largest positive EV gap.

The underlying method for generating those probability estimates may differ from
Serie A where the data characteristics of international football require it.

---

## Pick Generation

- **One pick per match**, covering all stages: group play through the final (104 matches total).
- **Markets in scope:** 1X2 (home win / draw / away win) and over/under goals.
- **Selection method:** The system generates its own probability estimate for each outcome,
  compares it to the market-implied probability derived from odds, and selects the outcome
  with the largest positive gap (highest EV).
- **Confidence rating:** Each pick carries a 1–3 star rating based on the size of the EV gap.
  - ⭐ Low confidence (small gap)
  - ⭐⭐ Medium confidence
  - ⭐⭐⭐ High confidence
- **No forced abstention:** Every match gets a pick. A low-confidence pick is still a pick,
  rated ⭐ rather than omitted. The system can learn over time whether low-confidence picks
  have signal.

---

## Cadence

- Picks are generated **per matchday**, not for the full tournament in advance.
- Target: run the system the day before (or morning of) each matchday.
- Up to 8 matches may fall on a single day during the group stage.

---

## Persistence

- All generated picks must be **stored** so they can be scored after results come in.
- Storage should follow the same pattern as the existing Serie A picks database.
- Scoring does not need to be automated in this phase — manual review is acceptable.

---

## Output

- Plain text output suitable for manual copy-paste into a social post (Bluesky / X).
- Format consistent with existing Serie A pick posts:
  ```
  Home / Away — Pick (⭐⭐)
  ```
- Posting is **manual** in this phase; no automated publishing required.

---

## AI Comparison (Nice to Have — Out of Scope for v1)

- The existing system compares model picks against picks from ChatGPT, Claude, and Gemini.
- Tooling for this comparison already exists for Serie A.
- For World Cup v1, the AI comparison will be done **manually if at all**.
- Automated AI comparison is explicitly out of scope until the core pick generation is working.

---

## Out of Scope for v1

- Automated social posting
- Automated AI pick comparison and scoring
- Batch generation of picks for all group stage matches at once
- Kelly criterion sizing or stake recommendations beyond the 1u flat stake currently used

---

## Success Criteria

The system is considered done when:

1. It generates one pick per match for the next World Cup matchday.
2. Each pick has a 1–3 star confidence rating.
3. Picks are stored for future scoring.
4. Output can be manually copied into a social post.

---

## Constraints and Preferences

- **Framework consistency:** The same analytical framework as Serie A must be preserved —
  own probability → market implied probability → EV gap → pick. The specific method
  used to generate probabilities may differ to account for the nature of international
  tournament football, but the framework logic must not change.
- **Simplicity of picks:** 1X2 and O/U only. No props, no parlays, no Asian handicap.
  Spreads were explicitly considered and deferred — not forgotten. In heavily lopsided
  matchups (e.g. Brazil vs. a significant underdog) the 1X2 market may be efficiently
  priced, and spreads could offer value in those cases. However, there is no backtested
  framework for spread probability estimation, and adding it would risk missing the June 11
  tournament start. Revisit after v1 is running.
- **Coverage:** All 104 matches, all stages. The system must work for knockout rounds,
  not just group play.