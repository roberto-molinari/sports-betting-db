# World Cup 2026 — Betting Pick System Design

## Overview

This document describes the design for extending the existing sports-betting-db system
to generate value betting picks for the 2026 FIFA World Cup (June 11 – July 19, 2026).
It should be read alongside `WC2026_BETTING_REQUIREMENTS.md`.

---

## Guiding Principles

- **Framework reuse, not full reuse.** The analytical framework (own probability →
  market implied probability → EV gap → pick) is preserved. The specific method for
  generating probabilities necessarily differs from Serie A due to the sparse match
  history of international football.
- **Clean extension, not a parallel system.** New code extends the existing codebase
  rather than duplicating it. The Poisson math, EV calculation, pick selection, and
  output formatting are all reused unchanged.
- **Ship by June 11.** Design decisions favor speed and simplicity where trade-offs exist.

---

## Architecture

### Two Phases

**Phase 1 — Setup (one time, before June 11)**

1. Pull squad lists for all 48 teams (which players represent each national team)
2. Pull current season club stats from TheStatsAPI for each player (xG, minutes, position)
3. Store player stats in the database
4. Run the aggregation model to derive team attack λ and defense λ per national team
5. Iterate on the aggregation formula until satisfied
6. Persist final λ values to the database before generating the first real picks

**Phase 2 — Daily pick generation (each matchday)**

1. Import that day's matches and odds (via sportsbook screenshot → AI assistant → database)
2. For each match, look up home and away team λ values from the database
3. Pass λ values into the new Poisson entry point, which returns p_home, p_draw, p_away, p_over25
4. Compute EV against market odds (existing logic, unchanged)
5. Select best pick per match, assign star rating (1–3 stars based on EV gap size)
6. Store picks in the database for future scoring
7. Print output for manual copy-paste into social post

---

## Team Strength Estimation

### Approach: Player-level club stats aggregated to national team strength

Rather than relying on sparse national team match history, team strength is derived
from the current season club performance of each squad's players. The logic is that
a player's quality doesn't change when they switch from club to national team kit.

### Data Source: TheStatsAPI (7-day free trial)

- Provides xG, minutes played, goals, assists, and position across 1,000+ competitions
- Coverage is sufficient for ~46 of 48 World Cup squads
- Qatar and Saudi Arabia are edge cases (most players in domestic leagues with thinner
  coverage) — handled by fallback to Elo-derived estimates for those teams if needed
- 100,000 requests/month at 120 requests/minute — no throttling risk for this use case
  (~1,200 players, completable in under 10 minutes)
- 7-day trial covers the full setup phase at no cost; no ongoing subscription needed
  since odds will come from sportsbook screenshots, not TheStatsAPI

### Aggregation Model

Player-level xG stats are aggregated to two per-team values: λ_attack (how many goals
this team is expected to score) and λ_defense (how many goals this team is expected to
concede). These are stored at the team level, not the match level.

At pick generation time, match-level expected goals are derived by combining the two
teams' values — the home team's λ_attack is modulated by the away team's λ_defense,
and vice versa — producing the inputs the Poisson model needs to compute p_home,
p_draw, p_away, and p_over25.

The aggregation formula is intentionally left as an implementation detail — it will
be developed and iterated on during the setup phase before being finalized.

Key considerations for the aggregation:
- Weight by minutes played (players with more minutes get more weight)
- Adjust for position (attackers and midfielders drive attack λ; defenders and keepers
  drive defense λ)
- Adjust for league quality (xG in the Saudi Pro League is not equivalent to xG in Serie A)

### λ Storage Strategy

- During development: λ is computed on the fly from stored player stats, allowing
  iteration on the aggregation formula without database churn
- Before first picks: once the aggregation formula is finalized, λ values are persisted
  to the database and used directly at pick generation time
- During the tournament: λ values should be updatable as World Cup match data accumulates.
  A team's group stage performance is meaningful signal that should be incorporable into
  the model for knockout round picks — ignoring it would leave real information on the table.
  Whether updates are manual, semi-automated, or fully automated is an implementation
  decision, but the design explicitly leaves this door open. The `soccer_wc_team_strength`
  table supports this via the `computed_at` and `notes` fields, allowing multiple versions
  of λ per team over time.

---

## Poisson Model

### New Entry Point

A new function — parallel to the existing `analyse_match()` — accepts λ values
directly as parameters rather than deriving them from match history. The Poisson
distribution math underneath is identical to the existing model.

```python
# Existing (Serie A) — queries match history internally
analyse_match(home_team_id, away_team_id, match_date, ..., conn=conn)

# New (World Cup) — accepts pre-computed λ directly
analyse_match_wc(lambda_home_attack, lambda_away_attack,
                 lambda_home_defense, lambda_away_defense)
```

Returns the same structure: `p_home`, `p_draw`, `p_away`, `p_over25`.

This keeps the World Cup model as a thin extension of the existing system rather
than a parallel one. The card generator, EV logic, and output formatting all sit
on top of this unchanged.

---

## Data Sources Summary

| Data | Source | Cost |
|---|---|---|
| Squad lists | TheStatsAPI or static lookup | Free |
| Player club stats (xG, minutes) | TheStatsAPI 7-day trial | Free |
| World Cup fixtures | TheStatsAPI or static | Free |
| Match odds | Sportsbook screenshots → AI → DB | Free |
| In-tournament performance updates | TheStatsAPI or manual | Free (trial) / TBD |

---

## Database

### Existing Database

All World Cup tables live in the existing `sports_betting.db` alongside the Serie A
and NHL data. The schema is sport-agnostic in structure and the World Cup tables are
a natural extension.

### New Tables

**`soccer_wc_teams`** — 48 national teams
- team_id, name, confederation, fifa_ranking

**`soccer_wc_players`** — squad members
- player_id, team_id, name, position, club, club_league

**`soccer_wc_player_stats`** — current season club stats per player
- player_id, season, minutes_played, xg, xg_per90, goals, assists, source

**`soccer_wc_matches`** — all 104 fixtures
- match_id, match_date, stage (group/R32/QF/SF/F), group, home_team_id, away_team_id,
  home_score, away_score, match_status

**`soccer_wc_odds`** — odds per match
- match_id, sportsbook, odds_date, home_moneyline, draw_moneyline, away_moneyline,
  over_under, over_odds, under_odds

**`soccer_wc_team_strength`** — computed λ values per team (populated once formula is finalized)
- team_id, lambda_attack, lambda_defense, computed_at, notes

**`soccer_wc_picks`** — generated picks for scoring
- pick_id, match_id, generated_at, side, odds, model_prob, ev, stars, result

---

## New Scripts

Following the naming convention of the existing codebase:

| Script | Purpose |
|---|---|
| `import_wc_squads.py` | Pull squad lists and player data from TheStatsAPI |
| `import_wc_player_stats.py` | Pull current season club stats per player |
| `import_wc_odds.py` | Import odds from AI-processed sportsbook screenshots |
| `compute_wc_team_strength.py` | Run aggregation model, optionally persist λ |
| `generate_wc_card.py` | Generate picks for the next matchday |
| `update_wc_results.py` | Record match results for scoring |

---

## Reuse Summary

| Component | Reused? | Notes |
|---|---|---|
| Poisson distribution math | ✅ Yes | Called from new entry point |
| EV calculation | ✅ Yes | Unchanged |
| Pick selection and ranking | ✅ Yes | Unchanged |
| Output / card formatting | ✅ Yes | Minor adaptation for WC context |
| `sports_betting.db` | ✅ Yes | New tables added |
| Team strength from match history | ❌ No | Replaced by player aggregation |
| `analyse_match()` entry point | ❌ No | New entry point wraps same math |

---

## Open Questions (Deferred to Implementation)

- Exact formula for player-level xG → team λ aggregation
- League quality adjustment factors
- Star rating thresholds (EV gap sizes that map to 1 / 2 / 3 stars)
- Handling of Qatar and Saudi Arabia if TheStatsAPI coverage is insufficient