# Value Bet Analysis Approach

## Overview
When asked for a value bet recommendation for a specific matchup, follow this systematic approach:

## Step 1: Identify Teams & Match
- Parse the user's request to extract team names and league
- Query `soccer_teams` table to get team IDs by name matching
- Query `soccer_matches` table to find the specific matchup (home_team_id, away_team_id)
- Locate the match_date and match_id

## Step 2: Gather Recent Form
- For each team, query recent completed matches (last 8 matches)
- Calculate key metrics:
  - **Win/Draw/Loss record** in last 5-10 games
  - **Goals scored** average per game
  - **Goals conceded** average per game
  - **Goal differential** trend
  - **Form trajectory** (improving/declining/stable)

## Step 3: Head-to-Head Analysis
- Query historical matchups between the two teams
- Identify patterns in home/away performance
- Track any recent results between these specific teams

## Step 4: Odds Research
- Query `soccer_betting_odds` table for the specific match_id
- Extract moneyline odds, spread odds, and over/under lines
- Compare odds across sportsbooks for line movement
- Calculate implied probability from odds

## Step 5: Value Assessment
Value bet exists when:
- **Implied probability from odds** < **Expected probability from form analysis**
- Look for mispricing based on:
  - Recent team form not yet reflected in odds
  - Injury/lineup factors
  - Public bias (home team bias, popular team bias, recency bias)
  - Historical matchup patterns

## Step 6: Make Recommendation
Provide the pick with reasoning based on:
1. Form advantage (which team is playing better)
2. Odds value (if available) - is the line offering good value?
3. Specific matchup dynamics
4. Clear recommendation: **Pick Team + Bet Type** (Moneyline/Spread/O/U)

## Example Output Format
```
Torino vs Sassuolo (May 10, 2026)

Analysis:
- Torino: 2W-1D-1L (last 4), avg 1.5 GF, 1.25 GA
- Sassuolo: 4W (last 4), avg 1.75 GF, 0.5 GA

Recent Form: Sassuolo significantly stronger
Odds: [if available - show implied probability vs actual]

Pick: Sassuolo ML (better form, better value at implied odds)
```

## Data Quality Notes
- Some matches may not have odds data yet (upcoming games)
- Use only completed matches (home_score IS NOT NULL) for form analysis
- Verify team names match exactly before analysis
- Check for data gaps or missing historical records

## Implementation
See `betting_analyzer.py` and `analyze_serie_a_advanced.py` for existing analysis functions that can be adapted for this workflow.

## Project Roadmap (Agreed Plan)

### What We Confirmed
- It is feasible to obtain the historical odds data needed for larger-scale validation.
- The current blocker is not feasibility, but building a production-grade odds import pipeline for the current schema.
- Existing historical odds coverage for Serie A is currently limited, so early formula work should use the smaller 2025-2026 subset.

### Phase 1: Formula Design on Current Data (Now)
- Build and test hand-crafted 1X2 expected-probability formulas using current in-database data.
- Evaluate candidate formulas on the existing odds-covered matches using:
  - ROI
  - Calibration metrics
- Select a reasonable starting formula based on evidence from this smaller sample.

### Phase 2: Historical Odds Backfill (Next)
- Implement a modern (non-legacy) Serie A odds ingestion pipeline for the current tables.
- Backfill multiple past seasons of odds data.
- Validate import quality via match-link coverage, duplicate checks, and sanity checks.

### Phase 3: Larger-Scale Iteration (After Backfill)
- Re-run backtests on the expanded historical dataset.
- Iterate expected-probability and confidence formulas with stronger statistical power.
- Promote the best-performing formula set into the main recommendation workflow.

### Guiding Principle
- Use the small dataset to get directionally correct quickly.
- Use expanded historical odds data to harden, validate, and iterate with confidence.
