# Betting Edge Hypotheses

Tracks each hypothesis tested, the methodology used, and whether the edge held up across seasons.

---

## Hypothesis 001 — Away heavy underdogs are systematically undervalued by Bet365

**Status: FALSE**

### Claim
When the Poisson model assigns a 20–35% probability to an away win, but the market has them priced at 5.0+ decimal odds (implying ≤20%), the model is finding genuine mispricing worth betting.

### Definition
- **"Heavy underdog"**: decimal away odds ≥ 5.0 (implied p ≤ 20%)
- **"Model sees value"**: Poisson model p_away ∈ [20%, 35%]
- **EV threshold tested**: EV > 0%

### Test Methodology
- Dataset: Serie A, seasons 2022–2025 (four complete seasons, ~380 matches each)
- Train/test split: chronological 60/40 within each season
- Model: Poisson 1X2, no shrinkage (k=0), no recency decay (decay=1.0), RECENT_N=10
- Odds source: Bet365 B365H/B365D/B365A from football-data.co.uk CSVs
- Script: `inspect_away_bets.py --seasons 2022 2023 2024 2025 --ev-threshold 0.0`

### Results

| Season | Bets | Win rate | ROI |
|--------|------|----------|-----|
| 2022   |  8   |  12.5%   | -34.4% |
| 2023   |  7   |   0.0%   | -100.0% |
| 2024   | 13   |  30.8%   | +73.1% |
| 2025   |  8   |  50.0%   | +212.5% |
| **Combined** | **36** | **25.0%** | **~+30%** |

### Why It Fails
- Sample sizes of 7–13 bets per season at 5+ odds means a single win/loss swings ROI by 50–100 percentage points
- 2022 and 2023 are solidly negative; 2024/2025 positive results are consistent with noise in a high-variance bucket
- No theoretical reason the Poisson model should be specifically better than the market at identifying mislabelled heavy away underdogs

### Conclusion
2 of 4 seasons positive, 2 badly negative. The combined result is positive but driven entirely by a handful of wins at long odds. No stable edge.

---

## Hypothesis 002 — Home teams have positive EV when the away team is a medium underdog (+200 to +400)

**Status: FALSE**

### Claim
When the away team's moneyline is between +200 and +400, the home team has positive EV and betting the home moneyline produces positive ROI — regardless of what the Poisson model thinks.

### Definition
- **"Medium underdog"**: away American moneyline strictly between +200 and +400 (implied away win probability ~20–33%)
- **Bet placed**: home moneyline, $1 flat stake
- **No model filter**: the model's EV signal is intentionally excluded

### Test Methodology
- Dataset: Serie A, seasons 2022–2025 (four complete seasons, ~380 matches each)
- No train/test split — full season used (market-data-only test, no model)
- Odds source: Bet365 B365H/B365A from football-data.co.uk CSVs
- Script: `test_h002_home_vs_medium_dog.py`

### Results

| Season | Bets | Win rate | ROI |
|--------|------|----------|-----|
| 2022   | 126  |  45.2%   | -10.6% |
| 2023   | 122  |  50.8%   | +5.5% |
| 2024   | 126  |  42.1%   | -14.8% |
| 2025   | 108  |  48.1%   | +2.0% |
| **Total** | **482** | **46.5%** | **-4.8%** |

### Sub-band breakdown (all seasons combined)

| Away odds band | Bets | Win rate | ROI |
|----------------|------|----------|-----|
| +200 to +250   | 204  |  39.2%   | -9.9% |
| +251 to +300   | 116  |  50.9%   | +4.6% |
| +301 to +350   |  90  |  50.0%   | -6.1% |
| +351 to +400   |  72  |  55.6%   | -3.7% |

### Why It Fails
- Overall ROI is -4.8% across 482 bets — no edge
- The +200–+250 sub-band (the most common case, n=204) is consistently negative in 3 of 4 seasons at -9.9% combined; these are matches where the home team is the clear favourite and the market prices them correctly
- Higher bands (+301–+400) have too few bets per season-band cell to draw conclusions
- One weak positive signal in the +251–+300 range (see H003)

### Conclusion
False. No overall edge betting the home team when the away team is +200–+400. The broad band mixes meaningfully different situations.

---

## Hypothesis 003 — Home teams have positive EV when the away team is priced +251 to +300

**Status: INCONCLUSIVE — weak signal, not actionable**

### Claim
When the away team's moneyline is between +251 and +300, the home team has positive EV and betting the home moneyline produces positive ROI across seasons.

### Definition
- **"Away band"**: away American moneyline strictly between +251 and +300 (implied away win probability ~25–28%)
- **Bet placed**: home moneyline, $1 flat stake
- **No model filter**: market-data-only test

### Test Methodology
- Dataset: Serie A, seasons 2022–2025
- Script: `test_h002_home_vs_medium_dog.py --away-min 251 --away-max 300`

### Results

| Season | Bets | Win rate | ROI |
|--------|------|----------|-----|
| 2022   | 36   | 55.6%    | +11.2% |
| 2023   | 30   | 53.3%    | +10.2% |
| 2024   | 25   | 44.0%    | -10.4% |
| 2025   | 25   | 48.0%    | +3.2% |
| **Total** | **116** | **50.9%** | **+4.6%** |

### Why It's Inconclusive
- +4.6% combined ROI barely exceeds the bookmaker's vigorish (~4–5%); could entirely be noise
- 2–3 result swings per season (n=25–36) are enough to flip the sign of ROI
- This band was identified by inspecting the H002 sub-band breakdown — in-sample selection inflates apparent performance
- Would need 3+ additional out-of-sample seasons to establish with confidence

### Conclusion
Weak positive signal, positive in 3 of 4 seasons, but too small a margin and too small a sample to act on. Not actionable.

---

## Hypothesis 004 — Elite home teams are underpriced when listed as moderate favourites (-110 to -180)

**Status: FALSE**

### Claim
When an elite Serie A home team (top-6 finisher the prior season) is priced between -110 and -180, betting the home moneyline produces positive ROI across seasons.

### Definition
- **"Elite home team"**: finished in the top 6 of Serie A the prior season
- **"Moderate favourite"**: home American moneyline between -110 and -180 (implied home win probability ~52–64%)
- **Bet placed**: home moneyline, $1 flat stake
- **No model filter**: market-data-only test

### Test Methodology
- Dataset: Serie A, seasons 2023–2025 (prior-season top-6 used to tag elite teams)
- Script: `test_h004_elite_home_moderate_fav.py`

### Results

| Season | Bets | Win rate | ROI |
|--------|------|----------|-----|
| 2023   | 23   | 60.9%    | +1.2% |
| 2024   | 28   | 53.6%    | -9.7% |
| 2025   | 15   | 46.7%    | -22.7% |
| **Total** | **66** | **54.5%** | **-8.9%** |

### Why It Fails
- Inter (15 bets, -33.8% ROI) and Napoli (7 bets, -51.9% ROI) together account for 22 of 66 bets and are by far the worst performers — they are the "elite of the elite" and the market prices them correctly or even conservatively
- Excluding Inter and Napoli, the remaining elite teams (AS Roma, Bologna, Lazio, AC Milan, Atalanta, Juventus) show mixed but less catastrophic results
- The broad top-6 definition bundles genuinely dominant clubs with "mid-range elite" clubs, masking potential within-group differences

### Conclusion
False as defined. The top-6 grouping is too coarse — the dominant clubs (Inter, Napoli) are correctly priced or overpriced as home favourites, pulling the whole group negative.

---

## Hypothesis 005 — Mid-tier elite home teams are underpriced as moderate favourites (-110 to -180)

**Status: INCONCLUSIVE (promising, but 2025 season drag and small samples require caution)**

### Claim
When a mid-tier elite Serie A home team is priced between -110 and -180, betting the home moneyline produces positive ROI across seasons.

### Definition
- **"Mid-tier elite"**: within the prior season's top-6 finishers, the teams that do NOT sit above the largest points gap. The gap-based split objectively identifies a "dominant" sub-tier (teams with a clear points separation above them) and excludes them.
- **Gap threshold**: 5 points — the largest gap within the top-6 must be ≥5pts to trigger a split; otherwise all top-6 are treated as mid-tier.
- **"Moderate favourite"**: home American moneyline between -110 and -180
- **Bet placed**: home moneyline, $1 flat stake
- **No model filter**: market-data-only test

### How the gap split played out (threshold=5)

| Prior season | Dominant (excl.) | Mid-tier elite |
|---|---|---|
| 2022 | Napoli (90pts; +16pt gap) | Lazio, Juventus, Inter, AC Milan, Atalanta |
| 2023 | Inter (94pts; +19pt gap) | AC Milan, Juventus, Atalanta, Bologna, AS Roma |
| 2024 | Napoli + Inter (82/81pts; +7pt gap to 3rd) | Atalanta, Juventus, AS Roma, Lazio |

### Rationale
In H004, the dominant clubs (Inter, Napoli) produced catastrophic ROI (-33.8% and -51.9%) as home moderate favourites — they are so prominently "elite" that the market prices them efficiently or generously. The gap-based split removes them without naming them, leaving only clubs that are strong but not dominant. These clubs may receive less sharp-money attention as home favourites, leaving a potential underpricing edge.

### Test Methodology
- Dataset: Serie A, seasons 2023–2025
- Script: `test_h004_elite_home_moderate_fav.py --gap-threshold 5`
- Prior season standings computed from DB match results

### Results

| Group | Bets | Win rate | ROI |
|---|---|---|---|
| Dominant (excluded) | 16 | 25.0% | **-58.7%** |
| **Mid-tier elite** | **50** | **64.0%** | **+7.1%** |
| Non-elite | 92 | 53.3% | -7.3% |
| ALL | 158 | 53.8% | -8.0% |

#### Mid-tier by season

| Season | Bets | Win rate | ROI |
|--------|------|----------|-----|
| 2023   | 18   | 77.8%    | +29.3% |
| 2024   | 22   | 63.6%    | +7.8% |
| 2025   | 10   | 40.0%    | -34.5% |
| **Total** | **50** | **64.0%** | **+7.1%** |

#### Mid-tier by odds band

| Odds band | Bets | Win rate | ROI |
|---|---|---|---|
| -110 to -120 | 4  | 50.0% | -5.8% |
| -121 to -140 | 13 | 53.8% | -3.9% |
| -141 to -160 | 12 | 58.3% | -3.2% |
| **-161 to -180** | **21** | **76.2%** | **+22.2%** |

### Why It's Inconclusive
- 2 of 3 seasons positive (2023, 2024); 2025 strongly negative (-34.5% on only 10 bets)
- The 2025 season is not yet complete (3 matchdays remain as of May 2026), so final figures may shift slightly
- The -161 to -180 sub-band is the sole consistently positive segment (+22.2% overall); the shorter odds bands are all mildly negative
- Total sample across 3 seasons is 50 bets — borderline for drawing firm conclusions
- The gap threshold (5pts) and top-N (6) are parameters we chose after seeing H004 data, introducing some snooping risk

### Conclusion
The direction is right: removing gap-dominant teams flips the sign from -8.9% (H004) to +7.1%. But two positive seasons and one negative season, with 50 total bets, is not sufficient to declare an edge. The -161 to -180 sub-band warrants its own follow-up hypothesis. Record as promising/inconclusive pending more seasons of data.

---

## Hypothesis 006 — Mid-tier elite home teams at -161 to -180 are the concentrated edge within H005

**Status: PROMISING — 3/3 seasons positive, but post-hoc sub-segment and thin sample**

### Claim
When a mid-tier elite Serie A home team (gap-split, threshold=5) is priced between -161 and -180, betting the home moneyline produces positive ROI across seasons. This is the sub-band driving the H005 signal; the shorter-odds range (-110 to -160) has no edge.

### Definition
- **"Mid-tier elite"**: same as H005 — gap-based split of prior-season top-6, threshold=5pts
- **"Heavier moderate favourite"**: home American moneyline between -161 and -180 (implied win probability ~62–64%)
- **Bet placed**: home moneyline, $1 flat stake
- **No model filter**: market-data-only test

### Rationale
The H005 odds-band breakdown showed the -161 to -180 band was responsible for essentially all the positive ROI (+22.2%), while -110 to -160 was flat to mildly negative. The hypothesis is that mid-tier elite teams in this price range represent fixtures where the market's implied ~62–64% probability systematically underestimates actual win rates (~76% observed). These are strong home teams in matches where their quality advantage is slightly larger than a generic "moderate favourite" pricing implies.

### Test Methodology
- Dataset: Serie A, seasons 2023–2025
- Script: `test_h004_elite_home_moderate_fav.py --gap-threshold 5 --home-min -180 --home-max -161`
- Prior season standings computed from DB match results

### How the gap split played out (threshold=5)

| Prior season | Dominant (excl.) | Mid-tier elite |
|---|---|---|
| 2022 | Napoli (90pts; +16pt gap) | Lazio, Juventus, Inter, AC Milan, Atalanta |
| 2023 | Inter (94pts; +19pt gap) | AC Milan, Juventus, Atalanta, Bologna, AS Roma |
| 2024 | Napoli + Inter (82/81pts; +7pt gap to 3rd) | Atalanta, Juventus, AS Roma, Lazio |

### Results

| Group | Bets | Win rate | ROI |
|---|---|---|---|
| Dominant (excluded) | 7 | 28.6% | -55.1% |
| **Mid-tier elite** | **21** | **76.2%** | **+22.2%** |
| Non-elite | 18 | 55.6% | -10.9% |

#### Mid-tier by season

| Season | Bets | Win rate | ROI |
|--------|------|----------|-----|
| 2023   | 9    | 77.8%    | +25.4% |
| 2024   | 8    | 75.0%    | +19.4% |
| 2025   | 4    | 75.0%    | +20.5% |
| **Total** | **21** | **76.2%** | **+22.2%** |

#### Mid-tier by team (min 3 bets)

| Team | Bets | Win rate | ROI |
|---|---|---|---|
| AS Roma | 3 | 100.0% | +59.7% |
| Lazio   | 5 | 80.0%  | +28.6% |
| Bologna | 4 | 75.0%  | +19.8% |
| Inter   | 4 | 50.0%  | -19.0% |

### Why It's Promising But Not Confirmed
- **3/3 seasons positive** — the most consistent result found so far across any segment
- ROI is stable and tight: +25.4%, +19.4%, +20.5% — unusually low variance for a betting edge
- **However**: only 21 bets total, averaging 7 per season — one bad run could flip a season
- This sub-band was identified by inspecting the H005 odds-band breakdown — it is a post-hoc selection, introducing meaningful snooping risk
- The 2025 season sample (4 bets) is particularly thin; the season ends in ~3 matchdays as of May 2026
- Dominant clubs (Napoli, Inter) in this same price range produce -55.1% ROI — confirming the gap-split is doing real work

### What Would Confirm It
- 3+ additional out-of-sample seasons (2026, 2027, 2028) showing continued positive ROI in this band
- Or a theoretical mechanism explaining *why* 62–64% implied probability systematically underestimates true win rates for mid-tier elite home teams (e.g. fixture congestion for dominant clubs they are matched against)

### Conclusion
The strongest and most consistent signal found so far. 3/3 seasons, +22.2% ROI, 76% win rate vs 62–64% implied. Cannot be acted on yet without accepting snooping risk, but should be tracked closely going forward — especially in the 2025/26 remaining fixtures and the full 2026/27 season.

---

### Live Tracking (post-hypothesis bets)

Bets placed *after* H006 was formalised (May 8, 2026). These are the true out-of-sample observations.

| Date | Home | Away | ML | Result | Profit | Notes |
|------|------|------|----|--------|--------|-------|
| 2026-05-17 | Atalanta | Bologna | -168 | TBD | TBD | H006 qualifier; Atalanta prior-season 3rd (mid-tier elite) |

**Live record**: 0W–0L, 0 bets settled, ROI n/a
