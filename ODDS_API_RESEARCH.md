# Multi-Book / Real-Time Odds — Data Source Research

Exploratory research into whether a per-book pick feature (see the "serious
book-agnostic bettor" and "casual book-specific bettor" discussion) is even
feasible from a data-sourcing standpoint. Nothing here is implemented or
decided — this is a durable record of the research, not a spec. Pricing/coverage
figures came from web search of vendor pages and third-party comparison blogs
in July 2026; several third-party sources disagreed with each other on exact
tier prices (flagged inline below) — **confirm directly with the vendor before
using any number here to actually plan or budget.**

## Why this research exists

A per-book feature needs live, multi-book odds. This project has never had
that: every odds row in `soccer_wc_odds` today was typed in by hand from a
screenshot. No major US retail sportsbook (DraftKings, FanDuel, BetMGM,
Bovada, ...) publishes a public developer API — the only way to get multi-book
odds programmatically is through a third-party aggregator that has already
solved that problem. This doc surveys the self-serve aggregators (the ones
with published pricing, vs. sales-gated platforms like OddsJam/OpticOdds/
Sportradar built for betting operators).

## Providers surveyed

### The Odds API (the-odds-api.com)

- **Sports/leagues:** 34+ sports. Soccer: EPL, EFL Championship, Bundesliga,
  La Liga, Serie A, Ligue 1, UEFA Champions/Europa League, Campeonato
  Brasileiro Série A, MLS (paid plans), "and much more" per their sports-APIs
  page — no single exhaustive soccer-league list found; worth checking
  `/v4/sports` directly.
- **Books (US region):** DraftKings, FanDuel, BetMGM, BetRivers, Unibet,
  William Hill, and others — full current list is on their bookmaker-APIs
  page (not fully enumerated in what I could pull via search).
- **Freshness:** no separate "speed" tier — same near-live data across all
  paid tiers; you pay for volume (credits), not for lower latency.
- **Billing model:** credits = markets × regions per call (e.g. 3 markets ×
  2 regions = 6 credits/call). Historical odds cost 10x more. Free tier: 500
  credits/mo (~83 typical multi-market calls).
- **Pricing:** Free (500 credits) · $30/mo (20K credits) · $59/mo (100K) ·
  $119/mo (5M) · $249/mo (15M). ~20-30% discount codes reportedly available.
- **Watch-out for this project's use case:** props are billed the same
  credit way but typically require per-event calls (not bulk per-league), so
  a props-heavy usage pattern could push cost up a full tier or two versus a
  1X2/totals-only estimate.

### SportsGameOdds (sportsgameodds.com)

- **Sports/leagues:** 28+ sports, 67+ leagues, 85+ books. Soccer: EPL, La
  Liga, Serie A, Ligue 1, MLS, Liga MX, Champions League, Europa League,
  "international soccer," and more — again no single exhaustive list found,
  check the `/leagues` endpoint directly for a given API key.
- **Books:** Pinnacle, DraftKings, FanDuel, BetMGM, Bet365, Caesars, Bovada,
  BetRivers, Circa, ESPN BET, Fanatics, Hard Rock Bet, MyBookie, PointsBet,
  and many more (85+ total) — plus DFS pick'em (PrizePicks, Underdog) and
  prediction markets (Kalshi, Polymarket).
- **Freshness — tiered by plan, this is the key differentiator vs. the others:**
  - Free: 9 books, 10-minute delay
  - Entry paid tier (reported as **$99/mo OR $149/mo** depending on source —
    unresolved conflict, confirm directly; possibly annual vs. monthly
    billing): 77 books, 3-minute delay
  - $249/mo+: WebSocket streaming, sub-second latency
- **Billing model:** "objects" — each event returned counts as one object,
  regardless of how many books/markets are in it. Flatter, more predictable
  than The Odds API's credit system for a broad-market, props-heavy use case.
- **Pricing:** Free · ~$99–149/mo · $249/mo · up to $499/mo top tier.

### OddsPapi (oddspapi.io)

- **Sports/leagues:** 59 sports, 300+ books claimed overall. Soccer coverage
  is the deepest of the three: **1,372 football tournaments** — top-5 European
  leagues down through lower divisions, youth, and women's football, plus
  Champions League, Europa League, MLS, Brasileirão, Eredivisie, Primeira
  Liga, Scottish Premiership, Süper Lig, and hundreds more.
- **Books:** DraftKings, FanDuel, BetMGM, Bovada, Caesars, BetRivers, Hard
  Rock, BetParx, Borgata, theScore Bet, BetOnline, Fanatics, Fliff, Circa
  (US) plus Bet365, Pinnacle, Betfair, Unibet, William Hill, Paddy Power, and
  many more internationally (300+ total, 140+ per individual match).
- **Freshness:** not clearly published in what I found — no stated delay-by-
  tier structure like SportsGameOdds has. Needs a direct check.
- **Billing model:** free tier is 250 requests/month (each request returns
  all 350+ books at once) — too thin for continuous polling. Paid tier
  starts ~$49/mo and scales with volume via custom pricing, not fixed steps.
- **Pricing:** least well-defined of the three for a firm monthly number —
  would need their actual quote for a specific usage pattern.

## Cost estimate under an example usage pattern

Assumptions used: 10 leagues, ~5-minute update cadence, 3 market types
(1X2, over/under, props).

| Provider | Estimated tier | Est. cost/mo | Reasoning |
|---|---|---|---|
| The Odds API | 5M-credit tier | ~$119/mo (could reach $249/mo w/ heavy props use) | 1X2+totals across 10 leagues is cheap (~20 credits/poll); props billed per-event separately is the cost risk |
| SportsGameOdds | Rookie tier | ~$99–149/mo | 5-min cadence doesn't need the $249 real-time WebSocket tier; flat per-event billing doesn't punish props the way credit billing does |
| OddsPapi | Custom | $49/mo+ | Free tier's 250 req/mo is far too thin for this cadence over a month; real number needs a direct quote |

**Bottom line so far:** all three land in the $100–250/month range for this
usage pattern, not the ~$30 entry-tier price that a naive read of "starts at
$30/mo" would suggest. SportsGameOdds looks like the best fit on paper for
this specific pattern (5-min cadence, props included) but that read is based
on secondhand pricing summaries with an unresolved internal contradiction on
its entry-tier price — confirm before treating it as decision-grade.

## Open questions / next steps (not started)

- Confirm exact soccer-league lists per provider directly against each
  `/leagues` or `/sports` endpoint (this doc's lists are as reported by
  search results, not verified against live API responses).
- Resolve the SportsGameOdds $99 vs $149 entry-tier conflict directly with
  the vendor.
- Get an OddsPapi quote for the specific usage pattern above (their pricing
  isn't published in fixed tiers).
- Confirm freshness/latency numbers directly for The Odds API and OddsPapi
  (only SportsGameOdds had a clearly tiered, published delay structure).
- Model actual call volume against a real match calendar (not a flat "every
  5 minutes, 24/7" assumption) — WC-style tournaments cluster games into a
  few hours/day, which likely changes the estimate meaningfully.
