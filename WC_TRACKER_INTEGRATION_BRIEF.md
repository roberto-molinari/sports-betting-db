# World Cup 2026 — `serie-a-bets-tracker` Integration Brief

**Hand this file to a fresh Claude session opened in the `serie-a-bets-tracker`
repo.** It is self-contained: it carries the plan, the verified facts, and the
exact changes needed, so that session can do the port without the originating
conversation.

---

## Goal

Extend the existing `serie-a-bets-tracker` tool to publish + score **2026 FIFA
World Cup** picks, using the **same flow** that worked all Serie A season:

1. (In the *other* repo, `sports-betting-db`) run the model to generate the
   day's value picks: `python generate_wc_card.py --date YYYY-MM-DD`.
2. Manually transcribe those picks into the tracker's picks `.txt` file
   (`[PICKS]` block, same format as Serie A).
3. Run the tracker's `publish` to: fetch ChatGPT/Claude/Gemini picks for the same
   games, store locally, and post to X/Bluesky.
4. Next day, run the tracker's `score` to grade and post the scoreboard.

**Architecture decision:** `sports-betting-db` stays the **data/model backend**
(it owns the WC tables and generates the card). `serie-a-bets-tracker` is the
**picks + posting + scoring frontend**. It is a one-way dependency: the tracker
**reads** `sports_betting.db` for odds and **never writes** to it.

---

## What the tracker needs from the backend DB

- **DB file:** `/Users/robertomolinari/code/sports-betting-db/sports_betting.db`
  (the tracker already accepts an odds-DB path as a CLI arg — point it here).
- **The only hard dependency is ODDS** (for payout-based scoring). Fixtures and
  final scores come from ESPN (see below), and AI/your picks come from the APIs /
  your picks file. So the *only* DB-touching change is repointing the odds lookup.

### WC tables (mirror the Serie A tables the tool already queries)

| Serie A table (current) | World Cup table (new target) |
|---|---|
| `soccer_matches`        | `soccer_wc_matches` |
| `soccer_teams`          | `soccer_wc_teams` |
| `soccer_betting_odds`   | `soccer_wc_odds` |

Relevant columns:

- `soccer_wc_teams(team_id, name, confederation, fifa_ranking)` — **`name` already
  matches Bovada spellings** (e.g. `USA`, `Côte d'Ivoire`, `Czechia`,
  `Bosnia & Herzegovina`).
- `soccer_wc_matches(match_id, match_date, stage, grp, home_team_id,
  away_team_id, home_score, away_score, match_status)`.
  **`match_date` is stored in UTC** as `'YYYY-MM-DD HH:MM:SS'`.
- `soccer_wc_odds(match_id, sportsbook, odds_date, home_moneyline, draw_moneyline,
  away_moneyline, over_under, over_odds, under_odds)`. `sportsbook = 'Bovada'`.
  Odds are **American**.

### Eastern matchday detail (important)

The tournament is in North America and matchdays are reckoned in **US Eastern**.
`match_date` is UTC, so a late game (e.g. 10pm ET) is stored on the next UTC day.
To filter by the Eastern matchday, apply a fixed `-4 hours` shift (the whole
tournament Jun 11–Jul 19 is EDT = UTC−4, no DST transition inside the window):

```sql
WHERE date(m.match_date, '-4 hours') = ?   -- ? = 'YYYY-MM-DD' Eastern day
```

Happily, **ESPN's WC feed already groups fixtures by the same Eastern-ish day**,
so `dates=20260611` returns both the Mexico opener *and* the 10pm-ET
Korea–Czechia game — matching this convention for free.

---

## The concrete changes (all small; verified by spike)

### 1. Fixtures + results: swap the ESPN endpoint

`fetch_serie_a_fixtures()` is generic ESPN scoreboard parsing. Only the league
code changes:

- Current: `https://site.api.espn.com/apis/site/v2/sports/soccer/ita.1/scoreboard?dates=YYYYMMDD`
- World Cup: `https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/scoreboard?dates=YYYYMMDD`

**Verified:** this endpoint returns all 72 group-stage fixtures with
`competitors[].homeAway`, `team.displayName`, `score`, and
`status.type.state` (`pre`/`in`/`post`) — i.e. fixtures *and* final scores, just
like the Serie A feed. So both `publish` (pre) and `score` (post) work unchanged.

### 2. Repoint the odds lookups to the WC tables

Change `get_fixture_odds()` **and** `get_fixture_totals_odds()` to query the
`soccer_wc_*` tables with the Eastern-day filter. Verified working SQL (returns
1X2 *and* totals in one row; split as needed for the two functions):

```sql
SELECT o.home_moneyline, o.draw_moneyline, o.away_moneyline,
       o.over_under, o.over_odds, o.under_odds
FROM soccer_wc_odds o
JOIN soccer_wc_matches m ON o.match_id = m.match_id
JOIN soccer_wc_teams th ON m.home_team_id = th.team_id
JOIN soccer_wc_teams ta ON m.away_team_id = ta.team_id
WHERE date(m.match_date, '-4 hours') = ?   -- Eastern matchday 'YYYY-MM-DD'
  AND th.name = ?                           -- DB home name (after alias map)
  AND ta.name = ?                           -- DB away name (after alias map)
ORDER BY o.odds_date DESC
LIMIT 1
```

### 3. Add the ESPN→DB team-name alias map

ESPN's WC display names match our DB for **44 of 48** teams. Only these 4 differ
(apply before the odds query). Extend the tool's existing `TEAM_ALIASES`
mechanism, or add a dedicated map:

```python
ESPN_TO_DB = {
    "United States":       "USA",
    "Congo DR":            "DR Congo",
    "Ivory Coast":         "Côte d'Ivoire",
    "Bosnia-Herzegovina":  "Bosnia & Herzegovina",
}
```

(Group stage is fully covered. Knockout teams are TBD until brackets resolve —
re-check names then; there may be a couple more.)

### 4. Cosmetic relabeling

`pick_prompt()` and any user-facing "Serie A" strings → "World Cup". Optional.

---

## What is reused unchanged (do NOT rebuild)

- **Pick grammar + parsing:** `parse_pick_spec`, `parse_totals_selection`. WC
  picks use the same vocabulary: `HOME` / `DRAW` / `AWAY` / `OVER X` / `UNDER X`.
  (Bovada posts single totals lines, so the split-line support is unused but
  harmless.) **Verified** working on WC picks.
- **Scoring/payout:** `settle_totals_pick`, `settle_totals_leg`,
  `outcome_from_scores`, `build_scoreboard_reply_with_payout`. **Verified.**
- **AI fetchers:** `ask_openai` / `ask_claude` / `ask_gemini`, caching.
- **Posting:** Bluesky + X.
- **Tracking JSON:** `data/posted_picks.json`, the `publish`/`score` CLI,
  `--dry-run`, `--test-mode`, sim-results.

---

## Suggested build + verification order

1. Add the WC ESPN URL constant + the alias map.
2. Repoint `get_fixture_odds` / `get_fixture_totals_odds` to the WC tables.
3. `publish --day <tomorrow> --dry-run` (point odds-DB at `sports_betting.db`).
   Confirm it lists the right fixtures, fetches AI picks, accepts a picks file,
   and resolves odds for every game (incl. an aliased one like USA or Canada vs
   Bosnia). No posting.
4. `score --day <yesterday> --dry-run` against a finished matchday (or
   `--sim-results-file`) to confirm grading + payout scoreboard.
5. Go live.

---

## Facts already verified by a spike (trust these)

- ESPN `fifa.world` scoreboard returns 72 group fixtures + scores + state. ✅
- Exactly 4 team-name mismatches (listed above). ✅
- The repointed odds SQL resolves 1X2 + O/U for tested fixtures, including
  aliased teams and the Eastern-day filter. ✅
- The tracker's `parse_pick_spec` / `settle_totals_pick` / `outcome_from_scores`
  run unchanged on WC picks. ✅

## Gotchas / open items

- **Don't write to `sports_betting.db`** from the tracker — backend owns it.
- **Knockouts:** only the 72 group fixtures exist now; knockout fixtures/odds
  appear later and may need a couple more name aliases.
- The model's own card picks live in `soccer_wc_picks` in the backend DB; the
  tracker does **not** read that table. Your personal entry comes from the picks
  `.txt` file (which you fill using the card output). If you ever want the model
  tracked as its own entity, that's a separate, later decision.
