# Club-League Model Pipeline — Plain-English Overview

How `poisson_v4` (FEATURE-011) actually turns match history into a home/draw/away
probability, as built today. This is the as-built pipeline, not the original design intent —
see `FEATURE-011_REQUIREMENTS.md` for scenarios still unbuilt (e.g. Scenario 0's starting-lineup
projection, Scenario 9's coach-change handling). Code: `compute_club_player_strength.py`
(player-level + blend) and `core/poisson_model.py` (team-level + Poisson math).

## 1. Which players count

**Not** an "expected roster for this matchday." There is no lineup projection. A player
counts for a team if their **single most recent appearance** (as of the match being
predicted) was for that team — a simple, well-defined roster rule, not a full lineup
projection (that's still-unbuilt Follow-up A; see BUGS.md, FEATURE-011). An
injured/suspended/benched player who hasn't played recently simply isn't in the pool (no
separate availability signal exists).

**As of 2026-08-06 (FEATURE-011 Follow-up B), this is season-blind**: a player's rolling
window (section 2) reaches back across a season boundary, and follows the player across a
team or league change, the same way it reaches back across an ordinary matchday — no
special-cased "prior season" discount layered on top. This replaced the earlier
season-scoped mechanism, which had the gap the previous version of this doc flagged here: a
player sold in January used to keep their old team's stats live all season (nothing removed
them once they'd left) while a new arrival contributed ~nothing until they'd racked up
enough current-season minutes. The season-blind window fixes this directly — a transferred
player's rating now updates as soon as they've played their next few matches for the new
club, using their most recent appearances regardless of which team or season those were for.
The `player_trust_score` / roster-change signal (section 4 below) still exists independently
— it answers a different question (how much to trust the player-level number for this team
*at all*, given roster churn), not "what is this player's current rate."

## 2. Player-level attack/defense number

For each player still in scope (section 1): take their last `PLAYER_RATING_PAST_MATCH_WINDOW_SIZE`
appearances (10 by default; wherever they actually played, ignoring season boundaries),
decay-weighted by recency (`PLAYER_RATING_PAST_MATCH_WINDOW_DECAY`, currently 1.0 = no
decay, every game in the window counts equally), and sum goals (or xG, current default) and
minutes across that window. A game played in a league with no calibrated
cross-league adjustment (`PLAYER_RATING_CROSS_LEAGUE_GOAL_ADJUSTMENT`) is excluded from the
attack side of the window entirely, not assumed Serie-A-equivalent — see
`compute_club_player_strength.py`'s `load_team_players` docstring for the full mechanics.
This mirrors the team-level number's own window/decay shape (section 3) one level down.

Each player's rate is then weighted by **minutes and position** to build the team's raw
attack/defense number:
- Attack weight: forwards count fully, midfielders 60%, defenders 20%, keepers not at all.
- Defense weight: keepers count fully, defenders 80%, midfielders 30%, forwards 10%.

A smoothing step ("shrinkage") then pulls each player's own rate partway toward the
league-wide average for their position — hard for a thin-minutes player (a sub who's played
200 minutes gets pulled most of the way to the position average), barely at all for an
every-week starter (2000+ minutes). This keeps one small-sample fluky match from swinging a
team's whole rating.

## 3. Team-level attack/defense number

Independent of anything above — the pre-existing system, unchanged in mechanism. For a
team's **home** rating: its last 10 home matches, most recent first (an optional recency-decay
knob exists but is currently off, so all 10 count equally). **Away** rating: last 10 away
matches, same way — home and away are tracked completely separately, never blended into one
number. Below 10 matches this shrinks toward the league average, filling the cold-start gap.

As of Aug 2, this number is no longer pure actual goals — it's a blend of actual goals and
xG (currently 100% xG by default, `TEAM_RATING_XG_V_GOALS_BLEND=1.0`), switched because a
small sample of actual goals is noisy. This is the change that fixed BUG-009's pooled
home/away bias but introduced the "spread compression" side effect logged there.

## 4. Blending player-level and team-level

Independently for attack and for defense:

```
lambda = (1 - w) * player_number + w * team_number
```

`w` (0 = pure player, 1 = pure team) is computed **per team**, from two things multiplied
together — both have to be non-trivial for the player signal to be trusted at all:
- **Data coverage**: of the *current* squad, what fraction of their tracked minutes comes
  from players who logged real (900+ minute) time last season. A squad with lots of
  unknowns (kids, new-to-the-league signings) scores low here.
- **Roster change**: how much of last season's minutes belong to players who've since left,
  plus how much of the current squad is new arrivals. A team that kept its whole squad
  scores low here — meaning `w` is pulled toward 1 (team-level), specifically because a
  *stable* squad has *nothing extra* to gain from the player-level signal, not because the
  player data is bad.

This weight is recomputed from whatever "current squad" signal is available (live: today's
actual roster; backtest: a point-in-time reconstruction) — it does **not** change smoothly
match-by-match through a season the way, say, accumulated minutes do. A January transfer
window can move it in one step.

## 5. Poisson

The blended attack/defense numbers become `lambda_home`/`lambda_away`, fed into the same
Poisson scoreline grid the team-level-only system (`poisson_v3`) already used, producing
home/draw/away probabilities. Nothing downstream of this (EV calculation, pick selection)
changes between `poisson_v3` and `poisson_v4` — only how `lambda_home`/`lambda_away` are
produced.

## What this means for interpreting the MD20-28 anomaly

At the time this anomaly was first investigated, two mechanisms were implied by the above:
1. ~~**Departed players linger, new arrivals are invisible for a while** (section 1)~~ —
   **resolved by the season-blind rolling window (2026-08-06, FEATURE-011 Follow-up B).**
   A transferred player's rating now updates from their next few matches at the new club
   instead of staying anchored to their old team's stats for the rest of the season. If
   winter-transfer churn was contributing to the MD20-28 anomaly, this mechanism no longer
   applies — worth re-checking the anomaly against post-Follow-up-B data if it recurs.
2. **The blend weight `w` can jump in one step at the transfer window** (section 4) rather
   than drifting gradually — a team's roster-change score (and therefore how much the
   player signal is trusted) can move sharply right around MD20-22, exactly where the ROI
   spike starts. Still true; unaffected by the windowing change (a separate signal, see
   section 1).

Neither of these was true of the "cold start" mechanism fixed earlier (that was about
*promoted* teams having zero history at *season start* — a different failure mode from an
*established* team's mid-season roster changing under a number that doesn't know it).
