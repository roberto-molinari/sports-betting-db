"""
FEATURE-011 prototype: compute player-based attack/defense lambdas for club-league
teams and blend them against the EXISTING team-level lambda system, per the agreed
Player-Level Strength Estimation design (FEATURE-011_REQUIREMENTS.md):

    lambda = (1 - w) * player_lambda + w * team_lambda      (independently for
                                                               attack and defense)

Reuses compute_wc_team_strength.py's aggregation approach (position-weighted rates,
minutes-based shrinkage toward positional priors) but drops the WC-specific FIFA-rank
fallback/blend entirely -- the "other side" of the blend here is the EXISTING
club-league team-level lambda (core.poisson_model.get_team_ratings), not FIFA rank.

Still out of scope (see FEATURE-011_BUILD_TRACKER.md for the current task list):
  - single scalar attack/defense per team, no home/away split (Scenario 4 deferred)
  - no league-quality factor applied: all players in scope play in the SAME league
    (Serie A), so LEAGUE_FACTORS would be a no-op here. It becomes relevant once a
    cross-league transfer scenario (Scenario 7) is in play.

Usage:
    python compute_club_player_strength.py --league "Serie A" --team "AC Milan"
    python compute_club_player_strength.py --league "Serie A" --limit-teams 3 --persist
"""

import argparse
import sqlite3
from datetime import date
from statistics import mean, pstdev

from core.sports_db import DATABASE_PATH, set_player_team_strength
from core.poisson_model import (
    get_team_ratings, get_league_averages,
    TEAM_RATING_MIN_MATCHES_TO_TRUST_TEAM_RATING_OVER_LEAGUE_AVERAGE,
    TEAM_RATING_PULL_TOWARD_AVERAGE_MATCHES,
    TEAM_PAST_MATCH_WINDOW_SIZE,
    _shrink,
)

# Same weights/rationale as compute_wc_team_strength.py (BUG-001/BUG-002 family):
# forwards carry attack, defenders/keepers carry defense, midfield contributes to both.
PLAYER_RATING_POSITION_ATTACK_WEIGHTS = {"FWD": 1.0, "MID": 0.6, "DEF": 0.2, "GK": 0.0}
PLAYER_RATING_POSITION_DEFENSE_WEIGHTS = {"GK": 1.0, "DEF": 0.8, "MID": 0.3, "FWD": 0.1}

# Same half-trust point as the WC system -- ~10 matches, set from how football works,
# not fit to this league.
PLAYER_RATING_MINUTES_TO_HALF_TRUST_OWN_RATE_OVER_LEAGUE_AVERAGE = 900.0

# Player-level rolling window (2026-08-06, MODEL_TUNING_PARAMETERS.md/BUGS.md FEATURE-
# 011 Follow-up B) -- replaces the old flat season-to-date sum load_team_players used
# to compute. Mirrors TEAM_PAST_MATCH_WINDOW_SIZE/_DECAY's shape exactly, one level
# down (player instead of team): a player's attack/defense rate is built from their
# last N appearances, decay-weighted by recency, SEASON-BLIND -- the window reaches
# back across a season boundary (and across a team/league change, following the
# PLAYER not the roster slot) the same way it reaches back across a matchday, with no
# special-cased "prior season" discount layered on top. This replaces the old
# blend_prior_season_attack/PRIOR_SEASON_DISCOUNT mechanism entirely (retired the same
# day this was added) -- that mechanism was a cruder, season-boundary-shaped version of
# exactly this same idea. Starting values match TEAM_PAST_MATCH_WINDOW_SIZE/_DECAY
# (10 games, decay=1.0/off) -- not yet independently tuned.
PLAYER_RATING_PAST_MATCH_WINDOW_SIZE = 10
PLAYER_RATING_PAST_MATCH_WINDOW_DECAY = 1.0

# Split 2026-08-06 (MODEL_TUNING_PARAMETERS.md) -- these used to be one constant each
# (MIN_ATTACK_WEIGHT/MIN_DEFENSE_WEIGHT) doing double duty: gating both whether a team
# gets its OWN player-based rating at all, and separately whether that team's raw
# number counts toward the league-wide average other teams get recentered against.
# Same starting value for both of each pair (300.0, lower than WC's 1000 -- a
# single-league squad has fewer thin-coverage players diluting the pool than a WC
# squad does) -- no behavior change from the split itself, just decoupled so either
# can be tuned independently later.
PLAYER_RATING_MIN_ATTACK_WEIGHTED_MINUTES_TO_HAVE_OWN_RATING = 300.0
PLAYER_RATING_MIN_ATTACK_WEIGHTED_MINUTES_TO_JOIN_LEAGUE_AVERAGE = 300.0
PLAYER_RATING_MIN_DEFENSE_WEIGHTED_MINUTES_TO_HAVE_OWN_RATING = 300.0
PLAYER_RATING_MIN_DEFENSE_WEIGHTED_MINUTES_TO_JOIN_LEAGUE_AVERAGE = 300.0

# Blend-weight resolution (FEATURE-011_REQUIREMENTS.md, Blend, resolved 2026-07-30).
# "Last season" (recency, not career totals) minutes a player needs before they count
# as a strong individual signal for the data-coverage score below.
PLAYER_RATING_MIN_MINUTES_FROM_PRIOR_SEASON = 900.0

# {league: {"attack": w, "defense": w}} -- a coarse override that forces EVERY team in
# that league to the same weight for that component, taking precedence over each
# team's computed default. `w` is this file's usual blend convention (0=pure player,
# 1=pure team) -- same as blend()/soccer_player_team_strength.weight_attack. Empty by
# default; add entries only with a documented reason, same convention as WC's
# FIFA_BLEND_OVERRIDES.
PLAYER_RATING_LEAGUE_WIDE_BLEND_WEIGHT_OVERRIDE = {}

# Cross-league attack-rate conversion for prior_season_attack_rate/blend_prior_season_
# attack (2026-08-03, FEATURE-011/BUG-010's cross-league player-history design) -- a
# goal in a weaker division doesn't predict the same Serie A output, so a prior-
# season rate sourced from a different league is scaled by this factor before being
# blended in. Deliberately GOALS-based, not xG: TheStatsAPI's xG coverage is real for
# Serie A and Championship, but Serie B/2. Bundesliga/Ligue 2/LaLiga 2 all return
# expected_goals=0.0 on every single shot (checked directly, incl. players with
# multiple shots and real goals) despite the competition metadata's xg_available=true
# claiming otherwise -- goals is the one signal available uniformly.
#
# Serie B's 0.663 is EMPIRICALLY measured (not guessed, per this project's "verify
# before building" discipline): 82 players with >=300 minutes in BOTH Serie A and
# Serie B (any season) compared to THEMSELVES across leagues -- 0.0956 goals/90 in
# Serie A vs 0.1442 in Serie B, pooled by minutes. Cross-checked against
# compute_wc_team_strength.py's separate, subjective LEAGUE_FACTORS table, which
# already had "Serie B": 0.60 for a different purpose (predicting INTERNATIONAL
# scoring from club form) -- close agreement, a reasonable sanity check though not
# the same question. A league with NO entry here is excluded from the prior-season
# blend entirely, never assumed equal to Serie A (see player_prior_season_attack_rate).
PLAYER_RATING_CROSS_LEAGUE_GOAL_ADJUSTMENT = {
    "Serie A": 1.0,
    "Serie B": 0.663,
}

# Team-level attack/defense blend between actual-goals-based and xG-based sources
# (2026-08-02/2026-08-05, BUG-009's mismatch-size-compression diagnosis). 0.0=pure
# goals (matches poisson_v3 exactly), 1.0=pure xG (DEFAULT, a true no-op matching the
# fix that cleared the Model Calibration success criterion -- a small sample of actual
# goals proved too noisy), values in between blend the two. Promoted from a bare
# function default to a named constant 2026-08-06 (MODEL_TUNING_PARAMETERS.md) so it's
# discoverable alongside the other tunable knobs instead of buried in two function
# signatures. See team_level_lambda's docstring for the full derivation.
TEAM_RATING_XG_V_GOALS_BLEND = 1.0

# Spreads team-level xG ratings' cross-team dispersion back out, toward (but not all
# the way to) actual-goals-level dispersion (2026-08-07, BUG-009's compression
# finding continued: xG has LESS team-to-team spread than actual goals by
# construction, which compresses win probabilities toward a coin flip specifically
# on the biggest favorites/underdogs -- worst in matches like a bottom-table team
# hosting a top-table one). For each of get_team_xg_ratings' four fields
# (home/away attack/defense), recenters on that field's own league-wide mean at the
# same before_date, then scales the deviation from that mean by this factor:
#     stretched = league_mean + (raw - league_mean) * factor
# 1.0 is an exact no-op (today's pre-2026-08-07 behavior). Swept 1.0/1.3/1.66/2.0
# (ad hoc, both Serie A seasons): bottom6-vs-other probability gap shrinks
# monotonically and ROI improves in BOTH seasons as the factor increases -- notably
# different from every other compression fix tried (blend, this constant's sibling
# above) which always showed one season winning while the other lost -- but pooled
# away-side bias grows with the stretch and breaches the +/-0.01-0.02 Model
# Calibration target at 1.66 in one season and at 2.0 in both. 1.3 is the largest
# value tested that stays inside the target in both seasons, so it's the DEFAULT
# here rather than 1.0 -- pass 1.0 to reproduce the exact pre-2026-08-07 shape for
# comparison. See BUGS.md, BUG-009, 2026-08-07 addendum for the full sweep and the
# combined-with-blend test (negative result -- don't combine the two).
TEAM_RATING_XG_SPREAD_STRETCH = 1.3

_MID_CODES = {"m", "mf", "cm", "dm", "am", "cdm", "cam", "rm", "lm", "mid"}
_DEF_CODES = {"d", "df", "cb", "lb", "rb", "wb", "rwb", "lwb", "def"}
_FWD_CODES = {"f", "fw", "fwd", "st", "cf", "ss", "rw", "lw"}


def normalize_position(pos):
    if not pos:
        return None
    p = pos.strip().lower()
    if "goal" in p or p in {"gk", "g"}:
        return "GK"
    if "midfield" in p or "winger" in p or p in _MID_CODES:
        return "MID"
    if "back" in p or "defen" in p or p in _DEF_CODES:
        return "DEF"
    if "forward" in p or "striker" in p or "attack" in p or p in _FWD_CODES:
        return "FWD"
    return None


def load_team_players(conn, team_ids, before_date, attack_xg_v_goals_source="xg",
                      defense_xg_v_goals_source="xga",
                      window_size=PLAYER_RATING_PAST_MATCH_WINDOW_SIZE,
                      decay=PLAYER_RATING_PAST_MATCH_WINDOW_DECAY,
                      league_strength=None, min_date=None):
    """Build one per-player-per-TEAM rating entry from each player's own last
    `window_size` appearances (2026-08-06, FEATURE-011 Follow-up B) -- replaces the
    old flat season-to-date sum (see MODEL_PIPELINE_OVERVIEW.md's original section 2,
    now out of date) and the separate blend_prior_season_attack/PRIOR_SEASON_DISCOUNT
    mechanism it needed to compensate for thin-current-season players (both retired
    the same day this landed).

    before_date: ISO date string, restricting to matches strictly before it (BUG-008
    no-lookahead discipline -- required now, not optional, since without a window
    bound at all a season-blind query has no other stopping point).

    SEASON-BLIND: the window reaches back across a season boundary the same way it
    reaches back across a matchday -- no special casing, no separate discount layered
    on top of decay. It also follows the PLAYER across a team or league change (their
    last N appearances, wherever they happened), not just the current team's own
    matches -- so a just-transferred player isn't cold-started back to zero the way a
    team-scoped window would leave them.

    ROSTER MEMBERSHIP: since "current season roster" no longer bounds anything, a
    player is attributed to whichever team their SINGLE most recent appearance
    (strictly before before_date) was for -- simple, well-defined, and season-blind,
    without attempting Follow-up A's fuller roster/lineup-projection scope (still
    unbuilt; see BUGS.md FEATURE-011). A player whose most recent appearance wasn't
    for a team in `team_ids` doesn't appear in the result at all (e.g. they left the
    league, or their most recent team isn't one being computed for right now).

    min_date: optional ISO date lower bound -- when given, the window additionally
    can't reach earlier than this (e.g. a season's start date), for A/B-comparing a
    season-scoped window against the season-blind default. None (default) means truly
    season-blind: reach back as far as necessary to fill the window.

    Rates are computed from decay-weighted summed totals (goals/xg over minutes), not
    by averaging each match's own per-90 rate -- same "sum before rate" reasoning as
    before (a single sub appearance of a few minutes would otherwise produce a wildly
    noisy per-match rate that a simple average wouldn't smooth out), now with each
    game additionally weighted by decay**rank (rank 0 = most recent in the window).

    attack_xg_v_goals_source: "xg" (default) uses real xG when at least one game in
    the window has it, falling back to goals for every game in the window otherwise
    (never mixing units within one player's rate) -- matching
    compute_wc_team_strength.py's fallback. "goals" always uses actual goals.

    defense_xg_v_goals_source: "xga" (default) uses club_xga_per90 (the opposing
    team's xG that match) when a game has it, falling back to club_ga_per90 (actual
    goals conceded) otherwise. "actual" forces club_ga_per90 always.

    Cross-league adjustment (`league_strength`, defaults to
    PLAYER_RATING_CROSS_LEAGUE_GOAL_ADJUSTMENT) is applied per-game to the ATTACK side
    only, same established asymmetry as before (BUG-010: a defense-side equivalent
    would need its own calibration) -- a game played in a league with NO factor entry
    is excluded entirely from the attack calculation (both the goal/xg numerator and
    the minutes denominator for that one game), not assumed Serie-A-equivalent. This
    can make a player's effective attack-side window smaller than their defense-side
    window when some games fall in an uncalibrated league -- both `attack_minutes` and
    `defense_minutes` are returned separately (decay-weighted, NOT raw minutes) rather
    than one shared `minutes` field, since they can legitimately differ.

    Each game is attributed to the team the player actually played for IN THAT MATCH
    (derived from venue + soccer_matches.home/away_team_id), NOT soccer_players.team_id
    (their most-recently-seen team, per add_player) -- confirmed necessary for real
    once this ran across the full 20-team Serie A (29 players, 463 rows misattributed
    when this used soccer_players.team_id).
    """
    league_strength = PLAYER_RATING_CROSS_LEAGUE_GOAL_ADJUSTMENT if league_strength is None else league_strength
    cur = conn.cursor()
    sql = """
        SELECT s.player_id, p.position, s.minutes_played, s.goals, s.xg,
               s.club_ga_per90, s.club_xga_per90,
               s.venue, m.home_team_id, m.away_team_id, m.match_date, m.league
        FROM soccer_player_stats s
        JOIN soccer_players p ON p.player_id = s.player_id
        JOIN soccer_matches m ON m.match_id = s.match_id
        WHERE s.match_id IS NOT NULL AND s.venue IS NOT NULL AND m.match_date < ?
    """
    params = [before_date]
    if min_date is not None:
        sql += " AND m.match_date >= ?"
        params.append(min_date)
    sql += " ORDER BY s.player_id, m.match_date DESC"
    cur.execute(sql, params)

    by_player = {}
    for player_id, position, minutes, goals, xg, club_ga90, club_xga90, venue, home_id, away_id, match_date, m_league in cur.fetchall():
        match_team_id = home_id if venue == "home" else away_id
        by_player.setdefault(player_id, []).append({
            "pos": normalize_position(position), "minutes": minutes or 0, "goals": goals or 0,
            "xg": xg, "club_ga90": club_ga90, "club_xga90": club_xga90,
            "team_id": match_team_id, "match_date": match_date, "league": m_league,
        })

    team_id_set = set(team_ids)
    by_team = {tid: [] for tid in team_ids}
    for player_id, games in by_player.items():
        # SQL already ORDER BY match_date DESC per player -- games[0] is most recent.
        current_team = games[0]["team_id"]
        if current_team not in team_id_set:
            continue
        window = games[:window_size]
        has_xg = attack_xg_v_goals_source == "xg" and any(g["xg"] is not None for g in window)

        attack_num = attack_den = 0.0
        ga_num = ga_den = 0.0
        xga_num = xga_den = 0.0
        for rank, g in enumerate(window):
            w = decay ** rank
            factor = league_strength.get(g["league"])
            if factor is not None:
                # When has_xg (ANY game in the window has real xg), a game WITHOUT
                # its own xg contributes 0, not its goals -- never mix units within
                # one player's rate. Only fall back to goals per-game when the whole
                # window has no xg at all (has_xg False).
                goal_val = (g["xg"] or 0) if has_xg else g["goals"]
                attack_num += w * factor * (goal_val or 0)
                attack_den += w * g["minutes"]
            if g["minutes"]:
                if g["club_ga90"] is not None:
                    ga_num += w * g["minutes"] * g["club_ga90"]
                    ga_den += w * g["minutes"]
                if g["club_xga90"] is not None:
                    xga_num += w * g["minutes"] * g["club_xga90"]
                    xga_den += w * g["minutes"]

        if attack_den <= 0:
            continue

        attack_rate = attack_num / attack_den * 90
        if defense_xg_v_goals_source == "xga" and xga_den > 0:
            club_ga_per90, defense_minutes = xga_num / xga_den, xga_den
        elif ga_den > 0:
            club_ga_per90, defense_minutes = ga_num / ga_den, ga_den
        else:
            club_ga_per90, defense_minutes = None, 0.0

        by_team[current_team].append({
            "player_id": player_id,
            "pos": games[0]["pos"],
            "attack_rate": attack_rate,
            "attack_minutes": attack_den,
            "club_ga_per90": club_ga_per90,
            "defense_minutes": defense_minutes,
        })
    return by_team


def positional_priors(by_team, field, weight_field):
    num, den = {}, {}
    for players in by_team.values():
        for p in players:
            pos, val, mins = p["pos"], p.get(field), p.get(weight_field)
            if pos and val is not None and mins:
                num[pos] = num.get(pos, 0.0) + mins * val
                den[pos] = den.get(pos, 0.0) + mins
    return {pos: num[pos] / den[pos] for pos in num}


# field -> which decay-weighted minutes field measures its credibility for shrinkage/
# aggregation weighting. Both fields are always present now that load_team_players
# itself is windowed/decayed (2026-08-06) -- no more raw-vs-combined-minutes distinction
# to fall back between (that distinction existed only to compensate for the old flat
# season-to-date sum via blend_prior_season_attack, retired the same day this landed).
# attack_minutes and defense_minutes can still legitimately differ per player (a game
# in an uncalibrated league is excluded from attack_minutes but not defense_minutes --
# see load_team_players).
SHRINKAGE_WEIGHT_FIELD = {"attack_rate": "attack_minutes", "club_ga_per90": "defense_minutes"}


def apply_shrinkage(by_team, k_minutes=PLAYER_RATING_MINUTES_TO_HALF_TRUST_OWN_RATE_OVER_LEAGUE_AVERAGE):
    for field in ("attack_rate", "club_ga_per90"):
        weight_field = SHRINKAGE_WEIGHT_FIELD[field]
        prior = positional_priors(by_team, field, weight_field=weight_field)
        for players in by_team.values():
            for p in players:
                pos, val = p["pos"], p.get(field)
                mins = p.get(weight_field)
                if pos in prior and val is not None and mins:
                    p[field] = (mins * val + k_minutes * prior[pos]) / (mins + k_minutes)


def raw_team_strength(players):
    a_num = a_w = d_num = d_w = 0.0
    for p in players:
        pos = p["pos"]
        if pos is None:
            continue
        if p["attack_rate"] is not None:
            w = p["attack_minutes"] * PLAYER_RATING_POSITION_ATTACK_WEIGHTS.get(pos, 0.0)
            if w > 0:
                a_num += w * p["attack_rate"]
                a_w += w
        if p["club_ga_per90"] is not None:
            w = p["defense_minutes"] * PLAYER_RATING_POSITION_DEFENSE_WEIGHTS.get(pos, 0.0)
            if w > 0:
                d_num += w * p["club_ga_per90"]
                d_w += w
    raw_attack = (a_num / a_w) if a_w > 0 else None
    raw_defense = (d_num / d_w) if d_w > 0 else None
    return raw_attack, a_w, raw_defense, d_w


def player_season_minutes(conn, season):
    """{player_id: total minutes played in `season`, summed across ALL teams/matches.

    Deliberately NOT team-scoped -- a summer transfer's minutes at their PREVIOUS club
    count in full toward their data-coverage signal (no cross-club discount), which is
    the entire point of tracking player identity across a transfer (api_player_id)."""
    cur = conn.cursor()
    cur.execute("""
        SELECT player_id, SUM(minutes_played)
        FROM soccer_player_stats
        WHERE season = ? AND match_id IS NOT NULL
        GROUP BY player_id
    """, (season,))
    return {pid: mins or 0 for pid, mins in cur.fetchall()}


def team_roster_minutes(conn, team_id, season, before_date=None):
    """{player_id: minutes played AT team_id specifically in `season`} -- team-scoped,
    since this answers "how much of team_id's OWN production is this player part of,"
    not the player's overall total (that's player_season_minutes).

    before_date: optional ISO date string, restricting to matches strictly before it.
    Used two ways: with no before_date, this gives a fully-completed season's final
    roster (safe -- the whole season is in the past by construction). WITH a
    before_date, it gives a point-in-time-correct read of the CURRENT season's roster
    so far, for backtesting (see squad_as_of_date) -- no lookahead, since it only
    counts matches that had already happened."""
    cur = conn.cursor()
    sql = """
        SELECT s.player_id, SUM(s.minutes_played)
        FROM soccer_player_stats s
        JOIN soccer_matches m ON m.match_id = s.match_id
        WHERE s.season = ? AND s.match_id IS NOT NULL AND s.venue IS NOT NULL
          AND ((s.venue = 'home' AND m.home_team_id = ?)
               OR (s.venue = 'away' AND m.away_team_id = ?))
    """
    params = [season, team_id, team_id]
    if before_date is not None:
        sql += " AND m.match_date < ?"
        params.append(before_date)
    sql += " GROUP BY s.player_id"
    cur.execute(sql, params)
    return {pid: mins or 0 for pid, mins in cur.fetchall()}


def current_squad_player_ids(conn, team_id):
    """Players whose most-recently-seen team (soccer_players.team_id, see add_player's
    api_player_id-first identity resolution) is team_id right now -- the best available
    read of "who's on the roster today," not scoped to any particular season.

    LIVE use only. This field only ever holds the single latest known team, so it
    can't answer "who was on the roster as of a PAST date" -- for backtesting, use
    squad_as_of_date instead (derived from real match appearances, so it's actually
    point-in-time correct)."""
    cur = conn.cursor()
    cur.execute("SELECT player_id FROM soccer_players WHERE team_id = ?", (team_id,))
    return {row[0] for row in cur.fetchall()}


def squad_as_of_date(conn, team_id, season, before_date):
    """Point-in-time "who's on this roster" for BACKTESTING a past season, where
    current_squad_player_ids can't be trusted (soccer_players.team_id reflects TODAY's
    state, not what was true at before_date). Derived from real match appearances:

    1. Players who've played for team_id in `season`'s own matches so far (before
       before_date) -- the season's own transfer activity, as it becomes visible.
    2. If none yet (very early in the season, before any match has revealed summer
       transfer activity), falls back to last season's full roster -- an honest
       approximation: with no historical squad-list snapshot available, the first
       matchday or two assumes roster continuity until match evidence says otherwise.
       This understates day-one churn; a documented, bounded limitation (a season has
       ~38 matchdays; this affects the first one or two), not a lookahead leak.

    NOT for live use -- current_squad_player_ids is the better signal there (updated
    from a squad-list pull before the season begins, so it knows about a transfer
    before a ball is kicked; this only knows once the player actually plays)."""
    this_season = team_roster_minutes(conn, team_id, season, before_date=before_date)
    if this_season:
        return set(this_season.keys())
    return set(team_roster_minutes(conn, team_id, season - 1).keys())


def player_trust_score(conn, team_id, season, current_squad_ids=None):
    """1.0 = fully trust the player-level lambda for this team; 0.0 = fully trust
    team-level. Two factors, BOTH required (product, not sum/average) -- a stable,
    well-tracked squad has nothing to gain from the player signal even with great
    coverage, and a squad we don't know yet still can't be trusted even if it churned
    completely (FEATURE-011_REQUIREMENTS.md, Blend):

    - data_coverage_score: of the CURRENT squad's tracked last-season minutes, the
      fraction belonging to players with >= PLAYER_RATING_MIN_MINUTES_FROM_PRIOR_SEASON last season
      (recency, not career totals -- team-agnostic, see player_season_minutes).
    - roster_change_score: last season's minutes lost to departed players (at THIS
      team specifically) plus incoming players' last-season minutes (wherever they
      played), as a fraction of the team's own total minutes last season. High churn
      means last season's team-level number describes a squad that's mostly gone.

    current_squad_ids: optional override for "who's on the roster right now" --
    defaults to current_squad_player_ids() (the live signal). Backtesting a PAST
    season must pass a point-in-time squad instead (e.g. from squad_as_of_date), since
    the live default has no history.

    NOTE: this is the INVERSE of the `w` convention used everywhere else in this file
    (blend(), soccer_player_team_strength.weight_attack/weight_defense -- there, 1.0
    means team-level). The inversion happens in exactly one place, resolve_blend_weight
    below, specifically so it isn't scattered across call sites.

    No last-season history for team_id at all (e.g. backfill not run yet) -> 0.0
    (caller falls back fully to team-level; not a crash)."""
    last_season = season - 1
    team_minutes = team_roster_minutes(conn, team_id, last_season)
    team_total_minutes = sum(team_minutes.values())
    if team_total_minutes <= 0:
        return 0.0

    all_minutes = player_season_minutes(conn, last_season)
    current_squad = current_squad_ids if current_squad_ids is not None else current_squad_player_ids(conn, team_id)
    last_season_roster = set(team_minutes.keys())

    qualifying = {p for p in current_squad if all_minutes.get(p, 0) >= PLAYER_RATING_MIN_MINUTES_FROM_PRIOR_SEASON}
    coverage_minutes = sum(all_minutes.get(p, 0) for p in qualifying)
    data_coverage_score = min(coverage_minutes / team_total_minutes, 1.0)

    departed = last_season_roster - current_squad
    joined = current_squad - last_season_roster
    departed_minutes = sum(team_minutes.get(p, 0) for p in departed)
    joined_minutes = sum(all_minutes.get(p, 0) for p in joined)
    roster_change_score = min((departed_minutes + joined_minutes) / team_total_minutes, 1.0)

    return data_coverage_score * roster_change_score


def resolve_blend_weight(conn, team_id, league, component, season, current_squad_ids=None):
    """Default per-team weight (`w`; 0=pure player, 1=pure team), with a league-wide
    override taking precedence per component (FEATURE-011_REQUIREMENTS.md, Blend).
    current_squad_ids: see player_trust_score -- pass a point-in-time squad (e.g. from
    squad_as_of_date) when backtesting a past season."""
    override = PLAYER_RATING_LEAGUE_WIDE_BLEND_WEIGHT_OVERRIDE.get(league, {}).get(component)
    if override is not None:
        return override
    return 1.0 - player_trust_score(conn, team_id, season, current_squad_ids=current_squad_ids)


def get_team_xg_ratings(conn, team_id, before_date, n=TEAM_PAST_MATCH_WINDOW_SIZE, league="Serie A"):
    """xG-based counterpart to core.poisson_model.get_team_ratings -- same shape
    (home/away_attack, home/away_defense, home/away_n), same last-N-matches/no-decay
    convention, but derived from soccer_player_stats (this project's own xG/xGA data)
    instead of soccer_matches' actual scores.

    Comparison/debugging use only (2026-08-02) -- lives entirely in this file, not
    core.poisson_model, so poisson_v3 and the rest of the team-level system are
    completely untouched; this is an alternate data source for THIS file's own
    team_level_lambda, nothing else. See FEATURE-011_BUILD_TRACKER.md before making
    this (or anything derived from it) a real default anywhere.

    A team's xG in a match = sum of that team's players' xg that match. A team's xGA
    in a match = the opposing team's xG that match -- already stored per player row
    as club_xga_per90 (backfill_club_xga.py), constant across a team's rows for a
    given match, so MAX() is a safe way to pull one copy of it per (match, venue).

    Coverage caveat: only reaches as far back as player-level stats exist (seasons
    2023+ as of 2026-08-02) -- soccer_matches itself goes back to season 2022, so an
    early-season match's N-game lookback can hit a real, silent gap before that.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT s.match_id, m.match_date, s.venue, m.home_team_id, m.away_team_id,
               SUM(s.xg) AS team_xg, MAX(s.club_xga_per90) AS team_xga
        FROM soccer_player_stats s
        JOIN soccer_matches m ON m.match_id = s.match_id
        WHERE m.league = ? AND s.venue IS NOT NULL AND m.match_date < ?
        GROUP BY s.match_id, s.venue
    """, (league, before_date))

    home_rows, away_rows = [], []
    for match_id, match_date, venue, home_id, away_id, team_xg, team_xga in cur.fetchall():
        match_team_id = home_id if venue == "home" else away_id
        if match_team_id != team_id:
            continue
        (home_rows if venue == "home" else away_rows).append((match_date, team_xg, team_xga))

    home_rows.sort(key=lambda r: r[0], reverse=True)
    away_rows.sort(key=lambda r: r[0], reverse=True)
    home_rows, away_rows = home_rows[:n], away_rows[:n]

    def avg(rows, idx):
        vals = [r[idx] for r in rows if r[idx] is not None]
        return (sum(vals) / len(vals)) if vals else None

    return {
        "home_attack":  avg(home_rows, 1), "home_defense": avg(home_rows, 2),
        "away_attack":  avg(away_rows, 1), "away_defense": avg(away_rows, 2),
        "home_n": len(home_rows), "away_n": len(away_rows),
    }


def league_xg_field_means(conn, team_ids, before_date, league="Serie A", n=TEAM_PAST_MATCH_WINDOW_SIZE):
    """League-wide mean of each of get_team_xg_ratings' four fields across
    team_ids, at the same (league, before_date, n) every team_ids member will
    be rated at -- the recentering point TEAM_RATING_XG_SPREAD_STRETCH stretches
    around. Depends only on (league, before_date, n), not on which team is
    currently being rated, so compute() calls this ONCE per call and passes the
    result to every team's team_level_lambda call, rather than each team
    re-deriving the whole league's snapshot itself."""
    fields = ("home_attack", "home_defense", "away_attack", "away_defense")
    sums = {f: 0.0 for f in fields}
    counts = {f: 0 for f in fields}
    for tid in team_ids:
        ratings = get_team_xg_ratings(conn, tid, before_date, n=n, league=league)
        for f in fields:
            if ratings[f] is not None:
                sums[f] += ratings[f]
                counts[f] += 1
    return {f: (sums[f] / counts[f] if counts[f] else None) for f in fields}


def team_level_lambda(conn, team_id, league, before_date, avg_home, avg_away, n=TEAM_PAST_MATCH_WINDOW_SIZE,
                      team_xg_v_goals_blend=TEAM_RATING_XG_V_GOALS_BLEND,
                      xg_spread_stretch=TEAM_RATING_XG_SPREAD_STRETCH, league_xg_means=None):
    """Home/away-split intrinsic attack/defense for a team from the EXISTING team-level
    system, mirroring estimate_lambdas()'s own fallback/shrink logic exactly
    (TEAM_RATING_MIN_MATCHES_TO_TRUST_TEAM_RATING_OVER_LEAGUE_AVERAGE/TEAM_RATING_PULL_TOWARD_AVERAGE_MATCHES, and now TEAM_PAST_MATCH_WINDOW_SIZE -- the old n=25 default here didn't match
    analyse_match()'s actual n=TEAM_PAST_MATCH_WINDOW_SIZE=10 window, a real behavioral mismatch found
    2026-08-01 while debugging the away-side bias, separate from the home/away-collapse
    bug fixed earlier the same day) -- this RESTORES that mechanism rather than
    approximating it, since it's what already correctly encodes home-field advantage
    (2026-08-01: averaging home/away into one scalar here had silently dropped that,
    independent of any player blending -- see FEATURE-011_BUILD_TRACKER.md task 5).

    Defense values are measured in the OPPONENT's scoring units, same convention
    estimate_lambdas() uses (home_defense shrinks toward avg_away, away_defense
    toward avg_home) -- "goals conceded" is fundamentally the opponent's attack.

    Returns (home_attack, away_attack, home_defense, away_defense) -- always defined
    (falls back to the relevant league average, never None), same guarantee
    estimate_lambdas() provides; callers no longer need a None/cold-start check.

    team_xg_v_goals_blend (0.0=pure goals, 1.0=pure xG, DEFAULT 1.0): blends the RAW goals-
    based rating (core.poisson_model.get_team_ratings, actual match scores, exactly
    what poisson_v3 uses) with the RAW xG-based rating (get_team_xg_ratings above,
    this file's own xG/xGA derivation) before the shrink-to-fallback step below.
    1.0 is a true no-op, exactly reproducing this function's behavior since
    2026-08-02 (the fix that cleared the Model Calibration success criterion, using
    pure xG because a small sample of ACTUAL goals is noisy -- see the Atalanta case
    in FEATURE-011_BUILD_TRACKER.md); 0.0 exactly reproduces the pre-2026-08-02
    goals-only behavior. Values in between were added 2026-08-05 (BUG-009's
    2026-08-04 diagnosis): pure xG has much LESS team-to-team spread than pure goals
    (measured directly on 2025-26 home attack ratings: xG stdev=0.304 vs goals
    stdev=0.505, same mean ~1.27, with zero shrinkage involved on either side --a
    property of the xG data itself, not a bug) -- the dominant cause of a mismatch-
    size-dependent bias (model probabilities compressed toward a coin flip for big
    favorites/underdogs) invisible to the pooled signed-bias metric BUG-009's
    ±0.01-0.02 target uses. 0.5 tested (ad hoc script, not committed) as a reasonable
    middle ground: pooled signed home bias -0.0108 (near the ±0.01-0.02 target,
    pure-xG's -0.0009 is better there) while cutting the mismatch-bucket compression
    roughly in half vs. pure xG. This function never touches core.poisson_model
    either way, so poisson_v3 is unaffected by this default regardless.

    home_n/away_n (the TEAM_RATING_MIN_MATCHES_TO_TRUST_TEAM_RATING_OVER_LEAGUE_AVERAGE gate below) use whichever single source is
    fetched when team_xg_v_goals_blend is exactly 0.0 or 1.0 (preserving exact behavior at
    those boundaries even though get_team_ratings/get_team_xg_ratings can have
    slightly different coverage -- see get_team_xg_ratings' coverage caveat); for a
    genuine blend (0 < team_xg_v_goals_blend < 1) uses the MIN of the two sources' counts,
    a conservative choice (don't fully trust a blend unless BOTH sources have enough
    matches).

    xg_spread_stretch (DEFAULT TEAM_RATING_XG_SPREAD_STRETCH -- see that constant's
    comment for the full derivation): re-spreads the RAW xG ratings around the
    league's own mean before they enter the blend above. Only takes effect when
    league_xg_means is also given (the per-field league averages needed to recenter
    on) -- this function only rates ONE team, so it can't compute a league-wide mean
    itself without an extra full-league query; compute() calculates it once per call
    and passes it down to every team rather than paying that cost per team. Callers
    that don't pass league_xg_means (e.g. this function's own unit tests) get the
    exact pre-2026-08-07 behavior regardless of xg_spread_stretch's value -- it's a
    silent no-op without its companion, by design, so isolated tests of the blend
    lever don't also need to fake up a league snapshot."""
    goals_ratings = (get_team_ratings(conn, team_id, before_date, n=n, league=league, decay=1.0)
                     if team_xg_v_goals_blend < 1.0 else None)
    xg_ratings = (get_team_xg_ratings(conn, team_id, before_date, n=n, league=league)
                 if team_xg_v_goals_blend > 0.0 else None)
    if xg_ratings is not None and xg_spread_stretch != 1.0 and league_xg_means is not None:
        xg_ratings = dict(xg_ratings)
        for field in ("home_attack", "home_defense", "away_attack", "away_defense"):
            v, m = xg_ratings[field], league_xg_means.get(field)
            if v is not None and m is not None:
                xg_ratings[field] = m + (v - m) * xg_spread_stretch

    def blend(field, n_field):
        g = goals_ratings[field] if goals_ratings is not None else None
        x = xg_ratings[field] if xg_ratings is not None else None
        if x is None:
            value = g
        elif g is None:
            value = x
        else:
            value = (1 - team_xg_v_goals_blend) * g + team_xg_v_goals_blend * x
        if goals_ratings is not None and xg_ratings is not None:
            n_matches = min(goals_ratings[n_field], xg_ratings[n_field])
        elif goals_ratings is not None:
            n_matches = goals_ratings[n_field]
        else:
            n_matches = xg_ratings[n_field]
        return value, n_matches

    def resolved(value, n_matches, fallback_avg):
        if value is not None and n_matches >= TEAM_RATING_MIN_MATCHES_TO_TRUST_TEAM_RATING_OVER_LEAGUE_AVERAGE:
            return _shrink(value, fallback_avg, n_matches, TEAM_RATING_PULL_TOWARD_AVERAGE_MATCHES)
        return fallback_avg

    home_attack_raw, home_n = blend("home_attack", "home_n")
    away_attack_raw, away_n = blend("away_attack", "away_n")
    home_defense_raw, _ = blend("home_defense", "home_n")
    away_defense_raw, _ = blend("away_defense", "away_n")

    home_attack  = resolved(home_attack_raw, home_n, avg_home)
    away_attack  = resolved(away_attack_raw, away_n, avg_away)
    home_defense = resolved(home_defense_raw, home_n, avg_away)
    away_defense = resolved(away_defense_raw, away_n, avg_home)
    return home_attack, away_attack, home_defense, away_defense


def compute(conn, team_ids, league, season, before_date, w_attack=None, w_defense=None,
           current_squad_ids_by_team=None, attack_xg_v_goals_source="xg", defense_xg_v_goals_source="xga",
           team_xg_v_goals_blend=TEAM_RATING_XG_V_GOALS_BLEND,
           xg_spread_stretch=TEAM_RATING_XG_SPREAD_STRETCH,
           player_window_size=PLAYER_RATING_PAST_MATCH_WINDOW_SIZE,
           player_window_decay=PLAYER_RATING_PAST_MATCH_WINDOW_DECAY,
           player_window_min_date=None):
    """w_attack/w_defense: force this weight for EVERY team, bypassing per-team
    resolution -- a manual debugging/comparison override, not the normal path. Leave
    as None (default) to use resolve_blend_weight() per team, per component.

    attack_xg_v_goals_source, defense_xg_v_goals_source: passed through to load_team_players -- see that
    function's docstring.

    team_xg_v_goals_blend: passed through to team_level_lambda -- 1.0 (default) is a true
    no-op (pure xG, today's shipped behavior since 2026-08-02); 0.0 is pure goals
    (matches poisson_v3 exactly); values in between blend the two -- see that
    function's docstring (BUG-009's mismatch-size-compression diagnosis).

    xg_spread_stretch: passed through to team_level_lambda, along with a
    league_xg_means snapshot computed ONCE per compute() call (via
    league_xg_field_means) across all of team_ids -- see TEAM_RATING_XG_SPREAD_STRETCH's
    comment for the full derivation. 1.3 (default) is the largest factor tested that
    stays inside the Model Calibration bias target in both seasons; pass 1.0 to
    reproduce the exact pre-2026-08-07 shape. Skipped entirely (no snapshot query) when
    team_xg_v_goals_blend is exactly 0.0 -- xG never enters the rating at that boundary,
    so there's nothing to stretch.

    player_window_size, player_window_decay: passed through to load_team_players --
    see that function's docstring. Not yet independently tuned (starting values match
    the team-level system's own TEAM_PAST_MATCH_WINDOW_SIZE/_DECAY).

    player_window_min_date: passed through to load_team_players as `min_date` --
    comparison/validation only (e.g. pass a season's start date to reproduce a
    season-SCOPED window, for A/B-checking against the season-blind default of None).
    Not used by any real caller; exists purely so the season-blind design decision
    (2026-08-06, replacing blend_prior_season_attack/PRIOR_SEASON_DISCOUNT) can be
    validated against bias/ROI the same way team_xg_v_goals_blend's rollout was.

    current_squad_ids_by_team: optional {team_id: set(player_id)} override for the
    blend-weight "current squad" signal, passed through to resolve_blend_weight per
    team -- pass this (built from squad_as_of_date per team) when backtesting a past
    season; leave None for live use. before_date is threaded through
    load_team_players and get_league_averages too, for the same reason: computing a
    PAST match's lambdas must only see data that existed before that match (live use,
    where before_date is always "now" or later than all data on hand, is unaffected
    either way)."""
    by_team = load_team_players(conn, team_ids, before_date,
                                attack_xg_v_goals_source=attack_xg_v_goals_source,
                                defense_xg_v_goals_source=defense_xg_v_goals_source,
                                window_size=player_window_size, decay=player_window_decay,
                                min_date=player_window_min_date)
    apply_shrinkage(by_team)

    league_xg_means = (league_xg_field_means(conn, team_ids, before_date, league=league)
                       if xg_spread_stretch != 1.0 and team_xg_v_goals_blend > 0.0 else None)

    raw = {}
    for tid, players in by_team.items():
        ra, aw, rd, dw = raw_team_strength(players)
        raw[tid] = {"ra": ra, "aw": aw, "rd": rd, "dw": dw}

    # No `seasons=` filter, matching estimate_lambdas()'s own call to this function --
    # the 100-match rolling window (LEAGUE_AVG_GOALS_PER_GAME_WINDOW_SIZE) is meant to smooth across a
    # season boundary; restricting to the current season alone starves it early in a
    # new season (2026-08-01: a single-match sample can even average to exactly 0.0
    # goals, causing a ZeroDivisionError downstream once avg_home/avg_away are used
    # as separate normalization baselines instead of one pooled number).
    avgs = get_league_averages(conn, league=league, before_date=before_date)
    avg_home, avg_away = avgs["avg_home"], avgs["avg_away"]

    attack_vals = [r["ra"] for r in raw.values() if r["ra"] is not None
                   and r["aw"] >= PLAYER_RATING_MIN_ATTACK_WEIGHTED_MINUTES_TO_JOIN_LEAGUE_AVERAGE]
    defense_vals = [r["rd"] for r in raw.values() if r["rd"] is not None
                     and r["dw"] >= PLAYER_RATING_MIN_DEFENSE_WEIGHTED_MINUTES_TO_JOIN_LEAGUE_AVERAGE]
    attack_mean = mean(attack_vals) if attack_vals else None
    attack_sd = pstdev(attack_vals) if len(attack_vals) > 1 else 0.0
    defense_mean = mean(defense_vals) if defense_vals else None

    results = {}
    for tid, players in by_team.items():
        r = raw[tid]
        has_attack = (r["ra"] is not None
                      and r["aw"] >= PLAYER_RATING_MIN_ATTACK_WEIGHTED_MINUTES_TO_HAVE_OWN_RATING
                      and attack_mean is not None)
        has_defense = (r["rd"] is not None
                       and r["dw"] >= PLAYER_RATING_MIN_DEFENSE_WEIGHTED_MINUTES_TO_HAVE_OWN_RATING
                       and defense_mean is not None)

        # No fixed target spread (unlike WC's ATTACK_LAMBDA_SD) -- the single-league
        # sample here is too small to set one responsibly. Just re-center to baseline
        # and keep the raw sample spread. Player data still has no home/away split of
        # its own (Scenario 4, deferred) -- the same recentered deviation is re-based
        # onto the home and away league averages separately here, purely so it's
        # dimensionally consistent with the team-level home/away split it's blended
        # against below (same "opponent-scored units" convention for defense).
        la_player_home = (avg_home + (r["ra"] - attack_mean)) if has_attack else None
        la_player_away = (avg_away + (r["ra"] - attack_mean)) if has_attack else None
        ld_player_home = (r["rd"] * (avg_away / defense_mean)) if has_defense else None
        ld_player_away = (r["rd"] * (avg_home / defense_mean)) if has_defense else None

        team_home_attack, team_away_attack, team_home_defense, team_away_defense = \
            team_level_lambda(conn, tid, league, before_date, avg_home, avg_away,
                              team_xg_v_goals_blend=team_xg_v_goals_blend,
                              xg_spread_stretch=xg_spread_stretch, league_xg_means=league_xg_means)

        def blend(player_val, team_val, w):
            # team_val is always defined now (team_level_lambda falls back to the
            # league average itself), so the only real branch is missing player data.
            if player_val is None:
                return team_val, 1.0
            return (1 - w) * player_val + w * team_val, w

        squad_ids = current_squad_ids_by_team.get(tid) if current_squad_ids_by_team is not None else None
        w_att = w_attack if w_attack is not None else resolve_blend_weight(
            conn, tid, league, "attack", season, current_squad_ids=squad_ids)
        w_def = w_defense if w_defense is not None else resolve_blend_weight(
            conn, tid, league, "defense", season, current_squad_ids=squad_ids)
        attack_home_blend, w_a_used = blend(la_player_home, team_home_attack, w_att)
        attack_away_blend, _ = blend(la_player_away, team_away_attack, w_att)
        defense_home_blend, w_d_used = blend(ld_player_home, team_home_defense, w_def)
        defense_away_blend, _ = blend(ld_player_away, team_away_defense, w_def)

        if w_a_used == 0.0 and w_d_used == 0.0:
            basis = "player"
        elif w_a_used == 1.0 and w_d_used == 1.0:
            basis = "team"
        else:
            basis = f"mix(w_att={w_a_used:g},w_def={w_d_used:g})"

        results[tid] = {
            "lambda_attack_player_home": la_player_home, "lambda_attack_player_away": la_player_away,
            "lambda_defense_player_home": ld_player_home, "lambda_defense_player_away": ld_player_away,
            "lambda_attack_team_home": team_home_attack, "lambda_attack_team_away": team_away_attack,
            "lambda_defense_team_home": team_home_defense, "lambda_defense_team_away": team_away_defense,
            "lambda_attack_home_blend": attack_home_blend, "lambda_attack_away_blend": attack_away_blend,
            "lambda_defense_home_blend": defense_home_blend, "lambda_defense_away_blend": defense_away_blend,
            "weight_attack": w_a_used, "weight_defense": w_d_used, "basis": basis,
            "avg_home": avg_home, "avg_away": avg_away,
        }
    return results


def parse_args():
    parser = argparse.ArgumentParser(description="Compute + blend club-league player-based lambdas.")
    parser.add_argument("--league", default="Serie A")
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--team", help="Only compute for this DB team name.")
    parser.add_argument("--limit-teams", type=int, help="Only the first N teams (by team_id).")
    parser.add_argument("--weight-attack", type=float, default=None,
                        help="Force this attack weight for ALL teams (0=pure player, 1=pure "
                             "team), bypassing per-team resolution. Debugging/comparison only.")
    parser.add_argument("--weight-defense", type=float, default=None)
    parser.add_argument("--persist", action="store_true", help="Store results in soccer_player_team_strength.")
    return parser.parse_args()


def main():
    args = parse_args()
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT t.team_id, t.name FROM soccer_teams t
        JOIN soccer_matches m ON m.home_team_id = t.team_id OR m.away_team_id = t.team_id
        WHERE m.league = ? AND m.season = ?
        ORDER BY t.name
    """, (args.league, args.season))
    all_teams = cur.fetchall()
    if args.team:
        all_teams = [(tid, name) for tid, name in all_teams if name == args.team]
    if args.limit_teams:
        all_teams = all_teams[:args.limit_teams]
    team_ids = [tid for tid, _ in all_teams]
    names = dict(all_teams)

    if not team_ids:
        print("No matching teams.")
        return

    before_date = date.today().isoformat()
    results = compute(conn, team_ids, args.league, args.season, before_date,
                      args.weight_attack, args.weight_defense)

    print(f"{'TEAM':<20} {'BLEND-ATT(H/A)':>17} {'BLEND-DEF(H/A)':>17}  BASIS")
    for tid in team_ids:
        r = results[tid]
        def fmt(v):
            return f"{v:6.3f}" if v is not None else f"{'--':>6}"
        print(f"{names[tid]:<20} "
              f"{fmt(r['lambda_attack_home_blend'])}/{fmt(r['lambda_attack_away_blend'])}   "
              f"{fmt(r['lambda_defense_home_blend'])}/{fmt(r['lambda_defense_away_blend'])}  {r['basis']}")

        if args.persist:
            # soccer_player_team_strength doesn't have home/away-split columns yet
            # (nothing downstream reads this table so far -- tasks 6/7, not built).
            # Persisting the home-side values only, as a known simplification.
            set_player_team_strength(
                tid, args.league,
                lambda_attack_player=r["lambda_attack_player_home"],
                lambda_defense_player=r["lambda_defense_player_home"],
                lambda_attack_team=r["lambda_attack_team_home"],
                lambda_defense_team=r["lambda_defense_team_home"],
                lambda_attack_blend=r["lambda_attack_home_blend"],
                lambda_defense_blend=r["lambda_defense_home_blend"],
                weight_attack=r["weight_attack"], weight_defense=r["weight_defense"],
                basis=r["basis"], notes="prototype v1 (home-side only; see FEATURE-011_BUILD_TRACKER.md)",
                conn=conn,
            )
    conn.close()


if __name__ == "__main__":
    main()
