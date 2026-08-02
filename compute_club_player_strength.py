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
    get_team_ratings, get_league_averages, MIN_MATCHES, SHRINKAGE_K, RECENT_N, _shrink,
)

# Same weights/rationale as compute_wc_team_strength.py (BUG-001/BUG-002 family):
# forwards carry attack, defenders/keepers carry defense, midfield contributes to both.
ATTACK_POS_WEIGHTS = {"FWD": 1.0, "MID": 0.6, "DEF": 0.2, "GK": 0.0}
DEFENSE_POS_WEIGHTS = {"GK": 1.0, "DEF": 0.8, "MID": 0.3, "FWD": 0.1}

# Same half-trust point as the WC system -- ~10 matches, set from how football works,
# not fit to this league.
K_SHRINK_MINUTES = 900.0

MIN_ATTACK_WEIGHT = 300.0    # lower than WC's 1000 -- a single-league squad has fewer
MIN_DEFENSE_WEIGHT = 300.0   # thin-coverage players diluting the pool than a WC squad does

# Blend-weight resolution (FEATURE-011_REQUIREMENTS.md, Blend, resolved 2026-07-30).
# "Last season" (recency, not career totals) minutes a player needs before they count
# as a strong individual signal for the data-coverage score below.
MIN_MINUTES_PER_PLAYER = 900.0

# {league: {"attack": w, "defense": w}} -- a coarse override that forces EVERY team in
# that league to the same weight for that component, taking precedence over each
# team's computed default. `w` is this file's usual blend convention (0=pure player,
# 1=pure team) -- same as blend()/soccer_player_team_strength.weight_attack. Empty by
# default; add entries only with a documented reason, same convention as WC's
# FIFA_BLEND_OVERRIDES.
LEAGUE_WEIGHT_OVERRIDES = {}

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


def load_team_players(conn, team_ids, season, before_date=None, attack_metric="xg",
                      defense_metric="xga"):
    """Aggregate per-MATCH rows into one per-player-per-TEAM entry.

    before_date: optional ISO date string, restricting to matches strictly before it.
    Live/current use (the whole season so far) needs no cutoff; BACKTESTING a specific
    historical match must pass one -- without it, an early-season match's player-level
    lambda would leak in stats from later matches in the same season that hadn't been
    played yet, the same class of lookahead bug as BUG-008.

    Rates are computed from summed raw totals (goals/xg over summed minutes), not by
    averaging each match's own per-90 rate -- a single sub appearance of a few minutes
    would otherwise produce a wildly noisy per-match rate (e.g. 1 goal in 10 minutes =
    9.0/90) that a simple average wouldn't smooth out the way summing totals first
    does.

    attack_metric: "xg" (default) uses real xG (summed across matches) when at least
    one match has it, falling back to goals otherwise -- matching
    compute_wc_team_strength.py's fallback. "goals" always uses actual goals scored,
    never xG, for consistency with the team-level system (get_team_ratings/
    estimate_lambdas), which is built entirely from actual match goals -- added
    2026-08-02 as a debugging/comparison override after finding the xG preference
    meant a real, sustained finishing overperformance (e.g. Atalanta's Retegui/Lookman
    scoring well above their combined xG in 2024-25) never reached the player-level
    signal at all, even though the team-level signal picked it up correctly from
    actual results. Not the default; see FEATURE-011_BUILD_TRACKER.md for the
    resulting bias comparison before changing the default.

    defense_metric: "xga" (default, added 2026-08-02) uses club_xga_per90 -- the
    OPPOSING team's total xG in that match, backfilled locally from already-imported
    per-player xg (backfill_club_xga.py; the API itself has no expected-goals-against
    field at all) -- for consistency with attack_metric's default xG preference; this
    fixes what had been an unnoticed asymmetry (attack used expected values, defense
    used actual). Falls back to club_ga_per90 (actual goals conceded) when a row has
    no xga value. "actual" forces club_ga_per90 always, matching the pre-2026-08-02
    behavior, for debugging/comparison.

    Each row is attributed to the team the player actually played for IN THAT MATCH
    (derived from venue + soccer_matches.home/away_team_id), NOT soccer_players.team_id
    (their most-recently-seen team, per add_player). A mid-season transfer means those
    are different for part of a squad -- confirmed for real once this ran across the
    full 20-team Serie A (29 players, 463 rows misattributed when this used
    soccer_players.team_id: e.g. Sebastiano Luperto's 23 Cagliari matches were being
    silently folded into Cremonese, his team as of the LAST match processed, leaving
    Cagliari's aggregate missing him entirely). A player who moved between two teams
    that are BOTH in team_ids correctly gets a separate (player, team) entry for each
    stint, keyed on (player_id, match_team_id) below -- not one combined entry.

    Requires match_id IS NOT NULL: old season-total prototype rows (pre-rework) share
    the same `season` value and would silently double-count with the new per-match
    rows if not excluded (the INNER JOIN to soccer_matches already drops these, since
    NULL never matches; s.match_id IS NOT NULL is kept for clarity).
    """
    cur = conn.cursor()
    sql = """
        SELECT s.player_id, p.position, s.minutes_played, s.goals, s.xg,
               s.club_ga_per90, s.club_xga_per90,
               s.venue, m.home_team_id, m.away_team_id
        FROM soccer_player_stats s
        JOIN soccer_players p ON p.player_id = s.player_id
        JOIN soccer_matches m ON m.match_id = s.match_id
        WHERE s.season = ? AND s.match_id IS NOT NULL AND s.venue IS NOT NULL
    """
    params = [season]
    if before_date is not None:
        sql += " AND m.match_date < ?"
        params.append(before_date)
    cur.execute(sql, params)

    team_id_set = set(team_ids)
    agg = {}  # (player_id, match_team_id) -> accumulators
    for player_id, position, minutes, goals, xg, club_ga90, club_xga90, venue, home_id, away_id in cur.fetchall():
        match_team_id = home_id if venue == "home" else away_id
        if match_team_id not in team_id_set:
            continue
        minutes = minutes or 0
        key = (player_id, match_team_id)
        a = agg.setdefault(key, {
            "team_id": match_team_id, "pos": normalize_position(position),
            "minutes": 0, "goals": 0, "xg": 0.0, "has_xg": False,
            "ga_num": 0.0, "ga_den": 0.0,
            "xga_num": 0.0, "xga_den": 0.0,
        })
        a["minutes"] += minutes
        a["goals"] += goals or 0
        if xg is not None:
            a["xg"] += xg
            a["has_xg"] = True
        if club_ga90 is not None and minutes:
            a["ga_num"] += minutes * club_ga90
            a["ga_den"] += minutes
        if club_xga90 is not None and minutes:
            a["xga_num"] += minutes * club_xga90
            a["xga_den"] += minutes

    by_team = {tid: [] for tid in team_ids}
    for (player_id, team_id), a in agg.items():
        if a["minutes"] <= 0:
            continue
        if a["has_xg"] and attack_metric == "xg":
            attack_rate = a["xg"] / a["minutes"] * 90
        else:
            attack_rate = a["goals"] / a["minutes"] * 90
        if a["xga_den"] > 0 and defense_metric == "xga":
            club_ga_per90 = a["xga_num"] / a["xga_den"]
        elif a["ga_den"] > 0:
            club_ga_per90 = a["ga_num"] / a["ga_den"]
        else:
            club_ga_per90 = None
        by_team[team_id].append({
            "pos": a["pos"],
            "minutes": a["minutes"],
            "attack_rate": attack_rate,
            "club_ga_per90": club_ga_per90,
        })
    return by_team


def positional_priors(by_team, field):
    num, den = {}, {}
    for players in by_team.values():
        for p in players:
            pos, val, mins = p["pos"], p.get(field), p["minutes"]
            if pos and val is not None and mins:
                num[pos] = num.get(pos, 0.0) + mins * val
                den[pos] = den.get(pos, 0.0) + mins
    return {pos: num[pos] / den[pos] for pos in num}


def apply_shrinkage(by_team, k_minutes=K_SHRINK_MINUTES):
    for field in ("attack_rate", "club_ga_per90"):
        prior = positional_priors(by_team, field)
        for players in by_team.values():
            for p in players:
                pos, val, mins = p["pos"], p.get(field), p["minutes"]
                if pos in prior and val is not None and mins:
                    p[field] = (mins * val + k_minutes * prior[pos]) / (mins + k_minutes)


def raw_team_strength(players):
    a_num = a_w = d_num = d_w = 0.0
    for p in players:
        pos = p["pos"]
        if pos is None:
            continue
        if p["attack_rate"] is not None:
            w = p["minutes"] * ATTACK_POS_WEIGHTS.get(pos, 0.0)
            if w > 0:
                a_num += w * p["attack_rate"]
                a_w += w
        if p["club_ga_per90"] is not None:
            w = p["minutes"] * DEFENSE_POS_WEIGHTS.get(pos, 0.0)
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
      fraction belonging to players with >= MIN_MINUTES_PER_PLAYER last season
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

    qualifying = {p for p in current_squad if all_minutes.get(p, 0) >= MIN_MINUTES_PER_PLAYER}
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
    override = LEAGUE_WEIGHT_OVERRIDES.get(league, {}).get(component)
    if override is not None:
        return override
    return 1.0 - player_trust_score(conn, team_id, season, current_squad_ids=current_squad_ids)


def get_team_xg_ratings(conn, team_id, before_date, n=RECENT_N, league="Serie A"):
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


def team_level_lambda(conn, team_id, league, before_date, avg_home, avg_away, n=RECENT_N,
                      team_metric="xg"):
    """Home/away-split intrinsic attack/defense for a team from the EXISTING team-level
    system, mirroring estimate_lambdas()'s own fallback/shrink logic exactly
    (MIN_MATCHES/SHRINKAGE_K, and now RECENT_N -- the old n=25 default here didn't match
    analyse_match()'s actual n=RECENT_N=10 window, a real behavioral mismatch found
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

    team_metric: "xg" (default since 2026-08-02) uses get_team_xg_ratings above --
    this file's own xG/xGA derivation, which cleared the Model Calibration success
    criterion (FEATURE-011_BUILD_TRACKER.md task 5) after a goals-based team rating
    could not, on the same last-N-matches window this function has always used --
    a small sample of ACTUAL goals is noisy (see the Atalanta case in the tracker),
    and xG smooths that out. "goals" uses core.poisson_model.get_team_ratings instead
    -- actual match scores, exactly what poisson_v3 uses; this function never touches
    that module either way, so poisson_v3 is unaffected by this default regardless."""
    if team_metric == "xg":
        ratings = get_team_xg_ratings(conn, team_id, before_date, n=n, league=league)
    else:
        ratings = get_team_ratings(conn, team_id, before_date, n=n, league=league, decay=1.0)

    def resolved(value, n_matches, fallback_avg):
        if value is not None and n_matches >= MIN_MATCHES:
            return _shrink(value, fallback_avg, n_matches, SHRINKAGE_K)
        return fallback_avg

    home_attack  = resolved(ratings["home_attack"],  ratings["home_n"], avg_home)
    away_attack  = resolved(ratings["away_attack"],  ratings["away_n"], avg_away)
    home_defense = resolved(ratings["home_defense"], ratings["home_n"], avg_away)
    away_defense = resolved(ratings["away_defense"], ratings["away_n"], avg_home)
    return home_attack, away_attack, home_defense, away_defense


def compute(conn, team_ids, league, season, before_date, w_attack=None, w_defense=None,
           current_squad_ids_by_team=None, attack_metric="xg", defense_metric="xga",
           team_metric="xg"):
    """w_attack/w_defense: force this weight for EVERY team, bypassing per-team
    resolution -- a manual debugging/comparison override, not the normal path. Leave
    as None (default) to use resolve_blend_weight() per team, per component.

    attack_metric, defense_metric: passed through to load_team_players -- see that
    function's docstring.

    team_metric: passed through to team_level_lambda -- "xg" (default since
    2026-08-02) or "goals" (matches poisson_v3 exactly), see that function's
    docstring.

    current_squad_ids_by_team: optional {team_id: set(player_id)} override for the
    blend-weight "current squad" signal, passed through to resolve_blend_weight per
    team -- pass this (built from squad_as_of_date per team) when backtesting a past
    season; leave None for live use. before_date is threaded through
    load_team_players and get_league_averages too, for the same reason: computing a
    PAST match's lambdas must only see data that existed before that match (live use,
    where before_date is always "now" or later than all data on hand, is unaffected
    either way)."""
    by_team = load_team_players(conn, team_ids, season, before_date=before_date,
                                attack_metric=attack_metric, defense_metric=defense_metric)
    apply_shrinkage(by_team)

    raw = {}
    for tid, players in by_team.items():
        ra, aw, rd, dw = raw_team_strength(players)
        raw[tid] = {"ra": ra, "aw": aw, "rd": rd, "dw": dw}

    # No `seasons=` filter, matching estimate_lambdas()'s own call to this function --
    # the 100-match rolling window (LEAGUE_AVG_WINDOW) is meant to smooth across a
    # season boundary; restricting to the current season alone starves it early in a
    # new season (2026-08-01: a single-match sample can even average to exactly 0.0
    # goals, causing a ZeroDivisionError downstream once avg_home/avg_away are used
    # as separate normalization baselines instead of one pooled number).
    avgs = get_league_averages(conn, league=league, before_date=before_date)
    avg_home, avg_away = avgs["avg_home"], avgs["avg_away"]

    attack_vals = [r["ra"] for r in raw.values() if r["ra"] is not None and r["aw"] >= MIN_ATTACK_WEIGHT]
    defense_vals = [r["rd"] for r in raw.values() if r["rd"] is not None and r["dw"] >= MIN_DEFENSE_WEIGHT]
    attack_mean = mean(attack_vals) if attack_vals else None
    attack_sd = pstdev(attack_vals) if len(attack_vals) > 1 else 0.0
    defense_mean = mean(defense_vals) if defense_vals else None

    results = {}
    for tid, players in by_team.items():
        r = raw[tid]
        has_attack = r["ra"] is not None and r["aw"] >= MIN_ATTACK_WEIGHT and attack_mean is not None
        has_defense = r["rd"] is not None and r["dw"] >= MIN_DEFENSE_WEIGHT and defense_mean is not None

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
            team_level_lambda(conn, tid, league, before_date, avg_home, avg_away, team_metric=team_metric)

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
