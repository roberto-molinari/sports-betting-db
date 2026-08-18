"""
Tests for compute_club_player_strength.py's per-match aggregation -- the part that
changed in the storage rework (season-total lookup -> aggregating soccer_player_stats
rows keyed by match_id). Deliberately focuses on the aggregation properties that are
easy to silently regress: summing raw totals before computing a rate (not averaging
noisy per-match rates), preferring real xG when present, minutes-weighting defense,
and excluding pre-rework season-total rows that share the same `season` value.
"""

import pytest

from core import sports_db
from core.leagues import LEAGUES
import compute_club_player_strength as strength
from compute_club_player_strength import PLAYER_RATING_MINUTES_TO_HALF_TRUST_OWN_RATE_OVER_LEAGUE_AVERAGE


def _seed_match(conn, home_name="Home", away_name="Away", season=2025, date="2025-09-01"):
    home = sports_db.ensure_soccer_team(home_name, "Serie A")
    away = sports_db.ensure_soccer_team(away_name, "Serie A")
    match_id = sports_db.add_soccer_match("Serie A", season, home, away, date)
    return home, away, match_id


# ── normalize_position ───────────────────────────────────────────────────────────

def test_normalize_position_common_cases():
    assert strength.normalize_position("Goalkeeper") == "GK"
    assert strength.normalize_position("G") == "GK"
    assert strength.normalize_position("Centre-Back") == "DEF"
    assert strength.normalize_position("CDM") == "MID"
    assert strength.normalize_position("Striker") == "FWD"
    assert strength.normalize_position(None) is None
    assert strength.normalize_position("") is None


# ── load_team_players: rate computation ──────────────────────────────────────────

def test_sums_totals_before_computing_rate_not_average_of_per_match_rates(db_path, conn):
    """A short sub appearance (1 goal in 10 minutes = a 9.0/90 per-match rate) must
    not distort the season rate when averaged naively against a fuller appearance.
    Correct: (1 goal + 0 goals) / (10 + 80 minutes) * 90 = 1.0, not (9.0 + 0) / 2 = 4.5."""
    team, opp, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    _, _, m2 = _seed_match(conn, "TeamA", "OppB", date="2025-09-08")  # reuses TeamA
    player = sports_db.add_player(team, "Sub Striker", position="F", conn=conn)

    sports_db.add_player_match_stats(player, m1, season=2025, venue="home", minutes_played=10, goals=1, conn=conn)
    sports_db.add_player_match_stats(player, m2, season=2025, venue="home", minutes_played=80, goals=0, conn=conn)

    by_team = strength.load_team_players(conn, [team], "2026-01-01", half_life_days=1.0e12, cutoff_days=1.0e12)
    assert len(by_team[team]) == 1
    assert by_team[team][0]["attack_rate"] == pytest.approx(1.0)
    assert by_team[team][0]["attack_rate"] != pytest.approx(4.5)
    assert by_team[team][0]["attack_minutes"] == pytest.approx(90)


def test_prefers_real_xg_over_goals_when_present(db_path, conn):
    team, opp, m1 = _seed_match(conn)
    player = sports_db.add_player(team, "Striker", position="F", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2025, venue="home", minutes_played=90,
                                     goals=0, xg=0.9, conn=conn)

    by_team = strength.load_team_players(conn, [team], "2026-01-01")
    assert by_team[team][0]["attack_rate"] == pytest.approx(0.9)   # xg, not goals=0


def test_falls_back_to_goals_when_no_match_has_xg(db_path, conn):
    team, opp, m1 = _seed_match(conn)
    player = sports_db.add_player(team, "Striker", position="F", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2025, venue="home", minutes_played=90,
                                     goals=1, xg=None, conn=conn)

    by_team = strength.load_team_players(conn, [team], "2026-01-01")
    assert by_team[team][0]["attack_rate"] == pytest.approx(1.0)   # goals/90 fallback


def test_uses_xg_total_even_when_only_some_matches_have_it(db_path, conn):
    """If ANY match has xg, has_xg is True for the player and the total xg (summed
    across matches, 0 contribution from matches without it) drives the rate --
    documents current behavior, not just asserts it."""
    team, opp, m1 = _seed_match(conn, date="2025-09-01")
    _, _, m2 = _seed_match(conn, "Home", "OppB", date="2025-09-08")  # same team, 2nd match
    player = sports_db.add_player(team, "Striker", position="F", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2025, venue="home", minutes_played=45,
                                     goals=1, xg=0.5, conn=conn)
    sports_db.add_player_match_stats(player, m2, season=2025, venue="home", minutes_played=45,
                                     goals=1, xg=None, conn=conn)

    by_team = strength.load_team_players(conn, [team], "2026-01-01", half_life_days=1.0e12, cutoff_days=1.0e12)
    # total xg = 0.5 (second match contributes 0, not its goal), total minutes = 90
    assert by_team[team][0]["attack_rate"] == pytest.approx(0.5)


# ── load_team_players: defense weighting ─────────────────────────────────────────

def test_defense_is_minutes_weighted_not_simple_average(db_path, conn):
    team, opp, m1 = _seed_match(conn, date="2025-09-01")
    _, _, m2 = _seed_match(conn, "Home", "OppB", date="2025-09-08")  # same team, 2nd match
    player = sports_db.add_player(team, "Defender", position="D", conn=conn)
    # 90 min at club_ga_per90=2, 10 min at club_ga_per90=0 -- weighted average should
    # be much closer to 2 than a simple average of (2+0)/2=1.0 would give.
    sports_db.add_player_match_stats(player, m1, season=2025, venue="home", minutes_played=90,
                                     club_ga_per90=2, conn=conn)
    sports_db.add_player_match_stats(player, m2, season=2025, venue="home", minutes_played=10,
                                     club_ga_per90=0, conn=conn)

    by_team = strength.load_team_players(conn, [team], "2026-01-01", half_life_days=1.0e12, cutoff_days=1.0e12)
    expected = (90 * 2 + 10 * 0) / 100
    assert by_team[team][0]["club_ga_per90"] == pytest.approx(expected)
    assert by_team[team][0]["club_ga_per90"] != pytest.approx(1.0)


# ── load_team_players: legacy row exclusion ──────────────────────────────────────

def test_excludes_legacy_season_total_rows_without_match_id(tmp_path):
    """A pre-rework season-total row (match_id NULL) sharing the same `season` value
    must not double-count alongside real per-match rows. The CURRENT schema enforces
    match_id NOT NULL on a freshly-created soccer_player_stats, so this scenario can
    only occur on an already-migrated database carrying old prototype rows (which is
    exactly the real sports_betting.db's current state) -- built by hand here rather
    than via the db_path fixture, which always creates the strict fresh schema."""
    import sqlite3
    path = tmp_path / "migrated_shape.db"
    raw = sqlite3.connect(path)
    raw.executescript("""
        CREATE TABLE soccer_players (player_id INTEGER PRIMARY KEY, team_id INTEGER,
                                     name TEXT, position TEXT);
        CREATE TABLE soccer_matches (match_id INTEGER PRIMARY KEY, home_team_id INTEGER,
                                     away_team_id INTEGER, match_date TEXT, league TEXT);
        CREATE TABLE soccer_player_stats (stat_id INTEGER PRIMARY KEY, player_id INTEGER,
                                          match_id INTEGER, season INTEGER, venue TEXT,
                                          minutes_played INTEGER, goals INTEGER,
                                          xg REAL, club_ga_per90 REAL, club_xga_per90 REAL);
    """)
    raw.execute("INSERT INTO soccer_players (player_id, team_id, position) VALUES (1, 10, 'M')")
    raw.execute("""INSERT INTO soccer_matches (match_id, home_team_id, away_team_id, match_date, league)
                   VALUES (501, 10, 99, '2025-09-01', 'Serie A')""")
    # Real per-match row.
    raw.execute("""INSERT INTO soccer_player_stats
                   (player_id, match_id, season, venue, minutes_played, goals)
                   VALUES (1, 501, 2025, 'home', 90, 1)""")
    # Legacy pre-rework row: match_id NULL, large minutes that would corrupt the
    # result if not excluded (the INNER JOIN to soccer_matches drops it regardless).
    raw.execute("""INSERT INTO soccer_player_stats
                   (player_id, match_id, season, venue, minutes_played, goals)
                   VALUES (1, NULL, 2025, NULL, 2500, 20)""")
    raw.commit()

    by_team = strength.load_team_players(raw, [10], "2026-01-01", half_life_days=1.0e12, cutoff_days=1.0e12)
    assert len(by_team[10]) == 1
    assert by_team[10][0]["attack_minutes"] == pytest.approx(90)   # NOT 90 + 2500
    raw.close()


def test_attributes_stats_to_match_time_team_not_current_team(db_path, conn):
    """Each game is attributed to the team the player actually played for IN THAT
    MATCH (derived from venue/home_away_team_id), not wherever soccer_players.team_id
    currently points -- the real bug found scaling this to the full 20-team Serie A
    (Sebastiano Luperto: 23 Cagliari matches were being silently folded into
    Cremonese, his team as of the last match processed). Proven here by a mid-season
    transfer: match-time attribution means BOTH games' real goals/minutes correctly
    reach the final rate (4 goals / 1350 min), not just whichever team's row
    soccer_players.team_id happened to point at.

    Season-blind + follows-the-player-across-teams (2026-08-06): unlike the old flat
    season sum, the OLD team no longer gets its own separate entry once the player has
    moved on -- their whole recent window (both stints) counts toward the CURRENT
    team (team_b, whichever team their single most recent appearance was for). See
    the roster-membership tests below for that behavior in isolation."""
    team_a, opp1, m1 = _seed_match(conn, "OldClub", "OppA", date="2025-09-01")
    team_b, opp2, m2 = _seed_match(conn, "NewClub", "OppB", date="2026-02-01")
    player = sports_db.add_player(team_a, "Mid-Season Mover", api_player_id="ext_msm", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2025, venue="home",
                                     minutes_played=900, goals=3, conn=conn)
    sports_db.add_player(team_b, "Mid-Season Mover", api_player_id="ext_msm", conn=conn)  # transfer
    sports_db.add_player_match_stats(player, m2, season=2025, venue="home",
                                     minutes_played=450, goals=1, conn=conn)

    by_team = strength.load_team_players(conn, [team_a, team_b], "2026-06-01", half_life_days=1.0e12, cutoff_days=1.0e12)
    assert by_team[team_a] == []
    assert len(by_team[team_b]) == 1
    assert by_team[team_b][0]["attack_minutes"] == pytest.approx(1350)
    assert by_team[team_b][0]["attack_rate"] == pytest.approx(4 / 1350 * 90)


def test_player_with_zero_total_minutes_is_excluded(db_path, conn):
    team, opp, m1 = _seed_match(conn)
    player = sports_db.add_player(team, "Unused Sub", position="F", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2025, minutes_played=0, goals=0, conn=conn)

    by_team = strength.load_team_players(conn, [team], "2026-01-01")
    assert by_team[team] == []


# ── blend-weight resolution (FEATURE-011_REQUIREMENTS.md, Blend, resolved 2026-07-30) ────
#
# player_trust_score: 1.0 = fully trust player-level, 0.0 = fully trust team-level.
# resolve_blend_weight returns the INVERSE (this file's usual `w` convention, where 1.0
# means team-level) -- these tests exercise player_trust_score directly for clarity and
# separately confirm resolve_blend_weight performs the inversion correctly, since a
# silent sign flip here would be a serious, hard-to-notice bug.

def _transfer(conn, team_id, name, api_id):
    """Move an existing (or create a new) player to team_id via api_player_id --
    the mechanism that actually represents "this player is now on this roster."""
    return sports_db.add_player(team_id, name, api_player_id=api_id, conn=conn)


def test_player_season_minutes_is_team_agnostic(db_path, conn):
    """A player's minutes are summed across ALL teams/matches in a season -- the
    signal a transferred-in player's prior-club minutes must not be dropped."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2025, date="2025-09-01")
    team_b, opp_b, m2 = _seed_match(conn, "TeamB", "OppB", season=2025, date="2025-10-01")
    player = sports_db.add_player(team_a, "Well-Traveled", api_player_id="ext_wt", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2025, venue="home", minutes_played=400, conn=conn)
    sports_db.add_player_match_stats(player, m2, season=2025, venue="home", minutes_played=600, conn=conn)

    totals = strength.player_season_minutes(conn, 2025)
    assert totals[player] == 1000


def test_team_roster_minutes_is_team_scoped(db_path, conn):
    """Unlike player_season_minutes, this only counts minutes played AT team_id --
    a player's minutes for a DIFFERENT team must not leak in."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2025, date="2025-09-01")
    team_b, opp_b, m2 = _seed_match(conn, "TeamB", "OppB", season=2025, date="2025-10-01")
    player = sports_db.add_player(team_a, "Split Season", api_player_id="ext_ss", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2025, venue="home", minutes_played=400, conn=conn)
    sports_db.add_player_match_stats(player, m2, season=2025, venue="home", minutes_played=600, conn=conn)

    team_a_minutes = strength.team_roster_minutes(conn, team_a, 2025)
    team_b_minutes = strength.team_roster_minutes(conn, team_b, 2025)
    assert team_a_minutes[player] == 400
    assert team_b_minutes[player] == 600


def test_team_roster_minutes_before_date_excludes_later_matches(db_path, conn):
    """before_date restricts to matches strictly before it -- the mechanism backtesting
    relies on to avoid leaking later-season data into an earlier match's computation."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2025, date="2025-09-01")
    _, _, m2 = _seed_match(conn, "TeamA", "OppB", season=2025, date="2025-11-01")
    player = sports_db.add_player(team_a, "Timeline Player", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2025, venue="home", minutes_played=90, conn=conn)
    sports_db.add_player_match_stats(player, m2, season=2025, venue="home", minutes_played=90, conn=conn)

    early = strength.team_roster_minutes(conn, team_a, 2025, before_date="2025-10-01")
    full = strength.team_roster_minutes(conn, team_a, 2025)
    assert early[player] == 90    # only m1
    assert full[player] == 180    # both


# ── season-blind recent-window helpers (BUG-010, 2026-08-11) ─────────────────────

def test_team_aggregated_recent_roster_minutes_reaches_across_a_season_boundary(db_path, conn):
    """The whole point of the fix: no season filter at all -- a match from an
    earlier season counts toward the window exactly like a same-season one would,
    as long as it's within the last `n` matches before before_date."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2023, date="2023-09-01")
    player = sports_db.add_player(team_a, "Veteran", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2023, venue="home", minutes_played=900, conn=conn)

    minutes = strength.team_aggregated_recent_roster_minutes(conn, team_a, "2025-01-01",
                                                               half_life_days=1.0e12, cutoff_days=1.0e12)
    assert minutes[player] == pytest.approx(900)


def test_team_aggregated_recent_roster_minutes_is_team_scoped(db_path, conn):
    """Same team-scoping guarantee as team_roster_minutes -- a player's minutes for
    a DIFFERENT team must not leak in. Both matches have a real opponent-side
    player too (real regression case, found live 2026-08-11: an earlier version of
    this function found the right match_ids but then summed minutes for BOTH
    sides of each match, not just team_id's own venue -- a query missing the
    match/venue join that a same-player-reused-for-both-teams fixture couldn't
    have caught, since there was no genuine opponent row to leak in)."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    team_b, opp_b, m2 = _seed_match(conn, "TeamB", "OppB", date="2025-09-08")
    player = sports_db.add_player(team_a, "Split", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2025, venue="home", minutes_played=400, conn=conn)
    sports_db.add_player_match_stats(player, m2, season=2025, venue="home", minutes_played=600, conn=conn)
    opp_a_player = sports_db.add_player(opp_a, "OppA Player", conn=conn)
    opp_b_player = sports_db.add_player(opp_b, "OppB Player", conn=conn)
    sports_db.add_player_match_stats(opp_a_player, m1, season=2025, venue="away", minutes_played=90, conn=conn)
    sports_db.add_player_match_stats(opp_b_player, m2, season=2025, venue="away", minutes_played=90, conn=conn)

    kw = dict(half_life_days=1.0e12, cutoff_days=1.0e12)
    assert strength.team_aggregated_recent_roster_minutes(conn, team_a, "2026-01-01", **kw) == pytest.approx({player: 400})
    assert strength.team_aggregated_recent_roster_minutes(conn, team_b, "2026-01-01", **kw) == pytest.approx({player: 600})
    assert strength.team_aggregated_recent_roster_minutes(conn, opp_a, "2026-01-01", **kw) == pytest.approx({opp_a_player: 90})


def test_team_aggregated_recent_roster_minutes_before_date_excludes_later_matches(db_path, conn):
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    _, _, m2 = _seed_match(conn, "TeamA", "OppB", date="2025-11-01")
    player = sports_db.add_player(team_a, "Timeline Player", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2025, venue="home", minutes_played=90, conn=conn)
    sports_db.add_player_match_stats(player, m2, season=2025, venue="home", minutes_played=90, conn=conn)

    early = strength.team_aggregated_recent_roster_minutes(conn, team_a, "2025-10-01",
                                                             half_life_days=1.0e12, cutoff_days=1.0e12)
    full = strength.team_aggregated_recent_roster_minutes(conn, team_a, "2026-01-01",
                                                            half_life_days=1.0e12, cutoff_days=1.0e12)
    assert early[player] == pytest.approx(90)    # only m1
    assert full[player] == pytest.approx(180)    # both


def test_team_aggregated_recent_roster_minutes_limits_to_last_n_matches(db_path, conn):
    """A match beyond the window is dropped entirely, same convention as every
    other N-game window in this file (e.g. load_team_players' window_size)."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    _, _, m2 = _seed_match(conn, "TeamA", "OppB", date="2025-09-08")
    player = sports_db.add_player(team_a, "Bench Player", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2025, venue="home", minutes_played=90, conn=conn)
    sports_db.add_player_match_stats(player, m2, season=2025, venue="home", minutes_played=90, conn=conn)

    windowed = strength.team_aggregated_recent_roster_minutes(conn, team_a, "2026-01-01", n=1,
                                                                half_life_days=1.0e12, cutoff_days=1.0e12)
    assert windowed[player] == pytest.approx(90)   # only m2, the more recent of the two


def test_team_aggregated_recent_roster_minutes_empty_when_no_matches_at_all(db_path, conn):
    """No history at all for this team (BUG-010's actual trigger: a league's
    first-ever tracked season) -> empty dict, not a crash -- player_trust_score
    relies on this to detect the no-data case and fall back to team-level."""
    team_a = sports_db.ensure_soccer_team("Brand New", "Serie A")
    assert strength.team_aggregated_recent_roster_minutes(conn, team_a, "2026-01-01") == {}


def test_players_recent_minutes_is_team_agnostic(db_path, conn):
    """Same team-agnostic guarantee as player_season_minutes -- a transferred
    player's minutes at their PREVIOUS club still count in full."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    team_b, opp_b, m2 = _seed_match(conn, "TeamB", "OppB", date="2025-10-01")
    player = sports_db.add_player(team_a, "Well-Traveled", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2025, venue="home", minutes_played=400, conn=conn)
    sports_db.add_player_match_stats(player, m2, season=2025, venue="home", minutes_played=600, conn=conn)

    minutes = strength.players_aggregated_recent_minutes(conn, {player}, "2026-01-01",
                                                           half_life_days=1.0e12, cutoff_days=1.0e12)
    assert minutes[player] == pytest.approx(1000)


def test_players_recent_minutes_only_computes_requested_players(db_path, conn):
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    p1 = sports_db.add_player(team_a, "Requested", conn=conn)
    p2 = sports_db.add_player(team_a, "Not Requested", conn=conn)
    sports_db.add_player_match_stats(p1, m1, season=2025, venue="home", minutes_played=90, conn=conn)
    sports_db.add_player_match_stats(p2, m1, season=2025, venue="home", minutes_played=90, conn=conn)

    minutes = strength.players_aggregated_recent_minutes(conn, {p1}, "2026-01-01",
                                                           half_life_days=1.0e12, cutoff_days=1.0e12)
    assert minutes == pytest.approx({p1: 90})


def test_players_recent_minutes_limits_to_last_n_appearances_per_player(db_path, conn):
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    _, _, m2 = _seed_match(conn, "TeamA", "OppB", date="2025-09-08")
    player = sports_db.add_player(team_a, "Frequent Flyer", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2025, venue="home", minutes_played=90, conn=conn)
    sports_db.add_player_match_stats(player, m2, season=2025, venue="home", minutes_played=90, conn=conn)

    windowed = strength.players_aggregated_recent_minutes(conn, {player}, "2026-01-01", n=1,
                                                            half_life_days=1.0e12, cutoff_days=1.0e12)
    assert windowed[player] == pytest.approx(90)   # only the more recent appearance


def test_players_recent_minutes_empty_for_no_player_ids(db_path, conn):
    assert strength.players_aggregated_recent_minutes(conn, set(), "2026-01-01") == {}


def test_player_trust_high_when_current_roster_has_full_coverage(db_path, conn):
    """The headline case this mechanism exists for (coverage-only design,
    BUG-012 root cause #4 v2, 2026-08-16): the current roster is well-tracked
    (>=300 min in each player's own last `window` appearances, wherever they
    played) relative to the team's own recent-match volume -> trust close to
    1.0, regardless of whether that roster overlaps with who played team_a's
    own recent matches."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2025, date="2025-09-01")
    team_b, opp_b, m2 = _seed_match(conn, "TeamB", "OppB", season=2025, date="2025-10-01")
    away_team = sports_db.ensure_soccer_team("MovedOn", "Serie A")

    saturation = int(strength.PLAYER_RATING_COVERAGE_SATURATION_MINUTES)
    p1 = _transfer(conn, team_a, "P1", "ext_p1")
    p2 = _transfer(conn, team_a, "P2", "ext_p2")
    sports_db.add_player_match_stats(p1, m1, season=2025, venue="home", minutes_played=saturation, conn=conn)
    sports_db.add_player_match_stats(p2, m1, season=2025, venue="home", minutes_played=saturation, conn=conn)

    p3 = _transfer(conn, team_b, "P3", "ext_p3")
    p4 = _transfer(conn, team_b, "P4", "ext_p4")
    sports_db.add_player_match_stats(p3, m2, season=2025, venue="home", minutes_played=saturation, conn=conn)
    sports_db.add_player_match_stats(p4, m2, season=2025, venue="home", minutes_played=saturation, conn=conn)

    # P1/P2 leave TeamA. P3/P4 join TeamA (from TeamB) -- churn no longer matters,
    # but P3/P4's own real minutes (from TeamB) are what drives coverage here.
    _transfer(conn, away_team, "P1", "ext_p1")
    _transfer(conn, away_team, "P2", "ext_p2")
    _transfer(conn, team_a, "P3", "ext_p3")
    _transfer(conn, team_a, "P4", "ext_p4")

    trust = strength.player_trust_score(conn, team_a, "2026-09-14", window=1,
                                         half_life_days=1.0e12, cutoff_days=1.0e12)
    assert trust == pytest.approx(1.0)
    # abs= needed here (not just the default rel=): the confidence ramp squares
    # calendar_recency_weight's near-1.0-but-not-exactly decay factor (0.5 **
    # (elapsed/1e12) isn't exactly 1.0 in floating point), so the residual is
    # tiny (~1e-10) but non-zero, and pytest.approx(0.0)'s default tolerance is
    # too strict for a comparison against exactly zero.
    assert strength.resolve_blend_weight(
        conn, team_a, "Serie A", "attack", "2026-09-14", window=1,
        half_life_days=1.0e12, cutoff_days=1.0e12) == pytest.approx(0.0, abs=1e-6)


def test_player_trust_high_even_when_roster_is_completely_stable(db_path, conn):
    """The deliberate behavior change from the old churn-gated design (BUG-012
    root cause #4 v2, 2026-08-16): a roster with ZERO turnover, but good
    coverage, now gets real player-level trust. Previously this was forced to
    exactly 0.0 on the theory that a stable, well-tracked squad has nothing to
    gain from the player signal (FEATURE-011_REQUIREMENTS.md, Blend). Real data
    didn't support that theory: a poisson_v4_4 A/B (same lambdas, only the
    blend weight changed) showed that even the old, already-team-heavy
    mechanism was getting real value from its player-level minority share --
    shifting further toward team-level (which is what stability-gating does)
    hurt Brier broadly across every league. See BUG-012 in BUGS.md."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2025, date="2025-09-01")
    saturation = int(strength.PLAYER_RATING_COVERAGE_SATURATION_MINUTES)
    p1 = sports_db.add_player(team_a, "Stalwart One", api_player_id="ext_s1", conn=conn)
    p2 = sports_db.add_player(team_a, "Stalwart Two", api_player_id="ext_s2", conn=conn)
    sports_db.add_player_match_stats(p1, m1, season=2025, venue="home", minutes_played=saturation, conn=conn)
    sports_db.add_player_match_stats(p2, m1, season=2025, venue="home", minutes_played=saturation, conn=conn)
    # No roster changes at all -- p1/p2 are both the reference AND the live current roster.

    trust = strength.player_trust_score(conn, team_a, "2026-09-14", window=1,
                                         half_life_days=1.0e12, cutoff_days=1.0e12)
    assert trust == pytest.approx(1.0)
    # abs= needed here -- see the sibling "full coverage" test above for why.
    assert strength.resolve_blend_weight(
        conn, team_a, "Serie A", "defense", "2026-09-14", window=1,
        half_life_days=1.0e12, cutoff_days=1.0e12) == pytest.approx(0.0, abs=1e-6)


def test_player_trust_scales_continuously_with_a_players_own_minutes(db_path, conn):
    """Continuous coverage ramp (BUG-012 root cause #4 v3, 2026-08-16): a player's
    contribution scales smoothly with their own tracked minutes relative to
    PLAYER_RATING_COVERAGE_SATURATION_MINUTES, not a binary qualify/disqualify
    cutoff. A player at exactly the saturation point contributes their full
    minutes; a player at HALF the saturation point contributes only a QUARTER of
    their raw minutes (0.5 confidence x 0.5 of raw minutes) -- a binary cutoff at
    any point below the saturation minutes would have credited them in full."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2025, date="2025-09-01")
    saturation = strength.PLAYER_RATING_COVERAGE_SATURATION_MINUTES
    reference_player = sports_db.add_player(team_a, "Reference Player", conn=conn)
    sports_db.add_player_match_stats(reference_player, m1, season=2025, venue="home",
                                     minutes_played=int(saturation), conn=conn)

    half_tracked = sports_db.add_player(team_a, "Half Tracked", conn=conn)
    team_b, opp_b, m2 = _seed_match(conn, "TeamB", "OppB", season=2025, date="2025-09-08")
    sports_db.add_player_match_stats(half_tracked, m2, season=2025, venue="home",
                                     minutes_played=int(saturation / 2), conn=conn)

    # team_total_minutes = saturation (reference_player's own match at team_a).
    # coverage_minutes = (saturation/2 raw minutes) * (0.5 confidence) = saturation/4.
    trust = strength.player_trust_score(
        conn, team_a, "2026-09-14", current_roster_ids={half_tracked}, window=1,
        half_life_days=1.0e12, cutoff_days=1.0e12)
    assert trust == pytest.approx(0.25)


def test_player_trust_low_when_current_roster_has_no_usable_track_record(db_path, conn):
    """Low/no coverage -> low trust, regardless of whether the roster looks
    "new" or not -- the edge case flagged in FEATURE-011_REQUIREMENTS.md (heavy
    churn into unproven players) still holds, just via coverage alone now
    rather than a churn-AND-coverage product."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2025, date="2025-09-01")
    reference_player = sports_db.add_player(team_a, "Reference Player", conn=conn)
    sports_db.add_player_match_stats(reference_player, m1, season=2025, venue="home", minutes_played=900, conn=conn)

    # Current roster is two rookies with NO tracked minutes anywhere (debutants/reserves).
    rookie_one = sports_db.add_player(team_a, "Rookie One", conn=conn)
    rookie_two = sports_db.add_player(team_a, "Rookie Two", conn=conn)

    trust = strength.player_trust_score(
        conn, team_a, "2026-09-14", current_roster_ids={rookie_one, rookie_two}, window=1,
        half_life_days=1.0e12, cutoff_days=1.0e12)
    assert trust == pytest.approx(0.0)


def test_player_trust_zero_when_no_recent_window_history(db_path, conn):
    """No recent-window data for the team at all (e.g. backfill not run far back
    enough yet -- the exact BUG-010 scenario: a league's first-ever tracked season)
    -> falls back fully to team-level, not a ZeroDivisionError."""
    team_a = sports_db.ensure_soccer_team("Brand New Import", "Serie A")
    trust = strength.player_trust_score(conn, team_a, "2026-09-14")
    assert trust == 0.0
    assert strength.resolve_blend_weight(conn, team_a, "Serie A", "attack", "2026-09-14") == 1.0


def test_player_trust_score_half_life_and_cutoff_thread_through_to_aggregations(db_path, conn):
    """BUG-012 (2026-08-14): half_life_days/cutoff_days must actually reach
    team_aggregated_recent_roster_minutes/players_aggregated_recent_minutes
    inside player_trust_score, not just sit accepted-but-unused. Reproduces
    test_player_trust_high_when_current_roster_has_full_coverage's exact setup
    (trust=1.0 under explicit near-no-op values -- Stage 2, 2026-08-15, shipped
    real module defaults, so this test pins its own no-op baseline explicitly
    rather than relying on the module's) and shows a real, tight cutoff_days
    collapses it: the coverage denominator's reference match (2025-09-01), now
    decayed directly against the real evaluation date (2026-09-14, over a year
    later), falls outside a 30-day cutoff, so its minutes are excluded entirely
    and team_total_minutes hits the same zero-history fallback the true
    no-data case uses."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2025, date="2025-09-01")
    team_b, opp_b, m2 = _seed_match(conn, "TeamB", "OppB", season=2025, date="2025-10-01")
    away_team = sports_db.ensure_soccer_team("MovedOn", "Serie A")
    saturation = int(strength.PLAYER_RATING_COVERAGE_SATURATION_MINUTES)

    p1 = _transfer(conn, team_a, "P1", "ext_p1")
    p2 = _transfer(conn, team_a, "P2", "ext_p2")
    sports_db.add_player_match_stats(p1, m1, season=2025, venue="home", minutes_played=saturation, conn=conn)
    sports_db.add_player_match_stats(p2, m1, season=2025, venue="home", minutes_played=saturation, conn=conn)

    p3 = _transfer(conn, team_b, "P3", "ext_p3")
    p4 = _transfer(conn, team_b, "P4", "ext_p4")
    sports_db.add_player_match_stats(p3, m2, season=2025, venue="home", minutes_played=saturation, conn=conn)
    sports_db.add_player_match_stats(p4, m2, season=2025, venue="home", minutes_played=saturation, conn=conn)

    _transfer(conn, away_team, "P1", "ext_p1")
    _transfer(conn, away_team, "P2", "ext_p2")
    _transfer(conn, team_a, "P3", "ext_p3")
    _transfer(conn, team_a, "P4", "ext_p4")

    default_trust = strength.player_trust_score(conn, team_a, "2026-09-14", window=1,
                                                  half_life_days=1.0e12, cutoff_days=1.0e12)
    assert default_trust == pytest.approx(1.0)

    tight_cutoff_trust = strength.player_trust_score(
        conn, team_a, "2026-09-14", window=1, half_life_days=30.0, cutoff_days=30.0)
    assert tight_cutoff_trust == pytest.approx(0.0)

    # And resolve_blend_weight (the inverse, `w`) reflects the same swing.
    assert strength.resolve_blend_weight(
        conn, team_a, "Serie A", "attack", "2026-09-14", window=1,
        half_life_days=30.0, cutoff_days=30.0) == pytest.approx(1.0)


def test_resolve_blend_weight_league_override_takes_precedence(db_path, conn, monkeypatch):
    """A league-wide override short-circuits the per-team computation entirely --
    confirmed here by NOT seeding any data (if it fell through to the real
    computation it would hit the no-history path and return 1.0, not the override)."""
    monkeypatch.setattr(strength, "PLAYER_RATING_LEAGUE_WIDE_BLEND_WEIGHT_OVERRIDE", {"Serie A": {"attack": 0.2}})
    team_a = sports_db.ensure_soccer_team("Overridden Team", "Serie A")

    assert strength.resolve_blend_weight(conn, team_a, "Serie A", "attack", "2026-09-14") == 0.2
    # Defense wasn't overridden -- falls through to the real (here: no-history) computation.
    assert strength.resolve_blend_weight(conn, team_a, "Serie A", "defense", "2026-09-14") == 1.0
    # A different league is unaffected by Serie A's override.
    assert strength.resolve_blend_weight(conn, team_a, "Premier League", "attack", "2026-09-14") == 1.0


# ── roster_as_of_date + before_date plumbing (backtesting support) ────────────────────

def test_roster_as_of_date_uses_current_season_matches_when_available(db_path, conn):
    """Once the season being backtested has its own match evidence, that's the signal
    -- not last season's roster."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2024, date="2024-09-01")
    _, _, m2 = _seed_match(conn, "TeamA", "OppB", season=2025, date="2025-09-01")
    p_old = sports_db.add_player(team_a, "Last Season Player", api_player_id="ext_lsp", conn=conn)
    p_new = sports_db.add_player(team_a, "This Season Player", api_player_id="ext_tsp", conn=conn)
    sports_db.add_player_match_stats(p_old, m1, season=2024, venue="home", minutes_played=900, conn=conn)
    sports_db.add_player_match_stats(p_new, m2, season=2025, venue="home", minutes_played=90, conn=conn)

    squad = strength.roster_as_of_date(conn, team_a, 2025, "2025-10-01")
    assert squad == {p_new}   # NOT p_old -- last season's roster isn't consulted


def test_roster_as_of_date_falls_back_to_last_season_when_no_matches_yet(db_path, conn):
    """Before the current season has any match evidence (the very start of a season),
    falls back to last season's final roster -- an honest, bounded approximation."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2024, date="2024-09-01")
    p_old = sports_db.add_player(team_a, "Last Season Player", conn=conn)
    sports_db.add_player_match_stats(p_old, m1, season=2024, venue="home", minutes_played=900, conn=conn)

    # Querying "as of" the very first day of the 2025 season -- no 2025 matches exist yet.
    squad = strength.roster_as_of_date(conn, team_a, 2025, "2025-08-01")
    assert squad == {p_old}


def test_roster_as_of_date_only_sees_matches_strictly_before_the_date(db_path, conn):
    """A match that happens ON OR AFTER the query date must not count as evidence --
    the whole point is no lookahead."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2025, date="2025-09-01")
    _, _, m2 = _seed_match(conn, "TeamA", "OppB", season=2025, date="2025-11-01")
    p_early = sports_db.add_player(team_a, "Early Player", conn=conn)
    p_later = sports_db.add_player(team_a, "Later Player", conn=conn)
    sports_db.add_player_match_stats(p_early, m1, season=2025, venue="home", minutes_played=90, conn=conn)
    sports_db.add_player_match_stats(p_later, m2, season=2025, venue="home", minutes_played=90, conn=conn)

    squad = strength.roster_as_of_date(conn, team_a, 2025, "2025-10-01")
    assert squad == {p_early}   # NOT p_later -- that match hasn't happened yet


def test_player_trust_score_accepts_current_roster_ids_override(db_path, conn):
    """The override parameter actually changes the result -- confirms it's wired
    through, not just accepted and ignored. Live default here is a rookie with
    zero tracked minutes (zero coverage, trust 0); overriding with a
    well-tracked player instead flips it to full trust."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2025, date="2025-09-01")
    old_starter = _transfer(conn, team_a, "Old Starter", "ext_old")
    sports_db.add_player_match_stats(old_starter, m1, season=2025, venue="home", minutes_played=900, conn=conn)
    away_team = sports_db.ensure_soccer_team("MovedOn", "Serie A")
    _transfer(conn, away_team, "Old Starter", "ext_old")   # no longer on team_a's LIVE roster
    sports_db.add_player(team_a, "Rookie", conn=conn)   # team_a's only current player, no minutes

    # Live default: current_roster_player_ids(team_a) == {rookie}, zero coverage.
    assert strength.player_trust_score(conn, team_a, "2026-09-14", window=1,
                                        half_life_days=1.0e12, cutoff_days=1.0e12) == pytest.approx(0.0)

    # Override with a well-tracked player instead -- full coverage, full trust.
    p2 = sports_db.add_player(team_a, "Hypothetical New Player", api_player_id="ext_hyp", conn=conn)
    team_b, opp_b, m2 = _seed_match(conn, "TeamB", "OppC", season=2025, date="2025-09-08")
    sports_db.add_player_match_stats(p2, m2, season=2025, venue="home",
                                     minutes_played=int(strength.PLAYER_RATING_COVERAGE_SATURATION_MINUTES), conn=conn)
    overridden_trust = strength.player_trust_score(
        conn, team_a, "2026-09-14", current_roster_ids={p2}, window=1,
        half_life_days=1.0e12, cutoff_days=1.0e12)
    assert overridden_trust == pytest.approx(1.0)


def test_player_trust_score_cache_avoids_recomputing_recent_window_aggregates(db_path, conn, monkeypatch):
    """BUG-011's caching still applies (2026-08-12 matchday-shifting rework changed
    WHAT the reference window is keyed by, not whether caching helps):
    resolve_blend_weight calls player_trust_score once per component (attack,
    defense) with IDENTICAL inputs for a given team/before_date, so a shared cache
    dict should still hit the underlying SQL aggregates once each across repeated
    calls at the SAME inputs."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2025, date="2025-09-01")
    p1 = _transfer(conn, team_a, "P1", "ext_p1")
    sports_db.add_player_match_stats(p1, m1, season=2025, venue="home", minutes_played=900, conn=conn)

    calls = {"team_aggregated_recent_roster_minutes": 0, "players_aggregated_recent_minutes": 0}
    orig_team_aggregated_recent_roster_minutes = strength.team_aggregated_recent_roster_minutes
    orig_players_aggregated_recent_minutes = strength.players_aggregated_recent_minutes

    def counting_team_aggregated_recent_roster_minutes(*args, **kwargs):
        calls["team_aggregated_recent_roster_minutes"] += 1
        return orig_team_aggregated_recent_roster_minutes(*args, **kwargs)

    def counting_players_aggregated_recent_minutes(*args, **kwargs):
        calls["players_aggregated_recent_minutes"] += 1
        return orig_players_aggregated_recent_minutes(*args, **kwargs)

    monkeypatch.setattr(strength, "team_aggregated_recent_roster_minutes", counting_team_aggregated_recent_roster_minutes)
    monkeypatch.setattr(strength, "players_aggregated_recent_minutes", counting_players_aggregated_recent_minutes)

    cache = {}
    results = [strength.player_trust_score(conn, team_a, "2026-09-14", cache=cache, window=1,
                                             half_life_days=1.0e12, cutoff_days=1.0e12)
              for _ in range(3)]

    assert calls["team_aggregated_recent_roster_minutes"] == 1
    assert calls["players_aggregated_recent_minutes"] == 1
    assert results[0] == results[1] == results[2]
    assert results[0] == pytest.approx(strength.player_trust_score(
        conn, team_a, "2026-09-14", window=1, half_life_days=1.0e12, cutoff_days=1.0e12))


def test_load_team_players_before_date_excludes_later_matches(db_path, conn):
    """Same no-lookahead guarantee as team_roster_minutes, for the function that
    actually feeds the player-level lambda computation."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    _, _, m2 = _seed_match(conn, "TeamA", "OppB", date="2025-11-01")
    player = sports_db.add_player(team_a, "Timeline Player", position="F", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2025, venue="home",
                                     minutes_played=90, goals=1, conn=conn)
    sports_db.add_player_match_stats(player, m2, season=2025, venue="home",
                                     minutes_played=90, goals=5, conn=conn)

    early = strength.load_team_players(conn, [team_a], "2025-10-01", half_life_days=1.0e12, cutoff_days=1.0e12)
    full = strength.load_team_players(conn, [team_a], "2026-01-01", half_life_days=1.0e12, cutoff_days=1.0e12)
    assert early[team_a][0]["attack_minutes"] == pytest.approx(90)     # only m1
    assert full[team_a][0]["attack_minutes"] == pytest.approx(180)     # both


def test_compute_falls_back_to_baseline_for_true_cold_start_team(db_path, conn):
    """A newly-promoted team's very first match: zero team-level history (no prior
    matches at all) AND zero player data (nothing clears the PLAYER_RATING_MIN_ATTACK/
    DEFENSE_WEIGHTED_MINUTES_TO_HAVE_OWN_RATING gates). team_level_lambda (2026-08-01
    home/away-split rewrite) always
    falls back to the relevant league average per component now, so this can no longer
    crash analyse_match_wc's arithmetic downstream the way the pre-fix None value did --
    same "assume average" philosophy estimate_lambdas() already uses for a team-level-
    only team with no history, now applied per home/away side instead of one pooled
    baseline."""
    team_id, opp_id, m1 = _seed_match(conn, "NewlyPromoted", "Opponent", date="2025-08-23")

    results = strength.compute(conn, [team_id], "Serie A", 2025, before_date="2025-08-23")
    r = results[team_id]
    assert r["lambda_attack_home_blend"] == pytest.approx(r["avg_home"])
    assert r["lambda_attack_away_blend"] == pytest.approx(r["avg_away"])
    assert r["lambda_defense_home_blend"] == pytest.approx(r["avg_away"])
    assert r["lambda_defense_away_blend"] == pytest.approx(r["avg_home"])


def test_compute_falls_back_to_team_level_when_league_attack_mean_is_exactly_zero(db_path, conn):
    """Found live (BUG-012 Stage 2 calibration sweep, 2026-08-14): a tight recency
    cutoff can leave so few qualifying teams that the league-wide attack_mean
    lands on EXACTLY 0.0 (here: the sole qualifying team's own raw rate is 0,
    since every one of their real matches was scoreless) -- used to reach
    `avg_home / attack_mean` and raise ZeroDivisionError. Must fall back to
    team-level instead, same as attack_mean being None entirely."""
    team_id, opp_id, m1 = _seed_match(conn, "Scoreless", "OppA", date="2025-09-01")
    _, _, m2 = _seed_match(conn, "Scoreless", "OppB", date="2025-09-08")
    _, _, m3 = _seed_match(conn, "Scoreless", "OppC", date="2025-09-15")
    _, _, m4 = _seed_match(conn, "Scoreless", "OppD", date="2025-09-22")
    player = sports_db.add_player(team_id, "Blanked Striker", position="F", conn=conn)
    for m in (m1, m2, m3, m4):
        sports_db.add_player_match_stats(player, m, season=2025, venue="home",
                                         minutes_played=90, goals=0, conn=conn)

    # Only team_id in scope -> it's the SOLE contributor to attack_vals, so
    # attack_mean == its own ra == 0.0 exactly, not None.
    results = strength.compute(conn, [team_id], "Serie A", 2025, before_date="2025-09-23")
    r = results[team_id]
    assert r["lambda_attack_home_blend"] == pytest.approx(r["avg_home"])   # team-level fallback
    assert r["lambda_attack_away_blend"] == pytest.approx(r["avg_away"])


# ── get_team_xg_ratings / team_level_lambda's team_metric switch ──────────────────
# 2026-08-02: team-level ratings now default to xG/xGA instead of actual goals (see
# FEATURE-011_BUILD_TRACKER.md task 5 -- this is what actually cleared the Model
# Calibration success criterion, after a goals-based last-N-matches window proved too
# noisy). No dedicated coverage existed for this before it became the default.

def test_get_team_xg_ratings_home_attack_is_own_teams_summed_xg(db_path, conn):
    """A team's home_attack (xG) is the sum of ITS OWN players' xg that match, not
    the opponent's."""
    team, opp, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    p1 = sports_db.add_player(team, "Striker", position="F", conn=conn)
    p2 = sports_db.add_player(opp, "OppStriker", position="F", conn=conn)
    sports_db.add_player_match_stats(p1, m1, season=2025, venue="home", minutes_played=90, xg=1.5, conn=conn)
    sports_db.add_player_match_stats(p2, m1, season=2025, venue="away", minutes_played=90, xg=0.8, conn=conn)

    ratings = strength.get_team_xg_ratings(conn, team, "2025-09-02", n=10, league="Serie A")
    assert ratings["home_attack"] == pytest.approx(1.5)
    assert ratings["home_n"] == 1


def test_get_team_xg_ratings_home_defense_is_opponents_summed_xg(db_path, conn):
    """A team's home_defense (xGA) is the AWAY opponent's total xG that match --
    already stored per-row as club_xga_per90 (backfill_club_xga.py), constant across
    a team's own rows for that match."""
    team, opp, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    p1 = sports_db.add_player(team, "Defender", position="D", conn=conn)
    sports_db.add_player_match_stats(p1, m1, season=2025, venue="home", minutes_played=90,
                                     xg=0.2, club_xga_per90=2.3, conn=conn)

    ratings = strength.get_team_xg_ratings(conn, team, "2025-09-02", n=10, league="Serie A")
    assert ratings["home_defense"] == pytest.approx(2.3)


def test_get_team_xg_ratings_respects_before_date(db_path, conn):
    """A match on/after before_date must not leak into the rating -- same no-lookahead
    discipline as get_team_ratings (BUG-008)."""
    team, opp, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    p1 = sports_db.add_player(team, "Striker", position="F", conn=conn)
    sports_db.add_player_match_stats(p1, m1, season=2025, venue="home", minutes_played=90, xg=5.0, conn=conn)

    ratings = strength.get_team_xg_ratings(conn, team, "2025-09-01", n=10, league="Serie A")
    assert ratings["home_n"] == 0
    assert ratings["home_attack"] is None


def test_get_team_xg_ratings_limits_to_last_n_matches(db_path, conn):
    """Only the most recent n matches (by date) are averaged in, same window
    convention as get_team_ratings."""
    team, opp, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    _, _, m2 = _seed_match(conn, "TeamA", "OppB", date="2025-09-08")
    p1 = sports_db.add_player(team, "Striker", position="F", conn=conn)
    sports_db.add_player_match_stats(p1, m1, season=2025, venue="home", minutes_played=90, xg=1.0, conn=conn)
    sports_db.add_player_match_stats(p1, m2, season=2025, venue="home", minutes_played=90, xg=3.0, conn=conn)

    ratings = strength.get_team_xg_ratings(conn, team, "2025-09-09", n=1, league="Serie A")
    assert ratings["home_n"] == 1
    assert ratings["home_attack"] == pytest.approx(3.0)  # only the more recent match


def test_get_team_xg_ratings_sums_all_of_a_teams_players_not_just_one(db_path, conn):
    """Team xG is the SUM across every one of the team's players who played that
    match, not a single player's value -- the whole "team-level xG is just a sum"
    question (raised digging into MD20-28's bottom6-vs-strong-team compression,
    2026-08-07) hinges on this actually being a correct multi-player sum in the SQL
    (GROUP BY match_id, venue), not silently picking one row."""
    team, opp, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    p1 = sports_db.add_player(team, "Striker", position="F", conn=conn)
    p2 = sports_db.add_player(team, "Winger", position="F", conn=conn)
    p3 = sports_db.add_player(team, "Midfielder", position="M", conn=conn)
    sports_db.add_player_match_stats(p1, m1, season=2025, venue="home", minutes_played=90, xg=1.2, conn=conn)
    sports_db.add_player_match_stats(p2, m1, season=2025, venue="home", minutes_played=90, xg=0.7, conn=conn)
    sports_db.add_player_match_stats(p3, m1, season=2025, venue="home", minutes_played=90, xg=0.3, conn=conn)

    ratings = strength.get_team_xg_ratings(conn, team, "2025-09-02", n=10, league="Serie A")
    assert ratings["home_attack"] == pytest.approx(1.2 + 0.7 + 0.3)


def test_get_team_xg_ratings_sum_ignores_teammates_with_no_xg_data(db_path, conn):
    """A teammate with no recorded xg (e.g. a keeper/defender with no shots, xg=None)
    must not zero out or otherwise corrupt the match's team total -- SQL SUM()
    ignores NULLs, it doesn't propagate them like arithmetic + would."""
    team, opp, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    p1 = sports_db.add_player(team, "Striker", position="F", conn=conn)
    p2 = sports_db.add_player(team, "Keeper", position="G", conn=conn)
    sports_db.add_player_match_stats(p1, m1, season=2025, venue="home", minutes_played=90, xg=1.4, conn=conn)
    sports_db.add_player_match_stats(p2, m1, season=2025, venue="home", minutes_played=90, xg=None, conn=conn)

    ratings = strength.get_team_xg_ratings(conn, team, "2025-09-02", n=10, league="Serie A")
    assert ratings["home_attack"] == pytest.approx(1.4)



def test_get_team_xg_ratings_decay_weights_recent_games_more(db_path, conn):
    """decay<1 weights the most recent match more than older ones in the window."""
    team, opp, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    _, _, m2 = _seed_match(conn, "TeamA", "OppB", date="2025-09-08")
    p1 = sports_db.add_player(team, "Striker", position="F", conn=conn)
    sports_db.add_player_match_stats(p1, m1, season=2025, venue="home", minutes_played=90, xg=1.0, conn=conn)
    sports_db.add_player_match_stats(p1, m2, season=2025, venue="home", minutes_played=90, xg=3.0, conn=conn)

    flat = strength.get_team_xg_ratings(conn, team, "2025-09-09", n=10, league="Serie A", decay=1.0)
    # most recent (3.0) weight 1, older (1.0) weight 0.5 => (3 + 0.5)/1.5 = 2.333...
    decayed = strength.get_team_xg_ratings(conn, team, "2025-09-09", n=10, league="Serie A", decay=0.5)
    assert flat["home_attack"] == pytest.approx(2.0)
    assert decayed["home_attack"] == pytest.approx((3.0 * 1.0 + 1.0 * 0.5) / 1.5)


def test_get_team_xg_ratings_opponent_adjust_boosts_xg_vs_stingy_defense(db_path, conn):
    """Same raw xG against a tougher defense should rate higher when adjust is on.

    Setup: TeamA away at StrongDef and SoftDef, 1.0 xG both times.
    StrongDef's home_defense (xGA allowed at home) is low; SoftDef's is high.
    With league_raw_means fixed, adjust scales attack by mean_def / opp_def.
    """
    team = sports_db.ensure_soccer_team("TeamA", "Serie A")
    strong = sports_db.ensure_soccer_team("StrongDef", "Serie A")
    soft = sports_db.ensure_soccer_team("SoftDef", "Serie A")
    fodder = sports_db.ensure_soccer_team("Fodder", "Serie A")

    # Give StrongDef a stingy home_defense history: low xGA at home before the rated matches.
    # StrongDef home vs Fodder on 2025-08-01: club_xga_per90=0.5 on StrongDef's home rows.
    m_s_hist = sports_db.add_soccer_match("Serie A", 2025, strong, fodder, "2025-08-01")
    ps = sports_db.add_player(strong, "SD1", position="D", conn=conn)
    sports_db.add_player_match_stats(ps, m_s_hist, season=2025, venue="home", minutes_played=90,
                                     xg=0.1, club_xga_per90=0.5, conn=conn)

    # SoftDef leaky at home: club_xga_per90=2.0
    m_w_hist = sports_db.add_soccer_match("Serie A", 2025, soft, fodder, "2025-08-02")
    pw = sports_db.add_player(soft, "WD1", position="D", conn=conn)
    sports_db.add_player_match_stats(pw, m_w_hist, season=2025, venue="home", minutes_played=90,
                                     xg=0.1, club_xga_per90=2.0, conn=conn)

    # TeamA away 1.0 xG at each (after their hist so opp ratings are available as-of match date)
    m1 = sports_db.add_soccer_match("Serie A", 2025, strong, team, "2025-09-01")
    m2 = sports_db.add_soccer_match("Serie A", 2025, soft, team, "2025-09-08")
    pa = sports_db.add_player(team, "Att", position="F", conn=conn)
    sports_db.add_player_match_stats(pa, m1, season=2025, venue="away", minutes_played=90, xg=1.0,
                                     club_xga_per90=1.0, conn=conn)
    sports_db.add_player_match_stats(pa, m2, season=2025, venue="away", minutes_played=90, xg=1.0,
                                     club_xga_per90=1.0, conn=conn)

    league_raw = {
        "home_attack": 1.2, "home_defense": 1.2,
        "away_attack": 1.0, "away_defense": 1.0,
    }
    raw = strength.get_team_xg_ratings(
        conn, team, "2025-09-09", n=10, league="Serie A",
        opponent_adjust=False,
    )
    adj = strength.get_team_xg_ratings(
        conn, team, "2025-09-09", n=10, league="Serie A",
        opponent_adjust=True, league_raw_means=league_raw,
    )
    assert raw["away_attack"] == pytest.approx(1.0)
    # vs strong (opp home_def=0.5): 1.0 * (1.2/0.5) = 2.4
    # vs soft   (opp home_def=2.0): 1.0 * (1.2/2.0) = 0.6
    # mean = 1.5
    assert adj["away_attack"] == pytest.approx(1.5)
    assert adj["away_attack"] > raw["away_attack"]


def test_get_team_xg_ratings_opponent_adjust_false_matches_legacy_mean(db_path, conn):
    """Default / opponent_adjust=False must stay a plain mean of team xG (no SOS)."""
    # TeamA must be the AWAY side so venue=away rows attach to away_team_id.
    opp, team, m1 = _seed_match(conn, "OppA", "TeamA", date="2025-09-01")
    p1 = sports_db.add_player(team, "Striker", position="F", conn=conn)
    sports_db.add_player_match_stats(p1, m1, season=2025, venue="away", minutes_played=90, xg=1.7, conn=conn)
    r = strength.get_team_xg_ratings(conn, team, "2025-09-02", n=10, league="Serie A")
    assert r["away_attack"] == pytest.approx(1.7)


def test_decay_weighted_mean_skips_none_and_orders_recent_first():
    assert strength._decay_weighted_mean([2.0, 0.0], 1.0) == pytest.approx(1.0)
    assert strength._decay_weighted_mean([2.0, None, 0.0], 1.0) == pytest.approx(1.0)
    assert strength._decay_weighted_mean([], 0.8) is None


def test_team_level_lambda_defaults_to_xg_not_goals(db_path, conn):
    """team_xg_v_goals_blend defaults to 1.0 (pure xG, 2026-08-02) -- must reflect the
    xG-based number, not the goals-based one, proving the default is really wired to
    get_team_xg_ratings and not silently still using core.get_team_ratings. Uses a
    deliberately large goals-vs-xg gap (5 actual goals a match vs 1.0 xg) so the two
    paths can't coincidentally agree."""
    team, opp, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    _, _, m2 = _seed_match(conn, "TeamA", "OppB", date="2025-09-08")
    _, _, m3 = _seed_match(conn, "TeamA", "OppC", date="2025-09-15")
    p1 = sports_db.add_player(team, "Striker", position="F", conn=conn)
    for mid in (m1, m2, m3):
        sports_db.update_soccer_match_result(mid, 5, 0)
        sports_db.add_player_match_stats(p1, mid, season=2025, venue="home", minutes_played=90, xg=1.0, conn=conn)

    default_result = strength.team_level_lambda(conn, team, "Serie A", "2025-09-16", avg_home=1.3, avg_away=1.1)
    xg_result = strength.team_level_lambda(conn, team, "Serie A", "2025-09-16", avg_home=1.3, avg_away=1.1, team_xg_v_goals_blend=1.0)
    goals_result = strength.team_level_lambda(conn, team, "Serie A", "2025-09-16", avg_home=1.3, avg_away=1.1, team_xg_v_goals_blend=0.0)

    assert default_result[0] == pytest.approx(xg_result[0]) == pytest.approx(1.0)
    assert goals_result[0] == pytest.approx(5.0)
    assert default_result[0] != pytest.approx(goals_result[0])


def test_team_level_lambda_team_xg_weight_blends_goals_and_xg(db_path, conn):
    """team_xg_v_goals_blend=0.5 (BUG-009's mismatch-size-compression diagnosis, 2026-08-05)
    must land exactly halfway between the pure-goals and pure-xg raw ratings, not
    just 'somewhere in between' -- same 5-goals-vs-1.0-xg setup as the pure-metric
    test above so the expected blend (3.0) is unambiguous. TEAM_RATING_PULL_TOWARD_AVERAGE_MATCHES=0 (confirmed
    disabled, BUG-009) means the shrink-to-fallback step is a no-op here since
    n_matches=3 clears TEAM_RATING_MIN_MATCHES_TO_TRUST_TEAM_RATING_OVER_LEAGUE_AVERAGE, so the raw blend passes through unchanged."""
    team, opp, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    _, _, m2 = _seed_match(conn, "TeamA", "OppB", date="2025-09-08")
    _, _, m3 = _seed_match(conn, "TeamA", "OppC", date="2025-09-15")
    p1 = sports_db.add_player(team, "Striker", position="F", conn=conn)
    for mid in (m1, m2, m3):
        sports_db.update_soccer_match_result(mid, 5, 0)
        sports_db.add_player_match_stats(p1, mid, season=2025, venue="home", minutes_played=90, xg=1.0, conn=conn)

    blended = strength.team_level_lambda(conn, team, "Serie A", "2025-09-16",
                                         avg_home=1.3, avg_away=1.1, team_xg_v_goals_blend=0.5)
    assert blended[0] == pytest.approx(3.0)   # (1-0.5)*5.0 + 0.5*1.0


def test_team_level_lambda_blend_requires_both_sources_to_clear_min_matches(db_path, conn):
    """A genuine blend (0 < team_xg_v_goals_blend < 1) uses the MIN of the goals-source and
    xg-source match counts for the TEAM_RATING_MIN_MATCHES_TO_TRUST_TEAM_RATING_OVER_LEAGUE_AVERAGE gate -- conservative by design (BUG-009,
    2026-08-05): don't trust a blended rating unless BOTH sources have enough matches,
    even if one alone would individually clear the bar. 3 matches have real results
    (goals_ratings n=3, clears TEAM_RATING_MIN_MATCHES_TO_TRUST_TEAM_RATING_OVER_LEAGUE_AVERAGE=3 alone) but only 2 have any player-stats/xg
    rows at all (xg_ratings n=2, does NOT clear it) -- the blend must fall back to
    avg_home entirely, even though a pure team_xg_v_goals_blend=0.0 (goals-only) call on the
    same data would NOT fall back."""
    team, opp, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    _, _, m2 = _seed_match(conn, "TeamA", "OppB", date="2025-09-08")
    _, _, m3 = _seed_match(conn, "TeamA", "OppC", date="2025-09-15")
    sports_db.update_soccer_match_result(m1, 5, 0)
    sports_db.update_soccer_match_result(m2, 5, 0)
    sports_db.update_soccer_match_result(m3, 5, 0)
    p1 = sports_db.add_player(team, "Striker", position="F", conn=conn)
    # Only 2 of the 3 matches get a player-stats/xg row.
    sports_db.add_player_match_stats(p1, m1, season=2025, venue="home", minutes_played=90, xg=1.0, conn=conn)
    sports_db.add_player_match_stats(p1, m2, season=2025, venue="home", minutes_played=90, xg=1.0, conn=conn)

    goals_only = strength.team_level_lambda(conn, team, "Serie A", "2025-09-16",
                                            avg_home=1.3, avg_away=1.1, team_xg_v_goals_blend=0.0)
    blended = strength.team_level_lambda(conn, team, "Serie A", "2025-09-16",
                                         avg_home=1.3, avg_away=1.1, team_xg_v_goals_blend=0.5)

    assert goals_only[0] == pytest.approx(5.0)   # goals alone clears TEAM_RATING_MIN_MATCHES_TO_TRUST_TEAM_RATING_OVER_LEAGUE_AVERAGE=3
    assert blended[0] == pytest.approx(1.3)      # blend falls back to avg_home (min(3,2)=2 < 3)


# ── spread_around_mean (BUG-014, 2026-08-14) ──────────────────────────────────────

def test_spread_around_mean_additive_matches_the_old_inline_formula():
    """mode="additive" must be byte-for-byte the same formula every call site
    used to compute inline, since this refactor's whole point is to be a
    verified no-op before any call site's behavior changes."""
    assert strength.spread_around_mean(0.15, 0.20, 2.0, mode="additive") == pytest.approx(
        0.20 + (0.15 - 0.20) * 2.0
    )


def test_spread_around_mean_additive_can_go_negative():
    """Documents the KNOWN defect, not a desired behavior -- additive mode is
    kept only for the no-op wiring stage, not as a fix."""
    result = strength.spread_around_mean(0.05, 0.20, 2.0, mode="additive")
    assert result < 0.0


def test_spread_around_mean_multiplicative_cannot_go_negative():
    """The actual fix (BUG-014): multiplicative mode on the exact same inputs
    that push additive negative above must stay >= 0."""
    result = strength.spread_around_mean(0.05, 0.20, 2.0, mode="multiplicative")
    assert result >= 0.0
    assert result == pytest.approx(0.20 * (0.05 / 0.20) ** 2.0)


def test_spread_around_mean_multiplicative_is_a_noop_at_raw_equals_mean():
    """Same no-op boundary as additive mode: when raw == mean, there's no
    distance to stretch, so both modes must agree and leave the value alone."""
    assert strength.spread_around_mean(0.20, 0.20, 2.0, mode="multiplicative") == pytest.approx(0.20)
    assert strength.spread_around_mean(0.20, 0.20, 2.0, mode="additive") == pytest.approx(0.20)


def test_spread_around_mean_multiplicative_only_reaches_zero_at_raw_zero():
    assert strength.spread_around_mean(0.0, 0.20, 2.0, mode="multiplicative") == pytest.approx(0.0)


def test_spread_around_mean_handles_none_and_non_positive_mean():
    assert strength.spread_around_mean(None, 0.20, 2.0, mode="additive") is None
    assert strength.spread_around_mean(0.15, None, 2.0, mode="additive") == pytest.approx(0.15)
    assert strength.spread_around_mean(0.15, 0.0, 2.0, mode="multiplicative") == pytest.approx(0.15)


def test_spread_around_mean_rejects_unknown_mode():
    with pytest.raises(ValueError):
        strength.spread_around_mean(0.15, 0.20, 2.0, mode="bogus")


# ── calendar_recency_weight (BUG-012 Stage 1, 2026-08-14) ─────────────────────────

def test_calendar_recency_weight_is_1_at_zero_elapsed_days():
    """A match played the day right before before_date has elapsed_days=1 (the
    smallest possible gap given every caller's strict `match_date < before_date`
    filter) -- weight must be very close to 1.0, not exactly 1.0."""
    w = strength.calendar_recency_weight("2025-08-30", "2025-08-31", half_life_days=90.0, cutoff_days=365.0)
    assert w == pytest.approx(0.5 ** (1 / 90.0))


def test_calendar_recency_weight_halves_at_the_half_life():
    w = strength.calendar_recency_weight("2025-06-01", "2025-08-30", half_life_days=90.0, cutoff_days=365.0)
    assert w == pytest.approx(0.5)


def test_calendar_recency_weight_is_zero_past_the_cutoff():
    """A hard floor, not an asymptote -- exactly 0.0 once elapsed_days exceeds
    cutoff_days, matching the design's 'any stats 3mo+ old are probably useless'
    intent (a real cutoff a caller can use to stop counting a match entirely)."""
    w = strength.calendar_recency_weight("2025-01-01", "2025-12-31", half_life_days=90.0, cutoff_days=180.0)
    assert w == 0.0


def test_calendar_recency_weight_at_exactly_the_cutoff_is_still_positive():
    """cutoff_days is the boundary elapsed_days must EXCEED to zero out -- a
    match exactly at the cutoff still gets its (small) decayed weight."""
    w = strength.calendar_recency_weight("2025-01-01", "2025-06-30", half_life_days=90.0, cutoff_days=180.0)
    assert w > 0.0
    assert w == pytest.approx(0.5 ** (180 / 90.0))


def test_calendar_recency_weight_longer_half_life_decays_slower():
    fast = strength.calendar_recency_weight("2025-06-01", "2025-08-30", half_life_days=30.0, cutoff_days=365.0)
    slow = strength.calendar_recency_weight("2025-06-01", "2025-08-30", half_life_days=180.0, cutoff_days=365.0)
    assert slow > fast


def test_calendar_recency_weight_linear_shape_is_a_straight_ramp_to_the_cutoff():
    """shape='linear' (added 2026-08-14, exploring a slower-feeling decay than
    exponential per user feedback that half-life decay felt too fast relative
    to the cutoff): weight = 1 - elapsed_days / cutoff_days -- half_life_days is
    unused in this shape, the cutoff alone defines the whole ramp."""
    w = strength.calendar_recency_weight("2025-06-01", "2025-08-30", cutoff_days=180.0, shape="linear")
    elapsed = 90
    assert w == pytest.approx(1.0 - elapsed / 180.0)


def test_calendar_recency_weight_linear_shape_is_1_at_zero_elapsed_and_0_at_cutoff():
    at_start = strength.calendar_recency_weight("2025-08-30", "2025-08-31", cutoff_days=90.0, shape="linear")
    assert at_start == pytest.approx(1.0 - 1 / 90.0)
    at_cutoff = strength.calendar_recency_weight("2025-06-02", "2025-08-31", cutoff_days=90.0, shape="linear")
    assert at_cutoff == pytest.approx(0.0, abs=1e-9)


def test_calendar_recency_weight_linear_decays_slower_than_exponential_near_the_start():
    """The whole point of adding this shape: at the SAME cutoff, linear should
    keep more weight on recent-but-not-brand-new matches than exponential decay
    with a half-life well inside that cutoff does."""
    linear = strength.calendar_recency_weight("2025-06-01", "2025-07-01", cutoff_days=90.0, shape="linear")
    exponential = strength.calendar_recency_weight("2025-06-01", "2025-07-01", half_life_days=30.0,
                                                    cutoff_days=90.0, shape="exponential")
    assert linear > exponential


def test_calendar_recency_weight_flat_shape_is_full_weight_inside_the_cutoff():
    """shape='flat' (added 2026-08-14): isolates the "count-window -> calendar-
    window" swap from adding any decay curve at all -- full weight (1.0) for
    any match within cutoff_days, same old-behavior flatness the count-based
    window used to have (decay=1.0 shipped), just bounded by calendar days
    instead of by game count. half_life_days is unused, same as linear."""
    just_inside = strength.calendar_recency_weight("2025-03-01", "2025-08-27", cutoff_days=180.0, shape="flat")
    assert just_inside == pytest.approx(1.0)


def test_calendar_recency_weight_flat_shape_is_zero_just_past_the_cutoff():
    just_outside = strength.calendar_recency_weight("2025-02-01", "2025-08-27", cutoff_days=180.0, shape="flat")
    assert just_outside == pytest.approx(0.0)


def test_calendar_recency_weight_rejects_unknown_shape():
    with pytest.raises(ValueError):
        strength.calendar_recency_weight("2025-08-30", "2025-08-31", cutoff_days=90.0, shape="bogus")


def test_calendar_recency_weight_same_calendar_day_is_not_an_error():
    """Two matches on the same calendar day with different kickoff times (a real
    case: an early and a late kickoff on the same matchday) truncate to
    elapsed_days == 0 -- valid, not a lookahead, full weight."""
    w = strength.calendar_recency_weight(
        "2025-08-22T16:30:00.000Z", "2025-08-22T18:30:00.000Z", half_life_days=90.0, cutoff_days=365.0
    )
    assert w == pytest.approx(1.0)
    assert strength.calendar_recency_weight("2025-08-31", "2025-08-31", half_life_days=90.0, cutoff_days=365.0) == pytest.approx(1.0)


def test_calendar_recency_weight_rejects_negative_elapsed_days():
    """A genuinely negative gap (before_date's calendar day earlier than
    match_date's) signals a real caller bug -- a lookahead -- not a valid
    input -- fail loud rather than silently return a meaningless weight."""
    with pytest.raises(ValueError):
        strength.calendar_recency_weight("2025-09-01", "2025-08-31", half_life_days=90.0, cutoff_days=365.0)


def test_calendar_recency_weight_handles_full_timestamp_match_dates():
    """soccer_matches.match_date carries a full timestamp for some data sources
    (e.g. TheStatsAPI-sourced leagues: '2025-08-22T18:30:00.000Z'), plain
    'YYYY-MM-DD' for others -- found live backfilling Bundesliga under this
    function. Must parse both the same way (first 10 chars)."""
    w = strength.calendar_recency_weight(
        "2025-08-22T18:30:00.000Z", "2025-08-31", half_life_days=90.0, cutoff_days=365.0
    )
    assert w == pytest.approx(0.5 ** (9 / 90.0))


def test_calendar_recency_weight_defaults_are_the_shipped_stage2_values():
    """Stage 2 (2026-08-15, shipped as poisson_v4_2): module defaults are the real,
    swept half_life=120d/cutoff=180d -- picked from a multi-league sweep pooled
    across all 5 leagues x 2 seasons (BUGS.md BUG-012). Pins the actual shipped
    behavior directly, rather than the old Stage 1 near-no-op assumption this test
    used to check (module defaults are no longer a no-op)."""
    assert strength.PLAYER_RATING_RECENCY_HALF_LIFE_DAYS == pytest.approx(120.0)
    assert strength.PLAYER_RATING_RECENCY_CUTOFF_DAYS == pytest.approx(180.0)
    # A match 120 days old should carry ~half weight; one past 180 days, none.
    w_at_half_life = strength.calendar_recency_weight("2025-05-03", "2025-08-31")
    assert w_at_half_life == pytest.approx(0.5, abs=1e-2)
    w_past_cutoff = strength.calendar_recency_weight("2025-01-01", "2025-08-31")
    assert w_past_cutoff == 0.0


# ── match_calendar_date/matches_on_date (BUG-016, 2026-08-15) ──────────────────

def test_match_calendar_date_truncates_a_full_timestamp():
    assert strength.match_calendar_date("2025-08-22T18:30:00.000Z") == "2025-08-22"


def test_match_calendar_date_is_a_noop_on_a_plain_date():
    assert strength.match_calendar_date("2025-08-22") == "2025-08-22"


def test_matches_on_date_groups_same_day_different_kickoff_times_together():
    """The bug this function fixes: two matches on the same Saturday at
    different kickoff times used to land in separate itertools.groupby groups
    (grouped by the exact match_date string) and trigger two redundant
    full-league compute() calls instead of one."""
    rows = [
        {"match_id": 1, "match_date": "2025-08-22T15:00:00.000Z"},
        {"match_id": 2, "match_date": "2025-08-22T17:30:00.000Z"},
        {"match_id": 3, "match_date": "2025-08-23T15:00:00.000Z"},
    ]
    same_day = strength.matches_on_date(rows, "2025-08-22")
    assert [r["match_id"] for r in same_day] == [1, 2]
    next_day = strength.matches_on_date(rows, "2025-08-23")
    assert [r["match_id"] for r in next_day] == [3]


def test_matches_on_date_handles_plain_date_rows_too():
    rows = [{"match_id": 1, "match_date": "2025-08-22"}, {"match_id": 2, "match_date": "2025-08-23"}]
    assert [r["match_id"] for r in strength.matches_on_date(rows, "2025-08-22")] == [1]


def test_matches_on_date_returns_empty_for_a_date_with_no_matches():
    rows = [{"match_id": 1, "match_date": "2025-08-22"}]
    assert strength.matches_on_date(rows, "2025-09-01") == []


def test_bare_calendar_date_before_date_excludes_every_same_day_match():
    """A bare 'YYYY-MM-DD' before_date (what matches_on_date's caller now
    passes to compute()) is a strict string-prefix of any full-timestamp
    match_date on that same day, so 'match_date < before_date' correctly
    excludes ALL of that day's matches -- including an earlier same-day
    kickoff, which the old exact-timestamp grouping could let leak into a
    later same-day match's rating (before_date = that later match's own,
    later timestamp)."""
    assert not ("2025-08-22T15:00:00.000Z" < "2025-08-22")
    assert "2025-08-15T18:30:00.000Z" < "2025-08-22"


# ── TEAM_RATING_XG_SPREAD_STRETCH_ATTACK/_DEFENSE (BUG-009, 2026-08-07 addendum;
# split into separate attack/defense constants 2026-08-12, BUG-010 continued) ────

def test_team_level_lambda_stretch_noop_without_league_means(db_path, conn):
    """xg_spread_stretch_attack/_defense default to 1.3/1.3, but team_level_lambda
    can't recenter without a league-wide mean to stretch around -- calling it
    directly (as every other test in this file does) without league_xg_means must
    be an EXACT no-op regardless of either stretch factor's value, so isolated
    blend tests above don't need to also fake up a league snapshot."""
    team, opp, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    _, _, m2 = _seed_match(conn, "TeamA", "OppB", date="2025-09-08")
    _, _, m3 = _seed_match(conn, "TeamA", "OppC", date="2025-09-15")
    p1 = sports_db.add_player(team, "Striker", position="F", conn=conn)
    for mid in (m1, m2, m3):
        sports_db.add_player_match_stats(p1, mid, season=2025, venue="home", minutes_played=90, xg=2.0, conn=conn)

    default_result = strength.team_level_lambda(conn, team, "Serie A", "2025-09-16", avg_home=1.3, avg_away=1.1)
    no_stretch_result = strength.team_level_lambda(conn, team, "Serie A", "2025-09-16",
                                                    avg_home=1.3, avg_away=1.1,
                                                    xg_spread_stretch_attack=1.0, xg_spread_stretch_defense=1.0)
    assert default_result[0] == pytest.approx(2.0) == pytest.approx(no_stretch_result[0])


def test_team_level_lambda_stretch_recenters_on_league_mean(db_path, conn):
    """With a league_xg_means snapshot supplied, xg_spread_stretch_attack actually
    moves the raw xG rating: stretched = league_mean * (raw / league_mean) ** factor
    (multiplicative, BUG-014 2026-08-14 -- see spread_around_mean; was additive
    before this fix, see git history/BUGS.md for the prior formula and the
    negative-overshoot defect it had).
    Team's own raw home_attack is 2.0 (3 matches clears TEAM_RATING_MIN_MATCHES_TO_TRUST_TEAM_RATING_OVER_LEAGUE_AVERAGE,
    and TEAM_RATING_PULL_TOWARD_AVERAGE_MATCHES=0 makes the shrink-to-fallback step a no-op), so the result
    should be exactly the stretched value, not the raw 2.0."""
    team, opp, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    _, _, m2 = _seed_match(conn, "TeamA", "OppB", date="2025-09-08")
    _, _, m3 = _seed_match(conn, "TeamA", "OppC", date="2025-09-15")
    p1 = sports_db.add_player(team, "Striker", position="F", conn=conn)
    for mid in (m1, m2, m3):
        sports_db.add_player_match_stats(p1, mid, season=2025, venue="home", minutes_played=90, xg=2.0, conn=conn)

    league_means = {"home_attack": 1.0, "home_defense": 1.0, "away_attack": 1.0, "away_defense": 1.0}
    result = strength.team_level_lambda(conn, team, "Serie A", "2025-09-16", avg_home=1.3, avg_away=1.1,
                                        xg_spread_stretch_attack=1.3, xg_spread_stretch_defense=1.3,
                                        league_xg_means=league_means)
    assert result[0] == pytest.approx(1.0 * (2.0 / 1.0) ** 1.3)  # ~2.462


def test_team_level_lambda_attack_and_defense_stretch_apply_independently(db_path, conn):
    """The whole point of splitting the constant (2026-08-12, BUG-010 continued):
    attack and defense fields must respond to their OWN factor, not share one --
    stretch attack hard while leaving defense untouched, and confirm only the
    attack-side field actually moved."""
    team, opp, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    _, _, m2 = _seed_match(conn, "TeamA", "OppB", date="2025-09-08")
    _, _, m3 = _seed_match(conn, "TeamA", "OppC", date="2025-09-15")
    p1 = sports_db.add_player(team, "Striker", position="F", conn=conn)
    for mid in (m1, m2, m3):
        sports_db.add_player_match_stats(p1, mid, season=2025, venue="home",
                                         minutes_played=90, xg=2.0, club_xga_per90=2.0, conn=conn)

    league_means = {"home_attack": 1.0, "home_defense": 1.0, "away_attack": 1.0, "away_defense": 1.0}
    home_attack, _, home_defense, _ = strength.team_level_lambda(
        conn, team, "Serie A", "2025-09-16", avg_home=1.3, avg_away=1.1,
        xg_spread_stretch_attack=2.0, xg_spread_stretch_defense=1.0, league_xg_means=league_means)
    assert home_attack == pytest.approx(1.0 * (2.0 / 1.0) ** 2.0)  # stretched (multiplicative), = 4.0
    assert home_defense == pytest.approx(2.0)                       # untouched (raw, factor=1.0)


def test_team_level_lambda_stretch_cannot_push_attack_negative(db_path, conn):
    """BUG-014 (2026-08-14): same unguarded formula the player-level stretch had
    (test_compute_player_spread_stretch_cannot_push_attack_negative), one level
    up. Was additive (`league_mean + (raw - league_mean) * factor`, no floor); a
    team with a raw home_attack of 0.0 against a league mean of 1.0, stretched
    by 2.0x, used to overshoot to -1.0. Now multiplicative (spread_around_mean,
    mode="multiplicative") -- structurally can't go negative."""
    team, opp, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    _, _, m2 = _seed_match(conn, "TeamA", "OppB", date="2025-09-08")
    _, _, m3 = _seed_match(conn, "TeamA", "OppC", date="2025-09-15")
    p1 = sports_db.add_player(team, "Striker", position="F", conn=conn)
    for mid in (m1, m2, m3):
        sports_db.add_player_match_stats(p1, mid, season=2025, venue="home", minutes_played=90, xg=0.0, conn=conn)

    league_means = {"home_attack": 1.0, "home_defense": 1.0, "away_attack": 1.0, "away_defense": 1.0}
    result = strength.team_level_lambda(conn, team, "Serie A", "2025-09-16", avg_home=1.3, avg_away=1.1,
                                        xg_spread_stretch_attack=2.0, xg_spread_stretch_defense=1.0,
                                        league_xg_means=league_means)
    home_attack = result[0]
    assert home_attack >= 0.0


def test_league_xg_field_means_averages_across_team_ids(db_path, conn):
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    team_b, opp_b, m2 = _seed_match(conn, "TeamB", "OppB", date="2025-09-01")
    p1 = sports_db.add_player(team_a, "StrikerA", position="F", conn=conn)
    p2 = sports_db.add_player(team_b, "StrikerB", position="F", conn=conn)
    sports_db.add_player_match_stats(p1, m1, season=2025, venue="home", minutes_played=90, xg=1.0, conn=conn)
    sports_db.add_player_match_stats(p2, m2, season=2025, venue="home", minutes_played=90, xg=3.0, conn=conn)

    means = strength.league_xg_field_means(conn, [team_a, team_b], "2025-09-02", league="Serie A")
    assert means["home_attack"] == pytest.approx((1.0 + 3.0) / 2)


def test_compute_wires_xg_spread_stretch_through_to_team_level_lambda(db_path, conn):
    """End-to-end: compute()'s default xg_spread_stretch_attack=1.3 must actually
    reach team_level_lambda via a real league_xg_means snapshot built from
    team_ids -- not just accepted as a parameter and silently dropped. Two teams
    with different raw xG so the league mean isn't equal to either team's own raw
    value, making the stretch's effect on the unblended team-level number
    unambiguous."""
    team_a, opp_a, ma1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    _, _, ma2 = _seed_match(conn, "TeamA", "OppA2", date="2025-09-08")
    _, _, ma3 = _seed_match(conn, "TeamA", "OppA3", date="2025-09-15")
    team_b, opp_b, mb1 = _seed_match(conn, "TeamB", "OppB", date="2025-09-01")
    _, _, mb2 = _seed_match(conn, "TeamB", "OppB2", date="2025-09-08")
    _, _, mb3 = _seed_match(conn, "TeamB", "OppB3", date="2025-09-15")
    pa = sports_db.add_player(team_a, "StrikerA", position="F", conn=conn)
    pb = sports_db.add_player(team_b, "StrikerB", position="F", conn=conn)
    for mid in (ma1, ma2, ma3):
        sports_db.add_player_match_stats(pa, mid, season=2025, venue="home", minutes_played=90, xg=2.0, conn=conn)
    for mid in (mb1, mb2, mb3):
        sports_db.add_player_match_stats(pb, mid, season=2025, venue="home", minutes_played=90, xg=1.0, conn=conn)

    stretched = strength.compute(conn, [team_a, team_b], "Serie A", 2025, "2025-09-16")
    unstretched = strength.compute(conn, [team_a, team_b], "Serie A", 2025, "2025-09-16",
                                   xg_spread_stretch_attack=1.0, xg_spread_stretch_defense=1.0)

    assert unstretched[team_a]["lambda_attack_team_home"] == pytest.approx(2.0)
    league_mean = (2.0 + 1.0) / 2
    assert stretched[team_a]["lambda_attack_team_home"] == pytest.approx(league_mean * (2.0 / league_mean) ** 1.3)
    assert stretched[team_a]["lambda_attack_team_home"] != pytest.approx(unstretched[team_a]["lambda_attack_team_home"])


# ── PLAYER_RATING_SPREAD_STRETCH_ATTACK/_DEFENSE (2026-08-12, BUG-010 continued) ──

def test_compute_player_spread_stretch_true_noop_at_1_1(db_path, conn):
    """Explicit 1.0/1.0 must reproduce the pre-2026-08-12 player-level rating
    exactly, same discipline as every other stretch/blend lever in this file
    having a true no-op boundary -- checked against a stretch value (2.0) known
    to move the rating, per test_compute_player_spread_stretch_moves_attack_away_
    from_the_mean, so this isn't vacuous."""
    team_a, opp_a, ma1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    team_b, opp_b, mb1 = _seed_match(conn, "TeamB", "OppB", date="2025-09-01")
    pa = sports_db.add_player(team_a, "StrikerA", position="F", conn=conn)
    pb = sports_db.add_player(team_b, "StrikerB", position="F", conn=conn)
    # 400, not exactly 300 -- clears PLAYER_RATING_MIN_ATTACK_WEIGHTED_MINUTES_TO_
    # HAVE_OWN_RATING with room to spare, so BUG-012's calendar decay (even at its
    # near-no-op Stage 1 defaults, which shave an infinitesimal amount off any
    # nonzero elapsed gap) can't flip this player across the boundary.
    sports_db.add_player_match_stats(pa, ma1, season=2025, venue="home", minutes_played=400, goals=2, conn=conn)
    sports_db.add_player_match_stats(pb, mb1, season=2025, venue="home", minutes_played=400, goals=0, conn=conn)

    noop = strength.compute(conn, [team_a, team_b], "Serie A", 2025, "2025-09-02",
                            xg_spread_stretch_attack=1.0, xg_spread_stretch_defense=1.0,
                            player_spread_stretch_attack=1.0, player_spread_stretch_defense=1.0)
    moved = strength.compute(conn, [team_a, team_b], "Serie A", 2025, "2025-09-02",
                             xg_spread_stretch_attack=1.0, xg_spread_stretch_defense=1.0,
                             player_spread_stretch_attack=2.0, player_spread_stretch_defense=1.0)
    assert noop[team_a]["lambda_attack_player_home"] != pytest.approx(moved[team_a]["lambda_attack_player_home"])


def test_compute_player_spread_stretch_default_is_the_locked_in_attack_value(db_path, conn):
    """2026-08-12: PLAYER_RATING_SPREAD_STRETCH_ATTACK shipped as 2.0 (calibration
    sweep, see its own comment for the bias/Brier/ROI tradeoff) -- compute() called
    WITHOUT explicit player_spread_stretch_attack/defense must now reproduce that
    2.0/1.0 shape, not 1.0/1.0 (which stopped being the default this same day)."""
    team_a, opp_a, ma1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    team_b, opp_b, mb1 = _seed_match(conn, "TeamB", "OppB", date="2025-09-01")
    pa = sports_db.add_player(team_a, "StrikerA", position="F", conn=conn)
    pb = sports_db.add_player(team_b, "StrikerB", position="F", conn=conn)
    sports_db.add_player_match_stats(pa, ma1, season=2025, venue="home", minutes_played=400, goals=2, conn=conn)
    sports_db.add_player_match_stats(pb, mb1, season=2025, venue="home", minutes_played=400, goals=0, conn=conn)

    default = strength.compute(conn, [team_a, team_b], "Serie A", 2025, "2025-09-02",
                               xg_spread_stretch_attack=1.0, xg_spread_stretch_defense=1.0)
    explicit = strength.compute(conn, [team_a, team_b], "Serie A", 2025, "2025-09-02",
                                xg_spread_stretch_attack=1.0, xg_spread_stretch_defense=1.0,
                                player_spread_stretch_attack=2.0, player_spread_stretch_defense=1.0)
    assert default[team_a]["lambda_attack_player_home"] == pytest.approx(explicit[team_a]["lambda_attack_player_home"])


def test_compute_player_spread_stretch_moves_attack_away_from_the_mean(db_path, conn):
    """A non-1.0 player_spread_stretch_attack pushes an above-mean team's raw
    player-level rate FURTHER from the league mean before the avg_home/attack_mean
    unit conversion -- confirms the stretch is applied at the right stage (before
    that conversion, per the docstring: a pure linear rescale afterward wouldn't
    change relative dispersion at all, so if this were wired in at the wrong point
    the stretch would have no visible effect on the unit-converted lambda)."""
    team_a, opp_a, ma1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    team_b, opp_b, mb1 = _seed_match(conn, "TeamB", "OppB", date="2025-09-01")
    pa = sports_db.add_player(team_a, "StrikerA", position="F", conn=conn)
    pb = sports_db.add_player(team_b, "StrikerB", position="F", conn=conn)
    sports_db.add_player_match_stats(pa, ma1, season=2025, venue="home", minutes_played=400, goals=2, conn=conn)
    sports_db.add_player_match_stats(pb, mb1, season=2025, venue="home", minutes_played=400, goals=0, conn=conn)

    unstretched = strength.compute(conn, [team_a, team_b], "Serie A", 2025, "2025-09-02",
                                   xg_spread_stretch_attack=1.0, xg_spread_stretch_defense=1.0,
                                   player_spread_stretch_attack=1.0)
    stretched = strength.compute(conn, [team_a, team_b], "Serie A", 2025, "2025-09-02",
                                 xg_spread_stretch_attack=1.0, xg_spread_stretch_defense=1.0,
                                 player_spread_stretch_attack=2.0)

    unstretched_val = unstretched[team_a]["lambda_attack_player_home"]
    stretched_val = stretched[team_a]["lambda_attack_player_home"]
    assert stretched_val != pytest.approx(unstretched_val)
    # team_a is the above-mean team (2 goals vs team_b's 0) -- stretching pushes it
    # FURTHER above the league's own avg_home baseline, not toward it.
    assert stretched_val > unstretched_val


def test_compute_player_spread_stretch_attack_and_defense_apply_independently(db_path, conn):
    """Same independence guarantee as the team-level pair -- stretching attack
    must not move the defense-side player lambda, and vice versa."""
    team_a, opp_a, ma1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    team_b, opp_b, mb1 = _seed_match(conn, "TeamB", "OppB", date="2025-09-01")
    pa = sports_db.add_player(team_a, "StrikerA", position="F", conn=conn)
    pb = sports_db.add_player(team_b, "StrikerB", position="F", conn=conn)
    sports_db.add_player_match_stats(pa, ma1, season=2025, venue="home", minutes_played=400,
                                     goals=2, club_ga_per90=2.0, conn=conn)
    sports_db.add_player_match_stats(pb, mb1, season=2025, venue="home", minutes_played=400,
                                     goals=0, club_ga_per90=0.5, conn=conn)

    baseline = strength.compute(conn, [team_a, team_b], "Serie A", 2025, "2025-09-02",
                                xg_spread_stretch_attack=1.0, xg_spread_stretch_defense=1.0,
                                player_spread_stretch_attack=1.0, player_spread_stretch_defense=1.0)
    attack_only = strength.compute(conn, [team_a, team_b], "Serie A", 2025, "2025-09-02",
                                   xg_spread_stretch_attack=1.0, xg_spread_stretch_defense=1.0,
                                   player_spread_stretch_attack=2.0, player_spread_stretch_defense=1.0)

    assert attack_only[team_a]["lambda_attack_player_home"] != pytest.approx(baseline[team_a]["lambda_attack_player_home"])
    assert attack_only[team_a]["lambda_defense_player_home"] == pytest.approx(baseline[team_a]["lambda_defense_player_home"])


def test_compute_player_spread_stretch_cannot_push_attack_negative(db_path, conn):
    """BUG-014 (2026-08-14): the player_spread_stretch_attack recentering
    (`new = mean + (raw - mean) * factor`) has no floor -- a team whose raw rate
    sits far enough below the league mean gets pushed past zero into a negative
    "attack rate," which is meaningless (a rate of scoring goals can't be
    negative) and was previously silently absorbed by an unrelated hardcoded
    floor on the FINAL match lambda three steps downstream (core.poisson_model.
    analyse_match_wc's `max(lambda, 0.1)`), not caught at its actual source.
    Found via 1. FC Koeln, whose real player pool (thinned further by BUG-013)
    produced exactly this shape.

    Scenario: TeamA plays 10 real matches with zero goals across its whole
    player-rating window (a genuinely toothless attack, not just a thin
    sample -- shrinkage alone isn't enough to protect it here). TeamB has one
    high-scoring outlier match that pulls the league-wide mean up hard. TeamA's
    shrunk rate ends up under half the resulting mean, so the locked-in 2.0x
    stretch overshoots past zero -- confirmed BEFORE this fix lands
    (lambda_attack_player_home == -0.1444...), which is exactly the defect this
    test locks in a fix for."""
    team_a, _, _ = _seed_match(conn, "WeakAttack", "WeakAttackOpp0", date="2025-01-01")
    for i in range(10):
        opp = sports_db.ensure_soccer_team(f"WeakAttackOpp{i}", "Serie A")
        match_id = sports_db.add_soccer_match("Serie A", 2025, team_a, opp, f"2025-{(i % 8) + 1:02d}-01")
        if i == 0:
            player_a = sports_db.add_player(team_a, "StrikerA", position="F", conn=conn)
        sports_db.add_player_match_stats(player_a, match_id, season=2025, venue="home",
                                         minutes_played=90, goals=0, conn=conn)

    team_b, _, match_b = _seed_match(conn, "StrongAttack", "StrongAttackOpp", date="2025-09-01")
    player_b = sports_db.add_player(team_b, "StrikerB", position="F", conn=conn)
    sports_db.add_player_match_stats(player_b, match_b, season=2025, venue="home",
                                     minutes_played=300, goals=10, conn=conn)

    result = strength.compute(conn, [team_a, team_b], "Serie A", 2025, "2025-09-02",
                              xg_spread_stretch_attack=1.0, xg_spread_stretch_defense=1.0,
                              player_spread_stretch_attack=2.0, player_spread_stretch_defense=1.0,
                              player_recency_half_life_days=1.0e12, player_recency_cutoff_days=1.0e12)
    assert result[team_a]["lambda_attack_player_home"] >= 0.0
    assert result[team_a]["lambda_attack_player_home"] is not None


def test_compute_player_spread_stretch_cannot_push_defense_negative(db_path, conn):
    """Defense-side sibling of test_compute_player_spread_stretch_cannot_push_
    attack_negative -- same defect, mirrored: a team with an exceptionally LOW
    (strong) raw defense rate, pulled far enough below the league mean by a
    high-conceding outlier team, overshoots past zero under stretch. Uses an
    EXPLICIT player_spread_stretch_defense=2.0 rather than the production
    default (currently 1.0, a true no-op) -- this test protects the formula
    itself for whenever that constant is ever tuned away from 1.0, not just
    today's shipped (inert) value.

    TeamB needs >=300 WEIGHTED minutes (minutes * DEF's 0.8 position weight)
    to even join the league-wide defense_mean at all
    (PLAYER_RATING_MIN_DEFENSE_WEIGHTED_MINUTES_TO_JOIN_LEAGUE_AVERAGE) -- 375
    raw minutes clears that (375*0.8=300); 300 raw minutes (the attack test's
    number) does NOT (300*0.8=240), which was tried first and silently made
    defense_mean equal TeamA's own value (a guaranteed no-op regardless of
    stretch mode) -- caught by manually re-deriving why an early version of
    this test passed even under still-additive code, not by assuming."""
    team_a, _, _ = _seed_match(conn, "StrongDefense", "StrongDefenseOpp0", date="2025-01-01")
    for i in range(10):
        opp = sports_db.ensure_soccer_team(f"StrongDefenseOpp{i}", "Serie A")
        match_id = sports_db.add_soccer_match("Serie A", 2025, team_a, opp, f"2025-{(i % 8) + 1:02d}-01")
        if i == 0:
            player_a = sports_db.add_player(team_a, "DefenderA", position="D", conn=conn)
        sports_db.add_player_match_stats(player_a, match_id, season=2025, venue="home",
                                         minutes_played=90, goals=0, club_ga_per90=0.0, conn=conn)

    team_b, _, match_b = _seed_match(conn, "WeakDefense", "WeakDefenseOpp", date="2025-09-01")
    player_b = sports_db.add_player(team_b, "DefenderB", position="D", conn=conn)
    sports_db.add_player_match_stats(player_b, match_b, season=2025, venue="home",
                                     minutes_played=375, goals=0, club_ga_per90=10.0, conn=conn)

    result = strength.compute(conn, [team_a, team_b], "Serie A", 2025, "2025-09-02",
                              xg_spread_stretch_attack=1.0, xg_spread_stretch_defense=1.0,
                              player_spread_stretch_attack=1.0, player_spread_stretch_defense=2.0,
                              player_recency_half_life_days=1.0e12, player_recency_cutoff_days=1.0e12)
    assert result[team_a]["lambda_defense_player_home"] >= 0.0
    assert result[team_a]["lambda_defense_player_home"] is not None


# ── load_team_players: rolling window (FEATURE-011 Follow-up B, 2026-08-06) ──────
# Replaces the old flat season-to-date sum, and the separate blend_prior_season_
# attack/PRIOR_SEASON_DISCOUNT mechanism retired the same day (see MODEL_TUNING_
# PARAMETERS.md and BUGS.md's FEATURE-011 entry for the design discussion).

def _seed_match_league(conn, league, home_name, away_name, season, date):
    home = sports_db.ensure_soccer_team(home_name, league)
    away = sports_db.ensure_soccer_team(away_name, league)
    match_id = sports_db.add_soccer_match(league, season, home, away, date)
    return home, away, match_id


def test_candidate_narrowing_excludes_a_player_outside_the_cutoff_window(db_path, conn):
    """BUG-012 root cause #3 (2026-08-15, v4_3): the candidate-narrowing gate is now
    calendar-bound (cutoff_days), replacing the old count-based "team's last
    window_size matches" rule. A player whose only appearance for `team` is further
    back than cutoff_days is excluded -- genuinely stale, not just off the count."""
    team, opp, m1 = _seed_match(conn, "Home", "OppA", date="2025-01-01")
    stale = sports_db.add_player(team, "Genuinely Stale", position="F", conn=conn)
    sports_db.add_player_match_stats(stale, m1, season=2025, venue="home",
                                     minutes_played=90, goals=1, conn=conn)

    by_team = strength.load_team_players(conn, [team], "2025-09-16", window_size=10,
                                         half_life_days=1.0e12, cutoff_days=180.0)
    assert stale not in {p["player_id"] for p in by_team[team]}


def test_candidate_narrowing_includes_a_player_within_cutoff_even_if_team_played_many_matches_since(db_path, conn):
    """The actual bug root cause #3 fixes: the OLD count-based gate ("team's last
    window_size matches") was fixture-density-sensitive -- a team squeezing many
    matches into a short calendar span could exclude a player who's still
    genuinely recent in real elapsed time. The NEW gate doesn't care how many
    matches the team has played since, only how long ago (in calendar time) the
    player's own appearance was."""
    team, opp, m1 = _seed_match(conn, "Home", "OppA", date="2025-09-01")
    recent = sports_db.add_player(team, "Recent But Bypassed", position="F", conn=conn)
    sports_db.add_player_match_stats(recent, m1, season=2025, venue="home",
                                     minutes_played=90, goals=1, conn=conn)
    # Team plays on WITHOUT this player -- 10 more matches in the following days (a
    # dense fixture stretch) -- would push m1 outside a window_size=10 count-based
    # gate even though only ~2 weeks have actually elapsed.
    for i in range(10):
        opp_i = sports_db.ensure_soccer_team(f"Opp{i}", "Serie A")
        sports_db.add_soccer_match("Serie A", 2025, team, opp_i, f"2025-09-{2 + i:02d}")

    by_team = strength.load_team_players(conn, [team], "2025-09-16", window_size=10,
                                         half_life_days=1.0e12, cutoff_days=180.0)
    assert recent in {p["player_id"] for p in by_team[team]}


def test_candidate_narrowing_excludes_a_player_below_the_minimum_weighted_minutes_floor(db_path, conn):
    """Real, recent appearance, but too thin to plausibly matter --
    PLAYER_RATING_MIN_TEAM_WEIGHTED_MINUTES_TO_BE_A_CANDIDATE (10.0) is a sanity
    floor against single-minute-cameo noise, not a meaningful calibration target
    (per the user's own reasoning: apply_shrinkage already handles thin samples
    that DO clear the gate, so the gate itself can stay deliberately low)."""
    team, opp, m1 = _seed_match(conn, "Home", "OppA", date="2025-09-01")
    cameo = sports_db.add_player(team, "Fleeting Cameo", position="F", conn=conn)
    sports_db.add_player_match_stats(cameo, m1, season=2025, venue="home", minutes_played=5, conn=conn)

    by_team = strength.load_team_players(conn, [team], "2025-09-02", window_size=10,
                                         half_life_days=1.0e12, cutoff_days=180.0)
    assert cameo not in {p["player_id"] for p in by_team[team]}


def test_candidate_narrowing_includes_a_player_at_the_minimum_weighted_minutes_floor(db_path, conn):
    """Inclusive floor: exactly the minimum weighted minutes still clears the gate.
    Uses same-calendar-day timestamps (elapsed_days == 0, a real, common case --
    see calendar_recency_weight's own docstring) so the decay weight is EXACTLY
    1.0, not a floating-point-near-1.0 value that could tip either side of the
    boundary depending on half_life_days -- isolates the floor's own >= semantics
    from decay-precision noise."""
    team, opp, m1 = _seed_match(conn, "Home", "OppA", date="2025-09-01T08:00:00")
    just_enough = sports_db.add_player(team, "Just Enough", position="F", conn=conn)
    sports_db.add_player_match_stats(just_enough, m1, season=2025, venue="home", minutes_played=10, conn=conn)

    by_team = strength.load_team_players(conn, [team], "2025-09-01T20:00:00", window_size=10,
                                         half_life_days=90.0, cutoff_days=180.0)
    assert just_enough in {p["player_id"] for p in by_team[team]}


def test_candidate_narrowing_sums_weighted_minutes_across_multiple_matches_for_the_same_team(db_path, conn):
    """Two short appearances that individually wouldn't clear the floor, but
    together do -- the gate sums weighted minutes across every qualifying match
    for that team within the cutoff, not just a single match."""
    team, opp, m1 = _seed_match(conn, "Home", "OppA", date="2025-08-25")
    _, _, m2 = _seed_match(conn, "Home", "OppB", date="2025-09-01")
    two_cameos = sports_db.add_player(team, "Two Cameos", position="F", conn=conn)
    sports_db.add_player_match_stats(two_cameos, m1, season=2025, venue="home", minutes_played=6, conn=conn)
    sports_db.add_player_match_stats(two_cameos, m2, season=2025, venue="home", minutes_played=6, conn=conn)

    by_team = strength.load_team_players(conn, [team], "2025-09-02", window_size=10,
                                         half_life_days=1.0e12, cutoff_days=180.0)
    assert two_cameos in {p["player_id"] for p in by_team[team]}


def test_candidate_narrowing_gate_is_scoped_to_the_players_actual_current_team(db_path, conn):
    """The floor is checked against weighted minutes AT THE TEAM the player is
    actually attributed to (their single most recent appearance), not against
    minutes summed across every team they've recently touched -- a genuine thin
    (5-minute) debut at a new team shouldn't get "unlocked" by a big substantial
    history at the OLD team the player just left."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-08-01")
    team_b, opp_b, m2 = _seed_match(conn, "TeamB", "OppB", date="2025-09-01")   # more recent -- the transfer
    p1 = _transfer(conn, team_a, "Thin Debut", "ext_thin_debut")
    sports_db.add_player_match_stats(p1, m1, season=2025, venue="home",
                                     minutes_played=900, conn=conn)   # big history at OLD team
    _transfer(conn, team_b, "Thin Debut", "ext_thin_debut")           # transfer to team_b
    sports_db.add_player_match_stats(p1, m2, season=2025, venue="home",
                                     minutes_played=5, conn=conn)     # thin debut at NEW team

    by_team = strength.load_team_players(conn, [team_a, team_b], "2025-09-02", window_size=10,
                                         half_life_days=1.0e12, cutoff_days=180.0)
    # Most recent appearance was for team_b, so that's who they'd be attributed to
    # -- but 5 weighted minutes there doesn't clear the floor. NOT rescued by
    # team_a's big history either -- that's not their current team anymore.
    assert p1 not in {p["player_id"] for p in by_team[team_a]}
    assert p1 not in {p["player_id"] for p in by_team[team_b]}


def test_candidate_narrowing_handles_near_infinite_cutoff_without_overflow(db_path, conn):
    """cutoff_days can be an intentionally huge near-no-op sentinel (Stage 1
    back-compat / other tests' explicit near-no-op overrides, e.g. 1e12) --
    timedelta can't represent that many days, so the SQL-level date bound must be
    skipped in that regime rather than raising OverflowError."""
    team, opp, m1 = _seed_match(conn, "Home", "OppA", date="2020-01-01")
    veteran = sports_db.add_player(team, "Ancient History", position="F", conn=conn)
    sports_db.add_player_match_stats(veteran, m1, season=2020, venue="home",
                                     minutes_played=90, goals=1, conn=conn)

    by_team = strength.load_team_players(conn, [team], "2025-09-16", window_size=10,
                                         half_life_days=1.0e12, cutoff_days=1.0e12)
    assert veteran in {p["player_id"] for p in by_team[team]}


def test_window_size_limits_to_most_recent_n_appearances(db_path, conn):
    """A player with more appearances than the window covers only has their most
    recent `window_size` games count -- the oldest is dropped entirely, same
    convention as the team-level system's TEAM_PAST_MATCH_WINDOW_SIZE."""
    team, opp, m1 = _seed_match(conn, "Home", "OppA", date="2025-09-01")
    _, _, m2 = _seed_match(conn, "Home", "OppB", date="2025-09-08")
    _, _, m3 = _seed_match(conn, "Home", "OppC", date="2025-09-15")
    player = sports_db.add_player(team, "Striker", position="F", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2025, venue="home",
                                     minutes_played=90, goals=1, conn=conn)   # oldest -- dropped
    sports_db.add_player_match_stats(player, m2, season=2025, venue="home",
                                     minutes_played=90, goals=2, conn=conn)
    sports_db.add_player_match_stats(player, m3, season=2025, venue="home",
                                     minutes_played=90, goals=3, conn=conn)   # most recent

    by_team = strength.load_team_players(conn, [team], "2025-09-16", window_size=2,
                                         half_life_days=1.0e12, cutoff_days=1.0e12)
    p = by_team[team][0]
    assert p["attack_minutes"] == pytest.approx(180)          # only m2+m3, not m1's 90 too
    assert p["attack_rate"] == pytest.approx((2 + 3) / 180 * 90)


def test_calendar_decay_downweights_older_appearances_in_window(db_path, conn):
    """BUG-012 (2026-08-14): within the window, calendar_recency_weight(match_date,
    before_date) applies to both the goal/xg numerator and the minutes denominator
    -- an older match (more elapsed calendar days) gets pulled down, same shape as
    the old rank-based decay**rank this replaced, but driven by actual elapsed
    time rather than list position."""
    team, opp, m1 = _seed_match(conn, "Home", "OppA", date="2025-09-08")
    _, _, m2 = _seed_match(conn, "Home", "OppB", date="2025-09-15")   # more recent
    player = sports_db.add_player(team, "Striker", position="F", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2025, venue="home",
                                     minutes_played=90, goals=2, conn=conn)
    sports_db.add_player_match_stats(player, m2, season=2025, venue="home",
                                     minutes_played=90, goals=3, conn=conn)

    by_team = strength.load_team_players(conn, [team], "2025-09-16", window_size=2,
                                         half_life_days=7.0, cutoff_days=365.0)
    p = by_team[team][0]
    w_m2 = strength.calendar_recency_weight("2025-09-15", "2025-09-16",
                                            half_life_days=7.0, cutoff_days=365.0)
    w_m1 = strength.calendar_recency_weight("2025-09-08", "2025-09-16",
                                            half_life_days=7.0, cutoff_days=365.0)
    expected_num = w_m2 * 3 + w_m1 * 2
    expected_den = w_m2 * 90 + w_m1 * 90
    assert p["attack_minutes"] == pytest.approx(expected_den)
    assert p["attack_rate"] == pytest.approx(expected_num / expected_den * 90)
    # And the more recent game's weight really is larger -- the actual behavior
    # being tested, not just an arithmetic identity against the same formula.
    assert w_m2 > w_m1


def test_season_blind_reaches_across_a_season_boundary(db_path, conn):
    """The window has no concept of a season edge -- a game from LAST season counts
    toward filling it exactly like an early-this-season game would, with no separate
    discount layered on. Proven by a stark before/after: a player with 0 goals this
    season so far gets a nonzero rate once last season's real form (10 goals/900 min)
    is reachable within the window, purely because the window is big enough to
    include it."""
    prior_team, _, m_prior = _seed_match_league(conn, "Serie A", "TeamA", "OppX", 2024, "2024-09-01")
    player = sports_db.add_player(prior_team, "Striker", position="F", conn=conn)
    sports_db.add_player_match_stats(player, m_prior, season=2024, venue="home",
                                     minutes_played=900, goals=10, conn=conn)

    cur_team, _, m_cur = _seed_match_league(conn, "Serie A", "TeamA", "OppY", 2025, "2025-09-01")
    sports_db.add_player_match_stats(player, m_cur, season=2025, venue="home",
                                     minutes_played=90, goals=0, conn=conn)

    season_blind = strength.load_team_players(conn, [cur_team], "2025-09-02", window_size=10,
                                               half_life_days=1.0e12, cutoff_days=1.0e12)
    assert season_blind[cur_team][0]["attack_minutes"] == pytest.approx(990)   # both games
    assert season_blind[cur_team][0]["attack_rate"] == pytest.approx(10 / 990 * 90)
    assert season_blind[cur_team][0]["attack_rate"] > 0.0


def test_min_date_reproduces_a_season_scoped_window_for_comparison(db_path, conn):
    """min_date is the A/B-comparison knob (MODEL_TUNING_PARAMETERS.md) -- passing
    the current season's start date stops the window from reaching into last
    season at all, reproducing the OLD season-scoped behavior on demand. Same setup
    as the season-blind test above, opposite conclusion."""
    prior_team, _, m_prior = _seed_match_league(conn, "Serie A", "TeamA", "OppX", 2024, "2024-09-01")
    player = sports_db.add_player(prior_team, "Striker", position="F", conn=conn)
    sports_db.add_player_match_stats(player, m_prior, season=2024, venue="home",
                                     minutes_played=900, goals=10, conn=conn)

    cur_team, _, m_cur = _seed_match_league(conn, "Serie A", "TeamA", "OppY", 2025, "2025-09-01")
    sports_db.add_player_match_stats(player, m_cur, season=2025, venue="home",
                                     minutes_played=90, goals=0, conn=conn)

    season_scoped = strength.load_team_players(conn, [cur_team], "2025-09-02",
                                               window_size=10, min_date="2025-08-01",
                                               half_life_days=1.0e12, cutoff_days=1.0e12)
    assert season_scoped[cur_team][0]["attack_minutes"] == pytest.approx(90)   # only m_cur
    assert season_scoped[cur_team][0]["attack_rate"] == pytest.approx(0.0)


def test_uncalibrated_league_game_excluded_from_both_attack_and_defense(db_path, conn):
    """Cross-league adjustment (PLAYER_RATING_CROSS_LEAGUE_GOAL_ADJUSTMENT) gates
    BOTH sides symmetrically (fixed 2026-08-12, BUG-010 continued -- used to be
    attack-only, so a game in an uncalibrated league still counted fully toward
    defense, unadjusted, which is exactly what let a promoted team's defense
    rating be built almost entirely from unadjusted feeder-league form; see Real
    Oviedo hosting Real Madrid). A game in a league with no calibration factor is
    dropped entirely -- goals, minutes, AND club_ga_per90 -- never assumed
    Serie-A-equivalent on either side. Same team across a league change
    (ensure_soccer_team dedupes by name), mirroring how a real team's league can
    change season to season."""
    team, _, m1 = _seed_match_league(conn, "Serie A", "SameTeam", "OppA", 2025, "2025-09-01")
    _, _, m2 = _seed_match_league(conn, "Serie C", "SameTeam", "OppC", 2025, "2025-09-08")
    player = sports_db.add_player(team, "Striker", position="F", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2025, venue="home",
                                     minutes_played=90, goals=1, club_ga_per90=2.0, conn=conn)
    sports_db.add_player_match_stats(player, m2, season=2025, venue="home",
                                     minutes_played=90, goals=5, club_ga_per90=3.0, conn=conn)

    by_team = strength.load_team_players(conn, [team], "2025-09-09",
                                         league_strength={"Serie A": 1.0},
                                         half_life_days=1.0e12, cutoff_days=1.0e12)
    p = by_team[team][0]
    assert p["attack_minutes"] == pytest.approx(90)             # only the Serie A game
    assert p["attack_rate"] == pytest.approx(1.0)                # only the Serie A goal
    assert p["defense_minutes"] == pytest.approx(90)             # only the Serie A game too now
    assert p["club_ga_per90"] == pytest.approx(2.0)               # NOT the Serie C game's 3.0


def test_calibrated_league_defense_is_unscaled_by_the_attack_factor(db_path, conn):
    """The gate now applies to defense, but the numeric league factor still does
    NOT scale defense -- Serie B's real 0.663 was measured from players' own
    goal-scoring rate specifically and has no established defensive meaning
    (2026-08-12). A game in a league that DOES have a (non-1.0) calibration
    entry contributes its club_ga_per90 UNSCALED, not multiplied by that factor."""
    team, _, m1 = _seed_match_league(conn, "Serie B", "SameTeam", "OppA", 2025, "2025-09-01")
    player = sports_db.add_player(team, "Defender", position="D", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2025, venue="home",
                                     minutes_played=90, goals=0, club_ga_per90=2.0, conn=conn)

    by_team = strength.load_team_players(conn, [team], "2025-09-08",
                                         league_strength={"Serie B": 0.663},
                                         half_life_days=1.0e12, cutoff_days=1.0e12)
    p = by_team[team][0]
    assert p["defense_minutes"] == pytest.approx(90)
    assert p["club_ga_per90"] == pytest.approx(2.0)   # NOT 2.0 * 0.663


def test_every_registered_feeder_division_has_a_cross_league_adjustment_entry():
    """BUG-013 (2026-08-14): a league with no PLAYER_RATING_CROSS_LEAGUE_GOAL_
    ADJUSTMENT entry has its games EXCLUDED ENTIRELY from a player's rating (see
    load_team_players' docstring/gate) -- not scaled, not discounted, just
    dropped. When 2. Bundesliga/Championship/LaLiga 2/Ligue 2 were added to
    core.leagues.LEAGUES as feeder divisions (multi-league expansion) they were
    never added here, so a promoted team's real recent history was silently
    thrown out -- found via 1. FC Koeln (promoted from 2. Bundesliga) having its
    player-level attack rating collapse to a single 90-minute, scoreless sample.
    This generic check (every league.lower_division in LEAGUES must have an
    entry here) would have caught that gap immediately, and catches it again for
    any future league added the same way -- not hardcoded to today's specific 4
    divisions."""
    missing = [
        league for league, cfg in LEAGUES.items()
        if cfg["lower_division"] is not None
        and cfg["lower_division"] not in strength.PLAYER_RATING_CROSS_LEAGUE_GOAL_ADJUSTMENT
    ]
    assert missing == [], (
        f"These leagues' feeder divisions have no PLAYER_RATING_CROSS_LEAGUE_"
        f"GOAL_ADJUSTMENT entry, so that promotion history is silently excluded: "
        f"{[LEAGUES[league]['lower_division'] for league in missing]}"
    )


def test_apply_shrinkage_pulls_low_minutes_player_toward_position_prior(db_path, conn):
    """Basic shrinkage sanity check with the new field names (attack_minutes/
    defense_minutes, both always populated by load_team_players now) -- a low-minutes
    player shrinks hard toward the shared positional prior, a high-minutes player
    barely moves off their own rate."""
    team, opp, m1 = _seed_match(conn, "Home", "OppA", date="2025-09-01")
    p_low = sports_db.add_player(team, "LowMinutes", position="F", conn=conn)
    sports_db.add_player_match_stats(p_low, m1, season=2025, venue="home",
                                     minutes_played=90, goals=0, conn=conn)     # rate 0.0, thin sample

    _, _, m2 = _seed_match(conn, "Home", "OppB", date="2025-09-08")
    p_high = sports_db.add_player(team, "HighMinutes", position="F", conn=conn)
    sports_db.add_player_match_stats(p_high, m2, season=2025, venue="home",
                                     minutes_played=9000, goals=200, conn=conn)  # rate 2.0, thick sample

    by_team = strength.load_team_players(conn, [team], "2025-09-09",
                                         half_life_days=1.0e12, cutoff_days=1.0e12)
    prior = strength.positional_priors(by_team, "attack_rate", weight_field="attack_minutes")["FWD"]
    assert prior == pytest.approx((90 * 0.0 + 9000 * 2.0) / (90 + 9000))

    strength.apply_shrinkage(by_team)
    rates = {p["player_id"]: p["attack_rate"] for p in by_team[team]}
    k = PLAYER_RATING_MINUTES_TO_HALF_TRUST_OWN_RATE_OVER_LEAGUE_AVERAGE
    expected_low = (90 * 0.0 + k * prior) / (90 + k)
    expected_high = (9000 * 2.0 + k * prior) / (9000 + k)
    assert rates[p_low] == pytest.approx(expected_low)
    assert rates[p_high] == pytest.approx(expected_high)
    # The point: the thin sample gets pulled most of the way to the prior, the thick
    # sample barely moves off its own 2.0.
    assert rates[p_low] > 1.0          # pulled well up from its own raw 0.0
    assert rates[p_high] == pytest.approx(2.0, abs=0.05)


# ── compute(): attack vs. defense recentering symmetry (BUG-009, found 2026-08-05,
#    fixed 2026-08-09) ────────────────────────────────────────────────────────────
#
# compute() rescales each team's raw player-level attack/defense number onto the
# league-average scale before blending it with the team-level number. Until
# 2026-08-09, the two components did this two DIFFERENT ways:
#   attack:  la_player_home = avg_home + (raw_attack - attack_mean)        additive
#   defense: ld_player_home = raw_defense * (avg_away / defense_mean)      multiplicative
# This asymmetry was found by hand while investigating spread compression, not by a
# test -- these tests turned it into an explicit, executable contract so the fix (make
# attack multiplicative too, matching defense and the team-level system's own
# `lambda_H = h_att * (a_def / avg_h)` convention -- also fixes the additive form's
# risk of driving lambda negative for a well-below-average team) was caught by a
# normal test run instead of only found by manual inspection.
#
# Fixture shape: two teams (TeamA, TeamB), one MID player each (an arbitrary non-zero-
# weight position -- with exactly one player, raw_team_strength's weighted average
# trivially reduces to that player's own shrunk rate, whatever the position weight
# is). Each player's xg and club_ga_per90 are set to the SAME number as each other on
# purpose (a "mirrored" fixture) -- since apply_shrinkage's positional-prior smoothing
# is a pure function of (value, weight) pairs, mirroring the raw inputs makes the
# post-shrinkage attack rate and defense rate come out identical too, without needing
# to reimplement the shrinkage formula by hand in the test. Both team's numbers are
# still worked out explicitly in the docstrings/comments below so failures are
# legible.

def _seed_mirrored_team(conn, name, season, date, minutes, rate):
    """One MID player whose xg-derived attack_rate and club_ga_per90 both equal
    `rate` -- see module comment above."""
    team, opp, match_id = _seed_match(conn, name, f"{name}Opp", season=season, date=date)
    player = sports_db.add_player(team, f"{name}Player", position="M", conn=conn)
    xg = rate * minutes / 90.0
    sports_db.add_player_match_stats(player, match_id, season=season, venue="home",
                                     minutes_played=minutes, xg=xg, club_ga_per90=rate, conn=conn)
    return team


def test_defense_recentering_scales_by_ratio_to_league_average(db_path, conn):
    """Documents TODAY's actual defense formula: ld_player_home = raw_defense *
    (avg_away / defense_mean) -- a multiplicative, proportional rescale. This is the
    reference/presumed-correct convention: it matches the team-level system's own
    `lambda_H = h_att * (a_def / avg_h)` shape (core.poisson_model.estimate_lambdas).

    No historical scored matches exist in this fixture, so get_league_averages falls
    back to its documented defaults (avg_home=1.3, avg_away=1.1) -- known, fixed
    numbers we can compute the expected result from directly, rather than needing to
    seed a separate history just to pin the baseline."""
    team_a = _seed_mirrored_team(conn, "TeamA", 2025, "2025-09-01", minutes=1200, rate=1.5)
    team_b = _seed_mirrored_team(conn, "TeamB", 2025, "2025-09-02", minutes=1200, rate=1.0)

    results = strength.compute(conn, [team_a, team_b], "Serie A", 2025, "2025-09-03",
                               player_recency_half_life_days=1.0e12, player_recency_cutoff_days=1.0e12)
    r = results[team_a]
    assert r["avg_home"] == pytest.approx(1.3)
    assert r["avg_away"] == pytest.approx(1.1)

    # Worked by hand from apply_shrinkage's formula (see module comment): with
    # mirrored inputs, TeamA's shrunk defense rate = (1200*1.5 + 900*1.25) / 2100,
    # where 1.25 is the minutes-weighted MID prior across both teams' mirrored values.
    prior = (1200 * 1.5 + 1200 * 1.0) / (1200 + 1200)
    assert prior == pytest.approx(1.25)
    rd_a = (1200 * 1.5 + PLAYER_RATING_MINUTES_TO_HALF_TRUST_OWN_RATE_OVER_LEAGUE_AVERAGE * prior) / (1200 + PLAYER_RATING_MINUTES_TO_HALF_TRUST_OWN_RATE_OVER_LEAGUE_AVERAGE)
    rd_b = (1200 * 1.0 + PLAYER_RATING_MINUTES_TO_HALF_TRUST_OWN_RATE_OVER_LEAGUE_AVERAGE * prior) / (1200 + PLAYER_RATING_MINUTES_TO_HALF_TRUST_OWN_RATE_OVER_LEAGUE_AVERAGE)
    defense_mean = (rd_a + rd_b) / 2

    expected_ld_home = rd_a * (r["avg_away"] / defense_mean)
    assert r["lambda_defense_player_home"] == pytest.approx(expected_ld_home)


def test_attack_recentering_scales_by_ratio_to_league_average(db_path, conn):
    """THE CONTRACT: attack and defense are structurally the same kind of quantity (a
    team's raw player-derived rate, rescaled onto the league-average baseline before
    blending with the team-level number) and are rescaled the SAME way -- defense does
    this by ratio (previous test); this asserts attack does too.

    Fixed 2026-08-09 (BUG-009) -- attack was previously a flat additive shift
    (`avg_home + (raw_attack - attack_mean)`), an unexplained asymmetry with defense's
    own multiplicative form found by hand, not by a test. Ad hoc testing (2026-08-05
    entry, BUGS.md) showed multiplicative uniformly improves calibration (compression
    bucket table, every bucket) with a season-inconsistent ROI signal too noisy at a
    2-season sample size to weigh against that -- shipped anyway since the additive
    form has no principled justification (it can even drive lambda negative for a
    well-below-average team, which a rate parameter must never be) and multiplicative
    matches the team-level system's own convention throughout
    (`lambda_H = h_att * (a_def / avg_h)`)."""
    team_a = _seed_mirrored_team(conn, "TeamA", 2025, "2025-09-01", minutes=1200, rate=1.5)
    team_b = _seed_mirrored_team(conn, "TeamB", 2025, "2025-09-02", minutes=1200, rate=1.0)

    results = strength.compute(conn, [team_a, team_b], "Serie A", 2025, "2025-09-03",
                               player_spread_stretch_attack=1.0, player_spread_stretch_defense=1.0,
                               player_recency_half_life_days=1.0e12, player_recency_cutoff_days=1.0e12)
    r = results[team_a]

    prior = (1200 * 1.5 + 1200 * 1.0) / (1200 + 1200)
    ra_a = (1200 * 1.5 + PLAYER_RATING_MINUTES_TO_HALF_TRUST_OWN_RATE_OVER_LEAGUE_AVERAGE * prior) / (1200 + PLAYER_RATING_MINUTES_TO_HALF_TRUST_OWN_RATE_OVER_LEAGUE_AVERAGE)
    ra_b = (1200 * 1.0 + PLAYER_RATING_MINUTES_TO_HALF_TRUST_OWN_RATE_OVER_LEAGUE_AVERAGE * prior) / (1200 + PLAYER_RATING_MINUTES_TO_HALF_TRUST_OWN_RATE_OVER_LEAGUE_AVERAGE)
    attack_mean = (ra_a + ra_b) / 2

    expected_la_home_multiplicative = ra_a * (r["avg_home"] / attack_mean)
    assert r["lambda_attack_player_home"] == pytest.approx(expected_la_home_multiplicative)
