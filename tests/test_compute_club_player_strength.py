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

    by_team = strength.load_team_players(conn, [team], "2026-01-01")
    assert len(by_team[team]) == 1
    assert by_team[team][0]["attack_rate"] == pytest.approx(1.0)
    assert by_team[team][0]["attack_rate"] != pytest.approx(4.5)
    assert by_team[team][0]["attack_minutes"] == 90


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

    by_team = strength.load_team_players(conn, [team], "2026-01-01")
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

    by_team = strength.load_team_players(conn, [team], "2026-01-01")
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

    by_team = strength.load_team_players(raw, [10], "2026-01-01")
    assert len(by_team[10]) == 1
    assert by_team[10][0]["attack_minutes"] == 90   # NOT 90 + 2500
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

    by_team = strength.load_team_players(conn, [team_a, team_b], "2026-06-01")
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

    minutes = strength.team_aggregated_recent_roster_minutes(conn, team_a, "2025-01-01")
    assert minutes[player] == 900


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

    assert strength.team_aggregated_recent_roster_minutes(conn, team_a, "2026-01-01") == {player: 400}
    assert strength.team_aggregated_recent_roster_minutes(conn, team_b, "2026-01-01") == {player: 600}
    assert strength.team_aggregated_recent_roster_minutes(conn, opp_a, "2026-01-01") == {opp_a_player: 90}


def test_team_aggregated_recent_roster_minutes_before_date_excludes_later_matches(db_path, conn):
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    _, _, m2 = _seed_match(conn, "TeamA", "OppB", date="2025-11-01")
    player = sports_db.add_player(team_a, "Timeline Player", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2025, venue="home", minutes_played=90, conn=conn)
    sports_db.add_player_match_stats(player, m2, season=2025, venue="home", minutes_played=90, conn=conn)

    early = strength.team_aggregated_recent_roster_minutes(conn, team_a, "2025-10-01")
    full = strength.team_aggregated_recent_roster_minutes(conn, team_a, "2026-01-01")
    assert early[player] == 90    # only m1
    assert full[player] == 180    # both


def test_team_aggregated_recent_roster_minutes_limits_to_last_n_matches(db_path, conn):
    """A match beyond the window is dropped entirely, same convention as every
    other N-game window in this file (e.g. load_team_players' window_size)."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    _, _, m2 = _seed_match(conn, "TeamA", "OppB", date="2025-09-08")
    player = sports_db.add_player(team_a, "Bench Player", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2025, venue="home", minutes_played=90, conn=conn)
    sports_db.add_player_match_stats(player, m2, season=2025, venue="home", minutes_played=90, conn=conn)

    windowed = strength.team_aggregated_recent_roster_minutes(conn, team_a, "2026-01-01", n=1)
    assert windowed[player] == 90   # only m2, the more recent of the two


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

    minutes = strength.players_aggregated_recent_minutes(conn, {player}, "2026-01-01")
    assert minutes[player] == 1000


def test_players_recent_minutes_only_computes_requested_players(db_path, conn):
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    p1 = sports_db.add_player(team_a, "Requested", conn=conn)
    p2 = sports_db.add_player(team_a, "Not Requested", conn=conn)
    sports_db.add_player_match_stats(p1, m1, season=2025, venue="home", minutes_played=90, conn=conn)
    sports_db.add_player_match_stats(p2, m1, season=2025, venue="home", minutes_played=90, conn=conn)

    minutes = strength.players_aggregated_recent_minutes(conn, {p1}, "2026-01-01")
    assert minutes == {p1: 90}


def test_players_recent_minutes_limits_to_last_n_appearances_per_player(db_path, conn):
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", date="2025-09-01")
    _, _, m2 = _seed_match(conn, "TeamA", "OppB", date="2025-09-08")
    player = sports_db.add_player(team_a, "Frequent Flyer", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2025, venue="home", minutes_played=90, conn=conn)
    sports_db.add_player_match_stats(player, m2, season=2025, venue="home", minutes_played=90, conn=conn)

    windowed = strength.players_aggregated_recent_minutes(conn, {player}, "2026-01-01", n=1)
    assert windowed[player] == 90   # only the more recent appearance


def test_players_recent_minutes_empty_for_no_player_ids(db_path, conn):
    assert strength.players_aggregated_recent_minutes(conn, set(), "2026-01-01") == {}


def test_player_trust_high_when_full_coverage_and_full_churn(db_path, conn):
    """The headline case this whole mechanism exists for: the prior-roster window
    left entirely, replaced by well-tracked players (>=300 min in their own last
    window elsewhere) -- the prior-roster reference describes a squad that's gone,
    and we have real signal on the new one. Both factors strong -> trust close to
    1.0. window=1 keeps the synthetic setup small: team_a's prior window is its
    single 2025-09-01 match (P1/P2); the later 2025-10-15 match only exists to
    push the CURRENT 1-match window past it, so the two windows the comparison
    uses are genuinely adjacent and non-overlapping (BUG-010, 2026-08-12 -- see
    team_prior_window_cutoff_date)."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2025, date="2025-09-01")
    team_b, opp_b, m2 = _seed_match(conn, "TeamB", "OppB", season=2025, date="2025-10-01")
    away_team = sports_db.ensure_soccer_team("MovedOn", "Serie A")
    _seed_match(conn, "TeamA", "OppC", season=2025, date="2025-10-15")   # boundary only

    p1 = _transfer(conn, team_a, "P1", "ext_p1")
    p2 = _transfer(conn, team_a, "P2", "ext_p2")
    sports_db.add_player_match_stats(p1, m1, season=2025, venue="home", minutes_played=900, conn=conn)
    sports_db.add_player_match_stats(p2, m1, season=2025, venue="home", minutes_played=900, conn=conn)

    p3 = _transfer(conn, team_b, "P3", "ext_p3")
    p4 = _transfer(conn, team_b, "P4", "ext_p4")
    sports_db.add_player_match_stats(p3, m2, season=2025, venue="home", minutes_played=1000, conn=conn)
    sports_db.add_player_match_stats(p4, m2, season=2025, venue="home", minutes_played=1000, conn=conn)

    # Departures: P1/P2 leave TeamA. Arrivals: P3/P4 join TeamA (from TeamB).
    _transfer(conn, away_team, "P1", "ext_p1")
    _transfer(conn, away_team, "P2", "ext_p2")
    _transfer(conn, team_a, "P3", "ext_p3")
    _transfer(conn, team_a, "P4", "ext_p4")

    trust = strength.player_trust_score(conn, team_a, "2026-09-14", window=1)
    assert trust == pytest.approx(1.0)
    assert strength.resolve_blend_weight(
        conn, team_a, "Serie A", "attack", "2026-09-14", window=1) == pytest.approx(0.0)


def test_player_trust_low_when_roster_is_stable_despite_full_coverage(db_path, conn):
    """Same players, same team -- the prior-roster reference still describes THIS
    squad, so there's nothing to gain from the player signal even though the data
    coverage is excellent. Guards the AND (product), not OR/average."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2025, date="2025-09-01")
    _seed_match(conn, "TeamA", "OppC", season=2025, date="2025-10-15")   # boundary only
    p1 = sports_db.add_player(team_a, "Stalwart One", api_player_id="ext_s1", conn=conn)
    p2 = sports_db.add_player(team_a, "Stalwart Two", api_player_id="ext_s2", conn=conn)
    sports_db.add_player_match_stats(p1, m1, season=2025, venue="home", minutes_played=900, conn=conn)
    sports_db.add_player_match_stats(p2, m1, season=2025, venue="home", minutes_played=900, conn=conn)
    # No roster changes -- p1/p2 remain on team_a (current roster == prior roster).

    trust = strength.player_trust_score(conn, team_a, "2026-09-14", window=1)
    assert trust == pytest.approx(0.0)
    assert strength.resolve_blend_weight(
        conn, team_a, "Serie A", "defense", "2026-09-14", window=1) == pytest.approx(1.0)


def test_player_trust_low_when_churn_is_high_but_new_players_are_unproven(db_path, conn):
    """The edge case flagged in FEATURE-011_REQUIREMENTS.md: heavy churn INTO players
    with no usable recent-window track record. The prior-roster reference is stale
    (squad mostly gone) AND we don't know the new squad either -- falls back to
    team-level as the least-bad option, not because it's trusted."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2025, date="2025-09-01")
    _seed_match(conn, "TeamA", "OppC", season=2025, date="2025-10-15")   # boundary only
    away_team = sports_db.ensure_soccer_team("MovedOn2", "Serie A")

    p1 = _transfer(conn, team_a, "P1b", "ext_p1b")
    p2 = _transfer(conn, team_a, "P2b", "ext_p2b")
    sports_db.add_player_match_stats(p1, m1, season=2025, venue="home", minutes_played=900, conn=conn)
    sports_db.add_player_match_stats(p2, m1, season=2025, venue="home", minutes_played=900, conn=conn)
    _transfer(conn, away_team, "P1b", "ext_p1b")
    _transfer(conn, away_team, "P2b", "ext_p2b")

    # Newcomers have NO tracked minutes anywhere in their own recent window (debutants/reserves).
    sports_db.add_player(team_a, "Rookie One", conn=conn)
    sports_db.add_player(team_a, "Rookie Two", conn=conn)

    trust = strength.player_trust_score(conn, team_a, "2026-09-14", window=1)
    assert trust == pytest.approx(0.0)


def test_prior_window_shifts_forward_as_more_matches_are_played(db_path, conn):
    """The exact bug found live 2026-08-12, after the season-blind fix above
    landed: that fix anchored the "prior roster" reference to the SEASON's own
    start date, computed once and reused for every matchday for the rest of the
    season -- so a squad overhaul stayed flagged as "brand new" all season long,
    even once the team-level rating itself was built entirely from real games
    with the current roster (found live: Burnley hosting Manchester City,
    2026-04-22, ~8 months after Burnley's summer signings, trust was still
    ~0.98-1.0 purely from stale summer-transfer churn). BUG-010's continuation:
    the reference must be TWO ADJACENT windows, both anchored to before_date
    (team_prior_window_cutoff_date), so the comparison itself shifts forward
    every matchday.

    Same team, same eventual roster (new_player), checked at two points in time:
    right after the churn (still flagged, high trust) and two matchdays later
    once BOTH adjacent windows sit entirely within the new era (no longer
    flagged -- team-level is trusted again, correctly, since it now reflects
    real games with this exact roster)."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2025, date="2025-09-01")
    _, _, m2 = _seed_match(conn, "TeamA", "OppB", season=2025, date="2025-09-08")
    _, _, m3 = _seed_match(conn, "TeamA", "OppC", season=2025, date="2025-09-15")
    _, _, m4 = _seed_match(conn, "TeamA", "OppD", season=2025, date="2025-09-22")

    old_player = sports_db.add_player(team_a, "Old Guard", conn=conn)
    new_player = sports_db.add_player(team_a, "New Signing", conn=conn)
    sports_db.add_player_match_stats(old_player, m1, season=2025, venue="home", minutes_played=900, conn=conn)
    for m in (m2, m3, m4):
        sports_db.add_player_match_stats(new_player, m, season=2025, venue="home", minutes_played=900, conn=conn)

    # Right after the churn: current (last 1 match) window is m2 (new signing's
    # debut), prior window is m1 (old guard) -- genuinely different rosters.
    just_after = strength.player_trust_score(
        conn, team_a, "2025-09-09", current_roster_ids={new_player}, window=1)
    assert just_after == pytest.approx(1.0)   # full coverage, full churn

    # Two matchdays later: current window is m4, prior window is m3 -- BOTH
    # already inside the new era, so the SAME roster comparison now reports zero
    # churn. This is the behavior the season_start_date anchor could never produce.
    later = strength.player_trust_score(
        conn, team_a, "2025-09-23", current_roster_ids={new_player}, window=1)
    assert later == pytest.approx(0.0)


def test_player_trust_zero_when_no_recent_window_history(db_path, conn):
    """No recent-window data for the team at all (e.g. backfill not run far back
    enough yet -- the exact BUG-010 scenario: a league's first-ever tracked season)
    -> falls back fully to team-level, not a ZeroDivisionError."""
    team_a = sports_db.ensure_soccer_team("Brand New Import", "Serie A")
    trust = strength.player_trust_score(conn, team_a, "2026-09-14")
    assert trust == 0.0
    assert strength.resolve_blend_weight(conn, team_a, "Serie A", "attack", "2026-09-14") == 1.0


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
    through, not just accepted and ignored. Same setup as the full-churn headline
    test, but passing an explicit (different) squad instead of the live default."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2025, date="2025-09-01")
    _seed_match(conn, "TeamA", "OppE", season=2025, date="2025-10-15")   # boundary only
    p1 = sports_db.add_player(team_a, "Stayed Player", api_player_id="ext_stay", conn=conn)
    sports_db.add_player_match_stats(p1, m1, season=2025, venue="home", minutes_played=900, conn=conn)
    # Live default: current_roster_player_ids(team_a) == {p1} -- roster unchanged, trust 0.
    assert strength.player_trust_score(conn, team_a, "2026-09-14", window=1) == pytest.approx(0.0)

    # Override with a squad that looks completely different from the prior-roster reference.
    p2 = sports_db.add_player(team_a, "Hypothetical New Player", api_player_id="ext_hyp", conn=conn)
    team_b, opp_b, m2 = _seed_match(conn, "TeamB", "OppC", season=2025, date="2025-09-08")
    sports_db.add_player_match_stats(p2, m2, season=2025, venue="home", minutes_played=1000, conn=conn)
    overridden_trust = strength.player_trust_score(
        conn, team_a, "2026-09-14", current_roster_ids={p2}, window=1)
    assert overridden_trust == pytest.approx(1.0)


def test_player_trust_score_cache_avoids_recomputing_recent_window_aggregates(db_path, conn, monkeypatch):
    """BUG-011's caching still applies (2026-08-12 matchday-shifting rework changed
    WHAT the reference window is keyed by, not whether caching helps):
    resolve_blend_weight calls player_trust_score once per component (attack,
    defense) with IDENTICAL inputs for a given team/before_date, so a shared cache
    dict should still hit the underlying SQL aggregates once each across repeated
    calls at the SAME inputs."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2025, date="2025-09-01")
    _seed_match(conn, "TeamA", "OppE", season=2025, date="2025-10-15")   # boundary only
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
    results = [strength.player_trust_score(conn, team_a, "2026-09-14", cache=cache, window=1)
              for _ in range(3)]

    assert calls["team_aggregated_recent_roster_minutes"] == 1
    assert calls["players_aggregated_recent_minutes"] == 1
    assert results[0] == results[1] == results[2]
    assert results[0] == pytest.approx(strength.player_trust_score(conn, team_a, "2026-09-14", window=1))


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

    early = strength.load_team_players(conn, [team_a], "2025-10-01")
    full = strength.load_team_players(conn, [team_a], "2026-01-01")
    assert early[team_a][0]["attack_minutes"] == 90     # only m1
    assert full[team_a][0]["attack_minutes"] == 180     # both


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
    moves the raw xG rating: stretched = league_mean + (raw - league_mean) * factor.
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
    assert result[0] == pytest.approx(1.0 + (2.0 - 1.0) * 1.3)  # 2.3


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
    assert home_attack == pytest.approx(1.0 + (2.0 - 1.0) * 2.0)   # stretched
    assert home_defense == pytest.approx(2.0)                       # untouched (raw, factor=1.0)


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
    assert stretched[team_a]["lambda_attack_team_home"] == pytest.approx(league_mean + (2.0 - league_mean) * 1.3)
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
    sports_db.add_player_match_stats(pa, ma1, season=2025, venue="home", minutes_played=300, goals=2, conn=conn)
    sports_db.add_player_match_stats(pb, mb1, season=2025, venue="home", minutes_played=300, goals=0, conn=conn)

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
    sports_db.add_player_match_stats(pa, ma1, season=2025, venue="home", minutes_played=300, goals=2, conn=conn)
    sports_db.add_player_match_stats(pb, mb1, season=2025, venue="home", minutes_played=300, goals=0, conn=conn)

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
    sports_db.add_player_match_stats(pa, ma1, season=2025, venue="home", minutes_played=300, goals=2, conn=conn)
    sports_db.add_player_match_stats(pb, mb1, season=2025, venue="home", minutes_played=300, goals=0, conn=conn)

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
    sports_db.add_player_match_stats(pa, ma1, season=2025, venue="home", minutes_played=300,
                                     goals=2, club_ga_per90=2.0, conn=conn)
    sports_db.add_player_match_stats(pb, mb1, season=2025, venue="home", minutes_played=300,
                                     goals=0, club_ga_per90=0.5, conn=conn)

    baseline = strength.compute(conn, [team_a, team_b], "Serie A", 2025, "2025-09-02",
                                xg_spread_stretch_attack=1.0, xg_spread_stretch_defense=1.0,
                                player_spread_stretch_attack=1.0, player_spread_stretch_defense=1.0)
    attack_only = strength.compute(conn, [team_a, team_b], "Serie A", 2025, "2025-09-02",
                                   xg_spread_stretch_attack=1.0, xg_spread_stretch_defense=1.0,
                                   player_spread_stretch_attack=2.0, player_spread_stretch_defense=1.0)

    assert attack_only[team_a]["lambda_attack_player_home"] != pytest.approx(baseline[team_a]["lambda_attack_player_home"])
    assert attack_only[team_a]["lambda_defense_player_home"] == pytest.approx(baseline[team_a]["lambda_defense_player_home"])


# ── load_team_players: rolling window (FEATURE-011 Follow-up B, 2026-08-06) ──────
# Replaces the old flat season-to-date sum, and the separate blend_prior_season_
# attack/PRIOR_SEASON_DISCOUNT mechanism retired the same day (see MODEL_TUNING_
# PARAMETERS.md and BUGS.md's FEATURE-011 entry for the design discussion).

def _seed_match_league(conn, league, home_name, away_name, season, date):
    home = sports_db.ensure_soccer_team(home_name, league)
    away = sports_db.ensure_soccer_team(away_name, league)
    match_id = sports_db.add_soccer_match(league, season, home, away, date)
    return home, away, match_id


def test_candidate_narrowing_excludes_a_player_absent_from_the_teams_own_recent_matches(db_path, conn):
    """2026-08-11 (performance + correctness, found validating BUG-010): a player
    whose most recent appearance for `team` is still their global most-recent
    appearance overall, but falls OUTSIDE team's own last `window_size` matches
    (the team has played on without them since), must be excluded entirely -- not
    just have a stale rating. They're not plausibly part of what the team's doing
    now, e.g. a long-term injury, so their old stats shouldn't dilute the rating."""
    team, opp, m1 = _seed_match(conn, "Home", "OppA", date="2025-09-01")   # player's only match
    _, _, m2 = _seed_match(conn, "Home", "OppB", date="2025-09-08")
    _, _, m3 = _seed_match(conn, "Home", "OppC", date="2025-09-15")        # most recent
    injured = sports_db.add_player(team, "Long-Term Injury", position="F", conn=conn)
    sports_db.add_player_match_stats(injured, m1, season=2025, venue="home",
                                     minutes_played=90, goals=1, conn=conn)
    # No stats for the injured player in m2/m3 -- team played on without them.

    # window_size=2: team's own "recent" matches are m2+m3, NOT m1.
    by_team = strength.load_team_players(conn, [team], "2025-09-16", window_size=2, decay=1.0)
    assert injured not in {p["player_id"] for p in by_team[team]}


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

    by_team = strength.load_team_players(conn, [team], "2025-09-16", window_size=2, decay=1.0)
    p = by_team[team][0]
    assert p["attack_minutes"] == pytest.approx(180)          # only m2+m3, not m1's 90 too
    assert p["attack_rate"] == pytest.approx((2 + 3) / 180 * 90)


def test_decay_downweights_older_appearances_in_window(db_path, conn):
    """Within the window, decay**rank (rank 0 = most recent) applies to both the
    goal/xg numerator and the minutes denominator -- decay < 1.0 pulls the rate
    toward the more recent game, not just a flat average of what's in the window."""
    team, opp, m1 = _seed_match(conn, "Home", "OppA", date="2025-09-08")
    _, _, m2 = _seed_match(conn, "Home", "OppB", date="2025-09-15")   # more recent
    player = sports_db.add_player(team, "Striker", position="F", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2025, venue="home",
                                     minutes_played=90, goals=2, conn=conn)
    sports_db.add_player_match_stats(player, m2, season=2025, venue="home",
                                     minutes_played=90, goals=3, conn=conn)

    by_team = strength.load_team_players(conn, [team], "2025-09-16", window_size=2, decay=0.5)
    p = by_team[team][0]
    # rank0 (m2, goals=3) weight 1.0, rank1 (m1, goals=2) weight 0.5.
    expected_num = 1.0 * 3 + 0.5 * 2
    expected_den = 1.0 * 90 + 0.5 * 90
    assert p["attack_minutes"] == pytest.approx(expected_den)
    assert p["attack_rate"] == pytest.approx(expected_num / expected_den * 90)


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

    season_blind = strength.load_team_players(conn, [cur_team], "2025-09-02", window_size=10)
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
                                               window_size=10, min_date="2025-08-01")
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
                                         league_strength={"Serie A": 1.0})
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
                                         league_strength={"Serie B": 0.663})
    p = by_team[team][0]
    assert p["defense_minutes"] == pytest.approx(90)
    assert p["club_ga_per90"] == pytest.approx(2.0)   # NOT 2.0 * 0.663


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

    by_team = strength.load_team_players(conn, [team], "2025-09-09")
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

    results = strength.compute(conn, [team_a, team_b], "Serie A", 2025, "2025-09-03")
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
                               player_spread_stretch_attack=1.0, player_spread_stretch_defense=1.0)
    r = results[team_a]

    prior = (1200 * 1.5 + 1200 * 1.0) / (1200 + 1200)
    ra_a = (1200 * 1.5 + PLAYER_RATING_MINUTES_TO_HALF_TRUST_OWN_RATE_OVER_LEAGUE_AVERAGE * prior) / (1200 + PLAYER_RATING_MINUTES_TO_HALF_TRUST_OWN_RATE_OVER_LEAGUE_AVERAGE)
    ra_b = (1200 * 1.0 + PLAYER_RATING_MINUTES_TO_HALF_TRUST_OWN_RATE_OVER_LEAGUE_AVERAGE * prior) / (1200 + PLAYER_RATING_MINUTES_TO_HALF_TRUST_OWN_RATE_OVER_LEAGUE_AVERAGE)
    attack_mean = (ra_a + ra_b) / 2

    expected_la_home_multiplicative = ra_a * (r["avg_home"] / attack_mean)
    assert r["lambda_attack_player_home"] == pytest.approx(expected_la_home_multiplicative)
