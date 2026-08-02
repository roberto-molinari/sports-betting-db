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

    by_team = strength.load_team_players(conn, [team], 2025)
    assert len(by_team[team]) == 1
    assert by_team[team][0]["attack_rate"] == pytest.approx(1.0)
    assert by_team[team][0]["attack_rate"] != pytest.approx(4.5)
    assert by_team[team][0]["minutes"] == 90


def test_prefers_real_xg_over_goals_when_present(db_path, conn):
    team, opp, m1 = _seed_match(conn)
    player = sports_db.add_player(team, "Striker", position="F", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2025, venue="home", minutes_played=90,
                                     goals=0, xg=0.9, conn=conn)

    by_team = strength.load_team_players(conn, [team], 2025)
    assert by_team[team][0]["attack_rate"] == pytest.approx(0.9)   # xg, not goals=0


def test_falls_back_to_goals_when_no_match_has_xg(db_path, conn):
    team, opp, m1 = _seed_match(conn)
    player = sports_db.add_player(team, "Striker", position="F", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2025, venue="home", minutes_played=90,
                                     goals=1, xg=None, conn=conn)

    by_team = strength.load_team_players(conn, [team], 2025)
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

    by_team = strength.load_team_players(conn, [team], 2025)
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

    by_team = strength.load_team_players(conn, [team], 2025)
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
                                     away_team_id INTEGER);
        CREATE TABLE soccer_player_stats (stat_id INTEGER PRIMARY KEY, player_id INTEGER,
                                          match_id INTEGER, season INTEGER, venue TEXT,
                                          minutes_played INTEGER, goals INTEGER,
                                          xg REAL, club_ga_per90 REAL, club_xga_per90 REAL);
    """)
    raw.execute("INSERT INTO soccer_players (player_id, team_id, position) VALUES (1, 10, 'M')")
    raw.execute("INSERT INTO soccer_matches (match_id, home_team_id, away_team_id) VALUES (501, 10, 99)")
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

    by_team = strength.load_team_players(raw, [10], 2025)
    assert len(by_team[10]) == 1
    assert by_team[10][0]["minutes"] == 90   # NOT 90 + 2500
    raw.close()


def test_attributes_stats_to_match_time_team_not_current_team(db_path, conn):
    """A player who transferred mid-season must have each stint's stats attributed to
    the team they actually played for IN THAT MATCH, not wherever soccer_players.team_id
    currently points (their LATEST team) -- the real bug found scaling this to the full
    20-team Serie A (Sebastiano Luperto: 23 Cagliari matches were being silently folded
    into Cremonese, his team as of the last match processed, leaving Cagliari's
    aggregate missing him entirely). Querying both teams must split his stats
    correctly, not lose or duplicate them."""
    team_a, opp1, m1 = _seed_match(conn, "OldClub", "OppA", date="2025-09-01")
    team_b, opp2, m2 = _seed_match(conn, "NewClub", "OppB", date="2026-02-01")
    player = sports_db.add_player(team_a, "Mid-Season Mover", api_player_id="ext_msm", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2025, venue="home",
                                     minutes_played=900, goals=3, conn=conn)
    sports_db.add_player(team_b, "Mid-Season Mover", api_player_id="ext_msm", conn=conn)  # transfer
    sports_db.add_player_match_stats(player, m2, season=2025, venue="home",
                                     minutes_played=450, goals=1, conn=conn)

    by_team = strength.load_team_players(conn, [team_a, team_b], 2025)
    assert len(by_team[team_a]) == 1
    assert by_team[team_a][0]["minutes"] == 900
    assert len(by_team[team_b]) == 1
    assert by_team[team_b][0]["minutes"] == 450


def test_player_with_zero_total_minutes_is_excluded(db_path, conn):
    team, opp, m1 = _seed_match(conn)
    player = sports_db.add_player(team, "Unused Sub", position="F", conn=conn)
    sports_db.add_player_match_stats(player, m1, season=2025, minutes_played=0, goals=0, conn=conn)

    by_team = strength.load_team_players(conn, [team], 2025)
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


def test_player_trust_high_when_full_coverage_and_full_churn(db_path, conn):
    """The headline case this whole mechanism exists for: last season's squad left
    entirely, replaced by well-tracked players (>=900 min at their PREVIOUS club last
    season) -- last season's team-level number describes a squad that's gone, and we
    have real signal on the new one. Both factors strong -> trust close to 1.0."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2025, date="2025-09-01")
    team_b, opp_b, m2 = _seed_match(conn, "TeamB", "OppB", season=2025, date="2025-10-01")
    away_team = sports_db.ensure_soccer_team("MovedOn", "Serie A")

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

    trust = strength.player_trust_score(conn, team_a, season=2026)
    assert trust == pytest.approx(1.0)
    assert strength.resolve_blend_weight(conn, team_a, "Serie A", "attack", season=2026) == pytest.approx(0.0)


def test_player_trust_low_when_roster_is_stable_despite_full_coverage(db_path, conn):
    """Same players, same team, year over year -- last season's team-level number
    still describes THIS squad, so there's nothing to gain from the player signal even
    though the data coverage is excellent. Guards the AND (product), not OR/average."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2025, date="2025-09-01")
    p1 = sports_db.add_player(team_a, "Stalwart One", api_player_id="ext_s1", conn=conn)
    p2 = sports_db.add_player(team_a, "Stalwart Two", api_player_id="ext_s2", conn=conn)
    sports_db.add_player_match_stats(p1, m1, season=2025, venue="home", minutes_played=900, conn=conn)
    sports_db.add_player_match_stats(p2, m1, season=2025, venue="home", minutes_played=900, conn=conn)
    # No roster changes -- p1/p2 remain on team_a (current squad == last season's roster).

    trust = strength.player_trust_score(conn, team_a, season=2026)
    assert trust == pytest.approx(0.0)
    assert strength.resolve_blend_weight(conn, team_a, "Serie A", "defense", season=2026) == pytest.approx(1.0)


def test_player_trust_low_when_churn_is_high_but_new_players_are_unproven(db_path, conn):
    """The edge case flagged in FEATURE-011_REQUIREMENTS.md: heavy churn INTO players
    with no usable last-season track record. Last season's team-level number is stale
    (squad mostly gone) AND we don't know the new squad either -- falls back to
    team-level as the least-bad option, not because it's trusted."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2025, date="2025-09-01")
    away_team = sports_db.ensure_soccer_team("MovedOn2", "Serie A")

    p1 = _transfer(conn, team_a, "P1b", "ext_p1b")
    p2 = _transfer(conn, team_a, "P2b", "ext_p2b")
    sports_db.add_player_match_stats(p1, m1, season=2025, venue="home", minutes_played=900, conn=conn)
    sports_db.add_player_match_stats(p2, m1, season=2025, venue="home", minutes_played=900, conn=conn)
    _transfer(conn, away_team, "P1b", "ext_p1b")
    _transfer(conn, away_team, "P2b", "ext_p2b")

    # Newcomers have NO tracked minutes anywhere last season (debutants/reserves).
    sports_db.add_player(team_a, "Rookie One", conn=conn)
    sports_db.add_player(team_a, "Rookie Two", conn=conn)

    trust = strength.player_trust_score(conn, team_a, season=2026)
    assert trust == pytest.approx(0.0)


def test_player_trust_zero_when_no_last_season_history(db_path, conn):
    """No last-season data for the team at all (e.g. backfill not run yet) -> falls
    back fully to team-level, not a ZeroDivisionError."""
    team_a = sports_db.ensure_soccer_team("Brand New Import", "Serie A")
    trust = strength.player_trust_score(conn, team_a, season=2026)
    assert trust == 0.0
    assert strength.resolve_blend_weight(conn, team_a, "Serie A", "attack", season=2026) == 1.0


def test_resolve_blend_weight_league_override_takes_precedence(db_path, conn, monkeypatch):
    """A league-wide override short-circuits the per-team computation entirely --
    confirmed here by NOT seeding any data (if it fell through to the real
    computation it would hit the no-history path and return 1.0, not the override)."""
    monkeypatch.setattr(strength, "LEAGUE_WEIGHT_OVERRIDES", {"Serie A": {"attack": 0.2}})
    team_a = sports_db.ensure_soccer_team("Overridden Team", "Serie A")

    assert strength.resolve_blend_weight(conn, team_a, "Serie A", "attack", season=2026) == 0.2
    # Defense wasn't overridden -- falls through to the real (here: no-history) computation.
    assert strength.resolve_blend_weight(conn, team_a, "Serie A", "defense", season=2026) == 1.0
    # A different league is unaffected by Serie A's override.
    assert strength.resolve_blend_weight(conn, team_a, "Premier League", "attack", season=2026) == 1.0


# ── squad_as_of_date + before_date plumbing (backtesting support) ────────────────────

def test_squad_as_of_date_uses_current_season_matches_when_available(db_path, conn):
    """Once the season being backtested has its own match evidence, that's the signal
    -- not last season's roster."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2024, date="2024-09-01")
    _, _, m2 = _seed_match(conn, "TeamA", "OppB", season=2025, date="2025-09-01")
    p_old = sports_db.add_player(team_a, "Last Season Player", api_player_id="ext_lsp", conn=conn)
    p_new = sports_db.add_player(team_a, "This Season Player", api_player_id="ext_tsp", conn=conn)
    sports_db.add_player_match_stats(p_old, m1, season=2024, venue="home", minutes_played=900, conn=conn)
    sports_db.add_player_match_stats(p_new, m2, season=2025, venue="home", minutes_played=90, conn=conn)

    squad = strength.squad_as_of_date(conn, team_a, 2025, "2025-10-01")
    assert squad == {p_new}   # NOT p_old -- last season's roster isn't consulted


def test_squad_as_of_date_falls_back_to_last_season_when_no_matches_yet(db_path, conn):
    """Before the current season has any match evidence (the very start of a season),
    falls back to last season's final roster -- an honest, bounded approximation."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2024, date="2024-09-01")
    p_old = sports_db.add_player(team_a, "Last Season Player", conn=conn)
    sports_db.add_player_match_stats(p_old, m1, season=2024, venue="home", minutes_played=900, conn=conn)

    # Querying "as of" the very first day of the 2025 season -- no 2025 matches exist yet.
    squad = strength.squad_as_of_date(conn, team_a, 2025, "2025-08-01")
    assert squad == {p_old}


def test_squad_as_of_date_only_sees_matches_strictly_before_the_date(db_path, conn):
    """A match that happens ON OR AFTER the query date must not count as evidence --
    the whole point is no lookahead."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2025, date="2025-09-01")
    _, _, m2 = _seed_match(conn, "TeamA", "OppB", season=2025, date="2025-11-01")
    p_early = sports_db.add_player(team_a, "Early Player", conn=conn)
    p_later = sports_db.add_player(team_a, "Later Player", conn=conn)
    sports_db.add_player_match_stats(p_early, m1, season=2025, venue="home", minutes_played=90, conn=conn)
    sports_db.add_player_match_stats(p_later, m2, season=2025, venue="home", minutes_played=90, conn=conn)

    squad = strength.squad_as_of_date(conn, team_a, 2025, "2025-10-01")
    assert squad == {p_early}   # NOT p_later -- that match hasn't happened yet


def test_player_trust_score_accepts_current_squad_ids_override(db_path, conn):
    """The override parameter actually changes the result -- confirms it's wired
    through, not just accepted and ignored. Same setup as the full-churn headline
    test, but passing an explicit (different) squad instead of the live default."""
    team_a, opp_a, m1 = _seed_match(conn, "TeamA", "OppA", season=2025, date="2025-09-01")
    p1 = sports_db.add_player(team_a, "Stayed Player", api_player_id="ext_stay", conn=conn)
    sports_db.add_player_match_stats(p1, m1, season=2025, venue="home", minutes_played=900, conn=conn)
    # Live default: current_squad_player_ids(team_a) == {p1} -- roster unchanged, trust 0.
    assert strength.player_trust_score(conn, team_a, season=2026) == pytest.approx(0.0)

    # Override with a squad that looks completely different from last season's roster.
    p2 = sports_db.add_player(team_a, "Hypothetical New Player", api_player_id="ext_hyp", conn=conn)
    team_b, opp_b, m2 = _seed_match(conn, "TeamB", "OppC", season=2025, date="2025-09-08")
    sports_db.add_player_match_stats(p2, m2, season=2025, venue="home", minutes_played=1000, conn=conn)
    overridden_trust = strength.player_trust_score(conn, team_a, season=2026, current_squad_ids={p2})
    assert overridden_trust == pytest.approx(1.0)


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

    early = strength.load_team_players(conn, [team_a], 2025, before_date="2025-10-01")
    full = strength.load_team_players(conn, [team_a], 2025)
    assert early[team_a][0]["minutes"] == 90     # only m1
    assert full[team_a][0]["minutes"] == 180     # both


def test_compute_falls_back_to_baseline_for_true_cold_start_team(db_path, conn):
    """A newly-promoted team's very first match: zero team-level history (no prior
    matches at all) AND zero player data (nothing clears MIN_ATTACK_WEIGHT/
    MIN_DEFENSE_WEIGHT). team_level_lambda (2026-08-01 home/away-split rewrite) always
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


def test_team_level_lambda_defaults_to_xg_not_goals(db_path, conn):
    """team_metric defaults to 'xg' (2026-08-02) -- must reflect the xG-based number,
    not the goals-based one, proving the default is really wired to
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
    xg_result = strength.team_level_lambda(conn, team, "Serie A", "2025-09-16", avg_home=1.3, avg_away=1.1, team_metric="xg")
    goals_result = strength.team_level_lambda(conn, team, "Serie A", "2025-09-16", avg_home=1.3, avg_away=1.1, team_metric="goals")

    assert default_result[0] == pytest.approx(xg_result[0]) == pytest.approx(1.0)
    assert goals_result[0] == pytest.approx(5.0)
    assert default_result[0] != pytest.approx(goals_result[0])
