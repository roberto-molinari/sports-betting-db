"""
Tests for FEATURE-011's player-level tables and helpers in core.sports_db:
soccer_players, soccer_player_stats (per-match), soccer_player_match_lineups,
soccer_player_team_strength, and the api_match_id column on soccer_matches.
"""

import sqlite3

import pytest

from core import sports_db


PLAYER_TABLES = {
    "soccer_players", "soccer_player_stats", "soccer_player_match_lineups",
    "soccer_player_team_strength",
}


def test_init_database_creates_player_tables(db_path, conn):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cur.fetchall()}
    assert PLAYER_TABLES <= tables


def test_soccer_player_stats_has_per_match_columns(db_path, conn):
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(soccer_player_stats)")
    cols = {row[1] for row in cur.fetchall()}
    assert {"match_id", "venue"} <= cols


def test_soccer_matches_has_api_match_id_column(db_path, conn):
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(soccer_matches)")
    cols = {row[1] for row in cur.fetchall()}
    assert "api_match_id" in cols


def test_ensure_player_stats_match_schema_migrates_older_db(tmp_path):
    """Simulate a database that predates the per-match rework (no match_id/venue on
    soccer_player_stats, no api_match_id on soccer_matches) and confirm the migration
    adds them without erroring. This is the actual migration PATH, distinct from the
    fresh-create path exercised by every other test via the db_path fixture."""
    path = tmp_path / "old_shaped.db"
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE soccer_matches (
            match_id INTEGER PRIMARY KEY, league TEXT, season INTEGER,
            home_team_id INTEGER, away_team_id INTEGER, match_date TIMESTAMP
        );
        CREATE TABLE soccer_player_stats (
            stat_id INTEGER PRIMARY KEY, player_id INTEGER, season INTEGER,
            minutes_played INTEGER
        );
    """)
    conn.commit()

    sports_db.ensure_player_stats_match_schema(conn)

    cur = conn.cursor()
    cur.execute("PRAGMA table_info(soccer_player_stats)")
    assert {"match_id", "venue"} <= {row[1] for row in cur.fetchall()}
    cur.execute("PRAGMA table_info(soccer_matches)")
    assert "api_match_id" in {row[1] for row in cur.fetchall()}
    # Re-running must not error (idempotent).
    sports_db.ensure_player_stats_match_schema(conn)
    conn.close()


# ── add_player identity lookup ───────────────────────────────────────────────────

def test_add_player_reuses_row_by_team_and_name_when_no_api_id(db_path, conn):
    team = sports_db.ensure_soccer_team("Inter", "Serie A")
    first = sports_db.add_player(team, "Lautaro Martinez", position="F", conn=conn)
    second = sports_db.add_player(team, "Lautaro Martinez", position="F", conn=conn)
    assert first == second


def test_add_player_prefers_api_player_id_over_team_and_name(db_path, conn):
    """A transferred player encountered via a historical match (old team) must
    resolve to the SAME player row when later encountered on their new team --
    api_player_id is the stable identity; (team_id, name) is not, across a
    transfer. This was a real bug fixed during the per-match rework."""
    old_team = sports_db.ensure_soccer_team("AC Milan", "Serie A")
    new_team = sports_db.ensure_soccer_team("Inter", "Serie A")

    original = sports_db.add_player(old_team, "Example Player", position="M",
                                    api_player_id="pl_999", conn=conn)
    after_transfer = sports_db.add_player(new_team, "Example Player", position="M",
                                          api_player_id="pl_999", conn=conn)

    assert original == after_transfer
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM soccer_players WHERE api_player_id = ?", ("pl_999",))
    assert cur.fetchone()[0] == 1
    # team_id reflects the most-recently-seen team, not a history of both.
    cur.execute("SELECT team_id FROM soccer_players WHERE player_id = ?", (original,))
    assert cur.fetchone()[0] == new_team


def test_add_player_without_api_id_does_not_collide_across_teams(db_path, conn):
    """Two different players with the same name on different teams, neither with an
    api_player_id, must stay distinct rows (falls back to (team_id, name))."""
    team_a = sports_db.ensure_soccer_team("Roma", "Serie A")
    team_b = sports_db.ensure_soccer_team("Lazio", "Serie A")
    a = sports_db.add_player(team_a, "Common Name", conn=conn)
    b = sports_db.add_player(team_b, "Common Name", conn=conn)
    assert a != b


def test_add_player_set_team_id_false_does_not_overwrite_existing_team(db_path, conn):
    """A historical/backfill import (e.g. a promoted team's PRIOR Serie B season,
    imported after their current Serie A season already exists) must not stomp
    team_id back to the old club -- that would corrupt the live
    current_squad_player_ids() signal. set_team_id=False should still update
    position/api_player_id, just not team_id, for an already-existing player."""
    old_team = sports_db.ensure_soccer_team("Spezia", "Serie B")
    new_team = sports_db.ensure_soccer_team("Cremonese", "Serie A")

    current = sports_db.add_player(new_team, "Example Player", position="M",
                                    api_player_id="pl_555", conn=conn)
    backfilled = sports_db.add_player(old_team, "Example Player", position="M",
                                      api_player_id="pl_555", conn=conn, set_team_id=False)

    assert current == backfilled
    cur = conn.cursor()
    cur.execute("SELECT team_id FROM soccer_players WHERE player_id = ?", (current,))
    assert cur.fetchone()[0] == new_team


def test_add_player_set_team_id_false_still_sets_team_id_on_new_player(db_path, conn):
    """set_team_id=False only protects an EXISTING row -- a brand-new player still
    needs a team_id to be usable at all."""
    team = sports_db.ensure_soccer_team("Sassuolo", "Serie B")
    player = sports_db.add_player(team, "New Player", position="D",
                                  api_player_id="pl_777", conn=conn, set_team_id=False)
    cur = conn.cursor()
    cur.execute("SELECT team_id FROM soccer_players WHERE player_id = ?", (player,))
    assert cur.fetchone()[0] == team


# ── add_player_match_stats ───────────────────────────────────────────────────────

def test_add_player_match_stats_round_trip(db_path, conn):
    team = sports_db.ensure_soccer_team("Napoli", "Serie A")
    opp = sports_db.ensure_soccer_team("Torino", "Serie A")
    player = sports_db.add_player(team, "Victor Osimhen", position="F", conn=conn)
    match_id = sports_db.add_soccer_match("Serie A", 2025, team, opp, "2025-09-01")

    stat_id = sports_db.add_player_match_stats(
        player, match_id, season=2025, venue="home", minutes_played=90,
        xg=0.8, xg_per90=0.8, goals=1, assists=0, club_ga_per90=1, source="thestatsapi",
        conn=conn,
    )
    cur = conn.cursor()
    cur.execute("""SELECT venue, minutes_played, goals, xg, club_ga_per90
                   FROM soccer_player_stats WHERE stat_id = ?""", (stat_id,))
    assert cur.fetchone() == ("home", 90, 1, 0.8, 1)


def test_add_player_match_stats_is_idempotent_and_coalesces(db_path, conn):
    """A re-run for the same (player, match) updates in place (no duplicate row),
    and COALESCE means a partial re-fetch (some fields None) doesn't wipe fields
    that were already stored."""
    team = sports_db.ensure_soccer_team("Fiorentina", "Serie A")
    opp = sports_db.ensure_soccer_team("Genoa", "Serie A")
    player = sports_db.add_player(team, "Moise Kean", conn=conn)
    match_id = sports_db.add_soccer_match("Serie A", 2025, team, opp, "2025-09-14")

    first = sports_db.add_player_match_stats(
        player, match_id, season=2025, venue="home", minutes_played=90,
        goals=2, club_ga_per90=0, conn=conn)
    # Second call omits `goals` (None) -- must NOT overwrite the stored value.
    second = sports_db.add_player_match_stats(
        player, match_id, season=2025, minutes_played=90, conn=conn)

    assert first == second
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM soccer_player_stats WHERE player_id = ? AND match_id = ?",
                (player, match_id))
    assert cur.fetchone()[0] == 1
    cur.execute("SELECT goals, venue FROM soccer_player_stats WHERE stat_id = ?", (first,))
    assert cur.fetchone() == (2, "home")   # preserved across the partial update


def test_add_player_match_stats_rejects_bad_venue(db_path, conn):
    team = sports_db.ensure_soccer_team("Udinese", "Serie A")
    opp = sports_db.ensure_soccer_team("Lecce", "Serie A")
    player = sports_db.add_player(team, "Some Player", conn=conn)
    match_id = sports_db.add_soccer_match("Serie A", 2025, team, opp, "2025-09-14")
    with pytest.raises(ValueError):
        sports_db.add_player_match_stats(player, match_id, venue="left", conn=conn)


# ── add_player_match_lineup ───────────────────────────────────────────────────────

def test_add_player_match_lineup_round_trip_and_idempotent(db_path, conn):
    team = sports_db.ensure_soccer_team("Bologna", "Serie A")
    opp = sports_db.ensure_soccer_team("Cagliari", "Serie A")
    player = sports_db.add_player(team, "Riccardo Orsolini", conn=conn)
    match_id = sports_db.add_soccer_match("Serie A", 2025, team, opp, "2025-09-20")

    first = sports_db.add_player_match_lineup(player, match_id, team, True,
                                              position="F", formation="4-2-3-1", conn=conn)
    # Re-run with an updated `started` value (e.g. subbed before kickoff correction).
    second = sports_db.add_player_match_lineup(player, match_id, team, False, conn=conn)

    assert first == second
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM soccer_player_match_lineups WHERE player_id = ? AND match_id = ?",
                (player, match_id))
    assert cur.fetchone()[0] == 1
    cur.execute("SELECT started, position, formation FROM soccer_player_match_lineups WHERE lineup_id = ?",
                (first,))
    row = cur.fetchone()
    assert row[0] == 0             # updated
    assert row[1] == "F"           # preserved (COALESCE, second call passed None)
    assert row[2] == "4-2-3-1"     # preserved


# ── set_match_api_id ─────────────────────────────────────────────────────────────

def test_set_match_api_id_round_trip(db_path, conn):
    home = sports_db.ensure_soccer_team("Verona", "Serie A")
    away = sports_db.ensure_soccer_team("Parma", "Serie A")
    match_id = sports_db.add_soccer_match("Serie A", 2025, home, away, "2025-10-01")
    sports_db.set_match_api_id(match_id, "mt_12345", conn=conn)
    cur = conn.cursor()
    cur.execute("SELECT api_match_id FROM soccer_matches WHERE match_id = ?", (match_id,))
    assert cur.fetchone()[0] == "mt_12345"


# ── soccer_player_team_strength ──────────────────────────────────────────────────

def test_player_team_strength_keeps_versions_and_returns_latest(db_path, conn):
    team = sports_db.ensure_soccer_team("Atalanta", "Serie A")
    sports_db.set_player_team_strength(
        team, "Serie A", lambda_attack_player=1.2, lambda_defense_player=1.0,
        lambda_attack_team=1.4, lambda_defense_team=0.9,
        lambda_attack_blend=1.3, lambda_defense_blend=0.95,
        weight_attack=0.5, weight_defense=0.5, basis="mix(w=0.5)", conn=conn,
    )
    sports_db.set_player_team_strength(
        team, "Serie A", lambda_attack_blend=1.35, lambda_defense_blend=0.9,
        weight_attack=0.3, weight_defense=0.3, basis="mix(w=0.3)", conn=conn,
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM soccer_player_team_strength WHERE team_id = ?", (team,))
    assert cur.fetchone()[0] == 2   # both versions retained

    latest = sports_db.get_latest_player_team_strength(team, conn=conn)
    assert latest["lambda_attack_blend"] == pytest.approx(1.35)
    assert latest["basis"] == "mix(w=0.3)"
    assert sports_db.get_latest_player_team_strength(999999, conn=conn) is None
