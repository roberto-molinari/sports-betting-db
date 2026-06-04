"""
Smoke tests for core.sports_db.

These initialise a fresh schema in a temp DB, insert a few sample rows through
the public helpers, and assert counts and round-trip reads. This is the
"initialise the DB, insert a couple of sample rows, assert counts" check called
out in ARCHITECTURE.md, plus coverage of the dedupe/upsert behaviour that the
import scripts rely on.
"""

import sqlite3

from core import sports_db


# ── Schema ───────────────────────────────────────────────────────────────────

EXPECTED_TABLES = {
    "soccer_teams", "soccer_matches", "soccer_betting_odds",
    "nhl_teams", "nhl_matches", "nhl_betting_odds",
}


def test_init_database_creates_all_tables(db_path, conn):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cur.fetchall()}
    assert EXPECTED_TABLES <= tables


def test_init_database_is_idempotent(db_path):
    # Running it twice must not raise or duplicate anything.
    sports_db.init_database()
    sports_db.init_database()


# ── Soccer round trip ────────────────────────────────────────────────────────

def test_soccer_full_round_trip(db_path):
    home = sports_db.ensure_soccer_team("Inter", "Serie A", country="Italy")
    away = sports_db.ensure_soccer_team("Juventus", "Serie A", country="Italy")
    assert home != away

    match_id = sports_db.add_soccer_match("Serie A", 2024, home, away, "2025-02-01")
    sports_db.update_soccer_match_result(match_id, 3, 1, halftime_home=1, halftime_away=0)
    sports_db.add_soccer_betting_odds(
        match_id, "Bet365", "2025-01-31",
        home_moneyline=-140, draw_moneyline=260, away_moneyline=380,
    )

    matches = sports_db.get_soccer_matches(league="Serie A", season=2024)
    assert len(matches) == 1
    row = matches[0]
    assert row["home_team_name"] == "Inter"
    assert row["away_team_name"] == "Juventus"
    assert row["home_score"] == 3
    assert row["away_score"] == 1
    assert row["halftime_home_score"] == 1
    assert row["match_status"] == "completed"


def test_ensure_soccer_team_is_idempotent(db_path):
    first = sports_db.ensure_soccer_team("Napoli", "Serie A")
    second = sports_db.ensure_soccer_team("Napoli", "Serie A")
    assert first == second
    assert sports_db.get_soccer_team_id("Napoli") == first
    assert sports_db.get_soccer_team_id("Nonexistent") is None


def test_get_soccer_matches_filters_by_status(db_path):
    home = sports_db.ensure_soccer_team("Roma", "Serie A")
    away = sports_db.ensure_soccer_team("Lazio", "Serie A")
    played = sports_db.add_soccer_match("Serie A", 2024, home, away, "2025-02-01")
    sports_db.update_soccer_match_result(played, 1, 1)
    sports_db.add_soccer_match("Serie A", 2024, away, home, "2025-05-01")  # scheduled

    completed = sports_db.get_soccer_matches(status="completed")
    scheduled = sports_db.get_soccer_matches(status="scheduled")
    assert len(completed) == 1
    assert len(scheduled) == 1


# ── NHL round trip + dedupe ──────────────────────────────────────────────────

def test_nhl_full_round_trip(db_path):
    home = sports_db.ensure_nhl_team("Boston Bruins")
    away = sports_db.ensure_nhl_team("Toronto Maple Leafs")
    match_id = sports_db.add_nhl_match(2025, home, away, "2025-11-01")
    sports_db.update_nhl_match_result(match_id, 4, 2)
    sports_db.add_nhl_betting_odds(
        match_id, "DraftKings", "2025-10-31",
        home_moneyline=-160, away_moneyline=135,
    )

    matches = sports_db.get_nhl_matches(season=2025)
    assert len(matches) == 1
    assert matches[0]["home_team_name"] == "Boston Bruins"
    assert matches[0]["home_score"] == 4
    assert matches[0]["match_status"] == "completed"


def test_add_nhl_match_dedupes_on_natural_key(db_path):
    """Re-adding the same (season, home, away, date) must reuse the match_id
    rather than create a duplicate — the import scripts depend on this."""
    home = sports_db.ensure_nhl_team("New York Rangers")
    away = sports_db.ensure_nhl_team("New Jersey Devils")
    first = sports_db.add_nhl_match(2025, home, away, "2025-12-01", status="scheduled")
    second = sports_db.add_nhl_match(2025, home, away, "2025-12-01", status="completed")
    assert first == second

    cur = sqlite3.connect(db_path).cursor()
    cur.execute("SELECT COUNT(*) FROM nhl_matches")
    assert cur.fetchone()[0] == 1
    # Status was upgraded on the existing row.
    matches = sports_db.get_nhl_matches(season=2025)
    assert matches[0]["match_status"] == "completed"


def test_canonical_nhl_team_name_normalises_alias(db_path):
    """The Montréal alias should resolve to a single canonical team."""
    a = sports_db.ensure_nhl_team("Montreal Canadiens")   # ascii feed spelling
    b = sports_db.ensure_nhl_team("Montréal Canadiens")   # canonical
    assert a == b
    assert sports_db.get_nhl_team_id("Montreal Canadiens") == a
