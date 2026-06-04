"""
Data-integrity checks against the live sports_betting.db.

These encode the invariants the ``validate_*.py`` scripts checked by eye, as
pass/fail assertions that can run unattended. They are marked ``data_integrity``
and are NOT run by the default ``pytest`` invocation (which sticks to the fast,
DB-independent unit tests). Run them explicitly:

    pytest -m data_integrity

They open the real database read-only and skip cleanly if it isn't present
(e.g. on a fresh checkout or in CI without the data file).

Note: these assert *invariants* that should always hold (referential integrity,
no duplicate fixtures, completed games have scores) — not coverage thresholds
like "X% of games have odds", which drift as data is collected. Odds-coverage
progress stays in validate_nhl_odds_coverage.py as a human-readable report.
"""

import sqlite3

import pytest

from core.sports_db import DATABASE_PATH

pytestmark = pytest.mark.data_integrity


@pytest.fixture(scope="module")
def live_conn():
    if not DATABASE_PATH.exists():
        pytest.skip(f"live database not found at {DATABASE_PATH}")
    # Read-only URI connection so a test can never mutate real data.
    uri = f"file:{DATABASE_PATH}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    try:
        yield connection
    finally:
        connection.close()


def _count(conn, sql, params=()):
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.fetchone()[0]


# ── Sanity: pointed at real data ─────────────────────────────────────────────

def test_match_tables_non_empty(live_conn):
    assert _count(live_conn, "SELECT COUNT(*) FROM soccer_matches") > 0
    assert _count(live_conn, "SELECT COUNT(*) FROM nhl_matches") > 0


# ── No duplicate fixtures ────────────────────────────────────────────────────

def test_no_duplicate_soccer_matches(live_conn):
    dupes = _count(live_conn, """
        SELECT COUNT(*) FROM (
            SELECT 1 FROM soccer_matches
            GROUP BY home_team_id, away_team_id, DATE(match_date)
            HAVING COUNT(*) > 1
        )
    """)
    assert dupes == 0


def test_no_duplicate_nhl_matches(live_conn):
    # NHL preseason has legitimate split-squad doubleheaders: the same two teams
    # play twice on one calendar date at different times (e.g. NSH vs FLA on
    # 2025-09-21 at 19:00Z and 23:00Z — both real, both FINAL per the NHL API).
    # So we dedupe on the full timestamp, which is the natural key the schema's
    # idx_nhl_match_unique already enforces — NOT on DATE(), which would falsely
    # flag those doubleheaders.
    dupes = _count(live_conn, """
        SELECT COUNT(*) FROM (
            SELECT 1 FROM nhl_matches
            GROUP BY season, home_team_id, away_team_id, match_date
            HAVING COUNT(*) > 1
        )
    """)
    assert dupes == 0


# ── Completed games must have scores ─────────────────────────────────────────

def test_completed_soccer_matches_have_scores(live_conn):
    missing = _count(live_conn, """
        SELECT COUNT(*) FROM soccer_matches
        WHERE match_status = 'completed'
          AND (home_score IS NULL OR away_score IS NULL)
    """)
    assert missing == 0


def test_completed_nhl_matches_have_scores(live_conn):
    missing = _count(live_conn, """
        SELECT COUNT(*) FROM nhl_matches
        WHERE match_status = 'completed'
          AND (home_score IS NULL OR away_score IS NULL)
    """)
    assert missing == 0


# ── Score sanity ─────────────────────────────────────────────────────────────

def test_scores_are_non_negative(live_conn):
    bad_soccer = _count(live_conn, """
        SELECT COUNT(*) FROM soccer_matches
        WHERE home_score < 0 OR away_score < 0
    """)
    bad_nhl = _count(live_conn, """
        SELECT COUNT(*) FROM nhl_matches
        WHERE home_score < 0 OR away_score < 0
    """)
    assert bad_soccer == 0
    assert bad_nhl == 0


def test_no_team_scores_absurdly_high(live_conn):
    """No single team should post >10 goals — in this data that's a parse error,
    not a real result."""
    soccer = _count(live_conn, "SELECT COUNT(*) FROM soccer_matches WHERE home_score > 10 OR away_score > 10")
    nhl = _count(live_conn, "SELECT COUNT(*) FROM nhl_matches WHERE home_score > 10 OR away_score > 10")
    assert soccer == 0
    assert nhl == 0


# ── Team coverage within league bounds ───────────────────────────────────────

def test_distinct_team_counts_within_bounds(live_conn):
    soccer_teams = _count(live_conn, """
        SELECT COUNT(DISTINCT team_id) FROM (
            SELECT home_team_id AS team_id FROM soccer_matches
            UNION SELECT away_team_id FROM soccer_matches
        )
    """)
    nhl_teams = _count(live_conn, """
        SELECT COUNT(DISTINCT team_id) FROM (
            SELECT home_team_id AS team_id FROM nhl_matches
            UNION SELECT away_team_id FROM nhl_matches
        )
    """)
    # Serie A: 20 per season, a few more across promotion/relegation over seasons.
    assert 0 < soccer_teams <= 30
    # NHL has 32 franchises.
    assert 0 < nhl_teams <= 32


# ── Referential integrity ────────────────────────────────────────────────────

@pytest.mark.parametrize("odds_table,match_table", [
    ("soccer_betting_odds", "soccer_matches"),
    ("nhl_betting_odds", "nhl_matches"),
])
def test_no_orphan_odds(live_conn, odds_table, match_table):
    orphans = _count(live_conn, f"""
        SELECT COUNT(*) FROM {odds_table} o
        LEFT JOIN {match_table} m ON o.match_id = m.match_id
        WHERE m.match_id IS NULL
    """)
    assert orphans == 0


@pytest.mark.parametrize("match_table,team_table", [
    ("soccer_matches", "soccer_teams"),
    ("nhl_matches", "nhl_teams"),
])
def test_matches_reference_existing_teams(live_conn, match_table, team_table):
    orphans = _count(live_conn, f"""
        SELECT COUNT(*) FROM {match_table} m
        WHERE m.home_team_id NOT IN (SELECT team_id FROM {team_table})
           OR m.away_team_id NOT IN (SELECT team_id FROM {team_table})
    """)
    assert orphans == 0
