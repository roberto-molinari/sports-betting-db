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


def test_no_duplicate_model_prediction_rows(live_conn):
    """One prediction row per (match, method) -- BUG-018, 2026-08-20: matches
    priced by several sportsbooks used to get one prediction row PER odds row
    from the backfill scripts' bare soccer_betting_odds join (Serie A 2025: 30
    matches doubled), silently double-counting those matches in every
    downstream Brier/ROI query. The backfills now dedupe to one odds row per
    match; this pins the resulting invariant."""
    dupes = _count(live_conn, """
        SELECT COUNT(*) FROM (
            SELECT 1 FROM soccer_model_predictions
            GROUP BY match_id, method
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
    """Per LEAGUE-SEASON, not a single global cap: the original global <=30 bound
    dated from the Serie-A-only DB and went stale twice over (multi-league
    expansion FEATURE-014, then 2026-08-20's history extension back to 2022 --
    254 distinct teams across 10 leagues x 4-5 seasons is legitimate). The real
    invariant is that one league-season names a plausible division's worth of
    teams: every current top flight/feeder here runs 18-24 clubs (Bundesliga/
    2. Bundesliga/Ligue 2 at 18, Championship at 24); fewer than 16 means a
    partial import, more than 26 means duplicate-team damage."""
    rows = live_conn.execute("""
        SELECT league, season, COUNT(DISTINCT team_id) AS n FROM (
            SELECT league, season, home_team_id AS team_id FROM soccer_matches
            UNION SELECT league, season, away_team_id FROM soccer_matches
        )
        GROUP BY league, season
        HAVING n < 16 OR n > 26
    """).fetchall()
    assert rows == [], f"league-seasons with implausible team counts: {rows}"

    nhl_teams = _count(live_conn, """
        SELECT COUNT(DISTINCT team_id) FROM (
            SELECT home_team_id AS team_id FROM nhl_matches
            UNION SELECT away_team_id FROM nhl_matches
        )
    """)
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


# ── Team league label freshness (FEATURE-019, 2026-08-19) ───────────────────────
# soccer_teams.league is only ever set once, at first insert, and never touched
# again except by the season-kickoff sync -- a promoted/relegated team (or any
# other code path that slips through) can silently drift out of sync with the
# league they're actually playing in. This is a direct, standing regression
# check for exactly that: confirmed live 2026-08-19 to catch Hull City/Coventry
# City/Le Mans/Troyes/Racing de Santander all sitting on a stale pre-promotion
# league label.

def test_team_league_matches_their_most_recent_match(live_conn):
    """A team's stored league must match the league of its own most recent
    soccer_matches row. Exemptions: a team with no matches yet (freshly
    inserted, fixtures not imported); a team deliberately marked with the
    "dropped out of everything we track" sentinel (FEATURE-019 decision 3);
    and a team whose stored league has ZERO matches in the CURRENT (latest)
    season yet (import_league_matches.py deliberately only imports FINISHED
    matches for a feeder division with no odds source -- e.g. Serie B's new
    season has no recorded matches at all until its first results come in, so
    every correctly-labeled Serie B team would otherwise show as "stale"
    against their last, older match in a different league from a PRIOR season
    -- a data-availability gap, not a label bug -- while a league that DOES
    have current-season matches still gets the full, real check). All three
    are expected states, not staleness."""
    SENTINEL = "(unknown - not seen this season)"
    cur = live_conn.cursor()
    cur.execute("SELECT MAX(season) FROM soccer_matches")
    latest_season = cur.fetchone()[0]
    cur.execute("SELECT DISTINCT league FROM soccer_matches WHERE season = ?", (latest_season,))
    leagues_with_current_season_matches = {row[0] for row in cur.fetchall()}

    cur.execute("""
        SELECT t.name, t.league,
               (SELECT m.league FROM soccer_matches m
                WHERE m.home_team_id = t.team_id OR m.away_team_id = t.team_id
                ORDER BY m.match_date DESC, m.match_id DESC LIMIT 1) AS actual_league
        FROM soccer_teams t
    """)
    stale = [
        (name, stored, actual) for name, stored, actual in cur.fetchall()
        if actual is not None and stored != actual and stored != SENTINEL
        and stored in leagues_with_current_season_matches
    ]
    assert stale == [], (
        f"{len(stale)} team(s) have a league label that doesn't match their most "
        f"recent match (name, stored_league, actual_league): {stale}"
    )
