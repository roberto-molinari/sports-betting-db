"""
Tests for import_league_matches.py's DB-facing dedup/re-sync logic. Excludes anything
that calls the live TheStatsAPI client, same convention as test_club_league_import.py.
"""

from datetime import datetime, timedelta, timezone

import core.sports_db as sports_db
import import_league_matches as league_matches
from core.leagues import LEAGUES, has_odds_source


# ── Serie B regression (2026-08-10 rename/generalization from
#    import_lower_division_matches.py) ────────────────────────────────────────────

def test_serie_b_keeps_its_original_thestatsapi_competition_id():
    assert LEAGUES["Serie B"]["thestatsapi_competition_id"] == "comp_5450"


def test_serie_b_still_has_no_odds_source_so_stays_finished_only():
    """Serie B was ALWAYS finished-only (no betting on lower divisions) -- the
    generalization must not have accidentally switched it to import scheduled
    fixtures too, since has_odds_source drives that branch in import_league_matches.
    main()."""
    assert has_odds_source("Serie B") is False


def test_find_existing_match_returns_none_when_absent(db_path, conn):
    assert league_matches.find_existing_match(conn, "mt_does_not_exist") is None


def test_find_existing_match_finds_by_thestatsapi_match_id(db_path, conn):
    home = sports_db.ensure_soccer_team("Cremonese", "Serie B", "Italy")
    away = sports_db.ensure_soccer_team("Spezia", "Serie B", "Italy")
    match_id = sports_db.add_soccer_match("Serie B", 2024, home, away, "2024-09-14T13:00:00Z")
    sports_db.set_thestatsapi_match_id(match_id, "mt_361107246", conn=conn)

    found = league_matches.find_existing_match(conn, "mt_361107246")
    assert found is not None
    assert found[0] == match_id


def test_find_existing_match_does_not_match_on_team_pairing_alone(db_path, conn):
    """A playoff rematch between the same two teams (same venue) is a DIFFERENT real
    match with a different thestatsapi_match_id -- must NOT be treated as a duplicate
    of the regular-season meeting. This is the exact bug found 2026-08-03 backfilling
    Serie B 2024 (Cremonese/Spezia met in both the regular season and the playoff
    final with the same home/away venue; an earlier (league, season, home, away)
    dedup key silently dropped the second, real match)."""
    home = sports_db.ensure_soccer_team("Cremonese", "Serie B", "Italy")
    away = sports_db.ensure_soccer_team("Spezia", "Serie B", "Italy")
    match_id = sports_db.add_soccer_match("Serie B", 2024, home, away, "2024-09-14T13:00:00Z")
    sports_db.set_thestatsapi_match_id(match_id, "mt_361107246", conn=conn)
    # A second, distinct match with the same (league, season, home, away) but a
    # different thestatsapi_match_id (the playoff rematch) must be found as absent.
    assert league_matches.find_existing_match(conn, "mt_363062002") is None


def test_resync_skipped_when_no_scheduled_match_is_overdue(db_path, conn):
    """A scheduled match kicking off in the future (or in the last <4h) must NOT
    trigger a resync -- there's nothing new to fetch yet."""
    home = sports_db.ensure_soccer_team("Team Future", "Premier League", "England")
    away = sports_db.ensure_soccer_team("Team FutureOpp", "Premier League", "England")
    future_kickoff = (datetime.now(timezone.utc) + timedelta(days=3)).isoformat()
    sports_db.add_soccer_match("Premier League", 2026, home, away, future_kickoff, status="scheduled")

    assert league_matches.any_match_due_for_resync(conn, "Premier League", 2026) is False


def test_resync_triggered_when_a_scheduled_match_kicked_off_over_4h_ago(db_path, conn):
    home = sports_db.ensure_soccer_team("Team Overdue", "Premier League", "England")
    away = sports_db.ensure_soccer_team("Team OverdueOpp", "Premier League", "England")
    overdue_kickoff = (datetime.now(timezone.utc) - timedelta(hours=5)).isoformat()
    sports_db.add_soccer_match("Premier League", 2026, home, away, overdue_kickoff, status="scheduled")

    assert league_matches.any_match_due_for_resync(conn, "Premier League", 2026) is True


def test_compute_conflicts_empty_when_nothing_changed():
    existing = (1, 2, 1, "2026-09-13T15:30:00.000Z", "completed")
    diffs = league_matches.compute_conflicts(existing, "completed", 2, 1, "2026-09-13T15:30:00.000Z")
    assert diffs == []


def test_compute_conflicts_flags_scheduled_to_completed_transition():
    existing = (1, None, None, "2026-09-13T15:30:00.000Z", "scheduled")
    diffs = league_matches.compute_conflicts(existing, "completed", 2, 1, "2026-09-13T15:30:00.000Z")
    assert ("match_status", "scheduled", "completed") in diffs
    assert ("score", "None-None", "2-1") in diffs


def test_compute_conflicts_flags_a_score_correction_on_an_already_completed_match():
    """A data source correcting itself after the fact (BUG report scenario from the
    module docstring) -- must be flagged, not silently missed just because the
    match was already 'completed' before this run."""
    existing = (1, 2, 1, "2026-09-13T15:30:00.000Z", "completed")
    diffs = league_matches.compute_conflicts(existing, "completed", 3, 1, "2026-09-13T15:30:00.000Z")
    assert diffs == [("score", "2-1", "3-1")]


def test_compute_conflicts_ignores_score_fields_while_still_scheduled():
    """A still-unplayed match reporting score=None on both sides must not be
    flagged as a score conflict -- only a status/date change matters pre-kickoff."""
    existing = (1, None, None, "2026-09-13T15:30:00.000Z", "scheduled")
    diffs = league_matches.compute_conflicts(existing, "scheduled", None, None, "2026-09-13T15:30:00.000Z")
    assert diffs == []


def test_compute_conflicts_flags_a_postponed_match_date_change():
    existing = (1, None, None, "2026-09-13T15:30:00.000Z", "scheduled")
    diffs = league_matches.compute_conflicts(existing, "scheduled", None, None, "2026-09-20T15:30:00.000Z")
    assert diffs == [("match_date", "2026-09-13T15:30:00.000Z", "2026-09-20T15:30:00.000Z")]


def test_resync_not_triggered_by_an_already_completed_match(db_path, conn):
    home = sports_db.ensure_soccer_team("Team Done", "Premier League", "England")
    away = sports_db.ensure_soccer_team("Team DoneOpp", "Premier League", "England")
    kickoff = (datetime.now(timezone.utc) - timedelta(hours=5)).isoformat()
    match_id = sports_db.add_soccer_match("Premier League", 2026, home, away, kickoff, status="scheduled")
    sports_db.update_soccer_match_result(match_id, 2, 1)

    assert league_matches.any_match_due_for_resync(conn, "Premier League", 2026) is False
