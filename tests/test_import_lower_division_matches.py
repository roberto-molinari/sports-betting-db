"""
Tests for import_lower_division_matches.py's DB-facing dedup logic. Excludes anything
that calls the live TheStatsAPI client, same convention as test_club_league_import.py.
"""

import core.sports_db as sports_db
import import_lower_division_matches as lower_div


def test_find_existing_match_id_returns_none_when_absent(db_path, conn):
    assert lower_div.find_existing_match_id(conn, "mt_does_not_exist") is None


def test_find_existing_match_id_finds_by_api_match_id(db_path, conn):
    home = sports_db.ensure_soccer_team("Cremonese", "Serie B")
    away = sports_db.ensure_soccer_team("Spezia", "Serie B")
    match_id = sports_db.add_soccer_match("Serie B", 2024, home, away, "2024-09-14T13:00:00Z")
    sports_db.set_match_api_id(match_id, "mt_361107246", conn=conn)

    assert lower_div.find_existing_match_id(conn, "mt_361107246") == match_id


def test_find_existing_match_id_does_not_match_on_team_pairing_alone(db_path, conn):
    """A playoff rematch between the same two teams (same venue) is a DIFFERENT real
    match with a different api_match_id -- must NOT be treated as a duplicate of the
    regular-season meeting. This is the exact bug found 2026-08-03 backfilling Serie B
    2024 (Cremonese/Spezia met in both the regular season and the playoff final with
    the same home/away venue; an earlier (league, season, home, away) dedup key
    silently dropped the second, real match)."""
    home = sports_db.ensure_soccer_team("Cremonese", "Serie B")
    away = sports_db.ensure_soccer_team("Spezia", "Serie B")
    sports_db.add_soccer_match("Serie B", 2024, home, away, "2024-09-14T13:00:00Z")
    # A second, distinct match with the same (league, season, home, away) but a
    # different api_match_id (the playoff rematch) must be found as absent.
    assert lower_div.find_existing_match_id(conn, "mt_363062002") is None
