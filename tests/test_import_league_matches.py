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


def test_update_soccer_match_date_persists_a_corrected_kickoff(db_path, conn):
    """BUG-024 (2026-08-31): a flagged match_date CONFLICT (a source-side
    reschedule) had no write path at all -- --allow-overwrite claimed to
    apply it but the only apply call was update_soccer_match_result, which
    only ever touches score/status, so the stale date silently stuck around
    (a real live case: Aston Villa v Arsenal stayed dated two days early and
    dropped out of the matchday window entirely)."""
    home = sports_db.ensure_soccer_team("Aston Villa", "Premier League", "England")
    away = sports_db.ensure_soccer_team("Arsenal", "Premier League", "England")
    match_id = sports_db.add_soccer_match("Premier League", 2026, home, away, "2026-08-29T14:00:00.000Z")

    sports_db.update_soccer_match_date(match_id, "2026-08-31T19:00:00.000Z")

    cur = conn.cursor()
    cur.execute("SELECT match_date FROM soccer_matches WHERE match_id = ?", (match_id,))
    assert cur.fetchone()[0] == "2026-08-31T19:00:00.000Z"


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


def test_compute_conflicts_treats_equivalent_date_formats_as_unchanged():
    """Found live 2026-09-04 migrating Serie A: its rows (previously sourced
    from football-data.org) store match_date without the ".000" milliseconds
    TheStatsAPI's own format always has -- a literal string compare falsely
    flagged every single Serie A match as a match_date CONFLICT on this
    script's first real run for that league, even though the instant is
    identical."""
    existing = (1, None, None, "2026-09-13T15:30:00Z", "scheduled")
    diffs = league_matches.compute_conflicts(existing, "scheduled", None, None, "2026-09-13T15:30:00.000Z")
    assert diffs == []


def test_compute_conflicts_still_flags_a_real_date_change_despite_format_normalization():
    """The normalization must not swallow genuine postponements -- a real
    instant change has to keep being flagged even when both sides happen to
    use TheStatsAPI's own format."""
    existing = (1, None, None, "2026-09-13T15:30:00.000Z", "scheduled")
    diffs = league_matches.compute_conflicts(existing, "scheduled", None, None, "2026-09-20T15:30:00.000Z")
    assert diffs == [("match_date", "2026-09-13T15:30:00.000Z", "2026-09-20T15:30:00.000Z")]


def test_compute_conflicts_falls_back_to_string_compare_on_unparseable_date():
    """A malformed date must not crash the comparison, and must never be
    silently treated as a match just because parsing failed."""
    existing = (1, None, None, "not-a-real-date", "scheduled")
    diffs = league_matches.compute_conflicts(existing, "scheduled", None, None, "2026-09-20T15:30:00.000Z")
    assert diffs == [("match_date", "not-a-real-date", "2026-09-20T15:30:00.000Z")]


def test_resync_not_triggered_by_an_already_completed_match(db_path, conn):
    home = sports_db.ensure_soccer_team("Team Done", "Premier League", "England")
    away = sports_db.ensure_soccer_team("Team DoneOpp", "Premier League", "England")
    kickoff = (datetime.now(timezone.utc) - timedelta(hours=5)).isoformat()
    match_id = sports_db.add_soccer_match("Premier League", 2026, home, away, kickoff, status="scheduled")
    sports_db.update_soccer_match_result(match_id, 2, 1)

    assert league_matches.any_match_due_for_resync(conn, "Premier League", 2026) is False


# ── Duplicate-fixture detection (2026-08-23 -- real Ligue 1 Rennes/PSG incident,
#    see find_conflicting_pairing()'s docstring) ───────────────────────────────────

def test_find_conflicting_pairing_flags_a_different_id_same_pairing_same_day(db_path, conn):
    """The exact shape of the real incident: a second thestatsapi_match_id shows up
    for a team pairing we already have a match id for, on the same day, with
    home/away reversed."""
    psg = sports_db.ensure_soccer_team("Paris Saint-Germain", "Ligue 1", "France")
    rennes = sports_db.ensure_soccer_team("Stade Rennais", "Ligue 1", "France")
    match_id = sports_db.add_soccer_match("Ligue 1", 2026, rennes, psg, "2026-08-23T18:45:00.000Z")
    sports_db.set_thestatsapi_match_id(match_id, "mt_466109840", conn=conn)

    dup = league_matches.find_conflicting_pairing(
        conn, "Ligue 1", 2026, psg, rennes, "mt_022917220", "2026-08-23T18:45:00.000Z")
    assert dup is not None
    assert dup[0] == match_id
    assert dup[1] == "mt_466109840"


def test_find_conflicting_pairing_ignores_the_same_id(db_path, conn):
    """A re-fetch of the SAME match (same thestatsapi_match_id) must never flag
    itself as a duplicate -- that's just find_existing_match's normal job."""
    psg = sports_db.ensure_soccer_team("Paris Saint-Germain", "Ligue 1", "France")
    rennes = sports_db.ensure_soccer_team("Stade Rennais", "Ligue 1", "France")
    match_id = sports_db.add_soccer_match("Ligue 1", 2026, rennes, psg, "2026-08-23T18:45:00.000Z")
    sports_db.set_thestatsapi_match_id(match_id, "mt_466109840", conn=conn)

    dup = league_matches.find_conflicting_pairing(
        conn, "Ligue 1", 2026, rennes, psg, "mt_466109840", "2026-08-23T18:45:00.000Z")
    assert dup is None


def test_find_conflicting_pairing_does_not_flag_a_real_home_and_away_leg(db_path, conn):
    """Two teams legitimately meet twice a season, months apart -- that must NOT be
    flagged as a duplicate (this is exactly what DUPLICATE_FIXTURE_TOLERANCE_DAYS
    exists to exclude)."""
    psg = sports_db.ensure_soccer_team("Paris Saint-Germain", "Ligue 1", "France")
    rennes = sports_db.ensure_soccer_team("Stade Rennais", "Ligue 1", "France")
    match_id = sports_db.add_soccer_match("Ligue 1", 2026, psg, rennes, "2026-08-23T18:45:00.000Z")
    sports_db.set_thestatsapi_match_id(match_id, "mt_022917298", conn=conn)

    dup = league_matches.find_conflicting_pairing(
        conn, "Ligue 1", 2026, rennes, psg, "mt_155794506", "2027-02-13T18:00:00.000Z")
    assert dup is None


def test_find_conflicting_pairing_scoped_to_league_and_season(db_path, conn):
    """A same-day pairing in a DIFFERENT season must not be flagged -- team ids can
    repeat across seasons and this isn't the same real-world ambiguity."""
    psg = sports_db.ensure_soccer_team("Paris Saint-Germain", "Ligue 1", "France")
    rennes = sports_db.ensure_soccer_team("Stade Rennais", "Ligue 1", "France")
    match_id = sports_db.add_soccer_match("Ligue 1", 2025, rennes, psg, "2026-08-23T18:45:00.000Z")
    sports_db.set_thestatsapi_match_id(match_id, "mt_466109840", conn=conn)

    dup = league_matches.find_conflicting_pairing(
        conn, "Ligue 1", 2026, psg, rennes, "mt_022917220", "2026-08-23T18:45:00.000Z")
    assert dup is None


# ── Routine completion vs. genuine conflict (2026-08-24 -- all-caps CONFLICT
#    logging for an expected scheduled->completed transition read as an error
#    to a first-time user; see BUGS.md and is_routine_completion()'s docstring) ──

def test_first_time_score_is_a_routine_completion():
    existing = (1, None, None, "2026-09-13T15:30:00.000Z", "scheduled")
    diffs = league_matches.compute_conflicts(existing, "completed", 2, 1, "2026-09-13T15:30:00.000Z")
    assert league_matches.is_routine_completion(existing, diffs) is True


def test_score_correction_on_already_completed_match_is_not_routine():
    """A source changing the score of a match ALREADY marked completed is a
    real correction -- must stay flagged, not silently reclassified."""
    existing = (1, 2, 1, "2026-09-13T15:30:00.000Z", "completed")
    diffs = league_matches.compute_conflicts(existing, "completed", 3, 1, "2026-09-13T15:30:00.000Z")
    assert league_matches.is_routine_completion(existing, diffs) is False


def test_postponement_date_change_is_not_routine():
    """A match_date change (postponement) must stay flagged even if it also
    happens to come with a first-time score/status change."""
    existing = (1, None, None, "2026-09-13T15:30:00.000Z", "scheduled")
    diffs = league_matches.compute_conflicts(existing, "completed", 2, 1, "2026-09-20T15:30:00.000Z")
    assert league_matches.is_routine_completion(existing, diffs) is False


def test_pure_date_change_on_still_scheduled_match_is_not_routine():
    existing = (1, None, None, "2026-09-13T15:30:00.000Z", "scheduled")
    diffs = league_matches.compute_conflicts(existing, "scheduled", None, None, "2026-09-20T15:30:00.000Z")
    assert league_matches.is_routine_completion(existing, diffs) is False
