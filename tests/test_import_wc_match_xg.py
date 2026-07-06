"""Tests for the pure logic in import_wc_match_xg.py (FEATURE-008): team-name
normalization, comp-match matching, per-team xG aggregation, and the
survivors-vs-all scope filter. Deliberately excludes anything that calls the
live TheStatsAPI client -- those are validated by a manual --dry-run --team
pull, same convention as this repo's other TheStatsAPI import scripts."""

import sqlite3

import core.sports_db as sdb
import import_wc_match_xg as xg


def test_normalize_is_case_and_whitespace_insensitive():
    assert xg.normalize(" Brazil ") == xg.normalize("brazil") == "brazil"


def test_find_comp_match_single_candidate():
    index = {("brazil", "norway"): [{"id": 1, "date": "2026-07-05"}]}
    assert xg.find_comp_match(index, "Brazil", "Norway", "2026-07-05")["id"] == 1


def test_find_comp_match_no_candidate_returns_none():
    index = {("brazil", "norway"): [{"id": 1, "date": "2026-07-05"}]}
    assert xg.find_comp_match(index, "Mexico", "England", "2026-07-05") is None


def test_find_comp_match_breaks_ties_on_closest_date():
    """Two meetings between the same pair (e.g. a rematch) resolve to whichever
    comp_6107 date is closest to our own stored match_date."""
    index = {("brazil", "norway"): [
        {"id": 1, "date": "2026-06-20"},
        {"id": 2, "date": "2026-07-05"},
    ]}
    best = xg.find_comp_match(index, "Brazil", "Norway", "2026-07-04")
    assert best["id"] == 2


def test_match_team_xg_sums_by_team_and_ignores_others():
    rows = [
        {"team_id": 10, "shooting": {"expected_goals": 0.4}},
        {"team_id": 10, "shooting": {"expected_goals": 0.6}},
        {"team_id": 20, "shooting": {"expected_goals": 0.3}},
        {"team_id": 999, "shooting": {"expected_goals": 5.0}},   # unrelated team, ignored
    ]
    home_xg, away_xg = xg.match_team_xg(rows, home_team_id=10, away_team_id=20)
    assert home_xg == 1.0
    assert away_xg == 0.3


def test_match_team_xg_handles_missing_shooting_block():
    rows = [{"team_id": 10}, {"team_id": 20, "shooting": {}}]
    home_xg, away_xg = xg.match_team_xg(rows, home_team_id=10, away_team_id=20)
    assert (home_xg, away_xg) == (0.0, 0.0)


def _seed_match(stage, home_name, away_name, date):
    home = sdb.ensure_wc_team(home_name)
    away = sdb.ensure_wc_team(away_name)
    match_id = sdb.ensure_wc_match(date, home, away, stage=stage)
    sdb.update_wc_match_result(match_id, 1, 0)
    return match_id


def test_survivor_team_names_only_includes_r16_plus(db_path, conn):
    _seed_match("Group", "Eliminated Early", "Also Eliminated", "2026-06-12 18:00:00")
    _seed_match("R32", "Won R32 Lost R16", "R32 Loser", "2026-06-28 18:00:00")
    _seed_match("R16", "Won R32 Lost R16", "R16 Winner", "2026-07-05 18:00:00")

    survivors = xg.survivor_team_names(conn)
    assert survivors == {"Won R32 Lost R16", "R16 Winner"}


def test_our_matches_survivors_scope_excludes_eliminated_teams(db_path, conn):
    conn.row_factory = sqlite3.Row
    _seed_match("Group", "Eliminated Early", "Also Eliminated", "2026-06-12 18:00:00")
    r16_match = _seed_match("R16", "Survivor A", "Survivor B", "2026-07-05 18:00:00")

    survivors_scope = xg.our_matches(conn, "survivors", team_filter=None)
    all_scope = xg.our_matches(conn, "all", team_filter=None)

    assert [m["match_id"] for m in survivors_scope] == [r16_match]
    assert len(all_scope) == 2   # both the eliminated-teams group game and the R16 game


def test_our_matches_all_scope_extends_with_zero_extra_work(db_path, conn):
    """The whole point of --scope all: it's the exact same code path, just without
    the survivor filter -- no special-casing needed for the post-tournament pull."""
    conn.row_factory = sqlite3.Row
    _seed_match("Group", "Team A", "Team B", "2026-06-12 18:00:00")
    _seed_match("R32", "Team C", "Team D", "2026-06-28 18:00:00")

    all_scope = xg.our_matches(conn, "all", team_filter=None)
    assert len(all_scope) == 2


def test_already_fetched_reflects_stored_rows(db_path, conn):
    match_id = _seed_match("R16", "Team A", "Team B", "2026-07-05 18:00:00")
    assert xg.already_fetched(conn, match_id) is False

    sdb.upsert_wc_external_xg(match_id=match_id, source="thestatsapi",
                              home_xg=1.2, away_xg=0.8, fetched_at="2026-07-06T00:00:00+00:00")
    assert xg.already_fetched(conn, match_id) is True
    # A different source hasn't been fetched.
    assert conn.execute(
        "SELECT 1 FROM soccer_wc_external_xg WHERE match_id = ? AND source = 'other'",
        (match_id,)).fetchone() is None
