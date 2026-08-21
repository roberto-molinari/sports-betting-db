"""Tests for club_league_scorecard.py's pure reporting logic (2026-08-21,
docs/PRE-AND-POST-MATCHDAY-EXPERIENCE.md) -- refresh_results() itself isn't
tested here (it shells out to live-data import scripts); scorecard()/
_pick_stats() are the parts worth locking in."""
from unittest.mock import patch

import pytest

import core.sports_db as sports_db
from club_league_scorecard import scorecard, _pick_stats
from core.matchday import matchday_utc_window


def _seed_graded_pick(conn, league, side, result, odds, match_date="2025-09-22T18:00:00.000Z", method="poisson_v4_4"):
    home = sports_db.ensure_soccer_team(f"{league} Home", league)
    away = sports_db.ensure_soccer_team(f"{league} Away", league)
    match_id = sports_db.add_soccer_match(league, 2025, home, away, match_date)
    pick_ids = sports_db.replace_club_league_picks_for_match(
        match_id=match_id, league=league, generated_at="2025-09-21T00:00:00Z",
        picks=[{"side": side, "odds": odds, "prob": 0.55, "ev": 0.05, "stars": 3}],
        method=method, conn=conn,
    )
    sports_db.set_club_league_pick_result(pick_ids[0], result, conn=conn)
    conn.commit()
    return pick_ids[0]


def test_pick_stats_win_loss_roi():
    stats = _pick_stats([(+150, "win"), (-110, "loss")])
    assert stats["wins"] == 1
    assert stats["losses"] == 1
    assert stats["staked"] == 2.0
    # +150 win: profit +1.5; -110 loss: profit -1.0 -> net +0.5 on 2u staked
    assert stats["profit"] == pytest.approx(0.5)
    assert stats["roi"] == pytest.approx(0.25)


def test_pick_stats_push_excluded_from_staked_and_profit():
    stats = _pick_stats([(+150, "win"), (-110, "push")])
    assert stats["pushes"] == 1
    assert stats["staked"] == 1.0   # the push doesn't count toward the ROI denominator
    assert stats["wins"] == 1


def test_scorecard_pools_correctly_across_leagues(db_path, conn):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        _seed_graded_pick(conn, "Serie A", "HOME", "win", +150)
        _seed_graded_pick(conn, "Premier League", "AWAY", "loss", -110)

    start, end = matchday_utc_window("2025-09-22")
    card = scorecard(conn, start, end)

    assert "Serie A" in card["by_league"]
    assert "Premier League" in card["by_league"]
    assert card["by_league"]["Serie A"]["wins"] == 1
    assert card["by_league"]["Premier League"]["losses"] == 1
    # overall pools both leagues' picks into one true dollar-weighted ROI
    assert card["overall"]["staked"] == 2.0
    assert card["overall"]["wins"] == 1
    assert card["overall"]["losses"] == 1


def test_scorecard_excludes_ungraded_picks(db_path, conn):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        home = sports_db.ensure_soccer_team("Home FC", "Serie A")
        away = sports_db.ensure_soccer_team("Away FC", "Serie A")
        match_id = sports_db.add_soccer_match("Serie A", 2025, home, away, "2025-09-22T18:00:00.000Z")
        sports_db.replace_club_league_picks_for_match(
            match_id=match_id, league="Serie A", generated_at="2025-09-21T00:00:00Z",
            picks=[{"side": "HOME", "odds": -110, "prob": 0.55, "ev": 0.05, "stars": 3}],
            conn=conn,
        )
        conn.commit()

    start, end = matchday_utc_window("2025-09-22")
    card = scorecard(conn, start, end)
    assert card["by_league"] == {}   # ungraded pick, nothing to report yet


def test_scorecard_window_scoping(db_path, conn):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        _seed_graded_pick(conn, "Serie A", "HOME", "win", +150, match_date="2025-09-22T18:00:00.000Z")
        _seed_graded_pick(conn, "Serie A", "HOME", "loss", -110, match_date="2025-09-25T18:00:00.000Z")

    start, end = matchday_utc_window("2025-09-22")
    card = scorecard(conn, start, end)
    assert card["overall"]["wins"] == 1
    assert card["overall"]["losses"] == 0   # the 9/25 pick is out of the 9/22 window


def test_scorecard_merges_dry_run_extra_picks_without_persisted_rows(db_path, conn):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        _seed_graded_pick(conn, "Serie A", "HOME", "win", +150)  # already persisted

    start, end = matchday_utc_window("2025-09-22")
    # simulates what grade_picks_in_window(..., dry_run=True) would return --
    # a pick that was never actually written to soccer_club_league_picks.result
    extra = [("Premier League", -110, "loss", "poisson_v4_4")]
    card = scorecard(conn, start, end, extra_picks=extra)

    assert card["overall"]["wins"] == 1
    assert card["overall"]["losses"] == 1
    assert "Premier League" in card["by_league"]


def test_methods_seen_tracks_single_model_version(db_path, conn):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        _seed_graded_pick(conn, "Serie A", "HOME", "win", +150, method="poisson_v4_4")

    start, end = matchday_utc_window("2025-09-22")
    card = scorecard(conn, start, end)
    assert card["methods_seen"] == {"poisson_v4_4"}


def test_methods_seen_flags_mixed_model_versions(db_path, conn, capsys):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        _seed_graded_pick(conn, "Serie A", "HOME", "win", +150, method="poisson_v4_4")
        _seed_graded_pick(conn, "Serie A", "AWAY", "loss", -110, method="poisson_v4_5")

    start, end = matchday_utc_window("2025-09-22")
    card = scorecard(conn, start, end)
    assert card["methods_seen"] == {"poisson_v4_4", "poisson_v4_5"}

    from club_league_scorecard import print_scorecard
    print_scorecard("test", card)
    out = capsys.readouterr().out
    assert "WARNING" in out
    assert "poisson_v4_4" in out and "poisson_v4_5" in out
