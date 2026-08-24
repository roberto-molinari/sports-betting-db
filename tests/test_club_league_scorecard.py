"""Tests for club_league_scorecard.py's pure reporting logic (2026-08-21,
docs/PRE-AND-POST-MATCHDAY-EXPERIENCE.md) -- refresh_results()'s actual
subprocess calls aren't tested here (it shells out to live-data import
scripts), but its call to import_fixtures() IS -- see test_refresh_results_
applies_newly_completed_matches below (BUGS.md, 2026-08-23: refresh was
silently detect-only, never applying a newly completed match's score)."""
from unittest.mock import patch

import pytest

import core.sports_db as sports_db
from club_league_scorecard import (scorecard, _pick_stats, _pick_profit, _biggest_winner,
                                   refresh_results, print_scorecard, print_scorecard_post_friendly)
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
    extra = [("Premier League", "AWAY", -110, "loss", "poisson_v4_4", "PL Home", "PL Away")]
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

    print_scorecard("test", card)
    out = capsys.readouterr().out
    assert "WARNING" in out
    assert "poisson_v4_4" in out and "poisson_v4_5" in out


def test_refresh_results_applies_newly_completed_matches(db_path, conn):
    """refresh_results() must call import_fixtures() with allow_overwrite=True
    -- the whole point of a "refresh" is to apply a match's real, newly-
    available score, not just detect and report it (import_league_matches.py's
    own default is report-only, meant for a human-reviewed one-off run).
    Regression test for the exact bug: a scorecard run left 15 picks stuck
    ungraded because this was missed."""
    with patch("core.sports_db.DATABASE_PATH", db_path):
        home = sports_db.ensure_soccer_team("Refresh Home", "Serie A")
        away = sports_db.ensure_soccer_team("Refresh Away", "Serie A")
        sports_db.add_soccer_match("Serie A", 2025, home, away, "2025-09-22T18:00:00.000Z")

    start, end = matchday_utc_window("2025-09-22")

    with patch("club_league_scorecard.import_fixtures") as mock_import:
        refresh_results(conn, start, end)

    mock_import.assert_called_once_with("Serie A", 2025, allow_overwrite=True)


# ── Biggest winner (2026-08-23 -- "the day's biggest winner" by ROI) ──────────

def test_pick_profit_win_loss_push():
    assert _pick_profit(+150, "win") == pytest.approx(1.5)
    assert _pick_profit(-110, "loss") == pytest.approx(-1.0)
    assert _pick_profit(-110, "push") == 0.0


def test_biggest_winner_picks_the_highest_profit_win():
    """Profit == ROI here (flat 1-unit stake), so the highest-profit win IS
    the highest-ROI pick -- a big underdog win must beat a small-favorite win
    even though the favorite might have a "safer" record elsewhere."""
    picks = [
        {"league": "Serie A", "side": "HOME", "odds": -535, "result": "win",
         "home": "Inter", "away": "Monza", "profit": _pick_profit(-535, "win")},
        {"league": "Premier League", "side": "AWAY", "odds": +221, "result": "win",
         "home": "Nottingham Forest", "away": "Leeds United", "profit": _pick_profit(221, "win")},
        {"league": "La Liga", "side": "HOME", "odds": +144, "result": "loss",
         "home": "Valencia", "away": "Celta Vigo", "profit": _pick_profit(144, "loss")},
    ]
    winner = _biggest_winner(picks)
    assert winner["home"] == "Nottingham Forest" and winner["away"] == "Leeds United"
    assert winner["profit"] == pytest.approx(2.21)


def test_biggest_winner_ignores_losses_and_pushes():
    picks = [
        {"league": "Serie A", "side": "HOME", "odds": +500, "result": "loss",
         "home": "A", "away": "B", "profit": _pick_profit(500, "loss")},
        {"league": "Serie A", "side": "UNDER 2.5", "odds": -110, "result": "push",
         "home": "C", "away": "D", "profit": _pick_profit(-110, "push")},
    ]
    assert _biggest_winner(picks) is None


def test_biggest_winner_none_when_no_picks_at_all():
    assert _biggest_winner([]) is None


def test_scorecard_reports_biggest_winner_across_leagues(db_path, conn):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        _seed_graded_pick(conn, "Serie A", "HOME", "win", -535)       # small win
        _seed_graded_pick(conn, "Premier League", "AWAY", "win", +221)  # bigger win
        _seed_graded_pick(conn, "La Liga", "HOME", "loss", +144)

    start, end = matchday_utc_window("2025-09-22")
    card = scorecard(conn, start, end)

    bw = card["biggest_winner"]
    assert bw is not None
    assert bw["league"] == "Premier League"
    assert bw["odds"] == 221


def test_scorecard_biggest_winner_none_when_nothing_won(db_path, conn):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        _seed_graded_pick(conn, "Serie A", "HOME", "loss", +150)

    start, end = matchday_utc_window("2025-09-22")
    card = scorecard(conn, start, end)
    assert card["biggest_winner"] is None


def test_print_scorecard_shows_biggest_winner(db_path, conn, capsys):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        _seed_graded_pick(conn, "Serie A", "HOME", "win", -535)

    start, end = matchday_utc_window("2025-09-22")
    card = scorecard(conn, start, end)
    print_scorecard("test", card)
    out = capsys.readouterr().out
    assert "Biggest winner" in out
    assert "Serie A Home" in out and "Serie A Away" in out


def test_print_scorecard_no_winner_message(db_path, conn, capsys):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        _seed_graded_pick(conn, "Serie A", "HOME", "loss", +150)

    start, end = matchday_utc_window("2025-09-22")
    card = scorecard(conn, start, end)
    print_scorecard("test", card)
    out = capsys.readouterr().out
    assert "no picks won" in out


def test_print_scorecard_post_friendly_format(db_path, conn, capsys):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        _seed_graded_pick(conn, "Serie A", "HOME", "win", -535)
        _seed_graded_pick(conn, "Premier League", "AWAY", "loss", +150)

    start, end = matchday_utc_window("2025-09-22")
    card = scorecard(conn, start, end)
    print_scorecard_post_friendly("matchday 2025-09-22", card)
    out = capsys.readouterr().out

    assert "Results" in out
    assert "Serie A: 1-0-0" in out
    assert "Premier League: 0-1-0" in out
    assert "Overall: 1-1-0" in out
    assert "Biggest winner" in out
    assert "Serie A Home" in out and "Serie A Away" in out


def test_print_scorecard_post_friendly_no_winner(db_path, conn, capsys):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        _seed_graded_pick(conn, "Serie A", "HOME", "loss", +150)

    start, end = matchday_utc_window("2025-09-22")
    card = scorecard(conn, start, end)
    print_scorecard_post_friendly("matchday 2025-09-22", card)
    out = capsys.readouterr().out
    assert "No winning picks today" in out
