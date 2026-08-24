"""Tests for grade_club_league_picks.py -- grading stored club-league picks
once their match has a final score (docs/PRE-AND-POST-MATCHDAY-EXPERIENCE.md,
2026-08-21). Reuses core.grading.grade_pick(), so only the wiring (which picks
get graded, date-window scoping, pending-count reporting) needs testing here."""
from unittest.mock import patch

import core.sports_db as sports_db
from grade_club_league_picks import grade_picks_in_window
from core.matchday import matchday_utc_window


def _seed_pick(conn, side, home_score=None, away_score=None, match_date="2025-09-22T18:00:00.000Z"):
    home = sports_db.ensure_soccer_team("Home FC", "Serie A")
    away = sports_db.ensure_soccer_team("Away FC", "Serie A")
    match_id = sports_db.add_soccer_match("Serie A", 2025, home, away, match_date)
    if home_score is not None:
        sports_db.update_soccer_match_result(match_id, home_score, away_score)
    pick_ids = sports_db.replace_club_league_picks_for_match(
        match_id=match_id, league="Serie A", generated_at="2025-09-21T00:00:00Z",
        picks=[{"side": side, "odds": -110, "prob": 0.55, "ev": 0.05, "stars": 3}],
        conn=conn,
    )
    conn.commit()
    return pick_ids[0]


def test_grades_completed_match_and_leaves_incomplete_ones_pending(db_path, conn):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        won_pick = _seed_pick(conn, "HOME", home_score=2, away_score=0)
        pending_pick = _seed_pick(conn, "AWAY")  # no score yet

    graded, pending, _ = grade_picks_in_window(conn)
    assert graded == 1
    assert pending == 1

    cur = conn.cursor()
    cur.execute("SELECT result FROM soccer_club_league_picks WHERE pick_id = ?", (won_pick,))
    assert cur.fetchone()[0] == "win"
    cur.execute("SELECT result FROM soccer_club_league_picks WHERE pick_id = ?", (pending_pick,))
    assert cur.fetchone()[0] is None


def test_totals_push_grades_as_push(db_path, conn):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        # OVER 2 with a 1-1 result (total=2) is a push at an integer line.
        pick_id = _seed_pick(conn, "OVER 2", home_score=1, away_score=1)

    grade_picks_in_window(conn)
    cur = conn.cursor()
    cur.execute("SELECT result FROM soccer_club_league_picks WHERE pick_id = ?", (pick_id,))
    assert cur.fetchone()[0] == "push"


def test_already_graded_pick_is_not_touched_again(db_path, conn):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        _seed_pick(conn, "HOME", home_score=2, away_score=0)

    graded_first, _, _ = grade_picks_in_window(conn)
    assert graded_first == 1
    graded_second, _, _ = grade_picks_in_window(conn)
    assert graded_second == 0   # already has a result -- WHERE result IS NULL excludes it


def test_window_scoping_only_grades_picks_in_range(db_path, conn):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        in_window = _seed_pick(conn, "HOME", home_score=1, away_score=0,
                               match_date="2025-09-22T18:00:00.000Z")
        out_of_window = _seed_pick(conn, "HOME", home_score=1, away_score=0,
                                   match_date="2025-09-25T18:00:00.000Z")

    start, end = matchday_utc_window("2025-09-22")
    graded, pending, _ = grade_picks_in_window(conn, start, end)
    assert graded == 1
    assert pending == 0   # the out-of-window pick isn't "pending" -- it's out of scope

    cur = conn.cursor()
    cur.execute("SELECT result FROM soccer_club_league_picks WHERE pick_id = ?", (in_window,))
    assert cur.fetchone()[0] == "win"
    cur.execute("SELECT result FROM soccer_club_league_picks WHERE pick_id = ?", (out_of_window,))
    assert cur.fetchone()[0] is None


def test_dry_run_computes_but_does_not_persist(db_path, conn):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        pick_id = _seed_pick(conn, "HOME", home_score=2, away_score=0)

    graded, pending, details = grade_picks_in_window(conn, dry_run=True)
    assert graded == 1
    assert pending == 0
    assert details == [("Serie A", "HOME", -110, "win", None, "Home FC", "Away FC")]

    cur = conn.cursor()
    cur.execute("SELECT result FROM soccer_club_league_picks WHERE pick_id = ?", (pick_id,))
    assert cur.fetchone()[0] is None   # nothing written

    # a real (non-dry-run) pass afterward still finds it and grades it for real
    graded_real, _, _ = grade_picks_in_window(conn)
    assert graded_real == 1
    cur.execute("SELECT result FROM soccer_club_league_picks WHERE pick_id = ?", (pick_id,))
    assert cur.fetchone()[0] == "win"
