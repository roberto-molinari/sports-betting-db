"""Tests for club_league_picks_report.py -- lists every stored club-league pick
for a matchday/range with pick/odds/EV, plus result/ROI once graded
(docs/PRE-AND-POST-MATCHDAY-EXPERIENCE.md, 2026-08-23). No existing tool did
this: generate_club_league_card.py only recomputes fresh picks (one league,
no stored result); club_league_scorecard.py only shows aggregated stats, not
individual picks."""
from unittest.mock import patch

import core.sports_db as sports_db
from club_league_picks_report import picks_in_window, group_by_league, print_report
from core.matchday import matchday_utc_window


def _seed_pick(conn, league, side, odds, result=None, match_date="2025-09-22T18:00:00.000Z",
               home="Home FC", away="Away FC", method="poisson_v4_4"):
    home_id = sports_db.ensure_soccer_team(f"{league} {home}", league)
    away_id = sports_db.ensure_soccer_team(f"{league} {away}", league)
    match_id = sports_db.add_soccer_match(league, 2025, home_id, away_id, match_date)
    pick_ids = sports_db.replace_club_league_picks_for_match(
        match_id=match_id, league=league, generated_at="2025-09-21T00:00:00Z",
        picks=[{"side": side, "odds": odds, "prob": 0.55, "ev": 0.05, "stars": 3}],
        method=method, conn=conn,
    )
    if result is not None:
        sports_db.set_club_league_pick_result(pick_ids[0], result, conn=conn)
    conn.commit()
    return pick_ids[0]


def test_pending_pick_has_no_result_or_profit(db_path, conn):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        _seed_pick(conn, "Serie A", "HOME", -110)

    start, end = matchday_utc_window("2025-09-22")
    by_league = group_by_league(picks_in_window(conn, start, end))

    pick = by_league["Serie A"][0]
    assert pick["result"] is None
    assert pick["profit"] is None


def test_graded_pick_has_result_and_profit(db_path, conn):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        _seed_pick(conn, "Serie A", "HOME", +150, result="win")

    start, end = matchday_utc_window("2025-09-22")
    by_league = group_by_league(picks_in_window(conn, start, end))

    pick = by_league["Serie A"][0]
    assert pick["result"] == "win"
    assert pick["profit"] == 1.5


def test_groups_by_league(db_path, conn):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        _seed_pick(conn, "Serie A", "HOME", -110)
        _seed_pick(conn, "Premier League", "AWAY", +120)

    start, end = matchday_utc_window("2025-09-22")
    by_league = group_by_league(picks_in_window(conn, start, end))

    assert set(by_league.keys()) == {"Serie A", "Premier League"}
    assert len(by_league["Serie A"]) == 1
    assert len(by_league["Premier League"]) == 1


def test_window_scoping_excludes_other_days(db_path, conn):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        _seed_pick(conn, "Serie A", "HOME", -110, match_date="2025-09-22T18:00:00.000Z")
        _seed_pick(conn, "Serie A", "HOME", -110, match_date="2025-09-25T18:00:00.000Z")

    start, end = matchday_utc_window("2025-09-22")
    by_league = group_by_league(picks_in_window(conn, start, end))
    assert len(by_league["Serie A"]) == 1


def test_print_report_shows_pending_and_graded(db_path, conn, capsys):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        _seed_pick(conn, "Serie A", "HOME", -110, result="win", home="Inter", away="Monza")
        _seed_pick(conn, "Premier League", "AWAY", +150, home="Ipswich", away="Sunderland")

    start, end = matchday_utc_window("2025-09-22")
    by_league = group_by_league(picks_in_window(conn, start, end))
    print_report("matchday 2025-09-22", by_league)
    out = capsys.readouterr().out

    assert "2 pick(s), 1 graded, 1 pending" in out
    assert "WIN" in out and "profit=" in out
    assert "PENDING" in out
    assert "Inter" in out and "Monza" in out
    assert "Ipswich" in out and "Sunderland" in out


def test_print_report_empty_window(capsys):
    print_report("matchday 2025-09-22", {})
    out = capsys.readouterr().out
    assert "No picks found" in out
