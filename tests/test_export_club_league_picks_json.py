"""Tests for export_club_league_picks_json.py -- the flat per-pick JSON export
backing the interactive ROI report page (2026-08-26). Only the pure
transform (graded_picks query + build_export) is tested here; file-writing
in main() is mechanical."""
from unittest.mock import patch

import core.sports_db as sports_db
from export_club_league_picks_json import graded_picks, build_export, market_for_side


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


def test_market_for_side():
    assert market_for_side("HOME") == "1x2"
    assert market_for_side("AWAY") == "1x2"
    assert market_for_side("DRAW") == "1x2"
    assert market_for_side("OVER 2.5") == "totals"
    assert market_for_side("UNDER 2.5") == "totals"


def test_graded_picks_excludes_pending(db_path, conn):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        _seed_pick(conn, "Serie A", "HOME", -110, result="win")
        _seed_pick(conn, "Serie A", "AWAY", +120)  # no result -- still pending

    rows = graded_picks(conn)
    assert len(rows) == 1
    assert rows[0][6] == "win"   # result column


def test_build_export_shape_and_profit(db_path, conn):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        _seed_pick(conn, "Serie A", "HOME", +150, result="win", home="Inter", away="Monza")

    export = build_export(graded_picks(conn))
    assert len(export) == 1
    pick = export[0]
    assert pick["league"] == "Serie A"
    assert pick["home"] == "Serie A Inter" and pick["away"] == "Serie A Monza"
    assert pick["market"] == "1x2"
    assert pick["side"] == "HOME"
    assert pick["odds"] == 150
    assert pick["result"] == "win"
    assert pick["profit"] == 1.5
    assert pick["method"] == "poisson_v4_4"
    assert pick["date"] == "2025-09-22"   # matchday, not raw kickoff timestamp


def test_build_export_push_has_zero_profit(db_path, conn):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        _seed_pick(conn, "Serie A", "UNDER 2.5", -110, result="push")

    export = build_export(graded_picks(conn))
    assert export[0]["profit"] == 0.0
    assert export[0]["market"] == "totals"


def test_build_export_uses_et_buffer_matchday_not_utc_date(db_path, conn):
    """A kickoff just after UTC midnight but still within the ET+buffer prior
    day must be labeled with that prior day -- same boundary every other
    tool in this repo uses (core.matchday)."""
    with patch("core.sports_db.DATABASE_PATH", db_path):
        # 01:00 UTC on the 23rd = 21:00 EDT on the 22nd -- ET matchday is the 22nd.
        _seed_pick(conn, "Serie A", "HOME", -110, result="win", match_date="2025-09-23T01:00:00.000Z")

    export = build_export(graded_picks(conn))
    assert export[0]["date"] == "2025-09-22"


def test_build_export_empty_when_no_graded_picks(db_path, conn):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        _seed_pick(conn, "Serie A", "HOME", -110)  # pending only

    assert build_export(graded_picks(conn)) == []
