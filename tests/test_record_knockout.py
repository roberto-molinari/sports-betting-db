"""Tests for record_knockout — parsing helpers and the full record() flow."""

import pytest

import record_knockout as rk
import core.sports_db as sdb


# ── parsing helpers ──────────────────────────────────────────────────────────

def test_parse_score():
    assert rk.parse_score("2-1") == (2, 1)
    assert rk.parse_score("") == (None, None)
    assert rk.parse_score(None) == (None, None)


def test_parse_score_bad():
    with pytest.raises(SystemExit):
        rk.parse_score("banana")


def test_parse_pk():
    assert rk.parse_pk("home:Neymar:goal", 1, 2) == (1, "Neymar", "goal")
    assert rk.parse_pk("away:Modric:saved", 1, 2) == (2, "Modric", "saved")


def test_parse_pk_bad_result():
    with pytest.raises(SystemExit):
        rk.parse_pk("home:X:hitpost", 1, 2)


def test_parse_pk_bad_side():
    with pytest.raises(SystemExit):
        rk.parse_pk("bench:X:goal", 1, 2)


def test_parse_et_goal():
    assert rk.parse_et_goal("home:Vini:98", 1, 2) == (1, "Vini", 98)


def test_parse_et_goal_bad_minute():
    with pytest.raises(SystemExit):
        rk.parse_et_goal("home:Vini:late", 1, 2)


@pytest.mark.parametrize("et,shootout,override,expected", [
    (None, None, None, "regulation"),
    (2, None, None, "extra_time"),
    (2, 4, None, "shootout"),
    (None, None, "shootout", "shootout"),   # explicit override wins
])
def test_infer_decided_by(et, shootout, override, expected):
    assert rk.infer_decided_by(et, shootout, override) == expected


# ── record() integration ─────────────────────────────────────────────────────

def test_record_shootout_grades_advance_and_stores_events(db_path, conn):
    home = sdb.ensure_wc_team("Brazil")
    away = sdb.ensure_wc_team("Croatia")
    match_id = sdb.ensure_wc_match("2026-06-30 18:00:00", home, away, stage="R32")
    # An ADVANCE bet (should win), a 90' HOME bet (1-1 draw -> loses), and an
    # OVER 2.5 bet (2 goals in 90' -> loses): proves 1X2/O-U settle on regulation.
    adv_pick = sdb.add_wc_pick(match_id, "2026-06-29T12:00:00", "HOME ADVANCE",
                               odds=-150, model_prob=0.7, ev=0.1, stars=2)
    home_pick = sdb.add_wc_pick(match_id, "2026-06-29T12:00:00", "HOME",
                                odds=120, model_prob=0.4, ev=0.05, stars=1)
    over_pick = sdb.add_wc_pick(match_id, "2026-06-29T12:00:00", "OVER 2.5",
                                odds=-110, model_prob=0.5, ev=0.0, stars=1)

    match = (match_id, home, away, "Brazil", "Croatia")
    summary = rk.record(conn, match, reg=(1, 1), extra_time=(2, 2), shootout=(4, 3),
                        decided_by="shootout",
                        pk_specs=["home:Neymar:goal", "away:Modric:saved"],
                        et_goal_specs=["home:Vinicius:98"])

    assert summary["advanced"] == "HOME"
    assert summary["advanced_name"] == "Brazil"
    assert summary["graded_picks"] == 3

    res = {pid: result for pid, result in conn.execute(
        "SELECT pick_id, result FROM soccer_wc_picks WHERE match_id = ?", (match_id,))}
    assert res[adv_pick] == "win"     # Brazil advanced via the shootout
    assert res[home_pick] == "loss"   # 1-1 in regulation
    assert res[over_pick] == "loss"   # total 2 < 2.5 in regulation

    assert conn.execute("SELECT COUNT(*) FROM soccer_penalty_kicks WHERE match_id = ?",
                        (match_id,)).fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM soccer_extra_time_goals WHERE match_id = ?",
                        (match_id,)).fetchone()[0] == 1

    row = conn.execute("""SELECT home_score, away_score, extra_time_home_score,
                                 shootout_home_score, decided_by, match_status
                          FROM soccer_wc_matches WHERE match_id = ?""", (match_id,)).fetchone()
    assert row == (1, 1, 2, 4, "shootout", "completed")


def test_record_regulation_advance_is_winner(db_path, conn):
    home = sdb.ensure_wc_team("Argentina")
    away = sdb.ensure_wc_team("Chile")
    match_id = sdb.ensure_wc_match("2026-07-01 18:00:00", home, away, stage="R32")
    pick = sdb.add_wc_pick(match_id, "2026-06-30T12:00:00", "AWAY ADVANCE",
                           odds=200, model_prob=0.35, ev=0.05, stars=1)
    match = (match_id, home, away, "Argentina", "Chile")
    summary = rk.record(conn, match, reg=(0, 2), extra_time=(None, None),
                        shootout=(None, None), decided_by="regulation",
                        pk_specs=[], et_goal_specs=[])
    assert summary["advanced"] == "AWAY"
    result = conn.execute("SELECT result FROM soccer_wc_picks WHERE pick_id = ?",
                          (pick,)).fetchone()[0]
    assert result == "win"   # Chile won in regulation -> AWAY ADVANCE wins
