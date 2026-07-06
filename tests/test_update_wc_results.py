"""Tests for update_wc_results.py's record_result -- specifically that it now
derives the 'advanced' side (via core.grading.advancing_side) so a knockout
match's ADVANCE picks grade correctly through the single-match/CSV path, not
just via --grade-only. Previously this always passed advanced=None, which
raised on any stored ADVANCE pick."""

import sqlite3

import core.sports_db as sdb
import update_wc_results as uwr


def _seed_knockout_with_advance_pick(db_path, advance_side):
    home = sdb.ensure_wc_team("Homeland")
    away = sdb.ensure_wc_team("Awayland")
    match_id = sdb.ensure_wc_match("2026-07-05 18:00:00", home, away, stage="R16")
    sdb.upsert_wc_odds(
        match_id=match_id, sportsbook="Test", odds_date="2026-07-04",
        home_moneyline=-150, draw_moneyline=280, away_moneyline=350,
        over_under=2.5, over_odds=-110, under_odds=-110,
        home_advance_ml=-140, away_advance_ml=115,
    )
    sdb.replace_wc_pick(
        match_id=match_id, generated_at="2026-07-04T00:00:00Z",
        side=advance_side, odds=-140 if advance_side == "HOME ADVANCE" else 115,
        model_prob=0.6, ev=0.05, stars=1, selection_mode="prediction",
    )
    return match_id


def test_record_result_grades_advance_pick_decided_in_regulation(db_path, monkeypatch):
    """A knockout tie decided in 90 minutes (no draw) must grade its stored
    ADVANCE pick via the single-match path, not raise -- this is exactly
    today's Brazil/Norway and Mexico/England case."""
    monkeypatch.setattr(uwr, "DATABASE_PATH", db_path)
    match_id = _seed_knockout_with_advance_pick(db_path, "HOME ADVANCE")
    conn = sqlite3.connect(db_path)
    try:
        graded = uwr.record_result(conn, match_id, home_score=2, away_score=1)
        conn.commit()
    finally:
        conn.close()
    assert graded == 1
    result = sqlite3.connect(db_path).execute(
        "SELECT result FROM soccer_wc_picks WHERE match_id = ?", (match_id,)).fetchone()[0]
    assert result == "win"


def test_record_result_grades_advance_loss_correctly(db_path, monkeypatch):
    """The flip side: backing the team that does NOT advance grades a loss."""
    monkeypatch.setattr(uwr, "DATABASE_PATH", db_path)
    match_id = _seed_knockout_with_advance_pick(db_path, "HOME ADVANCE")
    conn = sqlite3.connect(db_path)
    try:
        uwr.record_result(conn, match_id, home_score=1, away_score=2)
        conn.commit()
    finally:
        conn.close()
    result = sqlite3.connect(db_path).execute(
        "SELECT result FROM soccer_wc_picks WHERE match_id = ?", (match_id,)).fetchone()[0]
    assert result == "loss"


def test_grade_only_derives_advanced_from_stored_et_shootout(db_path, monkeypatch):
    """--grade-only's re-grade loop must also settle ADVANCE picks for ties that
    went to extra time/penalties, using the ET/shootout columns already on
    soccer_wc_matches (set via set_wc_match_advance_result)."""
    monkeypatch.setattr(uwr, "DATABASE_PATH", db_path)
    match_id = _seed_knockout_with_advance_pick(db_path, "AWAY ADVANCE")
    sdb.set_wc_match_advance_result(
        match_id, regulation_home=1, regulation_away=1,
        extra_time_home=1, extra_time_away=1,
        shootout_home=3, shootout_away=4, decided_by="shootout")

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("""SELECT match_id, home_score, away_score,
                              extra_time_home_score, extra_time_away_score,
                              shootout_home_score, shootout_away_score
                       FROM soccer_wc_matches
                       WHERE match_status = 'completed'
                         AND home_score IS NOT NULL AND away_score IS NOT NULL""")
        for mid, hs, as_, eth, eta, sh, sa in cur.fetchall():
            advanced = uwr.advancing_side(hs, as_, eth, eta, sh, sa)
            uwr.grade_match_picks(conn, mid, hs, as_, advanced)
        conn.commit()
    finally:
        conn.close()

    result = sqlite3.connect(db_path).execute(
        "SELECT result FROM soccer_wc_picks WHERE match_id = ?", (match_id,)).fetchone()[0]
    assert result == "win"   # away won the shootout 4-3
