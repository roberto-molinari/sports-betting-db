"""Tests for generate_wc_card pick selection (the longshot guardrail)."""

import sqlite3

import core.sports_db as sdb
import generate_wc_card as gwc


def _seed_longshot_game(db_path):
    """A clear favorite (home) vs a longshot underdog (away) priced at +2000, so
    the underdog has the highest EV purely from odds despite a tiny win prob."""
    fav = sdb.ensure_wc_team("Favoritia")
    dog = sdb.ensure_wc_team("Underdogia")
    match_id = sdb.ensure_wc_match("2026-06-20 18:00:00", fav, dog, stage="Group")
    sdb.upsert_wc_odds(
        match_id=match_id, sportsbook="Test", odds_date="2026-06-13",
        home_moneyline=-400, draw_moneyline=500, away_moneyline=2000,
        over_under=2.5, over_odds=-110, under_odds=-110,
    )
    sdb.set_wc_team_strength(fav, 2.0, 0.9)   # strong attack, mean defense
    sdb.set_wc_team_strength(dog, 0.7, 1.6)   # weak attack, leaky defense
    return match_id


def _fetch_match(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    matches = gwc.fetch_matches(conn, "2026-06-20", "2026-06-20")
    return conn, matches


def test_guardrail_demotes_sub_floor_longshot(db_path, monkeypatch):
    _seed_longshot_game(db_path)
    conn, matches = _fetch_match(db_path)
    assert len(matches) == 1
    match = matches[0]

    # With the floor disabled, the +2000 longshot wins on EV despite a tiny prob.
    monkeypatch.setattr(gwc, "MIN_PICK_PROBABILITY", 0.0)
    raw = gwc.best_pick_for_match(match, conn)
    assert raw["side"] == "AWAY"
    assert raw["prob"] < 0.25            # confirms it is a sub-floor longshot

    # With the 0.25 floor the chosen pick must clear the floor (any market).
    monkeypatch.setattr(gwc, "MIN_PICK_PROBABILITY", 0.25)
    guarded = gwc.best_pick_for_match(match, conn)
    assert guarded["side"] != "AWAY"
    assert guarded["prob"] >= 0.25
    conn.close()


def test_guardrail_demotes_sub_floor_draw(db_path, monkeypatch):
    """The floor applies to draws too: a longshot draw priced long enough to top
    the EV ranking is demoted, not surfaced."""
    fav = sdb.ensure_wc_team("Bigfav")
    dog = sdb.ensure_wc_team("Smalldog")
    match_id = sdb.ensure_wc_match("2026-06-21 18:00:00", fav, dog, stage="Group")
    sdb.upsert_wc_odds(
        match_id=match_id, sportsbook="Test", odds_date="2026-06-13",
        home_moneyline=-250, draw_moneyline=900, away_moneyline=600,
        over_under=2.5, over_odds=-200, under_odds=140,
    )
    sdb.set_wc_team_strength(fav, 2.0, 0.9)
    sdb.set_wc_team_strength(dog, 0.7, 1.6)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    match = gwc.fetch_matches(conn, "2026-06-21", "2026-06-21")[0]

    monkeypatch.setattr(gwc, "MIN_PICK_PROBABILITY", 0.0)
    raw = gwc.best_pick_for_match(match, conn)
    assert raw["side"] == "DRAW"
    assert raw["prob"] < 0.25            # a sub-floor longshot draw tops the EV

    monkeypatch.setattr(gwc, "MIN_PICK_PROBABILITY", 0.25)
    guarded = gwc.best_pick_for_match(match, conn)
    assert guarded["side"] != "DRAW"
    assert guarded["prob"] >= 0.25
    conn.close()


def test_guardrail_leaves_solid_picks_alone(db_path, monkeypatch):
    """A pick above the floor is untouched."""
    _seed_longshot_game(db_path)
    conn, matches = _fetch_match(db_path)
    match = matches[0]
    monkeypatch.setattr(gwc, "MIN_PICK_PROBABILITY", 0.01)
    pick = gwc.best_pick_for_match(match, conn)
    assert pick["side"] == "AWAY"
    conn.close()
