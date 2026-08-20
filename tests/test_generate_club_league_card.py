"""Tests for generate_club_league_card.py -- the club-league card generator that
replaced generate_serie_a_card.py 2026-08-07 (see BUGS.md): parameterized by
--league instead of hardcoded to Serie A, running the real FEATURE-011 player-blend
pipeline instead of the old team-only model, and applying the floor guardrail that
had never been ported from generate_wc_card.py."""

from datetime import datetime, timezone
from unittest.mock import patch

import core.sports_db as sports_db
import generate_club_league_card as gclc


def _seed_lopsided_league(conn, league="Serie A", season=2025):
    """A strong team and a weak team with enough match history to clear
    TEAM_RATING_MIN_MATCHES_TO_TRUST_TEAM_RATING_OVER_LEAGUE_AVERAGE, then a new
    (unplayed) match between them with betting odds -- realistic enough for
    compute()/analyse_match_wc() to produce genuinely different win probabilities
    for the two sides, not just fall back to the league average."""
    strong = sports_db.ensure_soccer_team("StrongFC", league)
    weak = sports_db.ensure_soccer_team("WeakFC", league)
    striker_s = sports_db.add_player(strong, "StrongStriker", position="F", conn=conn)
    striker_w = sports_db.add_player(weak, "WeakStriker", position="F", conn=conn)

    for i, date in enumerate(("2025-09-01", "2025-09-08", "2025-09-15")):
        opp_s = sports_db.ensure_soccer_team(f"OppS{i}", league)
        opp_w = sports_db.ensure_soccer_team(f"OppW{i}", league)
        m_s = sports_db.add_soccer_match(league, season, strong, opp_s, date)
        m_w = sports_db.add_soccer_match(league, season, weak, opp_w, date)
        sports_db.update_soccer_match_result(m_s, 3, 0)
        sports_db.update_soccer_match_result(m_w, 0, 2)
        sports_db.add_player_match_stats(striker_s, m_s, season=season, venue="home",
                                         minutes_played=90, xg=3.0, conn=conn)
        sports_db.add_player_match_stats(striker_w, m_w, season=season, venue="home",
                                         minutes_played=90, xg=0.2, conn=conn)

    # away_moneyline=+2000 (implied ~4.8%) is well below WeakFC's actual model
    # probability (~7%, computed from this seed) -- positive EV, but the model
    # probability itself is still under the floor, the exact "confident-looking EV
    # is actually noise" case the guardrail exists for (BUG-003's original
    # diagnosis).
    match_id = sports_db.add_soccer_match(league, season, strong, weak, "2025-09-22")
    sports_db.add_soccer_betting_odds(
        match_id=match_id, sportsbook="Test", odds_date="2025-09-20",
        home_moneyline=-350, draw_moneyline=450, away_moneyline=2000,
        over_under=2.5, over_odds=-110, under_odds=-110,
    )
    return match_id


def _run_card(capsys, db_path, days_ahead=(1, 4), now=datetime(2025, 9, 21, tzinfo=timezone.utc), league="Serie A",
              dry_run=False):
    # generate_club_league_card does `from core.sports_db import DATABASE_PATH`,
    # binding its own copy at import time -- the db_path fixture only patches
    # core.sports_db's and core.poisson_model's copies (see conftest.py's
    # docstring), so this module's copy needs patching too or it silently hits
    # the real production DB instead of the isolated test one.
    with patch("generate_club_league_card.DATABASE_PATH", db_path), \
         patch("generate_club_league_card.datetime") as mock_dt:
        mock_dt.now.return_value = now
        import sys
        old_argv = sys.argv
        sys.argv = ["generate_club_league_card.py", "--league", league,
                    "--days-ahead", str(days_ahead[0]), str(days_ahead[1])]
        if dry_run:
            sys.argv.append("--dry-run")
        try:
            gclc.main()
        finally:
            sys.argv = old_argv
    return capsys.readouterr().out


# ── build_candidates: pure unit tests ─────────────────────────────────────────────

def test_build_candidates_skips_sides_with_no_odds():
    match = {"match_id": 1, "match_date": "2025-09-22", "home": "A", "away": "B",
             "home_moneyline": -200, "draw_moneyline": None, "away_moneyline": 300,
             "over_odds": None, "under_odds": None}
    result = {"p_home": 0.6, "p_draw": 0.25, "p_away": 0.15,
              "ev_home": 0.1, "ev_draw": None, "ev_away": 0.05}
    candidates = gclc.build_candidates(match, result)
    sides = {c["side"] for c in candidates}
    assert sides == {"HOME", "AWAY"}


def test_build_candidates_includes_over_under_when_priced():
    match = {"match_id": 1, "match_date": "2025-09-22", "home": "A", "away": "B",
             "home_moneyline": -200, "draw_moneyline": 300, "away_moneyline": 300,
             "over_odds": -110, "under_odds": -110}
    result = {"p_home": 0.6, "p_draw": 0.2, "p_away": 0.2,
              "ev_home": 0.1, "ev_draw": 0.0, "ev_away": 0.0,
              "p_over": 0.55, "p_under": 0.45, "ev_over": 0.05, "ev_under": -0.05}
    candidates = gclc.build_candidates(match, result)
    sides = {c["side"] for c in candidates}
    assert "OVER 2.5" in sides and "UNDER 2.5" in sides


# ── end-to-end: real pipeline + guardrail ──────────────────────────────────────────

def test_card_uses_league_param_not_hardcoded(db_path, conn, capsys):
    _seed_lopsided_league(conn, league="Bundesliga")
    out = _run_card(capsys, db_path, league="Bundesliga")
    assert "LEAGUE Bundesliga" in out
    assert "MATCHES 1" in out

    out_wrong_league = _run_card(capsys, db_path, league="Serie A")
    assert "MATCHES 0" in out_wrong_league


def test_card_only_surfaces_positive_ev_guardrail_clear_picks(db_path, conn, capsys):
    _seed_lopsided_league(conn)
    out = _run_card(capsys, db_path)

    # Every printed pick line must be a real (guardrail-clear, positive-EV) pick --
    # spot check the format is present and the away longshot doesn't silently
    # replace a legitimate guardrail-driven exclusion.
    assert "StrongFC vs WeakFC" in out
    assert "TOP PICKS PER MATCH" in out


def test_card_excludes_subfloor_candidate_and_logs_it(db_path, conn, capsys):
    """WeakFC's away win probability against a much stronger home side lands under
    CLUB_LEAGUE_MIN_PICK_PROBABILITY (~7% modeled, well under the 25% floor) while
    the +2000 market odds still make it positive-EV -- the exact "confident-looking
    EV is actually noise" case BUG-003's guardrail exists for. Must show up in the
    GUARDRAIL LOG, never as a printed pick."""
    _seed_lopsided_league(conn)
    out = _run_card(capsys, db_path)

    assert "GUARDRAIL LOG" in out
    log_section = out.split("GUARDRAIL LOG")[1]
    picks_section = out.split("GUARDRAIL LOG")[0]
    assert "AWAY" in log_section and "floor" in log_section
    assert "AWAY" not in picks_section


# ── FEATURE-016: picks persisted for later scoring ─────────────────────────────────

def test_card_stores_picks_by_default(db_path, conn, capsys):
    _seed_lopsided_league(conn)
    out = _run_card(capsys, db_path)
    assert "stored in soccer_club_league_picks" in out
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM soccer_club_league_picks")
    assert cur.fetchone()[0] > 0


def test_card_dry_run_does_not_store(db_path, conn, capsys):
    _seed_lopsided_league(conn)
    out = _run_card(capsys, db_path, dry_run=True)
    assert "dry-run, not stored" in out
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM soccer_club_league_picks")
    assert cur.fetchone()[0] == 0


def test_card_rerun_replaces_ungraded_picks_not_stack(db_path, conn, capsys):
    _seed_lopsided_league(conn)
    _run_card(capsys, db_path)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM soccer_club_league_picks")
    first_count = cur.fetchone()[0]
    assert first_count > 0

    _run_card(capsys, db_path)   # re-run, same inputs -- must replace, not double
    cur.execute("SELECT COUNT(*) FROM soccer_club_league_picks")
    assert cur.fetchone()[0] == first_count
