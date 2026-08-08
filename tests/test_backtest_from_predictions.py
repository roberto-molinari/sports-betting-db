"""Tests for backtest_from_predictions.py -- ROI backtesting sourced from stored
soccer_model_predictions rows. run_totals() (2026-08-07) is the first coverage this
script has had at all: the model already computed p_over/p_under and it was already
stored, but nothing graded whether those picks actually won, so there was no way to
validate the over/under market the way 1x2 has been validated all along."""

from datetime import datetime, timezone

import pytest

import core.sports_db as sports_db
import backtest_from_predictions as bfp
from core.poisson_model import american_to_decimal

LEAGUE = "Serie A"
SEASON = 2025
METHOD = "test_method"


def _seed_match(conn, home_score, away_score, over_under=2.5,
                p_home=0.5, p_draw=0.25, p_away=0.25, p_over=0.5, p_under=0.5,
                home_moneyline=-150, draw_moneyline=250, away_moneyline=400,
                over_odds=-110, under_odds=-110, sportsbook="Bet365"):
    home = sports_db.ensure_soccer_team("Home", LEAGUE)
    away = sports_db.ensure_soccer_team("Away", LEAGUE)
    match_id = sports_db.add_soccer_match(LEAGUE, SEASON, home, away, "2025-09-01")
    sports_db.update_soccer_match_result(match_id, home_score, away_score)
    sports_db.add_soccer_betting_odds(
        match_id=match_id, sportsbook=sportsbook, odds_date="2025-08-30",
        home_moneyline=home_moneyline, draw_moneyline=draw_moneyline, away_moneyline=away_moneyline,
        over_under=over_under, over_odds=over_odds, under_odds=under_odds,
    )
    sports_db.add_soccer_model_prediction(
        match_id=match_id, league=LEAGUE, match_date="2025-09-01",
        generated_at=datetime.now(timezone.utc).isoformat(), method=METHOD,
        p_home=p_home, p_draw=p_draw, p_away=p_away,
        over_under_line=over_under, p_over=p_over, p_under=p_under,
        home_moneyline=home_moneyline, draw_moneyline=draw_moneyline, away_moneyline=away_moneyline,
        over_odds=over_odds, under_odds=under_odds, conn=conn,
    )
    return match_id


def test_run_totals_over_win_counted_correctly(db_path, conn):
    # 3 total goals > 2.5 line -- an OVER bet wins. Both sides clear the EV floor
    # here (threshold=-1.0 lets everything through) -- p_model only gates WHICH
    # bets get placed, not the profit amount, which is odds-only.
    _seed_match(conn, home_score=2, away_score=1, over_under=2.5, p_over=0.6, p_under=0.4)
    roi, bets = bfp.run_totals(conn, LEAGUE, SEASON, METHOD, ev_threshold=-1.0)
    assert bets == 2
    win_profit = american_to_decimal(-110) - 1   # over wins
    lose_profit = -1.0                            # under loses
    assert roi == pytest.approx((win_profit + lose_profit) / 2)


def test_run_totals_under_win_counted_correctly(db_path, conn):
    # 1 total goal < 2.5 line -- an UNDER bet wins this time instead. Same odds
    # (-110/-110) means the ROI shape is identical to the over-win case above --
    # this test exists to prove the ACTUAL/line comparison correctly flips which
    # side is graded as the winner, not to expect a different ROI number.
    _seed_match(conn, home_score=1, away_score=0, over_under=2.5, p_over=0.4, p_under=0.6)
    roi, bets = bfp.run_totals(conn, LEAGUE, SEASON, METHOD, ev_threshold=-1.0)
    assert bets == 2
    win_profit = american_to_decimal(-110) - 1   # under wins
    lose_profit = -1.0                            # over loses
    assert roi == pytest.approx((win_profit + lose_profit) / 2)


def test_run_totals_ev_threshold_filters_negative_ev(db_path, conn):
    _seed_match(conn, home_score=1, away_score=0, over_under=2.5, p_over=0.3, p_under=0.7)
    # EV threshold above both sides' EV -- nothing should clear it.
    roi, bets = bfp.run_totals(conn, LEAGUE, SEASON, METHOD, ev_threshold=10.0)
    assert bets == 0
    assert roi == 0.0


def test_run_totals_is_separate_from_1x2_run(db_path, conn):
    """1x2 and totals must never be pooled into the same staked/profit numbers --
    every existing ROI reference point in BUGS.md is 1x2-only."""
    _seed_match(conn, home_score=2, away_score=1, over_under=2.5,
               p_home=0.6, p_draw=0.25, p_away=0.15, p_over=0.6, p_under=0.4)
    roi_1x2, bets_1x2 = bfp.run(conn, LEAGUE, SEASON, METHOD, ev_threshold=-1.0)
    roi_totals, bets_totals = bfp.run_totals(conn, LEAGUE, SEASON, METHOD, ev_threshold=-1.0)
    assert bets_1x2 == 3     # home, draw, away all clear a -100% EV floor
    assert bets_totals == 2  # over, under
    assert roi_1x2 != roi_totals


def test_run_totals_skips_rows_with_no_over_under_line(db_path, conn):
    """A match with no posted totals line (over_under IS NULL) must be excluded
    from the totals report entirely, not treated as a push or a loss."""
    home = sports_db.ensure_soccer_team("Home", LEAGUE)
    away = sports_db.ensure_soccer_team("Away", LEAGUE)
    match_id = sports_db.add_soccer_match(LEAGUE, SEASON, home, away, "2025-09-01")
    sports_db.update_soccer_match_result(match_id, 1, 1)
    sports_db.add_soccer_betting_odds(
        match_id=match_id, sportsbook="Bet365", odds_date="2025-08-30",
        home_moneyline=-150, draw_moneyline=250, away_moneyline=400,
    )
    sports_db.add_soccer_model_prediction(
        match_id=match_id, league=LEAGUE, match_date="2025-09-01",
        generated_at=datetime.now(timezone.utc).isoformat(), method=METHOD,
        p_home=0.5, p_draw=0.25, p_away=0.25, p_over=None, p_under=None,
        conn=conn,
    )
    roi, bets = bfp.run_totals(conn, LEAGUE, SEASON, METHOD, ev_threshold=-1.0)
    assert bets == 0
