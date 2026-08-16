"""Unit tests for diagnose_home_bet_calibration pure helpers."""

import diagnose_home_bet_calibration as diag


def test_home_bet_row_rejects_non_positive_ev():
    # Fair-ish price: p=0.5 at +100 => EV=0, must not count as a bet.
    assert diag.home_bet_row(
        p_home=0.5, home_ml=100, home_score=1, away_score=0, floor=None,
    ) is None
    # Model below implied => negative EV.
    assert diag.home_bet_row(
        p_home=0.40, home_ml=-120, home_score=1, away_score=0, floor=None,
    ) is None


def test_home_bet_row_accepts_positive_ev_and_grades_win_loss():
    # p=0.60 at +100 => EV = 0.2 > 0
    won = diag.home_bet_row(
        p_home=0.60, home_ml=100, home_score=2, away_score=0,
        lambda_home=1.8, lambda_away=1.0, p_home_fair=0.50, floor=None,
    )
    lost = diag.home_bet_row(
        p_home=0.60, home_ml=100, home_score=0, away_score=1,
        lambda_home=1.8, lambda_away=1.0, p_home_fair=0.50, floor=None,
    )
    assert won is not None and lost is not None
    assert won["won"] is True and lost["won"] is False
    assert won["profit"] == 1.0 and lost["profit"] == -1.0
    assert abs(won["gap_soft"] - (0.60 - 0.50)) < 1e-9
    assert abs(won["gap_bf"] - 0.10) < 1e-9
    assert abs(won["lambda_diff"] - 0.8) < 1e-9


def test_home_bet_row_floor_rejects_sub_floor_probability():
    # Longshot: high EV from long odds but p below floor.
    row = diag.home_bet_row(
        p_home=0.20, home_ml=400, home_score=0, away_score=1, floor=0.25,
    )
    assert row is None
    ok = diag.home_bet_row(
        p_home=0.30, home_ml=400, home_score=0, away_score=1, floor=0.25,
    )
    assert ok is not None


def test_calib_error_and_roi():
    bets = [
        {"p_home": 0.60, "won": True, "profit": 1.0},
        {"p_home": 0.60, "won": False, "profit": -1.0},
    ]
    # mean p 0.60, wr 0.50 => calib +0.10; ROI 0
    assert abs(diag.calib_error(bets) - 0.10) < 1e-9
    assert abs(diag.roi(bets) - 0.0) < 1e-9


def test_summarize_and_buckets():
    bets = []
    for i in range(10):
        bets.append({
            "p_home": 0.5 + i * 0.01,
            "won": i % 2 == 0,
            "profit": 1.0 if i % 2 == 0 else -1.0,
            "gap_soft": 0.01 * i,
            "gap_bf": 0.02 * i if i < 8 else None,
            "lambda_diff": 0.1 * i,
            "lambda_home": 1.5,
            "lambda_away": 1.0,
        })
    s = diag.summarize_bets(bets)
    assert s["n"] == 10
    assert s["calib"] == s["calib"]  # not nan
    assert diag.gap_bf_bucket(0.03) == "gap_bf<=0.05"
    assert diag.gap_bf_bucket(0.12) == "gap_bf 0.10-0.15"
    assert diag.gap_bf_bucket(None) == "no_sharp"
    assert diag.lambda_diff_bucket(0.7) == "diff>=0.6"
    assert diag.lambda_diff_bucket(-0.4) == "diff<-0.3"


def test_quintile_slices_balanced():
    bets = [{"gap_soft": float(i), "p_home": 0.5, "won": False, "profit": -1.0}
            for i in range(25)]
    slices = diag.quintile_slices(bets, "gap_soft")
    assert len(slices) == 5
    assert sum(len(c) for _, c in slices) == 25


def test_rank_deep_dive_prefers_gap_bf():
    bets = [
        {"gap_bf": 0.05, "gap_soft": 0.40, "p_home": 0.5},
        {"gap_bf": 0.30, "gap_soft": 0.10, "p_home": 0.5},
        {"gap_bf": None, "gap_soft": 0.50, "p_home": 0.5},
    ]
    ranked = diag.rank_deep_dive_candidates(bets, 3)
    # 0.50 (soft fallback) > 0.30 > 0.05
    assert ranked[0]["gap_soft"] == 0.50
    assert ranked[1]["gap_bf"] == 0.30
    assert ranked[2]["gap_bf"] == 0.05


def test_rank_control_prefers_smallest_abs_gap_bf():
    bets = [
        {"match_id": 1, "gap_bf": 0.20, "gap_soft": 0.01, "p_home": 0.5},
        {"match_id": 2, "gap_bf": 0.02, "gap_soft": 0.30, "p_home": 0.5},
        {"match_id": 3, "gap_bf": None, "gap_soft": 0.00, "p_home": 0.5},
        {"match_id": 4, "gap_bf": -0.01, "gap_soft": 0.40, "p_home": 0.5},
    ]
    ranked = diag.rank_control_candidates(bets, 3)
    # | -0.01 | < 0.00 (soft) < 0.02  — wait: abs gap_bf -0.01 = 0.01,
    # soft fallback abs 0.00 is smaller, then 0.01, then 0.02
    assert ranked[0]["match_id"] == 3
    assert ranked[1]["match_id"] == 4
    assert ranked[2]["match_id"] == 2


def test_classify_driver_pattern_a_b_mixed():
    # High team weight => A
    assert diag.classify_driver_pattern({
        "home_w_att": 0.92,
        "home_att_player": 1.5,
        "home_att_team": 1.6,
    }) == "A"
    # Low weight + player attack clearly above team => B
    assert diag.classify_driver_pattern({
        "home_w_att": 0.10,
        "home_att_player": 2.2,
        "home_att_team": 1.5,
    }) == "B"
    # Low weight but no player lift => MIXED (not B)
    assert diag.classify_driver_pattern({
        "home_w_att": 0.20,
        "home_att_player": 1.4,
        "home_att_team": 1.5,
    }) == "MIXED"
    # Mid weight => MIXED even with lift
    assert diag.classify_driver_pattern({
        "home_w_att": 0.60,
        "home_att_player": 2.0,
        "home_att_team": 1.5,
    }) == "MIXED"


def test_home_att_player_lift():
    assert abs(diag.home_att_player_lift({
        "home_att_player": 2.1, "home_att_team": 1.5,
    }) - 0.6) < 1e-9
    assert diag.home_att_player_lift({
        "home_att_player": None, "home_att_team": 1.5,
    }) is None
