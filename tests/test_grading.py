"""Tests for core.grading — the pure, market-agnostic pick grader.

Covers 1X2 + O/U settlement (a regression guard preserving the prior grade_pick behaviour,
now in dict form), the new ADVANCE market, and the advancing_side derivation helper.
"""

import pytest

from core.grading import grade_pick, advancing_side


def outcome(home, away, advanced=None):
    return {"regulation_home": home, "regulation_away": away, "advanced": advanced}


# ── 1X2 + O/U (regression guard: same results as the old grade_pick) ──────────

@pytest.mark.parametrize("side,home,away,expected", [
    ("HOME", 2, 0, "win"),
    ("HOME", 1, 1, "loss"),      # a draw loses the home moneyline
    ("HOME", 0, 1, "loss"),
    ("AWAY", 0, 2, "win"),
    ("AWAY", 1, 1, "loss"),
    ("DRAW", 1, 1, "win"),
    ("DRAW", 2, 1, "loss"),
    ("OVER 2.5", 2, 1, "win"),   # total 3 > 2.5
    ("OVER 2.5", 1, 1, "loss"),  # total 2 < 2.5
    ("UNDER 3.5", 2, 0, "win"),  # total 2 < 3.5
    ("UNDER 3.5", 3, 1, "loss"),  # total 4 > 3.5
])
def test_grade_pick_1x2_and_totals(side, home, away, expected):
    assert grade_pick(side, outcome(home, away)) == expected


def test_grade_pick_integer_line_push():
    assert grade_pick("OVER 2", outcome(1, 1)) == "push"    # total exactly 2
    assert grade_pick("UNDER 3", outcome(2, 1)) == "push"   # total exactly 3


def test_grade_pick_unknown_side_raises():
    with pytest.raises(ValueError):
        grade_pick("PARLAY", outcome(1, 0))


# ── ADVANCE ───────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("side,advanced,expected", [
    ("HOME ADVANCE", "HOME", "win"),
    ("HOME ADVANCE", "AWAY", "loss"),
    ("AWAY ADVANCE", "AWAY", "win"),
    ("AWAY ADVANCE", "HOME", "loss"),
])
def test_grade_advance(side, advanced, expected):
    # ADVANCE ignores the 90' score; it only depends on who advanced.
    assert grade_pick(side, outcome(0, 0, advanced=advanced)) == expected


def test_grade_advance_without_outcome_raises():
    with pytest.raises(ValueError):
        grade_pick("HOME ADVANCE", outcome(1, 0, advanced=None))


# ── advancing_side ─────────────────────────────────────────────────────────────

def test_advancing_side_regulation():
    # Decided in 90': no ET / shootout supplied.
    assert advancing_side(2, 1) == "HOME"
    assert advancing_side(0, 1) == "AWAY"


def test_advancing_side_extra_time():
    # 1-1 after 90', 2-1 (cumulative) after ET -> home advances.
    assert advancing_side(1, 1, extra_time_home=2, extra_time_away=1) == "HOME"


def test_advancing_side_shootout():
    # Level through ET (2-2), away win the shootout 4-3.
    assert advancing_side(1, 1, extra_time_home=2, extra_time_away=2,
                          shootout_home=3, shootout_away=4) == "AWAY"


def test_advancing_side_deepest_level_decides():
    # Shootout is the decisive level even though earlier levels are level.
    assert advancing_side(0, 0, extra_time_home=0, extra_time_away=0,
                          shootout_home=5, shootout_away=4) == "HOME"


def test_advancing_side_none_when_unresolved():
    assert advancing_side(1, 1) is None
