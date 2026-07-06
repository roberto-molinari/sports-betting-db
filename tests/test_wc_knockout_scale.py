"""Tests for core.wc_knockout_scale (BUG-004): the knockout-stage goal-level
correction, scoped to non-Group stages only."""

from core.wc_knockout_scale import knockout_goal_scale, KNOCKOUT_GOAL_SCALE


def test_group_stage_untouched():
    """The over-projection bias doesn't show up in the group stage, so no
    correction applies there."""
    assert knockout_goal_scale("Group") == 1.0


def test_knockout_stages_get_the_correction():
    assert knockout_goal_scale("R32") == KNOCKOUT_GOAL_SCALE
    assert knockout_goal_scale("R16") == KNOCKOUT_GOAL_SCALE


def test_scale_is_conservative_not_full_correction():
    """0.85 was chosen deliberately short of the exact calibration-zeroing value
    (0.819) -- confirms we shipped the conservative option, not the aggressive one."""
    assert KNOCKOUT_GOAL_SCALE == 0.85
    assert KNOCKOUT_GOAL_SCALE > 0.819
