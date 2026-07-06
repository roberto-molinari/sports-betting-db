"""Tests for core.wc_host_advantage (BUG-006): the single source of truth for
World Cup co-host venue advantage, split into a stage-independent "currently
active host" tier and a stage-scoped "retired host" tier."""

from core.wc_host_advantage import host_advantage, HOST_HOME_ADVANTAGE


def test_current_host_gets_boost_regardless_of_stage():
    """A currently-active host nation (e.g. Mexico) is boosted in every stage,
    since all its matches so far (and its next one) are confirmed domestic."""
    assert host_advantage("Mexico", "Group") == HOST_HOME_ADVANTAGE
    assert host_advantage("Mexico", "R32") == HOST_HOME_ADVANTAGE
    assert host_advantage("Mexico", "R16") == HOST_HOME_ADVANTAGE


def test_retired_host_only_boosted_in_its_confirmed_stage():
    """Canada (retired after the group stage, per BUG-006) is boosted for its
    group matches but NOT for its knockout matches, since those are no longer
    confirmed to be played in Canada."""
    assert host_advantage("Canada", "Group") == HOST_HOME_ADVANTAGE
    assert host_advantage("Canada", "R32") == 1.0
    assert host_advantage("Canada", "R16") == 1.0


def test_non_host_never_boosted():
    assert host_advantage("Morocco", "Group") == 1.0
    assert host_advantage("Morocco", "R16") == 1.0


def test_current_host_boost_survives_missing_stage():
    """A currently-active host is boosted even if stage is None/unknown -- only
    retired hosts need a stage match, since active hosts are unconditional."""
    assert host_advantage("USA", None) == HOST_HOME_ADVANTAGE


def test_retired_host_not_boosted_with_missing_stage():
    assert host_advantage("Canada", None) == 1.0
