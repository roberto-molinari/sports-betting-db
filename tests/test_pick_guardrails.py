"""Tests for the shared floor/cap guardrail check (core/pick_guardrails.py),
extracted 2026-08-07 from generate_wc_card.py's select_pick() so the club-league
card generator can use the same, single-source-of-truth logic (BUG-009, 2026-08-05:
the club-league pipeline went without any guardrail for weeks simply because this
never got ported from the WC tool)."""
from core.pick_guardrails import guardrail_reasons, guardrail_excess


def test_guardrail_reasons_clean_pick_has_no_reasons():
    assert guardrail_reasons(prob=0.55, implied=0.50, floor=0.25, cap=2.0) == []


def test_guardrail_reasons_floor_fires_below_floor():
    reasons = guardrail_reasons(prob=0.20, implied=0.18, floor=0.25, cap=2.0)
    assert any("floor" in r for r in reasons)
    assert not any("cap" in r for r in reasons)


def test_guardrail_reasons_cap_fires_on_overrated_underdog():
    # model 0.353 vs market 0.121 = ~2.9x -- clears the floor, trips the cap.
    reasons = guardrail_reasons(prob=0.353, implied=0.121, floor=0.25, cap=2.0)
    assert any("cap" in r for r in reasons)
    assert not any("floor" in r for r in reasons)


def test_guardrail_reasons_both_fire_independently():
    reasons = guardrail_reasons(prob=0.20, implied=0.05, floor=0.25, cap=2.0)
    assert len(reasons) == 2
    assert any("floor" in r for r in reasons)
    assert any("cap" in r for r in reasons)


def test_guardrail_reasons_cap_none_skips_cap_entirely():
    """cap=None means a system that only wants the floor (e.g. the club-league
    pipeline, where the cap turned out to be inert on top of the xG-stretch fix --
    BUGS.md, BUG-009, 2026-08-07) never trips the cap check, no matter how extreme
    the disagreement."""
    reasons = guardrail_reasons(prob=0.90, implied=0.05, floor=0.25, cap=None)
    assert reasons == []


def test_guardrail_reasons_cap_ignores_favorites():
    # A favorite can't be >=2x its own high implied prob -- only underdogs can trip it.
    reasons = guardrail_reasons(prob=0.70, implied=0.60, floor=0.25, cap=2.0)
    assert reasons == []


def test_guardrail_reasons_market_floor_fires_on_market_longshot():
    """BUG-009 re-diagnosis (2026-08-20): a candidate the MODEL likes (clears the
    model floor) but the MARKET prices as a longshot -- exactly the losing segment
    the model floor can't catch, since a huge model-vs-market gap on a noisy model
    is far more likely estimation error than edge."""
    reasons = guardrail_reasons(prob=0.40, implied=0.20, floor=0.25, market_floor=0.32)
    assert any("market floor" in r for r in reasons)
    assert not any(r.startswith("floor") for r in reasons)


def test_guardrail_reasons_market_floor_clear_above_threshold():
    assert guardrail_reasons(prob=0.40, implied=0.35, floor=0.25, market_floor=0.32) == []


def test_guardrail_reasons_market_floor_none_skips_check():
    """Default None preserves pre-2026-08-20 behavior exactly -- e.g. the WC
    pipeline, where the market floor was never swept, keeps its old guardrails."""
    assert guardrail_reasons(prob=0.40, implied=0.05, floor=0.25, market_floor=None) == []


def test_guardrail_reasons_market_floor_skipped_when_implied_unknown():
    assert guardrail_reasons(prob=0.40, implied=None, floor=0.25, market_floor=0.32) == []


def test_guardrail_excess_none_when_clear():
    assert guardrail_excess(prob=0.55, implied=0.50, floor=0.25, cap=2.0) is None


def test_guardrail_excess_measures_floor_miss():
    assert round(guardrail_excess(prob=0.24, implied=0.20, floor=0.25, cap=2.0), 3) == 0.01


def test_guardrail_excess_takes_worst_of_multiple():
    # floor excess: 0.25 - 0.20 = 0.05; cap excess: 0.20 - 2.0*0.05 = 0.10 -- cap is worse.
    excess = guardrail_excess(prob=0.20, implied=0.05, floor=0.25, cap=2.0)
    assert round(excess, 3) == 0.10


def test_guardrail_excess_measures_market_floor_miss():
    excess = guardrail_excess(prob=0.40, implied=0.20, floor=0.25, market_floor=0.32)
    assert round(excess, 3) == 0.12
