"""
Unit tests for the World Cup Poisson extensions in core.poisson_model:
analyse_match_wc, totals_probs, compute_ev_totals, and ev_to_stars.

These are pure-math tests — no database needed.
"""

import pytest

import core.poisson_model as pm


# ── analyse_match_wc ─────────────────────────────────────────────────────────

def test_wc_symmetric_inputs_give_balanced_1x2():
    """Identical attack/defense for both teams (neutral venue) -> p_home ≈ p_away."""
    r = pm.analyse_match_wc(1.4, 1.4, 1.4, 1.4)
    assert r["p_home"] == pytest.approx(r["p_away"], abs=1e-9)
    assert r["p_draw"] > 0
    assert r["p_home"] + r["p_draw"] + r["p_away"] == pytest.approx(1.0, abs=1e-3)


def test_wc_stronger_attack_weaker_opponent_defense_favours_home():
    weak_home = pm.analyse_match_wc(1.2, 1.4, 1.4, 1.4)["p_home"]
    strong_home = pm.analyse_match_wc(2.4, 1.4, 1.4, 1.8)["p_home"]
    assert strong_home > weak_home


def test_wc_home_advantage_multiplier_raises_home_prob():
    neutral = pm.analyse_match_wc(1.5, 1.5, 1.2, 1.2, home_advantage=1.0)["p_home"]
    boosted = pm.analyse_match_wc(1.5, 1.5, 1.2, 1.2, home_advantage=1.2)["p_home"]
    assert boosted > neutral


def test_wc_away_advantage_multiplier_raises_away_prob():
    """A host listed as the away team still plays at home, so away_advantage
    must lift the away side's win probability."""
    neutral = pm.analyse_match_wc(1.5, 1.5, 1.2, 1.2, away_advantage=1.0)["p_away"]
    boosted = pm.analyse_match_wc(1.5, 1.5, 1.2, 1.2, away_advantage=1.2)["p_away"]
    assert boosted > neutral


def test_wc_returns_evs_and_totals_when_priced():
    r = pm.analyse_match_wc(
        2.0, 1.0, 0.8, 1.5,
        home_moneyline=-150, draw_moneyline=300, away_moneyline=500,
        ou_line=2.5, over_odds=-110, under_odds=-110,
    )
    for key in ("ev_home", "ev_draw", "ev_away", "ev_over", "ev_under",
                "p_over", "p_under", "ou_line"):
        assert key in r
    assert r["ou_line"] == 2.5


# ── totals_probs ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("line", [1.5, 2.5, 3.5, 4.5])
def test_totals_probs_sum_to_one_half_lines(line):
    grid = pm.scoreline_grid(1.6, 1.3, max_goals=pm.WC_MAX_GOALS)
    t = pm.totals_probs(grid, line)
    assert t["p_push"] == 0.0  # half-goal lines never push
    assert t["p_over"] + t["p_under"] == pytest.approx(1.0, abs=1e-4)


def test_totals_probs_integer_line_has_push():
    grid = pm.scoreline_grid(1.6, 1.3, max_goals=pm.WC_MAX_GOALS)
    t = pm.totals_probs(grid, 2.0)
    assert t["p_push"] > 0
    assert t["p_over"] + t["p_under"] + t["p_push"] == pytest.approx(1.0, abs=1e-4)


def test_totals_probs_higher_lambda_shifts_to_over():
    low = pm.scoreline_grid(0.9, 0.8, max_goals=pm.WC_MAX_GOALS)
    high = pm.scoreline_grid(2.5, 2.2, max_goals=pm.WC_MAX_GOALS)
    assert pm.totals_probs(high, 2.5)["p_over"] > pm.totals_probs(low, 2.5)["p_over"]


def test_high_line_not_undercounted_with_larger_grid():
    """A 4.5 line in a high-scoring game keeps more over mass at the WC grid cap."""
    lam_h, lam_a = 2.6, 2.4
    small = pm.totals_probs(pm.scoreline_grid(lam_h, lam_a, max_goals=6), 4.5)
    big = pm.totals_probs(pm.scoreline_grid(lam_h, lam_a, max_goals=pm.WC_MAX_GOALS), 4.5)
    assert big["p_over"] > small["p_over"]
    # The bigger grid is also closer to a normalized distribution.
    assert big["p_over"] + big["p_under"] == pytest.approx(1.0, abs=1e-3)


# ── compute_ev_totals ────────────────────────────────────────────────────────

def test_compute_ev_totals_matches_compute_ev_for_half_lines():
    """With no push, push-aware EV must equal the plain EV formula."""
    p_over = 0.55
    p_under = 1 - p_over
    assert pm.compute_ev_totals(p_over, p_under, -110) == pytest.approx(
        pm.compute_ev(p_over, -110))


def test_compute_ev_totals_push_returns_stake():
    """A pure-push outcome has EV 0 (stake returned)."""
    assert pm.compute_ev_totals(0.0, 0.0, -110) == pytest.approx(0.0)


# ── ev_to_stars ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("ev,expected", [
    (-0.10, 1), (0.0, 1), (0.0749, 1),   # below 2-star threshold -> floor
    (0.075, 2), (0.10, 2), (0.1499, 2),  # 2-star band
    (0.15, 3), (0.40, 3),                # 3-star band
])
def test_ev_to_stars_boundaries(ev, expected):
    assert pm.ev_to_stars(ev) == expected


def test_ev_to_stars_never_abstains():
    """Even a deeply negative EV best pick is still rated (1 star), not 0."""
    assert pm.ev_to_stars(-5.0) == 1
