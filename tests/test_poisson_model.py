"""
Unit tests for core.poisson_model.

Focus is the pure, deterministic logic that drives betting decisions:
odds conversion, Poisson math, expected value, the scoreline grid, outcome
probabilities, and the lambda estimation. These are the calculations where a
sign error or off-by-one produces plausible-but-wrong "value bets", so they get
the most coverage. Two thin DB-reading helpers are also exercised against a
temp database.
"""

import math

import pytest

from core import poisson_model as pm
from core import sports_db


# ── Odds conversion ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("american,expected", [
    (100, 2.0),     # even money
    (-100, 2.0),    # even money, other sign
    (150, 2.5),
    (-200, 1.5),
    (250, 3.5),
    (-150, 1 + 100 / 150),
])
def test_american_to_decimal(american, expected):
    assert pm.american_to_decimal(american) == pytest.approx(expected)


@pytest.mark.parametrize("american,expected", [
    (100, 0.5),
    (-100, 0.5),
    (150, 0.4),
    (-200, 2 / 3),
    (-150, 0.6),
])
def test_american_to_implied_prob(american, expected):
    assert pm.american_to_implied_prob(american) == pytest.approx(expected)


def test_favorite_has_higher_implied_prob_and_lower_payout():
    """A favorite (negative line) should imply >0.5 prob and pay <2.0 decimal."""
    assert pm.american_to_implied_prob(-200) > 0.5
    assert pm.american_to_decimal(-200) < 2.0
    # Underdog mirror.
    assert pm.american_to_implied_prob(200) < 0.5
    assert pm.american_to_decimal(200) > 2.0


# ── Poisson pmf ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("k,lam,expected", [
    (0, 2.0, math.exp(-2)),
    (1, 2.0, math.exp(-2) * 2),
    (2, 2.0, math.exp(-2) * 2),          # 2^2/2! = 2
    (3, 1.0, math.exp(-1) / 6),
])
def test_poisson_pmf_values(k, lam, expected):
    assert pm.poisson_pmf(k, lam) == pytest.approx(expected)


def test_poisson_pmf_sums_to_one():
    """The pmf over all k must sum to ~1 for a valid distribution."""
    total = sum(pm.poisson_pmf(k, 1.7) for k in range(0, 40))
    assert total == pytest.approx(1.0, abs=1e-9)


def test_poisson_pmf_zero_lambda_is_degenerate():
    """lambda <= 0 means the team scores exactly 0 with certainty."""
    assert pm.poisson_pmf(0, 0.0) == 1.0
    assert pm.poisson_pmf(1, 0.0) == 0.0
    assert pm.poisson_pmf(0, -5.0) == 1.0


# ── Expected value ───────────────────────────────────────────────────────────

def test_compute_ev_fair_odds_is_zero():
    # True 50% chance at +100 (decimal 2.0) is a break-even bet.
    assert pm.compute_ev(0.5, 100) == pytest.approx(0.0)


def test_compute_ev_positive_when_model_beats_market():
    # Model thinks 60% but price implies 50% -> +EV.
    assert pm.compute_ev(0.60, 100) == pytest.approx(0.2)


def test_compute_ev_negative_when_model_below_market():
    assert pm.compute_ev(0.40, 100) == pytest.approx(-0.2)


def test_compute_ev_matches_definition():
    # EV == p * decimal - 1, for an arbitrary favorite price.
    p, line = 0.7, -150
    expected = p * pm.american_to_decimal(line) - 1
    assert pm.compute_ev(p, line) == pytest.approx(expected)


# ── Scoreline grid ───────────────────────────────────────────────────────────

def test_scoreline_grid_dimensions():
    grid = pm.scoreline_grid(1.5, 1.2, max_goals=6)
    assert len(grid) == 7
    assert all(len(row) == 7 for row in grid)


def test_scoreline_grid_cell_is_product_of_marginals():
    lh, la = 1.4, 1.1
    grid = pm.scoreline_grid(lh, la, max_goals=6)
    for i in range(7):
        for j in range(7):
            assert grid[i][j] == pytest.approx(
                pm.poisson_pmf(i, lh) * pm.poisson_pmf(j, la)
            )


def test_scoreline_grid_mass_close_to_one_for_modest_lambdas():
    grid = pm.scoreline_grid(1.3, 1.1, max_goals=10)
    total = sum(cell for row in grid for cell in row)
    assert total == pytest.approx(1.0, abs=1e-4)


# ── Outcome probabilities ────────────────────────────────────────────────────

def test_outcome_probs_sum_to_grid_mass():
    grid = pm.scoreline_grid(1.6, 1.25, max_goals=10)
    probs = pm.outcome_probs(grid)
    grid_total = sum(cell for row in grid for cell in row)
    assert probs["p_home"] + probs["p_draw"] + probs["p_away"] == pytest.approx(grid_total)


def test_outcome_probs_symmetric_lambdas_give_equal_win_probs():
    """Equal expected goals -> home and away win probabilities must match."""
    grid = pm.scoreline_grid(1.4, 1.4, max_goals=10)
    probs = pm.outcome_probs(grid)
    assert probs["p_home"] == pytest.approx(probs["p_away"])


def test_outcome_probs_higher_lambda_favored():
    grid = pm.scoreline_grid(2.0, 1.0, max_goals=10)
    probs = pm.outcome_probs(grid)
    assert probs["p_home"] > probs["p_away"]


def test_over_under_complementary():
    grid = pm.scoreline_grid(1.5, 1.2, max_goals=10)
    probs = pm.outcome_probs(grid)
    assert probs["p_over25"] + probs["p_under25"] == pytest.approx(1.0)


# ── Shrinkage ────────────────────────────────────────────────────────────────

def test_shrink_no_shrinkage_returns_rating():
    assert pm._shrink(2.0, 1.0, n=10, k=0) == pytest.approx(2.0)


def test_shrink_blends_toward_league_average():
    # n == k -> exactly halfway between rating and league average.
    assert pm._shrink(2.0, 1.0, n=5, k=5) == pytest.approx(1.5)


def test_shrink_more_data_trusts_team_more():
    near = pm._shrink(2.0, 1.0, n=100, k=5)
    far = pm._shrink(2.0, 1.0, n=2, k=5)
    assert near > far  # more games -> closer to the team's own 2.0


# ── estimate_lambdas (pure, shrinkage off by default) ────────────────────────

def _ratings(home_attack, home_defense, away_attack, away_defense, n=10):
    """Build the ratings dict shape that estimate_lambdas consumes."""
    return {
        "home_attack": home_attack, "home_defense": home_defense,
        "away_attack": away_attack, "away_defense": away_defense,
        "home_n": n, "away_n": n,
    }


def test_estimate_lambdas_basic_formula():
    league = {"avg_home": 1.5, "avg_away": 1.2}
    home = _ratings(2.0, 1.0, 1.7, 1.3)
    away = _ratings(1.8, 0.9, 1.5, 1.2)
    # shrinkage_k defaults to 0, so ratings pass through unchanged.
    lam_h, lam_a = pm.estimate_lambdas(home, away, league)
    # lambda_H = home_attack * (away_defense / avg_home) = 2.0 * (1.2/1.5)
    assert lam_h == pytest.approx(2.0 * (1.2 / 1.5))
    # lambda_A = away_attack * (home_defense / avg_away) = 1.5 * (1.0/1.2)
    assert lam_a == pytest.approx(1.5 * (1.0 / 1.2))


def test_estimate_lambdas_falls_back_to_league_avg_with_few_matches():
    league = {"avg_home": 1.5, "avg_away": 1.2}
    # Below MIN_MATCHES -> ratings ignored, league averages used instead.
    home = _ratings(5.0, 0.1, 5.0, 0.1, n=1)
    away = _ratings(5.0, 0.1, 5.0, 0.1, n=1)
    lam_h, lam_a = pm.estimate_lambdas(home, away, league)
    # h_att=avg_home, a_def=avg_home -> lambda_H = avg_home * avg_home/avg_home
    assert lam_h == pytest.approx(league["avg_home"])
    assert lam_a == pytest.approx(league["avg_away"])


def test_estimate_lambdas_applies_floor():
    league = {"avg_home": 1.5, "avg_away": 1.2}
    home = _ratings(0.0, 0.0, 0.0, 0.0)   # zero attack -> zero product
    away = _ratings(0.0, 0.0, 0.0, 0.0)
    lam_h, lam_a = pm.estimate_lambdas(home, away, league)
    assert lam_h == pytest.approx(0.1)    # sanity floor
    assert lam_a == pytest.approx(0.1)


# ── DB-reading helpers (temp database) ───────────────────────────────────────

def test_get_league_averages_empty_db_uses_fallback(conn):
    avgs = pm.get_league_averages(conn, league="Serie A")
    assert avgs == {"avg_home": 1.3, "avg_away": 1.1}


def test_get_team_ratings_weighted_average_and_cutoff(db_path, conn):
    """Seed known home results and verify the (unweighted, decay=1.0) ratings
    and the strict before-date cutoff."""
    a = sports_db.ensure_soccer_team("Team A", "Serie A")
    b = sports_db.ensure_soccer_team("Team B", "Serie A")
    c = sports_db.ensure_soccer_team("Team C", "Serie A")

    # Two completed home games for A before the cutoff...
    m1 = sports_db.add_soccer_match("Serie A", 2024, a, b, "2025-01-01")
    sports_db.update_soccer_match_result(m1, 2, 0)
    m2 = sports_db.add_soccer_match("Serie A", 2024, a, c, "2025-01-08")
    sports_db.update_soccer_match_result(m2, 1, 1)
    # ...and one after the cutoff that must be excluded.
    m3 = sports_db.add_soccer_match("Serie A", 2024, a, b, "2025-03-01")
    sports_db.update_soccer_match_result(m3, 5, 5)

    ratings = pm.get_team_ratings(conn, a, before_date="2025-02-01", decay=1.0)

    assert ratings["home_n"] == 2                       # m3 excluded by cutoff
    assert ratings["home_attack"] == pytest.approx(1.5)  # (2 + 1) / 2
    assert ratings["home_defense"] == pytest.approx(0.5)  # (0 + 1) / 2
