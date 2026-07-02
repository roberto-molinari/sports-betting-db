"""Tests for generate_scoreline_heatmap's pure display logic."""

from core.poisson_model import scoreline_grid, outcome_probs
import generate_scoreline_heatmap as gsh


def test_display_matrix_conserves_probability():
    """Collapsing the full grid into the 'n+' display window keeps total mass:
    the truncated cells fold into the last row/col rather than being dropped."""
    grid = scoreline_grid(2.1, 1.3, max_goals=10)
    disp, _ = gsh.display_matrix(grid, n=5)
    full = sum(sum(r) for r in grid)
    shown = sum(sum(r) for r in disp)
    assert abs(full - shown) < 1e-12          # nothing lost in the fold
    assert len(disp) == 6 and len(disp[0]) == 6


def test_display_matrix_modal_cell():
    """The modal scoreline is the argmax of the FULL grid, clamped into display."""
    grid = scoreline_grid(2.13, 1.33, max_goals=10)
    _, modal = gsh.display_matrix(grid, n=6)
    # brute-force argmax over the full grid
    bi = bj = 0
    best = -1.0
    for i, row in enumerate(grid):
        for j, p in enumerate(row):
            if p > best:
                best, bi, bj = p, i, j
    assert modal == (min(bi, 6), min(bj, 6))


def test_display_matrix_tail_bucket_aggregates():
    """A tiny window pushes most mass into the 'n+' corner buckets."""
    grid = scoreline_grid(1.5, 1.5, max_goals=10)
    disp, _ = gsh.display_matrix(grid, n=1)        # only 0 and "1+"
    # disp[1][1] is home>=1 AND away>=1 — must hold real mass, not zero
    assert disp[1][1] > 0.2
    assert abs(sum(sum(r) for r in disp) - sum(sum(r) for r in grid)) < 1e-12


def test_build_svg_uses_lambda_not_xg():
    """Guard the labelling rule: the header says λ, never xG (DESIGN-001)."""
    grid = scoreline_grid(2.13, 1.33, max_goals=10)
    op = outcome_probs(grid)
    disp, modal = gsh.display_matrix(grid, n=6)
    svg, w, h = gsh.build_svg("Brazil", "Japan", "2026-06-29", 2.13, 1.33, disp, modal,
                              (op["p_home"], op["p_draw"], op["p_away"]))
    assert "λ (projected goals)" in svg
    assert "xG" not in svg
    assert w > 0 and h > 0


def test_build_svg_has_socials_footer():
    """Every graphic carries the minvest socials/URL branding footer."""
    grid = scoreline_grid(2.13, 1.33, max_goals=10)
    op = outcome_probs(grid)
    disp, modal = gsh.display_matrix(grid, n=6)
    svg, _, _ = gsh.build_svg("Brazil", "Japan", "2026-06-29", 2.13, 1.33, disp, modal,
                              (op["p_home"], op["p_draw"], op["p_away"]))
    assert "@minvest__" in svg
    assert "@minvest-picks" in svg
    assert "https://minvest.tech" in svg
