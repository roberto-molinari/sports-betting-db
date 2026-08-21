"""Tests for import_league_market_odds.py's devig() -- power-method devig
(2026-08-20, replacing the old proportional method; see that function's docstring
for why: proportional devig leaves a favorite-longshot bias in p_*_fair, measured
at calibration slope 1.154 against realized outcomes, BUGS.md BUG-009)."""
import pytest

from import_league_market_odds import devig, _proportional_devig


def test_devig_sums_to_one():
    p_h, p_d, p_a = devig(1.50, 4.00, 6.50)
    assert p_h + p_d + p_a == pytest.approx(1.0, abs=1e-9)


def test_devig_shrinks_longshot_more_than_favorite_vs_proportional():
    # A real book's overround, split unevenly across a heavy favorite/big longshot
    # line -- power devig should pull MORE probability off the longshot and give
    # MORE to the favorite than proportional devig does, since that's the specific
    # correction the favorite-longshot bias needs.
    h, d, a = 1.10, 8.00, 15.00
    power = devig(h, d, a)
    proportional = _proportional_devig((1 / h, 1 / d, 1 / a))
    assert power[0] > proportional[0]   # favorite: power devig rates it MORE likely
    assert power[2] < proportional[2]   # longshot: power devig rates it LESS likely


def test_devig_no_vig_line_matches_proportional(tmp_path, monkeypatch):
    # overround exactly 1.0 (no vig at all) -- both methods agree, and this hits
    # devig()'s <= 1.0 fallback path deliberately (there's no k > 1 to solve for
    # when there's no margin to remove). Redirects the fallback log so this test
    # doesn't append to the real, git-tracked warnings log as a side effect.
    monkeypatch.setattr("import_league_market_odds.DEVIG_FALLBACK_LOG_PATH", tmp_path / "warnings.log")
    assert devig(3.0, 3.0, 3.0) == pytest.approx((1 / 3, 1 / 3, 1 / 3))


def test_devig_bad_overround_falls_back_and_logs(tmp_path, monkeypatch):
    log_path = tmp_path / "devig_overround_warnings.log"
    monkeypatch.setattr("import_league_market_odds.DEVIG_FALLBACK_LOG_PATH", log_path)

    h, d, a = 3.1, 3.1, 3.1   # overround ~0.968 -- a data glitch, not a real book line
    result = devig(h, d, a, context="unit test")

    assert result == pytest.approx(_proportional_devig((1 / h, 1 / d, 1 / a)))
    assert log_path.exists()
    logged = log_path.read_text()
    assert "unit test" in logged
    assert "3.1" in logged
