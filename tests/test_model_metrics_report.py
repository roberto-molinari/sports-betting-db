"""
Tests for model_metrics_report.py's own logic (renamed from model_snapshot.py
2026-08-11) -- the report-building/CLI plumbing is validated in practice via real
runs (same convention as compare_model_vs_market_odds.py's --league threading), but
totals_brier_score() (FEATURE-015) and the all-up pooling functions (FEATURE-017)
have real branching (None-guards, push handling, cross-league/season aggregation)
worth synthetic-fixture tests like the rest of this session's pure-logic additions.
"""

import sys

import model_metrics_report as report
import core.sports_db as sports_db


def _seed_match(conn, home_score, away_score, over_under_line, p_over, p_under,
                 league="Serie A", season=2025, method="poisson_v4"):
    home = sports_db.ensure_soccer_team("Home FC", league)
    away = sports_db.ensure_soccer_team("Away FC", league)
    match_id = sports_db.add_soccer_match(league, season, home, away, "2025-09-13T15:30:00Z")
    sports_db.update_soccer_match_result(match_id, home_score, away_score)
    sports_db.add_soccer_model_prediction(
        match_id=match_id, league=league, match_date="2025-09-13T15:30:00Z",
        generated_at="2025-09-13T00:00:00Z", method=method,
        over_under_line=over_under_line, p_over=p_over, p_under=p_under,
        conn=conn,
    )
    return match_id


def test_totals_brier_score_perfect_prediction_scores_zero(db_path, conn):
    # 3 total goals > line 2.5 -> "over" actually happened; p_over=1.0 is a perfect call.
    _seed_match(conn, home_score=2, away_score=1, over_under_line=2.5, p_over=1.0, p_under=0.0)
    score, n = report.totals_brier_score(conn, "Serie A", 2025, "poisson_v4")
    assert n == 1
    assert score == 0.0


def test_totals_brier_score_worst_prediction_scores_two(db_path, conn):
    # Confidently wrong on a binary market: (1-0)^2 + (0-1)^2 = 2.0, the max for this scale.
    _seed_match(conn, home_score=2, away_score=1, over_under_line=2.5, p_over=0.0, p_under=1.0)
    score, n = report.totals_brier_score(conn, "Serie A", 2025, "poisson_v4")
    assert n == 1
    assert score == 2.0


def test_totals_brier_score_excludes_push(db_path, conn):
    """total_goals == line has no defined over/under outcome -- must be excluded,
    matching backtest_from_predictions.run_totals()'s own push handling."""
    _seed_match(conn, home_score=1, away_score=1, over_under_line=2.0, p_over=0.5, p_under=0.5)
    score, n = report.totals_brier_score(conn, "Serie A", 2025, "poisson_v4")
    assert n == 0
    assert score != score  # nan


def test_totals_brier_score_excludes_rows_missing_a_totals_prediction(db_path, conn):
    """A prediction row with no O/U line/probabilities (e.g. an older method that
    never computed totals) must not crash or silently count as n=1."""
    _seed_match(conn, home_score=2, away_score=1, over_under_line=None, p_over=None, p_under=None)
    score, n = report.totals_brier_score(conn, "Serie A", 2025, "poisson_v4")
    assert n == 0
    assert score != score  # nan


def test_totals_brier_score_pools_across_multiple_matches(db_path, conn):
    _seed_match(conn, home_score=2, away_score=1, over_under_line=2.5, p_over=1.0, p_under=0.0)
    _seed_match(conn, home_score=2, away_score=1, over_under_line=2.5, p_over=0.0, p_under=1.0)
    score, n = report.totals_brier_score(conn, "Serie A", 2025, "poisson_v4")
    assert n == 2
    assert score == 1.0  # average of 0.0 and 2.0


# ── FEATURE-017 (2026-08-11): all-up discovery/pooling ─────────────────────────

def test_discover_leagues_finds_every_league_with_predictions(db_path, conn):
    _seed_match(conn, home_score=2, away_score=1, over_under_line=2.5, p_over=1.0, p_under=0.0,
                league="Serie A")
    _seed_match(conn, home_score=1, away_score=0, over_under_line=2.5, p_over=0.5, p_under=0.5,
                league="Premier League")
    assert report.discover_leagues(conn, "poisson_v4") == ["Premier League", "Serie A"]


def test_discover_leagues_excludes_other_methods(db_path, conn):
    _seed_match(conn, home_score=2, away_score=1, over_under_line=2.5, p_over=1.0, p_under=0.0,
                method="poisson_v3")
    assert report.discover_leagues(conn, "poisson_v4") == []


def test_discover_seasons_finds_every_graded_season_across_given_leagues(db_path, conn):
    _seed_match(conn, home_score=2, away_score=1, over_under_line=2.5, p_over=1.0, p_under=0.0,
                league="Serie A", season=2024)
    _seed_match(conn, home_score=1, away_score=0, over_under_line=2.5, p_over=0.5, p_under=0.5,
                league="Premier League", season=2025)
    assert report.discover_seasons(conn, ["Serie A", "Premier League"], "poisson_v4") == [2024, 2025]


def test_discover_seasons_scoped_to_only_the_given_leagues(db_path, conn):
    _seed_match(conn, home_score=2, away_score=1, over_under_line=2.5, p_over=1.0, p_under=0.0,
                league="Serie A", season=2024)
    _seed_match(conn, home_score=1, away_score=0, over_under_line=2.5, p_over=0.5, p_under=0.5,
                league="Premier League", season=2025)
    assert report.discover_seasons(conn, ["Serie A"], "poisson_v4") == [2024]


def test_pooled_brier_matches_hand_computed_weighted_average(db_path, conn):
    """Two leagues, one match each -- pooled Brier must be the plain average here
    since both groups have n=1 (a real n-weighting difference is already covered
    by totals_brier_score's own pooling test above)."""
    _seed_match(conn, home_score=2, away_score=1, over_under_line=2.5, p_over=1.0, p_under=0.0,
                league="Serie A")
    _seed_match(conn, home_score=2, away_score=1, over_under_line=2.5, p_over=0.0, p_under=1.0,
                league="Premier League")
    score, n = report.pooled_brier(conn, ["Serie A", "Premier League"], [2025], "poisson_v4", totals=True)
    assert n == 2
    assert score == 1.0  # (0.0 + 2.0) / 2


def test_pooled_roi_sums_staked_and_profit_across_leagues_and_seasons(db_path, conn):
    """A real end-to-end pooling check against backtest_from_predictions -- two
    matches in different leagues/seasons, each with betting odds, must combine
    into one true portfolio-level ROI (sum profit / sum staked), not an average
    of the two individual ROIs (which would be wrong whenever stakes differ)."""
    for league, season, home_ml in (("Serie A", 2024, 100), ("Premier League", 2025, -200)):
        home = sports_db.ensure_soccer_team("Home FC", league)
        away = sports_db.ensure_soccer_team("Away FC", league)
        match_id = sports_db.add_soccer_match(league, season, home, away, "2025-09-13T15:30:00Z")
        sports_db.update_soccer_match_result(match_id, 2, 0)  # home wins both
        sports_db.add_soccer_betting_odds(
            match_id=match_id, sportsbook="Bet365", odds_date="2025-09-01T00:00:00Z",
            home_moneyline=home_ml, draw_moneyline=250, away_moneyline=250,
        )
        sports_db.add_soccer_model_prediction(
            match_id=match_id, league=league, match_date="2025-09-13T15:30:00Z",
            generated_at="2025-09-01T00:00:00Z", method="poisson_v4",
            p_home=0.9, p_draw=0.05, p_away=0.05, conn=conn,
        )
    r = report.pooled_roi(conn, ["Serie A", "Premier League"], [2024, 2025], "poisson_v4",
                           ev_threshold=0.0, sportsbook="Bet365", totals=False)
    # p_draw=p_away=0.05 against +250 odds is negative EV on both matches -> only the
    # home side clears EV>0% and gets staked, in each match.
    # Serie A: ml=+100 -> decimal 2.0, home wins -> profit +1.0 on stake 1.0.
    # Premier League: ml=-200 -> decimal 1.5, home wins -> profit +0.5 on stake 1.0.
    assert r["staked"] == 2.0
    assert r["profit"] == 1.5
    assert r["roi"] == 0.75
    assert r["bets"] == 2


def _seed_full_match(conn, home_score, away_score, p_home, p_draw, p_away,
                      over_under_line, p_over, p_under, league="Serie A", season=2025,
                      method="poisson_v4", home_ml=None, draw_ml=None, away_ml=None,
                      over_odds=None, under_odds=None):
    """Like _seed_match, but sets BOTH markets' predictions on one row (needed to
    test cross-market pooling, which reads the same match's 1X2 and totals
    predictions together)."""
    home = sports_db.ensure_soccer_team("Home FC", league)
    away = sports_db.ensure_soccer_team("Away FC", league)
    match_id = sports_db.add_soccer_match(league, season, home, away, "2025-09-13T15:30:00Z")
    sports_db.update_soccer_match_result(match_id, home_score, away_score)
    sports_db.add_soccer_model_prediction(
        match_id=match_id, league=league, match_date="2025-09-13T15:30:00Z",
        generated_at="2025-09-13T00:00:00Z", method=method,
        p_home=p_home, p_draw=p_draw, p_away=p_away,
        over_under_line=over_under_line, p_over=p_over, p_under=p_under,
        conn=conn,
    )
    if home_ml is not None:
        sports_db.add_soccer_betting_odds(
            match_id=match_id, sportsbook="Bet365", odds_date="2025-09-01T00:00:00Z",
            home_moneyline=home_ml, draw_moneyline=draw_ml, away_moneyline=away_ml,
            over_under=over_under_line, over_odds=over_odds, under_odds=under_odds,
        )
    return match_id


def test_pooled_brier_across_markets_blends_1x2_and_totals(db_path, conn):
    """One match: a perfect 1X2 call (brier=0) and the worst-possible totals call
    (brier=2). The cross-market pool must be the n-weighted average of BOTH
    markets' error sums -- (0*1 + 2*1)/2 = 1.0 -- not either market's own number,
    proving the two markets are actually being combined, not one silently
    shadowing the other."""
    _seed_full_match(conn, home_score=2, away_score=0,
                      p_home=1.0, p_draw=0.0, p_away=0.0,
                      over_under_line=1.5, p_over=0.0, p_under=1.0)  # total=2>1.5 (over); p_over=0.0 is worst-case
    score, n = report.pooled_brier_across_markets(conn, ["Serie A"], [2025], "poisson_v4")
    assert n == 2  # one match, counted once per market
    assert score == 1.0


def test_pooled_roi_across_markets_sums_both_markets_staked_and_profit(db_path, conn):
    _seed_full_match(conn, home_score=2, away_score=0,
                      p_home=0.9, p_draw=0.05, p_away=0.05,
                      over_under_line=1.5, p_over=0.9, p_under=0.1,
                      home_ml=100, draw_ml=250, away_ml=250,
                      over_odds=100, under_odds=100)
    r = report.pooled_roi_across_markets(conn, ["Serie A"], [2025], "poisson_v4",
                                          ev_threshold=0.0, sportsbook="Bet365")
    # 1X2 home: ml=+100 -> decimal 2.0, EV=0.9*2-1=+0.8 -> bet, home wins -> profit +1.0.
    # Totals over: odds=+100 -> decimal 2.0, EV=0.9*2-1=+0.8 -> bet, total=2>1.5 (over wins) -> profit +1.0.
    assert r["staked"] == 2.0
    assert r["profit"] == 2.0
    assert r["roi"] == 1.0
    assert r["bets"] == 2


# ── console-only preview mode: governed by --note, not argument count ──────────
# (2026-08-11: originally zero-args-only; 2026-08-12: broadened to "any invocation
# missing --note", since --guardrail alone used to fall through to the persisted
# path and error demanding --note, defeating a quick unpersisted look with a
# non-default flag set.)

def _run_main_with_argv(argv, monkeypatch):
    monkeypatch.setattr(sys, "argv", argv)
    report.main()


def test_no_args_prints_a_report_and_writes_no_file(db_path, conn, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(report, "SNAPSHOT_DIR", tmp_path / "model_snapshots")
    _seed_match(conn, home_score=2, away_score=1, over_under_line=2.5, p_over=1.0, p_under=0.0)

    _run_main_with_argv(["model_metrics_report.py"], monkeypatch)

    out = capsys.readouterr().out
    assert "ALL-UP" in out
    assert "console-only preview" in out
    assert "Written to" not in out  # the persisted-mode footer must not appear
    assert not (tmp_path / "model_snapshots").exists()  # dir never even created


def test_flag_without_note_still_previews_and_does_not_persist(db_path, conn, tmp_path, monkeypatch, capsys):
    """Regression test for the 2026-08-12 fix: `--guardrail` (or any flag) with no
    --note must NOT error demanding --note and must NOT persist a file -- it's
    still the console-only preview, just with that flag honored. Before the fix,
    `python model_metrics_report.py --guardrail` failed argparse's required-arg
    check instead of previewing."""
    monkeypatch.setattr(report, "SNAPSHOT_DIR", tmp_path / "model_snapshots")
    _seed_match(conn, home_score=2, away_score=1, over_under_line=2.5, p_over=1.0, p_under=0.0)

    _run_main_with_argv(["model_metrics_report.py", "--guardrail"], monkeypatch)

    out = capsys.readouterr().out
    assert "ALL-UP" in out
    assert "console-only preview" in out
    assert "Guardrail: floor=" in out  # the flag was actually honored, not ignored
    assert "Written to" not in out
    assert not (tmp_path / "model_snapshots").exists()


def test_note_persists_a_file_regardless_of_other_flags(db_path, conn, tmp_path, monkeypatch, capsys):
    """--note given, alone or combined with other flags, always persists --
    --note is what governs persistence, not argument count or which other flags
    are set."""
    snapshot_dir = tmp_path / "model_snapshots"
    monkeypatch.setattr(report, "SNAPSHOT_DIR", snapshot_dir)
    _seed_match(conn, home_score=2, away_score=1, over_under_line=2.5, p_over=1.0, p_under=0.0)

    _run_main_with_argv(["model_metrics_report.py", "--note", "regression check"], monkeypatch)

    out = capsys.readouterr().out
    assert "Written to" in out
    written = list(snapshot_dir.glob("*.txt"))
    assert len(written) == 1
    assert "console-only preview" not in written[0].read_text()


def test_note_with_guardrail_persists_with_guardrail_suffix(db_path, conn, tmp_path, monkeypatch, capsys):
    snapshot_dir = tmp_path / "model_snapshots"
    monkeypatch.setattr(report, "SNAPSHOT_DIR", snapshot_dir)
    _seed_match(conn, home_score=2, away_score=1, over_under_line=2.5, p_over=1.0, p_under=0.0)

    _run_main_with_argv(["model_metrics_report.py", "--guardrail", "--note", "regression check"], monkeypatch)

    out = capsys.readouterr().out
    assert "Written to" in out
    written = list(snapshot_dir.glob("*_guardrail.txt"))
    assert len(written) == 1
    assert "Guardrail: floor=" in written[0].read_text()
