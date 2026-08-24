"""Tests for season_kickoff.py's import_fixtures() -- specifically the
allow_overwrite threading added 2026-08-23 (BUGS.md) after club_league_
scorecard.py's refresh step was found to detect-but-never-apply newly
completed matches' scores, leaving real picks stuck ungraded."""

from unittest.mock import patch

import season_kickoff


def test_default_does_not_pass_allow_overwrite(monkeypatch):
    captured = {}

    def fake_run(cmd, check):
        captured["cmd"] = cmd

    monkeypatch.setattr(season_kickoff.subprocess, "run", fake_run)
    season_kickoff.import_fixtures("Premier League", 2026)

    assert "--allow-overwrite" not in captured["cmd"]


def test_allow_overwrite_true_passes_the_flag_through(monkeypatch):
    captured = {}

    def fake_run(cmd, check):
        captured["cmd"] = cmd

    monkeypatch.setattr(season_kickoff.subprocess, "run", fake_run)
    season_kickoff.import_fixtures("Premier League", 2026, allow_overwrite=True)

    assert "--allow-overwrite" in captured["cmd"]
    assert "import_league_matches.py" in captured["cmd"]


def test_serie_a_path_ignores_allow_overwrite(monkeypatch):
    """update_serie_a_results.py has no --allow-overwrite flag at all (it always
    applies fetched results directly) -- allow_overwrite=True must not add a
    flag that script doesn't understand."""
    captured = {}

    def fake_run(cmd, check):
        captured["cmd"] = cmd

    monkeypatch.setattr(season_kickoff.subprocess, "run", fake_run)
    season_kickoff.import_fixtures("Serie A", 2026, allow_overwrite=True)

    assert "update_serie_a_results.py" in captured["cmd"]
    assert "--allow-overwrite" not in captured["cmd"]
