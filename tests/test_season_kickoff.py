"""Tests for season_kickoff.py's import_fixtures() -- the allow_overwrite
threading added 2026-08-23 (BUGS.md) after club_league_scorecard.py's refresh
step was found to detect-but-never-apply newly completed matches' scores,
leaving real picks stuck ungraded, plus the Serie A auto-stamp step added
2026-09-04 once Serie A migrated onto the same TheStatsAPI pipeline as every
other league (BUGS.md)."""

import season_kickoff


def test_default_does_not_pass_allow_overwrite(monkeypatch):
    calls = []
    monkeypatch.setattr(season_kickoff.subprocess, "run", lambda cmd, check: calls.append(cmd))
    season_kickoff.import_fixtures("Premier League", 2026)

    assert len(calls) == 1
    assert "--allow-overwrite" not in calls[0]
    assert "import_league_matches.py" in calls[0]


def test_allow_overwrite_true_passes_the_flag_through(monkeypatch):
    calls = []
    monkeypatch.setattr(season_kickoff.subprocess, "run", lambda cmd, check: calls.append(cmd))
    season_kickoff.import_fixtures("Premier League", 2026, allow_overwrite=True)

    assert len(calls) == 1
    assert "--allow-overwrite" in calls[0]
    assert "import_league_matches.py" in calls[0]


def test_serie_a_runs_the_id_stamp_script_before_import_league_matches(monkeypatch):
    """Serie A must run migrate_serie_a_thestatsapi_ids.py --apply FIRST, every
    time -- not a one-time step -- since TheStatsAPI publishes fixtures
    progressively and a newly-published one could otherwise slip past
    import_league_matches.py's own (narrower) duplicate-fixture tolerance."""
    calls = []
    monkeypatch.setattr(season_kickoff.subprocess, "run", lambda cmd, check: calls.append(cmd))
    season_kickoff.import_fixtures("Serie A", 2026, allow_overwrite=True)

    assert len(calls) == 2
    assert "migrate_serie_a_thestatsapi_ids.py" in calls[0] and "--apply" in calls[0]
    assert "import_league_matches.py" in calls[1]
    assert "--league" in calls[1] and "Serie A" in calls[1]
    assert "--allow-overwrite" in calls[1]


def test_non_serie_a_league_does_not_run_the_id_stamp_script(monkeypatch):
    calls = []
    monkeypatch.setattr(season_kickoff.subprocess, "run", lambda cmd, check: calls.append(cmd))
    season_kickoff.import_fixtures("Bundesliga", 2026)

    assert len(calls) == 1
    assert "migrate_serie_a_thestatsapi_ids.py" not in calls[0]
