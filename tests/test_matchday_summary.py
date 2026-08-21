"""Tests for matchday_summary.py -- a read-only report of which leagues have
matches on a given matchday or range (docs/PRE-AND-POST-MATCHDAY-EXPERIENCE.md,
2026-08-21)."""
from unittest.mock import patch

import core.sports_db as sports_db
from matchday_summary import matches_in_window, summarize
from core.matchday import matchday_utc_window, matchday_range_utc_window


def _seed_match(conn, league, home, away, match_date):
    home_id = sports_db.ensure_soccer_team(home, league)
    away_id = sports_db.ensure_soccer_team(away, league)
    return sports_db.add_soccer_match(league, 2025, home_id, away_id, match_date)


def test_matches_in_window_groups_by_league(db_path, conn):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        _seed_match(conn, "Serie A", "Inter", "Milan", "2025-09-22T18:00:00.000Z")
        _seed_match(conn, "Premier League", "Arsenal", "Chelsea", "2025-09-22T14:00:00.000Z")

    start, end = matchday_utc_window("2025-09-22")
    rows = matches_in_window(conn, start, end)
    by_league = summarize(rows)

    assert set(by_league.keys()) == {"Serie A", "Premier League"}
    assert len(by_league["Serie A"]) == 1
    assert by_league["Serie A"][0][3:5] == ("Inter", "Milan")


def test_matches_in_window_excludes_league_with_no_matches_that_day(db_path, conn):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        _seed_match(conn, "Serie A", "Inter", "Milan", "2025-09-22T18:00:00.000Z")
        _seed_match(conn, "Bundesliga", "Bayern", "Dortmund", "2025-09-30T18:00:00.000Z")

    start, end = matchday_utc_window("2025-09-22")
    by_league = summarize(matches_in_window(conn, start, end))
    assert "Bundesliga" not in by_league


def test_range_covers_matches_across_multiple_days(db_path, conn):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        _seed_match(conn, "Serie A", "Inter", "Milan", "2025-09-22T18:00:00.000Z")
        _seed_match(conn, "Serie A", "Roma", "Lazio", "2025-09-24T18:00:00.000Z")

    start, end = matchday_range_utc_window("2025-09-22", "2025-09-25")
    by_league = summarize(matches_in_window(conn, start, end))
    assert len(by_league["Serie A"]) == 2


def test_matchday_label_per_match_uses_et_buffer_not_utc_date(db_path, conn):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        # 01:00 UTC on the 23rd = 21:00 EDT on the 22nd -- ET matchday is the 22nd.
        _seed_match(conn, "Serie A", "Inter", "Milan", "2025-09-23T01:00:00.000Z")

    start, end = matchday_utc_window("2025-09-22")
    by_league = summarize(matches_in_window(conn, start, end))
    matchday_date, match_date, kickoff_et, home, away, has_odds = by_league["Serie A"][0]
    assert str(matchday_date) == "2025-09-22"


def test_has_odds_flag_reflects_whether_match_is_priced(db_path, conn):
    # Found live 2026-08-21: this tool reported a match as scheduled while
    # generate_club_league_card.py silently excluded it (no odds row -> can't
    # compute EV), and the mismatch looked like a bug until traced to this.
    with patch("core.sports_db.DATABASE_PATH", db_path):
        priced_id = _seed_match(conn, "Serie A", "Inter", "Milan", "2025-09-22T18:00:00.000Z")
        _seed_match(conn, "Serie A", "Roma", "Lazio", "2025-09-22T18:00:00.000Z")   # no odds
        sports_db.add_soccer_betting_odds(
            match_id=priced_id, sportsbook="Test", odds_date="2025-09-20",
            home_moneyline=-150, draw_moneyline=250, away_moneyline=400,
        )

    start, end = matchday_utc_window("2025-09-22")
    by_league = summarize(matches_in_window(conn, start, end))
    by_matchup = {(home, away): has_odds for _, _, _, home, away, has_odds in by_league["Serie A"]}
    assert by_matchup[("Inter", "Milan")] is True
    assert by_matchup[("Roma", "Lazio")] is False


def test_kickoff_et_shows_local_time_not_utc(db_path, conn):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        # 18:30 UTC in August (EDT, UTC-4) = 2:30 PM ET
        _seed_match(conn, "Serie A", "Inter", "Milan", "2025-09-22T18:30:00.000Z")

    start, end = matchday_utc_window("2025-09-22")
    by_league = summarize(matches_in_window(conn, start, end))
    _, _, kickoff_et, _, _, _ = by_league["Serie A"][0]
    assert kickoff_et == "2:30 PM ET"
