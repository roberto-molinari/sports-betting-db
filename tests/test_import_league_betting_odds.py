"""Tests for import_league_betting_odds.py's runtime diagnostics (2026-08-23) --
when a CSV/API row can't be matched to a database row, the run must print WHY,
not just a silent count, so a real problem (e.g. a duplicate-fixture home/away
mismatch, see import_league_matches.py's find_conflicting_pairing()) surfaces
immediately instead of days later. Excludes anything that calls a live API,
same convention as test_import_league_matches.py."""

from unittest.mock import patch

import core.sports_db as sports_db
import import_league_betting_odds as odds


def _row(home, away, date="22/08/2026"):
    return {"Date": date, "HomeTeam": home, "AwayTeam": away,
            "B365H": "", "B365D": "", "B365A": "", "AHh": "",
            "B365>2.5": "", "B365<2.5": "", "B365AHH": "", "B365AHA": ""}


def test_unknown_team_is_logged_with_the_unmapped_name(db_path, conn, capsys):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        sports_db.ensure_soccer_team("Real Madrid", "La Liga", "Spain")

    stats = odds._import_rows(conn, "La Liga", [_row("Not A Real Team", "Real Madrid")],
                               2026, "Bet365", insert_missing=False, future_only=False,
                               source_label="test")
    out = capsys.readouterr().out

    assert stats["unknown_team"] == 1
    assert "UNKNOWN TEAM" in out
    assert "Not A Real Team" in out


def test_no_match_is_logged_with_both_team_names(db_path, conn, capsys):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        sports_db.ensure_soccer_team("Real Madrid", "La Liga", "Spain")
        sports_db.ensure_soccer_team("Barcelona", "La Liga", "Spain")

    stats = odds._import_rows(conn, "La Liga", [_row("Real Madrid", "Barcelona", date="22/08/2026")],
                               2026, "Bet365", insert_missing=False, future_only=False,
                               source_label="test")
    out = capsys.readouterr().out

    assert stats["no_match"] == 1
    assert "NO MATCH" in out
    assert "Real Madrid" in out and "Barcelona" in out


def test_matched_row_prints_nothing_extra(db_path, conn, capsys):
    with patch("core.sports_db.DATABASE_PATH", db_path):
        home = sports_db.ensure_soccer_team("Real Madrid", "La Liga", "Spain")
        away = sports_db.ensure_soccer_team("Barcelona", "La Liga", "Spain")
        sports_db.add_soccer_match("La Liga", 2026, home, away, "2026-08-22T20:00:00.000Z")

    stats = odds._import_rows(conn, "La Liga", [_row("Real Madrid", "Barcelona", date="22/08/2026")],
                               2026, "Bet365", insert_missing=False, future_only=False,
                               source_label="test")
    out = capsys.readouterr().out

    assert stats["no_match"] == 0
    assert stats["unknown_team"] == 0
    assert "NO MATCH" not in out
    assert "UNKNOWN TEAM" not in out
