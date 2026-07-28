"""Tests for the pure logic in import_wc_fifa_xg.py (FEATURE-008 extension):
team-name normalization, page-1/xG text parsing, team-pair matching, and the
score-agreement sanity check (including the extra-time-inclusive-score
quirk this was built to catch). Deliberately excludes the hub-page scraping
and PDF-download path -- those are validated by a manual --limit --dry-run
pull, same convention as this repo's other TheStatsAPI/web import scripts."""

from datetime import date

import import_wc_fifa_xg as fx


def test_normalize_team_applies_known_aliases():
    assert fx.normalize_team("Korea Republic") == "South Korea"
    assert fx.normalize_team("Cabo Verde") == "Cape Verde"
    assert fx.normalize_team("Brazil") == "Brazil"


def test_parse_page1_extracts_score_teams_and_date():
    text = "USA 4 - 1 Paraguay\nGroup D - Match 4\n12 June 2026\n18:00 Kick Off"
    report = fx.parse_page1(text)
    assert report["home"] == "USA"
    assert report["away"] == "Paraguay"
    assert report["home_score"] == 4
    assert report["away_score"] == 1
    assert report["date"] == date(2026, 6, 12)


def test_parse_page1_applies_alias_to_parsed_names():
    text = "Korea Republic 2 - 1 Czech Republic\nGroup A - Match 2\n13 June 2026"
    report = fx.parse_page1(text)
    assert report["home"] == "South Korea"
    assert report["away"] == "Czechia"


def test_parse_page1_none_when_not_a_score_line():
    assert fx.parse_page1("Not a match report at all") is None
    assert fx.parse_page1("") is None


def test_parse_xg_extracts_both_values():
    text = ("12 June 2026 - Los Angeles Stadium - 18:00\n"
            "Match Summary - Key Statistics\n"
            "Total 59.5% 11.8% 28.7% Total\n4 Goals 1\n1.88 xG (Expected Goals) 0.6\n")
    assert fx.parse_xg(text) == (1.88, 0.6)


def test_parse_xg_none_when_absent():
    assert fx.parse_xg("Match Summary - Teams\nSTARTING\n24 GK Matt FREESE") is None


def _match(match_id, home, away, home_score, away_score,
           et_home=None, et_away=None, match_date="2026-06-12"):
    return {
        "match_id": match_id, "home": home, "away": away,
        "home_score": home_score, "away_score": away_score,
        "extra_time_home_score": et_home, "extra_time_away_score": et_away,
        "match_date": match_date,
    }


def _report(home, away, home_score, away_score, report_date=date(2026, 6, 12)):
    return {"home": home, "away": away, "home_score": home_score,
            "away_score": away_score, "date": report_date}


def test_find_match_by_unordered_team_pair():
    matches = [_match(1, "USA", "Paraguay", 4, 1)]
    report = _report("Paraguay", "USA", 1, 4)   # FIFA lists them in the other order
    assert fx.find_match(matches, report)["match_id"] == 1


def test_find_match_none_when_no_pair_matches():
    matches = [_match(1, "USA", "Paraguay", 4, 1)]
    report = _report("Brazil", "Norway", 2, 1)
    assert fx.find_match(matches, report) is None


def test_find_match_breaks_ties_on_closest_date():
    matches = [
        _match(1, "USA", "Paraguay", 4, 1, match_date="2026-06-12"),
        _match(2, "USA", "Paraguay", 1, 1, match_date="2026-07-05"),
    ]
    report = _report("USA", "Paraguay", 1, 1, report_date=date(2026, 7, 4))
    assert fx.find_match(matches, report)["match_id"] == 2


def test_final_score_uses_regulation_when_no_extra_time():
    m = _match(1, "Germany", "Curacao", 4, 0)
    assert fx.final_score(m) == (4, 0)


def test_final_score_uses_extra_time_inclusive_when_decided_in_et():
    """The bug this was built to catch: FIFA's page-1 score for an ET-decided
    tie is the FINAL (extra-time-inclusive) score, not the bare 90' score --
    confirmed live on Belgium 2-2(3-2) Senegal, which reported as 3-2."""
    m = _match(1, "Belgium", "Senegal", 2, 2, et_home=3, et_away=2)
    assert fx.final_score(m) == (3, 2)


def test_scores_agree_true_for_matching_orientation():
    m = _match(1, "USA", "Paraguay", 4, 1)
    report = _report("USA", "Paraguay", 4, 1)
    assert fx.scores_agree(m, report) is True


def test_scores_agree_true_for_swapped_orientation():
    m = _match(1, "USA", "Paraguay", 4, 1)
    report = _report("Paraguay", "USA", 1, 4)
    assert fx.scores_agree(m, report) is True


def test_scores_agree_false_on_real_mismatch():
    m = _match(1, "USA", "Paraguay", 4, 1)
    report = _report("USA", "Paraguay", 2, 2)
    assert fx.scores_agree(m, report) is False


def test_scores_agree_uses_extra_time_inclusive_score():
    m = _match(1, "Belgium", "Senegal", 2, 2, et_home=3, et_away=2)
    report = _report("Belgium", "Senegal", 3, 2)
    assert fx.scores_agree(m, report) is True
    # the bare regulation score would NOT agree -- confirms the fix matters.
    assert fx.scores_agree(m, _report("Belgium", "Senegal", 2, 2)) is False


def test_oriented_xg_matches_our_home_away():
    m = _match(1, "USA", "Paraguay", 4, 1)
    report = dict(_report("USA", "Paraguay", 4, 1), home_xg=1.88, away_xg=0.6)
    assert fx.oriented_xg(m, report) == (1.88, 0.6)


def test_oriented_xg_swaps_when_fifa_order_differs():
    m = _match(1, "USA", "Paraguay", 4, 1)
    report = dict(_report("Paraguay", "USA", 1, 4), home_xg=0.6, away_xg=1.88)
    assert fx.oriented_xg(m, report) == (1.88, 0.6)


def test_already_fetched_reflects_stored_rows(db_path):
    import sqlite3
    import core.sports_db as sdb

    home = sdb.ensure_wc_team("Team A")
    away = sdb.ensure_wc_team("Team B")
    match_id = sdb.ensure_wc_match("2026-06-12 18:00:00", home, away, stage="Group")
    sdb.update_wc_match_result(match_id, 1, 0)

    conn = sqlite3.connect(db_path)
    assert fx.already_fetched(conn, match_id) is False
    sdb.upsert_wc_external_xg(match_id=match_id, source="fifa_official",
                              home_xg=1.2, away_xg=0.8, fetched_at="2026-07-07T00:00:00+00:00")
    assert fx.already_fetched(conn, match_id) is True
    conn.close()
