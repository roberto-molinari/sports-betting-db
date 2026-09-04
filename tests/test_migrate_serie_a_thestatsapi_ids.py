"""Tests for migrate_serie_a_thestatsapi_ids.py's pure matching logic. Excludes
anything that calls the live TheStatsAPI client, same convention as
test_import_league_matches.py."""

from migrate_serie_a_thestatsapi_ids import build_pair_index, best_match, SERIE_A_NAME_MAP


def _api_match(api_id, home, away, date, home_score=1, away_score=0, status="finished"):
    return {
        "id": api_id, "home_team": {"name": home}, "away_team": {"name": away},
        "utc_date": f"{date}T18:00:00.000Z",
        "score": {"home": home_score, "away": away_score},
        "status": status,
    }


def test_build_pair_index_groups_by_team_names():
    matches = [_api_match("mt_1", "Milan", "Roma", "2025-09-01"),
               _api_match("mt_2", "Roma", "Milan", "2026-01-10")]
    index = build_pair_index(matches)
    assert [m["id"] for m in index[("Milan", "Roma")]] == ["mt_1"]
    assert [m["id"] for m in index[("Roma", "Milan")]] == ["mt_2"]


def test_best_match_applies_serie_a_name_map():
    """DB name 'AC Milan' must resolve to TheStatsAPI's 'Milan' via SERIE_A_NAME_MAP."""
    assert SERIE_A_NAME_MAP["AC Milan"] == "Milan"
    matches = [_api_match("mt_1", "Milan", "Roma", "2025-09-14")]
    index = build_pair_index(matches)
    found = best_match("AC Milan", "AS Roma", "2025-09-14T18:00:00Z", index)
    assert found is not None and found["id"] == "mt_1"


def test_best_match_picks_closest_date_within_tolerance():
    matches = [_api_match("mt_near", "Milan", "Roma", "2025-09-14"),
               _api_match("mt_far", "Milan", "Roma", "2025-09-20")]
    index = build_pair_index(matches)
    found = best_match("AC Milan", "AS Roma", "2025-09-13T00:00:00Z", index)
    assert found["id"] == "mt_near"


def test_best_match_returns_none_outside_tolerance():
    matches = [_api_match("mt_1", "Milan", "Roma", "2025-09-14")]
    index = build_pair_index(matches)
    found = best_match("AC Milan", "AS Roma", "2025-10-01T00:00:00Z", index)
    assert found is None


def test_best_match_returns_none_for_unknown_pairing():
    matches = [_api_match("mt_1", "Milan", "Roma", "2025-09-14")]
    index = build_pair_index(matches)
    found = best_match("Juventus", "Napoli", "2025-09-14T00:00:00Z", index)
    assert found is None


def test_best_match_disambiguates_a_repeated_pairing_by_closest_date():
    """Real case found live (BUG-025 follow-on, match_id 6997): Spezia v Hellas
    Verona met twice in one season with the same orientation. Closest-date
    selection alone must pick the right one when dates are far apart -- the
    caller layers a score check on top for a completed match, but the date
    logic itself must not just grab the first candidate in list order."""
    matches = [_api_match("mt_regular", "Spezia", "Hellas Verona", "2023-03-05", 0, 0),
               _api_match("mt_decider", "Spezia", "Hellas Verona", "2023-06-11", 1, 3)]
    index = build_pair_index(matches)
    found = best_match("Spezia", "Hellas Verona", "2023-06-11T18:45:00+00:00", index)
    assert found["id"] == "mt_decider"
