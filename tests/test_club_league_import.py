"""
Tests for the pure logic in import_club_squads.py and import_club_player_stats.py:
team-name normalization, team matching, and match-id resolution by team pairing.
Deliberately excludes anything that calls the live TheStatsAPI client -- those are
validated by a manual --dry-run pull, same convention as test_import_wc_match_xg.py.
"""

import import_club_squads as squads
import import_club_player_stats as stats


# ── normalize_team_name ──────────────────────────────────────────────────────────

def test_normalize_strips_common_prefixes():
    assert squads.normalize_team_name("AC Milan") == squads.normalize_team_name("Milan")
    assert squads.normalize_team_name("AS Roma") == squads.normalize_team_name("Roma")


def test_normalize_strips_calcio_and_cfc_suffixes():
    assert squads.normalize_team_name("Cagliari Calcio") == squads.normalize_team_name("Cagliari")
    assert squads.normalize_team_name("Genoa CFC") == squads.normalize_team_name("Genoa")


def test_normalize_strips_trailing_founding_year():
    assert squads.normalize_team_name("Como 1907") == squads.normalize_team_name("Como")
    assert squads.normalize_team_name("AC Pisa 1909") == squads.normalize_team_name("Pisa")


def test_normalize_is_case_insensitive():
    assert squads.normalize_team_name("MILAN") == squads.normalize_team_name("milan")


# ── match_teams_to_api ───────────────────────────────────────────────────────────

def test_match_teams_to_api_matches_via_normalization():
    db_teams = {1: "AC Milan", 2: "Cagliari Calcio"}
    api_teams = [{"id": "tm_1", "name": "Milan"}, {"id": "tm_2", "name": "Cagliari"}]
    matched, unmatched_db, unmatched_api = squads.match_teams_to_api(db_teams, api_teams)
    assert {(tid, name, api["id"]) for tid, name, api in matched} == {
        (1, "AC Milan", "tm_1"), (2, "Cagliari Calcio", "tm_2"),
    }
    assert unmatched_db == []
    assert unmatched_api == []


def test_match_teams_to_api_reports_unmatched_both_sides():
    db_teams = {1: "AC Milan", 2: "Some Unmapped Club"}
    api_teams = [{"id": "tm_1", "name": "Milan"}, {"id": "tm_9", "name": "Unrelated Team"}]
    matched, unmatched_db, unmatched_api = squads.match_teams_to_api(db_teams, api_teams)
    assert len(matched) == 1
    assert unmatched_db == ["Some Unmapped Club"]
    assert unmatched_api == ["Unrelated Team"]


# ── match_db_matches_to_api_by_team_pairing ──────────────────────────────────────

def test_match_by_team_pairing_ignores_date():
    """The real bug this guards against: our soccer_matches date and the API's
    utc_date can disagree (verified for ~13% of Serie A 2025-26, clustered in the
    fixture-congested March-April window -- not a timezone issue, see
    FEATURE-011_PROTOTYPE_LOG.md). Matching must succeed regardless."""
    db_matches = [{"match_id": 101, "home_team_id": 1, "away_team_id": 2,
                  "match_date": "2026-03-08T00:00:00Z", "api_match_id": None}]
    team_api_id = {1: "tm_cagliari", 2: "tm_como"}
    api_matches = [{"id": "mt_555", "home_team": {"id": "tm_cagliari"},
                   "away_team": {"id": "tm_como"}, "utc_date": "2026-03-07T14:00:00.000Z"}]
    result = stats.match_db_matches_to_api_by_team_pairing(db_matches, team_api_id, api_matches)
    assert result == {101: "mt_555"}


def test_match_by_team_pairing_distinguishes_home_and_away():
    """(home, away) is an ORDERED pair -- the return fixture (teams reversed) must
    not accidentally match, since a double round-robin plays it separately."""
    db_matches = [{"match_id": 1, "home_team_id": 1, "away_team_id": 2,
                  "match_date": "2026-01-01T00:00:00Z", "api_match_id": None}]
    team_api_id = {1: "tm_a", 2: "tm_b"}
    api_matches = [
        {"id": "mt_reverse", "home_team": {"id": "tm_b"}, "away_team": {"id": "tm_a"},
         "utc_date": "2026-01-01T00:00:00.000Z"},
    ]
    result = stats.match_db_matches_to_api_by_team_pairing(db_matches, team_api_id, api_matches)
    assert result == {}   # only the reverse fixture exists in api_matches -- no match


def test_match_by_team_pairing_skips_matches_with_unresolved_team():
    """A DB team with no api_team_id mapping (e.g. a name that failed to match) must
    be skipped, not raise or match spuriously."""
    db_matches = [{"match_id": 1, "home_team_id": 1, "away_team_id": 999,
                  "match_date": "2026-01-01T00:00:00Z", "api_match_id": None}]
    team_api_id = {1: "tm_a"}   # team 999 unresolved
    api_matches = [{"id": "mt_x", "home_team": {"id": "tm_a"}, "away_team": {"id": "tm_b"},
                   "utc_date": "2026-01-01T00:00:00.000Z"}]
    result = stats.match_db_matches_to_api_by_team_pairing(db_matches, team_api_id, api_matches)
    assert result == {}


def test_match_by_team_pairing_only_returns_resolvable_matches():
    db_matches = [
        {"match_id": 1, "home_team_id": 1, "away_team_id": 2,
         "match_date": "2026-01-01T00:00:00Z", "api_match_id": None},
        {"match_id": 2, "home_team_id": 2, "away_team_id": 1,
         "match_date": "2026-05-01T00:00:00Z", "api_match_id": None},
    ]
    team_api_id = {1: "tm_a", 2: "tm_b"}
    api_matches = [
        {"id": "mt_first_leg", "home_team": {"id": "tm_a"}, "away_team": {"id": "tm_b"},
         "utc_date": "2026-01-01T00:00:00.000Z"},
        # return leg not present in api_matches -- match 2 should stay unresolved
    ]
    result = stats.match_db_matches_to_api_by_team_pairing(db_matches, team_api_id, api_matches)
    assert result == {1: "mt_first_leg"}
