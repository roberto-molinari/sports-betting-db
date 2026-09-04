"""
Per-league config registry -- the single source of truth for each league's external-
source identifiers. To add a new league, add an entry here; every ingestion script
reads from this dict instead of hardcoding per-league values.

A plain Python dict, not a database table: this is static, developer-owned config,
the same category as compute_club_player_strength.py's PLAYER_RATING_* constants,
which also stay in code so model_metrics_report.py can record their exact values per run.

Fields:
  country                     matches soccer_teams.country; used by
                               ensure_soccer_team's cross-country collision check.
  thestatsapi_competition_id  used by import_league_matches.py. Serie A's
                               (comp_5840, registered 2026-09-04) is the last of
                               the 5 top-flight leagues to switch onto this --
                               its existing soccer_matches rows had to be
                               reconciled/id-stamped first
                               (migrate_serie_a_thestatsapi_ids.py, BUGS.md) since
                               import_league_matches.py's dedup is keyed purely
                               on this id; registering it before that step would
                               have caused duplicate inserts.
  footballdatacouk_code       football-data.co.uk's CSV code (e.g. "E0"). None for
                               every feeder division -- no odds tracked there yet.
  odds_api_sport_key          The Odds API's sport key. None for the same reason.
  lower_division              this league's feeder division (a key into this same
                               dict) -- needed for cross-league promotion/call-up
                               history (BUG-010). None if it has no tracked feeder.

Feeder-division betting (Championship, 2. Bundesliga, LaLiga 2, Ligue 2, Serie B) is
a deliberate gap, not an oversight: turning it on later is just filling in
footballdatacouk_code/odds_api_sport_key per division (import_league_matches.py
already keys its scheduled-vs-finished-only behavior off exactly those two fields).
Codes already confirmed live: soccer_efl_champ / soccer_germany_bundesliga2 /
soccer_spain_segunda_division / soccer_france_ligue_two / soccer_italy_serie_b on
The Odds API; E1/D2/SP2/F2/I2 on football-data.co.uk.
"""

LEAGUES = {
    "Serie A": {
        "country": "Italy",
        "thestatsapi_competition_id": "comp_5840",
        "footballdatacouk_code": "I1",
        "odds_api_sport_key": "soccer_italy_serie_a",
        "lower_division": "Serie B",
    },
    "Serie B": {
        "country": "Italy",
        "thestatsapi_competition_id": "comp_5450",
        "footballdatacouk_code": None,
        "odds_api_sport_key": None,
        "lower_division": None,
    },
    "Premier League": {
        "country": "England",
        "thestatsapi_competition_id": "comp_3039",
        "footballdatacouk_code": "E0",
        "odds_api_sport_key": "soccer_epl",
        "lower_division": "Championship",
    },
    "Championship": {
        "country": "England",
        "thestatsapi_competition_id": "comp_8321",
        "footballdatacouk_code": None,
        "odds_api_sport_key": None,
        "lower_division": None,
    },
    "Bundesliga": {
        "country": "Germany",
        "thestatsapi_competition_id": "comp_4643",
        "footballdatacouk_code": "D1",
        "odds_api_sport_key": "soccer_germany_bundesliga",
        "lower_division": "2. Bundesliga",
    },
    "2. Bundesliga": {
        "country": "Germany",
        "thestatsapi_competition_id": "comp_0406",
        "footballdatacouk_code": None,
        "odds_api_sport_key": None,
        "lower_division": None,
    },
    "La Liga": {
        "country": "Spain",
        "thestatsapi_competition_id": "comp_8814",
        "footballdatacouk_code": "SP1",
        "odds_api_sport_key": "soccer_spain_la_liga",
        "lower_division": "LaLiga 2",
    },
    "LaLiga 2": {
        "country": "Spain",
        "thestatsapi_competition_id": "comp_0976",
        "footballdatacouk_code": None,
        "odds_api_sport_key": None,
        "lower_division": None,
    },
    "Ligue 1": {
        "country": "France",
        "thestatsapi_competition_id": "comp_0256",
        "footballdatacouk_code": "F1",
        "odds_api_sport_key": "soccer_france_ligue_one",
        "lower_division": "Ligue 2",
    },
    "Ligue 2": {
        "country": "France",
        "thestatsapi_competition_id": "comp_9777",
        "footballdatacouk_code": None,
        "odds_api_sport_key": None,
        "lower_division": None,
    },
}

# football-data.co.uk's season-label format (e.g. 2024 -> "2425") -- confirmed the
# SAME format across every league on that site, not league-specific, so this is one
# shared table rather than per-league.
FOOTBALLDATACOUK_SEASON_CODE = {2022: "2223", 2023: "2324", 2024: "2425", 2025: "2526", 2026: "2627"}


def has_odds_source(league):
    """True if `league` has a configured odds source (football-data.co.uk and/or The
    Odds API) -- used by import_league_matches.py to decide whether to import
    scheduled (not just finished) matches. Every top-flight league in LEAGUES has
    both; every second-tier/feeder division currently has neither."""
    entry = LEAGUES[league]
    return entry["footballdatacouk_code"] is not None or entry["odds_api_sport_key"] is not None
