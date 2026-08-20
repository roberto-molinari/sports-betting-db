"""
Per-league team-name maps: football-data.co.uk's short CSV team name -> the
canonical name already stored in soccer_teams (TheStatsAPI-sourced for every league
except Serie A, which stays on football-data.org until the tracked fast-follow
migration -- see BUGS.md and core/leagues.py).

Consolidates what were three independent, Serie-A-only, hand-maintained maps
(update_serie_a_results.py's CSV_TEAM_NAME_MAP/API_TEAM_NAME_MAP,
import_serie_a_odds.py's db_override_map/legacy_map, import_serie_a_market_odds.py's
own TEAM_NAME_MAP) into one shared module both odds scripts (import_league_betting_
odds.py, import_league_market_odds.py) read from -- previously each new league would
have meant tripling that duplication.

SERIE_A's entries were derived by running the OLD canonical_team_name() resolution
(import_serie_a_odds.py, pre-2026-08-10) against every team name that actually
appears in Serie A's football-data.co.uk files (2023-24 through 2025-26) -- byte-
identical results preserved, not re-derived by hand, so Serie A's already-working
odds ingestion sees no behavior change.

The 4 new leagues' entries were built semi-automated: exact match (case/punctuation-
normalized) first, a light fuzzy match for the remainder, hand-confirmed against the
real football-data.co.uk (2024-25, 2025-26) and TheStatsAPI-sourced soccer_teams team
lists -- zero names were left unmatched for either season actually in scope.
"""

TEAM_NAME_MAPS = {
    "Serie A": {
        "Atalanta": "Atalanta",
        "Bologna": "Bologna",
        "Cagliari": "Cagliari Calcio",
        "Como": "Como 1907",
        "Cremonese": "Cremonese",
        "Empoli": "Empoli",
        "Fiorentina": "Fiorentina",
        "Frosinone": "Frosinone Calcio",
        "Genoa": "Genoa CFC",
        "Inter": "Inter",
        "Juventus": "Juventus",
        "Lazio": "Lazio",
        "Lecce": "Lecce",
        "Milan": "AC Milan",
        "Monza": "Monza",
        "Napoli": "Napoli",
        "Parma": "Parma Calcio 1913",
        "Pisa": "AC Pisa 1909",
        "Roma": "AS Roma",
        "Salernitana": "Salernitana",
        "Sassuolo": "Sassuolo",
        "Torino": "Torino",
        "Udinese": "Udinese",
        "Venezia": "Venezia FC",
        "Verona": "Hellas Verona",
    },
    "Premier League": {
        "Arsenal": "Arsenal",
        "Aston Villa": "Aston Villa",
        "Bournemouth": "Bournemouth",
        "Brentford": "Brentford",
        "Brighton": "Brighton & Hove Albion",
        "Burnley": "Burnley",
        "Chelsea": "Chelsea",
        "Crystal Palace": "Crystal Palace",
        "Everton": "Everton",
        "Fulham": "Fulham",
        "Ipswich": "Ipswich Town",
        "Leeds": "Leeds United",
        "Leicester": "Leicester City",
        "Liverpool": "Liverpool",
        "Luton": "Luton Town",
        "Man City": "Manchester City",
        "Man United": "Manchester United",
        "Newcastle": "Newcastle United",
        "Nott'm Forest": "Nottingham Forest",
        "Southampton": "Southampton",
        "Sunderland": "Sunderland",
        "Tottenham": "Tottenham Hotspur",
        "West Ham": "West Ham United",
        "Wolves": "Wolverhampton",
    },
    "Bundesliga": {
        "Augsburg": "FC Augsburg",
        "Bayern Munich": "FC Bayern München",
        "Bochum": "VfL Bochum 1848",
        "Darmstadt": "Darmstadt 98",
        "Dortmund": "Borussia Dortmund",
        "Ein Frankfurt": "Eintracht Frankfurt",
        "FC Koln": "1. FC Köln",
        "Freiburg": "SC Freiburg",
        "Hamburg": "Hamburger SV",
        "Heidenheim": "1. FC Heidenheim",
        "Hertha": "Hertha BSC",
        "Hoffenheim": "TSG Hoffenheim",
        "Holstein Kiel": "Holstein Kiel",
        "Leverkusen": "Bayer 04 Leverkusen",
        "M'gladbach": "Borussia M'gladbach",
        "Mainz": "1. FSV Mainz 05",
        "RB Leipzig": "RB Leipzig",
        "Schalke 04": "FC Schalke 04",
        "St Pauli": "FC St. Pauli",
        "Stuttgart": "VfB Stuttgart",
        "Union Berlin": "1. FC Union Berlin",
        "Werder Bremen": "SV Werder Bremen",
        "Wolfsburg": "VfL Wolfsburg",
    },
    "La Liga": {
        "Alaves": "Deportivo Alavés",
        "Almeria": "Almería",
        "Ath Bilbao": "Athletic Club",
        "Ath Madrid": "Atlético Madrid",
        "Barcelona": "Barcelona",
        "Betis": "Real Betis",
        "Cadiz": "Cádiz",
        "Celta": "Celta Vigo",
        "Elche": "Elche",
        "Espanol": "Espanyol",
        "Getafe": "Getafe",
        "Girona": "Girona FC",
        "Las Palmas": "Las Palmas",
        "Leganes": "Leganés",
        "Levante": "Levante UD",
        "Mallorca": "Mallorca",
        "Osasuna": "Osasuna",
        "Oviedo": "Real Oviedo",
        "Real Madrid": "Real Madrid",
        "Sevilla": "Sevilla",
        "Sociedad": "Real Sociedad",
        "Valencia": "Valencia",
        "Valladolid": "Real Valladolid",
        "Vallecano": "Rayo Vallecano",
        "Villarreal": "Villarreal",
    },
    "Ligue 1": {
        "Angers": "Angers",
        "Auxerre": "Auxerre",
        "Brest": "Stade Brestois",
        "Clermont": "Clermont Foot",
        "Le Havre": "Le Havre",
        "Lens": "RC Lens",
        "Lille": "Lille",
        "Lorient": "Lorient",
        "Lyon": "Olympique Lyonnais",
        "Marseille": "Olympique de Marseille",
        "Metz": "Metz",
        "Monaco": "AS Monaco",
        "Montpellier": "Montpellier",
        "Nantes": "Nantes",
        "Nice": "Nice",
        "Paris FC": "Paris FC",
        "Paris SG": "Paris Saint-Germain",
        "Reims": "Stade de Reims",
        "Rennes": "Stade Rennais",
        "St Etienne": "Saint-Étienne",
        "Strasbourg": "RC Strasbourg",
        "Toulouse": "Toulouse",
    },
}


def canonical_team_name(league, csv_name):
    """football-data.co.uk's short name -> our DB's soccer_teams.name for `league`.
    Falls back to the raw csv_name unchanged if there's no mapping entry -- most
    already match the DB name directly (e.g. "Arsenal"), so only the genuinely
    different-spelling teams need an entry at all."""
    csv_name = (csv_name or "").strip()
    return TEAM_NAME_MAPS.get(league, {}).get(csv_name, csv_name)
