"""
Approximate FIFA world rankings for the 48 World Cup 2026 nations.

TheStatsAPI does not expose FIFA rankings, so these are hardcoded. They are used
ONLY as a fallback: when a team's squad has too little club-stat coverage to
compute strength from player data, compute_wc_team_strength.py derives its
lambdas from this ranking instead of flat baseline.

Values are approximate (late-2025 ballpark) and intentionally easy to edit —
exact numbers matter little since this only affects thin-coverage teams, and the
model normalizes around a baseline. Keys MUST match soccer_wc_teams.name exactly
(the names TheStatsAPI returns for competition comp_6107).
"""

FIFA_RANKINGS = {
    "Argentina": 1,
    "Spain": 2,
    "France": 3,
    "England": 4,
    "Brazil": 5,
    "Portugal": 6,
    "Netherlands": 7,
    "Belgium": 8,
    "Germany": 9,
    "Croatia": 10,
    "Morocco": 11,
    "Colombia": 13,
    "Uruguay": 14,
    "Japan": 15,
    "USA": 16,
    "Mexico": 17,
    "Switzerland": 18,
    "Senegal": 19,
    "Iran": 20,
    "Austria": 22,
    "Ecuador": 23,
    "South Korea": 24,
    "Australia": 25,
    "Türkiye": 26,
    "Sweden": 27,
    "Canada": 31,
    "Egypt": 32,
    "Scotland": 33,
    "Côte d'Ivoire": 34,
    "Paraguay": 35,
    "Czechia": 36,
    "Qatar": 37,
    "Tunisia": 38,
    "Norway": 39,
    "Panama": 41,
    "Algeria": 43,
    "South Africa": 56,
    "Uzbekistan": 57,
    "Saudi Arabia": 58,
    "Iraq": 59,
    "Jordan": 62,
    "Cape Verde": 70,
    "Ghana": 73,
    "Bosnia & Herzegovina": 74,
    "DR Congo": 53,
    "Curaçao": 82,
    "Haiti": 83,
    "New Zealand": 86,
}

# Used when a team is missing above (kept low so unknowns aren't over-rated).
DEFAULT_RANK = 80


def get_fifa_ranking(team_name, default=DEFAULT_RANK):
    """Return the approximate FIFA rank for a national team name."""
    return FIFA_RANKINGS.get(team_name, default)
