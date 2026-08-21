"""Tests for core/team_name_maps.py -- confirms the Odds API name entries
added 2026-08-21 (found live: 9 unmapped names across 4 leagues were silently
blocking live odds import for whichever matches involved those teams, e.g.
Inter vs Monza never got a price because "Inter Milan" had no entry)."""
from core.team_name_maps import canonical_team_name


def test_odds_api_names_resolve_to_canonical():
    cases = [
        ("Serie A", "Inter Milan", "Inter"),
        ("Serie A", "Atalanta BC", "Atalanta"),
        ("Premier League", "Brighton and Hove Albion", "Brighton & Hove Albion"),
        ("Bundesliga", "Bayer Leverkusen", "Bayer 04 Leverkusen"),
        ("Bundesliga", "Borussia Monchengladbach", "Borussia M'gladbach"),
        ("Bundesliga", "Elversberg", "SV 07 Elversberg"),
        ("Bundesliga", "FSV Mainz 05", "1. FSV Mainz 05"),
        ("Bundesliga", "SC Paderborn", "SC Paderborn 07"),
        ("La Liga", "Athletic Bilbao", "Athletic Club"),
        ("La Liga", "CA Osasuna", "Osasuna"),
        ("La Liga", "Elche CF", "Elche"),
        ("La Liga", "Real Racing Club de Santander", "Racing de Santander"),
        ("Ligue 1", "Le Mans FC", "Le Mans"),
        ("Ligue 1", "Paris Saint Germain", "Paris Saint-Germain"),
    ]
    for league, odds_api_name, expected_canonical in cases:
        assert canonical_team_name(league, odds_api_name) == expected_canonical


def test_football_data_couk_names_still_resolve():
    # The Odds API entries are additions, not replacements -- the existing
    # football-data.co.uk short names must keep working unchanged.
    assert canonical_team_name("Serie A", "Inter") == "Inter"
    assert canonical_team_name("Bundesliga", "Leverkusen") == "Bayer 04 Leverkusen"
    assert canonical_team_name("La Liga", "Ath Bilbao") == "Athletic Club"


def test_unknown_name_falls_back_to_itself():
    assert canonical_team_name("Serie A", "Some Team Nobody Mapped") == "Some Team Nobody Mapped"
