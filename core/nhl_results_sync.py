"""
Reusable NHL results synchronization logic.

This module powers both data_collector methods and the update_nhl_results CLI.
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Iterable, Optional

from core.sports_db import (
    add_nhl_match,
    ensure_nhl_team,
    init_database,
    update_nhl_match_result,
)

try:
    from nhlpy import NHLClient
except ImportError:  # package not installed
    NHLClient = None


FINAL_STATES = {"FINAL", "OFF"}


def current_nhl_season_year(now: Optional[datetime] = None) -> int:
    """Return NHL season start year (e.g. 2025 for 2025-26 season)."""
    now = now or datetime.now()
    return now.year if now.month >= 9 else now.year - 1


def season_code(start_year: int) -> str:
    """Convert a season start year into NHL API season code (YYYYYYYY)."""
    return f"{start_year}{start_year + 1:04d}"


def _team_abbr(team: dict) -> Optional[str]:
    return team.get("abbrev") or team.get("abbr")


def _status_from_game_state(game_state: str) -> str:
    return "completed" if game_state in FINAL_STATES else "scheduled"


def sync_nhl_results(
    season: int,
    *,
    completed_only: bool = False,
    initialize_db: bool = True,
    verbose: bool = True,
) -> Dict[str, int]:
    """
    Sync NHL matches/results for a season into nhl_matches.

    Args:
        season: Season start year, e.g. 2025 for 2025-26.
        completed_only: If True, only persist completed games.
        initialize_db: If True, initialize DB schema first.
        verbose: Print progress.

    Returns:
        Stats dictionary.

    Raises:
        RuntimeError: if nhl-api-py/nhlpy is unavailable.
    """
    if NHLClient is None:
        raise RuntimeError("nhl-api-py (import name: nhlpy) is not installed.")

    if initialize_db:
        init_database()

    season_str = season_code(season)
    client = NHLClient()

    if verbose:
        print(f"Fetching NHL teams for season {season_str}...")

    teams_list = client.teams.teams()
    teams_by_abbr: Dict[str, int] = {}
    for team in teams_list:
        abbr = team.get("abbr")
        if not abbr:
            continue
        team_id = ensure_nhl_team(team.get("name", ""), team.get("country", "USA/Canada"))
        teams_by_abbr[abbr] = team_id

    stats = {
        "season": season,
        "teams": len(teams_by_abbr),
        "games_seen": 0,
        "games_written": 0,
        "completed_written": 0,
        "scheduled_written": 0,
        "results_updated": 0,
        "unknown_team": 0,
        "skipped_state": 0,
        "schedule_errors": 0,
    }

    seen_games = set()

    if verbose:
        print(f"Fetching team schedules for {season_str}...")

    for abbr in sorted(teams_by_abbr):
        try:
            sched = client.schedule.team_season_schedule(team_abbr=abbr, season=season_str)
        except Exception as exc:
            stats["schedule_errors"] += 1
            if verbose:
                print(f"  Error fetching schedule for {abbr}: {exc}")
            continue

        for game in sched.get("games", []):
            home_team = game.get("homeTeam") or {}
            away_team = game.get("awayTeam") or {}
            home_abbr = _team_abbr(home_team)
            away_abbr = _team_abbr(away_team)

            game_key = game.get("id") or game.get("gameId")
            if game_key is None:
                game_key = (
                    home_abbr,
                    away_abbr,
                    game.get("startTimeUTC"),
                )

            if game_key in seen_games:
                continue
            seen_games.add(game_key)
            stats["games_seen"] += 1

            if not home_abbr or not away_abbr:
                stats["unknown_team"] += 1
                continue

            home_id = teams_by_abbr.get(home_abbr)
            away_id = teams_by_abbr.get(away_abbr)
            if not home_id or not away_id:
                stats["unknown_team"] += 1
                continue

            game_state = game.get("gameState", "")
            status = _status_from_game_state(game_state)
            if completed_only and status != "completed":
                stats["skipped_state"] += 1
                continue

            match_date = game.get("startTimeUTC")
            if not match_date:
                stats["skipped_state"] += 1
                continue

            match_id = add_nhl_match(
                season=season,
                home_team_id=home_id,
                away_team_id=away_id,
                match_date=match_date,
                status=status,
            )
            stats["games_written"] += 1
            if status == "completed":
                stats["completed_written"] += 1
            else:
                stats["scheduled_written"] += 1

            if status == "completed":
                home_score = home_team.get("score")
                away_score = away_team.get("score")
                if home_score is not None and away_score is not None:
                    update_nhl_match_result(
                        match_id=match_id,
                        home_score=home_score,
                        away_score=away_score,
                    )
                    stats["results_updated"] += 1

    return stats


def sync_many_nhl_seasons(
    seasons: Iterable[int],
    *,
    completed_only: bool = False,
    initialize_db: bool = True,
    verbose: bool = True,
) -> Dict[str, int]:
    """Sync multiple NHL seasons and aggregate stats."""
    total = {
        "seasons": 0,
        "games_seen": 0,
        "games_written": 0,
        "completed_written": 0,
        "scheduled_written": 0,
        "results_updated": 0,
        "unknown_team": 0,
        "skipped_state": 0,
        "schedule_errors": 0,
    }

    init_once = initialize_db
    for season in seasons:
        stats = sync_nhl_results(
            season,
            completed_only=completed_only,
            initialize_db=init_once,
            verbose=verbose,
        )
        init_once = False
        total["seasons"] += 1
        for key in (
            "games_seen",
            "games_written",
            "completed_written",
            "scheduled_written",
            "results_updated",
            "unknown_team",
            "skipped_state",
            "schedule_errors",
        ):
            total[key] += stats[key]

    return total
