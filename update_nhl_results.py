"""
Unified NHL Results Updater
===========================
Syncs NHL matches/results for a season into nhl_matches.

Usage:
    python update_nhl_results.py
    python update_nhl_results.py --season 2025
    python update_nhl_results.py --season 2025 --completed-only
"""

import argparse
from datetime import datetime

from nhl_results_sync import current_nhl_season_year, sync_nhl_results


def main():
    parser = argparse.ArgumentParser(
        description="Sync NHL fixtures and results from NHL API (via nhl-api-py)."
    )
    parser.add_argument(
        "--season",
        type=int,
        default=None,
        metavar="YYYY",
        help="Season start year to sync (default: current NHL season).",
    )
    parser.add_argument(
        "--completed-only",
        action="store_true",
        help="Only write completed games (useful for historical backfills).",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Reduce progress logging.",
    )
    args = parser.parse_args()

    season = args.season or current_nhl_season_year()
    print(
        f"=== NHL Sync  {datetime.now().strftime('%Y-%m-%d %H:%M')}  "
        f"season={season}  completed_only={args.completed_only} ===\n"
    )

    try:
        stats = sync_nhl_results(
            season,
            completed_only=args.completed_only,
            initialize_db=True,
            verbose=not args.quiet,
        )
    except RuntimeError as exc:
        parser.error(str(exc))

    print("Summary:")
    print(f"  teams loaded:       {stats['teams']}")
    print(f"  unique games seen:  {stats['games_seen']}")
    print(f"  matches written:    {stats['games_written']}")
    print(f"  completed written:  {stats['completed_written']}")
    print(f"  scheduled written:  {stats['scheduled_written']}")
    print(f"  results updated:    {stats['results_updated']}")
    print(f"  unknown team rows:  {stats['unknown_team']}")
    print(f"  skipped by state:   {stats['skipped_state']}")
    print(f"  schedule errors:    {stats['schedule_errors']}")
    print("\nDone.")


if __name__ == "__main__":
    main()
