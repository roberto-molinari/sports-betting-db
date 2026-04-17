"""
API Field Coverage Checker
===========================
Fetches all completed Serie A matches for a season from football-data.org
and reports what percentage of matches have each candidate field populated.

Usage:
    python check_api_field_coverage.py <api_key>
    python check_api_field_coverage.py <api_key> --season 2024
"""

import requests
import time
import argparse
from datetime import datetime


API_BASE = 'https://api.football-data.org/v4'
SERIE_A_CODE = 'SA'


def check_coverage(api_key, season):
    headers = {'X-Auth-Token': api_key}
    params = {'season': season}

    print(f"Fetching completed Serie A matches for season {season}...", end=' ', flush=True)
    r = requests.get(f"{API_BASE}/competitions/{SERIE_A_CODE}/matches",
                     headers=headers, params=params, timeout=15)
    r.raise_for_status()
    all_matches = r.json().get('matches', [])

    finished = [m for m in all_matches if m['status'] == 'FINISHED']
    total = len(finished)
    print(f"{total} finished matches found\n")

    if total == 0:
        print("No finished matches to analyse.")
        return

    def pct(n):
        return f"{n}/{total} ({100*n//total}%)"

    def count(fn):
        return sum(1 for m in finished if fn(m))

    def stat(label, fn):
        n = count(fn)
        flag = "✓" if n == total else ("~" if n > total * 0.9 else "✗")
        print(f"  {flag}  {label:50s} {pct(n)}")

    print("=== Score fields ===")
    stat("score.fullTime.home",
         lambda m: m.get('score', {}).get('fullTime', {}).get('home') is not None)
    stat("score.halfTime.home",
         lambda m: m.get('score', {}).get('halfTime', {}).get('home') is not None)
    stat("score.winner (HOME_TEAM/AWAY_TEAM/DRAW)",
         lambda m: m.get('score', {}).get('winner') is not None)
    stat("score.duration (REGULAR/EXTRA_TIME/etc)",
         lambda m: m.get('score', {}).get('duration') is not None)

    print("\n=== Match metadata ===")
    stat("matchday",
         lambda m: m.get('matchday') is not None)
    stat("venue",
         lambda m: bool(m.get('venue')))
    stat("attendance",
         lambda m: m.get('attendance') is not None)
    stat("odds.homeWin",
         lambda m: m.get('odds', {}).get('homeWin') is not None)
    stat("odds.draw",
         lambda m: m.get('odds', {}).get('draw') is not None)
    stat("odds.awayWin",
         lambda m: m.get('odds', {}).get('awayWin') is not None)

    print("\n=== Team info ===")
    stat("homeTeam.formation",
         lambda m: bool(m.get('homeTeam', {}).get('formation')))
    stat("awayTeam.formation",
         lambda m: bool(m.get('awayTeam', {}).get('formation')))
    stat("homeTeam.lineup (at least 1 player)",
         lambda m: len(m.get('homeTeam', {}).get('lineup', [])) > 0)
    stat("awayTeam.lineup (at least 1 player)",
         lambda m: len(m.get('awayTeam', {}).get('lineup', [])) > 0)

    print("\n=== Team statistics ===")
    def has_stat(m, side, key):
        return m.get(side, {}).get('statistics', {}).get(key) is not None
    for key in ['ball_possession', 'shots', 'shots_on_goal', 'corner_kicks',
                'fouls', 'yellow_cards', 'red_cards', 'saves', 'offsides']:
        stat(f"homeTeam.statistics.{key}",
             lambda m, k=key: has_stat(m, 'homeTeam', k))

    print("\n=== Events (arrays) ===")
    stat("goals[] (at least 1 entry OR 0-0 game)",
         lambda m: isinstance(m.get('goals'), list))
    stat("bookings[] present",
         lambda m: isinstance(m.get('bookings'), list))
    stat("substitutions[] present",
         lambda m: isinstance(m.get('substitutions'), list))
    stat("referees[] (at least 1)",
         lambda m: len(m.get('referees', [])) > 0)

    print("\n=== Goal detail (of matches that have goals) ===")
    matches_with_goals = [m for m in finished if m.get('goals')]
    g_total = len(matches_with_goals)
    if g_total:
        def gstat(label, fn):
            n = sum(1 for m in matches_with_goals
                    for g in m.get('goals', []) if fn(g))
            g_all = sum(len(m.get('goals', [])) for m in matches_with_goals)
            flag = "✓" if n == g_all else ("~" if n > g_all * 0.9 else "✗")
            print(f"  {flag}  {label:50s} {n}/{g_all} goals ({100*n//g_all if g_all else 0}%)")
        gstat("goal.minute",       lambda g: g.get('minute') is not None)
        gstat("goal.type",         lambda g: bool(g.get('type')))
        gstat("goal.scorer.name",  lambda g: bool(g.get('scorer', {}).get('name')))
        gstat("goal.assist.name",  lambda g: bool(g.get('assist', {}).get('name')))

    print(f"\nLegend: ✓ = 100%  ~ = >90%  ✗ = <90%")


def main():
    parser = argparse.ArgumentParser(
        description='Check which API fields are populated for Serie A matches.'
    )
    parser.add_argument('api_key', help='Your football-data.org API key')
    parser.add_argument('--season', type=int, default=None,
                        help='Season start year (default: 2025 for 2025-26)')
    args = parser.parse_args()

    season = args.season or 2025
    print(f"=== API Field Coverage  {datetime.now().strftime('%Y-%m-%d %H:%M')}  season={season} ===\n")
    check_coverage(args.api_key, season)


if __name__ == '__main__':
    main()
