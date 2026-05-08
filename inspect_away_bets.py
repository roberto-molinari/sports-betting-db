"""
inspect_away_bets.py
====================
Deep-dive into every away bet the Poisson model would place (EV > threshold).
For each away bet, records:
  - season, match_date, home_team, away_team
  - model p_away, implied p_away (from odds)
  - EV, decimal odds
  - odds bracket: "away_fav" (<2.0), "slight_dog" (2.0–3.0), "big_dog" (>3.0)
  - won/lost

Then produces breakdowns by:
  1. Odds bracket
  2. EV bucket
  3. Away team (which teams are being bet on)
  4. Home team (which opponents generate value)

Run against 2024 and 2025 side-by-side to find stable sub-segments.

Usage:
    python inspect_away_bets.py --seasons 2024 2025 --ev-threshold 0.0
    python inspect_away_bets.py --seasons 2024 2025 --ev-threshold 0.10
"""

import argparse
import sqlite3
from collections import defaultdict

from poisson_model import analyse_match
from sports_db import DATABASE_PATH


# ---------------------------------------------------------------------------
# Data loading (same as backtest.py)
# ---------------------------------------------------------------------------

def load_test_matches(conn, season: int, test_fraction: float = 0.4):
    cur = conn.cursor()
    cur.execute("""
        SELECT
            m.match_id, m.match_date,
            ht.name AS home_name, at.name AS away_name,
            m.home_team_id, m.away_team_id,
            m.home_score, m.away_score,
            o.home_moneyline, o.draw_moneyline, o.away_moneyline
        FROM soccer_matches m
        JOIN soccer_teams ht ON m.home_team_id = ht.team_id
        JOIN soccer_teams at ON m.away_team_id = at.team_id
        JOIN soccer_betting_odds o ON m.match_id = o.match_id
        WHERE m.league = 'Serie A'
          AND m.season = ?
          AND m.home_score IS NOT NULL
          AND o.home_moneyline IS NOT NULL
          AND o.draw_moneyline  IS NOT NULL
          AND o.away_moneyline  IS NOT NULL
        ORDER BY m.match_date ASC
    """, (season,))
    rows = cur.fetchall()
    keys = ["match_id", "match_date", "home_name", "away_name",
            "home_team_id", "away_team_id", "home_score", "away_score",
            "home_moneyline", "draw_moneyline", "away_moneyline"]
    matches = [dict(zip(keys, r)) for r in rows]
    split = int(len(matches) * (1 - test_fraction))
    return matches[:split], matches[split:]


def odds_bracket(decimal_odds: float) -> str:
    if decimal_odds < 2.0:
        return "away_fav  (<2.0)"
    elif decimal_odds < 3.0:
        return "slight_dog (2-3)"
    elif decimal_odds < 5.0:
        return "medium_dog (3-5)"
    else:
        return "big_dog    (5+) "


def ev_bucket(ev: float) -> str:
    if ev < 0.05:
        return "EV 0-5%"
    elif ev < 0.10:
        return "EV 5-10%"
    elif ev < 0.20:
        return "EV 10-20%"
    else:
        return "EV 20%+"


def p_model_bucket(p: float) -> str:
    """Bucket by model's estimated probability of away win."""
    if p < 0.20:
        return "p <20%  (strong underdog)"
    elif p < 0.35:
        return "p 20-35% (underdog)"
    elif p < 0.50:
        return "p 35-50% (slight dog)"
    else:
        return "p 50%+  (away favoured)"


# ---------------------------------------------------------------------------
# Core collector
# ---------------------------------------------------------------------------

def collect_away_bets(conn, season: int, ev_threshold: float, test_fraction: float):
    """Return list of dicts describing every away value bet in test window."""
    _, test_matches = load_test_matches(conn, season, test_fraction)
    records = []

    for m in test_matches:
        try:
            result = analyse_match(
                home_team_id=m["home_team_id"],
                away_team_id=m["away_team_id"],
                match_date=m["match_date"],
                home_moneyline=m["home_moneyline"],
                draw_moneyline=m["draw_moneyline"],
                away_moneyline=m["away_moneyline"],
                conn=conn,
            )
        except Exception:
            continue

        ev_away = result.get("ev_away", -999)
        if ev_away <= ev_threshold:
            continue

        actual_away_win = m["away_score"] > m["home_score"]
        dec = result["decimal_away"]
        profit = (dec - 1) if actual_away_win else -1.0

        records.append({
            "season":      season,
            "match_date":  m["match_date"],
            "home_team":   m["home_name"],
            "away_team":   m["away_name"],
            "score":       f"{m['home_score']}-{m['away_score']}",
            "p_model":     result["p_away"],
            "p_implied":   result["implied_away"],
            "ev":          ev_away,
            "decimal":     dec,
            "won":         actual_away_win,
            "profit":      profit,
            "odds_bracket": odds_bracket(dec),
            "ev_bucket":   ev_bucket(ev_away),
            "p_bucket":    p_model_bucket(result["p_away"]),
        })

    return records


# ---------------------------------------------------------------------------
# Breakdown printer
# ---------------------------------------------------------------------------

def breakdown(records: list[dict], group_key: str, label: str):
    groups = defaultdict(lambda: {"bets": 0, "wins": 0, "profit": 0.0})
    for r in records:
        g = r[group_key]
        groups[g]["bets"]   += 1
        groups[g]["wins"]   += 1 if r["won"] else 0
        groups[g]["profit"] += r["profit"]

    if not groups:
        print(f"\n  (no bets)\n")
        return

    print(f"\n--- By {label} ---")
    print(f"  {'Group':<28}  {'Bets':>5}  {'Wins':>5}  {'WinRate':>8}  {'Profit':>9}  {'ROI':>8}")
    print("  " + "-" * 72)
    for g in sorted(groups):
        s = groups[g]
        n = s["bets"]
        wr = s["wins"] / n if n else 0
        roi = s["profit"] / n if n else 0
        print(f"  {g:<28}  {n:>5}  {s['wins']:>5}  {wr:>8.1%}  ${s['profit']:>+8.2f}  {roi:>+7.1%}")


def team_breakdown(records: list[dict], team_key: str, label: str, min_bets: int = 3):
    groups = defaultdict(lambda: {"bets": 0, "wins": 0, "profit": 0.0})
    for r in records:
        g = r[team_key]
        groups[g]["bets"]   += 1
        groups[g]["wins"]   += 1 if r["won"] else 0
        groups[g]["profit"] += r["profit"]

    # Filter to teams with enough bets
    filtered = {k: v for k, v in groups.items() if v["bets"] >= min_bets}
    if not filtered:
        print(f"\n  (no team with >= {min_bets} bets)\n")
        return

    print(f"\n--- By {label} (min {min_bets} bets) ---")
    print(f"  {'Team':<28}  {'Bets':>5}  {'Wins':>5}  {'WinRate':>8}  {'Profit':>9}  {'ROI':>8}")
    print("  " + "-" * 72)
    # Sort by ROI descending
    for g in sorted(filtered, key=lambda k: filtered[k]["profit"] / filtered[k]["bets"], reverse=True):
        s = filtered[g]
        n = s["bets"]
        wr = s["wins"] / n
        roi = s["profit"] / n
        print(f"  {g:<28}  {n:>5}  {s['wins']:>5}  {wr:>8.1%}  ${s['profit']:>+8.2f}  {roi:>+7.1%}")


def cross_season_breakdown(records_by_season: dict, group_key: str, label: str, min_bets: int = 3):
    """Show a group side-by-side for multiple seasons; highlight groups present in all seasons."""
    seasons = sorted(records_by_season.keys())
    # Aggregate per (season, group)
    agg = {}  # (season, group) → {bets, wins, profit}
    all_groups = set()
    for season, records in records_by_season.items():
        for r in records:
            g = r[group_key]
            all_groups.add(g)
            key = (season, g)
            if key not in agg:
                agg[key] = {"bets": 0, "wins": 0, "profit": 0.0}
            agg[key]["bets"]   += 1
            agg[key]["wins"]   += 1 if r["won"] else 0
            agg[key]["profit"] += r["profit"]

    # Only show groups that appear in ALL seasons with min_bets
    stable = []
    for g in sorted(all_groups):
        ok = all(agg.get((s, g), {}).get("bets", 0) >= min_bets for s in seasons)
        if ok:
            stable.append(g)

    print(f"\n=== Cross-season: {label} (min {min_bets} bets per season, in all seasons) ===")
    if not stable:
        print(f"  (no group meets criteria)")
        # fall back: show all with >= min_bets in at least one season
        seen_any = {g for g in all_groups
                    if any(agg.get((s, g), {}).get("bets", 0) >= min_bets for s in seasons)}
        stable = sorted(seen_any)
        print(f"  Showing all groups with >= {min_bets} bets in at least one season:")

    header = f"  {'Group':<28}"
    for s in seasons:
        header += f"  {str(s)+' ROI':>10}  {str(s)+' bets':>8}"
    print(header)
    print("  " + "-" * (28 + len(seasons) * 22 + 4))

    for g in stable:
        row = f"  {g:<28}"
        for s in seasons:
            key = (s, g)
            if key in agg:
                n = agg[key]["bets"]
                roi = agg[key]["profit"] / n
                row += f"  {roi:>+9.1%}  {n:>8}"
            else:
                row += f"  {'--':>10}  {'--':>8}"
        print(row)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seasons", type=int, nargs="+", default=[2024, 2025])
    parser.add_argument("--ev-threshold", type=float, default=0.0)
    parser.add_argument("--test-fraction", type=float, default=0.4)
    parser.add_argument("--min-bets", type=int, default=3,
                        help="Minimum bets per group for cross-season comparison")
    args = parser.parse_args()

    conn = sqlite3.connect(DATABASE_PATH)
    records_by_season = {}

    for season in args.seasons:
        records = collect_away_bets(conn, season, args.ev_threshold, args.test_fraction)
        records_by_season[season] = records
        n = len(records)
        wins = sum(1 for r in records if r["won"])
        profit = sum(r["profit"] for r in records)
        roi = profit / n if n else 0

        print(f"\n{'='*70}")
        print(f"  Season {season}  |  Away bets (EV > {args.ev_threshold:+.1%})  |  "
              f"N={n}  wins={wins}  ROI={roi:+.1%}")
        print(f"{'='*70}")

        breakdown(records, "odds_bracket", "Odds Bracket")
        breakdown(records, "ev_bucket",    "EV Bucket")
        breakdown(records, "p_bucket",     "Model P(away win) Bucket")
        team_breakdown(records, "away_team", "Away Team (who we bet on)", min_bets=2)
        team_breakdown(records, "home_team", "Home Team (who we bet against)", min_bets=2)

    conn.close()

    # Cross-season comparisons
    print(f"\n\n{'#'*70}")
    print("  CROSS-SEASON STABILITY")
    print(f"{'#'*70}")
    cross_season_breakdown(records_by_season, "odds_bracket", "Odds Bracket",    min_bets=3)
    cross_season_breakdown(records_by_season, "ev_bucket",    "EV Bucket",       min_bets=3)
    cross_season_breakdown(records_by_season, "p_bucket",     "Model P Bucket",  min_bets=3)

    # Intersection: p_bucket x odds_bracket for each season
    print(f"\n\n{'#'*70}")
    print("  INTERSECTION: Model P Bucket × Odds Bracket")
    print(f"{'#'*70}")
    for season, records in sorted(records_by_season.items()):
        print(f"\n  --- Season {season} ---")
        groups = defaultdict(lambda: {"bets": 0, "wins": 0, "profit": 0.0})
        for r in records:
            g = f"{r['p_bucket']}  |  {r['odds_bracket']}"
            groups[g]["bets"]   += 1
            groups[g]["wins"]   += 1 if r["won"] else 0
            groups[g]["profit"] += r["profit"]
        print(f"  {'Segment':<55}  {'Bets':>5}  {'WinRate':>8}  {'ROI':>8}")
        print("  " + "-" * 82)
        for g in sorted(groups, key=lambda k: groups[k]["profit"] / groups[k]["bets"], reverse=True):
            s = groups[g]
            n = s["bets"]
            if n < 2:
                continue
            wr = s["wins"] / n
            roi = s["profit"] / n
            print(f"  {g:<55}  {n:>5}  {wr:>8.1%}  {roi:>+7.1%}")


if __name__ == "__main__":
    main()
