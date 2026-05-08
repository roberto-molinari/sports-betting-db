"""
test_h002_home_vs_medium_dog.py
================================
Tests Hypothesis 002:

  "When the away team's moneyline is between +200 and +400,
   the home team has positive EV and betting the home moneyline
   produces positive ROI — regardless of what the Poisson model thinks."

No model involved. Pure market-data test.

Methodology
-----------
  - For every completed Serie A match where the away moneyline is in [+200, +400]
  - Bet $1 on the home team at the listed home moneyline
  - Report bets, wins, ROI per season and overall
  - Also show a breakdown by away-odds sub-band to check if the edge
    concentrates in a narrower range

Usage
-----
    python test_h002_home_vs_medium_dog.py
    python test_h002_home_vs_medium_dog.py --seasons 2022 2023 2024 2025
    python test_h002_home_vs_medium_dog.py --away-min 200 --away-max 300
"""

import argparse
import sqlite3
from collections import defaultdict

from sports_db import DATABASE_PATH


def american_to_decimal(ml: float) -> float:
    if ml >= 0:
        return 1.0 + ml / 100.0
    return 1.0 + 100.0 / abs(ml)


def load_matches(conn, seasons: list[int], away_min: float, away_max: float) -> list[dict]:
    placeholders = ",".join("?" * len(seasons))
    cur = conn.cursor()
    cur.execute(f"""
        SELECT
            m.season,
            m.match_date,
            ht.name  AS home_name,
            at.name  AS away_name,
            m.home_score,
            m.away_score,
            o.home_moneyline,
            o.away_moneyline
        FROM soccer_matches m
        JOIN soccer_teams ht ON m.home_team_id = ht.team_id
        JOIN soccer_teams at ON m.away_team_id = at.team_id
        JOIN soccer_betting_odds o ON m.match_id = o.match_id
        WHERE m.league = 'Serie A'
          AND m.season IN ({placeholders})
          AND m.home_score IS NOT NULL
          AND o.home_moneyline IS NOT NULL
          AND o.away_moneyline  IS NOT NULL
          AND o.away_moneyline >= ?
          AND o.away_moneyline <= ?
        ORDER BY m.season, m.match_date
    """, (*seasons, away_min, away_max))

    keys = ["season", "match_date", "home_name", "away_name",
            "home_score", "away_score", "home_moneyline", "away_moneyline"]
    return [dict(zip(keys, r)) for r in cur.fetchall()]


def away_band(ml: float) -> str:
    if ml <= 250:
        return "+200 to +250"
    elif ml <= 300:
        return "+251 to +300"
    elif ml <= 350:
        return "+301 to +350"
    else:
        return "+351 to +400"


def summarise(records: list[dict]) -> dict:
    bets = len(records)
    wins = sum(1 for r in records if r["won"])
    profit = sum(r["profit"] for r in records)
    roi = profit / bets if bets else 0.0
    win_rate = wins / bets if bets else 0.0
    return {"bets": bets, "wins": wins, "profit": profit, "roi": roi, "win_rate": win_rate}


def print_table(rows: list[tuple], headers: list[str]):
    col_w = [max(len(h), max((len(str(r[i])) for r in rows), default=0))
             for i, h in enumerate(headers)]
    fmt = "  " + "  ".join(f"{{:<{w}}}" for w in col_w)
    print(fmt.format(*headers))
    print("  " + "-" * (sum(col_w) + 2 * len(col_w)))
    for row in rows:
        print(fmt.format(*row))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seasons", type=int, nargs="+", default=[2022, 2023, 2024, 2025])
    parser.add_argument("--away-min", type=float, default=200,
                        help="Minimum away moneyline (inclusive, default +200)")
    parser.add_argument("--away-max", type=float, default=400,
                        help="Maximum away moneyline (inclusive, default +400)")
    args = parser.parse_args()

    conn = sqlite3.connect(DATABASE_PATH)
    raw = load_matches(conn, args.seasons, args.away_min, args.away_max)
    conn.close()

    # Annotate each record
    records = []
    for r in raw:
        home_win = r["home_score"] > r["away_score"]
        dec_home = american_to_decimal(r["home_moneyline"])
        profit = (dec_home - 1.0) if home_win else -1.0
        records.append({**r, "won": home_win, "profit": profit, "dec_home": dec_home})

    print(f"\nHypothesis 002 — Bet home when away team is +{int(args.away_min)} to +{int(args.away_max)}")
    print(f"Seasons: {args.seasons}  |  Total qualifying matches: {len(records)}")
    print("=" * 65)

    # ── By season ──────────────────────────────────────────────────
    print("\n--- By Season ---")
    season_rows = []
    by_season = defaultdict(list)
    for r in records:
        by_season[r["season"]].append(r)

    for season in sorted(by_season):
        s = summarise(by_season[season])
        season_rows.append((
            str(season),
            str(s["bets"]),
            str(s["wins"]),
            f"{s['win_rate']:.1%}",
            f"${s['profit']:+.2f}",
            f"{s['roi']:+.1%}",
        ))
    season_rows.append((
        "TOTAL",
        str(len(records)),
        str(sum(1 for r in records if r["won"])),
        f"{sum(r['won'] for r in records)/len(records):.1%}" if records else "—",
        f"${sum(r['profit'] for r in records):+.2f}",
        f"{sum(r['profit'] for r in records)/len(records):+.1%}" if records else "—",
    ))
    print_table(season_rows, ["Season", "Bets", "Wins", "Win rate", "Profit", "ROI"])

    # ── By away-odds sub-band ───────────────────────────────────────
    print("\n--- By Away Odds Sub-band (all seasons combined) ---")
    by_band = defaultdict(list)
    for r in records:
        by_band[away_band(r["away_moneyline"])].append(r)

    band_rows = []
    for band in sorted(by_band):
        s = summarise(by_band[band])
        band_rows.append((
            band,
            str(s["bets"]),
            str(s["wins"]),
            f"{s['win_rate']:.1%}",
            f"${s['profit']:+.2f}",
            f"{s['roi']:+.1%}",
        ))
    print_table(band_rows, ["Away odds band", "Bets", "Wins", "Win rate", "Profit", "ROI"])

    # ── Sub-band × season ──────────────────────────────────────────
    print("\n--- Sub-band × Season ---")
    cross = defaultdict(list)
    for r in records:
        cross[(away_band(r["away_moneyline"]), r["season"])].append(r)

    seasons_sorted = sorted(args.seasons)
    bands_sorted = sorted({away_band(r["away_moneyline"]) for r in records})

    header = ["Away band"] + [f"{s} ROI ({s} n)" for s in seasons_sorted]
    cross_rows = []
    for band in bands_sorted:
        row = [band]
        for season in seasons_sorted:
            grp = cross[(band, season)]
            if grp:
                s = summarise(grp)
                row.append(f"{s['roi']:+.1%} ({s['bets']})")
            else:
                row.append("—")
        cross_rows.append(row)
    print_table(cross_rows, header)

    print()


if __name__ == "__main__":
    main()
