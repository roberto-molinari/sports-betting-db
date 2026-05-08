"""
test_h004_elite_home_moderate_fav.py
=====================================
Tests Hypothesis 004 / 005:

  H004: top-6 finishers as home moderate favourites (-110 to -180)
  H005: mid-tier elite only (gap-split removes the dominant clubs)

Gap-based tier splitting
------------------------
  Within the prior season's top-N finishers, find the LARGEST points gap
  between consecutive positions.  If that gap >= --gap-threshold (default 5),
  teams above the gap are labelled "dominant" and excluded from the mid-tier
  elite segment.  Teams at or below the gap (still in top-N) are "mid-tier
  elite" — the H005 segment.

Usage
-----
    python test_h004_elite_home_moderate_fav.py
    python test_h004_elite_home_moderate_fav.py --gap-threshold 8 --top-n 6
    python test_h004_elite_home_moderate_fav.py --home-min -180 --home-max -110
"""

import argparse
import sqlite3
from collections import defaultdict

from sports_db import DATABASE_PATH


def american_to_decimal(ml: float) -> float:
    if ml >= 0:
        return 1.0 + ml / 100.0
    return 1.0 + 100.0 / abs(ml)


def get_standings(conn, season: int, n: int = 6) -> list:
    """Return top-n teams as [{name, pts, rank}] sorted by points desc."""
    cur = conn.cursor()
    cur.execute("""
        SELECT t.name,
               SUM(CASE
                   WHEN m.home_team_id = t.team_id AND m.home_score > m.away_score THEN 3
                   WHEN m.away_team_id = t.team_id AND m.away_score > m.home_score THEN 3
                   WHEN m.home_score = m.away_score THEN 1
                   ELSE 0 END) AS pts
        FROM soccer_teams t
        JOIN soccer_matches m ON (m.home_team_id = t.team_id OR m.away_team_id = t.team_id)
        WHERE m.league = 'Serie A'
          AND m.season = ?
          AND m.home_score IS NOT NULL
        GROUP BY t.team_id
        ORDER BY pts DESC
        LIMIT ?
    """, (season, n))
    return [
        {"name": row[0], "pts": row[1], "rank": idx + 1}
        for idx, row in enumerate(cur.fetchall())
    ]


def find_tier_break(standings: list, gap_threshold: int) -> tuple:
    """
    Within the top-N standings list, find the position of the LARGEST
    points gap between consecutive entries.  If that gap >= gap_threshold,
    split there: teams above = dominant, teams at/below = mid_tier.

    Returns (dominant_set, mid_tier_set, break_info_str).
    If no gap meets the threshold, all teams are mid_tier.
    """
    if len(standings) < 2:
        return set(), {s["name"] for s in standings}, "no split (fewer than 2 teams)"

    gaps = [
        (standings[i]["pts"] - standings[i + 1]["pts"], i)
        for i in range(len(standings) - 1)
    ]
    max_gap, split_idx = max(gaps, key=lambda x: x[0])

    if max_gap < gap_threshold:
        names = {s["name"] for s in standings}
        info = (f"no clear tier break (largest gap {max_gap} pts between "
                f"rank {split_idx+1} and rank {split_idx+2} < threshold {gap_threshold}) "
                f"— all {len(standings)} teams treated as mid-tier elite")
        return set(), names, info

    dominant  = {s["name"] for s in standings[: split_idx + 1]}
    mid_tier  = {s["name"] for s in standings[split_idx + 1 :]}
    above_team = standings[split_idx]
    below_team = standings[split_idx + 1]
    info = (f"split after rank {split_idx+1} ({above_team['name']} {above_team['pts']}pts) "
            f"— gap {max_gap}pts → rank {split_idx+2} ({below_team['name']} {below_team['pts']}pts)")
    return dominant, mid_tier, info


def load_matches(conn, seasons: list, home_min: float, home_max: float) -> list:
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
          AND o.home_moneyline >= ?
          AND o.home_moneyline <= ?
        ORDER BY m.season, m.match_date
    """, (*seasons, home_min, home_max))
    keys = ["season", "match_date", "home_name", "away_name",
            "home_score", "away_score", "home_moneyline", "away_moneyline"]
    return [dict(zip(keys, r)) for r in cur.fetchall()]


def odds_band(ml: float) -> str:
    if ml >= -120:
        return "-110 to -120"
    elif ml >= -140:
        return "-121 to -140"
    elif ml >= -160:
        return "-141 to -160"
    else:
        return "-161 to -180"


def summarise(records: list) -> dict:
    n = len(records)
    wins = sum(1 for r in records if r["won"])
    profit = sum(r["profit"] for r in records)
    return {
        "bets": n,
        "wins": wins,
        "win_rate": wins / n if n else 0,
        "profit": profit,
        "roi": profit / n if n else 0,
    }


def print_table(rows, headers):
    if not rows:
        print("  (no data)")
        return
    col_w = [max(len(h), max(len(str(r[i])) for r in rows))
             for i, h in enumerate(headers)]
    fmt = "  " + "  ".join(f"{{:<{w}}}" for w in col_w)
    print(fmt.format(*headers))
    print("  " + "-" * (sum(col_w) + 2 * len(col_w)))
    for row in rows:
        print(fmt.format(*row))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seasons", type=int, nargs="+", default=[2023, 2024, 2025],
                        help="Seasons to test (need prior season in DB for elite tagging; default: 2023-2025)")
    parser.add_argument("--home-min", type=float, default=-180,
                        help="Minimum home moneyline (most negative / longest odds, default -180)")
    parser.add_argument("--home-max", type=float, default=-110,
                        help="Maximum home moneyline (least negative, default -110)")
    parser.add_argument("--top-n", type=int, default=6,
                        help="How many prior-season finishers count as elite pool (default 6)")
    parser.add_argument("--gap-threshold", type=int, default=5,
                        help="Minimum points gap to designate a 'dominant' sub-tier (default 5)")
    args = parser.parse_args()

    conn = sqlite3.connect(DATABASE_PATH)

    # Build prior-season tier sets
    dominant_by_season  = {}   # "elite of the elite" — excluded from H005
    midtier_by_season   = {}   # H005 target segment
    elite_by_season     = {}   # combined (H004 compatibility — dominant ∪ mid-tier)

    for season in args.seasons:
        prior = season - 1
        standings = get_standings(conn, prior, args.top_n)
        dominant, mid_tier, info = find_tier_break(standings, args.gap_threshold)
        dominant_by_season[season]  = dominant
        midtier_by_season[season]   = mid_tier
        elite_by_season[season]     = dominant | mid_tier
        print(f"Season {season} (prior={prior}):")
        for s in standings:
            tier = "DOMINANT " if s["name"] in dominant else "mid-tier "
            print(f"  rank {s['rank']:2d}  {tier}  {s['pts']:3d}pts  {s['name']}")
        print(f"  → {info}")

    raw = load_matches(conn, args.seasons, args.home_min, args.home_max)
    conn.close()

    # Annotate
    records = []
    for r in raw:
        home_win = r["home_score"] > r["away_score"]
        dec = american_to_decimal(r["home_moneyline"])
        profit = (dec - 1.0) if home_win else -1.0
        is_dominant = r["home_name"] in dominant_by_season.get(r["season"], set())
        is_midtier  = r["home_name"] in midtier_by_season.get(r["season"], set())
        is_elite    = is_dominant or is_midtier
        records.append({**r, "won": home_win, "profit": profit, "dec_home": dec,
                        "is_elite": is_elite, "is_dominant": is_dominant, "is_midtier": is_midtier})

    dominant_records = [r for r in records if r["is_dominant"]]
    midtier_records  = [r for r in records if r["is_midtier"]]
    elite_records    = [r for r in records if r["is_elite"]]
    nonelite_records = [r for r in records if not r["is_elite"]]

    print(f"\nH004/H005 — Bet home ({args.home_min} to {args.home_max}) "
          f"| top-{args.top_n}, gap-threshold={args.gap_threshold}pts")
    print(f"Seasons tested: {args.seasons}")
    print("=" * 65)

    # ── Three-way overview ──────────────────────────────────────────
    print("\n--- Overview: Dominant / Mid-Tier Elite / Non-Elite (all seasons) ---")
    ov_rows = []
    for label, grp in [
        ("Dominant (excl.)",  dominant_records),
        ("Mid-tier elite",    midtier_records),
        ("Non-elite",         nonelite_records),
        ("ALL elite",         elite_records),
        ("ALL",               records),
    ]:
        s = summarise(grp)
        ov_rows.append((label, str(s["bets"]), str(s["wins"]),
                        f"{s['win_rate']:.1%}", f"${s['profit']:+.2f}", f"{s['roi']:+.1%}"))
    print_table(ov_rows, ["Group", "Bets", "Wins", "Win rate", "Profit", "ROI"])

    # ── Mid-tier elite by season  (H005 target) ─────────────────────
    print("\n--- Mid-Tier Elite Home (H005): By Season ---")
    by_season = defaultdict(list)
    for r in midtier_records:
        by_season[r["season"]].append(r)
    season_rows = []
    for season in sorted(by_season):
        s = summarise(by_season[season])
        season_rows.append((str(season), str(s["bets"]), str(s["wins"]),
                            f"{s['win_rate']:.1%}", f"${s['profit']:+.2f}", f"{s['roi']:+.1%}"))
    if not season_rows:
        print("  (no mid-tier elite matches in range)")
    else:
        s = summarise(midtier_records)
        season_rows.append(("TOTAL", str(s["bets"]), str(s["wins"]),
                            f"{s['win_rate']:.1%}", f"${s['profit']:+.2f}", f"{s['roi']:+.1%}"))
        print_table(season_rows, ["Season", "Bets", "Wins", "Win rate", "Profit", "ROI"])

    # ── Dominant elite by season (for reference) ────────────────────
    print("\n--- Dominant Elite Home (excluded from H005): By Season ---")
    by_season_dom = defaultdict(list)
    for r in dominant_records:
        by_season_dom[r["season"]].append(r)
    dom_rows = []
    for season in sorted(by_season_dom):
        s = summarise(by_season_dom[season])
        dom_rows.append((str(season), str(s["bets"]), str(s["wins"]),
                         f"{s['win_rate']:.1%}", f"${s['profit']:+.2f}", f"{s['roi']:+.1%}"))
    if not dom_rows:
        print("  (no dominant matches in range, or no gap found)")
    else:
        s = summarise(dominant_records)
        dom_rows.append(("TOTAL", str(s["bets"]), str(s["wins"]),
                         f"{s['win_rate']:.1%}", f"${s['profit']:+.2f}", f"{s['roi']:+.1%}"))
        print_table(dom_rows, ["Season", "Bets", "Wins", "Win rate", "Profit", "ROI"])

    # ── Mid-tier home by odds band ──────────────────────────────────
    print("\n--- Mid-Tier Elite Home: By Odds Band (all seasons) ---")
    by_band = defaultdict(list)
    for r in midtier_records:
        by_band[odds_band(r["home_moneyline"])].append(r)
    band_rows = []
    for band in sorted(by_band):
        s = summarise(by_band[band])
        band_rows.append((band, str(s["bets"]), str(s["wins"]),
                          f"{s['win_rate']:.1%}", f"${s['profit']:+.2f}", f"{s['roi']:+.1%}"))
    print_table(band_rows, ["Odds band", "Bets", "Wins", "Win rate", "Profit", "ROI"])

    # ── Mid-tier team breakdown ─────────────────────────────────────
    print("\n--- Mid-Tier Elite Home: By Team (all seasons, min 3 bets) ---")
    by_team = defaultdict(list)
    for r in midtier_records:
        by_team[r["home_name"]].append(r)
    team_rows = []
    for team in sorted(by_team, key=lambda t: summarise(by_team[t])["roi"], reverse=True):
        s = summarise(by_team[team])
        if s["bets"] < 3:
            continue
        team_rows.append((team, str(s["bets"]), str(s["wins"]),
                          f"{s['win_rate']:.1%}", f"${s['profit']:+.2f}", f"{s['roi']:+.1%}"))
    print_table(team_rows, ["Team", "Bets", "Wins", "Win rate", "Profit", "ROI"])

    # ── All-elite team breakdown for reference ──────────────────────
    print("\n--- ALL Elite Home: By Team (all seasons, min 3 bets) ---")
    by_team_all = defaultdict(list)
    for r in elite_records:
        by_team_all[r["home_name"]].append(r)
    all_team_rows = []
    for team in sorted(by_team_all, key=lambda t: summarise(by_team_all[t])["roi"], reverse=True):
        s = summarise(by_team_all[team])
        if s["bets"] < 3:
            continue
        tier = "DOM" if team in dominant_by_season.get(args.seasons[-1], set()) \
               else "mid"
        all_team_rows.append((team, tier, str(s["bets"]), str(s["wins"]),
                              f"{s['win_rate']:.1%}", f"${s['profit']:+.2f}", f"{s['roi']:+.1%}"))
    print_table(all_team_rows, ["Team", "Tier", "Bets", "Wins", "Win rate", "Profit", "ROI"])

    print()


if __name__ == "__main__":
    main()
