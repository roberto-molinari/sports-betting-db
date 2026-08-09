"""
Comparison/sweep tool -- backfill soccer_model_predictions at an arbitrary xG-stretch
factor, to test values other than the shipped default. See BUGS.md, BUG-009,
2026-08-07 addendum for the investigation this supported.

2026-08-07: factor=1.3 (this script's own finding) is now the REAL shipped default
(TEAM_RATING_XG_SPREAD_STRETCH, compute_club_player_strength.py) -- for that value,
prefer backfill_player_blend_predictions.py (the production backfill script, which
now applies it automatically). This script still monkeypatches get_team_xg_ratings
for the duration of the run (compute_club_player_strength.py itself is untouched by
running it) -- kept around specifically for sweeping OTHER factor values (or
--team-xg-weight combinations) that the production script doesn't expose as a flag.
Mirrors backfill_player_blend_predictions.py's loop closely (point-in-time correct,
one compute() call per unique match_date).

Stretch: for each of get_team_xg_ratings' four fields (home_attack/away_attack/
home_defense/away_defense), recenter around that field's own league-wide mean (across
all teams active in the league that season) at the same before_date, then multiply
the deviation from that mean by `factor`:
    stretched = league_mean + (raw - league_mean) * factor
factor=1.0 is an exact no-op.

Usage:
    python3 backfill_with_xg_stretch.py --league "Serie A" --season 2024 --season 2025 \\
        --xg-stretch 1.3 --team-xg-weight 1.0 --method poisson_v4_stretch130
"""
import argparse
import sqlite3
from datetime import datetime, timezone
from functools import lru_cache
from itertools import groupby

from core.sports_db import (
    DATABASE_PATH,
    clear_soccer_model_predictions,
    add_soccer_model_prediction,
)
from core.poisson_model import analyse_match_wc
import compute_club_player_strength as strength

LEAGUE_DEFAULT = "Serie A"
_ORIG_GET_TEAM_XG_RATINGS = strength.get_team_xg_ratings
_FIELDS = ("home_attack", "away_attack", "home_defense", "away_defense")

# Local speedup only (BUG-009's real fix is separate, BUG-011 -- logged, unfixed in
# the real code); memoized here so a two-season, multi-config sweep finishes in
# minutes, not hours. No effect on results, just avoids redundant identical SQL.
strength.player_season_minutes = lru_cache(maxsize=None)(strength.player_season_minutes)
strength.team_roster_minutes = lru_cache(maxsize=None)(strength.team_roster_minutes)


def make_stretched(factor):
    """Drop-in replacement for get_team_xg_ratings, stretching each team's raw rating
    around the league-wide mean for that (league, before_date). Caches per (league,
    before_date, n) to avoid recomputing the whole league's raw ratings once per team."""
    if factor == 1.0:
        return _ORIG_GET_TEAM_XG_RATINGS

    league_cache = {}
    mean_cache = {}

    def get_snapshot(conn, league, before_date, n, team_ids):
        key = (league, before_date, n)
        if key not in league_cache:
            raw = {tid: _ORIG_GET_TEAM_XG_RATINGS(conn, tid, before_date, n=n, league=league)
                   for tid in team_ids}
            league_cache[key] = raw
            means = {}
            for field in _FIELDS:
                vals = [r[field] for r in raw.values() if r[field] is not None]
                means[field] = sum(vals) / len(vals) if vals else None
            mean_cache[key] = means
        return league_cache[key], mean_cache[key]

    def stretched(conn, team_id, before_date, n=10, league="Serie A"):
        team_ids = _CURRENT_TEAM_IDS[0]
        raw_by_team, means = get_snapshot(conn, league, before_date, n, team_ids)
        raw = raw_by_team.get(team_id) or _ORIG_GET_TEAM_XG_RATINGS(conn, team_id, before_date, n=n, league=league)
        out = dict(raw)
        for field in _FIELDS:
            v, m = raw[field], means.get(field)
            if v is not None and m is not None:
                out[field] = m + (v - m) * factor
        return out

    return stretched


_CURRENT_TEAM_IDS = [None]  # set per season in main(); read by the stretched closure


def load_all_team_ids(conn, league, season):
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT t.team_id FROM soccer_teams t
        JOIN soccer_matches m ON m.home_team_id = t.team_id OR m.away_team_id = t.team_id
        WHERE m.league = ? AND m.season = ?
    """, (league, season))
    return [r[0] for r in cur.fetchall()]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--league", default=LEAGUE_DEFAULT)
    parser.add_argument("--season", type=int, action="append", dest="seasons", required=True)
    parser.add_argument("--method", required=True)
    parser.add_argument("--xg-stretch", type=float, default=1.0,
                        help="Team-level xG rating spread multiplier (1.0=no-op/today's shipped shape).")
    parser.add_argument("--team-xg-weight", dest="team_xg_v_goals_blend", type=float, default=1.0,
                        help="Passed through to compute() unchanged -- 1.0=pure xG (default), 0.0=pure goals.")
    args = parser.parse_args()

    strength.get_team_xg_ratings = make_stretched(args.xg_stretch)

    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    generated_at = datetime.now(timezone.utc).isoformat()
    total_inserted = 0

    for season in args.seasons:
        clear_soccer_model_predictions(args.league, season, args.method, conn=conn)
        team_ids = load_all_team_ids(conn, args.league, season)
        _CURRENT_TEAM_IDS[0] = team_ids

        cur = conn.cursor()
        cur.execute("""
            SELECT sm.match_id, sm.home_team_id, sm.away_team_id, sm.match_date,
                   o.home_moneyline, o.draw_moneyline, o.away_moneyline,
                   o.over_under, o.over_odds, o.under_odds
            FROM soccer_matches sm
            JOIN soccer_betting_odds o ON o.match_id = sm.match_id
            WHERE sm.league = ? AND sm.season = ?
            ORDER BY sm.match_date
        """, (args.league, season))
        rows = cur.fetchall()
        if not rows:
            print(f"No matches with odds found for {args.league} season {season}")
            continue

        inserted = 0
        cache = {}  # BUG-011: memoizes last-season aggregates across this season's matchdays
        for match_date, date_rows in groupby(rows, key=lambda r: r["match_date"]):
            date_rows = list(date_rows)
            squad_ids_by_team = {tid: strength.squad_as_of_date(conn, tid, season, match_date)
                                 for tid in team_ids}
            results = strength.compute(conn, team_ids, args.league, season, match_date,
                                       team_xg_v_goals_blend=args.team_xg_v_goals_blend,
                                       current_squad_ids_by_team=squad_ids_by_team, cache=cache)

            for row in date_rows:
                home = results[row["home_team_id"]]
                away = results[row["away_team_id"]]
                avg_home, avg_away = home["avg_home"], home["avg_away"]
                result = analyse_match_wc(
                    lambda_home_attack=home["lambda_attack_home_blend"],
                    lambda_away_attack=away["lambda_attack_away_blend"],
                    lambda_home_defense=home["lambda_defense_home_blend"],
                    lambda_away_defense=away["lambda_defense_away_blend"],
                    home_moneyline=row["home_moneyline"],
                    draw_moneyline=row["draw_moneyline"],
                    away_moneyline=row["away_moneyline"],
                    ou_line=row["over_under"],
                    over_odds=row["over_odds"],
                    under_odds=row["under_odds"],
                    baseline=avg_home,
                    home_advantage=1.0,
                    away_advantage=avg_home / avg_away,
                )
                add_soccer_model_prediction(
                    match_id=row["match_id"], league=args.league, match_date=row["match_date"],
                    generated_at=generated_at, method=args.method,
                    lambda_home=result["lambda_H"], lambda_away=result["lambda_A"],
                    p_home=result["p_home"], p_draw=result["p_draw"], p_away=result["p_away"],
                    over_under_line=row["over_under"], p_over=result.get("p_over"), p_under=result.get("p_under"),
                    home_moneyline=row["home_moneyline"], draw_moneyline=row["draw_moneyline"],
                    away_moneyline=row["away_moneyline"], over_odds=row["over_odds"], under_odds=row["under_odds"],
                    ev_home=result.get("ev_home"), ev_draw=result.get("ev_draw"), ev_away=result.get("ev_away"),
                    ev_over=result.get("ev_over"), ev_under=result.get("ev_under"),
                    conn=conn,
                )
                inserted += 1
        total_inserted += inserted
        print(f"season {season}: inserted {inserted} rows (method={args.method}, "
              f"xg_stretch={args.xg_stretch}, team_xg_v_goals_blend={args.team_xg_v_goals_blend})")

    conn.close()
    print(f"Total inserted: {total_inserted}")


if __name__ == "__main__":
    main()
