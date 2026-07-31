"""
Backfill soccer_model_predictions for a completed season using FEATURE-011's
player-level blend (method='poisson_v4'), for Success Criteria validation (signed
bias vs. sharp closing lines, full-season ROI) against the existing team-level
poisson_v3 -- see FEATURE-011_REQUIREMENTS.md (Success Criteria) and
FEATURE-011_BUILD_TRACKER.md (task 5).

Point-in-time correct throughout, mirroring analyse_match()'s no-lookahead discipline
(BUG-008) -- for each match, the player-level lambda (compute_club_player_strength.
load_team_players' before_date), the blend weight's "current squad" signal
(squad_as_of_date, NOT soccer_players.team_id -- see that function's docstring for
why the live signal can't be used for a past season), and the league baseline
(get_league_averages' before_date) are all computed using ONLY data that existed
strictly before that match. Team-level ratings reuse the existing, already-safe
get_team_ratings via compute_club_player_strength.team_level_lambda.

Matches are processed one matchday (exact match_date) at a time: compute_club_player_
strength.compute() re-derives the WHOLE field's player-level ratings for every unique
before_date anyway (the cross-team normalization step needs the full field), so
grouping same-date matches avoids redundant recomputation without changing the result.

Usage:
    python backfill_player_blend_predictions.py --league "Serie A" --season 2025
"""

import argparse
import sqlite3
from datetime import datetime, timezone
from itertools import groupby

from core.sports_db import (
    DATABASE_PATH,
    clear_soccer_model_predictions,
    add_soccer_model_prediction,
)
from core.poisson_model import analyse_match_wc
import compute_club_player_strength as strength

DEFAULT_METHOD = "poisson_v4"


def load_all_team_ids(conn, league, season):
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT t.team_id FROM soccer_teams t
        JOIN soccer_matches m ON m.home_team_id = t.team_id OR m.away_team_id = t.team_id
        WHERE m.league = ? AND m.season = ?
    """, (league, season))
    return [r[0] for r in cur.fetchall()]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--league", default="Serie A")
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--method", default=DEFAULT_METHOD)
    args = parser.parse_args()

    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT sm.match_id, sm.home_team_id, sm.away_team_id, sm.match_date,
               o.home_moneyline, o.draw_moneyline, o.away_moneyline,
               o.over_under, o.over_odds, o.under_odds
        FROM soccer_matches sm
        JOIN soccer_betting_odds o ON o.match_id = sm.match_id
        WHERE sm.league = ? AND sm.season = ?
        ORDER BY sm.match_date
    """, (args.league, args.season))
    rows = cur.fetchall()

    if not rows:
        print(f"No matches with odds found for {args.league} season {args.season}")
        conn.close()
        return

    team_ids = load_all_team_ids(conn, args.league, args.season)
    clear_soccer_model_predictions(args.league, args.season, args.method, conn=conn)

    generated_at = datetime.now(timezone.utc).isoformat()
    inserted = 0

    for match_date, date_rows in groupby(rows, key=lambda r: r["match_date"]):
        date_rows = list(date_rows)
        squad_ids_by_team = {tid: strength.squad_as_of_date(conn, tid, args.season, match_date)
                             for tid in team_ids}
        results = strength.compute(conn, team_ids, args.league, args.season, match_date,
                                   current_squad_ids_by_team=squad_ids_by_team)

        for row in date_rows:
            home = results[row["home_team_id"]]
            away = results[row["away_team_id"]]

            result = analyse_match_wc(
                lambda_home_attack=home["lambda_attack_blend"],
                lambda_away_attack=away["lambda_attack_blend"],
                lambda_home_defense=home["lambda_defense_blend"],
                lambda_away_defense=away["lambda_defense_blend"],
                home_moneyline=row["home_moneyline"],
                draw_moneyline=row["draw_moneyline"],
                away_moneyline=row["away_moneyline"],
                ou_line=row["over_under"],
                over_odds=row["over_odds"],
                under_odds=row["under_odds"],
                baseline=home["baseline"],
            )

            add_soccer_model_prediction(
                match_id=row["match_id"],
                league=args.league,
                match_date=row["match_date"],
                generated_at=generated_at,
                method=args.method,
                lambda_home=result["lambda_H"],
                lambda_away=result["lambda_A"],
                p_home=result["p_home"],
                p_draw=result["p_draw"],
                p_away=result["p_away"],
                over_under_line=row["over_under"],
                p_over=result.get("p_over"),
                p_under=result.get("p_under"),
                home_moneyline=row["home_moneyline"],
                draw_moneyline=row["draw_moneyline"],
                away_moneyline=row["away_moneyline"],
                over_odds=row["over_odds"],
                under_odds=row["under_odds"],
                ev_home=result.get("ev_home"),
                ev_draw=result.get("ev_draw"),
                ev_away=result.get("ev_away"),
                ev_over=result.get("ev_over"),
                ev_under=result.get("ev_under"),
                conn=conn,
            )
            inserted += 1

    conn.close()
    print(f"Inserted {inserted} prediction rows for {args.league} season {args.season} "
          f"(method={args.method})")


if __name__ == "__main__":
    main()
