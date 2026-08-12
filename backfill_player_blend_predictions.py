"""
Backfill soccer_model_predictions for a completed season using FEATURE-011's
player-level blend (method='poisson_v4'), for Success Criteria validation (signed
bias vs. sharp closing lines, full-season ROI) against the existing team-level
poisson_v3 -- see FEATURE-011_REQUIREMENTS.md (Success Criteria) and
FEATURE-011_BUILD_TRACKER.md (task 5).

Point-in-time correct throughout, mirroring analyse_match()'s no-lookahead discipline
(BUG-008) -- for each match, the player-level lambda (compute_club_player_strength.
load_team_players' before_date), the blend weight's "current squad" signal
(roster_as_of_date, NOT soccer_players.team_id -- see that function's docstring for
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
    parser.add_argument("--weight-attack", type=float, default=None,
                        help="Force this attack weight for ALL teams (0=pure player, 1=pure "
                             "team), bypassing per-team resolution. Debugging/comparison only "
                             "-- e.g. --weight-attack 1 --weight-defense 1 isolates whether a "
                             "bias regression comes from the blend pipeline itself rather than "
                             "from player data.")
    parser.add_argument("--weight-defense", type=float, default=None)
    parser.add_argument("--attack-metric", dest="attack_xg_v_goals_source",
                        choices=["xg", "goals"], default="xg",
                        help="Player-level attack signal: xg (default) or goals. "
                             "Debugging/comparison only -- see compute_club_player_strength."
                             "load_team_players' docstring.")
    parser.add_argument("--team-xg-weight", dest="team_xg_v_goals_blend", type=float, default=1.0,
                        help="Team-level attack/defense xG/goals blend: 1.0 (default) "
                             "is pure xG, a no-op matching the 2026-08-02 fix that "
                             "cleared the Model Calibration success criterion; 0.0 is "
                             "pure goals (matches poisson_v3 exactly); values in "
                             "between blend the two -- see BUG-009's mismatch-size-"
                             "compression diagnosis and compute_club_player_strength."
                             "team_level_lambda's docstring. Never changes poisson_v3 "
                             "or core.poisson_model either way.")
    parser.add_argument("--xg-stretch-attack", dest="xg_spread_stretch_attack", type=float,
                        default=strength.TEAM_RATING_XG_SPREAD_STRETCH_ATTACK,
                        help="Team-level xG attack-rating spread multiplier (default: the "
                             "shipped TEAM_RATING_XG_SPREAD_STRETCH_ATTACK, currently 1.3). "
                             "1.0 reproduces the pre-2026-08-07 shape exactly -- see that "
                             "constant's comment and BUG-009's 2026-08-07 addendum.")
    parser.add_argument("--xg-stretch-defense", dest="xg_spread_stretch_defense", type=float,
                        default=strength.TEAM_RATING_XG_SPREAD_STRETCH_DEFENSE,
                        help="Team-level xG defense-rating spread multiplier -- see "
                             "--xg-stretch-attack and TEAM_RATING_XG_SPREAD_STRETCH_DEFENSE's "
                             "comment (split from a single shared constant 2026-08-12, "
                             "BUG-010 continued: defense showed more xG-vs-goals compression "
                             "than attack).")
    parser.add_argument("--player-stretch-attack", dest="player_spread_stretch_attack", type=float,
                        default=strength.PLAYER_RATING_SPREAD_STRETCH_ATTACK,
                        help="Player-level attack-rating spread multiplier, same mechanism "
                             "as --xg-stretch-attack one level down -- see "
                             "PLAYER_RATING_SPREAD_STRETCH_ATTACK's comment. Default is the "
                             "shipped, calibrated 2.0 (2026-08-12 sweep); pass 1.0 for the "
                             "true no-op.")
    parser.add_argument("--player-stretch-defense", dest="player_spread_stretch_defense", type=float,
                        default=strength.PLAYER_RATING_SPREAD_STRETCH_DEFENSE,
                        help="Player-level defense-rating spread multiplier -- see "
                             "--player-stretch-attack and "
                             "PLAYER_RATING_SPREAD_STRETCH_DEFENSE's comment.")
    parser.add_argument("--player-window-min-date", default=None,
                        help="Comparison/validation only: an ISO date lower bound "
                             "(e.g. a season start date) that stops the player-level "
                             "rolling window from reaching further back than this, for "
                             "A/B-checking the season-blind default (omit this flag) "
                             "against a season-SCOPED window. See compute_club_player_"
                             "strength.load_team_players' docstring "
                             "(FEATURE-011 Follow-up B, 2026-08-06).")
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
    cache = {}  # BUG-011: memoizes last-season aggregates across this run's matchdays

    for match_date, date_rows in groupby(rows, key=lambda r: r["match_date"]):
        date_rows = list(date_rows)
        roster_ids_by_team = {tid: strength.roster_as_of_date(conn, tid, args.season, match_date)
                             for tid in team_ids}
        results = strength.compute(conn, team_ids, args.league, args.season, match_date,
                                   w_attack=args.weight_attack, w_defense=args.weight_defense,
                                   attack_xg_v_goals_source=args.attack_xg_v_goals_source, team_xg_v_goals_blend=args.team_xg_v_goals_blend,
                                   xg_spread_stretch_attack=args.xg_spread_stretch_attack,
                                   xg_spread_stretch_defense=args.xg_spread_stretch_defense,
                                   player_spread_stretch_attack=args.player_spread_stretch_attack,
                                   player_spread_stretch_defense=args.player_spread_stretch_defense,
                                   player_window_min_date=args.player_window_min_date,
                                   current_roster_ids_by_team=roster_ids_by_team, cache=cache)

        for row in date_rows:
            home = results[row["home_team_id"]]
            away = results[row["away_team_id"]]

            # baseline=avg_home, away_advantage=avg_home/avg_away reproduces estimate_lambdas()'s
            # own dual-baseline formula (lambda_H normalized by avg_home, lambda_A by avg_away)
            # through analyse_match_wc's existing venue-multiplier params -- see
            # FEATURE-011_BUILD_TRACKER.md task 5 (2026-08-01 fix) for the derivation.
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
