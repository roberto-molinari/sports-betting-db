"""
Backfill soccer_model_predictions for a completed season.

Runs today's Poisson model (core/poisson_model.analyse_match) against every
match in a league/season using that match's own posted odds and a
before-this-match-only cutoff for team ratings (the same no-lookahead
behaviour analyse_match always uses) -- so the result is what the model
would have produced at the time, even though no pick was ever generated.

Usage:
    python backfill_soccer_model_predictions.py --league "Serie A" --season 2025
"""

import argparse
import sqlite3
from datetime import datetime, timezone

from core.sports_db import (
    DATABASE_PATH,
    clear_soccer_model_predictions,
    add_soccer_model_prediction,
)
from core.poisson_model import (
    analyse_match,
    scoreline_grid,
    totals_probs,
    compute_ev_totals,
)

DEFAULT_METHOD = "poisson_v3"  # v3 = post-BUG-009 partial fix (windowed league averages)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--league", default="Serie A")
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--method", default=DEFAULT_METHOD)
    args = parser.parse_args()

    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # ONE odds row per match -- see backfill_player_blend_predictions.py's
    # identical subquery for the rationale (BUG-018, 2026-08-20: multi-book
    # matches used to insert duplicate prediction rows).
    cur.execute("""
        SELECT sm.match_id, sm.home_team_id, sm.away_team_id, sm.match_date,
               o.home_moneyline, o.draw_moneyline, o.away_moneyline,
               o.over_under, o.over_odds, o.under_odds
        FROM soccer_matches sm
        JOIN soccer_betting_odds o ON o.odds_id = (
            SELECT o2.odds_id FROM soccer_betting_odds o2
            WHERE o2.match_id = sm.match_id
            ORDER BY (o2.sportsbook = 'Bet365') DESC, o2.odds_date DESC, o2.odds_id DESC
            LIMIT 1
        )
        WHERE sm.league = ? AND sm.season = ?
        ORDER BY sm.match_date
    """, (args.league, args.season))
    rows = cur.fetchall()

    if not rows:
        print(f"No matches with odds found for {args.league} season {args.season}")
        conn.close()
        return

    clear_soccer_model_predictions(args.league, args.season, args.method, conn=conn)

    generated_at = datetime.now(timezone.utc).isoformat()
    inserted = 0

    for row in rows:
        result = analyse_match(
            home_team_id=row["home_team_id"],
            away_team_id=row["away_team_id"],
            match_date=row["match_date"],
            home_moneyline=row["home_moneyline"],
            draw_moneyline=row["draw_moneyline"],
            away_moneyline=row["away_moneyline"],
            league=args.league,
            conn=conn,
        )

        p_over = p_under = ev_over = ev_under = None
        line = row["over_under"]
        if line is not None:
            grid = scoreline_grid(result["lambda_H"], result["lambda_A"])
            totals = totals_probs(grid, line)
            p_over, p_under = totals["p_over"], totals["p_under"]
            if row["over_odds"] is not None:
                ev_over = compute_ev_totals(p_over, p_under, row["over_odds"])
            if row["under_odds"] is not None:
                ev_under = compute_ev_totals(p_under, p_over, row["under_odds"])

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
            over_under_line=line,
            p_over=p_over,
            p_under=p_under,
            home_moneyline=row["home_moneyline"],
            draw_moneyline=row["draw_moneyline"],
            away_moneyline=row["away_moneyline"],
            over_odds=row["over_odds"],
            under_odds=row["under_odds"],
            ev_home=result.get("ev_home"),
            ev_draw=result.get("ev_draw"),
            ev_away=result.get("ev_away"),
            ev_over=ev_over,
            ev_under=ev_under,
            conn=conn,
        )
        inserted += 1

    conn.close()
    print(f"Inserted {inserted} prediction rows for {args.league} season {args.season} "
          f"(method={args.method})")


if __name__ == "__main__":
    main()
