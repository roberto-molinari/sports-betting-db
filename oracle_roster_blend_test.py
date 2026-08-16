"""
EXPERIMENTAL, not shipped -- estimates the CEILING impact on bias/ROI if the model's
player-roster awareness were "50% better" (FEATURE-011 Follow-up A). See BUGS.md,
FEATURE-011 Follow-up A, and the 2026-08-08 roster-coverage investigation this
supports.

No changes to compute_club_player_strength.py -- load_team_players is monkeypatched
for the duration of this script only, same pattern as backfill_with_xg_stretch.py.

METHOD (an "oracle blend," not a real lineup-prediction system): for the SPECIFIC
match being predicted, each player's rate is blended `BLEND_FRAC` of the way from
today's trailing-window rate toward a rate computed from what they ACTUALLY did in
THAT exact match (real minutes, real goals/xg, real club_xga_per90 that game). A
player currently invisible to the model (zero trailing history -- new arrivals,
long-unused-then-suddenly-started players alike) is added at BLEND_FRAC of their
real match weight/rate instead of being fully excluded.

THIS USES THE MATCH'S OWN OUTCOME AS AN INPUT -- it is hindsight the model would
never have before kickoff. The result is a CEILING estimate ("if a future
roster/lineup-prediction system got halfway to PERFECT foresight, what's the
payoff"), not a promise of what a real, realistic implementation would achieve.
Label results accordingly.

Usage:
    python3 oracle_roster_blend_test.py --league "Serie A" --season 2024 --season 2025 \\
        --blend-frac 0.5 --method poisson_v4_oracleroster50
"""
import argparse
import sqlite3
from datetime import datetime, timezone

from core.sports_db import DATABASE_PATH, clear_soccer_model_predictions, add_soccer_model_prediction
from core.poisson_model import analyse_match_wc
import compute_club_player_strength as strength

LEAGUE_DEFAULT = "Serie A"
_ORIG_LOAD_TEAM_PLAYERS = strength.load_team_players


def make_oracle_blend(blend_frac):
    """Drop-in replacement for load_team_players. Starts from the real function's
    output, then nudges each team's players toward what actually happened in that
    team's own match on `before_date` (there's at most one, since a team plays at
    most once per date)."""

    def oracle_load_team_players(conn, team_ids, before_date, **kwargs):
        by_team = _ORIG_LOAD_TEAM_PLAYERS(conn, team_ids, before_date, **kwargs)
        cur = conn.cursor()

        for team_id in team_ids:
            cur.execute("""
                SELECT match_id FROM soccer_matches
                WHERE (home_team_id=? OR away_team_id=?) AND match_date=?
            """, (team_id, team_id, before_date))
            row = cur.fetchone()
            if row is None:
                continue
            match_id = row[0]

            cur.execute("""
                SELECT s.player_id, p.position, s.minutes_played, s.goals, s.xg, s.club_xga_per90
                FROM soccer_player_stats s
                JOIN soccer_players p ON p.player_id = s.player_id
                JOIN soccer_matches m ON m.match_id = s.match_id
                WHERE s.match_id = ? AND (
                    (s.venue='home' AND m.home_team_id=?) OR (s.venue='away' AND m.away_team_id=?)
                )
            """, (match_id, team_id, team_id))
            actual_rows = cur.fetchall()

            existing = {p["player_id"]: p for p in by_team[team_id]}
            blended = []
            seen_ids = set()
            for pid, position, minutes, goals, xg, club_xga90 in actual_rows:
                if not minutes:
                    continue
                seen_ids.add(pid)
                oracle_attack = ((xg if xg is not None else goals) or 0) / minutes * 90
                oracle_defense = club_xga90

                if pid in existing:
                    p = dict(existing[pid])
                    p["attack_rate"] = (1 - blend_frac) * p["attack_rate"] + blend_frac * oracle_attack
                    if p["club_ga_per90"] is not None and oracle_defense is not None:
                        p["club_ga_per90"] = (1 - blend_frac) * p["club_ga_per90"] + blend_frac * oracle_defense
                    blended.append(p)
                else:
                    # Currently invisible (zero trailing history OR excluded because
                    # attack_den<=0, e.g. an all-bench recent run) -- add at
                    # blend_frac of their real match weight/rate.
                    blended.append({
                        "player_id": pid, "pos": strength.normalize_position(position),
                        "attack_rate": oracle_attack, "attack_minutes": blend_frac * minutes,
                        "club_ga_per90": oracle_defense, "defense_minutes": blend_frac * minutes if oracle_defense is not None else 0.0,
                    })

            for pid, p in existing.items():
                if pid not in seen_ids:
                    blended.append(p)
            by_team[team_id] = blended

        return by_team

    return oracle_load_team_players


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
    parser.add_argument("--blend-frac", type=float, default=0.5,
                        help="0.0=today's real behavior (no-op), 1.0=full oracle "
                             "(perfect same-match knowledge). Default 0.5.")
    args = parser.parse_args()

    strength.load_team_players = make_oracle_blend(args.blend_frac)

    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    generated_at = datetime.now(timezone.utc).isoformat()
    total_inserted = 0

    for season in args.seasons:
        clear_soccer_model_predictions(args.league, season, args.method, conn=conn)
        team_ids = load_all_team_ids(conn, args.league, season)

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
        dates = sorted({strength.match_calendar_date(r["match_date"]) for r in rows})
        for before_date in dates:
            date_rows = strength.matches_on_date(rows, before_date)
            results = strength.compute(conn, team_ids, args.league, season, before_date, cache=cache)

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
        print(f"season {season}: inserted {inserted} rows (method={args.method}, blend_frac={args.blend_frac})")

    conn.close()
    print(f"Total inserted: {total_inserted}")


if __name__ == "__main__":
    main()
