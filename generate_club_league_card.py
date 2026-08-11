"""
Club-league value card generator -- parameterized by --league rather than hardcoded
to one competition, so adding a second club league is a data problem, not a new
script (2026-08-07: replaces generate_serie_a_card.py, which was Serie A-specific
AND, separately, still on the pre-FEATURE-011 team-only model -- see BUGS.md).

Runs the REAL player-blend pipeline (compute_club_player_strength.compute() +
core.poisson_model.analyse_match_wc()), the same one backfill_player_blend_
predictions.py uses to build soccer_model_predictions for backtesting -- until now
nothing in the live path benefited from FEATURE-011/BUG-009/BUG-010's work at all.
current_squad_ids_by_team is left None (compute()'s live-use default: current actual
roster, not a point-in-time reconstruction).

Applies CLUB_LEAGUE_MIN_PICK_PROBABILITY as a guardrail (BUG-003's pattern, via the
shared core.pick_guardrails -- BUG-009 2026-08-05 found this had never been ported
from the WC card generator). No cap: swept on top of the xG-stretch fix and found
close to inert (BUGS.md, BUG-009, 2026-08-07 addendum) -- floor alone is the
validated guardrail here, not floor+cap.

Only surfaces genuine positive-EV, guardrail-clear candidates (unlike the old
script, which showed the top-2-by-EV per match regardless of EV sign) -- brings
this tool in line with backtest_from_predictions.py's own screening discipline.

Usage:
    python generate_club_league_card.py                        # Serie A, next 1-4 days
    python generate_club_league_card.py --league "Serie A" --days-ahead 1 7
"""

import argparse
import sqlite3
from datetime import datetime, timezone, timedelta
from itertools import groupby

from core.sports_db import DATABASE_PATH
from core.poisson_model import analyse_match_wc, american_to_implied_prob
from core.pick_guardrails import guardrail_reasons
import compute_club_player_strength as strength

DEFAULT_LEAGUE = "Serie A"
MAX_PICKS_PER_MATCH = 2

# Guardrail floor (BUG-003 pattern via core.pick_guardrails). Validated 2026-08-05
# (as a retrofit onto poisson_v4_priorblend) and re-checked 2026-08-07 on top of the
# xG-stretch fix -- still a real, if season-asymmetric, improvement; see BUGS.md,
# BUG-009. Owned independently from generate_wc_card.py's MIN_PICK_PROBABILITY (same
# value, 0.25, by coincidence of separate validation on different data, not a shared
# constant) since club leagues and the World Cup were tuned against different data.
CLUB_LEAGUE_MIN_PICK_PROBABILITY = 0.25


def load_team_ids(conn, league, season):
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT t.team_id FROM soccer_teams t
        JOIN soccer_matches m ON m.home_team_id = t.team_id OR m.away_team_id = t.team_id
        WHERE m.league = ? AND m.season = ?
    """, (league, season))
    return [r[0] for r in cur.fetchall()]


def build_candidates(match, result):
    candidates = []
    for side, prob, ev, ml in (
        ("HOME", result["p_home"], result.get("ev_home"), match["home_moneyline"]),
        ("DRAW", result["p_draw"], result.get("ev_draw"), match["draw_moneyline"]),
        ("AWAY", result["p_away"], result.get("ev_away"), match["away_moneyline"]),
        ("OVER 2.5", result.get("p_over"), result.get("ev_over"), match["over_odds"]),
        ("UNDER 2.5", result.get("p_under"), result.get("ev_under"), match["under_odds"]),
    ):
        if prob is None or ev is None or ml is None:
            continue
        implied = american_to_implied_prob(ml)
        candidates.append({
            "match_id": match["match_id"], "match_date": match["match_date"],
            "home": match["home"], "away": match["away"],
            "side": side, "odds": ml, "prob": prob, "implied": implied, "ev": ev,
        })
    return candidates


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--league", default=DEFAULT_LEAGUE)
    parser.add_argument("--days-ahead", type=int, nargs=2, default=(1, 4), metavar=("START", "END"),
                        help="Window is [today+START, today+END] (UTC calendar days). Default: 1 4.")
    args = parser.parse_args()

    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    now = datetime.now(timezone.utc)
    start_offset, end_offset = args.days_ahead
    start = (now.date() + timedelta(days=start_offset)).isoformat()
    end = (now.date() + timedelta(days=end_offset)).isoformat()

    cur.execute('''
        SELECT m.match_id, m.match_date, m.season, ht.name AS home, at.name AS away,
               m.home_team_id, m.away_team_id,
               o.home_moneyline, o.draw_moneyline, o.away_moneyline,
               o.over_under, o.over_odds, o.under_odds
        FROM soccer_matches m
        JOIN soccer_teams ht ON ht.team_id = m.home_team_id
        JOIN soccer_teams at ON at.team_id = m.away_team_id
        JOIN soccer_betting_odds o ON o.match_id = m.match_id
        WHERE m.league = ?
          AND date(m.match_date) >= date(?)
          AND date(m.match_date) <= date(?)
        ORDER BY m.match_date, m.match_id
    ''', (args.league, start, end))
    rows = cur.fetchall()

    print(f"LEAGUE {args.league}")
    print(f"WINDOW {start} to {end}")
    print(f"MATCHES {len(rows)}")
    print()

    if not rows:
        print("No matches with priced markets")
        conn.close()
        return

    ranked_matches = []
    excluded_log = []

    for match_date, date_rows in groupby(rows, key=lambda r: r["match_date"]):
        date_rows = list(date_rows)
        season = date_rows[0]["season"]
        team_ids = load_team_ids(conn, args.league, season)
        results = strength.compute(conn, team_ids, args.league, season, match_date)

        for row in date_rows:
            # Totals only priced when the posted line is 2.5 -- analyse_match_wc's
            # p_over/p_under are computed at whatever ou_line is passed in.
            ou_line = row["over_under"]
            if ou_line is not None and abs(float(ou_line) - 2.5) > 1e-9:
                ou_line = None

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
                ou_line=ou_line,
                over_odds=row["over_odds"] if ou_line is not None else None,
                under_odds=row["under_odds"] if ou_line is not None else None,
                baseline=avg_home,
                home_advantage=1.0,
                away_advantage=avg_home / avg_away,
            )

            candidates = build_candidates(row, result)
            for c in candidates:
                c["excluded_by"] = guardrail_reasons(c["prob"], c["implied"], CLUB_LEAGUE_MIN_PICK_PROBABILITY)

            clean = [c for c in candidates if not c["excluded_by"] and c["ev"] > 0]
            excluded_log.extend(c for c in candidates if c["excluded_by"] and c["ev"] > 0)

            if not clean:
                continue
            clean.sort(key=lambda c: c["ev"], reverse=True)
            ranked_matches.append(clean[:MAX_PICKS_PER_MATCH])

    ranked_matches.sort(key=lambda picks: picks[0]["ev"], reverse=True)

    def fmt_pick(rank_label, pick):
        return (
            f"{rank_label:>3} {pick['home']} vs {pick['away']} | {pick['side']:<9} "
            f"| odds {pick['odds']:+.0f} | model p {pick['prob']:.3f} | EV {pick['ev']:+.1%} | stake 1.00u"
        )

    print("=== TOP PICKS PER MATCH (guardrail-clear, positive-EV only, ranked by best pick's EV) ===")
    if ranked_matches:
        for index, picks in enumerate(ranked_matches, 1):
            print(fmt_pick(f"{index}.", picks[0]))
            for extra in picks[1:]:
                print(fmt_pick(f"{index}b", extra))
            print()
    else:
        print("No guardrail-clear positive-EV picks in this window")

    if excluded_log:
        print(f"=== GUARDRAIL LOG ({len(excluded_log)} positive-EV candidates excluded) ===")
        for c in sorted(excluded_log, key=lambda c: c["ev"], reverse=True):
            print(f"    {c['home']} vs {c['away']} | {c['side']:<9} | model p {c['prob']:.3f} "
                  f"| EV {c['ev']:+.1%} | {' & '.join(c['excluded_by'])}")

    conn.close()


if __name__ == "__main__":
    main()
