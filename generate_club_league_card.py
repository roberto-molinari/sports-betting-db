"""
Club-league value card generator -- parameterized by --league rather than hardcoded
to one competition, so adding a second club league is a data problem, not a new
script (2026-08-07: replaces generate_serie_a_card.py, which was Serie A-specific
AND, separately, still on the pre-FEATURE-011 team-only model -- see BUGS.md).

Runs the REAL player-blend pipeline (compute_club_player_strength.compute() +
core.poisson_model.analyse_match_wc()), the same one backfill_player_blend_
predictions.py uses to build soccer_model_predictions for backtesting -- until now
nothing in the live path benefited from FEATURE-011/BUG-009/BUG-010's work at all.
current_roster_ids_by_team is left None (compute()'s live-use default: current actual
roster, not a point-in-time reconstruction).

Applies CLUB_LEAGUE_MIN_PICK_PROBABILITY as a guardrail (BUG-003's pattern, via the
shared core.pick_guardrails -- BUG-009 2026-08-05 found this had never been ported
from the WC card generator), plus a market-side floor on the market's own implied
probability (BUG-009 re-diagnosis, 2026-08-20 -- see market_floor_for_league()'s
comment) -- CLUB_LEAGUE_MIN_MARKET_PROBABILITY by default, with a per-league
override for the two leagues that showed real, sustained signal for a different
value (2026-08-21). No cap: swept on top of the xG-stretch fix and found close to
inert (BUGS.md, BUG-009, 2026-08-07 addendum) -- the two floors are the validated
guardrails here, not floor+cap.

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

from core.sports_db import DATABASE_PATH, replace_club_league_picks_for_match
from core.poisson_model import analyse_match_wc, american_to_implied_prob, ev_to_stars
from core.pick_guardrails import guardrail_reasons
from core.leagues import LEAGUES, has_odds_source
import compute_club_player_strength as strength

# Card generation only makes sense for a league with a real odds source -- the
# feeder divisions in core.leagues.LEAGUES (Serie B, Championship, etc.) exist
# for cross-league player history, not for picks. Restricting --league's
# choices to this list (2026-08-19) means a typo'd league name now fails loudly
# via argparse's own error message instead of silently matching zero matches
# and printing "MATCHES 0" with no indication anything was wrong.
CARD_LEAGUES = sorted(lg for lg in LEAGUES if has_odds_source(lg))

DEFAULT_LEAGUE = "Serie A"
MAX_PICKS_PER_MATCH = 2

# Guardrail floor (BUG-003 pattern via core.pick_guardrails). Validated 2026-08-05
# (as a retrofit onto poisson_v4_priorblend) and re-checked 2026-08-07 on top of the
# xG-stretch fix -- still a real, if season-asymmetric, improvement; see BUGS.md,
# BUG-009. Owned independently from generate_wc_card.py's MIN_PICK_PROBABILITY (same
# value, 0.25, by coincidence of separate validation on different data, not a shared
# constant) since club leagues and the World Cup were tuned against different data.
CLUB_LEAGUE_MIN_PICK_PROBABILITY = 0.25

# MARKET-side floor (BUG-009 re-diagnosis, 2026-08-20): reject any candidate whose
# vig-inclusive single-side implied probability (1/decimal odds -- exactly what
# build_candidates() computes) is below this, regardless of what the model thinks.
# The model floor above can't catch the worst-losing segment (market prices the side
# as a longshot, model says >= 0.25) because a huge model-vs-market gap on a
# corr-0.83 model is far more likely estimation noise than edge, and the
# proportional devig's residual favorite-longshot bias makes long odds look fairer
# than they are (BUGS.md, BUG-009, 2026-08-20 -- both mechanisms measured).
#
# Re-swept 2026-08-21 (post power-devig() + 2022-23 backfill for the 4 new leagues,
# ~2x the data behind the original 0.32): implied 0.32-0.35 turned out to be a
# specifically weak zone -- the market itself overstates outcomes there (its own
# realized-vs-implied gap is the worst of any nearby band, present in every league/
# season checked), and the model's usual overconfidence relative to the market gets
# amplified on top of that. 0.35 is the largest floor before the curve flattens into
# thin-sample noise on the pooled data (5 leagues x 4 seasons): bets 2660->2214,
# profit -$157.40 -> -$58.86. Every league improves individually, including the one
# already-profitable league -- see BUGS.md.
CLUB_LEAGUE_MIN_MARKET_PROBABILITY = 0.35

# Per-league override (2026-08-21): most leagues showed no reliable signal for a
# different floor once checked with a smoothed sliding-window fit (not just the raw
# per-league sweep table, which is noisy enough at ~1,000-1,300 candidates/league to
# manufacture a fake "optimum" -- exactly the trap the 0.32-0.35 band investigation
# above was caught in once, at the pooled level). Only two leagues showed a real,
# wide, sustained positive-ROI zone under smoothing: Premier League (positive from
# ~0.22 to ~0.42) and La Liga (negative until ~0.38, then positive). Both are
# clamped to within +/-0.05 of CLUB_LEAGUE_MIN_MARKET_PROBABILITY -- deliberately
# not their raw full-strength targets (Premier League's smoothed zone starts as low
# as ~0.22-0.25) -- high-ROI tails in a backtest this size (EV>10% segments) are
# more likely to be small-sample luck than a real, durable edge against professional
# closing lines, so the clamp intentionally keeps these closer to the larger, more
# stable pooled evidence rather than chasing one league's best-looking number.
# Leagues not listed here use the shared floor above.
CLUB_LEAGUE_MARKET_PROBABILITY_BY_LEAGUE = {
    "Premier League": 0.30,
    "La Liga": 0.40,
}


def market_floor_for_league(league):
    """The market-side guardrail floor to use for `league` -- its own override if
    one exists (CLUB_LEAGUE_MARKET_PROBABILITY_BY_LEAGUE), else the shared default
    (CLUB_LEAGUE_MIN_MARKET_PROBABILITY). Single source of truth so the live card,
    backtest_from_predictions.py, and model_metrics_report.py can't drift apart."""
    return CLUB_LEAGUE_MARKET_PROBABILITY_BY_LEAGUE.get(league, CLUB_LEAGUE_MIN_MARKET_PROBABILITY)


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
    parser.add_argument("--league", default=DEFAULT_LEAGUE, choices=CARD_LEAGUES)
    parser.add_argument("--days-ahead", type=int, nargs=2, default=(1, 4), metavar=("START", "END"),
                        help="Window is [today+START, today+END] (UTC calendar days). Default: 1 4.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the card without storing picks in soccer_club_league_picks "
                             "(FEATURE-016) -- same flag/behavior as generate_wc_card.py.")
    args = parser.parse_args()

    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    now = datetime.now(timezone.utc)
    start_offset, end_offset = args.days_ahead
    start = (now.date() + timedelta(days=start_offset)).isoformat()
    end = (now.date() + timedelta(days=end_offset)).isoformat()

    # ONE odds row per match -- see backfill_player_blend_predictions.py's
    # identical subquery for the rationale (BUG-018, 2026-08-20: a match priced
    # by several books used to be processed once per book, silently overwriting
    # its stored picks with whichever odds row came last).
    cur.execute('''
        SELECT m.match_id, m.match_date, m.season, ht.name AS home, at.name AS away,
               m.home_team_id, m.away_team_id,
               o.home_moneyline, o.draw_moneyline, o.away_moneyline,
               o.over_under, o.over_odds, o.under_odds
        FROM soccer_matches m
        JOIN soccer_teams ht ON ht.team_id = m.home_team_id
        JOIN soccer_teams at ON at.team_id = m.away_team_id
        JOIN soccer_betting_odds o ON o.odds_id = (
            SELECT o2.odds_id FROM soccer_betting_odds o2
            WHERE o2.match_id = m.match_id
            ORDER BY (o2.sportsbook = 'Bet365') DESC, o2.odds_date DESC, o2.odds_id DESC
            LIMIT 1
        )
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
    generated_at = datetime.now(timezone.utc).isoformat()

    dates = sorted({strength.match_calendar_date(r["match_date"]) for r in rows})
    for before_date in dates:
        date_rows = strength.matches_on_date(rows, before_date)
        season = date_rows[0]["season"]
        team_ids = load_team_ids(conn, args.league, season)
        results = strength.compute(conn, team_ids, args.league, season, before_date)

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
                c["excluded_by"] = guardrail_reasons(c["prob"], c["implied"], CLUB_LEAGUE_MIN_PICK_PROBABILITY,
                                                     market_floor=market_floor_for_league(args.league))

            clean = [c for c in candidates if not c["excluded_by"] and c["ev"] > 0]
            excluded_log.extend(c for c in candidates if c["excluded_by"] and c["ev"] > 0)

            if not clean:
                continue
            clean.sort(key=lambda c: c["ev"], reverse=True)
            top_picks = clean[:MAX_PICKS_PER_MATCH]
            ranked_matches.append(top_picks)

            if not args.dry_run:
                for p in top_picks:
                    p["stars"] = ev_to_stars(p["ev"])
                replace_club_league_picks_for_match(
                    match_id=row["match_id"], league=args.league,
                    generated_at=generated_at, picks=top_picks,
                )

    ranked_matches.sort(key=lambda picks: picks[0]["ev"], reverse=True)

    def fmt_pick(rank_label, pick):
        match_date = strength.match_calendar_date(pick["match_date"])
        return (
            f"{rank_label:>3} {match_date} {pick['home']} vs {pick['away']} | {pick['side']:<9} "
            f"| odds {pick['odds']:+.0f} | model p {pick['prob']:.3f} | EV {pick['ev']:+.1%} | stake 1.00u"
        )

    picks_total = sum(len(picks) for picks in ranked_matches)
    print(f"=== TOP PICKS PER MATCH (guardrail-clear, positive-EV only, ranked by best pick's EV) ==="
          f"{f'  [{picks_total} picks stored in soccer_club_league_picks]' if not args.dry_run and picks_total else ''}"
          f"{'  (dry-run, not stored)' if args.dry_run else ''}")
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
            match_date = strength.match_calendar_date(c["match_date"])
            print(f"    {match_date} {c['home']} vs {c['away']} | {c['side']:<9} | model p {c['prob']:.3f} "
                  f"| EV {c['ev']:+.1%} | {' & '.join(c['excluded_by'])}")

    conn.close()


if __name__ == "__main__":
    main()
