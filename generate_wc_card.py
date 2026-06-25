"""
World Cup 2026 value card generator.

For each match in the target window this:
  - looks up the latest team strength (lambda_attack / lambda_defense) for both
    sides from soccer_wc_team_strength,
  - runs the Poisson model (analyse_match_wc) against the posted 1X2 and
    over/under odds — evaluating totals at whatever line the book posted,
  - selects the single best (highest-EV) pick per match,
  - assigns a 1-3 star confidence rating from the EV gap (no abstention: every
    priced match gets a pick, even a low- or negative-EV one),
  - stores the pick in soccer_wc_picks for later scoring, and
  - prints a diagnostic table plus a social-post-ready block.

Usage:
    python generate_wc_card.py                 # today + tomorrow (UTC)
    python generate_wc_card.py --date 2026-06-11
    python generate_wc_card.py --dry-run       # don't store picks
"""

import argparse
import sqlite3
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from core.sports_db import DATABASE_PATH, get_latest_wc_strength, replace_wc_pick
from core.poisson_model import (
    analyse_match_wc, advance_probs, ev_to_stars,
    american_to_implied_prob, compute_ev,
)
from compute_wc_team_strength import compute_bench_indices

# match_date is stored in UTC, but the tournament is hosted in North America and
# matchdays are reckoned in Eastern time (the US broadcast/posting frame), so a
# late-evening kickoff stays on its Eastern calendar day. We bucket by Eastern at
# query time. The whole tournament (Jun 11 - Jul 19) is EDT = UTC-4, with no DST
# transition inside the window, so a fixed shift is exact throughout.
TOURNAMENT_TZ = ZoneInfo("America/New_York")
# Matchday boundary is 4am ET (a "broadcast day"), not midnight. Games kick off
# only at 00:00 and 12:00-23:00 ET, leaving a 1am-11am ET dead zone, so a midnight
# (00:00 ET) game groups with the PRIOR day's slate instead of spilling onto the
# next calendar date. -8h total = -4h (UTC->ET) - 4h (midnight->4am boundary).
EASTERN_SQL_OFFSET = "-8 hours"   # match_date -> matchday (4am-ET broadcast day)

# The three host nations play their group games in their own countries, so they
# carry a real home-field edge regardless of which side FIFA lists as "home"
# (each host is the nominal away team in one of its three group games). We apply
# the boost to whichever side the host is on. 1.20 ~ a moderate home edge.
HOST_NATIONS = {"USA", "Mexico", "Canada"}
HOST_HOME_ADVANTAGE = 1.20

# Selection guardrails (BUG-003 — noise amplification on longshots). Both are
# evaluated INDEPENDENTLY on every candidate so we can log which one excluded a
# pick; a candidate must clear BOTH to be eligible.
#
# floor — we won't surface ANY pick (win, draw, or total) whose model probability
# is below this. At long odds a tiny probability overestimate becomes a large,
# fake EV%, so a sub-floor pick's EV is noise, not edge. Applies to all markets.
MIN_PICK_PROBABILITY = 0.25
# cap — we won't surface a pick whose model probability is >= this multiple of the
# market's implied probability. Only an underdog can trip it (a favorite can't be
# 2x its own high implied prob), so it targets the case the floor misses: the model
# *confidently over-rating a dog* (e.g. Jordan +725, model 0.353 vs market 0.121 =
# 2.9x — cleared the floor at 0.353, but the disagreement is the model under-rating
# the favorite, not edge). Set on principle ("twice the market's price"), not fit.
MAX_UNDERDOG_MARKET_DISAGREEMENT = 2.0


def parse_args():
    parser = argparse.ArgumentParser(description="Generate the World Cup 2026 pick card.")
    parser.add_argument("--date", help="Target a single Eastern-time matchday "
                                       "(YYYY-MM-DD). Defaults to today + tomorrow "
                                       "(US Eastern).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the card without storing picks in soccer_wc_picks.")
    return parser.parse_args()


def match_window(target_date):
    """Return (start, end) ISO Eastern-day dates for the query window."""
    if target_date:
        return target_date, target_date
    # Same 4am-ET broadcast-day boundary as EASTERN_SQL_OFFSET, so running in the
    # small hours (after a midnight game) still resolves to the prior day's slate.
    today = (datetime.now(timezone.utc).astimezone(TOURNAMENT_TZ)
             - timedelta(hours=4)).date()
    return today.isoformat(), (today + timedelta(days=1)).isoformat()


def fetch_matches(conn, start, end):
    cur = conn.cursor()
    cur.execute("""
        SELECT m.match_id, m.match_date, m.stage, m.grp,
               m.home_team_id, m.away_team_id,
               ht.name AS home, at.name AS away,
               o.home_moneyline, o.draw_moneyline, o.away_moneyline,
               o.over_under, o.over_odds, o.under_odds,
               o.home_advance_ml, o.away_advance_ml
        FROM soccer_wc_matches m
        JOIN soccer_wc_teams ht ON ht.team_id = m.home_team_id
        JOIN soccer_wc_teams at ON at.team_id = m.away_team_id
        JOIN soccer_wc_odds  o  ON o.match_id = m.match_id
        WHERE date(m.match_date, ?) >= date(?)
          AND date(m.match_date, ?) <= date(?)
        ORDER BY m.match_date, m.match_id
    """, (EASTERN_SQL_OFFSET, start, EASTERN_SQL_OFFSET, end))
    return cur.fetchall()


def display_pick(side, home, away):
    """Human-readable pick text for the social post."""
    if side == "HOME":
        return home
    if side == "AWAY":
        return away
    if side == "DRAW":
        return "Draw"
    if side == "HOME ADVANCE":
        return f"{home} to advance"
    if side == "AWAY ADVANCE":
        return f"{away} to advance"
    return side.replace("OVER", "Over").replace("UNDER", "Under")


def select_pick(priced):
    """Choose the pick from a list of priced candidates, applying the two BUG-003
    guardrails. Pure (no DB) so it's directly testable.

    Each candidate dict needs ``side``, ``odds``, ``prob`` (model probability),
    ``implied`` (market-implied probability), and ``ev``. The two guardrails are
    evaluated INDEPENDENTLY on every candidate (no short-circuit) so each exclusion
    records which check(s) fired:
      - floor: model prob below ``MIN_PICK_PROBABILITY`` (any market).
      - cap:   model prob >= ``MAX_UNDERDOG_MARKET_DISAGREEMENT`` x implied prob
               (only underdogs can trip it; catches the model over-rating a dog).
    A candidate must clear BOTH to be eligible. The chosen dict is returned with
    ``excluded_by`` set on every candidate, ``demoted`` = the higher-EV candidates
    that were excluded, and ``fallback`` = True if no candidate cleared both gates.
    """
    for c in priced:
        reasons = []
        if c["prob"] < MIN_PICK_PROBABILITY:
            reasons.append(
                f"floor (model {c['prob']:.3f} < {MIN_PICK_PROBABILITY:g})")
        if c["implied"] and c["prob"] >= MAX_UNDERDOG_MARKET_DISAGREEMENT * c["implied"]:
            reasons.append(
                f"cap (model {c['prob']:.3f} >= {MAX_UNDERDOG_MARKET_DISAGREEMENT:g}x "
                f"market {c['implied']:.3f})")
        c["excluded_by"] = reasons

    eligible = [c for c in priced if not c["excluded_by"]]
    if eligible:
        best = max(eligible, key=lambda c: c["ev"])
    else:
        # No abstention: nothing cleared both gates, so back the MOST LIKELY
        # outcome (highest model prob), not the highest EV — picking by EV would
        # hand it straight back to the longshot we just excluded.
        best = max(priced, key=lambda c: c["prob"])
        best["fallback"] = True
    # For the log: higher-EV candidates that were excluded (what we would have
    # picked without the guardrails, and why we didn't).
    best["demoted"] = [c for c in priced
                       if c["excluded_by"] and c["ev"] > best["ev"]]
    return best


def best_pick_for_match(match, conn, bench_indices=None):
    """Return the highest-EV pick dict for a match row, or None.

    None means we can't price the match (missing team strength or no markets).
    bench_indices ({team_id: field-centered bench index}) feeds the ET/shootout nudge
    for advance markets; omitted/None leaves the nudge off (zero indices).
    """
    home_strength = get_latest_wc_strength(match["home_team_id"], conn=conn)
    away_strength = get_latest_wc_strength(match["away_team_id"], conn=conn)
    if home_strength is None or away_strength is None:
        return None

    h_att, h_def = home_strength
    a_att, a_def = away_strength

    # A host nation gets the venue edge on whichever side it's listed, since it
    # plays its group games at home regardless of FIFA's home/away designation.
    home_adv = HOST_HOME_ADVANTAGE if match["home"] in HOST_NATIONS else 1.0
    away_adv = HOST_HOME_ADVANTAGE if match["away"] in HOST_NATIONS else 1.0

    r = analyse_match_wc(
        lambda_home_attack=h_att, lambda_away_attack=a_att,
        lambda_home_defense=h_def, lambda_away_defense=a_def,
        home_moneyline=match["home_moneyline"],
        draw_moneyline=match["draw_moneyline"],
        away_moneyline=match["away_moneyline"],
        ou_line=match["over_under"],
        over_odds=match["over_odds"],
        under_odds=match["under_odds"],
        home_advantage=home_adv,
        away_advantage=away_adv,
    )

    line = match["over_under"]
    line_label = f"{line:g}" if line is not None else ""
    candidates = [
        ("HOME", match["home_moneyline"], r.get("p_home"), r.get("ev_home")),
        ("DRAW", match["draw_moneyline"], r.get("p_draw"), r.get("ev_draw")),
        ("AWAY", match["away_moneyline"], r.get("p_away"), r.get("ev_away")),
        (f"OVER {line_label}",  match["over_odds"],  r.get("p_over"),  r.get("ev_over")),
        (f"UNDER {line_label}", match["under_odds"], r.get("p_under"), r.get("ev_under")),
    ]

    # Knockout "to advance" market. The book having posted advance odds is the SOLE
    # trigger: for group games / ties we haven't ingested, these are NULL and the card
    # is byte-for-byte unchanged. advance_probs reuses the 90' lambdas analyse_match_wc
    # derived; bench nudge stays off (Step 5) via the default zero indices.
    home_adv_ml = match["home_advance_ml"]
    away_adv_ml = match["away_advance_ml"]
    if home_adv_ml is not None and away_adv_ml is not None:
        bench = bench_indices or {}
        adv = advance_probs(
            r["lambda_H"], r["lambda_A"],
            bench_index_home=bench.get(match["home_team_id"], 0.0),
            bench_index_away=bench.get(match["away_team_id"], 0.0))
        candidates.append(("HOME ADVANCE", home_adv_ml, adv["p_home_advance"],
                           compute_ev(adv["p_home_advance"], home_adv_ml)))
        candidates.append(("AWAY ADVANCE", away_adv_ml, adv["p_away_advance"],
                           compute_ev(adv["p_away_advance"], away_adv_ml)))

    priced = [
        {"side": side, "odds": odds, "prob": prob, "ev": ev,
         "implied": american_to_implied_prob(odds)}
        for side, odds, prob, ev in candidates
        if ev is not None and odds is not None and prob is not None
    ]
    if not priced:
        return None

    best = select_pick(priced)
    best.update({
        "match_id": match["match_id"],
        "match_date": match["match_date"],
        "home": match["home"],
        "away": match["away"],
        "stars": ev_to_stars(best["ev"]),
    })
    return best


def main():
    args = parse_args()
    start, end = match_window(args.date)

    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    matches = fetch_matches(conn, start, end)

    # Bench indices are only needed (and only computed) when the window has a knockout
    # match with advance odds — group-stage cards do zero extra work.
    has_advance = any(m["home_advance_ml"] is not None for m in matches)
    bench_indices = compute_bench_indices(conn) if has_advance else None

    picks = []
    skipped = []
    for match in matches:
        pick = best_pick_for_match(match, conn, bench_indices)
        if pick is None:
            skipped.append(f"{match['home']} vs {match['away']}")
            continue
        picks.append(pick)

    picks.sort(key=lambda p: p["ev"], reverse=True)

    generated_at = datetime.now(timezone.utc).isoformat()
    if not args.dry_run:
        for p in picks:
            replace_wc_pick(
                match_id=p["match_id"], generated_at=generated_at,
                side=p["side"], odds=p["odds"], model_prob=p["prob"],
                ev=p["ev"], stars=p["stars"],
            )
    conn.close()

    print(f"WINDOW {start} to {end}")
    print(f"MATCHES {len(matches)}  PICKS {len(picks)}"
          f"{'  (dry-run, not stored)' if args.dry_run else ''}")
    if skipped:
        print(f"SKIPPED {len(skipped)} (no strength or no priced markets): "
              + ", ".join(skipped))
    print("")

    print("=== PICKS (RANKED BY EV) ===")
    for i, p in enumerate(picks, 1):
        imp = american_to_implied_prob(p["odds"])
        print(f"{i:>2}. {p['home']} vs {p['away']} | {p['side']:<9} "
              f"| odds {p['odds']:+.0f} | imp p {imp:.3f} | model p {p['prob']:.3f} "
              f"| EV {p['ev']:+.1%} | {'⭐' * p['stars']}")
    print("")

    demoted_any = [p for p in picks if p.get("demoted") or p.get("fallback")]
    if demoted_any:
        print("=== GUARDRAIL LOG (BUG-003) ===")
        for p in demoted_any:
            for d in p["demoted"]:
                print(f"  {p['home']} vs {p['away']}: {d['side']} "
                      f"(EV {d['ev']:+.1%}) EXCLUDED by {' & '.join(d['excluded_by'])} "
                      f"-> selected {p['side']}")
            if p.get("fallback"):
                print(f"  {p['home']} vs {p['away']}: no candidate cleared both gates "
                      f"-> fell back to most-likely side {p['side']} (model {p['prob']:.3f})")
        print("")

    print("=== SOCIAL POST ===")
    for p in picks:
        pick_text = display_pick(p["side"], p["home"], p["away"])
        print(f"{p['home']} / {p['away']} — {pick_text} ({'⭐' * p['stars']})")


if __name__ == "__main__":
    main()
