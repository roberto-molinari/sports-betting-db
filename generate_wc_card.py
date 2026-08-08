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
    american_to_implied_prob, american_to_decimal, compute_ev,
)
from core.wc_host_advantage import host_advantage
from core.wc_knockout_scale import knockout_goal_scale
import core.pick_guardrails as guardrails
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

# Host-nation venue advantage (BUG-006): single source of truth is
# core.wc_host_advantage, since a host only gets the boost when actually playing
# in its own country -- true for group games by tournament design, but NOT
# guaranteed for knockout stadium assignments. See that module for the team
# lists; host_advantage(team, stage) is the one function every consumer (this
# card, calibration/backtest scripts) should call.

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
# advance-edge cap (BUG-003 update 2026-06-29) — the ratio cap above is the WRONG tool
# for the "to advance" market. Advance probs compress toward 0.5 (the draw->ET->penalty
# path drags every team toward a coin flip), so a dog's advance prob has a small RATIO to
# the market even when the ABSOLUTE over-rating is huge — the mirage slips the 2x cap
# (Paraguay 0.377 vs market 0.190 = 1.98x cleared, yet +18.7 pts; SA 0.346 vs 0.278 =
# 1.24x cleared, +6.8 pts). The model's club-stats inputs make mismatches look closer than
# they are, inflating the underdog's advance number past any realistic edge. So for an
# underdog ADVANCE candidate we add an ABSOLUTE-points check: demote when model prob beats
# market implied by >= this gap. 0.07 catches both knockout cases (Paraguay & SA).
MAX_ADVANCE_ABSOLUTE_DISAGREEMENT = 0.07

# FEATURE-009 two-step selection (2026-07-03) — codifies "always name a best pick,
# never pass" as an explicit two-mode decision instead of a one-off hand-override.
# Both bars were backtested against all 85 graded picks to date (feature009_backtest.py)
# before being set; a full b1 x b2 grid sweep plus a neighboring-cell stress test (units,
# hit-rate, and a group-vs-knockout stage split) support these values. Kept as separate
# constants (not folded into the existing BUG-003 guardrails) because they gate a
# different decision — MODE selection (trust the model vs. trust the market), not
# per-candidate exclusion — and are expected to move independently as more results land.
#
# Step 1 — VALUE MODE (trust the model): among candidates that clear the existing
# guardrails AND have model probability >= this bar, take the highest-EV one IF that
# EV is positive. This is today's "realistic probability" floor from BUGS.md's
# FEATURE-009 design note — set well above MIN_PICK_PROBABILITY (0.25) because a model
# probability merely clearing the noise floor isn't enough to trust the model's own EV
# ranking; it must be in the range the model is actually well-calibrated.
VALUE_MODE_MIN_PROBABILITY = 0.60
# Step 2 — PREDICTION MODE (only if step 1 finds nothing): stop trusting the model's
# EV ranking and defer to the market instead. Among ALL candidates (guardrails don't
# apply here — we've already given up on the model for this match) with a market-
# IMPLIED probability >= this bar, take the best payout. The bar keeps "best payout"
# from landing back on a coin-flip loser (e.g. a -148 near-toss-up beating a genuine
# -152 favorite on payout alone) by restricting the payout search to genuine favorites.
PREDICTION_MODE_MIN_IMPLIED_PROBABILITY = 0.60

# Close-calls diagnostic (2026-07-06) — informational only, never changes the
# selected pick. A BUG-003-guardrail-excluded candidate still shows up in the
# value-mode candidate list (see mode_breakdown) if it missed clearing the
# guardrail by this little in probability points — e.g. a 2.01x cap instead of
# 2.0x, or a 0.24 floor instead of 0.25. Widen/narrow to surface more/fewer
# near-misses.
CLOSE_CALL_TOLERANCE = 0.02  # 200 basis points


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
    """Choose the pick from a list of priced candidates via FEATURE-009's two-step,
    never-pass selection. Pure (no DB) so it's directly testable.

    Each candidate dict needs ``side``, ``odds``, ``prob`` (model probability),
    ``implied`` (market-implied probability), and ``ev``.

    Guardrails (BUG-003) are evaluated INDEPENDENTLY on every candidate (no
    short-circuit) so each exclusion records which check(s) fired:
      - floor: model prob below ``MIN_PICK_PROBABILITY`` (any market).
      - cap:   model prob >= ``MAX_UNDERDOG_MARKET_DISAGREEMENT`` x implied prob
               (only underdogs can trip it; catches the model over-rating a dog).
      - advance-edge: absolute-points version of the cap, for the to-advance market.
    A candidate must clear ALL THREE guardrails to be eligible for step 1.

    Step 1 — VALUE MODE (trust the model). Among guardrail-clear candidates with
    model prob >= ``VALUE_MODE_MIN_PROBABILITY``, take the highest-EV one IF that
    EV is positive. If found, stop here.
    Step 2 — PREDICTION MODE (only if step 1 finds nothing). Guardrails no longer
    apply — we've stopped trusting the model's own ranking for this match. Among
    ALL candidates with market-implied prob >= ``PREDICTION_MODE_MIN_IMPLIED_PROBABILITY``,
    take the best payout (not EV).
    Fallback — neither step qualifies: back the most likely side (highest model
    prob among ALL candidates), same as today's existing safety net.

    The chosen dict is returned with ``excluded_by`` set on every candidate,
    ``demoted`` = the higher-EV guardrail-excluded candidates, ``mode`` =
    "value" | "prediction" | "fallback", and ``fallback`` = True in the fallback case.
    """
    for c in priced:
        reasons = guardrails.guardrail_reasons(
            c["prob"], c["implied"], MIN_PICK_PROBABILITY, MAX_UNDERDOG_MARKET_DISAGREEMENT)
        # advance-edge: ABSOLUTE-points cap for the to-advance market, where the ratio cap
        # above can't see the mirage (probs compress toward 0.5). Only an underdog ADVANCE
        # candidate (market implied < 0.5) can trip it — a favorite's model advance prob is
        # below the market here (the model under-rates favorites), so it never fires on them.
        if ("ADVANCE" in c["side"] and c["implied"] and c["implied"] < 0.5
                and c["prob"] - c["implied"] >= MAX_ADVANCE_ABSOLUTE_DISAGREEMENT):
            reasons.append(
                f"advance-edge (model {c['prob']:.3f} - market {c['implied']:.3f} "
                f"= {c['prob'] - c['implied']:+.3f} >= +{MAX_ADVANCE_ABSOLUTE_DISAGREEMENT:g})")
        c["excluded_by"] = reasons

    guardrail_clear = [c for c in priced if not c["excluded_by"]]
    value_candidates = [c for c in guardrail_clear
                        if c["prob"] >= VALUE_MODE_MIN_PROBABILITY and c["ev"] > 0]

    if value_candidates:
        best = max(value_candidates, key=lambda c: c["ev"])
        best["mode"] = "value"
    else:
        prediction_candidates = [
            c for c in priced
            if c["implied"] and c["implied"] >= PREDICTION_MODE_MIN_IMPLIED_PROBABILITY
        ]
        if prediction_candidates:
            best = max(prediction_candidates, key=lambda c: american_to_decimal(c["odds"]))
            best["mode"] = "prediction"
        else:
            # No abstention: nothing cleared either mode, so back the MOST LIKELY
            # outcome (highest model prob), not the highest EV — picking by EV would
            # hand it straight back to a longshot mirage.
            best = max(priced, key=lambda c: c["prob"])
            best["mode"] = "fallback"
            best["fallback"] = True

    # For the log: higher-EV candidates that were excluded by a guardrail (what we
    # would have picked without them, and why we didn't).
    best["demoted"] = [c for c in priced
                       if c["excluded_by"] and c["ev"] > best["ev"]]
    return best


def guardrail_excess(c):
    """How far an EXCLUDED candidate sits past whichever guardrail(s) it tripped,
    in probability points -- the LARGEST excess if more than one fired (a
    candidate must clear ALL of them to become guardrail-clear, so the hardest
    one to fix is what determines how close it really is). None if the
    candidate wasn't excluded. Used only by mode_breakdown's close-calls
    relaxation; requires ``excluded_by`` to already be set (a select_pick
    side effect)."""
    if not c["excluded_by"]:
        return None
    excesses = []
    shared = guardrails.guardrail_excess(
        c["prob"], c["implied"], MIN_PICK_PROBABILITY, MAX_UNDERDOG_MARKET_DISAGREEMENT)
    if shared is not None:
        excesses.append(shared)
    if ("ADVANCE" in c["side"] and c["implied"] and c["implied"] < 0.5
            and c["prob"] - c["implied"] >= MAX_ADVANCE_ABSOLUTE_DISAGREEMENT):
        excesses.append((c["prob"] - c["implied"]) - MAX_ADVANCE_ABSOLUTE_DISAGREEMENT)
    return max(excesses) if excesses else None


def why_not_value(c):
    """Human-readable reason a candidate ISN'T eligible for value mode -- a
    BUG-003 guardrail reason if one fired, else why it fails FEATURE-009's own
    bars. None means the candidate actually clears everything (it IS a real
    value-mode candidate -- if it's also the highest-EV one, it's the pick).
    Used only by the top-EV diagnostic in mode_breakdown."""
    if c["excluded_by"]:
        return " & ".join(c["excluded_by"])
    if c["prob"] < VALUE_MODE_MIN_PROBABILITY:
        return f"model prob {c['prob']:.3f} < value bar {VALUE_MODE_MIN_PROBABILITY:g}"
    if c["ev"] <= 0:
        return "EV not positive"
    return None


def mode_breakdown(priced, top_n=3):
    """Diagnostic-only "close calls" view: the top ``top_n`` candidates for EACH
    of FEATURE-009's three modes, independent of which one select_pick actually
    chose. Pure and read-only -- it never changes the pick; it exists purely so
    a human can eyeball what else was close. Requires ``priced`` to already have
    ``excluded_by`` set (select_pick does this as a side effect before this is
    called).

    - value: guardrail-clear (or a near-miss within CLOSE_CALL_TOLERANCE, tagged
      ``near_miss``) candidates with prob >= VALUE_MODE_MIN_PROBABILITY and
      positive EV, ranked by EV.
    - prediction: candidates with implied >= PREDICTION_MODE_MIN_IMPLIED_PROBABILITY,
      ranked by payout (decimal odds) -- guardrails don't apply here, matching
      select_pick's own step 2.
    - fallback: ALL candidates ranked by model probability -- guardrails don't
      apply here either, matching select_pick's own last resort.
    - top_ev: the top candidates by RAW EV, no probability filter and no
      guardrail filter at all -- the honest "what looked best on paper" list,
      each annotated (via why_not_value) with why it isn't a value pick. This
      is what answers "why isn't the model finding value here?" at a glance.
    """
    for c in priced:
        excess = guardrail_excess(c)
        c["near_miss"] = bool(c["excluded_by"]) and excess is not None \
            and excess <= CLOSE_CALL_TOLERANCE

    value_pool = [c for c in priced if not c["excluded_by"] or c["near_miss"]]
    value = sorted(
        [c for c in value_pool if c["prob"] >= VALUE_MODE_MIN_PROBABILITY and c["ev"] > 0],
        key=lambda c: c["ev"], reverse=True)[:top_n]

    prediction = sorted(
        [c for c in priced
         if c["implied"] and c["implied"] >= PREDICTION_MODE_MIN_IMPLIED_PROBABILITY],
        key=lambda c: american_to_decimal(c["odds"]), reverse=True)[:top_n]

    fallback = sorted(priced, key=lambda c: c["prob"], reverse=True)[:top_n]

    top_ev = sorted(priced, key=lambda c: c["ev"], reverse=True)[:top_n]

    return {"value": value, "prediction": prediction, "fallback": fallback, "top_ev": top_ev}


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

    # A host nation gets the venue edge on whichever side it's listed, but ONLY for
    # the stage(s) it's actually confirmed to be playing at home (BUG-006). Both
    # sides also get the knockout goal-level correction (BUG-004): the model
    # over-projects total goals in knockout play, so this scales both teams'
    # lambda down uniformly for any non-Group stage.
    level = knockout_goal_scale(match["stage"])
    home_adv = host_advantage(match["home"], match["stage"]) * level
    away_adv = host_advantage(match["away"], match["stage"]) * level

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
    best["breakdown"] = mode_breakdown(priced)
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
                ev=p["ev"], stars=p["stars"], selection_mode=p["mode"],
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
              f"| EV {p['ev']:+.1%} | {'⭐' * p['stars']} | mode {p['mode']}")
    print("")

    notable = [p for p in picks if p.get("demoted") or p["mode"] != "value"]
    if notable:
        print("=== GUARDRAIL LOG (BUG-003 / FEATURE-009) ===")
        for p in notable:
            for d in p["demoted"]:
                print(f"  {p['home']} vs {p['away']}: {d['side']} "
                      f"(EV {d['ev']:+.1%}) EXCLUDED by {' & '.join(d['excluded_by'])} "
                      f"-> selected {p['side']} ({p['mode']} mode)")
            if p["mode"] == "prediction":
                print(f"  {p['home']} vs {p['away']}: no value-mode candidate qualified "
                      f"-> deferred to the market, selected {p['side']} "
                      f"(implied {p['implied']:.3f})")
            if p.get("fallback"):
                print(f"  {p['home']} vs {p['away']}: no candidate cleared value or "
                      f"prediction mode -> fell back to most-likely side {p['side']} "
                      f"(model {p['prob']:.3f})")
        print("")

    print("=== CANDIDATE BREAKDOWN (close calls; informational only, does not change the pick) ===")
    for p in picks:
        print(f"{p['home']} vs {p['away']}")
        for label, key, rule in (
            ("VALUE", "value", f"model probability>={VALUE_MODE_MIN_PROBABILITY:g} & EV>0"),
            ("PREDICTION", "prediction", f"implied probability>={PREDICTION_MODE_MIN_IMPLIED_PROBABILITY:g} & highest payout"),
            ("FALLBACK", "fallback", "highest model probability"),
        ):
            cands = p["breakdown"][key]
            print(f"  {label:<10} ({rule}):")
            if not cands:
                print("    none")
            for i, c in enumerate(cands, 1):
                tag = f"  [near-miss: {' & '.join(c['excluded_by'])}]" if c.get("near_miss") else ""
                print(f"    {i}. {c['side']:<9} odds {c['odds']:+.0f} | model {c['prob']:.3f} "
                      f"| implied {c['implied']:.3f} | EV {c['ev']:+.1%}{tag}")

        print("  TOP EV     (raw EV, no probability or guardrail filter -- why isn't this the pick?):")
        for i, c in enumerate(p["breakdown"]["top_ev"], 1):
            reason = why_not_value(c)
            tag = f"  [not a value pick: {reason}]" if reason else "  [this IS a clean value candidate]"
            print(f"    {i}. {c['side']:<9} odds {c['odds']:+.0f} | model {c['prob']:.3f} "
                  f"| implied {c['implied']:.3f} | EV {c['ev']:+.1%}{tag}")
    print("")

    print("=== SOCIAL POST ===")
    for p in picks:
        pick_text = display_pick(p["side"], p["home"], p["away"])
        print(f"{p['home']} / {p['away']} — {pick_text} ({'⭐' * p['stars']})")


if __name__ == "__main__":
    main()
