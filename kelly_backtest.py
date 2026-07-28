"""
Post-hoc Kelly Criterion backtest for the World Cup 2026 model's picks.

Every stored pick already carries the model's own probability at the time
(soccer_wc_picks.model_prob) and the posted odds, so Kelly sizing can be
reconstructed exactly -- no re-estimation needed. Compares a compounding
Kelly-staked bankroll (full/half/quarter) against the flat-1u-per-pick
approach actually used, chronologically in the order picks were generated.

Kelly fraction: f* = (b*p - q) / b, where b = decimal_odds - 1, p = model_prob,
q = 1-p. Negative f* (no edge per the model's own number) is clipped to 0 --
Kelly's answer for a -EV bet is "don't bet it", not "bet it anyway at flat
size", so those picks are sat out rather than forced into the comparison.

Usage:
    python kelly_backtest.py
    python kelly_backtest.py --bankroll 500
"""

import argparse
import sqlite3
from collections import OrderedDict

from core.sports_db import DATABASE_PATH
from core.poisson_model import american_to_decimal
from roi_history import STAGE_ORDER, STAGE_LABELS

FRACTIONS = {"Full Kelly": 1.0, "Half Kelly": 0.5, "Quarter Kelly": 0.25}
PHASE_OF = {s: ("Group" if s == "Group" else "Knockout") for s in STAGE_ORDER}


def kelly_fraction(model_prob, odds):
    b = american_to_decimal(odds) - 1
    p, q = model_prob, 1 - model_prob
    f = (b * p - q) / b
    return max(f, 0.0)


def load_picks(conn):
    rows = conn.execute(
        """SELECT p.generated_at, p.odds, p.model_prob, p.result, p.selection_mode,
                  m.stage, m.match_date, p.ev
           FROM soccer_wc_picks p JOIN soccer_wc_matches m ON p.match_id = m.match_id
           WHERE p.result IS NOT NULL
           ORDER BY m.match_date, p.generated_at"""
    ).fetchall()
    return rows


def simulate(rows, fraction, start_bankroll):
    bankroll = start_bankroll
    peak = start_bankroll
    max_drawdown = 0.0
    n_staked = 0
    for _, odds, model_prob, result, _, _, _, _ in rows:
        f = kelly_fraction(model_prob, odds) * fraction
        if f <= 0:
            continue
        n_staked += 1
        stake = bankroll * f
        if result == "win":
            bankroll += stake * (american_to_decimal(odds) - 1)
        elif result == "loss":
            bankroll -= stake
        # push: no change
        peak = max(peak, bankroll)
        max_drawdown = max(max_drawdown, (peak - bankroll) / peak if peak else 0)
    return bankroll, n_staked, max_drawdown


def simulate_flat(rows, unit_size):
    bankroll = 0.0
    for _, odds, _, result, _, _, _, _ in rows:
        if result == "win":
            bankroll += unit_size * (american_to_decimal(odds) - 1)
        elif result == "loss":
            bankroll -= unit_size
    return bankroll


def simulate_by_group(rows, fraction, start_bankroll, group_fn):
    """Walk picks chronologically (bankroll compounds across the whole
    tournament, never resets), snapshotting bankroll at the start/end of each
    group_fn(stage) bucket -- so a group's "return" reflects what that stretch
    of the tournament did to whatever bankroll it inherited from before it,
    same as how the actual money would have moved through time."""
    bankroll = start_bankroll
    groups = OrderedDict()
    for _, odds, model_prob, result, _, stage, _, _ in rows:
        g = group_fn(stage)
        if g not in groups:
            groups[g] = {"start": bankroll, "end": bankroll, "n_picks": 0, "n_staked": 0}
        groups[g]["n_picks"] += 1
        f = kelly_fraction(model_prob, odds) * fraction
        if f > 0:
            groups[g]["n_staked"] += 1
            stake = bankroll * f
            if result == "win":
                bankroll += stake * (american_to_decimal(odds) - 1)
            elif result == "loss":
                bankroll -= stake
        groups[g]["end"] = bankroll
    return groups


def simulate_flat_by_group(rows, group_fn):
    groups = OrderedDict()
    for _, odds, _, result, _, stage, _, _ in rows:
        g = group_fn(stage)
        if g not in groups:
            groups[g] = {"units": 0.0, "n": 0}
        groups[g]["n"] += 1
        if result == "win":
            groups[g]["units"] += american_to_decimal(odds) - 1
        elif result == "loss":
            groups[g]["units"] -= 1
    return groups


def print_group_table(rows, group_fn, labels, bankroll):
    flat = simulate_flat_by_group(rows, group_fn)
    kelly = {name: simulate_by_group(rows, frac, bankroll, group_fn)
             for name, frac in FRACTIONS.items()}

    header = f"{'':<18}{'Picks':>7}{'Flat ROI':>11}"
    for name in FRACTIONS:
        header += f"{name:>16}"
    print(header)
    for g in flat:
        label = labels.get(g, g)
        n = flat[g]["n"]
        flat_roi = flat[g]["units"] / n * 100 if n else 0.0
        line = f"{label:<18}{n:>7}{flat_roi:>+10.1f}%"
        for name in FRACTIONS:
            gd = kelly[name][g]
            growth = (gd["end"] - gd["start"]) / gd["start"] if gd["start"] else 0.0
            line += f"{growth:>+15.1%}"
        print(line)


def ev_bucket(ev, width):
    """(sort_key, label) for an EV value. Non-positive EV gets its own bucket
    since Kelly always sizes those at 0% -- shown for context, not omitted."""
    if ev <= 0:
        return (-1, "EV <= 0%")
    width_pct = width * 100
    lo = int(ev * 100 // width_pct) * width_pct
    hi = lo + width_pct
    return (lo, f"{lo:.0f}-{hi:.0f}% EV")


def simulate_isolated(rows, fraction, start_bankroll):
    """Same Kelly math as simulate(), but for a single bucket's picks treated
    as its own independent series (fresh bankroll) -- appropriate for EV
    buckets, which aren't a contiguous stretch of the tournament's timeline
    the way stages are, so there's no single "inherited bankroll" to compound
    from one bucket into the next."""
    bankroll = start_bankroll
    peak = start_bankroll
    max_drawdown = 0.0
    for _, odds, model_prob, result, _, _, _, _ in rows:
        f = kelly_fraction(model_prob, odds) * fraction
        if f <= 0:
            continue
        stake = bankroll * f
        if result == "win":
            bankroll += stake * (american_to_decimal(odds) - 1)
        elif result == "loss":
            bankroll -= stake
        peak = max(peak, bankroll)
        max_drawdown = max(max_drawdown, (peak - bankroll) / peak if peak else 0)
    return bankroll, max_drawdown


def print_ev_table(rows, width, bankroll):
    buckets = OrderedDict()
    for row in rows:
        ev = row[7]
        key, label = ev_bucket(ev, width)
        buckets.setdefault((key, label), []).append(row)

    header = f"{'EV bucket':<14}{'Picks':>7}{'Win %':>8}{'Flat ROI':>11}"
    for name in FRACTIONS:
        header += f"{name:>16}"
    header += f"{'Max drawdown':>15}"
    print(header)
    for (_, label), bucket_rows in sorted(buckets.items()):
        n = len(bucket_rows)
        wins = sum(r[3] == "win" for r in bucket_rows)
        flat_units = simulate_flat(bucket_rows, 1.0)
        flat_roi = flat_units / n * 100
        line = f"{label:<14}{n:>7}{wins/n:>7.0%}{flat_roi:>+10.1f}%"
        dd_full = None
        for name, frac in FRACTIONS.items():
            final, dd = simulate_isolated(bucket_rows, frac, bankroll)
            ret = (final - bankroll) / bankroll
            line += f"{ret:>+15.1%}"
            if frac == 1.0:
                dd_full = dd
        line += f"{dd_full:>14.1%}"
        print(line)


def main():
    ap = argparse.ArgumentParser(description="Post-hoc Kelly backtest for WC picks.")
    ap.add_argument("--bankroll", type=float, default=100.0,
                    help="Starting bankroll for Kelly sims (default 100 units).")
    ap.add_argument("--by-phase", action="store_true",
                    help="Break the comparison down by Group vs Knockout, and by stage.")
    ap.add_argument("--by-ev", action="store_true",
                    help="Break the comparison down by the model's reported EV, bucketed.")
    ap.add_argument("--ev-bucket-width", type=float, default=0.05,
                    help="EV bucket width as a fraction, e.g. 0.05 = 5-point buckets (default).")
    args = ap.parse_args()

    conn = sqlite3.connect(DATABASE_PATH)
    rows = load_picks(conn)
    conn.close()
    if not rows:
        print("No graded picks yet.")
        return

    total = len(rows)
    edges = [kelly_fraction(p, o) for _, o, p, *_ in rows]
    n_edge = sum(f > 0 for f in edges)

    print(f"{total} graded picks total. {n_edge} had positive model-edge Kelly would "
          f"actually stake ({n_edge/total:.0%}); the other {total - n_edge} "
          f"({(total-n_edge)/total:.0%}) get 0% stake under pure Kelly -- the model's "
          f"own probability didn't clear the posted odds on those.\n")

    flat_final = simulate_flat(rows, 1.0)
    print(f"{'Approach':<16}{'Final bankroll':>16}{'Return':>10}{'Picks staked':>14}"
          f"{'Max drawdown':>14}")
    print(f"{'Flat 1u':<16}{args.bankroll + flat_final:>16.2f}"
          f"{flat_final/args.bankroll:>+10.1%}{total:>14}{'n/a':>14}")

    for name, frac in FRACTIONS.items():
        final, n_staked, dd = simulate(rows, frac, args.bankroll)
        ret = (final - args.bankroll) / args.bankroll
        print(f"{name:<16}{final:>16.2f}{ret:>+10.1%}{n_staked:>14}{dd:>13.1%}")

    print(f"\n(Starting bankroll: {args.bankroll:.0f} units. Flat row is the actual "
          f"approach used all tournament, shown as +{args.bankroll:.0f}-baseline units "
          f"for a fair side-by-side; Kelly rows compound proportionally to bankroll.)")

    if args.by_phase:
        print("\n=== BY PHASE (Group vs Knockout) ===")
        print("Flat ROI% is that phase's own picks in isolation (units won / picks, "
              "same as roi_history.py's per-stage figures). Kelly % is that phase's "
              "growth on whatever bankroll it inherited from the phase before it -- "
              "i.e. it reflects compounding, same as how the money actually would have "
              "moved through time, not an isolated restart each phase.")
        print_group_table(rows, lambda s: PHASE_OF.get(s, s), {}, args.bankroll)

        print("\n=== BY STAGE (detail) ===")
        stage_labels = {s: STAGE_LABELS.get(s, s) for s in STAGE_ORDER}
        print_group_table(rows, lambda s: s, stage_labels, args.bankroll)

    if args.by_ev:
        print(f"\n=== BY EV BUCKET (model's reported EV, {args.ev_bucket_width:.0%}-wide) ===")
        print("Each bucket is its OWN isolated Kelly sim (fresh bankroll) since EV "
              "buckets aren't a contiguous timeline the way stages are -- there's no "
              "single bankroll to compound from one bucket into the next. Flat ROI is "
              "that bucket's picks in isolation, same basis as the stage tables.")
        print_ev_table(rows, args.ev_bucket_width, args.bankroll)


if __name__ == "__main__":
    main()
