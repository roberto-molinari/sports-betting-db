"""
Record a tagged human deviation from the model's pick on a World Cup match.

The model's pick (soccer_wc_picks) stays the single source of truth; this logs what
you took INSTEAD, the price, and — crucially — WHY (a short category + free-text
reason). Over time `override_report.py` scores model-vs-you head-to-head and groups
the reasons, so a recurring, profitable instinct can be turned into a systematic
model feature (the goal: anyone can follow the model, no human in the loop).

Only deviate when the model is at its resolution limit (near-tied options) AND you
have information it structurally can't see (recent form, injury, lineup, motivation).

Usage:
    python record_override.py --home Norway --away Senegal \\
        --side "OVER 2.5" --odds -112 --category form \\
        --reason "Norway dismantled Iraq, Haaland hot; model has no recent-form signal"
    python record_override.py --match-id 43 --side DRAW --odds 240 --reason "..."
"""

import argparse
import sqlite3
import sys

from core.sports_db import DATABASE_PATH, add_wc_pick_override


def parse_args():
    ap = argparse.ArgumentParser(description="Log a tagged deviation from the model's WC pick.")
    ap.add_argument("--match-id", type=int, help="Match id (or use --home/--away).")
    ap.add_argument("--home", help="Home team name.")
    ap.add_argument("--away", help="Away team name.")
    ap.add_argument("--side", required=True,
                    help='The side you took, e.g. "OVER 2.5", "UNDER 3.5", HOME, AWAY, DRAW.')
    ap.add_argument("--odds", type=float, required=True, help="American odds you took (e.g. -112, 240).")
    ap.add_argument("--category",
                    help="Short tag for the reason: form/injury/lineup/motivation/market/...")
    ap.add_argument("--reason", required=True, help="Why you went against the model (free text).")
    return ap.parse_args()


def resolve_match(conn, args):
    if args.match_id:
        row = conn.execute(
            """SELECT m.match_id, h.name, a.name FROM soccer_wc_matches m
               JOIN soccer_wc_teams h ON m.home_team_id = h.team_id
               JOIN soccer_wc_teams a ON m.away_team_id = a.team_id
               WHERE m.match_id = ?""", (args.match_id,)).fetchone()
    elif args.home and args.away:
        row = conn.execute(
            """SELECT m.match_id, h.name, a.name FROM soccer_wc_matches m
               JOIN soccer_wc_teams h ON m.home_team_id = h.team_id
               JOIN soccer_wc_teams a ON m.away_team_id = a.team_id
               WHERE h.name = ? AND a.name = ?""", (args.home, args.away)).fetchone()
    else:
        sys.exit("Provide --match-id, or both --home and --away.")
    if not row:
        sys.exit("No matching fixture found.")
    return row[0], f"{row[1]} v {row[2]}"


def model_pick(conn, match_id):
    """The model's current pick for the match (latest stored), or (None, None)."""
    row = conn.execute(
        "SELECT side, odds FROM soccer_wc_picks WHERE match_id = ? ORDER BY pick_id DESC LIMIT 1",
        (match_id,)).fetchone()
    return (row[0], row[1]) if row else (None, None)


def main():
    args = parse_args()
    conn = sqlite3.connect(DATABASE_PATH)
    match_id, label = resolve_match(conn, args)
    m_side, m_odds = model_pick(conn, match_id)
    conn.close()

    if m_side is None:
        print(f"WARNING: no stored model pick for {label} — generate the card first so "
              f"there's a baseline to deviate from. Recording the override anyway.")
    if m_side == args.side:
        print(f"NOTE: your side ({args.side}) matches the model's pick — that's not a deviation.")

    oid = add_wc_pick_override(
        match_id=match_id, user_side=args.side, user_odds=args.odds,
        reason=args.reason, category=args.category, model_side=m_side, model_odds=m_odds)

    print(f"Logged override #{oid} for {label}")
    print(f"  model: {m_side} @ {m_odds:+.0f}" if m_side else "  model: (none)")
    print(f"  you:   {args.side} @ {args.odds:+.0f}"
          + (f"  [{args.category}]" if args.category else ""))
    print(f"  why:   {args.reason}")


if __name__ == "__main__":
    main()
