"""
Record a World Cup 2026 KNOCKOUT result — the full tie path — and grade picks.

Knockout ties can't end level: 90' regulation, then extra time, then a penalty shootout.
This records every level (so 1X2 + O/U settle on the 90' score and the to-advance market
settles on who actually advanced), plus optional player-level detail (penalty kicks and
extra-time goals) for later ET/PK analysis. Who advanced is DERIVED from the scores.

Group-stage results still go through update_wc_results.py; this is the knockout path.

Usage:
    python record_knockout.py --home Brazil --away Croatia \\
        --reg 1-1 --et 2-2 --shootout 4-3 \\
        --pk home:Neymar:goal --pk away:Modric:saved \\
        --et-goal home:Vinicius:98
    python record_knockout.py --match-id 90 --reg 2-1        # decided in regulation
"""

import argparse
import sqlite3
import sys

from core.sports_db import (
    DATABASE_PATH, set_wc_match_advance_result, add_penalty_kick, add_extra_time_goal,
)
from core.grading import advancing_side
from import_wc_odds import load_team_map, load_match_index, resolve_team, find_match
from update_wc_results import grade_match_picks, grade_match_overrides


def parse_score(text):
    """'2-1' -> (2, 1); blank/None -> (None, None)."""
    if not text:
        return None, None
    try:
        home, away = text.split("-")
        return int(home), int(away)
    except (ValueError, AttributeError):
        sys.exit(f"Bad score {text!r}; expected 'home-away' like '2-1'.")


def _side_team(side, home_id, away_id):
    s = side.strip().lower()
    if s == "home":
        return home_id
    if s == "away":
        return away_id
    sys.exit(f"side must be 'home' or 'away', got {side!r}.")


def parse_pk(spec, home_id, away_id):
    """'home:Neymar:goal' -> (team_id, 'Neymar', 'goal')."""
    parts = spec.split(":")
    if len(parts) != 3:
        sys.exit(f"Bad --pk {spec!r}; expected SIDE:PLAYER:RESULT.")
    side, player, result = parts
    if result not in ("goal", "miss", "saved"):
        sys.exit(f"--pk result must be goal/miss/saved, got {result!r}.")
    return _side_team(side, home_id, away_id), player.strip(), result


def parse_et_goal(spec, home_id, away_id):
    """'home:Vinicius:98' -> (team_id, 'Vinicius', 98)."""
    parts = spec.split(":")
    if len(parts) != 3:
        sys.exit(f"Bad --et-goal {spec!r}; expected SIDE:PLAYER:MINUTE.")
    side, player, minute = parts
    try:
        minute = int(minute)
    except ValueError:
        sys.exit(f"--et-goal minute must be an integer, got {minute!r}.")
    return _side_team(side, home_id, away_id), player.strip(), minute


def infer_decided_by(et_home, shootout_home, override=None):
    if override:
        return override
    if shootout_home is not None:
        return "shootout"
    if et_home is not None:
        return "extra_time"
    return "regulation"


def resolve_match(conn, args):
    """Return the match row (match_id, home_team_id, away_team_id, home_name, away_name)."""
    select = """SELECT m.match_id, m.home_team_id, m.away_team_id, h.name, a.name
                FROM soccer_wc_matches m
                JOIN soccer_wc_teams h ON h.team_id = m.home_team_id
                JOIN soccer_wc_teams a ON a.team_id = m.away_team_id
                WHERE m.match_id = ?"""
    if args.match_id:
        match_id = args.match_id
    elif args.home and args.away:
        home_id = resolve_team(args.home, load_team_map(conn))
        away_id = resolve_team(args.away, load_team_map(conn))
        match_id = (find_match(load_match_index(conn), home_id, away_id, args.date)
                    if home_id and away_id else None)
        if match_id is None:
            sys.exit(f"No fixture for {args.home} vs {args.away}.")
    else:
        sys.exit("Provide --match-id, or both --home and --away.")
    row = conn.execute(select, (match_id,)).fetchone()
    if not row:
        sys.exit(f"No match with id {match_id}.")
    return row


def resolve_player(conn, team_id, name):
    row = conn.execute(
        "SELECT player_id FROM soccer_wc_players WHERE team_id = ? AND name = ?",
        (team_id, name)).fetchone()
    return row[0] if row else None


def record(conn, match, reg, extra_time, shootout, decided_by, pk_specs, et_goal_specs):
    """Persist a knockout result + player events, grade picks/overrides. Returns a summary."""
    match_id, home_id, away_id, home_name, away_name = match
    reg_home, reg_away = reg
    et_home, et_away = extra_time
    shootout_home, shootout_away = shootout

    set_wc_match_advance_result(
        match_id, regulation_home=reg_home, regulation_away=reg_away,
        extra_time_home=et_home, extra_time_away=et_away,
        shootout_home=shootout_home, shootout_away=shootout_away, decided_by=decided_by)

    for order, spec in enumerate(pk_specs, start=1):
        team_id, player, result = parse_pk(spec, home_id, away_id)
        add_penalty_kick(match_id, team_id, kick_order=order, result=result,
                         player_id=resolve_player(conn, team_id, player), player_name=player)
    for spec in et_goal_specs:
        team_id, player, minute = parse_et_goal(spec, home_id, away_id)
        add_extra_time_goal(match_id, team_id, minute=minute,
                            player_id=resolve_player(conn, team_id, player), player_name=player)

    advanced = advancing_side(reg_home, reg_away, et_home, et_away, shootout_home, shootout_away)
    n_picks = grade_match_picks(conn, match_id, reg_home, reg_away, advanced=advanced)
    n_over = grade_match_overrides(conn, match_id, reg_home, reg_away, advanced=advanced)
    advanced_name = {"HOME": home_name, "AWAY": away_name}.get(advanced, "?")
    return {"home": home_name, "away": away_name, "advanced": advanced,
            "advanced_name": advanced_name, "graded_picks": n_picks,
            "graded_overrides": n_over, "decided_by": decided_by}


def parse_args():
    ap = argparse.ArgumentParser(description="Record a WC knockout result (full tie path).")
    ap.add_argument("--match-id", type=int)
    ap.add_argument("--home")
    ap.add_argument("--away")
    ap.add_argument("--date", help="Disambiguate when a pairing repeats.")
    ap.add_argument("--reg", required=True, help="90' regulation score, 'home-away' (e.g. 1-1).")
    ap.add_argument("--et", help="Cumulative score after extra time, 'home-away'.")
    ap.add_argument("--shootout", help="Penalty shootout tally, 'home-away'.")
    ap.add_argument("--decided-by", choices=["regulation", "extra_time", "shootout"],
                    help="Override the auto-inferred level the tie was decided at.")
    ap.add_argument("--pk", action="append", default=[], metavar="SIDE:PLAYER:RESULT",
                    help="A shootout kick, e.g. home:Neymar:goal (goal|miss|saved). Repeatable.")
    ap.add_argument("--et-goal", action="append", default=[], metavar="SIDE:PLAYER:MINUTE",
                    help="An extra-time goal, e.g. home:Vinicius:98. Repeatable.")
    return ap.parse_args()


def main():
    args = parse_args()
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    match = resolve_match(conn, args)
    reg = parse_score(args.reg)
    extra_time = parse_score(args.et)
    shootout = parse_score(args.shootout)
    decided_by = infer_decided_by(extra_time[0], shootout[0], args.decided_by)

    summary = record(conn, match, reg, extra_time, shootout, decided_by, args.pk, args.et_goal)
    conn.close()

    line = f"{summary['home']} {reg[0]}-{reg[1]} {summary['away']} ({decided_by})"
    if extra_time[0] is not None:
        line += f" | ET {extra_time[0]}-{extra_time[1]}"
    if shootout[0] is not None:
        line += f" | shootout {shootout[0]}-{shootout[1]}"
    print(line)
    print(f"  advanced: {summary['advanced_name']}")
    print(f"  recorded {len(args.pk)} penalty kick(s), {len(args.et_goal)} ET goal(s)")
    print(f"  graded {summary['graded_picks']} pick(s) and {summary['graded_overrides']} override(s)")


if __name__ == "__main__":
    main()
