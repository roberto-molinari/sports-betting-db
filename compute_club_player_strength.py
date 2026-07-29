"""
FEATURE-011 prototype: compute player-based attack/defense lambdas for club-league
teams and blend them against the EXISTING team-level lambda system, per the agreed
Player-Level Strength Estimation design (FEATURE-011_REQUIREMENTS.md):

    lambda = (1 - w) * player_lambda + w * team_lambda      (independently for
                                                               attack and defense)

Reuses compute_wc_team_strength.py's aggregation approach (position-weighted rates,
minutes-based shrinkage toward positional priors) but drops the WC-specific FIFA-rank
fallback/blend entirely -- the "other side" of the blend here is the EXISTING
club-league team-level lambda (core.poisson_model.get_team_ratings), not FIFA rank.

Prototype scope only (see FEATURE-011_PROTOTYPE_LOG.md for the full non-goals list):
  - single scalar attack/defense per team, no home/away split (Scenario 4 deferred)
  - fixed CLI-level blend weight, not the per-team default / league-wide-override
    resolution described in the requirements doc (that's a Cadence/Persistence-stage
    concern, not needed to prove the mechanism)
  - no league-quality factor applied: all players in scope play in the SAME league
    (Serie A), so LEAGUE_FACTORS would be a no-op here. It becomes relevant once a
    cross-league transfer scenario (Scenario 7) is in play.

Usage:
    python compute_club_player_strength.py --league "Serie A" --team "AC Milan"
    python compute_club_player_strength.py --league "Serie A" --limit-teams 3 --persist
"""

import argparse
import sqlite3
from datetime import date
from statistics import mean, pstdev

from core.sports_db import DATABASE_PATH, set_player_team_strength
from core.poisson_model import get_team_ratings, get_league_averages

# Same weights/rationale as compute_wc_team_strength.py (BUG-001/BUG-002 family):
# forwards carry attack, defenders/keepers carry defense, midfield contributes to both.
ATTACK_POS_WEIGHTS = {"FWD": 1.0, "MID": 0.6, "DEF": 0.2, "GK": 0.0}
DEFENSE_POS_WEIGHTS = {"GK": 1.0, "DEF": 0.8, "MID": 0.3, "FWD": 0.1}

# Same half-trust point as the WC system -- ~10 matches, set from how football works,
# not fit to this league.
K_SHRINK_MINUTES = 900.0

MIN_ATTACK_WEIGHT = 300.0    # lower than WC's 1000 -- a single-league squad has fewer
MIN_DEFENSE_WEIGHT = 300.0   # thin-coverage players diluting the pool than a WC squad does

_MID_CODES = {"m", "mf", "cm", "dm", "am", "cdm", "cam", "rm", "lm", "mid"}
_DEF_CODES = {"d", "df", "cb", "lb", "rb", "wb", "rwb", "lwb", "def"}
_FWD_CODES = {"f", "fw", "fwd", "st", "cf", "ss", "rw", "lw"}


def normalize_position(pos):
    if not pos:
        return None
    p = pos.strip().lower()
    if "goal" in p or p in {"gk", "g"}:
        return "GK"
    if "midfield" in p or "winger" in p or p in _MID_CODES:
        return "MID"
    if "back" in p or "defen" in p or p in _DEF_CODES:
        return "DEF"
    if "forward" in p or "striker" in p or "attack" in p or p in _FWD_CODES:
        return "FWD"
    return None


def load_team_players(conn, team_ids, season):
    cur = conn.cursor()
    placeholders = ",".join("?" * len(team_ids))
    cur.execute(f"""
        SELECT p.team_id, p.position, s.minutes_played, s.goals, s.club_ga_per90
        FROM soccer_players p
        JOIN soccer_player_stats s ON s.player_id = p.player_id
        WHERE p.team_id IN ({placeholders}) AND s.season = ?
    """, (*team_ids, season))
    by_team = {tid: [] for tid in team_ids}
    for team_id, position, minutes, goals, club_ga90 in cur.fetchall():
        minutes = minutes or 0
        goals_per90 = (goals / minutes * 90) if (goals is not None and minutes) else None
        by_team[team_id].append({
            "pos": normalize_position(position),
            "minutes": minutes,
            "attack_rate": goals_per90,
            "club_ga_per90": club_ga90,
        })
    return by_team


def positional_priors(by_team, field):
    num, den = {}, {}
    for players in by_team.values():
        for p in players:
            pos, val, mins = p["pos"], p.get(field), p["minutes"]
            if pos and val is not None and mins:
                num[pos] = num.get(pos, 0.0) + mins * val
                den[pos] = den.get(pos, 0.0) + mins
    return {pos: num[pos] / den[pos] for pos in num}


def apply_shrinkage(by_team, k_minutes=K_SHRINK_MINUTES):
    for field in ("attack_rate", "club_ga_per90"):
        prior = positional_priors(by_team, field)
        for players in by_team.values():
            for p in players:
                pos, val, mins = p["pos"], p.get(field), p["minutes"]
                if pos in prior and val is not None and mins:
                    p[field] = (mins * val + k_minutes * prior[pos]) / (mins + k_minutes)


def raw_team_strength(players):
    a_num = a_w = d_num = d_w = 0.0
    for p in players:
        pos = p["pos"]
        if pos is None:
            continue
        if p["attack_rate"] is not None:
            w = p["minutes"] * ATTACK_POS_WEIGHTS.get(pos, 0.0)
            if w > 0:
                a_num += w * p["attack_rate"]
                a_w += w
        if p["club_ga_per90"] is not None:
            w = p["minutes"] * DEFENSE_POS_WEIGHTS.get(pos, 0.0)
            if w > 0:
                d_num += w * p["club_ga_per90"]
                d_w += w
    raw_attack = (a_num / a_w) if a_w > 0 else None
    raw_defense = (d_num / d_w) if d_w > 0 else None
    return raw_attack, a_w, raw_defense, d_w


def team_level_lambda(conn, team_id, league, before_date, n=25):
    """Season-level intrinsic attack/defense for a team from the EXISTING team-level
    system -- average of home_attack/away_attack (and home_defense/away_defense),
    since player-level lambdas here are a single scalar with no home/away split
    (Scenario 4 is out of scope for this prototype)."""
    ratings = get_team_ratings(conn, team_id, before_date, n=n, league=league, decay=1.0)
    attacks = [v for v in (ratings["home_attack"], ratings["away_attack"]) if v is not None]
    defenses = [v for v in (ratings["home_defense"], ratings["away_defense"]) if v is not None]
    attack = mean(attacks) if attacks else None
    defense = mean(defenses) if defenses else None
    return attack, defense


def compute(conn, team_ids, league, season, before_date, w_attack, w_defense):
    by_team = load_team_players(conn, team_ids, season)
    apply_shrinkage(by_team)

    raw = {}
    for tid, players in by_team.items():
        ra, aw, rd, dw = raw_team_strength(players)
        raw[tid] = {"ra": ra, "aw": aw, "rd": rd, "dw": dw}

    avgs = get_league_averages(conn, league=league, seasons=[season])
    baseline = (avgs["avg_home"] + avgs["avg_away"]) / 2

    attack_vals = [r["ra"] for r in raw.values() if r["ra"] is not None and r["aw"] >= MIN_ATTACK_WEIGHT]
    defense_vals = [r["rd"] for r in raw.values() if r["rd"] is not None and r["dw"] >= MIN_DEFENSE_WEIGHT]
    attack_mean = mean(attack_vals) if attack_vals else None
    attack_sd = pstdev(attack_vals) if len(attack_vals) > 1 else 0.0
    defense_scale = (baseline / mean(defense_vals)) if defense_vals else None

    results = {}
    for tid, players in by_team.items():
        r = raw[tid]
        has_attack = r["ra"] is not None and r["aw"] >= MIN_ATTACK_WEIGHT and attack_mean is not None
        has_defense = r["rd"] is not None and r["dw"] >= MIN_DEFENSE_WEIGHT and defense_scale is not None

        # No fixed target spread (unlike WC's ATTACK_LAMBDA_SD) -- the single-league
        # sample here is too small to set one responsibly. Just re-center to baseline
        # and keep the raw sample spread.
        la_player = (baseline + (r["ra"] - attack_mean)) if has_attack else None
        ld_player = (r["rd"] * defense_scale) if has_defense else None

        team_attack, team_defense = team_level_lambda(conn, tid, league, before_date)

        def blend(player_val, team_val, w):
            if player_val is None:
                return team_val, 1.0
            if team_val is None:
                return player_val, 0.0
            return (1 - w) * player_val + w * team_val, w

        la_blend, w_a_used = blend(la_player, team_attack, w_attack)
        ld_blend, w_d_used = blend(ld_player, team_defense, w_defense)

        if w_a_used == 0.0 and w_d_used == 0.0:
            basis = "player"
        elif w_a_used == 1.0 and w_d_used == 1.0:
            basis = "team"
        else:
            basis = f"mix(w_att={w_a_used:g},w_def={w_d_used:g})"

        results[tid] = {
            "lambda_attack_player": la_player, "lambda_defense_player": ld_player,
            "lambda_attack_team": team_attack, "lambda_defense_team": team_defense,
            "lambda_attack_blend": la_blend, "lambda_defense_blend": ld_blend,
            "weight_attack": w_a_used, "weight_defense": w_d_used, "basis": basis,
        }
    return results


def parse_args():
    parser = argparse.ArgumentParser(description="Compute + blend club-league player-based lambdas.")
    parser.add_argument("--league", default="Serie A")
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--team", help="Only compute for this DB team name.")
    parser.add_argument("--limit-teams", type=int, help="Only the first N teams (by team_id).")
    parser.add_argument("--weight-attack", type=float, default=0.5,
                        help="Default blend weight toward team-level attack (0=pure player, 1=pure team).")
    parser.add_argument("--weight-defense", type=float, default=0.5)
    parser.add_argument("--persist", action="store_true", help="Store results in soccer_player_team_strength.")
    return parser.parse_args()


def main():
    args = parse_args()
    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT t.team_id, t.name FROM soccer_teams t
        JOIN soccer_matches m ON m.home_team_id = t.team_id OR m.away_team_id = t.team_id
        WHERE m.league = ? AND m.season = ?
        ORDER BY t.name
    """, (args.league, args.season))
    all_teams = cur.fetchall()
    if args.team:
        all_teams = [(tid, name) for tid, name in all_teams if name == args.team]
    if args.limit_teams:
        all_teams = all_teams[:args.limit_teams]
    team_ids = [tid for tid, _ in all_teams]
    names = dict(all_teams)

    if not team_ids:
        print("No matching teams.")
        return

    before_date = date.today().isoformat()
    results = compute(conn, team_ids, args.league, args.season, before_date,
                      args.weight_attack, args.weight_defense)

    print(f"{'TEAM':<20} {'P-ATT':>7} {'P-DEF':>7}   {'T-ATT':>7} {'T-DEF':>7}   "
          f"{'BLEND-ATT':>9} {'BLEND-DEF':>9}  BASIS")
    for tid in team_ids:
        r = results[tid]
        def fmt(v):
            return f"{v:7.3f}" if v is not None else f"{'--':>7}"
        print(f"{names[tid]:<20} {fmt(r['lambda_attack_player'])} {fmt(r['lambda_defense_player'])}   "
              f"{fmt(r['lambda_attack_team'])} {fmt(r['lambda_defense_team'])}   "
              f"{fmt(r['lambda_attack_blend'])} {fmt(r['lambda_defense_blend'])}  {r['basis']}")

        if args.persist:
            set_player_team_strength(
                tid, args.league,
                lambda_attack_player=r["lambda_attack_player"],
                lambda_defense_player=r["lambda_defense_player"],
                lambda_attack_team=r["lambda_attack_team"],
                lambda_defense_team=r["lambda_defense_team"],
                lambda_attack_blend=r["lambda_attack_blend"],
                lambda_defense_blend=r["lambda_defense_blend"],
                weight_attack=r["weight_attack"], weight_defense=r["weight_defense"],
                basis=r["basis"], notes="prototype v1", conn=conn,
            )
    conn.close()


if __name__ == "__main__":
    main()
