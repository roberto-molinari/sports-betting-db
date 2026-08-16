"""
Diagnose WHY poisson_v4 home +EV bets are miscalibrated vs realized win rate.

Context (2026-08-12 ROI investigation): mean signed bias vs Betfair can look fine
while home sides that clear EV>0 (optional floor) have model_p roughly 11-15pp
above actual win rate. That selection-conditional overconfidence is the main
1X2 ROI drag. This tool makes that failure measurable and attributable.

Two modes:

  1) SLICE REPORT (default, fast, stored predictions only)
     Among home bets (EV>0 vs Bet365, optional live floor), print calibration
     (model_p - wr), ROI, and λ/gap summaries by league, season, month,
     lambda_home-lambda_away, model-soft gap, and model-Betfair gap.

  2) DEEP DIVE (--deep-dive N)
     Take the N worst home bets by model_p - Betfair_fair (fallback: model -
     soft implied), recompute point-in-time player/team/blend λ components the
     same way backfill_player_blend_predictions.py does, and print a component
     table so trends can point at code/data (player vs team, weights, opponent
     defense, etc.).

Usage:
    python3 diagnose_home_bet_calibration.py
    python3 diagnose_home_bet_calibration.py --no-guardrail
    python3 diagnose_home_bet_calibration.py --deep-dive 15
    python3 diagnose_home_bet_calibration.py --league "Bundesliga" --deep-dive 10
"""

from __future__ import annotations

import argparse
import sqlite3
from collections import defaultdict
from statistics import mean

from core.sports_db import DATABASE_PATH
from core.poisson_model import (
    american_to_decimal,
    american_to_implied_prob,
    analyse_match_wc,
    compute_ev,
)
from core.pick_guardrails import guardrail_reasons
from generate_club_league_card import CLUB_LEAGUE_MIN_PICK_PROBABILITY
import compute_club_player_strength as strength

DEFAULT_METHOD = "poisson_v4"
DEFAULT_SPORTSBOOK = "Bet365"
DEFAULT_SHARP = "Betfair Exchange"
DEFAULT_SEASONS = (2024, 2025)

# Driver-pattern thresholds for deep-dive classification (see classify_driver_pattern).
# Pattern A = mostly team form already home-friendly.
# Pattern B = player side pumps home attack (low team weight + player att >> team).
PATTERN_A_MIN_TEAM_WEIGHT = 0.85
PATTERN_B_MAX_TEAM_WEIGHT = 0.40
PATTERN_B_MIN_HOME_ATT_LIFT = 0.15


# ── pure helpers (unit-tested) ─────────────────────────────────────────────────

def home_bet_row(
    *,
    p_home,
    home_ml,
    home_score,
    away_score,
    lambda_home=None,
    lambda_away=None,
    p_home_fair=None,
    floor=None,
):
    """Build one graded home-bet dict, or None if it fails EV/floor screens.

    floor=None means no probability floor (raw multi-side research bar).
    floor=0.25 matches generate_club_league_card / --guardrail metrics.
    """
    if p_home is None or home_ml is None:
        return None
    ev = compute_ev(p_home, home_ml)
    if ev is None or ev <= 0:
        return None
    implied = american_to_implied_prob(home_ml)
    if floor is not None and guardrail_reasons(p_home, implied, floor):
        return None
    won = home_score > away_score
    profit = (american_to_decimal(home_ml) - 1.0) if won else -1.0
    gap_soft = p_home - implied
    gap_bf = (p_home - p_home_fair) if p_home_fair is not None else None
    lh, la = lambda_home, lambda_away
    return {
        "p_home": p_home,
        "implied_soft": implied,
        "p_home_fair": p_home_fair,
        "gap_soft": gap_soft,
        "gap_bf": gap_bf,
        "ev": ev,
        "won": bool(won),
        "profit": profit,
        "lambda_home": lh,
        "lambda_away": la,
        "lambda_diff": (lh - la) if lh is not None and la is not None else None,
        "lambda_total": (lh + la) if lh is not None and la is not None else None,
    }


def calib_error(bets):
    """mean(model_p) - mean(win rate). Positive => overconfident."""
    if not bets:
        return float("nan")
    return mean(b["p_home"] for b in bets) - mean(float(b["won"]) for b in bets)


def roi(bets):
    if not bets:
        return float("nan")
    return mean(b["profit"] for b in bets)


def summarize_bets(bets):
    if not bets:
        return {
            "n": 0, "roi": float("nan"), "wr": float("nan"), "model_p": float("nan"),
            "calib": float("nan"), "gap_soft": float("nan"), "gap_bf": float("nan"),
            "lambda_diff": float("nan"), "lambda_home": float("nan"), "lambda_away": float("nan"),
        }
    with_bf = [b for b in bets if b.get("gap_bf") is not None]
    with_l = [b for b in bets if b.get("lambda_diff") is not None]
    return {
        "n": len(bets),
        "roi": roi(bets),
        "wr": mean(float(b["won"]) for b in bets),
        "model_p": mean(b["p_home"] for b in bets),
        "calib": calib_error(bets),
        "gap_soft": mean(b["gap_soft"] for b in bets),
        "gap_bf": mean(b["gap_bf"] for b in with_bf) if with_bf else float("nan"),
        "lambda_diff": mean(b["lambda_diff"] for b in with_l) if with_l else float("nan"),
        "lambda_home": mean(b["lambda_home"] for b in with_l) if with_l else float("nan"),
        "lambda_away": mean(b["lambda_away"] for b in with_l) if with_l else float("nan"),
    }


def quintile_slices(bets, key, labels=None):
    """Split bets with a non-None key into 5 equal-count quintiles (sorted ascending)."""
    usable = [b for b in bets if b.get(key) is not None]
    usable = sorted(usable, key=lambda b: b[key])
    n = len(usable)
    if n < 5:
        return []
    default_labels = [f"Q{i+1}" for i in range(5)]
    names = labels or default_labels
    out = []
    for i in range(5):
        chunk = usable[i * n // 5:(i + 1) * n // 5]
        out.append((names[i], chunk))
    return out


def gap_bf_bucket(gap_bf):
    if gap_bf is None:
        return "no_sharp"
    if gap_bf <= 0.05:
        return "gap_bf<=0.05"
    if gap_bf <= 0.10:
        return "gap_bf 0.05-0.10"
    if gap_bf <= 0.15:
        return "gap_bf 0.10-0.15"
    return "gap_bf>0.15"


def lambda_diff_bucket(diff):
    if diff is None:
        return "no_lambda"
    if diff < -0.3:
        return "diff<-0.3"
    if diff < 0:
        return "diff -0.3..0"
    if diff < 0.3:
        return "diff 0..0.3"
    if diff < 0.6:
        return "diff 0.3..0.6"
    return "diff>=0.6"


def format_summary_line(label, stats, width=42):
    if stats["n"] == 0:
        return f"  {label:<{width}} n=   0"
    def f(x, fmt="+.3f"):
        if x != x:  # nan
            return "   n/a"
        return format(x, fmt)
    return (
        f"  {label:<{width}} n={stats['n']:4d}  ROI={f(stats['roi'], '+.1%')}  "
        f"wr={f(stats['wr'], '.3f')}  model={f(stats['model_p'], '.3f')}  "
        f"calib={f(stats['calib'], '+.3f')}  gap_soft={f(stats['gap_soft'], '+.3f')}  "
        f"gap_bf={f(stats['gap_bf'], '+.3f')}  lh-la={f(stats['lambda_diff'], '+.2f')}"
    )


# ── DB load ────────────────────────────────────────────────────────────────────

def load_home_bets(conn, method, seasons, sportsbook, sharp_source, floor, league=None):
    placeholders = ",".join("?" * len(seasons))
    params = [sportsbook, sharp_source, method, *seasons]
    league_sql = ""
    if league:
        league_sql = " AND mp.league = ?"
        params.append(league)
    cur = conn.cursor()
    cur.execute(f"""
        SELECT mp.match_id, mp.league, m.season, mp.match_date,
               mp.lambda_home, mp.lambda_away, mp.p_home,
               o.home_moneyline, m.home_score, m.away_score,
               mo.p_home_fair,
               ht.name AS home_name, at.name AS away_name,
               m.home_team_id, m.away_team_id
        FROM soccer_model_predictions mp
        JOIN soccer_matches m ON m.match_id = mp.match_id
        JOIN soccer_betting_odds o
          ON o.match_id = mp.match_id AND o.sportsbook = ?
        JOIN soccer_teams ht ON ht.team_id = m.home_team_id
        JOIN soccer_teams at ON at.team_id = m.away_team_id
        LEFT JOIN soccer_market_odds mo
          ON mo.match_id = mp.match_id
         AND mo.source = ?
         AND mo.line_type = 'closing'
        WHERE mp.method = ?
          AND m.home_score IS NOT NULL
          AND m.season IN ({placeholders})
          {league_sql}
        ORDER BY mp.match_date, mp.match_id
    """, params)
    bets = []
    for row in cur.fetchall():
        (match_id, lg, season, mdate, lh, la, ph, mh, hs, aws, bfh,
         home_name, away_name, hid, aid) = row
        base = home_bet_row(
            p_home=ph, home_ml=mh, home_score=hs, away_score=aws,
            lambda_home=lh, lambda_away=la, p_home_fair=bfh, floor=floor,
        )
        if base is None:
            continue
        base.update({
            "match_id": match_id,
            "league": lg,
            "season": season,
            "match_date": mdate,
            "month": str(mdate)[5:7] if mdate else None,
            "home_name": home_name,
            "away_name": away_name,
            "home_team_id": hid,
            "away_team_id": aid,
            "home_score": hs,
            "away_score": aws,
            "home_ml": mh,
        })
        bets.append(base)
    return bets


def load_team_ids(conn, league, season):
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT t.team_id FROM soccer_teams t
        JOIN soccer_matches m ON m.home_team_id = t.team_id OR m.away_team_id = t.team_id
        WHERE m.league = ? AND m.season = ?
    """, (league, season))
    return [r[0] for r in cur.fetchall()]


# ── reports ────────────────────────────────────────────────────────────────────

def print_slice_report(bets, floor):
    floor_txt = f"floor={floor:g}" if floor is not None else "no floor"
    print("=" * 78)
    print(f"HOME-BET CALIBRATION  ({floor_txt}, EV>0 vs {DEFAULT_SPORTSBOOK})")
    print("=" * 78)
    print(format_summary_line("ALL home bets", summarize_bets(bets)))
    print()
    print("Positive calib = model_p too high vs realized home win rate (overconfident).")
    print("gap_bf = model_p_home - Betfair closing fair p_home.")
    print()

    print("-- BY LEAGUE --")
    for lg in sorted({b["league"] for b in bets}):
        print(format_summary_line(lg, summarize_bets([b for b in bets if b["league"] == lg])))

    print("\n-- BY SEASON --")
    for se in sorted({b["season"] for b in bets}):
        print(format_summary_line(str(se), summarize_bets([b for b in bets if b["season"] == se])))

    print("\n-- BY MONTH --")
    for mo in sorted({b["month"] for b in bets if b["month"]}):
        print(format_summary_line(f"month {mo}", summarize_bets([b for b in bets if b["month"] == mo])))

    print("\n-- BY model - Betfair GAP --")
    order = ["gap_bf<=0.05", "gap_bf 0.05-0.10", "gap_bf 0.10-0.15", "gap_bf>0.15", "no_sharp"]
    by = defaultdict(list)
    for b in bets:
        by[gap_bf_bucket(b.get("gap_bf"))].append(b)
    for name in order:
        if name in by:
            print(format_summary_line(name, summarize_bets(by[name])))

    print("\n-- BY lambda_home - lambda_away --")
    order_l = ["diff<-0.3", "diff -0.3..0", "diff 0..0.3", "diff 0.3..0.6", "diff>=0.6", "no_lambda"]
    by_l = defaultdict(list)
    for b in bets:
        by_l[lambda_diff_bucket(b.get("lambda_diff"))].append(b)
    for name in order_l:
        if name in by_l:
            print(format_summary_line(name, summarize_bets(by_l[name])))

    print("\n-- BY model-soft GAP quintile --")
    for label, chunk in quintile_slices(
        bets, "gap_soft",
        labels=["Q1 smallest disagreement", "Q2", "Q3", "Q4", "Q5 largest model>soft"],
    ):
        print(format_summary_line(label, summarize_bets(chunk)))

    print("\n-- BY lambda_diff quintile --")
    for label, chunk in quintile_slices(
        bets, "lambda_diff",
        labels=["Q1 smallest lh-la", "Q2", "Q3", "Q4", "Q5 largest lh-la"],
    ):
        print(format_summary_line(label, summarize_bets(chunk)))

    # Repeat home teams with enough bets and worst calib
    print("\n-- WORST HOME TEAMS (n>=8 home bets, by calib) --")
    by_team = defaultdict(list)
    for b in bets:
        by_team[b["home_name"]].append(b)
    ranked = []
    for name, rows in by_team.items():
        if len(rows) < 8:
            continue
        s = summarize_bets(rows)
        ranked.append((s["calib"], name, s))
    ranked.sort(reverse=True)
    for _, name, s in ranked[:15]:
        print(format_summary_line(name, s))


def rank_deep_dive_candidates(bets, n):
    """Prefer largest model-Betfair overconfidence; fall back to model-soft gap."""
    def sort_key(b):
        primary = b["gap_bf"] if b.get("gap_bf") is not None else b["gap_soft"]
        return primary

    return sorted(bets, key=sort_key, reverse=True)[:n]


def rank_control_candidates(bets, n):
    """Home bets where model ≈ Betfair (or soft): smallest |gap|, prefer gap_bf."""
    def sort_key(b):
        if b.get("gap_bf") is not None:
            return abs(b["gap_bf"])
        return abs(b["gap_soft"])

    return sorted(bets, key=sort_key)[:n]


def home_att_player_lift(digest):
    """player home attack minus team home attack; None if either missing."""
    p, t = digest.get("home_att_player"), digest.get("home_att_team")
    if p is None or t is None:
        return None
    return p - t


def classify_driver_pattern(digest):
    """Label recomputed deep-dive as Pattern A / B / MIXED.

    A — high team weight: overconfidence likely already in team λs / matchup.
    B — low team weight and player attack clearly above team (player pumps home).
    MIXED — neither clear rule fires (mid weights or no player lift).
    """
    w = digest.get("home_w_att")
    lift = home_att_player_lift(digest)
    if (
        w is not None
        and w <= PATTERN_B_MAX_TEAM_WEIGHT
        and lift is not None
        and lift >= PATTERN_B_MIN_HOME_ATT_LIFT
    ):
        return "B"
    if w is not None and w >= PATTERN_A_MIN_TEAM_WEIGHT:
        return "A"
    return "MIXED"


def pattern_label(code):
    return {
        "A": "A team-form",
        "B": "B player-pump",
        "MIXED": "MIXED",
    }.get(code, code)


def _fmt(v, spec=".3f"):
    if v is None:
        return "  n/a"
    if isinstance(v, float) and v != v:
        return "  n/a"
    return format(v, spec)


def _avg_key(digests, key):
    vals = [d[key] for d in digests if d.get(key) is not None]
    return mean(vals) if vals else float("nan")


def print_cohort_means(title, digests):
    """Print aggregate component means for a list of deep-dive digests."""
    if not digests:
        print(f"\n-- {title} --\n  (empty)")
        return
    print(f"\n-- {title} --")
    with_bf = [d for d in digests if d["bet"].get("gap_bf") is not None]
    gap_bf = mean(d["bet"]["gap_bf"] for d in with_bf) if with_bf else float("nan")
    print(
        f"  n={len(digests)}  mean stored gap_bf={_fmt(gap_bf, '+.3f')}  "
        f"wr={mean(float(d['bet']['won']) for d in digests):.3f}  "
        f"mean stored p={mean(d['bet']['p_home'] for d in digests):.3f}"
    )
    print(
        f"  home w_att={_avg_key(digests, 'home_w_att'):.3f}  "
        f"home w_def={_avg_key(digests, 'home_w_def'):.3f}  "
        f"away w_att={_avg_key(digests, 'away_w_att'):.3f}  "
        f"away w_def={_avg_key(digests, 'away_w_def'):.3f}"
    )
    print(
        f"  home att player/team/blend "
        f"{_avg_key(digests, 'home_att_player'):.3f}/"
        f"{_avg_key(digests, 'home_att_team'):.3f}/"
        f"{_avg_key(digests, 'home_att_blend'):.3f}"
    )
    print(
        f"  away att player/team/blend "
        f"{_avg_key(digests, 'away_att_player'):.3f}/"
        f"{_avg_key(digests, 'away_att_team'):.3f}/"
        f"{_avg_key(digests, 'away_att_blend'):.3f}"
    )
    print(
        f"  away def player/team/blend "
        f"{_avg_key(digests, 'away_def_player'):.3f}/"
        f"{_avg_key(digests, 'away_def_team'):.3f}/"
        f"{_avg_key(digests, 'away_def_blend'):.3f}"
    )
    ha_vals = [
        d["home_att_player"] - d["home_att_team"]
        for d in digests
        if d.get("home_att_player") is not None and d.get("home_att_team") is not None
    ]
    ad_vals = [
        d["away_def_player"] - d["away_def_team"]
        for d in digests
        if d.get("away_def_player") is not None and d.get("away_def_team") is not None
    ]
    ha_lift = mean(ha_vals) if ha_vals else float("nan")
    ad_lift = mean(ad_vals) if ad_vals else float("nan")
    print(
        f"  mean (player-team) home_att={_fmt(ha_lift, '+.3f')}  "
        f"away_def={_fmt(ad_lift, '+.3f')}"
    )
    by_pat = defaultdict(int)
    for d in digests:
        by_pat[classify_driver_pattern(d)] += 1
    parts = [f"{pattern_label(k)}={by_pat[k]}" for k in ("A", "B", "MIXED") if by_pat[k]]
    if parts:
        print(f"  patterns: {', '.join(parts)}")


def deep_dive_one(conn, bet, cache):
    """Recompute point-in-time λ stack for one match; return component dict."""
    league = bet["league"]
    season = bet["season"]
    match_date = bet["match_date"]
    team_ids = load_team_ids(conn, league, season)
    roster_ids_by_team = {
        tid: strength.roster_as_of_date(conn, tid, season, match_date)
        for tid in team_ids
    }
    results = strength.compute(
        conn, team_ids, league, season, match_date,
        current_roster_ids_by_team=roster_ids_by_team,
        cache=cache,
    )
    home = results[bet["home_team_id"]]
    away = results[bet["away_team_id"]]
    avg_home, avg_away = home["avg_home"], home["avg_away"]
    priced = analyse_match_wc(
        lambda_home_attack=home["lambda_attack_home_blend"],
        lambda_away_attack=away["lambda_attack_away_blend"],
        lambda_home_defense=home["lambda_defense_home_blend"],
        lambda_away_defense=away["lambda_defense_away_blend"],
        home_moneyline=bet["home_ml"],
        draw_moneyline=None,
        away_moneyline=None,
        baseline=avg_home,
        home_advantage=1.0,
        away_advantage=avg_home / avg_away,
    )
    # Expected goals decomposition used by analyse_match_wc:
    #   λ_H = home_att * (away_def / baseline) * home_adv
    #   λ_A = away_att * (home_def / baseline) * away_adv
    baseline = avg_home
    away_adv = avg_home / avg_away if avg_away else 1.0
    return {
        "bet": bet,
        "recomputed_p_home": priced["p_home"],
        "recomputed_lambda_H": priced["lambda_H"],
        "recomputed_lambda_A": priced["lambda_A"],
        "home_att_player": home["lambda_attack_player_home"],
        "home_att_team": home["lambda_attack_team_home"],
        "home_att_blend": home["lambda_attack_home_blend"],
        "home_def_player": home["lambda_defense_player_home"],
        "home_def_team": home["lambda_defense_team_home"],
        "home_def_blend": home["lambda_defense_home_blend"],
        "home_w_att": home["weight_attack"],
        "home_w_def": home["weight_defense"],
        "home_basis": home["basis"],
        "away_att_player": away["lambda_attack_player_away"],
        "away_att_team": away["lambda_attack_team_away"],
        "away_att_blend": away["lambda_attack_away_blend"],
        "away_def_player": away["lambda_defense_player_away"],
        "away_def_team": away["lambda_defense_team_away"],
        "away_def_blend": away["lambda_defense_away_blend"],
        "away_w_att": away["weight_attack"],
        "away_w_def": away["weight_defense"],
        "away_basis": away["basis"],
        "avg_home": avg_home,
        "avg_away": avg_away,
        "matchup_home_goal_factor": (
            home["lambda_attack_home_blend"] * (away["lambda_defense_away_blend"] / baseline)
            if home["lambda_attack_home_blend"] is not None
            and away["lambda_defense_away_blend"] is not None and baseline
            else None
        ),
        "matchup_away_goal_factor": (
            away["lambda_attack_away_blend"]
            * (home["lambda_defense_home_blend"] / baseline)
            * away_adv
            if away["lambda_attack_away_blend"] is not None
            and home["lambda_defense_home_blend"] is not None and baseline
            else None
        ),
    }


def _run_deep_dive_batch(conn, cands, cache, heading):
    """Recompute and print one batch of candidates; return digests list."""
    print()
    print("=" * 78)
    print(heading)
    print("Point-in-time recompute via compute() + roster_as_of_date (backfill-identical).")
    print(
        "Pattern A = high team weight (team form already home-friendly).  "
        "Pattern B = low team weight + player attack >> team."
    )
    print("=" * 78)

    digests = []
    for i, bet in enumerate(cands, 1):
        print(
            f"\n--- #{i}  {bet['match_date'][:10]}  {bet['league']}  "
            f"{bet['home_name']} vs {bet['away_name']}  "
            f"score {bet['home_score']}-{bet['away_score']}  "
            f"{'WON' if bet['won'] else 'LOST'} ---"
        )
        print(
            f"  stored: p_home={bet['p_home']:.3f}  soft={bet['implied_soft']:.3f}  "
            f"bfair={_fmt(bet['p_home_fair'])}  gap_soft={bet['gap_soft']:+.3f}  "
            f"gap_bf={_fmt(bet['gap_bf'], '+.3f')}  ev={bet['ev']:+.2f}  "
            f"stored_lh/la={_fmt(bet['lambda_home'])}/{_fmt(bet['lambda_away'])}"
        )
        try:
            d = deep_dive_one(conn, bet, cache)
        except Exception as exc:
            print(f"  RECOMPUTE FAILED: {exc}")
            continue
        digests.append(d)
        pat = classify_driver_pattern(d)
        lift = home_att_player_lift(d)
        dp = d["recomputed_p_home"]
        drift = (dp - bet["p_home"]) if dp is not None else None
        print(
            f"  pattern={pattern_label(pat)}  "
            f"home_w_att={_fmt(d['home_w_att'], '.3f')}  "
            f"home_att player-team={_fmt(lift, '+.3f')}"
        )
        print(
            f"  recomputed p_home={_fmt(dp)}  (vs stored Δ={_fmt(drift, '+.3f')})  "
            f"λ_H/λ_A={_fmt(d['recomputed_lambda_H'])}/{_fmt(d['recomputed_lambda_A'])}"
        )
        print(
            f"  HOME att  player/team/blend={_fmt(d['home_att_player'])}/"
            f"{_fmt(d['home_att_team'])}/{_fmt(d['home_att_blend'])}  "
            f"w_att={_fmt(d['home_w_att'], '.3f')}  basis={d['home_basis']}"
        )
        print(
            f"  HOME def  player/team/blend={_fmt(d['home_def_player'])}/"
            f"{_fmt(d['home_def_team'])}/{_fmt(d['home_def_blend'])}  "
            f"w_def={_fmt(d['home_w_def'], '.3f')}"
        )
        print(
            f"  AWAY att  player/team/blend={_fmt(d['away_att_player'])}/"
            f"{_fmt(d['away_att_team'])}/{_fmt(d['away_att_blend'])}  "
            f"w_att={_fmt(d['away_w_att'], '.3f')}  basis={d['away_basis']}"
        )
        print(
            f"  AWAY def  player/team/blend={_fmt(d['away_def_player'])}/"
            f"{_fmt(d['away_def_team'])}/{_fmt(d['away_def_blend'])}  "
            f"w_def={_fmt(d['away_w_def'], '.3f')}"
        )
        ha_p, ha_t = d["home_att_player"], d["home_att_team"]
        ad_p, ad_t = d["away_def_player"], d["away_def_team"]
        print(
            f"  matchup drivers: home_att player-team "
            f"{_fmt((ha_p - ha_t) if ha_p is not None and ha_t is not None else None, '+.3f')}; "
            f"away_def player-team "
            f"{_fmt((ad_p - ad_t) if ad_p is not None and ad_t is not None else None, '+.3f')} "
            f"(higher away_def = leakier defense / more home goals)"
        )
    return digests


def print_deep_dive(conn, bets, n, control_n=0):
    """Worst gap_bf tails + optional low-gap controls; pattern A/B split."""
    cache = {}
    bad = rank_deep_dive_candidates(bets, n)
    digests = _run_deep_dive_batch(
        conn, bad, cache,
        heading=(
            f"DEEP DIVE  top {len(bad)} home bets by model-Betfair gap "
            f"(else model-soft) — OVERCONFIDENT TAIL"
        ),
    )

    if digests:
        print_cohort_means("OVERCONFIDENT TAIL — ALL", digests)
        by_pat = defaultdict(list)
        for d in digests:
            by_pat[classify_driver_pattern(d)].append(d)
        for code in ("A", "B", "MIXED"):
            if by_pat[code]:
                print_cohort_means(
                    f"OVERCONFIDENT TAIL — Pattern {pattern_label(code)} only",
                    by_pat[code],
                )
        print(
            "\n  Plain English: Pattern A = model mostly trusts team form "
            "(home solid / away weak already in team λs). "
            "Pattern B = model leans on players and those players make home "
            "attack look much stronger than team form. "
            "MIXED = mid weights or no clear player attack lift."
        )

    if control_n and control_n > 0:
        # Avoid reusing the same matches already in the bad tail when possible.
        bad_ids = {b["match_id"] for b in bad}
        pool = [b for b in bets if b["match_id"] not in bad_ids] or bets
        controls = rank_control_candidates(pool, control_n)
        ctrl_digests = _run_deep_dive_batch(
            conn, controls, cache,
            heading=(
                f"CONTROL COHORT  {len(controls)} home bets with smallest "
                f"|model-Betfair| gap (else |model-soft|) — model ≈ market"
            ),
        )
        if ctrl_digests:
            print_cohort_means("CONTROL (model≈market) — ALL", ctrl_digests)
            print(
                "\n  Compare controls vs overconfident tail: do controls lack "
                "extreme home_w≈1 + crushed away attack / elite home def? "
                "If yes, those shapes are candidates for a real fix target."
            )


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--method", default=DEFAULT_METHOD)
    parser.add_argument("--season", dest="seasons", type=int, action="append",
                        help="Repeatable. Default: 2024 and 2025.")
    parser.add_argument("--league", default=None,
                        help="Optional single-league filter.")
    parser.add_argument("--sportsbook", default=DEFAULT_SPORTSBOOK)
    parser.add_argument("--sharp-source", default=DEFAULT_SHARP)
    parser.add_argument("--guardrail", dest="guardrail", action="store_true", default=True,
                        help="Apply CLUB_LEAGUE_MIN_PICK_PROBABILITY floor (default: on).")
    parser.add_argument("--no-guardrail", dest="guardrail", action="store_false",
                        help="Raw EV>0 home bets, no probability floor.")
    parser.add_argument("--deep-dive", type=int, default=0, metavar="N",
                        help="Recompute λ components for top-N overconfident home bets.")
    parser.add_argument(
        "--control", type=int, default=None, metavar="N",
        help="With --deep-dive: also recompute N low-gap control bets "
             "(default: same N as --deep-dive). Use 0 to skip controls.",
    )
    args = parser.parse_args()
    seasons = tuple(args.seasons) if args.seasons else DEFAULT_SEASONS
    floor = CLUB_LEAGUE_MIN_PICK_PROBABILITY if args.guardrail else None

    conn = sqlite3.connect(DATABASE_PATH)
    bets = load_home_bets(
        conn, args.method, seasons, args.sportsbook, args.sharp_source, floor,
        league=args.league,
    )
    if not bets:
        print("No home bets matched filters.")
        conn.close()
        return

    print(f"method={args.method}  seasons={list(seasons)}  "
          f"league={args.league or 'ALL'}  sportsbook={args.sportsbook}  "
          f"sharp={args.sharp_source}")
    print_slice_report(bets, floor)

    if args.deep_dive and args.deep_dive > 0:
        control_n = args.deep_dive if args.control is None else args.control
        print_deep_dive(conn, bets, args.deep_dive, control_n=control_n)

    conn.close()


if __name__ == "__main__":
    main()
