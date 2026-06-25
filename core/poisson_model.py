"""
Poisson-Based 1X2 Soccer Betting Model
=======================================
Estimates expected goals (lambda) for each team in a matchup using recent
historical results, then uses a Poisson scoreline grid to derive:
  - P(home win), P(draw), P(away win)
  - EV for home moneyline and away moneyline

Formula overview
----------------
1. Compute team attack/defense ratings from recent N matches.
2. Scale by league averages to get lambda_H and lambda_A.
3. Build a (MAX_GOALS+1) x (MAX_GOALS+1) Poisson probability grid.
4. Sum grid cells to get P(H), P(D), P(A).
5. Convert American moneyline odds to implied probability and decimal odds.
6. EV = p_model * decimal_odds - 1

All constants are at the top of the file so they are easy to tune.
"""

import math
import sqlite3
from core.sports_db import DATABASE_PATH

# ---------------------------------------------------------------------------
# Tunable constants
# ---------------------------------------------------------------------------

# Number of recent completed matches to use per team for attack/defense ratings.
# Only home matches are used for home_attack/home_defense; only away for away.
RECENT_N = 10

# Scoreline grid cap.  Cells beyond this are dropped (tail probability is tiny).
MAX_GOALS = 6

# Minimum matches required to compute a rating; fall back to league average if fewer.
MIN_MATCHES = 3

# Recency decay factor.  Each game further back in time is multiplied by this weight.
# e.g. 0.85 means the most recent game counts 1.0, the one before it 0.85,
# the one before that 0.85^2 = 0.72, etc.
# Set to 1.0 to disable (plain average).
RECENCY_DECAY = 1.0

# Shrinkage constant.  Blends each team rating toward the league average.
# Higher k = more shrinkage toward league avg, less trust in team's own data.
# rating_adj = (n * rating_team + k * league_avg) / (n + k)
# Set to 0 to disable shrinkage.
SHRINKAGE_K = 0

# ── World Cup constants ──────────────────────────────────────────────────────
# Baseline goals per team per international match.  Team strength lambdas are
# normalized around this, and it scales the attack/defense combination in
# analyse_match_wc (parallel to the league average used for Serie A).
WC_BASELINE = 1.35

# Larger scoreline grid for the World Cup model.  WC totals lines run higher
# than Serie A's (3.5 / 4.5 in lopsided games), so MAX_GOALS=6 would truncate
# the over tail; 10 keeps it.
WC_MAX_GOALS = 10

# EV-gap thresholds mapping a pick's EV to a 1-3 star confidence rating.
# ev >= 0.15 -> 3 stars, ev >= 0.075 -> 2 stars, otherwise 1 star.
# 1 star is the catch-all floor (incl. sub-threshold and negative EV): the
# requirements forbid abstention, so every match still gets a rated pick.
STAR_THRESHOLDS = (0.075, 0.15)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def american_to_decimal(american: float) -> float:
    """Convert American moneyline to decimal odds."""
    if american >= 0:
        return 1 + american / 100
    else:
        return 1 + 100 / abs(american)


def american_to_implied_prob(american: float) -> float:
    """
    Convert American moneyline to raw (vig-inclusive) implied probability.
    Note: the vig means home_implied + away_implied > 1.0 — this is intentional.
    We use raw implied prob only for reference; EV uses decimal odds (which
    already encode the vig on the payout side).
    """
    if american >= 0:
        return 100 / (american + 100)
    else:
        return abs(american) / (abs(american) + 100)


def poisson_pmf(k: int, lam: float) -> float:
    """P(X = k) for Poisson(lambda)."""
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return math.exp(-lam) * (lam ** k) / math.factorial(k)


# ---------------------------------------------------------------------------
# Data layer
# ---------------------------------------------------------------------------

def get_league_averages(conn, league: str = "Serie A", seasons: list = None) -> dict:
    """
    Return league-wide average goals per match (home and away separately).
    Used as the scaling baseline for attack/defense ratings.
    """
    cur = conn.cursor()
    if seasons:
        placeholders = ",".join("?" * len(seasons))
        cur.execute(f"""
            SELECT AVG(home_score), AVG(away_score)
            FROM soccer_matches
            WHERE league = ? AND home_score IS NOT NULL
              AND season IN ({placeholders})
        """, [league] + list(seasons))
    else:
        cur.execute("""
            SELECT AVG(home_score), AVG(away_score)
            FROM soccer_matches
            WHERE league = ? AND home_score IS NOT NULL
        """, (league,))
    row = cur.fetchone()
    avg_home = row[0] or 1.3
    avg_away = row[1] or 1.1
    return {"avg_home": avg_home, "avg_away": avg_away}


def get_team_ratings(conn, team_id: int, before_date: str,
                     n: int = RECENT_N, league: str = "Serie A",
                     decay: float = RECENCY_DECAY) -> dict:
    """
    Compute attack and defense ratings for a team from their last N completed
    home matches (for home ratings) and last N away matches (for away ratings),
    all played strictly before `before_date`.

    Games are weighted by recency: the most recent game has weight 1.0,
    the next has weight `decay`, then `decay^2`, etc.
    Set decay=1.0 for a plain unweighted average.

    Returns:
        home_attack  : decay-weighted avg goals scored in home games
        home_defense : decay-weighted avg goals conceded in home games
        away_attack  : decay-weighted avg goals scored in away games
        away_defense : decay-weighted avg goals conceded in away games
        home_n       : number of home matches used
        away_n       : number of away matches used
    """
    cur = conn.cursor()

    # Home matches — most recent first
    cur.execute("""
        SELECT home_score, away_score
        FROM soccer_matches
        WHERE home_team_id = ? AND league = ?
          AND home_score IS NOT NULL
          AND match_date < ?
        ORDER BY match_date DESC
        LIMIT ?
    """, (team_id, league, before_date, n))
    home_rows = cur.fetchall()

    # Away matches — most recent first
    cur.execute("""
        SELECT home_score, away_score
        FROM soccer_matches
        WHERE away_team_id = ? AND league = ?
          AND home_score IS NOT NULL
          AND match_date < ?
        ORDER BY match_date DESC
        LIMIT ?
    """, (team_id, league, before_date, n))
    away_rows = cur.fetchall()

    def weighted_avg(vals):
        """Compute decay-weighted average; index 0 is most recent."""
        if not vals:
            return None
        total_weight = 0.0
        total_value  = 0.0
        for k, v in enumerate(vals):
            w = decay ** k
            total_weight += w
            total_value  += w * v
        return total_value / total_weight

    home_scored   = [r[0] for r in home_rows]
    home_conceded = [r[1] for r in home_rows]
    away_scored   = [r[1] for r in away_rows]
    away_conceded = [r[0] for r in away_rows]

    return {
        "home_attack":  weighted_avg(home_scored),
        "home_defense": weighted_avg(home_conceded),
        "away_attack":  weighted_avg(away_scored),
        "away_defense": weighted_avg(away_conceded),
        "home_n":       len(home_rows),
        "away_n":       len(away_rows),
    }


# ---------------------------------------------------------------------------
# Core model
# ---------------------------------------------------------------------------

def _shrink(rating: float, league_avg: float, n: int, k: float = SHRINKAGE_K) -> float:
    """
    Bayesian shrinkage: blend team rating toward league average.
    With k=5: after 5 games the weight is 50/50; after 10 games it's 67% team / 33% avg.
    """
    return (n * rating + k * league_avg) / (n + k)


def estimate_lambdas(home_ratings: dict, away_ratings: dict,
                     league_avgs: dict, min_matches: int = MIN_MATCHES,
                     shrinkage_k: float = SHRINKAGE_K) -> tuple[float, float]:
    """
    Estimate expected goals (lambda) for each team.

    lambda_H = shrink(home_attack, avg_h) * shrink(away_defense, avg_h) / avg_h
    lambda_A = shrink(away_attack, avg_a) * shrink(home_defense, avg_a) / avg_a

    Shrinkage pulls extreme ratings toward the league average, reducing
    overconfidence when sample sizes are small.
    """
    avg_h = league_avgs["avg_home"]
    avg_a = league_avgs["avg_away"]

    # Home team attack (how many they score at home)
    if home_ratings["home_n"] >= min_matches and home_ratings["home_attack"] is not None:
        h_att = _shrink(home_ratings["home_attack"], avg_h, home_ratings["home_n"], shrinkage_k)
    else:
        h_att = avg_h

    # Away team defense (how many they concede away, measured in home-team-scored units)
    if away_ratings["away_n"] >= min_matches and away_ratings["away_defense"] is not None:
        a_def = _shrink(away_ratings["away_defense"], avg_h, away_ratings["away_n"], shrinkage_k)
    else:
        a_def = avg_h

    # Away team attack (how many they score away)
    if away_ratings["away_n"] >= min_matches and away_ratings["away_attack"] is not None:
        a_att = _shrink(away_ratings["away_attack"], avg_a, away_ratings["away_n"], shrinkage_k)
    else:
        a_att = avg_a

    # Home team defense (how many they concede at home, measured in away-team-scored units)
    if home_ratings["home_n"] >= min_matches and home_ratings["home_defense"] is not None:
        h_def = _shrink(home_ratings["home_defense"], avg_a, home_ratings["home_n"], shrinkage_k)
    else:
        h_def = avg_a

    lambda_H = h_att * (a_def / avg_h)
    lambda_A = a_att * (h_def / avg_a)

    # Sanity floor
    lambda_H = max(lambda_H, 0.1)
    lambda_A = max(lambda_A, 0.1)

    return lambda_H, lambda_A


def scoreline_grid(lambda_H: float, lambda_A: float,
                   max_goals: int = MAX_GOALS) -> list[list[float]]:
    """
    Build (max_goals+1) x (max_goals+1) grid of scoreline probabilities.
    grid[i][j] = P(home scores i, away scores j)
    """
    grid = []
    for i in range(max_goals + 1):
        row = []
        ph = poisson_pmf(i, lambda_H)
        for j in range(max_goals + 1):
            pa = poisson_pmf(j, lambda_A)
            row.append(ph * pa)
        grid.append(row)
    return grid


def outcome_probs(grid: list[list[float]]) -> dict:
    """
    Sum the scoreline grid into P(home win), P(draw), P(away win).
    Also compute P(over 2.5 goals) while we are here.
    """
    p_home = p_draw = p_away = p_over25 = 0.0
    for i, row in enumerate(grid):
        for j, prob in enumerate(row):
            if i > j:
                p_home += prob
            elif i == j:
                p_draw += prob
            else:
                p_away += prob
            if i + j > 2:
                p_over25 += prob
    return {
        "p_home":    p_home,
        "p_draw":    p_draw,
        "p_away":    p_away,
        "p_over25":  p_over25,
        "p_under25": 1 - p_over25,
    }


def compute_ev(p_model: float, american_odds: float) -> float:
    """
    EV per $1 bet = p_model * decimal_odds - 1
    Positive EV = value bet.
    """
    d = american_to_decimal(american_odds)
    return p_model * d - 1


def totals_probs(grid: list[list[float]], line: float) -> dict:
    """
    Split the scoreline grid's total-goals distribution around an arbitrary
    over/under line (1.5, 2.0, 2.5, 3.5, ...).

    For half-goal lines p_push is always 0.  For integer lines (2.0, 3.0) the
    mass landing exactly on the line is a push.
    """
    p_over = p_under = p_push = 0.0
    for i, row in enumerate(grid):
        for j, prob in enumerate(row):
            total = i + j
            if total > line:
                p_over += prob
            elif total < line:
                p_under += prob
            else:
                p_push += prob
    return {"p_over": p_over, "p_under": p_under, "p_push": p_push}


def compute_ev_totals(p_win: float, p_lose: float, american_odds: float) -> float:
    """
    Push-aware EV per $1 bet on a totals (over/under) market.

    EV = p_win * (decimal - 1) - p_lose * 1   (a push returns the stake -> 0).
    For half-goal lines p_lose == 1 - p_win and this reduces to compute_ev.
    """
    d = american_to_decimal(american_odds)
    return p_win * (d - 1) - p_lose


def ev_to_stars(ev: float, thresholds: tuple = STAR_THRESHOLDS) -> int:
    """
    Map a pick's EV to a 1-3 star confidence rating.

    ev >= thresholds[1] -> 3, ev >= thresholds[0] -> 2, otherwise 1.
    1 star is the floor (no abstention): even a low- or negative-EV best pick
    is still rated and posted.
    """
    low, high = thresholds
    if ev >= high:
        return 3
    if ev >= low:
        return 2
    return 1


# ---------------------------------------------------------------------------
# High-level entry point
# ---------------------------------------------------------------------------

def analyse_match(home_team_id: int, away_team_id: int,
                  match_date: str,
                  home_moneyline: float = None,
                  draw_moneyline: float = None,
                  away_moneyline: float = None,
                  league: str = "Serie A",
                  conn=None) -> dict:
    """
    Full model pipeline for a single match.

    Parameters
    ----------
    home_team_id, away_team_id : DB team IDs
    match_date    : ISO date string — ratings are computed from matches *before* this date
    home_moneyline, draw_moneyline, away_moneyline : American odds
                   (optional — EV skipped if None)
    league        : league filter for ratings and averages
    conn          : optional existing DB connection (created internally if None)

    Returns a dict with lambdas, probabilities, implied probs, EVs, and team ratings.
    """
    own_conn = conn is None
    if own_conn:
        conn = sqlite3.connect(DATABASE_PATH)

    try:
        league_avgs  = get_league_averages(conn, league)
        home_ratings = get_team_ratings(conn, home_team_id, match_date, league=league)
        away_ratings = get_team_ratings(conn, away_team_id, match_date, league=league)

        lambda_H, lambda_A = estimate_lambdas(home_ratings, away_ratings, league_avgs)
        grid  = scoreline_grid(lambda_H, lambda_A)
        probs = outcome_probs(grid)

        result = {
            "lambda_H":     lambda_H,
            "lambda_A":     lambda_A,
            "p_home":       probs["p_home"],
            "p_draw":       probs["p_draw"],
            "p_away":       probs["p_away"],
            "p_over25":     probs["p_over25"],
            "home_ratings": home_ratings,
            "away_ratings": away_ratings,
            "league_avgs":  league_avgs,
        }

        if home_moneyline is not None:
            result["ev_home"]           = compute_ev(probs["p_home"], home_moneyline)
            result["implied_home"]      = american_to_implied_prob(home_moneyline)
            result["decimal_home"]      = american_to_decimal(home_moneyline)

        if draw_moneyline is not None:
            result["ev_draw"]           = compute_ev(probs["p_draw"], draw_moneyline)
            result["implied_draw"]      = american_to_implied_prob(draw_moneyline)
            result["decimal_draw"]      = american_to_decimal(draw_moneyline)

        if away_moneyline is not None:
            result["ev_away"]           = compute_ev(probs["p_away"], away_moneyline)
            result["implied_away"]      = american_to_implied_prob(away_moneyline)
            result["decimal_away"]      = american_to_decimal(away_moneyline)

        return result

    finally:
        if own_conn:
            conn.close()


def analyse_match_wc(lambda_home_attack: float, lambda_away_attack: float,
                     lambda_home_defense: float, lambda_away_defense: float,
                     home_moneyline: float = None,
                     draw_moneyline: float = None,
                     away_moneyline: float = None,
                     ou_line: float = None,
                     over_odds: float = None,
                     under_odds: float = None,
                     baseline: float = WC_BASELINE,
                     home_advantage: float = 1.0,
                     away_advantage: float = 1.0) -> dict:
    """
    World Cup entry point — parallel to analyse_match() but accepts pre-computed
    team strength lambdas directly instead of deriving them from match history.

    The match-level expected goals are formed exactly like estimate_lambdas():
    a team's attack is modulated by the opponent's defense, scaled by the
    international baseline.

        lambda_H = home_attack * (away_defense / baseline) * home_advantage
        lambda_A = away_attack * (home_defense / baseline) * away_advantage

    Parameters
    ----------
    lambda_home_attack, lambda_away_attack   : per-team attack strength (goals)
    lambda_home_defense, lambda_away_defense : per-team defense strength
                                               (goals conceded; lower = stronger)
    home_moneyline, draw_moneyline, away_moneyline : American 1X2 odds (optional)
    ou_line, over_odds, under_odds : posted totals line and its odds (optional).
                                     The model evaluates over/under at THIS line.
    baseline       : international goals-per-team baseline (default WC_BASELINE)
    home_advantage : multiplier on lambda_H; 1.0 = neutral venue (default)
    away_advantage : multiplier on lambda_A; 1.0 = neutral (default). Set >1.0
                     when the nominal away team has the real venue edge — e.g. a
                     host nation listed away but still playing in its own country.

    Returns the same shape as analyse_match (lambdas, 1X2 probs, optional EVs)
    plus, when ou_line is given, totals fields for the posted line.
    """
    lambda_H = lambda_home_attack * (lambda_away_defense / baseline) * home_advantage
    lambda_A = lambda_away_attack * (lambda_home_defense / baseline) * away_advantage
    lambda_H = max(lambda_H, 0.1)
    lambda_A = max(lambda_A, 0.1)

    grid  = scoreline_grid(lambda_H, lambda_A, max_goals=WC_MAX_GOALS)
    probs = outcome_probs(grid)

    result = {
        "lambda_H":  lambda_H,
        "lambda_A":  lambda_A,
        "p_home":    probs["p_home"],
        "p_draw":    probs["p_draw"],
        "p_away":    probs["p_away"],
    }

    if home_moneyline is not None:
        result["ev_home"]      = compute_ev(probs["p_home"], home_moneyline)
        result["implied_home"] = american_to_implied_prob(home_moneyline)
        result["decimal_home"] = american_to_decimal(home_moneyline)

    if draw_moneyline is not None:
        result["ev_draw"]      = compute_ev(probs["p_draw"], draw_moneyline)
        result["implied_draw"] = american_to_implied_prob(draw_moneyline)
        result["decimal_draw"] = american_to_decimal(draw_moneyline)

    if away_moneyline is not None:
        result["ev_away"]      = compute_ev(probs["p_away"], away_moneyline)
        result["implied_away"] = american_to_implied_prob(away_moneyline)
        result["decimal_away"] = american_to_decimal(away_moneyline)

    if ou_line is not None:
        totals = totals_probs(grid, ou_line)
        result["ou_line"] = ou_line
        result["p_over"]  = totals["p_over"]
        result["p_under"] = totals["p_under"]
        result["p_push"]  = totals["p_push"]
        if over_odds is not None:
            result["ev_over"] = compute_ev_totals(
                totals["p_over"], totals["p_under"], over_odds)
        if under_odds is not None:
            result["ev_under"] = compute_ev_totals(
                totals["p_under"], totals["p_over"], under_odds)

    return result


# ---------------------------------------------------------------------------
# Knockout "to advance" market — regulation -> extra time -> penalty shootout
# ---------------------------------------------------------------------------

EXTRA_TIME_LAMBDA_FRACTION = 1 / 3   # extra time is 30 of 90 minutes
EXTRA_TIME_BENCH_WEIGHT = 0.10       # a stronger bench lifts extra-time scoring
SHOOTOUT_BENCH_WEIGHT = 0.10         # bench depth tilts the shootout win probability
SHOOTOUT_FAVORITE_WEIGHT = 0.05      # slight favorite edge from the 90' win-prob gap
SHOOTOUT_PROB_BOUNDS = (0.40, 0.60)  # a shootout stays near coin-flip


def _clamp(value: float, low: float, high: float) -> float:
    return low if value < low else high if value > high else value


def advance_probs(lambda_home: float, lambda_away: float,
                  bench_index_home: float = 0.0, bench_index_away: float = 0.0,
                  max_goals: int = WC_MAX_GOALS) -> dict:
    """
    Probability each side advances a knockout tie: 90' regulation, else extra time,
    else a penalty shootout.

        P(home advances) = P(home wins 90')
                         + P(draw 90') * [ P(home wins ET) + P(draw ET) * P(home wins SO) ]

    lambda_home/lambda_away are the 90' match expected goals (the lambda_H/lambda_A that
    analyse_match_wc derives). bench_index_* are field-centered bench-strength indices
    (default 0.0 -> no bench nudge, the Step-3 behaviour); when supplied they raise that
    team's extra-time scoring rate and tilt the shootout its way. Returns
    {"p_home_advance", "p_away_advance"} summing to 1.
    """
    reg = outcome_probs(scoreline_grid(lambda_home, lambda_away, max_goals=max_goals))
    p_home_90, p_draw_90, p_away_90 = reg["p_home"], reg["p_draw"], reg["p_away"]

    # Extra time: 1/3 of a match, each team's rate nudged by its bench depth.
    et_home_mult = 1 + EXTRA_TIME_BENCH_WEIGHT * bench_index_home
    et_away_mult = 1 + EXTRA_TIME_BENCH_WEIGHT * bench_index_away
    lambda_et_home = lambda_home * EXTRA_TIME_LAMBDA_FRACTION * et_home_mult
    lambda_et_away = lambda_away * EXTRA_TIME_LAMBDA_FRACTION * et_away_mult
    et = outcome_probs(scoreline_grid(lambda_et_home, lambda_et_away, max_goals=max_goals))
    p_home_et, p_draw_et = et["p_home"], et["p_draw"]

    # Penalty shootout: near coin-flip, tilted by bench depth and a slight favorite edge.
    delta_bench = bench_index_home - bench_index_away
    delta_strength = p_home_90 - p_away_90
    p_home_shootout = _clamp(
        0.5 + SHOOTOUT_BENCH_WEIGHT * delta_bench + SHOOTOUT_FAVORITE_WEIGHT * delta_strength,
        *SHOOTOUT_PROB_BOUNDS)

    p_home_advance = p_home_90 + p_draw_90 * (p_home_et + p_draw_et * p_home_shootout)
    return {"p_home_advance": p_home_advance, "p_away_advance": 1 - p_home_advance}


# ---------------------------------------------------------------------------
# CLI demo
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    conn = sqlite3.connect(DATABASE_PATH)
    cur = conn.cursor()

    # Default: Torino (home) vs Sassuolo (away) — upcoming May 10 match
    home_name = sys.argv[1] if len(sys.argv) > 1 else "Torino"
    away_name = sys.argv[2] if len(sys.argv) > 2 else "Sassuolo"

    cur.execute("SELECT team_id FROM soccer_teams WHERE name = ?", (home_name,))
    row = cur.fetchone()
    if not row:
        print(f"Team not found: {home_name}")
        sys.exit(1)
    home_id = row[0]

    cur.execute("SELECT team_id FROM soccer_teams WHERE name = ?", (away_name,))
    row = cur.fetchone()
    if not row:
        print(f"Team not found: {away_name}")
        sys.exit(1)
    away_id = row[0]

    # Use today as the cutoff (no future data leakage)
    from datetime import date
    today = date.today().isoformat()

    result = analyse_match(home_id, away_id, today,
                           home_moneyline=None, away_moneyline=None,
                           conn=conn)
    conn.close()

    print(f"\n{'='*55}")
    print(f"  {home_name} (home)  vs  {away_name} (away)")
    print(f"{'='*55}")
    print(f"  Expected goals   λ_H={result['lambda_H']:.3f}  λ_A={result['lambda_A']:.3f}")
    print(f"  Home win         {result['p_home']*100:.1f}%")
    print(f"  Draw             {result['p_draw']*100:.1f}%")
    print(f"  Away win         {result['p_away']*100:.1f}%")
    print(f"  Over 2.5         {result['p_over25']*100:.1f}%")
    print(f"  Home games used  {result['home_ratings']['home_n']}")
    print(f"  Away games used  {result['away_ratings']['away_n']}")
