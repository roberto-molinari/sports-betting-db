"""
Generate and persist a full calibration/ROI report for a soccer_model_predictions
method -- Brier score (all games, not just placed bets), pooled signed bias
(home/away split vs a sharp book), and ROI vs Bet365 at EV>0/5/10%. Covers both
markets the model supports, 1X2 and totals/over-under, kept as separate numbers
throughout -- never blended into one figure, matching backtest_from_predictions.
run_totals()'s own documented convention (different markets, different bet types).
Totals has no bias check yet: no sharp-book O/U odds are ingested anywhere in this
codebase (FEATURE-015, BUGS.md) -- a real data gap, reported as "not available"
rather than silently omitted.

Renamed from model_snapshot.py 2026-08-11 -- the old name didn't say what the tool
actually produces.

Persistence is governed by --note alone, not argument count (fixed 2026-08-12 --
previously ANY other flag, e.g. --guardrail with no --note, fell through to the
persisted path and errored demanding --note, defeating a quick unpersisted look
with a non-default flag set):

  --note OMITTED -- console-only preview: printed and NOT persisted, no file
  written under model_snapshots/, regardless of what OTHER flags are given (e.g.
  `--guardrail` alone still previews, it just doesn't require --note too). For a
  quick look without adding to the permanent record.

  --note GIVEN -- persisted mode, writes a NEW file under model_snapshots/ --
  never overwriting a previous run, so the whole before/after sequence stays on
  record. Two report shapes, chosen the same way either way:

    DEFAULT (no --league) -- the all-up report (2026-08-11, FEATURE-017):
    every league/season with real soccer_model_predictions rows for --method is
    discovered from the database itself (not a hardcoded list, so a newly-added
    league is picked up automatically). Three views: ALL-UP (every league x
    every season x every market, fully pooled into one Brier/Bias/ROI each), BY
    MARKET (pooled across leagues, split by season), BY LEAGUE (pooled across
    seasons, split by season, both markets shown). A summary view -- no
    compression-bucket table, no per-league constant dump repeated per section.
    Written to {timestamp}_all_leagues_{method}.txt.

    --league "X" -- the original single-league deep-dive report: adds the
    compression-bucket table (model vs sharp implied probability, bucketed by
    the market's own p_home) on top of what the all-up view shows, scoped to
    one league. Written to {timestamp}_{league}_{method}.txt.

--note is a free-text description of what's different about THIS run (e.g.
"baseline, shipped defaults"; the report auto-records every real committed
tuning constant, see MODEL_TUNING_PARAMETERS.md, so the note only needs to cover
what ISN'T already visible in committed code).

Usage:
    python3 model_metrics_report.py                     # console-only, not persisted
    python3 model_metrics_report.py --guardrail          # still console-only -- no --note given
    python3 model_metrics_report.py --note "baseline: shipped defaults, no changes"
    python3 model_metrics_report.py --league "Serie A" --season 2024 --season 2025 \\
        --note "ad hoc xG stretch=1.3, monkeypatched, not committed"
"""
import argparse
import io
import sqlite3
import sys
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path

from core.sports_db import DATABASE_PATH
import compute_club_player_strength as strength
import compare_model_vs_market_odds as cmvmo
import backtest_from_predictions as bfp
from generate_club_league_card import (CLUB_LEAGUE_MIN_PICK_PROBABILITY,
                                       CLUB_LEAGUE_MIN_MARKET_PROBABILITY,
                                       market_floor_for_league)

SNAPSHOT_DIR = Path(__file__).parent / "model_snapshots"
DEFAULT_METHOD = "poisson_v4"
DEFAULT_SEASONS = [2024, 2025]
DEFAULT_SHARP_SOURCE = "Betfair Exchange"
DEFAULT_SPORTSBOOK = "Bet365"

# 2022 cold-start burn-in exclusion (2026-08-20; BUGS.md WATCH entry): season 2022
# is the earliest data in the DB, so its opening months have NO prior history behind
# the season-blind rating windows -- measured calibration slope 0.385 (vs ~1.0
# everywhere else) before this date, recovering after it. Every metric this report
# computes (Brier, bias, ROI, both markets) excludes matches strictly before this
# cutoff, making 2022 a deliberate partial season: the report only ever grades
# predictions that were built off real prior history. This is a REPORTING scope,
# not a model constant -- backfills still cover full seasons, and
# backtest_from_predictions.py's own CLI still grades whole seasons unless a
# caller passes min_match_date explicitly.
METRICS_MIN_MATCH_DATE = "2022-11-01"
EV_THRESHOLDS = (0.0, 0.05, 0.10)
BUCKETS = [(0.00, 0.15), (0.15, 0.25), (0.25, 0.35), (0.35, 0.45), (0.45, 0.55), (0.55, 1.01)]

# Every real, committed tuning constant (MODEL_TUNING_PARAMETERS.md) -- auto-recorded
# so a snapshot always shows the exact shipped state, not just what the note claims.
KNOB_NAMES = [
    ("core.poisson_model", "TEAM_PAST_MATCH_WINDOW_SIZE"),
    ("core.poisson_model", "TEAM_PAST_MATCH_WINDOW_DECAY"),
    ("core.poisson_model", "TEAM_RATING_PULL_TOWARD_AVERAGE_MATCHES"),
    ("core.poisson_model", "TEAM_RATING_MIN_MATCHES_TO_TRUST_TEAM_RATING_OVER_LEAGUE_AVERAGE"),
    ("core.poisson_model", "LEAGUE_AVG_GOALS_PER_GAME_WINDOW_SIZE"),
    ("core.poisson_model", "LEAGUE_AVG_GOALS_PER_GAME_WINDOW_DECAY"),
    ("compute_club_player_strength", "TEAM_RATING_XG_V_GOALS_BLEND"),
    ("compute_club_player_strength", "PLAYER_RATING_MINUTES_TO_HALF_TRUST_OWN_RATE_OVER_LEAGUE_AVERAGE"),
    ("compute_club_player_strength", "PLAYER_RATING_PAST_MATCH_WINDOW_SIZE"),
    ("compute_club_player_strength", "PLAYER_RATING_RECENCY_HALF_LIFE_DAYS"),
    ("compute_club_player_strength", "PLAYER_RATING_RECENCY_CUTOFF_DAYS"),
    ("compute_club_player_strength", "PLAYER_RATING_MIN_ATTACK_WEIGHTED_MINUTES_TO_HAVE_OWN_RATING"),
    ("compute_club_player_strength", "PLAYER_RATING_MIN_ATTACK_WEIGHTED_MINUTES_TO_JOIN_LEAGUE_AVERAGE"),
    ("compute_club_player_strength", "PLAYER_RATING_MIN_DEFENSE_WEIGHTED_MINUTES_TO_HAVE_OWN_RATING"),
    ("compute_club_player_strength", "PLAYER_RATING_MIN_DEFENSE_WEIGHTED_MINUTES_TO_JOIN_LEAGUE_AVERAGE"),
    ("compute_club_player_strength", "PLAYER_RATING_COVERAGE_SATURATION_MINUTES"),
    ("compute_club_player_strength", "PLAYER_RATING_CROSS_LEAGUE_GOAL_ADJUSTMENT"),
    ("compute_club_player_strength", "PLAYER_RATING_LEAGUE_WIDE_BLEND_WEIGHT_OVERRIDE"),
]
_MODULES = {"core.poisson_model": __import__("core.poisson_model", fromlist=["_"]),
           "compute_club_player_strength": strength}


def committed_knob_values():
    return {f"{mod}.{name}": getattr(_MODULES[mod], name, "<not found>") for mod, name in KNOB_NAMES}


def brier_score(conn, league, season, method, min_match_date=METRICS_MIN_MATCH_DATE):
    """min_match_date defaults to METRICS_MIN_MATCH_DATE (the 2022 cold-start
    burn-in cutoff -- see that constant's comment); pass None to grade a full
    season without it."""
    cur = conn.cursor()
    cur.execute(f"""
        SELECT mp.p_home, mp.p_draw, mp.p_away, m.home_score, m.away_score
        FROM soccer_model_predictions mp
        JOIN soccer_matches m ON m.match_id = mp.match_id
        WHERE mp.league = ? AND mp.method = ? AND m.season = ? AND m.home_score IS NOT NULL
        {"AND m.match_date >= ?" if min_match_date is not None else ""}
    """, (league, method, season) + ((min_match_date,) if min_match_date is not None else ()))
    total, n = 0.0, 0
    for p_h, p_d, p_a, hs, as_ in cur.fetchall():
        if p_h is None:
            continue
        y_h, y_d, y_a = float(hs > as_), float(hs == as_), float(as_ > hs)
        total += (p_h - y_h) ** 2 + (p_d - y_d) ** 2 + (p_a - y_a) ** 2
        n += 1
    return (total / n if n else float("nan")), n


def totals_brier_score(conn, league, season, method, min_match_date=METRICS_MIN_MATCH_DATE):
    """Same shape/scale as brier_score() (sum of squared errors across every
    outcome class, not the single-term binary convention) so the two numbers stay
    comparable at a glance -- for a 2-class market centered near p=0.5 the naive
    baseline is ~0.5, not ~0.667 like 1X2's 3-class baseline.

    over_under_line comes from the prediction row itself (the line p_over/p_under
    were actually computed against), not a fresh join to soccer_betting_odds --
    consistent with what the model was graded on at prediction time. A push (total
    goals == line) has no defined over/under outcome and is excluded, matching
    backtest_from_predictions.run_totals()'s own handling.

    min_match_date: same burn-in cutoff default as brier_score()."""
    cur = conn.cursor()
    cur.execute(f"""
        SELECT mp.p_over, mp.p_under, mp.over_under_line, m.home_score, m.away_score
        FROM soccer_model_predictions mp
        JOIN soccer_matches m ON m.match_id = mp.match_id
        WHERE mp.league = ? AND mp.method = ? AND m.season = ? AND m.home_score IS NOT NULL
        {"AND m.match_date >= ?" if min_match_date is not None else ""}
    """, (league, method, season) + ((min_match_date,) if min_match_date is not None else ()))
    total, n = 0.0, 0
    for p_over, p_under, line, hs, as_ in cur.fetchall():
        if p_over is None or line is None:
            continue
        total_goals = hs + as_
        if total_goals == line:
            continue
        y_over = float(total_goals > line)
        y_under = 1.0 - y_over
        total += (p_over - y_over) ** 2 + (p_under - y_under) ** 2
        n += 1
    return (total / n if n else float("nan")), n


# ---------------------------------------------------------------------------
# All-up aggregation (FEATURE-017, 2026-08-11) -- pooling across leagues/seasons
# for the default (no --league) report.
# ---------------------------------------------------------------------------

def discover_leagues(conn, method):
    """Every league with at least one soccer_model_predictions row for `method` --
    queried live rather than a hardcoded list, so a newly-added league (or one
    whose feeder division has no predictions at all, e.g. Serie B) is included or
    excluded correctly without this file needing an update."""
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT league FROM soccer_model_predictions WHERE method = ? ORDER BY league",
                (method,))
    return [row[0] for row in cur.fetchall()]


def discover_seasons(conn, leagues, method):
    """Every season with at least one GRADED (result known) match across the given
    leagues -- same live-discovery reasoning as discover_leagues()."""
    if not leagues:
        return []
    placeholders = ",".join("?" * len(leagues))
    cur = conn.cursor()
    cur.execute(f"""
        SELECT DISTINCT m.season FROM soccer_model_predictions mp
        JOIN soccer_matches m ON m.match_id = mp.match_id
        WHERE mp.method = ? AND mp.league IN ({placeholders}) AND m.home_score IS NOT NULL
        ORDER BY m.season
    """, [method] + leagues)
    return [row[0] for row in cur.fetchall()]


def discover_all_methods(conn):
    """Every distinct soccer_model_predictions.method tag in the database, with row
    count and the most recent generated_at timestamp under that tag -- exactly the
    set of values --method (here and in every other model tool: backfill_player_
    blend_predictions.py, backtest_from_predictions.py, etc.) accepts. Sorted newest-
    generated first so "what's the latest version" is a direct read, not a guess
    from the method NAME (poisson_v4_1_1 sorting after poisson_v4_1 alphabetically
    is a naming-convention coincidence, not something to rely on)."""
    cur = conn.cursor()
    cur.execute("""
        SELECT method, COUNT(*), MAX(generated_at)
        FROM soccer_model_predictions
        GROUP BY method
        ORDER BY MAX(generated_at) DESC
    """)
    return cur.fetchall()


def print_methods_list(conn):
    """2026-08-15: added after a user ran --guardrail with no --method and got
    poisson_v4 (this file's long-standing DEFAULT_METHOD) without realizing it --
    silently NOT the latest shipped version. Printed on a bare invocation (no args
    at all) and on --help, so "what can I even pass to --method" and "which one is
    actually latest" are both answered before you accidentally grade the wrong
    version."""
    methods = discover_all_methods(conn)
    print("Available --method values (soccer_model_predictions.method), newest generated first:")
    if not methods:
        print("  (none -- no soccer_model_predictions rows in the database yet)")
        print()
        return
    for i, (method, n, last_generated) in enumerate(methods):
        tags = []
        if i == 0:
            tags.append("most recently generated")
        if method == DEFAULT_METHOD:
            tags.append("used when --method is omitted")
        tag_str = f"   <- {', '.join(tags)}" if tags else ""
        print(f"  {method:<24} rows={n:<6} last_generated={last_generated}{tag_str}")
    print()


def pooled_brier(conn, leagues, seasons, method, totals=False):
    """N-weighted pooled Brier across every (league, season) pair. Exact, not an
    approximation: Brier is itself a mean of squared errors, so a weighted sum of
    per-group means (weighted by each group's n) equals computing it on the raw
    concatenated data directly."""
    score_fn = totals_brier_score if totals else brier_score
    total, n = 0.0, 0
    for league in leagues:
        for season in seasons:
            score, m = score_fn(conn, league, season, method)
            if m:
                total += score * m
                n += m
    return (total / n if n else float("nan")), n


def pooled_bias(conn, leagues, seasons, method, sharp_source):
    """Pools by concatenating raw model-vs-market pairs across every (league,
    season) pair and calling compare_model_vs_market_odds.summarize() ONCE --
    not by averaging pre-computed per-group summaries, which would be wrong for
    summarize()'s non-linear stats (max_abs_diff, favored_agree_rate); only the
    mean would happen to come out right that way. Returns None if no pairs exist
    anywhere in scope."""
    all_pairs = []
    for league in leagues:
        for season in seasons:
            all_pairs.extend(cmvmo.fetch_pairs(conn, league, season, sharp_source,
                                                line_type="closing", method=method,
                                                min_match_date=METRICS_MIN_MATCH_DATE))
    return cmvmo.summarize(all_pairs) if all_pairs else None


def _guardrail_header_line(guardrail_floor, guardrail_market_floor, suffix, league=None):
    """One consistent 'Guardrail: ...' report-header line for whichever of the two
    floors (model-probability, market-probability -- BUG-009 2026-08-20) are set.
    guardrail_market_floor is truthy/None now (2026-08-21 per-league override) --
    if `league` is given (single-league report), the actual resolved value for
    that league is shown; otherwise (all-up, many leagues) it's labeled
    per-league since no single number applies."""
    parts = []
    if guardrail_floor is not None:
        parts.append(f"floor={guardrail_floor:g}")
    if guardrail_market_floor:
        if league is not None:
            parts.append(f"market_floor={market_floor_for_league(league):g}")
        else:
            parts.append(f"market_floor=per-league (default {CLUB_LEAGUE_MIN_MARKET_PROBABILITY:g}, "
                         f"see generate_club_league_card.CLUB_LEAGUE_MARKET_PROBABILITY_BY_LEAGUE)")
    if not parts:
        return ("Guardrail: none -- ROI below is raw model ROI, every EV-positive candidate "
                "regardless of probability (unchanged from before --guardrail existed)")
    return f"Guardrail: {', '.join(parts)} {suffix}"


def pooled_roi(conn, leagues, seasons, method, ev_threshold, sportsbook, totals=False, guardrail_floor=None, guardrail_market_floor=None):
    """True pooled ROI (sum of profit / sum of staked across every (league,
    season) pair), via backtest_from_predictions' stats-dict helpers -- not a
    weighted-average of pre-computed ROI ratios (mathematically equivalent if
    weighted by staked $, but summing the raw dollars directly is simpler).

    guardrail_floor: None (default) is today's unchanged raw-model ROI -- every
    EV-positive prediction, regardless of how low its probability is. Pass
    CLUB_LEAGUE_MIN_PICK_PROBABILITY (or any floor) to additionally reject
    candidates below it, the same guardrail generate_club_league_card.py applies
    to real picks -- see grade_1x2's docstring. guardrail_market_floor: same for
    the MARKET-side implied-probability floor (CLUB_LEAGUE_MIN_MARKET_PROBABILITY,
    BUG-009 2026-08-20). Brier/bias are NEVER guardrail-filtered (they're
    calibration checks over all games, not a betting-selection question) -- these
    parameters only exist on the ROI path."""
    grade_fn = bfp.grade_totals if totals else bfp.grade_1x2
    staked = profit = 0.0
    bets = wins = graded = 0
    for league in leagues:
        for season in seasons:
            stats = grade_fn(conn, league, season, method, ev_threshold, sportsbook=sportsbook,
                             guardrail_floor=guardrail_floor, guardrail_market_floor=guardrail_market_floor,
                             min_match_date=METRICS_MIN_MATCH_DATE)
            staked += stats["staked"]
            profit += stats["profit"]
            bets += stats["bets"]
            wins += stats["wins"]
            graded += stats["n_graded"]
    roi = profit / staked if staked else 0.0
    return {"roi": roi, "staked": staked, "profit": profit, "bets": bets, "wins": wins, "n_graded": graded}


def compression_bucket_table(conn, league, season, method, source):
    pairs = cmvmo.fetch_pairs(conn, league, season, source, line_type="closing", method=method,
                              min_match_date=METRICS_MIN_MATCH_DATE)
    by_bucket = {b: [] for b in BUCKETS}
    for p_h, p_d, p_a, m_h, m_d, m_a in pairs:
        for lo, hi in BUCKETS:
            if lo <= m_h < hi:
                by_bucket[(lo, hi)].append(p_h - m_h)
                break
    return {b: (sum(v) / len(v) if v else None, len(v)) for b, v in by_bucket.items()}


def build_report(conn, league, seasons, method, sharp_source, note, guardrail_floor=None, guardrail_market_floor=None):
    lines = []
    lines.append(f"# Model metrics report -- {method} / {league}")
    lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"Seasons: {seasons}")
    lines.append(f"Note: {note}")
    lines.append(_guardrail_header_line(guardrail_floor, guardrail_market_floor,
                                        "(ROI below reflects only guardrail-clear candidates)",
                                        league=league))
    lines.append(f"Scope: matches before {METRICS_MIN_MATCH_DATE} excluded from every metric "
                  f"(2022 cold-start burn-in -- BUGS.md WATCH entry, 2026-08-20; "
                  f"season 2022 is a deliberate partial season)")
    lines.append("")
    lines.append("## Committed model constants at run time")
    for name, val in committed_knob_values().items():
        lines.append(f"  {name} = {val}")
    lines.append("")

    lines.append("## Brier score (all games, not just bets -- lower is better; naive baseline ~0.667)")
    pooled_total, pooled_n = 0.0, 0
    for season in seasons:
        score, n = brier_score(conn, league, season, method)
        lines.append(f"  season {season}: Brier = {score:.4f}  (n={n})")
        pooled_total += score * n
        pooled_n += n
    if pooled_n:
        lines.append(f"  pooled: Brier = {pooled_total/pooled_n:.4f}  (n={pooled_n})")
    lines.append("")

    lines.append("## Brier score, TOTALS/over-under (lower is better; naive baseline ~0.5 -- "
                  "2-class scale, not directly comparable to 1X2's 3-class number above)")
    pooled_total, pooled_n = 0.0, 0
    for season in seasons:
        score, n = totals_brier_score(conn, league, season, method)
        lines.append(f"  season {season}: Brier = {score:.4f}  (n={n})")
        pooled_total += score * n
        pooled_n += n
    if pooled_n:
        lines.append(f"  pooled: Brier = {pooled_total/pooled_n:.4f}  (n={pooled_n})")
    lines.append("")

    lines.append(f"## Compression-bucket table (model p_home - {sharp_source} closing p_home, by market's own p_home)")
    for season in seasons:
        lines.append(f"  season {season}:")
        table = compression_bucket_table(conn, league, season, method, sharp_source)
        for (lo, hi), (val, n) in table.items():
            label = f"{lo:.2f}-{hi:.2f}"
            lines.append(f"    {label:>10}: n=0" if val is None else f"    {label:>10}: {val:+.3f}  (n={n})")
    lines.append("")

    lines.append(f"## Pooled signed bias vs {sharp_source} closing (home/away split, target +/-0.01-0.02)")
    for season in seasons:
        pairs = cmvmo.fetch_pairs(conn, league, season, sharp_source, line_type="closing", method=method)
        if not pairs:
            lines.append(f"  season {season}: no data")
            continue
        s = cmvmo.summarize(pairs)
        lines.append(f"  season {season}: home={s['mean_signed_diff']['home']:+.4f}  "
                     f"away={s['mean_signed_diff']['away']:+.4f}  "
                     f"draw={s['mean_signed_diff']['draw']:+.4f}  (n={s['n']})")
    lines.append("")

    lines.append("## Bias, TOTALS/over-under: not available")
    lines.append("  No sharp-book totals odds are ingested anywhere in this codebase (soccer_market_odds")
    lines.append("  has no O/U columns at all), so there's no line to bias-check the totals market")
    lines.append("  against yet -- not a wiring gap, a real data gap. See FEATURE-015, BUGS.md.")
    lines.append("")

    lines.append("## ROI vs Bet365 (ROI success criterion -- must be positive at EV>0/5/10%)")
    for season in seasons:
        for ev in (0.0, 0.05, 0.10):
            buf = io.StringIO()
            with redirect_stdout(buf):
                bfp.run(conn, league, season, method, ev, sportsbook="Bet365", guardrail_floor=guardrail_floor, guardrail_market_floor=guardrail_market_floor,
                        min_match_date=METRICS_MIN_MATCH_DATE)
            lines.append(buf.getvalue().rstrip())
    lines.append("")

    lines.append("## ROI vs Bet365, TOTALS/over-under (never pooled with 1X2 ROI -- different markets)")
    for season in seasons:
        for ev in (0.0, 0.05, 0.10):
            buf = io.StringIO()
            with redirect_stdout(buf):
                bfp.run_totals(conn, league, season, method, ev, sportsbook="Bet365", guardrail_floor=guardrail_floor, guardrail_market_floor=guardrail_market_floor,
                               min_match_date=METRICS_MIN_MATCH_DATE)
            lines.append(buf.getvalue().rstrip())
    lines.append("")

    return "\n".join(lines)


def pooled_brier_across_markets(conn, leagues, seasons, method):
    """True cross-market Brier: pools 1X2 and totals squared-error sums into ONE
    number. Legitimate, not just convenient -- both are sum-of-squared-probability-
    error scores that land on the same [0, 2] scale regardless of outcome-class
    count (a 3-class and a 2-class Brier only differ in their NAIVE baseline,
    ~0.667 vs ~0.5, not in the scale of the score itself -- see brier_score()/
    totals_brier_score()'s own docstrings), so n-weighted-averaging them answers a
    real question: mean squared probability error per graded prediction, regardless
    of which market that prediction was for."""
    b1x2, n1x2 = pooled_brier(conn, leagues, seasons, method, totals=False)
    btot, ntot = pooled_brier(conn, leagues, seasons, method, totals=True)
    n = n1x2 + ntot
    if not n:
        return float("nan"), 0
    return (b1x2 * n1x2 + btot * ntot) / n, n


def pooled_roi_across_markets(conn, leagues, seasons, method, ev_threshold, sportsbook, guardrail_floor=None, guardrail_market_floor=None):
    """True cross-market ROI: every bet is staked $1 regardless of which market it
    came from, so summing profit/staked across both markets is exact -- the
    portfolio-level return if every EV-positive bet in either market were placed
    (or, with guardrail_floor set, every EV-positive AND guardrail-clear bet --
    see pooled_roi's docstring)."""
    r1x2 = pooled_roi(conn, leagues, seasons, method, ev_threshold, sportsbook, totals=False,
                      guardrail_floor=guardrail_floor, guardrail_market_floor=guardrail_market_floor)
    rtot = pooled_roi(conn, leagues, seasons, method, ev_threshold, sportsbook, totals=True,
                      guardrail_floor=guardrail_floor, guardrail_market_floor=guardrail_market_floor)
    staked = r1x2["staked"] + rtot["staked"]
    profit = r1x2["profit"] + rtot["profit"]
    roi = profit / staked if staked else 0.0
    return {"roi": roi, "staked": staked, "profit": profit,
            "bets": r1x2["bets"] + rtot["bets"], "wins": r1x2["wins"] + rtot["wins"],
            "n_graded": r1x2["n_graded"] + rtot["n_graded"]}


def _all_up_block(conn, leagues, seasons, method, sharp_source, guardrail_floor=None, guardrail_market_floor=None):
    """The genuine ALL-UP block -- three numbers (Brier, Bias, ROI), each pooled
    across every league, season, AND market, not shown per-market like every other
    block in this report. Bias is the one metric that can't actually be pooled
    across markets: totals has no sharp-book O/U data to blend WITH (FEATURE-015),
    so this number is inherently 1X2-only -- noted inline rather than silently
    presented as if it covered both markets."""
    lines = []
    b, n = pooled_brier_across_markets(conn, leagues, seasons, method)
    lines.append(f"  Brier: {b:.4f}  (n={n}, 1X2 + totals pooled)")

    bias = pooled_bias(conn, leagues, seasons, method, sharp_source)
    if bias:
        lines.append(f"  Bias vs {sharp_source}: home={bias['mean_signed_diff']['home']:+.4f}  "
                      f"away={bias['mean_signed_diff']['away']:+.4f}  "
                      f"draw={bias['mean_signed_diff']['draw']:+.4f}  "
                      f"(n={bias['n']}, 1X2 only -- totals has no sharp-book data, FEATURE-015)")
    else:
        lines.append("  Bias: no data")

    for ev in EV_THRESHOLDS:
        r = pooled_roi_across_markets(conn, leagues, seasons, method, ev, DEFAULT_SPORTSBOOK,
                                      guardrail_floor=guardrail_floor, guardrail_market_floor=guardrail_market_floor)
        lines.append(f"  ROI @ EV>{ev:.0%}: {r['roi']:+.1%}  (1X2 + totals pooled; "
                      f"bets={r['bets']}, staked=${r['staked']:.2f}, profit=${r['profit']:+.2f})")
    return lines


def _both_markets_block(conn, leagues, seasons, method, sharp_source, guardrail_floor=None, guardrail_market_floor=None):
    """Brier/bias/ROI for BOTH markets, side by side, never blended into one
    number -- the block used for the ALL-UP section and each BY LEAGUE entry."""
    lines = []
    b1x2, n1x2 = pooled_brier(conn, leagues, seasons, method, totals=False)
    lines.append(f"  1X2 Brier:      {b1x2:.4f}  (n={n1x2})")
    btot, ntot = pooled_brier(conn, leagues, seasons, method, totals=True)
    lines.append(f"  Totals Brier:   {btot:.4f}  (n={ntot})")

    bias = pooled_bias(conn, leagues, seasons, method, sharp_source)
    if bias:
        lines.append(f"  1X2 Bias vs {sharp_source}: home={bias['mean_signed_diff']['home']:+.4f}  "
                      f"away={bias['mean_signed_diff']['away']:+.4f}  "
                      f"draw={bias['mean_signed_diff']['draw']:+.4f}  (n={bias['n']})")
    else:
        lines.append(f"  1X2 Bias vs {sharp_source}: no data")
    lines.append("  Totals Bias:    not available (no sharp-book O/U data ingested -- FEATURE-015)")

    for ev in EV_THRESHOLDS:
        r = pooled_roi(conn, leagues, seasons, method, ev, DEFAULT_SPORTSBOOK, totals=False,
                       guardrail_floor=guardrail_floor, guardrail_market_floor=guardrail_market_floor)
        lines.append(f"  1X2 ROI    @ EV>{ev:.0%}: {r['roi']:+.1%}  "
                      f"(bets={r['bets']}, staked=${r['staked']:.2f}, profit=${r['profit']:+.2f})")
    for ev in EV_THRESHOLDS:
        r = pooled_roi(conn, leagues, seasons, method, ev, DEFAULT_SPORTSBOOK, totals=True,
                       guardrail_floor=guardrail_floor, guardrail_market_floor=guardrail_market_floor)
        lines.append(f"  Totals ROI @ EV>{ev:.0%}: {r['roi']:+.1%}  "
                      f"(bets={r['bets']}, staked=${r['staked']:.2f}, profit=${r['profit']:+.2f})")
    return lines


def _single_market_block(conn, leagues, seasons, method, sharp_source, totals, guardrail_floor=None, guardrail_market_floor=None):
    """Brier/bias/ROI for ONE market only -- the block used inside the BY MARKET
    section, which is already scoped to a single market per subsection."""
    lines = []
    b, n = pooled_brier(conn, leagues, seasons, method, totals=totals)
    lines.append(f"  Brier: {b:.4f}  (n={n})")

    if totals:
        lines.append("  Bias:  not available (no sharp-book O/U data ingested -- FEATURE-015)")
    else:
        bias = pooled_bias(conn, leagues, seasons, method, sharp_source)
        if bias:
            lines.append(f"  Bias vs {sharp_source}: home={bias['mean_signed_diff']['home']:+.4f}  "
                          f"away={bias['mean_signed_diff']['away']:+.4f}  "
                          f"draw={bias['mean_signed_diff']['draw']:+.4f}  (n={bias['n']})")
        else:
            lines.append(f"  Bias vs {sharp_source}: no data")

    for ev in EV_THRESHOLDS:
        r = pooled_roi(conn, leagues, seasons, method, ev, DEFAULT_SPORTSBOOK, totals=totals,
                       guardrail_floor=guardrail_floor, guardrail_market_floor=guardrail_market_floor)
        lines.append(f"  ROI @ EV>{ev:.0%}: {r['roi']:+.1%}  "
                      f"(bets={r['bets']}, staked=${r['staked']:.2f}, profit=${r['profit']:+.2f})")
    return lines


def build_all_up_report(conn, method, sharp_source, note, seasons_filter=None, guardrail_floor=None, guardrail_market_floor=None):
    """The default (no --league) report -- FEATURE-017, 2026-08-11: a summary view
    across every league/season the model has real prediction data for, discovered
    live from the database (see discover_leagues/discover_seasons), not the
    detailed single-league deep-dive build_report() produces (no compression-
    bucket table, no per-section constant dump -- that level of detail is what
    --league "X" is for).

    Three views, all built from the same pooled_brier/pooled_bias/pooled_roi
    primitives so every number in this report is computed the same way regardless
    of scope:
      ALL-UP    -- every league x every season x every market, fully pooled into
                   three numbers (Brier, Bias, ROI). The one section in this report
                   where 1X2 and totals ARE blended together -- see _all_up_block()'s
                   docstring for why that's legitimate here specifically.
      BY MARKET -- pooled across leagues, split by season, one market at a time.
      BY LEAGUE -- pooled across seasons, split by season, both markets shown.
    """
    leagues = discover_leagues(conn, method)
    if not leagues:
        return f"No soccer_model_predictions rows found for method={method!r} -- nothing to report."
    seasons = discover_seasons(conn, leagues, method)
    if seasons_filter:
        seasons = [s for s in seasons if s in seasons_filter]

    lines = []
    lines.append(f"# Model metrics report -- ALL-UP -- {method}")
    lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"Leagues ({len(leagues)}): {', '.join(leagues)}")
    lines.append(f"Seasons ({len(seasons)}): {seasons}")
    lines.append(f"Note: {note}")
    lines.append(_guardrail_header_line(guardrail_floor, guardrail_market_floor,
                                        "(matches generate_club_league_card.py's shipped floors -- "
                                        "ROI below reflects only guardrail-clear candidates)"))
    lines.append(f"Scope: matches before {METRICS_MIN_MATCH_DATE} excluded from every metric "
                  f"(2022 cold-start burn-in -- BUGS.md WATCH entry, 2026-08-20; "
                  f"season 2022 is a deliberate partial season)")
    lines.append("")
    lines.append("## Committed model constants at run time")
    for name, val in committed_knob_values().items():
        lines.append(f"  {name} = {val}")
    lines.append("")
    lines.append("Brier and ROI in the ALL-UP section below are pooled across BOTH markets into one")
    lines.append("number each (legitimate for these two -- see pooled_brier_across_markets()/")
    lines.append("pooled_roi_across_markets()'s docstrings for why). Everywhere else in this report,")
    lines.append("1X2 and totals are kept as separate numbers, never blended -- different markets,")
    lines.append("matching backtest_from_predictions.run_totals()'s own documented convention.")
    lines.append("Brier/bias are calibration checks over ALL games and are NEVER guardrail-filtered")
    lines.append("regardless of the Guardrail setting above -- only ROI (a betting-selection")
    lines.append("question) is affected.")
    lines.append("")

    lines.append("=" * 78)
    lines.append("ALL-UP  (every league x every season x every market)")
    lines.append("=" * 78)
    lines.extend(_all_up_block(conn, leagues, seasons, method, sharp_source, guardrail_floor=guardrail_floor, guardrail_market_floor=guardrail_market_floor))
    lines.append("")

    lines.append("=" * 78)
    lines.append("BY MARKET  (pooled across every league)")
    lines.append("=" * 78)
    for totals in (False, True):
        market_label = "TOTALS/over-under" if totals else "1X2"
        lines.append(f"\n-- {market_label}, across seasons --")
        lines.extend(_single_market_block(conn, leagues, seasons, method, sharp_source, totals,
                                          guardrail_floor=guardrail_floor, guardrail_market_floor=guardrail_market_floor))
        for season in seasons:
            lines.append(f"\n-- {market_label}, season {season} --")
            lines.extend(_single_market_block(conn, leagues, [season], method, sharp_source, totals,
                                              guardrail_floor=guardrail_floor, guardrail_market_floor=guardrail_market_floor))
    lines.append("")

    lines.append("=" * 78)
    lines.append("BY LEAGUE  (both markets; pooled across seasons, then by season)")
    lines.append("=" * 78)
    for league in leagues:
        lines.append(f"\n-- {league}, across seasons --")
        lines.extend(_both_markets_block(conn, [league], seasons, method, sharp_source,
                                         guardrail_floor=guardrail_floor, guardrail_market_floor=guardrail_market_floor))
        for season in seasons:
            lines.append(f"\n-- {league}, season {season} --")
            lines.extend(_both_markets_block(conn, [league], [season], method, sharp_source,
                                             guardrail_floor=guardrail_floor, guardrail_market_floor=guardrail_market_floor))
    lines.append("")

    return "\n".join(lines)


def main():
    # 2026-08-15: print the available --method values before anything else, on a
    # bare invocation (no args at all) or --help/-h -- --help exits via argparse
    # inside parse_args() below, before any of our own code would otherwise run,
    # so this has to happen first, not after. See print_methods_list's docstring
    # for the confusion this fixes (a user ran --guardrail with no --method,
    # silently got the long-standing DEFAULT_METHOD, not the latest shipped
    # version, with no way to discover what "latest" even was).
    if len(sys.argv) == 1 or "-h" in sys.argv[1:] or "--help" in sys.argv[1:]:
        conn = sqlite3.connect(DATABASE_PATH)
        print_methods_list(conn)
        conn.close()

    # Console-only preview: whenever --note is omitted, print the report and don't
    # persist it -- no file under model_snapshots/. This governs regardless of
    # what OTHER flags are given (2026-08-12 fix: previously ANY flag at all,
    # even just --guardrail with no --note, fell through to the persisted path
    # and errored demanding --note, which defeated the point of a quick unpersisted
    # look with a non-default flag set -- see test_flag_without_note_still_previews_
    # and_does_not_persist). --note is the actual signal for "this is a real,
    # permanent record," not argument count.
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--note", default=None,
                        help="What's different about this run. Persists a snapshot file under "
                             "model_snapshots/ when given (this is the whole point of the log). "
                             "Omit for a console-only preview -- printed, not persisted, no file "
                             "written, regardless of what other flags are given.")
    parser.add_argument("--method", default=DEFAULT_METHOD)
    parser.add_argument("--league",
                         help="Single-league deep-dive report for this league (adds the compression-"
                              "bucket table). Omit for the default: the all-up report across every "
                              "league/season with real prediction data.")
    parser.add_argument("--season", type=int, action="append", dest="seasons",
                        help="Repeatable. Single-league mode default: 2024 and 2025. "
                             "All-up mode default: every season discovered in the data.")
    parser.add_argument("--sharp-source", default=DEFAULT_SHARP_SOURCE)
    parser.add_argument("--guardrail", action="store_true",
                        help="Report ROI with generate_club_league_card.py's real, shipped "
                             "guardrails applied on top of the EV threshold: "
                             "CLUB_LEAGUE_MIN_PICK_PROBABILITY (model floor) and "
                             "CLUB_LEAGUE_MIN_MARKET_PROBABILITY (market floor, BUG-009 "
                             "2026-08-20) -- 'what would ROI look like for what the live card "
                             "generator actually surfaces', not just every raw EV-positive "
                             "prediction. Brier/bias are unaffected either way (calibration "
                             "checks over all games, not a betting-selection question). Default "
                             "off: today's unchanged raw-model ROI.")
    args = parser.parse_args()
    guardrail_floor = CLUB_LEAGUE_MIN_PICK_PROBABILITY if args.guardrail else None
    guardrail_market_floor = args.guardrail   # resolved per-league inside backtest_from_predictions
    note = args.note if args.note is not None else "(console-only preview -- not persisted)"

    conn = sqlite3.connect(DATABASE_PATH)
    if args.league:
        seasons = args.seasons or DEFAULT_SEASONS
        report = build_report(conn, args.league, seasons, args.method, args.sharp_source, note,
                              guardrail_floor=guardrail_floor, guardrail_market_floor=guardrail_market_floor)
        league_slug = "".join(c if c.isalnum() else "_" for c in args.league.lower()).strip("_")
    else:
        report = build_all_up_report(conn, args.method, args.sharp_source, note,
                                      seasons_filter=args.seasons, guardrail_floor=guardrail_floor, guardrail_market_floor=guardrail_market_floor)
        league_slug = "all_leagues"
    conn.close()

    if args.note is None:
        print(report)
        return

    SNAPSHOT_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    guardrail_suffix = "_guardrail" if args.guardrail else ""
    out_path = SNAPSHOT_DIR / f"{timestamp}_{league_slug}_{args.method}{guardrail_suffix}.txt"
    out_path.write_text(report)

    print(report)
    print(f"\nWritten to {out_path}")


if __name__ == "__main__":
    main()
