"""
Generate and persist a full calibration/ROI snapshot for a soccer_model_predictions
method -- Brier score (all games, not just placed bets), the compression-bucket table
(model vs sharp implied probability, bucketed by the market's own p_home), pooled
signed bias (home/away split vs the sharp book), and ROI vs Bet365 at EV>0/5/10%.

Built for the ROI-improvement investigation started 2026-08-07 (BUGS.md, BUG-009
addendum) -- xG spread stretch, blend-lever retuning, output recalibration, and
guardrails are each meant to be checked against the SAME baseline snapshot, both
seasons separately (a pooled-only number has repeatedly hidden a real
season-inconsistent effect in this investigation).

Every run writes a NEW file under model_snapshots/, named {timestamp}_{league}_
{method}.txt -- it never overwrites a previous run -- so the whole before/after
sequence stays on record. (2026-08-10: league added to the filename -- multi-league
runs of the same method issued in the same second, e.g. a shell loop over leagues,
previously collided on timestamp+method alone and silently clobbered each other.)
--note is required: a free-text description of what's different about THIS run
(e.g. "baseline, shipped defaults" or "ad hoc xG stretch=1.3 on top of shipped
defaults, not committed"). The snapshot also auto-records the actual value of every
real, committed tuning constant at run time (see MODEL_TUNING_PARAMETERS.md), so the
note only needs to describe what's NOT already visible in the committed code (e.g. an
experimental lever tested via monkeypatch, not yet a real constant).

Usage:
    python3 model_snapshot.py --note "baseline: shipped defaults, no changes"
    python3 model_snapshot.py --method poisson_v4 --season 2024 --season 2025 \\
        --note "ad hoc xG stretch=1.3, monkeypatched, not committed"
"""
import argparse
import io
import sqlite3
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path

from core.sports_db import DATABASE_PATH
import compute_club_player_strength as strength
import compare_model_vs_market_odds as cmvmo
import backtest_from_predictions as bfp

SNAPSHOT_DIR = Path(__file__).parent / "model_snapshots"
DEFAULT_METHOD = "poisson_v4"
DEFAULT_LEAGUE = "Serie A"
DEFAULT_SEASONS = [2024, 2025]
DEFAULT_SHARP_SOURCE = "Betfair Exchange"
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
    ("compute_club_player_strength", "PLAYER_RATING_PAST_MATCH_WINDOW_DECAY"),
    ("compute_club_player_strength", "PLAYER_RATING_MIN_ATTACK_WEIGHTED_MINUTES_TO_HAVE_OWN_RATING"),
    ("compute_club_player_strength", "PLAYER_RATING_MIN_ATTACK_WEIGHTED_MINUTES_TO_JOIN_LEAGUE_AVERAGE"),
    ("compute_club_player_strength", "PLAYER_RATING_MIN_DEFENSE_WEIGHTED_MINUTES_TO_HAVE_OWN_RATING"),
    ("compute_club_player_strength", "PLAYER_RATING_MIN_DEFENSE_WEIGHTED_MINUTES_TO_JOIN_LEAGUE_AVERAGE"),
    ("compute_club_player_strength", "PLAYER_RATING_MIN_MINUTES_FROM_PRIOR_SEASON"),
    ("compute_club_player_strength", "PLAYER_RATING_CROSS_LEAGUE_GOAL_ADJUSTMENT"),
    ("compute_club_player_strength", "PLAYER_RATING_LEAGUE_WIDE_BLEND_WEIGHT_OVERRIDE"),
]
_MODULES = {"core.poisson_model": __import__("core.poisson_model", fromlist=["_"]),
           "compute_club_player_strength": strength}


def committed_knob_values():
    return {f"{mod}.{name}": getattr(_MODULES[mod], name, "<not found>") for mod, name in KNOB_NAMES}


def brier_score(conn, league, season, method):
    cur = conn.cursor()
    cur.execute("""
        SELECT mp.p_home, mp.p_draw, mp.p_away, m.home_score, m.away_score
        FROM soccer_model_predictions mp
        JOIN soccer_matches m ON m.match_id = mp.match_id
        WHERE mp.league = ? AND mp.method = ? AND m.season = ? AND m.home_score IS NOT NULL
    """, (league, method, season))
    total, n = 0.0, 0
    for p_h, p_d, p_a, hs, as_ in cur.fetchall():
        if p_h is None:
            continue
        y_h, y_d, y_a = float(hs > as_), float(hs == as_), float(as_ > hs)
        total += (p_h - y_h) ** 2 + (p_d - y_d) ** 2 + (p_a - y_a) ** 2
        n += 1
    return (total / n if n else float("nan")), n


def compression_bucket_table(conn, league, season, method, source):
    pairs = cmvmo.fetch_pairs(conn, league, season, source, line_type="closing", method=method)
    by_bucket = {b: [] for b in BUCKETS}
    for p_h, p_d, p_a, m_h, m_d, m_a in pairs:
        for lo, hi in BUCKETS:
            if lo <= m_h < hi:
                by_bucket[(lo, hi)].append(p_h - m_h)
                break
    return {b: (sum(v) / len(v) if v else None, len(v)) for b, v in by_bucket.items()}


def build_report(conn, league, seasons, method, sharp_source, note):
    lines = []
    lines.append(f"# Model snapshot -- {method} / {league}")
    lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"Seasons: {seasons}")
    lines.append(f"Note: {note}")
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

    lines.append("## ROI vs Bet365 (ROI success criterion -- must be positive at EV>0/5/10%)")
    for season in seasons:
        for ev in (0.0, 0.05, 0.10):
            buf = io.StringIO()
            with redirect_stdout(buf):
                bfp.run(conn, league, season, method, ev, sportsbook="Bet365")
            lines.append(buf.getvalue().rstrip())
    lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--note", required=True,
                        help="What's different about this run (required -- this is the whole point of the log).")
    parser.add_argument("--method", default=DEFAULT_METHOD)
    parser.add_argument("--league", default=DEFAULT_LEAGUE)
    parser.add_argument("--season", type=int, action="append", dest="seasons",
                        help="Repeatable. Default: 2024 and 2025.")
    parser.add_argument("--sharp-source", default=DEFAULT_SHARP_SOURCE)
    args = parser.parse_args()
    seasons = args.seasons or DEFAULT_SEASONS

    SNAPSHOT_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(DATABASE_PATH)
    report = build_report(conn, args.league, seasons, args.method, args.sharp_source, args.note)
    conn.close()

    league_slug = "".join(c if c.isalnum() else "_" for c in args.league.lower()).strip("_")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = SNAPSHOT_DIR / f"{timestamp}_{league_slug}_{args.method}.txt"
    out_path.write_text(report)

    print(report)
    print(f"\nWritten to {out_path}")


if __name__ == "__main__":
    main()
