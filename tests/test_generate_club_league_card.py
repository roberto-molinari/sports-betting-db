"""Tests for generate_club_league_card.py -- the club-league card generator that
replaced generate_serie_a_card.py 2026-08-07 (see BUGS.md): parameterized by
--league instead of hardcoded to Serie A, running the real FEATURE-011 player-blend
pipeline instead of the old team-only model, and applying the floor guardrail that
had never been ported from generate_wc_card.py."""

from datetime import datetime, timezone
from unittest.mock import patch

import core.sports_db as sports_db
import generate_club_league_card as gclc


def _seed_lopsided_league(conn, league="Serie A", season=2025):
    """A strong team and a weak team with enough match history to clear
    TEAM_RATING_MIN_MATCHES_TO_TRUST_TEAM_RATING_OVER_LEAGUE_AVERAGE, then a new
    (unplayed) match between them with betting odds -- realistic enough for
    compute()/analyse_match_wc() to produce genuinely different win probabilities
    for the two sides, not just fall back to the league average."""
    strong = sports_db.ensure_soccer_team("StrongFC", league)
    weak = sports_db.ensure_soccer_team("WeakFC", league)
    striker_s = sports_db.add_player(strong, "StrongStriker", position="F", conn=conn)
    striker_w = sports_db.add_player(weak, "WeakStriker", position="F", conn=conn)

    for i, date in enumerate(("2025-09-01", "2025-09-08", "2025-09-15")):
        opp_s = sports_db.ensure_soccer_team(f"OppS{i}", league)
        opp_w = sports_db.ensure_soccer_team(f"OppW{i}", league)
        m_s = sports_db.add_soccer_match(league, season, strong, opp_s, date)
        m_w = sports_db.add_soccer_match(league, season, weak, opp_w, date)
        sports_db.update_soccer_match_result(m_s, 3, 0)
        sports_db.update_soccer_match_result(m_w, 0, 2)
        sports_db.add_player_match_stats(striker_s, m_s, season=season, venue="home",
                                         minutes_played=90, xg=3.0, conn=conn)
        sports_db.add_player_match_stats(striker_w, m_w, season=season, venue="home",
                                         minutes_played=90, xg=0.2, conn=conn)

    # away_moneyline=+2000 (implied ~4.8%) is well below WeakFC's actual model
    # probability (~7%, computed from this seed) -- positive EV, but the model
    # probability itself is still under the floor, the exact "confident-looking EV
    # is actually noise" case the guardrail exists for (BUG-003's original
    # diagnosis).
    match_id = sports_db.add_soccer_match(league, season, strong, weak, "2025-09-22T18:00:00.000Z")
    sports_db.add_soccer_betting_odds(
        match_id=match_id, sportsbook="Test", odds_date="2025-09-20",
        home_moneyline=-350, draw_moneyline=450, away_moneyline=2000,
        over_under=2.5, over_odds=-110, under_odds=-110,
    )
    return match_id


def _run_card(capsys, db_path, now=datetime(2025, 9, 21, tzinfo=timezone.utc), league="Serie A",
              dry_run=False, matchday_date="2025-09-22", post_friendly=False):
    # generate_club_league_card does `from core.sports_db import DATABASE_PATH`,
    # binding its own copy at import time -- the db_path fixture only patches
    # core.sports_db's and core.poisson_model's copies (see conftest.py's
    # docstring), so this module's copy needs patching too or it silently hits
    # the real production DB instead of the isolated test one.
    # matchday_date: a single 'YYYY-MM-DD' string, or a (start, end) tuple/list
    # for the two-date range form.
    with patch("generate_club_league_card.DATABASE_PATH", db_path), \
         patch("generate_club_league_card.datetime") as mock_dt:
        mock_dt.now.return_value = now
        import sys
        old_argv = sys.argv
        sys.argv = ["generate_club_league_card.py", "--league", league, "--matchday-date"]
        if isinstance(matchday_date, (tuple, list)):
            sys.argv += list(matchday_date)
        else:
            sys.argv.append(matchday_date)
        if dry_run:
            sys.argv.append("--dry-run")
        if post_friendly:
            sys.argv.append("--post-friendly")
        try:
            gclc.main()
        finally:
            sys.argv = old_argv
    return capsys.readouterr().out


# ── build_candidates: pure unit tests ─────────────────────────────────────────────

def test_build_candidates_skips_sides_with_no_odds():
    match = {"match_id": 1, "match_date": "2025-09-22", "home": "A", "away": "B",
             "home_moneyline": -200, "draw_moneyline": None, "away_moneyline": 300,
             "over_odds": None, "under_odds": None}
    result = {"p_home": 0.6, "p_draw": 0.25, "p_away": 0.15,
              "ev_home": 0.1, "ev_draw": None, "ev_away": 0.05}
    candidates = gclc.build_candidates(match, result)
    sides = {c["side"] for c in candidates}
    assert sides == {"HOME", "AWAY"}


def test_build_candidates_includes_over_under_when_priced():
    match = {"match_id": 1, "match_date": "2025-09-22", "home": "A", "away": "B",
             "home_moneyline": -200, "draw_moneyline": 300, "away_moneyline": 300,
             "over_odds": -110, "under_odds": -110}
    result = {"p_home": 0.6, "p_draw": 0.2, "p_away": 0.2,
              "ev_home": 0.1, "ev_draw": 0.0, "ev_away": 0.0,
              "p_over": 0.55, "p_under": 0.45, "ev_over": 0.05, "ev_under": -0.05}
    candidates = gclc.build_candidates(match, result)
    sides = {c["side"] for c in candidates}
    assert "OVER 2.5" in sides and "UNDER 2.5" in sides


# ── end-to-end: real pipeline + guardrail ──────────────────────────────────────────

def test_card_uses_league_param_not_hardcoded(db_path, conn, capsys):
    _seed_lopsided_league(conn, league="Bundesliga")
    out = _run_card(capsys, db_path, league="Bundesliga")
    assert "LEAGUE Bundesliga" in out
    assert "MATCHES 1" in out

    out_wrong_league = _run_card(capsys, db_path, league="Serie A")
    assert "MATCHES 0" in out_wrong_league


def test_card_only_surfaces_positive_ev_guardrail_clear_picks(db_path, conn, capsys):
    _seed_lopsided_league(conn)
    out = _run_card(capsys, db_path)

    # Every printed pick line must be a real (guardrail-clear, positive-EV) pick --
    # spot check the format is present and the away longshot doesn't silently
    # replace a legitimate guardrail-driven exclusion.
    assert "StrongFC vs WeakFC" in out
    assert "TOP PICKS PER MATCH" in out


def test_card_excludes_subfloor_candidate_and_logs_it(db_path, conn, capsys):
    """WeakFC's away win probability against a much stronger home side lands under
    CLUB_LEAGUE_MIN_PICK_PROBABILITY (~7% modeled, well under the 25% floor) while
    the +2000 market odds still make it positive-EV -- the exact "confident-looking
    EV is actually noise" case BUG-003's guardrail exists for. Must show up in the
    GUARDRAIL LOG, never as a printed pick."""
    _seed_lopsided_league(conn)
    out = _run_card(capsys, db_path)

    assert "GUARDRAIL LOG" in out
    log_section = out.split("GUARDRAIL LOG")[1]
    picks_section = out.split("GUARDRAIL LOG")[0]
    assert "AWAY" in log_section and "floor" in log_section
    assert "AWAY" not in picks_section


def test_card_wires_market_floor_into_guardrail(db_path, conn, capsys):
    """BUG-009 re-diagnosis (2026-08-20): CLUB_LEAGUE_MIN_MARKET_PROBABILITY must
    actually reach guardrail_reasons() -- patched to 1.0 so EVERY candidate's market
    implied probability is below it, meaning no pick may survive and the exclusions
    must cite the market floor, not just the model floor."""
    _seed_lopsided_league(conn)
    with patch.object(gclc, "CLUB_LEAGUE_MIN_MARKET_PROBABILITY", 1.0):
        out = _run_card(capsys, db_path)

    assert "No guardrail-clear positive-EV picks in this window" in out
    assert "market floor" in out


# ── FEATURE-016: picks persisted for later scoring ─────────────────────────────────

def test_card_stores_picks_by_default(db_path, conn, capsys):
    _seed_lopsided_league(conn)
    out = _run_card(capsys, db_path)
    assert "stored in soccer_club_league_picks" in out
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM soccer_club_league_picks")
    assert cur.fetchone()[0] > 0


def test_card_dry_run_does_not_store(db_path, conn, capsys):
    _seed_lopsided_league(conn)
    out = _run_card(capsys, db_path, dry_run=True)
    assert "dry-run, not stored" in out
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM soccer_club_league_picks")
    assert cur.fetchone()[0] == 0


def test_card_rerun_replaces_ungraded_picks_not_stack(db_path, conn, capsys):
    _seed_lopsided_league(conn)
    _run_card(capsys, db_path)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM soccer_club_league_picks")
    first_count = cur.fetchone()[0]
    assert first_count > 0

    _run_card(capsys, db_path)   # re-run, same inputs -- must replace, not double
    cur.execute("SELECT COUNT(*) FROM soccer_club_league_picks")
    assert cur.fetchone()[0] == first_count


# ── market_floor_for_league (2026-08-21 per-league override) ──────────────────────

def test_market_floor_for_league_uses_override_when_present():
    assert gclc.market_floor_for_league("Premier League") == 0.30
    assert gclc.market_floor_for_league("La Liga") == 0.40


def test_market_floor_for_league_falls_back_to_shared_default():
    assert gclc.market_floor_for_league("Serie A") == gclc.CLUB_LEAGUE_MIN_MARKET_PROBABILITY
    assert gclc.market_floor_for_league("A League Not In The Override Dict") == gclc.CLUB_LEAGUE_MIN_MARKET_PROBABILITY


def test_per_league_overrides_within_5pp_of_shared_default():
    # The clamp discipline this session settled on -- deviations from the shared
    # floor should stay modest rather than chase one league's best-looking, likely
    # noisiest number (BUGS.md, 2026-08-21).
    for league, floor in gclc.CLUB_LEAGUE_MARKET_PROBABILITY_BY_LEAGUE.items():
        assert abs(floor - gclc.CLUB_LEAGUE_MIN_MARKET_PROBABILITY) <= 0.05 + 1e-9


# ── --matchday-date / --post-friendly (2026-08-21) ─────────────────────────────────

def test_matchday_date_finds_match_by_et_matchday_not_utc_calendar_date(db_path, conn, capsys):
    # Kickoff at 01:00 UTC on the 23rd = 21:00 EDT on the 22nd -- same ET matchday
    # as the 22nd, even though its UTC calendar date is already the 23rd. A plain
    # UTC-date query would miss this; matchday_utc_window() must not.
    match_id = _seed_lopsided_league(conn)
    cur = conn.cursor()
    cur.execute("UPDATE soccer_matches SET match_date = ? WHERE match_id = ?",
               ("2025-09-23T01:00:00.000Z", match_id))
    conn.commit()

    out = _run_card(capsys, db_path, matchday_date="2025-09-22", dry_run=True)
    assert "MATCHES 1" in out
    assert "StrongFC vs WeakFC" in out


def test_matchday_date_rejects_more_than_two_dates():
    import subprocess
    result = subprocess.run(
        ["python3", "generate_club_league_card.py", "--league", "Serie A",
         "--matchday-date", "2025-09-22", "2025-09-23", "2025-09-24"],
        capture_output=True, text=True,
    )
    assert result.returncode != 0
    assert "takes 1 or 2 dates" in result.stderr


def test_matchday_date_range_covers_every_day_in_between(db_path, conn, capsys):
    # One match on 9/22, another on 9/24 -- a range of 9/22..9/25 must find both.
    _seed_lopsided_league(conn)
    strong2 = sports_db.ensure_soccer_team("StrongFC2", "Serie A")
    weak2 = sports_db.ensure_soccer_team("WeakFC2", "Serie A")
    match_id_2 = sports_db.add_soccer_match("Serie A", 2025, strong2, weak2, "2025-09-24T18:00:00.000Z")
    sports_db.add_soccer_betting_odds(
        match_id=match_id_2, sportsbook="Test", odds_date="2025-09-23",
        home_moneyline=-350, draw_moneyline=450, away_moneyline=2000,
        over_under=2.5, over_odds=-110, under_odds=-110,
    )

    out = _run_card(capsys, db_path, matchday_date=("2025-09-22", "2025-09-25"), dry_run=True)
    assert "MATCHES 2" in out


def test_post_friendly_output_is_team_names_and_pick_only(db_path, conn, capsys):
    match_id = _seed_lopsided_league(conn)
    cur = conn.cursor()
    cur.execute("UPDATE soccer_matches SET match_date = ? WHERE match_id = ?",
               ("2025-09-22T18:00:00.000Z", match_id))
    conn.commit()

    out = _run_card(capsys, db_path, matchday_date="2025-09-22", post_friendly=True)
    assert "StrongFC vs WeakFC" in out
    # The date label must be a clean date, not the raw ['2025-09-22'] list repr
    # --matchday-date's nargs="+" produces (found live 2026-08-21).
    assert "— 2025-09-22" in out
    assert "[" not in out
    # None of the detailed-report fields leak into the post-friendly output.
    assert "EV" not in out
    assert "odds" not in out
    assert "model p" not in out


def test_post_friendly_range_date_label_shows_both_dates(db_path, conn, capsys):
    match_id = _seed_lopsided_league(conn)
    cur = conn.cursor()
    cur.execute("UPDATE soccer_matches SET match_date = ? WHERE match_id = ?",
               ("2025-09-22T18:00:00.000Z", match_id))
    conn.commit()

    out = _run_card(capsys, db_path, matchday_date=("2025-09-20", "2025-09-25"), post_friendly=True)
    assert "— 2025-09-20 to 2025-09-25" in out
    assert "[" not in out


def test_post_friendly_distinguishes_no_matches_from_no_clearing_picks(db_path, conn, capsys):
    # No matches at all on this date -> a plain "no matches" message.
    out = _run_card(capsys, db_path, matchday_date="2025-09-22", post_friendly=True)
    assert "No matches today" in out

    # Real matches existed but none cleared the guardrail -- a different,
    # more informative message (found live 2026-08-21: with no distinction,
    # a real no-picks day was indistinguishable from a possible bug). Tested
    # directly against print_post_friendly() -- forcing a real match's odds to
    # genuinely clear zero EV isn't reliable to construct through the full
    # pipeline, and this is the exact boundary the fix is about.
    capsys.readouterr()   # clear the previous call's captured output
    gclc.print_post_friendly("Serie A", ["2025-09-22"], [], match_count=3)
    out = capsys.readouterr().out
    assert "No guardrail-clear picks today" in out
    assert "3 matches" in out


def test_post_friendly_still_stores_picks_unless_dry_run(db_path, conn, capsys):
    match_id = _seed_lopsided_league(conn)
    cur = conn.cursor()
    cur.execute("UPDATE soccer_matches SET match_date = ? WHERE match_id = ?",
               ("2025-09-22T18:00:00.000Z", match_id))
    conn.commit()

    _run_card(capsys, db_path, matchday_date="2025-09-22", post_friendly=True)
    cur.execute("SELECT COUNT(*) FROM soccer_club_league_picks")
    assert cur.fetchone()[0] > 0


# ── bare invocation must be a safe no-op (2026-08-21) ──────────────────────────────

def test_no_args_prints_help_and_does_not_write_picks(db_path, conn, capsys):
    with patch("generate_club_league_card.DATABASE_PATH", db_path):
        import sys
        old_argv = sys.argv
        sys.argv = ["generate_club_league_card.py"]
        try:
            gclc.main()
            raised = False
        except SystemExit as e:
            raised = True
            assert e.code == 0
        finally:
            sys.argv = old_argv

    assert raised
    out = capsys.readouterr().out
    # Not asserting the exact "usage: generate_club_league_card.py" prog name --
    # under pytest (python -m pytest), argparse derives prog from sys.orig_argv
    # rather than the sys.argv this test patches (a Python 3.14 argparse
    # behavior, confirmed in isolation), so that part of the real script's
    # output can't be reproduced faithfully here. What matters is that this is
    # the REAL, full help text (not a truncated/wrong one) and nothing ran.
    assert out.startswith("usage:")
    assert "--league" in out
    assert "--matchday-date" in out
    assert "--post-friendly" in out

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM soccer_club_league_picks")
    assert cur.fetchone()[0] == 0


# ── model version tracking (2026-08-21) ─────────────────────────────────────────────

def test_stored_picks_are_tagged_with_the_model_version(db_path, conn, capsys):
    match_id = _seed_lopsided_league(conn)
    cur = conn.cursor()
    cur.execute("UPDATE soccer_matches SET match_date = ? WHERE match_id = ?",
               ("2025-09-22T18:00:00.000Z", match_id))
    conn.commit()

    _run_card(capsys, db_path, matchday_date="2025-09-22")

    cur.execute("SELECT DISTINCT method FROM soccer_club_league_picks")
    methods = {row[0] for row in cur.fetchall()}
    assert methods == {gclc.CARD_MODEL_VERSION}
