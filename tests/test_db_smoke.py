"""
Smoke tests for core.sports_db.

These initialise a fresh schema in a temp DB, insert a few sample rows through
the public helpers, and assert counts and round-trip reads. This is the
"initialise the DB, insert a couple of sample rows, assert counts" check called
out in ARCHITECTURE.md, plus coverage of the dedupe/upsert behaviour that the
import scripts rely on.
"""

import sqlite3

import pytest

from core import sports_db


# ── Schema ───────────────────────────────────────────────────────────────────

EXPECTED_TABLES = {
    "soccer_teams", "soccer_matches", "soccer_betting_odds",
    "nhl_teams", "nhl_matches", "nhl_betting_odds",
}


def test_init_database_creates_all_tables(db_path, conn):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cur.fetchall()}
    assert EXPECTED_TABLES <= tables


def test_init_database_is_idempotent(db_path):
    # Running it twice must not raise or duplicate anything.
    sports_db.init_database()
    sports_db.init_database()


# ── Soccer round trip ────────────────────────────────────────────────────────

def test_soccer_full_round_trip(db_path):
    home = sports_db.ensure_soccer_team("Inter", "Serie A", country="Italy")
    away = sports_db.ensure_soccer_team("Juventus", "Serie A", country="Italy")
    assert home != away

    match_id = sports_db.add_soccer_match("Serie A", 2024, home, away, "2025-02-01")
    sports_db.update_soccer_match_result(match_id, 3, 1, halftime_home=1, halftime_away=0)
    sports_db.add_soccer_betting_odds(
        match_id, "Bet365", "2025-01-31",
        home_moneyline=-140, draw_moneyline=260, away_moneyline=380,
    )

    matches = sports_db.get_soccer_matches(league="Serie A", season=2024)
    assert len(matches) == 1
    row = matches[0]
    assert row["home_team_name"] == "Inter"
    assert row["away_team_name"] == "Juventus"
    assert row["home_score"] == 3
    assert row["away_score"] == 1
    assert row["halftime_home_score"] == 1
    assert row["match_status"] == "completed"


def test_ensure_soccer_team_is_idempotent(db_path):
    first = sports_db.ensure_soccer_team("Napoli", "Serie A")
    second = sports_db.ensure_soccer_team("Napoli", "Serie A")
    assert first == second
    assert sports_db.get_soccer_team_id("Napoli") == first
    assert sports_db.get_soccer_team_id("Nonexistent") is None


def test_get_soccer_matches_filters_by_status(db_path):
    home = sports_db.ensure_soccer_team("Roma", "Serie A")
    away = sports_db.ensure_soccer_team("Lazio", "Serie A")
    played = sports_db.add_soccer_match("Serie A", 2024, home, away, "2025-02-01")
    sports_db.update_soccer_match_result(played, 1, 1)
    sports_db.add_soccer_match("Serie A", 2024, away, home, "2025-05-01")  # scheduled

    completed = sports_db.get_soccer_matches(status="completed")
    scheduled = sports_db.get_soccer_matches(status="scheduled")
    assert len(completed) == 1
    assert len(scheduled) == 1


# ── NHL round trip + dedupe ──────────────────────────────────────────────────

def test_nhl_full_round_trip(db_path):
    home = sports_db.ensure_nhl_team("Boston Bruins")
    away = sports_db.ensure_nhl_team("Toronto Maple Leafs")
    match_id = sports_db.add_nhl_match(2025, home, away, "2025-11-01")
    sports_db.update_nhl_match_result(match_id, 4, 2)
    sports_db.add_nhl_betting_odds(
        match_id, "DraftKings", "2025-10-31",
        home_moneyline=-160, away_moneyline=135,
    )

    matches = sports_db.get_nhl_matches(season=2025)
    assert len(matches) == 1
    assert matches[0]["home_team_name"] == "Boston Bruins"
    assert matches[0]["home_score"] == 4
    assert matches[0]["match_status"] == "completed"


def test_add_nhl_match_dedupes_on_natural_key(db_path):
    """Re-adding the same (season, home, away, date) must reuse the match_id
    rather than create a duplicate — the import scripts depend on this."""
    home = sports_db.ensure_nhl_team("New York Rangers")
    away = sports_db.ensure_nhl_team("New Jersey Devils")
    first = sports_db.add_nhl_match(2025, home, away, "2025-12-01", status="scheduled")
    second = sports_db.add_nhl_match(2025, home, away, "2025-12-01", status="completed")
    assert first == second

    cur = sqlite3.connect(db_path).cursor()
    cur.execute("SELECT COUNT(*) FROM nhl_matches")
    assert cur.fetchone()[0] == 1
    # Status was upgraded on the existing row.
    matches = sports_db.get_nhl_matches(season=2025)
    assert matches[0]["match_status"] == "completed"


def test_canonical_nhl_team_name_normalises_alias(db_path):
    """The Montréal alias should resolve to a single canonical team."""
    a = sports_db.ensure_nhl_team("Montreal Canadiens")   # ascii feed spelling
    b = sports_db.ensure_nhl_team("Montréal Canadiens")   # canonical
    assert a == b
    assert sports_db.get_nhl_team_id("Montreal Canadiens") == a


# ── World Cup tables ─────────────────────────────────────────────────────────

WC_TABLES = {
    "soccer_wc_teams", "soccer_wc_players", "soccer_wc_player_stats",
    "soccer_wc_matches", "soccer_wc_odds", "soccer_wc_team_strength",
    "soccer_wc_picks",
}


def test_init_database_creates_wc_tables(db_path, conn):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in cur.fetchall()}
    assert WC_TABLES <= tables


def test_ensure_wc_team_is_idempotent(db_path):
    first = sports_db.ensure_wc_team("Brazil", "CONMEBOL", 5)
    second = sports_db.ensure_wc_team("Brazil", "CONMEBOL", 5)
    assert first == second
    assert sports_db.get_wc_team_id("Brazil") == first
    assert sports_db.get_wc_team_id("Nowhere") is None


def test_wc_player_and_stats_round_trip(db_path, conn):
    team = sports_db.ensure_wc_team("Brazil", "CONMEBOL", 5)
    p1 = sports_db.add_wc_player(team, "Vinicius Jr", position="FWD",
                                 club="Real Madrid", club_league="La Liga")
    # Re-adding the same player for the team reuses the row.
    assert sports_db.add_wc_player(team, "Vinicius Jr") == p1

    first = sports_db.upsert_wc_player_stats(p1, season=2025, minutes_played=2700,
                                             xg=15.0, xg_per90=0.5, goals=18, assists=9,
                                             club_xga_per90=1.0, source="stub")
    # Upsert on the same (player, season) updates rather than duplicates.
    again = sports_db.upsert_wc_player_stats(p1, season=2025, minutes_played=2800,
                                             xg=16.0, xg_per90=0.51, source="stub")
    assert first == again
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM soccer_wc_player_stats WHERE player_id = ?", (p1,))
    assert cur.fetchone()[0] == 1
    cur.execute("SELECT minutes_played, club_xga_per90 FROM soccer_wc_player_stats WHERE stat_id = ?",
                (first,))
    row = cur.fetchone()
    assert row[0] == 2800            # updated minutes
    assert row[1] == pytest.approx(1.0)  # club xGA preserved across update path


def test_ensure_wc_match_dedupes_on_fixture(db_path, conn):
    home = sports_db.ensure_wc_team("Brazil")
    away = sports_db.ensure_wc_team("Serbia")
    first = sports_db.ensure_wc_match("2026-06-11T19:00:00", home, away,
                                      stage="group", grp="G")
    second = sports_db.ensure_wc_match("2026-06-11T19:00:00", home, away,
                                       stage="group", grp="G")
    assert first == second
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM soccer_wc_matches")
    assert cur.fetchone()[0] == 1


def test_wc_odds_upsert_updates_in_place(db_path, conn):
    home = sports_db.ensure_wc_team("Brazil")
    away = sports_db.ensure_wc_team("Serbia")
    match_id = sports_db.ensure_wc_match("2026-06-11T19:00:00", home, away)
    sports_db.upsert_wc_odds(match_id, "DraftKings", "2026-06-10",
                             home_moneyline=-200, draw_moneyline=320, away_moneyline=600,
                             over_under=3.5, over_odds=-110, under_odds=-110)
    sports_db.upsert_wc_odds(match_id, "DraftKings", "2026-06-11",
                             home_moneyline=-180, draw_moneyline=300, away_moneyline=550,
                             over_under=3.0, over_odds=-105, under_odds=-115)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM soccer_wc_odds WHERE match_id = ?", (match_id,))
    assert cur.fetchone()[0] == 1   # updated, not duplicated
    cur.execute("SELECT home_moneyline, over_under FROM soccer_wc_odds WHERE match_id = ?",
                (match_id,))
    assert cur.fetchone() == (-180, 3.0)


def test_wc_team_strength_keeps_versions_and_returns_latest(db_path, conn):
    team = sports_db.ensure_wc_team("Brazil")
    sports_db.set_wc_team_strength(team, 2.1, 0.8, notes="initial")
    sports_db.set_wc_team_strength(team, 2.3, 0.7, notes="post group stage")
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM soccer_wc_team_strength WHERE team_id = ?", (team,))
    assert cur.fetchone()[0] == 2   # both versions retained
    assert sports_db.get_latest_wc_strength(team) == (2.3, 0.7)
    assert sports_db.get_latest_wc_strength(999) is None


def test_wc_pick_add_and_grade(db_path, conn):
    home = sports_db.ensure_wc_team("Brazil")
    away = sports_db.ensure_wc_team("Serbia")
    match_id = sports_db.ensure_wc_match("2026-06-11T19:00:00", home, away)
    pick_id = sports_db.add_wc_pick(match_id, "2026-06-10T12:00:00", "UNDER 3.5",
                                    odds=-110, model_prob=0.63, ev=0.21, stars=3)
    sports_db.set_wc_pick_result(pick_id, "win")
    cur = conn.cursor()
    cur.execute("SELECT side, stars, result FROM soccer_wc_picks WHERE pick_id = ?",
                (pick_id,))
    assert cur.fetchone() == ("UNDER 3.5", 3, "win")


def test_replace_wc_pick_supersedes_ungraded(db_path, conn):
    home = sports_db.ensure_wc_team("France")
    away = sports_db.ensure_wc_team("Spain")
    match_id = sports_db.ensure_wc_match("2026-06-12T19:00:00", home, away)
    # first run: model picks DRAW
    sports_db.replace_wc_pick(match_id, "2026-06-11T10:00:00", "DRAW",
                              odds=240, model_prob=0.28, ev=0.05, stars=1)
    # re-run after a model improvement now picks OVER 2.5 -> should replace, not stack
    sports_db.replace_wc_pick(match_id, "2026-06-11T11:00:00", "OVER 2.5",
                              odds=-110, model_prob=0.55, ev=0.05, stars=1)
    cur = conn.cursor()
    cur.execute("SELECT side FROM soccer_wc_picks WHERE match_id = ?", (match_id,))
    assert cur.fetchall() == [("OVER 2.5",)]   # one row, latest pick wins


def test_replace_wc_pick_preserves_graded(db_path, conn):
    home = sports_db.ensure_wc_team("Italy")
    away = sports_db.ensure_wc_team("Croatia")
    match_id = sports_db.ensure_wc_match("2026-06-12T19:00:00", home, away)
    pick_id = sports_db.replace_wc_pick(match_id, "2026-06-11T10:00:00", "HOME",
                                        odds=-130, model_prob=0.6, ev=0.1, stars=2)
    sports_db.set_wc_pick_result(pick_id, "win")
    # a later re-run must not wipe an already-graded pick (locked history)
    sports_db.replace_wc_pick(match_id, "2026-06-11T11:00:00", "AWAY",
                              odds=300, model_prob=0.3, ev=0.05, stars=1)
    cur = conn.cursor()
    cur.execute("SELECT side, result FROM soccer_wc_picks WHERE match_id = ? ORDER BY pick_id",
                (match_id,))
    rows = cur.fetchall()
    assert ("HOME", "win") in rows   # graded pick retained
    assert len(rows) == 2            # graded kept, new ungraded added alongside


def test_wc_pick_override_round_trip_and_grade(db_path, conn):
    """A user override records the model snapshot + reason, leaves the model pick
    untouched, and grades independently on its own user_side."""
    home = sports_db.ensure_wc_team("Norway")
    away = sports_db.ensure_wc_team("Senegal")
    match_id = sports_db.ensure_wc_match("2026-06-22T20:00:00", home, away)
    sports_db.add_wc_pick(match_id, "2026-06-21T12:00:00", "AWAY",
                          odds=235, model_prob=0.34, ev=0.14, stars=2)
    oid = sports_db.add_wc_pick_override(
        match_id, user_side="OVER 2.5", user_odds=-112, reason="Haaland hot; model form-blind",
        category="form", model_side="AWAY", model_odds=235)
    sports_db.set_wc_override_result(oid, "win")
    cur = conn.cursor()
    cur.execute("""SELECT model_side, user_side, category, reason, result
                   FROM soccer_wc_pick_overrides WHERE override_id = ?""", (oid,))
    assert cur.fetchone() == ("AWAY", "OVER 2.5", "form", "Haaland hot; model form-blind", "win")
    # the model's own pick row is untouched (single source of truth)
    cur.execute("SELECT COUNT(*) FROM soccer_wc_picks WHERE match_id = ?", (match_id,))
    assert cur.fetchone()[0] == 1


def test_wc_pick_override_supersedes_ungraded(db_path, conn):
    home = sports_db.ensure_wc_team("Argentina")
    away = sports_db.ensure_wc_team("Austria")
    match_id = sports_db.ensure_wc_match("2026-06-22T13:00:00", home, away)
    sports_db.add_wc_pick_override(match_id, "UNDER 2.5", -115, "feels like a 2-0 game")
    sports_db.add_wc_pick_override(match_id, "DRAW", 330, "changed my mind")
    cur = conn.cursor()
    cur.execute("SELECT user_side FROM soccer_wc_pick_overrides WHERE match_id = ?", (match_id,))
    assert cur.fetchall() == [("DRAW",)]   # ungraded prior replaced, not stacked
