"""
Tests for backfill_club_xga.py's compute_match_team_xg -- the OTHER half of "is team
xG just a (possibly buggy) sum": get_team_xg_ratings sums a team's OWN players' xg
into their attack rating (tested in test_compute_club_player_strength.py); this file
sums the OPPOSING team's players' xg into club_xga_per90 (a team's defense/xGA
rating). No test previously existed for this function at all.
"""

import pytest

from core import sports_db
from backfill_club_xga import compute_match_team_xg


def _seed_match(conn, home_name="Home", away_name="Away", season=2025, date="2025-09-01"):
    home = sports_db.ensure_soccer_team(home_name, "Serie A")
    away = sports_db.ensure_soccer_team(away_name, "Serie A")
    match_id = sports_db.add_soccer_match("Serie A", season, home, away, date)
    return home, away, match_id


def test_sums_every_player_on_a_side_not_just_one(db_path, conn):
    """A team's per-match xG total (keyed by (match_id, venue)) must sum ALL of that
    side's players, not just whichever row happens to be seen first/last."""
    team, opp, m1 = _seed_match(conn, "TeamA", "OppA")
    p1 = sports_db.add_player(opp, "OppStriker", position="F", conn=conn)
    p2 = sports_db.add_player(opp, "OppWinger", position="F", conn=conn)
    sports_db.add_player_match_stats(p1, m1, season=2025, venue="away", minutes_played=90, xg=0.9, conn=conn)
    sports_db.add_player_match_stats(p2, m1, season=2025, venue="away", minutes_played=90, xg=0.6, conn=conn)

    totals = compute_match_team_xg(conn, 2025)
    assert totals[(m1, "away")] == pytest.approx(0.9 + 0.6)


def test_ignores_players_with_no_xg_data_rather_than_zeroing_the_team(db_path, conn):
    """SUM() ignores NULL rows -- a player with no shots recorded (e.g. a keeper)
    must not zero out or otherwise corrupt teammates' recorded xg."""
    team, opp, m1 = _seed_match(conn, "TeamA", "OppA")
    p1 = sports_db.add_player(opp, "OppStriker", position="F", conn=conn)
    p2 = sports_db.add_player(opp, "OppKeeper", position="G", conn=conn)
    sports_db.add_player_match_stats(p1, m1, season=2025, venue="away", minutes_played=90, xg=1.1, conn=conn)
    sports_db.add_player_match_stats(p2, m1, season=2025, venue="away", minutes_played=90, xg=None, conn=conn)

    totals = compute_match_team_xg(conn, 2025)
    assert totals[(m1, "away")] == pytest.approx(1.1)


def test_home_and_away_totals_are_tracked_separately(db_path, conn):
    """(match_id, venue) is the group key -- the two sides of the SAME match must not
    be summed together or overwrite each other."""
    team, opp, m1 = _seed_match(conn, "TeamA", "OppA")
    p_home = sports_db.add_player(team, "HomeStriker", position="F", conn=conn)
    p_away = sports_db.add_player(opp, "AwayStriker", position="F", conn=conn)
    sports_db.add_player_match_stats(p_home, m1, season=2025, venue="home", minutes_played=90, xg=2.0, conn=conn)
    sports_db.add_player_match_stats(p_away, m1, season=2025, venue="away", minutes_played=90, xg=0.5, conn=conn)

    totals = compute_match_team_xg(conn, 2025)
    assert totals[(m1, "home")] == pytest.approx(2.0)
    assert totals[(m1, "away")] == pytest.approx(0.5)


def test_writes_opponents_total_not_own_total_into_club_xga_per90(db_path, conn):
    """End-to-end: a player's club_xga_per90 must equal the OPPOSING side's summed
    xG for that match, not their own team's -- the actual bug class this exists to
    catch (own/opponent mixed up would silently produce a team's OWN xG as its
    defensive rating instead of what they conceded)."""
    team, opp, m1 = _seed_match(conn, "TeamA", "OppA")
    p_home = sports_db.add_player(team, "HomeStriker", position="F", conn=conn)
    p_away = sports_db.add_player(opp, "AwayStriker", position="F", conn=conn)
    sports_db.add_player_match_stats(p_home, m1, season=2025, venue="home", minutes_played=90, xg=2.0, conn=conn)
    sports_db.add_player_match_stats(p_away, m1, season=2025, venue="away", minutes_played=90, xg=0.5, conn=conn)

    team_xg = compute_match_team_xg(conn, 2025)
    opponent_venue = {"home": "away", "away": "home"}
    cur = conn.cursor()
    cur.execute("SELECT stat_id, match_id, venue FROM soccer_player_stats WHERE season = 2025")
    for stat_id, match_id, venue in cur.fetchall():
        opp_xg = team_xg[(match_id, opponent_venue[venue])]
        cur.execute("UPDATE soccer_player_stats SET club_xga_per90 = ? WHERE stat_id = ?", (opp_xg, stat_id))
    conn.commit()

    cur.execute("SELECT club_xga_per90 FROM soccer_player_stats WHERE player_id = ?", (p_home,))
    assert cur.fetchone()[0] == pytest.approx(0.5)  # home player's xGA = AWAY side's xG
    cur.execute("SELECT club_xga_per90 FROM soccer_player_stats WHERE player_id = ?", (p_away,))
    assert cur.fetchone()[0] == pytest.approx(2.0)  # away player's xGA = HOME side's xG
