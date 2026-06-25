"""Tests for knockout_report analysis queries (seeded via record_knockout)."""

import core.sports_db as sdb
import record_knockout as rk
import knockout_report as kr


def _seed_tie(conn, hname, aname, reg, et, shootout, pick_prob, pks, etgoals):
    home = sdb.ensure_wc_team(hname)
    away = sdb.ensure_wc_team(aname)
    match_id = sdb.ensure_wc_match("2026-06-30 18:00:00", home, away, stage="R32")
    sdb.add_wc_pick(match_id, "2026-06-29T12:00:00", "HOME ADVANCE",
                    odds=-110, model_prob=pick_prob, ev=0.0, stars=2)
    rk.record(conn, (match_id, home, away, hname, aname),
              reg, et, shootout, "shootout", pks, etgoals)
    return home, away


def _seed(conn):
    # Brazil beat Croatia on penalties (Brazil = HOME advances -> HOME ADVANCE wins).
    bra, cro = _seed_tie(
        conn, "Brazil", "Croatia", (1, 1), (2, 2), (4, 3), pick_prob=0.55,
        pks=["home:Neymar:goal", "away:Modric:miss"],
        etgoals=["home:Vinicius:98", "away:Modric:100"])
    # Argentina lost to Chile on penalties (HOME ADVANCE loses).
    arg, chi = _seed_tie(
        conn, "Argentina", "Chile", (0, 0), (0, 0), (2, 4), pick_prob=0.60,
        pks=["home:Messi:goal"], etgoals=[])
    return {"bra": bra, "cro": cro, "arg": arg, "chi": chi}


def test_team_shootout_records(db_path, conn):
    ids = _seed(conn)
    rec = kr.team_shootout_records(conn)
    assert rec[ids["bra"]] == [1, 0]
    assert rec[ids["cro"]] == [0, 1]
    assert rec[ids["chi"]] == [1, 0]
    assert rec[ids["arg"]] == [0, 1]


def test_advance_calibration(db_path, conn):
    _seed(conn)
    overall, by_path = kr.advance_calibration(conn)
    assert overall["n"] == 2
    assert overall["mean_model_prob"] == (0.55 + 0.60) / 2
    assert overall["win_rate"] == 0.5      # one advanced, one didn't
    assert by_path["shootout"]["n"] == 2


def test_team_et_goals(db_path, conn):
    ids = _seed(conn)
    et = kr.team_et_goals(conn)
    assert et[ids["bra"]] == [1, 1]   # Vinicius for, Modric against
    assert et[ids["cro"]] == [1, 1]
    assert ids["arg"] not in et       # no extra-time goals in that tie


def test_player_penalty_conversion(db_path, conn):
    _seed(conn)
    conv = {name: (goals, attempts)
            for name, goals, attempts in kr.player_penalty_conversion(conn)}
    assert conv["Neymar"] == (1, 1)
    assert conv["Modric"] == (0, 1)   # missed his kick
    assert conv["Messi"] == (1, 1)
