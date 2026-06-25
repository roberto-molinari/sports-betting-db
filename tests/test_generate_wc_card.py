"""Tests for generate_wc_card pick selection (the longshot guardrail)."""

import sqlite3

import core.sports_db as sdb
import generate_wc_card as gwc


def _seed_longshot_game(db_path):
    """A clear favorite (home) vs a longshot underdog (away) priced at +2000, so
    the underdog has the highest EV purely from odds despite a tiny win prob."""
    fav = sdb.ensure_wc_team("Favoritia")
    dog = sdb.ensure_wc_team("Underdogia")
    match_id = sdb.ensure_wc_match("2026-06-20 18:00:00", fav, dog, stage="Group")
    sdb.upsert_wc_odds(
        match_id=match_id, sportsbook="Test", odds_date="2026-06-13",
        home_moneyline=-400, draw_moneyline=500, away_moneyline=2000,
        over_under=2.5, over_odds=-110, under_odds=-110,
    )
    sdb.set_wc_team_strength(fav, 2.0, 0.9)   # strong attack, mean defense
    sdb.set_wc_team_strength(dog, 0.7, 1.6)   # weak attack, leaky defense
    return match_id


def _fetch_match(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    matches = gwc.fetch_matches(conn, "2026-06-20", "2026-06-20")
    return conn, matches


def test_guardrail_demotes_sub_floor_longshot(db_path, monkeypatch):
    _seed_longshot_game(db_path)
    conn, matches = _fetch_match(db_path)
    assert len(matches) == 1
    match = matches[0]

    # With the floor disabled, the +2000 longshot wins on EV despite a tiny prob.
    monkeypatch.setattr(gwc, "MIN_PICK_PROBABILITY", 0.0)
    raw = gwc.best_pick_for_match(match, conn)
    assert raw["side"] == "AWAY"
    assert raw["prob"] < 0.25            # confirms it is a sub-floor longshot

    # With the 0.25 floor the chosen pick must clear the floor (any market).
    monkeypatch.setattr(gwc, "MIN_PICK_PROBABILITY", 0.25)
    guarded = gwc.best_pick_for_match(match, conn)
    assert guarded["side"] != "AWAY"
    assert guarded["prob"] >= 0.25
    conn.close()


def test_guardrail_demotes_sub_floor_draw(db_path, monkeypatch):
    """The floor applies to draws too: a longshot draw priced long enough to top
    the EV ranking is demoted, not surfaced."""
    fav = sdb.ensure_wc_team("Bigfav")
    dog = sdb.ensure_wc_team("Smalldog")
    match_id = sdb.ensure_wc_match("2026-06-21 18:00:00", fav, dog, stage="Group")
    sdb.upsert_wc_odds(
        match_id=match_id, sportsbook="Test", odds_date="2026-06-13",
        home_moneyline=-250, draw_moneyline=900, away_moneyline=600,
        over_under=2.5, over_odds=-200, under_odds=140,
    )
    sdb.set_wc_team_strength(fav, 2.0, 0.9)
    sdb.set_wc_team_strength(dog, 0.7, 1.6)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    match = gwc.fetch_matches(conn, "2026-06-21", "2026-06-21")[0]

    monkeypatch.setattr(gwc, "MIN_PICK_PROBABILITY", 0.0)
    raw = gwc.best_pick_for_match(match, conn)
    assert raw["side"] == "DRAW"
    assert raw["prob"] < 0.25            # a sub-floor longshot draw tops the EV

    monkeypatch.setattr(gwc, "MIN_PICK_PROBABILITY", 0.25)
    guarded = gwc.best_pick_for_match(match, conn)
    assert guarded["side"] != "DRAW"
    assert guarded["prob"] >= 0.25
    conn.close()


def test_guardrail_leaves_solid_picks_alone(db_path, monkeypatch):
    """A pick above the floor is untouched."""
    _seed_longshot_game(db_path)
    conn, matches = _fetch_match(db_path)
    match = matches[0]
    monkeypatch.setattr(gwc, "MIN_PICK_PROBABILITY", 0.01)
    pick = gwc.best_pick_for_match(match, conn)
    assert pick["side"] == "AWAY"
    conn.close()


def test_midnight_et_match_buckets_to_prior_day(db_path):
    """A 00:00 ET kickoff (04:00 UTC) belongs to the PRIOR day's matchday slate,
    not the next calendar date — the 4am-ET broadcast-day boundary."""
    home = sdb.ensure_wc_team("Austria")
    away = sdb.ensure_wc_team("Jordan")
    # 04:00 UTC Jun 17 == midnight ET Jun 17 -> should land on the Jun 16 slate
    match_id = sdb.ensure_wc_match("2026-06-17 04:00:00", home, away, stage="Group")
    sdb.upsert_wc_odds(
        match_id=match_id, sportsbook="Test", odds_date="2026-06-16",
        home_moneyline=-110, draw_moneyline=240, away_moneyline=300,
        over_under=2.5, over_odds=-110, under_odds=-110,
    )
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        ids_16 = [m["match_id"] for m in gwc.fetch_matches(conn, "2026-06-16", "2026-06-16")]
        ids_17 = [m["match_id"] for m in gwc.fetch_matches(conn, "2026-06-17", "2026-06-17")]
    finally:
        conn.close()
    assert match_id in ids_16        # midnight game groups with the prior day
    assert match_id not in ids_17


# ── selection guardrails: floor + market-disagreement cap (BUG-003) ───────────

def _cand(side, prob, implied, ev, odds=100):
    return {"side": side, "prob": prob, "implied": implied, "ev": ev, "odds": odds}


def test_select_pick_cap_demotes_overrated_underdog():
    """A dog the model rates >= 2x the market's implied prob is demoted by the cap
    (it clears the floor, so only the cap fires)."""
    cands = [
        _cand("AWAY", prob=0.40, implied=0.15, ev=1.50),      # 2.67x — over-rated dog
        _cand("OVER 2.5", prob=0.55, implied=0.52, ev=0.06),
    ]
    best = gwc.select_pick(cands)
    assert best["side"] == "OVER 2.5"
    away = next(c for c in cands if c["side"] == "AWAY")
    assert any("cap" in r for r in away["excluded_by"])
    assert not any("floor" in r for r in away["excluded_by"])
    assert away in best["demoted"]


def test_select_pick_cap_leaves_reasonable_underdog_alone():
    """A dog the model rates only modestly above the market (< 2x) is kept — a
    sound underdog value pick, not the noise-amplification trap."""
    cands = [
        _cand("AWAY", prob=0.40, implied=0.28, ev=0.44),      # 1.43x — below the cap
        _cand("OVER 2.5", prob=0.55, implied=0.52, ev=0.06),
    ]
    best = gwc.select_pick(cands)
    assert best["side"] == "AWAY"
    assert cands[0]["excluded_by"] == []


def test_select_pick_records_floor_and_cap_independently():
    """A candidate that trips BOTH gates records both reasons (no short-circuit)."""
    cands = [
        _cand("AWAY", prob=0.22, implied=0.10, ev=1.20),      # <0.25 floor AND >=2x cap
        _cand("HOME", prob=0.60, implied=0.55, ev=0.05),
    ]
    best = gwc.select_pick(cands)
    away = next(c for c in cands if c["side"] == "AWAY")
    assert len(away["excluded_by"]) == 2
    assert any("floor" in r for r in away["excluded_by"])
    assert any("cap" in r for r in away["excluded_by"])
    assert best["side"] == "HOME"


def test_select_pick_fallback_is_most_probable_side():
    """When nothing clears both gates, fall back to the most LIKELY side (highest
    model prob), not the highest EV."""
    cands = [
        _cand("AWAY", prob=0.20, implied=0.08, ev=1.50),      # excluded (floor + cap)
        _cand("DRAW", prob=0.24, implied=0.21, ev=0.10),      # excluded (floor only)
    ]
    best = gwc.select_pick(cands)
    assert best.get("fallback") is True
    assert best["side"] == "DRAW"                              # 0.24 > 0.20


def _seed_overrated_dog(db_path):
    """Away side is genuinely stronger (model makes it the favorite) but the book
    prices it a +700 underdog — a large model-vs-market gap that trips the cap."""
    home = sdb.ensure_wc_team("Homeside")
    away = sdb.ensure_wc_team("Roaddog")
    match_id = sdb.ensure_wc_match("2026-06-20 18:00:00", home, away, stage="Group")
    sdb.upsert_wc_odds(
        match_id=match_id, sportsbook="Test", odds_date="2026-06-13",
        home_moneyline=-900, draw_moneyline=600, away_moneyline=700,
        over_under=2.5, over_odds=-110, under_odds=-110,
    )
    sdb.set_wc_team_strength(home, 1.1, 1.5)   # weak attack, leaky defense
    sdb.set_wc_team_strength(away, 2.1, 0.85)  # strong attack, stingy defense


def test_cap_demotes_overrated_underdog_end_to_end(db_path):
    """best_pick_for_match populates implied prob and routes through the cap."""
    _seed_overrated_dog(db_path)
    conn, matches = _fetch_match(db_path)
    pick = gwc.best_pick_for_match(matches[0], conn)
    conn.close()
    assert pick["side"] != "AWAY"
    away = next(d for d in pick["demoted"] if d["side"] == "AWAY")
    assert any("cap" in r for r in away["excluded_by"])


# ── to-advance market (FEATURE-002) ──────────────────────────────────────────

def _seed_knockout(db_path, with_advance):
    home = sdb.ensure_wc_team("Favoritia")
    away = sdb.ensure_wc_team("Underdogia")
    match_id = sdb.ensure_wc_match("2026-06-30 18:00:00", home, away, stage="R32")
    adv = dict(home_advance_ml=-150, away_advance_ml=250) if with_advance else {}
    sdb.upsert_wc_odds(
        match_id=match_id, sportsbook="Test", odds_date="2026-06-29",
        home_moneyline=-250, draw_moneyline=600, away_moneyline=2000,
        over_under=2.5, over_odds=-110, under_odds=-110, **adv)
    sdb.set_wc_team_strength(home, 2.0, 0.9)   # heavy favorite
    sdb.set_wc_team_strength(away, 0.7, 1.6)
    return match_id


def _fetch_knockout(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    matches = gwc.fetch_matches(conn, "2026-06-30", "2026-06-30")
    return conn, matches


def test_advance_market_surfaces_when_odds_present(db_path):
    """A heavy favorite priced at -150 to advance is the best +EV pick."""
    _seed_knockout(db_path, with_advance=True)
    conn, matches = _fetch_knockout(db_path)
    pick = gwc.best_pick_for_match(matches[0], conn)
    conn.close()
    assert pick["side"] == "HOME ADVANCE"
    assert pick["prob"] > 0.5


def test_no_advance_candidate_without_advance_odds(db_path):
    """Safety invariant: no advance odds -> no ADVANCE candidate considered at all."""
    _seed_knockout(db_path, with_advance=False)
    conn, matches = _fetch_knockout(db_path)
    pick = gwc.best_pick_for_match(matches[0], conn)
    conn.close()
    assert "ADVANCE" not in pick["side"]
    assert all("ADVANCE" not in d["side"] for d in pick.get("demoted", []))


def test_display_pick_advance():
    assert gwc.display_pick("HOME ADVANCE", "Brazil", "Chile") == "Brazil to advance"
    assert gwc.display_pick("AWAY ADVANCE", "Brazil", "Chile") == "Chile to advance"


def test_card_passes_bench_index_into_advance(db_path):
    """The bench_indices arg flows through to advance_probs: a stronger home bench
    raises the modeled P(home advances)."""
    _seed_knockout(db_path, with_advance=True)
    conn, matches = _fetch_knockout(db_path)
    home_id = matches[0]["home_team_id"]
    base = gwc.best_pick_for_match(matches[0], conn)
    nudged = gwc.best_pick_for_match(matches[0], conn, bench_indices={home_id: 2.0})
    conn.close()
    assert base["side"] == "HOME ADVANCE" and nudged["side"] == "HOME ADVANCE"
    assert nudged["prob"] > base["prob"]
