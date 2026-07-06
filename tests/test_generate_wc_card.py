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

    # Isolate the BUG-003 floor/cap from FEATURE-009's mode-selection bars (tested
    # separately) by disabling value/prediction mode so the fallback-vs-value split
    # collapses back to the pre-FEATURE-009 "eligible -> highest EV" behavior.
    monkeypatch.setattr(gwc, "VALUE_MODE_MIN_PROBABILITY", 0.0)
    monkeypatch.setattr(gwc, "PREDICTION_MODE_MIN_IMPLIED_PROBABILITY", 1.1)
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

    monkeypatch.setattr(gwc, "VALUE_MODE_MIN_PROBABILITY", 0.0)
    monkeypatch.setattr(gwc, "PREDICTION_MODE_MIN_IMPLIED_PROBABILITY", 1.1)
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
    monkeypatch.setattr(gwc, "VALUE_MODE_MIN_PROBABILITY", 0.0)
    monkeypatch.setattr(gwc, "PREDICTION_MODE_MIN_IMPLIED_PROBABILITY", 1.1)
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


def _disable_mode_bars(monkeypatch):
    """These BUG-003 guardrail tests predate FEATURE-009's value/prediction mode
    bars and are scoped to guardrail behavior specifically; disable the mode bars
    so they isolate to the pre-FEATURE-009 "guardrail-clear -> highest EV, else
    fall back to most-likely" behavior regardless of where VALUE_MODE_MIN_PROBABILITY /
    PREDICTION_MODE_MIN_IMPLIED_PROBABILITY happen to be tuned."""
    monkeypatch.setattr(gwc, "VALUE_MODE_MIN_PROBABILITY", 0.0)
    monkeypatch.setattr(gwc, "PREDICTION_MODE_MIN_IMPLIED_PROBABILITY", 1.1)


def test_select_pick_cap_demotes_overrated_underdog(monkeypatch):
    """A dog the model rates >= 2x the market's implied prob is demoted by the cap
    (it clears the floor, so only the cap fires)."""
    _disable_mode_bars(monkeypatch)
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


def test_select_pick_cap_leaves_reasonable_underdog_alone(monkeypatch):
    """A dog the model rates only modestly above the market (< 2x) is kept — a
    sound underdog value pick, not the noise-amplification trap."""
    _disable_mode_bars(monkeypatch)
    cands = [
        _cand("AWAY", prob=0.40, implied=0.28, ev=0.44),      # 1.43x — below the cap
        _cand("OVER 2.5", prob=0.55, implied=0.52, ev=0.06),
    ]
    best = gwc.select_pick(cands)
    assert best["side"] == "AWAY"
    assert cands[0]["excluded_by"] == []


def test_select_pick_records_floor_and_cap_independently(monkeypatch):
    """A candidate that trips BOTH gates records both reasons (no short-circuit)."""
    _disable_mode_bars(monkeypatch)
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


def test_select_pick_fallback_is_most_probable_side(monkeypatch):
    """When nothing clears both gates, fall back to the most LIKELY side (highest
    model prob), not the highest EV."""
    _disable_mode_bars(monkeypatch)
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


# ── advance-edge cap: absolute-points guardrail for the to-advance market ─────

def test_select_pick_advance_edge_demotes_overrated_dog(monkeypatch):
    """An underdog to-advance pick the model rates well above market in ABSOLUTE
    points is demoted even when the 2x RATIO cap misses it (advance probs compress
    toward 0.5). Mirrors Paraguay: model 0.377 vs market 0.190 = 1.98x but +18.7 pts."""
    _disable_mode_bars(monkeypatch)
    cands = [
        _cand("AWAY ADVANCE", prob=0.377, implied=0.190, ev=0.97),   # 1.98x < cap, +18.7 pts
        _cand("OVER 2.5", prob=0.55, implied=0.52, ev=0.06),
    ]
    best = gwc.select_pick(cands)
    assert best["side"] == "OVER 2.5"
    dog = next(c for c in cands if c["side"] == "AWAY ADVANCE")
    assert dog["excluded_by"] == [r for r in dog["excluded_by"] if "advance-edge" in r]
    assert any("advance-edge" in r for r in dog["excluded_by"])
    assert not any("cap (" in r for r in dog["excluded_by"])   # ratio cap did NOT fire
    assert dog in best["demoted"]


def test_select_pick_advance_edge_keeps_small_gap(monkeypatch):
    """An advance dog within the absolute gap is kept (a sound knockout value pick)."""
    _disable_mode_bars(monkeypatch)
    cands = [
        _cand("AWAY ADVANCE", prob=0.32, implied=0.28, ev=0.14),   # +4 pts < 0.07
        _cand("OVER 2.5", prob=0.55, implied=0.52, ev=0.06),
    ]
    best = gwc.select_pick(cands)
    assert best["side"] == "AWAY ADVANCE"
    assert cands[0]["excluded_by"] == []


def test_select_pick_advance_edge_ignores_favorite(monkeypatch):
    """The advance-edge cap targets only the underdog side (market implied < 0.5);
    a favorite to advance is never tripped by it."""
    _disable_mode_bars(monkeypatch)
    cands = [
        _cand("HOME ADVANCE", prob=0.65, implied=0.55, ev=0.18),   # +10 pts but favorite
        _cand("OVER 2.5", prob=0.40, implied=0.52, ev=-0.10),
    ]
    best = gwc.select_pick(cands)
    assert best["side"] == "HOME ADVANCE"
    assert not any("advance-edge" in r for r in cands[0]["excluded_by"])


def test_select_pick_advance_edge_is_advance_only(monkeypatch):
    """A regular (non-advance) ML dog with the same absolute gap is NOT tripped by
    the advance-edge cap — it applies only to the to-advance market."""
    _disable_mode_bars(monkeypatch)
    cands = [
        _cand("AWAY", prob=0.40, implied=0.28, ev=0.44),   # +12 pts, but not an advance pick
        _cand("OVER 2.5", prob=0.55, implied=0.52, ev=0.06),
    ]
    best = gwc.select_pick(cands)
    assert best["side"] == "AWAY"
    assert not any("advance-edge" in r for r in cands[0]["excluded_by"])


# ── FEATURE-009: two-step selection (value / prediction / fallback modes) ────

def test_select_pick_value_mode_wins_when_bar_cleared():
    """A guardrail-clear candidate with model prob >= VALUE_MODE_MIN_PROBABILITY
    and positive EV is chosen by highest EV in value mode, even over a candidate
    with higher EV that doesn't clear the probability bar."""
    cands = [
        _cand("HOME", prob=0.62, implied=0.50, ev=0.20),
        _cand("OVER 2.5", prob=0.58, implied=0.52, ev=0.30),   # higher EV, below the value bar
    ]
    best = gwc.select_pick(cands)
    assert best["side"] == "HOME"
    assert best["mode"] == "value"


def test_select_pick_value_mode_requires_positive_ev():
    """A candidate that clears the probability bar but has non-positive EV does
    NOT qualify for value mode -- it falls through to prediction mode instead."""
    cands = [
        _cand("HOME", prob=0.65, implied=0.70, ev=-0.05),   # high-prob favorite, priced short
        _cand("AWAY", prob=0.30, implied=0.25, ev=0.10),
    ]
    best = gwc.select_pick(cands)
    assert best["mode"] == "prediction"
    assert best["side"] == "HOME"


def test_select_pick_prediction_mode_takes_best_payout_not_ev():
    """When step 1 finds nothing, prediction mode picks the best PAYOUT among
    genuinely-likely (implied >= PREDICTION_MODE_MIN_IMPLIED_PROBABILITY)
    candidates -- not the highest EV."""
    cands = [
        _cand("HOME", prob=0.50, implied=0.65, ev=0.05, odds=-160),          # better EV, worse payout
        _cand("AWAY ADVANCE", prob=0.45, implied=0.62, ev=-0.02, odds=110),  # worse EV, better payout
    ]
    best = gwc.select_pick(cands)
    assert best["mode"] == "prediction"
    assert best["side"] == "AWAY ADVANCE"


def test_select_pick_prediction_mode_ignores_guardrail_exclusion():
    """Prediction mode considers ALL candidates, including ones a BUG-003 guardrail
    excluded from value mode -- we've already stopped trusting the model's own
    ranking once step 1 fails."""
    cands = [
        _cand("HOME", prob=0.20, implied=0.65, ev=-0.30),   # below the 0.25 floor
        _cand("AWAY", prob=0.30, implied=0.25, ev=0.05),
    ]
    best = gwc.select_pick(cands)
    home = next(c for c in cands if c["side"] == "HOME")
    assert home["excluded_by"] != []          # guardrail-excluded
    assert best["side"] == "HOME"             # still chosen, via prediction mode
    assert best["mode"] == "prediction"


def test_select_pick_fallback_when_neither_mode_qualifies():
    """Neither value mode (no prob >= bar) nor prediction mode (no implied >= bar)
    qualifies -- fall back to the most likely side, same as the pre-FEATURE-009
    safety net."""
    cands = [
        _cand("HOME", prob=0.45, implied=0.40, ev=0.10),
        _cand("AWAY", prob=0.35, implied=0.30, ev=0.05),
    ]
    best = gwc.select_pick(cands)
    assert best["mode"] == "fallback"
    assert best.get("fallback") is True
    assert best["side"] == "HOME"    # 0.45 > 0.35


# ── BUG-004: knockout goal-level correction ──────────────────────────────────

def test_knockout_stage_scales_projected_lambda_down():
    """best_pick_for_match's venue-advantage computation (host_advantage x
    knockout_goal_scale) must yield a strictly smaller total for a knockout
    stage than for an identical Group-stage matchup, mirroring exactly what
    generate_wc_card.py itself composes at the call site."""
    h_att, h_def, a_att, a_def = 1.6, 1.1, 1.4, 1.2

    def total_lambda(stage):
        level = gwc.knockout_goal_scale(stage)
        home_adv = gwc.host_advantage("Homeland", stage) * level
        away_adv = gwc.host_advantage("Awayland", stage) * level
        r = gwc.analyse_match_wc(h_att, a_att, h_def, a_def,
                                 home_advantage=home_adv, away_advantage=away_adv)
        return r["lambda_H"] + r["lambda_A"]

    assert total_lambda("R32") < total_lambda("Group")
    assert total_lambda("R16") < total_lambda("Group")


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


# ── close-calls diagnostic: guardrail_excess + mode_breakdown ────────────────

def test_guardrail_excess_none_when_not_excluded(monkeypatch):
    _disable_mode_bars(monkeypatch)
    cands = [_cand("HOME", prob=0.60, implied=0.50, ev=0.10)]
    gwc.select_pick(cands)
    assert gwc.guardrail_excess(cands[0]) is None


def test_guardrail_excess_measures_floor_miss(monkeypatch):
    _disable_mode_bars(monkeypatch)
    # MIN_PICK_PROBABILITY defaults to 0.25; 0.24 misses by 0.01.
    cands = [_cand("HOME", prob=0.24, implied=0.50, ev=0.10),
              _cand("AWAY", prob=0.40, implied=0.35, ev=0.05)]
    gwc.select_pick(cands)
    assert round(gwc.guardrail_excess(cands[0]), 3) == 0.01


def test_guardrail_excess_takes_worst_of_multiple(monkeypatch):
    """A candidate tripping both floor and cap reports the LARGER excess (the
    harder-to-clear guardrail), not the smaller one."""
    _disable_mode_bars(monkeypatch)
    # floor: 0.25 - 0.10 = 0.15 excess. cap: prob >= 2x implied -> 0.10 >= 2*0.04=0.08,
    # excess = 0.10 - 0.08 = 0.02. Floor's excess (0.15) is larger.
    cands = [_cand("AWAY", prob=0.10, implied=0.04, ev=0.50),
              _cand("HOME", prob=0.60, implied=0.55, ev=0.05)]
    gwc.select_pick(cands)
    away = next(c for c in cands if c["side"] == "AWAY")
    assert round(gwc.guardrail_excess(away), 3) == 0.15


def test_mode_breakdown_tags_near_miss_and_includes_in_value():
    """A candidate excluded by the CAP guardrail within CLOSE_CALL_TOLERANCE of
    clearing it is tagged near_miss and still surfaces in the value list -- but
    select_pick's actual choice (guardrail-clear only) is unaffected. Uses the
    real default bars, since this is exactly the production scenario the
    diagnostic targets."""
    cands = [
        _cand("HOME", prob=0.62, implied=0.58, ev=0.15),   # guardrail-clear
        _cand("AWAY", prob=0.63, implied=0.31, ev=0.35),   # cap: 0.63 >= 2*0.31=0.62, excess 0.01
    ]
    best = gwc.select_pick(cands)
    assert best["side"] == "HOME"                          # AWAY still excluded -> unaffected pick
    away = next(c for c in cands if c["side"] == "AWAY")
    assert any("cap" in r for r in away["excluded_by"])

    breakdown = gwc.mode_breakdown(cands)
    assert away["near_miss"] is True
    assert breakdown["value"][0]["side"] == "AWAY"         # higher EV, shown despite exclusion
    assert breakdown["value"][1]["side"] == "HOME"


def test_mode_breakdown_excludes_far_miss(monkeypatch):
    """A guardrail-excluded candidate that misses by MORE than the tolerance is
    left out of the value list entirely, even if its EV is high."""
    _disable_mode_bars(monkeypatch)
    monkeypatch.setattr(gwc, "CLOSE_CALL_TOLERANCE", 0.02)
    cands = [
        _cand("HOME", prob=0.61, implied=0.55, ev=0.10),
        _cand("AWAY", prob=0.10, implied=0.08, ev=0.90),   # floor miss by 0.15 -- far miss
    ]
    gwc.select_pick(cands)
    breakdown = gwc.mode_breakdown(cands)
    away = next(c for c in cands if c["side"] == "AWAY")
    assert away["near_miss"] is False
    assert away not in breakdown["value"]


def test_mode_breakdown_ranks_each_mode_by_its_own_rule():
    """value ranks by EV, prediction by payout, fallback by model probability --
    three independent orderings over the same candidate pool. Uses the real
    default mode bars (0.60/0.60) since the point is to exercise them."""
    cands = [
        _cand("HOME", prob=0.65, implied=0.62, ev=0.05, odds=-140),
        _cand("AWAY", prob=0.61, implied=0.60, ev=0.20, odds=+120),
        _cand("OVER 2.5", prob=0.70, implied=0.55, ev=0.02, odds=-105),
    ]
    gwc.select_pick(cands)
    breakdown = gwc.mode_breakdown(cands)
    assert breakdown["value"][0]["side"] == "AWAY"          # highest EV (0.20)
    assert breakdown["prediction"][0]["side"] == "AWAY"      # best payout among implied>=0.60
    assert breakdown["fallback"][0]["side"] == "OVER 2.5"    # highest model prob (0.70)


def test_mode_breakdown_caps_at_top_n(monkeypatch):
    _disable_mode_bars(monkeypatch)
    cands = [_cand(f"CAND{i}", prob=0.9 - i * 0.05, implied=0.5, ev=0.01) for i in range(5)]
    gwc.select_pick(cands)
    breakdown = gwc.mode_breakdown(cands, top_n=3)
    assert len(breakdown["fallback"]) == 3


# ── top-EV diagnostic: why_not_value + mode_breakdown's top_ev list ──────────

def test_why_not_value_none_for_a_clean_value_candidate():
    cands = [_cand("HOME", prob=0.65, implied=0.55, ev=0.20)]
    gwc.select_pick(cands)
    assert gwc.why_not_value(cands[0]) is None


def test_why_not_value_reports_guardrail_reason_first():
    """A guardrail-excluded candidate's reason takes priority over the bar checks
    (it wouldn't even reach the value-mode bars in select_pick)."""
    cands = [
        _cand("AWAY", prob=0.10, implied=0.04, ev=0.90),   # floor-excluded
        _cand("HOME", prob=0.65, implied=0.55, ev=0.05),
    ]
    gwc.select_pick(cands)
    away = next(c for c in cands if c["side"] == "AWAY")
    assert "floor" in gwc.why_not_value(away)


def test_why_not_value_reports_sub_bar_when_guardrail_clear():
    """A guardrail-clear candidate below the value-mode probability bar is
    annotated with that specific reason, not a guardrail reason (none fired)."""
    cands = [_cand("OVER 2.5", prob=0.52, implied=0.50, ev=0.05)]
    gwc.select_pick(cands)
    reason = gwc.why_not_value(cands[0])
    assert "value bar" in reason and cands[0]["excluded_by"] == []


def test_why_not_value_reports_negative_ev():
    cands = [_cand("HOME", prob=0.65, implied=0.75, ev=-0.05)]
    gwc.select_pick(cands)
    assert gwc.why_not_value(cands[0]) == "EV not positive"


def test_mode_breakdown_top_ev_ranks_by_raw_ev_unfiltered():
    """top_ev ignores guardrails and mode bars entirely -- a guardrail-excluded
    longshot with the highest EV on the board still tops this list, unlike the
    value list, which would drop it."""
    cands = [
        _cand("AWAY ADVANCE", prob=0.377, implied=0.190, ev=0.97),   # advance-edge excluded
        _cand("HOME", prob=0.65, implied=0.60, ev=0.10),
    ]
    gwc.select_pick(cands)
    breakdown = gwc.mode_breakdown(cands)
    assert breakdown["top_ev"][0]["side"] == "AWAY ADVANCE"
    assert "advance-edge" in gwc.why_not_value(breakdown["top_ev"][0])
    # confirms it's genuinely absent from the value list despite topping top_ev
    assert all(c["side"] != "AWAY ADVANCE" for c in breakdown["value"])


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
