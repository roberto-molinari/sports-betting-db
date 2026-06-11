"""
Unit tests for the pure logic in the World Cup scripts:
  - update_wc_results.grade_pick (settlement of each market)
  - compute_wc_team_strength.normalize_position / fifa_fallback
"""

import pytest

import update_wc_results as uwr
import compute_wc_team_strength as cws
from core.poisson_model import WC_BASELINE


# ── grade_pick ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize("side,home,away,expected", [
    ("HOME", 2, 0, "win"),
    ("HOME", 1, 1, "loss"),     # a draw loses the home moneyline
    ("HOME", 0, 1, "loss"),
    ("AWAY", 0, 2, "win"),
    ("AWAY", 1, 1, "loss"),
    ("DRAW", 1, 1, "win"),
    ("DRAW", 2, 1, "loss"),
    ("OVER 2.5", 2, 1, "win"),  # total 3 > 2.5
    ("OVER 2.5", 1, 1, "loss"), # total 2 < 2.5
    ("UNDER 3.5", 2, 0, "win"), # total 2 < 3.5
    ("UNDER 3.5", 3, 1, "loss"),# total 4 > 3.5
])
def test_grade_pick(side, home, away, expected):
    assert uwr.grade_pick(side, home, away) == expected


def test_grade_pick_integer_line_push():
    assert uwr.grade_pick("OVER 2", 1, 1) == "push"   # total exactly 2
    assert uwr.grade_pick("UNDER 3", 2, 1) == "push"  # total exactly 3


def test_grade_pick_unknown_side_raises():
    with pytest.raises(ValueError):
        uwr.grade_pick("PARLAY", 1, 0)


# ── normalize_position ───────────────────────────────────────────────────────

@pytest.mark.parametrize("raw,expected", [
    ("GK", "GK"), ("Goalkeeper", "GK"), ("G", "GK"),   # single-letter API codes
    ("Defender", "DEF"), ("CB", "DEF"), ("Right-Back", "DEF"), ("D", "DEF"),
    ("Midfielder", "MID"), ("CDM", "MID"), ("Winger", "MID"), ("M", "MID"),
    ("Forward", "FWD"), ("ST", "FWD"), ("Striker", "FWD"), ("F", "FWD"),
    ("", None), (None, None), ("Coach", None),
])
def test_normalize_position(raw, expected):
    assert cws.normalize_position(raw) == expected


# ── fifa_fallback ────────────────────────────────────────────────────────────

def test_fifa_fallback_better_rank_is_stronger():
    field = list(range(1, 49))                       # a 48-team field
    top_att, top_def = cws.fifa_fallback(1, field)
    mid_att, mid_def = cws.fifa_fallback(25, field)
    assert top_att > mid_att            # better rank -> more attack
    assert top_def < mid_def            # better rank -> less conceded
    assert top_att > WC_BASELINE > top_def


def test_fifa_fallback_is_field_relative_not_global():
    """The same global rank maps differently depending on the field: #24 is
    mid-pack in an elite 48-team field (near baseline), but strong in a weak
    field. This is the whole point of field-relative re-referencing."""
    elite_field = list(range(1, 49))                 # ranks 1..48 (strong)
    weak_field = list(range(24, 72))                 # ranks 24..71 (24 is best)
    att_elite, _ = cws.fifa_fallback(24, elite_field)
    att_weak, _ = cws.fifa_fallback(24, weak_field)
    assert abs(att_elite - WC_BASELINE) < 0.1        # mid-field -> ~baseline
    assert att_weak > att_elite                      # best-in-field -> strong


def test_fifa_fallback_missing_rank_is_baseline():
    assert cws.fifa_fallback(None, [1, 2, 3]) == (WC_BASELINE, WC_BASELINE)


# ── raw_team_strength (goals/90 attack signal) ───────────────────────────────

def _player(pos, attack_rate, club_xga, minutes=2700, league="Premier League"):
    return {"pos": pos, "league": league, "minutes": minutes,
            "attack_rate": attack_rate, "club_xga_per90": club_xga}


def test_raw_team_strength_attack_tracks_attack_rate():
    """A squad of higher-scoring players yields a higher raw attack value."""
    weak = [_player("FWD", 0.2, 1.2), _player("MID", 0.1, 1.2)]
    strong = [_player("FWD", 0.8, 1.2), _player("MID", 0.4, 1.2)]
    assert cws.raw_team_strength(strong)[0] > cws.raw_team_strength(weak)[0]


def test_raw_team_strength_defense_tracks_club_xga():
    """Lower club xGA among defenders yields a lower (better) raw defense value."""
    stingy = [_player("GK", 0.0, 0.7), _player("DEF", 0.05, 0.8)]
    leaky = [_player("GK", 0.0, 1.6), _player("DEF", 0.05, 1.7)]
    assert cws.raw_team_strength(stingy)[2] < cws.raw_team_strength(leaky)[2]


def test_raw_team_strength_skips_unknown_position_and_missing_data():
    """Players with no position or no attack_rate contribute no attack weight."""
    players = [_player(None, 0.9, 1.0), _player("FWD", None, 1.0)]
    raw_attack, attack_w, _, _ = cws.raw_team_strength(players)
    assert attack_w == 0
    assert raw_attack is None


# ── compute_strengths: manual FIFA overrides are sticky ──────────────────────

def _covered_squad():
    """A squad with enough weighted minutes to clear the stat-coverage gates."""
    return [_player("FWD", 0.8, 1.0), _player("MID", 0.5, 1.0),
            _player("DEF", 0.1, 0.9), _player("GK", 0.0, 0.9)]


def test_compute_strengths_override_pins_to_fifa(monkeypatch):
    """A team in FIFA_OVERRIDES takes the FIFA fallback even with full stat
    coverage, so a recompute never overwrites a manual override with
    player-aggregation values. An identical non-overridden team uses stats."""
    monkeypatch.setattr(cws, "FIFA_OVERRIDES", {"Pinned": "test reason"})
    teams = {
        1: {"name": "Pinned", "fifa": 8, "players": _covered_squad()},
        2: {"name": "Normal", "fifa": 20, "players": _covered_squad()},
    }
    field = [8, 20]
    out = cws.compute_strengths(teams)
    assert out[1]["basis"].startswith("fifa-override")
    assert (out[1]["lambda_attack"], out[1]["lambda_defense"]) == cws.fifa_fallback(8, field)
    assert out[2]["basis"] == "stats"   # same squad, not overridden -> stat-based


# ── apply_shrinkage: sample-size blend toward positional prior ────────────────

def test_apply_shrinkage_pulls_thin_samples_not_full_seasons():
    """A thin/extreme sample is pulled toward the positional prior; a full-season
    player keeps (close to) his own rate."""
    teams = {
        1: {"name": "Full", "fifa": None, "players": [
            {"pos": "FWD", "league": "Premier League", "minutes": 2700,
             "attack_rate": 0.80, "club_xga_per90": 1.0}]},
        2: {"name": "Thin", "fifa": None, "players": [
            {"pos": "FWD", "league": "Premier League", "minutes": 300,
             "attack_rate": 0.00, "club_xga_per90": 1.0}]},
    }
    prior = cws.positional_priors(teams, "attack_rate")["FWD"]   # (2700*.8)/3000 = .72
    cws.apply_shrinkage(teams, k_minutes=900)
    full = teams[1]["players"][0]["attack_rate"]
    thin = teams[2]["players"][0]["attack_rate"]
    assert 0.70 < full < 0.80                       # 2700 min: own rate dominates
    assert thin > 0.0                               # lifted off the spurious zero
    assert abs(thin - prior) < abs(0.0 - prior)     # moved toward the prior
