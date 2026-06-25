"""
Unit tests for the pure logic in the World Cup scripts:
  - compute_wc_team_strength.normalize_position / fifa_fallback

(Market settlement now lives in core.grading — see tests/test_grading.py.)
"""

from statistics import mean, pstdev

import pytest

import compute_wc_team_strength as cws
import core.sports_db as sdb
from core.poisson_model import WC_BASELINE


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


# ── bench_attack (FEATURE-002 bench nudge) ───────────────────────────────────

def test_bench_attack_uses_players_below_top_11():
    """The 11 most-played are the proxy XI; the bench is everyone else."""
    starters = [_player("FWD", 0.2, 1.2, minutes=3000) for _ in range(11)]
    subs = [_player("FWD", 0.9, 1.2, minutes=500),
            _player("MID", 0.7, 1.2, minutes=400)]
    bench = cws.bench_attack(starters + subs)
    full = cws.raw_team_strength(starters + subs)[0]
    # The bench here (high-rate subs) out-scores the starter-heavy full squad.
    assert bench is not None and bench > full


def test_bench_attack_none_when_no_bench():
    eleven = [_player("FWD", 0.5, 1.2) for _ in range(11)]   # exactly an XI, no bench
    assert cws.bench_attack(eleven) is None


def _seed_team_squad(team_name, bench_goals):
    """11 modest starters (most minutes) + one low-minute bench forward whose scoring
    sets the bench index."""
    tid = sdb.ensure_wc_team(team_name)
    for i in range(11):
        pid = sdb.add_wc_player(tid, f"{team_name}_s{i}", position="MID",
                                club="C", club_league="Premier League")
        sdb.upsert_wc_player_stats(pid, season=2025, minutes_played=3000,
                                   goals=10, club_ga_per90=1.2, source="t")
    pid = sdb.add_wc_player(tid, f"{team_name}_bench", position="FWD",
                            club="C", club_league="Premier League")
    sdb.upsert_wc_player_stats(pid, season=2025, minutes_played=900,
                               goals=bench_goals, club_ga_per90=1.2, source="t")
    return tid


def test_compute_bench_indices_centered_and_ordered(db_path, conn):
    strong = _seed_team_squad("Deepland", bench_goals=30)    # prolific bench forward
    weak = _seed_team_squad("Shallowland", bench_goals=2)    # weak bench forward
    idx = cws.compute_bench_indices(conn)
    assert idx[strong] > idx[weak]            # deeper bench -> higher index
    assert abs(idx[strong] + idx[weak]) < 1e-9  # field-centered (mean 0)


# ── attack league exponent (BUG-002) ─────────────────────────────────────────

def test_attack_applies_league_exponent_discount():
    """A weak-league scorer is discounted by league_factor**exponent, i.e. harder
    than the plain linear factor (top leagues at factor 1.0 are unaffected)."""
    lf = cws.league_factor("MLS")                      # < 1.0
    ra, _, _, _ = cws.raw_team_strength([_player("FWD", 1.0, 1.0, league="MLS")])
    assert ra == pytest.approx(lf ** cws.ATTACK_LEAGUE_EXPONENT)
    assert ra < lf                                     # harder than linear


def test_attack_exponent_leaves_top_league_untouched():
    ra, _, _, _ = cws.raw_team_strength(
        [_player("FWD", 0.8, 1.0, league="Premier League")])
    assert ra == pytest.approx(0.8)                    # 1.0**1.5 == 1.0


# ── defense league exponent (BUG-001) ────────────────────────────────────────

def test_defense_applies_softened_league_exponent():
    """A weak-league defender's concede rate is marked up by league_factor**exponent
    with exponent < 1 — softer than the full ÷ league_factor division, but still a
    markup vs the raw rate."""
    lf = cws.league_factor("MLS")                      # < 1.0
    _, _, rd, _ = cws.raw_team_strength([_player("GK", 0.0, 1.5, league="MLS")])
    assert rd == pytest.approx(1.5 / (lf ** cws.DEFENSE_LEAGUE_EXPONENT))
    assert rd < 1.5 / lf                               # softer than full division
    assert rd > 1.5                                    # still marked up vs raw rate


def test_defense_exponent_leaves_top_league_untouched():
    _, _, rd, _ = cws.raw_team_strength(
        [_player("GK", 0.0, 1.2, league="Premier League")])
    assert rd == pytest.approx(1.2)                    # 1.0**0.5 == 1.0


def test_compute_strengths_normalizes_attack_to_target_spread(monkeypatch):
    """Attack is normalized to baseline mean AND the fixed ATTACK_LAMBDA_SD spread,
    regardless of the raw distribution's spread. Tested with the FIFA blend off so
    it isolates the stat-normalization step (the blend has its own test)."""
    monkeypatch.setattr(cws, "FIFA_BLEND_WEIGHT", 0.0)
    teams = {
        i: {"name": f"T{i}", "fifa": 30, "players": [
            _player("FWD", r, 1.0), _player("MID", 0.5, 1.0),
            _player("DEF", 0.1, 0.9), _player("GK", 0.0, 0.9)]}
        for i, r in enumerate([0.2, 0.4, 0.6, 0.8, 1.0, 1.2])
    }
    out = cws.compute_strengths(teams)
    atts = [out[t]["lambda_attack"] for t in out]
    assert all(out[t]["basis"] == "stats" for t in out)
    assert mean(atts) == pytest.approx(WC_BASELINE)
    assert pstdev(atts) == pytest.approx(cws.ATTACK_LAMBDA_SD)


def test_compute_strengths_blends_stats_toward_fifa(monkeypatch):
    """With the blend on, two squads with IDENTICAL stats but different FIFA ranks
    diverge: the better-ranked team is pulled to a stronger attack and stingier
    defense, recovering pedigree the club aggregate alone would miss."""
    monkeypatch.setattr(cws, "FIFA_BLEND_WEIGHT", 0.3)
    teams = {
        1: {"name": "TopRank", "fifa": 2, "players": _covered_squad()},
        2: {"name": "LowRank", "fifa": 47, "players": _covered_squad()},
    }
    out = cws.compute_strengths(teams)
    assert out[1]["basis"].startswith("stats+fifa")
    assert out[1]["lambda_attack"] > out[2]["lambda_attack"]   # better rank -> more attack
    assert out[1]["lambda_defense"] < out[2]["lambda_defense"]  # better rank -> less conceded


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
    assert out[2]["basis"].startswith("stats")  # same squad, not overridden -> stat-based


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
