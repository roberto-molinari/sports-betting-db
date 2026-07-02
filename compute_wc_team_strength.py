"""
Aggregate squad club stats into per-team World Cup strength lambdas.

Reads soccer_wc_players + soccer_wc_player_stats and produces, per national team:
  - lambda_attack  : expected goals scored  (higher = stronger attack)
  - lambda_defense : expected goals conceded (lower  = stronger defense)

Approach (v1 — this is the intended iteration surface; weights and factors below
are tunable constants):

  lambda_attack  = minutes- and position-weighted average of players' club
                   xg_per90, league-quality adjusted, then normalized so the
                   average team sits at WC_BASELINE.
  lambda_defense = minutes- and position-weighted average of players' CLUB-TEAM
                   defensive rate (club_xga_per90), league adjusted and
                   normalized to WC_BASELINE. This is why club_xga_per90 is
                   fetched per player — without it defense has no real signal.

Stat-based teams are then lightly blended toward their FIFA-rank estimate
(FIFA_BLEND_WEIGHT) to recover national-team pedigree the club aggregate misses.
Teams whose squads lack usable stat coverage fall back entirely to the
FIFA-ranking-derived estimate (labeled as such in the notes).

Usage:
    python compute_wc_team_strength.py --print            # show, don't store
    python compute_wc_team_strength.py --persist          # write a new version
    python compute_wc_team_strength.py --persist --notes "post group stage"
"""

import argparse
import sqlite3
from statistics import mean, pstdev

from core.sports_db import DATABASE_PATH, set_wc_team_strength
from core.poisson_model import WC_BASELINE


# ── Tunable constants ────────────────────────────────────────────────────────

# Position weights for attack and defense aggregation.
ATTACK_POS_WEIGHTS  = {"FWD": 1.0, "MID": 0.6, "DEF": 0.2, "GK": 0.0}
DEFENSE_POS_WEIGHTS = {"GK": 1.0, "DEF": 0.8, "MID": 0.3, "FWD": 0.1}

# A player's club rate (goals/90, club xGA/90) is a noisy estimate, so we blend
# it toward the typical rate for his position, weighted by minutes played: with
# few or unrepresentative minutes the positional prior dominates; with a full
# track record his own rate does. This reins in small-sample outliers on BOTH
# tails (e.g. a star reading 0.00 from a partial season, or a domestic striker
# inflated by weak opposition) without disturbing well-sampled players. K is the
# half-trust point: at this many minutes a player's own rate and the positional
# prior are weighted 50/50. ~900 min ≈ 10 matches (about half a season). This is
# set from how football works, not by fitting the betting market.
K_SHRINK_MINUTES = 900.0

# Attack uses the league factor raised to this power (BUG-002). A weak-league
# goal is cheap for two compounding reasons — the league is less competitive AND
# the defenders are weaker — but a single league_factor only captures one. The
# exponent applies the discount more than once: top leagues are unaffected
# (1.0^x = 1.0, so genuine elite scorers are untouched) while weak-league scoring
# is progressively discounted. 1.5 (not 2.0) keeps mid-tier leagues like Liga MX
# from being over-penalized; the exact value isn't theory-derivable, so it's the
# conservative choice that fixes the over-rated teams with least collateral.
ATTACK_LEAGUE_EXPONENT = 1.5

# Defense uses the mirror idea but in the SOFTENING direction. A club concede rate
# from a weak league is marked up (divided by the league factor) because it was
# earned against weak attacks — but the full division over-amplifies: 2.1 ÷ 0.62
# (MLS) = 3.4 treats the entire league gap as extra goals conceded, when defensive
# organization and individual quality carry across leagues. An exponent BELOW 1.0
# applies only PART of that markup (÷ lf**0.5), keeping the right direction without
# inflating weak-league sides (Qatar, Gulf/African teams, Canada) into fake
# leakiness. See BUG-001. (Attack hardens its discount; defense softens its markup.)
DEFENSE_LEAGUE_EXPONENT = 0.5

# Target spread (standard deviation) of the normalized attack lambdas. We
# normalize attack to a fixed mean (WC_BASELINE) AND this spread, rather than the
# mean alone. Pinning only the mean lets the spread float with the raw
# distribution, so any change (like the exponent above) can blow the spread out
# and re-inflate the top teams. Fixing both keeps Brazil ~2.3 and the field at a
# realistic ~3x range regardless. Set to the post-shrinkage attack spread so the
# exponent change is a pure redistribution, not a spread shift.
ATTACK_LAMBDA_SD = 0.41

# League quality multiplier (1.0 = top tier). A goal in a weaker league predicts
# less international scoring, so its rate is discounted. Keys are the exact
# competition names TheStatsAPI returns (see soccer_wc_players.club_league).
# Values are subjective quality tiers — easy to tune; ordering matters most.
LEAGUE_FACTORS = {
    # Top-5 European + elite continental
    "Premier League": 1.00, "LaLiga": 1.00, "Bundesliga": 0.97,
    "Serie A": 0.96, "Ligue 1": 0.90,
    "UEFA Champions League": 1.00, "UEFA Europa League": 0.90,
    "UEFA Conference League": 0.80, "Club World Championship": 0.95,
    "CONMEBOL Libertadores": 0.80,
    # Strong European leagues
    "Liga Portugal Betclic": 0.82, "Eredivisie": 0.80, "Pro League": 0.78,
    "Brasileirão Série A": 0.78, "Championship": 0.75, "Trendyol Süper Lig": 0.72,
    "Russian Premier League": 0.68, "Swiss Super League": 0.68,
    "Austrian Bundesliga": 0.66, "Scottish Premiership": 0.65,
    "Stoiximan Super League": 0.63, "Czech First League": 0.62,
    "Danish Superliga": 0.62, "HNL": 0.60, "Eliteserien": 0.60,
    "Allsvenskan": 0.58, "Mozzart Bet Superliga": 0.58, "NB I": 0.55,
    "Cypriot First Division": 0.55, "Israeli Premier League": 0.55,
    # Second divisions / domestic cups (map to the country's tier)
    "2. Bundesliga": 0.62, "LaLiga 2": 0.60, "Serie B": 0.60,
    "Liga Portugal 2": 0.55, "Ligue 2": 0.58, "League One": 0.50,
    "National League": 0.35, "3. Liga": 0.50, "Superettan": 0.45,
    "DFB Pokal": 0.95, "Coupe de France": 0.88, "Copa Argentina": 0.78,
    "Scottish Cup": 0.62, "Russian Cup": 0.66,
    # Americas
    "Liga Profesional de Fútbol": 0.78, "Liga MX, Clausura": 0.68,
    "MLS": 0.62, "Primera A, Apertura": 0.62, "LigaPro Serie A": 0.58,
    "USL Championship": 0.45, "CONCACAF Champions Cup": 0.60,
    # Asia / Middle East / Africa / Oceania
    "Saudi Pro League": 0.62, "AFC Champions League Elite": 0.62,
    "K League 1": 0.65, "J1 League": 0.65, "J2 League": 0.50,
    "Persian Gulf Pro League": 0.52, "UAE Pro League": 0.52,
    "Stars League": 0.50, "AFC Champions League Two": 0.50,
    "A-League Men": 0.55,
    "South African Premier Division": 0.52, "CAF Champions League": 0.58,
    # Egypt's WC players are concentrated at Al Ahly/Zamalek/Pyramids — elite in
    # the non-European game (record CAF titles, Club World Cup competitive) — so
    # the league-wide tail under-sells them; nudged toward the MLS/Saudi tier.
    "Egyptian Premier League": 0.60,
    "Tunisian Ligue Professionnelle 1": 0.50, "Algerian Ligue 1": 0.50,
    "Indonesia Liga 1": 0.40,
    # Youth competitions (a player whose primary comp is youth = thin senior data)
    "UEFA Youth League": 0.45,
}
DEFAULT_LEAGUE_FACTOR = 0.60

# A team needs at least this much weighted coverage to use stat-based lambdas;
# otherwise it falls back to the FIFA-ranking estimate.
MIN_ATTACK_WEIGHT = 1000.0   # ~ sum(minutes * pos_weight) across contributors
MIN_DEFENSE_WEIGHT = 1000.0

# FIFA-ranking fallback: how far lambdas spread around WC_BASELINE between the
# strongest and weakest team IN THE FIELD. The fallback ranks the 48 WC teams
# against each other (not the global FIFA pool) and centers on the field median,
# so the mid-field team sits at baseline and only genuine extremes get the full
# spread. best-in-field -> attack +FIFA_SPREAD / defense -FIFA_SPREAD.
FIFA_SPREAD = 0.6

# How far a stats-based team's lambdas are pulled toward its FIFA-rank estimate
# (0 = pure club-stat aggregation, 1 = pure FIFA rank). Club-stat aggregation
# can't see national-team pedigree that doesn't show up in club numbers: it
# underrates cohesive sides whose players don't pile up club goals (Morocco, #11,
# rated below Scotland) and overrates teams whose players post flattering club
# stats (Scotland, #33). A light blend corrects that systematic bias. It is kept
# SMALL on purpose because the opposite failure also exists: where club stats
# correctly capture individual talent that FIFA rank lags (Haaland's Norway, #39),
# the blend wrongly drags them down. 0.2 keeps most of the club-stat signal (our
# edge) while nudging the field toward national-team reality. Overrides and
# thin-coverage fallbacks are already 100% FIFA, so the blend only touches the
# stats-based teams.
FIFA_BLEND_WEIGHT = 0.2

# Teams MANUALLY pinned to the FIFA-ranking fallback. These are known
# data-failures where club-stat aggregation is wrong for a reason we understand
# (not merely thin coverage, which the MIN_*_WEIGHT gates already handle). Unlike
# auto-fallback, an override is sticky: every --persist re-derives its lambdas
# from FIFA rank and never lets a player-aggregation recompute overwrite it, so
# the per-team `method` stays correct across reruns. Keep the reason for audit;
# remove a team here once its underlying data issue is fixed.
FIFA_OVERRIDES = {
    "Belgium": "player-agg attack collapsed (Lukaku injury minutes); squad far too strong for net-rank #28",
    "Uzbekistan": "weak-league inflation put net-rank #7, ahead of Argentina/Portugal",
    "South Korea": "goals/90 misses the squad's quality — Son's unrepresentative 0-goal MLS sample plus creators/defenders (Kim Min-jae, Lee Kang-in) who don't rack up club goals; net-rank #34 vs mid-field FIFA",
    "Egypt": "goals/90 buries the squad's quality — Salah's Liverpool output is diluted across an Egyptian-league squad, leaving net attack 1.01 (below baseline) for a #32 side the market makes a clear favorite (e.g. ~60% vs New Zealand); pin to FIFA rank (BUG-005)",
    "Côte d'Ivoire": "BUG-005 poster child — talent-rich club scorers (Bundesliga/Ligue 1) inflate attack to ~Germany tier (1.59) for a #34 international side, so the model read CIV a coin-flip vs Germany (market a +445 dog); pin to FIFA rank",
    "Jordan": "weak-league inflation over-credits a #62 squad — attack 1.15 (near baseline) reads Jordan a ~30% live dog vs Algeria, whom the market makes a clear ~63% favorite (1.89x over market, just under the cap); pin to FIFA rank (BUG-005, same family as Uzbekistan)",
    "Mexico": "BUG-001 — club-concede overstates national leakiness: 3 group clean sheets (incl. vs a decent South Korea) show a defense (FIFA 1.13) far stingier than the blend's 1.35. Pinned to FIFA #17 (2026-06-30). CAVEAT: the full pin also drops the attack 1.69->1.57, which the group goals (6 in 3) do NOT support — a defense-only / per-component blend would be cleaner; revisit when that capability is built.",
}

# Per-team, per-COMPONENT blend weight overrides (targeted BUG-001/BUG-005 fix).
# A full FIFA_OVERRIDES pin forces w=1.0 on BOTH attack and defense together, but
# results sometimes argue for moving only one lambda toward FIFA (e.g. a team's
# defense is genuinely mis-modeled while its attack matches the club-stat number).
# {team_name: {"attack": w, "defense": w}} — a component absent from the inner
# dict falls back to the global FIFA_BLEND_WEIGHT. Only applies to stats-based
# teams (has_attack and has_defense); FIFA_OVERRIDES and thin-coverage fallback
# both take precedence and are untouched by this. Keep the reason for audit.
FIFA_BLEND_OVERRIDES = {
    "Algeria": {
        "defense": 0.7,
        # rationale (2026-07-02, bumped from 0.5): actual GA (7 in 3) far exceeds
        # the w=0.2 blend's proxy (4.14), and the leak includes a concession to
        # FIFA-pinned Jordan (weak team) — not just quality opponents, so it reads
        # as genuine leakiness, not opponent-quality confound. More FIFA weight
        # (#43, a weak-ish rank) moves defense the RIGHT direction (leakier).
        # Attack is left at the global weight: Algeria's actual attack (5 in 3)
        # exceeds the blend's proxy (4.07) too, so pulling attack further toward
        # FIFA (which would lower it) moves the WRONG direction — only defense
        # is bumped.
    },
    "Switzerland": {
        "attack": 0.5,
        # rationale (2026-07-02): actual goals (7 in 3) far exceed the w=0.2
        # blend's proxy (4.95), the field's 2nd-worst attack under-projection.
        # Some of this is a confound (2 of 3 opponents, Canada + Bosnia, are
        # independently documented as genuinely leaky defenses), so it is not
        # treated as fully genuine — a moderate w=0.5 (not a full pin) pulls
        # attack toward FIFA's #18-implied number (1.541) without assuming the
        # entire surge reflects true Swiss attacking quality. Defense left at the
        # global weight: no comparable calibration problem there (gap +0.79).
    },
}


# Includes single-letter codes used by TheStatsAPI (G/D/M/F).
_MID_CODES = {"m", "mf", "cm", "dm", "am", "cdm", "cam", "rm", "lm", "mid"}
_DEF_CODES = {"d", "df", "cb", "lb", "rb", "wb", "rwb", "lwb", "def"}
_FWD_CODES = {"f", "fw", "fwd", "st", "cf", "ss", "rw", "lw"}


def normalize_position(pos):
    """Map free-form position text to one of GK / DEF / MID / FWD (or None).

    Handles full words ('Midfielder'), common codes ('CDM'), and the
    single-letter codes G/D/M/F that TheStatsAPI returns. Order matters:
    midfield is checked before defense so 'defensive midfielder' resolves to
    MID, and 'back' catches the full/centre/wing-back variants.
    """
    if not pos:
        return None
    p = pos.strip().lower()
    if "goal" in p or p in {"gk", "g"}:
        return "GK"
    if "midfield" in p or "winger" in p or p in _MID_CODES:
        return "MID"
    if "back" in p or "defen" in p or p in _DEF_CODES:
        return "DEF"
    if "forward" in p or "striker" in p or "attack" in p or p in _FWD_CODES:
        return "FWD"
    return None


def league_factor(league):
    return LEAGUE_FACTORS.get(league, DEFAULT_LEAGUE_FACTOR)


def load_team_players(conn):
    """Return {team_id: (name, fifa_ranking, [player_stat_rows])}."""
    cur = conn.cursor()
    cur.execute("SELECT team_id, name, fifa_ranking FROM soccer_wc_teams")
    teams = {tid: {"name": name, "fifa": fifa, "players": []}
             for tid, name, fifa in cur.fetchall()}

    cur.execute("""
        SELECT p.team_id, p.position, p.club_league,
               s.minutes_played, s.xg_per90, s.goals, s.club_xga_per90, s.club_ga_per90
        FROM soccer_wc_players p
        JOIN soccer_wc_player_stats s ON s.player_id = p.player_id
    """)
    for row in cur.fetchall():
        team_id, position, league, minutes, xg90, goals, xga90, ga90 = row
        if team_id in teams:
            minutes = minutes or 0
            goals_per90 = (goals / minutes * 90) if (goals is not None and minutes) else None
            teams[team_id]["players"].append({
                "pos": normalize_position(position),
                "league": league,
                "minutes": minutes,
                # Attack rate: real xG/90 when available (the later xG pass),
                # otherwise goals/90 from the season endpoint.
                "attack_rate": xg90 if xg90 is not None else goals_per90,
                "club_xga_per90": xga90 if xga90 is not None else ga90,
            })
    return teams


def positional_priors(teams, field):
    """Minutes-weighted mean of a player rate `field` per position, across all
    squads. Serves as the 'typical value for this position' the shrinkage pulls
    thin/extreme samples toward."""
    num, den = {}, {}
    for info in teams.values():
        for p in info["players"]:
            pos, val, mins = p["pos"], p.get(field), p["minutes"]
            if pos and val is not None and mins:
                num[pos] = num.get(pos, 0.0) + mins * val
                den[pos] = den.get(pos, 0.0) + mins
    return {pos: num[pos] / den[pos] for pos in num}


def apply_shrinkage(teams, k_minutes=K_SHRINK_MINUTES):
    """Blend each player's club rate toward its positional prior, weighted by
    minutes (empirical-Bayes style):

        adjusted = (minutes * own_rate + k * prior) / (minutes + k)

    Mutates the player dicts in `teams` in place. Applied to both the attack
    rate and the club-defense rate. Players with no value or no minutes for a
    field are left untouched (they already carry no aggregation weight)."""
    for field in ("attack_rate", "club_xga_per90"):
        prior = positional_priors(teams, field)
        for info in teams.values():
            for p in info["players"]:
                pos, val, mins = p["pos"], p.get(field), p["minutes"]
                if pos in prior and val is not None and mins:
                    p[field] = (mins * val + k_minutes * prior[pos]) / (mins + k_minutes)
    return teams


def raw_team_strength(players):
    """Return (raw_attack, attack_weight, raw_defense, defense_weight).

    raw_* are weighted averages on the per-90 scale (not yet normalized to
    WC_BASELINE). Weight is sum(minutes * position_weight) so we can decide
    whether coverage is sufficient.
    """
    a_num = a_w = d_num = d_w = 0.0
    for pl in players:
        pos = pl["pos"]
        if pos is None:
            continue
        lf = league_factor(pl["league"])
        # Attack: player's own scoring rate (xG/90 or goals/90), league-discounted.
        # The league factor is raised to ATTACK_LEAGUE_EXPONENT so weak-league
        # scoring is discounted harder than its competitive-strength factor alone
        # (BUG-002); top leagues (factor 1.0) are unaffected.
        if pl["attack_rate"] is not None:
            w = pl["minutes"] * ATTACK_POS_WEIGHTS.get(pos, 0.0)
            if w > 0:
                a_num += w * pl["attack_rate"] * (lf ** ATTACK_LEAGUE_EXPONENT)
                a_w += w
        # Defense: club-team concession rate, marked up for weaker leagues (a low
        # xGA vs weak opposition overstates real solidity). DEFENSE_LEAGUE_EXPONENT
        # < 1 applies only part of that markup, so the full ÷ league_factor doesn't
        # over-amplify weak-league concede rates into fake leakiness (BUG-001).
        if pl["club_xga_per90"] is not None:
            w = pl["minutes"] * DEFENSE_POS_WEIGHTS.get(pos, 0.0)
            if w > 0:
                d_num += w * (pl["club_xga_per90"] / (lf ** DEFENSE_LEAGUE_EXPONENT))
                d_w += w
    raw_attack = (a_num / a_w) if a_w > 0 else None
    raw_defense = (d_num / d_w) if d_w > 0 else None
    return raw_attack, a_w, raw_defense, d_w


BENCH_XI_SIZE = 11   # proxy starting XI: the 11 most-played squad members


def bench_attack(players):
    """Raw attack rate of a squad's bench — its members ranked BELOW the top-11 by club
    minutes (a proxy XI). Reuses raw_team_strength's attack weighting, so it is on the same
    scale as the team's attack. Returns None when the bench carries no real attack weight.
    """
    ranked = sorted(players, key=lambda p: p["minutes"] or 0, reverse=True)
    bench = ranked[BENCH_XI_SIZE:]
    raw_attack, a_w, _, _ = raw_team_strength(bench)
    return raw_attack if a_w >= 1.0 else None


def compute_bench_indices(conn=None):
    """Return {team_id: bench_index} — each team's bench attack, field-centered, for use as
    the extra-time / shootout nudge in advance_probs (stronger bench -> positive index, weaker
    -> negative). Teams with no computable bench get a neutral 0.0. Mirrors the strength
    pipeline's shrinkage so bench rates are comparable across teams.
    """
    own = conn is None
    if own:
        conn = sqlite3.connect(DATABASE_PATH)
    try:
        teams = load_team_players(conn)
        apply_shrinkage(teams)
        raw = {tid: bench_attack(info["players"]) for tid, info in teams.items()}
        present = [v for v in raw.values() if v is not None]
        mean_bench = mean(present) if present else 0.0
        return {tid: (v - mean_bench if v is not None else 0.0) for tid, v in raw.items()}
    finally:
        if own:
            conn.close()


def fifa_fallback(fifa_rank, field_ranks):
    """Derive (lambda_attack, lambda_defense) from a team's FIFA rank, scaled by
    its position WITHIN this field rather than the global FIFA pool.

    FIFA rank is measured against all ~210 nations, but the World Cup field is
    the elite slice, so its true middle is a much better global rank than ~50.
    We rank the field's teams against each other (best = 1 ... worst = N) and
    center on the median: the mid-field team lands at WC_BASELINE, and only the
    genuine extremes get the full +/-FIFA_SPREAD. `field_ranks` is the FIFA rank
    of every team in the field.
    """
    ranks = sorted(r for r in field_ranks if r)
    if not fifa_rank or len(ranks) <= 1:
        return WC_BASELINE, WC_BASELINE
    pos = ranks.index(fifa_rank) + 1          # 1 = strongest in the field
    n = len(ranks)
    offset = FIFA_SPREAD * (n + 1 - 2 * pos) / (n - 1)   # +SPREAD..-SPREAD, 0 at median
    return WC_BASELINE + offset, WC_BASELINE - offset


def compute_strengths(teams):
    """Return {team_id: {name, lambda_attack, lambda_defense, basis}}.

    Stat-based raw values are normalized to WC_BASELINE: defense to the baseline
    mean, attack to the baseline mean AND a fixed spread (ATTACK_LAMBDA_SD).
    Thin-coverage teams and manual overrides use the FIFA fallback.
    """
    raw = {}
    for tid, info in teams.items():
        ra, aw, rd, dw = raw_team_strength(info["players"])
        raw[tid] = {"ra": ra, "aw": aw, "rd": rd, "dw": dw}

    attack_vals = [r["ra"] for tid, r in raw.items()
                   if r["ra"] is not None and r["aw"] >= MIN_ATTACK_WEIGHT]
    defense_vals = [r["rd"] for tid, r in raw.items()
                    if r["rd"] is not None and r["dw"] >= MIN_DEFENSE_WEIGHT]
    attack_mean = mean(attack_vals) if attack_vals else None
    attack_sd = pstdev(attack_vals) if len(attack_vals) > 1 else 0.0
    attack_scale = (WC_BASELINE / attack_mean) if attack_mean else None
    defense_scale = (WC_BASELINE / mean(defense_vals)) if defense_vals else None

    def normalize_attack(ra):
        """Mean-AND-spread normalization: re-center to WC_BASELINE and rescale to
        ATTACK_LAMBDA_SD, so the field's attack spread stays realistic regardless
        of the raw distribution. Falls back to mean-only when the field is too
        small to estimate a spread (e.g. tiny test fixtures)."""
        if attack_sd > 1e-9:
            return WC_BASELINE + (ra - attack_mean) * (ATTACK_LAMBDA_SD / attack_sd)
        return ra * attack_scale

    # FIFA ranks of the whole field, so the fallback can rank teams against each
    # other (field-relative) rather than against the global FIFA pool.
    field_ranks = [info["fifa"] for info in teams.values()]

    out = {}
    for tid, info in teams.items():
        r = raw[tid]
        # Manual override wins over everything: pin to the FIFA fallback and never
        # recompute from player stats, so the decision survives every rerun.
        if info["name"] in FIFA_OVERRIDES:
            la, ld = fifa_fallback(info["fifa"], field_ranks)
            out[tid] = {
                "name": info["name"],
                "lambda_attack": la,
                "lambda_defense": ld,
                "basis": f"fifa-override(rank={info['fifa']}): {FIFA_OVERRIDES[info['name']]}",
            }
            continue
        has_attack = r["ra"] is not None and r["aw"] >= MIN_ATTACK_WEIGHT and attack_scale
        has_defense = r["rd"] is not None and r["dw"] >= MIN_DEFENSE_WEIGHT and defense_scale
        if has_attack and has_defense:
            # Normalized club-stat lambdas, then lightly blended toward the team's
            # FIFA-rank estimate to recover national-team pedigree the club
            # aggregate misses (both are WC_BASELINE-centered, so the blend keeps
            # the field mean at baseline — it only redistributes strength).
            la_stats = normalize_attack(r["ra"])
            ld_stats = r["rd"] * defense_scale
            overrides = FIFA_BLEND_OVERRIDES.get(info["name"], {})
            w_att = overrides.get("attack", FIFA_BLEND_WEIGHT)
            w_def = overrides.get("defense", FIFA_BLEND_WEIGHT)
            if w_att > 0 or w_def > 0:
                fa, fd = fifa_fallback(info["fifa"], field_ranks)
                la = (1 - w_att) * la_stats + w_att * fa
                ld = (1 - w_def) * ld_stats + w_def * fd
                basis = (f"stats+fifa{w_att:g}" if w_att == w_def
                         else f"stats+fifa(att={w_att:g},def={w_def:g})")
            else:
                la, ld, basis = la_stats, ld_stats, "stats"
            out[tid] = {
                "name": info["name"],
                "lambda_attack": la,
                "lambda_defense": ld,
                "basis": basis,
            }
        else:
            la, ld = fifa_fallback(info["fifa"], field_ranks)
            out[tid] = {
                "name": info["name"],
                "lambda_attack": la,
                "lambda_defense": ld,
                "basis": f"fifa-fallback(rank={info['fifa']})",
            }
    return out


def parse_args():
    parser = argparse.ArgumentParser(description="Compute WC team strength lambdas.")
    parser.add_argument("--persist", action="store_true",
                        help="Write a new strength version per team to the DB.")
    parser.add_argument("--print", dest="show", action="store_true",
                        help="Print computed lambdas (default if --persist is absent).")
    parser.add_argument("--notes", default="v1 aggregation",
                        help="Label stored with persisted strength rows.")
    parser.add_argument("--no-shrink", action="store_true",
                        help="Skip the sample-size shrinkage of player rates "
                             "(use raw club rates). For before/after comparison.")
    return parser.parse_args()


def main():
    args = parse_args()
    show = args.show or not args.persist

    conn = sqlite3.connect(DATABASE_PATH)
    teams = load_team_players(conn)
    conn.close()

    if not teams:
        print("No teams in soccer_wc_teams — import squads first.")
        return

    if not args.no_shrink:
        apply_shrinkage(teams)

    strengths = compute_strengths(teams)

    if show:
        print(f"{'TEAM':<22} {'ATT':>6} {'DEF':>6}  BASIS")
        for tid in sorted(strengths, key=lambda t: -strengths[t]["lambda_attack"]):
            s = strengths[tid]
            print(f"{s['name']:<22} {s['lambda_attack']:>6.2f} "
                  f"{s['lambda_defense']:>6.2f}  {s['basis']}")
        n_override = sum(1 for s in strengths.values() if s["basis"].startswith("fifa-override"))
        n_fallback = sum(1 for s in strengths.values() if s["basis"].startswith("fifa-fallback"))
        print(f"\n{len(strengths)} teams  "
              f"({n_override} manual FIFA override, {n_fallback} auto FIFA fallback)")

    if args.persist:
        for tid, s in strengths.items():
            method = "fifa_ranking" if s["basis"].startswith("fifa") else "player_aggregation"
            note = f"{args.notes} [{s['basis']}]"
            set_wc_team_strength(tid, s["lambda_attack"], s["lambda_defense"],
                                 method=method, notes=note)
        print(f"\nPersisted {len(strengths)} strength rows (notes: {args.notes!r}).")


if __name__ == "__main__":
    main()
