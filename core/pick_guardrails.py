"""Shared "is this candidate's edge real or noise" guardrail check (BUG-003's
original diagnosis: at long odds, a tiny probability overestimate produces a
large, fake EV%). Every card generator that selects real-money picks from
priced candidates should use this, rather than each owning its own copy --
the club-league pipeline went without ANY guardrail for weeks simply because
generate_wc_card.py's version never got ported (BUG-009, 2026-08-05).

A candidate is just a (prob, implied) pair -- the model's own probability for
one side of one match, and the market's implied probability for that same
side -- so this has no dependency on any particular card generator's data
shape. floor/cap values are NOT shared here; each system tunes and owns its
own (WC: MIN_PICK_PROBABILITY/MAX_UNDERDOG_MARKET_DISAGREEMENT in
generate_wc_card.py; club leagues: their own constants), since they were
validated independently against different data.
"""


def guardrail_reasons(prob, implied, floor, cap=None):
    """Independent per-guardrail checks -- a candidate must clear ALL of them
    to be guardrail-clear. floor: reject if prob < floor (a sub-floor
    probability is noise, not edge, at any market). cap: reject if
    prob >= cap * implied -- only an underdog can trip this (a favorite can't
    be `cap`x its own high implied prob), catching the model confidently
    over-rating a dog rather than genuinely disagreeing with the market. Pass
    cap=None to skip the cap check entirely (e.g. a system that only wants
    the floor). Returns a list of human-readable reason strings -- empty
    means guardrail-clear."""
    reasons = []
    if prob < floor:
        reasons.append(f"floor (model {prob:.3f} < {floor:g})")
    if cap is not None and implied and prob >= cap * implied:
        reasons.append(f"cap (model {prob:.3f} >= {cap:g}x market {implied:.3f})")
    return reasons


def guardrail_excess(prob, implied, floor, cap=None):
    """How far past whichever guardrail(s) fired, in probability points -- the
    LARGEST excess if more than one fired (a candidate must clear all of them,
    so the hardest one to fix is what determines how close it really is).
    None if it clears everything (nothing to report)."""
    excesses = []
    if prob < floor:
        excesses.append(floor - prob)
    if cap is not None and implied and prob >= cap * implied:
        excesses.append(prob - cap * implied)
    return max(excesses) if excesses else None
