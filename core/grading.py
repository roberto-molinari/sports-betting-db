"""Pure, market-agnostic grading for soccer picks (no DB, stdlib only).

This is the single source of truth for settling a pick side, importable/vendorable by the
external social/ROI tracker repo so it scores the exact same way this repo does — across
1X2, totals (O/U), and the knockout "to advance" market.

A pick ``side`` is graded against an ``outcome`` dict:

    outcome = {
        "regulation_home": int,   # 90' regulation goals (settles 1X2 + O/U)
        "regulation_away": int,
        "advanced": "HOME" | "AWAY" | None,   # who won the tie (settles ADVANCE); None if N/A
    }

``advancing_side`` derives the ``advanced`` value from a knockout match's path scores, so
"who advanced" is never stored — it's computed from regulation / extra-time / shootout goals.
"""


def grade_pick(side, outcome):
    """Return 'win' | 'loss' | 'push' for a pick ``side`` given a match ``outcome``.

    Side grammar:
      HOME / DRAW / AWAY            -> 90' regulation result
      OVER <line> / UNDER <line>    -> 90' regulation total (integer lines can push)
      HOME ADVANCE / AWAY ADVANCE   -> who advanced the tie
    """
    if side in ("HOME ADVANCE", "AWAY ADVANCE"):
        advanced = outcome.get("advanced")
        if advanced not in ("HOME", "AWAY"):
            raise ValueError(f"cannot grade {side!r}: outcome 'advanced' is {advanced!r}")
        want = "HOME" if side == "HOME ADVANCE" else "AWAY"
        return "win" if advanced == want else "loss"

    home = outcome["regulation_home"]
    away = outcome["regulation_away"]

    if side == "HOME":
        return "win" if home > away else "loss"
    if side == "AWAY":
        return "win" if away > home else "loss"
    if side == "DRAW":
        return "win" if home == away else "loss"
    if side.startswith("OVER ") or side.startswith("UNDER "):
        label, line_text = side.split(" ", 1)
        line = float(line_text)
        total = home + away
        if total == line:
            return "push"
        over = total > line
        return "win" if (over == (label == "OVER")) else "loss"
    raise ValueError(f"Unknown pick side: {side!r}")


def advancing_side(regulation_home, regulation_away,
                   extra_time_home=None, extra_time_away=None,
                   shootout_home=None, shootout_away=None):
    """Derive who advanced ('HOME' / 'AWAY') from a knockout tie's path scores.

    Compares the deepest level that was decisive — shootout, else extra time, else 90' —
    using cumulative scores (extra-time score includes the 90' goals). Returns None only if
    no level separates the teams (shouldn't happen for a completed knockout tie).
    """
    if (shootout_home is not None and shootout_away is not None
            and shootout_home != shootout_away):
        return "HOME" if shootout_home > shootout_away else "AWAY"
    if (extra_time_home is not None and extra_time_away is not None
            and extra_time_home != extra_time_away):
        return "HOME" if extra_time_home > extra_time_away else "AWAY"
    if regulation_home != regulation_away:
        return "HOME" if regulation_home > regulation_away else "AWAY"
    return None
