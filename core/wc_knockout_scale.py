"""
Single source of truth for BUG-004's knockout-stage goal-level correction.

totals_calibration.py showed the complete Round of 32 (16 games, 2026-07-05) ran
~18% below the model's mean projected total (2.901 projected vs 2.375 actual) --
the OPPOSITE direction from the group stage, which slightly UNDER-projects (2.776
vs 2.986). A single global change to WC_BASELINE can't fix both directions at
once, so this is scoped to non-Group stages only, applied as a separate
multiplier rather than a change to the shared baseline. See BUGS.md BUG-004.

KNOCKOUT_GOAL_SCALE multiplies uniformly into BOTH teams' venue-advantage factor
-- the same home_advantage/away_advantage slot host_advantage() already occupies
in analyse_match_wc -- so it scales lambda_H and lambda_A directly without
touching team strength (compute_wc_team_strength.py) or WC_BASELINE itself.

0.85 was chosen deliberately conservative: backtesting the full candidate range
against the 16 R32 games showed 0.819 (the exact calibration-zeroing value) and
0.80 tied for the best result (14-2-0, +5.77u, vs 1.00's 12-4-0, +2.92u); 0.85
gets nearly all of that improvement (14-2-0, +5.22u) with a smaller departure
from today's baseline. Net effect on the R32 backtest: 3 picks flipped from loss
to win, 1 flipped from win to loss -- a real, not spotless, net positive.

Because this scales BOTH lambda_H and lambda_A, it affects every market derived
from them for a knockout match (moneyline, to-advance, and totals), not only the
Over/Under market that surfaced the bug.

History:
  2026-07-05: Shipped at 0.85, applied to R16 onward (Group stage complete, no
    retroactive effect there). Backtest: knockout_baseline_backtest.py.
"""

KNOCKOUT_GOAL_SCALE = 0.85


def knockout_goal_scale(stage):
    """Goal-level multiplier for a match at `stage` (BUG-004).

    1.0 for Group (the bias doesn't show up there); KNOCKOUT_GOAL_SCALE otherwise.
    """
    return 1.0 if stage == "Group" else KNOCKOUT_GOAL_SCALE
