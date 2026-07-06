"""
Single source of truth for World Cup co-host venue advantage (BUG-006).

A host nation (USA / Mexico / Canada) gets a venue-advantage multiplier on its
own attack lambda ONLY when it is actually playing in its own country — NOT
simply because it is one of the three co-hosts. The tournament format
guarantees every host plays its group games domestically, but knockout-round
stadium assignment is independent of host status: a host team can (and does)
end up playing a knockout match in one of the other co-host countries.

Two tiers, because a host's remaining fixtures stop being guaranteed domestic
partway through the tournament and this has to be tracked by hand (there is no
venue/stadium column on soccer_wc_matches, so nothing here can be derived —
each entry below reflects a fact the user confirmed from the actual bracket):

  HOST_NATIONS             -- currently active hosts: ALL of this team's matches
                              so far, and its next match, are confirmed domestic.
                              Boost applies regardless of stage.
  GROUP_STAGE_HOST_NATIONS -- retired hosts whose domestic run stopped after the
                              group stage. Boost applies ONLY when stage == "Group".

When a currently-active host's domestic run ends, move it from HOST_NATIONS to
a new stage-scoped set (add one below, named for the stages it covers) rather
than deleting its history, so backtests/calibration tools that recompute past
matches from current team strength still apply the boost where it was real.

History:
  2026-07-04: Canada retired from HOST_NATIONS (confirmed: their R32 match vs
    South Africa, 2026-06-28, was played in Los Angeles, USA -- NOT Canada, so
    the group-stage-only scope is correct, not an oversight). Moved to
    GROUP_STAGE_HOST_NATIONS. This also revealed that Canada's stored R32 pick
    (match_id 73, HOME ADVANCE, South Africa) was originally computed with an
    erroneous boost on Canada's lambda -- see BUG-006 in BUGS.md. Left
    ungraded-pick history untouched (already graded, locked).
"""

HOST_HOME_ADVANTAGE = 1.20

HOST_NATIONS = {"USA", "Mexico"}
GROUP_STAGE_HOST_NATIONS = {"Canada"}


def host_advantage(team_name, stage):
    """Venue-advantage multiplier for `team_name` playing at `stage`.

    Currently-active hosts get it regardless of stage; retired hosts get it
    only for the stage(s) their domestic run actually covered.
    """
    if team_name in HOST_NATIONS:
        return HOST_HOME_ADVANTAGE
    if stage == "Group" and team_name in GROUP_STAGE_HOST_NATIONS:
        return HOST_HOME_ADVANTAGE
    return 1.0
