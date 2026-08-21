"""Tests for core/matchday.py's ET+buffer day-boundary logic (2026-08-21,
docs/PRE-AND-POST-MATCHDAY-EXPERIENCE.md) -- shared by pick generation,
grading, and scorecard reporting."""
from datetime import date

from core.matchday import matchday_for_match, matchday_utc_window, matchday_range_utc_window


def test_normal_afternoon_kickoff_matches_its_own_et_date():
    # 18:30 UTC in August (EDT, UTC-4) = 14:30 ET, same calendar date
    assert matchday_for_match("2026-08-22T18:30:00.000Z") == date(2026, 8, 22)


def test_kickoff_just_after_et_midnight_folds_back_to_prior_day():
    # 06:00 UTC on the 23rd = 02:00 EDT on the 23rd -- inside the 3h buffer
    assert matchday_for_match("2026-08-23T06:00:00.000Z") == date(2026, 8, 22)


def test_kickoff_past_the_buffer_belongs_to_the_next_day():
    # 08:00 UTC on the 23rd = 04:00 EDT on the 23rd -- outside the 3h buffer
    assert matchday_for_match("2026-08-23T08:00:00.000Z") == date(2026, 8, 23)


def test_kickoff_exactly_at_et_midnight_belongs_to_the_prior_day():
    # 04:00 UTC on the 23rd = 00:00 EDT on the 23rd -- midnight itself is inside
    # the buffer window, so it's still the tail of the 22nd's matchday, not the
    # start of the 23rd's (real kickoffs never land exactly here in practice).
    assert matchday_for_match("2026-08-23T04:00:00.000Z") == date(2026, 8, 22)


def test_kickoff_exactly_at_the_buffer_boundary_belongs_to_its_own_day():
    # 07:00 UTC on the 23rd = 03:00 EDT on the 23rd -- exactly at the buffer edge
    assert matchday_for_match("2026-08-23T07:00:00.000Z") == date(2026, 8, 23)


def test_window_and_membership_agree():
    """Every match_date the window includes (other than the ambiguous first
    MATCHDAY_BUFFER_HOURS of the matchday's own calendar date, which real
    kickoffs never land in) must map back to the same matchday -- the two
    functions have to never disagree, since pick generation uses one and
    grading/reporting use the other."""
    start_utc, end_utc = matchday_utc_window("2026-08-22")
    # mid-afternoon, well inside the window, maps back cleanly
    from datetime import timedelta
    midday = start_utc + timedelta(hours=18)
    assert matchday_for_match(midday) == date(2026, 8, 22)
    # one second before the window ends still belongs to it
    assert matchday_for_match(end_utc - timedelta(seconds=1)) == date(2026, 8, 22)
    # the window's own end boundary belongs to the NEXT matchday (half-open range)
    assert matchday_for_match(end_utc) == date(2026, 8, 23)


def test_accepts_date_object_or_iso_string_interchangeably():
    assert matchday_utc_window(date(2026, 8, 22)) == matchday_utc_window("2026-08-22")


def test_range_window_spans_multiple_days():
    start_utc, end_utc = matchday_range_utc_window("2026-08-20", "2026-08-22")
    single_start, _ = matchday_utc_window("2026-08-20")
    _, single_end = matchday_utc_window("2026-08-22")
    assert start_utc == single_start
    assert end_utc == single_end
