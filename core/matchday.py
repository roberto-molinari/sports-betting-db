"""
Shared "matchday" day-boundary logic -- used identically by pick generation,
grading, and scorecard reporting (docs/PRE-AND-POST-MATCHDAY-EXPERIENCE.md,
2026-08-21) so the three tools can never disagree about which matches belong
to which day.

A "matchday" is a US Eastern-time calendar day, not a UTC one -- soccer
kickoffs are scheduled from a European clock, so a UTC calendar day cuts
across a single US evening's viewing slate. On top of the Eastern calendar
day, MATCHDAY_BUFFER_HOURS extends the window past ET midnight so a rare very
late kickoff doesn't silently fall into "tomorrow" and get dropped from the
day it's actually part of, from a US viewer's perspective.

All match_date values in the database are stored as UTC ISO timestamps
(confirmed 2026-08-21: every row has real kickoff-time precision, not just a
bare date) -- these functions are the only place that conversion happens.
"""

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

EASTERN = ZoneInfo("America/New_York")
MATCHDAY_BUFFER_HOURS = 3


def _parse_match_date(match_date):
    """soccer_matches.match_date is stored like '2025-08-22T18:30:00.000Z' --
    normalize the trailing 'Z' (not accepted by fromisoformat on older
    pythons) and attach UTC if no offset is present."""
    if isinstance(match_date, datetime):
        dt = match_date
    else:
        dt = datetime.fromisoformat(match_date.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def matchday_for_match(match_date):
    """The ET matchday `date` a match belongs to, matching matchday_utc_window()'s
    windows: a kickoff in the first MATCHDAY_BUFFER_HOURS of its own ET calendar
    day is the "couple hours into the next day" tail of the PRIOR matchday, not
    the start of its own. Every normal daytime/evening kickoff (the vast
    majority in practice -- confirmed live 2026-08-21, no Big-5-league kickoff
    actually falls in this window) is untouched."""
    dt_et = _parse_match_date(match_date).astimezone(EASTERN)
    if dt_et.hour < MATCHDAY_BUFFER_HOURS:
        return dt_et.date() - timedelta(days=1)
    return dt_et.date()


def matchday_utc_window(matchday_date):
    """(start_utc, end_utc) -- the half-open UTC datetime range [start, end)
    covering every match that belongs to `matchday_date` (a date object or
    'YYYY-MM-DD' string): Eastern midnight through MATCHDAY_BUFFER_HOURS past
    the following Eastern midnight."""
    if isinstance(matchday_date, str):
        matchday_date = date.fromisoformat(matchday_date)
    start_et = datetime(matchday_date.year, matchday_date.month, matchday_date.day,
                        0, 0, 0, tzinfo=EASTERN)
    end_et = start_et + timedelta(days=1, hours=MATCHDAY_BUFFER_HOURS)
    return start_et.astimezone(timezone.utc), end_et.astimezone(timezone.utc)


def matchday_range_utc_window(start_date, end_date):
    """(start_utc, end_utc) covering every matchday from `start_date` through
    `end_date` inclusive (both a date object or 'YYYY-MM-DD' string) -- the
    multi-day version of matchday_utc_window(), for a report spanning several
    days rather than one."""
    start_utc, _ = matchday_utc_window(start_date)
    _, end_utc = matchday_utc_window(end_date)
    return start_utc, end_utc


def et_kickoff_time(match_date):
    """A match's UTC match_date -> a human-readable local (US Eastern) kickoff
    time string, e.g. '2:30 PM ET' -- for display only, not date-boundary math
    (see matchday_for_match for that)."""
    dt_et = _parse_match_date(match_date).astimezone(EASTERN)
    return dt_et.strftime("%-I:%M %p ET")


def format_db_timestamp(dt):
    """A UTC datetime -> the same string shape soccer_matches.match_date is
    stored in ('2025-08-22T18:30:00.000Z'), so a plain string comparison in
    SQL (match_date >= ? AND match_date < ?) sorts/compares correctly against
    a matchday_utc_window()/matchday_range_utc_window() boundary. Shared here
    so pick generation, grading, and scorecards all query the same way."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
