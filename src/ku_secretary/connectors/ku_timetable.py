"""KU class timetable connector via KUPID portal.

Wraps the vendored ku-portal-mcp `timetable` module so KU_Secretary's
synchronous job code can fetch a student's weekly schedule and optionally
export it to ICS for calendar import.

Authentication: requires a `Session` from `ku_portal_auth.login(...)`.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Literal

from ku_secretary._kupid.auth import Session
from ku_secretary._kupid.timetable import (
    TimetableEntry,
    fetch_full_timetable,
    fetch_timetable_day,
    timetable_to_ics,
)

logger = logging.getLogger(__name__)

DayName = Literal["sun", "mon", "tue", "wed", "thu", "fri", "sat"]
_DAY_INDEX: dict[str, int] = {
    "sun": 0,
    "mon": 1,
    "tue": 2,
    "wed": 3,
    "thu": 4,
    "fri": 5,
    "sat": 6,
}


def get_full_timetable(session: Session) -> list[TimetableEntry]:
    """Fetch the student's full Mon-Fri timetable in parallel."""
    return asyncio.run(fetch_full_timetable(session))


def get_timetable_for_day(session: Session, day: DayName) -> list[TimetableEntry]:
    """Fetch a single day's timetable.

    Raises:
        ValueError: if *day* is not one of sun..sat.
    """
    if day not in _DAY_INDEX:
        raise ValueError(
            f"Unknown day: {day!r}. Use one of: {list(_DAY_INDEX)}"
        )
    return asyncio.run(fetch_timetable_day(session, _DAY_INDEX[day]))


def export_ics(entries: list[TimetableEntry], semester_start: str = "") -> str:
    """Render timetable entries to an iCalendar string.

    Args:
        entries: list returned by :func:`get_full_timetable`.
        semester_start: optional YYYY-MM-DD anchoring the recurring events.
            Defaults to the next Monday from today.
    """
    return timetable_to_ics(entries, semester_start)
