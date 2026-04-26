"""KU enrolled-courses, course search, and syllabus connector.

Wraps the vendored ku-portal-mcp `courses` module. All public helpers are
synchronous so they can be called directly from KU_Secretary's CLI/job code.

Authentication: requires a `Session` from `ku_portal_auth.login(...)`.
"""

from __future__ import annotations

import asyncio
import logging

from ku_secretary._kupid.auth import Session
from ku_secretary._kupid.courses import (
    COLLEGE_CODES,
    CourseInfo,
    EnrolledCourse,
    fetch_departments as _fetch_departments,
    fetch_my_courses as _fetch_my_courses,
    fetch_syllabus as _fetch_syllabus,
    search_courses as _search_courses,
)

logger = logging.getLogger(__name__)


def get_my_courses(
    session: Session,
    *,
    year: str | None = None,
    semester: str | None = None,
) -> tuple[list[EnrolledCourse], str]:
    """Fetch the student's enrolled courses and total credit count.

    Args:
        year: e.g. "2026". When None, resolves to the current academic year.
        semester: "1", "2", "summer", or "winter". When None, resolves
            to the current term.
    """
    return asyncio.run(_fetch_my_courses(session, year=year, semester=semester))


def search_courses(
    session: Session,
    *,
    college: str,
    department: str,
    year: str | None = None,
    semester: str | None = None,
    campus: str = "1",
) -> list[CourseInfo]:
    """Search the offered-course catalog by college and department.

    Args:
        college: college code (e.g. "5720" for 정보대학; see
            :data:`list_colleges`).
        department: department code (use :func:`get_departments` to discover
            valid codes for a college).
        campus: "1" Seoul, "2" Sejong.
    """
    return asyncio.run(
        _search_courses(
            session,
            year=year,
            semester=semester,
            campus=campus,
            college=college,
            department=department,
        )
    )


def get_departments(
    session: Session,
    college_code: str,
    *,
    year: str | None = None,
    semester: str | None = None,
) -> list[dict]:
    """List the departments under a college as ``{"code": ..., "name": ...}``."""
    # Upstream calls the second time-component "term"; we keep "semester" at
    # this layer for parity with the rest of the connector and forward it
    # positionally (upstream signature: fetch_departments(session, college_code, year, term)).
    return asyncio.run(_fetch_departments(session, college_code, year, semester))


def get_syllabus(
    session: Session,
    course_code: str,
    *,
    section: str = "00",
    year: str | None = None,
    semester: str | None = None,
    grad_code: str = "",
) -> str:
    """Fetch the syllabus text for a given course code + section."""
    return asyncio.run(
        _fetch_syllabus(
            session,
            course_code=course_code,
            section=section,
            year=year,
            semester=semester,
            grad_code=grad_code,
        )
    )


def list_colleges() -> dict[str, str]:
    """Return the {college_code: college_name} map (Seoul campus)."""
    return dict(COLLEGE_CODES)
