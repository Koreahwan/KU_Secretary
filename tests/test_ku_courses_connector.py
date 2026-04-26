"""Unit tests for the courses connector."""

from __future__ import annotations

import time

from ku_secretary._kupid import auth as kupid_auth
from ku_secretary._kupid.courses import (
    COLLEGE_CODES,
    CourseInfo,
    EnrolledCourse,
)
from ku_secretary.connectors import ku_courses


def _session() -> kupid_auth.Session:
    return kupid_auth.Session(
        ssotoken="t",
        portal_session_id="p",
        grw_session_id="g",
        created_at=time.time(),
    )


def _enrolled(code: str = "AAI110") -> EnrolledCourse:
    return EnrolledCourse(
        course_code=code,
        section="00",
        course_type="전공선택",
        course_name="딥러닝",
        professor="석흥일",
        credits="2(2)",
        schedule="월(7-8) 애기능 301",
        retake=False,
        status="신청",
        grad_code="7298",
        dept_code="7313",
    )


def test_get_my_courses_passes_args(monkeypatch):
    captured: dict = {}

    async def fake(session, year=None, semester=None):
        captured.update(year=year, semester=semester)
        return [_enrolled()], "18.0"

    monkeypatch.setattr(ku_courses, "_fetch_my_courses", fake)

    courses, total = ku_courses.get_my_courses(
        _session(), year="2026", semester="1"
    )
    assert total == "18.0"
    assert courses[0].course_code == "AAI110"
    assert captured == {"year": "2026", "semester": "1"}


def test_search_courses_passes_args(monkeypatch):
    captured: dict = {}

    async def fake(session, *, year, semester, campus, college, department):
        captured.update(
            year=year, semester=semester, campus=campus,
            college=college, department=department,
        )
        return [
            CourseInfo(
                campus="1", course_code="COSE101", section="01",
                course_type="전공", course_name="자료구조",
                professor="홍길동", credits="3", schedule="화(3-4)",
            )
        ]

    monkeypatch.setattr(ku_courses, "_search_courses", fake)

    rows = ku_courses.search_courses(
        _session(), college="5720", department="5722",
        year="2026", semester="1", campus="1",
    )
    assert rows[0].course_code == "COSE101"
    assert captured["college"] == "5720"
    assert captured["department"] == "5722"


def test_get_departments_passes_args(monkeypatch):
    captured: dict = {}

    # Upstream uses (session, college_code, year, term) positionally.
    async def fake(session, college_code, year, term):
        captured.update(
            college_code=college_code, year=year, term=term
        )
        return [{"code": "5722", "name": "컴퓨터학과"}]

    monkeypatch.setattr(ku_courses, "_fetch_departments", fake)

    depts = ku_courses.get_departments(
        _session(), "5720", year="2026", semester="1"
    )
    assert depts[0]["name"] == "컴퓨터학과"
    assert captured["college_code"] == "5720"
    assert captured["year"] == "2026"
    assert captured["term"] == "1"


def test_get_syllabus_passes_args(monkeypatch):
    captured: dict = {}

    async def fake(session, *, course_code, section, year, semester, grad_code):
        captured.update(
            course_code=course_code, section=section,
            year=year, semester=semester, grad_code=grad_code,
        )
        return "Course outline ..."

    monkeypatch.setattr(ku_courses, "_fetch_syllabus", fake)

    text = ku_courses.get_syllabus(
        _session(), "COSE101", section="02", year="2026", semester="1"
    )
    assert text == "Course outline ..."
    assert captured["course_code"] == "COSE101"
    assert captured["section"] == "02"


def test_list_colleges_returns_copy():
    colleges = ku_courses.list_colleges()
    assert colleges == COLLEGE_CODES
    colleges["0000"] = "Hax"
    # Should not mutate the upstream constant
    assert "0000" not in COLLEGE_CODES
