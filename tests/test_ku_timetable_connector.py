"""Unit tests for the timetable connector."""

from __future__ import annotations

import time

import pytest

from ku_secretary._kupid import auth as kupid_auth
from ku_secretary._kupid import timetable as kupid_timetable
from ku_secretary._kupid.timetable import TimetableEntry
from ku_secretary.connectors import ku_timetable


def _session() -> kupid_auth.Session:
    return kupid_auth.Session(
        ssotoken="t",
        portal_session_id="p",
        grw_session_id="g",
        created_at=time.time(),
    )


def _entry(day: str = "월", period: str = "1-2") -> TimetableEntry:
    return TimetableEntry(
        day_of_week=day,
        period=period,
        subject_name="딥러닝",
        classroom="애기능 301",
        start_time="09:00",
        end_time="11:45",
    )


def test_get_full_timetable_calls_upstream(monkeypatch):
    captured: dict = {}

    async def fake(session):
        captured["session"] = session
        return [_entry()]

    monkeypatch.setattr(ku_timetable, "fetch_full_timetable", fake)

    s = _session()
    out = ku_timetable.get_full_timetable(s)
    assert captured["session"] is s
    assert len(out) == 1 and out[0].subject_name == "딥러닝"


def test_get_timetable_for_day_maps_day(monkeypatch):
    captured: dict = {}

    async def fake(session, day):
        captured["day"] = day
        return [_entry()]

    monkeypatch.setattr(ku_timetable, "fetch_timetable_day", fake)

    ku_timetable.get_timetable_for_day(_session(), "wed")
    assert captured["day"] == 3


def test_get_timetable_for_day_rejects_unknown():
    with pytest.raises(ValueError):
        ku_timetable.get_timetable_for_day(_session(), "foo")  # type: ignore[arg-type]


def test_export_ics_passthrough():
    ics = ku_timetable.export_ics([_entry()], semester_start="2026-03-02")
    assert "BEGIN:VCALENDAR" in ics
    assert "딥러닝" in ics
    assert "END:VCALENDAR" in ics
