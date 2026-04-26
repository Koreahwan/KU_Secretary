"""Unit tests for the KUPID SSO timetable adapter."""

from __future__ import annotations

import time
from types import SimpleNamespace

import pytest

from ku_secretary._kupid import auth as kupid_auth
from ku_secretary._kupid.timetable import TimetableEntry
from ku_secretary._kupid.courses import EnrolledCourse
from ku_secretary.connectors import ku_kupid_timetable as adapter


def _session() -> kupid_auth.Session:
    return kupid_auth.Session(
        ssotoken="t",
        portal_session_id="p",
        grw_session_id="g",
        created_at=time.time(),
    )


def _entry(day: str, period: str, subject: str, classroom: str = "정운오 406호") -> TimetableEntry:
    return TimetableEntry(
        day_of_week=day,
        period=period,
        subject_name=subject,
        classroom=classroom,
        start_time="09:00",
        end_time="10:15",
    )


def _enrolled(name: str, professor: str) -> EnrolledCourse:
    return EnrolledCourse(
        course_code="X",
        section="00",
        course_type="전공",
        course_name=name,
        professor=professor,
        credits="3(3)",
        schedule="",
        retake=False,
        status="신청",
        grad_code="",
        dept_code="",
    )


def test_resolve_credentials_prefers_env_then_secret_store(monkeypatch):
    monkeypatch.setenv("KU_PORTAL_ID", "2024000099")
    monkeypatch.setenv("KU_PORTAL_PW", "envpw")

    uid, pw = adapter._resolve_credentials(target=None, settings=SimpleNamespace())
    assert uid == "2024000099"
    assert pw == "envpw"


def test_resolve_credentials_uses_secret_store_when_available(tmp_path, monkeypatch):
    from ku_secretary.secret_store import FileSecretStore

    store = FileSecretStore(tmp_path)
    pw_ref = store.store_secret(key="ku_portal_password", secret="storepw")
    monkeypatch.delenv("KU_PORTAL_PW", raising=False)
    monkeypatch.setenv("KU_PORTAL_ID", "id-from-env")

    settings = SimpleNamespace()
    monkeypatch.setattr(adapter, "default_secret_store", lambda _s: store)

    uid, pw = adapter._resolve_credentials(
        target={
            "user_login_id": "id-from-target",
            "secret_kind": pw_ref.kind,
            "secret_ref": pw_ref.ref,
        },
        settings=settings,
    )
    assert uid == "id-from-target"
    assert pw == "storepw"


def test_resolve_credentials_raises_when_missing(monkeypatch):
    monkeypatch.delenv("KU_PORTAL_ID", raising=False)
    monkeypatch.delenv("KU_PORTAL_PW", raising=False)
    with pytest.raises(RuntimeError):
        adapter._resolve_credentials(target=None, settings=SimpleNamespace())


def test_fetch_kupid_sso_timetable_builds_events(monkeypatch):
    sample_session = _session()

    def fake_login(user_id, password):
        assert user_id == "uid"
        assert password == "pw"
        return sample_session

    monkeypatch.setattr(adapter, "kupid_login", fake_login)
    monkeypatch.setattr(
        adapter,
        "get_full_timetable",
        lambda s: [
            _entry("월", "1", "딥러닝"),
            _entry("화", "3", "운영체제", "신공학관 101호"),
        ],
    )
    monkeypatch.setattr(
        adapter,
        "get_my_courses",
        lambda s, year=None, semester=None: (
            [_enrolled("딥러닝", "석흥일"), _enrolled("운영체제", "최수경")],
            "6.0",
        ),
    )

    monkeypatch.setenv("KU_PORTAL_ID", "uid")
    monkeypatch.setenv("KU_PORTAL_PW", "pw")

    out = adapter.fetch_kupid_sso_timetable(
        settings=SimpleNamespace(timezone="Asia/Seoul"),
        target=None,
    )
    assert out["payload_source"] == "kupid_sso_timetable"
    assert out["fallback_used"] is False
    assert len(out["events"]) == 2

    e1, e2 = out["events"]
    assert e1["title"] == "딥러닝"
    assert e1["rrule"] == "FREQ=WEEKLY;BYDAY=MO"
    assert e1["metadata"]["instructor"] == "석흥일"
    assert e1["metadata"]["period"] == "1"
    assert e1["start_at"].endswith("+09:00")
    assert e2["title"] == "운영체제"
    assert e2["rrule"] == "FREQ=WEEKLY;BYDAY=TU"
    assert e2["location"] == "신공학관 101호"
    assert e2["metadata"]["instructor"] == "최수경"


def test_fetch_kupid_sso_timetable_skips_bad_rows(monkeypatch):
    monkeypatch.setattr(adapter, "kupid_login", lambda **k: _session())
    monkeypatch.setattr(
        adapter,
        "get_full_timetable",
        lambda s: [
            _entry("월", "1", "정상"),
            TimetableEntry("월", "2", "", "", "09:00", "10:15"),  # empty title
            TimetableEntry("MON", "3", "잘못된요일", "", "09:00", "10:15"),
            TimetableEntry("화", "4", "시간없음", "", "", ""),  # unresolved time
        ],
    )
    monkeypatch.setattr(adapter, "get_my_courses", lambda s, **kw: ([], ""))
    monkeypatch.setenv("KU_PORTAL_ID", "uid")
    monkeypatch.setenv("KU_PORTAL_PW", "pw")

    out = adapter.fetch_kupid_sso_timetable(settings=SimpleNamespace())
    assert len(out["events"]) == 1
    assert out["events"][0]["title"] == "정상"


def test_fetch_kupid_sso_timetable_handles_my_courses_failure(monkeypatch):
    monkeypatch.setattr(adapter, "kupid_login", lambda **k: _session())
    monkeypatch.setattr(adapter, "get_full_timetable", lambda s: [_entry("월", "1", "딥러닝")])

    def boom(*a, **k):
        raise RuntimeError("infodepot offline")

    monkeypatch.setattr(adapter, "get_my_courses", boom)
    monkeypatch.setenv("KU_PORTAL_ID", "uid")
    monkeypatch.setenv("KU_PORTAL_PW", "pw")

    out = adapter.fetch_kupid_sso_timetable(settings=SimpleNamespace())
    assert len(out["events"]) == 1
    # No instructor since enrichment failed silently
    assert out["events"][0]["metadata"].get("instructor") is None
