"""Unit tests for the Canvas LMS connector.

All upstream coroutines are monkeypatched, so no live Canvas hits.
"""

from __future__ import annotations

import time

import pytest

from ku_secretary._kupid import lms as kupid_lms
from ku_secretary._kupid.lms import LMSSession
from ku_secretary.connectors import ku_lms
from ku_secretary.secret_store import FileSecretStore


def _session() -> LMSSession:
    return LMSSession(
        cookies={"_canvas_session": "abc"},
        user_id="2024000001",
        user_name="Hong Gildong",
        canvas_user_id=12345,
        created_at=time.time(),
    )


def test_login_calls_upstream(monkeypatch):
    captured: dict = {}

    async def fake(user_id, password):
        captured.update(user_id=user_id, password=password)
        return _session()

    monkeypatch.setattr(kupid_lms, "lms_login", fake)
    s = ku_lms.login(user_id="2024000001", password="hunter2")
    assert s.user_id == "2024000001"
    assert captured == {"user_id": "2024000001", "password": "hunter2"}


def test_login_rejects_empty():
    with pytest.raises(ValueError):
        ku_lms.login(user_id="", password="x")
    with pytest.raises(ValueError):
        ku_lms.login(user_id="x", password="")


def test_login_with_secret_store_round_trip(tmp_path, monkeypatch):
    store = FileSecretStore(tmp_path / "secrets")
    id_ref = store.store_secret(key=ku_lms.LMS_ID_KEY, secret="2024000099")
    pw_ref = store.store_secret(key=ku_lms.LMS_PASSWORD_KEY, secret="pw!")

    captured: dict = {}

    async def fake(user_id, password):
        captured.update(user_id=user_id, password=password)
        return _session()

    monkeypatch.setattr(kupid_lms, "lms_login", fake)

    s = ku_lms.login_with_secret_store(
        store=store, id_ref=id_ref, password_ref=pw_ref
    )
    assert s.canvas_user_id == 12345
    assert captured == {"user_id": "2024000099", "password": "pw!"}


def test_configure_session_cache_redirects(tmp_path, monkeypatch):
    monkeypatch.setattr(kupid_lms, "CACHE_DIR", kupid_lms.CACHE_DIR)
    monkeypatch.setattr(
        kupid_lms, "LMS_SESSION_FILE", kupid_lms.LMS_SESSION_FILE
    )

    target = tmp_path / "data" / "lms_cache"
    resolved = ku_lms.configure_session_cache(target)
    assert resolved == target.resolve()
    assert kupid_lms.CACHE_DIR == target.resolve()
    assert kupid_lms.LMS_SESSION_FILE == target.resolve() / "lms_session.json"
    assert ku_lms.session_cache_path() == target.resolve() / "lms_session.json"


@pytest.mark.parametrize(
    "method,upstream_name,extra_kwargs,call_args",
    [
        ("get_courses", "fetch_lms_courses", {}, ()),
        ("get_todo", "fetch_lms_todo", {}, ()),
        ("get_upcoming_events", "fetch_lms_upcoming_events", {}, ()),
        ("get_dashboard", "fetch_lms_dashboard", {}, ()),
    ],
)
def test_no_arg_helpers(monkeypatch, method, upstream_name, extra_kwargs, call_args):
    received: dict = {"called": 0}

    async def fake(session):
        received["called"] += 1
        received["session"] = session
        return [{"id": 1}]

    monkeypatch.setattr(kupid_lms, upstream_name, fake)
    s = _session()
    out = getattr(ku_lms, method)(s, *call_args, **extra_kwargs)
    assert out == [{"id": 1}]
    assert received["called"] == 1
    assert received["session"] is s


def test_get_assignments_passes_flag(monkeypatch):
    captured: dict = {}

    async def fake(session, course_id, upcoming_only):
        captured.update(course_id=course_id, upcoming_only=upcoming_only)
        return [{"id": 7}]

    monkeypatch.setattr(kupid_lms, "fetch_lms_assignments", fake)

    out = ku_lms.get_assignments(_session(), 42, upcoming_only=True)
    assert out == [{"id": 7}]
    assert captured == {"course_id": 42, "upcoming_only": True}


def test_get_modules_default_includes_items(monkeypatch):
    captured: dict = {}

    async def fake(session, course_id, include_items):
        captured.update(course_id=course_id, include_items=include_items)
        return []

    monkeypatch.setattr(kupid_lms, "fetch_lms_modules", fake)

    ku_lms.get_modules(_session(), 99)
    assert captured == {"course_id": 99, "include_items": True}


def test_get_announcements_passes_course_ids(monkeypatch):
    captured: dict = {}

    async def fake(session, course_ids):
        captured["course_ids"] = course_ids
        return [{"id": "a1"}]

    monkeypatch.setattr(kupid_lms, "fetch_lms_announcements", fake)
    ku_lms.get_announcements(_session(), [1, 2, 3])
    assert captured["course_ids"] == [1, 2, 3]


def test_download_file_resolves_path(monkeypatch, tmp_path):
    captured: dict = {}

    async def fake(session, file_id, target, fname):
        captured.update(file_id=file_id, target=target, filename=fname)
        return {"path": str(target / "out.pdf"), "filename": "out.pdf", "size": 10}

    monkeypatch.setattr(kupid_lms, "download_lms_file", fake)

    out = ku_lms.download_file(
        _session(), 555, tmp_path / "downloads", filename="lec.pdf"
    )
    assert out["filename"] == "out.pdf"
    assert captured["file_id"] == 555
    assert captured["target"] == (tmp_path / "downloads").resolve()
    assert captured["filename"] == "lec.pdf"


def test_board_helpers(monkeypatch):
    calls: dict = {}

    async def fake_boards(session, cid):
        calls["boards"] = cid
        return [{"id": 1}]

    async def fake_posts(session, cid, bid, page, keyword):
        calls["posts"] = (cid, bid, page, keyword)
        return {"items": [{"id": 11}], "total": 1}

    async def fake_post(session, cid, bid, pid):
        calls["post"] = (cid, bid, pid)
        return {"id": pid, "title": "title"}

    monkeypatch.setattr(kupid_lms, "fetch_lms_boards", fake_boards)
    monkeypatch.setattr(kupid_lms, "fetch_lms_board_posts", fake_posts)
    monkeypatch.setattr(kupid_lms, "fetch_lms_board_post", fake_post)

    s = _session()
    assert ku_lms.list_boards(s, 7) == [{"id": 1}]
    assert ku_lms.list_board_posts(s, 7, 9, page=2, keyword="공지")["total"] == 1
    post = ku_lms.get_board_post(s, 7, 9, 11)
    assert post["id"] == 11

    assert calls["boards"] == 7
    assert calls["posts"] == (7, 9, 2, "공지")
    assert calls["post"] == (7, 9, 11)


def test_clear_session_calls_upstream(monkeypatch):
    called = {"n": 0}

    def fake_clear():
        called["n"] += 1

    monkeypatch.setattr(kupid_lms, "_clear_lms_session", fake_clear)
    ku_lms.clear_session()
    assert called["n"] == 1
