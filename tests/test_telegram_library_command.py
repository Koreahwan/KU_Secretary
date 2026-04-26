"""Tests for the /library Telegram command (parse + format)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from ku_secretary.connectors import telegram
from ku_secretary.jobs import pipeline


def test_parse_library_no_arg():
    out = telegram.parse_command_message("/library")
    assert out == {"command": "library", "ok": True}


def test_parse_library_with_name():
    out = telegram.parse_command_message("/library 중앙도서관")
    assert out == {"command": "library", "ok": True, "library": "중앙도서관"}


def test_parse_library_alias_lib():
    out = telegram.parse_command_message("/lib 과학도서관")
    assert out == {"command": "library", "ok": True, "library": "과학도서관"}


def test_parse_library_alias_seats():
    out = telegram.parse_command_message("/seats")
    assert out == {"command": "library", "ok": True}


def _stub_seats_payload() -> dict:
    return {
        "libraries": {
            "중앙도서관": [
                {
                    "room_name": "Forest Zone",
                    "room_name_eng": "Forest",
                    "total_seats": 100,
                    "available": 60,
                    "in_use": 40,
                    "disabled": 0,
                    "is_notebook_allowed": True,
                    "operating_hours": "09:00-22:00",
                },
                {
                    "room_name": "Quiet Zone",
                    "room_name_eng": "Quiet",
                    "total_seats": 50,
                    "available": 10,
                    "in_use": 40,
                    "disabled": 0,
                    "is_notebook_allowed": False,
                    "operating_hours": "09:00-22:00",
                },
            ]
        },
        "summary": {
            "total_seats": 150,
            "total_available": 70,
            "total_in_use": 80,
            "occupancy_rate": "53.3%",
        },
    }


def test_format_filtered_renders_room_breakdown(monkeypatch):
    monkeypatch.setattr(pipeline, "get_library_seats", lambda *_a, **_k: _stub_seats_payload())
    out = pipeline._format_telegram_library("중앙도서관")
    assert "KU 도서관 좌석 — 중앙도서관" in out
    assert "합계: 70/150석 가용 (점유율 53.3%)" in out
    assert "Forest Zone (노트북): 60/100" in out
    assert "Quiet Zone:" in out and "(노트북)" not in out.split("Quiet Zone")[1].split("\n")[0]


def test_format_no_arg_renders_overall(monkeypatch):
    monkeypatch.setattr(pipeline, "get_library_seats", lambda *_a, **_k: _stub_seats_payload())
    out = pipeline._format_telegram_library(None)
    assert out.startswith("KU 도서관 좌석\n")
    assert "중앙도서관 — 70/150석 가용" in out


def test_format_unknown_library_returns_help_message(monkeypatch):
    def raiser(_name=None):
        raise ValueError("unknown")

    monkeypatch.setattr(pipeline, "get_library_seats", raiser)
    out = pipeline._format_telegram_library("뉴욕공립도서관")
    assert "찾을 수 없습니다" in out
    assert "사용 가능:" in out


def test_format_network_error_surfaces_message(monkeypatch):
    def raiser(_name=None):
        raise RuntimeError("HODI 5xx")

    monkeypatch.setattr(pipeline, "get_library_seats", raiser)
    out = pipeline._format_telegram_library(None)
    assert "도서관 좌석 조회 실패" in out
    assert "HODI 5xx" in out


def test_parse_assignments_aliases():
    expected = {"command": "assignments", "ok": True}
    for cmd in ("/assignments", "/due", "/homework", "/과제"):
        assert telegram.parse_command_message(cmd) == expected, cmd


def test_format_assignments_renders_todo_and_events(monkeypatch):
    monkeypatch.setenv("KU_PORTAL_ID", "uid")
    monkeypatch.setenv("KU_PORTAL_PW", "pw")

    from ku_secretary.connectors import ku_lms

    monkeypatch.setattr(ku_lms, "login", lambda *, user_id, password: "stub-session")
    monkeypatch.setattr(
        ku_lms,
        "get_todo",
        lambda s: [
            {
                "assignment": {
                    "name": "중간고사 대체 과제",
                    "due_at": "2026-04-26T14:59:00Z",
                    "html_url": "https://mylms.korea.ac.kr/courses/1/assignments/2",
                }
            }
        ],
    )
    monkeypatch.setattr(
        ku_lms,
        "get_upcoming_events",
        lambda s: [
            {"title": "중간고사 대체 과제", "start_at": "2026-04-26T14:59:00Z"},
        ],
    )

    out = pipeline._format_telegram_assignments()
    assert "[KU] 내야 할 과제" in out
    assert "중간고사 대체 과제" in out
    assert "2026-04-26 14:59" in out
    assert "과제 (1건)" in out
    assert "다가오는 이벤트 (1건)" in out


def test_format_assignments_empty(monkeypatch):
    monkeypatch.setenv("KU_PORTAL_ID", "uid")
    monkeypatch.setenv("KU_PORTAL_PW", "pw")
    from ku_secretary.connectors import ku_lms
    monkeypatch.setattr(ku_lms, "login", lambda *, user_id, password: "s")
    monkeypatch.setattr(ku_lms, "get_todo", lambda s: [])
    monkeypatch.setattr(ku_lms, "get_upcoming_events", lambda s: [])
    out = pipeline._format_telegram_assignments()
    assert "마감 임박한 과제가 없습니다" in out


def test_format_assignments_missing_credentials(monkeypatch):
    monkeypatch.delenv("KU_PORTAL_ID", raising=False)
    monkeypatch.delenv("KU_PORTAL_PW", raising=False)
    out = pipeline._format_telegram_assignments()
    assert "환경변수가 비어 있습니다" in out


def test_format_assignments_login_failure(monkeypatch):
    monkeypatch.setenv("KU_PORTAL_ID", "uid")
    monkeypatch.setenv("KU_PORTAL_PW", "pw")
    from ku_secretary.connectors import ku_lms

    def boom(*, user_id, password):
        raise RuntimeError("KSSO down")

    monkeypatch.setattr(ku_lms, "login", boom)
    out = pipeline._format_telegram_assignments()
    assert "LMS 로그인 실패" in out
    assert "KSSO down" in out


def test_format_assignments_todo_fetch_failure(monkeypatch):
    monkeypatch.setenv("KU_PORTAL_ID", "uid")
    monkeypatch.setenv("KU_PORTAL_PW", "pw")
    from ku_secretary.connectors import ku_lms

    monkeypatch.setattr(ku_lms, "login", lambda *, user_id, password: "s")

    def boom(_session):
        raise RuntimeError("Canvas todo down")

    monkeypatch.setattr(ku_lms, "get_todo", boom)
    out = pipeline._format_telegram_assignments()
    assert "LMS 할 일 조회 실패" in out
    assert "Canvas todo down" in out


def test_format_assignments_ignores_event_fetch_failure(monkeypatch):
    monkeypatch.setenv("KU_PORTAL_ID", "uid")
    monkeypatch.setenv("KU_PORTAL_PW", "pw")
    from ku_secretary.connectors import ku_lms

    monkeypatch.setattr(ku_lms, "login", lambda *, user_id, password: "s")
    monkeypatch.setattr(
        ku_lms,
        "get_todo",
        lambda s: [{"assignment": {"name": "보고서", "due_at": None}}],
    )
    monkeypatch.setattr(
        ku_lms,
        "get_upcoming_events",
        lambda s: (_ for _ in ()).throw(RuntimeError("events down")),
    )
    out = pipeline._format_telegram_assignments()
    assert "과제 (1건)" in out
    assert "보고서" in out
    assert "다가오는 이벤트" not in out


def test_format_assignments_prefers_user_login_secret(monkeypatch):
    monkeypatch.delenv("KU_PORTAL_ID", raising=False)
    monkeypatch.delenv("KU_PORTAL_PW", raising=False)
    from ku_secretary.connectors import ku_lms

    seen: dict[str, str] = {}

    class FakeDb:
        def list_moodle_connections(self, **kwargs):
            seen["user_id"] = str(kwargs["user_id"])
            seen["chat_id"] = str(kwargs["chat_id"])
            return [
                {
                    "username": "student-id",
                    "login_secret_kind": "inline",
                    "login_secret_ref": "student-pw",
                }
            ]

    class FakeStore:
        def read_secret(self, *, ref):
            return ref.ref

    monkeypatch.setattr(pipeline, "default_secret_store", lambda settings=None: FakeStore())

    def fake_login(*, user_id, password):
        seen["login"] = f"{user_id}:{password}"
        return "s"

    monkeypatch.setattr(ku_lms, "login", fake_login)
    monkeypatch.setattr(ku_lms, "get_todo", lambda s: [])
    monkeypatch.setattr(ku_lms, "get_upcoming_events", lambda s: [])
    out = pipeline._format_telegram_assignments(
        settings=object(),
        db=FakeDb(),
        user_id=7,
        chat_id="123",
    )
    assert "마감 임박한 과제가 없습니다" in out
    assert seen == {
        "user_id": "7",
        "chat_id": "123",
        "login": "student-id:student-pw",
    }


def test_format_assignments_reads_settings_dotenv(monkeypatch, tmp_path):
    monkeypatch.delenv("KU_PORTAL_ID", raising=False)
    monkeypatch.delenv("KU_PORTAL_PW", raising=False)
    (tmp_path / ".env").write_text(
        "KU_PORTAL_ID=student-id\nKU_PORTAL_PW=student-pw\n",
        encoding="utf-8",
    )
    settings = SimpleNamespace(
        storage_root_dir=tmp_path,
        database_path=tmp_path / "data" / "ku.db",
    )

    from ku_secretary.connectors import ku_lms

    seen: dict[str, str] = {}

    def fake_login(*, user_id, password):
        seen["login"] = f"{user_id}:{password}"
        return "s"

    monkeypatch.setattr(ku_lms, "login", fake_login)
    monkeypatch.setattr(ku_lms, "get_todo", lambda s: [])
    monkeypatch.setattr(ku_lms, "get_upcoming_events", lambda s: [])

    out = pipeline._format_telegram_assignments(settings=settings)
    assert "마감 임박한 과제가 없습니다" in out
    assert seen == {"login": "student-id:student-pw"}


def test_parse_board_aliases():
    expected = {"command": "lms_board", "ok": True}
    for cmd in ("/board", "/lms_board", "/lmsboard", "/announcements", "/공지"):
        assert telegram.parse_command_message(cmd) == expected, cmd


def test_format_lms_board_renders_per_course(monkeypatch):
    monkeypatch.setenv("KU_PORTAL_ID", "uid")
    monkeypatch.setenv("KU_PORTAL_PW", "pw")
    from ku_secretary.connectors import ku_lms

    monkeypatch.setattr(ku_lms, "login", lambda *, user_id, password: "stub")
    monkeypatch.setattr(
        ku_lms,
        "get_courses",
        lambda s: [
            {"id": 11, "name": "사이버기술과법"},
            {"id": 22, "name": "빅데이터응용보안"},
        ],
    )
    monkeypatch.setattr(
        ku_lms,
        "get_announcements",
        lambda s, course_ids: [
            {
                "title": "중간고사 공지",
                "context_code": "course_11",
                "posted_at": "2026-04-25T09:00:00Z",
            }
        ],
    )

    def fake_list_boards(s, course_id):
        return (
            [{"id": 101, "name": "Q&A"}]
            if course_id == 22
            else []
        )

    def fake_list_board_posts(s, course_id, board_id, *, page=1, keyword=""):
        return {
            "posts": [
                {"title": "프로젝트 관련 질문", "posted_at": "2026-04-26T01:00:00Z"}
            ]
        }

    monkeypatch.setattr(ku_lms, "list_boards", fake_list_boards)
    monkeypatch.setattr(ku_lms, "list_board_posts", fake_list_board_posts)

    out = pipeline._format_telegram_lms_board()
    assert "[KU] 과목별 게시판/공지" in out
    assert "[사이버기술과법]" in out
    assert "공지 2026-04-25 09:00 | 중간고사 공지" in out
    assert "[빅데이터응용보안]" in out
    assert "Q&A 2026-04-26 01:00 | 프로젝트 관련 질문" in out


def test_format_lms_board_empty(monkeypatch):
    monkeypatch.setenv("KU_PORTAL_ID", "uid")
    monkeypatch.setenv("KU_PORTAL_PW", "pw")
    from ku_secretary.connectors import ku_lms
    monkeypatch.setattr(ku_lms, "login", lambda *, user_id, password: "s")
    monkeypatch.setattr(ku_lms, "get_courses", lambda s: [{"id": 1, "name": "A"}])
    monkeypatch.setattr(ku_lms, "get_announcements", lambda s, course_ids: [])
    monkeypatch.setattr(ku_lms, "list_boards", lambda s, course_id: [])
    out = pipeline._format_telegram_lms_board()
    assert "최근 글이 없습니다" in out


def test_format_lms_board_missing_credentials(monkeypatch):
    monkeypatch.delenv("KU_PORTAL_ID", raising=False)
    monkeypatch.delenv("KU_PORTAL_PW", raising=False)
    out = pipeline._format_telegram_lms_board()
    assert "환경변수가 비어 있습니다" in out


def test_format_lms_board_login_failure(monkeypatch):
    monkeypatch.setenv("KU_PORTAL_ID", "uid")
    monkeypatch.setenv("KU_PORTAL_PW", "pw")
    from ku_secretary.connectors import ku_lms

    def boom(*, user_id, password):
        raise RuntimeError("OTP enabled")

    monkeypatch.setattr(ku_lms, "login", boom)
    out = pipeline._format_telegram_lms_board()
    assert "LMS 로그인 실패" in out
    assert "OTP enabled" in out


def test_format_lms_board_course_list_failure(monkeypatch):
    monkeypatch.setenv("KU_PORTAL_ID", "uid")
    monkeypatch.setenv("KU_PORTAL_PW", "pw")
    from ku_secretary.connectors import ku_lms

    monkeypatch.setattr(ku_lms, "login", lambda *, user_id, password: "s")
    monkeypatch.setattr(
        ku_lms,
        "get_courses",
        lambda s: (_ for _ in ()).throw(RuntimeError("courses down")),
    )
    out = pipeline._format_telegram_lms_board()
    assert "강의 목록 조회 실패" in out
    assert "courses down" in out


def test_format_lms_board_skips_bad_course_and_board_ids(monkeypatch):
    monkeypatch.setenv("KU_PORTAL_ID", "uid")
    monkeypatch.setenv("KU_PORTAL_PW", "pw")
    from ku_secretary.connectors import ku_lms

    monkeypatch.setattr(ku_lms, "login", lambda *, user_id, password: "s")
    monkeypatch.setattr(
        ku_lms,
        "get_courses",
        lambda s: [
            {"id": "bad", "name": "무시"},
            {"id": 7, "name": "정상 강의"},
        ],
    )
    monkeypatch.setattr(ku_lms, "get_announcements", lambda s, course_ids: [])
    monkeypatch.setattr(
        ku_lms,
        "list_boards",
        lambda s, course_id: [{"id": "bad", "name": "무시"}],
    )
    out = pipeline._format_telegram_lms_board()
    assert "[KU] 과목별 게시판/공지" in out
    assert "최근 글이 없습니다" in out
    assert "무시" not in out


def test_dispatch_lms_board_calls_formatter(monkeypatch):
    monkeypatch.setattr(pipeline, "_format_telegram_lms_board", lambda **kwargs: "stub-board")
    monkeypatch.setattr(pipeline, "_is_telegram_chat_allowed", lambda *a, **k: True)
    monkeypatch.setattr(pipeline, "_resolve_user_scope", lambda *a, **k: {"user_id": 1})
    result = pipeline._execute_telegram_command(
        settings=object(), db=object(),
        command_payload={"command": "lms_board", "ok": True},
        chat_id="123", user_id=1,
    )
    assert result == {"ok": True, "message": "stub-board"}


def test_dispatch_assignments_calls_formatter(monkeypatch):
    monkeypatch.setattr(pipeline, "_format_telegram_assignments", lambda **kwargs: "stub-render")
    monkeypatch.setattr(pipeline, "_is_telegram_chat_allowed", lambda *a, **k: True)
    monkeypatch.setattr(pipeline, "_resolve_user_scope", lambda *a, **k: {"user_id": 1})
    result = pipeline._execute_telegram_command(
        settings=object(), db=object(),
        command_payload={"command": "assignments", "ok": True},
        chat_id="123", user_id=1,
    )
    assert result == {"ok": True, "message": "stub-render"}


def test_dispatch_library_calls_formatter(monkeypatch):
    captured: dict = {}

    def fake_format(query):
        captured["query"] = query
        return "stub-render"

    monkeypatch.setattr(pipeline, "_format_telegram_library", fake_format)

    # Stub the chat-allowed gate so we reach the library branch without
    # touching settings/db.
    monkeypatch.setattr(pipeline, "_is_telegram_chat_allowed", lambda *a, **k: True)
    monkeypatch.setattr(pipeline, "_resolve_user_scope", lambda *a, **k: {"user_id": 1})

    result = pipeline._execute_telegram_command(
        settings=object(),
        db=object(),
        command_payload={"command": "library", "ok": True, "library": "법학도서관"},
        chat_id="123",
        user_id=1,
    )
    assert result == {"ok": True, "message": "stub-render"}
    assert captured == {"query": "법학도서관"}
