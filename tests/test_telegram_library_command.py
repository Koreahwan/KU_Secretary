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
    for cmd in ("/assignments", "/due", "/homework", "/todo", "/to_submit", "/과제", "/제출할거", "/해야할거"):
        assert telegram.parse_command_message(cmd) == expected, cmd


def test_parse_assignment_refresh_detail_and_week():
    assert telegram.parse_command_message("/assignments refresh") == {
        "command": "assignments",
        "ok": True,
        "refresh": True,
    }
    assert telegram.parse_command_message("/assignment 2") == {
        "command": "assignment_detail",
        "ok": True,
        "index": "2",
    }
    assert telegram.parse_command_message("/week") == {"command": "assignment_week", "ok": True}


def test_parse_submitted_assignments_aliases():
    expected = {"command": "submitted_assignments", "ok": True}
    for cmd in ("/submitted", "/submissions", "/done_assignments", "/제출완료", "/낸과제"):
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
    assert "[할 일]" in out
    assert "중간고사 대체 과제" in out
    assert "04/26 23:59" in out
    assert "과제" in out
    assert "이벤트" not in out


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
    assert "[할 일]" in out
    assert "과제" in out
    assert "보고서" in out
    assert "이벤트" not in out


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


def test_format_assignments_uses_short_cache(monkeypatch, tmp_path):
    monkeypatch.setenv("KU_PORTAL_ID", "uid")
    monkeypatch.setenv("KU_PORTAL_PW", "pw")
    from ku_secretary.connectors import ku_lms
    from ku_secretary.db import Database

    db = Database(tmp_path / "ku.db")
    db.init()
    calls = {"login": 0}

    def fake_login(*, user_id, password):
        calls["login"] += 1
        return "s"

    monkeypatch.setattr(ku_lms, "login", fake_login)
    monkeypatch.setattr(
        ku_lms,
        "get_todo",
        lambda s: [{"assignment": {"name": "캐시 과제", "due_at": "2026-04-30T14:59:00Z"}}],
    )
    monkeypatch.setattr(ku_lms, "get_upcoming_events", lambda s: [])
    monkeypatch.setattr(ku_lms, "get_courses", lambda s: [])

    first = pipeline._format_telegram_assignments(db=db, user_id=7, chat_id="123")
    monkeypatch.setattr(ku_lms, "login", lambda **kwargs: pytest.fail("cache should skip login"))
    second = pipeline._format_telegram_assignments(db=db, user_id=7, chat_id="123")

    assert "캐시 과제" in first
    assert second == first
    assert calls["login"] == 1


def test_format_assignment_detail_and_week_use_assignments_cache(monkeypatch, tmp_path):
    monkeypatch.setenv("KU_PORTAL_ID", "uid")
    monkeypatch.setenv("KU_PORTAL_PW", "pw")
    from ku_secretary.connectors import ku_lms
    from ku_secretary.db import Database

    db = Database(tmp_path / "ku.db")
    db.init()
    monkeypatch.setattr(ku_lms, "login", lambda *, user_id, password: "s")
    monkeypatch.setattr(ku_lms, "get_todo", lambda s: [])
    monkeypatch.setattr(ku_lms, "get_upcoming_events", lambda s: [])
    monkeypatch.setattr(ku_lms, "get_courses", lambda s: [{"id": 11, "name": "사이버기술과법"}])
    monkeypatch.setattr(ku_lms, "get_assignments", lambda s, course_id, *, upcoming_only=False: [])
    monkeypatch.setattr(
        ku_lms,
        "get_announcements",
        lambda s, course_ids: [
            {
                "context_code": "course_11",
                "title": "보고서 제출 안내",
                "message": "보고서 과제는 2026-05-01 23:59까지 제출하세요.",
            }
        ],
    )
    monkeypatch.setattr(ku_lms, "get_modules", lambda s, course_id, *, include_items=True: [])
    monkeypatch.setattr(ku_lms, "list_boards", lambda s, course_id: [])

    listing = pipeline._format_telegram_assignments(db=db, user_id=7, chat_id="123")
    assert "- 1. 보고서 안내" in listing

    monkeypatch.setattr(ku_lms, "login", lambda **kwargs: pytest.fail("detail should use cache"))
    detail = pipeline._format_telegram_assignment_detail(index="1", db=db, user_id=7, chat_id="123")
    week = pipeline._format_telegram_assignment_week(db=db, user_id=7, chat_id="123")

    assert "[KU] 과제 상세 #1" in detail
    assert "보고서 안내" in detail
    assert "근거" in detail
    assert "[KU] 이번 주 마감" in week
    assert "- 1. 보고서 안내" in week


def test_parse_board_aliases():
    expected = {"command": "lms_board", "ok": True}
    for cmd in ("/board", "/lms_board", "/lmsboard", "/announcements", "/공지"):
        assert telegram.parse_command_message(cmd) == expected, cmd


def test_parse_materials_aliases():
    expected = {"command": "lms_materials", "ok": True}
    for cmd in ("/materials", "/material", "/files", "/자료", "/강의자료"):
        assert telegram.parse_command_message(cmd) == expected, cmd


def test_format_assignments_scans_each_course(monkeypatch):
    monkeypatch.setenv("KU_PORTAL_ID", "uid")
    monkeypatch.setenv("KU_PORTAL_PW", "pw")
    from ku_secretary.connectors import ku_lms

    monkeypatch.setattr(ku_lms, "login", lambda *, user_id, password: "s")
    monkeypatch.setattr(ku_lms, "get_todo", lambda s: [])
    monkeypatch.setattr(ku_lms, "get_upcoming_events", lambda s: [])
    monkeypatch.setattr(ku_lms, "get_courses", lambda s: [{"id": 11, "name": "사이버기술과법"}])
    monkeypatch.setattr(
        ku_lms,
        "get_assignments",
        lambda s, course_id, *, upcoming_only=False: [
            {"id": 1, "name": "개별 강의 과제", "due_at": "2026-04-30T14:00:00Z"}
        ],
    )

    out = pipeline._format_telegram_assignments()
    assert "[사이버기술과법]" in out
    assert "- 1. 개별 강의 과제" in out
    assert "  마감 04/30 23:00" in out


def test_format_assignments_skips_restricted_shell_courses(monkeypatch):
    monkeypatch.setenv("KU_PORTAL_ID", "uid")
    monkeypatch.setenv("KU_PORTAL_PW", "pw")
    from ku_secretary.connectors import ku_lms

    calls: list[int] = []
    monkeypatch.setattr(ku_lms, "login", lambda *, user_id, password: "s")
    monkeypatch.setattr(ku_lms, "get_todo", lambda s: [])
    monkeypatch.setattr(ku_lms, "get_upcoming_events", lambda s: [])
    monkeypatch.setattr(
        ku_lms,
        "get_courses",
        lambda s: [
            {"id": 72368, "access_restricted_by_date": True},
            {"id": 11, "name": "사이버기술과법"},
        ],
    )

    def fake_assignments(s, course_id, *, upcoming_only=False):
        calls.append(course_id)
        return []

    monkeypatch.setattr(ku_lms, "get_assignments", fake_assignments)
    monkeypatch.setattr(ku_lms, "get_announcements", lambda s, course_ids: [])
    monkeypatch.setattr(ku_lms, "get_modules", lambda s, course_id, *, include_items=True: [])
    monkeypatch.setattr(ku_lms, "list_boards", lambda s, course_id: [])

    out = pipeline._format_telegram_assignments()
    assert calls == [11]
    assert "확인: 1개 과목의 과제 목록과 공지/자료/게시판 제출 항목을 직접 확인했습니다." in out
    assert "일부 조회 실패" not in out


def test_format_assignments_scans_announcements_materials_and_boards(monkeypatch):
    monkeypatch.setenv("KU_PORTAL_ID", "uid")
    monkeypatch.setenv("KU_PORTAL_PW", "pw")
    from ku_secretary.connectors import ku_lms

    monkeypatch.setattr(ku_lms, "login", lambda *, user_id, password: "s")
    monkeypatch.setattr(ku_lms, "get_todo", lambda s: [])
    monkeypatch.setattr(ku_lms, "get_upcoming_events", lambda s: [])
    monkeypatch.setattr(ku_lms, "get_courses", lambda s: [{"id": 11, "name": "사이버기술과법"}])
    monkeypatch.setattr(ku_lms, "get_assignments", lambda s, course_id, *, upcoming_only=False: [])
    monkeypatch.setattr(
        ku_lms,
        "get_announcements",
        lambda s, course_ids: [
            {
                "context_code": "course_11",
                "title": "중간고사 과제 우수자 발표 안내",
                "message": "중간고사 과제 우수자는 2026-05-01 발표 예정입니다.",
            },
            {
                "context_code": "course_11",
                "title": "보고서 제출 안내",
                "message": "보고서 과제는 2026-05-01 23:59까지 제출하세요.",
            },
            {
                "context_code": "course_11",
                "title": "HW#1 공지",
                "message": "개선된 code와 ppt를 제출 바랍니다. 제출 마감: 2025.5.3 오후 7시까지",
            }
        ],
    )
    monkeypatch.setattr(
        ku_lms,
        "get_modules",
        lambda s, course_id, *, include_items=True: [
            {
                "name": "10주차",
                "items": [
                    {"title": "실습 과제 제출 2026-05-02 18:00", "type": "File"}
                ],
            }
        ],
    )
    monkeypatch.setattr(ku_lms, "list_boards", lambda s, course_id: [{"id": 3, "name": "자료실"}])
    monkeypatch.setattr(
        ku_lms,
        "list_board_posts",
        lambda s, course_id, board_id, *, page=1, keyword="": {
            "items": [{"id": 9, "title": "게시판 과제 공지"}]
        },
    )
    monkeypatch.setattr(
        ku_lms,
        "get_board_post",
        lambda s, course_id, board_id, post_id: {
            "body": "게시판 과제는 2026-05-03 12:00까지 제출"
        },
    )

    out = pipeline._format_telegram_assignments()
    assert "공지/자료/게시판 제출 항목" in out
    assert "중간고사 과제 우수자 발표 안내" not in out
    assert "[사이버기술과법]" in out
    assert "  공지 | 마감 05/01 23:59" in out
    assert "HW#1 공지" in out
    assert "  공지 | 마감 05/03 19:00" in out
    assert "  모듈/자료 | 마감 05/02 18:00" in out
    assert "  게시판 자료실 | 마감 05/03 12:00" in out
    assert "확인: 1개 과목의 과제 목록과 공지/자료/게시판 제출 항목을 직접 확인했습니다." in out


def test_format_submitted_assignments_renders_submission_status(monkeypatch):
    monkeypatch.setenv("KU_PORTAL_ID", "uid")
    monkeypatch.setenv("KU_PORTAL_PW", "pw")
    from ku_secretary.connectors import ku_lms

    monkeypatch.setattr(ku_lms, "login", lambda *, user_id, password: "s")
    monkeypatch.setattr(ku_lms, "get_courses", lambda s: [{"id": 11, "name": "사이버기술과법"}])
    monkeypatch.setattr(
        ku_lms,
        "get_submissions",
        lambda s, course_id: [
            {
                "submitted_at": "2026-04-25T12:30:00Z",
                "workflow_state": "submitted",
                "late": False,
                "assignment": {
                    "name": "완료한 과제",
                    "due_at": "2026-04-26T14:59:00Z",
                },
            },
            {
                "submitted_at": None,
                "workflow_state": "unsubmitted",
                "assignment": {"name": "아직 안 낸 과제"},
            },
        ],
    )

    out = pipeline._format_telegram_submitted_assignments()
    assert "[KU] 제출 완료 과제" in out
    assert "[사이버기술과법]" in out
    assert "- 완료한 과제" in out
    assert "  제출 04/25 21:30 | 제출됨 | 마감 04/26 23:59" in out
    assert "아직 안 낸 과제" not in out


def test_format_submitted_assignments_compacts_long_course_names(monkeypatch):
    monkeypatch.setenv("KU_PORTAL_ID", "uid")
    monkeypatch.setenv("KU_PORTAL_PW", "pw")
    from ku_secretary.connectors import ku_lms

    monkeypatch.setattr(ku_lms, "login", lambda *, user_id, password: "s")
    monkeypatch.setattr(
        ku_lms,
        "get_courses",
        lambda s: [
            {
                "id": 11,
                "name": "261R (서울-학부)사이버기술과법(CYBER TECHNOLOGY AND LAW)-00분반",
            }
        ],
    )
    monkeypatch.setattr(
        ku_lms,
        "get_submissions",
        lambda s, course_id: [
            {
                "submitted_at": "2026-04-25T12:30:00Z",
                "workflow_state": "graded",
                "grade": "24",
                "assignment": {
                    "name": "긴 과제명",
                    "due_at": "2026-04-26T14:59:00Z",
                },
            }
        ],
    )

    out = pipeline._format_telegram_submitted_assignments()
    assert "261R" not in out
    assert "CYBER TECHNOLOGY" not in out
    assert "[사이버기술과법 00분반]" in out
    assert "  제출 04/25 21:30 | 채점됨 | 마감 04/26 23:59 | 성적 24" in out


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
    assert "- 중간고사 공지" in out
    assert "  공지 | 04/25 18:00" in out
    assert "[빅데이터응용보안]" in out
    assert "- 프로젝트 관련 질문" in out
    assert "  Q&A | 04/26 10:00" in out


def test_format_lms_board_reads_items_payload_and_more_boards(monkeypatch):
    monkeypatch.setenv("KU_PORTAL_ID", "uid")
    monkeypatch.setenv("KU_PORTAL_PW", "pw")
    from ku_secretary.connectors import ku_lms

    monkeypatch.setattr(ku_lms, "login", lambda *, user_id, password: "s")
    monkeypatch.setattr(ku_lms, "get_courses", lambda s: [{"id": 7, "name": "자료 많은 강의"}])
    monkeypatch.setattr(ku_lms, "get_announcements", lambda s, course_ids: [])
    monkeypatch.setattr(
        ku_lms,
        "list_boards",
        lambda s, course_id: [
            {"id": 1, "name": "보드1"},
            {"id": 2, "name": "보드2"},
            {"id": 3, "name": "보드3"},
            {"id": 4, "name": "강의자료실"},
        ],
    )

    def fake_posts(s, course_id, board_id, *, page=1, keyword=""):
        if board_id == 4:
            return {"items": [{"title": "네번째 보드 자료", "created_at": "2026-04-26T02:00:00Z"}]}
        return {"items": []}

    monkeypatch.setattr(ku_lms, "list_board_posts", fake_posts)
    out = pipeline._format_telegram_lms_board()
    assert "- 네번째 보드 자료" in out
    assert "  강의자료실 | 04/26 11:00" in out
    assert "과목당 최대" in out


def test_format_lms_materials_scans_modules_and_boards(monkeypatch):
    monkeypatch.setenv("KU_PORTAL_ID", "uid")
    monkeypatch.setenv("KU_PORTAL_PW", "pw")
    from ku_secretary.connectors import ku_lms

    monkeypatch.setattr(ku_lms, "login", lambda *, user_id, password: "s")
    monkeypatch.setattr(ku_lms, "get_courses", lambda s: [{"id": 5, "name": "운영체제"}])
    monkeypatch.setattr(
        ku_lms,
        "get_modules",
        lambda s, course_id, *, include_items=True: [
            {"name": "9주차", "items": [{"type": "File", "title": "스케줄링.pdf"}]}
        ],
    )
    monkeypatch.setattr(ku_lms, "list_boards", lambda s, course_id: [{"id": 9, "name": "공지"}])
    monkeypatch.setattr(
        ku_lms,
        "list_board_posts",
        lambda s, course_id, board_id, *, page=1, keyword="": {
            "items": [{"title": "보강 자료 업로드"}]
        },
    )

    out = pipeline._format_telegram_lms_materials()
    assert "[KU] 강의자료 위치" in out
    assert "[운영체제]" in out
    assert "- 스케줄링.pdf" in out
    assert "  주차자료 파일 (9주차)" in out
    assert "- 보강 자료 업로드" in out
    assert "  게시판 공지" in out


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


def test_dispatch_lms_materials_calls_formatter(monkeypatch):
    monkeypatch.setattr(pipeline, "_format_telegram_lms_materials", lambda **kwargs: "stub-materials")
    monkeypatch.setattr(pipeline, "_is_telegram_chat_allowed", lambda *a, **k: True)
    monkeypatch.setattr(pipeline, "_resolve_user_scope", lambda *a, **k: {"user_id": 1})
    result = pipeline._execute_telegram_command(
        settings=object(), db=object(),
        command_payload={"command": "lms_materials", "ok": True},
        chat_id="123", user_id=1,
    )
    assert result == {"ok": True, "message": "stub-materials"}


def test_dispatch_submitted_assignments_calls_formatter(monkeypatch):
    monkeypatch.setattr(pipeline, "_format_telegram_submitted_assignments", lambda **kwargs: "stub-submitted")
    monkeypatch.setattr(pipeline, "_is_telegram_chat_allowed", lambda *a, **k: True)
    monkeypatch.setattr(pipeline, "_resolve_user_scope", lambda *a, **k: {"user_id": 1})
    result = pipeline._execute_telegram_command(
        settings=object(), db=object(),
        command_payload={"command": "submitted_assignments", "ok": True},
        chat_id="123", user_id=1,
    )
    assert result == {"ok": True, "message": "stub-submitted"}


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
