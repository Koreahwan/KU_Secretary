from __future__ import annotations

from datetime import datetime, timedelta
import json
from pathlib import Path
from types import SimpleNamespace

from ku_secretary.connectors.google_calendar import (
    GoogleCalendarClient,
    google_calendar_event_id,
)
from ku_secretary.db import Database
from ku_secretary.jobs import pipeline


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeGoogleSession:
    def __init__(self):
        self.requests: list[tuple[str, str, dict]] = []

    def request(self, method: str, url: str, **kwargs):
        self.requests.append((method, url, kwargs))
        if method == "PUT":
            return _FakeResponse(404)
        return _FakeResponse(200, {"htmlLink": "https://calendar.google/item"})


def test_google_calendar_client_inserts_after_missing_update() -> None:
    session = _FakeGoogleSession()
    client = GoogleCalendarClient(
        access_token="token",
        calendar_id="primary",
        session=session,
        api_base="https://calendar.example",
    )

    result = client.upsert_event(
        event_id="kus12345",
        payload={
            "summary": "KU 과제: HW 1",
            "start": {"dateTime": "2026-05-01T23:29:00+09:00"},
            "end": {"dateTime": "2026-05-01T23:59:00+09:00"},
        },
    )

    assert result.action == "created"
    assert [item[0] for item in session.requests] == ["PUT", "POST"]
    assert session.requests[1][2]["json"]["id"] == "kus12345"


def test_google_calendar_event_id_is_stable() -> None:
    first = google_calendar_event_id(user_id=7, source="uclass", external_id="uclass:assign:1")
    second = google_calendar_event_id(user_id=7, source="uclass", external_id="uclass:assign:1")

    assert first == second
    assert first.startswith("kus")


def _settings(tmp_path: Path) -> SimpleNamespace:
    token_file = tmp_path / "google_token.json"
    token_file.write_text(json.dumps({"access_token": "token"}), encoding="utf-8")
    return SimpleNamespace(
        google_calendar_sync_enabled=True,
        google_calendar_id="primary",
        google_calendar_token_file=token_file,
        google_calendar_credentials_file=None,
        google_calendar_task_duration_min=60,
        google_calendar_sync_window_days=30,
        timezone="Asia/Seoul",
    )


def test_sync_google_calendar_upserts_uclass_tasks_and_exam_events(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db = Database(tmp_path / "ku.db")
    db.init()
    due_at = (datetime.now().astimezone() + timedelta(days=3)).replace(
        hour=23,
        minute=59,
        second=59,
        microsecond=0,
    )
    timed_due_at = (due_at + timedelta(days=1)).replace(hour=15, minute=0, second=0)
    past_due_at = (datetime.now().astimezone() - timedelta(days=30)).replace(
        hour=23,
        minute=59,
        second=59,
        microsecond=0,
    )
    event_start = (due_at + timedelta(days=2)).replace(hour=15, minute=30, second=0)
    event_end = event_start + timedelta(hours=1)
    db.upsert_task(
        external_id="uclass:assign:1",
        source="uclass",
        due_at=due_at.isoformat(),
        title="HW 1",
        status="open",
        metadata_json={"course_name": "Algorithms 00분반"},
    )
    db.upsert_task(
        external_id="uclass:assign:midterm-replacement",
        source="uclass",
        due_at=timed_due_at.isoformat(),
        title="중간고사 대체 과제",
        status="open",
        metadata_json={"course_name": "Cyber Law", "item_type": "assignment"},
    )
    db.upsert_event(
        external_id="uclass:event:quiz",
        source="uclass",
        start=event_start.isoformat(),
        end=event_end.isoformat(),
        title="Quiz 1",
        location="Online",
        rrule=None,
        metadata_json={"course_name": "Algorithms 05분반"},
    )
    db.upsert_event(
        external_id="portal:class:1",
        source="portal",
        start=event_start.isoformat(),
        end=event_end.isoformat(),
        title="정규 강의",
        location="Room 1",
        rrule=None,
        metadata_json={},
    )
    db.upsert_task(
        external_id="inbox:deadline:1",
        source="inbox",
        due_at=due_at.isoformat(),
        title="도서관 책 반납",
        status="open",
        metadata_json={},
    )
    db.upsert_task(
        external_id="uclass:assign:past",
        source="uclass",
        due_at=past_due_at.isoformat(),
        title="Final exam",
        status="done",
        metadata_json={"course_name": "Computer Security"},
    )

    calls: list[tuple[str, dict]] = []

    class _FakeGoogleCalendarClient:
        @classmethod
        def from_oauth_token_file(cls, **kwargs):
            return cls()

        def upsert_event(self, *, event_id: str, payload: dict):
            calls.append((event_id, payload))
            return SimpleNamespace(action="created")

    monkeypatch.setattr(pipeline, "GoogleCalendarClient", _FakeGoogleCalendarClient)
    monkeypatch.setattr(
        pipeline,
        "_lms_calendar_persist_canvas_records",
        lambda *args, **kwargs: {"skipped": True},
    )

    result = pipeline.sync_google_calendar(_settings(tmp_path), db)

    assert result["ok"] is True
    assert result["selected_tasks"] == 4
    assert result["selected_events"] == 1
    assert result["created_events"] == 5
    summaries = [payload["summary"] for _, payload in calls]
    assert "Algorithms 과제" in summaries
    assert "Algorithms 00분반 과제" not in summaries
    assert "Law 과제 15:00" in summaries
    assert "Law 시험" not in summaries
    assert "도서관 책 반납 과제" in summaries
    assert "Computer Security 시험" not in summaries
    assert "Algorithms 퀴즈 15:30" in summaries
    assert "Algorithms 05분반 퀴즈 15:30" not in summaries
    hw_payload = next(payload for _, payload in calls if payload["summary"] == "Algorithms 과제")
    past_payload = next(payload for _, payload in calls if "Final exam" in payload["description"])
    assert hw_payload["start"] == {"date": due_at.date().isoformat()}
    assert hw_payload["end"] == {"date": (due_at.date() + timedelta(days=1)).isoformat()}
    assert hw_payload["reminders"] == {"useDefault": False, "overrides": []}
    assert "내용: HW 1" in hw_payload["description"]
    assert "마감:" not in hw_payload["description"]
    assert "분류:" not in hw_payload["description"]
    assert "과목:" not in hw_payload["description"]
    assert "외부 ID" not in hw_payload["description"]
    assert past_payload["summary"].endswith(" [완료]")
    assert "\u0336" not in past_payload["summary"]
    assert "상태: 완료" in past_payload["description"]
    timed_payload = next(payload for _, payload in calls if payload["summary"] == "Law 과제 15:00")
    assert timed_payload["start"] == {"date": timed_due_at.date().isoformat()}
    assert timed_payload["end"] == {"date": (timed_due_at.date() + timedelta(days=1)).isoformat()}
    event_payload = next(payload for _, payload in calls if payload["summary"] == "Algorithms 퀴즈 15:30")
    assert event_payload["start"] == {"date": event_start.date().isoformat()}
    assert event_payload["end"] == {"date": (event_start.date() + timedelta(days=1)).isoformat()}
    assert event_payload["reminders"] == {"useDefault": False, "overrides": []}


def test_lms_calendar_persist_canvas_records_stores_completed_assignments(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db = Database(tmp_path / "ku.db")
    db.init()
    due_at = (datetime.now().astimezone() - timedelta(days=7)).replace(microsecond=0)
    settings = _settings(tmp_path)

    class _FakeKuLms:
        @staticmethod
        def login(*, user_id: str, password: str):
            return object()

        @staticmethod
        def get_courses(session):
            return [{"id": 101, "name": "AI Security"}]

        @staticmethod
        def get_submissions(session, course_id: int):
            return [
                {
                    "assignment_id": 501,
                    "submitted_at": due_at.isoformat(),
                    "workflow_state": "graded",
                    "assignment": {
                        "id": 501,
                        "name": "Final presentation",
                        "due_at": due_at.isoformat(),
                        "html_url": "https://canvas.example/assignments/501",
                    },
                }
            ]

        @staticmethod
        def get_assignments(session, course_id: int, *, upcoming_only: bool = False):
            return [
                {
                    "id": 501,
                    "name": "Final presentation",
                    "due_at": due_at.isoformat(),
                    "html_url": "https://canvas.example/assignments/501",
                }
            ]

        @staticmethod
        def get_quizzes(session, course_id: int):
            return []

        @staticmethod
        def get_modules(session, course_id: int, *, include_items: bool = True):
            return []

        @staticmethod
        def list_boards(session, course_id: int):
            return []

        @staticmethod
        def get_announcements(session, course_ids: list[int]):
            return []

    monkeypatch.setattr(
        pipeline,
        "_resolve_telegram_lms_credentials",
        lambda **kwargs: ("2024000000", "pw"),
    )
    import ku_secretary.connectors.ku_lms as ku_lms

    monkeypatch.setattr(ku_lms, "login", _FakeKuLms.login)
    monkeypatch.setattr(ku_lms, "get_courses", _FakeKuLms.get_courses)
    monkeypatch.setattr(ku_lms, "get_submissions", _FakeKuLms.get_submissions)
    monkeypatch.setattr(ku_lms, "get_assignments", _FakeKuLms.get_assignments)
    monkeypatch.setattr(ku_lms, "get_quizzes", _FakeKuLms.get_quizzes)
    monkeypatch.setattr(ku_lms, "get_announcements", _FakeKuLms.get_announcements)
    monkeypatch.setattr(ku_lms, "get_modules", _FakeKuLms.get_modules)
    monkeypatch.setattr(ku_lms, "list_boards", _FakeKuLms.list_boards)

    result = pipeline._lms_calendar_persist_canvas_records(
        settings,
        db,
        user_id=0,
    )

    assert result["upserted_tasks"] >= 1
    tasks = db.list_tasks(open_only=False, user_id=0)
    task = next(item for item in tasks if item.external_id == "ku_lms:assignment:101:501")
    assert task.status == "done"
    assert task.due_at is not None
    assert "Final presentation" == task.title


def test_lms_calendar_persist_canvas_records_stores_source_hints_when_assignments_empty(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db = Database(tmp_path / "ku.db")
    db.init()
    settings = _settings(tmp_path)

    class _FakeKuLms:
        @staticmethod
        def login(*, user_id: str, password: str):
            return object()

        @staticmethod
        def get_courses(session):
            return [{"id": 88613, "name": "빅데이터응용보안 00분반"}]

        @staticmethod
        def get_submissions(session, course_id: int):
            return []

        @staticmethod
        def get_assignments(session, course_id: int, *, upcoming_only: bool = False):
            return []

        @staticmethod
        def get_quizzes(session, course_id: int):
            return []

        @staticmethod
        def get_announcements(session, course_ids: list[int]):
            return [
                {
                    "title": "HW#1 공지",
                    "message": "코드를 개선하여 제출하세요. 제출 마감: 2026.5.3 오후 7시까지",
                    "html_url": "https://canvas.example/announcements/1",
                }
            ]

        @staticmethod
        def get_modules(session, course_id: int, *, include_items: bool = True):
            return []

        @staticmethod
        def list_boards(session, course_id: int):
            return []

    monkeypatch.setattr(
        pipeline,
        "_resolve_telegram_lms_credentials",
        lambda **kwargs: ("2024000000", "pw"),
    )
    import ku_secretary.connectors.ku_lms as ku_lms

    monkeypatch.setattr(ku_lms, "login", _FakeKuLms.login)
    monkeypatch.setattr(ku_lms, "get_courses", _FakeKuLms.get_courses)
    monkeypatch.setattr(ku_lms, "get_submissions", _FakeKuLms.get_submissions)
    monkeypatch.setattr(ku_lms, "get_assignments", _FakeKuLms.get_assignments)
    monkeypatch.setattr(ku_lms, "get_quizzes", _FakeKuLms.get_quizzes)
    monkeypatch.setattr(ku_lms, "get_announcements", _FakeKuLms.get_announcements)
    monkeypatch.setattr(ku_lms, "get_modules", _FakeKuLms.get_modules)
    monkeypatch.setattr(ku_lms, "list_boards", _FakeKuLms.list_boards)

    result = pipeline._lms_calendar_persist_canvas_records(
        settings,
        db,
        user_id=0,
    )

    assert result["upserted_tasks"] == 1
    tasks = db.list_tasks(open_only=False, user_id=0)
    hint_task = next(item for item in tasks if item.external_id.startswith("ku_lms:source-hint:88613:"))
    assert hint_task.title == "HW#1 공지"
    assert hint_task.due_at is not None
    assert "2026-05-03" in hint_task.due_at
    with db.connection() as conn:
        cache_rows = conn.execute(
            """
            SELECT source_kind, title, body_text, parsed_task_ids_json
            FROM lms_source_cache
            WHERE user_id = 0 AND course_id = 88613
            """
        ).fetchall()
    assert len(cache_rows) == 1
    assert cache_rows[0]["source_kind"] == "announcement"
    assert cache_rows[0]["title"] == "HW#1 공지"
    assert "제출 마감" in cache_rows[0]["body_text"]
    assert "ku_lms:source-hint:88613:" in cache_rows[0]["parsed_task_ids_json"]


def test_lms_calendar_persist_canvas_records_stores_my_presentation_notice(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db = Database(tmp_path / "ku.db")
    db.init()
    settings = _settings(tmp_path)
    detail_calls = {"count": 0}

    class _FakeSession:
        user_id = "2099000000"
        user_name = "김학생(2099####00)"

    class _FakeKuLms:
        @staticmethod
        def login(*, user_id: str, password: str):
            return _FakeSession()

        @staticmethod
        def get_courses(session):
            return [{"id": 88612, "name": "사이버기술과법 00분반"}]

        @staticmethod
        def get_submissions(session, course_id: int):
            return []

        @staticmethod
        def get_assignments(session, course_id: int, *, upcoming_only: bool = False):
            return [
                {
                    "id": 229427,
                    "name": "중간고사 대체 과제",
                    "due_at": "2026-04-26T14:59:59+00:00",
                    "html_url": "https://canvas.example/assignments/229427",
                }
            ]

        @staticmethod
        def get_quizzes(session, course_id: int):
            return []

        @staticmethod
        def get_announcements(session, course_ids: list[int]):
            return []

        @staticmethod
        def get_modules(session, course_id: int, *, include_items: bool = True):
            return []

        @staticmethod
        def list_boards(session, course_id: int):
            return [{"id": 46312, "name": "강의자료실"}]

        @staticmethod
        def list_board_posts(session, course_id: int, board_id: int, *, page: int = 1, keyword: str = ""):
            return {
                "posts": [
                    {
                        "id": 312441,
                        "title": "2026년 1학기 사이버기술과법 중간고사 과제 발표 대상자 안내(04.29)",
                    }
                ]
            }

        @staticmethod
        def get_board_post(session, course_id: int, board_id: int, post_id: int):
            detail_calls["count"] += 1
            return {
                "id": post_id,
                "title": "2026년 1학기 사이버기술과법 중간고사 과제 발표 대상자 안내(04.29)",
                "body": "내일 수업에서 중간고사 대체 과제 발표를 진행합니다. 2099000000 김학생",
            }

    monkeypatch.setattr(
        pipeline,
        "_resolve_telegram_lms_credentials",
        lambda **kwargs: ("2099000000", "pw"),
    )
    import ku_secretary.connectors.ku_lms as ku_lms

    monkeypatch.setattr(ku_lms, "login", _FakeKuLms.login)
    monkeypatch.setattr(ku_lms, "get_courses", _FakeKuLms.get_courses)
    monkeypatch.setattr(ku_lms, "get_submissions", _FakeKuLms.get_submissions)
    monkeypatch.setattr(ku_lms, "get_assignments", _FakeKuLms.get_assignments)
    monkeypatch.setattr(ku_lms, "get_quizzes", _FakeKuLms.get_quizzes)
    monkeypatch.setattr(ku_lms, "get_announcements", _FakeKuLms.get_announcements)
    monkeypatch.setattr(ku_lms, "get_modules", _FakeKuLms.get_modules)
    monkeypatch.setattr(ku_lms, "list_boards", _FakeKuLms.list_boards)
    monkeypatch.setattr(ku_lms, "list_board_posts", _FakeKuLms.list_board_posts)
    monkeypatch.setattr(ku_lms, "get_board_post", _FakeKuLms.get_board_post)

    result = pipeline._lms_calendar_persist_canvas_records(
        settings,
        db,
        user_id=1,
    )

    assert result["upserted_tasks"] == 2
    assert detail_calls["count"] == 1
    tasks = db.list_tasks(open_only=False, user_id=1)
    presentation = next(
        item for item in tasks if item.external_id.startswith("ku_lms:presentation:88612:")
    )
    assert presentation.title == "2026년 1학기 사이버기술과법 중간고사 과제 발표 대상자 안내 04.29"
    assert presentation.due_at is not None
    assert "2026-04-29" in presentation.due_at
    with db.connection() as conn:
        cache_row = conn.execute(
            """
            SELECT source_kind, source_id, title, body_text, parsed_task_ids_json
            FROM lms_source_cache
            WHERE user_id = 1 AND course_id = 88612 AND source_kind = 'board_post'
            LIMIT 1
            """
        ).fetchone()
    assert cache_row is not None
    assert cache_row["source_id"] == "board:46312:post:312441"
    assert "2099000000 김학생" in cache_row["body_text"]
    assert "ku_lms:presentation:88612:" in cache_row["parsed_task_ids_json"]

    second = pipeline._lms_calendar_persist_canvas_records(
        settings,
        db,
        user_id=1,
    )

    assert second["source_cache"]["fresh_hits"] >= 1
    assert detail_calls["count"] == 1
