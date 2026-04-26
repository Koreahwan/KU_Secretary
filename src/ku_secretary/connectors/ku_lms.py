"""Korea University Canvas LMS connector (mylms.korea.ac.kr).

Wraps the vendored ku-portal-mcp `lms` module. Authentication uses KSSO SAML
SSO with RSA-decrypted Canvas password handoff — see the upstream module for
flow details. Note: KSSO accounts with OTP enabled cannot complete this flow.

Public helpers are synchronous so KU_Secretary's CLI/job code can call them
directly. The vendored module is async-first.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from ku_secretary._kupid import lms as _kupid_lms
from ku_secretary._kupid.lms import LMSSession
from ku_secretary.secret_store import SecretStore, StoredSecretRef

logger = logging.getLogger(__name__)

LMS_ID_KEY = "ku_portal_id"
LMS_PASSWORD_KEY = "ku_portal_password"


def configure_session_cache(cache_dir: str | Path) -> Path:
    """Redirect the upstream LMS session cache to *cache_dir*.

    Call once at startup with the KU_Secretary data directory so that the
    cached `lms_session.json` lives alongside the rest of the app's state.
    """
    resolved = Path(cache_dir).expanduser().resolve()
    resolved.mkdir(parents=True, exist_ok=True)
    _kupid_lms.CACHE_DIR = resolved
    _kupid_lms.LMS_SESSION_FILE = resolved / "lms_session.json"
    return resolved


def login(*, user_id: str, password: str) -> LMSSession:
    """Run KSSO SAML → Canvas login synchronously and return an LMSSession.

    Reuses the cached session if it is still valid (~25 min TTL). The
    supplied credentials are only used when a fresh login is required.
    """
    user_id = (user_id or "").strip()
    if not user_id or not password:
        raise ValueError("user_id and password are required for LMS login")
    return asyncio.run(_kupid_lms.lms_login(user_id, password))


def login_with_secret_store(
    *,
    store: SecretStore,
    id_ref: StoredSecretRef,
    password_ref: StoredSecretRef,
) -> LMSSession:
    """Read credentials from *store* and run :func:`login`."""
    user_id = store.read_secret(ref=id_ref).strip()
    password = store.read_secret(ref=password_ref)
    return login(user_id=user_id, password=password)


def clear_session() -> None:
    """Remove the cached LMS session file (forces re-login on next call)."""
    _kupid_lms._clear_lms_session()


def session_cache_path() -> Path:
    """Resolved path of the LMS session cache file."""
    return Path(_kupid_lms.LMS_SESSION_FILE)


# ---- read-only Canvas API helpers ------------------------------------


def get_courses(session: LMSSession) -> list[dict]:
    """List courses the user is enrolled in this term."""
    return asyncio.run(_kupid_lms.fetch_lms_courses(session))


def get_assignments(
    session: LMSSession,
    course_id: int,
    *,
    upcoming_only: bool = False,
) -> list[dict]:
    """List assignments for a course. ``upcoming_only`` filters to bucket=upcoming."""
    return asyncio.run(
        _kupid_lms.fetch_lms_assignments(session, course_id, upcoming_only)
    )


def get_modules(
    session: LMSSession,
    course_id: int,
    *,
    include_items: bool = True,
) -> list[dict]:
    """List weekly modules and (optionally) the items inside them."""
    return asyncio.run(
        _kupid_lms.fetch_lms_modules(session, course_id, include_items)
    )


def get_todo(session: LMSSession) -> list[dict]:
    """Canvas's todo feed for the user."""
    return asyncio.run(_kupid_lms.fetch_lms_todo(session))


def get_upcoming_events(session: LMSSession) -> list[dict]:
    """Upcoming calendar events for the user."""
    return asyncio.run(_kupid_lms.fetch_lms_upcoming_events(session))


def get_dashboard(session: LMSSession) -> list[dict]:
    """Dashboard cards (active courses + summary)."""
    return asyncio.run(_kupid_lms.fetch_lms_dashboard(session))


def get_announcements(session: LMSSession, course_ids: list[int]) -> list[dict]:
    """Course announcements filtered by course id list."""
    return asyncio.run(_kupid_lms.fetch_lms_announcements(session, course_ids))


def get_grades(session: LMSSession, course_id: int) -> list[dict]:
    """Enrollment grades (current/final score and grade) for a course."""
    return asyncio.run(_kupid_lms.fetch_lms_grades(session, course_id))


def get_submissions(session: LMSSession, course_id: int) -> list[dict]:
    """Submission status for the user across a course's assignments."""
    return asyncio.run(_kupid_lms.fetch_lms_submissions(session, course_id))


def get_quizzes(session: LMSSession, course_id: int) -> list[dict]:
    """Classic quizzes for a course (with assignment fallback)."""
    return asyncio.run(_kupid_lms.fetch_lms_quizzes(session, course_id))


def get_syllabus(session: LMSSession, course_id: int) -> dict:
    """Course syllabus (Canvas course object with `syllabus_body`)."""
    return asyncio.run(_kupid_lms.fetch_lms_syllabus(session, course_id))


def download_file(
    session: LMSSession,
    file_id: int,
    save_dir: str | Path,
    *,
    filename: str | None = None,
) -> dict:
    """Download a Canvas file to *save_dir* (absolute path).

    Returns dict with path, filename, size, content_type.
    """
    target = Path(save_dir).expanduser().resolve()
    return asyncio.run(
        _kupid_lms.download_lms_file(session, file_id, target, filename)
    )


# ---- LearningX board (per-course Q&A / 강의자료실) -------------------


def list_boards(session: LMSSession, course_id: int) -> list[dict]:
    """Boards (Q&A, 강의자료실 등) for a course."""
    return asyncio.run(_kupid_lms.fetch_lms_boards(session, course_id))


def list_board_posts(
    session: LMSSession,
    course_id: int,
    board_id: int,
    *,
    page: int = 1,
    keyword: str = "",
) -> dict:
    """Paged listing of board posts."""
    return asyncio.run(
        _kupid_lms.fetch_lms_board_posts(
            session, course_id, board_id, page, keyword
        )
    )


def get_board_post(
    session: LMSSession,
    course_id: int,
    board_id: int,
    post_id: int,
) -> dict:
    """Single board post with attachments (incl. canvas_file_id)."""
    return asyncio.run(
        _kupid_lms.fetch_lms_board_post(session, course_id, board_id, post_id)
    )
