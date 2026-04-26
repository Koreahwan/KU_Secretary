"""KUPID SSO-based timetable adapter.

Drop-in replacement for the ``fetch_ku_portal_timetable`` (playwright /
storage_state) path. Uses the vendored ku-portal-mcp connectors (httpx +
KUPID SSO) and emits events in the **same shape** as
``build_ku_timetable_event`` so that ``_apply_ku_portal_timetable_fetch_result``
can consume them unchanged.

Returned dict keys (consumed by the existing pipeline):
    events            list of build_ku_timetable_event() dicts
    payload_source    KUPID_SSO_TIMETABLE_SOURCE
    source_url        None (KUPID SSO has no single user-facing URL)
    current_url       None
    title             "KUPID SSO 시간표"
    fallback_used     False
    allow_empty_success  False
"""

from __future__ import annotations

import logging
import os
from typing import Any

from ku_secretary._kupid.academic import resolve_year_semester
from ku_secretary.connectors.ku_courses import get_my_courses
from ku_secretary.connectors.ku_portal import build_ku_timetable_event
from ku_secretary.connectors.ku_portal_auth import login as kupid_login
from ku_secretary.connectors.ku_timetable import get_full_timetable
from ku_secretary.secret_store import (
    SecretStore,
    StoredSecretRef,
    default_secret_store,
)

logger = logging.getLogger(__name__)

KUPID_SSO_TIMETABLE_SOURCE = "kupid_sso_timetable"

# Korean weekday → RFC 5545 BYDAY code.
_WEEKDAY_KO_TO_RRULE: dict[str, str] = {
    "월": "MO",
    "화": "TU",
    "수": "WE",
    "목": "TH",
    "금": "FR",
    "토": "SA",
    "일": "SU",
}

# resolve_year_semester() returns "1"/"2"/"summer"/"winter".
# Map to the integer-semester convention used by build_ku_timetable_event.
_TERM_LABEL_TO_INT: dict[str, int] = {
    "1": 1,
    "2": 2,
    "summer": 1,
    "winter": 2,
}


def _resolve_credentials(
    *,
    target: dict[str, Any] | None,
    settings: Any,
) -> tuple[str, str]:
    """Pull KU_PORTAL_ID/PW. Order: target.user_login_id + secret_store → env vars.

    Raises RuntimeError when neither path yields a complete credential pair.
    """
    target = dict(target or {})

    user_id = str(target.get("user_login_id") or "").strip()
    password = ""

    secret_kind = str(target.get("secret_kind") or "").strip()
    secret_ref = str(target.get("secret_ref") or "").strip()
    if secret_kind and secret_ref:
        try:
            store: SecretStore = default_secret_store(settings)
            password = store.read_secret(
                ref=StoredSecretRef(kind=secret_kind, ref=secret_ref)
            ).strip()
        except Exception as exc:
            logger.debug("kupid sso secret read failed: %s", exc)

    if not user_id:
        user_id = os.environ.get("KU_PORTAL_ID", "").strip()
    if not password:
        password = os.environ.get("KU_PORTAL_PW", "").strip()

    if not user_id or not password:
        raise RuntimeError(
            "KU_PORTAL_ID/KU_PORTAL_PW credentials missing — "
            "set env vars or attach (secret_kind, secret_ref, user_login_id) "
            "to the timetable target"
        )
    return user_id, password


def _resolve_year_semester_pair(
    *,
    settings: Any,
    year_override: str | None,
    semester_override: str | None,
) -> tuple[str | None, str | None]:
    raw_year = year_override or str(getattr(settings, "ku_openapi_year", "") or "").strip()
    raw_term = semester_override or str(getattr(settings, "ku_openapi_term", "") or "").strip()
    resolved_year, resolved_term = resolve_year_semester(
        raw_year or None, raw_term or None
    )
    return resolved_year or None, resolved_term or None


def _enrich_instructor_map(
    session: Any,
    *,
    year: str | None,
    semester: str | None,
) -> dict[str, str]:
    """Match subject names to instructor names via my_courses.

    Returns ``{subject_name: instructor}``. Errors are swallowed so that an
    instructor lookup failure never blocks the timetable sync.
    """
    try:
        courses, _total_credits = get_my_courses(
            session, year=year, semester=semester
        )
    except Exception as exc:  # noqa: BLE001 — best-effort enrichment
        logger.debug("my_courses enrichment skipped: %s", exc)
        return {}

    mapping: dict[str, str] = {}
    for course in courses:
        subject = (course.course_name or "").strip()
        instructor = (course.professor or "").strip()
        if subject and instructor:
            mapping[subject] = instructor
    return mapping


def fetch_kupid_sso_timetable(
    *,
    settings: Any,
    target: dict[str, Any] | None = None,
    timezone_name: str = "Asia/Seoul",
    year: str | None = None,
    semester: str | None = None,
) -> dict[str, Any]:
    """Fetch the student's weekly timetable via KUPID SSO.

    Args:
        settings: KU_Secretary Settings (for ku_openapi_year / ku_openapi_term
            defaults and secret_store routing).
        target: per-user target dict carrying optional secret_store refs and
            user_login_id; missing fields fall through to environment vars.
        timezone_name: tz used to build absolute start/end timestamps.
        year, semester: optional overrides; default to the academic-year
            resolver in ``ku_secretary._kupid.academic``.
    """
    user_id, password = _resolve_credentials(target=target, settings=settings)
    session = kupid_login(user_id=user_id, password=password)

    entries = get_full_timetable(session)
    resolved_year, resolved_term = _resolve_year_semester_pair(
        settings=settings,
        year_override=year,
        semester_override=semester,
    )
    academic_year_int = (
        int(resolved_year)
        if resolved_year and str(resolved_year).isdigit()
        else None
    )
    semester_int = _TERM_LABEL_TO_INT.get(resolved_term or "", None)

    instructor_by_subject = _enrich_instructor_map(
        session, year=resolved_year, semester=resolved_term
    )

    events: list[dict[str, Any]] = []
    for entry in entries:
        rrule_day = _WEEKDAY_KO_TO_RRULE.get(entry.day_of_week)
        if not rrule_day:
            logger.debug("skip unknown weekday: %s", entry.day_of_week)
            continue
        if not entry.start_time or ":" not in entry.start_time:
            logger.debug("skip entry without resolved start_time: %s", entry.subject_name)
            continue
        if not entry.end_time or ":" not in entry.end_time:
            logger.debug("skip entry without resolved end_time: %s", entry.subject_name)
            continue
        try:
            event = build_ku_timetable_event(
                weekday_code=rrule_day,
                start_hm=entry.start_time,
                end_hm=entry.end_time,
                title=entry.subject_name,
                timezone_name=timezone_name,
                location=entry.classroom or None,
                academic_year=academic_year_int,
                semester=semester_int,
                instructor=instructor_by_subject.get(
                    (entry.subject_name or "").strip()
                ),
                metadata={"period": entry.period},
                source=KUPID_SSO_TIMETABLE_SOURCE,
            )
        except Exception as exc:  # noqa: BLE001 — log + skip a single bad row
            logger.warning(
                "build_ku_timetable_event failed for %r: %s", entry.subject_name, exc
            )
            continue
        events.append(event)

    return {
        "events": events,
        "payload_source": KUPID_SSO_TIMETABLE_SOURCE,
        "source_url": None,
        "current_url": None,
        "title": "KUPID SSO 시간표",
        "fallback_used": False,
        "allow_empty_success": False,
    }
