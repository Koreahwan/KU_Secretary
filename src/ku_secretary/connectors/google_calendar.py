from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha1
import json
from pathlib import Path
from typing import Any
from urllib.parse import quote

from dateutil import parser as dt_parser
import requests


GOOGLE_CALENDAR_API_BASE = "https://www.googleapis.com/calendar/v3"
GOOGLE_OAUTH_TOKEN_URI = "https://oauth2.googleapis.com/token"
GOOGLE_CALENDAR_SCOPE = "https://www.googleapis.com/auth/calendar.events"


@dataclass
class GoogleCalendarUpsertResult:
    event_id: str
    action: str
    status_code: int
    html_link: str | None = None
    reason: str | None = None


def google_calendar_event_id(*, user_id: int | None, source: str, external_id: str) -> str:
    seed = f"{int(user_id or 0)}|{source}|{external_id}"
    return "kus" + sha1(seed.encode("utf-8")).hexdigest()


def _parse_expiry(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt_parser.isoparse(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _oauth_client_from_credentials(path: Path | None) -> dict[str, str]:
    if not path:
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return {}
    section = payload.get("installed") or payload.get("web") or payload
    if not isinstance(section, dict):
        return {}
    return {
        "client_id": str(section.get("client_id") or "").strip(),
        "client_secret": str(section.get("client_secret") or "").strip(),
        "token_uri": str(section.get("token_uri") or GOOGLE_OAUTH_TOKEN_URI).strip()
        or GOOGLE_OAUTH_TOKEN_URI,
    }


def _token_has_live_access_token(token: dict[str, Any], *, now: datetime | None = None) -> bool:
    access_token = str(token.get("access_token") or token.get("token") or "").strip()
    if not access_token:
        return False
    expiry = _parse_expiry(token.get("expiry") or token.get("expires_at"))
    if expiry is None:
        if str(token.get("refresh_token") or "").strip():
            return False
        return True
    current = now or datetime.now(timezone.utc)
    return expiry > current + timedelta(seconds=60)


class GoogleCalendarClient:
    def __init__(
        self,
        *,
        access_token: str,
        calendar_id: str = "primary",
        session: requests.Session | None = None,
        api_base: str = GOOGLE_CALENDAR_API_BASE,
    ) -> None:
        token = str(access_token or "").strip()
        if not token:
            raise ValueError("Google Calendar access token is required")
        self.access_token = token
        self.calendar_id = str(calendar_id or "primary").strip() or "primary"
        self.session = session or requests.Session()
        self.api_base = str(api_base or GOOGLE_CALENDAR_API_BASE).rstrip("/")

    @classmethod
    def from_oauth_token_file(
        cls,
        *,
        token_file: Path,
        credentials_file: Path | None = None,
        calendar_id: str = "primary",
        session: requests.Session | None = None,
        api_base: str = GOOGLE_CALENDAR_API_BASE,
    ) -> "GoogleCalendarClient":
        token_path = Path(token_file).expanduser()
        token = json.loads(token_path.read_text(encoding="utf-8"))
        if not isinstance(token, dict):
            raise ValueError("Google Calendar token file must contain a JSON object")

        if not _token_has_live_access_token(token):
            oauth_client = _oauth_client_from_credentials(credentials_file)
            refresh_token = str(token.get("refresh_token") or "").strip()
            client_id = str(token.get("client_id") or oauth_client.get("client_id") or "").strip()
            client_secret = str(
                token.get("client_secret") or oauth_client.get("client_secret") or ""
            ).strip()
            token_uri = str(token.get("token_uri") or oauth_client.get("token_uri") or "").strip()
            token.update(
                refresh_google_access_token(
                    refresh_token=refresh_token,
                    client_id=client_id,
                    client_secret=client_secret,
                    token_uri=token_uri,
                    session=session,
                )
            )
            token_path.write_text(
                json.dumps(token, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )

        access_token = str(token.get("access_token") or token.get("token") or "").strip()
        return cls(
            access_token=access_token,
            calendar_id=calendar_id,
            session=session,
            api_base=api_base,
        )

    def upsert_event(
        self,
        *,
        event_id: str,
        payload: dict[str, Any],
    ) -> GoogleCalendarUpsertResult:
        clean_event_id = str(event_id or "").strip()
        if not clean_event_id:
            raise ValueError("Google Calendar event id is required")
        body = dict(payload)
        body["id"] = clean_event_id
        ownership = _ku_secretary_ownership(body)
        if ownership:
            _attach_payload_hash(body)
            existing_response = self._request("GET", self._event_url(clean_event_id))
            if existing_response.status_code == 404:
                insert_response = self._request("POST", self._events_url(), json=body)
                if insert_response.status_code == 409:
                    existing_response = self._request("GET", self._event_url(clean_event_id))
                else:
                    insert_response.raise_for_status()
                    return _result_from_response(clean_event_id, "created", insert_response)

            if existing_response.status_code != 404:
                existing_response.raise_for_status()
                existing = _response_json_object(existing_response)
                guard_reason = _manual_edit_guard_reason(
                    existing=existing,
                    expected=body,
                    ownership=ownership,
                )
                if guard_reason:
                    color_id = str(body.get("colorId") or "").strip()
                    if color_id and str(existing.get("colorId") or "").strip() != color_id:
                        color_response = self.patch_event_color(
                            event_id=clean_event_id,
                            color_id=color_id,
                        )
                        color_response.reason = guard_reason
                        return color_response
                    return GoogleCalendarUpsertResult(
                        event_id=clean_event_id,
                        action="skipped",
                        status_code=int(existing_response.status_code),
                        html_link=_html_link_from_payload(existing),
                        reason=guard_reason,
                    )

            update_response = self._request(
                "PUT",
                self._event_url(clean_event_id),
                json=body,
            )
            update_response.raise_for_status()
            return _result_from_response(clean_event_id, "updated", update_response)

        update_response = self._request(
            "PUT",
            self._event_url(clean_event_id),
            json=body,
        )
        if update_response.status_code == 404:
            insert_response = self._request("POST", self._events_url(), json=body)
            if insert_response.status_code == 409:
                update_response = self._request(
                    "PUT",
                    self._event_url(clean_event_id),
                    json=body,
                )
                update_response.raise_for_status()
                return _result_from_response(clean_event_id, "updated", update_response)
            insert_response.raise_for_status()
            return _result_from_response(clean_event_id, "created", insert_response)
        update_response.raise_for_status()
        return _result_from_response(clean_event_id, "updated", update_response)

    def list_events(
        self,
        *,
        time_min: datetime,
        time_max: datetime,
        max_results: int = 2500,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "timeMin": _rfc3339(time_min),
            "timeMax": _rfc3339(time_max),
            "singleEvents": "true",
            "showDeleted": "false",
            "orderBy": "startTime",
            "maxResults": max(1, min(int(max_results or 2500), 2500)),
        }
        events: list[dict[str, Any]] = []
        while True:
            response = self._request("GET", self._events_url(), params=params)
            response.raise_for_status()
            payload = _response_json_object(response)
            for item in payload.get("items") or []:
                if isinstance(item, dict):
                    events.append(item)
            page_token = str(payload.get("nextPageToken") or "").strip()
            if not page_token:
                return events
            params["pageToken"] = page_token

    def patch_event_color(
        self,
        *,
        event_id: str,
        color_id: str,
    ) -> GoogleCalendarUpsertResult:
        clean_event_id = str(event_id or "").strip()
        clean_color_id = str(color_id or "").strip()
        if not clean_event_id:
            raise ValueError("Google Calendar event id is required")
        if not clean_color_id:
            raise ValueError("Google Calendar color id is required")
        response = self._request(
            "PATCH",
            self._event_url(clean_event_id),
            json={"colorId": clean_color_id},
        )
        response.raise_for_status()
        return _result_from_response(clean_event_id, "color_updated", response)

    def patch_event_completion_marker(
        self,
        *,
        event_id: str,
        summary: str,
        color_id: str,
    ) -> GoogleCalendarUpsertResult:
        clean_event_id = str(event_id or "").strip()
        clean_summary = str(summary or "").strip()
        clean_color_id = str(color_id or "").strip()
        if not clean_event_id:
            raise ValueError("Google Calendar event id is required")
        if not clean_summary:
            raise ValueError("Google Calendar event summary is required")
        if not clean_color_id:
            raise ValueError("Google Calendar color id is required")
        response = self._request(
            "PATCH",
            self._event_url(clean_event_id),
            json={"summary": clean_summary, "colorId": clean_color_id},
        )
        response.raise_for_status()
        return _result_from_response(clean_event_id, "completion_updated", response)

    def _events_url(self) -> str:
        calendar = quote(self.calendar_id, safe="")
        return f"{self.api_base}/calendars/{calendar}/events"

    def _event_url(self, event_id: str) -> str:
        return f"{self._events_url()}/{quote(event_id, safe='')}"

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        headers = dict(kwargs.pop("headers", {}) or {})
        headers["Authorization"] = f"Bearer {self.access_token}"
        headers.setdefault("Accept", "application/json")
        headers.setdefault("Content-Type", "application/json")
        return self.session.request(method, url, headers=headers, timeout=30, **kwargs)


def refresh_google_access_token(
    *,
    refresh_token: str,
    client_id: str,
    client_secret: str,
    token_uri: str | None = None,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    refresh = str(refresh_token or "").strip()
    cid = str(client_id or "").strip()
    secret = str(client_secret or "").strip()
    if not refresh or not cid or not secret:
        raise ValueError("refresh_token, client_id, and client_secret are required")
    client = session or requests.Session()
    response = client.post(
        str(token_uri or GOOGLE_OAUTH_TOKEN_URI).strip() or GOOGLE_OAUTH_TOKEN_URI,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh,
            "client_id": cid,
            "client_secret": secret,
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Google OAuth token endpoint returned invalid JSON")
    access_token = str(payload.get("access_token") or "").strip()
    if not access_token:
        raise RuntimeError("Google OAuth token endpoint did not return access_token")
    expires_in = int(payload.get("expires_in") or 3600)
    expiry = datetime.now(timezone.utc) + timedelta(seconds=max(expires_in, 60))
    return {
        "access_token": access_token,
        "expiry": expiry.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "token_uri": str(token_uri or GOOGLE_OAUTH_TOKEN_URI).strip()
        or GOOGLE_OAUTH_TOKEN_URI,
        "client_id": cid,
        "client_secret": secret,
        "refresh_token": refresh,
        "scope": str(payload.get("scope") or GOOGLE_CALENDAR_SCOPE),
        "token_type": str(payload.get("token_type") or "Bearer"),
    }


def _rfc3339(value: datetime) -> str:
    parsed = value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    parsed = parsed.astimezone(timezone.utc).replace(microsecond=0)
    return parsed.isoformat().replace("+00:00", "Z")


def _result_from_response(
    event_id: str,
    action: str,
    response: requests.Response,
) -> GoogleCalendarUpsertResult:
    payload = _response_json_object(response)
    return GoogleCalendarUpsertResult(
        event_id=event_id,
        action=action,
        status_code=int(response.status_code),
        html_link=_html_link_from_payload(payload),
    )


def _response_json_object(response: requests.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _html_link_from_payload(payload: dict[str, Any]) -> str | None:
    html_link = payload.get("htmlLink")
    return str(html_link) if html_link else None


def _private_properties(payload: dict[str, Any]) -> dict[str, Any]:
    extended = payload.get("extendedProperties")
    if not isinstance(extended, dict):
        return {}
    private = extended.get("private")
    return dict(private) if isinstance(private, dict) else {}


def _ku_secretary_ownership(payload: dict[str, Any]) -> dict[str, str]:
    private = _private_properties(payload)
    ownership: dict[str, str] = {}
    for key in (
        "ku_secretary_kind",
        "ku_secretary_external_id",
        "ku_secretary_source",
        "ku_secretary_user_id",
    ):
        value = str(private.get(key) or "").strip()
        if not value:
            return {}
        ownership[key] = value
    return ownership


def _attach_payload_hash(payload: dict[str, Any]) -> None:
    extended = payload.setdefault("extendedProperties", {})
    if not isinstance(extended, dict):
        extended = {}
        payload["extendedProperties"] = extended
    private = extended.setdefault("private", {})
    if not isinstance(private, dict):
        private = {}
        extended["private"] = private
    private["ku_secretary_payload_hash"] = _managed_payload_hash_v1(payload)
    private["ku_secretary_payload_hash_v2"] = _managed_payload_hash(payload)


def _manual_edit_guard_reason(
    *,
    existing: dict[str, Any],
    expected: dict[str, Any],
    ownership: dict[str, str],
) -> str | None:
    existing_private = _private_properties(existing)
    for key, value in ownership.items():
        if str(existing_private.get(key) or "").strip() != value:
            return "not_ku_secretary_event"

    stored_hash_v2 = str(existing_private.get("ku_secretary_payload_hash_v2") or "").strip()
    if stored_hash_v2:
        if stored_hash_v2 != _managed_payload_hash(existing):
            stored_hash_v1 = str(existing_private.get("ku_secretary_payload_hash") or "").strip()
            if stored_hash_v1 and stored_hash_v1 == _managed_payload_hash_v1(existing):
                return None
            return "manual_changes_detected"
        return None

    stored_hash = str(existing_private.get("ku_secretary_payload_hash") or "").strip()
    existing_hash = _managed_payload_hash(existing)
    expected_hash = _managed_payload_hash(expected)
    if stored_hash:
        if stored_hash != _managed_payload_hash_v1(existing):
            return "manual_changes_detected"
        if existing_hash != expected_hash:
            return "manual_changes_detected"
        return None
    if existing_hash != expected_hash:
        return "legacy_event_differs"
    return None


def _managed_payload_hash_v1(payload: dict[str, Any]) -> str:
    stable = {
        "summary": str(payload.get("summary") or ""),
        "description": str(payload.get("description") or ""),
        "start": _stable_calendar_value(payload.get("start")),
        "end": _stable_calendar_value(payload.get("end")),
        "location": str(payload.get("location") or ""),
        "reminders": _stable_calendar_value(payload.get("reminders")),
        "source": _stable_calendar_value(payload.get("source")),
    }
    encoded = json.dumps(stable, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return sha1(encoded.encode("utf-8")).hexdigest()


def _managed_payload_hash(payload: dict[str, Any]) -> str:
    stable = {
        "summary": str(payload.get("summary") or ""),
        "description": str(payload.get("description") or ""),
        "start": _stable_calendar_value(payload.get("start")),
        "end": _stable_calendar_value(payload.get("end")),
        "location": str(payload.get("location") or ""),
        "reminders": _stable_calendar_value(payload.get("reminders")),
        "source": _stable_calendar_value(payload.get("source")),
        "transparency": str(payload.get("transparency") or ""),
        "visibility": str(payload.get("visibility") or ""),
        "attendees": _stable_calendar_value(payload.get("attendees")),
        "recurrence": _stable_calendar_value(payload.get("recurrence")),
        "attachments": _stable_calendar_value(payload.get("attachments")),
        "conferenceData": _stable_calendar_value(payload.get("conferenceData")),
        "guestsCanModify": _stable_calendar_value(payload.get("guestsCanModify")),
        "guestsCanInviteOthers": _stable_calendar_value(payload.get("guestsCanInviteOthers")),
        "guestsCanSeeOtherGuests": _stable_calendar_value(payload.get("guestsCanSeeOtherGuests")),
        "anyoneCanAddSelf": _stable_calendar_value(payload.get("anyoneCanAddSelf")),
    }
    encoded = json.dumps(stable, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return sha1(encoded.encode("utf-8")).hexdigest()


def _stable_calendar_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _stable_calendar_value(val)
            for key, val in sorted(value.items(), key=lambda item: str(item[0]))
            if val not in (None, "", [], {})
        }
    if isinstance(value, list):
        return [_stable_calendar_value(item) for item in value]
    return value
