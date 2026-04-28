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


def _result_from_response(
    event_id: str,
    action: str,
    response: requests.Response,
) -> GoogleCalendarUpsertResult:
    try:
        payload = response.json()
    except Exception:
        payload = {}
    html_link = payload.get("htmlLink") if isinstance(payload, dict) else None
    return GoogleCalendarUpsertResult(
        event_id=event_id,
        action=action,
        status_code=int(response.status_code),
        html_link=str(html_link) if html_link else None,
    )
