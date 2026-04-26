"""KUPID portal authentication adapter for KU_Secretary.

Wraps the vendored ku-portal-mcp `auth` module so that KU_Secretary callers
can:

- supply credentials from `secret_store` instead of process-wide env vars
- redirect the upstream session cache out of `~/.cache/ku-portal-mcp/` and
  into the KU_Secretary data directory
- run the async login flow from synchronous CLI/job code

The upstream module (`ku_secretary._kupid.auth`) reads `KU_PORTAL_ID` /
`KU_PORTAL_PW` from `os.environ`. To keep the upstream code unmodified we
set those vars temporarily during the login call and restore them on exit.

Concurrency: do **not** drive concurrent KUPID logins through this adapter —
the env-var swap is process-global. Serialize logins at the caller (the
upstream module also keeps a 30-min session cache, so re-logins are rare).
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from ku_secretary._kupid import auth as _kupid_auth
from ku_secretary._kupid.auth import Session
from ku_secretary.secret_store import SecretStore, StoredSecretRef

logger = logging.getLogger(__name__)

KUPID_ID_KEY = "ku_portal_id"
KUPID_PASSWORD_KEY = "ku_portal_password"


@contextmanager
def _temporary_env(user_id: str, password: str) -> Iterator[None]:
    prev_id = os.environ.get("KU_PORTAL_ID")
    prev_pw = os.environ.get("KU_PORTAL_PW")
    try:
        os.environ["KU_PORTAL_ID"] = user_id
        os.environ["KU_PORTAL_PW"] = password
        yield
    finally:
        if prev_id is None:
            os.environ.pop("KU_PORTAL_ID", None)
        else:
            os.environ["KU_PORTAL_ID"] = prev_id
        if prev_pw is None:
            os.environ.pop("KU_PORTAL_PW", None)
        else:
            os.environ["KU_PORTAL_PW"] = prev_pw


def configure_session_cache(cache_dir: str | Path) -> Path:
    """Redirect the vendored auth module's session cache to *cache_dir*.

    Call once at startup with the KU_Secretary data directory so that the
    cached `session.json` lives alongside the rest of the app's local state.
    Returns the resolved cache directory.
    """
    resolved = Path(cache_dir).expanduser().resolve()
    resolved.mkdir(parents=True, exist_ok=True)
    _kupid_auth.CACHE_DIR = resolved
    _kupid_auth.SESSION_FILE = resolved / "session.json"
    return resolved


def login(*, user_id: str, password: str) -> Session:
    """Synchronously perform KUPID SSO login and return a Session.

    Reuses the cached session if it is still valid; the supplied credentials
    are only consumed when a fresh login is required.
    """
    user_id = (user_id or "").strip()
    if not user_id or not password:
        raise ValueError("user_id and password are required for KUPID login")
    with _temporary_env(user_id, password):
        return asyncio.run(_kupid_auth.login())


def login_with_secret_store(
    *,
    store: SecretStore,
    id_ref: StoredSecretRef,
    password_ref: StoredSecretRef,
) -> Session:
    """Read credentials from *store* and run :func:`login`."""
    user_id = store.read_secret(ref=id_ref).strip()
    password = store.read_secret(ref=password_ref)
    return login(user_id=user_id, password=password)


def store_credentials(
    *,
    store: SecretStore,
    user_id: str,
    password: str,
) -> tuple[StoredSecretRef, StoredSecretRef]:
    """Persist KUPID credentials in *store* under canonical keys.

    Returns the (id_ref, password_ref) pair the caller should keep alongside
    the rest of its config.
    """
    id_ref = store.store_secret(key=KUPID_ID_KEY, secret=user_id.strip())
    password_ref = store.store_secret(key=KUPID_PASSWORD_KEY, secret=password)
    return id_ref, password_ref


def clear_session() -> None:
    """Remove the cached KUPID session file (forces re-login on next call)."""
    _kupid_auth.clear_session()


def session_cache_path() -> Path:
    """Current resolved location of the KUPID session cache file."""
    return Path(_kupid_auth.SESSION_FILE)
