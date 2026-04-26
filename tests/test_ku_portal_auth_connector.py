"""Unit tests for the KUPID auth adapter.

No live KUPID hits — the upstream `auth.login` coroutine is monkeypatched.
"""

from __future__ import annotations

import os
import time

import pytest

from ku_secretary._kupid import auth as kupid_auth
from ku_secretary.connectors import ku_portal_auth
from ku_secretary.secret_store import FileSecretStore


def _make_session() -> kupid_auth.Session:
    return kupid_auth.Session(
        ssotoken="t-test",
        portal_session_id="psid",
        grw_session_id="grw",
        created_at=time.time(),
    )


def test_login_sets_env_during_call_and_restores(monkeypatch):
    seen: dict[str, str] = {}

    async def fake_login():
        seen["KU_PORTAL_ID"] = os.environ.get("KU_PORTAL_ID", "")
        seen["KU_PORTAL_PW"] = os.environ.get("KU_PORTAL_PW", "")
        return _make_session()

    monkeypatch.delenv("KU_PORTAL_ID", raising=False)
    monkeypatch.delenv("KU_PORTAL_PW", raising=False)
    monkeypatch.setattr(kupid_auth, "login", fake_login)

    session = ku_portal_auth.login(user_id="2024000000", password="pw")
    assert session.ssotoken == "t-test"
    assert seen == {"KU_PORTAL_ID": "2024000000", "KU_PORTAL_PW": "pw"}
    assert "KU_PORTAL_ID" not in os.environ
    assert "KU_PORTAL_PW" not in os.environ


def test_login_restores_prior_env(monkeypatch):
    async def fake_login():
        return _make_session()

    monkeypatch.setenv("KU_PORTAL_ID", "prior-id")
    monkeypatch.setenv("KU_PORTAL_PW", "prior-pw")
    monkeypatch.setattr(kupid_auth, "login", fake_login)

    ku_portal_auth.login(user_id="new-id", password="new-pw")

    assert os.environ["KU_PORTAL_ID"] == "prior-id"
    assert os.environ["KU_PORTAL_PW"] == "prior-pw"


def test_login_rejects_empty_credentials():
    with pytest.raises(ValueError):
        ku_portal_auth.login(user_id="", password="x")
    with pytest.raises(ValueError):
        ku_portal_auth.login(user_id="x", password="")


def test_configure_session_cache_redirects_module_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(kupid_auth, "CACHE_DIR", kupid_auth.CACHE_DIR)
    monkeypatch.setattr(kupid_auth, "SESSION_FILE", kupid_auth.SESSION_FILE)

    target = tmp_path / "data" / "kupid_cache"
    resolved = ku_portal_auth.configure_session_cache(target)

    assert resolved == target.resolve()
    assert kupid_auth.CACHE_DIR == target.resolve()
    assert kupid_auth.SESSION_FILE == target.resolve() / "session.json"
    assert ku_portal_auth.session_cache_path() == target.resolve() / "session.json"


def test_login_with_secret_store_round_trip(tmp_path, monkeypatch):
    store = FileSecretStore(tmp_path / "secrets")
    id_ref, pw_ref = ku_portal_auth.store_credentials(
        store=store, user_id="2024000001", password="hunter2"
    )

    captured: dict[str, str] = {}

    async def fake_login():
        captured["id"] = os.environ.get("KU_PORTAL_ID", "")
        captured["pw"] = os.environ.get("KU_PORTAL_PW", "")
        return _make_session()

    monkeypatch.setattr(kupid_auth, "login", fake_login)

    session = ku_portal_auth.login_with_secret_store(
        store=store, id_ref=id_ref, password_ref=pw_ref
    )
    assert session.ssotoken == "t-test"
    assert captured == {"id": "2024000001", "pw": "hunter2"}


def test_clear_session_calls_upstream(monkeypatch):
    called = {"n": 0}

    def fake_clear():
        called["n"] += 1

    monkeypatch.setattr(kupid_auth, "clear_session", fake_clear)
    ku_portal_auth.clear_session()
    assert called["n"] == 1
