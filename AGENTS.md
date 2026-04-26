# AGENTS.md

Keep this file short. Repository-level agent instructions help only when they add non-obvious, repo-specific constraints; detailed procedures belong in `README.md`.

## Scope

- This repository runs two real instances:
  - prod: `/path/to/apps/KU_secretary` with empty `INSTANCE_NAME`
  - beta: `/path/to/apps/KU_secretary_beta` with `INSTANCE_NAME = "beta"`
- Do not assume prod and beta share config, DBs, bot tokens, or public onboarding URLs.

## Config and State

- Use `./.venv/bin/python`; system Python may miss required packages.
- Config is loaded from `config.toml` if present, plus the sibling `.env`. If `config.toml` is missing, `.env` can still be the active source of truth.
- Before changing runtime behavior, verify which config source the target instance actually uses.
- Do not edit or overwrite `.env`, `credentials/`, or `data/` unless the user explicitly asks.

## Validation

- Default test command: `./.venv/bin/python -m pytest -q`
- New KU connector tests (no live network unless `LIBRARY_LIVE_TEST=1`):
  `./.venv/bin/python -m pytest -q tests/test_ku_*_connector.py`
- Live KUPID/Canvas smoke (requires `KU_PORTAL_ID`/`KU_PORTAL_PW` and OTP off):
  - Library: `LIBRARY_LIVE_TEST=1 pytest tests/test_ku_library_connector.py -k live -s`
  - Manual auth probe: `python -c "from ku_secretary.connectors.ku_portal_auth import login; print(login(user_id='…', password='…').is_valid)"`
- Runtime readiness: `./.venv/bin/python -m ku_secretary.cli doctor --config-file <config>`
- UClass/Canvas sanity check: `./.venv/bin/python -m ku_secretary.cli uclass probe --config-file <config>`
- For `/connect` failures, compare:
  - local onboarding: `http://127.0.0.1:8791/...`
  - public onboarding: `ONBOARDING_PUBLIC_BASE_URL/...`
  If local works and public fails, the problem is usually proxy/Funnel routing, not the bot or DB.

## Deployment

- `main` is the canonical local development/integration branch.
- `<deploy-remote>/beta` and `<deploy-remote>/deploy` are deployment refs on the remote bare repo, not normal long-lived development branches.
- Temporary local branches are optional. After testing, fast-forward or merge the intended commit into `main`, then deploy the tested `HEAD`.
- Beta deploy: `git push <deploy-remote> HEAD:beta`
- Prod promotion: `git push <deploy-remote> HEAD:deploy`
- Prod launchd labels are unsuffixed; beta labels end with `.beta`.
- After changing onboarding config, restart the matching `telegram-listener` and `onboarding` jobs.
- Never point beta and prod at the same public onboarding route unless they intentionally share the same DB and server process.

## Product Boundaries

- Current user-facing support is Korea University only.
- Generic Moodle onboarding exists for other schools, but portal/timetable automation is implemented only for KU.
- KU LMS is **Canvas** (mylms.korea.ac.kr), not Blackboard. Auth flows assume KSSO SAML SSO + RSA-decrypted Canvas password handoff (see `src/ku_secretary/_kupid/lms.py`).
- KSSO accounts with OTP enabled cannot complete Canvas SSO — surface this clearly to users instead of retry-looping.
- Do not widen supported-school messaging or defaults without an explicit product decision.

## Vendored ku-portal-mcp (v0.10.1, MIT)

- Source: https://github.com/SonAIengine/ku-portal-mcp (SonAIengine).
- Vendored under `src/ku_secretary/_kupid/`. Do not edit these files in place; preserve upstream as a clean drop. Modify behavior through adapters in `src/ku_secretary/connectors/ku_*.py`.
- `_kupid/LICENSE` must remain alongside the vendored modules.
- Server entry points (`server.py`, `__main__.py`) are intentionally omitted to avoid pulling in `mcp[cli]` / FastMCP runtime.
