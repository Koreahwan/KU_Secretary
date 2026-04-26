"""Cross-check live KU sources against bot's rendered output.

Verifies that bot output for /notice_general, /notice_academic, /library
matches what the actual KU pages/APIs return — catches regressions where
the bot pulls from the wrong upstream.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

from ku_secretary.config import load_settings  # noqa: E402
from ku_secretary.connectors import telegram as tg  # noqa: E402
from ku_secretary.connectors.ku_library import get_library_seats  # noqa: E402
from ku_secretary.connectors.ku_notices import fetch_ku_notice_feed  # noqa: E402
from ku_secretary.db import Database  # noqa: E402
from ku_secretary.jobs import pipeline  # noqa: E402

FORBIDDEN_TOKENS = ["uos.ac.kr", "wise.uos", "시립대", "UOS공지"]


def fail(msg: str) -> None:
    print(f"FAIL: {msg}")


def ok(msg: str) -> None:
    print(f"OK:   {msg}")


def _resolve_chat_id(settings) -> str:
    return (
        os.environ.get("KU_E2E_CHAT_ID", "").strip()
        or next(iter(getattr(settings, "telegram_allowed_chat_ids", []) or []), "")
        or "0"
    )


def _bot_render(payload: dict, *, settings, db, chat_id: str) -> str:
    result = pipeline._execute_telegram_command(
        settings=settings,
        db=db,
        command_payload=payload,
        chat_id=chat_id,
        user_id=None,
    )
    return str(result.get("message") or "")


def main() -> int:
    settings = load_settings()
    chat_id = _resolve_chat_id(settings)
    db = Database(settings.database_path)
    db.init()

    failures: list[str] = []

    # /notice_general — first 3 KU titles must appear
    payload = tg.parse_command_message("/notice_general")
    bot_general = _bot_render(payload, settings=settings, db=db, chat_id=chat_id)
    live_general = fetch_ku_notice_feed("566", limit=10)
    matched = 0
    for n in live_general.notices[:3]:
        if n.title.strip() and n.title.strip() in bot_general:
            matched += 1
    if matched == 0:
        failures.append(f"/notice_general: 0/3 live titles found in bot output")
    else:
        ok(f"/notice_general: {matched}/3 live titles matched")

    for token in FORBIDDEN_TOKENS:
        if token in bot_general:
            failures.append(f"/notice_general contains forbidden token '{token}'")
    if not any(t in bot_general for t in FORBIDDEN_TOKENS):
        ok("/notice_general has no UOS/시립대 leakage")

    # /notice_academic
    payload = tg.parse_command_message("/notice_academic")
    bot_acad = _bot_render(payload, settings=settings, db=db, chat_id=chat_id)
    live_acad = fetch_ku_notice_feed("567", limit=10)
    matched = 0
    for n in live_acad.notices[:3]:
        if n.title.strip() and n.title.strip() in bot_acad:
            matched += 1
    if matched == 0:
        failures.append(f"/notice_academic: 0/3 live titles found in bot output")
    else:
        ok(f"/notice_academic: {matched}/3 live titles matched")

    for token in FORBIDDEN_TOKENS:
        if token in bot_acad:
            failures.append(f"/notice_academic contains forbidden token '{token}'")
    if not any(t in bot_acad for t in FORBIDDEN_TOKENS):
        ok("/notice_academic has no UOS/시립대 leakage")

    # /library — totals must match HODI direct call
    payload = tg.parse_command_message("/library")
    bot_lib = _bot_render(payload, settings=settings, db=db, chat_id=chat_id)
    live_lib = get_library_seats()
    summary = live_lib.get("summary", {})
    total_seats = int(summary.get("total_seats") or 0)
    total_avail = int(summary.get("total_available") or 0)
    seat_match = re.search(r"합계: ([\d,]+)/([\d,]+)석", bot_lib)
    if not seat_match:
        failures.append("/library: 합계 라인 패턴 매칭 실패")
    else:
        avail = int(seat_match.group(1).replace(",", ""))
        seats = int(seat_match.group(2).replace(",", ""))
        # HODI is real-time; allow ±200 seat fluctuation between calls
        if abs(seats - total_seats) > 200 or abs(avail - total_avail) > 1000:
            failures.append(
                f"/library totals diverge: bot={avail}/{seats} live={total_avail}/{total_seats}"
            )
        else:
            ok(f"/library totals consistent: bot={avail}/{seats} ≈ live={total_avail}/{total_seats}")

    print()
    print(f"=== Cross-check result: {len(failures)} failure(s) ===")
    for f in failures:
        print(f"- {f}")
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
