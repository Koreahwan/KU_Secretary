"""End-to-end Telegram command smoke runner.

Exercises every read-only /command via the real dispatch path:
  parse_command_message  →  _execute_telegram_command  →  message text

It loads the real settings (.env) and the real SQLite DB (data/ku.db),
so it shows what a Telegram user would see right now.

Mutating commands (/apply, /done, /plan, /bot) are skipped on purpose.
"""

from __future__ import annotations

import os
import sys
import textwrap
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

from ku_secretary.config import load_settings  # noqa: E402
from ku_secretary.connectors import telegram as tg  # noqa: E402
from ku_secretary.db import Database  # noqa: E402
from ku_secretary.jobs import pipeline  # noqa: E402

# read-only / safe commands; mutating ones intentionally omitted
COMMANDS = [
    "/start",
    "/help",
    "/setup",
    "/status",
    "/today",
    "/tomorrow",
    "/weather",
    "/region",
    "/region 서울",
    "/todaysummary",
    "/tomorrowsummary",
    "/notice_general",
    "/notice_academic",
    "/notice_uclass",
    "/library",
    "/library 중앙도서관",
    "/lib 과학도서관",
    "/seats",
    "/assignments",
    "/due",
    "/homework",
    "/과제",
    "/board",
    "/announcements",
    "/공지",
    "/inbox",
    "/connect",
    # malformed / unsupported
    "/done",
    "/done task",
    "/apply",
    "/plan",
    "/bot",
    "/unknowncmd",
]


def banner(label: str) -> None:
    line = "=" * 70
    print(f"\n{line}\n{label}\n{line}")


def short(text: str, limit: int = 1200) -> str:
    text = str(text or "")
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n… [truncated {len(text) - limit} chars]"


def main() -> int:
    settings = load_settings()
    chat_id = (
        os.environ.get("KU_E2E_CHAT_ID", "").strip()
        or next(iter(getattr(settings, "telegram_allowed_chat_ids", []) or []), "")
        or "0"
    )
    db_path = Path(str(settings.database_path))
    if not db_path.exists():
        print(f"WARN: db not found at {db_path}; commands needing DB will misbehave")
    db = Database(settings.database_path)
    db.init()

    pass_count = 0
    fail_count = 0
    parse_misses = 0

    for raw in COMMANDS:
        banner(f"INPUT: {raw}")
        try:
            payload = tg.parse_command_message(raw)
        except Exception:
            parse_misses += 1
            print("PARSE EXCEPTION:\n" + traceback.format_exc())
            continue

        print(f"PARSED: {payload}")
        if payload is None:
            parse_misses += 1
            print("PARSE: None (not recognised as command)")
            continue

        try:
            result = pipeline._execute_telegram_command(
                settings=settings,
                db=db,
                command_payload=payload,
                chat_id=chat_id,
                user_id=None,
            )
        except SystemExit as exc:
            fail_count += 1
            print(f"DISPATCH SystemExit: {exc}")
            continue
        except Exception:
            fail_count += 1
            print("DISPATCH EXCEPTION:\n" + traceback.format_exc())
            continue

        ok = bool(result.get("ok"))
        msg = result.get("message")
        err = result.get("error")
        status = "OK" if ok else "ERR"
        if ok:
            pass_count += 1
        else:
            fail_count += 1
        print(f"STATUS: {status}")
        if err:
            print(f"ERROR: {err}")
        if msg:
            print("MESSAGE:")
            print(textwrap.indent(short(msg), "  "))

    banner(
        f"SUMMARY  ok={pass_count}  err={fail_count}  parse_miss={parse_misses}  "
        f"total={len(COMMANDS)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
