# AGENT_MEMORY.md

Persistent user rules and lessons for KU Secretary agents.

## User Commands

- When the user says "최신화 ㄱㄱ", refresh LMS-derived state and Google Calendar with `./.venv/bin/kus sync-google-calendar --wait --timeout-seconds 600` unless the user gives a narrower target.
- If the local Google Calendar OAuth token is expired/revoked, use the Codex app Google Calendar plugin for direct Calendar reads/updates instead of blocking on the repo token file. Keep the same calendar safety rules.
- As of 2026-05-11, the repo-local Google Calendar token was intentionally disabled (`google_calendar_token.json.disabled-*`) because Google issued a 7-day testing refresh token. Prefer an external/Codex MCP with working Calendar write auth for future calendar operations unless the user explicitly asks to restore local OAuth.
- After a refresh, confirm the sync result and call out skipped calendar events if user-owned or manually changed events were protected.
- When asked whether the Telegram bot is running, check both processes and health state; the long-running command is `./.venv/bin/kus telegram-listener`.

## Calendar Safety

- Never modify or delete Google Calendar events created by the user.
- Never overwrite schedule fields of a KU Secretary-created Google Calendar event if the user manually changed its date, time, or all-day state.
- The "do not touch modified KU Secretary events" rule is about schedule/content updates; color-only patches are allowed for completed/incomplete distinction when the event is classified as academic or explicitly requested by the user.
- Apply completed/incomplete colors to exams, assignments, quizzes, presentations, and user-added activities explicitly named by the user such as `기해실` and `Cykor`.
- If an academic or explicitly tracked calendar event is past its actual Google Calendar end time, mark it completed even when the user created or manually rescheduled it. This exception may update only completion presentation: gray color and `[완료]` in the title. Do not change date, time, all-day state, location, or other user-edited content.

## Completion Semantics

- Past deadlines/events may be displayed as completed, but do not mark a future or still-open LMS assignment as done just because another Canvas flag is ambiguous.
- Canvas/LMS submission states `unsubmitted`, `missing`, `pending_upload`, and `created` mean the assignment is open.
- If official LMS data says an assignment is open, sync is allowed to reopen a previously `done` task. This prevents unsubmitted assignments from staying falsely completed.
