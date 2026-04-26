# KU_Secretary — Codex 인계 문서

**작성**: 2026-04-26 KST
**컨텍스트**: Claude Code Ralph 세션에서 진행하다 cancel. 남은 검증 2건을 codex에서 마무리.

**Codex 완료 메모**: 2026-04-26 KST, 남은 p3 lock 회귀와 critic 사인오프 완료. 최종 검증: `TMPDIR=/tmp ./.venv/bin/python -m pytest -q tests/` → 477 passed, 1 skipped. Critic verdict: APPROVED.

---

## 1. 한 줄 요약

KU_Secretary Telegram 봇의 모든 슬래시 명령어를 실 사용자처럼 e2e 테스트 + 시립대 회귀 픽스 + LMS 보드/공지 신규 명령(`/board`) 추가 완료. **p3 pytest 청크 단독 회귀 1건과 critic 에이전트 사인오프 1건만 남음.**

## 2. 현재 코드/데이터 상태

### 수정된 파일 (이번 세션)

| 분류 | 파일 | 변경 요지 |
|---|---|---|
| 신규 | `src/ku_secretary/connectors/ku_notices.py` | KU 일반/학사/장학 공지 스크레이퍼 (`korea.ac.kr/ko/<board_id>/subview.do`). `fetch_ku_notice_feed(board_id, limit)` |
| 신규 | `tests/test_ku_notices_connector.py` | KU 공지 어댑터 단위 테스트 6개 |
| 신규 | `scripts/e2e_telegram_smoke.py` | 33개 명령(별칭 포함)을 실 dispatch 경로로 흘리는 e2e 하니스 |
| 신규 | `scripts/cross_check_live.py` | 라이브 KU 페이지/HODI vs 봇 출력 비교 |
| 수정 | `src/ku_secretary/connectors/telegram.py` | `/board` `/announcements` `/공지` 별칭 파싱 추가 |
| 수정 | `src/ku_secretary/jobs/pipeline.py` | (1) `fetch_uos_notice_feed`→`fetch_ku_notice_feed` 교체 (2) `UOS_NOTICE_FEEDS`에 `board_id` 추가 (3) `_format_telegram_lms_board()` 신규 (4) `_execute_telegram_command`에 `lms_board` 분기 (5) 메뉴/help 갱신 (6) `_register_uclass_courses`가 `db.find_course_by_external_id`로 기존 캐노니컬 우선 매칭 |
| 수정 | `src/ku_secretary/db.py` | (1) `upsert_course`에 두번째 `ON CONFLICT(user_id, source, external_course_id)` 절 추가 (2) `find_course_by_external_id` 메서드 신규 |
| 수정 | `tests/test_telegram_library_command.py` | `/assignments` 6개 + `/board` 5개 신규 단위 테스트 |
| 수정 | `tests/test_p3_telegram_commands.py` | monkeypatch 대상 `fetch_uos_notice_feed`→`fetch_ku_notice_feed`, lambda 시그니처 `board_id, *, limit`로 변경, source_url 어설션 KU 형태로 |
| 수정 | `tests/test_p4_uclass_sync_stages.py` | 캐노니컬 어설션 `uclass:uclass-uos-ac-kr:3821`→`uclass:kulms-korea-ac-kr:3821` (현실의 ws_base_url 반영) |
| 수정 | `tests/test_beta_critical_path.py` | 동일 monkeypatch 대상/시그니처 갱신 |

### 신규 명령 요약

- `/library [도서관명]`, 별칭 `/lib`, `/seats` — 6개 도서관 좌석(HODI public API)
- `/assignments`, 별칭 `/due`, `/homework`, `/과제` — Canvas LMS todo + upcoming events
- `/board`, 별칭 `/announcements`, `/lms_board`, `/lmsboard`, `/공지` — Canvas 각 강의 announcements + boards 모음

### 봇 메뉴 명령 수 = 20 (assistant 비활성 기준)

`_telegram_bot_menu_commands` 결과에 `library`, `assignments`, `board` 모두 포함.

## 3. 검증 완료 항목

| ID | 내용 | 결과 |
|---|---|---|
| US-001 | 시립대(uos.ac.kr) 회귀 제거 | ✅ `cross_check_live.py` exit 0; `/notice_general` 3/3 라이브 KU 제목 일치, 0 forbidden token |
| US-002 | 봇 메뉴/help에 신규 명령 노출 | ✅ count=20, library/assignments/board 모두 노출 |
| US-003 | `/board` 라이브 동작 | ✅ "[KU] 과목별 게시판/공지" + 빅데이터응용보안 4개 공지(Term-Project 등) 실 데이터 출력 |
| US-004 | E2e 스모크 | ✅ ok=26 / err=7(의도된 잘못된 입력) / parse_miss=0 |
| US-005 | 라이브 비교 검증 | ✅ general 3/3, academic 3/3, library 4816/4860 동일 |
| US-007 | 잘못된 입력 케이스 | ✅ /done /apply /plan /bot /unknowncmd 모두 적절한 ERR 메시지 |

### pytest 청크 결과(완료된 것만)

| 청크 | 명령 | 결과 |
|---|---|---|
| chunk1 | `tests/test_telegram_library_command.py tests/test_ku_notices_connector.py tests/test_telegram_parser.py tests/test_portal_notices.py tests/test_p0_*.py tests/test_p1_*.py tests/test_p2_*.py` | **81 passed** |
| chunk3 | `tests/test_assistant_*.py tests/test_cli_output_shapes.py tests/test_dashboard_publish.py tests/test_day_*.py tests/test_db_*.py tests/test_ku_*.py tests/test_llm_parser.py tests/test_local_llm_provider.py tests/test_material_*.py tests/test_notification_policies.py tests/test_onboarding_service.py tests/test_ops_dashboard.py` | **115 passed, 1 skipped** |
| chunk4 | `tests/test_p4_*.py tests/test_beta_critical_path.py` | **59 passed** |
| 부분 | `tests/test_p3_briefings_and_plan.py`(단독) | 11 passed |
| 부분 | `tests/test_p3_telegram_commands.py + test_beta_critical_path.py + test_portal_notices.py + test_telegram_library_command.py + test_ku_notices_connector.py`(notice 회귀 검증) | 101 passed |

**합계 (중복 제외 추정)**: 약 350+ tests passed.

## 4. ❗남은 작업 2건

### Task A — `tests/test_p3_sync_all_lock.py` 단일 타이밍 실패 조사 (1건)

- **결과**: p3 청크 단독 실행 완료. **191 passed, 1 failed**.
- **유일 실패 케이스**: `tests/test_p3_sync_all_lock.py::test_sync_all_wait_timeout_returns_lock_timeout`
  ```
  assert result.exit_code == 4
  AssertionError: assert 1 == 4
   +  where 1 = <Result AssertionError('run_all_jobs should not run before lock timeout')>.exit_code
  ```
- **분석**:
  - 헬퍼 `_hold_sync_lock_for_duration(lock_path, ready, hold_seconds=0.4)`는 spawn 프로세스가 lock을 0.4s만 잡고 풀어버림.
  - 테스트는 `proc.start()` → `ready.wait(10)` → `monkeypatch.setattr(...)` → `runner.invoke(...)` 사이의 시간이 0.4s 미만이라고 가정.
  - 다회 재현됨(첫 실행에서도, 단독 재실행에서도 동일 1초 이내 실패) — 타이밍 의존성이지만 결정적으로 늘 실패하는 환경(WSL, 메모리 압박, spawn 컨텍스트가 무거움) 으로 보임.
  - **이번 세션 코드 변경과 무관**: `cli._sync_all_lock_path`, `run_all_jobs`, sync-lock 로직 어떤 것도 건드리지 않았음.
  - 같은 `_hold_sync_lock_for_duration`을 쓰는 다른 테스트도 같은 파일에 있음(`test_sync_all_exits_fast_when_lock_is_held`는 `_hold_sync_lock`이라는 release 이벤트 기반 헬퍼를 쓰므로 통과). 이 테스트만 시간 기반 hold를 사용해 fragile.
- **권장 조치 (codex가 판단)**:
  1. **수정안 A (간단)**: 헬퍼 시그니처를 release-event 기반으로 변경하거나, hold_seconds를 5s 등 충분히 길게 늘리고 테스트 종료 시 `proc.terminate()`로 정리.
  2. **수정안 B**: WSL/spawn 환경 명시 skip (하지만 회귀 감지력이 떨어짐).
  3. **수정안 C**: `result.exit_code == 4 OR (1 with assertion match)` 두 케이스 모두 허용 — 비추(테스트 의미 약화).
- **합격 기준**: 단독 실행 시 1번 통과면 충분 (또는 의도적 skip 처리).
- **다른 청크 결과**:
  | 청크 | 결과 |
  |---|---|
  | p3 (단독 실행) | 191 passed, 1 failed (위 1건만) |
  | chunk1 (p0/p1/p2 + 신규 어댑터/명령) | 81 passed |
  | chunk3 (assistant + db + ku_* + materials) | 115 passed, 1 skipped |
  | chunk4 (p4 + beta) | 59 passed |

### Task B — Critic 에이전트 사인오프

- **이유**: PRD US-008 미충족. 아직 독립 검토 없음.
- **방법**: codex의 critic 워크플로(또는 `omc ask codex --agent-prompt critic`)에 아래 컨텍스트 전달:
  - `.omc/prd.json` 의 acceptance criteria 전체
  - 변경 파일 리스트 (위 표 §2)
  - 평가 요청: (1) 각 acceptance criterion 통과 여부, (2) 더 단순/빠른/유지보수성 좋은 대안 존재 여부, (3) 변경 파일과 그 호출자/피호출자/공유 타입까지 폭 넓게 검토.
- **합격 기준**: critic verdict = APPROVED + 각 criterion에 대해 'pass' 명시.

## 5. 알려진 환경/제약

- **WSL 메모리 압박**: 다른 Claude/Java 프로세스 동시 실행 시 pytest OOM 빈발. 단일 프로세스로 분할 필요.
- **자격증명**: `.env`에 `KU_PORTAL_ID`, `KU_PORTAL_PW`, `TELEGRAM_BOT_TOKEN`, `KU_TELEGRAM_ALLOWED_CHAT_IDS=<telegram-chat-id>`. OTP는 비활성 상태여야 Canvas SSO 동작.
- **DB**: `data/ku.db` (허용된 Telegram chat_id 등록됨, 12개 일정 동기화돼있음).
- **opt-in 플래그**: `KUPID_SSO_TIMETABLE_ENABLED` 토글 시 KUPID SSO timetable 어댑터 동작.
- **회귀 가능 영역**:
  - LMS 보드/공지: Canvas API 응답 형태가 강의별로 다를 수 있어 키 누락 시 그 강의만 빈 결과. 현재 캡: 8 강의 × 3 보드 × 3 게시물.
  - `_register_uclass_courses`: 새 호출자 추가 시 `db.find_course_by_external_id` 우선 매칭 패턴 유지 필요.

## 6. 핵심 검증 명령 모음

```bash
# E2e 스모크 (실 dispatch + 실 데이터)
.venv/bin/python scripts/e2e_telegram_smoke.py

# 라이브 비교 검증
.venv/bin/python scripts/cross_check_live.py

# Notice 회귀 영향 한 번에
.venv/bin/python -m pytest tests/test_p3_telegram_commands.py tests/test_beta_critical_path.py tests/test_portal_notices.py tests/test_telegram_library_command.py tests/test_ku_notices_connector.py -q

# 시립대 잔재 0회 확인
.venv/bin/python scripts/e2e_telegram_smoke.py 2>&1 | grep -ciE "uos\.ac\.kr|wise\.uos|시립대|UOS공지"
# → 0 이어야 함
```

## 7. 결과물 (PRD/Progress)

- `.omc/prd.json` — 8개 user story, US-001~US-005, US-007은 `passes: true`로 마킹. US-006, US-008은 `passes: false` (codex가 마무리).
- `.omc/progress.txt` — 각 US별 evidence 요약.

## 8. 인계 한 마디

코드 변경분은 안정적입니다 — 라이브 검증 + pytest 청크 합산 약 446 통과(중복 제외 추정 350+). 남은 건 (1) **p3의 타이밍 의존 단일 fail(`test_sync_all_wait_timeout_returns_lock_timeout`)**을 헬퍼 시그니처 변경으로 안정화, (2) **critic 사인오프** 받기. 둘 다 절차 작업이며 코어 로직 추가 변경은 불필요.
