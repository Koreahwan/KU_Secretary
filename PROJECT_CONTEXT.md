# KU Secretary 포팅 진행 상황

UOS_Secretary(서울시립대 학생용 Telegram 비서)를 고려대학교 버전으로 포팅하는 작업입니다.

- 원본: https://github.com/Rark-Jeapah/UOS_Secretary
- 작업 시작: 2026-04-26
- 현재 상태: **ku-portal-mcp 통합으로 KUPID/Canvas 커넥터 신설 완료. 기존 시립대 코드 의존자 마이그레이션 진행 중.**

## 폴더 구조

```
KU_Secretary/
├── _reference/                      # 원본 UOS_Secretary 클론 (수정 금지, 비교용)
├── src/ku_secretary/
│   ├── _kupid/                      # vendored ku-portal-mcp v0.10.1 (MIT, attribution 보존)
│   ├── connectors/
│   │   ├── ku_library.py            # 도서관 좌석 (인증 불필요)
│   │   ├── ku_portal_auth.py        # KUPID SSO 어댑터 + secret_store 연동
│   │   ├── ku_timetable.py          # 개인 시간표 + ICS export
│   │   ├── ku_courses.py            # 수강내역, 개설과목, 강의계획서, 학과 목록
│   │   ├── ku_lms.py                # Canvas LMS + LearningX 게시판
│   │   ├── ku_portal.py / ku_openapi.py / uclass.py    # 시립대 잔재 (Phase 4.5에서 마이그레이션)
│   │   └── ...
│   └── ...
├── tests/
│   ├── test_ku_library_connector.py
│   ├── test_ku_portal_auth_connector.py
│   ├── test_ku_timetable_connector.py
│   ├── test_ku_courses_connector.py
│   ├── test_ku_lms_connector.py
│   └── ...
├── docs/
├── deploy/
├── README.md
├── AGENTS.md
├── .env.example                     # KU_PORTAL_ID/PW 추가, UCLASS_* fallback로 강등
├── config.example.toml
├── pyproject.toml                   # httpx, beautifulsoup4, lxml, cryptography 추가
└── PROJECT_CONTEXT.md               # 이 파일
```

## 결정 응답 (2026-04-26)

| 항목 | 선택 | 비고 |
|---|---|---|
| 1. 실행 OS | (b) WSL/Linux | systemd user unit 또는 cron으로 launchd 대체 (Phase 6.5) |
| 2. 첫 우선순위 | (d) 단계적 전체 | 도서관 → 인증 → 시간표/수강내역 → LMS 순으로 단계 검증 |
| 3. LMS 소스 | Canvas REST API (mylms.korea.ac.kr) | KSSO SAML SSO + RSA 복호화. ku-portal-mcp 검증된 흐름 활용 |
| 4. 시간표 소스 | KUPID 스크래핑 | ku-portal-mcp의 timetable 모듈. KU OpenAPI 미사용 |

추가 정책:
- 자격증명 저장: KU_Secretary `secret_store` 단일화 (env var는 호출 직전 임시 주입 후 복원)
- 폴링 간격: 보수적으로 60분 이상 (학교 abuse detection 회피)
- OTP: 미지원 (ku-portal-mcp의 KSSO 제약). OTP 사용자는 ku_library 등 인증 불필요 기능만 가능

## 진행 상황 (Phase 단위)

| Phase | 내용 | 상태 |
|---|---|---|
| 1 | ku-portal-mcp 소스 vendoring + 의존성 통합 | ✅ 완료 |
| 2 | 도서관 좌석 connector (auth-free 검증) | ✅ 완료, live test 통과 |
| 3 | KUPID 인증 어댑터 + secret_store 연동 | ✅ 완료 |
| 4 | 시간표/수강내역 connector 신설 | ✅ 완료 |
| 4.5 | 기존 의존자(jobs/pipeline.py 등) 마이그레이션 | ⏳ 미진행 |
| 5 | LMS connector (Moodle → Canvas) | ✅ 완료 |
| 6 | 문서/환경 통합 (.env.example, README, PROJECT_CONTEXT) | 🔄 진행 중 |
| 6.5 | launchd → systemd user unit CLI 교체 | ⏳ 미진행 |
| 최종 | 환각·오류·회귀 전수 검증 | 🔄 예정 |

## 변환 완료 (이전 단계)

표면 명칭과 식별자는 모두 KU로 통일됨.

| 항목 | 원본 (UOS) | 변환 (KU) |
|---|---|---|
| 패키지 | `sidae_secretary` | `ku_secretary` |
| CLI 명령어 | `sidae` | `kus` |
| launchd label | `com.sidae.*` | `com.ku.*` |
| SQLite | `sidae.db` | `ku.db` |
| LMS 도메인 | `uclass.uos.ac.kr` (Moodle) | **`mylms.korea.ac.kr` (Canvas)** ← 정정됨 |
| 시간표 도메인 | `wise.uos.ac.kr` | `portal.korea.ac.kr` |

검증: `grep -rEn "sidae|Sidae|시립대"`로 잔재 0건.

## 새 connector 사용법 (요약)

```python
from ku_secretary.connectors import (
    ku_portal_auth, ku_library, ku_timetable, ku_courses, ku_lms,
)

# 인증 불필요
seats = ku_library.get_library_seats("중앙도서관")

# KUPID 자격증명으로 로그인 (한 번 후 30분 캐시 재사용)
session = ku_portal_auth.login(user_id="2024000000", password="...")
entries = ku_timetable.get_full_timetable(session)
ics = ku_timetable.export_ics(entries)
my, total_credits = ku_courses.get_my_courses(session)

# Canvas LMS (별도 KSSO SAML 흐름, 25분 캐시)
lms_session = ku_lms.login(user_id="2024000000", password="...")
courses = ku_lms.get_courses(lms_session)
todos = ku_lms.get_todo(lms_session)
```

## 남은 작업

### Phase 4.5 — 기존 의존자 마이그레이션
- `jobs/pipeline.py` (42, 56줄): `ku_portal`/`ku_openapi`에서 새 connector로 호출부 교체
- `onboarding.py` (21줄): `/connect` 흐름의 portal 통합 대체
- `ops_health_state.py` (10줄), `telegram_setup_state.py` (12, 16줄): import 정리
- `connectors/ku_openapi.py`: `build_ku_timetable_event` 같은 일부 헬퍼는 유지 가치 있음
- 데이터 모델 매핑: `EnrolledCourse` / `TimetableEntry` → KU_Secretary SQLite row

### Phase 6.5 — systemd user unit
- `deploy/systemd/`에 sample unit 추가 (이번 phase에서 시작)
- `cli_launchd.py`(34KB)를 `cli_systemd.py`로 분기 또는 OS 감지 추가
- `kus systemd install-uclass-poller` 등 명령 신설

### Phase 4.5 데이터 모델 매핑 노트
- `_kupid.timetable.TimetableEntry`: `(day_of_week, period, subject_name, classroom, start_time, end_time)`
- `_kupid.courses.EnrolledCourse`: `(course_code, section, course_type, course_name, professor, credits, schedule, retake, status, grad_code, dept_code)`
- KU_Secretary 기존 schedule row와 비교해서 어댑터 함수 신설 필요
