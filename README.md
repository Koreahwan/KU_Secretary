# KU Secretary

고려대학교 학생을 위한 로컬 실행형 Telegram 비서입니다.

본인 Mac에서 UClass, 고려대 시간표 API, 학교 공지를 동기화하고 Telegram 봇으로 오늘/내일 일정, 과제, 수업 자료, 공지, 리마인더를 확인할 수 있습니다. 데이터와 계정 정보는 기본적으로 사용자의 컴퓨터에 저장됩니다.

## 지원 범위

현재 사용자용 지원 범위는 고려대학교입니다.

- **Canvas LMS** (mylms.korea.ac.kr): 수강과목, 과제, 주차자료, 공지, 성적, 제출현황, 퀴즈, 게시판
- **KUPID 포털** (portal.korea.ac.kr): 시간표, 수강신청 내역, 개설과목 검색, 강의계획서, 전체 성적/누적 GPA
- **고려대 도서관** 좌석 현황 (인증 불필요)
- 학교 일반/학사 공지 조회
- Telegram 명령어 응답, 오전/저녁 브리핑, `/plan` 리마인더

> 고려대 메인 LMS는 **Canvas**입니다 (Blackboard 아님). KU_Secretary는 [ku-portal-mcp](https://github.com/SonAIengine/ku-portal-mcp) v0.10.1 (MIT)을 vendoring하여 KUPID SSO와 KSSO SAML SSO + RSA 복호화 흐름을 통해 Canvas REST API를 사용합니다. 자세한 내용은 [src/ku_secretary/_kupid/LICENSE](src/ku_secretary/_kupid/LICENSE)를 참고하세요.

> **OTP 미지원**: KSSO 계정에 OTP가 켜져 있으면 Canvas 자동 로그인 흐름이 실패합니다. KUPID에서 OTP를 끄고 사용하거나, 인증이 필요 없는 도서관 좌석 조회 등으로 한정해 사용하세요.

학교 계정 비밀번호, Telegram bot token, API key는 사용자가 직접 발급하고 관리해야 합니다. `.env`, `config.toml`, `data/`, `credentials/` 같은 로컬 설정/상태 파일은 공개 저장소에 올리지 마세요.

## 준비물

- macOS, **WSL2(Ubuntu)**, 또는 Linux (백그라운드 실행은 macOS launchd 또는 Linux systemd user unit)
- Python 3.11 이상
- Telegram 계정
- BotFather에서 만든 Telegram bot token
- 고려대 KUPID 학번/비밀번호 (Canvas + 시간표 + 공지)
- 선택 사항: 고려대 공공 API key (KU_OPENAPI_TIMETABLE_API_KEY)
- 선택 사항: `/connect` 웹 로그인 링크를 외부에서 열기 위한 HTTPS 터널 또는 리버스 프록시

Python은 macOS 기본 Python보다 Homebrew 또는 python.org 배포판을 권장합니다.

```bash
python3.11 -c "import ssl; print(ssl.OPENSSL_VERSION)"
```

`LibreSSL`이 보이면 네트워크 라이브러리에서 문제가 날 수 있으니 OpenSSL 기반 Python을 설치하는 편이 좋습니다.

## 설치

```bash
git clone https://github.com/Rark-Jeapah/KU_Secretary.git
cd KU_Secretary

python3.11 -m venv .venv
./.venv/bin/python -m pip install --upgrade pip
./.venv/bin/python -m pip install -e .

cp .env.example .env
```

개발/테스트까지 같이 설치하려면 마지막 설치 명령을 아래처럼 바꿉니다.

```bash
./.venv/bin/python -m pip install -e ".[dev]"
```

## Telegram 봇 만들기

1. Telegram에서 `@BotFather`를 엽니다.
2. `/newbot`을 입력하고 이름과 사용자명을 정합니다.
3. BotFather가 주는 token을 복사합니다.
4. 새로 만든 봇에게 `/start` 메시지를 보냅니다.
5. 아래 명령으로 `chat.id`를 확인합니다.

```bash
curl "https://api.telegram.org/bot<bot-token>/getUpdates"
```

응답 JSON에서 `message.chat.id` 값을 찾습니다. 이 값이 `TELEGRAM_ALLOWED_CHAT_IDS`에 들어갑니다.

## 기본 설정

`.env`를 열어 최소한 아래 값을 채웁니다. `<...>` 부분은 본인 값으로 바꾸세요.

```dotenv
STORAGE_ROOT_DIR=/path/to/KUSecretary
DATABASE_PATH=data/ku.db
TIMEZONE=Asia/Seoul

# KUPID 학번/비밀번호 (Canvas LMS + 시간표 + 공지)
KU_PORTAL_ID=<your-kupid-id>
KU_PORTAL_PW=<your-kupid-password>

# 선택: Moodle 호환 fallback (다른 학교 또는 구식 LMS 사용 시)
UCLASS_WS_BASE=
UCLASS_WSTOKEN=
UCLASS_USERNAME=
UCLASS_PASSWORD=

# 선택: KU OpenAPI 시간표 (KUPID 스크래핑이 기본, 이건 보조)
KU_OPENAPI_TIMETABLE_URL=
KU_OPENAPI_TIMETABLE_API_KEY=<optional-ku-api-key>

TELEGRAM_ENABLED=true
TELEGRAM_BOT_TOKEN=<telegram-bot-token>
TELEGRAM_ALLOWED_CHAT_IDS=<telegram-chat-id>
TELEGRAM_COMMANDS_ENABLED=true
TELEGRAM_SMART_COMMANDS_ENABLED=true

BRIEFING_ENABLED=true
BRIEFING_CHANNEL=telegram
BRIEFING_DELIVERY_MODE=direct
BRIEFING_MORNING_TIME_LOCAL=09:00
BRIEFING_EVENING_TIME_LOCAL=21:00

ONBOARDING_PUBLIC_BASE_URL=
```

`STORAGE_ROOT_DIR`는 동기화 산출물과 자료를 둘 폴더입니다. 예를 들어 본인 홈 아래의 `KUSecretary` 폴더를 쓰고 싶다면 실제 절대경로로 넣습니다.

UClass 연결은 `/connect` 웹 로그인 또는 `UCLASS_WSTOKEN` 사용을 권장합니다. `UCLASS_USERNAME`과 `UCLASS_PASSWORD`는 호환용 fallback으로 남아 있지만 기본 권장 방식은 아닙니다.

`config.example.toml`을 복사해 `config.toml`로 쓰는 방식도 지원합니다. 둘 다 있으면 `config.toml`과 같은 폴더의 `.env`가 함께 로드됩니다.

## 첫 실행 확인

설정 파일을 채운 뒤 먼저 로컬 설정을 확인합니다.

```bash
./.venv/bin/kus doctor --fix
./.venv/bin/kus status
```

`UCLASS_WSTOKEN`을 이미 넣었거나 `/connect`로 계정 연결을 마친 뒤에는 아래 순서로 실제 동기화를 확인합니다.

```bash
./.venv/bin/kus uclass probe
./.venv/bin/kus sync --all --wait --timeout-seconds 600
./.venv/bin/kus status
```

`doctor`는 설정과 로컬 폴더를 확인합니다. `uclass probe`는 UClass 연결을 확인합니다. `sync --all`은 UClass, 시간표, Telegram용 로컬 데이터를 한 번 동기화합니다.

## Telegram으로 사용하기

처음에는 백그라운드 등록 전에 터미널에서 직접 listener를 실행해 보는 것이 좋습니다.

```bash
./.venv/bin/kus telegram-listener
```

이 터미널을 켜 둔 상태에서 Telegram 봇에게 아래 명령을 보내세요.

```text
/start
/setup
/status
/today
/tomorrow
/todo
```

응답이 정상이라면 기본 사용 준비가 끝난 것입니다.

## 주요 Telegram 명령어

- `/start`: 시작 안내
- `/help`: 명령어 목록
- `/setup`: 현재 연결 상태와 다음 설정 안내
- `/status`: 마지막 동기화 상태와 저장된 데이터 개수
- `/today`: 오늘 수업, 일정, 과제
- `/tomorrow`: 내일 수업, 일정, 과제
- `/todo`: 개인 할일과 LMS 과제 통합 목록
- `/add <내용>`: 개인 할일 추가
- `/task <번호>`: `/todo` 항목 상세 보기
- `/task <번호> <새 내용>`: 개인 할일 수정
- `/done <번호>`: 개인 할일 완료 처리
- `/assignments`: 제출해야 할 LMS 과제와 공지/자료/게시판 제출 항목
- `/assignment <번호>`: `/assignments` 항목 상세 보기
- `/week`: 이번 주 마감 과제
- `/submitted`: 제출 완료 LMS 과제
- `/board`: 과목별 LMS 공지/게시판 최근 글
- `/materials`: 과목별 강의자료 위치
- `/todaysummary`: 오늘 수업 자료 요약
- `/tomorrowsummary`: 내일 수업 자료 요약
- `/notice_uclass`: 최근 UClass 공지
- `/notice_general`: 최근 학교 일반 공지
- `/notice_academic`: 최근 학교 학사 공지
- `/plan <내용>`: Telegram 리마인더 생성
- `/connect`: 웹 기반 계정 연결 링크 발급

## `/connect` 웹 로그인 사용

`/connect`는 Telegram에서 일회용 로그인 링크를 받고, 브라우저에서 학교 계정을 연결하는 흐름입니다. 이 기능을 쓰려면 onboarding 서버가 실행 중이어야 합니다.

로컬에서 먼저 실행합니다.

```bash
./.venv/bin/kus onboarding serve --host 127.0.0.1 --port 8791
```

외부 Telegram 앱에서 링크를 열려면 `http://127.0.0.1:8791`을 공개 HTTPS 주소로 전달해야 합니다. 예시는 다음과 같습니다.

- Tailscale Funnel
- Cloudflare Tunnel
- 본인 도메인과 Nginx/Caddy 리버스 프록시

HTTPS 주소를 준비한 뒤 `.env`에 아래처럼 넣습니다.

```dotenv
ONBOARDING_PUBLIC_BASE_URL=https://your-domain.example
```

주의할 점:

- 공개 URL은 반드시 HTTPS여야 합니다.
- 학교 계정 로그인 화면을 원격에서 여는 기능이므로 주소를 함부로 공유하지 마세요.
- 로컬 `http://127.0.0.1:8791/...`은 같은 Mac에서만 테스트용으로 사용하세요.

## 백그라운드 실행

터미널에서 직접 실행했을 때 문제가 없으면 백그라운드 등록이 가능합니다.

### macOS — launchd

macOS에서는 `launchd`에 등록해 항상 켜둘 수 있습니다.

```bash
sudo ./.venv/bin/kus launchd install-uclass-poller \
  --interval-minutes 60 \
  --connectivity-check-seconds 30 \
  --sync-timeout-seconds 600 \
  --scope daemon \
  --run-as-user "$(whoami)"

sudo ./.venv/bin/kus launchd install-telegram-listener \
  --poll-timeout-seconds 10 \
  --error-backoff-seconds 2 \
  --max-consecutive-errors 6 \
  --scope daemon \
  --run-as-user "$(whoami)"
```

`/connect`를 계속 쓸 경우 onboarding 서버도 등록합니다.

```bash
sudo ./.venv/bin/kus launchd install-onboarding \
  --host 127.0.0.1 \
  --port 8791 \
  --scope daemon \
  --run-as-user "$(whoami)"
```

상태 확인:

```bash
./.venv/bin/kus status
```

백그라운드 작업을 제거하려면 아래 명령을 씁니다.

```bash
sudo ./.venv/bin/kus launchd uninstall-uclass-poller --scope daemon
sudo ./.venv/bin/kus launchd uninstall-telegram-listener --scope daemon
sudo ./.venv/bin/kus launchd uninstall-onboarding --scope daemon
```

### Linux / WSL — systemd user unit

WSL2 또는 Linux에서는 `deploy/systemd/`에 있는 user unit 예제를 활용합니다. 자세한 설치 방법은 [`deploy/systemd/README.md`](deploy/systemd/README.md)를 참고하세요. KUPID/Canvas 자동 로그인이 잦으면 학교 abuse detection에 걸릴 수 있어 timer 간격은 60분 이상을 권장합니다.

## 새 KU connector 직접 사용

`kus` CLI 외에 Python 코드에서 직접 호출할 수도 있습니다.

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
my_courses, total_credits = ku_courses.get_my_courses(session)

# Canvas LMS (별도 KSSO SAML 흐름, 25분 캐시)
lms_session = ku_lms.login(user_id="2024000000", password="...")
courses = ku_lms.get_courses(lms_session)
todos = ku_lms.get_todo(lms_session)
```

자격증명을 `secret_store`에 보관해 두면 다음과 같이 부를 수도 있습니다.

```python
from ku_secretary.secret_store import default_secret_store
from ku_secretary.connectors import ku_portal_auth

store = default_secret_store(settings)
id_ref, pw_ref = ku_portal_auth.store_credentials(
    store=store, user_id="2024000000", password="..."
)
session = ku_portal_auth.login_with_secret_store(
    store=store, id_ref=id_ref, password_ref=pw_ref,
)
```

## 로컬 데이터와 보안

민감 정보가 들어갈 수 있는 위치입니다.

- `.env`: Telegram token, 학교 계정, API key
- `config.toml`: 설정을 TOML로 관리할 경우 민감 정보 포함 가능
- `data/ku.db`: 로컬 SQLite DB
- `data/secret_store*`: 로컬 secret 저장소
- `credentials/`: 브라우저나 외부 인증에 쓰는 로컬 자격 증명

권장 사항:

- 위 파일과 폴더를 Git에 커밋하지 마세요.
- token이나 비밀번호가 노출됐다고 생각되면 즉시 폐기하거나 회전하세요.
- Telegram bot token은 BotFather에서 재발급할 수 있습니다.
- `data/`를 삭제하면 로컬 동기화 상태와 캐시가 사라집니다. 삭제 전에는 필요한 자료를 백업하세요.

## 문제 해결

`doctor`에서 Python SSL 경고가 납니다.

- Homebrew 또는 python.org의 Python 3.11 이상으로 가상환경을 다시 만드세요.

Telegram 봇이 답하지 않습니다.

- `TELEGRAM_BOT_TOKEN`이 맞는지 확인하세요.
- 봇에게 `/start`를 한 번 보냈는지 확인하세요.
- `TELEGRAM_ALLOWED_CHAT_IDS`에 본인 chat id가 들어갔는지 확인하세요.
- `./.venv/bin/kus telegram-listener`가 실행 중인지 확인하세요.

UClass 연결이 실패합니다.

- `UCLASS_WS_BASE`가 고려대 UClass 주소인지 확인하세요.
- `UCLASS_USERNAME`, `UCLASS_PASSWORD`가 맞는지 확인하세요.
- 학교 인증 방식이 바뀐 경우 `/connect` 흐름을 사용하세요.

`/connect` 링크가 열리지 않습니다.

- 먼저 같은 Mac에서 `http://127.0.0.1:8791`이 열리는지 확인하세요.
- 로컬은 열리는데 공개 URL이 실패하면 터널, DNS, 리버스 프록시 설정 문제일 가능성이 큽니다.
- `ONBOARDING_PUBLIC_BASE_URL`은 `https://...`로 시작해야 합니다.

동기화 상태를 초기화하고 싶습니다.

1. launchd 작업을 중지합니다.
2. 필요한 파일을 백업합니다.
3. `data/` 폴더 또는 `DATABASE_PATH`에 지정한 DB를 삭제합니다.
4. `doctor --fix`와 `sync --all`을 다시 실행합니다.

## 개발과 테스트

테스트 의존성을 설치한 뒤 기본 테스트를 실행합니다.

```bash
./.venv/bin/python -m pip install -e ".[dev]"
./.venv/bin/python -m pytest -q
```

공개 배포용 staging을 만들 때는 sanitizer를 사용합니다.

```bash
./.venv/bin/python scripts/sanitize_release.py create dist/public --force
./.venv/bin/python scripts/sanitize_release.py validate dist/public
```

## 공개 저장소에 올리기 전 확인

개인 설정 파일과 런타임 산출물이 포함되지 않았는지 확인하세요.

```bash
git status --short
git grep -n -E "/Users/[A-Za-z0-9._-]+|[0-9]{1,3}(\\.[0-9]{1,3}){3}"
git grep -n -E "(TOKEN|PASSWORD|SECRET|API_KEY)=([A-Za-z0-9_./+=:-]{12,})" -- . ':!README.md' ':!.env.example' ':!config.example.toml' ':!tests/*'
```

실제 token, 비밀번호, 내부 호스트명, 개인 절대경로가 보이면 커밋하지 말고 먼저 제거하세요.

## 라이선스

이 프로젝트는 MIT License로 배포됩니다. 자세한 내용은 [LICENSE](LICENSE)를 확인하세요.
