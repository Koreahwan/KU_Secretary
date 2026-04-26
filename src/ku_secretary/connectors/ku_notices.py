"""Korea University global notice scraper.

Public board pages on `https://www.korea.ac.kr/ko/<board_id>/subview.do`
served as static HTML. No login required.

Boards:
- 566: 일반공지
- 567: 학사공지
- 568: 장학금공지

The page renders a `<table>` whose `<tbody>` rows expose:
- `td.td-num`     → 게시 번호
- `td.td-title`   → 제목 (anchor with `onclick="jf_view('<article_seq>', ...)"`)
- `td.td-write`   → 작성 부서
- `td.td-date`    → 게시일 (YYYY-MM-DD)
- `td.td-access`  → 조회수
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import re
from typing import Any

import requests


KU_NOTICE_BASE = "https://www.korea.ac.kr"
KU_NOTICE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko,en;q=0.8",
}

KU_NOTICE_BOARDS = {
    "general": {"label": "학교 일반공지", "board_id": "566"},
    "academic": {"label": "학교 학사공지", "board_id": "567"},
    "scholarship": {"label": "학교 장학금공지", "board_id": "568"},
}


@dataclass
class KuNotice:
    seq: str
    title: str
    department: str | None
    posted_on: str | None
    board_id: str
    source_url: str
    article_url: str | None = None
    sort: str | None = None
    list_id: str = ""
    menuid: str = ""


@dataclass
class KuNoticeFetchMetadata:
    board_id: str
    requested_limit: int
    requested_at: str
    source_url: str
    resolved_url: str
    fetched_at: str | None = None
    http_status: int | None = None
    page_title: str | None = None
    parser: str = "ku_notice_table_v1"
    parsed_count: int = 0
    empty_detected: bool = False
    list_id: str = ""
    menuid: str = ""


@dataclass
class KuNoticeFetchResult:
    notices: list[KuNotice]
    metadata: KuNoticeFetchMetadata


class KuNoticeFetchError(RuntimeError):
    def __init__(self, message: str, *, metadata: KuNoticeFetchMetadata | dict[str, Any] | None = None) -> None:
        super().__init__(message)
        if isinstance(metadata, KuNoticeFetchMetadata):
            self.metadata = asdict(metadata)
        elif isinstance(metadata, dict):
            self.metadata = dict(metadata)
        else:
            self.metadata = {}


_ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.DOTALL)
_TBODY_RE = re.compile(r"<tbody[^>]*>(.*?)</tbody>", re.DOTALL)
_TITLE_TD_RE = re.compile(
    r'<td[^>]*class="[^"]*td-title[^"]*"[^>]*>(.*?)</td>', re.DOTALL
)
_WRITE_TD_RE = re.compile(
    r'<td[^>]*class="[^"]*td-write[^"]*"[^>]*>(.*?)</td>', re.DOTALL
)
_DATE_TD_RE = re.compile(
    r'<td[^>]*class="[^"]*td-date[^"]*"[^>]*>(.*?)</td>', re.DOTALL
)
_NUM_TD_RE = re.compile(
    r'<td[^>]*class="[^"]*td-num[^"]*"[^>]*>(.*?)</td>', re.DOTALL
)
_JF_VIEW_RE = re.compile(r"jf_view\(\s*'([^']+)'")
_TAG_RE = re.compile(r"<[^>]+>")
_PAGE_TITLE_RE = re.compile(r"<title>(.*?)</title>", re.DOTALL | re.IGNORECASE)


def _strip_tags(value: str) -> str:
    return re.sub(r"\s+", " ", _TAG_RE.sub(" ", value)).strip()


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def fetch_ku_notice_feed(
    board_id: str,
    *,
    limit: int = 10,
    timeout_sec: int = 15,
) -> KuNoticeFetchResult:
    """Fetch the latest notice rows for a KU public board."""
    bid = str(board_id).strip()
    if not bid:
        raise ValueError("board_id is required")
    source_url = f"{KU_NOTICE_BASE}/ko/{bid}/subview.do"
    requested_at = _now_utc_iso()
    metadata = KuNoticeFetchMetadata(
        board_id=bid,
        requested_limit=int(limit),
        requested_at=requested_at,
        source_url=source_url,
        resolved_url=source_url,
        list_id=bid,
        menuid=bid,
    )
    try:
        response = requests.get(source_url, headers=KU_NOTICE_HEADERS, timeout=timeout_sec)
    except requests.RequestException as exc:
        metadata.fetched_at = _now_utc_iso()
        raise KuNoticeFetchError(f"network error: {exc}", metadata=metadata) from exc

    metadata.http_status = response.status_code
    metadata.fetched_at = _now_utc_iso()
    metadata.resolved_url = str(response.url or source_url)
    if response.status_code != 200:
        raise KuNoticeFetchError(
            f"upstream HTTP {response.status_code}", metadata=metadata
        )

    html = response.text
    page_title_match = _PAGE_TITLE_RE.search(html)
    if page_title_match:
        metadata.page_title = _strip_tags(page_title_match.group(1))[:80]

    body_match = _TBODY_RE.search(html)
    if not body_match:
        metadata.empty_detected = True
        return KuNoticeFetchResult(notices=[], metadata=metadata)

    body = body_match.group(1)
    rows = _ROW_RE.findall(body)
    notices: list[KuNotice] = []
    for row in rows:
        title_match = _TITLE_TD_RE.search(row)
        if not title_match:
            continue
        title_html = title_match.group(1)
        title_text = _strip_tags(title_html)
        if not title_text:
            continue
        seq_match = _JF_VIEW_RE.search(title_html)
        seq = seq_match.group(1).strip() if seq_match else ""
        if not seq:
            num_match = _NUM_TD_RE.search(row)
            seq = _strip_tags(num_match.group(1)) if num_match else ""
        write_match = _WRITE_TD_RE.search(row)
        date_match = _DATE_TD_RE.search(row)
        notice = KuNotice(
            seq=seq or title_text[:32],
            title=title_text,
            department=_strip_tags(write_match.group(1)) if write_match else None,
            posted_on=_strip_tags(date_match.group(1)) if date_match else None,
            board_id=bid,
            source_url=source_url,
            article_url=(
                f"{KU_NOTICE_BASE}/ko/{bid}/artclView.do?article_seq={seq}" if seq else None
            ),
            list_id=bid,
            menuid=bid,
        )
        notices.append(notice)
        if len(notices) >= int(limit):
            break

    metadata.parsed_count = len(notices)
    metadata.empty_detected = len(notices) == 0
    return KuNoticeFetchResult(notices=notices, metadata=metadata)
