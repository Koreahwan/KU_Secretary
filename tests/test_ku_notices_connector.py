"""Unit tests for the KU public notice scraper."""

from __future__ import annotations

from dataclasses import asdict

import pytest

from ku_secretary.connectors import ku_notices


_FIXTURE_HTML = """
<html><head><title>일반공지</title></head><body>
<table><tbody>
<tr>
  <td class="td-num"> 38 </td>
  <td class="td-title alignL">
    <a href="#1" onclick="jf_view('202604171054430778','1','ko');">
      LMS 서비스 중지 안내(5/3(일) 18시~20시)
    </a>
  </td>
  <td class="td-write"> 디지털전략팀 </td>
  <td class="td-date"> 2026-04-17 </td>
  <td class="td-access"> 386 </td>
</tr>
<tr>
  <td class="td-num"> 37 </td>
  <td class="td-title alignL">
    <a href="#1" onclick="jf_view('202603101402080330','1','ko');">
      ★[크림슨창업지원단] 학생창업자를 찾습니다★
    </a>
  </td>
  <td class="td-write"> 창업지원팀 </td>
  <td class="td-date"> 2026-03-12 </td>
  <td class="td-access"> 1440 </td>
</tr>
</tbody></table>
</body></html>
"""


class _StubResponse:
    def __init__(self, status_code: int, text: str, url: str = ""):
        self.status_code = status_code
        self.text = text
        self.url = url or "https://www.korea.ac.kr/ko/566/subview.do"


def test_fetch_ku_notice_feed_parses_rows(monkeypatch):
    captured = {}

    def fake_get(url, headers, timeout):
        captured["url"] = url
        captured["headers"] = headers
        return _StubResponse(200, _FIXTURE_HTML, url)

    monkeypatch.setattr(ku_notices.requests, "get", fake_get)
    result = ku_notices.fetch_ku_notice_feed("566", limit=10)
    assert result.metadata.parsed_count == 2
    assert result.metadata.http_status == 200
    assert "korea.ac.kr/ko/566" in captured["url"]
    seqs = [n.seq for n in result.notices]
    assert seqs == ["202604171054430778", "202603101402080330"]
    assert result.notices[0].title.startswith("LMS 서비스 중지")
    assert result.notices[0].department == "디지털전략팀"
    assert result.notices[0].posted_on == "2026-04-17"
    assert result.notices[0].article_url and "article_seq=" in result.notices[0].article_url


def test_fetch_ku_notice_feed_respects_limit(monkeypatch):
    monkeypatch.setattr(
        ku_notices.requests,
        "get",
        lambda url, headers, timeout: _StubResponse(200, _FIXTURE_HTML, url),
    )
    result = ku_notices.fetch_ku_notice_feed("566", limit=1)
    assert result.metadata.parsed_count == 1
    assert len(result.notices) == 1


def test_fetch_ku_notice_feed_empty_body(monkeypatch):
    html = "<html><body><p>no table</p></body></html>"
    monkeypatch.setattr(
        ku_notices.requests,
        "get",
        lambda url, headers, timeout: _StubResponse(200, html, url),
    )
    result = ku_notices.fetch_ku_notice_feed("566")
    assert result.notices == []
    assert result.metadata.empty_detected is True


def test_fetch_ku_notice_feed_http_error(monkeypatch):
    monkeypatch.setattr(
        ku_notices.requests,
        "get",
        lambda url, headers, timeout: _StubResponse(503, "boom", url),
    )
    with pytest.raises(ku_notices.KuNoticeFetchError) as exc_info:
        ku_notices.fetch_ku_notice_feed("566")
    err = exc_info.value
    assert "HTTP 503" in str(err)
    assert err.metadata["http_status"] == 503


def test_fetch_ku_notice_feed_network_error(monkeypatch):
    def boom(url, headers, timeout):
        raise ku_notices.requests.ConnectionError("nope")

    monkeypatch.setattr(ku_notices.requests, "get", boom)
    with pytest.raises(ku_notices.KuNoticeFetchError) as exc_info:
        ku_notices.fetch_ku_notice_feed("566")
    assert "network error" in str(exc_info.value)


def test_metadata_dataclass_round_trip():
    md = ku_notices.KuNoticeFetchMetadata(
        board_id="566",
        requested_limit=10,
        requested_at="2026-04-26T00:00:00+00:00",
        source_url="https://www.korea.ac.kr/ko/566/subview.do",
        resolved_url="https://www.korea.ac.kr/ko/566/subview.do",
    )
    payload = asdict(md)
    assert payload["board_id"] == "566"
    assert payload["parser"] == "ku_notice_table_v1"
