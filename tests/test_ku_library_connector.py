"""Unit tests for the KU library connector.

These tests stub the vendored async fetchers, so they do not hit the network.
A separate live integration check lives at the bottom and is skipped unless
the LIBRARY_LIVE_TEST environment variable is set.
"""

from __future__ import annotations

import os

import pytest

from ku_secretary._kupid.library import LIBRARY_CODES, ReadingRoomStatus
from ku_secretary.connectors import ku_library


def _sample_room(name: str = "열람실A", total: int = 100, available: int = 30) -> ReadingRoomStatus:
    return ReadingRoomStatus(
        room_name=name,
        room_name_eng="Sample",
        total_seats=total,
        available=available,
        in_use=total - available,
        disabled=0,
        is_notebook_allowed=True,
        operating_hours="09:00-22:00",
    )


def test_get_library_seats_all(monkeypatch):
    async def fake_all():
        return {
            "중앙도서관": [_sample_room("열람실1", 200, 50)],
            "과학도서관": [_sample_room("열람실A", 100, 30)],
        }

    monkeypatch.setattr(ku_library, "fetch_all_seats", fake_all)

    result = ku_library.get_library_seats()
    assert set(result["libraries"]) == {"중앙도서관", "과학도서관"}
    assert result["summary"]["total_seats"] == 300
    assert result["summary"]["total_in_use"] == 220
    assert result["summary"]["total_available"] == 80
    assert result["summary"]["occupancy_rate"] == "73.3%"


def test_get_library_seats_filter_by_name(monkeypatch):
    captured: dict = {}

    async def fake_seats(code: int):
        captured["code"] = code
        return [_sample_room("열람실1", 50, 10)]

    monkeypatch.setattr(ku_library, "fetch_library_seats", fake_seats)

    result = ku_library.get_library_seats("중앙도서관")
    assert "중앙도서관" in result["libraries"]
    assert captured["code"] == 1  # 중앙도서관 → code 1
    assert result["summary"]["total_seats"] == 50
    assert result["summary"]["total_in_use"] == 40


def test_get_library_seats_unknown_name():
    with pytest.raises(ValueError):
        ku_library.get_library_seats("뉴욕공립도서관")


def test_list_known_libraries_matches_upstream():
    assert ku_library.list_known_libraries() == list(LIBRARY_CODES.values())


@pytest.mark.skipif(
    not os.environ.get("LIBRARY_LIVE_TEST"),
    reason="LIBRARY_LIVE_TEST=1 to run live network test",
)
def test_live_fetch_central_library_smoke():
    result = ku_library.get_library_seats("중앙도서관")
    assert result["libraries"]
    assert "summary" in result
