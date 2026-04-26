"""Korea University library seat availability connector.

Public read-only HODI API at librsv.korea.ac.kr — no authentication required.
Wraps the vendored ku-portal-mcp library module to expose synchronous helpers
consistent with KU_Secretary's other connectors.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict
from typing import Any

from ku_secretary._kupid.library import (
    LIBRARY_CODES,
    fetch_all_seats,
    fetch_library_seats,
)

logger = logging.getLogger(__name__)


def get_library_seats(library_name: str | None = None) -> dict[str, Any]:
    """Fetch real-time seat availability for KU libraries.

    Args:
        library_name: Korean substring of a library name (e.g. "중앙도서관").
            If None, returns data for all six libraries.

    Returns:
        {
            "libraries": { name: [room_dict, ...], ... },
            "summary": {
                "total_seats": int,
                "total_available": int,
                "total_in_use": int,
                "occupancy_rate": "12.3%",
            },
        }

    Raises:
        ValueError: when library_name is given but does not match any known library.
    """
    if library_name:
        code = _resolve_library_code(library_name)
        if code is None:
            raise ValueError(
                f"Unknown library: {library_name!r}. "
                f"Available: {list(LIBRARY_CODES.values())}"
            )
        rooms = asyncio.run(fetch_library_seats(code))
        libraries = {LIBRARY_CODES[code]: [asdict(r) for r in rooms]}
    else:
        all_data = asyncio.run(fetch_all_seats())
        libraries = {
            name: [asdict(r) for r in rooms] for name, rooms in all_data.items()
        }

    total = available = in_use = 0
    for rooms in libraries.values():
        for room in rooms:
            total += room["total_seats"]
            available += room["available"]
            in_use += room["in_use"]

    return {
        "libraries": libraries,
        "summary": {
            "total_seats": total,
            "total_available": available,
            "total_in_use": in_use,
            "occupancy_rate": (
                f"{(in_use / total * 100):.1f}%" if total else "0%"
            ),
        },
    }


def list_known_libraries() -> list[str]:
    """Return the list of known KU library names (Korean)."""
    return list(LIBRARY_CODES.values())


def _resolve_library_code(name: str) -> int | None:
    name = name.strip()
    for code, label in LIBRARY_CODES.items():
        if name in label or label in name:
            return code
    return None
