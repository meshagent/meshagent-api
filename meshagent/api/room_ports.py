from __future__ import annotations

from typing import Literal

ROOM_INTERNAL_API_PORT = 8078
RESERVED_ROOM_SERVICE_PORTS = frozenset({ROOM_INTERNAL_API_PORT})


def room_api_base_url(
    *,
    host: str,
    scheme: Literal["http", "https", "ws", "wss"],
) -> str:
    return f"{scheme}://{host}:{ROOM_INTERNAL_API_PORT}"
