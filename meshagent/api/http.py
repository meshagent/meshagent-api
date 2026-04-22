from __future__ import annotations

from collections.abc import Mapping
from typing import Any
import ssl

from aiohttp import ClientSession, TCPConnector
import certifi


def new_client_session(*args: Any, **kwargs: Any) -> ClientSession:
    if "connector" not in kwargs:
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        kwargs["connector"] = TCPConnector(ssl=ssl_context)
    return ClientSession(*args, **kwargs)


def normalize_extra_headers(
    headers: Mapping[str, object] | None,
) -> dict[str, str]:
    normalized: dict[str, str] = {}
    if headers is None:
        return normalized

    for key, value in headers.items():
        if not isinstance(key, str) or key == "":
            continue
        if isinstance(value, str):
            normalized[key] = value

    return normalized
