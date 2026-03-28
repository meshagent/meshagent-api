from __future__ import annotations

from typing import Any
import ssl

from aiohttp import ClientSession, TCPConnector
import certifi


def new_client_session(*args: Any, **kwargs: Any) -> ClientSession:
    if "connector" not in kwargs:
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        kwargs["connector"] = TCPConnector(ssl=ssl_context)
    return ClientSession(*args, **kwargs)
