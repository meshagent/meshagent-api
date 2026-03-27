from __future__ import annotations

from typing import Any
import ssl

from aiohttp import ClientSession, TCPConnector
import certifi


def new_client_session(*args: Any, **kwargs: Any) -> ClientSession:
    ca_file = kwargs.pop("ca_file", None)
    if "connector" not in kwargs:
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        if ca_file is not None:
            ssl_context.load_verify_locations(cafile=ca_file)
        kwargs["connector"] = TCPConnector(ssl=ssl_context)
    return ClientSession(*args, **kwargs)
