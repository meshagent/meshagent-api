from __future__ import annotations

from collections.abc import Mapping
from typing import Any
import os
import ssl

from aiohttp import ClientSession, TCPConnector
from aiohttp.abc import AbstractResolver
from aiohttp.resolver import DefaultResolver
import certifi

LLM_ANNOTATION_HEADER_PREFIX = "X-Meshagent-Annotation-"
_LLM_ANNOTATION_HEADER_PREFIX_LOWER = LLM_ANNOTATION_HEADER_PREFIX.lower()


class _HostAliasResolver(AbstractResolver):
    def __init__(self, aliases: Mapping[str, str]) -> None:
        self._aliases = aliases
        self._resolver = DefaultResolver()

    async def resolve(
        self,
        host: str,
        port: int = 0,
        family: int = 0,
    ) -> list[dict[str, Any]]:
        return await self._resolver.resolve(
            self._aliases.get(host, host),
            port=port,
            family=family,
        )

    async def close(self) -> None:
        await self._resolver.close()


def _http_host_aliases() -> dict[str, str]:
    aliases: dict[str, str] = {}
    for item in os.environ.get("MESHAGENT_HTTP_HOST_ALIASES", "").split(","):
        source, separator, target = item.partition("=")
        if separator != "=":
            continue
        source = source.strip()
        target = target.strip()
        if source and target:
            aliases[source] = target
    return aliases


def new_tcp_connector(*args: Any, **kwargs: Any) -> TCPConnector:
    if "ssl" not in kwargs:
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        extra_ca_file = os.environ.get("MESHAGENT_EXTRA_CA_FILE", "").strip()
        if extra_ca_file:
            ssl_context.load_verify_locations(cafile=extra_ca_file)
        kwargs["ssl"] = ssl_context
    if "resolver" not in kwargs:
        aliases = _http_host_aliases()
        if aliases:
            kwargs["resolver"] = _HostAliasResolver(aliases)
    return TCPConnector(*args, **kwargs)


def new_client_session(*args: Any, **kwargs: Any) -> ClientSession:
    if "connector" not in kwargs:
        kwargs["connector"] = new_tcp_connector()
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


def normalize_llm_annotations(
    annotations: Mapping[str, object] | None,
) -> dict[str, str]:
    normalized: dict[str, str] = {}
    if annotations is None:
        return normalized

    for key, value in annotations.items():
        if not isinstance(key, str) or key == "":
            continue
        if isinstance(value, str):
            normalized[key.lower()] = value

    return normalized


def llm_annotation_headers(
    annotations: Mapping[str, object] | None,
) -> dict[str, str]:
    return {
        f"{LLM_ANNOTATION_HEADER_PREFIX}{key}": value
        for key, value in normalize_llm_annotations(annotations).items()
    }


def extract_llm_annotation_headers(
    headers: Mapping[str, object] | None,
) -> dict[str, str]:
    annotations: dict[str, str] = {}
    if headers is None:
        return annotations

    for key, value in headers.items():
        if not isinstance(key, str) or not isinstance(value, str):
            continue
        key_lower = key.lower()
        if not key_lower.startswith(_LLM_ANNOTATION_HEADER_PREFIX_LOWER):
            continue
        annotation_name = key_lower[len(_LLM_ANNOTATION_HEADER_PREFIX_LOWER) :]
        if annotation_name == "":
            continue
        annotations[annotation_name] = value

    return annotations


def remove_llm_annotation_headers(headers: dict[str, str]) -> None:
    for key in list(headers.keys()):
        if key.lower().startswith(_LLM_ANNOTATION_HEADER_PREFIX_LOWER):
            del headers[key]
