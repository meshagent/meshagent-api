from __future__ import annotations

from collections.abc import Mapping
from typing import Any
import ssl

from aiohttp import ClientSession, TCPConnector
import certifi

LLM_ANNOTATION_HEADER_PREFIX = "X-Meshagent-Annotation-"
_LLM_ANNOTATION_HEADER_PREFIX_LOWER = LLM_ANNOTATION_HEADER_PREFIX.lower()


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
