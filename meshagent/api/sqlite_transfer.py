"""Bounded, checksum-verified whole-database transfers for the SQLite toolkit."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import AsyncIterable, AsyncIterator
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from meshagent.api.messaging import BinaryContent, Content, ErrorContent
from meshagent.api.room_server_client import RoomException

if TYPE_CHECKING:
    from meshagent.api.room_server_client import SqliteClient

CHUNK_SIZE = 256 * 1024


class TransferComplete(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    kind: Literal["complete"]
    size: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class _TransferInput:
    def __init__(
        self,
        database: str,
        namespace: list[str] | None,
        source: AsyncIterable[bytes] | None = None,
    ) -> None:
        self.start = {"kind": "start", "database": database, "namespace": namespace}
        self.source = source.__aiter__() if source is not None else None
        self.pulls: asyncio.Queue[None] = asyncio.Queue()
        self.closed = False
        self.sent_start = False
        self.complete: TransferComplete | None = None
        self.digest = hashlib.sha256()
        self.size = 0
        self.error: Exception | None = None

    def request_next(self) -> None:
        self.pulls.put_nowait(None)

    def close(self) -> None:
        self.closed = True
        self.request_next()

    def __aiter__(self) -> _TransferInput:
        return self

    async def __anext__(self) -> Content:
        if not self.sent_start:
            self.sent_start = True
            return BinaryContent(data=b"", headers=self.start)
        await self.pulls.get()
        if self.closed:
            raise StopAsyncIteration
        if self.source is None:
            return BinaryContent(data=b"", headers={"kind": "pull"})
        if self.complete is not None:
            raise RoomException("Unexpected pull after SQLite upload completed")
        try:
            data = await self.source.__anext__()
            if not isinstance(data, bytes) or not 0 < len(data) <= CHUNK_SIZE:
                raise ValueError(
                    f"SQLite upload chunks must contain 1–{CHUNK_SIZE} bytes"
                )
        except StopAsyncIteration:
            self.complete = TransferComplete(
                kind="complete", size=self.size, sha256=self.digest.hexdigest()
            )
            return BinaryContent(data=b"", headers=self.complete.model_dump())
        except Exception as error:
            self.error = error
            return BinaryContent(data=b"", headers={"kind": "abort"})
        self.digest.update(data)
        self.size += len(data)
        return BinaryContent(data=data, headers={"kind": "data"})


def _binary(chunk: Content) -> BinaryContent:
    if isinstance(chunk, ErrorContent):
        raise RoomException(chunk.text, code=chunk.code)
    if not isinstance(chunk, BinaryContent):
        raise RoomException("SQLite transfer ended without verified completion")
    return chunk


def _complete(chunk: BinaryContent) -> TransferComplete:
    try:
        result = TransferComplete.model_validate(chunk.headers)
    except ValidationError as error:
        raise RoomException("Invalid SQLite transfer completion") from error
    if chunk.data:
        raise RoomException("Invalid SQLite transfer completion payload")
    return result


async def backup(
    client: SqliteClient,
    database: str,
    namespace: list[str] | None,
) -> AsyncIterator[bytes]:
    """Yield file bytes; callers must consume through verified completion."""
    inputs = _TransferInput(database, namespace)
    digest = hashlib.sha256()
    size = 0
    try:
        responses = await client._invoke_stream(operation="backup", input=inputs)
        inputs.request_next()
        async for response in responses:
            chunk = _binary(response)
            if chunk.headers.get("kind") == "complete":
                result = _complete(chunk)
                if result.size != size or result.sha256 != digest.hexdigest():
                    raise RoomException("SQLite backup size or SHA-256 mismatch")
                return
            if (
                chunk.headers.get("kind") != "data"
                or not 0 < len(chunk.data) <= CHUNK_SIZE
            ):
                raise RoomException("Invalid SQLite backup data frame")
            digest.update(chunk.data)
            size += len(chunk.data)
            yield chunk.data
            inputs.request_next()
        raise RoomException("SQLite backup interrupted before completion")
    finally:
        inputs.close()


async def restore(
    client: SqliteClient,
    database: str,
    namespace: list[str] | None,
    source: AsyncIterable[bytes],
) -> None:
    inputs = _TransferInput(database, namespace, source)
    try:
        responses = await client._invoke_stream(operation="restore", input=inputs)
        async for response in responses:
            if inputs.error is not None:
                raise inputs.error
            chunk = _binary(response)
            if chunk.headers.get("kind") == "pull" and not chunk.data:
                inputs.request_next()
            elif chunk.headers.get("kind") == "complete":
                result = _complete(chunk)
                if inputs.complete is None or result != inputs.complete:
                    raise RoomException(
                        "SQLite restore acknowledgement does not match upload"
                    )
                return
            else:
                raise RoomException("Invalid SQLite restore response")
        raise RoomException("SQLite restore ended without durable completion")
    finally:
        inputs.close()
