import hashlib
from collections.abc import AsyncIterable
from unittest.mock import AsyncMock

import pytest

from meshagent.api.messaging import BinaryContent, Content, ErrorContent
from meshagent.api.room_server_client import RoomClient, RoomException, SqliteClient
from meshagent.api.sqlite_transfer import CHUNK_SIZE


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "checksum", "truncated", "server"])
async def test_backup_checks_completion_and_requests_one_chunk_at_a_time(failure):
    client = SqliteClient(AsyncMock(spec=RoomClient))
    payload = b"SQLite format 3\x00" + b"x" * CHUNK_SIZE

    async def invoke(*, operation: str, input: AsyncIterable[Content]):
        assert operation == "backup"
        iterator = input.__aiter__()

        async def responses():
            start = await iterator.__anext__()
            assert start.headers == {
                "kind": "start",
                "database": "app",
                "namespace": ["team"],
            }
            for offset in range(0, len(payload), CHUNK_SIZE):
                pull = await iterator.__anext__()
                assert pull.headers == {"kind": "pull"}
                yield BinaryContent(
                    headers={"kind": "data"}, data=payload[offset : offset + CHUNK_SIZE]
                )
            if failure == "truncated":
                return
            if failure == "server":
                yield ErrorContent(text="read failed", code=1002)
                return
            yield BinaryContent(
                data=b"",
                headers={
                    "kind": "complete",
                    "size": len(payload),
                    "sha256": "0" * 64
                    if failure == "checksum"
                    else hashlib.sha256(payload).hexdigest(),
                },
            )

        return responses()

    client._invoke_stream = invoke
    if failure:
        with pytest.raises(RoomException):
            _ = [
                chunk
                async for chunk in client.backup(database="app", namespace=["team"])
            ]
    else:
        assert (
            b"".join(
                [
                    chunk
                    async for chunk in client.backup(database="app", namespace=["team"])
                ]
            )
            == payload
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "truncated", "mismatch", "source"])
async def test_restore_requires_matching_server_acknowledgement(failure):
    client = SqliteClient(AsyncMock(spec=RoomClient))
    payload = b"SQLite format 3\x00" + b"x" * 512

    async def source():
        yield payload
        if failure == "source":
            raise OSError("local read failed")

    async def invoke(*, operation, input):
        assert operation == "restore"
        iterator = input.__aiter__()

        async def responses():
            assert (await iterator.__anext__()).headers["database"] == "new"
            yield BinaryContent(data=b"", headers={"kind": "pull"})
            assert (await iterator.__anext__()).data == payload
            yield BinaryContent(data=b"", headers={"kind": "pull"})
            final = await iterator.__anext__()
            if failure == "source":
                assert final.headers["kind"] == "abort"
                yield ErrorContent(text="aborted", code=1002)
                return
            assert final.headers == {
                "kind": "complete",
                "size": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
            if failure == "truncated":
                return
            headers = dict(final.headers)
            if failure == "mismatch":
                headers["size"] += 1
            yield BinaryContent(data=b"", headers=headers)

        return responses()

    client._invoke_stream = invoke
    if failure:
        with pytest.raises(OSError if failure == "source" else RoomException):
            await client.restore(database="new", source=source())
    else:
        await client.restore(database="new", source=source())
