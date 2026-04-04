import asyncio
import base64
import contextlib
import json
from collections.abc import AsyncIterable, AsyncIterator
from types import SimpleNamespace

import pytest

from meshagent.api.messaging import (
    BinaryContent,
    Content,
    ControlCloseStatus,
    EmptyContent,
    ErrorContent,
    FileContent,
    JsonContent,
    TextContent,
    _ControlContent,
    pack_message,
)
from meshagent.api import ErrorCode
from meshagent.api.oauth import OAuthClientConfig
from meshagent.api.room_server_client import (
    AgentsClient,
    ContainersClient,
    DatabaseClient,
    DeveloperClient,
    DockerSecret,
    ListDataType,
    LivekitClient,
    MemoryClient,
    MemoryEntityRecord,
    MessagingClient,
    QueuesClient,
    RemoteParticipant,
    RoomMessage,
    RoomClient,
    RoomException,
    SecretsClient,
    ServicesClient,
    StorageClient,
    StructDataType,
    SyncClient,
    TextDataType,
)
from meshagent.api.specs.service import (
    ConfigMountSpec,
    ContainerMountSpec,
    EmptyDirMountSpec,
    RoomStorageMountSpec,
)
from meshagent.api.schema import ElementType, MeshSchema


class _FakeProtocol:
    def __init__(self):
        self.handlers: dict[str, object] = {}

    def register_handler(self, typ: str, handler: object) -> None:
        self.handlers[typ] = handler

    def unregister_handler(self, typ: str, handler: object) -> None:
        assert self.handlers[typ] is handler
        self.handlers.pop(typ)

    def get_handler(self, typ: str) -> object | None:
        return self.handlers.get(typ)

    async def wait_for_close(self) -> None:
        await asyncio.Future()


def test_room_exception_defaults_to_invalid_request_code() -> None:
    ex = RoomException("boom")
    assert ex.code == ErrorCode.INVALID_REQUEST


def test_room_exception_explicit_none_code_is_preserved() -> None:
    ex = RoomException("boom", code=None)
    assert ex.code is None


class _FakeRoom:
    _ensure_close_watcher = RoomClient._ensure_close_watcher
    _fail_tool_call_streams = RoomClient._fail_tool_call_streams
    _handle_tool_call_response_chunk = RoomClient._handle_tool_call_response_chunk
    _make_tool_call_stream = RoomClient._make_tool_call_stream
    _send_tool_call_request_chunk = RoomClient._send_tool_call_request_chunk
    _stream_tool_call_request_chunks = RoomClient._stream_tool_call_request_chunks
    invoke = RoomClient.invoke
    list_toolkits = RoomClient.list_toolkits

    def __init__(self):
        self.protocol = _FakeProtocol()
        self.events: list[tuple[str, dict]] = []
        self.requests: list[tuple[str, dict, bytes | None]] = []
        self._tool_call_streams = {}
        self._close_watcher_task = None
        self.list_toolkits_response: dict | None = None

    def emit(self, event_name: str, **kwargs) -> None:
        self.events.append((event_name, kwargs))

    async def send_request(
        self, typ: str, request: dict, data: bytes | None = None
    ) -> JsonContent | dict:
        self.requests.append((typ, request, data))
        if typ == "room.invoke_tool":
            await asyncio.sleep(0)
            return JsonContent(json={"ok": True})
        if typ == "room.list_toolkits" and self.list_toolkits_response is not None:
            return self.list_toolkits_response

        return {}


class _BadResponseRoom:
    async def send_request(
        self, typ: str, request: dict, data: bytes | None = None
    ) -> dict:
        del typ, request, data
        return {}

    async def invoke(self, **kwargs) -> dict:
        del kwargs
        return {}


async def _cancel_close_watcher(room: _FakeRoom) -> None:
    task = room._close_watcher_task
    if task is None:
        return

    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


class _BadStorageResponseRoom:
    def __init__(self) -> None:
        self.protocol = _FakeProtocol()

    async def send_request(
        self, typ: str, request: dict, data: bytes | None = None
    ) -> dict:
        del typ, request, data
        return {}

    async def invoke(self, **kwargs) -> dict:
        del kwargs
        return {}


class _StreamingStorageRoom:
    def __init__(self, *, upload_pull_chunk_size: int | None = 128 * 1024) -> None:
        self.protocol = _FakeProtocol()
        self.requests: list[dict] = []
        self.upload_starts: list[BinaryContent] = []
        self.upload_chunks: list[BinaryContent] = []
        self.download_starts: list[BinaryContent] = []
        self.download_pulls: list[BinaryContent] = []
        self.upload_pull_chunk_size = upload_pull_chunk_size

    def _upload_pull_headers(self) -> dict[str, object]:
        headers: dict[str, object] = {"kind": "pull"}
        if self.upload_pull_chunk_size is not None:
            headers["chunk_size"] = self.upload_pull_chunk_size
        return headers

    async def invoke(self, **kwargs):
        self.requests.append(kwargs)
        tool = kwargs["tool"]
        tool_input = kwargs["input"]

        if tool == "upload":
            assert isinstance(tool_input, AsyncIterable)

            async def stream() -> AsyncIterator[Content]:
                iterator = tool_input.__aiter__()
                start_chunk = await iterator.__anext__()
                assert isinstance(start_chunk, BinaryContent)
                self.upload_starts.append(start_chunk)
                yield BinaryContent(data=b"", headers=self._upload_pull_headers())
                async for chunk in iterator:
                    assert isinstance(chunk, BinaryContent)
                    self.upload_chunks.append(chunk)
                    yield BinaryContent(data=b"", headers=self._upload_pull_headers())
                yield _ControlContent(method="close")

            return stream()

        if tool == "download":
            assert isinstance(tool_input, AsyncIterable)

            async def stream() -> AsyncIterator[Content]:
                iterator = tool_input.__aiter__()
                start_chunk = await iterator.__anext__()
                assert isinstance(start_chunk, BinaryContent)
                self.download_starts.append(start_chunk)
                yield BinaryContent(
                    data=b"",
                    headers={
                        "kind": "start",
                        "name": "file.txt",
                        "mime_type": "text/plain",
                        "size": 11,
                    },
                )
                async for request_chunk in iterator:
                    assert isinstance(request_chunk, BinaryContent)
                    self.download_pulls.append(request_chunk)
                    if len(self.download_pulls) == 1:
                        yield BinaryContent(
                            data=b"hello ",
                            headers={
                                "kind": "data",
                            },
                        )
                    elif len(self.download_pulls) == 2:
                        yield BinaryContent(
                            data=b"world",
                            headers={
                                "kind": "data",
                            },
                        )
                        yield _ControlContent(method="close")
                        return
                    else:
                        yield _ControlContent(method="close")
                        return

            return stream()

        raise AssertionError(f"unexpected tool: {tool}")


class _BadSyncResponseRoom:
    def __init__(self) -> None:
        self.protocol = _FakeProtocol()

    async def send_request(
        self, typ: str, request: dict, data: bytes | None = None
    ) -> TextContent:
        del typ, request, data
        return TextContent(text="unexpected")

    async def invoke(self, **kwargs) -> TextContent:
        del kwargs
        return TextContent(text="unexpected")


@pytest.mark.asyncio
async def test_tool_call_response_chunk_unpacks_json_chunk_payload() -> None:
    room = _FakeRoom()
    chunk = JsonContent(json={"hello": "world"})

    await room._handle_tool_call_response_chunk(
        protocol=room.protocol,  # type: ignore[arg-type]
        message_id=1,
        typ="room.tool_call_response_chunk",
        data=pack_message(
            header={"tool_call_id": "tc-1", "chunk": chunk.to_json()},
            data=chunk.get_data(),
        ),
    )

    assert len(room.events) == 1
    assert room.events[0][0] == "room.tool_call_response_chunk"
    event = room.events[0][1]["event"]
    assert isinstance(event, dict)
    assert event["tool_call_id"] == "tc-1"
    assert isinstance(event["chunk"], JsonContent)
    assert event["chunk"].json == {"hello": "world"}


@pytest.mark.asyncio
async def test_list_toolkits_preserves_strict_tool_metadata() -> None:
    room = _FakeRoom()
    room.list_toolkits_response = {
        "tools": {
            "test": {
                "title": "Test",
                "description": "desc",
                "tools": {
                    "strict_tool": {
                        "title": "Strict Tool",
                        "description": "desc",
                        "input_spec": {
                            "types": ["json"],
                            "stream": False,
                            "schema": {
                                "type": "object",
                                "properties": {},
                                "required": [],
                                "additionalProperties": False,
                            },
                        },
                        "strict": False,
                    }
                },
            }
        }
    }

    toolkits = await room.list_toolkits()

    assert len(toolkits) == 1
    assert len(toolkits[0].tools) == 1
    assert toolkits[0].tools[0].strict is False


@pytest.mark.asyncio
async def test_memory_client_unexpected_response_uses_error_code() -> None:
    client = MemoryClient(room=_BadResponseRoom())  # type: ignore[arg-type]

    with pytest.raises(
        RoomException, match="unexpected return type from memory.list"
    ) as ex:
        await client.list()

    assert ex.value.code == ErrorCode.UNEXPECTED_RESPONSE_TYPE


@pytest.mark.asyncio
async def test_memory_client_uses_room_invoke_for_commands() -> None:
    class _FakeMemoryRoom(_FakeRoom):
        async def send_request(
            self, typ: str, request: dict, data: bytes | None = None
        ) -> Content | dict:
            self.requests.append((typ, request, data))
            if typ != "room.invoke_tool":
                return {}

            tool = request["tool"]
            if tool == "list":
                return JsonContent(json={"memories": []})
            if tool == "ingest_image":
                return JsonContent(
                    json={
                        "name": "graph",
                        "stats": {
                            "entities": 0,
                            "relationships": 0,
                            "sources": 0,
                        },
                        "entity_ids": [],
                    }
                )
            return EmptyContent()

    room = _FakeMemoryRoom()
    client = MemoryClient(room=room)  # type: ignore[arg-type]

    await client.create(name="graph")
    await client.list(namespace=["team"])
    await client.upsert_table(
        name="graph",
        table="Entity",
        records=[{"payload": b"hello"}],
    )
    await client.upsert_nodes(
        name="graph",
        records=[
            MemoryEntityRecord(
                name="Alice",
                entity_type="PERSON",
                metadata={"role": "admin"},
            )
        ],
    )
    await client.ingest_image(
        name="graph",
        caption="Alice",
        data=b"\x89PNG",
        mime_type="image/png",
        annotations={"kind": "demo"},
    )

    assert len(room.requests) == 5
    assert [request[0] for request in room.requests] == [
        "room.invoke_tool",
        "room.invoke_tool",
        "room.invoke_tool",
        "room.invoke_tool",
        "room.invoke_tool",
    ]
    assert [request[1]["toolkit"] for request in room.requests] == [
        "memory",
        "memory",
        "memory",
        "memory",
        "memory",
    ]
    assert [request[1]["tool"] for request in room.requests] == [
        "create",
        "list",
        "upsert_table",
        "upsert_nodes",
        "ingest_image",
    ]

    upsert_table_arguments = room.requests[2][1]["arguments"]
    assert isinstance(upsert_table_arguments, dict)
    upsert_table_input = upsert_table_arguments["json"]
    assert isinstance(upsert_table_input, dict)
    assert isinstance(upsert_table_input["records_json"], str)
    encoded_records = json.loads(upsert_table_input["records_json"])
    assert encoded_records == [
        {"payload": {"encoding": "base64", "data": base64.b64encode(b"hello").decode()}}
    ]

    ingest_image_arguments = room.requests[4][1]["arguments"]
    assert isinstance(ingest_image_arguments, dict)
    ingest_image_input = ingest_image_arguments["json"]
    assert isinstance(ingest_image_input, dict)
    assert ingest_image_input["data_base64"] == base64.b64encode(b"\x89PNG").decode()
    assert json.loads(ingest_image_input["annotations_json"]) == {"kind": "demo"}


@pytest.mark.asyncio
async def test_messaging_client_uses_room_invoke_for_commands() -> None:
    class _FakeMessagingRoom(_FakeRoom):
        def __init__(self) -> None:
            super().__init__()
            self.local_participant = SimpleNamespace(id="local-participant")

        async def send_request(
            self, typ: str, request: dict, data: bytes | None = None
        ) -> Content | dict:
            self.requests.append((typ, request, data))
            if typ != "room.invoke_tool":
                return {}
            return EmptyContent()

    room = _FakeMessagingRoom()
    client = MessagingClient(room=room)  # type: ignore[arg-type]

    await client.enable()
    await client.broadcast_message(
        type="broadcast",
        message={"hello": "world"},
        attachment=b"bytes",
    )
    await client.start()
    await client.send_message(
        to=SimpleNamespace(id="remote-participant"),
        type="direct",
        message={"value": 1},
        attachment=b"\x00\x01",
    )
    await client.stop()
    await client.disable()
    await _cancel_close_watcher(room)

    assert [request[0] for request in room.requests] == [
        "room.invoke_tool",
        "room.invoke_tool",
        "room.invoke_tool",
        "room.invoke_tool",
    ]
    assert [request[1]["toolkit"] for request in room.requests] == [
        "messaging",
        "messaging",
        "messaging",
        "messaging",
    ]
    assert [request[1]["tool"] for request in room.requests] == [
        "enable",
        "broadcast",
        "send",
        "disable",
    ]

    broadcast_arguments = room.requests[1][1]["arguments"]
    assert isinstance(broadcast_arguments, dict)
    broadcast_input = broadcast_arguments["json"]
    assert isinstance(broadcast_input, dict)
    assert json.loads(broadcast_input["message_json"]) == {"hello": "world"}
    assert broadcast_input["attachment_base64"] == base64.b64encode(b"bytes").decode()

    send_arguments = room.requests[2][1]["arguments"]
    assert isinstance(send_arguments, dict)
    send_input = send_arguments["json"]
    assert isinstance(send_input, dict)
    assert send_input["to_participant_id"] == "remote-participant"
    assert json.loads(send_input["message_json"]) == {"value": 1}
    assert send_input["attachment_base64"] == base64.b64encode(b"\x00\x01").decode()


@pytest.mark.asyncio
async def test_messaging_client_send_message_resolves_online_remote_participant_by_id() -> (
    None
):
    class _FakeMessagingRoom(_FakeRoom):
        def __init__(self) -> None:
            super().__init__()
            self.local_participant = SimpleNamespace(id="local-participant")

        async def send_request(
            self, typ: str, request: dict, data: bytes | None = None
        ) -> Content | dict:
            self.requests.append((typ, request, data))
            if typ != "room.invoke_tool":
                return {}
            return EmptyContent()

    room = _FakeMessagingRoom()
    client = MessagingClient(room=room)  # type: ignore[arg-type]

    await client.start()
    client._on_participant_enabled(
        RoomMessage(
            from_participant_id="remote-participant",
            type="participant.enabled",
            message={
                "id": "remote-participant",
                "role": "user",
                "attributes": {"name": "Remote User"},
            },
        )
    )

    await client.send_message(
        to=RemoteParticipant(id="remote-participant"),
        type="direct",
        message={"value": 1},
    )
    await client.stop()
    await _cancel_close_watcher(room)

    assert len(room.requests) == 1
    send_arguments = room.requests[0][1]["arguments"]
    assert isinstance(send_arguments, dict)
    send_input = send_arguments["json"]
    assert isinstance(send_input, dict)
    assert send_input["to_participant_id"] == "remote-participant"
    assert json.loads(send_input["message_json"]) == {"value": 1}


@pytest.mark.asyncio
async def test_messaging_client_drops_nowait_messages_for_removed_participant() -> None:
    class _FakeMessagingRoom(_FakeRoom):
        def __init__(self) -> None:
            super().__init__()
            self.local_participant = SimpleNamespace(id="local-participant")

        async def send_request(
            self, typ: str, request: dict, data: bytes | None = None
        ) -> Content | dict:
            self.requests.append((typ, request, data))
            if typ != "room.invoke_tool":
                return {}
            return EmptyContent()

    room = _FakeMessagingRoom()
    client = MessagingClient(room=room)  # type: ignore[arg-type]
    removed_participants: list[RemoteParticipant] = []
    client.on(
        "participant_removed",
        lambda participant: removed_participants.append(participant),
    )

    await client.start()
    client._on_participant_enabled(
        RoomMessage(
            from_participant_id="remote-participant",
            type="participant.enabled",
            message={
                "id": "remote-participant",
                "role": "user",
                "attributes": {"name": "Remote User"},
            },
        )
    )

    participant = client.get_participant("remote-participant")
    assert participant is not None
    assert participant.online is True

    client.send_message_nowait(
        to=participant,
        type="direct",
        message={"value": 1},
    )
    client._on_participant_disabled(
        RoomMessage(
            from_participant_id="remote-participant",
            type="participant.disabled",
            message={"id": "remote-participant"},
        )
    )
    await client.stop()

    assert participant.online is False
    assert client.get_participant("remote-participant") is None
    assert removed_participants == [participant]
    assert room.requests == []


@pytest.mark.asyncio
async def test_messaging_client_marks_participant_offline_when_send_returns_not_found() -> (
    None
):
    class _FailingMessagingRoom(_FakeRoom):
        def __init__(self) -> None:
            super().__init__()
            self.local_participant = SimpleNamespace(id="local-participant")

        async def invoke(
            self, *, toolkit: str, tool: str, input: dict
        ) -> Content | AsyncIterator[Content]:
            del toolkit, tool, input
            raise RoomException(
                "the participant was not found", code=ErrorCode.NOT_FOUND
            )

    room = _FailingMessagingRoom()
    client = MessagingClient(room=room)  # type: ignore[arg-type]
    removed_participants: list[RemoteParticipant] = []
    client.on(
        "participant_removed",
        lambda participant: removed_participants.append(participant),
    )

    await client.start()
    client._on_participant_enabled(
        RoomMessage(
            from_participant_id="remote-participant",
            type="participant.enabled",
            message={
                "id": "remote-participant",
                "role": "user",
                "attributes": {"name": "Remote User"},
            },
        )
    )

    participant = client.get_participant("remote-participant")
    assert participant is not None
    assert participant.online is True

    client.send_message_nowait(
        to=participant,
        type="direct",
        message={"value": 1},
    )
    await client.stop()

    assert participant.online is False
    assert client.get_participant("remote-participant") is None
    assert removed_participants == [participant]
    assert room.requests == []


@pytest.mark.asyncio
async def test_secrets_client_uses_room_invoke_for_commands() -> None:
    class _FakeSecretsRoom(_FakeRoom):
        async def send_request(
            self, typ: str, request: dict, data: bytes | None = None
        ) -> Content | dict:
            self.requests.append((typ, request, data))
            if typ != "room.invoke_tool":
                return {}

            tool = request["tool"]
            if tool == "get_secret":
                return FileContent(
                    data=b"secret",
                    name="secret.txt",
                    mime_type="text/plain",
                )
            if tool == "request_secret":
                return FileContent(
                    data=b"delegated",
                    name="delegated.txt",
                    mime_type="text/plain",
                )
            if tool == "list_secrets":
                return JsonContent(
                    json={
                        "secrets": [
                            {
                                "id": "secret-1",
                                "type": "text/plain",
                                "name": "secret.txt",
                                "delegated_to": None,
                            }
                        ]
                    }
                )
            if tool == "request_oauth_token":
                return JsonContent(json={"access_token": "oauth-token"})
            if tool == "get_offline_oauth_token":
                return JsonContent(json={"access_token": "offline-token"})
            return EmptyContent()

    room = _FakeSecretsRoom()
    client = SecretsClient(room=room)  # type: ignore[arg-type]

    await client.provide_oauth_authorization(request_id="req-1", code="code-1")
    await client.reject_oauth_authorization(request_id="req-2", error="nope")
    await client.provide_secret(request_id="req-3", data=b"secret-bytes")
    await client.reject_secret(request_id="req-4", error="declined")
    assert (
        await client.get_offline_oauth_token(
            oauth=OAuthClientConfig(
                client_id="client-id",
                authorization_endpoint="https://example.com/authorize",
                token_endpoint="https://example.com/token",
            ),
            delegated_by="provider",
        )
        == "offline-token"
    )
    assert (
        await client.request_oauth_token(
            oauth=OAuthClientConfig(
                client_id="client-id",
                authorization_endpoint="https://example.com/authorize",
                token_endpoint="https://example.com/token",
            ),
            from_participant_id="provider-id",
            redirect_uri="http://localhost/callback",
        )
        == "oauth-token"
    )
    secrets = await client.list_secrets()
    assert len(secrets) == 1
    await client.delete_secret(id="secret-1")
    await client.delete_requested_secret(
        url="https://example.com/secret", type="text/plain"
    )
    assert (
        await client.request_secret(
            url="https://example.com/secret",
            type="text/plain",
            from_participant_id="provider-id",
        )
        == b"delegated"
    )
    await client.set_secret(secret_id="secret-1", data=b"payload")
    secret = await client.get_secret(secret_id="secret-1")
    assert isinstance(secret, FileContent)
    assert secret.data == b"secret"

    assert [request[0] for request in room.requests] == ["room.invoke_tool"] * 12
    assert [request[1]["toolkit"] for request in room.requests] == ["secrets"] * 12
    assert [request[1]["tool"] for request in room.requests] == [
        "provide_oauth_authorization",
        "provide_oauth_authorization",
        "provide_secret",
        "provide_secret",
        "get_offline_oauth_token",
        "request_oauth_token",
        "list_secrets",
        "delete_secret",
        "delete_requested_secret",
        "request_secret",
        "set_secret",
        "get_secret",
    ]

    provide_secret_arguments = room.requests[2][1]["arguments"]
    assert isinstance(provide_secret_arguments, dict)
    assert provide_secret_arguments == {
        "type": "binary",
        "headers": {"request_id": "req-3", "error": None},
    }
    assert room.requests[2][2] == b"secret-bytes"

    reject_secret_arguments = room.requests[3][1]["arguments"]
    assert isinstance(reject_secret_arguments, dict)
    assert reject_secret_arguments == {
        "type": "binary",
        "headers": {"request_id": "req-4", "error": "declined"},
    }
    assert room.requests[3][2] == b""

    set_secret_arguments = room.requests[10][1]["arguments"]
    assert isinstance(set_secret_arguments, dict)
    assert set_secret_arguments == {
        "type": "binary",
        "headers": {
            "secret_id": "secret-1",
            "type": None,
            "name": None,
            "delegated_to": None,
            "for_identity": None,
            "has_data": True,
        },
    }
    assert room.requests[10][2] == b"payload"


@pytest.mark.asyncio
async def test_storage_client_unexpected_response_uses_error_code() -> None:
    client = StorageClient(room=_BadStorageResponseRoom())  # type: ignore[arg-type]

    with pytest.raises(
        RoomException, match="unexpected return type from storage.exists"
    ) as ex:
        await client.exists(path="file.txt")

    assert ex.value.code == ErrorCode.UNEXPECTED_RESPONSE_TYPE


@pytest.mark.asyncio
async def test_database_client_unexpected_response_uses_error_code() -> None:
    client = DatabaseClient(room=_BadResponseRoom())  # type: ignore[arg-type]

    with pytest.raises(
        RoomException, match="unexpected return type from database.list_tables"
    ) as ex:
        await client.list_tables()

    assert ex.value.code == ErrorCode.UNEXPECTED_RESPONSE_TYPE


@pytest.mark.asyncio
async def test_database_client_uses_room_invoke_for_commands() -> None:
    def _rows_chunk(payload: list[dict[str, object]]) -> dict[str, object]:
        rows = []
        for row in payload:
            columns = []
            for key, value in row.items():
                if isinstance(value, bytes):
                    encoded_value = {
                        "type": "binary",
                        "data": base64.b64encode(value).decode(),
                    }
                elif isinstance(value, int):
                    encoded_value = {"type": "int", "value": value}
                else:
                    encoded_value = {"type": "text", "value": value}
                columns.append({"name": key, "value": encoded_value})
            rows.append({"columns": columns})
        return {"kind": "rows", "rows": rows}

    class _StreamingDatabaseRoom:
        def __init__(self) -> None:
            self.protocol = _FakeProtocol()
            self.calls: list[dict[str, object]] = []
            self.write_starts: dict[str, dict[str, object]] = {}
            self.write_chunks: dict[str, list[dict[str, object]]] = {
                "create_table": [],
                "insert": [],
                "merge": [],
            }
            self.read_starts: dict[str, dict[str, object]] = {}
            self.read_pulls: dict[str, list[dict[str, object]]] = {
                "search": [],
                "sql": [],
            }

        async def invoke(self, **kwargs) -> Content | AsyncIterator[Content]:
            self.calls.append(kwargs)
            tool = kwargs["tool"]
            tool_input = kwargs["input"]

            if tool in self.write_chunks:
                assert isinstance(tool_input, AsyncIterable)

                async def stream() -> AsyncIterator[Content]:
                    iterator = tool_input.__aiter__()
                    start_chunk = await iterator.__anext__()
                    assert isinstance(start_chunk, JsonContent)
                    self.write_starts[tool] = start_chunk.json
                    yield JsonContent(json={"kind": "pull"})
                    async for chunk in iterator:
                        assert isinstance(chunk, JsonContent)
                        self.write_chunks[tool].append(chunk.json)
                        yield JsonContent(json={"kind": "pull"})
                    yield _ControlContent(method="close")

                return stream()

            if tool in self.read_pulls:
                assert isinstance(tool_input, AsyncIterable)

                async def stream() -> AsyncIterator[Content]:
                    iterator = tool_input.__aiter__()
                    start_chunk = await iterator.__anext__()
                    assert isinstance(start_chunk, JsonContent)
                    self.read_starts[tool] = start_chunk.json
                    pull_count = 0
                    async for chunk in iterator:
                        assert isinstance(chunk, JsonContent)
                        self.read_pulls[tool].append(chunk.json)
                        pull_count += 1
                        if pull_count == 1:
                            if tool == "search":
                                yield JsonContent(
                                    json=_rows_chunk([{"payload": b"hello"}])
                                )
                            else:
                                yield JsonContent(
                                    json=_rows_chunk(
                                        [{"id": 1, "payload": b"sql-result"}]
                                    )
                                )
                            continue
                        yield _ControlContent(method="close")
                        return

                return stream()

            if tool == "list_tables":
                return JsonContent(json={"tables": ["records"]})
            if tool == "inspect":
                return JsonContent(
                    json={
                        "fields": [
                            {
                                "name": "annotations",
                                "data_type": {
                                    "type": "list",
                                    "nullable": None,
                                    "metadata": None,
                                    "element_type": {
                                        "type": "struct",
                                        "nullable": None,
                                        "metadata": None,
                                        "fields": [
                                            {
                                                "name": "key",
                                                "data_type": {
                                                    "type": "text",
                                                    "nullable": None,
                                                    "metadata": None,
                                                },
                                            },
                                            {
                                                "name": "value",
                                                "data_type": {
                                                    "type": "text",
                                                    "nullable": None,
                                                    "metadata": None,
                                                },
                                            },
                                        ],
                                    },
                                },
                            }
                        ],
                        "metadata": None,
                    }
                )
            if tool == "count":
                return JsonContent(json={"count": 1})
            if tool == "list_versions":
                return JsonContent(
                    json={
                        "versions": [
                            {
                                "version": 1,
                                "timestamp": "2025-01-01T00:00:00Z",
                                "metadata_json": json.dumps({"kind": "demo"}),
                            }
                        ]
                    }
                )
            if tool == "list_indexes":
                return JsonContent(
                    json={
                        "indexes": [
                            {
                                "name": "idx_records_id",
                                "columns": ["id"],
                                "type": "btree",
                            }
                        ]
                    }
                )
            return EmptyContent()

    room = _StreamingDatabaseRoom()
    client = DatabaseClient(room=room)  # type: ignore[arg-type]

    await client.create_table_with_schema(
        name="records",
        namespace=["team"],
        data=[{"payload": b"hello"}],
        schema={
            "annotations": ListDataType(
                element_type=StructDataType(
                    fields={
                        "key": TextDataType(),
                        "value": TextDataType(),
                    }
                )
            )
        },
        metadata={"kind": "demo"},
    )
    await client.insert(
        table="records",
        namespace=["team"],
        records=[{"payload": b"inserted"}],
    )
    await client.merge(
        table="records",
        namespace=["team"],
        on="id",
        records=[{"id": 1, "payload": b"merged"}],
    )
    await client.update(
        table="records",
        where="id = 1",
        values={"payload": b"hello"},
    )
    tables = await client.list_tables(namespace=["team"])
    inspected = await client.inspect(table="records", namespace=["team"])
    rows = await client.search(table="records", namespace=["team"])
    sql_rows = await client.sql(
        query="SELECT * FROM records",
        tables=["records"],
    )
    count = await client.count(table="records", namespace=["team"])
    versions = await client.list_versions(table="records", namespace=["team"])
    indexes = await client.list_indexes(table="records", namespace=["team"])

    assert tables == ["records"]
    assert isinstance(inspected["annotations"], ListDataType)
    assert isinstance(inspected["annotations"].element_type, StructDataType)
    assert rows == [{"payload": b"hello"}]
    assert sql_rows == [{"id": 1, "payload": b"sql-result"}]
    assert count == 1
    assert versions[0].metadata == {"kind": "demo"}
    assert indexes[0].name == "idx_records_id"

    assert [call["tool"] for call in room.calls] == [
        "create_table",
        "insert",
        "merge",
        "update",
        "list_tables",
        "inspect",
        "search",
        "sql",
        "count",
        "list_versions",
        "list_indexes",
    ]
    assert all(call["toolkit"] == "database" for call in room.calls)

    create_start = room.write_starts["create_table"]
    assert create_start["kind"] == "start"
    assert create_start["namespace"] == ["team"]
    assert create_start["metadata"] == [{"key": "kind", "value": "demo"}]
    assert create_start["fields"][0]["name"] == "annotations"
    assert create_start["fields"][0]["data_type"]["type"] == "list"
    assert create_start["fields"][0]["data_type"]["element_type"]["type"] == "struct"
    assert create_start["fields"][0]["data_type"]["element_type"]["fields"] == [
        {
            "name": "key",
            "data_type": {"type": "text", "nullable": None, "metadata": None},
        },
        {
            "name": "value",
            "data_type": {"type": "text", "nullable": None, "metadata": None},
        },
    ]
    assert room.write_chunks["create_table"] == [_rows_chunk([{"payload": b"hello"}])]

    assert room.write_starts["insert"] == {
        "kind": "start",
        "table": "records",
        "namespace": ["team"],
    }
    assert room.write_chunks["insert"] == [_rows_chunk([{"payload": b"inserted"}])]

    assert room.write_starts["merge"] == {
        "kind": "start",
        "table": "records",
        "on": "id",
        "namespace": ["team"],
    }
    assert room.write_chunks["merge"] == [
        _rows_chunk([{"id": 1, "payload": b"merged"}])
    ]

    update_arguments = room.calls[3]["input"]
    assert update_arguments["values"] == [
        {
            "column": "payload",
            "value_json": json.dumps(
                {
                    "encoding": "base64",
                    "data": base64.b64encode(b"hello").decode(),
                }
            ),
        }
    ]
    assert update_arguments["values_sql"] is None
    assert room.read_starts["search"]["kind"] == "start"
    assert room.read_starts["search"]["table"] == "records"
    assert room.read_pulls["search"] == [{"kind": "pull"}, {"kind": "pull"}]
    assert room.read_starts["sql"]["kind"] == "start"
    assert room.read_starts["sql"]["query"] == "SELECT * FROM records"
    assert room.read_pulls["sql"] == [{"kind": "pull"}, {"kind": "pull"}]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "invoke"),
    [
        ("stat", lambda client: client.stat(path="file.txt")),
        ("upload", lambda client: client.upload(path="file.txt", data=b"test")),
        ("download", lambda client: client.download(path="file.txt")),
        ("download_url", lambda client: client.download_url(path="file.txt")),
        ("list", lambda client: client.list(path="folder")),
        ("delete", lambda client: client.delete("file.txt")),
    ],
)
async def test_storage_client_all_methods_use_unexpected_response_error_code(
    operation: str,
    invoke,
) -> None:
    client = StorageClient(room=_BadStorageResponseRoom())  # type: ignore[arg-type]

    with pytest.raises(
        RoomException, match=f"unexpected return type from storage.{operation}"
    ) as ex:
        await invoke(client)

    assert ex.value.code == ErrorCode.UNEXPECTED_RESPONSE_TYPE


@pytest.mark.asyncio
async def test_storage_client_streams_write_and_download() -> None:
    room = _StreamingStorageRoom()
    client = StorageClient(room=room)  # type: ignore[arg-type]

    payload = b"a" * ((64 * 1024) + 17)
    await client.upload(path="file.txt", data=payload)
    downloaded = await client.download(path="file.txt")

    assert downloaded.data == b"hello world"
    assert downloaded.name == "file.txt"
    assert downloaded.mime_type == "text/plain"

    assert room.requests[0]["toolkit"] == "storage"
    assert room.requests[0]["tool"] == "upload"
    assert room.upload_starts[0].headers == {
        "kind": "start",
        "path": "file.txt",
        "overwrite": False,
        "name": "file.txt",
        "mime_type": "text/plain",
        "size": len(payload),
    }
    assert len(room.upload_chunks) == 1
    assert b"".join(chunk.data for chunk in room.upload_chunks) == payload
    assert all(chunk.headers == {"kind": "data"} for chunk in room.upload_chunks)

    assert room.requests[1]["tool"] == "download"
    assert len(room.download_starts) == 1
    assert room.download_starts[0].headers == {
        "kind": "start",
        "path": "file.txt",
        "chunk_size": 64 * 1024,
    }
    assert [chunk.headers for chunk in room.download_pulls] == [
        {"kind": "pull"},
        {"kind": "pull"},
    ]


@pytest.mark.asyncio
async def test_storage_client_upload_stream_falls_back_to_default_chunk_size() -> None:
    room = _StreamingStorageRoom(upload_pull_chunk_size=None)
    client = StorageClient(room=room)  # type: ignore[arg-type]

    payload = b"a" * ((64 * 1024) + 17)
    await client.upload(path="file.txt", data=payload)

    assert len(room.upload_chunks) == 2
    assert b"".join(chunk.data for chunk in room.upload_chunks) == payload


@pytest.mark.asyncio
async def test_developer_client_uses_room_invoke_for_commands() -> None:
    room = _DeveloperLogRoom()
    client = DeveloperClient(room=room)  # type: ignore[arg-type]

    await client.log(type="custom", data={"hello": "world"})
    stream = client.logs()
    next_task = asyncio.create_task(stream.__anext__())
    invoke_request = None
    while invoke_request is None:
        await asyncio.sleep(0)
        invoke_request = next(
            (
                request
                for request in room.requests
                if request[0] == "room.invoke_tool" and request[1].get("tool") == "logs"
            ),
            None,
        )
    next_task.cancel()
    await asyncio.gather(next_task, return_exceptions=True)
    await asyncio.sleep(0)
    await _cancel_close_watcher(room)

    assert room.requests[0][0] == "room.invoke_tool"
    assert room.requests[0][1]["arguments"]["json"] == {
        "type": "custom",
        "data": {"hello": "world"},
    }
    assert invoke_request is not None
    assert invoke_request[1]["toolkit"] == "developer"
    assert invoke_request[1]["tool"] == "logs"
    assert invoke_request[1]["arguments"] == {
        "type": "control",
        "method": "open",
    }
    chunk_requests = [
        request
        for request in room.requests
        if request[0] == "room.tool_call_request_chunk"
    ]
    assert len(chunk_requests) == 1
    close_chunk = chunk_requests[0][1]["chunk"]
    assert close_chunk == {
        "type": "control",
        "method": "close",
        "status_code": ControlCloseStatus.NORMAL,
    }


@pytest.mark.asyncio
async def test_developer_client_emits_streamed_log_events() -> None:
    room = _DeveloperLogRoom()
    client = DeveloperClient(room=room)  # type: ignore[arg-type]
    stream = client.logs()
    next_task = asyncio.create_task(stream.__anext__())
    while not room.requests:
        await asyncio.sleep(0)

    invoke_request = next(
        request for request in room.requests if request[0] == "room.invoke_tool"
    )
    tool_call_id = invoke_request[1]["tool_call_id"]
    await room._handle_tool_call_response_chunk(
        protocol=room.protocol,  # type: ignore[arg-type]
        message_id=1,
        typ="room.tool_call_response_chunk",
        data=pack_message(
            header={
                "tool_call_id": tool_call_id,
                "chunk": BinaryContent(
                    data=json.dumps({"hello": "world"}).encode("utf-8"),
                    headers={"type": "custom"},
                ).to_json(),
            },
            data=json.dumps({"hello": "world"}).encode("utf-8"),
        ),
    )

    event = await next_task
    assert event.type == "custom"
    assert event.data == {"hello": "world"}

    await stream.aclose()
    await asyncio.sleep(0)
    await _cancel_close_watcher(room)


@pytest.mark.asyncio
async def test_services_client_unexpected_response_uses_error_code() -> None:
    client = ServicesClient(room=_BadResponseRoom())  # type: ignore[arg-type]

    with pytest.raises(
        RoomException, match="unexpected return type from services.list"
    ) as ex:
        await client.list_with_state()

    assert ex.value.code == ErrorCode.UNEXPECTED_RESPONSE_TYPE


@pytest.mark.asyncio
async def test_services_client_uses_room_invoke_and_translates_service_states() -> None:
    class _InvokeRoom:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        async def invoke(self, **kwargs) -> JsonContent:
            self.calls.append(kwargs)
            return JsonContent(
                json={
                    "services_json": [
                        json.dumps(
                            {
                                "kind": "Service",
                                "version": "v1",
                                "id": "svc-1",
                                "metadata": {"name": "svc-1"},
                                "container": {"image": "meshagent/cli:default"},
                                "ports": [],
                            }
                        )
                    ],
                    "service_states": [
                        {
                            "service_id": "svc-1",
                            "state": "running",
                            "container_id": "container-123",
                            "restart_scheduled_at": None,
                            "started_at": 123.0,
                            "restart_count": 2,
                            "last_exit_code": 137,
                            "last_exit_at": 122.0,
                        }
                    ],
                }
            )

    room = _InvokeRoom()
    client = ServicesClient(room=room)  # type: ignore[arg-type]

    result = await client.list_with_state()

    assert room.calls == [
        {
            "toolkit": "services",
            "tool": "list",
            "input": {},
        }
    ]
    assert len(result.services) == 1
    assert result.services[0].id == "svc-1"
    assert result.service_states["svc-1"].state == "running"
    assert result.service_states["svc-1"].container_id == "container-123"


@pytest.mark.asyncio
async def test_services_client_restart_uses_room_invoke() -> None:
    class _InvokeRoom:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        async def invoke(self, **kwargs) -> EmptyContent:
            self.calls.append(kwargs)
            return EmptyContent()

    room = _InvokeRoom()
    client = ServicesClient(room=room)  # type: ignore[arg-type]

    await client.restart(service_id="svc-1")

    assert room.calls == [
        {
            "toolkit": "services",
            "tool": "restart",
            "input": {"service_id": "svc-1"},
        }
    ]


@pytest.mark.asyncio
async def test_livekit_client_uses_room_invoke() -> None:
    class _InvokeRoom:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        async def invoke(self, **kwargs) -> JsonContent:
            self.calls.append(kwargs)
            return JsonContent(
                json={"url": "wss://livekit.example", "token": "jwt-token"}
            )

    room = _InvokeRoom()
    client = LivekitClient(room=room)  # type: ignore[arg-type]

    result = await client.get_connection_info(breakout_room="demo")

    assert room.calls == [
        {
            "toolkit": "livekit",
            "tool": "connect",
            "input": {"breakout_room": "demo"},
        }
    ]
    assert result.url == "wss://livekit.example"
    assert result.token == "jwt-token"


@pytest.mark.asyncio
async def test_livekit_client_unexpected_response_uses_error_code() -> None:
    client = LivekitClient(room=_BadResponseRoom())  # type: ignore[arg-type]

    with pytest.raises(
        RoomException, match="unexpected return type from livekit.connect"
    ) as ex:
        await client.get_connection_info()

    assert ex.value.code == ErrorCode.UNEXPECTED_RESPONSE_TYPE


@pytest.mark.asyncio
async def test_queues_client_list_unexpected_response_uses_error_code() -> None:
    client = QueuesClient(room=_BadResponseRoom())  # type: ignore[arg-type]

    with pytest.raises(
        RoomException, match="unexpected return type from queues.list"
    ) as ex:
        await client.list(name="jobs", message={})

    assert ex.value.code == ErrorCode.UNEXPECTED_RESPONSE_TYPE


@pytest.mark.asyncio
async def test_queues_client_receive_unexpected_response_uses_error_code() -> None:
    client = QueuesClient(room=_BadResponseRoom())  # type: ignore[arg-type]

    with pytest.raises(
        RoomException, match="unexpected return type from queues.receive"
    ) as ex:
        await client.receive(name="jobs")

    assert ex.value.code == ErrorCode.UNEXPECTED_RESPONSE_TYPE


@pytest.mark.asyncio
async def test_containers_client_uses_room_invoke_with_strict_payloads() -> None:
    class _FakeContainersRoom:
        def __init__(self) -> None:
            self.requests: list[dict[str, object]] = []

        async def invoke(self, **kwargs) -> Content | AsyncIterator[Content]:
            self.requests.append(kwargs)
            tool = kwargs["tool"]
            if tool in {
                "run",
                "push_image",
                "load_image",
                "save_image",
                "run_service",
            }:
                return JsonContent(json={"container_id": f"{tool}-ctr"})
            if tool in {"build", "start_build"}:
                return JsonContent(json={"build_id": f"{tool}-job"})
            if tool == "list_images":
                return JsonContent(
                    json={
                        "images": [
                            {
                                "id": "img-1",
                                "tags": ["demo:latest"],
                                "size": 1,
                                "labels": {},
                            }
                        ]
                    }
                )
            if tool == "list_containers":
                return JsonContent(
                    json={
                        "containers": [
                            {
                                "id": "container-1",
                                "started_by": {"id": "p1", "name": "user"},
                                "state": "RUNNING",
                                "private": False,
                            }
                        ]
                    }
                )
            if tool == "list_builds":
                return JsonContent(
                    json={
                        "builds": [
                            {
                                "id": "build-1",
                                "tag": "example:latest",
                                "status": "running",
                                "exit_code": None,
                            }
                        ]
                    }
                )
            return EmptyContent()

    room = _FakeContainersRoom()
    client = ContainersClient(room=room)  # type: ignore[arg-type]

    await client.pull_image(
        tag="demo:latest", credentials=[DockerSecret(username="u", password="p")]
    )
    await client.run(
        image="demo:latest",
        env={"KEY": "VALUE"},
        ports={8080: 80},
        mounts=ContainerMountSpec(
            room=[RoomStorageMountSpec(path="/workspace", read_only=False)],
            configs=[ConfigMountSpec()],
            empty_dirs=[EmptyDirMountSpec(path="/cache")],
        ),
    )
    await client.build(
        tag="example:latest",
        mounts=[
            ContainerMountSpec(
                room=[RoomStorageMountSpec(path="/workspace", read_only=False)]
            )
        ],
        context_path="/workspace",
    )
    await client.start_build(
        tag="example:latest",
        mounts=[
            ContainerMountSpec(
                room=[RoomStorageMountSpec(path="/workspace", read_only=False)]
            )
        ],
        context_path="/workspace",
        context_archive_path="/website",
        context_archive_ref="room.meshagent.com/website:latest",
        context_archive_mount_path="/context",
        context_archive_arch="amd64",
    )
    await client.run_service(service_id="svc-1", env={"A": "1"})
    await client.list_images()
    await client.list_builds()
    await client.cancel_build(build_id="build-1")
    await client.delete_build(build_id="build-1")
    await client.list()

    assert [request["tool"] for request in room.requests] == [
        "pull_image",
        "run",
        "build",
        "start_build",
        "run_service",
        "list_images",
        "list_builds",
        "cancel_build",
        "delete_build",
        "list_containers",
    ]

    pull_input = room.requests[0]["input"]
    assert isinstance(pull_input, dict)
    assert pull_input["credentials"] == [
        {"registry": None, "username": "u", "password": "p"}
    ]

    run_input = room.requests[1]["input"]
    assert isinstance(run_input, dict)
    assert run_input["env"] == [{"key": "KEY", "value": "VALUE"}]
    assert run_input["ports"] == [{"container_port": 8080, "host_port": 80}]
    assert isinstance(run_input["mounts"], dict)
    assert run_input["mounts"]["configs"] == [{"path": "/var/run/meshagent"}]
    assert run_input["mounts"]["empty_dirs"] == [{"path": "/cache", "read_only": False}]

    build_input = room.requests[2]["input"]
    assert isinstance(build_input, dict)
    assert build_input["context_archive_path"] is None

    start_build_input = room.requests[3]["input"]
    assert isinstance(start_build_input, dict)
    assert start_build_input["context_archive_path"] == "/website"
    assert (
        start_build_input["context_archive_ref"] == "room.meshagent.com/website:latest"
    )
    assert start_build_input["context_archive_mount_path"] == "/context"
    assert start_build_input["context_archive_arch"] == "amd64"

    run_service_input = room.requests[4]["input"]
    assert isinstance(run_service_input, dict)
    assert run_service_input["env"] == [{"key": "A", "value": "1"}]


@pytest.mark.asyncio
async def test_containers_client_exec_and_logs_use_streamed_invoke() -> None:
    class _FakeContainersRoom:
        def __init__(self) -> None:
            self.exec_chunks: list[BinaryContent] = []
            self.log_requests: list[dict[str, object]] = []
            self.build_log_requests: list[dict[str, object]] = []

        async def invoke(self, **kwargs) -> Content | AsyncIterator[Content]:
            tool = kwargs["tool"]
            input_value = kwargs["input"]

            if tool == "exec":
                assert isinstance(input_value, AsyncIterable)

                async def exec_stream() -> AsyncIterator[Content]:
                    iterator = input_value.__aiter__()
                    start_chunk = await iterator.__anext__()
                    assert isinstance(start_chunk, BinaryContent)
                    self.exec_chunks.append(start_chunk)
                    data_chunk = await iterator.__anext__()
                    assert isinstance(data_chunk, BinaryContent)
                    self.exec_chunks.append(data_chunk)
                    yield BinaryContent(
                        data=b"hello",
                        headers={
                            "request_id": start_chunk.headers["request_id"],
                            "container_id": start_chunk.headers["container_id"],
                            "channel": 1,
                        },
                    )
                    yield BinaryContent(
                        data=b'{"status": 0}',
                        headers={
                            "request_id": start_chunk.headers["request_id"],
                            "container_id": start_chunk.headers["container_id"],
                            "channel": 3,
                        },
                    )

                return exec_stream()

            if tool == "logs":
                assert isinstance(input_value, AsyncIterable)

                async def log_stream() -> AsyncIterator[Content]:
                    iterator = input_value.__aiter__()
                    start_chunk = await iterator.__anext__()
                    assert isinstance(start_chunk, BinaryContent)
                    self.log_requests.append(dict(start_chunk.headers))
                    yield BinaryContent(
                        data=b"line 1",
                        headers={
                            "request_id": start_chunk.headers["request_id"],
                            "container_id": start_chunk.headers["container_id"],
                            "channel": 1,
                        },
                    )
                    yield BinaryContent(
                        data=b"line 2",
                        headers={
                            "request_id": start_chunk.headers["request_id"],
                            "container_id": start_chunk.headers["container_id"],
                            "channel": 1,
                        },
                    )
                    yield _ControlContent(method="close")

                return log_stream()

            if tool == "get_build_logs":
                assert isinstance(input_value, AsyncIterable)

                async def build_log_stream() -> AsyncIterator[Content]:
                    iterator = input_value.__aiter__()
                    start_chunk = await iterator.__anext__()
                    assert isinstance(start_chunk, BinaryContent)
                    self.build_log_requests.append(dict(start_chunk.headers))
                    yield BinaryContent(
                        data=b"build line",
                        headers={
                            "request_id": start_chunk.headers["request_id"],
                            "build_id": start_chunk.headers["build_id"],
                            "channel": 1,
                        },
                    )
                    yield BinaryContent(
                        data=b'{"status": 0}',
                        headers={
                            "request_id": start_chunk.headers["request_id"],
                            "build_id": start_chunk.headers["build_id"],
                            "channel": 3,
                        },
                    )

                return build_log_stream()

            return EmptyContent()

    room = _FakeContainersRoom()
    client = ContainersClient(room=room)  # type: ignore[arg-type]

    session = await client.exec(container_id="container-1", command="echo hi")
    await session.wait_for_ready()
    await session.write(b"ping")
    stdout = [chunk async for chunk in session.stdout()]
    status = await session.result

    assert stdout == [b"hello"]
    assert status == 0
    assert room.exec_chunks[0].headers["kind"] == "start"
    assert room.exec_chunks[0].headers["container_id"] == "container-1"
    assert room.exec_chunks[1].headers["channel"] == 1
    assert room.exec_chunks[1].data == b"ping"

    log_stream = client.logs(container_id="container-1", follow=False)
    logs = [line async for line in log_stream.logs()]
    await log_stream
    assert logs == ["line 1", "line 2"]
    assert len(room.log_requests) == 1
    assert room.log_requests[0]["container_id"] == "container-1"
    assert room.log_requests[0]["follow"] is False
    assert room.log_requests[0]["kind"] == "start"

    build_log_stream = client.get_build_logs(build_id="build-1", follow=True)
    build_logs = [line async for line in build_log_stream.logs()]
    build_status = await build_log_stream
    assert build_logs == ["build line"]
    assert build_status == 0
    assert len(room.build_log_requests) == 1
    assert room.build_log_requests[0]["build_id"] == "build-1"
    assert room.build_log_requests[0]["follow"] is True
    assert room.build_log_requests[0]["kind"] == "start"


@pytest.mark.asyncio
async def test_containers_client_run_unexpected_response_uses_error_code() -> None:
    client = ContainersClient(room=_BadStorageResponseRoom())  # type: ignore[arg-type]

    with pytest.raises(
        RoomException, match="unexpected return type from containers.run"
    ) as ex:
        await client.run(image="alpine:latest")

    assert ex.value.code == ErrorCode.UNEXPECTED_RESPONSE_TYPE


@pytest.mark.asyncio
async def test_containers_client_list_unexpected_response_uses_error_code() -> None:
    client = ContainersClient(room=_BadStorageResponseRoom())  # type: ignore[arg-type]

    with pytest.raises(
        RoomException, match="unexpected return type from containers.list"
    ) as ex:
        await client.list()

    assert ex.value.code == ErrorCode.UNEXPECTED_RESPONSE_TYPE


@pytest.mark.asyncio
async def test_containers_client_build_unexpected_response_uses_error_code() -> None:
    client = ContainersClient(room=_BadStorageResponseRoom())  # type: ignore[arg-type]

    with pytest.raises(
        RoomException, match="unexpected return type from containers.build"
    ) as ex:
        await client.build(
            tag="example:latest",
            mounts=[
                ContainerMountSpec(
                    room=[RoomStorageMountSpec(path="/workspace", read_only=False)]
                )
            ],
            context_path="/workspace",
        )

    assert ex.value.code == ErrorCode.UNEXPECTED_RESPONSE_TYPE


@pytest.mark.asyncio
async def test_containers_client_push_image_unexpected_response_uses_error_code() -> (
    None
):
    client = ContainersClient(room=_BadStorageResponseRoom())  # type: ignore[arg-type]

    with pytest.raises(
        RoomException, match="unexpected return type from containers.push_image"
    ) as ex:
        await client.push_image(tag="example:latest")

    assert ex.value.code == ErrorCode.UNEXPECTED_RESPONSE_TYPE


@pytest.mark.asyncio
async def test_containers_client_load_uses_room_invoke_with_strict_payload() -> None:
    class _FakeContainersRoom:
        def __init__(self) -> None:
            self.requests: list[dict[str, object]] = []

        async def invoke(self, **kwargs) -> Content | AsyncIterator[Content]:
            self.requests.append(kwargs)
            return JsonContent(
                json={
                    "resolved_ref": "room.meshagent.com/images/example.tar:latest",
                    "refs": ["room.meshagent.com/images/example.tar:latest"],
                }
            )

    room = _FakeContainersRoom()
    client = ContainersClient(room=room)  # type: ignore[arg-type]

    loaded = await client.load(archive_path="/images/example.tar")

    assert loaded.resolved_ref == "room.meshagent.com/images/example.tar:latest"
    assert loaded.refs == ["room.meshagent.com/images/example.tar:latest"]
    assert room.requests == [
        {
            "toolkit": "containers",
            "tool": "load",
            "input": {"archive_path": "/images/example.tar"},
        }
    ]


@pytest.mark.asyncio
async def test_containers_client_load_unexpected_response_uses_error_code() -> None:
    client = ContainersClient(room=_BadStorageResponseRoom())  # type: ignore[arg-type]

    with pytest.raises(
        RoomException, match="unexpected return type from containers.load"
    ) as ex:
        await client.load(archive_path="/images/example.tar")

    assert ex.value.code == ErrorCode.UNEXPECTED_RESPONSE_TYPE


@pytest.mark.asyncio
async def test_containers_client_load_image_unexpected_response_uses_error_code() -> (
    None
):
    client = ContainersClient(room=_BadStorageResponseRoom())  # type: ignore[arg-type]

    with pytest.raises(
        RoomException, match="unexpected return type from containers.load_image"
    ) as ex:
        await client.load_image(
            mounts=[
                ContainerMountSpec(
                    room=[RoomStorageMountSpec(path="/workspace", read_only=False)]
                )
            ],
            archive_path="/workspace/image.tar",
        )

    assert ex.value.code == ErrorCode.UNEXPECTED_RESPONSE_TYPE


@pytest.mark.asyncio
async def test_containers_client_save_image_unexpected_response_uses_error_code() -> (
    None
):
    client = ContainersClient(room=_BadStorageResponseRoom())  # type: ignore[arg-type]

    with pytest.raises(
        RoomException, match="unexpected return type from containers.save_image"
    ) as ex:
        await client.save_image(
            tag="example:latest",
            mounts=[
                ContainerMountSpec(
                    room=[RoomStorageMountSpec(path="/workspace", read_only=False)]
                )
            ],
            archive_path="/workspace/image.tar",
        )

    assert ex.value.code == ErrorCode.UNEXPECTED_RESPONSE_TYPE


@pytest.mark.asyncio
async def test_tool_call_response_chunk_unpacks_file_chunk_payload() -> None:
    room = _FakeRoom()
    chunk = FileContent(
        name="step.png",
        mime_type="image/png",
        data=b"\x89PNG\r\n\x1a\n",
    )

    await room._handle_tool_call_response_chunk(
        protocol=room.protocol,  # type: ignore[arg-type]
        message_id=1,
        typ="room.tool_call_response_chunk",
        data=pack_message(
            header={"tool_call_id": "tc-2", "chunk": chunk.to_json()},
            data=chunk.get_data(),
        ),
    )

    assert len(room.events) == 1
    assert room.events[0][0] == "room.tool_call_response_chunk"
    event = room.events[0][1]["event"]
    assert isinstance(event, dict)
    assert event["tool_call_id"] == "tc-2"
    chunk = event["chunk"]
    assert isinstance(chunk, FileContent)
    assert chunk.name == "step.png"
    assert chunk.mime_type == "image/png"
    assert chunk.data == b"\x89PNG\r\n\x1a\n"


@pytest.mark.asyncio
async def test_tool_call_response_chunk_keeps_non_chunk_dict_payload() -> None:
    room = _FakeRoom()
    payload = {
        "type": "agent.event",
        "headline": "waiting for page",
        "state": "in_progress",
    }

    await room._handle_tool_call_response_chunk(
        protocol=room.protocol,  # type: ignore[arg-type]
        message_id=1,
        typ="room.tool_call_response_chunk",
        data=pack_message(
            header={"tool_call_id": "tc-3", "chunk": payload},
            data=b"ignored",
        ),
    )

    assert len(room.events) == 1
    assert room.events[0][0] == "room.tool_call_response_chunk"
    event = room.events[0][1]["event"]
    assert isinstance(event, dict)
    assert event["tool_call_id"] == "tc-3"
    assert event["chunk"] == payload


@pytest.mark.asyncio
async def test_invoke_tool_sends_control_chunks_for_request_stream() -> None:
    room = _FakeRoom()
    client = AgentsClient(room=room)  # type: ignore[arg-type]

    async def request_stream():
        yield JsonContent(json={"step": 1})
        yield TextContent(text="done")

    response = await client.invoke_tool(
        toolkit="test-toolkit",
        tool="streaming-tool",
        input=request_stream(),
    )
    assert isinstance(response, JsonContent)
    assert response.json == {"ok": True}

    assert room.requests[0][0] == "room.invoke_tool"
    invoke_request = room.requests[0][1]
    assert isinstance(invoke_request["tool_call_id"], str)
    assert invoke_request["tool_call_id"] != ""
    assert invoke_request["arguments"] == _ControlContent(method="open").to_json()
    assert "stream" not in invoke_request
    assert "input" not in invoke_request

    request_chunks = [
        request
        for request in room.requests
        if request[0] == "room.tool_call_request_chunk"
    ]
    assert len(request_chunks) == 3

    first_payload_chunk = request_chunks[0][1]["chunk"]
    assert first_payload_chunk["type"] == "json"
    assert first_payload_chunk["json"] == {"step": 1}

    second_payload_chunk = request_chunks[1][1]["chunk"]
    assert second_payload_chunk["type"] == "text"
    assert second_payload_chunk["text"] == "done"

    close_chunk = request_chunks[2][1]["chunk"]
    assert close_chunk["type"] == "control"
    assert close_chunk["method"] == "close"


@pytest.mark.asyncio
async def test_invoke_tool_rejects_non_content_stream_items() -> None:
    room = _FakeRoom()
    client = AgentsClient(room=room)  # type: ignore[arg-type]

    async def input_stream():
        yield {"step": 1}

    with pytest.raises(
        RoomException,
        match="invoke_tool input stream items must be Content values",
    ):
        await client.invoke_tool(
            toolkit="test-toolkit",
            tool="streaming-tool",
            input=input_stream(),
        )


@pytest.mark.asyncio
async def test_invoke_tool_does_not_send_stream_flag() -> None:
    room = _FakeRoom()
    client = AgentsClient(room=room)  # type: ignore[arg-type]

    response = await client.invoke_tool(
        toolkit="test-toolkit",
        tool="streaming-tool",
        input={"a": 1},
    )

    assert isinstance(response, JsonContent)
    assert response.json == {"ok": True}
    assert room.requests[0][0] == "room.invoke_tool"
    assert "stream" not in room.requests[0][1]


@pytest.mark.asyncio
async def test_invoke_tool_upgrades_dict_input_to_json_content() -> None:
    room = _FakeRoom()
    client = AgentsClient(room=room)  # type: ignore[arg-type]

    response = await client.invoke_tool(
        toolkit="test-toolkit",
        tool="streaming-tool",
        input={"a": 1},
    )

    assert isinstance(response, JsonContent)
    assert response.json == {"ok": True}
    request = room.requests[0]
    assert request[0] == "room.invoke_tool"
    assert request[1]["arguments"] == {"type": "json", "json": {"a": 1}}
    assert request[2] is None


@pytest.mark.asyncio
async def test_invoke_tool_upgrades_str_input_to_text_content() -> None:
    room = _FakeRoom()
    client = AgentsClient(room=room)  # type: ignore[arg-type]

    response = await client.invoke_tool(
        toolkit="test-toolkit",
        tool="streaming-tool",
        input="hello",
    )

    assert isinstance(response, JsonContent)
    assert response.json == {"ok": True}
    request = room.requests[0]
    assert request[0] == "room.invoke_tool"
    assert request[1]["arguments"] == {"type": "text", "text": "hello"}
    assert request[2] is None


@pytest.mark.asyncio
async def test_invoke_tool_rejects_attachment_keyword() -> None:
    room = _FakeRoom()
    client = AgentsClient(room=room)  # type: ignore[arg-type]

    with pytest.raises(
        TypeError,
        match="invoke_tool\\(\\) got unexpected keyword argument\\(s\\): attachment",
    ):
        await client.invoke_tool(
            toolkit="test-toolkit",
            tool="streaming-tool",
            input={"a": 1},
            attachment=b"bytes",
        )


class _OpenResponseRoom(_FakeRoom):
    async def send_request(
        self, typ: str, request: dict, data: bytes | None = None
    ) -> JsonContent | dict:
        self.requests.append((typ, request, data))
        if typ == "room.invoke_tool":
            await asyncio.sleep(0)
            return _ControlContent(method="open")
        return {}


class _OpenResponseBlockingChunkRoom(_FakeRoom):
    def __init__(self) -> None:
        super().__init__()
        self.chunk_send_started = asyncio.Event()
        self.chunk_send_cancelled = asyncio.Event()
        self.chunk_send_count = 0

    async def send_request(
        self, typ: str, request: dict, data: bytes | None = None
    ) -> JsonContent | dict:
        self.requests.append((typ, request, data))
        if typ == "room.invoke_tool":
            await asyncio.sleep(0)
            return _ControlContent(method="open")
        if typ == "room.tool_call_request_chunk":
            self.chunk_send_count += 1
            if self.chunk_send_count == 1:
                self.chunk_send_started.set()
                try:
                    await asyncio.Future()
                except asyncio.CancelledError:
                    self.chunk_send_cancelled.set()
                    raise
            return {}
        return {}


class _DeveloperLogRoom(_OpenResponseRoom):
    def __init__(self) -> None:
        super().__init__()

    async def send_request(
        self, typ: str, request: dict, data: bytes | None = None
    ) -> JsonContent | dict:
        response = await super().send_request(typ, request, data)
        if (
            typ == "room.tool_call_request_chunk"
            and request.get("chunk") == _ControlContent(method="close").to_json()
        ):
            await self._handle_tool_call_response_chunk(
                protocol=self.protocol,  # type: ignore[arg-type]
                message_id=1,
                typ="room.tool_call_response_chunk",
                data=pack_message(
                    header={
                        "tool_call_id": request["tool_call_id"],
                        "chunk": _ControlContent(method="close").to_json(),
                    }
                ),
            )
        return response


async def _cancel_close_watcher(room: _FakeRoom) -> None:
    close_watcher = room._close_watcher_task
    if close_watcher is not None:
        close_watcher.cancel()
        await asyncio.gather(close_watcher, return_exceptions=True)


@pytest.mark.asyncio
async def test_invoke_tool_returns_stream_when_response_is_open_control_chunk() -> None:
    room = _OpenResponseRoom()
    client = AgentsClient(room=room)  # type: ignore[arg-type]

    response = await client.invoke_tool(
        toolkit="test-toolkit",
        tool="streaming-tool",
        input={"a": 1},
    )
    assert not isinstance(response, Content)
    assert isinstance(response, AsyncIterator)
    client._fail_tool_call_streams(error=RoomException("test cleanup"))


@pytest.mark.asyncio
async def test_invoke_tool_returns_stream_for_content_input_when_response_is_open_control_chunk() -> (
    None
):
    room = _OpenResponseRoom()
    client = AgentsClient(room=room)  # type: ignore[arg-type]

    response = await client.invoke_tool(
        toolkit="test-toolkit",
        tool="streaming-tool",
        input=JsonContent(json={"a": 1}),
    )
    assert not isinstance(response, Content)
    assert isinstance(response, AsyncIterator)
    client._fail_tool_call_streams(error=RoomException("test cleanup"))


@pytest.mark.asyncio
async def test_invoke_tool_stream_close_cancels_request_stream_task() -> None:
    room = _OpenResponseBlockingChunkRoom()
    client = AgentsClient(room=room)  # type: ignore[arg-type]

    async def request_stream() -> AsyncIterator[Content]:
        yield TextContent(text="step 1")
        await asyncio.Future()

    response = await client.invoke_tool(
        toolkit="test-toolkit",
        tool="streaming-tool",
        input=request_stream(),
    )
    assert isinstance(response, AsyncIterator)
    await asyncio.wait_for(room.chunk_send_started.wait(), timeout=1)

    tool_call_id = room.requests[0][1]["tool_call_id"]
    await room._handle_tool_call_response_chunk(
        protocol=room.protocol,  # type: ignore[arg-type]
        message_id=1,
        typ="room.tool_call_response_chunk",
        data=pack_message(
            header={
                "tool_call_id": tool_call_id,
                "chunk": _ControlContent(method="close").to_json(),
            }
        ),
    )

    events = [item async for item in response]
    assert len(events) == 1
    assert isinstance(events[0], _ControlContent)
    assert events[0].method == "close"
    await asyncio.wait_for(room.chunk_send_cancelled.wait(), timeout=1)
    await asyncio.sleep(0)

    close_watcher = room._close_watcher_task
    if close_watcher is not None:
        close_watcher.cancel()
        await asyncio.gather(close_watcher, return_exceptions=True)


@pytest.mark.asyncio
async def test_invoke_tool_stream_allows_error_chunks_without_closing() -> None:
    room = _OpenResponseRoom()
    client = AgentsClient(room=room)  # type: ignore[arg-type]

    response = await client.invoke_tool(
        toolkit="test-toolkit",
        tool="streaming-tool",
        input={"a": 1},
    )
    assert isinstance(response, AsyncIterator)
    tool_call_id = room.requests[0][1]["tool_call_id"]

    await room._handle_tool_call_response_chunk(
        protocol=room.protocol,  # type: ignore[arg-type]
        message_id=1,
        typ="room.tool_call_response_chunk",
        data=pack_message(
            header={
                "tool_call_id": tool_call_id,
                "chunk": ErrorContent(text="recoverable").to_json(),
            }
        ),
    )
    await room._handle_tool_call_response_chunk(
        protocol=room.protocol,  # type: ignore[arg-type]
        message_id=1,
        typ="room.tool_call_response_chunk",
        data=pack_message(
            header={
                "tool_call_id": tool_call_id,
                "chunk": TextContent(text="still running").to_json(),
            }
        ),
    )
    await room._handle_tool_call_response_chunk(
        protocol=room.protocol,  # type: ignore[arg-type]
        message_id=1,
        typ="room.tool_call_response_chunk",
        data=pack_message(
            header={
                "tool_call_id": tool_call_id,
                "chunk": _ControlContent(method="close").to_json(),
            }
        ),
    )

    events = []
    async for item in response:
        events.append(item)

    assert len(events) == 3
    assert isinstance(events[0], ErrorContent)
    assert events[0].text == "recoverable"
    assert isinstance(events[1], TextContent)
    assert events[1].text == "still running"
    assert isinstance(events[2], _ControlContent)
    assert events[2].method == "close"


@pytest.mark.asyncio
async def test_invoke_tool_stream_raises_when_close_chunk_is_abnormal() -> None:
    room = _OpenResponseRoom()
    client = AgentsClient(room=room)  # type: ignore[arg-type]

    response = await client.invoke_tool(
        toolkit="test-toolkit",
        tool="streaming-tool",
        input={"a": 1},
    )
    assert isinstance(response, AsyncIterator)
    tool_call_id = room.requests[0][1]["tool_call_id"]

    await room._handle_tool_call_response_chunk(
        protocol=room.protocol,  # type: ignore[arg-type]
        message_id=1,
        typ="room.tool_call_response_chunk",
        data=pack_message(
            header={
                "tool_call_id": tool_call_id,
                "chunk": _ControlContent(
                    method="close",
                    status_code=ControlCloseStatus.INVALID_DATA,
                    message="bad schema",
                ).to_json(),
            }
        ),
    )

    with pytest.raises(RoomException, match="bad schema") as ex_info:
        async for _ in response:
            pass
    assert ex_info.value.status_code == ControlCloseStatus.INVALID_DATA


@pytest.mark.asyncio
async def test_invoke_tool_sends_empty_content_when_input_omitted() -> None:
    room = _FakeRoom()
    client = AgentsClient(room=room)  # type: ignore[arg-type]

    response = await client.invoke_tool(
        toolkit="test-toolkit",
        tool="streaming-tool",
    )

    assert isinstance(response, JsonContent)
    request = room.requests[0]
    assert request[1]["arguments"] == {"type": "empty"}
    assert request[2] is None


class _SyncRoom(_FakeRoom):
    def __init__(self, *, schema_json: dict, initial_payload: bytes = b""):
        self._schema_json = schema_json
        self._initial_payload = initial_payload
        self.protocol = _FakeProtocol()
        self.requests: list[dict[str, object]] = []
        self.open_start_chunks: list[BinaryContent] = []
        self.sync_input_chunks: list[BinaryContent] = []
        self.open_stream_closed = asyncio.Event()

    async def invoke(self, **kwargs) -> Content | AsyncIterator[Content]:
        self.requests.append(kwargs)
        tool = kwargs["tool"]
        input_value = kwargs["input"]

        if tool == "open":
            assert isinstance(input_value, AsyncIterable)

            async def open_stream() -> AsyncIterator[Content]:
                iterator = input_value.__aiter__()
                start_chunk = await iterator.__anext__()
                assert isinstance(start_chunk, BinaryContent)
                self.open_start_chunks.append(start_chunk)
                path = start_chunk.headers["path"]
                assert isinstance(path, str)
                yield BinaryContent(
                    data=self._initial_payload,
                    headers={
                        "kind": "state",
                        "path": path,
                        "schema": self._schema_json,
                    },
                )
                async for chunk in iterator:
                    assert isinstance(chunk, BinaryContent)
                    self.sync_input_chunks.append(chunk)
                self.open_stream_closed.set()

            return open_stream()

        if tool == "describe":
            return JsonContent(json={})

        if tool == "create":
            return EmptyContent()

        return EmptyContent()


@pytest.mark.asyncio
async def test_sync_client_normalizes_leading_slash_paths_for_open_sync_and_close() -> (
    None
):
    schema = MeshSchema(
        root_tag_name="thread",
        elements=[ElementType(tag_name="thread", properties=[])],
    )
    room = _SyncRoom(schema_json=schema.to_json())
    client = SyncClient(room=room)  # type: ignore[arg-type]
    await client.start()

    path = "/agents/assistant/threads/testing-chat-thread.thread"
    normalized_path = "agents/assistant/threads/testing-chat-thread.thread"
    try:
        doc = await asyncio.wait_for(client.open(path=path), timeout=1)
        assert doc is client._connected_documents[normalized_path].ref

        assert len(room.requests) == 1
        assert room.requests[0]["toolkit"] == "sync"
        assert room.requests[0]["tool"] == "open"
        assert len(room.open_start_chunks) == 1
        assert room.open_start_chunks[0].headers["kind"] == "start"
        assert room.open_start_chunks[0].headers["path"] == normalized_path
        assert room.open_start_chunks[0].headers["create"] is True

        await client.sync(path=path, data=b"YQ==")
        await asyncio.sleep(0)
        assert len(room.sync_input_chunks) == 1
        assert room.sync_input_chunks[0].headers == {"kind": "sync"}
        assert room.sync_input_chunks[0].data == b"YQ=="

        await client.close(path=path)
        await asyncio.wait_for(room.open_stream_closed.wait(), timeout=1)
        assert len(room.requests) == 1
    finally:
        await client.stop()


@pytest.mark.asyncio
async def test_sync_client_create_includes_schema_when_provided() -> None:
    schema = MeshSchema(
        root_tag_name="thread",
        elements=[ElementType(tag_name="thread", properties=[])],
    )
    room = _SyncRoom(schema_json=schema.to_json())
    client = SyncClient(room=room)  # type: ignore[arg-type]

    await client.create(
        path="/agents/assistant/threads/testing-chat-thread.thread",
        json={"thread": {"properties": []}},
        schema=schema,
    )

    assert room.requests[0]["toolkit"] == "sync"
    assert room.requests[0]["tool"] == "create"
    assert room.requests[0]["input"]["path"] == (
        "agents/assistant/threads/testing-chat-thread.thread"
    )
    assert room.requests[0]["input"]["json"] == {"thread": {"properties": []}}
    assert room.requests[0]["input"]["schema"] == (schema.to_json())


@pytest.mark.asyncio
async def test_sync_client_open_completes_on_initial_empty_sync() -> None:
    schema = MeshSchema(
        root_tag_name="thread",
        elements=[ElementType(tag_name="thread", properties=[])],
    )
    room = _SyncRoom(schema_json=schema.to_json(), initial_payload=b"")
    client = SyncClient(room=room)  # type: ignore[arg-type]
    await client.start()

    try:
        doc = await asyncio.wait_for(client.open(path="thread.thread"), timeout=1)
        assert doc.synchronized.done()
        assert doc.synchronized.result() is True
    finally:
        await client.stop()


@pytest.mark.asyncio
async def test_sync_client_sync_not_connected_uses_error_code() -> None:
    schema = MeshSchema(
        root_tag_name="thread",
        elements=[ElementType(tag_name="thread", properties=[])],
    )
    room = _SyncRoom(schema_json=schema.to_json())
    client = SyncClient(room=room)  # type: ignore[arg-type]

    with pytest.raises(
        RoomException,
        match="attempted to sync to a document that is not connected",
    ) as ex:
        await client.sync(path="missing.thread", data=b"YQ==")

    assert ex.value.code == ErrorCode.SYNC_NOT_CONNECTED


@pytest.mark.asyncio
async def test_sync_client_close_not_connected_uses_error_code() -> None:
    schema = MeshSchema(
        root_tag_name="thread",
        elements=[ElementType(tag_name="thread", properties=[])],
    )
    room = _SyncRoom(schema_json=schema.to_json())
    client = SyncClient(room=room)  # type: ignore[arg-type]

    with pytest.raises(RoomException, match="Not connected to missing.thread") as ex:
        await client.close(path="missing.thread")

    assert ex.value.code == ErrorCode.SYNC_NOT_CONNECTED


@pytest.mark.asyncio
async def test_sync_client_unexpected_response_uses_error_code() -> None:
    client = SyncClient(room=_BadSyncResponseRoom())  # type: ignore[arg-type]

    with pytest.raises(
        RoomException, match="unexpected return type from sync.open"
    ) as ex:
        await client.open(path="thread.thread")

    assert ex.value.code == ErrorCode.UNEXPECTED_RESPONSE_TYPE
