import asyncio
import base64
import contextlib
import json
import logging
import uuid
from collections.abc import AsyncIterable, AsyncIterator, Callable
from datetime import date, datetime, timezone
from types import SimpleNamespace

import aiohttp
import pyarrow as pa
import pytest

import meshagent.api.room_server_client as room_server_client
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
    pack_content,
    pack_message,
    unpack_message,
    unpack_content_parts,
)
from meshagent.api.protocol import (
    ProtocolCloseKind,
    ProtocolReconnectUnsupportedError,
)
from meshagent.api import ErrorCode
from meshagent.api.oauth import OAuthClientConfig
from meshagent.api.room_server_client import (
    AgentsClient,
    ContainersClient,
    DatasetsClient,
    DatasetJson,
    DeveloperClient,
    DockerSecret,
    DatasetExpression,
    DatasetStruct,
    Image,
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
    SyncClient,
    decode_records,
    encode_records,
)
from meshagent.api.specs.service import (
    ConfigMountSpec,
    ContainerMountSpec,
    EmptyDirMountSpec,
    RoomStorageMountSpec,
)
from meshagent.api.schema import ChildProperty, ElementType, MeshSchema, ValueProperty


def test_required_table_round_trips_full_arrow_schema_fidelity() -> None:
    schema = pa.schema(
        [
            pa.field(
                "annotations",
                pa.list_(
                    pa.struct(
                        [
                            pa.field(
                                "key",
                                pa.string(),
                                nullable=False,
                                metadata={b"role": b"key"},
                            ),
                            pa.field(
                                "value", pa.large_string(), metadata={b"role": b"value"}
                            ),
                        ]
                    )
                ),
                metadata={b"field": b"annotations"},
            ),
            pa.field("labels", pa.dictionary(pa.int32(), pa.string())),
            pa.field("amount", pa.decimal128(20, 4)),
        ],
        metadata={b"schema": b"required-table"},
    )
    requirement = room_server_client.RequiredTable(
        name="records",
        namespace=["team"],
        schema=schema,
        scalar_indexes=["amount"],
        full_text_search_indexes=["annotations"],
        vector_indexes=["embedding"],
    )

    encoded = requirement.to_json()
    decoded = room_server_client.Requirement.from_json(encoded)

    assert isinstance(decoded, room_server_client.RequiredTable)
    assert decoded.name == "records"
    assert decoded.namespace == ["team"]
    assert decoded.scalar_indexes == ["amount"]
    assert decoded.full_text_search_indexes == ["annotations"]
    assert decoded.vector_indexes == ["embedding"]
    assert decoded.schema.equals(schema, check_metadata=True)
    assert room_server_client._schema_from_arrow_ipc(
        base64.b64decode(encoded["schema"])
    ).equals(schema, check_metadata=True)


class _FakeProtocol:
    def __init__(self):
        self.handlers: dict[str, object] = {}
        self.entered = False
        self.exited = False
        self.sent_messages: list[tuple[str, bytes | str, int | None]] = []
        self.send_started = asyncio.Event()
        self._next_message_id = 0
        self._close_reason: str | None = None
        self._close_kind: str | None = None
        self._is_open = False

    def register_handler(self, typ: str, handler: object) -> None:
        self.handlers[typ] = handler

    def unregister_handler(self, typ: str, handler: object) -> None:
        assert self.handlers[typ] is handler
        self.handlers.pop(typ)

    def get_handler(self, typ: str) -> object | None:
        return self.handlers.get(typ)

    async def __aenter__(self):
        self.entered = True
        self._is_open = True
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb
        self.exited = True
        self._is_open = False

    def create_factory(self):
        protocol = self
        used = False

        def factory():
            nonlocal used
            if used:
                raise ProtocolReconnectUnsupportedError(
                    "protocol_factory was not configured for reconnecting this protocol"
                )
            used = True
            return protocol

        return factory

    def next_message_id(self) -> int:
        self._next_message_id += 1
        return self._next_message_id

    def send_nowait(
        self,
        type: str,
        data: bytes | str,
        message_id: int | None = None,
    ) -> int:
        self.sent_messages.append((type, data, message_id))
        self.send_started.set()
        return -1 if message_id is None else message_id

    async def send(
        self,
        type: str,
        data: bytes | str,
        message_id: int | None = None,
    ) -> int:
        return self.send_nowait(type=type, data=data, message_id=message_id)

    async def wait_for_close(self) -> None:
        await asyncio.Future()

    def close_reason(self) -> str | None:
        return self._close_reason

    def close_kind(self) -> str | None:
        return self._close_kind

    @property
    def is_open(self) -> bool:
        return self._is_open


class _CloseableProtocol(_FakeProtocol):
    def __init__(self, *, close_reason: str | None = None):
        super().__init__()
        self._close_event = asyncio.Event()
        self._close_reason = close_reason

    def close(self) -> None:
        self._is_open = False
        self._close_event.set()

    async def wait_for_close(self) -> None:
        await self._close_event.wait()

    def close_reason(self) -> str | None:
        return self._close_reason


def test_room_exception_defaults_to_invalid_request_code() -> None:
    ex = RoomException("boom")
    assert ex.code == ErrorCode.INVALID_REQUEST


def test_room_exception_explicit_none_code_is_preserved() -> None:
    ex = RoomException("boom", code=None)
    assert ex.code is None


def test_encode_decode_records_uuid_roundtrip() -> None:
    value = uuid.uuid4()

    encoded = encode_records([{"id": value}])

    assert encoded == [{"id": {"uuid": str(value)}}]

    decoded = decode_records(encoded)

    assert decoded == [{"id": value}]


def test_encode_decode_records_expression_roundtrip() -> None:
    encoded = encode_records([{"id": DatasetExpression("uuid()")}])

    assert encoded == [{"id": {"expression": "uuid()"}}]

    decoded = decode_records(encoded)

    assert decoded == [{"id": DatasetExpression("uuid()")}]


def test_encode_decode_records_struct_and_json_roundtrip() -> None:
    payload = {"kind": "demo", "count": 3, "tags": ["x", "y"]}

    encoded = encode_records(
        [
            {
                "meta": DatasetStruct(
                    {
                        "source": "studio",
                        "labels": ["a", "b"],
                        "payload": DatasetJson(payload),
                    }
                ),
                "payload": DatasetJson(payload),
            }
        ]
    )

    assert encoded == [
        {
            "meta": {
                "struct": {
                    "source": "studio",
                    "labels": {"list": ["a", "b"]},
                    "payload": {"json": payload},
                }
            },
            "payload": {"json": payload},
        }
    ]

    decoded = decode_records(encoded)

    assert decoded == [
        {
            "meta": DatasetStruct(
                {
                    "source": "studio",
                    "labels": ["a", "b"],
                    "payload": DatasetJson(payload),
                }
            ),
            "payload": DatasetJson(payload),
        }
    ]


def test_decode_records_rejects_non_string_expression_payload() -> None:
    with pytest.raises(ValueError, match="dataset expression values must be strings"):
        decode_records([{"id": {"expression": {"name": "uuid()"}}}])


def test_decode_records_rejects_unwrapped_object_payload() -> None:
    with pytest.raises(
        ValueError,
        match="dataset object values must use a single-key type wrapper",
    ):
        decode_records([{"meta": {"kind": "demo", "count": 3}}])


def test_encode_decode_records_date_and_timestamp_roundtrip() -> None:
    day = date(2026, 4, 9)
    moment = datetime(2026, 4, 9, 12, 30, 45, tzinfo=timezone.utc)

    encoded = encode_records([{"day": day, "moment": moment}])

    assert encoded == [
        {
            "day": {"date": "2026-04-09"},
            "moment": {"timestamp": "2026-04-09T12:30:45Z"},
        }
    ]

    decoded = decode_records(encoded)

    assert decoded == [{"day": day, "moment": moment}]


def test_dataset_stream_decode_value_returns_typed_date_and_timestamp() -> None:
    day = room_server_client._dataset_stream_decode_value(
        {"date": "2026-04-09"},
        operation="search",
    )
    moment = room_server_client._dataset_stream_decode_value(
        {"timestamp": "2026-04-09T12:30:45Z"},
        operation="search",
    )

    assert day == date(2026, 4, 9)
    assert moment == datetime(2026, 4, 9, 12, 30, 45, tzinfo=timezone.utc)


class _FakeRoom:
    _copy_exception = RoomClient._copy_exception
    _ensure_close_watcher = RoomClient._ensure_close_watcher
    _close_tool_call_streams = RoomClient._close_tool_call_streams
    _client_closed_terminal_state = RoomClient._client_closed_terminal_state
    _fail_pending_requests = RoomClient._fail_pending_requests
    _fail_pending_work = RoomClient._fail_pending_work
    _fail_tool_call_streams = RoomClient._fail_tool_call_streams
    _fail_tool_call_streams_and_wait = RoomClient._fail_tool_call_streams_and_wait
    _handle_tool_call_response_chunk = RoomClient._handle_tool_call_response_chunk
    _format_closed_message = RoomClient._format_closed_message
    _maybe_cancel_close_watcher = RoomClient._maybe_cancel_close_watcher
    _protocol_close_detail = RoomClient._protocol_close_detail
    _protocol_terminal_state = RoomClient._protocol_terminal_state
    _remove_tool_call_stream = RoomClient._remove_tool_call_stream
    _make_tool_call_stream = RoomClient._make_tool_call_stream
    _send_tool_call_request_chunk = RoomClient._send_tool_call_request_chunk
    _set_terminal_state = RoomClient._set_terminal_state
    _stream_tool_call_request_chunks = RoomClient._stream_tool_call_request_chunks
    invoke = RoomClient.invoke
    list_toolkits = RoomClient.list_toolkits

    def __init__(self):
        self.protocol = _FakeProtocol()
        self._protocol_instance = self.protocol
        self.events: list[tuple[str, dict]] = []
        self.requests: list[tuple[str, dict, bytes | None]] = []
        self._pending_requests = {}
        self._tool_call_streams = {}
        self._close_watcher_task = None
        self._lifecycle_task = None
        self._terminal_state = None
        self._room_closed = asyncio.Future()
        self._entered = True
        self._connected = True
        self._closing = False
        self._allow_disconnected_requests = False
        self._close_reason = None
        self.local_participant = None
        self.list_toolkits_response: dict | None = None

    def emit(self, event_name: str, **kwargs) -> None:
        self.events.append((event_name, kwargs))

    @property
    def is_connected(self) -> bool:
        return self._connected

    async def wait_until_connected(self) -> None:
        if self._connected:
            return
        await asyncio.shield(self._room_closed)

    async def _wait_until_connected_for_messages(self) -> None:
        await self.wait_until_connected()
        self._raise_if_terminal_for_messages()

    def _raise_if_terminal(self) -> None:
        state = self._terminal_state
        if state is not None:
            raise state.request_error()

    def _raise_if_terminal_for_messages(self) -> None:
        state = self._terminal_state
        if state is not None:
            raise state.message_send_error()

    def _coerce_message_send_error(self, error: RoomException) -> RoomException:
        return error

    def invoke_nowait(
        self,
        *,
        toolkit: str,
        tool: str,
        input: str | dict | Content | None = None,
        participant_id: str | None = None,
        on_behalf_of_id: str | None = None,
        caller_context: dict | None = None,
    ) -> None:
        if input is None:
            arguments: dict[str, object] = {"type": "empty"}
            data = None
        elif isinstance(input, dict):
            arguments = {"type": "json", "json": input}
            data = None
        elif isinstance(input, str):
            arguments = {"type": "text", "text": input}
            data = None
        else:
            arguments = input.to_json()
            data = input.get_data()

        request = {
            "toolkit": toolkit,
            "tool": tool,
            "participant_id": participant_id,
            "on_behalf_of_id": on_behalf_of_id,
            "arguments": arguments,
            "tool_call_id": uuid.uuid4().hex,
        }
        if caller_context is not None:
            request["caller_context"] = caller_context
        self.requests.append(("room.invoke_tool", request, data))

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

        if tool == "move":
            return EmptyContent()

        raise AssertionError(f"unexpected tool: {tool}")


class _StreamingBuildRoom:
    def __init__(self) -> None:
        self.protocol = _FakeProtocol()
        self.requests: list[dict[str, object]] = []
        self.start_chunk: BinaryContent | None = None
        self.data_chunks: list[BinaryContent] = []

    async def invoke(self, **kwargs):
        self.requests.append(kwargs)
        assert kwargs["tool"] == "build"
        tool_input = kwargs["input"]
        assert isinstance(tool_input, AsyncIterable)
        iterator = tool_input.__aiter__()
        start_chunk = await iterator.__anext__()
        assert isinstance(start_chunk, BinaryContent)
        self.start_chunk = start_chunk
        async for chunk in iterator:
            assert isinstance(chunk, BinaryContent)
            self.data_chunks.append(chunk)
        return JsonContent(json={"build_id": "build-1"})


async def _bytes_chunks(chunks: list[bytes]) -> AsyncIterator[bytes]:
    for chunk in chunks:
        yield chunk


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


class _ClosingProtocol(_FakeProtocol):
    def __init__(self) -> None:
        super().__init__()
        self._close_kind = ProtocolCloseKind.SERVER

    async def wait_for_close(self) -> None:
        return None


class _ClosingProtocolWithReason(_FakeProtocol):
    def __init__(self) -> None:
        super().__init__()
        self._close_kind = ProtocolCloseKind.SERVER

    async def wait_for_close(self) -> None:
        return None

    def close_reason(self) -> str | None:
        return "websocket closed with code 1008"


class _StatusClosingProtocol(_FakeProtocol):
    def __init__(self) -> None:
        super().__init__()
        self._close_kind = ProtocolCloseKind.SERVER

    async def wait_for_close(self) -> None:
        handler = self.handlers["room.status"]
        result = handler(
            self,
            1,
            "room.status",
            pack_message({"status": "error", "message": "room is starting"}, b""),
        )
        if asyncio.iscoroutine(result):
            await result
        return None

    def close_reason(self) -> str | None:
        return "websocket closed with code 1013"


class _StartupExceptionProtocol(_FakeProtocol):
    def __init__(self, *, message: str) -> None:
        super().__init__()
        self._message = message

    async def __aenter__(self):
        self.entered = True
        raise RuntimeError(self._message)


class _HandshakeStatusProtocol(_FakeProtocol):
    def __init__(self, *, status: int, message: str) -> None:
        super().__init__()
        self._status = status
        self._message = message

    async def __aenter__(self):
        self.entered = True
        raise aiohttp.ClientResponseError(
            request_info=None,
            history=(),
            status=self._status,
            message=self._message,
        )


class _ErrorClosingProtocol(_FakeProtocol):
    def __init__(self, *, close_reason: str) -> None:
        super().__init__()
        self._close_event = asyncio.Event()
        self._close_reason = close_reason
        self._close_kind = ProtocolCloseKind.ERROR

    async def __aenter__(self):
        await super().__aenter__()
        asyncio.get_running_loop().call_soon(self._close_event.set)
        self._is_open = False
        return self

    async def wait_for_close(self) -> None:
        await self._close_event.wait()

    def close_kind(self) -> ProtocolCloseKind | None:
        return self._close_kind

    def close_reason(self) -> str | None:
        return self._close_reason


async def _wait_until(
    predicate: Callable[[], bool], *, timeout: float = 1.0, interval: float = 0.01
) -> None:
    async def wait_loop() -> None:
        while not predicate():
            await asyncio.sleep(interval)

    await asyncio.wait_for(wait_loop(), timeout=timeout)


def _simple_thread_schema() -> MeshSchema:
    return MeshSchema(
        root_tag_name="thread",
        elements=[ElementType(tag_name="thread", properties=[])],
    )


class _ReconnectProtocol:
    def __init__(self, controller: "_ReconnectRoomController", *, index: int) -> None:
        self.controller = controller
        self.index = index
        self.handlers: dict[str, object] = {}
        self.sent_messages: list[tuple[str, bytes | str, int | None]] = []
        self.entered = False
        self.exited = False
        self._next_message_id = 0
        self._close_event = asyncio.Event()
        self._close_reason: str | None = None
        self._close_kind: ProtocolCloseKind | None = None
        self._is_open = False
        self._tool_calls: dict[str, tuple[str, str]] = {}
        self._token = "token"
        self._url = f"wss://example.test/room/{index}"
        self._background_send_error: BaseException | None = None
        self._background_send_tasks: set[asyncio.Task[None]] = set()

    @property
    def token(self) -> str:
        return self._token

    @property
    def url(self) -> str:
        return self._url

    @property
    def is_open(self) -> bool:
        return self._is_open

    def register_handler(self, typ: str, handler: object) -> None:
        self.handlers[typ] = handler

    def unregister_handler(self, typ: str, handler: object) -> None:
        assert self.handlers[typ] is handler
        self.handlers.pop(typ)

    def get_handler(self, typ: str) -> object | None:
        return self.handlers.get(typ)

    async def __aenter__(self):
        self.entered = True
        self._is_open = True
        asyncio.create_task(self._emit_ready())
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb
        self.exited = True
        self._is_open = False
        if self._close_kind is None:
            self._close_kind = ProtocolCloseKind.CLIENT
        self._close_event.set()

    def next_message_id(self) -> int:
        self._next_message_id += 1
        return self._next_message_id

    def _raise_background_send_error(self) -> None:
        if self._background_send_error is None:
            return
        error = self._background_send_error
        self._background_send_error = None
        raise error

    def _on_background_send_done(self, task: asyncio.Task[None]) -> None:
        self._background_send_tasks.discard(task)
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as ex:
            if self._background_send_error is None:
                self._background_send_error = ex

    def _start_handle_send_task(
        self,
        *,
        typ: str,
        data: bytes,
        message_id: int,
    ) -> asyncio.Task[None]:
        self._raise_background_send_error()
        task = asyncio.create_task(
            self.controller.handle_send(
                protocol=self,
                typ=typ,
                data=data,
                message_id=message_id,
            )
        )
        self._background_send_tasks.add(task)
        task.add_done_callback(self._on_background_send_done)
        return task

    def send_nowait(
        self,
        type: str,
        data: bytes | str,
        message_id: int | None = None,
    ) -> int:
        if message_id is None:
            message_id = self.next_message_id()
        self.sent_messages.append((type, data, message_id))
        if isinstance(data, str):
            data = data.encode("utf-8")
        self._start_handle_send_task(typ=type, data=data, message_id=message_id)
        return message_id

    async def send(
        self,
        type: str,
        data: bytes | str,
        message_id: int | None = None,
    ) -> int:
        if message_id is None:
            message_id = self.next_message_id()
        self.sent_messages.append((type, data, message_id))
        if isinstance(data, str):
            data = data.encode("utf-8")
        task = self._start_handle_send_task(typ=type, data=data, message_id=message_id)
        await task
        return message_id

    async def wait_for_close(self) -> None:
        await self._close_event.wait()

    def close_kind(self) -> ProtocolCloseKind | None:
        return self._close_kind

    def close_reason(self) -> str | None:
        return self._close_reason

    def close_unexpected(self, *, reason: str) -> None:
        self._close_kind = ProtocolCloseKind.ERROR
        self._close_reason = reason
        self._is_open = False
        self._close_event.set()

    def close_server(self, *, reason: str) -> None:
        self._close_kind = ProtocolCloseKind.SERVER
        self._close_reason = reason
        self._is_open = False
        self._close_event.set()

    async def _emit_ready(self) -> None:
        await asyncio.sleep(0)
        await self._emit(
            "room_ready",
            pack_message(
                {
                    "room_name": "test-room",
                    "room_url": "wss://example.test/room",
                    "session_id": "session-id",
                }
            ),
        )
        await self._emit(
            "connected",
            pack_message(
                {
                    "type": "init",
                    "participantId": "local-participant",
                    "attributes": {"name": "Local Participant"},
                }
            ),
        )

    async def _emit(self, typ: str, data: bytes, *, message_id: int = 1) -> None:
        handler = self.handlers.get(typ)
        if handler is None:
            return
        result = handler(self, message_id, typ, data)
        if asyncio.iscoroutine(result):
            await result

    async def emit_response(self, *, message_id: int, content: Content) -> None:
        await self._emit("__response__", pack_content(content), message_id=message_id)

    async def emit_tool_call_chunk(
        self,
        *,
        tool_call_id: str,
        chunk: Content,
        message_id: int = 1,
    ) -> None:
        await self._emit(
            "room.tool_call_response_chunk",
            pack_message(
                header={
                    "tool_call_id": tool_call_id,
                    "chunk": chunk.to_json(),
                },
                data=chunk.get_data(),
            ),
            message_id=message_id,
        )

    async def emit_messaging_enabled(self) -> None:
        await self._emit(
            "messaging.send",
            pack_message(
                header={
                    "from_participant_id": "room",
                    "type": "messaging.enabled",
                    "message": {"participants": self.controller.messaging_participants},
                }
            ),
        )


class _ReconnectRoomController:
    def __init__(
        self,
        *,
        schema: MeshSchema,
        initial_sync_payload: bytes = b"",
        delay_messaging_send_responses: bool = False,
    ) -> None:
        self.schema_json = schema.to_json()
        self.initial_sync_payload = initial_sync_payload
        self.delay_messaging_send_responses = delay_messaging_send_responses
        self.protocols: list[_ReconnectProtocol] = []
        self.sync_open_headers: list[dict[str, object]] = []
        self.sync_input_chunks: list[tuple[int, bytes]] = []
        self.messaging_enable_calls: list[int] = []
        self.messaging_send_inputs: list[tuple[int, dict[str, object]]] = []
        self.set_attribute_payloads: list[tuple[int, dict[str, object]]] = []
        self.messaging_participants = [
            {
                "id": "remote-participant",
                "role": "user",
                "attributes": {"name": "Remote Participant"},
            }
        ]

    def protocol_factory(self) -> _ReconnectProtocol:
        protocol = _ReconnectProtocol(self, index=len(self.protocols))
        self.protocols.append(protocol)
        return protocol

    async def handle_send(
        self,
        *,
        protocol: _ReconnectProtocol,
        typ: str,
        data: bytes,
        message_id: int,
    ) -> None:
        if typ == "room.invoke_tool":
            request, _ = unpack_message(data)
            toolkit = request["toolkit"]
            tool = request["tool"]
            tool_call_id = request["tool_call_id"]
            protocol._tool_calls[tool_call_id] = (toolkit, tool)
            if toolkit == "sync" and tool == "open":
                await protocol.emit_response(
                    message_id=message_id,
                    content=_ControlContent(method="open"),
                )
                return
            if toolkit == "messaging" and tool == "enable":
                self.messaging_enable_calls.append(protocol.index)
                await protocol.emit_response(
                    message_id=message_id,
                    content=EmptyContent(),
                )
                await protocol.emit_messaging_enabled()
                return
            if toolkit == "messaging" and tool == "send":
                arguments = request["arguments"]["json"]
                assert isinstance(arguments, dict)
                self.messaging_send_inputs.append((protocol.index, arguments))
                if self.delay_messaging_send_responses:
                    return
                await protocol.emit_response(
                    message_id=message_id,
                    content=EmptyContent(),
                )
                return
            if toolkit == "messaging" and tool in ("broadcast", "disable"):
                await protocol.emit_response(
                    message_id=message_id,
                    content=EmptyContent(),
                )
                return
            raise AssertionError(f"unexpected invoke request: {toolkit}.{tool}")

        if typ == "room.tool_call_request_chunk":
            request, payload = unpack_message(data)
            tool_call_id = request["tool_call_id"]
            toolkit, tool = protocol._tool_calls[tool_call_id]
            chunk = unpack_content_parts(header=request["chunk"], payload=payload)
            await protocol.emit_response(message_id=message_id, content=EmptyContent())
            if toolkit != "sync" or tool != "open":
                raise AssertionError(f"unexpected stream chunk for {toolkit}.{tool}")

            if isinstance(chunk, BinaryContent):
                kind = chunk.headers["kind"]
                assert isinstance(kind, str)
                if kind == "start":
                    self.sync_open_headers.append(dict(chunk.headers))
                    path = chunk.headers["path"]
                    assert isinstance(path, str)
                    await protocol.emit_tool_call_chunk(
                        tool_call_id=tool_call_id,
                        chunk=BinaryContent(
                            data=self.initial_sync_payload,
                            headers={
                                "kind": "state",
                                "path": path,
                                "schema": self.schema_json,
                            },
                        ),
                    )
                    return
                if kind == "sync":
                    self.sync_input_chunks.append((protocol.index, chunk.data))
                    return
                raise AssertionError(f"unexpected sync chunk kind: {kind}")

            if isinstance(chunk, _ControlContent):
                assert chunk.method == "close"
                await protocol.emit_tool_call_chunk(
                    tool_call_id=tool_call_id,
                    chunk=_ControlContent(method="close"),
                )
                return

            raise AssertionError(f"unexpected sync chunk type: {type(chunk)!r}")

        if typ == "set_attributes":
            payload, _ = unpack_message(data)
            self.set_attribute_payloads.append((protocol.index, payload))
            return

        raise AssertionError(f"unexpected protocol send: {typ}")


def _shared_document_schema() -> MeshSchema:
    return MeshSchema(
        root_tag_name="thread",
        elements=[
            ElementType(
                tag_name="thread",
                properties=[
                    ChildProperty(
                        name="children",
                        description="",
                        child_tag_names=["item"],
                    )
                ],
            ),
            ElementType(
                tag_name="item",
                properties=[
                    ValueProperty(name="text", description="", type="string"),
                ],
            ),
        ],
    )


def _document_item_texts(doc) -> list[str]:
    return sorted(str(child["text"]) for child in doc.root.get_children())


class _SharedReconnectProtocol:
    def __init__(
        self,
        controller: "_SharedReconnectRoomController",
        *,
        participant_key: str,
        index: int,
    ) -> None:
        self.controller = controller
        self.participant_key = participant_key
        self.index = index
        self.handlers: dict[str, object] = {}
        self.sent_messages: list[tuple[str, bytes | str, int | None]] = []
        self.entered = False
        self.exited = False
        self._next_message_id = 0
        self._close_event = asyncio.Event()
        self._close_reason: str | None = None
        self._close_kind: ProtocolCloseKind | None = None
        self._is_open = False
        self._tool_calls: dict[str, tuple[str, str]] = {}
        self._closed_notified = False
        self._token = "token"
        self._url = f"wss://example.test/shared-room/{participant_key}/{index}"
        self._background_send_error: BaseException | None = None
        self._background_send_tasks: set[asyncio.Task[None]] = set()

    @property
    def token(self) -> str:
        return self._token

    @property
    def url(self) -> str:
        return self._url

    @property
    def is_open(self) -> bool:
        return self._is_open

    def register_handler(self, typ: str, handler: object) -> None:
        self.handlers[typ] = handler

    def unregister_handler(self, typ: str, handler: object) -> None:
        assert self.handlers[typ] is handler
        self.handlers.pop(typ)

    def get_handler(self, typ: str) -> object | None:
        return self.handlers.get(typ)

    async def __aenter__(self):
        self.entered = True
        self._is_open = True
        self.controller.on_protocol_enter(self)
        asyncio.create_task(self._emit_ready())
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb
        self.exited = True
        self._is_open = False
        if self._close_kind is None:
            self._close_kind = ProtocolCloseKind.CLIENT
        self._notify_closed()
        self._close_event.set()

    def _notify_closed(self) -> None:
        if self._closed_notified:
            return
        self._closed_notified = True
        self.controller.on_protocol_closed(self)

    def next_message_id(self) -> int:
        self._next_message_id += 1
        return self._next_message_id

    def _raise_background_send_error(self) -> None:
        if self._background_send_error is None:
            return
        error = self._background_send_error
        self._background_send_error = None
        raise error

    def _on_background_send_done(self, task: asyncio.Task[None]) -> None:
        self._background_send_tasks.discard(task)
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as ex:
            if self._background_send_error is None:
                self._background_send_error = ex

    def _start_handle_send_task(
        self,
        *,
        typ: str,
        data: bytes,
        message_id: int,
    ) -> asyncio.Task[None]:
        self._raise_background_send_error()
        task = asyncio.create_task(
            self.controller.handle_send(
                protocol=self,
                typ=typ,
                data=data,
                message_id=message_id,
            )
        )
        self._background_send_tasks.add(task)
        task.add_done_callback(self._on_background_send_done)
        return task

    def send_nowait(
        self,
        type: str,
        data: bytes | str,
        message_id: int | None = None,
    ) -> int:
        if message_id is None:
            message_id = self.next_message_id()
        self.sent_messages.append((type, data, message_id))
        if isinstance(data, str):
            data = data.encode("utf-8")
        self._start_handle_send_task(typ=type, data=data, message_id=message_id)
        return message_id

    async def send(
        self,
        type: str,
        data: bytes | str,
        message_id: int | None = None,
    ) -> int:
        if message_id is None:
            message_id = self.next_message_id()
        self.sent_messages.append((type, data, message_id))
        if isinstance(data, str):
            data = data.encode("utf-8")
        task = self._start_handle_send_task(typ=type, data=data, message_id=message_id)
        await task
        return message_id

    async def wait_for_close(self) -> None:
        await self._close_event.wait()

    def close_kind(self) -> ProtocolCloseKind | None:
        return self._close_kind

    def close_reason(self) -> str | None:
        return self._close_reason

    def close_unexpected(self, *, reason: str) -> None:
        if self._close_event.is_set():
            return
        self._close_kind = ProtocolCloseKind.ERROR
        self._close_reason = reason
        self._is_open = False
        self._notify_closed()
        self._close_event.set()

    def close_server(self, *, reason: str) -> None:
        if self._close_event.is_set():
            return
        self._close_kind = ProtocolCloseKind.SERVER
        self._close_reason = reason
        self._is_open = False
        self._notify_closed()
        self._close_event.set()

    async def _emit_ready(self) -> None:
        await asyncio.sleep(0)
        await self._emit(
            "room_ready",
            pack_message(
                {
                    "room_name": "shared-room",
                    "room_url": "wss://example.test/shared-room",
                    "session_id": f"session-{self.participant_key}",
                }
            ),
        )
        participant = self.controller.participant_state(self.participant_key)
        await self._emit(
            "connected",
            pack_message(
                {
                    "type": "init",
                    "participantId": participant["id"],
                    "attributes": participant["attributes"],
                }
            ),
        )

    async def _emit(self, typ: str, data: bytes, *, message_id: int = 1) -> None:
        handler = self.handlers.get(typ)
        if handler is None:
            return
        result = handler(self, message_id, typ, data)
        if asyncio.iscoroutine(result):
            await result

    async def emit_response(self, *, message_id: int, content: Content) -> None:
        await self._emit("__response__", pack_content(content), message_id=message_id)

    async def emit_tool_call_chunk(
        self,
        *,
        tool_call_id: str,
        chunk: Content,
        message_id: int = 1,
    ) -> None:
        await self._emit(
            "room.tool_call_response_chunk",
            pack_message(
                header={
                    "tool_call_id": tool_call_id,
                    "chunk": chunk.to_json(),
                },
                data=chunk.get_data(),
            ),
            message_id=message_id,
        )


class _SharedReconnectRoomController:
    def __init__(self, *, schema: MeshSchema) -> None:
        self.schema = schema
        self.schema_json = schema.to_json()
        self.protocols: list[_SharedReconnectProtocol] = []
        self._active_protocols: dict[str, _SharedReconnectProtocol] = {}
        self._streams_by_path: dict[
            str, dict[str, tuple[_SharedReconnectProtocol, str]]
        ] = {}
        self._tool_call_paths: dict[tuple[int, str], str] = {}
        self._open_paths_by_protocol: dict[int, set[str]] = {}
        self._documents: dict[str, object] = {}
        self._messaging_enabled: set[str] = set()
        self._participants: dict[str, dict[str, object]] = {
            "alice": {
                "id": "alice",
                "role": "user",
                "attributes": {"name": "Alice"},
            },
            "bob": {
                "id": "bob",
                "role": "user",
                "attributes": {"name": "Bob"},
            },
        }

    def make_protocol_factory(
        self, participant_key: str
    ) -> Callable[[], _SharedReconnectProtocol]:
        def factory() -> _SharedReconnectProtocol:
            protocol = _SharedReconnectProtocol(
                self,
                participant_key=participant_key,
                index=len(self.protocols),
            )
            self.protocols.append(protocol)
            return protocol

        return factory

    def participant_state(self, participant_key: str) -> dict[str, object]:
        participant = self._participants[participant_key]
        return {
            "id": participant["id"],
            "role": participant["role"],
            "attributes": dict(participant["attributes"]),
        }

    def participant_attributes(self, participant_key: str) -> dict[str, object]:
        participant = self._participants[participant_key]
        return dict(participant["attributes"])

    def document_item_texts(self, path: str) -> list[str]:
        document = self._documents[path]
        return _document_item_texts(document)

    def cleanup(self) -> None:
        for document in self._documents.values():
            document.close()
        self._documents.clear()

    def on_protocol_enter(self, protocol: _SharedReconnectProtocol) -> None:
        self._active_protocols[protocol.participant_key] = protocol
        self._open_paths_by_protocol.setdefault(protocol.index, set())

    def on_protocol_closed(self, protocol: _SharedReconnectProtocol) -> None:
        active = self._active_protocols.get(protocol.participant_key)
        if active is protocol:
            self._active_protocols.pop(protocol.participant_key)

        open_paths = self._open_paths_by_protocol.pop(protocol.index, set())
        for path in open_paths:
            streams = self._streams_by_path.get(path)
            if streams is None:
                continue
            streams.pop(protocol.participant_key, None)
            if not streams:
                self._streams_by_path.pop(path)

        for key in list(self._tool_call_paths.keys()):
            protocol_index, _ = key
            if protocol_index == protocol.index:
                self._tool_call_paths.pop(key)

        if protocol.participant_key in self._messaging_enabled:
            self._messaging_enabled.remove(protocol.participant_key)
            self._schedule(
                self._broadcast_messaging_event(
                    source_participant_key=protocol.participant_key,
                    message_type="participant.disabled",
                    message={"id": self._participants[protocol.participant_key]["id"]},
                )
            )

    def _schedule(self, coroutine: asyncio.Future | asyncio.Task | object) -> None:
        if asyncio.iscoroutine(coroutine):
            asyncio.create_task(coroutine)

    def _ensure_document(self, *, path: str, schema: MeshSchema) -> object:
        existing = self._documents.get(path)
        if existing is not None:
            return existing

        def publish_sync(base64_value: str) -> None:
            self._schedule(
                self._broadcast_sync_update(path=path, base64_value=base64_value)
            )

        document = room_server_client.runtime.new_document(
            schema=schema,
            on_document_sync=publish_sync,
        )
        self._documents[path] = document
        return document

    async def _broadcast_sync_update(self, *, path: str, base64_value: str) -> None:
        streams = list(self._streams_by_path.get(path, {}).values())
        payload = base64_value.encode("utf-8")
        for protocol, tool_call_id in streams:
            await protocol.emit_tool_call_chunk(
                tool_call_id=tool_call_id,
                chunk=BinaryContent(
                    data=payload,
                    headers={"kind": "sync", "path": path},
                ),
            )

    def _participant_json(self, participant_key: str) -> dict[str, object]:
        participant = self._participants[participant_key]
        return {
            "id": participant["id"],
            "role": participant["role"],
            "attributes": dict(participant["attributes"]),
        }

    def _messaging_participants_json(self, *, exclude: str) -> list[dict[str, object]]:
        participants = []
        for participant_key in sorted(self._messaging_enabled):
            if participant_key == exclude:
                continue
            if participant_key not in self._active_protocols:
                continue
            participants.append(self._participant_json(participant_key))
        return participants

    async def _broadcast_messaging_event(
        self,
        *,
        source_participant_key: str,
        message_type: str,
        message: dict[str, object],
    ) -> None:
        source_participant = self._participants[source_participant_key]
        for participant_key in sorted(self._messaging_enabled):
            if participant_key == source_participant_key:
                continue
            protocol = self._active_protocols.get(participant_key)
            if protocol is None:
                continue
            await protocol._emit(
                "messaging.send",
                pack_message(
                    header={
                        "from_participant_id": source_participant["id"],
                        "type": message_type,
                        "message": message,
                    }
                ),
            )

    async def _handle_messaging_enable(
        self,
        *,
        protocol: _SharedReconnectProtocol,
        message_id: int,
    ) -> None:
        participant_key = protocol.participant_key
        self._messaging_enabled.add(participant_key)
        await protocol.emit_response(message_id=message_id, content=EmptyContent())
        participant = self._participants[participant_key]
        await protocol._emit(
            "messaging.send",
            pack_message(
                header={
                    "from_participant_id": participant["id"],
                    "type": "messaging.enabled",
                    "message": {
                        "participants": self._messaging_participants_json(
                            exclude=participant_key
                        )
                    },
                }
            ),
        )
        await self._broadcast_messaging_event(
            source_participant_key=participant_key,
            message_type="participant.enabled",
            message=self._participant_json(participant_key),
        )

    async def _handle_messaging_disable(
        self,
        *,
        protocol: _SharedReconnectProtocol,
        message_id: int,
    ) -> None:
        participant_key = protocol.participant_key
        self._messaging_enabled.discard(participant_key)
        await protocol.emit_response(message_id=message_id, content=EmptyContent())
        await self._broadcast_messaging_event(
            source_participant_key=participant_key,
            message_type="participant.disabled",
            message={"id": self._participants[participant_key]["id"]},
        )

    async def handle_send(
        self,
        *,
        protocol: _SharedReconnectProtocol,
        typ: str,
        data: bytes,
        message_id: int,
    ) -> None:
        if typ == "room.invoke_tool":
            request, _ = unpack_message(data)
            toolkit = request["toolkit"]
            tool = request["tool"]
            tool_call_id = request["tool_call_id"]
            protocol._tool_calls[tool_call_id] = (toolkit, tool)
            if toolkit == "sync" and tool == "open":
                await protocol.emit_response(
                    message_id=message_id,
                    content=_ControlContent(method="open"),
                )
                return
            if toolkit == "messaging" and tool == "enable":
                await self._handle_messaging_enable(
                    protocol=protocol,
                    message_id=message_id,
                )
                return
            if toolkit == "messaging" and tool == "disable":
                await self._handle_messaging_disable(
                    protocol=protocol,
                    message_id=message_id,
                )
                return
            if toolkit == "messaging" and tool in ("send", "broadcast"):
                await protocol.emit_response(
                    message_id=message_id,
                    content=EmptyContent(),
                )
                return
            raise AssertionError(f"unexpected invoke request: {toolkit}.{tool}")

        if typ == "room.tool_call_request_chunk":
            request, payload = unpack_message(data)
            tool_call_id = request["tool_call_id"]
            toolkit, tool = protocol._tool_calls[tool_call_id]
            chunk = unpack_content_parts(header=request["chunk"], payload=payload)
            await protocol.emit_response(message_id=message_id, content=EmptyContent())
            if toolkit != "sync" or tool != "open":
                raise AssertionError(f"unexpected stream chunk for {toolkit}.{tool}")

            if isinstance(chunk, BinaryContent):
                kind = chunk.headers["kind"]
                assert isinstance(kind, str)
                if kind == "start":
                    path = chunk.headers["path"]
                    assert isinstance(path, str)
                    vector_header = chunk.headers["vector"]
                    vector = (
                        None
                        if vector_header is None
                        else base64.standard_b64decode(str(vector_header))
                    )
                    schema_json = chunk.headers["schema"]
                    schema = (
                        self.schema
                        if not isinstance(schema_json, dict)
                        else MeshSchema.from_json(schema_json)
                    )
                    document = self._ensure_document(path=path, schema=schema)
                    self._streams_by_path.setdefault(path, {})[
                        protocol.participant_key
                    ] = (
                        protocol,
                        tool_call_id,
                    )
                    self._tool_call_paths[(protocol.index, tool_call_id)] = path
                    self._open_paths_by_protocol.setdefault(protocol.index, set()).add(
                        path
                    )
                    await protocol.emit_tool_call_chunk(
                        tool_call_id=tool_call_id,
                        chunk=BinaryContent(
                            data=base64.standard_b64encode(
                                document.get_state(vector=vector)
                            ),
                            headers={
                                "kind": "state",
                                "path": path,
                                "schema": document.schema.to_json(),
                            },
                        ),
                    )
                    return
                if kind == "sync":
                    path = self._tool_call_paths[(protocol.index, tool_call_id)]
                    document = self._documents[path]
                    room_server_client.runtime.apply_backend_changes(
                        document.id,
                        chunk.data.decode("utf-8"),
                    )
                    return
                raise AssertionError(f"unexpected sync chunk kind: {kind}")

            if isinstance(chunk, _ControlContent):
                assert chunk.method == "close"
                path = self._tool_call_paths.pop((protocol.index, tool_call_id))
                self._open_paths_by_protocol.setdefault(protocol.index, set()).discard(
                    path
                )
                streams = self._streams_by_path.get(path)
                if streams is not None:
                    streams.pop(protocol.participant_key, None)
                    if not streams:
                        self._streams_by_path.pop(path)
                await protocol.emit_tool_call_chunk(
                    tool_call_id=tool_call_id,
                    chunk=_ControlContent(method="close"),
                )
                return

            raise AssertionError(f"unexpected sync chunk type: {type(chunk)!r}")

        if typ == "set_attributes":
            payload, _ = unpack_message(data)
            participant = self._participants[protocol.participant_key]
            attributes = participant["attributes"]
            assert isinstance(attributes, dict)
            attributes.update(payload)
            if protocol.participant_key in self._messaging_enabled:
                await self._broadcast_messaging_event(
                    source_participant_key=protocol.participant_key,
                    message_type="participant.attributes",
                    message={"attributes": payload},
                )
            return

        raise AssertionError(f"unexpected protocol send: {typ}")


@pytest.mark.asyncio
async def test_room_client_enter_raises_if_connection_closes_before_ready() -> None:
    protocol = _ClosingProtocol()
    client = RoomClient(protocol_factory=protocol.create_factory())

    with pytest.raises(
        RoomException,
        match="room connection closed before the room became ready",
    ):
        await client.__aenter__()

    assert protocol.entered is True
    assert protocol.exited is True
    assert client.is_closed is True
    assert client.close_kind() == ProtocolCloseKind.SERVER
    await asyncio.wait_for(client.wait_for_close(), timeout=1)


@pytest.mark.asyncio
async def test_room_client_enter_includes_close_reason_when_connection_closes_early() -> (
    None
):
    protocol = _ClosingProtocolWithReason()
    client = RoomClient(protocol_factory=protocol.create_factory())

    with pytest.raises(
        RoomException,
        match=(
            "room connection closed before the room became ready: "
            "websocket closed with code 1008"
        ),
    ):
        await client.__aenter__()


@pytest.mark.asyncio
async def test_room_client_enter_does_not_include_last_room_status_when_connection_closes_early() -> (
    None
):
    protocol = _StatusClosingProtocol()
    client = RoomClient(protocol_factory=protocol.create_factory())

    with pytest.raises(
        RoomException,
        match=(
            "room connection closed before the room became ready: "
            "websocket closed with code 1013"
        ),
    ):
        await client.__aenter__()


@pytest.mark.asyncio
async def test_room_client_enter_retries_transient_error_close_before_ready() -> None:
    controller = _ReconnectRoomController(schema=_simple_thread_schema())
    protocol_factory_calls = 0

    def protocol_factory():
        nonlocal protocol_factory_calls
        protocol_factory_calls += 1
        if protocol_factory_calls == 1:
            return _ErrorClosingProtocol(
                close_reason=(
                    "websocket closed with code 1006: Cannot write to closing transport"
                )
            )
        return controller.protocol_factory()

    room = RoomClient(
        protocol_factory=protocol_factory,
        reconnect_timeout=0.1,
    )

    try:
        await room.__aenter__()
        assert protocol_factory_calls == 2
        assert len(controller.protocols) == 1
        assert room.is_connected is True
    finally:
        if room._entered:
            await room.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_room_client_enter_retries_transient_startup_exception() -> None:
    controller = _ReconnectRoomController(schema=_simple_thread_schema())
    protocol_factory_calls = 0

    def protocol_factory():
        nonlocal protocol_factory_calls
        protocol_factory_calls += 1
        if protocol_factory_calls == 1:
            return _StartupExceptionProtocol(message="transient startup error")
        return controller.protocol_factory()

    room = RoomClient(
        protocol_factory=protocol_factory,
        reconnect_timeout=0.1,
    )

    try:
        await room.__aenter__()
        assert protocol_factory_calls == 2
        assert len(controller.protocols) == 1
        assert room.is_connected is True
    finally:
        if room._entered:
            await room.__aexit__(None, None, None)


@pytest.mark.parametrize(
    ("status", "message"),
    [
        (403, "Forbidden"),
        (404, "Not Found"),
    ],
)
@pytest.mark.asyncio
async def test_room_client_enter_does_not_retry_non_retryable_handshake_response(
    status: int, message: str
) -> None:
    controller = _ReconnectRoomController(schema=_simple_thread_schema())
    protocol_factory_calls = 0

    def protocol_factory():
        nonlocal protocol_factory_calls
        protocol_factory_calls += 1
        if protocol_factory_calls == 1:
            return _HandshakeStatusProtocol(status=status, message=message)
        return controller.protocol_factory()

    room = RoomClient(
        protocol_factory=protocol_factory,
        reconnect_timeout=5,
    )

    with pytest.raises(RoomException) as ex_info:
        await room.__aenter__()

    assert str(ex_info.value) == (
        "room connection unexpectedly closed before the room became ready: "
        f"websocket connect failed with status {status}: {message}"
    )
    assert protocol_factory_calls == 1
    assert controller.protocols == []
    assert room.is_closed is True
    assert room.close_kind() == ProtocolCloseKind.ERROR
    assert (
        room.close_reason()
        == f"websocket connect failed with status {status}: {message}"
    )
    await asyncio.wait_for(room.wait_for_close(), timeout=1)


@pytest.mark.asyncio
async def test_room_client_enter_reconnect_timeout_closes_room_after_startup_failures() -> (
    None
):
    protocol_factory_calls = 0

    def protocol_factory():
        nonlocal protocol_factory_calls
        protocol_factory_calls += 1
        return _StartupExceptionProtocol(message="transient startup error")

    room = RoomClient(
        protocol_factory=protocol_factory,
        reconnect_timeout=0.1,
    )

    with pytest.raises(RoomException) as ex_info:
        await room.__aenter__()

    assert str(ex_info.value) == (
        "room connection unexpectedly closed before the room became ready: "
        "room reconnect timed out after 0.1s (transient startup error)"
    )
    assert protocol_factory_calls >= 2
    assert room.is_connected is False
    assert room.is_closed is True
    assert room.close_kind() == ProtocolCloseKind.ERROR
    assert room.close_reason() == (
        "room reconnect timed out after 0.1s (transient startup error)"
    )
    await asyncio.wait_for(room.wait_for_close(), timeout=1)

    with pytest.raises(RoomException) as request_ex_info:
        await room.send_request("room.ping", {"hello": "world"})
    assert str(request_ex_info.value) == (
        "room connection unexpectedly closed before request completed: "
        "room reconnect timed out after 0.1s (transient startup error)"
    )


@pytest.mark.parametrize(
    ("status", "message"),
    [
        (403, "Forbidden"),
        (404, "Not Found"),
    ],
)
@pytest.mark.asyncio
async def test_room_client_reconnect_does_not_retry_non_retryable_handshake_response(
    status: int, message: str
) -> None:
    controller = _ReconnectRoomController(schema=_simple_thread_schema())
    protocol_factory_calls = 0

    def protocol_factory():
        nonlocal protocol_factory_calls
        protocol_factory_calls += 1
        if protocol_factory_calls == 1:
            return controller.protocol_factory()
        return _HandshakeStatusProtocol(status=status, message=message)

    room = RoomClient(
        protocol_factory=protocol_factory,
        reconnect_timeout=5,
    )
    await room.__aenter__()

    try:
        controller.protocols[0].close_unexpected(reason="transient transport error")

        await asyncio.wait_for(room.wait_for_close(), timeout=1)

        assert protocol_factory_calls == 2
        assert room.is_closed is True
        assert room.close_kind() == ProtocolCloseKind.ERROR
        assert (
            room.close_reason()
            == f"websocket connect failed with status {status}: {message}"
        )

        with pytest.raises(RoomException) as ex_info:
            await room.send_request("room.ping", {"hello": "world"})
        assert str(ex_info.value) == (
            "room connection unexpectedly closed before request completed: "
            f"websocket connect failed with status {status}: {message}"
        )
    finally:
        if room._entered:
            await room.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_room_client_exit_fails_open_tool_streams_and_cancels_close_watcher() -> (
    None
):
    protocol = _FakeProtocol()
    client = RoomClient(protocol_factory=protocol.create_factory())
    client._ensure_close_watcher()

    started = asyncio.Event()

    async def _pending_request() -> Content:
        started.set()
        await asyncio.Future()

    request_task = asyncio.create_task(_pending_request())
    await started.wait()

    request_stream_cancelled = asyncio.Event()
    request_stream_started = asyncio.Event()

    async def _pending_request_stream() -> None:
        try:
            request_stream_started.set()
            await asyncio.Future()
        except asyncio.CancelledError:
            request_stream_cancelled.set()
            raise

    request_stream_task = asyncio.create_task(_pending_request_stream())
    await request_stream_started.wait()

    stream = client._make_tool_call_stream(
        tool_call_id="tc-1",
        request_task=request_task,
    )
    stream.attach_request_stream_task(request_stream_task)

    await client.__aexit__(None, None, None)
    await asyncio.gather(request_task, return_exceptions=True)
    await asyncio.wait_for(request_stream_cancelled.wait(), timeout=1)

    assert client._tool_call_streams == {}
    assert isinstance(stream.error, RoomException)
    assert str(stream.error) == "room client was closed before tool call completed"
    assert protocol.exited is True
    assert client._close_watcher_task is None
    assert request_stream_task.done()


@pytest.mark.asyncio
async def test_send_request_fails_when_connection_closes_before_response() -> None:
    protocol = _CloseableProtocol(close_reason="websocket closed with code 1013")
    client = RoomClient(protocol_factory=protocol.create_factory())

    request_task = asyncio.create_task(
        client.send_request("room.ping", {"hello": "world"})
    )
    await asyncio.wait_for(protocol.send_started.wait(), timeout=1)

    protocol.close()

    with pytest.raises(
        RoomException,
        match=(
            "room connection closed before request completed: "
            "websocket closed with code 1013"
        ),
    ):
        await request_task

    close_watcher = client._close_watcher_task
    if close_watcher is not None:
        await asyncio.gather(close_watcher, return_exceptions=True)

    assert client._pending_requests == {}
    assert client._close_watcher_task is None


@pytest.mark.asyncio
async def test_room_client_exit_fails_pending_requests_and_cancels_close_watcher() -> (
    None
):
    protocol = _CloseableProtocol()
    client = RoomClient(protocol_factory=protocol.create_factory())

    request_task = asyncio.create_task(
        client.send_request("room.ping", {"hello": "world"})
    )
    await asyncio.wait_for(protocol.send_started.wait(), timeout=1)

    await client.__aexit__(None, None, None)

    with pytest.raises(
        RoomException,
        match="room client was closed before request completed",
    ):
        await request_task

    assert client._pending_requests == {}
    assert protocol.exited is True
    assert client._close_watcher_task is None


@pytest.mark.asyncio
async def test_room_client_wait_for_close_ignores_unexpected_disconnects() -> None:
    controller = _ReconnectRoomController(schema=_simple_thread_schema())
    room = RoomClient(protocol_factory=controller.protocol_factory)
    await room.__aenter__()

    try:
        wait_task = asyncio.create_task(room.protocol.wait_for_close())

        controller.protocols[0].close_unexpected(reason="transient transport error")

        await _wait_until(lambda: len(controller.protocols) == 2)
        await _wait_until(lambda: room.is_connected)
        assert wait_task.done() is False

        controller.protocols[1].close_server(reason="websocket closed with code 1000")

        await asyncio.wait_for(wait_task, timeout=1)
        assert room.close_reason() == "websocket closed with code 1000"
    finally:
        await room.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_room_client_reconnect_timeout_zero_disables_retry_and_closes_room() -> (
    None
):
    controller = _ReconnectRoomController(schema=_simple_thread_schema())
    room = RoomClient(
        protocol_factory=controller.protocol_factory,
        reconnect_timeout=0,
    )
    await room.__aenter__()

    try:
        wait_task = asyncio.create_task(room.wait_for_close())

        controller.protocols[0].close_unexpected(reason="transient transport error")

        await asyncio.wait_for(wait_task, timeout=1)

        assert len(controller.protocols) == 1
        assert room.is_connected is False
        assert room.is_closed is True
        assert room.close_kind() == ProtocolCloseKind.ERROR
        assert room.close_reason() == "transient transport error"

        with pytest.raises(RoomException) as ex_info:
            await room.send_request("room.ping", {"hello": "world"})
        assert str(ex_info.value) == (
            "room connection unexpectedly closed before request completed: "
            "transient transport error"
        )
    finally:
        await room.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_room_client_reconnect_restores_sync_and_messaging_state() -> None:
    controller = _ReconnectRoomController(schema=_simple_thread_schema())
    room = RoomClient(protocol_factory=controller.protocol_factory)
    await room.__aenter__()

    try:
        observed_disconnect_reasons: list[str | None] = []
        observed_reconnect_count = 0

        def _on_disconnected(*, reason: str | None) -> None:
            observed_disconnect_reasons.append(reason)

        def _on_reconnected() -> None:
            nonlocal observed_reconnect_count
            observed_reconnect_count += 1

        room.on("disconnected", _on_disconnected)
        room.on("reconnected", _on_reconnected)

        room.messaging.enable()
        doc = await room.sync.open(path="thread.thread")
        original_local_participant = room.local_participant
        expected_vector = base64.standard_b64encode(doc.get_state_vector()).decode(
            "utf-8"
        )

        assert room.messaging.online is True
        assert len(room.messaging.remote_participants) == 1
        assert controller.sync_open_headers[0]["vector"] is None

        controller.protocols[0].close_unexpected(reason="transient transport error")

        await _wait_until(lambda: room.is_connected is False)
        assert room.messaging.online is False
        assert room.messaging.remote_participants == []

        with pytest.raises(
            RoomException,
            match="attempted to sync to a document that is not connected",
        ) as ex_info:
            await room.sync.sync(path="thread.thread", data=b"YQ==")
        assert ex_info.value.code == ErrorCode.SYNC_NOT_CONNECTED
        assert controller.sync_input_chunks == []

        send_task = asyncio.create_task(
            room.messaging.send_message(
                to=SimpleNamespace(id="remote-participant"),
                type="direct",
                message={"value": 1},
            )
        )
        await asyncio.sleep(0.05)
        assert send_task.done() is False

        await _wait_until(lambda: len(controller.protocols) == 2)
        await _wait_until(lambda: room.is_connected)
        await asyncio.wait_for(send_task, timeout=1)

        assert controller.messaging_enable_calls == [0, 1]
        assert controller.messaging_send_inputs == [
            (
                1,
                {
                    "to_participant_id": "remote-participant",
                    "type": "direct",
                    "message_json": '{"value": 1}',
                    "attachment_base64": None,
                },
            )
        ]
        assert controller.sync_open_headers[1]["vector"] == expected_vector
        assert room.messaging.online is True
        assert len(room.messaging.remote_participants) == 1
        assert room.local_participant is original_local_participant

        room.local_participant.set_attribute("status", "ready")
        await _wait_until(
            lambda: (1, {"status": "ready"}) in controller.set_attribute_payloads
        )
        assert (1, {"name": "Local Participant"}) in controller.set_attribute_payloads

        assert observed_disconnect_reasons == ["transient transport error"]
        assert observed_reconnect_count == 1
    finally:
        controller.protocols[-1].close_server(reason="websocket closed with code 1000")
        await room.wait_for_close()
        await room.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_room_client_close_fails_in_flight_message_send_with_room_closed_error() -> (
    None
):
    controller = _ReconnectRoomController(
        schema=_simple_thread_schema(),
        delay_messaging_send_responses=True,
    )
    room = RoomClient(protocol_factory=controller.protocol_factory)
    await room.__aenter__()

    try:
        room.messaging.enable()
        await _wait_until(lambda: room.messaging.online)
        await _wait_until(lambda: len(room.messaging.remote_participants) == 1)

        send_task = asyncio.create_task(
            room.messaging.send_message(
                to=room.messaging.remote_participants[0],
                type="direct",
                message={"value": 1},
            )
        )
        await _wait_until(lambda: len(controller.messaging_send_inputs) == 1)

        await room.__aexit__(None, None, None)

        with pytest.raises(
            RoomException,
            match="room client was closed before message send completed",
        ):
            await send_task
    finally:
        if room._entered:
            await room.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_room_client_reconnect_timeout_closes_room_and_fails_waiting_message_sends() -> (
    None
):
    controller = _ReconnectRoomController(schema=_simple_thread_schema())
    protocol_factory_calls = 0

    def protocol_factory() -> _ReconnectProtocol:
        nonlocal protocol_factory_calls
        protocol_factory_calls += 1
        if protocol_factory_calls == 1:
            return controller.protocol_factory()
        raise RuntimeError(f"reconnect attempt {protocol_factory_calls} failed")

    room = RoomClient(
        protocol_factory=protocol_factory,
        reconnect_timeout=0.1,
    )
    await room.__aenter__()

    try:
        room.messaging.enable()
        await _wait_until(lambda: room.messaging.online)

        controller.protocols[0].close_unexpected(reason="transient transport error")

        await _wait_until(lambda: room.is_connected is False)

        send_task = asyncio.create_task(
            room.messaging.send_message(
                to=SimpleNamespace(id="remote-participant"),
                type="direct",
                message={"value": 1},
            )
        )
        await asyncio.sleep(0.05)
        assert send_task.done() is False

        await asyncio.wait_for(room.wait_for_close(), timeout=1)

        assert protocol_factory_calls >= 2
        assert room.is_closed is True
        assert room.close_kind() == ProtocolCloseKind.ERROR
        assert room.close_reason() == (
            "room reconnect timed out after 0.1s (transient transport error)"
        )

        with pytest.raises(RoomException) as ex_info:
            await send_task
        assert str(ex_info.value) == (
            "room connection unexpectedly closed before message send completed: "
            "room reconnect timed out after 0.1s (transient transport error)"
        )
    finally:
        await room.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_room_client_closed_attribute_updates_do_not_warn(
    caplog: pytest.LogCaptureFixture,
) -> None:
    controller = _ReconnectRoomController(schema=_simple_thread_schema())
    room = RoomClient(protocol_factory=controller.protocol_factory)
    await room.__aenter__()

    try:
        controller.protocols[-1].close_server(reason="websocket closed with code 1000")
        await room.wait_for_close()

        with caplog.at_level(logging.WARNING, logger="room_server_client"):
            await room.local_participant.set_attribute("status", "closed")

        assert [
            record
            for record in caplog.records
            if record.name == "room_server_client" and record.levelno >= logging.WARNING
        ] == []
    finally:
        if room._entered:
            await room.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_room_client_unexpected_disconnect_warns_once_before_retrying_reconnects(
    caplog: pytest.LogCaptureFixture,
) -> None:
    controller = _ReconnectRoomController(schema=_simple_thread_schema())
    protocol_factory_calls = 0

    def protocol_factory() -> _ReconnectProtocol:
        nonlocal protocol_factory_calls
        protocol_factory_calls += 1
        if protocol_factory_calls in (2, 3):
            raise RuntimeError(f"reconnect attempt {protocol_factory_calls} failed")
        return controller.protocol_factory()

    room = RoomClient(protocol_factory=protocol_factory)
    await room.__aenter__()

    try:
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger="room_server_client"):
            controller.protocols[0].close_unexpected(reason="transient transport error")
            await _wait_until(lambda: len(controller.protocols) == 2)
            await _wait_until(lambda: room.is_connected)

        warning_records = [
            record
            for record in caplog.records
            if record.name == "room_server_client" and record.levelno >= logging.WARNING
        ]
        assert len(warning_records) == 1
        assert warning_records[0].message == (
            "room connection lost (transient transport error); automatically "
            "attempting to reconnect"
        )
    finally:
        controller.protocols[-1].close_server(reason="websocket closed with code 1000")
        await room.wait_for_close()
        await room.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_room_client_reconnect_resynchronizes_offline_sync_document_changes_from_both_participants() -> (
    None
):
    controller = _SharedReconnectRoomController(schema=_shared_document_schema())
    alice = RoomClient(protocol_factory=controller.make_protocol_factory("alice"))
    bob = RoomClient(protocol_factory=controller.make_protocol_factory("bob"))
    await alice.__aenter__()
    await bob.__aenter__()

    try:
        path = "thread.thread"
        alice_doc = await alice.sync.open(path=path)
        bob_doc = await bob.sync.open(path=path)

        alice_protocol = controller._active_protocols["alice"]
        alice_protocol.close_unexpected(reason="alice transport error")
        await _wait_until(lambda: alice.is_connected is False)

        alice_doc.root.append_child("item", {"text": "alice-offline"})
        bob_doc.root.append_child("item", {"text": "bob-online"})
        await _wait_until(
            lambda: controller.document_item_texts(path) == ["bob-online"]
        )
        await _wait_until(lambda: alice.is_connected)
        await _wait_until(
            lambda: (
                _document_item_texts(alice_doc) == ["alice-offline", "bob-online"]
                and _document_item_texts(bob_doc) == ["alice-offline", "bob-online"]
            )
        )

        bob_protocol = controller._active_protocols["bob"]
        bob_protocol.close_unexpected(reason="bob transport error")
        await _wait_until(lambda: bob.is_connected is False)

        bob_doc.root.append_child("item", {"text": "bob-offline"})
        alice_doc.root.append_child("item", {"text": "alice-online"})
        await _wait_until(
            lambda: (
                controller.document_item_texts(path)
                == ["alice-offline", "alice-online", "bob-online"]
            )
        )
        await _wait_until(lambda: bob.is_connected)
        await _wait_until(
            lambda: (
                _document_item_texts(alice_doc)
                == ["alice-offline", "alice-online", "bob-offline", "bob-online"]
                and _document_item_texts(bob_doc)
                == ["alice-offline", "alice-online", "bob-offline", "bob-online"]
            )
        )
    finally:
        await alice.__aexit__(None, None, None)
        await bob.__aexit__(None, None, None)
        controller.cleanup()


@pytest.mark.asyncio
async def test_room_client_reconnect_resynchronizes_offline_local_participant_attributes_from_both_participants() -> (
    None
):
    controller = _SharedReconnectRoomController(schema=_shared_document_schema())
    alice = RoomClient(protocol_factory=controller.make_protocol_factory("alice"))
    bob = RoomClient(protocol_factory=controller.make_protocol_factory("bob"))
    await alice.__aenter__()
    await bob.__aenter__()

    try:
        alice.messaging.enable()
        bob.messaging.enable()
        await _wait_until(lambda: alice.messaging.online and bob.messaging.online)
        await _wait_until(
            lambda: (
                alice.messaging.get_participant("bob") is not None
                and bob.messaging.get_participant("alice") is not None
            )
        )

        bob_protocol = controller._active_protocols["bob"]
        bob_protocol.close_unexpected(reason="bob transport error")
        await _wait_until(lambda: bob.is_connected is False)
        assert bob.messaging.online is False
        await _wait_until(lambda: alice.messaging.get_participant("bob") is None)

        bob.local_participant.set_attribute("status", "bob-offline")
        await _wait_until(lambda: bob.is_connected)
        await _wait_until(lambda: bob.messaging.online)
        await _wait_until(
            lambda: (
                alice.messaging.get_participant("bob") is not None
                and alice.messaging.get_participant("bob").get_attribute("status")
                == "bob-offline"
            )
        )

        alice_protocol = controller._active_protocols["alice"]
        alice_protocol.close_unexpected(reason="alice transport error")
        await _wait_until(lambda: alice.is_connected is False)
        assert alice.messaging.online is False
        await _wait_until(lambda: bob.messaging.get_participant("alice") is None)

        alice.local_participant.set_attribute("status", "alice-offline")
        await _wait_until(lambda: alice.is_connected)
        await _wait_until(lambda: alice.messaging.online)
        await _wait_until(
            lambda: (
                bob.messaging.get_participant("alice") is not None
                and bob.messaging.get_participant("alice").get_attribute("status")
                == "alice-offline"
            )
        )
    finally:
        await alice.__aexit__(None, None, None)
        await bob.__aexit__(None, None, None)
        controller.cleanup()


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
        {"payload": {"binary": base64.b64encode(b"hello").decode()}}
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
            self.messaging_client: MessagingClient | None = None

        def invoke_nowait(
            self,
            *,
            toolkit: str,
            tool: str,
            input: str | dict | Content | None = None,
            participant_id: str | None = None,
            on_behalf_of_id: str | None = None,
            caller_context: dict | None = None,
        ) -> None:
            super().invoke_nowait(
                toolkit=toolkit,
                tool=tool,
                input=input,
                participant_id=participant_id,
                on_behalf_of_id=on_behalf_of_id,
                caller_context=caller_context,
            )
            if tool == "enable":
                assert self.messaging_client is not None
                self.messaging_client._on_messaging_enabled(
                    RoomMessage(
                        from_participant_id="local-participant",
                        type="messaging.enabled",
                        message={"participants": []},
                    )
                )

        async def send_request(
            self, typ: str, request: dict, data: bytes | None = None
        ) -> Content | dict:
            self.requests.append((typ, request, data))
            if typ != "room.invoke_tool":
                return {}
            return EmptyContent()

    room = _FakeMessagingRoom()
    client = MessagingClient(room=room)  # type: ignore[arg-type]
    room.messaging_client = client

    await client.start()
    client.enable()
    assert client.online is True
    await client.broadcast_message(
        type="broadcast",
        message={"hello": "world"},
        attachment=b"bytes",
    )
    await client.send_message(
        to=SimpleNamespace(id="remote-participant"),
        type="direct",
        message={"value": 1},
        attachment=b"\x00\x01",
    )
    client.disable()
    assert client.online is False
    await client.stop()
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
            if tool == "exists":
                return JsonContent(json={"exists": True})
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
    assert (
        await client.exists(
            secret_id="secret-1",
            for_identity="service-agent",
        )
        is True
    )
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

    assert [request[0] for request in room.requests] == ["room.invoke_tool"] * 13
    assert [request[1]["toolkit"] for request in room.requests] == ["secrets"] * 13
    assert [request[1]["tool"] for request in room.requests] == [
        "provide_oauth_authorization",
        "provide_oauth_authorization",
        "provide_secret",
        "provide_secret",
        "get_offline_oauth_token",
        "request_oauth_token",
        "list_secrets",
        "exists",
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

    exists_arguments = room.requests[7][1]["arguments"]
    assert isinstance(exists_arguments, dict)
    assert exists_arguments == {
        "type": "json",
        "json": {
            "secret_id": "secret-1",
            "delegated_to": None,
            "for_identity": "service-agent",
        },
    }

    set_secret_arguments = room.requests[11][1]["arguments"]
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
    assert room.requests[11][2] == b"payload"


@pytest.mark.asyncio
async def test_storage_client_unexpected_response_uses_error_code() -> None:
    client = StorageClient(room=_BadStorageResponseRoom())  # type: ignore[arg-type]

    with pytest.raises(
        RoomException, match="unexpected return type from storage.exists"
    ) as ex:
        await client.exists(path="file.txt")

    assert ex.value.code == ErrorCode.UNEXPECTED_RESPONSE_TYPE


@pytest.mark.asyncio
async def test_datasets_client_unexpected_response_uses_error_code() -> None:
    client = DatasetsClient(room=_BadResponseRoom())  # type: ignore[arg-type]

    with pytest.raises(
        RoomException, match="unexpected return type from datasets.list_tables"
    ) as ex:
        await client.list_tables()

    assert ex.value.code == ErrorCode.UNEXPECTED_RESPONSE_TYPE


@pytest.mark.asyncio
async def test_datasets_client_uses_room_invoke_for_commands() -> None:
    inspect_schema = pa.schema(
        [
            pa.field(
                "payload",
                pa.list_(
                    pa.struct(
                        [
                            pa.field(
                                "key",
                                pa.string(),
                                nullable=False,
                                metadata={b"role": b"key"},
                            ),
                            pa.field(
                                "value", pa.large_string(), metadata={b"role": b"value"}
                            ),
                        ]
                    )
                ),
                metadata={b"field": b"payload"},
            ),
        ],
        metadata={b"schema": b"inspect"},
    )

    class _StreamingDatasetsRoom:
        def __init__(self) -> None:
            self.protocol = _FakeProtocol()
            self.calls: list[dict[str, object]] = []
            self.write_starts: dict[str, dict[str, object]] = {}
            self.write_chunks: dict[str, list[pa.Table]] = {
                "create_table": [],
                "insert": [],
                "merge": [],
            }
            self.read_starts: dict[str, dict[str, object]] = {}
            self.read_pulls: dict[str, list[dict[str, object]]] = {
                "search": [],
                "read_sql_query": [],
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
                    assert isinstance(start_chunk, BinaryContent)
                    self.write_starts[tool] = dict(start_chunk.headers)
                    yield BinaryContent(data=b"", headers={"kind": "pull"})
                    async for chunk in iterator:
                        assert isinstance(chunk, BinaryContent)
                        self.write_chunks[tool].append(
                            room_server_client._table_from_arrow_ipc(chunk.data)
                        )
                        yield BinaryContent(data=b"", headers={"kind": "pull"})
                    yield _ControlContent(method="close")

                return stream()

            if tool in self.read_pulls:
                assert isinstance(tool_input, AsyncIterable)

                async def stream() -> AsyncIterator[Content]:
                    iterator = tool_input.__aiter__()
                    start_chunk = await iterator.__anext__()
                    assert isinstance(start_chunk, BinaryContent)
                    self.read_starts[tool] = dict(start_chunk.headers)
                    pull_count = 0
                    async for chunk in iterator:
                        assert isinstance(chunk, BinaryContent)
                        self.read_pulls[tool].append(dict(chunk.headers))
                        pull_count += 1
                        if pull_count == 1:
                            if tool == "search":
                                yield BinaryContent(
                                    data=room_server_client._table_to_arrow_ipc(
                                        pa.table({"payload": [b"hello"]})
                                    ),
                                    headers={"kind": "data"},
                                )
                            else:
                                yield BinaryContent(
                                    data=room_server_client._table_to_arrow_ipc(
                                        pa.table(
                                            {
                                                "id": [1],
                                                "payload": [b"sql-result"],
                                            }
                                        )
                                    ),
                                    headers={"kind": "data"},
                                )
                            continue
                        yield _ControlContent(method="close")
                        return

                return stream()

            if tool in {"open_sql_query", "execute_sql"}:
                assert isinstance(tool_input, BinaryContent)
                return BinaryContent(
                    data=room_server_client._schema_to_arrow_ipc(
                        pa.table(
                            {
                                "id": [1],
                                "payload": [b"sql-result"],
                            }
                        ).schema
                    ),
                    headers={"kind": "query", "query_id": "sql-query-1"},
                )
            if tool == "execute_sql_statement":
                assert isinstance(tool_input, BinaryContent)
                return JsonContent(json={"rows_affected": 3})
            if tool == "close_sql_query":
                assert isinstance(tool_input, dict)
                return EmptyContent()
            if tool == "cancel_sql_query":
                assert isinstance(tool_input, dict)
                return JsonContent(json={"status": "cancelling"})
            if tool == "list_tables":
                return JsonContent(json={"tables": ["records"]})
            if tool == "inspect":
                return BinaryContent(
                    data=room_server_client._schema_to_arrow_ipc(inspect_schema),
                    headers={"kind": "schema"},
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
            if tool == "list_branches":
                return JsonContent(
                    json={
                        "branches": [
                            {
                                "name": "main",
                                "parent_branch": None,
                                "parent_version": None,
                                "created_at": None,
                                "manifest_size": None,
                            }
                        ]
                    }
                )
            return EmptyContent()

    room = _StreamingDatasetsRoom()
    client = DatasetsClient(room=room)  # type: ignore[arg-type]

    await client.create_table_with_schema(
        name="records",
        namespace=["team"],
        data=pa.table({"payload": [b"hello"]}),
        schema=pa.schema([pa.field("payload", pa.binary())]),
        metadata={"kind": "demo"},
    )
    await client.insert(
        table="records",
        namespace=["team"],
        records=pa.table({"payload": [b"inserted"]}),
    )
    await client.merge(
        table="records",
        namespace=["team"],
        on="id",
        records=pa.table({"id": [1], "payload": [b"merged"]}),
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
        namespace=["team"],
    )
    rows_affected = await client.execute_sql_statement(
        query="DELETE FROM records WHERE id = $id",
        namespace=["team"],
        params=pa.table({"id": [1]}),
    )
    cancel_result = await client.cancel_sql_query(query_id="sql-query-1")
    count = await client.count(table="records", namespace=["team"])
    versions = await client.list_versions(table="records", namespace=["team"])
    indexes = await client.list_indexes(table="records", namespace=["team"])
    branches = await client.list_branches(namespace=["team"])
    await client.create_branch(branch="exp", namespace=["team"])
    await client.delete_branch(branch="exp", namespace=["team"])

    assert tables == ["records"]
    assert inspected.equals(inspect_schema, check_metadata=True)
    assert rows.to_pylist() == [{"payload": b"hello"}]
    assert sql_rows.to_pylist() == [{"id": 1, "payload": b"sql-result"}]
    assert rows_affected == 3
    assert cancel_result.status == "cancelling"
    assert count == 1
    assert versions[0].metadata == {"kind": "demo"}
    assert indexes[0].name == "idx_records_id"
    assert branches[0].name == "main"

    assert [call["tool"] for call in room.calls] == [
        "create_table",
        "insert",
        "merge",
        "update",
        "list_tables",
        "inspect",
        "search",
        "execute_sql",
        "read_sql_query",
        "close_sql_query",
        "execute_sql_statement",
        "cancel_sql_query",
        "count",
        "list_versions",
        "list_indexes",
        "list_branches",
        "create_branch",
        "delete_branch",
    ]
    assert all(call["toolkit"] == "dataset" for call in room.calls)

    create_start = room.write_starts["create_table"]
    assert create_start["kind"] == "start"
    assert create_start["namespace"] == ["team"]
    assert create_start["branch"] is None
    assert create_start["metadata"] == [{"key": "kind", "value": "demo"}]
    assert room.write_chunks["create_table"][0].to_pylist() == [{"payload": b"hello"}]

    assert room.write_starts["insert"] == {
        "kind": "start",
        "table": "records",
        "namespace": ["team"],
        "branch": None,
    }
    assert room.write_chunks["insert"][0].to_pylist() == [{"payload": b"inserted"}]

    assert room.write_starts["merge"] == {
        "kind": "start",
        "table": "records",
        "on": "id",
        "namespace": ["team"],
        "branch": None,
    }
    assert room.write_chunks["merge"][0].to_pylist() == [
        {"id": 1, "payload": b"merged"}
    ]

    update_arguments = room.calls[3]["input"]
    assert update_arguments["values"] == [
        {
            "column": "payload",
            "value_json": json.dumps(
                {
                    "binary": base64.b64encode(b"hello").decode(),
                }
            ),
        }
    ]
    assert "values_sql" not in update_arguments
    assert room.read_starts["search"]["kind"] == "start"
    assert room.read_starts["search"]["table"] == "records"
    assert room.read_starts["search"]["branch"] is None
    assert room.read_starts["search"]["version"] is None
    assert room.read_pulls["search"] == [{"kind": "pull"}, {"kind": "pull"}]
    assert room.read_starts["read_sql_query"]["kind"] == "start"
    assert room.read_starts["read_sql_query"]["query_id"] == "sql-query-1"
    assert room.read_pulls["read_sql_query"] == [{"kind": "pull"}, {"kind": "pull"}]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "invoke"),
    [
        ("stat", lambda client: client.stat(path="file.txt")),
        (
            "move",
            lambda client: client.move(
                source_path="file.txt",
                destination_path="moved.txt",
            ),
        ),
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
async def test_storage_client_move_uses_room_invoke() -> None:
    room = _StreamingStorageRoom()
    client = StorageClient(room=room)  # type: ignore[arg-type]

    await client.move(
        source_path="folder/source.txt",
        destination_path="folder/destination.txt",
        overwrite=True,
    )

    assert room.requests[0] == {
        "toolkit": "storage",
        "tool": "move",
        "input": {
            "source_path": "folder/source.txt",
            "destination_path": "folder/destination.txt",
            "overwrite": True,
        },
        "caller_context": None,
    }


@pytest.mark.asyncio
async def test_storage_client_emits_moved_events() -> None:
    room = _FakeRoom()
    client = StorageClient(room=room)  # type: ignore[arg-type]
    events: list[dict[str, str]] = []

    client.on(
        "file.moved",
        lambda source_path, destination_path, participant_id: events.append(
            {
                "source_path": source_path,
                "destination_path": destination_path,
                "participant_id": participant_id,
            }
        ),
    )

    handler = room.protocol.get_handler("storage.file.moved")
    assert handler is not None
    await handler(
        room.protocol,
        0,
        "storage.file.moved",
        pack_message(
            {
                "source_path": "folder/source.txt",
                "destination_path": "folder/destination.txt",
                "participant_id": "participant-1",
            }
        ),
    )

    assert events == [
        {
            "source_path": "folder/source.txt",
            "destination_path": "folder/destination.txt",
            "participant_id": "participant-1",
        }
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


@pytest.mark.asyncio
async def test_containers_client_build_streams_tar_chunks() -> None:
    room = _StreamingBuildRoom()
    client = ContainersClient(room=room)  # type: ignore[arg-type]

    async def _chunks() -> AsyncIterator[bytes]:
        yield b"hello "
        yield b"world"

    build_id = await client.build(
        tag="repo/example:latest",
        mount_path="/context",
        context_path="/context",
        dockerfile_path="/context/Dockerfile",
        optimize_image=False,
        private=True,
        credentials=[DockerSecret(username="u", password="p")],
        builder_name="builder-1",
        chunks=_chunks(),
        size=11,
    )

    assert build_id == "build-1"
    assert room.start_chunk is not None
    assert room.start_chunk.headers == {
        "kind": "start",
        "tag": "repo/example:latest",
        "mount_path": "/context",
        "context_path": "/context",
        "dockerfile_path": "/context/Dockerfile",
        "optimize_image": False,
        "private": True,
        "credentials": [
            {"registry": None, "username": "u", "password": "p"},
        ],
        "builder_name": "builder-1",
        "size": 11,
    }
    assert [chunk.headers for chunk in room.data_chunks] == [
        {"kind": "data"},
        {"kind": "data"},
    ]
    assert b"".join(chunk.data for chunk in room.data_chunks) == b"hello world"


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
            self.build_start_chunk: BinaryContent | None = None
            self.build_data_chunks: list[BinaryContent] = []

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
            if tool == "build":
                tool_input = kwargs["input"]
                assert isinstance(tool_input, AsyncIterable)
                iterator = tool_input.__aiter__()
                start_chunk = await iterator.__anext__()
                assert isinstance(start_chunk, BinaryContent)
                self.build_start_chunk = start_chunk
                async for chunk in iterator:
                    assert isinstance(chunk, BinaryContent)
                    self.build_data_chunks.append(chunk)
                return JsonContent(json={"build_id": "build-job"})
            if tool == "list_images":
                return JsonContent(
                    json={
                        "images": [
                            {
                                "id": "img-1",
                                "preferred_ref": "demo:latest",
                                "references": ["demo:latest"],
                                "labels": [],
                                "created_at": "2026-01-01T00:00:00Z",
                                "updated_at": "2026-01-02T00:00:00Z",
                                "target_media_type": "application/vnd.oci.image.manifest.v1+json",
                            }
                        ]
                    }
                )
            if tool == "inspect_image":
                return JsonContent(
                    json={
                        "image": {
                            "id": "img-1",
                            "preferred_ref": "demo:latest",
                            "references": ["demo:latest"],
                            "labels": [{"key": "role", "value": "demo"}],
                            "created_at": "2026-01-01T00:00:00Z",
                            "updated_at": "2026-01-02T00:00:00Z",
                            "target_media_type": "application/vnd.oci.image.manifest.v1+json",
                        },
                        "target": {
                            "digest": "sha256:target",
                            "media_type": "application/vnd.oci.image.manifest.v1+json",
                            "size": 123,
                            "annotations": [],
                        },
                        "selected_manifest": {
                            "digest": "sha256:target",
                            "media_type": "application/vnd.oci.image.manifest.v1+json",
                            "size": 123,
                            "annotations": [],
                        },
                        "manifests": [],
                        "config": {
                            "digest": "sha256:config",
                            "media_type": "application/vnd.oci.image.config.v1+json",
                            "size": 45,
                            "annotations": [],
                        },
                        "layers": [
                            {
                                "digest": "sha256:layer-1",
                                "media_type": "application/vnd.oci.image.layer.v1.tar+gzip",
                                "size": 67,
                                "annotations": [],
                            }
                        ],
                        "content_size": 235,
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
        mount_path="/context",
        context_path="/workspace",
        chunks=_bytes_chunks([b"hello ", b"world"]),
        dockerfile_path="/workspace/Dockerfile",
        optimize_image=False,
        private=True,
        credentials=[DockerSecret(username="u2", password="p2")],
        builder_name="builder-1",
        size=11,
    )
    await client.run_service(service_id="svc-1", env={"A": "1"})
    images = await client.list_images()
    inspection = await client.inspect_image(image_id="img-1")
    await client.list_builds()
    await client.cancel_build(build_id="build-1")
    await client.delete_build(build_id="build-1")
    await client.list()

    assert [request["tool"] for request in room.requests] == [
        "pull_image",
        "run",
        "build",
        "run_service",
        "list_images",
        "inspect_image",
        "list_builds",
        "cancel_build",
        "delete_build",
        "list_containers",
    ]

    assert images == [
        Image(
            id="img-1",
            preferred_ref="demo:latest",
            references=["demo:latest"],
            labels={},
            created_at=datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
            updated_at=datetime(2026, 1, 2, 0, 0, tzinfo=timezone.utc),
            target_media_type="application/vnd.oci.image.manifest.v1+json",
        )
    ]
    assert inspection.image.id == "img-1"
    assert inspection.image.references == ["demo:latest"]
    assert inspection.target.digest == "sha256:target"
    assert inspection.content_size == 235

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
    assert isinstance(build_input, AsyncIterable)
    assert room.build_start_chunk is not None
    assert room.build_start_chunk.headers == {
        "kind": "start",
        "tag": "example:latest",
        "mount_path": "/context",
        "context_path": "/workspace",
        "dockerfile_path": "/workspace/Dockerfile",
        "optimize_image": False,
        "private": True,
        "credentials": [
            {"registry": None, "username": "u2", "password": "p2"},
        ],
        "builder_name": "builder-1",
        "size": 11,
    }
    assert [chunk.headers for chunk in room.build_data_chunks] == [
        {"kind": "data"},
        {"kind": "data"},
    ]

    run_service_input = room.requests[3]["input"]
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
                        data=b"build warning",
                        headers={
                            "request_id": start_chunk.headers["request_id"],
                            "build_id": start_chunk.headers["build_id"],
                            "channel": 2,
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
    assert build_logs == ["build line", "build warning"]
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
            mount_path="/context",
            context_path="/workspace",
            chunks=_bytes_chunks([b"hello"]),
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
                    "resolved_ref": "registry.meshagent.com/images/example.tar:latest",
                    "refs": ["registry.meshagent.com/images/example.tar:latest"],
                }
            )

    room = _FakeContainersRoom()
    client = ContainersClient(room=room)  # type: ignore[arg-type]

    loaded = await client.load(archive_path="/images/example.tar")

    assert loaded.resolved_ref == "registry.meshagent.com/images/example.tar:latest"
    assert loaded.refs == ["registry.meshagent.com/images/example.tar:latest"]
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
    assert room._close_watcher_task is None


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
    await _cancel_close_watcher(room)


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
    await _cancel_close_watcher(room)


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
