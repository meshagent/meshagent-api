import asyncio
import base64
from collections.abc import AsyncIterator

import pytest

from meshagent.api.messaging import (
    Content,
    ControlCloseStatus,
    ErrorContent,
    FileContent,
    JsonContent,
    TextContent,
    _ControlContent,
    pack_message,
)
from meshagent.api import ErrorCode
from meshagent.api.room_server_client import (
    AgentsClient,
    ContainersClient,
    MemoryClient,
    QueuesClient,
    RoomException,
    ServicesClient,
    StorageClient,
    SyncClient,
)
from meshagent.api.schema import ElementType, MeshSchema


class _FakeProtocol:
    def __init__(self):
        self.handlers: dict[str, object] = {}

    def register_handler(self, typ: str, handler: object) -> None:
        self.handlers[typ] = handler

    async def wait_for_close(self) -> None:
        await asyncio.Future()


def test_room_exception_defaults_to_invalid_request_code() -> None:
    ex = RoomException("boom")
    assert ex.code == ErrorCode.INVALID_REQUEST


def test_room_exception_explicit_none_code_is_preserved() -> None:
    ex = RoomException("boom", code=None)
    assert ex.code is None


class _FakeRoom:
    def __init__(self):
        self.protocol = _FakeProtocol()
        self.events: list[tuple[str, dict]] = []
        self.requests: list[tuple[str, dict, bytes | None]] = []

    def emit(self, event_name: str, **kwargs) -> None:
        self.events.append((event_name, kwargs))

    async def send_request(
        self, typ: str, request: dict, data: bytes | None = None
    ) -> JsonContent | dict:
        self.requests.append((typ, request, data))
        if typ == "agent.invoke_tool":
            await asyncio.sleep(0)
            return JsonContent(json={"ok": True})

        return {}


class _BadResponseRoom:
    async def send_request(
        self, typ: str, request: dict, data: bytes | None = None
    ) -> dict:
        del typ, request, data
        return {}


class _BadStorageResponseRoom:
    def __init__(self) -> None:
        self.protocol = _FakeProtocol()

    async def send_request(
        self, typ: str, request: dict, data: bytes | None = None
    ) -> dict:
        del typ, request, data
        return {}


class _BadSyncResponseRoom:
    def __init__(self) -> None:
        self.protocol = _FakeProtocol()

    async def send_request(
        self, typ: str, request: dict, data: bytes | None = None
    ) -> TextContent:
        del typ, request, data
        return TextContent(text="unexpected")


@pytest.mark.asyncio
async def test_tool_call_response_chunk_unpacks_json_chunk_payload() -> None:
    room = _FakeRoom()
    client = AgentsClient(room=room)  # type: ignore[arg-type]
    chunk = JsonContent(json={"hello": "world"})

    await client._handle_tool_call_response_chunk(
        protocol=room.protocol,  # type: ignore[arg-type]
        message_id=1,
        typ="agent.tool_call_response_chunk",
        data=pack_message(
            header={"tool_call_id": "tc-1", "chunk": chunk.to_json()},
            data=chunk.get_data(),
        ),
    )

    assert len(room.events) == 1
    assert room.events[0][0] == "agent.tool_call_response_chunk"
    event = room.events[0][1]["event"]
    assert isinstance(event, dict)
    assert event["tool_call_id"] == "tc-1"
    assert isinstance(event["chunk"], JsonContent)
    assert event["chunk"].json == {"hello": "world"}


@pytest.mark.asyncio
async def test_memory_client_unexpected_response_uses_error_code() -> None:
    client = MemoryClient(room=_BadResponseRoom())  # type: ignore[arg-type]

    with pytest.raises(
        RoomException, match="unexpected return type from memory.list"
    ) as ex:
        await client.list()

    assert ex.value.code == ErrorCode.UNEXPECTED_RESPONSE_TYPE


@pytest.mark.asyncio
async def test_storage_client_unexpected_response_uses_error_code() -> None:
    client = StorageClient(room=_BadStorageResponseRoom())  # type: ignore[arg-type]

    with pytest.raises(
        RoomException, match="unexpected return type from storage.exists"
    ) as ex:
        await client.exists(path="file.txt")

    assert ex.value.code == ErrorCode.UNEXPECTED_RESPONSE_TYPE


@pytest.mark.asyncio
async def test_services_client_unexpected_response_uses_error_code() -> None:
    client = ServicesClient(room=_BadResponseRoom())  # type: ignore[arg-type]

    with pytest.raises(
        RoomException, match="unexpected return type from services.list"
    ) as ex:
        await client.list_with_state()

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
async def test_tool_call_response_chunk_unpacks_file_chunk_payload() -> None:
    room = _FakeRoom()
    client = AgentsClient(room=room)  # type: ignore[arg-type]
    chunk = FileContent(
        name="step.png",
        mime_type="image/png",
        data=b"\x89PNG\r\n\x1a\n",
    )

    await client._handle_tool_call_response_chunk(
        protocol=room.protocol,  # type: ignore[arg-type]
        message_id=1,
        typ="agent.tool_call_response_chunk",
        data=pack_message(
            header={"tool_call_id": "tc-2", "chunk": chunk.to_json()},
            data=chunk.get_data(),
        ),
    )

    assert len(room.events) == 1
    assert room.events[0][0] == "agent.tool_call_response_chunk"
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
    client = AgentsClient(room=room)  # type: ignore[arg-type]
    payload = {
        "type": "agent.event",
        "headline": "waiting for page",
        "state": "in_progress",
    }

    await client._handle_tool_call_response_chunk(
        protocol=room.protocol,  # type: ignore[arg-type]
        message_id=1,
        typ="agent.tool_call_response_chunk",
        data=pack_message(
            header={"tool_call_id": "tc-3", "chunk": payload},
            data=b"ignored",
        ),
    )

    assert len(room.events) == 1
    assert room.events[0][0] == "agent.tool_call_response_chunk"
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

    assert room.requests[0][0] == "agent.invoke_tool"
    invoke_request = room.requests[0][1]
    assert isinstance(invoke_request["tool_call_id"], str)
    assert invoke_request["tool_call_id"] != ""
    assert invoke_request["arguments"] == _ControlContent(method="open").to_json()
    assert "stream" not in invoke_request
    assert "input" not in invoke_request

    request_chunks = [
        request
        for request in room.requests
        if request[0] == "agent.tool_call_request_chunk"
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
    assert room.requests[0][0] == "agent.invoke_tool"
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
    assert request[0] == "agent.invoke_tool"
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
    assert request[0] == "agent.invoke_tool"
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
        if typ == "agent.invoke_tool":
            await asyncio.sleep(0)
            return _ControlContent(method="open")
        return {}


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

    await client._handle_tool_call_response_chunk(
        protocol=room.protocol,  # type: ignore[arg-type]
        message_id=1,
        typ="agent.tool_call_response_chunk",
        data=pack_message(
            header={
                "tool_call_id": tool_call_id,
                "chunk": ErrorContent(text="recoverable").to_json(),
            }
        ),
    )
    await client._handle_tool_call_response_chunk(
        protocol=room.protocol,  # type: ignore[arg-type]
        message_id=1,
        typ="agent.tool_call_response_chunk",
        data=pack_message(
            header={
                "tool_call_id": tool_call_id,
                "chunk": TextContent(text="still running").to_json(),
            }
        ),
    )
    await client._handle_tool_call_response_chunk(
        protocol=room.protocol,  # type: ignore[arg-type]
        message_id=1,
        typ="agent.tool_call_response_chunk",
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

    await client._handle_tool_call_response_chunk(
        protocol=room.protocol,  # type: ignore[arg-type]
        message_id=1,
        typ="agent.tool_call_response_chunk",
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
    def __init__(self, *, schema_json: dict):
        super().__init__()
        self._schema_json = schema_json

    async def send_request(
        self, typ: str, request: dict, data: bytes | None = None
    ) -> JsonContent | dict:
        self.requests.append((typ, request, data))
        if typ == "room.connect":
            return JsonContent(json={"schema": self._schema_json})
        if typ == "room.describe":
            return JsonContent(json={})
        return JsonContent(json={})


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
        open_task = asyncio.create_task(client.open(path=path))

        for _ in range(100):
            if normalized_path in client._connected_documents:
                break
            await asyncio.sleep(0)
        else:
            pytest.fail("sync client did not connect document")

        assert room.requests[0][0] == "room.connect"
        assert room.requests[0][1]["path"] == normalized_path

        connected_doc = client._connected_documents[normalized_path].ref
        payload = base64.standard_b64encode(connected_doc.get_state()).decode("utf-8")

        await client._handle_sync(
            protocol=room.protocol,  # type: ignore[arg-type]
            message_id=1,
            type="room.sync",
            data=pack_message(
                header={"path": normalized_path},
                data=payload.encode("utf-8"),
            ),
        )

        doc = await asyncio.wait_for(open_task, timeout=1)
        assert doc is connected_doc

        await client.sync(path=path, data=b"YQ==")
        assert room.requests[-1][0] == "room.sync"
        assert room.requests[-1][1]["path"] == normalized_path
        assert room.requests[-1][2] == b"YQ=="

        await client.close(path=path)
        assert room.requests[-1][0] == "room.disconnect"
        assert room.requests[-1][1]["path"] == normalized_path
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

    assert room.requests[0][0] == "room.create"
    assert (
        room.requests[0][1]["path"]
        == "agents/assistant/threads/testing-chat-thread.thread"
    )
    assert room.requests[0][1]["json"] == {"thread": {"properties": []}}
    assert room.requests[0][1]["schema"] == schema.to_json()


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
