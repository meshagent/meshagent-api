import asyncio
from collections.abc import AsyncIterator

import pytest

from meshagent.api.messaging import (
    Chunk,
    FileChunk,
    JsonChunk,
    TextChunk,
    _ControlChunk,
    pack_message,
)
from meshagent.api.room_server_client import (
    AgentsClient,
    RoomException,
)


class _FakeProtocol:
    def __init__(self):
        self.handlers: dict[str, object] = {}

    def register_handler(self, typ: str, handler: object) -> None:
        self.handlers[typ] = handler

    async def wait_for_close(self) -> None:
        await asyncio.Future()


class _FakeRoom:
    def __init__(self):
        self.protocol = _FakeProtocol()
        self.events: list[tuple[str, dict]] = []
        self.requests: list[tuple[str, dict, bytes | None]] = []

    def emit(self, event_name: str, **kwargs) -> None:
        self.events.append((event_name, kwargs))

    async def send_request(
        self, typ: str, request: dict, data: bytes | None = None
    ) -> JsonChunk | dict:
        self.requests.append((typ, request, data))
        if typ == "agent.invoke_tool":
            await asyncio.sleep(0)
            return JsonChunk(json={"ok": True})

        return {}


@pytest.mark.asyncio
async def test_tool_call_response_chunk_unpacks_json_chunk_payload() -> None:
    room = _FakeRoom()
    client = AgentsClient(room=room)  # type: ignore[arg-type]
    chunk = JsonChunk(json={"hello": "world"})

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
    assert isinstance(event["chunk"], JsonChunk)
    assert event["chunk"].json == {"hello": "world"}


@pytest.mark.asyncio
async def test_tool_call_response_chunk_unpacks_file_chunk_payload() -> None:
    room = _FakeRoom()
    client = AgentsClient(room=room)  # type: ignore[arg-type]
    chunk = FileChunk(
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
    assert isinstance(chunk, FileChunk)
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
async def test_stream_tool_sends_control_chunks_for_request_stream() -> None:
    room = _FakeRoom()
    client = AgentsClient(room=room)  # type: ignore[arg-type]

    async def request_stream():
        yield JsonChunk(json={"step": 1})
        yield TextChunk(text="done")

    response = await client.stream_tool(
        toolkit="test-toolkit",
        tool="streaming-tool",
        input=request_stream(),
    )
    assert isinstance(response, JsonChunk)
    assert response.json == {"ok": True}

    assert room.requests[0][0] == "agent.invoke_tool"
    invoke_request = room.requests[0][1]
    assert isinstance(invoke_request["tool_call_id"], str)
    assert invoke_request["tool_call_id"] != ""
    assert invoke_request["arguments"] == _ControlChunk(method="open").to_json()
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
async def test_stream_tool_rejects_non_chunk_stream_items() -> None:
    room = _FakeRoom()
    client = AgentsClient(room=room)  # type: ignore[arg-type]

    async def input_stream():
        yield {"step": 1}

    with pytest.raises(
        RoomException,
        match="stream_tool input stream items must be Chunk values",
    ):
        await client.stream_tool(
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
        arguments={"a": 1},
    )

    assert isinstance(response, JsonChunk)
    assert response.json == {"ok": True}
    assert room.requests[0][0] == "agent.invoke_tool"
    assert "stream" not in room.requests[0][1]


class _OpenResponseRoom(_FakeRoom):
    async def send_request(
        self, typ: str, request: dict, data: bytes | None = None
    ) -> JsonChunk | dict:
        self.requests.append((typ, request, data))
        if typ == "agent.invoke_tool":
            await asyncio.sleep(0)
            return _ControlChunk(method="open")
        return {}


@pytest.mark.asyncio
async def test_invoke_tool_returns_stream_when_response_is_open_control_chunk() -> None:
    room = _OpenResponseRoom()
    client = AgentsClient(room=room)  # type: ignore[arg-type]

    response = await client.invoke_tool(
        toolkit="test-toolkit",
        tool="streaming-tool",
        arguments={"a": 1},
    )
    assert not isinstance(response, Chunk)
    assert isinstance(response, AsyncIterator)
    client._fail_tool_call_streams(error=RoomException("test cleanup"))


@pytest.mark.asyncio
async def test_stream_tool_returns_stream_when_response_is_open_control_chunk() -> None:
    room = _OpenResponseRoom()
    client = AgentsClient(room=room)  # type: ignore[arg-type]

    response = await client.stream_tool(
        toolkit="test-toolkit",
        tool="streaming-tool",
        input=JsonChunk(json={"a": 1}),
    )
    assert not isinstance(response, Chunk)
    assert isinstance(response, AsyncIterator)
    client._fail_tool_call_streams(error=RoomException("test cleanup"))
