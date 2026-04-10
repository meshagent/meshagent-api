import asyncio
import contextlib
import json
import logging
import mimetypes
import os
import aiohttp
from meshagent.api.protocol import Protocol, ClientProtocol
from meshagent.api.specs.service import ContainerMountSpec, ServiceSpec
from meshagent.api.websocket_protocol import WebSocketClientProtocol
from meshagent.api.participant_token import ApiScope
from pydantic import (
    BaseModel,
    Field,
    JsonValue,
    ConfigDict,
    TypeAdapter,
    ValidationError,
    field_validator,
)
from typing import (
    Optional,
    Callable,
    Dict,
    List,
    Any,
    Literal,
    Generic,
    TypeVar,
    AsyncIterator,
    Awaitable,
    Annotated,
    Union,
)
from collections.abc import AsyncIterable

import base64
import time
import traceback

from meshagent.api.chan import ChanClosed

from meshagent.api.runtime import runtime, RuntimeDocument
from meshagent.api.schema import MeshSchema
from meshagent.api.messaging import pack_message, unpack_message
from meshagent.api.participant import Participant
from meshagent.api.chan import Chan
from meshagent.api.messaging import (
    unpack_content,
    unpack_content_parts,
    BinaryContent,
    ControlCloseStatus,
    Content,
    TextContent,
    ErrorContent,
    JsonContent,
    EmptyContent,
    FileContent,
    pack_request_parts,
    ensure_content,
    _ControlContent,
)
from meshagent.api.oauth import OAuthClientConfig, ConnectorRef
from meshagent.api.error_codes import ErrorCode
import uuid

from datetime import date, datetime, timezone

from abc import ABC, abstractmethod
from dataclasses import dataclass

from meshagent.api.urls import websocket_room_url


class DatabaseValueEncoder(ABC):
    @abstractmethod
    def encode_database_value(self) -> Any:
        raise NotImplementedError


type DatabaseJsonScalarValue = None | bool | int | float | str
type DatabaseJsonValue = (
    DatabaseJsonScalarValue | list["DatabaseJsonValue"] | dict[str, "DatabaseJsonValue"]
)


def _normalize_database_json_value(
    value: Any,
    *,
    path: str = "json",
) -> DatabaseJsonValue:
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, list):
        return [
            _normalize_database_json_value(item, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(value, dict):
        normalized = dict[str, DatabaseJsonValue]()
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} object keys must be strings")
            normalized[key] = _normalize_database_json_value(
                item,
                path=f"{path}.{key}",
            )
        return normalized
    raise TypeError(f"{path} must contain only JSON-compatible values")


@dataclass(frozen=True, slots=True)
class DatabaseExpression(DatabaseValueEncoder):
    expression: str

    def __post_init__(self) -> None:
        normalized = self.expression.strip()
        if normalized == "":
            raise ValueError("database expression must not be empty")
        object.__setattr__(self, "expression", normalized)

    def encode_database_value(self) -> dict[str, str]:
        return {"expression": self.expression}


@dataclass(frozen=True, slots=True)
class DatabaseStruct(DatabaseValueEncoder):
    fields: dict[str, "DatabaseValue"]

    def __post_init__(self) -> None:
        normalized = dict[str, DatabaseValue]()
        for key, value in self.fields.items():
            if not isinstance(key, str):
                raise TypeError("database struct keys must be strings")
            normalized[key] = value
        object.__setattr__(self, "fields", normalized)

    def to_json(self) -> dict[str, Any]:
        return {key: _encode_record_value(value) for key, value in self.fields.items()}

    def encode_database_value(self) -> dict[str, dict[str, Any]]:
        return {"struct": self.to_json()}


@dataclass(frozen=True, slots=True)
class DatabaseJson(DatabaseValueEncoder):
    value: DatabaseJsonValue

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "value",
            _normalize_database_json_value(self.value),
        )

    def to_json(self) -> DatabaseJsonValue:
        return self.value

    def encode_database_value(self) -> dict[str, DatabaseJsonValue]:
        return {"json": self.value}


type DatabaseScalarValue = (
    None | bool | int | float | str | bytes | uuid.UUID | date | datetime
)
type DatabaseValue = DatabaseScalarValue | list["DatabaseValue"] | DatabaseValueEncoder
type DatabaseRecord = dict[str, DatabaseValue]
type DatabaseRows = list[DatabaseRecord]
type DatabaseRowChunks = AsyncIterable[DatabaseRows] | list[DatabaseRows]


def _parse_database_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("database date value is not valid") from exc


def _parse_database_timestamp(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError("database timestamp value is not valid") from exc


def _decode_record_value(value: Any) -> DatabaseValue:
    if isinstance(value, dict):
        if len(value) != 1:
            raise ValueError(
                "database object values must use a single-key type wrapper"
            )
        wrapper, payload = next(iter(value.items()))
        if wrapper == "binary":
            if not isinstance(payload, str):
                raise ValueError("database binary values must be base64 strings")
            return base64.b64decode(payload.encode())
        if wrapper == "uuid":
            if not isinstance(payload, str):
                raise ValueError("database uuid values must be strings")
            return uuid.UUID(payload)
        if wrapper == "expression":
            if not isinstance(payload, str):
                raise ValueError("database expression values must be strings")
            return DatabaseExpression(payload)
        if wrapper == "date":
            if not isinstance(payload, str):
                raise ValueError("database date values must be strings")
            return _parse_database_date(payload)
        if wrapper == "timestamp":
            if not isinstance(payload, str):
                raise ValueError("database timestamp values must be strings")
            return _parse_database_timestamp(payload)
        if wrapper == "list":
            if not isinstance(payload, list):
                raise ValueError("database list values must be arrays")
            return [_decode_record_value(item) for item in payload]
        if wrapper == "struct":
            if not isinstance(payload, dict):
                raise ValueError("database struct values must be objects")
            return DatabaseStruct(
                {key: _decode_record_value(item) for key, item in payload.items()}
            )
        if wrapper == "json":
            return DatabaseJson(payload)
        raise ValueError(f"unsupported database value wrapper '{wrapper}'")

    if isinstance(value, list):
        raise ValueError("database list values must use a {'list': [...]} wrapper")

    return value


def decode_records(records: list[dict[str, Any]]) -> DatabaseRows:
    decoded_records = list[dict[str, DatabaseValue]]()
    for record in records:
        if not isinstance(record, dict):
            raise ValueError("database records must be objects")
        decoded_records.append(
            {str(key): _decode_record_value(value) for key, value in record.items()}
        )
    return decoded_records


def _encode_record_value(value: DatabaseValue | Any) -> Any:
    if isinstance(value, DatabaseValueEncoder):
        return value.encode_database_value()

    if isinstance(value, bytes):
        return {"binary": base64.b64encode(value).decode()}

    if isinstance(value, uuid.UUID):
        return {"uuid": str(value)}

    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc)
        return {"timestamp": value.isoformat().replace("+00:00", "Z")}

    if isinstance(value, date):
        return {"date": value.isoformat()}

    if isinstance(value, list):
        return {"list": [_encode_record_value(item) for item in value]}

    if isinstance(value, dict):
        raise TypeError(
            "database object values must use DatabaseStruct or DatabaseJson"
        )

    return value


def encode_records(records: DatabaseRows):
    transformed_records = list[dict[str, Any]]()
    for record in records:
        transformed_records.append(
            {str(key): _encode_record_value(value) for key, value in record.items()}
        )
    return transformed_records


logger = logging.getLogger("room_server_client")
logger.setLevel(logging.WARN)


def _normalize_sync_path(path: str) -> str:
    normalized = path
    while normalized.startswith("./"):
        normalized = normalized[2:]
    while normalized.startswith("/"):
        normalized = normalized[1:]
    if normalized == ".":
        return ""
    return normalized


class RoomException(Exception):
    def __init__(
        self,
        message: str,
        *,
        status_code: int = 400,
        code: int | None = ErrorCode.INVALID_REQUEST,
    ):
        self.status_code = status_code
        self.code = code
        super().__init__(message)


class RoomAccessDeniedException(RoomException):
    def __init__(message: str):
        super().__init__(message, status_code=403)


_builtins = {
    "thread",
    "document",
    "transcript",
    "storage",
}


class Requirement(ABC):
    def __init__(
        self, *, name: str, callable: Optional[bool] = None, timeout: float = 30
    ):
        self.timeout = timeout
        self.name = name
        if callable is None and name in _builtins:
            callable = True
        else:
            callable = False

        self.callable = callable

    @staticmethod
    def from_json(r: dict) -> "Requirement":
        if "toolkit" in r:
            return RequiredToolkit(
                name=r["toolkit"], tools=r["tools"], callable=r.get("callable", None)
            )

        if "schema" in r:
            json = r.get("json")
            return RequiredSchema(
                name=r["schema"],
                schema=MeshSchema.from_json(json) if json is not None else None,
            )

        if "table" in r:
            return RequiredTable(
                name=r["table"],
                schema=r["schema"],
                namespace=r.get("namespace"),
                scalar_indexes=r.get("scalar_indexes"),
                full_text_search_indexes=r.get("full_text_search_indexes"),
                vector_indexes=r.get("vector_indexes"),
            )

        raise RoomException("invalid requirement json")

    @abstractmethod
    def to_json(self):
        pass


class _MakeCallRequest(BaseModel):
    url: str
    arguments: dict
    name: str
    api: Optional[ApiScope] = None


class RequiredToolkit(Requirement):
    # Require a toolkit to be present for this tool to execute, optionally a list of specific tools in the toolkit
    def __init__(
        self,
        *,
        name: str,
        tools: Optional[list["str"]] = None,
        callable: Optional[bool] = None,
        participant_name: Optional[str] = None,
        timeout: float = None,
    ):
        super().__init__(name=name, callable=callable, timeout=timeout)
        self.tools = tools
        self.participant_name = participant_name

    def to_json(self):
        return {
            "toolkit": self.name,
            "tools": self.tools,
            "callable": self.callable,
            "participant_name": self.participant_name,
        }


class RequiredSchema(Requirement):
    def __init__(
        self,
        *,
        name: str,
        callable: Optional[bool] = None,
        schema: Optional[MeshSchema] = None,
        timeout: float = None,
    ):
        super().__init__(name=name, callable=callable, timeout=timeout)
        self.schema = schema

    def to_json(self):
        return {
            "schema": self.name,
            "callable": self.callable,
            "json": self.schema.to_json() if self.schema is not None else None,
        }


class RequiredTable(Requirement):
    def __init__(
        self,
        *,
        name: str,
        schema: dict[str, "DataType"],
        namespace: Optional[list[str]] = None,
        scalar_indexes: Optional[list[str]] = None,
        full_text_search_indexes: Optional[list[str]] = None,
        vector_indexes: Optional[list[str]] = None,
    ):
        super().__init__(name=name)
        self.schema = schema
        self.namespace = namespace
        self.scalar_indexes = scalar_indexes
        self.full_text_search_indexes = full_text_search_indexes
        self.vector_indexes = vector_indexes

    def to_json(self):
        return {
            "table": self.name,
            "schema": self.schema,
            "namespace": self.namespace,
            "scalar_indexes": self.scalar_indexes,
            "full_text_search_indexes": self.full_text_search_indexes,
            "vector_indexes": self.vector_indexes,
        }


class _QueuedSync:
    def __init__(self, path: str, base64: str, protocol: ClientProtocol | None = None):
        self.path = path
        self.base64 = base64
        self.protocol = protocol


class _PendingRequest:
    def __init__(
        self,
        *,
        request_type: str,
        created_at: float,
        creation_trace: Optional[str] = None,
    ):
        self.request_type = request_type
        self.created_at = created_at
        self.creation_trace = creation_trace
        self.fut = asyncio.Future[dict]()


class LocalParticipant(Participant):
    def __init__(self, *, id: str, attributes: dict, protocol: ClientProtocol):
        super().__init__(id=id, attributes=attributes)
        self._protocol = protocol

    @property
    def protocol(self):
        return self._protocol

    async def set_attribute(self, name: str, value):
        self._attributes[name] = value
        await self.protocol.send("set_attributes", pack_message({name: value}))


class RemoteParticipant(Participant):
    def __init__(
        self,
        *,
        id: str,
        role: Optional[str] = None,
        attributes: Optional[dict] = None,
        online: bool | None = None,
    ):
        if attributes is None:
            attributes = {}

        if role is None:
            role = "unknown"

        self._role = role
        self._online = online

        super().__init__(id=id, attributes=attributes)

    def set_attribute(self, name: str, value):
        raise ("You can't set the attributes of another participant")

    @property
    def role(self):
        return self._role

    @property
    def online(self) -> bool | None:
        return self._online

    def _set_online(self, online: bool) -> None:
        self._online = online


class MeshDocument(RuntimeDocument):
    def __init__(self, **arguments):
        super().__init__(**arguments)
        self._synchronized = asyncio.Future()

    @property
    def synchronized(self) -> asyncio.Future:
        return self._synchronized


class FileHandle:
    def __init__(self, id: str):
        self._id = id

    @property
    def id(self):
        return self._id


class RoomMessage:
    def __init__(
        self,
        *,
        from_participant_id: str,
        type: str,
        message: dict,
        attachment: Optional[bytes] = None,
    ):
        self.from_participant_id = from_participant_id
        self.type = type
        self.message = message
        self.attachment = attachment


class _QueuedRoomMessage(RoomMessage):
    def __init__(
        self,
        *,
        from_participant_id,
        type,
        message,
        attachment=None,
        to: Participant | None,
        drop_if_offline: bool,
    ):
        super().__init__(
            from_participant_id=from_participant_id,
            type=type,
            message=message,
            attachment=attachment,
        )
        self.to = to
        self.drop_if_offline = drop_if_offline
        self.fut: asyncio.Future[bool] | None = (
            None if drop_if_offline else asyncio.Future()
        )


class RoomClient:
    def __init__(
        self,
        *,
        protocol: Optional[ClientProtocol] = None,
        session: aiohttp.ClientSession | None = None,
        oauth_token_request_handler: Optional[
            Callable[["OAuthTokenRequest"], Awaitable]
        ] = None,
    ):
        if protocol is None:
            room_name = os.getenv("MESHAGENT_ROOM")
            token = os.getenv("MESHAGENT_TOKEN")

            if room_name is not None and token is not None:
                protocol = WebSocketClientProtocol(
                    url=websocket_room_url(room_name=room_name),
                    token=token,
                    session=session,
                )

        if protocol is None:
            raise RoomException(
                "protocol or environment variables must be configured to create a room client"
            )

        self.protocol = protocol
        self.protocol.register_handler("room_ready", self._handle_ready)
        self.protocol.register_handler("room.status", self._handle_status)
        self.protocol.register_handler("connected", self._handle_participant)
        self.protocol.register_handler("__response__", self._handle_response)
        self.protocol.register_handler(
            "room.tool_call_response_chunk",
            self._handle_tool_call_response_chunk,
        )

        self._pending_requests = dict[int, _PendingRequest]()
        self._debug_pending_requests = os.getenv(
            "MESHAGENT_DEBUG_PENDING_REQUESTS", ""
        ).strip().lower() in ("1", "true", "yes", "on")
        self._debug_pending_request_stacks = os.getenv(
            "MESHAGENT_DEBUG_PENDING_REQUESTS_STACK", ""
        ).strip().lower() in ("1", "true", "yes", "on")
        self._local_participant = None
        self._ready = asyncio.Future()
        self._local_participant_ready = asyncio.Future()
        self._events = {}
        self._tool_call_streams = dict[str, _ToolCallChunkStream]()
        self._close_watcher_task: Optional[asyncio.Task[None]] = None

        self.agents = AgentsClient(room=self)
        self.storage = StorageClient(room=self)
        self.messaging = MessagingClient(room=self)
        self.sync = SyncClient(room=self)
        self.livekit = LivekitClient(room=self)
        self.developer = DeveloperClient(room=self)
        self.queues = QueuesClient(room=self)
        self.database = DatabaseClient(room=self)
        self.memory = MemoryClient(room=self)
        self.containers = ContainersClient(room=self)
        self.secrets = SecretsClient(
            room=self, oauth_token_request_handler=oauth_token_request_handler
        )
        self.services = ServicesClient(room=self)

        self._room_url = None
        self._room_name = None
        self._session_id = None

    def on(self, event_name: str, func: Callable):
        if event_name not in self._events:
            self._events[event_name] = []
        self._events[event_name].append(func)

    def emit(self, event_name, **kwargs):
        """Call all handlers associated with the given event."""
        handlers = self._events.get(event_name, [])
        for handler in handlers:
            handler(**kwargs)

    async def __aenter__(self):
        await self.protocol.__aenter__()

        async def startup():
            await self._ready

            await self.sync.start()

            await self.messaging.start()
            await self._local_participant_ready

        async def closed():
            # protect against early termination
            await self.protocol.wait_for_close()

        startup_task = asyncio.create_task(startup())
        close_task = asyncio.create_task(closed())
        try:
            done, _ = await asyncio.wait(
                [
                    startup_task,
                    close_task,
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )

            if startup_task in done:
                await startup_task
                close_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await close_task
                return self

            await close_task
            startup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await startup_task
            raise RoomException("room connection closed before the room became ready")
        except Exception:
            startup_task.cancel()
            close_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await startup_task
            with contextlib.suppress(asyncio.CancelledError):
                await close_task
            with contextlib.suppress(Exception):
                await self.sync.stop()
            with contextlib.suppress(Exception):
                await self.messaging.stop()
            with contextlib.suppress(Exception):
                await self.protocol.__aexit__(None, None, None)
            raise

    async def __aexit__(self, exc_type, exc, tb):
        self._fail_tool_call_streams(
            error=RoomException("room client was closed before tool call completed")
        )
        close_watcher = self._close_watcher_task
        self._close_watcher_task = None
        if close_watcher is not None:
            close_watcher.cancel()
        await self.sync.stop()
        await self.messaging.stop()
        await self.protocol.__aexit__(None, None, None)
        if close_watcher is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await close_watcher
        return

    @property
    def session_id(self) -> str:
        if self._session_id is None:
            raise RoomException("session_id is not available before the room is ready")

        return self._session_id

    @property
    def room_url(self) -> str:
        if self._room_url is None:
            raise RoomException("room url is not available before the room is ready")

        return self._room_url

    @property
    def room_name(self) -> str:
        if self._room_name is None:
            raise RoomException("room name is not available before the room is ready")

        return self._room_name

    # send a request, optionally with a binary trailer
    async def send_request(
        self, type: str, request: dict, data: bytes | None = None
    ) -> FileContent | None | dict | str:
        request_id = self.protocol.next_message_id()
        logger.debug("sending request %s %s", request_id, type)

        creation_trace = None
        if self._debug_pending_request_stacks:
            creation_trace = "".join(traceback.format_stack(limit=20))

        pr = _PendingRequest(
            request_type=type,
            created_at=time.monotonic(),
            creation_trace=creation_trace,
        )
        self._pending_requests[request_id] = pr

        message = pack_message(header=request, data=data)

        try:
            await self.protocol.send(type=type, data=message, message_id=request_id)
            result = await pr.fut
            logger.debug("returning response %s", type)
            return result
        except asyncio.CancelledError:
            pending = self._pending_requests.pop(request_id, None)
            if pending is not None:
                if self._debug_pending_requests and pending.creation_trace is not None:
                    logger.debug(
                        "request creation trace id=%s:\n%s",
                        request_id,
                        pending.creation_trace,
                    )
                pending.fut.cancel()
            raise
        except Exception:
            self._pending_requests.pop(request_id, None)
            raise

    async def _handle_status(
        self, protocol: Protocol, message_id: int, type: str, data: bytes
    ) -> None:
        init, _ = unpack_message(data)

        self.emit("room.status", **init)

    async def _handle_ready(
        self, protocol: Protocol, message_id: int, type: str, data: bytes
    ) -> None:
        init, _ = unpack_message(data)

        self._room_name = init["room_name"]
        self._room_url = init["room_url"]
        self._session_id = init["session_id"]

        self._ready.set_result(True)

    async def _handle_response(
        self, protocol: Protocol, message_id: int, type: str, data: bytes
    ) -> None:
        response = unpack_content(data=data)

        request_id = message_id
        if request_id in self._pending_requests:
            pr = self._pending_requests.pop(request_id)
            if pr.fut.done():
                logger.warning(
                    "late/duplicate response for completed request (id=%s type=%s cancelled=%s age=%.3fs)",
                    request_id,
                    pr.request_type,
                    pr.fut.cancelled(),
                    time.monotonic() - pr.created_at,
                )
                if self._debug_pending_requests and pr.creation_trace is not None:
                    logger.debug(
                        "request creation trace id=%s:\n%s",
                        request_id,
                        pr.creation_trace,
                    )
                return

            if isinstance(response, ErrorContent):
                try:
                    pr.fut.set_exception(
                        RoomException(response.text, code=response.code)
                    )
                except asyncio.InvalidStateError as ex:
                    logger.error(
                        "unable to set exception for request id=%s type=%s cancelled=%s",
                        request_id,
                        pr.request_type,
                        pr.fut.cancelled(),
                        exc_info=ex,
                    )
            else:
                try:
                    pr.fut.set_result(response)
                except asyncio.InvalidStateError as ex:
                    logger.error(
                        "unable to set result for request id=%s type=%s cancelled=%s",
                        request_id,
                        pr.request_type,
                        pr.fut.cancelled(),
                        exc_info=ex,
                    )
        else:
            logger.debug(
                "received a response for a request that is not pending {id}".format(
                    id=request_id
                )
            )
        return

    @property
    def local_participant(self):
        return self._local_participant

    def _on_participant_init(self, participant_id: str, attributes: dict):
        self._local_participant = LocalParticipant(
            id=participant_id, attributes=attributes, protocol=self.protocol
        )
        self._local_participant_ready.set_result(True)

    async def _handle_participant(self, protocol, message_id, msg_type, data):
        # Decode and parse the message
        message, _ = unpack_message(data)
        type = message["type"]

        if type == "init":
            participant_id = message["participantId"]
            attributes = message["attributes"]
            self._on_participant_init(participant_id, attributes)

    def _ensure_close_watcher(self) -> None:
        if self._close_watcher_task is not None:
            return

        async def watch_for_close() -> None:
            await self.protocol.wait_for_close()
            self._fail_tool_call_streams(
                error=RoomException("room client was closed before tool call completed")
            )

        self._close_watcher_task = asyncio.create_task(watch_for_close())

    def _fail_tool_call_streams(self, *, error: BaseException) -> None:
        open_streams = list(self._tool_call_streams.values())
        self._tool_call_streams.clear()
        for stream in open_streams:
            stream.close_with_error(error)

    async def _handle_tool_call_response_chunk(
        self, protocol: Protocol, message_id: int, typ: str, data: bytes
    ) -> None:
        del protocol
        del message_id
        del typ
        header, payload = unpack_message(data)
        tool_call_id = header.get("tool_call_id")
        if not isinstance(tool_call_id, str) or tool_call_id == "":
            logger.warning("ignoring tool call response chunk without tool_call_id")
            return

        chunk_payload = header.get("chunk")
        if isinstance(chunk_payload, dict) and isinstance(
            chunk_payload.get("type"), str
        ):
            try:
                chunk_payload = unpack_content_parts(
                    header=chunk_payload, payload=payload
                )
            except KeyError:
                pass
            except Exception as ex:
                logger.warning(
                    "unable to unpack tool call response chunk payload",
                    exc_info=ex,
                )

        stream = self._tool_call_streams.get(tool_call_id, None)
        if stream is not None:
            try:
                stream_chunk = (
                    chunk_payload
                    if isinstance(chunk_payload, Content)
                    else ensure_content(chunk_payload)
                )
                stream._push_chunk(stream_chunk)
            except Exception as ex:
                stream.close_with_error(
                    RoomException(f"unable to decode tool call stream chunk: {ex}")
                )

        self.emit(
            "room.tool_call_response_chunk",
            event={
                "tool_call_id": tool_call_id,
                "toolkit": header.get("toolkit"),
                "tool": header.get("tool"),
                "chunk": chunk_payload,
            },
        )

    def _make_tool_call_stream(
        self, *, tool_call_id: str, request_task: asyncio.Task[Content]
    ) -> "_ToolCallChunkStream":
        call_stream = _ToolCallChunkStream(
            tool_call_id=tool_call_id,
            task=request_task,
            on_close=lambda: self._tool_call_streams.pop(tool_call_id, None),
        )
        self._tool_call_streams[tool_call_id] = call_stream
        return call_stream

    async def _send_tool_call_request_chunk(
        self,
        *,
        tool_call_id: str,
        chunk: Content,
    ) -> None:
        request_header, request_data = pack_request_parts(chunk)
        await self.send_request(
            "room.tool_call_request_chunk",
            {"tool_call_id": tool_call_id, "chunk": request_header},
            data=request_data,
        )

    async def _stream_tool_call_request_chunks(
        self,
        *,
        tool_call_id: str,
        request_stream_parts: AsyncIterable[Content],
    ) -> None:
        # Let the invoke request be queued first to avoid early chunk races.
        await asyncio.sleep(0)
        try:
            async for item in request_stream_parts:
                if not isinstance(item, Content):
                    raise RoomException(
                        "invoke_tool input stream items must be Content values"
                    )
                await self._send_tool_call_request_chunk(
                    tool_call_id=tool_call_id,
                    chunk=item,
                )
        finally:
            await self._send_tool_call_request_chunk(
                tool_call_id=tool_call_id,
                chunk=_ControlContent(method="close"),
            )

    async def call(
        self,
        *,
        name: str,
        url: str,
        arguments: dict,
        api: Optional[ApiScope] = None,
    ) -> None:
        await self.send_request(
            "room.call",
            _MakeCallRequest(
                name=name,
                url=url,
                arguments=arguments,
                api=api,
            ).model_dump(mode="json"),
        )

    async def invoke(
        self,
        *,
        toolkit: str,
        tool: str,
        input: str | dict | Content | AsyncIterable[Content] | None = None,
        participant_id: Optional[str] = None,
        on_behalf_of_id: Optional[str] = None,
        caller_context: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Content | AsyncIterator[Content]:
        if "arguments" in kwargs and input is None:
            input = kwargs.pop("arguments")
            logger.warning(
                "invoke_tool(arguments=...) is deprecated; use invoke_tool(input=...)"
            )
        if kwargs:
            unexpected = ", ".join(sorted(kwargs.keys()))
            raise TypeError(
                f"invoke_tool() got unexpected keyword argument(s): {unexpected}"
            )
        if input is None:
            input = EmptyContent()

        resolved_tool_call_id = uuid.uuid4().hex

        request_payload: Dict[str, Any] = {
            "toolkit": toolkit,
            "tool": tool,
            "participant_id": participant_id,
            "on_behalf_of_id": on_behalf_of_id,
        }

        request_stream_task: Optional[asyncio.Task[None]] = None
        invoke_data: bytes | None = None
        if isinstance(input, AsyncIterable):
            # Request streaming starts with the initial open control chunk
            # carried in invoke arguments.
            request_header, _ = pack_request_parts(_ControlContent(method="open"))
            request_payload["arguments"] = request_header
            request_stream_task = asyncio.create_task(
                self._stream_tool_call_request_chunks(
                    tool_call_id=resolved_tool_call_id,
                    request_stream_parts=input,
                )
            )
        elif isinstance(input, (str, dict, Content)):
            input_content = ensure_content(input)
            request_header, invoke_data = pack_request_parts(input_content)
            request_payload["arguments"] = request_header
        else:
            raise RoomException(
                "invoke_tool input must be str, dict, Content, or an async iterable of Content values"
            )

        if caller_context is not None:
            request_payload["caller_context"] = caller_context

        request_payload["tool_call_id"] = resolved_tool_call_id

        self._ensure_close_watcher()
        invoke_task = asyncio.create_task(
            self.send_request(
                "room.invoke_tool",
                request_payload,
                invoke_data,
            )
        )
        call_stream = self._make_tool_call_stream(
            tool_call_id=resolved_tool_call_id, request_task=invoke_task
        )
        if request_stream_task is not None:

            def on_request_stream_done(task: asyncio.Task[None]) -> None:
                try:
                    task.result()
                except asyncio.CancelledError:
                    return
                except Exception as ex:
                    if isinstance(ex, RoomException):
                        wrapped_error = RoomException(
                            f"request stream failed: {ex}",
                            status_code=ex.status_code,
                            code=ex.code,
                        )
                    else:
                        wrapped_error = RoomException(f"request stream failed: {ex}")
                    call_stream.close_with_error(wrapped_error)

            request_stream_task.add_done_callback(on_request_stream_done)

        try:
            response = ensure_content(await invoke_task)
        except asyncio.CancelledError as ex:
            if request_stream_task is not None and not request_stream_task.done():
                request_stream_task.cancel()
                await asyncio.gather(request_stream_task, return_exceptions=True)
            if call_stream.error is not None:
                raise call_stream.error from ex
            raise
        except Exception:
            if request_stream_task is not None and not request_stream_task.done():
                request_stream_task.cancel()
                await asyncio.gather(request_stream_task, return_exceptions=True)
            raise

        if isinstance(response, _ControlContent) and response.method == "open":
            if request_stream_task is not None:
                call_stream.attach_request_stream_task(request_stream_task)
            return call_stream.stream()

        if request_stream_task is not None:
            await request_stream_task
        return response

    async def list_toolkits(
        self,
        *,
        participant_id: Optional[str] = None,
        participant_name: Optional[str] = None,
        timeout: Optional[int] = None,
    ) -> List["ToolkitDescription"]:
        """
        Fetch a list of available toolkits and parse into `ToolkitDescription` objects.
        """
        request: Dict[str, Any] = {}
        if participant_id is not None:
            request["participant_id"] = participant_id
        if participant_name is not None:
            request["participant_name"] = participant_name
        if timeout is not None:
            request["timeout"] = timeout

        response = await self.send_request("room.list_toolkits", request)
        # 'response["tools"]' is assumed to be a dict of toolkits by name
        toolkits_data = response["tools"]

        result = []
        for toolkit_name, tk_json in toolkits_data.items():
            # Parse top-level toolkit properties
            title = tk_json.get("title", "")
            description = tk_json.get("description", "")
            thumbnail_url = tk_json.get("thumbnail_url", None)
            participant_id = tk_json.get("participant_id", None)

            # Tools are usually a dict keyed by tool name
            tools = []
            raw_tools = tk_json.get("tools", {})
            for tool_name, tool_json in raw_tools.items():
                supports_context = tool_json.get(
                    "supports_context",
                    tool_json.get("supportsContext", False),
                )
                strict = tool_json.get("strict", None)
                input_spec = ToolContentSpec.from_json(tool_json.get("input_spec"))
                legacy_input_schema = tool_json.get("input_schema")
                if legacy_input_schema is not None and input_spec is None:
                    input_spec = ToolContentSpec(
                        types=["json"],
                        schema=legacy_input_schema,
                    )
                elif (
                    legacy_input_schema is not None
                    and input_spec is not None
                    and input_spec.includes("json")
                    and input_spec.schema is None
                ):
                    input_spec = ToolContentSpec(
                        types=input_spec.types,
                        stream=input_spec.stream,
                        schema=legacy_input_schema,
                    )

                output_spec = ToolContentSpec.from_json(tool_json.get("output_spec"))
                legacy_output_schema = tool_json.get("output_schema")
                if legacy_output_schema is not None and output_spec is None:
                    output_spec = ToolContentSpec(
                        types=["json"],
                        schema=legacy_output_schema,
                    )
                elif (
                    legacy_output_schema is not None
                    and output_spec is not None
                    and output_spec.includes("json")
                    and output_spec.schema is None
                ):
                    output_spec = ToolContentSpec(
                        types=output_spec.types,
                        stream=output_spec.stream,
                        schema=legacy_output_schema,
                    )

                tools.append(
                    ToolDescription(
                        name=tool_name,
                        title=tool_json.get("title", ""),
                        description=tool_json.get("description", ""),
                        input_spec=input_spec,
                        output_spec=output_spec,
                        thumbnail_url=tool_json.get("thumbnail_url", None),
                        defs=tool_json.get("defs", None),
                        pricing=tool_json.get("pricing", None),
                        supports_context=supports_context,
                        strict=strict if isinstance(strict, bool) else None,
                    )
                )

            result.append(
                ToolkitDescription(
                    name=toolkit_name,
                    title=title,
                    description=description,
                    tools=tools,
                    thumbnail_url=thumbnail_url,
                    participant_id=participant_id,
                )
            )
        return result


T = TypeVar("T")


class _RefCount(Generic[T]):
    def __init__(self, ref: T):
        self.ref = ref
        self.count = 1


class _SyncOpenStartChunkHeaders(BaseModel):
    kind: Literal["start"]
    path: str
    create: bool = True
    vector: str | None = None
    schema_value: dict[str, Any] | None = Field(default=None, alias="schema")
    schema_path: str | None = None
    initial_json: dict[str, Any] | None = None

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class _SyncOpenInputChunkHeaders(BaseModel):
    kind: Literal["sync"]

    model_config = ConfigDict(extra="forbid")


class _SyncOpenStateChunkHeaders(BaseModel):
    kind: Literal["state"]
    path: str
    schema_value: dict[str, Any] = Field(alias="schema")

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class _SyncOpenOutputChunkHeaders(BaseModel):
    kind: Literal["sync"]
    path: str

    model_config = ConfigDict(extra="forbid")


class _SyncOpenStreamState:
    _INPUT_STREAM_CLOSE = object()

    def __init__(
        self,
        *,
        path: str,
        create: bool,
        vector: str | None,
        schema: dict[str, Any] | None,
        schema_path: str | None,
        initial_json: dict[str, Any] | None,
    ) -> None:
        self._path = path
        self._create = create
        self._vector = vector
        self._schema = schema
        self._schema_path = schema_path
        self._initial_json = initial_json
        self._input_q: asyncio.Queue[BinaryContent | object] = asyncio.Queue()
        self._input_closed = False
        self._task: asyncio.Task[None] | None = None
        self._error: BaseException | None = None

    @property
    def error(self) -> BaseException | None:
        return self._error

    async def input_stream(self) -> AsyncIterator[Content]:
        yield BinaryContent(
            data=b"",
            headers={
                "kind": "start",
                "path": self._path,
                "create": self._create,
                "vector": self._vector,
                "schema": self._schema,
                "schema_path": self._schema_path,
                "initial_json": self._initial_json,
            },
        )

        while True:
            chunk = await self._input_q.get()
            if chunk is self._INPUT_STREAM_CLOSE:
                return
            if not isinstance(chunk, BinaryContent):
                raise RoomException("sync input queue produced an invalid stream chunk")
            yield chunk

    def attach_task(self, task: asyncio.Task[None]) -> None:
        self._task = task
        task.add_done_callback(self._on_task_done)

    def _on_task_done(self, task: asyncio.Task[None]) -> None:
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as ex:
            self._error = ex
        finally:
            self.close_input_stream()

    def close_input_stream(self) -> None:
        if self._input_closed:
            return
        self._input_closed = True
        self._input_q.put_nowait(self._INPUT_STREAM_CLOSE)

    def queue_sync(self, *, data: bytes) -> None:
        if self._error is not None:
            if isinstance(self._error, Exception):
                raise self._error
            raise RoomException(f"sync stream failed: {self._error}")
        if self._input_closed:
            raise RoomException(
                "attempted to sync to a document that is not connected",
                code=ErrorCode.SYNC_NOT_CONNECTED,
            )
        self._input_q.put_nowait(
            BinaryContent(
                data=data,
                headers={"kind": "sync"},
            )
        )

    async def wait(self) -> None:
        if self._task is not None:
            await self._task


class SyncClient:
    def __init__(self, *, room: RoomClient):
        self.room = room
        self._connected_documents = dict[str, _RefCount[MeshDocument]]()
        self._connecting_documents = dict[
            str, asyncio.Future[_RefCount[MeshDocument]]
        ]()
        self._document_streams = dict[str, _SyncOpenStreamState]()
        self._started = False

    @staticmethod
    def _unexpected_response_error(*, operation: str) -> RoomException:
        return RoomException(
            f"unexpected return type from sync.{operation}",
            code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
        )

    def get_open_documents(self) -> dict[str, MeshDocument]:
        open_documents = {}
        for k, v in self._connected_documents.items():
            open_documents[k] = v.ref
        return open_documents

    @staticmethod
    def _apply_sync_payload(*, doc: _RefCount[MeshDocument], payload: bytes) -> None:
        if payload:
            runtime.apply_backend_changes(doc.ref.id, payload.decode("utf-8"))
        if not doc.ref.synchronized.done():
            doc.ref.synchronized.set_result(True)

    async def start(self):
        if self._started:
            raise Exception("client already started")
        self._started = True

    async def stop(self):
        for path in list(self._connected_documents.keys()):
            ref = self._connected_documents.get(path)
            if ref is None:
                continue
            ref.count = 1
            try:
                await self.close(path=path)
            except Exception as ex:
                logger.debug("sync stream close failed for %s", path, exc_info=ex)
        self._started = False

    async def _invoke(
        self,
        *,
        operation: str,
        input: dict | Content | AsyncIterable[Content],
    ) -> Content | AsyncIterator[Content]:
        return await self.room.invoke(
            toolkit="sync",
            tool=operation,
            input=input,
        )

    async def create(
        self,
        *,
        path: str,
        json: Optional[dict] = None,
        schema: Optional[MeshSchema] = None,
    ) -> None:
        normalized_path = _normalize_sync_path(path)
        await self._invoke(
            operation="create",
            input={
                "path": normalized_path,
                "json": json,
                "schema": None if schema is None else schema.to_json(),
                "schema_path": None,
            },
        )

    async def describe(self, *, path: str, create: bool = True) -> dict:
        del create
        response = await self._invoke(
            operation="describe",
            input={
                "path": _normalize_sync_path(path),
                "schema_path": None,
            },
        )
        if not isinstance(response, JsonContent):
            raise self._unexpected_response_error(operation="describe")
        if not isinstance(response.json, dict):
            raise self._unexpected_response_error(operation="describe")
        return response.json

    async def open(
        self,
        *,
        path: str,
        create: bool = True,
        initial_json: Optional[dict] = None,
        schema: Optional[MeshSchema] = None,
    ) -> MeshDocument:
        path = _normalize_sync_path(path)
        if path in self._connecting_documents:
            await self._connecting_documents[path]

        if path in self._connected_documents:
            doc = self._connected_documents[path]
            doc.count = doc.count + 1
            return doc.ref

        # todo: add support for state vector / partial updates
        # todo: initial bytes loading

        connecting_fut = asyncio.Future[_RefCount[MeshDocument]]()

        def _consume_exception(fut: asyncio.Future[_RefCount[MeshDocument]]) -> None:
            try:
                fut.exception()
            except asyncio.CancelledError:
                pass

        connecting_fut.add_done_callback(_consume_exception)
        self._connecting_documents[path] = connecting_fut

        # if locally cached, can send state vector
        # vec = doc.get_state_vector()
        # "vector": base64.standard_b64encode(vec).decode("utf-8")
        try:
            stream_state = _SyncOpenStreamState(
                path=path,
                create=create,
                vector=None,
                schema=None if schema is None else schema.to_json(),
                schema_path=None,
                initial_json=initial_json,
            )
            response = await self._invoke(
                operation="open",
                input=stream_state.input_stream(),
            )
            if isinstance(response, Content):
                raise self._unexpected_response_error(operation="open")

            response_stream = response.__aiter__()
            try:
                first_chunk = await response_stream.__anext__()
            except StopAsyncIteration as exc:
                raise RoomException(
                    "sync.open stream closed before the initial document state was returned",
                    code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
                ) from exc

            if isinstance(first_chunk, ErrorContent):
                raise RoomException(first_chunk.text, code=first_chunk.code)
            if not isinstance(first_chunk, BinaryContent):
                raise self._unexpected_response_error(operation="open")

            try:
                state_headers = _SyncOpenStateChunkHeaders.model_validate(
                    first_chunk.headers
                )
            except ValidationError as exc:
                raise self._unexpected_response_error(operation="open") from exc

            if _normalize_sync_path(state_headers.path) != path:
                raise RoomException(
                    "sync.open stream returned a mismatched path",
                    code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
                )

            def publish_sync(base64: str) -> None:
                try:
                    stream_state.queue_sync(data=base64.encode("utf-8"))
                except Exception as ex:
                    logger.debug(
                        "dropping sync for closed document stream %s",
                        path,
                        exc_info=ex,
                    )

            doc: MeshDocument = runtime.new_document(
                schema=MeshSchema.from_json(state_headers.schema_value),
                on_document_sync=publish_sync,
                factory=MeshDocument,
            )

            ref = _RefCount(doc)
            self._connected_documents[path] = ref
            self._document_streams[path] = stream_state
            self._apply_sync_payload(doc=ref, payload=first_chunk.data)
            stream_state.attach_task(
                asyncio.create_task(
                    self._consume_open_stream(
                        path=path,
                        doc=ref,
                        response_stream=response_stream,
                        stream_state=stream_state,
                    )
                )
            )
            connecting_fut.set_result(ref)
            self._connecting_documents.pop(path)

            logger.info("Connected to %s", path)

        except asyncio.CancelledError:
            raise

        except Exception as e:
            if "stream_state" in locals():
                stream_state.close_input_stream()
            connecting_fut.set_exception(e)
            self._connecting_documents.pop(path)
            raise

        await doc.synchronized
        return doc

    async def close(self, *, path: str) -> None:
        path = _normalize_sync_path(path)
        if path not in self._connected_documents:
            raise RoomException(
                "Not connected to " + path,
                code=ErrorCode.SYNC_NOT_CONNECTED,
            )

        ref = self._connected_documents[path]
        ref.count = ref.count - 1
        if ref.count == 0:
            doc = self._connected_documents.pop(path)
            stream_state = self._document_streams.pop(path, None)
            if stream_state is not None:
                stream_state.close_input_stream()
                try:
                    await stream_state.wait()
                finally:
                    runtime._unregister_document(doc=doc.ref)
            else:
                runtime._unregister_document(doc=doc.ref)

    async def sync(self, *, path: str, data: bytes) -> None:
        path = _normalize_sync_path(path)
        if path not in self._connected_documents:
            raise RoomException(
                "attempted to sync to a document that is not connected",
                code=ErrorCode.SYNC_NOT_CONNECTED,
            )
        stream_state = self._document_streams.get(path)
        if stream_state is None:
            raise RoomException(
                "attempted to sync to a document that is not connected",
                code=ErrorCode.SYNC_NOT_CONNECTED,
            )
        stream_state.queue_sync(data=data)

    async def _consume_open_stream(
        self,
        *,
        path: str,
        doc: _RefCount[MeshDocument],
        response_stream: AsyncIterator[Content],
        stream_state: _SyncOpenStreamState,
    ) -> None:
        try:
            async for chunk in response_stream:
                if isinstance(chunk, ErrorContent):
                    raise RoomException(chunk.text, code=chunk.code)
                if isinstance(chunk, _ControlContent):
                    if chunk.method == "close":
                        return
                    raise self._unexpected_response_error(operation="open")
                if not isinstance(chunk, BinaryContent):
                    raise self._unexpected_response_error(operation="open")

                try:
                    chunk_headers = _SyncOpenOutputChunkHeaders.model_validate(
                        chunk.headers
                    )
                except ValidationError as exc:
                    raise self._unexpected_response_error(operation="open") from exc

                if _normalize_sync_path(chunk_headers.path) != path:
                    raise RoomException(
                        "sync.open stream returned a mismatched path",
                        code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
                    )

                try:
                    self._apply_sync_payload(doc=doc, payload=chunk.data)
                except ChanClosed:
                    pass
        finally:
            stream_state.close_input_stream()

    async def _handle_sync(
        self, protocol: Protocol, message_id: int, type: str, data: bytes
    ) -> None:
        del protocol, message_id, type, data
        logger.debug("ignoring legacy room.sync message for streamed sync client")


ToolContentType = Literal[
    "binary",
    "json",
    "text",
    "file",
    "link",
    "empty",
]
_SUPPORTED_TOOL_CONTENT_KINDS: set[str] = {
    "binary",
    "json",
    "text",
    "file",
    "link",
    "empty",
}


class ToolContentSpec:
    def __init__(
        self,
        *,
        types: list[ToolContentType],
        stream: bool = False,
        schema: dict | None = None,
    ):
        if not isinstance(types, list) or not all(
            isinstance(item, str) for item in types
        ):
            raise TypeError("types must be a list of supported content type strings")
        if len(types) == 0:
            raise ValueError("types must include at least one content type")
        unsupported = [
            item for item in types if item not in _SUPPORTED_TOOL_CONTENT_KINDS
        ]
        if len(unsupported) > 0:
            unsupported_list = ", ".join(sorted(set(unsupported)))
            raise ValueError(f"unsupported tool content type(s): {unsupported_list}")
        if not isinstance(stream, bool):
            raise TypeError("stream must be a boolean")
        if schema is not None and not isinstance(schema, dict):
            raise TypeError("schema must be an object when provided")

        self.types = [*types]
        self.stream = stream
        self.schema = schema

    def includes(self, content_type: ToolContentType) -> bool:
        return content_type in self.types

    def to_json(self) -> dict:
        value = {"types": [*self.types], "stream": self.stream}
        if self.schema is not None:
            value["schema"] = self.schema
        return value

    @staticmethod
    def from_json(value: dict | None) -> "ToolContentSpec | None":
        if value is None:
            return None
        if not isinstance(value, dict):
            raise TypeError("tool content type descriptor must be an object")

        raw_types = value.get("types", None)
        if not isinstance(raw_types, list) or not all(
            isinstance(item, str) for item in raw_types
        ):
            raise TypeError("tool content type descriptor requires a string[] 'types'")
        unsupported = [
            item for item in raw_types if item not in _SUPPORTED_TOOL_CONTENT_KINDS
        ]
        if len(unsupported) > 0:
            unsupported_list = ", ".join(sorted(set(unsupported)))
            raise ValueError(f"unsupported tool content type(s): {unsupported_list}")

        raw_stream = value.get("stream", False)
        if not isinstance(raw_stream, bool):
            raise TypeError("tool content type descriptor 'stream' must be a boolean")

        raw_schema = value.get("schema", None)
        if raw_schema is not None and not isinstance(raw_schema, dict):
            raise TypeError("tool content type descriptor 'schema' must be an object")

        return ToolContentSpec(types=[*raw_types], stream=raw_stream, schema=raw_schema)


class ToolDescription:
    def __init__(
        self,
        *,
        name: str,
        title: str,
        description: str,
        input_spec: ToolContentSpec | None = None,
        output_spec: ToolContentSpec | None = None,
        thumbnail_url: Optional[str] = None,
        defs: Optional[dict] = None,
        pricing: Optional[str] = None,
        supports_context: Optional[bool] = False,
        strict: Optional[bool] = None,
    ):
        self.name = name
        self.title = title
        self.description = description
        self.input_spec = input_spec
        self.output_spec = output_spec

        self.thumbnail_url = thumbnail_url
        self.defs = defs
        self.pricing = pricing
        if supports_context is None:
            supports_context = False
        self.supports_context = supports_context
        self.strict = strict

    @property
    def input_schema(self) -> dict | None:
        if self.input_spec is None:
            return None
        return self.input_spec.schema

    @property
    def output_schema(self) -> dict | None:
        if self.output_spec is None:
            return None
        return self.output_spec.schema

    def to_json(self):
        return {
            "name": self.name,
            "description": self.description,
            "title": self.title,
            "thumbnail_url": self.thumbnail_url,
            "input_spec": None
            if self.input_spec is None
            else self.input_spec.to_json(),
            "output_spec": None
            if self.output_spec is None
            else self.output_spec.to_json(),
            "defs": self.defs,
            "pricing": self.pricing,
            "supports_context": self.supports_context,
            "strict": self.strict,
        }


class ToolkitDescription:
    def __init__(
        self,
        *,
        name: str,
        title: str,
        description: str,
        tools: List[ToolDescription],
        thumbnail_url: Optional[str] = None,
        participant_id: Optional[str] = None,
    ):
        self.name = name
        self.title = title
        self.description = description
        self.tools = tools
        self.thumbnail_url = thumbnail_url
        self.participant_id = participant_id

    def get_tool(self, name: str) -> ToolDescription | None:
        for t in self.tools:
            if t.name == name:
                return t

        return None

    def to_json(self):
        return {
            "name": self.name,
            "description": self.description,
            "title": self.title,
            "thumbnail_url": self.thumbnail_url,
            "tools": list(map(lambda x: x.to_json(), self.tools)),
            "participant_id": self.participant_id,
        }


class ServiceRuntimeState(BaseModel):
    service_id: str
    state: str
    container_id: Optional[str] = None
    restart_scheduled_at: Optional[float] = None
    started_at: Optional[float] = None
    restart_count: int = 0
    last_exit_code: Optional[int] = None
    last_exit_at: Optional[float] = None


class ListServicesResult(BaseModel):
    services: list[ServiceSpec]
    service_states: Dict[str, ServiceRuntimeState] = Field(default_factory=dict)


class _ListServicesResponse(ListServicesResult):
    pass


class _ListServicesToolkitResponse(BaseModel):
    services_json: list[str] = Field(default_factory=list)
    service_states: list[ServiceRuntimeState] = Field(default_factory=list)


class _LivekitConnectionInfoResponse(BaseModel):
    url: str
    token: str


class ServicesClient:
    def __init__(self, *, room: RoomClient):
        self.room = room

    @staticmethod
    def _unexpected_response_error(*, operation: str) -> RoomException:
        return RoomException(
            f"unexpected return type from services.{operation}",
            code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
        )

    async def list(
        self,
    ) -> List[ServiceSpec]:
        """
        Fetch a list of services.
        """

        return (await self.list_with_state()).services

    async def list_with_state(
        self,
    ) -> ListServicesResult:
        """
        Fetch a list of services plus runtime state details from the service controller.
        """

        response = await self.room.invoke(
            toolkit="services",
            tool="list",
            input={},
        )

        if not isinstance(response, JsonContent):
            raise self._unexpected_response_error(operation="list")

        try:
            payload = _ListServicesToolkitResponse.model_validate(response.json)
        except ValidationError as exc:
            raise self._unexpected_response_error(operation="list") from exc

        try:
            services = [
                ServiceSpec.model_validate_json(service_json)
                for service_json in payload.services_json
            ]
        except ValidationError as exc:
            raise self._unexpected_response_error(operation="list") from exc

        return ListServicesResult(
            services=services,
            service_states={
                state.service_id: state for state in payload.service_states
            },
        )

    async def restart(self, *, service_id: str) -> None:
        """
        Restart a managed room service by service id.
        """
        await self.room.invoke(
            toolkit="services",
            tool="restart",
            input={"service_id": service_id},
        )


class _ToolCallChunkStream:
    def __init__(
        self,
        *,
        tool_call_id: str,
        task: asyncio.Task[Content],
        on_close: Callable[[], None],
    ):
        self._tool_call_id = tool_call_id
        self._task = task
        self._on_close = on_close
        self._queue = asyncio.Queue[Optional[Content]]()
        self._error: Optional[BaseException] = None
        self._closed = False
        self._opened = False
        self._request_stream_task: Optional[asyncio.Task[None]] = None
        task.add_done_callback(self._on_task_done)

    @property
    def tool_call_id(self) -> str:
        return self._tool_call_id

    @property
    def result(self) -> asyncio.Future[Content]:
        return asyncio.ensure_future(self._task)

    @property
    def error(self) -> Optional[BaseException]:
        return self._error

    def __await__(self):
        return self.result.__await__()

    def attach_request_stream_task(self, task: asyncio.Task[None]) -> None:
        self._request_stream_task = task

    async def _drain_request_stream_task(self, task: asyncio.Task[None]) -> None:
        await asyncio.gather(task, return_exceptions=True)

    def _close(
        self,
        *,
        error: Optional[BaseException] = None,
        result: Optional[Content] = None,
    ) -> None:
        if self._closed:
            return

        self._closed = True

        if error is not None:
            self._error = error
        elif result is not None:
            self._queue.put_nowait(result)

        self._queue.put_nowait(None)
        if (
            self._request_stream_task is not None
            and not self._request_stream_task.done()
        ):
            self._request_stream_task.cancel()
            asyncio.create_task(
                self._drain_request_stream_task(self._request_stream_task)
            )
        try:
            self._on_close()
        except Exception as ex:
            logger.error("tool call stream cleanup failed", exc_info=ex)

    def close_with_error(self, error: BaseException) -> None:
        self._close(error=error)
        if not self._task.done():
            self._task.cancel()

    async def cancel(self) -> None:
        request_stream_task = self._request_stream_task
        task = self._task
        self._close()
        if not task.done():
            task.cancel()

        awaitables: list[asyncio.Future[Any] | asyncio.Task[Any]] = []
        if request_stream_task is not None and not request_stream_task.done():
            awaitables.append(request_stream_task)
        if not task.done():
            awaitables.append(task)
        if awaitables:
            await asyncio.gather(*awaitables, return_exceptions=True)

    def _on_task_done(self, task: asyncio.Task[Content]) -> None:
        if self._closed:
            return

        try:
            result = task.result()
            if isinstance(result, _ControlContent) and result.method == "open":
                self._opened = True
                return
            if (
                isinstance(result, _ControlContent)
                and result.method == "close"
                and result.status_code is not None
                and result.status_code != ControlCloseStatus.NORMAL
            ):
                detail = result.message or "tool call stream closed abnormally"
                self._close(
                    error=RoomException(
                        detail,
                        status_code=result.status_code,
                    )
                )
                return
            self._close(result=result)
        except asyncio.CancelledError:
            self._close()
        except Exception as ex:
            self._close(error=ex)

    def _push_chunk(self, response_chunk: Content) -> None:
        if self._closed:
            return

        if (
            isinstance(response_chunk, _ControlContent)
            and response_chunk.method == "close"
        ):
            if (
                response_chunk.status_code is not None
                and response_chunk.status_code != ControlCloseStatus.NORMAL
            ):
                detail = response_chunk.message or "tool call stream closed abnormally"
                self._close(
                    error=RoomException(
                        detail,
                        status_code=response_chunk.status_code,
                    )
                )
            else:
                self._close(result=response_chunk)
            return

        self._queue.put_nowait(response_chunk)

    async def __aiter__(self) -> AsyncIterator[Content]:
        while True:
            item = await self._queue.get()
            if item is None:
                if self._error is not None:
                    raise self._error
                return
            yield item

    def stream(self) -> AsyncIterator[Content]:
        async def wrapped() -> AsyncIterator[Content]:
            try:
                async for item in self:
                    yield item
            finally:
                await self.cancel()

        return wrapped()


class AgentsClient:
    def __init__(self, *, room: RoomClient):
        self.room = room

    def _ensure_close_watcher(self) -> None:
        self.room._ensure_close_watcher()

    def _fail_tool_call_streams(self, *, error: BaseException) -> None:
        self.room._fail_tool_call_streams(error=error)

    async def _handle_tool_call_response_chunk(
        self, protocol: Protocol, message_id: int, typ: str, data: bytes
    ) -> None:
        await self.room._handle_tool_call_response_chunk(
            protocol=protocol,
            message_id=message_id,
            typ=typ,
            data=data,
        )

    async def make_call(
        self, *, name: str, url: str, arguments: dict, api: Optional[ApiScope] = None
    ) -> None:
        await self.room.call(
            name=name,
            url=url,
            arguments=arguments,
            api=api,
        )
        return None

    def _make_tool_call_stream(
        self, *, tool_call_id: str, request_task: asyncio.Task[Content]
    ) -> _ToolCallChunkStream:
        return self.room._make_tool_call_stream(
            tool_call_id=tool_call_id,
            request_task=request_task,
        )

    async def _send_tool_call_request_chunk(
        self,
        *,
        tool_call_id: str,
        chunk: Content,
    ) -> None:
        await self.room._send_tool_call_request_chunk(
            tool_call_id=tool_call_id,
            chunk=chunk,
        )

    async def _stream_tool_call_request_chunks(
        self,
        *,
        tool_call_id: str,
        request_stream_parts: AsyncIterable[Content],
    ) -> None:
        await self.room._stream_tool_call_request_chunks(
            tool_call_id=tool_call_id,
            request_stream_parts=request_stream_parts,
        )

    async def invoke_tool(
        self,
        *,
        toolkit: str,
        tool: str,
        input: str | dict | Content | AsyncIterable[Content] | None = None,
        participant_id: Optional[str] = None,
        on_behalf_of_id: Optional[str] = None,
        caller_context: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Content | AsyncIterator[Content]:
        return await self.room.invoke(
            toolkit=toolkit,
            tool=tool,
            input=input,
            participant_id=participant_id,
            on_behalf_of_id=on_behalf_of_id,
            caller_context=caller_context,
            **kwargs,
        )

    async def list_toolkits(
        self,
        *,
        participant_id: Optional[str] = None,
        participant_name: Optional[str] = None,
        timeout: Optional[int] = None,
    ) -> List[ToolkitDescription]:
        """
        Fetch a list of available toolkits and parse into `ToolkitDescription` objects.
        """
        return await self.room.list_toolkits(
            participant_id=participant_id,
            participant_name=participant_name,
            timeout=timeout,
        )


class LivekitConnectionInfo:
    def __init__(self, *, url: str, token: str):
        self.url = url
        self.token = token


class LivekitClient:
    def __init__(self, *, room: RoomClient):
        self.room = room

    @staticmethod
    def _unexpected_response_error() -> RoomException:
        return RoomException(
            "unexpected return type from livekit.connect",
            code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
        )

    async def get_connection_info(
        self, *, breakout_room: Optional[str] = None
    ) -> LivekitConnectionInfo:
        response = await self.room.invoke(
            toolkit="livekit",
            tool="connect",
            input={"breakout_room": breakout_room},
        )
        if not isinstance(response, JsonContent):
            raise self._unexpected_response_error()

        try:
            payload = _LivekitConnectionInfoResponse.model_validate(response.json)
        except ValidationError as exc:
            raise self._unexpected_response_error() from exc

        return LivekitConnectionInfo(
            url=payload.url,
            token=payload.token,
        )


class StorageEntry(BaseModel):
    name: str
    is_folder: bool
    size: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class StorageClient:
    """
    An API for managing files and folders within a remote storage system.
    Methods are all async and must be awaited.
    """

    def __init__(self, *, room: RoomClient):
        self.room = room
        self._events = {}
        room.protocol.register_handler("storage.file.deleted", self._on_file_deleted)
        room.protocol.register_handler("storage.file.moved", self._on_file_moved)
        room.protocol.register_handler("storage.file.updated", self._on_file_updated)

    @staticmethod
    def _unexpected_response_error(*, operation: str) -> RoomException:
        return RoomException(
            f"unexpected return type from storage.{operation}",
            code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
        )

    def on(self, event_name: str, func: Callable):
        if event_name not in self._events:
            self._events[event_name] = []
        self._events[event_name].append(func)

    def off(self, event_name: str, func: Callable):
        if event_name in self._events:
            self._events[event_name].remove(func)

    def emit(self, event_name: str, **kwargs):
        """Call all handlers associated with the given event."""
        handlers = self._events.get(event_name, [])
        for handler in handlers:
            handler(**kwargs)

    async def _on_file_deleted(self, protocol, message_id, msg_type, data):
        payload, _ = unpack_message(data)
        self.emit(
            "file.deleted",
            path=payload["path"],
            participant_id=payload["participant_id"],
        )

    async def _on_file_updated(self, protocol, message_id, msg_type, data):
        payload, _ = unpack_message(data)
        self.emit(
            "file.updated",
            path=payload["path"],
            participant_id=payload["participant_id"],
        )

    async def _on_file_moved(self, protocol, message_id, msg_type, data):
        payload, _ = unpack_message(data)
        self.emit(
            "file.moved",
            source_path=payload["source_path"],
            destination_path=payload["destination_path"],
            participant_id=payload["participant_id"],
        )

    async def _invoke(
        self,
        *,
        operation: str,
        input: dict | Content,
        caller_context: Optional[dict[str, Any]] = None,
    ) -> Content:
        response = await self.room.invoke(
            toolkit="storage",
            tool=operation,
            input=input,
            caller_context=caller_context,
        )
        if not isinstance(response, Content):
            raise self._unexpected_response_error(operation=operation)
        return response

    async def exists(self, *, path: str):
        """
        Determines whether a file or folder exists at the specified path.

        Arguments:
            path (str): The path to the file or folder.

        Returns:
            bool: True if the file or folder exists, otherwise False.

        Example:
            if await storage_client.exists(path="folder/data.json"):
                print("Data file exists!")
        """

        response = await self._invoke(operation="exists", input={"path": path})
        if not isinstance(response, JsonContent):
            raise self._unexpected_response_error(operation="exists")
        return response.json["exists"]

    async def stat(self, *, path: str) -> StorageEntry | None:
        response = await self._invoke(operation="stat", input={"path": path})
        if not isinstance(response, JsonContent):
            raise self._unexpected_response_error(operation="stat")
        payload = response.json
        exists = payload["exists"]
        if not exists:
            return None
        else:
            return StorageEntry(
                name=payload["name"],
                is_folder=payload["is_folder"],
                size=payload.get("size"),
                created_at=datetime.fromisoformat(payload["created_at"])
                if payload.get("created_at") is not None
                else None,
                updated_at=datetime.fromisoformat(payload["updated_at"])
                if payload.get("updated_at") is not None
                else None,
            )

    async def move(
        self,
        *,
        source_path: str,
        destination_path: str,
        overwrite: bool = False,
    ) -> None:
        await self._invoke(
            operation="move",
            input={
                "source_path": source_path,
                "destination_path": destination_path,
                "overwrite": overwrite,
            },
        )

    @staticmethod
    def _default_upload_name(*, path: str, name: str | None) -> str:
        if isinstance(name, str) and name != "":
            return name
        return os.path.basename(path)

    @staticmethod
    def _default_upload_mime_type(*, name: str, mime_type: str | None) -> str:
        if isinstance(mime_type, str) and mime_type != "":
            return mime_type
        guessed_mime_type, _ = mimetypes.guess_type(name)
        if guessed_mime_type is None:
            return "application/octet-stream"
        return guessed_mime_type

    async def upload_stream(
        self,
        *,
        path: str,
        chunks: AsyncIterable[bytes],
        overwrite: bool = False,
        chunk_size: int = 64 * 1024,
        size: int | None = None,
        name: str | None = None,
        mime_type: str | None = None,
    ) -> None:
        resolved_name = self._default_upload_name(path=path, name=name)
        resolved_mime_type = self._default_upload_mime_type(
            name=resolved_name,
            mime_type=mime_type,
        )
        input_stream = _StorageUploadInputStream(
            path=path,
            overwrite=overwrite,
            chunks=chunks,
            chunk_size=chunk_size,
            size=size,
            name=resolved_name,
            mime_type=resolved_mime_type,
        )
        response = await self.room.invoke(
            toolkit="storage",
            tool="upload",
            input=input_stream,
        )
        if isinstance(response, Content) or not isinstance(response, AsyncIterable):
            input_stream.close()
            raise self._unexpected_response_error(operation="upload")

        try:
            async for chunk in response:
                if isinstance(chunk, ErrorContent):
                    raise RoomException(chunk.text, code=chunk.code)
                if isinstance(chunk, _ControlContent):
                    if chunk.method == "close":
                        return
                    raise self._unexpected_response_error(operation="upload")
                if not isinstance(chunk, BinaryContent):
                    raise self._unexpected_response_error(operation="upload")
                if chunk.headers.get("kind") != "pull":
                    raise self._unexpected_response_error(operation="upload")
                raw_chunk_size = chunk.headers.get("chunk_size")
                input_stream.request_next(
                    raw_chunk_size
                    if isinstance(raw_chunk_size, int) and raw_chunk_size > 0
                    else None
                )
        finally:
            input_stream.close()

    async def upload(
        self,
        *,
        path: str,
        data: bytes,
        overwrite: bool = False,
        name: str | None = None,
        mime_type: str | None = None,
    ) -> None:
        """
        Uploads binary data to a storage path.

        Arguments:
            path (str): The destination file path.
            data (bytes): The data to be written.
            overwrite (bool): Whether to overwrite an existing file.
            name (str | None): Optional file name metadata for the upload stream.
            mime_type (str | None): Optional MIME type metadata for the upload stream.

        Returns:
            None

        Example:
            data_to_write = b"Sample data"
            await storage_client.upload(
                path="files/new.txt",
                data=data_to_write,
                overwrite=True,
            )
        """

        async def single_chunk() -> AsyncIterator[bytes]:
            yield data

        await self.upload_stream(
            path=path,
            chunks=single_chunk(),
            overwrite=overwrite,
            size=len(data),
            name=name,
            mime_type=mime_type,
        )

    async def download_stream(
        self, *, path: str, chunk_size: int = 64 * 1024
    ) -> AsyncIterator[BinaryContent]:
        input_stream = _StorageDownloadInputStream(path=path, chunk_size=chunk_size)
        response = await self.room.invoke(
            toolkit="storage",
            tool="download",
            input=input_stream,
        )
        if isinstance(response, Content) or not isinstance(response, AsyncIterable):
            input_stream.close()
            raise self._unexpected_response_error(operation="download")

        response_stream = response
        metadata_received = False
        expected_size: int | None = None
        bytes_received = 0
        try:
            async for chunk in response_stream:
                if isinstance(chunk, ErrorContent):
                    raise RoomException(chunk.text, code=chunk.code)
                if isinstance(chunk, _ControlContent):
                    if chunk.method == "close":
                        if not metadata_received:
                            raise self._unexpected_response_error(operation="download")
                        if expected_size is None or bytes_received != expected_size:
                            raise self._unexpected_response_error(operation="download")
                        return
                    raise self._unexpected_response_error(operation="download")
                if not isinstance(chunk, BinaryContent):
                    raise self._unexpected_response_error(operation="download")

                kind = chunk.headers.get("kind")
                if kind == "start":
                    if metadata_received:
                        raise self._unexpected_response_error(operation="download")
                    chunk_name = chunk.headers.get("name")
                    chunk_mime_type = chunk.headers.get("mime_type")
                    chunk_size_value = chunk.headers.get("size")
                    if (
                        not isinstance(chunk_name, str)
                        or not isinstance(chunk_mime_type, str)
                        or not isinstance(chunk_size_value, int)
                        or chunk_size_value < 0
                    ):
                        raise self._unexpected_response_error(operation="download")
                    metadata_received = True
                    expected_size = chunk_size_value
                    yield chunk
                    if expected_size > 0:
                        input_stream.request_next()
                    continue

                if kind != "data" or not metadata_received or expected_size is None:
                    raise self._unexpected_response_error(operation="download")

                bytes_received += len(chunk.data)
                if bytes_received > expected_size:
                    raise self._unexpected_response_error(operation="download")
                yield chunk
                if bytes_received < expected_size:
                    input_stream.request_next()
        finally:
            input_stream.close()

    async def download(self, *, path: str) -> FileContent:
        """
        Retrieves the content of a file from the remote storage system.

        Arguments:
            path (str): The file path to download.

        Returns:
            FileContent: A response containing the downloaded data.

        Example:
            file_response = await storage_client.download(path="files/data.bin")
            print(file_response.data)  # raw bytes
        """
        file_name: str | None = None
        mime_type: str | None = None
        expected_size: int | None = None
        bytes_received = 0
        chunks = bytearray()
        async for chunk in self.download_stream(path=path):
            kind = chunk.headers.get("kind")
            if kind == "start":
                chunk_name = chunk.headers.get("name")
                chunk_mime_type = chunk.headers.get("mime_type")
                chunk_size_value = chunk.headers.get("size")
                if (
                    not isinstance(chunk_name, str)
                    or not isinstance(chunk_mime_type, str)
                    or not isinstance(chunk_size_value, int)
                    or chunk_size_value < 0
                ):
                    raise self._unexpected_response_error(operation="download")
                file_name = chunk_name
                mime_type = chunk_mime_type
                expected_size = chunk_size_value
                continue

            if kind != "data":
                raise self._unexpected_response_error(operation="download")
            chunks.extend(chunk.data)
            bytes_received += len(chunk.data)

        if file_name is None or mime_type is None or expected_size is None:
            raise self._unexpected_response_error(operation="download")
        if bytes_received != expected_size:
            raise self._unexpected_response_error(operation="download")

        return FileContent(data=bytes(chunks), name=file_name, mime_type=mime_type)

    async def download_url(self, *, path: str) -> str:
        """
        Requests a downloadable URL for the specified file path.
        This URL may be an HTTP or WebSocket-based link,
        depending on server implementation.

        Arguments:
            path (str): The file path.

        Returns:
            str: A URL string for downloading the file.

        Example:
            url = await storage_client.download_url(path="files/report.pdf")
            print("Download using:", url)
        """

        response = await self._invoke(operation="download_url", input={"path": path})
        if not isinstance(response, JsonContent):
            raise self._unexpected_response_error(operation="download_url")
        return response["url"]

    async def list(self, *, path: str) -> list[StorageEntry]:
        """
        Lists files and folders at the specified path.

        Arguments:
            path (str): The folder path to list.

        Returns:
            list[StorageEntry]: A list of storage entries,
                                where each entry has a name and is_folder flag.

        Example:
            entries = await storage_client.list(path="folder")
            for e in entries:
                print(e.name, e.is_folder)
        """

        response = await self._invoke(operation="list", input={"path": path})
        if not isinstance(response, JsonContent):
            raise self._unexpected_response_error(operation="list")
        return list(
            map(
                lambda f: StorageEntry(
                    name=f["name"],
                    is_folder=f["is_folder"],
                    size=f.get("size"),
                    created_at=datetime.fromisoformat(f["created_at"])
                    if f.get("created_at") is not None
                    else None,
                    updated_at=datetime.fromisoformat(f["updated_at"])
                    if f.get("updated_at") is not None
                    else None,
                ),
                response["files"],
            )
        )

    async def delete(self, path: str, recursive: Optional[bool] = None):
        """
        Deletes a file  at the given path.

        Arguments:
            path (str): The file to delete.

        Returns:
            None

        Example:
            await storage_client.delete("folder/old_file.txt")
        """

        await self._invoke(
            operation="delete",
            input={"path": path, "recursive": recursive},
        )


class _StorageDownloadInputStream:
    def __init__(self, *, path: str, chunk_size: int):
        self._path = path
        self._chunk_size = chunk_size
        self._closed = asyncio.Event()
        self._pulls: asyncio.Queue[object] = asyncio.Queue()

    def request_next(self) -> None:
        if self._closed.is_set():
            return
        self._pulls.put_nowait(object())

    def close(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        self._pulls.put_nowait(None)

    def __aiter__(self) -> AsyncIterator[Content]:
        return self._stream()

    async def _stream(self) -> AsyncIterator[Content]:
        yield BinaryContent(
            data=b"",
            headers={
                "kind": "start",
                "path": self._path,
                "chunk_size": self._chunk_size,
            },
        )
        while True:
            await self._pulls.get()
            if self._closed.is_set():
                return
            yield BinaryContent(data=b"", headers={"kind": "pull"})


class _StorageUploadInputStream:
    def __init__(
        self,
        *,
        path: str,
        overwrite: bool,
        chunks: AsyncIterable[bytes],
        chunk_size: int,
        size: int | None,
        name: str,
        mime_type: str,
    ):
        self._path = path
        self._overwrite = overwrite
        self._source = chunks.__aiter__()
        self._chunk_size = chunk_size
        self._size = size
        self._name = name
        self._mime_type = mime_type
        self._closed = asyncio.Event()
        self._pulls: asyncio.Queue[int | None] = asyncio.Queue()
        self._pending_chunk = b""
        self._pending_offset = 0
        self._source_exhausted = False

    def request_next(self, chunk_size: int | None = None) -> None:
        if self._closed.is_set():
            return
        self._pulls.put_nowait(chunk_size)

    def close(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        self._pulls.put_nowait(None)

    def __aiter__(self) -> AsyncIterator[Content]:
        return self._stream()

    async def _next_data_chunk(self, requested_chunk_size: int) -> bytes | None:
        parts: list[bytes] = []
        bytes_buffered = 0

        while bytes_buffered < requested_chunk_size:
            if self._pending_offset < len(self._pending_chunk):
                start = self._pending_offset
                end = min(
                    start + (requested_chunk_size - bytes_buffered),
                    len(self._pending_chunk),
                )
                self._pending_offset = end
                part = self._pending_chunk[start:end]
                parts.append(part)
                bytes_buffered += len(part)
                continue

            if self._source_exhausted:
                break

            try:
                next_chunk = bytes(await self._source.__anext__())
            except StopAsyncIteration:
                self._source_exhausted = True
                break

            if len(next_chunk) == 0:
                continue

            self._pending_chunk = next_chunk
            self._pending_offset = 0

        if bytes_buffered == 0:
            return None
        if len(parts) == 1:
            return parts[0]
        return b"".join(parts)

    async def _stream(self) -> AsyncIterator[Content]:
        yield BinaryContent(
            data=b"",
            headers={
                "kind": "start",
                "path": self._path,
                "overwrite": self._overwrite,
                "name": self._name,
                "mime_type": self._mime_type,
                "size": self._size,
            },
        )
        while True:
            requested_chunk_size = await self._pulls.get()
            if self._closed.is_set():
                return

            next_chunk = await self._next_data_chunk(
                requested_chunk_size
                if isinstance(requested_chunk_size, int) and requested_chunk_size > 0
                else self._chunk_size
            )
            if next_chunk is None:
                return

            yield BinaryContent(data=next_chunk, headers={"kind": "data"})


class Queue:
    def __init__(self, *, name: str, size: int):
        self._name = name
        self._size = size

    @property
    def name(self):
        return self._name

    @property
    def size(self):
        return self._size


class QueuesClient:
    def __init__(self, *, room: RoomClient):
        self.room = room

    @staticmethod
    def _unexpected_response_error(*, operation: str) -> RoomException:
        return RoomException(
            f"unexpected return type from queues.{operation}",
            code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
        )

    async def _invoke(self, *, operation: str, arguments: dict) -> Content:
        response = await self.room.invoke(
            toolkit="queues",
            tool=operation,
            input=arguments,
        )
        if not isinstance(response, Content):
            raise self._unexpected_response_error(operation=operation)
        return response

    async def list(
        self,
        *,
        name: str | None = None,
        message: dict | None = None,
        create: bool = True,
    ) -> list[Queue]:
        del name
        del message
        del create
        response = await self._invoke(operation="list", arguments={})
        if not isinstance(response, JsonContent):
            raise self._unexpected_response_error(operation="list")
        queues = []
        for item in response.json["queues"]:
            queues.append(Queue(name=item["name"], size=int(item["size"])))
        return queues

    async def open(self, *, name: str) -> None:
        response = await self._invoke(operation="open", arguments={"name": name})
        if not isinstance(response, EmptyContent):
            raise self._unexpected_response_error(operation="open")

    async def send(self, *, name: str, message: dict, create: bool = True) -> None:
        response = await self._invoke(
            operation="send",
            arguments={"name": name, "create": create, "message": message},
        )
        if not isinstance(response, EmptyContent):
            raise self._unexpected_response_error(operation="send")

    async def drain(self, *, name: str) -> None:
        response = await self._invoke(operation="drain", arguments={"name": name})
        if not isinstance(response, EmptyContent):
            raise self._unexpected_response_error(operation="drain")

    async def close(self, *, name: str) -> None:
        response = await self._invoke(operation="close", arguments={"name": name})
        if not isinstance(response, EmptyContent):
            raise self._unexpected_response_error(operation="close")

    async def receive(
        self, *, name: str, create: bool = True, wait: bool = True
    ) -> dict | str | None:
        response = await self._invoke(
            operation="receive",
            arguments={"name": name, "create": create, "wait": wait},
        )
        if isinstance(response, EmptyContent):
            return None
        if isinstance(response, JsonContent):
            return response.json
        if isinstance(response, TextContent):
            return response.text
        raise self._unexpected_response_error(operation="receive")


class MessagingClient:
    def __init__(self, *, room: RoomClient):
        self.room = room
        self._participants = dict[str, RemoteParticipant]()
        self._events = {}
        room.protocol.register_handler("messaging.send", self._handle_message_send)
        self._message_queue = Chan[_QueuedRoomMessage]()
        self._send_task = None
        self._enabled = False

    @staticmethod
    def _message_json(message: dict) -> str:
        return json.dumps(message)

    @staticmethod
    def _attachment_base64(attachment: Optional[bytes]) -> str | None:
        if attachment is None:
            return None
        return base64.b64encode(attachment).decode("utf-8")

    async def _invoke(self, *, operation: str, input: dict) -> None:
        await self.room.invoke(
            toolkit="messaging",
            tool=operation,
            input=input,
        )

    @property
    def remote_participants(self) -> list[RemoteParticipant]:
        """
        get the other participants in the room with messaging enabled.
        """
        return list(self._participants.values())

    @property
    def is_enabled(self) -> bool:
        return self._enabled

    #
    def on(self, event_name: str, func: Callable):
        if event_name not in self._events:
            self._events[event_name] = []
        self._events[event_name].append(func)

    def off(self, event_name: str, func: Callable):
        if event_name in self._events:
            self._events[event_name].remove(func)

    def emit(self, event_name, **kwargs):
        """Call all handlers associated with the given event."""
        handlers = self._events.get(event_name, [])
        for handler in handlers:
            handler(**kwargs)

    def get_participants(self) -> list[RemoteParticipant]:
        return list(self._participants.values())

    def get_participant(self, id: str) -> RemoteParticipant | None:
        for part in self.remote_participants:
            if part.id == id:
                return part

        return None

    def get_participant_by_name(self, name: str) -> RemoteParticipant | None:
        for part in self.remote_participants:
            if part.get_attribute("name") == name:
                return part

        return None

    def _drop_queued_message(
        self, *, msg: _QueuedRoomMessage, error: RoomException
    ) -> None:
        logger.debug(
            "Dropping queued message for offline participant",
            extra={
                "participant_id": None if msg.to is None else msg.to.id,
                "type": msg.type,
            },
        )
        if msg.fut is not None and not msg.fut.done():
            msg.fut.set_exception(error)

    def _remove_participant(self, participant_id: str) -> RemoteParticipant | None:
        part = self._participants.pop(participant_id, None)
        if part is None:
            return None

        part._set_online(False)
        self.emit("participant_removed", participant=part)

        return part

    def _mark_participant_offline(self, participant: Participant | None) -> None:
        if not isinstance(participant, RemoteParticipant):
            return

        participant._set_online(False)
        current = self._participants.get(participant.id, None)
        if current is not None:
            self._remove_participant(participant.id)

    def _resolve_message_recipient(
        self, participant: Participant | None
    ) -> Participant | None:
        if participant is None:
            return None

        if not isinstance(participant, RemoteParticipant):
            return participant

        if participant.online is False:
            return None

        current = self._participants.get(participant.id, None)
        if current is None:
            return None

        return current

    async def enable(self):
        await self._invoke(operation="enable", input={})
        self._enabled = True

    async def disable(self):
        await self._invoke(operation="disable", input={})
        self._enabled = False

    async def _handle_message_send(
        self, protocol: Protocol, message_id: int, type: str, data: bytes
    ) -> None:
        header, payload = unpack_message(data)

        message = RoomMessage(
            from_participant_id=header["from_participant_id"],
            type=header["type"],
            message=header["message"],
            attachment=payload,
        )

        if message.type == "messaging.enabled":
            self._on_messaging_enabled(message)
        elif message.type == "participant.attributes":
            self._on_participant_attributes(message)
        elif message.type == "participant.enabled":
            self._on_participant_enabled(message)
        elif message.type == "participant.disabled":
            self._on_participant_disabled(message)
        else:
            self.emit("message", message=message)

    async def start(self):
        self._send_task = asyncio.create_task(self._send_messages())

    async def stop(self):
        self._message_queue.close()
        if self._send_task is not None:
            await asyncio.gather(self._send_task)
        self._enabled = False

    async def _send_messages(self):
        async for msg in self._message_queue:
            resolved_to = self._resolve_message_recipient(msg.to)
            if resolved_to is None:
                self._drop_queued_message(
                    msg=msg,
                    error=RoomException(
                        "the participant was not found", code=ErrorCode.NOT_FOUND
                    ),
                )
                continue

            try:
                await self._invoke(
                    operation="send",
                    input={
                        "to_participant_id": resolved_to.id,
                        "type": msg.type,
                        "message_json": self._message_json(msg.message),
                        "attachment_base64": self._attachment_base64(msg.attachment),
                    },
                )
                if msg.fut is not None and not msg.fut.done():
                    msg.fut.set_result(True)

            except asyncio.CancelledError:
                raise

            except RoomException as ex:
                if ex.code == ErrorCode.NOT_FOUND:
                    self._mark_participant_offline(msg.to)
                    if msg.drop_if_offline:
                        self._drop_queued_message(msg=msg, error=ex)
                        continue

                logger.info("Unable to send message to participant", exc_info=ex)
                if msg.fut is not None and not msg.fut.done():
                    msg.fut.set_exception(ex)

            except Exception as ex:
                logger.info("Unable to send message to participant", exc_info=ex)
                if msg.fut is not None and not msg.fut.done():
                    msg.fut.set_exception(ex)

    def send_message_nowait(
        self,
        *,
        to: Participant,
        type: str,
        message: dict,
        attachment: Optional[bytes] = None,
    ):
        if self._send_task is None:
            raise RoomException(
                "Cannot send messages because messaging has not been started"
            )

        self._message_queue.send_nowait(
            _QueuedRoomMessage(
                from_participant_id=self.room.local_participant.id,
                to=to,
                type=type,
                message=message,
                attachment=attachment,
                drop_if_offline=True,
            )
        )

    async def send_message(
        self,
        *,
        to: Participant,
        type: str,
        message: dict,
        attachment: Optional[bytes] = None,
    ):
        if self._send_task is None:
            raise RoomException(
                "Cannot send messages because messaging has not been started"
            )

        msg = _QueuedRoomMessage(
            from_participant_id=self.room.local_participant.id,
            to=to,
            type=type,
            message=message,
            attachment=attachment,
            drop_if_offline=False,
        )

        self._message_queue.send_nowait(msg)

        if msg.fut is None:
            raise RoomException("queued messaging future was not created")

        await msg.fut

    async def broadcast_message(
        self, *, type: str, message: dict, attachment: Optional[bytes] = None
    ):
        await self._invoke(
            operation="broadcast",
            input={
                "type": type,
                "message_json": self._message_json(message),
                "attachment_base64": self._attachment_base64(attachment),
            },
        )

    def _on_participant_enabled(self, message: RoomMessage):
        data = message.message
        participant = RemoteParticipant(id=data["id"], role=data["role"], online=True)

        for k, v in data["attributes"].items():
            participant._attributes[k] = v

        self._participants[data["id"]] = participant

        self.emit("participant_added", participant=participant)

    def _on_participant_attributes(self, message: RoomMessage):
        if message.from_participant_id in self._participants:
            part = self._participants[message.from_participant_id]
            for k, v in message.message["attributes"].items():
                part._attributes[k] = v

            self.emit("participant_attributes_updated", participant=part)

    def _on_participant_disabled(self, message: RoomMessage):
        self._remove_participant(message.message["id"])

    def _on_messaging_enabled(self, message: RoomMessage):
        self._enabled = True
        for data in message.message["participants"]:
            participant = RemoteParticipant(
                id=data["id"], role=data["role"], online=True
            )

            for k, v in data["attributes"].items():
                participant._attributes[k] = v

            self._participants[data["id"]] = participant

        self.emit("messaging_enabled")


@dataclass
class RoomLogEvent:
    type: str
    data: dict[str, Any]


class DeveloperClient:
    def __init__(self, room: RoomClient):
        self._room = room
        self._room.protocol.register_handler("developer.log", self._handle_log)
        self._events = dict[str, list[Callable]]()

    def on(self, event_name: str, func: Callable):
        if event_name not in self._events:
            self._events[event_name] = []
        self._events[event_name].append(func)

    def off(self, event_name: str, func: Callable):
        if event_name in self._events:
            self._events[event_name].remove(func)

    def emit(self, event_name: str, **kwargs):
        """Call all handlers associated with the given event."""
        handlers = self._events.get(event_name, [])
        for handler in handlers:
            handler(**kwargs)

    async def _handle_log(
        self, protocol: Protocol, message_id: int, type: str, data: bytes
    ) -> None:
        raw_json, _ = unpack_message(data)

        log_type = raw_json.get("type", "unknown")
        log_data = raw_json.get("data", {})

        self.emit("log", type=log_type, data=log_data)

    @staticmethod
    def _unexpected_response_error(*, operation: str) -> RoomException:
        return RoomException(
            f"unexpected return type from developer.{operation}",
            code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
        )

    async def _invoke(self, *, operation: str, input: dict) -> None:
        await self._room.invoke(
            toolkit="developer",
            tool=operation,
            input=input,
        )

    async def log(self, *, type: str, data: dict):
        await self._invoke(operation="log", input={"type": type, "data": data})

    def log_nowait(self, *, type: str, data: dict):
        task = asyncio.ensure_future(
            self._invoke(operation="log", input={"type": type, "data": data})
        )
        task.add_done_callback(
            lambda t: self._handle_developer_log_result(
                t, kind="log", payload={"type": type, "data": data}
            )
        )

    def info(self, message: str, *, extra: Optional[dict] = None):
        task = asyncio.ensure_future(
            self._invoke(
                operation="info",
                input={"message": message, "extra": extra or {}},
            )
        )
        task.add_done_callback(
            lambda t: self._handle_developer_log_result(
                t, kind="info", payload={"message": message, "extra": extra}
            )
        )

    def warning(self, message: str, *, extra: Optional[dict] = None):
        task = asyncio.ensure_future(
            self._invoke(
                operation="warning",
                input={"message": message, "extra": extra or {}},
            )
        )
        task.add_done_callback(
            lambda t: self._handle_developer_log_result(
                t, kind="warning", payload={"message": message, "extra": extra}
            )
        )

    def error(self, message: str, *, extra: Optional[dict] = None):
        task = asyncio.ensure_future(
            self._invoke(
                operation="error",
                input={"message": message, "extra": extra or {}},
            )
        )
        task.add_done_callback(
            lambda t: self._handle_developer_log_result(
                t, kind="error", payload={"message": message, "extra": extra}
            )
        )

    @staticmethod
    def _handle_developer_log_result(
        task: asyncio.Task, *, kind: str, payload: dict
    ) -> None:
        try:
            task.result()
        except Exception as exc:
            logger.warning(
                "unable to write developer log",
                extra={"type": kind, "payload": payload},
                exc_info=exc,
            )

    async def logs(self) -> AsyncIterator[RoomLogEvent]:
        input_stream = _DeveloperLogInputStream()
        response = await self._room.invoke(
            toolkit="developer",
            tool="logs",
            input=input_stream,
        )
        if isinstance(response, Content):
            input_stream.close()
            raise self._unexpected_response_error(operation="logs")

        response_stream = response
        try:
            async for chunk in response_stream:
                if isinstance(chunk, ErrorContent):
                    raise RoomException(chunk.text, code=chunk.code)
                if isinstance(chunk, _ControlContent):
                    if chunk.method == "close":
                        return
                    raise self._unexpected_response_error(operation="logs")
                if not isinstance(chunk, BinaryContent):
                    raise self._unexpected_response_error(operation="logs")

                log_type = chunk.headers.get("type")
                if not isinstance(log_type, str):
                    raise RoomException(
                        "developer.logs returned a chunk without a valid type",
                        code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
                    )

                try:
                    payload = (
                        json.loads(chunk.data.decode("utf-8")) if chunk.data else {}
                    )
                except Exception as ex:
                    raise RoomException(
                        "developer.logs returned invalid JSON data",
                        code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
                    ) from ex

                if not isinstance(payload, dict):
                    raise RoomException(
                        "developer.logs returned invalid JSON data",
                        code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
                    )

                event = RoomLogEvent(type=log_type, data=payload)
                self.emit("log", type=event.type, data=event.data)
                yield event
        finally:
            input_stream.close()


class _DeveloperLogInputStream:
    def __init__(self) -> None:
        self._closed = asyncio.Event()

    def close(self) -> None:
        self._closed.set()

    def __aiter__(self) -> "_DeveloperLogInputStream":
        return self

    async def __anext__(self) -> Content:
        if self._closed.is_set():
            raise StopAsyncIteration

        await self._closed.wait()
        raise StopAsyncIteration


class DataType(BaseModel, ABC):
    type: str
    nullable: Optional[bool] = None
    metadata: Optional[dict] = None

    model_config = ConfigDict(extra="allow")

    def _maybe_nullable_schema(self, t: str):
        if self.nullable:
            return [t, "null"]
        return t

    @abstractmethod
    def to_json_schema(self):
        pass


class IntDataType(DataType):
    type: Literal["int"] = "int"

    def to_json_schema(self):
        return {"type": self._maybe_nullable_schema("number")}


class BoolDataType(DataType):
    type: Literal["bool"] = "bool"

    def to_json_schema(self):
        return {"type": self._maybe_nullable_schema("boolean")}


class DateDataType(DataType):
    type: Literal["date"] = "date"

    def to_json_schema(self):
        return {
            "type": self._maybe_nullable_schema("string"),
            "description": "an ISO formatted date string",
        }


class TimestampDataType(DataType):
    type: Literal["timestamp"] = "timestamp"

    def to_json_schema(self):
        return {
            "type": self._maybe_nullable_schema("string"),
            "description": "an ISO formatted timestamp string",
        }


class FloatDataType(DataType):
    type: Literal["float"] = "float"

    def to_json_schema(self):
        return {
            "type": self._maybe_nullable_schema("number"),
        }


class VectorDataType(DataType):
    type: Literal["vector"] = "vector"
    size: int
    element_type: "DataTypeUnion"

    def to_json_schema(self):
        return {
            "type": "array",
            "items": {"type": self._maybe_nullable_schema("number")},
            "description": f"a vector with length {self.size}",
        }


class ListDataType(DataType):
    type: Literal["list"] = "list"
    element_type: "DataTypeUnion"

    def to_json_schema(self):
        return {
            "type": self._maybe_nullable_schema("array"),
            "items": self.element_type.to_json_schema(),
        }


class StructDataType(DataType):
    type: Literal["struct"] = "struct"
    fields: Dict[str, "DataTypeUnion"]

    def to_json_schema(self):
        return {
            "type": self._maybe_nullable_schema("object"),
            "properties": {
                field_name: field_type.to_json_schema()
                for field_name, field_type in self.fields.items()
            },
            "additionalProperties": False,
        }


class TextDataType(DataType):
    type: Literal["text"] = "text"

    def to_json_schema(self):
        return {
            "type": self._maybe_nullable_schema("string"),
        }


class JsonDataType(DataType):
    type: Literal["json"] = "json"

    def to_json_schema(self):
        value_schema: dict[str, Any] = {
            "anyOf": [
                {"type": "object"},
                {"type": "array"},
                {"type": "string"},
                {"type": "number"},
                {"type": "boolean"},
                {"type": "null"},
            ]
        }
        if self.nullable:
            return {"anyOf": [value_schema, {"type": "null"}]}
        return value_schema


class UuidDataType(DataType):
    type: Literal["uuid"] = "uuid"

    def to_json_schema(self):
        return {
            "type": self._maybe_nullable_schema("string"),
            "description": "a UUID string",
        }


class BinaryDataType(DataType):
    type: Literal["binary"] = "binary"

    def to_json_schema(self):
        return {
            "type": "array",
            "items": {"type": self._maybe_nullable_schema("number")},
            "description": "a byte array",
        }


DataTypeUnion = Annotated[
    Union[
        IntDataType,
        BoolDataType,
        DateDataType,
        TimestampDataType,
        FloatDataType,
        VectorDataType,
        ListDataType,
        StructDataType,
        TextDataType,
        JsonDataType,
        UuidDataType,
        BinaryDataType,
    ],
    Field(discriminator="type"),
]
_data_type_adapter = TypeAdapter(DataTypeUnion)
VectorDataType.model_rebuild()
ListDataType.model_rebuild()
StructDataType.model_rebuild()

CreateMode = Literal["create", "overwrite", "create_if_not_exists"]


def _require_non_empty_database_table_name(*, value: str, field_name: str) -> str:
    normalized = value.strip()
    if normalized == "":
        raise ValueError(f"{field_name} must not be empty")
    return normalized


class _CreateTableRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    data: Optional[Any] = None
    table_schema: Optional[Dict[str, DataTypeUnion]] = Field(
        default=None, alias="schema"
    )
    mode: CreateMode = "create"
    namespace: Optional[list[str]] = None
    branch: Optional[str] = None
    metadata: Optional[dict] = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        return _require_non_empty_database_table_name(
            value=value,
            field_name="table name",
        )


class _BranchRequest(BaseModel):
    branch: Optional[str] = None


class _ListTablesRequest(_BranchRequest):
    namespace: Optional[list[str]] = None


class _TableRequest(_BranchRequest):
    table: str
    namespace: Optional[list[str]] = None


class _VersionedTableRequest(_TableRequest):
    version: Optional[int] = None


class _InspectTableRequest(_VersionedTableRequest):
    pass


class _DropTableRequest(_BranchRequest):
    name: str
    ignore_missing: bool = False
    namespace: Optional[list[str]] = None


class _DropIndexRequest(_TableRequest):
    name: str


class _AddColumnsRequest(_TableRequest):
    new_columns: Dict[str, str | DataTypeUnion]


class _AlterColumnsRequest(_TableRequest):
    columns: Dict[str, DataTypeUnion]


class _DropColumnsRequest(_TableRequest):
    columns: List[str]


class _InsertRequest(_TableRequest):
    records: List[Dict[str, Any]]


class _UpdateRequest(_TableRequest):
    where: str
    values: Optional[Dict[str, Any]] = None
    values_sql: Optional[Dict[str, str]] = None


class _DeleteRequest(_TableRequest):
    where: str


class _MergeRequest(_TableRequest):
    on: str
    records: Any


class _SearchRequest(_VersionedTableRequest):
    text: Optional[str] = None
    vector: Optional[list[float]] = None
    text_columns: Optional[list[str]] = None
    where: Optional[str] = None
    offset: Optional[int] = None
    limit: Optional[int] = None
    select: Optional[List[str]] = None


class _CountRequest(_VersionedTableRequest):
    text: Optional[str] = None
    vector: Optional[list[float]] = None
    text_columns: Optional[list[str]] = None
    where: Optional[str] = None


class _OptimizeRequest(_TableRequest):
    pass


class _RestoreRequest(_TableRequest):
    version: int


class _ListVersionsRequest(_TableRequest):
    pass


class _CreateVectorIndexRequest(_TableRequest):
    column: str
    replace: Optional[bool] = None


class _CreateScalarIndexRequest(_TableRequest):
    column: str
    replace: Optional[bool] = None


class _CreateFullTextSearchIndexRequest(_TableRequest):
    column: str
    replace: Optional[bool] = None


class _ListIndexesRequest(_VersionedTableRequest):
    pass


class _CreateBranchRequest(BaseModel):
    branch: str
    from_branch: Optional[str] = None
    namespace: Optional[list[str]] = None


class _DeleteBranchRequest(BaseModel):
    branch: str
    namespace: Optional[list[str]] = None


class _ListBranchesRequest(BaseModel):
    namespace: Optional[list[str]] = None


MemoryIngestStrategy = Literal["heuristic", "llm"]


class MemoryEntityRecord(BaseModel):
    entity_id: Optional[str] = None
    name: str
    entity_type: Optional[str] = None
    context: Optional[str] = None
    confidence: Optional[float] = None
    created_at: Optional[str] = None
    valid_at: Optional[str] = None
    metadata: Optional[dict[str, str]] = None


class MemoryRelationshipRecord(BaseModel):
    source_entity_id: str
    target_entity_id: str
    relationship_type: str = "RELATED_TO"
    description: Optional[str] = None
    confidence: Optional[float] = None
    created_at: Optional[str] = None
    valid_at: Optional[str] = None
    expired_at: Optional[str] = None
    invalid_at: Optional[str] = None
    source_entity_name: Optional[str] = None
    target_entity_name: Optional[str] = None
    metadata: Optional[dict[str, str]] = None


class MemoryDatasetSummary(BaseModel):
    name: str
    rows: int
    columns: list[str] = Field(default_factory=list)


class MemoryDetails(BaseModel):
    name: str
    namespace: Optional[list[str]] = None
    path: str
    datasets: list[MemoryDatasetSummary] = Field(default_factory=list)


class MemoryIngestStats(BaseModel):
    entities: int = 0
    relationships: int = 0
    sources: int = 0


class MemoryIngestResult(BaseModel):
    name: str
    stats: MemoryIngestStats
    entity_ids: list[str] = Field(default_factory=list)


class MemoryRecallRelationship(BaseModel):
    source_entity_id: str
    target_entity_id: str
    relationship_type: str
    description: Optional[str] = None
    created_at: Optional[str] = None
    valid_at: Optional[str] = None
    expired_at: Optional[str] = None
    invalid_at: Optional[str] = None


class MemoryRecallItem(BaseModel):
    entity_id: str
    name: str
    entity_type: str
    context: Optional[str] = None
    confidence: Optional[float] = None
    created_at: Optional[str] = None
    valid_at: Optional[str] = None
    score: float
    relationships: list[MemoryRecallRelationship] = Field(default_factory=list)


class MemoryRecallResult(BaseModel):
    name: str
    query: str
    items: list[MemoryRecallItem] = Field(default_factory=list)


class MemoryDeleteEntitiesResult(BaseModel):
    name: str
    deleted_entities: int = 0
    deleted_relationships: int = 0


class MemoryDeleteRelationshipsResult(BaseModel):
    name: str
    deleted_relationships: int = 0


class MemoryOptimizeDatasetStats(BaseModel):
    dataset: str
    fragments_added: int = 0
    fragments_removed: int = 0
    files_added: int = 0
    files_removed: int = 0
    old_versions_removed: int = 0
    bytes_removed: int = 0


class MemoryOptimizeResult(BaseModel):
    name: str
    datasets: list[MemoryOptimizeDatasetStats] = Field(default_factory=list)


class _MemoryNamedRequest(BaseModel):
    name: str
    namespace: Optional[list[str]] = None


class _MemoryListRequest(BaseModel):
    namespace: Optional[list[str]] = None


class _MemoryCreateRequest(_MemoryNamedRequest):
    overwrite: bool = False
    ignore_exists: bool = False


class _MemoryDropRequest(_MemoryNamedRequest):
    ignore_missing: bool = False


class _MemoryQueryRequest(_MemoryNamedRequest):
    statement: str


class _MemoryInspectRequest(_MemoryNamedRequest):
    pass


class _MemoryUpsertTableRequest(_MemoryNamedRequest):
    table: str
    records: list[dict[str, Any]]
    merge: bool = True


class _MemoryUpsertNodesRequest(_MemoryNamedRequest):
    records: list[MemoryEntityRecord]
    merge: bool = True


class _MemoryUpsertRelationshipsRequest(_MemoryNamedRequest):
    records: list[MemoryRelationshipRecord]
    merge: bool = True


class _MemoryIngestRequest(_MemoryNamedRequest):
    strategy: MemoryIngestStrategy = "heuristic"
    llm_model: Optional[str] = None
    llm_temperature: Optional[float] = None


class _MemoryIngestTextRequest(_MemoryIngestRequest):
    text: str


class _MemoryIngestImageRequest(_MemoryIngestRequest):
    caption: Optional[str] = None
    mime_type: Optional[str] = None
    source: Optional[str] = None
    annotations: Optional[dict[str, str]] = None


class _MemoryIngestFileRequest(_MemoryIngestRequest):
    path: Optional[str] = None
    text: Optional[str] = None
    mime_type: Optional[str] = None


class _MemoryIngestFromTableRequest(_MemoryIngestRequest):
    table: str
    table_namespace: Optional[list[str]] = None
    text_columns: Optional[list[str]] = None
    limit: Optional[int] = None


class _MemoryIngestFromStorageRequest(_MemoryIngestRequest):
    paths: list[str]


class _MemoryRecallRequest(_MemoryNamedRequest):
    query: str
    limit: int = 5
    include_relationships: bool = True


class _MemoryDeleteEntitiesRequest(_MemoryNamedRequest):
    entity_ids: list[str]


class MemoryRelationshipSelector(BaseModel):
    source_entity_id: str
    target_entity_id: str
    relationship_type: Optional[str] = None


class _MemoryDeleteRelationshipsRequest(_MemoryNamedRequest):
    relationships: list[MemoryRelationshipSelector]


class _MemoryOptimizeRequest(_MemoryNamedRequest):
    compact: bool = True
    cleanup: bool = True


class SqlTableReference(BaseModel):
    name: str
    namespace: Optional[list[str]] = None
    alias: Optional[str] = None
    branch: Optional[str] = None
    version: Optional[int] = None


class _SqlRequest(BaseModel):
    query: str
    tables: List[SqlTableReference]
    params: Optional[Dict[str, Any]] = None


def _database_metadata_entries(metadata: dict | None) -> list[dict[str, str]] | None:
    if metadata is None:
        return None

    entries = list[dict[str, str]]()
    for key, value in metadata.items():
        encoded_value = (
            value if isinstance(value, str) else json.dumps(_encode_record_value(value))
        )
        entries.append({"key": str(key), "value": encoded_value})
    return entries


def _database_metadata_dict(metadata: object) -> dict[str, str]:
    if metadata is None:
        return {}
    if not isinstance(metadata, list):
        raise RoomException(
            "unexpected return type from database.inspect",
            code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
        )

    result = dict[str, str]()
    for entry in metadata:
        if not isinstance(entry, dict):
            raise RoomException(
                "unexpected return type from database.inspect",
                code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
            )
        key = entry.get("key")
        value = entry.get("value")
        if not isinstance(key, str) or not isinstance(value, str):
            raise RoomException(
                "unexpected return type from database.inspect",
                code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
            )
        result[key] = value
    return result


def _database_toolkit_data_type_json(data_type: DataType) -> dict[str, Any]:
    metadata = _database_metadata_entries(data_type.metadata)
    payload: dict[str, Any] = {
        "type": data_type.type,
        "nullable": data_type.nullable,
        "metadata": metadata,
    }

    if isinstance(data_type, VectorDataType):
        payload["size"] = data_type.size
        payload["element_type"] = _database_toolkit_data_type_json(
            data_type.element_type
        )
    elif isinstance(data_type, ListDataType):
        payload["element_type"] = _database_toolkit_data_type_json(
            data_type.element_type
        )
    elif isinstance(data_type, StructDataType):
        payload["fields"] = [
            {
                "name": field_name,
                "data_type": _database_toolkit_data_type_json(field_type),
            }
            for field_name, field_type in data_type.fields.items()
        ]

    return payload


def _database_toolkit_schema_entries(
    schema: dict[str, DataType] | None,
) -> list[dict[str, Any]] | None:
    if schema is None:
        return None

    return [
        {
            "name": name,
            "data_type": _database_toolkit_data_type_json(data_type),
        }
        for name, data_type in schema.items()
    ]


def _database_public_data_type_json(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RoomException(
            "unexpected return type from database.inspect",
            code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
        )

    type_name = value.get("type")
    if not isinstance(type_name, str):
        raise RoomException(
            "unexpected return type from database.inspect",
            code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
        )

    payload: dict[str, Any] = {
        "type": type_name,
        "nullable": value.get("nullable"),
        "metadata": _database_metadata_dict(value.get("metadata")),
    }

    if type_name == "vector":
        payload["size"] = value.get("size")
        payload["element_type"] = _database_public_data_type_json(
            value.get("element_type")
        )
    elif type_name == "list":
        payload["element_type"] = _database_public_data_type_json(
            value.get("element_type")
        )
    elif type_name == "struct":
        raw_fields = value.get("fields")
        if not isinstance(raw_fields, list):
            raise RoomException(
                "unexpected return type from database.inspect",
                code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
            )
        payload["fields"] = {
            field_name: _database_public_data_type_json(field_value)
            for field_name, field_value in (
                (
                    field.get("name"),
                    field.get("data_type"),
                )
                for field in raw_fields
                if isinstance(field, dict)
            )
            if isinstance(field_name, str)
        }
        if len(payload["fields"]) != len(raw_fields):
            raise RoomException(
                "unexpected return type from database.inspect",
                code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
            )

    return payload


def _database_data_type_from_toolkit(value: object) -> DataType:
    return _data_type_adapter.validate_python(_database_public_data_type_json(value))


def _database_schema_from_toolkit(
    value: object,
) -> dict[str, DataType]:
    if not isinstance(value, list):
        raise RoomException(
            "unexpected return type from database.inspect",
            code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
        )

    result = dict[str, DataType]()
    for field in value:
        if not isinstance(field, dict):
            raise RoomException(
                "unexpected return type from database.inspect",
                code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
            )
        name = field.get("name")
        if not isinstance(name, str):
            raise RoomException(
                "unexpected return type from database.inspect",
                code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
            )
        result[name] = _database_data_type_from_toolkit(field.get("data_type"))
    return result


def _database_records_json(records: object) -> str:
    if not isinstance(records, list):
        raise RoomException(
            "database toolkit records must be a list of objects",
            code=ErrorCode.INVALID_REQUEST,
        )
    return json.dumps(_encode_record_value(records))


def _database_data_json(data: object | None) -> str | None:
    if data is None:
        return None
    if isinstance(data, list) and all(isinstance(item, dict) for item in data):
        return json.dumps(encode_records(data))
    return json.dumps(_encode_record_value(data))


def _database_results_from_json(
    *,
    payload: dict[str, Any],
    operation: str,
) -> list[dict[str, Any]]:
    results = payload.get("results")
    if not isinstance(results, list):
        raise RoomException(
            f"unexpected return type from database.{operation}",
            code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
        )
    return decode_records(results)


def _database_value_json(value: Any) -> str:
    return json.dumps(_encode_record_value(value))


def _database_sql_literal(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).upper()
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, uuid.UUID):
        return f"X'{value.bytes.hex()}'"
    if isinstance(value, bytes):
        return f"X'{value.hex()}'"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(value)
    if value is None:
        return "NULL"
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc)
        return f"'{value.isoformat().replace('+00:00', 'Z')}'"
    if isinstance(value, date):
        return f"'{value.isoformat()}'"
    if isinstance(value, DatabaseJson):
        return "'" + json.dumps(value.to_json()).replace("'", "''") + "'"
    if isinstance(value, DatabaseStruct):
        fields = ", ".join(
            f"'{key}', {_database_sql_literal(inner)}"
            for key, inner in value.fields.items()
        )
        return f"named_struct({fields})"
    if isinstance(value, list):
        return "[" + ", ".join(_database_sql_literal(item) for item in value) + "]"
    if isinstance(value, dict):
        raise RoomException(
            "database object values must use DatabaseStruct or DatabaseJson",
            code=ErrorCode.INVALID_REQUEST,
        )
    raise RoomException(
        f"unsupported database value type {type(value).__name__}",
        code=ErrorCode.INVALID_REQUEST,
    )


def _database_stream_encode_value(value: Any) -> Any:
    return _encode_record_value(value)


def _database_stream_decode_value(
    value: object,
    *,
    operation: str,
) -> Any:
    try:
        return _decode_record_value(value)
    except Exception as exc:
        raise RoomException(
            f"unexpected return type from database.{operation}",
            code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
        ) from exc


def _database_stream_rows_chunk(records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "kind": "rows",
        "rows": [
            {
                "columns": [
                    {
                        "name": str(key),
                        "value": _database_stream_encode_value(value),
                    }
                    for key, value in record.items()
                ]
            }
            for record in records
        ],
    }


def _typed_rows_records_from_chunk(
    *,
    payload: dict[str, Any],
    operation: str,
) -> list[dict[str, Any]]:
    if payload.get("kind") != "rows":
        raise RoomException(
            f"unexpected return type from {operation}",
            code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
        )

    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise RoomException(
            f"unexpected return type from {operation}",
            code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
        )

    result = list[dict[str, Any]]()
    for row in rows:
        if not isinstance(row, dict):
            raise RoomException(
                f"unexpected return type from {operation}",
                code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
            )
        columns = row.get("columns")
        if not isinstance(columns, list):
            raise RoomException(
                f"unexpected return type from {operation}",
                code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
            )
        decoded_row = dict[str, Any]()
        for column in columns:
            if not isinstance(column, dict):
                raise RoomException(
                    f"unexpected return type from {operation}",
                    code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
                )
            name = column.get("name")
            if not isinstance(name, str):
                raise RoomException(
                    f"unexpected return type from {operation}",
                    code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
                )
            decoded_row[name] = _database_stream_decode_value(
                column.get("value"),
                operation=operation,
            )
        result.append(decoded_row)
    return result


def _database_stream_records_from_chunk(
    *,
    payload: dict[str, Any],
    operation: str,
) -> list[dict[str, Any]]:
    return _typed_rows_records_from_chunk(
        payload=payload,
        operation=f"database.{operation}",
    )


def _database_row_chunk_list(
    records: DatabaseRows,
    *,
    rows_per_chunk: int = 128,
) -> list[DatabaseRows]:
    if not isinstance(records, list):
        raise RoomException(
            "database stream records must be a list of objects",
            code=ErrorCode.INVALID_REQUEST,
        )
    if rows_per_chunk <= 0:
        raise RoomException(
            "rows_per_chunk must be greater than zero",
            code=ErrorCode.INVALID_REQUEST,
        )
    return [
        records[index : index + rows_per_chunk]
        for index in range(0, len(records), rows_per_chunk)
    ]


async def _database_async_row_chunks(
    chunks: DatabaseRowChunks,
) -> AsyncIterator[DatabaseRows]:
    if isinstance(chunks, AsyncIterable):
        async for chunk in chunks:
            yield chunk
        return
    for chunk in chunks:
        yield chunk


class _DatabaseWriteInputStream:
    def __init__(
        self,
        *,
        start: dict[str, Any],
        chunks: DatabaseRowChunks,
    ) -> None:
        self._start = start
        self._source = _database_async_row_chunks(chunks).__aiter__()
        self._closed = asyncio.Event()
        self._pulls: asyncio.Queue[object] = asyncio.Queue()
        self._sent_start = False

    def request_next(self) -> None:
        if self._closed.is_set():
            return
        self._pulls.put_nowait(object())

    def close(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        self._pulls.put_nowait(object())

    def __aiter__(self) -> "_DatabaseWriteInputStream":
        return self

    async def __anext__(self) -> Content:
        if not self._sent_start:
            self._sent_start = True
            return JsonContent(json=self._start)

        await self._pulls.get()
        if self._closed.is_set():
            raise StopAsyncIteration

        try:
            next_chunk = await self._source.__anext__()
        except StopAsyncIteration as exc:
            raise StopAsyncIteration from exc

        return JsonContent(json=_database_stream_rows_chunk(next_chunk))


class _DatabaseReadInputStream:
    def __init__(self, *, start: dict[str, Any]) -> None:
        self._start = start
        self._closed = asyncio.Event()
        self._pulls: asyncio.Queue[object] = asyncio.Queue()
        self._sent_start = False

    def request_next(self) -> None:
        if self._closed.is_set():
            return
        self._pulls.put_nowait(object())

    def close(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        self._pulls.put_nowait(object())

    def __aiter__(self) -> "_DatabaseReadInputStream":
        return self

    async def __anext__(self) -> Content:
        if not self._sent_start:
            self._sent_start = True
            return JsonContent(json=self._start)

        await self._pulls.get()
        if self._closed.is_set():
            raise StopAsyncIteration

        return JsonContent(json={"kind": "pull"})


class DatabaseClient:
    """
    A client for interacting with the 'database' toolkit on the room server.
    """

    def __init__(self, room: RoomClient):
        self.room = room

    @staticmethod
    def _unexpected_response_error(*, operation: str) -> RoomException:
        return RoomException(
            f"unexpected return type from database.{operation}",
            code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
        )

    async def _invoke(self, *, operation: str, input: dict) -> Content:
        response = await self.room.invoke(
            toolkit="database",
            tool=operation,
            input=input,
        )
        if not isinstance(response, Content):
            raise self._unexpected_response_error(operation=operation)
        return response

    async def _invoke_stream(
        self,
        *,
        operation: str,
        input: AsyncIterable[Content],
    ) -> AsyncIterable[Content]:
        response = await self.room.invoke(
            toolkit="database",
            tool=operation,
            input=input,
        )
        if isinstance(response, Content) or not isinstance(response, AsyncIterable):
            raise self._unexpected_response_error(operation=operation)
        return response

    async def _drain_write_stream(
        self,
        *,
        operation: str,
        input_stream: _DatabaseWriteInputStream,
    ) -> None:
        response_stream = await self._invoke_stream(
            operation=operation,
            input=input_stream,
        )
        try:
            async for chunk in response_stream:
                if isinstance(chunk, ErrorContent):
                    raise RoomException(chunk.text, code=chunk.code)
                if isinstance(chunk, _ControlContent):
                    if chunk.method == "close":
                        return
                    raise self._unexpected_response_error(operation=operation)
                if not isinstance(chunk, JsonContent):
                    raise self._unexpected_response_error(operation=operation)
                if chunk.json.get("kind") != "pull":
                    raise self._unexpected_response_error(operation=operation)
                input_stream.request_next()
        finally:
            input_stream.close()

    async def _stream_rows(
        self,
        *,
        operation: str,
        start: dict[str, Any],
    ) -> AsyncIterator[list[dict[str, Any]]]:
        input_stream = _DatabaseReadInputStream(start=start)
        response_stream = await self._invoke_stream(
            operation=operation,
            input=input_stream,
        )
        input_stream.request_next()
        try:
            async for chunk in response_stream:
                if isinstance(chunk, ErrorContent):
                    raise RoomException(chunk.text, code=chunk.code)
                if isinstance(chunk, _ControlContent):
                    if chunk.method == "close":
                        return
                    raise self._unexpected_response_error(operation=operation)
                if not isinstance(chunk, JsonContent):
                    raise self._unexpected_response_error(operation=operation)
                yield _database_stream_records_from_chunk(
                    payload=chunk.json,
                    operation=operation,
                )
                input_stream.request_next()
        finally:
            input_stream.close()

    async def list_tables(
        self,
        *,
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
    ) -> List[str]:
        response = await self._invoke(
            operation="list_tables",
            input={"namespace": namespace, "branch": branch},
        )
        if not isinstance(response, JsonContent):
            raise self._unexpected_response_error(operation="list_tables")
        return response.json.get("tables", [])

    async def inspect(
        self,
        *,
        table: str,
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
        version: Optional[int] = None,
    ) -> dict[str, DataType]:
        response = await self._invoke(
            operation="inspect",
            input={
                "table": table,
                "namespace": namespace,
                "branch": branch,
                "version": version,
            },
        )
        if not isinstance(response, JsonContent):
            raise self._unexpected_response_error(operation="inspect")
        return _database_schema_from_toolkit(response.json.get("fields"))

    async def _create_table(
        self,
        *,
        name: str,
        data: Optional[DatabaseRecord | DatabaseRows] = None,
        schema: Optional[Dict[str, DataType]] = None,
        mode: Optional[CreateMode] = "create",
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        chunks = (
            []
            if data is None
            else _database_row_chunk_list(
                data if isinstance(data, list) else [data],
            )
        )
        normalized_name = _require_non_empty_database_table_name(
            value=name,
            field_name="table name",
        )
        input_stream = _DatabaseWriteInputStream(
            start={
                "kind": "start",
                "name": normalized_name,
                "fields": _database_toolkit_schema_entries(schema),
                "mode": mode,
                "namespace": namespace,
                "branch": branch,
                "metadata": _database_metadata_entries(metadata),
            },
            chunks=chunks,
        )
        await self._drain_write_stream(
            operation="create_table",
            input_stream=input_stream,
        )

    async def create_table_with_schema(
        self,
        *,
        name: str,
        schema: Optional[Dict[str, DataType]] = None,
        data: Optional[DatabaseRows] = None,
        mode: Optional[CreateMode] = "create",
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        return await self._create_table(
            name=name,
            schema=schema,
            mode=mode,
            data=data,
            namespace=namespace,
            branch=branch,
            metadata=metadata,
        )

    async def create_table_from_data(
        self,
        *,
        name: str,
        data: Optional[DatabaseRows] = None,
        mode: Optional[CreateMode] = "create",
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        return await self._create_table(
            name=name,
            data=data,
            mode=mode,
            namespace=namespace,
            branch=branch,
            metadata=metadata,
        )

    async def create_table_from_data_stream(
        self,
        *,
        name: str,
        chunks: DatabaseRowChunks,
        schema: Optional[Dict[str, DataType]] = None,
        mode: Optional[CreateMode] = "create",
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        input_stream = _DatabaseWriteInputStream(
            start={
                "kind": "start",
                "name": name,
                "fields": _database_toolkit_schema_entries(schema),
                "mode": mode,
                "namespace": namespace,
                "branch": branch,
                "metadata": _database_metadata_entries(metadata),
            },
            chunks=chunks,
        )
        await self._drain_write_stream(
            operation="create_table",
            input_stream=input_stream,
        )

    async def drop_table(
        self,
        *,
        name: str,
        ignore_missing: bool = False,
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
    ) -> None:
        await self._invoke(
            operation="drop_table",
            input={
                "name": name,
                "ignore_missing": ignore_missing,
                "namespace": namespace,
                "branch": branch,
            },
        )

    async def drop_index(
        self,
        *,
        table: str,
        name: str,
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
    ) -> None:
        await self._invoke(
            operation="drop_index",
            input={
                "table": table,
                "name": name,
                "namespace": namespace,
                "branch": branch,
            },
        )

    async def add_columns(
        self,
        *,
        table: str,
        new_columns: Dict[str, str | DataType],
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
    ) -> None:
        columns = []
        for column_name, column_value in new_columns.items():
            if isinstance(column_value, DataType):
                columns.append(
                    {
                        "name": column_name,
                        "value_sql": None,
                        "data_type": _database_toolkit_data_type_json(column_value),
                    }
                )
            else:
                columns.append(
                    {
                        "name": column_name,
                        "value_sql": column_value,
                        "data_type": None,
                    }
                )
        await self._invoke(
            operation="add_columns",
            input={
                "table": table,
                "columns": columns,
                "namespace": namespace,
                "branch": branch,
            },
        )

    # TODO: not ready yet on lance side
    async def _alter_columns(
        self,
        *,
        table: str,
        columns: Dict[str, DataType],
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
    ) -> None:
        await self._invoke(
            operation="alter_columns",
            input={
                "table": table,
                "columns": _database_toolkit_schema_entries(columns),
                "namespace": namespace,
                "branch": branch,
            },
        )

    async def drop_columns(
        self,
        *,
        table: str,
        columns: List[str],
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
    ) -> None:
        await self._invoke(
            operation="drop_columns",
            input={
                "table": table,
                "columns": columns,
                "namespace": namespace,
                "branch": branch,
            },
        )

    async def insert(
        self,
        *,
        table: str,
        records: DatabaseRows,
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
    ) -> None:
        await self.insert_stream(
            table=table,
            chunks=_database_row_chunk_list(records),
            namespace=namespace,
            branch=branch,
        )

    async def insert_stream(
        self,
        *,
        table: str,
        chunks: DatabaseRowChunks,
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
    ) -> None:
        input_stream = _DatabaseWriteInputStream(
            start={
                "kind": "start",
                "table": table,
                "namespace": namespace,
                "branch": branch,
            },
            chunks=chunks,
        )
        await self._drain_write_stream(operation="insert", input_stream=input_stream)

    async def update(
        self,
        *,
        table: str,
        where: str,
        values: DatabaseRecord,
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
    ) -> None:
        await self._invoke(
            operation="update",
            input={
                "table": table,
                "where": where,
                "values": [
                    {
                        "column": column,
                        "value_json": _database_value_json(value),
                    }
                    for column, value in values.items()
                ],
                "namespace": namespace,
                "branch": branch,
            },
        )

    async def delete(
        self,
        *,
        table: str,
        where: str,
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
    ) -> None:
        await self._invoke(
            operation="delete",
            input={
                "table": table,
                "where": where,
                "namespace": namespace,
                "branch": branch,
            },
        )

    async def merge(
        self,
        *,
        table: str,
        on: str,
        records: DatabaseRows,
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
    ) -> None:
        await self.merge_stream(
            table=table,
            on=on,
            chunks=_database_row_chunk_list(records),
            namespace=namespace,
            branch=branch,
        )

    async def merge_stream(
        self,
        *,
        table: str,
        on: str,
        chunks: DatabaseRowChunks,
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
    ) -> None:
        input_stream = _DatabaseWriteInputStream(
            start={
                "kind": "start",
                "table": table,
                "on": on,
                "namespace": namespace,
                "branch": branch,
            },
            chunks=chunks,
        )
        await self._drain_write_stream(operation="merge", input_stream=input_stream)

    async def sql(
        self,
        *,
        query: str,
        tables: List[SqlTableReference | str],
        params: Optional[Dict[str, Any]] = None,
    ) -> list[Dict[str, Any]]:
        results = list[dict[str, Any]]()
        async for batch in self.sql_stream(
            query=query,
            tables=tables,
            params=params,
        ):
            results.extend(batch)
        return results

    async def sql_stream(
        self,
        *,
        query: str,
        tables: List[SqlTableReference | str],
        params: Optional[Dict[str, Any]] = None,
    ) -> AsyncIterator[list[Dict[str, Any]]]:
        table_refs = [
            SqlTableReference(name=table) if isinstance(table, str) else table
            for table in tables
        ]
        async for batch in self._stream_rows(
            operation="sql",
            start={
                "kind": "start",
                "query": query,
                "tables": [table.model_dump() for table in table_refs],
                "params_json": (
                    json.dumps(encode_records([params])[0])
                    if params is not None
                    else None
                ),
            },
        ):
            yield batch

    async def search(
        self,
        *,
        table: str,
        text: Optional[str] = None,
        vector: Optional[list[float]] = None,
        where: Optional[str] | dict = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
        select: Optional[List[str]] = None,
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
        version: Optional[int] = None,
    ) -> list[Dict[str, Any]]:
        results = list[dict[str, Any]]()
        async for batch in self.search_stream(
            table=table,
            text=text,
            vector=vector,
            where=where,
            offset=offset,
            limit=limit,
            select=select,
            namespace=namespace,
            branch=branch,
            version=version,
        ):
            results.extend(batch)
        return results

    async def search_stream(
        self,
        *,
        table: str,
        text: Optional[str] = None,
        vector: Optional[list[float]] = None,
        where: Optional[str] | dict = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
        select: Optional[List[str]] = None,
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
        version: Optional[int] = None,
    ) -> AsyncIterator[list[Dict[str, Any]]]:
        if isinstance(where, dict):
            where = " AND ".join(
                f"{column} = {_database_sql_literal(value)}"
                for column, value in where.items()
            )
        async for batch in self._stream_rows(
            operation="search",
            start={
                "kind": "start",
                "table": table,
                "text": text,
                "vector": vector,
                "text_columns": None,
                "where": where,
                "offset": offset,
                "limit": limit,
                "select": select,
                "namespace": namespace,
                "branch": branch,
                "version": version,
            },
        ):
            yield batch

    async def count(
        self,
        *,
        table: str,
        text: Optional[str] = None,
        vector: Optional[list[float]] = None,
        where: Optional[str] | dict = None,
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
        version: Optional[int] = None,
    ) -> list[Dict[str, Any]]:
        if isinstance(where, dict):
            where = " AND ".join(
                f"{column} = {_database_sql_literal(value)}"
                for column, value in where.items()
            )
        response = await self._invoke(
            operation="count",
            input={
                "table": table,
                "text": text,
                "vector": vector,
                "text_columns": None,
                "where": where,
                "namespace": namespace,
                "branch": branch,
                "version": version,
            },
        )
        if not isinstance(response, JsonContent):
            raise self._unexpected_response_error(operation="count")
        if not isinstance(response.json.get("count"), int):
            raise self._unexpected_response_error(operation="count")
        return response.json["count"]

    async def optimize(
        self,
        *,
        table: str,
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
    ) -> None:
        await self._invoke(
            operation="optimize",
            input={
                "table": table,
                "namespace": namespace,
                "branch": branch,
            },
        )

    async def restore(
        self,
        *,
        table: str,
        version: int,
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
    ) -> None:
        await self._invoke(
            operation="restore",
            input={
                "table": table,
                "version": version,
                "namespace": namespace,
                "branch": branch,
            },
        )

    async def list_versions(
        self,
        *,
        table: str,
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
    ) -> list["TableVersion"]:
        response = await self._invoke(
            operation="list_versions",
            input={
                "table": table,
                "namespace": namespace,
                "branch": branch,
            },
        )
        if not isinstance(response, JsonContent):
            raise self._unexpected_response_error(operation="list_versions")
        versions = response.json.get("versions")
        if not isinstance(versions, list):
            raise self._unexpected_response_error(operation="list_versions")
        parsed_versions = list[TableVersion]()
        for version in versions:
            if not isinstance(version, dict):
                raise self._unexpected_response_error(operation="list_versions")
            metadata_json = version.get("metadata_json")
            if not isinstance(metadata_json, str):
                raise self._unexpected_response_error(operation="list_versions")
            try:
                metadata = json.loads(metadata_json)
            except json.JSONDecodeError as exc:
                raise self._unexpected_response_error(
                    operation="list_versions"
                ) from exc
            parsed_versions.append(
                TableVersion.model_validate(
                    {
                        "version": version.get("version"),
                        "timestamp": version.get("timestamp"),
                        "metadata": metadata,
                    }
                )
            )
        return parsed_versions

    async def create_vector_index(
        self,
        *,
        table: str,
        column: str,
        replace: Optional[bool] = None,
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
    ) -> None:
        await self._invoke(
            operation="create_vector_index",
            input={
                "table": table,
                "column": column,
                "replace": replace,
                "namespace": namespace,
                "branch": branch,
            },
        )

    async def create_scalar_index(
        self,
        *,
        table: str,
        column: str,
        replace: Optional[bool] = None,
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
    ) -> None:
        await self._invoke(
            operation="create_scalar_index",
            input={
                "table": table,
                "column": column,
                "replace": replace,
                "namespace": namespace,
                "branch": branch,
            },
        )

    async def create_full_text_search_index(
        self,
        *,
        table: str,
        column: str,
        replace: Optional[bool] = None,
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
    ) -> None:
        await self._invoke(
            operation="create_full_text_search_index",
            input={
                "table": table,
                "column": column,
                "replace": replace,
                "namespace": namespace,
                "branch": branch,
            },
        )

    async def list_indexes(
        self,
        *,
        table: str,
        namespace: Optional[list[str]] = None,
        branch: Optional[str] = None,
        version: Optional[int] = None,
    ) -> list["TableIndex"]:
        response = await self._invoke(
            operation="list_indexes",
            input={
                "table": table,
                "namespace": namespace,
                "branch": branch,
                "version": version,
            },
        )
        if not isinstance(response, JsonContent):
            raise self._unexpected_response_error(operation="list_indexes")
        indexes = response.json.get("indexes")
        if not isinstance(indexes, list):
            raise self._unexpected_response_error(operation="list_indexes")
        return [TableIndex.model_validate(index_data) for index_data in indexes]

    async def list_branches(
        self, *, namespace: Optional[list[str]] = None
    ) -> list["TableBranch"]:
        response = await self._invoke(
            operation="list_branches",
            input={"namespace": namespace},
        )
        if not isinstance(response, JsonContent):
            raise self._unexpected_response_error(operation="list_branches")
        branches = response.json.get("branches")
        if not isinstance(branches, list):
            raise self._unexpected_response_error(operation="list_branches")
        return [TableBranch.model_validate(branch) for branch in branches]

    async def create_branch(
        self,
        *,
        branch: str,
        from_branch: Optional[str] = None,
        namespace: Optional[list[str]] = None,
    ) -> None:
        await self._invoke(
            operation="create_branch",
            input={
                "branch": branch,
                "from_branch": from_branch,
                "namespace": namespace,
            },
        )

    async def delete_branch(
        self, *, branch: str, namespace: Optional[list[str]] = None
    ) -> None:
        await self._invoke(
            operation="delete_branch",
            input={"branch": branch, "namespace": namespace},
        )


class MemoryClient:
    """
    A client for interacting with the 'memory' extension on the room server.
    """

    def __init__(self, room: RoomClient):
        self.room = room

    @staticmethod
    def _unexpected_response_error(*, operation: str) -> RoomException:
        return RoomException(
            f"unexpected return type from memory.{operation}",
            code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
        )

    async def _invoke(self, *, operation: str, input: dict) -> Content:
        return await self.room.invoke(
            toolkit="memory",
            tool=operation,
            input=input,
        )

    async def list(self, *, namespace: Optional[List[str]] = None) -> List[str]:
        request_model = _MemoryListRequest(namespace=namespace)
        response = await self._invoke(
            operation="list",
            input=request_model.model_dump(),
        )
        if isinstance(response, JsonContent):
            return list(response.json.get("memories", []))

        raise self._unexpected_response_error(operation="list")

    async def create(
        self,
        *,
        name: str,
        namespace: Optional[List[str]] = None,
        overwrite: bool = False,
        ignore_exists: bool = False,
    ) -> None:
        request_model = _MemoryCreateRequest(
            name=name,
            namespace=namespace,
            overwrite=overwrite,
            ignore_exists=ignore_exists,
        )
        await self._invoke(
            operation="create",
            input=request_model.model_dump(),
        )

    async def drop(
        self,
        *,
        name: str,
        namespace: Optional[List[str]] = None,
        ignore_missing: bool = False,
    ) -> None:
        request_model = _MemoryDropRequest(
            name=name,
            namespace=namespace,
            ignore_missing=ignore_missing,
        )
        await self._invoke(
            operation="drop",
            input=request_model.model_dump(),
        )

    async def inspect(
        self, *, name: str, namespace: Optional[List[str]] = None
    ) -> MemoryDetails:
        request_model = _MemoryInspectRequest(name=name, namespace=namespace)
        response = await self._invoke(
            operation="inspect",
            input=request_model.model_dump(),
        )
        if isinstance(response, JsonContent):
            return MemoryDetails.model_validate(response.json)

        raise self._unexpected_response_error(operation="inspect")

    async def query(
        self,
        *,
        name: str,
        statement: str,
        namespace: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        request_model = _MemoryQueryRequest(
            name=name,
            namespace=namespace,
            statement=statement,
        )
        response = await self._invoke(
            operation="query",
            input=request_model.model_dump(),
        )
        if isinstance(response, JsonContent):
            if isinstance(response.json.get("results"), list):
                return decode_records(response.json["results"])
            return _typed_rows_records_from_chunk(
                payload=response.json,
                operation="memory.query",
            )

        raise self._unexpected_response_error(operation="query")

    async def upsert_table(
        self,
        *,
        name: str,
        table: str,
        records: DatabaseRows,
        merge: bool = True,
        namespace: Optional[List[str]] = None,
    ) -> None:
        request_model = _MemoryUpsertTableRequest(
            name=name,
            namespace=namespace,
            table=table,
            records=encode_records(records),
            merge=merge,
        )
        await self._invoke(
            operation="upsert_table",
            input={
                "name": request_model.name,
                "namespace": request_model.namespace,
                "table": request_model.table,
                "records_json": json.dumps(request_model.records),
                "merge": request_model.merge,
            },
        )

    async def upsert_nodes(
        self,
        *,
        name: str,
        records: List[MemoryEntityRecord],
        merge: bool = True,
        namespace: Optional[List[str]] = None,
    ) -> None:
        request_model = _MemoryUpsertNodesRequest(
            name=name,
            namespace=namespace,
            records=records,
            merge=merge,
        )
        await self._invoke(
            operation="upsert_nodes",
            input={
                "name": request_model.name,
                "namespace": request_model.namespace,
                "records_json": json.dumps(
                    [record.model_dump() for record in request_model.records]
                ),
                "merge": request_model.merge,
            },
        )

    async def upsert_relationships(
        self,
        *,
        name: str,
        records: List[MemoryRelationshipRecord],
        merge: bool = True,
        namespace: Optional[List[str]] = None,
    ) -> None:
        request_model = _MemoryUpsertRelationshipsRequest(
            name=name,
            namespace=namespace,
            records=records,
            merge=merge,
        )
        await self._invoke(
            operation="upsert_relationships",
            input={
                "name": request_model.name,
                "namespace": request_model.namespace,
                "records_json": json.dumps(
                    [record.model_dump() for record in request_model.records]
                ),
                "merge": request_model.merge,
            },
        )

    async def ingest_text(
        self,
        *,
        name: str,
        text: str,
        namespace: Optional[List[str]] = None,
        strategy: MemoryIngestStrategy = "heuristic",
        llm_model: Optional[str] = None,
        llm_temperature: Optional[float] = None,
    ) -> MemoryIngestResult:
        request_model = _MemoryIngestTextRequest(
            name=name,
            namespace=namespace,
            text=text,
            strategy=strategy,
            llm_model=llm_model,
            llm_temperature=llm_temperature,
        )
        response = await self._invoke(
            operation="ingest_text",
            input=request_model.model_dump(),
        )
        if isinstance(response, JsonContent):
            return MemoryIngestResult.model_validate(response.json)

        raise self._unexpected_response_error(operation="ingest_text")

    async def ingest_image(
        self,
        *,
        name: str,
        caption: Optional[str] = None,
        data: Optional[bytes] = None,
        mime_type: Optional[str] = None,
        source: Optional[str] = None,
        annotations: Optional[dict[str, str]] = None,
        namespace: Optional[List[str]] = None,
        strategy: MemoryIngestStrategy = "heuristic",
        llm_model: Optional[str] = None,
        llm_temperature: Optional[float] = None,
    ) -> MemoryIngestResult:
        request_model = _MemoryIngestImageRequest(
            name=name,
            namespace=namespace,
            caption=caption,
            mime_type=mime_type,
            source=source,
            annotations=annotations,
            strategy=strategy,
            llm_model=llm_model,
            llm_temperature=llm_temperature,
        )
        response = await self._invoke(
            operation="ingest_image",
            input={
                "name": request_model.name,
                "namespace": request_model.namespace,
                "caption": request_model.caption,
                "data_base64": None
                if data is None
                else base64.b64encode(data).decode("utf-8"),
                "mime_type": request_model.mime_type,
                "source": request_model.source,
                "annotations_json": None
                if request_model.annotations is None
                else json.dumps(request_model.annotations),
                "strategy": request_model.strategy,
                "llm_model": request_model.llm_model,
                "llm_temperature": request_model.llm_temperature,
            },
        )
        if isinstance(response, JsonContent):
            return MemoryIngestResult.model_validate(response.json)

        raise self._unexpected_response_error(operation="ingest_image")

    async def ingest_file(
        self,
        *,
        name: str,
        path: Optional[str] = None,
        text: Optional[str] = None,
        mime_type: Optional[str] = None,
        namespace: Optional[List[str]] = None,
        strategy: MemoryIngestStrategy = "heuristic",
        llm_model: Optional[str] = None,
        llm_temperature: Optional[float] = None,
    ) -> MemoryIngestResult:
        request_model = _MemoryIngestFileRequest(
            name=name,
            namespace=namespace,
            path=path,
            text=text,
            mime_type=mime_type,
            strategy=strategy,
            llm_model=llm_model,
            llm_temperature=llm_temperature,
        )
        response = await self._invoke(
            operation="ingest_file",
            input=request_model.model_dump(),
        )
        if isinstance(response, JsonContent):
            return MemoryIngestResult.model_validate(response.json)

        raise self._unexpected_response_error(operation="ingest_file")

    async def ingest_from_table(
        self,
        *,
        name: str,
        table: str,
        text_columns: Optional[List[str]] = None,
        table_namespace: Optional[List[str]] = None,
        limit: Optional[int] = None,
        namespace: Optional[List[str]] = None,
        strategy: MemoryIngestStrategy = "heuristic",
        llm_model: Optional[str] = None,
        llm_temperature: Optional[float] = None,
    ) -> MemoryIngestResult:
        request_model = _MemoryIngestFromTableRequest(
            name=name,
            namespace=namespace,
            table=table,
            table_namespace=table_namespace,
            text_columns=text_columns,
            limit=limit,
            strategy=strategy,
            llm_model=llm_model,
            llm_temperature=llm_temperature,
        )
        response = await self._invoke(
            operation="ingest_from_table",
            input=request_model.model_dump(),
        )
        if isinstance(response, JsonContent):
            return MemoryIngestResult.model_validate(response.json)

        raise self._unexpected_response_error(operation="ingest_from_table")

    async def ingest_from_storage(
        self,
        *,
        name: str,
        paths: List[str],
        namespace: Optional[List[str]] = None,
        strategy: MemoryIngestStrategy = "heuristic",
        llm_model: Optional[str] = None,
        llm_temperature: Optional[float] = None,
    ) -> MemoryIngestResult:
        request_model = _MemoryIngestFromStorageRequest(
            name=name,
            namespace=namespace,
            paths=paths,
            strategy=strategy,
            llm_model=llm_model,
            llm_temperature=llm_temperature,
        )
        response = await self._invoke(
            operation="ingest_from_storage",
            input=request_model.model_dump(),
        )
        if isinstance(response, JsonContent):
            return MemoryIngestResult.model_validate(response.json)

        raise self._unexpected_response_error(operation="ingest_from_storage")

    async def recall(
        self,
        *,
        name: str,
        query: str,
        namespace: Optional[List[str]] = None,
        limit: int = 5,
        include_relationships: bool = True,
    ) -> MemoryRecallResult:
        request_model = _MemoryRecallRequest(
            name=name,
            namespace=namespace,
            query=query,
            limit=limit,
            include_relationships=include_relationships,
        )
        response = await self._invoke(
            operation="recall",
            input=request_model.model_dump(),
        )
        if isinstance(response, JsonContent):
            return MemoryRecallResult.model_validate(response.json)

        raise self._unexpected_response_error(operation="recall")

    async def delete_entities(
        self,
        *,
        name: str,
        entity_ids: List[str],
        namespace: Optional[List[str]] = None,
    ) -> MemoryDeleteEntitiesResult:
        request_model = _MemoryDeleteEntitiesRequest(
            name=name,
            namespace=namespace,
            entity_ids=entity_ids,
        )
        response = await self._invoke(
            operation="delete_entities",
            input=request_model.model_dump(),
        )
        if isinstance(response, JsonContent):
            return MemoryDeleteEntitiesResult.model_validate(response.json)

        raise self._unexpected_response_error(operation="delete_entities")

    async def delete_relationships(
        self,
        *,
        name: str,
        relationships: List[MemoryRelationshipSelector],
        namespace: Optional[List[str]] = None,
    ) -> MemoryDeleteRelationshipsResult:
        request_model = _MemoryDeleteRelationshipsRequest(
            name=name,
            namespace=namespace,
            relationships=relationships,
        )
        response = await self._invoke(
            operation="delete_relationships",
            input=request_model.model_dump(),
        )
        if isinstance(response, JsonContent):
            return MemoryDeleteRelationshipsResult.model_validate(response.json)

        raise self._unexpected_response_error(operation="delete_relationships")

    async def optimize(
        self,
        *,
        name: str,
        namespace: Optional[List[str]] = None,
        compact: bool = True,
        cleanup: bool = True,
    ) -> MemoryOptimizeResult:
        request_model = _MemoryOptimizeRequest(
            name=name,
            namespace=namespace,
            compact=compact,
            cleanup=cleanup,
        )
        response = await self._invoke(
            operation="optimize",
            input=request_model.model_dump(),
        )
        if isinstance(response, JsonContent):
            return MemoryOptimizeResult.model_validate(response.json)

        raise self._unexpected_response_error(operation="optimize")


class TableVersion(BaseModel):
    timestamp: datetime
    version: int
    metadata: dict[str, JsonValue]


class TableBranch(BaseModel):
    name: str
    parent_branch: Optional[str] = None
    parent_version: Optional[int] = None
    created_at: Optional[datetime] = None
    manifest_size: Optional[int] = None


class TableIndex(BaseModel):
    name: str
    columns: list[str]
    type: str


class ProgressDetail(BaseModel):
    current: Optional[int] = None
    total: Optional[int] = None


class LogProgress(BaseModel):
    layer: Optional[str] = None
    message: Optional[str] = None
    current: Optional[int] = None
    total: Optional[int] = None


class ErrorDetail(BaseModel):
    """Structured error information returned on failure."""

    code: Optional[int] = None
    message: str


class PullMessage(BaseModel):
    """
    One JSON object emitted by the Engine while pulling *or* pushing.

    Docker can add extra keys in new versions, so we allow unknown fields.
    """

    # Main variants ----------------------------------------------------------
    status: Optional[str] = None  # layer status ("Downloading", "Extracting", ...)
    id: Optional[str] = Field(None, alias="id")  # layer identifier / step number

    # Progress bar -----------------------------------------------------------
    progress: Optional[str] = None
    progress_detail: Optional[ProgressDetail] = Field(None, alias="progressDetail")

    # Success / aux payload --------------------------------------------------
    aux: Optional[Any] = None  # e.g. {"Digest": "sha256:…", "Size": 123}

    # Error handling ---------------------------------------------------------
    error: Optional[str] = None
    error_detail: Optional[ErrorDetail] = Field(None, alias="errorDetail")

    # Misc extras sometimes present -----------------------------------------
    time: Optional[int] = None  # seconds since epoch
    from_: Optional[str] = Field(None, alias="from")  # reserve‑word workaround

    model_config = ConfigDict(
        validate_by_name=True,  # accept field aliases on input
        extra="allow",  # keep unknown keys for forward‑compat
    )


class Image(BaseModel):
    id: str
    tags: List[str]
    size: int
    labels: Dict[str, str]


class DockerSecret(BaseModel):
    registry: Optional[str] = None
    username: str
    password: str


class ImagePullRequest(BaseModel):
    tag: str
    credentials: List[DockerSecret] = Field(default_factory=list)


class ImagePushRequest(BaseModel):
    tag: str
    credentials: List[DockerSecret] = Field(default_factory=list)
    private: bool = False


class ImageImportRequest(BaseModel):
    archive_path: str


class ImageLoadRequest(BaseModel):
    mounts: List[ContainerMountSpec]
    archive_path: str
    private: bool = False


class ImageSaveRequest(BaseModel):
    tag: str
    mounts: List[ContainerMountSpec]
    archive_path: str
    private: bool = False


class ListContainersRequest(BaseModel):
    all: Optional[bool] = None


class _RunRequest(BaseModel):
    request_id: Optional[str] = None
    image: str
    command: Optional[str] = None
    working_dir: Optional[str] = None
    env: Dict[str, str] = Field(default_factory=dict)
    mount_path: Optional[str] = None
    mount_subpath: Optional[str] = None
    role: Optional[str] = None
    participant_name: Optional[str] = None
    ports: Dict[int, int] = Field(default_factory=dict)
    credentials: Optional[List[DockerSecret]] = None
    name: Optional[str] = None
    annotations: Optional[Dict[str, str]] = None
    mounts: Optional[ContainerMountSpec] = None
    writable_root_fs: Optional[bool] = None
    private: Optional[bool] = None


class _BuildRequest(BaseModel):
    tag: str
    mounts: List[ContainerMountSpec]
    context_path: str
    dockerfile_path: Optional[str] = None
    optimize_image: bool = True
    private: bool = False
    credentials: Optional[List[DockerSecret]] = None
    context_archive_path: Optional[str] = None
    context_archive_ref: Optional[str] = None
    context_archive_mount_path: Optional[str] = None
    context_archive_arch: Optional[str] = None


class _ExecRequest(BaseModel):
    request_id: Optional[str] = None
    container_id: str
    command: Optional[list[str]] | str = None
    tty: Optional[bool] = None


class ContainerRunResult(BaseModel):
    container_id: str
    status: Optional[int] = None
    logs: List[str] = Field(default_factory=list)


class ImportedImage(BaseModel):
    resolved_ref: str
    refs: List[str] = Field(default_factory=list)


class BuildJob(BaseModel):
    id: str
    tag: str
    status: Literal["queued", "running", "failed", "cancelled", "succeeded"]
    exit_code: Optional[int] = None


class ContainerStartedBy(BaseModel):
    id: str
    name: str


class RoomContainer(BaseModel):
    id: str
    image: Optional[str] = None
    status: Optional[str] = None
    name: Optional[str] = None
    started_by: ContainerStartedBy
    state: Literal["CREATED", "RUNNING", "EXITED", "UNKNOWN"]
    private: bool
    service_id: Optional[str] = None

    # Accept arbitrary extras (names, created, state, etc.)
    model_config = ConfigDict(extra="allow")


# ---------------------------
# LogStream (awaitable + async generators)
# ---------------------------

T = TypeVar("T")


class LogStream(Generic[T]):
    """
    - await stream: waits for final result (T or None)
    - stream.logs(): async iterator of text lines
    - stream.progress(): async iterator of LogProgress
    - stream.cancel(): cancels on server
    """

    def __init__(
        self,
        *,
        task: asyncio.Task,
        cancel_cb: Callable[[], asyncio.Future[Any]],
    ):
        self._logs_q = asyncio.Queue[Optional[str]]()
        self._progress_q = asyncio.Queue[Optional[LogProgress]]()

        self._cancel_cb = cancel_cb
        self._task = task

        task.add_done_callback(self._on_task_done)

    def _on_task_done(self, t):
        self._logs_q.put_nowait(None)
        self._progress_q.put_nowait(None)

    @property
    def result(self):
        return asyncio.ensure_future(self._task)

    def __await__(self):
        return self.result.__await__()

    async def cancel(self):
        await self._cancel_cb()

    async def logs(self) -> AsyncIterator[str]:
        while True:
            line = await self._logs_q.get()
            if line is None:  # sentinel
                return
            yield line

    async def progress(self) -> AsyncIterator[LogProgress]:
        while True:
            p = await self._progress_q.get()
            if p is None:  # sentinel
                return
            yield p


class _ContainerLogInputStream:
    def __init__(
        self,
        *,
        request_id: str,
        container_id: str,
        follow: bool,
    ):
        self._start_chunk = BinaryContent(
            data=b"",
            headers={
                "kind": "start",
                "request_id": request_id,
                "container_id": container_id,
                "follow": follow,
            },
        )
        self._closed = asyncio.Event()

    def close(self) -> None:
        self._closed.set()

    async def __aiter__(self) -> AsyncIterator[Content]:
        yield self._start_chunk
        await self._closed.wait()


class _BuildLogInputStream:
    def __init__(
        self,
        *,
        request_id: str,
        build_id: str,
        follow: bool,
    ):
        self._start_chunk = BinaryContent(
            data=b"",
            headers={
                "kind": "start",
                "request_id": request_id,
                "build_id": build_id,
                "follow": follow,
            },
        )
        self._closed = asyncio.Event()

    def close(self) -> None:
        self._closed.set()

    async def __aiter__(self) -> AsyncIterator[Content]:
        yield self._start_chunk
        await self._closed.wait()


class _ContainerLogStream(LogStream[T]):
    async def logs(self) -> AsyncIterator[str]:
        try:
            async for line in super().logs():
                yield line
        finally:
            await self.cancel()


class _BuildLogStream(LogStream[T]):
    async def logs(self) -> AsyncIterator[str]:
        try:
            async for line in super().logs():
                yield line
        finally:
            await self.cancel()


# ---------------------------
# Container TTY
# ---------------------------


def _container_string_pairs(
    value: dict[str, str] | None,
) -> list[dict[str, str]]:
    if value is None:
        return []
    return [{"key": key, "value": item} for key, item in value.items()]


def _container_port_pairs(
    value: dict[int, int] | None,
) -> list[dict[str, int]]:
    if value is None:
        return []
    return [
        {"container_port": int(container_port), "host_port": int(host_port)}
        for container_port, host_port in value.items()
    ]


class ExecSession:
    """
    Provides async input/output streams for an interactive container session.
    """

    _INPUT_STREAM_CLOSE = object()

    def __init__(
        self,
        *,
        room: RoomClient,
        request_id: str,
        container_id: str,
        command: Optional[list[str]] | str,
        tty: Optional[bool],
        task: asyncio.Task[int],
    ):
        self._room = room
        self._request_id = request_id
        self._container_id = container_id
        self._command = command
        self._tty = tty
        self._error_q: asyncio.Queue[Optional[bytes]] = asyncio.Queue()
        self._output_q: asyncio.Queue[Optional[bytes]] = asyncio.Queue()
        self._input_q: asyncio.Queue[BinaryContent | object] = asyncio.Queue()
        self._closed = asyncio.ensure_future(task)
        self._task = task
        self._ready = asyncio.Future[bool]()
        self._input_closed = False
        self._last_resize_width: int | None = None
        self._last_resize_height: int | None = None
        task.add_done_callback(self._on_task_done)

    def _on_task_done(self, t):
        del t
        self._close_input_stream()
        self._output_q.put_nowait(None)
        self._error_q.put_nowait(None)
        if not self._ready.done():
            self._ready.set_exception(
                RoomException("container did not start successfully")
            )

    @property
    def result(self):
        return self._closed

    @property
    def request_id(self):
        return self._request_id

    async def input_stream(self) -> AsyncIterator[Content]:
        yield BinaryContent(
            data=b"",
            headers={
                "kind": "start",
                "request_id": self._request_id,
                "container_id": self._container_id,
                "command": self._command,
                "tty": self._tty,
            },
        )

        while True:
            chunk = await self._input_q.get()
            if chunk is self._INPUT_STREAM_CLOSE:
                return
            if not isinstance(chunk, BinaryContent):
                raise RoomException(
                    "container input queue produced an invalid stream chunk"
                )
            yield chunk

    def _close_input_stream(self) -> None:
        if self._input_closed:
            return
        self._input_closed = True
        self._input_q.put_nowait(self._INPUT_STREAM_CLOSE)

    def _queue_input(
        self,
        *,
        channel: int,
        data: bytes = b"",
        width: int | None = None,
        height: int | None = None,
    ) -> None:
        if self._input_closed:
            raise RoomException("container exec session is already closed")
        self._input_q.put_nowait(
            BinaryContent(
                data=data,
                headers={
                    "kind": "input",
                    "channel": channel,
                    "width": width,
                    "height": height,
                },
            )
        )

    async def close_stdin(self) -> None:
        await self._ready
        self._queue_input(channel=255)

    async def write(self, data: bytes) -> None:
        await self._ready
        self._queue_input(channel=1, data=data)

    async def wait_for_ready(self):
        await self._ready

    async def resize(self, *, width: int, height: int) -> None:
        """
        Resize the TTY for the running container.
        This sends a control message (channel 4) to adjust terminal dimensions.
        """
        if self._last_resize_width == width and self._last_resize_height == height:
            return
        self._last_resize_width = width
        self._last_resize_height = height
        self._queue_input(channel=4, width=width, height=height)

    async def stderr(self) -> AsyncIterator[bytes]:
        while True:
            chunk = await self._error_q.get()
            if chunk is None:
                return
            yield chunk

    async def stdout(self) -> AsyncIterator[bytes]:
        while True:
            chunk = await self._output_q.get()
            if chunk is None:
                return
            yield chunk

    async def stop(self):
        self._close_input_stream()

    def _mark_ready(self):
        if not self._ready.done():
            self._ready.set_result(True)

    # Internal
    def _push_output(self, data: bytes):
        self._output_q.put_nowait(data)

    # Internal
    def _push_err(self, data: bytes):
        self._error_q.put_nowait(data)

    async def kill(self):
        if self._input_closed:
            return
        self._queue_input(channel=5)


# ---------------------------
# ContainersClient
# ---------------------------


class ContainersClient:
    def __init__(self, *, room: RoomClient):
        self.room = room

    @staticmethod
    def _unexpected_response_error(*, operation: str) -> RoomException:
        return RoomException(
            f"unexpected return type from containers.{operation}",
            code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
        )

    @staticmethod
    def _image_payload(item: object) -> dict[str, Any]:
        if not isinstance(item, dict):
            raise RoomException(
                "unexpected return type from containers.list_images",
                code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
            )
        payload = dict(item)
        labels = payload.get("labels")
        if isinstance(labels, list):
            normalized_labels = dict[str, str]()
            for entry in labels:
                if not isinstance(entry, dict):
                    raise RoomException(
                        "unexpected return type from containers.list_images",
                        code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
                    )
                key = entry.get("key")
                value = entry.get("value")
                if not isinstance(key, str) or not isinstance(value, str):
                    raise RoomException(
                        "unexpected return type from containers.list_images",
                        code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
                    )
                normalized_labels[key] = value
            payload["labels"] = normalized_labels
        return payload

    @staticmethod
    def _build_request_payload(
        *,
        tag: str,
        mounts: List[ContainerMountSpec],
        context_path: str,
        dockerfile_path: Optional[str] = None,
        optimize_image: bool = True,
        private: bool = False,
        credentials: List[DockerSecret] | None = None,
        context_archive_path: Optional[str] = None,
        context_archive_ref: Optional[str] = None,
        context_archive_mount_path: Optional[str] = None,
        context_archive_arch: Optional[str] = None,
    ) -> dict[str, Any]:
        return {
            "tag": tag,
            "mounts": [
                mount.model_dump(mode="json", exclude_none=True) for mount in mounts
            ],
            "context_path": context_path,
            "dockerfile_path": dockerfile_path,
            "optimize_image": optimize_image,
            "private": private,
            "credentials": [
                credential.model_dump(mode="json") for credential in (credentials or [])
            ],
            "context_archive_path": context_archive_path,
            "context_archive_ref": context_archive_ref,
            "context_archive_mount_path": context_archive_mount_path,
            "context_archive_arch": context_archive_arch,
        }

    async def list_images(self) -> List[Image]:
        res = await self.room.invoke(
            toolkit="containers",
            tool="list_images",
            input={},
        )
        if not isinstance(res, JsonContent):
            raise self._unexpected_response_error(operation="list_images")
        imgs = res["images"]
        return [Image.model_validate(self._image_payload(i)) for i in imgs]

    async def delete_image(self, *, image: str) -> None:
        await self.room.invoke(
            toolkit="containers",
            tool="delete_image",
            input={"image": image},
        )

    async def pull_image(
        self, *, tag: str, credentials: List[DockerSecret] | None = None
    ) -> None:
        await self.room.invoke(
            toolkit="containers",
            tool="pull_image",
            input={
                "tag": tag,
                "credentials": [
                    credential.model_dump(mode="json")
                    for credential in (credentials or [])
                ],
            },
        )

    async def push_image(
        self,
        *,
        tag: str,
        credentials: List[DockerSecret] | None = None,
        private: bool = False,
    ) -> str:
        resp = await self.room.invoke(
            toolkit="containers",
            tool="push_image",
            input={
                "tag": tag,
                "credentials": [
                    credential.model_dump(mode="json")
                    for credential in (credentials or [])
                ],
                "private": private,
            },
        )
        if isinstance(resp, JsonContent):
            container_id: str = resp.json["container_id"]
            return container_id

        raise self._unexpected_response_error(operation="push_image")

    async def load(
        self,
        *,
        archive_path: str,
    ) -> ImportedImage:
        resp = await self.room.invoke(
            toolkit="containers",
            tool="load",
            input={
                "archive_path": archive_path,
            },
        )
        if isinstance(resp, JsonContent):
            return ImportedImage.model_validate(resp.json)

        raise self._unexpected_response_error(operation="load")

    async def load_image(
        self,
        *,
        mounts: List[ContainerMountSpec],
        archive_path: str,
        private: bool = False,
    ) -> str:
        resp = await self.room.invoke(
            toolkit="containers",
            tool="load_image",
            input={
                "mounts": [
                    mount.model_dump(mode="json", exclude_none=True) for mount in mounts
                ],
                "archive_path": archive_path,
                "private": private,
            },
        )
        if isinstance(resp, JsonContent):
            container_id: str = resp.json["container_id"]
            return container_id

        raise self._unexpected_response_error(operation="load_image")

    async def save_image(
        self,
        *,
        tag: str,
        mounts: List[ContainerMountSpec],
        archive_path: str,
        private: bool = False,
    ) -> str:
        resp = await self.room.invoke(
            toolkit="containers",
            tool="save_image",
            input={
                "tag": tag,
                "mounts": [
                    mount.model_dump(mode="json", exclude_none=True) for mount in mounts
                ],
                "archive_path": archive_path,
                "private": private,
            },
        )
        if isinstance(resp, JsonContent):
            container_id: str = resp.json["container_id"]
            return container_id

        raise self._unexpected_response_error(operation="save_image")

    # ---- Run Container ----

    async def run(
        self,
        *,
        image: str,
        command: Optional[str] = None,
        working_dir: Optional[str] = None,
        env: Dict[str, str] | None = None,
        mount_path: Optional[str] = None,
        mount_subpath: Optional[str] = None,
        role: Optional[str] = None,
        participant_name: Optional[str] = None,
        ports: Dict[int, int] | None = None,
        credentials: List[DockerSecret] | None = None,
        name: Optional[str] = None,
        mounts: Optional[ContainerMountSpec] = None,
        writable_root_fs: Optional[bool] = None,
        private: Optional[bool] = None,
    ) -> str:
        resp = await self.room.invoke(
            toolkit="containers",
            tool="run",
            input={
                "image": image,
                "command": command,
                "working_dir": working_dir,
                "env": _container_string_pairs(env),
                "mount_path": mount_path,
                "mount_subpath": mount_subpath,
                "role": role,
                "participant_name": participant_name,
                "ports": _container_port_pairs(ports),
                "credentials": [
                    credential.model_dump(mode="json")
                    for credential in (credentials or [])
                ],
                "name": name,
                "annotations": None,
                "mounts": mounts.model_dump(mode="json", exclude_none=True)
                if mounts is not None
                else None,
                "writable_root_fs": writable_root_fs,
                "private": private,
            },
        )
        if isinstance(resp, JsonContent):
            container_id: str = resp.json["container_id"]
            return container_id

        raise self._unexpected_response_error(operation="run")

    async def start_build(
        self,
        *,
        tag: str,
        mounts: List[ContainerMountSpec],
        context_path: str,
        dockerfile_path: Optional[str] = None,
        optimize_image: bool = True,
        private: bool = False,
        credentials: List[DockerSecret] | None = None,
        context_archive_path: Optional[str] = None,
        context_archive_ref: Optional[str] = None,
        context_archive_mount_path: Optional[str] = None,
        context_archive_arch: Optional[str] = None,
    ) -> str:
        resp = await self.room.invoke(
            toolkit="containers",
            tool="start_build",
            input=self._build_request_payload(
                tag=tag,
                mounts=mounts,
                context_path=context_path,
                dockerfile_path=dockerfile_path,
                optimize_image=optimize_image,
                private=private,
                credentials=credentials,
                context_archive_path=context_archive_path,
                context_archive_ref=context_archive_ref,
                context_archive_mount_path=context_archive_mount_path,
                context_archive_arch=context_archive_arch,
            ),
        )
        if isinstance(resp, JsonContent):
            build_id = resp.json.get("build_id")
            if not isinstance(build_id, str):
                raise self._unexpected_response_error(operation="start_build")
            return build_id

        raise self._unexpected_response_error(operation="start_build")

    async def build(
        self,
        *,
        tag: str,
        mounts: List[ContainerMountSpec],
        context_path: str,
        dockerfile_path: Optional[str] = None,
        optimize_image: bool = True,
        private: bool = False,
        credentials: List[DockerSecret] | None = None,
        context_archive_path: Optional[str] = None,
        context_archive_ref: Optional[str] = None,
        context_archive_mount_path: Optional[str] = None,
        context_archive_arch: Optional[str] = None,
    ) -> str:
        resp = await self.room.invoke(
            toolkit="containers",
            tool="build",
            input=self._build_request_payload(
                tag=tag,
                mounts=mounts,
                context_path=context_path,
                dockerfile_path=dockerfile_path,
                optimize_image=optimize_image,
                private=private,
                credentials=credentials,
                context_archive_path=context_archive_path,
                context_archive_ref=context_archive_ref,
                context_archive_mount_path=context_archive_mount_path,
                context_archive_arch=context_archive_arch,
            ),
        )
        if isinstance(resp, JsonContent):
            build_id = resp.json.get("build_id")
            if not isinstance(build_id, str):
                raise self._unexpected_response_error(operation="build")
            return build_id

        raise self._unexpected_response_error(operation="build")

    async def list_builds(self) -> List[BuildJob]:
        response = await self.room.invoke(
            toolkit="containers",
            tool="list_builds",
            input={},
        )
        if not isinstance(response, JsonContent):
            raise self._unexpected_response_error(operation="list_builds")
        builds = response.json.get("builds")
        if not isinstance(builds, list):
            raise self._unexpected_response_error(operation="list_builds")
        return [BuildJob.model_validate(build) for build in builds]

    async def cancel_build(self, *, build_id: str) -> None:
        await self.room.invoke(
            toolkit="containers",
            tool="cancel_build",
            input={"build_id": build_id},
        )

    async def delete_build(self, *, build_id: str) -> None:
        await self.room.invoke(
            toolkit="containers",
            tool="delete_build",
            input={"build_id": build_id},
        )

    async def run_service(
        self, *, service_id: str, env: Optional[dict[str, str]] = None
    ) -> str:
        resp = await self.room.invoke(
            toolkit="containers",
            tool="run_service",
            input={
                "service_id": service_id,
                "env": _container_string_pairs(env),
            },
        )
        if isinstance(resp, JsonContent):
            container_id: str = resp.json["container_id"]
            return container_id

        raise self._unexpected_response_error(operation="run_service")

    async def exec(
        self,
        *,
        container_id: str,
        command: Optional[list[str]] | str = None,
        tty: Optional[bool] = None,
    ) -> ExecSession:
        request_id = str(uuid.uuid4())

        async def run():
            try:
                response = await self.room.invoke(
                    toolkit="containers",
                    tool="exec",
                    input=container.input_stream(),
                )
                if isinstance(response, Content):
                    raise self._unexpected_response_error(operation="exec")
                container._mark_ready()
                async for chunk in response:
                    if isinstance(chunk, ErrorContent):
                        raise RoomException(chunk.text, code=chunk.code)
                    if isinstance(chunk, _ControlContent):
                        if chunk.method == "close":
                            break
                        raise self._unexpected_response_error(operation="exec")
                    if not isinstance(chunk, BinaryContent):
                        raise self._unexpected_response_error(operation="exec")

                    channel_value = chunk.headers.get("channel")
                    if isinstance(channel_value, bool) or not isinstance(
                        channel_value, int
                    ):
                        raise RoomException(
                            "containers.exec returned a chunk without a valid channel",
                            code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
                        )

                    if channel_value == 1:
                        container._push_output(chunk.data)
                        continue
                    if channel_value == 2:
                        container._push_err(chunk.data)
                        continue
                    if channel_value == 3:
                        status_payload = json.loads(chunk.data.decode("utf-8"))
                        status = status_payload.get("status")
                        if isinstance(status, int):
                            return status
                        raise RoomException(
                            "containers.exec returned an invalid status payload",
                            code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
                        )

                    logger.warning(
                        "ignoring unexpected containers.exec channel %s",
                        channel_value,
                    )

                raise RoomException(
                    "containers.exec stream closed before a status was returned",
                    code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
                )
            finally:
                container._close_input_stream()

        container = ExecSession(
            room=self.room,
            request_id=request_id,
            task=asyncio.create_task(run()),
            container_id=container_id,
            command=command,
            tty=tty,
        )

        return container

    # ---- Logs ----

    def logs(self, *, container_id: str, follow: bool = False) -> LogStream[None]:
        request_id = uuid.uuid4().hex
        input_stream = _ContainerLogInputStream(
            request_id=request_id,
            container_id=container_id,
            follow=follow,
        )
        task: asyncio.Task[None] | None = None

        async def cancel():
            input_stream.close()
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)

        async def _run():
            try:
                response = await self.room.invoke(
                    toolkit="containers",
                    tool="logs",
                    input=input_stream,
                )
                if isinstance(response, Content):
                    raise self._unexpected_response_error(operation="logs")
                async for chunk in response:
                    if isinstance(chunk, ErrorContent):
                        raise RoomException(chunk.text, code=chunk.code)
                    if isinstance(chunk, _ControlContent):
                        if chunk.method == "close":
                            return None
                        raise self._unexpected_response_error(operation="logs")
                    if not isinstance(chunk, BinaryContent):
                        raise self._unexpected_response_error(operation="logs")

                    channel_value = chunk.headers.get("channel")
                    if isinstance(channel_value, bool) or not isinstance(
                        channel_value, int
                    ):
                        raise RoomException(
                            "containers.logs returned a chunk without a valid channel",
                            code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
                        )
                    if channel_value != 1:
                        logger.warning(
                            "ignoring unexpected containers.logs channel %s",
                            channel_value,
                        )
                        continue

                    try:
                        text = chunk.data.decode("utf-8")
                    except UnicodeDecodeError as ex:
                        raise RoomException(
                            "containers.logs returned invalid UTF-8 data",
                            code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
                        ) from ex
                    stream._logs_q.put_nowait(text)
                return None
            finally:
                input_stream.close()

        run_task = asyncio.create_task(_run())
        stream = _ContainerLogStream(
            cancel_cb=cancel,
            task=run_task,
        )
        task = run_task
        return stream

    def get_build_logs(
        self,
        *,
        build_id: str,
        follow: bool = True,
    ) -> LogStream[int | None]:
        request_id = uuid.uuid4().hex
        input_stream = _BuildLogInputStream(
            request_id=request_id,
            build_id=build_id,
            follow=follow,
        )
        task: asyncio.Task[int | None] | None = None

        async def cancel():
            input_stream.close()
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)

        async def _run() -> int | None:
            try:
                response = await self.room.invoke(
                    toolkit="containers",
                    tool="get_build_logs",
                    input=input_stream,
                )
                if isinstance(response, Content):
                    raise self._unexpected_response_error(operation="get_build_logs")
                async for chunk in response:
                    if isinstance(chunk, ErrorContent):
                        raise RoomException(chunk.text, code=chunk.code)
                    if isinstance(chunk, _ControlContent):
                        if chunk.method == "close":
                            return None
                        raise self._unexpected_response_error(
                            operation="get_build_logs"
                        )
                    if not isinstance(chunk, BinaryContent):
                        raise self._unexpected_response_error(
                            operation="get_build_logs"
                        )

                    channel_value = chunk.headers.get("channel")
                    if isinstance(channel_value, bool) or not isinstance(
                        channel_value, int
                    ):
                        raise RoomException(
                            "containers.get_build_logs returned a chunk without a valid channel",
                            code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
                        )
                    if channel_value == 1:
                        try:
                            text = chunk.data.decode("utf-8")
                        except UnicodeDecodeError as ex:
                            raise RoomException(
                                "containers.get_build_logs returned invalid UTF-8 data",
                                code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
                            ) from ex
                        stream._logs_q.put_nowait(text)
                        continue
                    if channel_value == 3:
                        status_payload = json.loads(chunk.data.decode("utf-8"))
                        status = status_payload.get("status")
                        if isinstance(status, int):
                            return status
                        raise RoomException(
                            "containers.get_build_logs returned an invalid status payload",
                            code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
                        )

                    logger.warning(
                        "ignoring unexpected containers.get_build_logs channel %s",
                        channel_value,
                    )
                return None
            finally:
                input_stream.close()

        run_task = asyncio.create_task(_run())
        stream = _BuildLogStream(
            cancel_cb=cancel,
            task=run_task,
        )
        task = run_task
        return stream

    # ---- Misc ----

    async def stop(self, *, container_id: str, force: bool = False) -> None:
        await self.room.invoke(
            toolkit="containers",
            tool="stop_container",
            input={"container_id": container_id, "force": force},
        )

    async def wait_for_exit(self, *, container_id: str) -> int:
        response = await self.room.invoke(
            toolkit="containers",
            tool="wait_for_exit",
            input={"container_id": container_id},
        )
        if not isinstance(response, JsonContent):
            raise self._unexpected_response_error(operation="wait_for_exit")
        exit_code = response.json.get("exit_code")
        if isinstance(exit_code, bool) or not isinstance(exit_code, int):
            raise self._unexpected_response_error(operation="wait_for_exit")
        return exit_code

    async def delete(self, *, container_id: str) -> None:
        await self.room.invoke(
            toolkit="containers",
            tool="delete_container",
            input={"container_id": container_id},
        )

    async def list(self, all: Optional[bool] = None) -> List[RoomContainer]:
        res = await self.room.invoke(
            toolkit="containers",
            tool="list_containers",
            input={"all": all},
        )
        if not isinstance(res, JsonContent):
            raise self._unexpected_response_error(operation="list")
        return [RoomContainer(**c) for c in res["containers"]]


class _GetOfflineOAuthTokenRequest(BaseModel):
    connector: Optional[ConnectorRef] = None
    oauth: Optional[OAuthClientConfig] = None
    delegated_to: Optional[str] = None
    delegated_by: Optional[str] = None


class _GetOfflineOAuthTokenResponse(BaseModel):
    access_token: Optional[str] = None


class SecretRequestInfo(BaseModel):
    url: str
    type: str
    participant_id: str
    timeout: int = 60 * 5
    delegate_to: Optional[str] = None


class _RequestOAuthTokenRequest(BaseModel):
    connector: Optional[ConnectorRef] = None
    oauth: Optional[OAuthClientConfig] = None
    redirect_uri: str
    participant_id: str
    timeout: int = 60 * 5
    delegate_to: Optional[str] = None


class _RequestOAuthTokenResponse(BaseModel):
    access_token: Optional[str] = None


class _DeleteUserSecretRequest(BaseModel):
    id: str
    delegated_to: Optional[str] = None


class _DeleteUserSecretResponse(BaseModel):
    pass


class _DeleteRequestedSecretRequest(BaseModel):
    url: str
    type: str
    delegated_to: Optional[str] = None


class _DeleteRequestedSecretResponse(BaseModel):
    pass


class _ListUserSecretsRequest(BaseModel):
    pass


class SecretInfo(BaseModel):
    id: str
    type: str
    name: str
    delegated_to: Optional[str] = None


class _ListUserSecretsResponse(BaseModel):
    secrets: list[SecretInfo]


class _SecretExistsRequest(BaseModel):
    secret_id: str
    delegated_to: Optional[str] = None
    for_identity: Optional[str] = None


class _SecretExistsResponse(BaseModel):
    exists: bool


class OAuthCredentials(BaseModel):
    access_token: str
    refresh_token: Optional[str] = None
    expiration: Optional[datetime] = None
    scopes: Optional[list[str]] = None


class _ClientRequestOAuthTokenRequest(BaseModel):
    request_id: str
    request: _RequestOAuthTokenRequest
    challenge: Optional[str]


class _ClientRequestOAuthTokenResponse(BaseModel):
    request_id: str
    code: Optional[str] = None
    error: Optional[str] = None


class _ClientRequestSecretRequest(BaseModel):
    request_id: str
    request: SecretRequestInfo


class _ClientRequestSecretResponse(BaseModel):
    # secret will be passed back as data in message
    request_id: str
    error: Optional[str] = None


@dataclass
class OAuthTokenRequest:
    request_id: str
    authorization_endpoint: str
    token_endpoint: str
    challenge: str
    scopes: Optional[list[str]] = None


@dataclass
class SecretRequest:
    request_id: str
    url: str
    type: str
    delegate_to: Optional[str] = None


class _SetSecretRequest(BaseModel):
    secret_id: Optional[str] = Field(default=None)
    type: Optional[str] = Field(default=None)
    name: Optional[str] = Field(default=None)
    delegated_to: Optional[str] = Field(default=None)
    for_identity: Optional[str] = Field(default=None)


class _GetSecretRequest(BaseModel):
    secret_id: Optional[str] = Field(default=None)
    type: Optional[str] = Field(default=None)
    name: Optional[str] = Field(default=None)
    delegated_to: Optional[str] = Field(default=None)


class SecretsClient:
    def __init__(
        self,
        *,
        room: RoomClient,
        oauth_token_request_handler: Optional[
            Callable[[OAuthTokenRequest], Awaitable]
        ] = None,
        secret_request_handler: Optional[Callable[[SecretRequest], Awaitable]] = None,
    ):
        self.room = room
        # Hook server -> client events
        self.room.protocol.register_handler(
            "secrets.request_oauth_token", self._handle_client_oauth_token_request
        )

        self.room.protocol.register_handler(
            "secrets.request_secret", self._handle_request_secret_request
        )

        self._oauth_token_request_handler = oauth_token_request_handler
        self._secret_request_handler = secret_request_handler
        self._pending_authorization_requests = []
        self._pending_secret_requests = []

    @staticmethod
    def _unexpected_response_error(*, operation: str) -> RoomException:
        return RoomException(
            f"unexpected return type from secrets.{operation}",
            code=ErrorCode.UNEXPECTED_RESPONSE_TYPE,
        )

    async def _invoke(self, *, operation: str, input: dict | Content) -> Content:
        return await self.room.invoke(
            toolkit="secrets",
            tool=operation,
            input=input,
        )

    async def _handle_client_oauth_token_request(
        self, protocol: Protocol, message_id: int, type: str, data: bytes
    ) -> None:
        request, bytes = unpack_message(data=data)
        req = _ClientRequestOAuthTokenRequest.model_validate(request)

        if self._oauth_token_request_handler is None:
            raise RoomException("No oauth token handler registered")

        def on_done(t: asyncio.Task):
            try:
                t.result()
            finally:
                self._pending_authorization_requests.remove(t)

        task = asyncio.create_task(
            self._oauth_token_request_handler(
                OAuthTokenRequest(
                    request_id=req.request_id,
                    authorization_endpoint=req.request.oauth.authorization_endpoint,
                    token_endpoint=req.request.oauth.token_endpoint,
                    scopes=req.request.oauth.scopes,
                    challenge=req.challenge,
                )
            )
        )
        task.add_done_callback(on_done)
        self._pending_authorization_requests.append(task)

    async def _handle_request_secret_request(
        self, protocol: Protocol, message_id: int, type: str, data: bytes
    ) -> None:
        request, bytes = unpack_message(data=data)
        req = _ClientRequestSecretRequest.model_validate(request)

        if self._secret_request_handler is None:
            raise RoomException("No secret handler registered")

        def on_done(t: asyncio.Task):
            try:
                t.result()
            finally:
                self._pending_secret_requests.remove(t)

        task = asyncio.create_task(
            self._secret_request_handler(
                SecretRequest(
                    request_id=req.request_id,
                    url=req.request.url,
                    type=req.request.type,
                    delegate_to=req.request.delegate_to,
                )
            )
        )
        task.add_done_callback(on_done)
        self._pending_secret_requests.append(task)

    async def provide_oauth_authorization(
        self,
        *,
        request_id: str,
        code: str,
    ):
        await self._invoke(
            operation="provide_oauth_authorization",
            input=_ClientRequestOAuthTokenResponse(
                request_id=request_id,
                code=code,
                error=None,
            ).model_dump(mode="json"),
        )

    async def reject_oauth_authorization(
        self,
        *,
        request_id: str,
        error: str,
    ):
        await self._invoke(
            operation="provide_oauth_authorization",
            input=_ClientRequestOAuthTokenResponse(
                request_id=request_id,
                code=None,
                error=error,
            ).model_dump(mode="json"),
        )

    async def provide_secret(
        self,
        *,
        request_id: str,
        data: bytes,
    ) -> None:
        await self._invoke(
            operation="provide_secret",
            input=BinaryContent(
                data=data,
                headers={"request_id": request_id, "error": None},
            ),
        )

    async def reject_secret(
        self,
        *,
        request_id: str,
        error: str,
    ) -> None:
        await self._invoke(
            operation="provide_secret",
            input=BinaryContent(
                data=b"",
                headers={"request_id": request_id, "error": error},
            ),
        )

    # get a saved oauth token
    async def get_offline_oauth_token(
        self,
        *,
        connector: Optional[ConnectorRef] = None,
        oauth: Optional[OAuthClientConfig] = None,
        delegated_to: Optional[str] = None,
        delegated_by: Optional[str] = None,
    ):
        req = _GetOfflineOAuthTokenRequest(
            connector=connector,
            oauth=oauth,
            delegated_by=delegated_by,
            delegated_to=delegated_to,
        )
        response = await self._invoke(
            operation="get_offline_oauth_token",
            input=req.model_dump(mode="json"),
        )
        if isinstance(response, JsonContent):
            resp = _GetOfflineOAuthTokenResponse.model_validate(response.json)
            return resp.access_token
        raise self._unexpected_response_error(operation="get_offline_oauth_token")

    async def request_oauth_token(
        self,
        *,
        connector: Optional[ConnectorRef] = None,
        oauth: Optional[OAuthClientConfig] = None,
        timeout: int = 60 * 5,
        from_participant_id: str,
        redirect_uri: str,
        delegate_to: Optional[str] = None,
    ) -> str | None:
        req = _RequestOAuthTokenRequest(
            redirect_uri=redirect_uri,
            timeout=timeout,
            participant_id=from_participant_id,
            oauth=oauth,
            connector=connector,
            delegate_to=delegate_to,
        )
        response = await self._invoke(
            operation="request_oauth_token",
            input=req.model_dump(mode="json"),
        )
        if isinstance(response, JsonContent):
            resp = _RequestOAuthTokenResponse.model_validate(response.json)
            return resp.access_token
        raise self._unexpected_response_error(operation="request_oauth_token")

    async def list_secrets(self) -> list[SecretInfo]:
        response = await self._invoke(
            operation="list_secrets",
            input=_ListUserSecretsRequest().model_dump(mode="json"),
        )
        if isinstance(response, JsonContent):
            resp = _ListUserSecretsResponse.model_validate(response.json)
            return resp.secrets
        raise self._unexpected_response_error(operation="list_secrets")

    async def exists(
        self,
        *,
        secret_id: str,
        delegated_to: Optional[str] = None,
        for_identity: Optional[str] = None,
    ) -> bool:
        response = await self._invoke(
            operation="exists",
            input=_SecretExistsRequest(
                secret_id=secret_id,
                delegated_to=delegated_to,
                for_identity=for_identity,
            ).model_dump(mode="json"),
        )
        if isinstance(response, JsonContent):
            resp = _SecretExistsResponse.model_validate(response.json)
            return resp.exists
        raise self._unexpected_response_error(operation="exists")

    async def delete_secret(self, *, id: str, delegated_to: Optional[str] = None):
        response = await self._invoke(
            operation="delete_secret",
            input=_DeleteUserSecretRequest(
                id=id,
                delegated_to=delegated_to,
            ).model_dump(mode="json"),
        )
        if isinstance(response, (EmptyContent, JsonContent)):
            return
        raise self._unexpected_response_error(operation="delete_secret")

    async def delete_requested_secret(
        self,
        *,
        url: str,
        type: str,
        delegated_to: Optional[str] = None,
    ) -> None:
        response = await self._invoke(
            operation="delete_requested_secret",
            input=_DeleteRequestedSecretRequest(
                url=url,
                type=type,
                delegated_to=delegated_to,
            ).model_dump(mode="json"),
        )
        if isinstance(response, (EmptyContent, JsonContent)):
            return
        raise self._unexpected_response_error(operation="delete_requested_secret")

    async def request_secret(
        self,
        *,
        url: str,
        type: str,
        timeout: int = 60 * 5,
        from_participant_id: str,
        delegate_to: Optional[str] = None,
    ) -> bytes:
        req = SecretRequestInfo(
            url=url,
            type=type,
            participant_id=from_participant_id,
            timeout=timeout,
            delegate_to=delegate_to,
        )
        response = await self._invoke(
            operation="request_secret",
            input=req.model_dump(mode="json"),
        )
        if isinstance(response, FileContent):
            return response.data
        raise self._unexpected_response_error(operation="request_secret")

    async def set_secret(
        self,
        *,
        secret_id: Optional[str] = None,
        type: Optional[str] = None,
        name: Optional[str] = None,
        delegated_to: Optional[str] = None,
        for_identity: Optional[str] = None,
        data: Optional[bytes] = None,
    ) -> None:
        """
        Store/update a secret for the current user (or delegated target).
        """
        req = _SetSecretRequest(
            secret_id=secret_id,
            type=type,
            name=name,
            delegated_to=delegated_to,
            for_identity=for_identity,
        )
        if data is None:
            raise RoomException(
                "secret data is required",
                code=ErrorCode.INVALID_REQUEST,
            )

        response = await self._invoke(
            operation="set_secret",
            input=BinaryContent(
                data=data,
                headers={**req.model_dump(mode="json"), "has_data": True},
            ),
        )

        if isinstance(response, (EmptyContent, JsonContent)):
            return
        raise self._unexpected_response_error(operation="set_secret")

    async def get_secret(
        self,
        *,
        secret_id: Optional[str] = None,
        type: Optional[str] = None,
        name: Optional[str] = None,
        delegated_to: Optional[str] = None,
    ) -> Optional[FileContent]:
        """
        Fetch secret bytes. Returns FileContent (name/mime_type/data) or None if not found.
        """
        req = _GetSecretRequest(
            secret_id=secret_id,
            type=type,
            name=name,
            delegated_to=delegated_to,
        )

        response = await self._invoke(
            operation="get_secret",
            input=req.model_dump(mode="json"),
        )

        if isinstance(response, EmptyContent):
            return None

        if isinstance(response, FileContent):
            return response

        raise self._unexpected_response_error(operation="get_secret")
