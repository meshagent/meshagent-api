from meshagent.api.protocol import Protocol, ClientProtocol
from meshagent.api.specs.service import ContainerMountSpec, ServiceSpec
import json
import asyncio
import logging
import os
import aiohttp
from meshagent.api.websocket_protocol import WebSocketClientProtocol
from meshagent.api.participant_token import ApiScope
from pydantic import BaseModel, Field, JsonValue, ConfigDict, TypeAdapter
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
import uuid

from datetime import datetime

from abc import ABC, abstractmethod
from dataclasses import dataclass

from meshagent.api.urls import websocket_room_url


def _decode_record_value(value: Any) -> Any:
    if isinstance(value, list):
        return [_decode_record_value(item) for item in value]

    if isinstance(value, dict):
        if (
            "encoding" in value
            and "data" in value
            and len(value) == 2
            and value["encoding"] == "base64"
        ):
            return base64.b64decode(value["data"].encode())

        return {k: _decode_record_value(v) for k, v in value.items()}

    return value


def decode_records(records: list[dict]):
    for r in records:
        if isinstance(r, dict):
            for k in r.keys():
                r[k] = _decode_record_value(r[k])

    return records


def _encode_record_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {
            "encoding": "base64",
            "data": base64.b64encode(value).decode(),
        }

    if isinstance(value, list):
        return [_encode_record_value(item) for item in value]

    if isinstance(value, dict):
        return {k: _encode_record_value(v) for k, v in value.items()}

    return value


def encode_records(records: list[dict]):
    transformed_records = []

    for r in records:
        c = {}
        for k in r.keys():
            c[k] = _encode_record_value(r[k])

        transformed_records.append(c)

    return transformed_records


logger = logging.getLogger("room_server_client")
logger.setLevel(logging.WARN)


class RoomException(Exception):
    def __init__(self, message: str, *, status_code: int = 400):
        self.status_code = status_code
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
        self, *, id: str, role: Optional[str] = None, attributes: Optional[dict] = None
    ):
        if attributes is None:
            attributes = {}

        if role is None:
            role = "unknown"

        self._role = role

        super().__init__(id=id, attributes=attributes)

    def set_attribute(self, name: str, value):
        raise ("You can't set the attributes of another participant")

    @property
    def role(self):
        return self._role


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
        to: RemoteParticipant,
    ):
        super().__init__(
            from_participant_id=from_participant_id,
            type=type,
            message=message,
            attachment=attachment,
        )
        self.to = to
        self.fut = asyncio.Future()


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
        _, pending = await asyncio.wait(
            [
                startup_task,
                close_task,
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )

        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.sync.stop()
        await self.messaging.stop()
        await self.protocol.__aexit__(None, None, None)
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
                    pr.fut.set_exception(RoomException(response.text))
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


T = TypeVar("T")


class _RefCount(Generic[T]):
    def __init__(self, ref: T):
        self.ref = ref
        self.count = 1


class SyncClient:
    def __init__(self, *, room: RoomClient):
        self.room = room
        room.protocol.register_handler("room.sync", self._handle_sync)

        self._connected_documents = dict[str, _RefCount[MeshDocument]]()
        self._connecting_documents = dict[
            str, asyncio.Future[_RefCount[MeshDocument]]
        ]()
        self._sync_ch = Chan[_QueuedSync]()
        self._main_task = None

    def get_open_documents(self) -> dict[str, MeshDocument]:
        open_documents = {}
        for k, v in self._connected_documents.items():
            open_documents[k] = v.ref
        return open_documents

    async def _main(self):
        import contextlib

        with contextlib.suppress(asyncio.CancelledError):
            async for q in self._sync_ch:
                logger.info("client sync sending for {path}".format(path=q.path))
                await self.room.send_request(
                    "room.sync", {"path": q.path}, q.base64.encode("utf-8")
                )

    async def start(self):
        if self._main_task is not None:
            raise Exception("client already started")

        self._main_task = asyncio.create_task(self._main())

    async def stop(self):
        self._sync_ch.close()

        if self._main_task is not None:
            await asyncio.gather(self._main_task)

    async def create(self, *, path: str, json: Optional[dict] = None) -> None:
        await self.room.send_request("room.create", {"path": path, "json": json})

    async def describe(self, *, path: str, create: bool = True) -> MeshDocument:
        res = await self.room.send_request("room.describe", {"path": path})
        assert isinstance(res, JsonContent)
        return res.json

    async def open(
        self,
        *,
        path: str,
        create: bool = True,
        initial_json: Optional[dict] = None,
        schema: Optional[MeshSchema] = None,
    ) -> MeshDocument:
        if path in self._connecting_documents:
            await self._connecting_documents[path]

        if path in self._connected_documents:
            doc = self._connected_documents[path]
            doc.count = doc.count + 1
            return doc.ref

        # todo: add support for state vector / partial updates
        # todo: initial bytes loading

        connecting_fut = asyncio.Future[_RefCount[MeshDocument]]()
        self._connecting_documents[path] = connecting_fut

        def publish_sync(base64: str):
            self._sync_ch.send_nowait(_QueuedSync(path=path, base64=base64))

        # if locally cached, can send state vector
        # vec = doc.get_state_vector()
        # "vector": base64.standard_b64encode(vec).decode("utf-8")
        try:
            extra = {}

            if schema is not None:
                extra["schema"] = schema.to_json()

            if initial_json is not None:
                extra["initial_json"] = initial_json

            response = await self.room.send_request(
                "room.connect", {"path": path, "create": create, **extra}
            )

            schema_json = response["schema"]
            doc: MeshDocument = runtime.new_document(
                schema=MeshSchema.from_json(schema_json),
                on_document_sync=publish_sync,
                factory=MeshDocument,
            )

            ref = _RefCount(doc)
            self._connected_documents[path] = ref
            connecting_fut.set_result(ref)
            self._connecting_documents.pop(path)

            logger.info("Connected to %s", path)

        except asyncio.CancelledError:
            raise

        except Exception as e:
            connecting_fut.set_exception(e)
            self._connecting_documents.pop(path)
            raise

        await doc.synchronized
        return doc

    async def close(self, *, path: str) -> None:
        if path not in self._connected_documents:
            raise RoomException("Not connected to " + path)

        ref = self._connected_documents[path]
        ref.count = ref.count - 1
        if ref.count == 0:
            doc = self._connected_documents.pop(path)
            await self.room.send_request("room.disconnect", {"path": path})
            runtime._unregister_document(doc=doc.ref)

    async def sync(self, *, path: str, data: bytes) -> None:
        await self.room.send_request("room.sync", {"path": path}, data=data)

    async def _handle_sync(
        self, protocol: Protocol, message_id: int, type: str, data: bytes
    ) -> None:
        header, payload = unpack_message(data=data)
        path = header["path"]

        if path in self._connecting_documents:
            # Wait for document to be fully connected and initialized
            await self._connecting_documents[path]

        if path in self._connected_documents:
            doc = self._connected_documents[path]

            try:
                runtime.apply_backend_changes(doc.ref.id, payload.decode("utf-8"))
                if not doc.ref.synchronized.done():
                    doc.ref.synchronized.set_result(True)

            except ChanClosed:
                # ignore channel closing during sync (happens if connection is closed after receiving changes from server)
                pass
        else:
            logger.debug("received change for a document that is not connected:" + path)


ToolContentType = Literal[
    "json",
    "text",
    "file",
    "link",
    "empty",
]
_SUPPORTED_TOOL_CONTENT_KINDS: set[str] = {
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


class ServicesClient:
    def __init__(self, *, room: RoomClient):
        self.room = room

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

        response = await self.room.send_request(
            "services.list",
            {},
        )

        if not isinstance(response, JsonContent):
            raise RoomException("Invalid return type from list services call")

        return _ListServicesResponse.model_validate(response.json)

    async def restart(self, *, service_id: str) -> None:
        """
        Restart a managed room service by service id.
        """
        await self.room.send_request(
            "services.restart",
            {"service_id": service_id},
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
        try:
            self._on_close()
        except Exception as ex:
            logger.error("tool call stream cleanup failed", exc_info=ex)

    def close_with_error(self, error: BaseException) -> None:
        self._close(error=error)
        if not self._task.done():
            self._task.cancel()

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
        except Exception as ex:
            self._close(error=ex)

    def _push_chunk(self, response_chunk: Content) -> None:
        if self._closed:
            return

        self._queue.put_nowait(response_chunk)
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
                self._close()

    async def __aiter__(self) -> AsyncIterator[Content]:
        while True:
            item = await self._queue.get()
            if item is None:
                if self._error is not None:
                    raise self._error
                return
            yield item

    def stream(self) -> AsyncIterator[Content]:
        return self.__aiter__()


class AgentsClient:
    def __init__(self, *, room: RoomClient):
        self.room = room
        self._tool_call_streams: Dict[str, _ToolCallChunkStream] = {}
        self._close_watcher_task: Optional[asyncio.Task[None]] = None
        self.room.protocol.register_handler(
            "agent.tool_call_response_chunk", self._handle_tool_call_response_chunk
        )

    def _ensure_close_watcher(self) -> None:
        if self._close_watcher_task is not None:
            return

        async def watch_for_close() -> None:
            await self.room.protocol.wait_for_close()
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

        self.room.emit(
            "agent.tool_call_response_chunk",
            event={
                "tool_call_id": tool_call_id,
                "toolkit": header.get("toolkit"),
                "tool": header.get("tool"),
                "chunk": chunk_payload,
            },
        )

    async def make_call(
        self, *, name: str, url: str, arguments: dict, api: Optional[ApiScope] = None
    ) -> None:
        await self.room.send_request(
            "agent.call",
            _MakeCallRequest(
                name=name, url=url, arguments=arguments, api=api
            ).model_dump(mode="json"),
        )
        return None

    def _make_tool_call_stream(
        self, *, tool_call_id: str, request_task: asyncio.Task[Content]
    ) -> _ToolCallChunkStream:
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
        await self.room.send_request(
            "agent.tool_call_request_chunk",
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
            self.room.send_request(
                "agent.invoke_tool",
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
                    call_stream.close_with_error(
                        RoomException(f"request stream failed: {ex}")
                    )

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
    ) -> List[ToolkitDescription]:
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

        response = await self.room.send_request("agent.list_toolkits", request)
        # 'response["tools"]' is assumed to be a dict of toolkits by name
        toolkits_data = response["tools"]

        result = []
        for toolkit_name, tk_json in toolkits_data.items():
            # Parse top-level toolkit properties
            title = tk_json.get("title", "")
            description = tk_json.get("description", "")
            thumbnail_url = tk_json.get("thumbnail_url", None)
            participant_id = tk_json.get("participant_id", None)

            # Each toolkit has a dict of 'tools'
            tool_descriptions = []
            if "tools" in tk_json:
                for tool_name, tool_info in tk_json["tools"].items():
                    input_spec = ToolContentSpec.from_json(
                        tool_info.get("input_spec", None)
                    )
                    output_spec = ToolContentSpec.from_json(
                        tool_info.get("output_spec", None)
                    )

                    # Backwards compatibility for servers still sending top-level schema fields.
                    legacy_input_schema = tool_info.get("input_schema", None)
                    if isinstance(legacy_input_schema, dict) and input_spec is None:
                        input_spec = ToolContentSpec(
                            types=["json"],
                            stream=False,
                            schema=legacy_input_schema,
                        )

                    legacy_output_schema = tool_info.get("output_schema", None)
                    if isinstance(legacy_output_schema, dict):
                        if output_spec is None:
                            output_spec = ToolContentSpec(
                                types=["json"],
                                stream=False,
                                schema=legacy_output_schema,
                            )
                        elif (
                            output_spec.includes("json") and output_spec.schema is None
                        ):
                            output_spec = ToolContentSpec(
                                types=[*output_spec.types],
                                stream=output_spec.stream,
                                schema=legacy_output_schema,
                            )

                    tool_descriptions.append(
                        ToolDescription(
                            name=tool_name,
                            title=tool_info.get("title", ""),
                            description=tool_info.get("description", ""),
                            input_spec=input_spec,
                            output_spec=output_spec,
                            thumbnail_url=tool_info.get("thumbnail_url", None),
                            defs=tool_info.get("defs", None),
                            pricing=tool_info.get("pricing", None),
                            supports_context=tool_info.get("supports_context", False),
                        )
                    )

            toolkit = ToolkitDescription(
                name=toolkit_name,
                title=title,
                description=description,
                tools=tool_descriptions,
                thumbnail_url=thumbnail_url,
                participant_id=participant_id,
            )
            result.append(toolkit)

        return result


class LivekitConnectionInfo:
    def __init__(self, *, url: str, token: str):
        self.url = url
        self.token = token


class LivekitClient:
    def __init__(self, *, room: RoomClient):
        self.room = room

    async def get_connection_info(
        self, *, breakout_room: Optional[str] = None
    ) -> LivekitConnectionInfo:
        response = await self.room.send_request(
            "livekit.connect", {"breakout_room": breakout_room}
        )

        return LivekitConnectionInfo(
            url=response["url"],
            token=response["token"],
        )


class StorageEntry(BaseModel):
    name: str
    is_folder: bool
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
        room.protocol.register_handler("storage.file.updated", self._on_file_updated)

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

        response = await self.room.send_request("storage.exists", {"path": path})
        return response.json["exists"]

    async def stat(self, *, path: str) -> StorageEntry | None:
        response = (await self.room.send_request("storage.stat", {"path": path})).json
        exists = response["exists"]
        if not exists:
            return None
        else:
            return StorageEntry(
                name=response["name"],
                is_folder=response["is_folder"],
                created_at=datetime.fromisoformat(response["created_at"])
                if response.get("created_at") is not None
                else None,
                updated_at=datetime.fromisoformat(response["updated_at"])
                if response.get("updated_at") is not None
                else None,
            )

    async def open(self, *, path: str, overwrite: bool = False):
        """
        Opens a file for writing. Returns a file handle that can be used to
        write data or close the file.

        Arguments:
            path (str): The file path to open.
            overwrite (bool): Whether to overwrite if the file already exists.
                              Defaults to False.

        Returns:
            FileHandle: An object representing an open file.

        Example:
            handle = await storage_client.open(path="files/new.txt", overwrite=True)
        """

        response = await self.room.send_request(
            "storage.open", {"path": path, "overwrite": overwrite}
        )
        return FileHandle(id=response["handle"])

    async def write(self, *, handle: FileHandle, data: bytes) -> None:
        """
        Writes binary data to an open file handle.

        Arguments:
            handle (FileHandle): The file handle to which data will be written.
            data (bytes): The data to be written.

        Returns:
            None

        Example:
            data_to_write = b"Sample data"
            await storage_client.write(handle=my_handle, data=data_to_write)
        """

        await self.room.send_request("storage.write", {"handle": handle.id}, data=data)

    async def close(self, *, handle: FileHandle):
        """
        Closes an open file handle, ensuring all data has been written.

        Arguments:
            handle (FileHandle): The file handle to close.

        Returns:
            None

        Example:
            await storage_client.close(handle=my_handle)
        """

        await self.room.send_request("storage.close", {"handle": handle.id})

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

        response = await self.room.send_request("storage.download", {"path": path})
        return response

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

        response = await self.room.send_request("storage.download_url", {"path": path})
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

        response = await self.room.send_request("storage.list", {"path": path})
        return list(
            map(
                lambda f: StorageEntry(
                    name=f["name"],
                    is_folder=f["is_folder"],
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

    async def delete(self, path: str, recursive: Optional[True] = None):
        """
        Deletes a file  at the given path.

        Arguments:
            path (str): The file to delete.

        Returns:
            None

        Example:
            await storage_client.delete("folder/old_file.txt")
        """

        await self.room.send_request(
            "storage.delete", {"path": path, "recursive": recursive}
        )


class Queue:
    def __init__(self, *, name: str, size: int):
        self._name = name

    @property
    def name(self):
        return self._name

    @property
    def size(self):
        return self._size


class QueuesClient:
    def __init__(self, *, room: RoomClient):
        self.room = room

    async def list(
        self, *, name: str, message: dict, create: bool = True
    ) -> list[Queue]:
        response = await self.room.send_request("queues.list", {})
        queues = []
        if isinstance(response, JsonContent):
            for item in response.json["queues"]:
                queues.append(Queue(name=item["name"], size=int(item["size"])))
        return queues

    async def send(self, *, name: str, message: dict, create: bool = True) -> None:
        (
            await self.room.send_request(
                "queues.send", {"name": name, "create": create, "message": message}
            )
        )

    async def drain(self, *, name: str) -> None:
        (await self.room.send_request("queues.drain", {"name": name}))

    async def close(self, *, name: str) -> None:
        (await self.room.send_request("queues.close", {"name": name}))

    async def receive(
        self, *, name: str, create: bool = True, wait: bool = True
    ) -> dict | None:
        response = await self.room.send_request(
            "queues.receive", {"name": name, "create": create, "wait": wait}
        )
        if isinstance(response, EmptyContent):
            return None
        elif isinstance(response, JsonContent):
            return response.json
        elif isinstance(response, TextContent):
            return response.text
        else:
            raise RoomException("Unexpected response")


class MessagingClient:
    def __init__(self, *, room: RoomClient):
        self.room = room
        self._participants = dict[str, RemoteParticipant]()
        self._events = {}
        self._on_stream_accept_callback = None
        room.protocol.register_handler("messaging.send", self._handle_message_send)
        self._pending_streams: Dict[str, asyncio.Future] = {}

        self._remote_streams: Dict[str, MessageStream] = {}
        self._message_queue = Chan[_QueuedRoomMessage]()
        self._send_task = None

    @property
    def remote_participants(self) -> list[RemoteParticipant]:
        """
        get the other participants in the room with messaging enabled.
        """
        return list(self._participants.values())

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

    async def enable(
        self,
        *,
        on_stream_accept: Optional[Callable[["MessageStream"], None]] = None,
    ):
        await self.room.send_request("messaging.enable", {})
        self._on_stream_accept_callback = on_stream_accept

    async def disable(self):
        await self.room.send_request("messaging.disable", {})

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
        elif message.type == "stream.open":
            self._on_stream_open(message)
        elif message.type == "stream.accept":
            self._on_stream_accept(message)
        elif message.type == "stream.reject":
            self._on_stream_reject(message)
        elif message.type == "stream.chunk":
            self._on_stream_chunk(message)
        elif message.type == "stream.close":
            self._on_stream_close(message)
        else:
            self.emit("message", message=message)

    async def start(self):
        self._send_task = asyncio.create_task(self._send_messages())

    async def stop(self):
        self._message_queue.close()
        if self._send_task is not None:
            await asyncio.gather(self._send_task)

    async def _send_messages(self):
        async for msg in self._message_queue:
            try:
                body = {
                    "type": msg.type,
                    "message": msg.message,
                }

                body["to_participant_id"] = msg.to.id
                await self.room.send_request(
                    "messaging.send", body, data=msg.attachment
                )
                msg.fut.set_result(True)

            except asyncio.CancelledError:
                raise

            except Exception as ex:
                logger.info("Unable to send message to participant", exc_info=ex)
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
        )

        self._message_queue.send_nowait(msg)

        await msg.fut

    async def broadcast_message(
        self, *, type: str, message: dict, attachment: Optional[bytes] = None
    ):
        await self.room.send_request(
            "messaging.broadcast", {"type": type, "message": message}, data=attachment
        )

    def _on_participant_enabled(self, message: RoomMessage):
        data = message.message
        participant = RemoteParticipant(id=data["id"], role=data["role"])

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
        part = self._participants.pop(message.message["id"], None)
        if part is not None:
            self.emit("participant_removed", participant=part)

        for stream_id, stream in list(self._remote_streams.items()):
            if stream.to.id == part.id:
                stream._close()
                logger.warning(
                    f"stream {stream_id} closing due to disconnect of {stream.to.get_attribute('name')}"
                )
                self._remote_streams.pop(stream_id)

    def _on_messaging_enabled(self, message: RoomMessage):
        for data in message.message["participants"]:
            participant = RemoteParticipant(id=data["id"], role=data["role"])

            for k, v in data["attributes"].items():
                participant._attributes[k] = v

            self._participants[data["id"]] = participant

        self.emit("messaging_enabled")

    async def create_stream(self, *, to: Participant, header: dict) -> "MessageStream":
        stream_id = str(uuid.uuid4())  # Generate unique ID
        future = asyncio.Future()

        # Construct the writer
        stream = MessageStream(stream_id=stream_id, to=to, client=self, header=None)
        self._remote_streams[stream_id] = stream
        self._pending_streams[stream_id] = future

        # Send "stream.open"
        await self.send_message(
            to=to,
            type="stream.open",
            message={"stream_id": stream_id, "header": header},
        )

        # Wait for remote side to accept or reject
        await future
        return stream

    def _on_stream_open(self, message: RoomMessage):
        logger.info("stream open request recieved")
        """
        A remote participant is opening a new stream to us.
        We'll either accept or reject it, depending on `_on_stream_accept_callback`.
        """
        from_participant_id = message.from_participant_id
        from_participant = self._participants.get(from_participant_id, None)

        def on_send_complete(task: asyncio.Task):
            try:
                task.result()

            except Exception as e:
                logger.warning("unable to send stream response", exc_info=e)

        if not from_participant:
            # If we don't know who this is, reject
            send = asyncio.create_task(
                self.send_message(
                    to=None,  # no participant needed if we can't identify
                    type="stream.reject",
                    message={
                        "stream_id": message.message["stream_id"],
                        "error": "unknown participant",
                    },
                )
            )
            send.add_done_callback(on_send_complete)
            return

        stream_id = message.message["stream_id"]

        try:
            if self._on_stream_accept_callback is None:
                raise Exception("Streams are not allowed by this client")

            stream = MessageStream(
                stream_id=stream_id,
                to=from_participant,
                client=self,
                header=message.message,
            )

            self._on_stream_accept_callback(stream)
            self._remote_streams[stream_id] = stream

            logger.info(f"accepting stream {stream_id}")
            # Accept
            send = asyncio.create_task(
                self.send_message(
                    to=from_participant,
                    type="stream.accept",
                    message={"stream_id": stream_id},
                )
            )
            send.add_done_callback(on_send_complete)

        except asyncio.CancelledError:
            raise

        except Exception as e:
            logger.info(f"rejecting stream {stream_id}")
            # Reject
            send = asyncio.create_task(
                self.send_message(
                    to=from_participant,
                    type="stream.reject",
                    message={"stream_id": stream_id, "error": str(e)},
                )
            )
            send.add_done_callback(on_send_complete)
            return

    def _on_stream_accept(self, message: RoomMessage):
        """
        The remote side accepted our stream request.
        Complete the Future<MessageStreamWriter>.
        """
        stream_id = message.message["stream_id"]
        future = self._pending_streams.pop(stream_id, None)
        if future and not future.done():
            future.set_result(True)

    def _on_stream_reject(self, message: RoomMessage):
        """
        The remote side rejected our stream request.
        Complete the Future with an error.
        """
        stream_id = message.message["stream_id"]
        err = message.message.get(
            "error", "The stream was rejected by the remote client"
        )

        future = self._pending_streams.pop(stream_id, None)
        self._remote_streams.pop(stream_id)
        if future and not future.done():
            future.set_exception(Exception(err))

    def _on_stream_chunk(self, message: RoomMessage):
        """
        A chunk arrived on an existing stream.
        """
        stream_id = message.message["stream_id"]
        reader = self._remote_streams.get(stream_id, None)
        if reader:
            chunk = MessageStreamChunk(
                header=message.message["header"], data=message.attachment
            )
            reader._add_chunk(chunk)
        else:
            logger.warning(f"received a chunk for an unregistered stream {stream_id}")

    def _on_stream_close(self, message: RoomMessage):
        """
        The remote side closed the stream.
        """
        stream_id = message.message["stream_id"]
        stream = self._remote_streams.pop(stream_id, None)
        if stream:
            stream._close()


class MessageStreamChunk:
    def __init__(self, header: dict, data: Optional[bytes] = None):
        self.header = header
        self.data = data


class MessageStream:
    def __init__(
        self,
        stream_id: str,
        to: Participant,
        client: MessagingClient,
        header: Optional[dict] = None,
    ):
        self._stream_id = stream_id
        self._to = to
        self._client = client
        self._header = header
        self._queue = asyncio.Queue()
        self.closed = False

    @property
    def to(self):
        return self._to

    @property
    def header(self):
        return self._header

    async def read_chunks(self):
        """
        An async generator that yields `MessageStreamChunk` objects
        until the remote side closes the stream.
        """
        while True:
            chunk = await self._queue.get()
            if chunk is None:
                # Stream was closed
                break
            yield chunk

    def _add_chunk(self, chunk: MessageStreamChunk):
        """
        Internal: called by the MessagingClient when receiving a new chunk.
        """
        self._queue.put_nowait(chunk)

    def _close(self):
        """
        Internal: called by the MessagingClient when the remote side closes the stream.
        """
        if self.closed:
            return

        self.closed = True

        self._queue.put_nowait(None)

    async def write(self, chunk: MessageStreamChunk):
        """
        Sends a "stream.chunk" message to the remote participant.
        """

        if self.closed:
            raise RoomException("stream is closed")

        await self._client.send_message(
            to=self._to,
            type="stream.chunk",
            message={
                "stream_id": self._stream_id,
                "header": chunk.header,
            },
            attachment=chunk.data,
        )

    async def close(self):
        """
        Sends a "stream.close" message to the remote participant.
        """

        if self.closed:
            raise RoomException("stream is closed")

        self._close()

        await self._client.send_message(
            to=self._to, type="stream.close", message={"stream_id": self._stream_id}
        )


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

        type = raw_json.get("type", "unknown")
        data = raw_json.get("data", {})

        self.emit("log", type=type, data=data)

    async def log(self, *, type: str, data: dict):
        await self._room.send_request(
            type="developer.log", request={"type": type, "data": data}
        )

    def log_nowait(self, *, type: str, data: dict):
        task = asyncio.ensure_future(
            self._room.send_request(
                type="developer.log", request={"type": type, "data": data}
            )
        )
        task.add_done_callback(
            lambda t: self._handle_developer_log_result(
                t, kind="log", payload={"type": type, "data": data}
            )
        )

    def info(self, message: str, *, extra: Optional[dict] = None):
        task = asyncio.ensure_future(
            self._room.send_request(
                type="developer.info", request={"message": message, "extra": extra}
            )
        )
        task.add_done_callback(
            lambda t: self._handle_developer_log_result(
                t, kind="info", payload={"message": message, "extra": extra}
            )
        )

    def warning(self, message: str, *, extra: Optional[dict] = None):
        task = asyncio.ensure_future(
            self._room.send_request(
                type="developer.warning", request={"message": message, "extra": extra}
            )
        )
        task.add_done_callback(
            lambda t: self._handle_developer_log_result(
                t, kind="warning", payload={"message": message, "extra": extra}
            )
        )

    def error(self, message: str, *, extra: Optional[dict] = None):
        task = asyncio.ensure_future(
            self._room.send_request(
                type="developer.error", request={"message": message, "extra": extra}
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

    async def enable(self):
        await self._room.send_request(type="developer.watch", request={})

    async def disable(self):
        await self._room.send_request(type="developer.unwatch", request={})


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
        BinaryDataType,
    ],
    Field(discriminator="type"),
]
_data_type_adapter = TypeAdapter(DataTypeUnion)
VectorDataType.model_rebuild()
ListDataType.model_rebuild()
StructDataType.model_rebuild()

CreateMode = Literal["create", "overwrite", "create_if_not_exists"]


class _CreateTableRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    data: Optional[Any] = None
    table_schema: Optional[Dict[str, DataTypeUnion]] = Field(
        default=None, alias="schema"
    )
    mode: CreateMode = "create"
    namespace: Optional[list[str]] = None
    metadata: Optional[dict] = None


class _ListTablesRequest(BaseModel):
    namespace: Optional[list[str]] = None


class _TableRequest(BaseModel):
    table: str
    namespace: Optional[list[str]] = None


class _InspectTableRequest(_TableRequest):
    pass


class _DropTableRequest(BaseModel):
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


class _SearchRequest(_TableRequest):
    text: Optional[str] = None
    vector: Optional[list[float]] = None
    text_columns: Optional[list[str]] = None
    where: Optional[str] = None
    offset: Optional[int] = None
    limit: Optional[int] = None
    select: Optional[List[str]] = None


class _CountRequest(_TableRequest):
    text: Optional[str] = None
    vector: Optional[list[float]] = None
    text_columns: Optional[list[str]] = None
    where: Optional[str] = None


class _OptimizeRequest(_TableRequest):
    pass


class _RestoreRequest(_TableRequest):
    version: int


class _CheckoutRequest(_TableRequest):
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


class _ListIndexesRequest(_TableRequest):
    pass


MemoryIngestStrategy = Literal["heuristic", "llm"]


class MemoryEntityRecord(BaseModel):
    entity_id: Optional[str] = None
    name: str
    entity_type: Optional[str] = None
    context: Optional[str] = None
    confidence: Optional[float] = None
    metadata: Optional[dict[str, str]] = None


class MemoryRelationshipRecord(BaseModel):
    source_entity_id: str
    target_entity_id: str
    relationship_type: str = "RELATED_TO"
    description: Optional[str] = None
    confidence: Optional[float] = None
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


class MemoryRecallItem(BaseModel):
    entity_id: str
    name: str
    entity_type: str
    context: Optional[str] = None
    confidence: Optional[float] = None
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


class _SqlRequest(BaseModel):
    query: str
    tables: List[SqlTableReference]
    params: Optional[Dict[str, Any]] = None


class DatabaseClient:
    """
    A client for interacting with the 'database' extension on the room server.
    """

    def __init__(self, room: RoomClient):
        """
        :param room: The RoomClient used to send requests.
        """
        self.room = room

    async def list_tables(self, *, namespace: Optional[list[str]] = None) -> List[str]:
        """
        List all tables in the database.

        :return: A list of table names.
        """
        request_model = _ListTablesRequest(namespace=namespace)
        response: JsonContent = await self.room.send_request(
            "database.list_tables", request_model.model_dump()
        )
        return response.json.get("tables", [])

    async def inspect(
        self, *, table: str, namespace: Optional[list[str]] = None
    ) -> dict[str, DataType]:
        request_model = _InspectTableRequest(table=table, namespace=namespace)
        response: JsonContent = await self.room.send_request(
            "database.inspect", request_model.model_dump()
        )

        schema = dict[str, DataType]()

        for k, v in response.json["schema"].items():
            schema[k] = _data_type_adapter.validate_python(v)

        return schema

    async def _create_table(
        self,
        *,
        name: str,
        data: Optional[Any] = None,
        schema: Optional[Dict[str, DataType]] = None,
        mode: Optional[CreateMode] = "create",
        namespace: Optional[list[str]] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        """
        Create a new table.

        :param name: Table name.
        :param data: Optional initial data (list/dict).
        :param schema: Optional schema definition.
        :param mode: "create" or "overwrite" (default: "create")
        :return: Server response dict containing "status", "table", etc.
        """

        request_model = _CreateTableRequest(
            name=name,
            data=data,
            table_schema=schema,
            mode=mode,
            namespace=namespace,
            metadata=metadata,
        )
        await self.room.send_request(
            "database.create_table", request_model.model_dump(by_alias=True)
        )
        return None

    async def create_table_with_schema(
        self,
        *,
        name: str,
        schema: Optional[Dict[str, DataType]] = None,
        data: Optional[List[dict]] = None,
        mode: Optional[CreateMode] = "create",
        namespace: Optional[list[str]] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        return await self._create_table(
            name=name,
            schema=schema,
            mode=mode,
            data=data,
            namespace=namespace,
            metadata=metadata,
        )

    async def create_table_from_data(
        self,
        *,
        name: str,
        data: Optional[list[dict]] = None,
        mode: Optional[CreateMode] = "create",
        namespace: Optional[list[str]] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        return await self._create_table(
            name=name,
            data=data,
            mode=mode,
            namespace=namespace,
            metadata=metadata,
        )

    async def drop_table(
        self,
        *,
        name: str,
        ignore_missing: bool = False,
        namespace: Optional[list[str]] = None,
    ) -> None:
        """
        Drop (delete) a table.

        :param name: Table name.
        :param ignore_missing: If True, ignore if table doesn't exist.
        """
        request_model = _DropTableRequest(
            name=name, ignore_missing=ignore_missing, namespace=namespace
        )
        await self.room.send_request("database.drop_table", request_model.model_dump())
        return None

    async def drop_index(
        self, *, table: str, name: str, namespace: Optional[list[str]] = None
    ) -> None:
        """
        Drop (delete) a index.

        :param table: table name
        :param name: index name.
        """
        request_model = _DropIndexRequest(table=table, name=name, namespace=namespace)
        await self.room.send_request("database.drop_index", request_model.model_dump())
        return None

    async def add_columns(
        self,
        *,
        table: str,
        new_columns: Dict[str, str | DataType],
        namespace: Optional[list[str]] = None,
    ) -> None:
        """
        Add new columns to an existing table.

        :param table: Table name.
        :param new_columns: Dict of {column_name: default_value_expression}.
        """

        request_model = _AddColumnsRequest(
            table=table, new_columns=new_columns, namespace=namespace
        )
        await self.room.send_request("database.add_columns", request_model.model_dump())
        return None

    # TODO: not ready yet on lance side
    async def _alter_columns(
        self,
        *,
        table: str,
        columns: Dict[str, DataType],
        namespace: Optional[list[str]] = None,
    ) -> None:
        """
        Add new columns to an existing table.

        :param table: Table name.
        :param new_columns: Dict of {column_name: default_value_expression}.
        """

        request_model = _AlterColumnsRequest(
            table=table, columns=columns, namespace=namespace
        )
        await self.room.send_request(
            "database.alter_columns", request_model.model_dump()
        )
        return None

    async def drop_columns(
        self, *, table: str, columns: List[str], namespace: Optional[list[str]] = None
    ) -> None:
        """
        Drop columns from an existing table.

        :param table: Table name.
        :param columns: List of column names to drop.
        """
        request_model = _DropColumnsRequest(
            table=table, columns=columns, namespace=namespace
        )
        await self.room.send_request(
            "database.drop_columns", request_model.model_dump()
        )
        return None

    async def insert(
        self,
        *,
        table: str,
        records: List[Dict[str, Any]],
        namespace: Optional[list[str]] = None,
    ) -> None:
        """
        Insert new records into a table.

        :param table: Table name.
        :param records: The record(s) to insert (list or dict).
        """

        request_model = _InsertRequest(
            table=table,
            records=encode_records(records),
            namespace=namespace,
        )
        await self.room.send_request("database.insert", request_model.model_dump())

    async def update(
        self,
        *,
        table: str,
        where: str,
        values: Optional[Dict[str, Any]] = None,
        values_sql: Optional[Dict[str, str]] = None,
        namespace: Optional[list[str]] = None,
    ) -> None:
        """
        Update existing records in a table.

        :param table: Table name.
        :param where: SQL WHERE clause (e.g. "id = 123").
        :param values: Dict of column updates, e.g. {"col1": "new_value"}.
        :param values_sql: Dict of SQL expressions for updates, e.g. {"col2": "col2 + 1"}.
        """
        request_model = _UpdateRequest(
            table=table,
            where=where,
            values=values,
            values_sql=values_sql,
            namespace=namespace,
        )
        await self.room.send_request("database.update", request_model.model_dump())

    async def delete(
        self, *, table: str, where: str, namespace: Optional[list[str]] = None
    ) -> None:
        """
        Delete records from a table.

        :param table: Table name.
        :param where: SQL WHERE clause (e.g. "id = 123").
        """
        request_model = _DeleteRequest(table=table, where=where, namespace=namespace)
        await self.room.send_request("database.delete", request_model.model_dump())

        return None

    async def merge(
        self,
        *,
        table: str,
        on: str,
        records: Any,
        namespace: Optional[list[str]] = None,
    ) -> None:
        """
        Merge (upsert) records into a table.

        :param table: Table name.
        :param on: Column name to match on (e.g. "id").
        :param records: The record(s) to merge.
        """
        request_model = _MergeRequest(
            table=table, on=on, records=records, namespace=namespace
        )
        await self.room.send_request("database.merge", request_model.model_dump())
        return None

    async def sql(
        self,
        *,
        query: str,
        tables: List[SqlTableReference | str],
        params: Optional[Dict[str, Any]] = None,
    ) -> list[Dict[str, Any]]:
        """
        Execute a SQL query against one or more tables.

        :param query: SQL statement to execute.
        :param tables: Tables to register for the query.
        :param params: Typed parameters for DataFusion parameter binding.
        """
        table_refs = [
            SqlTableReference(name=table) if isinstance(table, str) else table
            for table in tables
        ]
        request_model = _SqlRequest(
            query=query,
            tables=table_refs,
            params=params,
        )
        response = await self.room.send_request(
            "database.sql", request_model.model_dump()
        )
        if isinstance(response, JsonContent):
            return decode_records(response.json["results"])
        return []

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
    ) -> list[Dict[str, Any]]:
        """
        Search for records in a table.

        :param table: Table name.
        :param text: The search text
        :param where: A filter clause or values to match
        :param limit: Limit the number of results.
        :param select: Columns to select.
        """

        if isinstance(where, dict):
            where = " AND ".join(
                map(lambda x: f"{x} = {json.dumps(where[x])}", where.keys())
            )
        request_model = _SearchRequest(
            table=table,
            where=where,
            text=text,
            vector=vector,
            offset=offset,
            limit=limit,
            select=select,
            namespace=namespace,
        )
        response = await self.room.send_request(
            "database.search", request_model.model_dump()
        )
        if isinstance(response, JsonContent):
            return decode_records(response.json["results"])
        return []

    async def count(
        self,
        *,
        table: str,
        text: Optional[str] = None,
        vector: Optional[list[float]] = None,
        where: Optional[str] | dict = None,
        namespace: Optional[list[str]] = None,
    ) -> list[Dict[str, Any]]:
        """
        Search for records in a table.

        :param table: Table name.
        :param text: The search text
        :param where: A filter clause or values to match
        """

        if isinstance(where, dict):
            where = " AND ".join(
                map(lambda x: f"{x} = {json.dumps(where[x])}", where.keys())
            )
        request_model = _CountRequest(
            table=table,
            where=where,
            text=text,
            vector=vector,
            namespace=namespace,
        )
        response = await self.room.send_request(
            "database.count", request_model.model_dump()
        )
        if isinstance(response, JsonContent):
            return response.json["count"]
        return []

    async def optimize(
        self, *, table: str, namespace: Optional[list[str]] = None
    ) -> None:
        """
        Optimize (compact/prune) a table.

        :param table: Table name.
        """
        request_model = _OptimizeRequest(table=table, namespace=namespace)
        await self.room.send_request("database.optimize", request_model.model_dump())
        return None

    async def restore(
        self, *, table: str, version: int, namespace: Optional[list[str]] = None
    ) -> None:
        """
        restore a table version.

        :param table: Table name.
        """
        request_model = _RestoreRequest(
            table=table, version=version, namespace=namespace
        )
        await self.room.send_request("database.restore", request_model.model_dump())
        return None

    async def checkout(
        self, *, table: str, version: int, namespace: Optional[list[str]] = None
    ) -> None:
        """
        checkout a table version.

        :param table: Table name.
        """
        request_model = _CheckoutRequest(
            table=table, version=version, namespace=namespace
        )
        await self.room.send_request("database.checkout", request_model.model_dump())
        return None

    async def list_versions(
        self, *, table: str, namespace: Optional[list[str]] = None
    ) -> list["TableVersion"]:
        """
        list a table's versions

        :param table: Table name.
        """
        request_model = _ListVersionsRequest(table=table, namespace=namespace)
        resp = await self.room.send_request(
            "database.list_versions", request_model.model_dump()
        )
        return [TableVersion.model_validate(v) for v in resp.json["versions"]]

    async def create_vector_index(
        self,
        *,
        table: str,
        column: str,
        replace: Optional[bool] = None,
        namespace: Optional[list[str]] = None,
    ) -> None:
        """
        Create a vector index on a given column.

        :param table: Table name.
        :param column: Vector column name.
        """
        request_model = _CreateVectorIndexRequest(
            table=table,
            column=column,
            replace=replace,
            namespace=namespace,
        )
        await self.room.send_request(
            "database.create_vector_index", request_model.model_dump()
        )
        return None

    async def create_scalar_index(
        self,
        *,
        table: str,
        column: str,
        replace: Optional[bool] = None,
        namespace: Optional[list[str]] = None,
    ) -> None:
        """
        Create a scalar index on a given column.

        :param table: Table name.
        :param column: Column name.
        """
        request_model = _CreateScalarIndexRequest(
            table=table,
            column=column,
            replace=replace,
            namespace=namespace,
        )
        await self.room.send_request(
            "database.create_scalar_index", request_model.model_dump()
        )
        return None

    async def create_full_text_search_index(
        self,
        *,
        table: str,
        column: str,
        replace: Optional[bool] = None,
        namespace: Optional[list[str]] = None,
    ) -> None:
        """
        Create a full-text search index on a given text column.

        :param table: Table name.
        :param column: Text column name.
        """
        request_model = _CreateFullTextSearchIndexRequest(
            table=table,
            column=column,
            replace=replace,
            namespace=namespace,
        )
        await self.room.send_request(
            "database.create_full_text_search_index",
            request_model.model_dump(),
        )
        return None

    async def list_indexes(
        self, *, table: str, namespace: Optional[list[str]] = None
    ) -> list["TableIndex"]:
        """
        List all indexes on a table.

        :param table: Table name.
        """
        request_model = _ListIndexesRequest(table=table, namespace=namespace)
        response = await self.room.send_request(
            "database.list_indexes", request_model.model_dump()
        )
        if isinstance(response, JsonContent):
            return [TableIndex.model_validate(i) for i in response.json["indexes"]]

        raise RoomException("unexpected return type")


class MemoryClient:
    """
    A client for interacting with the 'memory' extension on the room server.
    """

    def __init__(self, room: RoomClient):
        self.room = room

    async def list(self, *, namespace: Optional[List[str]] = None) -> List[str]:
        request_model = _MemoryListRequest(namespace=namespace)
        response = await self.room.send_request(
            "memory.list", request_model.model_dump()
        )
        if isinstance(response, JsonContent):
            return list(response.json.get("memories", []))

        raise RoomException("unexpected return type")

    async def create(
        self,
        *,
        name: str,
        namespace: Optional[List[str]] = None,
        overwrite: bool = False,
    ) -> None:
        request_model = _MemoryCreateRequest(
            name=name, namespace=namespace, overwrite=overwrite
        )
        await self.room.send_request("memory.create", request_model.model_dump())

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
        await self.room.send_request("memory.drop", request_model.model_dump())

    async def inspect(
        self, *, name: str, namespace: Optional[List[str]] = None
    ) -> MemoryDetails:
        request_model = _MemoryInspectRequest(name=name, namespace=namespace)
        response = await self.room.send_request(
            "memory.inspect", request_model.model_dump()
        )
        if isinstance(response, JsonContent):
            return MemoryDetails.model_validate(response.json)

        raise RoomException("unexpected return type")

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
        response = await self.room.send_request(
            "memory.query", request_model.model_dump()
        )
        if isinstance(response, JsonContent):
            return decode_records(response.json["results"])

        raise RoomException("unexpected return type")

    async def upsert_table(
        self,
        *,
        name: str,
        table: str,
        records: List[Dict[str, Any]],
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
        await self.room.send_request("memory.upsert_table", request_model.model_dump())

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
        await self.room.send_request("memory.upsert_nodes", request_model.model_dump())

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
        await self.room.send_request(
            "memory.upsert_relationships", request_model.model_dump()
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
        response = await self.room.send_request(
            "memory.ingest_text", request_model.model_dump()
        )
        if isinstance(response, JsonContent):
            return MemoryIngestResult.model_validate(response.json)

        raise RoomException("unexpected return type")

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
        response = await self.room.send_request(
            "memory.ingest_image", request_model.model_dump(), data
        )
        if isinstance(response, JsonContent):
            return MemoryIngestResult.model_validate(response.json)

        raise RoomException("unexpected return type")

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
        response = await self.room.send_request(
            "memory.ingest_file", request_model.model_dump()
        )
        if isinstance(response, JsonContent):
            return MemoryIngestResult.model_validate(response.json)

        raise RoomException("unexpected return type")

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
        response = await self.room.send_request(
            "memory.ingest_from_table", request_model.model_dump()
        )
        if isinstance(response, JsonContent):
            return MemoryIngestResult.model_validate(response.json)

        raise RoomException("unexpected return type")

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
        response = await self.room.send_request(
            "memory.ingest_from_storage", request_model.model_dump()
        )
        if isinstance(response, JsonContent):
            return MemoryIngestResult.model_validate(response.json)

        raise RoomException("unexpected return type")

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
        response = await self.room.send_request(
            "memory.recall", request_model.model_dump()
        )
        if isinstance(response, JsonContent):
            return MemoryRecallResult.model_validate(response.json)

        raise RoomException("unexpected return type")

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
        response = await self.room.send_request(
            "memory.delete_entities", request_model.model_dump()
        )
        if isinstance(response, JsonContent):
            return MemoryDeleteEntitiesResult.model_validate(response.json)

        raise RoomException("unexpected return type")

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
        response = await self.room.send_request(
            "memory.delete_relationships", request_model.model_dump()
        )
        if isinstance(response, JsonContent):
            return MemoryDeleteRelationshipsResult.model_validate(response.json)

        raise RoomException("unexpected return type")

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
        response = await self.room.send_request(
            "memory.optimize", request_model.model_dump()
        )
        if isinstance(response, JsonContent):
            return MemoryOptimizeResult.model_validate(response.json)

        raise RoomException("unexpected return type")


class TableVersion(BaseModel):
    timestamp: datetime
    version: int
    metadata: dict[str, JsonValue]


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


class _ExecRequest(BaseModel):
    request_id: Optional[str] = None
    container_id: str
    command: Optional[list[str]] | str = None
    tty: Optional[bool] = None


class ContainerRunResult(BaseModel):
    container_id: str
    status: Optional[int] = None
    logs: List[str] = Field(default_factory=list)


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


# ---------------------------
# Container TTY
# ---------------------------


class ExecSession:
    """
    Provides async input/output streams for an interactive container session.
    """

    def __init__(self, *, room: RoomClient, request_id: str, task: asyncio.Task):
        self._room = room
        self._request_id = request_id
        self._error_q: asyncio.Queue[Optional[bytes]] = asyncio.Queue()
        self._output_q: asyncio.Queue[Optional[bytes]] = asyncio.Queue()
        self._closed = asyncio.ensure_future(task)
        self._task = task
        self._ready = asyncio.Future[bool]()
        task.add_done_callback(self._on_task_done)

    def _on_task_done(self, t):
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

    async def close_stdin(self) -> None:
        await self._ready
        # If server supports TTY input; adjust route name if different.
        await self._room.send_request(
            "containers.container_input",
            {"request_id": self._request_id, "channel": 255},
            data=b"",
        )

    async def write(self, data: bytes) -> None:
        await self._ready
        # If server supports TTY input; adjust route name if different.
        await self._room.send_request(
            "containers.container_input",
            {"request_id": self._request_id, "channel": 1},
            data=data,
        )

    async def wait_for_ready(self):
        await self._ready

    async def resize(self, *, width: int, height: int) -> None:
        """
        Resize the TTY for the running container.
        This sends a control message (channel 4) to adjust terminal dimensions.
        """
        await self._room.send_request(
            "containers.container_input",
            {
                "request_id": self._request_id,
                "channel": 4,
                "width": width,
                "height": height,
            },
        )

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
        # send a kill message on channel 5
        await self._room.send_request(
            "containers.container_input",
            {"request_id": self._request_id, "channel": 5},
            data={},
        )

    def _mark_ready(self):
        self._ready.set_result(True)

    # Internal
    def _push_output(self, data: bytes):
        self._output_q.put_nowait(data)

    # Internal
    def _push_err(self, data: bytes):
        self._error_q.put_nowait(data)

    async def kill(self):
        await self._room.send_request(
            "containers.stop_exec",
            {
                "request_id": self._request_id,
            },
        )


# ---------------------------
# ContainersClient
# ---------------------------


class ContainersClient:
    def __init__(self, *, room: RoomClient):
        self.room = room
        # Hook server -> client events
        self.room.protocol.register_handler(
            "containers.log.chunk", self._handle_log_chunk
        )
        self.room.protocol.register_handler(
            "containers.run.output", self._handle_container_run_chunk
        )
        self.room.protocol.register_handler(
            "containers.progress", self._handle_progress
        )

        self._ttys: Dict[str, ExecSession] = {}
        self._log_streams = dict[str, LogStream]()

    # ---- Event handlers ----

    async def _handle_log_chunk(
        self, protocol: Protocol, message_id: int, typ: str, data: bytes
    ):
        header, _ = unpack_message(data)
        req_id = header["request_id"]
        log_line = header.get("log", "")
        q = self._log_streams.get(req_id)
        if q:
            q._logs_q.put_nowait(str(log_line))

    async def _handle_container_run_chunk(
        self, protocol: Protocol, message_id: int, typ: str, data: bytes
    ):
        header, payload = unpack_message(data)

        req_id: str = header["request_id"]
        channel: int = int(header["channel"])

        tty = self._ttys.get(req_id)
        if tty is None:
            logger.warning("received output from missing container %s", req_id)
            return  # tty closed or missing

        if channel == 2:
            tty._push_err(payload)

        elif channel == 1:
            tty._push_output(payload)

        elif channel == -1:
            control = json.loads(payload)
            if control["started"]:
                tty._mark_ready()
            else:
                logger.warning("unexpected control message, started missing")
        else:
            logger.warning("unexpected message received")

    async def _handle_progress(
        self, protocol: Protocol, message_id: int, typ: str, data: bytes
    ):
        header, _ = unpack_message(data)
        req_id = header["request_id"]
        detail = header.get("detail") or {}
        lp = LogProgress(
            layer=header.get("layer"),
            message=header.get("message"),
            current=(detail.get("current") if detail else None),
            total=(detail.get("total") if detail else None),
        )
        pq = self._log_streams.get(req_id)
        if pq:
            pq._progress_q.put_nowait(lp)

    # ---- High-level API ----

    async def list_images(self) -> List[Image]:
        res = await self.room.send_request("containers.list_images", {})
        imgs = res["images"]
        return [Image.model_validate(i) for i in imgs]

    async def delete_image(self, *, image: str) -> None:
        await self.room.send_request("containers.delete_image", {"image": image})

    # ---- Streaming helpers ----

    def _make_stream(
        self,
        *,
        cancel_cb: Callable[[], asyncio.Future[Any]],
        task: asyncio.Task,
        request_id: str,
    ) -> LogStream:
        log_stream = LogStream(cancel_cb=cancel_cb, task=task)
        self._log_streams[request_id] = log_stream

        def _pop(t):
            self._log_streams.pop(request_id)

        task.add_done_callback(_pop)

        return log_stream

    async def pull_image(
        self, *, tag: str, credentials: List[DockerSecret] | None = None
    ) -> None:
        req = ImagePullRequest(tag=tag, credentials=credentials or [])

        await self.room.send_request("containers.pull_image", req.model_dump())

        return None

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
        request_id = uuid.uuid4().hex

        req = _RunRequest(
            name=name,
            request_id=request_id,
            image=image,
            command=command,
            working_dir=working_dir,
            env=env or {},
            mount_path=mount_path,
            mount_subpath=mount_subpath,
            role=role,
            participant_name=participant_name,
            ports=ports or {},
            credentials=credentials or [],
            mounts=mounts,
            writable_root_fs=writable_root_fs,
            private=private,
        )

        resp = await self.room.send_request(
            "containers.run", req.model_dump(exclude_none=True)
        )
        if isinstance(resp, JsonContent):
            container_id: str = resp.json["container_id"]
            return container_id

        else:
            raise RoomException(f"Unexpected response type {resp}")

    async def run_service(
        self, *, service_id: str, env: Optional[dict[str, str]] = None
    ) -> str:
        req = {
            "service_id": service_id,
            "env": env,
        }

        resp = await self.room.send_request("containers.run_service", req)
        if isinstance(resp, JsonContent):
            container_id: str = resp.json["container_id"]
            return container_id

        else:
            raise RoomException(f"Unexpected response type {resp}")

    async def exec(
        self,
        *,
        container_id: str,
        command: Optional[list[str]] | str = None,
        tty: Optional[bool] = None,
    ) -> ExecSession:
        request_id = str(uuid.uuid4())

        req = _ExecRequest(
            request_id=request_id,
            container_id=container_id,
            command=command,
            tty=tty,
        )

        async def run():
            try:
                resp = await self.room.send_request(
                    "containers.exec", req.model_dump(exclude_none=True)
                )

                # close TTY on completion

                status = (
                    resp.json["status"]
                    if isinstance(resp, JsonContent)
                    else resp.get("status", "")
                )

                return status
            finally:
                self._ttys.pop(request_id, None)

        container = ExecSession(
            room=self.room, request_id=request_id, task=asyncio.create_task(run())
        )
        self._ttys[request_id] = container

        return container

    # ---- Logs ----

    def logs(self, *, container_id: str, follow: bool = False) -> LogStream[None]:
        request_id = uuid.uuid4().hex

        async def cancel():
            await self.room.send_request(
                "containers.stop_logs", {"request_id": request_id}
            )

        async def _run():
            await self.room.send_request(
                "containers.logs",
                {"request_id": request_id, "id": container_id, "follow": follow},
            )
            return None

        stream = self._make_stream(
            cancel_cb=cancel, task=asyncio.create_task(_run()), request_id=request_id
        )
        return stream

    # ---- Misc ----

    async def stop(self, *, container_id: str, force: bool = False) -> None:
        await self.room.send_request(
            "containers.stop_container", {"id": container_id, "force": force}
        )

    async def delete(self, *, container_id: str) -> None:
        await self.room.send_request(
            "containers.delete_container", {"id": container_id}
        )

    async def list(self, all: Optional[bool] = None) -> List[RoomContainer]:
        res = await self.room.send_request(
            "containers.list_containers",
            ListContainersRequest(all=all).model_dump(mode="json"),
        )
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
        await self.room.send_request(
            "secrets.provide_oauth_authorization",
            {
                "code": code,
                "request_id": request_id,
            },
        )

    async def reject_oauth_authorization(
        self,
        *,
        request_id: str,
        error: str,
    ):
        await self.room.send_request(
            "secrets.provide_oauth_authorization",
            {
                "error": error,
                "request_id": request_id,
            },
        )

    async def provide_secret(
        self,
        *,
        request_id: str,
        data: bytes,
    ) -> None:
        await self.room.send_request(
            "secrets.provide_secret",
            {
                "request_id": request_id,
            },
            data=data,
        )

    async def reject_secret(
        self,
        *,
        request_id: str,
        error: str,
    ) -> None:
        await self.room.send_request(
            "secrets.provide_secret",
            {
                "request_id": request_id,
                "error": error,
            },
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
        response = await self.room.send_request(
            "secrets.get_offline_oauth_token", req.model_dump(mode="json")
        )
        if isinstance(response, JsonContent):
            resp = _GetOfflineOAuthTokenResponse.model_validate(response.json)
            return resp.access_token
        else:
            raise RoomException("Invalid response received, expected JsonContent")

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
        response = await self.room.send_request(
            "secrets.request_oauth_token", req.model_dump(mode="json")
        )
        if isinstance(response, JsonContent):
            resp = _RequestOAuthTokenResponse.model_validate(response.json)
            return resp.access_token
        else:
            raise RoomException("Invalid response received, expected JsonContent")

    async def list_secrets(self) -> list[SecretInfo]:
        response = await self.room.send_request(
            "secrets.list_secrets", _ListUserSecretsRequest().model_dump(mode="json")
        )
        if isinstance(response, JsonContent):
            resp = _ListUserSecretsResponse.model_validate(response.json)
            return resp.secrets
        else:
            raise RoomException("Invalid response received, expected JsonContent")

    async def delete_secret(self, *, id: str, delegated_to: Optional[str] = None):
        await self.room.send_request(
            "secrets.delete_secret",
            _DeleteUserSecretRequest(id=id, delegated_to=delegated_to).model_dump(
                mode="json"
            ),
        )

    async def delete_requested_secret(
        self,
        *,
        url: str,
        type: str,
        delegated_to: Optional[str] = None,
    ) -> None:
        await self.room.send_request(
            "secrets.delete_requested_secret",
            _DeleteRequestedSecretRequest(
                url=url,
                type=type,
                delegated_to=delegated_to,
            ).model_dump(mode="json"),
        )

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
        response = await self.room.send_request(
            "secrets.request_secret", req.model_dump(mode="json")
        )
        if isinstance(response, FileContent):
            return response.data
        raise RoomException("Invalid response received, expected FileContent")

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

        response = await self.room.send_request(
            "secrets.set_secret",
            req.model_dump(mode="json"),
            data=data,
        )

        if isinstance(response, (EmptyContent, JsonContent)):
            return
        raise RoomException(
            "Invalid response received, expected EmptyContent or JsonContent"
        )

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

        response = await self.room.send_request(
            "secrets.get_secret",
            req.model_dump(mode="json"),
        )

        if isinstance(response, EmptyContent):
            return None

        if isinstance(response, FileContent):
            return response

        raise RoomException(
            "Invalid response received, expected FileContent or EmptyContent"
        )
