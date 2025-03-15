from meshagent.api.protocol import Protocol, ClientProtocol
import json
import asyncio
import logging
from typing import Optional, Callable, Dict, List, Any, Literal

from meshagent.api.runtime import runtime, RuntimeDocument
from meshagent.api.schema import MeshSchema
from meshagent.api.messaging import split_message_header, split_message_payload, pack_message
from meshagent.api.participant import Participant
from meshagent.api.chan import Chan
from meshagent.api.messaging import unpack_response, ErrorResponse, JsonResponse, TextResponse, EmptyResponse, FileResponse
import uuid

from abc import ABC, abstractmethod

logger = logging.getLogger("room_server_client")
logger.setLevel(logging.WARN)

class RoomException(Exception):
    pass

class Requirement(ABC):
    def __init__(self, *, name: str):
        self.name = name

    @staticmethod
    def from_json(r: dict) -> 'Requirement':
        if "toolkit" in r:
            return RequiredToolkit(name=r["toolkit"], tools=r["tools"])

        if "schema" in r:
            return RequiredSchema(name=r["schema"])
        
        raise RoomException("invalid requirement json")

    @abstractmethod
    def to_json(self):
        pass
        

class RequiredToolkit(Requirement):
    # Require a toolkit to be present for this tool to execute, optionally a list of specific tools in the toolkit
    def __init__(self, *, name: str, tools: Optional[list['str']] = None):
        super().__init__(name=name)
        self.tools = tools

    def to_json(self):
        return { "toolkit" : self.name, "tools" : self.tools }

class RequiredSchema(Requirement):
    def __init__(self, *, name: str):
        super().__init__(name=name)
    
    def to_json(self):
        return { "schema" : self.name }  

class _QueuedSync:
    def __init__(self, path: str, base64: str, protocol: ClientProtocol | None = None):
        self.path = path
        self.base64 = base64
        self.protocol = protocol

class _PendingRequest:
    def __init__(self):
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
        await self.protocol.send("set_attributes", json.dumps({ name: value}).encode("utf-8"))
        
class RemoteParticipant(Participant):
    def __init__(self, *, id: str, role: Optional[str] = None, attributes: Optional[dict] = None):
        if attributes == None:
            attributes = {}

        if role == None:
            role = "unknown"

        self._role = role
    
        super().__init__(id=id, attributes=attributes)

    def set_attribute(self, name: str, value):   
        raise("You can't set the attributes of another participant")
    
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
    def __init__(self, *, from_participant_id: str, type: str, message: dict, attachment: Optional[bytes] = None):
        self.from_participant_id = from_participant_id
        self.type = type
        self.message = message
        self.attachment = attachment

class RoomClient:

    def __init__(self, *, protocol: ClientProtocol):
        self.protocol = protocol
        self.protocol.register_handler("room_ready", self._handle_ready)
        self.protocol.register_handler("connected", self._handle_participant)
        self.protocol.register_handler("__response__", self._handle_response)

        self._pending_requests = dict[int, _PendingRequest]()
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
        
        self._room_url = None
        self._room_name = None
        self._session_id = None

    def on(self, event_name: str, func: Callable):
        if event_name not in self._events:
            self._events[event_name] = []
        self._events[event_name].append(func)
       
    def emit(self, event_name,  **kwargs):
        """Call all handlers associated with the given event."""
        handlers = self._events.get(event_name, [])
        for handler in handlers:
            handler(**kwargs)

    
    async def __aenter__(self):

        await self.protocol.__aenter__()

        await self._ready
        
        await self.sync.start()

        await self._local_participant_ready

        return self
    
    async def __aexit__(self, exc_type, exc, tb):        
        
        await self.sync.stop()
        await self.protocol.__aexit__(None, None, None)

        return
    
    @property
    def session_id(self) -> str:
        if self._session_id == None:
            raise RoomException("session_id is not available before the room is ready")
        
        return self._session_id
    
    @property
    def room_url(self) -> str:
        if self._room_url == None:
            raise RoomException("room url is not available before the room is ready")
        
        return self._room_url
        
    @property
    def room_name(self) -> str:
        if self._room_name == None:
            raise RoomException("room name is not available before the room is ready")
        
        return self._room_name
    
    # send a request, optionally with a binary trailer
    async def send_request(self, type: str, request: dict, data:bytes | None = None) -> FileResponse | None | dict | str:
        
        logger.info("sending request %s", type)
        request_id = self.protocol.next_message_id()
       
        pr = _PendingRequest()
        self._pending_requests[request_id] = pr

        message = pack_message(header=request, data=data)

        await self.protocol.send(type=type, data=message, message_id=request_id)
        return await pr.fut
    
    async def _handle_ready(self, protocol: Protocol, message_id: int, type: str, data: bytes) -> None:
        init = json.loads(data)

        self._room_name = init["room_name"]
        self._room_url = init["room_url"]
        self._session_id = init["session_id"]

        self._ready.set_result(True)        
    
    async def _handle_response(self, protocol: Protocol, message_id: int, type: str, data: bytes) -> None:
        logger.info("received response %s", type)

        response = unpack_response(data=data)
        
        request_id = message_id
        if request_id in self._pending_requests:
            pr = self._pending_requests.pop(request_id)
            if isinstance(response, ErrorResponse):
                pr.fut.set_exception(RoomException(response.text))
            else:
                pr.fut.set_result(response)
        else:
            logger.warning("received a response for a request that is not pending {id}".format(id=request_id))
        return
    
    @property
    def local_participant(self):
        return self._local_participant

    def _on_participant_init(self, participant_id: str, attributes: dict):
        self._local_participant = LocalParticipant(id=participant_id, attributes=attributes, protocol=self.protocol)
        self._local_participant_ready.set_result(True)   
    
    async def _handle_participant(self, protocol, message_id, msg_type, data):
        # Decode and parse the message
        message = json.loads(data.decode('utf-8'))        
        type = message["type"]

        if type == "init":
            participant_id = message["participantId"]
            attributes = message["attributes"]
            self._on_participant_init(participant_id, attributes)
            
class _RefCount[T]:
    def __init__(self, ref: T):
        self.ref = ref
        self.count = 0

class SyncClient:
    def __init__(self, *, room: RoomClient):
        self.room = room
        room.protocol.register_handler("room.sync", self._handle_sync)

        self._connected_documents = dict[str, _RefCount[MeshDocument]]()
        self._connecting_documents = dict[str, asyncio.Future[_RefCount[MeshDocument]]]()
        self._sync_ch = Chan[_QueuedSync]()
        self._main_task = None
    
    async def _main(self):
    
        async for q in self._sync_ch:
            logger.info("client sync sending for {path}".format(path=q.path))
            await self.room.send_request("room.sync", {"path":q.path}, q.base64.encode("utf-8"))

    async def start(self):
        
        if self._main_task != None:
            raise Exception("client already started")
  
        self._main_task = asyncio.create_task(self._main())

        
    async def stop(self):
        self._sync_ch.close()
        
        asyncio.gather(self._main_task)
        

    async def create(self, *, path: str, json: Optional[dict] = None) -> None:
         await self.room.send_request("room.create", { "path" : path, "json" : json })
        
    async def open(self, *, path: str, create: bool = True) -> MeshDocument:

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
            response = await self.room.send_request("room.connect", {
                "path": path,
                "create": create 
            })

            schema_json = response["schema"]
            doc : MeshDocument = runtime.new_document(schema=MeshSchema.from_json(schema_json), on_document_sync=publish_sync, factory=MeshDocument)
            
            ref = _RefCount(doc)
            self._connected_documents[path] = ref
            connecting_fut.set_result(ref)
            self._connecting_documents.pop(path)

            logger.info("Connected to %s", path)
        except Exception as e:
            connecting_fut.set_exception(e)
            self._connecting_documents.pop(path)
            raise
        
        await doc.synchronized
        return doc
    
    async def close(self, *, path: str) -> None:
        await asyncio.sleep(5) # TODO: flush pending changes instead of waiting for them

        if path not in self._connected_documents:
            raise RoomException("Not connected to "+path)

        ref = self._connected_documents[path]
        ref.count = ref.count - 1
        if ref.count == 0:
            doc = self._connected_documents.pop(path)
            response = await self.room.send_request("room.disconnect", {"path":path})
            runtime._unregister_document(doc=doc.ref)

    async def sync(self, *, path: str, data: bytes) -> None:
         await self.room.send_request("room.sync", {"path":path}, data=data)

    async def _handle_sync(self, protocol: Protocol, message_id: int, type: str, data: bytes) -> None:
        
        header_str = split_message_header(data=data)
        payload = split_message_payload(data=data)

        header = json.loads(header_str)
        path = header["path"]

        if path in self._connecting_documents:
            # Wait for document to be fully connected and initialized
            await self._connecting_documents[path]

        if path in self._connected_documents:
            doc = self._connected_documents[path]
            
            runtime.apply_backend_changes(doc.ref.id, payload.decode("utf-8"))
            if doc.ref.synchronized.done() == False:
                doc.ref.synchronized.set_result(True)
        else:
            raise RoomException("received change for a document that is not connected:" + path)



class AgentDescription:
    def __init__(
        self, 
        name: str, 
        title: str, 
        description: str, 
        input_schema: dict, 
        output_schema: Optional[dict] = None,
        requires: Optional[list[Requirement]] = None,
        supports_tools : bool = False,
        labels: Optional[list[str]] = None
    ):
        if labels == None:
            labels = []
        
        if requires == None:
            requires = []

        self.name = name
        self.title = title
        self.description = description
        self.input_schema = input_schema
        self.output_schema = output_schema
        self.requires = requires
        self.supports_tools = supports_tools
        self.labels = labels

class ToolDescription:
    def __init__(
        self,
        *,
        name: str,
        title: str,
        description: str,
        input_schema: dict,
        thumbnail_url: Optional[str] = None,
        defs: Optional[dict] = None,
        pricing: Optional[str] = None
    ):
        self.name = name
        self.title = title
        self.description = description
        self.input_schema = input_schema
        self.thumbnail_url = thumbnail_url
        self.defs = defs
        self.pricing = pricing

    def to_json(self):
        return {
            "name" : self.name,
            "description" : self.description,
            "title" : self.title,
            "thumbnail_url" : self.thumbnail_url,
            "input_schema" : self.input_schema,
            "defs" : self.defs,
            "pricing" : self.pricing
        }

class ToolkitDescription:
    def __init__(
        self,
        *,
        name: str,
        title: str,
        description: str,
        tools: List[ToolDescription],
        thumbnail_url: Optional[str] = None
    ):
        self.name = name
        self.title = title
        self.description = description
        self.tools = tools
        self.thumbnail_url = thumbnail_url

    def to_json(self):
        return {
            "name" : self.name,
            "description" : self.description,
            "title" : self.title,
            "thumbnail_url" : self.thumbnail_url,
            "tools" : list(map(lambda x: x.to_json(), self.tools))
        }


class AgentsClient:
    def __init__(self, *, room: RoomClient):
        self.room = room

    async def make_call(self, *, name: str, url: str, arguments: dict) -> None:
        await self.room.send_request("agent.call", { "name" : name, "url": url, "arguments": arguments})
        return None

    async def ask(self, *, agent: str, arguments: dict):
        response = await self.room.send_request("agent.ask", { "agent": agent, "arguments": arguments})
        return response["answer"]

    async def invoke_tool(self, *, toolkit: str, tool: str, arguments: dict, participant_id: Optional[str] = None, on_behalf_of_id: Optional[str] = None) -> FileResponse | str | dict | None:
    
        response = await self.room.send_request("agent.invoke_tool", {
            "toolkit" : toolkit,
            "tool" : tool,
            "participant_id" : participant_id,
            "on_behalf_of_id" : on_behalf_of_id,
            "arguments" : arguments
        })

        if isinstance(response, JsonResponse):
            return response.json
        
        if isinstance(response, TextResponse):
            return response.text
        
        if isinstance(response, EmptyResponse):
            return None
        
        return response
        

    async def list_agents(self) -> List[AgentDescription]:
        """
        Fetch a list of available agents and parse into `AgentDescription` objects.
        """
        response = await self.room.send_request("agent.list_agents", {
        })
        # 'response["agents"]' is assumed to be a list of dicts
        agents_data : list[dict] = response["agents"]
        agents = []
        for a in agents_data:

            requires_json : list[dict] = a.get("requires", [])
            requires = list(map(lambda j: Requirement.from_json(j), requires_json))
            
            agents.append(
                AgentDescription(
                    name=a["name"],
                    title=a.get("title", ""),
                    description=a.get("description", ""),
                    input_schema=a.get("input_schema", {}),
                    output_schema=a.get("output_schema", None),
                    requires=requires,
                    supports_tools=a.get("supports_tools", False),
                    labels=a.get("labels", None)
                )
            )
        return agents

    async def list_toolkits(self, participant_id: Optional[str] = None) -> List[ToolkitDescription]:
        """
        Fetch a list of available toolkits and parse into `ToolkitDescription` objects.
        """
        response = await self.room.send_request("agent.list_toolkits", {
            "participant_id" : participant_id
        })
        # 'response["tools"]' is assumed to be a dict of toolkits by name
        toolkits_data = response["tools"]

        result = []
        for toolkit_name, tk_json in toolkits_data.items():
            # Parse top-level toolkit properties
            title = tk_json.get("title", "")
            description = tk_json.get("description", "")
            thumbnail_url = tk_json.get("thumbnail_url", None)

            # Each toolkit has a dict of 'tools'
            tool_descriptions = []
            if "tools" in tk_json:
                for tool_name, tool_info in tk_json["tools"].items():
                    tool_descriptions.append(
                        ToolDescription(
                            name=tool_name,
                            title=tool_info.get("title", ""),
                            description=tool_info.get("description", ""),
                            input_schema=tool_info["input_schema"],
                            thumbnail_url=tool_info.get("thumbnail_url", None),
                            defs=tool_info.get("defs", None)
                        )
                    )

            toolkit = ToolkitDescription(
                name=toolkit_name,
                title=title,
                description=description,
                tools=tool_descriptions,
                thumbnail_url=thumbnail_url
            )
            result.append(toolkit)

        return result

class LivekitConnectionInfo:
    def __init__(self, *, url:str, token: str):
        self.url = url
        self.token = token

class LivekitClient:
    def __init__(self, *, room: RoomClient):
        self.room = room

    async def get_connection_info(self) -> LivekitConnectionInfo:
        response = await self.room.send_request("livekit.connect", {})

        return LivekitConnectionInfo(
            url=response["url"],
            token=response["token"],
        )

class StorageEntry:
    def __init__(self, name: str, is_folder: bool):
        self.name = name
        self.is_folder = is_folder

    

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
        self.emit("file.deleted", path=json.loads(data)["path"])

    async def _on_file_updated(self, protocol, message_id, msg_type, data):
        self.emit("file.updated", path=json.loads(data)["path"])

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
         
        response = await self.room.send_request("storage.exists", { "path":path })   
        return response["exists"]
    
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

        response = await self.room.send_request("storage.open", { "path":path, "overwrite" : overwrite })   
        return FileHandle(id=response["handle"]);     
    
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
         
        await self.room.send_request("storage.write", { "handle": handle.id }, data=data)

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

        await self.room.send_request("storage.close", { "handle": handle.id });   

    async def download(self, *, path: str) -> FileResponse:
        """
        Retrieves the content of a file from the remote storage system.

        Arguments:
            path (str): The file path to download.

        Returns:
            FileResponse: A response containing the downloaded data.

        Example:
            file_response = await storage_client.download(path="files/data.bin")
            print(file_response.data)  # raw bytes
        """

        response = await self.room.send_request("storage.download", { "path": path });   
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

        response = await self.room.send_request("storage.download_url", { "path": path });   
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
         
        response = await self.room.send_request("storage.list", { "path":path })        
        return list(map(lambda f : StorageEntry(**f), response["files"]))

    async def delete(self, path: str):
        """
        Deletes a file  at the given path.

        Arguments:
            path (str): The file to delete.

        Returns:
            None

        Example:
            await storage_client.delete("folder/old_file.txt")
        """
         
        await self.room.send_request("storage.delete", {"path": path})
    
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

    async def list(self, *, name: str, message: dict, create: bool = True) -> list[Queue]:
        response = (await self.room.send_request("queues.list", {}))
        queues = []
        if isinstance(response, JsonResponse):
            for item in response.json["queues"]:
                queues.append(Queue(name=item["name"], size=int(item["size"])))
        return queues

    
    async def send(self, *, name: str, message: dict, create: bool = True) -> None:
        (await self.room.send_request("queues.send", {"name":  name, "create" : create, "message" : message }))

    async def drain(self, *, name: str) -> None:
        (await self.room.send_request("queues.open", {"name":  name }))

    async def drain(self, *, name: str) -> None:
        (await self.room.send_request("queues.drain", {"name":  name }))

    async def close(self, *, name: str) -> None:
        (await self.room.send_request("queues.close", {"name":  name }))
    
    async def receive(self, *, name: str, create : bool = True, wait : bool = True) -> dict | None:

        response = (await self.room.send_request("queues.receive", {"name":  name, "create" : create, "wait" : wait }))
        if isinstance(response, EmptyResponse):
            return None
        elif isinstance(response, JsonResponse):
            return response.json
        
        else:
            raise RoomException("Unexpected response")



class MessagingClient:
    def __init__(self, *, room: RoomClient):
        self.room = room
        self._participants = dict[str, RemoteParticipant]()
        self._events = {}
        self._on_stream_accept_callback = None
        room.protocol.register_handler("messaging.send", self._handle_message_send)
        self._stream_writers: Dict[str, asyncio.Future] = {}
        self._stream_readers: Dict[str, MessageStreamReader] = {}


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

    def emit(self, event_name,  **kwargs):
        """Call all handlers associated with the given event."""
        handlers = self._events.get(event_name, [])
        for handler in handlers:
            handler(**kwargs)


    def get_participants(self) -> list[RemoteParticipant]:
        return list(self._participants.values())

    async def enable(self, *, on_stream_accept: Optional[Callable[["MessageStreamReader"], None]] = None):
        await self.room.send_request("messaging.enable", {})
        self._on_stream_accept_callback = on_stream_accept

    async def disable(self):
        await self.room.send_request("messaging.disable", {})
    
    async def _handle_message_send(self, protocol: Protocol, message_id: int, type: str, data: bytes) -> None:
        header_str = split_message_header(data)
        payload = split_message_payload(data)

        header = json.loads(header_str)

        message = RoomMessage(from_participant_id=header["from_participant_id"], type=header["type"], message=header["message"], attachment=payload)

       
        if message.type ==  "messaging.enabled":
            self._on_messaging_enabled(message)
        elif message.type ==  "participant.attributes":
            self._on_participant_attributes(message)
        elif message.type ==  "participant.enabled":
            self._on_participant_enabled(message)
        elif message.type ==  "participant.disabled":
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

    async def send_message(
        self,
        *,
        to: Participant,
        type: str,
        message: dict,
        attachment: Optional[bytes] = None
    ):
        body = {
            "type": type,
            "message": message,
        }
       
        body["to_participant_id"] = to.id


        await self.room.send_request("messaging.send", body, data=attachment)
        
    async def broadcast_message(
        self,
        *,
        type: str,
        message: dict,
        attachment: Optional[bytes] = None
    ):
        await self.room.send_request("messaging.broadcast",
            {
                "type": type,
                "message": message
            },
            data=attachment
        )
       
    def _on_participant_enabled(self, message: RoomMessage):
        data = message.message
        participant = RemoteParticipant(id=data["id"], role=data["role"])

        for k,v in data["attributes"].items():
            participant._attributes[k] = v
      
        self._participants[ data["id"] ] = participant

        self.emit("participant_added", participant=participant)
        
    def _on_participant_attributes(self, message: RoomMessage):
        if message.from_participant_id in self._participants:
            part = self._participants[message.from_participant_id]
            for k, v in message.message["attributes"].items():
                part._attributes[k] = v

            self.emit("participant_attributes_updated", participant=part)

    def _on_participant_disabled(self, message: RoomMessage):
        part = self._participants.pop(message.message["id"], None)
        if part != None:
            self.emit("participant_removed", participant=part)

    def _on_messaging_enabled(self, message: RoomMessage):
        for data in message.message["participants"]:
            participant = RemoteParticipant(id=data["id"], role=data["role"])

            for k,v in data["attributes"].items():
                participant._attributes[k] = v
        
            self._participants[ data["id"] ] = participant

        self.emit("messaging_enabled")

    async def create_stream(self, *, to: Participant, header: dict) -> "MessageStreamWriter":
        stream_id = str(uuid.uuid4())  # Generate unique ID
        future = asyncio.Future()
        self._stream_writers[stream_id] = future

        # Send "stream.open"
        await self.send_message(
            to=to,
            type="stream.open",
            message={
                "stream_id": stream_id,
                "header": header
            }
        )

        # Wait for remote side to accept or reject
        writer = await future
        return writer

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
            except e:
                logger.warning("unable to send stream response", exc_info=e)

        if not from_participant:
            # If we don't know who this is, reject
            send = asyncio.create_task(self.send_message(
                to=None,  # no participant needed if we can't identify
                type="stream.reject",
                message={
                    "stream_id": message.message["stream_id"],
                    "error": "unknown participant"
                }
            ))
            send.add_done_callback(on_send_complete)
            return

        stream_id = message.message["stream_id"]
       

        try:
            if self._on_stream_accept_callback is None:
                raise Exception("Streams are not allowed by this client")

            # User callback so they can "attach" to the stream
            reader = MessageStreamReader(stream_id=stream_id, to=from_participant, client=self, header=message.message)
            
            self._on_stream_accept_callback(reader)
            self._stream_readers[stream_id] = reader

            logger.info(f"accepting stream {stream_id}")
            # Accept
            send = asyncio.create_task(self.send_message(
                to=from_participant,
                type="stream.accept",
                message={
                    "stream_id": stream_id
                }
            ))
            send.add_done_callback(on_send_complete)

        except Exception as e:
            logger.info(f"rejecting stream {stream_id}")
            # Reject
            send = asyncio.create_task(self.send_message(
                to=from_participant,
                type="stream.reject",
                message={
                    "stream_id": stream_id,
                    "error": str(e)
                }
            ))
            send.add_done_callback(on_send_complete)
            return
      

    def _on_stream_accept(self, message: RoomMessage):
        """
        The remote side accepted our stream request. 
        Complete the Future<MessageStreamWriter>.
        """
        stream_id = message.message["stream_id"]
        future = self._stream_writers.pop(stream_id, None)
        if future and not future.done():
            from_part_id = message.from_participant_id
            from_part = self._participants.get(from_part_id, None)
            # Construct the writer
            writer = MessageStreamWriter(stream_id=stream_id, to=from_part, client=self)
            future.set_result(writer)

    def _on_stream_reject(self, message: RoomMessage):
        """
        The remote side rejected our stream request.
        Complete the Future with an error.
        """
        stream_id = message.message["stream_id"]
        err = message.message.get("error", "The stream was rejected by the remote client")

        future = self._stream_writers.pop(stream_id, None)
        if future and not future.done():
            future.set_exception(Exception(err))

    def _on_stream_chunk(self, message: RoomMessage):
        """
        A chunk arrived on an existing stream.
        """
        stream_id = message.message["stream_id"]
        reader = self._stream_readers.get(stream_id, None)
        if reader:
            chunk = MessageStreamChunk(
                header=message.message["header"],
                data=message.attachment
            )
            reader._add_chunk(chunk)

    def _on_stream_close(self, message: RoomMessage):
        """
        The remote side closed the stream.
        """
        stream_id = message.message["stream_id"]
        reader = self._stream_readers.pop(stream_id, None)
        if reader:
            reader._close()




class MessageStreamChunk:
    def __init__(self, header: dict, data: Optional[bytes] = None):
        self.header = header
        self.data = data


class MessageStreamReader:
    def __init__(self, stream_id: str, to: Participant, client: MessagingClient, header: dict):
        self._stream_id = stream_id
        self._to = to
        self._client = client
        self._header = header
        self._queue = asyncio.Queue()  # To buffer incoming chunks

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
        # Put a sentinel None to signal the end of the stream
        self._queue.put_nowait(None)


class MessageStreamWriter:
    def __init__(self, stream_id: str, to: Participant, client: MessagingClient):
        self._stream_id = stream_id
        self._to = to
        self._client = client

    async def write(self, chunk: MessageStreamChunk):
        """
        Sends a "stream.chunk" message to the remote participant.
        """
        await self._client.send_message(
            to=self._to,
            type="stream.chunk",
            message={
                "stream_id": self._stream_id,
                "header": chunk.header,
            },
            attachment=chunk.data
        )

    async def close(self):
        """
        Sends a "stream.close" message to the remote participant.
        """
        await self._client.send_message(
            to=self._to,
            type="stream.close",
            message={
                "stream_id": self._stream_id
            }
        )



class DeveloperClient:
    def __init__(self, room: RoomClient):

        self._room = room
        self._room.protocol.register_handler("developer.log", self._handle_log)
        self._events = dict[str,list[Callable]]()

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

    async def _handle_log(self, protocol: Protocol, message_id: int, type: str, data: bytes) -> None:
        raw_json : dict = json.loads(data)

        type = raw_json.get("type", "unknown")
        data = raw_json.get("data", {})

        
        self.emit("log", type=type, data=data)
    

    async def log(self, *, type: str, data: dict):
        await self._room.send_request(type="developer.log", request={
            "type" : type,
            "data" : data
        })

    def log_nowait(self, *, type: str, data: dict):
        asyncio.ensure_future(self._room.send_request(type="developer.log", request={
            "type" : type,
            "data" : data
        }))
    

    async def enable(self):
        await self._room.send_request(type="developer.watch", request={})
    

    async def disable(self):
         await self._room.send_request(type="developer.unwatch", request={})
    

_data_types = dict()

class DataType(ABC):
    pass

    @abstractmethod
    def to_json(self)-> dict:
        pass

    @staticmethod
    def from_json(data: dict) -> "DataType":
        return _data_types[data["type"]].from_json(data)


class IntDataType(DataType):

    def __init__(self):
        super().__init__()
    
    @staticmethod
    def from_json(data: dict):

        assert data["type"] == "int"
        return IntDataType()
    
    def to_json(self):
        return { "type" : "int" }


_data_types["int"] = IntDataType

class DateDataType(DataType):

    def __init__(self):
        super().__init__()
    
    @staticmethod
    def from_json(data: dict):

        assert data["type"] == "date"
        return DateDataType()
    
    def to_json(self):
        return { "type" : "date" }
    
_data_types["date"] = DateDataType


class FloatDataType(DataType):

    def __init__(self):
        super().__init__()
    

    @staticmethod
    def from_json(data: dict):

        assert data["type"] == "float"
        return FloatDataType()
    
    def to_json(self):
        return { "type" : "float" }
    
_data_types["float"] = FloatDataType


class VectorDataType(DataType):
    def __init__(self, *, size: int, element_type: DataType):
        self.size = size
        self.element_type = element_type


    @staticmethod
    def from_json(data: dict):

        assert data["type"] == "vector"
        return VectorDataType(size=data["size"], element_type=DataType.from_json(data["element_type"]))
    
    def to_json(self):
        return { "type" : "vector", "size" : self.size, "element_type" : self.element_type.to_json() }

_data_types["vector"] = VectorDataType

class TextDataType(DataType):

    def __init__(self):
        super().__init__()

    @staticmethod
    def from_json(data: dict):
        assert data["type"] == "text"
        return TextDataType()
    

    def to_json(self):
        return { "type" : "text" }

_data_types["text"] = TextDataType


CreateMode = Literal["create", "overwrite", "create_if_not_exists"]

class DatabaseClient:
    """
    A client for interacting with the 'database' extension on the room server.
    """
    def __init__(self, room: RoomClient):
        """
        :param room: The RoomClient used to send requests.
        """
        self.room = room

    async def list_tables(self) -> List[str]:
        """
        List all tables in the database.

        :return: A list of table names.
        """
        response : JsonResponse = await self.room.send_request("database.list_tables", {})
        return response.json.get("tables", [])
        
    async def _create_table(
        self,
        *,
        name: str,
        data: Optional[Any] = None,
        schema: Optional[Dict[str, DataType]] = None,
        mode: Optional[CreateMode] = "create"
    ) -> None:
        
        """
        Create a new table.

        :param name: Table name.
        :param data: Optional initial data (list/dict).
        :param schema: Optional schema definition.
        :param mode: "create" or "overwrite" (default: "create")
        :return: Server response dict containing "status", "table", etc.
        """

        schema_dict = None

        if schema != None:
            schema_dict = {}
            for k in schema.keys():
                schema_dict[k] = schema[k].to_json()

        payload = {
            "name": name,
            "data": data,
            "schema": schema_dict,
            "mode": mode
        }
        await self.room.send_request("database.create_table", payload)
        return None
   
    
    async def create_table_with_schema(self,
        *,
        name: str,
        schema: Optional[Dict[str, DataType]] = None,
        data: Optional[List[dict]] = None,
        mode: Optional[CreateMode] = "create"
    ) -> None:
        return await self._create_table(name=name, schema=schema, mode=mode, data=data)

    async def create_table_from_data(self,
        *,
        name: str,
        data: Optional[list[dict]] = None,
        mode: Optional[CreateMode] = "create"
    ) -> None:
        return await self._create_table(name=name, data=data, mode=mode)


    async def drop_table(self,
        *,
        name: str,
        ignore_missing: bool = False):

        """
        Drop (delete) a table.

        :param name: Table name.
        :param ignore_missing: If True, ignore if table doesn't exist.
        :return: Server response dict containing "status", "table", etc.
        """
        payload = {
            "name": name,
            "ignore_missing": ignore_missing
        }
        await self.room.send_request("database.drop_table", payload)
        return None

    async def add_columns(self, 
        *,
        table: str,
        new_columns: Dict[str, str]) -> None:
        """
        Add new columns to an existing table.

        :param table: Table name.
        :param new_columns: Dict of {column_name: default_value_expression}.
        """

        payload = {
            "table": table,
            "new_columns": new_columns
        }
        await self.room.send_request("database.add_columns", payload)
        return None

    async def drop_columns(self, 
        *,
        table: str,
        columns: List[str]
    ) -> None:
        """
        Drop columns from an existing table.

        :param table: Table name.
        :param columns: List of column names to drop.
        :return: Server response dict with "status", "table", "dropped_columns".
        """
        payload = {
            "table": table,
            "columns": columns
        }
        
        await self.room.send_request("database.drop_columns", payload)
        return None

    async def insert(self, 
        *,
        table: str,
        records: List[Dict[str, Any]]
    ) -> None:
        """
        Insert new records into a table.

        :param table: Table name.
        :param records: The record(s) to insert (list or dict).
        :return: Server response dict with "status", "table", "result".
        """
        payload = {
            "table": table,
            "records": records,
        }
        await self.room.send_request("database.insert", payload)
        
    async def update(
        self,
        *,
        table: str,
        where: str,
        values: Optional[Dict[str, Any]] = None,
        values_sql: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Update existing records in a table.

        :param table: Table name.
        :param where: SQL WHERE clause (e.g. "id = 123").
        :param values: Dict of column updates, e.g. {"col1": "new_value"}.
        :param values_sql: Dict of SQL expressions for updates, e.g. {"col2": "col2 + 1"}.
        :return: Server response dict with "status", "table", "where".
        """
        payload = {
            "table": table,
            "where": where,
            "values": values,
            "values_sql": values_sql
        }
        await self.room.send_request("database.update", payload)

    async def delete(self,
        *,
        table: str,
        where: str) -> None:
        """
        Delete records from a table.

        :param table: Table name.
        :param where: SQL WHERE clause (e.g. "id = 123").
        :return: Server response dict with "status", "table", "where".
        """
        payload = {
            "table": table,
            "where": where
        }
        await self.room.send_request("database.delete", payload)

        return None

    async def merge(self,
        *,
        table: str,
        on: str,
        records: Any) -> None:
        """
        Merge (upsert) records into a table.

        :param table: Table name.
        :param on: Column name to match on (e.g. "id").
        :param records: The record(s) to merge.
        :return: Server response dict with "status", "table", "on".
        """
        payload = {
            "table": table,
            "on": on,
            "records": records
        }
        await self.room.send_request("database.merge", payload)
        return None

    async def search(
        self,
        *,
        table: str,
        text: Optional[str] = None,
        vector: Optional[list[float]] = None,
        where: Optional[str] | dict = None,
        limit: Optional[int] = None,
        select: Optional[List[str]] = None
    ) -> list[Dict[str, Any]]:
        """
        Search for records in a table.

        :param table: Table name.
        :param text: The search text
        :param where: A filter clause or values to match
        :param limit: Limit the number of results.
        :param select: Columns to select.
        :return: Server response dict with "status", "table", "results".
        """

        if isinstance(where, dict):
            where = " AND ".join(map(lambda x: f"{x} = {json.dumps(where[x])}", where.keys()))
        payload = {
            "table": table,
            "where": where,
            "text": text,
        }
        if limit is not None:
            payload["limit"] = limit
        if select is not None:
            payload["select"] = select

        if vector is not None:
            payload["vector"] = vector

        response = await self.room.send_request("database.search", payload)
        if hasattr(response, "json"):
            return response.json["results"]
        return []

    async def optimize(self, table: str) -> None:
        """
        Optimize (compact/prune) a table.

        :param table: Table name.
        """
        payload = {
            "table": table,

        }
        await self.room.send_request("database.optimize", payload)
        return None

    async def create_vector_index(
        self,
        *,
        table: str,
        column: str,
        replace: Optional[bool] = None
    ) -> None:
        """
        Create a vector index on a given column.

        :param table: Table name.
        :param column: Vector column name.
        """
        payload = {
            "table": table,
            "column": column,
            "replace": replace,
        }
        await self.room.send_request("database.create_vector_index", payload)
        return None

    async def create_scalar_index(
        self,
        *,
        table: str,
        column: str,
        replace: Optional[bool] = None
    ) -> None:
        """
        Create a scalar index on a given column.

        :param table: Table name.
        :param column: Column name.
        """
        payload = {
            "table": table,
            "column": column,
            "replace": replace,
        }
        await self.room.send_request("database.create_scalar_index", payload)
        return None

    async def create_full_text_search_index(
        self,
        *,
        table: str,
        column: str,
        replace: Optional[bool] = None
    ) -> None:
        """
        Create a full-text search index on a given text column.

        :param table: Table name.
        :param column: Text column name.
        """
        payload = {
            "table": table,
            "column": column,
            "replace" : replace,
        }
        await self.room.send_request("database.create_full_text_search_index", payload)
        return None

    async def list_indexes(self,
        *,
        table: str) -> Dict[str, Any]:
        """
        List all indexes on a table.

        :param table: Table name.
        """
        payload = {
            "table": table
        }
        response = await self.room.send_request("database.list_indexes", payload)
        if hasattr(response, "json"):
            return response.json["indexes"]
        
        raise RoomException("unexpected return type")
