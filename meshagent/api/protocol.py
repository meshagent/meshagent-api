
import math
import json
import logging
import asyncio
from typing import Any, Callable
import inspect
import uuid
from meshagent.api.chan import Chan

def compute_packets(data: bytes) -> int:
    return  math.ceil(len(data) / 1024)

logger = logging.getLogger("protocol")
# logger.setLevel(logging.INFO)

class Message:
    def __init__(self, *, id: int, type: str, data: bytes):
        self.id = id
        self.type = type
        self.data = data

class Protocol:
    def __init__(self):
        self._message_id = 0
        self._send_ch = Chan[Message]()
        self._recv_ch = Chan[Message]()
        self._done_fut = asyncio.Future[bool]()

        self._handlers = dict[str,Callable]()
        self._main_task : None | asyncio.Task = None
        
        self.recv_state = "ready" 
        self.recv_message_id = -1
        self.recv_type : None | str = None
        self.recv_packet_id = 0
        self.recv_packet_total = 0
        self.recv_packets = list[bytes]()

    async def __aenter__(self):
        if self._main_task != None:
            raise Exception("protocol already started")
  
        self._main_task = asyncio.create_task(self._main())
        self._open = True
        
        return self

    async def wait_for_close(self):
        await self._done_fut

    @property
    def is_open(self) -> bool:
        return self._open
    
    def close(self):
        self._open = False

        if self._send_ch.closed == False:
            self._send_ch.close()
        
        if self._recv_ch.closed == False:
            self._recv_ch.close()

    async def __aexit__(self, exc_type, exc, tb):
        self.close()

        await asyncio.gather(self._main_task)
        return

    def unregister_handler(self, type:str, fn:Callable) -> None:
        assert self._handlers[type] == fn
        self._handlers.pop(type)
   
    def register_handler(self, type:str, fn:Callable) -> None:
        
        if type in self._handlers:
            raise(Exception("already registered handler for "+type))
        
        self._handlers[type] = fn

    async def _send_payload(self, message_id: int, data: bytes) -> None:  
        packets = compute_packets(data)
        for i in range(packets):
            packet = bytearray()
            packet.extend(message_id.to_bytes(8))
            packet.extend(int(i + 1).to_bytes(4))
            packet.extend(data[i * 1024 : min((i + 1) * 1024, len(data))])
            await self.send_packet(bytes(packet))
            
        return packets

    async def _handle_message(self, message_id: int, type: str, data: bytes) -> None:
        logger.info("invoking handler %s", type)
        try:
            if type in self._handlers:
                fn = self._handlers[type]
            else:
                if "*" in self._handlers:
                    fn = self._handlers["*"]
                else:
                    raise Exception("no handler registered for {type}".format(type=type))

            fut = fn(self, message_id, type, data)
            if inspect.isawaitable(fut):
                await fut
        except Exception as e:
            logger.error("error while invoking handler %s", type, exc_info=e)

    async def send_packet(self, data: bytes) -> None:
        raise(Exception("not implemented"))

    async def _send_message(self, message_id: int, type: str, data: bytes) -> None:
        
        packet_count = compute_packets(data)
        header_data = bytearray()
        header_data.extend(message_id.to_bytes(8))
        header_data.extend(int(0).to_bytes(4))
        header_data.extend(packet_count.to_bytes(4))
        header_data.extend(bytes(type, "utf-8"))
      
        await self.send_packet(header_data)
        
        logger.info("publishing message payload %s", message_id)
        await self._send_payload(message_id=message_id, data=data)


    def receive_packet(self, data: bytes) -> None:
    
        message_id = int.from_bytes(data[0:8], "big")
        packet = int.from_bytes(data[8:12], "big")

        def mark_ready():
            self.recv_state  = "ready"
            self.recv_packet_id = 0 
            self.recv_type = None
            self.recv_packets.clear()
            self.recv_message_id = -1
            
        if packet != self.recv_packet_id:
            self.recv_state  = "error"
            logger.error("received out of order packet %d, expected %d", packet, self.recv_packet_id)
            self.recv_packet_id = 0
            return
        
        if packet == 0:
            if self.recv_state  == "ready" or self.recv_state == "error":

                if self.recv_state == "error":
                    logger.error("received packet 0 in error state, recovering")
                
                self.recv_packet_total = int.from_bytes(data[12:16], "big")
                self.recv_message_id = message_id
                self.recv_type = str(data[16:], "utf-8")
                logger.info("recieved packet %s of %d", self.recv_type, self.recv_packet_total)
            
                if self.recv_packet_total == 0:

                    data = b''.join(self.recv_packets)
                    logger.info("sending single packet message")
                    self._recv_ch.send_nowait(Message(id=message_id, type=self.recv_type, data=data))
                    
                    mark_ready()
                else:                        
                    self.recv_packet_id += 1
                    self.recv_state = "processing"
            else:
                self.recv_state  = "error"
                self.recv_packet_id = 0
                logger.error("received packet 0 in invalid state")
                return
        elif self.recv_state != "processing":
            logger.error("received datapacket in invalid state")
            self.recv_packet_id = 0
            self.recv_state  = "error"
            return
        else:
            if message_id != self.recv_message_id:
                logger.error("received packet from incorrect message")
                self.recv_state  = "error"
                return
            
            self.recv_packets.append(data[12:])

            if self.recv_packet_total == self.recv_packet_id:
                # reset
                
                logger.info("sending multiple packet message %d of %d", self.recv_packet_id, self.recv_packet_total)
                    
                data = b''.join(self.recv_packets)
                self._recv_ch.send_nowait(Message(id=message_id, type=self.recv_type, data=data))

                mark_ready()
                
            else:
                self.recv_packet_id += 1
                            

    async def _send_task(self) -> None:
        logger.info("send channel task started")

        async for message in self._send_ch:
            logger.info("send queued message %d", message.id)
            message_id = message.id
            type = message.type
            data = message.data
            await self._send_message(message_id=message_id, type=type, data=data)

        logger.info("send channel task ended")

    async def _recv_task(self) -> None:

        logger.info("recv channel task started")

        async for message in self._recv_ch:
            logger.info("received queued message %d %s %d", message.id, message.type, len(message.data))
            message_id = message.id
            type = message.type
            data = message.data
            await self._handle_message(message_id=message_id, type=type, data=data)

        logger.info("recv channel task ended")
            

    def next_message_id(self):
        self._message_id += 1
        return self._message_id

    async def send(self, type: str, data: bytes | str, message_id: int | None = None) -> int:

        if message_id == None:
            message_id = self.next_message_id()
            logger.info("sending message %d", message_id)

        if isinstance(data, str):
            data = bytes(data, "utf-8")
        self._send_ch.send_nowait(Message(id=message_id, type=type, data=data))
        return message_id

    async def _main(self) -> None:
        
        try:
            await asyncio.gather(
                asyncio.create_task(self._recv_task()),
                asyncio.create_task(self._send_task()),
            )
        finally:
            self._done_fut.set_result(True)

class ClientProtocol(Protocol):

    def __init__(self, *, token: str):
        super().__init__()
        self._token = token

    @property
    def token(self):
        return self._token


class MemoryServerProtocol(Protocol):
    def __init__(self, *, input: Chan[bytes], output: Chan[bytes]):
        super().__init__()
        self.input = input
        self.output = output

    async def send_packet(self, data: bytes):
        self.output.send_nowait(data)

    async def __aenter__(self):
        self._byte_recv_task = asyncio.create_task(self._byte_recv_handler())
        await super().__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc, tb):
        self._byte_recv_task.cancel()
        return await super().__aexit__(exc_type, exc, tb)
    
    async def _byte_recv_handler(self):
        async for msg in self.input:
            self.receive_packet(msg)
    
class MemoryClientProtocol(ClientProtocol):
    def __init__(self, *, input: Chan[bytes], output: Chan[bytes], token: str):
        super().__init__(token=token)
        self.input = input
        self.output = output

    async def send_packet(self, data: bytes):
        self.output.send_nowait(data)

    async def __aenter__(self):
        self._byte_recv_task = asyncio.create_task(self._byte_recv_handler())
        await super().__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc, tb):
        self._byte_recv_task.cancel()
        return await super().__aexit__(exc_type, exc, tb)
    
    async def _byte_recv_handler(self):
        async for msg in self.input:
            self.receive_packet(msg)
    
