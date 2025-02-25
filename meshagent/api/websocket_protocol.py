import urllib.parse
from aiohttp import ClientSession, WSMsgType, web
import asyncio
import logging
import urllib

from meshagent.api.protocol import Protocol, ClientProtocol

logger = logging.getLogger("protocol.websocket")
logger.setLevel(logging.INFO)

class WebSocketClientProtocol(ClientProtocol):
    def __init__(self, *, url: str, token: str, heartbeat : float = 30):
        super().__init__(token=token)
        self._url = url
        self._heartbeat = heartbeat

    @property
    def url(self):
        return self._url
    

    async def __aenter__(self):
          
        self._session = ClientSession()

        await self._session.__aenter__()

        
        url_parts = urllib.parse.urlparse(self._url)
        query_dict = urllib.parse.parse_qs(url_parts.query)
        query_dict.update({
            "token" : self.token
        })
        new_query_string = urllib.parse.urlencode(query_dict, doseq=True)
        url_with_params = urllib.parse.urlunparse((url_parts.scheme, url_parts.netloc, url_parts.path, url_parts.params, new_query_string, url_parts.fragment))

        self._ws_ctx = self._session.ws_connect(url_with_params, heartbeat=self._heartbeat)
        self._ws = await self._ws_ctx.__aenter__()

        self._ws_recv_task = asyncio.create_task(self._ws_recv())
        
        await super().__aenter__()
        return self
    
    async def _ws_recv(self):
        try:
            async for msg in self._ws:
                if msg.type == WSMsgType.BINARY:
                    self.receive_packet(msg.data)
                elif msg.type == WSMsgType.CLOSED:
                    break
                elif msg.type == WSMsgType.ERROR:
                    break
                else:
                    raise(Exception("Unexpected message type"))        
        except asyncio.CancelledError:
            pass

        self.close()

    async def __aexit__(self, exc_type, exc, tb):
        if self._ws.closed == False:
            await self._ws.close()

        self._ws_recv_task.cancel()
        await self._session.__aexit__(exc_type, exc, tb)
        await self._ws_ctx.__aexit__(exc_type, exc, tb)
        await super().__aexit__(exc_type, exc, tb)
    

    async def send_packet(self, data:bytes) -> None:
        await self._ws.send_bytes(data)

 

class WebSocketServerProtocol(Protocol):
    def __init__(self, socket: web.WebSocketResponse):
        super().__init__()
        self.socket = socket

    async def __aenter__(self):
        
        self._ws_recv_task = asyncio.create_task(self._ws_recv())
        
        await super().__aenter__()
        return self
    
    async def _ws_recv(self):
        try:
            async for msg in self.socket:
                if msg.type == WSMsgType.BINARY:
                    self.receive_packet(msg.data)
                elif msg.type == WSMsgType.CLOSED:
                    break
                elif msg.type == WSMsgType.ERROR:
                    break
                else:
                    raise(Exception("Unexpected message type"))
        except asyncio.CancelledError:   
            pass

        self.close()

    async def __aexit__(self, exc_type, exc, tb):
        
        if self.socket.closed == False:
            await self.socket.close()
        
        self._ws_recv_task.cancel()

        await super().__aexit__(exc_type=exc_type, exc=exc, tb=tb)
    

    async def send_packet(self, data:bytes) -> None:
        await self.socket.send_bytes(data)





