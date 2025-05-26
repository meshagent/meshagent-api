import asyncio
from typing import Optional, Protocol
import logging
from aiohttp import web
import os
import signal
import json

import jwt
import hashlib
from aiohttp import ClientSession

from meshagent.api import RoomException
from meshagent.api.webhooks import WebhookServer
from meshagent.api import WebSocketClientProtocol, RoomMessage
from meshagent.api.room_server_client import RoomClient

logger = logging.getLogger("services")

class Portable(Protocol):
    async def start(room: RoomClient) -> None: ...
    async def stop() -> None: ...

class ServicePath:
    def __init__(self, *, path: str, cls: type[Portable]):
        self.cls = cls
        self.path = path


class ServiceHost:

    def __init__(self, *, host: Optional[str] = None, webhook_secret: Optional[str] = None, port: Optional[int] = None):

        if host == None:
            host = os.getenv("MESHAGENT_HOST", "0.0.0.0")

        self.host = host
        self.webhook_secret = webhook_secret
        if port == None:
            port = os.getenv("MESHAGENT_PORT", 8080)
        
        self.port = port
        self.paths = list[WebhookServer]()

        self._hosts = None
        self._app = None


    def path(self, path: str):

        def deco(cls: type[Portable]):
           
            self.paths.append(ServicePath(path=path, cls=cls))

            return cls

        return deco

    async def _liveness_check_request(self, request: web.Request):
       
        return web.json_response({"ok":True})
       
    async def _start_server(self):

        self._runner = web.AppRunner(self._app)
        
        await self._runner.setup()

        logger.info(f"starting service host on {self.host}:{self.port}")
        
        self._site = web.TCPSite(self._runner, self.host, self.port)
        await self._site.start()
        return self

    async def _stop_server(self):
     
        await self._site.stop()
        await self._runner.cleanup()

        self._app = None
        self._site = None
        self._runner = None

    def _create_host(self, p: ServicePath):

        class ServiceWebhookServer(WebhookServer):

            def __init__(self, *, host = None, port = None, webhook_secret = None, app = None, path = None, validate_webhook_secret = None):
                super().__init__(host=host, port=port, webhook_secret=webhook_secret, app=app, path=path, validate_webhook_secret=validate_webhook_secret)
            
            async def _spawn(self, *, room_name: str, room_url: str, token: str, arguments: Optional[dict] = None):
                
                logger.info(f"room: {room_name} url: {room_url} token: {token} arguments: {arguments}")
                agent = p.cls()

                async def run():
                    async with RoomClient(protocol=WebSocketClientProtocol(url=room_url, token=token)) as room:
            
                        dismissed = asyncio.Future()

                        def on_message(message: RoomMessage):
                            if message.type == "dismiss":
                                logger.info(f"dismissed by {message.from_participant_id}")
                                dismissed.set_result(True)

                        room.messaging.on("message", on_message)
                    
                        await agent.start(room=room)

                        done, pending = await asyncio.wait([
                            dismissed,
                            asyncio.ensure_future(room.protocol.wait_for_close())
                        ], return_when=asyncio.FIRST_COMPLETED)
                        
                        await agent.stop()

                def on_done(task: asyncio.Task):
                    try:
                        result = task.result()
                    except Exception as e:
                        logger.error("agent encountered an error", exc_info=e)

                task = asyncio.create_task(run())
                task.add_done_callback(on_done)
                
            async def on_call(self, event):
                    await self._spawn(room_name=event.room_name, room_url=event.room_url, token=event.token, arguments=event.arguments)
        
        host = ServiceWebhookServer(validate_webhook_secret=self.webhook_secret != None, path=p.path, app=self._app)
        return host

    async def start(self):

        if self._app != None:
            raise Exception("App is already started")
        
        self._app = web.Application()

        self._app.router.add_get("/", self._liveness_check_request)

        
        self._hosts : list[WebhookServer] = list(map(lambda x: self._create_host(x), self.paths))

        await asyncio.gather(*map(lambda x: x.start(), self._hosts))

        await self._start_server()

    async def stop(self):
        
        await self._stop_server()

        await asyncio.gather(*map(lambda x: x.stop(), self._hosts))

        self._hosts = None
    
    async def run(self):
        await self.start()
        try:
            term = asyncio.Future()

            def clean_termination(signal, frame):
                term.set_result(True)
            
            signal.signal(signal.SIGTERM, clean_termination)
            signal.signal(signal.SIGABRT, clean_termination)

            await term

        finally:
            await self.stop()
        

async def send_webhook(session: ClientSession, *, url: str, event: str, data: dict, secret: Optional[str] = None):
    
    payload_body = {
        "event" : event,
        "data" : data
    }

    payload = json.dumps(payload_body)
    hash = hashlib.sha256(payload.encode())

    headers = {
        "Content-Type" : "application/json",
    }

    if secret != None:
        headers["Authorization"] = "Bearer "+jwt.encode({
            "sha256" : hash.hexdigest()
        }, key=secret, algorithm='HS256')

    async with session.post(url, headers=headers, data=payload) as resp:

        try:
            resp.raise_for_status()

        except Exception as e:
            logger.warning("webhook call failed", exc_info=e)
            raise RoomException(f"error status returned from webhook call, http status code: {resp.status}")
            