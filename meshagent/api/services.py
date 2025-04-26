import asyncio

from typing import Optional

import logging
from typing import Protocol

from meshagent.api import WebSocketClientProtocol, RoomMessage

from .room_server_client import RoomClient

logger = logging.getLogger("services")

from meshagent.api.webhooks import WebhookServer

class Portable(Protocol):
    async def start(room: RoomClient) -> None: ...
    async def stop() -> None: ...

class ServiceHost:

    def __init__(self, *, host: Optional[str] = None, webhook_secret: Optional[str] = None):

        if host == None:
            host = "localhost"

        self.host = host
        self.webhook_secret = webhook_secret
        self.ports = list[WebhookServer]()

    def port(self, *, path: Optional[str] = None, port: int):
        def deco(cls: type[Portable]):

            class ServiceWebhookServer(WebhookServer):

                def __init__(self, *, host = None, port = None, webhook_secret = None, app = None, path = None, validate_webhook_secret = None):
                    super().__init__(host=host, port=port, webhook_secret=webhook_secret, app=app, path=path, validate_webhook_secret=validate_webhook_secret)
                
                async def _spawn(self, *, room_name: str, room_url: str, token: str, arguments: Optional[dict] = None):

                    logger.info(f"room: {room_name} url: {room_url} token: {token} arguments: {arguments}")
                    agent = cls()

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
            
            self.ports.append(ServiceWebhookServer(host=self.host, validate_webhook_secret=self.webhook_secret != None, path=path, port=port))

            return cls

        return deco

    async def start(self):
        await asyncio.gather(*map(lambda x: x.start(), self.ports))

    async def stop(self):
        await asyncio.gather(*map(lambda x: x.stop(), self.ports))
    
    async def run(self):
        await asyncio.gather(*map(lambda x: x.run(), self.ports))
        