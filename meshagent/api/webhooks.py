import json
import asyncio

from typing import Optional

import logging
import signal
from aiohttp import web
import os
import hashlib
import jwt

from meshagent.api import WebSocketClientProtocol, RoomException

from .schema import MeshSchema
from .helpers import deploy_schema, websocket_protocol, websocket_room_url, validate_schema_name
from .room_server_client import RoomClient

logger = logging.getLogger("webhooks")
logger.setLevel(logging.INFO)

class RoomStartedEvent:
    def __init__(self, *, room_name: str, room_url: str):
        self.room_name = room_name
        self.room_url = room_url

class RoomEndedEvent:
    def __init__(self, *, room_name: str):
        self.room_name = room_name

class CallEvent:
    def __init__(self, *, room_name: str, room_url: str, token: str, arguments: Optional[dict] = None):
        self.room_name = room_name
        self.room_url = room_url
        self.token = token
        self.arguments = arguments

class WebhookServer:

    def __init__(self, *, host: Optional[str] = None, port: Optional[int] = None, webhook_secret: Optional[str] = None, app: Optional[web.Application] = None, path: Optional[str] = None, validate_webhook_secret: Optional[bool] = None):
        if validate_webhook_secret == None:
            validate_webhook_secret = True

        if host == None:
            host = "0.0.0.0"


        if port == None:
            port_env = os.environ.get("MESHAGENT_PORT", None)
            if port_env == None:
                port = 8080
            else:
                port = int(port_env)

        self._host = host
        self._port = port
        
        if app == None:
            self._shared = False
            app = web.Application()
        else:
            self._shared = True

        if path == None:
            path = "/webhook"

        self._path = path

        self._app = app
        self._runner = None
        self._site = None

        self._validate_webhook_secret = validate_webhook_secret

        if webhook_secret == None:
            webhook_secret = os.getenv("MESHAGENT_WEBHOOK_SECRET")

        self._webhook_secret = webhook_secret

        self.add_routes(self._app)

    @property
    def app(self):
        return self._app
    
    def add_routes(self, app: web.Application) -> None:
        
        # add a root request handler, many serverless servers will require this
        if self._shared == False:
            app.router.add_get("/", self._liveness_check_request)

        app.router.add_post(self._path, self._webhook_request)
        
    async def _liveness_check_request(self, request: web.Request):
        return web.json_response({"ok":True})

    async def _webhook_request(self, request: web.Request):

        req : dict = await request.json()

        if isinstance(req, dict) == False:
            raise web.HTTPBadRequest(reason="invalid request body")
        
        event = req.get("event", None)
        data = req.get("data", None)

        
        if self._validate_webhook_secret:
            authorization = request.headers.get("Authorization")
            if authorization == None:
                raise web.HTTPUnauthorized(reason="missing authorization")
            
            if authorization.startswith("Bearer ") == False:
                raise web.HTTPUnauthorized(reason="missing authorization")
            
            raw_jwt = authorization.removeprefix("Bearer ")

            try:
                decoded_jwt:dict= jwt.decode(raw_jwt, key=self._webhook_secret, algorithms=["HS256"])
            except Exception as e:
                logger.warning("invalid jwt", exc_info=e)
                raise web.HTTPUnauthorized(reason="invalid jwt")

            sha256 = decoded_jwt["sha256"]

            payload = json.dumps(req)
            hash = hashlib.sha256(payload.encode())

            if hash.hexdigest() != sha256:
                raise web.HTTPUnauthorized(reason="signature does not match payload")

        logger.info(f"received webhook event={event} data={data}")
        await self.on_webhook(payload=req)

        return web.json_response({"ok":True})

    async def on_webhook(self, *, payload: dict):
        
        event = payload["event"]
        data = payload["data"]
        

        if event == "room.started":
            url = data["room_url"]
            await self.on_room_started(RoomStartedEvent(room_name=data["room_name"], room_url=url))

        elif event == "room.ended":
            await self.on_room_ended(RoomEndedEvent(room_name=data["room_name"]))

        elif event == "room.call":
            url = data["room_url"]
            await self.on_call(CallEvent(room_name=data["room_name"], room_url=url, token=data["token"], arguments=data["arguments"]))
        
    async def on_room_started(self, event: RoomStartedEvent):
        pass
    
    async def on_room_ended(self, event: RoomEndedEvent):
        pass

    async def on_call(self, event: CallEvent):
        pass
 
    async def __aenter__(self):
        
        if self._shared == False:
            self._runner = web.AppRunner(self._app)
            
            await self._runner.setup()

            logger.info(f"starting webhook server on {self._host}:{self._port}")
            
            self._site = web.TCPSite(self._runner, self._host, self._port)
            await self._site.start()
        
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._shared == False:
            await self._site.stop()
            await self._runner.cleanup()

    async def run(self):
        await self.__aenter__()
        try:

            term = asyncio.Future()

            def clean_termination(signal, frame):
                term.set_result(True)
            
            signal.signal(signal.SIGTERM, clean_termination)
            signal.signal(signal.SIGABRT, clean_termination)

            await term

        finally:
            await self.__aexit__(None, None, None)


class SchemaRegistration:
    def __init__(self, *, name: str, schema: MeshSchema):

        validate_schema_name(name=name)
        
        self._name = name
        self._schema = schema

    @property
    def name(self): 
        return self._name
    

    @property
    def schema(self):
        return self._schema


class SchemaRegistry(WebhookServer):
    def __init__(self, *, path: Optional[str] = None, app: Optional[web.Application] = None,  host = None, port = None, webhook_secret = None, schemas: list[SchemaRegistration], name: str = "schema.registry", validate_webhook_secret: Optional[bool] = None):
        self._name = name
        self._schemas = schemas
        
        super().__init__(path=path, app = app, host=host, port=port, webhook_secret=webhook_secret, validate_webhook_secret=validate_webhook_secret)
    
    @property
    def name(self):
        return self._name

    @property
    def schemas(self):
        return self._schemas

    async def on_call(self, event: CallEvent):
        
         async with RoomClient(protocol=WebSocketClientProtocol(url=event.room_url, token=event.token)) as room:
    
            await asyncio.gather(
                *list(
                    map(lambda r: deploy_schema(room=room, schema=r.schema, name=r.name), self._schemas)
                )
            )