import json
import asyncio

from typing import Optional

import logging
import signal
from aiohttp import web
import os
import hashlib
import jwt

logger = logging.getLogger("webhooks")


class RoomStartedEvent:
    def __init__(self, *, room_name: str, room_url: str):
        self.room_name = room_name
        self.room_url = room_url


class RoomEndedEvent:
    def __init__(self, *, room_name: str):
        self.room_name = room_name


class CallEvent:
    def __init__(
        self,
        *,
        room_name: str,
        room_url: str,
        token: str,
        arguments: Optional[dict] = None,
    ):
        self.room_name = room_name
        self.room_url = room_url
        self.token = token
        self.arguments = arguments


class WebhookServer:
    def __init__(
        self,
        *,
        host: Optional[str] = None,
        port: Optional[int] = None,
        webhook_secret: Optional[str] = None,
        app: Optional[web.Application] = None,
        path: Optional[str] = None,
        validate_webhook_secret: Optional[bool] = None,
    ):
        if validate_webhook_secret is None:
            validate_webhook_secret = True

        if host is None:
            host = "0.0.0.0"

        if port is None:
            port_env = os.environ.get("MESHAGENT_PORT", None)
            if port_env is None:
                port = 8080
            else:
                port = int(port_env)

        self._host = host
        self._port = port

        if app is None:
            self._shared = False
            app = web.Application()
        else:
            self._shared = True

        if path is None:
            path = "/webhook"

        self._path = path

        self._app = app
        self._runner = None
        self._site = None

        self._validate_webhook_secret = validate_webhook_secret

        if webhook_secret is None:
            webhook_secret = os.getenv("MESHAGENT_WEBHOOK_SECRET")

        self._webhook_secret = webhook_secret

        self.add_routes(self._app)

    @property
    def app(self):
        return self._app

    def add_routes(self, app: web.Application) -> None:
        # add a root request handler, many serverless servers will require this
        if not self._shared:
            app.router.add_get("/", self._liveness_check_request)

        app.router.add_post(self._path, self._webhook_request)

    async def _liveness_check_request(self, request: web.Request):
        return web.json_response({"ok": True})

    async def _webhook_request(self, request: web.Request):
        req: dict = await request.json()

        if not isinstance(req, dict):
            raise web.HTTPBadRequest(reason="invalid request body")

        event = req.get("event", None)
        data = req.get("data", None)

        if self._validate_webhook_secret:
            authorization = request.headers.get("Meshagent-Signature")
            if authorization is None:
                raise web.HTTPUnauthorized(reason="missing signature")

            if not authorization.startswith("Bearer "):
                raise web.HTTPUnauthorized(reason="missing signature")

            raw_jwt = authorization.removeprefix("Bearer ")

            try:
                decoded_jwt: dict = jwt.decode(
                    raw_jwt, key=self._webhook_secret, algorithms=["HS256"]
                )
            except Exception as e:
                logger.warning("invalid jwt", exc_info=e)
                raise web.HTTPUnauthorized(reason="invalid jwt")

            sha256 = decoded_jwt["sha256"]

            payload = json.dumps(req)
            hash = hashlib.sha256(payload.encode())

            if hash.hexdigest() != sha256:
                raise web.HTTPUnauthorized(reason="signature does not match payload")

        logger.debug(f"received webhook event={event} data={data}")
        await self.on_webhook(payload=req)

        return web.json_response({"ok": True})

    async def on_webhook(self, *, payload: dict):
        event = payload["event"]
        data = payload["data"]

        if event == "room.started":
            url = data["room_url"]
            await self.on_room_started(
                RoomStartedEvent(room_name=data["room_name"], room_url=url)
            )

        elif event == "room.ended":
            await self.on_room_ended(RoomEndedEvent(room_name=data["room_name"]))

        elif event == "room.call":
            url = data["room_url"]
            await self.on_call(
                CallEvent(
                    room_name=data["room_name"],
                    room_url=url,
                    token=data["token"],
                    arguments=data["arguments"],
                )
            )

    async def on_room_started(self, event: RoomStartedEvent):
        pass

    async def on_room_ended(self, event: RoomEndedEvent):
        pass

    async def on_call(self, event: CallEvent):
        pass

    async def __aenter__(self):
        if not self._shared:
            self._runner = web.AppRunner(self._app, access_log=None)

            await self._runner.setup()

            logger.info(f"starting webhook server on {self._host}:{self._port}")

            self._site = web.TCPSite(self._runner, self._host, self._port)
            await self._site.start()

        return self

    async def __aexit__(self, exc_type, exc, tb):
        if not self._shared:
            await self._site.stop()
            await self._runner.cleanup()

    async def start(self):
        await self.__aenter__()

    async def stop(self):
        await self.__aexit__(None, None, None)

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
