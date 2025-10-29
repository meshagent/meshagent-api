import asyncio
from typing import Optional, Protocol
import logging
from aiohttp import web
import os
import signal
import json

import jwt
import hashlib
import aiohttp

from meshagent.api import RoomException
from meshagent.api.participant_token import ApiScope
from meshagent.api.webhooks import WebhookServer
from meshagent.api import WebSocketClientProtocol, RoomMessage
from meshagent.api.room_server_client import RoomClient

from meshagent.api.specs.service import (
    ContainerSpec,
    ServiceSpec,
    PortSpec,
    EndpointSpec,
    MeshagentEndpointSpec,
    ServiceMetadata,
)

well_known_service_path = "/.well-known/meshagent-service.json"

logger = logging.getLogger("services")


class Portable(Protocol):
    async def start(self, *, room: RoomClient) -> None: ...
    async def stop(self) -> None: ...


class ServicePath:
    def __init__(
        self,
        *,
        path: str,
        cls: type[Portable],
        identity: Optional[str] = None,
        permissions: Optional[ApiScope] = None,
    ):
        self.cls = cls
        self.path = path
        self.identity = identity
        self.permissions = permissions


class ServiceHost:
    def __init__(
        self,
        *,
        host: Optional[str] = None,
        webhook_secret: Optional[str] = None,
        port: Optional[int] = None,
        name: Optional[str] = None,
    ):
        if host is None:
            host = os.getenv("MESHAGENT_HOST", "0.0.0.0")

        self.host = host
        self.webhook_secret = webhook_secret
        if port is None:
            port = os.getenv("MESHAGENT_PORT", 8081)
            if port is not None:
                port = int(port)

        self.port = port
        self.paths = list[ServicePath]()

        self._hosts = None
        self._app = None
        self._supports_websockets = True

        self.name = name

    def path(
        self,
        path: str,
        *,
        identity: Optional[str] = None,
        permissions: Optional[ApiScope] = None,
    ):
        def deco(cls: type[Portable]):
            self.paths.append(
                ServicePath(
                    path=path, cls=cls, identity=identity, permissions=permissions
                )
            )
            return cls

        return deco

    def add_path(
        self,
        path: str,
        *,
        cls,
        identity: Optional[str] = None,
        permissions: Optional[ApiScope] = None,
    ):
        self.paths.append(
            ServicePath(path=path, cls=cls, identity=identity, permissions=permissions)
        )

    async def _liveness_check_request(self, request: web.Request):
        return web.json_response({"ok": True})

    async def _start_server(self):
        self._runner = web.AppRunner(self._app, access_log=None)

        await self._runner.setup()

        for path in self.paths:
            logger.debug(
                f"starting -> {self.host}:{self.port}{path.path} -> {path.cls.__name__}"
            )

        self._site = web.TCPSite(self._runner, self.host, self.port)
        await self._site.start()
        return self

    async def _stop_server(self):
        await self._site.stop()

        self._app = None
        self._site = None

    def _create_host(self, p: ServicePath):
        supports_websockets = self._supports_websockets

        class ServiceWebhookServer(WebhookServer):
            def __init__(
                self,
                *,
                host=None,
                port=None,
                webhook_secret=None,
                app=None,
                path=None,
                validate_webhook_secret=None,
            ):
                super().__init__(
                    host=host,
                    port=port,
                    webhook_secret=webhook_secret,
                    app=app,
                    path=path,
                    validate_webhook_secret=validate_webhook_secret,
                )

                self._done = asyncio.Future()
                self._tasks = list[asyncio.Task]()

                self._supports_websockets = supports_websockets

            async def stop(self):
                self._done.set_result(True)

                logger.debug("waiting for service host tasks to complete")

                await asyncio.gather(*self._tasks)

                logger.debug("service host tasks completed")

                await super().stop()

            async def _spawn(
                self,
                *,
                room_name: str,
                room_url: str,
                token: str,
                arguments: Optional[dict] = None,
            ):
                async def run():
                    logger.debug("service host runner started")
                    async with RoomClient(
                        protocol=WebSocketClientProtocol(url=room_url, token=token)
                    ) as room:
                        task = asyncio.create_task(self.on_call_answered(room=room))

                        def task_done(t):
                            try:
                                t.result()
                            except Exception as e:
                                logger.error(
                                    f"Service host task failed {e}", exc_info=e
                                )

                        task.add_done_callback(task_done)

                        await asyncio.wait(
                            [
                                asyncio.wrap_future(self._done),
                                task,
                            ],
                            return_when=asyncio.FIRST_COMPLETED,
                        )

                        logger.debug("service host runner completed")

                task = asyncio.create_task(run())
                self._tasks.append(task)

            async def on_call_answered(self, room: RoomClient):
                dismissed = asyncio.Future()

                def on_message(message: RoomMessage):
                    if message.type == "dismiss":
                        logger.info(f"dismissed by {message.from_participant_id}")
                        dismissed.set_result(True)

                room.messaging.on("message", on_message)

                agent = p.cls()
                logger.info(
                    f"{getattr(agent, 'name', '')} answering call and joining room"
                )

                await agent.start(room=room)

                done, pending = await asyncio.wait(
                    [
                        asyncio.wrap_future(dismissed),
                        asyncio.create_task(room.protocol.wait_for_close()),
                    ],
                    return_when=asyncio.FIRST_COMPLETED,
                )

                await agent.stop()

            async def on_call(self, event):
                await self._spawn(
                    room_name=event.room_name,
                    room_url=event.room_url,
                    token=event.token,
                    arguments=event.arguments,
                )

        host = ServiceWebhookServer(
            validate_webhook_secret=self.webhook_secret is not None,
            path=p.path,
            app=self._app,
        )
        return host

    async def start(self):
        if self._app is not None:
            raise Exception("App is already started")

        self._app = web.Application()

        self._app.router.add_get("/", self._liveness_check_request)
        self._app.router.add_get(well_known_service_path, self._describe)

        self._hosts: list[WebhookServer] = list(
            map(lambda x: self._create_host(x), self.paths)
        )

        await asyncio.gather(*map(lambda x: x.start(), self._hosts))

        await self._start_server()

    async def _describe(self, request: web.Request):
        return web.json_response(
            self.get_service_spec(image="").model_dump(mode="json")
        )

    async def stop(self):
        logger.debug("stopping service host")
        await self._stop_server()

        logger.debug("stopping hosted ports")

        await asyncio.gather(*map(lambda x: x.stop(), self._hosts))

        logger.debug("cleaning up runner")
        await self._runner.cleanup()
        self._runner = None
        self._hosts = None

        logger.debug("service host stopped")

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

    def get_service_spec(
        self,
        *,
        image: str,
        command: Optional[str] = None,
        environment: Optional[dict[str, str]] = None,
    ) -> ServiceSpec:
        spec = ServiceSpec(
            version="v1",
            kind="Service",
            metadata=ServiceMetadata(
                name=self.name if self.name is not None else "<YOUR_SERVICE_NAME>"
            ),
            container=ContainerSpec(
                command=command,
                image=image,
                environment=environment,
            ),
        )

        port = PortSpec(
            num=self.port,
            type="http",
        )
        spec.ports.append(port)
        for p in self.paths:
            port.endpoints.append(
                EndpointSpec(
                    path=p.path,
                    meshagent=MeshagentEndpointSpec(
                        identity=p.identity if p.identity is not None else "agent",
                        api=p.permissions,
                    ),
                )
            )

        return spec


async def send_webhook(
    session: aiohttp.ClientSession,
    *,
    url: str,
    event: str,
    data: dict,
    secret: Optional[str] = None,
    headers: Optional[dict[str, str]] = None,
):
    payload_body = {"event": event, "data": data}

    payload = json.dumps(payload_body)
    hash = hashlib.sha256(payload.encode())

    if headers is None:
        headers = {}

    headers = {
        **headers,
        "Content-Type": "application/json",
    }

    if secret is not None:
        headers["Meshagent-Signature"] = "Bearer " + jwt.encode(
            {"sha256": hash.hexdigest()}, key=secret, algorithm="HS256"
        )

    else:
        async with session.post(url, headers=headers, data=payload) as resp:
            try:
                resp.raise_for_status()

            except asyncio.CancelledError:
                raise

            except Exception as e:
                logger.warning("webhook call failed %s %s", event, url, exc_info=e)
                raise RoomException(
                    f"error status returned from webhook call {url}, http status code: {resp.status}"
                )
