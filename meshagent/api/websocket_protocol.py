import contextlib
import urllib.parse
from aiohttp import ClientSession, WSMsgType, web, ClientWebSocketResponse
import asyncio
import logging
import os
import sys
import urllib
from meshagent.api.version import __version__
from meshagent.api.http import new_client_session
from typing import Optional

from meshagent.api.protocol import Protocol, ClientProtocol, ProtocolCloseKind

logger = logging.getLogger("protocol.websocket")

DEFAULT_WEBSOCKET_HEARTBEAT = 60.0
WEBSOCKET_HEARTBEAT_ENV = "MESHAGENT_WEBSOCKET_HEARTBEAT"


def resolve_websocket_heartbeat(heartbeat: float | None = None) -> float:
    if heartbeat is None:
        configured = os.getenv(WEBSOCKET_HEARTBEAT_ENV)
        if configured is None or configured == "":
            heartbeat = DEFAULT_WEBSOCKET_HEARTBEAT
        else:
            try:
                heartbeat = float(configured)
            except ValueError as ex:
                raise ValueError(
                    f"{WEBSOCKET_HEARTBEAT_ENV} must be a positive number"
                ) from ex

    if heartbeat <= 0:
        raise ValueError("websocket heartbeat must be greater than zero")

    return heartbeat


def _log_websocket_close(
    *,
    role: str,
    url: str | None,
    close_event: WSMsgType | None,
    close_code: int | None,
    exc: BaseException | None,
) -> None:
    log = logger.warning if exc is not None else logger.debug
    log(
        "%s websocket closed url=%s close_event=%s close_code=%s exception=%r",
        role,
        url,
        None if close_event is None else close_event.name,
        close_code,
        exc,
    )


def _format_websocket_close_reason(
    *,
    close_event: WSMsgType | None,
    close_code: int | None,
    exc: BaseException | None,
) -> str | None:
    exc_message = None
    if exc is not None:
        exc_message = str(exc).strip() or repr(exc)

    if close_code is not None:
        if exc_message is not None:
            return f"websocket closed with code {close_code}: {exc_message}"
        return f"websocket closed with code {close_code}"

    if exc_message is not None:
        return f"websocket error: {exc_message}"

    if close_event is not None:
        return f"websocket closed with event {close_event.name}"

    return None


class WebSocketClientProtocol(ClientProtocol):
    def __init__(
        self,
        *,
        url: str,
        token: str,
        heartbeat: float | None = None,
        session: ClientSession | None = None,
    ):
        super().__init__(token=token)
        self._url = url
        self._heartbeat = resolve_websocket_heartbeat(heartbeat)
        self._session = session
        self._session_external = session is not None
        self._session_entered = False
        self._ws_ctx = None
        self._ws = None
        self._ws_recv_task = None
        self._ws_entered = False

    @property
    def url(self):
        return self._url

    def create_factory(self):
        session = self._session if self._session_external else None

        def factory() -> ClientProtocol:
            return WebSocketClientProtocol(
                url=self._url,
                token=self.token,
                heartbeat=self._heartbeat,
                session=session,
            )

        return factory

    async def __aenter__(self):
        try:
            if self._session is None:
                self._session = new_client_session()
                self._session_external = False

            if not self._session_external:
                await self._session.__aenter__()
                self._session_entered = True

            url_parts = urllib.parse.urlparse(self._url)
            query_dict = urllib.parse.parse_qs(url_parts.query)
            query_dict.update({"token": self.token})
            query_dict.update({"v": __version__})
            new_query_string = urllib.parse.urlencode(query_dict, doseq=True)
            url_with_params = urllib.parse.urlunparse(
                (
                    url_parts.scheme,
                    url_parts.netloc,
                    url_parts.path,
                    url_parts.params,
                    new_query_string,
                    url_parts.fragment,
                )
            )

            self._ws_ctx = self._session.ws_connect(
                url_with_params, heartbeat=self._heartbeat
            )
            self._ws = await self._ws_ctx.__aenter__()
            self._ws_entered = True

            self._ws_recv_task = asyncio.create_task(self._ws_recv())

            await super().__aenter__()
            return self
        except BaseException:
            await self.__aexit__(*sys.exc_info())
            raise

    async def _ws_recv(self):
        close_event: WSMsgType | None = None
        if self._ws is None:
            return
        async for msg in self._ws:
            if msg.type == WSMsgType.BINARY:
                self.receive_packet(msg.data)
            elif msg.type == WSMsgType.CLOSED:
                close_event = msg.type
                break
            elif msg.type == WSMsgType.ERROR:
                close_event = msg.type
                break
            else:
                raise (Exception("Unexpected message type"))

        ws_exception = self._ws.exception()
        if self._ws.closed or close_event is not None or ws_exception is not None:
            close_reason = _format_websocket_close_reason(
                close_event=close_event,
                close_code=self._ws.close_code,
                exc=ws_exception,
            )
            close_kind = self.close_kind()
            if close_kind is None:
                close_kind = (
                    ProtocolCloseKind.ERROR
                    if ws_exception is not None
                    else ProtocolCloseKind.SERVER
                )
            self._set_close_state(kind=close_kind, reason=close_reason)
            _log_websocket_close(
                role="client",
                url=self._url,
                close_event=close_event,
                close_code=self._ws.close_code,
                exc=ws_exception,
            )

        if self._ws.closed or close_event is not None or ws_exception is not None:
            super()._shutdown()

    async def __aexit__(self, exc_type, exc, tb):
        if self._ws is not None and not self._ws.closed:
            with contextlib.suppress(Exception):
                await self._ws.close()
        if self._ws_recv_task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await self._ws_recv_task
        if self._ws_ctx is not None and self._ws_entered:
            with contextlib.suppress(Exception):
                await self._ws_ctx.__aexit__(exc_type, exc, tb)
            self._ws_entered = False
        if (
            self._session is not None
            and not self._session_external
            and self._session_entered
        ):
            await self._session.__aexit__(exc_type, exc, tb)
            self._session_entered = False
        await super().__aexit__(exc_type, exc, tb)

    async def send_packet(self, data: bytes) -> None:
        await self._ws.send_bytes(data)


class WebSocketServerProtocol(Protocol):
    def __init__(
        self,
        socket: web.WebSocketResponse | ClientWebSocketResponse,
        token: Optional[str] = None,
        url: Optional[str] = None,
    ):
        super().__init__()
        self.socket = socket
        self._token = token
        self._url = url

    @property
    def url(self):
        return self._url

    @property
    def token(self) -> str | None:
        return self._token

    async def __aenter__(self):
        self._ws_recv_task = asyncio.create_task(self._ws_recv())

        await super().__aenter__()
        return self

    async def _ws_recv(self):
        close_event: WSMsgType | None = None
        try:
            async for msg in self.socket:
                if msg.type == WSMsgType.BINARY:
                    self.receive_packet(msg.data)
                elif msg.type == WSMsgType.CLOSED:
                    close_event = msg.type
                    break
                elif msg.type == WSMsgType.ERROR:
                    close_event = msg.type
                    break
                else:
                    raise (Exception("Unexpected message type"))
        finally:
            socket_exception = self.socket.exception()
            if (
                self.socket.closed
                or close_event is not None
                or socket_exception is not None
            ):
                close_reason = _format_websocket_close_reason(
                    close_event=close_event,
                    close_code=self.socket.close_code,
                    exc=socket_exception,
                )
                close_kind = self.close_kind()
                if close_kind is None:
                    close_kind = (
                        ProtocolCloseKind.ERROR
                        if socket_exception is not None
                        else ProtocolCloseKind.SERVER
                    )
                self._set_close_state(kind=close_kind, reason=close_reason)
                _log_websocket_close(
                    role="server",
                    url=self._url,
                    close_event=close_event,
                    close_code=self.socket.close_code,
                    exc=socket_exception,
                )
            self._shutdown()

    async def __aexit__(self, exc_type, exc, tb):
        if not self.socket.closed:
            await self.socket.close()

        self._ws_recv_task.cancel()

        await super().__aexit__(exc_type=exc_type, exc=exc, tb=tb)

    async def send_packet(self, data: bytes) -> None:
        await self.socket.send_bytes(data)
