import importlib

import aiohttp
import pytest

from meshagent.api.protocol import ProtocolCloseKind
from meshagent.api.websocket_protocol import (
    DEFAULT_WEBSOCKET_HEARTBEAT,
    WEBSOCKET_HEARTBEAT_ENV,
    WebSocketClientProtocol,
    WebSocketServerProtocol,
    resolve_websocket_heartbeat,
)

websocket_protocol_module = importlib.import_module("meshagent.api.websocket_protocol")


class _FailingWebsocketContext:
    async def __aenter__(self):
        raise RuntimeError("websocket failed")

    async def __aexit__(self, exc_type, exc, tb) -> None:
        raise AssertionError("failed websocket enter should not exit websocket context")


class _FakeSession:
    def __init__(self) -> None:
        self.entered = False
        self.exited = False
        self.ws_connect_calls: list[
            tuple[str, dict[str, str] | None, float | None, int | None]
        ] = []

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.exited = True

    def ws_connect(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        heartbeat: float | None = None,
        compress: int | None = None,
    ):
        self.ws_connect_calls.append((url, headers, heartbeat, compress))
        return _FailingWebsocketContext()


class _ClosingWebSocket:
    closed = False

    async def send_bytes(self, data: bytes) -> None:
        raise aiohttp.client_exceptions.ClientConnectionResetError(
            "Cannot write to closing transport"
        )


def test_resolve_websocket_heartbeat_uses_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(WEBSOCKET_HEARTBEAT_ENV, raising=False)

    assert resolve_websocket_heartbeat() == DEFAULT_WEBSOCKET_HEARTBEAT


def test_resolve_websocket_heartbeat_uses_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(WEBSOCKET_HEARTBEAT_ENV, "120")

    assert resolve_websocket_heartbeat() == 120.0


def test_resolve_websocket_heartbeat_prefers_explicit_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(WEBSOCKET_HEARTBEAT_ENV, "120")

    assert resolve_websocket_heartbeat(90.0) == 90.0


def test_resolve_websocket_heartbeat_rejects_invalid_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(WEBSOCKET_HEARTBEAT_ENV, "abc")

    with pytest.raises(ValueError, match=WEBSOCKET_HEARTBEAT_ENV):
        resolve_websocket_heartbeat()


@pytest.mark.asyncio
async def test_websocket_client_protocol_closes_internal_session_when_enter_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _FakeSession()
    monkeypatch.setattr(
        websocket_protocol_module,
        "new_client_session",
        lambda: session,
    )
    protocol = WebSocketClientProtocol(url="ws://example.test/room", token="token")

    with pytest.raises(RuntimeError, match="websocket failed"):
        await protocol.__aenter__()

    assert session.entered is True
    assert session.exited is True


@pytest.mark.asyncio
async def test_websocket_client_protocol_keeps_external_session_open_when_enter_fails():
    session = _FakeSession()
    protocol = WebSocketClientProtocol(
        url="ws://example.test/room",
        token="token",
        session=session,
    )

    with pytest.raises(RuntimeError, match="websocket failed"):
        await protocol.__aenter__()

    assert session.entered is False
    assert session.exited is False
    assert len(session.ws_connect_calls) == 1
    assert session.ws_connect_calls[0][1] == {"Authorization": "Bearer token"}
    assert session.ws_connect_calls[0][3] == 15


@pytest.mark.asyncio
async def test_websocket_client_protocol_with_iap_omits_authorization_header():
    session = _FakeSession()
    protocol = WebSocketClientProtocol.withIAP(session=session)

    with pytest.raises(RuntimeError, match="websocket failed"):
        await protocol.__aenter__()

    assert protocol.url == "./.well-known/meshagent/room/connect"
    assert protocol.token is None
    assert len(session.ws_connect_calls) == 1
    assert session.ws_connect_calls[0][0].startswith(
        "./.well-known/meshagent/room/connect?v="
    )
    assert session.ws_connect_calls[0][1] is None


@pytest.mark.asyncio
async def test_websocket_client_protocol_treats_closing_transport_as_server_close():
    protocol = WebSocketClientProtocol(
        url="ws://example.test/room",
        token="token",
        session=_FakeSession(),
    )
    protocol._open = True
    protocol._ws = _ClosingWebSocket()

    await protocol.send_packet(b"hello")

    assert protocol.close_kind() == ProtocolCloseKind.SERVER
    assert protocol.close_reason() == "Cannot write to closing transport"
    assert protocol.is_open is False


@pytest.mark.asyncio
async def test_websocket_server_protocol_treats_closing_transport_as_client_close():
    protocol = WebSocketServerProtocol(socket=_ClosingWebSocket())
    protocol._open = True

    await protocol.send_packet(b"hello")

    assert protocol.close_kind() == ProtocolCloseKind.CLIENT
    assert protocol.close_reason() == "Cannot write to closing transport"
    assert protocol.is_open is False
