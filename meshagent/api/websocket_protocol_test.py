import importlib

import pytest

from meshagent.api.websocket_protocol import (
    DEFAULT_WEBSOCKET_HEARTBEAT,
    WEBSOCKET_HEARTBEAT_ENV,
    WebSocketClientProtocol,
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
        self.ws_connect_calls: list[tuple[str, float | None]] = []

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.exited = True

    def ws_connect(self, url: str, *, heartbeat: float | None = None):
        self.ws_connect_calls.append((url, heartbeat))
        return _FailingWebsocketContext()


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
