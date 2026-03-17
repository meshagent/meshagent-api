import pytest

from meshagent.api.websocket_protocol import (
    DEFAULT_WEBSOCKET_HEARTBEAT,
    WEBSOCKET_HEARTBEAT_ENV,
    resolve_websocket_heartbeat,
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
