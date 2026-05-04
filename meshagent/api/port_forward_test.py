import asyncio

import pytest

from meshagent.api import port_forward as port_forward_module
from meshagent.api.port_forward import port_forward
from meshagent.api.port_forward import LocalExposeHandle


class _FakeServer:
    def __init__(self):
        self.closed = False
        self.wait_closed_called = False

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        self.wait_closed_called = True


@pytest.mark.asyncio
async def test_local_expose_handle_close_suppresses_internal_task_cancellation() -> (
    None
):
    server = _FakeServer()
    task = asyncio.create_task(asyncio.sleep(60))
    handle = LocalExposeHandle(
        host="127.0.0.1",
        port=12345,
        server=server,
        task=task,
    )

    await handle.close()

    assert server.closed is True
    assert server.wait_closed_called is True
    assert task.cancelled() is True


@pytest.mark.asyncio
async def test_port_forward_closes_session_when_local_server_bind_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeSession:
        def __init__(self) -> None:
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    async def _fail_start_server(*args, **kwargs):
        del args, kwargs
        raise OSError("bind failed")

    session = _FakeSession()
    monkeypatch.setattr(
        port_forward_module,
        "new_client_session",
        lambda *, timeout: session,
    )
    monkeypatch.setattr(
        port_forward_module.asyncio,
        "start_server",
        _fail_start_server,
    )

    with pytest.raises(OSError, match="bind failed"):
        await port_forward(
            container_id="container-123",
            port=8080,
            token="token",
        )

    assert session.closed is True
