import asyncio

import pytest

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
