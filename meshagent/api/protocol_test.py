import pytest
import asyncio
import logging

from meshagent.api.protocol import MemoryServerProtocol, MemoryClientProtocol, Protocol
from meshagent.api.chan import Chan

logger = logging.getLogger("test")


def test_protocol_has_metadata_dict() -> None:
    protocol = Protocol(read=False, write=False)

    protocol.metadata["http.headers"] = {"User-Agent": "meshagent-test-client/1.0"}

    assert protocol.metadata == {
        "http.headers": {"User-Agent": "meshagent-test-client/1.0"}
    }


@pytest.mark.asyncio
async def test_protocol():
    input = Chan[bytes]()
    output = Chan[bytes]()

    async with MemoryClientProtocol(input=input, output=output, token="") as client:
        async with MemoryServerProtocol(input=output, output=input) as server:
            last_data = None
            last_type = None
            last_message_id = None

            fut = asyncio.Future()

            def handler(_, message_id, type, data):
                nonlocal last_data
                nonlocal last_type
                nonlocal last_message_id

                last_data = data
                last_type = type
                last_message_id = message_id

                fut.set_result(True)

            server.register_handler("hello", handler)

            size = 1000 * 100

            def yield_bytes():
                for i in range(size):
                    yield 1

            data = bytes(iter(yield_bytes()))

            await client.send(type="hello", data=data, message_id=2)

            await fut

            assert last_data is not None
            assert last_type == "hello"
            assert last_message_id == 2

            for i in range(size):
                assert last_data[i] == 1


@pytest.mark.asyncio
async def test_wait_for_close_does_not_cancel_protocol_on_waiter_cancellation() -> None:
    input = Chan[bytes]()
    output = Chan[bytes]()

    async with MemoryClientProtocol(input=input, output=output, token="") as client:
        waiter = asyncio.create_task(client.wait_for_close())
        await asyncio.sleep(0)
        waiter.cancel()

        with pytest.raises(asyncio.CancelledError):
            await waiter

        follow_up_waiter = asyncio.create_task(client.wait_for_close())
        await asyncio.sleep(0)
        assert follow_up_waiter.done() is False
        follow_up_waiter.cancel()

        with pytest.raises(asyncio.CancelledError):
            await follow_up_waiter
