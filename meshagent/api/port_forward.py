from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass

import aiohttp

from meshagent.api.helpers import meshagent_base_url


@dataclass
class LocalExposeHandle:
    host: str
    port: int
    server: asyncio.AbstractServer
    task: asyncio.Task

    async def close(self) -> None:
        self.server.close()
        await self.server.wait_closed()
        self.task.cancel()
        with contextlib.suppress(Exception):
            await self.task


async def port_forward(
    *,
    container_id: str,
    port: int,
    listen_host: str = "127.0.0.1",
    listen_port: int = 0,
    token: str,
    connect_timeout: float = 15.0,
    idle_timeout: float = 60.0,
    max_chunk: int = 64 * 1024,
) -> LocalExposeHandle:
    """
    Expose a remote tunnel WebSocket as a local TCP port.

    - Starts a local TCP server on (listen_host, listen_port). If listen_port == 0,
      the OS picks a free ephemeral port.
    - For each local TCP connection:
        * opens a WebSocket to tunnel_ws_url
        * forwards local bytes -> WS binary frames
        * forwards WS binary frames -> local socket
    - Intended for a *raw-byte* WS tunnel (binary frames are the byte stream).

    Returns a handle with the chosen port and a close() method.
    """

    tunnel_ws_url = (
        meshagent_base_url().replace("http", "ws", 1)
        + f"/tunnel/{container_id}/{port}/"
    )

    timeout = aiohttp.ClientTimeout(
        total=None,
        sock_connect=connect_timeout,
        sock_read=None,
    )

    session = aiohttp.ClientSession(timeout=timeout)

    async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        ws: aiohttp.ClientWebSocketResponse | None = None

        try:
            ws = await session.ws_connect(
                tunnel_ws_url,
                headers={"Authorization": "Bearer " + token},
                autoping=True,
                heartbeat=30.0,
                compress=0,
            )

            async def tcp_to_ws():
                try:
                    while True:
                        data = await reader.read(max_chunk)
                        if not data:
                            return
                        await ws.send_bytes(data)
                finally:
                    # Ensure upstream sees close
                    with contextlib.suppress(Exception):
                        await ws.close()

            async def ws_to_tcp():
                try:
                    while True:
                        msg = await ws.receive(timeout=idle_timeout)
                        if msg.type == aiohttp.WSMsgType.BINARY:
                            if msg.data:
                                writer.write(msg.data)
                                await writer.drain()
                        elif msg.type == aiohttp.WSMsgType.TEXT:
                            # Optional: ignore or treat as control messages
                            continue
                        elif msg.type in (
                            aiohttp.WSMsgType.CLOSE,
                            aiohttp.WSMsgType.CLOSING,
                            aiohttp.WSMsgType.CLOSED,
                        ):
                            return
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            return
                except asyncio.TimeoutError:
                    # Idle timeout: close connection
                    return
                finally:
                    with contextlib.suppress(Exception):
                        writer.close()
                        await writer.wait_closed()

            t1 = asyncio.create_task(tcp_to_ws())
            t2 = asyncio.create_task(ws_to_tcp())
            done, pending = await asyncio.wait(
                {t1, t2}, return_when=asyncio.FIRST_COMPLETED
            )
            for t in pending:
                t.cancel()
                with contextlib.suppress(Exception):
                    await t

        except Exception:
            # Best-effort close local socket if WS setup fails
            with contextlib.suppress(Exception):
                writer.close()
                await writer.wait_closed()
            raise
        finally:
            if ws is not None:
                with contextlib.suppress(Exception):
                    await ws.close()

    server = await asyncio.start_server(
        handle_client, host=listen_host, port=listen_port
    )
    sock = (server.sockets or [None])[0]
    if sock is None:
        await session.close()
        raise RuntimeError("failed to bind local port")

    chosen_port = sock.getsockname()[1]

    async def serve_forever():
        try:
            async with server:
                await server.serve_forever()
        finally:
            await session.close()

    task = asyncio.create_task(serve_forever())

    return LocalExposeHandle(
        host=listen_host,
        port=chosen_port,
        server=server,
        task=task,
    )
