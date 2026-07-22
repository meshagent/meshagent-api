from aiohttp import web
import pytest

from meshagent.api.services import ServiceHost, ServicePath


class _Portable:
    async def start(self, *, room) -> None:
        pass

    async def stop(self) -> None:
        pass


@pytest.mark.asyncio
async def test_service_host_passes_explicit_webhook_secret_to_path_host() -> None:
    host = ServiceHost(webhook_secret="explicit-secret")
    host._app = web.Application()

    path_host = host._create_host(ServicePath(path="/service", cls=_Portable))

    assert path_host._validate_webhook_secret is True
    assert path_host._webhook_secret == "explicit-secret"
