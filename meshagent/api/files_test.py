from collections.abc import AsyncIterator
from dataclasses import dataclass, field

import pytest
import pytest_asyncio
from aiohttp import web
from pydantic import ValidationError

from meshagent.api import FilesTarget, Meshagent
from meshagent.api.client import PermissionDeniedError
from meshagent.api.room_server_client import RoomException


@dataclass
class FilesApiFixture:
    base_url: str = ""
    config_calls: int = 0
    files_domain: str | None = None
    calls: list[tuple[str, str, dict[str, str], str]] = field(default_factory=list)
    blobs: dict[str, bytes] = field(default_factory=dict)
    actions: list[dict] = field(default_factory=list)


@pytest_asyncio.fixture
async def files_api() -> AsyncIterator[FilesApiFixture]:
    state = FilesApiFixture()
    app = web.Application()

    async def config(request: web.Request) -> web.Response:
        state.config_calls += 1
        assert request.headers["Authorization"] == "Bearer test-token"
        return web.json_response({"domains": {"files": state.files_domain}})

    async def files(request: web.Request) -> web.Response:
        state.calls.append(
            (
                request.method,
                request.raw_path,
                dict(request.query),
                request.headers.get("Authorization", ""),
            )
        )
        if request.path.endswith("/denied"):
            return web.json_response({"error": "denied"}, status=403)
        if request.path.endswith("/redirect"):
            return web.Response(
                status=307, headers={"Location": "https://other.invalid/"}
            )
        if request.path == "/v1/presign":
            body = await request.json()
            state.actions.append(body)
            return web.json_response(
                {
                    "url": f"{state.base_url}/signed?X-Amz-Signature=signature",
                    "method": body["method"],
                    "headers": {},
                    "expires_at": "2030-01-01T00:00:00Z",
                }
            )
        if "permissions" in request.query:
            return web.json_response({"actions": ["read"], "recursive_actions": []})
        if "list" in request.query:
            token = request.query.get("continuation_token")
            return web.json_response(
                {
                    "entries": [
                        {"name": "two.txt" if token else "one.txt", "is_folder": False}
                    ],
                    "continuation_token": None if token else "page-2",
                }
            )
        if request.method == "POST":
            state.actions.append(await request.json())
            return web.json_response({})
        if request.method == "PUT":
            state.blobs[request.path] = await request.read()
            return web.Response(status=204)
        if request.method == "DELETE":
            state.blobs.pop(request.path, None)
            return web.Response(status=204)
        data = state.blobs.get(request.path)
        if data is None:
            return web.Response(status=404)
        return web.Response(
            body=data,
            content_type="application/octet-stream",
            headers={"ETag": '"file-etag"'},
        )

    app.router.add_get("/config", config)
    app.router.add_route("*", "/{tail:.*}", files)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    host, port, *_ = runner.addresses[0]
    state.base_url = f"http://{host}:{port}"
    state.files_domain = state.base_url
    try:
        yield state
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_room_agent_streams_share_discovery_session_and_current_credentials(
    files_api,
):
    async with Meshagent(base_url=files_api.base_url, token="test-token") as client:
        room = client.files.for_room(project_id="project-1", room_name="same")
        agent = client.files.for_agent(project_id="project-1", agent_name="same")
        path = "folder/ résumé 100%2F.txt"

        async def chunks():
            yield b"\x00\xfffirst"
            yield b"second"

        await room.upload(path, chunks(), overwrite=False)
        await agent.upload(path, b"agent-data")
        assert await room.download(path) == b"\x00\xfffirstsecond"
        assert (
            b"".join(
                [chunk async for chunk in agent.download_stream(path, chunk_size=3)]
            )
            == b"agent-data"
        )
        metadata = await room.stat(path)
        assert metadata.size == len(b"\x00\xfffirstsecond")
        assert metadata.etag == '"file-etag"'
        assert await agent.exists(path)
        assert not await agent.exists("missing")
        client.token = "rotated-token"
        await agent.delete(path)
        assert files_api.calls[-1][3] == "Bearer rotated-token"
        assert not await agent.exists(path)
        assert await room.exists(path)
        assert files_api.config_calls == 1
        assert "%252F.txt" in files_api.calls[0][1]
        assert files_api.calls[0][2]["overwrite"] == "false"


@pytest.mark.asyncio
async def test_listing_copy_move_and_presign_keep_explicit_targets(files_api):
    async with Meshagent(base_url=files_api.base_url, token="test-token") as client:
        room = client.files.for_room(project_id="source-project", room_name="source")
        agent = client.files.for_agent(
            project_id="destination-project", agent_name="dest"
        )
        permissions = await agent.permissions("encoded +%/path")
        assert permissions.actions == ["read"]
        assert permissions.recursive_actions == []
        assert files_api.calls[-1][2] == {"permissions": ""}
        assert [entry.name async for entry in agent.list("folder")] == [
            "one.txt",
            "two.txt",
        ]
        await agent.copy(room.location("source.txt"), "dest.txt")
        assert files_api.actions[-1] == {
            "source": {
                "target": {
                    "project_id": "source-project",
                    "kind": "room",
                    "name": "source",
                },
                "path": "source.txt",
            },
            "overwrite": False,
        }
        await agent.move(room.location("source.txt"), "moved.txt", overwrite=True)
        assert files_api.calls[-1][2]["action"] == "move"
        await agent.mkdir("new-folder")
        assert files_api.calls[-1][2]["action"] == "mkdir"
        signed = await agent.presign("dest.txt", method="PUT", expires_in=120)
        assert signed.method == "PUT"
        assert files_api.actions[-1] == {
            "target": {
                "project_id": "destination-project",
                "kind": "agent",
                "name": "dest",
            },
            "path": "dest.txt",
            "method": "PUT",
            "expires_in": 120,
        }


@pytest.mark.asyncio
async def test_denials_and_redirects_are_not_existence_or_mutation_success(files_api):
    async with Meshagent(base_url=files_api.base_url, token="test-token") as client:
        agent = client.files.for_agent(project_id="p1", agent_name="a1")
        with pytest.raises(PermissionDeniedError):
            await agent.exists("denied")
        with pytest.raises(RoomException, match="307"):
            await agent.upload("redirect", b"secret")
        assert len(files_api.calls) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "domain",
    [
        None,
        "ftp://other.invalid",
        "https://user:secret@other.invalid",
        "https://other.invalid/?token=x",
        "https://other.invalid?",
        "https://other.invalid#",
    ],
)
async def test_missing_or_invalid_discovery_fails_before_file_request(
    files_api, domain
):
    files_api.files_domain = domain
    async with Meshagent(base_url=files_api.base_url, token="test-token") as client:
        with pytest.raises(RoomException):
            await client.files.for_agent(project_id="p1", agent_name="a1").download(
                "file"
            )
        assert files_api.calls == []


@pytest.mark.asyncio
async def test_invalid_paths_fail_before_discovery(files_api):
    async with Meshagent(base_url=files_api.base_url, token="test-token") as client:
        agent = client.files.for_agent(project_id="p1", agent_name="a1")
        for path in ["../data", "/data", "a//b", "a/./b", "a\\b", "a\0b"]:
            with pytest.raises(ValueError):
                await agent.upload(path, b"secret")
        assert files_api.config_calls == 0


@pytest.mark.parametrize("name", ["", "..", "a/b", "a\\b", "a\0b"])
def test_target_cannot_escape_namespace(name):
    with pytest.raises(ValidationError):
        FilesTarget(project_id="p1", kind="agent", name=name)
