"""Authenticated REST access to persistent room and managed-agent files."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, AsyncIterator
from datetime import datetime
from typing import TYPE_CHECKING, Literal
from urllib.parse import quote, urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator

from meshagent.api.room_server_client import RoomException, StorageEntry
from meshagent.api.file_permissions import (
    FILE_ACTIONS as FILE_ACTIONS,
    FILE_RELATIONS as FILE_RELATIONS,
    FileAction as FileAction,
    FileRole as FileRole,
    FilePathRule as FilePathRule,
    FilesPolicy as FilesPolicy,
    FilePermissions as FilePermissions,
    validate_file_path,
)

if TYPE_CHECKING:
    from aiohttp import ClientResponse
    from meshagent.api.client import Meshagent


def _component(value: str) -> str:
    if (
        not value
        or value in {".", ".."}
        or any(c in "/\\" or ord(c) < 32 or 127 <= ord(c) < 160 for c in value)
    ):
        raise ValueError("Files resource identifiers must be single path components")
    return value


_path = validate_file_path


class FilesTarget(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    project_id: str
    kind: Literal["room", "agent"]
    name: str

    @field_validator("project_id", "name")
    @classmethod
    def validate_component(cls, value: str) -> str:
        return _component(value)


class FileLocation(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    target: FilesTarget
    path: str

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        return _path(value)


class FilesPage(BaseModel):
    entries: list[StorageEntry]
    continuation_token: str | None = None


class FileMetadata(BaseModel):
    path: str
    size: int
    is_folder: bool = False
    content_type: str | None = None
    etag: str | None = None
    last_modified: str | None = None


class PresignedFileUrl(BaseModel):
    url: str
    method: Literal["GET", "HEAD", "PUT", "DELETE"]
    headers: dict[str, str] = Field(default_factory=dict)
    expires_at: datetime


class FilesClient:
    """Uses its owning Meshagent client's credentials, discovery and HTTP session.

    This client does not own a second session and must not outlive its parent.
    """

    def __init__(self, client: Meshagent):
        self._client = client
        self._base_url: str | None = None
        self._discovery_lock = asyncio.Lock()

    def for_room(self, *, project_id: str, room_name: str) -> TargetFilesClient:
        return self.for_target(
            FilesTarget(project_id=project_id, kind="room", name=room_name)
        )

    def for_agent(self, *, project_id: str, agent_name: str) -> TargetFilesClient:
        return self.for_target(
            FilesTarget(project_id=project_id, kind="agent", name=agent_name)
        )

    def for_target(self, target: FilesTarget) -> TargetFilesClient:
        return TargetFilesClient(self, target)

    async def _endpoint(self) -> str:
        async with self._discovery_lock:
            if self._base_url is None:
                config = await self._client.get_config()
                domain = config.domains.files
                if not domain:
                    raise RoomException(
                        "The files API is unavailable: deployment config has no domains.files"
                    )
                base_url = domain if "://" in domain else f"https://{domain}"
                parsed = urlsplit(base_url)
                if (
                    parsed.scheme not in {"http", "https"}
                    or not parsed.hostname
                    or parsed.username is not None
                    or parsed.password is not None
                    or "?" in base_url
                    or "#" in base_url
                ):
                    raise RoomException(
                        "Deployment config contains an invalid files endpoint"
                    )
                self._base_url = base_url.rstrip("/")
            return self._base_url

    async def _url(self, target: FilesTarget, path: str) -> str:
        _path(path)
        base_url = await self._endpoint()
        namespace = "rooms" if target.kind == "room" else "agents"
        return (
            f"{base_url}/{quote(target.project_id, safe='')}/{namespace}/"
            f"{quote(target.name, safe='')}/{quote(path, safe='/')}"
        )

    async def _check_response(self, response: ClientResponse) -> None:
        await self._client._raise_for_status(response)
        if not 200 <= response.status < 300:
            raise RoomException(
                f"Files request returned unexpected HTTP status {response.status}"
            )


class TargetFilesClient:
    def __init__(self, files: FilesClient, target: FilesTarget):
        self._files = files
        self.target = target

    def location(self, path: str) -> FileLocation:
        return FileLocation(target=self.target, path=path)

    async def permissions(self, path: str = "") -> FilePermissions:
        client = self._files._client
        async with client._session.get(
            await self._files._url(self.target, path),
            params={"permissions": ""},
            headers=client._get_headers(),
            allow_redirects=False,
        ) as response:
            await self._files._check_response(response)
            return await client._read_model(response, FilePermissions)

    async def list_page(
        self,
        path: str = "",
        *,
        continuation_token: str | None = None,
        limit: int = 1000,
    ) -> FilesPage:
        _path(path)
        if not 1 <= limit <= 1000:
            raise ValueError("Files listing limit must be between 1 and 1000")
        params = {"list": "", "prefix": path, "limit": str(limit)}
        if continuation_token is not None:
            params["continuation_token"] = continuation_token
        client = self._files._client
        async with client._session.get(
            await self._files._url(self.target, ""),
            params=params,
            headers=client._get_headers(),
            allow_redirects=False,
        ) as response:
            await self._files._check_response(response)
            return await client._read_model(response, FilesPage)

    async def list(self, path: str = "") -> AsyncIterator[StorageEntry]:
        token: str | None = None
        seen: set[str] = set()
        while True:
            page = await self.list_page(path, continuation_token=token)
            for entry in page.entries:
                yield entry
            token = page.continuation_token
            if token is None:
                return
            if token in seen:
                raise RoomException("Files server returned a repeated pagination token")
            seen.add(token)

    async def stat(self, path: str) -> FileMetadata:
        client = self._files._client
        async with client._session.head(
            await self._files._url(self.target, path),
            headers=client._get_headers(),
            allow_redirects=False,
        ) as response:
            await self._files._check_response(response)
            return FileMetadata(
                path=path,
                size=int(response.headers.get("Content-Length", "0")),
                is_folder=response.headers.get("X-Meshagent-Is-Folder") == "true",
                content_type=response.headers.get("Content-Type"),
                etag=response.headers.get("ETag"),
                last_modified=response.headers.get("Last-Modified"),
            )

    async def exists(self, path: str) -> bool:
        # Only 404 means absent; authorization and transport errors propagate.
        from meshagent.api.client import NotFoundError

        try:
            await self.stat(path)
        except NotFoundError:
            return False
        return True

    async def download_stream(
        self, path: str, *, chunk_size: int = 256 * 1024
    ) -> AsyncIterator[bytes]:
        if not 1 <= chunk_size <= 16 * 1024 * 1024:
            raise ValueError("Download chunk size must be between 1 byte and 16 MiB")
        client = self._files._client
        async with client._session.get(
            await self._files._url(self.target, path),
            headers=client._get_headers(),
            allow_redirects=False,
            auto_decompress=False,
        ) as response:
            await self._files._check_response(response)
            async for chunk in response.content.iter_chunked(chunk_size):
                yield chunk

    async def download(self, path: str) -> bytes:
        return b"".join([chunk async for chunk in self.download_stream(path)])

    async def upload(
        self,
        path: str,
        data: bytes | AsyncIterable[bytes],
        *,
        content_type: str = "application/octet-stream",
        overwrite: bool = True,
    ) -> None:
        client = self._files._client
        async with client._session.put(
            await self._files._url(self.target, path),
            params={"overwrite": str(overwrite).lower()},
            headers={**client._get_headers(), "Content-Type": content_type},
            data=data,
            allow_redirects=False,
        ) as response:
            await self._files._check_response(response)

    async def delete(self, path: str, *, recursive: bool = False) -> None:
        client = self._files._client
        async with client._session.delete(
            await self._files._url(self.target, path),
            params={"recursive": str(recursive).lower()},
            headers=client._get_headers(),
            allow_redirects=False,
        ) as response:
            await self._files._check_response(response)

    async def mkdir(self, path: str) -> None:
        await self._action(path, "mkdir", {})

    async def copy(
        self, source: FileLocation, destination: str, *, overwrite: bool = False
    ) -> None:
        await self._action(
            destination,
            "copy",
            {"source": source.model_dump(mode="json"), "overwrite": overwrite},
        )

    async def move(
        self, source: FileLocation, destination: str, *, overwrite: bool = False
    ) -> None:
        await self._action(
            destination,
            "move",
            {"source": source.model_dump(mode="json"), "overwrite": overwrite},
        )

    async def _action(self, path: str, action: str, body: dict) -> None:
        client = self._files._client
        async with client._session.post(
            await self._files._url(self.target, path),
            params={"action": action},
            headers=client._get_headers(),
            json=body,
            allow_redirects=False,
        ) as response:
            await self._files._check_response(response)

    async def presign(
        self,
        path: str,
        *,
        method: Literal["GET", "HEAD", "PUT", "DELETE"] = "GET",
        expires_in: int = 3600,
    ) -> PresignedFileUrl:
        _path(path)
        if method not in {"GET", "HEAD", "PUT", "DELETE"}:
            raise ValueError("Unsupported presigned file method")
        if not 1 <= expires_in <= 604800:
            raise ValueError(
                "Presigned URL lifetime must be between 1 second and 7 days"
            )
        client = self._files._client
        async with client._session.post(
            f"{await self._files._endpoint()}/v1/presign",
            headers=client._get_headers(),
            json={
                "target": self.target.model_dump(mode="json"),
                "path": path,
                "method": method,
                "expires_in": expires_in,
            },
            allow_redirects=False,
        ) as response:
            await self._files._check_response(response)
            return await client._read_model(response, PresignedFileUrl)
