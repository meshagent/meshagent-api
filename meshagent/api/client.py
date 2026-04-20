import aiohttp
import base64
import json
import re
from typing import Any, Dict, List, Optional, Literal, TypeVar, cast
from pydantic import (
    BaseModel,
    ValidationError,
    JsonValue,
    Field,
    ConfigDict,
    field_validator,
)
from meshagent.api import RoomException
from meshagent.api.participant_token import ApiScope, ParticipantToken
from meshagent.api.helpers import meshagent_base_url
from meshagent.api.http import new_client_session
from meshagent.api.oauth import ConnectorRef, OAuthClientConfig
from datetime import datetime
from meshagent.api.specs.service import (
    ServiceSpec,
    ServiceTemplateSpec,
)
import os


_ModelT = TypeVar("_ModelT", bound=BaseModel)

# ------------------------------------------------------------------
#  Secret models
# ------------------------------------------------------------------


class NotFoundError(RoomException):
    """404 – resource does not exist."""


class PermissionDeniedError(RoomException):
    """403 – permission denied."""


class ConflictError(RoomException):
    """409 – conflicting or duplicate resource."""


class ValidationErrorResponse(RoomException):
    """400 – invalid request payload."""


class ServerError(RoomException):
    """5xx – server-side failure."""


class OAuthClient(BaseModel):
    client_id: str
    client_secret: str
    grant_types: list[str]
    response_types: list[str]
    redirect_uris: list[str]
    scope: str
    project_id: str
    metadata: dict[str, str]
    official: bool


class RoomConnectionInfo(BaseModel):
    jwt: str
    room_name: str
    project_id: str
    room_url: str


class RoomShare(BaseModel):
    id: str
    project_id: str
    settings: dict[str, JsonValue]


class RoomSession(BaseModel):
    id: str
    room_id: Optional[str]
    room_name: str
    created_at: datetime
    is_active: bool
    participants: Optional[dict[str, int]] = None


class _ListRoomSessionsResponse(BaseModel):
    sessions: list[RoomSession]


class Room(BaseModel):
    id: str
    name: str
    metadata: dict[str, JsonValue]
    annotations: dict[str, str] = Field(default_factory=dict)


class ProjectRoomGrant(BaseModel):
    room: Room
    user_id: str
    permissions: ApiScope


class User(BaseModel):
    id: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: str


class UserRoomGrant(BaseModel):
    room: Room
    user: User
    permissions: ApiScope


class ProjectRoomGrantCount(BaseModel):
    room: Room
    count: int


class ProjectUserGrantCount(BaseModel):
    user_id: str
    count: int
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: str


class _CreateRoomGrantRequest(BaseModel):
    room_id: str
    user_id: Optional[str] = None
    email: Optional[str] = None
    permissions: ApiScope


class _UpdateRoomGrantRequest(BaseModel):
    room_id: str
    user_id: str
    permissions: ApiScope


class _BaseSecret(BaseModel):
    """Common fields shared by all secrets."""

    id: str
    name: str


class PullSecret(_BaseSecret):
    """
    A Docker-registry credential.

    When you call `model_dump() / dict()` this object produces the same
    structure consumed by `map_secret_data("docker", …)` in the room
    provisioner.
    """

    type: Literal["docker"] = "docker"

    server: str = Field(..., description="Registry host (e.g. registry-1.docker.io)")
    username: str
    password: str
    email: str = Field(
        "none@example.com",
        description="Email is required by the Docker spec, but is unused",
    )

    def to_payload(self) -> Dict[str, str]:
        return {
            "server": self.server,
            "username": self.username,
            "password": self.password,
            "email": self.email,
        }


class KeysSecret(_BaseSecret):
    """
    An *opaque* secret that will be exposed to containers as individual
    environment variables.

    Example:
        KeysSecret(
            id="sec-123",
            name="openai",
            data={"OPENAI_API_KEY": "sk-...", "ORG": "myorg"}
        )
    """

    type: Literal["keys"] = "keys"
    data: Dict[str, str]

    def to_payload(self) -> Dict[str, str]:
        return self.data


SecretLike = PullSecret | KeysSecret


class ManagedSecretInfo(BaseModel):
    id: str
    type: str
    name: str
    delegated_to: Optional[str] = None


class ManagedSecret(ManagedSecretInfo):
    data_base64: str

    @property
    def data(self) -> bytes:
        return base64.b64decode(self.data_base64.encode("ascii"))


class _ListManagedSecretsResponse(BaseModel):
    secrets: list[ManagedSecretInfo]


class ExternalOAuthClientRegistration(BaseModel):
    id: str
    delegated_to: str
    connector: Optional[ConnectorRef] = None
    oauth: Optional[OAuthClientConfig] = None
    client_id: str
    client_secret: Optional[str] = None


class _ListExternalOAuthClientRegistrationsResponse(BaseModel):
    registrations: list[ExternalOAuthClientRegistration]


def _encode_secret_bytes(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _parse_secret_payload(*, secret: ManagedSecretInfo, raw_data: bytes) -> SecretLike:
    try:
        payload = json.loads(raw_data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RoomException(f"Invalid secret payload for {secret.id}") from exc

    if not isinstance(payload, dict):
        raise RoomException(f"Invalid secret payload for {secret.id}")

    if secret.type == "docker":
        return PullSecret.model_validate(
            {
                "id": secret.id,
                "name": secret.name,
                "type": secret.type,
                **payload,
            }
        )

    return KeysSecret.model_validate(
        {
            "id": secret.id,
            "name": secret.name,
            "type": secret.type,
            "data": payload,
        }
    )


ProjectRole = Literal["member", "admin", "developer", "none"]


class _CreateMailboxRequest(BaseModel):
    room: str
    queue: str
    address: str
    public: bool
    annotations: Optional[dict[str, str]] = None


class _UpdateMailboxRequest(BaseModel):
    room: str
    queue: str
    public: bool
    annotations: Optional[dict[str, str]] = None


class Mailbox(BaseModel):
    """
    Minimal shape returned by the server for a mailbox.
    Extra fields (if any) from the server response will be ignored.
    """

    address: str
    room: str
    room_id: Optional[str] = None
    queue: str
    public: bool
    annotations: dict[str, str]


class _CreateRouteRequest(BaseModel):
    domain: str
    room_name: str
    port: str
    annotations: Optional[dict[str, str]] = None


class _UpdateRouteRequest(BaseModel):
    room_name: str
    port: str
    annotations: Optional[dict[str, str]] = None


class Route(BaseModel):
    domain: str
    room_name: str
    port: str
    annotations: dict[str, str]


_OCI_REPOSITORY_COMPONENT_RE = re.compile(r"^[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*$")


def _normalize_repository_name(value: str) -> str:
    normalized = value.strip()
    if normalized == "":
        raise ValueError("repository name must not be empty")

    parts = normalized.split("/")
    if any(
        part == "" or _OCI_REPOSITORY_COMPONENT_RE.fullmatch(part) is None
        for part in parts
    ):
        raise ValueError("repository name must be a valid OCI repository path")

    return normalized


class _RepositoryRequestBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    description: str = ""
    annotations: dict[str, str] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return _normalize_repository_name(value)


class CreateProjectRepositoryRequest(_RepositoryRequestBase):
    pass


class UpdateProjectRepositoryRequest(_RepositoryRequestBase):
    pass


# Backward-compatible aliases for older internal imports.
_CreateProjectRepositoryRequest = CreateProjectRepositoryRequest
_UpdateProjectRepositoryRequest = UpdateProjectRepositoryRequest


class ProjectRepository(BaseModel):
    id: str
    project_id: str
    name: str
    description: str = ""
    annotations: dict[str, str] = Field(default_factory=dict)
    created_at: datetime

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return _normalize_repository_name(value)

    @property
    def storage_prefix(self) -> str:
        return self.name


class ProjectInfo(BaseModel):
    id: str
    owner_user_id: str | None = Field(default=None, alias="owner_user_id")
    name: str
    project_key: str
    created_at: datetime | None = Field(default=None, alias="created_at")
    settings: dict[str, JsonValue] | None = None

    model_config = ConfigDict(populate_by_name=True)


class CreateRepositoryTokenRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    actions: list[Literal["pull", "push"]] = Field(default_factory=lambda: ["pull"])
    expires_in_seconds: int | None = None

    @field_validator("actions")
    @classmethod
    def validate_actions(cls, value: list[Literal["pull", "push"]]) -> list[str]:
        normalized_actions: list[str] = []
        seen: set[str] = set()
        for action in value:
            normalized_action = action.strip()
            if normalized_action not in {"pull", "push"}:
                raise ValueError(f"unsupported repository token action: {action}")
            if normalized_action not in seen:
                seen.add(normalized_action)
                normalized_actions.append(normalized_action)
        if len(normalized_actions) == 0:
            raise ValueError("repository token actions must not be empty")
        return normalized_actions

    @field_validator("expires_in_seconds")
    @classmethod
    def validate_expires_in_seconds(cls, value: int | None) -> int | None:
        if value is not None and value <= 0:
            raise ValueError("expires_in_seconds must be greater than zero")
        return value


class RepositoryToken(BaseModel):
    token: str
    expires_at: datetime


class Balance(BaseModel):
    balance: float
    auto_recharge_threshold: Optional[float] = Field(
        default=None, alias="auto_recharge_threshold"
    )
    auto_recharge_amount: Optional[float] = Field(
        default=None, alias="auto_recharge_amount"
    )
    last_recharge: Optional[datetime] = Field(default=None, alias="last_recharge")

    model_config = ConfigDict(populate_by_name=True)


class ProjectStatus(BaseModel):
    enabled: bool


class Transaction(BaseModel):
    id: str
    amount: float
    reference: Optional[str] = None
    reference_type: Optional[str] = Field(default=None, alias="referenceType")
    description: str
    created_at: datetime = Field(alias="created_at")

    model_config = ConfigDict(populate_by_name=True)


class ScheduledTask(BaseModel):
    id: str
    project_id: str
    room_name: str
    queue_name: str
    payload: dict
    schedule: str
    active: bool
    once: bool
    annotations: dict[str, str]

    room_id: Optional[str] = None
    last_run_id: Optional[int] = None
    last_start_time: Optional[datetime] = None
    last_end_time: Optional[datetime] = None
    last_status: Optional[str] = None
    last_return_message: Optional[str] = None


class _CreateScheduledTaskRequest(BaseModel):
    id: Optional[str] = None
    room_name: str
    queue_name: str
    payload: dict  # dict or json-string
    schedule: str
    active: bool = True
    once: bool = False
    annotations: dict[str, str]


class _UpdateScheduledTaskRequest(BaseModel):
    room_name: Optional[str] = None
    queue_name: Optional[str] = None
    payload: Optional[dict] = None  # dict or json-string
    schedule: Optional[str] = None
    active: Optional[bool] = None
    annotations: Optional[dict[str, str]] = None


class _ListScheduledTasksResponse(BaseModel):
    tasks: List[ScheduledTask]


class Meshagent:
    """
    A simple asynchronous client to interact with the accounts routes.
    """

    def __init__(
        self,
        *,
        base_url: str = meshagent_base_url(),
        token: str = os.getenv("MESHAGENT_API_KEY"),
        session: aiohttp.ClientSession | None = None,
    ):
        """
        :param base_url: The root URL of your server, e.g. 'http://localhost:8080'.
        :param token: A Bearer token for the Authorization header.
        """
        self.base_url = base_url.rstrip("/")
        self.token = token  # The "Bearer" token
        self._session_external = session is not None
        self._session = session or new_client_session()

    async def close(self):
        if not self._session_external and not self._session.closed:
            await self._session.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    def _get_headers(self) -> Dict[str, str]:
        """
        Returns the default headers including Bearer Authorization.
        """
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    async def _raise_for_status(
        self,
        resp: aiohttp.ClientResponse,
    ) -> None:
        if resp.status < 400:
            return

        try:
            body = await resp.text()
        except Exception:
            body = "<unable to read body>"

        msg = f"Status={resp.status}, body={body}"

        if resp.status == 404:
            raise NotFoundError(msg)
        if resp.status == 403:
            raise PermissionDeniedError(msg)
        if resp.status == 409:
            raise ConflictError(msg)
        if resp.status == 400:
            raise ValidationErrorResponse(msg)
        if resp.status >= 500:
            raise ServerError(msg)

        raise RoomException(msg)

    async def _ensure_success(
        self, resp: aiohttp.ClientResponse, *, action: str
    ) -> None:
        if resp.status < 400:
            return
        try:
            body = await resp.text()
        except Exception:
            body = "<unable to read body>"
        raise RoomException(
            f"Failed to {action}. Status code: {resp.status}, body: {body}"
        )

    async def _read_model(
        self,
        resp: aiohttp.ClientResponse,
        model_type: type[_ModelT],
    ) -> _ModelT:
        payload = await resp.json()
        try:
            return model_type.model_validate(payload)
        except ValidationError as exc:
            raise RoomException(
                f"Invalid {model_type.__name__} payload: {exc}"
            ) from exc

    async def upload(self, *, project_id: str, path: str, data: bytes) -> None:
        """Upload a file to project storage.

        Corresponds to: **POST /projects/:project_id/storage/upload**
        Query params: `path`
        Body: raw binary data (bytes)
        Raises RoomException on HTTP >= 400.
        """
        url = f"{self.base_url}/projects/{project_id}/storage/upload"
        params = {"path": path}

        async with self._session.post(
            url,
            params=params,
            headers={**self._get_headers(), "Content-Type": "application/octet-stream"},
            data=data,
        ) as resp:
            if resp.status >= 400:
                body = await resp.text()
                raise RoomException(
                    f"Failed to upload file. Status code: {resp.status}, body: {body}"
                )

    async def download(self, *, project_id: str, path: str) -> bytes:
        """Download a file from project storage.

        Corresponds to: **POST /projects/:project_id/storage/download** (HTTP GET in client)
        Query params: `path`
        Returns raw bytes of the file.
        Raises NotFoundException for 404, RoomException for other HTTP errors.
        """
        url = f"{self.base_url}/projects/{project_id}/storage/download"
        params = {"path": path}

        async with self._session.get(
            url, params=params, headers=self._get_headers()
        ) as resp:
            if resp.status == 404:
                raise RoomException("file was not found")
            if resp.status >= 400:
                body = await resp.text()
                raise RoomException(
                    f"Failed to download file. Status code: {resp.status}, body: {body}"
                )
            return await resp.read()

    async def get_project_role(self, project_id: str) -> ProjectRole:
        """
        Corresponds to: GET /accounts/projects/{id}/role
        Returns a JSON dict with { "role" : "member" | "admin" } on success.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/role"

        async with self._session.get(
            url,
            headers=self._get_headers(),
        ) as resp:
            await self._raise_for_status(resp)
            payload = await resp.json()
            return cast(ProjectRole, payload["role"])

    async def create_share(
        self, project_id: str, settings: Optional[dict] = None
    ) -> Dict[str, Any]:
        """
        Corresponds to: POST /accounts/projects
        Body: { "name": "<name>" }
        Returns a JSON dict with { "id" } on success.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/shares"

        payload = {"settings": settings or {}}

        async with self._session.post(
            url,
            headers=self._get_headers(),
            json=payload,
        ) as resp:
            await self._raise_for_status(resp)
            return await resp.json()

    async def delete_share(self, project_id: str, share_id: str) -> None:
        """
        Corresponds to: DELETE /accounts/projects/:id/shares/:share_id
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/shares/{share_id}"

        async with self._session.delete(
            url,
            headers=self._get_headers(),
        ) as resp:
            await self._raise_for_status(resp)
            return None

    async def update_share(
        self, project_id: str, share_id: str, settings: Optional[dict] = None
    ) -> None:
        """
        Corresponds to: PUT /accounts/projects/:id/shares/:share_id
        Body: { "settings" }
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/shares/{share_id}"

        payload = {"settings": settings or {}}

        async with self._session.put(
            url,
            headers=self._get_headers(),
            json=payload,
        ) as resp:
            await self._raise_for_status(resp)
            return None

    async def list_shares(self, project_id: str) -> list[RoomShare]:
        """
        Corresponds to: GET /accounts/projects/:id/shares
        Returns a JSON dict with { "shares" : [{ "id", "settings" }] } on success.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/shares"

        async with self._session.get(
            url,
            headers=self._get_headers(),
        ) as resp:
            await self._raise_for_status(resp)
            data = await resp.json()
            try:
                return [RoomShare.model_validate(item) for item in data["shares"]]
            except (KeyError, ValidationError) as exc:
                raise RoomException(f"Invalid shares payload: {exc}") from exc

    async def create_project(
        self, name: str, settings: Optional[dict] = None
    ) -> Dict[str, Any]:
        """
        Corresponds to: POST /accounts/projects
        Body: { "name": "<name>" }
        Returns a JSON dict with { "id", "owner_user_id", "name", "project_key" } on success.
        """
        url = f"{self.base_url}/accounts/projects"

        async with self._session.post(
            url,
            headers=self._get_headers(),
            json={"name": name, "settings": settings},
        ) as resp:
            await self._raise_for_status(resp)
            return await resp.json()

    async def add_user_to_project(
        self,
        project_id: str,
        user_id: str,
        is_admin: bool | None = None,
        is_developer: bool | None = None,
        can_create_rooms: bool | None = None,
        can_use_llm_proxy: bool | None = None,
    ) -> Dict[str, Any]:
        """
        Corresponds to: POST /accounts/projects/:id/users
        Body: { "project_id", "user_id" }
        Returns a JSON dict with { "ok": True } on success.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/users"
        body = {
            "project_id": project_id,
            "user_id": user_id,
            **({"is_admin": is_admin} if is_admin is not None else {}),
            **({"is_developer": is_developer} if is_developer is not None else {}),
            **(
                {"can_create_rooms": can_create_rooms}
                if can_create_rooms is not None
                else {}
            ),
            **(
                {"can_use_llm_proxy": can_use_llm_proxy}
                if can_use_llm_proxy is not None
                else {}
            ),
        }
        async with self._session.post(
            url, headers=self._get_headers(), json=body
        ) as resp:
            await self._raise_for_status(resp)
            return await resp.json()

    async def remove_user_from_project(
        self, project_id: str, user_id: str
    ) -> Dict[str, Any]:
        """
        Corresponds to: DELETE /accounts/projects/:project_id/users/:user_id
        Returns a JSON dict with { "ok": True } on success.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/users/{user_id}"

        async with self._session.delete(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            return await resp.json()

    async def update_project_user(
        self,
        project_id: str,
        user_id: str,
        *,
        is_admin: bool,
        is_developer: bool,
        can_create_rooms: bool,
        can_use_llm_proxy: bool,
    ) -> Dict[str, Any]:
        """
        Corresponds to: PUT /accounts/projects/:project_id/users/:user_id
        Body: { "is_admin", "is_developer", "can_create_rooms", "can_use_llm_proxy" }
        Returns a JSON dict with { "ok": True } on success.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/users/{user_id}"
        body = {
            "is_admin": is_admin,
            "is_developer": is_developer,
            "can_create_rooms": can_create_rooms,
            "can_use_llm_proxy": can_use_llm_proxy,
        }
        async with self._session.put(
            url, headers=self._get_headers(), json=body
        ) as resp:
            await self._raise_for_status(resp)
            return await resp.json()

    async def update_project_settings(
        self, project_id: str, settings: dict
    ) -> Dict[str, Any]:
        """
        Corresponds to: PUT /accounts/projects/:id/settings
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/settings"

        async with self._session.put(
            url, headers=self._get_headers(), json=settings
        ) as resp:
            await self._raise_for_status(resp)
            return await resp.json()

    async def get_users_in_project(self, project_id: str) -> Dict[str, Any]:
        """
        Corresponds to: GET /accounts/projects/:id/users
        Returns a JSON dict with { "users": [...] }.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/users"

        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            return await resp.json()

    async def get_user_profile(self, user_id: str) -> Dict[str, Any]:
        """
        Corresponds to: GET /accounts/profiles/:id
        Returns the user profile JSON, e.g. { "id", "first_name", "last_name", "email" } or raises 404 if not found.
        """
        url = f"{self.base_url}/accounts/profiles/{user_id}"

        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            return await resp.json()

    async def update_user_profile(
        self, user_id: str, first_name: str, last_name: str
    ) -> Dict[str, Any]:
        """
        Corresponds to: PUT /accounts/profiles/:id
        Body: { "first_name", "last_name" }
        Returns a JSON dict with { "ok": True } on success.
        """
        url = f"{self.base_url}/accounts/profiles/{user_id}"
        body = {"first_name": first_name, "last_name": last_name}

        async with self._session.put(
            url, headers=self._get_headers(), json=body
        ) as resp:
            await self._raise_for_status(resp)
            return await resp.json()

    async def list_projects(self) -> Dict[str, Any]:
        """
        Corresponds to: GET /accounts/projects
        Returns a JSON dict with { "projects": [...] }.
        """
        url = f"{self.base_url}/accounts/projects"

        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            return await resp.json()

    async def get_project(self, project_id: str) -> Dict[str, Any]:
        """
        Corresponds to: GET /accounts/projects
        Returns a JSON dict with { "projects": [...] }.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}"

        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            return await resp.json()

    async def get_project_info(self, project_id: str) -> ProjectInfo:
        url = f"{self.base_url}/accounts/projects/{project_id}"

        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            return await self._read_model(resp, ProjectInfo)

    async def create_api_key(
        self, project_id: str, name: str, description: str
    ) -> Dict[str, Any]:
        """
        Corresponds to: POST /accounts/projects/{project_id}/api-keys
        Body: { "name": "<>", "description": "<>" }
        Returns a JSON dict with { "id", "name", "description", "value" }.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/api-keys"
        payload = {"name": name, "description": description}

        async with self._session.post(
            url, headers=self._get_headers(), json=payload
        ) as resp:
            await self._raise_for_status(resp)
            return await resp.json()

    async def get_pricing(self) -> Dict[str, Any]:
        """GET /pricing"""
        url = f"{self.base_url}/pricing"
        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._ensure_success(resp, action="fetch pricing data")
            return await resp.json()

    async def get_project_status(self, project_id: str) -> ProjectStatus:
        """GET /accounts/projects/{project_id}/status"""
        url = f"{self.base_url}/accounts/projects/{project_id}/status"
        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._ensure_success(resp, action="fetch project status")
            data = await resp.json()
            try:
                return ProjectStatus.model_validate(data)
            except ValidationError as exc:
                raise RoomException(f"Invalid project status payload: {exc}") from exc

    async def get_balance(self, project_id: str) -> Balance:
        """GET /accounts/projects/{project_id}/balance"""
        url = f"{self.base_url}/accounts/projects/{project_id}/balance"
        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._ensure_success(resp, action="fetch balance")
            data = await resp.json()
            try:
                return Balance.model_validate(data)
            except ValidationError as exc:
                raise RoomException(f"Invalid balance payload: {exc}") from exc

    async def get_recent_transactions(self, project_id: str) -> List[Transaction]:
        """GET /accounts/projects/{project_id}/transactions"""
        url = f"{self.base_url}/accounts/projects/{project_id}/transactions"
        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._ensure_success(resp, action="fetch transactions")
            data = await resp.json()
            transactions = data.get("transactions", [])
            if not isinstance(transactions, list):
                raise RoomException(
                    "Invalid transactions payload: expected 'transactions' list"
                )
            try:
                return [Transaction.model_validate(item) for item in transactions]
            except ValidationError as exc:
                raise RoomException(f"Invalid transaction payload: {exc}") from exc

    async def set_auto_recharge(
        self,
        *,
        project_id: str,
        enabled: bool,
        amount: float,
        threshold: float,
    ) -> None:
        """POST /accounts/projects/{project_id}/recharge"""
        url = f"{self.base_url}/accounts/projects/{project_id}/recharge"
        payload = {
            "enabled": enabled,
            "amount": amount,
            "threshold": threshold,
        }
        async with self._session.post(
            url, headers=self._get_headers(), json=payload
        ) as resp:
            await self._ensure_success(resp, action="update auto recharge settings")

    async def get_checkout_url(
        self,
        project_id: str,
        *,
        success_url: str,
        cancel_url: str,
    ) -> str:
        """POST /accounts/projects/{project_id}/subscription"""
        url = f"{self.base_url}/accounts/projects/{project_id}/subscription"
        payload = {"success_url": success_url, "cancel_url": cancel_url}
        async with self._session.post(
            url, headers=self._get_headers(), json=payload
        ) as resp:
            await self._ensure_success(resp, action="create subscription checkout")
            data = await resp.json()
            checkout_url = data.get("checkout_url")
            if not isinstance(checkout_url, str):
                raise RoomException(
                    "Invalid subscription payload: expected 'checkout_url' string"
                )
            return checkout_url

    async def get_credits_checkout_url(
        self,
        project_id: str,
        *,
        success_url: str,
        cancel_url: str,
        quantity: float,
    ) -> str:
        """POST /accounts/projects/{project_id}/credits"""
        url = f"{self.base_url}/accounts/projects/{project_id}/credits"
        payload = {
            "quantity": quantity,
            "success_url": success_url,
            "cancel_url": cancel_url,
        }
        async with self._session.post(
            url, headers=self._get_headers(), json=payload
        ) as resp:
            await self._ensure_success(resp, action="create credits checkout")
            data = await resp.json()
            checkout_url = data.get("checkout_url")
            if not isinstance(checkout_url, str):
                raise RoomException(
                    "Invalid credits payload: expected 'checkout_url' string"
                )
            return checkout_url

    async def get_subscription(self, project_id: str) -> Dict[str, Any]:
        """GET /accounts/projects/{project_id}/subscription"""
        url = f"{self.base_url}/accounts/projects/{project_id}/subscription"
        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._ensure_success(resp, action="fetch subscription")
            return await resp.json()

    async def get_usage(
        self,
        project_id: str,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        interval: Optional[str] = None,
        report: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Corresponds to: GET /accounts/projects/{project_id}/usage
        Allows filtering using optional start/end timestamps, interval, and report name.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/usage"
        params: Dict[str, str] = {}
        if start is not None:
            params["start"] = start.isoformat()
        if end is not None:
            params["end"] = end.isoformat()
        if interval is not None:
            params["interval"] = interval
        if report is not None:
            params["report"] = report

        async with self._session.get(
            url,
            headers=self._get_headers(),
            params=params or None,
        ) as resp:
            await self._ensure_success(resp, action="retrieve usage")
            data = await resp.json()
            usage = data.get("usage", [])
            if not isinstance(usage, list):
                raise RoomException(
                    "Invalid usage payload: expected 'usage' to be a list"
                )
            return [item for item in usage if isinstance(item, dict)]

    async def delete_api_key(self, project_id: str, id: str) -> None:
        """
        Corresponds to: DELETE /accounts/projects/{project_id}/api-keys/{token_id}
        Returns 204 No Content on success (no JSON body).
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/api-keys/{id}"

        async with self._session.delete(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            # The server returns status 204 with no content, so no need to parse JSON.

    async def list_api_keys(self, project_id: str) -> Dict[str, Any]:
        """
        Corresponds to: GET /accounts/projects/{project_id}/api-keys
        Returns a JSON dict like: { "tokens": [ { ... }, ... ] }.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/api-keys"

        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            return await resp.json()

    async def get_session(self, project_id: str, session_id: str) -> Dict[str, Any]:
        """
        Corresponds to: GET /accounts/projects/{project_id}/sessions/{session_id}
        Returns a JSON dict: { "id", "room_name", "created_at }
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/sessions/{session_id}"

        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            return await resp.json()

    async def list_active_sessions(self, project_id: str) -> list[RoomSession]:
        """
        Corresponds to: GET /accounts/projects/{project_id}/sessions
        Returns a JSON dict: { "sessions": [...] }
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/sessions/active"

        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            data = await resp.json()
            sessions = data.get("sessions", [])
            return [RoomSession.model_validate(session) for session in sessions]

    async def list_recent_sessions(
        self,
        project_id: str,
        *,
        limit: int = 25,
        room_id: Optional[str] = None,
    ) -> list[RoomSession]:
        """
        Corresponds to: GET /accounts/projects/{project_id}/sessions
        Returns a JSON dict: { "sessions": [...] }
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/sessions"
        params: dict[str, str] = {"limit": str(limit)}
        if room_id is not None:
            params["room_id"] = room_id

        async with self._session.get(
            url, headers=self._get_headers(), params=params
        ) as resp:
            await self._raise_for_status(resp)
            data = await resp.json()
            sessions = data.get("sessions", [])
            return [RoomSession.model_validate(session) for session in sessions]

    async def list_session_events(
        self, project_id: str, session_id: str
    ) -> list[Dict[str, Any]]:
        """
        Corresponds to: GET /accounts/projects/{project_id}/sessions/{session_id}/events
        Returns a JSON dict: { "events": [...] }
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/sessions/{session_id}/events"

        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            data = await resp.json()
            return data.get("events", [])

    async def terminate(self, project_id: str, session_id: str) -> None:
        """
        Corresponds to: POST /accounts/projects/{project_id}/sessions/{session_id}/terminate
        Returns 204 No Content on success.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/sessions/{session_id}/terminate"

        async with self._session.post(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)

    async def list_session_spans(
        self, project_id: str, session_id: str
    ) -> list[Dict[str, Any]]:
        """
        Corresponds to: GET /accounts/projects/{project_id}/sessions/{session_id}/spans
        Returns a JSON dict: { "spans": [...] }
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/sessions/{session_id}/spans"

        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            data = await resp.json()
            return data.get("spans", [])

    async def list_session_metrics(
        self, project_id: str, session_id: str
    ) -> list[Dict[str, Any]]:
        """
        Corresponds to: GET /accounts/projects/{project_id}/sessions/{session_id}/metrics
        Returns a JSON dict: { "metrics": [...] }
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/sessions/{session_id}/metrics"

        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            data = await resp.json()
            return data.get("metrics", [])

    async def create_webhook(
        self,
        project_id: str,
        name: str,
        url: str,
        events: List[str],
        description: str = "",
        action: Optional[str] = "",
        payload: Optional[dict] = "",
    ) -> Dict[str, Any]:
        """
        Corresponds to: POST /accounts/projects/{project_id}/webhooks
        Body: { "name", "description", "url", "events" }
        The server might generate an internal webhook_id (or retrieve it from the request).
        Returns whatever JSON object the server responds with (likely empty or your new resource data).
        """
        endpoint = f"{self.base_url}/accounts/projects/{project_id}/webhooks"
        payload = {
            "name": name,
            "description": description,
            "url": url,
            "events": events,
            "action": action,
            "payload": payload,
        }

        async with self._session.post(
            endpoint, headers=self._get_headers(), json=payload
        ) as resp:
            await self._raise_for_status(resp)
            # If the server returns JSON with newly created webhook info, parse it:
            return await resp.json()

    async def update_webhook(
        self,
        project_id: str,
        webhook_id: str,
        name: str,
        url: str,
        events: List[str],
        description: str = "",
        action: Optional[str] = None,
        payload: Optional[dict] = None,
        secret: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Corresponds to: PUT /accounts/projects/{project_id}/webhooks/{webhook_id}
        Body: { "name", "description", "url", "events" }
        Returns JSON (could be the updated resource or an empty dict).
        """
        endpoint = (
            f"{self.base_url}/accounts/projects/{project_id}/webhooks/{webhook_id}"
        )
        payload = {
            "name": name,
            "description": description,
            "url": url,
            "events": events,
            "action": action,
            "payload": payload,
            "secret": secret,
        }

        async with self._session.put(
            endpoint, headers=self._get_headers(), json=payload
        ) as resp:
            await self._raise_for_status(resp)
            return await resp.json()

    async def list_webhooks(self, project_id: str) -> Dict[str, Any]:
        """
        Corresponds to: GET /accounts/projects/{project_id}/webhooks
        Returns a JSON dict like: { "webhooks": [ { ... }, ... ] }
        """
        endpoint = f"{self.base_url}/accounts/projects/{project_id}/webhooks"

        async with self._session.get(endpoint, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            return await resp.json()

    async def delete_webhook(self, project_id: str, webhook_id: str) -> None:
        """
        Corresponds to: DELETE /accounts/projects/{project_id}/webhooks/{webhook_id}
        Typically returns 200 or 204 on success (no JSON body).
        """
        endpoint = (
            f"{self.base_url}/accounts/projects/{project_id}/webhooks/{webhook_id}"
        )

        async with self._session.delete(endpoint, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)

    async def create_mailbox(
        self,
        *,
        project_id: str,
        address: str,
        room: str,
        queue: str,
        public: bool = False,
        annotations: Optional[dict[str, str]] = None,
    ) -> None:
        """
        POST /accounts/projects/{project_id}/mailboxes
        Body: { "address", "room", "queue" }
        Returns {} on success.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/mailboxes"
        payload = _CreateMailboxRequest(
            address=address,
            room=room,
            queue=queue,
            public=public,
            annotations=annotations,
        ).model_dump(mode="json")
        async with self._session.post(
            url, headers=self._get_headers(), json=payload
        ) as resp:
            await self._raise_for_status(resp)

    async def update_mailbox(
        self,
        *,
        project_id: str,
        address: str,
        room: str,
        queue: str,
        public: bool = False,
        annotations: Optional[dict[str, str]] = None,
    ) -> None:
        """
        PUT /accounts/projects/{project_id}/mailboxes/{address}
        Body: { "room", "queue" }
        Returns {} on success.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/mailboxes/{address}"
        payload = _UpdateMailboxRequest(
            room=room,
            queue=queue,
            public=public,
            annotations=annotations,
        ).model_dump(mode="json")
        async with self._session.put(
            url, headers=self._get_headers(), json=payload
        ) as resp:
            await self._raise_for_status(resp)

    async def get_mailbox(self, *, project_id: str, address: str) -> Mailbox:
        """
        GET /accounts/projects/{project_id}/mailboxes/{address}
        Returns a list[Mailbox].
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/mailboxes/{address}"
        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            return Mailbox.model_validate((await resp.json())["mailbox"])

    async def list_room_mailboxes(
        self, *, project_id: str, room_name: str
    ) -> List[Mailbox]:
        """
        GET /accounts/projects/{project_id}/mailboxes
        Returns a list[Mailbox].
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/rooms/{room_name}/mailboxes"
        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            data = await resp.json()
            try:
                return [Mailbox.model_validate(item) for item in data["mailboxes"]]
            except ValidationError as exc:
                raise RoomException(f"Invalid mailboxes payload: {exc}") from exc

    async def list_mailboxes(self, *, project_id: str) -> List[Mailbox]:
        """
        GET /accounts/projects/{project_id}/mailboxes
        Returns a list[Mailbox].
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/mailboxes"
        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            data = await resp.json()
            try:
                return [Mailbox.model_validate(item) for item in data["mailboxes"]]
            except ValidationError as exc:
                raise RoomException(f"Invalid mailboxes payload: {exc}") from exc

    async def delete_mailbox(self, *, project_id: str, address: str) -> None:
        """
        DELETE /accounts/projects/{project_id}/mailboxes/{address}
        Returns {} on success.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/mailboxes/{address}"
        async with self._session.delete(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)

    async def create_route(
        self,
        *,
        project_id: str,
        domain: str,
        room_name: str,
        port: str,
        annotations: Optional[dict[str, str]] = None,
    ) -> None:
        """
        POST /accounts/projects/{project_id}/routes
        Body: { "domain", "room_name" }
        Returns {} on success.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/routes"
        payload = _CreateRouteRequest(
            domain=domain, room_name=room_name, port=port, annotations=annotations
        ).model_dump(mode="json")
        async with self._session.post(
            url, headers=self._get_headers(), json=payload
        ) as resp:
            await self._raise_for_status(resp)

    async def update_route(
        self,
        *,
        project_id: str,
        domain: str,
        room_name: str,
        port: str,
        annotations: Optional[dict[str, str]] = None,
    ) -> None:
        """
        PUT /accounts/projects/{project_id}/routes/{domain}
        Body: { "room_name" }
        Returns {} on success.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/routes/{domain}"
        payload = _UpdateRouteRequest(
            room_name=room_name,
            port=port,
            annotations=annotations,
        ).model_dump(mode="json")
        async with self._session.put(
            url, headers=self._get_headers(), json=payload
        ) as resp:
            await self._raise_for_status(resp)

    async def get_route(self, *, project_id: str, domain: str) -> Route:
        """
        GET /accounts/projects/{project_id}/routes/{domain}
        Returns a Route.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/routes/{domain}"
        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            return Route.model_validate((await resp.json())["route"])

    async def list_room_routes(self, *, project_id: str, room_name: str) -> List[Route]:
        """
        GET /accounts/projects/{project_id}/rooms/{room_name}/routes
        Returns a list[Route].
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/rooms/{room_name}/routes"
        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            data = await resp.json()
            try:
                return [Route.model_validate(item) for item in data["routes"]]
            except ValidationError as exc:
                raise RoomException(f"Invalid routes payload: {exc}") from exc

    async def list_routes(self, *, project_id: str) -> List[Route]:
        """
        GET /accounts/projects/{project_id}/routes
        Returns a list[Route].
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/routes"
        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            data = await resp.json()
            try:
                return [Route.model_validate(item) for item in data["routes"]]
            except ValidationError as exc:
                raise RoomException(f"Invalid routes payload: {exc}") from exc

    async def delete_route(self, *, project_id: str, domain: str) -> None:
        """
        DELETE /accounts/projects/{project_id}/routes/{domain}
        Returns {} on success.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/routes/{domain}"
        async with self._session.delete(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)

    async def render_template(
        self,
        *,
        template: str,
        values: dict[str, str],
    ) -> ServiceTemplateSpec:
        """
        POST /templates/render
        Body: full service spec, e.g.
          {
            "template" : ""
            "values": {...}
          }
        Returns: {}
        """
        url = f"{self.base_url}/templates/render"
        async with self._session.post(
            url,
            headers=self._get_headers(),
            json={
                "template": template,
                "values": values,
            },
        ) as resp:
            await self._raise_for_status(resp)
            return await self._read_model(resp, ServiceTemplateSpec)

    async def discover_mcp_spec(
        self,
        *,
        url: str,
        format: Literal["service", "template"] = "service",
    ) -> ServiceSpec | ServiceTemplateSpec:
        """
        POST /mcp/discover
        Body:
          {
            "url": "https://.../mcp",
            "format": "service" | "template"
          }
        Returns: ServiceSpec when format="service", ServiceTemplateSpec when
        format="template".
        """
        if format not in ("service", "template"):
            raise RoomException("format must be 'service' or 'template'")

        url_path = f"{self.base_url}/mcp/discover"
        async with self._session.post(
            url_path,
            headers=self._get_headers(),
            json={"url": url, "format": format},
        ) as resp:
            await self._raise_for_status(resp)
            payload = await resp.json()
            if format == "service":
                return ServiceSpec.model_validate(payload)
            return ServiceTemplateSpec.model_validate(payload)

    async def discover_mcp_service(self, *, url: str) -> ServiceSpec:
        """
        Discover an MCP server and return a generated Service spec.
        """
        result = await self.discover_mcp_spec(url=url, format="service")
        if not isinstance(result, ServiceSpec):
            raise RoomException("Unexpected response type from /mcp/discover")
        return result

    async def discover_mcp_service_template(self, *, url: str) -> ServiceTemplateSpec:
        """
        Discover an MCP server and return a generated ServiceTemplate spec.
        """
        result = await self.discover_mcp_spec(url=url, format="template")
        if not isinstance(result, ServiceTemplateSpec):
            raise RoomException("Unexpected response type from /mcp/discover")
        return result

    async def create_service(
        self,
        *,
        project_id: str,
        service: ServiceSpec,
    ) -> ServiceSpec:
        """
        POST /accounts/projects/{project_id}/services
        Body: full service spec, e.g.
          {
            "name": "...",
            "image": "...",
            "pull_secret": "...",
            "environment": {...},
            "environment_secrets": [...],
            "runtime_secrets": {...},
            "command": "...",
            "ports": {...}
          }
        Returns: created service spec
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/services"
        async with self._session.post(
            url,
            headers=self._get_headers(),
            json=service.model_dump(mode="json"),
        ) as resp:
            await self._raise_for_status(resp)
            return await self._read_model(resp, ServiceSpec)

    async def update_service(
        self,
        *,
        project_id: str,
        service_id: str,
        service: ServiceSpec,
    ) -> ServiceSpec:
        """
        PUT /accounts/projects/{project_id}/services/{service_id}
        Body: same structure as create_service (fields you wish to change).
        Returns: updated service spec.
        """

        if service.id is None:
            raise RoomException("Service id must be set")

        url = f"{self.base_url}/accounts/projects/{project_id}/services/{service_id}"
        async with self._session.put(
            url, headers=self._get_headers(), json=service.model_dump(mode="json")
        ) as resp:
            await self._raise_for_status(resp)
            return await self._read_model(resp, ServiceSpec)

    async def create_service_from_template(
        self, *, project_id: str, template: str, values: Dict[str, str]
    ) -> ServiceSpec:
        """
        POST /accounts/projects/{project_id}/services
        Body: full service spec, e.g.
          {
            "name": "...",
            "image": "...",
            "pull_secret": "...",
            "environment": {...},
            "environment_secrets": [...],
            "runtime_secrets": {...},
            "command": "...",
            "ports": {...}
          }
        Returns: created service spec
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/services"
        async with self._session.post(
            url,
            headers=self._get_headers(),
            json={
                "template": template,
                "values": values,
            },
        ) as resp:
            await self._raise_for_status(resp)
            return await self._read_model(resp, ServiceSpec)

    async def update_service_from_template(
        self,
        *,
        project_id: str,
        service_id: str,
        template: str,
        values: Dict[str, str],
    ) -> ServiceSpec:
        """
        PUT /accounts/projects/{project_id}/services/{service_id}
        Body: same structure as create_service (fields you wish to change).
        Returns: updated service spec.
        """

        url = f"{self.base_url}/accounts/projects/{project_id}/services/{service_id}"
        async with self._session.put(
            url,
            headers=self._get_headers(),
            json={
                "template": template,
                "values": values,
            },
        ) as resp:
            await self._raise_for_status(resp)
            return await self._read_model(resp, ServiceSpec)

    async def get_service(self, *, project_id: str, service_id: str) -> ServiceSpec:
        """
        GET /accounts/projects/{project_id}/services/{service_id}
        Returns a `Service` instance.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/services/{service_id}"
        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            # Handler returns a JSON string, so we read text then validate
            raw = await resp.text()
            try:
                return ServiceSpec.model_validate_json(raw)
            except ValidationError as exc:
                raise RoomException(f"Invalid service payload: {exc}") from exc

    async def list_services(self, *, project_id: str) -> List[ServiceSpec]:
        """
        GET /accounts/projects/{project_id}/services
        Returns a list of `Service` instances.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/services"
        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            data = await resp.json()
            try:
                return [ServiceSpec.model_validate(item) for item in data["services"]]
            except ValidationError as exc:
                raise RoomException(f"Invalid services payload: {exc}") from exc

    async def delete_service(self, *, project_id: str, service_id: str) -> None:
        """
        DELETE /accounts/projects/{project_id}/services/{service_id}
        Returns nothing on success.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/services/{service_id}"
        async with self._session.delete(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)

    async def create_room_service(
        self,
        *,
        project_id: str,
        room_name: str,
        service: ServiceSpec,
    ) -> str:
        """
        POST /accounts/projects/{project_id}/services
        Body: full service spec, e.g.
          {
            "name": "...",
            "image": "...",
            "pull_secret": "...",
            "environment": {...},
            "environment_secrets": [...],
            "runtime_secrets": {...},
            "command": "...",
            "ports": {...}
          }
        Returns: { "id": "<service_id>" }
        """
        url = (
            f"{self.base_url}/accounts/projects/{project_id}/rooms/{room_name}/services"
        )
        async with self._session.post(
            url,
            headers=self._get_headers(),
            json=service.model_dump(mode="json"),
        ) as resp:
            await self._raise_for_status(resp)
            return (await resp.json())["id"]

    async def update_room_service(
        self,
        *,
        project_id: str,
        room_name: str,
        service_id: str,
        service: ServiceSpec,
    ) -> None:
        """
        PUT /accounts/projects/{project_id}/services/{service_id}
        Body: same structure as create_service (fields you wish to change).
        Returns: {} on success.
        """

        url = f"{self.base_url}/accounts/projects/{project_id}/rooms/{room_name}/services/{service_id}"
        async with self._session.put(
            url, headers=self._get_headers(), json=service.model_dump(mode="json")
        ) as resp:
            await self._raise_for_status(resp)
            await resp.json()

    async def create_room_service_from_template(
        self,
        *,
        project_id: str,
        room_name: str,
        template: str,
        values: Dict[str, str],
    ) -> ServiceSpec:
        """
        POST /accounts/projects/{project_id}/services
        Body: full service spec, e.g.
          {
            "name": "...",
            "image": "...",
            "pull_secret": "...",
            "environment": {...},
            "environment_secrets": [...],
            "runtime_secrets": {...},
            "command": "...",
            "ports": {...}
          }
        Returns: { "id": "<service_id>" }
        """
        url = (
            f"{self.base_url}/accounts/projects/{project_id}/rooms/{room_name}/services"
        )
        async with self._session.post(
            url,
            headers=self._get_headers(),
            json={
                "template": template,
                "values": values,
            },
        ) as resp:
            await self._raise_for_status(resp)
            return ServiceSpec.model_validate(await resp.json())

    async def update_room_service_from_template(
        self,
        *,
        project_id: str,
        room_name: str,
        service_id: str,
        template: str,
        values: Dict[str, str],
    ) -> ServiceSpec:
        """
        PUT /accounts/projects/{project_id}/services/{service_id}
        Body: same structure as create_service (fields you wish to change).
        Returns: {} on success.
        """

        url = f"{self.base_url}/accounts/projects/{project_id}/rooms/{room_name}/services/{service_id}"
        async with self._session.put(
            url,
            headers=self._get_headers(),
            json={
                "template": template,
                "values": values,
            },
        ) as resp:
            await self._raise_for_status(resp)
            return ServiceSpec.model_validate(await resp.json())

    async def get_room_service(
        self, *, project_id: str, room_name: str, service_id: str
    ) -> ServiceSpec:
        """
        GET /accounts/projects/{project_id}/services/{service_id}
        Returns a `Service` instance.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/rooms/{room_name}/services/{service_id}"
        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            # Handler returns a JSON string, so we read text then validate
            raw = await resp.text()
            try:
                return ServiceSpec.model_validate_json(raw)
            except ValidationError as exc:
                raise RoomException(f"Invalid service payload: {exc}") from exc

    async def list_room_services(
        self, *, project_id: str, room_name: str
    ) -> List[ServiceSpec]:
        """
        GET /accounts/projects/{project_id}/services
        Returns a list of `Service` instances.
        """
        url = (
            f"{self.base_url}/accounts/projects/{project_id}/rooms/{room_name}/services"
        )
        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            data = await resp.json()
            try:
                return [ServiceSpec.model_validate(item) for item in data["services"]]
            except ValidationError as exc:
                raise RoomException(f"Invalid services payload: {exc}") from exc

    async def delete_room_service(
        self, *, project_id: str, room_name: str, service_id: str
    ) -> None:
        """
        DELETE /accounts/projects/{project_id}/services/{service_id}
        Returns nothing on success.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/rooms/{room_name}/services/{service_id}"
        async with self._session.delete(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)

    async def create_repository(
        self,
        *,
        project_id: str,
        repository: CreateProjectRepositoryRequest,
    ) -> ProjectRepository:
        url = f"{self.base_url}/accounts/projects/{project_id}/repositories"
        async with self._session.post(
            url,
            headers=self._get_headers(),
            json=repository.model_dump(mode="json"),
        ) as resp:
            await self._raise_for_status(resp)
            return await self._read_model(resp, ProjectRepository)

    async def update_repository(
        self,
        *,
        project_id: str,
        repository_id: str,
        repository: UpdateProjectRepositoryRequest,
    ) -> ProjectRepository:
        url = (
            f"{self.base_url}/accounts/projects/{project_id}/repositories/"
            f"{repository_id}"
        )
        async with self._session.put(
            url,
            headers=self._get_headers(),
            json=repository.model_dump(mode="json"),
        ) as resp:
            await self._raise_for_status(resp)
            return await self._read_model(resp, ProjectRepository)

    async def get_repository(
        self, *, project_id: str, repository_id: str
    ) -> ProjectRepository:
        url = (
            f"{self.base_url}/accounts/projects/{project_id}/repositories/"
            f"{repository_id}"
        )
        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            return await self._read_model(resp, ProjectRepository)

    async def list_repositories(self, *, project_id: str) -> List[ProjectRepository]:
        url = f"{self.base_url}/accounts/projects/{project_id}/repositories"
        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            data = await resp.json()
            try:
                return [
                    ProjectRepository.model_validate(item)
                    for item in data["repositories"]
                ]
            except ValidationError as exc:
                raise RoomException(f"Invalid repositories payload: {exc}") from exc

    async def delete_repository(self, *, project_id: str, repository_id: str) -> None:
        url = (
            f"{self.base_url}/accounts/projects/{project_id}/repositories/"
            f"{repository_id}"
        )
        async with self._session.delete(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)

    async def create_repository_token(
        self,
        *,
        project_id: str,
        repository_id: str,
        request: CreateRepositoryTokenRequest,
    ) -> RepositoryToken:
        url = (
            f"{self.base_url}/accounts/projects/{project_id}/repositories/"
            f"{repository_id}/token"
        )
        async with self._session.post(
            url,
            headers=self._get_headers(),
            json=request.model_dump(mode="json"),
        ) as resp:
            await self._raise_for_status(resp)
            return await self._read_model(resp, RepositoryToken)

    async def create_project_secret(
        self,
        *,
        project_id: str,
        name: str,
        type: str,
        data: bytes,
    ) -> str:
        url = f"{self.base_url}/accounts/projects/{project_id}/secrets"
        payload = {
            "name": name,
            "type": type,
            "data_base64": _encode_secret_bytes(data),
        }
        async with self._session.post(
            url,
            headers=self._get_headers(),
            json=payload,
        ) as resp:
            await self._raise_for_status(resp)
            return (await resp.json())["id"]

    async def update_project_secret(
        self,
        *,
        project_id: str,
        secret_id: str,
        name: str,
        type: str,
        data: bytes,
    ) -> None:
        url = f"{self.base_url}/accounts/projects/{project_id}/secrets/{secret_id}"
        payload = {
            "name": name,
            "type": type,
            "data_base64": _encode_secret_bytes(data),
        }
        async with self._session.put(
            url,
            headers=self._get_headers(),
            json=payload,
        ) as resp:
            await self._raise_for_status(resp)

    async def get_project_secret(
        self,
        *,
        project_id: str,
        secret_id: str,
    ) -> ManagedSecret:
        url = f"{self.base_url}/accounts/projects/{project_id}/secrets/{secret_id}"
        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            try:
                return ManagedSecret.model_validate(await resp.json())
            except ValidationError as exc:
                raise RoomException(f"Invalid secret payload: {exc}") from exc

    async def list_project_secrets(
        self,
        *,
        project_id: str,
    ) -> list[ManagedSecretInfo]:
        url = f"{self.base_url}/accounts/projects/{project_id}/secrets"
        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            try:
                return _ListManagedSecretsResponse.model_validate(
                    await resp.json()
                ).secrets
            except ValidationError as exc:
                raise RoomException(f"Invalid secrets payload: {exc}") from exc

    async def delete_project_secret(
        self,
        *,
        project_id: str,
        secret_id: str,
    ) -> None:
        url = f"{self.base_url}/accounts/projects/{project_id}/secrets/{secret_id}"
        async with self._session.delete(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)

    async def create_room_secret(
        self,
        *,
        project_id: str,
        room_name: str,
        data: bytes,
        secret_id: str | None = None,
        name: str | None = None,
        type: str | None = None,
        delegated_to: str | None = None,
        for_identity: str | None = None,
    ) -> str:
        url = (
            f"{self.base_url}/accounts/projects/{project_id}/rooms/{room_name}/secrets"
        )
        payload: dict[str, Any] = {
            "data_base64": _encode_secret_bytes(data),
        }
        if secret_id is not None:
            payload["secret_id"] = secret_id
        if name is not None:
            payload["name"] = name
        if type is not None:
            payload["type"] = type
        if delegated_to is not None:
            payload["delegated_to"] = delegated_to
        if for_identity is not None:
            payload["for_identity"] = for_identity
        async with self._session.post(
            url,
            headers=self._get_headers(),
            json=payload,
        ) as resp:
            await self._raise_for_status(resp)
            return (await resp.json())["id"]

    async def update_room_secret(
        self,
        *,
        project_id: str,
        room_name: str,
        secret_id: str,
        data: bytes,
        name: str | None = None,
        type: str | None = None,
        delegated_to: str | None = None,
        for_identity: str | None = None,
    ) -> None:
        url = f"{self.base_url}/accounts/projects/{project_id}/rooms/{room_name}/secrets/{secret_id}"
        payload: dict[str, Any] = {
            "data_base64": _encode_secret_bytes(data),
        }
        if name is not None:
            payload["name"] = name
        if type is not None:
            payload["type"] = type
        if delegated_to is not None:
            payload["delegated_to"] = delegated_to
        if for_identity is not None:
            payload["for_identity"] = for_identity
        async with self._session.put(
            url,
            headers=self._get_headers(),
            json=payload,
        ) as resp:
            await self._raise_for_status(resp)

    async def get_room_secret(
        self,
        *,
        project_id: str,
        room_name: str,
        secret_id: str,
        delegated_to: str | None = None,
        for_identity: str | None = None,
    ) -> ManagedSecret:
        url = f"{self.base_url}/accounts/projects/{project_id}/rooms/{room_name}/secrets/{secret_id}"
        params: dict[str, str] = {}
        if delegated_to is not None:
            params["delegated_to"] = delegated_to
        if for_identity is not None:
            params["for_identity"] = for_identity
        async with self._session.get(
            url,
            headers=self._get_headers(),
            params=params or None,
        ) as resp:
            await self._raise_for_status(resp)
            try:
                return ManagedSecret.model_validate(await resp.json())
            except ValidationError as exc:
                raise RoomException(f"Invalid room secret payload: {exc}") from exc

    async def list_room_secrets(
        self,
        *,
        project_id: str,
        room_name: str,
        for_identity: str | None = None,
    ) -> list[ManagedSecretInfo]:
        url = (
            f"{self.base_url}/accounts/projects/{project_id}/rooms/{room_name}/secrets"
        )
        params: dict[str, str] = {}
        if for_identity is not None:
            params["for_identity"] = for_identity
        async with self._session.get(
            url,
            headers=self._get_headers(),
            params=params or None,
        ) as resp:
            await self._raise_for_status(resp)
            try:
                return _ListManagedSecretsResponse.model_validate(
                    await resp.json()
                ).secrets
            except ValidationError as exc:
                raise RoomException(f"Invalid room secrets payload: {exc}") from exc

    async def validate_participant_token(
        self,
        *,
        token: str,
    ) -> ParticipantToken:
        url = f"{self.base_url}/api/participant-token/validate"
        async with self._session.post(
            url,
            headers={"Content-Type": "application/json"},
            json={"token": token},
        ) as resp:
            await self._raise_for_status(resp)
            try:
                payload = await resp.json()
                if not isinstance(payload, dict):
                    raise RoomException("Invalid participant token payload")
                return ParticipantToken.from_json(payload)
            except (ValidationError, TypeError, RoomException) as exc:
                raise RoomException(
                    f"Invalid participant token payload: {exc}"
                ) from exc

    async def delete_room_secret(
        self,
        *,
        project_id: str,
        room_name: str,
        secret_id: str,
        delegated_to: str | None = None,
        for_identity: str | None = None,
    ) -> None:
        url = f"{self.base_url}/accounts/projects/{project_id}/rooms/{room_name}/secrets/{secret_id}"
        params: dict[str, str] = {}
        if delegated_to is not None:
            params["delegated_to"] = delegated_to
        if for_identity is not None:
            params["for_identity"] = for_identity
        async with self._session.delete(
            url,
            headers=self._get_headers(),
            params=params or None,
        ) as resp:
            await self._raise_for_status(resp)

    async def create_secret(
        self,
        *,
        project_id: str,
        secret: SecretLike,
    ) -> str:
        """
        POST /accounts/projects/{project_id}/secrets
        Returns the new secret_id.
        """
        return await self.create_project_secret(
            project_id=project_id,
            name=secret.name,
            type=secret.type,
            data=json.dumps(secret.to_payload(), sort_keys=True).encode("utf-8"),
        )

    async def update_secret(
        self,
        *,
        project_id: str,
        secret: SecretLike,
    ) -> None:
        """
        PUT /accounts/projects/{project_id}/secrets/{secret.id}
        Body ➜ { "name", "type", "data" }
        """
        await self.update_project_secret(
            project_id=project_id,
            secret_id=secret.id,
            name=secret.name,
            type=secret.type,
            data=json.dumps(secret.to_payload(), sort_keys=True).encode("utf-8"),
        )

    async def delete_secret(self, *, project_id: str, secret_id: str) -> None:
        """
        DELETE /accounts/projects/{project_id}/secrets/{secret_id}
        Returns {} (or 204 No Content) on success.
        """
        await self.delete_project_secret(project_id=project_id, secret_id=secret_id)

    async def list_secrets(self, project_id: str) -> List[SecretLike]:
        """
        GET /accounts/projects/{project_id}/secrets
        Returns [PullSecret | KeysSecret, …]
        """
        secret_infos = await self.list_project_secrets(project_id=project_id)
        secrets: list[SecretLike] = []
        for secret in secret_infos:
            secret_value = await self.get_project_secret(
                project_id=project_id,
                secret_id=secret.id,
            )
            secrets.append(
                _parse_secret_payload(
                    secret=secret_value,
                    raw_data=secret_value.data,
                )
            )
        return secrets

    async def create_project_external_oauth_registration(
        self,
        *,
        project_id: str,
        oauth: OAuthClientConfig,
        client_id: str,
        client_secret: str | None = None,
        delegated_to: str | None = None,
        connector: ConnectorRef | None = None,
    ) -> str:
        url = f"{self.base_url}/accounts/projects/{project_id}/external-oauth"
        payload: dict[str, Any] = {
            "oauth": oauth.model_dump(mode="json"),
            "client_id": client_id,
            "client_secret": client_secret,
            "connector": (
                connector.model_dump(mode="json") if connector is not None else None
            ),
            "delegated_to": delegated_to,
        }
        async with self._session.post(
            url,
            headers=self._get_headers(),
            json=payload,
        ) as resp:
            await self._raise_for_status(resp)
            return (await resp.json())["id"]

    async def update_project_external_oauth_registration(
        self,
        *,
        project_id: str,
        registration_id: str,
        oauth: OAuthClientConfig,
        client_id: str,
        client_secret: str | None = None,
        delegated_to: str | None = None,
        connector: ConnectorRef | None = None,
    ) -> None:
        url = f"{self.base_url}/accounts/projects/{project_id}/external-oauth/{registration_id}"
        payload: dict[str, Any] = {
            "oauth": oauth.model_dump(mode="json"),
            "client_id": client_id,
            "client_secret": client_secret,
            "connector": (
                connector.model_dump(mode="json") if connector is not None else None
            ),
            "delegated_to": delegated_to,
        }
        async with self._session.put(
            url,
            headers=self._get_headers(),
            json=payload,
        ) as resp:
            await self._raise_for_status(resp)

    async def list_project_external_oauth_registrations(
        self,
        *,
        project_id: str,
        delegated_to: str | None = None,
    ) -> list[ExternalOAuthClientRegistration]:
        url = f"{self.base_url}/accounts/projects/{project_id}/external-oauth"
        params: dict[str, str] = {}
        if delegated_to is not None:
            params["delegated_to"] = delegated_to
        async with self._session.get(
            url,
            headers=self._get_headers(),
            params=params or None,
        ) as resp:
            await self._raise_for_status(resp)
            try:
                return _ListExternalOAuthClientRegistrationsResponse.model_validate(
                    await resp.json()
                ).registrations
            except ValidationError as exc:
                raise RoomException(
                    f"Invalid external oauth registrations payload: {exc}"
                ) from exc

    async def delete_project_external_oauth_registration(
        self,
        *,
        project_id: str,
        registration_id: str,
        delegated_to: str | None = None,
    ) -> None:
        url = f"{self.base_url}/accounts/projects/{project_id}/external-oauth/{registration_id}"
        params: dict[str, str] = {}
        if delegated_to is not None:
            params["delegated_to"] = delegated_to
        async with self._session.delete(
            url,
            headers=self._get_headers(),
            params=params or None,
        ) as resp:
            await self._raise_for_status(resp)

    async def create_room_external_oauth_registration(
        self,
        *,
        project_id: str,
        room_name: str,
        oauth: OAuthClientConfig,
        client_id: str,
        client_secret: str | None = None,
        delegated_to: str | None = None,
        connector: ConnectorRef | None = None,
    ) -> str:
        url = f"{self.base_url}/accounts/projects/{project_id}/rooms/{room_name}/external-oauth"
        payload: dict[str, Any] = {
            "oauth": oauth.model_dump(mode="json"),
            "client_id": client_id,
            "client_secret": client_secret,
            "connector": (
                connector.model_dump(mode="json") if connector is not None else None
            ),
            "delegated_to": delegated_to,
        }
        async with self._session.post(
            url,
            headers=self._get_headers(),
            json=payload,
        ) as resp:
            await self._raise_for_status(resp)
            return (await resp.json())["id"]

    async def update_room_external_oauth_registration(
        self,
        *,
        project_id: str,
        room_name: str,
        registration_id: str,
        oauth: OAuthClientConfig,
        client_id: str,
        client_secret: str | None = None,
        delegated_to: str | None = None,
        connector: ConnectorRef | None = None,
    ) -> None:
        url = f"{self.base_url}/accounts/projects/{project_id}/rooms/{room_name}/external-oauth/{registration_id}"
        payload: dict[str, Any] = {
            "oauth": oauth.model_dump(mode="json"),
            "client_id": client_id,
            "client_secret": client_secret,
            "connector": (
                connector.model_dump(mode="json") if connector is not None else None
            ),
            "delegated_to": delegated_to,
        }
        async with self._session.put(
            url,
            headers=self._get_headers(),
            json=payload,
        ) as resp:
            await self._raise_for_status(resp)

    async def list_room_external_oauth_registrations(
        self,
        *,
        project_id: str,
        room_name: str,
        delegated_to: str | None = None,
    ) -> list[ExternalOAuthClientRegistration]:
        url = f"{self.base_url}/accounts/projects/{project_id}/rooms/{room_name}/external-oauth"
        params: dict[str, str] = {}
        if delegated_to is not None:
            params["delegated_to"] = delegated_to
        async with self._session.get(
            url,
            headers=self._get_headers(),
            params=params or None,
        ) as resp:
            await self._raise_for_status(resp)
            try:
                return _ListExternalOAuthClientRegistrationsResponse.model_validate(
                    await resp.json()
                ).registrations
            except ValidationError as exc:
                raise RoomException(
                    f"Invalid room external oauth registrations payload: {exc}"
                ) from exc

    async def delete_room_external_oauth_registration(
        self,
        *,
        project_id: str,
        room_name: str,
        registration_id: str,
        delegated_to: str | None = None,
    ) -> None:
        url = f"{self.base_url}/accounts/projects/{project_id}/rooms/{room_name}/external-oauth/{registration_id}"
        params: dict[str, str] = {}
        if delegated_to is not None:
            params["delegated_to"] = delegated_to
        async with self._session.delete(
            url,
            headers=self._get_headers(),
            params=params or None,
        ) as resp:
            await self._raise_for_status(resp)

    async def create_room(
        self,
        *,
        project_id: str,
        name: str,
        if_not_exists: bool = False,
        metadata: Optional[dict[str, any]] = None,
        annotations: Optional[dict[str, str]] = None,
        permissions: Optional[dict[str, ApiScope]] = None,
    ) -> Room:
        """
        POST /accounts/projects/{project_id}/rooms
        Body: { "name": str, "if_not_exists?": bool }
        Returns Room.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/rooms"
        payload = {
            "name": name,
            "if_not_exists": bool(if_not_exists),
            "metadata": metadata,
            "annotations": annotations,
            "permissions": permissions,
        }
        async with self._session.post(
            url, headers=self._get_headers(), json=payload
        ) as resp:
            await self._raise_for_status(resp)
            try:
                return Room.model_validate(await resp.json())
            except ValidationError as exc:
                raise RoomException(f"Invalid room payload: {exc}") from exc

    async def get_room(self, *, project_id: str, name: str) -> Room:
        """
        GET /accounts/projects/{project_id}/rooms/{room_name}
        Returns Room.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/rooms/{name}"
        async with self._session.get(url, headers=self._get_headers()) as resp:
            if resp.status == 404:
                raise RoomException("room not found")
            await self._raise_for_status(resp)
            try:
                return Room.model_validate(await resp.json())
            except ValidationError as exc:
                raise RoomException(f"Invalid room payload: {exc}") from exc

    async def update_room(
        self,
        *,
        project_id: str,
        room_id: str,
        name: str,
        metadata: Optional[dict[str, any]] = None,
        annotations: Optional[dict[str, str]] = None,
    ) -> None:
        """
        PUT /accounts/projects/{project_id}/rooms/{room_id}
        Body: { "name": str }
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/rooms/{room_id}"
        payload = {"name": name}

        if metadata is not None:
            payload["metadata"] = metadata
        if annotations is not None:
            payload["annotations"] = annotations

        async with self._session.put(
            url, headers=self._get_headers(), json=payload
        ) as resp:
            await self._raise_for_status(resp)

    async def delete_room(self, *, project_id: str, room_id: str) -> None:
        """
        DELETE /accounts/projects/{project_id}/rooms/{room_id}
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/rooms/{room_id}"
        async with self._session.delete(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)

    async def connect_room(self, *, project_id: str, room: str) -> RoomConnectionInfo:
        """
        POST /accounts/projects/{project_id}/rooms/{room_name}/connect
        Returns: { "jwt", "room_name", "project_id", "room_url" }
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/rooms/{room}/connect"
        async with self._session.post(
            url, headers=self._get_headers(), json={}
        ) as resp:
            await self._raise_for_status(resp)
            return RoomConnectionInfo.model_validate(await resp.json())

    async def create_room_grant(
        self,
        *,
        project_id: str,
        room_id: str,
        user_id: str,
        permissions: Dict[str, Any],
    ) -> None:
        """
        POST /accounts/projects/{project_id}/room-grants
        Body: { "room_id", "user_id", "permissions" }
        Returns {} on success.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/room-grants"
        payload = _CreateRoomGrantRequest(
            room_id=room_id,
            user_id=user_id,
            permissions=permissions,
        ).model_dump(mode="json")

        async with self._session.post(
            url, headers=self._get_headers(), json=payload
        ) as resp:
            await self._raise_for_status(resp)

    async def create_room_grant_by_email(
        self,
        *,
        project_id: str,
        room_id: str,
        email: str,
        permissions: ApiScope,
    ) -> None:
        """
        POST /accounts/projects/{project_id}/room-grants
        Body: { "room_id", "user_id", "permissions" }
        Returns {} on success.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/room-grants"
        payload = _CreateRoomGrantRequest(
            room_id=room_id,
            email=email,
            permissions=permissions,
        ).model_dump(mode="json")

        async with self._session.post(
            url, headers=self._get_headers(), json=payload
        ) as resp:
            await self._raise_for_status(resp)

    async def update_room_grant(
        self,
        *,
        project_id: str,
        room_id: str,
        user_id: str,
        permissions: ApiScope,
        grant_id: Optional[str] = None,
    ) -> None:
        """
        PUT /accounts/projects/{project_id}/room-grants/{grant_id}
        Body: { "room_id", "user_id", "permissions" }
        NOTE: The server handler currently ignores grant_id and updates by (project_id, room_id, user_id).
        """
        gid = grant_id or "unused"
        url = f"{self.base_url}/accounts/projects/{project_id}/room-grants/{gid}"
        payload = _UpdateRoomGrantRequest(
            room_id=room_id,
            user_id=user_id,
            permissions=permissions,
        ).model_dump(mode="json")

        async with self._session.put(
            url, headers=self._get_headers(), json=payload
        ) as resp:
            await self._raise_for_status(resp)

    async def delete_room_grant(
        self, *, project_id: str, room_id: str, user_id: str
    ) -> None:
        """
        DELETE /accounts/projects/{project_id}/room-grants/{room_id}/{user_id}
        Returns {} on success.
        """
        from urllib.parse import quote

        url = (
            f"{self.base_url}/accounts/projects/{project_id}"
            f"/room-grants/{quote(room_id, safe='')}/{quote(user_id, safe='')}"
        )
        async with self._session.delete(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)

    async def get_room_grant(
        self, *, project_id: str, room_id: str, user_id: str
    ) -> ProjectRoomGrant:
        """
        GET /accounts/projects/{project_id}/room-grants/{room_id}/{user_id}
        Returns ProjectRoomGrant
        """
        from urllib.parse import quote

        url = (
            f"{self.base_url}/accounts/projects/{project_id}"
            f"/room-grants/{quote(room_id, safe='')}/{quote(user_id, safe='')}"
        )
        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            data = await resp.json()
            try:
                return ProjectRoomGrant.model_validate(data)
            except ValidationError as exc:
                raise RoomException(f"Invalid room grant payload: {exc}") from exc

    async def list_rooms(
        self,
        *,
        project_id: str,
        limit: int = 50,
        offset: int = 0,
        order_by: str = "room_name",
    ) -> List[ProjectRoomGrant]:
        """
        GET /accounts/projects/{project_id}/rooms?limit=&offset=&order_by=
        Returns [Rooms]
        """
        params = {"limit": str(limit), "offset": str(offset), "order_by": order_by}
        url = f"{self.base_url}/accounts/projects/{project_id}/rooms"
        async with self._session.get(
            url, headers=self._get_headers(), params=params
        ) as resp:
            await self._raise_for_status(resp)
            data = await resp.json()
            try:
                return [Room.model_validate(item) for item in data["rooms"]]
            except ValidationError as exc:
                raise RoomException(f"Invalid rooms list payload: {exc}") from exc

    async def list_room_grants(
        self,
        *,
        project_id: str,
        limit: int = 50,
        offset: int = 0,
        order_by: str = "room_name",
    ) -> List[ProjectRoomGrant]:
        """
        GET /accounts/projects/{project_id}/room-grants?limit=&offset=&order_by=
        Returns [ProjectRoomGrant]
        """
        params = {"limit": str(limit), "offset": str(offset), "order_by": order_by}
        url = f"{self.base_url}/accounts/projects/{project_id}/room-grants"
        async with self._session.get(
            url, headers=self._get_headers(), params=params
        ) as resp:
            await self._raise_for_status(resp)
            data = await resp.json()
            try:
                return [
                    ProjectRoomGrant.model_validate(item)
                    for item in data["room_grants"]
                ]
            except ValidationError as exc:
                raise RoomException(f"Invalid room grants list payload: {exc}") from exc

    async def list_room_grants_by_user(
        self,
        *,
        project_id: str,
        user_id: str,
        limit: int = 50,
        offset: int = 0,
    ) -> List[ProjectRoomGrant]:
        """
        GET /accounts/projects/{project_id}/room-grants/by-user/{user_id}?limit=&offset=&order_by=
        Returns [ProjectRoomGrant]
        """
        from urllib.parse import quote

        params = {"limit": str(limit), "offset": str(offset)}
        url = (
            f"{self.base_url}/accounts/projects/{project_id}"
            f"/room-grants/by-user/{quote(user_id, safe='')}"
        )
        async with self._session.get(
            url, headers=self._get_headers(), params=params
        ) as resp:
            await self._raise_for_status(resp)
            data = await resp.json()
            try:
                return [
                    ProjectRoomGrant.model_validate(item)
                    for item in data["room_grants"]
                ]
            except ValidationError as exc:
                raise RoomException(
                    f"Invalid room grants-by-user payload: {exc}"
                ) from exc

    async def list_room_grants_by_room(
        self,
        *,
        project_id: str,
        room_name: str,
        limit: int = 50,
        offset: int = 0,
    ) -> List[ProjectRoomGrant]:
        """
        GET /accounts/projects/{project_id}/room-grants/by-room/{room_id}?limit=&offset=
        Returns [ProjectRoomGrant]
        """
        from urllib.parse import quote

        params = {"limit": str(limit), "offset": str(offset)}
        url = (
            f"{self.base_url}/accounts/projects/{project_id}"
            f"/room-grants/by-room/{quote(room_name, safe='')}"
        )
        async with self._session.get(
            url, headers=self._get_headers(), params=params
        ) as resp:
            await self._raise_for_status(resp)
            data = await resp.json()
            try:
                return [
                    ProjectRoomGrant.model_validate(item)
                    for item in data["room_grants"]
                ]
            except ValidationError as exc:
                raise RoomException(
                    f"Invalid room grants-by-room payload: {exc}"
                ) from exc

    async def list_unique_rooms_with_grants(
        self,
        *,
        project_id: str,
        limit: int = 50,
        offset: int = 0,
    ) -> List[ProjectRoomGrantCount]:
        """
        GET /accounts/projects/{project_id}/room-grants/by-room?limit=&offset=
        Returns [ProjectRoomGrantCount]; accepts either {"room": "..."} or {"room_name": "..."} shapes.
        """
        params = {"limit": str(limit), "offset": str(offset)}
        url = f"{self.base_url}/accounts/projects/{project_id}/room-grants/by-room"
        async with self._session.get(
            url, headers=self._get_headers(), params=params
        ) as resp:
            await self._raise_for_status(resp)
            data = await resp.json()
            items = data.get("rooms", [])
            out: List[ProjectRoomGrantCount] = []
            for item in items:
                # tolerate either key name
                out.append(ProjectRoomGrantCount.model_validate(item))
            return out

    async def list_unique_users_with_grants(
        self,
        *,
        project_id: str,
        limit: int = 50,
        offset: int = 0,
    ) -> List[ProjectUserGrantCount]:
        """
        GET /accounts/projects/{project_id}/room-grants/by-user?limit=&offset=
        Returns [ProjectUserGrantCount]
        """
        params = {"limit": str(limit), "offset": str(offset)}
        url = f"{self.base_url}/accounts/projects/{project_id}/room-grants/by-user"
        async with self._session.get(
            url, headers=self._get_headers(), params=params
        ) as resp:
            await self._raise_for_status(resp)
            data = await resp.json()
            items = data.get("users", [])
            out: List[ProjectUserGrantCount] = []
            for item in items:
                out.append(ProjectUserGrantCount.model_validate(item))
            return out

    async def create_oauth_client(
        self,
        *,
        project_id: str,
        grant_types: List[str],
        response_types: List[str],
        redirect_uris: List[str],
        scope: str,
        metadata: Optional[Dict[str, Any]] = None,
        official: bool = False,
    ) -> OAuthClient:
        """
        POST /accounts/projects/{project_id}/oauth/clients
        Body: { grant_types, response_types, redirect_uris, scope, metadata? }
        Returns the newly created client (including client_secret).
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/oauth/clients"
        payload = {
            "grant_types": grant_types,
            "response_types": response_types,
            "redirect_uris": redirect_uris,
            "scope": scope,
            "metadata": metadata or {},
            "official": official,
        }
        async with self._session.post(
            url, headers=self._get_headers(), json=payload
        ) as resp:
            await self._raise_for_status(resp)
            raw = await resp.json()
            try:
                return OAuthClient.model_validate(raw)
            except ValidationError as exc:
                raise RoomException(
                    f"Invalid create-oauth-client payload: {exc}"
                ) from exc

    async def update_oauth_client(
        self,
        *,
        project_id: str,
        client_id: str,
        grant_types: Optional[List[str]] = None,
        response_types: Optional[List[str]] = None,
        redirect_uris: Optional[List[str]] = None,
        scope: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        official: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        PUT /accounts/projects/{project_id}/oauth/clients/{client_id}
        Body: any subset of { grant_types, response_types, redirect_uris, scope, metadata }
        Returns { "ok": True } on success.
        """
        url = (
            f"{self.base_url}/accounts/projects/{project_id}/oauth/clients/{client_id}"
        )
        body: Dict[str, Any] = {}
        if grant_types is not None:
            body["grant_types"] = grant_types
        if response_types is not None:
            body["response_types"] = response_types
        if redirect_uris is not None:
            body["redirect_uris"] = redirect_uris
        if scope is not None:
            body["scope"] = scope
        if metadata is not None:
            body["metadata"] = metadata
        if official is not None:
            body["official"] = official

        async with self._session.put(
            url, headers=self._get_headers(), json=body
        ) as resp:
            await self._raise_for_status(resp)
            return await resp.json()

    async def list_oauth_clients(self, *, project_id: str) -> List[OAuthClient]:
        """
        GET /accounts/projects/{project_id}/oauth/clients
        Returns a list of OAuthClient (no secrets).
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/oauth/clients"
        async with self._session.get(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            raw = await resp.json()
            try:
                return [
                    OAuthClient.model_validate(item) for item in raw.get("clients", [])
                ]
            except ValidationError as exc:
                raise RoomException(
                    f"Invalid oauth-clients list payload: {exc}"
                ) from exc

    async def get_oauth_client(
        self, *, project_id: Optional[str], client_id: str
    ) -> OAuthClient:
        """
        GET /accounts/projects/{project_id}/oauth/clients/{client_id}
        Returns the OAuthClient (no secret).
        """

        url = (
            f"{self.base_url}/accounts/projects/{project_id}/oauth/clients/{client_id}"
        )
        async with self._session.get(url, headers=self._get_headers()) as resp:
            if resp.status == 404:
                raise RoomException("oauth client not found")
            await self._raise_for_status(resp)
            raw = await resp.json()
            try:
                return OAuthClient.model_validate(raw)
            except ValidationError as exc:
                raise RoomException(f"Invalid oauth-client payload: {exc}") from exc

    async def delete_oauth_client(self, *, project_id: str, client_id: str) -> None:
        """
        DELETE /accounts/projects/{project_id}/oauth/clients/{client_id}
        Returns 204 No Content on success.
        """
        url = (
            f"{self.base_url}/accounts/projects/{project_id}/oauth/clients/{client_id}"
        )
        async with self._session.delete(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)

    # ---------------------------
    # Scheduled Tasks
    # ---------------------------

    async def create_scheduled_task(
        self,
        *,
        project_id: str,
        room_name: str,
        queue_name: str,
        payload: Any,
        schedule: str,
        active: bool = True,
        task_id: Optional[str] = None,
        once: bool = False,
        annotations: Optional[dict[str, str]] = None,
    ) -> str:
        """
        POST /accounts/projects/{project_id}/scheduled-tasks

        payload can be dict (preferred) or json-string.
        Returns the created ScheduledTask when the server returns it.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/scheduled-tasks"

        body = _CreateScheduledTaskRequest(
            id=task_id,
            room_name=room_name,
            queue_name=queue_name,
            payload=payload,
            schedule=schedule,
            active=active,
            once=once,
            annotations=annotations,
        ).model_dump(mode="json", exclude_none=True)

        async with self._session.post(
            url, headers=self._get_headers(), json=body
        ) as resp:
            await self._raise_for_status(resp)
            data = await resp.json()
            return data["task_id"]

    async def update_scheduled_task(
        self,
        *,
        project_id: str,
        task_id: str,
        room_name: Optional[str] = None,
        queue_name: Optional[str] = None,
        payload: Optional[Any] = None,
        schedule: Optional[str] = None,
        active: Optional[bool] = None,
        annotations: Optional[dict[str, str]] = None,
    ) -> None:
        """
        PUT /accounts/projects/{project_id}/scheduled-tasks/{task_id}

        Patch-like update. Any omitted fields are left unchanged.
        Returns the updated ScheduledTask when the server returns it; otherwise fetches it.
        """
        url = (
            f"{self.base_url}/accounts/projects/{project_id}/scheduled-tasks/{task_id}"
        )

        body = _UpdateScheduledTaskRequest(
            room_name=room_name,
            queue_name=queue_name,
            payload=payload,
            schedule=schedule,
            active=active,
            annotations=annotations,
        ).model_dump(mode="json", exclude_none=True)

        async with self._session.put(
            url, headers=self._get_headers(), json=body
        ) as resp:
            await self._raise_for_status(resp)

    async def delete_scheduled_task(self, *, project_id: str, task_id: str) -> None:
        """
        DELETE /accounts/projects/{project_id}/scheduled-tasks/{task_id}
        Returns 204 No Content on success.
        """
        url = (
            f"{self.base_url}/accounts/projects/{project_id}/scheduled-tasks/{task_id}"
        )
        async with self._session.delete(url, headers=self._get_headers()) as resp:
            await self._raise_for_status(resp)
            return None

    async def list_scheduled_tasks(
        self,
        *,
        project_id: str,
        room_name: Optional[str] = None,
        task_id: Optional[str] = None,
        active: Optional[bool] = None,
        limit: int = 200,
        offset: int = 0,
    ) -> List[ScheduledTask]:
        """
        GET /accounts/projects/{project_id}/scheduled-tasks?room_name=&task_id=&active=&limit=&offset=
        Returns a list[ScheduledTask].
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/scheduled-tasks"
        params: Dict[str, str] = {
            "limit": str(limit),
            "offset": str(offset),
        }
        if room_name is not None:
            params["room_name"] = room_name
        if task_id is not None:
            params["task_id"] = task_id
        if active is not None:
            params["active"] = "true" if active else "false"

        async with self._session.get(
            url, headers=self._get_headers(), params=params
        ) as resp:
            await self._raise_for_status(resp)
            data = await resp.json()

        tasks_raw = data.get("tasks", [])
        if not isinstance(tasks_raw, list):
            raise RoomException(
                "Invalid scheduled-tasks payload: expected 'tasks' to be a list"
            )

        try:
            return [ScheduledTask.model_validate(item) for item in tasks_raw]
        except ValidationError as exc:
            raise RoomException(f"Invalid scheduled-tasks payload: {exc}") from exc
