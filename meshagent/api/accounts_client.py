import aiohttp
from typing import Any, Dict, List, Optional, Literal
from pydantic import BaseModel, ValidationError
from meshagent.api import RoomException

from pydantic import Field


# ------------------------------------------------------------------
#  Secret models
# ------------------------------------------------------------------


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


def _parse_secret(raw: dict) -> SecretLike:
    """
    Decide which concrete Pydantic class to use based on the 'type' field.
    """
    if raw.get("type") == "docker":
        return PullSecret.model_validate(
            {"id": raw["id"], "name": raw["name"], "type": raw["type"], **raw["data"]}
        )
    else:  # defaults to keys_secret
        return KeysSecret.model_validate(
            {
                "id": raw["id"],
                "name": raw["name"],
                "type": raw["type"],
                "data": raw["data"],
            }
        )


class Endpoint(BaseModel):
    type: Literal["mcp.sse", "meshagent.callable", "http", "tcp"]
    path: Optional[str | None] = None
    participant_name: Optional[str | None] = None


class Port(BaseModel):
    liveness_path: Optional[str | None] = None
    participant_name: Optional[str | None] = None

    type: Optional[Literal["mcp.sse", "meshagent.callable", "http", "tcp"]] = None
    path: Optional[str | None] = None

    endpoints: Optional[list[Endpoint]] = None


class Service(BaseModel):
    id: Optional[str] = None
    image: str
    name: str
    environment: Optional[Dict[str, str]] = None
    command: Optional[str] = None
    room_storage_path: Optional[str] = None
    room_storage_subpath: Optional[str] = None
    pull_secret: Optional[str] = None
    runtime_secrets: Optional[Dict[str, str]] = None
    environment_secrets: Optional[list[str]] = None
    created_at: Optional[str] = None
    ports: Optional[Dict[int, Port]] = (None,)
    role: Optional[Literal["user", "tool", "agent"]] = None
    builtin: bool = Field(exclude=True, default=False)


class Services(BaseModel):
    services: list["Service"]


ProjectRole = Literal["member", "admin"]


class AccountsClient:
    """
    A simple asynchronous client to interact with the accounts routes.
    """

    def __init__(self, base_url: str, token: str):
        """
        :param base_url: The root URL of your server, e.g. 'http://localhost:8080'.
        :param token: A Bearer token for the Authorization header.
        """
        self.base_url = base_url.rstrip("/")
        self.token = token  # The "Bearer" token

        session = aiohttp.ClientSession()
        self._session = session

    async def close(self):
        await self._session.close()

    def _get_headers(self) -> Dict[str, str]:
        """
        Returns the default headers including Bearer Authorization.
        """
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    async def get_project_role(self, project_id: str) -> ProjectRole:
        """
        Corresponds to: GET /accounts/projects/{id}/role
        Returns a JSON dict with { "role" : "member" | "admin" } on success.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}"

        async with self._session.get(
            url,
            headers=self._get_headers(),
        ) as resp:
            resp.raise_for_status()
            return await resp.json()["role"]

    async def create_share(
        self, project_id: str, settings: Optional[dict] = None
    ) -> Dict[str, Any]:
        """
        Corresponds to: POST /accounts/projects
        Body: { "name": "<name>" }
        Returns a JSON dict with { "id" } on success.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/shares"

        async with self._session.post(
            url,
            headers=self._get_headers(),
            json={"settings": settings},
        ) as resp:
            resp.raise_for_status()
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
            resp.raise_for_status()
            return None

    async def update_share(
        self, project_id: str, share_id: str, settings: Optional[dict] = None
    ) -> None:
        """
        Corresponds to: PUT /accounts/projects/:id/shares/:share_id
        Body: { "settings" }
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/shares/{share_id}"

        async with self._session.put(
            url,
            headers=self._get_headers(),
            json={"settings": settings},
        ) as resp:
            resp.raise_for_status()
            return None

    async def list_shares(self, project_id: str) -> None:
        """
        Corresponds to: GET /accounts/projects/:id/shares
        Returns a JSON dict with { "shares" : [{ "id", "settings" }] } on success.
        """
        url = f"{self.base_url}/shares/{project_id}"

        async with self._session.get(
            url,
            headers=self._get_headers(),
        ) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def connect_share(self, share_id: str) -> None:
        """
        Corresponds to: POST /shares/:share_id/connect
        Body: {}
        Returns a JSON dict with { "jwt", "room_url" } on success.
        """
        url = f"{self.base_url}/shares/{share_id}"

        async with self._session.post(
            url,
            headers=self._get_headers(),
            json={},
        ) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def create_project(
        self, name: str, settings: Optional[dict] = None
    ) -> Dict[str, Any]:
        """
        Corresponds to: POST /accounts/projects
        Body: { "name": "<name>" }
        Returns a JSON dict with { "id", "owner_user_id", "name" } on success.
        """
        url = f"{self.base_url}/accounts/projects"

        async with self._session.post(
            url,
            headers=self._get_headers(),
            json={"name": name, "settings": settings},
        ) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def add_user_to_project(
        self, project_id: str, user_id: str
    ) -> Dict[str, Any]:
        """
        Corresponds to: POST /accounts/projects/:id/users
        Body: { "project_id", "user_id" }
        Returns a JSON dict with { "ok": True } on success.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/users"
        body = {"project_id": project_id, "user_id": user_id}
        async with self._session.post(
            url, headers=self._get_headers(), json=body
        ) as resp:
            resp.raise_for_status()
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
            resp.raise_for_status()
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
            resp.raise_for_status()
            return await resp.json()

    async def get_users_in_project(self, project_id: str) -> Dict[str, Any]:
        """
        Corresponds to: GET /accounts/projects/:id/users
        Returns a JSON dict with { "users": [...] }.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/users"

        async with self._session.get(url, headers=self._get_headers()) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_user_profile(self, user_id: str) -> Dict[str, Any]:
        """
        Corresponds to: GET /accounts/profiles/:id
        Returns the user profile JSON, e.g. { "id", "first_name", "last_name", "email" } or raises 404 if not found.
        """
        url = f"{self.base_url}/accounts/profiles/{user_id}"

        async with self._session.get(url, headers=self._get_headers()) as resp:
            resp.raise_for_status()
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
            resp.raise_for_status()
            return await resp.json()

    async def list_projects(self) -> Dict[str, Any]:
        """
        Corresponds to: GET /accounts/projects
        Returns a JSON dict with { "projects": [...] }.
        """
        url = f"{self.base_url}/accounts/projects"

        async with self._session.get(url, headers=self._get_headers()) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_project(self, project_id: str) -> Dict[str, Any]:
        """
        Corresponds to: GET /accounts/projects
        Returns a JSON dict with { "projects": [...] }.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}"

        async with self._session.get(url, headers=self._get_headers()) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def create_project_participant_token(
        self, project_id: str, room_name: str
    ) -> Dict[str, Any]:
        """
        Corresponds to: POST /accounts/projects/{project_id}/participant-tokens
        Body: { "room_name": "<>" }
        Returns a JSON dict with { "token" }.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/participant-tokens"
        payload = {
            "room_name": room_name,
        }

        async with self._session.post(
            url, headers=self._get_headers(), json=payload
        ) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def create_project_api_key(
        self, project_id: str, name: str, description: str
    ) -> Dict[str, Any]:
        """
        Corresponds to: POST /accounts/projects/{project_id}/api-keys
        Body: { "name": "<>", "description": "<>" }
        Returns a JSON dict with { "id", "name", "description", "token" }.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/api-keys"
        payload = {"name": name, "description": description}

        async with self._session.post(
            url, headers=self._get_headers(), json=payload
        ) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_usage(self, project_id: str) -> list[map]:
        """
        Corresponds to: GET /accounts/projects/{project_id}/usage
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/usage"

        async with self._session.get(url, headers=self._get_headers()) as resp:
            resp.raise_for_status()

            return (await resp.json())["usage"]

    async def delete_project_api_key(self, project_id: str, id: str) -> None:
        """
        Corresponds to: DELETE /accounts/projects/{project_id}/api-keys/{token_id}
        Returns 204 No Content on success (no JSON body).
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/api-keys/{id}"

        async with self._session.delete(url, headers=self._get_headers()) as resp:
            resp.raise_for_status()
            # The server returns status 204 with no content, so no need to parse JSON.

    async def list_project_api_keys(self, project_id: str) -> Dict[str, Any]:
        """
        Corresponds to: GET /accounts/projects/{project_id}/api-keys
        Returns a JSON dict like: { "tokens": [ { ... }, ... ] }.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/api-keys"

        async with self._session.get(url, headers=self._get_headers()) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def decrypt_project_api_key(self, project_id: str, id: str) -> Dict[str, Any]:
        """
        Corresponds to: GET /accounts/projects/{project_id}/api-keys/{token_id}/decrypt
        Returns a JSON dict with { "tokens": <decrypted_token> }.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/api-keys/{id}/decrypt"

        async with self._session.get(url, headers=self._get_headers()) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_session(self, project_id: str, session_id: str) -> Dict[str, Any]:
        """
        Corresponds to: GET /accounts/projects/{project_id}/sessions/{session_id}
        Returns a JSON dict: { "id", "room_name", "created_at }
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/sessions"

        async with self._session.get(url, headers=self._get_headers()) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def list_recent_sessions(self, project_id: str) -> Dict[str, Any]:
        """
        Corresponds to: GET /accounts/projects/{project_id}/sessions
        Returns a JSON dict: { "sessions": [...] }
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/sessions"

        async with self._session.get(url, headers=self._get_headers()) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def list_session_events(
        self, project_id: str, session_id: str
    ) -> Dict[str, Any]:
        """
        Corresponds to: GET /accounts/projects/{project_id}/sessions/{session_id}/events
        Returns a JSON dict: { "events": [...] }
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/sessions/{session_id}/events"

        async with self._session.get(url, headers=self._get_headers()) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def create_project_webhook(
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
            resp.raise_for_status()
            # If the server returns JSON with newly created webhook info, parse it:
            return await resp.json()

    async def update_project_webhook(
        self,
        project_id: str,
        webhook_id: str,
        name: str,
        url: str,
        events: List[str],
        description: str = "",
        action: Optional[str] = None,
        payload: Optional[dict] = None,
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
        }

        async with self._session.put(
            endpoint, headers=self._get_headers(), json=payload
        ) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def list_project_webhooks(self, project_id: str) -> Dict[str, Any]:
        """
        Corresponds to: GET /accounts/projects/{project_id}/webhooks
        Returns a JSON dict like: { "webhooks": [ { ... }, ... ] }
        """
        endpoint = f"{self.base_url}/accounts/projects/{project_id}/webhooks"

        async with self._session.get(endpoint, headers=self._get_headers()) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def delete_project_webhook(self, project_id: str, webhook_id: str) -> None:
        """
        Corresponds to: DELETE /accounts/projects/{project_id}/webhooks/{webhook_id}
        Typically returns 200 or 204 on success (no JSON body).
        """
        endpoint = (
            f"{self.base_url}/accounts/projects/{project_id}/webhooks/{webhook_id}"
        )

        async with self._session.delete(endpoint, headers=self._get_headers()) as resp:
            resp.raise_for_status()

    # ---------------------------------------------------------------------
    # Services
    # ---------------------------------------------------------------------

    async def create_service(
        self,
        *,
        project_id: str,
        service: Service,
    ) -> Dict[str, Any]:
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
        url = f"{self.base_url}/accounts/projects/{project_id}/services"
        async with self._session.post(
            url,
            headers=self._get_headers(),
            json=service.model_dump(mode="json", exclude_unset=True),
        ) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def update_service(
        self,
        *,
        project_id: str,
        service_id: str,
        service: Dict[str, Any] | Service,
    ) -> Dict[str, Any]:
        """
        PUT /accounts/projects/{project_id}/services/{service_id}
        Body: same structure as create_service (fields you wish to change).
        Returns: {} on success.
        """

        if isinstance(service, Service):
            service = service.model_dump(mode="json", exclude_unset=True)

        url = f"{self.base_url}/accounts/projects/{project_id}/services/{service_id}"
        async with self._session.put(
            url, headers=self._get_headers(), json=service
        ) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_service(self, *, project_id: str, service_id: str) -> Service:
        """
        GET /accounts/projects/{project_id}/services/{service_id}
        Returns a `Service` instance.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/services/{service_id}"
        async with self._session.get(url, headers=self._get_headers()) as resp:
            resp.raise_for_status()
            # Handler returns a JSON string, so we read text then validate
            raw = await resp.text()
            try:
                return Service.model_validate_json(raw)
            except ValidationError as exc:
                raise RoomException(f"Invalid service payload: {exc}") from exc

    async def list_services(self, *, project_id: str) -> List[Service]:
        """
        GET /accounts/projects/{project_id}/services
        Returns a list of `Service` instances.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/services"
        async with self._session.get(url, headers=self._get_headers()) as resp:
            resp.raise_for_status()
            data = await resp.json()
            try:
                return [Service.model_validate(item) for item in data["services"]]
            except ValidationError as exc:
                raise RoomException(f"Invalid services payload: {exc}") from exc

    async def delete_service(self, *, project_id: str, service_id: str) -> None:
        """
        DELETE /accounts/projects/{project_id}/services/{service_id}
        Returns nothing on success.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/services/{service_id}"
        async with self._session.delete(url, headers=self._get_headers()) as resp:
            resp.raise_for_status()

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
        url = f"{self.base_url}/accounts/projects/{project_id}/secrets"
        payload = {
            "name": secret.name,
            "type": secret.type,  # "docker" | "keys"
            "data": secret.to_payload(),  # already shaped for the provisioner
        }
        async with self._session.post(
            url, headers=self._get_headers(), json=payload
        ) as resp:
            resp.raise_for_status()
            return (await resp.json())["id"]

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
        url = f"{self.base_url}/accounts/projects/{project_id}/secrets/{secret.id}"
        payload = {
            "name": secret.name,
            "type": secret.type,
            "data": secret.to_payload(),
        }
        async with self._session.put(
            url, headers=self._get_headers(), json=payload
        ) as resp:
            resp.raise_for_status()

    async def delete_secret(self, *, project_id: str, secret_id: str) -> None:
        """
        DELETE /accounts/projects/{project_id}/secrets/{secret_id}
        Returns {} (or 204 No Content) on success.
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/secrets/{secret_id}"
        async with self._session.delete(url, headers=self._get_headers()) as resp:
            resp.raise_for_status()

    async def list_secrets(self, project_id: str) -> List[SecretLike]:
        """
        GET /accounts/projects/{project_id}/secrets
        Returns [PullSecret | KeysSecret, …]
        """
        url = f"{self.base_url}/accounts/projects/{project_id}/secrets"
        async with self._session.get(url, headers=self._get_headers()) as resp:
            resp.raise_for_status()
            raw = await resp.json()
            return [_parse_secret(item) for item in raw["secrets"]]
