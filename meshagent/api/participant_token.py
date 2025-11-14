import os
import jwt
from typing import Optional, List, Literal
from datetime import datetime
import json
from pydantic import BaseModel
import logging
from .keys import parse_api_key
from .oauth import OAuthClientConfig, ConnectorRef
from .version import __version__

logger = logging.getLogger("participant-token")


class AgentsGrant(BaseModel):
    register_agent: bool = True
    register_public_toolkit: bool = True
    register_private_toolkit: bool = True
    call: bool = True
    use_agents: bool = True
    use_tools: bool = True
    allowed_toolkits: Optional[list[str]] = None


class LivekitGrant(BaseModel):
    breakout_rooms: Optional[list[str]] = None

    def can_join_breakout_room(self, name: str):
        return self.breakout_rooms is None or name in self.breakout_rooms


class QueuesGrant(BaseModel):
    send: Optional[list[str]] = None
    receive: Optional[list[str]] = None
    list: bool = True

    def can_send(self, queue: str):
        return self.send is None or queue in self.send

    def can_receive(self, queue: str):
        return self.receive is None or queue in self.receive


class MessagingGrant(BaseModel):
    broadcast: bool = True
    list: bool = True
    send: bool = True


class TableGrant(BaseModel):
    name: str
    write: bool = False
    read: bool = True
    alter: bool = False


class DatabaseGrant(BaseModel):
    tables: Optional[list[TableGrant]] = None
    list_tables: bool = True

    def can_write(self, table: str):
        if self.tables is None:
            return True

        for t in self.tables:
            if t.name == table:
                return t.write

        return False

    def can_read(self, table: str):
        if self.tables is None:
            return True

        for t in self.tables:
            if t.name == table:
                return t.read

        return False

    def can_alter(self, table: str):
        if self.tables is None:
            return True

        for t in self.tables:
            if t.name == table:
                return t.alter

        return False


class SyncPathGrant(BaseModel):
    path: str
    read_only: bool = False


class SyncGrant(BaseModel):
    paths: Optional[list[SyncPathGrant]] = None

    def can_read(self, path: str):
        if self.paths is None:
            return True

        for t in self.paths:
            if (
                t.path == path
                or t.path.endswith("*")
                and path.startswith(t.path.removesuffix("*"))
            ):
                return True

        return False

    def can_write(self, path: str):
        if self.paths is None:
            return True

        for t in self.paths:
            if (
                t.path == path
                or t.path.endswith("*")
                and path.startswith(t.path.removesuffix("*"))
            ):
                return not t.read_only

        return False


class StoragePathGrant(BaseModel):
    path: str
    read_only: bool = False


class StorageGrant(BaseModel):
    paths: Optional[list[StoragePathGrant]] = None

    def can_read(self, path: str):
        if self.paths is None:
            return True

        for t in self.paths:
            if path.startswith(t.path):
                return True

        return False

    def can_write(self, path: str):
        if self.paths is None:
            return True

        for t in self.paths:
            if path.startswith(t.path):
                return not t.read_only

        return False


class ContainersGrant(BaseModel):
    logs: bool = True

    pull: Optional[list[str]] = None
    run: Optional[list[str]] = None

    use_containers: bool = True

    def can_pull(self, tag: str):
        if self.pull is None:
            return True

        for t in self.pull:
            if tag == t or tag.startswith(t.removesuffix("*")):
                return True

        return False

    def can_run(self, tag: str):
        if self.run is None:
            return True

        for t in self.run:
            if tag == t or tag.startswith(t.removesuffix("*")):
                return True

        return False


class DeveloperGrant(BaseModel):
    logs: bool = True


class AdminGrant(BaseModel):
    config: bool = True


class OAuthEndpoint(BaseModel):
    endpoint: str
    client_id: str


class SecretsGrant(BaseModel):
    request_oauth_token: Optional[list[OAuthEndpoint]] = None

    def can_request_oauth_token(
        self,
        *,
        connector: Optional[ConnectorRef] = None,
        oauth: Optional[OAuthClientConfig],
    ):
        if self.request_oauth_token is None:
            return True

        for t in self.request_oauth_token:
            if oauth is not None:
                if (
                    t.endpoint == oauth.authorization_endpoint
                    or t.endpoint.endswith("*")
                    and oauth.authorization_endpoint.startswith(
                        t.endpoint.removesuffix("*")
                    )
                ) and t.client_id == oauth.authorization_endpoint.client_id:
                    return True

        return False


class ApiScope(BaseModel):
    livekit: Optional[LivekitGrant] = None
    queues: Optional[QueuesGrant] = None
    messaging: Optional[MessagingGrant] = None
    database: Optional[DatabaseGrant] = None
    sync: Optional[SyncGrant] = None
    storage: Optional[StorageGrant] = None
    containers: Optional[ContainersGrant] = None
    developer: Optional[DeveloperGrant] = None
    agents: Optional[AgentsGrant] = None
    admin: Optional[AdminGrant] = None
    secrets: Optional[SecretsGrant] = None

    # no secrets access, no admin access by default for agents
    @staticmethod
    def agent_default() -> "ApiScope":
        return ApiScope(
            livekit=LivekitGrant(),
            queues=QueuesGrant(),
            messaging=MessagingGrant(),
            database=DatabaseGrant(),
            sync=SyncGrant(),
            storage=StorageGrant(),
            containers=ContainersGrant(),
            developer=DeveloperGrant(),
            agents=AgentsGrant(),
        )

    @staticmethod
    def full() -> "ApiScope":
        return ApiScope(
            livekit=LivekitGrant(),
            queues=QueuesGrant(),
            messaging=MessagingGrant(),
            database=DatabaseGrant(),
            sync=SyncGrant(),
            storage=StorageGrant(),
            containers=ContainersGrant(),
            developer=DeveloperGrant(),
            agents=AgentsGrant(),
            admin=AdminGrant(),
            secrets=SecretsGrant(),
        )


class ParticipantGrant:
    def __init__(self, *, name: str, scope: Optional[str | ApiScope] = None):
        self.name = name
        self.scope = scope

    def to_json(self) -> dict:
        return {
            "name": self.name,
            "scope": self.scope.model_dump(
                mode="json", exclude_none=True, exclude_defaults=True
            )
            if self.name == "api"
            else self.scope,
        }

    @staticmethod
    def from_json(data: dict) -> "ParticipantGrant":
        if data["name"] == "api":
            scope = ApiScope.model_validate(data["scope"])
        else:
            scope = data["scope"]

        return ParticipantGrant(name=data["name"], scope=scope)


class ParticipantToken:
    def __init__(
        self,
        *,
        name: str,
        project_id: Optional[str] = None,
        api_key_id: str = None,
        grants: Optional[List[ParticipantGrant]] = None,
        extra_payload: Optional[dict] = None,
        version: Optional[None] = None,
    ):
        if grants is None:
            grants = []

        if version is None:
            version = __version__

        self.name = name
        self.grants = grants
        self.project_id = project_id
        self.api_key_id = api_key_id
        self.extra_payload = extra_payload
        self.version = version

    @property
    def role(self):
        for grant in self.grants:
            if grant.name == "role" and grant.scope != "user":
                return grant.scope

        return "user"

    @property
    def is_user(self):
        for grant in self.grants:
            if grant.name == "role" and grant.scope != "user":
                return False

        return True

    def add_tunnel_grant(self, ports: list[int]):
        ports_str = ",".join(ports)
        self.grants.append(ParticipantGrant(name="tunnel_ports", scope=ports_str))

    def add_role_grant(self, role: str):
        self.grants.append(ParticipantGrant(name="role", scope=role))

    def add_room_grant(self, room_name: str):
        self.grants.append(ParticipantGrant(name="room", scope=room_name))

    def add_api_grant(self, grant: ApiScope):
        for g in self.grants:
            if g.name == "api":
                raise ValueError("can only have a single api grant")

        self.grants.append(ParticipantGrant(name="api", scope=grant))

    def grant_scope(self, name: str) -> str | ApiScope | None:
        for g in self.grants:
            if g.name == name:
                return g.scope

        return None

    def get_api_grant(self) -> ApiScope | None:
        api = self.grant_scope("api")
        if self.version < "0.6.0" and api is None:
            # <= 0.6.0 did not use fine grained tokens and should default api access on
            return ApiScope(
                livekit=LivekitGrant(),
                queues=QueuesGrant(),
                messaging=MessagingGrant(),
                database=DatabaseGrant(),
                sync=SyncGrant(),
                storage=StorageGrant(),
                agents=AgentsGrant(),
                developer=DeveloperGrant(),
                # TODO: this should be removed so you have to use fine grained tokens to enable, temp hack to unblock powerboards
                containers=ContainersGrant(),
            )

        return api

    def to_json(self) -> dict:
        j = {"name": self.name, "grants": [g.to_json() for g in self.grants]}

        if self.project_id is not None:
            j["sub"] = self.project_id

        if self.api_key_id is not None:
            j["kid"] = self.api_key_id

        if self.version is not None:
            j["version"] = self.version

        return j

    def to_jwt(
        self,
        *,
        token: Optional[str] = None,
        expiration: Optional[datetime] = None,
        api_key: Optional[str] = None,
    ) -> str:
        api_grant = None
        for g in self.grants:
            if g.name == "api":
                api_grant = g
                break

        if api_grant is None and self.version > "0.3.5":
            logger.warning(
                "there is no ApiScope in the participant token, this participant will not be able to make calls to the the room API. Use add_api_grant to add an ApiScope to this token."
            )

        extra_payload = self.extra_payload
        if extra_payload is None:
            extra_payload = {}
        else:
            extra_payload = extra_payload.copy()

        if expiration is not None:
            extra_payload["exp"] = expiration

        payload = self.to_json()
        if api_key is None:
            api_key = os.getenv("MESHAGENT_API_KEY")

        if api_key is not None:
            parsed = parse_api_key(api_key)
            token = parsed.secret
            payload["kid"] = parsed.id
            payload["sub"] = parsed.project_id

        if token is None:
            token = os.getenv("MESHAGENT_SECRET")
            if "kid" in payload:
                # We are exporting a token using the default secret, so we should remove the key id
                payload.pop("kid")

        return jwt.encode(
            payload={**extra_payload, **payload}, key=token, algorithm="HS256"
        )

    @staticmethod
    def from_json(data: dict) -> "ParticipantToken":
        data = data.copy()
        if "name" not in data:
            raise Exception(
                f"Participant token does not have a name {json.dumps(data)}"
            )

        name = data.pop("name")
        grants = data.pop("grants")
        project_id = None
        api_key_id = None

        if "sub" in data:
            project_id = data.pop("sub")

        if "kid" in data:
            api_key_id = data.pop("kid")

        if "version" in data:
            version = data.pop("version")

        else:
            # did not encode a version prior to 0.5.3
            version = "0.5.3"

        return ParticipantToken(
            name=name,
            project_id=project_id,
            api_key_id=api_key_id,
            grants=[ParticipantGrant.from_json(g) for g in grants],
            extra_payload=data,
            version=version,
        )

    @staticmethod
    def from_jwt(
        jwt_str: str, *, token: Optional[str] = None, validate: Optional[bool] = True
    ) -> "ParticipantToken":
        if token is None:
            token = os.getenv("MESHAGENT_SECRET")

        if validate:
            decoded = jwt.decode(jwt=jwt_str, key=token, algorithms=["HS256"])
        else:
            decoded = jwt.decode(jwt=jwt_str, options={"verify_signature": False})

        return ParticipantToken.from_json(decoded)


class ParticipantTokenSpec(BaseModel):
    version: Literal["v1"]
    kind: Literal["ParticipantToken"]
    room: Optional[str] = None
    identity: str
    role: Optional[Literal["user", "agent", "tool"]] = None
    api: ApiScope
