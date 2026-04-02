import os
import jwt
from typing import Optional, List, Literal
from datetime import datetime
import json
from pydantic import BaseModel, Field
import logging
from .keys import parse_api_key
from .oauth import OAuthClientConfig, ConnectorRef
from .version import __version__

logger = logging.getLogger("participant-token")


def _normalize_namespace(namespace: Optional[list[str]]) -> tuple[str, ...]:
    return tuple(namespace or [])


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
    send: Optional[List[str]] = None
    receive: Optional[List[str]] = None
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
    namespace: Optional[list[str]] = None
    write: bool = False
    read: bool = True
    alter: bool = False


class DatabaseGrant(BaseModel):
    tables: Optional[list[TableGrant]] = None
    list_tables: bool = True

    def _matching_tables(
        self, *, table: str, namespace: Optional[list[str]]
    ) -> list[TableGrant]:
        if self.tables is None:
            return []

        requested_namespace = _normalize_namespace(namespace)
        matches = list[TableGrant]()
        for table_grant in self.tables:
            if table_grant.name != table:
                continue
            if table_grant.namespace is None:
                matches.append(table_grant)
                continue
            if _normalize_namespace(table_grant.namespace) == requested_namespace:
                matches.append(table_grant)

        return matches

    def can_write(self, table: str, *, namespace: Optional[list[str]] = None):
        if self.tables is None:
            return True

        matches = self._matching_tables(table=table, namespace=namespace)
        if len(matches) == 0:
            return False
        return any(table_grant.write for table_grant in matches)

    def can_read(self, table: str, *, namespace: Optional[list[str]] = None):
        if self.tables is None:
            return True

        matches = self._matching_tables(table=table, namespace=namespace)
        if len(matches) == 0:
            return False
        return any(table_grant.read for table_grant in matches)

    def can_alter(self, table: str, *, namespace: Optional[list[str]] = None):
        if self.tables is None:
            return True

        matches = self._matching_tables(table=table, namespace=namespace)
        if len(matches) == 0:
            return False
        return any(table_grant.alter for table_grant in matches)

    def can_access(self, table: str, *, namespace: Optional[list[str]] = None):
        return (
            self.can_read(table, namespace=namespace)
            or self.can_write(table, namespace=namespace)
            or self.can_alter(table, namespace=namespace)
        )


class MemoryPermissions(BaseModel):
    create: bool = True
    drop: bool = True
    inspect: bool = True
    query: bool = True
    upsert: bool = True
    ingest: bool = True
    recall: bool = True
    optimize: bool = True


class MemoryEntryGrant(BaseModel):
    name: str
    namespace: Optional[list[str]] = None
    permissions: MemoryPermissions = Field(default_factory=MemoryPermissions)


class MemoryGrant(BaseModel):
    list: bool = True
    memories: Optional[List[MemoryEntryGrant]] = None

    def _matching_memories(
        self, *, name: str, namespace: Optional[List[str]]
    ) -> List[MemoryEntryGrant]:
        if self.memories is None:
            return []

        requested_namespace = _normalize_namespace(namespace)
        matches = list[MemoryEntryGrant]()
        for memory_grant in self.memories:
            if memory_grant.name != name:
                continue
            if memory_grant.namespace is None:
                matches.append(memory_grant)
                continue
            if _normalize_namespace(memory_grant.namespace) == requested_namespace:
                matches.append(memory_grant)

        return matches

    def _can(
        self,
        *,
        name: str,
        namespace: Optional[List[str]],
        permission: Literal[
            "create",
            "drop",
            "inspect",
            "query",
            "upsert",
            "ingest",
            "recall",
            "optimize",
        ],
    ) -> bool:
        if self.memories is None:
            return True

        matches = self._matching_memories(name=name, namespace=namespace)
        if len(matches) == 0:
            return False

        for memory_grant in matches:
            permissions = memory_grant.permissions
            if permission == "create" and permissions.create:
                return True
            if permission == "drop" and permissions.drop:
                return True
            if permission == "inspect" and permissions.inspect:
                return True
            if permission == "query" and permissions.query:
                return True
            if permission == "upsert" and permissions.upsert:
                return True
            if permission == "ingest" and permissions.ingest:
                return True
            if permission == "recall" and permissions.recall:
                return True
            if permission == "optimize" and permissions.optimize:
                return True

        return False

    def can_create(self, *, name: str, namespace: Optional[List[str]] = None) -> bool:
        return self._can(
            name=name,
            namespace=namespace,
            permission="create",
        )

    def can_drop(self, *, name: str, namespace: Optional[List[str]] = None) -> bool:
        return self._can(
            name=name,
            namespace=namespace,
            permission="drop",
        )

    def can_inspect(self, *, name: str, namespace: Optional[List[str]] = None) -> bool:
        return self._can(
            name=name,
            namespace=namespace,
            permission="inspect",
        )

    def can_query(self, *, name: str, namespace: Optional[List[str]] = None) -> bool:
        return self._can(
            name=name,
            namespace=namespace,
            permission="query",
        )

    def can_upsert(self, *, name: str, namespace: Optional[List[str]] = None) -> bool:
        return self._can(
            name=name,
            namespace=namespace,
            permission="upsert",
        )

    def can_ingest(self, *, name: str, namespace: Optional[List[str]] = None) -> bool:
        return self._can(
            name=name,
            namespace=namespace,
            permission="ingest",
        )

    def can_recall(self, *, name: str, namespace: Optional[List[str]] = None) -> bool:
        return self._can(
            name=name,
            namespace=namespace,
            permission="recall",
        )

    def can_optimize(self, *, name: str, namespace: Optional[List[str]] = None) -> bool:
        return self._can(
            name=name,
            namespace=namespace,
            permission="optimize",
        )

    def can_access_existing(
        self, *, name: str, namespace: Optional[List[str]] = None
    ) -> bool:
        return (
            self.can_drop(name=name, namespace=namespace)
            or self.can_inspect(name=name, namespace=namespace)
            or self.can_query(name=name, namespace=namespace)
            or self.can_upsert(name=name, namespace=namespace)
            or self.can_ingest(name=name, namespace=namespace)
            or self.can_recall(name=name, namespace=namespace)
            or self.can_optimize(name=name, namespace=namespace)
        )


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
            if tag == t or (t.endswith("*") and tag.startswith(t.removesuffix("*"))):
                return True

        return False

    def can_run(self, tag: str):
        if self.run is None:
            return True

        for t in self.run:
            if tag == t or (t.endswith("*") and tag.startswith(t.removesuffix("*"))):
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
                authorization_endpoint = (
                    oauth.authorization_endpoint.strip()
                    if isinstance(oauth.authorization_endpoint, str)
                    else ""
                )
                client_id = (
                    oauth.client_id.strip() if isinstance(oauth.client_id, str) else ""
                )
                if authorization_endpoint == "" or client_id == "":
                    continue
                if (
                    t.endpoint == authorization_endpoint
                    or t.endpoint.endswith("*")
                    and authorization_endpoint.startswith(t.endpoint.removesuffix("*"))
                ) and t.client_id == client_id:
                    return True

        return False


class TunnelsGrant(BaseModel):
    ports: Optional[list[str]] = None


class ServicesGrant(BaseModel):
    list: bool = True


class ApiScope(BaseModel):
    livekit: Optional[LivekitGrant] = None
    queues: Optional[QueuesGrant] = None
    messaging: Optional[MessagingGrant] = None
    database: Optional[DatabaseGrant] = None
    memory: Optional[MemoryGrant] = None
    sync: Optional[SyncGrant] = None
    storage: Optional[StorageGrant] = None
    containers: Optional[ContainersGrant] = None
    developer: Optional[DeveloperGrant] = None
    agents: Optional[AgentsGrant] = None
    admin: Optional[AdminGrant] = None
    secrets: Optional[SecretsGrant] = None
    tunnels: Optional[TunnelsGrant] = None
    services: Optional[ServicesGrant] = None

    # no secrets access, no admin access by default for agents
    @staticmethod
    def agent_default(*, tunnels: bool = False) -> "ApiScope":
        return ApiScope(
            livekit=LivekitGrant(),
            queues=QueuesGrant(),
            messaging=MessagingGrant(),
            database=DatabaseGrant(),
            memory=MemoryGrant(),
            sync=SyncGrant(),
            storage=StorageGrant(),
            containers=ContainersGrant(),
            developer=DeveloperGrant(),
            agents=AgentsGrant(),
            services=ServicesGrant(),
            tunnels=TunnelsGrant() if tunnels else None,
        )

    @staticmethod
    def user_default() -> "ApiScope":
        return ApiScope(
            livekit=LivekitGrant(),
            queues=QueuesGrant(),
            messaging=MessagingGrant(),
            database=DatabaseGrant(),
            memory=MemoryGrant(),
            sync=SyncGrant(),
            storage=StorageGrant(),
            containers=ContainersGrant(),
            developer=DeveloperGrant(),
            agents=AgentsGrant(),
            secrets=SecretsGrant(),
            services=ServicesGrant(),
        )

    @staticmethod
    def full() -> "ApiScope":
        return ApiScope(
            livekit=LivekitGrant(),
            queues=QueuesGrant(),
            messaging=MessagingGrant(),
            database=DatabaseGrant(),
            memory=MemoryGrant(),
            sync=SyncGrant(),
            storage=StorageGrant(),
            containers=ContainersGrant(),
            developer=DeveloperGrant(),
            agents=AgentsGrant(),
            admin=AdminGrant(),
            secrets=SecretsGrant(),
            tunnels=TunnelsGrant(),
            services=ServicesGrant(),
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
        version: Optional[str] = None,
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
        return self.grant_scope("api")

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
            version = __version__

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
