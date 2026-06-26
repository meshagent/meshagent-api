import os
import jwt
from typing import Optional, List, Literal
from datetime import datetime
from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    model_validator,
)
import logging
from .keys import parse_api_key
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


class DatasetGrant(BaseModel):
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


class SqliteTableGrant(BaseModel):
    database: str
    table: str
    namespace: Optional[list[str]] = None
    write: bool = False
    read: bool = True
    alter: bool = False


class SqliteDatabaseGrant(BaseModel):
    name: str
    namespace: Optional[list[str]] = None
    create_table: bool = True
    drop: bool = False
    inspect: bool = True
    list_tables: bool = True
    execute: bool = True
    tables: Optional[list[SqliteTableGrant]] = None


class SqliteGrant(BaseModel):
    databases: Optional[list[SqliteDatabaseGrant]] = None
    create_database: bool = True
    list_databases: bool = True

    def _matching_databases(
        self, *, database: str, namespace: Optional[list[str]]
    ) -> list[SqliteDatabaseGrant]:
        if self.databases is None:
            return []

        requested_namespace = _normalize_namespace(namespace)
        matches = list[SqliteDatabaseGrant]()
        for database_grant in self.databases:
            if database_grant.name != database:
                continue
            if database_grant.namespace is None:
                matches.append(database_grant)
                continue
            if _normalize_namespace(database_grant.namespace) == requested_namespace:
                matches.append(database_grant)

        return matches

    def _matching_tables(
        self,
        *,
        database: str,
        table: str,
        namespace: Optional[list[str]],
    ) -> list[SqliteTableGrant]:
        if self.databases is None:
            return []

        requested_namespace = _normalize_namespace(namespace)
        matches = list[SqliteTableGrant]()
        for database_grant in self._matching_databases(
            database=database,
            namespace=namespace,
        ):
            if database_grant.tables is None:
                continue
            for table_grant in database_grant.tables:
                if table_grant.database != database or table_grant.table != table:
                    continue
                if table_grant.namespace is None:
                    matches.append(table_grant)
                    continue
                if _normalize_namespace(table_grant.namespace) == requested_namespace:
                    matches.append(table_grant)

        return matches

    def can_create_database(self) -> bool:
        return self.create_database

    def can_list_databases(self) -> bool:
        return self.list_databases

    def can_drop_database(
        self, *, database: str, namespace: Optional[list[str]] = None
    ) -> bool:
        if self.databases is None:
            return True
        return any(
            database_grant.drop
            for database_grant in self._matching_databases(
                database=database,
                namespace=namespace,
            )
        )

    def can_inspect_database(
        self, *, database: str, namespace: Optional[list[str]] = None
    ) -> bool:
        if self.databases is None:
            return True
        return any(
            database_grant.inspect
            for database_grant in self._matching_databases(
                database=database,
                namespace=namespace,
            )
        )

    def can_list_tables(
        self, *, database: str, namespace: Optional[list[str]] = None
    ) -> bool:
        if self.databases is None:
            return True
        return any(
            database_grant.list_tables
            for database_grant in self._matching_databases(
                database=database,
                namespace=namespace,
            )
        )

    def can_create_table(
        self, *, database: str, namespace: Optional[list[str]] = None
    ) -> bool:
        if self.databases is None:
            return True
        return any(
            database_grant.create_table
            for database_grant in self._matching_databases(
                database=database,
                namespace=namespace,
            )
        )

    def can_execute(
        self, *, database: str, namespace: Optional[list[str]] = None
    ) -> bool:
        if self.databases is None:
            return True
        return any(
            database_grant.execute
            for database_grant in self._matching_databases(
                database=database,
                namespace=namespace,
            )
        )

    def can_read(
        self,
        *,
        database: str,
        table: str,
        namespace: Optional[list[str]] = None,
    ) -> bool:
        if self.databases is None:
            return True
        matches = self._matching_tables(
            database=database,
            table=table,
            namespace=namespace,
        )
        if len(matches) > 0:
            return any(table_grant.read for table_grant in matches)
        return any(
            database_grant.tables is None
            for database_grant in self._matching_databases(
                database=database,
                namespace=namespace,
            )
        )

    def can_write(
        self,
        *,
        database: str,
        table: str,
        namespace: Optional[list[str]] = None,
    ) -> bool:
        if self.databases is None:
            return True
        matches = self._matching_tables(
            database=database,
            table=table,
            namespace=namespace,
        )
        if len(matches) > 0:
            return any(table_grant.write for table_grant in matches)
        return any(
            database_grant.tables is None
            for database_grant in self._matching_databases(
                database=database,
                namespace=namespace,
            )
        )

    def can_alter(
        self,
        *,
        database: str,
        table: str,
        namespace: Optional[list[str]] = None,
    ) -> bool:
        if self.databases is None:
            return True
        matches = self._matching_tables(
            database=database,
            table=table,
            namespace=namespace,
        )
        if len(matches) > 0:
            return any(table_grant.alter for table_grant in matches)
        return any(
            database_grant.tables is None
            for database_grant in self._matching_databases(
                database=database,
                namespace=namespace,
            )
        )

    def can_access(
        self,
        *,
        database: str,
        table: str,
        namespace: Optional[list[str]] = None,
    ) -> bool:
        return (
            self.can_read(database=database, table=table, namespace=namespace)
            or self.can_write(database=database, table=table, namespace=namespace)
            or self.can_alter(database=database, table=table, namespace=namespace)
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


def _matches_grant_pattern(
    patterns: Optional[list[str]],
    value: str,
    *,
    allow_if_unset: bool,
) -> bool:
    if patterns is None:
        return allow_if_unset

    for pattern in patterns:
        if value == pattern or (
            pattern.endswith("*") and value.startswith(pattern.removesuffix("*"))
        ):
            return True

    return False


class ContainerRegistryGrant(BaseModel):
    list: Optional[List[str]] = None
    pull: Optional[List[str]] = None
    run: Optional[List[str]] = None
    write: Optional[List[str]] = None

    def can_list(self, repository: str) -> bool:
        if self.list is not None:
            return _matches_grant_pattern(
                self.list,
                repository,
                allow_if_unset=False,
            )

        if self.pull is None and self.run is None and self.write is None:
            return True

        return any(
            _matches_grant_pattern(patterns, repository, allow_if_unset=False)
            for patterns in (self.pull, self.run, self.write)
            if patterns is not None
        )

    def can_pull(self, repository: str) -> bool:
        return _matches_grant_pattern(
            self.pull,
            repository,
            allow_if_unset=True,
        )

    def can_run(self, repository: str) -> bool:
        return _matches_grant_pattern(
            self.run,
            repository,
            allow_if_unset=True,
        )

    def can_write(self, repository: str) -> bool:
        return _matches_grant_pattern(
            self.write,
            repository,
            allow_if_unset=True,
        )


class ContainersGrant(BaseModel):
    logs: bool = True

    pull: Optional[list[str]] = None
    run: Optional[list[str]] = None
    registry: Optional[ContainerRegistryGrant] = None

    use_containers: bool = True

    def can_pull(self, tag: str):
        return _matches_grant_pattern(
            self.pull,
            tag,
            allow_if_unset=True,
        )

    def can_run(self, tag: str):
        return _matches_grant_pattern(
            self.run,
            tag,
            allow_if_unset=True,
        )

    def can_registry_list(self, repository: str) -> bool:
        if self.registry is None:
            return True
        return self.registry.can_list(repository)

    def can_registry_pull(self, repository: str) -> bool:
        if self.registry is None:
            return True
        return self.registry.can_pull(repository)

    def can_registry_run(self, repository: str) -> bool:
        if self.registry is None:
            return True
        return self.registry.can_run(repository)

    def can_registry_write(self, repository: str) -> bool:
        if self.registry is None:
            return True
        return self.registry.can_write(repository)


class DeveloperGrant(BaseModel):
    logs: bool = True


class AdminGrant(BaseModel):
    config: bool = True


class SecretsGrant(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TunnelsGrant(BaseModel):
    ports: Optional[list[str]] = None


class ServicesGrant(BaseModel):
    list: bool = True


class LLMGrant(BaseModel):
    models: Optional[list[str]] = None

    def can_use_provider(self, provider: str) -> bool:
        normalized_provider = provider.strip()
        if normalized_provider == "":
            return False
        if self.models is None:
            return True

        prefix = normalized_provider + "/"
        for pattern in self.models:
            normalized_pattern = pattern.strip()
            if normalized_pattern.startswith(prefix):
                return True

        return False

    def can_use_model(self, *, provider: str, model: str) -> bool:
        normalized_provider = provider.strip()
        normalized_model = model.strip()
        if normalized_provider == "" or normalized_model == "":
            return False

        return _matches_grant_pattern(
            self.models,
            f"{normalized_provider}/{normalized_model}",
            allow_if_unset=True,
        )


class ApiScope(BaseModel):
    livekit: Optional[LivekitGrant] = None
    queues: Optional[QueuesGrant] = None
    messaging: Optional[MessagingGrant] = None
    dataset: Optional[DatasetGrant] = Field(
        default=None,
        validation_alias=AliasChoices("dataset", "database", "datasets"),
    )
    sqlite: Optional[SqliteGrant] = None
    memory: Optional[MemoryGrant] = None
    sync: Optional[SyncGrant] = None
    storage: Optional[StorageGrant] = None
    containers: Optional[ContainersGrant] = None
    developer: Optional[DeveloperGrant] = None
    agents: Optional[AgentsGrant] = None
    llm: Optional[LLMGrant] = None
    admin: Optional[AdminGrant] = None
    secrets: Optional[SecretsGrant] = None
    tunnels: Optional[TunnelsGrant] = None
    services: Optional[ServicesGrant] = None

    # no admin access by default for agents
    @staticmethod
    def agent_default(*, tunnels: bool = False) -> "ApiScope":
        return ApiScope(
            livekit=LivekitGrant(),
            queues=QueuesGrant(),
            messaging=MessagingGrant(),
            dataset=DatasetGrant(),
            sqlite=SqliteGrant(),
            memory=MemoryGrant(),
            sync=SyncGrant(),
            storage=StorageGrant(),
            containers=ContainersGrant(),
            developer=DeveloperGrant(),
            agents=AgentsGrant(),
            llm=LLMGrant(),
            services=ServicesGrant(),
            tunnels=TunnelsGrant() if tunnels else None,
        )

    @staticmethod
    def user_default() -> "ApiScope":
        return ApiScope(
            livekit=LivekitGrant(),
            queues=QueuesGrant(),
            messaging=MessagingGrant(),
            dataset=DatasetGrant(),
            sqlite=SqliteGrant(),
            memory=MemoryGrant(),
            sync=SyncGrant(),
            storage=StorageGrant(),
            containers=ContainersGrant(),
            developer=DeveloperGrant(),
            agents=AgentsGrant(),
            services=ServicesGrant(),
        )

    @staticmethod
    def full() -> "ApiScope":
        return ApiScope(
            livekit=LivekitGrant(),
            queues=QueuesGrant(),
            messaging=MessagingGrant(),
            dataset=DatasetGrant(),
            sqlite=SqliteGrant(),
            memory=MemoryGrant(),
            sync=SyncGrant(),
            storage=StorageGrant(),
            containers=ContainersGrant(),
            developer=DeveloperGrant(),
            agents=AgentsGrant(),
            llm=LLMGrant(),
            admin=AdminGrant(),
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
        if not isinstance(data, dict):
            raise ValueError("participant grant must be a JSON object")
        if "name" not in data:
            raise ValueError("participant grant is missing required field: name")
        if "scope" not in data:
            raise ValueError("participant grant is missing required field: scope")

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

    def add_agent_grant(self, agent_name: str):
        self.grants.append(ParticipantGrant(name="agent", scope=agent_name))

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
        extra_payload = (
            self.extra_payload.copy() if self.extra_payload is not None else {}
        )
        j = {
            **extra_payload,
            "name": self.name,
            "grants": [g.to_json() for g in self.grants],
        }

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
        elif token is None:
            if "kid" in payload:
                payload.pop("kid")

        if token is None:
            token = os.getenv("MESHAGENT_SECRET")

        return jwt.encode(
            payload={**extra_payload, **payload}, key=token, algorithm="HS256"
        )

    @staticmethod
    def from_json(data: dict) -> "ParticipantToken":
        if not isinstance(data, dict):
            raise ValueError("participant token must be a JSON object")

        data = data.copy()
        if "name" not in data:
            raise ValueError("participant token is missing required field: name")
        if "grants" not in data:
            raise ValueError("participant token is missing required field: grants")

        name = data.pop("name")
        grants = data.pop("grants")
        if not isinstance(name, str) or name.strip() == "":
            raise ValueError("participant token field must be a non-empty string: name")
        if not isinstance(grants, list):
            raise ValueError("participant token field must be a list: grants")

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

        try:
            return ParticipantToken.from_json(decoded)
        except (KeyError, TypeError, ValueError, ValidationError) as exc:
            raise jwt.InvalidTokenError(
                "JWT payload is not a valid participant token"
            ) from exc


class ParticipantTokenSpec(BaseModel):
    version: Literal["v1"]
    kind: Literal["ParticipantToken"]
    room: Optional[str] = None
    identity: str
    role: Optional[Literal["user", "agent", "tool"]] = None
    api: ApiScope

    @model_validator(mode="after")
    def _ensure_llm_grant(self) -> "ParticipantTokenSpec":
        if self.api.llm is None:
            raise ValueError("Participant token api scope must include an llm grant.")
        return self
