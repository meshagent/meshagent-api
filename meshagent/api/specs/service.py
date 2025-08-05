from pydantic import BaseModel, PositiveInt
from typing import Optional, Literal
from datetime import datetime, timezone
from meshagent.api.accounts_client import (
    Port,
    Service,
    Endpoint,
    ServiceStorageMounts,
    RoomStorageMount,
    ProjectStorageMount,
)


class RoomStorageMountSpec(BaseModel):
    path: str
    subpath: Optional[str] = None
    read_only: bool = False


class ProjectStorageMountSpec(BaseModel):
    path: str
    subpath: Optional[str] = None
    read_only: bool = True


class ServiceStorageMountsSpec(BaseModel):
    room: Optional[list[RoomStorageMountSpec]] = None
    project: Optional[list[ProjectStorageMountSpec]] = None


class ServiceSpec(BaseModel):
    version: Literal["v1"]
    kind: Literal["Service"]
    id: Optional[str] = None
    name: str
    command: Optional[str] = None
    image: str
    ports: Optional[list["ServicePortSpec"]] = []
    role: Optional[Literal["user", "tool", "agent"]] = None
    environment: Optional[dict[str, str]] = {}
    secrets: list[str] = []
    pull_secret: Optional[str] = None
    storage: Optional[ServiceStorageMountsSpec] = None

    def to_service(self):
        ports = {}
        for p in self.ports:
            port = Port(liveness_path=p.liveness, type=p.type, endpoints=[])
            for endpoint in p.endpoints:
                type = port.type
                if endpoint.type is not None:
                    type = endpoint.type

                port.endpoints.append(
                    Endpoint(
                        type=type,
                        participant_name=endpoint.identity,
                        path=endpoint.path,
                        role=endpoint.role,
                    )
                )
            ports[str(p.num)] = port

        room_mounts = []
        if self.storage is not None and self.storage.room is not None:
            for rs in self.storage.room:
                room_mounts.append(
                    RoomStorageMount(
                        path=rs.path, subpath=rs.subpath, read_only=rs.read_only
                    )
                )

        project_mounts = []
        if self.storage is not None and self.storage.project is not None:
            for rs in self.storage.project:
                room_mounts.append(
                    ProjectStorageMount(
                        path=rs.path, subpath=rs.subpath, read_only=rs.read_only
                    )
                )

        return Service(
            id="",
            created_at=datetime.now(timezone.utc).isoformat(),
            name=self.name,
            command=self.command,
            image=self.image,
            ports=ports,
            role=self.role,
            environment=self.environment,
            environment_secrets=self.secrets,
            pull_secret=self.pull_secret,
            storage=ServiceStorageMounts(
                room=[*room_mounts], project=[*project_mounts]
            ),
        )


class ServicePortEndpointSpec(BaseModel):
    path: str
    identity: str
    role: Optional[Literal["user", "tool", "agent"]] = None
    type: Optional[Literal["mcp.sse", "meshagent.callable", "http", "tcp"]] = None


class ServicePortSpec(BaseModel):
    num: Literal["*"] | PositiveInt
    type: Optional[Literal["mcp.sse", "meshagent.callable", "http", "tcp"]] = None
    endpoints: list[ServicePortEndpointSpec] = []
    liveness: Optional[str] = None


class ServiceTemplateVariable(BaseModel):
    name: str
    description: Optional[str] = None
    obscure: bool = False
    enum: Optional[list[str]] = None
    optional: bool = False


class ServiceTemplateEnvironmentVariable(BaseModel):
    name: str
    value: str


class ServiceTemplateMountSpec(BaseModel):
    room: Optional[list[RoomStorageMountSpec]] = None


class ServiceTemplateMetadata(BaseModel):
    name: str
    description: Optional[str] = None
    repo: Optional[str] = None
    icon: Optional[str] = None


class ServiceTemplateSpec(BaseModel):
    version: Literal["v1"]
    kind: Literal["ServiceTemplate"]
    metadata: ServiceTemplateMetadata
    variables: Optional[list[ServiceTemplateVariable]] = None
    environment: Optional[list[ServiceTemplateEnvironmentVariable]] = None
    ports: list[ServicePortSpec] = []
    image: Optional[str] = None
    command: Optional[str] = None
    role: Optional[Literal["user", "tool", "agent"]] = None
    storage: Optional[ServiceTemplateMountSpec] = None

    def to_service_spec(self, *, values: dict[str, str]) -> ServiceSpec:
        env = {}
        if self.environment is not None:
            for e in self.environment:
                env[e.name] = e.value.format_map(values)

        return ServiceSpec(
            version=self.version,
            kind="Service",
            name=self.metadata.name,
            command=self.command,
            image=self.image,
            ports=self.ports,
            role=self.role,
            environment=env,
            storage=ServiceStorageMountsSpec(
                room=self.storage.room if self.storage is not None else None,
            ),
        )
