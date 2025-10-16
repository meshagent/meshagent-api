from pydantic import BaseModel, PositiveInt, ConfigDict
from typing import Optional, Literal
from meshagent.api.participant_token import ApiScope


class RoomStorageMountSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    path: str
    subpath: Optional[str] = None
    read_only: bool = False


class ProjectStorageMountSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    path: str
    subpath: Optional[str] = None
    read_only: bool = True


class ServiceStorageMountsSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    room: Optional[list[RoomStorageMountSpec]] = None
    project: Optional[list[ProjectStorageMountSpec]] = None


class ServiceApiKeySpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    role: Literal["admin"]
    name: str
    auto_provision: Optional[bool] = True


class ServiceMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    description: Optional[str] = None
    repo: Optional[str] = None
    icon: Optional[str] = None
    annotations: Optional[dict[str, str]] = None


class ServiceSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: Literal["v1"]
    metadata: ServiceMetadata
    kind: Literal["Service"]
    id: Optional[str] = None
    command: Optional[str] = None
    image: str
    ports: Optional[list["ServicePortSpec"]] = []
    role: Optional[Literal["user", "tool", "agent"]] = None
    environment: Optional[dict[str, str]] = {}
    secrets: list[str] = []
    pull_secret: Optional[str] = None
    storage: Optional[ServiceStorageMountsSpec] = None
    api_key: Optional[ServiceApiKeySpec] = None


class ServicePortEndpointSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    path: str
    identity: str
    role: Optional[Literal["user", "tool", "agent"]] = None
    type: Optional[Literal["mcp.sse", "meshagent.callable", "http", "tcp"]] = None
    api: Optional[ApiScope] = None


class ServicePortSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    num: Literal["*"] | PositiveInt
    type: Optional[Literal["mcp.sse", "meshagent.callable", "http", "tcp"]] = None
    endpoints: list[ServicePortEndpointSpec] = []
    liveness: Optional[str] = None


class ServiceTemplateVariable(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    description: Optional[str] = None
    obscure: bool = False
    enum: Optional[list[str]] = None
    optional: bool = False
    # Optional hint for variable type; absent in many templates
    type: Optional[Literal["email"]] = None


class ServiceTemplateEnvironmentVariable(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    value: str


class ServiceTemplateMountSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    room: Optional[list[RoomStorageMountSpec]] = None
    project: Optional[list[ProjectStorageMountSpec]] = None


class ServiceTemplateMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    description: Optional[str] = None
    repo: Optional[str] = None
    icon: Optional[str] = None
    annotations: Optional[dict[str, str]] = None


class ServiceTemplateSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
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
            metadata=ServiceMetadata(
                name=self.metadata.name,
                description=self.metadata.description,
                repo=self.metadata.repo,
                icon=self.metadata.icon,
                annotations=self.metadata.annotations,
            ),
            command=self.command,
            image=self.image,
            ports=self.ports,
            role=self.role,
            environment=env,
            storage=ServiceStorageMountsSpec(
                room=self.storage.room if self.storage is not None else None,
                project=self.storage.project if self.storage is not None else None,
            ),
        )
