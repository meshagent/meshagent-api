from pydantic import BaseModel, PositiveInt
from typing import Optional, Literal
from meshagent.api.participant_token import ApiScope


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


class ServiceApiKeySpec(BaseModel):
    role: Literal["admin"]
    name: str
    auto_provision: Optional[bool] = True


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
    api_key: Optional[ServiceApiKeySpec] = None


class ServicePortEndpointSpec(BaseModel):
    path: str
    identity: str
    role: Optional[Literal["user", "tool", "agent"]] = None
    type: Optional[Literal["mcp.sse", "meshagent.callable", "http", "tcp"]] = None
    api: Optional[ApiScope] = None


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
    # Optional hint for variable type; absent in many templates
    type: Optional[Literal["email"]] = None


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
