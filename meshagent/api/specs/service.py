from pydantic import BaseModel, PositiveInt, ConfigDict
from typing import Optional, Literal
from meshagent.api.participant_token import ApiScope


class EnvironmentVariable(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    value: str


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


class ContainerSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    command: Optional[str] = None
    image: str
    environment: Optional[list[EnvironmentVariable]] = None
    secrets: list[str] = []
    pull_secret: Optional[str] = None
    storage: Optional[ServiceStorageMountsSpec] = None
    api_key: Optional[ServiceApiKeySpec] = None


class ExternalServiceSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    url: str


class ServiceSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: Literal["v1"]
    kind: Literal["Service"]
    id: Optional[str] = None
    metadata: ServiceMetadata
    ports: Optional[list["ServicePortSpec"]] = []
    container: Optional[ContainerSpec] = None
    external: Optional[ExternalServiceSpec] = None


class MeshagentEndpointSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    identity: str
    api: Optional[ApiScope] = None


class AllowedMcpToolFilter(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tool_names: list[str] = None
    read_only: Optional[bool] = None


class OAuthClientConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    client_id: str
    client_secret: Optional[str] = None
    authorization_endpoint: str
    token_endpoint: str
    no_pcke: Optional[bool] = None
    scopes: list[str] = None


class McpEndpointSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    label: str
    description: str
    allowed_tools: Optional[list[AllowedMcpToolFilter]] = None
    headers: Optional[dict[str, str]] = None
    require_approval: Optional[Literal["always", "never"]] = None
    oauth: Optional[OAuthClientConfig] = None
    openai_connector_id: Optional[str] = None


class ServicePortEndpointSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    path: str
    meshagent: Optional[MeshagentEndpointSpec] = None
    mcp: Optional[MeshagentEndpointSpec] = None


class ServicePortSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    num: Literal["*"] | PositiveInt
    type: Optional[Literal["http", "tcp"]] = None
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


class ContainerTemplateSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    environment: Optional[list[EnvironmentVariable]] = None
    image: Optional[str] = None
    command: Optional[str] = None
    storage: Optional[ServiceTemplateMountSpec] = None


class ExternalServiceTemplateSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    url: str


class ServiceTemplateSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: Literal["v1"]
    kind: Literal["ServiceTemplate"]
    metadata: ServiceTemplateMetadata
    variables: Optional[list[ServiceTemplateVariable]] = None
    ports: list[ServicePortSpec] = []
    container: Optional[ContainerTemplateSpec] = None
    external: Optional[ExternalServiceTemplateSpec] = None

    def to_service_spec(self, *, values: dict[str, str]) -> ServiceSpec:
        env = []
        if self.container is not None:
            if self.container.environment is not None:
                for e in self.container.environment:
                    env.append(
                        EnvironmentVariable(
                            name=e.name, value=e.value.format_map(values)
                        )
                    )

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
            container=ContainerSpec(
                command=self.container.command,
                image=self.container.image,
                environment=env,
                storage=ServiceStorageMountsSpec(
                    room=self.container.storage.room
                    if self.container.storage is not None
                    else None,
                    project=self.container.storage.project
                    if self.container.storage is not None
                    else None,
                ),
            )
            if self.container is not None
            else None,
            ports=self.ports,
        )
