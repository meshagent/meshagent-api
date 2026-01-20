from pydantic import BaseModel, PositiveInt, ConfigDict, model_validator
from typing import Optional, Literal
from meshagent.api.participant_token import ApiScope
from meshagent.api.oauth import OAuthClientConfig
import json


class TokenValue(BaseModel):
    identity: str
    api: Optional[ApiScope] = None


class EnvironmentVariable(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    value: Optional[str] = None
    token: Optional[TokenValue] = None


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


class ImageStorageMountSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    image: str
    path: str
    subpath: Optional[str] = None
    read_only: bool = True


class ContainerMountSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    room: Optional[list[RoomStorageMountSpec]] = None
    project: Optional[list[ProjectStorageMountSpec]] = None
    images: Optional[list[ImageStorageMountSpec]] = None


class ServiceApiKeySpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    role: Literal["admin"]
    name: str
    auto_provision: Optional[bool] = True


ANNOTATION_SERVICE_ID = "meshagent.service.id"
ANNOTATION_SERVICE_README = "meshagent.service.readme"

ANNOTATION_AGENT_TYPE = "meshagent.agent.type"
ANNOTATION_AGENT_WIDGET = "meshagent.agent.widget"
ANNOTATION_AGENT_DATABASE_SCHEMA = "meshagent.agent.database.schema"
ANNOTATION_AGENT_SCHEDULE = "meshagent.agent.schedule"
ANNOTATION_AGENT_SHELL_COMMAND = "meshagent.agent.shell.command"

# events, adding this annotation to an agent's annotations will subscribe to the event
# the annotation's value should be the name of a queue to place the event into.
# use a worker agent to process the event
ANNOTATION_SERVICE_CREATED = "meshagent.events.service.created"
ANNOTATION_SERVICE_UPDATED = "meshagent.events.service.updated"

ANNOTATION_ROOM_USER_ADDED = "meshagent.events.room.user.grant.create"
ANNOTATION_ROOM_USER_REMOVED = "meshagent.events.room.user.grant.delete"
ANNOTATION_ROOM_USER_UPDATED = "meshagent.events.room.user.grant.update"

agent_type = Literal[
    "ChatBot",
    "VoiceBot",
    "Transcriber",
    "TaskRunner",
    "MailBot",
    "Worker",
    "Shell",
]


class AgentSpec(BaseModel):
    name: str
    description: Optional[str] = None
    annotations: Optional[dict[str, str]] = None


class ServiceMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    description: Optional[str] = None
    repo: Optional[str] = None
    icon: Optional[str] = None
    annotations: Optional[dict[str, str]] = None


class ContainerSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    image: str
    command: Optional[str] = None
    environment: Optional[list[EnvironmentVariable]] = None
    secrets: Optional[list[str]] = None
    pull_secret: Optional[str] = None
    storage: Optional[ContainerMountSpec] = None
    api_key: Optional[ServiceApiKeySpec] = None
    on_demand: Optional[bool] = None
    writable_root_fs: Optional[bool] = None


class ExternalServiceSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    url: Optional[str] = None


class ServiceSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: Literal["v1"]
    kind: Literal["Service"]
    id: Optional[str] = None
    metadata: ServiceMetadata
    agents: Optional[list[AgentSpec]] = None
    ports: Optional[list["PortSpec"]] = []
    container: Optional[ContainerSpec] = None
    external: Optional[ExternalServiceSpec] = None

    @model_validator(mode="after")
    def require_one_of(cls, m):
        if m.external is None and m.container is None:
            raise ValueError("Either 'external' or 'container' must be set")
        return m


class MeshagentEndpointSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    identity: str
    api: Optional[ApiScope] = None


class AllowedMcpToolFilter(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tool_names: list[str] = None
    read_only: Optional[bool] = None


class MCPEndpointSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    label: str
    description: Optional[str] = None
    allowed_tools: Optional[list[AllowedMcpToolFilter]] = None
    headers: Optional[dict[str, str]] = None
    require_approval: Optional[Literal["always", "never"]] = None
    oauth: Optional[OAuthClientConfig] = None
    openai_connector_id: Optional[str] = None


class EndpointSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    path: str
    meshagent: Optional[MeshagentEndpointSpec] = None
    mcp: Optional[MCPEndpointSpec] = None


class PortSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    num: Literal["*"] | PositiveInt = "*"
    type: Optional[Literal["http", "tcp"]] = "http"
    endpoints: list[EndpointSpec] = []
    liveness: Optional[str] = None
    host_port: Optional[PositiveInt] = None


class ServiceTemplateVariable(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    description: Optional[str] = None
    obscure: bool = False
    enum: Optional[list[str]] = None
    optional: bool = False
    # Optional hint for variable type; absent in many templates
    type: Optional[Literal["email"]] = None
    annotations: Optional[dict[str, str]] = None


class ServiceTemplateContainerMountSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    room: Optional[list[RoomStorageMountSpec]] = None
    project: Optional[list[ProjectStorageMountSpec]] = None
    images: Optional[list[ImageStorageMountSpec]] = None


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
    storage: Optional[ServiceTemplateContainerMountSpec] = None
    on_demand: Optional[bool] = None
    writable_root_fs: Optional[bool] = None


class ExternalServiceTemplateSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    url: str


class ServiceTemplateSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: Literal["v1"]
    kind: Literal["ServiceTemplate"]
    metadata: ServiceTemplateMetadata
    agents: Optional[list[AgentSpec]] = None
    variables: Optional[list[ServiceTemplateVariable]] = None
    ports: list[PortSpec] = []
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
            agents=self.agents,
            metadata=ServiceMetadata(
                name=self.metadata.name,
                description=self.metadata.description,
                repo=self.metadata.repo,
                icon=self.metadata.icon,
                annotations={
                    "meshagent.service.template.source": self.model_dump_json(),
                    "meshagent.service.template.values": json.dumps(values),
                    **self.metadata.annotations,
                },
            ),
            container=ContainerSpec(
                command=self.container.command,
                image=self.container.image,
                environment=env,
                storage=ContainerMountSpec(
                    room=self.container.storage.room
                    if self.container.storage is not None
                    else None,
                    project=self.container.storage.project
                    if self.container.storage is not None
                    else None,
                    images=self.container.storage.images
                    if self.container.storage is not None
                    else None,
                ),
                writeable_root_fs=self.container.writable_root_fs,
                on_demand=self.container.on_demand,
            )
            if self.container is not None
            else None,
            external=ExternalServiceSpec(
                url=self.external.url,
            )
            if self.external is not None
            else None,
            ports=self.ports,
        )
