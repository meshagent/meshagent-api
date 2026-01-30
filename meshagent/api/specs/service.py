from pydantic import BaseModel, PositiveInt, ConfigDict, Field, model_validator
from typing import Optional, Literal
from meshagent.api.participant_token import ApiScope
from meshagent.api.oauth import OAuthClientConfig
import json

import yaml as YAML
from yaml.loader import SafeLoader


class TokenValue(BaseModel):
    identity: str = Field(..., description="the name to use in the participant token")
    api: Optional[ApiScope] = Field(
        None,
        description=(
            "the api permissions that should be granted to this token, set to null "
            "or omit to use default permissions"
        ),
    )
    role: Optional[str] = Field(
        None,
        description="a role to use in the participant token, such as user, agent, or tool",
    )


class EnvironmentVariable(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    value: Optional[str] = None
    token: Optional[TokenValue] = None


class RoomStorageMountSpec(BaseModel):
    """mounts room storage at the specified path using a FUSE mount"""

    model_config = ConfigDict(extra="forbid")
    path: str = Field(
        ...,
        description="the path within the container for the room's storage to be mounted to",
    )
    subpath: Optional[str] = Field(
        None, description="mount only a portion of the rooms storage"
    )
    read_only: bool = False


class ProjectStorageMountSpec(BaseModel):
    """mounts shared project storage at the specified path using a FUSE mount"""

    model_config = ConfigDict(extra="forbid")
    path: str = Field(
        ...,
        description="the path within the container for the project storage to be mounted to",
    )
    subpath: Optional[str] = Field(
        None, description="mount only a portion of the project's storage"
    )
    read_only: bool = True


class ImageStorageMountSpec(BaseModel):
    """mounts a the content of a Docker / OCI image at the specified path within the container"""

    model_config = ConfigDict(extra="forbid")
    image: str = Field(..., description="the tag of an image that will be mounted")
    path: str = Field(
        ...,
        description="the path within the container for the image volume to be mounted to",
    )
    subpath: Optional[str] = Field(
        None, description="mount only a portion of the image volume"
    )
    read_only: bool = True


class FileStorageMountSpec(BaseModel):
    """mounts a static file into the container at the specified path"""

    model_config = ConfigDict(extra="forbid")
    path: str
    text: str
    read_only: bool = True


class ContainerMountSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    room: Optional[list[RoomStorageMountSpec]] = None
    project: Optional[list[ProjectStorageMountSpec]] = None
    images: Optional[list[ImageStorageMountSpec]] = None
    files: Optional[list[FileStorageMountSpec]] = None


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
    secrets: Optional[list[str]] = Field(
        None,
        description="ids of secrets that contains environment variables for this service to use",
    )
    pull_secret: Optional[str] = Field(
        None,
        description=(
            "the id of a pull secret, can be used to pull private container images"
        ),
    )
    storage: Optional[ContainerMountSpec] = Field(
        None, description="storage mounts that should be provided to this container"
    )
    api_key: Optional[ServiceApiKeySpec] = None
    on_demand: Optional[bool] = Field(None, description="an on demand service")
    writable_root_fs: Optional[bool] = None


class ExternalServiceSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    url: Optional[str] = None


class ServiceSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: Literal["v1"]
    kind: Literal["Service"]
    id: Optional[str] = None
    metadata: ServiceMetadata = Field(..., description="service metadata")
    agents: Optional[list[AgentSpec]] = Field(
        None, description="a list of agents that will be exposed by this service"
    )
    ports: Optional[list["PortSpec"]] = Field(
        default_factory=list,
        description="a list of ports that are exposed by this service",
    )
    container: Optional[ContainerSpec] = Field(
        None,
        description=(
            "container based services run agents in sandboxed containers inside the room"
        ),
    )
    external: Optional[ExternalServiceSpec] = Field(
        None,
        description=(
            "external services allow discovery of externally hosted agents, mcp servers, and tools"
        ),
    )

    @model_validator(mode="after")
    def require_one_of(cls, m):
        if m.external is None and m.container is None:
            raise ValueError("Either 'external' or 'container' must be set")
        return m

    @staticmethod
    def from_yaml(yaml: str) -> "ServiceSpec":
        return ServiceSpec.model_validate(YAML.safe_load(yaml))


class MeshagentEndpointSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    identity: str = Field(
        ...,
        description="the name to use for the participant token provided to this endpoint",
    )
    api: Optional[ApiScope] = Field(
        None,
        description=(
            "customize the permissions available to this endpoint, omit to use default agent permissions"
        ),
    )


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
    path: str = Field(
        ...,
        description="the path that should receive a webhook call when the service starts",
    )
    meshagent: Optional[MeshagentEndpointSpec] = Field(
        None,
        description=(
            "meshagent endpoints will be automatically notified when the service starts in order to call an agent or tool into the room"
        ),
    )
    mcp: Optional[MCPEndpointSpec] = None


class PortSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    num: Literal["*"] | PositiveInt = "*"
    type: Optional[Literal["http", "tcp"]] = "http"
    endpoints: list[EndpointSpec] = Field(
        default_factory=list, description="a list of endpoints exposed under this port"
    )
    liveness: Optional[str] = Field(
        None,
        description=(
            "a path that will accept a HTTP request and should return 200 when the port is live"
        ),
    )
    host_port: Optional[PositiveInt] = Field(
        None,
        description=(
            "expose a host port for this service, allows traffic to be tunneled to the container with port forwarding"
        ),
    )
    published: Optional[bool] = Field(
        None,
        description=(
            "allow traffic to be routed directly to this container from the internet, useful for implementing patterns such as webhooks"
        ),
    )
    public: Optional[bool] = Field(
        None,
        description=(
            "if a port is not public it will require a participant token to be passed as a Bearer token in the Authorization header"
        ),
    )


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
    files: Optional[list[FileStorageMountSpec]] = None


class ServiceTemplateMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    description: Optional[str] = None
    repo: Optional[str] = None
    icon: Optional[str] = None
    annotations: Optional[dict[str, str]] = None


class TemplateEnvironmentVariable(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    value: Optional[str] = None
    token: Optional[TokenValue] = None


class AgentTemplateSpec(BaseModel):
    name: str
    description: Optional[str] = None
    annotations: Optional[dict[str, str]] = None


class ContainerTemplateSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    environment: Optional[list[TemplateEnvironmentVariable]] = None
    image: Optional[str] = None
    command: Optional[str] = None
    storage: Optional[ServiceTemplateContainerMountSpec] = None
    on_demand: Optional[bool] = None
    writable_root_fs: Optional[bool] = None


class ExternalServiceTemplateSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    url: str


def format_yaml_value(original: Optional[str], values: dict[str, str]):
    if original is None:
        return None

    if original.startswith("!template "):
        from jinja2 import Template

        template = Template(original.removeprefix("!template "))
        return template.render(**values)

    else:
        return original


def format_yaml_map(original: Optional[dict[str, str]], values: dict[str, str]):
    if original is None:
        return None
    output = {}

    for k, v in original.items():
        output[k] = format_yaml_value(v, values)

    return output


class ServiceTemplateSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: Literal["v1"]
    kind: Literal["ServiceTemplate"]
    metadata: ServiceTemplateMetadata
    agents: Optional[list[AgentTemplateSpec]] = None
    variables: Optional[list[ServiceTemplateVariable]] = None
    ports: Optional[list[PortSpec]] = None
    container: Optional[ContainerTemplateSpec] = None
    external: Optional[ExternalServiceTemplateSpec] = None

    def to_service_spec(self) -> ServiceSpec:
        env = []
        if self.container is not None:
            if self.container.environment is not None:
                for e in self.container.environment:
                    env.append(
                        EnvironmentVariable(
                            name=e.name,
                            value=e.value,
                            token=e.token,
                        )
                    )

        return ServiceSpec(
            version=self.version,
            kind="Service",
            agents=[
                *(
                    AgentSpec(
                        name=a.name,
                        description=a.description,
                        annotations=a.annotations,
                    )
                    for a in self.agents
                )
            ]
            if self.agents is not None
            else None,
            metadata=ServiceMetadata(
                name=self.metadata.name,
                description=self.metadata.description,
                repo=self.metadata.repo,
                icon=self.metadata.icon,
                annotations={
                    **(self.metadata.annotations or {}),
                },
            ),
            container=ContainerSpec(
                command=self.container.command,
                image=self.container.image,
                environment=env,
                storage=ContainerMountSpec(
                    room=self.container.storage.room,
                    project=self.container.storage.project,
                    images=self.container.storage.images,
                    files=self.container.storage.files,
                )
                if self.container.storage is not None
                else None,
                writable_root_fs=self.container.writable_root_fs,
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

    @staticmethod
    def from_yaml(yaml: str, values: dict[str, str] = {}) -> "ServiceTemplateSpec":
        from jinja2 import Template

        class _ApplyTagLoader(SafeLoader):
            pass

        def _tagged_scalar(loader, tag_suffix, node):
            value = loader.construct_scalar(node)
            template = Template(value)
            return template.render(**values)

        _ApplyTagLoader.add_multi_constructor("!template", _tagged_scalar)

        def load_yaml(y: str):
            return YAML.load(y, Loader=_ApplyTagLoader)

        template = Template(yaml)

        rendered = template.render(**values)

        spec = ServiceTemplateSpec.model_validate(load_yaml(rendered))

        if spec.metadata.annotations is None:
            spec.metadata.annotations = {}

        spec.metadata.annotations["meshagent.service.template.yaml"] = yaml

        spec.metadata.annotations["meshagent.service.template.values"] = json.dumps(
            values
        )

        return spec
