from pydantic import (
    BaseModel,
    PositiveInt,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)
from typing import Any, Optional, Literal
from datetime import datetime, timezone
import re
from croniter import CroniterBadCronError, croniter
from meshagent.api.participant_token import ApiScope
from meshagent.api.oauth import OAuthClientConfig
from meshagent.api.agent_content import AgentInputContent
from meshagent.api.room_ports import RESERVED_ROOM_SERVICE_PORTS
import json


MIN_SCHEDULED_TASK_INTERVAL_SECONDS = 15 * 60
ContainerTemplate = Literal["agent", "none"]
_SCHEDULE_INTERVAL_RE = re.compile(
    r"^\s*(?P<count>\d+)\s+(?P<unit>second|seconds|minute|minutes|hour|hours|day|days)\s*$",
    re.IGNORECASE,
)


def _scheduled_task_interval_seconds(schedule: str) -> float:
    interval_match = _SCHEDULE_INTERVAL_RE.match(schedule)
    if interval_match is not None:
        count = int(interval_match.group("count"))
        unit = interval_match.group("unit").lower()
        multipliers = {
            "second": 1,
            "seconds": 1,
            "minute": 60,
            "minutes": 60,
            "hour": 60 * 60,
            "hours": 60 * 60,
            "day": 24 * 60 * 60,
            "days": 24 * 60 * 60,
        }
        return count * multipliers[unit]

    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    try:
        iterator = croniter(schedule, base)
        previous = iterator.get_next(datetime)
        min_interval: float | None = None
        for _ in range(100):
            current = iterator.get_next(datetime)
            interval = (current - previous).total_seconds()
            min_interval = (
                interval if min_interval is None else min(min_interval, interval)
            )
            previous = current
    except CroniterBadCronError as exc:
        raise ValueError(f"unsupported schedule: {schedule}") from exc

    if min_interval is None:
        raise ValueError(f"unsupported schedule: {schedule}")
    return min_interval


def _yaml_support():
    import yaml as YAML
    from yaml.loader import SafeLoader

    return YAML, SafeLoader


class SecretValue(BaseModel):
    identity: str = Field(..., description="the identity for the secret")
    id: str = Field(..., description="the id of the secret")


class TokenValue(BaseModel):
    identity: str = Field(..., description="the name to use in the participant token")
    api: Optional[ApiScope] = Field(
        None,
        description=(
            "the api permissions that should be granted to this token, set to null "
            "or omit to use the default permissions implied by role"
        ),
    )
    role: Optional[Literal["user", "agent", "tool"]] = Field(
        None,
        description="a role to use in the participant token, such as user, agent, or tool",
    )

    def resolved_api_scope(self) -> ApiScope:
        if self.api is not None:
            return self.api
        if self.role == "user":
            return ApiScope.user_default()
        return ApiScope.agent_default()


class EnvironmentVariable(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    value: Optional[str] = None
    token: Optional[TokenValue] = None
    secret: Optional[SecretValue] = None


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


class EmptyDirMountSpec(BaseModel):
    """mounts a writable temporary directory into the container at the specified path"""

    model_config = ConfigDict(extra="forbid")
    path: str
    read_only: bool = False


class ConfigMountSpec(BaseModel):
    """mounts meshagent runtime config files read-only into the specified folder"""

    model_config = ConfigDict(extra="forbid")
    path: str = Field(
        "/var/run/meshagent",
        description=(
            "the folder within the container where meshagent runtime files such as "
            "spec.json and members.json should be mounted"
        ),
    )


class ContainerMountSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    room: Optional[list[RoomStorageMountSpec]] = None
    project: Optional[list[ProjectStorageMountSpec]] = None
    images: Optional[list[ImageStorageMountSpec]] = None
    files: Optional[list[FileStorageMountSpec]] = None
    empty_dirs: Optional[list[EmptyDirMountSpec]] = None
    configs: Optional[list[ConfigMountSpec]] = None


class ServiceApiKeySpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    role: Literal["admin"]
    name: str
    auto_provision: Optional[bool] = True


class ServiceFileSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    path: str
    text: str


ANNOTATION_SERVICE_ID = "meshagent.service.id"
ANNOTATION_SERVICE_README = "meshagent.service.readme"

ANNOTATION_AGENT_TYPE = "meshagent.agent.type"
ANNOTATION_AGENT_WIDGET = "meshagent.agent.widget"
ANNOTATION_AGENT_DATASET_SCHEMA = "meshagent.agent.dataset.schema"
ANNOTATION_AGENT_SCHEDULE = "meshagent.agent.schedule"
ANNOTATION_AGENT_HEARTBEAT = "meshagent.agent.heartbeat"
ANNOTATION_AGENT_SHELL_COMMAND = "meshagent.agent.shell.command"

# events, adding this annotation to an agent's annotations will subscribe to the event
# the annotation's value should be the name of a queue to place the event into.
# use a worker agent to process the event
ANNOTATION_SERVICE_CREATED = "meshagent.events.service.created"
ANNOTATION_SERVICE_UPDATED = "meshagent.events.service.updated"

ANNOTATION_ROOM_USER_ADDED = "meshagent.events.room.user.grant.create"
ANNOTATION_ROOM_USER_REMOVED = "meshagent.events.room.user.grant.delete"
ANNOTATION_ROOM_USER_UPDATED = "meshagent.events.room.user.grant.update"

ANNOTATION_REQUEST_PROCESSOR = "meshagent.request.processor"
ANNOTATION_REQUEST_QUEUE = "meshagent.request.queue"
ANNOTATION_REQUEST_VALIDATION_METHOD = "meshagent.request.validation.method"
ANNOTATION_REQUEST_VALIDATION_SECRET = "meshagent.request.validation.secret"
ANNOTATION_STORAGE_CLASS = "meshagent.storage.class"
ANNOTATION_ROOM_MAX_RUNTIME_SECONDS = "meshagent.room.max-runtime-seconds"
ANNOTATION_ROOM_EMPTY_ROOM_TIMEOUT = "meshagent.room.empty-room-timeout"

ANNOTATION_FILE_PROMPT = "meshagent.prompt.file.matches.regex"

agent_type = Literal[
    "ChatBot",
    "VoiceBot",
    "Transcriber",
    "TaskRunner",
    "MailBot",
    "Worker",
    "Shell",
]


class PromptTemplate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    description: Optional[str] = None
    prompt: str
    annotations: Optional[dict[str, str]] = None


class ChannelSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    annotations: Optional[dict[str, str]] = None


class EmailChannel(ChannelSpec):
    model_config = ConfigDict(extra="forbid")
    address: str
    private: bool = True


class QueueChannel(ChannelSpec):
    model_config = ConfigDict(extra="forbid")
    queue: str
    threading_mode: Optional[Literal["default-new"]] = None
    message_schema: Optional[dict] = None


class MessagingChannel(ChannelSpec):
    model_config = ConfigDict(extra="forbid")
    protocol: str = "meshagent.agent-message.v1"
    prompts: Optional[list[PromptTemplate]] = None


class ToolkitChannel(ChannelSpec):
    model_config = ConfigDict(extra="forbid")
    name: str


class ChannelsSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    email: Optional[list[EmailChannel]] = None
    messaging: Optional[list[MessagingChannel]] = None
    queue: Optional[list[QueueChannel]] = None
    toolkit: Optional[list[ToolkitChannel]] = None


class EmailSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    address: str
    public: bool = False

    @model_validator(mode="after")
    def validate_address(self) -> "EmailSpec":
        if self.address.strip() == "":
            raise ValueError("email.address must not be empty")
        return self


class HeartbeatSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    queue: str
    thread_id: Optional[str] = None
    prompt: Optional[list[AgentInputContent]] = None
    minutes: PositiveInt

    @model_validator(mode="after")
    def validate_prompt_source(self) -> "HeartbeatSpec":
        if self.queue.strip() == "":
            raise ValueError("heartbeat.queue must not be empty")

        if self.prompt is None or len(self.prompt) == 0:
            raise ValueError("heartbeat requires prompt")

        return self


class AgentSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    description: Optional[str] = None
    annotations: Optional[dict[str, str]] = None
    channels: Optional[ChannelsSpec] = None
    email: Optional[EmailSpec] = None
    heartbeat: Optional[HeartbeatSpec] = None


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
    template: Optional[ContainerTemplate] = Field(
        "agent",
        description=(
            "container defaults to apply. 'agent' mounts room storage at /data "
            "and injects MeshAgent/OpenAI/Anthropic proxy environment variables "
            "with a token that has default agent permissions unless manually "
            "overridden. 'none' applies no defaults."
        ),
    )

    command: Optional[str] = None
    working_dir: Optional[str] = None
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
    on_demand: Optional[bool] = Field(None, description="an on demand service")
    writable_root_fs: Optional[bool] = None
    private: bool = True


class ScheduledTaskMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: Optional[str] = None
    annotations: dict[str, str] = Field(default_factory=dict)


class ScheduledTaskQueueSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    payload: dict[str, Any] = Field(default_factory=dict)
    storage_write_path: Optional[str] = None

    @model_validator(mode="after")
    def validate_name(self) -> "ScheduledTaskQueueSpec":
        if self.name.strip() == "":
            raise ValueError("queue.name must not be empty")
        return self


class ScheduledTaskSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: Literal["v1"] = "v1"
    kind: Literal["ScheduledTask"] = "ScheduledTask"
    metadata: ScheduledTaskMetadata = Field(default_factory=ScheduledTaskMetadata)
    schedule: str
    active: bool = True
    once: bool = False
    queue: Optional[ScheduledTaskQueueSpec] = None
    container: Optional[ContainerSpec] = None

    @model_validator(mode="after")
    def validate_target(self) -> "ScheduledTaskSpec":
        if self.schedule.strip() == "":
            raise ValueError("schedule must not be empty")
        interval_seconds = _scheduled_task_interval_seconds(self.schedule)
        if interval_seconds < MIN_SCHEDULED_TASK_INTERVAL_SECONDS:
            raise ValueError(
                "ScheduledTaskSpec schedule must run no more frequently than every 15 minutes"
            )
        if (self.queue is None) == (self.container is None):
            raise ValueError(
                "ScheduledTaskSpec requires exactly one of queue or container"
            )
        return self

    @staticmethod
    def from_yaml(yaml: str):
        YAML, SafeLoader = _yaml_support()
        obj = YAML.load(yaml, Loader=SafeLoader)
        return ScheduledTaskSpec.model_validate(obj)


class RouteMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    annotations: dict[str, str] = Field(default_factory=dict)


class RouteRoomBackendSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str


class RouteAgentBackendSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str


class RouteBackendSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    room: RouteRoomBackendSpec | None = None
    agent: RouteAgentBackendSpec | None = None

    @model_validator(mode="after")
    def validate_target(self) -> "RouteBackendSpec":
        targets = [self.room is not None, self.agent is not None]
        if sum(targets) != 1:
            raise ValueError("RouteSpec backend requires exactly one of room or agent")
        return self


class RoutePathSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    path: str = "/"
    pathType: Literal["prefix", "exact"] = "prefix"
    stripPrefix: bool = False
    targetPort: str | int

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        if not value.startswith("/"):
            raise ValueError("RouteSpec paths must start with /")
        return value

    @field_validator("targetPort")
    @classmethod
    def validate_target_port(cls, value: str | int) -> str | int:
        if isinstance(value, str) and value.strip() == "":
            raise ValueError("RouteSpec targetPort must not be empty")
        return value


class RouteSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: Literal["v1"] = "v1"
    kind: Literal["Route"] = "Route"
    metadata: RouteMetadata
    domain: str
    backend: RouteBackendSpec
    paths: list[RoutePathSpec] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def migrate_legacy_route(cls, value: object) -> object:
        if not isinstance(value, dict):
            return value
        if "backend" in value:
            return value
        if "room_name" not in value:
            return value
        domain = value.get("domain")
        room_name = value.get("room_name")
        port = value.get("port")
        annotations = value.get("annotations") or {}
        return {
            "version": "v1",
            "kind": "Route",
            "metadata": {"name": str(domain or ""), "annotations": annotations},
            "domain": domain,
            "backend": {"room": {"name": room_name}},
            "paths": [{"path": "/", "pathType": "prefix", "targetPort": port}],
        }

    @model_validator(mode="after")
    def validate_paths(self) -> "RouteSpec":
        if self.backend.room is not None and len(self.paths) == 0:
            raise ValueError("RouteSpec room backend requires at least one path")
        if self.backend.agent is not None and len(self.paths) != 0:
            raise ValueError("RouteSpec agent backend does not support paths")
        return self

    @property
    def name(self) -> str:
        return self.metadata.name

    @property
    def annotations(self) -> dict[str, str]:
        return self.metadata.annotations

    @property
    def room_name(self) -> str | None:
        return self.backend.room.name if self.backend.room is not None else None

    @property
    def agent_name(self) -> str | None:
        return self.backend.agent.name if self.backend.agent is not None else None

    @staticmethod
    def from_yaml(yaml: str) -> "RouteSpec":
        YAML, _ = _yaml_support()
        return RouteSpec.model_validate(YAML.safe_load(yaml))


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
    files: Optional[list[ServiceFileSpec]] = Field(
        None,
        description="files that should be created in room storage if they do not exist",
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

    @staticmethod
    def from_yaml(yaml: str) -> "ServiceSpec":
        YAML, _ = _yaml_support()
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
    annotations: Optional[dict[str, str]] = None


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
    annotations: Optional[dict[str, str]] = None

    @field_validator("num")
    @classmethod
    def _validate_reserved_service_port(
        cls, value: Literal["*"] | PositiveInt
    ) -> Literal["*"] | PositiveInt:
        if isinstance(value, int) and value in RESERVED_ROOM_SERVICE_PORTS:
            reserved_ports = ", ".join(
                str(port) for port in sorted(RESERVED_ROOM_SERVICE_PORTS)
            )
            raise ValueError(
                f"service port {value} is reserved for MeshAgent room infrastructure; "
                f"reserved ports: {reserved_ports}"
            )
        return value


class ServiceTemplateVariable(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    title: Optional[str] = None
    description: Optional[str] = None
    obscure: bool = False
    enum: Optional[list[str]] = None
    optional: bool = False
    # Optional hint for variable type; absent in many templates
    type: Optional[Literal["email", "route"]] = None
    annotations: Optional[dict[str, str]] = None


class ServiceTemplateContainerMountSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    room: Optional[list[RoomStorageMountSpec]] = None
    project: Optional[list[ProjectStorageMountSpec]] = None
    images: Optional[list[ImageStorageMountSpec]] = None
    files: Optional[list[FileStorageMountSpec]] = None
    empty_dirs: Optional[list[EmptyDirMountSpec]] = None
    configs: Optional[list[ConfigMountSpec]] = None


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
    secret: Optional[SecretValue] = None


class AgentTemplateSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    description: Optional[str] = None
    annotations: Optional[dict[str, str]] = None
    channels: Optional[ChannelsSpec] = None
    email: Optional[EmailSpec] = None
    heartbeat: Optional[HeartbeatSpec] = None


class ContainerTemplateSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    environment: Optional[list[TemplateEnvironmentVariable]] = None
    image: Optional[str] = None
    template: Optional[ContainerTemplate] = Field(
        "agent",
        description=(
            "container defaults to apply. 'agent' mounts room storage at /data "
            "and injects MeshAgent/OpenAI/Anthropic proxy environment variables "
            "with a token that has default agent permissions unless manually "
            "overridden. 'none' applies no defaults."
        ),
    )
    command: Optional[str] = None
    working_dir: Optional[str] = None
    storage: Optional[ServiceTemplateContainerMountSpec] = None
    on_demand: Optional[bool] = None
    writable_root_fs: Optional[bool] = None
    private: bool = True


class ExternalServiceTemplateSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    url: str


def format_yaml_value(original: Optional[str], values: dict[str, str]):
    if original is None:
        return None

    legacy_prefixed = original.startswith("!template ")
    value = original.removeprefix("!template ") if legacy_prefixed else original
    has_jinja_syntax = any(token in value for token in ("{{", "{%", "{#"))

    if not legacy_prefixed and not has_jinja_syntax:
        return original

    from jinja2 import Template

    template = Template(value)
    return template.render(**values)


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
    files: Optional[list[ServiceFileSpec]] = None
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
                            secret=e.secret,
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
                        channels=a.channels,
                        email=a.email,
                        heartbeat=a.heartbeat,
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
            files=self.files,
            container=ContainerSpec(
                command=self.container.command,
                working_dir=self.container.working_dir,
                image=self.container.image,
                template=self.container.template,
                environment=env,
                storage=ContainerMountSpec(
                    room=self.container.storage.room,
                    project=self.container.storage.project,
                    images=self.container.storage.images,
                    files=self.container.storage.files,
                    empty_dirs=self.container.storage.empty_dirs,
                    configs=self.container.storage.configs,
                )
                if self.container.storage is not None
                else None,
                writable_root_fs=self.container.writable_root_fs,
                on_demand=self.container.on_demand,
                private=self.container.private,
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

        YAML, SafeLoader = _yaml_support()

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
