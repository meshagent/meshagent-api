from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class AllowedModel(BaseModel):
    provider: str
    model: str


class AllowedOpenAIModel(AllowedModel):
    provider: Literal["openai"] = "openai"
    output_modalities: list[str] | None = None


class AllowedAnthropicModel(AllowedModel):
    provider: Literal["anthropic"] = "anthropic"


ManagedAllowedModel = Annotated[
    AllowedOpenAIModel | AllowedAnthropicModel,
    Field(discriminator="provider"),
]

ManagedAgentThreadIsolation = Literal["global", "participant"]


class ManagedAgentStorageMount(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str
    path: str
    read_only: bool = False
    subpath: str | None = None

    @field_validator("path")
    @classmethod
    def validate_path(cls, path: str) -> str:
        if path == "" or not path.startswith("/"):
            raise ValueError("path must be an absolute storage path")
        return path


class ManagedAgentRoomMount(ManagedAgentStorageMount):
    type: Literal["room"] = "room"
    room_name: str


class ManagedAgentProjectMount(ManagedAgentStorageMount):
    type: Literal["project"] = "project"
    room_name: str


class ManagedAgentAgentMount(ManagedAgentStorageMount):
    type: Literal["agent"] = "agent"


ManagedStorageMount = Annotated[
    ManagedAgentRoomMount | ManagedAgentProjectMount | ManagedAgentAgentMount,
    Field(discriminator="type"),
]


class ManagedAgentToolkit(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str


class ManagedAgentStorageToolkit(ManagedAgentToolkit):
    type: Literal["storage"] = "storage"
    mounts: list[ManagedStorageMount]


class ManagedAgentWebSearch(ManagedAgentToolkit):
    type: Literal["web_search"] = "web_search"


class ManagedAgentWebFetch(ManagedAgentToolkit):
    type: Literal["web_fetch"] = "web_fetch"


class ManagedAgentImageGeneration(ManagedAgentToolkit):
    type: Literal["image_generation"] = "image_generation"
    background: Literal["transparent", "opaque", "auto"] | None = None
    input_image_mask_url: str | None = None
    model: str | None = None
    moderation: str | None = None
    output_compression: int | None = None
    output_format: Literal["png", "webp", "jpeg"] | None = None
    partial_images: int | None = 1
    quality: Literal["auto", "low", "medium", "high"] | None = None
    size: Literal["1024x1024", "1024x1536", "1536x1024", "auto"] | None = None


class ManagedAgentShell(ManagedAgentToolkit):
    type: Literal["shell"] = "shell"
    room_name: str
    image: str = "meshagent/python:default"


class AgentSecretRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["agent"] = "agent"
    secret_id: str


class UserOAuthSecretRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    secret_name: str
    client_secret_id: str | None = None
    scopes: list[str] | None = None
    authorization_endpoint: str | None = None
    token_endpoint: str | None = None
    registration_endpoint: str | None = None
    redirect_uri: str | None = None
    client_id: str | None = None
    no_pkce: bool = False


class UserSecretRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["user"] = "user"
    secret_name: str
    prompt: str | None = None
    oauth: UserOAuthSecretRequest | None = None


ManagedAgentSecretRef = Annotated[
    AgentSecretRef | UserSecretRef,
    Field(discriminator="type"),
]


class ManagedAgentMCPBearerAuthorization(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["bearer"] = "bearer"
    secret: ManagedAgentSecretRef


class ManagedAgentMCPHeaderAuthorization(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["header"] = "header"
    name: str
    secret: ManagedAgentSecretRef


ManagedAgentMCPAuthorization = Annotated[
    ManagedAgentMCPBearerAuthorization | ManagedAgentMCPHeaderAuthorization,
    Field(discriminator="type"),
]


class ManagedAgentMCPHeader(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    value: str


class ManagedAgentMCPServer(BaseModel):
    model_config = ConfigDict(extra="forbid")

    server_label: str
    server_url: str
    allowed_tools: list[str] | None = None
    authorization: ManagedAgentMCPAuthorization | None = None
    headers: list[ManagedAgentMCPHeader] | None = None
    require_approval: Literal["always", "never"] | None = None
    always_require_approval: list[str] | None = None
    never_require_approval: list[str] | None = None
    openai_connector_id: str | None = None


class ManagedAgentMCPToolkit(ManagedAgentToolkit):
    type: Literal["mcp"] = "mcp"
    servers: list[ManagedAgentMCPServer]


ManagedToolkit = Annotated[
    ManagedAgentStorageToolkit
    | ManagedAgentWebSearch
    | ManagedAgentWebFetch
    | ManagedAgentImageGeneration
    | ManagedAgentShell
    | ManagedAgentMCPToolkit,
    Field(discriminator="type"),
]


class ManagedAgentMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    annotations: dict[str, str] = Field(default_factory=dict)


class ManagedAgentSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: Literal["v1"] = "v1"
    kind: Literal["ManagedAgent"] = "ManagedAgent"
    id: str | None = None
    metadata: ManagedAgentMetadata
    allowed_models: list[ManagedAllowedModel]
    thread_isolation: ManagedAgentThreadIsolation = "global"
    instructions: str | None = None
    toolkits: list[ManagedToolkit] | None = None
    output_modalities: list[str] | None = None
    store: bool = True

    @model_validator(mode="before")
    @classmethod
    def migrate_legacy_name(cls, value: object) -> object:
        if isinstance(value, dict) and "name" in value and "metadata" not in value:
            migrated = dict(value)
            migrated["metadata"] = {"name": migrated.pop("name")}
            return migrated
        return value

    @property
    def name(self) -> str:
        return self.metadata.name
