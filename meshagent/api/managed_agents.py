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


class ManagedAgentRunAs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    email: str = Field(
        ..., description="the service account email this managed agent runs as"
    )
    scopes: list[str] = Field(
        default_factory=lambda: ["secrets:proxy"],
        description=(
            "OAuth scopes to grant to the runtime service account token. "
            "The project scope is always added by the server."
        ),
    )

    @field_validator("email")
    @classmethod
    def validate_email(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized == "":
            raise ValueError("run_as.email must not be empty")
        return normalized

    @field_validator("scopes")
    @classmethod
    def validate_scopes(cls, value: list[str]) -> list[str]:
        normalized_scopes: list[str] = []
        seen: set[str] = set()
        for scope in value:
            normalized_scope = scope.strip()
            if normalized_scope == "":
                continue
            if normalized_scope not in seen:
                normalized_scopes.append(normalized_scope)
                seen.add(normalized_scope)
        return normalized_scopes


class ManagedAgentToolkit(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str


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


class ManagedAgentMCPHeader(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    value: str


class ManagedAgentMCPServer(BaseModel):
    model_config = ConfigDict(extra="forbid")

    server_label: str
    server_url: str
    allowed_tools: list[str] | None = None
    headers: list[ManagedAgentMCPHeader] | None = None
    require_approval: Literal["always", "never"] | None = None
    always_require_approval: list[str] | None = None
    never_require_approval: list[str] | None = None
    openai_connector_id: str | None = None
    use_proxy_secret: str | None = None

    @model_validator(mode="before")
    @classmethod
    def drop_legacy_authorization(cls, value: object) -> object:
        if isinstance(value, dict) and "authorization" in value:
            migrated = dict(value)
            migrated.pop("authorization", None)
            return migrated
        return value


class ManagedAgentMCPToolkit(ManagedAgentToolkit):
    type: Literal["mcp"] = "mcp"
    servers: list[ManagedAgentMCPServer]


ManagedToolkit = Annotated[
    ManagedAgentWebSearch
    | ManagedAgentWebFetch
    | ManagedAgentImageGeneration
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
    run_as: ManagedAgentRunAs | None = None
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
