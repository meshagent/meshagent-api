from __future__ import annotations

import re
from typing import Annotated, Any, Literal
from urllib.parse import urlsplit

import jsonschema
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class AllowedModel(BaseModel):
    provider: str
    model: str


class AllowedOpenAIModel(AllowedModel):
    provider: Literal["openai"] = "openai"
    output_modalities: list[str] | None = None


class AllowedAnthropicModel(AllowedModel):
    provider: Literal["anthropic"] = "anthropic"


class AllowedGrokModel(AllowedModel):
    provider: Literal["grok"] = "grok"


ManagedAllowedModel = Annotated[
    AllowedOpenAIModel | AllowedAnthropicModel | AllowedGrokModel,
    Field(discriminator="provider"),
]

ManagedAgentThreadIsolation = Literal["global", "participant"]
ManagedAgentLlmDelegation = Literal["required", "optional"]


class ManagedAgentRunAs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    email: str = Field(
        ..., description="the service account email this managed agent runs as"
    )
    scopes: list[str] = Field(
        default_factory=lambda: ["secrets:proxy", "llm_proxy"],
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
    allowed_domains: list[str] | None = None
    blocked_domains: list[str] | None = None


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

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        return value.strip().lower()


def _validate_python_tool_schema(schema: dict[str, Any], path: str = "input_schema"):
    allowed = {
        "type",
        "title",
        "description",
        "properties",
        "required",
        "additionalProperties",
        "items",
        "$defs",
        "$ref",
        "anyOf",
        "enum",
        "const",
        "format",
        "pattern",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
        "minItems",
        "maxItems",
    }
    if not isinstance(schema, dict):
        raise ValueError(f"{path}: expected a schema object")
    if not any(key in schema for key in ("type", "$ref", "anyOf")):
        raise ValueError(f"{path}: schema requires type, $ref, or anyOf")
    for key in schema:
        if key not in allowed:
            raise ValueError(f"{path}.{key}: unsupported strict-schema keyword")
    if "$ref" in schema and (
        not isinstance(schema["$ref"], str) or not schema["$ref"].startswith("#/$defs/")
    ):
        raise ValueError(f"{path}: only local $defs references are supported")
    schema_type = schema.get("type")
    if schema_type == "object" or (
        isinstance(schema_type, list) and "object" in schema_type
    ):
        properties = schema.get("properties")
        required = schema.get("required")
        if schema.get("additionalProperties") is not False:
            raise ValueError(f"{path}.additionalProperties must be false")
        if not isinstance(properties, dict) or not isinstance(required, list):
            raise ValueError(f"{path}: properties and required are required")
        if not all(isinstance(name, str) for name in required) or (
            len(set(required)) != len(required) or set(required) != set(properties)
        ):
            raise ValueError(f"{path}.required must list every property exactly once")
    for key in ("properties", "$defs"):
        children = schema.get(key, {})
        if isinstance(children, dict):
            for name, child in children.items():
                _validate_python_tool_schema(child, f"{path}.{key}.{name}")
    if "items" in schema:
        _validate_python_tool_schema(schema["items"], f"{path}.items")
    for index, child in enumerate(schema.get("anyOf", [])):
        _validate_python_tool_schema(child, f"{path}.anyOf[{index}]")


class ManagedAgentPythonTool(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["python"] = "python"
    name: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$", max_length=64)
    description: str = ""
    input_schema: dict[str, Any]
    source: str = Field(min_length=1, max_length=65536)
    entrypoint: str | None = Field(
        default=None, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$", max_length=64
    )

    @field_validator("source")
    @classmethod
    def validate_source(cls, value: str) -> str:
        if len(value.encode("utf-8")) > 65536:
            raise ValueError("source must contain at most 65536 bytes")
        return value

    @field_validator("input_schema")
    @classmethod
    def validate_input_schema(cls, value: dict[str, Any]) -> dict[str, Any]:
        if "anyOf" in value:
            raise ValueError("root anyOf is not supported")
        if value.get("type") != "object":
            raise ValueError("input_schema must be an object schema")
        try:
            jsonschema.Draft202012Validator.check_schema(value)
        except jsonschema.SchemaError as error:
            raise ValueError(f"invalid input_schema: {error.message}") from error
        _validate_python_tool_schema(value)
        return value


class ManagedAgentSecretHeader(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str

    @field_validator("id")
    @classmethod
    def validate_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("secret.id must not be empty")
        return value


class ManagedAgentConnectionHeader(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    value: str | None = None
    secret: ManagedAgentSecretHeader | None = None
    prefix: str = ""

    @field_validator("prefix")
    @classmethod
    def validate_prefix(cls, value: str) -> str:
        if any(ord(character) < 32 or ord(character) == 127 for character in value):
            raise ValueError("prefix must not contain control characters")
        return value

    @model_validator(mode="after")
    def validate_source(self) -> ManagedAgentConnectionHeader:
        if (self.value is None) == (self.secret is None):
            raise ValueError("header requires exactly one of value or secret")
        if self.value is not None and self.prefix:
            raise ValueError("prefix requires secret")
        return self


class ManagedAgentHttpConnection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    base_url: str
    use_proxy_secret: str | None = None
    headers: list[ManagedAgentConnectionHeader] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_headers(self) -> ManagedAgentHttpConnection:
        reserved = {
            "host",
            "content-length",
            "transfer-encoding",
            "connection",
            "proxy-authorization",
            "proxy-authenticate",
            "proxy-connection",
            "keep-alive",
            "upgrade",
            "te",
            "trailer",
            "cookie",
            "set-cookie",
            "sec-websocket-accept",
            "sec-websocket-extensions",
            "sec-websocket-key",
            "sec-websocket-protocol",
            "sec-websocket-version",
        }
        names: set[str] = set()
        secret_count = int(self.use_proxy_secret is not None)
        for header in self.headers:
            name = header.name
            normalized = name.lower()
            if (
                re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", name) is None
                or normalized in reserved
                or normalized in names
            ):
                raise ValueError(f"header {name} is not allowed")
            names.add(normalized)
            if header.value is not None:
                if any(
                    ord(character) < 32 or ord(character) == 127
                    for character in header.value
                ):
                    raise ValueError(f"header {name} has an invalid value")
            else:
                secret_count += 1
        if self.use_proxy_secret is not None and "authorization" in names:
            raise ValueError("Authorization conflicts with use_proxy_secret")
        if secret_count > 16:
            raise ValueError("a connection supports at most 16 secret headers")
        if secret_count and any(
            header.name.lower() == "authorization" and header.value is not None
            for header in self.headers
        ):
            raise ValueError(
                "literal Authorization cannot be combined with secret headers"
            )
        return self

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, value: str) -> str:
        parsed = urlsplit(value)
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
        ):
            raise ValueError("URL must be HTTP(S), without credentials or fragment")
        return value

    @field_validator("use_proxy_secret")
    @classmethod
    def validate_proxy_secret(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("use_proxy_secret must not be empty")
        return value


class ManagedAgentEnvironmentVariable(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$", max_length=128)
    value: str | None = None
    secret: ManagedAgentSecretHeader | None = None

    @model_validator(mode="after")
    def validate_source(self) -> ManagedAgentEnvironmentVariable:
        if (self.value is None) == (self.secret is None):
            raise ValueError(
                "environment entry requires exactly one of value or secret"
            )
        return self


class ManagedAgentDecisionCheck(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["decision"] = "decision"
    model: str = Field(pattern=r"^openai/[^/\s]+$")
    instructions: str = Field(min_length=1, max_length=16384)
    minimum_allow_probability: float = Field(ge=0, le=1, allow_inf_nan=False)

    @field_validator("instructions")
    @classmethod
    def validate_instructions(cls, value: str) -> str:
        if not value.strip() or len(value.encode("utf-8")) > 16384:
            raise ValueError(
                "guardrail instructions must contain 1 to 16384 UTF-8 bytes"
            )
        return value


class ManagedAgentGuardrailFailure(BaseModel):
    model_config = ConfigDict(extra="forbid")
    action: Literal["block", "deny", "replace", "pass", "fail"] = "block"
    message: str | None = None

    @model_validator(mode="after")
    def validate_replacement(self):
        if self.action == "replace" and not (self.message or "").strip():
            raise ValueError("replacement guardrails require a message")
        return self


class ManagedAgentGuardrail(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1)
    check: ManagedAgentDecisionCheck
    on_reject: ManagedAgentGuardrailFailure = Field(
        default_factory=ManagedAgentGuardrailFailure
    )
    on_error: ManagedAgentGuardrailFailure = Field(
        default_factory=lambda: ManagedAgentGuardrailFailure(action="fail")
    )
    max_retries: int = Field(default=2, ge=0, le=3, strict=True)


class ManagedAgentToolMatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    names: list[str] = Field(min_length=1)


class ManagedAgentToolGuardrails(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=1)
    match: ManagedAgentToolMatch
    input: list[ManagedAgentGuardrail] = Field(default_factory=list)
    output: list[ManagedAgentGuardrail] = Field(default_factory=list)


class ManagedAgentOutputGuardrails(BaseModel):
    model_config = ConfigDict(extra="forbid")
    checks: list[ManagedAgentGuardrail] = Field(default_factory=list)


class ManagedAgentGuardrails(BaseModel):
    model_config = ConfigDict(extra="forbid")
    input: list[ManagedAgentGuardrail] = Field(default_factory=list)
    tools: list[ManagedAgentToolGuardrails] = Field(default_factory=list)
    output: ManagedAgentOutputGuardrails = Field(
        default_factory=ManagedAgentOutputGuardrails
    )

    @model_validator(mode="after")
    def validate_rules(self):
        names: set[str] = set()
        rules = [*self.input, *self.output.checks]
        for tool in self.tools:
            if not tool.name.strip() or any(
                not name.strip() for name in tool.match.names
            ):
                raise ValueError("tool guardrails require nonempty names")
            rules.extend(tool.input)
            rules.extend(tool.output)
        if len(rules) > 32:
            raise ValueError("at most 32 guardrail checks are allowed")
        boundaries = [
            (self.input, {"block"}),
            (self.output.checks, {"block", "replace"}),
        ]
        for tool in self.tools:
            boundaries.extend(
                [
                    (tool.input, {"block", "deny"}),
                    (tool.output, {"block", "deny", "replace"}),
                ]
            )
        for checks, allowed in boundaries:
            for rule in checks:
                if (
                    rule.on_reject.action not in allowed
                    or rule.on_error.action not in allowed | {"pass", "fail"}
                ):
                    raise ValueError(
                        "guardrail action is not supported at this boundary"
                    )
        for rule in rules:
            if (
                not rule.name
                or len(rule.name) > 64
                or any(
                    not (char.isascii() and (char.isalnum() or char in "_.-"))
                    for char in rule.name
                )
                or rule.name in names
            ):
                raise ValueError("guardrail names must be nonempty and unique")
            names.add(rule.name)
            if not rule.check.instructions.strip():
                raise ValueError("guardrail instructions must not be empty")
        return self


class ManagedAgentSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: Literal["v1"] = "v1"
    kind: Literal["ManagedAgent"] = "ManagedAgent"
    id: str | None = None
    metadata: ManagedAgentMetadata
    allowed_models: list[ManagedAllowedModel]
    guardrails: ManagedAgentGuardrails | None = None
    run_as: ManagedAgentRunAs | None = None
    llm_delegation: ManagedAgentLlmDelegation = "optional"
    thread_isolation: ManagedAgentThreadIsolation = "global"
    instructions: str | None = None
    toolkits: list[ManagedToolkit] | None = None
    tools: list[ManagedAgentPythonTool] = Field(default_factory=list)
    connections: dict[str, ManagedAgentHttpConnection] = Field(default_factory=dict)
    environment: list[ManagedAgentEnvironmentVariable] = Field(default_factory=list)
    output_modalities: list[str] | None = None
    store: bool = True

    @field_validator("tools")
    @classmethod
    def validate_tool_names(cls, value: list[ManagedAgentPythonTool]):
        names = [tool.name for tool in value]
        if len(set(names)) != len(names):
            raise ValueError("duplicate Python tool name")
        return value

    @model_validator(mode="after")
    def validate_environment(self) -> ManagedAgentSpec:
        names = [entry.name for entry in self.environment]
        if len(names) != len(set(names)):
            raise ValueError("duplicate environment variable name")
        if len(names) > 64:
            raise ValueError("at most 64 environment variables are supported")
        if any(entry.secret is not None for entry in self.environment):
            if self.run_as is None or "secrets:read" not in self.run_as.scopes:
                raise ValueError(
                    "secret environment variables require run_as with secrets:read"
                )
        return self

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
