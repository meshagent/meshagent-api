from __future__ import annotations

import re
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_PORTABLE_TOOL_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]{0,63}$")
_DOMAIN_LABEL = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$")


def _non_empty(value: str, *, field: str) -> str:
    if value.strip() == "":
        raise ValueError(f"{field} must not be empty")
    return value


def _portable_tool_name(value: str) -> str:
    value = value.strip()
    if _PORTABLE_TOOL_NAME.fullmatch(value) is None:
        raise ValueError(
            "name must start with a letter or underscore, contain only letters, "
            "numbers, underscores, or dashes, and be at most 64 characters"
        )
    return value


def _domain_name(value: str) -> str:
    value = value.strip().lower().rstrip(".")
    if (
        value == ""
        or len(value) > 253
        or "://" in value
        or "/" in value
        or any(_DOMAIN_LABEL.fullmatch(label) is None for label in value.split("."))
    ):
        raise ValueError("domain entries must be bare DNS host names")
    return value


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class AllowedLlmModel(_StrictModel):
    provider: str
    model: str

    @field_validator("provider")
    @classmethod
    def _provider(cls, value: str) -> str:
        return _non_empty(value, field="provider").strip().lower()

    @field_validator("model")
    @classmethod
    def _model(cls, value: str) -> str:
        return _non_empty(value, field="model").strip()


class LlmAppFilterMode(StrEnum):
    BLOCKED = "blocked"
    ALLOWED = "allowed"


class LlmAppFilter(_StrictModel):
    mode: LlmAppFilterMode = LlmAppFilterMode.BLOCKED
    user_agent_patterns: list[str] = Field(
        default_factory=list, alias="userAgentPatterns"
    )
    block_missing_user_agent: bool = Field(default=False, alias="blockMissingUserAgent")

    @field_validator("user_agent_patterns")
    @classmethod
    def _patterns(cls, values: list[str]) -> list[str]:
        return [_non_empty(value, field="user-agent pattern") for value in values]


class LlmRouterProvider(StrEnum):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    GROK = "grok"


class LlmRouterApi(StrEnum):
    COMPLETIONS = "completions"
    CHAT_COMPLETIONS = "chat_completions"
    RESPONSES = "responses"
    MESSAGES = "messages"
    REALTIME = "realtime"


class LlmRouterTransport(StrEnum):
    HTTP = "http"
    WEBSOCKET = "websocket"


class LlmRouterMatch(_StrictModel):
    header: dict[str, str] = Field(default_factory=dict)
    models: list[str] = Field(default_factory=list)
    providers: list[LlmRouterProvider] = Field(default_factory=list)
    apis: list[LlmRouterApi] = Field(default_factory=list)
    transports: list[LlmRouterTransport] = Field(default_factory=list)

    @field_validator("header")
    @classmethod
    def _headers(cls, values: dict[str, str]) -> dict[str, str]:
        result: dict[str, str] = {}
        for name, pattern in values.items():
            name = name.strip().lower()
            if name == "" or not all(
                character.isalnum() or character in "!#$%&'*+-.^_`|~"
                for character in name
            ):
                raise ValueError("header names must be valid HTTP field names")
            if name in result:
                raise ValueError(f"duplicate header name: {name}")
            result[name] = _non_empty(pattern, field="header pattern")
        return result

    @field_validator("models")
    @classmethod
    def _models(cls, values: list[str]) -> list[str]:
        return [_non_empty(value, field="model pattern") for value in values]

    @field_validator("providers", mode="before")
    @classmethod
    def _providers(cls, value: object) -> object:
        if isinstance(value, list):
            return [
                item.strip().lower() if isinstance(item, str) else item
                for item in value
            ]
        return value


class LlmInstructionTransform(_StrictModel):
    replace: str | None = None
    append: str | None = None

    @field_validator("replace", "append")
    @classmethod
    def _text(cls, value: str | None) -> str | None:
        if value is not None:
            _non_empty(value, field="instruction")
        return value

    @model_validator(mode="after")
    def _operation(self) -> LlmInstructionTransform:
        if self.replace is None and self.append is None:
            raise ValueError("instructions requires replace or append")
        return self


class LlmToolSelector(_StrictModel):
    type: str
    name: str | None = None

    @field_validator("type")
    @classmethod
    def _type(cls, value: str) -> str:
        return _non_empty(value, field="tool type")

    @field_validator("name")
    @classmethod
    def _name(cls, value: str | None) -> str | None:
        return None if value is None else _non_empty(value, field="tool name")


class LlmWebSearchTool(_StrictModel):
    type: Literal["web_search"]
    search_context_size: Literal["low", "medium", "high"] | None = None
    allowed_domains: list[str] = Field(default_factory=list)
    blocked_domains: list[str] = Field(default_factory=list)

    @field_validator("allowed_domains", "blocked_domains")
    @classmethod
    def _domains(cls, values: list[str]) -> list[str]:
        result = [_domain_name(value) for value in values]
        if len(result) != len(set(result)):
            raise ValueError("domain lists must not contain duplicates")
        return result

    @model_validator(mode="after")
    def _domain_mode(self) -> LlmWebSearchTool:
        if self.allowed_domains and self.blocked_domains:
            raise ValueError(
                "allowed_domains and blocked_domains are mutually exclusive"
            )
        return self


class LlmStaticTool(_StrictModel):
    type: Literal["static"]
    name: str
    description: str
    content: str

    @field_validator("name")
    @classmethod
    def _name(cls, value: str) -> str:
        return _portable_tool_name(value)

    @field_validator("description", "content")
    @classmethod
    def _text(cls, value: str) -> str:
        return _non_empty(value, field="static tool text")


class LlmAdvisor(_StrictModel):
    provider: LlmRouterProvider
    model: str
    instructions: str | None = None
    max_output_tokens: int | None = Field(default=None, ge=1)

    @field_validator("provider", mode="before")
    @classmethod
    def _provider(cls, value: object) -> object:
        return value.strip().lower() if isinstance(value, str) else value

    @field_validator("model")
    @classmethod
    def _model(cls, value: str) -> str:
        return _non_empty(value, field="advisor model").strip()

    @field_validator("instructions")
    @classmethod
    def _instructions(cls, value: str | None) -> str | None:
        if value is not None:
            _non_empty(value, field="advisor instructions")
        return value


class LlmAdviceTool(_StrictModel):
    type: Literal["advice"]
    name: str
    description: str
    strategy: Literal["parallel"] = "parallel"
    advisors: list[LlmAdvisor] = Field(min_length=1, max_length=8)

    @field_validator("name")
    @classmethod
    def _name(cls, value: str) -> str:
        return _portable_tool_name(value)

    @field_validator("description")
    @classmethod
    def _description(cls, value: str) -> str:
        return _non_empty(value, field="advice tool description")


LlmEnsureTool = Annotated[
    LlmWebSearchTool | LlmStaticTool | LlmAdviceTool,
    Field(discriminator="type"),
]


class LlmToolTransform(_StrictModel):
    deny: list[LlmToolSelector] = Field(default_factory=list)
    ignore: list[LlmToolSelector] = Field(default_factory=list)
    ensure: list[LlmEnsureTool] = Field(default_factory=list)

    @model_validator(mode="after")
    def _operations(self) -> LlmToolTransform:
        if not self.deny and not self.ignore and not self.ensure:
            raise ValueError("tools requires deny, ignore, or ensure")
        identities: set[tuple[str, str | None]] = set()
        for tool in self.ensure:
            name = (
                tool.name if isinstance(tool, (LlmStaticTool, LlmAdviceTool)) else None
            )
            identity = (
                "function"
                if isinstance(tool, (LlmStaticTool, LlmAdviceTool))
                else tool.type,
                name,
            )
            if identity in identities:
                raise ValueError(f"duplicate ensured tool: {tool.type}")
            identities.add(identity)
        return self


class LlmRouterThen(_StrictModel):
    instructions: LlmInstructionTransform | None = None
    tools: LlmToolTransform | None = None

    @model_validator(mode="after")
    def _transform(self) -> LlmRouterThen:
        if self.instructions is None and self.tools is None:
            raise ValueError("then requires instructions or tools")
        return self


class LlmRouterRule(_StrictModel):
    name: str | None = None
    when: LlmRouterMatch = Field(default_factory=LlmRouterMatch)
    then: LlmRouterThen

    @field_validator("name")
    @classmethod
    def _name(cls, value: str | None) -> str | None:
        return None if value is None else _non_empty(value, field="rule name").strip()


class LlmRouterConfig(_StrictModel):
    allowed_models: list[AllowedLlmModel] | None = Field(
        default=None, alias="allowedModels"
    )
    apps: LlmAppFilter = Field(default_factory=LlmAppFilter)
    rules: list[LlmRouterRule] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _legacy_list(cls, value: Any) -> Any:
        return {"allowedModels": value} if isinstance(value, list) else value

    @model_validator(mode="after")
    def _rule_names(self) -> LlmRouterConfig:
        names = [rule.name for rule in self.rules if rule.name is not None]
        if len(names) != len(set(names)):
            raise ValueError("rule names must be unique")
        return self

    def to_document(self) -> dict[str, Any]:
        return self.model_dump(mode="json", by_alias=True)
