from __future__ import annotations

from typing import Annotated, Literal, TypeAlias

from pydantic import BaseModel, Field

AGENT_CONTENT_TYPE_TEXT = "text"
AGENT_CONTENT_TYPE_FILE = "file"


class AgentContent(BaseModel):
    pass


class AgentTextContent(AgentContent):
    type: Literal[AGENT_CONTENT_TYPE_TEXT]
    text: str


class AgentFileContent(AgentContent):
    type: Literal[AGENT_CONTENT_TYPE_FILE]
    url: str


AgentInputContent: TypeAlias = Annotated[
    AgentTextContent | AgentFileContent,
    Field(discriminator="type"),
]
