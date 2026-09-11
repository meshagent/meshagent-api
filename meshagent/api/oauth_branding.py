from typing import Literal

from pydantic import BaseModel, HttpUrl, field_validator


class OAuthClientBranding(BaseModel):
    """Optional login branding stored in an OAuth client's metadata."""

    logo_url: HttpUrl | None = None
    theme: Literal["light", "dark", "auto"] = "auto"

    @field_validator("logo_url", mode="before")
    @classmethod
    def empty_logo_is_default(cls, value: object) -> object:
        return None if value == "" else value
