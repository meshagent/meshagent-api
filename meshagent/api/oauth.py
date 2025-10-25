from pydantic import BaseModel, ConfigDict
from typing import Optional


class ConnectorRef(BaseModel):
    openai_connector_id: Optional[str] = None
    server_url: Optional[str] = None


class OAuthClientConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    client_id: str
    client_secret: Optional[str] = None
    authorization_endpoint: str
    token_endpoint: str
    no_pkce: Optional[bool] = None
    scopes: Optional[list[str]] = None
