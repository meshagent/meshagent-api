from pydantic import BaseModel, ConfigDict
from typing import Optional


class ConnectorRef(BaseModel):
    openai_connector_id: Optional[str] = None
    server_url: Optional[str] = None
    client_secret_id: Optional[str] = None


class OAuthClientConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    authorization_endpoint: Optional[str] = None
    token_endpoint: Optional[str] = None
    no_pkce: Optional[bool] = None
    scopes: Optional[list[str]] = None
