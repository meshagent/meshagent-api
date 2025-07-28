from pydantic import BaseModel, PositiveInt
from typing import Optional, Literal
from datetime import datetime, timezone
from meshagent.api.accounts_client import Port, Service, Endpoint


class ServiceSpec(BaseModel):
    version: Literal["v1"]
    kind: Literal["Service"]
    id: Optional[str] = None
    name: str
    command: Optional[str] = None
    image: str
    ports: Optional[list["ServicePortSpec"]] = []
    role: Optional[Literal["user", "tool", "agent"]] = None
    environment: Optional[dict[str, str]] = {}
    secrets: list[str] = []
    pull_secret: Optional[str] = None
    room_storage_path: Optional[str] = None
    room_storage_subpath: Optional[str] = None

    def to_service(self):
        ports = {}
        for p in self.ports:
            port = Port(liveness_path=p.liveness, type=p.type, endpoints=[])
            for endpoint in p.endpoints:
                type = port.type
                if endpoint.type is not None:
                    type = endpoint.type

                port.endpoints.append(
                    Endpoint(
                        type=type,
                        participant_name=endpoint.identity,
                        path=endpoint.path,
                        role=endpoint.role,
                    )
                )
            ports[str(p.num)] = port
        return Service(
            id="",
            created_at=datetime.now(timezone.utc).isoformat(),
            name=self.name,
            command=self.command,
            image=self.image,
            ports=ports,
            role=self.role,
            environment=self.environment,
            environment_secrets=self.secrets,
            pull_secret=self.pull_secret,
            room_storage_path=self.room_storage_path,
            room_storage_subpath=self.room_storage_subpath,
        )


class ServicePortEndpointSpec(BaseModel):
    path: str
    identity: str
    role: Optional[Literal["user", "tool", "agent"]] = None
    type: Optional[Literal["mcp.sse", "meshagent.callable", "http", "tcp"]] = None


class ServicePortSpec(BaseModel):
    num: Literal["*"] | PositiveInt
    type: Optional[Literal["mcp.sse", "meshagent.callable", "http", "tcp"]] = None
    endpoints: list[ServicePortEndpointSpec] = []
    liveness: Optional[str] = None


class ServiceTemplateVariable(BaseModel):
    name: str
    description: Optional[str] = None
    obscure: bool = False
    enum: Optional[list[str]] = None
    optional: bool = False


class ServiceTemplateEnvironmentVariable(BaseModel):
    name: str
    value: str


class ServiceTemplateSpec(BaseModel):
    version: Literal["v1"]
    kind: Literal["ServiceTemplate"]
    variables: Optional[list[ServiceTemplateVariable]] = None
    environment: Optional[list[ServiceTemplateEnvironmentVariable]] = None
    name: str
    image: Optional[str] = None
    description: Optional[str] = None
    ports: list[ServicePortSpec] = []
    command: Optional[str] = None
    role: Optional[Literal["user", "tool", "agent"]] = None
    room_storage_path: Optional[str] = None
    room_storage_subpath: Optional[str] = None

    def to_service_spec(self, *, values: dict[str, str]) -> ServiceSpec:
        env = {}
        if self.environment is not None:
            for e in self.environment:
                env[e.name] = e.value.format_map(values)

        return ServiceSpec(
            version=self.version,
            kind="Service",
            name=self.name,
            command=self.command,
            image=self.image,
            ports=self.ports,
            role=self.role,
            environment=env,
            # pull_secret=self.pull_secret,
            room_storage_path=self.room_storage_path,
            room_storage_subpath=self.room_storage_subpath,
        )
