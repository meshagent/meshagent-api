from pydantic import BaseModel, PositiveInt
from typing import Optional, Literal
from datetime import datetime, timezone
from meshagent.api.accounts_client import Port, Service, Endpoint


class ServicePortEndpointSpec(BaseModel):
    path: str
    identity: str
    type: Optional[Literal["mcp.sse", "meshagent.callable", "http", "tcp"]] = None


class ServicePortSpec(BaseModel):
    num: Literal["*"] | PositiveInt
    type: Literal["mcp.sse", "meshagent.callable", "http", "tcp"]
    endpoints: list[ServicePortEndpointSpec] = []
    liveness: Optional[str] = None


class ServiceSpec(BaseModel):
    version: Literal["v1"]
    kind: Literal["Service"]
    id: Optional[str] = None
    name: str
    command: Optional[str] = None
    image: str
    ports: Optional[list[ServicePortSpec]] = []
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
                    )
                )
            ports[p.num] = port
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


class ServiceTemplateVariable(BaseModel):
    name: str
    description: Optional[str]


class ServiceTemplateSpec(BaseModel):
    version: Literal["v1"]
    kind: Literal["ServiceTemplate"]
    service: ServiceSpec
    variables: Optional[list[ServiceTemplateVariable]] = None
    name: str
    description: Optional[str]

    def to_service_spec(self, *, values: dict[str, str]) -> ServiceSpec:
        env = self.service.environment if self.service.environment is not None else {}
        env = env.copy()
        for k, v in env.items():
            env[k] = v.format_map(v, values)

        return ServiceSpec(
            version=self.version,
            kind=self.service.kind,
            id=self.service.id,
            name=self.service.name,
            command=self.service.command,
            image=self.service.image,
            ports=self.service.ports,
            role=self.service.role,
            environment=env,
            secrets=self.service.secrets,
            pull_secret=self.service.pull_secret,
            room_storage_path=self.service.room_storage_path,
            room_storage_subpath=self.service.room_storage_subpath,
        )
