import json

import pytest
from pydantic import ValidationError

from meshagent.api import ParticipantGrant, ParticipantToken
from meshagent.api.managed_agents import (
    AllowedOpenAIModel,
    ManagedAgentMetadata,
    ManagedAgentSpec,
)
from meshagent.api.participant_token import ApiScope
from meshagent.api.client import ManagedAgentGrant, Meshagent
from meshagent.api.specs.service import (
    RouteBackendSpec,
    RouteMetadata,
    RoutePathSpec,
    RouteRoomBackendSpec,
    RouteSpec,
    ScheduledTaskQueueSpec,
    ScheduledTaskSpec,
    ServiceMetadata,
    ServiceSpec,
)


class _FakeResponse:
    def __init__(self, *, status: int, payload: dict):
        self.status = status
        self._payload = payload

    async def json(self):
        return self._payload

    async def text(self) -> str:
        return json.dumps(self._payload)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse]):
        self._responses = responses
        self.closed = False
        self.calls: list[tuple[str, str, dict | None]] = []

    def post(self, url: str, *, headers=None, json=None, data=None):
        self.calls.append(("post", url, data if data is not None else json))
        return self._responses.pop(0)

    def put(self, url: str, *, headers=None, json=None):
        self.calls.append(("put", url, json))
        return self._responses.pop(0)

    def get(self, url: str, *, headers=None, params=None):
        self.calls.append(("get", url, params))
        return self._responses.pop(0)

    def delete(self, url: str, *, headers=None, params=None):
        self.calls.append(("delete", url, params))
        return self._responses.pop(0)

    async def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_render_template_accepts_decoded_json_response():
    session = _FakeSession(
        [
            _FakeResponse(
                status=200,
                payload={
                    "version": "v1",
                    "kind": "ServiceTemplate",
                    "metadata": {"name": "eli"},
                    "variables": [],
                },
            )
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    template = await client.render_template(
        template="version: v1\nkind: ServiceTemplate\nmetadata:\n  name: eli\n",
        values={},
    )

    assert template.metadata.name == "eli"
    assert session.calls == [
        (
            "post",
            "http://example.test/templates/render",
            {
                "template": "version: v1\nkind: ServiceTemplate\nmetadata:\n  name: eli\n",
                "values": {},
            },
        )
    ]


@pytest.mark.asyncio
async def test_create_route_omits_default_strip_prefix_from_paths():
    session = _FakeSession([_FakeResponse(status=200, payload={})])
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    await client.create_route(
        project_id="project-1",
        spec=RouteSpec(
            metadata=RouteMetadata(name="app.example.test"),
            domain="app.example.test",
            backend=RouteBackendSpec(room=RouteRoomBackendSpec(name="room-1")),
            paths=[RoutePathSpec(path="/", targetPort=3000)],
        ),
    )

    assert session.calls == [
        (
            "post",
            "http://example.test/accounts/projects/project-1/routes",
            {
                "spec": {
                    "version": "v1",
                    "kind": "Route",
                    "metadata": {"name": "app.example.test", "annotations": {}},
                    "domain": "app.example.test",
                    "backend": {"room": {"name": "room-1"}, "agent": None},
                    "paths": [
                        {
                            "path": "/",
                            "pathType": "prefix",
                            "targetPort": 3000,
                        }
                    ],
                }
            },
        )
    ]


@pytest.mark.asyncio
async def test_create_route_serializes_true_strip_prefix():
    session = _FakeSession([_FakeResponse(status=200, payload={})])
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    await client.create_route(
        project_id="project-1",
        spec=RouteSpec(
            metadata=RouteMetadata(name="app.example.test"),
            domain="app.example.test",
            backend=RouteBackendSpec(room=RouteRoomBackendSpec(name="room-1")),
            paths=[
                RoutePathSpec(path="/api", targetPort=3000, stripPrefix=True),
            ],
        ),
    )

    assert session.calls[0][2]["spec"]["paths"] == [
        {
            "path": "/api",
            "pathType": "prefix",
            "stripPrefix": True,
            "targetPort": 3000,
        }
    ]


@pytest.mark.asyncio
async def test_exchange_oauth_token_posts_form_encoded_token_request():
    session = _FakeSession(
        [
            _FakeResponse(
                status=200,
                payload={
                    "access_token": "access-token",
                    "refresh_token": "refresh-token",
                    "expires_in": 3600,
                    "token_type": "Bearer",
                    "scope": "admin",
                },
            )
        ]
    )
    client = Meshagent(base_url="http://example.test", token="", session=session)

    token_response = await client.exchange_oauth_token(
        form={
            "grant_type": "authorization_code",
            "code": "auth-code",
            "client_id": "client-id",
        }
    )

    assert token_response.access_token == "access-token"
    assert token_response.refresh_token == "refresh-token"
    assert session.calls == [
        (
            "post",
            "http://example.test/oauth/token",
            {
                "grant_type": "authorization_code",
                "code": "auth-code",
                "client_id": "client-id",
            },
        )
    ]


@pytest.mark.asyncio
async def test_create_service_from_template_accepts_decoded_json_response():
    session = _FakeSession(
        [
            _FakeResponse(
                status=200,
                payload={
                    "version": "v1",
                    "kind": "Service",
                    "id": "svc_123",
                    "metadata": {"name": "eli"},
                },
            )
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    service = await client.create_service_from_template(
        project_id="proj_123",
        template="version: v1\nkind: ServiceTemplate\nmetadata:\n  name: eli\n",
        values={},
    )

    assert service.id == "svc_123"
    assert service.metadata.name == "eli"
    assert session.calls == [
        (
            "post",
            "http://example.test/accounts/projects/proj_123/services",
            {
                "template": "version: v1\nkind: ServiceTemplate\nmetadata:\n  name: eli\n",
                "values": {},
            },
        )
    ]


@pytest.mark.asyncio
async def test_create_service_omits_client_supplied_id():
    session = _FakeSession(
        [
            _FakeResponse(
                status=200,
                payload={
                    "version": "v1",
                    "kind": "Service",
                    "id": "server-service-id",
                    "metadata": {"name": "worker"},
                },
            )
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)
    service_spec = ServiceSpec(
        version="v1",
        kind="Service",
        id="client-service-id",
        metadata=ServiceMetadata(name="worker"),
    )

    service = await client.create_service(
        project_id="proj_123",
        service=service_spec,
    )

    assert service.id == "server-service-id"
    assert session.calls == [
        (
            "post",
            "http://example.test/accounts/projects/proj_123/services",
            {
                "version": "v1",
                "kind": "Service",
                "metadata": {"name": "worker"},
                "ports": [],
            },
        )
    ]


@pytest.mark.asyncio
async def test_create_room_service_omits_client_supplied_id():
    session = _FakeSession(
        [_FakeResponse(status=200, payload={"id": "server-service-id"})]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)
    service_spec = ServiceSpec(
        version="v1",
        kind="Service",
        id="client-service-id",
        metadata=ServiceMetadata(name="worker"),
    )

    service_id = await client.create_room_service(
        project_id="proj_123",
        room_name="room-1",
        service=service_spec,
    )

    assert service_id == "server-service-id"
    assert session.calls == [
        (
            "post",
            "http://example.test/accounts/projects/proj_123/rooms/room-1/services",
            {
                "version": "v1",
                "kind": "Service",
                "metadata": {"name": "worker"},
                "ports": [],
            },
        )
    ]


@pytest.mark.asyncio
async def test_get_service_by_name_uses_by_name_endpoint():
    session = _FakeSession(
        [
            _FakeResponse(
                status=200,
                payload={
                    "version": "v1",
                    "kind": "Service",
                    "id": "svc_123",
                    "metadata": {"name": "worker"},
                },
            )
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    service = await client.get_service_by_name(
        project_id="proj_123", service_name="worker/service"
    )

    assert service.id == "svc_123"
    assert session.calls == [
        (
            "get",
            "http://example.test/accounts/projects/proj_123/services/by-name/worker%2Fservice",
            None,
        )
    ]


@pytest.mark.asyncio
async def test_get_room_service_by_name_uses_by_name_endpoint():
    session = _FakeSession(
        [
            _FakeResponse(
                status=200,
                payload={
                    "version": "v1",
                    "kind": "Service",
                    "id": "svc_123",
                    "metadata": {"name": "worker"},
                },
            )
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    service = await client.get_room_service_by_name(
        project_id="proj_123", room_name="room 1", service_name="worker/service"
    )

    assert service.id == "svc_123"
    assert session.calls == [
        (
            "get",
            "http://example.test/accounts/projects/proj_123/rooms/room%201/services/by-name/worker%2Fservice",
            None,
        )
    ]


@pytest.mark.asyncio
async def test_update_scheduled_task_replaces_spec():
    session = _FakeSession([_FakeResponse(status=200, payload={})])
    client = Meshagent(base_url="http://example.test", token="token", session=session)
    spec = ScheduledTaskSpec(
        schedule="0 * * * *",
        queue=ScheduledTaskQueueSpec(name="jobs", payload={"action": "sync"}),
    )

    await client.update_scheduled_task(
        project_id="proj_123",
        task_id="task_123",
        spec=spec,
    )

    assert session.calls == [
        (
            "put",
            "http://example.test/accounts/projects/proj_123/scheduled-tasks/task_123",
            {
                "spec": {
                    "version": "v1",
                    "kind": "ScheduledTask",
                    "metadata": {"annotations": {}},
                    "schedule": "0 * * * *",
                    "active": True,
                    "once": False,
                    "queue": {"name": "jobs", "payload": {"action": "sync"}},
                },
            },
        )
    ]


@pytest.mark.asyncio
async def test_create_scheduled_task_uses_room_scoped_route():
    session = _FakeSession([_FakeResponse(status=200, payload={"task_id": "task_123"})])
    client = Meshagent(base_url="http://example.test", token="token", session=session)
    spec = ScheduledTaskSpec(
        schedule="0 * * * *",
        queue=ScheduledTaskQueueSpec(
            name="jobs",
            storage_write_path="scheduled/outbox",
        ),
    )

    task_id = await client.create_scheduled_task(
        project_id="proj_123",
        room_name="room a/b",
        spec=spec,
    )

    assert task_id == "task_123"
    assert session.calls == [
        (
            "post",
            "http://example.test/accounts/projects/proj_123/rooms/room%20a%2Fb/scheduled-tasks",
            {
                "spec": {
                    "version": "v1",
                    "kind": "ScheduledTask",
                    "metadata": {"annotations": {}},
                    "schedule": "0 * * * *",
                    "active": True,
                    "once": False,
                    "queue": {
                        "name": "jobs",
                        "payload": {},
                        "storage_write_path": "scheduled/outbox",
                    },
                },
            },
        )
    ]


@pytest.mark.parametrize("schedule", ["*/15 * * * *", "15 minutes", "0 * * * *"])
def test_scheduled_task_spec_accepts_minimum_interval(schedule: str) -> None:
    spec = ScheduledTaskSpec(
        schedule=schedule,
        queue=ScheduledTaskQueueSpec(name="jobs"),
    )

    assert spec.schedule == schedule


@pytest.mark.parametrize("schedule", ["* * * * *", "*/5 * * * *", "5 minutes"])
def test_scheduled_task_spec_rejects_too_frequent_schedules(schedule: str) -> None:
    with pytest.raises(ValidationError, match="every 15 minutes"):
        ScheduledTaskSpec(
            schedule=schedule,
            queue=ScheduledTaskQueueSpec(name="jobs"),
        )


@pytest.mark.asyncio
async def test_get_config_returns_typed_deployment_config():
    session = _FakeSession(
        [
            _FakeResponse(
                status=200,
                payload={
                    "domains": {
                        "studio": "studio.meshagent.life",
                        "accounts": "accounts.meshagent.life",
                        "api": "api.meshagent.life",
                        "mail": "mail.meshagent.life",
                        "pages": "meshagent.life",
                        "registry": "registry.meshagent.life",
                    },
                    "version": "0.41.5",
                },
            )
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    config = await client.get_config()

    assert config.domains.registry == "registry.meshagent.life"
    assert config.domains.api == "api.meshagent.life"
    assert config.version == "0.41.5"
    assert session.calls == [
        (
            "get",
            "http://example.test/config",
            None,
        )
    ]


@pytest.mark.asyncio
async def test_can_use_llm_proxy_reads_role_capability_flag():
    session = _FakeSession(
        [
            _FakeResponse(
                status=200,
                payload={
                    "role": "member",
                    "can_create_rooms": False,
                    "can_use_llm_proxy": True,
                },
            )
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    can_use_llm_proxy = await client.can_use_llm_proxy("proj_123")

    assert can_use_llm_proxy is True
    assert session.calls == [
        (
            "get",
            "http://example.test/accounts/projects/proj_123/role",
            None,
        )
    ]


@pytest.mark.asyncio
async def test_can_create_rooms_reads_current_user_role_capability_flag():
    session = _FakeSession(
        [
            _FakeResponse(
                status=200,
                payload={
                    "role": "member",
                    "can_create_rooms": True,
                    "can_use_llm_proxy": False,
                    "is_developer": False,
                },
            )
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    can_create_rooms = await client.can_create_rooms("proj_123")

    assert can_create_rooms is True
    assert session.calls == [
        (
            "get",
            "http://example.test/accounts/projects/proj_123/role",
            None,
        )
    ]


@pytest.mark.asyncio
async def test_create_project_secret_sends_base64_payload():
    session = _FakeSession([_FakeResponse(status=200, payload={"id": "secret-1"})])
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    secret_id = await client.create_project_secret(
        project_id="proj_123",
        name="registry",
        type="docker",
        data=b'{"server":"registry.example.com"}',
    )

    assert secret_id == "secret-1"
    assert session.calls == [
        (
            "post",
            "http://example.test/accounts/projects/proj_123/secrets",
            {
                "name": "registry",
                "type": "docker",
                "data_base64": "eyJzZXJ2ZXIiOiJyZWdpc3RyeS5leGFtcGxlLmNvbSJ9",
            },
        )
    ]


@pytest.mark.asyncio
async def test_list_secrets_compatibility_wrapper_fetches_secret_payloads():
    session = _FakeSession(
        [
            _FakeResponse(
                status=200,
                payload={
                    "secrets": [
                        {
                            "id": "secret-1",
                            "name": "registry",
                            "type": "docker",
                            "delegated_to": None,
                        }
                    ]
                },
            ),
            _FakeResponse(
                status=200,
                payload={
                    "id": "secret-1",
                    "name": "registry",
                    "type": "docker",
                    "data_base64": "eyJzZXJ2ZXIiOiJyZWdpc3RyeS5leGFtcGxlLmNvbSIsInVzZXJuYW1lIjoiYWxpY2UiLCJwYXNzd29yZCI6InNlY3JldCIsImVtYWlsIjoibm9uZUBleGFtcGxlLmNvbSJ9",
                },
            ),
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    secrets = await client.list_secrets("proj_123")

    assert len(secrets) == 1
    assert secrets[0].id == "secret-1"
    assert secrets[0].name == "registry"
    assert secrets[0].type == "docker"
    assert session.calls == [
        (
            "get",
            "http://example.test/accounts/projects/proj_123/secrets",
            None,
        ),
        (
            "get",
            "http://example.test/accounts/projects/proj_123/secrets/secret-1",
            None,
        ),
    ]


@pytest.mark.asyncio
async def test_room_secret_and_external_oauth_methods_pass_query_parameters():
    session = _FakeSession(
        [
            _FakeResponse(
                status=200,
                payload={
                    "id": "secret-1",
                    "name": "api-key",
                    "type": "application/octet-stream",
                    "delegated_to": "agent",
                    "data_base64": "c2VjcmV0",
                },
            ),
            _FakeResponse(
                status=200,
                payload={
                    "registrations": [
                        {
                            "id": "registration-1",
                            "delegated_to": "agent",
                            "connector": None,
                            "oauth": {
                                "authorization_endpoint": "https://auth.example.com/authorize",
                                "token_endpoint": "https://auth.example.com/token",
                                "client_id": "client-id",
                                "client_secret": None,
                                "scopes": ["openid"],
                            },
                            "client_id": "client-id",
                            "client_secret": "client-secret",
                        }
                    ]
                },
            ),
            _FakeResponse(status=200, payload={}),
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    secret = await client.get_room_secret(
        project_id="proj_123",
        room_name="room-a",
        secret_id="secret-1",
        delegated_to="agent",
        for_identity="agent",
    )
    registrations = await client.list_room_external_oauth_registrations(
        project_id="proj_123",
        room_name="room-a",
        delegated_to="agent",
    )
    await client.delete_room_external_oauth_registration(
        project_id="proj_123",
        room_name="room-a",
        registration_id="registration-1",
        delegated_to="agent",
    )

    assert secret.data == b"secret"
    assert registrations[0].id == "registration-1"
    assert session.calls == [
        (
            "get",
            "http://example.test/accounts/projects/proj_123/rooms/room-a/secrets/secret-1",
            {"delegated_to": "agent", "for_identity": "agent"},
        ),
        (
            "get",
            "http://example.test/accounts/projects/proj_123/rooms/room-a/external-oauth",
            {"delegated_to": "agent"},
        ),
        (
            "delete",
            "http://example.test/accounts/projects/proj_123/rooms/room-a/external-oauth/registration-1",
            {"delegated_to": "agent"},
        ),
    ]


@pytest.mark.asyncio
async def test_agent_secret_methods_use_agent_secret_routes():
    session = _FakeSession(
        [
            _FakeResponse(status=200, payload={"id": "secret-1"}),
            _FakeResponse(
                status=200,
                payload={
                    "secrets": [
                        {
                            "id": "secret-1",
                            "name": "api-key",
                            "type": "keys",
                            "agent_id": "agent-1",
                        }
                    ]
                },
            ),
            _FakeResponse(
                status=200,
                payload={
                    "id": "secret-1",
                    "name": "api-key",
                    "type": "keys",
                    "agent_id": "agent-1",
                    "data_base64": "c2VjcmV0",
                },
            ),
            _FakeResponse(status=200, payload={}),
            _FakeResponse(status=200, payload={}),
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    created_id = await client.create_agent_secret(
        project_id="proj_123",
        agent_id="agent-1",
        secret_id="secret-1",
        name="api-key",
        type="keys",
        data=b"secret",
    )
    listed = await client.list_agent_secrets(project_id="proj_123", agent_id="agent-1")
    fetched = await client.get_agent_secret(
        project_id="proj_123", agent_id="agent-1", secret_id="secret-1"
    )
    await client.update_agent_secret(
        project_id="proj_123",
        agent_id="agent-1",
        secret_id="secret-1",
        name="api-key",
        type="keys",
        data=b"new",
    )
    await client.delete_agent_secret(
        project_id="proj_123", agent_id="agent-1", secret_id="secret-1"
    )

    assert created_id == "secret-1"
    assert listed[0].agent_id == "agent-1"
    assert fetched.data == b"secret"
    assert session.calls == [
        (
            "post",
            "http://example.test/accounts/projects/proj_123/agents/agent-1/secrets",
            {
                "data_base64": "c2VjcmV0",
                "secret_id": "secret-1",
                "name": "api-key",
                "type": "keys",
            },
        ),
        (
            "get",
            "http://example.test/accounts/projects/proj_123/agents/agent-1/secrets",
            None,
        ),
        (
            "get",
            "http://example.test/accounts/projects/proj_123/agents/agent-1/secrets/secret-1",
            None,
        ),
        (
            "put",
            "http://example.test/accounts/projects/proj_123/agents/agent-1/secrets/secret-1",
            {"data_base64": "bmV3", "name": "api-key", "type": "keys"},
        ),
        (
            "delete",
            "http://example.test/accounts/projects/proj_123/agents/agent-1/secrets/secret-1",
            None,
        ),
    ]


@pytest.mark.asyncio
async def test_validate_participant_token_fetches_validated_token():
    payload = ParticipantToken(
        name="assistant",
        project_id="proj_123",
        grants=[
            ParticipantGrant(name="room", scope="room-a"),
            ParticipantGrant(name="api", scope=ApiScope.full()),
        ],
    ).to_json()
    session = _FakeSession([_FakeResponse(status=200, payload=payload)])
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    token = await client.validate_participant_token(token="jwt-token")

    assert token.name == "assistant"
    assert token.project_id == "proj_123"
    assert token.grant_scope("room") == "room-a"
    assert session.calls == [
        (
            "post",
            "http://example.test/api/participant-token/validate",
            {"token": "jwt-token"},
        )
    ]


@pytest.mark.asyncio
async def test_agent_crud_methods_use_agent_routes():
    configuration = ManagedAgentSpec(
        id="agent-1",
        metadata=ManagedAgentMetadata(name="planner"),
        allowed_models=[AllowedOpenAIModel(model="gpt-4.1")],
    )
    agent_payload = {
        "id": "agent-1",
        "name": "planner",
        "configuration": configuration.model_dump(mode="json"),
    }
    session = _FakeSession(
        [
            _FakeResponse(status=200, payload=agent_payload),
            _FakeResponse(status=200, payload=agent_payload),
            _FakeResponse(status=200, payload={"agents": [agent_payload], "total": 1}),
            _FakeResponse(status=200, payload={}),
            _FakeResponse(status=200, payload={}),
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    created = await client.create_agent(
        project_id="proj_123",
        configuration=configuration,
        if_not_exists=True,
        permissions={"user-1": ManagedAgentGrant()},
    )
    fetched = await client.get_agent(project_id="proj_123", name="planner")
    page = await client.list_agents_page(
        project_id="proj_123", limit=10, offset=5, filter="plan"
    )
    await client.update_agent(
        project_id="proj_123",
        agent_id="agent-1",
        configuration=configuration,
    )
    await client.delete_agent(project_id="proj_123", agent_id="agent-1")

    assert created.name == "planner"
    assert fetched.id == "agent-1"
    assert page.total == 1
    assert session.calls == [
        (
            "post",
            "http://example.test/accounts/projects/proj_123/agents",
            {
                "configuration": configuration.model_dump(mode="json"),
                "if_not_exists": True,
                "permissions": {"user-1": {"admin": False}},
            },
        ),
        (
            "get",
            "http://example.test/accounts/projects/proj_123/agents/planner",
            None,
        ),
        (
            "get",
            "http://example.test/accounts/projects/proj_123/agents",
            {
                "limit": "10",
                "offset": "5",
                "order_by": "agent_name",
                "filter": "plan",
            },
        ),
        (
            "put",
            "http://example.test/accounts/projects/proj_123/agents/agent-1",
            {"configuration": configuration.model_dump(mode="json")},
        ),
        (
            "delete",
            "http://example.test/accounts/projects/proj_123/agents/agent-1",
            None,
        ),
    ]


@pytest.mark.asyncio
async def test_agent_grant_methods_use_agent_grant_routes():
    configuration = {
        "id": "agent-1",
        "name": "planner",
        "allowed_models": [{"provider": "openai", "model": "gpt-4.1"}],
    }
    grant_payload = {
        "agent": {
            "id": "agent-1",
            "name": "planner",
            "configuration": configuration,
            "metadata": {},
            "annotations": {},
        },
        "user_id": "user-1",
        "permissions": {"admin": False},
    }
    user_grant_payload = {
        **grant_payload,
        "user": {
            "id": "user-1",
            "first_name": "Ada",
            "last_name": "Lovelace",
            "email": "ada@example.test",
        },
    }
    session = _FakeSession(
        [
            _FakeResponse(status=200, payload={}),
            _FakeResponse(status=200, payload={}),
            _FakeResponse(status=200, payload={}),
            _FakeResponse(status=200, payload=grant_payload),
            _FakeResponse(
                status=200, payload={"agent_grants": [grant_payload], "total": 1}
            ),
            _FakeResponse(status=200, payload={"agent_grants": [grant_payload]}),
            _FakeResponse(status=200, payload={"agent_grants": [user_grant_payload]}),
            _FakeResponse(
                status=200,
                payload={
                    "agents": [
                        {
                            "agent": grant_payload["agent"],
                            "count": 1,
                        }
                    ]
                },
            ),
            _FakeResponse(status=200, payload={}),
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    await client.create_agent_grant(
        project_id="proj_123", agent_id="agent-1", user_id="user-1"
    )
    await client.create_agent_grant_by_email(
        project_id="proj_123",
        agent_id="agent-1",
        email="ada@example.test",
        invite_redirect_url="https://studio.example.test",
    )
    await client.update_agent_grant(
        project_id="proj_123", agent_id="agent-1", user_id="user-1"
    )
    grant = await client.get_agent_grant(
        project_id="proj_123", agent_id="agent-1", user_id="user-1"
    )
    page = await client.list_agent_grants_by_user_page(
        project_id="proj_123", user_id="me", filter="plan"
    )
    grants = await client.list_agent_grants_by_agent(
        project_id="proj_123", agent_name="planner"
    )
    members = await client.list_agent_members_by_agent(
        project_id="proj_123", agent_name="planner"
    )
    counts = await client.list_unique_agents_with_grants(project_id="proj_123")
    await client.delete_agent_grant(
        project_id="proj_123", agent_id="agent-1", user_id="user-1"
    )

    assert grant.agent.name == "planner"
    assert page.total == 1
    assert grants[0].user_id == "user-1"
    assert members[0].user.email == "ada@example.test"
    assert counts[0].count == 1
    assert session.calls == [
        (
            "post",
            "http://example.test/accounts/projects/proj_123/agent-grants",
            {
                "agent_id": "agent-1",
                "user_id": "user-1",
                "email": None,
                "permissions": {"admin": False},
                "invite_redirect_url": None,
            },
        ),
        (
            "post",
            "http://example.test/accounts/projects/proj_123/agent-grants",
            {
                "agent_id": "agent-1",
                "user_id": None,
                "email": "ada@example.test",
                "permissions": {"admin": False},
                "invite_redirect_url": "https://studio.example.test",
            },
        ),
        (
            "put",
            "http://example.test/accounts/projects/proj_123/agent-grants/unused",
            {
                "agent_id": "agent-1",
                "user_id": "user-1",
                "permissions": {"admin": False},
            },
        ),
        (
            "get",
            "http://example.test/accounts/projects/proj_123/agent-grants/agent-1/user-1",
            None,
        ),
        (
            "get",
            "http://example.test/accounts/projects/proj_123/agent-grants/by-user/me",
            {
                "limit": "100",
                "offset": "0",
                "order_by": "agent_name",
                "filter": "plan",
            },
        ),
        (
            "get",
            "http://example.test/accounts/projects/proj_123/agent-grants/by-agent/planner",
            {"limit": "50", "offset": "0"},
        ),
        (
            "get",
            "http://example.test/accounts/projects/proj_123/members/by-agent/planner",
            {"limit": "50", "offset": "0"},
        ),
        (
            "get",
            "http://example.test/accounts/projects/proj_123/agent-grants/by-agent",
            {"limit": "50", "offset": "0"},
        ),
        (
            "delete",
            "http://example.test/accounts/projects/proj_123/agent-grants/agent-1/user-1",
            None,
        ),
    ]
