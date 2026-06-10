import json
from datetime import datetime

import pytest
from pydantic import ValidationError

from meshagent.api import ParticipantGrant, ParticipantToken
from meshagent.api.managed_agents import (
    AllowedOpenAIModel,
    ManagedAgentMetadata,
    ManagedAgentSpec,
)
from meshagent.api.participant_token import ApiScope
from meshagent.api.client import AccessResource, AccessSubject, Meshagent
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
async def test_mint_participant_token_accepts_serialized_grants():
    session = _FakeSession([_FakeResponse(status=200, payload={"token": "jwt-token"})])
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    token = await client.mint_participant_token(
        "proj_123",
        name="worker",
        grants=[
            {"name": "role", "scope": "agent"},
            {"name": "room", "scope": "room-1"},
            {"name": "tunnel_ports", "scope": "9000"},
        ],
    )

    assert token == "jwt-token"
    assert session.calls == [
        (
            "post",
            "http://example.test/accounts/projects/proj_123/participant-tokens",
            {
                "name": "worker",
                "grants": [
                    {"name": "role", "scope": "agent"},
                    {"name": "room", "scope": "room-1"},
                    {"name": "tunnel_ports", "scope": "9000"},
                ],
            },
        )
    ]


@pytest.mark.asyncio
async def test_get_current_user_llm_proxy_usage_builds_request_and_reads_usage():
    session = _FakeSession(
        [
            _FakeResponse(
                status=200,
                payload={
                    "usage": [
                        {
                            "provider": "openai",
                            "model": "gpt-4.1-mini",
                            "type": "llm_proxy_surcharge",
                            "total": 1.25,
                        }
                    ]
                },
            )
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    usage = await client.get_current_user_llm_proxy_usage(
        "proj_123",
        start=datetime.fromisoformat("2026-04-01T00:00:00+00:00"),
        end=datetime.fromisoformat("2026-04-30T00:00:00+00:00"),
        interval="day",
        annotations={"env": "prod"},
    )

    assert usage == [
        {
            "provider": "openai",
            "model": "gpt-4.1-mini",
            "type": "llm_proxy_surcharge",
            "total": 1.25,
        }
    ]
    assert session.calls == [
        (
            "get",
            "http://example.test/accounts/projects/proj_123/llm-proxy/usage",
            {
                "start": "2026-04-01T00:00:00+00:00",
                "end": "2026-04-30T00:00:00+00:00",
                "interval": "day",
                "annotations": '{"env": "prod"}',
            },
        )
    ]


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
async def test_create_oauth_client_accepts_client_envelope():
    session = _FakeSession(
        [
            _FakeResponse(
                status=200,
                payload={
                    "client": {
                        "client_id": "client-1",
                        "client_secret": "secret-1",
                        "grant_types": ["authorization_code"],
                        "response_types": ["code"],
                        "redirect_uris": ["https://example.test/callback"],
                        "scope": "rooms:read",
                        "project_id": "proj_123",
                        "metadata": {"name": "smoke"},
                        "official": True,
                    }
                },
            )
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    oauth_client = await client.create_oauth_client(
        project_id="proj_123",
        grant_types=["authorization_code"],
        response_types=["code"],
        redirect_uris=["https://example.test/callback"],
        scope="rooms:read",
        metadata={"name": "smoke"},
        official=True,
    )

    assert oauth_client.client_id == "client-1"
    assert oauth_client.official is True
    assert session.calls == [
        (
            "post",
            "http://example.test/accounts/projects/proj_123/oauth/clients",
            {
                "grant_types": ["authorization_code"],
                "response_types": ["code"],
                "redirect_uris": ["https://example.test/callback"],
                "scope": "rooms:read",
                "metadata": {"name": "smoke"},
                "official": True,
            },
        )
    ]


@pytest.mark.asyncio
async def test_create_route_omits_default_strip_prefix_from_paths():
    session = _FakeSession(
        [
            _FakeResponse(status=200, payload={}),
            _FakeResponse(status=200, payload={}),
        ]
    )
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
async def test_list_group_members_page_returns_typed_members():
    session = _FakeSession(
        [
            _FakeResponse(
                status=200,
                payload={
                    "members": [
                        {
                            "subject": {
                                "type": "user",
                                "id": "user-1",
                                "name": "Ada Lovelace",
                                "first_name": "Ada",
                                "last_name": "Lovelace",
                                "email": "ada@example.test",
                            },
                            "direct_roles": ["member", "manager"],
                        },
                        {
                            "subject": {
                                "type": "agent",
                                "id": "agent-1",
                                "name": "planner",
                            },
                            "direct_roles": ["member"],
                        },
                        {
                            "subject": {
                                "type": "group",
                                "id": "group-child",
                                "name": "child group",
                            },
                            "direct_roles": ["member"],
                        },
                    ],
                    "continuation_token": "next-page",
                },
            )
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    page = await client.list_group_members_page(
        project_id="proj_123",
        group_id="group-1",
        page_size=50,
        continuation_token="member-cursor",
    )

    assert page.continuation_token == "next-page"
    assert page.members[0].subject.email == "ada@example.test"
    assert page.members[0].direct_roles == ["member", "manager"]
    assert page.members[1].subject.type == "agent"
    assert page.members[2].subject.type == "group"
    assert session.calls == [
        (
            "get",
            "http://example.test/accounts/projects/proj_123/groups/group-1/members",
            {"page_size": 50, "continuation_token": "member-cursor"},
        )
    ]


@pytest.mark.asyncio
async def test_delete_group_member_accepts_agent_and_group_subjects():
    session = _FakeSession(
        [
            _FakeResponse(status=200, payload={}),
            _FakeResponse(status=200, payload={}),
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    await client.delete_group_member(
        project_id="proj_123",
        group_id="group-1",
        subject_type="agent",
        subject_id="agent-1",
    )
    await client.delete_group_member(
        project_id="proj_123",
        group_id="group-1",
        subject_type="group",
        subject_id="group-child",
    )

    assert session.calls == [
        (
            "delete",
            "http://example.test/accounts/projects/proj_123/groups/group-1/members/agent/agent-1",
            None,
        ),
        (
            "delete",
            "http://example.test/accounts/projects/proj_123/groups/group-1/members/group/group-child",
            None,
        ),
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


@pytest.mark.asyncio
async def test_list_scheduled_tasks_uses_page_size_and_continuation_token():
    spec = ScheduledTaskSpec(
        schedule="0 * * * *",
        queue=ScheduledTaskQueueSpec(name="jobs"),
    )
    task_payload = {
        "id": "task_123",
        "project_id": "proj_123",
        "room_id": "room_123",
        "room_name": "room-a",
        "spec": spec.model_dump(mode="json"),
        "schedule": "0 * * * *",
        "active": True,
        "once": False,
        "annotations": {},
    }
    session = _FakeSession(
        [
            _FakeResponse(
                status=200,
                payload={
                    "tasks": [task_payload],
                    "continuation_token": "next-page",
                },
            ),
            _FakeResponse(
                status=200,
                payload={"tasks": [task_payload], "total": 1},
            ),
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    project_page = await client.list_scheduled_tasks_page(
        project_id="proj_123",
        page_size=25,
        continuation_token="cursor-1",
        filter="sync",
    )
    room_page = await client.list_scheduled_tasks_page(
        project_id="proj_123",
        room_id="room_123",
        page_size=10,
        offset=20,
    )

    assert [task.id for task in project_page.tasks] == ["task_123"]
    assert project_page.continuation_token == "next-page"
    assert [task.id for task in room_page.tasks] == ["task_123"]
    assert room_page.total == 1
    assert session.calls == [
        (
            "get",
            "http://example.test/accounts/projects/proj_123/scheduled-tasks",
            {
                "page_size": "25",
                "continuation_token": "cursor-1",
                "filter": "sync",
            },
        ),
        (
            "get",
            "http://example.test/accounts/projects/proj_123/scheduled-tasks",
            {
                "page_size": "10",
                "room_id": "room_123",
                "offset": "20",
            },
        ),
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
            {"view": "all"},
        ),
        (
            "get",
            "http://example.test/accounts/projects/proj_123/secrets/secret-1",
            None,
        ),
    ]


@pytest.mark.asyncio
async def test_list_project_secrets_sends_selected_view():
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
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    secrets = await client.list_project_secrets(project_id="proj_123", view="my")

    assert [secret.id for secret in secrets] == ["secret-1"]
    assert session.calls == [
        (
            "get",
            "http://example.test/accounts/projects/proj_123/secrets",
            {"view": "my"},
        )
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
    )
    fetched = await client.get_agent(project_id="proj_123", name="planner")
    page = await client.list_agents_page(
        project_id="proj_123", page_size=10, filter="plan"
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
                "page_size": "10",
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
async def test_revoke_api_keys_by_msid_uses_service_account_route():
    session = _FakeSession(
        [_FakeResponse(status=200, payload={"revoked": ["key-1", "key-2"]})]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    result = await client.revoke_api_keys_by_msid(
        project_id="proj_123",
        service_account_id="service-account-1",
        msid="oauth-msid",
    )

    assert result.revoked == ["key-1", "key-2"]
    assert session.calls == [
        (
            "post",
            "http://example.test/accounts/projects/proj_123/service-accounts/service-account-1/api-keys:revoke",
            {"msid": "oauth-msid"},
        )
    ]


@pytest.mark.asyncio
async def test_service_account_and_scoped_api_key_methods_return_typed_models():
    service_account_payload = {
        "id": "service-account-1",
        "project_id": "proj_123",
        "key": "deploy-bot",
        "name": "deploy-bot",
        "description": "Deploy bot",
        "metadata": {"env": "test"},
        "annotations": {"team": "platform"},
    }
    api_key_payload = {
        "id": "key-1",
        "name": "ci",
        "description": "CI key",
        "project_id": "proj_123",
        "service_account_id": "service-account-1",
        "value": "secret-key",
    }
    session = _FakeSession(
        [
            _FakeResponse(status=200, payload=service_account_payload),
            _FakeResponse(status=200, payload=service_account_payload),
            _FakeResponse(
                status=200,
                payload={"service_accounts": [service_account_payload]},
            ),
            _FakeResponse(status=200, payload={"ok": True}),
            _FakeResponse(status=200, payload=api_key_payload),
            _FakeResponse(status=200, payload={"keys": [api_key_payload]}),
            _FakeResponse(status=204, payload={}),
            _FakeResponse(status=200, payload={"ok": True}),
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    created = await client.create_service_account(
        "proj_123",
        name="deploy-bot",
        description="Deploy bot",
        metadata={"env": "test"},
        annotations={"team": "platform"},
    )
    fetched = await client.get_service_account("proj_123", "service-account-1")
    page = await client.list_service_accounts("proj_123", page_size=50, filter="bot")
    await client.update_service_account(
        "proj_123",
        "service-account-1",
        name="deploy-bot-renamed",
        description="Deploy bot renamed",
    )
    api_key = await client.create_api_key(
        "proj_123",
        name="ci",
        description="CI key",
        service_account_id="service-account-1",
    )
    api_keys = await client.list_api_keys("proj_123", "service-account-1")
    await client.delete_api_key("proj_123", "key-1", "service-account-1")
    await client.delete_service_account("proj_123", "service-account-1")

    assert created.id == "service-account-1"
    assert fetched.key == "deploy-bot"
    assert page.service_accounts[0].metadata == {"env": "test"}
    assert api_key.value == "secret-key"
    assert api_keys.keys[0].service_account_id == "service-account-1"
    assert session.calls == [
        (
            "post",
            "http://example.test/accounts/projects/proj_123/service-accounts",
            {
                "name": "deploy-bot",
                "description": "Deploy bot",
                "metadata": {"env": "test"},
                "annotations": {"team": "platform"},
            },
        ),
        (
            "get",
            "http://example.test/accounts/projects/proj_123/service-accounts/service-account-1",
            None,
        ),
        (
            "get",
            "http://example.test/accounts/projects/proj_123/service-accounts",
            {"page_size": 50, "filter": "bot"},
        ),
        (
            "put",
            "http://example.test/accounts/projects/proj_123/service-accounts/service-account-1",
            {
                "name": "deploy-bot-renamed",
                "description": "Deploy bot renamed",
            },
        ),
        (
            "post",
            "http://example.test/accounts/projects/proj_123/service-accounts/service-account-1/api-keys",
            {"name": "ci", "description": "CI key"},
        ),
        (
            "get",
            "http://example.test/accounts/projects/proj_123/service-accounts/service-account-1/api-keys",
            None,
        ),
        (
            "delete",
            "http://example.test/accounts/projects/proj_123/service-accounts/service-account-1/api-keys/key-1",
            None,
        ),
        (
            "delete",
            "http://example.test/accounts/projects/proj_123/service-accounts/service-account-1",
            None,
        ),
    ]


@pytest.mark.asyncio
async def test_resource_policy_methods_use_iam_policy_routes():
    grant_payload = {
        "resource": {
            "type": "agent",
            "id": "agent-1",
            "name": "planner",
        },
        "subject": {"type": "user", "id": "user-1"},
        "direct_roles": ["operator", "list"],
    }
    session = _FakeSession(
        [
            _FakeResponse(status=200, payload={}),
            _FakeResponse(
                status=200,
                payload={
                    "resource": {"type": "agent", "id": "agent-1", "name": "planner"},
                    "access_grants": [grant_payload],
                    "continuation_token": "next-token",
                },
            ),
            _FakeResponse(
                status=200,
                payload={
                    "resource": {"type": "agent", "id": "agent-1", "name": "planner"},
                    "access_grants": [grant_payload],
                },
            ),
            _FakeResponse(status=200, payload={}),
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    await client.grant_resource_policy(
        project_id="proj_123",
        resource_type="agent",
        resource_id="agent-1",
        subject=AccessSubject(type="user", id="user-1"),
        roles=["operator", "list"],
    )
    page = await client.get_resource_policy_page(
        project_id="proj_123",
        resource_type="agent",
        resource_id="agent-1",
        continuation_token="cursor-1",
    )
    grants = await client.get_resource_policy(
        project_id="proj_123",
        resource_type="agent",
        resource_id="agent-1",
    )
    await client.revoke_resource_policy(
        project_id="proj_123",
        resource_type="agent",
        resource_id="agent-1",
        subject=AccessSubject(type="user", id="user-1"),
    )

    assert len(page.access_grants) == 1
    assert page.continuation_token == "next-token"
    assert page.access_grants[0].direct_roles == ["operator", "list"]
    assert grants[0].subject.id == "user-1"
    assert session.calls == [
        (
            "post",
            "http://example.test/accounts/projects/proj_123/iam/agent/agent-1/policy:grant",
            {
                "subject": {
                    "type": "user",
                    "id": "user-1",
                },
                "roles": ["operator", "list"],
            },
        ),
        (
            "get",
            "http://example.test/accounts/projects/proj_123/iam/agent/agent-1/policy",
            {"page_size": "50", "continuation_token": "cursor-1"},
        ),
        (
            "get",
            "http://example.test/accounts/projects/proj_123/iam/agent/agent-1/policy",
            {"page_size": "50"},
        ),
        (
            "post",
            "http://example.test/accounts/projects/proj_123/iam/agent/agent-1/policy:revoke",
            {
                "subject": {
                    "type": "user",
                    "id": "user-1",
                },
            },
        ),
    ]


@pytest.mark.asyncio
async def test_access_evaluator_methods_use_access_routes():
    session = _FakeSession(
        [
            _FakeResponse(status=200, payload={"allowed": True, "relation": "can_use"}),
            _FakeResponse(status=200, payload={"allowed": True, "relation": "can_use"}),
            _FakeResponse(
                status=200,
                payload={
                    "resource": {"type": "room", "id": "room-1", "name": "demo"},
                    "subject": {"type": "user", "id": "user-1"},
                    "effective_roles": ["developer"],
                    "capabilities": {"can_use": True, "can_manage": False},
                },
            ),
            _FakeResponse(
                status=200,
                payload={
                    "access_grants": [
                        {
                            "resource": {
                                "type": "room",
                                "id": "room-1",
                                "name": "demo",
                            },
                            "subject": {"type": "user", "id": "user-1"},
                            "direct_roles": ["developer"],
                        }
                    ]
                },
            ),
            _FakeResponse(
                status=200,
                payload={
                    "access_grants": [
                        {
                            "resource": {
                                "type": "agent",
                                "id": "agent-1",
                                "name": "planner",
                            },
                            "subject": {"type": "user", "id": "user-1"},
                            "direct_roles": ["admin"],
                        }
                    ]
                },
            ),
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    test_result = await client.test_access(
        project_id="proj_123",
        subject=AccessSubject(type="user", id="user-1"),
        resource=AccessResource(type="room", id="room-1"),
        relation="can_use",
    )
    userset_test_result = await client.test_access(
        project_id="proj_123",
        subject=AccessSubject(
            type="userset",
            id="proj_123",
            object_type="project",
            relation="member",
        ),
        resource=AccessResource(type="room", id="room-1"),
        relation="can_use",
    )
    effective = await client.get_effective_access(
        project_id="proj_123",
        subject=AccessSubject(type="user", id="user-1"),
        resource=AccessResource(type="room", id="room-1"),
        relations=["can_use", "can_manage"],
    )
    bindings_page = await client.list_access_bindings_page(
        project_id="proj_123",
        subject=AccessSubject(type="user", id="user-1"),
    )
    bindings = await client.list_access_bindings(
        project_id="proj_123",
        subject=AccessSubject(type="user", id="user-1"),
    )

    assert test_result.allowed is True
    assert userset_test_result.allowed is True
    assert effective.effective_roles == ["developer"]
    assert effective.capabilities == {"can_use": True, "can_manage": False}
    assert bindings_page.access_grants[0].resource.name == "demo"
    assert bindings_page.access_grants[0].direct_roles == ["developer"]
    assert bindings[0].resource.type == "agent"
    assert bindings[0].direct_roles == ["admin"]
    assert session.calls == [
        (
            "post",
            "http://example.test/accounts/projects/proj_123/access:test",
            {
                "subject": {"type": "user", "id": "user-1"},
                "resource": {"type": "room", "id": "room-1"},
                "relation": "can_use",
            },
        ),
        (
            "post",
            "http://example.test/accounts/projects/proj_123/access:test",
            {
                "subject": {
                    "type": "userset",
                    "id": "proj_123",
                    "object_type": "project",
                    "relation": "member",
                },
                "resource": {"type": "room", "id": "room-1"},
                "relation": "can_use",
            },
        ),
        (
            "post",
            "http://example.test/accounts/projects/proj_123/access:effective",
            {
                "subject": {"type": "user", "id": "user-1"},
                "resource": {"type": "room", "id": "room-1"},
                "relations": ["can_use", "can_manage"],
            },
        ),
        (
            "post",
            "http://example.test/accounts/projects/proj_123/access:bindings",
            {"subject": {"type": "user", "id": "user-1"}},
        ),
        (
            "post",
            "http://example.test/accounts/projects/proj_123/access:bindings",
            {"subject": {"type": "user", "id": "user-1"}},
        ),
    ]
