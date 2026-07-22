import base64
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
from meshagent.api.client import (
    AccessResource,
    AccessSubject,
    Meshagent,
    room_scope_for_role_compat,
)
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

    def patch(self, url: str, *, headers=None, json=None):
        self.calls.append(("patch", url, json))
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
async def test_connect_agent_normalizes_legacy_messages_url():
    session = _FakeSession(
        [
            _FakeResponse(
                status=200,
                payload={
                    "jwt": "agent-token",
                    "agent_name": "planner",
                    "project_id": "proj_123",
                    "agent_url": (
                        "wss://api.example.test/base/accounts/projects/proj_123/"
                        "agents/planner/messages?thread=one"
                    ),
                },
            )
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    connection = await client.connect_agent(project_id="proj_123", agent="planner")

    assert connection.agent_url == (
        "wss://api.example.test/base/agents/proj_123/planner/messages?thread=one"
    )
    assert session.calls == [
        (
            "post",
            "http://example.test/accounts/projects/proj_123/agents/planner/connect",
            {},
        )
    ]


@pytest.mark.parametrize("role", ["operator", "developer", "admin"])
def test_room_scope_for_role_compat_includes_sqlite(role):
    scope = room_scope_for_role_compat(role)

    assert scope.sqlite is not None


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
async def test_whoami_reads_typed_service_account_response():
    session = _FakeSession(
        [
            _FakeResponse(
                status=200,
                payload={
                    "id": "service-account-1",
                    "email": "worker@service.project.api.example.com",
                    "type": "service_account",
                    "service_account": {
                        "id": "service-account-1",
                        "project_id": "project-1",
                        "key": "service-account-key",
                        "name": "worker",
                        "email": "worker@service.project.api.example.com",
                        "display_name": "Worker",
                        "description": "",
                        "metadata": {},
                        "annotations": {},
                    },
                    "user": None,
                },
            )
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    identity = await client.whoami()

    assert identity.type == "service_account"
    assert identity.id == "service-account-1"
    assert identity.email == "worker@service.project.api.example.com"
    assert identity.service_account is not None
    assert identity.service_account.email == identity.email
    assert identity.user is None
    assert session.calls == [
        ("get", "http://example.test/accounts/whoami", None),
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


def _secret_payload(**overrides):
    payload = {
        "id": "secret-1",
        "project_id": "proj_123",
        "owner_user_id": "user-1",
        "owner_service_account_id": None,
        "created_by_user_id": "user-1",
        "created_by_service_account_id": None,
        "name": "registry",
        "type": "opaque",
        "http_only": False,
        "metadata": {"service": "github"},
        "annotations": {"meshagent.io/secret.account": "alice"},
        "current_version_id": None,
        "created_at": "2026-06-10T00:00:00+00:00",
        "updated_at": "2026-06-10T00:00:00+00:00",
    }
    payload.update(overrides)
    return payload


def _secret_version_payload(**overrides):
    payload = {
        "id": "version-1",
        "secret_id": "secret-1",
        "version": 1,
        "encryption_key_id": "supabase-vault",
        "value_sha256": None,
        "created_by_user_id": "user-1",
        "created_by_service_account_id": None,
        "created_at": "2026-06-10T00:00:00+00:00",
    }
    payload.update(overrides)
    return payload


@pytest.mark.asyncio
async def test_user_secret_methods_use_new_user_scoped_routes():
    session = _FakeSession(
        [
            _FakeResponse(status=200, payload=_secret_payload(http_only=True)),
            _FakeResponse(
                status=200,
                payload={
                    "secrets": [_secret_payload()],
                    "continuation_token": "next-page",
                },
            ),
            _FakeResponse(
                status=200,
                payload={
                    "secrets": [_secret_payload(name="github-token")],
                    "continuation_token": "next-search-page",
                },
            ),
            _FakeResponse(
                status=200,
                payload=_secret_payload(
                    value_base64=base64.b64encode(b"value").decode()
                ),
            ),
            _FakeResponse(status=200, payload=_secret_payload(name="renamed")),
            _FakeResponse(
                status=200,
                payload={"versions": [_secret_version_payload()]},
            ),
            _FakeResponse(
                status=200,
                payload=_secret_version_payload(
                    id="version-2",
                    value_sha256=base64.b64encode(b"hash").decode(),
                ),
            ),
            _FakeResponse(
                status=200,
                payload={
                    "secret_id": "secret-1",
                    "version_id": "version-1",
                    "value_base64": base64.b64encode(b"version value").decode(),
                },
            ),
            _FakeResponse(status=200, payload={}),
            _FakeResponse(
                status=200,
                payload={
                    "access_grants": [
                        {
                            "subject": {
                                "type": "service_account",
                                "id": "service-account-1",
                                "name": "GitHub proxy",
                            },
                            "roles": ["use_proxy"],
                        }
                    ],
                    "continuation_token": "next-grants-page",
                },
            ),
            _FakeResponse(status=200, payload={}),
            _FakeResponse(status=200, payload={}),
            _FakeResponse(status=200, payload={}),
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    created = await client.create_user_secret(
        project_id="proj_123",
        name="registry",
        type="oauth",
        http_only=True,
        metadata={"service": "github"},
        annotations={"meshagent.io/secret.account": "alice"},
    )
    page = await client.list_user_secrets(
        page_size=25,
        continuation_token="cursor",
        filter="github",
    )
    search_page = await client.search_user_secrets(
        name="token",
        type="oauth",
        http_only=True,
        metadata={"service": "github"},
        annotations={"meshagent.io/secret.account": "alice"},
        provider="github",
        email="alice@example.com",
        oauth_provider="github-oauth",
        page_size=10,
        continuation_token="search-cursor",
    )
    fetched = await client.get_user_secret(secret_id="secret-1", include_value=True)
    updated = await client.update_user_secret(secret_id="secret-1", name="renamed")
    versions = await client.list_user_secret_versions(secret_id="secret-1")
    created_version = await client.create_user_secret_version(
        secret_id="secret-1",
        value=b"secret value",
        set_current=False,
    )
    version_value = await client.access_user_secret_version(
        secret_id="secret-1",
        version_id="version-1",
    )
    await client.delete_user_secret_version(
        secret_id="secret-1",
        version_id="version-1",
    )
    proxy_access = await client.list_user_secret_proxy_access(
        secret_id="secret-1",
        page_size=10,
        continuation_token="grants-cursor",
    )
    await client.grant_user_secret_proxy_access(
        secret_id="secret-1",
        service_account_id="service-account-1",
    )
    await client.revoke_user_secret_proxy_access(
        secret_id="secret-1",
        service_account_id="service-account-1",
    )
    await client.delete_user_secret(secret_id="secret-1")

    assert created.http_only is True
    assert page.continuation_token == "next-page"
    assert page.secrets[0].metadata == {"service": "github"}
    assert search_page.continuation_token == "next-search-page"
    assert fetched.name == "registry"
    assert fetched.value_base64 == base64.b64encode(b"value").decode()
    assert updated.name == "renamed"
    assert versions[0].version == 1
    assert not hasattr(versions[0], "encryption_key_id")
    assert created_version.value_sha256 == base64.b64encode(b"hash").decode()
    assert not hasattr(created_version, "encryption_key_id")
    assert version_value == b"version value"
    assert proxy_access.continuation_token == "next-grants-page"
    assert proxy_access.access_grants[0].subject.type == "service_account"
    assert proxy_access.access_grants[0].roles == ["use_proxy"]
    assert session.calls == [
        (
            "post",
            "http://example.test/accounts/users/me/secrets",
            {
                "project_id": "proj_123",
                "name": "registry",
                "type": "oauth",
                "http_only": True,
                "metadata": {"service": "github"},
                "annotations": {"meshagent.io/secret.account": "alice"},
            },
        ),
        (
            "get",
            "http://example.test/accounts/users/me/secrets",
            {
                "page_size": 25,
                "continuation_token": "cursor",
                "filter": "github",
            },
        ),
        (
            "post",
            "http://example.test/accounts/users/me/secrets:search",
            {
                "page_size": 10,
                "name": "token",
                "type": "oauth",
                "http_only": True,
                "metadata": {"service": "github"},
                "annotations": {"meshagent.io/secret.account": "alice"},
                "provider": "github",
                "email": "alice@example.com",
                "oauth_provider": "github-oauth",
                "continuation_token": "search-cursor",
            },
        ),
        (
            "get",
            "http://example.test/accounts/users/me/secrets/secret-1",
            {"include_value": "true"},
        ),
        (
            "patch",
            "http://example.test/accounts/users/me/secrets/secret-1",
            {"name": "renamed"},
        ),
        (
            "get",
            "http://example.test/accounts/users/me/secrets/secret-1/versions",
            None,
        ),
        (
            "post",
            "http://example.test/accounts/users/me/secrets/secret-1/versions",
            {
                "value_base64": base64.b64encode(b"secret value").decode(),
                "set_current": False,
            },
        ),
        (
            "get",
            "http://example.test/accounts/users/me/secrets/secret-1/versions/version-1:access",
            None,
        ),
        (
            "delete",
            "http://example.test/accounts/users/me/secrets/secret-1/versions/version-1",
            None,
        ),
        (
            "get",
            "http://example.test/accounts/users/me/secrets/secret-1/access",
            {"page_size": 10, "continuation_token": "grants-cursor"},
        ),
        (
            "post",
            "http://example.test/accounts/users/me/secrets/secret-1/access:grant-proxy",
            {
                "subject": {
                    "type": "service_account",
                    "id": "service-account-1",
                }
            },
        ),
        (
            "post",
            "http://example.test/accounts/users/me/secrets/secret-1/access:revoke-proxy",
            {
                "subject": {
                    "type": "service_account",
                    "id": "service-account-1",
                }
            },
        ),
        ("delete", "http://example.test/accounts/users/me/secrets/secret-1", None),
    ]


def test_external_oauth_registration_methods_are_removed_from_client():
    removed = {
        "create_project_external_oauth_registration",
        "update_project_external_oauth_registration",
        "list_project_external_oauth_registrations",
        "delete_project_external_oauth_registration",
        "create_room_external_oauth_registration",
        "update_room_external_oauth_registration",
        "list_room_external_oauth_registrations",
        "delete_room_external_oauth_registration",
    }

    assert removed.isdisjoint(Meshagent.__dict__)


@pytest.mark.asyncio
async def test_service_account_secret_methods_use_service_account_scoped_routes():
    session = _FakeSession(
        [
            _FakeResponse(
                status=200,
                payload=_secret_payload(
                    owner_user_id=None,
                    owner_service_account_id="sa-1",
                    created_by_user_id=None,
                    created_by_service_account_id="sa-1",
                ),
            ),
            _FakeResponse(
                status=200,
                payload={
                    "secrets": [
                        _secret_payload(
                            owner_user_id=None,
                            owner_service_account_id="sa-1",
                            created_by_user_id=None,
                            created_by_service_account_id="sa-1",
                        )
                    ]
                },
            ),
            _FakeResponse(
                status=200,
                payload={
                    "secrets": [
                        _secret_payload(
                            owner_user_id=None,
                            owner_service_account_id="sa-1",
                            created_by_user_id=None,
                            created_by_service_account_id="sa-1",
                            name="github-token",
                        )
                    ]
                },
            ),
            _FakeResponse(
                status=200,
                payload=_secret_payload(
                    owner_user_id=None,
                    owner_service_account_id="sa-1",
                    created_by_user_id=None,
                    created_by_service_account_id="sa-1",
                    value_base64=base64.b64encode(b"service value").decode(),
                ),
            ),
            _FakeResponse(
                status=200,
                payload=_secret_payload(
                    owner_user_id=None,
                    owner_service_account_id="sa-1",
                    created_by_user_id=None,
                    created_by_service_account_id="sa-1",
                    http_only=True,
                ),
            ),
            _FakeResponse(
                status=200,
                payload={
                    "versions": [
                        _secret_version_payload(
                            created_by_user_id=None,
                            created_by_service_account_id="sa-1",
                        )
                    ]
                },
            ),
            _FakeResponse(
                status=200,
                payload=_secret_version_payload(
                    id="version-2",
                    created_by_user_id=None,
                    created_by_service_account_id="sa-1",
                ),
            ),
            _FakeResponse(
                status=200,
                payload={
                    "secret_id": "secret-1",
                    "version_id": "version-1",
                    "value_base64": base64.b64encode(b"service version value").decode(),
                },
            ),
            _FakeResponse(status=200, payload={}),
            _FakeResponse(
                status=200,
                payload={
                    "secrets": [
                        _secret_payload(
                            owner_user_id=None,
                            owner_service_account_id="sa-1",
                            created_by_user_id=None,
                            created_by_service_account_id="sa-1",
                        )
                    ]
                },
            ),
            _FakeResponse(status=200, payload={}),
            _FakeResponse(status=200, payload={}),
            _FakeResponse(status=200, payload={}),
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    created = await client.create_service_account_secret(
        project_id="proj_123",
        service_account_id="sa-1",
        name="registry",
    )
    listed = await client.list_service_account_secrets(
        project_id="proj_123",
        service_account_id="sa-1",
    )
    search_page = await client.search_service_account_secrets(
        project_id="proj_123",
        service_account_id="sa-1",
        name="token",
        metadata={"service": "github"},
        service="github",
        url="https://github.com",
        page_size=25,
    )
    fetched = await client.get_service_account_secret(
        project_id="proj_123",
        service_account_id="sa-1",
        secret_id="secret-1",
        include_value=True,
    )
    updated = await client.update_service_account_secret(
        project_id="proj_123",
        service_account_id="sa-1",
        secret_id="secret-1",
        http_only=True,
    )
    versions = await client.list_service_account_secret_versions(
        project_id="proj_123",
        service_account_id="sa-1",
        secret_id="secret-1",
    )
    created_version = await client.create_service_account_secret_version(
        project_id="proj_123",
        service_account_id="sa-1",
        secret_id="secret-1",
        value=b"secret value",
    )
    version_value = await client.access_service_account_secret_version(
        project_id="proj_123",
        service_account_id="sa-1",
        secret_id="secret-1",
        version_id="version-1",
    )
    await client.delete_service_account_secret_version(
        project_id="proj_123",
        service_account_id="sa-1",
        secret_id="secret-1",
        version_id="version-1",
    )
    pull_secrets = await client.list_service_account_pull_secrets(
        project_id="proj_123",
        service_account_id="sa-1",
    )
    await client.add_service_account_pull_secret(
        project_id="proj_123",
        service_account_id="sa-1",
        secret_id="secret-1",
    )
    await client.remove_service_account_pull_secret(
        project_id="proj_123",
        service_account_id="sa-1",
        secret_id="secret-1",
    )
    await client.delete_service_account_secret(
        project_id="proj_123",
        service_account_id="sa-1",
        secret_id="secret-1",
    )

    assert created.owner_service_account_id == "sa-1"
    assert listed.secrets[0].owner_service_account_id == "sa-1"
    assert search_page.secrets[0].name == "github-token"
    assert fetched.id == "secret-1"
    assert fetched.value_base64 == base64.b64encode(b"service value").decode()
    assert updated.http_only is True
    assert versions[0].created_by_service_account_id == "sa-1"
    assert not hasattr(versions[0], "encryption_key_id")
    assert created_version.id == "version-2"
    assert not hasattr(created_version, "encryption_key_id")
    assert version_value == b"service version value"
    assert pull_secrets[0].owner_service_account_id == "sa-1"
    assert session.calls == [
        (
            "post",
            "http://example.test/accounts/projects/proj_123/service-accounts/sa-1/secrets",
            {"name": "registry", "type": "opaque", "http_only": False},
        ),
        (
            "get",
            "http://example.test/accounts/projects/proj_123/service-accounts/sa-1/secrets",
            {"page_size": 100},
        ),
        (
            "post",
            "http://example.test/accounts/projects/proj_123/service-accounts/sa-1/secrets:search",
            {
                "page_size": 25,
                "name": "token",
                "metadata": {"service": "github"},
                "service": "github",
                "url": "https://github.com",
            },
        ),
        (
            "get",
            "http://example.test/accounts/projects/proj_123/service-accounts/sa-1/secrets/secret-1",
            {"include_value": "true"},
        ),
        (
            "patch",
            "http://example.test/accounts/projects/proj_123/service-accounts/sa-1/secrets/secret-1",
            {"http_only": True},
        ),
        (
            "get",
            "http://example.test/accounts/projects/proj_123/service-accounts/sa-1/secrets/secret-1/versions",
            None,
        ),
        (
            "post",
            "http://example.test/accounts/projects/proj_123/service-accounts/sa-1/secrets/secret-1/versions",
            {
                "value_base64": base64.b64encode(b"secret value").decode(),
                "set_current": True,
            },
        ),
        (
            "get",
            "http://example.test/accounts/projects/proj_123/service-accounts/sa-1/secrets/secret-1/versions/version-1:access",
            None,
        ),
        (
            "delete",
            "http://example.test/accounts/projects/proj_123/service-accounts/sa-1/secrets/secret-1/versions/version-1",
            None,
        ),
        (
            "get",
            "http://example.test/accounts/projects/proj_123/service-accounts/sa-1/pull-secrets",
            None,
        ),
        (
            "put",
            "http://example.test/accounts/projects/proj_123/service-accounts/sa-1/pull-secrets/secret-1",
            None,
        ),
        (
            "delete",
            "http://example.test/accounts/projects/proj_123/service-accounts/sa-1/pull-secrets/secret-1",
            None,
        ),
        (
            "delete",
            "http://example.test/accounts/projects/proj_123/service-accounts/sa-1/secrets/secret-1",
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
async def test_create_room_serializes_permissions_payload():
    room_payload = {
        "id": "room-1",
        "name": "room-a",
        "metadata": {},
        "annotations": {},
        "permissions": {},
    }
    session = _FakeSession([_FakeResponse(status=200, payload=room_payload)])
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    room = await client.create_room(
        project_id="proj_123",
        name="room-a",
        if_not_exists=True,
        metadata={"purpose": "test"},
        annotations={"meshagent.storage.class": "ephemeral"},
        permissions={"user-1": ApiScope.full()},
    )

    assert room.name == "room-a"
    assert session.calls == [
        (
            "post",
            "http://example.test/accounts/projects/proj_123/rooms",
            {
                "name": "room-a",
                "if_not_exists": True,
                "metadata": {"purpose": "test"},
                "annotations": {"meshagent.storage.class": "ephemeral"},
                "permissions": {"user-1": ApiScope.full().model_dump(mode="json")},
            },
        )
    ]


@pytest.mark.asyncio
async def test_agent_crud_methods_use_agent_routes():
    configuration = ManagedAgentSpec(
        id="agent-1",
        metadata=ManagedAgentMetadata(name="planner"),
        allowed_models=[AllowedOpenAIModel(model="gpt-4.1")],
        run_as={
            "email": "Agent@Service.Project.Example.test",
            "scopes": ["secrets:proxy", "llm_proxy", "llm_proxy", ""],
        },
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
    assert configuration.run_as is not None
    assert configuration.run_as.email == "agent@service.project.example.test"
    assert configuration.run_as.scopes == ["secrets:proxy", "llm_proxy"]
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
            "type": "room",
            "id": "room-1",
            "name": "demo",
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
                    "resource": {"type": "room", "id": "room-1", "name": "demo"},
                    "access_grants": [grant_payload],
                    "continuation_token": "next-token",
                },
            ),
            _FakeResponse(
                status=200,
                payload={
                    "resource": {"type": "room", "id": "room-1", "name": "demo"},
                    "access_grants": [grant_payload],
                },
            ),
            _FakeResponse(status=200, payload={}),
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    await client.grant_resource_policy(
        project_id="proj_123",
        resource_type="room",
        resource_id="room-1",
        subject=AccessSubject(type="user", id="user-1"),
        roles=["operator", "list"],
    )
    page = await client.get_resource_policy_page(
        project_id="proj_123",
        resource_type="room",
        resource_id="room-1",
        continuation_token="cursor-1",
    )
    grants = await client.get_resource_policy(
        project_id="proj_123",
        resource_type="room",
        resource_id="room-1",
    )
    await client.revoke_resource_policy(
        project_id="proj_123",
        resource_type="room",
        resource_id="room-1",
        subject=AccessSubject(type="user", id="user-1"),
    )

    assert len(page.access_grants) == 1
    assert page.continuation_token == "next-token"
    assert page.access_grants[0].direct_roles == ["operator", "list"]
    assert grants[0].subject.id == "user-1"
    assert session.calls == [
        (
            "post",
            "http://example.test/accounts/projects/proj_123/iam/room/room-1/policy:grant",
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
            "http://example.test/accounts/projects/proj_123/iam/room/room-1/policy",
            {"page_size": "50", "continuation_token": "cursor-1"},
        ),
        (
            "get",
            "http://example.test/accounts/projects/proj_123/iam/room/room-1/policy",
            {"page_size": "50"},
        ),
        (
            "post",
            "http://example.test/accounts/projects/proj_123/iam/room/room-1/policy:revoke",
            {
                "subject": {
                    "type": "user",
                    "id": "user-1",
                },
            },
        ),
    ]


@pytest.mark.asyncio
async def test_resource_policy_methods_reject_managed_agent_policies():
    session = _FakeSession([])
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    with pytest.raises(ValueError, match="managed agent resource policies"):
        await client.grant_resource_policy(
            project_id="proj_123",
            resource_type="agent",
            resource_id="agent-1",
            subject=AccessSubject(type="user", id="user-1"),
            roles=["operator", "list"],
        )

    with pytest.raises(ValueError, match="managed agent resource policies"):
        await client.get_resource_policy_page(
            project_id="proj_123",
            resource_type="agent",
            resource_id="agent-1",
        )

    with pytest.raises(ValueError, match="managed agent resource policies"):
        await client.revoke_resource_policy(
            project_id="proj_123",
            resource_type="agent",
            resource_id="agent-1",
            subject=AccessSubject(type="user", id="user-1"),
        )

    assert session.calls == []


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
