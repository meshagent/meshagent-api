import json

import pytest

from meshagent.api import ParticipantGrant, ParticipantToken
from meshagent.api.participant_token import ApiScope
from meshagent.api.client import Meshagent
from meshagent.api.specs.service import ContainerSpec, ServiceMetadata, ServiceSpec


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
async def test_create_room_service_omits_generated_service_id():
    session = _FakeSession([_FakeResponse(status=200, payload={"id": "svc_123"})])
    client = Meshagent(base_url="http://example.test", token="token", session=session)
    service = ServiceSpec(
        version="v1",
        kind="Service",
        metadata=ServiceMetadata(name="doctor-go-no-sdk"),
        container=ContainerSpec(image="repo/doctor-go-no-sdk:1"),
    )

    service_id = await client.create_room_service(
        project_id="proj_123",
        room_name="room-123",
        service=service,
    )

    assert service_id == "svc_123"
    assert session.calls == [
        (
            "post",
            "http://example.test/accounts/projects/proj_123/rooms/room-123/services",
            {
                "version": "v1",
                "kind": "Service",
                "metadata": {"name": "doctor-go-no-sdk"},
                "ports": [],
                "container": {
                    "image": "repo/doctor-go-no-sdk:1",
                    "private": True,
                },
            },
        )
    ]


@pytest.mark.asyncio
async def test_update_scheduled_task_allows_partial_update_without_annotations():
    session = _FakeSession([_FakeResponse(status=200, payload={})])
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    await client.update_scheduled_task(
        project_id="proj_123",
        task_id="task_123",
        schedule="0 * * * *",
    )

    assert session.calls == [
        (
            "put",
            "http://example.test/accounts/projects/proj_123/scheduled-tasks/task_123",
            {
                "schedule": "0 * * * *",
            },
        )
    ]


@pytest.mark.asyncio
async def test_update_scheduled_task_can_clear_storage_write_path():
    session = _FakeSession([_FakeResponse(status=200, payload={})])
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    await client.update_scheduled_task(
        project_id="proj_123",
        task_id="task_123",
        clear_storage_write_path=True,
    )

    assert session.calls == [
        (
            "put",
            "http://example.test/accounts/projects/proj_123/scheduled-tasks/task_123",
            {
                "storage_write_path": "",
            },
        )
    ]


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
                    }
                },
            )
        ]
    )
    client = Meshagent(base_url="http://example.test", token="token", session=session)

    config = await client.get_config()

    assert config.domains.registry == "registry.meshagent.life"
    assert config.domains.api == "api.meshagent.life"
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
