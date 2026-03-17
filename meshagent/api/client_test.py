import json

import pytest

from meshagent.api.client import Meshagent


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

    def post(self, url: str, *, headers=None, json=None):
        self.calls.append(("post", url, json))
        return self._responses.pop(0)

    def put(self, url: str, *, headers=None, json=None):
        self.calls.append(("put", url, json))
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
