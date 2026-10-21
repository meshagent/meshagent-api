import pytest
from pydantic import ValidationError

from meshagent.api.managed_agents import ManagedAgentSpec


def test_managed_agent_connection_supports_literal_and_secret_headers():
    from meshagent.api.managed_agents import (
        ManagedAgentHttpConnection,
        ManagedAgentConnectionHeader,
        ManagedAgentSecretHeader,
    )

    connection = ManagedAgentHttpConnection.model_validate(
        {
            "base_url": "https://api.example.com",
            "headers": [
                {
                    "name": "Authorization",
                    "secret": {"id": "basic"},
                    "prefix": "Basic ",
                },
                {"name": "X-API-Key", "secret": {"id": "api-key"}},
                {"name": "Accept", "value": "application/json"},
            ],
        }
    )
    assert connection.headers[0] == ManagedAgentConnectionHeader(
        name="Authorization",
        secret=ManagedAgentSecretHeader(id="basic"),
        prefix="Basic ",
    )
    assert connection.headers[1] == ManagedAgentConnectionHeader(
        name="X-API-Key", secret=ManagedAgentSecretHeader(id="api-key")
    )
    assert connection.headers[2].value == "application/json"


@pytest.mark.parametrize(
    "headers",
    [
        [{"name": "Host", "secret": {"id": "id"}}],
        [{"name": "X-API-Key", "secret": {"id": ""}}],
        [{"name": "X-API-Key", "secret": {"id": "id"}, "prefix": "bad\n"}],
        [
            {"name": "Authorization", "value": "Bearer value"},
            {"name": "X-API-Key", "secret": {"id": "id"}},
        ],
    ],
)
def test_managed_agent_connection_rejects_unsafe_headers(headers):
    from meshagent.api.managed_agents import ManagedAgentHttpConnection

    with pytest.raises(ValidationError):
        ManagedAgentHttpConnection(base_url="https://api.example.com", headers=headers)


def test_managed_agent_environment_requires_one_source_and_secret_scope():
    base = {
        "metadata": {"name": "planner"},
        "allowed_models": [],
        "environment": [
            {"name": "CRM_URL", "value": "https://api.example.com"},
            {"name": "CRM_KEY", "secret": {"id": "secret-1"}},
        ],
    }
    with pytest.raises(ValidationError, match="secrets:read"):
        ManagedAgentSpec.model_validate(base)
    base["run_as"] = {
        "email": "agent@service.example.meshagent.dev",
        "scopes": ["secrets:read", "llm_proxy"],
    }
    assert len(ManagedAgentSpec.model_validate(base).environment) == 2
    base["environment"].append({"name": "CRM_URL", "value": "duplicate"})
    with pytest.raises(ValidationError, match="duplicate environment"):
        ManagedAgentSpec.model_validate(base)
    base["environment"] = [{"name": "BAD", "value": "x", "secret": {"id": "id"}}]
    with pytest.raises(ValidationError, match="exactly one"):
        ManagedAgentSpec.model_validate(base)


def test_managed_agent_spec_accepts_missing_id_with_metadata_name():
    spec = ManagedAgentSpec.model_validate(
        {
            "version": "v1",
            "kind": "ManagedAgent",
            "metadata": {"name": "planner"},
            "allowed_models": [{"provider": "openai", "model": "gpt-4.1"}],
        }
    )

    assert spec.id is None
    assert spec.name == "planner"
    assert spec.metadata.name == "planner"
    assert spec.thread_isolation == "global"
    assert spec.store is True


def test_managed_agent_spec_accepts_disabled_storage():
    spec = ManagedAgentSpec.model_validate(
        {
            "version": "v1",
            "kind": "ManagedAgent",
            "metadata": {"name": "planner"},
            "allowed_models": [{"provider": "openai", "model": "gpt-4.1"}],
            "store": False,
        }
    )

    assert spec.store is False


def test_managed_agent_spec_accepts_participant_thread_isolation():
    spec = ManagedAgentSpec.model_validate(
        {
            "version": "v1",
            "kind": "ManagedAgent",
            "metadata": {"name": "planner"},
            "allowed_models": [{"provider": "openai", "model": "gpt-4.1"}],
            "thread_isolation": "participant",
        }
    )

    assert spec.thread_isolation == "participant"


def test_managed_agent_spec_rejects_invalid_thread_isolation():
    with pytest.raises(ValidationError):
        ManagedAgentSpec.model_validate(
            {
                "version": "v1",
                "kind": "ManagedAgent",
                "metadata": {"name": "planner"},
                "allowed_models": [{"provider": "openai", "model": "gpt-4.1"}],
                "thread_isolation": "private",
            }
        )


def test_managed_agent_spec_rejects_storage_toolkit():
    with pytest.raises(ValidationError):
        ManagedAgentSpec.model_validate(
            {
                "version": "v1",
                "kind": "ManagedAgent",
                "metadata": {"name": "planner"},
                "allowed_models": [{"provider": "openai", "model": "gpt-4.1"}],
                "toolkits": [
                    {
                        "type": "storage",
                        "mounts": [{"type": "agent", "path": "/agent2"}],
                    }
                ],
            }
        )


def test_managed_agent_spec_accepts_image_generation_toolkit():
    spec = ManagedAgentSpec.model_validate(
        {
            "version": "v1",
            "kind": "ManagedAgent",
            "metadata": {"name": "planner"},
            "allowed_models": [{"provider": "openai", "model": "gpt-5.5"}],
            "toolkits": [
                {
                    "type": "image_generation",
                    "model": "gpt-image-2",
                    "size": "1024x1024",
                    "quality": "high",
                }
            ],
        }
    )

    assert spec.toolkits is not None
    toolkit = spec.toolkits[0]
    assert toolkit.type == "image_generation"
    assert toolkit.model == "gpt-image-2"
    assert toolkit.size == "1024x1024"
    assert toolkit.quality == "high"


def test_managed_agent_spec_rejects_shell_toolkit():
    with pytest.raises(ValidationError):
        ManagedAgentSpec.model_validate(
            {
                "version": "v1",
                "kind": "ManagedAgent",
                "metadata": {"name": "planner"},
                "allowed_models": [{"provider": "openai", "model": "gpt-4.1"}],
                "toolkits": [
                    {
                        "type": "shell",
                        "room_name": "workspace",
                    }
                ],
            }
        )


def test_managed_agent_spec_accepts_mcp_proxy_secret():
    spec = ManagedAgentSpec.model_validate(
        {
            "version": "v1",
            "kind": "ManagedAgent",
            "metadata": {"name": "planner"},
            "allowed_models": [{"provider": "openai", "model": "gpt-4.1"}],
            "toolkits": [
                {
                    "type": "mcp",
                    "servers": [
                        {
                            "server_label": "github",
                            "server_url": "https://mcp.example.test",
                            "use_proxy_secret": "github-token",
                        }
                    ],
                }
            ],
        }
    )

    assert spec.toolkits is not None
    toolkit = spec.toolkits[0]
    assert toolkit.type == "mcp"
    assert toolkit.servers[0].use_proxy_secret == "github-token"


def test_managed_agent_spec_drops_legacy_mcp_secret_authorization():
    spec = ManagedAgentSpec.model_validate(
        {
            "version": "v1",
            "kind": "ManagedAgent",
            "metadata": {"name": "planner"},
            "allowed_models": [{"provider": "openai", "model": "gpt-4.1"}],
            "toolkits": [
                {
                    "type": "mcp",
                    "servers": [
                        {
                            "server_label": "linear",
                            "server_url": "https://mcp.example.test",
                            "authorization": {
                                "type": "header",
                                "name": "X-API-Key",
                                "secret": {
                                    "type": "user",
                                    "secret_name": "linear-token",
                                },
                            },
                        }
                    ],
                }
            ],
        }
    )

    assert spec.toolkits is not None
    toolkit = spec.toolkits[0]
    assert toolkit.type == "mcp"
    server_data = toolkit.servers[0].model_dump(mode="json")
    assert server_data["server_label"] == "linear"
    assert "authorization" not in server_data


@pytest.mark.parametrize(
    "filters",
    [
        {},
        {"allowed_domains": []},
        {"blocked_domains": []},
        {"allowed_domains": ["example.com"]},
        {"blocked_domains": ["blocked.example.com"]},
        {
            "allowed_domains": ["example.com"],
            "blocked_domains": ["blocked.example.com"],
        },
    ],
)
def test_managed_web_search_domain_filters_round_trip(filters):
    toolkit = {"type": "web_search", **filters}
    spec = ManagedAgentSpec.model_validate(
        {
            "metadata": {"name": "researcher"},
            "allowed_models": [{"provider": "openai", "model": "gpt-4.1"}],
            "toolkits": [toolkit],
        }
    )
    assert spec.model_dump(exclude_none=True)["toolkits"] == [toolkit]


@pytest.mark.parametrize("field", ["allowed_domains", "blocked_domains"])
def test_managed_web_search_domain_filters_require_lists(field):
    with pytest.raises(ValidationError):
        ManagedAgentSpec.model_validate(
            {
                "metadata": {"name": "researcher"},
                "allowed_models": [{"provider": "openai", "model": "gpt-4.1"}],
                "toolkits": [{"type": "web_search", field: "example.com"}],
            }
        )


def test_embedded_python_tool_round_trip():
    from meshagent.api.managed_agents import ManagedAgentPythonTool

    tool = ManagedAgentPythonTool(
        name="lookup",
        input_schema={
            "type": "object",
            "properties": {"id": {"type": "string"}},
            "required": ["id"],
            "additionalProperties": False,
        },
        source="async def lookup(args): return args",
    )
    spec = ManagedAgentSpec(
        metadata={"name": "lookup-agent"},
        allowed_models=[],
        tools=[tool],
        connections={"crm": {"base_url": "https://example.com"}},
    )
    assert ManagedAgentSpec.model_validate_json(spec.model_dump_json()) == spec
    assert spec.tools[0].entrypoint is None
    assert spec.connections["crm"].use_proxy_secret is None
    with pytest.raises(ValidationError, match="duplicate"):
        ManagedAgentSpec.model_validate({**spec.model_dump(), "tools": [tool, tool]})


@pytest.mark.parametrize(
    "schema",
    [
        {"type": "object", "properties": {}, "required": []},
        {
            "type": "object",
            "properties": {"id": {"type": "string"}},
            "required": [],
            "additionalProperties": False,
        },
        {
            "type": "object",
            "properties": {"nested": {"type": "object"}},
            "required": ["nested"],
            "additionalProperties": False,
        },
        {
            "type": "object",
            "$ref": "https://example.com/schema",
            "properties": {},
            "required": [],
            "additionalProperties": False,
        },
    ],
)
def test_python_tools_reject_non_strict_schema(schema):
    from meshagent.api.managed_agents import ManagedAgentPythonTool

    with pytest.raises(ValidationError):
        ManagedAgentPythonTool(
            name="lookup",
            input_schema=schema,
            source="async def lookup(args): return args",
        )


@pytest.mark.parametrize(
    "url",
    [
        "file:///etc/passwd",
        "https://user:secret@example.com",
        "https://example.com/#secret",
    ],
)
def test_python_connections_reject_invalid_urls(url):
    from meshagent.api.managed_agents import ManagedAgentHttpConnection

    with pytest.raises(ValidationError):
        ManagedAgentHttpConnection(base_url=url)


def _guarded_spec():
    return {
        "metadata": {"name": "support"},
        "allowed_models": [{"provider": "openai", "model": "gpt-6.1-sol"}],
        "guardrails": {
            "input": [
                {
                    "name": "scope",
                    "check": {
                        "type": "decision",
                        "model": "openai/gpt-6-luna",
                        "instructions": "Allow support requests.",
                        "minimum_allow_probability": 0.9,
                    },
                }
            ],
        },
    }


def test_managed_agent_guardrails_round_trip():
    spec = ManagedAgentSpec.model_validate(_guarded_spec())
    assert spec.guardrails.input[0].on_error.action == "fail"
    assert ManagedAgentSpec.model_validate_json(spec.model_dump_json()) == spec


@pytest.mark.parametrize("probability", [-0.1, 1.1, float("nan"), float("inf")])
def test_managed_agent_guardrails_reject_invalid_threshold(probability):
    payload = _guarded_spec()
    payload["guardrails"]["input"][0]["check"]["minimum_allow_probability"] = (
        probability
    )
    with pytest.raises(ValidationError):
        ManagedAgentSpec.model_validate(payload)


def test_managed_agent_guardrails_reject_duplicate_names_and_invalid_actions():
    payload = _guarded_spec()
    payload["guardrails"]["output"] = {"checks": payload["guardrails"]["input"]}
    with pytest.raises(ValidationError, match="unique"):
        ManagedAgentSpec.model_validate(payload)
    payload = _guarded_spec()
    payload["guardrails"]["input"][0]["on_reject"] = {"action": "deny"}
    with pytest.raises(ValidationError, match="boundary"):
        ManagedAgentSpec.model_validate(payload)


@pytest.mark.parametrize("instructions", [" " * 3, "é" * 8193])
def test_guardrail_instruction_limits_match_runtime_utf8_bytes(instructions):
    payload = _guarded_spec()
    payload["guardrails"]["input"][0]["check"]["instructions"] = instructions
    with pytest.raises(ValidationError, match="UTF-8 bytes"):
        ManagedAgentSpec.model_validate(payload)


def test_guardrail_instructions_accept_utf8_byte_limit():
    payload = _guarded_spec()
    payload["guardrails"]["input"][0]["check"]["instructions"] = "é" * 8192
    assert ManagedAgentSpec.model_validate(payload).guardrails is not None


@pytest.mark.parametrize("retries", [-1, 4, 1.5])
def test_guardrail_retries_are_bounded(retries):
    payload = _guarded_spec()
    payload["guardrails"]["input"][0]["max_retries"] = retries
    with pytest.raises(ValidationError):
        ManagedAgentSpec.model_validate(payload)


def test_guardrail_error_policy_defaults_to_fail_and_accepts_pass():
    payload = _guarded_spec()
    spec = ManagedAgentSpec.model_validate(payload)
    assert spec.guardrails.input[0].max_retries == 2
    assert spec.guardrails.input[0].on_error.action == "fail"
    payload["guardrails"]["input"][0]["on_error"] = {"action": "pass"}
    assert (
        ManagedAgentSpec.model_validate(payload).guardrails.input[0].on_error.action
        == "pass"
    )
    payload["guardrails"]["input"][0]["on_reject"] = {"action": "pass"}
    with pytest.raises(ValidationError):
        ManagedAgentSpec.model_validate(payload)
