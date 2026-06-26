import pytest
from pydantic import ValidationError

from meshagent.api.managed_agents import ManagedAgentSpec


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
