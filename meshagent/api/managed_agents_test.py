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


def test_managed_agent_storage_mount_accepts_agent_path():
    spec = ManagedAgentSpec.model_validate(
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

    assert spec.toolkits is not None
    storage_toolkit = spec.toolkits[0]
    assert storage_toolkit.type == "storage"
    assert storage_toolkit.mounts[0].path == "/agent2"


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


def test_managed_agent_storage_mount_requires_path():
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
                        "mounts": [{"type": "agent"}],
                    }
                ],
            }
        )


def test_managed_agent_storage_mount_forbids_unknown_fields():
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
                        "mounts": [
                            {
                                "type": "agent",
                                "path": "/agent",
                                "unexpected": "value",
                            }
                        ],
                    }
                ],
            }
        )


def test_managed_agent_room_mount_requires_room_name():
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
                        "mounts": [{"type": "room", "path": "/jesse"}],
                    }
                ],
            }
        )


def test_managed_agent_spec_accepts_mcp_agent_secret_authorization():
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
                            "authorization": {
                                "type": "bearer",
                                "secret": {
                                    "type": "agent",
                                    "secret_id": "github-token",
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
    assert toolkit.servers[0].authorization.secret.secret_id == "github-token"


def test_managed_agent_spec_accepts_mcp_user_secret_header_authorization():
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
                                    "prompt": "Linear API key",
                                    "oauth": {
                                        "secret_name": "linear-token",
                                        "scopes": ["read"],
                                        "registration_endpoint": "https://mcp.example.test/register",
                                        "redirect_uri": "meshagent-studio://oauth2/callback",
                                    },
                                },
                            },
                        }
                    ],
                }
            ],
        }
    )

    assert spec.toolkits is not None
    auth = spec.toolkits[0].servers[0].authorization
    assert auth.name == "X-API-Key"
    assert auth.secret.secret_name == "linear-token"
    assert auth.secret.oauth.scopes == ["read"]
    assert (
        auth.secret.oauth.registration_endpoint == "https://mcp.example.test/register"
    )
    assert auth.secret.oauth.redirect_uri == "meshagent-studio://oauth2/callback"
