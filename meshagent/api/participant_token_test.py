from datetime import datetime, timedelta, timezone

import jwt
import pytest
from pydantic import ValidationError

from .version import __version__

# ────────────────────────────────────────────────────────────────────────────────
# Replace this single import line as needed
from .participant_token import (  # noqa: E402, F401
    AgentsGrant,
    ContainerRegistryGrant,
    LivekitGrant,
    QueuesGrant,
    MessagingGrant,
    TableGrant,
    DatasetGrant,
    MemoryEntryGrant,
    MemoryGrant,
    MemoryPermissions,
    SyncGrant,
    SyncPathGrant,
    StorageGrant,
    StoragePathGrant,
    ContainersGrant,
    LLMGrant,
    ServicesGrant,
    ApiScope,
    ParticipantToken,
    ParticipantTokenSpec,
)


# ────────────────────────────────────────────────────────────────────────────────
# Basic, per‑grant behaviour
# ────────────────────────────────────────────────────────────────────────────────
def test_agents_grant_defaults() -> None:
    g = AgentsGrant()
    assert g.register_agent
    assert g.register_public_toolkit
    assert g.register_private_toolkit
    assert g.call
    assert g.use_agents
    assert g.use_tools


def test_api_scope_agent_default_includes_secrets_without_admin_or_tunnels() -> None:
    scope = ApiScope.agent_default()

    assert scope.livekit is not None
    assert scope.llm is not None
    assert scope.memory is not None
    assert scope.services is not None
    assert scope.secrets is not None
    assert scope.admin is None
    assert scope.tunnels is None


def test_api_scope_user_default_includes_secrets_without_admin_or_tunnels() -> None:
    scope = ApiScope.user_default()

    assert scope.livekit is not None
    assert scope.llm is None
    assert scope.memory is not None
    assert scope.services is not None
    assert scope.secrets is not None
    assert scope.admin is None
    assert scope.tunnels is None


def test_llm_grant_model_and_provider_restrictions() -> None:
    grant = LLMGrant(models=["openai/gpt-4o*", "anthropic/claude-sonnet-4-5"])

    assert grant.can_use_provider("openai")
    assert grant.can_use_provider("anthropic")
    assert not grant.can_use_provider("google")
    assert grant.can_use_model(provider="openai", model="gpt-4o-mini")
    assert not grant.can_use_model(provider="openai", model="gpt-4.1")
    assert grant.can_use_model(provider="anthropic", model="claude-sonnet-4-5")


def test_participant_token_spec_requires_llm_grant() -> None:
    with pytest.raises(ValidationError, match="llm grant"):
        ParticipantTokenSpec.model_validate(
            {
                "version": "v1",
                "kind": "ParticipantToken",
                "identity": "cli-agent",
                "api": {"storage": {}},
            }
        )


@pytest.mark.parametrize(
    "rooms,name,expected",
    [
        (None, "anything", True),
        (["blue", "red"], "blue", True),
        (["blue", "red"], "green", False),
    ],
)
def test_livekit_grant_can_join_breakout_room(rooms, name, expected) -> None:
    g = LivekitGrant(breakout_rooms=rooms)
    assert g.can_join_breakout_room(name) is expected


def test_queues_grant() -> None:
    g = QueuesGrant()
    assert g.can_send("alpha")
    assert g.can_receive("beta")

    restricted = QueuesGrant(send=["s1"], receive=["r1"])
    assert restricted.can_send("s1")
    assert not restricted.can_send("x")
    assert restricted.can_receive("r1")
    assert not restricted.can_receive("s1")


@pytest.mark.parametrize(
    ("grant_type", "payload"),
    [
        (QueuesGrant, {"list": False}),
        (MessagingGrant, {"list": False}),
        (MemoryGrant, {"list": False}),
        (ServicesGrant, {"list": False}),
    ],
)
def test_list_alias_fields_round_trip_for_grants(grant_type, payload) -> None:
    grant = grant_type.model_validate(payload)
    assert grant.list is False
    assert grant.model_dump()["list"] is False


def test_dataset_grant() -> None:
    # unrestricted
    g = DatasetGrant()
    assert g.can_read("tbl")
    assert g.can_write("tbl")
    assert g.can_alter("tbl")

    # table‑level rules
    tables = [
        TableGrant(name="read_only", read=True, write=False, alter=False),
        TableGrant(
            name="write_only",
            namespace=["analytics"],
            read=False,
            write=True,
            alter=False,
        ),
    ]
    g = DatasetGrant(tables=tables)
    assert g.can_read("read_only") and not g.can_write("read_only")
    assert g.can_write("write_only", namespace=["analytics"])
    assert not g.can_write("write_only", namespace=["default"])
    assert not g.can_read("write_only", namespace=["analytics"])
    assert not g.can_read("unknown") and not g.can_write("unknown")


@pytest.mark.parametrize("legacy_key", ["database", "datasets"])
def test_api_scope_reads_legacy_dataset_grant_keys_and_writes_dataset(
    legacy_key: str,
) -> None:
    scope = ApiScope.model_validate({legacy_key: {"list_tables": False}})

    assert scope.dataset is not None
    assert scope.dataset.list_tables is False
    assert scope.model_dump(mode="json", exclude_none=True) == {
        "dataset": {"list_tables": False}
    }


def test_memory_grant_scoped_to_memory_name_and_namespace() -> None:
    unrestricted = MemoryGrant()
    assert unrestricted.can_create(name="profile")
    assert unrestricted.can_query(name="profile")
    assert unrestricted.can_recall(name="profile")

    restricted = MemoryGrant(
        memories=[
            MemoryEntryGrant(
                name="memories",
                namespace=["agents", "assistant"],
                permissions=MemoryPermissions(
                    create=True,
                    drop=False,
                    inspect=True,
                    query=True,
                    upsert=True,
                    ingest=True,
                    recall=True,
                    optimize=False,
                ),
            )
        ]
    )
    assert restricted.can_create(name="memories", namespace=["agents", "assistant"])
    assert not restricted.can_drop(name="memories", namespace=["agents", "assistant"])
    assert not restricted.can_optimize(
        name="memories", namespace=["agents", "assistant"]
    )
    assert not restricted.can_query(name="memories", namespace=["agents", "other"])
    assert not restricted.can_query(name="other", namespace=["agents", "assistant"])


def test_sync_grant_path_and_wildcard() -> None:
    any_path = SyncGrant()
    assert any_path.can_read("/data/x") and any_path.can_write("/data/x")

    paths = [
        SyncPathGrant(path="/cfg/settings.json", read_only=True),
        SyncPathGrant(path="/public/*"),
    ]
    g = SyncGrant(paths=paths)

    assert g.can_read("/cfg/settings.json") and not g.can_write("/cfg/settings.json")
    assert g.can_write("/public/hello.txt")
    assert not g.can_read("/private/secret.txt")


def test_storage_grant() -> None:
    unrestricted = StorageGrant()
    assert unrestricted.can_write("bucket/file")

    g = StorageGrant(
        paths=[
            StoragePathGrant(path="bucket/photos/", read_only=True),
            StoragePathGrant(path="bucket/logs/"),
        ]
    )
    assert g.can_read("bucket/photos/pic.jpg") and not g.can_write(
        "bucket/photos/pic.jpg"
    )
    assert g.can_write("bucket/logs/app.log")
    assert not g.can_read("other/file")


def test_containers_grant() -> None:
    g = ContainersGrant()
    assert g.can_pull("repo/image") and g.can_run("repo/image")
    assert g.can_registry_list("team/app")
    assert g.can_registry_pull("team/app")
    assert g.can_registry_run("team/app")
    assert g.can_registry_write("team/app")

    g = ContainersGrant(pull=["lib/*"], run=["runtime/*"])
    # Pull follows pull‑list
    assert g.can_pull("lib/tool") and not g.can_pull("xxx/tool")
    # Run should follow *run‑list* (the current implementation mistakenly
    # looks at `pull`; this test will fail if that bug is present)
    assert g.can_run("runtime/app")
    assert not g.can_run("other/app")

    exact = ContainersGrant(pull=["repo/image"], run=["runtime/app"])
    assert exact.can_pull("repo/image")
    assert not exact.can_pull("repo/image-extra")
    assert exact.can_run("runtime/app")
    assert not exact.can_run("runtime/app-shell")

    registry = ContainersGrant(
        registry=ContainerRegistryGrant(
            pull=["team/*"],
            run=["runtime/*"],
            write=["publish/*"],
        )
    )
    assert registry.can_registry_list("team/app")
    assert registry.can_registry_list("runtime/app")
    assert registry.can_registry_list("publish/site")
    assert not registry.can_registry_list("other/app")
    assert registry.can_registry_pull("team/app")
    assert not registry.can_registry_pull("other/app")
    assert registry.can_registry_run("runtime/app")
    assert not registry.can_registry_run("team/app")
    assert registry.can_registry_write("publish/site")
    assert not registry.can_registry_write("team/app")

    exact_registry = ContainersGrant(
        registry=ContainerRegistryGrant(
            list=["catalog/*"],
            pull=["pull/*"],
            run=["run/*"],
            write=[],
        )
    )
    assert exact_registry.can_registry_list("catalog/app")
    assert not exact_registry.can_registry_list("pull/app")
    assert exact_registry.can_registry_pull("pull/app")
    assert exact_registry.can_registry_run("run/app")
    assert not exact_registry.can_registry_write("run/app")


# ────────────────────────────────────────────────────────────────────────────────
# ParticipantToken behaviour
# ────────────────────────────────────────────────────────────────────────────────
def test_participant_token_role_and_is_user() -> None:
    p = ParticipantToken(name="alice")
    assert p.role == "user" and p.is_user

    p.add_role_grant("admin")
    assert p.role == "admin" and not p.is_user


def test_get_api_grant_requires_explicit_api_scope() -> None:
    pt = ParticipantToken(name="bob", version="0.5.3")
    api = pt.get_api_grant()
    assert api is None


def test_token_json_round_trip() -> None:
    pt = ParticipantToken(name="charlie")
    pt.add_role_grant("moderator")
    pt.add_room_grant("main")
    pt.extra_payload = {"meshagent_bootstrap": True, "custom": "value"}

    clone = ParticipantToken.from_json(pt.to_json())
    assert clone.name == pt.name
    assert clone.role == "moderator"
    assert clone.grant_scope("room") == "main"
    assert clone.extra_payload == {"meshagent_bootstrap": True, "custom": "value"}


def test_token_jwt_round_trip() -> None:
    pt = ParticipantToken(name="dave")
    jwt_str = pt.to_jwt()

    recovered = ParticipantToken.from_jwt(jwt_str)
    assert recovered.name == "dave"


def test_token_from_json_rejects_missing_required_fields() -> None:
    with pytest.raises(ValueError, match="missing required field: name"):
        ParticipantToken.from_json({"grants": []})

    with pytest.raises(ValueError, match="missing required field: grants"):
        ParticipantToken.from_json({"name": "dave"})


def test_token_from_jwt_rejects_malformed_payload_as_invalid_token() -> None:
    secret = "malformed-payload-secret-32-bytes"
    jwt_str = jwt.encode(
        payload={"sub": "project-1", "grants": []},
        key=secret,
        algorithm="HS256",
    )

    with pytest.raises(jwt.InvalidTokenError, match="valid participant token"):
        ParticipantToken.from_jwt(jwt_str, token=secret)


def test_token_expiration() -> None:
    secret = "expire‑secret"
    pt = ParticipantToken(name="eve")
    exp = datetime.now(timezone.utc) + timedelta(seconds=5)
    token = pt.to_jwt(token=secret, expiration=exp)
    decoded = jwt.decode(token, key=secret, algorithms=["HS256"])
    assert abs(decoded["exp"] - int(exp.timestamp())) < 2  # within clock skew


def test_token_explicit_secret_preserves_kid() -> None:
    secret = "explicit-secret"
    pt = ParticipantToken(name="eve", project_id="project-1", api_key_id="key-1")
    token = pt.to_jwt(token=secret)
    decoded = jwt.decode(token, key=secret, algorithms=["HS256"])
    assert decoded["kid"] == "key-1"
    assert decoded["sub"] == "project-1"


def test_token_default_secret_strips_kid_without_api_key(monkeypatch) -> None:
    secret = "default-secret"
    monkeypatch.setenv("MESHAGENT_SECRET", secret)

    pt = ParticipantToken(name="eve", project_id="project-1", api_key_id="key-1")
    token = pt.to_jwt()
    decoded = jwt.decode(token, key=secret, algorithms=["HS256"])

    assert "kid" not in decoded
    assert decoded["sub"] == "project-1"


def test_unversioned_token_uses_current_version_and_no_implicit_api_scope():
    token = ParticipantToken.from_json(
        {
            "name": "72c17196-3f2d-4444-a55b-39825e35cbb7",
            "grants": [
                {"name": "room", "scope": "44bb91aa-2555-4487-8173-580027a87558"}
            ],
            "sub": "2",
        }
    )

    assert token.version == __version__
    api = token.get_api_grant()
    assert api is None
    assert token.grant_scope("room") == "44bb91aa-2555-4487-8173-580027a87558"
    assert token.name == "72c17196-3f2d-4444-a55b-39825e35cbb7"
