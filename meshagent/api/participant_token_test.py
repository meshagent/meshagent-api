from datetime import datetime, timedelta, timezone

import jwt
import pytest

# ────────────────────────────────────────────────────────────────────────────────
# Replace this single import line as needed
from .participant_token import (  # noqa: E402, F401
    AgentsGrant,
    LivekitGrant,
    QueuesGrant,
    TableGrant,
    DatabaseGrant,
    SyncGrant,
    SyncPathGrant,
    StorageGrant,
    StoragePathGrant,
    ContainersGrant,
    ApiScope,
    ParticipantToken,
)


# ────────────────────────────────────────────────────────────────────────────────
# Basic, per‑grant behaviour
# ────────────────────────────────────────────────────────────────────────────────
def test_agents_grant_defaults() -> None:
    g = AgentsGrant()
    assert all(
        getattr(g, field)
        for field in (
            "register_agent",
            "register_public_toolkit",
            "register_private_toolkit",
            "call",
            "use_agents",
            "use_tools",
        )
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


def test_database_grant() -> None:
    # unrestricted
    g = DatabaseGrant()
    assert g.can_read("tbl")
    assert g.can_write("tbl")
    assert g.can_alter("tbl")

    # table‑level rules
    tables = [
        TableGrant(name="read_only", read=True, write=False, alter=False),
        TableGrant(name="write_only", read=False, write=True, alter=False),
    ]
    g = DatabaseGrant(tables=tables)
    assert g.can_read("read_only") and not g.can_write("read_only")
    assert g.can_write("write_only") and not g.can_read("write_only")
    assert not g.can_read("unknown") and not g.can_write("unknown")


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

    g = ContainersGrant(pull=["lib/*"], run=["runtime/*"])
    # Pull follows pull‑list
    assert g.can_pull("lib/tool") and not g.can_pull("xxx/tool")
    # Run should follow *run‑list* (the current implementation mistakenly
    # looks at `pull`; this test will fail if that bug is present)
    assert g.can_run("runtime/app")
    assert not g.can_run("other/app")


# ────────────────────────────────────────────────────────────────────────────────
# ParticipantToken behaviour
# ────────────────────────────────────────────────────────────────────────────────
def test_participant_token_role_and_is_user() -> None:
    p = ParticipantToken(name="alice")
    assert p.role == "user" and p.is_user

    p.add_role_grant("admin")
    assert p.role == "admin" and not p.is_user


def test_get_api_grant_defaults_to_full_for_old_versions() -> None:
    pt = ParticipantToken(name="bob", version="0.5.3")
    api = pt.get_api_grant()
    assert isinstance(api, ApiScope) and api.queues and api.sync


def test_token_json_round_trip() -> None:
    pt = ParticipantToken(name="charlie")
    pt.add_role_grant("moderator")
    pt.add_room_grant("main")

    clone = ParticipantToken.from_json(pt.to_json())
    assert clone.name == pt.name
    assert clone.role == "moderator"
    assert clone.grant_scope("room") == "main"


def test_token_jwt_round_trip() -> None:
    pt = ParticipantToken(name="dave")
    jwt_str = pt.to_jwt()

    recovered = ParticipantToken.from_jwt(jwt_str)
    assert recovered.name == "dave"


def test_token_expiration() -> None:
    secret = "expire‑secret"
    pt = ParticipantToken(name="eve")
    exp = datetime.now(timezone.utc) + timedelta(seconds=5)
    token = pt.to_jwt(token=secret, expiration=exp)
    decoded = jwt.decode(token, key=secret, algorithms=["HS256"])
    assert abs(decoded["exp"] - int(exp.timestamp())) < 2  # within clock skew


def test_legacy_token():
    token = ParticipantToken.from_json(
        {
            "name": "72c17196-3f2d-4444-a55b-39825e35cbb7",
            "grants": [
                {"name": "room", "scope": "44bb91aa-2555-4487-8173-580027a87558"}
            ],
            "sub": "2",
        }
    )

    assert token.version == "0.5.3"
    api = token.get_api_grant()
    assert api is not None
    assert api.storage is not None
    assert api.livekit is not None
    assert api.agents is not None
    assert api.developer is not None
    assert api.database is not None
    assert api.messaging is not None
    assert api.queues is not None
    assert api.containers is None
    assert api.admin is None
    assert token.grant_scope("room") == "44bb91aa-2555-4487-8173-580027a87558"
    assert token.name == "72c17196-3f2d-4444-a55b-39825e35cbb7"
    assert api.storage.can_read("/test")
