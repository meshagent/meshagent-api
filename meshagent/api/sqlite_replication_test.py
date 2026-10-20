from unittest.mock import AsyncMock

import pytest

from meshagent.api.room_server_client import SqliteClient, SqliteReplicationSettings
from meshagent.api.messaging import EmptyContent, JsonContent


@pytest.mark.asyncio
async def test_replication_settings_wire_format():
    room = AsyncMock()
    room.invoke.return_value = EmptyContent()
    client = SqliteClient(room)
    await client.create_database(
        name="app", replication_mode="asynchronous", flush_interval_ms=250
    )
    assert room.invoke.call_args.kwargs["input"] == {
        "name": "app",
        "namespace": None,
        "mode": "create",
        "replication_mode": "asynchronous",
        "flush_interval_ms": 250,
    }
    room.invoke.return_value = JsonContent(
        json={"replication_mode": "asynchronous", "flush_interval_ms": 500}
    )
    settings = await client.update_database_settings(name="app", flush_interval_ms=500)
    assert settings == SqliteReplicationSettings(
        replication_mode="asynchronous", flush_interval_ms=500
    )
    assert room.invoke.call_args.kwargs["input"] == {
        "name": "app",
        "namespace": None,
        "flush_interval_ms": 500,
    }
    assert await client.get_database_settings(name="app") == settings
    await client.flush_database(name="app")
    assert room.invoke.call_args.kwargs["tool"] == "flush_database"
