"""Batch parity against a host-local Rust room-server-cli."""

import os
import uuid
from urllib.parse import urlparse

import pyarrow as pa
import pytest

from meshagent.api import (
    ApiScope,
    ParticipantToken,
    RoomClient,
    SqliteSqlCommand,
    SqliteSqlQuery,
    SqliteSqlStatement,
    WebSocketClientProtocol,
)
from meshagent.api.room_server_client import RoomException


@pytest.mark.asyncio
async def test_sqlite_batch_against_rust_api():
    base_url = os.environ.get("MESHAGENT_API_URL")
    if not base_url:
        pytest.skip("MESHAGENT_API_URL must point at a local Rust room server")
    assert urlparse(base_url).hostname in {"localhost", "127.0.0.1", "::1"}
    room_name = f"python-sqlite-batch-{uuid.uuid4()}"
    token = ParticipantToken(
        name="batch-client",
        project_id="testproject",
        api_key_id="test-key-secure-key-sample2560binarykey",
    )
    token.add_room_grant(room_name)
    token.add_role_grant("agent")
    token.add_api_grant(ApiScope.agent_default())
    protocol = WebSocketClientProtocol(
        url=base_url.replace("http", "ws", 1) + f"/rooms/{room_name}",
        token=token.to_jwt(token="test-secret-secure-secret-sample2560binarykey"),
    )
    async with RoomClient(protocol_factory=protocol.create_factory()) as room:
        db = room.sqlite.database(
            f"python_batch_{uuid.uuid4().hex}", namespace=["team"]
        )
        await db.create_database()
        assert await db.execute_sql_batch(commands=[]) == []
        results = await db.execute_sql_batch(
            commands=[
                SqliteSqlCommand(
                    "CREATE TABLE items(id INTEGER PRIMARY KEY, label TEXT)"
                ),
                SqliteSqlCommand("INSERT INTO items VALUES (?, ?)", [1, "Ada"]),
                SqliteSqlCommand(
                    "UPDATE items SET label=:label WHERE id=:id",
                    {"label": "Grace", "id": 1},
                ),
                SqliteSqlCommand("SELECT * FROM items"),
            ]
        )
        assert results[1] == SqliteSqlStatement(1)
        query = results[3]
        assert isinstance(query, SqliteSqlQuery)
        try:
            chunks = [
                chunk
                async for chunk in room.sqlite.read_sql_query(query_id=query.query_id)
            ]
            assert pa.concat_tables(chunks).to_pylist() == [{"id": 1, "label": "Grace"}]
            assert query.schema.names == ["id", "label"]
        finally:
            await room.sqlite.close_sql_query(query_id=query.query_id)
        with pytest.raises(RoomException, match="command 1 failed"):
            await db.execute_sql_batch(
                commands=[
                    SqliteSqlCommand("INSERT INTO items VALUES (2, 'rollback')"),
                    SqliteSqlCommand("INSERT INTO items VALUES (1, 'duplicate')"),
                ]
            )
        assert await db.count(table="items") == 1
