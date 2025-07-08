from typing import Optional
import logging
import asyncio
from aiohttp import web
from meshagent.api.services import Portable

from .schema import MeshSchema
from .helpers import deploy_schema, validate_schema_name
from .room_server_client import RoomClient


logger = logging.getLogger("schema")


class SchemaRegistration:
    def __init__(self, *, name: str, schema: MeshSchema):
        validate_schema_name(name=name)

        self._name = name
        self._schema = schema

    @property
    def name(self):
        return self._name

    @property
    def schema(self):
        return self._schema


class SchemaRegistry(Portable):
    def __init__(
        self,
        *,
        path: Optional[str] = None,
        app: Optional[web.Application] = None,
        host=None,
        port=None,
        webhook_secret=None,
        schemas: list[SchemaRegistration],
        name: str = "schema.registry",
        validate_webhook_secret: Optional[bool] = None,
    ):
        self._name = name
        self._schemas = schemas

        super().__init__(
            path=path,
            app=app,
            host=host,
            port=port,
            webhook_secret=webhook_secret,
            validate_webhook_secret=validate_webhook_secret,
        )

    @property
    def name(self):
        return self._name

    @property
    def schemas(self):
        return self._schemas

    async def start(self, room: RoomClient) -> None:
        await asyncio.gather(
            *list(
                map(
                    lambda r: deploy_schema(room=room, schema=r.schema, name=r.name),
                    self._schemas,
                )
            )
        )

    async def stop(self) -> None:
        pass
