from .room_server_client import RoomClient, MeshSchema, RoomException
import json
from .participant_token import ParticipantToken, ApiScope
from typing import Optional
import os
from .websocket_protocol import WebSocketClientProtocol
import re
from warnings import deprecated


def validate_schema_name(name: str):
    if name.find(".") != -1:
        raise RoomException("schema name cannot contain '.'")


async def deploy_schema(
    *, room: RoomClient, schema: MeshSchema, name: str, overwrite: bool = True
):
    validate_schema_name(name=name)
    handle = await room.storage.open(path=f".schemas/{name}.json", overwrite=overwrite)
    await room.storage.write(
        handle=handle, data=json.dumps(schema.to_json()).encode("utf-8")
    )
    await room.storage.close(handle=handle)


def meshagent_base_url(base_url: Optional[str] = None):
    return os.getenv("MESHAGENT_API_URL", "https://api.meshagent.com")


def websocket_room_url(*, room_name: str, base_url: Optional[str] = None) -> str:
    if base_url is None:
        api_url = os.getenv("MESHAGENT_API_URL")
        if api_url is None:
            base_url = "wss://api.meshagent.com"
        else:
            if api_url.startswith("https:"):
                api_url = "wss:" + api_url.removeprefix("https:")
            elif api_url.startswith("http:"):
                api_url = "ws:" + api_url.removeprefix("http:")
            base_url = api_url

    return f"{base_url}/rooms/{room_name}"


@deprecated("create a ParticipantToken directly instead")
def participant_token(
    *, participant_name: str, room_name: str, role: Optional[str] = None
):
    if os.getenv("MESHAGENT_PROJECT_ID") is None:
        raise RoomException(
            "MESHAGENT_PROJECT_ID must be set, you can find this value in the Meshagent Studio when you view API keys."
        )

    if os.getenv("MESHAGENT_KEY_ID") is None:
        raise RoomException(
            "MESHAGENT_KEY_ID must be set, you can find this value in the Meshagent Studio when you view API keys."
        )

    if os.getenv("MESHAGENT_SECRET") is None:
        raise RoomException(
            "MESHAGENT_SECRET is must be set with a valid api key, you can find this value in the Meshagent Studio when you view API keys."
        )

    token = ParticipantToken(
        name=participant_name,
        project_id=os.getenv("MESHAGENT_PROJECT_ID"),
        api_key_id=os.getenv("MESHAGENT_KEY_ID"),
    )
    token.add_api_grant(ApiScope.agent_default())
    token.add_room_grant(room_name=room_name)
    if role is not None:
        token.add_role_grant(role=role)

    return token


@deprecated("create WebSocketClientProtocol directly instead")
def websocket_protocol(
    *, participant_name: str, room_name: str, role: Optional[str] = None
):
    url = websocket_room_url(room_name=room_name)
    token_jwt = os.getenv("MESHAGENT_TOKEN", None)
    if token_jwt is None:
        token = participant_token(
            participant_name=participant_name, room_name=room_name, role=role
        )
        token_jwt = token.to_jwt(token=os.getenv("MESHAGENT_SECRET"))

    return WebSocketClientProtocol(url=url, token=token_jwt)


# Pre-compile the pattern once if you’ll call this many times
_ROOM_NAME_RE = re.compile(r"^(?=.{1,63}$)[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$")


def is_valid_room_name(identifier: str) -> bool:
    """
    Validate that `identifier` is 1-63 chars long, starts and ends with
    a lowercase letter or digit, and contains only lowercase letters,
    digits, or single hyphens in between (no consecutive hyphens).
    """
    return _ROOM_NAME_RE.fullmatch(identifier) is not None
