from .room_server_client import RoomClient, MeshSchema, RoomException
import json 
from .participant_token import ParticipantToken
from typing import Optional
import os
from .websocket_protocol import WebSocketClientProtocol

def validate_schema_name(name: str):
    if name.find(".") != -1:
        raise RoomException("schema name cannot contain '.'")

async def deploy_schema(*, room: RoomClient, schema: MeshSchema, name: str, overwrite: bool = True):

    validate_schema_name(name=name)
    handle = await room.storage.open(path=f".schemas/{name}.json", overwrite=overwrite)
    await room.storage.write(handle=handle, data=json.dumps(schema.to_json()).encode("utf-8"))
    await room.storage.close(handle=handle)

def meshagent_base_url(base_url: Optional[str] = None):
    return os.getenv("MESHAGENT_API_URL",  "https://api.meshagent.com")

def websocket_room_url(*, room_name: str, base_url: Optional[str] = None) -> str:
    
    if base_url == None:
        api_url = os.getenv("MESHAGENT_API_URL")
        if api_url == None:
            base_url = "wss://api.meshagent.com"
        else:
            if api_url.startswith("https:"):
                api_url = "wss:"+api_url.removeprefix("https:")
            elif api_url.startswith("http:"):
                api_url = "ws:"+api_url.removeprefix("http:")                
            base_url = api_url

    return f"{base_url}/rooms/{room_name}"

def participant_token(*, participant_name: str, room_name: str, role: Optional[str] = None):

    if os.getenv("MESHAGENT_PROJECT_ID") is None:
        raise Exception('MESHAGENT_PROJECT_ID must be set, you can find this value in the Meshagent Studio when you view API keys.')
    
    if os.getenv("MESHAGENT_KEY_ID") is None:
        raise Exception('MESHAGENT_KEY_ID is must be set, you can find this value in the Meshagent Studio when you view API keys.')
    
    if os.getenv("MESHAGENT_SECRET") is None:
        raise Exception('MESHAGENT_SECRET is must be set with a valid api key, you can find this value in the Meshagent Studio when you view API keys.')

    token = ParticipantToken(
        name=participant_name,
        project_id=os.getenv("MESHAGENT_PROJECT_ID"),
        api_key_id=os.getenv("MESHAGENT_KEY_ID")
    )
    token.add_room_grant(room_name=room_name)
    if role != None:
        token.add_role_grant(role=role)

    return token

def websocket_protocol(*, participant_name: str, room_name: str, role: Optional[str] = None):
     url = websocket_room_url(room_name=room_name)
     token = participant_token(participant_name=participant_name, room_name=room_name, role=role)
     return WebSocketClientProtocol(
        url = url,
        token = token.to_jwt(token=os.getenv("MESHAGENT_SECRET"))
    )
