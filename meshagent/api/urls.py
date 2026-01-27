from typing import Optional
import os


def meshagent_base_url(base_url: Optional[str] = None):
    return os.getenv("MESHAGENT_API_URL", "https://api.meshagent.com")


def websocket_room_url(*, room_name: str, base_url: Optional[str] = None) -> str:
    if base_url is None:
        api_url = os.getenv("MESHAGENT_ROOM_URL", os.getenv("MESHAGENT_API_URL"))
        if api_url is None:
            base_url = "wss://api.meshagent.com"
        else:
            if api_url.startswith("https:"):
                api_url = "wss:" + api_url.removeprefix("https:")
            elif api_url.startswith("http:"):
                api_url = "ws:" + api_url.removeprefix("http:")
            base_url = api_url

    return f"{base_url}/rooms/{room_name}"
