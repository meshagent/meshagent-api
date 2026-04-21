from typing import Optional
import os


def meshagent_base_url(base_url: Optional[str] = None):
    if base_url is not None:
        return base_url.rstrip("/")

    profile_api_url = os.getenv("MESHAGENT_PROFILE_API_URL")
    if profile_api_url:
        return profile_api_url.rstrip("/")

    env_api_url = os.getenv("MESHAGENT_API_URL")
    if env_api_url:
        return env_api_url.rstrip("/")

    return "https://api.meshagent.com"


def websocket_room_url(*, room_name: str, base_url: Optional[str] = None) -> str:
    if base_url is None:
        api_url = os.getenv("MESHAGENT_ROOM_URL")
        if api_url is None:
            api_url = os.getenv("MESHAGENT_PROFILE_API_URL") or os.getenv(
                "MESHAGENT_API_URL"
            )
        if api_url is None:
            base_url = "wss://api.meshagent.com"
        else:
            if api_url.startswith("https:"):
                api_url = "wss:" + api_url.removeprefix("https:")
            elif api_url.startswith("http:"):
                api_url = "ws:" + api_url.removeprefix("http:")
            base_url = api_url

    return f"{base_url}/rooms/{room_name}"
