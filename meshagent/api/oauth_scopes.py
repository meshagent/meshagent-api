from typing import Final

FULL_OAUTH_SCOPES: Final[tuple[str, ...]] = (
    "profile",
    "project/*",
    "room/*",
    "create_users",
    "create_rooms",
    "admin",
    "developer",
    "connect_room",
    "delete_room",
    "update_room",
)
FULL_OAUTH_SCOPE: Final[str] = " ".join(FULL_OAUTH_SCOPES)
