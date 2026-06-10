from typing import Final

FULL_OAUTH_SCOPES: Final[tuple[str, ...]] = (
    "profile",
    "project/*",
    "room/*",
    "create_users",
    "create_rooms",
    "create_agents",
    "create_mailboxes",
    "create_routes",
    "create_scheduled_tasks",
    "llm_proxy",
    "admin",
    "developer",
    "connect_room",
    "delete_room",
    "update_room",
    "delete_agent",
    "update_agent",
    "managed_agents",
)
FULL_OAUTH_SCOPE: Final[str] = " ".join(FULL_OAUTH_SCOPES)
