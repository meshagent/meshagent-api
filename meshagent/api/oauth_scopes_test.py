from meshagent.api import FULL_OAUTH_SCOPE, FULL_OAUTH_SCOPES


def test_full_oauth_scope_matches_scopes_tuple() -> None:
    assert FULL_OAUTH_SCOPE == " ".join(FULL_OAUTH_SCOPES)


def test_full_oauth_scopes_match_official_scope_set() -> None:
    assert FULL_OAUTH_SCOPES == (
        "profile",
        "project/*",
        "room/*",
        "create_users",
        "create_rooms",
        "llm_proxy",
        "admin",
        "developer",
        "connect_room",
        "delete_room",
        "update_room",
    )
