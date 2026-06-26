import pytest

from meshagent.api import FULL_OAUTH_SCOPE, FULL_OAUTH_SCOPES


EXPECTED_FULL_OAUTH_SCOPES = (
    "profile:read",
    "profile:write",
    "project/*",
    "room/*",
    "users:create",
    "users:read",
    "users:update",
    "users:delete",
    "projects:read",
    "projects:update",
    "projects:iam.read",
    "projects:iam.write",
    "projects:billing.read",
    "projects:billing.write",
    "rooms:create",
    "rooms:read",
    "rooms:connect",
    "rooms:update",
    "rooms:delete",
    "agents:create",
    "agents:read",
    "agents:update",
    "agents:delete",
    "agents:run",
    "agents:sessions.read",
    "mailboxes:create",
    "mailboxes:read",
    "mailboxes:update",
    "mailboxes:delete",
    "routes:create",
    "routes:read",
    "routes:update",
    "routes:delete",
    "scheduledTasks:create",
    "scheduledTasks:read",
    "scheduledTasks:update",
    "scheduledTasks:delete",
    "services:create",
    "services:read",
    "services:update",
    "services:delete",
    "repositories:create",
    "repositories:read",
    "repositories:update",
    "repositories:delete",
    "apiKeys:create",
    "apiKeys:read",
    "apiKeys:delete",
    "serviceAccounts:create",
    "serviceAccounts:read",
    "serviceAccounts:update",
    "serviceAccounts:delete",
    "oauthClients:create",
    "oauthClients:read",
    "oauthClients:update",
    "oauthClients:delete",
    "llm:invoke",
    "llm:usage.read",
    "llm:logs.read",
    "llm:logs.write",
    "secrets:read",
    "secrets:write",
    "secrets:delete",
    "secrets:grant",
    "secrets:proxy",
)


def test_full_oauth_scope_matches_scopes_tuple() -> None:
    assert FULL_OAUTH_SCOPE == " ".join(FULL_OAUTH_SCOPES)


def test_full_oauth_scopes_match_official_scope_set() -> None:
    assert FULL_OAUTH_SCOPES == EXPECTED_FULL_OAUTH_SCOPES


def test_full_oauth_scopes_are_unique_and_non_empty() -> None:
    assert len(FULL_OAUTH_SCOPES) == len(set(FULL_OAUTH_SCOPES))
    assert all(scope.strip() == scope and scope for scope in FULL_OAUTH_SCOPES)


@pytest.mark.parametrize("scope", EXPECTED_FULL_OAUTH_SCOPES)
def test_each_official_scope_is_in_full_oauth_scope_string(scope: str) -> None:
    assert scope in FULL_OAUTH_SCOPE.split()
