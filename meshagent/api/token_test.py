import os
import pytest
import jwt
from unittest.mock import patch

from meshagent.api.participant_token import ParticipantToken, ParticipantGrant

@pytest.fixture
def setup_env():
    # If MESHAGENT_SECRET is not set, we set it here for testing purposes.
    if not os.getenv("MESHAGENT_SECRET"):
        os.environ["MESHAGENT_SECRET"] = "testsecret"
    yield

def test_participant_grant_to_json():
    grant = ParticipantGrant(name="access_room", scope="read")
    assert grant.to_json() == {"name": "access_room", "scope": "read"}

def test_participant_grant_from_json():
    data = {"name": "access_room", "scope": "read"}
    grant = ParticipantGrant.from_json(data)
    assert grant.name == "access_room"
    assert grant.scope == "read"

def test_participant_token_to_json():
    token = ParticipantToken(name="John Doe")
    token.grants = [
        ParticipantGrant(name="access_room", scope="read"),
        ParticipantGrant(name="access_chat", scope="write")
    ]
    json_data = token.to_json()
    assert json_data["name"] == "John Doe"
    assert len(json_data["grants"]) == 2
    assert json_data["grants"][0] == {"name": "access_room", "scope": "read"}
    assert json_data["grants"][1] == {"name": "access_chat", "scope": "write"}

def test_participant_token_from_json():
    data = {
        "name": "Jane Doe",
        "grants": [
            {"name": "access_room", "scope": "read"},
            {"name": "access_chat", "scope": "write"}
        ]
    }
    token = ParticipantToken.from_json(data)
    assert token.name == "Jane Doe"
    assert len(token.grants) == 2
    assert token.grants[0].name == "access_room"
    assert token.grants[0].scope == "read"
    assert token.grants[1].name == "access_chat"
    assert token.grants[1].scope == "write"

@pytest.mark.usefixtures("setup_env")
def test_participant_token_to_jwt():
    token = ParticipantToken(name="John Doe")
    token.grants = [
        ParticipantGrant(name="access_room", scope="read"),
    ]
    encoded = token.to_jwt()
    assert isinstance(encoded, str)

    # Decode and verify payload
    decoded = jwt.decode(encoded, os.getenv("MESHAGENT_SECRET"), algorithms=["HS256"])
    assert decoded["name"] == "John Doe"
    assert len(decoded["grants"]) == 1
    assert decoded["grants"][0] == {"name": "access_room", "scope": "read"}

@pytest.mark.usefixtures("setup_env")
def test_participant_token_from_jwt():
    # First create a token and encode it
    original_token = ParticipantToken(name="Alice")
    original_token.grants = [
        ParticipantGrant(name="access_room", scope="read"),
        ParticipantGrant(name="access_chat", scope="write")
    ]
    encoded = original_token.to_jwt()

    # Now decode it back into a ParticipantToken
    decoded_token = ParticipantToken.from_jwt(encoded)
    assert decoded_token.name == "Alice"
    assert len(decoded_token.grants) == 2
    assert decoded_token.grants[0].name == "access_room"
    assert decoded_token.grants[0].scope == "read"
    assert decoded_token.grants[1].name == "access_chat"
    assert decoded_token.grants[1].scope == "write"

@pytest.mark.usefixtures("setup_env")
def test_participant_token_jwt_roundtrip():
    # Test a full round-trip: create a ParticipantToken, convert it to JWT,
    # then back from JWT to a ParticipantToken and ensure equality.
    token = ParticipantToken(name="Bob")
    token.grants = [ParticipantGrant(name="access_forum", scope="moderate")]

    jwt_str = token.to_jwt()
    new_token = ParticipantToken.from_jwt(jwt_str)

    assert new_token.name == token.name
    assert len(new_token.grants) == len(token.grants)
    assert new_token.grants[0].name == token.grants[0].name
    assert new_token.grants[0].scope == token.grants[0].scope