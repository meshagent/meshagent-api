import json
from pathlib import Path

from meshagent.api.file_permissions import FilesPolicy
from meshagent.api.participant_token import ApiScope, ParticipantToken, StorageGrant


def test_shared_storage_policy_vectors_round_trip_without_expanding_grants():
    fixture = json.loads(
        (
            Path(__file__).resolve().parents[4] / "testdata/files-permissions.json"
        ).read_text()
    )
    for key, cases in [("grant", "cases"), ("legacy_grant", "legacy_cases")]:
        grant = StorageGrant.model_validate(fixture[key])
        restored = StorageGrant.model_validate_json(grant.model_dump_json())
        participant = ParticipantToken(name="files-policy-test")
        participant.add_api_grant(ApiScope(storage=restored))
        secret = "files-policy-test-secret-at-least-32-bytes"
        decoded = ParticipantToken.from_jwt(
            participant.to_jwt(token=secret), token=secret
        )
        scope = decoded.get_api_grant()
        assert scope is not None and scope.storage is not None
        restored = scope.storage
        for case in fixture[cases]:
            path = case["path"]
            actual = {
                "read": restored.can_read(path),
                "write": restored.can_write(path),
                "delete": restored.can_delete(path),
                "list": restored.can_list(path),
                "list_directory": restored.can_list_directory(path),
                "presign": restored.can_presign(path),
                "read_tree": restored.can_read(path, subtree=True),
                "write_tree": restored.can_write(path, subtree=True),
                "delete_tree": restored.can_delete(path, recursive=True),
            }
            assert actual == {
                name: value for name, value in case.items() if name != "path"
            }, (key, path)
    assert not StorageGrant(files=FilesPolicy()).can_read("public/file")
    assert not StorageGrant(files=FilesPolicy()).can_write("public/file")
    assert not StorageGrant().can_presign("file")
