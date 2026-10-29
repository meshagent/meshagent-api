import pytest
from pydantic import ValidationError

from meshagent.api.storage_connections import CreateStorageConnection


@pytest.mark.parametrize(
    "prefix",
    [
        "../private",
        "/absolute",
        "a//b",
        "a/../b",
        "a%2fb",
        "a?token=secret",
        "gs://other-bucket",
    ],
)
def test_connection_rejects_unsafe_prefixes(prefix):
    with pytest.raises(ValidationError):
        CreateStorageConnection.model_validate(
            {
                "name": "analytics",
                "config": {
                    "provider": "gcs",
                    "bucket": "test-bucket",
                    "prefix": prefix,
                },
                "credentials": {"provider": "gcs", "service_account_json": {}},
            }
        )


def test_connection_rejects_mismatched_credentials():
    with pytest.raises(ValidationError):
        CreateStorageConnection.model_validate(
            {
                "name": "analytics",
                "config": {"provider": "gcs", "bucket": "test-bucket"},
                "credentials": {"provider": "azure", "account_key": "test-only"},
            }
        )


@pytest.mark.parametrize(
    "credentials", [{}, {"account_key": "test", "sas_token": "test"}]
)
def test_azure_requires_exactly_one_authentication_method(credentials):
    with pytest.raises(ValidationError):
        CreateStorageConnection.model_validate(
            {
                "name": "analytics",
                "config": {
                    "provider": "azure",
                    "account": "testaccount",
                    "container": "test-container",
                },
                "credentials": {"provider": "azure", **credentials},
            }
        )


def test_credential_repr_is_redacted():
    connection = CreateStorageConnection.model_validate(
        {
            "name": "analytics",
            "config": {
                "provider": "s3",
                "bucket": "test-bucket",
                "region": "us-east-1",
            },
            "credentials": {
                "provider": "s3",
                "access_key_id": "test-key",
                "secret_access_key": "never-display-this",
            },
        }
    )
    assert "never-display-this" not in repr(connection)
    assert "never-display-this" not in repr(connection.credentials)
