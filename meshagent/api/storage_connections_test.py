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


@pytest.mark.parametrize(
    "config,expected_options",
    [
        (
            {"provider": "gcs", "bucket": "test-bucket"},
            {"google_skip_signature": "true"},
        ),
        (
            {"provider": "s3", "bucket": "test-bucket", "region": "us-east-1"},
            {"aws_skip_signature": "true", "aws_region": "us-east-1"},
        ),
        (
            {
                "provider": "azure",
                "account": "testaccount",
                "container": "test-container",
            },
            {
                "azure_skip_signature": "true",
                "azure_storage_account_name": "testaccount",
            },
        ),
    ],
)
def test_public_connections_explicitly_disable_signing(config, expected_options):
    from uuid import uuid4
    from meshagent.api.storage_connections import (
        ResolvedStorageConnection,
        UpdateStorageConnection,
    )

    public = {**config, "anonymous": True}
    connection = CreateStorageConnection(name="public", config=public)
    assert connection.credentials is None
    resolved = ResolvedStorageConnection(id=uuid4(), revision=1, config=public)
    assert resolved.location_and_options("events.lance")[1] == expected_options
    with pytest.raises(ValidationError):
        CreateStorageConnection(name="private", config=config)
    for model in (CreateStorageConnection, UpdateStorageConnection):
        with pytest.raises(ValidationError):
            model(
                name="public",
                config=public,
                credentials={"provider": "gcs", "service_account_json": {}},
            )
