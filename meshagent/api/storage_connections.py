"""Room-owned external storage configuration and credential wire models."""

from datetime import datetime
import json
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class _StorageModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class _Location(_StorageModel):
    anonymous: bool = False
    prefix: str = ""

    @field_validator("prefix")
    @classmethod
    def validate_prefix(cls, value: str) -> str:
        if value and (
            value.startswith("/")
            or any(char in value for char in "\\:?#%")
            or any(part in {"", ".", ".."} for part in value.split("/"))
            or any(ord(char) < 32 for char in value)
        ):
            raise ValueError("prefix must be a relative object storage path")
        return value


StorageName = Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9.-]*[a-z0-9]$")]


class GCSStorageConfig(_Location):
    provider: Literal["gcs"] = "gcs"
    bucket: StorageName


class S3StorageConfig(_Location):
    provider: Literal["s3"] = "s3"
    bucket: StorageName
    region: Annotated[str, Field(pattern=r"^[a-z0-9-]+$")]


class AzureStorageConfig(_Location):
    provider: Literal["azure"] = "azure"
    account: Annotated[str, Field(pattern=r"^[a-z0-9]{3,24}$")]
    container: StorageName


StorageConfig = Annotated[
    GCSStorageConfig | S3StorageConfig | AzureStorageConfig,
    Field(discriminator="provider"),
]


class GCSStorageCredentials(_StorageModel):
    provider: Literal["gcs"] = "gcs"
    service_account_json: dict[str, Any] = Field(repr=False)


class S3StorageCredentials(_StorageModel):
    provider: Literal["s3"] = "s3"
    access_key_id: str = Field(min_length=1, repr=False)
    secret_access_key: str = Field(min_length=1, repr=False)
    session_token: str | None = Field(default=None, repr=False)


class AzureStorageCredentials(_StorageModel):
    provider: Literal["azure"] = "azure"
    account_key: str | None = Field(default=None, min_length=1, repr=False)
    sas_token: str | None = Field(default=None, min_length=1, repr=False)

    @model_validator(mode="after")
    def require_one_credential(self):
        if (self.account_key is None) == (self.sas_token is None):
            raise ValueError("provide exactly one of account_key or sas_token")
        return self


StorageCredentials = Annotated[
    GCSStorageCredentials | S3StorageCredentials | AzureStorageCredentials,
    Field(discriminator="provider"),
]
ConnectionName = Annotated[
    str, Field(pattern=r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
]


class CreateStorageConnection(_StorageModel):
    name: ConnectionName
    config: StorageConfig
    credentials: StorageCredentials | None = Field(default=None, repr=False)

    @model_validator(mode="after")
    def matching_provider(self):
        if self.config.anonymous:
            if self.credentials is not None:
                raise ValueError("anonymous connections must not contain credentials")
        elif self.credentials is None:
            raise ValueError("credentials are required for authenticated connections")
        elif self.config.provider != self.credentials.provider:
            raise ValueError("configuration and credentials must use the same provider")
        return self


class UpdateStorageConnection(_StorageModel):
    name: ConnectionName
    config: StorageConfig
    credentials: StorageCredentials | None = Field(default=None, repr=False)

    @model_validator(mode="after")
    def matching_provider(self):
        if self.config.anonymous and self.credentials is not None:
            raise ValueError("anonymous connections must not contain credentials")
        if self.credentials and self.config.provider != self.credentials.provider:
            raise ValueError("configuration and credentials must use the same provider")
        return self


class StorageConnection(_StorageModel):
    id: UUID
    room_id: UUID
    name: str
    config: StorageConfig
    revision: int
    created_at: datetime
    updated_at: datetime


class ResolvedStorageConnection(_StorageModel):
    id: UUID
    config: StorageConfig
    revision: int
    credentials: StorageCredentials | None = Field(default=None, repr=False)

    def location_and_options(self, path: str) -> tuple[str, dict[str, str]]:
        validate_external_dataset_path(path)
        options: dict[str, str] = {}
        config, credentials = self.config, self.credentials
        if config.anonymous:
            if credentials is not None:
                raise ValueError("anonymous connections must not contain credentials")
            if isinstance(config, GCSStorageConfig):
                root = f"gs://{config.bucket}"
                options["google_skip_signature"] = "true"
            elif isinstance(config, S3StorageConfig):
                root = f"s3://{config.bucket}"
                options.update(aws_region=config.region, aws_skip_signature="true")
            else:
                root = f"az://{config.container}"
                options.update(
                    azure_storage_account_name=config.account,
                    azure_skip_signature="true",
                )
        elif isinstance(config, GCSStorageConfig) and isinstance(
            credentials, GCSStorageCredentials
        ):
            root = f"gs://{config.bucket}"
            options["service_account_key"] = json.dumps(
                credentials.service_account_json
            )
        elif isinstance(config, S3StorageConfig) and isinstance(
            credentials, S3StorageCredentials
        ):
            root = f"s3://{config.bucket}"
            options.update(
                aws_region=config.region,
                aws_access_key_id=credentials.access_key_id,
                aws_secret_access_key=credentials.secret_access_key,
            )
            if credentials.session_token is not None:
                options["aws_session_token"] = credentials.session_token
        elif isinstance(config, AzureStorageConfig) and isinstance(
            credentials, AzureStorageCredentials
        ):
            root = f"az://{config.container}"
            options["azure_storage_account_name"] = config.account
            if credentials.account_key is not None:
                options["azure_storage_account_key"] = credentials.account_key
            elif credentials.sas_token is not None:
                options["azure_storage_sas_key"] = credentials.sas_token
        else:
            raise ValueError("storage connection credential provider mismatch")
        return "/".join(part for part in (root, config.prefix, path) if part), options


def validate_external_dataset_path(path: str) -> str:
    if not path:
        raise ValueError("external dataset path must not be empty")
    return _Location.validate_prefix(path)


class StorageConnectionsResponse(_StorageModel):
    connections: list[StorageConnection]
