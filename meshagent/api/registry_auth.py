from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Sequence

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519


RegistryAction = Literal["pull", "push"]
RegistryResourceType = Literal["repository"]

DEFAULT_REGISTRY_HOST = "registry.meshagent.com"
DEFAULT_REGISTRY_USERNAME = "meshagent"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_timestamp(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return int(value.timestamp())


def _coerce_datetime(value: int | float | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return datetime.fromtimestamp(float(value), tz=timezone.utc)


def _sorted_actions(actions: Sequence[RegistryAction]) -> list[RegistryAction]:
    seen: set[RegistryAction] = set()
    normalized: list[RegistryAction] = []
    for action in actions:
        if action in seen:
            continue
        seen.add(action)
        normalized.append(action)
    return normalized


@dataclass(frozen=True, slots=True)
class RegistryAccessGrant:
    name: str
    actions: tuple[RegistryAction, ...]
    resource_type: RegistryResourceType = "repository"

    def __post_init__(self) -> None:
        normalized_name = self.name.strip()
        if normalized_name == "":
            raise ValueError("registry access grant name must not be empty")
        normalized_actions = tuple(_sorted_actions(self.actions))
        if len(normalized_actions) == 0:
            raise ValueError("registry access grant must include at least one action")
        object.__setattr__(self, "name", normalized_name)
        object.__setattr__(self, "actions", normalized_actions)

    def to_jwt_payload(self) -> dict[str, object]:
        return {
            "type": self.resource_type,
            "name": self.name,
            "actions": list(self.actions),
        }


@dataclass(frozen=True, slots=True)
class RegistryRequestedScope:
    resource_type: str
    name: str
    actions: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RegistryTokenClaims:
    subject: str | None
    issued_at: datetime | None
    expires_at: datetime | None
    access: tuple[RegistryAccessGrant, ...]
    raw_claims: dict[str, Any]


def registry_private_key_from_secret(secret: str) -> ed25519.Ed25519PrivateKey:
    seed = hashlib.sha256(secret.encode("utf-8")).digest()
    return ed25519.Ed25519PrivateKey.from_private_bytes(seed)


def registry_public_key_from_secret(secret: str) -> ed25519.Ed25519PublicKey:
    return registry_private_key_from_secret(secret).public_key()


def registry_public_key_pem_from_secret(secret: str) -> str:
    public_key = registry_public_key_from_secret(secret)
    return public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode("utf-8")


def encode_registry_token(
    *,
    secret: str,
    subject: str,
    grants: Sequence[RegistryAccessGrant],
    expires_at: datetime,
    issuer: str = "meshagent",
    not_before: datetime | None = None,
    issued_at: datetime | None = None,
    extra_claims: dict[str, Any] | None = None,
) -> str:
    normalized_issued_at = issued_at or utc_now()
    normalized_not_before = not_before or normalized_issued_at
    payload: dict[str, Any] = {
        "iss": issuer,
        "sub": subject,
        "iat": _utc_timestamp(normalized_issued_at),
        "nbf": _utc_timestamp(normalized_not_before),
        "exp": _utc_timestamp(expires_at),
        "access": [grant.to_jwt_payload() for grant in grants],
    }
    if extra_claims:
        payload.update(extra_claims)
    return jwt.encode(
        payload,
        registry_private_key_from_secret(secret),
        algorithm="EdDSA",
    )


def decode_registry_token(
    *,
    secret: str,
    token: str,
    verify_expiration: bool = True,
) -> RegistryTokenClaims:
    decoded = jwt.decode(
        token,
        registry_public_key_from_secret(secret),
        algorithms=["EdDSA"],
        options={"verify_aud": False, "verify_exp": verify_expiration},
    )
    raw_access = decoded.get("access")
    grants: list[RegistryAccessGrant] = []
    if isinstance(raw_access, list):
        for item in raw_access:
            if not isinstance(item, dict):
                continue
            resource_type = item.get("type")
            name = item.get("name")
            actions = item.get("actions")
            if resource_type != "repository":
                continue
            if not isinstance(name, str):
                continue
            if not isinstance(actions, list) or not all(
                isinstance(action, str) for action in actions
            ):
                continue
            normalized_actions = [
                action for action in actions if action in {"pull", "push"}
            ]
            if len(normalized_actions) == 0:
                continue
            grants.append(
                RegistryAccessGrant(
                    resource_type="repository",
                    name=name,
                    actions=tuple(normalized_actions),
                )
            )
    return RegistryTokenClaims(
        subject=decoded.get("sub") if isinstance(decoded.get("sub"), str) else None,
        issued_at=_coerce_datetime(decoded.get("iat")),
        expires_at=_coerce_datetime(decoded.get("exp")),
        access=tuple(grants),
        raw_claims=decoded,
    )


def parse_registry_scope(scope: str) -> RegistryRequestedScope:
    resource_type, separator, remainder = scope.partition(":")
    if separator == "":
        raise ValueError("registry scope must include a resource type")
    name, separator, action_list = remainder.partition(":")
    if separator == "":
        raise ValueError("registry scope must include actions")
    normalized_type = resource_type.strip()
    if normalized_type == "":
        raise ValueError("registry scope resource type must not be empty")
    normalized_name = name.strip()
    normalized_actions = tuple(
        action.strip() for action in action_list.split(",") if action.strip() != ""
    )
    if len(normalized_actions) == 0:
        raise ValueError("registry scope actions must not be empty")
    return RegistryRequestedScope(
        resource_type=normalized_type,
        name=normalized_name,
        actions=normalized_actions,
    )


def parse_registry_scopes(scopes: Sequence[str]) -> tuple[RegistryRequestedScope, ...]:
    parsed: list[RegistryRequestedScope] = []
    for scope in scopes:
        normalized_scope = scope.strip()
        if normalized_scope == "":
            continue
        parsed.append(parse_registry_scope(normalized_scope))
    return tuple(parsed)


def repository_pattern_matches(*, pattern: str, repository: str) -> bool:
    normalized_pattern = pattern.strip()
    normalized_repository = repository.strip()
    if normalized_pattern == "*" or normalized_pattern == normalized_repository:
        return True
    if normalized_pattern.endswith("/*"):
        prefix = normalized_pattern.removesuffix("/*")
        if prefix == "":
            return True
        return normalized_repository.startswith(f"{prefix}/")
    return False


def scope_is_allowed(
    *,
    claims: RegistryTokenClaims,
    requested_scope: RegistryRequestedScope,
) -> bool:
    if requested_scope.resource_type != "repository":
        return False
    if requested_scope.name == "":
        return True
    for grant in claims.access:
        if grant.resource_type != "repository":
            continue
        if not repository_pattern_matches(
            pattern=grant.name,
            repository=requested_scope.name,
        ):
            continue
        if all(action in grant.actions for action in requested_scope.actions):
            return True
    return False


def all_scopes_allowed(
    *,
    claims: RegistryTokenClaims,
    requested_scopes: Sequence[RegistryRequestedScope],
) -> bool:
    return all(
        scope_is_allowed(claims=claims, requested_scope=requested_scope)
        for requested_scope in requested_scopes
    )


def encode_basic_registry_credentials(*, username: str, token: str) -> str:
    encoded = base64.b64encode(f"{username}:{token}".encode("utf-8")).decode("ascii")
    return f"Basic {encoded}"


def build_project_pull_grant(*, project_key: str) -> RegistryAccessGrant:
    normalized_project_key = project_key.strip()
    if normalized_project_key == "":
        raise ValueError("project_key must not be empty")
    return RegistryAccessGrant(
        name=f"{normalized_project_key}/*",
        actions=("pull",),
    )


def build_repository_push_grant(
    *,
    project_key: str,
    repository_name: str,
) -> RegistryAccessGrant:
    normalized_project_key = project_key.strip()
    normalized_repository_name = repository_name.strip().strip("/")
    if normalized_project_key == "":
        raise ValueError("project_key must not be empty")
    if normalized_repository_name == "":
        raise ValueError("repository_name must not be empty")
    return RegistryAccessGrant(
        name=f"{normalized_project_key}/{normalized_repository_name}",
        actions=("pull", "push"),
    )


def token_expires_in_seconds(
    *,
    claims: RegistryTokenClaims,
    now: datetime | None = None,
) -> int | None:
    if claims.expires_at is None:
        return None
    normalized_now = now or utc_now()
    remaining = int((claims.expires_at - normalized_now).total_seconds())
    return max(remaining, 0)


def bounded_expiration(
    *,
    requested_seconds: int | None,
    default_seconds: int,
    maximum_seconds: int,
    now: datetime | None = None,
) -> datetime:
    normalized_now = now or utc_now()
    ttl_seconds = default_seconds if requested_seconds is None else requested_seconds
    ttl_seconds = max(1, min(ttl_seconds, maximum_seconds))
    return normalized_now + timedelta(seconds=ttl_seconds)


def json_dumps_registry_token_response(
    *,
    token: str,
    claims: RegistryTokenClaims,
) -> str:
    payload: dict[str, Any] = {
        "token": token,
        "access_token": token,
    }
    expires_in = token_expires_in_seconds(claims=claims)
    if expires_in is not None:
        payload["expires_in"] = expires_in
    if claims.issued_at is not None:
        payload["issued_at"] = claims.issued_at.isoformat().replace("+00:00", "Z")
    return json.dumps(payload, sort_keys=True)
