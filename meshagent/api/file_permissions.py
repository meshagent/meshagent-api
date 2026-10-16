"""Transport-independent file permissions used by REST and participant tokens."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


def validate_file_path(value: str) -> str:
    if (
        value.startswith("/")
        or any(c == "\\" or ord(c) < 32 or 127 <= ord(c) < 160 for c in value)
        or (
            value
            and any(
                part in {"", ".", "..", ".meshagent-directory"}
                or part.startswith(".meshagent-upload-")
                for part in value.split("/")
            )
        )
    ):
        raise ValueError("Files paths must be canonical relative paths")
    return value


FileAction = Literal["list", "read", "write", "delete", "presign"]
FileRole = Literal[
    "files_list", "files_read", "files_write", "files_delete", "files_presign"
]
FILE_ACTIONS: tuple[FileAction, ...] = ("list", "read", "write", "delete", "presign")
FILE_RELATIONS: tuple[str, ...] = tuple(f"files_{action}" for action in FILE_ACTIONS)


class FilePathRule(BaseModel):
    """Segment-bounded file actions, with explicit excluded subtrees."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    path: str
    recursive: bool
    actions: list[FileAction]
    excluded_paths: list[str] = Field(default_factory=list, max_length=1000)

    @field_validator("path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        return validate_file_path(value)

    @field_validator("excluded_paths")
    @classmethod
    def validate_excluded_paths(cls, values: list[str]) -> list[str]:
        return [validate_file_path(value) for value in values]

    def allows(self, action: FileAction, path: str, *, subtree: bool = False) -> bool:
        validate_file_path(path)
        if action not in self.actions or (subtree and not self.recursive):
            return False
        if path != self.path and not (self.recursive and _descendant(self.path, path)):
            return False
        return not any(
            path == excluded
            or _descendant(excluded, path)
            or (subtree and _descendant(path, excluded))
            for excluded in self.excluded_paths
        )


def _descendant(parent: str, path: str) -> bool:
    return not parent or path.startswith(parent + "/")


class FilesPolicy(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    paths: list[FilePathRule] = Field(default_factory=list, max_length=1000)

    def allows(self, action: FileAction, path: str, *, subtree: bool = False) -> bool:
        validate_file_path(path)
        return any(rule.allows(action, path, subtree=subtree) for rule in self.paths)

    def can_list_directory(self, path: str) -> bool:
        validate_file_path(path)
        return self.allows("list", path) or any(
            rule.allows("list", rule.path) and _descendant(path, rule.path)
            for rule in self.paths
        )


class FilePermissions(BaseModel):
    actions: list[FileAction]
    recursive_actions: list[FileAction]


_STORAGE_READ_ACTIONS: tuple[FileAction, ...] = ("list", "read")
_STORAGE_WRITE_ACTIONS: tuple[FileAction, ...] = ("list", "read", "write", "delete")


def legacy_storage_rules(
    paths: Iterable[tuple[str, bool]] | None,
) -> list[FilePathRule]:
    """Convert ordered storage prefixes without granting URL delegation."""
    if paths is None:
        return [
            FilePathRule(path="", recursive=True, actions=list(_STORAGE_WRITE_ACTIONS))
        ]
    previous: list[str] = []
    result: list[FilePathRule] = []
    for raw_path, read_only in paths:
        path = raw_path.strip("/")
        result.append(
            FilePathRule(
                path=path,
                recursive=True,
                actions=list(
                    _STORAGE_READ_ACTIONS if read_only else _STORAGE_WRITE_ACTIONS
                ),
                excluded_paths=list(previous),
            )
        )
        previous.append(path)
    return result


def storage_request_path(path: str) -> str:
    """Retain legacy leading-slash/dot aliases while rejecting unsafe keys."""
    while path.startswith("./"):
        path = path[2:]
    path = path.strip("/")
    if path == ".":
        path = ""
    return validate_file_path(path)
