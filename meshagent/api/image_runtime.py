from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

IMAGE_RUNTIME_LABEL = "meshagent.runtime"
IMAGE_RUNTIME_MOUNT_PATH = "/app"
IMAGE_RUNTIME_MOUNT_SUBPATH = "app"

ImageRuntimeName = Literal["node", "python"]


@dataclass(frozen=True)
class ImageRuntimeDefinition:
    name: ImageRuntimeName
    base_image: str
    launcher: tuple[str, ...]


IMAGE_RUNTIME_BASES: dict[ImageRuntimeName, ImageRuntimeDefinition] = {
    "node": ImageRuntimeDefinition(
        name="node",
        base_image="meshagent/node:default",
        launcher=("node",),
    ),
    "python": ImageRuntimeDefinition(
        name="python",
        base_image="meshagent/python:default",
        launcher=("python",),
    ),
}
