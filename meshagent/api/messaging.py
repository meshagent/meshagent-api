import json
from abc import ABC, abstractmethod
from typing import Any, Literal

from opentelemetry.propagate import extract, inject


def split_message_payload(data: bytes) -> bytes:
    header_size = int.from_bytes(data[0:8], "big")
    return data[8 + header_size :]


def split_message_header(data: bytes) -> str:
    header_size = int.from_bytes(data[0:8], "big")
    return data[8 : 8 + header_size].decode("utf-8")


def unpack_message(data: bytes) -> tuple[dict, bytes]:
    header: dict = json.loads(split_message_header(data=data))
    payload = split_message_payload(data=data)

    meshagent_data: dict = header.get("__meshagent__")
    if meshagent_data is not None:
        del header["__meshagent__"]
        otel = meshagent_data.get("otel")
        if otel is not None:
            extract(otel)

    return header, payload


def pack_message(header: dict, data: bytes | None = None) -> bytes:
    otel: dict[str, Any] = {}
    inject(otel)

    extra = {"__meshagent__": {"v": 1, "otel": otel}}
    json_message = json.dumps({**header, **extra}, default=str).encode("utf-8")

    message = bytearray()
    message.extend(len(json_message).to_bytes(8))
    message.extend(json_message)
    if data is not None:
        message.extend(data)
    return bytes(message)


class Content(ABC):
    def get_data(self) -> bytes | None:
        return None

    @abstractmethod
    def to_json(self) -> dict:
        pass

    @abstractmethod
    def pack(self) -> bytes:
        pass


content_types: dict[str, type[Content]] = {}


class LinkContent(Content):
    def __init__(
        self,
        *,
        url: str,
        name: str,
    ):
        self.name = name
        self.url = url

    def to_json(self) -> dict:
        return {"type": "link", "name": self.name, "url": self.url}

    @staticmethod
    def unpack(*, header: dict, payload: bytes) -> "LinkContent":
        del payload
        return LinkContent(name=header["name"], url=header["url"])

    def pack(self) -> bytes:
        return pack_message(header=self.to_json())

    def __str__(self) -> str:
        return f"Link: name={self.name}, type={self.url}"


content_types["link"] = LinkContent


class FileContent(Content):
    def __init__(
        self,
        *,
        data: bytes,
        name: str,
        mime_type: str,
    ):
        self.data = data
        self.name = name
        self.mime_type = mime_type

    def to_json(self) -> dict:
        return {
            "type": "file",
            "name": self.name,
            "mime_type": self.mime_type,
        }

    @staticmethod
    def unpack(*, header: dict, payload: bytes) -> "FileContent":
        return FileContent(
            data=payload,
            name=header["name"],
            mime_type=header["mime_type"],
        )

    def get_data(self) -> bytes:
        return self.data

    def pack(self) -> bytes:
        return pack_message(header=self.to_json(), data=self.data)

    def __str__(self) -> str:
        return f"File: name={self.name}, type={self.mime_type}, length={len(self.data)}"


content_types["file"] = FileContent


class TextContent(Content):
    def __init__(
        self,
        *,
        text: str,
    ):
        self.text = text

    @staticmethod
    def unpack(*, header: dict, payload: bytes) -> "TextContent":
        del payload
        return TextContent(text=header["text"])

    def to_json(self) -> dict:
        return {"type": "text", "text": self.text}

    def pack(self) -> bytes:
        return pack_message(header=self.to_json())

    def __str__(self) -> str:
        return f"Text: text={self.text}"


content_types["text"] = TextContent


class EmptyContent(Content):
    def to_json(self) -> dict:
        return {"type": "empty"}

    @staticmethod
    def unpack(*, header: dict, payload: bytes) -> "EmptyContent":
        del payload
        return EmptyContent()

    def pack(self) -> bytes:
        return pack_message(header=self.to_json())

    def __str__(self) -> str:
        return "Empty"


content_types["empty"] = EmptyContent


class _ControlContent(Content):
    def __init__(
        self,
        *,
        method: Literal["open", "close"],
    ):
        self.method = method

    def to_json(self) -> dict:
        return {"type": "control", "method": self.method}

    @staticmethod
    def unpack(*, header: dict, payload: bytes) -> "_ControlContent":
        del payload
        return _ControlContent(method=header["method"])

    def pack(self) -> bytes:
        return pack_message(header=self.to_json())

    def __str__(self) -> str:
        return f"Control: method={self.method}"


content_types["control"] = _ControlContent


class ErrorContent(Content):
    def __init__(
        self,
        *,
        text: str,
    ):
        self.text = text

    def to_json(self) -> dict:
        return {"type": "error", "text": self.text}

    @staticmethod
    def unpack(*, header: dict, payload: bytes) -> "ErrorContent":
        del payload
        return ErrorContent(text=header["text"])

    def pack(self) -> bytes:
        return pack_message(header=self.to_json())

    def __str__(self) -> str:
        return f"Error: text={self.text}"


content_types["error"] = ErrorContent


class RawOutputsContent(Content):
    def __init__(
        self,
        *,
        outputs: list[dict],
    ):
        self.outputs = outputs

    def to_json(self) -> dict:
        return {"type": "raw", "outputs": self.outputs}

    @staticmethod
    def unpack(*, header: dict, payload: bytes) -> "RawOutputsContent":
        del payload
        return RawOutputsContent(outputs=header["outputs"])

    def pack(self) -> bytes:
        return pack_message(header=self.to_json())

    def __str__(self) -> str:
        return f"RawOutputsContent: outputs={json.dumps(self.outputs)}"


content_types["raw"] = RawOutputsContent


class JsonContent(Content):
    def __init__(
        self,
        *,
        json: dict,
    ):
        self.json = json

    def __getitem__(self, name: str) -> Any:
        return self.json[name]

    def to_json(self) -> dict:
        return {"type": "json", "json": self.json}

    @staticmethod
    def unpack(*, header: dict, payload: bytes) -> "JsonContent":
        del payload
        return JsonContent(json=header["json"])

    def pack(self) -> bytes:
        return pack_message(header=self.to_json())

    def __str__(self) -> str:
        return f"Json: json={json.dumps(self.json)}"


content_types["json"] = JsonContent


def pack_request_parts(content: Content) -> tuple[dict, bytes | None]:
    return content.to_json(), content.get_data()


def pack_content(content: Content) -> bytes:
    header, payload = pack_request_parts(content)
    return pack_message(header=header, data=payload)


def unpack_content_parts(header: dict, payload: bytes) -> Content:
    content_type = header.get("type")
    if not isinstance(content_type, str):
        raise Exception("content header is missing required 'type'")

    parser = content_types.get(content_type)
    if parser is None:
        raise Exception(f"unsupported content type: {content_type}")

    return parser.unpack(header=header, payload=payload)


def unpack_content(data: bytes) -> Content:
    header, payload = unpack_message(data)
    return unpack_content_parts(header=header, payload=payload)


def ensure_content(response: Any) -> Content:
    if isinstance(response, Content):
        return response
    if isinstance(response, dict):
        return JsonContent(json=response)
    if isinstance(response, str):
        return TextContent(text=response)
    if response is None:
        return EmptyContent()
    raise Exception(f"Invalid return type from request handler {type(response)}")
