import json
from abc import abstractmethod, ABC

from typing import Optional, Any, Dict, Literal

from opentelemetry.propagate import extract, inject


def split_message_payload(data: bytes):
    header_size = int.from_bytes(data[0:8], "big")
    payload = data[8 + header_size :]
    return payload


def split_message_header(data: bytes):
    header_size = int.from_bytes(data[0:8], "big")
    header_str = data[8 : 8 + header_size].decode("utf-8")
    return header_str


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
    otel = {}
    inject(otel)

    extra = {"__meshagent__": {"v": 1, "otel": otel}}

    json_message = json.dumps({**header, **extra}, default=str).encode("utf-8")

    message = bytearray()
    message.extend(len(json_message).to_bytes(8))
    message.extend(json_message)
    if data is not None:
        message.extend(data)
    return message


class Chunk(ABC):
    def __init__(
        self,
        *,
        usage: Optional[dict[str, float]] = None,
        caller_context: Optional[Dict[str, Any]] = None,
    ):
        self.usage = usage
        self.caller_context = caller_context

    def get_data(self) -> bytes | None:
        return None

    @abstractmethod
    def to_json(self) -> dict:
        pass

    @abstractmethod
    def pack(self) -> bytes:
        pass


Request = Chunk

chunk_types = dict[str, type[Chunk]]()


class LinkChunk(Chunk):
    def __init__(
        self,
        *,
        url: str,
        name: str,
        usage: Optional[dict[str, float]] = None,
        caller_context: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(usage=usage, caller_context=caller_context)
        self.name = name
        self.url = url

    def to_json(self):
        return {"type": "link", "name": self.name, "url": self.url, "usage": self.usage}

    @staticmethod
    def unpack(*, header: dict, payload: bytes):
        return LinkChunk(
            name=header["name"], url=header["url"], usage=header.get("usage", None)
        )

    def pack(self):
        return pack_message(header=self.to_json())

    def __str__(self):
        return f"Link: name={self.name}, type={self.url} usage={self.usage}"


chunk_types["link"] = LinkChunk


class FileChunk(Chunk):
    def __init__(
        self,
        *,
        data: bytes,
        name: str,
        mime_type: str,
        usage: Optional[dict[str, float]] = None,
        caller_context: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(usage=usage, caller_context=caller_context)
        self.data = data
        self.name = name
        self.mime_type = mime_type

    def to_json(self):
        return {
            "type": "file",
            "name": self.name,
            "mime_type": self.mime_type,
            "usage": self.usage,
        }

    @staticmethod
    def unpack(*, header: dict, payload: bytes):
        return FileChunk(
            data=payload,
            name=header["name"],
            mime_type=header["mime_type"],
            usage=header.get("usage", None),
        )

    def get_data(self) -> bytes:
        return self.data

    def pack(self):
        return pack_message(header=self.to_json(), data=self.data)

    def __str__(self):
        return f"File: name={self.name}, type={self.mime_type}, length={len(self.data)} usage={self.usage}"


chunk_types["file"] = FileChunk


class TextChunk(Chunk):
    def __init__(
        self,
        *,
        text: str,
        usage: Optional[dict[str, float]] = None,
        caller_context: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(usage=usage, caller_context=caller_context)
        self.text = text

    @staticmethod
    def unpack(*, header: dict, payload: bytes):
        return TextChunk(text=header["text"], usage=header.get("usage", None))

    def to_json(self):
        return {"type": "text", "text": self.text, "usage": self.usage}

    def pack(self):
        return pack_message(header=self.to_json())

    def __str__(self):
        return f"Text: text={self.text} usage={self.usage}"


chunk_types["text"] = TextChunk


class EmptyChunk(Chunk):
    def __init__(
        self,
        *,
        usage: Optional[dict[str, float]] = None,
        caller_context: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(usage=usage, caller_context=caller_context)

    def to_json(self):
        return {"type": "empty", "usage": self.usage}

    @staticmethod
    def unpack(*, header: dict, payload: bytes):
        return EmptyChunk(usage=header.get("usage", None))

    def pack(self):
        return pack_message(header=self.to_json())

    def __str__(self):
        return f"Empty: usage={self.usage}"


chunk_types["empty"] = EmptyChunk


class _ControlChunk(Chunk):
    def __init__(
        self,
        *,
        method: Literal["open", "close"],
        usage: Optional[dict[str, float]] = None,
        caller_context: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(usage=usage, caller_context=caller_context)
        self.method = method

    def to_json(self):
        return {
            "type": "control",
            "method": self.method,
            "usage": self.usage,
        }

    @staticmethod
    def unpack(*, header: dict, payload: bytes):
        return _ControlChunk(
            method=header["method"],
            usage=header.get("usage", None),
        )

    def pack(self):
        return pack_message(header=self.to_json())

    def __str__(self):
        return f"Control: method={self.method} usage={self.usage}"


chunk_types["control"] = _ControlChunk


class ErrorChunk(Chunk):
    def __init__(
        self,
        *,
        text: str,
        usage: Optional[dict[str, float]] = None,
        caller_context: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(usage=usage, caller_context=caller_context)
        self.text = text

    def to_json(self):
        return {
            "type": "error",
            "text": self.text,
            "usage": self.usage,
        }

    @staticmethod
    def unpack(*, header: dict, payload: bytes):
        return ErrorChunk(text=header["text"], usage=header.get("usage", None))

    def pack(self):
        return pack_message(header=self.to_json())

    def __str__(self):
        return f"Error: text={self.text} usage={self.usage}"


chunk_types["error"] = ErrorChunk


class RawOutputsChunk(Chunk):
    def __init__(
        self,
        *,
        outputs: list[dict],
        usage: Optional[dict[str, float]] = None,
        caller_context: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(usage=usage, caller_context=caller_context)
        self.outputs = outputs

    def to_json(self):
        return {"type": "raw", "outputs": self.outputs, "usage": self.usage}

    @staticmethod
    def unpack(*, header: dict, payload: bytes):
        return RawOutputsChunk(
            outputs=header["outputs"], usage=header.get("usage", None)
        )

    def pack(self):
        return pack_message(header=self.to_json())

    def __str__(self):
        return f"RawOutputsChunk: outputs={json.dumps(self.outputs)} usage={self.usage}"


chunk_types["raw"] = RawOutputsChunk


class JsonChunk(Chunk):
    def __init__(
        self,
        *,
        json: dict,
        usage: Optional[dict[str, float]] = None,
        caller_context: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(usage=usage, caller_context=caller_context)
        self.json = json

    def __getitem__(self, name: str):
        return self.json[name]

    def to_json(self):
        return {"type": "json", "json": self.json, "usage": self.usage}

    @staticmethod
    def unpack(*, header: dict, payload: bytes):
        return JsonChunk(json=header["json"], usage=header.get("usage", None))

    def pack(self):
        return pack_message(header=self.to_json())

    def __str__(self):
        return f"Json: json={json.dumps(self.json)} usage={self.usage}"


chunk_types["json"] = JsonChunk


def unpack_request_parts(header: dict, payload: bytes) -> Request:
    T = chunk_types[header["type"]]
    return T.unpack(header=header, payload=payload)


def unpack_request(data: bytes) -> Request:
    header, payload = unpack_message(data)
    return unpack_request_parts(header=header, payload=payload)


def unpack_response_parts(header: dict, payload: bytes) -> Chunk:
    T = chunk_types[header["type"]]
    return T.unpack(header=header, payload=payload)


def unpack_response(data: bytes) -> Chunk:
    header, payload = unpack_message(data)
    return unpack_response_parts(header=header, payload=payload)


def ensure_response(response) -> Chunk:
    if isinstance(response, Chunk):
        return response
    elif isinstance(response, dict):
        return JsonChunk(json=response)
    elif isinstance(response, str):
        return TextChunk(text=response)
    elif response is None:
        return EmptyChunk()
    else:
        raise Exception(f"Invalid return type from request handler {type(response)}")
