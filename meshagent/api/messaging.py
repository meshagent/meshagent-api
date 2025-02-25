import json
from abc import abstractmethod, ABC

from typing import Optional


def split_message_payload(data:bytes):
    header_size = int.from_bytes(data[0:8], "big")
    payload = data[8+header_size:]
    return payload

def split_message_header(data:bytes):
    header_size = int.from_bytes(data[0:8], "big")
    header_str = data[8:8+header_size].decode("utf-8")
    return header_str

def pack_message(*, header: dict, data:bytes|None = None) -> bytes:
    json_message = json.dumps(header).encode("utf-8")
    message = bytearray()
    message.extend(len(json_message).to_bytes(8))
    message.extend(json_message)
    if data != None:
        message.extend(data)
    return message


class Response(ABC):
    def __init__(self):
        pass
    
    @abstractmethod
    def to_json(self):
        pass

    @abstractmethod
    def pack(self) -> bytes:
        pass


response_types = dict[str, type]()

class LinkResponse(Response):
    def __init__(self, *, url: str, name: str):
        super().__init__()
        self.name = name
        self.url = url

    def to_json(self):
        return {
            "type" : "link",
            "name" : self.name,
            "url" : self.url
        }

    @staticmethod
    def unpack(*, header: dict, payload: bytes):
        return LinkResponse(name=header["name"], url=header["url"])

    def pack(self):
        return pack_message(header=self.to_json())
    
    def __str__(self):
        return f"LinkResponse name={self.name}, type={self.url}"
    
response_types["link"] = LinkResponse

class FileResponse(Response):
    def __init__(self, *, data: bytes, name: str, mime_type: str):
        super().__init__()
        self.data = data
        self.name = name
        self.mime_type = mime_type

    def to_json(self):
        return {
            "type" : "file",
            "name" : self.name,
            "mime_type" : self.mime_type
        }

    @staticmethod
    def unpack(*, header: dict, payload: bytes):
        return FileResponse(data=payload, name=header["name"], mime_type=header["mime_type"])   
    
    def pack(self):
        return pack_message(header=self.to_json(), data=self.data)
    
    def __str__(self):
        return f"FileResponse name={self.name}, type={self.mime_type}, length={len(self.data)}"

response_types["file"] = FileResponse

class TextResponse(Response):
    def __init__(self, *, text: str):
        super().__init__()
        self.text = text

    @staticmethod
    def unpack(*, header: dict, payload: bytes):
        return TextResponse(text=header["text"])

    def to_json(self):
        return {
            "type" : "text",
            "text" : self.text,
        }

    def pack(self):
        return pack_message(header=self.to_json())

    def __str__(self):
        return f"TextResponse {self.text}"

response_types["text"] = TextResponse

class EmptyResponse(Response):
    def __init__(self):
        super().__init__()

    
    def to_json(self):
        return {
            "type" : "empty",
        }
        

    @staticmethod
    def unpack(*, header: dict, payload: bytes):
        return EmptyResponse()

    def pack(self):
        return pack_message(header=self.to_json())
    
    def __str__(self):
        return f"EmptyResponse"

response_types["empty"] = EmptyResponse

class ErrorResponse(Response):
    def __init__(self, *, text: str):
        super().__init__()
        self.text = text

    def to_json(self):
        return {
            "type" : "error",
            "text" : self.text,
        }

    @staticmethod
    def unpack(*, header: dict, payload: bytes):
        return ErrorResponse(text=header["text"])

    def pack(self):
        return pack_message(header=self.to_json())
    
    def __str__(self):
        return f"ErrorResponse: {self.text}"
    

response_types["error"] = ErrorResponse

class JsonResponse(Response):
    def __init__(self, json: dict):
        super().__init__()
        self.json = json

    def __getitem__(self, name: str):
        return self.json[name]
    
    def to_json(self):
        return {
            "type" : "json",
            "json" : self.json,
        }
    
    @staticmethod
    def unpack(*, header: dict, payload: bytes):
        return JsonResponse(header["json"])

    def pack(self):
        return pack_message(header=self.to_json())
    
    def __str__(self):
        return f"JsonResponse: {json.dumps(self.json)}"


response_types["json"] = JsonResponse

def unpack_response(data: bytes) -> Response:
    header = json.loads(split_message_header(data=data))
    payload = split_message_payload(data=data)

    T = response_types[header["type"]]
    return T.unpack(header=header, payload=payload)


def ensure_response(response) -> Response:
    if isinstance(response, Response):
        return response
    elif isinstance(response, dict):
        return JsonResponse(json=response)
    elif isinstance(response, str):
        return TextResponse(text=response)
    elif response == None:
        return EmptyResponse()
    else:
        raise Exception(f"Invalid return type from request handler {type(response)}")