import uuid
import base64


def base36encode(number: int):
    if not isinstance(number, int):
        raise TypeError("number must be an integer")
    if number < 0:
        raise ValueError("number must be non-negative")

    alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
    if number == 0:
        return "0"
    base36 = ""
    while number:
        number, i = divmod(number, 36)
        base36 = alphabet[i] + base36
    return base36


def compress_uuid(guid_string: str):
    guid_int = int(guid_string.replace("-", ""), 16)
    return base36encode(guid_int)


def base36decode(number_str: str) -> int:
    """Decode a base36-encoded string into an integer."""
    alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
    base = 36
    number = 0
    for char in number_str:
        try:
            value = alphabet.index(char)
        except ValueError:
            raise ValueError(f"Invalid character '{char}' for base36 encoding")
        number = number * base + value
    return number


def decompress_uuid(compressed_uuid: str) -> str:
    """
    Reverse the compressed UUID to its standard 36-character UUID format.

    Args:
        compressed_uuid: A base36 string that represents a UUID compressed from its standard form.

    Returns:
        A string in the UUID format (8-4-4-4-12 hexadecimal characters).
    """
    # Decode the base36 string back to the original integer.
    guid_int = base36decode(compressed_uuid)

    # Convert the integer into a 32-digit hexadecimal string with leading zeros.
    hex_str = f"{guid_int:032x}"

    # Reinsert dashes to match the standard UUID format: 8-4-4-4-12.
    standard_uuid = f"{hex_str[0:8]}-{hex_str[8:12]}-{hex_str[12:16]}-{hex_str[16:20]}-{hex_str[20:32]}"
    return standard_uuid


def base64_compress_uuid(id: str):
    return (
        base64.urlsafe_b64encode(uuid.UUID(hex=id).bytes)
        .decode()
        .replace("-", ".")
        .rstrip("=")
    )


def base64_decompress_uuid(id: str):
    padding_needed = len(id) % 4
    if padding_needed:
        id += "=" * (4 - padding_needed)
    return str(uuid.UUID(bytes=base64.urlsafe_b64decode(id.replace(".", "-"))))


class ApiKey:
    def __init__(self, *, id: str, project_id: str, secret: str):
        self.id = id
        self.project_id = project_id
        self.secret = secret

    id: str
    project_id: str
    secret: str


def parse_api_key(key: str) -> ApiKey:
    if key.startswith("ma-"):
        parts = key.removeprefix("ma-").split("-", 2)

        kid = base64_decompress_uuid(parts[0])
        project_id = base64_decompress_uuid(parts[1])
        secret = parts[2]

        return ApiKey(id=kid, project_id=project_id, secret=secret)

    raise ValueError("invalid api key")


def encode_api_key(key: ApiKey) -> str:
    return (
        "ma-"
        + base64_compress_uuid(key.id)
        + "-"
        + base64_compress_uuid(key.project_id)
        + "-"
        + key.secret
    )
