from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from meshagent.api.room_server_client import (
    BinaryDataType,
    BoolDataType,
    DataTypeUnion,
    DateDataType,
    FloatDataType,
    IntDataType,
    TextDataType,
    TimestampDataType,
    VectorDataType,
)

TABLE_SCHEMA_GRAMMAR = """
TABLE_SCHEMA  := COLUMN_DEF ("," COLUMN_DEF)*
COLUMN_DEF    := IDENTIFIER TYPE_SPEC NULLABILITY?
TYPE_SPEC     := SIMPLE_TYPE | VECTOR_TYPE
SIMPLE_TYPE   := "int" | "bool" | "date" | "timestamp" | "float" | "text" | "binary"
VECTOR_TYPE   := "vector" "(" INT ("," TYPE_SPEC)? ")"
NULLABILITY   := "null" | "not" "null"
IDENTIFIER    := /[A-Za-z_][A-Za-z0-9_]*/
INT           := /[0-9]+/
""".strip()

ALLOWED_DATA_TYPES = (
    "int",
    "bool",
    "date",
    "timestamp",
    "float",
    "text",
    "binary",
    "vector",
)


@dataclass(frozen=True)
class _Token:
    kind: str
    value: str
    position: int


class SchemaParseError(ValueError):
    pass


def parse_table_schema(source: str) -> dict[str, DataTypeUnion]:
    tokens = _tokenize(source)
    parser = _SchemaParser(tokens)
    schema = parser.parse_schema()
    parser.ensure_end()
    return schema


class _SchemaParser:
    def __init__(self, tokens: Iterable[_Token]):
        self._tokens = list(tokens)
        self._index = 0

    def parse_schema(self) -> dict[str, DataTypeUnion]:
        schema: dict[str, DataTypeUnion] = {}
        while True:
            name, data_type = self._parse_column_def()
            if name in schema:
                raise SchemaParseError(f"Duplicate column name: {name}")
            schema[name] = data_type
            if not self._accept("COMMA"):
                break
        return schema

    def parse_type(self) -> DataTypeUnion:
        token = self._peek()
        if token is None or token.kind != "IDENT":
            raise self._error("Expected data type")
        type_name = token.value.casefold()
        if type_name == "vector":
            self._advance()
            return self._parse_vector_type()
        if type_name not in ALLOWED_DATA_TYPES:
            raise self._error(
                f"Unsupported data type '{token.value}'. Allowed: {', '.join(ALLOWED_DATA_TYPES)}"
            )
        self._advance()
        return _simple_type(type_name)

    def parse_schema_type(self) -> DataTypeUnion:
        return self.parse_type()

    def parse_schema_type_with_nullability(self) -> DataTypeUnion:
        data_type = self.parse_schema_type()
        if self._accept("IDENT", "null"):
            data_type.nullable = True
        elif self._accept("IDENT", "not"):
            self._expect("IDENT", "null")
            data_type.nullable = False
        return data_type

    def ensure_end(self) -> None:
        if self._peek() is not None:
            token = self._peek()
            raise SchemaParseError(
                f"Unexpected token '{token.value}' at position {token.position}"
            )

    def _parse_column_def(self) -> tuple[str, DataTypeUnion]:
        name_token = self._expect("IDENT")
        data_type = self.parse_schema_type_with_nullability()
        return name_token.value, data_type

    def _parse_vector_type(self) -> DataTypeUnion:
        self._expect("LPAREN")
        size_token = self._expect("INT")
        size = int(size_token.value)
        if size <= 0:
            raise self._error("Vector size must be a positive integer")
        element_type = FloatDataType()
        if self._accept("COMMA"):
            element_type = self.parse_schema_type()
        self._expect("RPAREN")
        return VectorDataType(size=size, element_type=element_type)

    def _expect(self, kind: str, value: str | None = None) -> _Token:
        token = self._peek()
        if token is None:
            raise self._error("Unexpected end of input")
        if token.kind != kind or (
            value is not None and token.value.casefold() != value.casefold()
        ):
            expected = f"{kind}{' ' + value if value else ''}"
            raise self._error(f"Expected {expected}")
        self._index += 1
        return token

    def _accept(self, kind: str, value: str | None = None) -> bool:
        token = self._peek()
        if token is None:
            return False
        if token.kind != kind:
            return False
        if value is not None and token.value.casefold() != value.casefold():
            return False
        self._index += 1
        return True

    def _peek(self) -> _Token | None:
        if self._index >= len(self._tokens):
            return None
        return self._tokens[self._index]

    def _advance(self) -> _Token:
        token = self._peek()
        if token is None:
            raise self._error("Unexpected end of input")
        self._index += 1
        return token

    def _error(self, message: str) -> SchemaParseError:
        token = self._peek()
        if token is None:
            return SchemaParseError(f"{message} at end of input")
        return SchemaParseError(f"{message} at position {token.position}")


def _tokenize(source: str) -> list[_Token]:
    tokens: list[_Token] = []
    idx = 0
    length = len(source)
    while idx < length:
        char = source[idx]
        if char.isspace():
            idx += 1
            continue
        if char == ",":
            tokens.append(_Token("COMMA", char, idx))
            idx += 1
            continue
        if char == "(":
            tokens.append(_Token("LPAREN", char, idx))
            idx += 1
            continue
        if char == ")":
            tokens.append(_Token("RPAREN", char, idx))
            idx += 1
            continue
        if char.isdigit():
            start = idx
            while idx < length and source[idx].isdigit():
                idx += 1
            tokens.append(_Token("INT", source[start:idx], start))
            continue
        if char.isalpha() or char == "_":
            start = idx
            while idx < length and (source[idx].isalnum() or source[idx] == "_"):
                idx += 1
            tokens.append(_Token("IDENT", source[start:idx], start))
            continue
        raise SchemaParseError(f"Unexpected character '{char}' at position {idx}")
    return tokens


def _simple_type(type_name: str) -> DataTypeUnion:
    if type_name == "int":
        return IntDataType()
    if type_name == "bool":
        return BoolDataType()
    if type_name == "date":
        return DateDataType()
    if type_name == "timestamp":
        return TimestampDataType()
    if type_name == "float":
        return FloatDataType()
    if type_name == "text":
        return TextDataType()
    if type_name == "binary":
        return BinaryDataType()
    raise SchemaParseError(
        f"Unsupported data type '{type_name}'. Allowed: {', '.join(ALLOWED_DATA_TYPES)}"
    )
