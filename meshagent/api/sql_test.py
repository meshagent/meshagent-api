import pytest

from meshagent.api.room_server_client import (
    FloatDataType,
    IntDataType,
    ListDataType,
    StructDataType,
    TextDataType,
    VectorDataType,
)
from meshagent.api.sql import SchemaParseError, parse_table_schema


def test_parse_schema_case_insensitive():
    schema = parse_table_schema("names VeCtOr(20) nUlL, test TeXT NoT NuLL, age INT")

    assert isinstance(schema["names"], VectorDataType)
    assert schema["names"].size == 20
    assert schema["names"].nullable is True
    assert isinstance(schema["names"].element_type, FloatDataType)

    assert isinstance(schema["test"], TextDataType)
    assert schema["test"].nullable is False

    assert isinstance(schema["age"], IntDataType)
    assert schema["age"].nullable is None


def test_parse_schema_vector_element_type_case_insensitive():
    schema = parse_table_schema("embedding vector(3, FLOAT)")
    column = schema["embedding"]

    assert isinstance(column, VectorDataType)
    assert column.size == 3
    assert isinstance(column.element_type, FloatDataType)


def test_parse_schema_duplicate_columns():
    with pytest.raises(SchemaParseError, match="Duplicate column name"):
        parse_table_schema("id int, id text")


def test_parse_schema_list_struct_type():
    schema = parse_table_schema(
        "labels list(struct(key text, value text)), weights list(struct(key text, value vector(2)))"
    )

    labels = schema["labels"]
    assert isinstance(labels, ListDataType)
    assert isinstance(labels.element_type, StructDataType)
    assert isinstance(labels.element_type.fields["key"], TextDataType)
    assert isinstance(labels.element_type.fields["value"], TextDataType)

    weights = schema["weights"]
    assert isinstance(weights, ListDataType)
    assert isinstance(weights.element_type, StructDataType)
    assert isinstance(weights.element_type.fields["key"], TextDataType)
    assert isinstance(weights.element_type.fields["value"], VectorDataType)
