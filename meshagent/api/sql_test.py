import pyarrow as pa
import pytest

from meshagent.api.sql import SchemaParseError, parse_table_schema


def test_parse_schema_case_insensitive():
    schema = parse_table_schema("names VeCtOr(20) nUlL, test TeXT NoT NuLL, age INT")

    assert schema.field("names").type == pa.list_(pa.float64(), 20)
    assert schema.field("names").nullable is True
    assert schema.field("test").type == pa.string()
    assert schema.field("test").nullable is False
    assert schema.field("age").type == pa.int64()
    assert schema.field("age").nullable is True


def test_parse_schema_vector_element_type_case_insensitive():
    schema = parse_table_schema("embedding vector(3, FLOAT)")

    assert schema.field("embedding").type == pa.list_(pa.float64(), 3)


def test_parse_schema_uuid_type():
    schema = parse_table_schema("id uuid not null")

    assert schema.field("id").type == pa.uuid()
    assert schema.field("id").nullable is False


def test_parse_schema_json_type():
    schema = parse_table_schema("payload json not null, history list(json)")

    assert schema.field("payload").type == pa.json_()
    assert schema.field("payload").nullable is False
    assert schema.field("history").type == pa.list_(pa.json_())


def test_parse_schema_duplicate_columns():
    with pytest.raises(SchemaParseError, match="Duplicate column name"):
        parse_table_schema("id int, id text")


def test_parse_schema_list_struct_type():
    schema = parse_table_schema(
        "labels list(struct(key text, value text)), weights list(struct(key text, value vector(2)))"
    )

    assert schema.field("labels").type == pa.list_(
        pa.struct(
            [
                pa.field("key", pa.string()),
                pa.field("value", pa.string()),
            ]
        )
    )
    assert schema.field("weights").type == pa.list_(
        pa.struct(
            [
                pa.field("key", pa.string()),
                pa.field("value", pa.list_(pa.float64(), 2)),
            ]
        )
    )
