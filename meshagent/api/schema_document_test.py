import json
import logging
from jsonschema import validate

from meshagent.api.schema import MeshSchema, ElementType, ChildProperty, ValueProperty
from meshagent.api.runtime import DocumentRuntime

logger = logging.getLogger(__name__)

schema = MeshSchema(
    root_tag_name="root",
    elements=[
        ElementType(
            tag_name="root",
            description="",
            properties=[
                ChildProperty(
                    name="children", description="", child_tag_names=["child", "child2"]
                ),
            ],
        ),
        ElementType(
            tag_name="child",
            description="",
            properties=[
                ValueProperty(name="attr", description="", type="string"),
            ],
        ),
        ElementType(
            tag_name="child2",
            description="",
            properties=[
                ValueProperty(name="attr", description="", type="string"),
            ],
        ),
    ],
)

expected = {"root": {"children": [{"child": {"attr": "test2"}}]}}


def test_document_to_json_from_json_produces_valid_json():
    with DocumentRuntime() as rt:
        doc = rt.new_document(schema=schema)

        # doc.root["attr"] = "test"

        child = doc.root.append_child("child")
        child["attr"] = "test2"

        to_json = doc.root.to_json()

        validate(expected, schema=schema.to_json())

        assert json.dumps(to_json) == json.dumps(expected)


def test_append_single_json():
    with DocumentRuntime() as rt:
        # can copy a single element
        copy = rt.new_document(schema=schema)

        # copy.root["attr"] = "test"
        copy.root.append_json(expected["root"]["children"][0])

        assert json.dumps(copy.root.to_json()) == json.dumps(expected)


def test_get_children_by_tag_name_():
    with DocumentRuntime() as rt:
        # can copy a single element
        copy = rt.new_document(schema=schema)

        # copy.root["attr"] = "test"
        copy.root.append_child("child")
        copy.root.append_child("child2")
        copy.root.append_child("child")

        assert len(copy.root.get_children_by_tag_name("child")) == 2
        assert len(copy.root.get_children_by_tag_name("child2")) == 1
        assert len(copy.root.get_children_by_tag_name("x")) == 0


def test_doc_from_json():
    with DocumentRuntime() as rt:
        copy = rt.new_document(schema=schema, json=expected)

        assert json.dumps(copy.root.to_json()) == json.dumps(expected)
