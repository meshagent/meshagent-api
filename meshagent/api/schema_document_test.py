import logging
from jsonschema import validate

from meshagent.api.schema import MeshSchema, ElementType, ChildProperty, ValueProperty
from meshagent.api.runtime import DocumentRuntime
from meshagent.api.schema_document import EventEmitter

logger = logging.getLogger(__name__)


def test_event_emitter_matches_python_handler_list_semantics():
    emitter = EventEmitter()
    seen = []

    def second(value):
        seen.append(("second", value))

    def first(value):
        seen.append(("first", value))
        emitter.on("message")(second)

    returned = emitter.on("message")(first)
    assert returned is first

    emitter.emit("missing", "ignored")
    emitter.emit("message", "one")
    assert seen == [("first", "one"), ("second", "one")]

    emitter.emit("message", "two")
    assert seen == [
        ("first", "one"),
        ("second", "one"),
        ("first", "two"),
        ("second", "two"),
        ("second", "two"),
    ]


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

        assert to_json == expected


def test_append_single_json():
    with DocumentRuntime() as rt:
        # can copy a single element
        copy = rt.new_document(schema=schema)

        # copy.root["attr"] = "test"
        copy.root.append_json(expected["root"]["children"][0])

        assert copy.root.to_json() == expected


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


def test_element_grep():
    with DocumentRuntime() as rt:
        doc = rt.new_document(schema=schema)

        child = doc.root.append_child("child")
        child["attr"] = "Hello"

    child2 = doc.root.append_child("child2")
    child2["attr"] = "World"

    child3 = doc.root.append_child("child")
    child3["attr"] = "Hello Again"

    assert doc.root.grep("child2") == [child2]
    assert doc.root.grep("attr") == [child, child2, child3]
    assert doc.root.grep("Hello") == [child, child3]
    assert doc.root.grep("World", before=1) == [child, child2]
    assert doc.root.grep("World", after=1) == [child2, child3]


def test_element_grep_supports_python_regex_branches():
    with DocumentRuntime() as rt:
        doc = rt.new_document(schema=schema)
        doc.root.append_child("child", {"attr": "Hello hello"})
        doc.root.append_child("child2", {"attr": "foo foo"})

        assert [node.to_json() for node in doc.root.grep("(?<=He)llo")] == [
            {"child": {"attr": "Hello hello"}}
        ]
        assert [node.to_json() for node in doc.root.grep(r"(?P<x>foo) (?P=x)")] == [
            {"child2": {"attr": "foo foo"}}
        ]
        assert [
            node.to_json() for node in doc.root.grep(r"(hello) \1", ignore_case=True)
        ] == [{"child": {"attr": "Hello hello"}}]


def test_element_grep_invalid_regex_errors_match_python():
    with DocumentRuntime() as rt:
        doc = rt.new_document(schema=schema)
        doc.root.append_child("child", {"attr": "Hello"})

        for pattern, expected in [
            ("[", "unterminated character set at position 0"),
            (r"\p{L}", r"bad escape \p at position 0"),
            (
                "(?P<x>a)(?P<x>b)",
                "redefinition of group name 'x' as group 2; was group 1 at position 12",
            ),
            ("(?<bad>target", "unknown extension ?<b at position 1"),
        ]:
            try:
                doc.root.grep(pattern)
                raise AssertionError("expected regex error")
            except Exception as exc:
                assert str(exc) == expected


def test_doc_from_json():
    with DocumentRuntime() as rt:
        copy = rt.new_document(schema=schema, json=expected)

        assert copy.root.to_json() == expected


def test_doc_from_json_malformed_input_errors_match_python():
    cases = [
        ({"other": {}}, KeyError, "'root'"),
        ({"root": "bad"}, AttributeError, "'str' object has no attribute 'items'"),
        (
            {"root": {"children": "bad"}},
            AttributeError,
            "'str' object has no attribute 'keys'",
        ),
        (
            {"root": {"children": {"x": {}}}},
            AttributeError,
            "'str' object has no attribute 'keys'",
        ),
        (
            {"root": {"children": 1}},
            TypeError,
            "'int' object is not iterable",
        ),
        (
            {"root": {"children": None}},
            TypeError,
            "'NoneType' object is not iterable",
        ),
        (
            {"root": {"children": True}},
            TypeError,
            "'bool' object is not iterable",
        ),
    ]
    for initial_json, error_type, message in cases:
        with DocumentRuntime() as rt:
            try:
                rt.new_document(schema=schema, json=initial_json)
            except error_type as exc:
                assert str(exc) == message
            else:
                raise AssertionError("expected malformed initial JSON to fail")


def test_doc_from_json_preserves_root_attributes_without_crdt_fragment_error():
    root_attr_schema = MeshSchema(
        root_tag_name="root",
        elements=[
            ElementType(
                tag_name="root",
                description="",
                properties=[
                    ValueProperty(name="title", description="", type="string"),
                    ChildProperty(
                        name="children", description="", child_tag_names=["child"]
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
        ],
    )
    initial_json = {
        "root": {"title": "hello", "children": [{"child": {"attr": "one"}}]}
    }

    with DocumentRuntime() as rt:
        copy = rt.new_document(schema=root_attr_schema, json=initial_json)

        assert copy.root.to_json() == initial_json


def test_doc_from_json_emits_sync_callback_per_child_insert_and_attribute_set():
    seen: list[str] = []
    initial_json = {
        "root": {
            "children": [
                {"child": {"attr": "one"}},
                {"child": {"attr": "two"}},
            ]
        }
    }

    with DocumentRuntime() as rt:
        copy = rt.new_document(
            schema=schema,
            json=initial_json,
            on_document_sync=seen.append,
        )

        assert copy.root.to_json() == initial_json
        assert len(seen) == 4
        assert all(isinstance(update, str) and update for update in seen)


def test_receive_changes_emits_insert_delete_and_update_events():
    with DocumentRuntime() as rt:
        doc = rt.new_document(schema=schema)
        events: list[tuple[str, dict | str]] = []
        doc.on("inserted")(lambda node: events.append(("inserted", node.to_json(True))))
        doc.on("deleted")(lambda node: events.append(("deleted", node.to_json(True))))
        doc.on("updated")(
            lambda node, name: events.append(("updated", f"{node.tag_name}:{name}"))
        )

        doc.receive_changes(
            {
                "root": True,
                "target": None,
                "elements": [
                    {
                        "insert": [
                            {
                                "element": {
                                    "tagName": "child",
                                    "attributes": {"$id": "child-1", "attr": "Hello"},
                                    "children": [],
                                }
                            }
                        ]
                    }
                ],
                "attributes": {"set": [], "delete": []},
                "text": [],
            }
        )
        doc.receive_changes(
            {
                "root": False,
                "target": "child-1",
                "elements": [],
                "attributes": {
                    "set": [{"name": "attr", "value": "World"}],
                    "delete": [],
                },
                "text": [],
            }
        )
        doc.receive_changes(
            {
                "root": True,
                "target": None,
                "elements": [{"delete": 1}],
                "attributes": {"set": [], "delete": []},
                "text": [],
            }
        )

        assert events == [
            ("inserted", {"child": {"$id": "child-1", "attr": "Hello"}}),
            ("updated", "child:attr"),
            ("deleted", {"child": {"$id": "child-1", "attr": "World"}}),
        ]


def test_receive_changes_target_none_resolves_to_root_like_python():
    with DocumentRuntime() as rt:
        doc = rt.new_document(schema=schema)
        events: list[tuple[str, dict | str]] = []
        doc.on("inserted")(lambda node: events.append(("inserted", node.to_json(True))))
        doc.on("updated")(
            lambda node, name: events.append(("updated", f"{node.tag_name}:{name}"))
        )

        doc.receive_changes(
            {
                "root": False,
                "target": None,
                "elements": [
                    {
                        "insert": [
                            {
                                "element": {
                                    "tagName": "child",
                                    "attributes": {},
                                    "children": [],
                                }
                            }
                        ]
                    }
                ],
                "attributes": {"set": [{"name": "title", "value": "x"}], "delete": []},
                "text": [],
            }
        )

        assert doc.root.to_json(True) == {
            "root": {"title": "x", "children": [{"child": {}}]}
        }
        assert events == [("inserted", {"child": {}}), ("updated", "root:title")]


def test_receive_changes_unsupported_element_delta_preserves_prior_mutations_like_python():
    with DocumentRuntime() as rt:
        doc = rt.new_document(schema=schema)
        doc.root.append_child("child", {"attr": "one"})
        doc.root.append_child("child2", {"attr": "two"})
        events: list[tuple[str, dict]] = []
        doc.on("deleted")(lambda node: events.append(("deleted", node.to_json(True))))

        try:
            doc.receive_changes(
                {
                    "root": True,
                    "target": None,
                    "elements": [{"delete": 1}, {"insert": [{}]}],
                    "attributes": {"set": [], "delete": []},
                    "text": [],
                }
            )
            raise AssertionError("expected unsupported element delta")
        except Exception as exc:
            assert str(exc) == "Unsupported element delta"

        assert doc.root.to_json() == {
            "root": {"children": [{"child2": {"attr": "two"}}]}
        }
        assert len(events) == 1
        event_name, deleted_node = events[0]
        assert event_name == "deleted"
        assert deleted_node["child"]["attr"] == "one"
        assert "$id" in deleted_node["child"]


def test_receive_changes_missing_target_errors_match_python():
    cases = [
        (
            {
                "root": False,
                "target": "missing",
                "elements": [
                    {
                        "insert": [
                            {
                                "element": {
                                    "tagName": "child",
                                    "attributes": {},
                                    "children": [],
                                }
                            }
                        ]
                    }
                ],
                "attributes": {"set": [], "delete": []},
                "text": [],
            },
            "'NoneType' object has no attribute '_data'",
        ),
        (
            {
                "root": False,
                "target": "missing",
                "elements": [],
                "attributes": {"set": [], "delete": []},
                "text": [{"insert": "x"}],
            },
            "'NoneType' object has no attribute 'tag_name'",
        ),
        (
            {
                "root": False,
                "target": "missing",
                "elements": [],
                "attributes": {"set": [{"name": "attr", "value": "x"}], "delete": []},
                "text": [],
            },
            "'NoneType' object has no attribute '_data'",
        ),
    ]

    for message, expected in cases:
        with DocumentRuntime() as rt:
            doc = rt.new_document(schema=schema)
            try:
                doc.receive_changes(message)
                raise AssertionError("expected receive_changes error")
            except Exception as exc:
                assert str(exc) == expected

    with DocumentRuntime() as rt:
        doc = rt.new_document(schema=schema)
        doc.receive_changes(
            {
                "root": False,
                "target": "missing",
                "elements": [],
                "attributes": {"set": [], "delete": []},
                "text": [],
            }
        )
        assert doc.root.to_json(True) == {"root": {"children": []}}


def test_receive_changes_text_error_prevents_attribute_mutation_like_python():
    with DocumentRuntime() as rt:
        doc = rt.new_document(schema=schema)
        child = doc.root.append_child("child")

        try:
            doc.receive_changes(
                {
                    "root": False,
                    "target": child.id,
                    "elements": [],
                    "attributes": {
                        "set": [{"name": "attr", "value": "mutated"}],
                        "delete": [],
                    },
                    "text": [{"insert": "x"}],
                }
            )
            raise AssertionError("expected receive_changes error")
        except Exception as exc:
            assert str(exc) == "Node is not a text node: child"

        assert child.to_json(True) == {"child": {"$id": child.id}}
