from meshagent.api.schema import MeshSchema, ElementType, ValueProperty, ChildProperty
from meshagent.api.schema_document import Element, Text
from meshagent.api import crdt
from meshagent.api import runtime

import logging

logger = logging.getLogger(__name__)

schema = MeshSchema(
    root_tag_name="root",
    elements=[
        ElementType(
            tag_name="root",
            description="",
            properties=[
                ValueProperty(name="hello", description="", type="string"),
                ValueProperty(name="hi", description="", type="string"),
                ValueProperty(name="test", description="", type="string"),
                ChildProperty(
                    name="children", description="", child_tag_names=["child", "text"]
                ),
            ],
        ),
        ElementType(
            tag_name="child",
            description="",
            properties=[
                ValueProperty(name="hello", description="", type="string"),
                ValueProperty(name="hi", description="", type="string"),
                ValueProperty(name="test", description="", type="string"),
                ChildProperty(
                    name="children", description="", child_tag_names=["child"]
                ),
            ],
        ),
        ElementType(
            tag_name="text",
            description="",
            properties=[
                ChildProperty(
                    name="children", description="", child_tag_names=["child"]
                ),
                ValueProperty(name="hello", description="", type="string"),
            ],
        ),
    ],
)


def test_runtime():
    with runtime.DocumentRuntime() as rt:
        doc = rt.new_document(schema=schema)
        element = doc.root.append_child("child", {"hello": "world"})
        e2 = element.append_child("child", {"hi": "there"})
        e2.append_child("child", {"hello": "hi"})
        element["test"] = "test2"
        element._remove_attribute("test")
        # text = element.create_child_element("text", { "hi" : "there" })
        # text.get_children()[0].insert(0, "hello world")


def test_set_root_attribute():
    try:
        with runtime.DocumentRuntime() as rt:
            client = rt.new_document(schema=schema)
            client.root["test"] = "v1"
            raise Exception("root set attribute is not allowed")
    except Exception:
        pass


def test_insert_and_delete_element():
    with runtime.DocumentRuntime() as rt:
        client = rt.new_document(schema=schema)
        child = client.root.append_child("child", {"hello": "world"})

        assert isinstance(child, Element)
        assert isinstance(child, Element)
        assert child.tag_name == "child"
        assert child["hello"] == "world"

        # Can delete node
        child.delete()
        assert len(client.root.get_children()) == 0


def test_append_json_non_object_attributes_error_matches_python():
    with runtime.DocumentRuntime() as rt:
        client = rt.new_document(schema=schema)
        try:
            client.root.append_json({"child": "not-object"})
        except AttributeError as exc:
            assert str(exc) == "'str' object has no attribute 'copy'"
        else:
            raise AssertionError("expected non-object attributes to fail")

        try:
            client.root.append_json({"unknown": "not-object"})
        except AttributeError as exc:
            assert str(exc) == "'str' object has no attribute 'copy'"
        else:
            raise AssertionError("expected attributes validation before tag lookup")


def test_crdt_registry_missing_document_errors_match_python():
    cases = [
        lambda: crdt.get_state("missing", None),
        lambda: crdt.get_state_vector("missing"),
        lambda: crdt.apply_backend_changes("missing", ""),
        lambda: crdt.apply_changes({"documentID": "missing", "changes": []}),
    ]
    for operation in cases:
        try:
            operation()
        except KeyError as exc:
            assert str(exc) == "'missing'"
        else:
            raise AssertionError("expected missing CRDT document to fail")


def test_crdt_register_document_invalid_initial_state_errors_match_python():
    cases = [
        ("AQ", "Incorrect padding"),
        ("é", "string argument should contain only ASCII characters"),
        (
            "not-base64",
            "Invalid base64-encoded string: number of data characters (9) "
            "cannot be 1 more than a multiple of 4",
        ),
    ]
    for payload, message in cases:
        doc_id = f"runtime-test-invalid-state-{payload!r}"
        crdt.unregister_document(doc_id)
        try:
            try:
                crdt.register_document(doc_id, payload, False)
            except Exception as exc:
                assert str(exc) == message
            else:
                raise AssertionError("expected invalid initial CRDT state to fail")
        finally:
            crdt.unregister_document(doc_id)


def test_crdt_apply_changes_mutates_server_document_like_python():
    doc_id = "runtime-test-apply-changes"
    crdt.unregister_document(doc_id)
    crdt.register_document(doc_id, None, False)
    try:
        crdt.apply_changes(
            {
                "documentID": doc_id,
                "changes": [
                    {
                        "insertChildren": {
                            "children": [
                                {
                                    "element": {
                                        "name": "child",
                                        "attributes": {
                                            "$id": "child-1",
                                            "attr": "one",
                                        },
                                        "children": [],
                                    }
                                }
                            ]
                        }
                    },
                    {
                        "nodeID": "child-1",
                        "setAttributes": {"attr": "two", "other": 3},
                    },
                    {
                        "nodeID": "child-1",
                        "removeAttributes": ["other"],
                    },
                    {
                        "insertChildren": {
                            "after": "child-1",
                            "children": [
                                {
                                    "element": {
                                        "name": "text",
                                        "attributes": {"$id": "text-1"},
                                        "children": [{"text": {"delta": []}}],
                                    }
                                }
                            ],
                        }
                    },
                    {
                        "nodeID": "text-1",
                        "insertText": {
                            "index": 0,
                            "text": "hello",
                            "attributes": {"bold": True},
                        },
                    },
                    {"nodeID": "child-1", "delete": {}},
                ],
            }
        )

        root = crdt.Documents[doc_id]._root
        assert len(root.children) == 1
        text_element = root.children[0]
        assert text_element.tag == "text"
        assert text_element.attributes["$id"] == "text-1"
        assert text_element.children[0].to_py() == "<bold>hello</bold>"
    finally:
        crdt.unregister_document(doc_id)


def test_crdt_apply_changes_mixed_operation_ordering_matches_python():
    doc_id = "runtime-test-apply-changes-ordering"
    crdt.unregister_document(doc_id)
    crdt.register_document(doc_id, None, False)
    try:
        crdt.apply_changes(
            {
                "documentID": doc_id,
                "changes": [
                    {
                        "insertChildren": {
                            "children": [
                                {
                                    "element": {
                                        "name": "child",
                                        "attributes": {
                                            "$id": "child-1",
                                            "attr": "old",
                                        },
                                        "children": [],
                                    }
                                }
                            ]
                        }
                    }
                ],
            }
        )

        crdt.apply_changes(
            {
                "documentID": doc_id,
                "changes": [
                    {
                        "nodeID": "child-1",
                        "removeAttributes": ["attr"],
                        "setAttributes": {"attr": "new"},
                    }
                ],
            }
        )
        child = crdt.Documents[doc_id]._find_node("child-1")
        assert child.attributes["attr"] == "new"

        try:
            crdt.apply_changes(
                {
                    "documentID": doc_id,
                    "changes": [
                        {
                            "nodeID": "child-1",
                            "setAttributes": {"attr": "before-error"},
                            "insertText": {"index": 0, "text": "x"},
                        }
                    ],
                }
            )
        except TypeError as exc:
            assert str(exc) == "Node is not <text>"
        else:
            raise AssertionError("expected non-text insertText to fail")
        assert child.attributes["attr"] == "before-error"

        crdt.apply_changes(
            {
                "documentID": doc_id,
                "changes": [
                    {
                        "nodeID": "child-1",
                        "delete": {},
                        "setAttributes": {"attr": "ignored"},
                    }
                ],
            }
        )
        assert crdt.Documents[doc_id]._find_node("child-1") is None
    finally:
        crdt.unregister_document(doc_id)


def test_crdt_apply_changes_text_edge_indexes_match_python():
    cases = [
        (
            "insert beyond",
            {"nodeID": "text-1", "insertText": {"index": 99, "text": "X"}},
            None,
            "abcX",
        ),
        (
            "insert negative",
            {"nodeID": "text-1", "insertText": {"index": -1, "text": "X"}},
            "out of range integral type conversion attempted",
            "abc",
        ),
        (
            "format beyond start",
            {
                "nodeID": "text-1",
                "formatText": {
                    "from": 99,
                    "length": 1,
                    "attributes": {"bold": True},
                },
            },
            None,
            "abc",
        ),
        (
            "format beyond length",
            {
                "nodeID": "text-1",
                "formatText": {
                    "from": 1,
                    "length": 99,
                    "attributes": {"bold": True},
                },
            },
            None,
            "a<bold>bc</bold>",
        ),
        (
            "format negative start",
            {
                "nodeID": "text-1",
                "formatText": {
                    "from": -1,
                    "length": 1,
                    "attributes": {"bold": True},
                },
            },
            "Negative start not supported",
            "abc",
        ),
        (
            "format negative length",
            {
                "nodeID": "text-1",
                "formatText": {
                    "from": 1,
                    "length": -1,
                    "attributes": {"bold": True},
                },
            },
            None,
            "abc",
        ),
        (
            "delete negative index",
            {"nodeID": "text-1", "deleteText": {"index": -1, "length": 1}},
            "Negative start not supported",
            "abc",
        ),
        (
            "delete negative length",
            {"nodeID": "text-1", "deleteText": {"index": 1, "length": -1}},
            None,
            "abc",
        ),
    ]

    for name, change, expected_error, expected_text in cases:
        doc_id = f"runtime-test-apply-changes-text-edge-{name.replace(' ', '-')}"
        crdt.unregister_document(doc_id)
        crdt.register_document(doc_id, None, False)
        try:
            crdt.apply_changes(
                {
                    "documentID": doc_id,
                    "changes": [
                        {
                            "insertChildren": {
                                "children": [
                                    {
                                        "element": {
                                            "name": "text",
                                            "attributes": {"$id": "text-1"},
                                            "children": [{"text": {}}],
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "nodeID": "text-1",
                            "insertText": {"index": 0, "text": "abc"},
                        },
                    ],
                }
            )
            text = crdt.Documents[doc_id]._find_node("text-1").children[0]
            if expected_error is None:
                crdt.apply_changes({"documentID": doc_id, "changes": [change]})
            else:
                try:
                    crdt.apply_changes({"documentID": doc_id, "changes": [change]})
                    raise AssertionError(f"expected raw text edge error for {name}")
                except Exception as exc:
                    assert str(exc) == expected_error
            assert text.to_py() == expected_text
        finally:
            crdt.unregister_document(doc_id)


def test_crdt_apply_changes_undo_redo_matches_python():
    for undo_enabled in [False, True]:
        doc_id = f"runtime-test-apply-changes-undo-{undo_enabled}"
        crdt.unregister_document(doc_id)
        crdt.register_document(doc_id, None, undo_enabled)
        try:
            crdt.apply_changes(
                {
                    "documentID": doc_id,
                    "changes": [
                        {
                            "insertChildren": {
                                "children": [
                                    {
                                        "element": {
                                            "name": "child",
                                            "attributes": {"$id": "a"},
                                            "children": [],
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "insertChildren": {
                                "children": [
                                    {
                                        "element": {
                                            "name": "child",
                                            "attributes": {"$id": "b"},
                                            "children": [],
                                        }
                                    }
                                ]
                            }
                        },
                    ],
                }
            )

            root = crdt.Documents[doc_id]._root
            assert [child.attributes["$id"] for child in root.children] == ["a", "b"]

            crdt.apply_changes({"documentID": doc_id, "changes": [{"undo": True}]})
            if undo_enabled:
                assert list(root.children) == []
            else:
                assert [child.attributes["$id"] for child in root.children] == [
                    "a",
                    "b",
                ]

            crdt.apply_changes({"documentID": doc_id, "changes": [{"redo": True}]})
            assert [child.attributes["$id"] for child in root.children] == ["a", "b"]
        finally:
            crdt.unregister_document(doc_id)


def test_runtime_document_sync_callback_replays_post_constructor_updates():
    updates: list[str] = []
    rt = runtime.DocumentRuntime()
    source = None
    mirror = None
    try:
        source = rt.new_document(
            schema=schema,
            id="runtime-test-sync-callback-source",
            on_document_sync=updates.append,
        )
        child = source.root.append_json({"child": {"hello": "one"}})
        child.set_attribute("hello", "two")

        assert updates
        mirror = rt.new_document(schema=schema, id="runtime-test-sync-callback-mirror")
        for update in updates:
            rt.apply_backend_changes(mirror.id, update)

        assert mirror.to_json() == source.to_json()
    finally:
        if source is not None:
            source.close()
        if mirror is not None:
            mirror.close()


def test_runtime_document_data_constructor_replays_backend_state_and_sync_callback():
    source_rt = runtime.DocumentRuntime()
    loaded_rt = runtime.DocumentRuntime()
    source = None
    loaded = None
    try:
        source = source_rt.new_document(
            schema=schema,
            id="runtime-test-data-constructor-source",
        )
        source.root.append_json({"child": {"hello": "world"}})
        state = source.get_state()

        updates: list[str] = []
        loaded = loaded_rt.new_document(
            schema=schema,
            id="runtime-test-data-constructor-loaded",
            data=state,
            on_document_sync=updates.append,
        )

        assert loaded.to_json() == source.to_json()
        assert len(updates) == 1
    finally:
        if source is not None:
            source.close()
        if loaded is not None:
            loaded.close()


def test_document_runtime_apply_changes_forwards_to_crdt_registry():
    rt = runtime.DocumentRuntime()
    doc = None
    try:
        doc = rt.new_document(schema=schema, id="runtime-test-apply-changes-wrapper")
        rt.apply_changes(
            {
                "documentID": doc.id,
                "changes": [
                    {
                        "nodeID": None,
                        "insertChildren": {
                            "children": [
                                {
                                    "element": {
                                        "name": "child",
                                        "attributes": {
                                            "$id": "runtime-wrapper-child",
                                            "hello": "world",
                                        },
                                        "children": [],
                                    }
                                }
                            ]
                        },
                    }
                ],
            }
        )

        assert doc.to_json() == {
            "root": {
                "children": [
                    {
                        "child": {
                            "hello": "world",
                            "children": [],
                        }
                    }
                ]
            }
        }
    finally:
        if doc is not None:
            doc.close()


def test_element_get_attribute_defaults_match_python():
    with runtime.DocumentRuntime() as rt:
        client = rt.new_document(schema=schema)
        child = client.root.append_child("child", {"hello": "world"})

        assert child.get_attribute("hello") == "world"
        assert child.get_attribute("missing") is None
        assert child.get_attribute("missing", "fallback") == "fallback"
        assert child["missing"] is None

    root_schema = MeshSchema(
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
                properties=[],
            ),
        ],
    )
    with runtime.DocumentRuntime() as rt:
        client = rt.new_document(
            schema=root_schema,
            json={"root": {"title": "Root title", "children": []}},
        )
        assert client.root.get_attribute("title") == "Root title"
        assert client.root.get_attribute("missing", "fallback") == "fallback"


def test_text_delta_property_read_matches_python():
    with runtime.DocumentRuntime() as rt:
        client = rt.new_document(schema=schema)
        child = client.root.append_child("text", {"hello": "world"})
        text = child.get_children()[0]

        assert text.delta == [{"insert": "", "attributes": {}}]
        text.insert(0, "hello world")
        assert text.delta == [{"insert": "hello world", "attributes": {}}]
        text.format(0, 5, {"bold": True})
        assert text.delta == [
            {"insert": "hello", "attributes": {"bold": True}},
            {"insert": " world", "attributes": {}},
        ]

        client.receive_changes(
            {
                "root": False,
                "target": child.id,
                "elements": [],
                "attributes": {"set": [], "delete": []},
                "text": [{"retain": 5}, {"insert": "!"}],
            }
        )
        assert text.delta == [
            {"insert": "hello!", "attributes": {"bold": True}},
            {"insert": " world", "attributes": {}},
        ]


def test_crdt_apply_changes_rejects_non_empty_text_child_delta_like_python():
    doc_id = "runtime-test-apply-changes-text-delta"
    crdt.unregister_document(doc_id)
    crdt.register_document(doc_id, None, False)
    try:
        try:
            crdt.apply_changes(
                {
                    "documentID": doc_id,
                    "changes": [
                        {
                            "insertChildren": {
                                "children": [
                                    {
                                        "text": {
                                            "delta": [
                                                {"insert": "not allowed"},
                                            ]
                                        }
                                    }
                                ]
                            }
                        }
                    ],
                }
            )
        except Exception as exc:
            assert str(exc) == "delta inserts on new child nodes are not allowed"
        else:
            raise AssertionError("expected non-empty text child delta to fail")
    finally:
        crdt.unregister_document(doc_id)


def test_crdt_apply_changes_error_branches_match_python():
    cases = [
        (
            [{"removeAttributes": ["x"]}],
            AttributeError,
            "'XmlFragment' object has no attribute 'attributes'",
        ),
        (
            [{"setAttributes": {"x": 1}}],
            AttributeError,
            "'XmlFragment' object has no attribute 'attributes'",
        ),
        (
            [{"insertText": {"index": 0, "text": "x"}}],
            AttributeError,
            "'XmlFragment' object has no attribute 'tag'",
        ),
        (
            [{"formatText": {"from": 0, "length": 1, "attributes": {"b": True}}}],
            AttributeError,
            "'XmlFragment' object has no attribute 'tag'",
        ),
        (
            [{"deleteText": {"index": 0, "length": 1}}],
            AttributeError,
            "'XmlFragment' object has no attribute 'tag'",
        ),
        (
            [{"deleteChildren": {"length": 1}}],
            ValueError,
            "index required when 'after' not supplied",
        ),
        (
            [{"deleteChildren": {"after": "missing", "length": 1}}],
            ValueError,
            "after id 'missing' not found",
        ),
        (
            [{"insertChildren": {"after": "missing", "children": []}}],
            ValueError,
            "after id 'missing' not found",
        ),
        (
            [{"insertChildren": {}}],
            TypeError,
            "ServerXmlDocument._insert_children() missing 1 required keyword-only "
            "argument: 'children'",
        ),
        (
            [{"insertChildren": {"children": None}}],
            TypeError,
            "'NoneType' object is not iterable",
        ),
        (
            [{"deleteChildren": {"index": 0}}],
            TypeError,
            "ServerXmlDocument._delete_children() missing 1 required keyword-only "
            "argument: 'length'",
        ),
        (
            [{"deleteChildren": {"index": 0, "length": None}}],
            TypeError,
            "unsupported operand type(s) for +: 'int' and 'NoneType'",
        ),
        (
            [{"deleteChildren": {"index": 0, "length": "x"}}],
            TypeError,
            "unsupported operand type(s) for +: 'int' and 'str'",
        ),
        (
            [{"nodeID": "missing", "setAttributes": {"x": 1}}],
            AttributeError,
            "'NoneType' object has no attribute 'attributes'",
        ),
        (
            [{"nodeID": "missing", "insertChildren": {"children": []}}],
            AttributeError,
            "'NoneType' object has no attribute 'children'",
        ),
        (
            [{"nodeID": "missing", "insertText": {"index": 0, "text": "x"}}],
            AttributeError,
            "'NoneType' object has no attribute 'tag'",
        ),
        (
            [{"nodeID": "missing", "delete": {}}],
            AttributeError,
            "'NoneType' object has no attribute 'parent'",
        ),
    ]
    for index, (changes, error_type, message) in enumerate(cases):
        doc_id = f"runtime-test-apply-changes-errors-{index}"
        crdt.unregister_document(doc_id)
        crdt.register_document(doc_id, None, False)
        try:
            try:
                crdt.apply_changes({"documentID": doc_id, "changes": changes})
            except error_type as exc:
                assert str(exc) == message
            else:
                raise AssertionError("expected apply_changes error")
        finally:
            crdt.unregister_document(doc_id)


def test_crdt_apply_changes_node_id_truthiness_matches_python():
    falsey_values = [None, "", 0, False, [], {}]
    for index, node_id in enumerate(falsey_values):
        doc_id = f"runtime-test-apply-changes-node-id-falsey-{index}"
        crdt.unregister_document(doc_id)
        crdt.register_document(doc_id, None, False)
        try:
            crdt.apply_changes(
                {
                    "documentID": doc_id,
                    "changes": [
                        {
                            "nodeID": node_id,
                            "insertChildren": {
                                "children": [
                                    {
                                        "element": {
                                            "name": "child",
                                            "attributes": {"$id": f"child-{index}"},
                                            "children": [],
                                        }
                                    }
                                ]
                            },
                        }
                    ],
                }
            )
            assert len(crdt.Documents[doc_id]._root.children) == 1
        finally:
            crdt.unregister_document(doc_id)

    truthy_values = [1, True, [1], {"x": 1}]
    for index, node_id in enumerate(truthy_values):
        doc_id = f"runtime-test-apply-changes-node-id-truthy-{index}"
        crdt.unregister_document(doc_id)
        crdt.register_document(doc_id, None, False)
        try:
            try:
                crdt.apply_changes(
                    {
                        "documentID": doc_id,
                        "changes": [
                            {
                                "nodeID": node_id,
                                "insertChildren": {"children": []},
                            }
                        ],
                    }
                )
            except AttributeError as exc:
                assert str(exc) == "'NoneType' object has no attribute 'children'"
            else:
                raise AssertionError("expected truthy non-string nodeID to miss target")
        finally:
            crdt.unregister_document(doc_id)


def test_update_attribute():
    with runtime.DocumentRuntime() as rt:
        client = rt.new_document(schema=schema)
        child = client.root.append_child("child", {"hello": "world"})

        # Can update attribute
        child["hello"] = "mod"
        assert child["hello"] == "mod"


def test_remove_attribute():
    with runtime.DocumentRuntime() as rt:
        client = rt.new_document(schema=schema)

        child = client.root.append_child("child", {"hello": "world"})
        child["hello"] = "mod"

        # Can remove attribute
        child._remove_attribute("hello")
        assert child["hello"] is None


def test_insert_extend_and_shrink_text_delta():
    with runtime.DocumentRuntime() as rt:
        client = rt.new_document(schema=schema)

        # Can insert text
        child = client.root.append_child("text", {"hello": "world"})
        assert isinstance(child, Element)

        text = child.get_children()[0]

        assert isinstance(text, Text), True
        assert child.tag_name == "text"
        assert child["hello"] == "world"

        text.insert(0, "hello world")
        assert len(text.delta) == 1
        assert text.delta[0]["insert"] == "hello world"

        text.insert(0, "hello world")
        assert len(text.delta) == 1
        assert text.delta[0]["insert"] == "hello worldhello world"

        text.delete(len("hello world"), len("hello world"))
        assert len(text.delta) == 1
        assert text.delta[0]["insert"] == "hello world"

        text.delete(len("hello world") - 1, 1)
        assert len(text.delta) == 1
        assert text.delta[0]["insert"] == "hello worl"


def test_insert_text_empty_attributes_match_unformatted_insert():
    with runtime.DocumentRuntime() as rt:
        client = rt.new_document(schema=schema)
        child = client.root.append_child("text", {"hello": "world"})
        assert isinstance(child, Element)

        text = child.get_children()[0]
        assert isinstance(text, Text)
        text.insert(0, "hello", {})
        text.insert(5, " world", {})

        assert text.delta == [{"insert": "hello world", "attributes": {}}]


def test_get_children_by_tag_name_raises_on_text_children_like_python():
    with runtime.DocumentRuntime() as rt:
        client = rt.new_document(schema=schema)
        child = client.root.append_child("text", {"hello": "world"})
        assert isinstance(child, Element)

        try:
            child.get_children_by_tag_name("child")
            raise AssertionError("expected AttributeError")
        except AttributeError as exc:
            assert str(exc) == "'Text' object has no attribute 'tag_name'"


def test_format_text_deltas():
    with runtime.DocumentRuntime() as rt:
        client = rt.new_document(schema=schema)

        # Can insert text
        child = client.root.append_child("text", {"hello": "world"})
        assert isinstance(child, Element)

        text = child.get_children()[0]
        assert isinstance(text, Text)
        assert child.tag_name == "text"
        assert child["hello"] == "world"

        assert (
            len(text.delta) == 0
            or len(text.delta) == 1
            and len(text.delta[0]["insert"]) == 0
        )
        text.insert(0, "hello world")
        # format whole item
        text.format(0, len("hello world"), {"bold": True})

        assert len(text.delta) == 1
        assert text.delta[0]["insert"] == "hello world"
        assert text.delta[0]["attributes"]["bold"]

        # format start
        text.format(0, len("hello"), {"italic": True})

        assert len(text.delta) == 2
        assert text.delta[0]["insert"] == "hello"
        assert text.delta[1]["insert"] == " world"
        assert text.delta[0]["attributes"]["bold"]
        assert text.delta[0]["attributes"]["italic"]
        assert text.delta[1]["attributes"]["bold"]
        assert "italic" not in text.delta[1]["attributes"]

        # format end
        text.format(3, 2, {"underline": True})

        assert len(text.delta) == 3
        assert text.delta[0]["insert"] == "hel"
        assert text.delta[1]["insert"] == "lo"
        assert text.delta[2]["insert"] == " world"

        assert text.delta[0]["attributes"]["bold"]
        assert text.delta[0]["attributes"]["italic"]
        assert "underline" not in text.delta[0]["attributes"]

        assert text.delta[1]["attributes"]["bold"]
        assert text.delta[1]["attributes"]["italic"]
        assert text.delta[1]["attributes"]["underline"]

        assert text.delta[2]["attributes"]["bold"]
        assert "italic" not in text.delta[2]["attributes"]
        assert "underline" not in text.delta[2]["attributes"]

        # format across items
        text.format(0, len("hello world"), {"strikethrough": True})

        assert len(text.delta) == 3
        assert text.delta[0]["insert"] == "hel"
        assert text.delta[1]["insert"] == "lo"
        assert text.delta[2]["insert"] == " world"

        assert text.delta[0]["attributes"]["bold"]
        assert text.delta[0]["attributes"]["italic"]
        assert "underline" not in text.delta[0]["attributes"]
        assert text.delta[0]["attributes"]["strikethrough"]

        assert text.delta[1]["attributes"]["bold"]
        assert text.delta[1]["attributes"]["italic"]
        assert text.delta[1]["attributes"]["underline"]
        assert text.delta[1]["attributes"]["strikethrough"]

        assert text.delta[2]["attributes"]["bold"]
        assert "italic" not in text.delta[2]["attributes"]
        assert "underline" not in text.delta[2]["attributes"]
        assert text.delta[2]["attributes"]["strikethrough"]

        # format across items
        text.format(1, 1, {"dot": True})

        assert len(text.delta) == 5
        assert text.delta[0]["insert"] == "h"
        assert text.delta[1]["insert"] == "e"
        assert text.delta[2]["insert"] == "l"


def test_text_edge_indexes_match_python_pycrdt_behavior():
    with runtime.DocumentRuntime() as rt:
        client = rt.new_document(schema=schema)
        child = client.root.append_child("text", {"hello": "world"})
        text = child.get_children()[0]
        assert isinstance(text, Text)

        text.insert(0, "abc")
        text.insert(99, "X")
        assert text.delta == [{"insert": "abcX", "attributes": {}}]

        text.format(99, 1, {"bold": True})
        assert text.delta == [{"insert": "abcX", "attributes": {}}]

        text.format(1, 99, {"bold": True})
        assert text.delta == [
            {"insert": "a", "attributes": {}},
            {"insert": "bcX", "attributes": {"bold": True}},
        ]

        text.format(1, -1, {"italic": True})
        assert text.delta == [
            {"insert": "a", "attributes": {}},
            {"insert": "bcX", "attributes": {"bold": True}},
        ]

        text.delete(1, -1)
        assert text.delta == [
            {"insert": "a", "attributes": {}},
            {"insert": "bcX", "attributes": {"bold": True}},
        ]


def test_receive_changes_text_insert_inherits_active_attributes():
    with runtime.DocumentRuntime() as rt:
        client = rt.new_document(schema=schema)
        child = client.root.append_child("text", {"hello": "world"})
        assert isinstance(child, Element)

        client.receive_changes(
            {
                "root": False,
                "target": child.id,
                "elements": [],
                "attributes": {"set": [], "delete": []},
                "text": [{"insert": "hello world"}],
            }
        )
        client.receive_changes(
            {
                "root": False,
                "target": child.id,
                "elements": [],
                "attributes": {"set": [], "delete": []},
                "text": [{"retain": len("hello"), "attributes": {"bold": True}}],
            }
        )
        client.receive_changes(
            {
                "root": False,
                "target": child.id,
                "elements": [],
                "attributes": {"set": [], "delete": []},
                "text": [{"retain": len("hello")}, {"insert": "!"}],
            }
        )

        text = child.get_children()[0]
        assert isinstance(text, Text)
        assert text.delta == [
            {"insert": "hello!", "attributes": {"bold": True}},
            {"insert": " world", "attributes": {}},
        ]


def test_receive_changes_text_boundary_deltas_match_python():
    def formatted_text():
        rt = runtime.DocumentRuntime()
        client = rt.new_document(schema=schema)
        child = client.root.append_child("text", {"hello": "world"})
        assert isinstance(child, Element)
        client.receive_changes(
            {
                "root": False,
                "target": child.id,
                "elements": [],
                "attributes": {"set": [], "delete": []},
                "text": [{"insert": "hello world"}],
            }
        )
        client.receive_changes(
            {
                "root": False,
                "target": child.id,
                "elements": [],
                "attributes": {"set": [], "delete": []},
                "text": [{"retain": len("hello"), "attributes": {"bold": True}}],
            }
        )
        return rt, client, child

    rt, _client, child = formatted_text()
    with rt:
        child.doc.receive_changes(
            {
                "root": False,
                "target": child.id,
                "elements": [],
                "attributes": {"set": [], "delete": []},
                "text": [{"retain": 3}, {"delete": 5}],
            }
        )
        text = child.get_children()[0]
        assert isinstance(text, Text)
        assert text.delta == [
            {"insert": "hel", "attributes": {"bold": True}},
            {"insert": "rld", "attributes": {}},
        ]

    rt, _client, child = formatted_text()
    with rt:
        child.doc.receive_changes(
            {
                "root": False,
                "target": child.id,
                "elements": [],
                "attributes": {"set": [], "delete": []},
                "text": [
                    {"retain": len("hello")},
                    {"retain": 1, "attributes": {"italic": True}},
                ],
            }
        )
        text = child.get_children()[0]
        assert isinstance(text, Text)
        assert text.delta == [
            {"insert": "hello", "attributes": {"bold": True}},
            {"insert": "", "attributes": {"bold": True, "italic": True}},
            {"insert": " ", "attributes": {"italic": True}},
            {"insert": "world", "attributes": {}},
        ]


def test_receive_changes_text_list_insert_errors_match_python():
    cases = [
        (
            "list first",
            [{"insert": [{"text": {"delta": []}}]}],
            'can only concatenate str (not "list") to str',
            [{"insert": "", "attributes": {}}],
        ),
        (
            "string then list",
            [{"insert": "abc"}, {"insert": [{"text": {"delta": []}}]}],
            'can only concatenate str (not "list") to str',
            [{"insert": "abc", "attributes": {}}],
        ),
        (
            "list after retain past end",
            [
                {"insert": "abc"},
                {"retain": 1},
                {"insert": [{"text": {"delta": []}}]},
            ],
            "list index out of range",
            [{"insert": "abc", "attributes": {}}],
        ),
    ]
    for _name, text_deltas, expected_error, expected_delta in cases:
        with runtime.DocumentRuntime() as rt:
            client = rt.new_document(schema=schema)
            child = client.root.append_child("text", {"hello": "world"})
            text = child.get_children()[0]
            assert isinstance(text, Text)

            try:
                client.receive_changes(
                    {
                        "root": False,
                        "target": child.id,
                        "elements": [],
                        "attributes": {"set": [], "delete": []},
                        "text": text_deltas,
                    }
                )
                raise AssertionError("expected list text insert to fail")
            except Exception as exc:
                assert str(exc) == expected_error
            assert text.delta == expected_delta


def test_receive_changes_element_delta_inserts_text_child_descriptor():
    with runtime.DocumentRuntime() as rt:
        client = rt.new_document(schema=schema)
        events: list[dict] = []
        client.on("inserted")(lambda node: events.append(node.to_json()))
        client.on("deleted")(lambda node: events.append(node.to_json()))

        client.receive_changes(
            {
                "root": True,
                "target": None,
                "elements": [
                    {
                        "insert": [
                            {
                                "text": {
                                    "delta": [
                                        {
                                            "insert": "hi",
                                            "attributes": {},
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                ],
                "attributes": {"set": [], "delete": []},
                "text": [],
            }
        )

        assert client.root.to_json() == {
            "root": {
                "children": [{"text": {"delta": [{"insert": "hi", "attributes": {}}]}}]
            }
        }
        assert events == [{"text": {"delta": [{"insert": "hi", "attributes": {}}]}}]
        client.receive_changes(
            {
                "root": True,
                "target": None,
                "elements": [{"delete": 1}],
                "attributes": {"set": [], "delete": []},
                "text": [],
            }
        )

        assert client.root.to_json() == {"root": {"children": []}}
        assert events == [
            {"text": {"delta": [{"insert": "hi", "attributes": {}}]}},
            {"text": {"delta": [{"insert": "hi", "attributes": {}}]}},
        ]


def test_receive_changes_element_descriptor_preserves_missing_id_and_nested_text():
    with runtime.DocumentRuntime() as rt:
        client = rt.new_document(schema=schema)
        events: list[dict] = []

        def append_inserted(node):
            if isinstance(node, Text):
                events.append(node.to_json())
            else:
                events.append(node.to_json(True))

        client.on("inserted")(append_inserted)

        client.receive_changes(
            {
                "root": True,
                "target": None,
                "elements": [
                    {
                        "insert": [
                            {
                                "element": {
                                    "tagName": "text",
                                    "attributes": {"hello": "world"},
                                    "children": [
                                        {
                                            "text": {
                                                "delta": [
                                                    {
                                                        "insert": "nested",
                                                        "attributes": {},
                                                    }
                                                ]
                                            }
                                        }
                                    ],
                                }
                            }
                        ]
                    }
                ],
                "attributes": {"set": [], "delete": []},
                "text": [],
            }
        )

        expected = {
            "text": {
                "hello": "world",
                "children": [
                    {"text": {"delta": [{"insert": "nested", "attributes": {}}]}}
                ],
            }
        }
        assert client.root.to_json(True) == {"root": {"children": [expected]}}
        assert events == [
            {"text": {"delta": [{"insert": "nested", "attributes": {}}]}},
            expected,
        ]


def test_delete_start_of_delta_text():
    with runtime.DocumentRuntime() as rt:
        client = rt.new_document(schema=schema)

        # Text Delete
        client.root.append_child("text", {"hello": "world"})
        assert len(client.root.get_children()) == 1
        child = client.root.get_children()[0]

        # Delete start
        assert isinstance(child, Element)
        text = child.get_children()[0]
        assert isinstance(text, Text)
        text.insert(0, "hello world")
        assert len(text.delta) == 1
        assert text.delta[0]["insert"] == "hello world"
        text.delete(0, len("hello "))
        assert len(text.delta) == 1
        assert text.delta[0]["insert"] == "world"


def test_delete_end_of_delta_text():
    with runtime.DocumentRuntime() as rt:
        client = rt.new_document(schema=schema)

        # Text Delete
        client.root.append_child("text", {"hello": "world"})
        assert len(client.root.get_children()) == 1
        child = client.root.get_children()[0]
        text = child.get_children()[0]
        text.insert(0, "world")

        # Delete end
        text.delete(len("world") - 1, 1)
        assert len(text.delta) == 1
        assert text.delta[0]["insert"] == "worl"


def test_delete_center_of_delta_text():
    with runtime.DocumentRuntime() as rt:
        client = rt.new_document(schema=schema)
        client.root.append_child("text", {"hello": "world"})
        assert len(client.root.get_children()) == 1
        child = client.root.get_children()[0]
        text = child.get_children()[0]
        text.insert(0, "worl")

        # Delete center
        text.delete(2, 1)
        assert len(text.delta) == 1
        assert text.delta[0]["insert"] == "wol"


def test_insert_elements_at_positions():
    with runtime.DocumentRuntime() as rt:
        client = rt.new_document(schema=schema)

        # Inserts at end
        client.root.append_child("child", {"hello": "world2"})

        child = client.root.get_children()[0]
        assert child.tag_name == "child"
        assert child["hello"] == "world2"

        # Inserts deep
        child.append_child("child", {"hello": "world3"})

        deepChild = child

        child = child.get_children()[0]
        assert child.tag_name == "child"
        assert child["hello"] == "world3"

        # Inserts after deep
        child.parent.append_child("child", {"hello": "world4"})

        child = deepChild.get_children()[1]
        assert child.tag_name == "child"
        assert child["hello"] == "world4"

        # Inserts after element deep
        deepChild.insert_child_after(
            deepChild.get_children()[0], "child", {"hello": "world5"}
        )

        child = deepChild.get_children()[1]
        assert child.tag_name == "child"
        assert child["hello"] == "world5"

        # Inserts after element deep
        deepChild.insert_child_at(2, "child", {"hello": "world6"})

        child = deepChild.get_children()[2]
        assert child.tag_name == "child"
        assert child["hello"] == "world6"

        logger.info(child)


def test_insert_child_after_validates_child_before_after_lookup():
    with runtime.DocumentRuntime() as rt:
        client = rt.new_document(schema=schema)
        parent = client.root.append_child("child", {"hello": "parent"})

        try:
            parent.insert_child_after(parent, "missing_tag", {"hello": "bad"})
            raise AssertionError("insert_child_after should reject invalid child first")
        except Exception as exc:
            assert str(exc) == "cannot add missing_tag to child, allowed tags ['child']"


def test_insert_child_after_rejects_element_from_different_parent():
    with runtime.DocumentRuntime() as rt:
        client = rt.new_document(schema=schema)
        first_parent = client.root.append_child("child", {"hello": "first"})
        second_parent = client.root.append_child("child", {"hello": "second"})
        nested = second_parent.append_child("child", {"hello": "nested"})

        try:
            first_parent.insert_child_after(nested, "child", {"hello": "bad"})
            raise AssertionError(
                "insert_child_after should reject a child from another parent"
            )
        except Exception as exc:
            assert str(exc) == "Element does not belong to this node"
