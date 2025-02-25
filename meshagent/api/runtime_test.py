from meshagent.api.schema import MeshSchema, ElementType, ValueProperty, ChildProperty, SimpleValue
from meshagent.api.schema_document import Element, Text
from meshagent.api import runtime

import logging

logger = logging.getLogger(__name__)

schema=MeshSchema(
    root_tag_name="root",
    elements=[
        ElementType(
            tag_name="root",
            description="",
            properties=[
                ValueProperty(name="hello",description="", type="string"),
                ValueProperty(name="hi",description="", type="string"),
                ValueProperty(name="test",description="", type="string"),
                ChildProperty(name="children",description="", child_tag_names=["child","text"])
            ]
        ),
        ElementType(
            tag_name="child",
            description="",
            properties=[
                ValueProperty(name="hello",description="", type="string"),
                ValueProperty(name="hi",description="", type="string"),
                ValueProperty(name="test",description="", type="string"),
                ChildProperty(name="children",description="", child_tag_names=["child"])
            ]
        ),
        ElementType(
            tag_name="text",
            description="",
            properties=[
                ChildProperty(name="children",description="", child_tag_names=["child"]),
                ValueProperty(name="hello",description="", type="string"),
            ]
        )
    ]
)


def test_runtime():

    with runtime.DocumentRuntime() as rt:

        doc = rt.new_document(schema=schema)
        element = doc.root.append_child("child", { "hello" : "world" })
        e2 = element.append_child("child", { "hi" : "there" })
        e2.append_child("child", { "hello" : "hi" })
        element["test"] = "test2"
        element._remove_attribute("test")
        #text = element.create_child_element("text", { "hi" : "there" })
        #text.get_children()[0].insert(0, "hello world")

def test_set_attribute():
   
    with runtime.DocumentRuntime() as rt:

        client = rt.new_document(schema=schema)
        client.root["test"] = "v1"
        assert client.root["test"] == "v1"


def test_insert_and_delete_element():
   
    with runtime.DocumentRuntime() as rt:

        client = rt.new_document(schema=schema)
        child = client.root.append_child("child", { "hello" : "world" })
        
        assert isinstance(child, Element) == True
        assert isinstance(child, Element) == True
        assert child.tag_name == "child"
        assert child["hello"] == "world"

        # Can delete node
        child.delete()
        assert len(client.root.get_children()) == 0


def test_update_attribute():
      with runtime.DocumentRuntime() as rt:

        client = rt.new_document(schema=schema)
        child = client.root.append_child("child", { "hello" : "world" })

        # Can update attribute
        child["hello"] = "mod"
        assert child["hello"] == "mod"


def test_remove_attribute():
    with runtime.DocumentRuntime() as rt:

        client = rt.new_document(schema=schema)

        child = client.root.append_child("child", { "hello" : "world" })
        child["hello"] = "mod"

        # Can remove attribute
        child._remove_attribute("hello")
        assert child["hello"] == None


def test_insert_extend_and_shrink_text_delta():

    with runtime.DocumentRuntime() as rt:

        client = rt.new_document(schema=schema)

        # Can insert text
        child = client.root.append_child("text", { "hello" : "world" })
        assert isinstance(child, Element) == True
        
        text = child.get_children()[0]
        
        assert isinstance(text, Text), True
        assert child.tag_name=="text"
        assert child["hello"]=="world"

        assert len(text.delta)==0
        text.insert(0, "hello world")
        assert len(text.delta)==1
        assert text.delta[0]["insert"]=="hello world"

        text.insert(0, "hello world")
        assert len(text.delta)==1
        assert text.delta[0]["insert"]=="hello worldhello world"

        text.delete(len("hello world"), len("hello world"))
        assert len(text.delta)==1
        assert text.delta[0]["insert"]=="hello world"

        text.delete(len("hello world")-1, 1)
        assert len(text.delta)==1
        assert text.delta[0]["insert"]=="hello worl"




def test_format_text_deltas():
    with runtime.DocumentRuntime() as rt:

        client = rt.new_document(schema=schema)

        # Can insert text
        child = client.root.append_child("text", { "hello" : "world" })
        assert isinstance(child,Element)==True
        
        text = child.get_children()[0]
        assert isinstance(text, Text)==True
        assert child.tag_name == "text"
        assert child["hello"]=="world"

        assert len(text.delta)==0
        text.insert(0, "hello world")
        # format whole item
        text.format(0, len("hello world"), { 
            "bold" : True
        })

        assert len(text.delta)==1
        assert text.delta[0]["insert"]=="hello world"
        assert text.delta[0]["attributes"]["bold"]==True

        # format start
        text.format(0, len("hello"), { 
            "italic" : True
        })

        assert len(text.delta)==2
        assert text.delta[0]["insert"]== "hello"
        assert text.delta[1]["insert"]== " world"
        assert text.delta[0]["attributes"]["bold"]==True
        assert text.delta[0]["attributes"]["italic"]==True
        assert text.delta[1]["attributes"]["bold"]==True
        assert "italic" not in text.delta[1]["attributes"]

        # format end
        text.format(3, 2, { 
            "underline" : True
        })

        assert len(text.delta)==3
        assert text.delta[0]["insert"]=="hel"
        assert text.delta[1]["insert"]=="lo"
        assert text.delta[2]["insert"]==" world"

        assert text.delta[0]["attributes"]["bold"]==True
        assert text.delta[0]["attributes"]["italic"]==True
        assert "underline" not in text.delta[0]["attributes"]

        assert text.delta[1]["attributes"]["bold"]==True
        assert text.delta[1]["attributes"]["italic"]==True
        assert text.delta[1]["attributes"]["underline"]==True

        assert text.delta[2]["attributes"]["bold"]==True
        assert "italic" not in text.delta[2]["attributes"]
        assert "underline" not in text.delta[2]["attributes"]


        # format across items
        text.format(0, len("hello world"), { 
            "strikethrough" : True
        })

        assert len(text.delta)==3
        assert text.delta[0]["insert"]=="hel"
        assert text.delta[1]["insert"]=="lo"
        assert text.delta[2]["insert"]==" world"

        assert text.delta[0]["attributes"]["bold"]==True
        assert text.delta[0]["attributes"]["italic"]==True
        assert "underline" not in text.delta[0]["attributes"]
        assert text.delta[0]["attributes"]["strikethrough"]==True

        assert text.delta[1]["attributes"]["bold"]==True
        assert text.delta[1]["attributes"]["italic"]==True
        assert text.delta[1]["attributes"]["underline"]==True
        assert text.delta[1]["attributes"]["strikethrough"]==True

        assert text.delta[2]["attributes"]["bold"]==True
        assert "italic" not in text.delta[2]["attributes"]
        assert "underline" not in text.delta[2]["attributes"]
        assert text.delta[2]["attributes"]["strikethrough"]==True

        # format across items
        text.format(1, 1, { 
            "dot" : True
        })

        assert len(text.delta)==5
        assert text.delta[0]["insert"]=="h"
        assert text.delta[1]["insert"]=="e"
        assert text.delta[2]["insert"]=="l"

def test_delete_start_of_delta_text():

    with runtime.DocumentRuntime() as rt:

        client = rt.new_document(schema=schema)

        # Text Delete
        client.root.append_child("text", { "hello" : "world" })
        assert len(client.root.get_children())==1
        child = client.root.get_children()[0]

        # Delete start
        assert isinstance(child,Element)==True
        text = child.get_children()[0]
        assert isinstance(text, Text)==True
        text.insert(0, "hello world")
        assert len(text.delta)==1
        assert text.delta[0]["insert"]=="hello world"
        text.delete(0, len("hello "))
        assert len(text.delta)==1
        assert text.delta[0]["insert"]=="world"

def test_delete_end_of_delta_text():

    with runtime.DocumentRuntime() as rt:

        client = rt.new_document(schema=schema)
       
        # Text Delete
        client.root.append_child("text", { "hello" : "world" })
        assert len(client.root.get_children())==1
        child = client.root.get_children()[0]
        text = child.get_children()[0]
        text.insert(0, "world")

        # Delete end
        text.delete(len("world") - 1, 1)
        assert len(text.delta)==1
        assert text.delta[0]["insert"]=="worl"

def test_delete_center_of_delta_text():

    with runtime.DocumentRuntime() as rt:

        client = rt.new_document(schema=schema)
        client.root.append_child("text", { "hello" : "world" })
        assert len(client.root.get_children())==1
        child = client.root.get_children()[0]
        text = child.get_children()[0]
        text.insert(0, "worl")

        # Delete center
        text.delete(2, 1)
        assert len(text.delta)==1
        assert text.delta[0]["insert"]=="wol"


def test_insert_elements_at_positions():
    with runtime.DocumentRuntime() as rt:

        client = rt.new_document(schema=schema)
        
        # Inserts at end
        client.root.append_child("child", { "hello" : "world2" })

        child = client.root.get_children()[0]
        assert child.tag_name=="child"
        assert child["hello"]=="world2"

        # Inserts deep
        child.append_child("child", { "hello" : "world3" })

        deepChild = child

        child = child.get_children()[0]
        assert child.tag_name=="child"
        assert child["hello"]=="world3"

        # Inserts after deep
        child.parent.append_child("child", { "hello" : "world4" })

        child = deepChild.get_children()[1]
        assert child.tag_name=="child"
        assert child["hello"] == "world4"

        # Inserts after element deep
        deepChild.insert_child_after(deepChild.get_children()[0], "child", { "hello" : "world5" })

        child = deepChild.get_children()[1]
        assert child.tag_name=="child"
        assert child["hello"] == "world5"


        # Inserts after element deep
        deepChild.insert_child_at(2, "child", { "hello" : "world6" })

        child = deepChild.get_children()[2]
        assert child.tag_name=="child"
        assert child["hello"] == "world6"

        logger.info(child)

