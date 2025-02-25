import json
import logging

from meshagent.api.schema import MeshSchema, ElementType, SimpleValue,  ChildProperty, ValueProperty, MeshSchemaValidationException
from jsonschema import validate, ValidationError

logger = logging.getLogger(__name__)


def test_schema_validates_tag_names():
    try:
        s = MeshSchema(root_tag_name="sample2",
                elements=[
                    ElementType(
                        tag_name="sample",
                        description="test",
                        properties=[
                        ]
                    )
                ]
            )
        
        raise Exception("schema did not prevent extra prop")
    except MeshSchemaValidationException as e:
        pass


def test_schema_validates_value_names():
    try:
        s = MeshSchema(root_tag_name="sample2",
                elements=[
                    ElementType(
                        tag_name="sample",
                        description="test",
                        properties=[
                            ValueProperty(name="string", description="", type="bad")
                        ]
                    )
                ]
            )
        
        raise Exception("schema did not prevent extra prop")
    except MeshSchemaValidationException as e:
        pass


def test_schema_validates_child_tag_names():
    try:
        s = MeshSchema(root_tag_name="sample2",
                elements=[
                    ElementType(
                        tag_name="sample",
                        description="test",
                        properties=[
                            ChildProperty(name="children", description="", child_tag_names=["blah"])
                        ]
                    )
                ]
            )
        
        raise Exception("schema did not prevent extra prop")
    except MeshSchemaValidationException as e:
        pass


def test_schema_requires_properties():

    s = MeshSchema(root_tag_name="sample",
            elements=[
                ElementType(
                    tag_name="sample",
                    description="test",
                    properties=[
                        ValueProperty(
                            name="prop",
                            description="desc",
                            type="number"
                        )
                    ]
                )
            ]
        )

    schema = s.to_json()

    validate({        
        "sample" : { "prop" : 1  }   
    }, schema)

    try:
        validate({        
            "smple" : { "test" : 1 },
            "sample" : 1,
        }, schema)

        raise Exception("schema did not prevent extra prop")
    except ValidationError as e:
        pass


    try:
        validate({           
        }, schema)

        raise Exception("schema did not prevent missing prop")
    except ValidationError as e:
        pass




def test_nested_schema_object():

    s = MeshSchema(root_tag_name="sample",
        elements=[
            ElementType(
                tag_name="sample",
                description="test",
                properties=[
                    ValueProperty(name="sample2",
                        description="desc",
                        type="number"
                    )
                ]
            )
        ])

    schema = s.to_json()

    validate({        
        "sample" : {
            "sample2" : 1
        }
    }, schema)

    try:
        validate({        
            "sample" : {
                "sample2" : "test"
            }
        }, schema)

        raise Exception("schema did not prevent bad prop")
    except ValidationError as e:
        pass
    


def test_nested_array_values():

    s = MeshSchema(
        root_tag_name="sample",
        elements=[
            ElementType(
                tag_name="sample",
                description="test",
                properties=[
                    ChildProperty(
                        name="children",
                        description="desc",
                        child_tag_names=[
                            "string_tag"
                        ]
                    )
                ]
            ),
            ElementType(
                tag_name="string_tag",
                description="",
                properties=[
                    ValueProperty(name="value", description="", type="string")
                ]
            )
        ])

    schema = s.to_json()
  
    validate({        
        "sample" : { "children" : [ { "string_tag" : { "value" : "test" }}] }
    }, schema)

    try:
        validate({        
             "sample" : { "children" : {} }
        }, schema)

        raise Exception("schema did not prevent bad prop")
    except ValidationError as e:
        pass

def test_nested_array_objects():

    s = MeshSchema(
        root_tag_name="sample",
        elements=[
            ElementType(
                description="test",
                tag_name="sample",
                properties=[
                    ChildProperty(name="children", description="desc", child_tag_names=[
                        "sample2"
                    ])
                ]
            ),
            ElementType(tag_name="sample2", description="desc2", properties=[
                ValueProperty(name="prop", description="desc", type="number")
            ])
        ])

    schema = s.to_json()
    
    validate({        
        "sample" : { "children" : [{"sample2" : { "prop" : 1 } }] }
    }, schema)

    try:
        validate({        
            "sample" : { "children" : [{}] }
        }, schema)

        raise Exception("schema did not prevent bad prop")
    except ValidationError as e:
        pass

    try:
        validate({        
            "sample" : {}
        }, schema)

        raise Exception("schema did not prevent bad prop")
    except ValidationError as e:
        pass
    


def test_nested_array_multi_objects():

    s = MeshSchema(
        root_tag_name="sample",
        elements=[
            ElementType(
                description="test",
                tag_name="sample",
                properties=[
                    ChildProperty(name="children", description="desc", child_tag_names=[
                        "child1",
                        "child2"
                    ]
                )]),

             ElementType(tag_name="child1", description="child", properties=[ValueProperty(name="prop", description="desc", type="number")]),
             ElementType(tag_name="child2", description="child", properties=[ValueProperty(name="prop", description="desc", type="string")]),
        ]
    )

    schema = s.to_json()

    validate({        
        "sample" : { "children" : [{"child1" : { "prop" : 1  }}, {"child2" : { "prop" : "test" } }] }
    }, schema)

    try:
        validate({        
            "sample" : { "children" : [{"child1":"test"}] }
        }, schema)

        raise Exception("schema did not prevent bad prop")
    except ValidationError as e:
        pass

    try:
        validate({        
            "sample" : {}
        }, schema)

        raise Exception("schema did not prevent bad prop")
    except ValidationError as e:
        pass


def test_nested_array_multi_objects_ordered():

    s = MeshSchema(
        root_tag_name="sample",
        elements=[
            ElementType(
                description="test",
                tag_name="sample",
                properties=[
                    ChildProperty(name="children", description="desc",
                    ordered=True,
                    child_tag_names=[
                        "child1",
                        "child2"
                    ]
                )]),

             ElementType(tag_name="child1", description="child", properties=[ValueProperty(name="prop", description="desc", type="number")]),
             ElementType(tag_name="child2", description="child", properties=[ValueProperty(name="prop", description="desc", type="string")]),
        ]
    )

    schema = s.to_json()

    validate({        
        "sample" : { "children" : [{"child1" : { "prop" : 1  }}, {"child2" : { "prop" : "test" } }] }
    }, schema)

    try:
        validate({        
        "sample" : { "children" : [{"child2" : { "prop" : 1  }}, {"child1" : { "prop" : "test" } }] }
        }, schema)

        raise Exception("schema did not prevent bad order")
    except ValidationError as e:
        pass

    try:
        validate({        
            "sample" : { "children" : [{"child1":"test"}] }
        }, schema)

        raise Exception("schema did not prevent bad prop")
    except ValidationError as e:
        pass

    try:
        validate({        
            "sample" : {}
        }, schema)

        raise Exception("schema did not prevent bad prop")
    except ValidationError as e:
        pass 

def test_roudntrip_schema_json():

    s = MeshSchema(
        root_tag_name="sample",
        elements=[
            ElementType(
                description="test",
                tag_name="sample",
                properties=[
                    ChildProperty(name="children", description="desc", child_tag_names=[
                        "child1",
                        "child2"
                    ]
                )]),

             ElementType(
                description="test",
                tag_name="sample2",
                properties=[
                    ChildProperty(name="children", description="desc", 
                        ordered=True,
                        child_tag_names=[
                        "child1",
                        "child2"
                    ]
                )]),

             ElementType(tag_name="child1", description="child", properties=[ValueProperty(name="prop", description="desc", type="number")]),
             ElementType(tag_name="child2", description="child", properties=[ValueProperty(name="prop", description="desc", type="string")]),
        ]
    )

    json1 = s.to_json()

    logger.info(json.dumps(json1))

    s2 = MeshSchema.from_json(json1)

    json2 = s2.to_json()

    assert json.dumps(json1) == json.dumps(json2)
