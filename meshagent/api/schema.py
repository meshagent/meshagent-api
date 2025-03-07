from enum import Enum
from typing import Optional
from abc import ABC, abstractmethod
import logging
from typing import Literal, get_args

logger = logging.getLogger("schema")
logger.setLevel(logging.INFO)

class MeshSchemaException(Exception):
    pass

class MeshSchemaValidationException(MeshSchemaException):
    pass

# a basic value type
SimpleValue = Literal[
    "number",
    "string",
    "null",
    "boolean"
]


# defines a schema that can be used to represent a document, all schemas must have a single root object
class MeshSchema:
    def __init__(self, *, root_tag_name: str, elements: list['ElementType']):
        self._root_tag_name = root_tag_name
        self._elements = elements
        self.elements_by_tag_name = dict[str, 'ElementType']()
        for t in elements:
            if t.tag_name in self.elements_by_tag_name:
                raise MeshSchemaValidationException("{tag_name} was found more than once in tags".format(tag_name=t.tag_name))
            self.elements_by_tag_name[t.tag_name] = t

        if root_tag_name not in self.elements_by_tag_name:
            raise MeshSchemaValidationException("{root_tag_name} was not found in tags".format(root_tag_name=root_tag_name))
        
        self.validate()

    @staticmethod
    def from_json(json: dict):
        
        elements = []

        root_tag_ref :str = json["$root_tag_ref"]
        root_tag_name = root_tag_ref.removeprefix("#/$defs/")

        for _, element_json in json["$defs"].items():
            
            element = ElementType.from_json(element_json)
            elements.append(element)

        return MeshSchema(root_tag_name=root_tag_name, elements=elements)
        
    def to_json(self) -> dict:
        defs = {}
        for t in self.elements:
            defs[t.tag_name] = t.to_json()

        return  {
            "$root_tag_ref" : "#/$defs/"+self._root_tag_name,
            **self.root.to_json(), #["properties"][self.root.tag_name],
            "$defs" : defs,
        }

    def element(self, name: str):
        if name not in self.elements_by_tag_name:
            raise MeshSchemaException("An element with the name '{name}' is not defined in schema".format(name=name))

        return self.elements_by_tag_name[name]

    def validate(self):
        for e in self.elements:
            e.validate(self)

    @property
    def root(self):
        if self._root_tag_name not in self.elements_by_tag_name:
            raise MeshSchemaException("The root element is not defined in schema '{name}'".format(name=self._root_tag_name))
        
        return self.elements_by_tag_name[self._root_tag_name]
     
    @property
    def elements(self):
        return self._elements

        

# defines the schema for a property
class ElementProperty(ABC):
    def __init__(self, name:str,  description: Optional[str] = None):
        self._name = name
        self._description = description
    
    @abstractmethod
    def validate(self, schema: MeshSchema):
        pass

    @property
    def name(self):
        return self._name
    
    @property
    def description(self):
        return self._description
    
    @abstractmethod
    def to_json(self) -> dict:
        pass

# defines a schema for a simple value type
class ValueProperty(ElementProperty):
    def __init__(self, *, name:str, type: SimpleValue, description:str|None = None, enum : list | None = None):
        super().__init__(name=name, description=description)
        self._type = type
        self._enum = enum
    
    def validate(self, schema: MeshSchema):
        if self._type not in get_args(SimpleValue):
            raise(MeshSchemaValidationException("Invalid value type: "+self._type))

    @property
    def enum(self):
        return self._enum
    

    @property
    def type(self):
        return self._type
    

    def to_json(self) -> dict:
        if self._enum != None:
            prop = {
                "type": self.type,
                "enum" : self._enum,
            }
        else:
            prop =  {
                "type": self.type,
            }

        if self.description != None:
             prop["description"] = self.description

        return  {
            self.name : prop
        }
        


# defines a schema for an array, the array can contain any type matching the items of the schema
class ChildProperty(ElementProperty):
    def __init__(self, *, name:str, child_tag_names:list[str], description:str|None=None, ordered:bool=False):
        super().__init__(name=name, description=description)
        self._child_tag_names = child_tag_names
        self._ordered = ordered

    def validate(self, schema: MeshSchema):
        for item in self._child_tag_names:
            if isinstance(schema.element(item), ElementType):
                pass
            else:
                raise(MeshSchemaValidationException("element child types must me element types"))
    
    def is_tag_allowed(self, tag_name: str) -> bool:
        return tag_name in self._child_tag_names


    @property
    def ordered(self):
        return self._ordered
    
    @property
    def child_tag_names(self):
        return self._child_tag_names

    def to_json(self) -> dict:
        if self._ordered:
            prop = {
                "type" : "array",
                "prefixItems" : [
                        *map(lambda p:  { "$ref" : "#/$defs/"+p }, self.child_tag_names)
                ],
                "items" : False,
            }
        else:
            prop = {
                "type" : "array",
                "items" : {
                    "anyOf": [
                        *map(lambda p:  { "$ref" : "#/$defs/"+p }, self.child_tag_names)
                    ]
                }
            }
        
        if self.description != None:
             prop["description"] = self.description

        return  {
            self.name : prop
        }
        
# defines the schema for an object. all properties must be required.
class ElementType:
    def __init__(self, *, tag_name: str, properties:list[ElementProperty], description: Optional[str] = None):
        self._tag_name = tag_name
        self._properties = properties.copy()
        self._description = description
        self._property_lookup = dict[str,ElementProperty]()
        
        self._child_property_name = None
        for p in properties:
            if isinstance(p, ChildProperty):
                if self._child_property_name != None:
                    raise MeshSchemaValidationException("Only one child property is allowed")
                self._child_property_name = p.name

            if p.name in self._property_lookup:
                raise MeshSchemaValidationException("Duplicate property "+p.name)
            
            self._property_lookup[p.name] = p

    @staticmethod
    def from_json(json: dict):
        description = None
        if "description" in json:
            description = json["description"]
      
        for k, type_json in json["properties"].items():
      
            tag_name = k
            properties = []

            for prop_name, p in type_json["properties"].items():

                if p["type"] == "array":
                    
                    child_tag_names = []

                    if "items" in p and isinstance(p["items"],dict) and "anyOf" in p["items"]:
                        for i in p["items"]["anyOf"]:

                            ordered = False
                            
                            child_tag_ref = i["$ref"]
                            child_tag_name = child_tag_ref.removeprefix("#/$defs/")

                            child_tag_names.append(child_tag_name)
                    elif "prefixItems" in p:
                         for i in p["prefixItems"]:

                            ordered = True
                            
                            child_tag_ref = i["$ref"]
                            child_tag_name = child_tag_ref.removeprefix("#/$defs/")

                            child_tag_names.append(child_tag_name)
                    else:
                        raise Exception("Unexpected list type")

                    
                    pdesc = None
                    if "description" in p:
                        pdesc = p["description"]
                    
                    properties.append(
                        ChildProperty(
                            name=prop_name, 
                            description=pdesc,
                            child_tag_names=child_tag_names,
                            ordered=ordered
                        )
                    )

                else:
                    pdesc = None
                    if "description" in p:
                        pdesc = p["description"]
                    properties.append(ValueProperty(name=prop_name, description=pdesc, type=p["type"]))
            
            return ElementType(tag_name=tag_name, description=description, properties=properties)
        
        raise MeshSchemaValidationException("Invalid schema json")
    
    def to_json(self) -> dict:
        
        props = dict()
        required = []
        for p in self.properties:
            j = p.to_json()
            for k in j.keys():
                required.append(k)
                if k not in props:
                    props[k] = j[k]
                else:
                    raise MeshSchemaValidationException("duplicate key in schema: "+k)
                

        type =  {
            "type" : "object",
            "additionalProperties" : False,
            "required" :  [ self.tag_name ],
            "properties" : {
                self.tag_name: {
                    "type" : "object",
                    "additionalProperties" : False,
                    "required" : required,
                    "properties" : props,
                }
            }
        }

        if self.description != None:
            type["description"] = self.description

        return type

    def validate(self, schema: MeshSchema):
        for p in self.properties:
            p.validate(schema=schema)

    @property
    def child_property_name(self):
        return self._child_property_name

    @property
    def tag_name(self):
        return self._tag_name

    @property
    def description(self):
        return self._description
    
    @property
    def properties(self):
        return self._properties
    
    def property(self, name):
        if name not in self._property_lookup:
            raise MeshSchemaException(f"{name} is not a valid property on {self.tag_name}")
        return self._property_lookup[name]

