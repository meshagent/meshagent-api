import uuid
from typing import Callable, Any, Optional
import json
from meshagent.api.schema import MeshSchema, ElementType, ChildProperty, ValueProperty

import logging
logger = logging.getLogger("schema_document")
logger.setLevel(logging.INFO)


def tag_name_from_json(json: dict) -> str:
    if len(json.keys()) != 1:
            raise Exception("JSON element must have a single key")
    
    for k, v in json.items():
        return k
    
    raise Exception("JSON element must have a single key")
    

def attributes_from_json(json:dict):
    if len(json.keys()) != 1:
        raise Exception("JSON element must have a single key")
   
    for k, v in json.items():
        attributes : dict = json[k].copy()
        return attributes
    
    raise Exception("JSON element must have a single key")

class EventEmitter:
    def __init__(self):
        # A dictionary mapping event names to lists of handler functions
        self._events = {}

    def on(self, event_name):
        """Decorator to register a function as a handler for a given event."""
        def decorator(func):
            if event_name not in self._events:
                self._events[event_name] = []
            self._events[event_name].append(func)
            return func
        return decorator

    def emit(self, event_name, *args, **kwargs):
        """Call all handlers associated with the given event."""
        handlers = self._events.get(event_name, [])
        for handler in handlers:
            handler(*args, **kwargs)


class Element(EventEmitter):
    def __init__(
        self,
        parent: 'Element | None',
        data: dict,
        doc : 'Document',
        schema : ElementType,
    ):
        super().__init__()

        self._data = { 
            "tagName": data["tagName"], 
            "attributes": data["attributes"],
            "children": [],
        }
        self._parent = parent
        self._doc = doc
        self._schema = schema


    @property
    def doc(self) -> 'Document':
        return self._doc
    

    def get_node_by_id(self, id:str) -> 'Element | None':
        if(id == self.id):
            return self
        
        for child in self.get_children():
            if isinstance(child, Element):
                n = child.get_node_by_id(id)
                if n != None:
                    return n
        
        return None

    @property
    def id(self) -> str | None:
        return self["$id"]

    @property
    def tag_name(self) -> str:
        return self._data["tagName"]
    
    @property
    def parent(self) -> 'Element | None':
        return self._parent

    @property
    def schema(self) -> ElementType:
        return self._schema

    def set_attribute(self, name: str, value: Any):
        self._data["attributes"][name] = value
    
    def get_attribute(self, name: str, default: Optional[Any] = None):
         if name in self._data["attributes"]:
            return self._data["attributes"][name]
         return default
    
    def __getitem__(self, name: str):
        if name in self._data["attributes"]:
            return self._data["attributes"][name]
        return None
    

    def __setitem__(self, name: str, value) -> None:
        # make sure prop exists
        prop = self._schema.property(name)

        if isinstance(prop, ChildProperty):
            raise Exception("you must add a child element to set values for the property {name}".format(name=prop.name))

        # todo: should we aggressively validate data type?
        
        self.doc._broadcast_changes([{
            "nodeID": self.id,
            "setAttributes": {
                name: value
            }
        }])

    def _remove_attribute(self, name: str) -> None:
        self.doc._broadcast_changes([{ 
            "nodeID": self.id,
            "removeAttributes": [name],
        }])
    
    def _ensure_child_valid(self, tag_name: str) -> ElementType:
        if self._schema.child_property_name == None:
            raise Exception("children are not allowed on this element")
        
        child_schema = self._schema.property(self._schema.child_property_name)
        cp : ChildProperty = child_schema
        
        if cp.is_tag_allowed(tag_name=tag_name) == False:
            raise Exception("cannot add {tag_name} to {self_tag_name}".format(tag_name=tag_name, self_tag_name=self.tag_name))
        
        return self._doc._schema.element(tag_name)

    def append_json(self, json: dict) -> 'Element':
        tag_name = tag_name_from_json(json)

        attributes = attributes_from_json(json)

        element_type = self.doc.schema.element(tag_name)

        if element_type.child_property_name != None:
            if element_type.child_property_name in attributes:
                children = attributes.pop(element_type.child_property_name)
                element = self.append_child(tag_name=tag_name, attributes=attributes)
                
                for child in children:
                    element.append_json(child)

                return element

        else:
            return self.append_child(tag_name=tag_name, attributes=attributes)

    def append_child(self, tag_name:str, attributes:dict = {}) -> 'Element':

        child_type = self._ensure_child_valid(tag_name=tag_name)
        
        for k, v in attributes.items():
            prop = child_type.property(k)

        elementData = {
            "name" : tag_name,
            "attributes" : {
                "$id" : str(uuid.uuid4()),
            },
            "children" : self._defaultChildren(tag_name),
        }

        for k,v in attributes.items():
            elementData["attributes"][k] = v
        
        self.doc._broadcast_changes([{
            "nodeID": self.id,
            "insertChildren": {
                "children": [
                    {
                        "element" : elementData
                    }
                ]
            }
        }])
        return self.get_node_by_id(elementData["attributes"]["$id"])
    

    def insert_child_at(self, index: int, tag_name: str, attributes: dict) -> 'Element':

        child_type = self._ensure_child_valid(tag_name=tag_name)

        for k, v in attributes.items():
            prop = child_type.property(k)

        elementData = {
            "name" : tag_name,
            "attributes" : {
                "$id" : str(uuid.uuid4()),
            },
            "children" : self._defaultChildren(tag_name),
        }

        for k,v in attributes.items():
            elementData["attributes"][k] = v
        
        self.doc._broadcast_changes([{
            "nodeID": self.id,
            "insertChildren": {
                "index" : index,
                "children": [
                    {
                        "element": elementData,
                    }
                ]
            }
        }])
        return self.get_node_by_id(elementData["attributes"]["$id"])


    def insert_child_after(self, element : 'Element', tag_name: str, attributes: dict) -> 'Element':

        child_type = self._ensure_child_valid(tag_name=tag_name)

        for k, v in attributes.items():
            prop = child_type.property(k)

        if element.parent != None:
            if element.parent.id != self.id:
                raise(Exception("Element does not belong to this node"))
            
        elementData = {
            "name" : tag_name,
            "attributes" : {
                "$id" : str(uuid.uuid4()),
            },
            "children" : self._defaultChildren(tag_name),
        }

        for k,v in attributes.items():
            elementData["attributes"][k] = v
        
        self.doc._broadcast_changes([{
            "nodeID": self.id,
            "insertChildren": {
                "after" : element.id,
                "children": [
                    {
                        "element": elementData,
                    }
                ]
            }
        }])
        return self.get_node_by_id(elementData["attributes"]["$id"])
    

    def _defaultChildren(self, tag_name:str) -> None:
        if tag_name == "text":
            return [{
                "text" : {
                    "delta" : []
                }
            }]
        
        return []

    def delete(self) -> None:
        self.doc._broadcast_changes([{
            "nodeID": self.id,
            "delete": {
            }
        }])

    def get_children(self) -> list['Element | Text']:
        return self._data["children"]
    

    def to_json(self, include_ids : bool = False) -> dict:
        props = dict()

        for k, v in self._data["attributes"].items():
            if k != "$id" or include_ids:
                props[k] = v

        if self._schema.child_property_name != None:
            props[self._schema.child_property_name] = list(map(lambda x: x.to_json(include_ids=include_ids), self.get_children()))

        return {
            self.tag_name: props
        }


class Text(EventEmitter):
    def __init__(self, parent : Element, data: dict, doc: 'Document'):
        super().__init__()
        
        self._data = data
        self.parent = parent
        self.doc = doc

    @property
    def delta(self) -> list:
        return self._data["delta"]
    

    def insert(self, index:int, text:str, attributes: dict | None = None) -> None:
        self.doc._broadcast_changes([{
            "nodeID" : self.parent.id,
            "insertText" : {
                "index" : index,
                "text" : text,
                "attributes" : attributes,
            }
        }])

    def format(self, start:int, length:int, attributes:dict) -> None:
        self.doc._broadcast_changes([{
            "nodeID" :  self.parent.id,
            "formatText" : {
                "from" : start,
                "length" : length,
                "attributes" : attributes,
            }
        }])
    

    def delete(self, index:int, length:int) -> None:
        self.doc._broadcast_changes([{
            "nodeID" :  self.parent.id,
            "deleteText" : {
                "index" : index,
                "length" : length,
            }
        }])

def str_slice(s:str, start:int, end: int | None = None) -> str:
    if end == None:
        return s[start:]
    
    return s[start:end]

def splice(l:list, start:int, count: int, item = None) -> list:
    removed = []
    for i in range(count):
        removed.append(l.pop(start))
    
    if item != None:
        l.insert(start, item)

    return removed
    

class Document(EventEmitter): 
    def __init__(self, schema: MeshSchema, broadcast_changes : Callable, json: dict | None = None):
        super().__init__()
        
        self._schema = schema
        self._broadcast_changes = broadcast_changes
        
        if json != None: 
            
            self._root = Element(None, { "tagName": schema.root.tag_name, "attributes": {}, "children": [] }, self, schema.root)
            
            for k,v in json[schema.root.tag_name].items():

                if self._root.schema.child_property_name == k:
                    for c in json[schema.root.tag_name][k]:
                        self._root.append_json(c)

                else:
                    self._root[k] = v
   
        else:
            self._root = Element(None, { "tagName": schema.root.tag_name, "attributes": {}, "children": [] }, self, schema.root)

    @property
    def schema(self):
        return self._schema

    @property
    def root(self) -> Element:
        return self._root
    
    def to_json(self):
        return self._root.to_json()

    def _createNode(self, parent: Element, data: dict) -> None:
        parent_schema = parent.schema
        if "element" in data:
            tag_name = data["element"]["tagName"]
            element_type = self._schema.element(tag_name)

            element = Element(parent, data["element"], self, schema=element_type)
            if data["element"]["children"]:
                for child in data["element"]["children"]:
                    node = self._createNode(element, child)
                    element._data["children"].append(node)
                    element.emit("inserted", node)
                    self.emit("inserted", node)
            return element
        elif "text" in data:
            return Text(parent, data["text"], self)
        else:
            raise Exception("Unsupported "+json.dumps(data))
    

    def receive_changes(self, message:dict) -> None:
        nodeID = None
        if "target" in message:
            nodeID = message["target"]

        target:Element
        
        if "root" in message and message["root"]:
            target = self.root
        else:
            target = self.root.get_node_by_id(nodeID)

        # process element deltas
        retain = 0
        if "elements" in message:
            for delta in message["elements"]:
                if "retain" in delta:
                    retain += delta["retain"]
                
                if "insert" in delta:
                    for insert in delta["insert"]:
                        if "element" in insert:
                            node = self._createNode(target, insert)
                            splice(target._data["children"], retain, 0, node)
                            target.emit("inserted", node)
                            self.emit("inserted", node)
                            retain+=1
                        elif "text" in insert:
                            node = self._createNode(target, insert)
                            splice(target._data["children"], retain, 0, node)
                            target.emit("inserted", node)
                            self.emit("inserted", node)
                            retain+=1
                        else:
                            raise Exception("Unsupported element delta")
        
                elif "delete" in delta:
                    removed = splice(target._data["children"], retain, delta["delete"])
                    for r in removed:
                        target.emit("deleted", r)
                        self.emit("deleted", r)
                    retain -= delta["delete"]
            

        # process text deltas
        if "text" in message and len(message["text"]) != 0:
            
            if target.tag_name != "text":
                raise Exception("Node is not a text node: "+target.tag_name)     
    
            textNode = target._data["children"][0]
            retain = 0
            i = 0
            offset = 0

            if "delta" not in textNode._data:
                raise(Exception("Text node is missing delta"))


            targetDelta : list = textNode._data["delta"]
           
            for delta in message["text"]:
               
                if "insert" in delta:
                    if i == len(targetDelta):
                        attr = {}
                        if "attributes" in delta:
                            attr = delta["attributes"]

                        targetDelta.append({
                            "insert": delta["insert"],
                            "attributes": attr
                        })

                        i+=1
                        offset += len(delta["insert"])
                        retain += len(delta["insert"])
                    else:
                        str_insert = targetDelta[i]["insert"]
                        targetDelta[i]["insert"] = str_slice(str_insert, 0, retain - offset) + delta["insert"] + str_slice(str_insert, retain - offset)
                        retain += len(delta["insert"])
                    
                elif "delete" in delta:
                    deleted = 0
                    while delta["delete"] > deleted:
                        remaining = delta["delete"] - deleted
                    
                        # delete ends after item
                        if retain > offset:
                            # delete end
                            str_insert = targetDelta[i]["insert"]
                            start = str_slice(str_insert, 0, retain - offset)
                            end =  str_slice(str_insert, retain - offset)

                            if remaining >= len(end):
                                targetDelta[i]["insert"] = start
                                deleted += len(end)
                                i+=1
                                offset += len(str_insert)
                            else:
                                targetDelta[i]["insert"] = start + str_slice(end, remaining)
                                deleted += len(targetDelta[i]["insert"])
                            

                            # TODO? increase retain?
                        elif delta["delete"] - deleted >= len(targetDelta[i]["insert"]):
                            deleted += len(targetDelta[i]["insert"])
                            offset += splice(targetDelta, i, 1)
                        else:
                            # delete ends inside item, delete front
                            str_insert = targetDelta[i]["insert"]
                            start = str_insert[0:remaining]
                            end = str_slice(str_insert, remaining)
                            targetDelta[i]["insert"] = end
                            deleted += len(start)
                elif "attributes" in delta:
                    formatted = 0
                    while(delta["retain"] > formatted):
                        # format ends after item
                        remaining = delta["retain"] - formatted

                        if retain > offset:
                            # format end
                            str_insert = targetDelta[i]["insert"]
                            start = str_slice(str_insert, 0, retain - offset)
                            end =  str_slice(str_insert, retain - offset)
                            
                            
                            if remaining >= len(end):
                                targetDelta[i]["insert"] = start
                                attributes = dict()
                                for k,v in targetDelta[i]["attributes"].items():
                                    attributes[k] = v

                                for k,v in delta["attributes"].items():
                                    attributes[k] = v
                                    
                                splice(targetDelta, i+1, 0, { "insert": end, "attributes": attributes })

                                formatted += len(end)
                                # move to next item
                                i+=1
                                i+=1
                                offset += len(str_insert)
                            else:
                                targetDelta[i]["insert"] = start

                                attributes = dict()
                                for k,v in targetDelta[i]["attributes"].items():
                                    attributes[k] = v

                                other_attributes = attributes.copy()
                                for k,v in delta["attributes"].items():
                                    attributes[k] = v

                                splice(targetDelta, i+1, 0, { "insert": str_slice(end, 0, remaining), "attributes": attributes })
                                splice(targetDelta, i+2, 0, { "insert": str_slice(end, remaining), "attributes": other_attributes })

                                formatted += remaining
                                i+=1
                                i+=1
                                i+=1
                                offset += len(start) + remaining

                        elif delta["retain"] - formatted >= len(targetDelta[i]["insert"]): 
                            formatted += len(targetDelta[i]["insert"])
                            
                            # format whole item
                            for k, v in delta["attributes"].items():
                                targetDelta[i]["attributes"][k] = v
        
                            offset += len(targetDelta[i]["insert"])
                            i+=1
                        else:
                            # format ends inside item, format front
                            str_insert = targetDelta[i]["insert"]
                            start = str_slice(str_insert, 0, remaining)
                            end = str_slice(str_insert, remaining)
                            targetDelta[i]["insert"] = start

                            attributes = dict()
                            for k,v in targetDelta[i]["attributes"].items():
                                attributes[k] = v

                            targetDelta.append({ "insert": end, "attributes": attributes})
                            for k,v in delta["attributes"].items():
                                targetDelta[i]["attributes"][k] = v
                        
                            formatted += (delta["retain"] - formatted)
                    retain += delta["retain"]
                elif delta["retain"]:
                    if delta["retain"]:
                        retain += delta["retain"]
                    
                    if "insert" in targetDelta[i]:
                        while retain > offset + len(targetDelta[i]["insert"]):
                            offset += len(targetDelta[i]["insert"])
                            i+=1
             

        for change in message["attributes"]["set"]:
            target._data["attributes"][change["name"]] = change["value"]
            target.emit("updated", target, change["name"])
            self.emit("updated", target, change["name"])
        

        for name in message["attributes"]["delete"]:
            target._data["attributes"].pop(name)
            target.emit("updated", target, name)
            self.emit("updated", target, name)
        
 