import STPyV8
import json
import uuid
from contextlib import AbstractContextManager
import base64
import logging
from typing import Callable
import secrets

from meshagent.api.schema import MeshSchema
from meshagent.api.schema_document import Document

import os

_js : str
with open(os.path.join(os.path.dirname(__file__), "entrypoint.js"), 'r') as file:
    _js = file.read()


logger = logging.getLogger("document_runtime")
#logger.setLevel(logging.INFO)


random = secrets.SystemRandom()

class DocumentRuntime(AbstractContextManager):
  
    class Global(STPyV8.JSClass):
        def __init__(self, runtime: 'DocumentRuntime'):
            runtime = runtime
            
        def onSendUpdateToBackend(self, value: dict):
            # value is an array of bytes
            logger.info("send to server from runtime %s", value)
            parsed = json.loads(value)                    
            runtime.on_document_sync(document_id=parsed["documentID"], base64=parsed["data"])
            
        def onSendUpdateToClient(self, value: str):
            # value is a string
            logger.info("send to client from runtime %s", value)
            
            parsed = json.loads(value)

            doc = runtime.get_doc(parsed["documentID"])
            doc.receive_changes(parsed["data"])


        def onGetRandomValues(self, width: int, length: int):
            if width == 1:
                vals = [* map(lambda _: random.randrange(0,0xff), range(length)) ]
            elif width == 2:
                vals = [* map(lambda _: random.randrange(0,0xffff), range(length)) ]
            elif width == 4:
                vals = [* map(lambda _: random.randrange(0,0xffffffff), range(length)) ]
            elif width == 8:
                vals = [* map(lambda _: random.randrange(0,0xffffffffffffffff), range(length)) ]
            else:
                raise Exception("Unexpected width {width}".format(width=width))
            
            return vals
    
    def __init__(self):
        self._docs = dict[str,Document]()
        # TODO: Polyfill crypto
       
    def __enter__(self):
        self._v8 = STPyV8.JSContext(DocumentRuntime.Global(runtime=self))
        self._v8.__enter__()
        self._v8.eval(
        '''

        const module = {
            exports: {
            }
        };

        const crypto = {
            getRandomValues(v) {
                let rands = onGetRandomValues(v.BYTES_PER_ELEMENT, v.length);
                for(let i = 0; i < v.length; i++) {
                    v[i] = rands[i];
                }
                return v;
            }
        };

        '''
            +_js)        
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        logger.info("stopping v8")
        self._v8.__exit__(exc_type, exc_value, traceback)
        return None

    def close(self):
        self._v8.close()
        self._v8 = None

    def execute(self, code: str):
        return self._v8.eval(code)

    def get_doc(self, id:str) -> 'RuntimeDocument':
        return self._docs[id]
    
    def new_document(self, schema: MeshSchema, id: str | None = None, data: bytes | None = None, on_document_sync: Callable | None = None, json:dict|None=None, factory: Callable = None) -> 'RuntimeDocument':
        if factory is None:
            factory = RuntimeDocument
        return factory(schema=schema, id=id, data=data, json=json, on_document_sync=on_document_sync)

    def on_document_sync(self, document_id: str, base64: str):        
        doc = self.get_doc(document_id)
        if doc.on_document_sync != None:
            logger.info("publishing backend changes to document %s: %s", document_id, base64)
            doc.on_document_sync(base64)

    def apply_backend_changes(self, document_id: str, base64: str):
        logger.info("applying backend changes to document %s: %s", document_id, base64)
        self.execute("module.exports.applyBackendChanges({id},{base64})".format(id=json.dumps(document_id),base64=json.dumps(base64)))

    def _register_document(self, doc: 'RuntimeDocument', data: bytes | None = None) -> None:
        self._docs[doc.id] = doc
        if data == None:
            self.execute("module.exports.registerDocument({id})".format(id=json.dumps(doc.id)))
        else:
            self.execute("module.exports.registerDocument({id}, {data})".format(id=json.dumps(doc.id), data= json.dumps(base64.standard_b64encode(data).decode("utf-8"))))

    def _unregister_document(self,  doc: 'RuntimeDocument') -> None:
        self.execute("module.exports.unregisterDocument({id})".format(id=json.dumps(doc.id)))
        self._docs.pop(doc.id)

runtime = DocumentRuntime()
runtime.__enter__()

class RuntimeDocument(Document):
    def __init__(self, schema: MeshSchema, on_document_sync: Callable | None, id: str | None = None, data: bytes | None = None, json: dict | None = None):
        if id == None:
            self._id = str(uuid.uuid4())
        else:
            self._id = id
                
        self.on_document_sync = on_document_sync
        
        runtime._register_document(self)
        super().__init__(schema=schema, broadcast_changes=self.send_changes, json=json)
        if data != None:
            runtime.apply_backend_changes(self.id, base64.standard_b64encode(data).decode("utf-8"))

    def get_state(self, vector: bytes | None = None) -> bytes:

        if vector == None:
            base64_state = runtime.execute('''
                module.exports.getState({id});
            '''.format(id=json.dumps(self._id)))
        else:
            base64_state = runtime.execute('''
                module.exports.getState({id}, {vector});
            '''.format(id=json.dumps(self._id), vector=json.dumps(base64.standard_b64encode(vector))))

        return base64.standard_b64decode(base64_state)

    def get_state_vector(self) -> bytes:
        base64_state = runtime.execute('''
            module.exports.getStateVector({id});
        '''.format(id=json.dumps(self._id)))

        return base64.standard_b64decode(base64_state)

    def send_changes(self, changes) -> None:
        changes = {
            "documentID" : self.id,
            "changes" : changes,
        }
        changes_json = json.dumps(changes)
        logger.info("applying changes to document %s: %s", self.id, changes_json)
     
        runtime.execute('''
            module.exports.applyChanges({changes});
        '''.format(changes=changes_json))
        
    @property
    def id(self) -> str:
        return self._id
    
    def close(self):
        runtime._unregister_document(self)