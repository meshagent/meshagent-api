import json
import uuid
from contextlib import AbstractContextManager
import base64
import logging
from typing import Callable
import secrets

from meshagent.api.schema import MeshSchema
from meshagent.api.schema_document import Document
from importlib import resources


_js: str

with resources.files("meshagent.api").joinpath("entrypoint.js").open("r") as f:
    _js = f.read()


logger = logging.getLogger("document_runtime")
#


random = secrets.SystemRandom()

try:
    import STPyV8

    class DocumentRuntime(AbstractContextManager):
        class Global(STPyV8.JSClass):
            def __init__(self, runtime: "DocumentRuntime"):
                runtime = runtime

            def onSendUpdateToBackend(self, value: dict):
                # value is an array of bytes
                logger.debug("send to server from runtime %s", value)
                parsed = json.loads(value)
                runtime.on_document_sync(
                    document_id=parsed["documentID"], base64=parsed["data"]
                )

            def onSendUpdateToClient(self, value: str):
                # value is a string
                logger.debug("send to client from runtime %s", value)

                parsed = json.loads(value)

                doc = runtime.get_doc(parsed["documentID"])
                doc.receive_changes(parsed["data"])

            def onGetRandomValues(self, width: int, length: int):
                if width == 1:
                    vals = [*map(lambda _: random.randrange(0, 0xFF), range(length))]
                elif width == 2:
                    vals = [*map(lambda _: random.randrange(0, 0xFFFF), range(length))]
                elif width == 4:
                    vals = [
                        *map(lambda _: random.randrange(0, 0xFFFFFFFF), range(length))
                    ]
                elif width == 8:
                    vals = [
                        *map(
                            lambda _: random.randrange(0, 0xFFFFFFFFFFFFFFFF),
                            range(length),
                        )
                    ]
                else:
                    raise Exception("Unexpected width {width}".format(width=width))

                return vals

        def __init__(self):
            self._docs = dict[str, Document]()
            # TODO: Polyfill crypto

        def __enter__(self):
            self._v8 = STPyV8.JSContext(DocumentRuntime.Global(runtime=self))
            self._v8.__enter__()
            self._v8.eval(
                """

            const crypto = {
                getRandomValues(v) {
                    let rands = onGetRandomValues(v.BYTES_PER_ELEMENT, v.length);
                    for(let i = 0; i < v.length; i++) {
                        v[i] = rands[i];
                    }
                    return v;
                }
            };

            """
                + _js
            )
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            logger.debug("stopping v8")
            self._v8.__exit__(exc_type, exc_value, traceback)
            return None

        def close(self):
            self._v8.close()
            self._v8 = None

        def execute(self, code: str):
            return self._v8.eval(code)

        def get_doc(self, id: str) -> "RuntimeDocument":
            return self._docs[id]

        def new_document(
            self,
            schema: MeshSchema,
            id: str | None = None,
            data: bytes | None = None,
            on_document_sync: Callable | None = None,
            json: dict | None = None,
            factory: Callable = None,
        ) -> "RuntimeDocument":
            if factory is None:
                factory = RuntimeDocument
            return factory(
                schema=schema,
                id=id,
                data=data,
                json=json,
                on_document_sync=on_document_sync,
            )

        def on_document_sync(self, document_id: str, base64: str):
            doc = self.get_doc(document_id)
            if doc.on_document_sync is not None:
                logger.debug(
                    "publishing backend changes to document %s: %s", document_id, base64
                )
                doc.on_document_sync(base64)

        def apply_backend_changes(self, document_id: str, base64: str):
            logger.debug(
                "applying backend changes to document %s: %s", document_id, base64
            )
            self.execute(
                "meshagent.applyBackendChanges({id},{base64})".format(
                    id=json.dumps(document_id), base64=json.dumps(base64)
                )
            )

        def _register_document(
            self, doc: "RuntimeDocument", data: bytes | None = None
        ) -> None:
            self._docs[doc.id] = doc
            if data is None:
                self.execute(
                    "meshagent.registerDocument({id}, null, false)".format(
                        id=json.dumps(doc.id)
                    )
                )
            else:
                self.execute(
                    "meshagent.registerDocument({id}, {data}, false)".format(
                        id=json.dumps(doc.id),
                        data=json.dumps(
                            base64.standard_b64encode(data).decode("utf-8")
                        ),
                    )
                )

        def _unregister_document(self, doc: "RuntimeDocument") -> None:
            self.execute(
                "meshagent.unregisterDocument({id})".format(id=json.dumps(doc.id))
            )
            self._docs.pop(doc.id)
except ImportError:

    class DocumentRuntime(AbstractContextManager):
        ...

        def __enter__(self): ...

        def __exit__(self, exc_type, exc_value, traceback):
            return None


runtime = DocumentRuntime()
runtime.__enter__()


class RuntimeDocument(Document):
    def __init__(
        self,
        schema: MeshSchema,
        on_document_sync: Callable | None,
        id: str | None = None,
        data: bytes | None = None,
        json: dict | None = None,
    ):
        if id is None:
            self._id = str(uuid.uuid4())
        else:
            self._id = id

        self.on_document_sync = on_document_sync

        runtime._register_document(self)
        super().__init__(schema=schema, broadcast_changes=self.send_changes, json=json)
        if data is not None:
            runtime.apply_backend_changes(
                self.id, base64.standard_b64encode(data).decode("utf-8")
            )

    def get_state(self, vector: bytes | None = None) -> bytes:
        if vector is None:
            base64_state = runtime.execute(
                """
                meshagent.getState({id});
            """.format(id=json.dumps(self._id))
            )
        else:
            base64_state = runtime.execute(
                """
                meshagent.getState({id}, {vector});
            """.format(
                    id=json.dumps(self._id),
                    vector=json.dumps(base64.standard_b64encode(vector)),
                )
            )

        return base64.standard_b64decode(base64_state)

    def get_state_vector(self) -> bytes:
        base64_state = runtime.execute(
            """
            meshagent.getStateVector({id});
        """.format(id=json.dumps(self._id))
        )

        return base64.standard_b64decode(base64_state)

    def send_changes(self, changes) -> None:
        changes = {
            "documentID": self.id,
            "changes": changes,
        }
        changes_json = json.dumps(changes)
        logger.debug("applying changes to document %s: %s", self.id, changes_json)

        runtime.execute(
            """
            meshagent.applyChanges({changes});
        """.format(changes=changes_json)
        )

    @property
    def id(self) -> str:
        return self._id

    def close(self):
        runtime._unregister_document(self)
