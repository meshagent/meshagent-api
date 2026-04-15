import json
import uuid
from contextlib import AbstractContextManager
import base64 as b64
import logging
import secrets
from typing import Callable

from .crdt import (
    apply_backend_changes as abc,
    apply_changes as ac,
    get_state as gs,
    get_state_vector as gsv,
    register_document,
    unregister_document as urd,
)
from meshagent.api.schema import MeshSchema
from meshagent.api.schema_document import Document
from importlib import resources


_js: str

with resources.files("meshagent.api").joinpath("entrypoint.js").open("r") as f:
    _js = f.read()


logger = logging.getLogger("document_runtime")
#


def _sync_payload_summary(base64_payload: str) -> str:
    return f"{len(base64_payload)} base64 chars"


random = secrets.SystemRandom()


class DocumentRuntime(AbstractContextManager):
    def __init__(self):
        self._docs = dict[str, Document]()
        # TODO: Polyfill crypto

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return None

    def close(self):
        pass

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
                "publishing backend changes to document %s (%s)",
                document_id,
                _sync_payload_summary(base64),
            )
            doc.on_document_sync(base64)

    def apply_backend_changes(self, document_id: str, base64: str):
        logger.debug(
            "applying backend changes to document %s (%s)",
            document_id,
            _sync_payload_summary(base64),
        )
        abc(document_id, base64)

    def _register_document(
        self, doc: "RuntimeDocument", data: bytes | None = None
    ) -> None:
        self._docs[doc.id] = doc

        def send_update_to_backend(bytes: str):
            runtime.on_document_sync(
                document_id=doc.id, base64=b64.b64encode(bytes).decode()
            )

        if data is None:
            register_document(
                doc.id,
                None,
                False,
                send_update_to_backend=send_update_to_backend,
                send_update_to_client=lambda x: doc.receive_changes(x),
            )
        else:
            register_document(
                doc.id,
                b64.standard_b64encode(data),
                False,
                send_update_to_backend=send_update_to_backend,
                send_update_to_client=lambda x: doc.receive_changes(x),
            )

    def _unregister_document(self, doc: "RuntimeDocument") -> None:
        urd(doc.id)
        self._docs.pop(doc.id)

    def get_state(self, id: str, vector: bytes | None) -> str:
        return gs(id, b64.b64encode(vector) if vector else None)

    def get_state_vector(self, id: str):
        return gsv(id)

    def apply_changes(self, changes: dict):
        ac(changes)


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
                self.id, b64.standard_b64encode(data).decode("utf-8")
            )

    def get_state(self, vector: bytes | None = None) -> bytes:
        base64_state = runtime.get_state(self._id, vector)
        return b64.standard_b64decode(base64_state)

    def get_state_vector(self) -> bytes:
        base64_state = runtime.get_state_vector(self._id)
        return b64.standard_b64decode(base64_state)

    def send_changes(self, changes) -> None:
        changes = {
            "documentID": self.id,
            "changes": changes,
        }
        changes_json = json.dumps(changes)
        logger.debug(
            "applying changes to document %s (%s changes, %s json chars)",
            self.id,
            len(changes["changes"]),
            len(changes_json),
        )

        runtime.apply_changes(changes)

    @property
    def id(self) -> str:
        return self._id

    def close(self):
        runtime._unregister_document(self)
