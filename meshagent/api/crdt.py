# server_document.py
"""
Server‑side XML CRDT wrapper built on pycrdt.

* Requires:  pip install pycrdt  (>= 0.10)
"""

from __future__ import annotations


import base64
import uuid
from dataclasses import field
from typing import Any, Callable, Dict, List, Optional

from pycrdt import Doc, UndoManager, XmlElement, XmlFragment, XmlText
from pydantic import BaseModel
import copy

# --------------------------------------------------------------------------- #
# 1.  Change‑notification payloads                                            #
# --------------------------------------------------------------------------- #


class DeltaEl(BaseModel):
    retain: Optional[int] = None
    insert: Optional[List[Any]] = None  # serialised children
    delete: Optional[int] = None


class DeltaAttrSet(BaseModel):
    name: str
    value: Any


class DeltaAttr(BaseModel):
    set: List[DeltaAttrSet] = field(default_factory=list)
    delete: List[str] = field(default_factory=list)


class DeltaText(BaseModel):
    retain: Optional[int] = None
    insert: Optional[str | List[Any]] = None
    delete: Optional[int] = None
    attributes: Optional[Dict[str, Any]] = None


class ChangeNotification(BaseModel):
    root: bool
    target: Optional[str]
    elements: List[DeltaEl] = field(default_factory=list)
    attributes: DeltaAttr = field(default_factory=DeltaAttr)
    text: List[DeltaText] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# 2.  Server‑side document wrapper                                            #
# --------------------------------------------------------------------------- #
class ServerXmlDocument:
    """
    Python equivalent of the TypeScript `ServerXmlDocument`, using pycrdt.
    """

    def __init__(
        self,
        doc: Doc,
        undo: bool,
        notify_changes: Callable[[ChangeNotification], None],
    ) -> None:
        # Root <xml> fragment (lazy‑create if missing)

        if "xml" not in doc:
            doc["xml"] = XmlFragment()
        self._root: XmlFragment = doc["xml"]  # fragment not element
        self.doc = doc
        self._undo_mgr = UndoManager(self._root) if undo else None
        self._notify = notify_changes

        # Deep observation (all element / text descendants):contentReference[oaicite:0]{index=0}
        self._sub = self._root.observe_deep(self._handle_event)

    # --------------------------------------------------------------------- #
    # 2.1  Undo / redo                                                     #
    # --------------------------------------------------------------------- #
    def do_undo(self) -> None:
        self._undo_mgr.undo() if self._undo_mgr else None

    def do_redo(self) -> None:
        self._undo_mgr.redo() if self._undo_mgr else None

    # --------------------------------------------------------------------- #
    # 2.2  Backend updates                                                 #
    # --------------------------------------------------------------------- #
    def apply_backend_changes(self, update: bytes) -> None:
        """Apply an update produced somewhere else (e.g. websocket)."""
        self.doc.apply_update(update)  # pycrdt API:contentReference[oaicite:1]{index=1}

    # --------------------------------------------------------------------- #
    # 2.3  Low‑level mutation helpers                                      #
    # --------------------------------------------------------------------- #
    # Element / text removal --------------------------------------------- #
    @staticmethod
    def _delete_node(node: XmlElement | XmlText) -> None:
        parent = node.parent
        if parent is None:
            raise RuntimeError("Cannot delete top‑level node")
        for idx in range(len(parent.children)):
            if parent.children[idx] == node:
                del parent.children[idx]
                return

        raise RuntimeError("Node was not found to delete")

    # Node construction --------------------------------------------------- #
    def _create_nodes(
        self, parent: XmlElement, index, children: List[Dict[str, Any]]
    ) -> List[XmlElement | XmlText]:
        out: List[XmlElement | XmlText] = []
        i = 0
        for child in children:
            if "text" in child:
                txt = XmlText()
                parent.children.insert(index + i, txt)
                if "delta" in child["text"]:
                    if len(child["text"]["delta"]) != 0:
                        # apply_delta not yet exposed; fall back to simple insert
                        # txt.insert(0, child["text"]["delta"])  # type: ignore[arg-type]
                        raise Exception(
                            "delta inserts on new child nodes are not allowed"
                        )
                    else:
                        return

                out.append(txt)
            elif "element" in child:
                el_def = child["element"]
                el = XmlElement(
                    el_def["name"],
                    {"$id": el_def["attributes"].get("$id", str(uuid.uuid4()))},
                )
                parent.children.insert(index + i, el)
                for k, v in el_def["attributes"].items():
                    if k != "$id":
                        el.attributes[k] = v
                if el_def.get("children"):
                    self._create_nodes(el, 0, el_def["children"])
                out.append(el)
            else:
                raise ValueError("Unknown child descriptor")

            i += 1
        return out

    # Child insertion ----------------------------------------------------- #
    def _insert_children(
        self,
        element: XmlElement,
        *,
        after: Optional[str] = None,
        index: Optional[int] = None,
        children: List[Any],
    ) -> None:
        if after:
            for i, c in enumerate(element.children):
                if isinstance(c, XmlElement) and c.attributes.get("$id") == after:
                    index = i + 1
                    break
            else:
                raise ValueError(f"after id {after!r} not found")
        if index is None:
            index = len(element.children)

        self._create_nodes(element, index, children)

    # Child deletion ------------------------------------------------------ #
    @staticmethod
    def _delete_children(
        element: XmlElement,
        *,
        after: Optional[str] = None,
        index: Optional[int] = None,
        length: int,
    ) -> None:
        if after:
            for i, c in enumerate(element.children):
                if isinstance(c, XmlElement) and c.attributes.get("$id") == after:
                    index = i + 1
                    break
            else:
                raise ValueError(f"after id {after!r} not found")
        if index is None:
            raise ValueError("index required when 'after' not supplied")
        del element.children[index : index + length]

    # Text ops ------------------------------------------------------------ #
    @staticmethod
    def _insert_text(
        element: XmlElement,
        index: int,
        text: str,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> None:
        if element.tag != "text":
            raise TypeError("Node is not <text>")
        txt = element.children[0]  # XmlText
        txt.insert(index, text, attributes)

    @staticmethod
    def _format_text(
        element: XmlElement, start: int, length: int, attributes: Dict[str, Any]
    ) -> None:
        if element.tag != "text":
            raise TypeError("Node is not <text>")
        txt = element.children[0]
        txt.format(start, start + length, attributes)

    @staticmethod
    def _delete_text(element: XmlElement, index: int, length: int) -> None:
        if element.tag != "text":
            raise TypeError("Node is not <text>")
        txt = element.children[0]
        del txt[index : index + length]

    # --------------------------------------------------------------------- #
    # 2.4  Public high‑level change API                                    #
    # --------------------------------------------------------------------- #
    def apply_changes(self, changes: List[Dict[str, Any]]) -> None:
        """
        Apply a sequence of JSON‑serialisable change descriptions
        (the `AppliedChange[]` structure from the TS code).
        """
        for change in changes:
            tgt = (
                self._root
                if not change.get("nodeID")
                else self._find_node(change["nodeID"])
            )
            # 1) remove element
            if "delete" in change:
                self._delete_node(tgt)  # type: ignore[arg-type]
                continue

            # 2) children insert/delete
            if "insertChildren" in change:
                self._insert_children(tgt, **change["insertChildren"])  # type: ignore[arg-type]
            if "deleteChildren" in change:
                self._delete_children(tgt, **change["deleteChildren"])  # type: ignore[arg-type]

            # 3) attrs
            if "removeAttributes" in change:
                for k in change["removeAttributes"]:
                    del tgt.attributes[k]
            if "setAttributes" in change:
                for k, v in change["setAttributes"].items():
                    tgt.attributes[k] = v

            # 4) text ops
            if "insertText" in change:
                self._insert_text(tgt, **change["insertText"])
            if "formatText" in change:
                start = change["formatText"]["from"]
                length = change["formatText"]["length"]
                attributes = change["formatText"]["attributes"]
                self._format_text(tgt, start, length, attributes)
            if "deleteText" in change:
                self._delete_text(tgt, **change["deleteText"])

            # 5) undo / redo
            if change.get("undo"):
                self.do_undo()
            if change.get("redo"):
                self.do_redo()

    # --------------------------------------------------------------------- #
    # 2.5  Helpers                                                         #
    # --------------------------------------------------------------------- #
    def _find_node(
        self, node_id: str, root: XmlElement | XmlFragment | XmlText | None = None
    ):
        root = self._root if root is None else root
        if isinstance(root, (XmlElement, XmlFragment)):
            if isinstance(root, XmlElement) and root.attributes.get("$id") == node_id:
                return root
            for c in root.children:
                found = self._find_node(node_id, c)
                if found:
                    return found
        return None

    # --------------------------------------------------------------------- #
    # 2.6  Observer callback                                               #
    # --------------------------------------------------------------------- #
    def _handle_event(self, events) -> None:
        for ev in events:
            target_id = None

            if isinstance(ev.target, XmlText):
                target_id = (
                    ev.target.parent.attributes.get("$id")
                    if hasattr(ev.target.parent, "attributes")
                    else None
                )  # type: ignore[attr-defined]
            else:
                target_id = (
                    ev.target.attributes.get("$id")
                    if hasattr(ev.target, "attributes")
                    else None
                )  # type: ignore[attr-defined]

            # ev: pycrdt.XmlEvent
            # Serialise into the same “shape” the TS code produces.
            msg = ChangeNotification(root=ev.target == self._root, target=target_id)

            # Elements delta ---------------------------------------------------
            if getattr(ev, "delta", None) and not isinstance(ev.target, XmlText):
                for d in ev.delta:
                    if "insert" in d:
                        ins = [
                            {
                                "text": {
                                    "delta": [
                                        {
                                            "insert": child,
                                        }
                                    ]
                                }
                            }
                            if isinstance(child, str)
                            else self._serialise(child)
                            for child in d["insert"]  # type: ignore[index]
                        ]
                        msg.elements.append(DeltaEl(retain=d.get("retain"), insert=ins))

                    elif "delete" in d:
                        msg.elements.append(
                            DeltaEl(retain=d.get("retain"), delete=d["delete"])  # type: ignore[index]
                        )
                    elif "retain" in d:
                        msg.elements.append(DeltaEl(retain=d["retain"]))  # type: ignore[index]

            # Attributes -------------------------------------------------------
            if getattr(ev, "keys", None):
                for k, c in ev.keys.items():
                    if c["action"] in ("add", "update"):
                        msg.attributes.set.append(
                            DeltaAttrSet(name=k, value=ev.target.attributes.get(k))  # type: ignore[attr-defined]
                        )
                    elif c["action"] == "delete":
                        msg.attributes.delete.append(k)

            # Text delta -------------------------------------------------------
            if getattr(ev, "delta", None) and isinstance(ev.target, XmlText):
                for d in ev.delta:
                    attrs = d.get("attributes")
                    if "insert" in d:
                        msg.text.append(
                            DeltaText(
                                retain=d.get("retain"),
                                insert=d["insert"],
                                attributes=attrs,
                            )
                        )
                    elif "delete" in d:
                        msg.text.append(
                            DeltaText(retain=d.get("retain"), delete=d["delete"])
                        )
                    elif "retain" in d:
                        msg.text.append(DeltaText(retain=d["retain"], attributes=attrs))

            self._notify(msg.model_dump(mode="json"))

    # --------------------------------------------------------------------- #
    # 2.7  Serialisation helper                                            #
    # --------------------------------------------------------------------- #
    def _serialise(self, node: XmlElement | XmlText | str):
        if isinstance(node, XmlElement):
            return {
                "element": {
                    "tagName": node.tag,
                    "attributes": dict(node.attributes),
                    "children": [self._serialise(c) for c in node.children],
                }
            }

        if isinstance(node, XmlText):
            # TODO: This should be a full delta or formatting will be lost
            return {"text": {"delta": [{"insert": node.to_py(), "attributes": {}}]}}

        raise TypeError(type(node))


# --------------------------------------------------------------------------- #
# 3.  Simple client‑side wiring (unchanged public surface)                    #
# --------------------------------------------------------------------------- #
class ClientProtocol:
    """
    Very small shim used in the original code to keep “client”/“server” docs
    separate.  Here we simply expose the pycrdt.Doc and forward binary updates.
    """

    def __init__(self, on_update: Callable[[bytes, Any], None]) -> None:
        self.doc = Doc()

        def _obs(ev):
            on_update(ev.update, ev)  # ev.update = bytes

        self._sub = self.doc.observe(_obs)

    def update(self, update: bytes) -> None:
        self.doc.apply_update(update)


# --------------------------------------------------------------------------- #
# 4.  Registry utilities (as in the TS file)                                  #
# --------------------------------------------------------------------------- #
ProtocolMap: Dict[str, ClientProtocol] = {}
Documents: Dict[str, ServerXmlDocument] = {}


def register_document(
    doc_id: str,
    base64_state: Optional[str],
    undo: bool = False,
    *,
    send_update_to_backend: Callable[[str], None] | None = None,
    send_update_to_client: Callable[[str], None] | None = None,
):
    if doc_id in Documents:
        raise RuntimeError(f"document already registered: {doc_id}")

    # 4.1 client side ------------------------------------------------------
    client = ClientProtocol(
        lambda upd, _origin=None: (lambda _: None)(
            send_update_to_backend(upd) if send_update_to_backend else 0
        )
    )

    # 4.2 server side ------------------------------------------------------
    def _notify(delta: dict) -> None:
        (send_update_to_client or (lambda _: None))(delta)

    server = ServerXmlDocument(client.doc, undo, _notify)

    # initial state --------------------------------------------------------
    if base64_state:
        server.apply_backend_changes(base64.b64decode(base64_state))

    ProtocolMap[doc_id] = client
    Documents[doc_id] = server


def unregister_document(doc_id: str) -> None:
    ProtocolMap.pop(doc_id, None)
    Documents.pop(doc_id, None)


# small helpers -------------------------------------------------------------
def apply_changes(payload: Dict[str, Any]) -> None:
    payload = copy.deepcopy(payload)
    doc = Documents[payload["documentID"]]
    doc.apply_changes(payload["changes"])


def get_state(doc_id: str, vector_b64: Optional[str] = None) -> str:
    doc = Documents[doc_id].doc
    if vector_b64:
        vec = base64.b64decode(vector_b64)
        update = doc.get_update(vec)
    else:
        update = doc.get_update(b"\x00")
    return base64.b64encode(update).decode()


def get_state_vector(doc_id: str) -> str:
    return base64.b64encode(Documents[doc_id].doc.get_state()).decode()


def apply_backend_changes(doc_id: str, changes_b64: str) -> None:
    Documents[doc_id].apply_backend_changes(base64.b64decode(changes_b64))
