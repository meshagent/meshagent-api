from meshagent.api.messaging import (
    Content,
    ErrorContent,
    MessageProtocolError,
    RawOutputsContent,
    _ControlContent,
    content_types,
    pack_message,
    unpack_content,
    unpack_message,
)


def test_content_type_registry_uses_content_classes() -> None:
    assert content_types["json"] is not None
    assert content_types["file"] is not None
    assert content_types["text"] is not None
    assert content_types["error"] is not None
    assert content_types["control"] is not None
    assert issubclass(content_types["json"], Content)


def test_unpack_raw_outputs_content() -> None:
    payload = pack_message(
        header={"type": "raw", "outputs": [{"id": "a"}, {"id": "b"}]},
    )
    response = unpack_content(payload)
    assert isinstance(response, RawOutputsContent)
    assert response.outputs == [{"id": "a"}, {"id": "b"}]


def test_unpack_control_content() -> None:
    payload = pack_message(
        header={"type": "control", "method": "open"},
    )
    response = unpack_content(payload)
    assert isinstance(response, _ControlContent)
    assert response.method == "open"


def test_unpack_error_content_with_code() -> None:
    payload = pack_message(
        header={"type": "error", "text": "boom", "code": 2001},
    )
    response = unpack_content(payload)
    assert isinstance(response, ErrorContent)
    assert response.text == "boom"
    assert response.code == 2001


def test_unpack_message_rejects_short_message_with_protocol_error() -> None:
    try:
        unpack_message(b"")
    except MessageProtocolError as ex:
        assert str(ex) == "message is too short"
        assert ex.code == 1002
    else:
        raise AssertionError("expected MessageProtocolError")


def test_unpack_message_rejects_incomplete_header_with_protocol_error() -> None:
    try:
        unpack_message((3).to_bytes(8) + b"{}")
    except MessageProtocolError as ex:
        assert str(ex) == "message header is incomplete"
        assert ex.code == 1002
    else:
        raise AssertionError("expected MessageProtocolError")


def test_unpack_message_rejects_invalid_json_header_with_protocol_error() -> None:
    try:
        unpack_message((1).to_bytes(8) + b"{")
    except MessageProtocolError as ex:
        assert str(ex).startswith("invalid message header:")
        assert ex.code == 1002
    else:
        raise AssertionError("expected MessageProtocolError")


def test_unpack_message_rejects_non_object_header_with_protocol_error() -> None:
    header = b"[]"
    try:
        unpack_message(len(header).to_bytes(8) + header)
    except MessageProtocolError as ex:
        assert str(ex) == "message header must be an object"
        assert ex.code == 1002
    else:
        raise AssertionError("expected MessageProtocolError")


def test_unpack_content_rejects_missing_type_with_protocol_error() -> None:
    try:
        unpack_content(pack_message(header={"json": {"ok": True}}))
    except MessageProtocolError as ex:
        assert str(ex) == "content header is missing required 'type'"
        assert ex.code == 1002
    else:
        raise AssertionError("expected MessageProtocolError")


def test_unpack_content_rejects_unsupported_type_with_protocol_error() -> None:
    try:
        unpack_content(pack_message(header={"type": "bogus"}))
    except MessageProtocolError as ex:
        assert str(ex) == "unsupported content type: bogus"
        assert ex.code == 1002
    else:
        raise AssertionError("expected MessageProtocolError")


def test_unpack_content_rejects_missing_required_field_with_protocol_error() -> None:
    try:
        unpack_content(pack_message(header={"type": "json"}))
    except MessageProtocolError as ex:
        assert str(ex) == "json content is missing required field 'json'"
        assert ex.code == 1002
    else:
        raise AssertionError("expected MessageProtocolError")
