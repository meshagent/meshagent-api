from meshagent.api.messaging import (
    Content,
    ErrorContent,
    RawOutputsContent,
    _ControlContent,
    content_types,
    pack_message,
    unpack_content,
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
