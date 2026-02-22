from meshagent.api.messaging import (
    Chunk,
    RawOutputsChunk,
    _ControlChunk,
    chunk_types,
    pack_message,
    unpack_response,
)


def test_chunk_type_registry_uses_chunk_classes() -> None:
    assert chunk_types["json"] is not None
    assert chunk_types["file"] is not None
    assert chunk_types["text"] is not None
    assert chunk_types["error"] is not None
    assert chunk_types["control"] is not None
    assert issubclass(chunk_types["json"], Chunk)


def test_unpack_raw_outputs_chunk() -> None:
    payload = pack_message(
        header={"type": "raw", "outputs": [{"id": "a"}, {"id": "b"}]},
    )
    response = unpack_response(payload)
    assert isinstance(response, RawOutputsChunk)
    assert response.outputs == [{"id": "a"}, {"id": "b"}]


def test_unpack_control_chunk() -> None:
    payload = pack_message(
        header={"type": "control", "method": "open"},
    )
    response = unpack_response(payload)
    assert isinstance(response, _ControlChunk)
    assert response.method == "open"
