import pytest
from pydantic import ValidationError

from meshagent.api.webhooks import CallWebhookPayload, WebhookServer


class _CaptureWebhookServer(WebhookServer):
    def __init__(self):
        super().__init__(validate_webhook_secret=False)
        self.calls = []

    async def on_call(self, event):
        self.calls.append(event)


def test_call_webhook_payload_validates_room_call_envelope():
    payload = CallWebhookPayload.model_validate(
        {
            "event": "room.call",
            "data": {
                "room_name": "room-1",
                "room_url": "ws://127.0.0.1/rooms/room-1",
                "token": "token-1",
                "arguments": {"hello": "world"},
            },
        }
    )

    assert payload.data.room_name == "room-1"
    assert payload.data.room_url == "ws://127.0.0.1/rooms/room-1"
    assert payload.data.token == "token-1"
    assert payload.data.arguments == {"hello": "world"}


def test_call_webhook_payload_rejects_other_events():
    with pytest.raises(ValidationError):
        CallWebhookPayload.model_validate(
            {
                "event": "room.started",
                "data": {
                    "room_name": "room-1",
                    "room_url": "ws://127.0.0.1/rooms/room-1",
                    "token": "token-1",
                },
            }
        )


@pytest.mark.asyncio
async def test_webhook_server_on_webhook_uses_typed_call_payload():
    server = _CaptureWebhookServer()

    await server.on_webhook(
        payload={
            "event": "room.call",
            "data": {
                "room_name": "room-1",
                "room_url": "ws://127.0.0.1/rooms/room-1",
                "token": "token-1",
                "arguments": {"hello": "world"},
            },
        }
    )

    assert len(server.calls) == 1
    assert server.calls[0].room_name == "room-1"
    assert server.calls[0].room_url == "ws://127.0.0.1/rooms/room-1"
    assert server.calls[0].token == "token-1"
    assert server.calls[0].arguments == {"hello": "world"}
