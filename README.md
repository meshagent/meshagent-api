# [Meshagent](https://www.meshagent.com)

## MeshAgent API

The ``meshagent.api`` is the foundation that all other packages build on. It includes foundational protocols, JWT authentication, room management, document sync, and more. 

### JWT Authentication
 MeshAgent uses **JSON Web Tokens (JWTs)** to authenticate participants. A token encodes who you are (participant name) and what you’re allowed to access (project ID, room name, role). The token is signed, so the server can verify it without storing any state.

```Python Python
from meshagent.api import ParticipantToken
token = ParticipantToken(
    name="alice",
    project_id="your-project-id",
    api_key_id="your-api-key-id",
)
token.add_room_grant(room_name="my-room", role="user")
jwt = token.to_jwt(token="your-api-secret")
```

### WebSocket Protocol
A WebSocket keeps a two-way connection open between your Python code and the Meshagent server. This allows instant messaging, file transfers, and document updates. ``WebSocketClientProtocol`` manages the underlying connection:

```Python Python
from meshagent.api import WebSocketClientProtocol
protocol = WebSocketClientProtocol(url=room_url, token=jwt)
async with protocol:
    # communication occurs over this protocol
```

Messages are encoded and decoded using a ``Protocol`` layer that is transport-agnostic.

### RoomClient
``RoomClient`` is the main entry point for interacting with a room. Once you pass in the protocol, the room becomes ready and you gain access to specialized sub-clients:

- ``messaging``: send or broadcast text/files.
- ``storage``: open or write files in the room.
- ``sync``: collaborate on structured documents.
- ``agents``: manage agent instances.
- ``queues``, ``database``, ``livekit``, and more.

### Document Runtime and Schemas
``SyncClient`` and the document runtime allow multiple participants to edit structured documents (defined by a ``MeshSchema``) with real-time updates propagated via WebSocket messages.

### WebhookServer
``WebhookServer`` can run in your own service to receive signed events (HTTP webhooks) from MeshAgent—such as room lifecycle events (e.g., room started/ended)—allowing you to trigger custom logic.

### Meshagent
Separate from rooms, ``Meshagent`` is a REST-based client for managing projects, API keys, and secrets. It is useful for administrative tasks.

### ServiceHost
``ServiceHost`` allows you to expose agents or tools as an HTTP service. The MeshAgent Server or CLI can invoke the service via webhook calls. The ``Servicehost`` spins up the agent or tool, connects it to the specified room, and manages its lifecycle until the call completes or is dismissed. This is how examples like the ChatBot or VoiceBot can be run locally and also enables you to deploy an agent as a MeshAgent Service using the same applicable service path once your agent or tool is ready. 

When a call to the agent or tool arrives through a webhook, the ``ServiceHost`` spawns that agent or tool and connects it to the requested room via the ``RoomClient`` and ``WebSocketClientProtocol``. The ``ServiceHost`` starts an HTTP servier and registers each path so that multiple agents or toolkits can be hosted.  

```Python Python
from meshagent.api.services import ServiceHost

service = ServiceHost() # port defaults to an available port if not assigned

@service.path("/chat")
class SimpleChatbot(ChatBot):
    ...
print(f"running on port {service.port}")
asyncio.run(service.run())
```

---
### Learn more about MeshAgent on our website or check out the docs for additional examples!

**Website**: [www.meshagent.com](https://www.meshagent.com/)

**Documentation**: [docs.meshagent.com](https://docs.meshagent.com/)

---
