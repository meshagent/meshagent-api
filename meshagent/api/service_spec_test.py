from meshagent.api.agent_content import (
    AgentFileContent,
    AgentTextContent,
)
from meshagent.api.specs.service import (
    ServiceFileSpec,
    ServiceSpec,
    ServiceTemplateSpec,
)


def test_service_spec_channels_round_trip_from_yaml() -> None:
    yaml_spec = """
version: v1
kind: Service
metadata:
  name: channel-service
files:
  - path: /agents/agent-1/heartbeat.md
    text: Review recent room activity before acting.
agents:
  - name: agent-1
    description: Handles requests
    annotations:
      role: support
    email:
      address: assistant@example.com
      public: true
    channels:
      email:
        - address: support@example.com
          private: false
          annotations:
            label: inbox
      messaging:
        - prompts:
            - name: welcome
              prompt: Hello there
      queue:
        - queue: jobs
          threading_mode: default-new
          message_schema:
            type: object
            properties:
              task:
                type: string
      toolkit:
        - name: helper-tools
    heartbeat:
      queue: heartbeats
      thread_id: /agents/agent-1/threads/heartbeats/{YYYY}/{MM}/{DD}/{HH}/{mm}/heartbeat.thread
      prompt:
        - type: file
          url: room:///agents/agent-1/heartbeat.md
        - type: text
          text: Review the pending room activity
        - type: file
          url: room:///docs/today.md
      minutes: 60
container:
  image: meshagent/example
"""

    service = ServiceSpec.from_yaml(yaml_spec)
    payload = service.model_dump(mode="json", exclude_none=True)
    restored = ServiceSpec.model_validate(payload)

    assert restored.agents is not None
    assert restored.agents[0].channels is not None
    assert restored.agents[0].email is not None
    assert restored.agents[0].email.address == "assistant@example.com"
    assert restored.agents[0].email.public is True
    assert restored.agents[0].channels.email is not None
    assert restored.agents[0].channels.email[0].address == "support@example.com"
    assert restored.agents[0].channels.email[0].private is False
    assert restored.agents[0].channels.messaging is not None
    assert (
        payload["agents"][0]["channels"]["messaging"][0]["protocol"]
        == "meshagent.agent-message.v1"
    )
    assert (
        restored.agents[0].channels.messaging[0].protocol
        == "meshagent.agent-message.v1"
    )
    assert restored.agents[0].channels.messaging[0].prompts is not None
    assert restored.agents[0].channels.messaging[0].prompts[0].name == "welcome"
    assert restored.agents[0].channels.messaging[0].prompts[0].description is None
    assert restored.agents[0].channels.queue is not None
    assert restored.agents[0].channels.queue[0].threading_mode == "default-new"
    assert restored.agents[0].channels.queue[0].message_schema == {
        "type": "object",
        "properties": {"task": {"type": "string"}},
    }
    assert restored.agents[0].channels.toolkit is not None
    assert restored.agents[0].channels.toolkit[0].name == "helper-tools"
    assert restored.agents[0].heartbeat is not None
    assert restored.agents[0].heartbeat.queue == "heartbeats"
    assert (
        restored.agents[0].heartbeat.thread_id
        == "/agents/agent-1/threads/heartbeats/{YYYY}/{MM}/{DD}/{HH}/{mm}/heartbeat.thread"
    )
    assert restored.agents[0].heartbeat.minutes == 60
    assert restored.agents[0].heartbeat.prompt is not None
    assert isinstance(restored.agents[0].heartbeat.prompt[0], AgentFileContent)
    assert (
        restored.agents[0].heartbeat.prompt[0].url
        == "room:///agents/agent-1/heartbeat.md"
    )
    assert isinstance(restored.agents[0].heartbeat.prompt[1], AgentTextContent)
    assert (
        restored.agents[0].heartbeat.prompt[1].text
        == "Review the pending room activity"
    )
    assert isinstance(restored.agents[0].heartbeat.prompt[2], AgentFileContent)
    assert restored.agents[0].heartbeat.prompt[2].url == "room:///docs/today.md"
    assert restored.files is not None
    assert isinstance(restored.files[0], ServiceFileSpec)
    assert restored.files[0].path == "/agents/agent-1/heartbeat.md"
    assert restored.files[0].text == "Review recent room activity before acting."


def test_service_template_spec_preserves_agent_channels() -> None:
    yaml_spec = """
version: v1
kind: ServiceTemplate
metadata:
  name: channel-template
files:
  - path: /agents/helper/heartbeat.md
    text: Summarize unresolved room work.
agents:
  - name: helper
    email:
      address: "helper-{{role}}@example.com"
    heartbeat:
      queue: heartbeats
      thread_id: /agents/helper/threads/heartbeats/{YYYY}/{MM}/{DD}/{HH}/{mm}/heartbeat.thread
      prompt:
        - type: text
          text: Summarize the request
      minutes: 30
    channels:
      messaging:
        - prompts:
            - name: summary
              prompt: Summarize the request
      queue:
        - queue: jobs
          threading_mode: default-new
      toolkit:
        - name: docs
container:
  image: meshagent/example
"""

    service = ServiceTemplateSpec.from_yaml(
        yaml=yaml_spec,
        values={"role": "ops"},
    ).to_service_spec()

    assert service.agents is not None
    assert service.agents[0].channels is not None
    assert service.agents[0].email is not None
    assert service.agents[0].email.address == "helper-ops@example.com"
    assert service.agents[0].email.public is False
    assert service.agents[0].channels.messaging is not None
    assert (
        service.agents[0].channels.messaging[0].protocol == "meshagent.agent-message.v1"
    )
    assert service.agents[0].channels.messaging[0].prompts is not None
    assert service.agents[0].channels.messaging[0].prompts[0].description is None
    assert (
        service.agents[0].channels.messaging[0].prompts[0].prompt
        == "Summarize the request"
    )
    assert service.agents[0].channels.queue is not None
    assert service.agents[0].channels.queue[0].queue == "jobs"
    assert service.agents[0].channels.queue[0].threading_mode == "default-new"
    assert service.agents[0].channels.toolkit is not None
    assert service.agents[0].channels.toolkit[0].name == "docs"
    assert service.agents[0].heartbeat is not None
    assert service.agents[0].heartbeat.queue == "heartbeats"
    assert service.agents[0].heartbeat.minutes == 30
    assert service.agents[0].heartbeat.prompt is not None
    assert isinstance(service.agents[0].heartbeat.prompt[0], AgentTextContent)
    assert service.agents[0].heartbeat.prompt[0].text == "Summarize the request"
    assert service.files is not None
    assert isinstance(service.files[0], ServiceFileSpec)
    assert service.files[0].path == "/agents/helper/heartbeat.md"
    assert service.files[0].text == "Summarize unresolved room work."
