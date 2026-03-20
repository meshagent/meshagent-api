from meshagent.api.specs.service import ServiceSpec, ServiceTemplateSpec


def test_service_spec_channels_round_trip_from_yaml() -> None:
    yaml_spec = """
version: v1
kind: Service
metadata:
  name: channel-service
agents:
  - name: agent-1
    description: Handles requests
    annotations:
      role: support
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
          message_schema:
            type: object
            properties:
              task:
                type: string
      toolkit:
        - name: helper-tools
container:
  image: meshagent/example
"""

    service = ServiceSpec.from_yaml(yaml_spec)
    payload = service.model_dump(mode="json", exclude_none=True)
    restored = ServiceSpec.model_validate(payload)

    assert restored.agents is not None
    assert restored.agents[0].channels is not None
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
    assert restored.agents[0].channels.queue[0].message_schema == {
        "type": "object",
        "properties": {"task": {"type": "string"}},
    }
    assert restored.agents[0].channels.toolkit is not None
    assert restored.agents[0].channels.toolkit[0].name == "helper-tools"


def test_service_template_spec_preserves_agent_channels() -> None:
    yaml_spec = """
version: v1
kind: ServiceTemplate
metadata:
  name: channel-template
agents:
  - name: helper
    channels:
      messaging:
        - prompts:
            - name: summary
              prompt: Summarize the request
      toolkit:
        - name: docs
container:
  image: meshagent/example
"""

    service = ServiceTemplateSpec.from_yaml(yaml=yaml_spec, values={}).to_service_spec()

    assert service.agents is not None
    assert service.agents[0].channels is not None
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
    assert service.agents[0].channels.toolkit is not None
    assert service.agents[0].channels.toolkit[0].name == "docs"
