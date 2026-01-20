import json

from pydantic_yaml import parse_yaml_raw_as

from meshagent.api.specs.service import (
    AgentSpec,
    ContainerTemplateSpec,
    EnvironmentVariable,
    ExternalServiceTemplateSpec,
    ServiceTemplateMetadata,
    ServiceTemplateSpec,
)


def test_service_template_spec_renders_jinja_values():
    template = ServiceTemplateSpec(
        version="v1",
        kind="ServiceTemplate",
        metadata=ServiceTemplateMetadata(
            name="!template {{service_name}}",
            description="!template Hello {{user}}",
            repo="https://example.com/{{service_name}}",
            annotations={"greeting": "!template hi {{user}}"},
        ),
        agents=[
            AgentSpec(
                name="!template agent-{{service_name}}",
                description="!template handles {{role}}",
                annotations={"role": "!template {{role}}"},
            )
        ],
        external=ExternalServiceTemplateSpec(url="!template https://{{host}}/api"),
    )

    values = {
        "service_name": "Concierge",
        "user": "Rina",
        "role": "support",
        "host": "meshagent.dev",
    }

    service = template.to_service_spec(values=values)

    assert service.metadata.annotations is not None
    assert service.agents is not None
    assert service.external is not None
    assert service.metadata.name == "Concierge"
    assert service.metadata.description == "Hello Rina"
    assert service.metadata.repo == "https://example.com/{{service_name}}"
    assert service.metadata.annotations["greeting"] == "hi Rina"
    assert service.external.url == "https://meshagent.dev/api"
    assert service.agents[0].name == "agent-Concierge"
    assert service.agents[0].description == "handles support"
    assert service.agents[0].annotations is not None
    assert service.agents[0].annotations["role"] == "support"

    source = service.metadata.annotations["meshagent.service.template.source"]
    values_json = service.metadata.annotations["meshagent.service.template.values"]
    assert json.loads(values_json) == values
    assert "ServiceTemplate" in source


def test_service_template_spec_from_yaml():
    yaml_spec = """
version: v1
kind: ServiceTemplate
metadata:
  name: "!template {{service_name}}"
  description: "!template Hello {{user}}"
  repo: null
  annotations:
    greeting: "!template hi {{user}}"
agents:
  - name: "!template agent-{{service_name}}"
    description: "!template handles {{role}}"
external:
  url: "!template https://{{host}}/api"
"""

    template = parse_yaml_raw_as(ServiceTemplateSpec, yaml_spec.encode())
    values = {
        "service_name": "Concierge",
        "user": "Rina",
        "role": "support",
        "host": "meshagent.dev",
    }

    service = template.to_service_spec(values=values)

    assert service.metadata.annotations is not None
    assert service.agents is not None
    assert service.external is not None
    assert service.metadata.name == "Concierge"
    assert service.metadata.description == "Hello Rina"
    assert service.metadata.annotations["greeting"] == "hi Rina"
    assert service.external.url == "https://meshagent.dev/api"
    assert service.agents[0].name == "agent-Concierge"
    assert service.agents[0].description == "handles support"


def test_service_template_spec_replaces_email_in_command():
    template = ServiceTemplateSpec(
        version="v1",
        kind="ServiceTemplate",
        metadata=ServiceTemplateMetadata(
            name="PropertyAssistant",
            description="Email template",
            repo=None,
            annotations=None,
        ),
        container=ContainerTemplateSpec(
            image="us-central1-docker.pkg.dev/meshagent-public/images/cli:{SERVER_VERSION}-esgz",
            command=(
                '!template meshagent multi service -c "chatbot --require-uuid '
                "--agent-name=PropertyAssistant --image-generation=gpt-image-1 "
                "--require-storage --require-toolkit=propertyemail --require-table-write=propertyinsurance "
                "--require-table-write=propertyexpenses --mcp --web-search "
                "-rr='agents/PropertyAssistant/assistantrules.txt' --rule='you have access to "
                "the email tool, and you can send out emails.'; mailbot --reply-all "
                "--enable-attachments --room-rules='/agents/PropertyAssistant/emailrules.txt' "
                "--rule='never respnod in JSON or HTML, only in text.' --agent-name=PropertyAssistant "
                "--require-table-write=propertyinsurance --require-table-write=propertyexpenses "
                "--queue={{email}} --require-uuid --reply-all --require-storage "
                "--email-address={{email}} --require-web-search --toolkit-name=propertyemail; "
                "worker --require-storage --room-rules='/agents/PropertyAssistant/workerrules.txt' "
                "--agent-name=PropertyAssistant --require-toolkit=propertyemail --queue=sendupdate "
                "--require-table-read=propertyinsurance --require-table-read=propertyexpenses "
                "--rule='Use the read_file tool to read PDFs.'\""
            ),
        ),
    )

    service = template.to_service_spec(values={"email": "owner@example.com"})

    assert service.container is not None
    assert service.container.command is not None
    assert "{{email}}" not in service.container.command
    assert "--queue=owner@example.com" in service.container.command
    assert "--email-address=owner@example.com" in service.container.command


def test_service_template_spec_handles_none_values():
    template = ServiceTemplateSpec(
        version="v1",
        kind="ServiceTemplate",
        metadata=ServiceTemplateMetadata(
            name="Plain Service",
            description=None,
            repo=None,
            icon=None,
            annotations=None,
        ),
        agents=[
            AgentSpec(
                name="Support",
                description=None,
                annotations=None,
            )
        ],
        container=ContainerTemplateSpec(
            image="meshagent/example",
            command=None,
            environment=[EnvironmentVariable(name="EMPTY", value=None)],
        ),
    )

    service = template.to_service_spec(values={})

    assert service.metadata.annotations is not None
    assert service.agents is not None
    assert service.container is not None
    assert service.container.environment is not None
    assert service.metadata.description is None
    assert service.metadata.repo is None
    assert service.metadata.icon is None
    assert service.metadata.annotations["meshagent.service.template.source"]
    assert service.metadata.annotations["meshagent.service.template.values"] == "{}"
    assert len(service.metadata.annotations) == 2
    assert service.agents[0].description is None
    assert service.agents[0].annotations is None
    assert service.container.environment[0].value is None
