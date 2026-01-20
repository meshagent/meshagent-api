import json

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

    assert service.metadata.name == "Concierge"
    assert service.metadata.description == "Hello Rina"
    assert service.metadata.repo == "https://example.com/{{service_name}}"
    assert service.metadata.annotations["greeting"] == "hi Rina"
    assert service.external.url == "https://meshagent.dev/api"
    assert service.agents[0].name == "agent-Concierge"
    assert service.agents[0].description == "handles support"
    assert service.agents[0].annotations["role"] == "support"

    source = service.metadata.annotations["meshagent.service.template.source"]
    values_json = service.metadata.annotations["meshagent.service.template.values"]
    assert json.loads(values_json) == values
    assert "ServiceTemplate" in source


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

    assert service.metadata.description is None
    assert service.metadata.repo is None
    assert service.metadata.icon is None
    assert service.metadata.annotations["meshagent.service.template.source"]
    assert service.metadata.annotations["meshagent.service.template.values"] == "{}"
    assert len(service.metadata.annotations) == 2
    assert service.agents[0].description is None
    assert service.agents[0].annotations is None
    assert service.container.environment[0].value is None
