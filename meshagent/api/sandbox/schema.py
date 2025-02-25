from meshagent.api.schema import ValueProperty, MeshSchema, ElementType, ChildProperty

sandbox = MeshSchema(
    root_tag_name="sandbox",
    elements=[
        ElementType(tag_name="sandbox", properties=[
            ValueProperty(name="voice", description="whether to enable voice", type="boolean"),
            ValueProperty(name="text", description="whether to text input", type="boolean"),
            ChildProperty(name="config", description="the configuration of this sandbox", child_tag_names=[
                "agent",
            ]),
        ]),
        
        ElementType(tag_name="agent", description="the configuration for an agent", properties=[
            ValueProperty(name="name", description="the name of the agent", type="string"),
            ValueProperty(name="url", description="the url of the agent", type="string"),
        ]),
    ],
)

