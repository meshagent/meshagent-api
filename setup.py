import os
import pathlib
from typing import Any, Dict

import setuptools  # type: ignore

here = pathlib.Path(__file__).parent.resolve()
about: Dict[Any, Any] = {}
with open(os.path.join(here, "version.py"), "r") as f:
    exec(f.read(), about)


setuptools.setup(
    name="meshagent-api",
    version=about["__version__"],
    description="Python Server API for Meshagent",
    long_description=(here / "README.md").read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    url="",
    classifiers=[       
    ],
    keywords=[],
    license="Apache License 2.0",
    packages=setuptools.find_namespace_packages(include=[
        "meshagent.*",
    ]),
    python_requires=">=3.9.0",
    install_requires=[
        "pyjwt>=2.0.0",
        "aiohttp>=3.11.8",  
        "jsonschema>=4.23.0",  
        "pytest>=8.3.4",
        "pytest-asyncio>=0.24.0",
        "PyJWT>=2.10.1",
        "stpyv8>=13.1.201.8"
    ],
    package_data={        
        "meshagent.api": ["py.typed", "*.pyi", "**/*.pyi",  "**/*.js"],
    },
    project_urls={
        "Documentation": "https://meshagent.com",
        "Website": "https://meshagent.com",
        "Source": "https://github.com/meshagent",
    },
)
