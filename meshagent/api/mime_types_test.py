import pytest

from meshagent.api import mime_types
from meshagent.api.mime_types import guess_mime_type


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("report.pdf", "application/pdf"),
        (
            "book.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ),
        ("data.iif", "text/x-iif"),
        (
            "letter.docx",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ),
        ("letter.odt", "application/vnd.oasis.opendocument.text"),
        (
            "deck.pptx",
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        ),
        ("source.dart", "text/x-dart"),
        ("source.rs", "application/x-rust"),
        ("source.ts", "application/typescript"),
        ("component.tsx", "text/tsx"),
        ("Dockerfile", "text/x-dockerfile"),
        ("CMakeLists.txt", "text/x-cmake"),
        ("Makefile", "text/x-makefile"),
        ("https://example.test/source.DART?download=1#part", "text/x-dart"),
        ("archive.zip", "application/zip"),
        ("unknown.meshagent-unknown", None),
        ("no-extension", None),
    ],
)
def test_guess_mime_type_is_deterministic(path: str, expected: str | None) -> None:
    assert guess_mime_type(path) == expected


def test_guess_mime_type_falls_back_to_platform_mappings(monkeypatch) -> None:
    monkeypatch.setattr(
        mime_types.mimetypes,
        "guess_type",
        lambda path: ("application/x-platform-extra", None),
    )

    assert guess_mime_type("sound.platform-extra") == "application/x-platform-extra"
    assert guess_mime_type("source.dart") == "text/x-dart"
