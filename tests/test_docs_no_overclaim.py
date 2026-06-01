from __future__ import annotations

from pathlib import Path


DOC_FILES = [Path("README.md"), *sorted(Path("docs").glob("*.md"))]

BANNED_PHRASES = [
    "curve solved by carla_direct",
    "carla_direct default",
    "physical mapping enabled by default",
    "calibration fixes curve",
    "Apollo fully solved",
]

REQUIRED_DOCS = [
    Path("docs/town01_route_health.md"),
    Path("docs/carla_direct_ab.md"),
    Path("docs/calibration_pipeline.md"),
]

REQUIRED_TEST_REFERENCES = [
    "tests/test_route_health.py",
    "tests/test_route_schema.py",
    "tests/test_route_health_report.py",
    "tests/test_route_health_run_artifact.py",
    "tests/test_canonical_routes.py",
    "tests/test_ab_manifest.py",
    "tests/test_direct_ab_dry_run.py",
    "tests/test_ab_report.py",
    "tests/test_calibration_profile.py",
    "tests/test_calibration_report.py",
]


def _doc_text() -> str:
    chunks: list[str] = []
    for path in DOC_FILES:
        if path.is_file():
            chunks.append(path.read_text(encoding="utf-8"))
    return "\n".join(chunks)


def test_docs_do_not_overclaim_town01_apollo_status() -> None:
    text = _doc_text().lower()

    for phrase in BANNED_PHRASES:
        assert phrase.lower() not in text


def test_workflow_docs_exist_and_are_linked_from_readme() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    for path in REQUIRED_DOCS:
        assert path.is_file()
        assert str(path) in readme


def test_docs_list_ci_friendly_entry_tests() -> None:
    text = _doc_text()

    for test_path in REQUIRED_TEST_REFERENCES:
        assert test_path in text
