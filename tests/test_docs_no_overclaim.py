from __future__ import annotations

from pathlib import Path


DOC_FILES = [Path("README.md"), *sorted(Path("docs").rglob("*.md"))]

BANNED_PHRASES = [
    "carla_direct default",
    "carla_direct solved curve",
    "curve solved by carla_direct",
    "Apollo fully drives Town01 naturally",
    "Apollo 完整自然驾驶",
    "traffic light solved",
    "calibration fixes curve",
    "physical mapping enabled by default",
    "steer_scale=0.25 is root cause",
    "lateral_guard_apply_count=0 proves bridge irrelevant",
    "Autoware pass based on RViz only",
    "Apollo fully solved",
]

CAVEAT_TERMS = [
    "experimental",
    "candidate",
    "requires local verification",
    "truth-input MVP",
    "assisted",
    "not full perception reproduction",
    "diagnostic only",
    "insufficient_data",
    "requires natural_driving_report.json",
    "does not prove",
    "not default",
    "must not",
    "cannot",
    "do not",
    "not evidence",
    "不",
    "不是",
    "不能",
    "禁止",
    "需要",
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


def _paragraph_for_offset(text: str, offset: int) -> str:
    start = text.rfind("\n\n", 0, offset)
    end = text.find("\n\n", offset)
    if start == -1:
        start = 0
    else:
        start += 2
    if end == -1:
        end = len(text)
    return text[start:end]


def _nearest_heading_before(text: str, offset: int) -> str:
    heading = ""
    for line in text[:offset].splitlines():
        if line.lstrip().startswith("#"):
            heading = line.strip("# ").strip()
    return heading


def _is_forbidden_example_context(text: str, offset: int) -> bool:
    paragraph = _paragraph_for_offset(text, offset).lower()
    heading = _nearest_heading_before(text, offset).lower()
    markers = (
        "forbidden phrases",
        "forbidden examples",
        "banned phrases",
        "do not say",
        "禁止表达",
        "禁用表达",
        "过度表述",
    )
    return any(marker in paragraph or marker in heading for marker in markers)


def _has_caveat(paragraph: str) -> bool:
    lowered = paragraph.lower()
    return any(term.lower() in lowered for term in CAVEAT_TERMS)


def _line_number(text: str, offset: int) -> int:
    return text[:offset].count("\n") + 1


def test_docs_do_not_overclaim_town01_apollo_status() -> None:
    violations: list[str] = []
    for path in DOC_FILES:
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        lowered = text.lower()
        for phrase in BANNED_PHRASES:
            start = 0
            while True:
                index = lowered.find(phrase.lower(), start)
                if index == -1:
                    break
                paragraph = _paragraph_for_offset(text, index)
                if not _is_forbidden_example_context(text, index) and not _has_caveat(paragraph):
                    violations.append(f"{path}:{_line_number(text, index)} unqualified phrase: {phrase!r}")
                start = index + len(phrase)

    assert not violations, "\n".join(violations)


def test_workflow_docs_exist_and_are_linked_from_readme() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    for path in REQUIRED_DOCS:
        assert path.is_file()
        assert str(path) in readme


def test_docs_list_ci_friendly_entry_tests() -> None:
    text = _doc_text()

    for test_path in REQUIRED_TEST_REFERENCES:
        assert test_path in text
