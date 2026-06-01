from __future__ import annotations

import re
from pathlib import Path

DOC_FILES = [Path("README.md"), *sorted(Path("docs").rglob("*.md"))]

BANNED_PHRASES = [
    "Apollo fully drives Town01 naturally",
    "Apollo 完整自然驾驶",
    "carla_direct solved curve",
    "traffic light solved",
    "Traffic-light scenarios are placeholders",
    "calibration fixes curve",
    "physical mapping enabled by default",
]

CAVEAT_TERMS = [
    "goal",
    "experimental",
    "requires local verification",
    "truth-input mvp",
    "not full perception reproduction",
    "does not prove",
    "not default",
    "must not",
    "cannot",
    "not evidence",
    "not proof",
    "不",
    "不是",
    "不能",
    "禁止",
    "需要",
]

NATURAL_DRIVING_PASS_PATTERNS = [
    re.compile(r"natural driving.{0,80}\bpass\b", re.I),
    re.compile(r"\bpass\b.{0,80}natural driving", re.I),
    re.compile(r"自然驾驶.{0,40}(通过|pass)", re.I),
]


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


def _line_number(text: str, offset: int) -> int:
    return text[:offset].count("\n") + 1


def _has_caveat(paragraph: str) -> bool:
    lowered = paragraph.lower()
    return any(term.lower() in lowered for term in CAVEAT_TERMS)


def _iter_docs() -> list[tuple[Path, str]]:
    return [(path, path.read_text(encoding="utf-8")) for path in DOC_FILES if path.is_file()]


def test_docs_do_not_overclaim_town01_natural_driving() -> None:
    violations: list[str] = []
    for path, text in _iter_docs():
        lowered = text.lower()
        for phrase in BANNED_PHRASES:
            start = 0
            while True:
                index = lowered.find(phrase.lower(), start)
                if index == -1:
                    break
                paragraph = _paragraph_for_offset(text, index)
                if not _has_caveat(paragraph):
                    violations.append(f"{path}:{_line_number(text, index)} unqualified phrase: {phrase!r}")
                start = index + len(phrase)

    assert not violations, "\n".join(violations)


def test_natural_driving_pass_claims_reference_report_artifact() -> None:
    violations: list[str] = []
    for path, text in _iter_docs():
        for pattern in NATURAL_DRIVING_PASS_PATTERNS:
            for match in pattern.finditer(text):
                paragraph = _paragraph_for_offset(text, match.start())
                if "natural_driving_report.json" not in paragraph:
                    violations.append(
                        f"{path}:{_line_number(text, match.start())} natural-driving pass claim "
                        "must mention natural_driving_report.json in the same paragraph"
                    )

    assert not violations, "\n".join(violations)


def test_readme_points_to_truth_mvp_and_verification_artifact() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "docs/apollo_town01_truth_natural_driving.md" in readme
    assert "truth-input MVP" in readme
    assert "natural_driving_report.json" in readme
    assert "not full perception reproduction" in readme
