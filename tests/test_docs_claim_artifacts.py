from __future__ import annotations

import re
from pathlib import Path

DOC_FILES = [Path("README.md"), *sorted(Path("docs").rglob("*.md"))]

CLAIM_RULES = [
    {
        "label": "natural-driving pass claim",
        "patterns": [
            re.compile(r"natural[- ]driving.{0,80}\bpass\b", re.I),
            re.compile(r"\bpass\b.{0,80}natural[- ]driving", re.I),
            re.compile(r"自然驾驶.{0,40}(通过|pass)", re.I),
        ],
        "required_any": ["natural_driving_report.json"],
    },
    {
        "label": "curve conclusion claim",
        "patterns": [
            re.compile(r"curve.{0,80}(conclusion|claim|semantics conclusion|semantics conclusions)", re.I),
            re.compile(r"curve lateral semantics.{0,80}(conclusion|claim|healthy|solved)", re.I),
        ],
        "required_any": ["route_health.json", "apollo_lateral_semantics_report.json"],
    },
    {
        "label": "carla_direct improvement claim",
        "patterns": [
            re.compile(r"carla_direct.{0,80}(improvement|improves|improved|better|positive)", re.I),
            re.compile(r"direct[- ]transport.{0,80}improvement", re.I),
        ],
        "required_any": ["ab_report.json"],
    },
    {
        "label": "calibration promotion claim",
        "patterns": [
            re.compile(r"calibration.{0,100}(promotion|promote|promoting)", re.I),
            re.compile(r"calibration.{0,80}自动.{0,30}promotion", re.I),
        ],
        "required_all": ["calibration_report.json", "no-regression"],
    },
    {
        "label": "Autoware/Apollo comparison claim",
        "patterns": [
            re.compile(r"autoware.{0,120}apollo.{0,80}(comparison|compare|对比)", re.I),
            re.compile(r"apollo.{0,120}autoware.{0,80}(comparison|compare|对比)", re.I),
        ],
        "required_any": ["same acceptance gate", "natural_driving_report.json"],
    },
]


def test_capability_claims_reference_required_artifacts() -> None:
    violations: list[str] = []
    for path in DOC_FILES:
        if not path.is_file():
            continue
        text = path.read_text(encoding="utf-8")
        for rule in CLAIM_RULES:
            for pattern in rule["patterns"]:
                for match in pattern.finditer(text):
                    if _is_forbidden_example_context(text, match.start()):
                        continue
                    paragraph = _paragraph_for_offset(text, match.start())
                    required_any = list(rule.get("required_any") or [])
                    required_all = list(rule.get("required_all") or [])
                    if required_any and not _contains_any(paragraph, required_any):
                        violations.append(
                            f"{path}:{_line_number(text, match.start())} {rule['label']} must mention one of "
                            f"{required_any} in the same paragraph"
                        )
                    missing_all = [term for term in required_all if term.lower() not in paragraph.lower()]
                    if missing_all:
                        violations.append(
                            f"{path}:{_line_number(text, match.start())} {rule['label']} must mention "
                            f"{missing_all} in the same paragraph"
                        )

    assert not violations, "\n".join(violations)


def _contains_any(text: str, terms: list[str]) -> bool:
    lowered = text.lower()
    return any(term.lower() in lowered for term in terms)


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


def _line_number(text: str, offset: int) -> int:
    return text[:offset].count("\n") + 1
