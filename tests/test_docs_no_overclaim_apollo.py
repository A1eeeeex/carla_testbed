from __future__ import annotations

import re
from pathlib import Path


DOC_FILES = [Path("README.md"), *sorted(Path("docs").rglob("*.md"))]

EXACT_OVERCLAIMS = [
    "Apollo fully reproduced",
    "Apollo 完整复现",
    "carla_direct solved curve",
    "curve solved by carla_direct",
    "carla_direct default",
    "physical mapping enabled by default",
    "calibration fixes curve",
    "steer_scale=0.25 is the root cause",
    "lateral_guard_apply_count=0 proves bridge irrelevant",
]

CAVEAT_TERMS = [
    "experimental",
    "not default",
    "does not prove",
    "does not",
    "requires local verification",
    "claim_not_supported",
    "not supported",
    "does not support",
    "do not",
    "must not",
    "cannot",
    "required before",
    "not evidence",
    "not proof",
    "not be treated",
    "不",
    "不能",
    "不是",
    "不会",
    "禁止",
    "必须",
    "需要",
]

CONTEXTUAL_RISK_PATTERNS = [
    (
        "Apollo full reproduction claim",
        re.compile(r"\bapollo\b.{0,80}(fully reproduced|fully solved|complete reproduction)", re.I),
    ),
    (
        "Apollo 完整复现 claim",
        re.compile(r"apollo.{0,40}(完整复现|完整跑通|完全复现)", re.I),
    ),
    (
        "carla_direct curve solved claim",
        re.compile(r"carla_direct.{0,80}(solved|fixed|fixes|healthy).{0,80}curve", re.I),
    ),
    (
        "curve solved by carla_direct claim",
        re.compile(r"curve.{0,80}(solved|fixed|healthy).{0,80}carla_direct", re.I),
    ),
    (
        "carla_direct default claim",
        re.compile(r"carla_direct.{0,80}(default|默认)", re.I),
    ),
    (
        "calibration fixes curve claim",
        re.compile(r"calibration.{0,80}(fixes|fixed|solves|修复|解决).{0,80}curve", re.I),
    ),
    (
        "physical mapping default claim",
        re.compile(r"physical mapping.{0,80}(enabled by default|default|默认启用|should be enabled)", re.I),
    ),
    (
        "steer_scale root-cause claim",
        re.compile(r"steer_scale\s*=?\s*0\.25.{0,80}(root cause|根因)", re.I),
    ),
    (
        "lateral guard proves bridge irrelevant claim",
        re.compile(
            r"lateral_guard_apply_count\s*=?\s*0.{0,100}(bridge irrelevant|completely unrelated|完全无关)",
            re.I,
        ),
    ),
    (
        "tuned Apollo as upstream claim",
        re.compile(r"(apollo_tuned_town01|tuned apollo|调参后).{0,100}(upstream|原版)", re.I),
    ),
]

REQUIRED_GUARDRAIL_REFERENCES = {
    "docs/apollo_reproduction.md": [
        "L0",
        "L5",
        "evaluate_reproduction_gate",
    ],
    "docs/apollo_algorithm_inventory.md": [
        "apollo_tuned_town01",
        "upstream",
        "variant_id",
    ],
    "docs/carla_direct_ab.md": [
        "experimental",
        "default backend",
        "candidate_positive",
    ],
    "docs/town01_route_health.md": [
        "route_health",
        "missing_fields",
        "curve lateral semantics",
    ],
}


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


def _has_caveat(paragraph: str) -> bool:
    lowered = paragraph.lower()
    return any(term.lower() in lowered for term in CAVEAT_TERMS)


def _line_number(text: str, offset: int) -> int:
    return text[:offset].count("\n") + 1


def _doc_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_docs_do_not_use_unqualified_apollo_overclaim_phrases() -> None:
    violations: list[str] = []
    for path in DOC_FILES:
        if not path.is_file():
            continue
        text = _doc_text(path)
        lowered = text.lower()
        for phrase in EXACT_OVERCLAIMS:
            start = 0
            while True:
                idx = lowered.find(phrase.lower(), start)
                if idx == -1:
                    break
                paragraph = _paragraph_for_offset(text, idx)
                if not _has_caveat(paragraph):
                    violations.append(f"{path}:{_line_number(text, idx)} unqualified phrase: {phrase!r}")
                start = idx + len(phrase)

    assert not violations, "\n".join(violations)


def test_docs_risky_apollo_claims_have_same_paragraph_caveats() -> None:
    violations: list[str] = []
    for path in DOC_FILES:
        if not path.is_file():
            continue
        text = _doc_text(path)
        for label, pattern in CONTEXTUAL_RISK_PATTERNS:
            for match in pattern.finditer(text):
                paragraph = _paragraph_for_offset(text, match.start())
                if not _has_caveat(paragraph):
                    violations.append(
                        f"{path}:{_line_number(text, match.start())} risky Apollo claim lacks caveat: {label}"
                    )

    assert not violations, "\n".join(violations)


def test_docs_keep_required_reproduction_gate_references() -> None:
    missing: list[str] = []
    for file_name, required_terms in REQUIRED_GUARDRAIL_REFERENCES.items():
        path = Path(file_name)
        assert path.is_file(), f"missing required docs file: {file_name}"
        text = _doc_text(path)
        for term in required_terms:
            if term not in text:
                missing.append(f"{file_name} missing {term!r}")

    assert not missing, "\n".join(missing)
