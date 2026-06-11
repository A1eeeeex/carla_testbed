from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from carla_testbed.algorithms.gt_replacement_matrix import load_gt_replacement_matrix


REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_ROOT = REPO_ROOT / "configs"
MATRIX_PATH = CONFIG_ROOT / "reference" / "apollo_gt_replacement_matrix.yaml"
CLAIM_GATE_PATH = CONFIG_ROOT / "gates" / "claim_natural_driving.yaml"
LIST_FIELDS = {
    "required_evidence",
    "required_artifacts",
    "forbidden_assists",
    "evidence_paths",
}


def _load_yaml(path: Path) -> Any:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _walk_config(value: Any, path: tuple[str, ...]) -> list[tuple[tuple[str, ...], Any]]:
    rows: list[tuple[tuple[str, ...], Any]] = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = path + (str(key),)
            rows.append((child_path, child))
            rows.extend(_walk_config(child, child_path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            rows.extend(_walk_config(child, path + (str(index),)))
    return rows


def test_config_evidence_list_fields_are_list_of_strings() -> None:
    bad: list[str] = []
    for config_path in sorted(CONFIG_ROOT.rglob("*.yaml")):
        payload = _load_yaml(config_path)
        for field_path, value in _walk_config(payload, (str(config_path.relative_to(REPO_ROOT)),)):
            if field_path[-1] not in LIST_FIELDS:
                continue
            if not isinstance(value, list):
                bad.append(f"{'/'.join(field_path)} must be list[str], got {type(value).__name__}")
                continue
            for index, item in enumerate(value):
                if not isinstance(item, str):
                    bad.append(
                        f"{'/'.join(field_path)}/{index} must be str, got {type(item).__name__}"
                    )
                elif " - " in item:
                    bad.append(
                        f"{'/'.join(field_path)}/{index} looks like a collapsed YAML list item: {item!r}"
                    )

    assert bad == []


def test_gt_replacement_matrix_modules_have_owner_source_contract_and_evidence() -> None:
    matrix = load_gt_replacement_matrix(MATRIX_PATH)
    missing: list[str] = []
    for module in matrix["modules"]:
        name = module["name"]
        for field in ("owner", "current_source", "output_contract", "required_evidence"):
            if not module.get(field):
                missing.append(f"{name}: missing {field}")
        assert isinstance(module["required_evidence"], list)
        assert all(isinstance(item, str) for item in module["required_evidence"])

    assert missing == []


def test_claim_gate_is_strict_about_warn_and_insufficient_data() -> None:
    gate = _load_yaml(CLAIM_GATE_PATH)
    fail_on_status = set(gate.get("gate", {}).get("fail_on_status") or [])

    assert {"fail", "warn", "insufficient_data"}.issubset(fail_on_status)


def test_claim_gate_requires_runtime_boundary_and_pass_only_core_reports() -> None:
    gate = _load_yaml(CLAIM_GATE_PATH)
    claim_requires = set(gate.get("gate", {}).get("claim_requires") or [])
    rules = {rule["id"]: rule for rule in gate.get("gate", {}).get("rules") or []}

    assert "runtime_claim_boundary_pass" in claim_requires
    for rule_id in [
        "runtime_claim_boundary_pass",
        "apollo_module_consumption_pass",
        "apollo_route_contract_pass",
        "localization_contract_pass",
        "chassis_gt_contract_pass",
        "hdmap_projection_pass",
        "reference_line_contract_pass",
        "control_handoff_pass",
    ]:
        rule = rules[rule_id]
        assert rule["op"] == "=="
        assert rule["value"] == "pass"


def test_claim_gate_expected_artifacts_are_parseable_strings() -> None:
    gate = _load_yaml(CLAIM_GATE_PATH)
    artifacts = gate.get("evidence", {}).get("expected_artifacts")

    assert isinstance(artifacts, list)
    assert artifacts
    assert all(isinstance(item, str) and item.strip() for item in artifacts)
