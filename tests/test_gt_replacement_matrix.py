from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from carla_testbed.algorithms.gt_replacement_matrix import (
    GTReplacementMatrixError,
    blocked_capabilities,
    evidence_required_for_module,
    load_gt_replacement_matrix,
    module_replacement,
    validate_gt_replacement_matrix,
)
from carla_testbed.algorithms.reference_chain import load_apollo_reference_chain

MATRIX_PATH = Path("configs/reference/apollo_gt_replacement_matrix.yaml")
CHAIN_PATH = Path("configs/reference/apollo_reference_chain.yaml")


def _load_matrix() -> dict:
    return load_gt_replacement_matrix(MATRIX_PATH)


def test_gt_replacement_matrix_yaml_loads() -> None:
    matrix = load_gt_replacement_matrix(MATRIX_PATH)

    assert matrix["schema_version"] == "gt_replacement_matrix.v1"
    assert matrix["algorithm"] == "apollo"
    assert matrix["applies_to_variant"] == "apollo_ported_carla_gt"
    assert validate_gt_replacement_matrix(matrix).ok


def test_matrix_validates_against_reference_chain() -> None:
    reference_chain = load_apollo_reference_chain(CHAIN_PATH)
    matrix = load_gt_replacement_matrix(MATRIX_PATH, reference_chain=reference_chain)

    assert validate_gt_replacement_matrix(matrix, reference_chain=reference_chain).ok


def test_required_module_replacement_statuses() -> None:
    matrix = load_gt_replacement_matrix(MATRIX_PATH)

    assert module_replacement(matrix, "hdmap")["replacement_status"] == "native"
    assert module_replacement(matrix, "routing")["replacement_status"] == "native"
    assert module_replacement(matrix, "localization")["replacement_status"] == "gt_replaced"
    assert module_replacement(matrix, "chassis")["replacement_status"] == "gt_replaced"
    assert module_replacement(matrix, "perception_obstacles")["replacement_status"] == "gt_replaced"
    assert module_replacement(matrix, "prediction")["replacement_status"] != "unknown"
    assert module_replacement(matrix, "traffic_light_perception")["replacement_status"] != "unknown"
    assert module_replacement(matrix, "planning")["replacement_status"] == "native"
    assert module_replacement(matrix, "control")["replacement_status"] == "native"
    assert module_replacement(matrix, "vehicle_interface")["replacement_status"] == "carla_replaced"
    assert module_replacement(matrix, "cyberrt")["replacement_status"] == "native"
    assert module_replacement(matrix, "dreamview")["replacement_status"] == "operator_evidence"


@pytest.mark.parametrize("module_name", ["control", "planning", "hdmap"])
def test_critical_native_modules_cannot_be_gt_replaced(module_name: str) -> None:
    matrix = deepcopy(_load_matrix())
    module_replacement(matrix, module_name)["replacement_status"] = "gt_replaced"

    validation = validate_gt_replacement_matrix(matrix)

    assert not validation.ok
    assert any(f"{module_name}: replacement_status must not be gt_replaced" in e for e in validation.errors)


def test_prediction_unknown_fails() -> None:
    matrix = deepcopy(_load_matrix())
    module_replacement(matrix, "prediction")["replacement_status"] = "unknown"

    validation = validate_gt_replacement_matrix(matrix)

    assert not validation.ok
    assert any("prediction: replacement_status must not be unknown" in e for e in validation.errors)


def test_traffic_light_unknown_fails() -> None:
    matrix = deepcopy(_load_matrix())
    module_replacement(matrix, "traffic_light_perception")["replacement_status"] = "unknown"

    validation = validate_gt_replacement_matrix(matrix)

    assert not validation.ok
    assert any(
        "traffic_light_perception: replacement_status must not be unknown" in e
        for e in validation.errors
    )


def test_traffic_light_hard_gate_requires_planning_consumed_evidence() -> None:
    matrix = deepcopy(_load_matrix())
    traffic_light = module_replacement(matrix, "traffic_light_perception")
    traffic_light["hard_gate_eligible"] = True
    traffic_light["required_evidence"] = [
        evidence
        for evidence in traffic_light["required_evidence"]
        if "planning_consumed" not in evidence
    ]

    validation = validate_gt_replacement_matrix(matrix)

    assert not validation.ok
    assert any("planning consumed evidence" in e for e in validation.errors)


def test_bypassed_without_bypass_reason_fails() -> None:
    matrix = deepcopy(_load_matrix())
    prediction = module_replacement(matrix, "prediction")
    prediction["replacement_status"] = "bypassed"
    prediction["bypass_reason"] = None

    validation = validate_gt_replacement_matrix(matrix)

    assert not validation.ok
    assert any("prediction: bypassed modules must include bypass_reason" in e for e in validation.errors)


def test_blocked_capabilities_include_traffic_light_when_evidence_insufficient() -> None:
    matrix = load_gt_replacement_matrix(MATRIX_PATH)

    assert "traffic_light" in blocked_capabilities(matrix)


def test_localization_required_evidence_includes_contract_report() -> None:
    matrix = load_gt_replacement_matrix(MATRIX_PATH)

    assert "localization_contract_report.json" in evidence_required_for_module(matrix, "localization")


def test_load_rejects_planning_gt_replacement(tmp_path: Path) -> None:
    matrix = deepcopy(_load_matrix())
    module_replacement(matrix, "planning")["replacement_status"] = "gt_replaced"
    path = tmp_path / "bad_gt_replacement_matrix.yaml"

    import yaml

    path.write_text(yaml.safe_dump(matrix), encoding="utf-8")

    with pytest.raises(GTReplacementMatrixError, match="planning: replacement_status"):
        load_gt_replacement_matrix(path)


def test_required_evidence_collapsed_yaml_list_item_fails(tmp_path: Path) -> None:
    matrix = deepcopy(_load_matrix())
    module_replacement(matrix, "chassis")["required_evidence"] = [
        "chassis_gt_contract_report.json - control_health_report.json",
    ]
    path = tmp_path / "bad_gt_replacement_matrix.yaml"

    import yaml

    path.write_text(yaml.safe_dump(matrix), encoding="utf-8")

    with pytest.raises(GTReplacementMatrixError, match="collapsed YAML list item"):
        load_gt_replacement_matrix(path)
