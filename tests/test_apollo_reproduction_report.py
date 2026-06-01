from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest
import yaml

from carla_testbed.algorithms.reproduction import (
    LEVEL_ORDER,
    ReproductionReportError,
    load_reproduction_report,
    validate_reproduction_report,
)

REPORT_PATH = Path("configs/algorithms/apollo_reproduction_report.example.yaml")


def _read_report() -> dict:
    payload = yaml.safe_load(REPORT_PATH.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def test_example_reproduction_report_loads() -> None:
    report = load_reproduction_report(REPORT_PATH)

    assert report["schema_version"] == "algorithm_reproduction.v1"
    assert report["algorithm"] == "apollo"
    assert list(report["levels"]) == list(LEVEL_ORDER)
    assert report["_validation"]["ok"] is True


def test_l5_pass_with_l3_fail_is_invalid() -> None:
    report = _read_report()
    report["levels"]["L3_carla_to_apollo_adapter_contract"]["status"] = "fail"
    report["levels"]["L3_carla_to_apollo_adapter_contract"]["failure_reason"] = "frame_contract_mismatch"

    validation = validate_reproduction_report(report)

    assert not validation.ok
    assert any("L5_closed_loop pass requires L3" in error for error in validation.errors)


def test_l3_fail_cannot_be_algorithm_capability_attribution() -> None:
    report = _read_report()
    report["levels"]["L3_carla_to_apollo_adapter_contract"]["status"] = "fail"
    report["levels"]["L3_carla_to_apollo_adapter_contract"][
        "failure_reason"
    ] = "apollo_algorithm_capability"
    report["levels"]["L5_closed_loop"]["status"] = "blocked"

    validation = validate_reproduction_report(report)

    assert not validation.ok
    assert any("L3 fail must not be attributed" in error for error in validation.errors)


def test_l4_fail_requires_input_contract_or_planning_semantics_reason() -> None:
    report = _read_report()
    report["levels"]["L4_shadow_mode"]["status"] = "fail"
    report["levels"]["L4_shadow_mode"]["failure_reason"] = "actuation_issue"
    report["levels"]["L5_closed_loop"]["status"] = "blocked"

    validation = validate_reproduction_report(report)

    assert not validation.ok
    assert any("L4 fail must use failure_reason=input_contract_or_planning_semantics_issue" in error for error in validation.errors)


def test_missing_required_artifacts_warn_or_fail() -> None:
    report = _read_report()
    l1 = report["levels"]["L1_upstream_demo_or_record_replay"]
    l1["status"] = "warn"
    l1["observed_artifacts"] = ["record_info.txt"]
    report["levels"]["L5_closed_loop"]["status"] = "blocked"

    validation = validate_reproduction_report(report)

    assert validation.ok
    assert validation.status == "warn"
    assert any("L1_upstream_demo_or_record_replay" in warning for warning in validation.warnings)


def test_pass_level_missing_required_observed_artifact_fails() -> None:
    report = _read_report()
    l2 = report["levels"]["L2_module_golden_replay"]
    l2["observed_artifacts"] = ["apollo_output.record"]

    validation = validate_reproduction_report(report)

    assert not validation.ok
    assert any("L2_module_golden_replay: pass requires observed artifacts" in error for error in validation.errors)


def test_tuned_variant_requires_tuning_patch() -> None:
    report = _read_report()
    report["variant_id"] = "apollo_tuned_town01_example"
    report["variant_type"] = "tuned_town01"

    validation = validate_reproduction_report(report)

    assert not validation.ok
    assert any("tuning_patch_path" in error for error in validation.errors)
    assert any("tuning_reason" in error for error in validation.errors)


def test_loader_raises_for_invalid_report(tmp_path: Path) -> None:
    report = deepcopy(_read_report())
    report["levels"]["L5_closed_loop"]["status"] = "pass"
    report["levels"]["L0_environment_frozen"]["status"] = "fail"
    path = tmp_path / "invalid_reproduction.yaml"
    path.write_text(yaml.safe_dump(report), encoding="utf-8")

    with pytest.raises(ReproductionReportError):
        load_reproduction_report(path)
