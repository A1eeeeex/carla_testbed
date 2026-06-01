from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from carla_testbed.calibration.profile import (
    CalibrationProfileError,
    load_calibration_profile,
    validate_calibration_profile,
)


PROFILE_PATH = Path("configs/calibration/control_actuation.yaml")


def test_example_yaml_loads_successfully() -> None:
    profile = load_calibration_profile(PROFILE_PATH)

    assert profile.schema_version == "calibration_profile.v1"
    assert profile.profile_id == "town01_control_actuation_legacy_draft"
    assert profile.status == "draft"
    assert {gate.route_id for gate in profile.no_regression_gates} == {"097", "217", "031"}


def test_steer_scale_is_recorded_not_modified() -> None:
    profile = load_calibration_profile(PROFILE_PATH)

    assert profile.control_mapping.actuator_mapping_mode == "legacy"
    assert profile.control_mapping.steer_scale == 0.25


def test_missing_097_217_031_gates_fail() -> None:
    profile = load_calibration_profile(PROFILE_PATH)
    incomplete = replace(profile, validation_routes=("097",), no_regression_gates=profile.no_regression_gates[:1])

    result = validate_calibration_profile(incomplete)

    assert not result.ok
    assert any("missing required routes" in error for error in result.errors)


def test_draft_can_warn_on_missing_gates_when_explicitly_allowed() -> None:
    profile = load_calibration_profile(PROFILE_PATH)
    incomplete = replace(profile, validation_routes=("097",), no_regression_gates=profile.no_regression_gates[:1])

    result = validate_calibration_profile(incomplete, allow_missing_gates=True)

    assert result.ok
    assert any("missing required routes" in warning for warning in result.warnings)


def test_physical_mapping_without_passed_gates_fails() -> None:
    profile = load_calibration_profile(PROFILE_PATH)
    physical_mapping = replace(profile.control_mapping, actuator_mapping_mode="physical")
    physical_profile = replace(profile, control_mapping=physical_mapping)

    result = validate_calibration_profile(physical_profile)

    assert not result.ok
    assert any("physical actuator mapping requires" in error for error in result.errors)


def test_draft_allows_null_latency() -> None:
    profile = load_calibration_profile(PROFILE_PATH)
    result = validate_calibration_profile(profile)

    assert result.ok
    assert any("null latency_ms" in warning for warning in result.warnings)
    assert profile.latency_ms.steer is None
    assert profile.latency_ms.throttle is None
    assert profile.latency_ms.brake is None


def test_validated_requires_all_gates_pass() -> None:
    profile = load_calibration_profile(PROFILE_PATH)
    validated = replace(profile, status="validated")

    result = validate_calibration_profile(validated)

    assert not result.ok
    assert any("validated profile requires" in error for error in result.errors)


def test_loader_reports_invalid_profile(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        """
schema_version: calibration_profile.v1
profile_id: ""
status: validated
control_mapping:
  actuator_mapping_mode: physical
  steer_scale: 0
recommendation_policy:
  can_modify_mainline_config: true
""",
        encoding="utf-8",
    )

    with pytest.raises(CalibrationProfileError) as exc:
        load_calibration_profile(bad)
    assert "profile_id is required" in str(exc.value)
    assert "steer_scale must be > 0" in str(exc.value)
