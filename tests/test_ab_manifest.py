from __future__ import annotations

import json
from pathlib import Path

from carla_testbed.experiments.ab_consistency import check_ab_manifest, check_ab_manifest_file
from carla_testbed.experiments.ab_manifest import CANDIDATE_POSITIVE_INPUT_KEYS, FIXED_VARIABLE_KEYS, load_ab_manifest


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "ab"


def test_valid_manifest_passes() -> None:
    result = check_ab_manifest_file(FIXTURES / "ab_manifest_valid.json")

    assert result.status == "pass"
    assert result.errors == ()
    assert result.missing_fixed_variables == ()


def test_allowed_backend_difference_passes() -> None:
    manifest = load_ab_manifest(FIXTURES / "ab_manifest_valid.json")

    assert manifest.baseline_backend == "ros2_gt"
    assert manifest.candidate_backend == "carla_direct"
    assert "backend" in manifest.allowed_differences
    assert "direct_stale_world_frame_policy" in manifest.allowed_differences
    assert check_ab_manifest(manifest).status == "pass"


def test_missing_warn_fields_warn_without_required_failure(tmp_path: Path) -> None:
    payload = json.loads((FIXTURES / "ab_manifest_valid.json").read_text(encoding="utf-8"))
    for key in ["carla_version", "apollo_version", "map_hash", "vehicle_physics_hash"]:
        payload["fixed_variables"][key] = None
    path = tmp_path / "warn.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    result = check_ab_manifest_file(path)

    assert result.status == "warn"
    assert result.errors == ()
    assert {"carla_version", "apollo_version", "map_hash", "vehicle_physics_hash"}.issubset(
        set(result.missing_fixed_variables)
    )


def test_missing_required_manifest_fails() -> None:
    result = check_ab_manifest_file(FIXTURES / "ab_manifest_missing_required.json")

    assert result.status == "fail"
    assert any("fixed variable fixed_delta_seconds is required" in error for error in result.errors)
    assert any("steer_scale" in error for error in result.errors)
    assert any("spawn_pose or spawn_ref is required" in error for error in result.errors)


def test_must_match_difference_fails() -> None:
    result = check_ab_manifest_file(FIXTURES / "ab_manifest_inconsistent.json")

    assert result.status == "fail"
    assert any("MUST_MATCH variable differs: spawn_pose" in error for error in result.errors)
    assert any("MUST_MATCH variable differs: steer_scale" in error for error in result.errors)


def test_must_match_difference_without_schema_errors_is_not_comparable(tmp_path: Path) -> None:
    payload = json.loads((FIXTURES / "ab_manifest_valid.json").read_text(encoding="utf-8"))
    payload["fixed_variables"]["spawn_pose"] = {
        "baseline": {"x": 0.0, "y": 0.0, "yaw": 0.0},
        "candidate": {"x": 1.0, "y": 0.0, "yaw": 0.0},
    }
    path = tmp_path / "not_comparable.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    result = check_ab_manifest_file(path)

    assert result.status == "not_comparable"
    assert any("MUST_MATCH variable differs: spawn_pose" in error for error in result.errors)


def test_active_assist_mismatch_in_manifest_is_not_comparable(tmp_path: Path) -> None:
    payload = json.loads((FIXTURES / "ab_manifest_valid.json").read_text(encoding="utf-8"))
    payload["fixed_variables"]["active_assists"] = {
        "baseline": [],
        "candidate": ["carla_direct_transport", "straight_lane_lateral_stabilizer"],
    }
    path = tmp_path / "assist_mismatch.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    result = check_ab_manifest_file(path)

    assert result.status == "not_comparable"
    assert any("MUST_MATCH variable differs: active_assists" in error for error in result.errors)


def test_missing_must_match_field_is_not_comparable(tmp_path: Path) -> None:
    payload = json.loads((FIXTURES / "ab_manifest_valid.json").read_text(encoding="utf-8"))
    payload["fixed_variables"]["route_definition_hash"] = None
    path = tmp_path / "missing_must_match.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    result = check_ab_manifest_file(path)

    assert result.status == "not_comparable"
    assert any("MUST_MATCH variable missing: route_definition_hash" in error for error in result.errors)


def test_one_metric_candidate_positive_is_rejected() -> None:
    result = check_ab_manifest_file(FIXTURES / "ab_manifest_inconsistent.json")

    assert result.status == "fail"
    assert any("one_metric_positive is not sufficient" in error for error in result.errors)
    assert any("candidate_positive_inputs incomplete" in error for error in result.errors)


def test_schema_constants_cover_required_keys() -> None:
    assert "steer_scale" in FIXED_VARIABLE_KEYS
    assert "guard_config_hash" in FIXED_VARIABLE_KEYS
    assert "artifact_complete" in CANDIDATE_POSITIVE_INPUT_KEYS
