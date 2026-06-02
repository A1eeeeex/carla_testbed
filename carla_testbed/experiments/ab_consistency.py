from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .ab_manifest import (
    AB_MANIFEST_SCHEMA_VERSION,
    CANDIDATE_POSITIVE_INPUT_KEYS,
    FIXED_VARIABLE_KEYS,
    ABManifest,
    load_ab_manifest,
)

MUST_MATCH = [
    "route_id",
    "town_map",
    "map_hash",
    "route_definition_hash",
    "spawn_pose",
    "goal_pose",
    "ego_blueprint",
    "vehicle_physics_hash",
    "fixed_delta_seconds",
    "sim_duration_s",
    "random_seed",
    "traffic_actors",
    "sensor_rig",
    "control_mode",
    "actuator_mapping_mode",
    "steer_scale",
    "guard_config_hash",
    "calibration_profile_id",
    "active_assists",
    "timeout_policy",
]

MAY_DIFFER = [
    "backend",
    "transport_mode",
    "bridge_mode",
    "backend_config_path",
    "candidate_backend_config_path",
    "ros2_gt_vs_carla_direct",
    "ros2_gt_vs_carla_direct transport details",
    "direct_control_apply_mode",
    "direct_stale_world_frame_policy",
]

WARN_IF_MISSING = ["carla_version", "apollo_version", "map_hash", "vehicle_physics_hash"]
FAIL_IF_MISSING = [
    "route_id",
    "town_map",
    "fixed_delta_seconds",
    "sim_duration_s",
    "control_mode",
    "actuator_mapping_mode",
    "steer_scale",
]
NON_BLOCKING_TRANSPORT_ASSISTS = {"carla_direct_transport"}


@dataclass(frozen=True)
class ABConsistencyResult:
    status: str
    errors: tuple[str, ...]
    warnings: tuple[str, ...]
    missing_fixed_variables: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return self.status in {"pass", "warn"}

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
            "missing_fixed_variables": list(self.missing_fixed_variables),
        }


def _is_missing(value: Any) -> bool:
    return value is None or value == "" or value == []


def _fixed_pair(value: Any) -> tuple[Any, Any] | None:
    if isinstance(value, Mapping):
        if "baseline" in value or "candidate" in value:
            return value.get("baseline"), value.get("candidate")
        if "baseline_value" in value or "candidate_value" in value:
            return value.get("baseline_value"), value.get("candidate_value")
    return None


def _value_missing_for_fixed(value: Any) -> bool:
    pair = _fixed_pair(value)
    if pair is None:
        return _is_missing(value)
    return _is_missing(pair[0]) or _is_missing(pair[1])


def _fixed_values_differ(value: Any) -> bool:
    pair = _fixed_pair(value)
    if pair is None:
        return False
    return pair[0] != pair[1]


def _match_value_missing(key: str, value: Any) -> bool:
    if key == "traffic_actors":
        pair = _fixed_pair(value)
        if pair is None:
            return value is None or value == ""
        return pair[0] is None or pair[1] is None
    return _value_missing_for_fixed(value)


def _assist_values_differ(value: Any) -> bool:
    pair = _fixed_pair(value)
    if pair is None:
        return False
    baseline = {str(item) for item in (pair[0] or []) if item} - NON_BLOCKING_TRANSPORT_ASSISTS
    candidate = {str(item) for item in (pair[1] or []) if item} - NON_BLOCKING_TRANSPORT_ASSISTS
    return baseline != candidate


def _has_spawn_reference(fixed: Mapping[str, Any]) -> bool:
    if not _value_missing_for_fixed(fixed.get("spawn_pose")):
        return True
    if not _value_missing_for_fixed(fixed.get("spawn_ref")):
        return True
    return False


def _has_goal_reference(fixed: Mapping[str, Any]) -> bool:
    if not _value_missing_for_fixed(fixed.get("goal_pose")):
        return True
    if not _value_missing_for_fixed(fixed.get("goal_ref")):
        return True
    return False


def _candidate_positive_inputs_complete(inputs: Mapping[str, Any]) -> bool:
    # ``failure_reason`` is complete when present even if its value is null:
    # null is the expected encoding for "no failure observed".
    present = [key for key in CANDIDATE_POSITIVE_INPUT_KEYS if key in inputs]
    return len(present) >= len(CANDIDATE_POSITIVE_INPUT_KEYS)


def check_ab_manifest(manifest: ABManifest | Mapping[str, Any]) -> ABConsistencyResult:
    if isinstance(manifest, Mapping):
        manifest = ABManifest.from_mapping(dict(manifest))
    errors: list[str] = []
    warnings: list[str] = []
    not_comparable: list[str] = []
    missing: list[str] = []
    fixed = manifest.fixed_variables

    if manifest.schema_version != AB_MANIFEST_SCHEMA_VERSION:
        errors.append(f"schema_version must be {AB_MANIFEST_SCHEMA_VERSION}")
    if not manifest.batch_id:
        errors.append("batch_id is required")
    if not manifest.route_set:
        errors.append("route_set is required")
    if not manifest.baseline_backend or not manifest.candidate_backend:
        errors.append("baseline_backend and candidate_backend are required")
    if not manifest.runs:
        errors.append("runs must not be empty")

    for key in FIXED_VARIABLE_KEYS:
        if key not in fixed:
            missing.append(key)
            if key in FAIL_IF_MISSING or key in WARN_IF_MISSING:
                continue
            warnings.append(f"fixed variable {key} is absent")
    for key in WARN_IF_MISSING:
        if key not in fixed or _value_missing_for_fixed(fixed.get(key)):
            if key not in missing:
                missing.append(key)
            warnings.append(f"fixed variable {key} is missing; A/B should be treated as warn")
    for key in FAIL_IF_MISSING:
        if key not in fixed or _value_missing_for_fixed(fixed.get(key)):
            if key not in missing:
                missing.append(key)
            errors.append(f"fixed variable {key} is required")
    if not _has_spawn_reference(fixed):
        if "spawn_pose" not in missing:
            missing.append("spawn_pose")
        errors.append("spawn_pose or spawn_ref is required")

    for key in MUST_MATCH:
        if key == "active_assists":
            if key in fixed and _assist_values_differ(fixed.get(key)):
                not_comparable.append("MUST_MATCH variable differs: active_assists")
            continue
        if key not in fixed or _match_value_missing(key, fixed.get(key)):
            if key in WARN_IF_MISSING or key in FAIL_IF_MISSING:
                continue
            if key == "spawn_pose" and _has_spawn_reference(fixed):
                continue
            if key == "goal_pose" and _has_goal_reference(fixed):
                continue
            not_comparable.append(f"MUST_MATCH variable missing: {key}")
            continue
        if _fixed_values_differ(fixed.get(key)):
            not_comparable.append(f"MUST_MATCH variable differs: {key}")

    allowed = set(manifest.allowed_differences)
    if manifest.baseline_backend == manifest.candidate_backend:
        warnings.append("baseline_backend and candidate_backend are identical")
    if "backend" not in allowed:
        warnings.append("allowed_differences should include backend")

    candidate_inputs = manifest.candidate_positive_inputs
    if not candidate_inputs:
        errors.append("candidate_positive_inputs is required")
    elif not _candidate_positive_inputs_complete(candidate_inputs):
        missing_inputs = [key for key in CANDIDATE_POSITIVE_INPUT_KEYS if key not in candidate_inputs]
        errors.append(f"candidate_positive_inputs incomplete: {', '.join(missing_inputs)}")
    if candidate_inputs.get("one_metric_positive") is True:
        errors.append("one_metric_positive is not sufficient for candidate_positive")

    for index, run in enumerate(manifest.runs):
        prefix = f"runs[{index}]"
        if not run.run_id:
            errors.append(f"{prefix}.run_id is required")
        if not run.route_id:
            errors.append(f"{prefix}.route_id is required")
        if not run.backend:
            errors.append(f"{prefix}.backend is required")
        if not run.status:
            errors.append(f"{prefix}.status is required")
        if run.backend == "carla_direct":
            if not run.transport_mode:
                warnings.append(f"{prefix}.transport_mode should be recorded for carla_direct")
            if not run.direct_control_apply_mode:
                warnings.append(f"{prefix}.direct_control_apply_mode should be recorded for carla_direct")
            if not run.direct_stale_world_frame_policy:
                warnings.append(f"{prefix}.direct_stale_world_frame_policy should be recorded for carla_direct")

    missing_tuple = tuple(sorted(set(missing)))
    if errors:
        return ABConsistencyResult("fail", tuple(errors + not_comparable), tuple(warnings), missing_tuple)
    if not_comparable:
        return ABConsistencyResult("not_comparable", tuple(not_comparable), tuple(warnings), missing_tuple)
    if warnings:
        return ABConsistencyResult("warn", (), tuple(warnings), missing_tuple)
    return ABConsistencyResult("pass", (), (), missing_tuple)


def check_ab_manifest_file(path: str | Path) -> ABConsistencyResult:
    return check_ab_manifest(load_ab_manifest(path))
