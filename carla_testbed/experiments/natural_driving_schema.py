from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

SCHEMA_VERSION = "natural_driving_suite.v1"
SUPPORTED_SCENARIO_CLASSES = {
    "lane_keep",
    "curve_diagnostic",
    "junction_turn",
    "traffic_light_red_stop",
    "traffic_light_green_go",
    "traffic_light_red_to_green_release",
}
TRAFFIC_LIGHT_SCENARIO_CLASSES = {
    "traffic_light_red_stop",
    "traffic_light_green_go",
    "traffic_light_red_to_green_release",
}
TRAFFIC_LIGHT_EXPECTED_BEHAVIOR_BY_CLASS = {
    "traffic_light_red_stop": "red_stop",
    "traffic_light_green_go": "green_go",
    "traffic_light_red_to_green_release": "red_to_green_release",
}
SUPPORTED_TRAFFIC_LIGHT_STATES = {"RED", "YELLOW", "GREEN", "UNKNOWN"}
SUPPORTED_TRAFFIC_LIGHT_STIMULUS_MODES = {
    "carla_actual_observed",
    "deterministic_gt_control",
}
CLAIM_GRADE_TRAFFIC_LIGHT_STIMULUS_MODES = {"deterministic_gt_control"}
REQUIRED_GATES = {
    "link_health",
    "geometry_health",
    "behavior_health",
    "control_health",
}
REQUIRED_SCENARIO_FIELDS = {
    "route_id",
    "scenario_id",
    "scenario_class",
    "map",
    "duration_s",
    "route_ref",
    "gate_role",
    "required_channels",
    "success_criteria",
    "required_artifacts",
}
REQUIRED_ARTIFACTS = {
    "summary.json",
    "manifest.json",
    "events.jsonl",
    "route_health.json",
    "route_health.csv",
    "curve_segments.csv",
    "route_health_summary.md",
    "apollo_channel_health_report.json",
    "control_health_report.json",
    "failure_timeline_report.json",
    "route_start_alignment_report.json",
    "artifact_completeness_report.json",
}
SUPPORTED_GATE_ROLES = {"hard_gate", "diagnostic_gate", "informational"}
REQUIRED_MODE_FIELDS = {
    "truth_input",
    "algorithm_variant_id",
    "algorithm_variant_manifest_path",
    "transport_mode",
    "backend",
    "default_backend",
}


class NaturalDrivingSuiteError(ValueError):
    pass


@dataclass(frozen=True)
class NaturalDrivingSuiteValidation:
    errors: tuple[str, ...]
    warnings: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
        }


def _scenarios(suite: Mapping[str, Any]) -> list[dict[str, Any]]:
    scenarios = suite.get("scenarios")
    if not isinstance(scenarios, list):
        return []
    return [item for item in scenarios if isinstance(item, dict)]


def _artifact_names(values: Any) -> set[str]:
    if not isinstance(values, list):
        return set()
    return {Path(str(item)).name for item in values}


def _has_timeseries(values: Any) -> bool:
    names = _artifact_names(values)
    return bool({"timeseries.csv", "timeseries.jsonl"} & names)


def _has_resolved_config(values: Any) -> bool:
    names = _artifact_names(values)
    return bool({"config.resolved.yaml", "effective_config.yaml"} & names)


def validate_natural_driving_suite(suite: Mapping[str, Any]) -> NaturalDrivingSuiteValidation:
    errors: list[str] = []
    warnings: list[str] = []

    if suite.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"schema_version must be {SCHEMA_VERSION}")
    if suite.get("map") != "Town01":
        errors.append("map must be Town01")
    gates = suite.get("gates")
    if not isinstance(gates, Mapping):
        errors.append("gates must be a mapping")
    else:
        missing_gates = sorted(REQUIRED_GATES - set(gates))
        if missing_gates:
            errors.append(f"gates missing: {', '.join(missing_gates)}")

    mode = suite.get("mode")
    if isinstance(mode, Mapping):
        missing_mode_fields = sorted(REQUIRED_MODE_FIELDS - set(mode))
        if missing_mode_fields:
            errors.append(f"mode missing: {', '.join(missing_mode_fields)}")
        if mode.get("truth_input") is not True:
            errors.append("mode.truth_input must be true for this suite")
        if mode.get("algorithm_variant_id") in {None, ""}:
            errors.append("mode.algorithm_variant_id is required")
        if mode.get("transport_mode") in {None, ""}:
            errors.append("mode.transport_mode is required")
        if mode.get("backend") in {None, ""}:
            errors.append("mode.backend is required")
        if mode.get("apollo_perception_reproduced") is True:
            errors.append("truth-input suite must not claim Apollo perception reproduction")
        if mode.get("apollo_localization_reproduced") is True:
            errors.append("truth-input suite must not claim Apollo localization reproduction")
        if mode.get("default_backend") == "carla_direct":
            errors.append("carla_direct must not be marked as default backend")
    else:
        warnings.append("mode section missing")

    scenarios = _scenarios(suite)
    if not scenarios:
        errors.append("scenarios must be a non-empty list")
        return NaturalDrivingSuiteValidation(tuple(errors), tuple(warnings))

    seen_ids: set[str] = set()
    route_ids: set[str] = set()
    classes: set[str] = set()
    for scenario in scenarios:
        scenario_id = str(scenario.get("scenario_id") or "")
        route_id = str(scenario.get("route_id") or "")
        if scenario_id in seen_ids:
            errors.append(f"duplicate scenario_id: {scenario_id}")
        if scenario_id:
            seen_ids.add(scenario_id)
        if route_id:
            route_ids.add(route_id)
        missing_fields = sorted(REQUIRED_SCENARIO_FIELDS - set(scenario))
        if missing_fields:
            errors.append(f"{scenario_id or '<missing scenario_id>'}: missing fields {', '.join(missing_fields)}")
        scenario_class = str(scenario.get("scenario_class") or "")
        classes.add(scenario_class)
        if scenario_class not in SUPPORTED_SCENARIO_CLASSES:
            errors.append(f"{scenario_id}: unsupported scenario_class {scenario_class!r}")
        gate_role = str(scenario.get("gate_role") or "")
        if gate_role not in SUPPORTED_GATE_ROLES:
            errors.append(f"{scenario_id}: unsupported gate_role {gate_role!r}")
        if "backend_role" in scenario:
            errors.append(f"{scenario_id}: backend_role is deprecated; use gate_role")
        if "spawn_pose" not in scenario and "spawn_ref" not in scenario:
            errors.append(f"{scenario_id}: missing spawn_pose or spawn_ref")
        if "goal_pose" not in scenario and "goal_ref" not in scenario:
            errors.append(f"{scenario_id}: missing goal_pose or goal_ref")
        if scenario.get("map") != suite.get("map"):
            errors.append(f"{scenario_id}: scenario map must match suite map")
        duration = scenario.get("duration_s")
        if not isinstance(duration, (int, float)) or duration <= 0:
            errors.append(f"{scenario_id}: duration_s must be positive")
        if not isinstance(scenario.get("required_channels"), list) or not scenario.get("required_channels"):
            errors.append(f"{scenario_id}: required_channels must be a non-empty list")
        success = scenario.get("success_criteria")
        if not isinstance(success, Mapping):
            errors.append(f"{scenario_id}: success_criteria must be a mapping")
        else:
            missing_success_gates = sorted(REQUIRED_GATES - set(success))
            if missing_success_gates:
                errors.append(f"{scenario_id}: success_criteria missing gates {', '.join(missing_success_gates)}")
        artifacts = scenario.get("required_artifacts")
        if not isinstance(artifacts, list) or not artifacts:
            errors.append(f"{scenario_id}: required_artifacts must be a non-empty list")
        else:
            missing_artifacts = sorted(REQUIRED_ARTIFACTS - _artifact_names(artifacts))
            if missing_artifacts:
                errors.append(f"{scenario_id}: required_artifacts missing {', '.join(missing_artifacts)}")
            if not _has_resolved_config(artifacts):
                errors.append(
                    f"{scenario_id}: required_artifacts must include config.resolved.yaml or effective_config.yaml"
                )
            if not _has_timeseries(artifacts):
                errors.append(f"{scenario_id}: required_artifacts must include timeseries.csv or timeseries.jsonl")
            if scenario_class in TRAFFIC_LIGHT_SCENARIO_CLASSES:
                expectation = scenario.get("traffic_light_expectation")
                expected_behavior = TRAFFIC_LIGHT_EXPECTED_BEHAVIOR_BY_CLASS[scenario_class]
                if not isinstance(expectation, Mapping):
                    errors.append(f"{scenario_id}: traffic-light scenarios must define traffic_light_expectation")
                else:
                    if expectation.get("expected_behavior") != expected_behavior:
                        errors.append(
                            f"{scenario_id}: traffic_light_expectation.expected_behavior "
                            f"must be {expected_behavior}"
                        )
                    for state_field in ("expected_initial_state", "expected_release_state"):
                        state = expectation.get(state_field)
                        if state not in {None, ""} and str(state).upper() not in SUPPORTED_TRAFFIC_LIGHT_STATES:
                            errors.append(
                                f"{scenario_id}: traffic_light_expectation.{state_field} "
                                f"must be one of {', '.join(sorted(SUPPORTED_TRAFFIC_LIGHT_STATES))}"
                            )
                    required_report_fields = expectation.get("required_report_fields")
                    if not isinstance(required_report_fields, list) or not required_report_fields:
                        errors.append(
                            f"{scenario_id}: traffic_light_expectation.required_report_fields "
                            "must be a non-empty list"
                        )
                    stimulus_mode = str(expectation.get("stimulus_mode") or "").strip()
                    if stimulus_mode not in SUPPORTED_TRAFFIC_LIGHT_STIMULUS_MODES:
                        errors.append(
                            f"{scenario_id}: traffic_light_expectation.stimulus_mode "
                            f"must be one of {', '.join(sorted(SUPPORTED_TRAFFIC_LIGHT_STIMULUS_MODES))}"
                        )
                    if (
                        stimulus_mode not in CLAIM_GRADE_TRAFFIC_LIGHT_STIMULUS_MODES
                        and scenario.get("gate_role") != "informational"
                    ):
                        errors.append(
                            f"{scenario_id}: non-claim-grade traffic-light stimulus must remain informational"
                        )
                names = _artifact_names(artifacts)
                if "traffic_light_contract_report.json" not in names:
                    errors.append(
                        f"{scenario_id}: traffic-light scenarios must include traffic_light_contract_report.json"
                    )
                if "traffic_light_behavior_report.json" not in names:
                    errors.append(
                        f"{scenario_id}: traffic-light scenarios must include traffic_light_behavior_report.json"
                    )
                if "/apollo/perception/traffic_light" not in scenario.get("required_channels", []):
                    errors.append(
                        f"{scenario_id}: traffic-light scenarios must require /apollo/perception/traffic_light"
                    )
        if str(scenario.get("stable_id", "")).startswith("placeholder:"):
            warnings.append(f"{scenario_id}: stable_id is a placeholder")
        if str(route_id).endswith("_tbd"):
            warnings.append(f"{scenario_id}: route_id is a placeholder")
        for ref_name in ("spawn_ref", "goal_ref", "route_ref"):
            if str(scenario.get(ref_name, "")).startswith("placeholder:"):
                warnings.append(f"{scenario_id}: {ref_name} is a placeholder")

    for required_route in ("lane097", "lane217", "junction031"):
        if required_route not in route_ids:
            errors.append(f"missing required route_id: {required_route}")
    hard_gate_routes = {
        str(scenario.get("route_id"))
        for scenario in scenarios
        if scenario.get("gate_role") == "hard_gate"
    }
    for required_route in ("lane097", "lane217", "junction031"):
        if required_route not in hard_gate_routes:
            errors.append(f"{required_route} must be gate_role=hard_gate")
    diagnostic_routes = {
        str(scenario.get("route_id"))
        for scenario in scenarios
        if scenario.get("gate_role") == "diagnostic_gate"
    }
    for route_id in ("curve217", "curve213"):
        if route_id in route_ids and route_id not in diagnostic_routes:
            errors.append(f"{route_id} must be gate_role=diagnostic_gate")
    for required_class in (
        "traffic_light_red_stop",
        "traffic_light_green_go",
        "traffic_light_red_to_green_release",
    ):
        if required_class not in classes:
            errors.append(f"missing traffic-light scenario_class: {required_class}")

    return NaturalDrivingSuiteValidation(tuple(errors), tuple(warnings))


def load_natural_driving_suite(path: str | Path) -> dict[str, Any]:
    suite_path = Path(path).expanduser()
    payload = yaml.safe_load(suite_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise NaturalDrivingSuiteError(f"natural driving suite must be a mapping: {suite_path}")
    validation = validate_natural_driving_suite(payload)
    if validation.errors:
        raise NaturalDrivingSuiteError("; ".join(validation.errors))
    payload["_validation"] = validation.to_dict()
    payload["_source_path"] = str(suite_path)
    return payload


def list_scenarios(suite: Mapping[str, Any]) -> list[dict[str, Any]]:
    return list(_scenarios(suite))


def list_scenarios_by_class(suite: Mapping[str, Any], scenario_class: str) -> list[dict[str, Any]]:
    return [scenario for scenario in _scenarios(suite) if scenario.get("scenario_class") == scenario_class]
