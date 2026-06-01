from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

SCHEMA_VERSION = "algorithm_reproduction.v1"
LEVEL_ORDER = (
    "L0_environment_frozen",
    "L1_upstream_demo_or_record_replay",
    "L2_module_golden_replay",
    "L3_carla_to_apollo_adapter_contract",
    "L4_shadow_mode",
    "L5_closed_loop",
)
LEVEL_STATUSES = {"not_run", "pass", "fail", "warn", "blocked", "waived"}
LEVEL_FIELDS = {
    "status",
    "required_artifacts",
    "observed_artifacts",
    "checks",
    "failure_reason",
    "next_action",
}
REQUIRED_ARTIFACTS_BY_LEVEL = {
    "L0_environment_frozen": {
        "algorithm_variant_manifest.yaml",
        "environment_manifest.json",
    },
    "L1_upstream_demo_or_record_replay": {
        "record_info.txt",
        "dreamview_screenshot.png",
        "cyber_channel_stats.json",
    },
    "L2_module_golden_replay": {
        "apollo_output.record",
        "planning_digest.json",
        "control_digest.json",
        "replay_report.json",
    },
    "L3_carla_to_apollo_adapter_contract": {
        "adapter_contract_report.json",
        "frame_transform_report.json",
        "routing_contract_report.json",
        "channel_stats.json",
    },
    "L4_shadow_mode": {
        "shadow_mode_report.json",
        "planning_vs_route.csv",
        "control_shadow_timeseries.csv",
        "route_health.json",
    },
    "L5_closed_loop": {
        "summary.json",
        "manifest.json",
        "route_health.json",
    },
}
L5_TIMESERIES_OPTIONS = {"timeseries.csv", "timeseries.jsonl"}
L0_PATCH_MARKERS = {"apollo_config_diff.patch", "no_patch.marker", "explicit_no_patch.marker"}


class ReproductionReportError(ValueError):
    pass


@dataclass(frozen=True)
class ReproductionReportValidation:
    errors: tuple[str, ...]
    warnings: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.errors

    @property
    def status(self) -> str:
        if self.errors:
            return "fail"
        if self.warnings:
            return "warn"
        return "pass"

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "status": self.status,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
        }


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _artifact_names(value: Any) -> set[str]:
    names: set[str] = set()
    for item in _as_list(value):
        names.add(Path(str(item)).name)
    return names


def _levels(report: Mapping[str, Any]) -> Mapping[str, Any]:
    levels = report.get("levels")
    return levels if isinstance(levels, Mapping) else {}


def _level(report: Mapping[str, Any], level_name: str) -> Mapping[str, Any]:
    value = _levels(report).get(level_name)
    return value if isinstance(value, Mapping) else {}


def _level_status(report: Mapping[str, Any], level_name: str) -> str:
    return str(_level(report, level_name).get("status") or "")


def _has_artifact(level: Mapping[str, Any], artifact: str) -> bool:
    names = _artifact_names(level.get("observed_artifacts"))
    return artifact in names


def _validate_level_shape(level_name: str, level: Mapping[str, Any], errors: list[str]) -> None:
    missing = sorted(LEVEL_FIELDS - set(level))
    if missing:
        errors.append(f"{level_name}: missing fields {', '.join(missing)}")
    status = level.get("status")
    if status not in LEVEL_STATUSES:
        errors.append(f"{level_name}: status must be one of {sorted(LEVEL_STATUSES)}")
    if not isinstance(level.get("required_artifacts"), list):
        errors.append(f"{level_name}: required_artifacts must be a list")
    if not isinstance(level.get("observed_artifacts"), list):
        errors.append(f"{level_name}: observed_artifacts must be a list")
    if not isinstance(level.get("checks"), list):
        errors.append(f"{level_name}: checks must be a list")


def _validate_required_artifacts(
    level_name: str,
    level: Mapping[str, Any],
    errors: list[str],
    warnings: list[str],
) -> None:
    required = _artifact_names(level.get("required_artifacts"))
    expected = REQUIRED_ARTIFACTS_BY_LEVEL[level_name]
    missing_from_required = sorted(expected - required)
    if missing_from_required:
        errors.append(f"{level_name}: required_artifacts missing schema items {', '.join(missing_from_required)}")

    observed = _artifact_names(level.get("observed_artifacts"))
    if level_name == "L0_environment_frozen":
        if not (required | observed).intersection(L0_PATCH_MARKERS):
            errors.append(
                "L0_environment_frozen: required_artifacts must include apollo_config_diff.patch "
                "or an explicit no_patch marker"
            )
    if level_name == "L5_closed_loop":
        if not (required | observed).intersection(L5_TIMESERIES_OPTIONS):
            errors.append("L5_closed_loop: required_artifacts must include timeseries.csv or timeseries.jsonl")

    if level.get("status") == "pass":
        missing_observed = sorted(expected - observed)
        if level_name == "L5_closed_loop" and not observed.intersection(L5_TIMESERIES_OPTIONS):
            missing_observed.append("timeseries.csv|timeseries.jsonl")
        if level_name == "L0_environment_frozen" and not observed.intersection(L0_PATCH_MARKERS):
            missing_observed.append("apollo_config_diff.patch|explicit_no_patch_marker")
        if missing_observed:
            errors.append(f"{level_name}: pass requires observed artifacts {', '.join(missing_observed)}")
    else:
        missing_observed = sorted(expected - observed)
        if missing_observed:
            warnings.append(f"{level_name}: observed_artifacts missing {', '.join(missing_observed)}")


def _validate_l5_dependencies(report: Mapping[str, Any], errors: list[str]) -> None:
    if _level_status(report, "L5_closed_loop") != "pass":
        return
    for dependency in LEVEL_ORDER[:-1]:
        status = _level_status(report, dependency)
        if status not in {"pass", "waived"}:
            errors.append(f"L5_closed_loop pass requires {dependency} to be pass or waived, got {status!r}")


def _validate_failure_attribution(report: Mapping[str, Any], errors: list[str], warnings: list[str]) -> None:
    l3 = _level(report, "L3_carla_to_apollo_adapter_contract")
    l4 = _level(report, "L4_shadow_mode")
    l5 = _level(report, "L5_closed_loop")

    if l3.get("status") == "fail":
        reason = str(l3.get("failure_reason") or "")
        if "apollo_algorithm_capability" in reason:
            errors.append("L3 fail must not be attributed to Apollo algorithm capability")
        if not reason:
            warnings.append("L3 fail should record adapter/input contract failure_reason")

    if l4.get("status") == "fail":
        reason = str(l4.get("failure_reason") or "")
        if reason != "input_contract_or_planning_semantics_issue":
            errors.append(
                "L4 fail must use failure_reason=input_contract_or_planning_semantics_issue "
                "before considering actuation"
            )

    if l4.get("status") == "pass" and l5.get("status") == "fail":
        reason = str(l5.get("failure_reason") or "")
        if reason and reason not in {
            "control_bridge_or_actuation_or_latency",
            "control_bridge",
            "actuation",
            "latency",
        }:
            warnings.append(
                "L4 pass + L5 fail should prioritize control bridge / actuation / latency checks"
            )


def _validate_tuned_variant(report: Mapping[str, Any], errors: list[str]) -> None:
    if report.get("variant_type") != "tuned_town01":
        return
    if not report.get("tuning_patch_path"):
        errors.append("tuned_town01 reproduction report requires tuning_patch_path")
    if not report.get("tuning_reason"):
        errors.append("tuned_town01 reproduction report requires tuning_reason")


def validate_reproduction_report(report: Mapping[str, Any]) -> ReproductionReportValidation:
    errors: list[str] = []
    warnings: list[str] = []

    if report.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"schema_version must be {SCHEMA_VERSION}")
    if report.get("algorithm") != "apollo":
        errors.append("algorithm must be apollo")
    if not report.get("variant_id"):
        errors.append("variant_id is required")
    if not report.get("created_at"):
        errors.append("created_at is required")
    if report.get("overall_status") not in {"not_run", "pass", "fail", "warn", "blocked"}:
        errors.append("overall_status must be one of not_run/pass/fail/warn/blocked")
    if not isinstance(report.get("missing_artifacts"), list):
        errors.append("missing_artifacts must be a list")
    if not isinstance(report.get("blocking_failures"), list):
        errors.append("blocking_failures must be a list")

    levels = report.get("levels")
    if not isinstance(levels, Mapping):
        errors.append("levels must be a mapping")
        return ReproductionReportValidation(tuple(errors), tuple(warnings))

    missing_levels = [level for level in LEVEL_ORDER if level not in levels]
    if missing_levels:
        errors.append("levels missing: " + ", ".join(missing_levels))

    for level_name in LEVEL_ORDER:
        level = _level(report, level_name)
        if not level:
            continue
        _validate_level_shape(level_name, level, errors)
        _validate_required_artifacts(level_name, level, errors, warnings)

    _validate_l5_dependencies(report, errors)
    _validate_failure_attribution(report, errors, warnings)
    _validate_tuned_variant(report, errors)

    return ReproductionReportValidation(tuple(errors), tuple(warnings))


def load_reproduction_report(path: str | Path) -> dict[str, Any]:
    report_path = Path(path).expanduser()
    payload = yaml.safe_load(report_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ReproductionReportError(f"reproduction report must be a mapping: {report_path}")
    validation = validate_reproduction_report(payload)
    if validation.errors:
        raise ReproductionReportError("; ".join(validation.errors))
    payload["_validation"] = validation.to_dict()
    payload["_source_path"] = str(report_path)
    return payload
