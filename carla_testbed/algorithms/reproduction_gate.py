from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import yaml

SCHEMA_VERSION = "reproduction_gate.v1"
CONFIG_SCHEMA_VERSION = "reproduction_gate_config.v1"
DEFAULT_REQUIRED_BEFORE_CLOSED_LOOP = (
    "L0_environment_frozen",
    "L1_upstream_demo_or_record_replay",
    "L2_module_golden_replay",
    "L3_carla_to_apollo_adapter_contract",
    "L4_shadow_mode",
)
VALID_STATUSES = {"pass", "warn", "fail", "blocked"}


class ReproductionGateError(ValueError):
    pass


def load_reproduction_gate_config(path: str | Path) -> dict[str, Any]:
    config_path = Path(path).expanduser()
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ReproductionGateError(f"reproduction gate config must be a mapping: {config_path}")
    if payload.get("schema_version") != CONFIG_SCHEMA_VERSION:
        raise ReproductionGateError(f"schema_version must be {CONFIG_SCHEMA_VERSION}")
    return payload


def _levels(report: Mapping[str, Any]) -> Mapping[str, Any]:
    levels = report.get("levels")
    return levels if isinstance(levels, Mapping) else {}


def _level_status(report: Mapping[str, Any], level_name: str) -> str:
    level = _levels(report).get(level_name)
    if isinstance(level, Mapping):
        return str(level.get("status") or "not_run")
    return "missing"


def _status_rank(status: str) -> int:
    return {"pass": 0, "warn": 1, "fail": 2, "blocked": 3}.get(status, 3)


def _combine_status(current: str, candidate: str) -> str:
    return candidate if _status_rank(candidate) > _status_rank(current) else current


def _report_status(report: Mapping[str, Any] | None) -> str | None:
    if report is None:
        return None
    return str(report.get("status") or report.get("verdict", {}).get("status") or "")


def _has_required_report(report: Mapping[str, Any] | None) -> bool:
    if report is None:
        return False
    status = _report_status(report)
    return status not in {"", "missing", "insufficient_data", "artifact_missing"}


def _wants_physical_mapping_promotion(calibration_report: Mapping[str, Any] | None) -> bool:
    if calibration_report is None:
        return False
    recommendation = calibration_report.get("recommendation")
    if isinstance(recommendation, Mapping) and recommendation.get("enable_physical_mapping") is True:
        return True
    return calibration_report.get("requested_change") == "physical_mapping_promotion"


def _wants_steer_scale_change(calibration_report: Mapping[str, Any] | None) -> bool:
    if calibration_report is None:
        return False
    recommendation = calibration_report.get("recommendation")
    if isinstance(recommendation, Mapping) and recommendation.get("change_steer_scale") is True:
        return True
    return calibration_report.get("requested_change") == "steer_scale_change"


def evaluate_reproduction_gate(
    reproduction_report: Mapping[str, Any],
    route_health: Mapping[str, Any] | None = None,
    ab_report: Mapping[str, Any] | None = None,
    calibration_report: Mapping[str, Any] | None = None,
    *,
    config: Mapping[str, Any] | None = None,
    requested_claims: list[str] | None = None,
) -> dict[str, Any]:
    cfg = config or {}
    required_levels = tuple(cfg.get("required_before_closed_loop_eval") or DEFAULT_REQUIRED_BEFORE_CLOSED_LOOP)
    requested_claims = requested_claims or []
    blocking_reasons: list[str] = []
    missing_artifacts: list[str] = []
    required_next_actions: list[str] = []
    status = "pass"
    can_run_closed_loop_eval = True
    can_claim_algorithm_limitation = True

    if reproduction_report.get("schema_version") != "algorithm_reproduction.v1":
        blocking_reasons.append("invalid_or_missing_reproduction_report")
        required_next_actions.append("provide algorithm_reproduction.v1 report")
        status = "blocked"
        can_run_closed_loop_eval = False
        can_claim_algorithm_limitation = False

    for level_name in required_levels:
        level_status = _level_status(reproduction_report, level_name)
        if level_status not in {"pass", "waived"}:
            reason = f"{level_name}_not_pass:{level_status}"
            blocking_reasons.append(reason)
            can_run_closed_loop_eval = False
            can_claim_algorithm_limitation = False
            status = "blocked"
            if level_name in {
                "L0_environment_frozen",
                "L1_upstream_demo_or_record_replay",
                "L2_module_golden_replay",
                "L3_carla_to_apollo_adapter_contract",
            }:
                required_next_actions.append(f"fix {level_name} before algorithm capability attribution")
            elif level_name == "L4_shadow_mode":
                required_next_actions.append("check route/reference-line/planning semantics before L5 capability claim")

    l3_status = _level_status(reproduction_report, "L3_carla_to_apollo_adapter_contract")
    l4_status = _level_status(reproduction_report, "L4_shadow_mode")
    l5_status = _level_status(reproduction_report, "L5_closed_loop")

    if l3_status not in {"pass", "waived"}:
        blocking_reasons.append("adapter_contract_not_proven")
        can_claim_algorithm_limitation = False
    if l4_status not in {"pass", "waived"}:
        blocking_reasons.append("shadow_mode_not_proven")
        can_run_closed_loop_eval = False
        can_claim_algorithm_limitation = False
    if l5_status == "fail" and l4_status in {"pass", "waived"} and calibration_report is None:
        status = _combine_status(status, "warn")
        can_claim_algorithm_limitation = False
        missing_artifacts.append("control_actuation_report")
        required_next_actions.append("attach control_actuation_report before treating L5 failure as algorithm limitation")

    if not _has_required_report(route_health):
        status = _combine_status(status, "warn")
        missing_artifacts.append("route_health_report")
        required_next_actions.append("attach route_health_report before curve lateral semantics conclusion")
    if not _has_required_report(ab_report):
        status = _combine_status(status, "warn")
        missing_artifacts.append("ab_report")
        required_next_actions.append("attach ab_report before carla_direct improvement claim")
    if calibration_report is None:
        status = _combine_status(status, "warn")
        missing_artifacts.append("calibration_report")
        required_next_actions.append("calibration_report is optional for general claims but required for mapping changes")

    if "curve_lateral_semantics_conclusion" in requested_claims and not _has_required_report(route_health):
        can_claim_algorithm_limitation = False
        blocking_reasons.append("route_health_required_for_curve_claim")
    if "carla_direct_improvement" in requested_claims and not _has_required_report(ab_report):
        can_claim_algorithm_limitation = False
        blocking_reasons.append("ab_report_required_for_direct_improvement_claim")

    wants_physical_mapping = "physical_mapping_promotion" in requested_claims or _wants_physical_mapping_promotion(calibration_report)
    wants_steer_scale_change = "steer_scale_change" in requested_claims or _wants_steer_scale_change(calibration_report)
    if (wants_physical_mapping or wants_steer_scale_change) and calibration_report is None:
        status = "fail"
        can_claim_algorithm_limitation = False
        if wants_physical_mapping:
            blocking_reasons.append("calibration_required_for_physical_mapping_promotion")
            required_next_actions.append("provide calibration_report before physical mapping promotion")
        if wants_steer_scale_change:
            blocking_reasons.append("calibration_required_for_steer_scale_change")
            required_next_actions.append("provide calibration_report before steer_scale change")

    if status not in VALID_STATUSES:
        status = "blocked" if blocking_reasons else "warn"
    if blocking_reasons and status == "pass":
        status = "blocked"

    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "can_run_closed_loop_eval": can_run_closed_loop_eval,
        "can_claim_algorithm_limitation": can_claim_algorithm_limitation and not blocking_reasons,
        "blocking_reasons": sorted(set(blocking_reasons)),
        "missing_artifacts": sorted(set(missing_artifacts)),
        "required_next_actions": sorted(set(required_next_actions)),
        "inputs": {
            "reproduction_status": reproduction_report.get("overall_status"),
            "route_health_status": _report_status(route_health),
            "ab_report_status": _report_status(ab_report),
            "calibration_status": _report_status(calibration_report),
            "requested_claims": requested_claims,
        },
    }
