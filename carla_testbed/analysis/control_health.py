from __future__ import annotations

import csv
import json
import math
import re
from pathlib import Path
from typing import Any, Mapping, Sequence

CONTROL_HEALTH_REPORT_SCHEMA_VERSION = "control_health_report.v1"
EXPECTED_HANDOFF_STATUS = "control_consuming_with_nonzero_planning"
DEFAULT_THRESHOLDS = {
    "max_control_latency_p95_ms": 500.0,
    "max_brake_throttle_conflict_frames": 0,
    "max_mapped_applied_abs_error_p95": 0.10,
    "max_control_apply_observation_delay_s": 5.0,
    "min_route_progress_after_applied_control_m": 1.0,
    "min_control_bridge_apply_frame_hz": 10.0,
    "max_control_bridge_same_frame_drop_ratio": 0.80,
    "min_control_bridge_apply_frame_coverage_ratio": 0.95,
    "max_control_bridge_ego_bind_delay_s": 5.0,
    "max_control_bridge_bind_to_first_apply_s": 5.0,
    "max_applied_throttle_brake_switch_count": 10,
    "max_raw_throttle_brake_switch_count": 10,
    "max_mapped_throttle_brake_switch_count": 10,
    "max_vehicle_response_sign_switch_count": 10,
}
CONTROL_TRACE_FIELDS = [
    "apollo_steer_raw",
    "bridge_steer_mapped",
    "carla_steer_applied",
    "throttle_raw",
    "throttle_mapped",
    "throttle_applied",
    "brake_raw",
    "brake_mapped",
    "brake_applied",
]
APPLIED_CONTROL_TRACE_FIELDS = [
    "carla_steer_applied",
    "throttle_applied",
    "brake_applied",
]


def analyze_control_health_run_dir(
    run_dir: str | Path,
    *,
    thresholds: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    root = Path(run_dir).expanduser()
    active_thresholds = dict(DEFAULT_THRESHOLDS)
    active_thresholds.update(thresholds or {})

    summary_path = root / "summary.json"
    manifest_path = root / "manifest.json"
    timeseries_path = _find_first(root, ["timeseries.csv", "timeseries.jsonl"])
    control_bridge_log_path = _find_first(
        root,
        [
            "artifacts/cyber_control_bridge.err.log",
            "artifacts/cyber_control_bridge.out.log",
            "cyber_control_bridge.err.log",
            "cyber_control_bridge.out.log",
        ],
    )
    control_decode_debug_path = _find_first(
        root,
        [
            "artifacts/control_decode_debug.jsonl",
            "artifacts/bridge_control_decode.jsonl",
            "control_decode_debug.jsonl",
            "bridge_control_decode.jsonl",
        ],
    )
    direct_control_apply_path = _find_first(
        root,
        [
            "artifacts/direct_bridge_control_apply.jsonl",
            "direct_bridge_control_apply.jsonl",
        ],
    )
    control_handoff_path = _find_first(
        root,
        [
            "artifacts/control_handoff_summary.json",
            "control_handoff_summary.json",
        ],
    )
    planning_summary_path = _find_first(
        root,
        [
            "artifacts/planning_topic_debug_summary.json",
            "planning_topic_debug_summary.json",
        ],
    )
    cyber_bridge_stats_path = _find_first(
        root,
        [
            "artifacts/cyber_bridge_stats.json",
            "cyber_bridge_stats.json",
        ],
    )
    control_attribution_path = _find_first(
        root,
        [
            "analysis/control_attribution/control_attribution_report.json",
            "control_attribution_report.json",
        ],
    )
    apollo_control_handoff_report_path = _find_first(
        root,
        [
            "analysis/apollo_control_handoff/apollo_control_handoff_report.json",
            "apollo_control_handoff_report.json",
        ],
    )
    summary = _read_json(summary_path)
    manifest = _read_json(manifest_path)
    control_handoff_debug = _read_json(control_handoff_path) if control_handoff_path else {}
    planning_debug = _read_json(planning_summary_path) if planning_summary_path else {}
    cyber_bridge_stats = _read_json(cyber_bridge_stats_path) if cyber_bridge_stats_path else {}
    control_attribution = _read_json(control_attribution_path) if control_attribution_path else {}
    apollo_control_handoff = (
        _read_json(apollo_control_handoff_report_path)
        if apollo_control_handoff_report_path
        else {}
    )
    rows = _read_rows(timeseries_path)
    control_bridge_log = _analyze_control_bridge_log(control_bridge_log_path)
    control_decode_debug = _analyze_control_decode_debug(control_decode_debug_path)
    direct_control_apply = _analyze_direct_control_apply_log(direct_control_apply_path)
    control_application = _control_application_metrics(rows)
    control_apply_delay = _control_apply_delay_metrics(
        control_application=control_application,
        direct_control_apply=direct_control_apply,
    )
    steer_error = _paired_abs_error_metrics(
        rows,
        "bridge_steer_mapped",
        "carla_steer_applied",
    )
    throttle_error = _paired_abs_error_metrics(
        rows,
        "throttle_mapped",
        "throttle_applied",
    )
    brake_error = _paired_abs_error_metrics(
        rows,
        "brake_mapped",
        "brake_applied",
    )

    missing_inputs: list[str] = []
    if not summary_path.exists():
        missing_inputs.append("summary")
    if timeseries_path is None:
        missing_inputs.append("timeseries")

    handoff = _control_handoff_status(summary)
    metrics = _summary_metrics(summary)
    summary_latency = _num(metrics.get("control_latency_p95_ms"))
    timeseries_latency = _percentile(_series(rows, "control_latency_ms"), 0.95)
    decode_latency = _num(control_decode_debug.get("control_latency_p95_ms"))
    control_latency_p95_ms = _first_number(summary_latency, timeseries_latency, decode_latency)
    report_metrics = {
        "control_latency_p95_ms": control_latency_p95_ms,
        "control_latency_source": _control_latency_source(
            summary_latency=summary_latency,
            timeseries_latency=timeseries_latency,
            decode_latency=decode_latency,
        ),
        **control_application,
        **control_apply_delay,
        **_route_progress_metrics(rows),
        "brake_throttle_conflict_frames": _brake_throttle_conflicts(rows),
        **_applied_throttle_brake_switch_metrics(rows),
        "mapped_applied_steer_abs_error_p95": steer_error["best_lag_abs_error_p95"],
        "mapped_applied_steer_abs_error_p95_zero_lag": steer_error["zero_lag_abs_error_p95"],
        "mapped_applied_steer_best_lag_frames": steer_error["best_lag_frames"],
        "mapped_applied_steer_best_lag_sample_count": steer_error["best_lag_sample_count"],
        "mapped_applied_throttle_abs_error_p95": throttle_error["best_lag_abs_error_p95"],
        "mapped_applied_throttle_abs_error_p95_zero_lag": throttle_error["zero_lag_abs_error_p95"],
        "mapped_applied_throttle_best_lag_frames": throttle_error["best_lag_frames"],
        "mapped_applied_throttle_best_lag_sample_count": throttle_error["best_lag_sample_count"],
        "mapped_applied_brake_abs_error_p95": brake_error["best_lag_abs_error_p95"],
        "mapped_applied_brake_abs_error_p95_zero_lag": brake_error["zero_lag_abs_error_p95"],
        "mapped_applied_brake_best_lag_frames": brake_error["best_lag_frames"],
        "mapped_applied_brake_best_lag_sample_count": brake_error["best_lag_sample_count"],
        "lateral_guard_apply_count": _bool_count(rows, "lateral_guard_applied"),
        "trajectory_contract_guard_apply_count": _bool_count(
            rows,
            "trajectory_contract_guard_applied",
            "trajectory_contract_lateral_guard_applied",
        ),
        "link_delay_decomposition": _link_delay_decomposition(
            summary=summary,
            control_handoff_debug=control_handoff_debug,
            planning_debug=planning_debug,
            control_bridge_log=control_bridge_log,
            cyber_bridge_stats=cyber_bridge_stats,
        ),
        "control_bridge_log": control_bridge_log,
        "control_decode_debug": control_decode_debug,
        "direct_control_apply_log": direct_control_apply,
        "control_attribution": _control_attribution_summary(control_attribution, control_attribution_path),
    }
    report_metrics["oscillation_decomposition"] = _oscillation_decomposition(
        rows=rows,
        control_bridge_log=control_bridge_log,
        thresholds=active_thresholds,
    )
    report_metrics["control_mapping_claim_boundary"] = _control_mapping_claim_boundary(
        rows=rows,
        summary=summary,
        manifest=manifest,
        cyber_bridge_stats=cyber_bridge_stats,
    )
    missing_fields = _missing_control_fields(rows)
    external_control_evidence_available = _external_control_evidence_available(
        control_bridge_log=control_bridge_log,
        control_decode_debug=control_decode_debug,
    )
    status, failure_reason, verdict_missing, warnings = _verdict(
        summary=summary,
        handoff_status=handoff,
        metrics=report_metrics,
        missing_inputs=missing_inputs,
        missing_fields=missing_fields,
        external_control_evidence_available=external_control_evidence_available,
        thresholds=active_thresholds,
    )
    missing_fields.extend(verdict_missing)
    return {
        "schema_version": CONTROL_HEALTH_REPORT_SCHEMA_VERSION,
        "run_id": _first_text(summary, "run_id", manifest, "run_id", default=root.name),
        "scenario_class": _first_text(summary, "scenario_class", manifest, "scenario_class"),
        "route_id": _first_text(summary, "route_id", manifest, "route_id"),
        "status": status,
        "failure_reason": failure_reason,
        "runtime_contract_status": _runtime_contract_status(summary, manifest),
        "routing_materialized": _summary_bool(summary, "routing_materialized"),
        "planning_materialized": _summary_bool(summary, "planning_materialized"),
        "control_handoff_status": handoff,
        "apollo_control_handoff_status": apollo_control_handoff.get("verdict"),
        "apollo_control_handoff_failure_stage": apollo_control_handoff.get("failure_stage"),
        "apollo_control_handoff_blocking_reasons": list(
            apollo_control_handoff.get("blocking_reasons") or []
        ),
        "apollo_control_handoff_report_path": (
            str(apollo_control_handoff_report_path)
            if apollo_control_handoff_report_path
            else None
        ),
        "raw_mapped_applied_control_available": not missing_fields or external_control_evidence_available,
        "raw_mapped_applied_control_source": (
            "timeseries"
            if not missing_fields
            else ("bridge_decode_plus_timeseries" if external_control_evidence_available else None)
        ),
        "metrics": report_metrics,
        "missing_fields": sorted(set(missing_fields)),
        "missing_inputs": sorted(set(missing_inputs)),
        "warnings": sorted(set(warnings)),
        "thresholds": active_thresholds,
        "source": {
            "run_dir": str(root),
            "summary_path": str(summary_path) if summary_path.exists() else None,
            "manifest_path": str(manifest_path) if manifest_path.exists() else None,
            "timeseries_path": str(timeseries_path) if timeseries_path else None,
            "control_bridge_log_path": str(control_bridge_log_path) if control_bridge_log_path else None,
            "control_decode_debug_path": str(control_decode_debug_path) if control_decode_debug_path else None,
            "direct_control_apply_path": str(direct_control_apply_path) if direct_control_apply_path else None,
            "control_handoff_path": str(control_handoff_path) if control_handoff_path else None,
            "planning_summary_path": str(planning_summary_path) if planning_summary_path else None,
            "cyber_bridge_stats_path": str(cyber_bridge_stats_path) if cyber_bridge_stats_path else None,
            "control_attribution_path": str(control_attribution_path) if control_attribution_path else None,
            "apollo_control_handoff_report_path": (
                str(apollo_control_handoff_report_path)
                if apollo_control_handoff_report_path
                else None
            ),
        },
        "interpretation_boundary": (
            "Control-health report checks handoff and raw/mapped/applied command evidence only. "
            "It does not prove Apollo planning quality or full perception/localization."
        ),
    }


def _control_attribution_summary(
    report: Mapping[str, Any],
    path: Path | None,
) -> dict[str, Any]:
    attribution = report.get("attribution") if isinstance(report.get("attribution"), Mapping) else {}
    verdict = report.get("verdict") if isinstance(report.get("verdict"), Mapping) else {}
    return {
        "available": bool(report),
        "path": str(path) if path else None,
        "status": verdict.get("status"),
        "dominant_breakpoint": attribution.get("dominant_breakpoint"),
        "raw_control_available": report.get("raw_control_available"),
        "mapped_control_available": report.get("mapped_control_available"),
        "applied_control_available": report.get("applied_control_available"),
        "vehicle_response_available": report.get("vehicle_response_available"),
    }


def _oscillation_decomposition(
    *,
    rows: Sequence[Mapping[str, Any]],
    control_bridge_log: Mapping[str, Any],
    thresholds: Mapping[str, float],
) -> dict[str, Any]:
    raw = _command_oscillation_layer(
        rows,
        layer="apollo_raw_command",
        throttle_fields=("throttle_raw", "apollo_desired_throttle", "commanded_throttle_raw"),
        brake_fields=("brake_raw", "apollo_desired_brake", "commanded_brake_raw"),
        steer_fields=("apollo_steer_raw", "apollo_desired_steer", "steering_target", "source_steer"),
        max_switch_count=thresholds["max_raw_throttle_brake_switch_count"],
    )
    mapped = _command_oscillation_layer(
        rows,
        layer="bridge_mapped_command",
        throttle_fields=("throttle_mapped", "mapped_throttle_cmd", "commanded_throttle"),
        brake_fields=("brake_mapped", "mapped_brake_cmd", "commanded_brake"),
        steer_fields=("bridge_steer_mapped", "mapped_carla_steer_cmd", "mapped_steer"),
        max_switch_count=thresholds["max_mapped_throttle_brake_switch_count"],
    )
    applied = _command_oscillation_layer(
        rows,
        layer="carla_applied_command",
        throttle_fields=("throttle_applied", "measured_throttle"),
        brake_fields=("brake_applied", "measured_brake"),
        steer_fields=("carla_steer_applied", "measured_steer"),
        max_switch_count=thresholds["max_applied_throttle_brake_switch_count"],
    )
    vehicle = _vehicle_response_oscillation_layer(
        rows,
        max_switch_count=thresholds["max_vehicle_response_sign_switch_count"],
    )
    cadence = _bridge_apply_cadence_layer(control_bridge_log, thresholds)
    layers = {
        "apollo_raw_command": raw,
        "bridge_mapped_command": mapped,
        "carla_applied_command": applied,
        "vehicle_response": vehicle,
        "bridge_apply_cadence": cadence,
    }
    priority = [
        "bridge_apply_cadence",
        "apollo_raw_command",
        "bridge_mapped_command",
        "carla_applied_command",
        "vehicle_response",
    ]
    dominant = "insufficient_data"
    for name in priority:
        status = layers[name].get("status")
        if status == "fail":
            dominant = name
            break
    else:
        for name in priority:
            if layers[name].get("status") == "warn":
                dominant = name
                break
        else:
            if any(layer.get("status") == "pass" for layer in layers.values()):
                dominant = "none"
    return {
        "layers": layers,
        "dominant_oscillation_layer": dominant,
        "interpretation": (
            "Oscillation is decomposed into Apollo raw command, bridge mapped command, "
            "CARLA applied command, vehicle response, and bridge apply cadence. Do not "
            "smooth or clamp commands to hide reference-line/localization errors."
        ),
    }


def _command_oscillation_layer(
    rows: Sequence[Mapping[str, Any]],
    *,
    layer: str,
    throttle_fields: Sequence[str],
    brake_fields: Sequence[str],
    steer_fields: Sequence[str],
    max_switch_count: float,
) -> dict[str, Any]:
    states: list[str] = []
    steer_values: list[float] = []
    conflict_frames = 0
    sample_count = 0
    resolved_fields = {
        "throttle": _first_present_field(rows, throttle_fields),
        "brake": _first_present_field(rows, brake_fields),
        "steer": _first_present_field(rows, steer_fields),
    }
    for row in rows:
        throttle = _first_row_number(row, *throttle_fields)
        brake = _first_row_number(row, *brake_fields)
        steer = _first_row_number(row, *steer_fields)
        if throttle is None and brake is None and steer is None:
            continue
        sample_count += 1
        throttle_f = float(throttle or 0.0)
        brake_f = float(brake or 0.0)
        if throttle_f > 0.05 and brake_f > 0.05:
            conflict_frames += 1
            states.append("conflict")
        elif throttle_f > 0.05:
            states.append("throttle")
        elif brake_f > 0.05:
            states.append("brake")
        else:
            states.append("coast")
        if steer is not None:
            steer_values.append(float(steer))
    compact_states = [state for state in states if state in {"throttle", "brake"}]
    switch_count = sum(1 for prev, cur in zip(compact_states, compact_states[1:]) if prev != cur)
    steer_sign_switch_count = _sign_switch_count(steer_values, deadband=0.01)
    if sample_count <= 0:
        status = "insufficient_data"
        reason = "missing_layer_fields"
    elif conflict_frames > 0:
        status = "fail"
        reason = "throttle_brake_conflict"
    elif switch_count > max_switch_count:
        status = "fail"
        reason = "throttle_brake_switching"
    elif steer_sign_switch_count > max_switch_count:
        status = "warn"
        reason = "steer_sign_switching"
    else:
        status = "pass"
        reason = None
    return {
        "layer": layer,
        "status": status,
        "reason": reason,
        "sample_count": sample_count,
        "resolved_fields": resolved_fields,
        "throttle_brake_switch_count": switch_count if sample_count else None,
        "throttle_brake_conflict_frames": conflict_frames if sample_count else None,
        "steer_sign_switch_count": steer_sign_switch_count if steer_values else None,
    }


def _vehicle_response_oscillation_layer(
    rows: Sequence[Mapping[str, Any]],
    *,
    max_switch_count: float,
) -> dict[str, Any]:
    yaw_fields = ("ego_yaw_rate", "ego_yaw_rate_rad_s", "yaw_rate_rps", "measured_yaw_rate_rps")
    accel_fields = (
        "measured_forward_accel_mps2",
        "ego_accel_mps2",
        "linear_acceleration_x",
        "forward_accel_mps2",
    )
    yaw_values = [value for row in rows if (value := _first_row_number(row, *yaw_fields)) is not None]
    accel_values = [value for row in rows if (value := _first_row_number(row, *accel_fields)) is not None]
    yaw_switches = _sign_switch_count(yaw_values, deadband=0.01)
    accel_switches = _sign_switch_count(accel_values, deadband=0.05)
    sample_count = max(len(yaw_values), len(accel_values))
    if sample_count <= 0:
        status = "insufficient_data"
        reason = "missing_vehicle_response_fields"
    elif yaw_switches > max_switch_count or accel_switches > max_switch_count:
        status = "warn"
        reason = "vehicle_response_sign_switching"
    else:
        status = "pass"
        reason = None
    return {
        "layer": "vehicle_response",
        "status": status,
        "reason": reason,
        "sample_count": sample_count,
        "resolved_fields": {
            "yaw_rate": _first_present_field(rows, yaw_fields),
            "forward_accel": _first_present_field(rows, accel_fields),
        },
        "yaw_rate_sign_switch_count": yaw_switches if yaw_values else None,
        "forward_accel_sign_switch_count": accel_switches if accel_values else None,
    }


def _bridge_apply_cadence_layer(
    control_bridge_log: Mapping[str, Any],
    thresholds: Mapping[str, float],
) -> dict[str, Any]:
    if not isinstance(control_bridge_log, Mapping) or control_bridge_log.get("available") is not True:
        return {
            "layer": "bridge_apply_cadence",
            "status": "insufficient_data",
            "reason": "control_bridge_log_missing",
        }
    configured = _num(control_bridge_log.get("configured_apply_hz"))
    observed = _num(control_bridge_log.get("apply_world_frame_hz"))
    drop_ratio = _num(control_bridge_log.get("same_frame_drop_ratio"))
    coverage_ratio = _num(control_bridge_log.get("apply_frame_coverage_ratio"))
    sync_to_world_tick = _parse_bool(control_bridge_log.get("sync_to_world_tick"))
    min_hz = _num(thresholds.get("min_control_bridge_apply_frame_hz"))
    low_cadence = bool(observed is not None and min_hz is not None and observed < min_hz)
    expected_same_frame_drop = bool(
        sync_to_world_tick is True
        and coverage_ratio is not None
        and coverage_ratio >= thresholds["min_control_bridge_apply_frame_coverage_ratio"]
    )
    high_drop = bool(
        drop_ratio is not None
        and drop_ratio > thresholds["max_control_bridge_same_frame_drop_ratio"]
        and not expected_same_frame_drop
    )
    if low_cadence:
        status = "fail"
        reason = "control_bridge_world_frame_cadence_low"
    elif high_drop:
        status = "fail"
        reason = "control_bridge_drop_same_frame_high"
    else:
        status = "pass"
        reason = None
    return {
        "layer": "bridge_apply_cadence",
        "status": status,
        "reason": reason,
        "configured_apply_hz": configured,
        "observed_apply_world_frame_hz": observed,
        "sync_to_world_tick": sync_to_world_tick,
        "same_frame_drop_ratio": drop_ratio,
        "apply_frame_coverage_ratio": coverage_ratio,
        "same_frame_drop_expected_from_sync_tick": expected_same_frame_drop,
    }


def _control_mapping_claim_boundary(
    *,
    rows: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
    cyber_bridge_stats: Mapping[str, Any],
) -> dict[str, Any]:
    mode = _first_text(manifest, "actuator_mapping_mode", summary, "actuator_mapping_mode")
    if mode is None:
        mode = _first_present_value(rows, ("actuator_mapping_mode",))
    if mode is None:
        stats_mapping = cyber_bridge_stats.get("actuator_mapping")
        if isinstance(stats_mapping, Mapping):
            mode = _text_or_none(stats_mapping.get("mode"))
    steer_scale = _first_number(
        manifest.get("steer_scale"),
        summary.get("steer_scale"),
        _first_present_number(rows, ("steer_scale",)),
    )
    steering_sign = _first_number(
        manifest.get("steering_sign"),
        summary.get("steering_sign"),
        _first_present_number(rows, ("steering_sign",)),
    )
    calibration_profile_id = _first_text(
        manifest,
        "calibration_profile_id",
        summary,
        "calibration_profile_id",
    )
    stats_mapping = cyber_bridge_stats.get("actuator_mapping")
    calibration_status = None
    if isinstance(stats_mapping, Mapping):
        calibration = stats_mapping.get("calibration")
        if isinstance(calibration, Mapping):
            calibration_status = calibration.get("loaded") or calibration.get("status")
    claim_grade = bool(str(mode or "").lower() == "physical" and calibration_profile_id)
    steering_parameters_source = (
        "calibration_profile" if calibration_profile_id else "runtime_config_or_unknown"
    )
    warnings: list[str] = []
    if str(mode or "").lower() == "legacy":
        warnings.append("legacy_mapping_smoke_only")
    if not calibration_profile_id:
        warnings.append("calibration_profile_missing")
        warnings.append("steering_parameters_not_backed_by_calibration_profile")
    return {
        "actuator_mapping_mode": mode,
        "steer_scale": steer_scale,
        "steering_sign": steering_sign,
        "steering_parameters_source": steering_parameters_source,
        "calibration_profile_id": calibration_profile_id,
        "calibration_loaded_or_status": calibration_status,
        "claim_grade_control_mapping": claim_grade,
        "warnings": warnings,
        "interpretation": (
            "Legacy mapping can support smoke/debug evidence only. Claim-grade actuation "
            "requires physical/calibrated mapping or an explicit vehicle calibration profile."
        ),
    }


def _first_present_field(rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> str | None:
    for field in fields:
        if any(row.get(field) not in {None, ""} for row in rows):
            return field
    return None


def _first_present_value(rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> str | None:
    for field in fields:
        for row in rows:
            value = row.get(field)
            if value not in {None, ""}:
                return str(value)
    return None


def _first_present_number(rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> float | None:
    for field in fields:
        for row in rows:
            value = _num(row.get(field))
            if value is not None:
                return value
    return None


def _sign_switch_count(values: Sequence[float], *, deadband: float) -> int:
    signs: list[int] = []
    for value in values:
        number = float(value)
        if abs(number) <= deadband:
            continue
        signs.append(1 if number > 0.0 else -1)
    return sum(1 for prev, cur in zip(signs, signs[1:]) if prev != cur)


def write_control_health_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "control_health_report.json"
    md_path = output_dir / "control_health_summary.md"
    json_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_markdown(report), encoding="utf-8")
    return {
        "control_health_report": str(json_path),
        "control_health_summary": str(md_path),
    }


def _verdict(
    *,
    summary: Mapping[str, Any],
    handoff_status: str | None,
    metrics: Mapping[str, Any],
    missing_inputs: Sequence[str],
    missing_fields: Sequence[str],
    external_control_evidence_available: bool,
    thresholds: Mapping[str, float],
) -> tuple[str, str | None, list[str], list[str]]:
    verdict_missing: list[str] = []
    warnings: list[str] = []
    if missing_inputs:
        return "insufficient_data", "missing_control_inputs", verdict_missing, warnings
    runtime_status = _runtime_contract_status(summary, {})
    if runtime_status != "aligned":
        verdict_missing.append("runtime_contract.status")
        if runtime_status:
            return "fail", "runtime_contract_not_aligned", verdict_missing, warnings
        return "insufficient_data", "runtime_contract_missing_status", verdict_missing, warnings
    routing = _summary_bool(summary, "routing_materialized")
    planning = _summary_bool(summary, "planning_materialized")
    if routing is None:
        verdict_missing.append("routing_materialized")
    if planning is None:
        verdict_missing.append("planning_materialized")
    if handoff_status is None:
        verdict_missing.append("control_handoff_status")
    if verdict_missing:
        return "insufficient_data", "missing_link_health_fields", verdict_missing, warnings
    if routing is False:
        return "fail", "routing_missing", verdict_missing, warnings
    if planning is False:
        return "fail", "planning_missing", verdict_missing, warnings
    if handoff_status != EXPECTED_HANDOFF_STATUS:
        return "fail", "control_handoff_not_consuming", verdict_missing, warnings
    if missing_fields:
        missing_applied_fields = [field for field in missing_fields if field in APPLIED_CONTROL_TRACE_FIELDS]
        if external_control_evidence_available and not missing_applied_fields:
            warnings.append("external_control_trace_from_bridge_artifacts")
        else:
            return "insufficient_data", "missing_control_trace_fields", verdict_missing, warnings

    if missing_fields and not external_control_evidence_available:
        return "insufficient_data", "missing_control_trace_fields", verdict_missing, warnings

    mapped_frames = _num(metrics.get("nonzero_mapped_control_frames"))
    applied_frames = _num(metrics.get("nonzero_applied_control_frames"))
    if mapped_frames is None:
        verdict_missing.append("nonzero_mapped_control_frames")
    if applied_frames is None:
        verdict_missing.append("nonzero_applied_control_frames")
    if verdict_missing:
        return "insufficient_data", "missing_control_metrics", verdict_missing, warnings
    if mapped_frames > 0 and applied_frames <= 0:
        return "fail", "control_apply_missing", verdict_missing, warnings
    apply_delay = _num(metrics.get("control_apply_observation_delay_s"))
    if (
        apply_delay is not None
        and apply_delay > thresholds["max_control_apply_observation_delay_s"]
    ):
        warnings.append("control_apply_observation_delay_high")
    warnings.extend(_control_bridge_log_warnings(metrics, thresholds))

    progress_after_apply = _num(metrics.get("route_s_after_first_applied_control_delta_m"))
    if (
        applied_frames is not None
        and applied_frames > 0
        and progress_after_apply is not None
        and progress_after_apply < thresholds["min_route_progress_after_applied_control_m"]
    ):
        warnings.append("route_progress_stalled_after_control")
    oscillation = metrics.get("oscillation_decomposition")
    oscillation_layers = oscillation.get("layers") if isinstance(oscillation, Mapping) else {}
    if isinstance(oscillation_layers, Mapping):
        raw_layer = oscillation_layers.get("apollo_raw_command")
        mapped_layer = oscillation_layers.get("bridge_mapped_command")
        if isinstance(raw_layer, Mapping) and raw_layer.get("status") == "fail":
            return "fail", "apollo_raw_command_oscillation", verdict_missing, warnings
        if isinstance(mapped_layer, Mapping) and mapped_layer.get("status") == "fail":
            return "fail", "bridge_mapped_command_oscillation", verdict_missing, warnings

    conflict_frames = _num(metrics.get("brake_throttle_conflict_frames"))
    if conflict_frames is not None and conflict_frames > thresholds["max_brake_throttle_conflict_frames"]:
        return "fail", "brake_throttle_conflict", verdict_missing, warnings
    switch_count = _num(metrics.get("applied_throttle_brake_switch_count"))
    if (
        switch_count is not None
        and switch_count > thresholds["max_applied_throttle_brake_switch_count"]
    ):
        if "control_bridge_world_frame_cadence_low" in warnings:
            return "fail", "control_bridge_world_frame_cadence_low", verdict_missing, warnings
        if "control_bridge_drop_same_frame_high" in warnings:
            return "fail", "control_bridge_drop_same_frame_high", verdict_missing, warnings
        return "fail", "applied_actuation_oscillation", verdict_missing, warnings
    for field in (
        "mapped_applied_steer_abs_error_p95",
        "mapped_applied_throttle_abs_error_p95",
        "mapped_applied_brake_abs_error_p95",
    ):
        value = _num(metrics.get(field))
        if value is None:
            if external_control_evidence_available and missing_fields:
                axis = field.replace("mapped_applied_", "").replace("_abs_error_p95", "")
                warnings.append(f"mapped_applied_{axis}_pairing_unavailable_without_p0_raw_mapped")
            else:
                verdict_missing.append(field)
        elif value > thresholds["max_mapped_applied_abs_error_p95"]:
            axis = field.replace("mapped_applied_", "").replace("_abs_error_p95", "")
            warnings.append(f"mapped_applied_{axis}_mismatch")
    latency = _num(metrics.get("control_latency_p95_ms"))
    if latency is None:
        warnings.append("control_latency_missing")
    elif latency > thresholds["max_control_latency_p95_ms"]:
        warnings.append("control_latency_high")
    if verdict_missing:
        return "insufficient_data", "missing_control_metrics", verdict_missing, warnings
    if warnings:
        return "warn", "control_health_warn", verdict_missing, warnings
    return "pass", None, verdict_missing, warnings


def _missing_control_fields(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    if not rows:
        return list(CONTROL_TRACE_FIELDS)
    missing: list[str] = []
    for field in CONTROL_TRACE_FIELDS:
        if not any(row.get(field) not in {None, ""} for row in rows):
            missing.append(field)
    return missing


def _external_control_evidence_available(
    *,
    control_bridge_log: Mapping[str, Any],
    control_decode_debug: Mapping[str, Any],
) -> bool:
    """Return true when external-stack control can be diagnosed outside P0.

    For Apollo/Autoware runs the harness does not own actuation, so P0
    raw/mapped fields may intentionally be null. In that case bridge decode
    artifacts plus CARLA apply logs are the authoritative external control
    evidence; they are weaker than per-frame P0 pairing but better than
    treating the run as blind.
    """

    decode_available = bool(control_decode_debug.get("available") is True)
    decode_rows = _num(control_decode_debug.get("parsed_line_count"))
    if decode_rows is None:
        decode_rows = _num(control_decode_debug.get("line_count"))
    bridge_available = bool(control_bridge_log.get("available") is True)
    applied_count = _num(control_bridge_log.get("final_applied_count"))
    return bool(decode_available and (decode_rows or 0) > 0 and bridge_available and (applied_count or 0) > 0)


def _brake_throttle_conflicts(rows: Sequence[Mapping[str, Any]]) -> int | None:
    seen = False
    conflicts = 0
    for row in rows:
        throttle = _num(row.get("throttle_applied"))
        brake = _num(row.get("brake_applied"))
        if throttle is None or brake is None:
            continue
        seen = True
        if throttle > 0.05 and brake > 0.05:
            conflicts += 1
    return conflicts if seen else None


def _applied_throttle_brake_switch_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    states: list[str] = []
    for row in rows:
        throttle = _num(row.get("throttle_applied"))
        brake = _num(row.get("brake_applied"))
        if throttle is None and brake is None:
            continue
        throttle_f = float(throttle or 0.0)
        brake_f = float(brake or 0.0)
        if throttle_f > 0.05 and brake_f > 0.05:
            states.append("conflict")
        elif throttle_f > 0.05:
            states.append("throttle")
        elif brake_f > 0.05:
            states.append("brake")
        else:
            states.append("coast")
    compact_states = [state for state in states if state in {"throttle", "brake"}]
    switch_count = sum(1 for prev, cur in zip(compact_states, compact_states[1:]) if prev != cur)
    return {
        "applied_throttle_brake_switch_count": switch_count if states else None,
        "applied_throttle_frames": states.count("throttle"),
        "applied_brake_frames": states.count("brake"),
        "applied_throttle_brake_conflict_frames": states.count("conflict"),
    }


def _control_application_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    mapped_frames = 0
    applied_frames = 0
    first_mapped_time: float | None = None
    first_applied_time: float | None = None
    seen = False
    for row in rows:
        mapped_active = _row_has_nonzero_control(
            row,
            throttle_field="throttle_mapped",
            brake_field="brake_mapped",
            steer_field="bridge_steer_mapped",
        )
        applied_active = _row_has_nonzero_control(
            row,
            throttle_field="throttle_applied",
            brake_field="brake_applied",
            steer_field="carla_steer_applied",
        )
        if mapped_active is None and applied_active is None:
            continue
        seen = True
        row_time = _row_time_s(row)
        if mapped_active is True:
            mapped_frames += 1
            if first_mapped_time is None:
                first_mapped_time = row_time
        if applied_active is True:
            applied_frames += 1
            if first_applied_time is None:
                first_applied_time = row_time
    delay = None
    if first_mapped_time is not None and first_applied_time is not None:
        delay = first_applied_time - first_mapped_time
    return {
        "nonzero_mapped_control_frames": mapped_frames if seen else None,
        "nonzero_applied_control_frames": applied_frames if seen else None,
        "first_nonzero_mapped_control_s": first_mapped_time,
        "first_nonzero_applied_control_s": first_applied_time,
        "control_apply_observation_delay_s": delay,
        "control_apply_observation_delay_source": "timeseries_applied_control",
    }


def _link_delay_decomposition(
    *,
    summary: Mapping[str, Any],
    control_handoff_debug: Mapping[str, Any],
    planning_debug: Mapping[str, Any],
    control_bridge_log: Mapping[str, Any],
    cyber_bridge_stats: Mapping[str, Any],
) -> dict[str, Any]:
    command_materialization = cyber_bridge_stats.get("command_materialization")
    command_gate = (
        command_materialization.get("gate_state")
        if isinstance(command_materialization, Mapping)
        else None
    )
    if not isinstance(command_gate, Mapping):
        command_gate = {}
    bridge_bind_s = _num(control_bridge_log.get("ego_bind_wall_s"))
    bridge_first_apply_s = _num(control_bridge_log.get("first_apply_wall_s"))
    routing_first_request_s = _first_number(
        summary.get("routing_first_request_at"),
        planning_debug.get("routing_first_response_after_last_routing_send_boundary_ts_sec"),
        planning_debug.get("routing_first_response_ts_sec"),
    )
    planning_first_message_s = _first_number(
        summary.get("planning_first_message_at"),
        planning_debug.get("first_msg_ts_sec"),
    )
    planning_first_nonempty_s = _first_number(
        summary.get("planning_first_nonempty_at"),
        control_handoff_debug.get("planning_first_nonempty_at"),
        planning_debug.get("first_nonzero_trajectory_timestamp"),
    )
    control_first_consume_s = _first_number(
        summary.get("control_first_consume_at"),
        control_handoff_debug.get("control_first_consume_at"),
    )
    command_first_eval_s = _num(command_gate.get("first_eval_ts_sec"))
    command_first_eligible_s = _num(command_gate.get("first_eligible_ts_sec"))
    command_first_ready_s = _num(command_gate.get("first_ready_to_send_ts_sec"))
    segments = {
        "bridge_bind_to_routing_first_request_s": _positive_span(bridge_bind_s, routing_first_request_s),
        "command_gate_startup_delay_observed_s": _positive_span(
            command_first_eval_s,
            command_first_eligible_s,
        ),
        "command_gate_apollo_warmup_delay_observed_s": _positive_span(
            command_first_eligible_s,
            command_first_ready_s,
        ),
        "command_gate_ready_to_routing_first_request_s": _positive_span(
            command_first_ready_s,
            routing_first_request_s,
        ),
        "routing_first_request_to_planning_first_message_s": _positive_span(
            routing_first_request_s,
            planning_first_message_s,
        ),
        "routing_first_request_to_planning_nonempty_s": _positive_span(
            routing_first_request_s,
            planning_first_nonempty_s,
        ),
        "planning_nonempty_to_control_first_consume_s": _positive_span(
            planning_first_nonempty_s,
            control_first_consume_s,
        ),
        "control_first_consume_to_bridge_first_apply_s": _positive_span(
            control_first_consume_s,
            bridge_first_apply_s,
        ),
        "bridge_bind_to_first_apply_s": _positive_span(bridge_bind_s, bridge_first_apply_s),
    }
    stage_candidates = {
        key: value
        for key, value in segments.items()
        if value is not None
        and key
        not in {
            "bridge_bind_to_first_apply_s",
            "routing_first_request_to_planning_first_message_s",
        }
    }
    primary_stage = None
    if stage_candidates:
        primary_stage = max(stage_candidates.items(), key=lambda item: item[1])[0]
    return {
        "bridge_ego_bind_wall_s": bridge_bind_s,
        "routing_first_request_at_s": routing_first_request_s,
        "planning_first_message_at_s": planning_first_message_s,
        "planning_first_nonempty_at_s": planning_first_nonempty_s,
        "control_first_consume_at_s": control_first_consume_s,
        "bridge_first_apply_wall_s": bridge_first_apply_s,
        "command_gate_first_eval_ts_sec": command_first_eval_s,
        "command_gate_first_eligible_ts_sec": command_first_eligible_s,
        "command_gate_first_ready_to_send_ts_sec": command_first_ready_s,
        "command_gate_first_blocking_reason": _text_or_none(command_gate.get("first_blocking_reason")),
        "command_gate_last_blocking_reason": _text_or_none(command_gate.get("last_blocking_reason")),
        "command_gate_last_error_snapshot": _text_or_none(command_gate.get("last_error_snapshot")),
        **segments,
        "primary_delay_stage": primary_stage,
    }


def _positive_span(start: float | None, end: float | None) -> float | None:
    value = _span(start, end)
    if value is None:
        return None
    return max(0.0, value)


def _control_apply_delay_metrics(
    *,
    control_application: Mapping[str, Any],
    direct_control_apply: Mapping[str, Any],
) -> dict[str, Any]:
    timeseries_delay = _num(control_application.get("control_apply_observation_delay_s"))
    first_mapped = _num(control_application.get("first_nonzero_mapped_control_s"))
    first_direct_apply = _num(direct_control_apply.get("first_apply_ts_sec"))
    if direct_control_apply.get("available") is True and first_mapped is not None and first_direct_apply is not None:
        return {
            "control_apply_observation_delay_s": max(0.0, first_direct_apply - first_mapped),
            "control_apply_observation_delay_source": "direct_bridge_control_apply.jsonl",
            "control_apply_observation_delay_timeseries_s": timeseries_delay,
        }
    return {
        "control_apply_observation_delay_s": timeseries_delay,
        "control_apply_observation_delay_source": control_application.get("control_apply_observation_delay_source"),
        "control_apply_observation_delay_timeseries_s": timeseries_delay,
    }


def _route_progress_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    route_samples: list[tuple[int, float | None, float]] = []
    speed_samples: list[tuple[int, float | None, float]] = []
    first_mapped_idx: int | None = None
    first_applied_idx: int | None = None
    for index, row in enumerate(rows):
        row_time = _row_time_s(row)
        route_s = _first_row_number(row, "route_s", "nearest_route_s")
        if route_s is not None:
            route_samples.append((index, row_time, route_s))
        speed = _first_row_number(row, "ego_speed", "speed_mps", "ego_speed_mps")
        if speed is not None:
            speed_samples.append((index, row_time, speed))
        if first_mapped_idx is None and _row_has_nonzero_control(
            row,
            throttle_field="throttle_mapped",
            brake_field="brake_mapped",
            steer_field="bridge_steer_mapped",
        ):
            first_mapped_idx = index
        if first_applied_idx is None and _row_has_nonzero_control(
            row,
            throttle_field="throttle_applied",
            brake_field="brake_applied",
            steer_field="carla_steer_applied",
        ):
            first_applied_idx = index

    route_values = [sample[2] for sample in route_samples]
    speed_values = [sample[2] for sample in speed_samples]
    return {
        "route_s_start_m": route_values[0] if route_values else None,
        "route_s_end_m": route_values[-1] if route_values else None,
        "route_s_delta_m": (route_values[-1] - route_values[0]) if len(route_values) >= 2 else None,
        "route_s_range_m": (max(route_values) - min(route_values)) if route_values else None,
        "route_s_after_first_mapped_control_delta_m": _sample_delta_from_index(
            route_samples,
            first_mapped_idx,
        ),
        "route_s_after_first_applied_control_delta_m": _sample_delta_from_index(
            route_samples,
            first_applied_idx,
        ),
        "ego_speed_mean_mps": _mean(speed_values),
        "ego_speed_p95_mps": _percentile(speed_values, 0.95),
        "ego_speed_max_mps": max(speed_values) if speed_values else None,
        "stopped_ratio": _stopped_ratio(speed_values),
        "stopped_ratio_after_first_applied_control": _stopped_ratio(
            [sample[2] for sample in speed_samples if first_applied_idx is not None and sample[0] >= first_applied_idx]
        ),
    }


def _sample_delta_from_index(
    samples: Sequence[tuple[int, float | None, float]],
    start_index: int | None,
) -> float | None:
    if start_index is None:
        return None
    values = [value for index, _time_s, value in samples if index >= start_index]
    if len(values) < 2:
        return None
    return values[-1] - values[0]


def _first_row_number(row: Mapping[str, Any], *fields: str) -> float | None:
    for field in fields:
        value = _num(row.get(field))
        if value is not None:
            return value
    return None


_CONTROL_BRIDGE_LOG_LINE_RE = re.compile(
    r"^\[(?P<level>[A-Z]+)\] \[(?P<timestamp>[0-9]+(?:\.[0-9]+)?)\] "
    r"\[carla_control_bridge\]: (?P<message>.*)$"
)
_CONTROL_BRIDGE_KEY_VALUE_RE = re.compile(r"(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>[^,\s]+)")


def _analyze_control_bridge_log(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {"available": False}
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as exc:
        return {
            "available": False,
            "read_error": str(exc),
        }

    buffered_warning_count = 0
    no_ego_skip_count = 0
    first_ego_warning_wall_s: float | None = None
    last_ego_warning_wall_s: float | None = None
    bind_wall_s: float | None = None
    bind_actor_id: int | None = None
    bind_role: str | None = None
    configured_apply_hz: float | None = None
    sync_to_world_tick: bool | None = None
    apply_events: list[dict[str, Any]] = []

    for line in lines:
        parsed = _parse_control_bridge_log_line(line)
        if parsed is None:
            continue
        timestamp = parsed["timestamp"]
        message = str(parsed["message"])
        if "Bridge listening on" in message:
            values = _parse_control_bridge_key_values(message)
            configured_apply_hz = _num(values.get("apply_hz"))
            sync_to_world_tick = _parse_bool(values.get("sync_to_world_tick"))
            continue
        if "ego vehicle not found; control commands will be buffered" in message:
            buffered_warning_count += 1
            first_ego_warning_wall_s = _first_number(first_ego_warning_wall_s, timestamp)
            last_ego_warning_wall_s = timestamp
            continue
        if "no ego found yet; skip control" in message:
            no_ego_skip_count += 1
            first_ego_warning_wall_s = _first_number(first_ego_warning_wall_s, timestamp)
            last_ego_warning_wall_s = timestamp
            continue
        if message.startswith("control target bound "):
            values = _parse_control_bridge_key_values(message)
            bind_wall_s = timestamp
            bind_actor_id = _int(values.get("actor_id"))
            bind_role = values.get("role")
            continue
        if message.startswith("apply frame="):
            event = _parse_control_bridge_apply_event(timestamp, message)
            if event:
                apply_events.append(event)

    source_counts: dict[str, int] = {}
    for event in apply_events:
        source = str(event.get("source") or "unknown")
        source_counts[source] = source_counts.get(source, 0) + 1

    first_apply = apply_events[0] if apply_events else None
    last_apply = apply_events[-1] if apply_events else None
    first_pending = _first_event_with_source(apply_events, "pending")
    first_watchdog = _first_event_with_source(apply_events, "watchdog")
    first_nonzero_throttle = _first_event_with_nonzero_throttle(apply_events)
    last_rx_change = _last_rx_change_event(apply_events)
    final_drop = _event_int(last_apply, "drop_same_frame") if last_apply else None
    final_applied = _event_int(last_apply, "applied") if last_apply else None
    final_rx = _event_int(last_apply, "rx") if last_apply else None

    apply_wall_span_s = _span(
        _event_num(first_apply, "wall_s"),
        _event_num(last_apply, "wall_s"),
    )
    apply_frame_span = _span(
        _event_num(first_apply, "frame"),
        _event_num(last_apply, "frame"),
    )
    same_frame_drop_ratio = None
    if final_drop is not None and final_applied is not None and (final_drop + final_applied) > 0:
        same_frame_drop_ratio = final_drop / (final_drop + final_applied)
    apply_frame_coverage_ratio = None
    if final_applied is not None and apply_frame_span is not None and apply_frame_span > 0:
        apply_frame_coverage_ratio = min(1.0, final_applied / apply_frame_span)
    bind_to_first_apply_s = _span(bind_wall_s, _event_num(first_apply, "wall_s"))
    rx_stopped_before_watchdog_s = _span(
        _event_num(last_rx_change, "wall_s"),
        _event_num(first_watchdog, "wall_s"),
    )

    return {
        "available": True,
        "line_count": len(lines),
        "buffered_before_ego_count": buffered_warning_count,
        "no_ego_skip_count": no_ego_skip_count,
        "first_ego_warning_wall_s": first_ego_warning_wall_s,
        "last_ego_warning_wall_s": last_ego_warning_wall_s,
        "ego_bind_wall_s": bind_wall_s,
        "ego_bind_actor_id": bind_actor_id,
        "ego_bind_role": bind_role,
        "configured_apply_hz": configured_apply_hz,
        "sync_to_world_tick": sync_to_world_tick,
        "ego_bind_delay_s": _span(first_ego_warning_wall_s, bind_wall_s),
        "apply_log_count": len(apply_events),
        "apply_source_counts": source_counts,
        "first_apply_wall_s": _event_num(first_apply, "wall_s"),
        "first_pending_apply_wall_s": _event_num(first_pending, "wall_s"),
        "first_watchdog_apply_wall_s": _event_num(first_watchdog, "wall_s"),
        "first_nonzero_throttle_apply_wall_s": _event_num(first_nonzero_throttle, "wall_s"),
        "bind_to_first_apply_s": bind_to_first_apply_s,
        "first_pending_to_watchdog_s": _span(
            _event_num(first_pending, "wall_s"),
            _event_num(first_watchdog, "wall_s"),
        ),
        "last_rx_change_wall_s": _event_num(last_rx_change, "wall_s"),
        "rx_stopped_before_watchdog_s": rx_stopped_before_watchdog_s,
        "apply_frame_start": _event_int(first_apply, "frame"),
        "apply_frame_end": _event_int(last_apply, "frame"),
        "apply_frame_span": apply_frame_span,
        "apply_wall_span_s": apply_wall_span_s,
        "apply_world_frame_hz": _safe_div(apply_frame_span, apply_wall_span_s),
        "apply_frame_coverage_ratio": apply_frame_coverage_ratio,
        "final_rx_count": final_rx,
        "final_applied_count": final_applied,
        "final_drop_same_frame_count": final_drop,
        "same_frame_drop_ratio": same_frame_drop_ratio,
        "max_rx_count": _max_event_int(apply_events, "rx"),
        "max_applied_count": _max_event_int(apply_events, "applied"),
        "max_drop_same_frame_count": _max_event_int(apply_events, "drop_same_frame"),
    }


def _parse_control_bridge_log_line(line: str) -> dict[str, Any] | None:
    match = _CONTROL_BRIDGE_LOG_LINE_RE.match(line.strip())
    if not match:
        return None
    timestamp = _num(match.group("timestamp"))
    if timestamp is None:
        return None
    return {
        "level": match.group("level"),
        "timestamp": timestamp,
        "message": match.group("message"),
    }


def _parse_control_bridge_key_values(message: str) -> dict[str, str]:
    return {
        match.group("key"): match.group("value")
        for match in _CONTROL_BRIDGE_KEY_VALUE_RE.finditer(message)
    }


def _parse_control_bridge_apply_event(timestamp: float, message: str) -> dict[str, Any] | None:
    values = _parse_control_bridge_key_values(message)
    frame = _int(values.get("frame"))
    if frame is None:
        return None
    return {
        "wall_s": timestamp,
        "frame": frame,
        "source": values.get("source"),
        "actor_id": _int(values.get("actor_id")),
        "role": values.get("role"),
        "throttle": _num(values.get("throttle")),
        "brake": _num(values.get("brake")),
        "rx": _int(values.get("rx")),
        "applied": _int(values.get("applied")),
        "drop_same_frame": _int(values.get("drop_same_frame")),
    }


def _control_bridge_log_warnings(
    metrics: Mapping[str, Any],
    thresholds: Mapping[str, float],
) -> list[str]:
    log_metrics = metrics.get("control_bridge_log")
    if not isinstance(log_metrics, Mapping) or log_metrics.get("available") is not True:
        return []
    warnings: list[str] = []
    frame_hz = _num(log_metrics.get("apply_world_frame_hz"))
    if (
        frame_hz is not None
        and frame_hz < thresholds["min_control_bridge_apply_frame_hz"]
        and _num(log_metrics.get("apply_log_count")) not in {None, 0}
    ):
        warnings.append("control_bridge_world_frame_cadence_low")
    drop_ratio = _num(log_metrics.get("same_frame_drop_ratio"))
    coverage_ratio = _num(log_metrics.get("apply_frame_coverage_ratio"))
    sync_to_world_tick = _parse_bool(log_metrics.get("sync_to_world_tick"))
    same_frame_drop_expected = bool(
        sync_to_world_tick is True
        and coverage_ratio is not None
        and coverage_ratio >= thresholds["min_control_bridge_apply_frame_coverage_ratio"]
    )
    if (
        drop_ratio is not None
        and drop_ratio > thresholds["max_control_bridge_same_frame_drop_ratio"]
        and not same_frame_drop_expected
    ):
        warnings.append("control_bridge_drop_same_frame_high")
    bind_delay = _num(log_metrics.get("ego_bind_delay_s"))
    if (
        bind_delay is not None
        and bind_delay > thresholds["max_control_bridge_ego_bind_delay_s"]
    ):
        warnings.append("control_bridge_ego_bind_delay_high")
    bind_to_first_apply = _num(log_metrics.get("bind_to_first_apply_s"))
    if (
        bind_to_first_apply is not None
        and bind_to_first_apply > thresholds["max_control_bridge_bind_to_first_apply_s"]
    ):
        warnings.append("control_bridge_first_apply_delay_high")
    if log_metrics.get("first_watchdog_apply_wall_s") is not None and _num(log_metrics.get("final_rx_count")):
        warnings.append("control_bridge_watchdog_after_rx_stop")
    return warnings


def _control_latency_source(
    *,
    summary_latency: float | None,
    timeseries_latency: float | None,
    decode_latency: float | None,
) -> str | None:
    if summary_latency is not None:
        return "summary.metrics.control_latency_p95_ms"
    if timeseries_latency is not None:
        return "timeseries.control_latency_ms"
    if decode_latency is not None:
        return "control_decode_debug_jsonl.control_latency_ms"
    return None


def _analyze_control_decode_debug(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {"available": False}

    control_latency_values: list[float] = []
    control_message_age_values: list[float] = []
    planning_message_age_values: list[float] = []
    line_count = 0
    parsed_count = 0
    malformed_count = 0

    try:
        with path.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if not line.strip():
                    continue
                line_count += 1
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    malformed_count += 1
                    continue
                if not isinstance(payload, Mapping):
                    malformed_count += 1
                    continue
                parsed_count += 1
                if (value := _nested_first_number(payload, _CONTROL_LATENCY_PATHS)) is not None:
                    control_latency_values.append(value)
                if (value := _nested_first_number(payload, _CONTROL_MESSAGE_AGE_PATHS)) is not None:
                    control_message_age_values.append(value)
                if (value := _nested_first_number(payload, _PLANNING_MESSAGE_AGE_PATHS)) is not None:
                    planning_message_age_values.append(value)
    except OSError as exc:
        return {
            "available": False,
            "path": str(path),
            "read_error": str(exc),
        }

    return {
        "available": True,
        "path": str(path),
        "line_count": line_count,
        "parsed_line_count": parsed_count,
        "malformed_line_count": malformed_count,
        "control_latency_sample_count": len(control_latency_values),
        "control_latency_p50_ms": _percentile(control_latency_values, 0.50),
        "control_latency_p95_ms": _percentile(control_latency_values, 0.95),
        "control_latency_max_ms": max(control_latency_values) if control_latency_values else None,
        "control_message_age_sample_count": len(control_message_age_values),
        "control_message_age_p95_ms": _percentile(control_message_age_values, 0.95),
        "planning_message_age_sample_count": len(planning_message_age_values),
        "planning_message_age_p95_ms": _percentile(planning_message_age_values, 0.95),
    }


def _analyze_direct_control_apply_log(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {"available": False}

    line_count = 0
    parsed_count = 0
    malformed_count = 0
    first_apply: Mapping[str, Any] | None = None
    last_apply: Mapping[str, Any] | None = None
    max_throttle: float | None = None
    max_brake: float | None = None
    source_counts: dict[str, int] = {}
    actor_id: int | None = None

    try:
        with path.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if not line.strip():
                    continue
                line_count += 1
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    malformed_count += 1
                    continue
                if not isinstance(payload, Mapping):
                    malformed_count += 1
                    continue
                parsed_count += 1
                if first_apply is None:
                    first_apply = payload
                last_apply = payload
                source = str(payload.get("source") or "unknown")
                source_counts[source] = source_counts.get(source, 0) + 1
                actor_id = _int(payload.get("actor_id")) if actor_id is None else actor_id
                throttle = _num(payload.get("throttle"))
                brake = _num(payload.get("brake"))
                if throttle is not None:
                    max_throttle = throttle if max_throttle is None else max(max_throttle, throttle)
                if brake is not None:
                    max_brake = brake if max_brake is None else max(max_brake, brake)
    except OSError as exc:
        return {
            "available": False,
            "path": str(path),
            "read_error": str(exc),
        }

    first_ts = _event_num(first_apply, "ts_sec")
    last_ts = _event_num(last_apply, "ts_sec")
    first_frame = _event_int(first_apply, "frame_id")
    last_frame = _event_int(last_apply, "frame_id")
    return {
        "available": True,
        "path": str(path),
        "line_count": line_count,
        "parsed_line_count": parsed_count,
        "malformed_line_count": malformed_count,
        "apply_count": parsed_count,
        "first_apply_ts_sec": first_ts,
        "last_apply_ts_sec": last_ts,
        "apply_wall_span_s": _span(first_ts, last_ts),
        "first_apply_frame_id": first_frame,
        "last_apply_frame_id": last_frame,
        "apply_frame_span": _span(
            float(first_frame) if first_frame is not None else None,
            float(last_frame) if last_frame is not None else None,
        ),
        "source_counts": source_counts,
        "actor_id": actor_id,
        "max_throttle": max_throttle,
        "max_brake": max_brake,
    }


_CONTROL_LATENCY_PATHS = (
    ("output_to_carla", "control_latency_ms"),
    ("parsed_control", "control_latency_ms"),
    ("control_latency_ms",),
)
_CONTROL_MESSAGE_AGE_PATHS = (
    ("output_to_carla", "control_message_age_ms"),
    ("parsed_control", "control_message_age_ms"),
    ("control_message_age_ms",),
)
_PLANNING_MESSAGE_AGE_PATHS = (
    ("output_to_carla", "planning_message_age_ms"),
    ("planning_message_age_ms",),
)


def _nested_first_number(
    payload: Mapping[str, Any],
    paths: Sequence[Sequence[str]],
) -> float | None:
    for path in paths:
        current: Any = payload
        for key in path:
            if not isinstance(current, Mapping):
                current = None
                break
            current = current.get(key)
        value = _num(current)
        if value is not None:
            return value
    return None


def _first_event_with_source(
    events: Sequence[Mapping[str, Any]],
    source: str,
) -> Mapping[str, Any] | None:
    for event in events:
        if event.get("source") == source:
            return event
    return None


def _first_event_with_nonzero_throttle(events: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    for event in events:
        throttle = _num(event.get("throttle"))
        if throttle is not None and throttle > 0.05:
            return event
    return None


def _last_rx_change_event(events: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    previous_rx: int | None = None
    last_change: Mapping[str, Any] | None = None
    for event in events:
        rx = _int(event.get("rx"))
        if rx is None:
            continue
        if previous_rx is None or rx != previous_rx:
            last_change = event
            previous_rx = rx
    return last_change


def _event_num(event: Mapping[str, Any] | None, key: str) -> float | None:
    if event is None:
        return None
    return _num(event.get(key))


def _event_int(event: Mapping[str, Any] | None, key: str) -> int | None:
    if event is None:
        return None
    return _int(event.get(key))


def _max_event_int(events: Sequence[Mapping[str, Any]], key: str) -> int | None:
    values = [value for event in events if (value := _int(event.get(key))) is not None]
    return max(values) if values else None


def _span(start: float | None, end: float | None) -> float | None:
    if start is None or end is None:
        return None
    return end - start


def _safe_div(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in {None, 0}:
        return None
    return numerator / denominator


def _int(value: Any) -> int | None:
    number = _num(value)
    if number is None:
        return None
    return int(number)


def _mean(values: Sequence[float]) -> float | None:
    cleaned = [value for value in values if math.isfinite(float(value))]
    return sum(cleaned) / len(cleaned) if cleaned else None


def _stopped_ratio(values: Sequence[float], *, threshold_mps: float = 0.2) -> float | None:
    cleaned = [abs(value) for value in values if math.isfinite(float(value))]
    if not cleaned:
        return None
    return sum(1 for value in cleaned if value < threshold_mps) / len(cleaned)


def _row_has_nonzero_control(
    row: Mapping[str, Any],
    *,
    throttle_field: str,
    brake_field: str,
    steer_field: str,
) -> bool | None:
    throttle = _num(row.get(throttle_field))
    brake = _num(row.get(brake_field))
    steer = _num(row.get(steer_field))
    if throttle is None and brake is None and steer is None:
        return None
    return (
        (throttle is not None and throttle > 0.05)
        or (brake is not None and brake > 0.05)
        or (steer is not None and abs(steer) > 0.001)
    )


def _row_time_s(row: Mapping[str, Any]) -> float | None:
    for field in ("sim_time", "time_s", "wall_time_s"):
        value = _num(row.get(field))
        if value is not None:
            return value
    return None


def _paired_abs_error_metrics(
    rows: Sequence[Mapping[str, Any]],
    left_field: str,
    right_field: str,
    *,
    max_lag_frames: int = 5,
) -> dict[str, Any]:
    start_index = _first_applied_control_index(rows)
    if start_index is None:
        return {
            "zero_lag_abs_error_p95": None,
            "best_lag_abs_error_p95": None,
            "best_lag_frames": None,
            "best_lag_sample_count": 0,
        }
    by_lag: list[tuple[int, float | None, int]] = []
    for lag in range(max_lag_frames + 1):
        values: list[float] = []
        for index in range(start_index + lag, len(rows)):
            left = _num(rows[index - lag].get(left_field))
            right = _num(rows[index].get(right_field))
            if left is not None and right is not None:
                values.append(abs(left - right))
        by_lag.append((lag, _percentile(values, 0.95), len(values)))

    zero_lag = by_lag[0][1] if by_lag else None
    candidates = [item for item in by_lag if item[1] is not None]
    best = min(candidates, key=lambda item: (float(item[1]), item[0])) if candidates else (None, None, 0)
    return {
        "zero_lag_abs_error_p95": zero_lag,
        "best_lag_abs_error_p95": best[1],
        "best_lag_frames": best[0],
        "best_lag_sample_count": best[2],
    }


def _first_applied_control_index(rows: Sequence[Mapping[str, Any]]) -> int | None:
    for index, row in enumerate(rows):
        if _row_has_nonzero_control(
            row,
            throttle_field="throttle_applied",
            brake_field="brake_applied",
            steer_field="carla_steer_applied",
        ):
            return index
    return None


def _bool_count(rows: Sequence[Mapping[str, Any]], *fields: str) -> int:
    count = 0
    for row in rows:
        if any(_parse_bool(row.get(field)) is True for field in fields):
            count += 1
    return count


def _series(rows: Sequence[Mapping[str, Any]], field: str) -> list[float]:
    return [value for row in rows if (value := _num(row.get(field))) is not None]


def _percentile(values: Sequence[float], q: float) -> float | None:
    cleaned = sorted(value for value in values if math.isfinite(float(value)))
    if not cleaned:
        return None
    if len(cleaned) == 1:
        return cleaned[0]
    rank = (len(cleaned) - 1) * q
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return cleaned[lower]
    weight = rank - lower
    return cleaned[lower] * (1.0 - weight) + cleaned[upper] * weight


def _first_number(*values: Any) -> float | None:
    for value in values:
        number = _num(value)
        if number is not None:
            return number
    return None


def _num(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _parse_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def _summary_bool(summary: Mapping[str, Any], name: str) -> bool | None:
    value = _parse_bool(summary.get(name))
    if value is not None:
        return value
    metrics = summary.get("metrics")
    if isinstance(metrics, Mapping):
        return _parse_bool(metrics.get(name))
    return None


def _summary_metrics(summary: Mapping[str, Any]) -> Mapping[str, Any]:
    metrics = summary.get("metrics")
    return metrics if isinstance(metrics, Mapping) else summary


def _runtime_contract_status(summary: Mapping[str, Any], manifest: Mapping[str, Any]) -> str | None:
    for source in (summary, manifest):
        runtime_contract = source.get("runtime_contract")
        if isinstance(runtime_contract, Mapping) and runtime_contract.get("status") not in {None, ""}:
            return str(runtime_contract.get("status"))
        value = source.get("runtime_contract_status")
        if value not in {None, ""}:
            return str(value)
    return None


def _control_handoff_status(summary: Mapping[str, Any]) -> str | None:
    value = summary.get("control_handoff_status")
    if value not in {None, ""}:
        return str(value)
    handoff = summary.get("control_handoff")
    if isinstance(handoff, Mapping) and handoff.get("control_handoff_status") not in {None, ""}:
        return str(handoff.get("control_handoff_status"))
    return None


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_rows(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    if path.suffix == ".jsonl":
        rows: list[dict[str, Any]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
        return rows
    with path.open(encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _find_first(root: Path, relative_paths: Sequence[str]) -> Path | None:
    for relative in relative_paths:
        path = root / relative
        if path.exists():
            return path
    basenames = {Path(relative).name for relative in relative_paths}
    for candidate in sorted(root.rglob("*")):
        if candidate.is_file() and candidate.name in basenames:
            return candidate
    return None


def _first_text(*args: Any, default: str | None = None) -> str | None:
    pairs = list(zip(args[0::2], args[1::2]))
    for mapping, key in pairs:
        if isinstance(mapping, Mapping):
            value = mapping.get(str(key))
            if value not in {None, ""}:
                return str(value)
    return default


def _text_or_none(value: Any) -> str | None:
    if value in {None, ""}:
        return None
    return str(value)


def _markdown(report: Mapping[str, Any]) -> str:
    metrics = report.get("metrics") if isinstance(report.get("metrics"), Mapping) else {}
    lines = [
        "# Control Health Summary",
        "",
        f"- status: `{report.get('status')}`",
        f"- failure_reason: `{report.get('failure_reason')}`",
        f"- control_handoff_status: `{report.get('control_handoff_status')}`",
        f"- control_latency_p95_ms: `{metrics.get('control_latency_p95_ms')}`",
        f"- brake_throttle_conflict_frames: `{metrics.get('brake_throttle_conflict_frames')}`",
        f"- nonzero_mapped_control_frames: `{metrics.get('nonzero_mapped_control_frames')}`",
        f"- nonzero_applied_control_frames: `{metrics.get('nonzero_applied_control_frames')}`",
        f"- control_apply_observation_delay_s: `{metrics.get('control_apply_observation_delay_s')}`",
        f"- control_apply_observation_delay_source: `{metrics.get('control_apply_observation_delay_source')}`",
        f"- control_apply_observation_delay_timeseries_s: `{metrics.get('control_apply_observation_delay_timeseries_s')}`",
        f"- route_s_delta_m: `{metrics.get('route_s_delta_m')}`",
        (
            "- route_s_after_first_applied_control_delta_m: "
            f"`{metrics.get('route_s_after_first_applied_control_delta_m')}`"
        ),
        f"- link_delay_decomposition: `{metrics.get('link_delay_decomposition')}`",
        f"- stopped_ratio_after_first_applied_control: `{metrics.get('stopped_ratio_after_first_applied_control')}`",
        f"- mapped_applied_steer_abs_error_p95: `{metrics.get('mapped_applied_steer_abs_error_p95')}`",
        f"- mapped_applied_throttle_abs_error_p95: `{metrics.get('mapped_applied_throttle_abs_error_p95')}`",
        (
            "- mapped_applied_throttle_abs_error_p95_zero_lag: "
            f"`{metrics.get('mapped_applied_throttle_abs_error_p95_zero_lag')}`"
        ),
        f"- mapped_applied_throttle_best_lag_frames: `{metrics.get('mapped_applied_throttle_best_lag_frames')}`",
        f"- mapped_applied_brake_abs_error_p95: `{metrics.get('mapped_applied_brake_abs_error_p95')}`",
        f"- oscillation_decomposition: `{_markdown_oscillation_decomposition(metrics)}`",
        f"- control_mapping_claim_boundary: `{metrics.get('control_mapping_claim_boundary')}`",
        f"- control_bridge_log: `{_markdown_control_bridge_log(metrics)}`",
        f"- direct_control_apply_log: `{_markdown_direct_control_apply_log(metrics)}`",
        f"- warnings: `{report.get('warnings')}`",
        "",
        "This report is artifact-derived and does not prove full Apollo perception.",
        "",
    ]
    return "\n".join(lines)


def _markdown_control_bridge_log(metrics: Mapping[str, Any]) -> dict[str, Any]:
    log_metrics = metrics.get("control_bridge_log")
    if not isinstance(log_metrics, Mapping):
        return {"available": False}
    return {
        "available": log_metrics.get("available"),
        "configured_apply_hz": log_metrics.get("configured_apply_hz"),
        "sync_to_world_tick": log_metrics.get("sync_to_world_tick"),
        "apply_world_frame_hz": log_metrics.get("apply_world_frame_hz"),
        "apply_frame_coverage_ratio": log_metrics.get("apply_frame_coverage_ratio"),
        "same_frame_drop_ratio": log_metrics.get("same_frame_drop_ratio"),
        "bind_to_first_apply_s": log_metrics.get("bind_to_first_apply_s"),
        "first_watchdog_apply_wall_s": log_metrics.get("first_watchdog_apply_wall_s"),
        "final_rx_count": log_metrics.get("final_rx_count"),
        "final_applied_count": log_metrics.get("final_applied_count"),
    }


def _markdown_oscillation_decomposition(metrics: Mapping[str, Any]) -> dict[str, Any]:
    decomposition = metrics.get("oscillation_decomposition")
    if not isinstance(decomposition, Mapping):
        return {"available": False}
    layers = decomposition.get("layers") if isinstance(decomposition.get("layers"), Mapping) else {}
    return {
        "dominant_oscillation_layer": decomposition.get("dominant_oscillation_layer"),
        "apollo_raw_command": (layers.get("apollo_raw_command") or {}).get("status")
        if isinstance(layers.get("apollo_raw_command"), Mapping)
        else None,
        "bridge_mapped_command": (layers.get("bridge_mapped_command") or {}).get("status")
        if isinstance(layers.get("bridge_mapped_command"), Mapping)
        else None,
        "carla_applied_command": (layers.get("carla_applied_command") or {}).get("status")
        if isinstance(layers.get("carla_applied_command"), Mapping)
        else None,
        "vehicle_response": (layers.get("vehicle_response") or {}).get("status")
        if isinstance(layers.get("vehicle_response"), Mapping)
        else None,
        "bridge_apply_cadence": (layers.get("bridge_apply_cadence") or {}).get("status")
        if isinstance(layers.get("bridge_apply_cadence"), Mapping)
        else None,
    }


def _markdown_direct_control_apply_log(metrics: Mapping[str, Any]) -> dict[str, Any]:
    apply_metrics = metrics.get("direct_control_apply_log")
    if not isinstance(apply_metrics, Mapping):
        return {"available": False}
    return {
        "available": apply_metrics.get("available"),
        "apply_count": apply_metrics.get("apply_count"),
        "first_apply_ts_sec": apply_metrics.get("first_apply_ts_sec"),
        "first_apply_frame_id": apply_metrics.get("first_apply_frame_id"),
        "actor_id": apply_metrics.get("actor_id"),
    }
