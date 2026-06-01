from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from statistics import mean
from typing import Any, Mapping, Sequence

REPORT_SCHEMA_VERSION = "apollo_town01_shadow_mode_report.v1"
DEFAULT_THRESHOLDS = {
    "max_trajectory_heading_error_p95_rad": 0.35,
    "max_matched_point_distance_m": 5.0,
    "max_target_point_jump_m": 3.0,
    "high_steer_abs_threshold": 0.85,
}
REQUIRED_TIMESERIES_FIELDS = {
    "route_s",
    "route_heading",
    "route_curvature",
    "ego_heading",
    "cross_track_error",
    "heading_error",
    "apollo_trajectory_heading",
    "apollo_steer_raw",
    "matched_point_distance",
    "target_point_distance",
}


def analyze_apollo_shadow_mode_timeseries(
    timeseries_csv: str | Path,
    *,
    thresholds: Mapping[str, float] | None = None,
    summary_json: str | Path | None = None,
) -> dict[str, Any]:
    rows = _read_csv_rows(timeseries_csv)
    active_thresholds = dict(DEFAULT_THRESHOLDS)
    active_thresholds.update(thresholds or {})
    missing_fields = _missing_fields(rows)
    summary_payload = _read_summary_json(summary_json)
    summary_semantics = _summary_semantics(summary_payload)

    if not rows:
        report = _empty_report(
            timeseries_csv,
            status="insufficient_data",
            failure_reason="missing_timeseries",
            missing_fields=["timeseries"],
            thresholds=active_thresholds,
            summary_json=summary_json,
            summary_semantics=summary_semantics,
        )
        return report

    route_id = _first_present(rows, "route_id")
    run_id = _first_present(rows, "run_id")
    trajectory_errors = _trajectory_heading_errors(rows)
    trajectory_heading_error_p95 = _percentile(trajectory_errors, 0.95)
    planning_available = _field_has_numeric_value(rows, "apollo_trajectory_heading")
    control_available = _field_has_numeric_value(rows, "apollo_steer_raw")
    matched_distances = _numeric_values(rows, "matched_point_distance", absolute=True)
    target_distances = _numeric_values(rows, "target_point_distance", absolute=True)
    matched_point_anomaly_count = sum(
        1 for value in matched_distances if value > active_thresholds["max_matched_point_distance_m"]
    )
    target_point_jump_count = _target_point_jump_count(
        rows,
        active_thresholds["max_target_point_jump_m"],
    )
    first_high_steer = _first_high_steer(
        rows,
        active_thresholds["high_steer_abs_threshold"],
    )
    status, failure_reason = _verdict(
        missing_fields=missing_fields,
        planning_available=planning_available,
        control_available=control_available,
        trajectory_heading_error_p95=trajectory_heading_error_p95,
        matched_point_anomaly_count=matched_point_anomaly_count,
        target_point_jump_count=target_point_jump_count,
        first_high_steer_s=first_high_steer["sim_time"],
        thresholds=active_thresholds,
    )
    required_next_fields = _required_next_fields(missing_fields)
    if status == "insufficient_data" and summary_semantics:
        if required_next_fields:
            failure_reason = "per_frame_p1_missing_summary_semantics_available"

    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "status": status,
        "failure_reason": failure_reason,
        "run_id": run_id,
        "route_id": route_id,
        "source": {
            "timeseries_csv": str(Path(timeseries_csv)),
            "summary_json": None if summary_json is None else str(Path(summary_json)),
        },
        "evidence_scope": "per_frame_timeseries_with_optional_summary_semantics",
        "planning_available": planning_available,
        "control_available": control_available,
        "per_frame_p1_complete": not required_next_fields,
        "summary_semantics_available": bool(summary_semantics),
        "summary_semantics": summary_semantics,
        "required_next_fields": required_next_fields,
        "trajectory_heading_error_p95": trajectory_heading_error_p95,
        "matched_point_anomaly_count": matched_point_anomaly_count if "matched_point_distance" not in missing_fields else None,
        "target_point_jump_count": target_point_jump_count if "target_point_distance" not in missing_fields else None,
        "first_high_steer_s": first_high_steer["sim_time"],
        "first_high_steer_route_s": first_high_steer["route_s"],
        "metrics": {
            "row_count": len(rows),
            "trajectory_heading_error_mean": mean(trajectory_errors) if trajectory_errors else None,
            "trajectory_heading_error_p95": trajectory_heading_error_p95,
            "cross_track_error_abs_p95": _percentile(
                _numeric_values(rows, "cross_track_error", absolute=True), 0.95
            ),
            "heading_error_abs_p95": _percentile(
                _numeric_values(rows, "heading_error", absolute=True), 0.95
            ),
            "matched_point_distance_abs_p95": _percentile(matched_distances, 0.95),
            "target_point_distance_abs_p95": _percentile(target_distances, 0.95),
        },
        "thresholds": active_thresholds,
        "missing_fields": missing_fields,
        "warnings": _warnings(status, failure_reason),
        "interpretation_boundary": (
            "Shadow mode checks Apollo planning/control output semantics without applying "
            "control to CARLA; it must not be interpreted as closed-loop success."
        ),
    }


def write_apollo_shadow_mode_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "apollo_shadow_mode_report.json"
    summary_path = output_dir / "summary.md"
    report_path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path.write_text(_summary_markdown(report), encoding="utf-8")
    return {
        "apollo_shadow_mode_report": str(report_path),
        "summary": str(summary_path),
    }


def _empty_report(
    timeseries_csv: str | Path,
    *,
    status: str,
    failure_reason: str,
    missing_fields: list[str],
    thresholds: Mapping[str, float],
    summary_json: str | Path | None = None,
    summary_semantics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    summary_semantics = dict(summary_semantics or {})
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "status": status,
        "failure_reason": failure_reason,
        "run_id": None,
        "route_id": None,
        "source": {
            "timeseries_csv": str(Path(timeseries_csv)),
            "summary_json": None if summary_json is None else str(Path(summary_json)),
        },
        "evidence_scope": "per_frame_timeseries_with_optional_summary_semantics",
        "planning_available": False,
        "control_available": False,
        "per_frame_p1_complete": False,
        "summary_semantics_available": bool(summary_semantics),
        "summary_semantics": summary_semantics,
        "required_next_fields": sorted(REQUIRED_TIMESERIES_FIELDS),
        "trajectory_heading_error_p95": None,
        "matched_point_anomaly_count": None,
        "target_point_jump_count": None,
        "first_high_steer_s": None,
        "first_high_steer_route_s": None,
        "metrics": {},
        "thresholds": dict(thresholds),
        "missing_fields": missing_fields,
        "warnings": ["timeseries unavailable"],
        "interpretation_boundary": (
            "Shadow mode checks Apollo planning/control output semantics without applying "
            "control to CARLA; it must not be interpreted as closed-loop success."
        ),
    }


def _read_csv_rows(path: str | Path) -> list[dict[str, Any]]:
    csv_path = Path(path).expanduser()
    with csv_path.open(encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _read_summary_json(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    summary_path = Path(path).expanduser()
    if not summary_path.exists():
        return {}
    try:
        payload = json.loads(summary_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _first_mapping_value(payload: Mapping[str, Any], key: str) -> Any:
    if key in payload:
        return payload.get(key)
    for value in payload.values():
        if isinstance(value, Mapping) and key in value:
            return value.get(key)
    return None


def _summary_semantics(summary: Mapping[str, Any]) -> dict[str, Any]:
    if not summary:
        return {}
    keys = (
        "semantic_window_anchor_kind",
        "semantic_window_anchor_seq",
        "semantic_window_anchor_at",
        "first_high_steer_seq",
        "first_high_steer_at",
        "first_matched_point_too_large_seq",
        "first_matched_point_too_large_at",
        "high_steer_before_first_matched_point_too_large",
        "first_path_fallback_trigger_reason_family",
        "first_path_fallback_trigger_lon_diff",
        "first_relapse_after_recovery_reason_family",
        "first_relapse_after_recovery_lon_diff",
        "apollo_simple_lat_lateral_error_abs_p95",
        "apollo_simple_lat_heading_error_abs_p95",
        "apollo_simple_lat_target_point_kappa_abs_p95",
        "simple_lat_lateral_error_abs_p95_before_anchor",
        "simple_lat_heading_error_abs_p95_before_anchor",
        "target_point_kappa_abs_p95_before_anchor",
    )
    result = {
        key: _first_mapping_value(summary, key)
        for key in keys
        if _first_mapping_value(summary, key) not in (None, "")
    }
    if result:
        result["evidence_level"] = "summary_derived_not_per_frame_p1"
        result["claim_not_supported"] = (
            "summary-derived semantics do not replace per-frame matched/target/"
            "trajectory fields for closed-loop curve attribution"
        )
    return result


def _required_next_fields(missing_fields: Sequence[str]) -> list[str]:
    p1_fields = {
        "apollo_trajectory_heading",
        "matched_point_distance",
        "target_point_distance",
    }
    return sorted(field for field in missing_fields if field in p1_fields)


def _missing_fields(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    missing: list[str] = []
    if not rows:
        return sorted(REQUIRED_TIMESERIES_FIELDS)
    for field in sorted(REQUIRED_TIMESERIES_FIELDS):
        if not _field_has_value(rows, field):
            missing.append(field)
    return missing


def _field_has_value(rows: Sequence[Mapping[str, Any]], field: str) -> bool:
    return any(row.get(field) not in {None, ""} for row in rows)


def _field_has_numeric_value(rows: Sequence[Mapping[str, Any]], field: str) -> bool:
    return bool(_numeric_values(rows, field))


def _first_present(rows: Sequence[Mapping[str, Any]], field: str) -> str | None:
    for row in rows:
        value = row.get(field)
        if value not in {None, ""}:
            return str(value)
    return None


def _float(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _numeric_values(
    rows: Sequence[Mapping[str, Any]],
    field: str,
    *,
    absolute: bool = False,
) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = _float(row.get(field))
        if value is None:
            continue
        values.append(abs(value) if absolute else value)
    return values


def _normalize_angle(angle_rad: float) -> float:
    while angle_rad > math.pi:
        angle_rad -= 2.0 * math.pi
    while angle_rad < -math.pi:
        angle_rad += 2.0 * math.pi
    return angle_rad


def _trajectory_heading_errors(rows: Sequence[Mapping[str, Any]]) -> list[float]:
    errors: list[float] = []
    for row in rows:
        route_heading = _float(row.get("route_heading"))
        trajectory_heading = _float(row.get("apollo_trajectory_heading"))
        if route_heading is None or trajectory_heading is None:
            continue
        errors.append(abs(_normalize_angle(trajectory_heading - route_heading)))
    return errors


def _target_point_jump_count(rows: Sequence[Mapping[str, Any]], threshold: float) -> int:
    previous: float | None = None
    count = 0
    for row in rows:
        current = _float(row.get("target_point_distance"))
        if current is None:
            continue
        if previous is not None and abs(current - previous) > threshold:
            count += 1
        previous = current
    return count


def _first_high_steer(rows: Sequence[Mapping[str, Any]], threshold: float) -> dict[str, float | None]:
    for row in rows:
        steer = _float(row.get("apollo_steer_raw"))
        if steer is not None and abs(steer) >= threshold:
            return {
                "sim_time": _float(row.get("sim_time")),
                "route_s": _float(row.get("route_s")),
            }
    return {"sim_time": None, "route_s": None}


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


def _verdict(
    *,
    missing_fields: Sequence[str],
    planning_available: bool,
    control_available: bool,
    trajectory_heading_error_p95: float | None,
    matched_point_anomaly_count: int,
    target_point_jump_count: int,
    first_high_steer_s: float | None,
    thresholds: Mapping[str, float],
) -> tuple[str, str | None]:
    matched_target_missing = {
        field for field in missing_fields if field in {"matched_point_distance", "target_point_distance"}
    }
    if not planning_available:
        return "insufficient_data", "planning_output_missing"
    if not control_available:
        return "insufficient_data", "control_output_missing"
    if matched_target_missing:
        return "insufficient_data", "missing_matched_or_target_point_fields"
    if "apollo_trajectory_heading" in missing_fields:
        return "insufficient_data", "missing_trajectory_heading"
    if trajectory_heading_error_p95 is not None and trajectory_heading_error_p95 > thresholds[
        "max_trajectory_heading_error_p95_rad"
    ]:
        return "fail", "trajectory_heading_divergence"
    if matched_point_anomaly_count > 0:
        return "fail", "matched_point_anomaly"
    if target_point_jump_count > 0:
        return "fail", "target_point_jump"
    if first_high_steer_s is not None:
        return "warn", "high_steer_observed"
    if missing_fields:
        return "insufficient_data", "missing_required_timeseries_fields"
    return "pass", None


def _warnings(status: str, failure_reason: str | None) -> list[str]:
    warnings: list[str] = []
    if status == "insufficient_data":
        warnings.append("shadow_mode_inputs_incomplete")
    if failure_reason == "per_frame_p1_missing_summary_semantics_available":
        warnings.append("summary_semantics_available_but_per_frame_p1_missing")
    if failure_reason == "high_steer_observed":
        warnings.append("high_steer_observed_without_closed_loop_actuation")
    return warnings


def _summary_markdown(report: Mapping[str, Any]) -> str:
    metrics = report.get("metrics") if isinstance(report.get("metrics"), Mapping) else {}
    semantics = (
        report.get("summary_semantics")
        if isinstance(report.get("summary_semantics"), Mapping)
        else {}
    )
    return "\n".join(
        [
            "# Apollo Town01 Shadow Mode Summary",
            "",
            f"- status: `{report.get('status')}`",
            f"- failure_reason: `{report.get('failure_reason')}`",
            f"- route_id: `{report.get('route_id')}`",
            f"- per_frame_p1_complete: `{report.get('per_frame_p1_complete')}`",
            f"- summary_semantics_available: `{report.get('summary_semantics_available')}`",
            f"- required_next_fields: `{', '.join(report.get('required_next_fields') or [])}`",
            f"- summary_anchor: `{semantics.get('semantic_window_anchor_kind')}`",
            f"- summary_first_matched_point_too_large_seq: `{semantics.get('first_matched_point_too_large_seq')}`",
            f"- planning_available: `{report.get('planning_available')}`",
            f"- control_available: `{report.get('control_available')}`",
            f"- trajectory_heading_error_p95: `{report.get('trajectory_heading_error_p95')}`",
            f"- matched_point_anomaly_count: `{report.get('matched_point_anomaly_count')}`",
            f"- target_point_jump_count: `{report.get('target_point_jump_count')}`",
            f"- first_high_steer_s: `{report.get('first_high_steer_s')}`",
            f"- first_high_steer_route_s: `{report.get('first_high_steer_route_s')}`",
            f"- cross_track_error_abs_p95: `{metrics.get('cross_track_error_abs_p95')}`",
            "",
            "This report evaluates Apollo output semantics in shadow mode only; it does not judge closed-loop success.",
            "",
        ]
    )
