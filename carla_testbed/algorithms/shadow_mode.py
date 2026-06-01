from __future__ import annotations

import csv
import json
from pathlib import Path
from statistics import mean
from typing import Any, Iterable, Mapping

import yaml

SCHEMA_VERSION = "apollo_shadow_mode_report.v1"
CONFIG_SCHEMA_VERSION = "apollo_shadow_mode_check.v1"
DEFAULT_THRESHOLDS = {
    "min_planning_hz": 2.0,
    "min_control_hz": 2.0,
    "max_heading_error_p95_rad": 0.35,
    "max_lateral_error_p95_m": 1.5,
    "max_matched_point_distance_m": 5.0,
    "max_target_point_distance_m": 8.0,
    "high_steer_abs_threshold": 0.85,
    "max_steer_spike_count": 3,
    "max_brake_throttle_conflict_count": 0,
    "max_matched_point_too_large_count": 0,
    "max_target_point_jump_count": 0,
    "route_curvature_correlation_warn_min": 0.1,
}
REQUIRED_BASE_FIELDS = {"sim_time", "route_id"}
PLANNING_RATE_FIELDS = ("planning_available", "planning_nonempty", "planning_message_present")
CONTROL_RATE_FIELDS = ("control_available", "apollo_steer_raw", "throttle_raw", "brake_raw")


class ShadowModeError(ValueError):
    pass


def load_shadow_mode_config(config: str | Path | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(config, Mapping):
        payload = dict(config)
    else:
        path = Path(config).expanduser()
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ShadowModeError("shadow mode config must be a mapping")
    thresholds = dict(DEFAULT_THRESHOLDS)
    thresholds.update(payload.get("thresholds") or {})
    payload["thresholds"] = thresholds
    if payload.get("schema_version") not in {None, CONFIG_SCHEMA_VERSION}:
        raise ShadowModeError(f"schema_version must be {CONFIG_SCHEMA_VERSION}")
    return payload


def _read_csv_rows(path: str | Path) -> list[dict[str, Any]]:
    csv_path = Path(path).expanduser()
    with csv_path.open(encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _boolish(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def _numeric_values(rows: Iterable[Mapping[str, Any]], field: str, *, absolute: bool = False) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = _float(row.get(field))
        if value is None:
            continue
        values.append(abs(value) if absolute else value)
    return values


def _p95(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(0.95 * (len(ordered) - 1))))
    return ordered[index]


def _field_exists(rows: list[Mapping[str, Any]], field: str) -> bool:
    return any(field in row and row.get(field) not in {None, ""} for row in rows)


def _count_available(rows: list[Mapping[str, Any]], fields: tuple[str, ...]) -> int:
    count = 0
    for row in rows:
        available = False
        for field in fields:
            value = row.get(field)
            bool_value = _boolish(value)
            if bool_value is True:
                available = True
                break
            if bool_value is None and _float(value) is not None:
                available = True
                break
        if available:
            count += 1
    return count


def _duration_s(rows: list[Mapping[str, Any]]) -> float | None:
    times = _numeric_values(rows, "sim_time")
    if len(times) < 2:
        return None
    duration = max(times) - min(times)
    return duration if duration > 0 else None


def _rate_hz(rows: list[Mapping[str, Any]], fields: tuple[str, ...]) -> float:
    duration = _duration_s(rows)
    count = _count_available(rows, fields)
    if duration is None:
        return 0.0
    return count / duration


def _correlation(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) < 2 or len(xs) != len(ys):
        return None
    x_mean = mean(xs)
    y_mean = mean(ys)
    numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
    x_den = sum((x - x_mean) ** 2 for x in xs)
    y_den = sum((y - y_mean) ** 2 for y in ys)
    if x_den <= 0 or y_den <= 0:
        return None
    return numerator / ((x_den * y_den) ** 0.5)


def _route_curvature_correlation(rows: list[Mapping[str, Any]]) -> float | None:
    curvatures: list[float] = []
    steer_values: list[float] = []
    for row in rows:
        curvature = _float(row.get("route_curvature") or row.get("curvature_at_nearest"))
        steer = _float(row.get("apollo_steer_raw"))
        if curvature is None or steer is None:
            continue
        curvatures.append(curvature)
        steer_values.append(steer)
    return _correlation(curvatures, steer_values)


def _count_threshold(rows: list[Mapping[str, Any]], field: str, threshold: float) -> int:
    count = 0
    for row in rows:
        value = _float(row.get(field))
        if value is not None and abs(value) > threshold:
            count += 1
    return count


def _first_threshold_time(rows: list[Mapping[str, Any]], field: str, threshold: float) -> float | None:
    for row in rows:
        value = _float(row.get(field))
        if value is not None and abs(value) > threshold:
            return _float(row.get("sim_time"))
    return None


def _brake_throttle_conflicts(rows: list[Mapping[str, Any]]) -> int:
    count = 0
    for row in rows:
        throttle = _float(row.get("throttle_raw"))
        brake = _float(row.get("brake_raw"))
        if throttle is not None and brake is not None and throttle > 0.05 and brake > 0.05:
            count += 1
    return count


def _matched_point_anomaly_count(rows: list[Mapping[str, Any]], threshold: float) -> tuple[int, bool]:
    if not _field_exists(rows, "apollo_matched_point_distance"):
        return 0, False
    count = sum(
        1
        for value in _numeric_values(rows, "apollo_matched_point_distance", absolute=True)
        if value > threshold
    )
    return count, True


def _target_point_jump_count(rows: list[Mapping[str, Any]], threshold: float) -> tuple[int, bool]:
    if not _field_exists(rows, "apollo_target_point_distance"):
        return 0, False
    count = sum(
        1
        for value in _numeric_values(rows, "apollo_target_point_distance", absolute=True)
        if value > threshold
    )
    return count, True


def _report_identity(rows: list[Mapping[str, Any]]) -> tuple[str | None, str | None, str | None]:
    first = rows[0] if rows else {}
    return (
        first.get("run_id") or None,
        first.get("variant_id") or first.get("apollo_variant_id") or None,
        first.get("route_id") or None,
    )


def analyze_shadow_mode_timeseries(csv_path: str | Path, config: str | Path | Mapping[str, Any]) -> dict[str, Any]:
    cfg = load_shadow_mode_config(config)
    thresholds = cfg["thresholds"]
    rows = _read_csv_rows(csv_path)
    run_id, variant_id, route_id = _report_identity(rows)
    missing_fields: list[str] = []
    warnings: list[str] = []

    if not rows:
        return {
            "schema_version": SCHEMA_VERSION,
            "run_id": None,
            "variant_id": None,
            "route_id": None,
            "status": "insufficient_data",
            "planning_available": False,
            "control_available": False,
            "planning_rate_hz": 0.0,
            "control_rate_hz": 0.0,
            "trajectory_route_alignment": {},
            "control_reasonableness": {},
            "matched_target_anomalies": {},
            "missing_fields": ["timeseries"],
            "failure_reason": "missing_timeseries",
        }

    for field in sorted(REQUIRED_BASE_FIELDS):
        if not _field_exists(rows, field):
            missing_fields.append(field)
    for field in ("cross_track_error", "heading_error"):
        if not _field_exists(rows, field):
            missing_fields.append(field)
    if not any(_field_exists(rows, field) for field in PLANNING_RATE_FIELDS):
        missing_fields.append("planning_available")
    if not any(_field_exists(rows, field) for field in CONTROL_RATE_FIELDS):
        missing_fields.append("control_available")
    for field in ("apollo_matched_point_distance", "apollo_target_point_distance"):
        if not _field_exists(rows, field):
            missing_fields.append(field)

    planning_rate_hz = _rate_hz(rows, PLANNING_RATE_FIELDS)
    control_rate_hz = _rate_hz(rows, CONTROL_RATE_FIELDS)
    lateral_abs = _numeric_values(rows, "cross_track_error", absolute=True)
    heading_abs = _numeric_values(rows, "heading_error", absolute=True)
    lateral_p95 = _p95(lateral_abs)
    heading_p95 = _p95(heading_abs)
    route_curvature_correlation = _route_curvature_correlation(rows)
    steer_raw_available = _field_exists(rows, "apollo_steer_raw")
    throttle_raw_available = _field_exists(rows, "throttle_raw")
    brake_raw_available = _field_exists(rows, "brake_raw")
    steer_spike_count = _count_threshold(rows, "apollo_steer_raw", thresholds["high_steer_abs_threshold"])
    brake_throttle_conflict_count = _brake_throttle_conflicts(rows)
    matched_count, matched_available = _matched_point_anomaly_count(rows, thresholds["max_matched_point_distance_m"])
    target_count, target_available = _target_point_jump_count(rows, thresholds["max_target_point_distance_m"])
    first_high_steer_s = _first_threshold_time(rows, "apollo_steer_raw", thresholds["high_steer_abs_threshold"])

    planning_available = planning_rate_hz > 0
    control_available = control_rate_hz > 0
    status = "pass"
    failure_reason: str | None = None

    if "planning_available" in missing_fields:
        status = "insufficient_data"
        failure_reason = "planning_missing"
    elif "control_available" in missing_fields:
        status = "insufficient_data"
        failure_reason = "control_missing"
    elif not planning_available or planning_rate_hz < thresholds["min_planning_hz"]:
        status = "fail"
        failure_reason = "planning_missing"
    elif not control_available or control_rate_hz < thresholds["min_control_hz"]:
        status = "fail"
        failure_reason = "control_missing"
    elif heading_p95 is not None and heading_p95 > thresholds["max_heading_error_p95_rad"]:
        status = "fail"
        failure_reason = "heading_divergence"
    elif lateral_p95 is not None and lateral_p95 > thresholds["max_lateral_error_p95_m"]:
        status = "fail"
        failure_reason = "high_lateral_error"
    elif matched_available and matched_count > thresholds["max_matched_point_too_large_count"]:
        status = "fail"
        failure_reason = "matched_point_anomaly"
    elif target_available and target_count > thresholds["max_target_point_jump_count"]:
        status = "fail"
        failure_reason = "target_point_anomaly"
    elif not lateral_abs or not heading_abs:
        status = "insufficient_data"
        failure_reason = "missing_route_alignment_fields"
    elif not steer_raw_available or not throttle_raw_available or not brake_raw_available:
        status = "insufficient_data"
        failure_reason = "missing_control_raw_fields"
    elif missing_fields:
        status = "insufficient_data"
        failure_reason = "missing_apollo_semantic_fields"
    elif steer_spike_count > thresholds["max_steer_spike_count"]:
        status = "warn"
        failure_reason = "high_steer_spikes"
    elif brake_throttle_conflict_count > thresholds["max_brake_throttle_conflict_count"]:
        status = "warn"
        failure_reason = "brake_throttle_conflict"
    elif (
        route_curvature_correlation is not None
        and abs(route_curvature_correlation) < thresholds["route_curvature_correlation_warn_min"]
    ):
        status = "warn"
        failure_reason = "weak_route_curvature_control_correlation"

    if not matched_available:
        warnings.append("apollo_matched_point_distance unavailable")
    if not target_available:
        warnings.append("apollo_target_point_distance unavailable")

    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "variant_id": variant_id,
        "route_id": route_id,
        "status": status,
        "planning_available": planning_available,
        "control_available": control_available,
        "planning_rate_hz": planning_rate_hz,
        "control_rate_hz": control_rate_hz,
        "trajectory_route_alignment": {
            "mean_heading_error_rad": mean(heading_abs) if heading_abs else None,
            "p95_heading_error_rad": heading_p95,
            "mean_lateral_error_m": mean(lateral_abs) if lateral_abs else None,
            "p95_lateral_error_m": lateral_p95,
            "route_curvature_correlation": route_curvature_correlation,
        },
        "control_reasonableness": {
            "steer_raw_available": steer_raw_available,
            "throttle_raw_available": throttle_raw_available,
            "brake_raw_available": brake_raw_available,
            "steer_spike_count": steer_spike_count,
            "brake_throttle_conflict_count": brake_throttle_conflict_count,
        },
        "matched_target_anomalies": {
            "matched_point_too_large_count": matched_count if matched_available else None,
            "target_point_jump_count": target_count if target_available else None,
            "first_high_steer_s": first_high_steer_s,
        },
        "missing_fields": sorted(set(missing_fields)),
        "warnings": warnings,
        "failure_reason": failure_reason,
    }


def write_shadow_mode_report(report: Mapping[str, Any], out_dir: str | Path) -> dict[str, str]:
    output_dir = Path(out_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "shadow_mode_report.json"
    path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"shadow_mode_report": str(path)}
