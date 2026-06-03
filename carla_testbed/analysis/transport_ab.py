from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from statistics import mean
from typing import Any, Mapping, Sequence

from carla_testbed.analysis.assist_ledger import NON_BLOCKING_ASSISTS, build_assist_ledger, read_assist_ledger_from_run_dir
from carla_testbed.experiments.ab_manifest import ABManifest, ABRunRecord, load_ab_manifest

from .failure_reason import classify_failure
from .route_curve_artifact_gap import analyze_route_curve_artifact_gap
from .route_health_report import load_timeseries_rows

AB_REPORT_SCHEMA_VERSION = "ab_report.v1"
HARD_GATE_ROUTE_IDS = ("lane097", "lane217", "junction031")
CURVE_DIAGNOSTIC_ROUTE_IDS = ("curve217", "curve213")

AB_REPORT_CSV_FIELDS = [
    "run_id",
    "route_id",
    "backend",
    "run_status",
    "return_code",
    "transport_mode",
    "bridge_mode",
    "backend_config_path",
    "active_assists",
    "blocking_assists",
    "can_claim_unassisted",
    "duration_s",
    "artifact_dir",
    "actual_run_dir",
    "summary_path",
    "route_health_path",
    "route_source",
    "route_evidence_level",
    "route_hard_gate_eligible",
    "route_curve_artifact_gap_path",
    "route_curve_artifact_gap_source",
    "route_curve_artifact_gap_status",
    "route_curve_artifact_gap_failure_reason",
    "route_curve_per_frame_p1_complete",
    "route_curve_missing_p1_fields",
    "route_curve_summary_semantics_available",
    "timeseries_path",
    "events_path",
    "command_stdout_path",
    "command_stderr_path",
    "apollo_channel_health_path",
    "apollo_channel_health_status",
    "apollo_channel_health_missing_required_channels",
    "apollo_channel_health_low_rate_channels",
    "control_health_path",
    "control_health_status",
    "control_health_failure_reason",
    "control_health_warnings",
    "apollo_control_handoff_path",
    "apollo_control_handoff_status",
    "apollo_control_handoff_failure_stage",
    "apollo_control_handoff_blocking_reasons",
    "control_apply_observation_delay_s",
    "route_s_after_first_applied_control_delta_m",
    "stopped_ratio_after_first_applied_control",
    "control_bridge_apply_world_frame_hz",
    "control_bridge_same_frame_drop_ratio",
    "control_bridge_bind_to_first_apply_s",
    "control_bridge_first_watchdog_apply_wall_s",
    "control_bridge_final_rx_count",
    "control_bridge_final_applied_count",
    "control_bridge_final_drop_same_frame_count",
    "artifact_complete",
    "bridge_loc_count",
    "bridge_chassis_count",
    "bridge_obstacles_count",
    "bridge_control_rx_count",
    "bridge_control_tx_count",
    "bridge_routing_request_count",
    "bridge_routing_success_count",
    "bridge_loc_hz",
    "bridge_chassis_hz",
    "bridge_cadence_duration_s",
    "bridge_cadence_duration_source",
    "bridge_stats_sim_time_sec",
    "bridge_loc_hz_by_stats_sim_time",
    "bridge_chassis_hz_by_stats_sim_time",
    "direct_snapshot_hz_by_stats_sim_time",
    "direct_control_apply_mode",
    "direct_stale_world_frame_policy",
    "direct_stale_world_frame_skip_count",
    "direct_stale_world_frame_republish_count",
    "direct_snapshot_count",
    "direct_stale_world_frame_policy_source",
    "direct_transport_contract_status",
    "direct_transport_contract_reasons",
    "steering_percent_normalization_planned",
    "route_completion",
    "lateral_error_mean_m",
    "lateral_error_p95_m",
    "lateral_error_max_m",
    "heading_error_mean_rad",
    "heading_error_p95_rad",
    "heading_error_max_rad",
    "curve_segment_lateral_error_p95_m",
    "curve_segment_lateral_error_max_m",
    "recovery_after_curve_s",
    "steering_command_smoothness",
    "applied_steering_smoothness",
    "apollo_control_hz",
    "carla_applied_control_hz",
    "planning_hz",
    "localization_hz",
    "chassis_hz",
    "control_latency_p95_ms",
    "brake_throttle_conflict_frames",
    "stop_ratio",
    "stuck_duration_s",
    "off_route_duration_s",
    "collision_count",
    "lane_invasion_count",
    "first_safety_event_type",
    "first_safety_event_time_s",
    "first_safety_event_frame_id",
    "first_safety_event_route_s",
    "first_safety_event_cross_track_error_m",
    "first_safety_event_heading_error_rad",
    "first_safety_event_ego_speed_mps",
    "first_safety_event_apollo_steer_raw",
    "first_safety_event_bridge_steer_mapped",
    "first_safety_event_carla_steer_applied",
    "first_safety_event_throttle_raw",
    "first_safety_event_throttle_mapped",
    "first_safety_event_throttle_applied",
    "first_safety_event_brake_raw",
    "first_safety_event_brake_mapped",
    "first_safety_event_brake_applied",
    "first_safety_event_lateral_guard_applied",
    "first_safety_event_trajectory_contract_guard_applied",
    "first_safety_event_control_context",
    "first_safety_event_source",
    "safety_window_duration_s",
    "safety_window_sample_count",
    "safety_window_cross_track_error_start_m",
    "safety_window_cross_track_error_end_m",
    "safety_window_cross_track_error_delta_m",
    "safety_window_cross_track_error_max_m",
    "safety_window_apollo_steer_raw_abs_p95",
    "safety_window_bridge_steer_mapped_abs_p95",
    "safety_window_carla_steer_applied_abs_p95",
    "safety_window_zero_lateral_command_ratio",
    "safety_window_control_context",
    "steering_normalization_modes",
    "steering_normalization_mode_counts",
    "steering_normalization_trace_path",
    "failure_reason",
    "route_health_error",
    "missing_fields",
]

BAD_POSITIVE_FAILURE_REASONS = {
    "no_control",
    "planning_missing",
    "control_missing",
    "bridge_drop",
    "timeout",
    "artifact_missing",
}

BAD_EXECUTION_STATUSES = {
    "failed",
    "timeout",
    "skipped",
    "cancelled",
    "canceled",
}
ROUTE_COMPLETION_REGRESSION_TOLERANCE = 0.02
LATERAL_ERROR_P95_RELATIVE_TOLERANCE = 1.2
LATERAL_ERROR_P95_ABS_TOLERANCE_M = 0.05
HEADING_ERROR_P95_RELATIVE_TOLERANCE = 1.2
HEADING_ERROR_P95_ABS_TOLERANCE_RAD = 0.02


def _comparison_tolerances() -> dict[str, float]:
    return {
        "route_completion_abs": ROUTE_COMPLETION_REGRESSION_TOLERANCE,
        "lateral_error_p95_relative": LATERAL_ERROR_P95_RELATIVE_TOLERANCE,
        "lateral_error_p95_abs_m": LATERAL_ERROR_P95_ABS_TOLERANCE_M,
        "heading_error_p95_relative": HEADING_ERROR_P95_RELATIVE_TOLERANCE,
        "heading_error_p95_abs_rad": HEADING_ERROR_P95_ABS_TOLERANCE_RAD,
    }


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _resolve(path_text: str | None, *, manifest_dir: Path, run_dir: Path | None = None) -> Path | None:
    if not path_text:
        return None
    path = Path(path_text).expanduser()
    if path.is_absolute():
        return path
    if path.exists():
        return path
    if run_dir is not None and (run_dir / path).exists():
        return run_dir / path
    manifest_path = manifest_dir / path
    if manifest_path.exists():
        return manifest_path
    return path


def _first_recursive(root: Path | None, name: str) -> Path | None:
    if root is None or not root.exists():
        return None
    direct = root / name
    if direct.exists():
        return direct
    candidates = [path for path in root.rglob(name) if path.is_file()]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _num(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _int_count(value: Any) -> int | None:
    num = _num(value)
    if num is None:
        return None
    return int(num)


def _count_rate(count: Any, duration_s: float | None) -> float | None:
    num = _num(count)
    if num is None or duration_s is None or duration_s <= 0:
        return None
    return float(num) / float(duration_s)


def _ratio(numerator: Any, denominator: Any) -> float | None:
    num = _num(numerator)
    den = _num(denominator)
    if num is None or den is None or den <= 0:
        return None
    return num / den


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _nullable_boolish(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    return _boolish(value)


def _percentile(values: Sequence[float], q: float) -> float | None:
    cleaned = sorted(float(value) for value in values if value is not None and math.isfinite(float(value)))
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


def _series(rows: Sequence[Mapping[str, Any]], *names: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        for name in names:
            value = _num(row.get(name))
            if value is not None:
                values.append(value)
                break
    return values


def _abs_series(rows: Sequence[Mapping[str, Any]], *names: str) -> list[float]:
    return [abs(value) for value in _series(rows, *names)]


def _metric_summary(values: Sequence[float]) -> tuple[float | None, float | None, float | None]:
    if not values:
        return None, None, None
    return mean(values), _percentile(values, 0.95), max(values)


def _smoothness(values: Sequence[float]) -> float | None:
    if len(values) < 2:
        return None
    return mean(abs(float(values[index]) - float(values[index - 1])) for index in range(1, len(values)))


def _duration_from_rows(rows: Sequence[Mapping[str, Any]], fallback: float | None) -> float | None:
    times = _series(rows, "sim_time", "t", "timestamp")
    if len(times) >= 2:
        span = max(times) - min(times)
        if span > 0:
            return span
    return fallback


def _rate(rows: Sequence[Mapping[str, Any]], duration_s: float | None, *names: str, bool_only: bool = False) -> float | None:
    if not rows or not duration_s or duration_s <= 0:
        return None
    if not any(name in row for row in rows for name in names):
        return None
    count = 0
    for row in rows:
        if bool_only:
            if any(_boolish(row.get(name)) for name in names):
                count += 1
        elif any(row.get(name) not in (None, "") for name in names):
            count += 1
    return count / duration_s


def _row_rate(rows: Sequence[Mapping[str, Any]], duration_s: float | None) -> float | None:
    if not rows or not duration_s or duration_s <= 0:
        return None
    return len(rows) / duration_s


def _summary_count_rate(summary: Mapping[str, Any], duration_s: float | None, *names: str) -> float | None:
    if not duration_s or duration_s <= 0:
        return None
    for name in names:
        count = _num(summary.get(name))
        if count is not None and count > 0:
            return count / duration_s
    return None


def _summary_planning_rate(
    summary: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    duration_s: float | None,
) -> float | None:
    if not duration_s or duration_s <= 0:
        return None
    for name in ("planning_message_count", "planning_nonempty_count", "planning_row_count"):
        count = _num(summary.get(name))
        if count is not None and count > 0:
            return count / duration_s
    ratio = _num(summary.get("planning_nonzero_ratio"))
    if ratio is not None and ratio > 0:
        row_rate = _row_rate(rows, duration_s)
        return row_rate * ratio if row_rate is not None else ratio
    if summary.get("planning_materialized") is True or summary.get("planning_first_nonempty_at") is not None:
        row_rate = _row_rate(rows, duration_s)
        return row_rate if row_rate is not None else 1.0 / duration_s
    return None


def _infer_direct_stale_world_frame_policy(cyber_bridge_stats: Mapping[str, Any]) -> tuple[str | None, str]:
    republish_count = _int_count(cyber_bridge_stats.get("direct_stale_world_frame_republish_count"))
    skip_count = _int_count(cyber_bridge_stats.get("direct_stale_world_frame_skip_count"))
    if republish_count is None and skip_count is None:
        return None, ""
    republish_count = republish_count or 0
    skip_count = skip_count or 0
    if republish_count > 0 and skip_count > 0:
        return "mixed_observed", "cyber_bridge_stats_counts"
    if republish_count > 0:
        return "always_republish", "cyber_bridge_stats_counts"
    if skip_count > 0:
        return "until_control", "cyber_bridge_stats_counts"
    return None, ""


def _curve_lateral_stats(rows: Sequence[Mapping[str, Any]]) -> tuple[float | None, float | None]:
    values: list[float] = []
    for row in rows:
        if not _boolish(row.get("is_curve_segment")):
            continue
        raw_value = row.get("cross_track_error")
        if raw_value in (None, ""):
            raw_value = row.get("lateral_error")
        value = _num(raw_value)
        if value is not None:
            values.append(abs(value))
    return _percentile(values, 0.95), max(values) if values else None


def _load_rows(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    return load_timeseries_rows(path)


def _route_curve_artifact_gap_report(
    *,
    run_dir: Path | None,
    timeseries_path: Path | None,
    summary_path: Path | None,
) -> tuple[dict[str, Any], Path | None, str | None]:
    report_path = _first_recursive(run_dir, "route_curve_artifact_gap_report.json")
    report = _read_json(report_path)
    if report:
        return report, report_path, "report_file"
    if timeseries_path is None:
        return {}, None, None
    report = analyze_route_curve_artifact_gap(timeseries_path, summary_json=summary_path)
    return report, None, "computed_inline"


def _route_requires_curve_artifact_gap(route_id: Any) -> bool:
    route = str(route_id or "")
    return route in CURVE_DIAGNOSTIC_ROUTE_IDS


def _route_curve_gap_issue(row: Mapping[str, Any] | None, *, role: str) -> str | None:
    if not row or not _route_requires_curve_artifact_gap(row.get("route_id")):
        return None
    if row.get("route_curve_artifact_gap_status") == "pass":
        return None
    fields = row.get("route_curve_missing_p1_fields") or []
    suffix = f": missing {', '.join(fields)}" if fields else ""
    return (
        f"{role} route_curve_artifact_gap not pass"
        f" ({row.get('route_curve_artifact_gap_status') or 'missing'}){suffix}"
    )


def _load_jsonl_rows(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _row_time(row: Mapping[str, Any]) -> float | None:
    return _num(row.get("sim_time") or row.get("t") or row.get("timestamp") or row.get("wall_time_s"))


def _row_frame(row: Mapping[str, Any]) -> str | None:
    for name in ("frame_id", "frame"):
        value = row.get(name)
        if value not in (None, ""):
            return str(value)
    return None


def _first_present(row: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        value = row.get(name)
        if value not in (None, ""):
            return value
    return None


def _safety_control_context(
    *,
    cte: float | None,
    apollo_steer: float | None,
    mapped_steer: float | None,
    applied_steer: float | None,
) -> str | None:
    high_lateral_error = cte is not None and abs(cte) >= 0.5
    raw_zero = apollo_steer is not None and abs(apollo_steer) < 0.001
    mapped_zero = mapped_steer is not None and abs(mapped_steer) < 0.001
    applied_zero = applied_steer is not None and abs(applied_steer) < 0.001
    if high_lateral_error and raw_zero and mapped_zero and applied_zero:
        return "no_lateral_command_at_safety_event"
    if high_lateral_error and apollo_steer is not None and mapped_steer is not None:
        if abs(apollo_steer) >= 0.01 and abs(mapped_steer) < 0.001:
            return "mapping_suppressed_lateral_command"
    if high_lateral_error and mapped_steer is not None and applied_steer is not None:
        if abs(mapped_steer) >= 0.01 and abs(applied_steer) < 0.001:
            return "actuation_or_apply_mismatch"
    if apollo_steer is None and mapped_steer is None and applied_steer is None:
        return None
    return "control_trace_available"


def _safety_context_from_row(
    row: Mapping[str, Any] | None,
    *,
    event_type: str,
    source: str,
    event_time: float | None = None,
    event_frame: str | None = None,
) -> dict[str, Any]:
    row = row or {}
    apollo_steer = _num(_first_present(row, "apollo_steer_raw", "apollo_raw_steer", "raw_steer"))
    mapped_steer = _num(_first_present(row, "bridge_steer_mapped", "mapped_steer", "clamped_steer"))
    applied_steer = _num(_first_present(row, "carla_steer_applied", "applied_steer", "steer"))
    cte = _num(_first_present(row, "cross_track_error", "lateral_error"))
    return {
        "first_safety_event_type": event_type,
        "first_safety_event_time_s": event_time if event_time is not None else _row_time(row),
        "first_safety_event_frame_id": event_frame if event_frame is not None else _row_frame(row),
        "first_safety_event_route_s": _num(row.get("route_s")),
        "first_safety_event_cross_track_error_m": cte,
        "first_safety_event_heading_error_rad": _num(row.get("heading_error")),
        "first_safety_event_ego_speed_mps": _num(row.get("ego_speed") or row.get("v_mps") or row.get("speed_mps")),
        "first_safety_event_apollo_steer_raw": apollo_steer,
        "first_safety_event_bridge_steer_mapped": mapped_steer,
        "first_safety_event_carla_steer_applied": applied_steer,
        "first_safety_event_throttle_raw": _num(_first_present(row, "throttle_raw", "cmd_throttle", "throttle")),
        "first_safety_event_throttle_mapped": _num(_first_present(row, "throttle_mapped", "clamped_throttle")),
        "first_safety_event_throttle_applied": _num(_first_present(row, "throttle_applied", "applied_throttle")),
        "first_safety_event_brake_raw": _num(_first_present(row, "brake_raw", "cmd_brake", "brake")),
        "first_safety_event_brake_mapped": _num(_first_present(row, "brake_mapped", "clamped_brake")),
        "first_safety_event_brake_applied": _num(_first_present(row, "brake_applied", "applied_brake")),
        "first_safety_event_lateral_guard_applied": _nullable_boolish(row.get("lateral_guard_applied") or row.get("dbg_lat_guard")),
        "first_safety_event_trajectory_contract_guard_applied": _nullable_boolish(
            row.get("trajectory_contract_guard_applied")
        ),
        "first_safety_event_control_context": _safety_control_context(
            cte=cte,
            apollo_steer=apollo_steer,
            mapped_steer=mapped_steer,
            applied_steer=applied_steer,
        ),
        "first_safety_event_source": source,
    }


def _matching_timeseries_row(
    rows: Sequence[Mapping[str, Any]],
    *,
    frame: str | None,
    time_s: float | None,
) -> Mapping[str, Any] | None:
    if frame is not None:
        for row in rows:
            if _row_frame(row) == str(frame):
                return row
    if time_s is None:
        return None
    candidates: list[tuple[float, Mapping[str, Any]]] = []
    for row in rows:
        row_time = _row_time(row)
        if row_time is not None:
            candidates.append((abs(row_time - time_s), row))
    if not candidates:
        return None
    delta, row = min(candidates, key=lambda item: item[0])
    return row if delta <= 0.25 else None


def _first_counter_safety_event(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    counters = (
        ("collision_count", "collision"),
        ("lane_invasion_count", "lane_invasion"),
    )
    max_seen = {name: 0 for name, _event_type in counters}
    events: list[dict[str, Any]] = []
    for row in rows:
        for counter_name, event_type in counters:
            value = _int_count(row.get(counter_name))
            if value is None:
                continue
            if value > max_seen[counter_name]:
                events.append(
                    _safety_context_from_row(
                        row,
                        event_type=event_type,
                        source=f"timeseries:{counter_name}",
                    )
                )
            max_seen[counter_name] = max(max_seen[counter_name], value)
    if not events:
        return None
    return min(
        events,
        key=lambda item: (
            item.get("first_safety_event_time_s")
            if item.get("first_safety_event_time_s") is not None
            else float("inf")
        ),
    )


def _event_type_from_event_row(row: Mapping[str, Any]) -> str | None:
    event_type = str(row.get("event_type") or "").strip().lower()
    reason = str(row.get("reason") or row.get("fail_reason") or "").strip().lower()
    text = f"{event_type} {reason}"
    if "collision" in text:
        return "collision"
    if "lane_invasion" in text or "lane invasion" in text:
        return "lane_invasion"
    return None


def _first_jsonl_safety_event(
    rows: Sequence[Mapping[str, Any]],
    events: Sequence[Mapping[str, Any]],
) -> dict[str, Any] | None:
    contexts: list[dict[str, Any]] = []
    for event in events:
        event_type = _event_type_from_event_row(event)
        if event_type is None:
            continue
        event_time = _row_time(event)
        event_frame = _row_frame(event)
        timeseries_row = _matching_timeseries_row(rows, frame=event_frame, time_s=event_time)
        contexts.append(
            _safety_context_from_row(
                timeseries_row or event,
                event_type=event_type,
                source="events.jsonl",
                event_time=event_time,
                event_frame=event_frame,
            )
        )
    if not contexts:
        return None
    return min(
        contexts,
        key=lambda item: (
            item.get("first_safety_event_time_s")
            if item.get("first_safety_event_time_s") is not None
            else float("inf")
        ),
    )


def _first_safety_event(
    rows: Sequence[Mapping[str, Any]],
    events: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    candidates = [
        item
        for item in (
            _first_counter_safety_event(rows),
            _first_jsonl_safety_event(rows, events),
        )
        if item is not None
    ]
    empty = _safety_context_from_row(
        None,
        event_type="",
        source="",
    )
    if not candidates:
        return {key: None for key in empty}
    return min(
        candidates,
        key=lambda item: (
            item.get("first_safety_event_time_s")
            if item.get("first_safety_event_time_s") is not None
            else float("inf")
        ),
    )


def _pre_safety_window_rows(
    rows: Sequence[Mapping[str, Any]],
    safety_event: Mapping[str, Any],
    *,
    window_s: float = 2.0,
) -> list[Mapping[str, Any]]:
    event_time = _num(safety_event.get("first_safety_event_time_s"))
    if event_time is None:
        return []
    start = event_time - window_s
    selected: list[Mapping[str, Any]] = []
    for row in rows:
        row_time = _row_time(row)
        if row_time is None:
            continue
        if start <= row_time <= event_time:
            selected.append(row)
    return selected


def _abs_values(rows: Sequence[Mapping[str, Any]], *names: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = _num(_first_present(row, *names))
        if value is not None:
            values.append(abs(value))
    return values


def _window_control_context(
    *,
    cte_end: float | None,
    cte_delta: float | None,
    raw_p95: float | None,
    mapped_p95: float | None,
    applied_p95: float | None,
) -> str | None:
    high_and_growing_cte = (
        cte_end is not None
        and cte_end >= 0.5
        and cte_delta is not None
        and cte_delta >= 0.05
    )
    raw_zero = raw_p95 is not None and raw_p95 < 0.001
    mapped_zero = mapped_p95 is not None and mapped_p95 < 0.001
    applied_zero = applied_p95 is not None and applied_p95 < 0.001
    if high_and_growing_cte and raw_zero and mapped_zero and applied_zero:
        return "sustained_no_lateral_command_before_safety_event"
    if high_and_growing_cte and raw_p95 is not None and mapped_p95 is not None:
        if raw_p95 >= 0.01 and mapped_zero:
            return "mapping_suppressed_lateral_command_before_safety_event"
    if high_and_growing_cte and mapped_p95 is not None and applied_p95 is not None:
        if mapped_p95 >= 0.01 and applied_zero:
            return "actuation_or_apply_mismatch_before_safety_event"
    if raw_p95 is None and mapped_p95 is None and applied_p95 is None:
        return None
    return "pre_safety_window_control_trace_available"


def _pre_safety_window_summary(
    rows: Sequence[Mapping[str, Any]],
    safety_event: Mapping[str, Any],
    *,
    window_s: float = 2.0,
) -> dict[str, Any]:
    keys = {
        "safety_window_duration_s": None,
        "safety_window_sample_count": None,
        "safety_window_cross_track_error_start_m": None,
        "safety_window_cross_track_error_end_m": None,
        "safety_window_cross_track_error_delta_m": None,
        "safety_window_cross_track_error_max_m": None,
        "safety_window_apollo_steer_raw_abs_p95": None,
        "safety_window_bridge_steer_mapped_abs_p95": None,
        "safety_window_carla_steer_applied_abs_p95": None,
        "safety_window_zero_lateral_command_ratio": None,
        "safety_window_control_context": None,
    }
    window_rows = _pre_safety_window_rows(rows, safety_event, window_s=window_s)
    if not window_rows:
        return keys
    times = [_row_time(row) for row in window_rows]
    times = [time for time in times if time is not None]
    cte_values = [
        value
        for value in (_num(_first_present(row, "cross_track_error", "lateral_error")) for row in window_rows)
        if value is not None
    ]
    raw_values = _abs_values(window_rows, "apollo_steer_raw", "apollo_raw_steer", "raw_steer")
    mapped_values = _abs_values(window_rows, "bridge_steer_mapped", "mapped_steer", "clamped_steer")
    applied_values = _abs_values(window_rows, "carla_steer_applied", "applied_steer", "steer")
    raw_p95 = _percentile(raw_values, 0.95)
    mapped_p95 = _percentile(mapped_values, 0.95)
    applied_p95 = _percentile(applied_values, 0.95)
    zero_command_frames = 0
    command_frames = 0
    for row in window_rows:
        raw = _num(_first_present(row, "apollo_steer_raw", "apollo_raw_steer", "raw_steer"))
        mapped = _num(_first_present(row, "bridge_steer_mapped", "mapped_steer", "clamped_steer"))
        applied = _num(_first_present(row, "carla_steer_applied", "applied_steer", "steer"))
        if raw is None and mapped is None and applied is None:
            continue
        command_frames += 1
        if all(value is not None and abs(value) < 0.001 for value in (raw, mapped, applied)):
            zero_command_frames += 1
    cte_start = cte_values[0] if cte_values else None
    cte_end = cte_values[-1] if cte_values else None
    cte_delta = None if cte_start is None or cte_end is None else cte_end - cte_start
    return {
        "safety_window_duration_s": None if len(times) < 2 else max(times) - min(times),
        "safety_window_sample_count": len(window_rows),
        "safety_window_cross_track_error_start_m": cte_start,
        "safety_window_cross_track_error_end_m": cte_end,
        "safety_window_cross_track_error_delta_m": cte_delta,
        "safety_window_cross_track_error_max_m": max(abs(value) for value in cte_values) if cte_values else None,
        "safety_window_apollo_steer_raw_abs_p95": raw_p95,
        "safety_window_bridge_steer_mapped_abs_p95": mapped_p95,
        "safety_window_carla_steer_applied_abs_p95": applied_p95,
        "safety_window_zero_lateral_command_ratio": None
        if not command_frames
        else zero_command_frames / float(command_frames),
        "safety_window_control_context": _window_control_context(
            cte_end=cte_end,
            cte_delta=cte_delta,
            raw_p95=raw_p95,
            mapped_p95=mapped_p95,
            applied_p95=applied_p95,
        ),
    }


def _nested_mapping(payload: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = payload.get(key)
    return value if isinstance(value, Mapping) else {}


def _bridge_stats_sim_time_sec(cyber_bridge_stats: Mapping[str, Any]) -> float | None:
    timing = _nested_mapping(cyber_bridge_stats, "timing")
    return (
        _num(timing.get("sim_time_sec"))
        or _num(cyber_bridge_stats.get("sim_time_sec"))
        or _num(cyber_bridge_stats.get("duration_s"))
    )


def _steering_normalization_trace(run_dir: Path | None) -> dict[str, Any]:
    path = _first_recursive(run_dir, "bridge_control_decode.jsonl")
    source = "bridge_control_decode"
    if path is None:
        path = _first_recursive(run_dir, "control_decode_debug.jsonl")
        source = "control_decode_debug" if path is not None else ""
    counts: dict[str, int] = {}
    for row in _load_jsonl_rows(path):
        candidates = [
            row.get("steering_normalization_mode"),
            _nested_mapping(row, "parsed_control").get("steering_normalization_mode"),
            _nested_mapping(row, "output_to_carla").get("steering_normalization_mode"),
        ]
        for candidate in candidates:
            mode = str(candidate or "").strip()
            if not mode:
                continue
            counts[mode] = counts.get(mode, 0) + 1
            break
    return {
        "path": None if path is None else str(path),
        "source": source,
        "modes": sorted(counts),
        "mode_counts": counts,
        "sample_count": sum(counts.values()),
    }


def _control_health_summary(control_health: Mapping[str, Any]) -> dict[str, Any]:
    metrics = control_health.get("metrics")
    if not isinstance(metrics, Mapping):
        metrics = {}
    bridge_log = metrics.get("control_bridge_log")
    if not isinstance(bridge_log, Mapping):
        bridge_log = {}
    return {
        "control_health_status": control_health.get("status"),
        "control_health_failure_reason": control_health.get("failure_reason"),
        "control_health_warnings": list(control_health.get("warnings") or []),
        "control_apply_observation_delay_s": _num(metrics.get("control_apply_observation_delay_s")),
        "route_s_after_first_applied_control_delta_m": _num(
            metrics.get("route_s_after_first_applied_control_delta_m")
        ),
        "stopped_ratio_after_first_applied_control": _num(
            metrics.get("stopped_ratio_after_first_applied_control")
        ),
        "control_bridge_apply_world_frame_hz": _num(bridge_log.get("apply_world_frame_hz")),
        "control_bridge_same_frame_drop_ratio": _num(bridge_log.get("same_frame_drop_ratio")),
        "control_bridge_bind_to_first_apply_s": _num(bridge_log.get("bind_to_first_apply_s")),
        "control_bridge_first_watchdog_apply_wall_s": _num(
            bridge_log.get("first_watchdog_apply_wall_s")
        ),
        "control_bridge_final_rx_count": _int_count(bridge_log.get("final_rx_count")),
        "control_bridge_final_applied_count": _int_count(bridge_log.get("final_applied_count")),
        "control_bridge_final_drop_same_frame_count": _int_count(
            bridge_log.get("final_drop_same_frame_count")
        ),
    }


def _apollo_control_handoff_summary(
    report: Mapping[str, Any],
    path: Path | None,
) -> dict[str, Any]:
    return {
        "apollo_control_handoff_path": None if path is None else str(path),
        "apollo_control_handoff_status": report.get("verdict"),
        "apollo_control_handoff_failure_stage": report.get("failure_stage"),
        "apollo_control_handoff_blocking_reasons": list(report.get("blocking_reasons") or []),
    }


def _channel_health_summary(channel_health: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "apollo_channel_health_status": channel_health.get("status"),
        "apollo_channel_health_missing_required_channels": list(
            channel_health.get("missing_required_channels") or []
        ),
        "apollo_channel_health_low_rate_channels": list(channel_health.get("low_rate_channels") or []),
    }


def _run_result(record: ABRunRecord, *, manifest_dir: Path) -> dict[str, Any]:
    run_dir = _resolve(record.run_dir, manifest_dir=manifest_dir)
    summary_path = _resolve(record.summary_path, manifest_dir=manifest_dir, run_dir=run_dir)
    route_health_path = _resolve(record.route_health_path, manifest_dir=manifest_dir, run_dir=run_dir)
    control_health_path = _resolve(record.control_health_path, manifest_dir=manifest_dir, run_dir=run_dir)
    channel_health_path = _resolve(
        record.apollo_channel_health_path,
        manifest_dir=manifest_dir,
        run_dir=run_dir,
    )
    timeseries_path = None
    if summary_path is None or not summary_path.exists():
        summary_path = _first_recursive(run_dir, "summary.json")
    if route_health_path is None or not route_health_path.exists():
        route_health_path = _first_recursive(run_dir, "route_health.json")
    if control_health_path is None or not control_health_path.exists():
        control_health_path = _first_recursive(run_dir, "control_health_report.json")
    apollo_control_handoff_path = _first_recursive(run_dir, "apollo_control_handoff_report.json")
    if channel_health_path is None or not channel_health_path.exists():
        channel_health_path = _first_recursive(run_dir, "apollo_channel_health_report.json")
    if run_dir is not None:
        for candidate in [run_dir / "timeseries.csv", run_dir / "timeseries.jsonl"]:
            if candidate.exists():
                timeseries_path = candidate
                break
        if timeseries_path is None:
            timeseries_path = _first_recursive(run_dir, "timeseries.csv") or _first_recursive(run_dir, "timeseries.jsonl")
    events_path = _first_recursive(run_dir, "events.jsonl")

    summary = _read_json(summary_path)
    route_health = _read_json(route_health_path)
    control_health = _read_json(control_health_path)
    apollo_control_handoff = _read_json(apollo_control_handoff_path)
    channel_health = _read_json(channel_health_path)
    direct_stats = _read_json(_first_recursive(run_dir, "direct_bridge_stats.json"))
    cyber_bridge_stats = _read_json(_first_recursive(run_dir, "cyber_bridge_stats.json"))
    assist_ledger = read_assist_ledger_from_run_dir(run_dir) if run_dir is not None else build_assist_ledger()
    if record.backend == "carla_direct" and "carla_direct_transport" not in set(assist_ledger.get("active_assists") or []):
        assist_ledger = build_assist_ledger(
            config=None,
            bridge_stats={**direct_stats, **cyber_bridge_stats},
            summary=summary,
            manifest={"backend": record.backend, "assist_ledger": assist_ledger},
        )
    steering_trace = _steering_normalization_trace(run_dir)
    route_health_source = "route_health_file" if route_health else None
    if not route_health and isinstance(summary.get("route_health"), dict):
        route_health = dict(summary.get("route_health") or {})
        route_health_source = "summary_embedded_route_health"
    rows = _load_rows(timeseries_path)
    route_curve_gap, route_curve_gap_path, route_curve_gap_source = _route_curve_artifact_gap_report(
        run_dir=run_dir,
        timeseries_path=timeseries_path,
        summary_path=summary_path,
    )
    missing_fields: list[str] = []
    artifact_complete = bool(summary and route_health and rows)
    if not summary:
        missing_fields.append("summary")
    if not route_health:
        missing_fields.append("route_health")
    if not rows:
        missing_fields.append("timeseries")
    if _route_requires_curve_artifact_gap(record.route_id) and route_curve_gap.get("status") != "pass":
        missing_fields.extend(
            f"route_curve_artifact_gap.{field}"
            for field in (route_curve_gap.get("missing_p1_fields") or ["status"])
        )
    event_rows = _load_jsonl_rows(events_path)
    safety_event = _first_safety_event(rows, event_rows)
    safety_window = _pre_safety_window_summary(rows, safety_event)

    metrics = route_health.get("run_metrics") or {}
    route_completion = _num(
        summary.get("route_completion_ratio")
        or summary.get("route_completion")
        or route_health.get("route_completion_ratio")
        or route_health.get("route_completion_percentage")
    )
    duration = _duration_from_rows(rows, record.duration_s)
    lateral_mean_from_rows, lateral_p95_from_rows, lateral_max_from_rows = _metric_summary(
        _abs_series(rows, "cross_track_error", "lateral_error")
    )
    heading_mean_from_rows, heading_p95_from_rows, heading_max_from_rows = _metric_summary(
        _abs_series(rows, "heading_error")
    )
    lateral_p95 = _num(metrics.get("lateral_error_p95_m")) or lateral_p95_from_rows
    lateral_max = _num(metrics.get("lateral_error_max_m")) or lateral_max_from_rows
    heading_p95 = _num(metrics.get("heading_error_p95_rad")) or heading_p95_from_rows
    heading_max = _num(metrics.get("heading_error_max_rad")) or heading_max_from_rows
    for key, value in {
        "lateral_error_p95_m": lateral_p95,
        "heading_error_p95_rad": heading_p95,
    }.items():
        if value is None:
            missing_fields.append(key)
    curve_p95, curve_max = _curve_lateral_stats(rows)
    direct_policy_source = ""
    direct_policy = direct_stats.get("stale_world_frame_policy")
    if direct_policy is not None:
        direct_policy_source = "direct_bridge_stats"
    if direct_policy is None:
        direct_policy = cyber_bridge_stats.get("direct_stale_world_frame_policy")
        if direct_policy is not None:
            direct_policy_source = "cyber_bridge_stats"
    if direct_policy is None:
        direct_policy, direct_policy_source = _infer_direct_stale_world_frame_policy(cyber_bridge_stats)
    if direct_policy is None and record.direct_stale_world_frame_policy is not None:
        direct_policy = record.direct_stale_world_frame_policy
        direct_policy_source = "manifest"
    if direct_policy is None and (direct_stats or record.backend == "carla_direct"):
        direct_policy = "until_control"
        direct_policy_source = "implicit_default"
    observed_apply_mode = (
        (summary.get("direct_control_apply_summary") or {}).get("apply_mode")
        or direct_stats.get("control_apply_mode")
    )
    direct_apply_mode = observed_apply_mode or record.direct_control_apply_mode
    direct_transport_contract_status = "not_applicable"
    direct_transport_contract_reasons: list[str] = []
    if record.backend == "carla_direct":
        if direct_stats and record.direct_control_apply_mode and observed_apply_mode != record.direct_control_apply_mode:
            direct_transport_contract_reasons.append(
                "direct_control_apply_mode mismatch"
            )
        if (
            direct_stats
            and record.direct_stale_world_frame_policy
            and direct_policy != record.direct_stale_world_frame_policy
        ):
            direct_transport_contract_reasons.append(
                "direct_stale_world_frame_policy mismatch"
            )
        if artifact_complete and not direct_stats:
            direct_transport_contract_reasons.append("direct_bridge_stats missing")
        if artifact_complete and direct_policy_source in {"manifest", "implicit_default"}:
            direct_transport_contract_reasons.append(
                "direct_stale_world_frame_policy not observed in artifacts"
            )
        if direct_transport_contract_reasons:
            direct_transport_contract_status = "mismatch"
        elif artifact_complete:
            direct_transport_contract_status = "aligned"
        else:
            direct_transport_contract_status = "planned_only"
    apollo_steer = _series(rows, "apollo_steer_raw", "apollo_raw_steer", "raw_steer")
    applied_steer = _series(rows, "carla_steer_applied", "applied_steer", "steer")
    ego_speeds = _series(rows, "ego_speed", "v_mps", "speed_mps")
    cross_track = [abs(value) for value in _series(rows, "cross_track_error", "lateral_error")]
    apollo_control_hz = _rate(rows, duration, "apollo_steer_raw", "apollo_raw_steer", "raw_steer")
    if apollo_control_hz is None:
        apollo_control_hz = _summary_count_rate(
            summary,
            duration,
            "apollo_control_raw_row_count",
            "control_consume_row_count",
            "control_tx_count",
        )
    carla_applied_control_hz = _rate(rows, duration, "carla_steer_applied", "applied_steer", "steer")
    if carla_applied_control_hz is None:
        carla_applied_control_hz = _summary_count_rate(
            summary,
            duration,
            "direct_control_apply_count",
            "control_apply_count",
            "control_consume_row_count",
        )
    planning_hz = _rate(rows, duration, "planning_available", "planning_timestamp", bool_only=True)
    if planning_hz is None:
        planning_hz = _summary_planning_rate(summary, rows, duration)
    localization_hz = _rate(rows, duration, "localization_timestamp", "ego_x")
    if localization_hz is None and rows and duration and duration > 0 and any(row.get("ego_x") not in (None, "") for row in rows):
        localization_hz = len(rows) / duration
    bridge_loc_count = _int_count(cyber_bridge_stats.get("loc_count"))
    bridge_chassis_count = _int_count(cyber_bridge_stats.get("chassis_count"))
    bridge_rate_duration = record.duration_s if record.duration_s and record.duration_s > 0 else duration
    bridge_rate_duration_source = (
        "manifest_duration_s"
        if record.duration_s and record.duration_s > 0
        else ("timeseries_duration_s" if duration and duration > 0 else None)
    )
    bridge_stats_sim_time = _bridge_stats_sim_time_sec(cyber_bridge_stats)

    result = {
        "run_id": record.run_id,
        "route_id": record.route_id,
        "backend": record.backend,
        "run_status": record.status,
        "return_code": record.return_code,
        "transport_mode": record.transport_mode,
        "bridge_mode": record.bridge_mode,
        "backend_config_path": record.backend_config_path,
        "assist_ledger": assist_ledger,
        "active_assists": list(assist_ledger.get("active_assists") or []),
        "blocking_assists": list(assist_ledger.get("blocking_assists") or []),
        "non_blocking_assists": list(assist_ledger.get("non_blocking_assists") or []),
        "can_claim_unassisted": assist_ledger.get("can_claim_unassisted_natural_driving"),
        "duration_s": record.duration_s,
        "artifact_dir": None if run_dir is None else str(run_dir),
        "actual_run_dir": record.actual_run_dir,
        "summary_path": None if summary_path is None else str(summary_path),
        "route_health_path": None if route_health_path is None else str(route_health_path),
        "route_source": route_health.get("route_source"),
        "route_evidence_level": route_health.get("evidence_level"),
        "route_hard_gate_eligible": route_health.get("hard_gate_eligible"),
        "route_curve_artifact_gap_path": None
        if route_curve_gap_path is None
        else str(route_curve_gap_path),
        "route_curve_artifact_gap_source": route_curve_gap_source,
        "route_curve_artifact_gap_status": route_curve_gap.get("status"),
        "route_curve_artifact_gap_failure_reason": route_curve_gap.get("failure_reason"),
        "route_curve_per_frame_p1_complete": route_curve_gap.get("per_frame_p1_complete"),
        "route_curve_missing_p1_fields": route_curve_gap.get("missing_p1_fields") or [],
        "route_curve_summary_semantics_available": route_curve_gap.get("summary_semantics_available"),
        "timeseries_path": None if timeseries_path is None else str(timeseries_path),
        "events_path": None if events_path is None else str(events_path),
        "command_stdout_path": record.command_stdout_path,
        "command_stderr_path": record.command_stderr_path,
        "apollo_channel_health_path": None if channel_health_path is None else str(channel_health_path),
        **_channel_health_summary(channel_health),
        "control_health_path": None if control_health_path is None else str(control_health_path),
        **_control_health_summary(control_health),
        **_apollo_control_handoff_summary(apollo_control_handoff, apollo_control_handoff_path),
        "artifact_complete": artifact_complete,
        "bridge_loc_count": bridge_loc_count,
        "bridge_chassis_count": bridge_chassis_count,
        "bridge_obstacles_count": _int_count(cyber_bridge_stats.get("obstacles_count")),
        "bridge_control_rx_count": _int_count(cyber_bridge_stats.get("control_rx_count")),
        "bridge_control_tx_count": _int_count(cyber_bridge_stats.get("control_tx_count")),
        "bridge_routing_request_count": _int_count(cyber_bridge_stats.get("routing_request_count")),
        "bridge_routing_success_count": _int_count(cyber_bridge_stats.get("routing_success_count")),
        "bridge_loc_hz": _count_rate(bridge_loc_count, bridge_rate_duration),
        "bridge_chassis_hz": _count_rate(bridge_chassis_count, bridge_rate_duration),
        "bridge_cadence_duration_s": bridge_rate_duration,
        "bridge_cadence_duration_source": bridge_rate_duration_source,
        "bridge_stats_sim_time_sec": bridge_stats_sim_time,
        "bridge_loc_hz_by_stats_sim_time": _count_rate(bridge_loc_count, bridge_stats_sim_time),
        "bridge_chassis_hz_by_stats_sim_time": _count_rate(bridge_chassis_count, bridge_stats_sim_time),
        "direct_snapshot_hz_by_stats_sim_time": _count_rate(
            direct_stats.get("snapshot_count"),
            bridge_stats_sim_time,
        ),
        "direct_control_apply_mode": direct_apply_mode,
        "direct_stale_world_frame_policy": direct_policy,
        "direct_stale_world_frame_skip_count": cyber_bridge_stats.get(
            "direct_stale_world_frame_skip_count"
        ),
        "direct_stale_world_frame_republish_count": cyber_bridge_stats.get(
            "direct_stale_world_frame_republish_count"
        ),
        "direct_snapshot_count": direct_stats.get("snapshot_count"),
        "direct_stale_world_frame_policy_source": direct_policy_source,
        "direct_transport_contract_status": direct_transport_contract_status,
        "direct_transport_contract_reasons": sorted(set(direct_transport_contract_reasons)),
        "steering_percent_normalization_planned": record.steering_percent_normalization,
        "route_health_source": route_health_source,
        "route_completion": route_completion,
        "max_speed_mps": _num(summary.get("max_speed_mps")) or (max(ego_speeds) if ego_speeds else None),
        "lateral_error_mean_m": _num(metrics.get("lateral_error_mean_m")) or lateral_mean_from_rows,
        "lateral_error_p95_m": lateral_p95,
        "lateral_error_max_m": lateral_max,
        "heading_error_mean_rad": _num(metrics.get("heading_error_mean_rad")) or heading_mean_from_rows,
        "heading_error_p95_rad": heading_p95,
        "heading_error_max_rad": heading_max,
        "curve_segment_lateral_error_p95_m": curve_p95,
        "curve_segment_lateral_error_max_m": curve_max,
        "recovery_after_curve_s": None,
        "steering_command_smoothness": _smoothness(apollo_steer),
        "applied_steering_smoothness": _smoothness(applied_steer),
        "apollo_control_hz": apollo_control_hz,
        "carla_applied_control_hz": carla_applied_control_hz,
        "planning_hz": planning_hz,
        "localization_hz": localization_hz,
        "chassis_hz": _count_rate(bridge_chassis_count, bridge_rate_duration)
        or _rate(rows, duration, "chassis_timestamp", "chassis_available", bool_only=True),
        "control_latency_p95_ms": _percentile(_series(rows, "control_latency_ms"), 0.95),
        "brake_throttle_conflict_frames": sum(
            1
            for row in rows
            if (_num(row.get("throttle_applied")) or 0.0) > 0.05 and (_num(row.get("brake_applied")) or 0.0) > 0.05
        ),
        "stop_ratio": (
            None
            if not ego_speeds
            else sum(1 for speed in ego_speeds if abs(speed) < 0.2) / float(len(ego_speeds))
        ),
        "stuck_duration_s": None if duration is None or not ego_speeds else duration * sum(1 for speed in ego_speeds if abs(speed) < 0.2) / len(ego_speeds),
        "off_route_duration_s": None if duration is None or not cross_track else duration * sum(1 for value in cross_track if value > 2.0) / len(cross_track),
        "collision_count": int(summary.get("collision_count") or 0),
        "lane_invasion_count": int(summary.get("lane_invasion_count") or 0),
        **safety_event,
        **safety_window,
        "steering_normalization_modes": steering_trace["modes"],
        "steering_normalization_mode_counts": steering_trace["mode_counts"],
        "steering_normalization_trace_path": steering_trace["path"],
        "steering_normalization_trace_source": steering_trace["source"],
        "steering_normalization_trace_sample_count": steering_trace["sample_count"],
        "failure_reason": None,
        "route_health_error": record.route_health_error,
        "missing_fields": sorted(set(missing_fields)),
    }
    result["failure_reason"] = classify_failure(
        {
            **result,
            "exit_reason": summary.get("exit_reason") or summary.get("fail_reason") or record.status,
        },
        {"artifact_complete": artifact_complete, "route_health_missing": not bool(route_health)},
    )
    return result


def _compare_pair(
    baseline: dict[str, Any] | None,
    candidate: dict[str, Any] | None,
    *,
    completion_threshold: float = 0.5,
    allow_assist_mismatch: bool = False,
) -> dict[str, Any]:
    route_id = (candidate or baseline or {}).get("route_id")
    duration_s = (candidate or baseline or {}).get("duration_s")
    if baseline is None or candidate is None:
        return {
            "route_id": route_id,
            "duration_s": duration_s,
            "status": "insufficient_data",
            "reasons": ["missing baseline or candidate run"],
            "metric_tolerances": _comparison_tolerances(),
        }
    cadence_comparison = {
        "bridge_loc_hz_ratio": _ratio(candidate.get("bridge_loc_hz"), baseline.get("bridge_loc_hz")),
        "bridge_chassis_hz_ratio": _ratio(candidate.get("bridge_chassis_hz"), baseline.get("bridge_chassis_hz")),
        "bridge_loc_hz_by_stats_sim_time_ratio": _ratio(
            candidate.get("bridge_loc_hz_by_stats_sim_time"),
            baseline.get("bridge_loc_hz_by_stats_sim_time"),
        ),
        "bridge_chassis_hz_by_stats_sim_time_ratio": _ratio(
            candidate.get("bridge_chassis_hz_by_stats_sim_time"),
            baseline.get("bridge_chassis_hz_by_stats_sim_time"),
        ),
        "bridge_loc_count_delta": (
            None
            if _num(candidate.get("bridge_loc_count")) is None or _num(baseline.get("bridge_loc_count")) is None
            else int(_num(candidate.get("bridge_loc_count")) - _num(baseline.get("bridge_loc_count")))
        ),
        "bridge_chassis_count_delta": (
            None
            if _num(candidate.get("bridge_chassis_count")) is None or _num(baseline.get("bridge_chassis_count")) is None
            else int(_num(candidate.get("bridge_chassis_count")) - _num(baseline.get("bridge_chassis_count")))
        ),
    }
    control_apply_comparison = {
        "baseline_control_health_status": baseline.get("control_health_status"),
        "candidate_control_health_status": candidate.get("control_health_status"),
        "baseline_apollo_control_handoff_status": baseline.get("apollo_control_handoff_status"),
        "candidate_apollo_control_handoff_status": candidate.get("apollo_control_handoff_status"),
        "baseline_apollo_control_handoff_failure_stage": baseline.get(
            "apollo_control_handoff_failure_stage"
        ),
        "candidate_apollo_control_handoff_failure_stage": candidate.get(
            "apollo_control_handoff_failure_stage"
        ),
        "baseline_control_apply_observation_delay_s": baseline.get("control_apply_observation_delay_s"),
        "candidate_control_apply_observation_delay_s": candidate.get("control_apply_observation_delay_s"),
        "baseline_control_bridge_apply_world_frame_hz": baseline.get("control_bridge_apply_world_frame_hz"),
        "candidate_control_bridge_apply_world_frame_hz": candidate.get("control_bridge_apply_world_frame_hz"),
        "baseline_control_bridge_same_frame_drop_ratio": baseline.get(
            "control_bridge_same_frame_drop_ratio"
        ),
        "candidate_control_bridge_same_frame_drop_ratio": candidate.get(
            "control_bridge_same_frame_drop_ratio"
        ),
        "baseline_control_bridge_final_rx_count": baseline.get("control_bridge_final_rx_count"),
        "candidate_control_bridge_final_rx_count": candidate.get("control_bridge_final_rx_count"),
        "baseline_control_bridge_final_applied_count": baseline.get(
            "control_bridge_final_applied_count"
        ),
        "candidate_control_bridge_final_applied_count": candidate.get(
            "control_bridge_final_applied_count"
        ),
    }
    reasons: list[str] = []
    baseline_blocking = set(str(item) for item in (baseline.get("blocking_assists") or []) if item)
    candidate_blocking = set(str(item) for item in (candidate.get("blocking_assists") or []) if item)
    baseline_claim_relevant = set(str(item) for item in (baseline.get("active_assists") or []) if item) - NON_BLOCKING_ASSISTS
    candidate_claim_relevant = set(str(item) for item in (candidate.get("active_assists") or []) if item) - NON_BLOCKING_ASSISTS
    assist_mismatch = baseline_blocking != candidate_blocking or baseline_claim_relevant != candidate_claim_relevant
    if assist_mismatch and not allow_assist_mismatch:
        return {
            "route_id": route_id,
            "duration_s": duration_s,
            "baseline_run_id": baseline.get("run_id"),
            "candidate_run_id": candidate.get("run_id"),
            "status": "not_comparable",
            "reasons": [
                "assist mismatch blocks candidate_positive",
                f"baseline_blocking_assists={sorted(baseline_blocking)}",
                f"candidate_blocking_assists={sorted(candidate_blocking)}",
            ],
            "cadence_comparison": cadence_comparison,
            "control_apply_comparison": control_apply_comparison,
            "metric_tolerances": _comparison_tolerances(),
        }
    baseline_modes = set(str(item) for item in (baseline.get("steering_normalization_modes") or []) if item)
    candidate_modes = set(str(item) for item in (candidate.get("steering_normalization_modes") or []) if item)
    if baseline_modes or candidate_modes:
        if not baseline_modes or not candidate_modes:
            reasons.append("steering_normalization_mode missing on one side")
        elif baseline_modes != candidate_modes:
            reasons.append("steering_normalization_mode mismatch")
    for role, row in (("baseline", baseline), ("candidate", candidate)):
        status = str(row.get("run_status") or "").lower()
        return_code = row.get("return_code")
        if return_code not in (None, "", 0, "0"):
            reasons.append(f"{role} return_code={return_code}")
        if status in BAD_EXECUTION_STATUSES:
            reasons.append(f"{role} run_status={row.get('run_status')}")
    required_metric_keys = [
        "route_completion",
        "lateral_error_p95_m",
        "heading_error_p95_rad",
        "planning_hz",
        "carla_applied_control_hz",
        "localization_hz",
        "chassis_hz",
    ]
    for role, row in (("baseline", baseline), ("candidate", candidate)):
        if not row.get("artifact_complete"):
            reasons.append(f"{role} artifact_complete is false")
        missing_required = [key for key in required_metric_keys if row.get(key) is None]
        if missing_required:
            reasons.append(
                f"{role} missing required multi-metric evidence: {', '.join(missing_required)}"
            )
        if route_id in HARD_GATE_ROUTE_IDS and row.get("route_hard_gate_eligible") is not True:
            reasons.append(f"{role} route_health hard_gate_eligible is not true")
    if candidate.get("failure_reason") in BAD_POSITIVE_FAILURE_REASONS:
        reasons.append(f"candidate failure_reason={candidate.get('failure_reason')}")
    if candidate.get("backend") == "carla_direct" and candidate.get("direct_transport_contract_status") == "mismatch":
        reasons.append(
            "candidate direct transport contract mismatch: "
            + "; ".join(candidate.get("direct_transport_contract_reasons") or [])
        )
    if candidate.get("control_health_status") == "fail":
        reasons.append(
            f"candidate control_health failed: {candidate.get('control_health_failure_reason') or 'unknown'}"
        )
    candidate_handoff_status = candidate.get("apollo_control_handoff_status")
    candidate_handoff_stage = candidate.get("apollo_control_handoff_failure_stage")
    if candidate_handoff_status == "fail":
        reasons.append(
            "candidate apollo_control_handoff failed: "
            + str(candidate_handoff_stage or "unknown")
        )
    elif candidate_handoff_stage not in {None, "", "none"}:
        reasons.append(
            "candidate apollo_control_handoff failure_stage="
            + str(candidate_handoff_stage)
        )
    if reasons:
        return {
            "route_id": route_id,
            "duration_s": duration_s,
            "baseline_run_id": baseline.get("run_id"),
            "candidate_run_id": candidate.get("run_id"),
            "status": "insufficient_data",
            "reasons": reasons,
            "cadence_comparison": cadence_comparison,
            "control_apply_comparison": control_apply_comparison,
            "metric_tolerances": _comparison_tolerances(),
        }

    baseline_completion = _num(baseline.get("route_completion")) or 0.0
    candidate_completion = _num(candidate.get("route_completion")) or 0.0
    completion_floor = min(baseline_completion, completion_threshold)
    if candidate_completion + ROUTE_COMPLETION_REGRESSION_TOLERANCE + 1e-9 < completion_floor:
        reasons.append("route_completion regressed")
    baseline_lat = _num(baseline.get("lateral_error_p95_m"))
    candidate_lat = _num(candidate.get("lateral_error_p95_m"))
    baseline_heading = _num(baseline.get("heading_error_p95_rad"))
    candidate_heading = _num(candidate.get("heading_error_p95_rad"))
    if baseline_lat is None or candidate_lat is None or baseline_heading is None or candidate_heading is None:
        reasons.append("baseline or candidate missing lateral/heading evidence")
    else:
        if candidate_lat > max(
            baseline_lat * LATERAL_ERROR_P95_RELATIVE_TOLERANCE,
            baseline_lat + LATERAL_ERROR_P95_ABS_TOLERANCE_M,
        ):
            reasons.append("lateral_error_p95_m degraded")
        if candidate_heading > max(
            baseline_heading * HEADING_ERROR_P95_RELATIVE_TOLERANCE,
            baseline_heading + HEADING_ERROR_P95_ABS_TOLERANCE_RAD,
        ):
            reasons.append("heading_error_p95_rad degraded")
    if candidate.get("failure_reason") not in {"success", None}:
        reasons.append(f"candidate failure_reason={candidate.get('failure_reason')}")
    route_curve_issues = [
        issue
        for issue in (
            _route_curve_gap_issue(baseline, role="baseline"),
            _route_curve_gap_issue(candidate, role="candidate"),
        )
        if issue
    ]
    if not reasons and route_curve_issues:
        return {
            "route_id": route_id,
            "duration_s": duration_s,
            "baseline_run_id": baseline.get("run_id"),
            "candidate_run_id": candidate.get("run_id"),
            "status": "insufficient_data",
            "reasons": route_curve_issues,
            "cadence_comparison": cadence_comparison,
            "control_apply_comparison": control_apply_comparison,
            "metric_tolerances": _comparison_tolerances(),
        }
    if reasons:
        return {
            "route_id": route_id,
            "duration_s": duration_s,
            "baseline_run_id": baseline.get("run_id"),
            "candidate_run_id": candidate.get("run_id"),
            "status": "candidate_negative",
            "reasons": reasons,
            "cadence_comparison": cadence_comparison,
            "control_apply_comparison": control_apply_comparison,
            "metric_tolerances": _comparison_tolerances(),
        }
    return {
        "route_id": route_id,
        "duration_s": duration_s,
        "baseline_run_id": baseline.get("run_id"),
        "candidate_run_id": candidate.get("run_id"),
        "status": "candidate_positive",
        "reasons": ["multi_metric_gate_passed"],
        "cadence_comparison": cadence_comparison,
        "control_apply_comparison": control_apply_comparison,
        "metric_tolerances": _comparison_tolerances(),
    }


def _build_comparisons(manifest: ABManifest, run_results: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, float | None], dict[str, dict[str, Any]]] = {}
    for result in run_results:
        key = (str(result.get("route_id")), result.get("duration_s"))
        grouped.setdefault(key, {})[str(result.get("backend"))] = result
    comparisons: list[dict[str, Any]] = []
    allow_assist_mismatch = any("assist" in str(item).lower() for item in (manifest.allowed_differences or []))
    for (_route_id, _duration), by_backend in sorted(grouped.items(), key=lambda item: (item[0][0], item[0][1] or 0)):
        comparisons.append(
            _compare_pair(
                by_backend.get(manifest.baseline_backend),
                by_backend.get(manifest.candidate_backend),
                allow_assist_mismatch=allow_assist_mismatch,
            )
        )
    return comparisons


def _hard_gate_summary(comparisons: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_route = {str(item.get("route_id")): dict(item) for item in comparisons}
    missing: list[str] = []
    positive: list[str] = []
    negative: list[str] = []
    insufficient: list[str] = []
    for route_id in HARD_GATE_ROUTE_IDS:
        comparison = by_route.get(route_id)
        if comparison is None:
            missing.append(route_id)
            continue
        status = str(comparison.get("status") or "")
        if status == "candidate_positive":
            positive.append(route_id)
        elif status in {"candidate_negative", "candidate_degraded"}:
            negative.append(route_id)
        else:
            insufficient.append(route_id)
    if negative:
        status = "hard_gate_fail"
    elif insufficient:
        status = "hard_gate_insufficient_data"
    elif missing:
        status = "hard_gate_incomplete"
    else:
        status = "hard_gate_pass"
    return {
        "status": status,
        "expected_routes": list(HARD_GATE_ROUTE_IDS),
        "observed_routes": sorted(set(HARD_GATE_ROUTE_IDS) - set(missing)),
        "positive_routes": positive,
        "negative_routes": negative,
        "degraded_routes": negative,
        "insufficient_routes": insufficient,
        "missing_routes": missing,
        "complete": not missing,
        "pass": status == "hard_gate_pass",
    }


def analyze_ab_manifest(manifest_path: str | Path, *, batch_root: str | Path | None = None) -> dict[str, Any]:
    manifest_file = Path(manifest_path).expanduser()
    manifest = load_ab_manifest(manifest_file)
    manifest_dir = Path(batch_root).expanduser() if batch_root is not None else manifest_file.parent
    run_results = [_run_result(record, manifest_dir=manifest_dir) for record in manifest.runs]
    comparisons = _build_comparisons(manifest, run_results)
    missing_fields = sorted({field for result in run_results for field in result.get("missing_fields", [])})
    counts: dict[str, int] = {}
    for comparison in comparisons:
        counts[comparison["status"]] = counts.get(comparison["status"], 0) + 1
    if counts.get("candidate_negative") or counts.get("candidate_degraded"):
        status = "candidate_negative"
    elif counts.get("not_comparable"):
        status = "not_comparable"
    elif counts.get("candidate_positive") and not counts.get("insufficient_data"):
        status = "candidate_positive"
    elif counts.get("candidate_positive"):
        status = "inconclusive"
    else:
        status = "insufficient_data"
    return {
        "schema_version": AB_REPORT_SCHEMA_VERSION,
        "batch_id": manifest.batch_id,
        "source_manifest": str(manifest_file),
        "routes": sorted({result["route_id"] for result in run_results}),
        "durations_s": sorted({float(result["duration_s"]) for result in run_results if result.get("duration_s") is not None}),
        "backends": sorted({result["backend"] for result in run_results}),
        "run_results": run_results,
        "comparisons": comparisons,
        "missing_fields": missing_fields,
        "verdict": {
            "status": status,
            "comparison_counts": counts,
            "candidate_positive_requires_multi_metric_evidence": True,
            "hard_gate_summary": _hard_gate_summary(comparisons),
        },
    }


def check_ab_report_requirements(
    report: Mapping[str, Any],
    *,
    require_hard_gate_pass: bool = False,
    required_positive_routes: Sequence[str] | None = None,
    required_steering_mode: str | None = None,
    required_direct_control_apply_mode: str | None = None,
    required_direct_stale_world_frame_policy: str | None = None,
    require_direct_transport_contract_aligned: bool = False,
    required_direct_bridge_cadence_ratio_min: float | None = None,
    require_route_curve_p1_complete: bool = False,
) -> dict[str, Any]:
    failures: list[str] = []
    verdict = report.get("verdict") or {}
    hard_gate = verdict.get("hard_gate_summary") or {}
    run_results_by_id = {
        str(row.get("run_id")): row
        for row in (report.get("run_results") or [])
        if row.get("run_id")
    }
    if require_hard_gate_pass and not bool(hard_gate.get("pass")):
        failures.append(f"hard_gate_summary.status={hard_gate.get('status')}")
    if required_positive_routes:
        comparisons_by_route = {
            str(item.get("route_id")): item
            for item in (report.get("comparisons") or [])
            if item.get("route_id")
        }
        for route_id in required_positive_routes:
            comparison = comparisons_by_route.get(str(route_id))
            status = None if comparison is None else comparison.get("status")
            if status != "candidate_positive":
                failures.append(f"comparison_status for {route_id}: expected candidate_positive, got {status or 'missing'}")
    for row in report.get("run_results") or []:
        status = str(row.get("run_status") or "").lower()
        return_code = row.get("return_code")
        if return_code not in (None, "", 0, "0"):
            failures.append(f"run return_code nonzero for {row.get('run_id')}: {return_code}")
        if status in BAD_EXECUTION_STATUSES:
            failures.append(f"run_status not successful for {row.get('run_id')}: {row.get('run_status')}")
    if required_steering_mode:
        expected = str(required_steering_mode)
        for row in report.get("run_results") or []:
            modes = [str(item) for item in (row.get("steering_normalization_modes") or [])]
            if modes != [expected]:
                failures.append(
                    "steering_normalization_mode mismatch for "
                    f"{row.get('run_id')}: expected [{expected}], got {modes or 'missing'}"
                )
    if required_direct_control_apply_mode:
        expected_apply = str(required_direct_control_apply_mode)
        for row in report.get("run_results") or []:
            if row.get("backend") != "carla_direct":
                continue
            observed = row.get("direct_control_apply_mode")
            if observed != expected_apply:
                failures.append(
                    "direct_control_apply_mode mismatch for "
                    f"{row.get('run_id')}: expected {expected_apply}, got {observed or 'missing'}"
                )
    if required_direct_stale_world_frame_policy:
        expected_policy = str(required_direct_stale_world_frame_policy)
        for row in report.get("run_results") or []:
            if row.get("backend") != "carla_direct":
                continue
            observed = row.get("direct_stale_world_frame_policy")
            if observed != expected_policy:
                failures.append(
                    "direct_stale_world_frame_policy mismatch for "
                    f"{row.get('run_id')}: expected {expected_policy}, got {observed or 'missing'}"
                )
    if require_direct_transport_contract_aligned:
        for row in report.get("run_results") or []:
            if row.get("backend") != "carla_direct":
                continue
            observed = row.get("direct_transport_contract_status")
            if observed != "aligned":
                failures.append(
                    "direct_transport_contract_status mismatch for "
                    f"{row.get('run_id')}: expected aligned, got {observed or 'missing'}"
                )
    if required_direct_bridge_cadence_ratio_min is not None:
        expected_ratio = float(required_direct_bridge_cadence_ratio_min)
        for comparison in report.get("comparisons") or []:
            candidate = run_results_by_id.get(str(comparison.get("candidate_run_id") or ""))
            if not candidate or candidate.get("backend") != "carla_direct":
                continue
            cadence = comparison.get("cadence_comparison") or {}
            loc_ratio = _num(cadence.get("bridge_loc_hz_ratio"))
            chassis_ratio = _num(cadence.get("bridge_chassis_hz_ratio"))
            if loc_ratio is None or chassis_ratio is None:
                failures.append(
                    "direct_bridge_cadence_ratio missing for "
                    f"{comparison.get('route_id')} duration={comparison.get('duration_s')}"
                )
                continue
            if loc_ratio + 1e-9 < expected_ratio or chassis_ratio + 1e-9 < expected_ratio:
                failures.append(
                    "direct_bridge_cadence_ratio too low for "
                    f"{comparison.get('route_id')} duration={comparison.get('duration_s')}: "
                    f"loc={loc_ratio}, chassis={chassis_ratio}, expected >= {expected_ratio}"
                )
    if require_route_curve_p1_complete:
        curve_rows = [
            row
            for row in (report.get("run_results") or [])
            if _route_requires_curve_artifact_gap(row.get("route_id"))
        ]
        if not curve_rows:
            failures.append("route_curve_artifact_gap missing: no curve diagnostic run rows")
        for row in curve_rows:
            if row.get("route_curve_artifact_gap_status") != "pass" or row.get(
                "route_curve_per_frame_p1_complete"
            ) is not True:
                missing = row.get("route_curve_missing_p1_fields") or []
                suffix = f"; missing_p1={','.join(missing)}" if missing else ""
                failures.append(
                    "route_curve_artifact_gap not pass for "
                    f"{row.get('run_id')}: status={row.get('route_curve_artifact_gap_status') or 'missing'}"
                    f"{suffix}"
                )
    return {
        "passed": not failures,
        "failures": failures,
    }


def write_ab_report_json(path: str | Path, report: Mapping[str, Any]) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")


def write_ab_report_csv(path: str | Path, report: Mapping[str, Any]) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=AB_REPORT_CSV_FIELDS)
        writer.writeheader()
        for row in report.get("run_results") or []:
            payload = dict(row)
            payload["missing_fields"] = ";".join(payload.get("missing_fields") or [])
            payload["active_assists"] = ";".join(payload.get("active_assists") or [])
            payload["blocking_assists"] = ";".join(payload.get("blocking_assists") or [])
            payload["apollo_channel_health_missing_required_channels"] = ";".join(
                payload.get("apollo_channel_health_missing_required_channels") or []
            )
            payload["apollo_channel_health_low_rate_channels"] = ";".join(
                payload.get("apollo_channel_health_low_rate_channels") or []
            )
            payload["route_curve_missing_p1_fields"] = ";".join(
                payload.get("route_curve_missing_p1_fields") or []
            )
            payload["direct_transport_contract_reasons"] = ";".join(
                payload.get("direct_transport_contract_reasons") or []
            )
            payload["control_health_warnings"] = ";".join(payload.get("control_health_warnings") or [])
            payload["apollo_control_handoff_blocking_reasons"] = ";".join(
                payload.get("apollo_control_handoff_blocking_reasons") or []
            )
            payload["steering_normalization_modes"] = ";".join(payload.get("steering_normalization_modes") or [])
            payload["steering_normalization_mode_counts"] = json.dumps(
                payload.get("steering_normalization_mode_counts") or {},
                sort_keys=True,
            )
            writer.writerow({field: payload.get(field) for field in AB_REPORT_CSV_FIELDS})


def _format_modes(row: Mapping[str, Any]) -> str:
    modes = row.get("steering_normalization_modes") or []
    if not modes:
        return ""
    counts = row.get("steering_normalization_mode_counts") or {}
    parts: list[str] = []
    for mode in modes:
        count = counts.get(mode) if isinstance(counts, Mapping) else None
        parts.append(f"{mode}:{count}" if count is not None else str(mode))
    return "; ".join(parts)


def _display(value: Any) -> Any:
    return "" if value is None else value


def render_ab_report_summary(report: Mapping[str, Any]) -> str:
    verdict = report.get("verdict") or {}
    lines = [
        "# A/B Report Summary",
        "",
        f"- batch_id: `{report.get('batch_id')}`",
        f"- source_manifest: `{report.get('source_manifest')}`",
        f"- routes: `{', '.join(report.get('routes') or [])}`",
        f"- durations_s: `{', '.join(str(item) for item in report.get('durations_s') or [])}`",
        f"- backends: `{', '.join(report.get('backends') or [])}`",
        f"- verdict: `{verdict.get('status')}`",
        f"- comparison_counts: `{json.dumps(verdict.get('comparison_counts') or {}, sort_keys=True)}`",
    ]
    hard_gate = verdict.get("hard_gate_summary") or {}
    if hard_gate:
        lines.extend(
            [
                f"- hard_gate_status: `{hard_gate.get('status')}`",
                f"- hard_gate_positive: `{', '.join(hard_gate.get('positive_routes') or [])}`",
                f"- hard_gate_negative: `{', '.join(hard_gate.get('negative_routes') or hard_gate.get('degraded_routes') or [])}`",
                f"- hard_gate_insufficient: `{', '.join(hard_gate.get('insufficient_routes') or [])}`",
                f"- hard_gate_missing: `{', '.join(hard_gate.get('missing_routes') or [])}`",
            ]
        )
    lines.extend(
        [
            "",
            "Candidate positive requires route completion, planning/control/localization evidence, lateral and heading error evidence, failure reason, and artifact completeness. It is not inferred from vehicle motion alone.",
            "",
            "| route_id | duration_s | status | reasons |",
            "| --- | ---: | --- | --- |",
        ]
    )
    for comparison in report.get("comparisons") or []:
        lines.append(
            f"| {comparison.get('route_id')} | {comparison.get('duration_s')} | {comparison.get('status')} | {'; '.join(comparison.get('reasons') or [])} |"
        )
    route_curve_rows = [
        row
        for row in (report.get("run_results") or [])
        if row.get("route_curve_artifact_gap_status") is not None
        or row.get("route_id") in CURVE_DIAGNOSTIC_ROUTE_IDS
    ]
    if route_curve_rows:
        lines.extend(
            [
                "",
                "## Route-Curve Artifact Gap",
                "",
                "Curve diagnostic A/B evidence needs per-frame P1 matched/target/trajectory fields. Summary-level semantics can guide probes, but they do not by themselves prove curve health.",
                "",
                "| run_id | route_id | backend | status | failure_reason | per_frame_p1_complete | missing_p1_fields | source | path |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in route_curve_rows:
            lines.append(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
                    row.get("run_id"),
                    row.get("route_id"),
                    row.get("backend"),
                    row.get("route_curve_artifact_gap_status") or "",
                    row.get("route_curve_artifact_gap_failure_reason") or "",
                    _display(row.get("route_curve_per_frame_p1_complete")),
                    "; ".join(row.get("route_curve_missing_p1_fields") or []),
                    row.get("route_curve_artifact_gap_source") or "",
                    row.get("route_curve_artifact_gap_path") or "",
                )
            )
    cadence_comparisons = [
        comparison
        for comparison in (report.get("comparisons") or [])
        if any(
            (comparison.get("cadence_comparison") or {}).get(key) is not None
            for key in (
                "bridge_loc_hz_ratio",
                "bridge_chassis_hz_ratio",
                "bridge_loc_count_delta",
                "bridge_chassis_count_delta",
            )
        )
    ]
    if cadence_comparisons:
        lines.extend(
            [
                "",
                "## Bridge Cadence Comparison",
                "",
                "Cadence ratios are candidate carla_direct bridge localization/chassis Hz divided by baseline ros2_gt bridge localization/chassis Hz for the same route and duration. They are transport/materialization evidence only; they do not prove curve health, lateral semantics, natural-driving behavior, or full Apollo perception/localization reproduction.",
                "",
                "`loc_hz_ratio` and `chassis_hz_ratio` use the planned manifest duration for the strict gate. Stats-timing Hz fields below use `cyber_bridge_stats.timing.sim_time_sec` when present and are diagnostic context, not a replacement for the strict gate.",
                "",
                "| route_id | duration_s | loc_hz_ratio | chassis_hz_ratio | loc_count_delta | chassis_count_delta |",
                "| --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for comparison in cadence_comparisons:
            cadence = comparison.get("cadence_comparison") or {}
            lines.append(
                f"| {comparison.get('route_id')} | {comparison.get('duration_s')} | {cadence.get('bridge_loc_hz_ratio') or ''} | {cadence.get('bridge_chassis_hz_ratio') or ''} | {cadence.get('bridge_loc_count_delta') if cadence.get('bridge_loc_count_delta') is not None else ''} | {cadence.get('bridge_chassis_count_delta') if cadence.get('bridge_chassis_count_delta') is not None else ''} |"
            )
    timing_rows = [
        row
        for row in (report.get("run_results") or [])
        if row.get("bridge_stats_sim_time_sec") is not None
        or row.get("bridge_cadence_duration_s") is not None
    ]
    if timing_rows:
        lines.extend(
            [
                "",
                "## Bridge Cadence Timing Context",
                "",
                "| run_id | backend | duration_source | duration_s | stats_sim_time_s | loc_hz_manifest | loc_hz_stats | chassis_hz_manifest | chassis_hz_stats | direct_snapshot_hz_stats |",
                "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in timing_rows:
            lines.append(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
                    row.get("run_id"),
                    row.get("backend"),
                    row.get("bridge_cadence_duration_source") or "",
                    row.get("bridge_cadence_duration_s") or "",
                    row.get("bridge_stats_sim_time_sec") or "",
                    row.get("bridge_loc_hz") or "",
                    row.get("bridge_loc_hz_by_stats_sim_time") or "",
                    row.get("bridge_chassis_hz") or "",
                    row.get("bridge_chassis_hz_by_stats_sim_time") or "",
                    row.get("direct_snapshot_hz_by_stats_sim_time") or "",
                )
            )
    control_apply_rows = [
        row
        for row in (report.get("run_results") or [])
        if row.get("control_health_status") is not None
        or row.get("control_bridge_apply_world_frame_hz") is not None
        or row.get("control_apply_observation_delay_s") is not None
    ]
    if control_apply_rows:
        lines.extend(
            [
                "",
                "## Control Apply Diagnostics",
                "",
                (
                    "Control apply diagnostics are artifact evidence from control_health_report.json. "
                    "They help separate Apollo output availability from ROS2 control bridge apply cadence "
                    "and CARLA actuation observation. They do not by themselves prove carla_direct "
                    "promotion or curve health."
                ),
                "",
                (
                    "| run_id | backend | control_health | warnings | apply_delay_s | "
                    "route_delta_after_apply_m | bridge_frame_hz | same_frame_drop_ratio | "
                    "bind_to_first_apply_s | final_rx | final_applied | final_drop_same_frame |"
                ),
                "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in control_apply_rows:
            lines.append(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
                    row.get("run_id"),
                    row.get("backend"),
                    row.get("control_health_status") or "",
                    "; ".join(row.get("control_health_warnings") or []),
                    _display(row.get("control_apply_observation_delay_s")),
                    _display(row.get("route_s_after_first_applied_control_delta_m")),
                    _display(row.get("control_bridge_apply_world_frame_hz")),
                    _display(row.get("control_bridge_same_frame_drop_ratio")),
                    _display(row.get("control_bridge_bind_to_first_apply_s")),
                    _display(row.get("control_bridge_final_rx_count")),
                    _display(row.get("control_bridge_final_applied_count")),
                    _display(row.get("control_bridge_final_drop_same_frame_count")),
                )
            )
    safety_rows = [
        row
        for row in (report.get("run_results") or [])
        if row.get("first_safety_event_type")
    ]
    if safety_rows:
        lines.extend(
            [
                "",
                "## Safety Event Context",
                "",
                "First safety-event context is diagnostic evidence for where a run first failed safety checks. It does not by itself prove a transport or Apollo root cause.",
                "",
                "| run_id | route_id | backend | event | time_s | frame | route_s | cross_track_error_m | heading_error_rad | ego_speed_mps | raw_steer | mapped_steer | applied_steer | control_context | window_context | window_cte_delta_m | window_raw_p95 | window_mapped_p95 | window_applied_p95 | source |",
                "| --- | --- | --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | --- |",
            ]
        )
        for row in safety_rows:
            lines.append(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
                    row.get("run_id"),
                    row.get("route_id"),
                    row.get("backend"),
                    _display(row.get("first_safety_event_type")),
                    _display(row.get("first_safety_event_time_s")),
                    _display(row.get("first_safety_event_frame_id")),
                    _display(row.get("first_safety_event_route_s")),
                    _display(row.get("first_safety_event_cross_track_error_m")),
                    _display(row.get("first_safety_event_heading_error_rad")),
                    _display(row.get("first_safety_event_ego_speed_mps")),
                    _display(row.get("first_safety_event_apollo_steer_raw")),
                    _display(row.get("first_safety_event_bridge_steer_mapped")),
                    _display(row.get("first_safety_event_carla_steer_applied")),
                    _display(row.get("first_safety_event_control_context")),
                    _display(row.get("safety_window_control_context")),
                    _display(row.get("safety_window_cross_track_error_delta_m")),
                    _display(row.get("safety_window_apollo_steer_raw_abs_p95")),
                    _display(row.get("safety_window_bridge_steer_mapped_abs_p95")),
                    _display(row.get("safety_window_carla_steer_applied_abs_p95")),
                    _display(row.get("first_safety_event_source")),
                )
            )
    trace_rows = [row for row in (report.get("run_results") or []) if row.get("steering_normalization_modes")]
    if trace_rows:
        lines.extend(
            [
                "",
                "## Steering Normalization Trace",
                "",
                "| run_id | route_id | backend | modes | trace_path |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for row in trace_rows:
            lines.append(
                f"| {row.get('run_id')} | {row.get('route_id')} | {row.get('backend')} | {_format_modes(row)} | {row.get('steering_normalization_trace_path') or ''} |"
            )
    direct_rows = [
        row
        for row in (report.get("run_results") or [])
        if row.get("backend") == "carla_direct" or row.get("direct_stale_world_frame_policy")
    ]
    if direct_rows:
        lines.extend(
            [
                "",
                "## Direct Transport Cadence",
                "",
                "| run_id | route_id | policy | snapshots | stale_republish | stale_skip |",
                "| --- | --- | --- | ---: | ---: | ---: |",
            ]
        )
        for row in direct_rows:
            lines.append(
                f"| {row.get('run_id')} | {row.get('route_id')} | {row.get('direct_stale_world_frame_policy') or ''} | {row.get('direct_snapshot_count') or ''} | {row.get('direct_stale_world_frame_republish_count') or 0} | {row.get('direct_stale_world_frame_skip_count') or 0} |"
            )
        lines.extend(
            [
                "",
                "| run_id | transport_contract_status | reasons | policy_source |",
                "| --- | --- | --- | --- |",
            ]
        )
        for row in direct_rows:
            lines.append(
                f"| {row.get('run_id')} | {row.get('direct_transport_contract_status') or ''} | {'; '.join(row.get('direct_transport_contract_reasons') or [])} | {row.get('direct_stale_world_frame_policy_source') or ''} |"
            )
    cadence_rows = [
        row
        for row in (report.get("run_results") or [])
        if any(
            row.get(key) is not None
            for key in (
                "bridge_loc_count",
                "bridge_chassis_count",
                "bridge_control_rx_count",
                "bridge_control_tx_count",
                "bridge_loc_hz",
            )
        )
    ]
    if cadence_rows:
        lines.extend(
            [
                "",
                "## Bridge Cadence Counters",
                "",
                "| run_id | backend | loc_count | chassis_count | loc_hz | chassis_hz | control_rx | control_tx | routing_success |",
                "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in cadence_rows:
            lines.append(
                f"| {row.get('run_id')} | {row.get('backend')} | {row.get('bridge_loc_count') or ''} | {row.get('bridge_chassis_count') or ''} | {row.get('bridge_loc_hz') or ''} | {row.get('bridge_chassis_hz') or ''} | {row.get('bridge_control_rx_count') or ''} | {row.get('bridge_control_tx_count') or ''} | {row.get('bridge_routing_success_count') or ''} |"
            )
    lines.append("")
    return "\n".join(lines)


def write_ab_report_summary(path: str | Path, report: Mapping[str, Any]) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_ab_report_summary(report), encoding="utf-8")


def write_ab_report(out_dir: str | Path, report: Mapping[str, Any]) -> dict[str, str]:
    output = Path(out_dir)
    output.mkdir(parents=True, exist_ok=True)
    paths = {
        "ab_report_json": output / "ab_report.json",
        "ab_report_csv": output / "ab_report.csv",
        "ab_report_summary_md": output / "ab_report_summary.md",
    }
    write_ab_report_json(paths["ab_report_json"], report)
    write_ab_report_csv(paths["ab_report_csv"], report)
    write_ab_report_summary(paths["ab_report_summary_md"], report)
    return {key: str(path) for key, path in paths.items()}
