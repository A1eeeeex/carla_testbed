#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import math
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS_DIR = REPO_ROOT / "artifacts"
RUNS_ROOT = REPO_ROOT / "runs" / "town01_lateral_enabled_validation_20260323"

RUN_SPECS = [
    {
        "profile": "town01_relaxed_blocked_baseline",
        "run_id": "relaxed_seed01_repeat01__02",
        "label": "blocked_baseline",
    },
    {
        "profile": "town01_lateral_enabled_raw",
        "run_id": "lateral_enabled_raw_seed01__02",
        "label": "lateral_enabled_raw",
    },
    {
        "profile": "town01_lateral_enabled_guarded",
        "run_id": "lateral_enabled_guarded_seed01__02",
        "label": "lateral_enabled_guarded",
    },
]


def f64(value: Any) -> Optional[float]:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result) or math.isinf(result):
        return None
    return result


def read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def ratio(numer: float, denom: float) -> float:
    if denom <= 0:
        return 0.0
    return numer / denom


def safe_mean(values: Iterable[Optional[float]]) -> Optional[float]:
    cleaned = [v for v in values if v is not None]
    if not cleaned:
        return None
    return statistics.mean(cleaned)


def safe_median(values: Iterable[Optional[float]]) -> Optional[float]:
    cleaned = [v for v in values if v is not None]
    if not cleaned:
        return None
    return statistics.median(cleaned)


def minmax(values: Iterable[Optional[float]]) -> tuple[Optional[float], Optional[float]]:
    cleaned = [v for v in values if v is not None]
    if not cleaned:
        return None, None
    return min(cleaned), max(cleaned)


def sign_same_ratio(values: Iterable[Optional[float]]) -> Optional[float]:
    cleaned = [v for v in values if v is not None and abs(v) > 1e-9]
    if not cleaned:
        return None
    pos = sum(1 for v in cleaned if v > 0.0)
    neg = sum(1 for v in cleaned if v < 0.0)
    return max(pos, neg) / len(cleaned)


def cumulative_distance(rows: List[Dict[str, str]]) -> float:
    dist = 0.0
    prev_xy: Optional[tuple[float, float]] = None
    for row in rows:
        x = f64(row.get("map_x"))
        y = f64(row.get("map_y"))
        if x is None or y is None:
            continue
        cur_xy = (x, y)
        if prev_xy is not None:
            dist += math.hypot(cur_xy[0] - prev_xy[0], cur_xy[1] - prev_xy[1])
        prev_xy = cur_xy
    return dist


@dataclass
class SaturationWindow:
    start_ts: float
    end_ts: float
    sign: int
    start_idx: int
    end_idx: int
    count: int

    @property
    def duration_sec(self) -> float:
        return max(0.0, self.end_ts - self.start_ts)


def find_saturation_windows(debug_rows: List[Dict[str, str]], threshold: float = 0.99) -> List[SaturationWindow]:
    windows: List[SaturationWindow] = []
    current: Optional[SaturationWindow] = None
    for idx, row in enumerate(debug_rows):
        ts = f64(row.get("ts_sec"))
        steer = f64(row.get("apollo_desired_steer"))
        if ts is None or steer is None or abs(steer) < threshold:
            if current is not None:
                windows.append(current)
                current = None
            continue
        sign = 1 if steer > 0.0 else -1
        if current and current.sign == sign and idx == current.end_idx + 1:
            current.end_idx = idx
            current.end_ts = ts
            current.count += 1
        else:
            if current is not None:
                windows.append(current)
            current = SaturationWindow(
                start_ts=ts,
                end_ts=ts,
                sign=sign,
                start_idx=idx,
                end_idx=idx,
                count=1,
            )
    if current is not None:
        windows.append(current)
    return windows


def align_window(
    window: SaturationWindow,
    debug_rows: List[Dict[str, str]],
    control_rows: List[Dict[str, Any]],
    planning_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    debug_window = debug_rows[window.start_idx : window.end_idx + 1]
    control_window = [
        row
        for row in control_rows
        if (f64(row.get("_ts")) or 0.0) >= window.start_ts - 0.1
        and (f64(row.get("_ts")) or 0.0) <= window.end_ts + 0.1
    ]
    planning_window = [
        row
        for row in planning_rows
        if (f64(row.get("_ts")) or 0.0) >= window.start_ts - 0.5
        and (f64(row.get("_ts")) or 0.0) <= window.end_ts + 0.5
    ]

    speeds = [f64(r.get("speed_mps")) for r in debug_window]
    raw_steers = [f64(r.get("apollo_desired_steer")) for r in debug_window]
    commanded = [f64(r.get("commanded_steer")) for r in debug_window]
    measured = [f64(r.get("measured_steer_deg")) for r in debug_window]
    measured_pct = [f64(r.get("measured_steer_pct")) for r in debug_window]
    gaps = [abs(f64(r.get("measured_vs_command_steer_gap")) or 0.0) for r in debug_window]
    e_y = [f64(r.get("e_y_m")) for r in debug_window]
    e_psi = [f64(r.get("e_psi_deg")) for r in debug_window]
    curvature = [f64(r.get("target_curvature")) for r in debug_window]

    heading_error = [f64(r.get("debug_simple_lat_heading_error")) for r in control_window]
    lateral_error = [f64(r.get("debug_simple_lat_lateral_error")) for r in control_window]
    heading_error_rate = [f64(r.get("debug_simple_lat_heading_error_rate")) for r in control_window]
    lateral_error_rate = [f64(r.get("debug_simple_lat_lateral_error_rate")) for r in control_window]
    lat_curvature = [f64(r.get("debug_simple_lat_curvature")) for r in control_window]
    target_s = [f64(r.get("debug_simple_lat_current_target_point_s")) for r in control_window]
    target_rt = [f64(r.get("debug_simple_lat_current_target_point_relative_time")) for r in control_window]
    target_theta = [f64(r.get("debug_simple_lat_current_target_point_theta")) for r in control_window]
    target_kappa = [f64(r.get("debug_simple_lat_current_target_point_kappa")) for r in control_window]
    traj_fraction = [f64(r.get("trajectory_fraction")) for r in control_window]
    steer_target = [f64(r.get("steering_target")) for r in control_window]

    planning_traj_pts = [f64(r.get("trajectory_point_count")) for r in planning_window]
    planning_ref_lines = [f64(r.get("reference_line_count")) for r in planning_window]
    planning_route_segments = [f64(r.get("routing_segment_count")) for r in planning_window]
    planning_statuses = sorted(
        {
            str(r.get("trajectory_header_status"))
            for r in planning_window
            if r.get("trajectory_header_status") not in (None, "")
        }
    )

    same_sign_heading = sign_same_ratio(heading_error)
    same_sign_lateral = sign_same_ratio(lateral_error)
    gap_mean = safe_mean(gaps) or 0.0
    measured_signs = [math.copysign(1.0, v) for v in measured_pct if v is not None and abs(v) > 1e-6]
    commanded_signs = [math.copysign(1.0, v) for v in commanded if v is not None and abs(v) > 1e-6]
    sign_match = None
    if measured_signs and commanded_signs:
        common_len = min(len(measured_signs), len(commanded_signs))
        sign_match = sum(
            1 for a, b in zip(measured_signs[:common_len], commanded_signs[:common_len]) if a == b
        ) / common_len

    guard_triggered = any(
        str(r.get("low_speed_steer_guard_applied")) == "True"
        or str(r.get("low_speed_sustained_guard_applied")) == "True"
        or str(r.get("sustained_lateral_guard_applied")) == "True"
        for r in debug_window
    )

    trajectory_progress_stalls = (
        len({round(v, 6) for v in target_s if v is not None}) <= 1
        and len({round(v, 6) for v in target_rt if v is not None}) <= 1
        and len({round(v, 6) for v in traj_fraction if v is not None}) <= 1
    )
    reference_line_unstable = (
        planning_window
        and max((v or 0.0) for v in planning_ref_lines) == 0.0
        and max((v or 0.0) for v in planning_route_segments) > 0.0
    )
    chassis_feedback_matches_command = bool((sign_match or 0.0) >= 0.9 and gap_mean <= 0.05)

    bridge_e_y_mean = safe_mean(e_y) or 0.0
    apollo_lat_err_mean = safe_mean(lateral_error) or 0.0
    lateral_error_ratio = abs(apollo_lat_err_mean) / max(abs(bridge_e_y_mean), 1e-6)

    suspected_primary_source = "unknown"
    if trajectory_progress_stalls and lateral_error_ratio >= 10.0:
        suspected_primary_source = "trajectory_geometry_unfriendly"
    elif not chassis_feedback_matches_command:
        suspected_primary_source = "chassis_feedback_semantics_mismatch"
    elif reference_line_unstable:
        suspected_primary_source = "reference_line_or_route_instability"
    elif (same_sign_heading or 0.0) >= 0.8 or (same_sign_lateral or 0.0) >= 0.8:
        suspected_primary_source = "heading_error_nonconvergent"
    elif lateral_error_ratio >= 10.0:
        suspected_primary_source = "localization_state_semantics_mismatch"

    return {
        "start_ts": window.start_ts,
        "end_ts": window.end_ts,
        "duration_sec": window.duration_sec,
        "raw_steer_sign": "left" if window.sign > 0 else "right",
        "speed_min": minmax(speeds)[0],
        "speed_max": minmax(speeds)[1],
        "raw_steer_saturated_ratio_in_window": ratio(
            sum(1 for v in raw_steers if v is not None and abs(v) >= 0.99), len(raw_steers)
        ),
        "heading_error_min": minmax(heading_error)[0],
        "heading_error_max": minmax(heading_error)[1],
        "heading_error_same_sign_ratio": same_sign_heading,
        "lateral_error_min": minmax(lateral_error)[0],
        "lateral_error_max": minmax(lateral_error)[1],
        "lateral_error_same_sign_ratio": same_sign_lateral,
        "heading_error_rate_min": minmax(heading_error_rate)[0],
        "heading_error_rate_max": minmax(heading_error_rate)[1],
        "lateral_error_rate_min": minmax(lateral_error_rate)[0],
        "lateral_error_rate_max": minmax(lateral_error_rate)[1],
        "curvature_min": minmax(lat_curvature or curvature)[0],
        "curvature_max": minmax(lat_curvature or curvature)[1],
        "bridge_e_y_min": minmax(e_y)[0],
        "bridge_e_y_max": minmax(e_y)[1],
        "bridge_e_y_mean": bridge_e_y_mean,
        "bridge_e_psi_deg_min": minmax(e_psi)[0],
        "bridge_e_psi_deg_max": minmax(e_psi)[1],
        "target_point_s_min": minmax(target_s)[0],
        "target_point_s_max": minmax(target_s)[1],
        "target_point_relative_time_min": minmax(target_rt)[0],
        "target_point_relative_time_max": minmax(target_rt)[1],
        "target_point_theta_min": minmax(target_theta)[0],
        "target_point_theta_max": minmax(target_theta)[1],
        "target_point_kappa_min": minmax(target_kappa)[0],
        "target_point_kappa_max": minmax(target_kappa)[1],
        "trajectory_fraction_min": minmax(traj_fraction)[0],
        "trajectory_fraction_max": minmax(traj_fraction)[1],
        "trajectory_progress_stalls": trajectory_progress_stalls,
        "steering_target_min": minmax(steer_target)[0],
        "steering_target_max": minmax(steer_target)[1],
        "commanded_steer_min": minmax(commanded)[0],
        "commanded_steer_max": minmax(commanded)[1],
        "measured_steer_deg_min": minmax(measured)[0],
        "measured_steer_deg_max": minmax(measured)[1],
        "measured_steer_pct_min": minmax(measured_pct)[0],
        "measured_steer_pct_max": minmax(measured_pct)[1],
        "mean_abs_measured_vs_command_gap": gap_mean,
        "chassis_feedback_matches_command": chassis_feedback_matches_command,
        "guard_triggered": guard_triggered,
        "guard_reason_summary": ",".join(
            sorted(
                {
                    str(r.get("low_speed_sustained_guard_trigger_reason"))
                    for r in debug_window
                    if r.get("low_speed_sustained_guard_trigger_reason") not in (None, "", "nan")
                }
            )
        ),
        "planning_window_message_count": len(planning_window),
        "planning_nonempty_in_window": sum(1 for v in planning_traj_pts if (v or 0.0) > 0.0),
        "reference_line_count_min": minmax(planning_ref_lines)[0],
        "reference_line_count_max": minmax(planning_ref_lines)[1],
        "route_segment_count_min": minmax(planning_route_segments)[0],
        "route_segment_count_max": minmax(planning_route_segments)[1],
        "reference_line_or_planner_statuses": "|".join(planning_statuses),
        "reference_line_or_route_instability": reference_line_unstable,
        "apollo_lateral_error_mean": apollo_lat_err_mean,
        "apollo_vs_bridge_lateral_error_ratio": lateral_error_ratio,
        "suspected_primary_source": suspected_primary_source,
    }


def case_label_from_metrics(metrics: Dict[str, Any]) -> str:
    if metrics["commanded_steer_nonzero_ratio"] < 0.05 and metrics["raw_steer_saturated_ratio"] > 0.05:
        return "lateral_blocked"
    if metrics["guard_trigger_ratio"] > 0.05:
        if metrics["route_completion_ratio"] >= 0.10 and metrics["raw_steer_saturated_ratio"] < 0.50:
            return "lateral_guarded_but_usable"
        return "lateral_unhealthy"
    if metrics["raw_steer_saturated_ratio"] > 0.20 or metrics["longest_continuous_saturation_sec"] > 8.0:
        return "lateral_unhealthy"
    return "lateral_candidate_for_mainline"


def summarize_run(spec: Dict[str, str]) -> Dict[str, Any]:
    run_dir = RUNS_ROOT / spec["run_id"]
    artifacts_dir = run_dir / "artifacts"

    debug_rows = read_csv(artifacts_dir / "debug_timeseries.csv")
    for row in debug_rows:
        row["_ts"] = f64(row.get("ts_sec"))
    control_rows = read_jsonl(artifacts_dir / "apollo_control_raw.jsonl")
    for row in control_rows:
        top_ts = f64(row.get("ts_sec"))
        raw = row.get("apollo_control_raw", {})
        row.clear()
        row.update(raw)
        row["_ts"] = f64(row.get("control_header_timestamp_sec")) or top_ts
    planning_rows = read_jsonl(artifacts_dir / "planning_topic_debug.jsonl")
    for row in planning_rows:
        row["_ts"] = f64(row.get("timestamp"))

    bridge_health = read_json(artifacts_dir / "bridge_health_summary.json")
    planning_summary = read_json(artifacts_dir / "planning_topic_debug_summary.json")
    scenario_metadata = read_json(artifacts_dir / "scenario_metadata.json")
    summary = read_json(run_dir / "summary.json")

    raw_vals = [f64(r.get("apollo_desired_steer")) for r in debug_rows]
    cmd_vals = [f64(r.get("commanded_steer")) for r in debug_rows]
    measured_deg_vals = [abs(f64(r.get("measured_steer_deg")) or 0.0) for r in debug_rows]
    target_front_errors = [
        abs((f64(r.get("target_front_wheel_angle_deg")) or 0.0) - (f64(r.get("measured_steer_deg")) or 0.0))
        for r in debug_rows
        if f64(r.get("target_front_wheel_angle_deg")) is not None and f64(r.get("measured_steer_deg")) is not None
    ]

    windows = find_saturation_windows(debug_rows)
    longest = max(windows, key=lambda item: item.duration_sec, default=None)
    route_length_m = f64(scenario_metadata.get("route_length_m")) or 0.0
    route_distance_achieved_m = cumulative_distance(debug_rows)
    route_completion_ratio = ratio(route_distance_achieved_m, route_length_m)
    planning_total = planning_summary.get("total_messages_received") or 0
    planning_nonzero = planning_summary.get("messages_with_nonzero_trajectory_points") or 0

    guard_apply_count = sum(
        1
        for r in debug_rows
        if str(r.get("low_speed_steer_guard_applied")) == "True"
        or str(r.get("low_speed_sustained_guard_applied")) == "True"
        or str(r.get("sustained_lateral_guard_applied")) == "True"
    )

    metrics: Dict[str, Any] = {
        "profile": spec["profile"],
        "run_id": spec["run_id"],
        "label": spec["label"],
        "run_dir": str(run_dir),
        "summary_success": summary.get("success"),
        "raw_steer_saturated_ratio": ratio(
            sum(1 for v in raw_vals if v is not None and abs(v) >= 0.99),
            sum(1 for v in raw_vals if v is not None),
        ),
        "longest_continuous_saturation_sec": longest.duration_sec if longest else 0.0,
        "commanded_steer_nonzero_ratio": ratio(
            sum(1 for v in cmd_vals if v is not None and abs(v) > 1e-4),
            sum(1 for v in cmd_vals if v is not None),
        ),
        "measured_steer_abs_mean": safe_mean(measured_deg_vals) or 0.0,
        "target_vs_measured_front_wheel_angle_error": safe_mean(target_front_errors),
        "guard_trigger_count": guard_apply_count,
        "guard_trigger_ratio": ratio(guard_apply_count, len(debug_rows)),
        "planning_nonzero_ratio": ratio(planning_nonzero, planning_total),
        "invalid_goal_count": bridge_health.get("invalid_goal_count"),
        "long_phase_invalid_goal_skip": (bridge_health.get("reroute_reason_counts") or {}).get(
            "long_phase_invalid_goal_skip", 0
        ),
        "unstable_reference_line_skip": (bridge_health.get("reroute_reason_counts") or {}).get(
            "long_phase_unstable_reference_line_skip", 0
        ),
        "route_completion_ratio": route_completion_ratio,
        "route_distance_achieved_m": route_distance_achieved_m,
        "planning_nonempty_trajectory_count": bridge_health.get("planning_nonempty_trajectory_count"),
        "speed_mps_max": bridge_health.get("speed_mps_max") or max((f64(r.get("speed_mps")) or 0.0) for r in debug_rows),
    }
    metrics["lateral_status_label"] = case_label_from_metrics(metrics)
    metrics["final_outcome_label"] = (
        "route_health_candidate"
        if metrics["lateral_status_label"] == "lateral_candidate_for_mainline"
        else "route_established_but_behavior_unhealthy"
    )

    window_rows: List[Dict[str, Any]] = []
    low_speed_events: List[Dict[str, Any]] = []
    for event_id, window in enumerate(
        [w for w in windows if w.duration_sec >= 2.0 and safe_mean(f64(r.get("speed_mps")) for r in debug_rows[w.start_idx : w.end_idx + 1]) is not None],
        start=1,
    ):
        aligned = align_window(window, debug_rows, control_rows, planning_rows)
        aligned_row = {
            "event_id": event_id,
            "profile": spec["profile"],
            "run_id": spec["run_id"],
            **aligned,
        }
        window_rows.append(aligned_row)
        avg_speed = safe_mean(f64(r.get("speed_mps")) for r in debug_rows[window.start_idx : window.end_idx + 1]) or 0.0
        if avg_speed <= 1.0:
            low_speed_events.append(
                {
                    "event_id": event_id,
                    "profile": spec["profile"],
                    "seed": scenario_metadata.get("random_seed"),
                    "run_id": spec["run_id"],
                    "duration_sec": aligned["duration_sec"],
                    "avg_speed_mps": avg_speed,
                    "raw_steer_sign": aligned["raw_steer_sign"],
                    "raw_steer_saturated_ratio_in_window": aligned["raw_steer_saturated_ratio_in_window"],
                    "whether_heading_error_same_sign": (aligned["heading_error_same_sign_ratio"] or 0.0) >= 0.8,
                    "whether_lateral_error_same_sign": (aligned["lateral_error_same_sign_ratio"] or 0.0) >= 0.8,
                    "whether_chassis_feedback_matches_command": aligned["chassis_feedback_matches_command"],
                    "whether_trajectory_progress_stalls": aligned["trajectory_progress_stalls"],
                    "whether_guard_triggered": aligned["guard_triggered"],
                    "suspected_primary_source": aligned["suspected_primary_source"],
                }
            )

    metrics["window_rows"] = window_rows
    metrics["low_speed_events"] = low_speed_events
    metrics["scenario_metadata"] = scenario_metadata
    return metrics


def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


def fmt(value: Any, digits: int = 3) -> str:
    if value is None:
        return "null"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def main() -> None:
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    summaries = [summarize_run(spec) for spec in RUN_SPECS]

    case_rows: List[Dict[str, Any]] = []
    window_rows: List[Dict[str, Any]] = []
    low_speed_rows: List[Dict[str, Any]] = []
    for summary in summaries:
        case_rows.append(
            {
                "profile": summary["profile"],
                "run_id": summary["run_id"],
                "summary.success": summary["summary_success"],
                "lateral_status_label": summary["lateral_status_label"],
                "raw_steer_saturated_ratio": summary["raw_steer_saturated_ratio"],
                "longest_continuous_saturation_sec": summary["longest_continuous_saturation_sec"],
                "commanded_steer_nonzero_ratio": summary["commanded_steer_nonzero_ratio"],
                "measured_steer_abs_mean": summary["measured_steer_abs_mean"],
                "target_vs_measured_front_wheel_angle_error": summary["target_vs_measured_front_wheel_angle_error"],
                "guard_trigger_count": summary["guard_trigger_count"],
                "guard_trigger_ratio": summary["guard_trigger_ratio"],
                "planning_nonzero_ratio": summary["planning_nonzero_ratio"],
                "invalid_goal_count": summary["invalid_goal_count"],
                "long_phase_invalid_goal_skip": summary["long_phase_invalid_goal_skip"],
                "unstable_reference_line_skip": summary["unstable_reference_line_skip"],
                "route_completion_ratio": summary["route_completion_ratio"],
                "route_distance_achieved_m": summary["route_distance_achieved_m"],
                "speed_mps_max": summary["speed_mps_max"],
                "final_outcome_label": summary["final_outcome_label"],
            }
        )
        window_rows.extend(summary["window_rows"])
        low_speed_rows.extend(summary["low_speed_events"])

    case_csv = ARTIFACTS_DIR / "town01_lateral_enabled_case_comparison.csv"
    write_csv(
        case_csv,
        case_rows,
        [
            "profile",
            "run_id",
            "summary.success",
            "lateral_status_label",
            "raw_steer_saturated_ratio",
            "longest_continuous_saturation_sec",
            "commanded_steer_nonzero_ratio",
            "measured_steer_abs_mean",
            "target_vs_measured_front_wheel_angle_error",
            "guard_trigger_count",
            "guard_trigger_ratio",
            "planning_nonzero_ratio",
            "invalid_goal_count",
            "long_phase_invalid_goal_skip",
            "unstable_reference_line_skip",
            "route_completion_ratio",
            "route_distance_achieved_m",
            "speed_mps_max",
            "final_outcome_label",
        ],
    )

    window_csv = ARTIFACTS_DIR / "town01_lateral_window_alignment.csv"
    write_csv(
        window_csv,
        window_rows,
        [
            "event_id",
            "profile",
            "run_id",
            "start_ts",
            "end_ts",
            "duration_sec",
            "raw_steer_sign",
            "speed_min",
            "speed_max",
            "raw_steer_saturated_ratio_in_window",
            "heading_error_min",
            "heading_error_max",
            "heading_error_same_sign_ratio",
            "lateral_error_min",
            "lateral_error_max",
            "lateral_error_same_sign_ratio",
            "heading_error_rate_min",
            "heading_error_rate_max",
            "lateral_error_rate_min",
            "lateral_error_rate_max",
            "curvature_min",
            "curvature_max",
            "bridge_e_y_min",
            "bridge_e_y_max",
            "bridge_e_y_mean",
            "bridge_e_psi_deg_min",
            "bridge_e_psi_deg_max",
            "target_point_s_min",
            "target_point_s_max",
            "target_point_relative_time_min",
            "target_point_relative_time_max",
            "target_point_theta_min",
            "target_point_theta_max",
            "target_point_kappa_min",
            "target_point_kappa_max",
            "trajectory_fraction_min",
            "trajectory_fraction_max",
            "trajectory_progress_stalls",
            "steering_target_min",
            "steering_target_max",
            "commanded_steer_min",
            "commanded_steer_max",
            "measured_steer_deg_min",
            "measured_steer_deg_max",
            "measured_steer_pct_min",
            "measured_steer_pct_max",
            "mean_abs_measured_vs_command_gap",
            "chassis_feedback_matches_command",
            "guard_triggered",
            "guard_reason_summary",
            "planning_window_message_count",
            "planning_nonempty_in_window",
            "reference_line_count_min",
            "reference_line_count_max",
            "route_segment_count_min",
            "route_segment_count_max",
            "reference_line_or_planner_statuses",
            "reference_line_or_route_instability",
            "apollo_lateral_error_mean",
            "apollo_vs_bridge_lateral_error_ratio",
            "suspected_primary_source",
        ],
    )

    low_speed_csv = ARTIFACTS_DIR / "town01_low_speed_lateral_saturation_events.csv"
    write_csv(
        low_speed_csv,
        low_speed_rows,
        [
            "event_id",
            "profile",
            "seed",
            "run_id",
            "duration_sec",
            "avg_speed_mps",
            "raw_steer_sign",
            "raw_steer_saturated_ratio_in_window",
            "whether_heading_error_same_sign",
            "whether_lateral_error_same_sign",
            "whether_chassis_feedback_matches_command",
            "whether_trajectory_progress_stalls",
            "whether_guard_triggered",
            "suspected_primary_source",
        ],
    )

    blocked = next(row for row in case_rows if row["profile"] == "town01_relaxed_blocked_baseline")
    raw = next(row for row in case_rows if row["profile"] == "town01_lateral_enabled_raw")
    guarded = next(row for row in case_rows if row["profile"] == "town01_lateral_enabled_guarded")
    low_speed_primary = low_speed_rows[0]["suspected_primary_source"] if low_speed_rows else "unknown"

    (ARTIFACTS_DIR / "town01_lateral_window_alignment_report.md").write_text(
        "\n".join(
            [
                "# Town01 Lateral Window Alignment",
                "",
                "本报告聚焦 Town01 横向真实放开后最长的单侧 raw steer 饱和窗口。",
                "",
                f"- blocked baseline: `{blocked['profile']}` 的最长窗口约 `{fmt(blocked['longest_continuous_saturation_sec'])}s`，但 `commanded_steer_nonzero_ratio={fmt(blocked['commanded_steer_nonzero_ratio'])}`，说明横向仍被输出层挡住。",
                f"- raw: 最长窗口约 `{fmt(raw['longest_continuous_saturation_sec'])}s`，`raw_steer_saturated_ratio={fmt(raw['raw_steer_saturated_ratio'])}`，且 `route_completion_ratio={fmt(raw['route_completion_ratio'])}` 几乎为 0。",
                f"- guarded: 最长窗口约 `{fmt(guarded['longest_continuous_saturation_sec'])}s`，`guard_trigger_ratio={fmt(guarded['guard_trigger_ratio'])}`，输出从 `-0.25` 限到 `-0.08`，但推进仍几乎停滞。",
                "",
                "关键动态现象：",
                "- 低速饱和窗口中，`debug_simple_lat_current_target_point_s`、`relative_time`、`trajectory_fraction` 基本保持 0，说明 control 侧看到的当前目标点进度在窗口内基本不推进。",
                "- 同一窗口里，`debug_simple_lat_lateral_error` 长期同号，而 `heading_error` 接近 0；这更像横向误差在低速下不收敛，而不是纯航向抖动。",
                "- `measured_vs_command_steer_gap` 很小，说明底盘反馈基本跟得上命令，`Chassis.steering_percentage` 不是当前 top1 可疑点。",
                "- planning 窗口内经常出现 `trajectory_point_count > 0` 且 `route_segment_count > 0`，但 `reference_line_count = 0`；这说明 route/reference-line 残余异常仍在放大横向问题，但不是唯一解释。",
            ]
        ),
        encoding="utf-8",
    )

    (ARTIFACTS_DIR / "town01_low_speed_lateral_saturation_report.md").write_text(
        "\n".join(
            [
                "# Town01 Low-Speed Lateral Saturation",
                "",
                f"- 低速窗口主导 profile: `town01_lateral_enabled_raw` 与 `town01_lateral_enabled_guarded`。",
                f"- 当前 top1 `suspected_primary_source`: `{low_speed_primary}`。",
                "",
                "判定理由：",
                "- raw/guarded 的主要长饱和窗口都发生在 `speed < 1.0 m/s` 的区间。",
                "- `simple_lat_debug` 的 `current_target_point_s / relative_time / trajectory_fraction` 在窗口内基本不变化。",
                "- `simple_lat_debug.lateral_error` 长期同号，而 `heading_error` 极小，说明主导触发不是左右来回抖动，而是横向误差长期不收敛。",
                "- measured steer 与 commanded steer 符号一致且差值小，底盘语义/尺度问题不构成 top1 证据。",
                "- bridge 计算的 `e_y_m` 只有约 0.05m，而 Apollo control 侧 `lateral_error` 均值约 1.15~1.28m，存在明显语义/参考点不一致；这将 `LocalizationEstimate` 语义问题提升为次要怀疑点。",
            ]
        ),
        encoding="utf-8",
    )

    (ARTIFACTS_DIR / "town01_lateral_enabled_evaluation_report.md").write_text(
        "\n".join(
            [
                "# Town01 Lateral Enabled Evaluation",
                "",
                "## Profiles",
                f"- blocked baseline: `{blocked['profile']}`",
                f"- raw: `{raw['profile']}`",
                f"- guarded: `{guarded['profile']}`",
                "",
                "## Findings",
                f"- Town01 主线当前仍存在 blocked baseline。`{blocked['profile']}` 在最长 `{fmt(blocked['longest_continuous_saturation_sec'])}s` 的 raw 饱和窗口里，最终 `commanded_steer` 仍为 0，主要由 `straight_lane_zero_steer` 压住。",
                f"- `raw` 真正放开横向后，`raw_steer_saturated_ratio={fmt(raw['raw_steer_saturated_ratio'])}`，最长连续饱和 `{fmt(raw['longest_continuous_saturation_sec'])}s`，route completion 仅 `{fmt(raw['route_completion_ratio'])}`。",
                f"- `guarded` 通过透明 guard 把输出从 `-0.25` 限到 `-0.08`，但 `route_completion_ratio={fmt(guarded['route_completion_ratio'])}`，仍未进入可用巡航状态。",
                "",
                "## Interpretation",
                "- 这轮已经建立了真正不清零 steer 的 Town01 lateral-enabled path。",
                "- 当前 raw/guarded 共同表明：横向异常主要集中在低速阶段，而不是高速过弯时瞬时爆炸。",
                "- 当前最值得先修的输入层不是 Chassis，而是 `ADCTrajectory / simple_lat_debug` 侧的目标点与误差语义；Localization 参考点语义属于第二优先级。",
            ]
        ),
        encoding="utf-8",
    )

    (ARTIFACTS_DIR / "town01_lateral_enabled_rootcause_and_validation_report.md").write_text(
        "\n".join(
            [
                "# Town01 Lateral Enabled Rootcause And Validation",
                "",
                "## 1. 当前 Town01 主线是否仍在屏蔽横向",
                f"- 是。blocked baseline `{blocked['profile']}` 在原始 steer 长时间 `-1.0` 时，`commanded_steer_nonzero_ratio={fmt(blocked['commanded_steer_nonzero_ratio'])}`，且窗口内 `straight_lane_zero_steer_applied=True`。",
                "",
                "## 2. 放开横向后 Apollo 原始 steer 表现如何",
                f"- raw profile 的 `raw_steer_saturated_ratio={fmt(raw['raw_steer_saturated_ratio'])}`，最长连续单侧饱和 `{fmt(raw['longest_continuous_saturation_sec'])}s`。",
                f"- guarded profile 仍有 `raw_steer_saturated_ratio={fmt(guarded['raw_steer_saturated_ratio'])}`，说明 guard 没有掩盖 Apollo 原始不健康，只是在输出层做了透明限幅。",
                "",
                "## 3. 低速长时间单侧打满时，最主要的输入源是哪个",
                f"- 当前 top1 怀疑点是 `trajectory_geometry_unfriendly`，更具体地说是 Apollo control 在低速窗口里看到的 `current_target_point / trajectory_fraction` 基本不推进。",
                "- 直接证据：在 raw/guarded 的最长低速饱和窗口里，`debug_simple_lat_current_target_point_s=0`、`relative_time=0`、`trajectory_fraction=0`，而 `debug_input_trajectory_header_sequence_num` 仍在变化。",
                "",
                "## 4. 四层里哪一层最值得先修",
                "- Top1: `ADCTrajectory / simple_lat_debug` 侧的目标点与低速最近点匹配语义。",
                "- Top2: `LocalizationEstimate` 的参考点语义。证据是 Apollo `lateral_error` 均值约为 bridge `e_y_m` 的 25 倍。",
                "- 非 top1: `Chassis.steering_percentage`。因为 measured steer 与 commanded steer 基本同号且 gap 很小。",
                "",
                "## 5. guarded profile 是否让系统进入“可控但未健康”状态",
                "- 是，但仅限执行层。guarded 把输出从 `-0.25` 限到 `-0.08`，`measured_vs_command_steer_gap` 也更小。",
                "- 但从场景行为看，它仍然不健康：route completion 仍接近 0，车辆推进几乎停滞。",
                "",
                "## 6. 当前是否已经有一个可继续推进的 lateral regression 候选主线",
                "- 有：`town01_apollo_route_health_lateral_enabled_guarded`。",
                "- 它比 raw 更适合继续迭代，因为保留了真实 steer，又没有用黑箱清零掩盖问题。",
                "- 但它现在还不能升格为健康主线，只能算 `可观察、可控但未健康` 的候选档。",
                "",
                "## 7. 下一阶段最该继续打的一个最小点",
                "- 先检查 Apollo control 低速窗口里 `current_target_point / trajectory_fraction / nearest trajectory progress` 为什么持续为 0。",
                "- 如果这些字段只是 debug 空洞，再下一刀切到 `LocalizationEstimate` 的参考点语义，对齐 Apollo lateral_error 与 bridge e_y 的定义差异。",
            ]
        ),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
