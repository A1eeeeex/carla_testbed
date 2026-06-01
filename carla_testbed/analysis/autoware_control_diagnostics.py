from __future__ import annotations

import csv
import json
import math
import re
from pathlib import Path
from typing import Any, Iterable

import yaml


SCHEMA_VERSION = "autoware_control_diagnostics.v1"
BRIDGE_OVERSPEED_DIAGNOSTIC_THRESHOLD_MPS = 2.0
BRIDGE_OVERSPEED_THROTTLE_FAILURE_ROWS = 10
PLANNING_ACCEPTANCE_SPEED_MARGIN_MPS = 0.1
PLANNING_CONTROL_COMPRESSION_THRESHOLD_MPS = 2.0
KINEMATIC_TWIST_SPEED_MISMATCH_MIN_SPEED_MPS = 1.0
KINEMATIC_TWIST_FORWARD_RATIO_WARN = 0.5
REQUESTED_PLANNING_MAX_GAP_WARN_MPS = 2.0
LANELET_DEFAULT_URBAN_SPEED_MPS = 50.0 / 3.6
LANELET_DEFAULT_URBAN_SPEED_TOLERANCE_MPS = 0.05

VELOCITY_LIMIT_TOPIC_FILES = {
    "max_velocity": "ros2_topic__planning__scenario_planning__max_velocity.jsonl",
    "max_velocity_default": "ros2_topic__planning__scenario_planning__max_velocity_default.jsonl",
    "current_max_velocity": "ros2_topic__planning__scenario_planning__current_max_velocity.jsonl",
    "max_velocity_candidates": "ros2_topic__planning__scenario_planning__max_velocity_candidates.jsonl",
    "velocity_limit": "ros2_topic__planning__scenario_planning__velocity_limit.jsonl",
}


LONGITUDINAL_DIAGNOSTIC_FIELDS = {
    1: "current_velocity_mps",
    2: "target_velocity_mps",
    3: "target_acceleration_mps2",
    4: "nearest_velocity_mps",
    5: "nearest_acceleration_mps2",
    13: "control_state",
    18: "published_acceleration_mps2",
    28: "stop_distance_m",
    36: "smooth_stop_mode",
}

LONGITUDINAL_CONTROL_STATE_NAMES = {
    0: "DRIVE",
    1: "STOPPING",
    2: "STOPPED",
    3: "EMERGENCY",
}

NEAR_FIELD_DISTANCE_BANDS_M = (
    (0.0, 0.5, "0_0p5m"),
    (0.5, 1.0, "0p5_1m"),
    (1.0, 2.0, "1_2m"),
    (2.0, 5.0, "2_5m"),
    (5.0, 10.0, "5_10m"),
)

VELOCITY_THRESHOLDS_MPS = (0.5, 1.0, 2.0)


def _velocity_threshold_key(threshold_mps: float) -> str:
    return f"{threshold_mps:g}".replace(".", "p")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = yaml.safe_load(path.read_text()) or {}
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def _read_jsonl_family(path: Path) -> list[dict[str, Any]]:
    rows = _read_jsonl(path)
    if not path.parent.exists():
        return rows
    stem = path.stem
    for sibling in sorted(path.parent.glob(f"{stem}_*.jsonl")):
        if sibling == path:
            continue
        rows.extend(_read_jsonl(sibling))
    return rows


def _read_text(path: Path) -> str:
    if not path.exists():
        return ""
    try:
        return path.read_text(errors="replace")
    except Exception:
        return ""


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open(newline="") as handle:
            return list(csv.DictReader(handle))
    except Exception:
        return []


def _first_float(row: dict[str, Any], keys: Iterable[str]) -> float | None:
    for key in keys:
        value = row.get(key)
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _nested_get(row: dict[str, Any], keys: Iterable[str]) -> Any:
    cursor: Any = row
    for key in keys:
        if not isinstance(cursor, dict):
            return None
        cursor = cursor.get(key)
    return cursor


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _expected_motion_speed_mps(run_path: Path, summary: dict[str, Any]) -> float | None:
    """Return the configured smoke/natural-driving speed expectation when available."""

    candidates = [
        _nested_get(summary, ("acceptance", "checks", "ego_motion", "threshold")),
        _nested_get(summary, ("acceptance", "min_speed_mps")),
        summary.get("min_speed_mps"),
    ]
    for config_name in ("effective.yaml", "config.resolved.yaml"):
        cfg = _read_yaml(run_path / config_name)
        candidates.extend(
            [
                _nested_get(cfg, ("acceptance", "min_speed_mps")),
                _nested_get(cfg, ("acceptance", "target_cruise_speed_mps")),
            ]
        )
    for candidate in candidates:
        value = _safe_float(candidate)
        if value is not None and value > 0:
            return value
    return None


def _log_timestamp_s(line: str) -> float | None:
    """Extract the ROS/logger timestamp from a launch log line when present."""

    candidates = re.findall(r"\[(-?\d+(?:\.\d+)?)\]", line)
    if not candidates:
        return None
    try:
        return float(candidates[-1])
    except ValueError:
        return None


def _launch_log_signals(artifacts: Path) -> dict[str, Any]:
    text = "\n".join(
        _read_text(artifacts / name)
        for name in ("autoware_launch.stdout", "autoware_launch.stderr", "entrypoint.log")
    )
    patterns = {
        "kinematics_timeout_count": "Subscribed kinematics is timed out",
        "control_cmd_timeout_count": "Subscribed control_cmd is timed out",
        "gear_command_missing_count": "GearCommand is not received yet",
        "operation_mode_change_rejected_count": "The mode change condition is not satisfied",
        "waiting_for_operation_mode_count": "waiting for operation mode info",
        "waiting_for_control_predicted_trajectory_count": "waiting for control predicted trajectory",
        "component_process_died_count": "process has died",
        "guard_condition_failure_count": "failed to add guard condition to wait set",
    }
    signals: dict[str, Any] = {}
    for key, pattern in patterns.items():
        event_times = [
            stamp
            for line in text.splitlines()
            if pattern in line
            for stamp in [_log_timestamp_s(line)]
            if stamp is not None
        ]
        signals[key] = text.count(pattern)
        signals[f"{key}_event_times_s"] = event_times
    behavior_crash_times = [
        stamp
        for line in text.splitlines()
        if "process has died" in line and "behavior_planning_container" in line
        for stamp in [_log_timestamp_s(line)]
        if stamp is not None
    ]
    signals["behavior_planning_container_crash_count"] = len(
        [
            line
            for line in text.splitlines()
            if "process has died" in line and "behavior_planning_container" in line
        ]
    )
    signals["behavior_planning_container_crash_count_event_times_s"] = behavior_crash_times
    signals["has_launch_logs"] = bool(text.strip())
    return signals


def _diagnostic_graph_log_stats(artifacts: Path, first_control_node_stamp: float | None) -> dict[str, Any]:
    text = _read_text(artifacts / "autoware_launch.stdout")
    blocks: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for line in text.splitlines():
        if "[logging_diag_graph]" in line and "The target mode is not available" in line:
            if current:
                blocks.append(current)
            current = {"timestamp_s": _log_timestamp_s(line), "lines": [line]}
            continue
        if current is not None and line.startswith("[logging_node-"):
            current["lines"].append(line)
            continue
        if current is not None and line.strip() and not line.startswith("[logging_node-"):
            blocks.append(current)
            current = None
    if current:
        blocks.append(current)

    before = 0
    after = 0
    leaf_counts: dict[str, int] = {}
    for block in blocks:
        timestamp = block.get("timestamp_s")
        if first_control_node_stamp is not None and timestamp is not None and timestamp >= first_control_node_stamp:
            after += 1
        else:
            before += 1
        for line in block.get("lines") or []:
            match = re.search(r"- (/[A-Za-z0-9_./-]+) (ERROR|STALE|WARN)", line)
            if match:
                key = f"{match.group(1)}:{match.group(2)}"
                leaf_counts[key] = leaf_counts.get(key, 0) + 1

    return {
        "has_log": bool(text.strip()),
        "not_available_block_count": len(blocks),
        "not_available_before_first_control_count": before,
        "not_available_after_first_control_count": after,
        "available_now_count": text.count("The target mode is available now"),
        "top_leaf_failures": [
            {"name": name, "count": count}
            for name, count in sorted(leaf_counts.items(), key=lambda item: (-item[1], item[0]))[:20]
        ],
    }


def _numbers(values: Iterable[Any]) -> list[float]:
    out: list[float] = []
    for value in values:
        if value is None:
            continue
        try:
            out.append(float(value))
        except (TypeError, ValueError):
            continue
    return out


def _series_stats(values: Iterable[Any]) -> dict[str, float | None]:
    nums = _numbers(values)
    if not nums:
        return {"min": None, "max": None, "mean": None, "last": None}
    return {
        "min": min(nums),
        "max": max(nums),
        "mean": sum(nums) / len(nums),
        "last": nums[-1],
    }


def _stamp_seconds(row: dict[str, Any]) -> float | None:
    stamp = row.get("stamp") or (row.get("header") or {}).get("stamp")
    if not isinstance(stamp, dict) or "sec" not in stamp:
        return None
    try:
        return float(stamp.get("sec", 0)) + float(stamp.get("nanosec", 0)) * 1e-9
    except (TypeError, ValueError):
        return None


def _stamp_domain(stamp_max: float | None) -> str | None:
    if stamp_max is None:
        return None
    # Sim time for CARLA smoke runs is normally small; wall stamps are Unix-like.
    if stamp_max > 1_000_000_000:
        return "wall_time"
    return "sim_time"


def _stamp_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    stamps = _numbers(_stamp_seconds(row) for row in rows)
    node_stamps = _numbers(row.get("_node_stamp") for row in rows)
    stamp_gaps = [b - a for a, b in zip(stamps, stamps[1:])]
    node_stamp_gaps = [b - a for a, b in zip(node_stamps, node_stamps[1:])]
    stats = _series_stats(stamps)
    node_stats = _series_stats(node_stamps)
    return {
        "message_count": len(rows),
        "stamp": stats,
        "node_stamp": node_stats,
        "stamp_max_gap_s": max(stamp_gaps) if stamp_gaps else None,
        "node_stamp_max_gap_s": max(node_stamp_gaps) if node_stamp_gaps else None,
        "stamp_domain": _stamp_domain(stats["max"]),
    }


def _classify_launch_events_relative_to_first_control(
    launch_log_signals: dict[str, Any],
    control_rows: list[dict[str, Any]],
) -> None:
    node_stamps = _numbers(row.get("_node_stamp") for row in control_rows)
    first_control_node_stamp = min(node_stamps) if node_stamps else None
    launch_log_signals["first_control_node_stamp_s"] = first_control_node_stamp
    if first_control_node_stamp is None:
        return

    for key in (
        "control_cmd_timeout_count",
        "kinematics_timeout_count",
        "gear_command_missing_count",
    ):
        event_times = _numbers(launch_log_signals.get(f"{key}_event_times_s") or [])
        before = [stamp for stamp in event_times if stamp < first_control_node_stamp]
        after = [stamp for stamp in event_times if stamp >= first_control_node_stamp]
        launch_log_signals[f"{key}_before_first_control_count"] = len(before)
        launch_log_signals[f"{key}_after_first_control_count"] = len(after)


def _nearest_by_stamp(rows: list[dict[str, Any]], stamp_s: float | None) -> dict[str, Any] | None:
    if stamp_s is None or not rows:
        return None
    best: dict[str, Any] | None = None
    best_dt: float | None = None
    for row in rows:
        row_stamp = _stamp_seconds(row)
        if row_stamp is None:
            continue
        dt = abs(row_stamp - stamp_s)
        if best_dt is None or dt < best_dt:
            best = row
            best_dt = dt
    return best


def _xy_from_pose(row: dict[str, Any] | None) -> tuple[float, float] | None:
    if not row:
        return None
    pose = row.get("pose") or {}
    try:
        return float(pose["x"]), float(pose["y"])
    except (KeyError, TypeError, ValueError):
        return None


def _speed_from_kinematic(row: dict[str, Any] | None) -> float | None:
    if not row:
        return None
    twist = (row.get("twist") or {}).get("linear") or {}
    try:
        x = float(twist.get("x", 0.0) or 0.0)
        y = float(twist.get("y", 0.0) or 0.0)
        z = float(twist.get("z", 0.0) or 0.0)
    except (TypeError, ValueError):
        return None
    return math.sqrt(x * x + y * y + z * z)


def _kinematic_twist_contract_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    linear_x: list[float] = []
    linear_y: list[float] = []
    linear_z: list[float] = []
    speeds: list[float] = []
    for row in rows:
        twist = (row.get("twist") or {}).get("linear") or {}
        try:
            x = float(twist.get("x", 0.0) or 0.0)
            y = float(twist.get("y", 0.0) or 0.0)
            z = float(twist.get("z", 0.0) or 0.0)
        except (TypeError, ValueError):
            continue
        linear_x.append(x)
        linear_y.append(y)
        linear_z.append(z)
        speeds.append(math.sqrt(x * x + y * y + z * z))
    abs_x_max = max((abs(value) for value in linear_x), default=None)
    abs_y_max = max((abs(value) for value in linear_y), default=None)
    speed_max = max(speeds, default=None)
    forward_ratio = (
        abs_x_max / speed_max if abs_x_max is not None and speed_max and speed_max > 1e-6 else None
    )
    return {
        "message_count": len(rows),
        "linear_x_mps": _series_stats(linear_x),
        "linear_y_mps": _series_stats(linear_y),
        "linear_z_mps": _series_stats(linear_z),
        "speed_norm_mps": _series_stats(speeds),
        "abs_linear_x_max_mps": abs_x_max,
        "abs_linear_y_max_mps": abs_y_max,
        "forward_component_ratio_at_max_speed": forward_ratio,
    }


def _vehicle_speed_response(
    control_rows: list[dict[str, Any]],
    kinematic_rows: list[dict[str, Any]],
    *,
    positive_accel_threshold_mps2: float = 0.05,
) -> dict[str, Any]:
    speed_rows: list[dict[str, float]] = []
    for row in kinematic_rows:
        stamp_s = _stamp_seconds(row)
        speed = _speed_from_kinematic(row)
        if stamp_s is None or speed is None:
            continue
        speed_rows.append({"stamp_s": stamp_s, "speed_mps": speed})
    speed_rows.sort(key=lambda row: row["stamp_s"])

    accel_rows: list[dict[str, float]] = []
    for prev, cur in zip(speed_rows, speed_rows[1:]):
        dt = cur["stamp_s"] - prev["stamp_s"]
        if dt <= 1e-6:
            continue
        accel_rows.append(
            {
                "stamp_s": cur["stamp_s"],
                "acceleration_mps2": (cur["speed_mps"] - prev["speed_mps"]) / dt,
                "speed_mps": cur["speed_mps"],
            }
        )

    positive_pairs: list[dict[str, float | None]] = []
    for control in control_rows:
        stamp_s = _stamp_seconds(control)
        if stamp_s is None:
            continue
        commanded_accel = (control.get("longitudinal") or {}).get("acceleration")
        try:
            commanded_accel_f = float(commanded_accel)
        except (TypeError, ValueError):
            continue
        if commanded_accel_f < positive_accel_threshold_mps2:
            continue
        nearest_accel = _nearest_by_value(accel_rows, stamp_s, key="stamp_s")
        positive_pairs.append(
            {
                "stamp_s": stamp_s,
                "commanded_acceleration_mps2": commanded_accel_f,
                "actual_acceleration_mps2": (
                    None if nearest_accel is None else nearest_accel.get("acceleration_mps2")
                ),
                "actual_speed_mps": None if nearest_accel is None else nearest_accel.get("speed_mps"),
            }
        )

    commanded_positive = _numbers(row.get("commanded_acceleration_mps2") for row in positive_pairs)
    actual_when_positive = _numbers(row.get("actual_acceleration_mps2") for row in positive_pairs)
    commanded_mean = sum(commanded_positive) / len(commanded_positive) if commanded_positive else None
    actual_mean = sum(actual_when_positive) / len(actual_when_positive) if actual_when_positive else None
    ratio = (
        actual_mean / commanded_mean
        if commanded_mean is not None and abs(commanded_mean) > 1e-6 and actual_mean is not None
        else None
    )
    sample_indices = sorted({0, len(positive_pairs) // 2, len(positive_pairs) - 1}) if positive_pairs else []
    return {
        "kinematic_speed_mps": _series_stats(row.get("speed_mps") for row in speed_rows),
        "actual_acceleration_mps2": _series_stats(row.get("acceleration_mps2") for row in accel_rows),
        "positive_command_count": len(positive_pairs),
        "commanded_positive_acceleration_mps2": _series_stats(commanded_positive),
        "actual_acceleration_when_positive_command_mps2": _series_stats(actual_when_positive),
        "actual_over_commanded_positive_acceleration_ratio": ratio,
        "samples": [positive_pairs[idx] for idx in sample_indices],
    }


def _nearest_by_value(
    rows: list[dict[str, Any]],
    value: float | None,
    *,
    key: str,
) -> dict[str, Any] | None:
    if value is None or not rows:
        return None
    best: dict[str, Any] | None = None
    best_delta: float | None = None
    for row in rows:
        try:
            delta = abs(float(row[key]) - float(value))
        except (KeyError, TypeError, ValueError):
            continue
        if best_delta is None or delta < best_delta:
            best = row
            best_delta = delta
    return best


def _trajectory_sample_alignment(
    trajectory_rows: list[dict[str, Any]],
    control_rows: list[dict[str, Any]],
    kinematic_rows: list[dict[str, Any]],
    *,
    high_velocity_threshold_mps: float = 1.0,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    empty_bands = {
        label: {
            "max_velocity_mps": _series_stats([]),
            "mean_velocity_mps": _series_stats([]),
            "sample_count": _series_stats([]),
        }
        for _, _, label in NEAR_FIELD_DISTANCE_BANDS_M
    }
    empty_thresholds = {
        _velocity_threshold_key(threshold): {
            "threshold_mps": threshold,
            "distance_m": _series_stats([]),
            "index": _series_stats([]),
            "velocity_mps": _series_stats([]),
        }
        for threshold in VELOCITY_THRESHOLDS_MPS
    }
    if not trajectory_rows or not control_rows or not kinematic_rows:
        return {
            "sample_count": 0,
            "nearest_sample_velocity_mps": _series_stats([]),
            "nearest_sample_index": _series_stats([]),
            "nearest_sample_distance_m": _series_stats([]),
            "control_target_minus_nearest_sample_velocity_mps": _series_stats([]),
            "control_target_minus_first_high_velocity_sample_mps": _series_stats([]),
            "near_field_velocity_profile": empty_bands,
            "first_sample_ge_velocity_threshold": empty_thresholds,
            "samples": [],
        }

    for control in control_rows:
        stamp_s = _stamp_seconds(control)
        trajectory = _nearest_by_stamp(trajectory_rows, stamp_s)
        kinematic = _nearest_by_stamp(kinematic_rows, stamp_s)
        ego_xy = _xy_from_pose(kinematic)
        points = (trajectory or {}).get("trajectory_points_sample") or []
        if ego_xy is None or not points:
            continue
        nearest = None
        nearest_dist = None
        band_velocities: dict[str, list[float]] = {
            label: [] for _, _, label in NEAR_FIELD_DISTANCE_BANDS_M
        }
        threshold_hits: dict[str, dict[str, Any] | None] = {
            _velocity_threshold_key(threshold): None for threshold in VELOCITY_THRESHOLDS_MPS
        }
        for point in points:
            point_xy = _xy_from_pose(point)
            if point_xy is None:
                continue
            dist = math.hypot(point_xy[0] - ego_xy[0], point_xy[1] - ego_xy[1])
            if nearest_dist is None or dist < nearest_dist:
                nearest = point
                nearest_dist = dist
            velocity = point.get("longitudinal_velocity_mps")
            try:
                velocity_f = float(velocity)
            except (TypeError, ValueError):
                velocity_f = None
            if velocity_f is None:
                continue
            for band_start_m, band_end_m, band_label in NEAR_FIELD_DISTANCE_BANDS_M:
                if band_start_m <= dist < band_end_m:
                    band_velocities[band_label].append(velocity_f)
            for threshold in VELOCITY_THRESHOLDS_MPS:
                if velocity_f < threshold:
                    continue
                threshold_key = _velocity_threshold_key(threshold)
                current = threshold_hits.get(threshold_key)
                if current is None or dist < float(current.get("distance_m", float("inf"))):
                    threshold_hits[threshold_key] = {
                        "index": point.get("index"),
                        "distance_m": dist,
                        "velocity_mps": velocity_f,
                    }
        if nearest is None:
            continue
        target_velocity = (control.get("longitudinal") or {}).get("velocity")
        nearest_velocity = nearest.get("longitudinal_velocity_mps")
        try:
            target_minus_nearest = float(target_velocity) - float(nearest_velocity)
        except (TypeError, ValueError):
            target_minus_nearest = None
        first_high = threshold_hits[_velocity_threshold_key(high_velocity_threshold_mps)]
        try:
            target_minus_first_high = float(target_velocity) - float(
                (first_high or {}).get("velocity_mps")
            )
        except (TypeError, ValueError):
            target_minus_first_high = None
        near_field_profile = {
            label: {
                "sample_count": len(values),
                "max_velocity_mps": max(values) if values else None,
                "mean_velocity_mps": (sum(values) / len(values)) if values else None,
            }
            for label, values in band_velocities.items()
        }
        rows.append(
            {
                "stamp_s": stamp_s,
                "ego_x": ego_xy[0],
                "ego_y": ego_xy[1],
                "control_target_velocity_mps": target_velocity,
                "nearest_sample_index": nearest.get("index"),
                "nearest_sample_distance_m": nearest_dist,
                "nearest_sample_velocity_mps": nearest_velocity,
                "control_target_minus_nearest_sample_velocity_mps": target_minus_nearest,
                "control_target_minus_first_high_velocity_sample_mps": target_minus_first_high,
                "first_sample_ge_threshold_index": (first_high or {}).get("index"),
                "first_sample_ge_threshold_distance_m": (first_high or {}).get("distance_m"),
                "first_sample_ge_threshold_velocity_mps": (first_high or {}).get("velocity_mps"),
                "near_field_velocity_profile": near_field_profile,
                "first_sample_ge_velocity_threshold": threshold_hits,
            }
        )

    summary_rows = rows
    sample_indices = []
    if summary_rows:
        sample_indices = sorted({0, len(summary_rows) // 2, len(summary_rows) - 1})
    near_field_summary = {}
    for _, _, label in NEAR_FIELD_DISTANCE_BANDS_M:
        near_field_summary[label] = {
            "max_velocity_mps": _series_stats(
                ((row.get("near_field_velocity_profile") or {}).get(label) or {}).get(
                    "max_velocity_mps"
                )
                for row in summary_rows
            ),
            "mean_velocity_mps": _series_stats(
                ((row.get("near_field_velocity_profile") or {}).get(label) or {}).get(
                    "mean_velocity_mps"
                )
                for row in summary_rows
            ),
            "sample_count": _series_stats(
                ((row.get("near_field_velocity_profile") or {}).get(label) or {}).get(
                    "sample_count"
                )
                for row in summary_rows
            ),
        }
    threshold_summary = {}
    for threshold in VELOCITY_THRESHOLDS_MPS:
        key = _velocity_threshold_key(threshold)
        threshold_summary[key] = {
            "threshold_mps": threshold,
            "distance_m": _series_stats(
                ((row.get("first_sample_ge_velocity_threshold") or {}).get(key) or {}).get(
                    "distance_m"
                )
                for row in summary_rows
            ),
            "index": _series_stats(
                ((row.get("first_sample_ge_velocity_threshold") or {}).get(key) or {}).get(
                    "index"
                )
                for row in summary_rows
            ),
            "velocity_mps": _series_stats(
                ((row.get("first_sample_ge_velocity_threshold") or {}).get(key) or {}).get(
                    "velocity_mps"
                )
                for row in summary_rows
            ),
        }
    return {
        "sample_count": len(summary_rows),
        "nearest_sample_velocity_mps": _series_stats(
            row.get("nearest_sample_velocity_mps") for row in summary_rows
        ),
        "nearest_sample_index": _series_stats(row.get("nearest_sample_index") for row in summary_rows),
        "nearest_sample_distance_m": _series_stats(
            row.get("nearest_sample_distance_m") for row in summary_rows
        ),
        "control_target_minus_nearest_sample_velocity_mps": _series_stats(
            row.get("control_target_minus_nearest_sample_velocity_mps") for row in summary_rows
        ),
        "control_target_minus_first_high_velocity_sample_mps": _series_stats(
            row.get("control_target_minus_first_high_velocity_sample_mps") for row in summary_rows
        ),
        "first_sample_ge_threshold_distance_m": _series_stats(
            row.get("first_sample_ge_threshold_distance_m") for row in summary_rows
        ),
        "near_field_velocity_profile": near_field_summary,
        "first_sample_ge_velocity_threshold": threshold_summary,
        "samples": [summary_rows[idx] for idx in sample_indices],
    }


def _trajectory_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    point_counts = _numbers(row.get("trajectory_point_count") for row in rows)
    max_velocities = _numbers((row.get("trajectory_velocity_mps") or {}).get("max") for row in rows)
    first_velocities = _numbers((row.get("trajectory_velocity_mps") or {}).get("first") for row in rows)
    mean_velocities = _numbers((row.get("trajectory_velocity_mps") or {}).get("mean") for row in rows)
    first_nonzero = _numbers(row.get("first_nonzero_velocity_index") for row in rows)
    return {
        "message_count": len(rows),
        "point_count": _series_stats(point_counts),
        "velocity_max_mps": _series_stats(max_velocities),
        "velocity_first_mps": _series_stats(first_velocities),
        "velocity_mean_mps": _series_stats(mean_velocities),
        "first_nonzero_velocity_index": _series_stats(first_nonzero),
    }


def _control_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    velocities = []
    accelerations = []
    steer = []
    for row in rows:
        longitudinal = row.get("longitudinal") or {}
        lateral = row.get("lateral") or {}
        velocities.append(longitudinal.get("velocity", longitudinal.get("speed")))
        accelerations.append(longitudinal.get("acceleration"))
        steer.append(lateral.get("steering_tire_angle"))
    return {
        "message_count": len(rows),
        "target_velocity_mps": _series_stats(velocities),
        "target_acceleration_mps2": _series_stats(accelerations),
        "steering_tire_angle_rad": _series_stats(steer),
    }


def _topic_capture_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    stats: dict[str, Any] = {
        "message_count": len(rows),
        "stamp": _stamp_stats(rows),
    }
    if rows and any("trajectory_point_count" in row for row in rows):
        stats["trajectory"] = _trajectory_stats(rows)
    return stats


def _first_nested_float(row: dict[str, Any], paths: Iterable[Iterable[str]]) -> float | None:
    for path in paths:
        value = _safe_float(_nested_get(row, path))
        if value is not None:
            return value
    return None


def _velocity_limit_value(row: dict[str, Any]) -> float | None:
    value = _first_float(
        row,
        (
            "max_velocity",
            "max_velocity_mps",
            "velocity",
            "velocity_mps",
            "limit_velocity",
            "speed_limit",
            "current_max_velocity",
        ),
    )
    if value is not None:
        return value
    value = _first_nested_float(
        row,
        (
            ("velocity_limit", "max_velocity"),
            ("velocity_limit", "max_velocity_mps"),
            ("velocity_limit", "velocity"),
            ("limit", "max_velocity"),
            ("limit", "velocity"),
        ),
    )
    if value is not None:
        return value
    data = row.get("data")
    if isinstance(data, (int, float, str)):
        return _safe_float(data)
    if isinstance(data, list) and data:
        return _safe_float(data[0])
    return None


def _velocity_limit_candidate_values(row: dict[str, Any]) -> list[float]:
    candidates: list[float] = []
    for key in ("candidates", "velocity_limits", "limits"):
        values = row.get(key)
        if not isinstance(values, list):
            continue
        for item in values:
            if isinstance(item, dict):
                value = _velocity_limit_value(item)
            else:
                value = _safe_float(item)
            if value is not None:
                candidates.append(value)
    return candidates


def _velocity_limit_topic_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    values = [_velocity_limit_value(row) for row in rows]
    candidate_values = [
        value for row in rows for value in _velocity_limit_candidate_values(row)
    ]
    return {
        "message_count": len(rows),
        "stamp": _stamp_stats(rows),
        "max_velocity_mps": _series_stats(values),
        "candidate_count": _series_stats(len(_velocity_limit_candidate_values(row)) for row in rows),
        "candidate_velocity_mps": _series_stats(candidate_values),
    }


def _controller_debug_alignment(
    debug_rows_by_name: dict[str, list[dict[str, Any]]],
    control_rows: list[dict[str, Any]],
    kinematic_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        name: _trajectory_sample_alignment(rows, control_rows, kinematic_rows)
        for name, rows in debug_rows_by_name.items()
    }


def _counter(values: Iterable[Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        if value is None:
            continue
        key = str(value)
        counts[key] = counts.get(key, 0) + 1
    return counts


def _longitudinal_diagnostic_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    values_by_name: dict[str, list[float]] = {
        name: [] for name in LONGITUDINAL_DIAGNOSTIC_FIELDS.values()
    }
    samples: list[dict[str, Any]] = []
    for row in rows:
        data = row.get("data")
        if not isinstance(data, list):
            continue
        sample: dict[str, Any] = {"stamp_s": _stamp_seconds(row)}
        for index, name in LONGITUDINAL_DIAGNOSTIC_FIELDS.items():
            if index >= len(data):
                continue
            try:
                value = float(data[index])
            except (TypeError, ValueError):
                continue
            values_by_name[name].append(value)
            sample[name] = value
        if sample:
            state = sample.get("control_state")
            if state is not None:
                sample["control_state_name"] = LONGITUDINAL_CONTROL_STATE_NAMES.get(
                    int(round(float(state))), "UNKNOWN"
                )
            samples.append(sample)

    sample_indices = sorted({0, len(samples) // 2, len(samples) - 1}) if samples else []
    state_names = [
        LONGITUDINAL_CONTROL_STATE_NAMES.get(int(round(value)), "UNKNOWN")
        for value in values_by_name.get("control_state", [])
    ]
    return {
        "message_count": len(rows),
        "stamp": _stamp_stats(rows),
        "fields": {
            name: _series_stats(values)
            for name, values in values_by_name.items()
            if values or name in LONGITUDINAL_DIAGNOSTIC_FIELDS.values()
        },
        "control_state_counts": _counter(state_names),
        "samples": [samples[idx] for idx in sample_indices],
    }


def _operation_mode_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "message_count": 0,
            "last_mode": None,
            "autoware_control_enabled_any": None,
            "autoware_control_enabled_last": None,
            "autonomous_available_any": None,
            "autonomous_available_last": None,
            "autonomous_available_true_count": 0,
            "autonomous_available_false_count": 0,
            "autonomous_available_transition_count": 0,
            "autonomous_available_ratio": None,
        }
    autonomous_flags = [row.get("is_autonomous_mode_available") for row in rows]
    control_flags = [row.get("is_autoware_control_enabled") for row in rows]
    bool_autonomous_flags = [bool(v) for v in autonomous_flags]
    true_count = sum(1 for v in bool_autonomous_flags if v)
    false_count = sum(1 for v in bool_autonomous_flags if not v)
    transitions = sum(
        1 for prev, cur in zip(bool_autonomous_flags, bool_autonomous_flags[1:]) if prev != cur
    )
    return {
        "message_count": len(rows),
        "last_mode": rows[-1].get("mode"),
        "autoware_control_enabled_any": any(bool(v) for v in control_flags),
        "autoware_control_enabled_last": bool(control_flags[-1]) if control_flags else None,
        "autonomous_available_any": any(bool(v) for v in autonomous_flags),
        "autonomous_available_last": bool(autonomous_flags[-1]) if autonomous_flags else None,
        "autonomous_available_true_count": true_count,
        "autonomous_available_false_count": false_count,
        "autonomous_available_transition_count": transitions,
        "autonomous_available_ratio": true_count / len(bool_autonomous_flags),
    }


def _diagnostics_topic_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    status_rows: list[dict[str, Any]] = []
    for row in rows:
        for status in row.get("status") or []:
            if isinstance(status, dict):
                status_rows.append(status)

    counts_by_level: dict[str, int] = {}
    failure_counts: dict[str, int] = {}
    for status in status_rows:
        raw_level = status.get("level")
        if isinstance(raw_level, str) and len(raw_level) == 1:
            level = ord(raw_level)
        else:
            try:
                level = int(raw_level)
            except (TypeError, ValueError):
                level = -1
        level_name = {
            0: "ok",
            1: "warn",
            2: "error",
            3: "stale",
        }.get(level, str(level))
        counts_by_level[level_name] = counts_by_level.get(level_name, 0) + 1
        if level > 0:
            name = str(status.get("name") or "<unnamed>")
            message = str(status.get("message") or "")
            key = f"{name}:{level_name}:{message}"
            failure_counts[key] = failure_counts.get(key, 0) + 1

    return {
        "message_count": len(rows),
        "status_detail_count": len(status_rows),
        "has_status_details": bool(status_rows),
        "counts_by_level": counts_by_level,
        "top_failures": [
            {"name": name, "count": count}
            for name, count in sorted(failure_counts.items(), key=lambda item: (-item[1], item[0]))[:20]
        ],
    }


def _bridge_log_stats(text: str, first_control_node_stamp: float | None = None) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for line in text.splitlines():
        if "apply frame=" not in line:
            continue
        row: dict[str, Any] = {}
        stamp = _log_timestamp_s(line)
        if stamp is not None:
            row["node_stamp"] = stamp
        for key in ("target_speed", "accel", "current_speed", "throttle", "brake"):
            match = re.search(rf"(?:^|\s){key}=(-?\d+(?:\.\d+)?|null)", line)
            if match and match.group(1) != "null":
                row[key] = float(match.group(1))
        source_match = re.search(r"speed_source=([^ ]+)", line)
        if source_match:
            row["speed_source"] = source_match.group(1)
        control_source_match = re.search(r"source=([^ ]+)", line)
        if control_source_match:
            row["control_source"] = control_source_match.group(1)
        mode_match = re.search(r"longitudinal_mode=([^ ]+)", line)
        if mode_match:
            row["longitudinal_mode"] = mode_match.group(1)
        if row:
            target_speed = row.get("target_speed")
            current_speed = row.get("current_speed")
            if target_speed is not None and current_speed is not None:
                row["current_minus_target_speed"] = float(current_speed) - float(target_speed)
            rows.append(row)

    states: list[str] = []
    for row in rows:
        throttle = float(row.get("throttle") or 0.0)
        brake = float(row.get("brake") or 0.0)
        if throttle > 0.01 and brake > 0.01:
            states.append("conflict")
        elif throttle > 0.01:
            states.append("throttle")
        elif brake > 0.01:
            states.append("brake")
        else:
            states.append("coast")
    compact_states = [state for state in states if state in {"throttle", "brake"}]
    throttle_brake_switch_count = sum(
        1 for prev, cur in zip(compact_states, compact_states[1:]) if prev != cur
    )
    source_counts: dict[str, int] = {}
    for row in rows:
        source = str(row.get("control_source") or "unknown")
        source_counts[source] = source_counts.get(source, 0) + 1
    watchdog_before_first_control = 0
    watchdog_after_first_control = 0
    for row in rows:
        if row.get("control_source") != "watchdog":
            continue
        stamp = row.get("node_stamp")
        if first_control_node_stamp is not None and stamp is not None and stamp >= first_control_node_stamp:
            watchdog_after_first_control += 1
        else:
            watchdog_before_first_control += 1
    overspeed_threshold = BRIDGE_OVERSPEED_DIAGNOSTIC_THRESHOLD_MPS
    overspeed_rows = [
        row
        for row in rows
        if row.get("current_minus_target_speed") is not None
        and float(row["current_minus_target_speed"]) > overspeed_threshold
    ]
    throttle_while_overspeed_rows = [
        row for row in overspeed_rows if float(row.get("throttle") or 0.0) > 0.01
    ]
    coast_while_overspeed_rows = [
        row
        for row in overspeed_rows
        if float(row.get("throttle") or 0.0) <= 0.01 and float(row.get("brake") or 0.0) <= 0.01
    ]
    positive_accel_while_overspeed_rows = [
        row for row in overspeed_rows if float(row.get("accel") or 0.0) > 0.05
    ]
    return {
        "has_log": bool(text.strip()),
        "apply_log_rows": len(rows),
        "source_counts": source_counts,
        "pending_rows": source_counts.get("pending", 0),
        "watchdog_rows": source_counts.get("watchdog", 0),
        "watchdog_before_first_control_count": watchdog_before_first_control,
        "watchdog_after_first_control_count": watchdog_after_first_control,
        "target_speed_mps": _series_stats(row.get("target_speed") for row in rows),
        "accel_mps2": _series_stats(row.get("accel") for row in rows),
        "current_speed_mps": _series_stats(row.get("current_speed") for row in rows),
        "current_minus_target_speed_mps": _series_stats(
            row.get("current_minus_target_speed") for row in rows
        ),
        "throttle": _series_stats(row.get("throttle") for row in rows),
        "brake": _series_stats(row.get("brake") for row in rows),
        "throttle_positive_rows": sum(1 for row in rows if float(row.get("throttle") or 0.0) > 0.01),
        "brake_positive_rows": sum(1 for row in rows if float(row.get("brake") or 0.0) > 0.01),
        "throttle_brake_switch_count": throttle_brake_switch_count,
        "overspeed_threshold_mps": overspeed_threshold,
        "overspeed_gt_threshold_rows": len(overspeed_rows),
        "throttle_while_overspeed_gt_threshold_rows": len(throttle_while_overspeed_rows),
        "coast_while_overspeed_gt_threshold_rows": len(coast_while_overspeed_rows),
        "positive_accel_while_overspeed_gt_threshold_rows": len(
            positive_accel_while_overspeed_rows
        ),
        "speed_sources": sorted({str(row.get("speed_source")) for row in rows if row.get("speed_source")}),
        "longitudinal_modes": sorted(
            {str(row.get("longitudinal_mode")) for row in rows if row.get("longitudinal_mode")}
        ),
    }


def _applied_control_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    states: list[str] = []
    throttle_values: list[float] = []
    brake_values: list[float] = []
    steer_values: list[float] = []

    for row in rows:
        throttle = _first_float(row, ("throttle_applied", "carla_throttle_applied", "applied_throttle"))
        brake = _first_float(row, ("brake_applied", "carla_brake_applied", "applied_brake"))
        steer = _first_float(row, ("carla_steer_applied", "steer_applied", "applied_steer"))
        if throttle is not None:
            throttle_values.append(throttle)
        if brake is not None:
            brake_values.append(brake)
        if steer is not None:
            steer_values.append(steer)

        throttle_f = float(throttle or 0.0)
        brake_f = float(brake or 0.0)
        if throttle_f > 0.01 and brake_f > 0.01:
            states.append("conflict")
        elif throttle_f > 0.01:
            states.append("throttle")
        elif brake_f > 0.01:
            states.append("brake")
        else:
            states.append("coast")

    compact_states = [state for state in states if state in {"throttle", "brake"}]
    throttle_brake_switch_count = sum(
        1 for prev, cur in zip(compact_states, compact_states[1:]) if prev != cur
    )
    state_counts = {state: states.count(state) for state in ("throttle", "brake", "conflict", "coast")}
    return {
        "available": bool(rows),
        "sample_count": len(rows),
        "state_counts": state_counts,
        "throttle": _series_stats(throttle_values),
        "brake": _series_stats(brake_values),
        "steer": _series_stats(steer_values),
        "throttle_positive_rows": state_counts["throttle"] + state_counts["conflict"],
        "brake_positive_rows": state_counts["brake"] + state_counts["conflict"],
        "throttle_brake_conflict_rows": state_counts["conflict"],
        "throttle_brake_switch_count": throttle_brake_switch_count,
    }


def analyze_autoware_control_run(
    run_dir: str | Path,
    *,
    planning_speed_threshold_mps: float = 3.0,
    control_speed_threshold_mps: float = 1.0,
) -> dict[str, Any]:
    run_path = Path(run_dir)
    artifacts = run_path / "artifacts"
    summary = _read_json(run_path / "summary.json")

    planning_rows = _read_jsonl(artifacts / "ros2_topic__planning__scenario_planning__trajectory.jsonl")
    smoother_rows = _read_jsonl(
        artifacts / "ros2_topic__planning__scenario_planning__velocity_smoother__trajectory.jsonl"
    )
    relayed_rows = _read_jsonl(artifacts / "ros2_topic__planning__trajectory.jsonl")
    follower_rows = _read_jsonl(artifacts / "ros2_topic__control__trajectory_follower__control_cmd.jsonl")
    gated_rows = _read_jsonl(artifacts / "autoware_control.jsonl")
    resampled_reference_rows = _read_jsonl(
        artifacts
        / "ros2_topic__control__trajectory_follower__controller_node_exe__debug__resampled_reference_trajectory.jsonl"
    )
    debug_predicted_frenet_rows = _read_jsonl(
        artifacts
        / "ros2_topic__control__trajectory_follower__controller_node_exe__debug__predicted_trajectory_in_frenet_coordinate.jsonl"
    )
    lateral_predicted_rows = _read_jsonl(
        artifacts
        / "ros2_topic__control__trajectory_follower__lateral__predicted_trajectory.jsonl"
    )
    longitudinal_diagnostic_rows = _read_jsonl(
        artifacts / "ros2_topic__control__trajectory_follower__longitudinal__diagnostic.jsonl"
    )
    operation_rows = _read_jsonl(artifacts / "ros2_topic__control__vehicle_cmd_gate__operation_mode.jsonl")
    system_operation_rows = _read_jsonl(artifacts / "ros2_topic__system__operation_mode__state.jsonl")
    diagnostics_rows = _read_jsonl(artifacts / "ros2_topic__diagnostics.jsonl")
    kinematic_rows = _read_jsonl(artifacts / "ros2_topic__localization__kinematic_state.jsonl")
    velocity_limit_rows = {
        name: _read_jsonl_family(artifacts / filename)
        for name, filename in VELOCITY_LIMIT_TOPIC_FILES.items()
    }
    timeseries_rows = _read_csv(run_path / "timeseries.csv")
    planning_common_override = _read_json(artifacts / "autoware_planning_common_override.json")
    launch_log_signals = _launch_log_signals(artifacts)
    _classify_launch_events_relative_to_first_control(
        launch_log_signals, follower_rows or gated_rows
    )
    diagnostic_graph_log = _diagnostic_graph_log_stats(
        artifacts, launch_log_signals.get("first_control_node_stamp_s")
    )
    bridge_control = _bridge_log_stats(
        _read_text(artifacts / "autoware_carla_control_bridge.log"),
        launch_log_signals.get("first_control_node_stamp_s"),
    )
    applied_control = _applied_control_stats(timeseries_rows)

    planning = _trajectory_stats(planning_rows)
    smoother = _trajectory_stats(smoother_rows)
    relayed = _trajectory_stats(relayed_rows)
    follower = _control_stats(follower_rows)
    gated = _control_stats(gated_rows)
    controller_debug_topics = {
        "resampled_reference_trajectory": _topic_capture_stats(resampled_reference_rows),
        "predicted_trajectory_in_frenet_coordinate": _topic_capture_stats(debug_predicted_frenet_rows),
        "lateral_predicted_trajectory": _topic_capture_stats(lateral_predicted_rows),
    }
    controller_debug_alignment = _controller_debug_alignment(
        {
            "resampled_reference_trajectory": resampled_reference_rows,
            "predicted_trajectory_in_frenet_coordinate": debug_predicted_frenet_rows,
            "lateral_predicted_trajectory": lateral_predicted_rows,
        },
        follower_rows or gated_rows,
        kinematic_rows,
    )
    longitudinal_diagnostic = _longitudinal_diagnostic_stats(longitudinal_diagnostic_rows)
    velocity_limits = {
        name: _velocity_limit_topic_stats(rows)
        for name, rows in velocity_limit_rows.items()
    }
    operation = _operation_mode_stats(operation_rows)
    diagnostics = _diagnostics_topic_stats(diagnostics_rows)
    time_domains = {
        "kinematic_state": _stamp_stats(kinematic_rows),
        "trajectory_follower_control": _stamp_stats(follower_rows),
        "gated_control": _stamp_stats(gated_rows),
        "vehicle_cmd_gate_operation_mode": _stamp_stats(operation_rows),
    }
    trajectory_alignment = _trajectory_sample_alignment(relayed_rows, follower_rows, kinematic_rows)
    speed_response = _vehicle_speed_response(gated_rows or follower_rows, kinematic_rows)
    kinematic_twist_contract = _kinematic_twist_contract_stats(kinematic_rows)

    missing_inputs: list[str] = []
    if not summary:
        missing_inputs.append("summary.json")
    if not planning_rows:
        missing_inputs.append("planning_trajectory")
    if not smoother_rows:
        missing_inputs.append("velocity_smoother_trajectory")
    if not relayed_rows:
        missing_inputs.append("relayed_planning_trajectory")
    if not follower_rows:
        missing_inputs.append("trajectory_follower_control")
    if not gated_rows:
        missing_inputs.append("gated_control")
    if not operation_rows:
        missing_inputs.append("vehicle_cmd_gate_operation_mode")
    if not system_operation_rows:
        missing_inputs.append("system_operation_mode_state")
    if not diagnostics_rows:
        missing_inputs.append("diagnostics")
    if not kinematic_rows:
        missing_inputs.append("kinematic_state")
    if not timeseries_rows:
        missing_inputs.append("timeseries.csv")

    warnings: list[str] = []
    failure_reasons: list[str] = []
    planning_max = relayed["velocity_max_mps"]["max"] or smoother["velocity_max_mps"]["max"] or planning["velocity_max_mps"]["max"]
    control_max = gated["target_velocity_mps"]["max"] or follower["target_velocity_mps"]["max"]
    expected_motion_speed = _expected_motion_speed_mps(run_path, summary)
    planning_control_speed_gap = (
        float(planning_max) - float(control_max)
        if planning_max is not None and control_max is not None
        else None
    )
    control_target_expected_speed_margin = (
        float(expected_motion_speed) - float(control_max)
        if expected_motion_speed is not None and control_max is not None
        else None
    )
    requested_values = planning_common_override.get("requested_values") or {}
    requested_planning_common_max_mps = _safe_float(
        planning_common_override.get("requested_max_vel_mps")
        or requested_values.get("max_vel")
    )
    planning_requested_speed_gap = (
        float(requested_planning_common_max_mps) - float(planning_max)
        if requested_planning_common_max_mps is not None and planning_max is not None
        else None
    )
    velocity_limit_message_count = sum(
        int((stats.get("message_count") or 0)) for stats in velocity_limits.values()
    )
    current_velocity_limit_max = _safe_float(
        ((velocity_limits.get("current_max_velocity") or {}).get("max_velocity_mps") or {}).get("max")
    )
    planning_current_limit_gap = (
        float(current_velocity_limit_max) - float(planning_max)
        if current_velocity_limit_max is not None and planning_max is not None
        else None
    )
    if (
        planning_requested_speed_gap is not None
        and planning_requested_speed_gap >= REQUESTED_PLANNING_MAX_GAP_WARN_MPS
    ):
        failure_reasons.append("planning_velocity_below_requested_common_max")
        warnings.append(
            "Autoware common max_vel override requests a higher speed than the published "
            "trajectory reaches; inspect scenario_planning velocity-limit/current-max-velocity "
            "topics before treating this as bridge or CARLA actuation behavior"
        )
        if velocity_limit_message_count <= 0:
            failure_reasons.append("velocity_limit_topics_missing")
            warnings.append(
                "velocity limit topics were not captured, so the source of the planning speed "
                "ceiling is still unproven"
            )
        if (
            planning_max is not None
            and abs(float(planning_max) - LANELET_DEFAULT_URBAN_SPEED_MPS)
            <= LANELET_DEFAULT_URBAN_SPEED_TOLERANCE_MPS
        ):
            failure_reasons.append("planning_velocity_matches_lanelet_default_urban_speed_candidate")
            warnings.append(
                "trajectory max speed is approximately 50 km/h while common max_vel requests more; "
                "this is consistent with a Lanelet2/map traffic-rule speed limit candidate, not a "
                "bridge-side throttle limit"
            )
    if (
        planning_current_limit_gap is not None
        and planning_current_limit_gap >= REQUESTED_PLANNING_MAX_GAP_WARN_MPS
    ):
        failure_reasons.append("planning_velocity_below_current_velocity_limit")
        warnings.append(
            "scenario_planning/current_max_velocity is higher than the trajectory speed ceiling; "
            "the remaining cap is likely upstream/downstream of the external velocity-limit selector"
        )
    if (
        expected_motion_speed is not None
        and planning_max is not None
        and planning_max + PLANNING_ACCEPTANCE_SPEED_MARGIN_MPS < expected_motion_speed
    ):
        failure_reasons.append("planning_velocity_ceiling_below_acceptance_speed")
        warnings.append(
            "planning/velocity-smoother trajectory never reaches the configured motion speed "
            "threshold; inspect route speed limits, map semantics, velocity smoother params, and "
            "scenario target speed before adding bridge-side acceleration assist"
        )
    if (
        expected_motion_speed is not None
        and planning_max is not None
        and control_max is not None
        and planning_max + PLANNING_ACCEPTANCE_SPEED_MARGIN_MPS >= expected_motion_speed
        and control_max + PLANNING_ACCEPTANCE_SPEED_MARGIN_MPS < expected_motion_speed
    ):
        failure_reasons.append("control_target_below_expected_motion_speed")
        warnings.append(
            "planning trajectory reaches the configured motion speed threshold, but "
            "trajectory follower/gated control target does not; inspect velocity smoother "
            "output near ego, longitudinal target selection, and vehicle_cmd_gate semantics"
        )
    if (
        planning_control_speed_gap is not None
        and planning_control_speed_gap >= PLANNING_CONTROL_COMPRESSION_THRESHOLD_MPS
        and planning_max is not None
        and control_max is not None
        and planning_max >= planning_speed_threshold_mps
        and control_max >= control_speed_threshold_mps
    ):
        failure_reasons.append("planning_to_control_velocity_compression")
        warnings.append(
            "planning trajectory has substantially higher max speed than follower/gated "
            "control target; this is a planning-to-control handoff/target-selection issue, "
            "not just a CARLA actuation issue"
        )
    kinematic_speed_max = (kinematic_twist_contract.get("speed_norm_mps") or {}).get("max")
    kinematic_forward_ratio = kinematic_twist_contract.get("forward_component_ratio_at_max_speed")
    kinematic_abs_y_max = kinematic_twist_contract.get("abs_linear_y_max_mps")
    kinematic_abs_x_max = kinematic_twist_contract.get("abs_linear_x_max_mps")
    if (
        kinematic_speed_max is not None
        and kinematic_forward_ratio is not None
        and kinematic_speed_max >= KINEMATIC_TWIST_SPEED_MISMATCH_MIN_SPEED_MPS
        and kinematic_forward_ratio < KINEMATIC_TWIST_FORWARD_RATIO_WARN
    ):
        failure_reasons.append("kinematic_twist_forward_component_low")
        warnings.append(
            "localization/kinematic_state twist.linear.x is much smaller than the speed norm; "
            "Autoware longitudinal control expects base_link forward velocity in twist.linear.x, "
            "not map-frame velocity components"
        )
    if (
        kinematic_abs_x_max is not None
        and kinematic_abs_y_max is not None
        and kinematic_abs_y_max > max(kinematic_abs_x_max * 2.0, 1.0)
    ):
        failure_reasons.append("kinematic_twist_velocity_axis_swapped_candidate")
        warnings.append(
            "kinematic_state carries most velocity in twist.linear.y; inspect CARLA-to-Autoware "
            "odometry twist frame semantics"
        )
    if planning_max is not None and control_max is not None:
        if planning_max >= planning_speed_threshold_mps and control_max < control_speed_threshold_mps:
            failure_reasons.append("control_target_low_despite_planning_speed")
            warnings.append(
                "planning trajectory has usable far-point speed, but follower/gated control target remains low"
            )
    controller_reference_max = max(
        _numbers(
            [
                (
                    (topic.get("trajectory") or {}).get("velocity_max_mps") or {}
                ).get("max")
                for topic in controller_debug_topics.values()
            ]
        )
        or [None]
    )
    if (
        controller_reference_max is not None
        and control_max is not None
        and controller_reference_max >= 2.0
        and control_max < control_speed_threshold_mps
    ):
        failure_reasons.append("control_target_low_despite_controller_reference_speed")
        warnings.append(
            "controller debug reference/predicted trajectory contains faster points, "
            "but final control target remains low; inspect target selection and low-speed PID behavior"
        )
    controller_nearest_reference_max = max(
        _numbers(
            [
                (alignment.get("nearest_sample_velocity_mps") or {}).get("max")
                for alignment in controller_debug_alignment.values()
            ]
        )
        or [None]
    )
    if (
        controller_reference_max is not None
        and controller_nearest_reference_max is not None
        and controller_reference_max >= 2.0
        and controller_nearest_reference_max < control_speed_threshold_mps
    ):
        failure_reasons.append("controller_nearest_reference_velocity_low")
        warnings.append(
            "controller debug trajectory has faster far points, but the ego-nearest debug reference "
            "is still low-speed; inspect nearest/preview target selection and start-state logic"
        )
    elif (
        controller_nearest_reference_max is not None
        and control_max is not None
        and controller_nearest_reference_max >= control_speed_threshold_mps
        and control_max < control_speed_threshold_mps
    ):
        failure_reasons.append("control_target_low_despite_controller_nearest_reference_speed")
        warnings.append(
            "ego-nearest controller debug reference is fast enough, but final control target remains low; "
            "inspect longitudinal PID, stop-state, and vehicle_cmd_gate behavior"
        )
    longitudinal_diag_target_max = (
        (longitudinal_diagnostic.get("fields") or {}).get("target_velocity_mps") or {}
    ).get("max")
    longitudinal_diag_nearest_max = (
        (longitudinal_diagnostic.get("fields") or {}).get("nearest_velocity_mps") or {}
    ).get("max")
    if (
        planning_max is not None
        and longitudinal_diag_target_max is not None
        and planning_max >= planning_speed_threshold_mps
        and longitudinal_diag_target_max < control_speed_threshold_mps
    ):
        failure_reasons.append("longitudinal_pid_target_velocity_low")
        warnings.append(
            "longitudinal PID diagnostic target velocity remains low while planning has faster points"
        )
    if (
        planning_max is not None
        and longitudinal_diag_nearest_max is not None
        and planning_max >= planning_speed_threshold_mps
        and longitudinal_diag_nearest_max < control_speed_threshold_mps
    ):
        failure_reasons.append("longitudinal_pid_nearest_velocity_low")
        warnings.append(
            "longitudinal PID diagnostic nearest velocity remains low; inspect trajectory projection/start-state"
        )
    control_state_counts = longitudinal_diagnostic.get("control_state_counts") or {}
    if (
        planning_max is not None
        and planning_max >= planning_speed_threshold_mps
        and longitudinal_diagnostic.get("message_count", 0) > 0
        and not control_state_counts.get("DRIVE")
    ):
        failure_reasons.append("longitudinal_pid_not_drive_state")
        warnings.append(
            "longitudinal PID diagnostic never reports DRIVE while planning has faster points"
        )
    nearest_velocity_max = trajectory_alignment["nearest_sample_velocity_mps"]["max"]
    target_minus_nearest_values = _numbers(
        [
            trajectory_alignment["control_target_minus_nearest_sample_velocity_mps"]["min"],
            trajectory_alignment["control_target_minus_nearest_sample_velocity_mps"]["max"],
        ]
    )
    target_minus_nearest_abs_max = (
        max(abs(value) for value in target_minus_nearest_values)
        if target_minus_nearest_values
        else None
    )
    if (
        planning_max is not None
        and nearest_velocity_max is not None
        and planning_max >= planning_speed_threshold_mps
        and nearest_velocity_max < control_speed_threshold_mps
    ):
        failure_reasons.append("nearest_trajectory_sample_velocity_low")
        warnings.append(
            "ego-nearest sampled trajectory point remains low-speed even when far trajectory has usable speed"
        )
    if target_minus_nearest_abs_max is not None and target_minus_nearest_abs_max <= 0.25:
        warnings.append("control target velocity closely follows ego-nearest sampled trajectory velocity")
    requested_positive_accel_mean = (
        speed_response.get("commanded_positive_acceleration_mps2") or {}
    ).get("mean")
    actual_positive_accel_mean = (
        speed_response.get("actual_acceleration_when_positive_command_mps2") or {}
    ).get("mean")
    if (
        requested_positive_accel_mean is not None
        and actual_positive_accel_mean is not None
        and requested_positive_accel_mean >= 0.4
        and actual_positive_accel_mean < max(0.2, requested_positive_accel_mean * 0.5)
    ):
        failure_reasons.append("actuation_accel_underresponse_candidate")
        warnings.append(
            "Autoware commands positive acceleration, but measured ego acceleration remains low; "
            "check bridge throttle mapping and vehicle actuation calibration"
        )
    if bridge_control["throttle_brake_switch_count"] >= 10:
        failure_reasons.append("actuation_oscillation_candidate")
        warnings.append("control bridge alternates throttle/brake frequently; tune feedback/hysteresis before promotion")
    if applied_control["throttle_brake_switch_count"] >= 10:
        failure_reasons.append("applied_actuation_oscillation_candidate")
        warnings.append(
            "CARLA applied control alternates throttle/brake frequently; this is not a natural-driving pass"
        )
    if (
        bridge_control["throttle_while_overspeed_gt_threshold_rows"]
        >= BRIDGE_OVERSPEED_THROTTLE_FAILURE_ROWS
    ):
        failure_reasons.append("bridge_throttle_while_above_target_candidate")
        warnings.append(
            "control bridge applies throttle while ego speed is already well above Autoware target speed; "
            "treat this as bridge-assisted motion rather than natural control evidence"
        )
    try:
        collision_count = int(summary.get("collision_count") or 0)
    except (TypeError, ValueError):
        collision_count = 0
    try:
        lane_invasion_count = int(summary.get("lane_invasion_count") or 0)
    except (TypeError, ValueError):
        lane_invasion_count = 0
    exit_reason = str(summary.get("exit_reason") or "")
    if collision_count > 0 or exit_reason == "COLLISION":
        failure_reasons.append("collision")
        warnings.append("run summary reports collision; this is never a natural-driving pass")
    if lane_invasion_count > 0 or exit_reason == "LANE_INVASION":
        failure_reasons.append("lane_invasion")
        warnings.append("run summary reports lane invasion; faster actuation candidate is not safe for promotion")
    if bridge_control.get("watchdog_after_first_control_count", 0) > 0:
        warnings.append(
            "control bridge watchdog applied fallback braking after first captured control; inspect command gaps"
        )
    elif bridge_control.get("watchdog_rows", 0) > 0:
        warnings.append("control bridge watchdog fallback was observed only before the first captured control command")
    if operation["autonomous_available_any"] is False:
        failure_reasons.append("operation_mode_not_autonomous_available")
        warnings.append("vehicle_cmd_gate reports autonomous mode unavailable")
    elif (
        operation.get("autonomous_available_any") is True
        and (
            (operation.get("autonomous_available_transition_count") or 0) > 0
            or (operation.get("autonomous_available_ratio") or 0.0) < 0.9
        )
    ):
        engaged = operation.get("last_mode") == 2 or operation.get("autoware_control_enabled_any") is True
        if engaged:
            warnings.append(
                "vehicle_cmd_gate autonomous availability toggles, but Autoware reports autonomous/control-enabled mode"
            )
        else:
            failure_reasons.append("operation_mode_autonomous_availability_unstable")
            warnings.append("vehicle_cmd_gate autonomous availability toggles during the run")
    kinematics_timeout_after_first_control = launch_log_signals.get(
        "kinematics_timeout_count_after_first_control_count"
    )
    if kinematics_timeout_after_first_control is None:
        kinematics_timeout_after_first_control = launch_log_signals["kinematics_timeout_count"]
    if kinematics_timeout_after_first_control > 0:
        failure_reasons.append("operation_mode_kinematics_timeout")
        warnings.append("operation mode transition manager reports subscribed kinematics timeout")
    elif launch_log_signals["kinematics_timeout_count"] > 0:
        warnings.append(
            "kinematics timeout was observed only before the first captured control command"
        )

    control_timeout_after_first_control = launch_log_signals.get(
        "control_cmd_timeout_count_after_first_control_count"
    )
    if control_timeout_after_first_control is None:
        control_timeout_after_first_control = launch_log_signals["control_cmd_timeout_count"]
    if control_timeout_after_first_control > 0:
        failure_reasons.append("operation_mode_control_cmd_timeout")
        warnings.append("operation mode transition manager reports subscribed control_cmd timeout")
    elif launch_log_signals["control_cmd_timeout_count"] > 0:
        warnings.append(
            "control_cmd timeout was observed only before the first captured control command"
        )
    if launch_log_signals["operation_mode_change_rejected_count"] > 0:
        failure_reasons.append("operation_mode_change_rejected")
        warnings.append("Autoware rejected autonomous operation mode change condition")
    if launch_log_signals.get("behavior_planning_container_crash_count", 0) > 0:
        failure_reasons.append("behavior_planning_container_crash")
        warnings.append("Autoware behavior_planning_container exited during the run; planning trajectory materialization cannot be trusted")
    if launch_log_signals.get("guard_condition_failure_count", 0) > 0:
        failure_reasons.append("ros2_guard_condition_failure")
        warnings.append("Autoware launch log reports a ROS2 guard condition failure; inspect executor/container shutdown or crash timing")
    kinematic_domain = time_domains["kinematic_state"]["stamp_domain"]
    control_domain = (
        time_domains["trajectory_follower_control"]["stamp_domain"]
        or time_domains["gated_control"]["stamp_domain"]
    )
    operation_domain = time_domains["vehicle_cmd_gate_operation_mode"]["stamp_domain"]
    if kinematic_domain and (control_domain or operation_domain):
        other_domains = {domain for domain in (control_domain, operation_domain) if domain}
        if any(domain != kinematic_domain for domain in other_domains):
            failure_reasons.append("mixed_time_domain_between_kinematics_and_control")
            warnings.append(
                "kinematic_state stamps and control/operation-mode stamps appear to use different time domains"
            )
    if "system_operation_mode_state" in missing_inputs:
        warnings.append("system operation mode topic is missing; operation-mode root cause remains incomplete")
    if diagnostics_rows and not diagnostics["has_status_details"]:
        warnings.append("diagnostics topic was captured without DiagnosticArray status details")

    if missing_inputs and ("planning_trajectory" in missing_inputs or "gated_control" in missing_inputs):
        status = "insufficient_data"
    elif failure_reasons or warnings:
        status = "warn"
    else:
        status = "pass"

    return {
        "schema_version": SCHEMA_VERSION,
        "run_dir": str(run_path),
        "artifacts_dir": str(artifacts),
        "summary": {
            "success": summary.get("success"),
            "exit_reason": summary.get("exit_reason"),
            "fail_reason": summary.get("fail_reason"),
            "max_speed_mps": summary.get("max_speed_mps"),
            "collision_count": summary.get("collision_count"),
            "lane_invasion_count": summary.get("lane_invasion_count"),
        },
        "planning": planning,
        "velocity_smoother": smoother,
        "relayed_planning": relayed,
        "trajectory_follower_control": follower,
        "gated_control": gated,
        "controller_debug_topics": controller_debug_topics,
        "controller_debug_alignment": controller_debug_alignment,
        "longitudinal_diagnostic": longitudinal_diagnostic,
        "planning_common_override": planning_common_override,
        "velocity_limits": velocity_limits,
        "operation_mode": operation,
        "diagnostics": diagnostics,
        "launch_log_signals": launch_log_signals,
        "diagnostic_graph_log": diagnostic_graph_log,
        "bridge_control": bridge_control,
        "applied_control": applied_control,
        "time_domains": time_domains,
        "trajectory_ego_alignment": trajectory_alignment,
        "vehicle_speed_response": speed_response,
        "kinematic_twist_contract": kinematic_twist_contract,
        "missing_inputs": missing_inputs,
        "warnings": warnings,
        "verdict": {
            "status": status,
            "failure_reasons": failure_reasons,
            "planning_speed_threshold_mps": planning_speed_threshold_mps,
            "control_speed_threshold_mps": control_speed_threshold_mps,
            "expected_motion_speed_mps": expected_motion_speed,
            "planning_acceptance_speed_margin_mps": PLANNING_ACCEPTANCE_SPEED_MARGIN_MPS,
            "planning_control_compression_threshold_mps": PLANNING_CONTROL_COMPRESSION_THRESHOLD_MPS,
            "planning_control_speed_gap_mps": planning_control_speed_gap,
            "control_target_expected_speed_margin_mps": control_target_expected_speed_margin,
            "requested_planning_common_max_mps": requested_planning_common_max_mps,
            "requested_planning_common_speed_gap_mps": planning_requested_speed_gap,
            "current_velocity_limit_max_mps": current_velocity_limit_max,
            "planning_current_velocity_limit_gap_mps": planning_current_limit_gap,
        },
    }


def write_autoware_control_diagnostics(report: dict[str, Any], out_dir: str | Path) -> None:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "autoware_control_diagnostics.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    )
    (out / "autoware_control_diagnostics.md").write_text(_format_markdown(report))


def _format_markdown(report: dict[str, Any]) -> str:
    verdict = report.get("verdict") or {}
    summary = report.get("summary") or {}
    planning = report.get("relayed_planning") or report.get("planning") or {}
    control = report.get("gated_control") or {}
    operation = report.get("operation_mode") or {}
    launch_log_signals = report.get("launch_log_signals") or {}
    diagnostic_graph_log = report.get("diagnostic_graph_log") or {}
    diagnostics = report.get("diagnostics") or {}
    bridge_control = report.get("bridge_control") or {}
    applied_control = report.get("applied_control") or {}
    time_domains = report.get("time_domains") or {}
    alignment = report.get("trajectory_ego_alignment") or {}
    speed_response = report.get("vehicle_speed_response") or {}
    kinematic_twist_contract = report.get("kinematic_twist_contract") or {}
    debug_topics = report.get("controller_debug_topics") or {}
    debug_alignment = report.get("controller_debug_alignment") or {}
    longitudinal_diagnostic = report.get("longitudinal_diagnostic") or {}
    longitudinal_diag_fields = longitudinal_diagnostic.get("fields") or {}
    velocity_limits = report.get("velocity_limits") or {}
    lines = [
        "# Autoware Control Diagnostics",
        "",
        f"- run_dir: `{report.get('run_dir')}`",
        f"- status: `{verdict.get('status')}`",
        f"- failure_reasons: `{', '.join(verdict.get('failure_reasons') or []) or 'none'}`",
        f"- max_speed_mps: `{summary.get('max_speed_mps')}`",
        f"- expected_motion_speed_mps: `{verdict.get('expected_motion_speed_mps')}`",
        f"- planning_velocity_max_mps: `{(planning.get('velocity_max_mps') or {}).get('max')}`",
        f"- control_target_velocity_max_mps: `{(control.get('target_velocity_mps') or {}).get('max')}`",
        f"- planning_control_speed_gap_mps: `{verdict.get('planning_control_speed_gap_mps')}`",
        f"- control_target_expected_speed_margin_mps: `{verdict.get('control_target_expected_speed_margin_mps')}`",
        f"- requested_planning_common_max_mps: `{verdict.get('requested_planning_common_max_mps')}`",
        f"- requested_planning_common_speed_gap_mps: `{verdict.get('requested_planning_common_speed_gap_mps')}`",
        f"- planning_current_velocity_limit_gap_mps: `{verdict.get('planning_current_velocity_limit_gap_mps')}`",
        f"- current_max_velocity_topic_max_mps: `{(((velocity_limits.get('current_max_velocity') or {}).get('max_velocity_mps') or {}).get('max'))}`",
        f"- max_velocity_topic_max_mps: `{(((velocity_limits.get('max_velocity') or {}).get('max_velocity_mps') or {}).get('max'))}`",
        f"- max_velocity_default_topic_max_mps: `{(((velocity_limits.get('max_velocity_default') or {}).get('max_velocity_mps') or {}).get('max'))}`",
        f"- velocity_limit_topic_message_count: `{sum(int(((stats or {}).get('message_count') or 0)) for stats in velocity_limits.values())}`",
        f"- nearest_trajectory_sample_velocity_max_mps: `{(alignment.get('nearest_sample_velocity_mps') or {}).get('max')}`",
        f"- nearest_trajectory_sample_index_last: `{(alignment.get('nearest_sample_index') or {}).get('last')}`",
        f"- first_high_velocity_sample_distance_max_m: `{(alignment.get('first_sample_ge_threshold_distance_m') or {}).get('max')}`",
        f"- first_high_velocity_sample_distance_last_m: `{(alignment.get('first_sample_ge_threshold_distance_m') or {}).get('last')}`",
        f"- first_velocity_ge_0p5_distance_last_m: `{((((alignment.get('first_sample_ge_velocity_threshold') or {}).get('0p5') or {}).get('distance_m') or {}).get('last'))}`",
        f"- first_velocity_ge_1p0_distance_last_m: `{((((alignment.get('first_sample_ge_velocity_threshold') or {}).get('1') or {}).get('distance_m') or {}).get('last'))}`",
        f"- first_velocity_ge_2p0_distance_last_m: `{((((alignment.get('first_sample_ge_velocity_threshold') or {}).get('2') or {}).get('distance_m') or {}).get('last'))}`",
        f"- near_field_0_0p5m_max_velocity_max_mps: `{((((alignment.get('near_field_velocity_profile') or {}).get('0_0p5m') or {}).get('max_velocity_mps') or {}).get('max'))}`",
        f"- near_field_1_2m_max_velocity_max_mps: `{((((alignment.get('near_field_velocity_profile') or {}).get('1_2m') or {}).get('max_velocity_mps') or {}).get('max'))}`",
        f"- near_field_2_5m_max_velocity_max_mps: `{((((alignment.get('near_field_velocity_profile') or {}).get('2_5m') or {}).get('max_velocity_mps') or {}).get('max'))}`",
        f"- control_target_minus_first_high_velocity_sample_min_mps: `{(alignment.get('control_target_minus_first_high_velocity_sample_mps') or {}).get('min')}`",
        f"- commanded_positive_acceleration_mean_mps2: `{(speed_response.get('commanded_positive_acceleration_mps2') or {}).get('mean')}`",
        f"- actual_acceleration_when_positive_command_mean_mps2: `{(speed_response.get('actual_acceleration_when_positive_command_mps2') or {}).get('mean')}`",
        f"- actual_over_commanded_positive_acceleration_ratio: `{speed_response.get('actual_over_commanded_positive_acceleration_ratio')}`",
        f"- kinematic_twist_linear_x_max_mps: `{((kinematic_twist_contract.get('linear_x_mps') or {}).get('max'))}`",
        f"- kinematic_twist_linear_y_max_mps: `{((kinematic_twist_contract.get('linear_y_mps') or {}).get('max'))}`",
        f"- kinematic_twist_speed_norm_max_mps: `{((kinematic_twist_contract.get('speed_norm_mps') or {}).get('max'))}`",
        f"- kinematic_twist_forward_component_ratio: `{kinematic_twist_contract.get('forward_component_ratio_at_max_speed')}`",
        f"- autonomous_available_any: `{operation.get('autonomous_available_any')}`",
        f"- autoware_control_enabled_any: `{operation.get('autoware_control_enabled_any')}`",
        f"- operation_mode_last: `{operation.get('last_mode')}`",
        f"- autonomous_available_ratio: `{operation.get('autonomous_available_ratio')}`",
        f"- kinematics_timeout_count: `{launch_log_signals.get('kinematics_timeout_count')}`",
        f"- kinematics_timeout_after_first_control_count: `{launch_log_signals.get('kinematics_timeout_count_after_first_control_count')}`",
        f"- control_cmd_timeout_count: `{launch_log_signals.get('control_cmd_timeout_count')}`",
        f"- control_cmd_timeout_after_first_control_count: `{launch_log_signals.get('control_cmd_timeout_count_after_first_control_count')}`",
        f"- gear_command_missing_count: `{launch_log_signals.get('gear_command_missing_count')}`",
        f"- gear_command_missing_after_first_control_count: `{launch_log_signals.get('gear_command_missing_count_after_first_control_count')}`",
        f"- operation_mode_change_rejected_count: `{launch_log_signals.get('operation_mode_change_rejected_count')}`",
        f"- first_control_node_stamp_s: `{launch_log_signals.get('first_control_node_stamp_s')}`",
        f"- diag_graph_not_available_blocks: `{diagnostic_graph_log.get('not_available_block_count')}`",
        f"- diag_graph_not_available_after_first_control_count: `{diagnostic_graph_log.get('not_available_after_first_control_count')}`",
        f"- diagnostics_message_count: `{diagnostics.get('message_count')}`",
        f"- diagnostics_status_detail_count: `{diagnostics.get('status_detail_count')}`",
        f"- bridge_current_speed_max_mps: `{(bridge_control.get('current_speed_mps') or {}).get('max')}`",
        f"- bridge_current_minus_target_speed_max_mps: `{(bridge_control.get('current_minus_target_speed_mps') or {}).get('max')}`",
        f"- bridge_overspeed_threshold_mps: `{bridge_control.get('overspeed_threshold_mps')}`",
        f"- bridge_overspeed_gt_threshold_rows: `{bridge_control.get('overspeed_gt_threshold_rows')}`",
        f"- bridge_throttle_while_overspeed_gt_threshold_rows: `{bridge_control.get('throttle_while_overspeed_gt_threshold_rows')}`",
        f"- bridge_positive_accel_while_overspeed_gt_threshold_rows: `{bridge_control.get('positive_accel_while_overspeed_gt_threshold_rows')}`",
        f"- bridge_throttle_brake_switch_count: `{bridge_control.get('throttle_brake_switch_count')}`",
        f"- bridge_watchdog_rows: `{bridge_control.get('watchdog_rows')}`",
        f"- bridge_watchdog_after_first_control_count: `{bridge_control.get('watchdog_after_first_control_count')}`",
        f"- bridge_speed_sources: `{', '.join(bridge_control.get('speed_sources') or []) or 'none'}`",
        f"- applied_throttle_brake_switch_count: `{applied_control.get('throttle_brake_switch_count')}`",
        f"- applied_throttle_max: `{((applied_control.get('throttle') or {}).get('max'))}`",
        f"- applied_brake_max: `{((applied_control.get('brake') or {}).get('max'))}`",
        f"- debug_resampled_reference_message_count: `{((debug_topics.get('resampled_reference_trajectory') or {}).get('message_count'))}`",
        f"- debug_resampled_reference_velocity_max_mps: `{((((debug_topics.get('resampled_reference_trajectory') or {}).get('trajectory') or {}).get('velocity_max_mps') or {}).get('max'))}`",
        f"- debug_resampled_reference_nearest_velocity_max_mps: `{(((debug_alignment.get('resampled_reference_trajectory') or {}).get('nearest_sample_velocity_mps') or {}).get('max'))}`",
        f"- debug_resampled_reference_first_high_distance_last_m: `{(((debug_alignment.get('resampled_reference_trajectory') or {}).get('first_sample_ge_threshold_distance_m') or {}).get('last'))}`",
        f"- debug_resampled_reference_near_field_1_2m_max_velocity_max_mps: `{(((((debug_alignment.get('resampled_reference_trajectory') or {}).get('near_field_velocity_profile') or {}).get('1_2m') or {}).get('max_velocity_mps') or {}).get('max'))}`",
        f"- debug_predicted_frenet_message_count: `{((debug_topics.get('predicted_trajectory_in_frenet_coordinate') or {}).get('message_count'))}`",
        f"- debug_predicted_frenet_velocity_max_mps: `{((((debug_topics.get('predicted_trajectory_in_frenet_coordinate') or {}).get('trajectory') or {}).get('velocity_max_mps') or {}).get('max'))}`",
        f"- debug_predicted_frenet_nearest_velocity_max_mps: `{(((debug_alignment.get('predicted_trajectory_in_frenet_coordinate') or {}).get('nearest_sample_velocity_mps') or {}).get('max'))}`",
        f"- debug_predicted_frenet_first_high_distance_last_m: `{(((debug_alignment.get('predicted_trajectory_in_frenet_coordinate') or {}).get('first_sample_ge_threshold_distance_m') or {}).get('last'))}`",
        f"- debug_predicted_frenet_near_field_1_2m_max_velocity_max_mps: `{(((((debug_alignment.get('predicted_trajectory_in_frenet_coordinate') or {}).get('near_field_velocity_profile') or {}).get('1_2m') or {}).get('max_velocity_mps') or {}).get('max'))}`",
        f"- lateral_predicted_trajectory_message_count: `{((debug_topics.get('lateral_predicted_trajectory') or {}).get('message_count'))}`",
        f"- lateral_predicted_trajectory_velocity_max_mps: `{((((debug_topics.get('lateral_predicted_trajectory') or {}).get('trajectory') or {}).get('velocity_max_mps') or {}).get('max'))}`",
        f"- lateral_predicted_trajectory_nearest_velocity_max_mps: `{(((debug_alignment.get('lateral_predicted_trajectory') or {}).get('nearest_sample_velocity_mps') or {}).get('max'))}`",
        f"- lateral_predicted_trajectory_first_high_distance_last_m: `{(((debug_alignment.get('lateral_predicted_trajectory') or {}).get('first_sample_ge_threshold_distance_m') or {}).get('last'))}`",
        f"- lateral_predicted_trajectory_near_field_1_2m_max_velocity_max_mps: `{(((((debug_alignment.get('lateral_predicted_trajectory') or {}).get('near_field_velocity_profile') or {}).get('1_2m') or {}).get('max_velocity_mps') or {}).get('max'))}`",
        f"- longitudinal_diagnostic_message_count: `{longitudinal_diagnostic.get('message_count')}`",
        f"- longitudinal_diag_target_velocity_max_mps: `{((longitudinal_diag_fields.get('target_velocity_mps') or {}).get('max'))}`",
        f"- longitudinal_diag_nearest_velocity_max_mps: `{((longitudinal_diag_fields.get('nearest_velocity_mps') or {}).get('max'))}`",
        f"- longitudinal_diag_control_state_counts: `{longitudinal_diagnostic.get('control_state_counts')}`",
        f"- longitudinal_diag_stop_distance_last_m: `{((longitudinal_diag_fields.get('stop_distance_m') or {}).get('last'))}`",
        f"- longitudinal_diag_smooth_stop_mode_last: `{((longitudinal_diag_fields.get('smooth_stop_mode') or {}).get('last'))}`",
        f"- kinematic_stamp_domain: `{(time_domains.get('kinematic_state') or {}).get('stamp_domain')}`",
        f"- control_stamp_domain: `{(time_domains.get('trajectory_follower_control') or {}).get('stamp_domain')}`",
        f"- missing_inputs: `{', '.join(report.get('missing_inputs') or []) or 'none'}`",
        "",
        "## Warnings",
    ]
    warnings = report.get("warnings") or []
    lines.extend(f"- {warning}" for warning in warnings)
    if not warnings:
        lines.append("- none")
    top_leaf_failures = diagnostic_graph_log.get("top_leaf_failures") or []
    if top_leaf_failures:
        lines.extend(
            [
                "",
                "## Diagnostic Graph Leaf Failures",
            ]
        )
        lines.extend(
            f"- `{item.get('name')}`: `{item.get('count')}`" for item in top_leaf_failures[:10]
        )
    top_diagnostic_failures = diagnostics.get("top_failures") or []
    if top_diagnostic_failures:
        lines.extend(
            [
                "",
                "## Diagnostics Topic Failures",
            ]
        )
        lines.extend(
            f"- `{item.get('name')}`: `{item.get('count')}`" for item in top_diagnostic_failures[:10]
        )
    lines.append("")
    return "\n".join(lines)
