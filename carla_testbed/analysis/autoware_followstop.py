"""Follow-stop scene geometry diagnostics for Autoware/CARLA runs.

This module intentionally uses only run artifacts. It does not import CARLA,
ROS2, or Autoware runtime modules, so it can run in CI and on archived runs.
"""

from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "autoware_followstop_diagnostics.v1"

PLANNING_TOPIC_FILES = {
    "behavior_path_with_lane_id": (
        "ros2_topic__planning__scenario_planning__lane_driving__behavior_planning__path_with_lane_id.jsonl"
    ),
    "behavior_path": (
        "ros2_topic__planning__scenario_planning__lane_driving__behavior_planning__path.jsonl"
    ),
    "scenario_trajectory": "ros2_topic__planning__scenario_planning__trajectory.jsonl",
    "velocity_smoother_trajectory": (
        "ros2_topic__planning__scenario_planning__velocity_smoother__trajectory.jsonl"
    ),
    "planning_trajectory": "ros2_topic__planning__trajectory.jsonl",
}

CAUSAL_TOPIC_FILES = {
    "motion_velocity_planner_factor": {
        "jsonl": "ros2_topic__planning__planning_factors__motion_velocity_planner.jsonl",
        "log": "ros2_topic__planning__planning_factors__motion_velocity_planner.log",
    },
    "scenario_stop_reasons": {
        "jsonl": "ros2_topic__planning__scenario_planning__status__stop_reasons.jsonl",
        "log": "ros2_topic__planning__scenario_planning__status__stop_reasons.log",
    },
    "longitudinal_stop_reason": {
        "jsonl": "ros2_topic__control__trajectory_follower__longitudinal__stop_reason.jsonl",
        "log": "ros2_topic__control__trajectory_follower__longitudinal__stop_reason.log",
    },
}

PLANNING_FACTOR_TOPIC_FILES = {
    "obstacle_stop": "ros2_topic__planning__planning_factors__obstacle_stop.jsonl",
    "stop_line": "ros2_topic__planning__planning_factors__stop_line.jsonl",
    "motion_velocity_planner": "ros2_topic__planning__planning_factors__motion_velocity_planner.jsonl",
}


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _wrap_deg(delta_deg: float) -> float:
    return (float(delta_deg) + 180.0) % 360.0 - 180.0


def _relative_pose(ego: dict[str, Any], target: dict[str, Any]) -> dict[str, float | None]:
    ex = _safe_float(ego.get("x"))
    ey = _safe_float(ego.get("y"))
    yaw_deg = _safe_float(ego.get("yaw_deg", ego.get("ego_heading")))
    tx = _safe_float(target.get("x"))
    ty = _safe_float(target.get("y"))
    tyaw = _safe_float(target.get("yaw_deg", target.get("ego_heading")))
    if ex is None or ey is None or tx is None or ty is None or yaw_deg is None:
        return {
            "longitudinal_m": None,
            "lateral_m": None,
            "euclidean_m": None,
            "heading_diff_deg": None,
        }
    dx = tx - ex
    dy = ty - ey
    yaw = math.radians(yaw_deg)
    forward_x = math.cos(yaw)
    forward_y = math.sin(yaw)
    right_x = -math.sin(yaw)
    right_y = math.cos(yaw)
    heading_diff = _wrap_deg((tyaw or yaw_deg) - yaw_deg) if tyaw is not None else None
    return {
        "longitudinal_m": dx * forward_x + dy * forward_y,
        "lateral_m": dx * right_x + dy * right_y,
        "euclidean_m": math.hypot(dx, dy),
        "heading_diff_deg": heading_diff,
    }


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def _read_timeseries(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def _stamp_s(message: dict[str, Any]) -> float | None:
    stamp = message.get("stamp") or (message.get("header") or {}).get("stamp") or {}
    sec = _safe_float(stamp.get("sec"))
    nsec = _safe_float(stamp.get("nanosec"))
    if sec is None:
        return None
    return sec + ((nsec or 0.0) / 1_000_000_000.0)


def _nearest_timeseries_row(
    rows: list[dict[str, str]],
    stamp_s: float | None,
) -> dict[str, str] | None:
    if not rows or stamp_s is None:
        return None
    with_time = [row for row in rows if _safe_float(row.get("sim_time") or row.get("t")) is not None]
    if not with_time:
        return None
    return min(
        with_time,
        key=lambda row: abs((_safe_float(row.get("sim_time") or row.get("t")) or 0.0) - stamp_s),
    )


def _point_at_index(message: dict[str, Any], index: int | None) -> dict[str, Any] | None:
    if index is None:
        return None
    for point in message.get("trajectory_points_sample") or []:
        if point.get("index") == index:
            return point
    return None


def _first_stop_index(message: dict[str, Any]) -> int | None:
    stops = message.get("first_stop_velocity_indices") or []
    if not stops:
        return None
    try:
        return int(stops[0])
    except (TypeError, ValueError):
        return None


def _velocity_stats(message: dict[str, Any]) -> dict[str, float | None]:
    raw = message.get("trajectory_velocity_mps")
    if isinstance(raw, dict):
        return {
            "min": _safe_float(raw.get("min")),
            "max": _safe_float(raw.get("max")),
            "mean": _safe_float(raw.get("mean")),
            "first": _safe_float(raw.get("first")),
            "last": _safe_float(raw.get("last")),
        }
    return {"min": None, "max": None, "mean": None, "first": None, "last": None}


def _logger_status(jsonl_path: Path, log_path: Path) -> dict[str, Any]:
    messages = _read_jsonl(jsonl_path)
    log_text = log_path.read_text(errors="replace") if log_path.exists() else ""
    if messages:
        status = "messages_logged"
    elif not log_path.exists():
        status = "logger_not_started"
    elif "Logging " in log_text:
        status = "subscribed_no_messages"
    elif "force AnyMsg" in log_text or "fallback AnyMsg" in log_text:
        status = "anymsg_subscribed_no_messages"
    elif "Topic type for" in log_text and "not discovered yet" in log_text:
        status = "topic_type_not_discovered"
    elif "Failed to create subscription" in log_text:
        status = "subscription_failed"
    else:
        status = "no_messages"
    return {
        "jsonl": str(jsonl_path),
        "log": str(log_path),
        "message_count": len(messages),
        "status": status,
    }


def _analyze_causal_observability(run: Path) -> dict[str, Any]:
    artifacts = run / "artifacts"
    topics = {
        name: _logger_status(artifacts / paths["jsonl"], artifacts / paths["log"])
        for name, paths in CAUSAL_TOPIC_FILES.items()
    }
    missing = [
        name
        for name, info in topics.items()
        if info["status"]
        in {"logger_not_started", "topic_type_not_discovered", "subscription_failed", "no_messages"}
    ]
    return {
        "schema_version": "autoware_followstop_causal_observability.v1",
        "topics": topics,
        "missing_or_unusable_topics": missing,
    }


def _planning_factor_distance_profile(
    run: Path,
    *,
    near_distance_m: float = 15.0,
) -> dict[str, Any]:
    artifacts = run / "artifacts"
    topics: dict[str, Any] = {}
    for topic, filename in PLANNING_FACTOR_TOPIC_FILES.items():
        path = artifacts / filename
        messages = _read_jsonl(path)
        min_distance: float | None = None
        first_near: dict[str, Any] | None = None
        first_factor: dict[str, Any] | None = None
        for message_index, message in enumerate(messages):
            for factor in message.get("planning_factors_sample") or []:
                for point in factor.get("control_points_sample") or []:
                    distance = _safe_float(point.get("distance"))
                    if distance is not None:
                        min_distance = distance if min_distance is None else min(min_distance, distance)
                    event = {
                        "message_index": message_index,
                        "stamp_s": _stamp_s(message),
                        "module": factor.get("module"),
                        "behavior": factor.get("behavior"),
                        "detail": factor.get("detail"),
                        "distance_m": distance,
                        "pose": point.get("pose"),
                    }
                    if first_factor is None:
                        first_factor = event
                    if distance is not None and distance <= near_distance_m and first_near is None:
                        first_near = event
        topics[topic] = {
            "path": str(path),
            "message_count": len(messages),
            "min_control_point_distance_m": min_distance,
            "first_control_point": first_factor,
            "first_near_control_point": first_near,
        }
    return {
        "schema_version": "autoware_followstop_planning_factor_distance.v1",
        "near_distance_threshold_m": float(near_distance_m),
        "topics": topics,
        "obstacle_stop_near_ego": bool(
            (topics.get("obstacle_stop") or {}).get("first_near_control_point") is not None
        ),
    }


def _trajectory_stop_event(
    topic: str,
    message_index: int,
    message: dict[str, Any],
    rows: list[dict[str, str]],
) -> dict[str, Any] | None:
    first_stop_index = _first_stop_index(message)
    if first_stop_index is None:
        return None
    stamp_s = _stamp_s(message)
    nearest = _nearest_timeseries_row(rows, stamp_s)
    point = _point_at_index(message, first_stop_index)
    pose = (point or {}).get("pose") or {}
    stop_x = _safe_float(pose.get("x"))
    ego_x = _safe_float((nearest or {}).get("ego_x"))
    ego_speed = _safe_float((nearest or {}).get("ego_speed") or (nearest or {}).get("v_mps"))
    longitudinal_by_x_m = abs(ego_x - stop_x) if ego_x is not None and stop_x is not None else None
    return {
        "topic": topic,
        "message_index": message_index,
        "stamp_s": stamp_s,
        "trajectory_point_count": message.get("trajectory_point_count"),
        "velocity": _velocity_stats(message),
        "first_stop_index": first_stop_index,
        "first_stop_pose": pose or None,
        "first_stop_velocity_mps": _safe_float((point or {}).get("longitudinal_velocity_mps")),
        "first_stop_acceleration_mps2": _safe_float((point or {}).get("acceleration_mps2")),
        # Current Baguang evidence uses CARLA x and Autoware x in the same axis,
        # while y is frame-inverted. Keep this explicitly diagnostic instead of
        # pretending it is a full 2D metric.
        "ego_to_stop_abs_dx_m": longitudinal_by_x_m,
        "nearest_ego_x": ego_x,
        "nearest_ego_speed_mps": ego_speed,
    }


def _analyze_planning_speed_profile(
    run: Path,
    rows: list[dict[str, str]],
    *,
    near_ego_stop_dx_m: float = 15.0,
) -> dict[str, Any]:
    artifacts = run / "artifacts"
    topics: dict[str, Any] = {}
    first_near_stop: dict[str, Any] | None = None
    missing: list[str] = []
    for topic, filename in PLANNING_TOPIC_FILES.items():
        messages = _read_jsonl(artifacts / filename)
        if not messages:
            missing.append(topic)
            topics[topic] = {
                "path": str(artifacts / filename),
                "message_count": 0,
                "first_stop_event": None,
                "first_near_ego_stop_event": None,
            }
            continue
        first_stop: dict[str, Any] | None = None
        first_near: dict[str, Any] | None = None
        for idx, message in enumerate(messages):
            event = _trajectory_stop_event(topic, idx, message, rows)
            if event is None:
                continue
            if first_stop is None:
                first_stop = event
            dx = event.get("ego_to_stop_abs_dx_m")
            if dx is not None and dx <= near_ego_stop_dx_m:
                first_near = event
                if first_near_stop is None:
                    first_near_stop = event
                break
        topics[topic] = {
            "path": str(artifacts / filename),
            "message_count": len(messages),
            "first_stop_event": first_stop,
            "first_near_ego_stop_event": first_near,
            "last_velocity": _velocity_stats(messages[-1]),
        }
    return {
        "schema_version": "autoware_followstop_planning_speed_profile.v1",
        "near_ego_stop_dx_threshold_m": float(near_ego_stop_dx_m),
        "missing_topics": missing,
        "topics": topics,
        "first_near_ego_stop_event": first_near_stop,
        "classification": (
            "planning_stop_near_ego"
            if first_near_stop is not None
            else "no_near_ego_planning_stop_observed"
        ),
    }


def _looks_like_origin_placeholder(pose: dict[str, Any]) -> bool:
    x = _safe_float(pose.get("x"))
    y = _safe_float(pose.get("y"))
    z = _safe_float(pose.get("z"))
    yaw = _safe_float(pose.get("yaw_deg", pose.get("ego_heading")))
    values = [x, y, z, yaw]
    return all(value is not None and abs(value) <= 1e-6 for value in values)


def _ego_pose_from_row(row: dict[str, str]) -> dict[str, float | None]:
    return {
        "x": _safe_float(row.get("ego_x")),
        "y": _safe_float(row.get("ego_y")),
        "z": _safe_float(row.get("ego_z")),
        "yaw_deg": math.degrees(_safe_float(row.get("ego_heading")) or 0.0)
        if _safe_float(row.get("ego_heading")) is not None
        else None,
    }


def analyze_followstop_run(
    run_dir: str | Path,
    *,
    min_ahead_m: float = 20.0,
    max_ahead_m: float = 320.0,
    max_lateral_m: float = 4.0,
    max_heading_diff_deg: float = 35.0,
    stop_zone_m: float = 15.0,
    stopped_speed_mps: float = 1.0,
) -> dict[str, Any]:
    """Analyze whether a run actually represents a follow-stop scene."""

    run = Path(run_dir)
    scenario = _read_json(run / "artifacts" / "scenario_metadata.json")
    summary = _read_json(run / "summary.json")
    rows = _read_timeseries(run / "timeseries.csv")

    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "run_dir": str(run),
        "status": "insufficient_data",
        "failure_reasons": [],
        "warnings": [],
        "inputs": {
            "scenario_metadata": str(run / "artifacts" / "scenario_metadata.json"),
            "summary": str(run / "summary.json"),
            "timeseries": str(run / "timeseries.csv"),
        },
        "thresholds": {
            "min_ahead_m": float(min_ahead_m),
            "max_ahead_m": float(max_ahead_m),
            "max_lateral_m": float(max_lateral_m),
            "max_heading_diff_deg": float(max_heading_diff_deg),
            "stop_zone_m": float(stop_zone_m),
            "stopped_speed_mps": float(stopped_speed_mps),
        },
        "scene": {},
        "run_metrics": {},
        "verdict": {},
    }

    ego_spawn = scenario.get("spawn") or {}
    front_spawn = scenario.get("front_spawn") or {}
    if not ego_spawn or not front_spawn:
        report["failure_reasons"].append("scenario_spawn_metadata_missing")
        report["status"] = "insufficient_data"
        return report
    if _looks_like_origin_placeholder(front_spawn):
        report["failure_reasons"].append("front_spawn_origin_placeholder")
        report["warnings"].append(
            "scenario_metadata.front_spawn is exactly at origin; this usually means metadata was captured before CARLA committed the spawned actor pose"
        )
        report["status"] = "insufficient_data"
        return report

    initial_ego_pose = ego_spawn
    initial_pose_source = "scenario_metadata.spawn"
    if rows:
        row_pose = _ego_pose_from_row(rows[0])
        if row_pose.get("x") is not None and row_pose.get("y") is not None:
            initial_ego_pose = row_pose
            initial_pose_source = "timeseries.first_row"

    initial_relative = _relative_pose(initial_ego_pose, front_spawn)
    final_relative = None
    if rows:
        final_relative = _relative_pose(_ego_pose_from_row(rows[-1]), front_spawn)
    else:
        report["warnings"].append("timeseries_missing")

    speeds = [_safe_float(row.get("ego_speed") or row.get("v_mps")) for row in rows]
    speeds = [v for v in speeds if v is not None]
    final_speed = speeds[-1] if speeds else None
    min_lead = (
        ((summary.get("metrics") or {}).get("min_lead_distance_m"))
        or summary.get("min_lead_distance_m")
    )
    min_lead = _safe_float(min_lead)
    planning_speed_profile = _analyze_planning_speed_profile(run, rows)
    causal_observability = _analyze_causal_observability(run)
    planning_factor_distance = _planning_factor_distance_profile(run, near_distance_m=stop_zone_m)

    report["scene"] = {
        "front_idx": scenario.get("front_idx"),
        "ego_idx": scenario.get("ego_idx"),
        "ego_spawn": ego_spawn,
        "front_spawn": front_spawn,
        "initial_ego_pose_source": initial_pose_source,
        "initial_ego_pose": initial_ego_pose,
        "initial_front_relative_to_ego": initial_relative,
        "final_front_relative_to_ego": final_relative,
    }
    report["run_metrics"] = {
        "frames": summary.get("frames"),
        "exit_reason": summary.get("exit_reason"),
        "max_speed_mps": _safe_float(
            ((summary.get("metrics") or {}).get("max_speed_mps")) or summary.get("max_speed_mps")
        ),
        "final_speed_mps": final_speed,
        "min_lead_distance_m": min_lead,
        "collision_count": summary.get("collision_count"),
        "lane_invasion_count": summary.get("lane_invasion_count"),
    }
    report["planning_speed_profile"] = planning_speed_profile
    report["causal_observability"] = causal_observability
    report["planning_factor_distance"] = planning_factor_distance

    lon = initial_relative.get("longitudinal_m")
    lat = initial_relative.get("lateral_m")
    heading = initial_relative.get("heading_diff_deg")
    if lon is None or lat is None:
        report["failure_reasons"].append("front_relative_pose_unavailable")
    else:
        if lon < min_ahead_m:
            report["failure_reasons"].append("front_not_ahead_of_ego")
        if lon > max_ahead_m:
            report["failure_reasons"].append("front_too_far_ahead")
        if abs(lat) > max_lateral_m:
            report["failure_reasons"].append("front_lateral_misaligned")
        if heading is not None and abs(heading) > max_heading_diff_deg:
            report["failure_reasons"].append("front_heading_misaligned")

    reached_stop_zone = bool(min_lead is not None and min_lead <= stop_zone_m)
    stopped_in_stop_zone = bool(
        reached_stop_zone and final_speed is not None and final_speed <= stopped_speed_mps
    )
    report["verdict"] = {
        "front_initially_ahead": bool(lon is not None and lon >= min_ahead_m),
        "front_initially_same_lane": bool(lat is not None and abs(lat) <= max_lateral_m),
        "front_heading_aligned": bool(
            heading is None or abs(heading) <= max_heading_diff_deg
        ),
        "reached_stop_zone": reached_stop_zone,
        "stopped_in_stop_zone": stopped_in_stop_zone,
        "is_followstop_evidence": False,
    }

    if report["failure_reasons"]:
        report["status"] = "fail"
    elif not reached_stop_zone:
        report["status"] = "warn"
        report["failure_reasons"].append("front_stop_zone_not_reached")
        if planning_speed_profile.get("classification") == "planning_stop_near_ego":
            report["failure_reasons"].append("planning_stop_near_ego_before_front_stop_zone")
            obstacle_profile = (planning_factor_distance.get("topics") or {}).get("obstacle_stop") or {}
            if obstacle_profile.get("first_near_control_point") is None:
                report["warnings"].append("near_ego_stop_not_explained_by_obstacle_stop_factor")
        if causal_observability.get("missing_or_unusable_topics"):
            report["warnings"].append("causal_stop_reason_topics_missing_or_unusable")
    elif not stopped_in_stop_zone:
        report["status"] = "warn"
        report["failure_reasons"].append("ego_not_stopped_in_stop_zone")
    else:
        report["status"] = "pass"
        report["verdict"]["is_followstop_evidence"] = True

    return report


def write_followstop_report(report: dict[str, Any], out_dir: str | Path) -> dict[str, str]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "autoware_followstop_diagnostics.json"
    md_path = out / "autoware_followstop_diagnostics.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    md_path.write_text(render_followstop_markdown(report))
    return {"json": str(json_path), "markdown": str(md_path)}


def render_followstop_markdown(report: dict[str, Any]) -> str:
    scene = report.get("scene") or {}
    rel0 = scene.get("initial_front_relative_to_ego") or {}
    relf = scene.get("final_front_relative_to_ego") or {}
    metrics = report.get("run_metrics") or {}
    verdict = report.get("verdict") or {}
    speed_profile = report.get("planning_speed_profile") or {}
    stop_event = speed_profile.get("first_near_ego_stop_event") or {}
    factor_profile = report.get("planning_factor_distance") or {}
    obstacle_profile = (factor_profile.get("topics") or {}).get("obstacle_stop") or {}
    causal = report.get("causal_observability") or {}
    return "\n".join(
        [
            "# Autoware Follow-Stop Diagnostics",
            "",
            f"- status: `{report.get('status')}`",
            f"- failure_reasons: `{report.get('failure_reasons')}`",
            f"- run_dir: `{report.get('run_dir')}`",
            f"- ego_idx: `{scene.get('ego_idx')}`",
            f"- front_idx: `{scene.get('front_idx')}`",
            f"- initial_front_longitudinal_m: `{rel0.get('longitudinal_m')}`",
            f"- initial_front_lateral_m: `{rel0.get('lateral_m')}`",
            f"- initial_front_heading_diff_deg: `{rel0.get('heading_diff_deg')}`",
            f"- final_front_longitudinal_m: `{relf.get('longitudinal_m')}`",
            f"- final_front_lateral_m: `{relf.get('lateral_m')}`",
            f"- min_lead_distance_m: `{metrics.get('min_lead_distance_m')}`",
            f"- max_speed_mps: `{metrics.get('max_speed_mps')}`",
            f"- final_speed_mps: `{metrics.get('final_speed_mps')}`",
            f"- reached_stop_zone: `{verdict.get('reached_stop_zone')}`",
            f"- stopped_in_stop_zone: `{verdict.get('stopped_in_stop_zone')}`",
            f"- is_followstop_evidence: `{verdict.get('is_followstop_evidence')}`",
            f"- planning_speed_profile_classification: `{speed_profile.get('classification')}`",
            f"- first_near_ego_stop_topic: `{stop_event.get('topic')}`",
            f"- first_near_ego_stop_abs_dx_m: `{stop_event.get('ego_to_stop_abs_dx_m')}`",
            f"- obstacle_stop_min_control_point_distance_m: `{obstacle_profile.get('min_control_point_distance_m')}`",
            f"- obstacle_stop_near_ego: `{factor_profile.get('obstacle_stop_near_ego')}`",
            f"- causal_missing_or_unusable_topics: `{causal.get('missing_or_unusable_topics')}`",
            "",
        ]
    )
