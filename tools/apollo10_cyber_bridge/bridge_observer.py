from __future__ import annotations

import math
from typing import Any, Callable, Dict, Optional, Sequence, Tuple


ScalarResolver = Callable[[Any], Any]


def _finite_or_none(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _wrap_deg(angle_deg: float) -> float:
    return ((float(angle_deg) + 180.0) % 360.0) - 180.0


def build_timing_snapshot(
    *,
    latest_sim_time_sec: Optional[float],
    latest_wall_time_sec: Optional[float],
    latest_world_frame: Optional[int],
    latest_odom_stamp_sec: Optional[float],
    latest_odom_frame_id: str,
    bridge_tick_owner_model: str,
    bridge_is_tick_owner: bool,
    control_bridge_sync_to_world_tick: bool,
    bridge_timing_source_model: str,
    event_wall_time_sec: Optional[float] = None,
    control_cycle_time_sec: Optional[float] = None,
    latest_planning_age_ms: Optional[float] = None,
) -> Dict[str, Any]:
    return {
        "sim_time_sec": latest_sim_time_sec,
        "wall_time_sec": (
            float(event_wall_time_sec)
            if event_wall_time_sec is not None and math.isfinite(float(event_wall_time_sec))
            else latest_wall_time_sec
        ),
        "world_frame": latest_world_frame,
        "odom_header_stamp_sec": latest_odom_stamp_sec,
        "odom_frame_id": str(latest_odom_frame_id or ""),
        "tick_owner": str(bridge_tick_owner_model or ""),
        "bridge_is_tick_owner": bool(bridge_is_tick_owner),
        "control_bridge_sync_to_world_tick": bool(control_bridge_sync_to_world_tick),
        "timing_source_rule": str(bridge_timing_source_model or ""),
        "control_cycle_time_sec": control_cycle_time_sec,
        "latest_planning_age_ms": latest_planning_age_ms,
    }


def build_projection_debug_summary(projection: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    projection = dict(projection or {})
    if not projection:
        return {}
    return {
        "available": bool(projection.get("available", False)),
        "applied": bool(projection.get("applied", False)),
        "accepted": bool(projection.get("accepted", False)),
        "rejected": bool(projection.get("rejected", False)),
        "reason": str(projection.get("reason", "") or ""),
        "reject_reason": str(projection.get("reject_reason", "") or ""),
        "source_type": str(projection.get("source_type", "") or ""),
        "map_file": str(projection.get("map_file", "") or ""),
        "trusted_lane_centerline": bool(projection.get("trusted_lane_centerline", False)),
        "source_trusted_lane_centerline": bool(
            projection.get("source_trusted_lane_centerline", False)
        ),
        "distance_m": _finite_or_none(projection.get("distance_m")),
        "proj_x": _finite_or_none(projection.get("proj_x")),
        "proj_y": _finite_or_none(projection.get("proj_y")),
        "lane_yaw_deg": _finite_or_none(projection.get("lane_yaw_deg")),
        "signed_e_y_m": _finite_or_none(projection.get("signed_e_y_m")),
        "heading_diff_to_vehicle_deg": _finite_or_none(
            projection.get("heading_diff_to_vehicle_deg")
        ),
        "heading_diff_to_vehicle_deg_abs": _finite_or_none(
            projection.get("heading_diff_to_vehicle_deg_abs")
        ),
        "suspicious_snap_rejected": bool(projection.get("suspicious_snap_rejected", False)),
    }


def build_seed_pose_debug_summary(
    *,
    auto_routing_seed_pose: Optional[Sequence[float]],
    auto_routing_use_seed_heading: bool,
    current_x: float,
    current_y: float,
    current_yaw: float,
) -> Dict[str, Any]:
    if auto_routing_seed_pose is None:
        return {
            "available": False,
            "use_seed_heading": bool(auto_routing_use_seed_heading),
        }
    seed_x, seed_y, seed_yaw = auto_routing_seed_pose
    return {
        "available": True,
        "use_seed_heading": bool(auto_routing_use_seed_heading),
        "seed_x": float(seed_x),
        "seed_y": float(seed_y),
        "seed_yaw_deg": math.degrees(float(seed_yaw)),
        "current_x": float(current_x),
        "current_y": float(current_y),
        "current_yaw_deg": math.degrees(float(current_yaw)),
        "seed_to_current_distance_m": math.hypot(
            float(current_x) - float(seed_x),
            float(current_y) - float(seed_y),
        ),
        "seed_heading_delta_deg": _wrap_deg(math.degrees(float(current_yaw) - float(seed_yaw))),
    }


def build_route_debug_observability_snapshot(
    route_debug: Optional[Dict[str, Any]],
    *,
    scalar_resolver: ScalarResolver,
    age_sec: Optional[float] = None,
) -> Dict[str, Any]:
    route_debug = dict(route_debug or {})
    if not route_debug:
        return {}
    return {
        "timestamp": _finite_or_none(route_debug.get("timestamp")),
        "age_sec": _finite_or_none(age_sec),
        "route_debug_source": scalar_resolver(route_debug.get("route_debug_source")),
        "planning_header_sequence_num": (
            int(route_debug.get("planning_header_sequence_num"))
            if route_debug.get("planning_header_sequence_num") is not None
            else None
        ),
        "planning_header_timestamp_sec": _finite_or_none(
            route_debug.get("planning_header_timestamp_sec")
        ),
        "current_lane_id": scalar_resolver(route_debug.get("current_lane_id")),
        "lane_id_first": scalar_resolver(route_debug.get("lane_id_first")),
        "target_lane_id_first": scalar_resolver(route_debug.get("target_lane_id_first")),
        "reference_line_count": int(route_debug.get("reference_line_count", 0) or 0),
        "reference_line_length": _finite_or_none(route_debug.get("reference_line_length")),
        "route_segment_count": int(route_debug.get("route_segment_count", 0) or 0),
        "route_segment_total_length": _finite_or_none(route_debug.get("route_segment_total_length")),
        "routing_road_count": int(route_debug.get("routing_road_count", 0) or 0),
        "routing_passage_count": int(route_debug.get("routing_passage_count", 0) or 0),
        "routing_lane_window_count": int(route_debug.get("routing_lane_window_count", 0) or 0),
        "routing_lane_window_signature": str(route_debug.get("routing_lane_window_signature") or "none"),
        "routing_unique_lane_count": int(route_debug.get("routing_unique_lane_count", 0) or 0),
        "routing_unique_lane_signature": str(route_debug.get("routing_unique_lane_signature") or "none"),
        "reference_line_provider_status": scalar_resolver(
            route_debug.get("reference_line_provider_status")
        ),
        "create_route_segments_status": scalar_resolver(
            route_debug.get("create_route_segments_status")
        ),
        "lane_follow_map_status": scalar_resolver(route_debug.get("lane_follow_map_status")),
        "lane_follow_map_inconsistent": bool(route_debug.get("lane_follow_map_inconsistent", False)),
        "runtime_map_dir": scalar_resolver(route_debug.get("runtime_map_dir")),
        "planning_empty_reason_guess": scalar_resolver(
            route_debug.get("planning_empty_reason_guess")
        ),
    }


def build_bridge_observer_summary(
    *,
    planning_reader_enabled: bool,
    planning_channel: str,
    planning_message_type: str,
    timing: Dict[str, Any],
    health_summary_path: str,
    planning_topic_debug_summary_path: str,
    planning_route_segment_debug_path: str,
    apollo_map_runtime_debug_path: str,
    apollo_reference_line_debug_path: str,
    apollo_route_segment_debug_path: str,
    control_trajectory_consume_live_path: str,
    routing_event_debug_path: str,
    reroute_decision_debug_path: str,
    goal_validity_debug_path: str,
    lateral_guard_debug_path: str,
) -> Dict[str, Any]:
    return {
        "source": "bridge_observer",
        "planning_reader_enabled": bool(planning_reader_enabled),
        "planning_topic_name": str(planning_channel or ""),
        "planning_message_type": str(planning_message_type or ""),
        "timing_source_rule": str((timing or {}).get("timing_source_rule") or ""),
        "tick_owner": str((timing or {}).get("tick_owner") or ""),
        "artifact_paths": {
            "health_summary_path": str(health_summary_path),
            "planning_topic_debug_summary_path": str(planning_topic_debug_summary_path),
            "planning_route_segment_debug_path": str(planning_route_segment_debug_path),
            "apollo_map_runtime_debug_path": str(apollo_map_runtime_debug_path),
            "apollo_reference_line_debug_path": str(apollo_reference_line_debug_path),
            "apollo_route_segment_debug_path": str(apollo_route_segment_debug_path),
            "control_trajectory_consume_live_path": str(control_trajectory_consume_live_path),
            "routing_event_debug_path": str(routing_event_debug_path),
            "reroute_decision_debug_path": str(reroute_decision_debug_path),
            "goal_validity_debug_path": str(goal_validity_debug_path),
            "lateral_guard_debug_path": str(lateral_guard_debug_path),
        },
    }
