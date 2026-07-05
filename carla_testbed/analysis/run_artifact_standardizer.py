from __future__ import annotations

import csv
import json
import math
import shutil
from pathlib import Path
from typing import Any, Mapping, Sequence

import yaml

from carla_testbed.record.route_curve_context import route_definition_from_metadata
from carla_testbed.record.route_curve_fields import (
    ROUTE_CURVE_P0_FIELDS,
    ROUTE_CURVE_P1_FIELDS,
    ensure_route_curve_p0_fields,
)
from carla_testbed.routes.geometry import normalize_angle, project_onto_route
from carla_testbed.routes.schema import RouteDefinition, RoutePoint

RUN_ARTIFACT_STANDARDIZER_SCHEMA_VERSION = "run_artifact_standardizer.v1"

CAPABILITY_PROFILE_TO_SCENARIO_CLASS = {
    "lane_keep": "lane_keep",
    "curve_lane_follow": "curve_diagnostic",
    "junction_traverse": "junction_turn",
    "traffic_light_actual": "traffic_light_red_stop",
}


def standardize_legacy_run_artifacts(
    run_dir: str | Path,
    *,
    refresh: bool = False,
) -> dict[str, Any]:
    """Create standard diagnostic aliases for legacy Town01 route-health runs.

    This helper only derives files from existing run artifacts. It never marks a
    behavior as successful and it leaves unavailable fields empty so downstream
    gates can report insufficient_data.
    """
    root = Path(run_dir).expanduser()
    root.mkdir(parents=True, exist_ok=True)
    effective_cfg = _load_effective_config(root)
    summary = _read_json(root / "summary.json")
    scenario_meta = _read_json(root / "artifacts" / "scenario_metadata.json")

    actions: list[dict[str, Any]] = []
    _ensure_manifest(root, summary=summary, scenario_meta=scenario_meta, effective_cfg=effective_cfg, actions=actions)
    _ensure_route_json(root, scenario_meta=scenario_meta, actions=actions, refresh=refresh)
    _ensure_config_resolved(root, actions=actions, refresh=refresh)
    _ensure_timeseries_csv(root, summary=summary, scenario_meta=scenario_meta, actions=actions, refresh=refresh)
    _ensure_events_jsonl(root, summary=summary, actions=actions, refresh=refresh)

    return {
        "schema_version": RUN_ARTIFACT_STANDARDIZER_SCHEMA_VERSION,
        "run_dir": str(root),
        "actions": actions,
        "status": "updated" if actions else "no_change",
        "interpretation_boundary": (
            "Standardization only creates diagnostic aliases from existing artifacts. "
            "Missing values remain blank and must not be interpreted as pass evidence."
        ),
    }


def _ensure_manifest(
    root: Path,
    *,
    summary: Mapping[str, Any],
    scenario_meta: Mapping[str, Any],
    effective_cfg: Mapping[str, Any],
    actions: list[dict[str, Any]],
) -> None:
    path = root / "manifest.json"
    manifest = _read_json(path)
    before = dict(manifest)

    runtime_mode = summary.get("runtime_mode") if isinstance(summary.get("runtime_mode"), Mapping) else {}
    capability_profile = _first_nonempty(
        summary.get("capability_profile"),
        runtime_mode.get("capability_profile") if isinstance(runtime_mode, Mapping) else None,
        _nested_get(effective_cfg, "run", "capability_profile"),
        scenario_meta.get("capability_profile"),
    )
    manifest.setdefault("schema_version", "natural_driving_run_manifest.v1")
    _set_if_known(manifest, "run_id", summary.get("run_id") or _nested_get(effective_cfg, "run", "profile_name") or root.name)
    _set_if_known(manifest, "scenario_id", summary.get("scenario_id") or scenario_meta.get("scenario_id"))
    _set_if_known(manifest, "scenario_class", _scenario_class(summary, scenario_meta, effective_cfg, root.name))
    _set_if_known(manifest, "route_id", summary.get("route_id") or scenario_meta.get("route_id"))
    _set_if_known(manifest, "map", summary.get("map") or scenario_meta.get("map") or _nested_get(effective_cfg, "run", "map"))
    _set_if_known(
        manifest,
        "algorithm_variant_id",
        _nested_get(effective_cfg, "algo", "apollo", "algorithm_variant_id")
        or summary.get("algorithm_variant_id")
        or scenario_meta.get("algorithm_variant_id"),
    )
    _set_if_known(
        manifest,
        "algorithm_variant_manifest_path",
        _nested_get(effective_cfg, "algo", "apollo", "algorithm_variant_manifest_path")
        or summary.get("algorithm_variant_manifest_path")
        or scenario_meta.get("algorithm_variant_manifest_path"),
    )
    _set_if_known(
        manifest,
        "transport_mode",
        summary.get("transport_mode")
        or scenario_meta.get("transport_mode")
        or _nested_get(effective_cfg, "algo", "apollo", "transport_mode")
        or _transport_mode_from_effective_config(effective_cfg),
    )
    _set_if_known(
        manifest,
        "backend",
        summary.get("backend")
        or scenario_meta.get("backend")
        or _nested_get(effective_cfg, "backend", "name")
        or _backend_from_effective_config(effective_cfg),
    )
    truth_input = _truth_input_from_sources(summary, scenario_meta, effective_cfg)
    if truth_input is not None and manifest.get("truth_input") in {None, ""}:
        manifest["truth_input"] = truth_input
    if capability_profile and manifest.get("capability_profile") in {None, ""}:
        manifest["capability_profile"] = capability_profile

    if manifest != before or not path.exists():
        path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        actions.append({"artifact": "manifest.json", "action": "created_or_updated"})


def _ensure_route_json(
    root: Path,
    *,
    scenario_meta: Mapping[str, Any],
    actions: list[dict[str, Any]],
    refresh: bool,
) -> None:
    target = root / "route.json"
    if target.exists() and not refresh:
        return
    route = route_definition_from_metadata(scenario_meta)
    if route is None:
        return
    payload = {
        "route_id": route.route_id,
        "map": route.map_name,
        "source": route.source,
        "spawn_pose": route.spawn_pose,
        "goal_pose": route.goal_pose,
        "metadata": dict(route.metadata or {}),
        "points": [
            {
                "index": point.index,
                "x": point.x,
                "y": point.y,
                "z": point.z,
                "s": point.s,
                "heading": point.heading,
                "curvature": point.curvature,
                "lane_id": point.lane_id,
                "tags": list(point.tags),
            }
            for point in route.points
        ],
    }
    target.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    actions.append(
        {
            "artifact": "route.json",
            "action": "derived",
            "source": "artifacts/scenario_metadata.json:route_trace",
            "point_count": len(route.points),
        }
    )


def _ensure_config_resolved(root: Path, *, actions: list[dict[str, Any]], refresh: bool) -> None:
    target = root / "config.resolved.yaml"
    if target.exists() and not refresh:
        return
    source = _find_first(
        root,
        [
            "effective_config.yaml",
            "effective.yaml",
            "config/effective.yaml",
            "config/effective_config.yaml",
        ],
    )
    if source is None:
        return
    shutil.copyfile(source, target)
    actions.append({"artifact": "config.resolved.yaml", "action": "copied", "source": str(source)})


def _ensure_events_jsonl(
    root: Path,
    *,
    summary: Mapping[str, Any],
    actions: list[dict[str, Any]],
    refresh: bool,
) -> None:
    target = root / "events.jsonl"
    if target.exists() and not refresh:
        return
    source = _find_first(root, ["artifacts/town01_route_health_state_timeline.jsonl"])
    if source is None and not summary:
        return
    events: list[dict[str, Any]] = []
    if source is not None:
        for payload in _read_jsonl(source):
            event = {
                "event_type": "route_health_state",
                "state": payload.get("state"),
                "wall_time_s": payload.get("entered_ts_sec"),
                "reason": payload.get("reason"),
                "source": str(source),
            }
            events.append({key: value for key, value in event.items() if value is not None})
    events.extend(_summary_safety_events(root, summary))
    if not events:
        return
    _write_jsonl(target, events)
    actions.append(
        {
            "artifact": "events.jsonl",
            "action": "derived",
            "source": str(source) if source is not None else "summary.json",
            "event_count": len(events),
        }
    )


def _summary_safety_events(root: Path, summary: Mapping[str, Any]) -> list[dict[str, Any]]:
    exit_reason = str(summary.get("exit_reason") or summary.get("fail_reason") or "").strip()
    first_failure_step = _int_or_none(summary.get("first_failure_step"))
    row_context = _timeseries_row_context(root, first_failure_step)
    events: list[dict[str, Any]] = []
    if "LANE_INVASION" in exit_reason or _float_or_none(summary.get("lane_invasion_count")):
        events.append(
            _compact_event(
                {
                    "event_type": "lane_invasion",
                    "reason": exit_reason or "summary_lane_invasion_count",
                    "step": first_failure_step,
                    "lane_invasion_count": _int_or_none(summary.get("lane_invasion_count")),
                    "source": "summary.json",
                    **row_context,
                }
            )
        )
    if "COLLISION" in exit_reason or _float_or_none(summary.get("collision_count")):
        events.append(
            _compact_event(
                {
                    "event_type": "collision",
                    "reason": exit_reason or "summary_collision_count",
                    "step": first_failure_step,
                    "collision_count": _int_or_none(summary.get("collision_count")),
                    "source": "summary.json",
                    **row_context,
                }
            )
        )
    return events


def _timeseries_row_context(root: Path, step: int | None) -> dict[str, Any]:
    if step is None or step < 0:
        return {}
    source = _find_first(root, ["timeseries.csv", "artifacts/debug_timeseries.csv"])
    if source is None:
        return {}
    rows = _read_csv_rows(source)
    if step >= len(rows):
        return {}
    row = rows[step]
    return {
        "context_source": f"{source.name}:row_index_from_summary_step",
        "sim_time": _first_float(row, "sim_time", "t", "ts_sec"),
        "route_s": _first_float(row, "route_s"),
        "route_index": _int_or_none(row.get("route_index")),
        "nearest_route_index": _int_or_none(row.get("nearest_route_index")),
        "ego_x": _first_float(row, "ego_x", "map_x"),
        "ego_y": _first_float(row, "ego_y", "map_y"),
        "ego_speed": _first_float(row, "ego_speed", "speed_mps", "v_mps"),
        "cross_track_error": _first_float(row, "cross_track_error", "e_y_m", "apollo_debug_simple_lat_lateral_error_m"),
        "heading_error": _first_float(row, "heading_error", "apollo_debug_simple_lat_heading_error_rad")
        or _deg_to_rad(_first_float(row, "e_psi_deg")),
        "apollo_matched_point_distance": _first_float(row, "apollo_matched_point_distance"),
        "apollo_target_point_distance": _first_float(row, "apollo_target_point_distance"),
    }


def _compact_event(event: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in event.items() if value not in {None, ""}}


def _ensure_timeseries_csv(
    root: Path,
    *,
    summary: Mapping[str, Any],
    scenario_meta: Mapping[str, Any],
    actions: list[dict[str, Any]],
    refresh: bool,
) -> None:
    target = root / "timeseries.csv"
    if target.exists() and not refresh:
        return
    source = _find_first(root, ["artifacts/debug_timeseries.csv"])
    if source is None:
        return
    rows = _read_csv_rows(source)
    if not rows:
        return
    route_id = _first_nonempty(summary.get("route_id"), scenario_meta.get("route_id"))
    route = _route_for_debug_timeseries(route_definition_from_metadata(scenario_meta), rows)
    normalized = [_debug_row_to_timeseries(row, route_id=route_id, route=route) for row in rows]
    fieldnames = _fieldnames(rows, normalized)
    with target.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in normalized:
            writer.writerow({field: _csv_value(row.get(field)) for field in fieldnames})
    actions.append({"artifact": "timeseries.csv", "action": "derived", "source": str(source), "row_count": len(normalized)})


def _debug_row_to_timeseries(
    row: Mapping[str, Any],
    *,
    route_id: Any,
    route: RouteDefinition | None = None,
) -> dict[str, Any]:
    payload = dict(row)
    target_distance = _point_distance(
        row,
        x_names=(
            "apollo_target_point_x",
            "apollo_debug_simple_lat_target_point_x",
            "debug_simple_lat_target_point_x",
            "debug_simple_lat_current_target_point_x",
        ),
        y_names=(
            "apollo_target_point_y",
            "apollo_debug_simple_lat_target_point_y",
            "debug_simple_lat_target_point_y",
            "debug_simple_lat_current_target_point_y",
        ),
    )
    matched_distance = _point_distance(
        row,
        x_names=(
            "apollo_matched_point_x",
            "apollo_debug_simple_lon_matched_point_x",
            "debug_simple_lon_matched_point_x",
            "debug_simple_lon_current_matched_point_x",
            "apollo_debug_simple_mpc_matched_point_x",
            "debug_simple_mpc_matched_point_x",
            "debug_simple_mpc_current_matched_point_x",
        ),
        y_names=(
            "apollo_matched_point_y",
            "apollo_debug_simple_lon_matched_point_y",
            "debug_simple_lon_matched_point_y",
            "debug_simple_lon_current_matched_point_y",
            "apollo_debug_simple_mpc_matched_point_y",
            "debug_simple_mpc_matched_point_y",
            "debug_simple_mpc_current_matched_point_y",
        ),
    )
    route_context = _route_context_for_debug_row(row, route)
    ego_heading = _first_float(row, "ego_heading", "heading_rad")
    if ego_heading is None:
        ego_heading = _deg_to_rad(_first_float(row, "map_yaw_deg", "yaw_deg"))
    projected_heading_error = _heading_error_from_route_context(ego_heading, route_context)
    observed_heading_error = _first_float(row, "heading_error", "apollo_debug_simple_lat_heading_error_rad")
    if observed_heading_error is None:
        observed_heading_error = _deg_to_rad(_first_float(row, "e_psi_deg"))
    payload.update(
        {
            "sim_time": _first_float(row, "sim_time", "t", "ts_sec"),
            "frame_id": _first_nonempty(row.get("frame_id"), row.get("frame")),
            "route_id": _first_nonempty(row.get("route_id"), route_id),
            **route_context,
            "route_curvature": _first_float(row, "route_curvature", "target_curvature", "apollo_debug_simple_lat_curvature"),
            "ego_x": _first_float(row, "ego_x", "map_x"),
            "ego_y": _first_float(row, "ego_y", "map_y"),
            "ego_z": _first_float(row, "ego_z", "map_z"),
            "ego_heading": ego_heading,
            "ego_speed": _first_float(row, "ego_speed", "speed_mps", "v_mps"),
            "cross_track_error": _first_float(
                row,
                "cross_track_error",
                "e_y_m",
                "apollo_debug_simple_lat_lateral_error_m",
            )
            if _first_float(
                row,
                "cross_track_error",
                "e_y_m",
                "apollo_debug_simple_lat_lateral_error_m",
            )
            is not None
            else route_context.get("cross_track_error"),
            "heading_error": projected_heading_error
            if projected_heading_error is not None
            else observed_heading_error,
            "curvature_at_nearest": _first_float(
                row,
                "curvature_at_nearest",
                "target_curvature",
                "apollo_debug_simple_lat_curvature",
            ),
            "apollo_steer_raw": _first_float(row, "apollo_steer_raw", "raw_steer", "apollo_desired_steer"),
            "bridge_steer_mapped": _first_float(
                row,
                "mapped_carla_steer_cmd",
                "bridge_steer_mapped",
                "commanded_steer",
            ),
            "carla_steer_applied": _first_float(row, "carla_steer_applied", "measured_steer", "applied_steer"),
            "throttle_raw": _first_float(row, "throttle_raw", "apollo_desired_throttle"),
            "throttle_mapped": _first_float(row, "throttle_mapped", "mapped_throttle_cmd", "commanded_throttle"),
            "throttle_applied": _first_float(row, "throttle_applied", "measured_throttle", "applied_throttle"),
            "brake_raw": _first_float(row, "brake_raw", "apollo_desired_brake"),
            "brake_mapped": _first_float(row, "brake_mapped", "mapped_brake_cmd", "commanded_brake"),
            "brake_applied": _first_float(row, "brake_applied", "measured_brake", "applied_brake"),
            "lateral_guard_applied": _any_bool(
                row,
                "lateral_guard_applied",
                "sustained_lateral_guard_applied",
                "low_speed_steer_guard_applied",
                "low_speed_sustained_guard_applied",
                "force_zero_steer_applied",
            ),
            "trajectory_contract_guard_applied": _first_bool(
                row,
                "trajectory_contract_guard_applied",
                "trajectory_contract_lateral_guard_applied",
            ),
            "apollo_trajectory_heading": _first_float(
                row,
                "apollo_trajectory_heading",
                "apollo_debug_simple_lat_target_point_theta_rad",
                "debug_simple_lat_target_point_theta",
                "debug_simple_lat_current_target_point_theta",
            ),
            "apollo_trajectory_curvature": _first_float(
                row,
                "apollo_trajectory_curvature",
                "apollo_debug_simple_lat_target_point_kappa",
                "debug_simple_lat_target_point_kappa",
                "debug_simple_lat_current_target_point_kappa",
            ),
            "apollo_matched_point_distance": _first_float(
                row,
                "apollo_matched_point_distance",
                "matched_point_distance",
                "apollo_debug_simple_lon_matched_point_distance",
                "debug_simple_lon_matched_point_distance",
                "apollo_debug_simple_mpc_matched_point_distance",
                "debug_simple_mpc_matched_point_distance",
            )
            if _first_float(
                row,
                "apollo_matched_point_distance",
                "matched_point_distance",
                "apollo_debug_simple_lon_matched_point_distance",
                "debug_simple_lon_matched_point_distance",
                "apollo_debug_simple_mpc_matched_point_distance",
                "debug_simple_mpc_matched_point_distance",
            )
            is not None
            else matched_distance,
            "apollo_target_point_distance": _first_float(
                row,
                "apollo_target_point_distance",
                "target_point_distance",
                "apollo_debug_simple_lat_target_point_distance",
                "debug_simple_lat_target_point_distance",
            )
            if _first_float(
                row,
                "apollo_target_point_distance",
                "target_point_distance",
                "apollo_debug_simple_lat_target_point_distance",
                "debug_simple_lat_target_point_distance",
            )
            is not None
            else target_distance,
            "apollo_target_point_s": _first_float(
                row,
                "apollo_target_point_s",
                "apollo_debug_simple_lat_target_point_s",
                "debug_simple_lat_target_point_s",
                "debug_simple_lat_current_target_point_s",
            ),
            "apollo_matched_point_s": _first_float(
                row,
                "apollo_matched_point_s",
                "apollo_debug_simple_lon_matched_point_s",
                "debug_simple_lon_matched_point_s",
                "debug_simple_lon_current_matched_point_s",
                "apollo_debug_simple_mpc_matched_point_s",
                "debug_simple_mpc_matched_point_s",
                "debug_simple_mpc_current_matched_point_s",
            ),
            "apollo_target_point_relative_time_sec": _first_float(
                row,
                "apollo_target_point_relative_time_sec",
                "apollo_debug_simple_lat_target_point_relative_time_sec",
                "debug_simple_lat_target_point_relative_time_sec",
                "debug_simple_lat_current_target_point_relative_time",
            ),
            "localization_timestamp": _first_float(
                row,
                "localization_timestamp",
                "localization_timestamp_sec",
                "localization_ts_sec",
            ),
            "chassis_timestamp": _first_float(
                row,
                "chassis_timestamp",
                "chassis_timestamp_sec",
                "chassis_ts_sec",
            ),
            "planning_timestamp": _first_float(
                row,
                "planning_timestamp",
                "planning_header_timestamp_sec_used",
                "latest_planning_msg_timestamp",
                "planning_lateral_latest_timestamp_sec",
            ),
            "control_timestamp": _first_float(
                row,
                "control_timestamp",
                "control_tx_timestamp",
                "control_tx_timestamp_sec",
            ),
            "control_rx_timestamp": _first_float(
                row,
                "control_rx_timestamp",
                "control_rx_timestamp_sec",
            ),
            "control_latency_ms": _first_float(
                row,
                "control_latency_ms",
                "control_bridge_latency_ms",
                "bridge_control_latency_ms",
            ),
            "planning_message_age_ms": _first_float(
                row,
                "planning_message_age_ms",
                "latest_planning_msg_age_ms",
                "planning_lateral_latest_age_ms",
            ),
            "control_message_age_ms": _first_float(
                row,
                "control_message_age_ms",
                "control_msg_age_ms",
            ),
        }
    )
    for field in ROUTE_CURVE_P1_FIELDS:
        payload.setdefault(field, None)
    return ensure_route_curve_p0_fields(payload)


def _route_for_debug_timeseries(
    route: RouteDefinition | None,
    rows: Sequence[Mapping[str, Any]],
) -> RouteDefinition | None:
    if route is None or not rows:
        return route
    first_row = rows[0]
    ego_y = _first_float(first_row, "ego_y", "map_y", "y", "position_y")
    route_y = None
    if route.spawn_pose:
        route_y = _float_or_none(route.spawn_pose.get("y"))
    if route_y is None and route.points:
        route_y = route.points[0].y
    if ego_y is None or route_y is None:
        return route
    if abs(ego_y) <= 1e-6 or abs(route_y) <= 1e-6:
        return route
    if ego_y * route_y >= 0:
        return route
    return _mirror_route_y(route)


def _mirror_route_y(route: RouteDefinition) -> RouteDefinition:
    return RouteDefinition(
        route_id=route.route_id,
        map_name=route.map_name,
        source=f"{route.source}:y_mirrored_for_debug_timeseries",
        points=[
            RoutePoint(
                index=point.index,
                x=point.x,
                y=-point.y,
                z=point.z,
                s=point.s,
                heading=None if point.heading is None else normalize_angle(-float(point.heading)),
                curvature=None if point.curvature is None else -float(point.curvature),
                lane_id=point.lane_id,
                tags=list(point.tags),
            )
            for point in route.points
        ],
        spawn_pose=_mirror_pose_y(route.spawn_pose),
        goal_pose=_mirror_pose_y(route.goal_pose),
        metadata=dict(route.metadata or {}),
    )


def _mirror_pose_y(pose: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(pose, Mapping):
        return None
    mirrored = dict(pose)
    y = _float_or_none(mirrored.get("y"))
    if y is not None:
        mirrored["y"] = -y
    if _float_or_none(mirrored.get("yaw")) is not None:
        mirrored["yaw"] = normalize_angle(-float(mirrored["yaw"]))
    if _float_or_none(mirrored.get("heading")) is not None:
        mirrored["heading"] = normalize_angle(-float(mirrored["heading"]))
    if _float_or_none(mirrored.get("yaw_deg")) is not None:
        mirrored["yaw_deg"] = -float(mirrored["yaw_deg"])
    if _float_or_none(mirrored.get("heading_deg")) is not None:
        mirrored["heading_deg"] = -float(mirrored["heading_deg"])
    return mirrored


def _route_context_for_debug_row(
    row: Mapping[str, Any],
    route: RouteDefinition | None,
) -> dict[str, Any]:
    empty = {
        "route_index": _first_nonempty(row.get("route_index")),
        "nearest_route_index": _first_nonempty(row.get("nearest_route_index")),
        "route_s": _first_nonempty(row.get("route_s")),
        "route_x": _first_nonempty(row.get("route_x")),
        "route_y": _first_nonempty(row.get("route_y")),
        "route_z": _first_nonempty(row.get("route_z")),
        "route_heading": _first_nonempty(row.get("route_heading")),
    }
    if route is None:
        return empty
    ego_x = _first_float(row, "ego_x", "map_x", "x", "position_x")
    ego_y = _first_float(row, "ego_y", "map_y", "y", "position_y")
    if ego_x is None or ego_y is None:
        return empty
    projection = project_onto_route(route.points, ego_x, ego_y, extend_ends=True)
    if projection is None:
        return empty
    point = _point_by_route_index(route, projection.nearest_index)
    if point is None:
        return empty
    return {
        "route_index": projection.segment_start_index if projection.segment_start_index is not None else point.index,
        "nearest_route_index": projection.nearest_index,
        "route_s": projection.s,
        "route_x": projection.x,
        "route_y": projection.y,
        "route_z": projection.z,
        "route_heading": projection.heading,
        "route_projection_t": projection.t,
        "route_segment_start_index": projection.segment_start_index,
        "route_segment_end_index": projection.segment_end_index,
        "cross_track_error": projection.cross_track_error,
    }


def _heading_error_from_route_context(ego_heading: float | None, route_context: Mapping[str, Any]) -> float | None:
    route_heading = _float_or_none(route_context.get("route_heading"))
    if ego_heading is None or route_heading is None:
        return None
    return normalize_angle(float(ego_heading) - float(route_heading))


def _point_by_route_index(route: RouteDefinition, index: int) -> RoutePoint | None:
    for point in route.points:
        if int(point.index) == int(index):
            return point
    if 0 <= int(index) < len(route.points):
        return route.points[int(index)]
    return None


def _scenario_class(
    summary: Mapping[str, Any],
    scenario_meta: Mapping[str, Any],
    effective_cfg: Mapping[str, Any],
    name: str,
) -> str | None:
    direct = _first_nonempty(summary.get("scenario_class"), scenario_meta.get("scenario_class"))
    if direct:
        return str(direct)
    profile = _first_nonempty(
        summary.get("capability_profile"),
        _nested_get(effective_cfg, "run", "capability_profile"),
        scenario_meta.get("capability_profile"),
    )
    if profile in CAPABILITY_PROFILE_TO_SCENARIO_CLASS:
        return CAPABILITY_PROFILE_TO_SCENARIO_CLASS[str(profile)]
    text = name.lower()
    if "traffic_light_red_to_green" in text:
        return "traffic_light_red_to_green_release"
    if "traffic_light" in text or "red_stop" in text:
        return "traffic_light_red_stop"
    if "junction" in text:
        return "junction_turn"
    if "curve" in text:
        return "curve_diagnostic"
    if "lane" in text:
        return "lane_keep"
    return None


def _transport_mode_from_effective_config(cfg: Mapping[str, Any]) -> str | None:
    if _nested_get(cfg, "algo", "apollo", "direct_bridge", "enabled") is True:
        return "carla_direct"
    if _nested_get(cfg, "scenario", "publish_ros2_gt") is True:
        return "ros2_gt"
    return None


def _backend_from_effective_config(cfg: Mapping[str, Any]) -> str | None:
    if str(_nested_get(cfg, "algo", "stack") or "").strip().lower() == "apollo":
        return "apollo_cyberrt"
    return None


def _truth_input_from_sources(
    summary: Mapping[str, Any],
    scenario_meta: Mapping[str, Any],
    cfg: Mapping[str, Any],
) -> bool | None:
    for source in (summary, scenario_meta):
        value = source.get("truth_input")
        if isinstance(value, bool):
            return value
    if str(_nested_get(cfg, "algo", "stack") or "").strip().lower() == "apollo" and (
        _nested_get(cfg, "scenario", "gt") is not None
        or _nested_get(cfg, "scenario", "publish_ros2_gt") is True
        or _nested_get(cfg, "algo", "apollo", "direct_bridge", "enabled") is True
    ):
        return True
    return None


def _load_effective_config(root: Path) -> dict[str, Any]:
    path = _find_first(root, ["effective_config.yaml", "effective.yaml", "config.resolved.yaml"])
    if path is None:
        return {}
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
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


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(dict(row), sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def _read_csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _fieldnames(original_rows: Sequence[Mapping[str, Any]], normalized_rows: Sequence[Mapping[str, Any]]) -> list[str]:
    names: list[str] = []
    for row in [*(original_rows[:1] or []), *(normalized_rows[:1] or [])]:
        for key in row.keys():
            if key not in names:
                names.append(str(key))
    for field in ROUTE_CURVE_P0_FIELDS:
        if field not in names:
            names.append(field)
    for field in ROUTE_CURVE_P1_FIELDS:
        if normalized_rows and field in normalized_rows[0]:
            if field not in names:
                names.append(field)
    for field in (
        "localization_timestamp",
        "chassis_timestamp",
        "planning_timestamp",
        "control_timestamp",
        "control_latency_ms",
        "planning_message_age_ms",
        "control_message_age_ms",
        "control_rx_timestamp",
    ):
        if any(field in row for row in normalized_rows) and field not in names:
            names.append(field)
    for field in (
        "apollo_target_point_s",
        "apollo_target_point_relative_time_sec",
        "apollo_matched_point_s",
    ):
        if any(field in row for row in normalized_rows) and field not in names:
            names.append(field)
    return names


def _find_first(root: Path, relative_paths: Sequence[str]) -> Path | None:
    for relative in relative_paths:
        path = root / relative
        if path.exists():
            return path
    return None


def _nested_get(source: Mapping[str, Any], *parts: str) -> Any:
    cursor: Any = source
    for part in parts:
        if not isinstance(cursor, Mapping) or part not in cursor:
            return None
        cursor = cursor[part]
    return cursor


def _set_if_known(target: dict[str, Any], key: str, value: Any) -> None:
    if target.get(key) not in {None, ""}:
        return
    if value in {None, ""}:
        return
    target[key] = value


def _first_nonempty(*values: Any) -> Any:
    for value in values:
        if value not in {None, ""}:
            return value
    return None


def _first_float(source: Mapping[str, Any], *names: str) -> float | None:
    for name in names:
        value = _float_or_none(source.get(name))
        if value is not None:
            return value
    return None


def _point_distance(
    source: Mapping[str, Any],
    *,
    x_names: Sequence[str],
    y_names: Sequence[str],
) -> float | None:
    ego_x = _first_float(source, "ego_x", "map_x", "x", "position_x")
    ego_y = _first_float(source, "ego_y", "map_y", "y", "position_y")
    point_x = _first_float(source, *x_names)
    point_y = _first_float(source, *y_names)
    if ego_x is None or ego_y is None or point_x is None or point_y is None:
        return None
    return math.hypot(float(point_x) - float(ego_x), float(point_y) - float(ego_y))


def _float_or_none(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _int_or_none(value: Any) -> int | None:
    number = _float_or_none(value)
    return None if number is None else int(number)


def _deg_to_rad(value: float | None) -> float | None:
    return None if value is None else math.radians(float(value))


def _first_bool(source: Mapping[str, Any], *names: str) -> bool | None:
    for name in names:
        value = _bool_or_none(source.get(name))
        if value is not None:
            return value
    return None


def _any_bool(source: Mapping[str, Any], *names: str) -> bool | None:
    seen = False
    for name in names:
        value = _bool_or_none(source.get(name))
        if value is None:
            continue
        seen = True
        if value:
            return True
    return False if seen else None


def _bool_or_none(value: Any) -> bool | None:
    if value in {None, ""}:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def _csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, float) and not math.isfinite(value):
        return ""
    text = str(value)
    return "" if text.lower() in {"nan", "inf", "-inf"} else value
