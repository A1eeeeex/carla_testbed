from __future__ import annotations

import csv
import copy
import json
import math
import sys
import time
from pathlib import Path
from typing import Any, Mapping

import yaml

from carla_testbed.analysis.fixed_scene_contract import (
    analyze_fixed_scene_contract_run_dir,
    write_fixed_scene_contract_report,
)
from carla_testbed.analysis.phase1_postprocess import run_phase1_postprocess
from carla_testbed.analysis.scenario_actor_contract import (
    analyze_scenario_actor_contract_run_dir,
    write_scenario_actor_contract_report,
)
from carla_testbed.contracts import ChassisState, EgoState, FrameStamp, ObstacleTruth, Pose3D, Quaternion, SceneTruth, Vector3D
from carla_testbed.control import SimpleAccRouteFollowerConfig, SimpleAccRouteFollowerController, apply_control_to_vehicle
from carla_testbed.scenario_player.carla_runtime import CarlaFixedSceneRuntime, _offset_transform
from carla_testbed.scenario_player.compiler import compile_fixed_scene_template
from carla_testbed.scenario_player.manifest_contract import fixed_scene_manifest_fields_from_storyboard
from carla_testbed.scenario_player.schema import load_fixed_scene_template
from carla_testbed.scenario_player.target_actor import resolve_target_actor_contract
from carla_testbed.scenarios.followstop_geometry import select_waypoint_ahead_transform


def run_builtin_ego_scenario(
    *,
    template_path: str | Path,
    run_dir: str | Path,
    host: str = "localhost",
    port: int = 2000,
    town: str | None = "Town01",
    duration_s: float | None = None,
    fixed_dt_s: float = 0.05,
    ego_spawn_index: int | None = None,
    ego_spawn_s_offset_m: float | None = None,
    target_speed_mps: float | None = None,
    follow_spectator: bool = False,
    spectator_distance: float = 14.0,
    spectator_height: float = 5.0,
    spectator_pitch: float = -18.0,
    realtime: bool = False,
) -> dict[str, Any]:
    """Run a CARLA-only diagnostic scenario with the builtin ego controller.

    Fixed-scene scenarios delegate to the existing fixed-scene runner. Route-only
    scenarios write the same Phase 1 artifact spine but mark v-t-gap as not
    applicable through a not-required target actor contract.
    """

    scenario = _load_scenario_mapping(template_path)
    if isinstance(scenario.get("fixed_scene"), Mapping) or scenario.get("schema_version") == "fixed_scene_template.v1":
        return run_builtin_ego_fixed_scene(
            template_path=template_path,
            run_dir=run_dir,
            host=host,
            port=port,
            town=town,
            duration_s=duration_s,
            fixed_dt_s=fixed_dt_s,
            ego_spawn_index=ego_spawn_index,
            ego_spawn_s_offset_m=ego_spawn_s_offset_m,
            target_speed_mps=target_speed_mps,
            follow_spectator=follow_spectator,
            spectator_distance=spectator_distance,
            spectator_height=spectator_height,
            spectator_pitch=spectator_pitch,
            realtime=realtime,
        )
    return _run_builtin_ego_route_only(
        scenario=scenario,
        template_path=template_path,
        run_dir=run_dir,
        host=host,
        port=port,
        town=town or scenario.get("map") or "Town01",
        duration_s=duration_s,
        fixed_dt_s=fixed_dt_s,
        ego_spawn_index=ego_spawn_index,
        ego_spawn_s_offset_m=ego_spawn_s_offset_m,
        target_speed_mps=target_speed_mps,
        follow_spectator=follow_spectator,
        spectator_distance=spectator_distance,
        spectator_height=spectator_height,
        spectator_pitch=spectator_pitch,
        realtime=realtime,
    )


def _run_builtin_ego_route_only(
    *,
    scenario: Mapping[str, Any],
    template_path: str | Path,
    run_dir: str | Path,
    host: str,
    port: int,
    town: str | None,
    duration_s: float | None,
    fixed_dt_s: float,
    ego_spawn_index: int | None,
    ego_spawn_s_offset_m: float | None,
    target_speed_mps: float | None,
    follow_spectator: bool,
    spectator_distance: float,
    spectator_height: float,
    spectator_pitch: float,
    realtime: bool,
) -> dict[str, Any]:
    carla = _import_carla()
    run_root = Path(run_dir).expanduser()
    artifacts = run_root / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)
    route = scenario.get("route") if isinstance(scenario.get("route"), Mapping) else {}
    scenario_id = str(scenario.get("scenario_id") or Path(template_path).stem)
    scenario_class = str(scenario.get("scenario_class") or "route_only")
    target_contract = resolve_target_actor_contract(scenario)
    duration = float(duration_s or _scenario_duration_s(scenario) or 30.0)
    target_speed = float(target_speed_mps or _scenario_target_speed_mps(scenario) or 13.89)
    spawn_index = ego_spawn_index if ego_spawn_index is not None else _spawn_index_from_ref(str(route.get("spawn_ref") or ""))
    spawn_s_offset_m = (
        float(ego_spawn_s_offset_m)
        if ego_spawn_s_offset_m is not None
        else _route_ego_spawn_s_offset_m(scenario)
    )

    client = carla.Client(host, int(port))
    client.set_timeout(30.0)
    world = client.load_world(town) if town else client.get_world()
    original_settings = world.get_settings()
    sync_settings = world.get_settings()
    sync_settings.synchronous_mode = True
    sync_settings.fixed_delta_seconds = float(fixed_dt_s)
    world.apply_settings(sync_settings)

    ego = None
    controller = SimpleAccRouteFollowerController(SimpleAccRouteFollowerConfig(target_speed_mps=target_speed))
    cleanup_errors: list[str] = []
    trace_path = artifacts / "ego_control_trace.jsonl"
    timeseries_path = run_root / "timeseries.csv"
    events_path = run_root / "events.jsonl"
    start_wall = time.time()
    ticks = 0
    runtime_status = "pass"
    safety_tracker: _SafetyEventTracker | None = None
    try:
        _append_event(events_path, {"event": "run_started", "wall_time_s": start_wall})
        ego = _spawn_ego(world, spawn_index=spawn_index, spawn_s_offset_m=spawn_s_offset_m)
        # Let CARLA settle the newly spawned ego before safety sensors start
        # producing claimable run events. Spawn-time lane-invasion callbacks are
        # setup provenance, not backend behavior evidence.
        _tick_world_if_available(world)
        route_plan = _build_route_plan(world, ego, scenario)
        safety_tracker = _SafetyEventTracker(world=world, ego=ego, artifact_dir=artifacts)
        with trace_path.open("w", encoding="utf-8") as trace_fh, timeseries_path.open(
            "w", encoding="utf-8", newline=""
        ) as ts_fh:
            writer = csv.DictWriter(
                ts_fh,
                fieldnames=[
                    "sim_time",
                    "frame_id",
                    "ego_speed_mps",
                    "ego_x",
                    "ego_y",
                    "ego_yaw_rad",
                    "ego_route_s",
                    "ego_lane_id",
                    "ego_length_m",
                    "ego_width_m",
                    "ego_height_m",
                    "bbox_extent_x_m",
                    "bbox_extent_y_m",
                    "bbox_extent_z_m",
                    "heading_error_rad",
                    "route_lookahead_heading_error_rad",
                    "cross_track_error_m",
                    "collision_count",
                    "lane_invasion_count",
                    "collision_sensor_available",
                    "lane_invasion_sensor_available",
                    "throttle",
                    "brake",
                    "steer",
                    "control_source",
                ],
            )
            writer.writeheader()
            while ticks * float(fixed_dt_s) < duration:
                frame_id = world.tick()
                ticks += 1
                sim_time = ticks * float(fixed_dt_s)
                stamp = FrameStamp(frame_id=int(frame_id) if frame_id is not None else ticks, sim_time_s=sim_time)
                ego_state = _ego_state_from_actor(world, ego, stamp, route_plan=route_plan)
                scene = SceneTruth(stamp=stamp, ego=ego_state, obstacles=tuple(), metadata=dict(ego_state.metadata))
                if follow_spectator:
                    _spectator_follow(
                        world,
                        ego,
                        distance_m=spectator_distance,
                        height_m=spectator_height,
                        pitch_deg=spectator_pitch,
                    )
                command = controller.step(stamp, ego_state, scene)
                apply_result = apply_control_to_vehicle(ego, command, stamp=stamp)
                safety_snapshot = safety_tracker.snapshot() if safety_tracker is not None else _empty_safety_snapshot()
                trace_fh.write(
                    json.dumps(
                        {
                            "schema_version": "ego_control_trace.v1",
                            "sim_time": sim_time,
                            "frame_id": stamp.frame_id,
                            "controller": controller.name,
                            "command": command.to_dict(),
                            "apply_result": apply_result.to_dict(),
                        },
                        sort_keys=True,
                    )
                    + "\n"
                )
                ego_pos = ego_state.pose.position
                ego_dimensions = _actor_dimensions(ego)
                writer.writerow(
                    {
                        "sim_time": sim_time,
                        "frame_id": stamp.frame_id,
                        "ego_speed_mps": command.metadata.get("ego_speed_mps"),
                        "ego_x": ego_pos.x,
                        "ego_y": ego_pos.y,
                        "ego_yaw_rad": _yaw_from_actor(ego),
                        "ego_route_s": ego_state.metadata.get("route_s"),
                        "ego_lane_id": ego_state.metadata.get("lane_id"),
                        "ego_length_m": ego_dimensions.get("length_m"),
                        "ego_width_m": ego_dimensions.get("width_m"),
                        "ego_height_m": ego_dimensions.get("height_m"),
                        "bbox_extent_x_m": ego_dimensions.get("bbox_extent_x_m"),
                        "bbox_extent_y_m": ego_dimensions.get("bbox_extent_y_m"),
                        "bbox_extent_z_m": ego_dimensions.get("bbox_extent_z_m"),
                        "heading_error_rad": ego_state.metadata.get("heading_error_rad"),
                        "route_lookahead_heading_error_rad": ego_state.metadata.get(
                            "route_lookahead_heading_error_rad"
                        ),
                        "cross_track_error_m": ego_state.metadata.get("cross_track_error_m"),
                        "collision_count": safety_snapshot["collision_count"],
                        "lane_invasion_count": safety_snapshot["lane_invasion_count"],
                        "collision_sensor_available": safety_snapshot["collision_sensor_available"],
                        "lane_invasion_sensor_available": safety_snapshot["lane_invasion_sensor_available"],
                        "throttle": command.throttle,
                        "brake": command.brake,
                        "steer": command.steer,
                        "control_source": command.source,
                    }
                )
                if realtime:
                    _sleep_until(start_wall + ticks * float(fixed_dt_s))
    except Exception as exc:
        runtime_status = "fail"
        cleanup_errors.append(f"runtime_failed:{type(exc).__name__}: {exc}")
        raise
    finally:
        if safety_tracker is not None:
            cleanup_errors.extend(safety_tracker.destroy())
        if ego is not None:
            try:
                ego.destroy()
            except Exception as exc:  # pragma: no cover - defensive CARLA cleanup
                cleanup_errors.append(f"ego_destroy_failed:{type(exc).__name__}: {exc}")
        try:
            world.apply_settings(original_settings)
        except Exception as exc:  # pragma: no cover - defensive CARLA cleanup
            cleanup_errors.append(f"restore_world_settings_failed:{type(exc).__name__}: {exc}")

    end_wall = time.time()
    _append_event(events_path, {"event": "run_finished", "wall_time_s": end_wall, "ticks": ticks})
    safety_summary = safety_tracker.snapshot() if safety_tracker is not None else _empty_safety_snapshot()
    manifest = {
        "schema_version": "builtin_ego_route_only_manifest.v1",
        "run_id": run_root.name,
        "scenario_id": scenario_id,
        "scenario_class": scenario_class,
        "scenario_case": scenario_id,
        "map": town or scenario.get("map"),
        "route": dict(route),
        "backend": "carla_builtin",
        "backend_name": "carla_builtin",
        "backend_type": "planning_control_backend",
        "algorithm_variant_id": "simple_acc_route_follower",
        "input_contract": "scene_truth_direct",
        "adapter_path": "carla_testbed.scenario_player.builtin_ego_runner",
        "available_truth_fields": ["ego_state", "route_waypoint_context"],
        "output_control_mode": "carla_vehicle_control",
        "transport_mode": "direct_python_api",
        "artifact_contract_version": "phase1_scenario_run_artifacts.v1",
        "target_actor_contract": target_contract,
        "fixed_scene_enabled": False,
        "fixed_scene_case": None,
        "ego_spawn_index": int(spawn_index),
        "ego_spawn_s_offset_m": float(spawn_s_offset_m),
        "starts_runtime": True,
        "ego_control_source": "carla_testbed_builtin_controller",
        "scenario_actor_control_source": "none",
        "background_traffic_control_source": "none",
        "background_walker_control_source": "none",
        "needs_local_carla": True,
        "starts_apollo": False,
        "starts_autoware": False,
        "interpretation_boundary": "CARLA-only diagnostic ego controller, not an Apollo/Autoware autonomy claim.",
        "ego_spawn_source": _ego_spawn_source(spawn_s_offset_m),
    }
    (run_root / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary = {
        "schema_version": "builtin_ego_route_only_summary.v1",
        "run_id": run_root.name,
        "scenario_id": scenario_id,
        "scenario_class": scenario_class,
        "scenario_case": scenario_id,
        "status": runtime_status,
        "success": runtime_status in {"pass", "warn"},
        "ticks": ticks,
        "sim_duration_s": ticks * float(fixed_dt_s),
        "wall_duration_s": end_wall - start_wall,
        "cleanup_errors": cleanup_errors,
        "claim_boundary": "diagnostic_only_not_natural_driving_evidence",
        "ego_control_source": "carla_testbed_builtin_controller",
        "ego_spawn_index": int(spawn_index),
        "ego_spawn_s_offset_m": float(spawn_s_offset_m),
        "ego_spawn_source": _ego_spawn_source(spawn_s_offset_m),
        "runtime_status": runtime_status,
        "collision_count": safety_summary["collision_count"],
        "lane_invasion_count": safety_summary["lane_invasion_count"],
        "collision_sensor_available": safety_summary["collision_sensor_available"],
        "lane_invasion_sensor_available": safety_summary["lane_invasion_sensor_available"],
        "safety_event_trace_path": str(artifacts / "safety_event_trace.jsonl"),
        "safety_event_warnings": safety_summary["warnings"],
    }
    (run_root / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    phase1_postprocess = run_phase1_postprocess(run_root)
    summary.update(
        {
            "v_t_gap_status": phase1_postprocess.get("v_t_gap_status"),
            "phase1_status": phase1_postprocess.get("phase1_status"),
            "phase1_failure_reason": phase1_postprocess.get("phase1_failure_reason"),
            "artifact_completeness_status": phase1_postprocess.get("artifact_completeness_status"),
        }
    )
    (run_root / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "schema_version": "builtin_ego_route_only_result.v1",
        "run_dir": str(run_root),
        "status": runtime_status,
        "manifest": str(run_root / "manifest.json"),
        "summary": str(run_root / "summary.json"),
        "ego_control_trace": str(trace_path),
        "timeseries": str(timeseries_path),
        "events": str(events_path),
        "safety_event_trace": str(artifacts / "safety_event_trace.jsonl"),
        "phase1_postprocess": phase1_postprocess,
    }


def run_builtin_ego_fixed_scene(
    *,
    template_path: str | Path,
    run_dir: str | Path,
    host: str = "localhost",
    port: int = 2000,
    town: str | None = "Town01",
    duration_s: float | None = None,
    fixed_dt_s: float = 0.05,
    ego_spawn_index: int | None = None,
    ego_spawn_s_offset_m: float | None = None,
    target_speed_mps: float | None = None,
    follow_spectator: bool = False,
    spectator_distance: float = 14.0,
    spectator_height: float = 5.0,
    spectator_pitch: float = -18.0,
    realtime: bool = False,
) -> dict[str, Any]:
    """Run a CARLA-only fixed scene with the diagnostic builtin ego controller."""

    carla = _import_carla()
    run_root = Path(run_dir).expanduser()
    artifacts = run_root / "artifacts"
    analysis = run_root / "analysis"
    artifacts.mkdir(parents=True, exist_ok=True)
    template = load_fixed_scene_template(template_path)
    storyboard = compile_fixed_scene_template(template)
    params = dict(storyboard.get("params") or {})
    duration = float(duration_s or params.get("duration_s", 45.0) or 45.0)
    target_speed = float(target_speed_mps or params.get("ego_target_speed_mps", 19.44) or 19.44)
    spawn_index = ego_spawn_index if ego_spawn_index is not None else _spawn_index_from_ref(_ego_spawn_ref(storyboard))
    spawn_s_offset_m = (
        float(ego_spawn_s_offset_m)
        if ego_spawn_s_offset_m is not None
        else _ego_spawn_s_offset_m(storyboard)
    )

    client = carla.Client(host, int(port))
    client.set_timeout(30.0)
    world = client.load_world(town) if town else client.get_world()
    original_settings = world.get_settings()
    sync_settings = world.get_settings()
    sync_settings.synchronous_mode = True
    sync_settings.fixed_delta_seconds = float(fixed_dt_s)
    world.apply_settings(sync_settings)

    ego = None
    runtime = CarlaFixedSceneRuntime()
    controller = SimpleAccRouteFollowerController(
        SimpleAccRouteFollowerConfig(
            target_speed_mps=target_speed,
            max_throttle=float(params.get("ego_max_throttle", 0.7) or 0.7),
            max_brake=float(params.get("ego_max_brake", 1.0) or 1.0),
            speed_kp=float(params.get("ego_speed_kp", 0.16) or 0.16),
            brake_kp=float(params.get("ego_brake_kp", 0.24) or 0.24),
            lead_lateral_gate_m=float(params.get("ego_lead_lateral_gate_m", 4.0) or 4.0),
            target_gap_m=float(params.get("ego_target_gap_m", 20.0) or 20.0),
            min_gap_m=float(params.get("ego_min_gap_m", 8.0) or 8.0),
            time_headway_s=float(params.get("ego_time_headway_s", 0.9) or 0.9),
            emergency_gap_m=float(params.get("ego_emergency_gap_m", 6.0) or 6.0),
        )
    )
    cleanup_errors: list[str] = []
    trace_path = artifacts / "ego_control_trace.jsonl"
    timeseries_path = run_root / "timeseries.csv"
    events_path = run_root / "events.jsonl"
    start_wall = time.time()
    ticks = 0
    safety_tracker: _SafetyEventTracker | None = None
    try:
        _append_event(events_path, {"event": "run_started", "wall_time_s": start_wall})
        ego = _spawn_ego(world, spawn_index=spawn_index, spawn_s_offset_m=spawn_s_offset_m)
        # Let CARLA settle the newly spawned ego before safety sensors start
        # producing claimable run events. Spawn-time lane-invasion callbacks are
        # setup provenance, not backend behavior evidence.
        _tick_world_if_available(world)
        _set_initial_speed(ego, _safe_call(ego, "get_transform"), _ego_initial_speed(storyboard))
        route_plan = _build_route_plan(world, ego, storyboard)
        state = runtime.setup(
            {
                "world": world,
                "ego_actor": ego,
                "artifact_dir": artifacts,
                "run_dir": run_root,
            },
            storyboard,
        )
        safety_tracker = _SafetyEventTracker(world=world, ego=ego, artifact_dir=artifacts)
        with trace_path.open("w", encoding="utf-8") as trace_fh, timeseries_path.open(
            "w", encoding="utf-8", newline=""
        ) as ts_fh:
            writer = csv.DictWriter(
                ts_fh,
                fieldnames=[
                    "sim_time",
                    "frame_id",
                    "ego_speed_mps",
                    "ego_x",
                    "ego_y",
                    "ego_yaw_rad",
                    "ego_route_s",
                    "ego_lane_id",
                    "ego_length_m",
                    "ego_width_m",
                    "ego_height_m",
                    "bbox_extent_x_m",
                    "bbox_extent_y_m",
                    "bbox_extent_z_m",
                    "lead_gap_m",
                    "lead_speed_mps",
                    "heading_error_rad",
                    "route_lookahead_heading_error_rad",
                    "cross_track_error_m",
                    "collision_count",
                    "lane_invasion_count",
                    "collision_sensor_available",
                    "lane_invasion_sensor_available",
                    "throttle",
                    "brake",
                    "steer",
                    "control_source",
                ],
            )
            writer.writeheader()
            while ticks * float(fixed_dt_s) < duration:
                frame_id = world.tick()
                ticks += 1
                sim_time = ticks * float(fixed_dt_s)
                runtime.tick(
                    {
                        "world": world,
                        "ego_actor": ego,
                        "artifact_dir": artifacts,
                        "run_dir": run_root,
                        "sim_time_sec": sim_time,
                        "world_frame": int(frame_id) if frame_id is not None else ticks,
                    }
                )
                stamp = FrameStamp(frame_id=int(frame_id) if frame_id is not None else ticks, sim_time_s=sim_time)
                ego_state = _ego_state_from_actor(world, ego, stamp, route_plan=route_plan)
                scene = _scene_truth_from_runtime(world, runtime, ego_state, stamp)
                if follow_spectator:
                    _spectator_follow(
                        world,
                        ego,
                        distance_m=spectator_distance,
                        height_m=spectator_height,
                        pitch_deg=spectator_pitch,
                    )
                command = controller.step(stamp, ego_state, scene)
                apply_result = apply_control_to_vehicle(ego, command, stamp=stamp)
                safety_snapshot = safety_tracker.snapshot() if safety_tracker is not None else _empty_safety_snapshot()
                trace_row = {
                    "schema_version": "ego_control_trace.v1",
                    "sim_time": sim_time,
                    "frame_id": stamp.frame_id,
                    "controller": controller.name,
                    "command": command.to_dict(),
                    "apply_result": apply_result.to_dict(),
                }
                trace_fh.write(json.dumps(trace_row, sort_keys=True) + "\n")
                lead = command.metadata
                ego_pos = ego_state.pose.position
                ego_dimensions = _actor_dimensions(ego)
                writer.writerow(
                    {
                        "sim_time": sim_time,
                        "frame_id": stamp.frame_id,
                        "ego_speed_mps": command.metadata.get("ego_speed_mps"),
                        "ego_x": ego_pos.x,
                        "ego_y": ego_pos.y,
                        "ego_yaw_rad": _yaw_from_actor(ego),
                        "ego_route_s": ego_state.metadata.get("route_s"),
                        "ego_lane_id": ego_state.metadata.get("lane_id"),
                        "ego_length_m": ego_dimensions.get("length_m"),
                        "ego_width_m": ego_dimensions.get("width_m"),
                        "ego_height_m": ego_dimensions.get("height_m"),
                        "bbox_extent_x_m": ego_dimensions.get("bbox_extent_x_m"),
                        "bbox_extent_y_m": ego_dimensions.get("bbox_extent_y_m"),
                        "bbox_extent_z_m": ego_dimensions.get("bbox_extent_z_m"),
                        "lead_gap_m": lead.get("lead_gap_m"),
                        "lead_speed_mps": lead.get("lead_speed_mps"),
                        "heading_error_rad": ego_state.metadata.get("heading_error_rad"),
                        "route_lookahead_heading_error_rad": ego_state.metadata.get(
                            "route_lookahead_heading_error_rad"
                        ),
                        "cross_track_error_m": ego_state.metadata.get("cross_track_error_m"),
                        "collision_count": safety_snapshot["collision_count"],
                        "lane_invasion_count": safety_snapshot["lane_invasion_count"],
                        "collision_sensor_available": safety_snapshot["collision_sensor_available"],
                        "lane_invasion_sensor_available": safety_snapshot["lane_invasion_sensor_available"],
                        "throttle": command.throttle,
                        "brake": command.brake,
                        "steer": command.steer,
                        "control_source": command.source,
                    }
                )
                if realtime:
                    _sleep_until(start_wall + ticks * float(fixed_dt_s))
        runtime_status = "pass" if not state.errors else "fail"
    finally:
        cleanup_errors.extend(runtime.teardown())
        if safety_tracker is not None:
            cleanup_errors.extend(safety_tracker.destroy())
        if ego is not None:
            try:
                ego.destroy()
            except Exception as exc:  # pragma: no cover - defensive CARLA cleanup
                cleanup_errors.append(f"ego_destroy_failed:{type(exc).__name__}: {exc}")
        try:
            world.apply_settings(original_settings)
        except Exception as exc:  # pragma: no cover - defensive CARLA cleanup
            cleanup_errors.append(f"restore_world_settings_failed:{type(exc).__name__}: {exc}")

    end_wall = time.time()
    _append_event(events_path, {"event": "run_finished", "wall_time_s": end_wall, "ticks": ticks})
    safety_summary = safety_tracker.snapshot() if safety_tracker is not None else _empty_safety_snapshot()
    manifest = {
        "schema_version": "builtin_ego_fixed_scene_manifest.v1",
        "run_id": run_root.name,
        "scenario_id": storyboard.get("scene_id"),
        "scenario_class": storyboard.get("scenario_class"),
        "scenario_case": storyboard.get("scene_id"),
        "map": town,
        "backend": "carla_builtin",
        "backend_name": "carla_builtin",
        "backend_type": "planning_control_backend",
        "algorithm_variant_id": "simple_acc_route_follower",
        "input_contract": "scene_truth_direct",
        "adapter_path": "carla_testbed.scenario_player.builtin_ego_runner",
        "available_truth_fields": [
            "ego_state",
            "target_actor_state",
            "fixed_scene_roles",
            "route_waypoint_context",
        ],
        "output_control_mode": "carla_vehicle_control",
        "transport_mode": "direct_python_api",
        **fixed_scene_manifest_fields_from_storyboard(storyboard),
        "starts_runtime": True,
        "ego_control_source": "carla_testbed_builtin_controller",
        "scenario_actor_control_source": "fixed_scene_player",
        "background_traffic_control_source": "none",
        "background_walker_control_source": "none",
        "ego_spawn_index": int(spawn_index),
        "ego_spawn_s_offset_m": float(spawn_s_offset_m),
        "ego_spawn_source": _ego_spawn_source(spawn_s_offset_m),
        "needs_local_carla": True,
        "starts_apollo": False,
        "starts_autoware": False,
        "interpretation_boundary": "CARLA-only diagnostic ego controller, not an Apollo/Autoware autonomy claim.",
    }
    (run_root / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    fixed_report = analyze_fixed_scene_contract_run_dir(run_root)
    actor_report = analyze_scenario_actor_contract_run_dir(run_root)
    fixed_paths = write_fixed_scene_contract_report(fixed_report, analysis / "fixed_scene_contract")
    actor_paths = write_scenario_actor_contract_report(actor_report, analysis / "scenario_actor_contract")
    status = _combine_statuses(
        runtime_status,
        str(fixed_report.get("status") or "insufficient_data"),
        str(actor_report.get("status") or "insufficient_data"),
    )
    summary = {
        "schema_version": "builtin_ego_fixed_scene_summary.v1",
        "run_id": run_root.name,
        "status": status,
        "success": status in {"pass", "warn"},
        "ticks": ticks,
        "sim_duration_s": ticks * float(fixed_dt_s),
        "wall_duration_s": end_wall - start_wall,
        "cleanup_errors": cleanup_errors,
        "claim_boundary": "diagnostic_only_not_natural_driving_evidence",
        "ego_control_source": "carla_testbed_builtin_controller",
        "ego_spawn_index": int(spawn_index),
        "ego_spawn_s_offset_m": float(spawn_s_offset_m),
        "ego_spawn_source": _ego_spawn_source(spawn_s_offset_m),
        "runtime_status": runtime_status,
        "fixed_scene_contract_status": fixed_report.get("status"),
        "scenario_actor_contract_status": actor_report.get("status"),
        "collision_count": safety_summary["collision_count"],
        "lane_invasion_count": safety_summary["lane_invasion_count"],
        "collision_sensor_available": safety_summary["collision_sensor_available"],
        "lane_invasion_sensor_available": safety_summary["lane_invasion_sensor_available"],
        "safety_event_trace_path": str(artifacts / "safety_event_trace.jsonl"),
        "safety_event_warnings": safety_summary["warnings"],
    }
    (run_root / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    phase1_postprocess = run_phase1_postprocess(run_root)
    summary.update(
        {
            "v_t_gap_status": phase1_postprocess.get("v_t_gap_status"),
            "phase1_status": phase1_postprocess.get("phase1_status"),
            "phase1_failure_reason": phase1_postprocess.get("phase1_failure_reason"),
            "artifact_completeness_status": phase1_postprocess.get("artifact_completeness_status"),
        }
    )
    (run_root / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "schema_version": "builtin_ego_fixed_scene_result.v1",
        "run_dir": str(run_root),
        "status": status,
        "manifest": str(run_root / "manifest.json"),
        "summary": str(run_root / "summary.json"),
        "ego_control_trace": str(trace_path),
        "timeseries": str(timeseries_path),
        "events": str(events_path),
        "safety_event_trace": str(artifacts / "safety_event_trace.jsonl"),
        "fixed_scene_contract": fixed_paths,
        "scenario_actor_contract": actor_paths,
        "phase1_postprocess": phase1_postprocess,
    }


class _SafetyEventTracker:
    """Attach CARLA safety-event sensors to ego and expose comparable counters.

    The tracker is deliberately best-effort: if a local fake world or a CARLA
    runtime cannot create a sensor, the run continues, but the sensor availability
    fields stay false so ScenarioComparison does not treat the counters as a
    comparable safety-event surface.
    """

    def __init__(self, *, world: Any, ego: Any, artifact_dir: Path) -> None:
        self.collision_count = 0
        self.lane_invasion_count = 0
        self.collision_sensor_available = False
        self.lane_invasion_sensor_available = False
        self.warnings: list[str] = []
        self._sensors: list[Any] = []
        self._trace_path = artifact_dir / "safety_event_trace.jsonl"
        self._trace_path.parent.mkdir(parents=True, exist_ok=True)
        if self._trace_path.exists():
            self._trace_path.unlink()
        self.collision_sensor_available = self._attach_sensor(
            world=world,
            ego=ego,
            blueprint_id="sensor.other.collision",
            event_name="collision",
            callback=self._on_collision,
        )
        self.lane_invasion_sensor_available = self._attach_sensor(
            world=world,
            ego=ego,
            blueprint_id="sensor.other.lane_invasion",
            event_name="lane_invasion",
            callback=self._on_lane_invasion,
        )

    def snapshot(self) -> dict[str, Any]:
        return {
            "collision_count": int(self.collision_count),
            "lane_invasion_count": int(self.lane_invasion_count),
            "collision_sensor_available": bool(self.collision_sensor_available),
            "lane_invasion_sensor_available": bool(self.lane_invasion_sensor_available),
            "warnings": list(self.warnings),
        }

    def destroy(self) -> list[str]:
        errors: list[str] = []
        for sensor in reversed(self._sensors):
            try:
                stop = getattr(sensor, "stop", None)
                if callable(stop):
                    stop()
            except Exception as exc:  # pragma: no cover - defensive CARLA cleanup
                errors.append(f"safety_sensor_stop_failed:{type(exc).__name__}: {exc}")
            try:
                destroy = getattr(sensor, "destroy", None)
                if callable(destroy):
                    destroy()
            except Exception as exc:  # pragma: no cover - defensive CARLA cleanup
                errors.append(f"safety_sensor_destroy_failed:{type(exc).__name__}: {exc}")
        self._sensors.clear()
        return errors

    def _attach_sensor(
        self,
        *,
        world: Any,
        ego: Any,
        blueprint_id: str,
        event_name: str,
        callback: Any,
    ) -> bool:
        blueprint = _find_sensor_blueprint(world, blueprint_id)
        if blueprint is None:
            self.warnings.append(f"{event_name}_sensor_blueprint_missing:{blueprint_id}")
            return False
        sensor = _spawn_attached_sensor(world=world, blueprint=blueprint, ego=ego)
        if sensor is None:
            self.warnings.append(f"{event_name}_sensor_spawn_failed:{blueprint_id}")
            return False
        listen = getattr(sensor, "listen", None)
        if not callable(listen):
            self.warnings.append(f"{event_name}_sensor_listen_missing:{blueprint_id}")
            _destroy_sensor_quietly(sensor)
            return False
        try:
            listen(callback)
        except Exception as exc:
            self.warnings.append(f"{event_name}_sensor_listen_failed:{type(exc).__name__}")
            _destroy_sensor_quietly(sensor)
            return False
        self._sensors.append(sensor)
        return True

    def _on_collision(self, event: Any) -> None:
        self.collision_count += 1
        other_actor = getattr(event, "other_actor", None)
        self._write_event(
            {
                "event_type": "collision",
                "collision_count": self.collision_count,
                "frame": getattr(event, "frame", None),
                "timestamp": getattr(event, "timestamp", None),
                "other_actor_id": getattr(other_actor, "id", None),
                "other_actor_type_id": getattr(other_actor, "type_id", None),
            }
        )

    def _on_lane_invasion(self, event: Any) -> None:
        self.lane_invasion_count += 1
        markings = getattr(event, "crossed_lane_markings", None)
        marking_types: list[str] = []
        if markings is not None:
            for marking in markings:
                marking_type = getattr(marking, "type", None)
                marking_types.append(str(marking_type) if marking_type is not None else "unknown")
        self._write_event(
            {
                "event_type": "lane_invasion",
                "lane_invasion_count": self.lane_invasion_count,
                "frame": getattr(event, "frame", None),
                "timestamp": getattr(event, "timestamp", None),
                "crossed_lane_marking_count": len(marking_types),
                "crossed_lane_marking_types": marking_types,
            }
        )

    def _write_event(self, payload: Mapping[str, Any]) -> None:
        row = {
            "schema_version": "builtin_safety_event.v1",
            **dict(payload),
        }
        with self._trace_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def _empty_safety_snapshot() -> dict[str, Any]:
    return {
        "collision_count": 0,
        "lane_invasion_count": 0,
        "collision_sensor_available": False,
        "lane_invasion_sensor_available": False,
        "warnings": ["safety_event_tracker_not_initialized"],
    }


def _find_sensor_blueprint(world: Any, blueprint_id: str) -> Any | None:
    try:
        return world.get_blueprint_library().find(blueprint_id)
    except Exception:
        return None


def _spawn_attached_sensor(*, world: Any, blueprint: Any, ego: Any) -> Any | None:
    transform = _sensor_transform()
    spawn_actor = getattr(world, "spawn_actor", None)
    if callable(spawn_actor):
        try:
            return spawn_actor(blueprint, transform, attach_to=ego)
        except TypeError:
            try:
                return spawn_actor(blueprint, transform)
            except Exception:
                pass
        except Exception:
            pass
    try_spawn_actor = getattr(world, "try_spawn_actor", None)
    if callable(try_spawn_actor):
        try:
            return try_spawn_actor(blueprint, transform, attach_to=ego)
        except TypeError:
            try:
                return try_spawn_actor(blueprint, transform)
            except Exception:
                pass
        except Exception:
            pass
    return None


def _sensor_transform() -> Any:
    try:
        carla = _import_carla()
        return carla.Transform()
    except Exception:
        return None


def _destroy_sensor_quietly(sensor: Any) -> None:
    try:
        destroy = getattr(sensor, "destroy", None)
        if callable(destroy):
            destroy()
    except Exception:
        return


def _spawn_index_from_ref(spawn_ref: str | None) -> int:
    if not spawn_ref:
        return 0
    digits = "".join(ch for ch in str(spawn_ref) if ch.isdigit())
    return int(digits) if digits else 0


def _load_scenario_mapping(path: str | Path) -> dict[str, Any]:
    data = yaml.safe_load(Path(path).expanduser().read_text(encoding="utf-8")) or {}
    if not isinstance(data, Mapping):
        raise ValueError(f"scenario file must contain a mapping: {path}")
    return dict(data)


def _scenario_duration_s(scenario: Mapping[str, Any]) -> float | None:
    for container_key in ("params", "run", "runtime"):
        container = scenario.get(container_key) if isinstance(scenario.get(container_key), Mapping) else {}
        if container.get("duration_s") is not None:
            return float(container["duration_s"])
    if scenario.get("duration_s") is not None:
        return float(scenario["duration_s"])
    return None


def _scenario_target_speed_mps(scenario: Mapping[str, Any]) -> float | None:
    for container_key in ("params", "success_intent", "run", "runtime"):
        container = scenario.get(container_key) if isinstance(scenario.get(container_key), Mapping) else {}
        for key in ("ego_target_speed_mps", "target_speed_mps", "desired_speed_mps"):
            if container.get(key) is not None:
                return float(container[key])
    return None


def _ego_spawn_ref(storyboard: Mapping[str, Any]) -> str | None:
    ego = (storyboard.get("roles") or {}).get("ego") if isinstance(storyboard.get("roles"), Mapping) else {}
    return str(ego.get("spawn_ref")) if isinstance(ego, Mapping) and ego.get("spawn_ref") else None


def _ego_spawn_s_offset_m(storyboard: Mapping[str, Any]) -> float:
    roles = storyboard.get("roles") if isinstance(storyboard.get("roles"), Mapping) else {}
    ego = roles.get("ego") if isinstance(roles.get("ego"), Mapping) else {}
    return _spawn_s_offset_from_mapping(ego)


def _route_ego_spawn_s_offset_m(scenario: Mapping[str, Any]) -> float:
    route = scenario.get("route") if isinstance(scenario.get("route"), Mapping) else {}
    for source in (route, scenario):
        offset = _spawn_s_offset_from_mapping(source)
        if offset:
            return offset
    return 0.0


def _spawn_s_offset_from_mapping(source: Mapping[str, Any]) -> float:
    spawn = source.get("spawn") if isinstance(source.get("spawn"), Mapping) else {}
    for key in ("spawn_s_offset_m", "ego_spawn_s_offset_m", "s_offset_m"):
        if source.get(key) is not None:
            return float(source[key])
    if spawn.get("s_offset_m") is not None:
        return float(spawn["s_offset_m"])
    return 0.0


def _ego_spawn_source(spawn_s_offset_m: float) -> str:
    return "carla_spawn_waypoint_offset" if abs(float(spawn_s_offset_m or 0.0)) >= 1e-9 else "carla_spawn_point"


def _ego_initial_speed(storyboard: Mapping[str, Any]) -> float | None:
    roles = storyboard.get("roles") if isinstance(storyboard.get("roles"), Mapping) else {}
    ego = roles.get("ego") if isinstance(roles.get("ego"), Mapping) else {}
    params = storyboard.get("params") if isinstance(storyboard.get("params"), Mapping) else {}
    for value in (ego.get("initial_speed_mps"), params.get("ego_initial_speed_mps")):
        if value is not None:
            return float(value)
    return None


def _build_route_plan(world: Any, ego: Any, route_context: Mapping[str, Any]) -> list[dict[str, Any]] | None:
    goal_index = _goal_spawn_index(route_context)
    if goal_index is None:
        return None
    try:
        spawns = list(world.get_map().get_spawn_points())
    except Exception:
        return None
    if goal_index < 0 or goal_index >= len(spawns):
        return None
    ego_tf = _safe_call(ego, "get_transform")
    if ego_tf is None:
        return None
    goal_location = getattr(spawns[goal_index], "location")
    waypoints: list[Any] = []
    planner_cls = _global_route_planner_class()
    if planner_cls is not None:
        try:
            planner = planner_cls(world.get_map(), 2.0)
            route = planner.trace_route(getattr(ego_tf, "location"), goal_location)
            for item in route or []:
                waypoint = item[0] if isinstance(item, (list, tuple)) and item else item
                if waypoint is not None:
                    waypoints.append(waypoint)
        except Exception:
            waypoints = []
    if not waypoints:
        waypoints = _greedy_waypoint_route(world, getattr(ego_tf, "location"), goal_location)
    return _prepare_route_plan(waypoints)


def _goal_spawn_index(route_context: Mapping[str, Any]) -> int | None:
    route = route_context.get("route") if isinstance(route_context.get("route"), Mapping) else {}
    candidates = [
        route.get("goal_ref") if isinstance(route, Mapping) else None,
        route_context.get("goal_ref"),
        route_context.get("route_id"),
        route_context.get("scene_id"),
        route_context.get("scenario_id"),
    ]
    for candidate in candidates:
        text = str(candidate or "")
        marker = "goal"
        lower = text.lower()
        if marker not in lower:
            continue
        suffix = lower.split(marker, 1)[1]
        digits = "".join(ch for ch in suffix if ch.isdigit())
        if digits:
            return int(digits)
    return None


def _global_route_planner_class() -> Any | None:
    for root in (
        Path("/home/ubuntu/CARLA_0.9.16/PythonAPI/carla"),
        Path("/home/ubuntu/carla/PythonAPI/carla"),
    ):
        if root.exists() and str(root) not in sys.path:
            sys.path.append(str(root))
    try:
        from agents.navigation.global_route_planner import GlobalRoutePlanner  # type: ignore

        return GlobalRoutePlanner
    except Exception:
        return None


def _greedy_waypoint_route(
    world: Any,
    start_location: Any,
    goal_location: Any,
    *,
    step_m: float = 2.0,
    max_steps: int = 500,
    goal_tolerance_m: float = 3.0,
) -> list[Any]:
    carla_map = world.get_map() if hasattr(world, "get_map") else None
    if carla_map is None or not hasattr(carla_map, "get_waypoint"):
        return []
    try:
        current = carla_map.get_waypoint(start_location)
    except Exception:
        return []
    if current is None:
        return []
    route = [current]
    visited: set[tuple[Any, Any, Any, int]] = set()
    for _ in range(max_steps):
        current_location = getattr(getattr(current, "transform", None), "location", None)
        if current_location is None:
            break
        if _location_distance(current_location, goal_location) <= goal_tolerance_m:
            break
        key = (
            getattr(current, "road_id", None),
            getattr(current, "section_id", None),
            getattr(current, "lane_id", None),
            int(round(float(getattr(current, "s", 0.0) or 0.0) / max(step_m, 0.1))),
        )
        visited.add(key)
        try:
            candidates = list(current.next(float(step_m)))
        except Exception:
            candidates = []
        if not candidates:
            break
        current = min(candidates, key=lambda candidate: _greedy_route_score(candidate, goal_location, visited, step_m))
        route.append(current)
    return route


def _greedy_route_score(candidate: Any, goal_location: Any, visited: set[tuple[Any, Any, Any, int]], step_m: float) -> float:
    transform = getattr(candidate, "transform", None)
    location = getattr(transform, "location", None)
    score = _location_distance(location, goal_location)
    key = (
        getattr(candidate, "road_id", None),
        getattr(candidate, "section_id", None),
        getattr(candidate, "lane_id", None),
        int(round(float(getattr(candidate, "s", 0.0) or 0.0) / max(step_m, 0.1))),
    )
    if key in visited:
        score += 1000.0
    return score


def _location_distance(left: Any, right: Any) -> float:
    return math.hypot(
        (_float_attr(left, "x", 0.0) or 0.0) - (_float_attr(right, "x", 0.0) or 0.0),
        (_float_attr(left, "y", 0.0) or 0.0) - (_float_attr(right, "y", 0.0) or 0.0),
    )


def _prepare_route_plan(waypoints: list[Any]) -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    cumulative = 0.0
    previous: tuple[float, float] | None = None
    for waypoint in waypoints:
        transform = getattr(waypoint, "transform", None)
        location = getattr(transform, "location", None)
        rotation = getattr(transform, "rotation", None)
        if location is None:
            continue
        x = _float_attr(location, "x", 0.0) or 0.0
        y = _float_attr(location, "y", 0.0) or 0.0
        if previous is not None:
            cumulative += math.hypot(x - previous[0], y - previous[1])
        previous = (x, y)
        points.append(
            {
                "x": x,
                "y": y,
                "yaw_rad": math.radians(_float_attr(rotation, "yaw", 0.0) or 0.0),
                "route_s": cumulative,
                "lane_id": _lane_id_from_waypoint(waypoint),
            }
        )
    return points


def _route_plan_metadata(route_plan: list[dict[str, Any]], transform: Any) -> dict[str, Any]:
    location = getattr(transform, "location", None)
    rotation = getattr(transform, "rotation", None)
    if location is None or not route_plan:
        return {}
    x = _float_attr(location, "x", 0.0) or 0.0
    y = _float_attr(location, "y", 0.0) or 0.0
    ego_yaw = math.radians(_float_attr(rotation, "yaw", 0.0) or 0.0)
    nearest_index = min(
        range(len(route_plan)),
        key=lambda idx: math.hypot(x - float(route_plan[idx]["x"]), y - float(route_plan[idx]["y"])),
    )
    nearest = route_plan[nearest_index]
    nearest_s = float(nearest.get("route_s", 0.0) or 0.0)
    lookahead_s = nearest_s + 8.0
    target = route_plan[-1]
    for point in route_plan[nearest_index:]:
        if float(point.get("route_s", 0.0) or 0.0) >= lookahead_s:
            target = point
            break
    dx_target = float(target["x"]) - x
    dy_target = float(target["y"]) - y
    target_yaw = math.atan2(dy_target, dx_target) if abs(dx_target) > 1e-6 or abs(dy_target) > 1e-6 else float(nearest["yaw_rad"])
    segment_yaw = _route_plan_segment_yaw(route_plan, nearest_index)
    dx_nearest = x - float(nearest["x"])
    dy_nearest = y - float(nearest["y"])
    cross_track = -dx_nearest * math.sin(segment_yaw) + dy_nearest * math.cos(segment_yaw)
    return {
        "heading_error_rad": _wrap_pi(ego_yaw - segment_yaw),
        "route_lookahead_heading_error_rad": _wrap_pi(ego_yaw - target_yaw),
        "cross_track_error_m": cross_track,
        "route_s": nearest_s,
        "lane_id": nearest.get("lane_id"),
        "route_plan_available": True,
        "route_plan_source": "carla_global_route_planner",
        "route_plan_nearest_index": nearest_index,
    }


def _route_plan_segment_yaw(route_plan: list[dict[str, Any]], nearest_index: int) -> float:
    if len(route_plan) < 2:
        return float(route_plan[nearest_index].get("yaw_rad", 0.0) or 0.0)
    current = route_plan[nearest_index]
    next_index = min(nearest_index + 1, len(route_plan) - 1)
    if next_index == nearest_index:
        next_index = max(0, nearest_index - 1)
    nxt = route_plan[next_index]
    dx = float(nxt["x"]) - float(current["x"])
    dy = float(nxt["y"]) - float(current["y"])
    if abs(dx) < 1e-6 and abs(dy) < 1e-6:
        return float(current.get("yaw_rad", 0.0) or 0.0)
    return math.atan2(dy, dx)


def _spawn_ego(world: Any, *, spawn_index: int, spawn_s_offset_m: float = 0.0) -> Any:
    blueprint = _find_ego_blueprint(world)
    transform = _ego_spawn_transform(world, spawn_index=spawn_index, spawn_s_offset_m=spawn_s_offset_m)
    actor = world.try_spawn_actor(blueprint, transform)
    if actor is None:
        actor = world.spawn_actor(blueprint, transform)
    if hasattr(actor, "set_autopilot"):
        actor.set_autopilot(False)
    return actor


def _ego_spawn_transform(world: Any, *, spawn_index: int, spawn_s_offset_m: float = 0.0) -> Any:
    spawns = list(world.get_map().get_spawn_points())
    if not spawns:
        raise RuntimeError("CARLA map has no spawn points")
    transform = spawns[max(0, min(int(spawn_index), len(spawns) - 1))]
    offset = float(spawn_s_offset_m or 0.0)
    if abs(offset) < 1e-9:
        return transform
    carla_map = world.get_map()
    if not hasattr(carla_map, "get_waypoint"):
        raise RuntimeError("ego_spawn_offset_failed:get_waypoint_unavailable")
    try:
        waypoint = carla_map.get_waypoint(getattr(transform, "location"), project_to_road=True)
    except TypeError:
        waypoint = carla_map.get_waypoint(getattr(transform, "location"))
    except Exception as exc:
        raise RuntimeError(f"ego_spawn_offset_failed:get_waypoint:{type(exc).__name__}") from exc
    selection = select_waypoint_ahead_transform(waypoint, offset)
    if not selection.found or selection.selected_transform is None:
        reason = selection.reason or "no_selected_transform"
        raise RuntimeError(f"ego_spawn_offset_failed:{reason}")
    return _spawn_height_adjusted_transform(transform, selection.selected_transform)


def _spawn_height_adjusted_transform(base_transform: Any, shifted_transform: Any) -> Any:
    base_location = getattr(base_transform, "location", None)
    shifted_location = getattr(shifted_transform, "location", None)
    if base_location is None or shifted_location is None:
        return shifted_transform
    try:
        shifted_z = _float_attr(shifted_location, "z", 0.0) or 0.0
        base_z = _float_attr(base_location, "z", 0.0) or 0.0
    except Exception:
        return shifted_transform
    desired_z = base_z if abs(base_z) >= 1e-9 else shifted_z + 0.35
    z_offset = desired_z - shifted_z
    if abs(z_offset) < 1e-9:
        return shifted_transform
    try:
        adjusted = copy.deepcopy(shifted_transform)
        getattr(adjusted, "location").z = desired_z
        return adjusted
    except Exception:
        pass
    return _offset_transform(
        shifted_transform,
        {"s_offset_m": 0.0, "lateral_offset_m": 0.0, "z_offset_m": z_offset},
    )


def _tick_world_if_available(world: Any) -> None:
    tick = getattr(world, "tick", None)
    if callable(tick):
        tick()


def _spectator_follow(
    world: Any,
    actor: Any,
    *,
    distance_m: float,
    height_m: float,
    pitch_deg: float,
) -> None:
    transform = _safe_call(actor, "get_transform")
    if transform is None:
        return
    carla = _import_carla()
    location = getattr(transform, "location", None)
    rotation = getattr(transform, "rotation", None)
    if location is None or rotation is None:
        return
    yaw_deg = float(getattr(rotation, "yaw", 0.0) or 0.0)
    yaw = math.radians(yaw_deg)
    camera_location = carla.Location(
        x=float(getattr(location, "x", 0.0) or 0.0) - float(distance_m) * math.cos(yaw),
        y=float(getattr(location, "y", 0.0) or 0.0) - float(distance_m) * math.sin(yaw),
        z=float(getattr(location, "z", 0.0) or 0.0) + float(height_m),
    )
    camera_rotation = carla.Rotation(pitch=float(pitch_deg), yaw=yaw_deg, roll=0.0)
    try:
        world.get_spectator().set_transform(carla.Transform(camera_location, camera_rotation))
    except Exception:
        return


def _combine_statuses(*statuses: str) -> str:
    normalized = [str(status or "insufficient_data") for status in statuses]
    for status in ("fail", "insufficient_data", "warn"):
        if status in normalized:
            return status
    return "pass"


def _sleep_until(target_wall_time: float) -> None:
    remaining = float(target_wall_time) - time.time()
    if remaining > 0.0:
        time.sleep(remaining)


def _find_ego_blueprint(world: Any) -> Any:
    library = world.get_blueprint_library()
    for blueprint_id in ("vehicle.lincoln.mkz_2020", "vehicle.lincoln.mkz_2017", "vehicle.tesla.model3"):
        try:
            return library.find(blueprint_id)
        except Exception:
            continue
    matches = list(library.filter("vehicle.*"))
    if not matches:
        raise RuntimeError("no vehicle blueprint available")
    return matches[0]


def _ego_state_from_actor(
    world: Any,
    actor: Any,
    stamp: FrameStamp,
    *,
    route_plan: list[dict[str, Any]] | None = None,
) -> EgoState:
    transform = _safe_call(actor, "get_transform")
    velocity = _safe_call(actor, "get_velocity")
    location = getattr(transform, "location", None)
    rotation = getattr(transform, "rotation", None)
    yaw_rad = math.radians(_float_attr(rotation, "yaw", 0.0))
    metadata = _route_error_metadata(world, transform, route_plan=route_plan)
    speed = _vector_norm(velocity)
    return EgoState(
        stamp=stamp,
        pose=Pose3D(
            position=Vector3D(
                x=_float_attr(location, "x", 0.0),
                y=_float_attr(location, "y", 0.0),
                z=_float_attr(location, "z", 0.0),
            ),
            orientation=_quaternion_from_yaw(yaw_rad),
        ),
        linear_velocity=_vector3d(velocity),
        chassis=ChassisState(speed_mps=speed),
        metadata=metadata,
    )


def _yaw_from_actor(actor: Any) -> float | None:
    transform = _safe_call(actor, "get_transform")
    rotation = getattr(transform, "rotation", None)
    if rotation is None:
        return None
    return math.radians(_float_attr(rotation, "yaw", 0.0))


def _scene_truth_from_runtime(world: Any, runtime: CarlaFixedSceneRuntime, ego: EgoState, stamp: FrameStamp) -> SceneTruth:
    obstacles: list[ObstacleTruth] = []
    for role, actor in runtime.actors.items():
        transform = _safe_call(actor, "get_transform")
        velocity = _safe_call(actor, "get_velocity")
        location = getattr(transform, "location", None)
        rotation = getattr(transform, "rotation", None)
        route_metadata = _route_error_metadata(world, transform)
        runtime_state = runtime.actor_state(role)
        progress_metadata = {}
        if runtime_state is not None:
            progress_metadata = {
                "trajectory_progress_m": runtime_state.trajectory_progress_m,
                "route_progress_gap_m": runtime_state.route_progress_gap_m,
                "route_progress_gap_source": runtime_state.route_progress_gap_source,
            }
        obstacles.append(
            ObstacleTruth(
                obstacle_id=str(getattr(actor, "id", role)),
                obstacle_type="vehicle",
                pose=Pose3D(
                    position=Vector3D(
                        x=_float_attr(location, "x", 0.0),
                        y=_float_attr(location, "y", 0.0),
                        z=_float_attr(location, "z", 0.0),
                    ),
                    orientation=_quaternion_from_yaw(math.radians(_float_attr(rotation, "yaw", 0.0))),
                ),
                linear_velocity=_vector3d(velocity),
                metadata={
                    "role": role,
                    "control_source": "fixed_scene_player",
                    "route_s": route_metadata.get("route_s"),
                    "lane_id": route_metadata.get("lane_id"),
                    **progress_metadata,
                },
            )
        )
    return SceneTruth(stamp=stamp, ego=ego, obstacles=tuple(obstacles), metadata=dict(ego.metadata))


def _route_error_metadata(
    world: Any,
    transform: Any,
    *,
    route_plan: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if route_plan:
        metadata = _route_plan_metadata(route_plan, transform)
        if metadata:
            return metadata
    carla_map = world.get_map() if hasattr(world, "get_map") else None
    if carla_map is None or transform is None or not hasattr(carla_map, "get_waypoint"):
        return {}
    try:
        waypoint = carla_map.get_waypoint(getattr(transform, "location"))
    except Exception:
        return {}
    if waypoint is None:
        return {}
    ego_loc = getattr(transform, "location", None)
    wp_tf = getattr(waypoint, "transform", None)
    wp_loc = getattr(wp_tf, "location", None)
    wp_rot = getattr(wp_tf, "rotation", None)
    route_yaw = math.radians(_float_attr(wp_rot, "yaw", 0.0))
    ego_yaw = math.radians(_float_attr(getattr(transform, "rotation", None), "yaw", 0.0))
    dx = _float_attr(ego_loc, "x", 0.0) - _float_attr(wp_loc, "x", 0.0)
    dy = _float_attr(ego_loc, "y", 0.0) - _float_attr(wp_loc, "y", 0.0)
    cross_track = -dx * math.sin(route_yaw) + dy * math.cos(route_yaw)
    lookahead_error = _route_lookahead_heading_error(transform, waypoint)
    return {
        "heading_error_rad": _wrap_pi(ego_yaw - route_yaw),
        **(
            {"route_lookahead_heading_error_rad": lookahead_error}
            if lookahead_error is not None
            else {}
        ),
        "cross_track_error_m": cross_track,
        "route_s": _float_attr(waypoint, "s"),
        "lane_id": _lane_id_from_waypoint(waypoint),
    }


def _route_lookahead_heading_error(transform: Any, waypoint: Any, *, lookahead_m: float = 8.0) -> float | None:
    selection = select_waypoint_ahead_transform(waypoint, float(lookahead_m))
    target = selection.selected_transform if selection.found else getattr(waypoint, "transform", None)
    actor_location = getattr(transform, "location", None)
    target_location = getattr(target, "location", None)
    if actor_location is None or target_location is None:
        return None
    dx = _float_attr(target_location, "x", 0.0) - _float_attr(actor_location, "x", 0.0)
    dy = _float_attr(target_location, "y", 0.0) - _float_attr(actor_location, "y", 0.0)
    if abs(dx) < 1e-6 and abs(dy) < 1e-6:
        return None
    target_yaw = math.atan2(dy, dx)
    ego_yaw = math.radians(_float_attr(getattr(transform, "rotation", None), "yaw", 0.0))
    return _wrap_pi(ego_yaw - target_yaw)


def _lane_id_from_waypoint(waypoint: Any) -> str | None:
    parts = [
        getattr(waypoint, "road_id", None),
        getattr(waypoint, "section_id", None),
        getattr(waypoint, "lane_id", None),
    ]
    if any(part is None for part in parts):
        return None
    return ":".join(str(part) for part in parts)


def _set_initial_speed(actor: Any, transform: Any, speed_mps: float | None) -> None:
    if speed_mps is None or speed_mps <= 0.0:
        return
    setter = getattr(actor, "set_target_velocity", None)
    if not callable(setter):
        return
    yaw = math.radians(_float_attr(getattr(transform, "rotation", None), "yaw", 0.0))
    vector = _make_carla_vector(x=speed_mps * math.cos(yaw), y=speed_mps * math.sin(yaw), z=0.0)
    try:
        setter(vector)
    except Exception:
        return


def _make_carla_vector(*, x: float, y: float, z: float) -> Any:
    carla = _import_carla()
    return carla.Vector3D(x=x, y=y, z=z)


def _quaternion_from_yaw(yaw_rad: float) -> Quaternion:
    return Quaternion(x=0.0, y=0.0, z=math.sin(yaw_rad / 2.0), w=math.cos(yaw_rad / 2.0))


def _vector3d(vector: Any) -> Vector3D:
    return Vector3D(
        x=_float_attr(vector, "x", 0.0),
        y=_float_attr(vector, "y", 0.0),
        z=_float_attr(vector, "z", 0.0),
    )


def _vector_norm(vector: Any) -> float:
    x = _float_attr(vector, "x", 0.0)
    y = _float_attr(vector, "y", 0.0)
    z = _float_attr(vector, "z", 0.0)
    return math.sqrt(x * x + y * y + z * z)


def _actor_dimensions(actor: Any | None) -> dict[str, float | None]:
    bbox = getattr(actor, "bounding_box", None) if actor is not None else None
    extent = getattr(bbox, "extent", None)
    if extent is None:
        return {
            "length_m": None,
            "width_m": None,
            "height_m": None,
            "bbox_extent_x_m": None,
            "bbox_extent_y_m": None,
            "bbox_extent_z_m": None,
        }
    extent_x = _float_attr(extent, "x", 0.0)
    extent_y = _float_attr(extent, "y", 0.0)
    extent_z = _float_attr(extent, "z", 0.0)
    return {
        "length_m": 2.0 * extent_x,
        "width_m": 2.0 * extent_y,
        "height_m": 2.0 * extent_z,
        "bbox_extent_x_m": extent_x,
        "bbox_extent_y_m": extent_y,
        "bbox_extent_z_m": extent_z,
    }


def _append_event(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(dict(payload), sort_keys=True) + "\n")


def _float_attr(obj: Any, name: str, default: float = 0.0) -> float:
    if obj is None:
        return float(default)
    value = getattr(obj, name, default)
    if value is None:
        return float(default)
    return float(value)


def _safe_call(obj: Any, method_name: str) -> Any:
    method = getattr(obj, method_name, None)
    if callable(method):
        return method()
    return None


def _wrap_pi(angle: float) -> float:
    while angle > math.pi:
        angle -= 2.0 * math.pi
    while angle < -math.pi:
        angle += 2.0 * math.pi
    return angle


def _import_carla() -> Any:
    try:
        import carla  # type: ignore

        return carla
    except Exception as exc:  # pragma: no cover - local runtime dependency
        raise RuntimeError("CARLA Python API is required for builtin ego fixed-scene runs") from exc
