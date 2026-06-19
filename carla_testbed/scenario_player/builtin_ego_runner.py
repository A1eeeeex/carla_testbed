from __future__ import annotations

import csv
import json
import math
import time
from pathlib import Path
from typing import Any, Mapping

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
from carla_testbed.scenario_player.carla_runtime import CarlaFixedSceneRuntime
from carla_testbed.scenario_player.compiler import compile_fixed_scene_template
from carla_testbed.scenario_player.safety_events import SAFETY_TRACE_FILENAME, SafetyEventTracker, empty_safety_snapshot
from carla_testbed.scenario_player.schema import load_fixed_scene_template


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
    safety_tracker: SafetyEventTracker | None = None
    try:
        _append_event(events_path, {"event": "run_started", "wall_time_s": start_wall})
        ego = _spawn_ego(world, spawn_index=spawn_index)
        safety_tracker = SafetyEventTracker(world=world, ego=ego, artifact_dir=artifacts)
        _tick_world_if_available(world)
        _set_initial_speed(ego, _safe_call(ego, "get_transform"), _ego_initial_speed(storyboard))
        state = runtime.setup(
            {
                "world": world,
                "ego_actor": ego,
                "artifact_dir": artifacts,
                "run_dir": run_root,
            },
            storyboard,
        )
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
                    "ego_length_m",
                    "ego_width_m",
                    "ego_height_m",
                    "bbox_extent_x_m",
                    "bbox_extent_y_m",
                    "bbox_extent_z_m",
                    "lead_gap_m",
                    "lead_speed_mps",
                    "collision_count",
                    "lane_invasion_count",
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
                ego_state = _ego_state_from_actor(world, ego, stamp)
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
                safety_snapshot = safety_tracker.snapshot() if safety_tracker is not None else empty_safety_snapshot()
                writer.writerow(
                    {
                        "sim_time": sim_time,
                        "frame_id": stamp.frame_id,
                        "ego_speed_mps": command.metadata.get("ego_speed_mps"),
                        "ego_x": ego_pos.x,
                        "ego_y": ego_pos.y,
                        "ego_yaw_rad": _yaw_from_actor(ego),
                        "ego_length_m": ego_dimensions.get("length_m"),
                        "ego_width_m": ego_dimensions.get("width_m"),
                        "ego_height_m": ego_dimensions.get("height_m"),
                        "bbox_extent_x_m": ego_dimensions.get("bbox_extent_x_m"),
                        "bbox_extent_y_m": ego_dimensions.get("bbox_extent_y_m"),
                        "bbox_extent_z_m": ego_dimensions.get("bbox_extent_z_m"),
                        "lead_gap_m": lead.get("lead_gap_m"),
                        "lead_speed_mps": lead.get("lead_speed_mps"),
                        "collision_count": safety_snapshot["collision_count"],
                        "lane_invasion_count": safety_snapshot["lane_invasion_count"],
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
    safety_summary = safety_tracker.snapshot() if safety_tracker is not None else empty_safety_snapshot()
    _append_event(events_path, {"event": "run_finished", "wall_time_s": end_wall, "ticks": ticks})
    manifest = {
        "schema_version": "builtin_ego_fixed_scene_manifest.v1",
        "run_id": run_root.name,
        "scenario_id": storyboard.get("scene_id"),
        "scenario_class": storyboard.get("scenario_class"),
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
        "starts_runtime": True,
        "ego_control_source": "carla_testbed_builtin_controller",
        "scenario_actor_control_source": "fixed_scene_player",
        "background_traffic_control_source": "none",
        "background_walker_control_source": "none",
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
        "collision_count": safety_summary["collision_count"],
        "lane_invasion_count": safety_summary["lane_invasion_count"],
        "collision_sensor_available": safety_summary["collision_sensor_available"],
        "lane_invasion_sensor_available": safety_summary["lane_invasion_sensor_available"],
        "safety_event_trace_path": str(artifacts / SAFETY_TRACE_FILENAME),
        "safety_event_warnings": safety_summary["warnings"],
        "runtime_status": runtime_status,
        "fixed_scene_contract_status": fixed_report.get("status"),
        "scenario_actor_contract_status": actor_report.get("status"),
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
        "safety_event_trace": str(artifacts / SAFETY_TRACE_FILENAME),
        "fixed_scene_contract": fixed_paths,
        "scenario_actor_contract": actor_paths,
        "phase1_postprocess": phase1_postprocess,
    }


def _spawn_index_from_ref(spawn_ref: str | None) -> int:
    if not spawn_ref:
        return 0
    digits = "".join(ch for ch in str(spawn_ref) if ch.isdigit())
    return int(digits) if digits else 0


def _ego_spawn_ref(storyboard: Mapping[str, Any]) -> str | None:
    ego = (storyboard.get("roles") or {}).get("ego") if isinstance(storyboard.get("roles"), Mapping) else {}
    return str(ego.get("spawn_ref")) if isinstance(ego, Mapping) and ego.get("spawn_ref") else None


def _ego_initial_speed(storyboard: Mapping[str, Any]) -> float | None:
    roles = storyboard.get("roles") if isinstance(storyboard.get("roles"), Mapping) else {}
    ego = roles.get("ego") if isinstance(roles.get("ego"), Mapping) else {}
    params = storyboard.get("params") if isinstance(storyboard.get("params"), Mapping) else {}
    for value in (ego.get("initial_speed_mps"), params.get("ego_initial_speed_mps")):
        if value is not None:
            return float(value)
    return None


def _spawn_ego(world: Any, *, spawn_index: int) -> Any:
    blueprint = _find_ego_blueprint(world)
    spawns = list(world.get_map().get_spawn_points())
    if not spawns:
        raise RuntimeError("CARLA map has no spawn points")
    transform = spawns[max(0, min(int(spawn_index), len(spawns) - 1))]
    actor = world.try_spawn_actor(blueprint, transform)
    if actor is None:
        actor = world.spawn_actor(blueprint, transform)
    if hasattr(actor, "set_autopilot"):
        actor.set_autopilot(False)
    return actor


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


def _ego_state_from_actor(world: Any, actor: Any, stamp: FrameStamp) -> EgoState:
    transform = _safe_call(actor, "get_transform")
    velocity = _safe_call(actor, "get_velocity")
    location = getattr(transform, "location", None)
    rotation = getattr(transform, "rotation", None)
    yaw_rad = math.radians(_float_attr(rotation, "yaw", 0.0))
    metadata = _route_error_metadata(world, transform)
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
                metadata={"role": role, "control_source": "fixed_scene_player"},
            )
        )
    return SceneTruth(stamp=stamp, ego=ego, obstacles=tuple(obstacles), metadata=dict(ego.metadata))


def _route_error_metadata(world: Any, transform: Any) -> dict[str, float]:
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
    return {
        "heading_error_rad": _wrap_pi(ego_yaw - route_yaw),
        "cross_track_error_m": cross_track,
    }


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
