from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import carla

from carla_testbed.config import HarnessConfig
from carla_testbed.contracts import FrameStamp as ControlFrameStamp
from carla_testbed.core.lifecycle import CleanupError, LifecycleManager, cleanup_methods
from carla_testbed.control import LegacyControllerConfig, LegacyFollowStopController
from carla_testbed.control.applicator import apply_control_to_vehicle, read_vehicle_control
from carla_testbed.evaluation import MetricsAccumulator
from carla_testbed.config.rig_loader import dump_rig
from carla_testbed.config.rig_postprocess import (
    load_meta_typed,
    meta_from_rig,
    expand_specs,
    derive_calibration,
    derive_time_sync,
    derive_noise,
    derive_data_format,
    save_json,
    hash_file,
)
from carla_testbed.record import (
    ROUTE_CURVE_FIELDS_SCHEMA_VERSION,
    RunArtifactStore,
    TimeseriesRecorder,
    build_manifest,
    build_carla_world_identity,
    build_route_curve_row,
    route_definition_from_metadata,
)
from carla_testbed.record.monitor import SignalMonitor
from carla_testbed.record.manager import RecordManager, RecordOptions
from carla_testbed.record.fail_capture import FailFrameCapture
from carla_testbed.ros2 import GroundTruthRos2Publisher
from carla_testbed.schemas import ControlCommand, Event, FramePacket, GroundTruthPacket, ObjectTruth
from carla_testbed.sensors import CollisionEventSource, LaneInvasionEventSource, SensorRig
from carla_testbed.sim import tick_world
from carla_testbed.runner.hooks import FrameContext, RunHook
from carla_testbed.runner.tick_loop import HookDispatcher, adapt_tick_callbacks, hook_error_summaries
from tbio.backends.ros2_native import Ros2NativePublisher


@dataclass
class HarnessState:
    step: int = 0
    frame_id: Optional[int] = None
    timestamp: Optional[float] = None
    success: bool = False
    fail_reason: Optional[str] = None
    first_failure_step: Optional[int] = None
    continued_steps_after_failure: int = 0
    collision_count: int = 0
    lane_invasion_count: int = 0
    max_speed_mps: float = 0.0


def _cleanup_error_summaries(errors: Tuple[CleanupError, ...]) -> List[dict]:
    return [
        {
            "name": err.name,
            "error_type": type(err.error).__name__,
            "message": str(err.error),
        }
        for err in errors
    ]


def _runtime_carla_world_identity(
    world: carla.World,
    carla_map: carla.Map,
    *,
    configured_town: str | None,
) -> dict:
    warnings: list[str] = []
    loaded_map_name = None
    spawn_point_count = None
    try:
        loaded_map_name = str(getattr(carla_map, "name", None) or "")
    except Exception as exc:
        warnings.append(f"map_name_unavailable:{type(exc).__name__}:{exc}")
    if not loaded_map_name:
        try:
            loaded_map_name = str(getattr(world.get_map(), "name", None) or "")
        except Exception as exc:
            warnings.append(f"world_get_map_name_unavailable:{type(exc).__name__}:{exc}")
    try:
        spawn_point_count = len(carla_map.get_spawn_points())
    except Exception as exc:
        warnings.append(f"spawn_points_unavailable:{type(exc).__name__}:{exc}")
    return build_carla_world_identity(
        configured_town=configured_town,
        loaded_map_name=loaded_map_name or None,
        spawn_point_count=spawn_point_count,
        source="harness.runtime",
        warnings=warnings,
    )


def _register_harness_cleanup_resources(
    lifecycle: LifecycleManager,
    *,
    col_src=None,
    inv_src=None,
    rig=None,
    ros_native=None,
    gt_pub=None,
    rviz_launcher=None,
    bag_recorder=None,
    fail_cap=None,
    record_mgr=None,
    monitor=None,
    ts_rec=None,
    persist_gt_stats: Optional[Callable[[], None]] = None,
) -> None:
    """Register run resources so lifecycle reverse cleanup preserves legacy order."""
    if ts_rec is not None:
        lifecycle.register_methods("timeseries_recorder", ts_rec, "close")
    if monitor is not None and (hasattr(monitor, "close") or hasattr(monitor, "stop")):
        lifecycle.register("monitor", monitor, cleanup_methods("close", "stop"))
    if record_mgr is not None:
        lifecycle.register_methods("record_manager", record_mgr, "stop")
    if fail_cap is not None:
        lifecycle.register_methods("fail_capture", fail_cap, "stop")
    if bag_recorder is not None:
        lifecycle.register_methods("bag_recorder", bag_recorder, "stop")
    if rviz_launcher is not None:
        lifecycle.register_methods("rviz_launcher", rviz_launcher, "stop")
    if gt_pub is not None:
        def _close_gt_pub(pub) -> None:
            if persist_gt_stats is not None:
                persist_gt_stats()
            close = getattr(pub, "close", None)
            if close is not None:
                close()

        lifecycle.register("ros2_gt_publisher", gt_pub, _close_gt_pub)
    if ros_native is not None:
        lifecycle.register_methods("ros2_native_publisher", ros_native, "teardown")
    if rig is not None:
        # SensorRig.stop owns its CARLA sensor stop/destroy sequence.
        lifecycle.register_methods("sensor_rig", rig, "stop")
    if inv_src is not None:
        # Event source stop owns its CARLA sensor stop/destroy sequence.
        lifecycle.register_methods("lane_invasion_sensor", inv_src, "stop")
    if col_src is not None:
        lifecycle.register_methods("collision_sensor", col_src, "stop")


class TestHarness:
    """Minimal harness with fail_fast / log_and_continue logic and CSV/summary recording."""

    def __init__(self, cfg: HarnessConfig):
        self.cfg = cfg
        self.state = HarnessState()

    def reset(self):
        self.state = HarnessState()

    def _quat_from_euler(self, roll: float, pitch: float, yaw: float):
        r = math.radians(roll or 0.0)
        p = math.radians(pitch or 0.0)
        yv = math.radians(yaw or 0.0)
        cr, sr = math.cos(r / 2.0), math.sin(r / 2.0)
        cp, sp = math.cos(p / 2.0), math.sin(p / 2.0)
        cy, sy = math.cos(yv / 2.0), math.sin(yv / 2.0)
        qw = cr * cp * cy + sr * sp * sy
        qx = sr * cp * cy - cr * sp * sy
        qy = cr * sp * cy + sr * cp * sy
        qz = cr * cp * sy - sr * sp * cy
        return {"qx": qx, "qy": qy, "qz": qz, "qw": qw}

    def _vec3(self, vec) -> Dict[str, float]:
        return {"x": getattr(vec, "x", 0.0), "y": getattr(vec, "y", 0.0), "z": getattr(vec, "z", 0.0)}

    def _route_curve_ego_pose(self, ego) -> dict[str, float] | None:
        try:
            tr = ego.get_transform()
        except Exception as exc:
            print(f"[WARN] route-curve ego pose unavailable: {exc}")
            return None
        loc = tr.location
        rot = tr.rotation
        return {
            "x": loc.x,
            "y": loc.y,
            "z": loc.z,
            "heading": math.radians(rot.yaw or 0.0),
        }

    def _route_curve_ego_yaw_rate(self, ego) -> float | None:
        getter = getattr(ego, "get_angular_velocity", None)
        if getter is None:
            return None
        try:
            angular = getter()
        except Exception as exc:
            print(f"[WARN] route-curve ego yaw-rate unavailable: {exc}")
            return None
        # CARLA reports angular velocity in degrees/s; route diagnostics use radians.
        return math.radians(float(getattr(angular, "z", 0.0)))

    def _pose_from_actor(self, actor) -> Dict[str, float]:
        tr = actor.get_transform()
        loc = tr.location
        rot = tr.rotation
        quat = self._quat_from_euler(rot.roll, rot.pitch, rot.yaw)
        return {
            "x": loc.x,
            "y": loc.y,
            "z": loc.z,
            "qx": quat["qx"],
            "qy": quat["qy"],
            "qz": quat["qz"],
            "qw": quat["qw"],
        }

    def _build_truth_packet(
        self,
        frame_id: int,
        timestamp: float,
        ego: carla.Vehicle,
        front: Optional[carla.Vehicle],
        collisions,
        invasions,
    ) -> GroundTruthPacket:
        events: List[Event] = []
        for col in collisions or []:
            other = getattr(col, "other_actor", None)
            meta = {
                "other_actor_id": getattr(other, "id", None),
                "other_actor_type": getattr(other, "type_id", None),
                "impulse": self._vec3(getattr(col, "normal_impulse", {})),
            }
            events.append(Event(event_type="collision", frame_id=frame_id, timestamp=timestamp, meta=meta))
        for inv in invasions or []:
            marks = getattr(inv, "crossed_lane_markings", []) or []
            meta = {"mark_types": [str(getattr(m, "type", "")) for m in marks]}
            events.append(Event(event_type="lane_invasion", frame_id=frame_id, timestamp=timestamp, meta=meta))

        ego_truth = None
        if ego:
            ego_truth = ObjectTruth(
                object_id="ego",
                pose=self._pose_from_actor(ego),
                velocity=self._vec3(ego.get_velocity()),
                acceleration=self._vec3(ego.get_acceleration()),
            )
        actors = []
        if front:
            actors.append(
                ObjectTruth(
                    object_id="front",
                    pose=self._pose_from_actor(front),
                    velocity=self._vec3(front.get_velocity()),
                    acceleration=self._vec3(front.get_acceleration()),
                )
            )
        return GroundTruthPacket(frame_id=frame_id, timestamp=timestamp, ego=ego_truth, actors=actors, events=events)

    def _spectator_follow(self, world: carla.World, ego: carla.Vehicle):
        """Move CARLA spectator to chase the ego vehicle."""
        if not self.cfg.follow_spectator or ego is None:
            return
        try:
            spectator = world.get_spectator()
            tr = ego.get_transform()
            yaw = math.radians(tr.rotation.yaw)
            dx = -self.cfg.spectator_distance * math.cos(yaw)
            dy = -self.cfg.spectator_distance * math.sin(yaw)
            loc = carla.Location(
                tr.location.x + dx,
                tr.location.y + dy,
                tr.location.z + self.cfg.spectator_height,
            )
            rot = carla.Rotation(pitch=self.cfg.spectator_pitch, yaw=tr.rotation.yaw, roll=0.0)
            spectator.set_transform(carla.Transform(loc, rot))
        except Exception as exc:
            print(f"[WARN] spectator follow failed: {exc}")

    def _ensure_config_outputs(
        self,
        out_dir: Path,
        sensor_out: Path,
        rig_final: Optional[dict],
        rig_name: Optional[str],
        sensor_specs: Optional[list],
        ego_id: str,
    ):
        cfg_dir = out_dir / "config"
        cfg_dir.mkdir(parents=True, exist_ok=True)
        meta_path = sensor_out / "meta.json" if sensor_out else None
        meta_typed = None
        if meta_path and meta_path.exists():
            meta_typed = load_meta_typed(meta_path)
        elif rig_final:
            meta_typed = meta_from_rig(rig_final)
            if meta_typed:
                (cfg_dir / "meta_typed.json").write_text(json.dumps(meta_typed, indent=2))

        expanded = None
        if meta_typed:
            expanded = expand_specs(meta_typed, rig_final or {}, rig_name or "", ego_id=ego_id)
            save_json(cfg_dir / "sensors_expanded.json", expanded)
            save_json(cfg_dir / "calibration.json", derive_calibration(expanded))
            save_json(cfg_dir / "time_sync.json", derive_time_sync(expanded))
            save_json(cfg_dir / "noise_model.json", derive_noise(expanded))
            save_json(cfg_dir / "data_format.json", derive_data_format(expanded))
        return cfg_dir, meta_typed, expanded

    def _check_fail(self, collision_events, invasion_events) -> Optional[str]:
        if collision_events:
            return "COLLISION"
        if invasion_events:
            hard = False
            for e in invasion_events:
                marks = getattr(e, "crossed_lane_markings", []) or []
                for m in marks:
                    if hasattr(m, "type") and "Solid" in str(m.type):
                        hard = True
                        break
                if hard:
                    break
            return "LANE_INVASION_HARD" if hard else "LANE_INVASION"
        return None

    def run(
        self,
        world: carla.World,
        carla_map: carla.Map,
        ego: carla.Vehicle,
        front: carla.Vehicle,
        controller_cfg: LegacyControllerConfig,
        out_dir: Path,
        sensor_specs: Optional[list] = None,
        rig_raw: Optional[dict] = None,
        rig_final: Optional[dict] = None,
        rig_name: Optional[str] = None,
        events_cfg: Optional[dict] = None,
        enable_fail_capture: bool = False,
        record_manager: Optional[RecordManager] = None,
        client: Optional[carla.Client] = None,
        rviz_launcher=None,
        bag_recorder_cfg: Optional[dict] = None,
        monitor: Optional[SignalMonitor] = None,
        disable_control: bool = False,
        sensor_capture_enabled: bool = True,
        tick_callbacks: Optional[List[Callable[..., None]]] = None,
        hooks: Optional[List[RunHook]] = None,
        scenario_metadata: Optional[dict] = None,
    ) -> Tuple[HarnessState, dict]:
        run_start_wall_time_s = time.time()
        out_dir.mkdir(parents=True, exist_ok=True)
        artifact_store = RunArtifactStore(out_dir).ensure()
        carla_world_identity = _runtime_carla_world_identity(
            world,
            carla_map,
            configured_town=self.cfg.town,
        )
        artifact_store.write_manifest(
            build_manifest(
                run_id=out_dir.name,
                start_time_wall_s=run_start_wall_time_s,
                config_path=None,
                carla_town=self.cfg.town,
                carla_world_identity=carla_world_identity,
                scenario_name="legacy_follow_stop",
                backend_name="harness",
                metadata={
                    "output_dir": str(out_dir),
                    "record_modes": list(self.cfg.record_modes) if hasattr(self.cfg, "record_modes") else [],
                    "ros2_native_enabled": bool(self.cfg.enable_ros2_native),
                    "ros2_gt_enabled": bool(self.cfg.enable_ros2_gt),
                    "scenario_metadata": json.loads(json.dumps(scenario_metadata or {}, default=str)),
                },
            )
        )
        artifact_store.write_resolved_config({"harness": self.cfg})
        artifact_events = artifact_store.open_events()
        artifact_events.append(
            {
                "event_type": "run_start",
                "wall_time_s": run_start_wall_time_s,
                "run_id": out_dir.name,
                "scenario_name": "legacy_follow_stop",
                "backend_name": "harness",
            }
        )
        ts_rec = TimeseriesRecorder(artifact_store.timeseries_csv_path)
        sensor_out = out_dir / "sensors"
        cfg_dir = out_dir / "config"
        controller = LegacyFollowStopController(
            cfg=controller_cfg,
            world=world,
            carla_map=carla_map,
            ego=ego,
            front=front,
        )
        controller.reset()

        events_cfg = events_cfg or {"collision": True, "lane_invasion": True}
        enable_collision = events_cfg.get("collision", True)
        enable_invasion = events_cfg.get("lane_invasion", True)
        col_src = CollisionEventSource(world, ego) if enable_collision else None
        inv_src = LaneInvasionEventSource(world, ego) if enable_invasion else None
        if col_src:
            col_src.start(enable_ros=self.cfg.enable_ros2_native, name="collision")
        if inv_src:
            inv_src.start(enable_ros=self.cfg.enable_ros2_native, name="lane_invasion")
        rig = None
        if sensor_specs:
            rig = SensorRig(
                world,
                ego,
                sensor_specs,
                sensor_out,
                enable_ros=self.cfg.enable_ros2_native,
                invert_tf=self.cfg.ros_invert_tf,
                ego_id=self.cfg.ego_id,
            )
            rig.start()
        fail_cap = FailFrameCapture(world, ego, out_dir, dt=self.cfg.dt) if enable_fail_capture else None
        record_mgr = record_manager
        if record_mgr:
            try:
                record_mgr.start(world=world, ego=ego, client=client, dt=self.cfg.dt)
            except Exception as exc:
                print(f"[WARN] record manager start failed: {exc}")
        cfg_dir.mkdir(parents=True, exist_ok=True)
        cfg_meta = (cfg_dir, None, None)
        if sensor_out.exists() or sensor_specs or rig_final:
            cfg_meta = self._ensure_config_outputs(out_dir, sensor_out, rig_final, rig_name, sensor_specs, ego_id=self.cfg.ego_id)
        cfg_dir_final, meta_typed, expanded = cfg_meta
        if record_mgr:
            record_mgr.config_paths = {
                "calibration": cfg_dir_final / "calibration.json",
                "sensors": cfg_dir_final / "sensors_expanded.json",
                "time_sync": cfg_dir_final / "time_sync.json",
            }
        ros_native = None
        gt_pub = None
        gt_stats_path = out_dir / "artifacts" / "ros2_gt_live_stats.json"
        tm = None
        bag_recorder = None
        cleanup_errors: Tuple[CleanupError, ...] = ()

        def _persist_gt_stats() -> None:
            if gt_pub is None:
                return
            try:
                gt_stats_path.parent.mkdir(parents=True, exist_ok=True)
                gt_stats_path.write_text(json.dumps(gt_pub.get_stats(), indent=2))
            except Exception as exc:
                print(f"[WARN] ROS2 GT stats persist failed: {exc}")

        if self.cfg.enable_ros2_gt:
            try:
                gt_pub = GroundTruthRos2Publisher(
                    ego_id=self.cfg.ego_id,
                    namespace=self.cfg.ros2_namespace,
                    invert_tf=self.cfg.ros_invert_tf,
                    calibration_path=cfg_dir_final / "calibration.json",
                    publish_tf=self.cfg.ros2_gt_publish_tf,
                    publish_odom=self.cfg.ros2_gt_publish_odom,
                    publish_objects3d=self.cfg.ros2_gt_publish_objects3d,
                    publish_markers=self.cfg.ros2_gt_publish_markers,
                    objects_radius_m=self.cfg.ros2_gt_objects_radius_m,
                    max_objects=self.cfg.ros2_gt_max_objects,
                    odom_hz=self.cfg.ros2_gt_odom_hz,
                    tf_hz=self.cfg.ros2_gt_tf_hz,
                    objects_hz=self.cfg.ros2_gt_objects_hz,
                    markers_hz=self.cfg.ros2_gt_markers_hz,
                    qos_reliability=self.cfg.ros2_gt_qos_reliability,
                    qos_depth=self.cfg.ros2_gt_qos_depth,
                )
                gt_pub.publish_static_tf_from_calibration()
                _persist_gt_stats()
            except Exception as exc:
                print(f"[WARN] failed to initialize ROS2 GT publisher: {exc}")
                gt_pub = None
        if self.cfg.enable_ros2_native:
            try:
                tm = world.get_trafficmanager()
            except Exception as exc:
                print(f"[WARN] traffic manager lookup failed: {exc}")
                tm = None
            ros_native = Ros2NativePublisher(
                world=world,
                traffic_manager=tm,
                ego_vehicle=ego,
                rig_spec=rig or sensor_specs or [],
                ego_id=self.cfg.ego_id,
                invert_tf=self.cfg.ros_invert_tf,
            )
            ros_native.setup_publishers()
            if rviz_launcher is not None:
                try:
                    rviz_launcher.start()
                except Exception as exc:
                    print(f"[WARN] RViz launcher failed: {exc}")
            if bag_recorder_cfg:
                try:
                    from carla_testbed.record import Ros2BagRecorder

                    bag_recorder = Ros2BagRecorder(**bag_recorder_cfg)
                    bag_recorder.start()
                    print(f"[ROS2 bag] recording to {bag_recorder_cfg.get('out_path')}")
                except FileNotFoundError:
                    print("[ERROR] ros2 命令未找到，无法录制 rosbag2")
                except Exception as exc:
                    print(f"[WARN] rosbag2 recorder start failed: {exc}")

        stopped_counter = 0
        hold_steps = max(1, int(1.0 / self.cfg.dt)) if self.cfg.dt > 0 else 20
        progress_interval = max(1, self.cfg.max_steps // 20)  # print ~20 updates
        actor_filter_failures: Dict[str, int] = {}
        metrics = MetricsAccumulator()
        route_curve_diagnostics: dict[str, int] = {
            "missing_route_context": 0,
            "missing_ego_pose": 0,
            "external_control_trace_unavailable": 0,
        }
        route_curve_route = route_definition_from_metadata(scenario_metadata)
        hook_dispatcher = HookDispatcher(list(hooks or []) + adapt_tick_callbacks(tick_callbacks), warn=print)
        run_hook_context: dict[str, Any] = {
            "out_dir": out_dir,
            "state": self.state,
            "metadata": {
                "disable_control": disable_control,
                "sensor_capture_enabled": sensor_capture_enabled,
            },
        }
        lifecycle = LifecycleManager()
        _register_harness_cleanup_resources(
            lifecycle,
            col_src=col_src,
            inv_src=inv_src,
            rig=rig,
            ros_native=ros_native,
            gt_pub=gt_pub,
            rviz_launcher=rviz_launcher,
            bag_recorder=bag_recorder,
            fail_cap=fail_cap,
            record_mgr=record_mgr,
            monitor=monitor,
            ts_rec=ts_rec,
            persist_gt_stats=_persist_gt_stats,
        )
        lifecycle.register_methods("run_hooks", hook_dispatcher, "close")
        lifecycle.register_methods("artifact_events", artifact_events, "close")

        try:
            hook_dispatcher.notify("on_run_start", run_hook_context)
            for step in range(self.cfg.max_steps):
                frame_ctx = FrameContext(step=step, metadata={"progress_interval": progress_interval})

                # Stage 1: advance CARLA exactly once through the harness tick owner.
                hook_dispatcher.notify("before_tick", frame_ctx)
                frame_id, timestamp, _ = tick_world(world)
                self.state.step = step
                self.state.frame_id = frame_id
                self.state.timestamp = timestamp
                frame_ctx.frame_id = frame_id
                frame_ctx.sim_time_s = timestamp
                hook_dispatcher.notify("after_world_tick", frame_ctx)

                # Stage 2: collect simulation state for this frame.
                v = (ego.get_velocity().length())
                route_curve_ego_pose = self._route_curve_ego_pose(ego)
                route_curve_ego_yaw_rate = self._route_curve_ego_yaw_rate(ego)
                self.state.max_speed_mps = max(self.state.max_speed_mps, v)
                collisions = col_src.fetch_and_clear() if col_src else []
                invasions = inv_src.fetch_and_clear() if inv_src else []
                self.state.collision_count += len(collisions)
                self.state.lane_invasion_count += len(invasions)
                for _ in collisions or []:
                    artifact_events.append(
                        {
                            "event_type": "collision",
                            "frame": frame_id,
                            "t": timestamp,
                            "step": step,
                            "collision_count": self.state.collision_count,
                        }
                    )
                for _ in invasions or []:
                    artifact_events.append(
                        {
                            "event_type": "lane_invasion",
                            "frame": frame_id,
                            "t": timestamp,
                            "step": step,
                            "lane_invasion_count": self.state.lane_invasion_count,
                        }
                    )
                frame_ctx.ego_state = {"speed_mps": v}
                frame_ctx.scene_state = {
                    "collision_count": self.state.collision_count,
                    "lane_invasion_count": self.state.lane_invasion_count,
                    "new_collisions": len(collisions),
                    "new_lane_invasions": len(invasions),
                }

                # Stage 3: capture configured sensors for the current frame.
                if rig and sensor_capture_enabled:
                    rig.capture(frame_id, timestamp=timestamp, return_samples=False)
                    frame_ctx.metadata["rig_capture"] = True
                    if monitor:
                        monitor.record_sensor("rig_capture", timestamp)
                else:
                    frame_ctx.metadata["rig_capture"] = False

                # Stage 4: publish ground truth and notify integrations.
                hook_dispatcher.notify("after_state_collect", frame_ctx)
                if gt_pub is not None:
                    try:
                        extra_actors = []
                        try:
                            extra_actors.extend(list(world.get_actors().filter("vehicle.*")))
                        except Exception as exc:
                            count = actor_filter_failures.get("vehicle", 0) + 1
                            actor_filter_failures["vehicle"] = count
                            if count <= 3:
                                print(f"[WARN] GT actor filter failed kind=vehicle step={step}: {exc}")
                        try:
                            extra_actors.extend(list(world.get_actors().filter("walker.*")))
                        except Exception as exc:
                            count = actor_filter_failures.get("walker", 0) + 1
                            actor_filter_failures["walker"] = count
                            if count <= 3:
                                print(f"[WARN] GT actor filter failed kind=walker step={step}: {exc}")
                        gt_pub.publish_tick(world, ego, extra_actors)
                        if step < 3 or step % progress_interval == 0:
                            _persist_gt_stats()
                    except Exception as exc:
                        print(f"[WARN] GT publish tick failed: {exc}")
                        gt_pub = None

                # Stage 5: compute or poll control.
                cmd: ControlCommand = controller.step(
                    t=step * self.cfg.dt,
                    dt=self.cfg.dt,
                    world=world,
                    carla_map=carla_map,
                    ego=ego,
                    front=front,
                )
                frame_ctx.control_command = cmd
                hook_dispatcher.notify("before_control_apply", frame_ctx)

                # Stage 6: apply control to CARLA, unless an external stack owns actuation.
                control_apply_result = None
                if not disable_control:
                    control_apply_result = apply_control_to_vehicle(
                        ego,
                        cmd,
                        stamp=ControlFrameStamp(frame_id=frame_id, sim_time_s=timestamp),
                    )
                    if not control_apply_result.applied_ok:
                        print(
                            "[WARN] control apply failed "
                            f"actor_id={control_apply_result.actor_id}: {control_apply_result.error}"
                        )
                applied_snapshot = (
                    control_apply_result.applied_control if control_apply_result is not None else read_vehicle_control(ego)
                )
                frame_ctx.metadata["applied_control"] = dict(applied_snapshot)
                if control_apply_result is not None:
                    frame_ctx.metadata["control_apply"] = control_apply_result.to_dict()
                if monitor and not disable_control:
                    clamped_monitor_ctrl = control_apply_result.clamped_command if control_apply_result is not None else {}
                    monitor.record_control(
                        "carla_control_applied",
                        timestamp,
                        ctrl={
                            "throttle": clamped_monitor_ctrl.get("throttle", cmd.throttle),
                            "brake": clamped_monitor_ctrl.get("brake", cmd.brake),
                            "steer": clamped_monitor_ctrl.get("steer", cmd.steer),
                            "reverse": clamped_monitor_ctrl.get("reverse", cmd.reverse),
                            "requested_throttle": cmd.throttle,
                            "requested_brake": cmd.brake,
                            "requested_steer": cmd.steer,
                            "applied_ok": control_apply_result.applied_ok if control_apply_result is not None else None,
                        },
                    )
                hook_dispatcher.notify("after_control_apply", frame_ctx)

                # Stage 7: record artifacts and evaluate run termination.
                applied_throttle = float(applied_snapshot.get("throttle", 0.0))
                applied_brake = float(applied_snapshot.get("brake", 0.0))
                applied_steer = float(applied_snapshot.get("steer", 0.0))
                clamped_command = control_apply_result.clamped_command if control_apply_result is not None else {}
                clamped_throttle = float(clamped_command.get("throttle", cmd.throttle))
                clamped_brake = float(clamped_command.get("brake", cmd.brake))
                clamped_steer = float(clamped_command.get("steer", cmd.steer))
                if monitor:
                    snap_ctrl = {
                        "throttle": applied_throttle,
                        "brake": applied_brake,
                        "steer": applied_steer,
                    }
                    monitor.maybe_snapshot(step, timestamp, ctrl=snap_ctrl)

                last_debug = cmd.meta.get("last_debug", {}) if cmd.meta else {}
                lead_distance_m = last_debug.get("gap_euclid")
                metrics.update(
                    frame_id=frame_id,
                    sim_time_s=timestamp,
                    ego_speed_mps=v,
                    lead_distance_m=lead_distance_m if isinstance(lead_distance_m, (int, float)) else None,
                    collision_events_count=len(collisions),
                    lane_invasion_events_count=len(invasions),
                    applied_control=(
                        None
                        if control_apply_result is None
                        else {"applied_ok": control_apply_result.applied_ok, **control_apply_result.applied_control}
                    ),
                )
                row = {
                    "frame": frame_id,
                    "t": timestamp,
                    "step": step,
                    "v_mps": v,
                    # Keep throttle/brake/steer as effective control seen by CARLA
                    # so external-stack runs don't look like a second internal controller.
                    "throttle": applied_throttle,
                    "brake": applied_brake,
                    "steer": applied_steer,
                    "cmd_throttle": cmd.throttle,
                    "cmd_brake": cmd.brake,
                    "cmd_steer": cmd.steer,
                    "clamped_throttle": clamped_throttle,
                    "clamped_brake": clamped_brake,
                    "clamped_steer": clamped_steer,
                    "applied_throttle": applied_throttle,
                    "applied_brake": applied_brake,
                    "applied_steer": applied_steer,
                    "control_applied_ok": "" if control_apply_result is None else control_apply_result.applied_ok,
                    "control_apply_error": "" if control_apply_result is None else control_apply_result.error or "",
                    "control_actor_id": "" if control_apply_result is None else control_apply_result.actor_id,
                    "control_source": "external_stack" if disable_control else "harness_controller",
                    "collision_count": self.state.collision_count,
                    "lane_invasion_count": self.state.lane_invasion_count,
                }
                if disable_control:
                    # An external stack owns CARLA actuation. The harness
                    # controller command is only a compatibility placeholder
                    # here; recording it as Apollo raw/mapped control would
                    # create false control-mapping evidence.
                    route_curve_raw_control = None
                    route_curve_mapped_control = None
                    route_curve_diagnostics["external_control_trace_unavailable"] += 1
                else:
                    route_curve_raw_control = {"throttle": cmd.throttle, "brake": cmd.brake, "steer": cmd.steer}
                    route_curve_mapped_control = {
                        "throttle": clamped_throttle,
                        "brake": clamped_brake,
                        "steer": clamped_steer,
                    }

                route_curve_row = build_route_curve_row(
                    sim_time=timestamp,
                    frame_id=frame_id,
                    route_id=(
                        (last_debug.get("route_id") if isinstance(last_debug, dict) else None)
                        or (route_curve_route.route_id if route_curve_route is not None else None)
                    ),
                    route=route_curve_route,
                    ego_pose=route_curve_ego_pose,
                    ego_speed=v,
                    ego_yaw_rate=route_curve_ego_yaw_rate,
                    apollo_control=last_debug,
                    raw_control=route_curve_raw_control,
                    mapped_control=route_curve_mapped_control,
                    applied_control={
                        "throttle": applied_throttle,
                        "brake": applied_brake,
                        "steer": applied_steer,
                    },
                    guard_flags=last_debug,
                )
                if route_curve_row["route_x"] is None:
                    route_curve_diagnostics["missing_route_context"] += 1
                if route_curve_row["ego_x"] is None:
                    route_curve_diagnostics["missing_ego_pose"] += 1
                row.update(route_curve_row)
                # append debug fields
                for k, vdbg in last_debug.items():
                    row[f"dbg_{k}"] = vdbg
                ts_rec.write_row(row)

                fail_now = self._check_fail(collisions, invasions)
                if fail_cap:
                    fail_cap.capture()

                if fail_now and self.state.fail_reason is None:
                    self.state.fail_reason = fail_now
                    self.state.first_failure_step = step
                    artifact_events.append(
                        {
                            "event_type": "failure",
                            "frame": frame_id,
                            "t": timestamp,
                            "step": step,
                            "reason": fail_now,
                        }
                    )
                    if fail_cap:
                        fail_cap.save(frame_id=frame_id, timestamp=timestamp, ts_csv_path=ts_rec.path)

                if self.state.fail_reason is not None:
                    if self.cfg.fail_strategy == "fail_fast":
                        break
                    else:
                        self.state.continued_steps_after_failure += 1
                        if self.state.continued_steps_after_failure >= self.cfg.post_fail_steps:
                            break
                        # else continue running
                # success condition: stopped near front with small speed and valid gap
                if v < 0.2:
                    stopped_counter += 1
                else:
                    stopped_counter = 0

                gap_euclid = last_debug.get("gap_euclid")
                gap_valid = last_debug.get("gap_s_valid")
                if (
                    stopped_counter >= hold_steps
                    and self.state.fail_reason is None
                    and step > hold_steps
                    and gap_valid
                    and isinstance(gap_euclid, (int, float))
                    and gap_euclid < 12.0
                ):
                    self.state.success = True
                    break
                # Keep the local CARLA UI useful for demo recording. Previously
                # spectator updates happened only on progress logs, which made
                # screen-captured demos stay at a stale or first-person-like view.
                self._spectator_follow(world, ego)
                if record_mgr:
                    record_mgr.capture(frame_id, timestamp)

                if step % progress_interval == 0:
                    remaining = (self.cfg.max_steps - step - 1) * self.cfg.dt
                    pct = step / float(self.cfg.max_steps) * 100.0
                    print(f"[progress] {step}/{self.cfg.max_steps} ({pct:.0f}%), est remaining {remaining:.1f}s")
                    _persist_gt_stats()
        finally:
            run_hook_context["state"] = self.state
            hook_dispatcher.notify("on_run_end", run_hook_context)
            artifact_events.append(
                {
                    "event_type": "run_end",
                    "wall_time_s": time.time(),
                    "frame": self.state.frame_id,
                    "t": self.state.timestamp,
                    "step": self.state.step,
                    "success": self.state.success and self.state.fail_reason is None,
                    "fail_reason": self.state.fail_reason,
                }
            )
            _persist_gt_stats()
            cleanup_errors = lifecycle.cleanup_all()
            for err in cleanup_errors:
                print(
                    f"[WARN] harness cleanup failed "
                    f"component={err.name}: {type(err.error).__name__}: {err.error}"
                )

        if record_mgr:
            try:
                ts_path = ts_rec.path if hasattr(ts_rec, "path") else out_dir / "timeseries.csv"
                record_mgr.finalize(timeseries_path=ts_path, dt=self.cfg.dt)
            except Exception as exc:
                print(f"[WARN] record manager finalize failed: {exc}")

        run_end_wall_time_s = time.time()
        frames = max(0, int(self.state.step) + 1) if self.state.frame_id is not None else 0
        sim_duration_s = self.state.timestamp
        success = self.state.success and self.state.fail_reason is None
        exit_reason = (
            "success"
            if success
            else self.state.fail_reason
            or ("max_steps_reached" if frames >= self.cfg.max_steps else "stopped")
        )
        summary = {
            "success": success,
            "exit_reason": exit_reason,
            "frames": frames,
            "sim_duration_s": sim_duration_s,
            "wall_duration_s": run_end_wall_time_s - run_start_wall_time_s,
            "fail_reason": self.state.fail_reason,
            "collision_count": self.state.collision_count,
            "lane_invasion_count": self.state.lane_invasion_count,
            "max_speed_mps": self.state.max_speed_mps,
            "first_failure_step": self.state.first_failure_step,
            "continued_steps_after_failure": self.state.continued_steps_after_failure,
            "controller": controller_cfg.controller_mode,
            "lateral_mode": controller_cfg.lateral_mode,
            "policy_mode": controller_cfg.policy_mode,
            "agent_type": controller_cfg.agent_type,
            "fail_strategy": self.cfg.fail_strategy,
            "post_fail_steps": self.cfg.post_fail_steps,
            "sensors_enabled": sensor_specs is not None,
            "sensor_frames_saved": None if rig is None else rig.stats.frames_saved,
            "sensor_dropped": None if rig is None else rig.stats.dropped,
            "sensor_stats": None if rig is None else rig.stats.to_dict(),
            "record_modes": self.cfg.record_modes if hasattr(self.cfg, "record_modes") else [],
            "rig_name": rig_name,
            "ros2_native_enabled": self.cfg.enable_ros2_native,
            "ros2_gt_enabled": self.cfg.enable_ros2_gt,
            "ros2_gt": gt_pub.get_stats() if gt_pub is not None else None,
            "ros2_bag_enabled": self.cfg.enable_ros2_bag if hasattr(self.cfg, "enable_ros2_bag") else False,
            "carla_world": carla_world_identity,
            "cleanup_error_count": len(cleanup_errors),
            "cleanup_errors_count": len(cleanup_errors),
            "cleanup_errors": _cleanup_error_summaries(cleanup_errors),
            "hook_error_count": len(hook_dispatcher.errors),
            "hook_errors": hook_error_summaries(hook_dispatcher.errors),
            "route_curve_fields_schema_version": ROUTE_CURVE_FIELDS_SCHEMA_VERSION,
            "route_curve_diagnostics": route_curve_diagnostics,
        }
        summary["metrics"] = metrics.finalize(
            wall_duration_s=summary["wall_duration_s"],
            exit_reason=exit_reason,
        ).to_dict()
        if bag_recorder_cfg:
            summary["ros2_bag_out"] = str(bag_recorder_cfg.get("out_path"))
            summary["ros2_bag_topics"] = bag_recorder_cfg.get("topics", [])
        artifact_store.update_manifest({"end_time_wall_s": run_end_wall_time_s})
        artifact_store.write_summary(summary)
        if monitor:
            monitor.persist(out_dir / "artifacts" / "monitor.json")
        if rig_raw is not None and rig_final is not None:
            dump_rig(out_dir, rig_raw, rig_final, sensor_specs or [], meta_path=sensor_out / "meta.json")

        # Post-process calibration/IO/time/noise/data_format (ensure available even when ROS disabled)
        meta_path = sensor_out / "meta.json"
        if meta_typed is None:
            cfg_dir_final, meta_typed, expanded = self._ensure_config_outputs(out_dir, sensor_out, rig_final, rig_name, sensor_specs, ego_id=self.cfg.ego_id)

        if meta_typed and expanded:
            manifest = {
                "rig_name": rig_name,
                "rig_version": rig_final.get("version") if rig_final else None,
                "meta_hash": hash_file(meta_path) if meta_path.exists() else None,
                "meta_typed_hash": hash_file(cfg_dir_final / "meta_typed.json") if (cfg_dir_final / "meta_typed.json").exists() else None,
                "expanded_hash": hash_file(cfg_dir_final / "sensors_expanded.json"),
                "calib_hash": hash_file(cfg_dir_final / "calibration.json"),
                "time_sync_hash": hash_file(cfg_dir_final / "time_sync.json"),
                "noise_hash": hash_file(cfg_dir_final / "noise_model.json"),
                "format_hash": hash_file(cfg_dir_final / "data_format.json"),
                "time_base": "sim_time",
            }
            save_json(cfg_dir_final / "manifest.json", manifest)
        return self.state, summary
