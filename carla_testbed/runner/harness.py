from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import carla

from carla_testbed.config import HarnessConfig
from carla_testbed.control import LegacyControllerConfig, LegacyFollowStopController
from carla_testbed.config.rig_loader import dump_rig
from carla_testbed.config.rig_postprocess import (
    load_meta_typed,
    meta_from_rig,
    expand_specs,
    derive_calibration,
    derive_time_sync,
    derive_noise,
    derive_data_format,
    derive_io,
    save_json,
    hash_file,
)
from carla_testbed.record import DemoRecorder, SummaryRecorder, TimeseriesRecorder
from carla_testbed.record.fail_capture import FailFrameCapture
from carla_testbed.schemas import ControlCommand, Event, FramePacket, GroundTruthPacket, ObjectTruth
from carla_testbed.sensors import CollisionEventSource, LaneInvasionEventSource, SensorRig
from carla_testbed.sim import tick_world


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

    def _ensure_config_outputs(
        self,
        out_dir: Path,
        sensor_out: Path,
        rig_final: Optional[dict],
        rig_name: Optional[str],
        sensor_specs: Optional[list],
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
            expanded = expand_specs(meta_typed, rig_final or {}, rig_name or "")
            save_json(cfg_dir / "sensors_expanded.json", expanded)
            save_json(cfg_dir / "calibration.json", derive_calibration(expanded))
            save_json(cfg_dir / "time_sync.json", derive_time_sync(expanded))
            save_json(cfg_dir / "noise_model.json", derive_noise(expanded))
            save_json(cfg_dir / "data_format.json", derive_data_format(expanded))
            save_json(cfg_dir / "io_contract_ros2.yaml", derive_io(expanded, "ros2"))
            save_json(cfg_dir / "io_contract_cyber.yaml", derive_io(expanded, "cyber"))
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
        enable_fail_capture: bool = False,
        enable_demo: bool = False,
        make_hud: bool = False,
    ) -> Tuple[HarnessState, dict]:
        out_dir.mkdir(parents=True, exist_ok=True)
        ts_rec = TimeseriesRecorder(out_dir / "timeseries.csv")
        summary_rec = SummaryRecorder(out_dir / "summary.json")
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

        col_src = CollisionEventSource(world, ego)
        inv_src = LaneInvasionEventSource(world, ego)
        col_src.start()
        inv_src.start()
        rig = None
        if sensor_specs:
            rig = SensorRig(world, ego, sensor_specs, sensor_out)
            rig.start()
        fail_cap = FailFrameCapture(world, ego, out_dir, dt=self.cfg.dt) if enable_fail_capture else None
        demo = None
        if enable_demo:
            demo = DemoRecorder(world, ego, out_dir / "demo", dt=self.cfg.dt)
            demo.start()
        cfg_dir.mkdir(parents=True, exist_ok=True)
        cfg_meta = (cfg_dir, None, None)
        if sensor_out.exists() or sensor_specs or rig_final:
            cfg_meta = self._ensure_config_outputs(out_dir, sensor_out, rig_final, rig_name, sensor_specs)
        adapter = None
        if self.cfg.enable_ros2_bridge:
            try:
                from carla_testbed.io.ros2_adapter import ROS2BridgeAdapter
            except Exception as exc:
                print(f"[WARN] ROS2 bridge not started: {exc}")
            else:
                contract_path = (self.cfg.ros2_contract_path or cfg_dir / "io_contract_ros2.yaml")
                calib_path = cfg_dir / "calibration.json"
                time_sync_path = cfg_dir / "time_sync.json"
                sensors_path = cfg_dir / "sensors_expanded.json"
                try:
                    adapter = ROS2BridgeAdapter(
                        contract_path=contract_path,
                        calib_path=calib_path,
                        time_sync_path=time_sync_path,
                        sensors_path=sensors_path,
                    )
                    adapter.start()
                    adapter.spin_once(0.0)
                except Exception as exc:
                    adapter = None
                    print(f"[WARN] Failed to start ROS2 bridge: {exc}")

        stopped_counter = 0
        hold_steps = max(1, int(1.0 / self.cfg.dt)) if self.cfg.dt > 0 else 20
        progress_interval = max(1, self.cfg.max_steps // 20)  # print ~20 updates

        try:
            for step in range(self.cfg.max_steps):
                frame_id, timestamp, _ = tick_world(world)
                self.state.step = step
                self.state.frame_id = frame_id
                self.state.timestamp = timestamp

                cmd: ControlCommand = controller.step(
                    t=step * self.cfg.dt,
                    dt=self.cfg.dt,
                    world=world,
                    carla_map=carla_map,
                    ego=ego,
                    front=front,
                )
                ego.apply_control(
                    carla.VehicleControl(
                        throttle=cmd.throttle,
                        brake=cmd.brake,
                        steer=cmd.steer,
                        reverse=cmd.reverse,
                        hand_brake=cmd.hand_brake,
                        manual_gear_shift=cmd.manual_gear_shift,
                        gear=cmd.gear,
                    )
                )

                v = (ego.get_velocity().length())
                self.state.max_speed_mps = max(self.state.max_speed_mps, v)
                collisions = col_src.fetch_and_clear()
                invasions = inv_src.fetch_and_clear()
                self.state.collision_count += len(collisions)
                self.state.lane_invasion_count += len(invasions)
                if adapter:
                    samples = {}
                    if rig:
                        captured = rig.capture(frame_id, timestamp=timestamp, return_samples=True)
                        if captured:
                            samples = captured
                    frame_packet = FramePacket(
                        frame_id=frame_id,
                        timestamp=timestamp,
                        ego_pose_world=self._pose_from_actor(ego),
                        samples=samples or {},
                    )
                    truth_packet = self._build_truth_packet(frame_id, timestamp, ego, front, collisions, invasions)
                    adapter.publish_frame(frame_packet)
                    adapter.publish_truth(truth_packet)
                    adapter.spin_once(0.0)
                else:
                    if rig:
                        rig.capture(frame_id, timestamp=timestamp, return_samples=False)

                last_debug = cmd.meta.get("last_debug", {}) if cmd.meta else {}
                row = {
                    "frame": frame_id,
                    "t": timestamp,
                    "step": step,
                    "v_mps": v,
                    "throttle": cmd.throttle,
                    "brake": cmd.brake,
                    "steer": cmd.steer,
                    "collision_count": self.state.collision_count,
                    "lane_invasion_count": self.state.lane_invasion_count,
                }
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
                if demo:
                    demo.capture(frame_id)

                if step % progress_interval == 0:
                    remaining = (self.cfg.max_steps - step - 1) * self.cfg.dt
                    pct = step / float(self.cfg.max_steps) * 100.0
                    print(f"[progress] {step}/{self.cfg.max_steps} ({pct:.0f}%), est remaining {remaining:.1f}s")
        finally:
            col_src.stop()
            inv_src.stop()
            if rig:
                rig.stop()
            if adapter:
                adapter.stop()
            if fail_cap:
                fail_cap.stop()
            if demo:
                demo.stop()
            ts_rec.close()

        if enable_demo and demo:
            try:
                demo.finalize(ts_rec.path if hasattr(ts_rec, "path") else out_dir / "timeseries.csv", make_hud=make_hud, fps=1.0 / self.cfg.dt)
            except Exception as e:
                print(f"[WARN] demo finalize failed: {e}")

        summary = {
            "success": self.state.success and self.state.fail_reason is None,
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
            "demo_enabled": enable_demo,
            "rig_name": rig_name,
            "ros2_bridge_enabled": self.cfg.enable_ros2_bridge,
        }
        summary_rec.write(summary)
        if rig_raw is not None and rig_final is not None:
            dump_rig(out_dir, rig_raw, rig_final, sensor_specs or [], meta_path=sensor_out / "meta.json")

        # Post-process calibration/IO/time/noise/data_format (ensure available even when bridge disabled)
        meta_path = sensor_out / "meta.json"
        cfg_dir_final, meta_typed, expanded = cfg_meta
        if meta_typed is None:
            cfg_dir_final, meta_typed, expanded = self._ensure_config_outputs(out_dir, sensor_out, rig_final, rig_name, sensor_specs)

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
                "io_ros2_hash": hash_file(cfg_dir_final / "io_contract_ros2.yaml"),
                "io_cyber_hash": hash_file(cfg_dir_final / "io_contract_cyber.yaml"),
                "time_base": "sim_time",
            }
            save_json(cfg_dir_final / "manifest.json", manifest)
        return self.state, summary
