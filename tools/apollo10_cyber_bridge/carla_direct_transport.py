from __future__ import annotations

import json
import math
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import carla

ROS2_MESSAGE_RUNTIME_AVAILABLE = True

try:
    from nav_msgs.msg import Odometry
except Exception:
    ROS2_MESSAGE_RUNTIME_AVAILABLE = False
    try:
        from ros_shims import OdometryShim as Odometry
    except Exception:
        from tools.apollo10_cyber_bridge.ros_shims import OdometryShim as Odometry  # type: ignore

try:
    from ackermann_msgs.msg import AckermannDriveStamped
except Exception:
    try:
        from ros_shims import AckermannDriveStampedShim as AckermannDriveStamped
    except Exception:
        from tools.apollo10_cyber_bridge.ros_shims import (  # type: ignore
            AckermannDriveStampedShim as AckermannDriveStamped,
        )

try:
    from geometry_msgs.msg import Twist
except Exception:
    try:
        from ros_shims import TwistMsgShim as Twist
    except Exception:
        from tools.apollo10_cyber_bridge.ros_shims import TwistMsgShim as Twist  # type: ignore

try:
    from std_msgs.msg import Float32MultiArray
except Exception:
    try:
        from ros_shims import Float32MultiArrayShim as Float32MultiArray
    except Exception:
        from tools.apollo10_cyber_bridge.ros_shims import (  # type: ignore
            Float32MultiArrayShim as Float32MultiArray,
        )


def _clamp(val: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, val))


def _wrap_rad(angle: float) -> float:
    return math.atan2(math.sin(angle), math.cos(angle))


def _quat_from_rpy_deg(roll_deg: float, pitch_deg: float, yaw_deg: float) -> Tuple[float, float, float, float]:
    roll = math.radians(roll_deg or 0.0)
    pitch = math.radians(pitch_deg or 0.0)
    yaw = math.radians(yaw_deg or 0.0)
    cr = math.cos(roll / 2.0)
    sr = math.sin(roll / 2.0)
    cp = math.cos(pitch / 2.0)
    sp = math.sin(pitch / 2.0)
    cy = math.cos(yaw / 2.0)
    sy = math.sin(yaw / 2.0)
    qw = cr * cp * cy + sr * sp * sy
    qx = sr * cp * cy - cr * sp * sy
    qy = cr * sp * cy + sr * cp * sy
    qz = cr * cp * sy - sr * sp * cy
    return qx, qy, qz, qw


def _carla_angular_velocity_deg_to_ros_rad(value: Any) -> Tuple[float, float, float]:
    """CARLA Actor.get_angular_velocity returns deg/s; ROS odometry uses rad/s."""

    return (
        math.radians(float(getattr(value, "x", 0.0))),
        math.radians(float(getattr(value, "y", 0.0))),
        math.radians(float(getattr(value, "z", 0.0))),
    )


class _StampShim:
    def __init__(self, sec: int, nanosec: int) -> None:
        self.sec = int(sec)
        self.nanosec = int(nanosec)


class _NowShim:
    def to_msg(self) -> _StampShim:
        now = time.time()
        sec = int(now)
        nanosec = int((now - sec) * 1e9)
        return _StampShim(sec, nanosec)


class _ClockShim:
    def now(self) -> _NowShim:
        return _NowShim()


class NoopExecutor:
    def shutdown(self, timeout_sec: float | None = None) -> bool:
        return True


STALE_WORLD_FRAME_POLICY_UNTIL_CONTROL = "until_control"
STALE_WORLD_FRAME_POLICY_ALWAYS_REPUBLISH = "always_republish"
STALE_WORLD_FRAME_POLICY_SKIP = "skip"
STALE_WORLD_FRAME_POLICIES = {
    STALE_WORLD_FRAME_POLICY_UNTIL_CONTROL,
    STALE_WORLD_FRAME_POLICY_ALWAYS_REPUBLISH,
    STALE_WORLD_FRAME_POLICY_SKIP,
}


def normalize_stale_world_frame_policy(policy: str | None) -> str:
    normalized = str(policy or STALE_WORLD_FRAME_POLICY_UNTIL_CONTROL).strip().lower()
    if normalized not in STALE_WORLD_FRAME_POLICIES:
        return STALE_WORLD_FRAME_POLICY_UNTIL_CONTROL
    return normalized


def should_republish_stale_world_frame(policy: str | None, *, control_tx_count: int) -> bool:
    normalized = normalize_stale_world_frame_policy(policy)
    if normalized == STALE_WORLD_FRAME_POLICY_ALWAYS_REPUBLISH:
        return True
    if normalized == STALE_WORLD_FRAME_POLICY_SKIP:
        return False
    return int(control_tx_count or 0) <= 0


class CarlaDirectTransport:
    def __init__(
        self,
        *,
        artifacts_dir: Path,
        carla_host: str,
        carla_port: int,
        ego_role_name: str,
        control_out_type: str,
        radius_m: float,
        max_obstacles: int,
        publish_rate_hz: float,
        sync_to_world_tick: bool,
        timeout_sec: float,
        max_steer_angle: float,
        speed_gain: float,
        brake_gain: float,
        watchdog_wait_for_first_msg: bool,
        watchdog_arm_delay_sec: float,
        startup_brake_suppression_enabled: bool,
        startup_brake_suppression_speed_mps: float,
        startup_brake_suppression_max_brake: float,
        startup_brake_suppression_min_throttle: float,
        startup_brake_suppression_hold_sec: float,
        startup_brake_recent_throttle_window_sec: float,
        route_command_mode: str = "cyber_direct",
        require_no_ros2_runtime: bool = False,
        client_timeout_sec: float = 5.0,
        control_apply_mode: str = "immediate_or_defer",
        stale_world_frame_policy: str = STALE_WORLD_FRAME_POLICY_UNTIL_CONTROL,
        invert_tf: bool = True,
        straight_lane_lateral_stabilizer_enabled: bool = False,
        straight_lane_lateral_stabilizer_max_abs_steer: float = 0.04,
        straight_lane_lateral_stabilizer_k_cte: float = 0.015,
        straight_lane_lateral_stabilizer_k_heading: float = 0.25,
        straight_lane_lateral_stabilizer_max_cte_m: float = 4.0,
        straight_lane_lateral_stabilizer_max_heading_error_deg: float = 20.0,
        straight_lane_lateral_stabilizer_max_speed_mps: float = 30.0,
    ) -> None:
        self.artifacts_dir = Path(artifacts_dir)
        self.control_out_type = str(control_out_type or "direct").lower()
        self.carla_host = str(carla_host or "127.0.0.1")
        self.carla_port = int(carla_port)
        self.ego_role_name = str(ego_role_name or "hero")
        self.radius_m = max(float(radius_m), 1.0)
        self.max_obstacles = max(int(max_obstacles), 1)
        self.publish_rate_hz = max(float(publish_rate_hz), 1.0)
        self.sync_to_world_tick = bool(sync_to_world_tick)
        self.timeout_sec = max(float(timeout_sec), 0.1)
        self.max_steer_angle = max(float(max_steer_angle), 1e-3)
        self.speed_gain = max(float(speed_gain), 0.1)
        self.brake_gain = max(float(brake_gain), 0.1)
        self.watchdog_wait_for_first_msg = bool(watchdog_wait_for_first_msg)
        self.watchdog_arm_delay_sec = max(float(watchdog_arm_delay_sec), 0.0)
        self.startup_brake_suppression_enabled = bool(startup_brake_suppression_enabled)
        self.startup_brake_suppression_speed_mps = max(float(startup_brake_suppression_speed_mps), 0.0)
        self.startup_brake_suppression_max_brake = max(float(startup_brake_suppression_max_brake), 0.0)
        self.startup_brake_suppression_min_throttle = max(float(startup_brake_suppression_min_throttle), 0.0)
        self.startup_brake_suppression_hold_sec = max(float(startup_brake_suppression_hold_sec), 0.0)
        self.startup_brake_recent_throttle_window_sec = max(
            float(startup_brake_recent_throttle_window_sec), 0.0
        )
        self.route_command_mode = str(route_command_mode or "cyber_direct").strip().lower() or "cyber_direct"
        self.require_no_ros2_runtime = bool(require_no_ros2_runtime)
        self.client_timeout_sec = max(float(client_timeout_sec), 0.5)
        self.control_apply_mode = str(control_apply_mode or "immediate_or_defer").strip().lower()
        if self.control_apply_mode not in {"immediate_or_defer", "frame_flush_only"}:
            self.control_apply_mode = "immediate_or_defer"
        self.stale_world_frame_policy = normalize_stale_world_frame_policy(stale_world_frame_policy)
        self.invert_tf = bool(invert_tf)
        self.straight_lane_lateral_stabilizer_enabled = bool(straight_lane_lateral_stabilizer_enabled)
        self.straight_lane_lateral_stabilizer_max_abs_steer = max(
            float(straight_lane_lateral_stabilizer_max_abs_steer), 0.0
        )
        self.straight_lane_lateral_stabilizer_k_cte = float(straight_lane_lateral_stabilizer_k_cte)
        self.straight_lane_lateral_stabilizer_k_heading = float(
            straight_lane_lateral_stabilizer_k_heading
        )
        self.straight_lane_lateral_stabilizer_max_cte_m = max(
            float(straight_lane_lateral_stabilizer_max_cte_m), 0.0
        )
        self.straight_lane_lateral_stabilizer_max_heading_error_deg = max(
            float(straight_lane_lateral_stabilizer_max_heading_error_deg), 0.0
        )
        self.straight_lane_lateral_stabilizer_max_speed_mps = max(
            float(straight_lane_lateral_stabilizer_max_speed_mps), 0.0
        )

        self.client: Optional[carla.Client] = None
        self.world: Optional[carla.World] = None
        self.ego: Optional[carla.Vehicle] = None
        self._bound_actor_id: Optional[int] = None
        self._ego_bound_monotonic: Optional[float] = None
        self._first_msg_received = False
        self._recent_positive_throttle_monotonic: Optional[float] = None
        self._last_applied_frame: Optional[int] = None
        self._pending_control: Optional[Dict[str, Any]] = None
        self._last_control_wall_sec = 0.0
        self._last_snapshot_wall_sec = 0.0
        self._cached_snapshot: Optional[Dict[str, Any]] = None
        self._cached_world_frame: Optional[int] = None
        self._cached_actor_snapshot: Dict[str, Any] = {}
        self._warned_no_ego = False
        self._last_discovery_sec = 0.0
        self._last_discovery_warn_sec = 0.0
        self.last_error = ""
        self._clock = _ClockShim()
        self._rx_counts = {"odom": 0, "objects3d": 0, "markers": 0, "objects_json": 0}
        self.stats: Dict[str, Any] = {
            "source": "carla_direct_transport",
            "transport_mode": "carla_direct",
            "gt_source": "carla_world_snapshot_direct",
            "control_apply_path": "bridge_direct_actor_apply",
            "tick_owner": "runner_harness_world_tick",
            "uses_ros2_gt": False,
            "uses_ros2_control_bridge": False,
            "requires_ros2_reexec": False,
            "route_command_mode": self.route_command_mode,
            "route_command_path": "cyber_direct_bridge_command_path",
            "require_no_ros2_runtime": self.require_no_ros2_runtime,
            "using_ros2_message_runtime": ROS2_MESSAGE_RUNTIME_AVAILABLE,
            "reference": {
                "pose_source": "carla_direct_actor_transform_origin",
                "localization_reference_mode": "vehicle_origin",
                "apollo_control_state_reference": "rear_axle_input_com_internal",
                "angular_velocity_source_api": "Actor.get_angular_velocity",
                "angular_velocity_source_unit": "deg_per_s",
                "odom_angular_velocity_unit": "rad_per_s",
            },
            "connect_ok": False,
            "connect_count": 0,
            "client_timeout_sec": self.client_timeout_sec,
            "ego_discovery_attempt_count": 0,
            "ego_discovery_fail_count": 0,
            "snapshot_count": 0,
            "world_frame_repeat_count": 0,
            "control_apply_count": 0,
            "control_apply_deferred_count": 0,
            "control_apply_mode": self.control_apply_mode,
            "stale_world_frame_policy": self.stale_world_frame_policy,
            "control_apply_frame_flush_queue_count": 0,
            "control_apply_frame_flush_overwrite_count": 0,
            "control_apply_pending_overwrite_count": 0,
            "control_apply_pending_flush_count": 0,
            "control_apply_duplicate_frame_skip_count": 0,
            "control_apply_watchdog_count": 0,
            "control_apply_fail_count": 0,
            "control_apply_first_frame": None,
            "control_apply_last_frame": None,
            "control_apply_frame_span": None,
            "control_apply_first_ts_sec": None,
            "control_apply_last_ts_sec": None,
            "control_apply_max_throttle": 0.0,
            "control_apply_max_brake": 0.0,
            "control_apply_max_speed_mps": 0.0,
            "straight_lane_lateral_stabilizer_enabled": self.straight_lane_lateral_stabilizer_enabled,
            "straight_lane_lateral_stabilizer_apply_count": 0,
            "straight_lane_lateral_stabilizer_skip_count": 0,
            "straight_lane_lateral_stabilizer_error_count": 0,
            "last_straight_lane_lateral_stabilizer": {},
            "startup_brake_suppressed_count": 0,
            "world_map": "",
            "ego_actor_id": None,
            "ego_role_name": self.ego_role_name,
            "last_speed_mps": 0.0,
            "last_error": "",
            "rx_counts": dict(self._rx_counts),
        }
        self.control_apply_path = self.artifacts_dir / "direct_bridge_control_apply.jsonl"
        self.stats_path = self.artifacts_dir / "direct_bridge_stats.json"
        self.actor_snapshot_path = self.artifacts_dir / "direct_bridge_actor_snapshot.json"
        self._connect()

    def get_clock(self) -> _ClockShim:
        return self._clock

    def destroy_node(self) -> None:
        self.write_artifacts()

    def _write_json_file(self, path: Path, payload: Dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2))

    def _append_jsonl(self, path: Path, payload: Dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fp:
            fp.write(json.dumps(payload, separators=(",", ":")) + "\n")

    def _stamp_from_elapsed(self, elapsed_seconds: float, stamp: Any) -> None:
        sec = int(math.floor(elapsed_seconds))
        nanosec = int((elapsed_seconds - sec) * 1e9)
        if nanosec < 0:
            sec -= 1
            nanosec += int(1e9)
        stamp.sec = sec
        stamp.nanosec = nanosec

    def _loc_xyz(self, loc: carla.Location) -> Tuple[float, float, float]:
        x = float(loc.x)
        y = float(-loc.y if self.invert_tf else loc.y)
        z = float(loc.z)
        return x, y, z

    def _rot_rpy(self, rot: carla.Rotation) -> Tuple[float, float, float]:
        roll = float(rot.roll)
        pitch = float(-rot.pitch if self.invert_tf else rot.pitch)
        yaw = float(-rot.yaw if self.invert_tf else rot.yaw)
        return roll, pitch, yaw

    def _vec_xyz(self, vec: Any) -> Tuple[float, float, float]:
        x = float(getattr(vec, "x", 0.0))
        y_raw = float(getattr(vec, "y", 0.0))
        y = float(-y_raw if self.invert_tf else y_raw)
        z = float(getattr(vec, "z", 0.0))
        return x, y, z

    def _connect(self) -> None:
        try:
            self.client = carla.Client(self.carla_host, self.carla_port)
            self.client.set_timeout(self.client_timeout_sec)
            self.world = self.client.get_world()
            self.stats["connect_count"] = int(self.stats.get("connect_count", 0)) + 1
            self.stats["connect_ok"] = True
            try:
                self.stats["world_map"] = str(self.world.get_map().name)
            except Exception:
                self.stats["world_map"] = ""
        except Exception as exc:
            self.last_error = f"connect_failed:{exc}"
            self.stats["last_error"] = self.last_error
            self.client = None
            self.world = None

    def _discover_ego(self, *, force_world_refresh: bool) -> Optional[carla.Vehicle]:
        if self.client is None:
            return None
        self.stats["ego_discovery_attempt_count"] = int(
            self.stats.get("ego_discovery_attempt_count", 0)
        ) + 1
        try:
            if force_world_refresh or self.world is None:
                self.world = self.client.get_world()
            world = self.world
            if world is None:
                return None
            vehicles = list(world.get_actors().filter("vehicle.*"))
            exact_matches = []
            alias_matches = []
            for vehicle in vehicles:
                role = ((vehicle.attributes or {}).get("role_name") or "").strip()
                if role == self.ego_role_name:
                    exact_matches.append(vehicle)
                elif role in {"hero", "ego", "tb_ego"}:
                    alias_matches.append(vehicle)
            matched = exact_matches or alias_matches
            if not matched:
                return None
            candidate = max(matched, key=lambda actor: int(actor.id))
            if self._bound_actor_id != int(candidate.id):
                self._bound_actor_id = int(candidate.id)
                self._ego_bound_monotonic = time.monotonic()
                self.stats["ego_actor_id"] = int(candidate.id)
            return candidate
        except Exception as exc:
            self.world = None
            self.ego = None
            self.stats["ego_discovery_fail_count"] = int(
                self.stats.get("ego_discovery_fail_count", 0)
            ) + 1
            now = time.monotonic()
            if now - self._last_discovery_warn_sec > 2.0:
                self._last_discovery_warn_sec = now
                self.last_error = f"discover_ego_failed:{exc}"
                self.stats["last_error"] = self.last_error
            return None

    def _ensure_ego(self) -> bool:
        if self.world is None:
            self._connect()
        now = time.monotonic()
        force_refresh = (now - self._last_discovery_sec) > 0.5
        if self.ego is None or force_refresh:
            self.ego = self._discover_ego(force_world_refresh=force_refresh)
            if force_refresh:
                self._last_discovery_sec = now
        if self.ego is None:
            if not self._warned_no_ego:
                self.last_error = "ego_missing"
                self.stats["last_error"] = self.last_error
                self._warned_no_ego = True
            return False
        self._warned_no_ego = False
        return True

    def _class_name(self, actor: carla.Actor) -> str:
        tid = (getattr(actor, "type_id", "") or "").lower()
        if tid.startswith("vehicle."):
            return "vehicle"
        if tid.startswith("walker.") or "pedestrian" in tid:
            return "pedestrian"
        return "unknown"

    def _filtered_objects(self, ego: carla.Actor, actors: Sequence[carla.Actor]) -> List[carla.Actor]:
        ego_loc = ego.get_transform().location
        ranked: List[Tuple[float, carla.Actor]] = []
        for actor in actors:
            if actor is None or actor.id == ego.id:
                continue
            tid = getattr(actor, "type_id", "") or ""
            if not (tid.startswith("vehicle.") or tid.startswith("walker.")):
                continue
            try:
                dist = ego_loc.distance(actor.get_transform().location)
            except Exception:
                continue
            if dist > self.radius_m:
                continue
            ranked.append((dist, actor))
        ranked.sort(key=lambda item: item[0])
        return [actor for _, actor in ranked[: self.max_obstacles]]

    def _build_odom(self, *, elapsed_seconds: float, ego: carla.Vehicle) -> Odometry:
        odom = Odometry()
        self._stamp_from_elapsed(elapsed_seconds, odom.header.stamp)
        odom.header.frame_id = "odom"
        odom.child_frame_id = "base_link"
        tr = ego.get_transform()
        x, y, z = self._loc_xyz(tr.location)
        roll, pitch, yaw = self._rot_rpy(tr.rotation)
        qx, qy, qz, qw = _quat_from_rpy_deg(roll, pitch, yaw)
        odom.pose.pose.position.x = x
        odom.pose.pose.position.y = y
        odom.pose.pose.position.z = z
        odom.pose.pose.orientation.x = qx
        odom.pose.pose.orientation.y = qy
        odom.pose.pose.orientation.z = qz
        odom.pose.pose.orientation.w = qw
        vx, vy, vz = self._vec_xyz(ego.get_velocity())
        wx, wy, wz = _carla_angular_velocity_deg_to_ros_rad(ego.get_angular_velocity())
        odom.twist.twist.linear.x = vx
        odom.twist.twist.linear.y = vy
        odom.twist.twist.linear.z = vz
        odom.twist.twist.angular.x = wx
        odom.twist.twist.angular.y = wy
        odom.twist.twist.angular.z = wz
        return odom

    def _build_objects_json(self, *, elapsed_seconds: float, actors: Sequence[carla.Actor]) -> str:
        payload: Dict[str, Any] = {"stamp": float(elapsed_seconds), "frame_id": "odom", "objects": []}
        actor_rows: List[Dict[str, Any]] = []
        for actor in actors:
            try:
                tr = actor.get_transform()
                extent = actor.bounding_box.extent
                vel = actor.get_velocity()
            except Exception:
                continue
            x, y, z = self._loc_xyz(tr.location)
            roll, pitch, yaw = self._rot_rpy(tr.rotation)
            vx, vy, vz = self._vec_xyz(vel)
            row = {
                "id": str(actor.id),
                "class": self._class_name(actor),
                "pose": {"x": x, "y": y, "z": z, "roll": roll, "pitch": pitch, "yaw": yaw},
                "velocity": {"x": vx, "y": vy, "z": vz},
                "size": {
                    "x": float(extent.x) * 2.0,
                    "y": float(extent.y) * 2.0,
                    "z": float(extent.z) * 2.0,
                },
            }
            payload["objects"].append(row)
            actor_rows.append(row)
        self._cached_actor_snapshot = {
            "ego_actor_id": self.stats.get("ego_actor_id"),
            "ego_role_name": self.ego_role_name,
            "object_count": len(actor_rows),
            "objects": actor_rows,
        }
        return json.dumps(payload, separators=(",", ":"))

    def _current_world_frame(self) -> Optional[int]:
        if self.world is None:
            return None
        try:
            return int(getattr(self.world.get_snapshot(), "frame", -1))
        except Exception:
            return None

    def _current_speed_mps(self) -> float:
        if self.ego is None:
            return 0.0
        try:
            return float(self.ego.get_velocity().length())
        except Exception:
            return 0.0

    def _maybe_suppress_startup_brake(self, ctrl: carla.VehicleControl, *, source: str) -> carla.VehicleControl:
        if not self.startup_brake_suppression_enabled or source != "pending":
            return ctrl
        if float(ctrl.brake) <= 0.0 or float(ctrl.brake) > self.startup_brake_suppression_max_brake:
            return ctrl
        now = time.monotonic()
        speed_mps = self._current_speed_mps()
        recent_throttle = (
            self._recent_positive_throttle_monotonic is not None
            and (now - self._recent_positive_throttle_monotonic) <= self.startup_brake_recent_throttle_window_sec
        )
        within_hold = (
            self._ego_bound_monotonic is not None
            and (now - self._ego_bound_monotonic) <= self.startup_brake_suppression_hold_sec
        )
        if speed_mps > self.startup_brake_suppression_speed_mps:
            return ctrl
        if not recent_throttle and not within_hold:
            return ctrl
        ctrl.brake = 0.0
        self.stats["startup_brake_suppressed_count"] = int(self.stats.get("startup_brake_suppressed_count", 0)) + 1
        return ctrl

    def _should_apply_on_frame(self, frame_id: Optional[int]) -> bool:
        if not self.sync_to_world_tick or frame_id is None:
            return True
        if self._last_applied_frame is None or frame_id != self._last_applied_frame:
            return True
        return False

    def _defer_control(
        self,
        ctrl: carla.VehicleControl,
        *,
        source: str,
        source_steer: float,
        steer_norm: float,
        steer_clamped: bool,
        frame_id: Optional[int],
    ) -> None:
        if self._pending_control is not None:
            self.stats["control_apply_pending_overwrite_count"] = int(
                self.stats.get("control_apply_pending_overwrite_count", 0)
            ) + 1
        self._pending_control = {
            "ctrl": ctrl,
            "source": source,
            "source_steer": float(source_steer),
            "steer_norm": float(steer_norm),
            "steer_clamped": bool(steer_clamped),
            "deferred_frame_id": frame_id,
            "deferred_ts_sec": time.time(),
        }
        self.stats["control_apply_deferred_count"] = int(
            self.stats.get("control_apply_deferred_count", 0)
        ) + 1
        self.stats["control_apply_duplicate_frame_skip_count"] = int(
            self.stats.get("control_apply_duplicate_frame_skip_count", 0)
        ) + 1

    def _queue_frame_flush_control(
        self,
        ctrl: carla.VehicleControl,
        *,
        source: str,
        source_steer: float,
        steer_norm: float,
        steer_clamped: bool,
    ) -> None:
        if self._pending_control is not None:
            self.stats["control_apply_frame_flush_overwrite_count"] = int(
                self.stats.get("control_apply_frame_flush_overwrite_count", 0)
            ) + 1
        self._pending_control = {
            "ctrl": ctrl,
            "source": source,
            "source_steer": float(source_steer),
            "steer_norm": float(steer_norm),
            "steer_clamped": bool(steer_clamped),
            "deferred_frame_id": self._current_world_frame(),
            "deferred_ts_sec": time.time(),
        }
        self.stats["control_apply_frame_flush_queue_count"] = int(
            self.stats.get("control_apply_frame_flush_queue_count", 0)
        ) + 1

    def _maybe_apply_straight_lane_lateral_stabilizer(
        self,
        ctrl: carla.VehicleControl,
        *,
        speed_mps: float,
    ) -> Tuple[carla.VehicleControl, Dict[str, Any]]:
        payload: Dict[str, Any] = {
            "enabled": self.straight_lane_lateral_stabilizer_enabled,
            "applied": False,
            "reason": "disabled",
        }
        if not self.straight_lane_lateral_stabilizer_enabled:
            return ctrl, payload
        if self.ego is None or self.world is None:
            payload["reason"] = "missing_ego_or_world"
            self.stats["straight_lane_lateral_stabilizer_skip_count"] = int(
                self.stats.get("straight_lane_lateral_stabilizer_skip_count", 0)
            ) + 1
            self.stats["last_straight_lane_lateral_stabilizer"] = payload
            return ctrl, payload
        if float(speed_mps) > self.straight_lane_lateral_stabilizer_max_speed_mps:
            payload["reason"] = "speed_above_limit"
            payload["speed_mps"] = float(speed_mps)
            self.stats["straight_lane_lateral_stabilizer_skip_count"] = int(
                self.stats.get("straight_lane_lateral_stabilizer_skip_count", 0)
            ) + 1
            self.stats["last_straight_lane_lateral_stabilizer"] = payload
            return ctrl, payload
        try:
            transform = self.ego.get_transform()
            carla_map = self.world.get_map()
            lane_type = getattr(getattr(carla, "LaneType", object), "Driving", None)
            if lane_type is not None:
                waypoint = carla_map.get_waypoint(
                    transform.location,
                    project_to_road=True,
                    lane_type=lane_type,
                )
            else:
                waypoint = carla_map.get_waypoint(transform.location, project_to_road=True)
            if waypoint is None:
                payload["reason"] = "no_waypoint"
                self.stats["straight_lane_lateral_stabilizer_skip_count"] = int(
                    self.stats.get("straight_lane_lateral_stabilizer_skip_count", 0)
                ) + 1
                self.stats["last_straight_lane_lateral_stabilizer"] = payload
                return ctrl, payload
            lane_transform = waypoint.transform
            ego_loc = transform.location
            lane_loc = lane_transform.location
            lane_yaw_rad = math.radians(float(lane_transform.rotation.yaw))
            ego_yaw_rad = math.radians(float(transform.rotation.yaw))
            dx = float(ego_loc.x - lane_loc.x)
            dy = float(ego_loc.y - lane_loc.y)
            left_x = -math.sin(lane_yaw_rad)
            left_y = math.cos(lane_yaw_rad)
            cte_m = dx * left_x + dy * left_y
            heading_error_rad = _wrap_rad(ego_yaw_rad - lane_yaw_rad)
            heading_error_deg = math.degrees(heading_error_rad)
            payload.update(
                {
                    "reason": "computed",
                    "speed_mps": float(speed_mps),
                    "cross_track_error_m": float(cte_m),
                    "heading_error_rad": float(heading_error_rad),
                    "heading_error_deg": float(heading_error_deg),
                    "lane_id": int(getattr(waypoint, "lane_id", 0)),
                    "road_id": int(getattr(waypoint, "road_id", 0)),
                    "section_id": int(getattr(waypoint, "section_id", 0)),
                    "original_steer": float(ctrl.steer),
                }
            )
            if abs(cte_m) > self.straight_lane_lateral_stabilizer_max_cte_m:
                payload["reason"] = "cte_above_limit"
            elif abs(heading_error_deg) > self.straight_lane_lateral_stabilizer_max_heading_error_deg:
                payload["reason"] = "heading_error_above_limit"
            else:
                steer_cmd = -(
                    self.straight_lane_lateral_stabilizer_k_cte * cte_m
                    + self.straight_lane_lateral_stabilizer_k_heading * heading_error_rad
                )
                steer_cmd = _clamp(
                    steer_cmd,
                    -self.straight_lane_lateral_stabilizer_max_abs_steer,
                    self.straight_lane_lateral_stabilizer_max_abs_steer,
                )
                ctrl.steer = float(steer_cmd)
                payload["applied"] = True
                payload["reason"] = "applied"
                payload["stabilized_steer"] = float(ctrl.steer)
                self.stats["straight_lane_lateral_stabilizer_apply_count"] = int(
                    self.stats.get("straight_lane_lateral_stabilizer_apply_count", 0)
                ) + 1
            if not payload["applied"]:
                self.stats["straight_lane_lateral_stabilizer_skip_count"] = int(
                    self.stats.get("straight_lane_lateral_stabilizer_skip_count", 0)
                ) + 1
            self.stats["last_straight_lane_lateral_stabilizer"] = payload
            return ctrl, payload
        except Exception as exc:
            payload["reason"] = "error"
            payload["error"] = str(exc)
            self.stats["straight_lane_lateral_stabilizer_error_count"] = int(
                self.stats.get("straight_lane_lateral_stabilizer_error_count", 0)
            ) + 1
            self.stats["last_straight_lane_lateral_stabilizer"] = payload
            return ctrl, payload

    def _apply_pending_control_if_ready(self) -> None:
        pending = self._pending_control
        if pending is None:
            return
        if not self._ensure_ego():
            return
        frame_id = self._current_world_frame()
        if not self._should_apply_on_frame(frame_id):
            return
        self._pending_control = None
        self.stats["control_apply_pending_flush_count"] = int(
            self.stats.get("control_apply_pending_flush_count", 0)
        ) + 1
        self._apply_control(
            pending["ctrl"],
            source=str(pending["source"]),
            source_steer=float(pending["source_steer"]),
            steer_norm=float(pending["steer_norm"]),
            steer_clamped=bool(pending["steer_clamped"]),
            defer_if_duplicate=False,
        )

    def _apply_control(
        self,
        ctrl: carla.VehicleControl,
        *,
        source: str,
        source_steer: float,
        steer_norm: float,
        steer_clamped: bool,
        defer_if_duplicate: bool = True,
    ) -> None:
        if not self._ensure_ego():
            return
        frame_id = self._current_world_frame()
        if not self._should_apply_on_frame(frame_id):
            if defer_if_duplicate and source != "watchdog":
                self._defer_control(
                    ctrl,
                    source=source,
                    source_steer=source_steer,
                    steer_norm=steer_norm,
                    steer_clamped=steer_clamped,
                    frame_id=frame_id,
                )
            return
        ctrl = self._maybe_suppress_startup_brake(ctrl, source=source)
        try:
            speed_mps_before_apply = float(self._current_speed_mps())
            ctrl, stabilizer_payload = self._maybe_apply_straight_lane_lateral_stabilizer(
                ctrl,
                speed_mps=speed_mps_before_apply,
            )
            self.ego.apply_control(ctrl)
            if frame_id is not None:
                self._last_applied_frame = frame_id
                if self.stats.get("control_apply_first_frame") is None:
                    self.stats["control_apply_first_frame"] = int(frame_id)
                self.stats["control_apply_last_frame"] = int(frame_id)
                first_frame = self.stats.get("control_apply_first_frame")
                if first_frame is not None:
                    self.stats["control_apply_frame_span"] = int(frame_id) - int(first_frame) + 1
            now_ts = time.time()
            speed_mps = float(self._current_speed_mps())
            self.stats["control_apply_count"] = int(self.stats.get("control_apply_count", 0)) + 1
            if self.stats.get("control_apply_first_ts_sec") is None:
                self.stats["control_apply_first_ts_sec"] = float(now_ts)
            self.stats["control_apply_last_ts_sec"] = float(now_ts)
            self.stats["control_apply_max_throttle"] = max(
                float(self.stats.get("control_apply_max_throttle", 0.0) or 0.0),
                float(ctrl.throttle),
            )
            self.stats["control_apply_max_brake"] = max(
                float(self.stats.get("control_apply_max_brake", 0.0) or 0.0),
                float(ctrl.brake),
            )
            self.stats["control_apply_max_speed_mps"] = max(
                float(self.stats.get("control_apply_max_speed_mps", 0.0) or 0.0),
                speed_mps,
            )
            if source == "watchdog":
                self.stats["control_apply_watchdog_count"] = int(
                    self.stats.get("control_apply_watchdog_count", 0)
                ) + 1
            payload = {
                "ts_sec": time.time(),
                "source": source,
                "frame_id": frame_id,
                "actor_id": self.stats.get("ego_actor_id"),
                "source_steer": float(source_steer),
                "steer_norm": float(steer_norm),
                "steer_clamped": bool(steer_clamped),
                "throttle": float(ctrl.throttle),
                "brake": float(ctrl.brake),
                "steer": float(ctrl.steer),
                "speed_mps": speed_mps,
                "straight_lane_lateral_stabilizer": stabilizer_payload,
            }
            self._append_jsonl(self.control_apply_path, payload)
        except Exception as exc:
            self.last_error = f"apply_control_failed:{exc}"
            self.stats["last_error"] = self.last_error
            self.stats["control_apply_fail_count"] = int(self.stats.get("control_apply_fail_count", 0)) + 1

    def _build_control_from_target(
        self,
        *,
        target_speed: float,
        steer_norm: float,
        accel: float = 0.0,
    ) -> tuple[carla.VehicleControl, bool]:
        ctrl = carla.VehicleControl()
        steer_out = _clamp(float(steer_norm), -1.0, 1.0)
        steer_clamped = abs(steer_out - float(steer_norm)) > 1e-6
        ctrl.steer = steer_out
        if target_speed >= 0.0:
            base = float(target_speed) / max(self.speed_gain, 0.1)
            if accel:
                base += float(accel) * 0.05
            ctrl.throttle = _clamp(base, 0.0, 1.0)
            ctrl.brake = 0.0
        else:
            ctrl.throttle = 0.0
            ctrl.brake = _clamp(abs(float(target_speed)) / max(self.brake_gain, 0.1), 0.0, 1.0)
        return ctrl, steer_clamped

    def _apply_watchdog_if_needed(self) -> None:
        if not self._ensure_ego():
            return
        if self.watchdog_wait_for_first_msg and not self._first_msg_received:
            return
        if (
            self._ego_bound_monotonic is not None
            and (time.monotonic() - self._ego_bound_monotonic) < self.watchdog_arm_delay_sec
        ):
            return
        if self._last_control_wall_sec <= 0.0:
            return
        if (time.time() - self._last_control_wall_sec) <= self.timeout_sec:
            return
        ctrl = carla.VehicleControl(throttle=0.0, brake=1.0, steer=0.0)
        self._apply_control(
            ctrl,
            source="watchdog",
            source_steer=0.0,
            steer_norm=0.0,
            steer_clamped=False,
        )
        self._last_control_wall_sec = time.time()

    def tick(self) -> None:
        self._apply_pending_control_if_ready()
        self._apply_watchdog_if_needed()

    def snapshot(self) -> Dict[str, Any]:
        now = time.time()
        min_period = 1.0 / max(self.publish_rate_hz, 1e-3)
        if self._cached_snapshot is not None and (now - self._last_snapshot_wall_sec) < min_period:
            return {
                "odom": self._cached_snapshot.get("odom"),
                "objects3d": None,
                "markers": None,
                "objects_json": self._cached_snapshot.get("objects_json"),
                "world_frame": self._cached_snapshot.get("world_frame"),
                "world_frame_advanced": False,
                "rx_counts": dict(self._rx_counts),
            }
        if not self._ensure_ego() or self.world is None or self.ego is None:
            return {
                "odom": None,
                "objects3d": None,
                "markers": None,
                "objects_json": None,
                "world_frame": None,
                "world_frame_advanced": False,
                "rx_counts": dict(self._rx_counts),
            }
        try:
            world_snapshot = self.world.get_snapshot()
            elapsed = float(world_snapshot.timestamp.elapsed_seconds)
            frame = int(getattr(world_snapshot, "frame", -1))
            if self._cached_snapshot is not None and self._cached_world_frame is not None and frame == self._cached_world_frame:
                self.stats["world_frame_repeat_count"] = int(self.stats.get("world_frame_repeat_count", 0)) + 1
                self.stats["last_speed_mps"] = float(self._current_speed_mps())
                self.stats["last_error"] = ""
                self.last_error = ""
                return {
                    "odom": self._cached_snapshot.get("odom"),
                    "objects3d": None,
                    "markers": None,
                    "objects_json": self._cached_snapshot.get("objects_json"),
                    "world_frame": self._cached_snapshot.get("world_frame"),
                    "world_frame_advanced": False,
                    "rx_counts": dict(self._rx_counts),
                }
            actors = list(self.world.get_actors())
            filtered = self._filtered_objects(self.ego, actors)
            odom = self._build_odom(elapsed_seconds=elapsed, ego=self.ego)
            objects_json = self._build_objects_json(elapsed_seconds=elapsed, actors=filtered)
            self._rx_counts["odom"] += 1
            self._rx_counts["objects_json"] += 1
            self.stats["snapshot_count"] = int(self.stats.get("snapshot_count", 0)) + 1
            self.stats["rx_counts"] = dict(self._rx_counts)
            self.stats["last_speed_mps"] = float(self._current_speed_mps())
            self.stats["last_error"] = ""
            self.last_error = ""
            payload = {
                "odom": odom,
                "objects3d": None,
                "markers": None,
                "objects_json": objects_json,
                "world_frame": frame,
                "world_frame_advanced": True,
                "rx_counts": dict(self._rx_counts),
            }
            self._cached_snapshot = payload
            self._cached_world_frame = frame
            self._last_snapshot_wall_sec = now
            return payload
        except Exception as exc:
            self.last_error = f"snapshot_failed:{exc}"
            self.stats["last_error"] = self.last_error
            return {
                "odom": None,
                "objects3d": None,
                "markers": None,
                "objects_json": None,
                "world_frame": None,
                "world_frame_advanced": False,
                "rx_counts": dict(self._rx_counts),
            }

    def publish_control(self, msg: Any) -> None:
        self._first_msg_received = True
        self._last_control_wall_sec = time.time()
        if self.control_out_type == "direct":
            data = list(getattr(msg, "data", []) or [])
            if len(data) < 3:
                self.last_error = "direct_control_shape_invalid"
                self.stats["last_error"] = self.last_error
                return
            raw_steer = float(data[2])
            steer = _clamp(raw_steer, -1.0, 1.0)
            ctrl = carla.VehicleControl(
                throttle=_clamp(float(data[0]), 0.0, 1.0),
                brake=_clamp(float(data[1]), 0.0, 1.0),
                steer=steer,
            )
            if float(ctrl.throttle) >= self.startup_brake_suppression_min_throttle:
                self._recent_positive_throttle_monotonic = time.monotonic()
            if self.control_apply_mode == "frame_flush_only":
                self._queue_frame_flush_control(
                    ctrl,
                    source="pending",
                    source_steer=raw_steer,
                    steer_norm=raw_steer,
                    steer_clamped=abs(steer - raw_steer) > 1e-6,
                )
                return
            self._apply_control(
                ctrl,
                source="pending",
                source_steer=raw_steer,
                steer_norm=raw_steer,
                steer_clamped=abs(steer - raw_steer) > 1e-6,
            )
            return
        if self.control_out_type == "ackermann":
            if AckermannDriveStamped is None or not isinstance(msg, AckermannDriveStamped):
                self.last_error = "ackermann_control_type_unavailable"
                self.stats["last_error"] = self.last_error
                return
            steer_norm = float(msg.drive.steering_angle) / max(self.max_steer_angle, 1e-3)
            ctrl, steer_clamped = self._build_control_from_target(
                target_speed=float(msg.drive.speed),
                steer_norm=steer_norm,
                accel=float(msg.drive.acceleration),
            )
            if self.control_apply_mode == "frame_flush_only":
                self._queue_frame_flush_control(
                    ctrl,
                    source="pending",
                    source_steer=float(msg.drive.steering_angle),
                    steer_norm=steer_norm,
                    steer_clamped=steer_clamped,
                )
                return
            self._apply_control(
                ctrl,
                source="pending",
                source_steer=float(msg.drive.steering_angle),
                steer_norm=steer_norm,
                steer_clamped=steer_clamped,
            )
            return
        if self.control_out_type == "twist":
            if Twist is None or not isinstance(msg, Twist):
                self.last_error = "twist_control_type_unavailable"
                self.stats["last_error"] = self.last_error
                return
            steer_norm = float(msg.angular.z)
            ctrl, steer_clamped = self._build_control_from_target(
                target_speed=float(msg.linear.x),
                steer_norm=steer_norm,
                accel=0.0,
            )
            if self.control_apply_mode == "frame_flush_only":
                self._queue_frame_flush_control(
                    ctrl,
                    source="pending",
                    source_steer=steer_norm,
                    steer_norm=steer_norm,
                    steer_clamped=steer_clamped,
                )
                return
            self._apply_control(
                ctrl,
                source="pending",
                source_steer=steer_norm,
                steer_norm=steer_norm,
                steer_clamped=steer_clamped,
            )
            return
        self.last_error = f"unsupported_control_out_type:{self.control_out_type}"
        self.stats["last_error"] = self.last_error

    def transport_summary(self) -> Dict[str, Any]:
        return {
            "mode": "carla_direct",
            "gt_source": "carla_world_snapshot_direct",
            "control_apply_path": "bridge_direct_actor_apply",
            "control_apply_mode": self.control_apply_mode,
            "stale_world_frame_policy": self.stale_world_frame_policy,
            "tick_owner": "runner_harness_world_tick",
            "uses_ros2_gt": False,
            "uses_ros2_control_bridge": False,
            "requires_ros2_reexec": False,
            "route_command_mode": self.route_command_mode,
            "route_command_path": "cyber_direct_bridge_command_path",
            "require_no_ros2_runtime": self.require_no_ros2_runtime,
            "using_ros2_message_runtime": ROS2_MESSAGE_RUNTIME_AVAILABLE,
            "carla_host": self.carla_host,
            "carla_port": self.carla_port,
            "ego_role_name": self.ego_role_name,
            "poll_hz": self.publish_rate_hz,
            "obstacle_radius_m": self.radius_m,
            "max_obstacles": self.max_obstacles,
        }

    def write_artifacts(self) -> None:
        self.stats["last_error"] = self.last_error
        self._write_json_file(self.stats_path, dict(self.stats))
        self._write_json_file(self.actor_snapshot_path, dict(self._cached_actor_snapshot or {}))
