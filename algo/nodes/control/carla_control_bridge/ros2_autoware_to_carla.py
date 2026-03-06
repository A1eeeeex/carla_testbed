from __future__ import annotations

import argparse
import time
from typing import Optional

import carla
import rclpy
try:
    from ackermann_msgs.msg import AckermannDriveStamped
except Exception:
    AckermannDriveStamped = None
try:
    from geometry_msgs.msg import Twist
except Exception:
    Twist = None
try:
    from std_msgs.msg import Float32MultiArray
except Exception:
    Float32MultiArray = None
from rclpy.node import Node


def clamp(val: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, val))


class CarlaControlBridge(Node):
    def __init__(
        self,
        carla_host: str,
        carla_port: int,
        control_topic: str,
        ego_role_name: str,
        control_type: str,
        max_steer_angle: float,
        timeout_sec: float,
        speed_gain: float,
        brake_gain: float,
        apply_hz: float,
        sync_to_world_tick: bool,
        dryrun: bool,
        connect_timeout_sec: float,
        watchdog_wait_for_first_msg: bool,
        watchdog_arm_delay_sec: float,
        startup_brake_suppression_enabled: bool,
        startup_brake_suppression_speed_mps: float,
        startup_brake_suppression_max_brake: float,
        startup_brake_suppression_min_throttle: float,
        startup_brake_suppression_hold_sec: float,
        startup_brake_recent_throttle_window_sec: float,
    ) -> None:
        super().__init__("carla_control_bridge")
        self.carla_host = carla_host
        self.carla_port = carla_port
        self.control_topic = control_topic
        self.ego_role_name = ego_role_name
        self.control_type = str(control_type or "ackermann").lower()
        self.max_steer_angle = max_steer_angle
        self.timeout_sec = timeout_sec
        self.speed_gain = speed_gain
        self.brake_gain = brake_gain
        self.apply_hz = max(float(apply_hz), 1.0)
        self.sync_to_world_tick = bool(sync_to_world_tick)
        self.dryrun = dryrun
        self.connect_timeout_sec = max(float(connect_timeout_sec), 0.5)
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

        self.client: Optional[carla.Client] = None
        self.world: Optional[carla.World] = None
        self.ego: Optional[carla.Vehicle] = None
        self.last_msg_time = self.get_clock().now()
        self._warned_no_ego = False
        self._warned_fallback_role = False
        self._last_discovery_sec = 0.0
        self._last_discovery_warn_sec = 0.0
        self._last_apply_log_sec = 0.0
        self._bound_actor_id: Optional[int] = None

        self._pending_ctrl: Optional[carla.VehicleControl] = None
        self._pending_source_steer: float = 0.0
        self._pending_steer_norm: float = 0.0
        self._pending_steer_clamped: bool = False
        self._last_applied_frame: Optional[int] = None
        self._received_count = 0
        self._applied_count = 0
        self._dropped_same_frame_count = 0
        self._first_msg_received = False
        self._ego_bound_monotonic: Optional[float] = None
        self._recent_positive_throttle_monotonic: Optional[float] = None
        self._startup_brake_suppressed_count = 0

        self._connect_carla()
        if self.control_type == "ackermann":
            if AckermannDriveStamped is None:
                raise RuntimeError("ackermann_msgs is unavailable, cannot subscribe ackermann control")
            self.create_subscription(AckermannDriveStamped, control_topic, self._on_ackermann, 10)
        elif self.control_type == "twist":
            if Twist is None:
                raise RuntimeError("geometry_msgs/Twist is unavailable, cannot subscribe twist control")
            self.create_subscription(Twist, control_topic, self._on_twist, 10)
        elif self.control_type == "direct":
            if Float32MultiArray is None:
                raise RuntimeError("std_msgs/Float32MultiArray is unavailable, cannot subscribe direct control")
            self.create_subscription(Float32MultiArray, control_topic, self._on_direct, 10)
        else:
            raise RuntimeError(f"unsupported control_type={self.control_type}, expected ackermann|twist|direct")
        self.create_timer(1.0 / self.apply_hz, self._flush_pending_control)
        self.get_logger().info(
            f"Bridge listening on {control_topic} ({self.control_type}), "
            f"carla={carla_host}:{carla_port}, ego_role={ego_role_name}, dryrun={self.dryrun}, "
            f"apply_hz={self.apply_hz:.1f}, sync_to_world_tick={self.sync_to_world_tick}, "
            f"watchdog_wait_for_first_msg={self.watchdog_wait_for_first_msg}, "
            f"watchdog_arm_delay_sec={self.watchdog_arm_delay_sec:.2f}, "
            f"startup_brake_suppression={self.startup_brake_suppression_enabled}"
        )

    def _connect_carla(self) -> None:
        try:
            self.client = carla.Client(self.carla_host, self.carla_port)
            self.client.set_timeout(self.connect_timeout_sec)
            self.world = self.client.get_world()
            self.ego = self._discover_ego(force_world_refresh=False)
            if self.ego is None:
                self.get_logger().warn("ego vehicle not found; control commands will be buffered")
        except Exception as exc:
            self.get_logger().error(f"Failed to connect to CARLA: {exc}")

    def _discover_ego(self, *, force_world_refresh: bool) -> Optional[carla.Vehicle]:
        if self.client is None:
            return None
        try:
            if force_world_refresh or self.world is None:
                self.world = self.client.get_world()
            world = self.world
            if world is None:
                return None
            vehicles = list(world.get_actors().filter("vehicle.*"))
            for v in vehicles:
                role = (v.attributes or {}).get("role_name")
                if role == self.ego_role_name or role in ["ego", "hero", "tb_ego"]:
                    if self._bound_actor_id != int(v.id):
                        self._bound_actor_id = int(v.id)
                        self._ego_bound_monotonic = time.monotonic()
                        self.get_logger().info(
                            f"control target bound actor_id={v.id} role={role or '<empty>'}"
                        )
                    return v
            if not vehicles:
                return None
            non_front = [v for v in vehicles if "front" not in ((v.attributes or {}).get("role_name", "").lower())]
            candidate_pool = non_front if non_front else vehicles
            candidate = max(candidate_pool, key=lambda v: int(v.id))
            if not self._warned_fallback_role:
                roles = [str((v.attributes or {}).get("role_name", "")) for v in vehicles]
                self.get_logger().warning(
                    f"ego role '{self.ego_role_name}' not found; fallback to actor_id={candidate.id}, roles={roles}"
                )
                self._warned_fallback_role = True
            if self._bound_actor_id != int(candidate.id):
                self._bound_actor_id = int(candidate.id)
                self._ego_bound_monotonic = time.monotonic()
                role = (candidate.attributes or {}).get("role_name", "")
                self.get_logger().info(
                    f"control target fallback actor_id={candidate.id} role={role or '<empty>'}"
                )
            return candidate
        except Exception as exc:
            now = time.monotonic()
            if now - self._last_discovery_warn_sec > 2.0:
                self._last_discovery_warn_sec = now
                self.get_logger().warning(f"discover ego failed: {exc}")
            return None

    def _warn_no_ego_once(self) -> None:
        now = time.monotonic()
        if self._warned_no_ego and (now - self._last_discovery_warn_sec) < 2.0:
            return
        self._last_discovery_warn_sec = now
        self.get_logger().warning("no ego found yet; skip control")
        self._warned_no_ego = True

    def _ensure_ego(self) -> bool:
        if self.world is None:
            self._connect_carla()
        if self.ego is None:
            now_sec = time.monotonic()
            force_refresh = (now_sec - self._last_discovery_sec) > 0.5
            self.ego = self._discover_ego(force_world_refresh=force_refresh)
            if force_refresh:
                self._last_discovery_sec = now_sec
        if self.ego is None:
            self._warn_no_ego_once()
            return False
        self._warned_no_ego = False
        return True

    def _queue_control(
        self,
        ctrl: carla.VehicleControl,
        *,
        source_steer: float,
        steer_norm: float,
        steer_clamped: bool,
    ) -> None:
        self.last_msg_time = self.get_clock().now()
        self._pending_ctrl = ctrl
        self._pending_source_steer = float(source_steer)
        self._pending_steer_norm = float(steer_norm)
        self._pending_steer_clamped = bool(steer_clamped)
        self._received_count += 1
        self._first_msg_received = True
        if float(ctrl.throttle) >= self.startup_brake_suppression_min_throttle:
            self._recent_positive_throttle_monotonic = time.monotonic()

    def _build_control_from_target(
        self,
        target_speed: float,
        steer_norm: float,
        accel: float = 0.0,
    ) -> tuple[carla.VehicleControl, bool]:
        ctrl = carla.VehicleControl()
        steer_out = clamp(float(steer_norm), -1.0, 1.0)
        steer_clamped = abs(steer_out - float(steer_norm)) > 1e-6
        ctrl.steer = steer_out

        if target_speed >= 0.0:
            base = float(target_speed) / max(self.speed_gain, 0.1)
            if accel:
                base += float(accel) * 0.05
            ctrl.throttle = clamp(base, 0.0, 1.0)
            ctrl.brake = 0.0
        else:
            ctrl.throttle = 0.0
            ctrl.brake = clamp(abs(float(target_speed)) / max(self.brake_gain, 0.1), 0.0, 1.0)
        return ctrl, steer_clamped

    def _current_world_frame(self) -> Optional[int]:
        if self.world is None:
            return None
        try:
            snap = self.world.get_snapshot()
            return int(getattr(snap, "frame", -1))
        except Exception:
            return None

    def _current_speed_mps(self) -> float:
        if self.ego is None:
            return 0.0
        try:
            return float(self.ego.get_velocity().length())
        except Exception:
            return 0.0

    def _maybe_suppress_startup_brake(
        self,
        ctrl: carla.VehicleControl,
        *,
        source: str,
    ) -> carla.VehicleControl:
        if not self.startup_brake_suppression_enabled:
            return ctrl
        if source != "pending":
            return ctrl
        if float(ctrl.brake) <= 0.0 or float(ctrl.brake) > self.startup_brake_suppression_max_brake:
            return ctrl

        now = time.monotonic()
        speed_mps = self._current_speed_mps()
        recent_throttle = (
            self._recent_positive_throttle_monotonic is not None
            and (now - self._recent_positive_throttle_monotonic) <= self.startup_brake_recent_throttle_window_sec
        )
        within_startup_hold = (
            self._ego_bound_monotonic is not None
            and (now - self._ego_bound_monotonic) <= self.startup_brake_suppression_hold_sec
        )
        if speed_mps > self.startup_brake_suppression_speed_mps:
            return ctrl
        if not recent_throttle and not within_startup_hold:
            return ctrl

        ctrl.brake = 0.0
        self._startup_brake_suppressed_count += 1
        if self._startup_brake_suppressed_count <= 5 or (self._startup_brake_suppressed_count % 20) == 0:
            self.get_logger().info(
                f"startup brake suppressed count={self._startup_brake_suppressed_count} "
                f"speed={speed_mps:.3f} recent_throttle={recent_throttle} within_hold={within_startup_hold}"
            )
        return ctrl

    def _flush_pending_control(self) -> None:
        if not self._ensure_ego():
            return

        frame_id = self._current_world_frame() if self.sync_to_world_tick else None
        if (
            self.sync_to_world_tick
            and frame_id is not None
            and self._last_applied_frame is not None
            and frame_id == self._last_applied_frame
        ):
            self._dropped_same_frame_count += 1
            return

        elapsed = (self.get_clock().now() - self.last_msg_time).nanoseconds / 1e9
        source = "pending"
        if elapsed > self.timeout_sec:
            if self.watchdog_wait_for_first_msg and not self._first_msg_received:
                return
            if (
                self._ego_bound_monotonic is not None
                and (time.monotonic() - self._ego_bound_monotonic) < self.watchdog_arm_delay_sec
            ):
                return
            ctrl = carla.VehicleControl(throttle=0.0, brake=1.0, steer=0.0)
            source_steer = 0.0
            steer_norm = 0.0
            steer_clamped = False
            source = "watchdog"
        else:
            if self._pending_ctrl is None:
                return
            ctrl = self._pending_ctrl
            source_steer = self._pending_source_steer
            steer_norm = self._pending_steer_norm
            steer_clamped = self._pending_steer_clamped
            ctrl = self._maybe_suppress_startup_brake(ctrl, source=source)

        now = time.monotonic()
        if now - self._last_apply_log_sec > 1.0:
            self._last_apply_log_sec = now
            role = (self.ego.attributes or {}).get("role_name", "") if self.ego else ""
            self.get_logger().info(
                f"apply frame={frame_id} source={source} actor_id={self.ego.id if self.ego else -1} "
                f"role={role or '<empty>'} src_steer={source_steer:.3f} norm_steer={steer_norm:.3f} "
                f"carla_steer={ctrl.steer:.3f} clamped={steer_clamped} "
                f"throttle={ctrl.throttle:.3f} brake={ctrl.brake:.3f} "
                f"rx={self._received_count} applied={self._applied_count} drop_same_frame={self._dropped_same_frame_count}"
            )

        if not self.dryrun:
            try:
                self.ego.apply_control(ctrl)
            except Exception as exc:
                self.get_logger().error(f"apply_control failed: {exc}")
                return
        self._applied_count += 1
        if frame_id is not None:
            self._last_applied_frame = frame_id

    def _on_ackermann(self, msg: AckermannDriveStamped) -> None:
        steer_norm = float(msg.drive.steering_angle) / max(self.max_steer_angle, 1e-3)
        ctrl, steer_clamped = self._build_control_from_target(
            target_speed=float(msg.drive.speed),
            steer_norm=steer_norm,
            accel=float(msg.drive.acceleration),
        )
        self._queue_control(
            ctrl,
            source_steer=float(msg.drive.steering_angle),
            steer_norm=steer_norm,
            steer_clamped=steer_clamped,
        )

    def _on_twist(self, msg: Twist) -> None:
        steer_norm = float(msg.angular.z)
        ctrl, steer_clamped = self._build_control_from_target(
            target_speed=float(msg.linear.x),
            steer_norm=steer_norm,
            accel=0.0,
        )
        self._queue_control(
            ctrl,
            source_steer=steer_norm,
            steer_norm=steer_norm,
            steer_clamped=steer_clamped,
        )

    def _on_direct(self, msg: Float32MultiArray) -> None:
        data = list(msg.data or [])
        if len(data) < 3:
            self.get_logger().warning("direct control expects [throttle, brake, steer]")
            return
        raw_steer = float(data[2])
        steer = clamp(raw_steer, -1.0, 1.0)
        ctrl = carla.VehicleControl()
        ctrl.throttle = clamp(float(data[0]), 0.0, 1.0)
        ctrl.brake = clamp(float(data[1]), 0.0, 1.0)
        ctrl.steer = steer
        self._queue_control(
            ctrl,
            source_steer=raw_steer,
            steer_norm=raw_steer,
            steer_clamped=abs(steer - raw_steer) > 1e-6,
        )


def parse_args():
    ap = argparse.ArgumentParser(description="ROS2 Ackermann -> CARLA VehicleControl bridge")
    ap.add_argument("--carla-host", default="127.0.0.1")
    ap.add_argument("--carla-port", type=int, default=2000)
    ap.add_argument("--connect-timeout-sec", type=float, default=6.0)
    ap.add_argument("--control-topic", default="/tb/ego/control_cmd")
    ap.add_argument("--control-type", default="ackermann", choices=["ackermann", "twist", "direct"])
    ap.add_argument("--ego-role-name", default="ego")
    ap.add_argument("--max-steer-angle", type=float, default=0.6)
    ap.add_argument("--timeout-sec", type=float, default=0.8)
    ap.add_argument("--speed-gain", type=float, default=10.0, help="speed (m/s) to throttle mapping denominator")
    ap.add_argument("--brake-gain", type=float, default=5.0)
    ap.add_argument("--apply-hz", type=float, default=20.0, help="control apply frequency, one command per apply tick")
    ap.add_argument(
        "--sync-to-world-tick",
        dest="sync_to_world_tick",
        action="store_true",
        help="gate apply to at most one command per CARLA world frame",
    )
    ap.add_argument(
        "--no-sync-to-world-tick",
        dest="sync_to_world_tick",
        action="store_false",
        help="disable world-frame gate and apply purely by timer frequency",
    )
    ap.set_defaults(sync_to_world_tick=True)
    ap.add_argument(
        "--watchdog-wait-for-first-msg",
        dest="watchdog_wait_for_first_msg",
        action="store_true",
        help="do not arm watchdog braking until at least one control msg has been received",
    )
    ap.add_argument(
        "--no-watchdog-wait-for-first-msg",
        dest="watchdog_wait_for_first_msg",
        action="store_false",
        help="arm watchdog immediately even before the first control msg",
    )
    ap.set_defaults(watchdog_wait_for_first_msg=True)
    ap.add_argument("--watchdog-arm-delay-sec", type=float, default=1.5)
    ap.add_argument(
        "--startup-brake-suppression-enabled",
        dest="startup_brake_suppression_enabled",
        action="store_true",
        help="suppress small startup brake pulses while ego is nearly stopped",
    )
    ap.add_argument(
        "--startup-brake-suppression-disabled",
        dest="startup_brake_suppression_enabled",
        action="store_false",
        help="disable startup brake suppression",
    )
    ap.set_defaults(startup_brake_suppression_enabled=True)
    ap.add_argument("--startup-brake-suppression-speed-mps", type=float, default=1.0)
    ap.add_argument("--startup-brake-suppression-max-brake", type=float, default=0.2)
    ap.add_argument("--startup-brake-suppression-min-throttle", type=float, default=0.3)
    ap.add_argument("--startup-brake-suppression-hold-sec", type=float, default=3.0)
    ap.add_argument("--startup-brake-recent-throttle-window-sec", type=float, default=1.0)
    ap.add_argument("--dryrun", action="store_true", help="log mapped controls without applying to CARLA")
    return ap.parse_args()


def main():
    args = parse_args()
    rclpy.init(args=None)
    node = CarlaControlBridge(
        carla_host=args.carla_host,
        carla_port=args.carla_port,
        connect_timeout_sec=args.connect_timeout_sec,
        control_topic=args.control_topic,
        ego_role_name=args.ego_role_name,
        control_type=args.control_type,
        max_steer_angle=args.max_steer_angle,
        timeout_sec=args.timeout_sec,
        speed_gain=args.speed_gain,
        brake_gain=args.brake_gain,
        apply_hz=args.apply_hz,
        sync_to_world_tick=args.sync_to_world_tick,
        dryrun=args.dryrun,
        watchdog_wait_for_first_msg=args.watchdog_wait_for_first_msg,
        watchdog_arm_delay_sec=args.watchdog_arm_delay_sec,
        startup_brake_suppression_enabled=args.startup_brake_suppression_enabled,
        startup_brake_suppression_speed_mps=args.startup_brake_suppression_speed_mps,
        startup_brake_suppression_max_brake=args.startup_brake_suppression_max_brake,
        startup_brake_suppression_min_throttle=args.startup_brake_suppression_min_throttle,
        startup_brake_suppression_hold_sec=args.startup_brake_suppression_hold_sec,
        startup_brake_recent_throttle_window_sec=args.startup_brake_recent_throttle_window_sec,
    )
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    rclpy.try_shutdown()


if __name__ == "__main__":
    main()
