from __future__ import annotations

import argparse
import math
import sys
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

        self._connect_carla()
        if self.control_type == "ackermann":
            if AckermannDriveStamped is None:
                raise RuntimeError("ackermann_msgs is unavailable, cannot subscribe ackermann control")
            self.create_subscription(AckermannDriveStamped, control_topic, self._on_ackermann, 10)
        elif self.control_type == "twist":
            if Twist is None:
                raise RuntimeError("geometry_msgs/Twist is unavailable, cannot subscribe twist control")
            self.create_subscription(Twist, control_topic, self._on_twist, 10)
        else:
            raise RuntimeError(f"unsupported control_type={self.control_type}, expected ackermann|twist")
        self.create_timer(0.2, self._watchdog)
        self.get_logger().info(
            f"Bridge listening on {control_topic} ({self.control_type}), "
            f"carla={carla_host}:{carla_port}, ego_role={ego_role_name}"
        )

    def _connect_carla(self) -> None:
        try:
            self.client = carla.Client(self.carla_host, self.carla_port)
            self.client.set_timeout(2.0)
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
                        self.get_logger().info(
                            f"control target bound actor_id={v.id} role={role or '<empty>'}"
                        )
                    return v
            if not vehicles:
                return None
            # Fallback for scenes where role_name is absent/mismatched after map reload.
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

    def _apply_control(self, target_speed: float, steer_norm: float, accel: float = 0.0) -> None:
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
            return
        self._warned_no_ego = False

        ctrl = carla.VehicleControl()
        ctrl.steer = clamp(steer_norm, -1.0, 1.0)

        if target_speed >= 0:
            base = target_speed / max(self.speed_gain, 0.1)
            if accel:
                base += accel * 0.05
            ctrl.throttle = clamp(base, 0.0, 1.0)
            ctrl.brake = 0.0
        else:
            ctrl.throttle = 0.0
            ctrl.brake = clamp(abs(target_speed) / max(self.brake_gain, 0.1), 0.0, 1.0)

        try:
            self.ego.apply_control(ctrl)
            now = time.monotonic()
            if now - self._last_apply_log_sec > 2.0:
                self._last_apply_log_sec = now
                role = (self.ego.attributes or {}).get("role_name", "")
                self.get_logger().info(
                    f"apply_control actor_id={self.ego.id} role={role or '<empty>'} "
                    f"throttle={ctrl.throttle:.3f} brake={ctrl.brake:.3f} steer={ctrl.steer:.3f}"
                )
        except Exception as exc:
            self.get_logger().error(f"apply_control failed: {exc}")

    def _on_ackermann(self, msg: AckermannDriveStamped) -> None:
        self.last_msg_time = self.get_clock().now()
        steer_norm = msg.drive.steering_angle / max(self.max_steer_angle, 1e-3)
        self._apply_control(msg.drive.speed, steer_norm, msg.drive.acceleration)

    def _on_twist(self, msg: Twist) -> None:
        self.last_msg_time = self.get_clock().now()
        self._apply_control(msg.linear.x, msg.angular.z, 0.0)

    def _watchdog(self) -> None:
        elapsed = (self.get_clock().now() - self.last_msg_time).nanoseconds / 1e9
        if elapsed > self.timeout_sec:
            if self.ego:
                try:
                    self.ego.apply_control(carla.VehicleControl(throttle=0.0, brake=1.0, steer=0.0))
                except Exception:
                    pass
            self.last_msg_time = self.get_clock().now()


def parse_args():
    ap = argparse.ArgumentParser(description="ROS2 Ackermann -> CARLA VehicleControl bridge")
    ap.add_argument("--carla-host", default="127.0.0.1")
    ap.add_argument("--carla-port", type=int, default=2000)
    ap.add_argument("--control-topic", default="/tb/ego/control_cmd")
    ap.add_argument("--control-type", default="ackermann", choices=["ackermann", "twist"])
    ap.add_argument("--ego-role-name", default="ego")
    ap.add_argument("--max-steer-angle", type=float, default=0.6)
    ap.add_argument("--timeout-sec", type=float, default=0.8)
    ap.add_argument("--speed-gain", type=float, default=10.0, help="speed (m/s) to throttle mapping denominator")
    ap.add_argument("--brake-gain", type=float, default=5.0)
    return ap.parse_args()


def main():
    args = parse_args()
    rclpy.init(args=None)
    node = CarlaControlBridge(
        carla_host=args.carla_host,
        carla_port=args.carla_port,
        control_topic=args.control_topic,
        ego_role_name=args.ego_role_name,
        control_type=args.control_type,
        max_steer_angle=args.max_steer_angle,
        timeout_sec=args.timeout_sec,
        speed_gain=args.speed_gain,
        brake_gain=args.brake_gain,
    )
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    rclpy.try_shutdown()


if __name__ == "__main__":
    main()
