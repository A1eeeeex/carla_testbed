from __future__ import annotations

import argparse
import math
import sys
import time
from typing import Optional

import carla
import rclpy
from ackermann_msgs.msg import AckermannDriveStamped
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
        self.max_steer_angle = max_steer_angle
        self.timeout_sec = timeout_sec
        self.speed_gain = speed_gain
        self.brake_gain = brake_gain

        self.client: Optional[carla.Client] = None
        self.world: Optional[carla.World] = None
        self.ego: Optional[carla.Vehicle] = None
        self.last_msg_time = self.get_clock().now()

        self._connect_carla()
        self.create_subscription(AckermannDriveStamped, control_topic, self._on_cmd, 10)
        self.create_timer(0.2, self._watchdog)
        self.get_logger().info(
            f"Bridge listening on {control_topic}, carla={carla_host}:{carla_port}, ego_role={ego_role_name}"
        )

    def _connect_carla(self) -> None:
        try:
            self.client = carla.Client(self.carla_host, self.carla_port)
            self.client.set_timeout(2.0)
            self.world = self.client.get_world()
            self.ego = None
            for v in self.world.get_actors().filter("vehicle.*"):
                role = (v.attributes or {}).get("role_name")
                if role == self.ego_role_name or (self.ego_role_name == "ego" and role in ["ego", "hero", "tb_ego"]):
                    self.ego = v
                    break
            if self.ego is None:
                self.get_logger().warn("ego vehicle not found; control commands will be buffered")
        except Exception as exc:
            self.get_logger().error(f"Failed to connect to CARLA: {exc}")

    def _apply_control(self, msg: AckermannDriveStamped) -> None:
        if self.ego is None:
            # retry discovery lazily
            try:
                for v in self.world.get_actors().filter("vehicle.*"):
                    role = (v.attributes or {}).get("role_name")
                    if role == self.ego_role_name or role in ["ego", "hero", "tb_ego"]:
                        self.ego = v
                        break
            except Exception:
                pass
        if self.ego is None:
            self.get_logger().warn_once("no ego found yet; skip control")
            return

        drive = msg.drive
        ctrl = carla.VehicleControl()
        steer_norm = drive.steering_angle / max(self.max_steer_angle, 1e-3)
        ctrl.steer = clamp(steer_norm, -1.0, 1.0)

        target_speed = drive.speed
        accel = drive.acceleration
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
        except Exception as exc:
            self.get_logger().error(f"apply_control failed: {exc}")

    def _on_cmd(self, msg: AckermannDriveStamped) -> None:
        self.last_msg_time = self.get_clock().now()
        self._apply_control(msg)

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
