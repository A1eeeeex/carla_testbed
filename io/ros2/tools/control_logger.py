from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy, DurabilityPolicy

# Try common control message types
try:
    from ackermann_msgs.msg import AckermannDriveStamped
except ImportError:  # pragma: no cover - optional
    AckermannDriveStamped = None
try:
    from autoware_auto_control_msgs.msg import AckermannControlCommand
except ImportError:  # pragma: no cover - optional
    AckermannControlCommand = None


def msg_to_dict(msg: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if hasattr(msg, "header"):
        out["stamp"] = {
            "sec": msg.header.stamp.sec,
            "nanosec": msg.header.stamp.nanosec,
        }
        out["frame_id"] = msg.header.frame_id
    # AckermannDriveStamped
    drive = getattr(msg, "drive", None)
    if drive:
        out.update(
            {
                "steering_angle": getattr(drive, "steering_angle", None),
                "steering_angle_velocity": getattr(drive, "steering_angle_velocity", None),
                "speed": getattr(drive, "speed", None),
                "acceleration": getattr(drive, "acceleration", None),
                "jerk": getattr(drive, "jerk", None),
            }
        )
    # Autoware AckermannControlCommand
    if hasattr(msg, "lateral"):
        lat = msg.lateral
        out["lateral"] = {
            "steering_tire_angle": getattr(lat, "steering_tire_angle", None),
            "steering_tire_rotation_rate": getattr(lat, "steering_tire_rotation_rate", None),
        }
    if hasattr(msg, "longitudinal"):
        lon = msg.longitudinal
        out["longitudinal"] = {
            "speed": getattr(lon, "speed", None),
            "acceleration": getattr(lon, "acceleration", None),
            "jerk": getattr(lon, "jerk", None),
        }
    return out


class ControlLogger(Node):
    def __init__(self, topic: str, out_path: Path, max_msgs: int | None):
        super().__init__("control_logger")
        self.topic = topic
        self.out = out_path
        self.out.parent.mkdir(parents=True, exist_ok=True)
        qos = QoSProfile(
            history=HistoryPolicy.KEEP_LAST,
            depth=10,
            reliability=ReliabilityPolicy.RELIABLE,
            durability=DurabilityPolicy.VOLATILE,
        )
        self.count = 0
        self.max_msgs = max_msgs
        # choose message type lazily
        msg_types = [t for t in [AckermannDriveStamped, AckermannControlCommand] if t is not None]
        if not msg_types:
            raise RuntimeError("No control message types available (ackermann_msgs or autoware_auto_control_msgs missing)")
        # subscribe using AnyMsg to be generic if preferred? We'll use first available type
        self.subscription = self.create_subscription(msg_types[0], topic, self._cb, qos)
        self.get_logger().info(f"Logging {topic} to {out_path}")

    def _cb(self, msg):
        data = msg_to_dict(msg)
        data["_node_stamp"] = self.get_clock().now().nanoseconds / 1e9
        with self.out.open("a") as f:
            f.write(json.dumps(data) + "\n")
        self.count += 1
        if self.max_msgs and self.count >= self.max_msgs:
            self.get_logger().info("Reached max_msgs, shutting down")
            rclpy.shutdown()


def main():
    ap = argparse.ArgumentParser(description="Log control topic to JSONL")
    ap.add_argument("--topic", default="/control/command/control_cmd")
    ap.add_argument("--out", type=Path, required=True, help="output jsonl path")
    ap.add_argument("--max-msgs", type=int, default=None, help="stop after N messages")
    args = ap.parse_args()

    rclpy.init()
    node = ControlLogger(args.topic, args.out, args.max_msgs)
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    rclpy.try_shutdown()


if __name__ == "__main__":
    main()
