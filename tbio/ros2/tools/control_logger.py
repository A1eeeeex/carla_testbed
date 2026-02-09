from __future__ import annotations

import argparse
import base64
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy, DurabilityPolicy
from rclpy.any_msg import AnyMsg
from rosidl_runtime_py import message_to_ordereddict

# Try common control message types
try:
    from ackermann_msgs.msg import AckermannDriveStamped
except ImportError:  # pragma: no cover - optional
    AckermannDriveStamped = None
try:
    from autoware_auto_control_msgs.msg import AckermannControlCommand
except ImportError:  # pragma: no cover - optional
    AckermannControlCommand = None
try:
    from autoware_control_msgs.msg import Control  # legacy type
except ImportError:  # pragma: no cover - optional
    Control = None


def msg_to_dict(msg: Any) -> Dict[str, Any]:
    # Try structured fields, else fallback to generic ordered dict
    try:
        out: Dict[str, Any] = {}
        if hasattr(msg, "header"):
            out["stamp"] = {
                "sec": msg.header.stamp.sec,
                "nanosec": msg.header.stamp.nanosec,
            }
            out["frame_id"] = msg.header.frame_id
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
        if out:
            return out
    except Exception:
        pass
    try:
        return message_to_ordereddict(msg)
    except Exception:
        return {"raw_repr": repr(msg)}


class ControlLogger(Node):
    def __init__(
        self,
        topic: str,
        out_path: Path,
        max_msgs: int | None,
        *,
        force_anymsg: bool = False,
        reliability: ReliabilityPolicy = ReliabilityPolicy.BEST_EFFORT,
    ):
        super().__init__("control_logger")
        self.topic = topic
        self.out = out_path
        self.out.parent.mkdir(parents=True, exist_ok=True)
        qos = QoSProfile(
            history=HistoryPolicy.KEEP_LAST,
            depth=10,
            reliability=reliability,
            durability=DurabilityPolicy.VOLATILE,
        )
        self.count = 0
        self.max_msgs = max_msgs
        self.topic_type = self._resolve_topic_type(topic)

        msg_types: List[Any] = [t for t in [AckermannDriveStamped, AckermannControlCommand, Control] if t is not None]
        subscription = None
        try:
            if not force_anymsg and msg_types:
                subscription = self.create_subscription(msg_types[0], topic, self._cb, qos)
                self.get_logger().info(
                    f"Logging {topic} as {msg_types[0].__name__} -> {out_path} "
                    f"(QoS reliability={reliability.name})"
                )
            else:
                subscription = self.create_subscription(AnyMsg, topic, self._cb_any, qos)
                mode = "force AnyMsg" if force_anymsg else "fallback AnyMsg"
                self.get_logger().warn(
                    f"{mode}; subscribing AnyMsg on {topic} (QoS reliability={reliability.name}, type={self.topic_type})"
                )
        except Exception as exc:  # pragma: no cover - defensive
            self.get_logger().error(f"Failed to create subscription on {topic}: {exc}")
            subscription = None

        if subscription is None:
            self.get_logger().error("Subscription creation failed; exiting so adapter can surface the error")
            raise SystemExit(1)

        self.subscription = subscription

    def _write(self, data: Dict[str, Any]):
        if self.topic_type and "type" not in data:
            data["type"] = self.topic_type
        data["_node_stamp"] = self.get_clock().now().nanoseconds / 1e9
        with self.out.open("a") as f:
            f.write(json.dumps(data) + "\n")
        self.count += 1
        if self.max_msgs and self.count >= self.max_msgs:
            self.get_logger().info("Reached max_msgs, shutting down")
            rclpy.shutdown()

    def _cb(self, msg):
        self._write(msg_to_dict(msg))

    def _cb_any(self, msg: AnyMsg):
        raw = bytes(getattr(msg, "_buff", b""))
        payload = {
            "type": self.topic_type or "unknown",
            "raw_len": len(raw),
        }
        try:
            payload["raw_b64"] = base64.b64encode(raw).decode("ascii")
        except Exception:  # pragma: no cover - extremely unlikely
            payload["raw_repr"] = repr(raw)
        self._write(payload)

    def _resolve_topic_type(self, topic: str) -> Optional[str]:
        try:
            for name, types in self.get_topic_names_and_types():
                if name == topic and types:
                    return ",".join(types)
        except Exception:
            return None
        return None


def main():
    ap = argparse.ArgumentParser(description="Log control topic to JSONL")
    ap.add_argument("--topic", default="/control/command/control_cmd")
    ap.add_argument("--out", type=Path, required=True, help="output jsonl path")
    ap.add_argument("--max-msgs", type=int, default=None, help="stop after N messages")
    ap.add_argument("--force-anymsg", action="store_true", help="always subscribe with AnyMsg")
    ap.add_argument(
        "--reliability",
        choices=["best_effort", "reliable"],
        default="best_effort",
        help="QoS reliability for subscription (SensorData-friendly by default)",
    )
    args = ap.parse_args()

    rclpy.init()
    reliability = ReliabilityPolicy.RELIABLE if args.reliability == "reliable" else ReliabilityPolicy.BEST_EFFORT
    try:
        node = ControlLogger(
            args.topic,
            args.out,
            args.max_msgs,
            force_anymsg=args.force_anymsg,
            reliability=reliability,
        )
    except SystemExit:
        rclpy.try_shutdown()
        raise
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    rclpy.try_shutdown()


if __name__ == "__main__":
    main()
