from __future__ import annotations

import argparse
import base64
import json
import re
from pathlib import Path
from typing import Any, Dict, Optional

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy, DurabilityPolicy
from rosidl_runtime_py import message_to_ordereddict
from rosidl_runtime_py.utilities import get_message

try:
    from rclpy.any_msg import AnyMsg
except Exception:  # pragma: no cover - optional by distro
    AnyMsg = None

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
try:
    from geometry_msgs.msg import Twist
except ImportError:  # pragma: no cover - optional
    Twist = None


def _node_name_for_topic(topic: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_]+", "_", (topic or "topic").strip("/"))
    safe = safe.strip("_") or "topic"
    # ROS node names can be long, but keeping them short makes Autoware's
    # duplicated-node diagnostics readable.
    return f"control_logger_{safe}"[:120]


def _pose_summary(pose: Any) -> Dict[str, Any]:
    position = getattr(pose, "position", None)
    orientation = getattr(pose, "orientation", None)
    return {
        "x": getattr(position, "x", None),
        "y": getattr(position, "y", None),
        "z": getattr(position, "z", None),
        "qx": getattr(orientation, "x", None),
        "qy": getattr(orientation, "y", None),
        "qz": getattr(orientation, "z", None),
        "qw": getattr(orientation, "w", None),
    }


def _vector3_summary(vec: Any) -> Dict[str, Any]:
    return {
        "x": getattr(vec, "x", None),
        "y": getattr(vec, "y", None),
        "z": getattr(vec, "z", None),
    }


def _twist_summary(twist: Any) -> Dict[str, Any]:
    return {
        "linear": _vector3_summary(getattr(twist, "linear", None)),
        "angular": _vector3_summary(getattr(twist, "angular", None)),
    }


def _classification_summary(classifications: Any) -> list[Dict[str, Any]]:
    out = []
    for cls in list(classifications or []):
        out.append(
            {
                "label": getattr(cls, "label", None),
                "probability": getattr(cls, "probability", None),
            }
        )
    return out


def _shape_summary(shape: Any) -> Dict[str, Any]:
    return {
        "type": getattr(shape, "type", None),
        "dimensions": _vector3_summary(getattr(shape, "dimensions", None)),
    }


def _predicted_object_summary(obj: Any, index: int | None = None) -> Dict[str, Any]:
    kinematics = getattr(obj, "kinematics", None)
    pose_with_cov = getattr(kinematics, "initial_pose_with_covariance", None)
    twist_with_cov = getattr(kinematics, "initial_twist_with_covariance", None)
    predicted_paths = list(getattr(kinematics, "predicted_paths", []) or [])
    return {
        "index": index,
        "existence_probability": getattr(obj, "existence_probability", None),
        "classification": _classification_summary(getattr(obj, "classification", [])),
        "pose": _pose_summary(getattr(pose_with_cov, "pose", None)),
        "twist": _twist_summary(getattr(twist_with_cov, "twist", None)),
        "shape": _shape_summary(getattr(obj, "shape", None)),
        "predicted_path_count": len(predicted_paths),
    }


def _predicted_objects_summary(objects: Any) -> Dict[str, Any]:
    objs = list(objects or [])
    sample_limit = 8
    return {
        "object_count": len(objs),
        "objects_sample": [
            _predicted_object_summary(obj, index=idx)
            for idx, obj in enumerate(objs[:sample_limit])
        ],
    }


def _trajectory_point_payload(point: Any) -> Any:
    # Autoware has both TrajectoryPoint and PathPointWithLaneId. The latter
    # wraps the actual path point under `.point`, while lane ids stay on the
    # wrapper. Unwrap only for kinematic fields.
    return getattr(point, "point", point)


def _trajectory_point_summary(point: Any, index: int | None = None) -> Dict[str, Any]:
    payload = _trajectory_point_payload(point)
    out = {
        "index": index,
        "pose": _pose_summary(getattr(payload, "pose", None)),
        "longitudinal_velocity_mps": getattr(payload, "longitudinal_velocity_mps", None),
        "lateral_velocity_mps": getattr(payload, "lateral_velocity_mps", None),
        "acceleration_mps2": getattr(payload, "acceleration_mps2", None),
        "heading_rate_rps": getattr(payload, "heading_rate_rps", None),
        "front_wheel_angle_rad": getattr(payload, "front_wheel_angle_rad", None),
        "rear_wheel_angle_rad": getattr(payload, "rear_wheel_angle_rad", None),
    }
    if hasattr(point, "lane_ids"):
        out["lane_ids"] = list(getattr(point, "lane_ids", []) or [])
    return out


def _numeric_stats(values: list[Any]) -> Dict[str, Any]:
    nums = [float(v) for v in values if v is not None]
    if not nums:
        return {"min": None, "max": None, "mean": None, "first": None, "last": None}
    return {
        "min": min(nums),
        "max": max(nums),
        "mean": sum(nums) / len(nums),
        "first": nums[0],
        "last": nums[-1],
    }


def _trajectory_summary(points: Any) -> Dict[str, Any]:
    pts = list(points or [])
    payloads = [_trajectory_point_payload(p) for p in pts]
    velocities = [getattr(p, "longitudinal_velocity_mps", None) for p in payloads]
    accelerations = [getattr(p, "acceleration_mps2", None) for p in payloads]
    first_nonzero_velocity_index = None
    for idx, velocity in enumerate(velocities):
        if velocity is not None and abs(float(velocity)) > 1e-3:
            first_nonzero_velocity_index = idx
            break
    # Autoware longitudinal issues often hide in the first few meters of the
    # trajectory. Keep near-field points dense, then thin the tail to avoid
    # turning every topic capture into a full trajectory dump.
    dense_near_field_count = 80
    sample_indices = set(range(min(len(pts), dense_near_field_count)))
    sample_indices.update(range(dense_near_field_count, len(pts), 20))
    sample_indices.update(range(max(0, len(pts) - 5), len(pts)))
    sample_indices = {idx for idx in sample_indices if 0 <= idx < len(pts)}
    stop_indices = [
        idx
        for idx, velocity in enumerate(velocities)
        if velocity is not None and abs(float(velocity)) <= 1e-3
    ][:10]
    return {
        "trajectory_point_count": len(pts),
        "trajectory_velocity_mps": _numeric_stats(velocities),
        "trajectory_acceleration_mps2": _numeric_stats(accelerations),
        "first_nonzero_velocity_index": first_nonzero_velocity_index,
        "first_stop_velocity_indices": stop_indices,
        # Keep the logger lightweight while preserving enough evidence to debug
        # stop-only trajectories without dumping every point every frame.
        "trajectory_points_sample": [
            _trajectory_point_summary(pts[idx], index=idx) for idx in sorted(sample_indices)
        ],
    }


def _planning_control_point_summary(point: Any, index: int | None = None) -> Dict[str, Any]:
    return {
        "index": index,
        "pose": _pose_summary(getattr(point, "pose", None)),
        "velocity": getattr(point, "velocity", None),
        "shift_length": getattr(point, "shift_length", None),
        "distance": getattr(point, "distance", None),
    }


def _planning_factor_summary(factor: Any, index: int | None = None) -> Dict[str, Any]:
    control_points = list(getattr(factor, "control_points", []) or [])
    safety_factors = getattr(factor, "safety_factors", None)
    safety_items = list(getattr(safety_factors, "factors", []) or [])
    return {
        "index": index,
        "module": getattr(factor, "module", None),
        "behavior": getattr(factor, "behavior", None),
        "detail": getattr(factor, "detail", None),
        "is_driving_forward": getattr(factor, "is_driving_forward", None),
        "control_point_count": len(control_points),
        "control_points_sample": [
            _planning_control_point_summary(point, index=idx)
            for idx, point in enumerate(control_points[:8])
        ],
        "safety_factor_count": len(safety_items),
        "safety_is_safe": getattr(safety_factors, "is_safe", None),
        "safety_detail": getattr(safety_factors, "detail", None),
    }


def _planning_factors_summary(factors: Any) -> Dict[str, Any]:
    items = list(factors or [])
    return {
        "planning_factor_count": len(items),
        "planning_factors_sample": [
            _planning_factor_summary(factor, index=idx)
            for idx, factor in enumerate(items[:8])
        ],
    }


def _diagnostic_status_summary(status_list: Any) -> list[Dict[str, Any]]:
    statuses = []
    for status in list(status_list or []):
        values = []
        for value in list(getattr(status, "values", []) or []):
            values.append(
                {
                    "key": str(getattr(value, "key", "")),
                    "value": str(getattr(value, "value", "")),
                }
            )
        raw_level = getattr(status, "level", 0)
        if isinstance(raw_level, (bytes, bytearray)):
            level = int(raw_level[0]) if raw_level else 0
        elif isinstance(raw_level, str) and len(raw_level) == 1:
            level = ord(raw_level)
        else:
            level = int(raw_level or 0)
        statuses.append(
            {
                "name": str(getattr(status, "name", "")),
                "level": level,
                "message": str(getattr(status, "message", "")),
                "hardware_id": str(getattr(status, "hardware_id", "")),
                "values": values,
            }
        )
    return statuses


def _has_non_header_payload(data: Dict[str, Any]) -> bool:
    header_only_keys = {"stamp", "frame_id", "control_time"}
    return any(key not in header_only_keys for key in data)


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
        elif hasattr(msg, "stamp"):
            stamp = getattr(msg, "stamp")
            out["stamp"] = {
                "sec": getattr(stamp, "sec", None),
                "nanosec": getattr(stamp, "nanosec", None),
            }
        if hasattr(msg, "control_time"):
            control_time = getattr(msg, "control_time")
            out["control_time"] = {
                "sec": getattr(control_time, "sec", None),
                "nanosec": getattr(control_time, "nanosec", None),
            }
        for scalar_field in (
            "steering_tire_angle",
            "mode",
            "report",
            "state",
            "behavior",
            "is_autoware_control_enabled",
            "is_in_transition",
            "is_stop_mode_available",
            "is_autonomous_mode_available",
            "is_local_mode_available",
            "is_remote_mode_available",
            "max_velocity",
            "use_constraints",
            "sender",
        ):
            if hasattr(msg, scalar_field):
                out[scalar_field] = getattr(msg, scalar_field, None)
        if hasattr(msg, "constraints"):
            constraints = getattr(msg, "constraints", None)
            out["constraints"] = {
                "max_acceleration": getattr(constraints, "max_acceleration", None),
                "min_acceleration": getattr(constraints, "min_acceleration", None),
                "max_jerk": getattr(constraints, "max_jerk", None),
                "min_jerk": getattr(constraints, "min_jerk", None),
            }
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
                "velocity": getattr(lon, "velocity", None),
                "speed": getattr(lon, "speed", None),
                "acceleration": getattr(lon, "acceleration", None),
                "jerk": getattr(lon, "jerk", None),
            }
        if hasattr(msg, "data"):
            try:
                out["data"] = list(getattr(msg, "data", []) or [])
            except TypeError:
                out["data"] = getattr(msg, "data", None)
        if hasattr(msg, "points"):
            out.update(_trajectory_summary(getattr(msg, "points", [])))
        if hasattr(msg, "factors"):
            out.update(_planning_factors_summary(getattr(msg, "factors", [])))
        if hasattr(msg, "objects"):
            out.update(_predicted_objects_summary(getattr(msg, "objects", [])))
        if hasattr(msg, "traffic_light_groups"):
            out["traffic_light_group_count"] = len(
                list(getattr(msg, "traffic_light_groups", []) or [])
            )
        if hasattr(msg, "pose"):
            pose_field = getattr(msg, "pose", None)
            out["pose"] = _pose_summary(getattr(pose_field, "pose", pose_field))
        if hasattr(msg, "twist"):
            twist_field = getattr(msg, "twist", None)
            out["twist"] = _twist_summary(getattr(twist_field, "twist", twist_field))
        if hasattr(msg, "longitudinal_velocity"):
            out["velocity_report"] = {
                "longitudinal_velocity": getattr(msg, "longitudinal_velocity", None),
                "lateral_velocity": getattr(msg, "lateral_velocity", None),
                "heading_rate": getattr(msg, "heading_rate", None),
            }
        if hasattr(msg, "accel"):
            accel_field = getattr(msg, "accel", None)
            accel_inner = getattr(accel_field, "accel", accel_field)
            out["accel"] = {
                "linear": _vector3_summary(getattr(accel_inner, "linear", None)),
                "angular": _vector3_summary(getattr(accel_inner, "angular", None)),
            }
        if hasattr(msg, "status"):
            out["status"] = _diagnostic_status_summary(getattr(msg, "status", []))
        if out and _has_non_header_payload(out):
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
        explicit_topic_type: Optional[str] = None,
        force_anymsg: bool = False,
        reliability: ReliabilityPolicy = ReliabilityPolicy.BEST_EFFORT,
    ):
        super().__init__(_node_name_for_topic(topic))
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
        self.topic_type = explicit_topic_type.strip() if explicit_topic_type else None
        self.subscription = None
        self._retry_timer = None
        self._force_anymsg = force_anymsg
        self._qos = qos
        self._reliability = reliability
        if not self._ensure_subscription(initial=True):
            self._retry_timer = self.create_timer(0.5, self._retry_subscribe)

    def _ensure_subscription(self, *, initial: bool) -> bool:
        if self.subscription is not None:
            return True
        discovered_type = self._resolve_topic_type(self.topic)
        if discovered_type:
            self.topic_type = discovered_type
        try:
            if self.topic_type:
                dynamic_type = self.topic_type.split(",")[0].strip()
                msg_cls = get_message(dynamic_type)
                self.subscription = self.create_subscription(msg_cls, self.topic, self._cb, self._qos)
                self.get_logger().info(
                    f"Logging {self.topic} as {dynamic_type} -> {self.out} "
                    f"(QoS reliability={self._reliability.name})"
                )
                return True
            if self._force_anymsg and AnyMsg is not None:
                self.subscription = self.create_subscription(AnyMsg, self.topic, self._cb_any, self._qos)
                self.get_logger().warn(
                    f"force AnyMsg; subscribing AnyMsg on {self.topic} "
                    f"(QoS reliability={self._reliability.name}, type={self.topic_type})"
                )
                return True
            if initial and AnyMsg is not None:
                self.subscription = self.create_subscription(AnyMsg, self.topic, self._cb_any, self._qos)
                self.get_logger().warn(
                    f"fallback AnyMsg; subscribing AnyMsg on {self.topic} "
                    f"(QoS reliability={self._reliability.name}, type={self.topic_type})"
                )
                return True
        except Exception as exc:  # pragma: no cover - defensive
            self.get_logger().error(f"Failed to create subscription on {self.topic}: {exc}")
            return False
        if initial:
            self.get_logger().warn(
                f"Topic type for {self.topic} not discovered yet; waiting before creating subscription "
                f"(QoS reliability={self._reliability.name})"
            )
        return False

    def _retry_subscribe(self) -> None:
        if self._ensure_subscription(initial=False) and self._retry_timer is not None:
            self._retry_timer.cancel()
            self._retry_timer = None

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
    ap.add_argument("--topic-type", default="", help="explicit ROS2 message type, e.g. std_msgs/msg/Float32MultiArray")
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
            explicit_topic_type=args.topic_type or None,
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
