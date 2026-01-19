from __future__ import annotations

import math
from typing import Any, Dict

from builtin_interfaces.msg import Time
from geometry_msgs.msg import TransformStamped
from tf2_msgs.msg import TFMessage


def to_ros_time(stamp_sec: float) -> Time:
    base = float(stamp_sec or 0.0)
    sec = int(base)
    nanosec = int((base - sec) * 1e9)
    return Time(sec=sec, nanosec=nanosec)


def _quat_from_rpy(roll_deg: float, pitch_deg: float, yaw_deg: float):
    r = math.radians(roll_deg or 0.0)
    p = math.radians(pitch_deg or 0.0)
    y = math.radians(yaw_deg or 0.0)
    cr, sr = math.cos(r / 2.0), math.sin(r / 2.0)
    cp, sp = math.cos(p / 2.0), math.sin(p / 2.0)
    cy, sy = math.cos(y / 2.0), math.sin(y / 2.0)
    qw = cr * cp * cy + sr * sp * sy
    qx = sr * cp * cy - cr * sp * sy
    qy = cr * sp * cy + sr * cp * sy
    qz = cr * cp * sy - sr * sp * cy
    return qx, qy, qz, qw


def build_tf_static_msgs(calibration: Dict[str, Any]) -> TFMessage:
    """Build TF static message list from calibration frames."""
    transforms = []
    frames = calibration.get("frames", []) if calibration else []
    for frame in frames:
        tr = frame.get("transform", {}) or {}
        parent = frame.get("parent") or "base_link"
        # Harmonize ego naming to base_link for ROS consumption
        parent_frame_id = "base_link" if parent in ["ego", "ego_vehicle"] else parent
        child = frame.get("id") or frame.get("frame_id") or ""
        ts = TransformStamped()
        ts.header.frame_id = parent_frame_id
        ts.child_frame_id = child
        ts.header.stamp = Time(sec=0, nanosec=0)
        ts.transform.translation.x = float(tr.get("x", 0.0) or 0.0)
        ts.transform.translation.y = float(tr.get("y", 0.0) or 0.0)
        ts.transform.translation.z = float(tr.get("z", 0.0) or 0.0)
        qx, qy, qz, qw = _quat_from_rpy(tr.get("roll", 0.0), tr.get("pitch", 0.0), tr.get("yaw", 0.0))
        ts.transform.rotation.x = qx
        ts.transform.rotation.y = qy
        ts.transform.rotation.z = qz
        ts.transform.rotation.w = qw
        transforms.append(ts)
    return TFMessage(transforms=transforms)


# Placeholders for runtime builders; implemented in subsequent commits
def build_image_msg(*args, **kwargs):
    raise NotImplementedError("Image bridge not implemented yet.")


def build_pointcloud2_msg(*args, **kwargs):
    raise NotImplementedError("PointCloud2 bridge not implemented yet.")


def build_imu_msg(*args, **kwargs):
    raise NotImplementedError("IMU bridge not implemented yet.")


def build_navsatfix_msg(*args, **kwargs):
    raise NotImplementedError("NavSatFix bridge not implemented yet.")


def build_event_msg(*args, **kwargs):
    raise NotImplementedError("Event bridge not implemented yet.")
