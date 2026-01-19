from __future__ import annotations

import json
import math
from typing import Any, Dict, Iterable, List, Optional

import numpy as np
from builtin_interfaces.msg import Time
from geometry_msgs.msg import TransformStamped
from sensor_msgs.msg import Image, Imu, NavSatFix, NavSatStatus, PointCloud2, PointField
from std_msgs.msg import String
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


def build_image_msg(sample, stamp: Time, frame_id: str) -> Image:
    payload = sample.payload or {}
    bgra = payload.get("bgra")
    if bgra is None:
        raise ValueError("Camera sample missing BGRA payload")
    rgb = np.ascontiguousarray(bgra[:, :, :3][:, :, ::-1])  # BGRA -> RGB, contiguous for ROS
    height, width = rgb.shape[0], rgb.shape[1]
    msg = Image()
    msg.header.stamp = stamp
    msg.header.frame_id = frame_id
    msg.height = int(payload.get("height", height))
    msg.width = int(payload.get("width", width))
    msg.encoding = "rgb8"
    msg.step = int(msg.width * 3)
    msg.data = rgb.tobytes()
    return msg


def build_imu_msg(sample, stamp: Time, frame_id: str, covariances: Optional[Dict[str, Iterable[float]]] = None) -> Imu:
    payload = sample.payload or {}
    cov = covariances or {}
    imu_msg = Imu()
    imu_msg.header.stamp = stamp
    imu_msg.header.frame_id = frame_id

    ang = payload.get("gyroscope") or {}
    lin = payload.get("accelerometer") or {}
    imu_msg.angular_velocity.x = float(ang.get("x", 0.0) or 0.0)
    imu_msg.angular_velocity.y = float(ang.get("y", 0.0) or 0.0)
    imu_msg.angular_velocity.z = float(ang.get("z", 0.0) or 0.0)

    imu_msg.linear_acceleration.x = float(lin.get("x", 0.0) or 0.0)
    imu_msg.linear_acceleration.y = float(lin.get("y", 0.0) or 0.0)
    imu_msg.linear_acceleration.z = float(lin.get("z", 0.0) or 0.0)

    imu_msg.orientation.w = 1.0
    imu_msg.orientation.x = 0.0
    imu_msg.orientation.y = 0.0
    imu_msg.orientation.z = 0.0
    imu_msg.orientation_covariance[0] = -1.0  # mark orientation invalid

    ang_cov: List[float] = list(cov.get("angular_velocity", []))
    if len(ang_cov) == 9:
        imu_msg.angular_velocity_covariance = ang_cov
    else:
        imu_msg.angular_velocity_covariance[0] = 1e-6
        imu_msg.angular_velocity_covariance[4] = 1e-6
        imu_msg.angular_velocity_covariance[8] = 1e-6

    lin_cov: List[float] = list(cov.get("linear_acceleration", []))
    if len(lin_cov) == 9:
        imu_msg.linear_acceleration_covariance = lin_cov
    else:
        imu_msg.linear_acceleration_covariance[0] = 1e-6
        imu_msg.linear_acceleration_covariance[4] = 1e-6
        imu_msg.linear_acceleration_covariance[8] = 1e-6
    return imu_msg


def build_navsatfix_msg(sample, stamp: Time, frame_id: str, covariance: Optional[Iterable[float]] = None) -> NavSatFix:
    payload = sample.payload or {}
    msg = NavSatFix()
    msg.header.stamp = stamp
    msg.header.frame_id = frame_id
    msg.latitude = float(payload.get("latitude", 0.0) or 0.0)
    msg.longitude = float(payload.get("longitude", 0.0) or 0.0)
    msg.altitude = float(payload.get("altitude", 0.0) or 0.0)
    msg.status.status = NavSatStatus.STATUS_FIX
    msg.status.service = NavSatStatus.SERVICE_GPS

    cov_list = list(covariance) if covariance is not None else []
    if len(cov_list) == 9:
        msg.position_covariance = cov_list
        msg.position_covariance_type = NavSatFix.COVARIANCE_TYPE_DIAGONAL_KNOWN
    else:
        msg.position_covariance = [0.0] * 9
        msg.position_covariance_type = NavSatFix.COVARIANCE_TYPE_UNKNOWN
    return msg


# Placeholders to be filled when corresponding publishers are implemented
def build_pointcloud2_msg(sample, stamp: Time, frame_id: str, fields: Optional[List[PointField]] = None) -> PointCloud2:
    payload = sample.payload or {}
    pts = payload.get("points")
    if pts is None:
        raise ValueError("Lidar sample missing points payload")
    arr = np.asarray(pts, dtype=np.float32)
    if arr.ndim != 2 or arr.shape[1] < 4:
        raise ValueError(f"Unexpected point shape {arr.shape}")
    arr = np.ascontiguousarray(arr[:, :4], dtype=np.float32)
    msg = PointCloud2()
    msg.header.stamp = stamp
    msg.header.frame_id = frame_id
    msg.height = 1
    msg.width = arr.shape[0]
    msg.is_bigendian = False
    msg.is_dense = False
    msg.point_step = 16
    msg.row_step = msg.point_step * msg.width
    msg.fields = fields or [
        PointField(name="x", offset=0, datatype=PointField.FLOAT32, count=1),
        PointField(name="y", offset=4, datatype=PointField.FLOAT32, count=1),
        PointField(name="z", offset=8, datatype=PointField.FLOAT32, count=1),
        PointField(name="intensity", offset=12, datatype=PointField.FLOAT32, count=1),
    ]
    msg.data = arr.tobytes()
    return msg


def build_event_msg(event, stamp: Time, frame_id: str) -> String:
    body = {
        "event_type": getattr(event, "event_type", ""),
        "frame": getattr(event, "frame_id", None),
        "timestamp": getattr(event, "timestamp", None),
        "frame_id": frame_id,
        "meta": getattr(event, "meta", {}) or {},
    }
    return String(data=json.dumps(body))
