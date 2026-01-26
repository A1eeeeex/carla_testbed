from __future__ import annotations

import json
import math
import hashlib
from pathlib import Path
from typing import Dict, List, Tuple

import yaml

from carla_testbed.sensors.specs import SensorSpec


def _parse_scalar(val):
    if isinstance(val, (int, float, bool)) or val is None:
        return val
    if isinstance(val, str):
        low = val.lower()
        if low in ["true", "false"]:
            return low == "true"
        try:
            if "." in val:
                return float(val)
            return int(val)
        except Exception:
            return val
    return val


def load_meta_typed(meta_path: Path) -> Dict[str, Dict]:
    meta_raw = json.loads(meta_path.read_text())
    typed = {}
    for sid, info in meta_raw.items():
        info_t = dict(info)
        attrs = info.get("attributes") or {}
        info_t["attributes_raw"] = attrs
        info_t["attributes"] = {k: _parse_scalar(v) for k, v in attrs.items()}
        typed[sid] = info_t
    meta_typed = {"sensors": typed}
    meta_typed_path = meta_path.parent / "meta_typed.json"
    meta_typed_path.write_text(json.dumps(meta_typed, indent=2))
    return meta_typed


def meta_from_rig(rig_final: Dict) -> Dict:
    sensors = {}
    for s in rig_final.get("sensors", []):
        attrs = s.get("attributes") or {}
        sensors[s["id"]] = {
            "type": s.get("type"),
            "blueprint": s.get("blueprint"),
            "sensor_tick": s.get("sensor_tick", 0.05),
            "transform": s.get("transform", {}),
            "attributes_raw": {k: str(v) for k, v in attrs.items()},
            "attributes": {k: _parse_scalar(v) for k, v in attrs.items()},
        }
    meta = {"sensors": sensors}
    return meta


def _rpy_to_matrix(x, y, z, roll, pitch, yaw):
    r = math.radians(roll)
    p = math.radians(pitch)
    yv = math.radians(yaw)
    cr, sr = math.cos(r), math.sin(r)
    cp, sp = math.cos(p), math.sin(p)
    cy, sy = math.cos(yv), math.sin(yv)
    R = [
        [cy * cp, cy * sp * sr - sy * cr, cy * sp * cr + sy * sr],
        [sy * cp, sy * sp * sr + cy * cr, sy * sp * cr - cy * sr],
        [-sp, cp * sr, cp * cr],
    ]
    T = [
        [R[0][0], R[0][1], R[0][2], x],
        [R[1][0], R[1][1], R[1][2], y],
        [R[2][0], R[2][1], R[2][2], z],
        [0, 0, 0, 1],
    ]
    return T


def derive_camera_intrinsics(attrs: Dict[str, float]) -> Dict[str, float]:
    w = float(attrs.get("image_size_x", 0))
    h = float(attrs.get("image_size_y", 0))
    fov = float(attrs.get("fov", 90.0))
    if w <= 0 or h <= 0:
        return {}
    fx = fy = w / (2.0 * math.tan(math.radians(fov) / 2.0))
    cx = w / 2.0
    cy = h / 2.0
    return {"fx": fx, "fy": fy, "cx": cx, "cy": cy, "K": [fx, 0, cx, 0, fy, cy, 0, 0, 1]}


def expand_specs(meta_typed: Dict, rig_final: Dict, rig_name: str = "", ego_id: str = "hero") -> List[Dict]:
    sensors = []
    meta_sensors = meta_typed.get("sensors", {})
    rig_sensors = {s["id"]: s for s in rig_final.get("sensors", [])}
    ros2_prefix = f"/carla/{ego_id}"
    for sid, info in meta_sensors.items():
        rig_info = rig_sensors.get(sid, {})
        enabled = rig_info.get("enabled", True)
        t = info.get("transform", {})
        extrinsic = _rpy_to_matrix(
            t.get("x", 0.0), t.get("y", 0.0), t.get("z", 0.0), t.get("roll", 0.0), t.get("pitch", 0.0), t.get("yaw", 0.0)
        )
        sensor_tick = float(info.get("sensor_tick", rig_info.get("sensor_tick", 0.05) or 0.0))
        expected_rate = (1.0 / sensor_tick) if sensor_tick > 0 else None
        attributes = info.get("attributes") or {}
        attributes_raw = info.get("attributes_raw") or {}
        sensor_type = info.get("type", rig_info.get("type", "custom"))
        entry = {
            "id": sid,
            "type": sensor_type,
            "blueprint": info.get("blueprint", rig_info.get("blueprint")),
            "enabled": enabled,
            "parent_frame": "ego",
            "frame_id": sid,
            "transform": t,
            "extrinsic_matrix": extrinsic,
            "sensor_tick": sensor_tick,
            "expected_rate_hz": expected_rate,
            "time_source": "sim_time",
            "attributes": attributes,
            "attributes_raw": attributes_raw,
            "noise_model": {"enabled": False, "covariance": None},
            "data_format": {},
            "io": {"ros2": {}, "cyber": {}},
            "tf_static": True,
            "rig": rig_name,
        }
        sensor_path = sid
        if sensor_type == "camera":
            intr = derive_camera_intrinsics(attributes)
            entry["camera"] = {"intrinsics": intr, "distortion": {"model": "none", "coeffs": []}}
            entry["data_format"] = {"encoding": "rgb8", "compression": None}
            entry["io"]["ros2"] = {"topic": f"{ros2_prefix}/{sensor_path}/image", "msg_type": "sensor_msgs/msg/Image", "qos": "SENSOR_DATA"}
            entry["io"]["cyber"] = {"channel": f"/{sid}", "proto": "apollo.drivers.Image", "mapping_required": True}
        elif sensor_type == "lidar":
            entry["data_format"] = {"encoding": "pointcloud_xyzirt", "compression": None}
            entry["io"]["ros2"] = {"topic": f"{ros2_prefix}/{sensor_path}/points", "msg_type": "sensor_msgs/msg/PointCloud2", "qos": "SENSOR_DATA"}
            entry["io"]["cyber"] = {"channel": f"/{sid}", "proto": "apollo.drivers.PointCloud", "mapping_required": True}
        elif sensor_type == "radar":
            entry["data_format"] = {"encoding": "radar_returns", "compression": None}
            entry["io"]["ros2"] = {"topic": f"{ros2_prefix}/{sensor_path}/radar", "msg_type": "sensor_msgs/msg/PointCloud2", "qos": "SENSOR_DATA"}
            entry["io"]["cyber"] = {"channel": f"/{sid}", "proto": "apollo.drivers.ContiRadar", "mapping_required": True}
        elif sensor_type == "imu":
            entry["data_format"] = {"encoding": "imu", "compression": None}
            entry["io"]["ros2"] = {"topic": f"{ros2_prefix}/{sensor_path}", "msg_type": "sensor_msgs/msg/Imu", "qos": "SENSOR_DATA"}
            entry["io"]["cyber"] = {"channel": f"/{sid}", "proto": "apollo.drivers.Imu", "mapping_required": True}
        elif sensor_type == "gnss":
            entry["data_format"] = {"encoding": "navsatfix", "compression": None}
            entry["io"]["ros2"] = {"topic": f"{ros2_prefix}/{sensor_path}", "msg_type": "sensor_msgs/msg/NavSatFix", "qos": "SENSOR_DATA"}
            entry["io"]["cyber"] = {"channel": f"/{sid}", "proto": "apollo.drivers.GnssBestPose", "mapping_required": True}
        sensors.append(entry)
    return sensors


def save_json(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2))


def derive_calibration(expanded: List[Dict]) -> Dict:
    frames = []
    for s in expanded:
        frames.append(
            {
                "id": s["id"],
                "parent": s.get("parent_frame", "ego"),
                "transform": s.get("transform", {}),
                "matrix": s.get("extrinsic_matrix", []),
                "type": s.get("type"),
            }
        )
    calib = {"frames": frames, "conventions": {"time_base": "sim_time", "coord": "carla_world"}}
    return calib


def derive_time_sync(expanded: List[Dict]) -> Dict:
    expected = {s["id"]: s.get("expected_rate_hz") for s in expanded}
    return {
        "time_source": "carla_snapshot.elapsed_seconds",
        "frame_id_source": "carla_snapshot.frame",
        "sync_policy": "soft_sync",
        "tolerance_sec": 0.05,
        "expected_rate_hz": expected,
    }


def derive_noise(expanded: List[Dict]) -> Dict:
    out = {}
    for s in expanded:
        out[s["id"]] = s.get("noise_model", {"enabled": False, "covariance": None})
    return out


def derive_data_format(expanded: List[Dict]) -> Dict:
    out = {}
    for s in expanded:
        out[s["id"]] = s.get("data_format", {})
    return out


def derive_io(expanded: List[Dict], kind: str) -> Dict:
    key = "ros2" if kind == "ros2" else "cyber"
    out = {}
    for s in expanded:
        out[s["id"]] = s.get("io", {}).get(key, {})
    return out


def hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()
