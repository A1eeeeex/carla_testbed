#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import importlib
import importlib.util
import json
import math
import os
import re
import signal
import sys
import threading
import time
import types
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import yaml
from google.protobuf import empty_pb2

try:
    import rclpy
    from nav_msgs.msg import Odometry
    from rclpy.executors import MultiThreadedExecutor
    from rclpy.node import Node
    from rclpy.qos import QoSHistoryPolicy, QoSProfile, QoSReliabilityPolicy
    from rclpy.qos import qos_profile_sensor_data
    from std_msgs.msg import String
except Exception as exc:  # pragma: no cover
    raise RuntimeError(f"ROS2 imports failed, source ROS2 first. err={exc}") from exc

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

try:
    from vision_msgs.msg import Detection3DArray
except Exception:
    Detection3DArray = None

try:
    from visualization_msgs.msg import MarkerArray
except Exception:
    MarkerArray = None

try:
    import carla
except Exception:
    carla = None


def _clamp(val: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, val))


def _quat_to_yaw(qx: float, qy: float, qz: float, qw: float) -> float:
    siny_cosp = 2.0 * (qw * qz + qx * qy)
    cosy_cosp = 1.0 - 2.0 * (qy * qy + qz * qz)
    return math.atan2(siny_cosp, cosy_cosp)


def _wrap_to_pi(angle: float) -> float:
    while angle > math.pi:
        angle -= 2.0 * math.pi
    while angle < -math.pi:
        angle += 2.0 * math.pi
    return angle


def _rad_to_deg(angle: float) -> float:
    return math.degrees(_wrap_to_pi(angle))


def _wrap_deg(angle_deg: float) -> float:
    return ((angle_deg + 180.0) % 360.0) - 180.0


def _quantize_right_angle_deg(angle_deg: float) -> float:
    candidates = (-180.0, -90.0, 0.0, 90.0, 180.0)
    best = min(candidates, key=lambda cand: abs((((angle_deg - cand) + 180.0) % 360.0) - 180.0))
    return best


def _yaw_to_quat(yaw: float) -> Tuple[float, float, float, float]:
    half = 0.5 * yaw
    return 0.0, 0.0, math.sin(half), math.cos(half)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _sign(value: float, eps: float = 1e-6) -> int:
    if value > eps:
        return 1
    if value < -eps:
        return -1
    return 0


def _normalize_ns(ns: str) -> str:
    ns = (ns or "/carla").strip()
    if not ns.startswith("/"):
        ns = "/" + ns
    ns = ns.rstrip("/")
    return ns or "/carla"


def _stamp_to_sec(stamp: Any) -> float:
    if stamp is None:
        return time.time()
    return float(getattr(stamp, "sec", 0)) + float(getattr(stamp, "nanosec", 0)) * 1e-9


def _safe_set(obj: Any, attr: str, value: Any) -> None:
    if hasattr(obj, attr):
        setattr(obj, attr, value)


def _ensure_paths(apollo_root: Path, pb_root: Path) -> None:
    candidates = [
        apollo_root,
        apollo_root / "cyber" / "python",
        pb_root,
    ]
    for path in candidates:
        text = str(path.resolve())
        if text not in sys.path:
            sys.path.insert(0, text)


def _import_cyber(apollo_root: Path):
    _ensure_paths(apollo_root, Path("."))
    try:
        from cyber.python.cyber_py3 import cyber  # type: ignore
        from cyber.python.cyber_py3 import cyber_time  # type: ignore

        return cyber, cyber_time
    except Exception:
        pass
    try:
        from cyber_py3 import cyber  # type: ignore
        from cyber_py3 import cyber_time  # type: ignore

        return cyber, cyber_time
    except Exception as exc:
        raise RuntimeError(
            "failed to import cyber_py3. ensure APOLLO_ROOT and Apollo cyber setup are ready"
        ) from exc


def _import_apollo_pb(pb_root: Path):
    _ensure_paths(Path(os.environ.get("APOLLO_ROOT", ".")), pb_root)
    action_command_pb2 = None
    command_status_pb2 = None
    try:
        from modules.common_msgs.chassis_msgs import chassis_pb2  # type: ignore
        from modules.common_msgs.control_msgs import control_cmd_pb2  # type: ignore
        from modules.common_msgs.external_command_msgs import lane_follow_command_pb2  # type: ignore
        from modules.common_msgs.localization_msgs import localization_pb2  # type: ignore
        from modules.common_msgs.perception_msgs import perception_obstacle_pb2  # type: ignore
        from modules.common_msgs.routing_msgs import routing_pb2  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            f"failed to import Apollo pb2 modules from {pb_root}. run gen_pb2.sh first"
        ) from exc
    try:
        from modules.common_msgs.external_command_msgs import action_command_pb2  # type: ignore
        from modules.common_msgs.external_command_msgs import command_status_pb2  # type: ignore
    except Exception:
        # Some environments don't have these pb2 generated locally.
        pass
    return (
        localization_pb2,
        chassis_pb2,
        perception_obstacle_pb2,
        control_cmd_pb2,
        routing_pb2,
        action_command_pb2,
        command_status_pb2,
        lane_follow_command_pb2,
    )


def _candidate_apollo_python_roots(apollo_root: Path) -> List[Path]:
    roots: List[Path] = []
    seen: set[str] = set()

    def add(path: Path) -> None:
        try:
            key = str(path.resolve())
        except Exception:
            key = str(path)
        if key in seen:
            return
        seen.add(key)
        roots.append(path)

    add(apollo_root / ".aem" / "envroot" / "opt" / "apollo" / "neo" / "python")
    for base in [apollo_root, apollo_root.parent, *apollo_root.parents]:
        add(base / "python")
        add(base / ".aem" / "envroot" / "opt" / "apollo" / "neo" / "python")
        add(base / "opt" / "apollo" / "neo" / "python")
        if base.name == "src":
            add(base.parent / "python")
    add(Path("/opt/apollo/neo/python"))
    return roots


def _import_optional_pb_module(apollo_root: Path, pb_root: Path, module_name: str):
    _ensure_paths(apollo_root, pb_root)
    extra_roots = _candidate_apollo_python_roots(apollo_root)
    pkg_parts = module_name.split(".")[:-1]
    for path in extra_roots:
        try:
            text = str(path.resolve())
        except Exception:
            continue
        if path.exists() and text not in sys.path:
            sys.path.insert(0, text)
        if not path.exists():
            continue
        pkg_path = path
        for idx, part in enumerate(pkg_parts):
            pkg_path = pkg_path / part
            pkg_name = ".".join(pkg_parts[: idx + 1])
            pkg_mod = sys.modules.get(pkg_name)
            if pkg_mod is not None and hasattr(pkg_mod, "__path__"):
                candidate = str(pkg_path)
                if candidate not in list(getattr(pkg_mod, "__path__", [])):
                    pkg_mod.__path__.append(candidate)
    try:
        return importlib.import_module(module_name)
    except Exception:
        pass
    rel_parts = module_name.split(".")
    rel_file = Path(*rel_parts).with_suffix(".py")
    for root in extra_roots:
        mod_file = root / rel_file
        if not mod_file.exists():
            continue
        for idx, part in enumerate(pkg_parts):
            pkg_name = ".".join(pkg_parts[: idx + 1])
            pkg_path = root / Path(*pkg_parts[: idx + 1])
            pkg_mod = sys.modules.get(pkg_name)
            if pkg_mod is None:
                pkg_mod = types.ModuleType(pkg_name)
                pkg_mod.__path__ = [str(pkg_path)]
                sys.modules[pkg_name] = pkg_mod
            elif hasattr(pkg_mod, "__path__"):
                candidate = str(pkg_path)
                if candidate not in list(pkg_mod.__path__):
                    pkg_mod.__path__.append(candidate)
        try:
            spec = importlib.util.spec_from_file_location(module_name, mod_file)
            if spec is None or spec.loader is None:
                continue
            mod = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = mod
            spec.loader.exec_module(mod)
            return mod
        except Exception:
            sys.modules.pop(module_name, None)
            continue
    return None


def _enum_attr(obj: Any, names: Sequence[str], default: Any = None) -> Any:
    for name in names:
        if hasattr(obj, name):
            return getattr(obj, name)
    return default


@dataclass
class Transform2D:
    tx: float = 0.0
    ty: float = 0.0
    tz: float = 0.0
    yaw_deg: float = 0.0

    @property
    def yaw_rad(self) -> float:
        return math.radians(self.yaw_deg)

    def apply_position(self, x: float, y: float, z: float) -> Tuple[float, float, float]:
        c = math.cos(self.yaw_rad)
        s = math.sin(self.yaw_rad)
        xx = c * x - s * y + self.tx
        yy = s * x + c * y + self.ty
        zz = z + self.tz
        return xx, yy, zz

    def apply_vector(self, x: float, y: float, z: float) -> Tuple[float, float, float]:
        c = math.cos(self.yaw_rad)
        s = math.sin(self.yaw_rad)
        xx = c * x - s * y
        yy = s * x + c * y
        return xx, yy, z

    def apply_yaw(self, yaw: float) -> float:
        return yaw + self.yaw_rad


def _extract_xy_series(map_file: Path) -> List[Tuple[float, float]]:
    text = map_file.read_text(errors="ignore")
    pairs: List[Tuple[float, float]] = []
    pattern = re.compile(
        r"x:\s*(-?\d+(?:\.\d+)?)\s+"
        r"(?:z:\s*-?\d+(?:\.\d+)?\s+)?"
        r"y:\s*(-?\d+(?:\.\d+)?)",
        re.MULTILINE,
    )
    for match in pattern.finditer(text):
        pairs.append((float(match.group(1)), float(match.group(2))))
    if pairs:
        return pairs
    x_vals = [float(m.group(1)) for m in re.finditer(r"\bx:\s*(-?\d+(?:\.\d+)?)", text)]
    y_vals = [float(m.group(1)) for m in re.finditer(r"\by:\s*(-?\d+(?:\.\d+)?)", text)]
    return list(zip(x_vals, y_vals))


def _build_map_segments(points: Sequence[Tuple[float, float]], *, max_gap_m: float = 25.0) -> List[Tuple[float, float, float, float]]:
    segs: List[Tuple[float, float, float, float]] = []
    if len(points) < 2:
        return segs
    for a, b in zip(points[:-1], points[1:]):
        dx = b[0] - a[0]
        dy = b[1] - a[1]
        dist = math.hypot(dx, dy)
        if dist < 0.05 or dist > max_gap_m:
            continue
        segs.append((a[0], a[1], b[0], b[1]))
    return segs


def _nearest_segment_metrics(
    x: float,
    y: float,
    segments: Sequence[Tuple[float, float, float, float]],
) -> Optional[Dict[str, float]]:
    best: Optional[Dict[str, float]] = None
    best_dist = float("inf")
    for x1, y1, x2, y2 in segments:
        dx = x2 - x1
        dy = y2 - y1
        denom = dx * dx + dy * dy
        if denom <= 1e-9:
            continue
        t = _clamp(((x - x1) * dx + (y - y1) * dy) / denom, 0.0, 1.0)
        px = x1 + t * dx
        py = y1 + t * dy
        ex = x - px
        ey = y - py
        dist = math.hypot(ex, ey)
        if dist >= best_dist:
            continue
        seg_yaw = math.atan2(dy, dx)
        signed = math.sin(seg_yaw) * (x - px) - math.cos(seg_yaw) * (y - py)
        best_dist = dist
        best = {
            "dist": dist,
            "proj_x": px,
            "proj_y": py,
            "seg_yaw": seg_yaw,
            "signed_e_y": signed,
            "curvature": 0.0,
        }
    return best


class CarlaFeedbackClient:
    def __init__(self, host: str, port: int, ego_role_name: str, timeout_sec: float = 1.5) -> None:
        self.host = host
        self.port = int(port)
        self.ego_role_name = ego_role_name
        self.timeout_sec = float(timeout_sec)
        self.client = None
        self.world = None
        self.actor = None
        self.last_error = ""
        self._vehicle_characteristics: Optional[Dict[str, Any]] = None
        self._vehicle_characteristics_actor_id: Optional[int] = None

    def _connect(self) -> bool:
        if carla is None:
            self.last_error = "carla_py_unavailable"
            return False
        try:
            self.client = carla.Client(self.host, self.port)
            self.client.set_timeout(self.timeout_sec)
            self.world = self.client.get_world()
            self.last_error = ""
            return True
        except Exception as exc:
            self.last_error = f"connect_failed:{exc}"
            self.client = None
            self.world = None
            self.actor = None
            return False

    def _discover_actor(self):
        if self.client is None and not self._connect():
            return None
        try:
            self.world = self.client.get_world()
            actors = list(self.world.get_actors().filter("vehicle.*"))
            for actor in actors:
                role = (actor.attributes or {}).get("role_name", "")
                if role == self.ego_role_name or role in ("hero", "ego", "tb_ego"):
                    self.actor = actor
                    self._vehicle_characteristics = None
                    self._vehicle_characteristics_actor_id = None
                    self.last_error = ""
                    return actor
            self.actor = None
            self._vehicle_characteristics = None
            self._vehicle_characteristics_actor_id = None
            self.last_error = "ego_not_found"
            return None
        except Exception as exc:
            self.last_error = f"discover_failed:{exc}"
            self.actor = None
            self._vehicle_characteristics = None
            self._vehicle_characteristics_actor_id = None
            return None

    @staticmethod
    def _maybe_metric_scale(values: Sequence[float], *, threshold: float = 20.0) -> float:
        finite = [abs(float(v)) for v in values if v is not None]
        if not finite:
            return 1.0
        return 0.01 if max(finite) > threshold else 1.0

    def _probe_vehicle_characteristics(self, actor: Any) -> Optional[Dict[str, Any]]:
        actor_id = int(getattr(actor, "id", 0) or 0)
        if self._vehicle_characteristics is not None and self._vehicle_characteristics_actor_id == actor_id:
            return dict(self._vehicle_characteristics)
        try:
            bbox = actor.bounding_box
            extent = getattr(bbox, "extent", None)
            loc = getattr(bbox, "location", None)
            if extent is None or loc is None:
                raise RuntimeError("bounding_box_missing")
            length = max(2.0 * float(getattr(extent, "x", 0.0)), 0.0)
            width = max(2.0 * float(getattr(extent, "y", 0.0)), 0.0)
            height = max(2.0 * float(getattr(extent, "z", 0.0)), 0.0)
            front_edge_to_center = max(float(getattr(extent, "x", 0.0)) + float(getattr(loc, "x", 0.0)), 0.0)
            back_edge_to_center = max(float(getattr(extent, "x", 0.0)) - float(getattr(loc, "x", 0.0)), 0.0)
            left_edge_to_center = max(float(getattr(extent, "y", 0.0)) + float(getattr(loc, "y", 0.0)), 0.0)
            right_edge_to_center = max(float(getattr(extent, "y", 0.0)) - float(getattr(loc, "y", 0.0)), 0.0)

            physics = actor.get_physics_control()
            wheels = list(getattr(physics, "wheels", []) or [])
            raw_positions: List[float] = []
            for wheel in wheels:
                pos = getattr(wheel, "position", None)
                if pos is None:
                    continue
                raw_positions.extend(
                    [
                        float(getattr(pos, "x", 0.0)),
                        float(getattr(pos, "y", 0.0)),
                        float(getattr(pos, "z", 0.0)),
                    ]
                )
            scale = self._maybe_metric_scale(raw_positions) if raw_positions else 1.0
            wheel_x_positions: List[float] = []
            wheel_radius_candidates: List[float] = []
            max_steer_candidates: List[float] = []
            for wheel in wheels:
                pos = getattr(wheel, "position", None)
                if pos is not None:
                    wheel_x_positions.append(float(getattr(pos, "x", 0.0)) * scale)
                radius = float(getattr(wheel, "radius", 0.0) or 0.0)
                if radius > 0.0:
                    wheel_radius_candidates.append(radius * (0.01 if radius > 5.0 else 1.0))
                max_steer = float(getattr(wheel, "max_steer_angle", 0.0) or 0.0)
                if abs(max_steer) > 1e-6:
                    max_steer_candidates.append(abs(max_steer))
            wheel_base = None
            if len(wheel_x_positions) >= 2:
                xs = sorted(wheel_x_positions)
                front_x = sum(xs[-2:]) / min(2, len(xs[-2:]))
                rear_x = sum(xs[:2]) / min(2, len(xs[:2]))
                if front_x > rear_x:
                    wheel_base = front_x - rear_x
            max_steer_angle_deg = max(max_steer_candidates) if max_steer_candidates else None
            min_turn_radius = None
            if wheel_base and max_steer_angle_deg and max_steer_angle_deg > 1e-3:
                min_turn_radius = wheel_base / max(math.tan(math.radians(max_steer_angle_deg)), 1e-6)
            out: Dict[str, Any] = {
                "actor_id": actor_id,
                "role_name": ((getattr(actor, "attributes", {}) or {}).get("role_name", "") or "").strip(),
                "length": length,
                "width": width,
                "height": height,
                "front_edge_to_center": front_edge_to_center,
                "back_edge_to_center": back_edge_to_center,
                "left_edge_to_center": left_edge_to_center,
                "right_edge_to_center": right_edge_to_center,
                "wheel_base": wheel_base,
                "max_steer_angle_deg": max_steer_angle_deg,
                "min_turn_radius": min_turn_radius,
                "wheel_rolling_radius": (
                    sum(wheel_radius_candidates) / len(wheel_radius_candidates)
                    if wheel_radius_candidates
                    else None
                ),
                "mass_kg": float(getattr(physics, "mass", 0.0) or 0.0),
                "drag_coefficient": float(getattr(physics, "drag_coefficient", 0.0) or 0.0),
                "physics_wheel_count": len(wheels),
                "physics_position_scale": scale,
            }
            self._vehicle_characteristics = dict(out)
            self._vehicle_characteristics_actor_id = actor_id
            self.last_error = ""
            return out
        except Exception as exc:
            self.last_error = f"physics_probe_failed:{exc}"
            self._vehicle_characteristics = None
            self._vehicle_characteristics_actor_id = None
            return None

    def read_control(self) -> Optional[Dict[str, float]]:
        actor = self.actor or self._discover_actor()
        if actor is None:
            return None
        try:
            ctrl = actor.get_control()
            vehicle = self._probe_vehicle_characteristics(actor)
            out = {
                "throttle": float(getattr(ctrl, "throttle", 0.0)),
                "brake": float(getattr(ctrl, "brake", 0.0)),
                "steer": float(getattr(ctrl, "steer", 0.0)),
                "reverse": 1.0 if bool(getattr(ctrl, "reverse", False)) else 0.0,
                "hand_brake": 1.0 if bool(getattr(ctrl, "hand_brake", False)) else 0.0,
            }
            steer_feedback_pct = out["steer"] * 100.0
            steer_feedback_deg = None
            steer_feedback_source = "carla_control"
            if vehicle:
                max_steer_deg = _safe_float(vehicle.get("max_steer_angle_deg"), 0.0)
                get_wheel_angle = getattr(actor, "get_wheel_steer_angle", None)
                wheel_loc_enum = getattr(carla, "VehicleWheelLocation", None) if carla is not None else None
                if callable(get_wheel_angle) and wheel_loc_enum is not None and max_steer_deg > 1e-3:
                    try:
                        fl = float(get_wheel_angle(getattr(wheel_loc_enum, "FL_Wheel")))
                        fr = float(get_wheel_angle(getattr(wheel_loc_enum, "FR_Wheel")))
                        steer_feedback_deg = 0.5 * (fl + fr)
                        steer_feedback_pct = _clamp(steer_feedback_deg / max_steer_deg, -1.0, 1.0) * 100.0
                        steer_feedback_source = "wheel_angle"
                    except Exception:
                        pass
            vel = actor.get_velocity()
            accel = actor.get_acceleration()
            speed_mps = math.sqrt(
                float(getattr(vel, "x", 0.0)) ** 2
                + float(getattr(vel, "y", 0.0)) ** 2
                + float(getattr(vel, "z", 0.0)) ** 2
            )
            accel_mag = math.sqrt(
                float(getattr(accel, "x", 0.0)) ** 2
                + float(getattr(accel, "y", 0.0)) ** 2
                + float(getattr(accel, "z", 0.0)) ** 2
            )
            fwd = actor.get_transform().get_forward_vector()
            forward_accel = (
                float(getattr(accel, "x", 0.0)) * float(getattr(fwd, "x", 0.0))
                + float(getattr(accel, "y", 0.0)) * float(getattr(fwd, "y", 0.0))
                + float(getattr(accel, "z", 0.0)) * float(getattr(fwd, "z", 0.0))
            )
            out["steer_feedback_pct"] = float(steer_feedback_pct)
            out["steer_feedback_source"] = steer_feedback_source
            if steer_feedback_deg is not None:
                out["steer_feedback_deg"] = float(steer_feedback_deg)
            out["speed_mps"] = float(speed_mps)
            out["accel_mps2"] = float(accel_mag)
            out["forward_accel_mps2"] = float(forward_accel)
            if vehicle:
                out["vehicle_characteristics"] = dict(vehicle)
            self.last_error = ""
            return out
        except Exception as exc:
            self.last_error = f"read_failed:{exc}"
            self.actor = None
            self._vehicle_characteristics = None
            self._vehicle_characteristics_actor_id = None
            return None

    def find_vehicle_by_roles(self, role_names: Sequence[str]):
        if self.client is None and not self._connect():
            return None
        wanted = {str(name) for name in role_names if str(name)}
        if not wanted:
            return None
        try:
            self.world = self.client.get_world()
            for actor in self.world.get_actors().filter("vehicle.*"):
                role = ((getattr(actor, "attributes", {}) or {}).get("role_name", "") or "").strip()
                if role in wanted:
                    self.last_error = ""
                    return actor
        except Exception as exc:
            self.last_error = f"find_role_failed:{exc}"
        return None


class RosCacheNode(Node):
    def __init__(
        self,
        *,
        odom_topic: str,
        objects3d_topic: str,
        objects_markers_topic: str,
        objects_json_topic: str,
        control_out_topic: str,
        control_out_type: str,
        use_objects3d: bool,
        use_markers: bool,
    ) -> None:
        super().__init__("apollo10_ros_cache_bridge")
        self.lock = threading.Lock()
        self.latest_odom: Optional[Odometry] = None
        self.latest_objects3d: Optional[Any] = None
        self.latest_markers: Optional[Any] = None
        self.latest_objects_json: Optional[str] = None
        self.rx_counts = {"odom": 0, "objects3d": 0, "markers": 0, "objects_json": 0}
        self.control_out_type = str(control_out_type or "ackermann").lower()
        if self.control_out_type == "ackermann":
            if AckermannDriveStamped is None:
                raise RuntimeError("ackermann_msgs is not available, cannot publish ackermann control")
            self.control_pub = self.create_publisher(AckermannDriveStamped, control_out_topic, 10)
        elif self.control_out_type == "twist":
            if Twist is None:
                raise RuntimeError("geometry_msgs/Twist is not available, cannot publish twist control")
            self.control_pub = self.create_publisher(Twist, control_out_topic, 10)
        elif self.control_out_type == "direct":
            if Float32MultiArray is None:
                raise RuntimeError("std_msgs/Float32MultiArray is not available, cannot publish direct control")
            self.control_pub = self.create_publisher(Float32MultiArray, control_out_topic, 10)
        else:
            raise RuntimeError(
                f"unsupported control_out_type={self.control_out_type}, expected ackermann|twist|direct"
            )
        best_effort_qos = QoSProfile(
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=20,
            reliability=QoSReliabilityPolicy.BEST_EFFORT,
        )
        self.create_subscription(Odometry, odom_topic, self._on_odom, qos_profile_sensor_data)
        if use_objects3d and Detection3DArray is not None:
            self.create_subscription(Detection3DArray, objects3d_topic, self._on_objects3d, best_effort_qos)
        if use_markers and MarkerArray is not None:
            self.create_subscription(MarkerArray, objects_markers_topic, self._on_markers, best_effort_qos)
        self.create_subscription(String, objects_json_topic, self._on_objects_json, best_effort_qos)

    def _on_odom(self, msg: Odometry) -> None:
        with self.lock:
            self.latest_odom = msg
            self.rx_counts["odom"] += 1

    def _on_objects3d(self, msg: Any) -> None:
        with self.lock:
            self.latest_objects3d = msg
            self.rx_counts["objects3d"] += 1

    def _on_markers(self, msg: Any) -> None:
        with self.lock:
            self.latest_markers = msg
            self.rx_counts["markers"] += 1

    def _on_objects_json(self, msg: String) -> None:
        with self.lock:
            self.latest_objects_json = msg.data
            self.rx_counts["objects_json"] += 1

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "odom": self.latest_odom,
                "objects3d": self.latest_objects3d,
                "markers": self.latest_markers,
                "objects_json": self.latest_objects_json,
                "rx_counts": dict(self.rx_counts),
            }

    def publish_control(self, msg: Any) -> None:
        self.control_pub.publish(msg)


class ApolloGtBridge:
    def __init__(self, cfg: Dict[str, Any], *, stats_path: Path, apollo_root: Path, pb_root: Path) -> None:
        self.cfg = cfg
        self.stats_path = stats_path
        self.apollo_root = apollo_root
        self.pb_root = pb_root
        self.stop_event = threading.Event()
        self.artifacts_dir = self.stats_path.parent
        self.seq = 0
        self.last_control = {"throttle": 0.0, "brake": 0.0, "steer_pct": 0.0}
        self.stats = {
            "last_publish_ts_sec": 0.0,
            "loc_count": 0,
            "chassis_count": 0,
            "obstacles_count": 0,
            "publish_errors": 0,
            "control_rx_count": 0,
            "control_tx_count": 0,
            "routing_request_count": 0,
            "routing_response_count": 0,
            "routing_success_count": 0,
            "routing_empty_count": 0,
            "routing_last_road_count": 0,
            "action_follow_count": 0,
            "lane_follow_count": 0,
            "lane_follow_no_response_count": 0,
            "lane_follow_disabled_on_no_response": False,
            "last_error": "",
            "last_publish_error": "",
            "ros_input_counts": {
                "odom": 0,
                "objects3d": 0,
                "markers": 0,
                "objects_json": 0,
            },
            "last_obstacles_count": 0,
            "last_control_raw": {},
            "last_control_in": {},
            "last_control_out": {},
            "last_measured_control": {},
            "carla_vehicle": {},
            "last_pose_debug": {},
            "auto_calib": {"enabled": False, "applied": False},
            "steer_sign_auto_check": {
                "suggested": None,
                "confidence": 0.0,
                "samples": 0,
                "applied": False,
            },
            "control_anomaly": {"active": False, "count": 0},
            "last_routing_anchor": {},
            "last_routing_goal": {},
            "last_routing_projection": {},
            "routing_phase_counts": {"startup": 0, "long": 0},
            "last_routing_reason": "",
            "traffic_light": {},
            "front_obstacle_behavior": {},
            "planning": {},
            "health": {},
        }

        bridge_cfg = (cfg.get("bridge", {}) or {})
        tf_cfg = (bridge_cfg.get("carla_to_apollo", {}) or {})
        self.tf = Transform2D(
            tx=float(tf_cfg.get("tx", 0.0)),
            ty=float(tf_cfg.get("ty", 0.0)),
            tz=float(tf_cfg.get("tz", 0.0)),
            yaw_deg=float(tf_cfg.get("yaw_deg", 0.0)),
        )
        self.publish_rate_hz = float(bridge_cfg.get("publish_rate_hz", 20.0))
        self.max_obstacles = int(bridge_cfg.get("max_obstacles", 64))
        self.radius_m = float(bridge_cfg.get("radius_m", 120.0))
        self.localization_back_offset_m = float(
            bridge_cfg.get("localization_back_offset_m", 1.4375)
        )
        self.map_file_path = str(bridge_cfg.get("map_file", "")).strip()
        self.map_bounds_file = str(bridge_cfg.get("map_bounds_file", "")).strip()
        self.map_segments: List[Tuple[float, float, float, float]] = []
        self.debug_pose_print = bool(bridge_cfg.get("debug_pose_print", False))
        self.debug_csv_path = self.artifacts_dir / "debug_timeseries.csv"
        self.health_summary_path = self.artifacts_dir / "bridge_health_summary.json"
        self.carla_vehicle_path = self.artifacts_dir / "carla_vehicle_characteristics.json"
        self._debug_csv_header_written = False
        self._carla_vehicle_written = False
        self._debug_window: deque[Dict[str, Any]] = deque()
        self._debug_window_sec = 5.0
        self._steer_sat_counter = 0
        self._last_sat_snapshot_key = ""
        self._last_control_print_sec = 0.0
        self._last_publish_error_log_sec = 0.0
        self._warn_value_log_sec: Dict[str, float] = {}
        self.auto_calib_enabled = bool(tf_cfg.get("auto_calib", False))
        self.auto_calib_snap_right_angle = bool(tf_cfg.get("auto_calib_snap_right_angle", False))
        self.auto_calib_dump_file = str(
            tf_cfg.get("auto_calib_dump_file") or (self.artifacts_dir / "auto_calib_suggestion.json")
        )
        self._auto_calib_samples: List[Dict[str, float]] = []
        self._auto_calib_sample_target = max(5, int(tf_cfg.get("auto_calib_samples", 20)))
        self._auto_calib_applied = False
        self.stats["auto_calib"] = {
            "enabled": self.auto_calib_enabled,
            "applied": False,
            "sample_target": self._auto_calib_sample_target,
            "snap_right_angle": self.auto_calib_snap_right_angle,
        }
        ctrl_map = bridge_cfg.get("control_mapping", {}) or {}
        self.debug_dump_control_raw = bool(bridge_cfg.get("debug_dump_control_raw", False))
        self.control_decode_dump_path = self.artifacts_dir / "control_decode_debug.jsonl"
        self.steer_sign_suggestion_path = self.artifacts_dir / "steer_sign_suggestion.json"
        self.control_anomaly_path_prefix = self.artifacts_dir / "control_anomaly_snapshot"
        self.max_steer_angle = float(ctrl_map.get("max_steer_angle", 0.6))
        self.speed_gain = float(ctrl_map.get("speed_gain", 10.0))
        self.brake_gain = float(ctrl_map.get("brake_gain", 5.0))
        self.steer_sign = float(ctrl_map.get("steer_sign", 1.0))
        self.auto_apply_steer_sign = bool(ctrl_map.get("auto_apply_steer_sign", False))
        self.throttle_scale = float(ctrl_map.get("throttle_scale", 1.0))
        self.brake_scale = float(ctrl_map.get("brake_scale", 1.0))
        self.steer_scale = float(ctrl_map.get("steer_scale", 1.0))
        self.brake_deadzone = float(ctrl_map.get("brake_deadzone", 0.05))
        self.zero_hold_sec = float(ctrl_map.get("zero_hold_sec", 0.0))
        self.startup_throttle_boost_enabled = bool(ctrl_map.get("startup_throttle_boost_enabled", True))
        self.startup_throttle_boost_add = float(ctrl_map.get("startup_throttle_boost_add", 0.08))
        self.startup_throttle_boost_cap = float(ctrl_map.get("startup_throttle_boost_cap", 0.25))
        self.straight_lane_zero_steer_enabled = bool(ctrl_map.get("straight_lane_zero_steer_enabled", False))
        self.straight_lane_zero_steer_max_speed_mps = float(
            ctrl_map.get("straight_lane_zero_steer_max_speed_mps", 8.0)
        )
        self.straight_lane_zero_steer_max_e_y_m = float(ctrl_map.get("straight_lane_zero_steer_max_e_y_m", 0.15))
        self.straight_lane_zero_steer_max_e_psi_deg = float(
            ctrl_map.get("straight_lane_zero_steer_max_e_psi_deg", 3.0)
        )
        self.straight_lane_zero_steer_max_curvature = float(
            ctrl_map.get("straight_lane_zero_steer_max_curvature", 0.001)
        )
        self.straight_lane_zero_steer_latch_enabled = bool(
            ctrl_map.get("straight_lane_zero_steer_latch_enabled", True)
        )
        self.straight_lane_zero_steer_release_max_e_y_m = float(
            ctrl_map.get("straight_lane_zero_steer_release_max_e_y_m", 0.6)
        )
        self.straight_lane_zero_steer_release_max_e_psi_deg = float(
            ctrl_map.get("straight_lane_zero_steer_release_max_e_psi_deg", 6.0)
        )
        self.straight_lane_zero_steer_release_ignore_e_psi_below_speed_mps = float(
            ctrl_map.get("straight_lane_zero_steer_release_ignore_e_psi_below_speed_mps", 0.5)
        )
        self.low_speed_steer_guard_enabled = bool(ctrl_map.get("low_speed_steer_guard_enabled", True))
        self.low_speed_steer_guard_speed_mps = float(ctrl_map.get("low_speed_steer_guard_speed_mps", 0.2))
        self.low_speed_steer_guard_max_abs_steer = float(ctrl_map.get("low_speed_steer_guard_max_abs_steer", 0.05))
        self.low_speed_steer_guard_max_e_y_m = float(ctrl_map.get("low_speed_steer_guard_max_e_y_m", 0.2))
        self.low_speed_steer_guard_max_e_psi_deg = float(
            ctrl_map.get("low_speed_steer_guard_max_e_psi_deg", 2.0)
        )
        self.force_zero_steer_output = bool(ctrl_map.get("force_zero_steer_output", False))
        straight_acc_cfg = (ctrl_map.get("straight_acc_override", {}) or {})
        self.straight_acc_override_enabled = bool(straight_acc_cfg.get("enabled", False))
        self.straight_acc_override_mode = str(
            straight_acc_cfg.get("mode", "cruise_then_stop") or "cruise_then_stop"
        ).strip().lower()
        self.straight_acc_override_target_speed_mps = float(
            straight_acc_cfg.get("target_speed_mps", 0.0)
        )
        self.straight_acc_override_min_throttle = float(
            straight_acc_cfg.get("min_cruise_throttle", 0.22)
        )
        self.straight_acc_override_max_throttle = float(
            straight_acc_cfg.get("max_throttle", 0.55)
        )
        self.straight_acc_override_speed_kp = float(
            straight_acc_cfg.get("speed_kp", 0.03)
        )
        self.straight_acc_override_coast_band_mps = float(
            straight_acc_cfg.get("coast_band_mps", 0.4)
        )
        self.straight_acc_override_max_brake = float(
            straight_acc_cfg.get("max_brake", 0.45)
        )
        self.straight_acc_override_brake_kp = float(
            straight_acc_cfg.get("brake_kp", 0.12)
        )
        self.straight_acc_override_slowdown_distance_m = float(
            straight_acc_cfg.get("slowdown_distance_m", 40.0)
        )
        self.straight_acc_override_stop_distance_m = float(
            straight_acc_cfg.get("stop_distance_m", 10.0)
        )
        self.straight_acc_override_full_brake_distance_m = float(
            straight_acc_cfg.get("full_brake_distance_m", 6.0)
        )
        self.straight_acc_override_stop_hold_brake = float(
            straight_acc_cfg.get("stop_hold_brake", 0.25)
        )
        self.straight_acc_override_max_e_y_m = float(
            straight_acc_cfg.get("max_e_y_m", 1.0)
        )
        self.straight_acc_override_max_curvature = float(
            straight_acc_cfg.get("max_curvature", 0.002)
        )
        self.straight_acc_override_max_lateral_gap_m = float(
            straight_acc_cfg.get("max_lateral_gap_m", 3.5)
        )
        self.straight_acc_override_min_front_gap_m = float(
            straight_acc_cfg.get("min_valid_front_gap_m", 2.0)
        )
        terminal_stop_hold_cfg = (ctrl_map.get("terminal_stop_hold", {}) or {})
        self.terminal_stop_hold_enabled = bool(terminal_stop_hold_cfg.get("enabled", False))
        self.terminal_stop_hold_activate_gap_m = float(
            terminal_stop_hold_cfg.get("activate_gap_m", 15.0)
        )
        self.terminal_stop_hold_release_gap_m = float(
            terminal_stop_hold_cfg.get(
                "release_gap_m",
                max(self.terminal_stop_hold_activate_gap_m + 4.0, self.terminal_stop_hold_activate_gap_m),
            )
        )
        self.terminal_stop_hold_activate_speed_mps = float(
            terminal_stop_hold_cfg.get("activate_speed_mps", 0.6)
        )
        self.terminal_stop_hold_release_speed_mps = float(
            terminal_stop_hold_cfg.get("release_speed_mps", 1.5)
        )
        self.terminal_stop_hold_brake = float(
            terminal_stop_hold_cfg.get("hold_brake", 0.18)
        )
        self.terminal_stop_hold_recent_brake_window_sec = float(
            terminal_stop_hold_cfg.get("recent_brake_window_sec", 2.0)
        )
        self.terminal_stop_hold_brake_trigger = float(
            terminal_stop_hold_cfg.get("brake_trigger", 0.05)
        )
        self.terminal_stop_hold_max_lateral_gap_m = float(
            terminal_stop_hold_cfg.get("max_lateral_gap_m", 3.5)
        )
        self.terminal_stop_hold_min_hold_sec = float(
            terminal_stop_hold_cfg.get("min_hold_sec", 0.6)
        )
        self._last_longitudinal_override: Dict[str, Any] = {
            "enabled": self.straight_acc_override_enabled,
            "active": False,
            "mode": self.straight_acc_override_mode,
        }
        self._steer_sign_window: deque[Dict[str, Any]] = deque()
        self._steer_sign_window_size = max(10, int(ctrl_map.get("steer_sign_check_frames", 30)))
        self._steer_sign_auto_applied = False
        self._control_anomaly_counter = 0
        self._last_control_anomaly_key = ""
        self._last_nonzero_throttle = 0.0
        self._last_nonzero_ts = 0.0
        self._latest_speed_mps = 0.0
        self._straight_lane_zero_steer_active = False
        self._max_speed_mps = 0.0
        self._terminal_stop_hold_active = False
        self._terminal_stop_hold_started_ts = 0.0
        self._terminal_stop_hold_last_brake_ts = 0.0
        self._terminal_stop_hold_engaged_count = 0
        front_obstacle_cfg = (bridge_cfg.get("front_obstacle_behavior", {}) or {})
        self.front_obstacle_behavior_mode = str(
            front_obstacle_cfg.get("mode", "normal") or "normal"
        ).strip().lower()
        self.front_obstacle_activate_distance_m = float(
            front_obstacle_cfg.get("activate_distance_m", 18.0)
        )
        self.front_obstacle_release_distance_m = float(
            front_obstacle_cfg.get(
                "release_distance_m",
                max(self.front_obstacle_activate_distance_m + 6.0, self.front_obstacle_activate_distance_m),
            )
        )
        self.front_obstacle_min_longitudinal_m = float(
            front_obstacle_cfg.get("min_longitudinal_m", 2.0)
        )
        self.front_obstacle_max_lateral_m = float(
            front_obstacle_cfg.get("max_lateral_m", 3.5)
        )
        self.front_obstacle_latch_enabled = bool(
            front_obstacle_cfg.get("latch_enabled", True)
        )
        self.front_obstacle_role_names = [
            str(item) for item in (front_obstacle_cfg.get("role_names") or ["front"])
        ]
        self._front_obstacle_visible = self.front_obstacle_behavior_mode == "normal"
        self._front_obstacle_last_gap: Dict[str, Any] = {}
        self._front_obstacle_suppressed_frames = 0
        self._front_obstacle_state_changed_ts = 0.0
        self.front_obstacle_cache_enabled = bool(front_obstacle_cfg.get("cache_enabled", True))
        self.front_obstacle_cache_ttl_sec = float(front_obstacle_cfg.get("cache_ttl_sec", 0.5))
        self._obstacle_cache_payload: Optional[bytes] = None
        self._obstacle_cache_count = 0
        self._obstacle_cache_ts_sec = 0.0
        self._obstacle_cache_hit_count = 0
        self._obstacle_cache_last_hit_ts_sec = 0.0

        ros_cfg = cfg.get("ros2", {}) or {}
        cyber_cfg = cfg.get("cyber", {}) or {}
        self.ros_ego_id = str(ros_cfg.get("ego_id", "hero"))
        self.odom_topic = str(ros_cfg["odom_topic"])
        self.objects3d_topic = str(ros_cfg["objects3d_topic"])
        self.objects_markers_topic = str(ros_cfg["objects_markers_topic"])
        self.objects_json_topic = str(ros_cfg["objects_json_topic"])
        self.control_out_topic = str(ros_cfg["control_out_topic"])
        self.control_out_type = str(ros_cfg.get("control_out_type", "ackermann"))
        self.localization_channel = str(cyber_cfg["localization_channel"])
        self.chassis_channel = str(cyber_cfg["chassis_channel"])
        self.obstacles_channel = str(cyber_cfg["obstacles_channel"])
        self.control_channel = str(cyber_cfg["control_channel"])
        self.routing_request_channel = str(cyber_cfg.get("routing_request_channel", "/apollo/raw_routing_request"))
        self.action_channel = str(cyber_cfg.get("action_channel", "/apollo/external_command/action"))
        self.lane_follow_channel = str(cyber_cfg.get("lane_follow_channel", "/apollo/external_command/lane_follow"))
        self.routing_response_channel = str(cyber_cfg.get("routing_response_channel", "/apollo/routing_response"))
        self.traffic_light_channel = str(cyber_cfg.get("traffic_light_channel", "/apollo/perception/traffic_light"))
        self.planning_channel = str(cyber_cfg.get("planning_channel", "/apollo/planning"))

        auto_routing_cfg = ((cfg.get("bridge", {}) or {}).get("auto_routing", {}) or {})
        self.auto_routing_enabled = bool(auto_routing_cfg.get("enabled", False))
        self.auto_routing_goal_mode = str(auto_routing_cfg.get("goal_mode", "ego_seed_ahead") or "ego_seed_ahead")
        self.auto_routing_end_ahead_m = float(auto_routing_cfg.get("end_ahead_m", 80.0))
        self.auto_routing_min_end_ahead_m = float(auto_routing_cfg.get("min_end_ahead_m", 3.0))
        self.auto_routing_startup_end_ahead_m = float(auto_routing_cfg.get("startup_end_ahead_m", 6.0))
        self.auto_routing_startup_speed_threshold_mps = float(
            auto_routing_cfg.get("startup_speed_threshold_mps", 0.5)
        )
        self.auto_routing_startup_hold_sec = float(auto_routing_cfg.get("startup_hold_sec", 3.0))
        # Nudge routing start waypoint along lane heading so Apollo routing does
        # not occasionally classify the start as a tiny negative-s point.
        self.auto_routing_start_nudge_m = float(auto_routing_cfg.get("start_nudge_m", 2.0))
        self.auto_routing_start_nudge_retry_step_m = float(
            auto_routing_cfg.get("start_nudge_retry_step_m", 1.5)
        )
        self.auto_routing_start_nudge_min_safe_m = float(
            auto_routing_cfg.get("start_nudge_min_safe_m", 1.5)
        )
        self.auto_routing_start_nudge_max_m = float(
            auto_routing_cfg.get("start_nudge_max_m", 6.0)
        )
        self.auto_routing_resend_sec = float(auto_routing_cfg.get("resend_sec", 5.0))
        self.auto_routing_max_attempts = int(auto_routing_cfg.get("max_attempts", 5))
        self.auto_routing_target_speed = float(auto_routing_cfg.get("target_speed_mps", 8.0))
        self.auto_routing_startup_delay_sec = float(auto_routing_cfg.get("startup_delay_sec", 3.0))
        self.auto_routing_lane_follow_refresh_sec = float(
            auto_routing_cfg.get("lane_follow_refresh_sec", auto_routing_cfg.get("command_refresh_sec", 1.0))
        )
        self.auto_routing_send_action = bool(auto_routing_cfg.get("send_action", False))
        self.auto_routing_send_lane_follow = bool(auto_routing_cfg.get("send_lane_follow", False))
        self.auto_routing_disable_lane_follow_on_no_response = bool(
            auto_routing_cfg.get("disable_lane_follow_on_no_response", True)
        )
        # Apollo planning requires an external command (lane_follow/action). If both are off,
        # planning stays in "planning_command not ready"; keep lane_follow on as a safe fallback.
        if self.auto_routing_enabled and (not self.auto_routing_send_action) and (not self.auto_routing_send_lane_follow):
            self.auto_routing_send_lane_follow = True
            print(
                "[bridge][routing][warn] send_action=false and send_lane_follow=false; "
                "auto-enable lane_follow to provide planning_command"
            )
        self.auto_routing_send_routing = bool(auto_routing_cfg.get("send_routing_request", True))
        self.auto_routing_freeze_after_success = bool(auto_routing_cfg.get("freeze_after_success", True))
        self.auto_routing_use_seed_heading = bool(auto_routing_cfg.get("use_seed_heading", True))
        self.auto_routing_use_long_goal_after_move = bool(
            auto_routing_cfg.get("use_long_goal_after_move", True)
        )
        self.auto_routing_clamp_to_map_bounds = bool(auto_routing_cfg.get("clamp_to_map_bounds", True))
        self.auto_routing_map_bounds_margin_m = float(auto_routing_cfg.get("map_bounds_margin_m", 2.0))
        self.auto_routing_routing_sent = 0
        self.auto_routing_lane_follow_sent = 0
        self.auto_routing_last_routing_ts = 0.0
        self.auto_routing_last_lane_follow_ts = 0.0
        self.auto_routing_established = False
        self.auto_routing_seed_pose: Optional[Tuple[float, float, float]] = None
        self.auto_routing_active_goal: Optional[Tuple[float, float, float]] = None
        self.auto_routing_pending_goal: Optional[Tuple[float, float, float]] = None
        self.auto_routing_startup_routing_sent = False
        self.auto_routing_long_routing_sent = False
        self.auto_routing_startup_lane_follow_sent = False
        self.auto_routing_long_lane_follow_sent = False
        self.auto_routing_current_phase = "startup"
        self._lane_follow_disabled_runtime = False
        self.auto_routing_fixed_goal_xy = self._parse_goal_point(auto_routing_cfg.get("fixed_goal_xy"))
        scenario_goal_path = str(auto_routing_cfg.get("scenario_goal_path") or "scenario_goal.json").strip()
        self.auto_routing_scenario_goal_path = Path(scenario_goal_path)
        if not self.auto_routing_scenario_goal_path.is_absolute():
            self.auto_routing_scenario_goal_path = (self.artifacts_dir / self.auto_routing_scenario_goal_path).resolve()
        self._first_odom_ts: Optional[float] = None
        self.map_bounds_xy: Optional[Tuple[float, float, float, float]] = None
        self._scenario_goal_cache: Optional[Dict[str, Any]] = None
        self._scenario_goal_mtime_ns: Optional[int] = None
        self._load_map_geometry()
        self._load_map_bounds(auto_routing_cfg)

        traffic_light_cfg = (bridge_cfg.get("traffic_light", {}) or {})
        self.traffic_light_policy = str(traffic_light_cfg.get("policy", "force_green") or "force_green")
        self.traffic_light_force_ids = [str(item) for item in (traffic_light_cfg.get("force_ids") or ["TL_signal14"])]
        self.traffic_light_publish_hz = float(traffic_light_cfg.get("publish_hz", 10.0))
        self.traffic_light_channel = str(traffic_light_cfg.get("channel", self.traffic_light_channel))
        self.traffic_light_ignore_roll_enabled = bool(traffic_light_cfg.get("ignore_roll_enabled", False))
        self.traffic_light_ignore_roll_distance_m = float(
            traffic_light_cfg.get("ignore_roll_distance_m", 45.0)
        )
        self.traffic_light_ignore_roll_ahead_m = float(
            traffic_light_cfg.get("ignore_roll_ahead_m", 8.0)
        )
        self.traffic_light_ignore_roll_max_refresh = int(
            traffic_light_cfg.get("ignore_roll_max_refresh", 12)
        )
        self.traffic_light_pb2 = None
        self.traffic_light_writer = None
        self._last_traffic_light_publish_ts = 0.0
        self._traffic_light_publish_count = 0
        self._traffic_light_last_publish_ts = 0.0
        self._traffic_light_proto_error = ""
        self._ignore_roll_route_count = 0
        self._ignore_roll_start_xy: Optional[Tuple[float, float]] = None

        self.planning_pb2 = None
        self._planning_reader_enabled = False
        self._planning_msg_count = 0
        self._planning_nonempty_count = 0
        self._planning_empty_count = 0
        self._planning_last_points = 0
        self._planning_last_distance_to_destination: Optional[float] = None
        self._planning_last_msg_ts = 0.0

        carla_feedback_cfg = (bridge_cfg.get("carla_feedback", {}) or {})
        self.carla_feedback = None
        if bool(carla_feedback_cfg.get("enabled", True)):
            self.carla_feedback = CarlaFeedbackClient(
                host=str(carla_feedback_cfg.get("host", "127.0.0.1")),
                port=int(carla_feedback_cfg.get("port", 2000)),
                ego_role_name=str(carla_feedback_cfg.get("ego_role_name", self.ros_ego_id)),
                timeout_sec=float(carla_feedback_cfg.get("timeout_sec", 1.5)),
            )

        self.cyber, self.cyber_time = _import_cyber(apollo_root)
        (
            self.localization_pb2,
            self.chassis_pb2,
            self.perception_pb2,
            self.control_pb2,
            self.routing_pb2,
            self.action_pb2,
            self.command_status_pb2,
            self.lane_follow_pb2,
        ) = _import_apollo_pb(pb_root)
        self.traffic_light_pb2 = _import_optional_pb_module(
            self.apollo_root,
            self.pb_root,
            "modules.common_msgs.perception_msgs.traffic_light_detection_pb2",
        )
        self.planning_pb2 = _import_optional_pb_module(
            self.apollo_root,
            self.pb_root,
            "modules.common_msgs.planning_msgs.planning_pb2",
        )

        rclpy.init(args=None)
        self.node = RosCacheNode(
            odom_topic=self.odom_topic,
            objects3d_topic=self.objects3d_topic,
            objects_markers_topic=self.objects_markers_topic,
            objects_json_topic=self.objects_json_topic,
            control_out_topic=self.control_out_topic,
            control_out_type=self.control_out_type,
            use_objects3d=Detection3DArray is not None,
            use_markers=MarkerArray is not None,
        )
        self.executor = MultiThreadedExecutor(num_threads=2)
        self.executor.add_node(self.node)
        self.ros_thread = threading.Thread(target=self.executor.spin, name="ros2_executor", daemon=True)

        self.cyber.init("tb_apollo10_gt_bridge")
        self.cyber_node = self.cyber.Node("tb_apollo10_gt_bridge")
        self.loc_writer = self._cyber_create_writer(
            self.localization_channel, self.localization_pb2.LocalizationEstimate
        )
        self.chassis_writer = self._cyber_create_writer(
            self.chassis_channel, self.chassis_pb2.Chassis
        )
        self.obs_writer = self._cyber_create_writer(
            self.obstacles_channel, self.perception_pb2.PerceptionObstacles
        )
        self.routing_writer = None
        self.action_client = None
        self.lane_follow_client = None
        if self.auto_routing_enabled:
            if self.auto_routing_send_routing:
                self.routing_writer = self._cyber_create_writer(
                    self.routing_request_channel, self.routing_pb2.RoutingRequest
                )
            status_resp_type = (
                self.command_status_pb2.CommandStatus
                if self.command_status_pb2 is not None
                else empty_pb2.Empty
            )
            if self.auto_routing_send_action and self.action_pb2 is not None:
                self.action_client = self._cyber_create_client(
                    self.action_channel,
                    self.action_pb2.ActionCommand,
                    status_resp_type,
                )
            if self.auto_routing_send_lane_follow:
                self.lane_follow_client = self._cyber_create_client(
                    self.lane_follow_channel,
                    self.lane_follow_pb2.LaneFollowCommand,
                    status_resp_type,
                )
            self._cyber_create_reader(
                self.routing_response_channel, self.routing_pb2.RoutingResponse, self._on_routing_response
            )
        if self.traffic_light_policy == "force_green":
            if self.traffic_light_pb2 is not None and hasattr(self.traffic_light_pb2, "TrafficLightDetection"):
                self.traffic_light_writer = self._cyber_create_writer(
                    self.traffic_light_channel, self.traffic_light_pb2.TrafficLightDetection
                )
            else:
                self._traffic_light_proto_error = "traffic_light_detection_pb2_missing"
                self.traffic_light_policy = "ignore"
                print(
                    "[bridge][warn] traffic light proto unavailable, downgrade policy to ignore"
                )
        if self.planning_pb2 is not None and hasattr(self.planning_pb2, "ADCTrajectory"):
            self._cyber_create_reader(self.planning_channel, self.planning_pb2.ADCTrajectory, self._on_planning)
            self._planning_reader_enabled = True
        self._cyber_create_reader(self.control_channel, self.control_pb2.ControlCommand, self._on_control_cmd)
        self.cyber_spin_thread = None
        if hasattr(self.cyber_node, "spin"):
            self.cyber_spin_thread = threading.Thread(
                target=self.cyber_node.spin,
                name="cyber_spin",
                daemon=True,
            )
        self.stats["traffic_light"] = self._traffic_light_status()
        self.stats["front_obstacle_behavior"] = self._front_obstacle_behavior_status()
        self.stats["planning"] = self._planning_status()
        self.stats["health"] = self._health_summary()

    def _cyber_create_writer(self, channel: str, msg_type: Any):
        for fn in ("create_writer", "CreateWriter"):
            if hasattr(self.cyber_node, fn):
                return getattr(self.cyber_node, fn)(channel, msg_type)
        raise RuntimeError("cyber Node has no create_writer/CreateWriter")

    def _cyber_create_client(self, channel: str, req_type: Any, resp_type: Any):
        for fn in ("create_client", "CreateClient"):
            if hasattr(self.cyber_node, fn):
                return getattr(self.cyber_node, fn)(channel, req_type, resp_type)
        raise RuntimeError("cyber Node has no create_client/CreateClient")

    def _cyber_create_reader(self, channel: str, msg_type: Any, callback):
        for fn in ("create_reader", "CreateReader"):
            if hasattr(self.cyber_node, fn):
                return getattr(self.cyber_node, fn)(channel, msg_type, callback)
        raise RuntimeError("cyber Node has no create_reader/CreateReader")

    def _next_seq(self) -> int:
        self.seq += 1
        return self.seq

    def _fill_header(self, header: Any, ts_sec: float, module_name: str) -> None:
        if header is None:
            return
        _safe_set(header, "timestamp_sec", ts_sec)
        _safe_set(header, "module_name", module_name)
        _safe_set(header, "sequence_num", self._next_seq())

    def _command_now_sec(self) -> float:
        try:
            now_fn = getattr(self.cyber_time.Time, "now", None)
            if callable(now_fn):
                now_obj = now_fn()
                to_sec_fn = getattr(now_obj, "to_sec", None)
                if callable(to_sec_fn):
                    return float(to_sec_fn())
        except Exception:
            pass
        return time.time()

    def _warn_bad_value(self, field: str, value: Any, source: str) -> None:
        now = time.time()
        key = f"{source}:{field}"
        if (now - self._warn_value_log_sec.get(key, 0.0)) < 2.0:
            return
        self._warn_value_log_sec[key] = now
        print(f"[bridge][warn] invalid float field {field} from {source}: value={value!r}")

    def _coerce_float(self, value: Any, default: float, field: str, source: str) -> float:
        if value is None or value == "":
            self._warn_bad_value(field, value, source)
            return float(default)
        try:
            return float(value)
        except Exception:
            self._warn_bad_value(field, value, source)
            return float(default)

    def _append_jsonl(self, path: Path, payload: Dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a") as fp:
            fp.write(json.dumps(payload, ensure_ascii=True) + "\n")

    def _write_json_file(self, path: Path, payload: Dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2))
        tmp.replace(path)

    def _parse_goal_point(self, raw: Any) -> Optional[Dict[str, float]]:
        payload = raw
        if isinstance(payload, dict) and isinstance(payload.get("goal"), dict):
            payload = payload.get("goal")
        if not isinstance(payload, dict):
            return None
        if "x" not in payload or "y" not in payload:
            return None
        try:
            point = {
                "x": float(payload["x"]),
                "y": float(payload["y"]),
            }
        except Exception:
            return None
        if "z" in payload and payload.get("z") is not None:
            try:
                point["z"] = float(payload["z"])
            except Exception:
                pass
        return point

    def _load_scenario_goal_point(self) -> Optional[Dict[str, Any]]:
        path = self.auto_routing_scenario_goal_path
        if not path.exists():
            self._scenario_goal_cache = None
            self._scenario_goal_mtime_ns = None
            return None
        try:
            stat = path.stat()
            if self._scenario_goal_cache is not None and stat.st_mtime_ns == self._scenario_goal_mtime_ns:
                return dict(self._scenario_goal_cache)
            payload = json.loads(path.read_text())
            if not isinstance(payload, dict):
                return None
            parsed: Dict[str, Any] = {
                "path": str(path),
                "source": str(payload.get("source", "")),
                "frame": str(payload.get("frame", "apollo_map") or "apollo_map"),
            }
            if payload.get("relative_to") == "ego_heading" and payload.get("goal_ahead_m") is not None:
                parsed["relative_to"] = "ego_heading"
                parsed["goal_ahead_m"] = float(payload.get("goal_ahead_m"))
            else:
                point = self._parse_goal_point(payload)
                if point is None:
                    return None
                if parsed["frame"].lower() in {"carla", "carla_raw", "raw"}:
                    x_map, y_map, z_map = self.tf.apply_position(
                        float(point["x"]),
                        float(point["y"]),
                        float(point.get("z", 0.0)),
                    )
                    parsed.update({"x": x_map, "y": y_map, "z": z_map})
                else:
                    parsed.update(point)
            self._scenario_goal_cache = dict(parsed)
            self._scenario_goal_mtime_ns = stat.st_mtime_ns
            return parsed
        except Exception as exc:
            self.stats["last_error"] = f"scenario_goal_read_failed:{exc}"
            return None

    def _ignore_roll_active(self, x0: float, y0: float) -> bool:
        if self.traffic_light_policy != "ignore" or not self.traffic_light_ignore_roll_enabled:
            return False
        if self._ignore_roll_start_xy is None:
            return False
        dist = math.hypot(x0 - self._ignore_roll_start_xy[0], y0 - self._ignore_roll_start_xy[1])
        return dist < max(0.0, self.traffic_light_ignore_roll_distance_m)

    def _resolve_nested_float(self, msg: Any, chains: Sequence[Sequence[str]]) -> Optional[float]:
        for chain in chains:
            cur = msg
            for attr in chain:
                if cur is None or not hasattr(cur, attr):
                    cur = None
                    break
                cur = getattr(cur, attr)
            if cur is None:
                continue
            try:
                return float(cur)
            except Exception:
                continue
        return None

    def _planning_status(self) -> Dict[str, Any]:
        return {
            "reader_enabled": self._planning_reader_enabled,
            "msg_count": self._planning_msg_count,
            "nonempty_trajectory_count": self._planning_nonempty_count,
            "empty_trajectory_count": self._planning_empty_count,
            "last_trajectory_point_count": self._planning_last_points,
            "last_distance_to_destination": self._planning_last_distance_to_destination,
            "last_msg_ts_sec": self._planning_last_msg_ts,
        }

    def _traffic_light_status(self) -> Dict[str, Any]:
        return {
            "policy": self.traffic_light_policy,
            "channel": self.traffic_light_channel,
            "force_ids": list(self.traffic_light_force_ids),
            "publish_hz": self.traffic_light_publish_hz,
            "publish_count": self._traffic_light_publish_count,
            "last_publish_ts_sec": self._traffic_light_last_publish_ts,
            "writer_enabled": self.traffic_light_writer is not None,
            "proto_error": self._traffic_light_proto_error,
            "ignore_roll_enabled": self.traffic_light_ignore_roll_enabled,
            "ignore_roll_distance_m": self.traffic_light_ignore_roll_distance_m,
            "ignore_roll_ahead_m": self.traffic_light_ignore_roll_ahead_m,
            "ignore_roll_route_count": self._ignore_roll_route_count,
        }

    def _front_obstacle_behavior_status(self) -> Dict[str, Any]:
        gap = dict(self._front_obstacle_last_gap) if self._front_obstacle_last_gap else {}
        return {
            "mode": self.front_obstacle_behavior_mode,
            "role_names": list(self.front_obstacle_role_names),
            "activate_distance_m": self.front_obstacle_activate_distance_m,
            "release_distance_m": self.front_obstacle_release_distance_m,
            "min_longitudinal_m": self.front_obstacle_min_longitudinal_m,
            "max_lateral_m": self.front_obstacle_max_lateral_m,
            "latch_enabled": self.front_obstacle_latch_enabled,
            "visible": self._front_obstacle_visible,
            "suppressed_frames": self._front_obstacle_suppressed_frames,
            "last_state_changed_ts_sec": self._front_obstacle_state_changed_ts,
            "last_gap": gap,
            "cache_enabled": self.front_obstacle_cache_enabled,
            "cache_ttl_sec": self.front_obstacle_cache_ttl_sec,
            "cache_hit_count": self._obstacle_cache_hit_count,
            "cache_last_hit_ts_sec": self._obstacle_cache_last_hit_ts_sec,
        }

    def _terminal_stop_hold_status(self) -> Dict[str, Any]:
        gap = dict(self._front_obstacle_last_gap) if self._front_obstacle_last_gap else {}
        return {
            "enabled": self.terminal_stop_hold_enabled,
            "active": self._terminal_stop_hold_active,
            "activate_gap_m": self.terminal_stop_hold_activate_gap_m,
            "release_gap_m": self.terminal_stop_hold_release_gap_m,
            "activate_speed_mps": self.terminal_stop_hold_activate_speed_mps,
            "release_speed_mps": self.terminal_stop_hold_release_speed_mps,
            "hold_brake": self.terminal_stop_hold_brake,
            "recent_brake_window_sec": self.terminal_stop_hold_recent_brake_window_sec,
            "brake_trigger": self.terminal_stop_hold_brake_trigger,
            "min_hold_sec": self.terminal_stop_hold_min_hold_sec,
            "engaged_count": self._terminal_stop_hold_engaged_count,
            "last_brake_ts_sec": self._terminal_stop_hold_last_brake_ts,
            "started_ts_sec": self._terminal_stop_hold_started_ts,
            "front_gap": gap,
        }

    def _health_summary(self) -> Dict[str, Any]:
        last_goal = self.stats.get("last_routing_goal", {}) or {}
        front_status = self._front_obstacle_behavior_status()
        front_gap = front_status.get("last_gap", {}) or {}
        terminal_hold = self._terminal_stop_hold_status()
        return {
            "routing_goal_dist_m": last_goal.get("goal_dist_m"),
            "routing_goal_mode": last_goal.get("goal_mode"),
            "routing_startup_phase_used": last_goal.get("startup_phase_used"),
            "routing_phase": self.auto_routing_current_phase,
            "routing_request_count": self.stats.get("routing_request_count", 0),
            "routing_phase_counts": dict(self.stats.get("routing_phase_counts", {})),
            "distance_to_destination": self._planning_last_distance_to_destination,
            "planning_reader_enabled": self._planning_reader_enabled,
            "planning_has_trajectory": (
                self._planning_nonempty_count > 0 if self._planning_reader_enabled else None
            ),
            "planning_nonempty_trajectory_count": self._planning_nonempty_count,
            "planning_empty_trajectory_count": self._planning_empty_count,
            "traffic_light_policy": self.traffic_light_policy,
            "traffic_light_force_green_publish_count": self._traffic_light_publish_count,
            "traffic_light_last_publish_ts_sec": self._traffic_light_last_publish_ts,
            "traffic_light_force_green_active": (
                self.traffic_light_policy == "force_green" and self.traffic_light_writer is not None
            ),
            "traffic_light_ignore_roll_route_count": self._ignore_roll_route_count,
            "lane_follow_no_response_count": self.stats.get("lane_follow_no_response_count", 0),
            "lane_follow_disabled_on_no_response": self.stats.get("lane_follow_disabled_on_no_response", False),
            "carla_vehicle_detected": bool(self.stats.get("carla_vehicle")),
            "carla_feedback_source": (self.stats.get("last_measured_control", {}) or {}).get("source"),
            "front_obstacle_behavior_mode": front_status.get("mode"),
            "front_obstacle_visible": front_status.get("visible"),
            "front_obstacle_suppressed_frames": front_status.get("suppressed_frames"),
            "front_obstacle_gap_lon_m": front_gap.get("lon_m"),
            "front_obstacle_gap_lat_m": front_gap.get("lat_m"),
            "front_obstacle_gap_distance_m": front_gap.get("distance_m"),
            "front_obstacle_cache_hit_count": front_status.get("cache_hit_count"),
            "front_obstacle_cache_last_hit_ts_sec": front_status.get("cache_last_hit_ts_sec"),
            "terminal_stop_hold_active": terminal_hold.get("active"),
            "terminal_stop_hold_engaged_count": terminal_hold.get("engaged_count"),
            "terminal_stop_hold_front_gap_lon_m": (terminal_hold.get("front_gap", {}) or {}).get("lon_m"),
            "longitudinal_override_active": self._last_longitudinal_override.get("active", False),
            "longitudinal_override_phase": self._last_longitudinal_override.get("phase"),
            "longitudinal_target_speed_mps": self._last_longitudinal_override.get("target_speed_mps"),
            "longitudinal_front_gap_lon_m": self._last_longitudinal_override.get("front_gap_lon_m"),
            "speed_mps_last": self._latest_speed_mps,
            "speed_mps_max": self._max_speed_mps,
            "last_error": self.stats.get("last_error", ""),
        }

    def _write_health_summary(self) -> None:
        payload = self._health_summary()
        self.stats["planning"] = self._planning_status()
        self.stats["traffic_light"] = self._traffic_light_status()
        self.stats["front_obstacle_behavior"] = self._front_obstacle_behavior_status()
        self.stats["terminal_stop_hold"] = self._terminal_stop_hold_status()
        self.stats["health"] = payload
        self._write_json_file(self.health_summary_path, payload)

    def _on_planning(self, msg: Any) -> None:
        self._planning_msg_count += 1
        pts = getattr(msg, "trajectory_point", None)
        pt_count = len(pts) if pts is not None else 0
        self._planning_last_points = int(pt_count)
        self._planning_last_msg_ts = self._command_now_sec()
        if pt_count > 0:
            self._planning_nonempty_count += 1
        else:
            self._planning_empty_count += 1
        self._planning_last_distance_to_destination = self._resolve_nested_float(
            msg,
            (
                ("distance_to_destination",),
                ("debug", "distance_to_destination"),
                ("debug", "planning_data", "distance_to_destination"),
            ),
        )
        self.stats["planning"] = self._planning_status()

    def _maybe_publish_traffic_lights(self, ts_sec: float) -> None:
        if self.traffic_light_policy != "force_green" or self.traffic_light_writer is None:
            return
        period = 1.0 / max(self.traffic_light_publish_hz, 1e-3)
        if (ts_sec - self._last_traffic_light_publish_ts) < period:
            return
        try:
            msg = self.traffic_light_pb2.TrafficLightDetection()
            self._fill_header(getattr(msg, "header", None), self._command_now_sec(), "tb_apollo10_gt_bridge")
            if hasattr(msg, "contain_lights"):
                msg.contain_lights = bool(self.traffic_light_force_ids)
            if not hasattr(msg, "traffic_light"):
                raise RuntimeError("TrafficLightDetection has no traffic_light field")
            for signal_id in self.traffic_light_force_ids:
                light = msg.traffic_light.add()
                if hasattr(light, "id"):
                    light.id = str(signal_id)
                green_value = _enum_attr(type(light), ("GREEN",), None)
                if green_value is None and hasattr(self.traffic_light_pb2, "TrafficLight"):
                    green_value = _enum_attr(self.traffic_light_pb2.TrafficLight, ("GREEN",), None)
                if green_value is None:
                    green_value = 3
                if hasattr(light, "color"):
                    light.color = green_value
                elif hasattr(light, "status"):
                    light.status = green_value
                if hasattr(light, "confidence"):
                    light.confidence = 1.0
                if hasattr(light, "tracking_time"):
                    light.tracking_time = 0.1
            self.traffic_light_writer.write(msg)
            self._last_traffic_light_publish_ts = ts_sec
            self._traffic_light_last_publish_ts = ts_sec
            self._traffic_light_publish_count += 1
            self.stats["traffic_light"] = self._traffic_light_status()
        except Exception as exc:
            self._traffic_light_proto_error = str(exc)
            self.stats["last_error"] = f"traffic_light_publish_failed:{exc}"

    def _transform_pose(self, pos: Any, ori: Any) -> Dict[str, float]:
        raw_x = float(getattr(pos, "x", 0.0))
        raw_y = float(getattr(pos, "y", 0.0))
        raw_z = float(getattr(pos, "z", 0.0))
        raw_yaw = _quat_to_yaw(float(getattr(ori, "x", 0.0)), float(getattr(ori, "y", 0.0)), float(getattr(ori, "z", 0.0)), float(getattr(ori, "w", 1.0)))
        map_x, map_y, map_z = self.tf.apply_position(raw_x, raw_y, raw_z)
        map_yaw = self.tf.apply_yaw(raw_yaw)
        if self.localization_back_offset_m != 0.0:
            map_x -= self.localization_back_offset_m * math.cos(map_yaw)
            map_y -= self.localization_back_offset_m * math.sin(map_yaw)
        return {
            "raw_x": raw_x,
            "raw_y": raw_y,
            "raw_z": raw_z,
            "raw_yaw": raw_yaw,
            "map_x": map_x,
            "map_y": map_y,
            "map_z": map_z,
            "map_yaw": map_yaw,
        }

    def _pose_diagnostics(self, pose_info: Dict[str, float], ts_sec: float) -> Dict[str, Any]:
        x = pose_info["map_x"]
        y = pose_info["map_y"]
        yaw = pose_info["map_yaw"]
        in_bounds = True
        if self.map_bounds_xy is not None:
            x_min, x_max, y_min, y_max = self.map_bounds_xy
            in_bounds = (x_min <= x <= x_max) and (y_min <= y <= y_max)
        lane = _nearest_segment_metrics(x, y, self.map_segments) if self.map_segments else None
        lane_dist = float(lane["dist"]) if lane is not None else float("inf")
        e_y = float(lane["signed_e_y"]) if lane is not None else float("nan")
        e_psi = _wrap_to_pi(yaw - float(lane["seg_yaw"])) if lane is not None else float("nan")
        debug = {
            "ts_sec": ts_sec,
            "raw_x": pose_info["raw_x"],
            "raw_y": pose_info["raw_y"],
            "raw_yaw_deg": math.degrees(pose_info["raw_yaw"]),
            "map_x": x,
            "map_y": y,
            "map_yaw_deg": math.degrees(yaw),
            "in_bounds": in_bounds,
            "lane_dist_m": lane_dist,
            "lane_inside": bool(lane is not None and abs(e_y) <= 2.0),
            "e_y_m": e_y,
            "e_psi_deg": _rad_to_deg(e_psi) if lane is not None else float("nan"),
            "preview_x": float(lane["proj_x"]) if lane is not None else float("nan"),
            "preview_y": float(lane["proj_y"]) if lane is not None else float("nan"),
            "preview_heading_deg": math.degrees(float(lane["seg_yaw"])) if lane is not None else float("nan"),
            "target_curvature": float(lane["curvature"]) if lane is not None else float("nan"),
        }
        self.stats["last_pose_debug"] = debug
        if self.debug_pose_print:
            print(
                "[bridge][pose] raw=(%.2f,%.2f,%.1fdeg) map=(%.2f,%.2f,%.1fdeg) in_bounds=%s lane_dist=%.2f e_y=%.2f e_psi=%.1f"
                % (
                    debug["raw_x"],
                    debug["raw_y"],
                    debug["raw_yaw_deg"],
                    debug["map_x"],
                    debug["map_y"],
                    debug["map_yaw_deg"],
                    debug["in_bounds"],
                    debug["lane_dist_m"],
                    debug["e_y_m"],
                    debug["e_psi_deg"],
                )
            )
        return debug

    def _maybe_record_carla_vehicle(self, payload: Dict[str, Any]) -> None:
        if not payload:
            return
        self.stats["carla_vehicle"] = dict(payload)
        if self._carla_vehicle_written and self.carla_vehicle_path.exists():
            return
        self._write_json_file(self.carla_vehicle_path, payload)
        self._carla_vehicle_written = True

    def _read_measured_control(self) -> Dict[str, Any]:
        measured: Dict[str, Any] = {
            "available": False,
            "source": "unavailable",
            "throttle": None,
            "brake": None,
            "steer": None,
        }
        if self.carla_feedback is None:
            self.stats["last_measured_control"] = measured
            return measured
        state = self.carla_feedback.read_control()
        if state is None:
            measured["source"] = f"carla:{self.carla_feedback.last_error}"
            self.stats["last_measured_control"] = measured
            return measured
        vehicle_characteristics = state.get("vehicle_characteristics")
        if isinstance(vehicle_characteristics, dict):
            self._maybe_record_carla_vehicle(vehicle_characteristics)
        measured.update(
            {
                "available": True,
                "source": str(state.get("steer_feedback_source") or "carla.get_control"),
                "throttle": float(state["throttle"]),
                "brake": float(state["brake"]),
                "steer": float(state.get("steer_feedback_pct", float(state["steer"]) * 100.0)) / 100.0,
                "reverse": bool(state.get("reverse", 0.0)),
                "hand_brake": bool(state.get("hand_brake", 0.0)),
            }
        )
        if "steer_feedback_pct" in state:
            measured["steer_feedback_pct"] = float(state["steer_feedback_pct"])
        if "steer_feedback_deg" in state:
            measured["steer_feedback_deg"] = float(state["steer_feedback_deg"])
        if "speed_mps" in state:
            measured["speed_mps"] = float(state["speed_mps"])
        if "accel_mps2" in state:
            measured["accel_mps2"] = float(state["accel_mps2"])
        if "forward_accel_mps2" in state:
            measured["forward_accel_mps2"] = float(state["forward_accel_mps2"])
        self.stats["last_measured_control"] = measured
        return measured

    def _extract_raw_control_fields(self, cmd: Any) -> Dict[str, Any]:
        raw: Dict[str, Any] = {}
        for key in (
            "throttle",
            "brake",
            "steering_target",
            "steering_percentage",
            "steering",
            "steering_rate",
            "parking_brake",
            "driving_mode",
            "gear_location",
            "estop",
            "is_in_safe_mode",
        ):
            if hasattr(cmd, key):
                try:
                    raw[key] = getattr(cmd, key)
                except Exception:
                    raw[key] = "<unreadable>"
        return raw

    def _nearest_obstacle_distance(self, snapshot: Dict[str, Any], ego_xy: Tuple[float, float]) -> Optional[float]:
        best: Optional[float] = None
        ex, ey = ego_xy

        def consider(x: float, y: float) -> None:
            nonlocal best
            dist = math.hypot(x - ex, y - ey)
            if best is None or dist < best:
                best = dist

        dets = snapshot.get("objects3d")
        if dets is not None and hasattr(dets, "detections"):
            for det in list(dets.detections):
                center = getattr(det, "bbox", None)
                center = getattr(center, "center", None)
                pos = getattr(center, "position", None)
                if pos is None:
                    continue
                x, y, _ = self.tf.apply_position(
                    self._coerce_float(getattr(pos, "x", 0.0), 0.0, "det.position.x", "objects3d"),
                    self._coerce_float(getattr(pos, "y", 0.0), 0.0, "det.position.y", "objects3d"),
                    self._coerce_float(getattr(pos, "z", 0.0), 0.0, "det.position.z", "objects3d"),
                )
                consider(x, y)
        raw_json = snapshot.get("objects_json")
        if raw_json:
            try:
                payload = json.loads(raw_json)
                for item in payload.get("objects", []):
                    pose = item.get("pose", {}) or {}
                    x, y, _ = self.tf.apply_position(
                        self._coerce_float(pose.get("x", 0.0), 0.0, "objects_json.pose.x", "objects_json"),
                        self._coerce_float(pose.get("y", 0.0), 0.0, "objects_json.pose.y", "objects_json"),
                        self._coerce_float(pose.get("z", 0.0), 0.0, "objects_json.pose.z", "objects_json"),
                    )
                    consider(x, y)
            except Exception:
                pass
        markers = snapshot.get("markers")
        if markers is not None and hasattr(markers, "markers"):
            for marker in list(markers.markers):
                pose = getattr(marker, "pose", None)
                pos = getattr(pose, "position", None)
                if pos is None:
                    continue
                x, y, _ = self.tf.apply_position(
                    self._coerce_float(getattr(pos, "x", 0.0), 0.0, "marker.pose.x", "markers"),
                    self._coerce_float(getattr(pos, "y", 0.0), 0.0, "marker.pose.y", "markers"),
                    self._coerce_float(getattr(pos, "z", 0.0), 0.0, "marker.pose.z", "markers"),
                )
                consider(x, y)
        return best

    def _front_actor_gap(self) -> Optional[Dict[str, float]]:
        if self.carla_feedback is None:
            return None
        ego = self.carla_feedback.actor or self.carla_feedback._discover_actor()
        if ego is None:
            return None
        front = self.carla_feedback.find_vehicle_by_roles(self.front_obstacle_role_names)
        if front is None:
            return None
        try:
            ego_tr = ego.get_transform()
            front_tr = front.get_transform()
            ego_loc = ego_tr.location
            front_loc = front_tr.location
            dx = float(getattr(front_loc, "x", 0.0)) - float(getattr(ego_loc, "x", 0.0))
            dy = float(getattr(front_loc, "y", 0.0)) - float(getattr(ego_loc, "y", 0.0))
            dz = float(getattr(front_loc, "z", 0.0)) - float(getattr(ego_loc, "z", 0.0))
            fwd = ego_tr.get_forward_vector()
            fx = float(getattr(fwd, "x", 1.0))
            fy = float(getattr(fwd, "y", 0.0))
            norm = math.hypot(fx, fy)
            if norm <= 1e-6:
                fx, fy = 1.0, 0.0
            else:
                fx /= norm
                fy /= norm
            lon = dx * fx + dy * fy
            lat = -dx * fy + dy * fx
            dist = math.sqrt(dx * dx + dy * dy + dz * dz)
            return {
                "actor_id": int(getattr(front, "id", 0) or 0),
                "role_name": ((getattr(front, "attributes", {}) or {}).get("role_name", "") or "").strip(),
                "distance_m": float(dist),
                "lon_m": float(lon),
                "lat_m": float(lat),
                "dx_m": float(dx),
                "dy_m": float(dy),
                "dz_m": float(dz),
            }
        except Exception:
            return None

    def _front_obstacle_visible_now(self) -> bool:
        if self.front_obstacle_behavior_mode != "cruise_then_stop":
            self._front_obstacle_visible = True
            self._front_obstacle_last_gap = {}
            return True
        gap = self._front_actor_gap()
        self._front_obstacle_last_gap = dict(gap) if gap else {}
        if gap is None:
            self._front_obstacle_visible = True
            return True
        lon = float(gap.get("lon_m", float("inf")))
        lat = abs(float(gap.get("lat_m", 0.0)))
        if lon <= self.front_obstacle_min_longitudinal_m or lat > self.front_obstacle_max_lateral_m:
            self._front_obstacle_visible = True
            return True
        enter_visible = lon <= self.front_obstacle_activate_distance_m
        hold_visible = lon <= self.front_obstacle_release_distance_m
        prev_visible = self._front_obstacle_visible
        if self.front_obstacle_latch_enabled:
            self._front_obstacle_visible = (prev_visible and hold_visible) or enter_visible
        else:
            self._front_obstacle_visible = enter_visible
        if self._front_obstacle_visible != prev_visible:
            self._front_obstacle_state_changed_ts = time.time()
        return self._front_obstacle_visible

    def _compute_straight_acc_override(
        self,
        *,
        throttle_in: float,
        brake_in: float,
        target_curvature_abs: float,
        e_y_abs: float,
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "enabled": self.straight_acc_override_enabled,
            "active": False,
            "mode": self.straight_acc_override_mode,
            "throttle": throttle_in,
            "brake": brake_in,
            "target_speed_mps": None,
            "front_gap_lon_m": None,
            "front_gap_lat_m": None,
            "front_gap_distance_m": None,
            "phase": "disabled",
            "speed_error_mps": None,
        }
        if not self.straight_acc_override_enabled:
            self._last_longitudinal_override = result
            return result
        if target_curvature_abs > self.straight_acc_override_max_curvature:
            result["phase"] = "curvature_blocked"
            self._last_longitudinal_override = result
            return result
        if e_y_abs > self.straight_acc_override_max_e_y_m:
            result["phase"] = "lateral_blocked"
            self._last_longitudinal_override = result
            return result
        target_speed = float(self.straight_acc_override_target_speed_mps)
        if target_speed <= 0.0:
            target_speed = float(self.auto_routing_target_speed or 0.0)
        target_speed = max(0.0, target_speed)
        gap = self._front_actor_gap()
        if gap:
            result["front_gap_lon_m"] = float(gap.get("lon_m", 0.0))
            result["front_gap_lat_m"] = float(gap.get("lat_m", 0.0))
            result["front_gap_distance_m"] = float(gap.get("distance_m", 0.0))
        gap_valid = (
            gap is not None
            and float(gap.get("lon_m", -1.0)) > self.straight_acc_override_min_front_gap_m
            and abs(float(gap.get("lat_m", 0.0))) <= self.straight_acc_override_max_lateral_gap_m
        )
        desired_speed = target_speed
        phase = "cruise"
        if gap_valid and self.straight_acc_override_mode == "cruise_then_stop":
            lon = float(gap.get("lon_m", 0.0))
            if lon <= self.straight_acc_override_full_brake_distance_m:
                desired_speed = 0.0
                phase = "full_stop"
            elif lon <= self.straight_acc_override_stop_distance_m:
                desired_speed = 0.0
                phase = "stop_hold"
            elif lon <= self.straight_acc_override_slowdown_distance_m:
                span = max(
                    self.straight_acc_override_slowdown_distance_m
                    - self.straight_acc_override_stop_distance_m,
                    1e-3,
                )
                ratio = _clamp(
                    (lon - self.straight_acc_override_stop_distance_m) / span, 0.0, 1.0
                )
                desired_speed = target_speed * ratio
                phase = "slowdown"
        speed_error = float(desired_speed - self._latest_speed_mps)
        result["target_speed_mps"] = desired_speed
        result["speed_error_mps"] = speed_error
        if desired_speed <= 0.05:
            result["throttle"] = 0.0
            result["brake"] = max(
                self.straight_acc_override_stop_hold_brake,
                self.straight_acc_override_brake_kp * max(self._latest_speed_mps, 0.0),
            )
            result["active"] = True
            result["phase"] = phase
            self._last_longitudinal_override = result
            return result
        if speed_error > self.straight_acc_override_coast_band_mps:
            throttle_cmd = self.straight_acc_override_min_throttle + (
                self.straight_acc_override_speed_kp * speed_error
            )
            result["throttle"] = _clamp(
                throttle_cmd, 0.0, self.straight_acc_override_max_throttle
            )
            result["brake"] = 0.0
        elif speed_error < -self.straight_acc_override_coast_band_mps:
            result["throttle"] = 0.0
            result["brake"] = _clamp(
                self.straight_acc_override_brake_kp * (-speed_error),
                0.0,
                self.straight_acc_override_max_brake,
            )
        else:
            # In the cruise band, hold a small positive throttle so the demo keeps
            # rolling smoothly instead of coasting into Apollo's stop-happy edge cases.
            hold_throttle = min(
                self.straight_acc_override_min_throttle,
                self.straight_acc_override_max_throttle,
            )
            if desired_speed <= 0.5:
                hold_throttle = 0.0
            result["throttle"] = _clamp(hold_throttle, 0.0, 1.0)
            result["brake"] = 0.0
        result["active"] = True
        result["phase"] = phase
        self._last_longitudinal_override = result
        return result

    def _compute_terminal_stop_hold(
        self,
        *,
        throttle_in: float,
        brake_in: float,
        ts_sec: float,
    ) -> Dict[str, Any]:
        gap = dict(self._front_obstacle_last_gap) if self._front_obstacle_last_gap else {}
        result: Dict[str, Any] = {
            "enabled": self.terminal_stop_hold_enabled,
            "active": False,
            "phase": "disabled" if not self.terminal_stop_hold_enabled else "idle",
            "front_gap_lon_m": gap.get("lon_m"),
            "front_gap_lat_m": gap.get("lat_m"),
            "front_gap_distance_m": gap.get("distance_m"),
            "throttle": float(throttle_in),
            "brake": float(brake_in),
        }
        if not self.terminal_stop_hold_enabled:
            self._terminal_stop_hold_active = False
            return result

        if float(brake_in) >= self.terminal_stop_hold_brake_trigger:
            self._terminal_stop_hold_last_brake_ts = float(ts_sec)
        lon = _safe_float(gap.get("lon_m"), float("inf"))
        lat = abs(_safe_float(gap.get("lat_m"), float("inf")))
        gap_valid = (
            self._front_obstacle_visible
            and math.isfinite(lon)
            and math.isfinite(lat)
            and lon >= 0.0
            and lat <= self.terminal_stop_hold_max_lateral_gap_m
        )
        recent_brake = (
            self._terminal_stop_hold_last_brake_ts > 0.0
            and (float(ts_sec) - self._terminal_stop_hold_last_brake_ts)
            <= self.terminal_stop_hold_recent_brake_window_sec
        )
        should_activate = (
            gap_valid
            and lon <= self.terminal_stop_hold_activate_gap_m
            and self._latest_speed_mps <= self.terminal_stop_hold_activate_speed_mps
            and (float(brake_in) >= self.terminal_stop_hold_brake_trigger or recent_brake)
        )
        if should_activate and not self._terminal_stop_hold_active:
            self._terminal_stop_hold_active = True
            self._terminal_stop_hold_started_ts = float(ts_sec)
            self._terminal_stop_hold_engaged_count += 1

        if self._terminal_stop_hold_active:
            held_for_sec = max(0.0, float(ts_sec) - self._terminal_stop_hold_started_ts)
            can_release = held_for_sec >= self.terminal_stop_hold_min_hold_sec
            should_release = (
                (not gap_valid)
                or lon >= self.terminal_stop_hold_release_gap_m
                or self._latest_speed_mps >= self.terminal_stop_hold_release_speed_mps
            )
            if can_release and should_release:
                self._terminal_stop_hold_active = False
            else:
                result.update(
                    {
                        "active": True,
                        "phase": "hold",
                        "throttle": 0.0,
                        "brake": max(float(brake_in), self.terminal_stop_hold_brake),
                    }
                )
                return result
        result["phase"] = "armed" if should_activate else "idle"
        return result

    def _extract_routing_anchor(
        self,
        snapshot: Dict[str, Any],
        *,
        x0: float,
        y0: float,
        yaw0: float,
    ) -> Optional[Dict[str, float]]:
        heading_x = math.cos(yaw0)
        heading_y = math.sin(yaw0)
        best: Optional[Dict[str, float]] = None
        best_dist = float("inf")
        max_anchor_dist = max(40.0, min(float(self.radius_m), 80.0))

        def consider(
            cls_name: str,
            obj_id: str,
            x: float,
            y: float,
            z: float,
        ) -> None:
            nonlocal best, best_dist
            cls_norm = (cls_name or "").lower()
            if cls_norm and "vehicle" not in cls_norm and "car" not in cls_norm:
                return
            dx = x - x0
            dy = y - y0
            lon = dx * heading_x + dy * heading_y
            lat = -dx * heading_y + dy * heading_x
            dist = math.hypot(dx, dy)
            if lon <= 5.0:
                return
            if abs(lat) > 4.0:
                return
            if dist > max_anchor_dist:
                return
            if dist >= best_dist:
                return
            best_dist = dist
            best = {
                "id": obj_id,
                "class": cls_name or "unknown",
                "x": x,
                "y": y,
                "z": z,
                "distance_m": dist,
                "lon_m": lon,
                "lat_m": lat,
            }

        # Prefer the explicit followstop front actor when it exists.
        if self.carla_feedback is not None:
            actor = self.carla_feedback.find_vehicle_by_roles(("front",))
            if actor is not None:
                try:
                    tr = actor.get_transform()
                    x, y, z = self.tf.apply_position(
                        float(tr.location.x),
                        float(tr.location.y),
                        float(tr.location.z),
                    )
                    consider("vehicle", str(getattr(actor, "id", "")), x, y, z)
                    if best is not None:
                        best["source"] = "carla_front_role"
                        return best
                except Exception:
                    pass

        raw_json = snapshot.get("objects_json")
        if raw_json:
            try:
                payload = json.loads(raw_json)
                for item in payload.get("objects", []):
                    pose = item.get("pose", {}) or {}
                    x, y, z = self.tf.apply_position(
                        self._coerce_float(pose.get("x", 0.0), 0.0, "objects_json.pose.x", "routing_anchor"),
                        self._coerce_float(pose.get("y", 0.0), 0.0, "objects_json.pose.y", "routing_anchor"),
                        self._coerce_float(pose.get("z", 0.0), 0.0, "objects_json.pose.z", "routing_anchor"),
                    )
                    consider(str(item.get("class", "unknown")), str(item.get("id", "")), x, y, z)
            except Exception:
                pass
        if best is not None:
            best.setdefault("source", "objects_json")
            return best

        dets = snapshot.get("objects3d")
        if dets is not None and hasattr(dets, "detections"):
            for det in list(dets.detections):
                center = getattr(getattr(det, "bbox", None), "center", None)
                pos = getattr(center, "position", None)
                if pos is None:
                    continue
                cls_name = "unknown"
                if len(getattr(det, "results", [])) > 0:
                    hyp = det.results[0]
                    if hasattr(hyp, "hypothesis") and hasattr(hyp.hypothesis, "class_id"):
                        cls_name = str(hyp.hypothesis.class_id)
                x, y, z = self.tf.apply_position(
                    self._coerce_float(getattr(pos, "x", 0.0), 0.0, "det.position.x", "routing_anchor"),
                    self._coerce_float(getattr(pos, "y", 0.0), 0.0, "det.position.y", "routing_anchor"),
                    self._coerce_float(getattr(pos, "z", 0.0), 0.0, "det.position.z", "routing_anchor"),
                )
                consider(cls_name, str(getattr(det, "id", "")), x, y, z)
        if best is not None:
            best.setdefault("source", "objects3d")
        return best

    def _snap_xy_to_lane(self, x: float, y: float) -> Tuple[float, float, Dict[str, Any]]:
        if not self.map_segments:
            return x, y, {"available": False, "applied": False, "reason": "no_map_segments"}
        metrics = _nearest_segment_metrics(x, y, self.map_segments)
        if metrics is None:
            return x, y, {"available": False, "applied": False, "reason": "no_segment_match"}
        snapped_x = float(metrics["proj_x"])
        snapped_y = float(metrics["proj_y"])
        return snapped_x, snapped_y, {
            "available": True,
            "applied": True,
            "distance_m": float(metrics["dist"]),
            "proj_x": snapped_x,
            "proj_y": snapped_y,
            "lane_yaw_deg": math.degrees(float(metrics["seg_yaw"])),
            "signed_e_y_m": float(metrics["signed_e_y"]),
        }

    def _update_steer_sign_auto_check(self, row: Dict[str, Any]) -> None:
        if not bool(row.get("lane_inside", False)):
            return
        e_y = self._coerce_float(row.get("e_y_m"), float("nan"), "row.e_y_m", "steer_sign_check")
        e_psi = self._coerce_float(row.get("e_psi_deg"), float("nan"), "row.e_psi_deg", "steer_sign_check")
        cmd_steer = self._coerce_float(row.get("commanded_steer"), 0.0, "row.commanded_steer", "steer_sign_check")
        if math.isnan(e_y) or math.isnan(e_psi):
            return
        if abs(e_y) >= 0.2 or abs(e_psi) >= 10.0 or abs(e_psi) <= 1.0:
            return
        expected = -1 if e_psi > 0.0 else 1
        actual = _sign(cmd_steer)
        if actual == 0:
            return
        sample = {
            "ts_sec": self._coerce_float(row.get("ts_sec"), time.time(), "row.ts_sec", "steer_sign_check"),
            "e_y_m": e_y,
            "e_psi_deg": e_psi,
            "expected_sign": expected,
            "actual_sign": actual,
            "match": bool(expected == actual),
            "current_steer_sign": self.steer_sign,
        }
        self._steer_sign_window.append(sample)
        while len(self._steer_sign_window) > self._steer_sign_window_size:
            self._steer_sign_window.popleft()
        total = len(self._steer_sign_window)
        if total < self._steer_sign_window_size:
            self.stats["steer_sign_auto_check"] = {
                "suggested": None,
                "confidence": 0.0,
                "samples": total,
                "applied": self._steer_sign_auto_applied,
            }
            return
        matches = sum(1 for item in self._steer_sign_window if item["match"])
        mismatches = total - matches
        agree_ratio = matches / float(total)
        mismatch_ratio = mismatches / float(total)
        suggested = self.steer_sign
        confidence = agree_ratio
        reason = "current_sign_consistent"
        if mismatch_ratio >= 0.8:
            suggested = -1.0 if self.steer_sign > 0 else 1.0
            confidence = mismatch_ratio
            reason = "flip_recommended"
        elif agree_ratio < 0.8:
            reason = "inconclusive"
        payload = {
            "suggested": suggested,
            "confidence": confidence,
            "samples": total,
            "matches": matches,
            "mismatches": mismatches,
            "current_steer_sign": self.steer_sign,
            "reason": reason,
            "applied": self._steer_sign_auto_applied,
        }
        self.stats["steer_sign_auto_check"] = payload
        self.steer_sign_suggestion_path.write_text(json.dumps(payload, indent=2))
        if self.auto_apply_steer_sign and not self._steer_sign_auto_applied and suggested != self.steer_sign:
            self.steer_sign = float(suggested)
            self._steer_sign_auto_applied = True
            payload["applied"] = True
            payload["applied_steer_sign"] = self.steer_sign
            self.stats["steer_sign_auto_check"] = payload
            self.steer_sign_suggestion_path.write_text(json.dumps(payload, indent=2))

    def _maybe_dump_control_anomaly(
        self,
        row: Dict[str, Any],
        snapshot: Dict[str, Any],
        measured: Dict[str, Any],
    ) -> None:
        lane_inside = bool(row.get("lane_inside", False))
        e_y = abs(self._coerce_float(row.get("e_y_m"), 0.0, "row.e_y_m", "control_anomaly"))
        e_psi = abs(self._coerce_float(row.get("e_psi_deg"), 0.0, "row.e_psi_deg", "control_anomaly"))
        steer_mag = abs(
            self._coerce_float(
                row.get("commanded_steer"),
                0.0,
                "row.commanded_steer",
                "control_anomaly",
            )
        )
        if lane_inside and e_y < 0.1 and e_psi < 8.0 and steer_mag > 0.95:
            self._control_anomaly_counter += 1
        else:
            self._control_anomaly_counter = 0
            self.stats["control_anomaly"] = {"active": False, "count": 0}
            return
        self.stats["control_anomaly"] = {"active": True, "count": self._control_anomaly_counter}
        if self._control_anomaly_counter < 20:
            return
        ts_sec = int(self._coerce_float(row.get("ts_sec"), time.time(), "row.ts_sec", "control_anomaly"))
        key = str(ts_sec)
        if key == self._last_control_anomaly_key:
            return
        self._last_control_anomaly_key = key
        payload = {
            "ts_sec": row.get("ts_sec"),
            "row": row,
            "last_control_raw": self.stats.get("last_control_raw", {}),
            "last_control_in": self.stats.get("last_control_in", {}),
            "last_control_out": self.stats.get("last_control_out", {}),
            "last_measured_control": measured,
            "last_pose_debug": self.stats.get("last_pose_debug", {}),
            "routing": {
                "established": self.auto_routing_established,
                "routing_last_road_count": self.stats.get("routing_last_road_count", 0),
                "lane_follow_count": self.stats.get("lane_follow_count", 0),
            },
            "ros_input_counts": self.stats.get("ros_input_counts", {}),
            "last_obstacles_count": self.stats.get("last_obstacles_count", 0),
        }
        out = self.control_anomaly_path_prefix.with_name(f"{self.control_anomaly_path_prefix.name}_{key}.json")
        out.write_text(json.dumps(payload, indent=2))

    def _write_debug_row(self, row: Dict[str, Any]) -> None:
        self.debug_csv_path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = list(row.keys())
        mode = "a" if self._debug_csv_header_written and self.debug_csv_path.exists() else "w"
        with self.debug_csv_path.open(mode, newline="") as fp:
            writer = csv.DictWriter(fp, fieldnames=fieldnames)
            if not self._debug_csv_header_written or mode == "w":
                writer.writeheader()
                self._debug_csv_header_written = True
            writer.writerow(row)

    def _maybe_dump_saturation_snapshot(self, row: Dict[str, Any]) -> None:
        ts_sec = float(row.get("ts_sec", time.time()))
        self._debug_window.append(dict(row))
        while self._debug_window and (ts_sec - float(self._debug_window[0].get("ts_sec", ts_sec))) > self._debug_window_sec:
            self._debug_window.popleft()
        steer_mag = abs(
            _safe_float(
                row.get(
                    "apollo_desired_steer",
                    row.get("commanded_steer", 0.0),
                ),
                0.0,
            )
        )
        if steer_mag > 0.95:
            self._steer_sat_counter += 1
        else:
            self._steer_sat_counter = 0
            self._last_sat_snapshot_key = ""
            return
        if self._steer_sat_counter < 20:
            return
        key = str(int(ts_sec))
        if key == self._last_sat_snapshot_key:
            return
        self._last_sat_snapshot_key = key
        payload = {
            "reason_hint": "coordinate_misalignment"
            if abs(_safe_float(row.get("e_y_m", 0.0), 0.0)) > 1.5
            or abs(_safe_float(row.get("e_psi_deg", 0.0), 0.0)) > 20.0
            else "feedback_or_controller_saturation",
            "rows": list(self._debug_window),
        }
        out = self.artifacts_dir / f"steer_saturation_snapshot_{key}.json"
        out.write_text(json.dumps(payload, indent=2))

    def _odom_to_loc(self, odom: Odometry):
        loc = self.localization_pb2.LocalizationEstimate()
        header_ts_sec = self._command_now_sec()
        self._fill_header(
            getattr(loc, "header", None), header_ts_sec, "tb_apollo10_gt_bridge"
        )

        pos = odom.pose.pose.position
        ori = odom.pose.pose.orientation
        vel = odom.twist.twist.linear
        ang = odom.twist.twist.angular
        pose_info = self._transform_pose(pos, ori)
        self._maybe_auto_calibrate(
            pose_info["raw_x"],
            pose_info["raw_y"],
            pose_info["raw_yaw"],
            pose_info["map_x"],
            pose_info["map_y"],
            pose_info["map_yaw"],
        )
        if self._auto_calib_applied:
            pose_info = self._transform_pose(pos, ori)
        x = pose_info["map_x"]
        y = pose_info["map_y"]
        z = pose_info["map_z"]
        vx, vy, vz = self.tf.apply_vector(float(vel.x), float(vel.y), float(vel.z))
        wz = float(ang.z)
        yaw_ap = pose_info["map_yaw"]
        c_yaw = math.cos(yaw_ap)
        s_yaw = math.sin(yaw_ap)
        vx_vrf = c_yaw * vx + s_yaw * vy
        vy_vrf = -s_yaw * vx + c_yaw * vy
        qx, qy, qz, qw = _yaw_to_quat(yaw_ap)

        pose = getattr(loc, "pose", None)
        if pose is not None:
            if hasattr(pose, "position"):
                _safe_set(pose.position, "x", x)
                _safe_set(pose.position, "y", y)
                _safe_set(pose.position, "z", z)
            if hasattr(pose, "orientation"):
                _safe_set(pose.orientation, "qx", qx)
                _safe_set(pose.orientation, "qy", qy)
                _safe_set(pose.orientation, "qz", qz)
                _safe_set(pose.orientation, "qw", qw)
            if hasattr(pose, "heading"):
                _safe_set(pose, "heading", yaw_ap)
            if hasattr(pose, "linear_velocity"):
                _safe_set(pose.linear_velocity, "x", vx)
                _safe_set(pose.linear_velocity, "y", vy)
                _safe_set(pose.linear_velocity, "z", vz)
            if hasattr(pose, "linear_velocity_vrf"):
                _safe_set(pose.linear_velocity_vrf, "x", vx_vrf)
                _safe_set(pose.linear_velocity_vrf, "y", vy_vrf)
                _safe_set(pose.linear_velocity_vrf, "z", vz)
            if hasattr(pose, "angular_velocity"):
                _safe_set(pose.angular_velocity, "x", 0.0)
                _safe_set(pose.angular_velocity, "y", 0.0)
                _safe_set(pose.angular_velocity, "z", wz)
            if hasattr(pose, "angular_velocity_vrf"):
                _safe_set(pose.angular_velocity_vrf, "x", 0.0)
                _safe_set(pose.angular_velocity_vrf, "y", 0.0)
                _safe_set(pose.angular_velocity_vrf, "z", wz)
            if hasattr(pose, "linear_acceleration"):
                _safe_set(pose.linear_acceleration, "x", 0.0)
                _safe_set(pose.linear_acceleration, "y", 0.0)
                _safe_set(pose.linear_acceleration, "z", 0.0)
            if hasattr(pose, "linear_acceleration_vrf"):
                _safe_set(pose.linear_acceleration_vrf, "x", 0.0)
                _safe_set(pose.linear_acceleration_vrf, "y", 0.0)
                _safe_set(pose.linear_acceleration_vrf, "z", 0.0)
        # Apollo vehicle state freshness checks use measurement_time/header against cyber clock.
        _safe_set(loc, "measurement_time", header_ts_sec)
        pose_debug = self._pose_diagnostics(pose_info, header_ts_sec)
        return loc, header_ts_sec, (vx, vy, vz), pose_debug, pose_info

    def _odom_to_chassis(
        self,
        ts_sec: float,
        vel_xyz: Tuple[float, float, float],
        measured_control: Optional[Dict[str, Any]],
    ):
        ch = self.chassis_pb2.Chassis()
        self._fill_header(getattr(ch, "header", None), ts_sec, "tb_apollo10_gt_bridge")
        vx, vy, vz = vel_xyz
        speed = math.sqrt(vx * vx + vy * vy + vz * vz)
        desired = self.stats.get("last_control_out", {}) or {}
        throttle_cmd_pct = float(desired.get("throttle", 0.0)) * 100.0
        brake_cmd_pct = float(desired.get("brake", 0.0)) * 100.0
        steer_cmd_pct = float(desired.get("steer", 0.0)) * 100.0
        measured = measured_control or {}
        throttle_pct = (
            float(measured.get("throttle", 0.0)) * 100.0
            if measured.get("available")
            else 0.0
        )
        brake_pct = (
            float(measured.get("brake", 0.0)) * 100.0
            if measured.get("available")
            else 0.0
        )
        if measured.get("available") and measured.get("steer_feedback_pct") is not None:
            steer_pct = float(measured.get("steer_feedback_pct", 0.0))
        else:
            steer_pct = (
                float(measured.get("steer", 0.0)) * 100.0
                if measured.get("available")
                else 0.0
            )
        self.stats["last_control_feedback"] = {
            "desired": {
                "throttle_pct": throttle_cmd_pct,
                "brake_pct": brake_cmd_pct,
                "steer_pct": steer_cmd_pct,
            },
            "measured": {
                "throttle_pct": throttle_pct,
                "brake_pct": brake_pct,
                "steer_pct": steer_pct,
                "available": bool(measured.get("available", False)),
                "source": measured.get("source", "unavailable"),
                "steer_angle_deg": measured.get("steer_feedback_deg"),
                "speed_mps": measured.get("speed_mps"),
                "accel_mps2": measured.get("accel_mps2"),
                "forward_accel_mps2": measured.get("forward_accel_mps2"),
            },
        }
        _safe_set(ch, "engine_started", True)
        _safe_set(ch, "speed_mps", speed)
        _safe_set(ch, "throttle_percentage", throttle_pct)
        _safe_set(ch, "brake_percentage", brake_pct)
        _safe_set(ch, "steering_percentage", steer_pct)
        _safe_set(ch, "throttle_percentage_cmd", throttle_cmd_pct)
        _safe_set(ch, "brake_percentage_cmd", brake_cmd_pct)
        _safe_set(ch, "steering_percentage_cmd", steer_cmd_pct)
        _safe_set(ch, "parking_brake", False)
        if hasattr(ch.__class__, "COMPLETE_AUTO_DRIVE") and hasattr(ch, "driving_mode"):
            _safe_set(ch, "driving_mode", getattr(ch.__class__, "COMPLETE_AUTO_DRIVE"))
        if hasattr(ch.__class__, "GEAR_DRIVE") and hasattr(ch, "gear_location"):
            _safe_set(ch, "gear_location", getattr(ch.__class__, "GEAR_DRIVE"))
        return ch

    def _class_to_apollo_type(self, class_name: str) -> int:
        cls = (class_name or "").lower()
        pb = self.perception_pb2.PerceptionObstacle
        if "pedestrian" in cls or "walker" in cls:
            return int(getattr(pb, "PEDESTRIAN", getattr(pb, "UNKNOWN", 0)))
        if "bike" in cls or "bicycle" in cls:
            return int(getattr(pb, "BICYCLE", getattr(pb, "UNKNOWN", 0)))
        if "vehicle" in cls or "car" in cls:
            return int(getattr(pb, "VEHICLE", getattr(pb, "UNKNOWN", 0)))
        return int(getattr(pb, "UNKNOWN", 0))

    def _append_obstacle(
        self,
        arr: Any,
        *,
        ts_sec: float,
        obj_id: int,
        cls_name: str,
        x: float,
        y: float,
        z: float,
        yaw: float,
        vx: float,
        vy: float,
        vz: float,
        sx: float,
        sy: float,
        sz: float,
    ) -> None:
        obs = arr.add()
        _safe_set(obs, "id", int(obj_id))
        if hasattr(obs, "position"):
            _safe_set(obs.position, "x", x)
            _safe_set(obs.position, "y", y)
            _safe_set(obs.position, "z", z)
        if hasattr(obs, "velocity"):
            _safe_set(obs.velocity, "x", vx)
            _safe_set(obs.velocity, "y", vy)
            _safe_set(obs.velocity, "z", vz)
        _safe_set(obs, "theta", yaw)
        _safe_set(obs, "length", float(sx))
        _safe_set(obs, "width", float(sy))
        _safe_set(obs, "height", float(sz))
        _safe_set(obs, "type", self._class_to_apollo_type(cls_name))
        _safe_set(obs, "timestamp", ts_sec)
        _safe_set(obs, "tracking_time", 0.1)
        _safe_set(obs, "confidence", 1.0)
        if hasattr(obs, "polygon_point"):
            half_x = max(float(sx) * 0.5, 0.05)
            half_y = max(float(sy) * 0.5, 0.05)
            cos_yaw = math.cos(yaw)
            sin_yaw = math.sin(yaw)
            for dx, dy in ((half_x, half_y), (half_x, -half_y), (-half_x, -half_y), (-half_x, half_y)):
                pt = obs.polygon_point.add()
                _safe_set(pt, "x", x + dx * cos_yaw - dy * sin_yaw)
                _safe_set(pt, "y", y + dx * sin_yaw + dy * cos_yaw)
                _safe_set(pt, "z", z)

    def _store_obstacle_cache(self, msg: Any, count: int, ts_sec: float) -> None:
        if (not self.front_obstacle_cache_enabled) or count <= 0:
            return
        try:
            self._obstacle_cache_payload = msg.SerializeToString()
            self._obstacle_cache_count = int(count)
            self._obstacle_cache_ts_sec = float(ts_sec)
        except Exception:
            self._obstacle_cache_payload = None
            self._obstacle_cache_count = 0
            self._obstacle_cache_ts_sec = 0.0

    def _reuse_cached_obstacles(self, ts_sec: float, *, reason: str) -> Optional[Tuple[Any, int]]:
        if not self.front_obstacle_cache_enabled:
            return None
        if not self._obstacle_cache_payload or self._obstacle_cache_count <= 0:
            return None
        age_sec = max(0.0, float(ts_sec) - float(self._obstacle_cache_ts_sec))
        if age_sec > max(self.front_obstacle_cache_ttl_sec, 0.0):
            return None
        try:
            msg = self.perception_pb2.PerceptionObstacles()
            msg.ParseFromString(self._obstacle_cache_payload)
            self._fill_header(getattr(msg, "header", None), ts_sec, "tb_apollo10_gt_bridge")
            arr = getattr(msg, "perception_obstacle", None)
            if arr is not None:
                for obs in list(arr):
                    _safe_set(obs, "timestamp", ts_sec)
                    if hasattr(obs, "tracking_time"):
                        _safe_set(obs, "tracking_time", max(0.1, age_sec))
            self._obstacle_cache_hit_count += 1
            self._obstacle_cache_last_hit_ts_sec = float(ts_sec)
            self.stats["front_obstacle_behavior"] = self._front_obstacle_behavior_status()
            if self.debug_dump_control_raw and (
                self._obstacle_cache_hit_count <= 5 or (self._obstacle_cache_hit_count % 20) == 0
            ):
                print(
                    "[bridge][obstacle_cache] reuse reason=%s age=%.3fs count=%d hits=%d"
                    % (reason, age_sec, self._obstacle_cache_count, self._obstacle_cache_hit_count)
                )
            return msg, int(self._obstacle_cache_count)
        except Exception:
            return None

    def _objects_to_obstacles(self, snapshot: Dict[str, Any], ts_sec: float, ego_xy: Tuple[float, float]):
        msg = self.perception_pb2.PerceptionObstacles()
        self._fill_header(getattr(msg, "header", None), ts_sec, "tb_apollo10_gt_bridge")
        out = getattr(msg, "perception_obstacle", None)
        if out is None:
            return msg, 0
        if not self._front_obstacle_visible_now():
            self._front_obstacle_suppressed_frames += 1
            return msg, 0
        count = 0

        dets = snapshot.get("objects3d")
        if dets is not None and hasattr(dets, "detections"):
            for det in list(dets.detections):
                if count >= self.max_obstacles:
                    break
                center = getattr(det.bbox, "center", None)
                if center is None:
                    continue
                p = getattr(center, "position", None)
                o = getattr(center, "orientation", None)
                if p is None or o is None:
                    continue
                yaw = _quat_to_yaw(float(o.x), float(o.y), float(o.z), float(o.w))
                x, y, z = self.tf.apply_position(float(p.x), float(p.y), float(p.z))
                sx = float(getattr(det.bbox.size, "x", 1.0))
                sy = float(getattr(det.bbox.size, "y", 1.0))
                sz = float(getattr(det.bbox.size, "z", 1.0))
                cls = "unknown"
                if len(det.results) > 0:
                    hyp = det.results[0]
                    if hasattr(hyp, "hypothesis") and hasattr(hyp.hypothesis, "class_id"):
                        cls = str(hyp.hypothesis.class_id)
                obj_id = int(getattr(det, "id", count + 1) or (count + 1))
                self._append_obstacle(
                    out,
                    ts_sec=ts_sec,
                    obj_id=obj_id,
                    cls_name=cls,
                    x=x,
                    y=y,
                    z=z,
                    yaw=self.tf.apply_yaw(yaw),
                    vx=0.0,
                    vy=0.0,
                    vz=0.0,
                    sx=sx,
                    sy=sy,
                    sz=sz,
                )
                count += 1
            if count > 0:
                self._store_obstacle_cache(msg, count, ts_sec)
                return msg, count

        raw_json = snapshot.get("objects_json")
        if raw_json:
            try:
                payload = json.loads(raw_json)
                for item in payload.get("objects", []):
                    if count >= self.max_obstacles:
                        break
                    pose = item.get("pose", {}) or {}
                    size = item.get("size", {}) or {}
                    x, y, z = self.tf.apply_position(
                        float(pose.get("x", 0.0)),
                        float(pose.get("y", 0.0)),
                        float(pose.get("z", 0.0)),
                    )
                    velocity = item.get("velocity", {}) or item.get("twist", {}) or {}
                    vx, vy, vz = self.tf.apply_vector(
                        float(velocity.get("x", 0.0)),
                        float(velocity.get("y", 0.0)),
                        float(velocity.get("z", 0.0)),
                    )
                    self._append_obstacle(
                        out,
                        ts_sec=ts_sec,
                        obj_id=int(item.get("id", count + 1)),
                        cls_name=str(item.get("class", "unknown")),
                        x=x,
                        y=y,
                        z=z,
                        yaw=self.tf.apply_yaw(float(pose.get("yaw", 0.0))),
                        vx=vx,
                        vy=vy,
                        vz=vz,
                        sx=float(size.get("x", 1.0)),
                        sy=float(size.get("y", 1.0)),
                        sz=float(size.get("z", 1.0)),
                    )
                    count += 1
            except Exception as exc:
                self.stats["last_error"] = f"objects_json parse error: {exc}"
            if count > 0:
                self._store_obstacle_cache(msg, count, ts_sec)
                return msg, count

        markers = snapshot.get("markers")
        if markers is not None and hasattr(markers, "markers"):
            for marker in list(markers.markers):
                if count >= self.max_obstacles:
                    break
                if int(getattr(marker, "action", 0)) != 0:  # skip DELETE/DELETEALL
                    continue
                pose = getattr(marker, "pose", None)
                if pose is None:
                    continue
                p = getattr(pose, "position", None)
                o = getattr(pose, "orientation", None)
                if p is None or o is None:
                    continue
                yaw = _quat_to_yaw(float(o.x), float(o.y), float(o.z), float(o.w))
                x, y, z = self.tf.apply_position(float(p.x), float(p.y), float(p.z))
                cls = str(getattr(marker, "ns", "unknown"))
                self._append_obstacle(
                    out,
                    ts_sec=ts_sec,
                    obj_id=int(getattr(marker, "id", count + 1)),
                    cls_name=cls,
                    x=x,
                    y=y,
                    z=z,
                    yaw=self.tf.apply_yaw(yaw),
                    vx=0.0,
                    vy=0.0,
                    vz=0.0,
                    sx=float(getattr(marker.scale, "x", 1.0)),
                    sy=float(getattr(marker.scale, "y", 1.0)),
                    sz=float(getattr(marker.scale, "z", 1.0)),
                )
                count += 1
        if count > 0:
            self._store_obstacle_cache(msg, count, ts_sec)
            return msg, count
        cached = self._reuse_cached_obstacles(ts_sec, reason="source_empty")
        if cached is not None:
            return cached
        return msg, count

    def _on_control_cmd(self, cmd: Any) -> None:
        self.stats["control_rx_count"] += 1
        raw_fields = self._extract_raw_control_fields(cmd)
        throttle_pct = self._coerce_float(raw_fields.get("throttle", 0.0), 0.0, "throttle", "apollo.control")
        brake_pct = self._coerce_float(raw_fields.get("brake", 0.0), 0.0, "brake", "apollo.control")
        steer_pct = 0.0
        steer_source = "missing"
        for key in ("steering_target", "steering_percentage", "steering", "steering_rate"):
            if key in raw_fields:
                steer_pct = self._coerce_float(raw_fields.get(key), 0.0, key, "apollo.control")
                steer_source = key
                break
        raw_throttle = _clamp(throttle_pct / 100.0, 0.0, 1.0)
        raw_brake = _clamp(brake_pct / 100.0, 0.0, 1.0)
        raw_steer = _clamp(steer_pct / 100.0, -1.0, 1.0)
        self.last_control = {"throttle": raw_throttle, "brake": raw_brake, "steer_pct": raw_steer}
        estop = False
        for key in ("is_in_safe_mode", "estop"):
            if key in raw_fields:
                estop = bool(raw_fields.get(key))
                break
        mode = str(raw_fields.get("driving_mode", ""))
        gear = str(raw_fields.get("gear_location", ""))
        self.stats["last_control_in"] = {
            "throttle": raw_throttle,
            "brake": raw_brake,
            "steer": raw_steer,
            "mode": mode,
            "gear": gear,
            "estop": estop,
            "steer_source": steer_source,
            "raw_steer_value": steer_pct,
        }
        throttle = _clamp(raw_throttle * self.throttle_scale, 0.0, 1.0)
        brake_before_deadzone = _clamp(raw_brake * self.brake_scale, 0.0, 1.0)
        brake = 0.0 if brake_before_deadzone < self.brake_deadzone else brake_before_deadzone
        now_sec = time.time()
        if throttle > 0.01:
            self._last_nonzero_throttle = throttle
            self._last_nonzero_ts = now_sec
        elif (
            self.zero_hold_sec > 0.0
            and brake <= 0.0
            and self._last_nonzero_throttle > 0.0
            and (now_sec - self._last_nonzero_ts) <= self.zero_hold_sec
        ):
            throttle = self._last_nonzero_throttle
        steer_pre = raw_steer * self.steer_sign * self.steer_scale
        steer = _clamp(steer_pre, -1.0, 1.0)
        pose_debug = self.stats.get("last_pose_debug", {}) or {}
        e_y_abs = abs(_safe_float(pose_debug.get("e_y_m"), float("inf")))
        e_psi_abs = abs(_wrap_deg(_safe_float(pose_debug.get("e_psi_deg"), float("inf"))))
        target_curvature_abs = abs(_safe_float(pose_debug.get("target_curvature"), float("inf")))
        straight_lane_zero_steer_applied = False
        low_speed_steer_guard_applied = False
        force_zero_steer_applied = False
        straight_lane_enter_ok = (
            self.straight_lane_zero_steer_enabled
            and self._latest_speed_mps <= self.straight_lane_zero_steer_max_speed_mps
            and target_curvature_abs <= self.straight_lane_zero_steer_max_curvature
            and e_y_abs <= self.straight_lane_zero_steer_max_e_y_m
            and e_psi_abs <= self.straight_lane_zero_steer_max_e_psi_deg
        )
        release_ignore_e_psi = (
            self.straight_lane_zero_steer_release_ignore_e_psi_below_speed_mps > 0.0
            and self._latest_speed_mps <= self.straight_lane_zero_steer_release_ignore_e_psi_below_speed_mps
        )
        straight_lane_hold_ok = (
            self.straight_lane_zero_steer_enabled
            and self._latest_speed_mps <= self.straight_lane_zero_steer_max_speed_mps
            and target_curvature_abs <= self.straight_lane_zero_steer_max_curvature
            and e_y_abs <= self.straight_lane_zero_steer_release_max_e_y_m
            and (release_ignore_e_psi or e_psi_abs <= self.straight_lane_zero_steer_release_max_e_psi_deg)
        )
        if self.straight_lane_zero_steer_latch_enabled:
            self._straight_lane_zero_steer_active = (
                straight_lane_hold_ok and (self._straight_lane_zero_steer_active or straight_lane_enter_ok)
            )
        else:
            self._straight_lane_zero_steer_active = straight_lane_enter_ok
        if self._straight_lane_zero_steer_active:
            steer = 0.0
            straight_lane_zero_steer_applied = True
        elif (
            self.low_speed_steer_guard_enabled
            and self._latest_speed_mps <= self.low_speed_steer_guard_speed_mps
            and abs(raw_steer) >= 0.95
            and e_y_abs <= self.low_speed_steer_guard_max_e_y_m
            and e_psi_abs <= self.low_speed_steer_guard_max_e_psi_deg
        ):
            steer = _clamp(steer, -self.low_speed_steer_guard_max_abs_steer, self.low_speed_steer_guard_max_abs_steer)
            low_speed_steer_guard_applied = True
        if self.force_zero_steer_output:
            force_zero_steer_applied = abs(steer) > 1e-6
            steer = 0.0
        throttle_cmd = throttle
        brake_cmd = brake
        longitudinal_override = self._compute_straight_acc_override(
            throttle_in=throttle_cmd,
            brake_in=brake_cmd,
            target_curvature_abs=target_curvature_abs,
            e_y_abs=e_y_abs,
        )
        if longitudinal_override.get("active"):
            throttle_cmd = _clamp(
                float(longitudinal_override.get("throttle", throttle_cmd)), 0.0, 1.0
            )
            brake_cmd = _clamp(float(longitudinal_override.get("brake", brake_cmd)), 0.0, 1.0)
        terminal_stop_hold = self._compute_terminal_stop_hold(
            throttle_in=throttle_cmd,
            brake_in=brake_cmd,
            ts_sec=now_sec,
        )
        if terminal_stop_hold.get("active"):
            throttle_cmd = _clamp(float(terminal_stop_hold.get("throttle", 0.0)), 0.0, 1.0)
            brake_cmd = _clamp(float(terminal_stop_hold.get("brake", brake_cmd)), 0.0, 1.0)
        throttle_after_boost = throttle_cmd
        startup_boost_window = (
            self._first_odom_ts is not None and (now_sec - self._first_odom_ts) <= 2.0
        ) or self._latest_speed_mps < 0.2
        if (
            self.startup_throttle_boost_enabled
            and startup_boost_window
            and throttle_after_boost > 0.0
            and brake_cmd <= 0.0
        ):
            # Boost must never reduce Apollo throttle. `startup_throttle_boost_cap`
            # is treated as a boosted upper target, not a hard global clamp.
            boosted_target = throttle_after_boost + self.startup_throttle_boost_add
            if self.startup_throttle_boost_cap > 0.0:
                boosted_target = min(self.startup_throttle_boost_cap, boosted_target)
            throttle_after_boost = max(
                throttle_after_boost,
                _clamp(boosted_target, 0.0, 1.0),
            )
        self.stats["last_control_out"] = {
            "throttle": throttle_after_boost,
            "brake": brake_cmd,
            "steer": steer,
            "type": self.node.control_out_type,
            "steer_pre_clamp": steer_pre,
            "steer_clamped": abs(steer_pre) > 1.0,
            "steer_sign": self.steer_sign,
            "throttle_before_boost": throttle_cmd,
            "throttle_after_boost": throttle_after_boost,
            "startup_boost_applied": throttle_after_boost > throttle_cmd,
            "brake_before_deadzone": brake_before_deadzone,
            "brake_after_deadzone": brake_cmd,
            "straight_lane_zero_steer_applied": straight_lane_zero_steer_applied,
            "low_speed_steer_guard_applied": low_speed_steer_guard_applied,
            "force_zero_steer_output": self.force_zero_steer_output,
            "force_zero_steer_applied": force_zero_steer_applied,
            "longitudinal_override_enabled": longitudinal_override.get("enabled", False),
            "longitudinal_override_applied": longitudinal_override.get("active", False),
            "longitudinal_override_mode": longitudinal_override.get("mode", ""),
            "longitudinal_override_phase": longitudinal_override.get("phase", ""),
            "longitudinal_target_speed_mps": longitudinal_override.get("target_speed_mps"),
            "longitudinal_speed_error_mps": longitudinal_override.get("speed_error_mps"),
            "longitudinal_front_gap_lon_m": longitudinal_override.get("front_gap_lon_m"),
            "longitudinal_front_gap_lat_m": longitudinal_override.get("front_gap_lat_m"),
            "longitudinal_front_gap_distance_m": longitudinal_override.get("front_gap_distance_m"),
            "terminal_stop_hold_enabled": terminal_stop_hold.get("enabled", False),
            "terminal_stop_hold_active": terminal_stop_hold.get("active", False),
            "terminal_stop_hold_phase": terminal_stop_hold.get("phase", ""),
            "terminal_stop_hold_front_gap_lon_m": terminal_stop_hold.get("front_gap_lon_m"),
            "terminal_stop_hold_front_gap_lat_m": terminal_stop_hold.get("front_gap_lat_m"),
            "terminal_stop_hold_front_gap_distance_m": terminal_stop_hold.get("front_gap_distance_m"),
        }
        control_ts = self._command_now_sec()
        raw_dump = {
            "ts_sec": control_ts,
            "raw_control_msg_dump": raw_fields,
            "parsed_control": dict(self.stats["last_control_in"]),
            "output_to_carla": dict(self.stats["last_control_out"]),
        }
        self.stats["last_control_raw"] = raw_dump
        if self.debug_dump_control_raw:
            self._append_jsonl(self.control_decode_dump_path, raw_dump)
        if (time.time() - self._last_control_print_sec) >= 1.0 and self.debug_dump_control_raw:
            print(
                "[bridge][control_raw] steer_src=%s raw_steer=%s parsed(%.3f,%.3f,%.3f) out(%.3f,%.3f,%.3f)"
                % (
                    steer_source,
                    raw_fields.get(steer_source, None),
                    raw_throttle,
                    raw_brake,
                    raw_steer,
                    throttle_after_boost,
                    brake,
                    steer,
                )
            )
        if self.node.control_out_type == "ackermann":
            speed_cmd = (
                -brake_cmd * self.brake_gain
                if (brake_cmd > throttle_after_boost and brake_cmd > 0.01)
                else throttle_after_boost * self.speed_gain
            )
            msg = AckermannDriveStamped()
            msg.header.stamp = self.node.get_clock().now().to_msg()
            msg.header.frame_id = "base_link"
            msg.drive.speed = speed_cmd
            msg.drive.acceleration = 0.0
            msg.drive.steering_angle = steer * self.max_steer_angle
        elif self.node.control_out_type == "direct":
            msg = Float32MultiArray()
            msg.data = [float(throttle_after_boost), float(brake_cmd), float(steer)]
        else:
            speed_cmd = (
                -brake_cmd * self.brake_gain
                if (brake_cmd > throttle_after_boost and brake_cmd > 0.01)
                else throttle_after_boost * self.speed_gain
            )
            msg = Twist()
            msg.linear.x = speed_cmd
            msg.angular.z = steer
        self.node.publish_control(msg)
        self.stats["control_tx_count"] += 1

    def _on_routing_response(self, msg: Any) -> None:
        self.stats["routing_response_count"] += 1
        roads = getattr(msg, "road", None)
        road_count = len(roads) if roads is not None else 0
        self.stats["routing_last_road_count"] = int(road_count)
        if road_count > 0:
            self.stats["routing_success_count"] += 1
            self.auto_routing_established = True
            if self.auto_routing_pending_goal is not None:
                self.auto_routing_active_goal = self.auto_routing_pending_goal
        else:
            self.stats["routing_empty_count"] += 1

    def _resolve_map_file(self) -> Optional[Path]:
        if self.map_file_path:
            map_file = Path(self.map_file_path)
            if not map_file.is_absolute():
                map_file = (self.apollo_root / map_file).resolve()
            return map_file
        guessed = self._guess_map_file()
        if guessed is not None and guessed.exists():
            return guessed
        if guessed is not None and guessed.with_suffix(".xml").exists():
            return guessed.with_suffix(".xml")
        return guessed

    def _guess_map_file(self) -> Optional[Path]:
        flagfile = self.apollo_root / "modules" / "common" / "data" / "global_flagfile.txt"
        if not flagfile.exists():
            return None
        map_dir: Optional[str] = None
        try:
            for raw in flagfile.read_text(errors="ignore").splitlines():
                line = raw.strip()
                if line.startswith("--map_dir="):
                    map_dir = line.split("=", 1)[1].strip()
        except Exception:
            return None
        if not map_dir:
            return None
        return (self.apollo_root / map_dir / "base_map.txt").resolve()

    def _load_map_geometry(self) -> None:
        map_file = self._resolve_map_file()
        if map_file is None or not map_file.exists():
            self.stats["map_geometry"] = {
                "enabled": False,
                "reason": "map_file_missing",
                "map_file": str(map_file) if map_file is not None else "",
            }
            return
        try:
            points = _extract_xy_series(map_file)
            segments = _build_map_segments(points)
            self.map_segments = segments
            self.stats["map_geometry"] = {
                "enabled": bool(segments),
                "map_file": str(map_file),
                "points": len(points),
                "segments": len(segments),
            }
        except Exception as exc:
            self.map_segments = []
            self.stats["map_geometry"] = {
                "enabled": False,
                "reason": f"parse_error:{exc}",
                "map_file": str(map_file),
            }

    def _load_map_bounds(self, auto_routing_cfg: Dict[str, Any]) -> None:
        if not self.auto_routing_clamp_to_map_bounds:
            self.stats["map_bounds"] = {"enabled": False, "reason": "disabled"}
            return
        if self.map_bounds_file:
            bounds_file = Path(self.map_bounds_file)
            if not bounds_file.is_absolute():
                bounds_file = (Path.cwd() / bounds_file).resolve()
            if bounds_file.exists():
                try:
                    payload = json.loads(bounds_file.read_text())
                    x_min = float(payload["min_x"])
                    x_max = float(payload["max_x"])
                    y_min = float(payload["min_y"])
                    y_max = float(payload["max_y"])
                    self.map_bounds_xy = (x_min, x_max, y_min, y_max)
                    self.stats["map_bounds"] = {
                        "enabled": True,
                        "reason": "",
                        "map_bounds_file": str(bounds_file),
                        "x_min": x_min,
                        "x_max": x_max,
                        "y_min": y_min,
                        "y_max": y_max,
                        "margin_m": self.auto_routing_map_bounds_margin_m,
                    }
                    return
                except Exception as exc:
                    self.stats["map_bounds"] = {
                        "enabled": False,
                        "reason": f"parse_error:{exc}",
                        "map_bounds_file": str(bounds_file),
                    }
                    return
            self.stats["map_bounds"] = {
                "enabled": False,
                "reason": "map_file_missing",
                "map_bounds_file": str(bounds_file),
            }
            return
        map_file = self._resolve_map_file()
        if map_file is None or not map_file.exists():
            self.stats["map_bounds"] = {"enabled": False, "reason": "map_file_missing"}
            return
        try:
            points = _extract_xy_series(map_file)
            x_vals = [p[0] for p in points]
            y_vals = [p[1] for p in points]
            if not x_vals or not y_vals:
                self.stats["map_bounds"] = {"enabled": False, "reason": "empty_xy", "map_file": str(map_file)}
                return
            x_min, x_max = min(x_vals), max(x_vals)
            y_min, y_max = min(y_vals), max(y_vals)
            self.map_bounds_xy = (x_min, x_max, y_min, y_max)
            self.stats["map_bounds"] = {
                "enabled": True,
                "map_file": str(map_file),
                "x_min": x_min,
                "x_max": x_max,
                "y_min": y_min,
                "y_max": y_max,
                "margin_m": self.auto_routing_map_bounds_margin_m,
            }
        except Exception as exc:
            self.stats["map_bounds"] = {"enabled": False, "reason": f"parse_error:{exc}", "map_file": str(map_file)}

    def _clamp_xy_to_bounds(self, x: float, y: float) -> Tuple[float, float]:
        if self.map_bounds_xy is None:
            return x, y
        x_min, x_max, y_min, y_max = self.map_bounds_xy
        margin = max(0.0, self.auto_routing_map_bounds_margin_m)
        x_lo, x_hi = x_min + margin, x_max - margin
        y_lo, y_hi = y_min + margin, y_max - margin
        if x_lo > x_hi:
            x_lo, x_hi = x_min, x_max
        if y_lo > y_hi:
            y_lo, y_hi = y_min, y_max
        return (_clamp(x, x_lo, x_hi), _clamp(y, y_lo, y_hi))

    def _maybe_auto_calibrate(
        self,
        raw_x: float,
        raw_y: float,
        raw_yaw: float,
        map_x: float,
        map_y: float,
        map_yaw: float,
    ) -> None:
        if not self.auto_calib_enabled or self._auto_calib_applied:
            return
        self._auto_calib_samples.append(
            {
                "raw_x": raw_x,
                "raw_y": raw_y,
                "raw_yaw": raw_yaw,
                "map_x": map_x,
                "map_y": map_y,
                "map_yaw": map_yaw,
            }
        )
        if len(self._auto_calib_samples) < self._auto_calib_sample_target:
            self.stats["auto_calib"]["collected"] = len(self._auto_calib_samples)
            return
        avg_raw_x = sum(s["raw_x"] for s in self._auto_calib_samples) / len(self._auto_calib_samples)
        avg_raw_y = sum(s["raw_y"] for s in self._auto_calib_samples) / len(self._auto_calib_samples)
        avg_raw_yaw = math.atan2(
            sum(math.sin(s["raw_yaw"]) for s in self._auto_calib_samples),
            sum(math.cos(s["raw_yaw"]) for s in self._auto_calib_samples),
        )
        target_x = None
        target_y = None
        target_yaw = self.tf.yaw_rad
        lane = _nearest_segment_metrics(
            self.tf.apply_position(avg_raw_x, avg_raw_y, 0.0)[0],
            self.tf.apply_position(avg_raw_x, avg_raw_y, 0.0)[1],
            self.map_segments,
        )
        if lane is not None:
            target_x = lane["proj_x"]
            target_y = lane["proj_y"]
            target_yaw = lane["seg_yaw"]
        # Do not calibrate against map bounds center: it can map ego onto a wrong lane
        # when lane geometry is unavailable (e.g., missing map_file), which breaks planning.
        if target_x is None or target_y is None:
            self.stats["auto_calib"]["reason"] = "no_lane_reference"
            self._auto_calib_applied = True
            return
        yaw_delta_deg = _rad_to_deg(target_yaw - avg_raw_yaw)
        if self.auto_calib_snap_right_angle:
            yaw_delta_deg = _quantize_right_angle_deg(yaw_delta_deg)
        yaw_delta = math.radians(yaw_delta_deg)
        c = math.cos(yaw_delta)
        s = math.sin(yaw_delta)
        rot_x = c * avg_raw_x - s * avg_raw_y
        rot_y = s * avg_raw_x + c * avg_raw_y
        self.tf.tx = target_x - rot_x
        self.tf.ty = target_y - rot_y
        self.tf.yaw_deg = yaw_delta_deg
        self._auto_calib_applied = True
        payload = {
            "suggested": {
                "tx": self.tf.tx,
                "ty": self.tf.ty,
                "tz": self.tf.tz,
                "yaw_deg": self.tf.yaw_deg,
            },
            "avg_raw_pose": {"x": avg_raw_x, "y": avg_raw_y, "yaw_deg": math.degrees(avg_raw_yaw)},
            "target_pose": {"x": target_x, "y": target_y, "yaw_deg": math.degrees(target_yaw)},
            "lane_available": lane is not None,
            "snap_right_angle": self.auto_calib_snap_right_angle,
        }
        dump_path = Path(self.auto_calib_dump_file)
        if not dump_path.is_absolute():
            dump_path = (Path.cwd() / dump_path).resolve()
        dump_path.parent.mkdir(parents=True, exist_ok=True)
        dump_path.write_text(json.dumps(payload, indent=2))
        self.stats["auto_calib"] = {
            "enabled": True,
            "applied": True,
            "collected": len(self._auto_calib_samples),
            **payload["suggested"],
            "dump_file": str(dump_path),
        }

    def _current_routing_phase(self, ts_sec: float, speed_mps: float) -> str:
        if not self.auto_routing_startup_routing_sent:
            return "startup"
        if self._first_odom_ts is None:
            return "startup"
        elapsed = max(0.0, ts_sec - self._first_odom_ts)
        moved = speed_mps >= self.auto_routing_startup_speed_threshold_mps
        in_startup = elapsed < self.auto_routing_startup_hold_sec
        if moved and self.auto_routing_use_long_goal_after_move:
            in_startup = False
        return "startup" if in_startup else "long"

    def _routing_end_point(
        self,
        x0: float,
        y0: float,
        z0: float,
        yaw0: float,
        *,
        dist_m: float,
        anchor: Optional[Dict[str, float]] = None,
        prefer_front_anchor: bool = False,
    ) -> Tuple[float, float, float, str]:
        if self.auto_routing_seed_pose is None:
            self.auto_routing_seed_pose = (x0, y0, yaw0)
        seed_x, seed_y, seed_yaw = self.auto_routing_seed_pose
        base_yaw = seed_yaw if self.auto_routing_use_seed_heading else yaw0
        base_x = seed_x if self.auto_routing_use_seed_heading else x0
        base_y = seed_y if self.auto_routing_use_seed_heading else y0
        base_z = z0
        anchor_mode = "ego_seed"
        if prefer_front_anchor and anchor is not None:
            base_x = float(anchor.get("x", x0))
            base_y = float(anchor.get("y", y0))
            base_z = float(anchor.get("z", z0))
            anchor_mode = "front_vehicle"
        x1 = base_x + dist_m * math.cos(base_yaw)
        y1 = base_y + dist_m * math.sin(base_yaw)
        x1, y1 = self._clamp_xy_to_bounds(x1, y1)
        return x1, y1, base_z, anchor_mode

    def _build_routing_goal(
        self,
        x0: float,
        y0: float,
        z0: float,
        yaw0: float,
        *,
        anchor: Optional[Dict[str, float]],
        phase: str,
        ignore_roll_active: bool = False,
    ) -> Tuple[float, float, float, Dict[str, Any]]:
        requested_mode = self.auto_routing_goal_mode
        use_front_anchor = requested_mode == "front_vehicle_ahead"
        startup_phase = phase == "startup"
        if ignore_roll_active:
            effective_dist = _clamp(
                float(self.traffic_light_ignore_roll_ahead_m),
                self.auto_routing_min_end_ahead_m,
                max(self.auto_routing_startup_end_ahead_m, self.traffic_light_ignore_roll_ahead_m),
            )
            x1 = x0 + effective_dist * math.cos(yaw0)
            y1 = y0 + effective_dist * math.sin(yaw0)
            x1, y1 = self._clamp_xy_to_bounds(x1, y1)
            return x1, y1, z0, {
                "requested_goal_mode": requested_mode,
                "goal_mode": "ego_seed_ahead",
                "goal_source": "traffic_light_ignore_roll",
                "startup_phase_used": True,
                "end_ahead_m_effective": effective_dist,
                "anchor_mode": "ego_current",
            }
        if startup_phase:
            startup_cap = max(self.auto_routing_min_end_ahead_m, self.auto_routing_startup_end_ahead_m)
            effective_dist = _clamp(
                float(self.auto_routing_end_ahead_m),
                self.auto_routing_min_end_ahead_m,
                startup_cap,
            )
            x1, y1, z1, anchor_mode = self._routing_end_point(
                x0,
                y0,
                z0,
                yaw0,
                dist_m=effective_dist,
                anchor=anchor,
                prefer_front_anchor=use_front_anchor,
            )
            goal_mode = "front_vehicle_ahead" if anchor_mode == "front_vehicle" else "ego_seed_ahead"
            return x1, y1, z1, {
                "requested_goal_mode": requested_mode,
                "goal_mode": goal_mode,
                "goal_source": "startup_short_ahead",
                "startup_phase_used": True,
                "end_ahead_m_effective": effective_dist,
                "anchor_mode": anchor_mode,
            }

        if requested_mode in {"fixed_xy", "scenario_xy"}:
            point = self.auto_routing_fixed_goal_xy if requested_mode == "fixed_xy" else self._load_scenario_goal_point()
            if point is not None:
                if point.get("relative_to") == "ego_heading" and point.get("goal_ahead_m") is not None:
                    effective_dist = max(self.auto_routing_min_end_ahead_m, float(point.get("goal_ahead_m")))
                    x1 = x0 + effective_dist * math.cos(yaw0)
                    y1 = y0 + effective_dist * math.sin(yaw0)
                    z1 = z0
                    goal_source = "scenario_goal_relative"
                    end_ahead = effective_dist
                else:
                    x1 = float(point["x"])
                    y1 = float(point["y"])
                    z1 = float(point.get("z", z0))
                    goal_source = "fixed_xy" if requested_mode == "fixed_xy" else "scenario_goal_file"
                    end_ahead = None
                x1, y1 = self._clamp_xy_to_bounds(x1, y1)
                return x1, y1, z1, {
                    "requested_goal_mode": requested_mode,
                    "goal_mode": requested_mode,
                    "goal_source": goal_source,
                    "startup_phase_used": False,
                    "end_ahead_m_effective": end_ahead,
                    "anchor_mode": "fixed_goal",
                }

        effective_dist = max(self.auto_routing_min_end_ahead_m, float(self.auto_routing_end_ahead_m))
        x1, y1, z1, anchor_mode = self._routing_end_point(
            x0,
            y0,
            z0,
            yaw0,
            dist_m=effective_dist,
            anchor=anchor,
            prefer_front_anchor=use_front_anchor,
        )
        goal_mode = "front_vehicle_ahead" if anchor_mode == "front_vehicle" else "ego_seed_ahead"
        goal_source = "long_ahead_fallback"
        if requested_mode in {"ego_seed_ahead", "front_vehicle_ahead"}:
            goal_source = "long_ahead"
        elif requested_mode == "scenario_xy":
            goal_source = "scenario_goal_missing_fallback"
        elif requested_mode == "fixed_xy":
            goal_source = "fixed_goal_missing_fallback"
        return x1, y1, z1, {
            "requested_goal_mode": requested_mode,
            "goal_mode": goal_mode,
            "goal_source": goal_source,
            "startup_phase_used": False,
            "end_ahead_m_effective": effective_dist,
            "anchor_mode": anchor_mode,
        }

    def _maybe_send_routing_request(
        self,
        snapshot: Dict[str, Any],
        odom: Odometry,
        ts_sec: float,
        pose_info: Optional[Dict[str, float]] = None,
        speed_mps: Optional[float] = None,
    ) -> None:
        if not self.auto_routing_enabled:
            return
        if self.routing_writer is None and self.lane_follow_client is None and self.action_client is None:
            return
        if self._first_odom_ts is None:
            self._first_odom_ts = ts_sec
        if (ts_sec - self._first_odom_ts) < self.auto_routing_startup_delay_sec:
            return
        if speed_mps is None:
            speed_mps = self._latest_speed_mps
        phase = self._current_routing_phase(ts_sec, float(speed_mps))
        self.auto_routing_current_phase = phase
        ignore_roll_active = self._ignore_roll_active(0.0, 0.0)
        route_phase_sent = (
            self.auto_routing_startup_routing_sent if phase == "startup" else self.auto_routing_long_routing_sent
        )
        lane_phase_sent = (
            self.auto_routing_startup_lane_follow_sent
            if phase == "startup"
            else self.auto_routing_long_lane_follow_sent
        )

        route_ready = self.routing_writer is not None and self.auto_routing_send_routing
        route_cooldown_ok = (
            self.auto_routing_routing_sent == 0
            or (ts_sec - self.auto_routing_last_routing_ts) >= self.auto_routing_resend_sec
        )
        max_routing_attempts = max(1, int(self.auto_routing_max_attempts))
        route_attempts_left = self.auto_routing_routing_sent < max_routing_attempts
        send_routing_now = False

        lane_follow_ready = (
            (
                self.lane_follow_client is not None
                and self.auto_routing_send_lane_follow
                and (not self._lane_follow_disabled_runtime)
            )
            or (self.action_client is not None and self.auto_routing_send_action)
        )
        lane_follow_cooldown_ok = (
            self.auto_routing_lane_follow_sent == 0
            or (ts_sec - self.auto_routing_last_lane_follow_ts) >= self.auto_routing_lane_follow_refresh_sec
        )
        send_lane_follow_now = bool(lane_follow_ready and lane_follow_cooldown_ok and not lane_phase_sent)

        if pose_info is None:
            pose = odom.pose.pose
            pos = pose.position
            ori = pose.orientation
            pose_info = self._transform_pose(pos, ori)
        x0 = float(pose_info["map_x"])
        y0 = float(pose_info["map_y"])
        z0 = float(pose_info["map_z"])
        yaw0 = float(pose_info["map_yaw"])
        x0, y0 = self._clamp_xy_to_bounds(x0, y0)
        x0, y0, start_proj = self._snap_xy_to_lane(x0, y0)
        start_raw_xy = (x0, y0)
        start_nudge_applied = False
        start_nudge_effective = 0.0
        start_nudge_heading_deg: Optional[float] = None
        if self.auto_routing_start_nudge_m > 0.0:
            # Increase nudge for later attempts in the same run to escape map
            # seam/boundary cases where start_s is very close to zero or negative.
            start_nudge_effective = self.auto_routing_start_nudge_m + (
                max(0, int(self.auto_routing_routing_sent)) * self.auto_routing_start_nudge_retry_step_m
            )
            start_nudge_effective = max(self.auto_routing_start_nudge_min_safe_m, start_nudge_effective)
            if self.auto_routing_start_nudge_max_m > 0.0:
                start_nudge_effective = min(self.auto_routing_start_nudge_max_m, start_nudge_effective)
            nudge_heading = yaw0
            if bool(start_proj.get("available", False)):
                lane_yaw_deg = self._coerce_float(
                    start_proj.get("lane_yaw_deg", math.degrees(yaw0)),
                    math.degrees(yaw0),
                    "start_proj.lane_yaw_deg",
                    "routing_start_nudge",
                )
                nudge_heading = math.radians(lane_yaw_deg)
            nudged_x = x0 + start_nudge_effective * math.cos(nudge_heading)
            nudged_y = y0 + start_nudge_effective * math.sin(nudge_heading)
            nudged_x, nudged_y = self._clamp_xy_to_bounds(nudged_x, nudged_y)
            x0, y0, start_proj = self._snap_xy_to_lane(nudged_x, nudged_y)
            start_nudge_applied = True
            start_nudge_heading_deg = math.degrees(nudge_heading)
        ignore_roll_active = self._ignore_roll_active(x0, y0)
        if ignore_roll_active:
            route_attempts_left = route_attempts_left and (
                self._ignore_roll_route_count < max(1, self.traffic_light_ignore_roll_max_refresh)
            )
            send_routing_now = bool(route_ready and route_attempts_left and route_cooldown_ok)
            send_lane_follow_now = False
        else:
            send_routing_now = bool(route_ready and route_cooldown_ok and not route_phase_sent)
            if send_routing_now and not route_attempts_left:
                send_routing_now = False
        if not send_routing_now and not send_lane_follow_now:
            if route_ready and (not self.auto_routing_established) and (not route_attempts_left):
                self.stats["last_error"] = "routing_not_established_after_max_attempts"
            return
        routing_anchor = self._extract_routing_anchor(snapshot, x0=x0, y0=y0, yaw0=yaw0)
        if self.auto_routing_routing_sent == 0 and self.auto_routing_active_goal is None:
            rx_counts = snapshot.get("rx_counts", {}) or {}
            obstacle_rx = int(rx_counts.get("objects_json", 0)) + int(rx_counts.get("objects3d", 0)) + int(
                rx_counts.get("markers", 0)
            )
            wait_extra_sec = 3.0
            if obstacle_rx <= 0:
                self.stats["last_error"] = "waiting_for_obstacle_gt_before_initial_routing"
                return
            if (
                self.auto_routing_goal_mode == "front_vehicle_ahead"
                and routing_anchor is None
                and (ts_sec - (self._first_odom_ts or ts_sec))
                < (self.auto_routing_startup_delay_sec + wait_extra_sec)
            ):
                self.stats["last_error"] = "waiting_for_front_anchor_before_initial_routing"
                return
        self.stats["last_routing_anchor"] = routing_anchor or {}
        x1, y1, z1, goal_meta = self._build_routing_goal(
            x0,
            y0,
            z0,
            yaw0,
            anchor=routing_anchor,
            phase=phase,
            ignore_roll_active=ignore_roll_active,
        )
        x1, y1, goal_proj = self._snap_xy_to_lane(x1, y1)
        goal_dist_m = math.hypot(x1 - x0, y1 - y0)
        self.stats["last_routing_goal"] = {
            "start": {"x": x0, "y": y0, "z": z0},
            "start_raw": {"x": start_raw_xy[0], "y": start_raw_xy[1], "z": z0},
            "goal": {"x": x1, "y": y1, "z": z1},
            "anchor_mode": goal_meta.get("anchor_mode", "ego_seed"),
            "goal_mode": goal_meta.get("goal_mode", "ego_seed_ahead"),
            "goal_mode_requested": goal_meta.get("requested_goal_mode", self.auto_routing_goal_mode),
            "goal_source": goal_meta.get("goal_source", ""),
            "goal_dist_m": goal_dist_m,
            "startup_phase_used": bool(goal_meta.get("startup_phase_used", False)),
            "end_ahead_m_effective": goal_meta.get("end_ahead_m_effective"),
            "start_nudge_applied": start_nudge_applied,
            "start_nudge_m_effective": start_nudge_effective if start_nudge_applied else 0.0,
            "start_nudge_heading_deg": start_nudge_heading_deg,
            "phase": phase,
        }
        self.stats["last_routing_projection"] = {
            "start": start_proj,
            "goal": goal_proj,
        }
        self.stats["last_routing_reason"] = f"{phase}:{goal_meta.get('goal_source', '')}"
        command_ts = self._command_now_sec()

        if self.routing_writer is not None and send_routing_now:
            req = self.routing_pb2.RoutingRequest()
            self._fill_header(getattr(req, "header", None), command_ts, "tb_apollo10_gt_bridge")
            wp0 = req.waypoint.add()
            wp1 = req.waypoint.add()
            if hasattr(wp0, "pose"):
                wp0.pose.x, wp0.pose.y, wp0.pose.z = x0, y0, z0
                wp1.pose.x, wp1.pose.y, wp1.pose.z = x1, y1, z1
            elif hasattr(wp0, "x"):
                wp0.x, wp0.y, wp0.z = x0, y0, z0
                wp1.x, wp1.y, wp1.z = x1, y1, z1
            self.routing_writer.write(req)
            self.auto_routing_pending_goal = (x1, y1, z1)
            self.auto_routing_routing_sent += 1
            self.auto_routing_last_routing_ts = ts_sec
            self.stats["routing_request_count"] += 1
            if ignore_roll_active:
                self._ignore_roll_route_count += 1
            else:
                self.stats["routing_phase_counts"][phase] = int(self.stats["routing_phase_counts"].get(phase, 0)) + 1
            if phase == "startup" and not ignore_roll_active:
                self.auto_routing_startup_routing_sent = True
                if self._ignore_roll_start_xy is None:
                    self._ignore_roll_start_xy = (x0, y0)
            else:
                if not ignore_roll_active:
                    self.auto_routing_long_routing_sent = True
            print(
                "[bridge][routing] request sent "
                f"phase={phase} mode={self.stats['last_routing_goal'].get('goal_mode', '')} "
                f"goal_dist={goal_dist_m:.2f}m nudge={start_nudge_effective:.2f}m"
            )

        if self.action_client is not None and self.auto_routing_send_action and send_lane_follow_now:
            act = self.action_pb2.ActionCommand()
            self._fill_header(getattr(act, "header", None), command_ts, "tb_apollo10_gt_bridge")
            if hasattr(act, "command_id"):
                act.command_id = int(self._next_seq())
            if hasattr(self.action_pb2, "ActionCommandType") and hasattr(self.action_pb2.ActionCommandType, "FOLLOW"):
                act.command = self.action_pb2.ActionCommandType.FOLLOW
            else:
                # FOLLOW=1 in Apollo enum.
                act.command = 1
            try:
                act_resp = self.action_client.send_request(act)
                self.stats["action_follow_count"] += 1
                if act_resp is not None and hasattr(act_resp, "status"):
                    self.stats["last_error"] = ""
            except Exception as exc:
                self.stats["last_error"] = f"action follow request failed: {exc}"
            if phase == "startup":
                self.auto_routing_startup_lane_follow_sent = True
            else:
                self.auto_routing_long_lane_follow_sent = True

        if self.lane_follow_client is not None and self.auto_routing_send_lane_follow and send_lane_follow_now:
            cmd = self.lane_follow_pb2.LaneFollowCommand()
            self._fill_header(getattr(cmd, "header", None), command_ts, "tb_apollo10_gt_bridge")
            if hasattr(cmd, "command_id"):
                cmd.command_id = int(self._next_seq())
            if hasattr(cmd, "is_start_pose_set"):
                cmd.is_start_pose_set = True
            if hasattr(cmd, "target_speed"):
                cmd.target_speed = float(self.auto_routing_target_speed)
            if hasattr(cmd, "way_point"):
                wp = cmd.way_point.add()
                wp.x = x0
                wp.y = y0
            if hasattr(cmd, "end_pose"):
                cmd.end_pose.x = x1
                cmd.end_pose.y = y1
            try:
                lf_resp = self.lane_follow_client.send_request(cmd)
                self.stats["lane_follow_count"] += 1
                if lf_resp is None:
                    self.stats["lane_follow_no_response_count"] = int(
                        self.stats.get("lane_follow_no_response_count", 0)
                    ) + 1
                    if self.auto_routing_disable_lane_follow_on_no_response:
                        self._lane_follow_disabled_runtime = True
                        self.auto_routing_send_lane_follow = False
                        self.stats["lane_follow_disabled_on_no_response"] = True
                        self.stats["last_error"] = ""
                        print(
                            "[bridge][routing][warn] lane_follow no response; "
                            "disable lane_follow and keep routing_request path"
                        )
                    elif self.auto_routing_send_routing:
                        self.stats["last_error"] = ""
                    else:
                        self.stats["last_error"] = "lane_follow no response"
                elif hasattr(lf_resp, "status"):
                    # CommandStatusType::ERROR == 2 in Apollo external command proto.
                    if int(getattr(lf_resp, "status", 0)) == 2:
                        self.stats["last_error"] = f"lane_follow error: {getattr(lf_resp, 'message', '')}"
                    else:
                        self.stats["last_error"] = ""
            except Exception as exc:
                self.stats["lane_follow_no_response_count"] = int(
                    self.stats.get("lane_follow_no_response_count", 0)
                ) + 1
                if self.auto_routing_disable_lane_follow_on_no_response:
                    self._lane_follow_disabled_runtime = True
                    self.auto_routing_send_lane_follow = False
                    self.stats["lane_follow_disabled_on_no_response"] = True
                    self.stats["last_error"] = ""
                    print(
                        "[bridge][routing][warn] lane_follow request failed; "
                        "disable lane_follow and keep routing_request path"
                    )
                elif self.auto_routing_send_routing:
                    self.stats["last_error"] = ""
                else:
                    self.stats["last_error"] = f"lane_follow request failed: {exc}"
            self.auto_routing_lane_follow_sent += 1
            self.auto_routing_last_lane_follow_ts = ts_sec
            if phase == "startup":
                self.auto_routing_startup_lane_follow_sent = True
            else:
                self.auto_routing_long_lane_follow_sent = True
            self.auto_routing_active_goal = (x1, y1, z1)

    def _write_stats(self) -> None:
        self._write_health_summary()
        self._write_json_file(self.stats_path, self.stats)

    def run(self) -> int:
        self.ros_thread.start()
        if self.cyber_spin_thread is not None:
            self.cyber_spin_thread.start()

        period = 1.0 / max(self.publish_rate_hz, 1e-3)
        last_stats_flush = 0.0
        while not self.stop_event.is_set():
            if hasattr(self.cyber, "is_shutdown") and self.cyber.is_shutdown():
                break
            try:
                snapshot = self.node.snapshot()
                self.stats["ros_input_counts"] = snapshot.get("rx_counts", self.stats["ros_input_counts"])
                odom = snapshot.get("odom")
                if odom is not None:
                    loc, ts_sec, vel_xyz, pose_debug, pose_info = self._odom_to_loc(odom)
                    self.loc_writer.write(loc)
                    self.stats["loc_count"] += 1
                    measured = self._read_measured_control()
                    ch = self._odom_to_chassis(ts_sec, vel_xyz, measured)
                    self.chassis_writer.write(ch)
                    self.stats["chassis_count"] += 1
                    ex = float(pose_info["map_x"])
                    ey = float(pose_info["map_y"])
                    obs_msg, obs_count = self._objects_to_obstacles(snapshot, ts_sec, (ex, ey))
                    self.obs_writer.write(obs_msg)
                    self.stats["obstacles_count"] += int(obs_count)
                    self.stats["last_obstacles_count"] = int(obs_count)
                    desired_in = self.stats.get("last_control_in", {}) or {}
                    desired_out = self.stats.get("last_control_out", {}) or {}
                    speed_mps = math.sqrt(sum(v * v for v in vel_xyz))
                    self._latest_speed_mps = float(speed_mps)
                    self._max_speed_mps = max(self._max_speed_mps, self._latest_speed_mps)
                    nearest_obs = self._nearest_obstacle_distance(snapshot, (ex, ey))
                    front_status = self._front_obstacle_behavior_status()
                    front_gap = front_status.get("last_gap", {}) or {}
                    row = {
                        "ts_sec": ts_sec,
                        "map_x": self._coerce_float(pose_debug.get("map_x"), float("nan"), "pose_debug.map_x", "publish_row"),
                        "map_y": self._coerce_float(pose_debug.get("map_y"), float("nan"), "pose_debug.map_y", "publish_row"),
                        "map_yaw_deg": self._coerce_float(pose_debug.get("map_yaw_deg"), float("nan"), "pose_debug.map_yaw_deg", "publish_row"),
                        "in_bounds": pose_debug.get("in_bounds"),
                        "lane_inside": pose_debug.get("lane_inside"),
                        "lane_dist_m": self._coerce_float(pose_debug.get("lane_dist_m"), float("nan"), "pose_debug.lane_dist_m", "publish_row"),
                        "e_y_m": self._coerce_float(pose_debug.get("e_y_m"), float("nan"), "pose_debug.e_y_m", "publish_row"),
                        "e_psi_deg": self._coerce_float(pose_debug.get("e_psi_deg"), float("nan"), "pose_debug.e_psi_deg", "publish_row"),
                        "preview_x": self._coerce_float(pose_debug.get("preview_x"), float("nan"), "pose_debug.preview_x", "publish_row"),
                        "preview_y": self._coerce_float(pose_debug.get("preview_y"), float("nan"), "pose_debug.preview_y", "publish_row"),
                        "preview_heading_deg": self._coerce_float(pose_debug.get("preview_heading_deg"), float("nan"), "pose_debug.preview_heading_deg", "publish_row"),
                        "target_curvature": self._coerce_float(pose_debug.get("target_curvature"), float("nan"), "pose_debug.target_curvature", "publish_row"),
                        "apollo_desired_throttle": self._coerce_float(desired_in.get("throttle"), float("nan"), "desired_in.throttle", "publish_row"),
                        "apollo_desired_brake": self._coerce_float(desired_in.get("brake"), float("nan"), "desired_in.brake", "publish_row"),
                        "apollo_desired_steer": self._coerce_float(desired_in.get("steer"), float("nan"), "desired_in.steer", "publish_row"),
                        "apollo_mode": desired_in.get("mode", ""),
                        "apollo_gear": desired_in.get("gear", ""),
                        "apollo_estop": desired_in.get("estop", False),
                        "commanded_throttle": self._coerce_float(desired_out.get("throttle"), float("nan"), "desired_out.throttle", "publish_row"),
                        "throttle_after_boost": self._coerce_float(desired_out.get("throttle_after_boost"), float("nan"), "desired_out.throttle_after_boost", "publish_row"),
                        "commanded_brake": self._coerce_float(desired_out.get("brake"), float("nan"), "desired_out.brake", "publish_row"),
                        "brake_after_deadzone": self._coerce_float(desired_out.get("brake_after_deadzone"), float("nan"), "desired_out.brake_after_deadzone", "publish_row"),
                        "commanded_steer": self._coerce_float(desired_out.get("steer"), float("nan"), "desired_out.steer", "publish_row"),
                        "commanded_steer_pre_clamp": self._coerce_float(desired_out.get("steer_pre_clamp"), float("nan"), "desired_out.steer_pre_clamp", "publish_row"),
                        "commanded_steer_clamped": desired_out.get("steer_clamped", False),
                        "straight_lane_zero_steer_applied": desired_out.get("straight_lane_zero_steer_applied", False),
                        "low_speed_steer_guard_applied": desired_out.get("low_speed_steer_guard_applied", False),
                        "force_zero_steer_applied": desired_out.get("force_zero_steer_applied", False),
                        "traffic_light_policy": self.traffic_light_policy,
                        "measured_throttle": self._coerce_float(measured.get("throttle"), float("nan"), "measured.throttle", "publish_row"),
                        "measured_brake": self._coerce_float(measured.get("brake"), float("nan"), "measured.brake", "publish_row"),
                        "measured_steer": self._coerce_float(measured.get("steer"), float("nan"), "measured.steer", "publish_row"),
                        "measured_steer_pct": self._coerce_float(measured.get("steer_feedback_pct"), float("nan"), "measured.steer_feedback_pct", "publish_row"),
                        "measured_steer_deg": self._coerce_float(measured.get("steer_feedback_deg"), float("nan"), "measured.steer_feedback_deg", "publish_row"),
                        "measured_forward_accel_mps2": self._coerce_float(measured.get("forward_accel_mps2"), float("nan"), "measured.forward_accel_mps2", "publish_row"),
                        "measured_available": measured.get("available", False),
                        "measured_source": measured.get("source", "unavailable"),
                        "measured_vs_command_throttle_gap": self._coerce_float(measured.get("throttle"), 0.0, "measured.throttle_gap_src", "publish_row") - self._coerce_float(desired_out.get("throttle"), 0.0, "desired_out.throttle_gap_src", "publish_row"),
                        "measured_vs_command_brake_gap": self._coerce_float(measured.get("brake"), 0.0, "measured.brake_gap_src", "publish_row") - self._coerce_float(desired_out.get("brake"), 0.0, "desired_out.brake_gap_src", "publish_row"),
                        "measured_vs_command_steer_gap": self._coerce_float(measured.get("steer"), 0.0, "measured.steer_gap_src", "publish_row") - self._coerce_float(desired_out.get("steer"), 0.0, "desired_out.steer_gap_src", "publish_row"),
                        "speed_mps": speed_mps,
                        "nearest_obstacle_dist_m": nearest_obs if nearest_obs is not None else float("nan"),
                        "front_obstacle_mode": front_status.get("mode", ""),
                        "front_obstacle_visible": front_status.get("visible", True),
                        "front_obstacle_gap_lon_m": self._coerce_float(front_gap.get("lon_m"), float("nan"), "front_gap.lon_m", "publish_row"),
                        "front_obstacle_gap_lat_m": self._coerce_float(front_gap.get("lat_m"), float("nan"), "front_gap.lat_m", "publish_row"),
                        "front_obstacle_gap_distance_m": self._coerce_float(front_gap.get("distance_m"), float("nan"), "front_gap.distance_m", "publish_row"),
                        "front_obstacle_suppressed_frames": front_status.get("suppressed_frames", 0),
                        "longitudinal_override_applied": desired_out.get("longitudinal_override_applied", False),
                        "longitudinal_override_mode": desired_out.get("longitudinal_override_mode", ""),
                        "longitudinal_override_phase": desired_out.get("longitudinal_override_phase", ""),
                        "longitudinal_target_speed_mps": self._coerce_float(desired_out.get("longitudinal_target_speed_mps"), float("nan"), "desired_out.longitudinal_target_speed_mps", "publish_row"),
                        "longitudinal_speed_error_mps": self._coerce_float(desired_out.get("longitudinal_speed_error_mps"), float("nan"), "desired_out.longitudinal_speed_error_mps", "publish_row"),
                        "longitudinal_front_gap_lon_m": self._coerce_float(desired_out.get("longitudinal_front_gap_lon_m"), float("nan"), "desired_out.longitudinal_front_gap_lon_m", "publish_row"),
                        "longitudinal_front_gap_lat_m": self._coerce_float(desired_out.get("longitudinal_front_gap_lat_m"), float("nan"), "desired_out.longitudinal_front_gap_lat_m", "publish_row"),
                        "longitudinal_front_gap_distance_m": self._coerce_float(desired_out.get("longitudinal_front_gap_distance_m"), float("nan"), "desired_out.longitudinal_front_gap_distance_m", "publish_row"),
                        "terminal_stop_hold_active": desired_out.get("terminal_stop_hold_active", False),
                        "terminal_stop_hold_phase": desired_out.get("terminal_stop_hold_phase", ""),
                        "terminal_stop_hold_front_gap_lon_m": self._coerce_float(desired_out.get("terminal_stop_hold_front_gap_lon_m"), float("nan"), "desired_out.terminal_stop_hold_front_gap_lon_m", "publish_row"),
                        "terminal_stop_hold_front_gap_lat_m": self._coerce_float(desired_out.get("terminal_stop_hold_front_gap_lat_m"), float("nan"), "desired_out.terminal_stop_hold_front_gap_lat_m", "publish_row"),
                        "terminal_stop_hold_front_gap_distance_m": self._coerce_float(desired_out.get("terminal_stop_hold_front_gap_distance_m"), float("nan"), "desired_out.terminal_stop_hold_front_gap_distance_m", "publish_row"),
                        "routing_established": self.auto_routing_established,
                        "routing_last_road_count": self.stats.get("routing_last_road_count", 0),
                        "lane_follow_count": self.stats.get("lane_follow_count", 0),
                        "control_rx_count": self.stats.get("control_rx_count", 0),
                        "control_tx_count": self.stats.get("control_tx_count", 0),
                    }
                    self._write_debug_row(row)
                    self._update_steer_sign_auto_check(row)
                    self._maybe_dump_saturation_snapshot(row)
                    self._maybe_dump_control_anomaly(row, snapshot, measured)
                    now_wall = time.time()
                    if now_wall - self._last_control_print_sec >= 1.0:
                        self._last_control_print_sec = now_wall
                        print(
                            "[bridge][control] desired(thr=%.3f brk=%.3f str=%.3f) measured(thr=%s brk=%s str=%s src=%s)"
                            % (
                                float(desired_out.get("throttle", 0.0) or 0.0),
                                float(desired_out.get("brake", 0.0) or 0.0),
                                float(desired_out.get("steer", 0.0) or 0.0),
                                "n/a" if measured.get("throttle") is None else f"{float(measured.get('throttle', 0.0)):.3f}",
                                "n/a" if measured.get("brake") is None else f"{float(measured.get('brake', 0.0)):.3f}",
                                "n/a" if measured.get("steer") is None else f"{float(measured.get('steer', 0.0)):.3f}",
                                measured.get("source", "unavailable"),
                            )
                        )
                    self._maybe_publish_traffic_lights(ts_sec)
                    self._maybe_send_routing_request(snapshot, odom, ts_sec, pose_info, speed_mps=speed_mps)
                    self.stats["last_publish_ts_sec"] = ts_sec
                    self.stats["last_publish_error"] = ""
                    if str(self.stats.get("last_error", "")).startswith("publish loop error:"):
                        self.stats["last_error"] = ""
            except Exception as exc:
                self.stats["publish_errors"] = int(self.stats.get("publish_errors", 0)) + 1
                msg = f"publish loop error: {exc}"
                self.stats["last_publish_error"] = msg
                self.stats["last_error"] = msg
                now_wall = time.time()
                if (now_wall - self._last_publish_error_log_sec) >= 2.0:
                    self._last_publish_error_log_sec = now_wall
                    print(f"[bridge][warn] {msg}")

            now = time.time()
            if (now - last_stats_flush) >= 1.0:
                self._write_stats()
                last_stats_flush = now
            time.sleep(period)

        self._write_stats()
        return 0

    def shutdown(self) -> None:
        self.stop_event.set()
        try:
            self.executor.shutdown(timeout_sec=1.0)
        except Exception:
            pass
        try:
            self.node.destroy_node()
        except Exception:
            pass
        try:
            rclpy.try_shutdown()
        except Exception:
            pass
        try:
            if hasattr(self.cyber, "shutdown"):
                self.cyber.shutdown()
        except Exception:
            pass


def _deep_update(base: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_update(base[key], value)
        else:
            base[key] = value
    return base


def _default_config() -> Dict[str, Any]:
    return {
        "ros2": {
            "ego_id": "hero",
            "odom_topic": "",
            "objects3d_topic": "",
            "objects_markers_topic": "",
            "objects_json_topic": "",
            "control_out_topic": "/tb/ego/control_cmd",
            "control_out_type": "direct",
        },
        "cyber": {
            "localization_channel": "/apollo/localization/pose",
            "chassis_channel": "/apollo/canbus/chassis",
            "obstacles_channel": "/apollo/perception/obstacles",
            "control_channel": "/apollo/control",
            "routing_request_channel": "/apollo/raw_routing_request",
            "action_channel": "/apollo/external_command/action",
            "lane_follow_channel": "/apollo/external_command/lane_follow",
            "routing_response_channel": "/apollo/routing_response",
            "traffic_light_channel": "/apollo/perception/traffic_light",
            "planning_channel": "/apollo/planning",
        },
        "bridge": {
            "publish_rate_hz": 20.0,
            "max_obstacles": 64,
            "radius_m": 120.0,
            "map_file": "",
            "map_bounds_file": "",
            "debug_pose_print": False,
            "debug_dump_control_raw": False,
            "auto_routing": {
                "enabled": False,
                "goal_mode": "ego_seed_ahead",
                "fixed_goal_xy": {},
                "end_ahead_m": 80.0,
                "min_end_ahead_m": 3.0,
                "startup_end_ahead_m": 6.0,
                "startup_speed_threshold_mps": 0.5,
                "startup_hold_sec": 3.0,
                "start_nudge_m": 2.0,
                "start_nudge_retry_step_m": 1.5,
                "start_nudge_min_safe_m": 1.5,
                "start_nudge_max_m": 6.0,
                "resend_sec": 5.0,
                "max_attempts": 5,
                "target_speed_mps": 8.0,
                "startup_delay_sec": 3.0,
                "use_long_goal_after_move": True,
                "scenario_goal_path": "scenario_goal.json",
                "send_action": False,
                "send_lane_follow": False,
                "disable_lane_follow_on_no_response": True,
                "send_routing_request": True,
            },
            "traffic_light": {
                "policy": "force_green",
                "force_ids": ["TL_signal14"],
                "publish_hz": 10.0,
                "channel": "/apollo/perception/traffic_light",
                "ignore_roll_enabled": False,
                "ignore_roll_distance_m": 45.0,
                "ignore_roll_ahead_m": 8.0,
                "ignore_roll_max_refresh": 12,
            },
            "carla_to_apollo": {
                "tx": 0.0,
                "ty": 0.0,
                "tz": 0.0,
                "yaw_deg": 0.0,
                "auto_calib": False,
                "auto_calib_snap_right_angle": False,
                "auto_calib_samples": 20,
                "auto_calib_dump_file": "",
            },
            "carla_feedback": {
                "enabled": True,
                "host": "127.0.0.1",
                "port": 2000,
                "ego_role_name": "hero",
                "timeout_sec": 1.5,
            },
            "front_obstacle_behavior": {
                "mode": "normal",
                "activate_distance_m": 18.0,
                "release_distance_m": 24.0,
                "min_longitudinal_m": 2.0,
                "max_lateral_m": 3.5,
                "latch_enabled": True,
                "cache_enabled": True,
                "cache_ttl_sec": 0.5,
                "role_names": ["front"],
            },
            "control_mapping": {
                "max_steer_angle": 0.6,
                "speed_gain": 10.0,
                "brake_gain": 5.0,
                "steer_sign": 1.0,
                "throttle_scale": 1.0,
                "brake_scale": 1.0,
                "steer_scale": 1.0,
                "brake_deadzone": 0.05,
                "zero_hold_sec": 0.0,
                "startup_throttle_boost_enabled": True,
                "startup_throttle_boost_add": 0.08,
                "startup_throttle_boost_cap": 0.25,
                "straight_lane_zero_steer_enabled": False,
                "straight_lane_zero_steer_max_speed_mps": 8.0,
                "straight_lane_zero_steer_max_e_y_m": 0.15,
                "straight_lane_zero_steer_max_e_psi_deg": 3.0,
                "straight_lane_zero_steer_max_curvature": 0.001,
                "straight_lane_zero_steer_latch_enabled": True,
                "straight_lane_zero_steer_release_max_e_y_m": 0.6,
                "straight_lane_zero_steer_release_max_e_psi_deg": 6.0,
                "straight_lane_zero_steer_release_ignore_e_psi_below_speed_mps": 0.5,
                "low_speed_steer_guard_enabled": True,
                "low_speed_steer_guard_speed_mps": 0.2,
                "low_speed_steer_guard_max_abs_steer": 0.05,
                "low_speed_steer_guard_max_e_y_m": 0.2,
                "low_speed_steer_guard_max_e_psi_deg": 2.0,
                "force_zero_steer_output": False,
                "straight_acc_override": {
                    "enabled": False,
                    "mode": "cruise_then_stop",
                    "target_speed_mps": 0.0,
                    "min_cruise_throttle": 0.22,
                    "max_throttle": 0.55,
                    "speed_kp": 0.03,
                    "coast_band_mps": 0.4,
                    "max_brake": 0.45,
                    "brake_kp": 0.12,
                    "slowdown_distance_m": 40.0,
                    "stop_distance_m": 10.0,
                    "full_brake_distance_m": 6.0,
                    "stop_hold_brake": 0.25,
                    "max_e_y_m": 1.0,
                    "max_curvature": 0.002,
                    "max_lateral_gap_m": 3.5,
                    "min_valid_front_gap_m": 2.0,
                },
                "terminal_stop_hold": {
                    "enabled": False,
                    "activate_gap_m": 15.0,
                    "release_gap_m": 20.0,
                    "activate_speed_mps": 0.6,
                    "release_speed_mps": 1.5,
                    "hold_brake": 0.18,
                    "recent_brake_window_sec": 2.0,
                    "brake_trigger": 0.05,
                    "max_lateral_gap_m": 3.5,
                    "min_hold_sec": 0.6,
                },
                "auto_apply_steer_sign": False,
                "steer_sign_check_frames": 30,
            },
        },
    }


def _load_config(path: Path, ns_override: Optional[str] = None, ego_override: Optional[str] = None) -> Dict[str, Any]:
    cfg = _default_config()
    if path.exists():
        loaded = yaml.safe_load(path.read_text()) or {}
        _deep_update(cfg, loaded)
    ego_id = str(ego_override or cfg.get("ros2", {}).get("ego_id") or "hero")
    cfg["ros2"]["ego_id"] = ego_id
    ns = _normalize_ns(ns_override or cfg.get("ros2", {}).get("namespace") or "/carla")
    cfg["ros2"]["namespace"] = ns
    prefix = f"{ns}/{ego_id}"
    if not cfg["ros2"].get("odom_topic"):
        cfg["ros2"]["odom_topic"] = f"{prefix}/odom"
    if not cfg["ros2"].get("objects3d_topic"):
        cfg["ros2"]["objects3d_topic"] = f"{prefix}/objects3d"
    if not cfg["ros2"].get("objects_markers_topic"):
        cfg["ros2"]["objects_markers_topic"] = f"{prefix}/objects_markers"
    if not cfg["ros2"].get("objects_json_topic"):
        cfg["ros2"]["objects_json_topic"] = f"{prefix}/objects_gt_json"
    return cfg


def main() -> int:
    ap = argparse.ArgumentParser(description="ROS2 GT <-> Apollo 10 CyberRT bridge")
    ap.add_argument("--config", type=Path, required=True)
    ap.add_argument("--stats-path", type=Path, required=True)
    ap.add_argument("--apollo-root", type=Path, default=Path(os.environ.get("APOLLO_ROOT", "")))
    ap.add_argument("--pb-root", type=Path, default=Path(__file__).resolve().parent / "pb")
    ap.add_argument("--ego-id", type=str, default=None)
    ap.add_argument("--namespace", type=str, default=None)
    args = ap.parse_args()

    if not args.apollo_root or not str(args.apollo_root):
        raise RuntimeError("apollo-root is required (or set APOLLO_ROOT)")
    if not args.apollo_root.exists():
        raise RuntimeError(f"apollo-root not found: {args.apollo_root}")
    if not args.pb_root.exists():
        raise RuntimeError(f"pb-root not found: {args.pb_root}. run gen_pb2.sh first")

    cfg = _load_config(args.config, ns_override=args.namespace, ego_override=args.ego_id)
    bridge = ApolloGtBridge(
        cfg,
        stats_path=args.stats_path,
        apollo_root=args.apollo_root,
        pb_root=args.pb_root,
    )

    def _handle_signal(_sig, _frame):
        bridge.stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        return bridge.run()
    finally:
        bridge.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
