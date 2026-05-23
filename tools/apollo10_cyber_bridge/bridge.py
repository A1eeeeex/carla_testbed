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
from collections import Counter, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import yaml

# Apollo 10.0 shipped pb2 files are not compatible with protobuf>=4 C++ descriptors.
# Force the pure-Python runtime inside the bridge process so planning/traffic-light
# readers can import the generated modules instead of silently disabling them.
os.environ.setdefault("PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION", "python")
from google.protobuf import empty_pb2

ROS2_RUNTIME_IMPORT_OK = True
ROS2_RUNTIME_IMPORT_ERROR = ""

try:
    import rclpy
    from nav_msgs.msg import Odometry
    from rclpy.executors import MultiThreadedExecutor
    from rclpy.node import Node
    from rclpy.qos import QoSHistoryPolicy, QoSProfile, QoSReliabilityPolicy
    from rclpy.qos import qos_profile_sensor_data
    from std_msgs.msg import String
except Exception as exc:  # pragma: no cover
    ROS2_RUNTIME_IMPORT_OK = False
    ROS2_RUNTIME_IMPORT_ERROR = str(exc)
    rclpy = None  # type: ignore[assignment]
    try:
        from ros_shims import OdometryShim as Odometry
        from ros_shims import StringShim as String
    except Exception:
        from tools.apollo10_cyber_bridge.ros_shims import (  # type: ignore
            OdometryShim as Odometry,
            StringShim as String,
        )

    class Node:  # type: ignore[override]
        pass

    class MultiThreadedExecutor:  # type: ignore[override]
        def __init__(self, *args, **kwargs) -> None:
            del args, kwargs

        def spin(self) -> None:
            return None

        def shutdown(self, timeout_sec: float | None = None) -> bool:
            del timeout_sec
            return True

    class QoSHistoryPolicy:  # type: ignore[override]
        KEEP_LAST = 0

    class QoSReliabilityPolicy:  # type: ignore[override]
        BEST_EFFORT = 0

    class QoSProfile:  # type: ignore[override]
        def __init__(self, *args, **kwargs) -> None:
            del args, kwargs

    qos_profile_sensor_data = object()

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

try:
    from actuator_mapping import ActuatorCalibration, load_actuator_calibration, resolve_calibration_path
except Exception:
    from tools.apollo10_cyber_bridge.actuator_mapping import (  # type: ignore
        ActuatorCalibration,
        load_actuator_calibration,
        resolve_calibration_path,
    )

try:
    from control_mapping import (
        ControlMappingConfig,
        derive_longitudinal_targets as derive_longitudinal_targets_impl,
        legacy_map_base_controls as legacy_map_base_controls_impl,
        normalize_steering_command as normalize_steering_command_impl,
        physical_map_base_controls as physical_map_base_controls_impl,
        select_steering_field as select_steering_field_impl,
    )
except Exception:
    from tools.apollo10_cyber_bridge.control_mapping import (  # type: ignore
        ControlMappingConfig,
        derive_longitudinal_targets as derive_longitudinal_targets_impl,
        legacy_map_base_controls as legacy_map_base_controls_impl,
        normalize_steering_command as normalize_steering_command_impl,
        physical_map_base_controls as physical_map_base_controls_impl,
        select_steering_field as select_steering_field_impl,
    )

try:
    from bridge_policy import (
        build_bridge_policy_summary as build_bridge_policy_summary_impl,
        increment_policy_counter as increment_policy_counter_impl,
        increment_policy_reason_count as increment_policy_reason_count_impl,
        record_lateral_guard_trigger as record_lateral_guard_trigger_impl,
    )
except Exception:
    from tools.apollo10_cyber_bridge.bridge_policy import (  # type: ignore
        build_bridge_policy_summary as build_bridge_policy_summary_impl,
        increment_policy_counter as increment_policy_counter_impl,
        increment_policy_reason_count as increment_policy_reason_count_impl,
        record_lateral_guard_trigger as record_lateral_guard_trigger_impl,
    )

try:
    from bridge_observer import (
        build_bridge_observer_summary as build_bridge_observer_summary_impl,
        build_projection_debug_summary as build_projection_debug_summary_impl,
        build_route_debug_observability_snapshot as build_route_debug_observability_snapshot_impl,
        build_seed_pose_debug_summary as build_seed_pose_debug_summary_impl,
        build_timing_snapshot as build_timing_snapshot_impl,
    )
except Exception:
    from tools.apollo10_cyber_bridge.bridge_observer import (  # type: ignore
        build_bridge_observer_summary as build_bridge_observer_summary_impl,
        build_projection_debug_summary as build_projection_debug_summary_impl,
        build_route_debug_observability_snapshot as build_route_debug_observability_snapshot_impl,
        build_seed_pose_debug_summary as build_seed_pose_debug_summary_impl,
        build_timing_snapshot as build_timing_snapshot_impl,
    )

try:
    from ingress_egress import build_ingress_egress_summary as build_ingress_egress_summary_impl
    from ingress_egress import build_bridge_transport_summary as build_bridge_transport_summary_impl
except Exception:
    from tools.apollo10_cyber_bridge.ingress_egress import (  # type: ignore
        build_ingress_egress_summary as build_ingress_egress_summary_impl,
        build_bridge_transport_summary as build_bridge_transport_summary_impl,
    )

try:
    from carla_direct_transport import CarlaDirectTransport, NoopExecutor
except Exception:
    from tools.apollo10_cyber_bridge.carla_direct_transport import (  # type: ignore
        CarlaDirectTransport,
        NoopExecutor,
    )


REAR_AXLE_LOCALIZATION_BACK_OFFSET_M = 1.4235
REAR_AXLE_LOCALIZATION_BACK_OFFSET_TOLERANCE_M = 0.05
APOLLO_CONTROL_STATE_REFERENCE = "rear_axle_input_com_internal"


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


def _localization_reference_mode(back_offset_m: float) -> str:
    if abs(float(back_offset_m) - REAR_AXLE_LOCALIZATION_BACK_OFFSET_M) <= (
        REAR_AXLE_LOCALIZATION_BACK_OFFSET_TOLERANCE_M
    ):
        return "rear_axle"
    if abs(float(back_offset_m)) <= REAR_AXLE_LOCALIZATION_BACK_OFFSET_TOLERANCE_M:
        return "vehicle_origin"
    return "custom_offset"


def _finite_or_none(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _finite_delta_sec(later: Any, earlier: Any) -> Optional[float]:
    later_value = _finite_or_none(later)
    earlier_value = _finite_or_none(earlier)
    if later_value is None or earlier_value is None:
        return None
    return float(later_value) - float(earlier_value)


def _summary_stats(values: Sequence[float]) -> Dict[str, Any]:
    finite = [float(item) for item in values if math.isfinite(float(item))]
    if not finite:
        return {"count": 0, "min": None, "max": None, "mean": None}
    return {
        "count": len(finite),
        "min": min(finite),
        "max": max(finite),
        "mean": sum(finite) / float(len(finite)),
    }


def _normalize_field_priority(
    raw: Any,
    *,
    default: Sequence[str],
    preferred: Any = None,
) -> List[str]:
    out: List[str] = []
    if preferred is not None:
        text = str(preferred or "").strip()
        if text:
            out.append(text)
    if isinstance(raw, (list, tuple)):
        for item in raw:
            text = str(item or "").strip()
            if text:
                out.append(text)
    elif isinstance(raw, str):
        text = raw.strip()
        if text:
            out.extend([part.strip() for part in text.split(",") if part.strip()])
    for item in default:
        text = str(item or "").strip()
        if text:
            out.append(text)
    deduped: List[str] = []
    seen = set()
    for item in out:
        if item in seen:
            continue
        deduped.append(item)
        seen.add(item)
    return deduped


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

    def ensure_pkg_path(pkg_name: str, pkg_path: Path) -> None:
        candidate = str(pkg_path)
        try:
            pkg_mod = importlib.import_module(pkg_name)
        except Exception:
            pkg_mod = sys.modules.get(pkg_name)
        if pkg_mod is None:
            pkg_mod = types.ModuleType(pkg_name)
            pkg_mod.__path__ = [candidate]
            sys.modules[pkg_name] = pkg_mod
        elif hasattr(pkg_mod, "__path__") and candidate not in list(getattr(pkg_mod, "__path__", [])):
            pkg_mod.__path__.append(candidate)

    for path in extra_roots:
        try:
            text = str(path.resolve())
        except Exception:
            continue
        if path.exists() and text not in sys.path:
            sys.path.insert(0, text)
        if not path.exists():
            continue
        for idx, part in enumerate(pkg_parts):
            ensure_pkg_path(".".join(pkg_parts[: idx + 1]), path / Path(*pkg_parts[: idx + 1]))
        common_msgs_root = path / "modules" / "common_msgs"
        if common_msgs_root.exists():
            for child in common_msgs_root.iterdir():
                if child.is_dir():
                    ensure_pkg_path(f"modules.common_msgs.{child.name}", child)
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


def _extract_lane_centerline_polylines(map_file: Path) -> List[List[Tuple[float, float]]]:
    polylines: List[List[Tuple[float, float]]] = []
    stack: List[str] = []
    current_polyline: Optional[List[Tuple[float, float]]] = None
    current_point: Optional[Dict[str, float]] = None

    with map_file.open("r", errors="ignore") as fp:
        for raw in fp:
            line = raw.strip()
            if not line:
                continue
            if line.endswith("{"):
                label = line[:-1].strip()
                stack.append(label)
                if label == "line_segment" and "central_curve" in stack:
                    current_polyline = []
                elif label == "point" and current_polyline is not None and stack.count("point") == 1:
                    current_point = {}
                continue
            if line == "}":
                if not stack:
                    continue
                label = stack.pop()
                if label == "point" and current_polyline is not None and current_point is not None:
                    x = current_point.get("x")
                    y = current_point.get("y")
                    if x is not None and y is not None:
                        current_polyline.append((float(x), float(y)))
                    current_point = None
                elif label == "line_segment" and current_polyline is not None:
                    if len(current_polyline) >= 2:
                        polylines.append(list(current_polyline))
                    current_polyline = None
                    current_point = None
                continue
            if current_point is not None and stack and stack[-1] == "point":
                if line.startswith("x:"):
                    current_point["x"] = float(line.split(":", 1)[1].strip())
                elif line.startswith("y:"):
                    current_point["y"] = float(line.split(":", 1)[1].strip())
    return polylines


def _lane_id_tokens(lane_id: Any) -> Tuple[str, str]:
    text = str(lane_id or "").strip()
    if not text:
        return "", ""
    parts = text.split("_")
    road_id = parts[0].strip() if len(parts) >= 1 else ""
    section_id = parts[1].strip() if len(parts) >= 2 else ""
    return road_id, section_id


def _extract_road_section_lane_metadata(map_file: Path) -> Dict[str, Dict[str, Any]]:
    lane_metadata: Dict[str, Dict[str, Any]] = {}
    stack: List[str] = []
    current_road_id = ""
    current_road_junction_id = ""
    current_section_id = ""
    current_lane_entries: List[Tuple[str, str]] = []

    with map_file.open("r", errors="ignore") as fp:
        for raw in fp:
            line = raw.strip()
            if not line:
                continue
            if line.endswith("{"):
                label = line[:-1].strip()
                stack.append(label)
                if label == "road":
                    current_road_id = ""
                    current_road_junction_id = ""
                    current_section_id = ""
                    current_lane_entries = []
                continue
            if line == "}":
                if not stack:
                    continue
                label = stack.pop()
                if label == "road":
                    junction_id = str(current_road_junction_id or "").strip()
                    for lane_id, section_id in current_lane_entries:
                        lane_metadata[lane_id] = {
                            "road_id": str(current_road_id or "").strip(),
                            "section_id": str(section_id or "").strip(),
                            "junction_id": junction_id,
                            "is_junction": bool(junction_id),
                        }
                    current_road_id = ""
                    current_road_junction_id = ""
                    current_section_id = ""
                    current_lane_entries = []
                elif label == "section":
                    current_section_id = ""
                continue
            if not stack or "road" not in stack or not line.startswith("id:"):
                continue
            road_index = max(i for i, item in enumerate(stack) if item == "road")
            suffix = stack[road_index + 1 :]
            value = line.split(":", 1)[1].strip().strip('"')
            if suffix == ["id"]:
                current_road_id = value
            elif suffix == ["junction_id"]:
                current_road_junction_id = value
            elif suffix == ["section", "id"]:
                current_section_id = value
            elif suffix == ["section", "lane_id"]:
                current_lane_entries.append((value, current_section_id))
    return lane_metadata


def _apollo_lane_debug_metadata_for_id(
    lane_id: Any,
    lane_metadata: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    text = str(lane_id or "").strip()
    if not text:
        return {
            "lane_id": None,
            "road_id": None,
            "section_id": None,
            "junction_id": None,
            "is_junction": None,
            "metadata_source": "missing_lane_id",
        }
    metadata = dict((lane_metadata or {}).get(text) or {})
    road_id_fallback, section_id_fallback = _lane_id_tokens(text)
    if metadata:
        road_id = str(metadata.get("road_id") or road_id_fallback or "").strip() or None
        section_id = str(metadata.get("section_id") or section_id_fallback or "").strip() or None
        junction_id = str(metadata.get("junction_id") or "").strip() or None
        is_junction: Optional[bool] = bool(junction_id)
        metadata_source = "map_lane_metadata"
    else:
        road_id = road_id_fallback or None
        section_id = section_id_fallback or None
        junction_id = None
        is_junction = None
        metadata_source = "lane_id_fallback"
    return {
        "lane_id": text,
        "road_id": road_id,
        "section_id": section_id,
        "junction_id": junction_id,
        "is_junction": is_junction,
        "metadata_source": metadata_source,
    }


def _planning_lane_metadata_fields(
    current_lane_id: Any,
    lane_id_first: Any,
    target_lane_id_first: Any,
    lane_metadata: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    current_lane = _apollo_lane_debug_metadata_for_id(current_lane_id, lane_metadata)
    lane_first = _apollo_lane_debug_metadata_for_id(lane_id_first, lane_metadata)
    target_lane = _apollo_lane_debug_metadata_for_id(target_lane_id_first, lane_metadata)
    return {
        "current_lane_id": current_lane["lane_id"],
        "current_lane_road_id": current_lane["road_id"],
        "current_lane_section_id": current_lane["section_id"],
        "current_lane_junction_id": current_lane["junction_id"],
        "current_lane_is_junction": current_lane["is_junction"],
        "current_lane_metadata_source": current_lane["metadata_source"],
        "lane_id_first": lane_first["lane_id"],
        "lane_id_first_road_id": lane_first["road_id"],
        "lane_id_first_section_id": lane_first["section_id"],
        "lane_id_first_junction_id": lane_first["junction_id"],
        "lane_id_first_is_junction": lane_first["is_junction"],
        "lane_id_first_metadata_source": lane_first["metadata_source"],
        "target_lane_id_first": target_lane["lane_id"],
        "target_lane_id_first_road_id": target_lane["road_id"],
        "target_lane_id_first_section_id": target_lane["section_id"],
        "target_lane_id_first_junction_id": target_lane["junction_id"],
        "target_lane_id_first_is_junction": target_lane["is_junction"],
        "target_lane_id_first_metadata_source": target_lane["metadata_source"],
        "target_lane_road_id": target_lane["road_id"],
        "target_lane_section_id": target_lane["section_id"],
        "target_lane_junction_id": target_lane["junction_id"],
        "target_lane_is_junction": target_lane["is_junction"],
        "target_lane_metadata_source": target_lane["metadata_source"],
    }


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


def _build_map_segments_from_polylines(
    polylines: Sequence[Sequence[Tuple[float, float]]],
    *,
    max_gap_m: float = 25.0,
) -> List[Tuple[float, float, float, float]]:
    segs: List[Tuple[float, float, float, float]] = []
    for polyline in polylines:
        segs.extend(_build_map_segments(polyline, max_gap_m=max_gap_m))
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
        self._last_speed_sample_ts: Optional[float] = None
        self._last_speed_sample_mps: Optional[float] = None

    def _estimate_forward_accel(
        self,
        *,
        speed_mps: float,
        raw_forward_accel_mps2: float,
        throttle_cmd: float,
        brake_cmd: float,
    ) -> Tuple[float, Optional[float]]:
        now = time.monotonic()
        dvdt_accel: Optional[float] = None
        if self._last_speed_sample_ts is not None and self._last_speed_sample_mps is not None:
            dt = float(now - self._last_speed_sample_ts)
            if 1e-3 <= dt <= 0.5:
                dvdt_accel = float(speed_mps - self._last_speed_sample_mps) / dt
        self._last_speed_sample_ts = now
        self._last_speed_sample_mps = float(speed_mps)

        filtered = float(raw_forward_accel_mps2)
        low_speed = float(speed_mps) < 3.0
        tiny_command = float(throttle_cmd) <= 0.05 and float(brake_cmd) <= 0.05
        if dvdt_accel is not None:
            if low_speed:
                # CARLA actor.get_acceleration() occasionally emits implausible low-speed spikes.
                if abs(filtered - dvdt_accel) > 6.0 or abs(filtered) > 8.0:
                    filtered = float(dvdt_accel)
                else:
                    filtered = 0.65 * float(dvdt_accel) + 0.35 * filtered
            elif tiny_command and abs(filtered - dvdt_accel) > 10.0:
                filtered = float(dvdt_accel)
        if low_speed:
            limit = 6.0 if float(brake_cmd) > 0.1 else 5.0
            filtered = _clamp(filtered, -limit, limit)
        return float(filtered), (float(dvdt_accel) if dvdt_accel is not None else None)

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
            tr = actor.get_transform()
            out = {
                "throttle": float(getattr(ctrl, "throttle", 0.0)),
                "brake": float(getattr(ctrl, "brake", 0.0)),
                "steer": float(getattr(ctrl, "steer", 0.0)),
                "reverse": 1.0 if bool(getattr(ctrl, "reverse", False)) else 0.0,
                "hand_brake": 1.0 if bool(getattr(ctrl, "hand_brake", False)) else 0.0,
                "gear": float(getattr(ctrl, "gear", 0.0) or 0.0),
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
            ang = actor.get_angular_velocity()
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
            fwd = tr.get_forward_vector()
            right = tr.get_right_vector()
            forward_accel = (
                float(getattr(accel, "x", 0.0)) * float(getattr(fwd, "x", 0.0))
                + float(getattr(accel, "y", 0.0)) * float(getattr(fwd, "y", 0.0))
                + float(getattr(accel, "z", 0.0)) * float(getattr(fwd, "z", 0.0))
            )
            lateral_accel = (
                float(getattr(accel, "x", 0.0)) * float(getattr(right, "x", 0.0))
                + float(getattr(accel, "y", 0.0)) * float(getattr(right, "y", 0.0))
                + float(getattr(accel, "z", 0.0)) * float(getattr(right, "z", 0.0))
            )
            yaw_rate_rps = math.radians(float(getattr(ang, "z", 0.0)))
            out["steer_feedback_pct"] = float(steer_feedback_pct)
            out["steer_feedback_source"] = steer_feedback_source
            if steer_feedback_deg is not None:
                out["steer_feedback_deg"] = float(steer_feedback_deg)
            filtered_forward_accel, dvdt_accel = self._estimate_forward_accel(
                speed_mps=float(speed_mps),
                raw_forward_accel_mps2=float(forward_accel),
                throttle_cmd=float(out["throttle"]),
                brake_cmd=float(out["brake"]),
            )
            out["speed_mps"] = float(speed_mps)
            out["accel_mps2"] = float(accel_mag)
            out["forward_accel_mps2"] = float(filtered_forward_accel)
            out["raw_forward_accel_mps2"] = float(forward_accel)
            if dvdt_accel is not None:
                out["dvdt_forward_accel_mps2"] = float(dvdt_accel)
            out["lateral_accel_mps2"] = float(lateral_accel)
            out["yaw_rate_rps"] = float(yaw_rate_rps)
            out["curvature"] = float(yaw_rate_rps / max(speed_mps, 1e-3)) if speed_mps > 0.3 else 0.0
            out["pose_x"] = float(getattr(tr.location, "x", 0.0))
            out["pose_y"] = float(getattr(tr.location, "y", 0.0))
            out["pose_z"] = float(getattr(tr.location, "z", 0.0))
            out["pose_yaw_deg"] = float(getattr(tr.rotation, "yaw", 0.0))
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
            "routing_skipped_due_to_freeze_count": 0,
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
            "reroute_reason_counts": {},
            "last_routing_reason": "",
            "goal_validity_last": {},
            "invalid_goal_count": 0,
            "traffic_light": {},
            "front_obstacle_behavior": {},
            "planning": {},
            "command_materialization": {},
            "timing": {},
            "health": {},
            "actuator_mapping": {},
            "lateral_guard_reason_counts": {},
            "lateral_guard_trigger_count": 0,
            "lateral_guard_apply_count": 0,
            "trajectory_contract_lateral_guard_apply_count": 0,
            "straight_lane_zero_steer_apply_count": 0,
            "low_speed_steer_guard_apply_count": 0,
            "force_zero_steer_apply_count": 0,
            "direct_stale_world_frame_skip_count": 0,
            "direct_stale_world_frame_republish_count": 0,
        }

        bridge_cfg = (cfg.get("bridge", {}) or {})
        self.bridge_node_name_base = str(
            bridge_cfg.get("node_name_base", "tb_apollo10_gt_bridge") or "tb_apollo10_gt_bridge"
        ).strip() or "tb_apollo10_gt_bridge"
        self.bridge_node_name = f"{self.bridge_node_name_base}_{os.getpid()}"
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
            bridge_cfg.get("localization_back_offset_m", 0.0)
        )
        self.localization_reference_mode = _localization_reference_mode(
            self.localization_back_offset_m
        )
        self.apollo_control_state_reference = APOLLO_CONTROL_STATE_REFERENCE
        self.map_file_path = str(bridge_cfg.get("map_file", "")).strip()
        self.map_bounds_file = str(bridge_cfg.get("map_bounds_file", "")).strip()
        self.map_segments: List[Tuple[float, float, float, float]] = []
        self.map_geometry_source_file = ""
        self.map_geometry_source_type = "missing"
        self.map_geometry_trusted_lane_centerline = False
        self.map_lane_metadata: Dict[str, Dict[str, Any]] = {}
        self.map_lane_metadata_source_file = ""
        self.map_lane_metadata_source_type = "missing"
        self.debug_pose_print = bool(bridge_cfg.get("debug_pose_print", False))
        self.stats["localization"] = {
            "back_offset_m": float(self.localization_back_offset_m),
            "reference_mode": self.localization_reference_mode,
            "expected_rear_axle_back_offset_m": REAR_AXLE_LOCALIZATION_BACK_OFFSET_M,
            "offset_error_m": float(
                self.localization_back_offset_m - REAR_AXLE_LOCALIZATION_BACK_OFFSET_M
            ),
            "apollo_control_state_reference": self.apollo_control_state_reference,
        }
        self.debug_csv_path = self.artifacts_dir / "debug_timeseries.csv"
        self.apollo_control_raw_path = self.artifacts_dir / "apollo_control_raw.jsonl"
        self.bridge_control_decode_path = self.artifacts_dir / "bridge_control_decode.jsonl"
        self.vehicle_response_csv_path = self.artifacts_dir / "carla_vehicle_response.csv"
        self.geometry_debug_csv_path = self.artifacts_dir / "lateral_geometry_debug.csv"
        self.startup_geometry_debug_path = self.artifacts_dir / "startup_geometry_debug.jsonl"
        self.startup_geometry_summary_path = self.artifacts_dir / "startup_geometry_summary.json"
        self.planning_topic_debug_path = self.artifacts_dir / "planning_topic_debug.jsonl"
        self.planning_topic_debug_summary_path = self.artifacts_dir / "planning_topic_debug_summary.json"
        self.planning_route_segment_debug_path = self.artifacts_dir / "planning_route_segment_debug.jsonl"
        self.apollo_map_runtime_debug_path = self.artifacts_dir / "apollo_map_runtime_debug.jsonl"
        self.stage5_apollo_map_runtime_debug_path = (
            self.artifacts_dir / "stage5_apollo_map_runtime_debug.jsonl"
        )
        self.apollo_reference_line_debug_path = self.artifacts_dir / "apollo_reference_line_debug.jsonl"
        self.stage5_apollo_reference_line_debug_path = (
            self.artifacts_dir / "stage5_apollo_reference_line_debug.jsonl"
        )
        self.apollo_route_segment_debug_path = self.artifacts_dir / "apollo_route_segment_debug.jsonl"
        self.stage5_apollo_route_segment_debug_path = (
            self.artifacts_dir / "stage5_apollo_route_segment_debug.jsonl"
        )
        self.stage5_apollo_lane_follow_map_debug_path = (
            self.artifacts_dir / "stage5_apollo_lane_follow_map_debug.jsonl"
        )
        self.control_trajectory_consume_live_path = (
            self.artifacts_dir / "control_trajectory_consume_debug_live.jsonl"
        )
        self.routing_event_debug_path = self.artifacts_dir / "routing_event_debug.jsonl"
        self.reroute_decision_debug_path = self.artifacts_dir / "reroute_decision_debug.jsonl"
        self.stage5_reroute_decision_debug_path = (
            self.artifacts_dir / "stage5_reroute_decision_debug.jsonl"
        )
        self.goal_validity_debug_path = self.artifacts_dir / "goal_validity_debug.jsonl"
        self.obstacle_contract_debug_path = self.artifacts_dir / "obstacle_contract_debug.jsonl"
        self.lateral_guard_debug_path = self.artifacts_dir / "lateral_guard_debug.jsonl"
        self.health_summary_path = self.artifacts_dir / "bridge_health_summary.json"
        self.bridge_transport_summary_path = self.artifacts_dir / "bridge_transport_summary.json"
        self.command_materialization_summary_path = (
            self.artifacts_dir / "command_materialization_summary.json"
        )
        self.carla_vehicle_path = self.artifacts_dir / "carla_vehicle_characteristics.json"
        self._debug_csv_header_written = False
        self._split_csv_headers_written: Dict[str, bool] = {}
        self._carla_vehicle_written = False
        self._debug_window: deque[Dict[str, Any]] = deque()
        self._debug_window_sec = 5.0
        self._steer_sat_counter = 0
        self._last_sat_snapshot_key = ""
        self._low_speed_sustained_guard_active = False
        self._low_speed_sustained_guard_saturation_streak = 0
        self._low_speed_sustained_guard_release_streak = 0
        self._sustained_lateral_guard_active = False
        self._sustained_lateral_guard_saturation_streak = 0
        self._sustained_lateral_guard_release_streak = 0
        self._last_control_print_sec = 0.0
        self._last_publish_error_log_sec = 0.0
        self._warn_value_log_sec: Dict[str, float] = {}
        self._command_gate_state: Dict[str, Any] = {
            "evaluation_count": 0,
            "first_eval_ts_sec": None,
            "last_eval_ts_sec": None,
            "first_eligible_ts_sec": None,
            "last_eligible_ts_sec": None,
            "first_ready_to_send_ts_sec": None,
            "last_ready_to_send_ts_sec": None,
            "last_phase": None,
            "last_status": "not_evaluated",
            "last_blocking_reason": "",
            "first_blocking_reason": "",
            "last_blocking_detail": "",
            "route_ready": False,
            "lane_follow_ready": False,
            "route_cooldown_ok": False,
            "lane_follow_cooldown_ok": False,
            "route_attempts_left": False,
            "route_phase_sent": False,
            "lane_phase_sent": False,
            "routing_request_count_at_last_eval": 0,
            "planning_nonempty_count_at_last_eval": 0,
            "control_tx_count_at_last_eval": 0,
            "send_routing_now": False,
            "send_lane_follow_now": False,
            "startup_delay_remaining_sec": None,
            "apollo_warmup_remaining_sec": None,
            "last_error_snapshot": "",
        }
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
        self.actuator_mapping_mode = str(ctrl_map.get("actuator_mapping_mode", "legacy") or "legacy").strip().lower()
        if self.actuator_mapping_mode not in {"legacy", "physical"}:
            self.actuator_mapping_mode = "legacy"
        physical_cfg = (ctrl_map.get("physical", {}) or {})
        self._physical_cfg = dict(physical_cfg)
        self._actuator_calibration_path = resolve_calibration_path(
            str(physical_cfg.get("calibration_file", "") or ""),
            repo_root=Path.cwd(),
            artifacts_dir=self.artifacts_dir,
        )
        self._actuator_calibration = load_actuator_calibration(self._actuator_calibration_path)
        self.physical_allow_legacy_fallback = bool(physical_cfg.get("allow_legacy_fallback", True))
        self.physical_apollo_max_steer_angle_deg = _safe_float(
            physical_cfg.get("apollo_max_steer_angle_deg"),
            _safe_float(ctrl_map.get("apollo_max_steer_angle_deg"), 8.2030),
        )
        self.physical_apollo_max_accel_mps2 = max(
            0.1,
            _safe_float(
                physical_cfg.get("apollo_max_accel_mps2"),
                _safe_float(ctrl_map.get("apollo_max_accel_mps2"), 4.0),
            ),
        )
        self.physical_apollo_max_decel_mps2 = max(
            0.1,
            abs(
                _safe_float(
                    physical_cfg.get("apollo_max_decel_mps2"),
                    _safe_float(ctrl_map.get("apollo_max_decel_mps2"), 6.0),
                )
            ),
        )
        self.physical_use_top_level_acceleration = bool(physical_cfg.get("use_top_level_acceleration", True))
        self.physical_use_lon_debug = bool(physical_cfg.get("use_lon_debug", True))
        self.physical_steer_field_priority = _normalize_field_priority(
            physical_cfg.get("steering_field_priority"),
            default=("steering_target", "steering_percentage", "steering", "steering_rate"),
            preferred=physical_cfg.get("preferred_steering_field"),
        )
        self.physical_acceleration_field_priority = _normalize_field_priority(
            physical_cfg.get("acceleration_field_priority"),
            default=(
                "acceleration",
                "debug_simple_lon_acceleration_cmd",
                "debug_simple_lon_acceleration_lookup",
            ),
            preferred=physical_cfg.get("preferred_acceleration_field"),
        )
        self.stats["actuator_mapping"] = {
            "mode": self.actuator_mapping_mode,
            "allow_legacy_fallback": self.physical_allow_legacy_fallback,
            "apollo_max_steer_angle_deg": self.physical_apollo_max_steer_angle_deg,
            "apollo_max_accel_mps2": self.physical_apollo_max_accel_mps2,
            "apollo_max_decel_mps2": self.physical_apollo_max_decel_mps2,
            "steering_field_priority": list(self.physical_steer_field_priority),
            "acceleration_field_priority": list(self.physical_acceleration_field_priority),
            "calibration": self._actuator_calibration.status(),
        }
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
        self.low_speed_sustained_guard_enabled = bool(
            ctrl_map.get("low_speed_sustained_saturation_guard_enabled", False)
        )
        self.low_speed_sustained_guard_speed_mps = float(
            ctrl_map.get("low_speed_sustained_saturation_guard_speed_mps", 1.0)
        )
        self.low_speed_sustained_guard_raw_threshold = float(
            ctrl_map.get("low_speed_sustained_saturation_guard_raw_threshold", 0.99)
        )
        self.low_speed_sustained_guard_trigger_frames = max(
            1, int(ctrl_map.get("low_speed_sustained_saturation_guard_trigger_frames", 20))
        )
        self.low_speed_sustained_guard_release_frames = max(
            1, int(ctrl_map.get("low_speed_sustained_saturation_guard_release_frames", 10))
        )
        self.low_speed_sustained_guard_max_abs_steer = float(
            ctrl_map.get("low_speed_sustained_saturation_guard_max_abs_steer", 0.08)
        )
        self.low_speed_sustained_guard_max_curvature = float(
            ctrl_map.get("low_speed_sustained_saturation_guard_max_curvature", 0.002)
        )
        self.low_speed_sustained_guard_max_e_y_m = float(
            ctrl_map.get("low_speed_sustained_saturation_guard_max_e_y_m", float("inf"))
        )
        self.low_speed_sustained_guard_max_e_psi_deg = float(
            ctrl_map.get("low_speed_sustained_saturation_guard_max_e_psi_deg", float("inf"))
        )
        self.low_speed_sustained_guard_require_lane_inside = bool(
            ctrl_map.get("low_speed_sustained_saturation_guard_require_lane_inside", True)
        )
        self.sustained_lateral_guard_enabled = bool(
            ctrl_map.get("sustained_lateral_guard_enabled", False)
        )
        self.sustained_lateral_guard_raw_threshold = float(
            ctrl_map.get("sustained_lateral_guard_raw_threshold", 0.99)
        )
        self.sustained_lateral_guard_trigger_frames = max(
            1, int(ctrl_map.get("sustained_lateral_guard_trigger_frames", 20))
        )
        self.sustained_lateral_guard_release_frames = max(
            1, int(ctrl_map.get("sustained_lateral_guard_release_frames", 10))
        )
        self.sustained_lateral_guard_max_abs_steer = float(
            ctrl_map.get("sustained_lateral_guard_max_abs_steer", 0.35)
        )
        self.sustained_lateral_guard_max_e_y_m = float(
            ctrl_map.get("sustained_lateral_guard_max_e_y_m", 0.8)
        )
        self.sustained_lateral_guard_max_e_psi_deg = float(
            ctrl_map.get("sustained_lateral_guard_max_e_psi_deg", 15.0)
        )
        self.sustained_lateral_guard_max_curvature = float(
            ctrl_map.get("sustained_lateral_guard_max_curvature", 0.01)
        )
        self.sustained_lateral_guard_min_speed_mps = float(
            ctrl_map.get("sustained_lateral_guard_min_speed_mps", 0.0)
        )
        self.sustained_lateral_guard_max_speed_mps = float(
            ctrl_map.get("sustained_lateral_guard_max_speed_mps", 20.0)
        )
        self.trajectory_contract_lateral_guard_enabled = bool(
            ctrl_map.get("trajectory_contract_lateral_guard_enabled", False)
        )
        self.trajectory_contract_lateral_guard_max_abs_steer = float(
            ctrl_map.get("trajectory_contract_lateral_guard_max_abs_steer", 0.05)
        )
        self.trajectory_contract_lateral_guard_max_speed_mps = float(
            ctrl_map.get("trajectory_contract_lateral_guard_max_speed_mps", 3.0)
        )
        self.trajectory_contract_lateral_guard_raw_threshold = float(
            ctrl_map.get("trajectory_contract_lateral_guard_raw_threshold", 0.95)
        )
        self.trajectory_contract_lateral_guard_max_planning_age_ms = float(
            ctrl_map.get("trajectory_contract_lateral_guard_max_planning_age_ms", 300.0)
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
        apollo_cfg = (cfg.get("algo", {}) or {}).get("apollo", {}) or {}
        self.transport_mode = str(apollo_cfg.get("transport_mode") or "ros2_gt").strip().lower() or "ros2_gt"
        if self.transport_mode not in {"ros2_gt", "carla_direct"}:
            raise RuntimeError(f"unsupported transport_mode={self.transport_mode}, expected ros2_gt|carla_direct")
        self.direct_bridge_cfg = dict(apollo_cfg.get("direct_bridge", {}) or {})
        self.require_no_ros2_runtime = bool(self.direct_bridge_cfg.get("require_no_ros2_runtime", False))
        self.route_command_mode = (
            str(self.direct_bridge_cfg.get("route_command_mode") or "cyber_direct").strip().lower()
            or "cyber_direct"
        )
        if self.route_command_mode not in {"cyber_direct", "dreamview"}:
            raise RuntimeError(
                f"unsupported direct_bridge.route_command_mode={self.route_command_mode}; "
                "expected cyber_direct|dreamview"
            )
        if self.transport_mode != "carla_direct":
            self.require_no_ros2_runtime = False
            self.route_command_mode = "cyber_direct"
        if self.transport_mode == "ros2_gt" and not ROS2_RUNTIME_IMPORT_OK:
            raise RuntimeError(
                "ROS2 imports failed for ros2_gt transport; source ROS2 first. "
                f"err={ROS2_RUNTIME_IMPORT_ERROR}"
            )
        if self.transport_mode == "carla_direct" and self.route_command_mode == "dreamview":
            raise RuntimeError(
                "direct_bridge.route_command_mode=dreamview is not implemented for Town01 direct candidate yet; "
                "use cyber_direct for transport A/B."
            )
        self.ros_ego_id = str(ros_cfg.get("ego_id", "hero"))
        self.odom_topic = str(ros_cfg["odom_topic"])
        self.objects3d_topic = str(ros_cfg["objects3d_topic"])
        self.objects_markers_topic = str(ros_cfg["objects_markers_topic"])
        self.objects_json_topic = str(ros_cfg["objects_json_topic"])
        self.control_out_topic = str(ros_cfg["control_out_topic"])
        self.control_out_type = str(ros_cfg.get("control_out_type", "ackermann"))
        self.gt_source = "ros2_gt"
        self.control_apply_path = "ros2_control_bridge"
        self.route_command_path = "cyber_direct_bridge_command_path"
        self.uses_ros2_gt = bool(self.transport_mode == "ros2_gt")
        self.uses_ros2_control_bridge = bool(self.transport_mode == "ros2_gt")
        self.requires_ros2_reexec = False
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
        self.auto_routing_start_nudge_m = float(auto_routing_cfg.get("start_nudge_m", 0.0))
        self.auto_routing_start_nudge_retry_step_m = float(
            auto_routing_cfg.get("start_nudge_retry_step_m", 0.0)
        )
        self.auto_routing_start_nudge_min_safe_m = float(
            auto_routing_cfg.get("start_nudge_min_safe_m", 0.0)
        )
        self.auto_routing_start_nudge_max_m = float(
            auto_routing_cfg.get("start_nudge_max_m", 0.0)
        )
        self.auto_routing_resend_sec = float(auto_routing_cfg.get("resend_sec", 5.0))
        self.auto_routing_max_attempts = int(auto_routing_cfg.get("max_attempts", 5))
        self.auto_routing_target_speed = float(auto_routing_cfg.get("target_speed_mps", 8.0))
        self.auto_routing_startup_delay_sec = float(auto_routing_cfg.get("startup_delay_sec", 3.0))
        self.auto_routing_startup_apollo_warmup_sec = max(
            0.0, float(auto_routing_cfg.get("startup_apollo_warmup_sec", 0.0))
        )
        self.auto_routing_lane_follow_refresh_sec = float(
            auto_routing_cfg.get("lane_follow_refresh_sec", auto_routing_cfg.get("command_refresh_sec", 1.0))
        )
        self.auto_routing_lane_follow_no_response_grace_sec = max(
            0.0, float(auto_routing_cfg.get("lane_follow_no_response_grace_sec", 0.0))
        )
        self.auto_routing_send_action = bool(auto_routing_cfg.get("send_action", False))
        self.auto_routing_send_lane_follow = bool(auto_routing_cfg.get("send_lane_follow", False))
        self.auto_routing_auto_enable_lane_follow_fallback = bool(
            auto_routing_cfg.get("auto_enable_lane_follow_fallback", True)
        )
        self.auto_routing_disable_lane_follow_on_no_response = bool(
            auto_routing_cfg.get("disable_lane_follow_on_no_response", True)
        )
        # Apollo planning requires an external command (lane_follow/action). If both are off,
        # planning stays in "planning_command not ready"; keep lane_follow on as a safe fallback.
        if (
            self.auto_routing_enabled
            and self.auto_routing_auto_enable_lane_follow_fallback
            and (not self.auto_routing_send_action)
            and (not self.auto_routing_send_lane_follow)
        ):
            self.auto_routing_send_lane_follow = True
            print(
                "[bridge][routing][warn] send_action=false and send_lane_follow=false; "
                "auto-enable lane_follow to provide planning_command"
            )
        self.auto_routing_send_routing = bool(auto_routing_cfg.get("send_routing_request", True))
        self.auto_routing_freeze_after_success = bool(auto_routing_cfg.get("freeze_after_success", True))
        self.auto_routing_freeze_after_long_route_success_only = bool(
            auto_routing_cfg.get("freeze_after_long_route_success_only", False)
        )
        self.auto_routing_use_seed_heading = bool(auto_routing_cfg.get("use_seed_heading", True))
        self.auto_routing_use_long_goal_after_move = bool(
            auto_routing_cfg.get("use_long_goal_after_move", True)
        )
        self.auto_routing_defer_long_goal_until_planning_ready = bool(
            auto_routing_cfg.get("defer_long_goal_until_planning_ready", False)
        )
        self.auto_routing_long_goal_planning_ready_min_nonempty_count = int(
            auto_routing_cfg.get("long_goal_planning_ready_min_nonempty_count", 1)
        )
        self.auto_routing_defer_long_goal_max_wait_sec = max(
            0.0, float(auto_routing_cfg.get("defer_long_goal_max_wait_sec", 0.0))
        )
        self.auto_routing_defer_long_goal_until_route_debug_ready = bool(
            auto_routing_cfg.get("defer_long_goal_until_route_debug_ready", False)
        )
        # These lane-snap helpers rely on geometric nearest-segment heuristics from map text.
        # Keep them configurable so users can disable them when heading/lane projection is suspicious.
        self.auto_routing_snap_start_to_lane = bool(auto_routing_cfg.get("snap_start_to_lane", False))
        self.auto_routing_snap_goal_to_lane = bool(auto_routing_cfg.get("snap_goal_to_lane", False))
        self.auto_routing_start_nudge_use_lane_heading = bool(
            auto_routing_cfg.get("start_nudge_use_lane_heading", False)
        )
        self.auto_routing_snap_source_mode = str(
            auto_routing_cfg.get("snap_source_mode", "lane_centerline_only") or "lane_centerline_only"
        ).strip().lower()
        self.auto_routing_snap_allow_untrusted_source = bool(
            auto_routing_cfg.get("snap_allow_untrusted_source", False)
        )
        self.auto_routing_snap_heading_diff_max_deg = float(
            auto_routing_cfg.get("snap_heading_diff_max_deg", 30.0)
        )
        self.auto_routing_snap_heading_diff_hard_reject_deg = float(
            auto_routing_cfg.get("snap_heading_diff_hard_reject_deg", 45.0)
        )
        self.auto_routing_lane_heading_nudge_max_heading_diff_deg = float(
            auto_routing_cfg.get("lane_heading_nudge_max_heading_diff_deg", 20.0)
        )
        self.auto_routing_disable_nudge_when_snap_rejected = bool(
            auto_routing_cfg.get("disable_nudge_when_snap_rejected", True)
        )
        self.auto_routing_goal_validity_check_enabled = bool(
            auto_routing_cfg.get("goal_validity_check_enabled", True)
        )
        self.auto_routing_goal_validity_fallback_enabled = bool(
            auto_routing_cfg.get("goal_validity_fallback_enabled", True)
        )
        self.auto_routing_skip_invalid_long_route = bool(
            auto_routing_cfg.get("skip_invalid_long_route", True)
        )
        self.auto_routing_suppress_long_phase_reroute_on_unstable_reference_line = bool(
            auto_routing_cfg.get("suppress_long_phase_reroute_on_unstable_reference_line", False)
        )
        self.auto_routing_goal_validity_reference_line_stale_sec = float(
            auto_routing_cfg.get("goal_validity_reference_line_stale_sec", 1.0)
        )
        self.auto_routing_wait_for_obstacle_gt_before_initial_routing = bool(
            auto_routing_cfg.get("wait_for_obstacle_gt_before_initial_routing", True)
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
        self._routing_freeze_active = False
        self._routing_last_request_phase = "startup"
        self._lane_follow_disabled_runtime = False
        self.auto_routing_fixed_goal_xy = self._parse_goal_point(auto_routing_cfg.get("fixed_goal_xy"))
        scenario_goal_path = str(auto_routing_cfg.get("scenario_goal_path") or "scenario_goal.json").strip()
        self.auto_routing_scenario_goal_path = Path(scenario_goal_path)
        if not self.auto_routing_scenario_goal_path.is_absolute():
            self.auto_routing_scenario_goal_path = (self.artifacts_dir / self.auto_routing_scenario_goal_path).resolve()
        self._first_odom_ts: Optional[float] = None
        self.map_bounds_xy: Optional[Tuple[float, float, float, float]] = None
        self.apollo_runtime_map_dir = str(bridge_cfg.get("apollo_runtime_map_dir", "") or "").strip()
        self.dreamview_selected_map = str(bridge_cfg.get("dreamview_selected_map", "") or "").strip()
        self.map_contract_invalid = bool(bridge_cfg.get("map_contract_invalid", False))
        self.map_contract_mismatch_reason = str(
            bridge_cfg.get("map_contract_mismatch_reason", "") or ""
        ).strip()
        self.map_contract_mismatch_classification = str(
            bridge_cfg.get("map_contract_mismatch_classification", "") or ""
        ).strip()
        self.map_contract_same_derivation_chain = bool(
            bridge_cfg.get("map_contract_same_derivation_chain", False)
        )
        self.apollo_map_identity_signature = str(
            bridge_cfg.get("apollo_map_identity_signature", "") or ""
        ).strip()
        host_container_map_path_mapping = bridge_cfg.get("host_container_map_path_mapping", {}) or {}
        self.host_container_map_path_mapping = dict(host_container_map_path_mapping)
        self._scenario_goal_cache: Optional[Dict[str, Any]] = None
        self._scenario_goal_mtime_ns: Optional[int] = None
        self._bridge_start_wall_sec = time.time()
        self._load_map_geometry()
        self._load_map_lane_metadata()
        self._load_map_bounds(auto_routing_cfg)

        traffic_light_cfg = (bridge_cfg.get("traffic_light", {}) or {})
        self.traffic_light_policy = str(traffic_light_cfg.get("policy", "force_green") or "force_green")
        default_force_ids = ["TL_signal14"] if self.traffic_light_policy == "force_green" else []
        self.traffic_light_force_ids = [str(item) for item in (traffic_light_cfg.get("force_ids") or default_force_ids)]
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
        self._planning_reader_enable_reason = "planning_proto_not_loaded"
        self._planning_message_type = "apollo.planning.ADCTrajectory"
        self._planning_msg_count = 0
        self._planning_nonempty_count = 0
        self._planning_empty_count = 0
        self._planning_last_points = 0
        self._planning_last_distance_to_destination: Optional[float] = None
        self._planning_last_msg_ts = 0.0
        self._planning_first_msg_ts_sec: Optional[float] = None
        self._planning_first_nonempty_ts_sec: Optional[float] = None
        self._planning_first_msg_last_reroute_ts_sec: Optional[float] = None
        self._planning_first_msg_last_routing_send_ts_sec: Optional[float] = None
        self._planning_first_route_debug_ts_sec: Optional[float] = None
        self._planning_first_route_debug_last_reroute_ts_sec: Optional[float] = None
        self._planning_first_route_debug_last_routing_send_ts_sec: Optional[float] = None
        self._planning_parse_fail_count = 0
        self._planning_parse_fail_reasons: Counter[str] = Counter()
        self._planning_point_counts: List[int] = []
        self._planning_debug_write_lock = threading.Lock()
        self._planning_recent_events: deque[Dict[str, Any]] = deque(maxlen=4096)
        self._planning_last_event: Optional[Dict[str, Any]] = None
        self._planning_last_route_debug_event: Optional[Dict[str, Any]] = None
        self._routing_first_response_ts_sec: Optional[float] = None
        self._routing_first_success_response_ts_sec: Optional[float] = None
        self._routing_last_response_ts_sec: Optional[float] = None
        self._routing_last_success_response_ts_sec: Optional[float] = None
        self._routing_first_response_after_last_routing_send_ts_sec: Optional[float] = None
        self._routing_first_response_after_last_routing_send_boundary_ts_sec: Optional[float] = None
        self._routing_first_success_response_after_last_routing_send_ts_sec: Optional[float] = None
        self._routing_first_success_response_after_last_routing_send_boundary_ts_sec: Optional[float] = None
        self._startup_geometry_records: List[Dict[str, Any]] = []
        self._latest_sim_time_sec: Optional[float] = None
        self._latest_wall_time_sec: Optional[float] = None
        self._latest_world_frame: Optional[int] = None
        self._latest_odom_stamp_sec: Optional[float] = None
        self._latest_odom_frame_id: str = ""
        control_bridge_cfg = (apollo_cfg.get("carla_control_bridge", {}) or {})
        self.control_bridge_sync_to_world_tick = bool(control_bridge_cfg.get("sync_to_world_tick", True))
        self.bridge_is_tick_owner = False
        self.bridge_tick_owner_model = "runner_harness_world_tick"
        self.bridge_timing_source_model = "ros_odom_header_stamp_for_sim_time"

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

        if self.transport_mode == "carla_direct":
            self.gt_source = "carla_world_snapshot_direct"
            self.control_apply_path = "bridge_direct_actor_apply"
            self.bridge_timing_source_model = "carla_snapshot_to_fabricated_odom_header"
            direct_poll_hz = float(self.direct_bridge_cfg.get("poll_hz", self.publish_rate_hz))
            direct_radius_m = float(self.direct_bridge_cfg.get("obstacle_radius_m", self.radius_m))
            direct_max_obstacles = int(self.direct_bridge_cfg.get("max_obstacles", self.max_obstacles))
            direct_host = str(
                self.direct_bridge_cfg.get("carla_host")
                or (carla_feedback_cfg.get("host", "127.0.0.1") if isinstance(carla_feedback_cfg, dict) else "127.0.0.1")
            )
            direct_port = int(
                self.direct_bridge_cfg.get("carla_port")
                or (carla_feedback_cfg.get("port", 2000) if isinstance(carla_feedback_cfg, dict) else 2000)
            )
            direct_role = str(
                self.direct_bridge_cfg.get("ego_role_name")
                or (carla_feedback_cfg.get("ego_role_name", self.ros_ego_id) if isinstance(carla_feedback_cfg, dict) else self.ros_ego_id)
            )
            self.node = CarlaDirectTransport(
                artifacts_dir=self.artifacts_dir,
                carla_host=direct_host,
                carla_port=direct_port,
                ego_role_name=direct_role,
                control_out_type=self.control_out_type,
                radius_m=direct_radius_m,
                max_obstacles=direct_max_obstacles,
                publish_rate_hz=direct_poll_hz,
                sync_to_world_tick=bool(control_bridge_cfg.get("sync_to_world_tick", True)),
                timeout_sec=float(control_bridge_cfg.get("timeout_sec", 0.8)),
                max_steer_angle=float(control_bridge_cfg.get("max_steer_angle", self.max_steer_angle)),
                speed_gain=float(control_bridge_cfg.get("speed_gain", self.speed_gain)),
                brake_gain=float(control_bridge_cfg.get("brake_gain", self.brake_gain)),
                watchdog_wait_for_first_msg=bool(control_bridge_cfg.get("watchdog_wait_for_first_msg", True)),
                watchdog_arm_delay_sec=float(control_bridge_cfg.get("watchdog_arm_delay_sec", 1.5)),
                startup_brake_suppression_enabled=bool(
                    control_bridge_cfg.get("startup_brake_suppression_enabled", True)
                ),
                startup_brake_suppression_speed_mps=float(
                    control_bridge_cfg.get("startup_brake_suppression_speed_mps", 1.0)
                ),
                startup_brake_suppression_max_brake=float(
                    control_bridge_cfg.get("startup_brake_suppression_max_brake", 0.2)
                ),
                startup_brake_suppression_min_throttle=float(
                    control_bridge_cfg.get("startup_brake_suppression_min_throttle", 0.3)
                ),
                startup_brake_suppression_hold_sec=float(
                    control_bridge_cfg.get("startup_brake_suppression_hold_sec", 3.0)
                ),
                startup_brake_recent_throttle_window_sec=float(
                    control_bridge_cfg.get("startup_brake_recent_throttle_window_sec", 1.0)
                ),
                route_command_mode=self.route_command_mode,
                require_no_ros2_runtime=self.require_no_ros2_runtime,
                client_timeout_sec=float(self.direct_bridge_cfg.get("client_timeout_sec", 5.0)),
            )
            self.executor = NoopExecutor()
            self.ros_thread = threading.Thread(target=lambda: None, name="carla_direct_transport", daemon=True)
        else:
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

        self.cyber.init(self.bridge_node_name)
        self.cyber_node = self.cyber.Node(self.bridge_node_name)
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
        if self.traffic_light_policy in {"force_green", "carla_actual"}:
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
            self._planning_reader_enable_reason = "enabled"
        elif self.planning_pb2 is None:
            self._planning_reader_enable_reason = "planning_pb2_import_failed"
            print(
                "[bridge][planning][warn] planning reader disabled: failed to import planning_pb2 "
                f"for channel {self.planning_channel}"
            )
        else:
            self._planning_reader_enable_reason = "ADCTrajectory_missing_in_planning_pb2"
            print(
                "[bridge][planning][warn] planning reader disabled: ADCTrajectory missing "
                f"for channel {self.planning_channel}"
            )
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
        self._write_planning_topic_debug_summary()

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

    def _write_csv_row(self, path: Path, row: Dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = list(row.keys())
        mode = "a" if self._split_csv_headers_written.get(str(path)) and path.exists() else "w"
        with path.open(mode, newline="") as fp:
            writer = csv.DictWriter(fp, fieldnames=fieldnames)
            if mode == "w" or not self._split_csv_headers_written.get(str(path), False):
                writer.writeheader()
                self._split_csv_headers_written[str(path)] = True
            writer.writerow(row)

    def _write_json_file(self, path: Path, payload: Dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2))
        tmp.replace(path)

    def _extract_ros_stamp_sec(self, msg: Any) -> Optional[float]:
        header = getattr(msg, "header", None)
        stamp = getattr(header, "stamp", None) if header is not None else None
        stamp_sec = _stamp_to_sec(stamp)
        return stamp_sec if math.isfinite(stamp_sec) and stamp_sec > 0.0 else None

    def _record_timing_from_odom(
        self,
        odom: Any,
        *,
        wall_time_sec: float,
        direct_world_frame: Optional[int] = None,
    ) -> None:
        self._latest_wall_time_sec = float(wall_time_sec)
        sim_time_sec = self._extract_ros_stamp_sec(odom)
        self._latest_sim_time_sec = sim_time_sec
        self._latest_odom_stamp_sec = sim_time_sec
        header = getattr(odom, "header", None)
        frame_id = getattr(header, "frame_id", "") if header is not None else ""
        self._latest_odom_frame_id = str(frame_id or "")
        world_frame: Optional[int] = None
        try:
            if direct_world_frame is not None:
                world_frame = int(direct_world_frame)
            else:
                direct_frame = getattr(odom, "_tb_world_frame", None)
                if direct_frame is not None:
                    world_frame = int(direct_frame)
        except Exception:
            world_frame = None
        try:
            if world_frame is not None:
                self._latest_world_frame = world_frame
                return
            seq_value = getattr(header, "seq", None) if header is not None else None
            if seq_value is not None:
                world_frame = int(seq_value)
        except Exception:
            world_frame = None
        self._latest_world_frame = world_frame

    def _timing_snapshot(
        self,
        *,
        event_wall_time_sec: Optional[float] = None,
        control_cycle_time_sec: Optional[float] = None,
        latest_planning_age_ms: Optional[float] = None,
    ) -> Dict[str, Any]:
        return build_timing_snapshot_impl(
            latest_sim_time_sec=self._latest_sim_time_sec,
            latest_wall_time_sec=self._latest_wall_time_sec,
            latest_world_frame=self._latest_world_frame,
            latest_odom_stamp_sec=self._latest_odom_stamp_sec,
            latest_odom_frame_id=self._latest_odom_frame_id,
            bridge_tick_owner_model=self.bridge_tick_owner_model,
            bridge_is_tick_owner=bool(self.bridge_is_tick_owner),
            control_bridge_sync_to_world_tick=bool(self.control_bridge_sync_to_world_tick),
            bridge_timing_source_model=self.bridge_timing_source_model,
            event_wall_time_sec=event_wall_time_sec,
            control_cycle_time_sec=control_cycle_time_sec,
            latest_planning_age_ms=latest_planning_age_ms,
        )

    def _record_routing_event(self, payload: Dict[str, Any]) -> None:
        self._append_jsonl(self.routing_event_debug_path, payload)

    def _record_goal_validity_event(self, payload: Dict[str, Any]) -> None:
        self._append_jsonl(self.goal_validity_debug_path, payload)

    def _record_obstacle_contract_event(self, payload: Dict[str, Any]) -> None:
        self._append_jsonl(self.obstacle_contract_debug_path, payload)

    def _record_lateral_guard_event(self, payload: Dict[str, Any]) -> None:
        self._append_jsonl(self.lateral_guard_debug_path, payload)

    def _record_reroute_decision_event(self, payload: Dict[str, Any]) -> None:
        self._append_jsonl(self.reroute_decision_debug_path, payload)
        self._append_jsonl(self.stage5_reroute_decision_debug_path, payload)

    def _projection_debug_summary(self, projection: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        return build_projection_debug_summary_impl(projection)

    def _seed_pose_debug_summary(
        self,
        *,
        current_x: float,
        current_y: float,
        current_yaw: float,
    ) -> Dict[str, Any]:
        return build_seed_pose_debug_summary_impl(
            auto_routing_seed_pose=self.auto_routing_seed_pose,
            auto_routing_use_seed_heading=bool(self.auto_routing_use_seed_heading),
            current_x=current_x,
            current_y=current_y,
            current_yaw=current_yaw,
        )

    def _planning_decision_snapshot(
        self,
        *,
        command_ts: Optional[float] = None,
    ) -> Dict[str, Any]:
        last_msg_age_sec = None
        if (
            command_ts is not None
            and self._planning_last_msg_ts is not None
            and math.isfinite(float(self._planning_last_msg_ts))
            and float(self._planning_last_msg_ts) > 0.0
        ):
            last_msg_age_sec = max(0.0, float(command_ts) - float(self._planning_last_msg_ts))
        last_route_debug = dict(self._planning_last_route_debug_event or {})
        last_route_debug_ts = _safe_float(last_route_debug.get("timestamp"))
        last_route_debug_age_sec = (
            max(0.0, float(command_ts) - float(last_route_debug_ts))
            if command_ts is not None and last_route_debug_ts is not None
            else None
        )
        last_routing_send_ts = _finite_or_none(self.auto_routing_last_routing_ts)
        route_debug_since_last_routing_send = self._planning_route_debug_window_summary(
            boundary_ts_sec=last_routing_send_ts,
            command_ts=command_ts,
        )
        route_debug_since_first_routing_response = self._planning_route_debug_window_summary(
            boundary_ts_sec=self._routing_first_response_after_last_routing_send_ts_sec,
            command_ts=command_ts,
        )
        return {
            "planning_msg_count": int(self._planning_msg_count),
            "planning_nonempty_count": int(self._planning_nonempty_count),
            "planning_empty_count": int(self._planning_empty_count),
            "planning_last_points": int(self._planning_last_points),
            "planning_last_distance_to_destination": _finite_or_none(
                self._planning_last_distance_to_destination
            ),
            "planning_first_nonempty_seen": bool(self._planning_first_nonempty_ts_sec is not None),
            "planning_last_msg_age_sec": _finite_or_none(last_msg_age_sec),
            "last_route_debug_event_seen": bool(last_route_debug),
            "last_route_debug_timestamp": _finite_or_none(last_route_debug_ts),
            "last_route_debug_age_sec": _finite_or_none(last_route_debug_age_sec),
            "last_route_debug_source": self._proto_scalar(last_route_debug.get("route_debug_source")),
            "last_route_debug_routing_lane_window_signature": self._proto_scalar(
                last_route_debug.get("routing_lane_window_signature")
            ),
            "last_route_debug_reference_line_count": int(
                last_route_debug.get("reference_line_count", 0) or 0
            ),
            "last_route_debug_route_segment_count": int(
                last_route_debug.get("route_segment_count", 0) or 0
            ),
            "last_route_debug_lane_follow_map_status": self._proto_scalar(
                last_route_debug.get("lane_follow_map_status")
            ),
            "route_debug_since_last_routing_send": route_debug_since_last_routing_send,
            "route_debug_since_first_routing_response_after_last_routing_send": (
                route_debug_since_first_routing_response
            ),
        }

    def _update_command_gate_state(
        self,
        *,
        ts_sec: float,
        phase: Optional[str],
        status: str,
        blocking_reason: str = "",
        blocking_detail: str = "",
        eligible: bool = False,
        ready_to_send: bool = False,
        route_ready: bool = False,
        lane_follow_ready: bool = False,
        route_cooldown_ok: bool = False,
        lane_follow_cooldown_ok: bool = False,
        route_attempts_left: bool = False,
        route_phase_sent: bool = False,
        lane_phase_sent: bool = False,
        send_routing_now: bool = False,
        send_lane_follow_now: bool = False,
        startup_delay_remaining_sec: Optional[float] = None,
        apollo_warmup_remaining_sec: Optional[float] = None,
    ) -> None:
        state = self._command_gate_state
        state["evaluation_count"] = int(state.get("evaluation_count", 0) or 0) + 1
        if state.get("first_eval_ts_sec") is None:
            state["first_eval_ts_sec"] = _finite_or_none(ts_sec)
        state["last_eval_ts_sec"] = _finite_or_none(ts_sec)
        if eligible:
            if state.get("first_eligible_ts_sec") is None:
                state["first_eligible_ts_sec"] = _finite_or_none(ts_sec)
            state["last_eligible_ts_sec"] = _finite_or_none(ts_sec)
        if ready_to_send:
            if state.get("first_ready_to_send_ts_sec") is None:
                state["first_ready_to_send_ts_sec"] = _finite_or_none(ts_sec)
            state["last_ready_to_send_ts_sec"] = _finite_or_none(ts_sec)
        state["last_phase"] = str(phase or "")
        state["last_status"] = str(status or "unknown")
        state["last_blocking_reason"] = str(blocking_reason or "")
        if blocking_reason and not state.get("first_blocking_reason"):
            state["first_blocking_reason"] = str(blocking_reason)
        state["last_blocking_detail"] = str(blocking_detail or "")
        state["route_ready"] = bool(route_ready)
        state["lane_follow_ready"] = bool(lane_follow_ready)
        state["route_cooldown_ok"] = bool(route_cooldown_ok)
        state["lane_follow_cooldown_ok"] = bool(lane_follow_cooldown_ok)
        state["route_attempts_left"] = bool(route_attempts_left)
        state["route_phase_sent"] = bool(route_phase_sent)
        state["lane_phase_sent"] = bool(lane_phase_sent)
        state["send_routing_now"] = bool(send_routing_now)
        state["send_lane_follow_now"] = bool(send_lane_follow_now)
        state["startup_delay_remaining_sec"] = _finite_or_none(startup_delay_remaining_sec)
        state["apollo_warmup_remaining_sec"] = _finite_or_none(apollo_warmup_remaining_sec)
        state["routing_request_count_at_last_eval"] = int(self.stats.get("routing_request_count", 0) or 0)
        state["planning_nonempty_count_at_last_eval"] = int(self._planning_nonempty_count)
        state["control_tx_count_at_last_eval"] = int(self.stats.get("control_tx_count", 0) or 0)
        state["last_error_snapshot"] = str(self.stats.get("last_error", "") or "")

    def _direct_transport_runtime_stats(self) -> Dict[str, Any]:
        node_stats = getattr(self.node, "stats", None)
        if isinstance(node_stats, dict):
            return dict(node_stats)
        return {}

    def _command_materialization_summary(self) -> Dict[str, Any]:
        gate = dict(self._command_gate_state)
        direct_stats = self._direct_transport_runtime_stats()
        routing_request_count = int(self.stats.get("routing_request_count", 0) or 0)
        routing_response_count = int(self.stats.get("routing_response_count", 0) or 0)
        routing_success_count = int(self.stats.get("routing_success_count", 0) or 0)
        planning_msg_count = int(self._planning_msg_count)
        planning_nonempty_count = int(self._planning_nonempty_count)
        control_rx_count = int(self.stats.get("control_rx_count", 0) or 0)
        control_tx_count = int(self.stats.get("control_tx_count", 0) or 0)
        direct_control_apply_count = int(direct_stats.get("control_apply_count", 0) or 0)
        direct_control_apply_frame_span = _finite_or_none(direct_stats.get("control_apply_frame_span"))
        speed_mps = _finite_or_none(self._latest_speed_mps)
        gate_reason = str(gate.get("last_blocking_reason") or "").strip()
        direct_last_error = str(direct_stats.get("last_error", "") or "").strip()
        stage = "unknown"
        layer = "unknown"
        reason = gate_reason or str(self.stats.get("last_error", "") or "").strip()

        if self._first_odom_ts is None:
            stage = "waiting_for_first_odometry"
            layer = "gt_ingress"
            reason = reason or direct_last_error or "first_odometry_not_seen"
        elif not self.auto_routing_enabled:
            stage = "auto_routing_disabled"
            layer = "command_path"
            reason = reason or "auto_routing_disabled"
        elif routing_request_count <= 0:
            if not bool(gate.get("route_ready")) and not bool(gate.get("lane_follow_ready")):
                stage = "command_interface_unavailable"
                layer = "command_path"
                reason = reason or "command_interface_unavailable"
            else:
                stage = "routing_not_sent"
                layer = "command_path"
                reason = reason or "routing_not_sent"
        elif routing_response_count <= 0:
            stage = "waiting_for_routing_response"
            layer = "routing_materialization"
            reason = reason or "routing_response_pending"
        elif routing_success_count <= 0:
            stage = "routing_response_empty_only"
            layer = "routing_materialization"
            reason = reason or "routing_response_empty_only"
        elif planning_msg_count <= 0:
            stage = "routing_established_waiting_for_planning"
            layer = "planning_materialization"
            reason = reason or "planning_message_pending"
        elif planning_nonempty_count <= 0:
            stage = "planning_empty_only"
            layer = "planning_materialization"
            reason = reason or "planning_empty_only"
        elif control_rx_count <= 0:
            stage = "planning_ready_no_control_rx"
            layer = "control_materialization"
            reason = reason or "control_message_pending"
        elif control_tx_count <= 0:
            stage = "control_rx_no_bridge_tx"
            layer = "control_materialization"
            reason = reason or "bridge_control_tx_pending"
        elif self.transport_mode == "carla_direct" and direct_control_apply_count <= 0:
            stage = "control_tx_no_direct_apply"
            layer = "actuation"
            reason = reason or direct_last_error or "direct_apply_pending"
        elif (
            self.transport_mode == "carla_direct"
            and direct_control_apply_count > 0
            and direct_control_apply_frame_span is not None
            and direct_control_apply_frame_span < 20
        ):
            stage = "control_applied_short_window"
            layer = "actuation_window"
            reason = reason or "direct_apply_window_too_short_for_behavior_judgment"
        elif speed_mps is not None and speed_mps <= 0.5:
            stage = "control_applied_no_motion"
            layer = "actuation"
            reason = reason or "vehicle_speed_not_rising"
        else:
            stage = "materialized"
            layer = "materialized"
            reason = reason or "routing_planning_control_alive"

        return {
            "summary_status": "provisional",
            "finalized_from_event_stream": False,
            "transport_mode": self.transport_mode,
            "route_command_mode": self.route_command_mode,
            "route_command_path": self.route_command_path,
            "command_path_stage": stage,
            "first_divergence_layer": layer,
            "first_divergence_reason": reason,
            "auto_routing_enabled": bool(self.auto_routing_enabled),
            "require_no_ros2_runtime": bool(self.require_no_ros2_runtime),
            "command_interfaces": {
                "routing_writer_ready": bool(self.routing_writer is not None),
                "lane_follow_client_ready": bool(self.lane_follow_client is not None),
                "action_client_ready": bool(self.action_client is not None),
                "routing_request_path_enabled": bool(self.auto_routing_send_routing),
                "lane_follow_path_enabled": bool(self.auto_routing_send_lane_follow),
                "action_path_enabled": bool(self.auto_routing_send_action),
                "lane_follow_disabled_runtime": bool(self._lane_follow_disabled_runtime),
            },
            "gate_state": gate,
            "observed_counters": {
                "routing_request_count": routing_request_count,
                "routing_response_count": routing_response_count,
                "routing_success_count": routing_success_count,
                "planning_message_count": planning_msg_count,
                "planning_nonempty_trajectory_count": planning_nonempty_count,
                "control_rx_count": control_rx_count,
                "control_tx_count": control_tx_count,
                "direct_control_apply_count": direct_control_apply_count,
            },
            "timing": {
                "first_odom_ts_sec": _finite_or_none(self._first_odom_ts),
                "first_routing_response_ts_sec": _finite_or_none(self._routing_first_response_ts_sec),
                "first_successful_routing_response_ts_sec": _finite_or_none(
                    self._routing_first_success_response_ts_sec
                ),
                "first_planning_message_ts_sec": _finite_or_none(self._planning_first_msg_ts_sec),
                "first_nonempty_planning_ts_sec": _finite_or_none(self._planning_first_nonempty_ts_sec),
            },
            "speed_mps_last": speed_mps,
            "last_error": str(self.stats.get("last_error", "") or ""),
            "direct_transport": {
                "ego_actor_id": direct_stats.get("ego_actor_id"),
                "connect_ok": bool(direct_stats.get("connect_ok", False)),
                "snapshot_count": int(direct_stats.get("snapshot_count", 0) or 0),
                "world_frame_repeat_count": int(direct_stats.get("world_frame_repeat_count", 0) or 0),
                "control_apply_count": direct_control_apply_count,
                "control_apply_fail_count": int(direct_stats.get("control_apply_fail_count", 0) or 0),
                "control_apply_first_frame": direct_stats.get("control_apply_first_frame"),
                "control_apply_last_frame": direct_stats.get("control_apply_last_frame"),
                "control_apply_frame_span": direct_stats.get("control_apply_frame_span"),
                "control_apply_max_throttle": _finite_or_none(direct_stats.get("control_apply_max_throttle")),
                "control_apply_max_speed_mps": _finite_or_none(
                    direct_stats.get("control_apply_max_speed_mps")
                ),
                "stale_world_frame_skip_count": int(
                    self.stats.get("direct_stale_world_frame_skip_count", 0) or 0
                ),
                "stale_world_frame_republish_count": int(
                    self.stats.get("direct_stale_world_frame_republish_count", 0) or 0
                ),
                "last_speed_mps": _finite_or_none(direct_stats.get("last_speed_mps")),
                "last_error": str(direct_stats.get("last_error", "") or ""),
            },
        }

    def _route_debug_row_visible(self, route_debug: Optional[Dict[str, Any]]) -> bool:
        route_debug = dict(route_debug or {})
        if not route_debug:
            return False
        if int(route_debug.get("reference_line_count", 0) or 0) > 0:
            return True
        if int(route_debug.get("route_segment_count", 0) or 0) > 0:
            return True
        if _finite_or_none(route_debug.get("reference_line_length")) is not None:
            return True
        if _finite_or_none(route_debug.get("route_segment_total_length")) is not None:
            return True
        routing_signature = str(route_debug.get("routing_lane_window_signature") or "").strip().lower()
        return bool(routing_signature and routing_signature != "none")

    def _planning_route_debug_window_summary(
        self,
        *,
        boundary_ts_sec: Optional[float],
        command_ts: Optional[float] = None,
    ) -> Dict[str, Any]:
        planning_rows: List[Dict[str, Any]] = []
        visible_route_debug_rows: List[Dict[str, Any]] = []
        for item in self._planning_recent_events:
            row = dict(item or {})
            row_ts = _finite_or_none(row.get("timestamp"))
            if boundary_ts_sec is not None and (row_ts is None or row_ts < float(boundary_ts_sec)):
                continue
            planning_rows.append(row)
            if self._route_debug_row_visible(row):
                visible_route_debug_rows.append(row)
        first_planning = dict(planning_rows[0]) if planning_rows else {}
        last_planning = dict(planning_rows[-1]) if planning_rows else {}
        first_visible = dict(visible_route_debug_rows[0]) if visible_route_debug_rows else {}
        last_visible = dict(visible_route_debug_rows[-1]) if visible_route_debug_rows else {}
        last_visible_ts = _finite_or_none(last_visible.get("timestamp"))
        last_planning_ts = _finite_or_none(last_planning.get("timestamp"))
        first_visible_ts = _finite_or_none(first_visible.get("timestamp"))
        first_planning_ts = _finite_or_none(first_planning.get("timestamp"))
        last_visible_age_sec = (
            max(0.0, float(command_ts) - float(last_visible_ts))
            if command_ts is not None and last_visible_ts is not None
            else None
        )
        last_planning_age_sec = (
            max(0.0, float(command_ts) - float(last_planning_ts))
            if command_ts is not None and last_planning_ts is not None
            else None
        )
        return {
            "boundary_ts_sec": _finite_or_none(boundary_ts_sec),
            "planning_msg_count": int(len(planning_rows)),
            "visible_route_debug_count": int(len(visible_route_debug_rows)),
            "visible_route_debug_seen": bool(visible_route_debug_rows),
            "first_planning_ts_sec": _finite_or_none(first_planning_ts),
            "last_planning_ts_sec": _finite_or_none(last_planning_ts),
            "first_planning_after_boundary_sec": _finite_delta_sec(first_planning_ts, boundary_ts_sec),
            "last_planning_after_boundary_sec": _finite_delta_sec(last_planning_ts, boundary_ts_sec),
            "last_planning_age_sec": _finite_or_none(last_planning_age_sec),
            "first_visible_route_debug_ts_sec": _finite_or_none(first_visible_ts),
            "last_visible_route_debug_ts_sec": _finite_or_none(last_visible_ts),
            "first_visible_route_debug_after_boundary_sec": _finite_delta_sec(
                first_visible_ts,
                boundary_ts_sec,
            ),
            "last_visible_route_debug_after_boundary_sec": _finite_delta_sec(
                last_visible_ts,
                boundary_ts_sec,
            ),
            "last_visible_route_debug_age_sec": _finite_or_none(last_visible_age_sec),
            "last_visible_route_debug_signature": self._proto_scalar(
                last_visible.get("routing_lane_window_signature")
            ),
            "last_visible_route_segment_count": int(last_visible.get("route_segment_count", 0) or 0),
            "last_visible_reference_line_count": int(
                last_visible.get("reference_line_count", 0) or 0
            ),
            "last_visible_lane_follow_map_status": self._proto_scalar(
                last_visible.get("lane_follow_map_status")
            ),
        }

    def _route_debug_observability_snapshot(
        self,
        route_debug: Optional[Dict[str, Any]],
        *,
        age_sec: Optional[float] = None,
    ) -> Dict[str, Any]:
        return build_route_debug_observability_snapshot_impl(
            route_debug,
            scalar_resolver=self._proto_scalar,
            age_sec=age_sec,
        )

    def _goal_validity_debug_summary(self, validity: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        validity = dict(validity or {})
        if not validity:
            return {}
        return {
            "timestamp": _finite_or_none(validity.get("timestamp")),
            "routing_phase": str(validity.get("routing_phase", "") or ""),
            "requested_goal_mode": str(validity.get("requested_goal_mode", "") or ""),
            "goal_mode": str(validity.get("goal_mode", "") or ""),
            "goal_source": str(validity.get("goal_source", "") or ""),
            "goal_distance_m": _finite_or_none(validity.get("goal_distance_m")),
            "goal_projection_available": bool(validity.get("goal_projection_available", False)),
            "goal_projection_distance_m": _finite_or_none(
                validity.get("goal_projection_distance_m")
            ),
            "reference_line_count": int(validity.get("reference_line_count", 0) or 0),
            "reference_line_length": _finite_or_none(validity.get("reference_line_length")),
            "remain_length_to_dest": _finite_or_none(validity.get("remain_length_to_dest")),
            "dest_beyond_reference_line": validity.get("dest_beyond_reference_line"),
            "route_segment_count": int(validity.get("route_segment_count", 0) or 0),
            "route_segment_total_length": _finite_or_none(
                validity.get("route_segment_total_length")
            ),
            "routing_lane_window_count": int(validity.get("routing_lane_window_count", 0) or 0),
            "routing_lane_window_signature": str(
                validity.get("routing_lane_window_signature", "") or ""
            ),
            "routing_unique_lane_count": int(validity.get("routing_unique_lane_count", 0) or 0),
            "routing_unique_lane_signature": str(
                validity.get("routing_unique_lane_signature", "") or ""
            ),
            "current_lane_id": self._proto_scalar(validity.get("current_lane_id")),
            "lane_id_first": self._proto_scalar(validity.get("lane_id_first")),
            "target_lane_id_first": self._proto_scalar(validity.get("target_lane_id_first")),
            "reference_line_provider_status": self._proto_scalar(
                validity.get("reference_line_provider_status")
            ),
            "create_route_segments_status": self._proto_scalar(
                validity.get("create_route_segments_status")
            ),
            "lane_follow_map_status": self._proto_scalar(
                validity.get("lane_follow_map_status")
            ),
            "recent_route_debug_present": bool(validity.get("recent_route_debug_present", False)),
            "recent_route_debug_fresh": bool(validity.get("recent_route_debug_fresh", False)),
            "recent_route_debug_timestamp": _finite_or_none(
                validity.get("recent_route_debug_timestamp")
            ),
            "recent_route_debug_source": self._proto_scalar(
                validity.get("recent_route_debug_source")
            ),
            "reference_line_debug_missing_but_trajectory_nonzero": bool(
                validity.get("reference_line_debug_missing_but_trajectory_nonzero", False)
            ),
            "recent_route_debug_age_sec": _finite_or_none(validity.get("recent_route_debug_age_sec")),
            "invalid_goal": bool(validity.get("invalid_goal", False)),
            "invalid_goal_reason": str(validity.get("invalid_goal_reason", "") or ""),
            "fallback_applied": bool(validity.get("fallback_applied", False)),
            "fallback_from_invalid_reason": str(
                validity.get("fallback_from_invalid_reason", "") or ""
            ),
        }

    def _lane_follow_map_inconsistent_active(self, route_debug: Optional[Dict[str, Any]] = None) -> bool:
        route_debug = dict(route_debug or self._planning_last_route_debug_event or {})
        status = str(route_debug.get("lane_follow_map_status", "") or "").strip().lower()
        return status in {
            "route_segments_present_reference_line_missing",
            "reference_line_missing",
            "parse_failed",
        }

    def _build_ahead_goal(
        self,
        x0: float,
        y0: float,
        z0: float,
        yaw0: float,
        *,
        anchor: Optional[Dict[str, float]],
        phase: str,
    ) -> Tuple[float, float, float, Dict[str, Any]]:
        effective_dist = max(self.auto_routing_min_end_ahead_m, float(self.auto_routing_end_ahead_m))
        if phase == "startup":
            effective_dist = _clamp(
                effective_dist,
                self.auto_routing_min_end_ahead_m,
                max(self.auto_routing_min_end_ahead_m, self.auto_routing_startup_end_ahead_m),
            )
        x1, y1, z1, anchor_mode = self._routing_end_point(
            x0,
            y0,
            z0,
            yaw0,
            dist_m=effective_dist,
            anchor=anchor,
            prefer_front_anchor=False,
        )
        return x1, y1, z1, {
            "requested_goal_mode": self.auto_routing_goal_mode,
            "goal_mode": "ego_seed_ahead",
            "goal_source": "invalid_goal_fallback_ahead",
            "startup_phase_used": phase == "startup",
            "end_ahead_m_effective": effective_dist,
            "anchor_mode": anchor_mode,
        }

    def _evaluate_goal_validity(
        self,
        *,
        x0: float,
        y0: float,
        x1: float,
        y1: float,
        phase: str,
        goal_meta: Dict[str, Any],
        goal_proj: Dict[str, Any],
        command_ts: float,
    ) -> Dict[str, Any]:
        recent = dict(self._planning_last_route_debug_event or {})
        recent_ts = _safe_float(recent.get("timestamp"))
        recent_age_sec = (command_ts - recent_ts) if recent_ts is not None else None
        recent_fresh = bool(
            recent_ts is not None
            and recent_age_sec is not None
            and recent_age_sec <= max(0.0, self.auto_routing_goal_validity_reference_line_stale_sec)
        )
        route_segment_total_length = _safe_float(recent.get("route_segment_total_length"))
        routing_lane_window_count = int(recent.get("routing_lane_window_count", 0) or 0)
        routing_lane_window_signature = str(recent.get("routing_lane_window_signature", "") or "")
        routing_unique_lane_count = int(recent.get("routing_unique_lane_count", 0) or 0)
        routing_unique_lane_signature = str(recent.get("routing_unique_lane_signature", "") or "")
        current_lane_id = self._proto_scalar(recent.get("current_lane_id"))
        lane_id_first = self._proto_scalar(recent.get("lane_id_first"))
        target_lane_id_first = self._proto_scalar(recent.get("target_lane_id_first"))
        reference_line_provider_status = self._proto_scalar(
            recent.get("reference_line_provider_status")
        )
        create_route_segments_status = self._proto_scalar(
            recent.get("create_route_segments_status")
        )
        lane_follow_map_status = self._proto_scalar(recent.get("lane_follow_map_status"))
        reference_line_count = int(recent.get("reference_line_count", 0) or 0)
        reference_line_length = _safe_float(recent.get("reference_line_length"))
        remain_length_to_dest = _safe_float(recent.get("remain_length_to_dest"))
        dest_beyond_reference_line = recent.get("dest_beyond_reference_line")
        route_segment_count = int(recent.get("route_segment_count", 0) or 0)
        reference_line_debug_missing_but_trajectory_nonzero = bool(
            recent.get("reference_line_debug_missing_but_trajectory_nonzero")
        )
        goal_projection_available = bool(goal_proj.get("available", False))
        goal_projection_distance_m = _safe_float(goal_proj.get("distance_m"))
        requested_mode = str(goal_meta.get("requested_goal_mode", self.auto_routing_goal_mode) or "")
        invalid_goal_reason = ""
        if self.auto_routing_goal_validity_check_enabled:
            if requested_mode in {"scenario_xy", "fixed_xy"} and self.auto_routing_snap_goal_to_lane and (not goal_projection_available):
                invalid_goal_reason = "goal_projection_unavailable"
            elif recent_fresh and dest_beyond_reference_line is True:
                invalid_goal_reason = "dest_beyond_reference_line_risk"
            elif (
                recent_fresh
                and reference_line_count <= 0
                and phase == "long"
                and not reference_line_debug_missing_but_trajectory_nonzero
            ):
                invalid_goal_reason = "reference_line_unavailable"
            elif recent_fresh and route_segment_count <= 0 and phase == "long":
                invalid_goal_reason = "route_segment_unavailable"
        validity = {
            "timestamp": command_ts,
            "routing_phase": phase,
            "requested_goal_mode": requested_mode,
            "goal_mode": str(goal_meta.get("goal_mode", "") or ""),
            "goal_source": str(goal_meta.get("goal_source", "") or ""),
            "goal_x": float(x1),
            "goal_y": float(y1),
            "start_x": float(x0),
            "start_y": float(y0),
            "goal_distance_m": math.hypot(float(x1) - float(x0), float(y1) - float(y0)),
            "goal_projection_available": goal_projection_available,
            "goal_projection_distance_m": goal_projection_distance_m,
            "reference_line_count": reference_line_count,
            "reference_line_length": reference_line_length,
            "remain_length_to_dest": remain_length_to_dest,
            "dest_beyond_reference_line": dest_beyond_reference_line,
            "route_segment_count": route_segment_count,
            "route_segment_total_length": route_segment_total_length,
            "routing_lane_window_count": routing_lane_window_count,
            "routing_lane_window_signature": routing_lane_window_signature,
            "routing_unique_lane_count": routing_unique_lane_count,
            "routing_unique_lane_signature": routing_unique_lane_signature,
            "current_lane_id": current_lane_id,
            "lane_id_first": lane_id_first,
            "target_lane_id_first": target_lane_id_first,
            "reference_line_provider_status": reference_line_provider_status,
            "create_route_segments_status": create_route_segments_status,
            "lane_follow_map_status": lane_follow_map_status,
            "recent_route_debug_present": bool(recent),
            "recent_route_debug_fresh": recent_fresh,
            "recent_route_debug_timestamp": recent_ts,
            "recent_route_debug_source": self._proto_scalar(recent.get("route_debug_source")),
            "reference_line_debug_missing_but_trajectory_nonzero": (
                reference_line_debug_missing_but_trajectory_nonzero
            ),
            "recent_route_debug_age_sec": recent_age_sec,
            "invalid_goal": bool(invalid_goal_reason),
            "invalid_goal_reason": invalid_goal_reason,
        }
        return validity

    def _routing_request_reason(
        self,
        *,
        phase: str,
        ignore_roll_active: bool,
        routing_skipped_due_to_freeze: bool,
        routing_skipped_due_to_invalid_goal: bool,
        routing_skipped_due_to_unstable_reference_line: bool,
        routing_waiting_for_route_debug_ready: bool,
    ) -> Tuple[str, str, str]:
        if routing_skipped_due_to_freeze:
            return "long_phase_route", "freeze_after_success_skip", "freeze_after_success_guard"
        if routing_skipped_due_to_invalid_goal:
            return "long_phase_route", "long_phase_invalid_goal_skip", "goal_validity_guard"
        if routing_waiting_for_route_debug_ready:
            return "long_phase_route", "long_phase_wait_route_debug_ready", "route_debug_readiness_guard"
        if routing_skipped_due_to_unstable_reference_line:
            return (
                "long_phase_route",
                "long_phase_unstable_reference_line_skip",
                "reference_line_guard",
            )
        if ignore_roll_active:
            return "explicit_reroute", "traffic_light_ignore_roll_refresh", "traffic_light_ignore_roll"
        if phase == "startup":
            if int(self.auto_routing_routing_sent) == 0:
                return "startup_route", "startup_initial_route", "auto_routing_startup"
            return "startup_route", "startup_route_refresh", "auto_routing_startup"
        if not self.auto_routing_long_routing_sent:
            return "long_phase_route", "long_phase_transition", "auto_routing_long_phase"
        return "explicit_reroute", "long_phase_refresh", "auto_routing_long_phase"

    def _resolve_nested_value(
        self, obj: Any, paths: Sequence[Sequence[str]], default: Any = None
    ) -> Any:
        for path in paths:
            cur = obj
            ok = True
            for attr in path:
                if cur is None or not hasattr(cur, attr):
                    ok = False
                    break
                cur = getattr(cur, attr)
            if ok and cur is not None:
                return cur
        return default

    def _proto_scalar(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, (bool, int, float, str)):
            return value
        text = str(value).strip()
        return text or None

    def _enum_name_from_msg(self, msg: Any, field_name: str, value: Any) -> Any:
        if value is None:
            return None
        try:
            field_desc = getattr(getattr(msg, "DESCRIPTOR", None), "fields_by_name", {}).get(field_name)
            if field_desc is not None and getattr(field_desc, "enum_type", None) is not None:
                enum_value = field_desc.enum_type.values_by_number.get(int(value))
                if enum_value is not None:
                    return enum_value.name
        except Exception:
            pass
        return value

    def _trajectory_point_xy(self, point: Any) -> Tuple[Optional[float], Optional[float]]:
        x = self._resolve_nested_float(
            point,
            (
                ("path_point", "x"),
                ("x",),
            ),
        )
        y = self._resolve_nested_float(
            point,
            (
                ("path_point", "y"),
                ("y",),
            ),
        )
        return x, y

    def _header_timestamp_sec(self, obj: Any) -> Optional[float]:
        return self._resolve_nested_float(
            obj,
            (
                ("header", "timestamp_sec"),
                ("timestamp_sec",),
            ),
        )

    def _header_sequence_num(self, obj: Any) -> Optional[int]:
        value = self._resolve_nested_value(
            obj,
            (
                ("header", "sequence_num"),
                ("sequence_num",),
            ),
        )
        try:
            if value is None:
                return None
            return int(value)
        except Exception:
            return None

    def _trajectory_time_bounds(self, points: Any) -> Tuple[Optional[float], Optional[float]]:
        rel_times: List[float] = []
        if points is None:
            return None, None
        for point in list(points):
            rel = self._resolve_nested_float(point, (("relative_time",),))
            if rel is not None and math.isfinite(rel):
                rel_times.append(float(rel))
        if not rel_times:
            return None, None
        return min(rel_times), max(rel_times)

    def _planning_reference_line_lengths(self, msg: Any) -> List[float]:
        lines = self._resolve_nested_value(
            msg,
            (
                ("debug", "planning_data", "reference_line"),
                ("debug", "reference_line"),
            ),
            default=[],
        )
        out: List[float] = []
        try:
            for item in list(lines or []):
                length = self._resolve_nested_float(item, (("length",),))
                if length is not None and math.isfinite(length):
                    out.append(float(length))
        except Exception:
            return []
        return out

    def _planning_routing_counts(self, msg: Any) -> Tuple[int, int, int]:
        routing = self._resolve_nested_value(
            msg,
            (
                ("debug", "planning_data", "routing"),
                ("routing",),
            ),
        )
        roads = getattr(routing, "road", None) if routing is not None else None
        road_count = len(roads) if roads is not None else 0
        passage_count = 0
        segment_count = 0
        try:
            for road in list(roads or []):
                passages = getattr(road, "passage", None)
                if passages is None:
                    continue
                passage_count += len(passages)
                for passage in list(passages):
                    segments = getattr(passage, "segment", None)
                    if segments is not None:
                        segment_count += len(segments)
        except Exception:
            return road_count, passage_count, segment_count
        return road_count, passage_count, segment_count

    def _planning_routing_total_length(self, msg: Any) -> Optional[float]:
        routing = self._resolve_nested_value(
            msg,
            (
                ("debug", "planning_data", "routing"),
                ("routing",),
            ),
        )
        roads = getattr(routing, "road", None) if routing is not None else None
        total = 0.0
        has_length = False
        try:
            for road in list(roads or []):
                passages = getattr(road, "passage", None)
                if passages is None:
                    continue
                for passage in list(passages):
                    segments = getattr(passage, "segment", None)
                    if segments is None:
                        continue
                    for segment in list(segments):
                        length = self._resolve_nested_float(segment, (("length",),))
                        if length is None:
                            start_s = self._resolve_nested_float(segment, (("start_s",),))
                            end_s = self._resolve_nested_float(segment, (("end_s",),))
                            if start_s is not None and end_s is not None:
                                length = max(0.0, float(end_s) - float(start_s))
                        if length is not None and math.isfinite(length):
                            total += float(length)
                            has_length = True
        except Exception:
            return total if has_length else None
        return total if has_length else None

    def _planning_routing_lane_windows(self, msg: Any) -> List[Dict[str, Any]]:
        routing = self._resolve_nested_value(
            msg,
            (
                ("debug", "planning_data", "routing"),
                ("routing",),
            ),
        )
        roads = getattr(routing, "road", None) if routing is not None else None
        rows: List[Dict[str, Any]] = []
        try:
            for road_index, road in enumerate(list(roads or [])):
                road_id = self._proto_scalar(self._resolve_nested_value(road, (("id",),)))
                passages = getattr(road, "passage", None)
                if passages is None:
                    continue
                for passage_index, passage in enumerate(list(passages)):
                    segments = getattr(passage, "segment", None)
                    if segments is None:
                        continue
                    for segment_index, segment in enumerate(list(segments)):
                        lane_id = self._proto_scalar(self._resolve_nested_value(segment, (("id",),)))
                        start_s = self._resolve_nested_float(segment, (("start_s",),))
                        end_s = self._resolve_nested_float(segment, (("end_s",),))
                        length = self._resolve_nested_float(segment, (("length",),))
                        if length is None and start_s is not None and end_s is not None:
                            length = max(0.0, float(end_s) - float(start_s))
                        rows.append(
                            {
                                "road_index": int(road_index),
                                "road_id": road_id,
                                "passage_index": int(passage_index),
                                "segment_index": int(segment_index),
                                "lane_id": lane_id,
                                "start_s": start_s,
                                "end_s": end_s,
                                "length": length,
                            }
                        )
        except Exception:
            return rows
        return rows

    def _planning_routing_lane_window_summary(
        self,
        msg: Any,
        *,
        max_items: int = 8,
    ) -> Tuple[int, int, str, str]:
        def _fmt_window_bound(value: Any) -> str:
            try:
                if value is None:
                    return ""
                out = float(value)
            except Exception:
                return ""
            if not math.isfinite(out):
                return ""
            return f"{out:.4f}"

        rows = self._planning_routing_lane_windows(msg)
        unique_lane_ids: List[str] = []
        seen_lane_ids = set()
        window_parts: List[str] = []
        for row in rows[: max(1, int(max_items))]:
            lane_id = str(row.get("lane_id") or "").strip() or "unknown_lane"
            start_s = _fmt_window_bound(row.get("start_s"))
            end_s = _fmt_window_bound(row.get("end_s"))
            if start_s or end_s:
                window_parts.append(f"{lane_id}@{start_s}->{end_s}")
            else:
                window_parts.append(lane_id)
        if len(rows) > max_items:
            window_parts.append(f"...(+{len(rows) - max_items} more)")
        for row in rows:
            lane_id = str(row.get("lane_id") or "").strip()
            if not lane_id or lane_id in seen_lane_ids:
                continue
            seen_lane_ids.add(lane_id)
            unique_lane_ids.append(lane_id)
        unique_lane_parts = list(unique_lane_ids[: max(1, int(max_items))])
        if len(unique_lane_ids) > max_items:
            unique_lane_parts.append(f"...(+{len(unique_lane_ids) - max_items} more)")
        return (
            len(rows),
            len(unique_lane_ids),
            (" | ".join(window_parts) if window_parts else "none"),
            (" | ".join(unique_lane_parts) if unique_lane_parts else "none"),
        )

    def _planning_stage_info(self, msg: Any) -> Tuple[Optional[str], Optional[str]]:
        scenario = self._resolve_nested_value(msg, (("debug", "planning_data", "scenario"),))
        scenario_name = self._proto_scalar(
            self._resolve_nested_value(
                scenario,
                (
                    ("scenario_plugin_type",),
                    ("msg",),
                ),
            )
        )
        stage_name = self._proto_scalar(
            self._resolve_nested_value(
                scenario,
                (
                    ("stage_plugin_type",),
                    ("stage_type",),
                ),
            )
        )
        return (
            str(scenario_name) if scenario_name not in (None, "") else None,
            str(stage_name) if stage_name not in (None, "") else None,
        )

    def _estimate_ego_front_to_center(self) -> Optional[float]:
        carla_vehicle = self.stats.get("carla_vehicle", {}) or {}
        value = _finite_or_none(carla_vehicle.get("front_edge_to_center"))
        if value is not None:
            return float(value)
        return None

    def _latest_planning_event_for_seq(self, sequence_num: Optional[int]) -> Optional[Dict[str, Any]]:
        if sequence_num is None:
            return dict(self._planning_last_event) if self._planning_last_event is not None else None
        for item in reversed(self._planning_recent_events):
            if int(item.get("planning_header_sequence_num") or -1) == int(sequence_num):
                return dict(item)
        return dict(self._planning_last_event) if self._planning_last_event is not None else None

    def _exact_planning_event_for_seq(self, sequence_num: Optional[int]) -> Optional[Dict[str, Any]]:
        if sequence_num is None:
            return None
        for item in reversed(self._planning_recent_events):
            if int(item.get("planning_header_sequence_num") or -1) == int(sequence_num):
                return dict(item)
        return None

    def _planning_timing_summary(self) -> Dict[str, Any]:
        first_msg_after_last_routing_send_sec = _finite_delta_sec(
            self._planning_first_msg_ts_sec,
            self._planning_first_msg_last_routing_send_ts_sec,
        )
        first_route_debug_after_last_routing_send_sec = _finite_delta_sec(
            self._planning_first_route_debug_ts_sec,
            self._planning_first_route_debug_last_routing_send_ts_sec,
        )
        routing_first_response_after_last_routing_send_sec = _finite_delta_sec(
            self._routing_first_response_after_last_routing_send_ts_sec,
            self._routing_first_response_after_last_routing_send_boundary_ts_sec,
        )
        routing_first_success_response_after_last_routing_send_sec = _finite_delta_sec(
            self._routing_first_success_response_after_last_routing_send_ts_sec,
            self._routing_first_success_response_after_last_routing_send_boundary_ts_sec,
        )
        return {
            "last_reroute_boundary_semantics": "compat_alias_for_last_routing_send",
            "first_msg_ts_sec": self._planning_first_msg_ts_sec,
            "first_msg_last_reroute_ts_sec": self._planning_first_msg_last_reroute_ts_sec,
            "first_msg_last_routing_send_ts_sec": self._planning_first_msg_last_routing_send_ts_sec,
            "first_msg_after_last_reroute_sec": first_msg_after_last_routing_send_sec,
            "first_msg_after_last_routing_send_sec": first_msg_after_last_routing_send_sec,
            "first_route_debug_ts_sec": self._planning_first_route_debug_ts_sec,
            "first_route_debug_last_reroute_ts_sec": self._planning_first_route_debug_last_reroute_ts_sec,
            "first_route_debug_last_routing_send_ts_sec": (
                self._planning_first_route_debug_last_routing_send_ts_sec
            ),
            "first_route_debug_after_last_reroute_sec": first_route_debug_after_last_routing_send_sec,
            "first_route_debug_after_last_routing_send_sec": first_route_debug_after_last_routing_send_sec,
            "routing_first_response_ts_sec": self._routing_first_response_ts_sec,
            "routing_first_success_response_ts_sec": self._routing_first_success_response_ts_sec,
            "routing_last_response_ts_sec": self._routing_last_response_ts_sec,
            "routing_last_success_response_ts_sec": self._routing_last_success_response_ts_sec,
            "routing_first_response_after_last_routing_send_boundary_ts_sec": (
                self._routing_first_response_after_last_routing_send_boundary_ts_sec
            ),
            "routing_first_response_after_last_routing_send_ts_sec": (
                self._routing_first_response_after_last_routing_send_ts_sec
            ),
            "routing_first_response_after_last_routing_send_sec": (
                routing_first_response_after_last_routing_send_sec
            ),
            "routing_first_success_response_after_last_routing_send_boundary_ts_sec": (
                self._routing_first_success_response_after_last_routing_send_boundary_ts_sec
            ),
            "routing_first_success_response_after_last_routing_send_ts_sec": (
                self._routing_first_success_response_after_last_routing_send_ts_sec
            ),
            "routing_first_success_response_after_last_routing_send_sec": (
                routing_first_success_response_after_last_routing_send_sec
            ),
            "first_msg_after_first_routing_response_after_last_routing_send_sec": _finite_delta_sec(
                self._planning_first_msg_ts_sec,
                self._routing_first_response_after_last_routing_send_ts_sec,
            ),
            "first_msg_after_first_successful_routing_response_after_last_routing_send_sec": (
                _finite_delta_sec(
                    self._planning_first_msg_ts_sec,
                    self._routing_first_success_response_after_last_routing_send_ts_sec,
                )
            ),
            "first_route_debug_after_first_routing_response_after_last_routing_send_sec": (
                _finite_delta_sec(
                    self._planning_first_route_debug_ts_sec,
                    self._routing_first_response_after_last_routing_send_ts_sec,
                )
            ),
            "first_route_debug_after_first_successful_routing_response_after_last_routing_send_sec": (
                _finite_delta_sec(
                    self._planning_first_route_debug_ts_sec,
                    self._routing_first_success_response_after_last_routing_send_ts_sec,
                )
            ),
        }

    def _planning_topic_debug_summary_payload(self) -> Dict[str, Any]:
        point_counts = [int(v) for v in self._planning_point_counts]
        parse_fail_topk = [
            {"reason": reason, "count": count}
            for reason, count in self._planning_parse_fail_reasons.most_common(5)
        ]
        last_event = self._planning_last_event or {}
        timing_summary = self._planning_timing_summary()
        return {
            "generated_at_unix_sec": time.time(),
            "summary_status": "provisional",
            "finalized_from_event_stream": False,
            "planning_reader_enabled": bool(self._planning_reader_enabled),
            "planning_reader_enable_reason": str(self._planning_reader_enable_reason or ""),
            "topic_name": self.planning_channel,
            "message_type": self._planning_message_type,
            "total_messages_received": int(self._planning_msg_count),
            "messages_with_nonzero_trajectory_points": int(self._planning_nonempty_count),
            "messages_with_zero_trajectory_points": int(self._planning_empty_count),
            "max_trajectory_point_count": (max(point_counts) if point_counts else 0),
            "mean_trajectory_point_count": (
                (sum(point_counts) / float(len(point_counts))) if point_counts else 0.0
            ),
            **timing_summary,
            "first_nonzero_trajectory_timestamp": self._planning_first_nonempty_ts_sec,
            "parse_fail_count": int(self._planning_parse_fail_count),
            "parse_fail_reasons_topk": parse_fail_topk,
            "last_trajectory_point_count": int(self._planning_last_points),
            "last_msg_ts_sec": self._planning_last_msg_ts or None,
            "last_distance_to_destination": self._planning_last_distance_to_destination,
            "last_routing_total_length": _finite_or_none(last_event.get("routing_total_length")),
            "last_routing_lane_window_count": int(last_event.get("routing_lane_window_count", 0) or 0),
            "last_routing_lane_window_signature": str(last_event.get("routing_lane_window_signature") or "none"),
            "last_routing_unique_lane_count": int(last_event.get("routing_unique_lane_count", 0) or 0),
            "last_routing_unique_lane_signature": str(
                last_event.get("routing_unique_lane_signature") or "none"
            ),
            "last_planning_header_sequence_num": (
                int(last_event.get("planning_header_sequence_num"))
                if last_event.get("planning_header_sequence_num") is not None
                else None
            ),
        }

    def _write_planning_topic_debug_summary(self) -> None:
        with self._planning_debug_write_lock:
            payload = self._planning_topic_debug_summary_payload()
            self.stats["planning_topic_debug"] = payload
            self._write_json_file(self.planning_topic_debug_summary_path, payload)

    def _lane_projection_probe(self, x: float, y: float) -> Dict[str, Any]:
        map_file = str(self.map_geometry_source_file or self.map_file_path or "")
        if not self.map_segments:
            return {
                "available": False,
                "applied": False,
                "reason": "no_map_segments",
                "map_file": map_file,
                "source_type": self.map_geometry_source_type,
                "trusted_lane_centerline": bool(self.map_geometry_trusted_lane_centerline),
            }
        metrics = _nearest_segment_metrics(x, y, self.map_segments)
        if metrics is None:
            return {
                "available": False,
                "applied": False,
                "reason": "no_segment_match",
                "map_file": map_file,
                "source_type": self.map_geometry_source_type,
                "trusted_lane_centerline": bool(self.map_geometry_trusted_lane_centerline),
            }
        snapped_x = float(metrics["proj_x"])
        snapped_y = float(metrics["proj_y"])
        return {
            "available": True,
            "applied": False,
            "distance_m": float(metrics["dist"]),
            "proj_x": snapped_x,
            "proj_y": snapped_y,
            "lane_yaw_deg": math.degrees(float(metrics["seg_yaw"])),
            "signed_e_y_m": float(metrics["signed_e_y"]),
            "map_file": map_file,
            "source_type": self.map_geometry_source_type,
            "trusted_lane_centerline": bool(self.map_geometry_trusted_lane_centerline),
        }

    def _startup_geometry_summary_payload(self) -> Dict[str, Any]:
        sent = [r for r in self._startup_geometry_records if bool(r.get("routing_request_sent"))]
        skipped = [r for r in self._startup_geometry_records if bool(r.get("routing_skipped_due_to_freeze"))]
        suspicious = [r for r in self._startup_geometry_records if bool(r.get("suspicious_snap"))]
        rejected = [r for r in self._startup_geometry_records if bool(r.get("snap_rejected"))]
        suspicious_rejected = [
            r for r in self._startup_geometry_records if bool(r.get("suspicious_snap_rejected"))
        ]
        accepted_heading = [
            abs(float(r["heading_diff_to_vehicle_deg"]))
            for r in self._startup_geometry_records
            if bool(r.get("snap_applied")) and _finite_or_none(r.get("heading_diff_to_vehicle_deg")) is not None
        ]
        loc_to_final = [
            float(r["localization_to_final_start_distance_m"])
            for r in self._startup_geometry_records
            if _finite_or_none(r.get("localization_to_final_start_distance_m")) is not None
        ]
        raw_to_snap = [
            float(r["raw_start_to_snapped_start_distance_m"])
            for r in self._startup_geometry_records
            if _finite_or_none(r.get("raw_start_to_snapped_start_distance_m")) is not None
        ]
        heading_diff = [
            abs(float(r["heading_diff_vehicle_vs_snap_lane_deg"]))
            for r in self._startup_geometry_records
            if _finite_or_none(r.get("heading_diff_vehicle_vs_snap_lane_deg")) is not None
        ]
        return {
            "generated_at_unix_sec": time.time(),
            "summary_status": "provisional",
            "finalized_from_event_stream": False,
            "record_count": len(self._startup_geometry_records),
            "routing_request_sent_count": len(sent),
            "routing_skipped_due_to_freeze_count": len(skipped),
            "suspicious_snap_count": len(suspicious),
            "snap_rejected_count": len(rejected),
            "suspicious_snap_rejected_count": len(suspicious_rejected),
            "routing_phase_counts": dict(self.stats.get("routing_phase_counts", {})),
            "freeze_after_success_config": bool(self.auto_routing_freeze_after_success),
            "freeze_after_success_effective": bool(self._routing_freeze_active),
            "map_geometry": dict(self.stats.get("map_geometry", {}) or {}),
            "distance_stats": {
                "localization_to_final_start_distance_m": _summary_stats(loc_to_final),
                "raw_start_to_snapped_start_distance_m": _summary_stats(raw_to_snap),
                "heading_diff_vehicle_vs_snap_lane_deg_abs": _summary_stats(heading_diff),
                "accepted_snap_heading_diff_to_vehicle_deg_abs": _summary_stats(accepted_heading),
            },
            "first_record": dict(self._startup_geometry_records[0]) if self._startup_geometry_records else {},
            "last_record": dict(self._startup_geometry_records[-1]) if self._startup_geometry_records else {},
            "routing_event_debug_path": str(self.routing_event_debug_path),
            "goal_validity_debug_path": str(self.goal_validity_debug_path),
        }

    def _write_startup_geometry_summary(self) -> None:
        payload = self._startup_geometry_summary_payload()
        self.stats["startup_geometry"] = payload
        self._write_json_file(self.startup_geometry_summary_path, payload)

    def _record_startup_geometry_event(self, payload: Dict[str, Any]) -> None:
        self._startup_geometry_records.append(dict(payload))
        self._append_jsonl(self.startup_geometry_debug_path, payload)
        self._write_startup_geometry_summary()

    def _format_xy(self, x: Optional[float], y: Optional[float]) -> str:
        if x is None or y is None or (not math.isfinite(float(x))) or (not math.isfinite(float(y))):
            return "n/a"
        return f"({float(x):.2f},{float(y):.2f})"

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
        timing_summary = self._planning_timing_summary()
        return {
            "reader_enabled": self._planning_reader_enabled,
            "reader_enable_reason": self._planning_reader_enable_reason,
            "topic_name": self.planning_channel,
            "message_type": self._planning_message_type,
            "summary_status": "provisional",
            "finalized_from_event_stream": False,
            "msg_count": self._planning_msg_count,
            "nonempty_trajectory_count": self._planning_nonempty_count,
            "empty_trajectory_count": self._planning_empty_count,
            "last_trajectory_point_count": self._planning_last_points,
            "last_distance_to_destination": self._planning_last_distance_to_destination,
            "last_msg_ts_sec": self._planning_last_msg_ts,
            "first_nonempty_ts_sec": self._planning_first_nonempty_ts_sec,
            "last_reroute_boundary_semantics": timing_summary["last_reroute_boundary_semantics"],
            "first_msg_ts_sec": timing_summary["first_msg_ts_sec"],
            "first_msg_last_reroute_ts_sec": timing_summary["first_msg_last_reroute_ts_sec"],
            "first_msg_last_routing_send_ts_sec": timing_summary["first_msg_last_routing_send_ts_sec"],
            "first_msg_after_last_reroute_sec": timing_summary["first_msg_after_last_reroute_sec"],
            "first_msg_after_last_routing_send_sec": timing_summary["first_msg_after_last_routing_send_sec"],
            "first_route_debug_ts_sec": timing_summary["first_route_debug_ts_sec"],
            "first_route_debug_last_reroute_ts_sec": timing_summary["first_route_debug_last_reroute_ts_sec"],
            "first_route_debug_last_routing_send_ts_sec": (
                timing_summary["first_route_debug_last_routing_send_ts_sec"]
            ),
            "first_route_debug_after_last_reroute_sec": timing_summary["first_route_debug_after_last_reroute_sec"],
            "first_route_debug_after_last_routing_send_sec": (
                timing_summary["first_route_debug_after_last_routing_send_sec"]
            ),
            "routing_first_response_ts_sec": timing_summary["routing_first_response_ts_sec"],
            "routing_first_success_response_ts_sec": timing_summary["routing_first_success_response_ts_sec"],
            "routing_first_response_after_last_routing_send_ts_sec": (
                timing_summary["routing_first_response_after_last_routing_send_ts_sec"]
            ),
            "routing_first_success_response_after_last_routing_send_ts_sec": (
                timing_summary["routing_first_success_response_after_last_routing_send_ts_sec"]
            ),
            "parse_fail_count": self._planning_parse_fail_count,
            "last_planning_header_sequence_num": (
                (self._planning_last_event or {}).get("planning_header_sequence_num")
            ),
            "planning_topic_debug_summary_path": str(self.planning_topic_debug_summary_path),
            "planning_route_segment_debug_path": str(self.planning_route_segment_debug_path),
            "goal_validity_debug_path": str(self.goal_validity_debug_path),
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

    def _apollo_traffic_light_color_value(self, color_name: str) -> int:
        normalized = str(color_name or "").strip().upper()
        if normalized not in {"RED", "YELLOW", "GREEN", "BLACK", "UNKNOWN"}:
            normalized = "UNKNOWN"
        pb_enum_owner = getattr(self.traffic_light_pb2, "TrafficLight", None) if self.traffic_light_pb2 else None
        return int(
            _enum_attr(pb_enum_owner, (normalized,), None)
            or _enum_attr(self.traffic_light_pb2, (normalized,), None)
            or {"UNKNOWN": 0, "RED": 1, "YELLOW": 2, "GREEN": 3, "BLACK": 4}[normalized]
        )

    def _traffic_light_actor_signal_ids(self, actor: Any) -> List[str]:
        candidates: List[str] = []
        opendrive_id = None
        if hasattr(actor, "get_opendrive_id"):
            try:
                opendrive_id = actor.get_opendrive_id()
            except Exception:
                opendrive_id = None
        raw_text = str(opendrive_id or "").strip()
        if raw_text:
            candidates.append(raw_text)
            lower_text = raw_text.lower()
            if lower_text.startswith("signal"):
                candidates.append(f"TL_{raw_text}")
            elif lower_text.isdigit():
                candidates.extend([f"signal{raw_text}", f"TL_signal{raw_text}"])
        actor_id = getattr(actor, "id", None)
        if actor_id is not None:
            candidates.extend([str(actor_id), f"carla_actor_{actor_id}"])
        ordered: List[str] = []
        seen = set()
        for item in candidates:
            text = str(item or "").strip()
            if not text or text in seen:
                continue
            ordered.append(text)
            seen.add(text)
        return ordered

    def _carla_traffic_light_color_name(self, actor: Any) -> str:
        state = None
        if hasattr(actor, "get_state"):
            try:
                state = actor.get_state()
            except Exception:
                state = None
        raw_text = str(state or "").strip().upper()
        if raw_text.endswith("RED"):
            return "RED"
        if raw_text.endswith("YELLOW"):
            return "YELLOW"
        if raw_text.endswith("GREEN"):
            return "GREEN"
        if raw_text.endswith("OFF"):
            return "BLACK"
        return "UNKNOWN"

    def _current_carla_actual_traffic_lights(self) -> List[Tuple[str, str]]:
        if self.world is None and self.client is not None:
            try:
                self.world = self.client.get_world()
            except Exception:
                self.world = None
        if self.world is None:
            return []
        try:
            actors = list(self.world.get_actors().filter("traffic.traffic_light*"))
        except Exception:
            return []
        configured_ids = {
            str(item).strip()
            for item in list(self.traffic_light_force_ids or [])
            if str(item).strip()
        }
        resolved: Dict[str, str] = {}
        for actor in actors:
            signal_ids = self._traffic_light_actor_signal_ids(actor)
            publish_id = ""
            if configured_ids:
                publish_id = next((item for item in signal_ids if item in configured_ids), "")
            elif signal_ids:
                publish_id = next((item for item in signal_ids if item.startswith("TL_")), signal_ids[0])
            if not publish_id:
                continue
            resolved[publish_id] = self._carla_traffic_light_color_name(actor)
        return sorted(resolved.items())

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
        goal_validity_last = dict(self.stats.get("goal_validity_last", {}) or {})
        front_status = self._front_obstacle_behavior_status()
        front_gap = front_status.get("last_gap", {}) or {}
        terminal_hold = self._terminal_stop_hold_status()
        timing = self._timing_snapshot()
        planning_timing_summary = self._planning_timing_summary()
        bridge_policy_summary = build_bridge_policy_summary_impl(
            goal_validity_last=goal_validity_last,
            stats=self.stats,
        )
        bridge_observer_summary = build_bridge_observer_summary_impl(
            planning_reader_enabled=bool(self._planning_reader_enabled),
            planning_channel=self.planning_channel,
            planning_message_type=self._planning_message_type,
            timing=timing,
            health_summary_path=str(self.health_summary_path),
            planning_topic_debug_summary_path=str(self.planning_topic_debug_summary_path),
            planning_route_segment_debug_path=str(self.planning_route_segment_debug_path),
            apollo_map_runtime_debug_path=str(self.apollo_map_runtime_debug_path),
            apollo_reference_line_debug_path=str(self.apollo_reference_line_debug_path),
            apollo_route_segment_debug_path=str(self.apollo_route_segment_debug_path),
            control_trajectory_consume_live_path=str(self.control_trajectory_consume_live_path),
            routing_event_debug_path=str(self.routing_event_debug_path),
            reroute_decision_debug_path=str(self.reroute_decision_debug_path),
            goal_validity_debug_path=str(self.goal_validity_debug_path),
            lateral_guard_debug_path=str(self.lateral_guard_debug_path),
        )
        ingress_egress_summary = build_ingress_egress_summary_impl(
            transport_mode=self.transport_mode,
            gt_source=self.gt_source,
            control_apply_path=self.control_apply_path,
            tick_owner=self.bridge_tick_owner_model,
            ros_ego_id=self.ros_ego_id,
            odom_topic=self.odom_topic,
            objects3d_topic=self.objects3d_topic,
            objects_markers_topic=self.objects_markers_topic,
            objects_json_topic=self.objects_json_topic,
            control_out_topic=self.control_out_topic,
            control_out_type=self.control_out_type,
            localization_channel=self.localization_channel,
            chassis_channel=self.chassis_channel,
            obstacles_channel=self.obstacles_channel,
            control_channel=self.control_channel,
            planning_channel=self.planning_channel,
            routing_request_channel=self.routing_request_channel,
            routing_response_channel=self.routing_response_channel,
            lane_follow_channel=self.lane_follow_channel,
            action_channel=self.action_channel,
            traffic_light_channel=self.traffic_light_channel,
        )
        direct_transport_summary = {}
        if hasattr(self.node, "transport_summary"):
            try:
                direct_transport_summary = dict(self.node.transport_summary() or {})
            except Exception:
                direct_transport_summary = {}
        command_materialization_summary = self._command_materialization_summary()
        bridge_transport_summary = build_bridge_transport_summary_impl(
            transport_mode=self.transport_mode,
            gt_source=self.gt_source,
            control_apply_path=self.control_apply_path,
            tick_owner=self.bridge_tick_owner_model,
            bridge_is_tick_owner=bool(self.bridge_is_tick_owner),
            ros2_gt_enabled=bool(self.transport_mode == "ros2_gt"),
            uses_ros2_gt=bool(self.uses_ros2_gt),
            uses_ros2_control_bridge=bool(self.uses_ros2_control_bridge),
            requires_ros2_reexec=bool(self.requires_ros2_reexec),
            route_command_mode=self.route_command_mode,
            route_command_path=self.route_command_path,
            control_out_type=self.control_out_type,
            direct_bridge=direct_transport_summary,
        )
        return {
            "summary_status": "provisional",
            "finalized_from_event_stream": False,
            "transport_mode": self.transport_mode,
            "gt_source": self.gt_source,
            "control_apply_path": self.control_apply_path,
            "tick_owner": self.bridge_tick_owner_model,
            "uses_ros2_gt": self.uses_ros2_gt,
            "uses_ros2_control_bridge": self.uses_ros2_control_bridge,
            "requires_ros2_reexec": self.requires_ros2_reexec,
            "route_command_mode": self.route_command_mode,
            "route_command_path": self.route_command_path,
            "require_no_ros2_runtime": self.require_no_ros2_runtime,
            "routing_goal_dist_m": last_goal.get("goal_dist_m"),
            "routing_goal_mode": last_goal.get("goal_mode"),
            "routing_startup_phase_used": last_goal.get("startup_phase_used"),
            "routing_phase": self.auto_routing_current_phase,
            "routing_request_count": self.stats.get("routing_request_count", 0),
            "routing_phase_counts": dict(self.stats.get("routing_phase_counts", {})),
            "reroute_reason_counts": dict(self.stats.get("reroute_reason_counts", {}) or {}),
            "last_routing_reason": self.stats.get("last_routing_reason", ""),
            "distance_to_destination": self._planning_last_distance_to_destination,
            "planning_reader_enabled": self._planning_reader_enabled,
            "planning_reader_enable_reason": self._planning_reader_enable_reason,
            "planning_topic_name": self.planning_channel,
            "planning_message_type": self._planning_message_type,
            "planning_has_trajectory": (
                self._planning_nonempty_count > 0 if self._planning_reader_enabled else None
            ),
            "planning_nonempty_trajectory_count": self._planning_nonempty_count,
            "planning_empty_trajectory_count": self._planning_empty_count,
            "planning_first_msg_ts_sec": planning_timing_summary["first_msg_ts_sec"],
            "planning_first_nonempty_ts_sec": self._planning_first_nonempty_ts_sec,
            "planning_last_reroute_boundary_semantics": planning_timing_summary["last_reroute_boundary_semantics"],
            "planning_first_msg_last_reroute_ts_sec": planning_timing_summary["first_msg_last_reroute_ts_sec"],
            "planning_first_msg_last_routing_send_ts_sec": (
                planning_timing_summary["first_msg_last_routing_send_ts_sec"]
            ),
            "planning_first_msg_after_last_reroute_sec": (
                planning_timing_summary["first_msg_after_last_reroute_sec"]
            ),
            "planning_first_msg_after_last_routing_send_sec": (
                planning_timing_summary["first_msg_after_last_routing_send_sec"]
            ),
            "planning_first_route_debug_ts_sec": planning_timing_summary["first_route_debug_ts_sec"],
            "planning_first_route_debug_last_reroute_ts_sec": (
                planning_timing_summary["first_route_debug_last_reroute_ts_sec"]
            ),
            "planning_first_route_debug_last_routing_send_ts_sec": (
                planning_timing_summary["first_route_debug_last_routing_send_ts_sec"]
            ),
            "planning_first_route_debug_after_last_reroute_sec": (
                planning_timing_summary["first_route_debug_after_last_reroute_sec"]
            ),
            "planning_first_route_debug_after_last_routing_send_sec": (
                planning_timing_summary["first_route_debug_after_last_routing_send_sec"]
            ),
            "routing_first_response_ts_sec": planning_timing_summary["routing_first_response_ts_sec"],
            "routing_first_success_response_ts_sec": (
                planning_timing_summary["routing_first_success_response_ts_sec"]
            ),
            "routing_last_response_ts_sec": planning_timing_summary["routing_last_response_ts_sec"],
            "routing_last_success_response_ts_sec": planning_timing_summary["routing_last_success_response_ts_sec"],
            "routing_first_response_after_last_routing_send_boundary_ts_sec": (
                planning_timing_summary["routing_first_response_after_last_routing_send_boundary_ts_sec"]
            ),
            "routing_first_response_after_last_routing_send_ts_sec": (
                planning_timing_summary["routing_first_response_after_last_routing_send_ts_sec"]
            ),
            "routing_first_response_after_last_routing_send_sec": (
                planning_timing_summary["routing_first_response_after_last_routing_send_sec"]
            ),
            "routing_first_success_response_after_last_routing_send_boundary_ts_sec": (
                planning_timing_summary["routing_first_success_response_after_last_routing_send_boundary_ts_sec"]
            ),
            "routing_first_success_response_after_last_routing_send_ts_sec": (
                planning_timing_summary["routing_first_success_response_after_last_routing_send_ts_sec"]
            ),
            "routing_first_success_response_after_last_routing_send_sec": (
                planning_timing_summary["routing_first_success_response_after_last_routing_send_sec"]
            ),
            "planning_first_msg_after_first_routing_response_after_last_routing_send_sec": (
                planning_timing_summary["first_msg_after_first_routing_response_after_last_routing_send_sec"]
            ),
            "planning_first_msg_after_first_successful_routing_response_after_last_routing_send_sec": (
                planning_timing_summary[
                    "first_msg_after_first_successful_routing_response_after_last_routing_send_sec"
                ]
            ),
            "planning_first_route_debug_after_first_routing_response_after_last_routing_send_sec": (
                planning_timing_summary[
                    "first_route_debug_after_first_routing_response_after_last_routing_send_sec"
                ]
            ),
            "planning_first_route_debug_after_first_successful_routing_response_after_last_routing_send_sec": (
                planning_timing_summary[
                    "first_route_debug_after_first_successful_routing_response_after_last_routing_send_sec"
                ]
            ),
            "planning_timing_summary": planning_timing_summary,
            "planning_parse_fail_count": self._planning_parse_fail_count,
            "planning_last_header_sequence_num": (
                (self._planning_last_event or {}).get("planning_header_sequence_num")
            ),
            "planning_topic_debug_summary_path": str(self.planning_topic_debug_summary_path),
            "planning_route_segment_debug_path": str(self.planning_route_segment_debug_path),
            "apollo_map_runtime_debug_path": str(self.apollo_map_runtime_debug_path),
            "apollo_reference_line_debug_path": str(self.apollo_reference_line_debug_path),
            "apollo_route_segment_debug_path": str(self.apollo_route_segment_debug_path),
            "control_trajectory_consume_live_path": str(self.control_trajectory_consume_live_path),
            "routing_event_debug_path": str(self.routing_event_debug_path),
            "reroute_decision_debug_path": str(self.reroute_decision_debug_path),
            "goal_validity_debug_path": str(self.goal_validity_debug_path),
            "lateral_guard_debug_path": str(self.lateral_guard_debug_path),
            "map_contract_invalid": self.map_contract_invalid,
            "map_contract_mismatch_reason": self.map_contract_mismatch_reason,
            "apollo_runtime_map_dir": self.apollo_runtime_map_dir,
            "dreamview_selected_map": self.dreamview_selected_map,
            "traffic_light_policy": self.traffic_light_policy,
            "traffic_light_force_green_publish_count": self._traffic_light_publish_count,
            "traffic_light_last_publish_ts_sec": self._traffic_light_last_publish_ts,
            "traffic_light_force_green_active": (
                self.traffic_light_policy == "force_green" and self.traffic_light_writer is not None
            ),
            "traffic_light_ignore_roll_route_count": self._ignore_roll_route_count,
            "lane_follow_no_response_count": self.stats.get("lane_follow_no_response_count", 0),
            "lane_follow_disabled_on_no_response": self.stats.get("lane_follow_disabled_on_no_response", False),
            "auto_enable_lane_follow_fallback": self.auto_routing_auto_enable_lane_follow_fallback,
            "routing_snap_start_to_lane": self.auto_routing_snap_start_to_lane,
            "routing_snap_goal_to_lane": self.auto_routing_snap_goal_to_lane,
            "routing_snap_source_mode": self.auto_routing_snap_source_mode,
            "routing_snap_allow_untrusted_source": self.auto_routing_snap_allow_untrusted_source,
            "routing_snap_heading_diff_max_deg": self.auto_routing_snap_heading_diff_max_deg,
            "routing_snap_heading_diff_hard_reject_deg": self.auto_routing_snap_heading_diff_hard_reject_deg,
            "routing_lane_heading_nudge_max_heading_diff_deg": self.auto_routing_lane_heading_nudge_max_heading_diff_deg,
            "routing_start_nudge_use_lane_heading": self.auto_routing_start_nudge_use_lane_heading,
            "routing_freeze_after_success": self.auto_routing_freeze_after_success,
            "routing_freeze_active": self._routing_freeze_active,
            "routing_skipped_due_to_freeze_count": self.stats.get("routing_skipped_due_to_freeze_count", 0),
            "goal_validity_last": goal_validity_last,
            "invalid_goal_count": int(self.stats.get("invalid_goal_count", 0) or 0),
            "bridge_policy": bridge_policy_summary,
            "bridge_observer": bridge_observer_summary,
            "ingress_egress": ingress_egress_summary,
            "bridge_transport": bridge_transport_summary,
            "command_materialization": command_materialization_summary,
            "command_materialization_summary_path": str(self.command_materialization_summary_path),
            "timing": timing,
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
            "force_zero_steer_output_enabled": self.force_zero_steer_output,
            "straight_lane_zero_steer_enabled": self.straight_lane_zero_steer_enabled,
            "low_speed_steer_guard_enabled": self.low_speed_steer_guard_enabled,
            "low_speed_sustained_guard_enabled": self.low_speed_sustained_guard_enabled,
            "sustained_lateral_guard_enabled": self.sustained_lateral_guard_enabled,
            "trajectory_contract_lateral_guard_enabled": self.trajectory_contract_lateral_guard_enabled,
            "lateral_guard_trigger_count": int(self.stats.get("lateral_guard_trigger_count", 0) or 0),
            "lateral_guard_apply_count": int(self.stats.get("lateral_guard_apply_count", 0) or 0),
            "trajectory_contract_lateral_guard_apply_count": int(
                self.stats.get("trajectory_contract_lateral_guard_apply_count", 0) or 0
            ),
            "lateral_guard_reason_counts": dict(self.stats.get("lateral_guard_reason_counts", {}) or {}),
            "straight_lane_zero_steer_apply_count": int(
                self.stats.get("straight_lane_zero_steer_apply_count", 0) or 0
            ),
            "low_speed_steer_guard_apply_count": int(
                self.stats.get("low_speed_steer_guard_apply_count", 0) or 0
            ),
            "low_speed_sustained_guard_apply_count": int(
                self.stats.get("low_speed_sustained_guard_apply_count", 0) or 0
            ),
            "force_zero_steer_apply_count": int(self.stats.get("force_zero_steer_apply_count", 0) or 0),
            "speed_mps_last": self._latest_speed_mps,
            "speed_mps_max": self._max_speed_mps,
            "startup_geometry_summary_path": str(self.startup_geometry_summary_path),
            "map_contract_invalid": self.map_contract_invalid,
            "map_contract_mismatch_reason": self.map_contract_mismatch_reason,
            "apollo_runtime_map_dir": self.apollo_runtime_map_dir,
            "dreamview_selected_map": self.dreamview_selected_map,
            "last_error": self.stats.get("last_error", ""),
        }

    def _write_health_summary(self) -> None:
        payload = self._health_summary()
        self.stats["planning"] = self._planning_status()
        self.stats["traffic_light"] = self._traffic_light_status()
        self.stats["front_obstacle_behavior"] = self._front_obstacle_behavior_status()
        self.stats["terminal_stop_hold"] = self._terminal_stop_hold_status()
        self.stats["command_materialization"] = dict(payload.get("command_materialization", {}) or {})
        self.stats["timing"] = dict(payload.get("timing", {}) or {})
        self.stats["health"] = payload
        self._write_json_file(self.health_summary_path, payload)
        self._write_json_file(
            self.bridge_transport_summary_path,
            dict(payload.get("bridge_transport", {}) or {}),
        )
        self._write_json_file(
            self.command_materialization_summary_path,
            dict(payload.get("command_materialization", {}) or {}),
        )

    def _on_planning(self, msg: Any) -> None:
        self._planning_msg_count += 1
        now_sec = self._command_now_sec()
        self._planning_last_msg_ts = now_sec
        current_last_routing_send_ts: Optional[float] = None
        if self.auto_routing_last_routing_ts and math.isfinite(float(self.auto_routing_last_routing_ts)):
            current_last_routing_send_ts = float(self.auto_routing_last_routing_ts)
        if self._planning_first_msg_ts_sec is None:
            self._planning_first_msg_ts_sec = now_sec
            self._planning_first_msg_last_reroute_ts_sec = current_last_routing_send_ts
            self._planning_first_msg_last_routing_send_ts_sec = current_last_routing_send_ts
        planning_header_ts = self._header_timestamp_sec(msg)
        planning_header_seq = self._header_sequence_num(msg)
        timing = self._timing_snapshot(
            event_wall_time_sec=now_sec,
            latest_planning_age_ms=(
                ((now_sec - planning_header_ts) * 1000.0)
                if planning_header_ts is not None and math.isfinite(float(planning_header_ts))
                else None
            ),
        )
        debug_row: Dict[str, Any] = {
            "timestamp": now_sec,
            "wall_time_sec": timing.get("wall_time_sec"),
            "sim_time_sec": timing.get("sim_time_sec"),
            "world_frame": timing.get("world_frame"),
            "tick_owner": timing.get("tick_owner"),
            "bridge_is_tick_owner": timing.get("bridge_is_tick_owner"),
            "topic_name": self.planning_channel,
            "message_type": self._planning_message_type,
            "message_received": True,
            "planning_header_timestamp_sec": planning_header_ts,
            "planning_header_sequence_num": planning_header_seq,
            "trajectory_point_count": 0,
            "trajectory_header_status": None,
            "estop": None,
            "engage_advice": None,
            "trajectory_type": None,
            "planning_message_parsed_successfully": False,
            "parse_fail_reason": "",
            "first_trajectory_point_x": None,
            "first_trajectory_point_y": None,
            "first_trajectory_point_theta": None,
            "first_trajectory_point_kappa": None,
            "first_trajectory_point_v": None,
            "first_trajectory_point_relative_time_sec": None,
            "last_trajectory_point_x": None,
            "last_trajectory_point_y": None,
            "trajectory_total_path_length": None,
            "trajectory_total_time": None,
            "trajectory_relative_time_min_sec": None,
            "trajectory_relative_time_max_sec": None,
            "is_replan": None,
            "replan_reason": None,
            "lane_id_first": None,
            "target_lane_id_first": None,
            "reference_line_count": 0,
            "reference_line_lengths": [],
            "reference_line_length_max": None,
            "routing_road_count": 0,
            "routing_passage_count": 0,
            "routing_segment_count": 0,
            "routing_total_length": None,
            "routing_lane_window_count": 0,
            "routing_lane_window_signature": "none",
            "routing_unique_lane_count": 0,
            "routing_unique_lane_signature": "none",
            "scenario_plugin_type": None,
            "stage_plugin_type": None,
            "distance_to_destination": None,
        }
        route_debug_row: Dict[str, Any] = {
            "timestamp": now_sec,
            "wall_time_sec": timing.get("wall_time_sec"),
            "sim_time_sec": timing.get("sim_time_sec"),
            "world_frame": timing.get("world_frame"),
            "planning_header_timestamp_sec": planning_header_ts,
            "planning_header_sequence_num": planning_header_seq,
            "current_lane_id": None,
            "current_lane_road_id": None,
            "current_lane_section_id": None,
            "current_lane_junction_id": None,
            "current_lane_is_junction": None,
            "current_lane_metadata_source": None,
            "lane_id_first": None,
            "lane_id_first_road_id": None,
            "lane_id_first_section_id": None,
            "lane_id_first_junction_id": None,
            "lane_id_first_is_junction": None,
            "lane_id_first_metadata_source": None,
            "target_lane_id_first": None,
            "target_lane_id_first_road_id": None,
            "target_lane_id_first_section_id": None,
            "target_lane_id_first_junction_id": None,
            "target_lane_id_first_is_junction": None,
            "target_lane_id_first_metadata_source": None,
            "target_lane_road_id": None,
            "target_lane_section_id": None,
            "target_lane_junction_id": None,
            "target_lane_is_junction": None,
            "target_lane_metadata_source": None,
            "reference_line_count": 0,
            "reference_line_length": None,
            "route_segment_count": 0,
            "route_segment_total_length": None,
            "routing_road_count": 0,
            "routing_passage_count": 0,
            "routing_lane_window_count": 0,
            "routing_lane_window_signature": "none",
            "routing_unique_lane_count": 0,
            "routing_unique_lane_signature": "none",
            "ego_s": None,
            "ego_l": None,
            "dest_s": None,
            "dest_l": None,
            "ego_front_to_center": self._estimate_ego_front_to_center(),
            "remain_length_to_dest": None,
            "dest_beyond_reference_line": None,
            "path_end_like_condition": False,
            "reference_line_provider_status": None,
            "create_route_segments_status": None,
            "lane_follow_map_status": None,
            "planning_stage_name": None,
            "task_name": None,
            "planning_empty_reason_guess": "",
        }
        try:
            pts = getattr(msg, "trajectory_point", None)
            pt_count = len(pts) if pts is not None else 0
            self._planning_last_points = int(pt_count)
            self._planning_point_counts.append(int(pt_count))
            if pt_count > 0:
                self._planning_nonempty_count += 1
                if self._planning_first_nonempty_ts_sec is None:
                    self._planning_first_nonempty_ts_sec = now_sec
            else:
                self._planning_empty_count += 1
            distance_to_destination = self._resolve_nested_float(
                msg,
                (
                    ("distance_to_destination",),
                    ("debug", "distance_to_destination"),
                    ("debug", "planning_data", "distance_to_destination"),
                ),
            )
            self._planning_last_distance_to_destination = distance_to_destination
            header_status = self._resolve_nested_value(
                msg,
                (
                    ("header", "status", "msg"),
                    ("header", "status", "error_message"),
                    ("header", "status", "error_code"),
                    ("header", "status"),
                ),
            )
            estop = self._resolve_nested_value(msg, (("estop", "is_estop"), ("estop",)))
            engage_advice = self._resolve_nested_value(
                msg,
                (
                    ("engage_advice", "advice"),
                    ("engage_advice",),
                ),
            )
            trajectory_type = self._enum_name_from_msg(
                msg,
                "trajectory_type",
                self._resolve_nested_value(msg, (("trajectory_type",),)),
            )
            rel_time_min, rel_time_max = self._trajectory_time_bounds(pts)
            lane_ids = getattr(msg, "lane_id", None)
            lane_id_first = None
            if lane_ids:
                first_lane = lane_ids[0]
                lane_id_first = self._proto_scalar(self._resolve_nested_value(first_lane, (("id",),)))
            target_lane_ids = getattr(msg, "target_lane_id", None)
            target_lane_id_first = None
            if target_lane_ids:
                first_target_lane = target_lane_ids[0]
                target_lane_id_first = self._proto_scalar(
                    self._resolve_nested_value(first_target_lane, (("id",),))
                )
            ref_lengths = self._planning_reference_line_lengths(msg)
            routing_road_count, routing_passage_count, routing_segment_count = self._planning_routing_counts(msg)
            routing_total_length = self._planning_routing_total_length(msg)
            (
                routing_lane_window_count,
                routing_unique_lane_count,
                routing_lane_window_signature,
                routing_unique_lane_signature,
            ) = self._planning_routing_lane_window_summary(msg)
            scenario_plugin_type, stage_plugin_type = self._planning_stage_info(msg)
            current_lane_id = lane_id_first or target_lane_id_first
            lane_metadata_fields = _planning_lane_metadata_fields(
                current_lane_id,
                lane_id_first,
                target_lane_id_first,
                self.map_lane_metadata,
            )
            first_x = first_y = last_x = last_y = None
            first_theta = first_kappa = first_v = first_relative_time = None
            if pt_count > 0:
                first_x, first_y = self._trajectory_point_xy(pts[0])
                last_x, last_y = self._trajectory_point_xy(pts[-1])
                first_theta = self._resolve_nested_float(pts[0], (("path_point", "theta"),))
                first_kappa = self._resolve_nested_float(pts[0], (("path_point", "kappa"),))
                first_v = self._resolve_nested_float(pts[0], (("v",),))
                first_relative_time = self._resolve_nested_float(pts[0], (("relative_time",),))
            debug_row.update(
                {
                    "trajectory_point_count": int(pt_count),
                    "trajectory_header_status": self._proto_scalar(header_status),
                    "estop": self._proto_scalar(estop),
                    "engage_advice": self._proto_scalar(engage_advice),
                    "trajectory_type": self._proto_scalar(trajectory_type),
                    "planning_message_parsed_successfully": True,
                    "first_trajectory_point_x": first_x,
                    "first_trajectory_point_y": first_y,
                    "first_trajectory_point_theta": first_theta,
                    "first_trajectory_point_kappa": first_kappa,
                    "first_trajectory_point_v": first_v,
                    "first_trajectory_point_relative_time_sec": first_relative_time,
                    "last_trajectory_point_x": last_x,
                    "last_trajectory_point_y": last_y,
                    "trajectory_total_path_length": self._resolve_nested_float(
                        msg,
                        (("total_path_length",),),
                    ),
                    "trajectory_total_time": self._resolve_nested_float(
                        msg,
                        (
                            ("total_path_time",),
                            ("total_path_sec",),
                        ),
                    ),
                    "trajectory_relative_time_min_sec": rel_time_min,
                    "trajectory_relative_time_max_sec": rel_time_max,
                    "is_replan": self._proto_scalar(self._resolve_nested_value(msg, (("is_replan",),))),
                    "replan_reason": self._proto_scalar(self._resolve_nested_value(msg, (("replan_reason",),))),
                    "lane_id_first": lane_id_first,
                    "target_lane_id_first": target_lane_id_first,
                    "reference_line_count": len(ref_lengths),
                    "reference_line_lengths": ref_lengths,
                    "reference_line_length_max": (max(ref_lengths) if ref_lengths else None),
                    "routing_road_count": routing_road_count,
                    "routing_passage_count": routing_passage_count,
                    "routing_segment_count": routing_segment_count,
                    "routing_total_length": routing_total_length,
                    "routing_lane_window_count": routing_lane_window_count,
                    "routing_lane_window_signature": routing_lane_window_signature,
                    "routing_unique_lane_count": routing_unique_lane_count,
                    "routing_unique_lane_signature": routing_unique_lane_signature,
                    "scenario_plugin_type": scenario_plugin_type,
                    "stage_plugin_type": stage_plugin_type,
                    "distance_to_destination": distance_to_destination,
                    "lane_id_first_road_id": lane_metadata_fields["lane_id_first_road_id"],
                    "lane_id_first_section_id": lane_metadata_fields["lane_id_first_section_id"],
                    "lane_id_first_junction_id": lane_metadata_fields["lane_id_first_junction_id"],
                    "lane_id_first_is_junction": lane_metadata_fields["lane_id_first_is_junction"],
                    "lane_id_first_metadata_source": lane_metadata_fields["lane_id_first_metadata_source"],
                    "target_lane_id_first_road_id": lane_metadata_fields["target_lane_id_first_road_id"],
                    "target_lane_id_first_section_id": lane_metadata_fields["target_lane_id_first_section_id"],
                    "target_lane_id_first_junction_id": lane_metadata_fields["target_lane_id_first_junction_id"],
                    "target_lane_id_first_is_junction": lane_metadata_fields["target_lane_id_first_is_junction"],
                    "target_lane_id_first_metadata_source": lane_metadata_fields["target_lane_id_first_metadata_source"],
                }
            )
            ref_len_max = max(ref_lengths) if ref_lengths else None
            remain_length_to_dest = distance_to_destination
            dest_beyond_reference_line: Optional[bool] = None
            ego_front_to_center = route_debug_row.get("ego_front_to_center")
            if (
                ref_len_max is not None
                and remain_length_to_dest is not None
                and ego_front_to_center is not None
                and math.isfinite(float(ref_len_max))
                and math.isfinite(float(remain_length_to_dest))
                and math.isfinite(float(ego_front_to_center))
            ):
                dest_beyond_reference_line = (
                    float(remain_length_to_dest) + float(ego_front_to_center) > float(ref_len_max)
                )
            trajectory_nonzero_reference_line_debug_missing = bool(
                pt_count > 0 and len(ref_lengths) <= 0 and routing_segment_count > 0
            )
            if pt_count <= 0 and len(ref_lengths) <= 0:
                empty_reason_guess = "reference_line_missing"
            elif pt_count <= 0 and routing_segment_count <= 0:
                empty_reason_guess = "route_segment_missing"
            elif pt_count <= 0 and dest_beyond_reference_line is True:
                empty_reason_guess = "dest_beyond_reference_line"
            elif pt_count <= 0:
                empty_reason_guess = "zero_trajectory_points"
            else:
                empty_reason_guess = ""
            if trajectory_nonzero_reference_line_debug_missing:
                lane_follow_map_status = "trajectory_nonzero_reference_line_debug_missing"
            elif len(ref_lengths) <= 0 and routing_segment_count > 0:
                lane_follow_map_status = "route_segments_present_reference_line_missing"
            elif len(ref_lengths) <= 0:
                lane_follow_map_status = "reference_line_missing"
            elif routing_segment_count <= 0:
                lane_follow_map_status = "route_segment_missing"
            else:
                lane_follow_map_status = "ready"
            route_debug_row.update(
                {
                    **lane_metadata_fields,
                    "reference_line_count": len(ref_lengths),
                    "reference_line_length": ref_len_max,
                    "route_segment_count": routing_segment_count,
                    "route_segment_total_length": routing_total_length,
                    "routing_road_count": routing_road_count,
                    "routing_passage_count": routing_passage_count,
                    "routing_lane_window_count": routing_lane_window_count,
                    "routing_lane_window_signature": routing_lane_window_signature,
                    "routing_unique_lane_count": routing_unique_lane_count,
                    "routing_unique_lane_signature": routing_unique_lane_signature,
                    "remain_length_to_dest": remain_length_to_dest,
                    "dest_beyond_reference_line": dest_beyond_reference_line,
                    "reference_line_debug_missing_but_trajectory_nonzero": (
                        trajectory_nonzero_reference_line_debug_missing
                    ),
                    "path_end_like_condition": bool(
                        pt_count <= 0
                        and (
                            len(ref_lengths) <= 0
                            or routing_segment_count <= 0
                            or dest_beyond_reference_line is True
                        )
                    ),
                    "reference_line_provider_status": (
                        "trajectory_nonzero_debug_missing"
                        if trajectory_nonzero_reference_line_debug_missing
                        else ("ready" if len(ref_lengths) > 0 else "failed")
                    ),
                    "create_route_segments_status": ("ready" if routing_segment_count > 0 else "failed"),
                    "lane_follow_map_status": lane_follow_map_status,
                    "planning_stage_name": stage_plugin_type,
                    "task_name": scenario_plugin_type,
                    "planning_empty_reason_guess": empty_reason_guess,
                }
            )
        except Exception as exc:
            self._planning_parse_fail_count += 1
            reason = f"{type(exc).__name__}:{exc}"
            self._planning_parse_fail_reasons[reason] += 1
            self._planning_last_points = 0
            self._planning_empty_count += 1
            self._planning_last_distance_to_destination = None
            debug_row["parse_fail_reason"] = reason
            route_debug_row["planning_empty_reason_guess"] = "planning_parse_failed"
            route_debug_row["reference_line_provider_status"] = "parse_failed"
            route_debug_row["create_route_segments_status"] = "parse_failed"
            route_debug_row["lane_follow_map_status"] = "parse_failed"
        self._append_jsonl(self.planning_topic_debug_path, debug_row)
        self._append_jsonl(self.planning_route_segment_debug_path, route_debug_row)
        enriched_route_debug = {
            **route_debug_row,
            "route_debug_source": "stage5_runtime",
            "runtime_map_dir": self.apollo_runtime_map_dir,
            "dreamview_selected_map": self.dreamview_selected_map or None,
            "last_reroute_timestamp": (
                float(self.auto_routing_last_routing_ts)
                if self.auto_routing_last_routing_ts and math.isfinite(float(self.auto_routing_last_routing_ts))
                else None
            ),
            "map_contract_invalid": bool(self.map_contract_invalid),
            "map_contract_mismatch_active": bool(self.map_contract_invalid),
            "map_contract_mismatch_reason": self.map_contract_mismatch_reason,
            "map_contract_mismatch_classification": self.map_contract_mismatch_classification or None,
            "map_identity_hash_or_signature": self.apollo_map_identity_signature or None,
            "host_container_map_path_mapping": dict(self.host_container_map_path_mapping or {}),
            "lane_follow_map_inconsistent": self._lane_follow_map_inconsistent_active(route_debug_row),
        }
        map_runtime_row = {
            "timestamp": now_sec,
            "runtime_map_dir": self.apollo_runtime_map_dir,
            "dreamview_selected_map": self.dreamview_selected_map or None,
            "current_lane_id": route_debug_row.get("current_lane_id"),
            "current_lane_road_id": route_debug_row.get("current_lane_road_id"),
            "current_lane_section_id": route_debug_row.get("current_lane_section_id"),
            "current_lane_junction_id": route_debug_row.get("current_lane_junction_id"),
            "current_lane_is_junction": route_debug_row.get("current_lane_is_junction"),
            "current_lane_metadata_source": route_debug_row.get("current_lane_metadata_source"),
            "target_lane_id_first": route_debug_row.get("target_lane_id_first"),
            "target_lane_road_id": route_debug_row.get("target_lane_road_id"),
            "target_lane_section_id": route_debug_row.get("target_lane_section_id"),
            "target_lane_junction_id": route_debug_row.get("target_lane_junction_id"),
            "target_lane_is_junction": route_debug_row.get("target_lane_is_junction"),
            "target_lane_metadata_source": route_debug_row.get("target_lane_metadata_source"),
            "reference_line_count": route_debug_row.get("reference_line_count"),
            "route_segment_count": route_debug_row.get("route_segment_count"),
            "lane_follow_map_status": route_debug_row.get("lane_follow_map_status"),
            "map_contract_invalid": bool(self.map_contract_invalid),
            "map_contract_mismatch_active": bool(self.map_contract_invalid),
            "map_contract_mismatch_reason": self.map_contract_mismatch_reason,
            "map_contract_mismatch_classification": self.map_contract_mismatch_classification or None,
            "map_identity_hash_or_signature": self.apollo_map_identity_signature or None,
            "host_container_map_path_mapping": dict(self.host_container_map_path_mapping or {}),
        }
        self._append_jsonl(self.apollo_map_runtime_debug_path, map_runtime_row)
        self._append_jsonl(self.stage5_apollo_map_runtime_debug_path, map_runtime_row)
        self._append_jsonl(self.apollo_reference_line_debug_path, enriched_route_debug)
        self._append_jsonl(self.apollo_route_segment_debug_path, enriched_route_debug)
        self._append_jsonl(self.stage5_apollo_reference_line_debug_path, enriched_route_debug)
        self._append_jsonl(self.stage5_apollo_route_segment_debug_path, enriched_route_debug)
        self._append_jsonl(self.stage5_apollo_lane_follow_map_debug_path, enriched_route_debug)
        if self._planning_first_route_debug_ts_sec is None:
            self._planning_first_route_debug_ts_sec = now_sec
            self._planning_first_route_debug_last_reroute_ts_sec = current_last_routing_send_ts
            self._planning_first_route_debug_last_routing_send_ts_sec = current_last_routing_send_ts
        self._planning_recent_events.append(dict(debug_row))
        self._planning_last_event = dict(debug_row)
        self._planning_last_route_debug_event = dict(enriched_route_debug)
        self.stats["planning"] = self._planning_status()
        self._write_planning_topic_debug_summary()

    def _maybe_publish_traffic_lights(self, ts_sec: float) -> None:
        if self.traffic_light_policy not in {"force_green", "carla_actual"} or self.traffic_light_writer is None:
            return
        period = 1.0 / max(self.traffic_light_publish_hz, 1e-3)
        if (ts_sec - self._last_traffic_light_publish_ts) < period:
            return
        try:
            msg = self.traffic_light_pb2.TrafficLightDetection()
            self._fill_header(getattr(msg, "header", None), self._command_now_sec(), "tb_apollo10_gt_bridge")
            if not hasattr(msg, "traffic_light"):
                raise RuntimeError("TrafficLightDetection has no traffic_light field")
            if self.traffic_light_policy == "force_green":
                entries = [(signal_id, "GREEN") for signal_id in self.traffic_light_force_ids]
            else:
                entries = self._current_carla_actual_traffic_lights()
            if hasattr(msg, "contain_lights"):
                msg.contain_lights = bool(entries)
            for signal_id, color_name in entries:
                light = msg.traffic_light.add()
                if hasattr(light, "id"):
                    light.id = str(signal_id)
                color_value = self._apollo_traffic_light_color_value(color_name)
                if hasattr(light, "color"):
                    light.color = color_value
                elif hasattr(light, "status"):
                    light.status = color_value
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
        map_x_before_back_offset = map_x
        map_y_before_back_offset = map_y
        if self.localization_back_offset_m != 0.0:
            map_x -= self.localization_back_offset_m * math.cos(map_yaw)
            map_y -= self.localization_back_offset_m * math.sin(map_yaw)
        return {
            "raw_x": raw_x,
            "raw_y": raw_y,
            "raw_z": raw_z,
            "raw_yaw": raw_yaw,
            "raw_yaw_deg": math.degrees(raw_yaw),
            "map_x_before_back_offset": map_x_before_back_offset,
            "map_y_before_back_offset": map_y_before_back_offset,
            "map_x": map_x,
            "map_y": map_y,
            "map_z": map_z,
            "map_yaw": map_yaw,
            "map_yaw_deg": math.degrees(map_yaw),
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
            "localization_back_offset_m": float(self.localization_back_offset_m),
            "localization_reference_mode": self.localization_reference_mode,
            "apollo_control_state_reference": self.apollo_control_state_reference,
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
        if "raw_forward_accel_mps2" in state:
            measured["raw_forward_accel_mps2"] = float(state["raw_forward_accel_mps2"])
        if "dvdt_forward_accel_mps2" in state:
            measured["dvdt_forward_accel_mps2"] = float(state["dvdt_forward_accel_mps2"])
        if "lateral_accel_mps2" in state:
            measured["lateral_accel_mps2"] = float(state["lateral_accel_mps2"])
        if "yaw_rate_rps" in state:
            measured["yaw_rate_rps"] = float(state["yaw_rate_rps"])
        if "curvature" in state:
            measured["curvature"] = float(state["curvature"])
        if "pose_x" in state:
            measured["pose_x"] = float(state["pose_x"])
        if "pose_y" in state:
            measured["pose_y"] = float(state["pose_y"])
        if "pose_z" in state:
            measured["pose_z"] = float(state["pose_z"])
        if "pose_yaw_deg" in state:
            measured["pose_yaw_deg"] = float(state["pose_yaw_deg"])
        if "gear" in state:
            measured["gear"] = float(state["gear"])
        self.stats["last_measured_control"] = measured
        return measured

    def _extract_raw_control_fields(self, cmd: Any) -> Dict[str, Any]:
        raw: Dict[str, Any] = {}
        raw["control_header_timestamp_sec"] = self._header_timestamp_sec(cmd)
        raw["control_header_sequence_num"] = self._header_sequence_num(cmd)
        for key in (
            "throttle",
            "brake",
            "acceleration",
            "speed",
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
        debug_msg = getattr(cmd, "debug", None)
        lon_debug = getattr(debug_msg, "simple_lon_debug", None) if debug_msg is not None else None
        if lon_debug is not None:
            for key in (
                "station_reference",
                "station_error",
                "current_station",
                "path_remain",
                "acceleration_cmd",
                "acceleration_cmd_closeloop",
                "acceleration_lookup",
                "acceleration_reference",
                "current_acceleration",
                "speed_reference",
                "current_speed",
                "throttle_cmd",
                "brake_cmd",
                "calibration_value",
                "is_full_stop",
                "is_full_stop_soft",
                "is_stop_reason_by_destination",
                "is_stop_reason_by_prdestrian",
                "vehicle_pitch",
                "current_steer_interval",
                "is_wait_steer",
            ):
                if hasattr(lon_debug, key):
                    try:
                        raw[f"debug_simple_lon_{key}"] = getattr(lon_debug, key)
                    except Exception:
                        raw[f"debug_simple_lon_{key}"] = "<unreadable>"
        lat_debug = getattr(debug_msg, "simple_lat_debug", None) if debug_msg is not None else None
        if lat_debug is not None:
            for key in (
                "lateral_error",
                "heading_error",
                "heading_error_rate",
                "lateral_error_rate",
                "lateral_error_feedback",
                "heading_error_feedback",
                "steer_angle",
                "steer_angle_limited",
                "steering_position",
                "curvature",
                "ref_heading",
                "heading",
                "ref_heading_rate",
                "heading_rate",
                "lateral_acceleration",
                "lateral_jerk",
                "lateral_centripetal_acceleration",
                "ref_speed",
            ):
                if hasattr(lat_debug, key):
                    try:
                        raw[f"debug_simple_lat_{key}"] = getattr(lat_debug, key)
                    except Exception:
                        raw[f"debug_simple_lat_{key}"] = "<unreadable>"
            for prefix, field in (
                ("debug_simple_lat_current_target_point", "current_target_point"),
                ("debug_simple_lat_current_reference_point", "current_reference_point"),
                ("debug_simple_lat_preview_reference_point", "preview_reference_point"),
            ):
                point = getattr(lat_debug, field, None)
                path_point = getattr(point, "path_point", None) if point is not None else None
                if point is not None:
                    raw[f"{prefix}_relative_time"] = self._resolve_nested_float(
                        point,
                        (("relative_time",),),
                    )
                    raw[f"{prefix}_v"] = self._resolve_nested_float(
                        point,
                        (("v",),),
                    )
                if path_point is not None:
                    for suffix in ("x", "y", "theta", "kappa", "dkappa", "s"):
                        raw[f"{prefix}_{suffix}"] = self._resolve_nested_float(
                            path_point,
                            ((suffix,),),
                        )
        raw["engage_advice"] = self._proto_scalar(self._resolve_nested_value(cmd, (("engage_advice", "advice"), ("engage_advice",))))
        input_debug = getattr(debug_msg, "input_debug", None) if debug_msg is not None else None
        if input_debug is not None:
            raw["debug_input_trajectory_header_timestamp_sec"] = self._resolve_nested_float(
                input_debug,
                (("trajectory_header", "timestamp_sec"),),
            )
            raw["debug_input_trajectory_header_sequence_num"] = self._header_sequence_num(
                getattr(input_debug, "trajectory_header", None)
            )
            raw["debug_input_latest_replan_trajectory_header_timestamp_sec"] = self._resolve_nested_float(
                input_debug,
                (("latest_replan_trajectory_header", "timestamp_sec"),),
            )
            raw["debug_input_latest_replan_trajectory_header_sequence_num"] = self._header_sequence_num(
                getattr(input_debug, "latest_replan_trajectory_header", None)
            )
        if hasattr(cmd, "trajectory_fraction"):
            raw["trajectory_fraction"] = self._resolve_nested_float(
                cmd,
                (("trajectory_fraction",),),
            )
        return raw

    def _select_steering_field(self, raw_fields: Dict[str, Any], *, physical_mode: bool) -> Tuple[str, float]:
        return select_steering_field_impl(
            raw_fields,
            physical_mode=physical_mode,
            physical_steer_field_priority=self.physical_steer_field_priority,
            coerce_float=self._coerce_float,
        )

    @staticmethod
    def _normalize_steering_command(steer_value: float, *, physical_mode: bool) -> float:
        return normalize_steering_command_impl(steer_value, physical_mode=physical_mode)

    def _apply_zero_hold(self, throttle_cmd: float, brake_cmd: float, now_sec: float) -> float:
        throttle = float(throttle_cmd)
        if throttle > 0.01:
            self._last_nonzero_throttle = throttle
            self._last_nonzero_ts = now_sec
            return throttle
        if (
            self.zero_hold_sec > 0.0
            and brake_cmd <= 0.0
            and self._last_nonzero_throttle > 0.0
            and (now_sec - self._last_nonzero_ts) <= self.zero_hold_sec
        ):
            return self._last_nonzero_throttle
        return throttle

    def _control_mapping_config(self) -> ControlMappingConfig:
        return ControlMappingConfig(
            throttle_scale=float(self.throttle_scale),
            brake_scale=float(self.brake_scale),
            steer_scale=float(self.steer_scale),
            steer_sign=float(self.steer_sign),
            brake_deadzone=float(self.brake_deadzone),
            physical_allow_legacy_fallback=bool(self.physical_allow_legacy_fallback),
            physical_apollo_max_steer_angle_deg=float(self.physical_apollo_max_steer_angle_deg),
            physical_apollo_max_accel_mps2=float(self.physical_apollo_max_accel_mps2),
            physical_apollo_max_decel_mps2=float(self.physical_apollo_max_decel_mps2),
            physical_use_top_level_acceleration=bool(self.physical_use_top_level_acceleration),
            physical_use_lon_debug=bool(self.physical_use_lon_debug),
            physical_steer_field_priority=tuple(self.physical_steer_field_priority),
            physical_acceleration_field_priority=tuple(self.physical_acceleration_field_priority),
        )

    def _legacy_map_base_controls(
        self,
        *,
        raw_throttle: float,
        raw_brake: float,
        raw_steer: float,
        now_sec: float,
    ) -> Dict[str, Any]:
        return legacy_map_base_controls_impl(
            raw_throttle=raw_throttle,
            raw_brake=raw_brake,
            raw_steer=raw_steer,
            now_sec=now_sec,
            config=self._control_mapping_config(),
            apply_zero_hold=self._apply_zero_hold,
        )

    def _derive_longitudinal_targets(
        self,
        *,
        raw_fields: Dict[str, Any],
        raw_throttle: float,
        raw_brake: float,
    ) -> Dict[str, Any]:
        return derive_longitudinal_targets_impl(
            raw_fields=raw_fields,
            raw_throttle=raw_throttle,
            raw_brake=raw_brake,
            config=self._control_mapping_config(),
        )

    def _physical_map_base_controls(
        self,
        *,
        raw_fields: Dict[str, Any],
        raw_throttle: float,
        raw_brake: float,
        raw_steer: float,
        now_sec: float,
    ) -> Dict[str, Any]:
        return physical_map_base_controls_impl(
            raw_fields=raw_fields,
            raw_throttle=raw_throttle,
            raw_brake=raw_brake,
            raw_steer=raw_steer,
            now_sec=now_sec,
            config=self._control_mapping_config(),
            calibration=self._actuator_calibration,
            latest_speed_mps=self._latest_speed_mps,
            apply_zero_hold=self._apply_zero_hold,
        )

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
        probe = self._lane_projection_probe(x, y)
        if not bool(probe.get("available", False)):
            return x, y, probe
        snapped_x = float(probe["proj_x"])
        snapped_y = float(probe["proj_y"])
        probe["applied"] = True
        return snapped_x, snapped_y, probe

    def _evaluate_snap_candidate(self, x: float, y: float, vehicle_yaw: float) -> Dict[str, Any]:
        probe = self._lane_projection_probe(x, y)
        heading_diff_deg: Optional[float] = None
        heading_diff_abs = None
        if bool(probe.get("available", False)):
            lane_yaw_deg = self._coerce_float(
                probe.get("lane_yaw_deg"),
                math.degrees(vehicle_yaw),
                "probe.lane_yaw_deg",
                "snap_candidate",
            )
            heading_diff_deg = _wrap_deg(math.degrees(vehicle_yaw) - lane_yaw_deg)
            heading_diff_abs = abs(float(heading_diff_deg))
        trusted_source = bool(probe.get("trusted_lane_centerline", False))
        reject_reason = ""
        accepted = False
        suspicious_rejected = False
        if not bool(probe.get("available", False)):
            reject_reason = str(probe.get("reason", "candidate_unavailable") or "candidate_unavailable")
        elif (not trusted_source) and (not self.auto_routing_snap_allow_untrusted_source):
            reject_reason = "untrusted_snap_source"
        elif heading_diff_abs is not None and heading_diff_abs > self.auto_routing_snap_heading_diff_hard_reject_deg:
            reject_reason = "heading_diff_hard_reject"
            suspicious_rejected = True
        elif heading_diff_abs is not None and heading_diff_abs > self.auto_routing_snap_heading_diff_max_deg:
            reject_reason = "heading_diff_exceeds_max"
        else:
            accepted = True
        out = dict(probe)
        out["heading_diff_to_vehicle_deg"] = heading_diff_deg
        out["heading_diff_to_vehicle_deg_abs"] = heading_diff_abs
        out["accepted"] = accepted
        out["rejected"] = bool(bool(probe.get("available", False)) and not accepted)
        out["reject_reason"] = reject_reason
        out["suspicious_snap_rejected"] = suspicious_rejected
        out["source_trusted_lane_centerline"] = trusted_source
        return out

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

    def _odom_to_loc(self, odom: Odometry, *, direct_world_frame: Optional[int] = None):
        loc = self.localization_pb2.LocalizationEstimate()
        header_ts_sec = self._command_now_sec()
        self._record_timing_from_odom(
            odom,
            wall_time_sec=header_ts_sec,
            direct_world_frame=direct_world_frame,
        )
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
        physical_mode = self.actuator_mapping_mode == "physical"
        throttle_pct = self._coerce_float(raw_fields.get("throttle", 0.0), 0.0, "throttle", "apollo.control")
        brake_pct = self._coerce_float(raw_fields.get("brake", 0.0), 0.0, "brake", "apollo.control")
        steer_source, steer_pct = self._select_steering_field(
            raw_fields,
            physical_mode=physical_mode,
        )
        raw_throttle = _clamp(throttle_pct / 100.0, 0.0, 1.0)
        raw_brake = _clamp(brake_pct / 100.0, 0.0, 1.0)
        raw_steer = self._normalize_steering_command(steer_pct, physical_mode=physical_mode)
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
            "steering_field_priority": (
                list(self.physical_steer_field_priority)
                if self.actuator_mapping_mode == "physical"
                else ["steering_target", "steering_percentage", "steering", "steering_rate"]
            ),
            "raw_steer_value": steer_pct,
            "acceleration_mps2": _safe_float(raw_fields.get("acceleration"), float("nan")),
            "speed_mps": _safe_float(raw_fields.get("speed"), float("nan")),
            "debug_simple_lon_acceleration_cmd_mps2": _safe_float(
                raw_fields.get("debug_simple_lon_acceleration_cmd"), float("nan")
            ),
            "debug_simple_lon_acceleration_lookup_mps2": _safe_float(
                raw_fields.get("debug_simple_lon_acceleration_lookup"), float("nan")
            ),
            "debug_simple_lon_throttle_cmd_pct": _safe_float(
                raw_fields.get("debug_simple_lon_throttle_cmd"), float("nan")
            ),
            "debug_simple_lon_brake_cmd_pct": _safe_float(
                raw_fields.get("debug_simple_lon_brake_cmd"), float("nan")
            ),
            "debug_simple_lat_lateral_error_m": _safe_float(
                raw_fields.get("debug_simple_lat_lateral_error"), float("nan")
            ),
            "debug_simple_lat_heading_error_rad": _safe_float(
                raw_fields.get("debug_simple_lat_heading_error"), float("nan")
            ),
            "debug_simple_lat_heading_error_rate_radps": _safe_float(
                raw_fields.get("debug_simple_lat_heading_error_rate"), float("nan")
            ),
            "debug_simple_lat_lateral_error_rate_mps": _safe_float(
                raw_fields.get("debug_simple_lat_lateral_error_rate"), float("nan")
            ),
            "debug_simple_lat_curvature": _safe_float(
                raw_fields.get("debug_simple_lat_curvature"), float("nan")
            ),
            "debug_simple_lat_ref_heading_rad": _safe_float(
                raw_fields.get("debug_simple_lat_ref_heading"), float("nan")
            ),
            "debug_simple_lat_heading_rad": _safe_float(
                raw_fields.get("debug_simple_lat_heading"), float("nan")
            ),
            "debug_simple_lat_ref_heading_rate_radps": _safe_float(
                raw_fields.get("debug_simple_lat_ref_heading_rate"), float("nan")
            ),
            "debug_simple_lat_heading_rate_radps": _safe_float(
                raw_fields.get("debug_simple_lat_heading_rate"), float("nan")
            ),
            "debug_simple_lat_lateral_acceleration_mps2": _safe_float(
                raw_fields.get("debug_simple_lat_lateral_acceleration"), float("nan")
            ),
            "debug_simple_lat_lateral_jerk_mps3": _safe_float(
                raw_fields.get("debug_simple_lat_lateral_jerk"), float("nan")
            ),
            "debug_simple_lat_target_point_s": _safe_float(
                raw_fields.get("debug_simple_lat_current_target_point_s"), float("nan")
            ),
            "debug_simple_lat_target_point_relative_time_sec": _safe_float(
                raw_fields.get("debug_simple_lat_current_target_point_relative_time"), float("nan")
            ),
            "debug_simple_lat_target_point_theta_rad": _safe_float(
                raw_fields.get("debug_simple_lat_current_target_point_theta"), float("nan")
            ),
            "debug_simple_lat_target_point_kappa": _safe_float(
                raw_fields.get("debug_simple_lat_current_target_point_kappa"), float("nan")
            ),
            "debug_simple_lat_current_reference_point_s": _safe_float(
                raw_fields.get("debug_simple_lat_current_reference_point_s"), float("nan")
            ),
            "debug_simple_lat_preview_reference_point_s": _safe_float(
                raw_fields.get("debug_simple_lat_preview_reference_point_s"), float("nan")
            ),
        }
        def _nonzero_int_or_none(value: Any) -> Optional[int]:
            try:
                parsed = int(value) if value is not None else None
            except Exception:
                parsed = None
            if parsed is None or parsed <= 0:
                return None
            return parsed

        control_input_primary_seq_int = _nonzero_int_or_none(
            raw_fields.get("debug_input_trajectory_header_sequence_num")
        )
        control_input_latest_replan_seq_int = _nonzero_int_or_none(
            raw_fields.get("debug_input_latest_replan_trajectory_header_sequence_num")
        )
        control_input_primary_ts = _finite_or_none(
            raw_fields.get("debug_input_trajectory_header_timestamp_sec")
        )
        control_input_latest_replan_ts = _finite_or_none(
            raw_fields.get("debug_input_latest_replan_trajectory_header_timestamp_sec")
        )
        matched_planning_seq_int = control_input_primary_seq_int
        control_input_candidate_seq_int = (
            control_input_primary_seq_int
            if control_input_primary_seq_int is not None
            else control_input_latest_replan_seq_int
        )
        control_input_candidate_ts = (
            control_input_primary_ts
            if control_input_primary_seq_int is not None
            else control_input_latest_replan_ts
        )
        control_input_candidate_source = (
            "primary"
            if control_input_primary_seq_int is not None
            else ("latest_replan" if control_input_latest_replan_seq_int is not None else "missing")
        )
        now_sec = time.time()
        latest_known_planning = (
            dict(self._planning_last_event) if self._planning_last_event is not None else {}
        )
        exact_primary_planning = self._exact_planning_event_for_seq(control_input_primary_seq_int)
        exact_latest_replan_planning = self._exact_planning_event_for_seq(control_input_latest_replan_seq_int)
        exact_candidate_planning = self._exact_planning_event_for_seq(control_input_candidate_seq_int)
        exact_matched_planning = self._exact_planning_event_for_seq(matched_planning_seq_int)
        matched_planning = self._latest_planning_event_for_seq(matched_planning_seq_int)
        effective_planning = matched_planning or latest_known_planning or {}
        latest_known_planning_seq = (
            int(latest_known_planning.get("planning_header_sequence_num"))
            if latest_known_planning.get("planning_header_sequence_num") is not None
            else None
        )
        effective_planning_source = (
            "matched_seq"
            if exact_matched_planning is not None
            else ("latest_known_fallback" if latest_known_planning else "missing")
        )
        latest_known_seq_gap = (
            int(latest_known_planning_seq - matched_planning_seq_int)
            if latest_known_planning_seq is not None and matched_planning_seq_int is not None
            else None
        )
        candidate_latest_known_seq_gap = (
            int(latest_known_planning_seq - control_input_candidate_seq_int)
            if latest_known_planning_seq is not None and control_input_candidate_seq_int is not None
            else None
        )
        latest_planning_ts = _finite_or_none(effective_planning.get("timestamp"))
        latest_planning_points = int(effective_planning.get("trajectory_point_count", 0) or 0)
        latest_planning_parse_success = bool(
            effective_planning.get("planning_message_parsed_successfully", False)
        )
        latest_planning_header_ts = _finite_or_none(
            effective_planning.get("planning_header_timestamp_sec")
        )
        latest_planning_age_ms = (
            ((now_sec - latest_planning_ts) * 1000.0) if latest_planning_ts is not None else None
        )
        control_now_relative_to_planning_header_sec = (
            (now_sec - latest_planning_header_ts)
            if latest_planning_header_ts is not None
            else None
        )
        latest_traj_first_rel = _finite_or_none(
            effective_planning.get("trajectory_relative_time_min_sec")
        )
        latest_traj_last_rel = _finite_or_none(
            effective_planning.get("trajectory_relative_time_max_sec")
        )
        latest_traj_all_expired = bool(
            control_now_relative_to_planning_header_sec is not None
            and latest_traj_last_rel is not None
            and control_now_relative_to_planning_header_sec > (latest_traj_last_rel + 0.05)
        )
        latest_traj_not_started = bool(
            control_now_relative_to_planning_header_sec is not None
            and latest_traj_first_rel is not None
            and control_now_relative_to_planning_header_sec < (latest_traj_first_rel - 0.05)
        )
        planning_lateral_contract_valid = bool(
            latest_planning_points > 0
            and latest_planning_parse_success
            and latest_planning_age_ms is not None
            and latest_planning_age_ms <= self.trajectory_contract_lateral_guard_max_planning_age_ms
            and control_now_relative_to_planning_header_sec is not None
            and latest_traj_first_rel is not None
            and latest_traj_last_rel is not None
            and (not latest_traj_all_expired)
            and (not latest_traj_not_started)
        )
        planning_lateral_contract_reason = ""
        if not planning_lateral_contract_valid:
            if latest_planning_ts is None:
                planning_lateral_contract_reason = "no_recent_planning_message"
            elif not latest_planning_parse_success:
                planning_lateral_contract_reason = "planning_parse_failed"
            elif latest_planning_points <= 0:
                planning_lateral_contract_reason = "zero_trajectory_points"
            elif latest_planning_age_ms is None or (
                latest_planning_age_ms > self.trajectory_contract_lateral_guard_max_planning_age_ms
            ):
                planning_lateral_contract_reason = "stale_planning_message"
            elif latest_traj_all_expired:
                planning_lateral_contract_reason = "trajectory_all_points_expired"
            elif latest_traj_not_started:
                planning_lateral_contract_reason = "trajectory_not_started_yet"
            else:
                planning_lateral_contract_reason = "trajectory_invalid_format"
        base_mapping = (
            self._physical_map_base_controls(
                raw_fields=raw_fields,
                raw_throttle=raw_throttle,
                raw_brake=raw_brake,
                raw_steer=raw_steer,
                now_sec=now_sec,
            )
            if self.actuator_mapping_mode == "physical"
            else self._legacy_map_base_controls(
                raw_throttle=raw_throttle,
                raw_brake=raw_brake,
                raw_steer=raw_steer,
                now_sec=now_sec,
            )
        )
        throttle = float(base_mapping.get("throttle", 0.0))
        brake = float(base_mapping.get("brake", 0.0))
        steer = float(base_mapping.get("steer", 0.0))
        steer_pre = float(base_mapping.get("steer_pre_clamp", steer))
        pose_debug = self.stats.get("last_pose_debug", {}) or {}
        e_y_abs = abs(_safe_float(pose_debug.get("e_y_m"), float("inf")))
        e_psi_abs = abs(_wrap_deg(_safe_float(pose_debug.get("e_psi_deg"), float("inf"))))
        target_curvature_abs = abs(_safe_float(pose_debug.get("target_curvature"), float("inf")))
        lane_inside = bool(pose_debug.get("lane_inside", False))
        steer_before_lateral_guards = float(steer)
        straight_lane_zero_steer_applied = False
        low_speed_steer_guard_applied = False
        low_speed_sustained_guard_applied = False
        low_speed_sustained_guard_triggered = False
        low_speed_sustained_guard_trigger_reason = ""
        low_speed_sustained_guard_limited_amount = 0.0
        sustained_lateral_guard_applied = False
        sustained_lateral_guard_triggered = False
        trajectory_contract_lateral_guard_applied = False
        trajectory_contract_lateral_guard_limited_amount = 0.0
        force_zero_steer_applied = False
        sustained_lateral_guard_trigger_reason = ""
        sustained_lateral_guard_limited_amount = 0.0
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
            self.trajectory_contract_lateral_guard_enabled
            and (not planning_lateral_contract_valid)
            and self._latest_speed_mps <= self.trajectory_contract_lateral_guard_max_speed_mps
            and (
                abs(raw_steer) >= self.trajectory_contract_lateral_guard_raw_threshold
                or abs(steer) > self.trajectory_contract_lateral_guard_max_abs_steer
            )
        ):
            steer_before_trajectory_contract_guard = float(steer)
            steer = _clamp(
                steer,
                -self.trajectory_contract_lateral_guard_max_abs_steer,
                self.trajectory_contract_lateral_guard_max_abs_steer,
            )
            trajectory_contract_lateral_guard_limited_amount = abs(
                steer_before_trajectory_contract_guard - steer
            )
            trajectory_contract_lateral_guard_applied = (
                trajectory_contract_lateral_guard_limited_amount > 1e-6
            )
        elif (
            self.low_speed_steer_guard_enabled
            and self._latest_speed_mps <= self.low_speed_steer_guard_speed_mps
            and abs(raw_steer) >= 0.95
            and e_y_abs <= self.low_speed_steer_guard_max_e_y_m
            and e_psi_abs <= self.low_speed_steer_guard_max_e_psi_deg
        ):
            steer = _clamp(steer, -self.low_speed_steer_guard_max_abs_steer, self.low_speed_steer_guard_max_abs_steer)
            low_speed_steer_guard_applied = True
        low_speed_sustained_guard_context_ok = (
            self.low_speed_sustained_guard_enabled
            and self._latest_speed_mps <= self.low_speed_sustained_guard_speed_mps
            and target_curvature_abs <= self.low_speed_sustained_guard_max_curvature
            and e_y_abs <= self.low_speed_sustained_guard_max_e_y_m
            and e_psi_abs <= self.low_speed_sustained_guard_max_e_psi_deg
            and (
                not self.low_speed_sustained_guard_require_lane_inside
                or lane_inside
            )
            and not straight_lane_zero_steer_applied
        )
        sustained_guard_context_ok = (
            self.sustained_lateral_guard_enabled
            and self.sustained_lateral_guard_min_speed_mps <= self._latest_speed_mps <= self.sustained_lateral_guard_max_speed_mps
            and target_curvature_abs <= self.sustained_lateral_guard_max_curvature
            and e_y_abs <= self.sustained_lateral_guard_max_e_y_m
            and e_psi_abs <= self.sustained_lateral_guard_max_e_psi_deg
            and not straight_lane_zero_steer_applied
        )
        raw_saturated = abs(raw_steer) >= self.sustained_lateral_guard_raw_threshold
        low_speed_raw_saturated = abs(raw_steer) >= self.low_speed_sustained_guard_raw_threshold
        if low_speed_sustained_guard_context_ok and low_speed_raw_saturated:
            self._low_speed_sustained_guard_saturation_streak += 1
            self._low_speed_sustained_guard_release_streak = 0
        elif (
            self._low_speed_sustained_guard_active
            and low_speed_sustained_guard_context_ok
            and not low_speed_raw_saturated
        ):
            self._low_speed_sustained_guard_release_streak += 1
            if self._low_speed_sustained_guard_release_streak >= self.low_speed_sustained_guard_release_frames:
                self._low_speed_sustained_guard_active = False
                self._low_speed_sustained_guard_saturation_streak = 0
                self._low_speed_sustained_guard_release_streak = 0
        else:
            self._low_speed_sustained_guard_active = False
            self._low_speed_sustained_guard_saturation_streak = 0
            self._low_speed_sustained_guard_release_streak = 0
        if (
            low_speed_sustained_guard_context_ok
            and not self._low_speed_sustained_guard_active
            and self._low_speed_sustained_guard_saturation_streak >= self.low_speed_sustained_guard_trigger_frames
        ):
            self._low_speed_sustained_guard_active = True
            low_speed_sustained_guard_triggered = True
            low_speed_sustained_guard_trigger_reason = "low_speed_sustained_raw_saturation"
            record_lateral_guard_trigger_impl(
                self.stats,
                reason=low_speed_sustained_guard_trigger_reason,
            )
        if self._low_speed_sustained_guard_active:
            steer_before_low_speed_sustained_guard = float(steer)
            steer = _clamp(
                steer,
                -self.low_speed_sustained_guard_max_abs_steer,
                self.low_speed_sustained_guard_max_abs_steer,
            )
            low_speed_sustained_guard_limited_amount = abs(
                steer_before_low_speed_sustained_guard - steer
            )
            low_speed_sustained_guard_applied = low_speed_sustained_guard_limited_amount > 1e-6
        if sustained_guard_context_ok and raw_saturated:
            self._sustained_lateral_guard_saturation_streak += 1
            self._sustained_lateral_guard_release_streak = 0
        elif self._sustained_lateral_guard_active and sustained_guard_context_ok and not raw_saturated:
            self._sustained_lateral_guard_release_streak += 1
            if self._sustained_lateral_guard_release_streak >= self.sustained_lateral_guard_release_frames:
                self._sustained_lateral_guard_active = False
                self._sustained_lateral_guard_saturation_streak = 0
                self._sustained_lateral_guard_release_streak = 0
        else:
            self._sustained_lateral_guard_active = False
            self._sustained_lateral_guard_saturation_streak = 0
            self._sustained_lateral_guard_release_streak = 0
        if (
            sustained_guard_context_ok
            and not self._sustained_lateral_guard_active
            and self._sustained_lateral_guard_saturation_streak >= self.sustained_lateral_guard_trigger_frames
        ):
            self._sustained_lateral_guard_active = True
            sustained_lateral_guard_triggered = True
            sustained_lateral_guard_trigger_reason = "sustained_raw_saturation"
            record_lateral_guard_trigger_impl(
                self.stats,
                reason=sustained_lateral_guard_trigger_reason,
            )
        if self._sustained_lateral_guard_active:
            steer_before_sustained_guard = float(steer)
            steer = _clamp(
                steer,
                -self.sustained_lateral_guard_max_abs_steer,
                self.sustained_lateral_guard_max_abs_steer,
            )
            sustained_lateral_guard_limited_amount = abs(steer_before_sustained_guard - steer)
            sustained_lateral_guard_applied = sustained_lateral_guard_limited_amount > 1e-6
        if self.force_zero_steer_output:
            force_zero_steer_applied = abs(steer) > 1e-6
            steer = 0.0
        if straight_lane_zero_steer_applied:
            increment_policy_counter_impl(self.stats, "straight_lane_zero_steer_apply_count")
        if low_speed_steer_guard_applied:
            increment_policy_counter_impl(self.stats, "low_speed_steer_guard_apply_count")
        if low_speed_sustained_guard_applied:
            increment_policy_counter_impl(self.stats, "low_speed_sustained_guard_apply_count")
        if sustained_lateral_guard_applied:
            increment_policy_counter_impl(self.stats, "lateral_guard_apply_count")
        if trajectory_contract_lateral_guard_applied:
            reason = planning_lateral_contract_reason or "planning_lateral_contract_invalid"
            increment_policy_counter_impl(self.stats, "trajectory_contract_lateral_guard_apply_count")
            increment_policy_reason_count_impl(self.stats, reason=reason)
        if force_zero_steer_applied:
            increment_policy_counter_impl(self.stats, "force_zero_steer_apply_count")
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
            "steer_before_lateral_guards": steer_before_lateral_guards,
            "actuator_mapping_mode": self.actuator_mapping_mode,
            "type": self.node.control_out_type,
            "steer_pre_clamp": steer_pre,
            "steer_clamped": bool(base_mapping.get("steer_clamped", abs(steer_pre) > 1.0)),
            "steer_sign": base_mapping.get("steer_sign", self.steer_sign),
            "throttle_before_boost": throttle_cmd,
            "throttle_after_boost": throttle_after_boost,
            "startup_boost_applied": throttle_after_boost > throttle_cmd,
            "brake_before_deadzone": base_mapping.get("brake_before_deadzone", brake_cmd),
            "brake_after_deadzone": brake_cmd,
            "throttle_mapping_source": base_mapping.get("throttle_source", ""),
            "brake_mapping_source": base_mapping.get("brake_source", ""),
            "steer_mapping_source": base_mapping.get("steer_source", ""),
            "physical_fallback_reason": base_mapping.get("physical_fallback_reason", ""),
            "planning_lateral_contract_valid": planning_lateral_contract_valid,
            "planning_lateral_contract_reason": planning_lateral_contract_reason,
            "planning_lateral_latest_sequence_num": effective_planning.get(
                "planning_header_sequence_num"
            ),
            "planning_lateral_latest_point_count": latest_planning_points,
            "planning_lateral_latest_age_ms": latest_planning_age_ms,
            "planning_lateral_matched_sequence_num": matched_planning_seq_int,
            "planning_lateral_used_effective_match": bool(matched_planning),
            "target_front_wheel_angle_deg": base_mapping.get("target_front_wheel_angle_deg"),
            "target_accel_mps2": base_mapping.get("target_accel_mps2"),
            "target_decel_mps2": base_mapping.get("target_decel_mps2"),
            "target_accel_source": base_mapping.get("target_accel_source", ""),
            "target_decel_source": base_mapping.get("target_decel_source", ""),
            "selected_signed_acceleration_field": base_mapping.get("selected_signed_acceleration_field", ""),
            "mapped_throttle_cmd": base_mapping.get("mapped_throttle_cmd", throttle_cmd),
            "mapped_brake_cmd": base_mapping.get("mapped_brake_cmd", brake_cmd),
            "mapped_carla_steer_cmd": base_mapping.get("mapped_carla_steer_cmd", steer),
            "straight_lane_zero_steer_applied": straight_lane_zero_steer_applied,
            "low_speed_steer_guard_applied": low_speed_steer_guard_applied,
            "low_speed_sustained_guard_enabled": self.low_speed_sustained_guard_enabled,
            "low_speed_sustained_guard_active": self._low_speed_sustained_guard_active,
            "low_speed_sustained_guard_applied": low_speed_sustained_guard_applied,
            "low_speed_sustained_guard_triggered": low_speed_sustained_guard_triggered,
            "low_speed_sustained_guard_trigger_reason": low_speed_sustained_guard_trigger_reason,
            "low_speed_sustained_guard_limited_amount": low_speed_sustained_guard_limited_amount,
            "low_speed_sustained_guard_saturation_streak": self._low_speed_sustained_guard_saturation_streak,
            "sustained_lateral_guard_enabled": self.sustained_lateral_guard_enabled,
            "sustained_lateral_guard_active": self._sustained_lateral_guard_active,
            "sustained_lateral_guard_applied": sustained_lateral_guard_applied,
            "sustained_lateral_guard_triggered": sustained_lateral_guard_triggered,
            "sustained_lateral_guard_trigger_reason": sustained_lateral_guard_trigger_reason,
            "sustained_lateral_guard_limited_amount": sustained_lateral_guard_limited_amount,
            "sustained_lateral_guard_saturation_streak": self._sustained_lateral_guard_saturation_streak,
            "trajectory_contract_lateral_guard_applied": trajectory_contract_lateral_guard_applied,
            "trajectory_contract_lateral_guard_limited_amount": trajectory_contract_lateral_guard_limited_amount,
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
            "top_level_acceleration_mps2": base_mapping.get("top_level_acceleration_mps2"),
            "debug_longitudinal_acceleration_cmd_mps2": base_mapping.get(
                "debug_longitudinal_acceleration_cmd_mps2"
            ),
            "debug_longitudinal_acceleration_lookup_mps2": base_mapping.get(
                "debug_longitudinal_acceleration_lookup_mps2"
            ),
        }
        control_ts = self._command_now_sec()
        raw_dump = {
            "ts_sec": control_ts,
            "raw_control_msg_dump": raw_fields,
            "parsed_control": dict(self.stats["last_control_in"]),
            "output_to_carla": dict(self.stats["last_control_out"]),
        }
        self.stats["last_control_raw"] = raw_dump
        if (
            straight_lane_zero_steer_applied
            or low_speed_steer_guard_applied
            or low_speed_sustained_guard_applied
            or low_speed_sustained_guard_triggered
            or sustained_lateral_guard_applied
            or sustained_lateral_guard_triggered
            or force_zero_steer_applied
        ):
            self._record_lateral_guard_event(
                {
                    "timestamp": control_ts,
                    **self._timing_snapshot(
                        event_wall_time_sec=control_ts,
                        control_cycle_time_sec=control_ts,
                        latest_planning_age_ms=None,
                    ),
                    "raw_steer": raw_steer,
                    "steer_before_lateral_guards": steer_before_lateral_guards,
                    "steer_after_lateral_guards": steer,
                    "speed_mps": self._latest_speed_mps,
                    "e_y_m": _finite_or_none(pose_debug.get("e_y_m")),
                    "e_psi_deg": _finite_or_none(pose_debug.get("e_psi_deg")),
                    "target_curvature": _finite_or_none(pose_debug.get("target_curvature")),
                    "straight_lane_zero_steer_applied": straight_lane_zero_steer_applied,
                    "low_speed_steer_guard_applied": low_speed_steer_guard_applied,
                    "low_speed_sustained_guard_enabled": self.low_speed_sustained_guard_enabled,
                    "low_speed_sustained_guard_active": self._low_speed_sustained_guard_active,
                    "low_speed_sustained_guard_applied": low_speed_sustained_guard_applied,
                    "low_speed_sustained_guard_triggered": low_speed_sustained_guard_triggered,
                    "low_speed_sustained_guard_trigger_reason": low_speed_sustained_guard_trigger_reason,
                    "low_speed_sustained_guard_limited_amount": low_speed_sustained_guard_limited_amount,
                    "low_speed_sustained_guard_saturation_streak": self._low_speed_sustained_guard_saturation_streak,
                    "sustained_lateral_guard_enabled": self.sustained_lateral_guard_enabled,
                    "sustained_lateral_guard_active": self._sustained_lateral_guard_active,
                    "sustained_lateral_guard_applied": sustained_lateral_guard_applied,
                    "sustained_lateral_guard_triggered": sustained_lateral_guard_triggered,
                    "sustained_lateral_guard_trigger_reason": sustained_lateral_guard_trigger_reason,
                    "sustained_lateral_guard_limited_amount": sustained_lateral_guard_limited_amount,
                    "sustained_lateral_guard_saturation_streak": self._sustained_lateral_guard_saturation_streak,
                    "force_zero_steer_output": self.force_zero_steer_output,
                    "force_zero_steer_applied": force_zero_steer_applied,
                }
            )
        latest_planning = latest_known_planning if latest_known_planning else None
        latest_planning_ts = _finite_or_none(effective_planning.get("timestamp"))
        latest_planning_header_ts = _finite_or_none(
            effective_planning.get("planning_header_timestamp_sec")
        )
        latest_planning_seq = (
            int(effective_planning.get("planning_header_sequence_num"))
            if effective_planning.get("planning_header_sequence_num") is not None
            else None
        )
        latest_planning_points = int(effective_planning.get("trajectory_point_count", 0) or 0)
        latest_planning_parse_success = bool(
            effective_planning.get("planning_message_parsed_successfully", False)
        )
        age_ms = ((control_ts - latest_planning_ts) * 1000.0) if latest_planning_ts is not None else None
        now_rel_to_planning = (
            (control_ts - latest_planning_header_ts) if latest_planning_header_ts is not None else None
        )
        traj_first_rel = _finite_or_none(effective_planning.get("trajectory_relative_time_min_sec"))
        traj_last_rel = _finite_or_none(effective_planning.get("trajectory_relative_time_max_sec"))
        traj_all_expired = bool(
            now_rel_to_planning is not None
            and traj_last_rel is not None
            and now_rel_to_planning > (traj_last_rel + 0.05)
        )
        traj_not_started = bool(
            now_rel_to_planning is not None
            and traj_first_rel is not None
            and now_rel_to_planning < (traj_first_rel - 0.05)
        )
        traj_window_valid = bool(
            latest_planning_points > 0
            and now_rel_to_planning is not None
            and traj_first_rel is not None
            and traj_last_rel is not None
            and (not traj_all_expired)
            and (not traj_not_started)
        )
        latest_known_seq = (
            int((latest_planning or {}).get("planning_header_sequence_num"))
            if (latest_planning or {}).get("planning_header_sequence_num") is not None
            else None
        )
        used_planning = bool(
            matched_planning_seq_int is not None and latest_planning_points > 0 and latest_planning_parse_success
        )
        used_cached = bool(
            used_planning
            and latest_known_seq is not None
            and matched_planning_seq_int is not None
            and matched_planning_seq_int < latest_known_seq
        )
        timing = self._timing_snapshot(
            event_wall_time_sec=control_ts,
            control_cycle_time_sec=control_ts,
            latest_planning_age_ms=age_ms,
        )
        control_consume_live = {
            "event_type": "control_success",
            "timestamp": control_ts,
            "wall_time_sec": timing.get("wall_time_sec"),
            "sim_time_sec": timing.get("sim_time_sec"),
            "world_frame": timing.get("world_frame"),
            "tick_owner": timing.get("tick_owner"),
            "bridge_is_tick_owner": timing.get("bridge_is_tick_owner"),
            "control_cycle_index": int(self.stats.get("control_rx_count", 0)),
            "latest_planning_msg_received": latest_planning_ts is not None,
            "latest_planning_msg_timestamp": latest_planning_ts,
            "latest_planning_msg_age_ms": age_ms,
            "latest_planning_msg_sequence_num": latest_planning_seq,
            "control_input_trajectory_header_sequence_num": matched_planning_seq_int,
            "control_input_primary_trajectory_header_sequence_num": control_input_primary_seq_int,
            "control_input_primary_trajectory_header_timestamp_sec": control_input_primary_ts,
            "control_input_latest_replan_trajectory_header_sequence_num": control_input_latest_replan_seq_int,
            "control_input_latest_replan_trajectory_header_timestamp_sec": control_input_latest_replan_ts,
            "control_input_candidate_trajectory_header_sequence_num": control_input_candidate_seq_int,
            "control_input_candidate_trajectory_header_timestamp_sec": control_input_candidate_ts,
            "control_input_candidate_source": control_input_candidate_source,
            "matched_planning_event_found_exactly": exact_matched_planning is not None,
            "matched_primary_planning_event_found_exactly": exact_primary_planning is not None,
            "matched_latest_replan_planning_event_found_exactly": exact_latest_replan_planning is not None,
            "matched_candidate_planning_event_found_exactly": exact_candidate_planning is not None,
            "effective_planning_source": effective_planning_source,
            "latest_known_planning_sequence_num": latest_known_planning_seq,
            "latest_known_planning_sequence_gap": latest_known_seq_gap,
            "candidate_latest_known_planning_sequence_gap": candidate_latest_known_seq_gap,
            "latest_planning_trajectory_point_count": latest_planning_points,
            "latest_planning_parse_success": latest_planning_parse_success,
            "trajectory_first_point_relative_time": traj_first_rel,
            "trajectory_last_point_relative_time": traj_last_rel,
            "control_now_relative_to_planning_header_sec": now_rel_to_planning,
            "trajectory_all_points_expired": traj_all_expired,
            "trajectory_not_started_yet": traj_not_started,
            "trajectory_time_window_valid": traj_window_valid,
            "control_used_planning_trajectory": used_planning,
            "control_used_cached_trajectory": used_cached,
            "control_had_no_trajectory": False,
            "reject_reason": "",
            "auto_drive_mode": self._proto_scalar(raw_fields.get("driving_mode")),
            "engage_state": self._proto_scalar(raw_fields.get("engage_advice")),
            "engage_advice": self._proto_scalar(raw_fields.get("engage_advice")),
            "estop_flag": bool(estop),
            "chassis_speed_mps": _finite_or_none(raw_fields.get("speed")),
            "gear": self._proto_scalar(raw_fields.get("gear_location")),
            "drive_mode": self._proto_scalar(raw_fields.get("driving_mode")),
            "planning_header_sequence_num_used": matched_planning_seq_int,
            "planning_header_timestamp_sec_used": _finite_or_none(
                effective_planning.get("planning_header_timestamp_sec")
            ),
        }
        self._append_jsonl(self.control_trajectory_consume_live_path, control_consume_live)
        if self.debug_dump_control_raw:
            self._append_jsonl(
                self.apollo_control_raw_path,
                {
                    "ts_sec": control_ts,
                    "apollo_control_raw": raw_fields,
                    "selected_steering_field": steer_source,
                },
            )
            self._append_jsonl(
                self.bridge_control_decode_path,
                {
                    "ts_sec": control_ts,
                    "mapping_mode": self.actuator_mapping_mode,
                    "selected_steering_field": steer_source,
                    "steering_field_priority": self.stats["last_control_in"].get("steering_field_priority", []),
                    "raw_throttle": raw_throttle,
                    "raw_brake": raw_brake,
                    "raw_steer": raw_steer,
                    "commanded_steer_pre_lateral_guards": steer_before_lateral_guards,
                    "commanded_throttle": throttle_after_boost,
                    "commanded_brake": brake_cmd,
                    "commanded_steer": steer,
                    "planning_lateral_contract_valid": self.stats["last_control_out"].get(
                        "planning_lateral_contract_valid", False
                    ),
                    "planning_lateral_contract_reason": self.stats["last_control_out"].get(
                        "planning_lateral_contract_reason", ""
                    ),
                    "planning_lateral_latest_sequence_num": self.stats["last_control_out"].get(
                        "planning_lateral_latest_sequence_num"
                    ),
                    "planning_lateral_latest_point_count": self.stats["last_control_out"].get(
                        "planning_lateral_latest_point_count"
                    ),
                    "straight_lane_zero_steer_applied": self.stats["last_control_out"].get(
                        "straight_lane_zero_steer_applied", False
                    ),
                    "low_speed_steer_guard_applied": self.stats["last_control_out"].get(
                        "low_speed_steer_guard_applied", False
                    ),
                    "sustained_lateral_guard_applied": self.stats["last_control_out"].get(
                        "sustained_lateral_guard_applied", False
                    ),
                    "sustained_lateral_guard_trigger_reason": self.stats["last_control_out"].get(
                        "sustained_lateral_guard_trigger_reason", ""
                    ),
                    "sustained_lateral_guard_limited_amount": self.stats["last_control_out"].get(
                        "sustained_lateral_guard_limited_amount", 0.0
                    ),
                    "trajectory_contract_lateral_guard_applied": self.stats["last_control_out"].get(
                        "trajectory_contract_lateral_guard_applied", False
                    ),
                    "trajectory_contract_lateral_guard_limited_amount": self.stats["last_control_out"].get(
                        "trajectory_contract_lateral_guard_limited_amount", 0.0
                    ),
                    "force_zero_steer_applied": self.stats["last_control_out"].get(
                        "force_zero_steer_applied", False
                    ),
                    "target_front_wheel_angle_deg": self.stats["last_control_out"].get("target_front_wheel_angle_deg"),
                    "target_accel_mps2": self.stats["last_control_out"].get("target_accel_mps2"),
                    "target_decel_mps2": self.stats["last_control_out"].get("target_decel_mps2"),
                    "target_accel_source": self.stats["last_control_out"].get("target_accel_source", ""),
                    "target_decel_source": self.stats["last_control_out"].get("target_decel_source", ""),
                    "selected_signed_acceleration_field": self.stats["last_control_out"].get(
                        "selected_signed_acceleration_field", ""
                    ),
                    "mapped_throttle_cmd": self.stats["last_control_out"].get("mapped_throttle_cmd"),
                    "mapped_brake_cmd": self.stats["last_control_out"].get("mapped_brake_cmd"),
                    "mapped_carla_steer_cmd": self.stats["last_control_out"].get("mapped_carla_steer_cmd"),
                    "physical_fallback_reason": self.stats["last_control_out"].get("physical_fallback_reason", ""),
                    "debug_simple_lat_lateral_error": _finite_or_none(
                        raw_fields.get("debug_simple_lat_lateral_error")
                    ),
                    "debug_simple_lat_heading_error": _finite_or_none(
                        raw_fields.get("debug_simple_lat_heading_error")
                    ),
                    "debug_simple_lat_heading_error_rate": _finite_or_none(
                        raw_fields.get("debug_simple_lat_heading_error_rate")
                    ),
                    "debug_simple_lat_lateral_error_rate": _finite_or_none(
                        raw_fields.get("debug_simple_lat_lateral_error_rate")
                    ),
                    "debug_simple_lat_curvature": _finite_or_none(
                        raw_fields.get("debug_simple_lat_curvature")
                    ),
                    "debug_simple_lat_current_target_point_x": _finite_or_none(
                        raw_fields.get("debug_simple_lat_current_target_point_x")
                    ),
                    "debug_simple_lat_current_target_point_y": _finite_or_none(
                        raw_fields.get("debug_simple_lat_current_target_point_y")
                    ),
                    "debug_simple_lat_current_target_point_theta": _finite_or_none(
                        raw_fields.get("debug_simple_lat_current_target_point_theta")
                    ),
                    "debug_simple_lat_current_target_point_s": _finite_or_none(
                        raw_fields.get("debug_simple_lat_current_target_point_s")
                    ),
                    "debug_simple_lat_current_target_point_kappa": _finite_or_none(
                        raw_fields.get("debug_simple_lat_current_target_point_kappa")
                    ),
                    "debug_simple_lat_current_target_point_dkappa": _finite_or_none(
                        raw_fields.get("debug_simple_lat_current_target_point_dkappa")
                    ),
                    "debug_simple_lat_current_target_point_v": _finite_or_none(
                        raw_fields.get("debug_simple_lat_current_target_point_v")
                    ),
                    "debug_simple_lat_current_target_point_relative_time": _finite_or_none(
                        raw_fields.get("debug_simple_lat_current_target_point_relative_time")
                    ),
                    "trajectory_fraction": _finite_or_none(raw_fields.get("trajectory_fraction")),
                    "control_speed_mps": _finite_or_none(raw_fields.get("speed")),
                    "control_drive_mode": self._proto_scalar(raw_fields.get("driving_mode")),
                    "control_gear_location": self._proto_scalar(raw_fields.get("gear_location")),
                },
            )
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
        now_sec = self._command_now_sec()
        self.stats["routing_response_count"] += 1
        self._routing_last_response_ts_sec = now_sec
        if self._routing_first_response_ts_sec is None:
            self._routing_first_response_ts_sec = now_sec
        last_routing_send_ts = _finite_or_none(self.auto_routing_last_routing_ts)
        if (
            last_routing_send_ts is not None
            and self._routing_first_response_after_last_routing_send_ts_sec is None
        ):
            self._routing_first_response_after_last_routing_send_boundary_ts_sec = last_routing_send_ts
            self._routing_first_response_after_last_routing_send_ts_sec = now_sec
        roads = getattr(msg, "road", None)
        road_count = len(roads) if roads is not None else 0
        self.stats["routing_last_road_count"] = int(road_count)
        if road_count > 0:
            self.stats["routing_success_count"] += 1
            self._routing_last_success_response_ts_sec = now_sec
            if self._routing_first_success_response_ts_sec is None:
                self._routing_first_success_response_ts_sec = now_sec
            if (
                last_routing_send_ts is not None
                and self._routing_first_success_response_after_last_routing_send_ts_sec is None
            ):
                self._routing_first_success_response_after_last_routing_send_boundary_ts_sec = (
                    last_routing_send_ts
                )
                self._routing_first_success_response_after_last_routing_send_ts_sec = now_sec
            self.auto_routing_established = True
            if self.auto_routing_freeze_after_success and (
                (not self.auto_routing_freeze_after_long_route_success_only)
                or str(self._routing_last_request_phase) == "long"
            ):
                self._routing_freeze_active = True
            if self.auto_routing_pending_goal is not None:
                self.auto_routing_active_goal = self.auto_routing_pending_goal
        else:
            self.stats["routing_empty_count"] += 1
        self.stats["planning"] = self._planning_status()
        self._write_health_summary()
        self._write_planning_topic_debug_summary()

    def _resolve_map_file(self) -> Optional[Path]:
        if self.apollo_runtime_map_dir:
            runtime_root = Path(self.apollo_runtime_map_dir).expanduser()
            if not runtime_root.is_absolute():
                runtime_root = (self.apollo_root / runtime_root).resolve()
            for candidate in ("base_map.txt", "base_map.xml", "sim_map.txt", "sim_map.xml"):
                path = runtime_root / candidate
                if path.exists():
                    return path.resolve()
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

    def _resolve_map_dir(self) -> Optional[Path]:
        if self.apollo_runtime_map_dir:
            runtime_root = Path(self.apollo_runtime_map_dir).expanduser()
            if not runtime_root.is_absolute():
                runtime_root = (self.apollo_root / runtime_root).resolve()
            if runtime_root.exists():
                return runtime_root.resolve()
        map_file = self._resolve_map_file()
        if map_file is not None:
            return map_file.parent
        guessed = self._guess_map_file()
        return guessed.parent if guessed is not None else None

    def _load_lane_centerline_segments(
        self, map_file: Path, *, source_type: str
    ) -> Tuple[List[Tuple[float, float, float, float]], Dict[str, Any]]:
        polylines = _extract_lane_centerline_polylines(map_file)
        segments = _build_map_segments_from_polylines(polylines)
        meta = {
            "enabled": bool(segments),
            "map_file": str(map_file),
            "points": sum(len(poly) for poly in polylines),
            "polylines": len(polylines),
            "segments": len(segments),
            "source_type": source_type,
            "risk_level": "low",
            "trusted_lane_centerline": True,
        }
        return segments, meta

    def _load_legacy_map_segments(
        self, map_file: Path
    ) -> Tuple[List[Tuple[float, float, float, float]], Dict[str, Any]]:
        points = _extract_xy_series(map_file)
        segments = _build_map_segments(points)
        meta = {
            "enabled": bool(segments),
            "map_file": str(map_file),
            "points": len(points),
            "segments": len(segments),
            "source_type": "base_map_text_xy_heuristic",
            "risk_level": "high",
            "trusted_lane_centerline": False,
        }
        return segments, meta

    def _load_map_lane_metadata(self) -> None:
        map_file = self._resolve_map_file()
        if map_file is None or not map_file.exists():
            self.map_lane_metadata = {}
            self.map_lane_metadata_source_file = str(map_file) if map_file is not None else ""
            self.map_lane_metadata_source_type = "missing"
            self.stats["map_lane_metadata"] = {
                "enabled": False,
                "reason": "map_file_missing",
                "map_file": str(map_file) if map_file is not None else "",
                "source_type": "missing",
                "apollo_runtime_map_dir": self.apollo_runtime_map_dir,
                "map_contract_invalid": self.map_contract_invalid,
                "map_contract_mismatch_reason": self.map_contract_mismatch_reason,
            }
            return
        try:
            metadata = _extract_road_section_lane_metadata(map_file)
            junction_lane_count = sum(1 for item in metadata.values() if bool(item.get("is_junction")))
            self.map_lane_metadata = metadata
            self.map_lane_metadata_source_file = str(map_file)
            self.map_lane_metadata_source_type = "road_section_lane_metadata"
            self.stats["map_lane_metadata"] = {
                "enabled": bool(metadata),
                "map_file": str(map_file),
                "source_type": "road_section_lane_metadata",
                "lane_count": len(metadata),
                "junction_lane_count": junction_lane_count,
                "apollo_runtime_map_dir": self.apollo_runtime_map_dir,
                "map_contract_invalid": self.map_contract_invalid,
                "map_contract_mismatch_reason": self.map_contract_mismatch_reason,
            }
        except Exception as exc:
            self.map_lane_metadata = {}
            self.map_lane_metadata_source_file = str(map_file)
            self.map_lane_metadata_source_type = "parse_error"
            self.stats["map_lane_metadata"] = {
                "enabled": False,
                "reason": f"parse_error:{exc}",
                "map_file": str(map_file),
                "source_type": "parse_error",
                "apollo_runtime_map_dir": self.apollo_runtime_map_dir,
                "map_contract_invalid": self.map_contract_invalid,
                "map_contract_mismatch_reason": self.map_contract_mismatch_reason,
            }

    def _load_map_geometry(self) -> None:
        map_dir = self._resolve_map_dir()
        map_file = self._resolve_map_file()
        if map_dir is None and (map_file is None or not map_file.exists()):
            self.map_segments = []
            self.map_geometry_source_file = ""
            self.map_geometry_source_type = "missing"
            self.map_geometry_trusted_lane_centerline = False
            self.stats["map_geometry"] = {
                "enabled": False,
                "reason": "map_file_missing",
                "map_file": str(map_file) if map_file is not None else "",
                "source_type": "missing",
                "apollo_runtime_map_dir": self.apollo_runtime_map_dir,
                "map_contract_invalid": self.map_contract_invalid,
                "map_contract_mismatch_reason": self.map_contract_mismatch_reason,
            }
            return
        try:
            selected_segments: List[Tuple[float, float, float, float]] = []
            selected_meta: Dict[str, Any] = {}
            source_mode = self.auto_routing_snap_source_mode
            allow_lane_centerline = source_mode in {
                "lane_centerline_only",
                "lane_centerline_preferred",
            }
            allow_legacy_base_map = source_mode in {
                "legacy_base_map_xy",
                "lane_centerline_preferred",
            }
            if allow_lane_centerline and map_dir is not None:
                centerline_candidates = [
                    (map_dir / "routing_map.txt", "routing_map_lane_centerline"),
                    (map_dir / "sim_map.txt", "sim_map_lane_centerline"),
                ]
                for candidate_file, source_type in centerline_candidates:
                    if not candidate_file.exists():
                        continue
                    segments, meta = self._load_lane_centerline_segments(
                        candidate_file,
                        source_type=source_type,
                    )
                    if segments:
                        selected_segments = segments
                        selected_meta = meta
                        break
            if (
                not selected_segments
                and allow_legacy_base_map
                and map_file is not None
                and map_file.exists()
            ):
                selected_segments, selected_meta = self._load_legacy_map_segments(map_file)
            if not selected_segments and source_mode == "lane_centerline_only":
                selected_meta = {
                    "enabled": False,
                    "reason": "lane_centerline_source_missing",
                    "map_file": str(map_dir) if map_dir is not None else "",
                    "source_type": "lane_centerline_only_missing",
                    "risk_level": "high",
                    "trusted_lane_centerline": False,
                }
            self.map_segments = selected_segments
            self.map_geometry_source_file = str(selected_meta.get("map_file", ""))
            self.map_geometry_source_type = str(selected_meta.get("source_type", "missing"))
            self.map_geometry_trusted_lane_centerline = bool(
                selected_meta.get("trusted_lane_centerline", False)
            )
            selected_meta["snap_source_mode"] = source_mode
            selected_meta["apollo_runtime_map_dir"] = self.apollo_runtime_map_dir
            selected_meta["map_contract_invalid"] = self.map_contract_invalid
            selected_meta["map_contract_mismatch_reason"] = self.map_contract_mismatch_reason
            self.stats["map_geometry"] = selected_meta
        except Exception as exc:
            self.map_segments = []
            self.map_geometry_source_file = str(map_file) if map_file is not None else ""
            self.map_geometry_source_type = "parse_error"
            self.map_geometry_trusted_lane_centerline = False
            self.stats["map_geometry"] = {
                "enabled": False,
                "reason": f"parse_error:{exc}",
                "map_file": str(map_file),
                "source_type": "parse_error",
                "risk_level": "high",
                "trusted_lane_centerline": False,
                "snap_source_mode": self.auto_routing_snap_source_mode,
                "apollo_runtime_map_dir": self.apollo_runtime_map_dir,
                "map_contract_invalid": self.map_contract_invalid,
                "map_contract_mismatch_reason": self.map_contract_mismatch_reason,
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
        planning_ready_escape = bool(
            self.auto_routing_defer_long_goal_max_wait_sec > 0.0
            and elapsed
            >= (
                float(self.auto_routing_startup_hold_sec)
                + float(self.auto_routing_defer_long_goal_max_wait_sec)
            )
        )
        if self.auto_routing_use_long_goal_after_move:
            if moved:
                in_startup = False
            else:
                in_startup = True
        if (
            not in_startup
            and self.auto_routing_defer_long_goal_until_planning_ready
            and int(self._planning_nonempty_count)
            < max(1, int(self.auto_routing_long_goal_planning_ready_min_nonempty_count))
        ):
            in_startup = not planning_ready_escape
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
            self._update_command_gate_state(
                ts_sec=ts_sec,
                phase=None,
                status="disabled",
                blocking_reason="auto_routing_disabled",
                blocking_detail="bridge.auto_routing.enabled=false",
            )
            return
        if self.routing_writer is None and self.lane_follow_client is None and self.action_client is None:
            self._update_command_gate_state(
                ts_sec=ts_sec,
                phase=None,
                status="blocked",
                blocking_reason="command_interface_unavailable",
                blocking_detail="routing_writer and command clients are all unavailable",
            )
            return
        if self._first_odom_ts is None:
            self._first_odom_ts = ts_sec
        startup_delay_elapsed_sec = ts_sec - self._first_odom_ts
        if startup_delay_elapsed_sec < self.auto_routing_startup_delay_sec:
            startup_delay_remaining_sec = max(0.0, self.auto_routing_startup_delay_sec - startup_delay_elapsed_sec)
            self._update_command_gate_state(
                ts_sec=ts_sec,
                phase="startup",
                status="blocked",
                blocking_reason="startup_delay",
                blocking_detail="waiting for startup delay before first routing command",
                eligible=False,
                startup_delay_remaining_sec=startup_delay_remaining_sec,
            )
            return
        startup_apollo_warmup_elapsed_sec = max(0.0, time.time() - self._bridge_start_wall_sec)
        if (
            self.auto_routing_routing_sent == 0
            and self.auto_routing_startup_apollo_warmup_sec > 0.0
            and startup_apollo_warmup_elapsed_sec < self.auto_routing_startup_apollo_warmup_sec
        ):
            self.stats["last_error"] = "waiting_for_apollo_startup_warmup"
            self._update_command_gate_state(
                ts_sec=ts_sec,
                phase="startup",
                status="blocked",
                blocking_reason="apollo_startup_warmup",
                blocking_detail="waiting for configured Apollo startup warmup window",
                eligible=True,
                apollo_warmup_remaining_sec=max(
                    0.0, self.auto_routing_startup_apollo_warmup_sec - startup_apollo_warmup_elapsed_sec
                ),
            )
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
        raw_x0, raw_y0 = self._clamp_xy_to_bounds(x0, y0)
        start_raw_xy = (raw_x0, raw_y0)
        start_snap_candidate = self._evaluate_snap_candidate(raw_x0, raw_y0, yaw0)
        snap_candidate_available = bool(start_snap_candidate.get("available", False))
        snapped_x = float(start_snap_candidate.get("proj_x", raw_x0)) if snap_candidate_available else raw_x0
        snapped_y = float(start_snap_candidate.get("proj_y", raw_y0)) if snap_candidate_available else raw_y0
        snap_source_type = str(start_snap_candidate.get("source_type", self.map_geometry_source_type or "unknown"))
        heading_diff_to_vehicle_deg = _finite_or_none(start_snap_candidate.get("heading_diff_to_vehicle_deg"))
        heading_diff_abs = (
            abs(float(heading_diff_to_vehicle_deg))
            if heading_diff_to_vehicle_deg is not None
            else None
        )
        snap_applied = False
        snap_rejected = False
        snap_reject_reason = ""
        suspicious_snap_rejected = False
        if self.auto_routing_snap_start_to_lane:
            if bool(start_snap_candidate.get("accepted", False)):
                snap_applied = True
            else:
                snap_rejected = True
                snap_reject_reason = str(
                    start_snap_candidate.get("reject_reason", "snap_rejected") or "snap_rejected"
                )
                suspicious_snap_rejected = bool(start_snap_candidate.get("suspicious_snap_rejected", False))
        x0, y0 = (snapped_x, snapped_y) if snap_applied else (raw_x0, raw_y0)
        start_proj: Dict[str, Any] = dict(start_snap_candidate)
        start_proj["applied"] = snap_applied
        if not self.auto_routing_snap_start_to_lane:
            start_proj["reason"] = "disabled_by_config"
            start_proj["rejected"] = False
            start_proj["reject_reason"] = ""

        start_nudge_applied = False
        start_nudge_effective = 0.0
        start_nudge_heading_deg: Optional[float] = None
        start_nudge_heading_source = "disabled"
        nudged_x: Optional[float] = None
        nudged_y: Optional[float] = None
        if self.auto_routing_start_nudge_m > 0.0:
            nudge_blocked_by_rejected_snap = (
                self.auto_routing_snap_start_to_lane
                and (not snap_applied)
                and self.auto_routing_disable_nudge_when_snap_rejected
            )
            if not nudge_blocked_by_rejected_snap:
                # Increase nudge for later attempts in the same run to escape map
                # seam/boundary cases where start_s is very close to zero or negative.
                start_nudge_effective = self.auto_routing_start_nudge_m + (
                    max(0, int(self.auto_routing_routing_sent)) * self.auto_routing_start_nudge_retry_step_m
                )
                start_nudge_effective = max(self.auto_routing_start_nudge_min_safe_m, start_nudge_effective)
                if self.auto_routing_start_nudge_max_m > 0.0:
                    start_nudge_effective = min(self.auto_routing_start_nudge_max_m, start_nudge_effective)
                nudge_heading = yaw0
                start_nudge_heading_source = "vehicle_yaw"
                if (
                    self.auto_routing_start_nudge_use_lane_heading
                    and snap_applied
                    and heading_diff_abs is not None
                    and heading_diff_abs <= self.auto_routing_lane_heading_nudge_max_heading_diff_deg
                ):
                    lane_yaw_deg = self._coerce_float(
                        start_snap_candidate.get("lane_yaw_deg", math.degrees(yaw0)),
                        math.degrees(yaw0),
                        "start_snap_candidate.lane_yaw_deg",
                        "routing_start_nudge",
                    )
                    nudge_heading = math.radians(lane_yaw_deg)
                    start_nudge_heading_source = "lane_heading"
                nudged_x = x0 + start_nudge_effective * math.cos(nudge_heading)
                nudged_y = y0 + start_nudge_effective * math.sin(nudge_heading)
                nudged_x, nudged_y = self._clamp_xy_to_bounds(nudged_x, nudged_y)
                x0, y0 = nudged_x, nudged_y
                if snap_applied:
                    post_nudge_snap = self._evaluate_snap_candidate(nudged_x, nudged_y, yaw0)
                    if bool(post_nudge_snap.get("accepted", False)):
                        x0 = float(post_nudge_snap.get("proj_x", nudged_x))
                        y0 = float(post_nudge_snap.get("proj_y", nudged_y))
                        start_proj = dict(post_nudge_snap)
                        start_proj["applied"] = True
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
        routing_skipped_due_to_freeze = False
        routing_skipped_due_to_invalid_goal = False
        routing_skipped_due_to_unstable_reference_line = False
        if send_routing_now and self._routing_freeze_active:
            routing_skipped_due_to_freeze = True
            send_routing_now = False
            self.stats["routing_skipped_due_to_freeze_count"] = int(
                self.stats.get("routing_skipped_due_to_freeze_count", 0)
            ) + 1
            if not ignore_roll_active:
                if phase == "startup":
                    self.auto_routing_startup_routing_sent = True
                else:
                    self.auto_routing_long_routing_sent = True
        if not send_routing_now and not send_lane_follow_now and not routing_skipped_due_to_freeze:
            blocking_reason = "command_idle"
            blocking_detail = "no routing or lane-follow command requested on this evaluation"
            if route_ready and (not self.auto_routing_established) and (not route_attempts_left):
                blocking_reason = "routing_not_established_after_max_attempts"
                blocking_detail = "routing path exhausted configured attempts before route establishment"
            elif route_phase_sent and not ignore_roll_active:
                blocking_reason = "routing_phase_already_sent"
                blocking_detail = "routing for the current phase was already sent and cooldown is active"
            elif route_ready and not route_cooldown_ok:
                blocking_reason = "routing_cooldown"
                blocking_detail = "routing resend cooldown is still active"
            elif lane_follow_ready and not lane_follow_cooldown_ok:
                blocking_reason = "lane_follow_cooldown"
                blocking_detail = "lane-follow refresh cooldown is still active"
            if route_ready and (not self.auto_routing_established) and (not route_attempts_left):
                self.stats["last_error"] = "routing_not_established_after_max_attempts"
            self._update_command_gate_state(
                ts_sec=ts_sec,
                phase=phase,
                status="blocked",
                blocking_reason=blocking_reason,
                blocking_detail=blocking_detail,
                eligible=True,
                route_ready=route_ready,
                lane_follow_ready=lane_follow_ready,
                route_cooldown_ok=route_cooldown_ok,
                lane_follow_cooldown_ok=lane_follow_cooldown_ok,
                route_attempts_left=route_attempts_left,
                route_phase_sent=route_phase_sent,
                lane_phase_sent=lane_phase_sent,
                send_routing_now=False,
                send_lane_follow_now=False,
            )
            return
        routing_anchor = self._extract_routing_anchor(snapshot, x0=x0, y0=y0, yaw0=yaw0)
        if self.auto_routing_routing_sent == 0 and self.auto_routing_active_goal is None:
            rx_counts = snapshot.get("rx_counts", {}) or {}
            obstacle_rx = int(rx_counts.get("objects_json", 0)) + int(rx_counts.get("objects3d", 0)) + int(
                rx_counts.get("markers", 0)
            )
            wait_extra_sec = 3.0
            if self.auto_routing_wait_for_obstacle_gt_before_initial_routing and obstacle_rx <= 0:
                self.stats["last_error"] = "waiting_for_obstacle_gt_before_initial_routing"
                self._update_command_gate_state(
                    ts_sec=ts_sec,
                    phase=phase,
                    status="blocked",
                    blocking_reason="waiting_for_obstacle_gt_before_initial_routing",
                    blocking_detail="initial routing is waiting for obstacle GT observability",
                    eligible=True,
                    route_ready=route_ready,
                    lane_follow_ready=lane_follow_ready,
                    route_cooldown_ok=route_cooldown_ok,
                    lane_follow_cooldown_ok=lane_follow_cooldown_ok,
                    route_attempts_left=route_attempts_left,
                    route_phase_sent=route_phase_sent,
                    lane_phase_sent=lane_phase_sent,
                    send_routing_now=send_routing_now,
                    send_lane_follow_now=send_lane_follow_now,
                )
                return
            if (
                self.auto_routing_goal_mode == "front_vehicle_ahead"
                and routing_anchor is None
                and (ts_sec - (self._first_odom_ts or ts_sec))
                < (self.auto_routing_startup_delay_sec + wait_extra_sec)
            ):
                self.stats["last_error"] = "waiting_for_front_anchor_before_initial_routing"
                self._update_command_gate_state(
                    ts_sec=ts_sec,
                    phase=phase,
                    status="blocked",
                    blocking_reason="waiting_for_front_anchor_before_initial_routing",
                    blocking_detail="front_vehicle_ahead goal mode has not materialized an anchor yet",
                    eligible=True,
                    route_ready=route_ready,
                    lane_follow_ready=lane_follow_ready,
                    route_cooldown_ok=route_cooldown_ok,
                    lane_follow_cooldown_ok=lane_follow_cooldown_ok,
                    route_attempts_left=route_attempts_left,
                    route_phase_sent=route_phase_sent,
                    lane_phase_sent=lane_phase_sent,
                    send_routing_now=send_routing_now,
                    send_lane_follow_now=send_lane_follow_now,
                )
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
        if self.auto_routing_snap_goal_to_lane:
            x1, y1, goal_proj = self._snap_xy_to_lane(x1, y1)
        else:
            goal_proj = self._lane_projection_probe(x1, y1)
            goal_proj["reason"] = "disabled_by_config"
            goal_proj["applied"] = False
        command_ts = self._command_now_sec()
        goal_validity = self._evaluate_goal_validity(
            x0=x0,
            y0=y0,
            x1=x1,
            y1=y1,
            phase=phase,
            goal_meta=goal_meta,
            goal_proj=goal_proj,
            command_ts=command_ts,
        )
        pre_fallback_goal_x1, pre_fallback_goal_y1, pre_fallback_goal_z1 = x1, y1, z1
        pre_fallback_goal_meta = dict(goal_meta)
        pre_fallback_goal_proj = dict(goal_proj)
        pre_fallback_goal_validity = dict(goal_validity)
        goal_fallback_applied = False
        original_invalid_goal_reason = str(goal_validity.get("invalid_goal_reason", "") or "")
        if (
            bool(goal_validity.get("invalid_goal"))
            and self.auto_routing_goal_validity_fallback_enabled
        ):
            fallback_x1, fallback_y1, fallback_z1, fallback_meta = self._build_ahead_goal(
                x0,
                y0,
                z0,
                yaw0,
                anchor=routing_anchor,
                phase=phase,
            )
            if self.auto_routing_snap_goal_to_lane:
                fallback_x1, fallback_y1, goal_proj = self._snap_xy_to_lane(fallback_x1, fallback_y1)
            else:
                goal_proj = self._lane_projection_probe(fallback_x1, fallback_y1)
                goal_proj["reason"] = "disabled_by_config"
                goal_proj["applied"] = False
            x1, y1, z1, goal_meta = fallback_x1, fallback_y1, fallback_z1, fallback_meta
            goal_validity = self._evaluate_goal_validity(
                x0=x0,
                y0=y0,
                x1=x1,
                y1=y1,
                phase=phase,
                goal_meta=goal_meta,
                goal_proj=goal_proj,
                command_ts=command_ts,
            )
            goal_validity["fallback_applied"] = True
            goal_validity["fallback_from_invalid_reason"] = original_invalid_goal_reason
            goal_fallback_applied = True
        else:
            goal_validity["fallback_applied"] = False
            goal_validity["fallback_from_invalid_reason"] = ""
        if original_invalid_goal_reason:
            self.stats["invalid_goal_count"] = int(self.stats.get("invalid_goal_count", 0) or 0) + 1
        recent_route_debug = dict(self._planning_last_route_debug_event or {})
        recent_route_debug_ts = _safe_float(recent_route_debug.get("timestamp"))
        recent_route_debug_age_sec = (
            command_ts - recent_route_debug_ts if recent_route_debug_ts is not None else None
        )
        recent_route_debug_fresh = bool(
            recent_route_debug_ts is not None
            and recent_route_debug_age_sec is not None
            and recent_route_debug_age_sec <= max(0.0, self.auto_routing_goal_validity_reference_line_stale_sec)
        )
        if (
            send_routing_now
            and phase == "long"
            and self.auto_routing_skip_invalid_long_route
            and bool(goal_validity.get("invalid_goal"))
        ):
            routing_skipped_due_to_invalid_goal = True
            send_routing_now = False
            self.stats["routing_skipped_due_to_invalid_goal_count"] = int(
                self.stats.get("routing_skipped_due_to_invalid_goal_count", 0) or 0
            ) + 1
            self.auto_routing_long_routing_sent = True
        recent_reference_line_count = int(recent_route_debug.get("reference_line_count", 0) or 0)
        recent_route_segment_count = int(recent_route_debug.get("route_segment_count", 0) or 0)
        recent_reference_line_debug_missing_but_trajectory_nonzero = bool(
            recent_route_debug.get("reference_line_debug_missing_but_trajectory_nonzero")
        )
        recent_lane_follow_map_status = str(
            recent_route_debug.get("lane_follow_map_status", "") or ""
        ).strip()
        lane_follow_map_inconsistent_active = self._lane_follow_map_inconsistent_active(recent_route_debug)
        startup_elapsed_sec = (
            max(0.0, command_ts - float(self._first_odom_ts))
            if self._first_odom_ts is not None
            else None
        )
        long_goal_defer_escape_active = bool(
            self.auto_routing_defer_long_goal_max_wait_sec > 0.0
            and startup_elapsed_sec is not None
            and startup_elapsed_sec
            >= (
                float(self.auto_routing_startup_hold_sec)
                + float(self.auto_routing_defer_long_goal_max_wait_sec)
            )
        )
        route_debug_ready_for_long_phase = bool(
            recent_route_debug_fresh
            and (
                recent_reference_line_count > 0
                or recent_route_segment_count > 0
                or recent_reference_line_debug_missing_but_trajectory_nonzero
            )
        )
        route_debug_ready_reason = "missing_or_stale"
        if recent_route_debug_fresh:
            if recent_reference_line_count > 0:
                route_debug_ready_reason = "reference_line_count"
            elif recent_route_segment_count > 0:
                route_debug_ready_reason = "route_segment_count"
            elif recent_reference_line_debug_missing_but_trajectory_nonzero:
                route_debug_ready_reason = "trajectory_nonzero_debug_missing"
            else:
                route_debug_ready_reason = "fresh_but_empty"
        routing_waiting_for_route_debug_ready = bool(
            send_routing_now
            and phase == "long"
            and not ignore_roll_active
            and self.auto_routing_defer_long_goal_until_route_debug_ready
            and not route_debug_ready_for_long_phase
            and not long_goal_defer_escape_active
        )
        if routing_waiting_for_route_debug_ready:
            send_routing_now = False
            self.stats["routing_waiting_for_route_debug_ready_count"] = int(
                self.stats.get("routing_waiting_for_route_debug_ready_count", 0) or 0
            ) + 1
        unstable_long_reroute_guard_active = bool(
            phase == "long"
            and not routing_waiting_for_route_debug_ready
            and not ignore_roll_active
            and self.auto_routing_suppress_long_phase_reroute_on_unstable_reference_line
            and (
                bool(goal_validity.get("invalid_goal"))
                or (
                    recent_route_debug_fresh
                    and (
                        recent_reference_line_count <= 0
                        and not recent_reference_line_debug_missing_but_trajectory_nonzero
                    )
                )
                or (recent_route_debug_fresh and recent_route_segment_count <= 0)
                or (recent_route_debug_fresh and lane_follow_map_inconsistent_active)
                or bool(self.map_contract_invalid)
            )
        )
        if unstable_long_reroute_guard_active:
            routing_skipped_due_to_unstable_reference_line = True
            if send_routing_now:
                send_routing_now = False
                self.stats["routing_skipped_due_to_unstable_reference_line_count"] = int(
                    self.stats.get("routing_skipped_due_to_unstable_reference_line_count", 0) or 0
                ) + 1
            self.auto_routing_long_routing_sent = True
        self.stats["goal_validity_last"] = dict(goal_validity)
        self._record_goal_validity_event(
            {
                **goal_validity,
                **self._timing_snapshot(event_wall_time_sec=command_ts),
            }
        )
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
            "invalid_goal_reason": original_invalid_goal_reason,
            "goal_fallback_applied": goal_fallback_applied,
        }
        self.stats["last_routing_projection"] = {
            "start": start_proj,
            "goal": goal_proj,
        }
        request_kind, reroute_reason, reroute_trigger_source = self._routing_request_reason(
            phase=phase,
            ignore_roll_active=ignore_roll_active,
            routing_skipped_due_to_freeze=routing_skipped_due_to_freeze,
            routing_skipped_due_to_invalid_goal=routing_skipped_due_to_invalid_goal,
            routing_skipped_due_to_unstable_reference_line=routing_skipped_due_to_unstable_reference_line,
            routing_waiting_for_route_debug_ready=routing_waiting_for_route_debug_ready,
        )
        self.stats["last_routing_reason"] = reroute_reason
        reason_counts = dict(self.stats.get("reroute_reason_counts", {}) or {})
        reason_counts[reroute_reason] = int(reason_counts.get(reroute_reason, 0) or 0) + 1
        self.stats["reroute_reason_counts"] = reason_counts
        gate_status = "ready_to_send" if (send_routing_now or send_lane_follow_now) else "blocked"
        gate_blocking_reason = ""
        gate_blocking_detail = ""
        if not send_routing_now and not send_lane_follow_now:
            if routing_skipped_due_to_freeze:
                gate_blocking_reason = "routing_freeze"
                gate_blocking_detail = "routing send suppressed because freeze-after-success is active"
            elif routing_skipped_due_to_invalid_goal:
                gate_blocking_reason = "invalid_long_goal_skipped"
                gate_blocking_detail = "long-phase invalid goal was skipped by guard"
            elif routing_waiting_for_route_debug_ready:
                gate_blocking_reason = "route_debug_not_ready_for_long_phase"
                gate_blocking_detail = (
                    f"recent route debug not ready for long-phase reroute ({route_debug_ready_reason})"
                )
            elif routing_skipped_due_to_unstable_reference_line:
                gate_blocking_reason = "unstable_reference_line_guard"
                gate_blocking_detail = "long-phase reroute suppressed by unstable reference-line guard"
            elif route_ready and not route_attempts_left and not self.auto_routing_established:
                gate_blocking_reason = "routing_not_established_after_max_attempts"
                gate_blocking_detail = "routing path exhausted configured attempts before route establishment"
            elif route_ready and not route_cooldown_ok:
                gate_blocking_reason = "routing_cooldown"
                gate_blocking_detail = "routing resend cooldown is still active"
            elif lane_follow_ready and not lane_follow_cooldown_ok:
                gate_blocking_reason = "lane_follow_cooldown"
                gate_blocking_detail = "lane-follow refresh cooldown is still active"
            elif route_phase_sent and not ignore_roll_active:
                gate_blocking_reason = "routing_phase_already_sent"
                gate_blocking_detail = "routing for the current phase was already sent"
            else:
                gate_blocking_reason = "command_idle"
                gate_blocking_detail = "gate evaluated without sending a routing or lane-follow command"
        self._update_command_gate_state(
            ts_sec=command_ts,
            phase=phase,
            status=gate_status,
            blocking_reason=gate_blocking_reason,
            blocking_detail=gate_blocking_detail,
            eligible=True,
            ready_to_send=bool(send_routing_now or send_lane_follow_now),
            route_ready=route_ready,
            lane_follow_ready=lane_follow_ready,
            route_cooldown_ok=route_cooldown_ok,
            lane_follow_cooldown_ok=lane_follow_cooldown_ok,
            route_attempts_left=route_attempts_left,
            route_phase_sent=route_phase_sent,
            lane_phase_sent=lane_phase_sent,
            send_routing_now=send_routing_now,
            send_lane_follow_now=send_lane_follow_now,
        )
        raw_to_snapped_start_distance_m = math.hypot(snapped_x - raw_x0, snapped_y - raw_y0)
        snapped_to_nudged_start_distance_m = (
            math.hypot(float(nudged_x) - snapped_x, float(nudged_y) - snapped_y)
            if nudged_x is not None and nudged_y is not None
            else 0.0
        )
        localization_to_final_start_distance_m = math.hypot(
            float(pose_info["map_x"]) - x0,
            float(pose_info["map_y"]) - y0,
        )
        suspicious_snap = bool(
            (heading_diff_to_vehicle_deg is not None and abs(heading_diff_to_vehicle_deg) > self.auto_routing_snap_heading_diff_hard_reject_deg)
            or raw_to_snapped_start_distance_m > 2.5
        )
        routing_request_index = int(self.auto_routing_routing_sent) + 1
        timing = self._timing_snapshot(event_wall_time_sec=command_ts)
        recent_route_debug_snapshot = self._route_debug_observability_snapshot(
            recent_route_debug,
            age_sec=recent_route_debug_age_sec,
        )
        start_projection_summary = self._projection_debug_summary(start_proj)
        goal_projection_summary = self._projection_debug_summary(goal_proj)
        pre_fallback_goal_projection_summary = self._projection_debug_summary(pre_fallback_goal_proj)
        goal_validity_summary = self._goal_validity_debug_summary(goal_validity)
        pre_fallback_goal_validity_summary = self._goal_validity_debug_summary(
            pre_fallback_goal_validity
        )
        seed_pose_summary = self._seed_pose_debug_summary(
            current_x=x0,
            current_y=y0,
            current_yaw=yaw0,
        )
        planning_state_summary = self._planning_decision_snapshot(command_ts=command_ts)
        route_debug_since_last_routing_send = dict(
            planning_state_summary.get("route_debug_since_last_routing_send") or {}
        )
        route_debug_since_first_routing_response = dict(
            planning_state_summary.get(
                "route_debug_since_first_routing_response_after_last_routing_send"
            )
            or {}
        )
        pre_fallback_goal_distance_m = math.hypot(
            float(pre_fallback_goal_x1) - float(x0),
            float(pre_fallback_goal_y1) - float(y0),
        )
        goal_resolution_summary = {
            "requested_goal_mode": pre_fallback_goal_meta.get(
                "requested_goal_mode", self.auto_routing_goal_mode
            ),
            "pre_fallback_goal_mode": pre_fallback_goal_meta.get("goal_mode"),
            "pre_fallback_goal_source": pre_fallback_goal_meta.get("goal_source"),
            "pre_fallback_anchor_mode": pre_fallback_goal_meta.get("anchor_mode"),
            "pre_fallback_goal_distance_m": pre_fallback_goal_distance_m,
            "invalid_goal_detected": bool(original_invalid_goal_reason),
            "invalid_goal_reason": original_invalid_goal_reason,
            "fallback_applied": bool(goal_fallback_applied),
            "fallback_from_invalid_reason": str(
                goal_validity.get("fallback_from_invalid_reason", "") or ""
            ),
            "final_goal_mode": goal_meta.get("goal_mode"),
            "final_goal_source": goal_meta.get("goal_source"),
            "final_anchor_mode": goal_meta.get("anchor_mode"),
            "final_goal_distance_m": goal_dist_m,
        }
        self._record_reroute_decision_event(
            {
                "timestamp": command_ts,
                "wall_time_sec": timing.get("wall_time_sec"),
                "sim_time_sec": timing.get("sim_time_sec"),
                "world_frame": timing.get("world_frame"),
                "routing_phase": phase,
                "routing_request_index": routing_request_index,
                "reroute_reason": reroute_reason,
                "trigger_source": reroute_trigger_source,
                "trigger_condition_snapshot": {
                    "route_ready": route_ready,
                    "route_cooldown_ok": route_cooldown_ok,
                    "route_attempts_left": route_attempts_left,
                    "route_phase_sent": route_phase_sent,
                    "lane_phase_sent": lane_phase_sent,
                    "ignore_roll_active": ignore_roll_active,
                    "routing_freeze_active": self._routing_freeze_active,
                    "invalid_goal": bool(goal_validity.get("invalid_goal")),
                    "invalid_goal_reason": goal_validity.get("invalid_goal_reason"),
                    "skip_invalid_long_route": self.auto_routing_skip_invalid_long_route,
                    "suppress_long_phase_reroute_on_unstable_reference_line": self.auto_routing_suppress_long_phase_reroute_on_unstable_reference_line,
                    "defer_long_goal_until_route_debug_ready": self.auto_routing_defer_long_goal_until_route_debug_ready,
                    "recent_route_debug_fresh": recent_route_debug_fresh,
                    "recent_route_debug_age_sec": recent_route_debug_age_sec,
                    "recent_route_debug_timestamp": recent_route_debug_ts,
                    "recent_route_debug_source": self._proto_scalar(
                        recent_route_debug.get("route_debug_source")
                    ),
                    "recent_route_debug_signature": self._proto_scalar(
                        recent_route_debug.get("routing_lane_window_signature")
                    ),
                    "recent_route_debug_provider_status": self._proto_scalar(
                        recent_route_debug.get("reference_line_provider_status")
                    ),
                    "recent_route_debug_create_route_segments_status": self._proto_scalar(
                        recent_route_debug.get("create_route_segments_status")
                    ),
                    "reference_line_count": recent_reference_line_count,
                    "route_segment_count": recent_route_segment_count,
                    "route_debug_ready_for_long_phase": route_debug_ready_for_long_phase,
                    "route_debug_ready_reason": route_debug_ready_reason,
                    "visible_route_debug_count_since_last_routing_send": int(
                        route_debug_since_last_routing_send.get("visible_route_debug_count", 0) or 0
                    ),
                    "visible_route_debug_count_since_first_routing_response": int(
                        route_debug_since_first_routing_response.get("visible_route_debug_count", 0)
                        or 0
                    ),
                    "last_visible_route_debug_after_last_routing_send_sec": _finite_or_none(
                        route_debug_since_last_routing_send.get(
                            "last_visible_route_debug_after_boundary_sec"
                        )
                    ),
                    "last_visible_route_debug_after_first_routing_response_sec": _finite_or_none(
                        route_debug_since_first_routing_response.get(
                            "last_visible_route_debug_after_boundary_sec"
                        )
                    ),
                    "startup_elapsed_sec": startup_elapsed_sec,
                    "defer_long_goal_max_wait_sec": self.auto_routing_defer_long_goal_max_wait_sec,
                    "long_goal_defer_escape_active": long_goal_defer_escape_active,
                    "lane_follow_map_status": recent_lane_follow_map_status,
                    "lane_follow_map_inconsistent": lane_follow_map_inconsistent_active,
                    "map_contract_invalid": bool(self.map_contract_invalid),
                },
                "current_goal": {
                    "x": x1,
                    "y": y1,
                    "z": z1,
                    "requested_goal_mode": goal_meta.get("requested_goal_mode"),
                    "goal_mode": goal_meta.get("goal_mode"),
                    "goal_source": goal_meta.get("goal_source"),
                    "anchor_mode": goal_meta.get("anchor_mode"),
                    "goal_distance_m": goal_dist_m,
                },
                "pre_fallback_goal": {
                    "x": pre_fallback_goal_x1,
                    "y": pre_fallback_goal_y1,
                    "z": pre_fallback_goal_z1,
                    "requested_goal_mode": pre_fallback_goal_meta.get("requested_goal_mode"),
                    "goal_mode": pre_fallback_goal_meta.get("goal_mode"),
                    "goal_source": pre_fallback_goal_meta.get("goal_source"),
                    "anchor_mode": pre_fallback_goal_meta.get("anchor_mode"),
                    "goal_distance_m": pre_fallback_goal_distance_m,
                },
                "pre_fallback_goal_projection": pre_fallback_goal_projection_summary,
                "pre_fallback_goal_validity_snapshot": pre_fallback_goal_validity_summary,
                "goal_resolution": goal_resolution_summary,
                "seed_pose": seed_pose_summary,
                "planning_state": planning_state_summary,
                "start_projection": start_projection_summary,
                "goal_projection": goal_projection_summary,
                "goal_validity_snapshot": goal_validity_summary,
                "recent_route_debug_snapshot": recent_route_debug_snapshot,
                "current_lane_id": recent_route_debug.get("current_lane_id"),
                "reference_line_count": recent_route_debug.get("reference_line_count"),
                "route_segment_count": recent_route_debug.get("route_segment_count"),
                "lane_follow_map_status": recent_lane_follow_map_status or None,
                "lane_follow_map_inconsistent": lane_follow_map_inconsistent_active,
                "map_contract_invalid": bool(self.map_contract_invalid),
                "routing_request_sent": send_routing_now,
                "routing_waiting_for_route_debug_ready": routing_waiting_for_route_debug_ready,
                "routing_skipped_due_to_freeze": routing_skipped_due_to_freeze,
                "routing_skipped_due_to_invalid_goal": routing_skipped_due_to_invalid_goal,
                "routing_skipped_due_to_unstable_reference_line": routing_skipped_due_to_unstable_reference_line,
            }
        )
        startup_event = {
            "ts_sec": command_ts,
            "wall_time_sec": timing.get("wall_time_sec"),
            "sim_time_sec": timing.get("sim_time_sec"),
            "world_frame": timing.get("world_frame"),
            "tick_owner": timing.get("tick_owner"),
            "bridge_is_tick_owner": timing.get("bridge_is_tick_owner"),
            "raw_carla_x": float(pose_info["raw_x"]),
            "raw_carla_y": float(pose_info["raw_y"]),
            "raw_carla_yaw_deg": float(pose_info.get("raw_yaw_deg", math.degrees(pose_info["raw_yaw"]))),
            "map_x_before_back_offset": float(pose_info["map_x_before_back_offset"]),
            "map_y_before_back_offset": float(pose_info["map_y_before_back_offset"]),
            "map_yaw_deg": float(pose_info.get("map_yaw_deg", math.degrees(yaw0))),
            "map_x_after_back_offset": float(pose_info["map_x"]),
            "map_y_after_back_offset": float(pose_info["map_y"]),
            "localization_back_offset_m": float(self.localization_back_offset_m),
            "localization_reference_mode": self.localization_reference_mode,
            "apollo_control_state_reference": self.apollo_control_state_reference,
            "routing_start_raw_x": raw_x0,
            "routing_start_raw_y": raw_y0,
            "snap_applied": bool(snap_applied),
            "snap_candidate_available": snap_candidate_available,
            "snap_source_file": str(start_snap_candidate.get("map_file", self.map_file_path or "")),
            "snap_source_type": snap_source_type,
            "snapped_x": snapped_x,
            "snapped_y": snapped_y,
            "snapped_lane_heading_deg": (
                float(start_snap_candidate["lane_yaw_deg"]) if snap_candidate_available else None
            ),
            "snapped_distance_m": (
                float(start_snap_candidate["distance_m"]) if snap_candidate_available else None
            ),
            "heading_diff_to_vehicle_deg": heading_diff_to_vehicle_deg,
            "nudge_enabled": bool(self.auto_routing_start_nudge_m > 0.0),
            "nudge_base_m": float(self.auto_routing_start_nudge_m),
            "nudge_retry_step_m": float(self.auto_routing_start_nudge_retry_step_m),
            "nudge_effective_m": (
                float(start_nudge_effective) if start_nudge_applied else 0.0
            ),
            "nudge_heading_source": start_nudge_heading_source,
            "nudge_heading_deg": start_nudge_heading_deg,
            "nudged_x": nudged_x,
            "nudged_y": nudged_y,
            "routing_phase": phase,
            "routing_request_index": routing_request_index,
            "routing_request_kind": request_kind,
            "reroute_reason": reroute_reason,
            "reroute_trigger_source": reroute_trigger_source,
            "freeze_after_success_config": bool(self.auto_routing_freeze_after_success),
            "freeze_after_success_effective": bool(self._routing_freeze_active),
            "routing_skipped_due_to_freeze": routing_skipped_due_to_freeze,
            "routing_skipped_due_to_invalid_goal": routing_skipped_due_to_invalid_goal,
            "snap_rejected": bool(snap_rejected),
            "snap_reject_reason": snap_reject_reason,
            "suspicious_snap_rejected": bool(suspicious_snap_rejected),
            "final_start_x": x0,
            "final_start_y": y0,
            "final_goal_x": x1,
            "final_goal_y": y1,
            "routing_request_sent": bool(send_routing_now),
            "invalid_goal_reason": original_invalid_goal_reason,
            "goal_fallback_applied": goal_fallback_applied,
            "goal_projection_available": bool(goal_proj.get("available", False)),
            "goal_projection_distance_m": _finite_or_none(goal_proj.get("distance_m")),
            "dest_beyond_reference_line": goal_validity.get("dest_beyond_reference_line"),
            "reference_line_length": goal_validity.get("reference_line_length"),
            "remain_length_to_dest": goal_validity.get("remain_length_to_dest"),
            "localization_to_final_start_distance_m": localization_to_final_start_distance_m,
            "raw_start_to_snapped_start_distance_m": raw_to_snapped_start_distance_m,
            "snapped_start_to_nudged_start_distance_m": snapped_to_nudged_start_distance_m,
            "heading_diff_vehicle_vs_snap_lane_deg": heading_diff_to_vehicle_deg,
            "suspicious_snap": suspicious_snap,
        }
        if send_routing_now or routing_skipped_due_to_freeze or routing_skipped_due_to_invalid_goal:
            self._record_startup_geometry_event(startup_event)
            self._record_routing_event(
                {
                    "timestamp": command_ts,
                    "wall_time_sec": timing.get("wall_time_sec"),
                    "sim_time_sec": timing.get("sim_time_sec"),
                    "world_frame": timing.get("world_frame"),
                    "routing_phase": phase,
                    "routing_request_index": routing_request_index,
                    "routing_request_kind": request_kind,
                    "reroute_reason": reroute_reason,
                    "trigger_source": reroute_trigger_source,
                    "goal_mode": goal_meta.get("goal_mode"),
                    "goal_source": goal_meta.get("goal_source"),
                    "requested_goal_mode": goal_meta.get("requested_goal_mode"),
                    "anchor_mode": goal_meta.get("anchor_mode"),
                    "ego_x": x0,
                    "ego_y": y0,
                    "ego_yaw_deg": math.degrees(yaw0),
                    "ego_speed_mps": float(speed_mps or 0.0),
                    "start_raw_x": raw_x0,
                    "start_raw_y": raw_y0,
                    "goal_raw_x": x1,
                    "goal_raw_y": y1,
                    "goal_x": x1,
                    "goal_y": y1,
                    "goal_distance_m": goal_dist_m,
                    "seed_pose": seed_pose_summary,
                    "start_projection": start_projection_summary,
                    "goal_projection": goal_projection_summary,
                    "goal_validity_snapshot": goal_validity_summary,
                    "recent_route_debug_snapshot": recent_route_debug_snapshot,
                    "routing_request_sent": bool(send_routing_now),
                    "routing_skipped_due_to_freeze": bool(routing_skipped_due_to_freeze),
                    "routing_skipped_due_to_invalid_goal": bool(routing_skipped_due_to_invalid_goal),
                    "invalid_goal_reason": original_invalid_goal_reason,
                    "goal_fallback_applied": bool(goal_fallback_applied),
                }
            )
            print(
                "[bridge][routing][startup-geometry] "
                f"phase={phase} request_index={routing_request_index} "
                f"freeze={int(self.auto_routing_freeze_after_success)}/{int(self._routing_freeze_active)} "
                f"reason={reroute_reason} trigger={reroute_trigger_source} "
                f"raw->{self._format_xy(raw_x0, raw_y0)} snap={int(snap_applied)}/{int(snap_rejected)}[{snap_reject_reason or 'ok'}] "
                f"src={snap_source_type} snap_xy->{self._format_xy(snapped_x, snapped_y)} "
                f"nudge->{self._format_xy(nudged_x, nudged_y)} final->{self._format_xy(x0, y0)} "
                f"goal_dist={goal_dist_m:.2f}m "
                f"snap_dist={(raw_to_snapped_start_distance_m if math.isfinite(raw_to_snapped_start_distance_m) else float('nan')):.2f}m "
                f"heading_diff={(heading_diff_to_vehicle_deg if heading_diff_to_vehicle_deg is not None else float('nan')):.2f}deg "
                f"skip_due_to_freeze={routing_skipped_due_to_freeze} "
                f"skip_due_to_invalid_goal={routing_skipped_due_to_invalid_goal}"
            )

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
            self._routing_last_request_phase = str(phase)
            self.auto_routing_routing_sent += 1
            self.auto_routing_last_routing_ts = ts_sec
            self._routing_first_response_after_last_routing_send_boundary_ts_sec = float(ts_sec)
            self._routing_first_response_after_last_routing_send_ts_sec = None
            self._routing_first_success_response_after_last_routing_send_boundary_ts_sec = float(ts_sec)
            self._routing_first_success_response_after_last_routing_send_ts_sec = None
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
                    in_no_response_grace = (
                        self.auto_routing_lane_follow_no_response_grace_sec > 0.0
                        and (time.time() - self._bridge_start_wall_sec)
                        < self.auto_routing_lane_follow_no_response_grace_sec
                    )
                    if in_no_response_grace:
                        self.stats["last_error"] = "waiting_for_lane_follow_ready"
                    elif self.auto_routing_disable_lane_follow_on_no_response:
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
                in_no_response_grace = (
                    self.auto_routing_lane_follow_no_response_grace_sec > 0.0
                    and (time.time() - self._bridge_start_wall_sec)
                    < self.auto_routing_lane_follow_no_response_grace_sec
                )
                if in_no_response_grace:
                    self.stats["last_error"] = "waiting_for_lane_follow_ready"
                elif self.auto_routing_disable_lane_follow_on_no_response:
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
        self._write_startup_geometry_summary()
        self._write_planning_topic_debug_summary()
        if hasattr(self.node, "write_artifacts"):
            try:
                self.node.write_artifacts()
            except Exception:
                pass
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
                if hasattr(self.node, "tick"):
                    try:
                        self.node.tick()
                    except Exception:
                        pass
                snapshot = self.node.snapshot()
                self.stats["ros_input_counts"] = snapshot.get("rx_counts", self.stats["ros_input_counts"])
                odom = snapshot.get("odom")
                if (
                    self.transport_mode == "carla_direct"
                    and odom is not None
                    and not bool(snapshot.get("world_frame_advanced", False))
                ):
                    if int(self.stats.get("control_tx_count", 0) or 0) <= 0:
                        # Direct transport must not tick CARLA, but Apollo
                        # control still expects current localization/chassis
                        # messages while the harness is in setup/probe waits.
                        # Re-publish cached GT only until control first outputs;
                        # after that, repeated stale pose messages can make
                        # Apollo perceive poor vehicle response and brake early.
                        self.stats["direct_stale_world_frame_republish_count"] = int(
                            self.stats.get("direct_stale_world_frame_republish_count", 0) or 0
                        ) + 1
                    else:
                        self.stats["direct_stale_world_frame_skip_count"] = int(
                            self.stats.get("direct_stale_world_frame_skip_count", 0) or 0
                        ) + 1
                        now = time.time()
                        if (now - last_stats_flush) >= 1.0:
                            self._write_stats()
                            last_stats_flush = now
                        time.sleep(period)
                        continue
                if odom is not None:
                    direct_world_frame = snapshot.get("world_frame")
                    loc, ts_sec, vel_xyz, pose_debug, pose_info = self._odom_to_loc(
                        odom,
                        direct_world_frame=direct_world_frame,
                    )
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
                    front_actor = self.carla_feedback.find_vehicle_by_roles(self.front_obstacle_role_names)
                    front_actor_id = None
                    front_actor_role = ""
                    front_actor_x = None
                    front_actor_y = None
                    front_actor_yaw_deg = None
                    front_actor_speed_mps = None
                    if front_actor is not None:
                        try:
                            front_actor_id = int(getattr(front_actor, "id"))
                        except Exception:
                            front_actor_id = None
                        try:
                            attrs = getattr(front_actor, "attributes", {}) or {}
                            front_actor_role = str(attrs.get("role_name", "") or "")
                        except Exception:
                            front_actor_role = ""
                        try:
                            tr = front_actor.get_transform()
                            front_actor_x = float(getattr(tr.location, "x", 0.0))
                            front_actor_y = float(getattr(tr.location, "y", 0.0))
                            front_actor_yaw_deg = float(getattr(tr.rotation, "yaw", 0.0))
                        except Exception:
                            front_actor_x = None
                            front_actor_y = None
                            front_actor_yaw_deg = None
                        try:
                            vel = front_actor.get_velocity()
                            front_actor_speed_mps = math.sqrt(
                                float(getattr(vel, "x", 0.0)) ** 2
                                + float(getattr(vel, "y", 0.0)) ** 2
                                + float(getattr(vel, "z", 0.0)) ** 2
                            )
                        except Exception:
                            front_actor_speed_mps = None
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
                        "apollo_acceleration_mps2": self._coerce_float(
                            desired_in.get("acceleration_mps2"), float("nan"), "desired_in.acceleration_mps2", "publish_row"
                        ),
                        "apollo_debug_simple_lat_lateral_error_m": self._coerce_float(
                            desired_in.get("debug_simple_lat_lateral_error_m"),
                            float("nan"),
                            "desired_in.debug_simple_lat_lateral_error_m",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lat_heading_error_rad": self._coerce_float(
                            desired_in.get("debug_simple_lat_heading_error_rad"),
                            float("nan"),
                            "desired_in.debug_simple_lat_heading_error_rad",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lat_heading_error_rate_radps": self._coerce_float(
                            desired_in.get("debug_simple_lat_heading_error_rate_radps"),
                            float("nan"),
                            "desired_in.debug_simple_lat_heading_error_rate_radps",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lat_lateral_error_rate_mps": self._coerce_float(
                            desired_in.get("debug_simple_lat_lateral_error_rate_mps"),
                            float("nan"),
                            "desired_in.debug_simple_lat_lateral_error_rate_mps",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lat_curvature": self._coerce_float(
                            desired_in.get("debug_simple_lat_curvature"),
                            float("nan"),
                            "desired_in.debug_simple_lat_curvature",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lat_ref_heading_rad": self._coerce_float(
                            desired_in.get("debug_simple_lat_ref_heading_rad"),
                            float("nan"),
                            "desired_in.debug_simple_lat_ref_heading_rad",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lat_heading_rad": self._coerce_float(
                            desired_in.get("debug_simple_lat_heading_rad"),
                            float("nan"),
                            "desired_in.debug_simple_lat_heading_rad",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lat_lateral_acceleration_mps2": self._coerce_float(
                            desired_in.get("debug_simple_lat_lateral_acceleration_mps2"),
                            float("nan"),
                            "desired_in.debug_simple_lat_lateral_acceleration_mps2",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lat_lateral_jerk_mps3": self._coerce_float(
                            desired_in.get("debug_simple_lat_lateral_jerk_mps3"),
                            float("nan"),
                            "desired_in.debug_simple_lat_lateral_jerk_mps3",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lat_target_point_s": self._coerce_float(
                            desired_in.get("debug_simple_lat_target_point_s"),
                            float("nan"),
                            "desired_in.debug_simple_lat_target_point_s",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lat_target_point_relative_time_sec": self._coerce_float(
                            desired_in.get("debug_simple_lat_target_point_relative_time_sec"),
                            float("nan"),
                            "desired_in.debug_simple_lat_target_point_relative_time_sec",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lat_target_point_theta_rad": self._coerce_float(
                            desired_in.get("debug_simple_lat_target_point_theta_rad"),
                            float("nan"),
                            "desired_in.debug_simple_lat_target_point_theta_rad",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lat_target_point_kappa": self._coerce_float(
                            desired_in.get("debug_simple_lat_target_point_kappa"),
                            float("nan"),
                            "desired_in.debug_simple_lat_target_point_kappa",
                            "publish_row",
                        ),
                        "apollo_mode": desired_in.get("mode", ""),
                        "apollo_gear": desired_in.get("gear", ""),
                        "apollo_estop": desired_in.get("estop", False),
                        "actuator_mapping_mode": desired_out.get("actuator_mapping_mode", self.actuator_mapping_mode),
                        "commanded_throttle": self._coerce_float(desired_out.get("throttle"), float("nan"), "desired_out.throttle", "publish_row"),
                        "throttle_after_boost": self._coerce_float(desired_out.get("throttle_after_boost"), float("nan"), "desired_out.throttle_after_boost", "publish_row"),
                        "commanded_brake": self._coerce_float(desired_out.get("brake"), float("nan"), "desired_out.brake", "publish_row"),
                        "brake_after_deadzone": self._coerce_float(desired_out.get("brake_after_deadzone"), float("nan"), "desired_out.brake_after_deadzone", "publish_row"),
                        "commanded_steer": self._coerce_float(desired_out.get("steer"), float("nan"), "desired_out.steer", "publish_row"),
                        "commanded_steer_pre_lateral_guards": self._coerce_float(
                            desired_out.get("steer_before_lateral_guards"),
                            float("nan"),
                            "desired_out.steer_before_lateral_guards",
                            "publish_row",
                        ),
                        "commanded_steer_pre_clamp": self._coerce_float(desired_out.get("steer_pre_clamp"), float("nan"), "desired_out.steer_pre_clamp", "publish_row"),
                        "commanded_steer_clamped": desired_out.get("steer_clamped", False),
                        "throttle_mapping_source": desired_out.get("throttle_mapping_source", ""),
                        "brake_mapping_source": desired_out.get("brake_mapping_source", ""),
                        "steer_mapping_source": desired_out.get("steer_mapping_source", ""),
                        "physical_fallback_reason": desired_out.get("physical_fallback_reason", ""),
                        "target_front_wheel_angle_deg": self._coerce_float(
                            desired_out.get("target_front_wheel_angle_deg"), float("nan"), "desired_out.target_front_wheel_angle_deg", "publish_row"
                        ),
                        "mapped_carla_steer_cmd": self._coerce_float(
                            desired_out.get("mapped_carla_steer_cmd"), float("nan"), "desired_out.mapped_carla_steer_cmd", "publish_row"
                        ),
                        "target_accel_mps2": self._coerce_float(
                            desired_out.get("target_accel_mps2"), float("nan"), "desired_out.target_accel_mps2", "publish_row"
                        ),
                        "mapped_throttle_cmd": self._coerce_float(
                            desired_out.get("mapped_throttle_cmd"), float("nan"), "desired_out.mapped_throttle_cmd", "publish_row"
                        ),
                        "target_decel_mps2": self._coerce_float(
                            desired_out.get("target_decel_mps2"), float("nan"), "desired_out.target_decel_mps2", "publish_row"
                        ),
                        "mapped_brake_cmd": self._coerce_float(
                            desired_out.get("mapped_brake_cmd"), float("nan"), "desired_out.mapped_brake_cmd", "publish_row"
                        ),
                        "target_accel_source": desired_out.get("target_accel_source", ""),
                        "target_decel_source": desired_out.get("target_decel_source", ""),
                        "straight_lane_zero_steer_applied": desired_out.get("straight_lane_zero_steer_applied", False),
                        "low_speed_steer_guard_applied": desired_out.get("low_speed_steer_guard_applied", False),
                        "low_speed_sustained_guard_applied": desired_out.get("low_speed_sustained_guard_applied", False),
                        "low_speed_sustained_guard_active": desired_out.get("low_speed_sustained_guard_active", False),
                        "low_speed_sustained_guard_triggered": desired_out.get("low_speed_sustained_guard_triggered", False),
                        "low_speed_sustained_guard_trigger_reason": desired_out.get("low_speed_sustained_guard_trigger_reason", ""),
                        "low_speed_sustained_guard_limited_amount": self._coerce_float(
                            desired_out.get("low_speed_sustained_guard_limited_amount"),
                            float("nan"),
                            "desired_out.low_speed_sustained_guard_limited_amount",
                            "publish_row",
                        ),
                        "low_speed_sustained_guard_saturation_streak": desired_out.get(
                            "low_speed_sustained_guard_saturation_streak", 0
                        ),
                        "sustained_lateral_guard_applied": desired_out.get("sustained_lateral_guard_applied", False),
                        "trajectory_contract_lateral_guard_applied": desired_out.get(
                            "trajectory_contract_lateral_guard_applied", False
                        ),
                        "trajectory_contract_lateral_guard_limited_amount": self._coerce_float(
                            desired_out.get("trajectory_contract_lateral_guard_limited_amount"),
                            float("nan"),
                            "desired_out.trajectory_contract_lateral_guard_limited_amount",
                            "publish_row",
                        ),
                        "planning_lateral_contract_valid": desired_out.get(
                            "planning_lateral_contract_valid", False
                        ),
                        "planning_lateral_contract_reason": desired_out.get(
                            "planning_lateral_contract_reason", ""
                        ),
                        "planning_lateral_latest_sequence_num": desired_out.get(
                            "planning_lateral_latest_sequence_num"
                        ),
                        "planning_lateral_latest_point_count": desired_out.get(
                            "planning_lateral_latest_point_count"
                        ),
                        "planning_lateral_latest_age_ms": self._coerce_float(
                            desired_out.get("planning_lateral_latest_age_ms"),
                            float("nan"),
                            "desired_out.planning_lateral_latest_age_ms",
                            "publish_row",
                        ),
                        "sustained_lateral_guard_active": desired_out.get("sustained_lateral_guard_active", False),
                        "sustained_lateral_guard_triggered": desired_out.get("sustained_lateral_guard_triggered", False),
                        "sustained_lateral_guard_trigger_reason": desired_out.get("sustained_lateral_guard_trigger_reason", ""),
                        "sustained_lateral_guard_limited_amount": self._coerce_float(
                            desired_out.get("sustained_lateral_guard_limited_amount"),
                            float("nan"),
                            "desired_out.sustained_lateral_guard_limited_amount",
                            "publish_row",
                        ),
                        "sustained_lateral_guard_saturation_streak": desired_out.get(
                            "sustained_lateral_guard_saturation_streak", 0
                        ),
                        "force_zero_steer_applied": desired_out.get("force_zero_steer_applied", False),
                        "traffic_light_policy": self.traffic_light_policy,
                        "measured_throttle": self._coerce_float(measured.get("throttle"), float("nan"), "measured.throttle", "publish_row"),
                        "measured_brake": self._coerce_float(measured.get("brake"), float("nan"), "measured.brake", "publish_row"),
                        "measured_steer": self._coerce_float(measured.get("steer"), float("nan"), "measured.steer", "publish_row"),
                        "measured_steer_pct": self._coerce_float(measured.get("steer_feedback_pct"), float("nan"), "measured.steer_feedback_pct", "publish_row"),
                        "measured_steer_deg": self._coerce_float(measured.get("steer_feedback_deg"), float("nan"), "measured.steer_feedback_deg", "publish_row"),
                        "measured_forward_accel_mps2": self._coerce_float(measured.get("forward_accel_mps2"), float("nan"), "measured.forward_accel_mps2", "publish_row"),
                        "measured_forward_accel_raw_mps2": self._coerce_float(measured.get("raw_forward_accel_mps2"), float("nan"), "measured.raw_forward_accel_mps2", "publish_row"),
                        "measured_forward_accel_dvdt_mps2": self._coerce_float(measured.get("dvdt_forward_accel_mps2"), float("nan"), "measured.dvdt_forward_accel_mps2", "publish_row"),
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
                    self._record_obstacle_contract_event(
                        {
                            "timestamp": ts_sec,
                            **self._timing_snapshot(event_wall_time_sec=ts_sec),
                            "front_obstacle_visible": front_status.get("visible"),
                            "front_obstacle_mode": front_status.get("mode"),
                            "front_obstacle_role_names": list(front_status.get("role_names", []) or []),
                            "front_obstacle_actor_found": front_actor is not None,
                            "front_obstacle_actor_id": front_actor_id,
                            "front_obstacle_actor_role": front_actor_role,
                            "front_obstacle_actor_x": front_actor_x,
                            "front_obstacle_actor_y": front_actor_y,
                            "front_obstacle_actor_yaw_deg": front_actor_yaw_deg,
                            "front_obstacle_actor_speed_mps": front_actor_speed_mps,
                            "front_obstacle_gap_lon_m": _finite_or_none(front_gap.get("lon_m")),
                            "front_obstacle_gap_lat_m": _finite_or_none(front_gap.get("lat_m")),
                            "front_obstacle_gap_distance_m": _finite_or_none(front_gap.get("distance_m")),
                            "nearest_obstacle_dist_m": nearest_obs,
                            "obstacle_cache_enabled": bool(front_status.get("cache_enabled")),
                            "obstacle_cache_hit_count": int(front_status.get("cache_hit_count", 0) or 0),
                            "published_obstacle_count": int(obs_count),
                        }
                    )
                    self._write_debug_row(row)
                    self._write_csv_row(
                        self.vehicle_response_csv_path,
                        {
                            "ts_sec": row["ts_sec"],
                            "speed_mps": row["speed_mps"],
                            "forward_accel_mps2": row["measured_forward_accel_mps2"],
                            "yaw_rate_rps": self._coerce_float(
                                measured.get("yaw_rate_rps"),
                                float("nan"),
                                "measured.yaw_rate_rps",
                                "vehicle_response",
                            ),
                            "measured_steer_deg": row["measured_steer_deg"],
                            "curvature": self._coerce_float(
                                measured.get("curvature"),
                                float("nan"),
                                "measured.curvature",
                                "vehicle_response",
                            ),
                            "lateral_accel_mps2": self._coerce_float(
                                measured.get("lateral_accel_mps2"),
                                float("nan"),
                                "measured.lateral_accel_mps2",
                                "vehicle_response",
                            ),
                            "pose_x": self._coerce_float(
                                measured.get("pose_x"),
                                float("nan"),
                                "measured.pose_x",
                                "vehicle_response",
                            ),
                            "pose_y": self._coerce_float(
                                measured.get("pose_y"),
                                float("nan"),
                                "measured.pose_y",
                                "vehicle_response",
                            ),
                            "pose_z": self._coerce_float(
                                measured.get("pose_z"),
                                float("nan"),
                                "measured.pose_z",
                                "vehicle_response",
                            ),
                            "pose_yaw_deg": self._coerce_float(
                                measured.get("pose_yaw_deg"),
                                float("nan"),
                                "measured.pose_yaw_deg",
                                "vehicle_response",
                            ),
                            "gear": self._coerce_float(
                                measured.get("gear"),
                                float("nan"),
                                "measured.gear",
                                "vehicle_response",
                            ),
                            "reverse": measured.get("reverse", False),
                            "hand_brake": measured.get("hand_brake", False),
                            "measured_throttle": row["measured_throttle"],
                            "measured_brake": row["measured_brake"],
                            "measured_steer": row["measured_steer"],
                            "commanded_throttle": row["commanded_throttle"],
                            "commanded_brake": row["commanded_brake"],
                            "commanded_steer": row["commanded_steer"],
                            "mapping_mode": row["actuator_mapping_mode"],
                            "measured_source": row["measured_source"],
                        },
                    )
                    self._write_csv_row(
                        self.geometry_debug_csv_path,
                        {
                            "ts_sec": row["ts_sec"],
                            "map_x": row["map_x"],
                            "map_y": row["map_y"],
                            "map_yaw_deg": row["map_yaw_deg"],
                            "lane_dist_m": row["lane_dist_m"],
                            "e_y_m": row["e_y_m"],
                            "e_psi_deg": row["e_psi_deg"],
                            "preview_x": row["preview_x"],
                            "preview_y": row["preview_y"],
                            "preview_heading_deg": row["preview_heading_deg"],
                            "reference_lane_curvature": row["target_curvature"],
                            "routing_established": row["routing_established"],
                            "routing_last_road_count": row["routing_last_road_count"],
                            "planning_has_trajectory": self._planning_last_points > 0,
                            "planning_last_points": self._planning_last_points,
                            "front_obstacle_visible": row["front_obstacle_visible"],
                            "front_obstacle_gap_lon_m": row["front_obstacle_gap_lon_m"],
                            "front_obstacle_gap_lat_m": row["front_obstacle_gap_lat_m"],
                            "front_obstacle_gap_distance_m": row["front_obstacle_gap_distance_m"],
                        },
                    )
                    self._update_steer_sign_auto_check(row)
                    self._maybe_dump_saturation_snapshot(row)
                    self._maybe_dump_control_anomaly(row, snapshot, measured)
                    now_wall = time.time()
                    if now_wall - self._last_control_print_sec >= 1.0:
                        self._last_control_print_sec = now_wall
                        print(
                            "[bridge][control] mode=%s desired(thr=%.3f brk=%.3f str=%.3f) measured(thr=%s brk=%s str=%s src=%s)"
                            % (
                                desired_out.get("actuator_mapping_mode", self.actuator_mapping_mode),
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
            if rclpy is not None:
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
            "localization_back_offset_m": 0.0,
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
                "start_nudge_m": 0.0,
                "start_nudge_retry_step_m": 0.0,
                "start_nudge_min_safe_m": 0.0,
                "start_nudge_max_m": 0.0,
                "resend_sec": 5.0,
                "max_attempts": 5,
                "target_speed_mps": 8.0,
                "startup_delay_sec": 3.0,
                "startup_apollo_warmup_sec": 0.0,
                "lane_follow_no_response_grace_sec": 0.0,
                "use_long_goal_after_move": True,
                "snap_start_to_lane": False,
                "snap_goal_to_lane": False,
                "start_nudge_use_lane_heading": False,
                "snap_source_mode": "lane_centerline_only",
                "snap_allow_untrusted_source": False,
                "snap_heading_diff_max_deg": 30.0,
                "snap_heading_diff_hard_reject_deg": 45.0,
                "lane_heading_nudge_max_heading_diff_deg": 20.0,
                "disable_nudge_when_snap_rejected": True,
                "goal_validity_check_enabled": True,
                "goal_validity_fallback_enabled": True,
                "goal_validity_reference_line_stale_sec": 1.0,
                "suppress_long_phase_reroute_on_unstable_reference_line": False,
                "scenario_goal_path": "scenario_goal.json",
                "send_action": False,
                "send_lane_follow": False,
                "auto_enable_lane_follow_fallback": True,
                "disable_lane_follow_on_no_response": True,
                "send_routing_request": True,
                "freeze_after_success": True,
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
                "actuator_mapping_mode": "legacy",
                "physical": {
                    "calibration_file": "",
                    "allow_legacy_fallback": True,
                    "apollo_max_steer_angle_deg": 8.2030,
                    "apollo_max_accel_mps2": 4.0,
                    "apollo_max_decel_mps2": 6.0,
                    "use_top_level_acceleration": True,
                    "use_lon_debug": True,
                },
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
                "trajectory_contract_lateral_guard_enabled": False,
                "trajectory_contract_lateral_guard_max_abs_steer": 0.05,
                "trajectory_contract_lateral_guard_max_speed_mps": 3.0,
                "trajectory_contract_lateral_guard_raw_threshold": 0.95,
                "trajectory_contract_lateral_guard_max_planning_age_ms": 300.0,
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
