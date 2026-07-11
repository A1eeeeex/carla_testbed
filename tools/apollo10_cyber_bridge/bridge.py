#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import importlib
import importlib.util
import json
import math
import os
import queue
import re
import signal
import sys
import threading
import time
import types
from collections import Counter, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import yaml

from carla_testbed.adapters.apollo.messages import (
    build_localization_estimate_dict_from_map_state,
    fill_header as fill_apollo_header,
    localization_debug_from_dict,
    write_localization_estimate_to_pb,
)
from carla_testbed.adapters.apollo.vehicle_reference import (
    VehicleReferenceConfig,
    load_vehicle_reference,
    resolve_localization_back_offset_m as resolve_vehicle_reference_localization_back_offset_m,
)
from carla_testbed.analysis.apollo_reference_line_contract import build_reference_line_contract_event
from carla_testbed.sim.vehicle_geometry import wheelbase_from_world_positions
from tools.apollo10_cyber_bridge.control_apply_trace import (
    ControlCriticalWindowRecorder,
    build_control_apply_trace_payload,
)

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
        apply_throttle_brake_mutual_exclusion as apply_throttle_brake_mutual_exclusion_impl,
        derive_longitudinal_targets as derive_longitudinal_targets_impl,
        legacy_map_base_controls as legacy_map_base_controls_impl,
        normalize_steering_command as normalize_steering_command_impl,
        physical_map_base_controls as physical_map_base_controls_impl,
        select_steering_field as select_steering_field_impl,
        STEERING_NORMALIZATION_ANGLE_DEGREE_AT_SELECT,
        STEERING_NORMALIZATION_SINGLE_PERCENT_AT_SELECT,
        STEERING_PERCENT_NORMALIZATION_MODES,
        steering_normalization_mode as steering_normalization_mode_impl,
    )
except Exception:
    from tools.apollo10_cyber_bridge.control_mapping import (  # type: ignore
        ControlMappingConfig,
        apply_throttle_brake_mutual_exclusion as apply_throttle_brake_mutual_exclusion_impl,
        derive_longitudinal_targets as derive_longitudinal_targets_impl,
        legacy_map_base_controls as legacy_map_base_controls_impl,
        normalize_steering_command as normalize_steering_command_impl,
        physical_map_base_controls as physical_map_base_controls_impl,
        select_steering_field as select_steering_field_impl,
        STEERING_NORMALIZATION_ANGLE_DEGREE_AT_SELECT,
        STEERING_NORMALIZATION_SINGLE_PERCENT_AT_SELECT,
        STEERING_PERCENT_NORMALIZATION_MODES,
        steering_normalization_mode as steering_normalization_mode_impl,
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
    from planning_debug import (
        build_planning_debug_presence as build_planning_debug_presence_impl,
        build_trajectory_shape_debug as build_trajectory_shape_debug_impl,
    )
except Exception:
    from tools.apollo10_cyber_bridge.planning_debug import (  # type: ignore
        build_planning_debug_presence as build_planning_debug_presence_impl,
        build_trajectory_shape_debug as build_trajectory_shape_debug_impl,
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
    from carla_direct_transport import (
        CarlaDirectTransport,
        NoopExecutor,
        should_republish_stale_world_frame,
    )
except Exception:
    from tools.apollo10_cyber_bridge.carla_direct_transport import (  # type: ignore
        CarlaDirectTransport,
        NoopExecutor,
        should_republish_stale_world_frame,
    )


REAR_AXLE_LOCALIZATION_BACK_OFFSET_M = 1.4235
REAR_AXLE_LOCALIZATION_BACK_OFFSET_TOLERANCE_M = 0.05
APOLLO_CONTROL_STATE_REFERENCE = "rear_axle_input_com_internal"
DEFAULT_APOLLO_MAP_ROOT_CANDIDATES = (
    "/apollo/modules/map/data",
    "/home/ubuntu/Apollo10.0/application-core/data/map_data",
    "/home/ubuntu/application-core/data/map_data",
    "/home/ubuntu/apollo/modules/map/data",
)


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


def _rfu_to_enu_quat_from_heading(heading: float) -> Tuple[float, float, float, float]:
    # Apollo Pose.orientation rotates vehicle RFU (Right/Forward/Up) to ENU.
    # This is equivalent to an ordinary local-X yaw quaternion at heading - pi/2.
    return _yaw_to_quat(float(heading) - math.pi / 2.0)


def _rotate_vector_by_quat(
    qx: float,
    qy: float,
    qz: float,
    qw: float,
    vx: float,
    vy: float,
    vz: float,
) -> Tuple[float, float, float]:
    # q * v * q^-1 for unit quaternions.
    tx = 2.0 * (qy * vz - qz * vy)
    ty = 2.0 * (qz * vx - qx * vz)
    tz = 2.0 * (qx * vy - qy * vx)
    return (
        vx + qw * tx + (qy * tz - qz * ty),
        vy + qw * ty + (qz * tx - qx * tz),
        vz + qw * tz + (qx * ty - qy * tx),
    )


def _decode_rfu_to_enu_heading_from_quat(
    qx: float,
    qy: float,
    qz: float,
    qw: float,
) -> float:
    # Apollo RFU forward axis is local +Y.
    fx, fy, _ = _rotate_vector_by_quat(qx, qy, qz, qw, 0.0, 1.0, 0.0)
    return math.atan2(fy, fx)


def _odom_angular_velocity_rad_per_s(odom: Odometry) -> Tuple[float, float, float]:
    # ROS Odometry twist.angular is already rad/s. CARLA deg/s conversion must
    # happen before odom construction.
    ang = odom.twist.twist.angular
    return float(ang.x), float(ang.y), float(ang.z)


def _localization_acceleration_from_velocity(
    previous_sample: Optional[Tuple[float, float, float, float]],
    timestamp_sec: float,
    vx: float,
    vy: float,
    vz: float,
    *,
    previous_acceleration: Optional[Tuple[float, float, float]] = None,
    smoothing_alpha: float = 1.0,
    max_abs_mps2: Optional[float] = None,
    max_delta_mps2: Optional[float] = None,
) -> Tuple[float, float, float, str]:
    if previous_sample is None:
        return 0.0, 0.0, 0.0, "initial_sample_missing_previous_velocity"
    prev_timestamp, prev_vx, prev_vy, prev_vz = previous_sample
    dt = float(timestamp_sec) - float(prev_timestamp)
    if not math.isfinite(dt) or dt <= 1e-9:
        return 0.0, 0.0, 0.0, "stale_timestamp_republish"
    raw = (
        (float(vx) - float(prev_vx)) / dt,
        (float(vy) - float(prev_vy)) / dt,
        (float(vz) - float(prev_vz)) / dt,
    )
    filtered = _filter_localization_acceleration(
        raw,
        previous_acceleration=previous_acceleration,
        smoothing_alpha=smoothing_alpha,
        max_abs_mps2=max_abs_mps2,
        max_delta_mps2=max_delta_mps2,
    )
    source = "finite_difference_filtered" if filtered != raw else "finite_difference"
    return filtered[0], filtered[1], filtered[2], source


def _filter_localization_acceleration(
    raw_acceleration: Tuple[float, float, float],
    *,
    previous_acceleration: Optional[Tuple[float, float, float]] = None,
    smoothing_alpha: float = 1.0,
    max_abs_mps2: Optional[float] = None,
    max_delta_mps2: Optional[float] = None,
) -> Tuple[float, float, float]:
    alpha = _clamp(float(smoothing_alpha), 0.0, 1.0)
    out: List[float] = []
    changed = False
    for index, raw_value in enumerate(raw_acceleration):
        value = float(raw_value)
        prev_value = None
        if previous_acceleration is not None and len(previous_acceleration) > index:
            prev_value = float(previous_acceleration[index])
        if prev_value is not None and alpha < 1.0:
            value = alpha * value + (1.0 - alpha) * prev_value
        if prev_value is not None and max_delta_mps2 is not None and max_delta_mps2 > 0.0:
            value = _clamp(
                value,
                prev_value - float(max_delta_mps2),
                prev_value + float(max_delta_mps2),
            )
        if max_abs_mps2 is not None and max_abs_mps2 > 0.0:
            value = _clamp(value, -float(max_abs_mps2), float(max_abs_mps2))
        if abs(value - float(raw_value)) > 1e-9:
            changed = True
        out.append(value)
    if not changed:
        return raw_acceleration
    return out[0], out[1], out[2]


def _limit_forward_acceleration_for_nonnegative_speed_prediction(
    speed_mps: float,
    forward_acceleration_mps2: float,
    prediction_horizon_s: float,
) -> Tuple[float, bool, Optional[float]]:
    """Keep the GT kinematic state feasible for Apollo's replan model."""
    speed = float(speed_mps)
    acceleration = float(forward_acceleration_mps2)
    horizon = float(prediction_horizon_s)
    if (
        not math.isfinite(speed)
        or not math.isfinite(acceleration)
        or not math.isfinite(horizon)
        or horizon <= 0.0
    ):
        return acceleration, False, None
    minimum_acceleration = -max(speed, 0.0) / horizon
    if acceleration >= minimum_acceleration:
        return acceleration, False, minimum_acceleration
    return minimum_acceleration, True, minimum_acceleration


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


def _resolve_localization_back_offset_m(
    raw_value: Any,
    *,
    vehicle_reference: Optional[VehicleReferenceConfig],
) -> Tuple[float, str, str]:
    return resolve_vehicle_reference_localization_back_offset_m(
        raw_value,
        vehicle_reference=vehicle_reference,
    )


def _bridge_path_text(
    raw: Any,
    *,
    apollo_map_root_candidates: Sequence[str | Path] = DEFAULT_APOLLO_MAP_ROOT_CANDIDATES,
) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    text = _replace_apollo_map_root_placeholder(text, apollo_map_root_candidates)
    return os.path.expanduser(os.path.expandvars(text))


def _bridge_path(
    raw: Any,
    *,
    apollo_map_root_candidates: Sequence[str | Path] = DEFAULT_APOLLO_MAP_ROOT_CANDIDATES,
) -> Path:
    return Path(_bridge_path_text(raw, apollo_map_root_candidates=apollo_map_root_candidates))


def _replace_apollo_map_root_placeholder(
    text: str,
    candidates: Sequence[str | Path],
) -> str:
    if "${APOLLO_MAP_ROOT}" not in text and "$APOLLO_MAP_ROOT" not in text:
        return text
    env_root = os.environ.get("APOLLO_MAP_ROOT")
    ordered: list[str | Path] = []
    if env_root:
        ordered.append(env_root)
    ordered.extend(candidates)
    for candidate in ordered:
        root = Path(candidate).expanduser()
        if root.exists():
            replacement = str(root)
            return text.replace("${APOLLO_MAP_ROOT}", replacement).replace(
                "$APOLLO_MAP_ROOT",
                replacement,
            )
    return text


def _finite_or_none(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def _actor_type_label_from_type_id(type_id: Any) -> Optional[str]:
    text = str(type_id or "").strip().lower()
    if not text:
        return None
    if text.startswith("vehicle.") or ".vehicle." in text or "vehicle" in text:
        return "VEHICLE"
    if text.startswith("walker.") or "pedestrian" in text or "walker" in text:
        return "PEDESTRIAN"
    if "bicycle" in text or "bike" in text:
        return "BICYCLE"
    return "UNKNOWN"


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
    heading_offset_deg: float = 0.0

    @property
    def yaw_rad(self) -> float:
        return math.radians(self.yaw_deg)

    @property
    def heading_offset_rad(self) -> float:
        return math.radians(self.heading_offset_deg)

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
        return yaw + self.yaw_rad + self.heading_offset_rad


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
            wheel_positions: List[Tuple[float, float, float]] = []
            wheel_radius_candidates: List[float] = []
            max_steer_candidates: List[float] = []
            for wheel in wheels:
                pos = getattr(wheel, "position", None)
                if pos is not None:
                    wheel_positions.append(
                        (
                            float(getattr(pos, "x", 0.0)),
                            float(getattr(pos, "y", 0.0)),
                            float(getattr(pos, "z", 0.0)),
                        )
                    )
                radius = float(getattr(wheel, "radius", 0.0) or 0.0)
                if radius > 0.0:
                    wheel_radius_candidates.append(radius * (0.01 if radius > 5.0 else 1.0))
                max_steer = float(getattr(wheel, "max_steer_angle", 0.0) or 0.0)
                if abs(max_steer) > 1e-6:
                    max_steer_candidates.append(abs(max_steer))
            transform = actor.get_transform()
            actor_location = getattr(transform, "location", None)
            actor_forward = transform.get_forward_vector()
            wheel_base = wheelbase_from_world_positions(
                wheel_positions,
                actor_location=(
                    float(getattr(actor_location, "x", 0.0)),
                    float(getattr(actor_location, "y", 0.0)),
                    float(getattr(actor_location, "z", 0.0)),
                ),
                actor_forward=(
                    float(getattr(actor_forward, "x", 0.0)),
                    float(getattr(actor_forward, "y", 0.0)),
                    float(getattr(actor_forward, "z", 0.0)),
                ),
                position_scale=scale,
            )
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
                "wheel_position_frame": "carla_world",
                "wheelbase_method": "world_positions_projected_onto_actor_forward",
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
            "first_publish_ts_sec": 0.0,
            "last_publish_ts_sec": 0.0,
            "first_publish_wall_ts_sec": 0.0,
            "last_publish_wall_ts_sec": 0.0,
            "publish_elapsed_sim_sec": 0.0,
            "publish_elapsed_wall_sec": 0.0,
            "loc_count": 0,
            "chassis_count": 0,
            "obstacles_count": 0,
            "publish_errors": 0,
            "control_rx_count": 0,
            "control_tx_count": 0,
            "control_publish_exception_count": 0,
            "last_control_publish_error": "",
            "routing_request_count": 0,
            "routing_response_count": 0,
            "raw_routing_response_count": 0,
            "planning_routing_response_count": 0,
            "routing_response_relay_count": 0,
            "routing_response_relay_error_count": 0,
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
            "obstacle_message_count": 0,
            "obstacle_object_count": 0,
            "obstacle_empty_message_count": 0,
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
            "routing_phase_counts": {"startup": 0, "long": 0, "claim": 0},
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
            "direct_stale_world_frame_policy": "",
            "gt_stale_sample_policy": "",
            "gt_stale_sample_skip_count": 0,
            "gt_stale_sample_republish_count": 0,
            "gt_stale_sample_duplicate_count": 0,
            "localization_fresh_publish_count": 0,
            "localization_duplicate_timestamp_count": 0,
            "chassis_fresh_publish_count": 0,
            "chassis_duplicate_timestamp_count": 0,
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
            heading_offset_deg=float(tf_cfg.get("heading_offset_deg", 0.0)),
        )
        self.publish_rate_hz = float(bridge_cfg.get("publish_rate_hz", 20.0))
        self.obstacle_publish_rate_hz = float(
            bridge_cfg.get("obstacle_publish_rate_hz", self.publish_rate_hz)
        )
        self._last_obstacle_publish_sim_time: Optional[float] = None
        self.stats["obstacle_publish_rate_policy"] = {
            "configured_hz": self.obstacle_publish_rate_hz,
            "time_base": "simulation_time",
            "reason": "align_prediction_triggered_planning_with_planning_loop_rate",
        }
        self.localization_time_source = str(
            bridge_cfg.get("localization_time_source", "auto") or "auto"
        ).strip().lower()
        if self.localization_time_source not in {"auto", "sim_time", "cyber_time"}:
            self.localization_time_source = "auto"
        self.stats["localization_time_source_policy"] = {
            "configured": self.localization_time_source,
            "boundary": (
                "sim_time_is_claim_grade_gt_time_base; cyber_time_is_diagnostic_only"
            ),
        }
        run_cfg = (cfg.get("run", {}) or {})
        typed_runtime_cfg = (cfg.get("typed_runtime", {}) or {})
        reports_cfg = (cfg.get("reports", {}) or {})
        self.materialization_probe_enabled = bool(
            run_cfg.get("materialization_probe")
            or bridge_cfg.get("materialization_probe")
            or reports_cfg.get("require_route_materialization")
        )
        self.claim_profile_enabled = bool(
            run_cfg.get("claim_profile")
            or bridge_cfg.get("claim_profile")
            or self.materialization_probe_enabled
            or str(typed_runtime_cfg.get("runtime_dispatch_kind") or "")
            == "typed_apollo_claim_runtime"
        )
        claim_grade_cfg = bridge_cfg.get("claim_grade", {}) or {}
        self.claim_grade_enabled = bool(
            claim_grade_cfg.get("enabled", False) or self.claim_profile_enabled
        )
        self.gt_stale_sample_policy = str(
            claim_grade_cfg.get(
                "stale_world_frame_policy",
                bridge_cfg.get(
                    "stale_world_frame_policy",
                    "skip" if self.claim_grade_enabled else "republish_debug",
                ),
            )
            or ""
        ).strip().lower()
        if self.gt_stale_sample_policy not in {"skip", "republish_debug", "always_republish"}:
            self.gt_stale_sample_policy = "skip" if self.claim_grade_enabled else "republish_debug"
        self.localization_publish_policy = str(
            claim_grade_cfg.get(
                "localization_publish_policy",
                "once_per_new_sim_frame" if self.claim_grade_enabled else "allow_cached_republish",
            )
            or ""
        )
        self.chassis_publish_policy = str(
            claim_grade_cfg.get(
                "chassis_publish_policy",
                "once_per_new_sim_frame" if self.claim_grade_enabled else "allow_cached_republish",
            )
            or ""
        )
        self._last_gt_publish_sample_key: Optional[Tuple[Any, ...]] = None
        self.stats["gt_stale_sample_policy"] = self.gt_stale_sample_policy
        self.stats["claim_grade_publish_policy"] = {
            "enabled": self.claim_grade_enabled,
            "claim_profile_enabled": self.claim_profile_enabled,
            "materialization_probe_enabled": self.materialization_probe_enabled,
            "localization_publish_policy": self.localization_publish_policy,
            "chassis_publish_policy": self.chassis_publish_policy,
        }
        accel_filter_cfg = bridge_cfg.get("localization_acceleration_filter", {}) or {}
        self.localization_acceleration_filter_enabled = bool(accel_filter_cfg.get("enabled", False))
        self.localization_acceleration_filter_alpha = _clamp(
            _safe_float(accel_filter_cfg.get("alpha"), 1.0), 0.0, 1.0
        )
        self.localization_acceleration_filter_max_abs_mps2 = (
            _safe_float(accel_filter_cfg.get("max_abs_mps2"), 0.0) or None
        )
        self.localization_acceleration_filter_max_delta_mps2 = (
            _safe_float(accel_filter_cfg.get("max_delta_mps2"), 0.0) or None
        )
        self.localization_acceleration_nonnegative_speed_prediction_horizon_s = max(
            0.0,
            _safe_float(
                accel_filter_cfg.get("nonnegative_speed_prediction_horizon_s"),
                0.0,
            ),
        )
        self._localization_acceleration_nonnegative_speed_correction_count = 0
        self._last_localization_acceleration_sample: Optional[Tuple[float, float, float]] = None
        self.stats["localization_acceleration_filter"] = {
            "enabled": self.localization_acceleration_filter_enabled,
            "alpha": self.localization_acceleration_filter_alpha,
            "max_abs_mps2": self.localization_acceleration_filter_max_abs_mps2,
            "max_delta_mps2": self.localization_acceleration_filter_max_delta_mps2,
            "nonnegative_speed_prediction_horizon_s": (
                self.localization_acceleration_nonnegative_speed_prediction_horizon_s
            ),
            "nonnegative_speed_correction_count": 0,
            "source": "finite_difference_filter",
            "claim_boundary": "gt_input_quality_only_not_control_smoothing",
        }
        self.max_obstacles = int(bridge_cfg.get("max_obstacles", 64))
        self.radius_m = float(bridge_cfg.get("radius_m", 120.0))
        default_vehicle_reference_path = (
            Path(__file__).resolve().parents[2]
            / "configs"
            / "vehicles"
            / "ego_vehicle_reference.verified.yaml"
        )
        raw_vehicle_reference_path = (
            bridge_cfg.get("vehicle_reference_path", default_vehicle_reference_path)
            or default_vehicle_reference_path
        )
        vehicle_reference_path = Path(str(raw_vehicle_reference_path)).expanduser()
        if not vehicle_reference_path.is_absolute():
            vehicle_reference_path = (
                Path(__file__).resolve().parents[2] / vehicle_reference_path
            ).resolve()
        self.vehicle_reference_path = str(vehicle_reference_path)
        self.vehicle_reference_confidence = "unknown"
        self.vehicle_reference_hard_gate_eligible = False
        self.vehicle_reference_load_error = ""
        vehicle_reference: Optional[VehicleReferenceConfig] = None
        try:
            vehicle_reference = load_vehicle_reference(self.vehicle_reference_path)
            self.vehicle_reference_confidence = vehicle_reference.confidence
            self.vehicle_reference_hard_gate_eligible = bool(vehicle_reference.hard_gate_eligible)
        except Exception as exc:
            self.vehicle_reference_load_error = str(exc)
        (
            self.localization_back_offset_m,
            self.localization_back_offset_source,
            self.localization_back_offset_resolve_error,
        ) = _resolve_localization_back_offset_m(
            bridge_cfg.get("localization_back_offset_m", 0.0),
            vehicle_reference=vehicle_reference,
        )
        self.localization_reference_mode = _localization_reference_mode(
            self.localization_back_offset_m
        )
        self.apollo_control_state_reference = APOLLO_CONTROL_STATE_REFERENCE
        self.map_file_path_raw = str(bridge_cfg.get("map_file", "")).strip()
        self.map_file_path = _bridge_path_text(self.map_file_path_raw)
        self.map_bounds_file_raw = str(bridge_cfg.get("map_bounds_file", "")).strip()
        self.map_bounds_file = _bridge_path_text(self.map_bounds_file_raw)
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
            "back_offset_source": self.localization_back_offset_source,
            "back_offset_resolve_error": self.localization_back_offset_resolve_error,
            "reference_mode": self.localization_reference_mode,
            "expected_rear_axle_back_offset_m": REAR_AXLE_LOCALIZATION_BACK_OFFSET_M,
            "offset_error_m": float(
                self.localization_back_offset_m - REAR_AXLE_LOCALIZATION_BACK_OFFSET_M
            ),
            "apollo_control_state_reference": self.apollo_control_state_reference,
            "vehicle_reference_path": self.vehicle_reference_path,
            "vehicle_reference_confidence": self.vehicle_reference_confidence,
            "vehicle_reference_hard_gate_eligible": self.vehicle_reference_hard_gate_eligible,
            "vehicle_reference_load_error": self.vehicle_reference_load_error,
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
        self.apollo_reference_line_contract_path = (
            self.artifacts_dir / "apollo_reference_line_contract.jsonl"
        )
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
        self.control_publish_error_path = self.artifacts_dir / "control_publish_errors.jsonl"
        self.routing_event_debug_path = self.artifacts_dir / "routing_event_debug.jsonl"
        self.routing_response_decoded_path = self.artifacts_dir / "routing_response_decoded.json"
        self.routing_response_decoded_jsonl_path = self.artifacts_dir / "routing_response_decoded.jsonl"
        self.reroute_decision_debug_path = self.artifacts_dir / "reroute_decision_debug.jsonl"
        self.stage5_reroute_decision_debug_path = (
            self.artifacts_dir / "stage5_reroute_decision_debug.jsonl"
        )
        self.goal_validity_debug_path = self.artifacts_dir / "goal_validity_debug.jsonl"
        self.goal_validity_report_path = self.artifacts_dir / "goal_validity_report.json"
        self.topic_publish_stats_path = self.artifacts_dir / "topic_publish_stats.jsonl"
        self.publish_gap_trace_path = self.artifacts_dir / "publish_gap_trace.jsonl"
        self.control_apply_trace_path = self.artifacts_dir / "control_apply_trace.jsonl"
        self.control_critical_window_trace_path = (
            self.artifacts_dir / "control_critical_window_trace.jsonl"
        )
        self.obstacle_gt_contract_path = self.artifacts_dir / "obstacle_gt_contract.jsonl"
        self.obstacle_contract_debug_path = self.obstacle_gt_contract_path
        self._front_actor_dimension_cache: Dict[int, Dict[str, float]] = {}
        self.lateral_guard_debug_path = self.artifacts_dir / "lateral_guard_debug.jsonl"
        self.health_summary_path = self.artifacts_dir / "bridge_health_summary.json"
        self.bridge_transport_summary_path = self.artifacts_dir / "bridge_transport_summary.json"
        self.command_materialization_summary_path = (
            self.artifacts_dir / "command_materialization_summary.json"
        )
        self.carla_vehicle_path = self.artifacts_dir / "carla_vehicle_characteristics.json"
        self._debug_csv_header_written = False
        self._split_csv_headers_written: Dict[str, bool] = {}
        self._jsonl_artifact_handles: Dict[str, Any] = {}
        self._csv_artifact_states: Dict[str, Dict[str, Any]] = {}
        self._artifact_write_lock = threading.Lock()
        self.artifact_async_write_enabled = bool(bridge_cfg.get("artifact_async_write_enabled", True))
        artifact_async_queue_max = max(
            0,
            int(bridge_cfg.get("artifact_async_queue_max_rows", 0) or 0),
        )
        self.artifact_async_queue_soft_limit_rows = max(
            0,
            int(
                bridge_cfg.get(
                    "artifact_async_queue_soft_limit_rows",
                    artifact_async_queue_max if artifact_async_queue_max > 0 else 5000,
                )
                or 0
            ),
        )
        self._artifact_write_queue: Optional[queue.Queue[Any]] = (
            queue.Queue(maxsize=artifact_async_queue_max)
            if self.artifact_async_write_enabled
            else None
        )
        self._artifact_writer_thread: Optional[threading.Thread] = None
        self._artifact_writer_started = False
        self.artifact_flush_interval_s = max(
            0.0,
            float(bridge_cfg.get("artifact_flush_interval_s", 0.0) or 0.0),
        )
        self.artifact_flush_max_pending_rows = max(
            0,
            int(bridge_cfg.get("artifact_flush_max_pending_rows", 0) or 0),
        )
        self.artifact_stats_flush_interval_s = max(
            0.0,
            float(bridge_cfg.get("artifact_stats_flush_interval_s", 0.0) or 0.0),
        )
        self.stage5_debug_artifact_sample_stride = max(
            1,
            int(bridge_cfg.get("stage5_debug_artifact_sample_stride", 1) or 1),
        )
        self.reference_debug_artifact_sample_stride = max(
            1,
            int(bridge_cfg.get("reference_debug_artifact_sample_stride", 1) or 1),
        )
        self.control_debug_artifact_sample_stride = max(
            1,
            int(bridge_cfg.get("control_debug_artifact_sample_stride", 1) or 1),
        )
        self.claim_evidence_artifact_sample_stride = max(
            1,
            int(bridge_cfg.get("claim_evidence_artifact_sample_stride", 1) or 1),
        )
        critical_trace_cfg = bridge_cfg.get("control_critical_window_trace", {}) or {}
        self.control_critical_window_trace_enabled = bool(
            critical_trace_cfg.get(
                "enabled",
                bridge_cfg.get("control_critical_window_trace_enabled", True),
            )
        )
        self._control_critical_window_recorder = ControlCriticalWindowRecorder(
            enabled=self.control_critical_window_trace_enabled,
            pre_samples=int(critical_trace_cfg.get("pre_samples", 20) or 20),
            post_samples=int(critical_trace_cfg.get("post_samples", 40) or 40),
            max_windows=int(critical_trace_cfg.get("max_windows", 3) or 3),
            max_rows=int(critical_trace_cfg.get("max_rows", 240) or 240),
            route_lateral_error_threshold_m=float(
                critical_trace_cfg.get("route_lateral_error_threshold_m", 0.5) or 0.5
            ),
            simple_lat_lateral_error_threshold_m=float(
                critical_trace_cfg.get("simple_lat_lateral_error_threshold_m", 0.5) or 0.5
            ),
            raw_steer_threshold=float(critical_trace_cfg.get("raw_steer_threshold", 0.85) or 0.85),
            matched_point_distance_threshold_m=float(
                critical_trace_cfg.get("matched_point_distance_threshold_m", 8.0) or 8.0
            ),
            target_point_distance_threshold_m=float(
                critical_trace_cfg.get("target_point_distance_threshold_m", 8.0) or 8.0
            ),
        )
        self._stage5_debug_artifact_sample_counters: Counter[str] = Counter()
        self._reference_debug_artifact_sample_counters: Counter[str] = Counter()
        self._control_debug_artifact_sample_counters: Counter[str] = Counter()
        self._claim_evidence_artifact_sample_counters: Counter[str] = Counter()
        self._last_artifact_stats_flush_sec = time.time()
        self._artifact_pending_rows: Dict[str, int] = {}
        self._artifact_last_flush_sec: Dict[str, float] = {}
        self.stats["artifact_buffering"] = {
            "enabled": True,
            "async_write_enabled": self.artifact_async_write_enabled,
            "async_queue_max_rows": artifact_async_queue_max,
            "async_queue_soft_limit_rows": self.artifact_async_queue_soft_limit_rows,
            "async_queue_size": 0,
            "async_queue_size_max": 0,
            "async_queue_full_count": 0,
            "async_queue_soft_drop_count": 0,
            "async_queue_blocked_duration_s": 0.0,
            "async_enqueued_count": 0,
            "async_written_count": 0,
            "async_write_error_count": 0,
            "flush_interval_s": self.artifact_flush_interval_s,
            "flush_max_pending_rows": self.artifact_flush_max_pending_rows,
            "stats_flush_interval_s": self.artifact_stats_flush_interval_s,
            "stage5_debug_artifact_sample_stride": self.stage5_debug_artifact_sample_stride,
            "stage5_debug_artifact_seen_counts": {},
            "stage5_debug_artifact_sampled_out_counts": {},
            "stage5_debug_artifact_written_counts": {},
            "reference_debug_artifact_sample_stride": self.reference_debug_artifact_sample_stride,
            "reference_debug_artifact_seen_counts": {},
            "reference_debug_artifact_sampled_out_counts": {},
            "reference_debug_artifact_written_counts": {},
            "control_debug_artifact_sample_stride": self.control_debug_artifact_sample_stride,
            "control_debug_artifact_seen_counts": {},
            "control_debug_artifact_sampled_out_counts": {},
            "control_debug_artifact_written_counts": {},
            "claim_evidence_artifact_sample_stride": self.claim_evidence_artifact_sample_stride,
            "claim_evidence_artifact_seen_counts": {},
            "claim_evidence_artifact_sampled_out_counts": {},
            "claim_evidence_artifact_written_counts": {},
            "control_critical_window_trace": self._control_critical_window_recorder.stats(),
            "flush_count": 0,
        }
        self._start_artifact_writer()
        self._publish_loop_duration_window: deque[float] = deque(maxlen=2000)
        self._publish_phase_duration_windows: Dict[str, deque[float]] = {}
        self._publish_phase_overrun_counts: Counter[str] = Counter()
        self._publish_loop_overrun_count = 0
        self._publish_loop_iteration_count = 0
        self._publish_loop_publish_count = 0
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
            "apollo_warmup_bypassed_by_readiness": False,
            "apollo_warmup_readiness": {},
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
        self.require_valid_planning_before_first_publish = bool(
            ctrl_map.get("require_valid_planning_before_first_publish", False)
        )
        self._valid_planning_publish_gate_open = not self.require_valid_planning_before_first_publish
        self.stats["control_startup_publish_gate"] = {
            "enabled": self.require_valid_planning_before_first_publish,
            "open": self._valid_planning_publish_gate_open,
            "skip_count": 0,
            "first_open_timestamp_sec": None,
            "last_skip_reason": "",
            "claim_boundary": (
                "startup_handoff_only; raw Apollo Control remains recorded and the gate never "
                "closes after the first valid Planning trajectory"
            ),
        }
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
        self.steering_percent_normalization = str(
            ctrl_map.get("steering_percent_normalization", STEERING_NORMALIZATION_SINGLE_PERCENT_AT_SELECT)
            or STEERING_NORMALIZATION_SINGLE_PERCENT_AT_SELECT
        ).strip().lower()
        if self.steering_percent_normalization not in STEERING_PERCENT_NORMALIZATION_MODES:
            self.steering_percent_normalization = STEERING_NORMALIZATION_SINGLE_PERCENT_AT_SELECT
        self.brake_deadzone = float(ctrl_map.get("brake_deadzone", 0.05))
        self.throttle_brake_mutual_exclusion_enabled = bool(
            ctrl_map.get("throttle_brake_mutual_exclusion_enabled", True)
        )
        self.throttle_brake_hysteresis_frames = max(
            0,
            int(ctrl_map.get("throttle_brake_hysteresis_frames", 2) or 0),
        )
        self.throttle_brake_min_command = max(
            0.0,
            float(ctrl_map.get("throttle_brake_min_command", 0.05) or 0.0),
        )
        self._throttle_brake_policy_state = "coast"
        self._throttle_brake_hysteresis_counter = 0
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
        self.physical_map_steering = bool(physical_cfg.get("map_steering", True))
        self.physical_map_longitudinal = bool(physical_cfg.get("map_longitudinal", True))
        self.physical_map_throttle = bool(
            physical_cfg.get("map_throttle", self.physical_map_longitudinal)
        )
        self.physical_map_brake = bool(
            physical_cfg.get("map_brake", self.physical_map_longitudinal)
        )
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
            "steer_scale": self.steer_scale,
            "steer_sign": self.steer_sign,
            "allow_legacy_fallback": self.physical_allow_legacy_fallback,
            "map_steering": self.physical_map_steering,
            "map_longitudinal": self.physical_map_longitudinal,
            "map_throttle": self.physical_map_throttle,
            "map_brake": self.physical_map_brake,
            "apollo_max_steer_angle_deg": self.physical_apollo_max_steer_angle_deg,
            "apollo_max_accel_mps2": self.physical_apollo_max_accel_mps2,
            "apollo_max_decel_mps2": self.physical_apollo_max_decel_mps2,
            "steering_field_priority": list(self.physical_steer_field_priority),
            "steering_percent_normalization": self.steering_percent_normalization,
            "acceleration_field_priority": list(self.physical_acceleration_field_priority),
            "throttle_brake_mutual_exclusion_enabled": self.throttle_brake_mutual_exclusion_enabled,
            "throttle_brake_hysteresis_frames": self.throttle_brake_hysteresis_frames,
            "throttle_brake_min_command": self.throttle_brake_min_command,
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
        self.front_obstacle_actor_probe_enabled = bool(
            front_obstacle_cfg.get(
                "actor_probe_enabled",
                self.front_obstacle_behavior_mode != "normal",
            )
        )
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
        self.raw_routing_response_channel = str(
            cyber_cfg.get("raw_routing_response_channel", "/apollo/raw_routing_response")
            or "/apollo/raw_routing_response"
        )
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
        self.auto_routing_startup_route_enabled = bool(
            auto_routing_cfg.get("startup_route_enabled", True)
        )
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
        self.auto_routing_startup_apollo_warmup_bypass_when_ready = bool(
            auto_routing_cfg.get(
                "startup_apollo_warmup_bypass_when_ready",
                bool(self.claim_profile_enabled or self.materialization_probe_enabled),
            )
        )
        self.auto_routing_startup_apollo_warmup_bypass_min_elapsed_sec = max(
            0.0,
            float(
                auto_routing_cfg.get(
                    "startup_apollo_warmup_bypass_min_elapsed_sec",
                    max(self.auto_routing_startup_delay_sec, 8.0),
                )
            ),
        )
        self.auto_routing_startup_apollo_warmup_ready_min_planning_messages = max(
            0,
            int(auto_routing_cfg.get("startup_apollo_warmup_ready_min_planning_messages", 3) or 0),
        )
        self.auto_routing_startup_apollo_warmup_ready_accept_preplanning_gt_only = bool(
            auto_routing_cfg.get(
                "startup_apollo_warmup_ready_accept_preplanning_gt_only",
                False,
            )
        )
        self.auto_routing_startup_apollo_warmup_ready_min_gt_fresh_samples = max(
            0,
            int(auto_routing_cfg.get("startup_apollo_warmup_ready_min_gt_fresh_samples", 3) or 0),
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
        if self.claim_profile_enabled:
            self.auto_routing_auto_enable_lane_follow_fallback = False
            self.auto_routing_startup_route_enabled = False
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
        self.auto_routing_relay_raw_routing_response_to_planning = bool(
            auto_routing_cfg.get("relay_raw_routing_response_to_planning", True)
        )
        self.stats["routing_channels"] = {
            "routing_request_channel": self.routing_request_channel,
            "raw_routing_response_channel": self.raw_routing_response_channel,
            "planning_routing_response_channel": self.routing_response_channel,
            "relay_raw_routing_response_to_planning": bool(
                self.auto_routing_relay_raw_routing_response_to_planning
                and self.raw_routing_response_channel != self.routing_response_channel
            ),
            "claim_boundary": (
                "Apollo 10 routing emits raw RoutingResponse on /apollo/raw_routing_response; "
                "planning consumes /apollo/routing_response. The relay, when enabled, forwards "
                "only observed Apollo raw responses and must not synthesize scenario routes."
            ),
        }
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
        if self.claim_profile_enabled:
            self.auto_routing_goal_validity_fallback_enabled = False
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
        self.stats["claim_profile_route_policy"] = {
            "claim_profile_enabled": bool(self.claim_profile_enabled),
            "materialization_probe_enabled": bool(self.materialization_probe_enabled),
            "startup_route_enabled": bool(self.auto_routing_startup_route_enabled),
            "auto_enable_lane_follow_fallback": bool(
                self.auto_routing_auto_enable_lane_follow_fallback
            ),
            "startup_apollo_warmup_bypass_when_ready": bool(
                self.auto_routing_startup_apollo_warmup_bypass_when_ready
            ),
            "startup_apollo_warmup_bypass_min_elapsed_sec": float(
                self.auto_routing_startup_apollo_warmup_bypass_min_elapsed_sec
            ),
            "startup_apollo_warmup_ready_min_planning_messages": int(
                self.auto_routing_startup_apollo_warmup_ready_min_planning_messages
            ),
            "startup_apollo_warmup_ready_accept_preplanning_gt_only": bool(
                self.auto_routing_startup_apollo_warmup_ready_accept_preplanning_gt_only
            ),
            "startup_apollo_warmup_ready_min_gt_fresh_samples": int(
                self.auto_routing_startup_apollo_warmup_ready_min_gt_fresh_samples
            ),
            "goal_validity_fallback_enabled": bool(
                self.auto_routing_goal_validity_fallback_enabled
            ),
            "fallback_blocked_by_claim_profile_count": 0,
        }
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
        self.apollo_runtime_map_dir_raw = str(bridge_cfg.get("apollo_runtime_map_dir", "") or "").strip()
        self.apollo_runtime_map_dir = _bridge_path_text(self.apollo_runtime_map_dir_raw)
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
        self.stats["map_path_resolution"] = {
            "map_file_raw": self.map_file_path_raw,
            "map_file_resolved": self.map_file_path,
            "map_bounds_file_raw": self.map_bounds_file_raw,
            "map_bounds_file_resolved": self.map_bounds_file,
            "apollo_runtime_map_dir_raw": self.apollo_runtime_map_dir_raw,
            "apollo_runtime_map_dir_resolved": self.apollo_runtime_map_dir,
            "apollo_map_root_env": os.environ.get("APOLLO_MAP_ROOT", ""),
        }
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
        self._traffic_light_last_entries: List[Tuple[str, str]] = []
        self._traffic_light_last_contain_lights = False
        self._traffic_light_last_color_source = ""
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
        # 2026-06-30: Planning stall detection — track consecutive empty trajectories
        self._planning_stall_consecutive_empty: int = 0
        self._planning_stall_first_empty_ts_sec: Optional[float] = None
        self._planning_stall_last_reported: int = 0
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
                invert_tf=bool(
                    self.direct_bridge_cfg.get(
                        "invert_tf",
                        ((apollo_cfg.get("carla_to_apollo", {}) or {}).get("invert_tf", True)),
                    )
                ),
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
                control_apply_mode=str(
                    self.direct_bridge_cfg.get("control_apply_mode", "immediate_or_defer")
                ),
                stale_world_frame_policy=str(
                    self.direct_bridge_cfg.get("stale_world_frame_policy", "until_control")
                ),
                straight_lane_lateral_stabilizer_enabled=bool(
                    self.direct_bridge_cfg.get("straight_lane_lateral_stabilizer_enabled", False)
                ),
                straight_lane_lateral_stabilizer_max_abs_steer=float(
                    self.direct_bridge_cfg.get("straight_lane_lateral_stabilizer_max_abs_steer", 0.04)
                ),
                straight_lane_lateral_stabilizer_k_cte=float(
                    self.direct_bridge_cfg.get("straight_lane_lateral_stabilizer_k_cte", 0.015)
                ),
                straight_lane_lateral_stabilizer_k_heading=float(
                    self.direct_bridge_cfg.get("straight_lane_lateral_stabilizer_k_heading", 0.25)
                ),
                straight_lane_lateral_stabilizer_max_cte_m=float(
                    self.direct_bridge_cfg.get("straight_lane_lateral_stabilizer_max_cte_m", 4.0)
                ),
                straight_lane_lateral_stabilizer_max_heading_error_deg=float(
                    self.direct_bridge_cfg.get("straight_lane_lateral_stabilizer_max_heading_error_deg", 20.0)
                ),
                straight_lane_lateral_stabilizer_max_speed_mps=float(
                    self.direct_bridge_cfg.get("straight_lane_lateral_stabilizer_max_speed_mps", 30.0)
                ),
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
        self.routing_response_writer = None
        self.action_client = None
        self.lane_follow_client = None
        if self.auto_routing_enabled:
            if self.auto_routing_send_routing:
                self.routing_writer = self._cyber_create_writer(
                    self.routing_request_channel, self.routing_pb2.RoutingRequest
                )
            relay_raw_response = bool(
                self.auto_routing_relay_raw_routing_response_to_planning
                and self.raw_routing_response_channel
                and self.routing_response_channel
                and self.raw_routing_response_channel != self.routing_response_channel
            )
            if relay_raw_response:
                self.routing_response_writer = self._cyber_create_writer(
                    self.routing_response_channel, self.routing_pb2.RoutingResponse
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
            if relay_raw_response:
                self._cyber_create_reader(
                    self.raw_routing_response_channel,
                    self.routing_pb2.RoutingResponse,
                    self._on_raw_routing_response,
                )
            else:
                self._cyber_create_reader(
                    self.routing_response_channel,
                    self.routing_pb2.RoutingResponse,
                    self._on_routing_response,
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

    def _front_actor_dimensions(self, actor_id: Optional[int], actor: Any) -> Dict[str, Any]:
        length = None
        width = None
        height = None
        source = "missing"
        warnings: List[str] = []
        try:
            extent = getattr(getattr(actor, "bounding_box", None), "extent", None)
            if extent is not None:
                length = 2.0 * float(getattr(extent, "x", 0.0))
                width = 2.0 * float(getattr(extent, "y", 0.0))
                height = 2.0 * float(getattr(extent, "z", 0.0))
                source = "carla_actor_bbox"
        except Exception:
            length = None
            width = None
            height = None
            source = "carla_actor_bbox_error"
        values = {"length": length, "width": width, "height": height}
        if all(_finite_or_none(value) is not None and float(value) > 0.0 for value in values.values()):
            if actor_id is not None:
                self._front_actor_dimension_cache[int(actor_id)] = {
                    key: float(value) for key, value in values.items() if value is not None
                }
            return {**values, "source": source, "warnings": warnings}
        cached = self._front_actor_dimension_cache.get(int(actor_id)) if actor_id is not None else None
        if cached:
            warnings.append("front_actor_bbox_non_positive_used_cached_dimensions")
            return {
                "length": cached.get("length"),
                "width": cached.get("width"),
                "height": cached.get("height"),
                "source": "cached_carla_actor_bbox",
                "warnings": warnings,
            }
        if source != "missing":
            warnings.append("front_actor_bbox_non_positive")
        return {**values, "source": source, "warnings": warnings}

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

    def _fill_header(
        self,
        header: Any,
        ts_sec: float,
        module_name: str,
        frame_id: str = "map",
    ) -> Optional[int]:
        if header is None:
            return None
        sequence_num = self._next_seq()
        fill_apollo_header(
            header,
            timestamp_sec=ts_sec,
            module_name=module_name,
            sequence_num=sequence_num,
            frame_id=frame_id,
        )
        return sequence_num

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
            # publish_row is a wide diagnostic/evidence row. Many fields are
            # optional and intentionally absent for a given controller/profile;
            # analyzers should handle those as missing fields instead of making
            # the 20 Hz GT publish loop print/rate-limit warnings.
            if source != "publish_row":
                self._warn_bad_value(field, value, source)
            return float(default)
        try:
            return float(value)
        except Exception:
            self._warn_bad_value(field, value, source)
            return float(default)

    def _start_artifact_writer(self) -> None:
        if not bool(getattr(self, "artifact_async_write_enabled", False)):
            return
        if getattr(self, "_artifact_writer_started", False):
            return
        write_queue = getattr(self, "_artifact_write_queue", None)
        if write_queue is None:
            return
        thread = threading.Thread(
            target=self._artifact_writer_loop,
            name="apollo-bridge-artifact-writer",
            daemon=True,
        )
        self._artifact_writer_thread = thread
        self._artifact_writer_started = True
        thread.start()

    def _artifact_writer_loop(self) -> None:
        write_queue = getattr(self, "_artifact_write_queue", None)
        if write_queue is None:
            return
        while True:
            item = write_queue.get()
            try:
                if item is None:
                    return
                if len(item) == 4:
                    kind, path, payload, enqueued_wall_s = item
                else:
                    kind, path, payload = item
                    enqueued_wall_s = None
                write_start_s = time.time()
                if enqueued_wall_s is not None:
                    lag_s = max(0.0, write_start_s - float(enqueued_wall_s))
                    buffering = self.stats.setdefault("artifact_buffering", {})
                    buffering["artifact_writer_queue_lag_s"] = lag_s
                    buffering["artifact_writer_queue_lag_s_max"] = max(
                        float(buffering.get("artifact_writer_queue_lag_s_max", 0.0) or 0.0),
                        lag_s,
                    )
                if kind == "jsonl":
                    self._append_jsonl_direct(Path(path), payload)
                elif kind == "csv":
                    self._write_csv_row_direct(Path(path), payload)
                write_duration_s = max(0.0, time.time() - write_start_s)
                buffering = self.stats.setdefault("artifact_buffering", {})
                buffering["async_written_count"] = int(buffering.get("async_written_count", 0) or 0) + 1
                buffering["last_writer_write_duration_s"] = write_duration_s
                buffering["writer_write_duration_s_max"] = max(
                    float(buffering.get("writer_write_duration_s_max", 0.0) or 0.0),
                    write_duration_s,
                )
            except Exception as exc:
                if not hasattr(self, "stats") or getattr(self, "stats", None) is None:
                    self.stats = {}
                buffering = self.stats.setdefault("artifact_buffering", {})
                buffering["async_write_error_count"] = int(
                    buffering.get("async_write_error_count", 0) or 0
                ) + 1
                buffering["last_async_write_error"] = f"{type(exc).__name__}:{exc}"
            finally:
                write_queue.task_done()

    def _enqueue_artifact_write(self, kind: str, path: Path, payload: Dict[str, Any]) -> bool:
        if not bool(getattr(self, "artifact_async_write_enabled", False)):
            return False
        write_queue = getattr(self, "_artifact_write_queue", None)
        if write_queue is None:
            return False
        self._start_artifact_writer()
        blocked_duration_s = 0.0
        buffering = self.stats.setdefault("artifact_buffering", {})
        try:
            queue_size = int(write_queue.qsize())
        except Exception:
            queue_size = 0
        soft_limit = int(getattr(self, "artifact_async_queue_soft_limit_rows", 0) or 0)
        if soft_limit > 0 and queue_size >= soft_limit:
            buffering["async_queue_soft_drop_count"] = int(
                buffering.get("async_queue_soft_drop_count", 0) or 0
            ) + 1
            buffering["async_dropped_count"] = int(buffering.get("async_dropped_count", 0) or 0) + 1
            buffering["last_async_drop_kind"] = str(kind)
            buffering["last_async_drop_path"] = str(path)
            buffering["artifact_backpressure_claim_blocking"] = True
            buffering["async_queue_size"] = queue_size
            buffering["async_queue_size_max"] = max(
                int(buffering.get("async_queue_size_max", 0) or 0),
                queue_size,
            )
            return True
        try:
            write_queue.put_nowait((kind, str(path), dict(payload), time.time()))
        except queue.Full:
            # Never let diagnostic artifact IO block the publish loop. Queue
            # saturation is explicit evidence that the run is not claim-grade.
            buffering["async_queue_full_count"] = int(
                buffering.get("async_queue_full_count", 0) or 0
            ) + 1
            buffering["async_dropped_count"] = int(buffering.get("async_dropped_count", 0) or 0) + 1
            buffering["last_async_drop_kind"] = str(kind)
            buffering["last_async_drop_path"] = str(path)
            buffering["artifact_backpressure_claim_blocking"] = True
            return True
        buffering["async_enqueued_count"] = int(buffering.get("async_enqueued_count", 0) or 0) + 1
        if blocked_duration_s > 0.0:
            buffering["async_queue_full_count"] = int(
                buffering.get("async_queue_full_count", 0) or 0
            ) + 1
            buffering["async_queue_blocked_duration_s"] = float(
                buffering.get("async_queue_blocked_duration_s", 0.0) or 0.0
            ) + blocked_duration_s
        try:
            queue_size = int(write_queue.qsize())
            buffering["async_queue_size"] = queue_size
            buffering["async_queue_size_max"] = max(
                int(buffering.get("async_queue_size_max", 0) or 0),
                queue_size,
            )
        except Exception:
            pass
        return True

    def _append_jsonl(self, path: Path, payload: Dict[str, Any]) -> None:
        if self._enqueue_artifact_write("jsonl", path, payload):
            return
        self._append_jsonl_direct(path, payload)

    def _append_jsonl_direct(self, path: Path, payload: Dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        key = str(path)
        handles = getattr(self, "_jsonl_artifact_handles", None)
        if handles is None:
            handles = {}
            self._jsonl_artifact_handles = handles
        with getattr(self, "_artifact_write_lock", threading.Lock()):
            fp = handles.get(key)
            if fp is None or getattr(fp, "closed", False):
                fp = path.open("a")
                handles[key] = fp
                getattr(self, "_artifact_last_flush_sec", {}).setdefault(key, time.time())
            fp.write(json.dumps(payload, ensure_ascii=True) + "\n")
            self._maybe_flush_artifact_buffer(key, fp)

    def _maybe_flush_artifact_buffer(self, key: str, fp: Any, *, force: bool = False) -> None:
        pending_rows = getattr(self, "_artifact_pending_rows", None)
        if pending_rows is None:
            pending_rows = {}
            self._artifact_pending_rows = pending_rows
        last_flush = getattr(self, "_artifact_last_flush_sec", None)
        if last_flush is None:
            last_flush = {}
            self._artifact_last_flush_sec = last_flush
        if not force:
            pending_rows[key] = int(pending_rows.get(key, 0) or 0) + 1
        pending = int(pending_rows.get(key, 0) or 0)
        now = time.time()
        interval_s = max(0.0, float(getattr(self, "artifact_flush_interval_s", 0.5) or 0.0))
        max_pending_raw = getattr(self, "artifact_flush_max_pending_rows", 100)
        try:
            max_pending = int(max_pending_raw)
        except Exception:
            max_pending = 100
        previous_flush = float(last_flush.get(key, now) or now)
        due = force or (
            (max_pending > 0 and pending >= max_pending)
            or (interval_s > 0.0 and pending > 0 and (now - previous_flush) >= interval_s)
        )
        if not due:
            return
        fp.flush()
        pending_rows[key] = 0
        last_flush[key] = now
        if not hasattr(self, "stats") or getattr(self, "stats", None) is None:
            self.stats = {}
        buffering = self.stats.setdefault("artifact_buffering", {})
        buffering["flush_count"] = int(buffering.get("flush_count", 0) or 0) + 1
        buffering["last_flush_path"] = key
        buffering["last_flush_pending_rows"] = pending

    def _flush_artifact_buffers(self, *, close: bool = False) -> None:
        write_queue = getattr(self, "_artifact_write_queue", None)
        if close and write_queue is not None:
            try:
                write_queue.join()
            except Exception:
                pass
        if write_queue is not None:
            try:
                buffering = self.stats.setdefault("artifact_buffering", {})
                queue_size = int(write_queue.qsize())
                buffering["async_queue_size"] = queue_size
                buffering["async_queue_size_max"] = max(
                    int(buffering.get("async_queue_size_max", 0) or 0),
                    queue_size,
                )
            except Exception:
                pass
        for handles in (
            getattr(self, "_jsonl_artifact_handles", {}) or {},
            {
                key: state.get("fp")
                for key, state in (getattr(self, "_csv_artifact_states", {}) or {}).items()
                if isinstance(state, dict)
            },
        ):
            with getattr(self, "_artifact_write_lock", threading.Lock()):
                for key, fp in list(handles.items()):
                    try:
                        self._maybe_flush_artifact_buffer(key, fp, force=True)
                    except Exception:
                        pass
                    if close:
                        try:
                            fp.close()
                        except Exception:
                            pass
                        try:
                            handles.pop(key, None)
                        except Exception:
                            pass
                        try:
                            self._artifact_pending_rows.pop(key, None)
                            self._artifact_last_flush_sec.pop(key, None)
                        except Exception:
                            pass
        if close and write_queue is not None:
            try:
                write_queue.put(None, timeout=1.0)
                writer_thread = getattr(self, "_artifact_writer_thread", None)
                if writer_thread is not None:
                    writer_thread.join(timeout=5.0)
            except Exception:
                pass

    def _record_publish_loop_timing(
        self,
        *,
        start_wall_s: float,
        end_wall_s: float,
        target_period_s: float,
        published_gt: bool,
        phase: str,
    ) -> None:
        duration_s = max(0.0, float(end_wall_s) - float(start_wall_s))
        window = getattr(self, "_publish_loop_duration_window", None)
        if window is None:
            window = deque(maxlen=2000)
            self._publish_loop_duration_window = window
        window.append(duration_s)
        self._publish_loop_iteration_count = int(getattr(self, "_publish_loop_iteration_count", 0) or 0) + 1
        if published_gt:
            self._publish_loop_publish_count = int(getattr(self, "_publish_loop_publish_count", 0) or 0) + 1
        if target_period_s > 0.0 and duration_s > target_period_s:
            self._publish_loop_overrun_count = int(getattr(self, "_publish_loop_overrun_count", 0) or 0) + 1
        ordered = sorted(float(item) for item in window)
        p95 = ordered[int(round((len(ordered) - 1) * 0.95))] if ordered else None
        self.stats["publish_loop_timing"] = {
            "mode": "deadline",
            "last_phase": phase,
            "target_period_s": float(target_period_s),
            "last_duration_s": duration_s,
            "recent_sample_count": len(window),
            "recent_duration_p95_s": p95,
            "iteration_count": int(getattr(self, "_publish_loop_iteration_count", 0) or 0),
            "published_gt_iteration_count": int(getattr(self, "_publish_loop_publish_count", 0) or 0),
            "over_target_period_count": int(getattr(self, "_publish_loop_overrun_count", 0) or 0),
        }

    def _record_publish_phase_timing(
        self,
        phase: str,
        *,
        start_wall_s: float,
        end_wall_s: float,
        target_period_s: float,
    ) -> None:
        phase_name = str(phase or "unknown")
        duration_s = max(0.0, float(end_wall_s) - float(start_wall_s))
        windows = getattr(self, "_publish_phase_duration_windows", None)
        if windows is None:
            windows = {}
            self._publish_phase_duration_windows = windows
        window = windows.get(phase_name)
        if window is None:
            window = deque(maxlen=2000)
            windows[phase_name] = window
        window.append(duration_s)
        overrun_counts = getattr(self, "_publish_phase_overrun_counts", None)
        if overrun_counts is None:
            overrun_counts = Counter()
            self._publish_phase_overrun_counts = overrun_counts
        if target_period_s > 0.0 and duration_s > target_period_s:
            overrun_counts[phase_name] += 1
        phase_stats: Dict[str, Any] = {}
        for name, values in sorted(windows.items()):
            ordered = sorted(float(item) for item in values)
            if not ordered:
                continue
            p95 = ordered[int(round((len(ordered) - 1) * 0.95))]
            phase_stats[name] = {
                "recent_sample_count": len(ordered),
                "last_duration_s": float(values[-1]),
                "recent_duration_p95_s": p95,
                "recent_duration_max_s": ordered[-1],
                "over_target_period_count": int(overrun_counts.get(name, 0)),
            }
        self.stats["publish_loop_phase_timing"] = phase_stats

    def _record_topic_publish_stats(
        self,
        *,
        channel: str,
        msg: Any,
        sim_time_sec: Optional[float],
        payload_count: Optional[int],
        source: str,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        path = getattr(self, "topic_publish_stats_path", None)
        if path is None:
            return
        header = getattr(msg, "header", None)
        frame_id = getattr(header, "frame_id", None) if header is not None else None
        world_frame = self._latest_world_frame
        if world_frame is None and extra:
            world_frame = extra.get("world_frame") or extra.get("carla_world_frame")
        row: Dict[str, Any] = {
            "channel": str(channel),
            "wall_time_sec": time.time(),
            "sim_time_sec": _finite_or_none(sim_time_sec),
            "header_timestamp_sec": _finite_or_none(self._header_timestamp_sec(msg)),
            "sequence_num": self._header_sequence_num(msg),
            "frame_id": None if frame_id is None else str(frame_id),
            "carla_world_frame": world_frame,
            "world_frame": world_frame,
            "source_clock": "sim_time" if sim_time_sec is not None else "wall_time",
            "payload_count": payload_count,
            "source": source,
            "message_count_increment": 1,
        }
        if extra:
            row.update(extra)
        self._append_jsonl(Path(path), row)

    def _record_control_apply_trace(self, row: Dict[str, Any], measured: Dict[str, Any]) -> None:
        path = getattr(self, "control_apply_trace_path", None)
        should_write_sampled = bool(path is not None) and self._should_write_claim_evidence_artifact(
            "control_apply_trace"
        )
        critical_recorder = getattr(self, "_control_critical_window_recorder", None)
        critical_enabled = bool(
            critical_recorder is not None
            and getattr(critical_recorder, "enabled", False)
            and getattr(self, "control_critical_window_trace_path", None) is not None
        )
        if not should_write_sampled and not critical_enabled:
            return
        payload = build_control_apply_trace_payload(row, measured, self.stats)
        if critical_enabled:
            critical_rows = critical_recorder.observe(payload)
            buffering = self.stats.setdefault("artifact_buffering", {})
            buffering["control_critical_window_trace"] = critical_recorder.stats()
            for critical_row in critical_rows:
                self._append_jsonl(Path(self.control_critical_window_trace_path), critical_row)
        if should_write_sampled:
            self._append_jsonl(Path(path), payload)

    def _gt_sample_key_from_odom(
        self,
        odom: Any,
        *,
        direct_world_frame: Optional[int] = None,
    ) -> Optional[Tuple[Any, ...]]:
        sim_time_sec = self._extract_ros_stamp_sec(odom)
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
        if world_frame is None:
            header = getattr(odom, "header", None)
            try:
                seq_value = getattr(header, "seq", None) if header is not None else None
                if seq_value is not None:
                    world_frame = int(seq_value)
            except Exception:
                world_frame = None
        if sim_time_sec is not None:
            return ("sim_time", round(float(sim_time_sec), 9), "world_frame", world_frame)
        if world_frame is not None:
            return ("world_frame", world_frame)
        return None

    def _should_publish_gt_sample(
        self,
        odom: Any,
        *,
        direct_world_frame: Optional[int] = None,
    ) -> Tuple[bool, Optional[Tuple[Any, ...]], str]:
        key = self._gt_sample_key_from_odom(odom, direct_world_frame=direct_world_frame)
        if key is None:
            return True, None, "sample_identity_missing_publish_for_diagnostic"
        previous = getattr(self, "_last_gt_publish_sample_key", None)
        if previous != key:
            self._last_gt_publish_sample_key = key
            return True, key, "fresh_sample"

        self.stats["gt_stale_sample_duplicate_count"] = int(
            self.stats.get("gt_stale_sample_duplicate_count", 0) or 0
        ) + 1
        policy = str(getattr(self, "gt_stale_sample_policy", "") or "").strip().lower()
        self.stats["gt_stale_sample_policy"] = policy
        if policy == "skip" or bool(getattr(self, "claim_grade_enabled", False)):
            self.stats["gt_stale_sample_skip_count"] = int(
                self.stats.get("gt_stale_sample_skip_count", 0) or 0
            ) + 1
            return False, key, "stale_sample_skipped"
        self.stats["gt_stale_sample_republish_count"] = int(
            self.stats.get("gt_stale_sample_republish_count", 0) or 0
        ) + 1
        return True, key, "stale_sample_republished_for_debug"

    def _should_publish_obstacles(self, sim_time_sec: float) -> bool:
        period_s = 1.0 / max(float(self.obstacle_publish_rate_hz), 1e-3)
        previous = self._last_obstacle_publish_sim_time
        if (
            previous is None
            or sim_time_sec < previous
            or (sim_time_sec - previous) >= period_s - 1e-6
        ):
            self._last_obstacle_publish_sim_time = float(sim_time_sec)
            return True
        return False

    def _write_csv_row(self, path: Path, row: Dict[str, Any]) -> None:
        if self._enqueue_artifact_write("csv", path, row):
            return
        self._write_csv_row_direct(path, row)

    def _write_csv_row_direct(self, path: Path, row: Dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = list(row.keys())
        key = str(path)
        states = getattr(self, "_csv_artifact_states", None)
        if states is None:
            states = {}
            self._csv_artifact_states = states
        with getattr(self, "_artifact_write_lock", threading.Lock()):
            state = states.get(key)
            if state is not None and state.get("fieldnames") != fieldnames:
                try:
                    state.get("fp").flush()
                    state.get("fp").close()
                except Exception:
                    pass
                states.pop(key, None)
                state = None
            if state is None:
                mode = "a" if self._split_csv_headers_written.get(key) and path.exists() else "w"
                fp = path.open(mode, newline="")
                writer = csv.DictWriter(fp, fieldnames=fieldnames)
                if mode == "w" or not self._split_csv_headers_written.get(key, False):
                    writer.writeheader()
                    self._split_csv_headers_written[key] = True
                state = {"fp": fp, "writer": writer, "fieldnames": fieldnames}
                states[key] = state
                getattr(self, "_artifact_last_flush_sec", {}).setdefault(key, time.time())
            writer = state["writer"]
            writer.writerow(row)
            self._maybe_flush_artifact_buffer(key, state["fp"])

    def _write_json_file(self, path: Path, payload: Dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2))
        tmp.replace(path)

    def _extract_ros_stamp_sec(self, msg: Any) -> Optional[float]:
        header = getattr(msg, "header", None)
        stamp = getattr(header, "stamp", None) if header is not None else None
        if stamp is None:
            return None
        stamp_sec = _stamp_to_sec(stamp)
        return stamp_sec if math.isfinite(stamp_sec) and stamp_sec > 0.0 else None

    def _localization_time_from_odom(
        self,
        odom: Any,
        *,
        fallback_wall_time_sec: float,
    ) -> Tuple[float, str]:
        source = str(getattr(self, "localization_time_source", "auto") or "auto").lower()
        if source == "cyber_time":
            return float(fallback_wall_time_sec), "cyber_time_diagnostic"
        sim_time_sec = self._extract_ros_stamp_sec(odom)
        if source == "sim_time":
            if sim_time_sec is not None:
                return float(sim_time_sec), "sim_time"
            return float(fallback_wall_time_sec), "sim_time_missing_cyber_time_fallback"
        if sim_time_sec is not None:
            return float(sim_time_sec), "sim_time"
        return float(fallback_wall_time_sec), "cyber_time_fallback"

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
        report = {
            "schema_version": "goal_validity_report.v1",
            "status": "blocked"
            if bool(payload.get("fallback_blocked_by_claim_profile"))
            else ("fail" if bool(payload.get("invalid_goal")) else "pass"),
            "claim_profile_enabled": bool(getattr(self, "claim_profile_enabled", False)),
            "materialization_probe_enabled": bool(
                getattr(self, "materialization_probe_enabled", False)
            ),
            "fallback_blocked_by_claim_profile": bool(
                payload.get("fallback_blocked_by_claim_profile", False)
            ),
            "invalid_goal": bool(payload.get("invalid_goal", False)),
            "invalid_goal_reason": str(payload.get("invalid_goal_reason", "") or ""),
            "goal_source": str(payload.get("goal_source", "") or ""),
            "goal_mode": str(payload.get("goal_mode", "") or ""),
            "requested_goal_mode": str(payload.get("requested_goal_mode", "") or ""),
            "routing_phase": str(payload.get("routing_phase", "") or ""),
            "fallback_applied": bool(payload.get("fallback_applied", False)),
            "claim_boundary": (
                "Claim/materialization profiles must use an explicit scenario/fixed route goal. "
                "ego_seed_ahead/startup_short_ahead/invalid-goal fallback routes are diagnostic "
                "only and cannot support Apollo natural-driving claims."
            ),
        }
        self._write_json_file(self.goal_validity_report_path, report)

    def _record_publish_gap_trace(
        self,
        *,
        snapshot: Optional[Dict[str, Any]] = None,
        odom: Any = None,
        expected_publish: bool = True,
        published_localization: bool = False,
        published_chassis: bool = False,
        skip_reason: str = "unknown",
        loop_start_wall_s: Optional[float] = None,
        publish_loop_duration_s: Optional[float] = None,
    ) -> None:
        snapshot = snapshot or {}
        now_wall_s = time.time()
        if publish_loop_duration_s is None and loop_start_wall_s is not None:
            publish_loop_duration_s = max(0.0, now_wall_s - float(loop_start_wall_s))
        odom_stamp = self._extract_ros_stamp_sec(odom) if odom is not None else None
        latest_odom_age_ms: Optional[float] = None
        # ROS stamps may use sim-time rather than wall-time. Only report
        # wall-age when the stamp is clearly in Unix-time space.
        if odom_stamp is not None and odom_stamp > 1_000_000_000.0:
            latest_odom_age_ms = max(0.0, (now_wall_s - odom_stamp) * 1000.0)
        buffering = self.stats.get("artifact_buffering") if isinstance(self.stats, dict) else {}
        if not isinstance(buffering, Mapping):
            buffering = {}
        world_frame = snapshot.get("world_frame")
        if world_frame is None:
            world_frame = getattr(self, "_latest_world_frame", None)
        snapshot_wall_s = self._first_number_from_mapping(
            snapshot,
            (
                "snapshot_wall_time_sec",
                "snapshot_wall_time_s",
                "created_wall_time_sec",
                "created_wall_time_s",
                "wall_time_sec",
                "wall_time_s",
            ),
        )
        snapshot_age_ms = None
        if snapshot_wall_s is not None and snapshot_wall_s > 1_000_000_000.0:
            snapshot_age_ms = max(0.0, (now_wall_s - snapshot_wall_s) * 1000.0)
        carla_tick_gap_ms = self._first_number_from_mapping(
            snapshot,
            (
                "carla_tick_gap_ms",
                "world_tick_gap_ms",
                "inter_tick_wall_interval_ms",
            ),
        )
        if carla_tick_gap_ms is None:
            carla_tick_gap_s = self._first_number_from_mapping(
                snapshot,
                (
                    "carla_tick_gap_s",
                    "world_tick_gap_s",
                    "inter_tick_wall_interval_s",
                    "world_tick_wall_interval_s",
                ),
            )
            if carla_tick_gap_s is not None:
                carla_tick_gap_ms = carla_tick_gap_s * 1000.0
        payload_build_ms = self._first_number_from_mapping(
            snapshot,
            ("artifact_payload_build_ms", "payload_build_ms"),
        )
        if payload_build_ms is None:
            payload_build_s = self._first_number_from_mapping(
                snapshot,
                ("artifact_payload_build_s", "payload_build_s"),
            )
            if payload_build_s is not None:
                payload_build_ms = payload_build_s * 1000.0
        self._append_jsonl(
            self.publish_gap_trace_path,
            {
                "schema_version": "publish_gap_trace.v1",
                "wall_time_sec": now_wall_s,
                "world_frame": world_frame,
                "carla_world_frame": world_frame,
                "sim_time": odom_stamp if odom_stamp is not None else getattr(self, "_latest_sim_time_sec", None),
                "sim_time_sec": odom_stamp if odom_stamp is not None else getattr(self, "_latest_sim_time_sec", None),
                "source_clock": "sim_time" if odom_stamp is not None else "wall_time",
                "expected_publish": bool(expected_publish),
                "published_localization": bool(published_localization),
                "published_chassis": bool(published_chassis),
                "skip_reason": str(skip_reason or "unknown"),
                "latest_odom_age_ms": latest_odom_age_ms,
                "snapshot_age_ms": snapshot_age_ms,
                "carla_tick_gap_ms": carla_tick_gap_ms,
                "publish_loop_duration_ms": (
                    publish_loop_duration_s * 1000.0
                    if publish_loop_duration_s is not None
                    else None
                ),
                "artifact_payload_build_ms": payload_build_ms,
                "writer_write_duration_ms": (
                    float(buffering.get("last_writer_write_duration_s", 0.0) or 0.0) * 1000.0
                    if buffering
                    else None
                ),
                "writer_write_duration_ms_max": (
                    float(buffering.get("writer_write_duration_s_max", 0.0) or 0.0) * 1000.0
                    if buffering
                    else None
                ),
                "artifact_writer_queue_lag_ms": (
                    float(buffering.get("artifact_writer_queue_lag_s", 0.0) or 0.0) * 1000.0
                    if buffering
                    else None
                ),
                "artifact_writer_oldest_item_age_ms": (
                    float(buffering.get("artifact_writer_queue_lag_s_max", 0.0) or 0.0) * 1000.0
                    if buffering
                    else None
                ),
                "stats_write_ms": (
                    float(buffering.get("stats_write_duration_s", 0.0) or 0.0) * 1000.0
                    if buffering
                    else None
                ),
                "async_queue_depth": buffering.get("async_queue_size"),
                "async_queue_depth_max": buffering.get("async_queue_size_max"),
                "async_queue_full_count": buffering.get("async_queue_full_count"),
                "async_queue_soft_drop_count": buffering.get("async_queue_soft_drop_count"),
                "async_dropped_count": buffering.get("async_dropped_count"),
                "artifact_backpressure_s": buffering.get("async_queue_blocked_duration_s"),
                "artifact_backpressure": bool(
                    int(buffering.get("async_queue_full_count", 0) or 0) > 0
                    or int(buffering.get("async_dropped_count", 0) or 0) > 0
                ),
            },
        )

    def _first_number_from_mapping(
        self,
        mapping: Mapping[str, Any],
        keys: Sequence[str],
    ) -> Optional[float]:
        if not isinstance(mapping, Mapping):
            return None
        for key in keys:
            try:
                value = mapping.get(key)
                if value is None:
                    continue
                number = float(value)
            except Exception:
                continue
            if number == number:
                return number
        return None

    def _record_obstacle_contract_event(self, payload: Dict[str, Any]) -> None:
        if not self._should_write_claim_evidence_artifact("obstacle_gt_contract"):
            return
        self._append_jsonl(self.obstacle_contract_debug_path, payload)

    def _ego_actor_id_for_obstacle_contract(self) -> Optional[Any]:
        # ros2_gt mode does not own a direct CARLA vehicle object; this field is
        # diagnostic evidence only and must not break the GT publish loop.
        return getattr(getattr(self, "vehicle", None), "id", None)

    def _record_lateral_guard_event(self, payload: Dict[str, Any]) -> None:
        self._append_jsonl(self.lateral_guard_debug_path, payload)

    def _record_reroute_decision_event(self, payload: Dict[str, Any]) -> None:
        self._append_jsonl(self.reroute_decision_debug_path, payload)
        self._append_stage5_debug_jsonl(
            "stage5_reroute_decision_debug",
            self.stage5_reroute_decision_debug_path,
            payload,
        )

    def _should_write_stage5_debug_artifact(self, artifact_name: str) -> bool:
        stride = max(1, int(getattr(self, "stage5_debug_artifact_sample_stride", 1) or 1))
        buffering = self.stats.setdefault("artifact_buffering", {})
        buffering["stage5_debug_artifact_sample_stride"] = stride
        seen_counts = buffering.setdefault("stage5_debug_artifact_seen_counts", {})
        sampled_out_counts = buffering.setdefault("stage5_debug_artifact_sampled_out_counts", {})
        written_counts = buffering.setdefault("stage5_debug_artifact_written_counts", {})
        counters = getattr(self, "_stage5_debug_artifact_sample_counters", None)
        if counters is None:
            counters = Counter()
            self._stage5_debug_artifact_sample_counters = counters
        counters[artifact_name] += 1
        count = int(counters[artifact_name])
        seen_counts[artifact_name] = count
        if stride <= 1 or count == 1 or (count % stride) == 0:
            written_counts[artifact_name] = int(written_counts.get(artifact_name, 0) or 0) + 1
            return True
        sampled_out_counts[artifact_name] = int(sampled_out_counts.get(artifact_name, 0) or 0) + 1
        return False

    def _append_stage5_debug_jsonl(
        self,
        artifact_name: str,
        path: Path,
        payload: Dict[str, Any],
    ) -> None:
        if self._should_write_stage5_debug_artifact(artifact_name):
            self._append_jsonl(path, payload)

    def _should_write_reference_debug_artifact(self, artifact_name: str) -> bool:
        stride = max(1, int(getattr(self, "reference_debug_artifact_sample_stride", 1) or 1))
        buffering = self.stats.setdefault("artifact_buffering", {})
        buffering["reference_debug_artifact_sample_stride"] = stride
        seen_counts = buffering.setdefault("reference_debug_artifact_seen_counts", {})
        sampled_out_counts = buffering.setdefault("reference_debug_artifact_sampled_out_counts", {})
        written_counts = buffering.setdefault("reference_debug_artifact_written_counts", {})
        counters = getattr(self, "_reference_debug_artifact_sample_counters", None)
        if counters is None:
            counters = Counter()
            self._reference_debug_artifact_sample_counters = counters
        counters[artifact_name] += 1
        count = int(counters[artifact_name])
        seen_counts[artifact_name] = count
        if stride <= 1 or count == 1 or (count % stride) == 0:
            written_counts[artifact_name] = int(written_counts.get(artifact_name, 0) or 0) + 1
            return True
        sampled_out_counts[artifact_name] = int(sampled_out_counts.get(artifact_name, 0) or 0) + 1
        return False

    def _append_reference_debug_jsonl(
        self,
        artifact_name: str,
        path: Path,
        payload: Dict[str, Any],
    ) -> None:
        if self._should_write_reference_debug_artifact(artifact_name):
            self._append_jsonl(path, payload)

    def _should_write_control_debug_artifact(self, artifact_name: str) -> bool:
        stride = max(1, int(getattr(self, "control_debug_artifact_sample_stride", 1) or 1))
        buffering = self.stats.setdefault("artifact_buffering", {})
        buffering["control_debug_artifact_sample_stride"] = stride
        seen_counts = buffering.setdefault("control_debug_artifact_seen_counts", {})
        sampled_out_counts = buffering.setdefault("control_debug_artifact_sampled_out_counts", {})
        written_counts = buffering.setdefault("control_debug_artifact_written_counts", {})
        counters = getattr(self, "_control_debug_artifact_sample_counters", None)
        if counters is None:
            counters = Counter()
            self._control_debug_artifact_sample_counters = counters
        counters[artifact_name] += 1
        count = int(counters[artifact_name])
        seen_counts[artifact_name] = count
        if stride <= 1 or count == 1 or (count % stride) == 0:
            written_counts[artifact_name] = int(written_counts.get(artifact_name, 0) or 0) + 1
            return True
        sampled_out_counts[artifact_name] = int(sampled_out_counts.get(artifact_name, 0) or 0) + 1
        return False

    def _append_control_debug_jsonl(
        self,
        artifact_name: str,
        path: Path,
        payload: Dict[str, Any],
    ) -> None:
        if self._should_write_control_debug_artifact(artifact_name):
            self._append_jsonl(path, payload)

    def _should_write_claim_evidence_artifact(self, artifact_name: str) -> bool:
        stride = max(
            1,
            int(getattr(self, "claim_evidence_artifact_sample_stride", 1) or 1),
        )
        buffering = self.stats.setdefault("artifact_buffering", {})
        buffering["claim_evidence_artifact_sample_stride"] = stride
        seen_counts = buffering.setdefault("claim_evidence_artifact_seen_counts", {})
        sampled_out_counts = buffering.setdefault(
            "claim_evidence_artifact_sampled_out_counts", {}
        )
        written_counts = buffering.setdefault("claim_evidence_artifact_written_counts", {})
        counters = getattr(self, "_claim_evidence_artifact_sample_counters", None)
        if counters is None:
            counters = Counter()
            self._claim_evidence_artifact_sample_counters = counters
        counters[artifact_name] += 1
        count = int(counters[artifact_name])
        seen_counts[artifact_name] = count
        if stride <= 1 or count == 1 or (count % stride) == 0:
            written_counts[artifact_name] = int(written_counts.get(artifact_name, 0) or 0) + 1
            return True
        sampled_out_counts[artifact_name] = int(sampled_out_counts.get(artifact_name, 0) or 0) + 1
        return False

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
        apollo_warmup_bypassed_by_readiness: Optional[bool] = None,
        apollo_warmup_readiness: Optional[Dict[str, Any]] = None,
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
        if apollo_warmup_bypassed_by_readiness is not None:
            state["apollo_warmup_bypassed_by_readiness"] = bool(apollo_warmup_bypassed_by_readiness)
        if apollo_warmup_readiness is not None:
            state["apollo_warmup_readiness"] = dict(apollo_warmup_readiness)
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
        max_speed_mps = _finite_or_none(direct_stats.get("control_apply_max_speed_mps"))
        if max_speed_mps is None:
            max_speed_mps = _finite_or_none(getattr(self, "_max_speed_mps", None))
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
            reason = "planning_message_pending"
        elif planning_nonempty_count <= 0:
            stage = "planning_empty_only"
            layer = "planning_materialization"
            reason = "planning_trajectory_empty"
        elif control_rx_count <= 0:
            stage = "planning_ready_no_control_rx"
            layer = "control_materialization"
            reason = "control_message_pending"
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
        elif (
            speed_mps is not None
            and speed_mps <= 0.5
            and (max_speed_mps is None or max_speed_mps <= 0.5)
        ):
            stage = "control_applied_no_motion"
            layer = "actuation"
            reason = "vehicle_speed_not_rising"
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
                "routing_response_relay_writer_ready": bool(self.routing_response_writer is not None),
                "lane_follow_client_ready": bool(self.lane_follow_client is not None),
                "action_client_ready": bool(self.action_client is not None),
                "routing_request_path_enabled": bool(self.auto_routing_send_routing),
                "raw_routing_response_channel": self.raw_routing_response_channel,
                "planning_routing_response_channel": self.routing_response_channel,
                "raw_routing_response_relay_enabled": bool(
                    self.auto_routing_relay_raw_routing_response_to_planning
                    and self.raw_routing_response_channel != self.routing_response_channel
                ),
                "lane_follow_path_enabled": bool(self.auto_routing_send_lane_follow),
                "action_path_enabled": bool(self.auto_routing_send_action),
                "lane_follow_disabled_runtime": bool(self._lane_follow_disabled_runtime),
            },
            "gate_state": gate,
            "observed_counters": {
                "routing_request_count": routing_request_count,
                "routing_response_count": routing_response_count,
                "raw_routing_response_count": int(self.stats.get("raw_routing_response_count", 0) or 0),
                "planning_routing_response_count": int(
                    self.stats.get("planning_routing_response_count", 0) or 0
                ),
                "routing_response_relay_count": int(
                    self.stats.get("routing_response_relay_count", 0) or 0
                ),
                "routing_response_relay_error_count": int(
                    self.stats.get("routing_response_relay_error_count", 0) or 0
                ),
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
            "speed_mps_max": max_speed_mps,
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
                "stale_world_frame_policy": str(
                    direct_stats.get("stale_world_frame_policy")
                    or self.stats.get("direct_stale_world_frame_policy")
                    or ""
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
            "fallback_blocked_by_claim_profile": bool(
                validity.get("fallback_blocked_by_claim_profile", False)
            ),
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

    def _claim_profile_blocks_route_goal(self, goal_meta: Dict[str, Any]) -> bool:
        if not bool(getattr(self, "claim_profile_enabled", False)):
            return False
        goal_source = str(goal_meta.get("goal_source", "") or "").strip()
        goal_mode = str(goal_meta.get("goal_mode", "") or "").strip()
        forbidden_sources = {
            "startup_short_ahead",
            "invalid_goal_fallback_ahead",
            "scenario_goal_missing_fallback",
            "fixed_goal_missing_fallback",
            "long_ahead_fallback",
        }
        if goal_source in forbidden_sources:
            return True
        if goal_mode == "ego_seed_ahead":
            return True
        return False

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
        post_routing_reference_evidence_available = bool(
            self.auto_routing_established
            or reference_line_count > 0
            or route_segment_count > 0
            or routing_lane_window_count > 0
        )
        invalid_goal_reason = ""
        if self.auto_routing_goal_validity_check_enabled:
            if requested_mode in {"scenario_xy", "fixed_xy"} and self.auto_routing_snap_goal_to_lane and (not goal_projection_available):
                invalid_goal_reason = "goal_projection_unavailable"
            elif recent_fresh and dest_beyond_reference_line is True:
                invalid_goal_reason = "dest_beyond_reference_line_risk"
            elif (
                recent_fresh
                and reference_line_count <= 0
                and post_routing_reference_evidence_available
                and phase == "long"
                and not reference_line_debug_missing_but_trajectory_nonzero
            ):
                invalid_goal_reason = "reference_line_unavailable"
            elif (
                recent_fresh
                and route_segment_count <= 0
                and post_routing_reference_evidence_available
                and phase == "long"
            ):
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
            "post_routing_reference_evidence_available": post_routing_reference_evidence_available,
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
        phase_route_kind = "claim_route" if phase == "claim" else "long_phase_route"
        phase_trigger = "claim_route_policy" if phase == "claim" else "auto_routing_long_phase"
        if routing_skipped_due_to_freeze:
            return phase_route_kind, "freeze_after_success_skip", "freeze_after_success_guard"
        if routing_skipped_due_to_invalid_goal:
            reason = "claim_route_invalid_goal_skip" if phase == "claim" else "long_phase_invalid_goal_skip"
            return phase_route_kind, reason, "goal_validity_guard"
        if routing_waiting_for_route_debug_ready:
            return phase_route_kind, "long_phase_wait_route_debug_ready", "route_debug_readiness_guard"
        if routing_skipped_due_to_unstable_reference_line:
            return (
                phase_route_kind,
                "long_phase_unstable_reference_line_skip",
                "reference_line_guard",
            )
        if ignore_roll_active:
            return "explicit_reroute", "traffic_light_ignore_roll_refresh", "traffic_light_ignore_roll"
        if phase == "startup":
            if int(self.auto_routing_routing_sent) == 0:
                return "startup_route", "startup_initial_route", "auto_routing_startup"
            return "startup_route", "startup_route_refresh", "auto_routing_startup"
        if phase == "claim":
            if not self.auto_routing_long_routing_sent:
                return "claim_route", "claim_initial_route", "claim_route_policy"
            return "claim_route", "claim_route_refresh", "claim_route_policy"
        if not self.auto_routing_long_routing_sent:
            return "long_phase_route", "long_phase_transition", phase_trigger
        return "explicit_reroute", "long_phase_refresh", phase_trigger

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

        # P0-1 fix (2026-06-30): path fallback when reference_line is empty.
        # When enable_reference_line_stitching=false, Apollo does not populate
        # debug.planning_data.reference_line but does populate path items with
        # path_points containing x, y, theta, kappa geometry.
        if not out:
            planning_data = self._resolve_nested_value(msg, (("debug", "planning_data",),))
            if planning_data is not None:
                try:
                    path_items = getattr(planning_data, "path", None)
                    if path_items is not None:
                        for path_item in list(path_items or [])[:6]:
                            pts = getattr(path_item, "path_point", None)
                            if pts is None:
                                continue
                            total = 0.0
                            prev_x = prev_y = None
                            for pt in list(pts):
                                x = self._resolve_nested_float(pt, (("x",),))
                                y = self._resolve_nested_float(pt, (("y",),))
                                if x is None or y is None:
                                    continue
                                if prev_x is not None and prev_y is not None:
                                    total += math.hypot(x - prev_x, y - prev_y)
                                prev_x, prev_y = x, y
                            if total > 0.0:
                                out.append(total)
                except Exception:
                    pass
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

    def _routing_response_lane_windows(self, msg: Any) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        roads = getattr(msg, "road", None)
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
                        window_signature = self._routing_lane_window_signature(
                            lane_id,
                            start_s,
                            end_s,
                        )
                        rows.append(
                            {
                                "road_index": int(road_index),
                                "road_id": road_id,
                                "passage_index": int(passage_index),
                                "segment_index": int(segment_index),
                                "lane_id": lane_id,
                                "start_s": start_s,
                                "end_s": end_s,
                                "length_m": length,
                                "lane_window_signature": window_signature,
                            }
                        )
        except Exception:
            return rows
        return rows

    def _routing_lane_window_signature(
        self,
        lane_id: Any,
        start_s: Optional[float],
        end_s: Optional[float],
    ) -> str:
        lane = str(lane_id or "").strip()

        def _fmt(value: Optional[float]) -> str:
            if value is None:
                return ""
            try:
                out = float(value)
            except Exception:
                return ""
            if not math.isfinite(out):
                return ""
            return f"{out:.4f}"

        return f"{lane}@{_fmt(start_s)}->{_fmt(end_s)}" if lane else f"@{_fmt(start_s)}->{_fmt(end_s)}"

    def _routing_response_decoded_payload(
        self,
        msg: Any,
        *,
        now_sec: float,
        source_channel: str = "/apollo/routing_response",
    ) -> Dict[str, Any]:
        lane_segments = self._routing_response_lane_windows(msg)
        road_segments: List[Dict[str, Any]] = []
        roads = getattr(msg, "road", None)
        try:
            for road_index, road in enumerate(list(roads or [])):
                road_id = self._proto_scalar(self._resolve_nested_value(road, (("id",),)))
                road_payload: Dict[str, Any] = {"road_index": int(road_index), "road_id": road_id, "passages": []}
                passages = getattr(road, "passage", None)
                for passage_index, passage in enumerate(list(passages or [])):
                    passage_payload: Dict[str, Any] = {
                        "passage_index": int(passage_index),
                        "segments": [],
                    }
                    segments = getattr(passage, "segment", None)
                    for segment_index, segment in enumerate(list(segments or [])):
                        lane_id = self._proto_scalar(self._resolve_nested_value(segment, (("id",),)))
                        start_s = self._resolve_nested_float(segment, (("start_s",),))
                        end_s = self._resolve_nested_float(segment, (("end_s",),))
                        length = self._resolve_nested_float(segment, (("length",),))
                        if length is None and start_s is not None and end_s is not None:
                            length = max(0.0, float(end_s) - float(start_s))
                        passage_payload["segments"].append(
                            {
                                "segment_index": int(segment_index),
                                "lane_id": lane_id,
                                "start_s": start_s,
                                "end_s": end_s,
                                "length_m": length,
                            }
                        )
                    road_payload["passages"].append(passage_payload)
                road_segments.append(road_payload)
        except Exception:
            road_segments = []
        total_length = 0.0
        has_length = False
        lane_sequence: List[str] = []
        lane_window_sequence: List[str] = []
        seen_lanes = set()
        for row in lane_segments:
            length = row.get("length_m")
            if length is not None:
                try:
                    if math.isfinite(float(length)):
                        total_length += float(length)
                        has_length = True
                except Exception:
                    pass
            lane_id = str(row.get("lane_id") or "").strip()
            window_signature = str(row.get("lane_window_signature") or "").strip()
            if window_signature:
                lane_window_sequence.append(window_signature)
            if lane_id and lane_id not in seen_lanes:
                seen_lanes.add(lane_id)
                lane_sequence.append(lane_id)
        lane_window_signature = " | ".join(lane_window_sequence)
        unique_lane_signature = " | ".join(lane_sequence)
        return {
            "schema_version": "routing_response_decoded.v1",
            "source": source_channel,
            "planning_facing_channel": self.routing_response_channel,
            "raw_routing_response_channel": self.raw_routing_response_channel,
            "timestamp_sec": _finite_or_none(now_sec),
            "header_timestamp_sec": _finite_or_none(self._header_timestamp_sec(msg)),
            "response_sequence": self._header_sequence_num(msg),
            "road_count": len(list(roads or [])) if roads is not None else 0,
            "passage_count": sum(len(road.get("passages") or []) for road in road_segments),
            "lane_segment_count": len(lane_segments),
            "road_segments": road_segments,
            "lane_segments": lane_segments,
            "total_length_m": total_length if has_length else None,
            "lane_sequence_signature": lane_sequence,
            "lane_window_signature": lane_window_signature,
            "unique_lane_signature": unique_lane_signature,
            "claim_boundary": (
                "This artifact decodes a real Apollo RoutingResponse observed on the "
                "reported source channel. "
                "It does not by itself prove route identity, HDMap projection, Planning "
                "materialization, or Control handoff."
            ),
        }

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
        recent_events = list(self._planning_recent_events)

        def _recent_bool_ratio(key: str) -> Optional[float]:
            values = [event.get(key) for event in recent_events if event.get(key) is not None]
            if not values:
                return None
            return sum(1 for value in values if bool(value)) / float(len(values))

        def _recent_nonzero_ratio(key: str) -> Optional[float]:
            values: List[float] = []
            for event in recent_events:
                value = event.get(key)
                if value is None:
                    continue
                try:
                    number = float(value)
                except Exception:
                    continue
                if math.isfinite(number):
                    values.append(number)
            if not values:
                return None
            return sum(1 for value in values if value > 0.0) / float(len(values))

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
            "planning_debug_presence": {
                "last_diagnosis": last_event.get("planning_debug_diagnosis"),
                "last_reference_line_path": last_event.get("planning_debug_reference_line_path"),
                "last_routing_path": last_event.get("planning_debug_routing_path"),
                "last_planning_data_present": last_event.get(
                    "planning_debug_planning_data_present"
                ),
                "last_reference_line_field_present": last_event.get(
                    "planning_debug_reference_line_field_present"
                ),
                "last_reference_line_count": last_event.get("planning_debug_reference_line_count"),
                "last_routing_segment_count": last_event.get(
                    "planning_debug_routing_segment_count"
                ),
                "last_field_inventory": last_event.get("planning_debug_field_inventory"),
                "last_path_candidate_summary": last_event.get(
                    "planning_debug_path_candidate_summary"
                ),
                "planning_data_present_ratio": _recent_bool_ratio(
                    "planning_debug_planning_data_present"
                ),
                "reference_line_field_present_ratio": _recent_bool_ratio(
                    "planning_debug_reference_line_field_present"
                ),
                "reference_line_nonempty_ratio": _recent_nonzero_ratio(
                    "planning_debug_reference_line_count"
                ),
                "routing_field_present_ratio": _recent_bool_ratio(
                    "planning_debug_routing_field_present"
                ),
                "routing_segment_nonempty_ratio": _recent_nonzero_ratio(
                    "planning_debug_routing_segment_count"
                ),
            },
            "last_planning_header_sequence_num": (
                int(last_event.get("planning_header_sequence_num"))
                if last_event.get("planning_header_sequence_num") is not None
                else None
            ),
            "planning_stall": {
                "consecutive_empty_count": int(self._planning_stall_consecutive_empty),
                "first_empty_ts_sec": self._planning_stall_first_empty_ts_sec,
                "max_consecutive_empty_reported": int(self._planning_stall_last_reported),
            },
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
            "color_source": self._traffic_light_last_color_source,
            "contain_lights": self._traffic_light_last_contain_lights,
            "last_entries": [
                {"signal_id": signal_id, "color": color}
                for signal_id, color in self._traffic_light_last_entries
            ],
            "claim_grade_ready": bool(
                self.traffic_light_policy == "carla_actual"
                and self._traffic_light_last_color_source in {"carla_actor_state", "carla_traffic_light_actor_state"}
                and self._traffic_light_last_contain_lights
                and self.traffic_light_writer is not None
            ),
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
            "actor_probe_enabled": self.front_obstacle_actor_probe_enabled,
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
            raw_routing_response_channel=self.raw_routing_response_channel,
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
            "routing_response_count": self.stats.get("routing_response_count", 0),
            "raw_routing_response_count": self.stats.get("raw_routing_response_count", 0),
            "planning_routing_response_count": self.stats.get("planning_routing_response_count", 0),
            "routing_response_relay_count": self.stats.get("routing_response_relay_count", 0),
            "routing_response_relay_error_count": self.stats.get("routing_response_relay_error_count", 0),
            "raw_routing_response_channel": self.raw_routing_response_channel,
            "planning_routing_response_channel": self.routing_response_channel,
            "raw_routing_response_relay_enabled": bool(
                self.auto_routing_relay_raw_routing_response_to_planning
                and self.raw_routing_response_channel != self.routing_response_channel
            ),
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
            "trajectory_kappa": {"count": 0, "min": None, "max": None, "max_abs": None, "p95_abs": None},
            "trajectory_theta_delta_abs": {"count": 0, "min": None, "max": None, "max_abs": None, "p95_abs": None},
            "trajectory_xy_step_m": {"count": 0, "min": None, "max": None, "max_abs": None, "p95_abs": None},
            "trajectory_kappa_spike_count_abs_ge_0_05": 0,
            "trajectory_kappa_spike_count_abs_ge_0_10": 0,
            "trajectory_first_segment_heading": None,
            "trajectory_first_theta_minus_first_segment_heading_rad": None,
            "trajectory_expired_prefix_count": None,
            "trajectory_first_nonexpired_point_index": None,
            "trajectory_first_nonexpired_point_relative_time": None,
            "trajectory_first_nonexpired_point_theta": None,
            "trajectory_first_nonexpired_remaining_point_count": None,
            "trajectory_future_first_segment_heading": None,
            "trajectory_first_nonexpired_theta_minus_future_segment_heading_rad": None,
            "trajectory_future_lookahead_heading": None,
            "trajectory_future_lookahead_point_index": None,
            "trajectory_future_lookahead_distance_m": None,
            "trajectory_future_lookahead_time_s": None,
            "trajectory_first_nonexpired_theta_minus_future_lookahead_heading_rad": None,
            "trajectory_sample_points": [],
            "trajectory_future_window_points": [],
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
            "planning_debug_debug_present": None,
            "planning_debug_planning_data_present": None,
            "planning_debug_reference_line_path": None,
            "planning_debug_reference_line_field_present": None,
            "planning_debug_reference_line_count": None,
            "planning_debug_reference_line_lengths": [],
            "planning_debug_path_fallback_used": None,
            "planning_debug_path_item_count": None,
            "planning_debug_path_point_total": None,
            "planning_debug_routing_path": None,
            "planning_debug_routing_field_present": None,
            "planning_debug_routing_road_count": None,
            "planning_debug_routing_passage_count": None,
            "planning_debug_routing_segment_count": None,
            "planning_debug_diagnosis": None,
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
            "planning_empty_reason_detail": {},
            "planning_debug_debug_present": None,
            "planning_debug_planning_data_present": None,
            "planning_debug_reference_line_path": None,
            "planning_debug_reference_line_field_present": None,
            "planning_debug_reference_line_count": None,
            "planning_debug_reference_line_lengths": [],
            "planning_debug_path_fallback_used": None,
            "planning_debug_path_item_count": None,
            "planning_debug_path_point_total": None,
            "planning_debug_routing_path": None,
            "planning_debug_routing_field_present": None,
            "planning_debug_routing_road_count": None,
            "planning_debug_routing_passage_count": None,
            "planning_debug_routing_segment_count": None,
            "planning_debug_diagnosis": None,
        }
        try:
            planning_debug_presence = build_planning_debug_presence_impl(msg)
            debug_row.update(planning_debug_presence)
            route_debug_row.update(planning_debug_presence)
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
            # -----------------------------------------------------------------
            # Planning stall detection (2026-06-30)
            # Track consecutive empty trajectories. When >50 consecutive empty
            # messages are seen with routing present but no reference_line,
            # emit a detailed stall diagnostic.
            # -----------------------------------------------------------------
            if pt_count > 0:
                self._planning_stall_consecutive_empty = 0
                self._planning_stall_first_empty_ts_sec = None
            else:
                self._planning_stall_consecutive_empty += 1
                if self._planning_stall_first_empty_ts_sec is None:
                    self._planning_stall_first_empty_ts_sec = now_sec
                stall_threshold = 50
                if (
                    self._planning_stall_consecutive_empty >= stall_threshold
                    and self._planning_stall_consecutive_empty > self._planning_stall_last_reported
                ):
                    self._planning_stall_last_reported = self._planning_stall_consecutive_empty
                    stall_dur = (
                        now_sec - float(self._planning_stall_first_empty_ts_sec)
                        if self._planning_stall_first_empty_ts_sec is not None
                        else 0.0
                    )
                    print(
                        f"[bridge][planning][ERROR] PLANNING_STALL_DETECTED "
                        f"consecutive_empty={self._planning_stall_consecutive_empty} "
                        f"stall_duration_sec={stall_dur:.1f} "
                        f"routing_segment_count={routing_segment_count} "
                        f"reference_line_count={len(ref_lengths)} "
                        f"routing_established={self.auto_routing_established} "
                        f"map_contract_invalid={self.map_contract_invalid} "
                        f"suppress_unstable_reroute={self.auto_routing_suppress_long_phase_reroute_on_unstable_reference_line} "
                        f"current_routing_phase={self.auto_routing_current_phase}"
                    )
            trajectory_shape_debug = build_trajectory_shape_debug_impl(pts)
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
                    **trajectory_shape_debug,
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
            # -----------------------------------------------------------------
            # Planning empty trajectory diagnosis (enhanced 2026-06-30)
            # Causal chain for reference_line_missing:
            #   1. map_contract_invalid → route debug missing/stale
            #   2. suppress_long_phase_reroute_on_unstable_reference_line
            #      blocks long-phase routing when map_contract_invalid=true
            #   3. Apollo stuck with startup 30m route only
            #   4. Planning reaches end of short reference line → empty trajectory
            #   5. Control enters safe hold → vehicle drifts → CTE grows
            # -----------------------------------------------------------------
            empty_reason_guess = ""
            empty_reason_detail = {}
            if pt_count <= 0:
                if len(ref_lengths) <= 0 and routing_segment_count <= 0:
                    empty_reason_guess = "reference_line_and_routing_empty"
                    empty_reason_detail = {
                        "ref_lengths_count": len(ref_lengths),
                        "routing_segment_count": routing_segment_count,
                        "routing_road_count": routing_road_count,
                        "planning_data_present": planning_debug_presence.get(
                            "planning_debug_planning_data_present"
                        ),
                        "reference_line_field_present": planning_debug_presence.get(
                            "planning_debug_reference_line_field_present"
                        ),
                        "routing_field_present": planning_debug_presence.get(
                            "planning_debug_routing_field_present"
                        ),
                        "map_contract_invalid": bool(self.map_contract_invalid),
                        "routing_established": bool(self.auto_routing_established),
                        "suppress_unstable_reroute": bool(
                            self.auto_routing_suppress_long_phase_reroute_on_unstable_reference_line
                        ),
                        "current_routing_phase": str(
                            self.auto_routing_current_phase or ""
                        ),
                    }
                elif len(ref_lengths) <= 0:
                    empty_reason_guess = "reference_line_missing"
                    empty_reason_detail = {
                        "ref_lengths_count": 0,
                        "routing_segment_count": routing_segment_count,
                        "map_contract_invalid": bool(self.map_contract_invalid),
                    }
                elif routing_segment_count <= 0:
                    empty_reason_guess = "route_segment_missing"
                    empty_reason_detail = {
                        "routing_segment_count": 0,
                        "ref_lengths_count": len(ref_lengths),
                    }
                elif dest_beyond_reference_line is True:
                    empty_reason_guess = "dest_beyond_reference_line"
                    empty_reason_detail = {
                        "ref_len_max": ref_len_max,
                        "remain_length_to_dest": remain_length_to_dest,
                        "ego_front_to_center": ego_front_to_center,
                    }
                else:
                    empty_reason_guess = "zero_trajectory_points_unknown"
                    empty_reason_detail = {
                        "ref_lengths_count": len(ref_lengths),
                        "routing_segment_count": routing_segment_count,
                        "pt_count": int(pt_count),
                    }
            # Log empty trajectory with detail at WARNING level every 50th event
            if pt_count <= 0 and self._planning_empty_count > 0 and self._planning_empty_count % 50 == 0:
                print(
                    f"[bridge][planning][warn] empty_trajectory_#{self._planning_empty_count} "
                    f"reason={empty_reason_guess} detail={empty_reason_detail} "
                    f"nonempty_ratio={self._planning_nonempty_count}/{self._planning_msg_count}"
                )
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
                    "planning_empty_reason_detail": empty_reason_detail,
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
        self._append_reference_debug_jsonl(
            "planning_route_segment_debug",
            self.planning_route_segment_debug_path,
            route_debug_row,
        )
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
        self._append_reference_debug_jsonl(
            "apollo_map_runtime_debug",
            self.apollo_map_runtime_debug_path,
            map_runtime_row,
        )
        self._append_stage5_debug_jsonl(
            "stage5_apollo_map_runtime_debug",
            self.stage5_apollo_map_runtime_debug_path,
            map_runtime_row,
        )
        self._append_reference_debug_jsonl(
            "apollo_reference_line_debug",
            self.apollo_reference_line_debug_path,
            enriched_route_debug,
        )
        self._append_reference_debug_jsonl(
            "apollo_route_segment_debug",
            self.apollo_route_segment_debug_path,
            enriched_route_debug,
        )
        self._append_stage5_debug_jsonl(
            "stage5_apollo_reference_line_debug",
            self.stage5_apollo_reference_line_debug_path,
            enriched_route_debug,
        )
        self._append_stage5_debug_jsonl(
            "stage5_apollo_route_segment_debug",
            self.stage5_apollo_route_segment_debug_path,
            enriched_route_debug,
        )
        self._append_stage5_debug_jsonl(
            "stage5_apollo_lane_follow_map_debug",
            self.stage5_apollo_lane_follow_map_debug_path,
            enriched_route_debug,
        )
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
                color_source = "forced_green"
            else:
                entries = self._current_carla_actual_traffic_lights()
                color_source = "carla_actor_state"
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
            self._record_topic_publish_stats(
                channel=self.traffic_light_channel,
                msg=msg,
                sim_time_sec=ts_sec,
                payload_count=len(entries),
                source="bridge_writer",
                extra={
                    "light_count": len(entries),
                    "contain_lights": bool(entries),
                    "color_source": color_source,
                    "traffic_light_policy": self.traffic_light_policy,
                    "empty_message": not bool(entries),
                },
            )
            self._last_traffic_light_publish_ts = ts_sec
            self._traffic_light_last_publish_ts = ts_sec
            self._traffic_light_publish_count += 1
            self._traffic_light_last_entries = list(entries)
            self._traffic_light_last_contain_lights = bool(entries)
            self._traffic_light_last_color_source = color_source
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
            "localization_back_offset_source": self.localization_back_offset_source,
            "localization_back_offset_resolve_error": self.localization_back_offset_resolve_error,
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

        def capture_trajectory_point(prefix: str, point: Any) -> None:
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
                "station_error_limited",
                "preview_station_error",
                "speed_reference",
                "speed_error",
                "speed_controller_input_limited",
                "preview_speed_reference",
                "preview_speed_error",
                "preview_acceleration_reference",
                "acceleration_cmd_closeloop",
                "acceleration_cmd",
                "acceleration_lookup",
                "speed_lookup",
                "calibration_value",
                "throttle_cmd",
                "brake_cmd",
                "is_full_stop",
                "slope_offset_compensation",
                "current_station",
                "path_remain",
                "acceleration_reference",
                "current_acceleration",
                "acceleration_error",
                "jerk_reference",
                "current_jerk",
                "jerk_error",
                "pid_saturation_status",
                "leadlag_saturation_status",
                "speed_offset",
                "current_speed",
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
            for prefix, field in (
                ("debug_simple_lon_current_matched_point", "current_matched_point"),
                ("debug_simple_lon_current_reference_point", "current_reference_point"),
                ("debug_simple_lon_preview_reference_point", "preview_reference_point"),
            ):
                capture_trajectory_point(prefix, getattr(lon_debug, field, None))
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
                "steer_angle_feedforward",
                "steer_angle_lateral_contribution",
                "steer_angle_lateral_rate_contribution",
                "steer_angle_heading_contribution",
                "steer_angle_heading_rate_contribution",
                "steer_angle_feedback",
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
                capture_trajectory_point(prefix, getattr(lat_debug, field, None))
        mpc_debug = getattr(debug_msg, "simple_mpc_debug", None) if debug_msg is not None else None
        if mpc_debug is not None:
            for prefix, field in (
                ("debug_simple_mpc_current_matched_point", "current_matched_point"),
                ("debug_simple_mpc_current_reference_point", "current_reference_point"),
                ("debug_simple_mpc_preview_reference_point", "preview_reference_point"),
            ):
                capture_trajectory_point(prefix, getattr(mpc_debug, field, None))
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
            percent_normalization_mode=getattr(
                self,
                "steering_percent_normalization",
                STEERING_NORMALIZATION_SINGLE_PERCENT_AT_SELECT,
            ),
            max_steer_angle_deg=getattr(self, "physical_apollo_max_steer_angle_deg", 30.0),
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
            throttle_brake_mutual_exclusion_enabled=bool(self.throttle_brake_mutual_exclusion_enabled),
            throttle_brake_hysteresis_frames=int(self.throttle_brake_hysteresis_frames),
            throttle_brake_min_command=float(self.throttle_brake_min_command),
            physical_allow_legacy_fallback=bool(self.physical_allow_legacy_fallback),
            physical_apollo_max_steer_angle_deg=float(self.physical_apollo_max_steer_angle_deg),
            physical_apollo_max_accel_mps2=float(self.physical_apollo_max_accel_mps2),
            physical_apollo_max_decel_mps2=float(self.physical_apollo_max_decel_mps2),
            physical_use_top_level_acceleration=bool(self.physical_use_top_level_acceleration),
            physical_use_lon_debug=bool(self.physical_use_lon_debug),
            physical_steer_field_priority=tuple(self.physical_steer_field_priority),
            physical_acceleration_field_priority=tuple(self.physical_acceleration_field_priority),
            physical_map_steering=bool(self.physical_map_steering),
            physical_map_longitudinal=bool(self.physical_map_longitudinal),
            physical_map_throttle=bool(self.physical_map_throttle),
            physical_map_brake=bool(self.physical_map_brake),
        )

    def _allow_control_publish_after_startup(
        self,
        *,
        planning_valid: bool,
        planning_reason: str,
        timestamp_sec: float,
    ) -> bool:
        """Latch CARLA publication open after the first valid Planning trajectory."""

        enabled = bool(getattr(self, "require_valid_planning_before_first_publish", False))
        gate_open = bool(getattr(self, "_valid_planning_publish_gate_open", not enabled))
        gate = self.stats.setdefault(
            "control_startup_publish_gate",
            {
                "enabled": enabled,
                "open": gate_open,
                "skip_count": 0,
                "first_open_timestamp_sec": None,
                "last_skip_reason": "",
                "claim_boundary": (
                    "startup_handoff_only; raw Apollo Control remains recorded and the gate never "
                    "closes after the first valid Planning trajectory"
                ),
            },
        )
        if not enabled or gate_open:
            gate["open"] = True
            return True
        if planning_valid:
            self._valid_planning_publish_gate_open = True
            gate["open"] = True
            gate["first_open_timestamp_sec"] = float(timestamp_sec)
            gate["last_skip_reason"] = ""
            return True
        gate["skip_count"] = int(gate.get("skip_count", 0) or 0) + 1
        gate["last_skip_reason"] = str(planning_reason or "planning_not_valid")
        return False

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
            probe["accepted"] = False
            probe["rejected"] = False
            probe["applied"] = False
            return x, y, probe
        trusted_source = bool(probe.get("trusted_lane_centerline", False))
        accepted = bool(trusted_source or self.auto_routing_snap_allow_untrusted_source)
        probe["accepted"] = accepted
        probe["rejected"] = not accepted
        probe["source_trusted_lane_centerline"] = trusted_source
        if not accepted:
            probe["reject_reason"] = "untrusted_snap_source"
            probe["applied"] = False
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
        if not self._should_write_claim_evidence_artifact("debug_timeseries"):
            return
        self._write_csv_row(self.debug_csv_path, row)
        self._debug_csv_header_written = True

    def _record_apollo_reference_line_contract_event(self, row: Dict[str, Any]) -> None:
        if not self._should_write_claim_evidence_artifact("apollo_reference_line_contract"):
            return
        payload: Dict[str, Any] = {}
        if isinstance(getattr(self, "_planning_last_event", None), dict):
            payload.update(self._planning_last_event)
        if isinstance(getattr(self, "_planning_last_route_debug_event", None), dict):
            payload.update(self._planning_last_route_debug_event)
        payload.update(row)
        try:
            event = build_reference_line_contract_event(
                payload,
                source_confidence="bridge_debug_timeseries",
            )
            self._append_jsonl(self.apollo_reference_line_contract_path, event)
        except Exception as exc:
            self.stats["apollo_reference_line_contract_write_error"] = f"{type(exc).__name__}:{exc}"

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
        wall_time_sec = self._command_now_sec()
        header_ts_sec, localization_time_base = self._localization_time_from_odom(
            odom,
            fallback_wall_time_sec=wall_time_sec,
        )
        self._record_timing_from_odom(
            odom,
            wall_time_sec=wall_time_sec,
            direct_world_frame=direct_world_frame,
        )
        sequence_num = self._fill_header(
            getattr(loc, "header", None), header_ts_sec, "tb_apollo10_gt_bridge", frame_id="map"
        )

        pos = odom.pose.pose.position
        ori = odom.pose.pose.orientation
        vel = odom.twist.twist.linear
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
        wx_raw, wy_raw, wz_raw = _odom_angular_velocity_rad_per_s(odom)
        wx, wy, wz = self.tf.apply_vector(wx_raw, wy_raw, wz_raw)
        yaw_ap = pose_info["map_yaw"]
        c_yaw = math.cos(yaw_ap)
        s_yaw = math.sin(yaw_ap)
        velocity_right_vrf = s_yaw * vx - c_yaw * vy
        velocity_forward_vrf = c_yaw * vx + s_yaw * vy
        angular_right_vrf = s_yaw * wx - c_yaw * wy
        angular_forward_vrf = c_yaw * wx + s_yaw * wy
        ax, ay, az, acceleration_source = _localization_acceleration_from_velocity(
            getattr(self, "_last_localization_velocity_sample", None),
            header_ts_sec,
            vx,
            vy,
            vz,
            previous_acceleration=(
                getattr(self, "_last_localization_acceleration_sample", None)
                if getattr(self, "localization_acceleration_filter_enabled", False)
                else None
            ),
            smoothing_alpha=(
                getattr(self, "localization_acceleration_filter_alpha", 1.0)
                if getattr(self, "localization_acceleration_filter_enabled", False)
                else 1.0
            ),
            max_abs_mps2=(
                getattr(self, "localization_acceleration_filter_max_abs_mps2", None)
                if getattr(self, "localization_acceleration_filter_enabled", False)
                else None
            ),
            max_delta_mps2=(
                getattr(self, "localization_acceleration_filter_max_delta_mps2", None)
                if getattr(self, "localization_acceleration_filter_enabled", False)
                else None
            ),
        )
        acceleration_right_vrf = s_yaw * ax - c_yaw * ay
        acceleration_forward_vrf = c_yaw * ax + s_yaw * ay
        acceleration_forward_vrf_unconstrained = acceleration_forward_vrf
        localization_speed_mps = math.sqrt(vx * vx + vy * vy + vz * vz)
        (
            acceleration_forward_vrf,
            acceleration_nonnegative_speed_prediction_limited,
            acceleration_nonnegative_speed_prediction_min_mps2,
        ) = _limit_forward_acceleration_for_nonnegative_speed_prediction(
            localization_speed_mps,
            acceleration_forward_vrf,
            getattr(
                self,
                "localization_acceleration_nonnegative_speed_prediction_horizon_s",
                0.0,
            ),
        )
        if acceleration_nonnegative_speed_prediction_limited:
            # Preserve the lateral/right component and only repair the
            # longitudinal GT state that Apollo's replan model consumes.
            ax = s_yaw * acceleration_right_vrf + c_yaw * acceleration_forward_vrf
            ay = -c_yaw * acceleration_right_vrf + s_yaw * acceleration_forward_vrf
            acceleration_source = (
                f"{acceleration_source}_nonnegative_speed_prediction_limited"
            )
            self._localization_acceleration_nonnegative_speed_correction_count += 1
            self.stats["localization_acceleration_filter"][
                "nonnegative_speed_correction_count"
            ] = self._localization_acceleration_nonnegative_speed_correction_count
        if acceleration_source != "stale_timestamp_republish":
            self._last_localization_velocity_sample = (header_ts_sec, vx, vy, vz)
            self._last_localization_acceleration_sample = (ax, ay, az)
        angular_velocity_source = (
            "carla_direct_odom_twist" if self.transport_mode == "carla_direct" else "ros2_odom_twist"
        )
        localization_payload = build_localization_estimate_dict_from_map_state(
            timestamp_sec=header_ts_sec,
            sequence_num=sequence_num,
            position={"x": x, "y": y, "z": z},
            heading=yaw_ap,
            linear_velocity={"x": vx, "y": vy, "z": vz},
            linear_velocity_vrf={"x": velocity_right_vrf, "y": velocity_forward_vrf, "z": vz},
            angular_velocity={"x": wx, "y": wy, "z": wz},
            angular_velocity_vrf={"x": angular_right_vrf, "y": angular_forward_vrf, "z": wz},
            linear_acceleration={"x": ax, "y": ay, "z": az},
            linear_acceleration_vrf={"x": acceleration_right_vrf, "y": acceleration_forward_vrf, "z": az},
            module_name="tb_apollo10_gt_bridge",
            frame_id="map",
            time_base=localization_time_base,
            heading_source="odom_quaternion_yaw_after_frame_transform",
            position_reference="rear_axle_center",
            vehicle_reference_confidence=self.vehicle_reference_confidence,
            vehicle_reference_hard_gate_eligible=self.vehicle_reference_hard_gate_eligible,
            metadata={
                "carla_frame_id": direct_world_frame,
                "localization_reference_mode": self.localization_reference_mode,
                "localization_back_offset_m": float(self.localization_back_offset_m),
                "localization_back_offset_source": self.localization_back_offset_source,
                "localization_back_offset_resolve_error": self.localization_back_offset_resolve_error,
                "apollo_control_state_reference": self.apollo_control_state_reference,
                "angular_velocity_source": angular_velocity_source,
                "acceleration_source": acceleration_source,
                "acceleration_semantics": "finite_difference_or_physical",
                "localization_acceleration_filter_enabled": getattr(
                    self, "localization_acceleration_filter_enabled", False
                ),
                "acceleration_forward_vrf_unconstrained_mps2": (
                    acceleration_forward_vrf_unconstrained
                ),
                "acceleration_nonnegative_speed_prediction_limited": (
                    acceleration_nonnegative_speed_prediction_limited
                ),
                "acceleration_nonnegative_speed_prediction_min_mps2": (
                    acceleration_nonnegative_speed_prediction_min_mps2
                ),
                "acceleration_nonnegative_speed_prediction_horizon_s": getattr(
                    self,
                    "localization_acceleration_nonnegative_speed_prediction_horizon_s",
                    0.0,
                ),
            },
        )
        write_localization_estimate_to_pb(loc, localization_payload)
        actual_header = getattr(loc, "header", None)
        actual_frame_id = getattr(actual_header, "frame_id", None)
        localization_payload["header"]["frame_id"] = actual_frame_id
        decoded_orientation_heading = localization_payload["metadata"]["quaternion_heading"]
        orientation_heading_diff_rad = localization_payload["metadata"]["quaternion_heading_diff_rad"]
        pose_debug = self._pose_diagnostics(pose_info, header_ts_sec)
        localization_debug = localization_debug_from_dict(localization_payload)
        localization_debug.update(
            {
                "localization_carla_frame_id": direct_world_frame,
                "direct_world_frame": direct_world_frame,
                "localization_orientation_raw_yaw": yaw_ap - math.pi / 2.0,
            }
        )
        pose_debug.update(localization_debug)
        self.stats["localization"].update(
            {
                "header_timestamp_sec": header_ts_sec,
                "measurement_time": header_ts_sec,
                "sequence_num": localization_debug["localization_sequence_num"],
                "module_name": localization_debug["localization_module_name"],
                "frame_id": localization_debug["localization_frame_id"],
                "time_base": localization_debug["localization_time_base"],
                "orientation_convention": localization_debug["orientation_convention"],
                "ego_yaw_rate_rad_s": wz,
                "localization_yaw_rate_rad_s": wz,
                "localization_angular_velocity_z_rad_s": wz,
                "orientation_heading_diff_rad": orientation_heading_diff_rad,
                "angular_velocity_unit": "rad_per_s",
                "angular_velocity_source": localization_debug["angular_velocity_source"],
                "linear_acceleration_x": ax,
                "linear_acceleration_y": ay,
                "linear_acceleration_z": az,
                "linear_acceleration_vrf_x": acceleration_right_vrf,
                "linear_acceleration_vrf_y": acceleration_forward_vrf,
                "linear_acceleration_vrf_z": az,
                "acceleration_source": acceleration_source,
                "localization_acceleration_filter_enabled": getattr(
                    self, "localization_acceleration_filter_enabled", False
                ),
                "acceleration_forward_vrf_unconstrained_mps2": (
                    acceleration_forward_vrf_unconstrained
                ),
                "acceleration_nonnegative_speed_prediction_limited": (
                    acceleration_nonnegative_speed_prediction_limited
                ),
                "acceleration_nonnegative_speed_prediction_min_mps2": (
                    acceleration_nonnegative_speed_prediction_min_mps2
                ),
                "acceleration_nonnegative_speed_prediction_horizon_s": getattr(
                    self,
                    "localization_acceleration_nonnegative_speed_prediction_horizon_s",
                    0.0,
                ),
                "uncertainty_policy": localization_debug["localization_uncertainty_policy"],
                "msf_status_policy": localization_debug["localization_msf_status_policy"],
                "sensor_status_policy": localization_debug["localization_sensor_status_policy"],
            }
        )
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
        steer_sign = float(desired.get("steer_sign", self.steer_sign) or 1.0)
        steer_feedback_sign = -1.0 if steer_sign < 0.0 else 1.0
        carla_steer_cmd_pct = float(desired.get("steer", 0.0)) * 100.0
        apollo_steer_cmd = desired.get("steering_normalized_for_mapping")
        if apollo_steer_cmd is None:
            apollo_steer_cmd = desired.get("steering_selected_normalized")
        if apollo_steer_cmd is None:
            apollo_steer_cmd = float(desired.get("steer", 0.0)) * steer_feedback_sign
        steer_cmd_pct = float(apollo_steer_cmd) * 100.0
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
            carla_steer_pct = float(measured.get("steer_feedback_pct", 0.0))
        else:
            carla_steer_pct = (
                float(measured.get("steer", 0.0)) * 100.0
                if measured.get("available")
                else 0.0
            )
        # Apollo Chassis steering feedback is consumed by Apollo Control in
        # Apollo control-command sign semantics. CARLA applied steer remains in
        # CARLA sign semantics and is preserved separately for artifacts.
        steer_pct = carla_steer_pct * steer_feedback_sign
        self.stats["last_control_feedback"] = {
            "desired": {
                "throttle_pct": throttle_cmd_pct,
                "brake_pct": brake_cmd_pct,
                "steer_pct": steer_cmd_pct,
                "carla_steer_pct": carla_steer_cmd_pct,
                "steering_percentage_frame": "apollo_control",
                "carla_steering_percentage_frame": "carla_control",
                "steer_feedback_sign": steer_feedback_sign,
            },
            "measured": {
                "throttle_pct": throttle_pct,
                "brake_pct": brake_pct,
                "steer_pct": steer_pct,
                "carla_steer_pct": carla_steer_pct,
                "steering_percentage_frame": "apollo_control",
                "carla_steering_percentage_frame": "carla_control",
                "steer_feedback_sign": steer_feedback_sign,
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
        if hasattr(ch.__class__, "NO_ERROR") and hasattr(ch, "error_code"):
            _safe_set(ch, "error_code", getattr(ch.__class__, "NO_ERROR"))
        chassis_state = {
            "speed_mps": speed,
            "driving_mode": self._enum_name_from_msg(
                ch, "driving_mode", getattr(ch, "driving_mode", None)
            ),
            "driving_mode_value": getattr(ch, "driving_mode", None)
            if hasattr(ch, "driving_mode")
            else None,
            "gear_location": self._enum_name_from_msg(
                ch, "gear_location", getattr(ch, "gear_location", None)
            ),
            "gear_location_value": getattr(ch, "gear_location", None)
            if hasattr(ch, "gear_location")
            else None,
            "error_code": self._enum_name_from_msg(
                ch, "error_code", getattr(ch, "error_code", None)
            ),
            "error_code_value": getattr(ch, "error_code", None)
            if hasattr(ch, "error_code")
            else None,
            "throttle_percentage": throttle_pct,
            "brake_percentage": brake_pct,
            "steering_percentage": steer_pct,
            "steering_percentage_frame": "apollo_control",
            "carla_steering_percentage": carla_steer_pct,
            "carla_steering_percentage_frame": "carla_control",
            "steering_feedback_sign": steer_feedback_sign,
            "throttle_percentage_cmd": throttle_cmd_pct,
            "brake_percentage_cmd": brake_cmd_pct,
            "steering_percentage_cmd": steer_cmd_pct,
            "carla_steering_percentage_cmd": carla_steer_cmd_pct,
            "steering_percentage_cmd_frame": "apollo_control",
            "feedback_source": measured.get("source", "unavailable"),
            "feedback_available": bool(measured.get("available", False)),
        }
        self.stats["chassis"] = chassis_state
        self.stats["last_control_feedback"]["chassis"] = dict(chassis_state)
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
        control_rx_ts = self._command_now_sec()
        self.stats["control_rx_count"] += 1
        self.stats["last_control_rx_ts_sec"] = control_rx_ts
        raw_fields = self._extract_raw_control_fields(cmd)
        physical_mode = self.actuator_mapping_mode == "physical"
        throttle_pct = self._coerce_float(raw_fields.get("throttle", 0.0), 0.0, "throttle", "apollo.control")
        brake_pct = self._coerce_float(raw_fields.get("brake", 0.0), 0.0, "brake", "apollo.control")
        steer_source, steer_pct = self._select_steering_field(
            raw_fields,
            physical_mode=physical_mode,
        )
        steer_normalization_mode = steering_normalization_mode_impl(
            steer_source,
            percent_normalization_mode=self.steering_percent_normalization,
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
            "control_rx_timestamp": control_rx_ts,
            "control_header_timestamp_sec": raw_fields.get("control_header_timestamp_sec"),
            "control_header_sequence_num": raw_fields.get("control_header_sequence_num"),
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
            "steering_percent_normalization": self.steering_percent_normalization,
            "steering_normalization_mode": steer_normalization_mode,
            "steering_selected_normalized": steer_pct,
            "steering_normalized_for_mapping": raw_steer,
            "acceleration_mps2": _safe_float(raw_fields.get("acceleration"), float("nan")),
            "speed_mps": _safe_float(raw_fields.get("speed"), float("nan")),
            "debug_simple_lon_station_reference_m": _safe_float(
                raw_fields.get("debug_simple_lon_station_reference"), float("nan")
            ),
            "debug_simple_lon_station_error_m": _safe_float(
                raw_fields.get("debug_simple_lon_station_error"), float("nan")
            ),
            "debug_simple_lon_station_error_limited_m": _safe_float(
                raw_fields.get("debug_simple_lon_station_error_limited"), float("nan")
            ),
            "debug_simple_lon_preview_station_error_m": _safe_float(
                raw_fields.get("debug_simple_lon_preview_station_error"), float("nan")
            ),
            "debug_simple_lon_current_station_m": _safe_float(
                raw_fields.get("debug_simple_lon_current_station"), float("nan")
            ),
            "debug_simple_lon_path_remain_m": _safe_float(
                raw_fields.get("debug_simple_lon_path_remain"), float("nan")
            ),
            "debug_simple_lon_speed_reference_mps": _safe_float(
                raw_fields.get("debug_simple_lon_speed_reference"), float("nan")
            ),
            "debug_simple_lon_speed_error_mps": _safe_float(
                raw_fields.get("debug_simple_lon_speed_error"), float("nan")
            ),
            "debug_simple_lon_speed_controller_input_limited": _safe_float(
                raw_fields.get("debug_simple_lon_speed_controller_input_limited"), float("nan")
            ),
            "debug_simple_lon_preview_speed_reference_mps": _safe_float(
                raw_fields.get("debug_simple_lon_preview_speed_reference"), float("nan")
            ),
            "debug_simple_lon_preview_speed_error_mps": _safe_float(
                raw_fields.get("debug_simple_lon_preview_speed_error"), float("nan")
            ),
            "debug_simple_lon_current_speed_mps": _safe_float(
                raw_fields.get("debug_simple_lon_current_speed"), float("nan")
            ),
            "debug_simple_lon_speed_lookup_mps": _safe_float(
                raw_fields.get("debug_simple_lon_speed_lookup"), float("nan")
            ),
            "debug_simple_lon_speed_offset_mps": _safe_float(
                raw_fields.get("debug_simple_lon_speed_offset"), float("nan")
            ),
            "debug_simple_lon_acceleration_cmd_mps2": _safe_float(
                raw_fields.get("debug_simple_lon_acceleration_cmd"), float("nan")
            ),
            "debug_simple_lon_acceleration_cmd_closeloop_mps2": _safe_float(
                raw_fields.get("debug_simple_lon_acceleration_cmd_closeloop"), float("nan")
            ),
            "debug_simple_lon_acceleration_lookup_mps2": _safe_float(
                raw_fields.get("debug_simple_lon_acceleration_lookup"), float("nan")
            ),
            "debug_simple_lon_acceleration_lookup_limit_mps2": _safe_float(
                raw_fields.get("debug_simple_lon_acceleration_lookup_limit"), float("nan")
            ),
            "debug_simple_lon_acceleration_reference_mps2": _safe_float(
                raw_fields.get("debug_simple_lon_acceleration_reference"), float("nan")
            ),
            "debug_simple_lon_preview_acceleration_reference_mps2": _safe_float(
                raw_fields.get("debug_simple_lon_preview_acceleration_reference"), float("nan")
            ),
            "debug_simple_lon_current_acceleration_mps2": _safe_float(
                raw_fields.get("debug_simple_lon_current_acceleration"), float("nan")
            ),
            "debug_simple_lon_acceleration_error_mps2": _safe_float(
                raw_fields.get("debug_simple_lon_acceleration_error"), float("nan")
            ),
            "debug_simple_lon_jerk_reference_mps3": _safe_float(
                raw_fields.get("debug_simple_lon_jerk_reference"), float("nan")
            ),
            "debug_simple_lon_current_jerk_mps3": _safe_float(
                raw_fields.get("debug_simple_lon_current_jerk"), float("nan")
            ),
            "debug_simple_lon_jerk_error_mps3": _safe_float(
                raw_fields.get("debug_simple_lon_jerk_error"), float("nan")
            ),
            "debug_simple_lon_throttle_cmd_pct": _safe_float(
                raw_fields.get("debug_simple_lon_throttle_cmd"), float("nan")
            ),
            "debug_simple_lon_brake_cmd_pct": _safe_float(
                raw_fields.get("debug_simple_lon_brake_cmd"), float("nan")
            ),
            "debug_simple_lon_calibration_value": _safe_float(
                raw_fields.get("debug_simple_lon_calibration_value"), float("nan")
            ),
            "debug_simple_lon_pid_saturation_status": _safe_float(
                raw_fields.get("debug_simple_lon_pid_saturation_status"), float("nan")
            ),
            "debug_simple_lon_leadlag_saturation_status": _safe_float(
                raw_fields.get("debug_simple_lon_leadlag_saturation_status"), float("nan")
            ),
            "debug_simple_lon_slope_offset_compensation": _safe_float(
                raw_fields.get("debug_simple_lon_slope_offset_compensation"), float("nan")
            ),
            "debug_simple_lon_vehicle_pitch": _safe_float(
                raw_fields.get("debug_simple_lon_vehicle_pitch"), float("nan")
            ),
            "debug_simple_lon_current_steer_interval": _safe_float(
                raw_fields.get("debug_simple_lon_current_steer_interval"), float("nan")
            ),
            "debug_simple_lon_is_full_stop": bool(raw_fields.get("debug_simple_lon_is_full_stop", False)),
            "debug_simple_lon_is_full_stop_soft": bool(
                raw_fields.get("debug_simple_lon_is_full_stop_soft", False)
            ),
            "debug_simple_lon_is_stop_reason_by_destination": bool(
                raw_fields.get("debug_simple_lon_is_stop_reason_by_destination", False)
            ),
            "debug_simple_lon_is_stop_reason_by_prdestrian": bool(
                raw_fields.get("debug_simple_lon_is_stop_reason_by_prdestrian", False)
            ),
            "debug_simple_lon_is_wait_steer": bool(raw_fields.get("debug_simple_lon_is_wait_steer", False)),
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
            "debug_simple_lat_target_point_x": _safe_float(
                raw_fields.get("debug_simple_lat_current_target_point_x"), float("nan")
            ),
            "debug_simple_lat_target_point_y": _safe_float(
                raw_fields.get("debug_simple_lat_current_target_point_y"), float("nan")
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
            "debug_simple_lat_current_reference_point_x": _safe_float(
                raw_fields.get("debug_simple_lat_current_reference_point_x"), float("nan")
            ),
            "debug_simple_lat_current_reference_point_y": _safe_float(
                raw_fields.get("debug_simple_lat_current_reference_point_y"), float("nan")
            ),
            "debug_simple_lat_preview_reference_point_s": _safe_float(
                raw_fields.get("debug_simple_lat_preview_reference_point_s"), float("nan")
            ),
            "debug_simple_lon_matched_point_s": _safe_float(
                raw_fields.get("debug_simple_lon_current_matched_point_s"), float("nan")
            ),
            "debug_simple_lon_matched_point_x": _safe_float(
                raw_fields.get("debug_simple_lon_current_matched_point_x"), float("nan")
            ),
            "debug_simple_lon_matched_point_y": _safe_float(
                raw_fields.get("debug_simple_lon_current_matched_point_y"), float("nan")
            ),
            "debug_simple_lon_matched_point_theta_rad": _safe_float(
                raw_fields.get("debug_simple_lon_current_matched_point_theta"), float("nan")
            ),
            "debug_simple_lon_matched_point_kappa": _safe_float(
                raw_fields.get("debug_simple_lon_current_matched_point_kappa"), float("nan")
            ),
            "debug_simple_mpc_matched_point_s": _safe_float(
                raw_fields.get("debug_simple_mpc_current_matched_point_s"), float("nan")
            ),
            "debug_simple_mpc_matched_point_x": _safe_float(
                raw_fields.get("debug_simple_mpc_current_matched_point_x"), float("nan")
            ),
            "debug_simple_mpc_matched_point_y": _safe_float(
                raw_fields.get("debug_simple_mpc_current_matched_point_y"), float("nan")
            ),
            "debug_simple_mpc_matched_point_theta_rad": _safe_float(
                raw_fields.get("debug_simple_mpc_current_matched_point_theta"), float("nan")
            ),
            "debug_simple_mpc_matched_point_kappa": _safe_float(
                raw_fields.get("debug_simple_mpc_current_matched_point_kappa"), float("nan")
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
        throttle_before_mutual_exclusion = float(throttle_after_boost)
        brake_before_mutual_exclusion = float(brake_cmd)
        mutual_exclusion = apply_throttle_brake_mutual_exclusion_impl(
            throttle=throttle_after_boost,
            brake=brake_cmd,
            previous_state=self._throttle_brake_policy_state,
            hysteresis_counter=self._throttle_brake_hysteresis_counter,
            config=self._control_mapping_config(),
        )
        throttle_after_boost = float(mutual_exclusion.get("throttle", throttle_after_boost))
        brake_cmd = float(mutual_exclusion.get("brake", brake_cmd))
        self._throttle_brake_policy_state = str(
            mutual_exclusion.get("policy_state") or self._throttle_brake_policy_state
        )
        self._throttle_brake_hysteresis_counter = int(
            mutual_exclusion.get("hysteresis_counter", self._throttle_brake_hysteresis_counter) or 0
        )
        self.stats["last_control_out"] = {
            "throttle": throttle_after_boost,
            "brake": brake_cmd,
            "steer": steer,
            "steer_before_lateral_guards": steer_before_lateral_guards,
            "actuator_mapping_mode": self.actuator_mapping_mode,
            "steering_normalization_mode": steer_normalization_mode,
            "steering_selected_normalized": steer_pct,
            "steering_normalized_for_mapping": raw_steer,
            "type": self.node.control_out_type,
            "steer_pre_clamp": steer_pre,
            "steer_clamped": bool(base_mapping.get("steer_clamped", abs(steer_pre) > 1.0)),
            "steer_sign": base_mapping.get("steer_sign", self.steer_sign),
            "steer_scale": base_mapping.get("steer_scale", self.steer_scale),
            "throttle_before_boost": throttle_cmd,
            "throttle_after_boost": throttle_after_boost,
            "startup_boost_applied": throttle_before_mutual_exclusion > throttle_cmd,
            "throttle_before_mutual_exclusion": throttle_before_mutual_exclusion,
            "brake_before_mutual_exclusion": brake_before_mutual_exclusion,
            "throttle_brake_mutual_exclusion_enabled": self.throttle_brake_mutual_exclusion_enabled,
            "throttle_brake_mutual_exclusion_applied": bool(mutual_exclusion.get("policy_applied", False)),
            "throttle_brake_mutual_exclusion_reason": mutual_exclusion.get("policy_reason", ""),
            "throttle_brake_mutual_exclusion_state": mutual_exclusion.get("policy_state", ""),
            "throttle_brake_mutual_exclusion_desired_state": mutual_exclusion.get("desired_state", ""),
            "throttle_brake_hysteresis_counter": self._throttle_brake_hysteresis_counter,
            "throttle_brake_hysteresis_held": bool(mutual_exclusion.get("hysteresis_held", False)),
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
            "base_mapped_carla_steer_cmd": base_mapping.get("mapped_carla_steer_cmd"),
            "mapped_carla_steer_cmd": steer,
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
        control_latency_ms = (
            max(0.0, (control_ts - control_rx_ts) * 1000.0)
            if _finite_or_none(control_rx_ts) is not None and _finite_or_none(control_ts) is not None
            else None
        )
        control_header_ts = _finite_or_none(raw_fields.get("control_header_timestamp_sec"))
        control_message_age_ms = (
            max(0.0, (control_ts - control_header_ts) * 1000.0)
            if control_header_ts is not None
            else None
        )
        gt_state_publish = self.stats.get("last_gt_state_publish", {}) or {}
        if not isinstance(gt_state_publish, dict):
            gt_state_publish = {}
        gt_publish_wall = _finite_or_none(gt_state_publish.get("publish_wall_time_sec"))
        gt_sim_time = _finite_or_none(gt_state_publish.get("sim_time_sec"))
        gt_world_frame = gt_state_publish.get("world_frame")
        try:
            gt_world_frame_int = int(gt_world_frame) if gt_world_frame is not None else None
        except Exception:
            gt_world_frame_int = None
        gt_state_age_wall_ms = (
            max(0.0, (control_ts - gt_publish_wall) * 1000.0)
            if gt_publish_wall is not None
            else None
        )
        latest_sim_time = _finite_or_none(getattr(self, "_latest_sim_time_sec", None))
        gt_state_age_sim_ms = (
            max(0.0, (latest_sim_time - gt_sim_time) * 1000.0)
            if latest_sim_time is not None and gt_sim_time is not None
            else None
        )
        previous_control_gt_world_frame = getattr(self, "_last_control_gt_world_frame", None)
        if gt_world_frame_int is not None and previous_control_gt_world_frame == gt_world_frame_int:
            self._same_gt_world_frame_control_cycle_streak = int(
                getattr(self, "_same_gt_world_frame_control_cycle_streak", 0) or 0
            ) + 1
        elif gt_world_frame_int is not None:
            self._same_gt_world_frame_control_cycle_streak = 1
            self._last_control_gt_world_frame = gt_world_frame_int
        else:
            self._same_gt_world_frame_control_cycle_streak = 0
            self._last_control_gt_world_frame = None
        gt_freshness_fields = {
            "gt_state_sim_time_sec": gt_sim_time,
            "gt_state_world_frame": gt_world_frame_int,
            "gt_state_publish_wall_time_sec": gt_publish_wall,
            "gt_state_sample_reason": gt_state_publish.get("sample_reason"),
            "gt_state_fresh_sample": bool(gt_state_publish.get("fresh_sample", False)),
            "gt_state_age_wall_ms": gt_state_age_wall_ms,
            "gt_state_age_sim_ms": gt_state_age_sim_ms,
            "gt_state_localization_sequence_num": gt_state_publish.get("localization_sequence_num"),
            "gt_state_chassis_sequence_num": gt_state_publish.get("chassis_sequence_num"),
            "same_gt_world_frame_control_cycle_streak": int(
                getattr(self, "_same_gt_world_frame_control_cycle_streak", 0) or 0
            ),
        }
        self.stats["last_control_in"].update(
            {
                "control_timestamp": control_ts,
                "control_latency_ms": control_latency_ms,
                "control_message_age_ms": control_message_age_ms,
                **gt_freshness_fields,
            }
        )
        self.stats["last_control_out"].update(
            {
                "control_rx_timestamp": control_rx_ts,
                "control_timestamp": control_ts,
                "control_latency_ms": control_latency_ms,
                "control_message_age_ms": control_message_age_ms,
                "planning_timestamp": latest_planning_header_ts if latest_planning_header_ts is not None else latest_planning_ts,
                "planning_message_age_ms": latest_planning_age_ms,
                **gt_freshness_fields,
            }
        )
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
            "control_rx_timestamp": control_rx_ts,
            "control_latency_ms": control_latency_ms,
            "control_message_age_ms": control_message_age_ms,
            "gt_state_sim_time_sec": gt_freshness_fields.get("gt_state_sim_time_sec"),
            "gt_state_world_frame": gt_freshness_fields.get("gt_state_world_frame"),
            "gt_state_publish_wall_time_sec": gt_freshness_fields.get(
                "gt_state_publish_wall_time_sec"
            ),
            "gt_state_sample_reason": gt_freshness_fields.get("gt_state_sample_reason"),
            "gt_state_fresh_sample": gt_freshness_fields.get("gt_state_fresh_sample"),
            "gt_state_age_wall_ms": gt_freshness_fields.get("gt_state_age_wall_ms"),
            "gt_state_age_sim_ms": gt_freshness_fields.get("gt_state_age_sim_ms"),
            "same_gt_world_frame_control_cycle_streak": gt_freshness_fields.get(
                "same_gt_world_frame_control_cycle_streak"
            ),
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
        self._append_control_debug_jsonl(
            "control_trajectory_consume_debug_live",
            self.control_trajectory_consume_live_path,
            control_consume_live,
        )
        if self.debug_dump_control_raw:
            self._append_control_debug_jsonl(
                "apollo_control_raw",
                self.apollo_control_raw_path,
                {
                    "ts_sec": control_ts,
                    "apollo_control_raw": raw_fields,
                    "selected_steering_field": steer_source,
                },
            )
            self._append_control_debug_jsonl(
                "bridge_control_decode",
                self.bridge_control_decode_path,
                {
                    "ts_sec": control_ts,
                    "mapping_mode": self.actuator_mapping_mode,
                    "steer_scale": self.stats["last_control_out"].get("steer_scale", self.steer_scale),
                    "steering_sign": self.stats["last_control_out"].get("steer_sign", self.steer_sign),
                    "selected_steering_field": steer_source,
                    "steering_field_priority": self.stats["last_control_in"].get("steering_field_priority", []),
                    "steering_normalization_mode": steer_normalization_mode,
                    "steering_selected_normalized": steer_pct,
                    "steering_normalized_for_mapping": raw_steer,
                    "raw_throttle": raw_throttle,
                    "raw_brake": raw_brake,
                    "raw_steer": raw_steer,
                    "commanded_steer_pre_lateral_guards": steer_before_lateral_guards,
                    "commanded_throttle": throttle_after_boost,
                    "commanded_brake": brake_cmd,
                    "commanded_steer": steer,
                    "throttle_before_mutual_exclusion": self.stats["last_control_out"].get(
                        "throttle_before_mutual_exclusion"
                    ),
                    "brake_before_mutual_exclusion": self.stats["last_control_out"].get(
                        "brake_before_mutual_exclusion"
                    ),
                    "throttle_brake_mutual_exclusion_enabled": self.stats["last_control_out"].get(
                        "throttle_brake_mutual_exclusion_enabled"
                    ),
                    "throttle_brake_mutual_exclusion_applied": self.stats["last_control_out"].get(
                        "throttle_brake_mutual_exclusion_applied"
                    ),
                    "throttle_brake_mutual_exclusion_reason": self.stats["last_control_out"].get(
                        "throttle_brake_mutual_exclusion_reason", ""
                    ),
                    "throttle_brake_mutual_exclusion_state": self.stats["last_control_out"].get(
                        "throttle_brake_mutual_exclusion_state", ""
                    ),
                    "throttle_brake_mutual_exclusion_desired_state": self.stats["last_control_out"].get(
                        "throttle_brake_mutual_exclusion_desired_state", ""
                    ),
                    "throttle_brake_hysteresis_held": self.stats["last_control_out"].get(
                        "throttle_brake_hysteresis_held", False
                    ),
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
            self._append_control_debug_jsonl(
                "control_decode_debug",
                self.control_decode_dump_path,
                raw_dump,
            )
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
        publish_allowed = self._allow_control_publish_after_startup(
            planning_valid=planning_lateral_contract_valid,
            planning_reason=planning_lateral_contract_reason,
            timestamp_sec=control_ts,
        )
        startup_gate = self.stats.get("control_startup_publish_gate", {}) or {}
        self.stats["last_control_out"].update(
            {
                "startup_publish_gate_enabled": bool(startup_gate.get("enabled", False)),
                "startup_publish_gate_open": bool(startup_gate.get("open", True)),
                "startup_publish_suppressed": not publish_allowed,
                "startup_publish_suppress_reason": (
                    str(startup_gate.get("last_skip_reason") or "") if not publish_allowed else ""
                ),
            }
        )
        if not publish_allowed:
            return
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
            if Float32MultiArray is not None:
                msg = Float32MultiArray()
            else:
                # carla_direct should not require ROS2 std_msgs at runtime. The
                # direct transport only consumes a `.data` sequence.
                msg = types.SimpleNamespace(data=[])
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
        try:
            self.node.publish_control(msg)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            self.stats["control_publish_exception_count"] = int(
                self.stats.get("control_publish_exception_count", 0) or 0
            ) + 1
            self.stats["last_control_publish_error"] = error
            self.stats["last_error"] = f"control_publish_failed:{error}"
            self._append_jsonl(
                self.control_publish_error_path,
                {
                    "ts_sec": control_ts,
                    "control_out_type": self.node.control_out_type,
                    "error": error,
                    "output_to_carla": dict(self.stats.get("last_control_out", {}) or {}),
                },
            )
            print(f"[bridge][warning] control_publish_failed: {error}", file=sys.stderr)
            return
        self.stats["last_control_publish_error"] = ""
        self.stats["control_tx_count"] += 1

    def _on_raw_routing_response(self, msg: Any) -> None:
        self._on_routing_response(
            msg,
            source_channel=self.raw_routing_response_channel,
            relay_to_planning=True,
        )

    def _on_routing_response(
        self,
        msg: Any,
        *,
        source_channel: Optional[str] = None,
        relay_to_planning: bool = False,
    ) -> None:
        now_sec = self._command_now_sec()
        source_channel = str(source_channel or self.routing_response_channel or "")
        if source_channel == self.raw_routing_response_channel:
            self.stats["raw_routing_response_count"] += 1
        if source_channel == self.routing_response_channel:
            self.stats["planning_routing_response_count"] += 1
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
        try:
            decoded_payload = self._routing_response_decoded_payload(
                msg,
                now_sec=now_sec,
                source_channel=source_channel,
            )
            self._append_jsonl(self.routing_response_decoded_jsonl_path, decoded_payload)
            self._write_json_file(self.routing_response_decoded_path, decoded_payload)
            self.stats["routing_response_decoded_path"] = str(self.routing_response_decoded_path)
            self.stats["routing_response_decoded_jsonl_path"] = str(
                self.routing_response_decoded_jsonl_path
            )
            self.stats["routing_response_decoded_lane_segment_count"] = int(
                decoded_payload.get("lane_segment_count") or 0
            )
            self.stats["routing_response_decoded_total_length_m"] = _finite_or_none(
                decoded_payload.get("total_length_m")
            )
        except Exception as exc:
            self.stats["routing_response_decode_error"] = f"{type(exc).__name__}: {exc}"
        if (
            relay_to_planning
            and self.routing_response_writer is not None
            and source_channel != self.routing_response_channel
        ):
            try:
                self.routing_response_writer.write(msg)
                self.stats["routing_response_relay_count"] += 1
                self.stats["last_routing_response_relay"] = {
                    "source_channel": source_channel,
                    "target_channel": self.routing_response_channel,
                    "timestamp_sec": _finite_or_none(now_sec),
                    "road_count": int(road_count),
                    "claim_boundary": (
                        "Relay forwards the Apollo routing module's raw RoutingResponse to "
                        "the planning-facing /apollo/routing_response channel. It must not "
                        "be replaced by scenario-route synthesis."
                    ),
                }
            except Exception as exc:
                self.stats["routing_response_relay_error_count"] += 1
                self.stats["last_routing_response_relay_error"] = f"{type(exc).__name__}: {exc}"
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
            runtime_root = _bridge_path(self.apollo_runtime_map_dir)
            if not runtime_root.is_absolute():
                runtime_root = (self.apollo_root / runtime_root).resolve()
            for candidate in ("base_map.txt", "base_map.xml", "sim_map.txt", "sim_map.xml"):
                path = runtime_root / candidate
                if path.exists():
                    return path.resolve()
        if self.map_file_path:
            map_file = _bridge_path(self.map_file_path)
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
            runtime_root = _bridge_path(self.apollo_runtime_map_dir)
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
            bounds_file = _bridge_path(self.map_bounds_file)
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
        if not self.auto_routing_startup_route_enabled:
            return "long" if self.auto_routing_use_long_goal_after_move else "claim"
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

    def _apollo_startup_warmup_readiness(
        self,
        *,
        startup_delay_elapsed_sec: float,
        startup_apollo_warmup_elapsed_sec: float,
    ) -> Dict[str, Any]:
        min_elapsed_sec = max(
            float(getattr(self, "auto_routing_startup_delay_sec", 0.0) or 0.0),
            float(
                getattr(
                    self,
                    "auto_routing_startup_apollo_warmup_bypass_min_elapsed_sec",
                    0.0,
                )
                or 0.0
            ),
        )
        planning_message_count = int(getattr(self, "_planning_msg_count", 0) or 0)
        min_planning_messages = int(
            getattr(
                self,
                "auto_routing_startup_apollo_warmup_ready_min_planning_messages",
                0,
            )
            or 0
        )
        accept_preplanning_gt_only = bool(
            getattr(
                self,
                "auto_routing_startup_apollo_warmup_ready_accept_preplanning_gt_only",
                False,
            )
        )
        localization_fresh = int(self.stats.get("localization_fresh_publish_count", 0) or 0)
        chassis_fresh = int(self.stats.get("chassis_fresh_publish_count", 0) or 0)
        min_gt_fresh = int(
            getattr(
                self,
                "auto_routing_startup_apollo_warmup_ready_min_gt_fresh_samples",
                0,
            )
            or 0
        )
        routing_interface_ready = bool(
            (self.routing_writer is not None and self.auto_routing_send_routing)
            or (
                self.lane_follow_client is not None
                and self.auto_routing_send_lane_follow
                and (not self._lane_follow_disabled_runtime)
            )
            or (self.action_client is not None and self.auto_routing_send_action)
        )
        checks = {
            "policy_enabled": bool(
                getattr(self, "auto_routing_startup_apollo_warmup_bypass_when_ready", False)
            ),
            "startup_delay_elapsed_sec": _finite_or_none(startup_delay_elapsed_sec),
            "startup_apollo_warmup_elapsed_sec": _finite_or_none(startup_apollo_warmup_elapsed_sec),
            "min_elapsed_sec": _finite_or_none(min_elapsed_sec),
            "planning_message_count": planning_message_count,
            "min_planning_messages": min_planning_messages,
            "planning_reader_observed": planning_message_count >= min_planning_messages,
            "accept_preplanning_gt_only": accept_preplanning_gt_only,
            "localization_fresh_publish_count": localization_fresh,
            "chassis_fresh_publish_count": chassis_fresh,
            "min_gt_fresh_samples": min_gt_fresh,
            "routing_interface_ready": routing_interface_ready,
        }
        missing: List[str] = []
        if not checks["policy_enabled"]:
            missing.append("policy_disabled")
        if startup_delay_elapsed_sec < min_elapsed_sec:
            missing.append("startup_elapsed_below_ready_min")
        if (
            planning_message_count < min_planning_messages
            and not accept_preplanning_gt_only
        ):
            missing.append("planning_reader_not_observed")
        if localization_fresh < min_gt_fresh:
            missing.append("localization_fresh_samples_insufficient")
        if chassis_fresh < min_gt_fresh:
            missing.append("chassis_fresh_samples_insufficient")
        if not routing_interface_ready:
            missing.append("routing_or_command_interface_unavailable")
        checks["ready"] = not missing
        checks["missing_ready_conditions"] = missing
        if planning_message_count >= min_planning_messages:
            readiness_source = "planning_reader_and_gt_inputs"
        elif accept_preplanning_gt_only:
            readiness_source = "preplanning_gt_inputs_and_command_interface"
        else:
            readiness_source = "not_ready"
        checks["readiness_source"] = readiness_source
        checks["readiness_policy"] = (
            "claim_route_send_may_bypass_fixed_apollo_warmup_after_planning_and_gt_inputs; "
            "if explicitly configured, preplanning_gt_only can send once GT inputs and command "
            "interfaces are ready but must be reported as route-establishment timing evidence only"
        )
        return checks

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
            warmup_readiness = self._apollo_startup_warmup_readiness(
                startup_delay_elapsed_sec=startup_delay_elapsed_sec,
                startup_apollo_warmup_elapsed_sec=startup_apollo_warmup_elapsed_sec,
            )
            self.stats["apollo_startup_warmup_readiness"] = dict(warmup_readiness)
            if bool(warmup_readiness.get("ready")):
                self.stats["apollo_startup_warmup_bypass_count"] = int(
                    self.stats.get("apollo_startup_warmup_bypass_count", 0) or 0
                ) + 1
                self.stats["last_error"] = ""
                self._update_command_gate_state(
                    ts_sec=ts_sec,
                    phase="startup",
                    status="warmup_bypassed_by_readiness",
                    blocking_reason="",
                    blocking_detail=(
                        "configured Apollo startup warmup bypassed after readiness policy "
                        f"{warmup_readiness.get('readiness_source', 'unknown')} passed"
                    ),
                    eligible=True,
                    apollo_warmup_remaining_sec=max(
                        0.0,
                        self.auto_routing_startup_apollo_warmup_sec
                        - startup_apollo_warmup_elapsed_sec,
                    ),
                    apollo_warmup_bypassed_by_readiness=True,
                    apollo_warmup_readiness=warmup_readiness,
                )
            else:
                self.stats["last_error"] = "waiting_for_apollo_startup_warmup"
                self._update_command_gate_state(
                    ts_sec=ts_sec,
                    phase="startup",
                    status="blocked",
                    blocking_reason="apollo_startup_warmup",
                    blocking_detail="waiting for configured Apollo startup warmup window or readiness-based bypass",
                    eligible=True,
                    apollo_warmup_remaining_sec=max(
                        0.0,
                        self.auto_routing_startup_apollo_warmup_sec
                        - startup_apollo_warmup_elapsed_sec,
                    ),
                    apollo_warmup_readiness=warmup_readiness,
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
        routing_blocked_due_to_claim_profile_fallback = False
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
        if self._claim_profile_blocks_route_goal(goal_meta):
            routing_blocked_due_to_claim_profile_fallback = True
            routing_skipped_due_to_invalid_goal = True
            send_routing_now = False
            goal_validity["invalid_goal"] = True
            goal_validity["invalid_goal_reason"] = "fallback_route_blocked_by_claim_profile"
            goal_validity["fallback_blocked_by_claim_profile"] = True
            goal_validity["claim_profile_enabled"] = bool(self.claim_profile_enabled)
            goal_validity["materialization_probe_enabled"] = bool(self.materialization_probe_enabled)
            self.stats["fallback_blocked_by_claim_profile_count"] = int(
                self.stats.get("fallback_blocked_by_claim_profile_count", 0) or 0
            ) + 1
            claim_route_policy = dict(self.stats.get("claim_profile_route_policy", {}) or {})
            claim_route_policy["fallback_blocked_by_claim_profile_count"] = int(
                self.stats.get("fallback_blocked_by_claim_profile_count", 0) or 0
            )
            self.stats["claim_profile_route_policy"] = claim_route_policy
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
        post_routing_reference_guard_applicable = bool(
            self.auto_routing_established
            or recent_reference_line_count > 0
            or recent_route_segment_count > 0
            or int(recent_route_debug.get("routing_lane_window_count", 0) or 0) > 0
        )
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
        # -----------------------------------------------------------------
        # Unstable reference line guard: suppress long-phase reroute when
        # planning cannot produce reference lines (2026-06-30 enhanced)
        #
        # Blocking conditions (any one triggers guard):
        #   A) invalid_goal — goal projection failed or goal is outside map
        #   B) reference_line_count <= 0 AND not debug_missing — no ref line
        #      and the debug field itself is present (so it's a real missing,
        #      not just debug not populated)
        #   C) route_segment_count <= 0 — no route segments from routing
        #   D) lane_follow_map_inconsistent — map/lane metadata mismatch
        #   E) map_contract_invalid — bridge map ≠ runtime map (WARNING:
        #      this alone blocks ALL long-phase routing regardless of
        #      reference_line status!)
        # -----------------------------------------------------------------
        unstable_long_reroute_guard_triggers = []
        unstable_long_reroute_guard_active = False
        if (
            phase == "long"
            and not routing_waiting_for_route_debug_ready
            and not ignore_roll_active
            and self.auto_routing_suppress_long_phase_reroute_on_unstable_reference_line
            and post_routing_reference_guard_applicable
        ):
            if bool(goal_validity.get("invalid_goal")):
                unstable_long_reroute_guard_triggers.append(
                    f"invalid_goal:{goal_validity.get('invalid_goal_reason', 'unknown')}"
                )
            if (
                recent_route_debug_fresh
                and recent_reference_line_count <= 0
                and not recent_reference_line_debug_missing_but_trajectory_nonzero
            ):
                unstable_long_reroute_guard_triggers.append(
                    f"reference_line_empty(ref_count={recent_reference_line_count})"
                )
            if recent_route_debug_fresh and recent_route_segment_count <= 0:
                unstable_long_reroute_guard_triggers.append(
                    f"route_segment_empty(seg_count={recent_route_segment_count})"
                )
            if recent_route_debug_fresh and lane_follow_map_inconsistent_active:
                unstable_long_reroute_guard_triggers.append(
                    f"lane_follow_inconsistent(status={recent_lane_follow_map_status})"
                )
            if bool(self.map_contract_invalid):
                unstable_long_reroute_guard_triggers.append(
                    f"map_contract_invalid(reason={self.map_contract_mismatch_reason[:80]})"
                )
            unstable_long_reroute_guard_active = bool(unstable_long_reroute_guard_triggers)
        if unstable_long_reroute_guard_active:
            routing_skipped_due_to_unstable_reference_line = True
            if send_routing_now:
                send_routing_now = False
                self.stats["routing_skipped_due_to_unstable_reference_line_count"] = int(
                    self.stats.get("routing_skipped_due_to_unstable_reference_line_count", 0) or 0
                ) + 1
            # Log detailed trigger info on first skip and every 50th
            skip_count = int(
                self.stats.get("routing_skipped_due_to_unstable_reference_line_count", 0) or 0
            )
            if skip_count <= 1 or skip_count % 50 == 0:
                print(
                    f"[bridge][routing][warn] unstable_long_reroute_guard "
                    f"skip_count={skip_count} triggers={unstable_long_reroute_guard_triggers} "
                    f"map_invalid={self.map_contract_invalid} "
                    f"suppress_enabled={self.auto_routing_suppress_long_phase_reroute_on_unstable_reference_line}"
                )
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
        if routing_blocked_due_to_claim_profile_fallback:
            request_kind = "blocked"
            reroute_reason = "fallback_route_blocked_by_claim_profile"
            reroute_trigger_source = "claim_profile_route_policy"
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
                if routing_blocked_due_to_claim_profile_fallback:
                    gate_blocking_reason = "fallback_route_blocked_by_claim_profile"
                    gate_blocking_detail = (
                        "claim/materialization profile forbids ego-seed/startup/fallback "
                        "routing goals"
                    )
                else:
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
                "carla_world_frame": timing.get("world_frame"),
                "source_clock": "sim_time" if timing.get("sim_time_sec") is not None else "wall_time",
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
                    "fallback_blocked_by_claim_profile": bool(
                        goal_validity.get("fallback_blocked_by_claim_profile", False)
                    ),
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
                    "unstable_long_reroute_guard_active": unstable_long_reroute_guard_active,
                    "unstable_long_reroute_guard_triggers": unstable_long_reroute_guard_triggers,
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
            "localization_back_offset_source": self.localization_back_offset_source,
            "localization_back_offset_resolve_error": self.localization_back_offset_resolve_error,
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
            "routing_blocked_due_to_claim_profile_fallback": routing_blocked_due_to_claim_profile_fallback,
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
        if (
            send_routing_now
            or routing_skipped_due_to_freeze
            or routing_skipped_due_to_invalid_goal
            or routing_blocked_due_to_claim_profile_fallback
        ):
            self._record_startup_geometry_event(startup_event)
            self._record_routing_event(
                {
                    "timestamp": command_ts,
                    "wall_time_sec": timing.get("wall_time_sec"),
                    "sim_time_sec": timing.get("sim_time_sec"),
                    "world_frame": timing.get("world_frame"),
                    "carla_world_frame": timing.get("world_frame"),
                    "source_clock": "sim_time" if timing.get("sim_time_sec") is not None else "wall_time",
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
                    "routing_blocked_due_to_claim_profile_fallback": bool(
                        routing_blocked_due_to_claim_profile_fallback
                    ),
                    "fallback_blocked_by_claim_profile": bool(
                        goal_validity.get("fallback_blocked_by_claim_profile", False)
                    ),
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

    def _write_stats(self, *, full: bool = True) -> None:
        stats_write_start_s = time.time()
        buffering = self.stats.setdefault("artifact_buffering", {})
        stats_write_mode = "full" if full else "lightweight"
        buffering["last_stats_write_mode"] = stats_write_mode
        count_key = f"{stats_write_mode}_stats_write_count"
        buffering[count_key] = int(buffering.get(count_key, 0) or 0) + 1

        def record_stats_phase_duration(key: str, start_s: float) -> None:
            duration_s = max(0.0, time.time() - start_s)
            buffering[key] = duration_s
            max_key = f"{key}_max"
            buffering[max_key] = max(float(buffering.get(max_key, 0.0) or 0.0), duration_s)

        if full:
            try:
                stats_flush_interval_s = max(
                    0.0,
                    float(getattr(self, "artifact_stats_flush_interval_s", 5.0) or 0.0),
                )
                last_flush = float(getattr(self, "_last_artifact_stats_flush_sec", 0.0) or 0.0)
                now = time.time()
                if stats_flush_interval_s > 0.0 and (now - last_flush) >= stats_flush_interval_s:
                    flush_start_s = time.time()
                    self._flush_artifact_buffers(close=False)
                    record_stats_phase_duration("artifact_stats_flush_duration_s", flush_start_s)
                    self._last_artifact_stats_flush_sec = now
            except Exception:
                pass
            health_start_s = time.time()
            self._write_health_summary()
            record_stats_phase_duration("health_summary_write_duration_s", health_start_s)
            startup_start_s = time.time()
            self._write_startup_geometry_summary()
            record_stats_phase_duration("startup_geometry_summary_write_duration_s", startup_start_s)
            planning_start_s = time.time()
            self._write_planning_topic_debug_summary()
            record_stats_phase_duration("planning_summary_write_duration_s", planning_start_s)
            if hasattr(self.node, "write_artifacts"):
                try:
                    node_artifacts_start_s = time.time()
                    self.node.write_artifacts()
                    record_stats_phase_duration(
                        "node_write_artifacts_duration_s",
                        node_artifacts_start_s,
                    )
                except Exception:
                    pass
        stats_file_start_s = time.time()
        self._write_json_file(self.stats_path, self.stats)
        record_stats_phase_duration("stats_json_write_duration_s", stats_file_start_s)
        record_stats_phase_duration("stats_write_duration_s", stats_write_start_s)
        record_stats_phase_duration(
            f"{stats_write_mode}_stats_write_duration_s",
            stats_write_start_s,
        )
        if full:
            # Persist shutdown/full-mode timing fields that are only known after
            # the first JSON write. Keep the 20 Hz lightweight path single-write.
            self._write_json_file(self.stats_path, self.stats)

    def run(self) -> int:
        self.ros_thread.start()
        if self.cyber_spin_thread is not None:
            self.cyber_spin_thread.start()

        period = 1.0 / max(self.publish_rate_hz, 1e-3)
        next_publish_wall_s = time.time()

        def sleep_until_next_publish_cycle() -> None:
            nonlocal next_publish_wall_s
            next_publish_wall_s += period
            now_sleep = time.time()
            overrun_s = max(0.0, now_sleep - next_publish_wall_s)
            sleep_s = max(0.0, next_publish_wall_s - now_sleep)
            if sleep_s > 0.0:
                time.sleep(sleep_s)
            else:
                # If the bridge loop overruns, do not add another full-period
                # sleep. Re-anchor so configured publish_rate_hz is a target
                # cadence rather than processing_time + period.
                next_publish_wall_s = now_sleep
            self.stats["publish_loop_rate_limiter"] = {
                "target_period_s": period,
                "last_sleep_s": sleep_s,
                "last_overrun_s": overrun_s,
                "mode": "deadline",
            }

        last_stats_flush = time.time()

        def maybe_write_periodic_stats(now_s: float) -> None:
            nonlocal last_stats_flush
            interval_s = max(
                0.0,
                float(getattr(self, "artifact_stats_flush_interval_s", 0.0) or 0.0),
            )
            if interval_s <= 0.0:
                return
            if (now_s - last_stats_flush) >= interval_s:
                # Keep periodic progress visible to wrappers without flushing
                # heavier summaries or node artifacts from the 20 Hz GT publish
                # hot path. Full stats/summaries are still forced at shutdown.
                self._write_stats(full=False)
                last_stats_flush = now_s

        while not self.stop_event.is_set():
            loop_start_wall_s = time.time()
            loop_published_gt = False
            snapshot: Dict[str, Any] = {}
            odom: Any = None
            if hasattr(self.cyber, "is_shutdown") and self.cyber.is_shutdown():
                break
            try:
                phase_start_wall_s = time.time()
                if hasattr(self.node, "tick"):
                    try:
                        self.node.tick()
                    except Exception:
                        pass
                self._record_publish_phase_timing(
                    "node_tick",
                    start_wall_s=phase_start_wall_s,
                    end_wall_s=time.time(),
                    target_period_s=period,
                )
                phase_start_wall_s = time.time()
                snapshot = self.node.snapshot()
                self._record_publish_phase_timing(
                    "ros_snapshot",
                    start_wall_s=phase_start_wall_s,
                    end_wall_s=time.time(),
                    target_period_s=period,
                )
                self.stats["ros_input_counts"] = snapshot.get("rx_counts", self.stats["ros_input_counts"])
                odom = snapshot.get("odom")
                if (
                    self.transport_mode == "carla_direct"
                    and odom is not None
                    and not bool(snapshot.get("world_frame_advanced", False))
                ):
                    stale_policy = str(
                        getattr(self.node, "stale_world_frame_policy", "until_control")
                        or "until_control"
                    )
                    self.stats["direct_stale_world_frame_policy"] = stale_policy
                    if should_republish_stale_world_frame(
                        stale_policy,
                        control_tx_count=int(self.stats.get("control_tx_count", 0) or 0),
                    ):
                        # Direct transport must not tick CARLA. This policy only
                        # controls whether the bridge republishes the latest cached
                        # GT sample when the harness has not advanced the world yet.
                        self.stats["direct_stale_world_frame_republish_count"] = int(
                            self.stats.get("direct_stale_world_frame_republish_count", 0) or 0
                        ) + 1
                    else:
                        self.stats["direct_stale_world_frame_skip_count"] = int(
                            self.stats.get("direct_stale_world_frame_skip_count", 0) or 0
                        ) + 1
                        self._record_publish_gap_trace(
                            snapshot=snapshot,
                            odom=odom,
                            expected_publish=True,
                            published_localization=False,
                            published_chassis=False,
                            skip_reason="world_frame_not_advanced",
                            loop_start_wall_s=loop_start_wall_s,
                        )
                        now = time.time()
                        maybe_write_periodic_stats(now)
                        self._record_publish_loop_timing(
                            start_wall_s=loop_start_wall_s,
                            end_wall_s=time.time(),
                            target_period_s=period,
                            published_gt=False,
                            phase="direct_stale_world_frame_skip",
                        )
                        sleep_until_next_publish_cycle()
                        continue
                if odom is not None:
                    direct_world_frame = snapshot.get("world_frame")
                    phase_start_wall_s = time.time()
                    should_publish_gt, _sample_key, sample_reason = self._should_publish_gt_sample(
                        odom,
                        direct_world_frame=direct_world_frame,
                    )
                    self._record_publish_phase_timing(
                        "gt_sample_freshness_check",
                        start_wall_s=phase_start_wall_s,
                        end_wall_s=time.time(),
                        target_period_s=period,
                    )
                    if not should_publish_gt:
                        self._record_publish_gap_trace(
                            snapshot=snapshot,
                            odom=odom,
                            expected_publish=True,
                            published_localization=False,
                            published_chassis=False,
                            skip_reason=sample_reason or "unknown",
                            loop_start_wall_s=loop_start_wall_s,
                        )
                        now = time.time()
                        maybe_write_periodic_stats(now)
                        self._record_publish_loop_timing(
                            start_wall_s=loop_start_wall_s,
                            end_wall_s=time.time(),
                            target_period_s=period,
                            published_gt=False,
                            phase="gt_stale_sample_skip",
                        )
                        sleep_until_next_publish_cycle()
                        continue
                    phase_start_wall_s = time.time()
                    loc, ts_sec, vel_xyz, pose_debug, pose_info = self._odom_to_loc(
                        odom,
                        direct_world_frame=direct_world_frame,
                    )
                    self._record_publish_phase_timing(
                        "odom_to_localization",
                        start_wall_s=phase_start_wall_s,
                        end_wall_s=time.time(),
                        target_period_s=period,
                    )
                    phase_start_wall_s = time.time()
                    self.loc_writer.write(loc)
                    self._record_publish_phase_timing(
                        "cyber_write_localization",
                        start_wall_s=phase_start_wall_s,
                        end_wall_s=time.time(),
                        target_period_s=period,
                    )
                    self.stats["loc_count"] += 1
                    if sample_reason == "fresh_sample":
                        self.stats["localization_fresh_publish_count"] = int(
                            self.stats.get("localization_fresh_publish_count", 0) or 0
                        ) + 1
                    else:
                        self.stats["localization_duplicate_timestamp_count"] = int(
                            self.stats.get("localization_duplicate_timestamp_count", 0) or 0
                        ) + 1
                    self._record_topic_publish_stats(
                        channel=self.localization_channel,
                        msg=loc,
                        sim_time_sec=ts_sec,
                        payload_count=1,
                        source="bridge_writer",
                        extra={
                            "fresh_sample": sample_reason == "fresh_sample",
                            "sample_reason": sample_reason,
                        },
                    )
                    phase_start_wall_s = time.time()
                    measured = self._read_measured_control()
                    self._record_publish_phase_timing(
                        "carla_feedback_read_control",
                        start_wall_s=phase_start_wall_s,
                        end_wall_s=time.time(),
                        target_period_s=period,
                    )
                    phase_start_wall_s = time.time()
                    ch = self._odom_to_chassis(ts_sec, vel_xyz, measured)
                    self.chassis_writer.write(ch)
                    self._record_publish_phase_timing(
                        "chassis_build_and_write",
                        start_wall_s=phase_start_wall_s,
                        end_wall_s=time.time(),
                        target_period_s=period,
                    )
                    self.stats["chassis_count"] += 1
                    if sample_reason == "fresh_sample":
                        self.stats["chassis_fresh_publish_count"] = int(
                            self.stats.get("chassis_fresh_publish_count", 0) or 0
                        ) + 1
                    else:
                        self.stats["chassis_duplicate_timestamp_count"] = int(
                            self.stats.get("chassis_duplicate_timestamp_count", 0) or 0
                        ) + 1
                    self._record_topic_publish_stats(
                        channel=self.chassis_channel,
                        msg=ch,
                        sim_time_sec=ts_sec,
                        payload_count=1,
                        source="bridge_writer",
                        extra={
                            "fresh_sample": sample_reason == "fresh_sample",
                            "sample_reason": sample_reason,
                        },
                    )
                    self.stats["last_gt_state_publish"] = {
                        "schema_version": "apollo_gt_state_publish.v1",
                        "publish_wall_time_sec": time.time(),
                        "sim_time_sec": _finite_or_none(ts_sec),
                        "world_frame": self._latest_world_frame,
                        "sample_reason": sample_reason,
                        "fresh_sample": sample_reason == "fresh_sample",
                        "localization_sequence_num": self._header_sequence_num(loc),
                        "chassis_sequence_num": self._header_sequence_num(ch),
                        "localization_header_timestamp_sec": _finite_or_none(
                            self._header_timestamp_sec(loc)
                        ),
                        "chassis_header_timestamp_sec": _finite_or_none(
                            self._header_timestamp_sec(ch)
                        ),
                    }
                    ex = float(pose_info["map_x"])
                    ey = float(pose_info["map_y"])
                    if self._should_publish_obstacles(ts_sec):
                        obstacle_period = 1.0 / max(self.obstacle_publish_rate_hz, 1e-3)
                        phase_start_wall_s = time.time()
                        obs_msg, obs_count = self._objects_to_obstacles(snapshot, ts_sec, (ex, ey))
                        self._record_publish_phase_timing(
                            "objects_to_obstacles",
                            start_wall_s=phase_start_wall_s,
                            end_wall_s=time.time(),
                            target_period_s=obstacle_period,
                        )
                        phase_start_wall_s = time.time()
                        self.obs_writer.write(obs_msg)
                        self._record_publish_phase_timing(
                            "cyber_write_obstacles",
                            start_wall_s=phase_start_wall_s,
                            end_wall_s=time.time(),
                            target_period_s=obstacle_period,
                        )
                        self.stats["obstacle_message_count"] = int(
                            self.stats.get("obstacle_message_count", 0) or 0
                        ) + 1
                        self.stats["obstacle_object_count"] = int(
                            self.stats.get("obstacle_object_count", 0) or 0
                        ) + int(obs_count)
                        if int(obs_count) <= 0:
                            self.stats["obstacle_empty_message_count"] = int(
                                self.stats.get("obstacle_empty_message_count", 0) or 0
                            ) + 1
                        self.stats["obstacles_count"] = int(
                            self.stats.get("obstacle_object_count", 0) or 0
                        )
                        self.stats["last_obstacles_count"] = int(obs_count)
                        self._record_topic_publish_stats(
                            channel=self.obstacles_channel,
                            msg=obs_msg,
                            sim_time_sec=ts_sec,
                            payload_count=int(obs_count),
                            source="bridge_writer",
                            extra={
                                "obstacle_count": int(obs_count),
                                "empty_message": int(obs_count) <= 0,
                                "fresh_sample": sample_reason == "fresh_sample",
                                "sample_reason": sample_reason,
                            },
                        )
                    else:
                        self.stats["obstacle_publish_rate_limited_skip_count"] = int(
                            self.stats.get("obstacle_publish_rate_limited_skip_count", 0) or 0
                        ) + 1
                    loop_published_gt = True
                    debug_row_build_start_wall_s = time.time()
                    desired_in = self.stats.get("last_control_in", {}) or {}
                    desired_out = self.stats.get("last_control_out", {}) or {}
                    speed_mps = math.sqrt(sum(v * v for v in vel_xyz))
                    self._latest_speed_mps = float(speed_mps)
                    self._max_speed_mps = max(self._max_speed_mps, self._latest_speed_mps)
                    pose_debug.update(
                        {
                            "localization_speed_mps": float(speed_mps),
                            "chassis_speed_mps": float(speed_mps),
                            "velocity_norm_vs_chassis_speed_mps": 0.0,
                            "chassis_header_timestamp_sec": ts_sec,
                            "localization_chassis_timestamp_delta_ms": 0.0,
                        }
                    )
                    self.stats["localization"].update(
                        {
                            "localization_speed_mps": float(speed_mps),
                            "chassis_speed_mps": float(speed_mps),
                            "velocity_norm_vs_chassis_speed_mps": 0.0,
                            "localization_chassis_timestamp_delta_ms": 0.0,
                        }
                    )
                    phase_start_wall_s = time.time()
                    nearest_obs = self._nearest_obstacle_distance(snapshot, (ex, ey))
                    self._record_publish_phase_timing(
                        "nearest_obstacle_distance",
                        start_wall_s=phase_start_wall_s,
                        end_wall_s=time.time(),
                        target_period_s=period,
                    )
                    front_status = self._front_obstacle_behavior_status()
                    front_gap = front_status.get("last_gap", {}) or {}
                    front_actor = None
                    if self.front_obstacle_actor_probe_enabled and self.carla_feedback is not None:
                        phase_start_wall_s = time.time()
                        front_actor = self.carla_feedback.find_vehicle_by_roles(self.front_obstacle_role_names)
                        self._record_publish_phase_timing(
                            "front_obstacle_actor_probe",
                            start_wall_s=phase_start_wall_s,
                            end_wall_s=time.time(),
                            target_period_s=period,
                        )
                    front_actor_id = None
                    front_actor_role = ""
                    front_actor_x = None
                    front_actor_y = None
                    front_actor_yaw_deg = None
                    front_actor_speed_mps = None
                    front_actor_length_m = None
                    front_actor_width_m = None
                    front_actor_height_m = None
                    front_actor_dimension_source = "missing"
                    front_actor_dimension_warnings: List[str] = []
                    front_actor_type_id = None
                    if front_actor is not None:
                        try:
                            front_actor_id = int(getattr(front_actor, "id"))
                        except Exception:
                            front_actor_id = None
                        try:
                            attrs = getattr(front_actor, "attributes", {}) or {}
                            front_actor_role = str(attrs.get("role_name", "") or "")
                            front_actor_type_id = str(
                                getattr(front_actor, "type_id", "")
                                or attrs.get("type_id", "")
                                or attrs.get("base_type", "")
                                or ""
                            )
                        except Exception:
                            front_actor_role = ""
                            front_actor_type_id = None
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
                        dimensions = self._front_actor_dimensions(front_actor_id, front_actor)
                        front_actor_length_m = dimensions.get("length")
                        front_actor_width_m = dimensions.get("width")
                        front_actor_height_m = dimensions.get("height")
                        front_actor_dimension_source = str(dimensions.get("source") or "missing")
                        front_actor_dimension_warnings = list(dimensions.get("warnings") or [])
                    def desired_point_distance(prefix: str) -> Optional[float]:
                        px = _finite_or_none(desired_in.get(f"{prefix}_x"))
                        py = _finite_or_none(desired_in.get(f"{prefix}_y"))
                        if px is None or py is None:
                            return None
                        return float(math.hypot(float(px) - ex, float(py) - ey))

                    matched_point_distance = desired_point_distance("debug_simple_lon_matched_point")
                    if matched_point_distance is None:
                        matched_point_distance = desired_point_distance("debug_simple_mpc_matched_point")
                    target_point_distance = desired_point_distance("debug_simple_lat_target_point")
                    chassis_feedback = (
                        (self.stats.get("last_control_feedback", {}) or {}).get("chassis", {})
                        or {}
                    )
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
                        "localization_header_timestamp_sec": self._coerce_float(
                            pose_debug.get("localization_header_timestamp_sec"),
                            float("nan"),
                            "pose_debug.localization_header_timestamp_sec",
                            "publish_row",
                        ),
                        "localization_measurement_time": self._coerce_float(
                            pose_debug.get("localization_measurement_time"),
                            float("nan"),
                            "pose_debug.localization_measurement_time",
                            "publish_row",
                        ),
                        "localization_sequence_num": pose_debug.get("localization_sequence_num"),
                        "localization_module_name": pose_debug.get("localization_module_name"),
                        "localization_frame_id": pose_debug.get("localization_frame_id"),
                        "localization_carla_frame_id": pose_debug.get("localization_carla_frame_id"),
                        "localization_time_base": pose_debug.get("localization_time_base"),
                        "localization_heading": self._coerce_float(
                            pose_debug.get("localization_heading"), float("nan"), "pose_debug.localization_heading", "publish_row"
                        ),
                        "localization_orientation_yaw": self._coerce_float(
                            pose_debug.get("localization_orientation_yaw"),
                            float("nan"),
                            "pose_debug.localization_orientation_yaw",
                            "publish_row",
                        ),
                        "localization_orientation_qx": self._coerce_float(
                            pose_debug.get("localization_orientation_qx"),
                            float("nan"),
                            "pose_debug.localization_orientation_qx",
                            "publish_row",
                        ),
                        "localization_orientation_qy": self._coerce_float(
                            pose_debug.get("localization_orientation_qy"),
                            float("nan"),
                            "pose_debug.localization_orientation_qy",
                            "publish_row",
                        ),
                        "localization_orientation_qz": self._coerce_float(
                            pose_debug.get("localization_orientation_qz"),
                            float("nan"),
                            "pose_debug.localization_orientation_qz",
                            "publish_row",
                        ),
                        "localization_orientation_qw": self._coerce_float(
                            pose_debug.get("localization_orientation_qw"),
                            float("nan"),
                            "pose_debug.localization_orientation_qw",
                            "publish_row",
                        ),
                        "decoded_orientation_heading": self._coerce_float(
                            pose_debug.get("decoded_orientation_heading"),
                            float("nan"),
                            "pose_debug.decoded_orientation_heading",
                            "publish_row",
                        ),
                        "decoded_orientation_heading_diff_rad": self._coerce_float(
                            pose_debug.get("decoded_orientation_heading_diff_rad"),
                            float("nan"),
                            "pose_debug.decoded_orientation_heading_diff_rad",
                            "publish_row",
                        ),
                        "orientation_heading_diff_rad": self._coerce_float(
                            pose_debug.get("orientation_heading_diff_rad"),
                            float("nan"),
                            "pose_debug.orientation_heading_diff_rad",
                            "publish_row",
                        ),
                        "heading_source": pose_debug.get("heading_source"),
                        "orientation_convention": pose_debug.get("orientation_convention"),
                        "ego_yaw_rate_rad_s": self._coerce_float(
                            pose_debug.get("ego_yaw_rate_rad_s"),
                            float("nan"),
                            "pose_debug.ego_yaw_rate_rad_s",
                            "publish_row",
                        ),
                        "localization_yaw_rate_rad_s": self._coerce_float(
                            pose_debug.get("localization_yaw_rate_rad_s"),
                            float("nan"),
                            "pose_debug.localization_yaw_rate_rad_s",
                            "publish_row",
                        ),
                        "localization_angular_velocity_z_rad_s": self._coerce_float(
                            pose_debug.get("localization_angular_velocity_z_rad_s"),
                            float("nan"),
                            "pose_debug.localization_angular_velocity_z_rad_s",
                            "publish_row",
                        ),
                        "localization_angular_velocity_unit": pose_debug.get("localization_angular_velocity_unit"),
                        "angular_velocity_source": pose_debug.get("angular_velocity_source"),
                        "linear_acceleration_x": self._coerce_float(
                            pose_debug.get("linear_acceleration_x"),
                            float("nan"),
                            "pose_debug.linear_acceleration_x",
                            "publish_row",
                        ),
                        "linear_acceleration_y": self._coerce_float(
                            pose_debug.get("linear_acceleration_y"),
                            float("nan"),
                            "pose_debug.linear_acceleration_y",
                            "publish_row",
                        ),
                        "linear_acceleration_z": self._coerce_float(
                            pose_debug.get("linear_acceleration_z"),
                            float("nan"),
                            "pose_debug.linear_acceleration_z",
                            "publish_row",
                        ),
                        "linear_acceleration_vrf_x": self._coerce_float(
                            pose_debug.get("linear_acceleration_vrf_x"),
                            float("nan"),
                            "pose_debug.linear_acceleration_vrf_x",
                            "publish_row",
                        ),
                        "linear_acceleration_vrf_y": self._coerce_float(
                            pose_debug.get("linear_acceleration_vrf_y"),
                            float("nan"),
                            "pose_debug.linear_acceleration_vrf_y",
                            "publish_row",
                        ),
                        "linear_acceleration_vrf_z": self._coerce_float(
                            pose_debug.get("linear_acceleration_vrf_z"),
                            float("nan"),
                            "pose_debug.linear_acceleration_vrf_z",
                            "publish_row",
                        ),
                        "linear_acceleration_available": pose_debug.get("linear_acceleration_available"),
                        "linear_acceleration_vrf_available": pose_debug.get("linear_acceleration_vrf_available"),
                        "angular_velocity_vrf_available": pose_debug.get("angular_velocity_vrf_available"),
                        "acceleration_source": pose_debug.get("acceleration_source"),
                        "localization_uncertainty_policy": pose_debug.get("localization_uncertainty_policy"),
                        "localization_msf_status_policy": pose_debug.get("localization_msf_status_policy"),
                        "localization_sensor_status_policy": pose_debug.get("localization_sensor_status_policy"),
                        "localization_speed_mps": self._coerce_float(
                            pose_debug.get("localization_speed_mps"),
                            float("nan"),
                            "pose_debug.localization_speed_mps",
                            "publish_row",
                        ),
                        "chassis_speed_mps": self._coerce_float(
                            pose_debug.get("chassis_speed_mps"),
                            float("nan"),
                            "pose_debug.chassis_speed_mps",
                            "publish_row",
                        ),
                        "velocity_norm_vs_chassis_speed_mps": self._coerce_float(
                            pose_debug.get("velocity_norm_vs_chassis_speed_mps"),
                            float("nan"),
                            "pose_debug.velocity_norm_vs_chassis_speed_mps",
                            "publish_row",
                        ),
                        "chassis_header_timestamp_sec": self._coerce_float(
                            pose_debug.get("chassis_header_timestamp_sec"),
                            float("nan"),
                            "pose_debug.chassis_header_timestamp_sec",
                            "publish_row",
                        ),
                        "localization_chassis_timestamp_delta_ms": self._coerce_float(
                            pose_debug.get("localization_chassis_timestamp_delta_ms"),
                            float("nan"),
                            "pose_debug.localization_chassis_timestamp_delta_ms",
                            "publish_row",
                        ),
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
                        "apollo_debug_simple_lat_target_point_x": self._coerce_float(
                            desired_in.get("debug_simple_lat_target_point_x"),
                            float("nan"),
                            "desired_in.debug_simple_lat_target_point_x",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lat_target_point_y": self._coerce_float(
                            desired_in.get("debug_simple_lat_target_point_y"),
                            float("nan"),
                            "desired_in.debug_simple_lat_target_point_y",
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
                        "apollo_debug_simple_lon_station_reference_m": self._coerce_float(
                            desired_in.get("debug_simple_lon_station_reference_m"),
                            float("nan"),
                            "desired_in.debug_simple_lon_station_reference_m",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lon_station_error_m": self._coerce_float(
                            desired_in.get("debug_simple_lon_station_error_m"),
                            float("nan"),
                            "desired_in.debug_simple_lon_station_error_m",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lon_station_error_limited_m": self._coerce_float(
                            desired_in.get("debug_simple_lon_station_error_limited_m"),
                            float("nan"),
                            "desired_in.debug_simple_lon_station_error_limited_m",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lon_preview_station_error_m": self._coerce_float(
                            desired_in.get("debug_simple_lon_preview_station_error_m"),
                            float("nan"),
                            "desired_in.debug_simple_lon_preview_station_error_m",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lon_current_station_m": self._coerce_float(
                            desired_in.get("debug_simple_lon_current_station_m"),
                            float("nan"),
                            "desired_in.debug_simple_lon_current_station_m",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lon_path_remain_m": self._coerce_float(
                            desired_in.get("debug_simple_lon_path_remain_m"),
                            float("nan"),
                            "desired_in.debug_simple_lon_path_remain_m",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lon_speed_reference_mps": self._coerce_float(
                            desired_in.get("debug_simple_lon_speed_reference_mps"),
                            float("nan"),
                            "desired_in.debug_simple_lon_speed_reference_mps",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lon_speed_error_mps": self._coerce_float(
                            desired_in.get("debug_simple_lon_speed_error_mps"),
                            float("nan"),
                            "desired_in.debug_simple_lon_speed_error_mps",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lon_current_speed_mps": self._coerce_float(
                            desired_in.get("debug_simple_lon_current_speed_mps"),
                            float("nan"),
                            "desired_in.debug_simple_lon_current_speed_mps",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lon_speed_lookup_mps": self._coerce_float(
                            desired_in.get("debug_simple_lon_speed_lookup_mps"),
                            float("nan"),
                            "desired_in.debug_simple_lon_speed_lookup_mps",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lon_speed_offset_mps": self._coerce_float(
                            desired_in.get("debug_simple_lon_speed_offset_mps"),
                            float("nan"),
                            "desired_in.debug_simple_lon_speed_offset_mps",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lon_acceleration_cmd_closeloop_mps2": self._coerce_float(
                            desired_in.get("debug_simple_lon_acceleration_cmd_closeloop_mps2"),
                            float("nan"),
                            "desired_in.debug_simple_lon_acceleration_cmd_closeloop_mps2",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lon_acceleration_cmd_mps2": self._coerce_float(
                            desired_in.get("debug_simple_lon_acceleration_cmd_mps2"),
                            float("nan"),
                            "desired_in.debug_simple_lon_acceleration_cmd_mps2",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lon_acceleration_lookup_mps2": self._coerce_float(
                            desired_in.get("debug_simple_lon_acceleration_lookup_mps2"),
                            float("nan"),
                            "desired_in.debug_simple_lon_acceleration_lookup_mps2",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lon_acceleration_reference_mps2": self._coerce_float(
                            desired_in.get("debug_simple_lon_acceleration_reference_mps2"),
                            float("nan"),
                            "desired_in.debug_simple_lon_acceleration_reference_mps2",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lon_current_acceleration_mps2": self._coerce_float(
                            desired_in.get("debug_simple_lon_current_acceleration_mps2"),
                            float("nan"),
                            "desired_in.debug_simple_lon_current_acceleration_mps2",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lon_acceleration_error_mps2": self._coerce_float(
                            desired_in.get("debug_simple_lon_acceleration_error_mps2"),
                            float("nan"),
                            "desired_in.debug_simple_lon_acceleration_error_mps2",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lon_jerk_reference_mps3": self._coerce_float(
                            desired_in.get("debug_simple_lon_jerk_reference_mps3"),
                            float("nan"),
                            "desired_in.debug_simple_lon_jerk_reference_mps3",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lon_current_jerk_mps3": self._coerce_float(
                            desired_in.get("debug_simple_lon_current_jerk_mps3"),
                            float("nan"),
                            "desired_in.debug_simple_lon_current_jerk_mps3",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lon_jerk_error_mps3": self._coerce_float(
                            desired_in.get("debug_simple_lon_jerk_error_mps3"),
                            float("nan"),
                            "desired_in.debug_simple_lon_jerk_error_mps3",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lon_throttle_cmd_pct": self._coerce_float(
                            desired_in.get("debug_simple_lon_throttle_cmd_pct"),
                            float("nan"),
                            "desired_in.debug_simple_lon_throttle_cmd_pct",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lon_brake_cmd_pct": self._coerce_float(
                            desired_in.get("debug_simple_lon_brake_cmd_pct"),
                            float("nan"),
                            "desired_in.debug_simple_lon_brake_cmd_pct",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lon_is_full_stop": bool(
                            desired_in.get("debug_simple_lon_is_full_stop", False)
                        ),
                        "apollo_debug_simple_lon_matched_point_s": self._coerce_float(
                            desired_in.get("debug_simple_lon_matched_point_s"),
                            float("nan"),
                            "desired_in.debug_simple_lon_matched_point_s",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lon_matched_point_x": self._coerce_float(
                            desired_in.get("debug_simple_lon_matched_point_x"),
                            float("nan"),
                            "desired_in.debug_simple_lon_matched_point_x",
                            "publish_row",
                        ),
                        "apollo_debug_simple_lon_matched_point_y": self._coerce_float(
                            desired_in.get("debug_simple_lon_matched_point_y"),
                            float("nan"),
                            "desired_in.debug_simple_lon_matched_point_y",
                            "publish_row",
                        ),
                        "apollo_debug_simple_mpc_matched_point_s": self._coerce_float(
                            desired_in.get("debug_simple_mpc_matched_point_s"),
                            float("nan"),
                            "desired_in.debug_simple_mpc_matched_point_s",
                            "publish_row",
                        ),
                        "apollo_debug_simple_mpc_matched_point_x": self._coerce_float(
                            desired_in.get("debug_simple_mpc_matched_point_x"),
                            float("nan"),
                            "desired_in.debug_simple_mpc_matched_point_x",
                            "publish_row",
                        ),
                        "apollo_debug_simple_mpc_matched_point_y": self._coerce_float(
                            desired_in.get("debug_simple_mpc_matched_point_y"),
                            float("nan"),
                            "desired_in.debug_simple_mpc_matched_point_y",
                            "publish_row",
                        ),
                        "apollo_matched_point_distance": matched_point_distance
                        if matched_point_distance is not None
                        else float("nan"),
                        "apollo_target_point_distance": target_point_distance
                        if target_point_distance is not None
                        else float("nan"),
                        "apollo_mode": desired_in.get("mode", ""),
                        "apollo_gear": desired_in.get("gear", ""),
                        "apollo_estop": desired_in.get("estop", False),
                        "driving_mode": chassis_feedback.get("driving_mode"),
                        "driving_mode_value": chassis_feedback.get("driving_mode_value"),
                        "gear_location": chassis_feedback.get("gear_location"),
                        "gear_location_value": chassis_feedback.get("gear_location_value"),
                        "chassis_error_code": chassis_feedback.get("error_code"),
                        "chassis_error_code_value": chassis_feedback.get("error_code_value"),
                        "chassis_feedback_source": chassis_feedback.get("feedback_source"),
                        "chassis_control_feedback_available": chassis_feedback.get("feedback_available"),
                        "throttle_applied": self._coerce_float(
                            desired_out.get("throttle"), float("nan"), "desired_out.throttle", "publish_row"
                        ),
                        "brake_applied": self._coerce_float(
                            desired_out.get("brake"), float("nan"), "desired_out.brake", "publish_row"
                        ),
                        "carla_steer_applied": self._coerce_float(
                            desired_out.get("steer"), float("nan"), "desired_out.steer", "publish_row"
                        ),
                        "throttle_feedback": self._coerce_float(
                            measured.get("throttle"), float("nan"), "measured.throttle", "publish_row"
                        ),
                        "brake_feedback": self._coerce_float(
                            measured.get("brake"), float("nan"), "measured.brake", "publish_row"
                        ),
                        "steer_feedback": self._coerce_float(
                            measured.get("steer"), float("nan"), "measured.steer", "publish_row"
                        ),
                        "chassis_throttle_percentage": self._coerce_float(
                            chassis_feedback.get("throttle_percentage"),
                            float("nan"),
                            "chassis_feedback.throttle_percentage",
                            "publish_row",
                        ),
                        "chassis_brake_percentage": self._coerce_float(
                            chassis_feedback.get("brake_percentage"),
                            float("nan"),
                            "chassis_feedback.brake_percentage",
                            "publish_row",
                        ),
                        "chassis_steering_percentage": self._coerce_float(
                            chassis_feedback.get("steering_percentage"),
                            float("nan"),
                            "chassis_feedback.steering_percentage",
                            "publish_row",
                        ),
                        "chassis_steering_percentage_frame": chassis_feedback.get(
                            "steering_percentage_frame", ""
                        ),
                        "chassis_carla_steering_percentage": self._coerce_float(
                            chassis_feedback.get("carla_steering_percentage"),
                            float("nan"),
                            "chassis_feedback.carla_steering_percentage",
                            "publish_row",
                        ),
                        "chassis_steering_feedback_sign": self._coerce_float(
                            chassis_feedback.get("steering_feedback_sign"),
                            float("nan"),
                            "chassis_feedback.steering_feedback_sign",
                            "publish_row",
                        ),
                        "chassis_steering_percentage_cmd": self._coerce_float(
                            chassis_feedback.get("steering_percentage_cmd"),
                            float("nan"),
                            "chassis_feedback.steering_percentage_cmd",
                            "publish_row",
                        ),
                        "chassis_carla_steering_percentage_cmd": self._coerce_float(
                            chassis_feedback.get("carla_steering_percentage_cmd"),
                            float("nan"),
                            "chassis_feedback.carla_steering_percentage_cmd",
                            "publish_row",
                        ),
                        "localization_timestamp": ts_sec,
                        "chassis_timestamp": ts_sec,
                        "planning_timestamp": self._coerce_float(
                            desired_out.get("planning_timestamp"),
                            float("nan"),
                            "desired_out.planning_timestamp",
                            "publish_row",
                        ),
                        "control_rx_timestamp": self._coerce_float(
                            desired_out.get("control_rx_timestamp"),
                            float("nan"),
                            "desired_out.control_rx_timestamp",
                            "publish_row",
                        ),
                        "control_timestamp": self._coerce_float(
                            desired_out.get("control_timestamp"),
                            float("nan"),
                            "desired_out.control_timestamp",
                            "publish_row",
                        ),
                        "control_latency_ms": self._coerce_float(
                            desired_out.get("control_latency_ms"),
                            float("nan"),
                            "desired_out.control_latency_ms",
                            "publish_row",
                        ),
                        "planning_message_age_ms": self._coerce_float(
                            desired_out.get("planning_message_age_ms"),
                            float("nan"),
                            "desired_out.planning_message_age_ms",
                            "publish_row",
                        ),
                        "control_message_age_ms": self._coerce_float(
                            desired_out.get("control_message_age_ms"),
                            float("nan"),
                            "desired_out.control_message_age_ms",
                            "publish_row",
                        ),
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
                        "steer_scale": self._coerce_float(
                            desired_out.get("steer_scale", self.steer_scale),
                            float("nan"),
                            "desired_out.steer_scale",
                            "publish_row",
                        ),
                        "steering_sign": self._coerce_float(
                            desired_out.get("steer_sign", self.steer_sign),
                            float("nan"),
                            "desired_out.steer_sign",
                            "publish_row",
                        ),
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
                        "base_mapped_carla_steer_cmd": self._coerce_float(
                            desired_out.get("base_mapped_carla_steer_cmd"),
                            float("nan"),
                            "desired_out.base_mapped_carla_steer_cmd",
                            "publish_row",
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
                    self._record_publish_phase_timing(
                        "debug_row_build",
                        start_wall_s=debug_row_build_start_wall_s,
                        end_wall_s=time.time(),
                        target_period_s=period,
                    )
                    phase_start_wall_s = time.time()
                    artifact_subphase_start_wall_s = time.time()
                    self._record_obstacle_contract_event(
                        {
                            "timestamp": ts_sec,
                            **self._timing_snapshot(event_wall_time_sec=ts_sec),
                            "front_obstacle_visible": front_status.get("visible"),
                            "front_obstacle_mode": front_status.get("mode"),
                            "front_obstacle_role_names": list(front_status.get("role_names", []) or []),
                            "front_obstacle_actor_found": front_actor is not None,
                            "front_obstacle_actor_probe_enabled": self.front_obstacle_actor_probe_enabled,
                            "front_obstacle_actor_id": front_actor_id,
                            "front_obstacle_actor_role": front_actor_role,
                            "ego_actor_id": self._ego_actor_id_for_obstacle_contract(),
                            "carla_actor_id": front_actor_id,
                            "apollo_perception_id": front_actor_id,
                            "blueprint_id": front_actor_type_id,
                            "type": _actor_type_label_from_type_id(front_actor_type_id),
                            "obstacle_type_source": "carla_actor_type_id" if front_actor_type_id else "missing",
                            "is_ego": False,
                            "front_obstacle_actor_x": front_actor_x,
                            "front_obstacle_actor_y": front_actor_y,
                            "front_obstacle_actor_yaw_deg": front_actor_yaw_deg,
                            "front_obstacle_actor_speed_mps": front_actor_speed_mps,
                            "frame_transform_checked": front_actor is not None,
                            "theta_frame_checked": front_actor is not None,
                            "position_frame_apollo_map": front_actor is not None,
                            "length": _finite_or_none(front_actor_length_m),
                            "width": _finite_or_none(front_actor_width_m),
                            "height": _finite_or_none(front_actor_height_m),
                            "dimension_source": front_actor_dimension_source,
                            "dimension_warnings": front_actor_dimension_warnings,
                            "velocity_source": "carla_actor_state" if front_actor is not None else "missing",
                            "velocity": {
                                "x": _finite_or_none(front_actor_speed_mps),
                                "y": 0.0,
                                "z": 0.0,
                            },
                            "dynamic": bool(front_actor_speed_mps and front_actor_speed_mps > 1e-3),
                            "actually_stationary": bool(front_actor_speed_mps is not None and front_actor_speed_mps <= 1e-3),
                            "tracking_time": max(0.1, ts_sec),
                            "front_obstacle_gap_lon_m": _finite_or_none(front_gap.get("lon_m")),
                            "front_obstacle_gap_lat_m": _finite_or_none(front_gap.get("lat_m")),
                            "front_obstacle_gap_distance_m": _finite_or_none(front_gap.get("distance_m")),
                            "nearest_obstacle_dist_m": nearest_obs,
                            "obstacle_cache_enabled": bool(front_status.get("cache_enabled")),
                            "obstacle_cache_hit_count": int(front_status.get("cache_hit_count", 0) or 0),
                            "published_obstacle_count": int(obs_count),
                        }
                    )
                    self._record_publish_phase_timing(
                        "artifact_obstacle_contract",
                        start_wall_s=artifact_subphase_start_wall_s,
                        end_wall_s=time.time(),
                        target_period_s=period,
                    )
                    artifact_subphase_start_wall_s = time.time()
                    self._write_debug_row(row)
                    self._record_publish_phase_timing(
                        "artifact_debug_timeseries",
                        start_wall_s=artifact_subphase_start_wall_s,
                        end_wall_s=time.time(),
                        target_period_s=period,
                    )
                    artifact_subphase_start_wall_s = time.time()
                    self._record_control_apply_trace(row, measured)
                    self._record_publish_phase_timing(
                        "artifact_control_apply_trace",
                        start_wall_s=artifact_subphase_start_wall_s,
                        end_wall_s=time.time(),
                        target_period_s=period,
                    )
                    artifact_subphase_start_wall_s = time.time()
                    self._record_apollo_reference_line_contract_event(row)
                    self._record_publish_phase_timing(
                        "artifact_reference_line_contract",
                        start_wall_s=artifact_subphase_start_wall_s,
                        end_wall_s=time.time(),
                        target_period_s=period,
                    )
                    artifact_subphase_start_wall_s = time.time()
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
                    self._record_publish_phase_timing(
                        "artifact_vehicle_response_csv",
                        start_wall_s=artifact_subphase_start_wall_s,
                        end_wall_s=time.time(),
                        target_period_s=period,
                    )
                    artifact_subphase_start_wall_s = time.time()
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
                    self._record_publish_phase_timing(
                        "artifact_geometry_debug_csv",
                        start_wall_s=artifact_subphase_start_wall_s,
                        end_wall_s=time.time(),
                        target_period_s=period,
                    )
                    self._record_publish_phase_timing(
                        "artifact_trace_writes",
                        start_wall_s=phase_start_wall_s,
                        end_wall_s=time.time(),
                        target_period_s=period,
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
                    phase_start_wall_s = time.time()
                    self._maybe_publish_traffic_lights(ts_sec)
                    self._record_publish_phase_timing(
                        "traffic_light_publish",
                        start_wall_s=phase_start_wall_s,
                        end_wall_s=time.time(),
                        target_period_s=period,
                    )
                    phase_start_wall_s = time.time()
                    self._maybe_send_routing_request(snapshot, odom, ts_sec, pose_info, speed_mps=speed_mps)
                    self._record_publish_phase_timing(
                        "routing_request_check",
                        start_wall_s=phase_start_wall_s,
                        end_wall_s=time.time(),
                        target_period_s=period,
                    )
                    if not float(self.stats.get("first_publish_ts_sec", 0.0) or 0.0):
                        self.stats["first_publish_ts_sec"] = ts_sec
                    if not float(self.stats.get("first_publish_wall_ts_sec", 0.0) or 0.0):
                        self.stats["first_publish_wall_ts_sec"] = now_wall
                    self.stats["last_publish_ts_sec"] = ts_sec
                    self.stats["last_publish_wall_ts_sec"] = now_wall
                    first_sim = float(self.stats.get("first_publish_ts_sec", 0.0) or 0.0)
                    first_wall = float(self.stats.get("first_publish_wall_ts_sec", 0.0) or 0.0)
                    self.stats["publish_elapsed_sim_sec"] = max(0.0, float(ts_sec) - first_sim)
                    self.stats["publish_elapsed_wall_sec"] = max(0.0, float(now_wall) - first_wall)
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
            publish_loop_duration_s = max(0.0, now - loop_start_wall_s)
            self._record_publish_loop_timing(
                start_wall_s=loop_start_wall_s,
                end_wall_s=now,
                target_period_s=period,
                published_gt=loop_published_gt,
                phase="publish_gt" if loop_published_gt else "idle_or_error",
            )
            if publish_loop_duration_s > period:
                self._record_publish_gap_trace(
                    snapshot=snapshot if isinstance(snapshot, dict) else {},
                    odom=odom,
                    expected_publish=True,
                    published_localization=bool(loop_published_gt),
                    published_chassis=bool(loop_published_gt),
                    skip_reason="publish_loop_overrun",
                    loop_start_wall_s=loop_start_wall_s,
                    publish_loop_duration_s=publish_loop_duration_s,
                )
            maybe_write_periodic_stats(now)
            sleep_until_next_publish_cycle()

        self._write_stats()
        try:
            self._flush_artifact_buffers(close=True)
        except Exception:
            pass
        return 0

    def shutdown(self) -> None:
        self.stop_event.set()
        try:
            self._flush_artifact_buffers(close=True)
        except Exception:
            pass
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
            "raw_routing_response_channel": "/apollo/raw_routing_response",
            "traffic_light_channel": "/apollo/perception/traffic_light",
            "planning_channel": "/apollo/planning",
        },
        "bridge": {
            "publish_rate_hz": 20.0,
            "obstacle_publish_rate_hz": 10.0,
            "max_obstacles": 64,
            "radius_m": 120.0,
            "localization_back_offset_m": "auto",
            "vehicle_reference_path": "configs/vehicles/ego_vehicle_reference.verified.yaml",
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
                "relay_raw_routing_response_to_planning": True,
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
                "throttle_brake_mutual_exclusion_enabled": True,
                "throttle_brake_hysteresis_frames": 2,
                "throttle_brake_min_command": 0.05,
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
