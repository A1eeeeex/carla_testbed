#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import re
import signal
import sys
import threading
import time
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
    from vision_msgs.msg import Detection3DArray
except Exception:
    Detection3DArray = None

try:
    from visualization_msgs.msg import MarkerArray
except Exception:
    MarkerArray = None


def _clamp(val: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, val))


def _quat_to_yaw(qx: float, qy: float, qz: float, qw: float) -> float:
    siny_cosp = 2.0 * (qw * qz + qx * qy)
    cosy_cosp = 1.0 - 2.0 * (qy * qy + qz * qz)
    return math.atan2(siny_cosp, cosy_cosp)


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
        self.control_out_type = str(control_out_type or "ackermann").lower()
        if self.control_out_type == "ackermann":
            if AckermannDriveStamped is None:
                raise RuntimeError("ackermann_msgs is not available, cannot publish ackermann control")
            self.control_pub = self.create_publisher(AckermannDriveStamped, control_out_topic, 10)
        elif self.control_out_type == "twist":
            if Twist is None:
                raise RuntimeError("geometry_msgs/Twist is not available, cannot publish twist control")
            self.control_pub = self.create_publisher(Twist, control_out_topic, 10)
        else:
            raise RuntimeError(f"unsupported control_out_type={self.control_out_type}, expected ackermann|twist")
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

    def _on_objects3d(self, msg: Any) -> None:
        with self.lock:
            self.latest_objects3d = msg

    def _on_markers(self, msg: Any) -> None:
        with self.lock:
            self.latest_markers = msg

    def _on_objects_json(self, msg: String) -> None:
        with self.lock:
            self.latest_objects_json = msg.data

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "odom": self.latest_odom,
                "objects3d": self.latest_objects3d,
                "markers": self.latest_markers,
                "objects_json": self.latest_objects_json,
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
        self.seq = 0
        self.last_control = {"throttle": 0.0, "brake": 0.0, "steer_pct": 0.0}
        self.stats = {
            "last_publish_ts_sec": 0.0,
            "loc_count": 0,
            "chassis_count": 0,
            "obstacles_count": 0,
            "control_rx_count": 0,
            "control_tx_count": 0,
            "routing_request_count": 0,
            "routing_response_count": 0,
            "routing_success_count": 0,
            "routing_empty_count": 0,
            "routing_last_road_count": 0,
            "action_follow_count": 0,
            "lane_follow_count": 0,
            "last_error": "",
        }

        self.tf = Transform2D(**(((cfg.get("bridge", {}) or {}).get("carla_to_apollo", {})) or {}))
        self.publish_rate_hz = float((cfg.get("bridge", {}) or {}).get("publish_rate_hz", 20.0))
        self.max_obstacles = int((cfg.get("bridge", {}) or {}).get("max_obstacles", 64))
        self.radius_m = float((cfg.get("bridge", {}) or {}).get("radius_m", 120.0))
        ctrl_map = (cfg.get("bridge", {}) or {}).get("control_mapping", {}) or {}
        self.max_steer_angle = float(ctrl_map.get("max_steer_angle", 0.6))
        self.speed_gain = float(ctrl_map.get("speed_gain", 10.0))
        self.brake_gain = float(ctrl_map.get("brake_gain", 5.0))
        self.throttle_scale = float(ctrl_map.get("throttle_scale", 1.0))
        self.brake_scale = float(ctrl_map.get("brake_scale", 1.0))
        self.brake_deadzone = float(ctrl_map.get("brake_deadzone", 0.01))

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

        auto_routing_cfg = ((cfg.get("bridge", {}) or {}).get("auto_routing", {}) or {})
        self.auto_routing_enabled = bool(auto_routing_cfg.get("enabled", False))
        self.auto_routing_end_ahead_m = float(auto_routing_cfg.get("end_ahead_m", 80.0))
        self.auto_routing_resend_sec = float(auto_routing_cfg.get("resend_sec", 5.0))
        self.auto_routing_max_attempts = int(auto_routing_cfg.get("max_attempts", 5))
        self.auto_routing_target_speed = float(auto_routing_cfg.get("target_speed_mps", 8.0))
        self.auto_routing_freeze_after_success = bool(auto_routing_cfg.get("freeze_after_success", True))
        self.auto_routing_use_seed_heading = bool(auto_routing_cfg.get("use_seed_heading", True))
        self.auto_routing_clamp_to_map_bounds = bool(auto_routing_cfg.get("clamp_to_map_bounds", True))
        self.auto_routing_map_bounds_margin_m = float(auto_routing_cfg.get("map_bounds_margin_m", 2.0))
        self.auto_routing_sent = 0
        self.auto_routing_last_sent_ts = 0.0
        self.auto_routing_established = False
        self.auto_routing_seed_pose: Optional[Tuple[float, float, float]] = None
        self.map_bounds_xy: Optional[Tuple[float, float, float, float]] = None
        self._load_map_bounds(auto_routing_cfg)

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
            self.routing_writer = self._cyber_create_writer(
                self.routing_request_channel, self.routing_pb2.RoutingRequest
            )
            status_resp_type = (
                self.command_status_pb2.CommandStatus
                if self.command_status_pb2 is not None
                else empty_pb2.Empty
            )
            if self.action_pb2 is not None:
                self.action_client = self._cyber_create_client(
                    self.action_channel,
                    self.action_pb2.ActionCommand,
                    status_resp_type,
                )
            self.lane_follow_client = self._cyber_create_client(
                self.lane_follow_channel,
                self.lane_follow_pb2.LaneFollowCommand,
                status_resp_type,
            )
            self._cyber_create_reader(
                self.routing_response_channel, self.routing_pb2.RoutingResponse, self._on_routing_response
            )
        self._cyber_create_reader(self.control_channel, self.control_pb2.ControlCommand, self._on_control_cmd)
        self.cyber_spin_thread = None
        if hasattr(self.cyber_node, "spin"):
            self.cyber_spin_thread = threading.Thread(
                target=self.cyber_node.spin,
                name="cyber_spin",
                daemon=True,
            )

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

    def _odom_to_loc(self, odom: Odometry):
        loc = self.localization_pb2.LocalizationEstimate()
        sim_ts_sec = _stamp_to_sec(getattr(odom.header, "stamp", None))
        header_ts_sec = self._command_now_sec()
        self._fill_header(
            getattr(loc, "header", None), header_ts_sec, "tb_apollo10_gt_bridge"
        )

        pos = odom.pose.pose.position
        ori = odom.pose.pose.orientation
        vel = odom.twist.twist.linear
        ang = odom.twist.twist.angular
        yaw = _quat_to_yaw(ori.x, ori.y, ori.z, ori.w)
        x, y, z = self.tf.apply_position(float(pos.x), float(pos.y), float(pos.z))
        vx, vy, vz = self.tf.apply_vector(float(vel.x), float(vel.y), float(vel.z))
        wz = float(ang.z)
        yaw_ap = self.tf.apply_yaw(yaw)
        qx, qy, qz, qw = (float(ori.x), float(ori.y), float(ori.z), float(ori.w))

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
                _safe_set(pose.linear_velocity_vrf, "x", vx)
                _safe_set(pose.linear_velocity_vrf, "y", vy)
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
        return loc, header_ts_sec, (vx, vy, vz)

    def _odom_to_chassis(self, ts_sec: float, vel_xyz: Tuple[float, float, float]):
        ch = self.chassis_pb2.Chassis()
        self._fill_header(getattr(ch, "header", None), ts_sec, "tb_apollo10_gt_bridge")
        vx, vy, vz = vel_xyz
        speed = math.sqrt(vx * vx + vy * vy + vz * vz)
        _safe_set(ch, "speed_mps", speed)
        _safe_set(ch, "throttle_percentage", float(self.last_control["throttle"]) * 100.0)
        _safe_set(ch, "brake_percentage", float(self.last_control["brake"]) * 100.0)
        _safe_set(ch, "steering_percentage", float(self.last_control["steer_pct"]) * 100.0)
        if hasattr(ch.__class__, "COMPLETE_AUTO_DRIVE") and hasattr(ch, "driving_mode"):
            _safe_set(ch, "driving_mode", getattr(ch.__class__, "COMPLETE_AUTO_DRIVE"))
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

    def _objects_to_obstacles(self, snapshot: Dict[str, Any], ts_sec: float, ego_xy: Tuple[float, float]):
        msg = self.perception_pb2.PerceptionObstacles()
        self._fill_header(getattr(msg, "header", None), ts_sec, "tb_apollo10_gt_bridge")
        out = getattr(msg, "perception_obstacle", None)
        if out is None:
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
                if math.hypot(x - ego_xy[0], y - ego_xy[1]) > self.radius_m:
                    continue
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
                if math.hypot(x - ego_xy[0], y - ego_xy[1]) > self.radius_m:
                    continue
                cls = str(getattr(marker, "ns", "unknown"))
                self._append_obstacle(
                    out,
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
                    if math.hypot(x - ego_xy[0], y - ego_xy[1]) > self.radius_m:
                        continue
                    self._append_obstacle(
                        out,
                        obj_id=int(item.get("id", count + 1)),
                        cls_name=str(item.get("class", "unknown")),
                        x=x,
                        y=y,
                        z=z,
                        yaw=self.tf.apply_yaw(float(pose.get("yaw", 0.0))),
                        vx=0.0,
                        vy=0.0,
                        vz=0.0,
                        sx=float(size.get("x", 1.0)),
                        sy=float(size.get("y", 1.0)),
                        sz=float(size.get("z", 1.0)),
                    )
                    count += 1
            except Exception as exc:
                self.stats["last_error"] = f"objects_json parse error: {exc}"
        return msg, count

    def _on_control_cmd(self, cmd: Any) -> None:
        self.stats["control_rx_count"] += 1
        throttle_pct = float(getattr(cmd, "throttle", 0.0))
        brake_pct = float(getattr(cmd, "brake", 0.0))
        steer_pct = 0.0
        for key in ("steering_target", "steering", "steering_rate"):
            if hasattr(cmd, key):
                steer_pct = float(getattr(cmd, key))
                break
        throttle = _clamp((throttle_pct / 100.0) * self.throttle_scale, 0.0, 1.0)
        brake = _clamp((brake_pct / 100.0) * self.brake_scale, 0.0, 1.0)
        if brake < self.brake_deadzone:
            brake = 0.0
        steer = _clamp(steer_pct / 100.0, -1.0, 1.0)
        self.last_control = {"throttle": throttle, "brake": brake, "steer_pct": steer}

        speed_cmd = -brake * self.brake_gain if (brake > throttle and brake > 0.01) else throttle * self.speed_gain
        if self.node.control_out_type == "ackermann":
            msg = AckermannDriveStamped()
            msg.header.stamp = self.node.get_clock().now().to_msg()
            msg.header.frame_id = "base_link"
            msg.drive.speed = speed_cmd
            msg.drive.acceleration = 0.0
            msg.drive.steering_angle = steer * self.max_steer_angle
        else:
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
        else:
            self.stats["routing_empty_count"] += 1

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

    def _load_map_bounds(self, auto_routing_cfg: Dict[str, Any]) -> None:
        if not self.auto_routing_clamp_to_map_bounds:
            self.stats["map_bounds"] = {"enabled": False, "reason": "disabled"}
            return
        map_file_raw = str(auto_routing_cfg.get("map_file", "")).strip()
        if map_file_raw:
            map_file = Path(map_file_raw)
            if not map_file.is_absolute():
                map_file = (self.apollo_root / map_file).resolve()
        else:
            map_file = self._guess_map_file()
        if map_file is None or not map_file.exists():
            self.stats["map_bounds"] = {"enabled": False, "reason": "map_file_missing"}
            return
        try:
            text = map_file.read_text(errors="ignore")
            x_vals = [float(m.group(1)) for m in re.finditer(r"\bx:\s*(-?\d+(?:\.\d+)?)", text)]
            y_vals = [float(m.group(1)) for m in re.finditer(r"\by:\s*(-?\d+(?:\.\d+)?)", text)]
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

    def _routing_end_point(self, x0: float, y0: float, z0: float, yaw0: float) -> Tuple[float, float, float]:
        if self.auto_routing_seed_pose is None:
            self.auto_routing_seed_pose = (x0, y0, yaw0)
        seed_x, seed_y, seed_yaw = self.auto_routing_seed_pose
        base_yaw = seed_yaw if self.auto_routing_use_seed_heading else yaw0
        # Try a few directions progressively so route request doesn't get stuck on a single bad heading.
        d_primary = max(10.0, float(self.auto_routing_end_ahead_m))
        d_short = max(10.0, min(d_primary, d_primary * 0.5))
        candidates = [
            (d_primary, 0.0),
            (d_short, 0.0),
            (d_short, math.pi),
            (d_short, math.pi / 2.0),
            (d_short, -math.pi / 2.0),
            (d_primary, math.pi),
            (d_primary, math.pi / 2.0),
            (d_primary, -math.pi / 2.0),
        ]
        idx = max(0, self.auto_routing_sent) % len(candidates)
        dist, yaw_offset = candidates[idx]
        angle = base_yaw + yaw_offset
        base_x = seed_x if self.auto_routing_use_seed_heading else x0
        base_y = seed_y if self.auto_routing_use_seed_heading else y0
        x1 = base_x + dist * math.cos(angle)
        y1 = base_y + dist * math.sin(angle)
        x1, y1 = self._clamp_xy_to_bounds(x1, y1)
        return x1, y1, z0

    def _maybe_send_routing_request(self, odom: Odometry, ts_sec: float) -> None:
        if not self.auto_routing_enabled:
            return
        if self.routing_writer is None and self.lane_follow_client is None and self.action_client is None:
            return
        if self.auto_routing_freeze_after_success and self.auto_routing_established:
            return
        if self.auto_routing_sent >= self.auto_routing_max_attempts:
            return
        if self.auto_routing_sent > 0 and (ts_sec - self.auto_routing_last_sent_ts) < self.auto_routing_resend_sec:
            return

        pose = odom.pose.pose
        pos = pose.position
        ori = pose.orientation
        yaw = _quat_to_yaw(float(ori.x), float(ori.y), float(ori.z), float(ori.w))
        x0, y0, z0 = self.tf.apply_position(float(pos.x), float(pos.y), float(pos.z))
        x0, y0 = self._clamp_xy_to_bounds(x0, y0)
        yaw0 = self.tf.apply_yaw(yaw)
        x1, y1, z1 = self._routing_end_point(x0, y0, z0, yaw0)
        command_ts = self._command_now_sec()

        if self.action_client is not None:
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

        if self.lane_follow_client is not None:
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
                if lf_resp is not None and hasattr(lf_resp, "status"):
                    # CommandStatusType::ERROR == 2 in Apollo external command proto.
                    if int(getattr(lf_resp, "status", 0)) == 2:
                        self.stats["last_error"] = f"lane_follow error: {getattr(lf_resp, 'message', '')}"
                    else:
                        self.stats["last_error"] = ""
            except Exception as exc:
                self.stats["last_error"] = f"lane_follow request failed: {exc}"

        if self.routing_writer is not None:
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
        self.auto_routing_sent += 1
        self.auto_routing_last_sent_ts = ts_sec
        self.stats["routing_request_count"] += 1

    def _write_stats(self) -> None:
        self.stats_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.stats_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.stats, indent=2))
        tmp.replace(self.stats_path)

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
                odom = snapshot.get("odom")
                if odom is not None:
                    loc, ts_sec, vel_xyz = self._odom_to_loc(odom)
                    self.loc_writer.write(loc)
                    self.stats["loc_count"] += 1
                    ch = self._odom_to_chassis(ts_sec, vel_xyz)
                    self.chassis_writer.write(ch)
                    self.stats["chassis_count"] += 1
                    ego_pose = getattr(odom.pose.pose, "position", None)
                    ex, ey = (float(getattr(ego_pose, "x", 0.0)), float(getattr(ego_pose, "y", 0.0)))
                    obs_msg, obs_count = self._objects_to_obstacles(snapshot, ts_sec, (ex, ey))
                    self.obs_writer.write(obs_msg)
                    self.stats["obstacles_count"] += int(obs_count)
                    self._maybe_send_routing_request(odom, ts_sec)
                    self.stats["last_publish_ts_sec"] = ts_sec
            except Exception as exc:
                self.stats["last_error"] = f"publish loop error: {exc}"

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
            "control_out_type": "ackermann",
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
        },
        "bridge": {
            "publish_rate_hz": 20.0,
            "max_obstacles": 64,
            "radius_m": 120.0,
            "auto_routing": {
                "enabled": False,
                "end_ahead_m": 80.0,
                "resend_sec": 5.0,
                "max_attempts": 5,
                "target_speed_mps": 8.0,
            },
            "carla_to_apollo": {"tx": 0.0, "ty": 0.0, "tz": 0.0, "yaw_deg": 0.0},
            "control_mapping": {"max_steer_angle": 0.6, "speed_gain": 10.0, "brake_gain": 5.0},
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
