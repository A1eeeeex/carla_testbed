from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from carla_testbed.io.ros2_msg_builders import (
    build_image_msg,
    build_imu_msg,
    build_event_msg,
    build_navsatfix_msg,
    build_pointcloud2_msg,
    build_tf_static_msgs,
    to_ros_time,
)
from carla_testbed.io.ros2_qos import qos_from_contract


class ROS2BridgeAdapter:
    """
    Minimal runtime ROS2 bridge. Creates publishers from io_contract_ros2.yaml,
    publishes /clock each tick, and sends /tf_static once on start.
    """

    def __init__(self, contract_path: Path, calib_path: Path, time_sync_path: Path, node_name: str = "carla_testbed_bridge", sensors_path: Optional[Path] = None):
        self.contract_path = Path(contract_path)
        self.calib_path = Path(calib_path)
        self.time_sync_path = Path(time_sync_path)
        self.sensors_path = Path(sensors_path) if sensors_path else self.contract_path.parent / "sensors_expanded.json"
        self.node_name = node_name

        self._rclpy = None
        self.node = None
        self.contract: Dict[str, Dict[str, Any]] = {}
        self.sensors: Dict[str, Dict[str, Any]] = {}
        self.calibration: Dict[str, Any] = {}
        self.sensor_publishers: Dict[str, Any] = {}
        self.event_publishers: Dict[str, Any] = {}
        self.clock_pub = None
        self.tf_static_pub = None
        self._tf_sent = False
        self._warned_missing_contract = set()
        self._warned_radar = set()

    def _load_rclpy(self):
        if self._rclpy is None:
            try:
                import rclpy  # type: ignore
                from rclpy.qos import QoSDurabilityPolicy, QoSReliabilityPolicy, QoSProfile  # noqa: F401
                from rosgraph_msgs.msg import Clock  # noqa: F401
                from tf2_msgs.msg import TFMessage  # noqa: F401
            except ImportError as exc:  # pragma: no cover - requires ROS2 runtime
                raise RuntimeError("ROS2BridgeAdapter requires rclpy; please source your ROS2 environment.") from exc
            self._rclpy = rclpy

    def _load_configs(self):
        def _load(path: Path):
            if not path.exists():
                return {}
            text = path.read_text()
            try:
                return yaml.safe_load(text) or {}
            except Exception:
                return json.loads(text)

        contract_raw = _load(self.contract_path)
        if isinstance(contract_raw, dict):
            self.contract = contract_raw
        elif isinstance(contract_raw, list):
            self.contract = {c.get("id"): c for c in contract_raw if isinstance(c, dict)}
        sensors_raw = _load(self.sensors_path)
        if isinstance(sensors_raw, dict) and "sensors" in sensors_raw:
            sensors_raw = sensors_raw.get("sensors") or {}
        if isinstance(sensors_raw, list):
            self.sensors = {s.get("id"): s for s in sensors_raw if isinstance(s, dict)}
        elif isinstance(sensors_raw, dict):
            self.sensors = sensors_raw
        self.calibration = _load(self.calib_path)

    def _resolve_msg_cls(self, msg_type: str):
        if not msg_type:
            return None
        mt = msg_type.replace(" ", "")
        try:
            from sensor_msgs.msg import Image, Imu, NavSatFix, PointCloud2
            from std_msgs.msg import String
        except Exception:
            return None
        mapping = {
            "sensor_msgs/msg/Image": Image,
            "sensor_msgs/Image": Image,
            "sensor_msgs/msg/Imu": Imu,
            "sensor_msgs/Imu": Imu,
            "sensor_msgs/msg/NavSatFix": NavSatFix,
            "sensor_msgs/NavSatFix": NavSatFix,
            "sensor_msgs/msg/PointCloud2": PointCloud2,
            "sensor_msgs/PointCloud2": PointCloud2,
            "std_msgs/msg/String": String,
        }
        return mapping.get(mt)

    def _default_qos_for_type(self, sensor_type: str):
        from rclpy.qos import QoSProfile, QoSReliabilityPolicy, QoSDurabilityPolicy

        if sensor_type == "camera" or sensor_type == "lidar":
            return QoSProfile(depth=5, reliability=QoSReliabilityPolicy.BEST_EFFORT, durability=QoSDurabilityPolicy.VOLATILE)
        if sensor_type == "imu":
            return QoSProfile(depth=50, reliability=QoSReliabilityPolicy.BEST_EFFORT, durability=QoSDurabilityPolicy.VOLATILE)
        if sensor_type == "gnss":
            return QoSProfile(depth=10, reliability=QoSReliabilityPolicy.BEST_EFFORT, durability=QoSDurabilityPolicy.VOLATILE)
        return QoSProfile(depth=10)

    def _create_publishers(self):
        from rclpy.qos import QoSProfile, QoSReliabilityPolicy, QoSDurabilityPolicy
        from rosgraph_msgs.msg import Clock
        from tf2_msgs.msg import TFMessage

        self.clock_pub = self.node.create_publisher(
            Clock, "/clock", QoSProfile(depth=10, reliability=QoSReliabilityPolicy.RELIABLE, durability=QoSDurabilityPolicy.VOLATILE)
        )
        self.tf_static_pub = self.node.create_publisher(
            TFMessage, "/tf_static", QoSProfile(depth=1, reliability=QoSReliabilityPolicy.RELIABLE, durability=QoSDurabilityPolicy.TRANSIENT_LOCAL)
        )
        self.sensor_publishers = {}
        for sensor_id, spec in self.sensors.items():
            if not spec or not spec.get("enabled", True):
                continue
            io_cfg = self.contract.get(sensor_id, {})
            if not io_cfg:
                if sensor_id not in self._warned_missing_contract:
                    print(f"[ROS2Bridge] missing contract for sensor {sensor_id}, skip publisher")
                    self._warned_missing_contract.add(sensor_id)
                continue
            msg_type = io_cfg.get("msg_type")
            msg_cls = self._resolve_msg_cls(msg_type)
            if msg_cls is None:
                print(f"[ROS2Bridge] unsupported msg_type for {sensor_id}: {msg_type}")
                continue
            topic = io_cfg.get("topic") or f"/{sensor_id}"
            qos_profile = qos_from_contract(io_cfg.get("qos"), default_profile=self._default_qos_for_type(spec.get("type")))
            pub = self.node.create_publisher(msg_cls, topic, qos_profile)
            self.sensor_publishers[sensor_id] = {
                "publisher": pub,
                "msg_cls": msg_cls,
                "type": spec.get("type"),
                "frame_id": spec.get("frame_id", sensor_id),
            }
        self._create_event_publishers()

    def _create_event_publishers(self):
        from rclpy.qos import QoSProfile, QoSReliabilityPolicy
        from std_msgs.msg import String

        qos = QoSProfile(depth=50, reliability=QoSReliabilityPolicy.RELIABLE)
        self.event_publishers = {
            "collision": self.node.create_publisher(String, "/sim/ego/events/collision", qos),
            "lane_invasion": self.node.create_publisher(String, "/sim/ego/events/lane_invasion", qos),
        }

    def start(self):
        self._load_rclpy()
        self._load_configs()
        self._rclpy.init(args=None)
        self.node = self._rclpy.create_node(self.node_name)
        self._create_publishers()
        self._publish_tf_static_once()

    def _publish_tf_static_once(self):
        if self._tf_sent or self.tf_static_pub is None:
            return
        msg = build_tf_static_msgs(self.calibration)
        if getattr(msg, "transforms", None):
            self.tf_static_pub.publish(msg)
            self._tf_sent = True

    def stop(self):
        if self.node is not None:
            try:
                self.node.destroy_node()
            except Exception:
                pass
            self.node = None
        if self._rclpy:
            try:
                self._rclpy.shutdown()
            except Exception:
                pass

    def publish_frame(self, frame_packet):
        if self.node is None or frame_packet is None:
            return
        from rosgraph_msgs.msg import Clock

        stamp = to_ros_time(frame_packet.timestamp)
        if self.clock_pub:
            clk = Clock()
            clk.clock = stamp
            self.clock_pub.publish(clk)
        for sid, sample in (frame_packet.samples or {}).items():
            info = self.sensor_publishers.get(sid)
            if not info:
                continue
            msg = None
            frame_id = info.get("frame_id") or sid
            if sample.sensor_type == "camera":
                msg = build_image_msg(sample, stamp, frame_id)
            elif sample.sensor_type == "imu":
                msg = build_imu_msg(sample, stamp, frame_id)
            elif sample.sensor_type == "gnss":
                msg = build_navsatfix_msg(sample, stamp, frame_id)
            elif sample.sensor_type == "lidar":
                msg = build_pointcloud2_msg(sample, stamp, frame_id)
            elif sample.sensor_type == "radar":
                if sid not in self._warned_radar:
                    print(f"[ROS2Bridge] radar mapping not implemented for {sid}, skipping publication")
                    self._warned_radar.add(sid)
                continue
            if msg is not None:
                info["publisher"].publish(msg)

    def publish_truth(self, truth_packet):
        if self.node is None or truth_packet is None:
            return
        if not getattr(truth_packet, "events", None):
            return
        stamp = to_ros_time(truth_packet.timestamp)
        for evt in truth_packet.events:
            key = None
            if "collision" in evt.event_type.lower():
                key = "collision"
            elif "lane" in evt.event_type.lower():
                key = "lane_invasion"
            if key is None:
                continue
            pub = self.event_publishers.get(key)
            if not pub:
                continue
            msg = build_event_msg(evt, stamp, frame_id="base_link")
            pub.publish(msg)

    def poll_algo_output(self):
        return None

    def spin_once(self, timeout_sec: float = 0.0):
        if self.node is None:
            return
        try:
            self._rclpy.spin_once(self.node, timeout_sec=timeout_sec)
        except Exception:
            # swallow spin errors to keep harness alive
            return
