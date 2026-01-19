from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from carla_testbed.io.ros2_msg_builders import build_tf_static_msgs, to_ros_time


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
        self.publishers: Dict[str, Any] = {}
        self.clock_pub = None
        self.tf_static_pub = None
        self._tf_sent = False

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

        self.contract = _load(self.contract_path)
        self.sensors = _load(self.sensors_path)
        self.calibration = _load(self.calib_path)

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

    def publish_truth(self, truth_packet):
        # Placeholder: event/ego publication added in later commits
        return

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
