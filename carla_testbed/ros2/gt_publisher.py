from __future__ import annotations

import ctypes
import json
import math
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import carla


def _quat_from_rpy_deg(roll_deg: float, pitch_deg: float, yaw_deg: float) -> Tuple[float, float, float, float]:
    roll = math.radians(roll_deg or 0.0)
    pitch = math.radians(pitch_deg or 0.0)
    yaw = math.radians(yaw_deg or 0.0)
    cr = math.cos(roll / 2.0)
    sr = math.sin(roll / 2.0)
    cp = math.cos(pitch / 2.0)
    sp = math.sin(pitch / 2.0)
    cy = math.cos(yaw / 2.0)
    sy = math.sin(yaw / 2.0)
    qw = cr * cp * cy + sr * sp * sy
    qx = sr * cp * cy - cr * sp * sy
    qy = cr * sp * cy + sr * cp * sy
    qz = cr * cp * sy - sr * sp * cy
    return qx, qy, qz, qw


def _normalize_namespace(namespace: str) -> str:
    ns = (namespace or "/carla").strip()
    if not ns.startswith("/"):
        ns = "/" + ns
    ns = ns.rstrip("/")
    return ns or "/carla"


def _normalize_reliability(value: str) -> str:
    val = (value or "").strip().lower()
    return "reliable" if val == "reliable" else "best_effort"


class GroundTruthRos2Publisher:
    """
    Publish CARLA ground-truth to ROS2 from testbed process.

    Optional deps are handled gracefully:
    - no rclpy: publisher disabled
    - no vision_msgs/visualization_msgs: degrade objects output
    """

    def __init__(
        self,
        *,
        ego_id: str = "hero",
        namespace: str = "/carla",
        invert_tf: bool = True,
        calibration_path: Optional[Path] = None,
        publish_tf: bool = True,
        publish_odom: bool = True,
        publish_objects3d: bool = True,
        publish_markers: bool = True,
        objects_radius_m: float = 120.0,
        max_objects: int = 64,
        publish_map_to_odom: bool = True,
        odom_hz: float = 20.0,
        tf_hz: float = 20.0,
        objects_hz: float = 10.0,
        markers_hz: float = 10.0,
        qos_reliability: str = "best_effort",
        qos_depth: int = 10,
    ):
        self.ego_id = ego_id
        self.namespace = _normalize_namespace(namespace)
        self.topic_prefix = f"{self.namespace}/{self.ego_id}"
        self.invert_tf = bool(invert_tf)
        self.calibration_path = Path(calibration_path) if calibration_path else None
        self.publish_tf = bool(publish_tf)
        self.publish_odom = bool(publish_odom)
        self.publish_objects3d = bool(publish_objects3d)
        self.publish_markers = bool(publish_markers)
        self.objects_radius_m = float(objects_radius_m)
        self.max_objects = int(max_objects)
        self.publish_map_to_odom = bool(publish_map_to_odom)
        self.qos_reliability = _normalize_reliability(qos_reliability)
        self.qos_depth = max(1, int(qos_depth))

        self._period_odom = (1.0 / float(odom_hz)) if odom_hz and odom_hz > 0 else 0.0
        self._period_tf = (1.0 / float(tf_hz)) if tf_hz and tf_hz > 0 else 0.0
        self._period_objects = (1.0 / float(objects_hz)) if objects_hz and objects_hz > 0 else 0.0
        self._period_markers = (1.0 / float(markers_hz)) if markers_hz and markers_hz > 0 else 0.0
        self._last_pub: Dict[str, Optional[float]] = {
            "odom": None,
            "tf": None,
            "objects": None,
            "markers": None,
        }

        self.enabled = False
        self._owns_rclpy = False
        self._static_published = False
        self._warned_once = set()
        self._stats: Dict[str, Any] = {
            "enabled": False,
            "closed": False,
            "namespace": self.namespace,
            "ego_id": self.ego_id,
            "topic_prefix": self.topic_prefix,
            "counts": {
                "odom": 0,
                "tf": 0,
                "tf_static": 0,
                "objects3d": 0,
                "objects_markers": 0,
                "objects_json": 0,
            },
            "rate_skips": {"odom": 0, "tf": 0, "objects": 0, "markers": 0},
            "objects_mode": "disabled",
            "topics": {
                "odom": f"{self.topic_prefix}/odom",
                "tf": "/tf",
                "tf_static": "/tf_static",
                "objects3d": f"{self.topic_prefix}/objects3d",
                "objects_markers": f"{self.topic_prefix}/objects_markers",
                "objects_json": f"{self.topic_prefix}/objects_gt_json",
            },
            "dependencies": {
                "rclpy": False,
                "vision_msgs": False,
                "visualization_msgs": False,
            },
            "publishers": {
                "odom": False,
                "tf": False,
                "tf_static": False,
                "objects3d": False,
                "objects_markers": False,
                "objects_json": False,
            },
            "last_elapsed": None,
            "last_error": None,
        }

        self._rclpy = None
        self._node = None
        self._clock = None
        self._Parameter = None
        self._QoSProfile = None
        self._ReliabilityPolicy = None
        self._DurabilityPolicy = None
        self._HistoryPolicy = None
        self._TimeMsg = None
        self._TransformStamped = None
        self._TFMessage = None
        self._Odometry = None
        self._Detection3DArray = None
        self._Detection3D = None
        self._ObjectHypothesisWithPose = None
        self._MarkerArray = None
        self._Marker = None
        self._String = None

        self._odom_pub = None
        self._tf_pub = None
        self._tf_static_pub = None
        self._objects3d_pub = None
        self._markers_pub = None
        self._objects_json_pub = None

        self._init_ros()

    def _warn_once(self, key: str, msg: str) -> None:
        if key in self._warned_once:
            return
        self._warned_once.add(key)
        print(msg)

    def _init_ros(self) -> None:
        self._bootstrap_ros_python_paths()
        try:
            import rclpy
            from builtin_interfaces.msg import Time as TimeMsg
            from geometry_msgs.msg import TransformStamped
            from nav_msgs.msg import Odometry
            from rclpy.parameter import Parameter
            from rclpy.qos import DurabilityPolicy, HistoryPolicy, QoSProfile, ReliabilityPolicy
            from std_msgs.msg import String
            from tf2_msgs.msg import TFMessage
        except Exception as exc:
            self._stats["last_error"] = f"rclpy unavailable: {exc}"
            print(f"[GT] rclpy unavailable, disable GT publisher: {exc}")
            return

        self._rclpy = rclpy
        self._TimeMsg = TimeMsg
        self._TransformStamped = TransformStamped
        self._Odometry = Odometry
        self._Parameter = Parameter
        self._QoSProfile = QoSProfile
        self._ReliabilityPolicy = ReliabilityPolicy
        self._DurabilityPolicy = DurabilityPolicy
        self._HistoryPolicy = HistoryPolicy
        self._String = String
        self._TFMessage = TFMessage
        self._stats["dependencies"]["rclpy"] = True

        try:
            from vision_msgs.msg import Detection3D, Detection3DArray, ObjectHypothesisWithPose

            self._Detection3DArray = Detection3DArray
            self._Detection3D = Detection3D
            self._ObjectHypothesisWithPose = ObjectHypothesisWithPose
            self._stats["dependencies"]["vision_msgs"] = True
        except Exception as exc:
            self._warn_once("vision_msgs", f"[GT] vision_msgs unavailable, objects3d disabled: {exc}")

        try:
            from visualization_msgs.msg import Marker, MarkerArray

            self._MarkerArray = MarkerArray
            self._Marker = Marker
            self._stats["dependencies"]["visualization_msgs"] = True
        except Exception as exc:
            self._warn_once("visualization_msgs", f"[GT] visualization_msgs unavailable, marker GT disabled: {exc}")

        try:
            if not self._rclpy.ok():
                self._rclpy.init(args=None)
                self._owns_rclpy = True
            self._node = self._rclpy.create_node(f"tb_gt_publisher_{self.ego_id}")
            try:
                self._node.set_parameters([self._Parameter("use_sim_time", self._Parameter.Type.BOOL, True)])
            except Exception:
                pass
            self._clock = self._node.get_clock()
        except Exception as exc:
            self._stats["last_error"] = f"failed to initialize ROS2 node: {exc}"
            print(f"[GT] failed to initialize ROS2 node, disable GT publisher: {exc}")
            self.close()
            return

        qos_dyn = self._QoSProfile(
            history=self._HistoryPolicy.KEEP_LAST,
            depth=self.qos_depth,
            reliability=(
                self._ReliabilityPolicy.RELIABLE
                if self.qos_reliability == "reliable"
                else self._ReliabilityPolicy.BEST_EFFORT
            ),
            durability=self._DurabilityPolicy.VOLATILE,
        )
        qos_static = self._QoSProfile(
            history=self._HistoryPolicy.KEEP_LAST,
            depth=1,
            reliability=self._ReliabilityPolicy.RELIABLE,
            durability=self._DurabilityPolicy.TRANSIENT_LOCAL,
        )

        if self.publish_odom:
            self._odom_pub = self._node.create_publisher(self._Odometry, f"{self.topic_prefix}/odom", qos_dyn)
            self._stats["publishers"]["odom"] = True
        if self.publish_tf:
            self._tf_pub = self._node.create_publisher(self._TFMessage, "/tf", qos_dyn)
            self._tf_static_pub = self._node.create_publisher(self._TFMessage, "/tf_static", qos_static)
            self._stats["publishers"]["tf"] = True
            self._stats["publishers"]["tf_static"] = True
        if self.publish_objects3d and self._Detection3DArray is not None:
            self._objects3d_pub = self._node.create_publisher(
                self._Detection3DArray,
                f"{self.topic_prefix}/objects3d",
                qos_dyn,
            )
            self._stats["publishers"]["objects3d"] = True
            self._stats["objects_mode"] = "objects3d"
        if self.publish_markers and self._MarkerArray is not None:
            self._markers_pub = self._node.create_publisher(
                self._MarkerArray,
                f"{self.topic_prefix}/objects_markers",
                qos_dyn,
            )
            self._stats["publishers"]["objects_markers"] = True
            if self._stats["objects_mode"] == "disabled":
                self._stats["objects_mode"] = "objects_markers"

        # JSON fallback when Detection3D is unavailable; keep it even if markers are enabled.
        if self.publish_objects3d and self._objects3d_pub is None:
            self._objects_json_pub = self._node.create_publisher(
                self._String,
                f"{self.topic_prefix}/objects_gt_json",
                qos_dyn,
            )
            self._stats["publishers"]["objects_json"] = True
            if self._stats["objects_mode"] == "objects_markers":
                self._stats["objects_mode"] = "objects_markers+json"
            else:
                self._stats["objects_mode"] = "objects_json"

        if self.publish_objects3d and self._stats["objects_mode"] == "disabled":
            self._stats["objects_mode"] = "none"

        self.enabled = True
        self._stats["enabled"] = True
        self._stats["closed"] = False

    def _bootstrap_ros_python_paths(self) -> None:
        ros_prefix = Path("/opt/ros/humble")
        pyver = f"python{sys.version_info.major}.{sys.version_info.minor}"
        candidates = [
            ros_prefix / "local" / "lib" / pyver / "dist-packages",
            ros_prefix / "lib" / pyver / "site-packages",
            ros_prefix / "lib" / "python3.10" / "site-packages",
            ros_prefix / "local" / "lib" / "python3.10" / "dist-packages",
        ]
        for path in candidates:
            if path.exists():
                text = str(path)
                if text not in sys.path:
                    sys.path.append(text)

        if not os.environ.get("ROS_LOG_DIR"):
            os.environ["ROS_LOG_DIR"] = "/tmp/ros_logs"
        try:
            Path(os.environ["ROS_LOG_DIR"]).mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

        lib_candidates = [
            ros_prefix / "lib",
            ros_prefix / "local" / "lib",
            ros_prefix / "lib" / "x86_64-linux-gnu",
        ]
        lib_paths = [str(path) for path in lib_candidates if path.exists()]
        if lib_paths:
            old = os.environ.get("LD_LIBRARY_PATH", "")
            pieces = [p for p in old.split(":") if p]
            for path in reversed(lib_paths):
                if path not in pieces:
                    pieces.insert(0, path)
            os.environ["LD_LIBRARY_PATH"] = ":".join(pieces)

        preload = [
            ros_prefix / "lib" / "librcl_action.so",
            ros_prefix / "lib" / "librcl.so",
            ros_prefix / "lib" / "librmw.so",
            ros_prefix / "lib" / "librcutils.so",
            ros_prefix / "lib" / "librcl_yaml_param_parser.so",
            ros_prefix / "lib" / "librosidl_runtime_c.so",
        ]
        for lib in preload:
            if not lib.exists():
                continue
            try:
                ctypes.CDLL(str(lib), mode=ctypes.RTLD_GLOBAL)
            except Exception:
                pass

    def _stamp_from_elapsed(self, elapsed_seconds: float):
        sec = int(math.floor(elapsed_seconds))
        nanosec = int((elapsed_seconds - sec) * 1e9)
        if nanosec < 0:
            sec -= 1
            nanosec += int(1e9)
        stamp = self._TimeMsg()
        stamp.sec = sec
        stamp.nanosec = nanosec
        return stamp

    def _loc_xyz(self, loc: carla.Location) -> Tuple[float, float, float]:
        x = float(loc.x)
        y = float(-loc.y if self.invert_tf else loc.y)
        z = float(loc.z)
        return x, y, z

    def _rot_rpy(self, rot: carla.Rotation) -> Tuple[float, float, float]:
        roll = float(rot.roll)
        pitch = float(-rot.pitch if self.invert_tf else rot.pitch)
        yaw = float(-rot.yaw if self.invert_tf else rot.yaw)
        return roll, pitch, yaw

    def _vec_xyz(self, vec) -> Tuple[float, float, float]:
        x = float(getattr(vec, "x", 0.0))
        y_raw = float(getattr(vec, "y", 0.0))
        y = float(-y_raw if self.invert_tf else y_raw)
        z = float(getattr(vec, "z", 0.0))
        return x, y, z

    def _mark_publish(self, key: str) -> None:
        self._stats["counts"][key] = int(self._stats["counts"].get(key, 0)) + 1

    def _should_publish(self, key: str, elapsed: float, period: float) -> bool:
        if period <= 0:
            self._last_pub[key] = elapsed
            return True
        last = self._last_pub.get(key)
        if last is None or (elapsed - last) >= (period - 1e-6):
            self._last_pub[key] = elapsed
            return True
        self._stats["rate_skips"][key] = int(self._stats["rate_skips"].get(key, 0)) + 1
        return False

    def publish_static_tf_from_calibration(self) -> None:
        if not self.enabled or self._tf_static_pub is None or self._static_published:
            return
        transforms = []
        stamp = self._stamp_from_elapsed(0.0)
        if self.publish_map_to_odom:
            t = self._TransformStamped()
            t.header.stamp = stamp
            t.header.frame_id = "map"
            t.child_frame_id = "odom"
            t.transform.rotation.w = 1.0
            transforms.append(t)

        if self.calibration_path and self.calibration_path.exists():
            try:
                calib = json.loads(self.calibration_path.read_text())
                for frame in calib.get("frames", []):
                    child = frame.get("id")
                    if not child:
                        continue
                    parent = frame.get("parent", "base_link")
                    if parent == "ego":
                        parent = "base_link"
                    tf = self._TransformStamped()
                    tf.header.stamp = stamp
                    tf.header.frame_id = parent
                    tf.child_frame_id = str(child)
                    tr = frame.get("transform", {}) or {}
                    tf.transform.translation.x = float(tr.get("x", 0.0))
                    tf.transform.translation.y = float(tr.get("y", 0.0))
                    tf.transform.translation.z = float(tr.get("z", 0.0))
                    qx, qy, qz, qw = _quat_from_rpy_deg(
                        float(tr.get("roll", 0.0)),
                        float(tr.get("pitch", 0.0)),
                        float(tr.get("yaw", 0.0)),
                    )
                    tf.transform.rotation.x = qx
                    tf.transform.rotation.y = qy
                    tf.transform.rotation.z = qz
                    tf.transform.rotation.w = qw
                    transforms.append(tf)
            except Exception as exc:
                self._stats["last_error"] = f"failed to parse calibration for tf_static: {exc}"
                print(f"[GT] failed to parse calibration for tf_static: {exc}")

        msg = self._TFMessage()
        msg.transforms = transforms
        self._tf_static_pub.publish(msg)
        self._mark_publish("tf_static")
        self._static_published = True

    def _publish_odom(self, elapsed_seconds: float, ego: carla.Vehicle) -> None:
        if self._odom_pub is None:
            return
        odom = self._Odometry()
        odom.header.stamp = self._stamp_from_elapsed(elapsed_seconds)
        odom.header.frame_id = "odom"
        odom.child_frame_id = "base_link"

        tr = ego.get_transform()
        x, y, z = self._loc_xyz(tr.location)
        roll, pitch, yaw = self._rot_rpy(tr.rotation)
        qx, qy, qz, qw = _quat_from_rpy_deg(roll, pitch, yaw)
        odom.pose.pose.position.x = x
        odom.pose.pose.position.y = y
        odom.pose.pose.position.z = z
        odom.pose.pose.orientation.x = qx
        odom.pose.pose.orientation.y = qy
        odom.pose.pose.orientation.z = qz
        odom.pose.pose.orientation.w = qw

        vx, vy, vz = self._vec_xyz(ego.get_velocity())
        wx, wy, wz = self._vec_xyz(ego.get_angular_velocity())
        odom.twist.twist.linear.x = vx
        odom.twist.twist.linear.y = vy
        odom.twist.twist.linear.z = vz
        odom.twist.twist.angular.x = wx
        odom.twist.twist.angular.y = wy
        odom.twist.twist.angular.z = wz
        self._odom_pub.publish(odom)
        self._mark_publish("odom")

    def _publish_dynamic_tf(self, elapsed_seconds: float, ego: carla.Vehicle) -> None:
        if self._tf_pub is None:
            return
        tr = ego.get_transform()
        x, y, z = self._loc_xyz(tr.location)
        roll, pitch, yaw = self._rot_rpy(tr.rotation)
        qx, qy, qz, qw = _quat_from_rpy_deg(roll, pitch, yaw)

        tf = self._TransformStamped()
        tf.header.stamp = self._stamp_from_elapsed(elapsed_seconds)
        tf.header.frame_id = "odom"
        tf.child_frame_id = "base_link"
        tf.transform.translation.x = x
        tf.transform.translation.y = y
        tf.transform.translation.z = z
        tf.transform.rotation.x = qx
        tf.transform.rotation.y = qy
        tf.transform.rotation.z = qz
        tf.transform.rotation.w = qw
        msg = self._TFMessage()
        msg.transforms = [tf]
        self._tf_pub.publish(msg)
        self._mark_publish("tf")

    def _class_name(self, actor: carla.Actor) -> str:
        tid = (getattr(actor, "type_id", "") or "").lower()
        if tid.startswith("vehicle."):
            return "vehicle"
        if tid.startswith("walker.") or "pedestrian" in tid:
            return "pedestrian"
        return "unknown"

    def _filtered_objects(self, ego: carla.Actor, actors: Sequence[carla.Actor]) -> List[carla.Actor]:
        ego_loc = ego.get_transform().location
        out = []
        fallback = []
        for actor in actors:
            if actor is None or actor.id == ego.id:
                continue
            tid = getattr(actor, "type_id", "") or ""
            if not (tid.startswith("vehicle.") or tid.startswith("walker.")):
                continue
            try:
                dist = ego_loc.distance(actor.get_transform().location)
            except Exception:
                continue
            fallback.append((dist, actor))
            if dist > self.objects_radius_m:
                continue
            out.append((dist, actor))
        out.sort(key=lambda item: item[0])
        if out:
            return [a for _, a in out[: self.max_objects]]
        fallback.sort(key=lambda item: item[0])
        limit = 1 if self.max_objects > 0 else 0
        return [a for _, a in fallback[:limit]]

    def _publish_objects3d(self, elapsed_seconds: float, actors: Sequence[carla.Actor]) -> None:
        if self._objects3d_pub is None:
            return
        arr = self._Detection3DArray()
        arr.header.stamp = self._stamp_from_elapsed(elapsed_seconds)
        arr.header.frame_id = "odom"
        for actor in actors:
            try:
                tr = actor.get_transform()
                extent = actor.bounding_box.extent
            except Exception:
                continue
            det = self._Detection3D()
            if hasattr(det, "id"):
                det.id = str(actor.id)
            x, y, z = self._loc_xyz(tr.location)
            roll, pitch, yaw = self._rot_rpy(tr.rotation)
            qx, qy, qz, qw = _quat_from_rpy_deg(roll, pitch, yaw)
            det.bbox.center.position.x = x
            det.bbox.center.position.y = y
            det.bbox.center.position.z = z
            det.bbox.center.orientation.x = qx
            det.bbox.center.orientation.y = qy
            det.bbox.center.orientation.z = qz
            det.bbox.center.orientation.w = qw
            det.bbox.size.x = float(extent.x) * 2.0
            det.bbox.size.y = float(extent.y) * 2.0
            det.bbox.size.z = float(extent.z) * 2.0
            hyp = self._ObjectHypothesisWithPose()
            hyp.hypothesis.class_id = self._class_name(actor)
            hyp.hypothesis.score = 1.0
            det.results.append(hyp)
            arr.detections.append(det)
        self._objects3d_pub.publish(arr)
        self._mark_publish("objects3d")

    def _publish_markers(self, elapsed_seconds: float, actors: Sequence[carla.Actor]) -> None:
        if self._markers_pub is None:
            return
        marker_array = self._MarkerArray()
        delete_all = self._Marker()
        delete_all.header.stamp = self._stamp_from_elapsed(elapsed_seconds)
        delete_all.header.frame_id = "odom"
        delete_all.ns = "gt_objects"
        delete_all.action = self._Marker.DELETEALL
        marker_array.markers.append(delete_all)
        for actor in actors:
            try:
                tr = actor.get_transform()
                extent = actor.bounding_box.extent
            except Exception:
                continue
            marker = self._Marker()
            marker.header.stamp = self._stamp_from_elapsed(elapsed_seconds)
            marker.header.frame_id = "odom"
            marker.ns = "gt_objects"
            marker.id = int(actor.id)
            marker.type = self._Marker.CUBE
            marker.action = self._Marker.ADD
            x, y, z = self._loc_xyz(tr.location)
            roll, pitch, yaw = self._rot_rpy(tr.rotation)
            qx, qy, qz, qw = _quat_from_rpy_deg(roll, pitch, yaw)
            marker.pose.position.x = x
            marker.pose.position.y = y
            marker.pose.position.z = z
            marker.pose.orientation.x = qx
            marker.pose.orientation.y = qy
            marker.pose.orientation.z = qz
            marker.pose.orientation.w = qw
            marker.scale.x = float(extent.x) * 2.0
            marker.scale.y = float(extent.y) * 2.0
            marker.scale.z = float(extent.z) * 2.0
            cls = self._class_name(actor)
            if cls == "vehicle":
                marker.color.r, marker.color.g, marker.color.b, marker.color.a = (0.0, 0.7, 1.0, 0.8)
            elif cls == "pedestrian":
                marker.color.r, marker.color.g, marker.color.b, marker.color.a = (1.0, 0.6, 0.0, 0.8)
            else:
                marker.color.r, marker.color.g, marker.color.b, marker.color.a = (0.8, 0.8, 0.8, 0.8)
            marker.lifetime.sec = 0
            marker.lifetime.nanosec = int(0.2 * 1e9)
            marker_array.markers.append(marker)
        self._markers_pub.publish(marker_array)
        self._mark_publish("objects_markers")

    def _publish_objects_json(self, elapsed_seconds: float, actors: Sequence[carla.Actor]) -> None:
        if self._objects_json_pub is None:
            return
        payload = {"stamp": elapsed_seconds, "frame_id": "odom", "objects": []}
        for actor in actors:
            try:
                tr = actor.get_transform()
                extent = actor.bounding_box.extent
            except Exception:
                continue
            x, y, z = self._loc_xyz(tr.location)
            roll, pitch, yaw = self._rot_rpy(tr.rotation)
            payload["objects"].append(
                {
                    "id": str(actor.id),
                    "class": self._class_name(actor),
                    "pose": {"x": x, "y": y, "z": z, "roll": roll, "pitch": pitch, "yaw": yaw},
                    "size": {
                        "x": float(extent.x) * 2.0,
                        "y": float(extent.y) * 2.0,
                        "z": float(extent.z) * 2.0,
                    },
                }
            )
        msg = self._String()
        msg.data = json.dumps(payload, separators=(",", ":"))
        self._objects_json_pub.publish(msg)
        self._mark_publish("objects_json")

    def publish_tick(
        self,
        world: carla.World,
        ego: carla.Vehicle,
        extra_actors: Optional[Iterable[carla.Actor]] = None,
    ) -> None:
        if not self.enabled:
            return
        try:
            snapshot = world.get_snapshot()
            elapsed = float(snapshot.timestamp.elapsed_seconds)
            self._stats["last_elapsed"] = elapsed

            if self.publish_odom and self._should_publish("odom", elapsed, self._period_odom):
                self._publish_odom(elapsed, ego)
            if self.publish_tf and self._should_publish("tf", elapsed, self._period_tf):
                self._publish_dynamic_tf(elapsed, ego)

            actors = list(extra_actors or [])
            filtered = self._filtered_objects(ego, actors) if actors else []

            if (self._objects3d_pub is not None or self._objects_json_pub is not None) and self._should_publish(
                "objects", elapsed, self._period_objects
            ):
                self._publish_objects3d(elapsed, filtered)
                self._publish_objects_json(elapsed, filtered)

            if self._markers_pub is not None and self._should_publish("markers", elapsed, self._period_markers):
                self._publish_markers(elapsed, filtered)

            try:
                self._rclpy.spin_once(self._node, timeout_sec=0.0)
            except Exception:
                pass
        except Exception as exc:
            self._stats["last_error"] = f"publish_tick failed: {exc}"
            self._warn_once("publish_tick", f"[WARN] GT publish tick failed: {exc}")

    def get_stats(self) -> Dict[str, Any]:
        return json.loads(json.dumps(self._stats))

    def close(self) -> None:
        if self._node is not None:
            try:
                self._node.destroy_node()
            except Exception:
                pass
            self._node = None
        if self._rclpy is not None and self._owns_rclpy:
            try:
                self._rclpy.try_shutdown()
            except Exception:
                pass
        self.enabled = False
        self._stats["closed"] = True
