from __future__ import annotations

import argparse
import math
import time
from typing import Optional

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy

try:
    import carla
except ImportError:
    raise SystemExit("carla module not found; ensure CARLA wheel is mounted and installed inside container")

try:
    from autoware_perception_msgs.msg import (
        PredictedObject,
        PredictedObjects,
        ObjectClassification,
        Shape,
    )
    from geometry_msgs.msg import TransformStamped, Twist
    from nav_msgs.msg import Odometry
    from tf2_ros import TransformBroadcaster
except ImportError as exc:
    raise SystemExit(f"Required ROS messages not found: {exc}")


def carla_to_ros_yaw(yaw_deg: float) -> float:
    return math.radians(-yaw_deg)


def yaw_to_quat(yaw_rad: float):
    # Minimal quaternion for planar yaw
    from geometry_msgs.msg import Quaternion

    q = Quaternion()
    q.z = math.sin(yaw_rad * 0.5)
    q.w = math.cos(yaw_rad * 0.5)
    return q


def carla_to_ros_twist(vec: carla.Vector3D) -> Twist:
    t = Twist()
    t.linear.x = vec.x
    t.linear.y = -vec.y
    t.linear.z = vec.z
    return t


def classification_from_actor(actor: carla.Actor) -> ObjectClassification:
    cls = ObjectClassification()
    type_id = actor.type_id or ""
    if type_id.startswith("vehicle"):
        cls.label = ObjectClassification.CAR
    elif type_id.startswith("walker"):
        cls.label = ObjectClassification.PEDESTRIAN
    else:
        cls.label = ObjectClassification.UNKNOWN
    cls.probability = 1.0
    return cls


class GTBridge(Node):
    def __init__(
        self,
        host: str,
        port: int,
        ego_role: str,
        objects_topic: str,
        odom_topic: str,
        map_frame: str = "map",
        base_frame: str = "base_link",
        rate_hz: float = 20.0,
    ):
        super().__init__("gt_bridge")
        self.declare_parameter("use_sim_time", True)
        self.host = host
        self.port = port
        self.ego_role = ego_role
        self.map_frame = map_frame
        self.base_frame = base_frame
        self.rate_hz = rate_hz
        qos = QoSProfile(depth=10, reliability=ReliabilityPolicy.BEST_EFFORT, history=HistoryPolicy.KEEP_LAST)
        self.objects_pub = self.create_publisher(PredictedObjects, objects_topic, qos)
        self.odom_pub = self.create_publisher(Odometry, odom_topic, qos)
        self.tf_broadcaster = TransformBroadcaster(self)

        self.client: Optional[carla.Client] = None
        self.world: Optional[carla.World] = None
        self.connect_backoff = 1.0
        self.next_connect_time = 0.0
        self.last_ego_warn = 0.0

        self.timer = self.create_timer(1.0 / rate_hz, self.tick)

    # Connection handling with backoff (4.4)
    def _ensure_world(self) -> bool:
        now = time.time()
        if self.client and self.world:
            return True
        if now < self.next_connect_time:
            return False
        try:
            self.client = carla.Client(self.host, self.port)
            self.client.set_timeout(2.0)
            self.world = self.client.get_world()
            self.get_logger().info("Connected to CARLA")
            self.connect_backoff = 1.0
            return True
        except Exception as exc:
            self.get_logger().warn(f"CARLA connect failed: {exc}")
            self.client = None
            self.world = None
            self.next_connect_time = now + self.connect_backoff
            self.connect_backoff = min(self.connect_backoff * 2, 30.0)
            return False

    def _find_ego(self, actors) -> Optional[carla.Actor]:
        for a in actors.filter("vehicle.*"):
            if a.attributes.get("role_name", "") == self.ego_role:
                return a
        return None

    def _publish_tf_and_odom(self, ego: carla.Actor, stamp):
        tr = ego.get_transform()
        yaw = carla_to_ros_yaw(tr.rotation.yaw)

        tf_msg = TransformStamped()
        tf_msg.header.stamp = stamp
        tf_msg.header.frame_id = self.map_frame
        tf_msg.child_frame_id = self.base_frame
        tf_msg.transform.translation.x = tr.location.x
        tf_msg.transform.translation.y = -tr.location.y
        tf_msg.transform.translation.z = tr.location.z
        tf_msg.transform.rotation = yaw_to_quat(yaw)
        self.tf_broadcaster.sendTransform(tf_msg)

        odom = Odometry()
        odom.header.stamp = stamp
        odom.header.frame_id = self.map_frame
        odom.child_frame_id = self.base_frame
        odom.pose.pose.position.x = tr.location.x
        odom.pose.pose.position.y = -tr.location.y
        odom.pose.pose.position.z = tr.location.z
        odom.pose.pose.orientation = yaw_to_quat(yaw)
        odom.twist.twist = carla_to_ros_twist(ego.get_velocity())
        self.odom_pub.publish(odom)

    def _build_object(self, actor: carla.Actor, stamp) -> PredictedObject:
        obj = PredictedObject()
        obj.existence_probability = 1.0
        obj.classification = [classification_from_actor(actor)]
        tr = actor.get_transform()
        yaw = carla_to_ros_yaw(tr.rotation.yaw)
        obj.kinematics.initial_pose_with_covariance.pose.position.x = tr.location.x
        obj.kinematics.initial_pose_with_covariance.pose.position.y = -tr.location.y
        obj.kinematics.initial_pose_with_covariance.pose.position.z = tr.location.z
        obj.kinematics.initial_pose_with_covariance.pose.orientation = yaw_to_quat(yaw)
        obj.kinematics.initial_twist_with_covariance.twist = carla_to_ros_twist(actor.get_velocity())
        bb = actor.bounding_box.extent
        obj.shape.type = Shape.BOUNDING_BOX
        obj.shape.dimensions.x = bb.x * 2
        obj.shape.dimensions.y = bb.y * 2
        obj.shape.dimensions.z = bb.z * 2
        obj.kinematics.initial_pose_with_covariance.covariance = [0.0] * 36
        obj.kinematics.initial_twist_with_covariance.covariance = [0.0] * 36
        obj.kinematics.predicted_paths = []
        obj.kinematics.is_stationary = False
        obj.kinematics.has_position_covariance = False
        obj.kinematics.has_twist = True
        obj.kinematics.has_twist_covariance = False
        obj.kinematics.header.stamp = stamp
        obj.kinematics.header.frame_id = self.map_frame
        return obj

    def tick(self):
        if not self._ensure_world():
            return
        try:
            actors = self.world.get_actors()
        except Exception as exc:
            self.get_logger().warn(f"CARLA world query failed: {exc}")
            self.client = None
            self.world = None
            return

        ego = self._find_ego(actors)
        now = self.get_clock().now().to_msg()
        if ego is None:
            if time.time() - self.last_ego_warn > 5.0:
                self.get_logger().warn(f"ego '{self.ego_role}' not found; retrying")
                self.last_ego_warn = time.time()
            return

        self._publish_tf_and_odom(ego, now)

        objects_msg = PredictedObjects()
        objects_msg.header.stamp = now
        objects_msg.header.frame_id = self.map_frame
        objs = []
        for actor in actors:
            role = actor.attributes.get("role_name", "")
            if role == self.ego_role:
                continue
            type_id = actor.type_id or ""
            if not (type_id.startswith("vehicle") or type_id.startswith("walker")):
                continue
            try:
                objs.append(self._build_object(actor, now))
            except Exception as exc:  # be robust
                self.get_logger().warn(f"build object failed for {actor.id}: {exc}")
        objects_msg.objects = objs
        self.objects_pub.publish(objects_msg)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=2000)
    ap.add_argument("--ego", default="ego", help="CARLA role_name for ego")
    ap.add_argument("--objects-topic", default="/perception/object_recognition/objects")
    ap.add_argument("--odom-topic", default="/localization/kinematic_state")
    ap.add_argument("--map-frame", default="map")
    ap.add_argument("--base-frame", default="base_link")
    ap.add_argument("--rate", type=float, default=20.0)
    args = ap.parse_args()

    rclpy.init()
    node = GTBridge(
        args.host,
        args.port,
        args.ego,
        args.objects_topic,
        args.odom_topic,
        args.map_frame,
        args.base_frame,
        args.rate,
    )
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    rclpy.try_shutdown()


if __name__ == "__main__":
    main()
