from __future__ import annotations

import argparse
import math
import time
from typing import Optional

import rclpy
from rclpy.node import Node

try:
    import carla
except ImportError:
    raise SystemExit("carla module not found; ensure CARLA wheel is mounted and installed inside container")

try:
    from autoware_perception_msgs.msg import PredictedObject, PredictedObjects, ObjectClassification, Shape
    from geometry_msgs.msg import Pose, Quaternion, Vector3, Twist
except ImportError as exc:
    raise SystemExit(f"autoware_perception_msgs not found: {exc}")


def carla_to_ros_quat(yaw_deg: float) -> Quaternion:
    yaw_rad = math.radians(-yaw_deg)  # invert yaw for ROS frame
    q = Quaternion()
    q.z = math.sin(yaw_rad * 0.5)
    q.w = math.cos(yaw_rad * 0.5)
    return q


def carla_to_ros_pose(tr: carla.Transform) -> Pose:
    pose = Pose()
    pose.position.x = tr.location.x
    pose.position.y = -tr.location.y  # ROS y is inverted
    pose.position.z = tr.location.z
    pose.orientation = carla_to_ros_quat(tr.rotation.yaw)
    return pose


def carla_to_ros_twist(vel: carla.Vector3D) -> Twist:
    t = Twist()
    t.linear.x = vel.x
    t.linear.y = -vel.y
    t.linear.z = vel.z
    return t


def classification_from_actor(actor: carla.Actor) -> ObjectClassification:
    cls = ObjectClassification()
    type_id = actor.type_id or ""
    if type_id.startswith("vehicle"):
        cls.label = ObjectClassification.CAR
        cls.probability = 1.0
    elif type_id.startswith("walker"):
        cls.label = ObjectClassification.PEDESTRIAN
        cls.probability = 1.0
    else:
        cls.label = ObjectClassification.UNKNOWN
        cls.probability = 1.0
    return cls


class CarlaTruthPublisher(Node):
    def __init__(self, host: str, port: int, ego_role: str, topic: str, rate_hz: float):
        super().__init__("carla_truth_to_aw_objects")
        self.client = carla.Client(host, port)
        self.client.set_timeout(2.0)
        self.ego_role = ego_role
        self.pub = self.create_publisher(PredictedObjects, topic, 10)
        self.timer = self.create_timer(1.0 / rate_hz, self.tick)

    def _ego_actor(self, actors):
        for a in actors:
            if a.attributes.get("role_name", "") == self.ego_role:
                return a
        return None

    def tick(self):
        try:
            world = self.client.get_world()
            actors = world.get_actors()
        except Exception as exc:
            self.get_logger().warn(f"CARLA connection failed: {exc}")
            return

        ego = self._ego_actor(actors.filter("vehicle.*"))
        msg = PredictedObjects()
        msg.header.stamp = self.get_clock().now().to_msg()
        msg.header.frame_id = "map"
        objects = []

        for actor in actors:
            type_id = actor.type_id
            role = actor.attributes.get("role_name", "")
            if role == self.ego_role:
                continue
            if not (type_id.startswith("vehicle") or type_id.startswith("walker")):
                continue
            obj = PredictedObject()
            obj.existence_probability = 1.0
            obj.classification = [classification_from_actor(actor)]
            obj.kinematics.initial_pose_with_covariance.pose = carla_to_ros_pose(actor.get_transform())
            obj.kinematics.initial_twist_with_covariance.twist = carla_to_ros_twist(actor.get_velocity())
            bb = actor.bounding_box.extent
            obj.shape.type = Shape.BOUNDING_BOX
            obj.shape.dimensions.x = bb.x * 2
            obj.shape.dimensions.y = bb.y * 2
            obj.shape.dimensions.z = bb.z * 2
            objects.append(obj)

        msg.objects = objects
        self.pub.publish(msg)
        self.get_logger().debug(f"published {len(objects)} objects")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--carla-host", default="127.0.0.1")
    ap.add_argument("--carla-port", type=int, default=2000)
    ap.add_argument("--ego-role", default="ego")
    ap.add_argument("--topic", default="/perception/object_recognition/objects")
    ap.add_argument("--rate", type=float, default=20.0)
    args = ap.parse_args()

    rclpy.init()
    node = CarlaTruthPublisher(args.carla_host, args.carla_port, args.ego_role, args.topic, args.rate)
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    rclpy.try_shutdown()


if __name__ == "__main__":
    main()
