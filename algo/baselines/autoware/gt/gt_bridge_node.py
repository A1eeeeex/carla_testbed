from __future__ import annotations

import argparse
import copy
import math
import time
from typing import Optional

import rclpy
from rclpy.node import Node
from rclpy.qos import DurabilityPolicy, QoSProfile, ReliabilityPolicy, HistoryPolicy

try:
    import carla
except ImportError:
    raise SystemExit("carla module not found; ensure CARLA wheel is mounted and installed inside container")

try:
    from autoware_adapi_v1_msgs.msg import LocalizationInitializationState
    from autoware_perception_msgs.msg import (
        PredictedObject,
        PredictedObjects,
        PredictedPath,
        ObjectClassification,
        Shape,
        TrafficLightGroupArray,
    )
    from autoware_vehicle_msgs.msg import (
        ControlModeReport,
        GearReport,
        HazardLightsReport,
        SteeringReport,
        TurnIndicatorsReport,
        VelocityReport,
    )
    from geometry_msgs.msg import (
        AccelWithCovarianceStamped,
        Pose,
        PoseStamped,
        PoseWithCovarianceStamped,
        TransformStamped,
        Twist,
    )
    from nav_msgs.msg import OccupancyGrid, Odometry
    from rosgraph_msgs.msg import Clock
    from sensor_msgs.msg import Imu, PointCloud2, PointField
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


def vehicle_frame_twist(longitudinal: float, lateral: float, heading_rate: float) -> Twist:
    """Build a base_link-frame twist for Autoware localization consumers."""
    t = Twist()
    t.linear.x = float(longitudinal)
    t.linear.y = float(lateral)
    t.linear.z = 0.0
    t.angular.z = float(heading_rate)
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
        max_steer_angle: float = 0.7,
        autoware_steer_sign: float = -1.0,
    ):
        super().__init__("gt_bridge")
        if not self.has_parameter("use_sim_time"):
            self.declare_parameter("use_sim_time", True)
        self.host = host
        self.port = port
        self.ego_role = ego_role
        self.map_frame = map_frame
        self.base_frame = base_frame
        self.rate_hz = rate_hz
        self.max_steer_angle = float(max_steer_angle)
        self.autoware_steer_sign = -1.0 if float(autoware_steer_sign) < 0.0 else 1.0
        qos = QoSProfile(depth=10, reliability=ReliabilityPolicy.RELIABLE, history=HistoryPolicy.KEEP_LAST)
        state_qos = QoSProfile(
            depth=1,
            reliability=ReliabilityPolicy.RELIABLE,
            durability=DurabilityPolicy.TRANSIENT_LOCAL,
            history=HistoryPolicy.KEEP_LAST,
        )
        self.objects_pub = self.create_publisher(PredictedObjects, objects_topic, qos)
        self.clock_pub = self.create_publisher(Clock, "/clock", qos)
        self.odom_pub = self.create_publisher(Odometry, odom_topic, qos)
        self.occupancy_grid_pub = self.create_publisher(
            OccupancyGrid,
            "/perception/occupancy_grid_map/map",
            qos,
        )
        self.obstacle_pointcloud_pub = self.create_publisher(
            PointCloud2,
            "/perception/obstacle_segmentation/pointcloud",
            qos,
        )
        self.pose_twist_fusion_pose_pub = self.create_publisher(
            PoseStamped,
            "/localization/pose_twist_fusion_filter/pose",
            qos,
        )
        self.initialpose3d_pub = self.create_publisher(
            PoseWithCovarianceStamped,
            "/initialpose3d",
            qos,
        )
        self.traffic_lights_pub = self.create_publisher(
            TrafficLightGroupArray,
            "/perception/traffic_light_recognition/traffic_signals",
            qos,
        )
        self.localization_state_pub = self.create_publisher(
            LocalizationInitializationState,
            "/localization/initialization_state",
            state_qos,
        )
        self.api_localization_state_pub = self.create_publisher(
            LocalizationInitializationState,
            "/api/localization/initialization_state",
            state_qos,
        )
        self.accel_pub = self.create_publisher(AccelWithCovarianceStamped, "/localization/acceleration", qos)
        self.imu_pub = self.create_publisher(Imu, "/sensing/imu/imu_data", qos)
        self.velocity_pub = self.create_publisher(VelocityReport, "/vehicle/status/velocity_status", qos)
        self.steering_pub = self.create_publisher(SteeringReport, "/vehicle/status/steering_status", qos)
        self.control_mode_pub = self.create_publisher(ControlModeReport, "/vehicle/status/control_mode", qos)
        self.gear_pub = self.create_publisher(GearReport, "/vehicle/status/gear_status", qos)
        self.turn_pub = self.create_publisher(TurnIndicatorsReport, "/vehicle/status/turn_indicators_status", qos)
        self.hazard_pub = self.create_publisher(HazardLightsReport, "/vehicle/status/hazard_lights_status", qos)
        self.tf_broadcaster = TransformBroadcaster(self)

        self.client: Optional[carla.Client] = None
        self.world: Optional[carla.World] = None
        self.connect_backoff = 1.0
        self.next_connect_time = 0.0
        self.last_ego_warn = 0.0
        self.ego_retry_period_s = 0.5
        self._bound_ego_id: Optional[int] = None
        self._last_longitudinal_velocity: Optional[float] = None
        self._last_velocity_time: Optional[float] = None

        self.timer = self.create_timer(1.0 / rate_hz, self.tick)

    def _carla_stamp(self):
        if self.world is None:
            return self.get_clock().now().to_msg()
        try:
            elapsed = max(float(self.world.get_snapshot().timestamp.elapsed_seconds), 0.0)
        except Exception:
            return self.get_clock().now().to_msg()
        sec = int(math.floor(elapsed))
        nanosec = int(round((elapsed - sec) * 1_000_000_000))
        if nanosec >= 1_000_000_000:
            sec += 1
            nanosec -= 1_000_000_000
        stamp = Clock().clock
        stamp.sec = sec
        stamp.nanosec = nanosec
        return stamp

    def _publish_clock(self, stamp) -> None:
        clock = Clock()
        clock.clock = stamp
        self.clock_pub.publish(clock)

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

    def _sync_world_snapshot(self) -> None:
        if self.world is None:
            return
        try:
            # The runner remains the CARLA tick owner. In synchronous mode this
            # separate client still needs to observe ticks, otherwise
            # get_snapshot() can keep returning a stale frame/time.
            self.world.wait_for_tick(max(0.001, min(0.05, 1.0 / max(self.rate_hz, 1.0))))
        except Exception:
            pass

    def _refresh_world(self, reason: str) -> None:
        if not self.client:
            return
        try:
            new_world = self.client.get_world()
            old_map = self.world.get_map().name if self.world is not None else "<none>"
            new_map = new_world.get_map().name
            self.world = new_world
            self.get_logger().info(f"Refreshed CARLA world ({reason}): {old_map} -> {new_map}")
        except Exception as exc:
            self.get_logger().warn(f"CARLA world refresh failed ({reason}): {exc}")
            self.client = None
            self.world = None

    def _find_ego(self, actors) -> Optional[carla.Actor]:
        for a in actors.filter("vehicle.*"):
            if a.attributes.get("role_name", "") == self.ego_role:
                return a
        return None

    def _query_actors(self):
        if self.world is None:
            return None
        actors = self.world.get_actors()
        if len(actors) > 0:
            return actors
        try:
            # In synchronous CARLA worlds, a fresh client can report an empty
            # actor cache until it observes a tick. The runner remains the tick
            # owner; this short wait only syncs this client-side cache.
            self.world.wait_for_tick(0.2)
            actors = self.world.get_actors()
        except Exception:
            pass
        return actors

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
        longitudinal_v, lateral_v, heading_rate = self._vehicle_velocity_components(ego)
        # nav_msgs/Odometry twist is consumed by Autoware's longitudinal
        # controller as base_link velocity. Publishing map-frame x/y here makes
        # a north/south route look nearly stationary in twist.linear.x.
        odom.twist.twist = vehicle_frame_twist(longitudinal_v, lateral_v, heading_rate)
        self.odom_pub.publish(odom)

        pose_msg = PoseStamped()
        pose_msg.header.stamp = stamp
        pose_msg.header.frame_id = self.map_frame
        pose_msg.pose = odom.pose.pose
        self.pose_twist_fusion_pose_pub.publish(pose_msg)

        # In GT localization mode Autoware's localization stack is disabled,
        # but component_state_monitor still expects /initialpose3d to exist.
        initialpose = PoseWithCovarianceStamped()
        initialpose.header.stamp = stamp
        initialpose.header.frame_id = self.map_frame
        initialpose.pose.pose = odom.pose.pose
        initialpose.pose.covariance = [0.0] * 36
        self.initialpose3d_pub.publish(initialpose)

        state_msg = LocalizationInitializationState()
        state_msg.stamp = stamp
        state_msg.state = LocalizationInitializationState.INITIALIZED
        self.localization_state_pub.publish(state_msg)
        self.api_localization_state_pub.publish(state_msg)

    def _vehicle_velocity_components(self, ego: carla.Actor) -> tuple[float, float, float]:
        tr = ego.get_transform()
        vel = ego.get_velocity()
        yaw = math.radians(float(tr.rotation.yaw))
        hx = math.cos(yaw)
        hy = math.sin(yaw)
        longitudinal = (float(vel.x) * hx) + (float(vel.y) * hy)
        lateral = (-float(vel.x) * hy) + (float(vel.y) * hx)
        try:
            angular = ego.get_angular_velocity()
            heading_rate = -math.radians(float(angular.z))
        except Exception:
            heading_rate = 0.0
        return longitudinal, lateral, heading_rate

    def _publish_acceleration_and_vehicle_status(self, ego: carla.Actor, stamp) -> None:
        longitudinal_v, lateral_v, heading_rate = self._vehicle_velocity_components(ego)
        now = time.monotonic()
        accel_x = 0.0
        if self._last_longitudinal_velocity is not None and self._last_velocity_time is not None:
            dt = max(now - self._last_velocity_time, 1e-3)
            accel_x = (longitudinal_v - self._last_longitudinal_velocity) / dt
        self._last_longitudinal_velocity = longitudinal_v
        self._last_velocity_time = now

        accel = AccelWithCovarianceStamped()
        accel.header.stamp = stamp
        accel.header.frame_id = self.base_frame
        accel.accel.accel.linear.x = float(accel_x)
        accel.accel.accel.linear.y = 0.0
        accel.accel.accel.linear.z = 0.0
        accel.accel.accel.angular.z = float(heading_rate)
        accel.accel.covariance = [0.0] * 36
        self.accel_pub.publish(accel)

        imu = Imu()
        imu.header.stamp = stamp
        imu.header.frame_id = self.base_frame
        imu.orientation_covariance = [-1.0] + [0.0] * 8
        imu.angular_velocity.z = float(heading_rate)
        imu.angular_velocity_covariance = [0.0] * 9
        imu.linear_acceleration.x = float(accel_x)
        imu.linear_acceleration_covariance = [0.0] * 9
        self.imu_pub.publish(imu)

        velocity = VelocityReport()
        velocity.header.stamp = stamp
        velocity.header.frame_id = self.base_frame
        velocity.longitudinal_velocity = float(longitudinal_v)
        velocity.lateral_velocity = float(lateral_v)
        velocity.heading_rate = float(heading_rate)
        self.velocity_pub.publish(velocity)

        ctrl = ego.get_control()
        steering = SteeringReport()
        steering.stamp = stamp
        # CARLA and Autoware use opposite steering signs in the current bridge
        # configuration. Publish feedback in Autoware coordinates so the
        # trajectory follower does not wait forever for steering convergence.
        steering.steering_tire_angle = (
            self.autoware_steer_sign * float(ctrl.steer) * self.max_steer_angle
        )
        self.steering_pub.publish(steering)

        # The CARLA bridge is the simulated vehicle interface. Reporting
        # AUTONOMOUS here tells Autoware that the interface can accept gated
        # commands; operation-mode gating still happens inside Autoware.
        control_mode = ControlModeReport()
        control_mode.stamp = stamp
        control_mode.mode = ControlModeReport.AUTONOMOUS
        self.control_mode_pub.publish(control_mode)

        gear = GearReport()
        gear.stamp = stamp
        gear.report = GearReport.REVERSE if bool(getattr(ctrl, "reverse", False)) else GearReport.DRIVE
        self.gear_pub.publish(gear)

        turn = TurnIndicatorsReport()
        turn.stamp = stamp
        turn.report = TurnIndicatorsReport.DISABLE
        self.turn_pub.publish(turn)

        hazard = HazardLightsReport()
        hazard.stamp = stamp
        hazard.report = HazardLightsReport.DISABLE
        self.hazard_pub.publish(hazard)

    def _publish_minimal_perception_primitives(self, ego: carla.Actor, stamp) -> None:
        """Publish empty free-space inputs required by Autoware lane driving.

        In truth-input mode, dynamic objects are published as object messages.
        Some Autoware planning/control components still wait for occupancy grid
        and obstacle pointcloud topics before producing trajectories. These
        messages are intentionally conservative placeholders: free local grid,
        no segmented obstacle points.
        """
        tr = ego.get_transform()

        grid = OccupancyGrid()
        grid.header.stamp = stamp
        grid.header.frame_id = self.map_frame
        grid.info.resolution = 1.0
        grid.info.width = 240
        grid.info.height = 240
        grid.info.origin.position.x = float(tr.location.x) - 120.0
        grid.info.origin.position.y = -float(tr.location.y) - 120.0
        grid.info.origin.position.z = 0.0
        grid.info.origin.orientation.w = 1.0
        grid.data = [0] * (grid.info.width * grid.info.height)
        self.occupancy_grid_pub.publish(grid)

        cloud = PointCloud2()
        cloud.header.stamp = stamp
        # AEB consumes this obstacle point cloud in the ego-local frame.
        # Keep occupancy grid in map frame, but publish the empty obstacle cloud
        # as base_link to satisfy Autoware's control-check contract.
        cloud.header.frame_id = self.base_frame
        cloud.height = 1
        cloud.width = 0
        cloud.fields = [
            PointField(name="x", offset=0, datatype=PointField.FLOAT32, count=1),
            PointField(name="y", offset=4, datatype=PointField.FLOAT32, count=1),
            PointField(name="z", offset=8, datatype=PointField.FLOAT32, count=1),
        ]
        cloud.is_bigendian = False
        cloud.point_step = 12
        cloud.row_step = 0
        cloud.data = b""
        cloud.is_dense = True
        self.obstacle_pointcloud_pub.publish(cloud)

        # Lane-keep/follow-stop truth-input smoke has no active traffic lights.
        # Publish an empty array so Autoware's planning diagnostics distinguish
        # "no signal state in this scene" from a missing perception contract.
        # Real traffic-light scenarios must replace this with mapped CARLA
        # signal ids/colors aligned to the Autoware vector map.
        traffic_lights = TrafficLightGroupArray()
        traffic_lights.stamp = stamp
        traffic_lights.traffic_light_groups = []
        self.traffic_lights_pub.publish(traffic_lights)

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
        obj.kinematics.predicted_paths = [
            self._build_constant_velocity_predicted_path(actor, tr, horizon_s=5.0, step_s=1.0)
        ]
        # Autoware Universe message fields differ across releases. Populate
        # optional convenience flags only when the installed message supports
        # them; the core pose/twist/shape fields above are the contract.
        if hasattr(obj.kinematics, "is_stationary"):
            speed = math.hypot(float(actor.get_velocity().x), float(actor.get_velocity().y))
            obj.kinematics.is_stationary = speed < 0.1
        if hasattr(obj.kinematics, "has_position_covariance"):
            obj.kinematics.has_position_covariance = False
        if hasattr(obj.kinematics, "has_twist"):
            obj.kinematics.has_twist = True
        if hasattr(obj.kinematics, "has_twist_covariance"):
            obj.kinematics.has_twist_covariance = False
        if hasattr(obj.kinematics, "header"):
            obj.kinematics.header.stamp = stamp
            obj.kinematics.header.frame_id = self.map_frame
        return obj

    def _build_constant_velocity_predicted_path(
        self,
        actor: carla.Actor,
        transform: carla.Transform,
        *,
        horizon_s: float,
        step_s: float,
    ) -> PredictedPath:
        """Build a minimal truth-input predicted path for Autoware planners.

        Motion velocity planning expects object kinematics to include at least
        one predicted path. For a parked follow-stop lead vehicle this is a
        stationary path; for moving CARLA actors it is a conservative
        constant-velocity extrapolation in the same map frame used by the
        object's initial pose. This preserves the AD stack boundary: CARLA GT
        supplies object motion truth, Autoware still decides how to plan.
        """

        path = PredictedPath()
        velocity = actor.get_velocity()
        yaw = carla_to_ros_yaw(transform.rotation.yaw)
        orientation = yaw_to_quat(yaw)
        count = max(1, int(math.floor(float(horizon_s) / max(float(step_s), 1e-3))) + 1)
        poses = []
        for i in range(count):
            dt = float(i) * float(step_s)
            pose = Pose()
            pose.position.x = float(transform.location.x) + (float(velocity.x) * dt)
            pose.position.y = -float(transform.location.y) - (float(velocity.y) * dt)
            pose.position.z = float(transform.location.z) + (float(velocity.z) * dt)
            pose.orientation = copy.deepcopy(orientation)
            poses.append(pose)
        path.path = poses
        if hasattr(path, "time_step"):
            path.time_step.sec = int(math.floor(float(step_s)))
            path.time_step.nanosec = int(round((float(step_s) - path.time_step.sec) * 1_000_000_000))
        if hasattr(path, "confidence"):
            path.confidence = 1.0
        return path

    def tick(self):
        if not self._ensure_world():
            return
        self._sync_world_snapshot()
        try:
            actors = self._query_actors()
        except Exception as exc:
            self.get_logger().warn(f"CARLA world query failed: {exc}")
            self.client = None
            self.world = None
            return
        if actors is None:
            return

        now = self._carla_stamp()
        self._publish_clock(now)
        ego = self._find_ego(actors)
        if ego is None:
            if time.time() - self.last_ego_warn > self.ego_retry_period_s:
                self.get_logger().warn(f"ego '{self.ego_role}' not found; retrying")
                self.last_ego_warn = time.time()
                self._refresh_world("ego_not_found")
            return
        if self._bound_ego_id != int(ego.id):
            self._bound_ego_id = int(ego.id)
            role = (ego.attributes or {}).get("role_name", "")
            self.get_logger().info(f"GT target bound actor_id={ego.id} role={role or '<empty>'}")

        self._publish_tf_and_odom(ego, now)
        self._publish_acceleration_and_vehicle_status(ego, now)
        self._publish_minimal_perception_primitives(ego, now)

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
    ap.add_argument("--max-steer-angle", type=float, default=0.7)
    ap.add_argument(
        "--autoware-steer-sign",
        type=float,
        default=-1.0,
        help="Sign converting CARLA steer feedback into Autoware steering_tire_angle.",
    )
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
        args.max_steer_angle,
        args.autoware_steer_sign,
    )
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    rclpy.try_shutdown()


if __name__ == "__main__":
    main()
