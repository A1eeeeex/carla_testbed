from __future__ import annotations

import importlib
import json
import sys
import tempfile
import time
import types
import unittest
from pathlib import Path


class _FakeStamp:
    def __init__(self) -> None:
        self.sec = 0
        self.nanosec = 0


class _FakeHeader:
    def __init__(self) -> None:
        self.stamp = _FakeStamp()
        self.frame_id = ""


class _FakeVec3:
    def __init__(self) -> None:
        self.x = 0.0
        self.y = 0.0
        self.z = 0.0


class _FakeQuaternion:
    def __init__(self) -> None:
        self.x = 0.0
        self.y = 0.0
        self.z = 0.0
        self.w = 1.0


class _FakePose:
    def __init__(self) -> None:
        self.position = _FakeVec3()
        self.orientation = _FakeQuaternion()


class _FakePoseWithCovariance:
    def __init__(self) -> None:
        self.pose = _FakePose()


class _FakeTwist:
    def __init__(self) -> None:
        self.linear = _FakeVec3()
        self.angular = _FakeVec3()


class _FakeTwistWithCovariance:
    def __init__(self) -> None:
        self.twist = _FakeTwist()


class _FakeOdometry:
    __slots__ = ("header", "child_frame_id", "pose", "twist")

    def __init__(self) -> None:
        self.header = _FakeHeader()
        self.child_frame_id = ""
        self.pose = _FakePoseWithCovariance()
        self.twist = _FakeTwistWithCovariance()


class _FakeLocation:
    def __init__(self, x: float, y: float, z: float) -> None:
        self.x = x
        self.y = y
        self.z = z

    def distance(self, other: "_FakeLocation") -> float:
        dx = self.x - other.x
        dy = self.y - other.y
        dz = self.z - other.z
        return (dx * dx + dy * dy + dz * dz) ** 0.5


class _FakeRotation:
    def __init__(self, roll: float = 0.0, pitch: float = 0.0, yaw: float = 0.0) -> None:
        self.roll = roll
        self.pitch = pitch
        self.yaw = yaw


class _FakeTransform:
    def __init__(self, location: _FakeLocation, rotation: _FakeRotation | None = None) -> None:
        self.location = location
        self.rotation = rotation or _FakeRotation()


class _FakeVector:
    def __init__(self, x: float = 0.0, y: float = 0.0, z: float = 0.0) -> None:
        self.x = x
        self.y = y
        self.z = z

    def length(self) -> float:
        return float((self.x * self.x + self.y * self.y + self.z * self.z) ** 0.5)


class _FakeExtent:
    def __init__(self, x: float = 1.0, y: float = 0.5, z: float = 0.5) -> None:
        self.x = x
        self.y = y
        self.z = z


class _FakeBoundingBox:
    def __init__(self) -> None:
        self.extent = _FakeExtent()


class _FakeVehicleControl:
    def __init__(self, throttle: float = 0.0, brake: float = 0.0, steer: float = 0.0) -> None:
        self.throttle = throttle
        self.brake = brake
        self.steer = steer


class _FakeActor:
    def __init__(self, actor_id: int, role_name: str, type_id: str, location: _FakeLocation) -> None:
        self.id = actor_id
        self.attributes = {"role_name": role_name}
        self.type_id = type_id
        self.bounding_box = _FakeBoundingBox()
        self._transform = _FakeTransform(location)
        self._velocity = _FakeVector()
        self._angular_velocity = _FakeVector()
        self._last_control = _FakeVehicleControl()

    def get_transform(self):
        return self._transform

    def get_velocity(self):
        return self._velocity

    def get_angular_velocity(self):
        return self._angular_velocity

    def apply_control(self, ctrl):
        self._last_control = ctrl


class _FakeActorList(list):
    def filter(self, pattern: str):
        if pattern == "vehicle.*":
            return [item for item in self if item.type_id.startswith("vehicle.")]
        return []


class _FakeTimestamp:
    def __init__(self, elapsed_seconds: float) -> None:
        self.elapsed_seconds = elapsed_seconds


class _FakeSnapshot:
    def __init__(self, frame: int, elapsed_seconds: float) -> None:
        self.frame = frame
        self.timestamp = _FakeTimestamp(elapsed_seconds)


class _FakeWorld:
    def __init__(self, actors):
        self._actors = _FakeActorList(actors)
        self._frame = 101

    def get_actors(self):
        return self._actors

    def get_snapshot(self):
        self._frame += 1
        return _FakeSnapshot(self._frame, 12.5)

    def get_map(self):
        return types.SimpleNamespace(name="Town01")


class _RepeatingFrameWorld(_FakeWorld):
    def __init__(self, actors, frame: int = 777):
        self._actors = _FakeActorList(actors)
        self._frame = frame

    def get_snapshot(self):
        return _FakeSnapshot(self._frame, 12.5)


class _SequenceFrameWorld(_FakeWorld):
    def __init__(self, actors, frames):
        self._actors = _FakeActorList(actors)
        self._frames = list(frames)
        self._last_frame = int(self._frames[-1] if self._frames else 1)

    def get_snapshot(self):
        if self._frames:
            self._last_frame = int(self._frames.pop(0))
        return _FakeSnapshot(self._last_frame, 12.5)


class _FakeClient:
    def __init__(self, host: str, port: int, world: _FakeWorld) -> None:
        self.host = host
        self.port = port
        self._world = world
        self.timeout = None

    def set_timeout(self, timeout: float) -> None:
        self.timeout = timeout

    def get_world(self):
        return self._world


class CarlaDirectTransportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        nav_msgs = types.ModuleType("nav_msgs")
        nav_msgs_msg = types.ModuleType("nav_msgs.msg")
        nav_msgs_msg.Odometry = _FakeOdometry
        nav_msgs.msg = nav_msgs_msg
        sys.modules["nav_msgs"] = nav_msgs
        sys.modules["nav_msgs.msg"] = nav_msgs_msg

        carla_mod = types.ModuleType("carla")
        cls._world = _FakeWorld(
            [
                _FakeActor(1, "hero", "vehicle.tesla.model3", _FakeLocation(0.0, 0.0, 0.0)),
                _FakeActor(2, "npc1", "vehicle.audi.a2", _FakeLocation(10.0, 2.0, 0.0)),
                _FakeActor(3, "walker1", "walker.pedestrian.0001", _FakeLocation(8.0, -1.0, 0.0)),
            ]
        )
        carla_mod.Client = lambda host, port: _FakeClient(host, port, cls._world)
        carla_mod.VehicleControl = _FakeVehicleControl
        carla_mod.World = _FakeWorld
        carla_mod.Vehicle = _FakeActor
        carla_mod.Actor = _FakeActor
        carla_mod.Location = _FakeLocation
        carla_mod.Rotation = _FakeRotation
        carla_mod.Transform = _FakeTransform
        sys.modules["carla"] = carla_mod

        sys.modules.pop("tools.apollo10_cyber_bridge.carla_direct_transport", None)
        cls.mod = importlib.import_module("tools.apollo10_cyber_bridge.carla_direct_transport")

    def test_snapshot_and_transport_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            transport = self.mod.CarlaDirectTransport(
                artifacts_dir=Path(tmpdir),
                carla_host="127.0.0.1",
                carla_port=2000,
                ego_role_name="hero",
                control_out_type="direct",
                radius_m=50.0,
                max_obstacles=8,
                publish_rate_hz=20.0,
                sync_to_world_tick=True,
                timeout_sec=0.8,
                max_steer_angle=0.6,
                speed_gain=10.0,
                brake_gain=5.0,
                watchdog_wait_for_first_msg=True,
                watchdog_arm_delay_sec=1.5,
                startup_brake_suppression_enabled=True,
                startup_brake_suppression_speed_mps=1.0,
                startup_brake_suppression_max_brake=0.2,
                startup_brake_suppression_min_throttle=0.3,
                startup_brake_suppression_hold_sec=3.0,
                startup_brake_recent_throttle_window_sec=1.0,
                route_command_mode="cyber_direct",
                require_no_ros2_runtime=True,
            )
            snapshot = transport.snapshot()
            self.assertIsNotNone(snapshot["odom"])
            self.assertIsNotNone(snapshot["objects_json"])
            self.assertIsInstance(snapshot["world_frame"], int)
            self.assertGreater(snapshot["world_frame"], 0)
            payload = json.loads(snapshot["objects_json"])
            self.assertEqual(len(payload["objects"]), 2)
            summary = transport.transport_summary()
            self.assertEqual(summary["mode"], "carla_direct")
            self.assertEqual(summary["control_apply_path"], "bridge_direct_actor_apply")
            self.assertEqual(summary["tick_owner"], "runner_harness_world_tick")
            self.assertFalse(summary["uses_ros2_gt"])
            self.assertFalse(summary["uses_ros2_control_bridge"])
            self.assertFalse(summary["requires_ros2_reexec"])
            self.assertEqual(summary["route_command_mode"], "cyber_direct")
            self.assertEqual(summary["route_command_path"], "cyber_direct_bridge_command_path")
            self.assertTrue(summary["require_no_ros2_runtime"])
            self.assertEqual(summary["stale_world_frame_policy"], "until_control")

    def test_stale_world_frame_policy_helper(self) -> None:
        helper = self.mod.should_republish_stale_world_frame
        self.assertTrue(helper("until_control", control_tx_count=0))
        self.assertFalse(helper("until_control", control_tx_count=1))
        self.assertTrue(helper("always_republish", control_tx_count=10))
        self.assertFalse(helper("skip", control_tx_count=0))
        self.assertTrue(helper("unknown_policy", control_tx_count=0))

    def test_configured_client_timeout_is_used_for_carla_attach(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            transport = self.mod.CarlaDirectTransport(
                artifacts_dir=Path(tmpdir),
                carla_host="127.0.0.1",
                carla_port=2000,
                ego_role_name="hero",
                control_out_type="direct",
                radius_m=50.0,
                max_obstacles=8,
                publish_rate_hz=20.0,
                sync_to_world_tick=True,
                timeout_sec=0.8,
                max_steer_angle=0.6,
                speed_gain=10.0,
                brake_gain=5.0,
                watchdog_wait_for_first_msg=True,
                watchdog_arm_delay_sec=1.5,
                startup_brake_suppression_enabled=True,
                startup_brake_suppression_speed_mps=1.0,
                startup_brake_suppression_max_brake=0.2,
                startup_brake_suppression_min_throttle=0.3,
                startup_brake_suppression_hold_sec=3.0,
                startup_brake_recent_throttle_window_sec=1.0,
                client_timeout_sec=7.5,
            )
            self.assertAlmostEqual(transport.client.timeout, 7.5)
            self.assertAlmostEqual(transport.stats["client_timeout_sec"], 7.5)

    def test_stale_world_frame_policy_is_configurable(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            transport = self.mod.CarlaDirectTransport(
                artifacts_dir=Path(tmpdir),
                carla_host="127.0.0.1",
                carla_port=2000,
                ego_role_name="hero",
                control_out_type="direct",
                radius_m=50.0,
                max_obstacles=8,
                publish_rate_hz=20.0,
                sync_to_world_tick=True,
                timeout_sec=0.8,
                max_steer_angle=0.6,
                speed_gain=10.0,
                brake_gain=5.0,
                watchdog_wait_for_first_msg=True,
                watchdog_arm_delay_sec=1.5,
                startup_brake_suppression_enabled=True,
                startup_brake_suppression_speed_mps=1.0,
                startup_brake_suppression_max_brake=0.2,
                startup_brake_suppression_min_throttle=0.3,
                startup_brake_suppression_hold_sec=3.0,
                startup_brake_recent_throttle_window_sec=1.0,
                stale_world_frame_policy="always_republish",
            )
            self.assertEqual(transport.stale_world_frame_policy, "always_republish")
            self.assertEqual(transport.stats["stale_world_frame_policy"], "always_republish")
            self.assertEqual(
                transport.transport_summary()["stale_world_frame_policy"],
                "always_republish",
            )

    def test_publish_control_applies_vehicle_control_and_writes_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            transport = self.mod.CarlaDirectTransport(
                artifacts_dir=Path(tmpdir),
                carla_host="127.0.0.1",
                carla_port=2000,
                ego_role_name="hero",
                control_out_type="direct",
                radius_m=50.0,
                max_obstacles=8,
                publish_rate_hz=20.0,
                sync_to_world_tick=True,
                timeout_sec=0.8,
                max_steer_angle=0.6,
                speed_gain=10.0,
                brake_gain=5.0,
                watchdog_wait_for_first_msg=True,
                watchdog_arm_delay_sec=1.5,
                startup_brake_suppression_enabled=True,
                startup_brake_suppression_speed_mps=1.0,
                startup_brake_suppression_max_brake=0.2,
                startup_brake_suppression_min_throttle=0.3,
                startup_brake_suppression_hold_sec=3.0,
                startup_brake_recent_throttle_window_sec=1.0,
            )
            ego = transport.world.get_actors()[0]
            msg = types.SimpleNamespace(data=[0.4, 0.0, 0.2])
            transport.publish_control(msg)
            self.assertAlmostEqual(ego._last_control.throttle, 0.4)
            self.assertAlmostEqual(ego._last_control.brake, 0.0)
            self.assertAlmostEqual(ego._last_control.steer, 0.2)
            self.assertEqual(transport.stats["control_apply_count"], 1)
            self.assertIsInstance(transport.stats["control_apply_first_frame"], int)
            self.assertEqual(
                transport.stats["control_apply_last_frame"],
                transport.stats["control_apply_first_frame"],
            )
            self.assertEqual(transport.stats["control_apply_frame_span"], 1)
            self.assertAlmostEqual(transport.stats["control_apply_max_throttle"], 0.4)
            transport.write_artifacts()
            self.assertTrue((Path(tmpdir) / "direct_bridge_stats.json").exists())
            self.assertTrue((Path(tmpdir) / "direct_bridge_actor_snapshot.json").exists())
            self.assertTrue((Path(tmpdir) / "direct_bridge_control_apply.jsonl").exists())

    def test_snapshot_marks_repeated_world_frame_as_not_advanced(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            transport = self.mod.CarlaDirectTransport(
                artifacts_dir=Path(tmpdir),
                carla_host="127.0.0.1",
                carla_port=2000,
                ego_role_name="hero",
                control_out_type="direct",
                radius_m=50.0,
                max_obstacles=8,
                publish_rate_hz=20.0,
                sync_to_world_tick=True,
                timeout_sec=0.8,
                max_steer_angle=0.6,
                speed_gain=10.0,
                brake_gain=5.0,
                watchdog_wait_for_first_msg=True,
                watchdog_arm_delay_sec=1.5,
                startup_brake_suppression_enabled=True,
                startup_brake_suppression_speed_mps=1.0,
                startup_brake_suppression_max_brake=0.2,
                startup_brake_suppression_min_throttle=0.3,
                startup_brake_suppression_hold_sec=3.0,
                startup_brake_recent_throttle_window_sec=1.0,
            )
            hero = transport.world.get_actors()[0]
            transport.world = _RepeatingFrameWorld([hero], frame=880)
            transport.ego = hero
            transport._last_discovery_sec = time.monotonic()

            first = transport.snapshot()
            self.assertTrue(first["world_frame_advanced"])
            self.assertEqual(first["world_frame"], 880)
            self.assertEqual(transport.stats["snapshot_count"], 1)

            transport._last_snapshot_wall_sec = 0.0
            second = transport.snapshot()
            self.assertFalse(second["world_frame_advanced"])
            self.assertEqual(second["world_frame"], 880)
            self.assertEqual(transport.stats["snapshot_count"], 1)
            self.assertEqual(transport.stats["world_frame_repeat_count"], 1)

    def test_duplicate_frame_control_is_deferred_until_next_world_frame(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            transport = self.mod.CarlaDirectTransport(
                artifacts_dir=Path(tmpdir),
                carla_host="127.0.0.1",
                carla_port=2000,
                ego_role_name="hero",
                control_out_type="direct",
                radius_m=50.0,
                max_obstacles=8,
                publish_rate_hz=20.0,
                sync_to_world_tick=True,
                timeout_sec=0.8,
                max_steer_angle=0.6,
                speed_gain=10.0,
                brake_gain=5.0,
                watchdog_wait_for_first_msg=True,
                watchdog_arm_delay_sec=1.5,
                startup_brake_suppression_enabled=True,
                startup_brake_suppression_speed_mps=1.0,
                startup_brake_suppression_max_brake=0.2,
                startup_brake_suppression_min_throttle=0.3,
                startup_brake_suppression_hold_sec=3.0,
                startup_brake_recent_throttle_window_sec=1.0,
            )
            hero = transport.world.get_actors()[0]
            transport.world = _SequenceFrameWorld([hero], frames=[900, 900, 901, 901])
            transport.ego = hero
            transport._last_discovery_sec = time.monotonic()

            transport.publish_control(types.SimpleNamespace(data=[0.2, 0.0, 0.0]))
            self.assertEqual(transport.stats["control_apply_count"], 1)
            self.assertAlmostEqual(hero._last_control.throttle, 0.2)

            transport.publish_control(types.SimpleNamespace(data=[0.8, 0.0, 0.1]))
            self.assertEqual(transport.stats["control_apply_count"], 1)
            self.assertEqual(transport.stats["control_apply_deferred_count"], 1)
            self.assertIsNotNone(transport._pending_control)

            transport.tick()
            self.assertEqual(transport.stats["control_apply_count"], 2)
            self.assertEqual(transport.stats["control_apply_pending_flush_count"], 1)
            self.assertIsNone(transport._pending_control)
            self.assertAlmostEqual(hero._last_control.throttle, 0.8)
            self.assertAlmostEqual(hero._last_control.steer, 0.1)

    def test_frame_flush_only_control_queues_until_tick(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            transport = self.mod.CarlaDirectTransport(
                artifacts_dir=Path(tmpdir),
                carla_host="127.0.0.1",
                carla_port=2000,
                ego_role_name="hero",
                control_out_type="direct",
                radius_m=50.0,
                max_obstacles=8,
                publish_rate_hz=20.0,
                sync_to_world_tick=True,
                timeout_sec=0.8,
                max_steer_angle=0.6,
                speed_gain=10.0,
                brake_gain=5.0,
                watchdog_wait_for_first_msg=True,
                watchdog_arm_delay_sec=1.5,
                startup_brake_suppression_enabled=True,
                startup_brake_suppression_speed_mps=1.0,
                startup_brake_suppression_max_brake=0.2,
                startup_brake_suppression_min_throttle=0.3,
                startup_brake_suppression_hold_sec=3.0,
                startup_brake_recent_throttle_window_sec=1.0,
                control_apply_mode="frame_flush_only",
            )
            hero = transport.world.get_actors()[0]
            transport.world = _SequenceFrameWorld([hero], frames=[910, 911])
            transport.ego = hero
            transport._last_discovery_sec = time.monotonic()

            transport.publish_control(types.SimpleNamespace(data=[0.4, 0.0, 0.2]))
            self.assertEqual(transport.stats["control_apply_count"], 0)
            self.assertEqual(transport.stats["control_apply_frame_flush_queue_count"], 1)
            self.assertIsNotNone(transport._pending_control)

            transport.tick()
            self.assertEqual(transport.stats["control_apply_count"], 1)
            self.assertEqual(transport.stats["control_apply_pending_flush_count"], 1)
            self.assertIsNone(transport._pending_control)
            self.assertAlmostEqual(hero._last_control.throttle, 0.4)
            self.assertAlmostEqual(hero._last_control.brake, 0.0)
            self.assertAlmostEqual(hero._last_control.steer, 0.2)
            self.assertEqual(transport.transport_summary()["control_apply_mode"], "frame_flush_only")


if __name__ == "__main__":
    unittest.main()
