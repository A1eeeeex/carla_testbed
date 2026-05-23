from __future__ import annotations

from carla_testbed.contracts import (
    ChassisState,
    EgoState,
    FrameStamp,
    ObstacleTruth,
    Pose3D,
    Quaternion,
    SceneTruth,
    TrafficLightTruth,
    Vector3D,
)


def test_ego_state_serializes_nested_geometry() -> None:
    ego = EgoState(
        stamp=FrameStamp(frame_id=4, sim_time_s=0.2),
        pose=Pose3D(position=Vector3D(x=1.0, y=2.0, z=0.5), orientation=Quaternion(w=1.0)),
        linear_velocity=Vector3D(x=3.0),
        chassis=ChassisState(speed_mps=3.0, driving_mode="complete_auto_drive"),
    )

    ego.validate()
    payload = ego.to_dict()

    assert payload["stamp"]["frame_id"] == 4
    assert payload["pose"]["position"]["x"] == 1.0
    assert payload["linear_velocity"]["x"] == 3.0
    assert payload["chassis"]["driving_mode"] == "complete_auto_drive"


def test_scene_truth_serializes_obstacles_and_traffic_lights() -> None:
    scene = SceneTruth(
        stamp=FrameStamp(frame_id=5, sim_time_s=0.25),
        obstacles=(
            ObstacleTruth(
                obstacle_id="vehicle_1",
                obstacle_type="vehicle",
                pose=Pose3D(position=Vector3D(x=10.0)),
                size=Vector3D(x=4.5, y=2.0, z=1.5),
            ),
        ),
        traffic_lights=(TrafficLightTruth(traffic_light_id="tl_1", state="green"),),
        metadata={"debug_source": "unit"},
    )

    scene.validate()
    payload = scene.to_dict()

    assert payload["obstacles"][0]["obstacle_id"] == "vehicle_1"
    assert payload["obstacles"][0]["size"]["x"] == 4.5
    assert payload["traffic_lights"][0]["state"] == "green"
    assert payload["metadata"] == {"debug_source": "unit"}
