from __future__ import annotations

from dataclasses import asdict, dataclass, field

from .frame import FrameStamp
from .geometry import Pose3D, Vector3D
from .state import EgoState


@dataclass(frozen=True)
class ObstacleTruth:
    obstacle_id: str
    obstacle_type: str = "unknown"
    pose: Pose3D = field(default_factory=Pose3D)
    linear_velocity: Vector3D = field(default_factory=Vector3D)
    linear_acceleration: Vector3D = field(default_factory=Vector3D)
    size: Vector3D | None = None
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "obstacle_id": self.obstacle_id,
            "obstacle_type": self.obstacle_type,
            "pose": self.pose.to_dict(),
            "linear_velocity": self.linear_velocity.to_dict(),
            "linear_acceleration": self.linear_acceleration.to_dict(),
            "size": None if self.size is None else self.size.to_dict(),
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class TrafficLightTruth:
    traffic_light_id: str
    state: str = "unknown"
    stop_line_pose: Pose3D | None = None
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "traffic_light_id": self.traffic_light_id,
            "state": self.state,
            "stop_line_pose": None if self.stop_line_pose is None else self.stop_line_pose.to_dict(),
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class SceneTruth:
    stamp: FrameStamp
    ego: EgoState | None = None
    obstacles: tuple[ObstacleTruth, ...] = ()
    traffic_lights: tuple[TrafficLightTruth, ...] = ()
    metadata: dict = field(default_factory=dict)

    def validate(self) -> None:
        self.stamp.validate()
        if self.ego is not None:
            self.ego.validate()

    def to_dict(self) -> dict:
        return {
            "stamp": self.stamp.to_dict(),
            "ego": None if self.ego is None else self.ego.to_dict(),
            "obstacles": [item.to_dict() for item in self.obstacles],
            "traffic_lights": [item.to_dict() for item in self.traffic_lights],
            "metadata": dict(self.metadata),
        }
