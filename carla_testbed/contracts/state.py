from __future__ import annotations

from dataclasses import asdict, dataclass, field

from .frame import FrameStamp
from .geometry import Pose3D, Vector3D


@dataclass(frozen=True)
class ChassisState:
    speed_mps: float = 0.0
    acceleration_mps2: float = 0.0
    steering_percentage: float = 0.0
    throttle_percentage: float = 0.0
    brake_percentage: float = 0.0
    gear: int = 0
    driving_mode: str = "unknown"
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class EgoState:
    stamp: FrameStamp
    pose: Pose3D = field(default_factory=Pose3D)
    linear_velocity: Vector3D = field(default_factory=Vector3D)
    linear_acceleration: Vector3D = field(default_factory=Vector3D)
    angular_velocity: Vector3D = field(default_factory=Vector3D)
    chassis: ChassisState = field(default_factory=ChassisState)
    metadata: dict = field(default_factory=dict)

    def validate(self) -> None:
        self.stamp.validate()

    def to_dict(self) -> dict:
        return {
            "stamp": self.stamp.to_dict(),
            "pose": self.pose.to_dict(),
            "linear_velocity": self.linear_velocity.to_dict(),
            "linear_acceleration": self.linear_acceleration.to_dict(),
            "angular_velocity": self.angular_velocity.to_dict(),
            "chassis": self.chassis.to_dict(),
            "metadata": dict(self.metadata),
        }
