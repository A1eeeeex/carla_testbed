"""Core platform contracts.

These dataclasses are the internal, simulator-neutral language for frames,
state, truth, and control. Adapters own CARLA/protobuf/ROS2/CyberRT conversion.

`metadata` fields are reserved for non-contract-critical debug annotations and
must not be required for adapter correctness.
"""

from .control import AppliedControl, ControlCommand, clamp
from .frame import FrameStamp, is_monotonic, validate_frame_stamp
from .geometry import Pose3D, Quaternion, Transform3D, Vector3D
from .state import ChassisState, EgoState
from .truth import ObstacleTruth, SceneTruth, TrafficLightTruth

__all__ = [
    "AppliedControl",
    "ChassisState",
    "ControlCommand",
    "EgoState",
    "FrameStamp",
    "ObstacleTruth",
    "Pose3D",
    "Quaternion",
    "SceneTruth",
    "TrafficLightTruth",
    "Transform3D",
    "Vector3D",
    "clamp",
    "is_monotonic",
    "validate_frame_stamp",
]
