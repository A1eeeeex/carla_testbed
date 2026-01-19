from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional

SensorType = Literal["camera", "lidar", "radar", "imu", "gnss", "segmentation", "depth", "custom"]


@dataclass(frozen=True)
class SensorSample:
    sensor_id: str
    sensor_type: SensorType
    frame_id: int
    timestamp: float  # seconds (sim time recommended)
    payload: Any  # raw bytes / numpy / message object / file ref
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FramePacket:
    frame_id: int
    timestamp: float
    ego_pose_world: Optional[Dict[str, Any]] = None  # {x,y,z,qw,qx,qy,qz} or Pose dataclass
    samples: Dict[str, SensorSample] = field(default_factory=dict)
    frame_meta: Dict[str, Any] = field(default_factory=dict)
