from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict


@dataclass
class SensorSpec:
    sensor_id: str
    sensor_type: str  # camera/lidar/radar/imu/gnss/custom
    blueprint: str
    enabled: bool = True
    sensor_tick: float = 0.05
    transform: Dict[str, float] = field(default_factory=dict)  # x,y,z,roll,pitch,yaw
    attributes: Dict[str, str] = field(default_factory=dict)
