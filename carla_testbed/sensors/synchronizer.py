from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class SensorSyncStatus(str, Enum):
    EXACT = "exact"
    STALE = "stale"
    FUTURE = "future"
    MISSING = "missing"


@dataclass(frozen=True)
class SensorSyncResult:
    status: SensorSyncStatus
    world_frame_id: int
    sensor_frame_id: int | None
    frame_delta: int | None
    tolerance_frames: int = 0

    @property
    def aligned(self) -> bool:
        return self.status == SensorSyncStatus.EXACT


class SensorSynchronizer:
    def __init__(self, tolerance_frames: int = 0):
        self.tolerance_frames = max(0, int(tolerance_frames))

    def compare(self, *, world_frame_id: int, sensor_sample: Any = None, sensor_frame_id: int | None = None) -> SensorSyncResult:
        frame_value = sensor_frame_id
        if frame_value is None and sensor_sample is not None:
            frame_value = getattr(sensor_sample, "frame", None)
        if frame_value is None:
            return SensorSyncResult(
                status=SensorSyncStatus.MISSING,
                world_frame_id=int(world_frame_id),
                sensor_frame_id=None,
                frame_delta=None,
                tolerance_frames=self.tolerance_frames,
            )

        sensor_frame = int(frame_value)
        delta = sensor_frame - int(world_frame_id)
        if abs(delta) <= self.tolerance_frames:
            status = SensorSyncStatus.EXACT
        elif delta < 0:
            status = SensorSyncStatus.STALE
        else:
            status = SensorSyncStatus.FUTURE
        return SensorSyncResult(
            status=status,
            world_frame_id=int(world_frame_id),
            sensor_frame_id=sensor_frame,
            frame_delta=delta,
            tolerance_frames=self.tolerance_frames,
        )
