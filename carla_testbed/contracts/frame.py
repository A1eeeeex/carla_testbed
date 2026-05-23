from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable


@dataclass(frozen=True)
class FrameStamp:
    frame_id: int
    sim_time_s: float
    wall_time_s: float | None = None

    def validate(self) -> None:
        if int(self.frame_id) < 0:
            raise ValueError(f"FrameStamp.frame_id must be >= 0, got {self.frame_id}")
        if float(self.sim_time_s) < 0.0:
            raise ValueError(f"FrameStamp.sim_time_s must be >= 0, got {self.sim_time_s}")
        if self.wall_time_s is not None and float(self.wall_time_s) < 0.0:
            raise ValueError(f"FrameStamp.wall_time_s must be >= 0, got {self.wall_time_s}")

    def to_dict(self) -> dict:
        return asdict(self)


def validate_frame_stamp(stamp: FrameStamp) -> FrameStamp:
    stamp.validate()
    return stamp


def is_monotonic(stamps: Iterable[FrameStamp]) -> bool:
    prev: FrameStamp | None = None
    for stamp in stamps:
        stamp.validate()
        if prev is not None:
            if stamp.frame_id < prev.frame_id:
                return False
            if stamp.sim_time_s < prev.sim_time_s:
                return False
            if stamp.wall_time_s is not None and prev.wall_time_s is not None:
                if stamp.wall_time_s < prev.wall_time_s:
                    return False
        prev = stamp
    return True
