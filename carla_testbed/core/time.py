from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class FrameStamp:
    frame_id: int
    sim_time_s: float
    wall_time_s: float


def is_monotonic(stamps: Iterable[FrameStamp]) -> bool:
    prev: FrameStamp | None = None
    for stamp in stamps:
        if prev is not None:
            if stamp.frame_id < prev.frame_id:
                return False
            if stamp.sim_time_s < prev.sim_time_s:
                return False
            if stamp.wall_time_s < prev.wall_time_s:
                return False
        prev = stamp
    return True
