from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ApolloTimeAdapter:
    """Convert core frame stamps into Apollo-style time fields."""

    time_source: str = "sim_time"

    def to_apollo_time(self, stamp: Any) -> dict[str, int | float]:
        frame_id = int(getattr(stamp, "frame_id"))
        if self.time_source != "sim_time":
            raise ValueError(f"ApolloTimeAdapter only supports sim_time, got {self.time_source}")
        timestamp_sec = float(getattr(stamp, "sim_time_s"))
        return {"timestamp_sec": timestamp_sec, "sequence_num": frame_id}
