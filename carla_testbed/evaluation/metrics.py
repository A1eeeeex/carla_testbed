from __future__ import annotations

from dataclasses import dataclass
from typing import Any


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_count(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (list, tuple, set)):
        return len(value)
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


@dataclass(frozen=True)
class RunMetrics:
    frames: int = 0
    sim_duration_s: float | None = None
    wall_duration_s: float | None = None
    collision_count: int = 0
    lane_invasion_count: int = 0
    avg_speed_mps: float | None = None
    max_speed_mps: float | None = None
    min_lead_distance_m: float | None = None
    control_frames: int = 0
    exit_reason: str | None = None

    def to_dict(self) -> dict:
        return {
            "frames": int(self.frames),
            "sim_duration_s": self.sim_duration_s,
            "wall_duration_s": self.wall_duration_s,
            "collision_count": int(self.collision_count),
            "lane_invasion_count": int(self.lane_invasion_count),
            "avg_speed_mps": self.avg_speed_mps,
            "max_speed_mps": self.max_speed_mps,
            "min_lead_distance_m": self.min_lead_distance_m,
            "control_frames": int(self.control_frames),
            "exit_reason": self.exit_reason,
        }


class MetricsAccumulator:
    """Minimal per-run metrics accumulator with no simulator dependency."""

    def __init__(self) -> None:
        self.frames = 0
        self._first_sim_time_s: float | None = None
        self._last_sim_time_s: float | None = None
        self._speed_sum_mps = 0.0
        self._max_speed_mps: float | None = None
        self.collision_count = 0
        self.lane_invasion_count = 0
        self._min_lead_distance_m: float | None = None
        self.control_frames = 0

    def update(
        self,
        *,
        frame_id: int | None = None,
        sim_time_s: float | None = None,
        ego_speed_mps: float | None = None,
        lead_distance_m: float | None = None,
        collision_events_count: int | None = None,
        lane_invasion_events_count: int | None = None,
        applied_control: Any | None = None,
    ) -> None:
        del frame_id
        self.frames += 1

        sim_time = _safe_float(sim_time_s)
        if sim_time is not None:
            if self._first_sim_time_s is None:
                self._first_sim_time_s = sim_time
            self._last_sim_time_s = sim_time

        speed = _safe_float(ego_speed_mps)
        if speed is not None:
            self._speed_sum_mps += speed
            self._max_speed_mps = speed if self._max_speed_mps is None else max(self._max_speed_mps, speed)

        lead_distance = _safe_float(lead_distance_m)
        if lead_distance is not None:
            self._min_lead_distance_m = (
                lead_distance
                if self._min_lead_distance_m is None
                else min(self._min_lead_distance_m, lead_distance)
            )

        self.collision_count += _safe_count(collision_events_count)
        self.lane_invasion_count += _safe_count(lane_invasion_events_count)

        if _has_applied_control(applied_control):
            self.control_frames += 1

    def finalize(self, *, wall_duration_s: float | None = None, exit_reason: str | None = None) -> RunMetrics:
        avg_speed = None if self.frames <= 0 else self._speed_sum_mps / float(self.frames)
        sim_duration = None
        if self._first_sim_time_s is not None and self._last_sim_time_s is not None:
            sim_duration = max(0.0, self._last_sim_time_s - self._first_sim_time_s)
        return RunMetrics(
            frames=self.frames,
            sim_duration_s=sim_duration,
            wall_duration_s=None if wall_duration_s is None else float(wall_duration_s),
            collision_count=self.collision_count,
            lane_invasion_count=self.lane_invasion_count,
            avg_speed_mps=avg_speed,
            max_speed_mps=self._max_speed_mps,
            min_lead_distance_m=self._min_lead_distance_m,
            control_frames=self.control_frames,
            exit_reason=exit_reason,
        )


def _has_applied_control(applied_control: Any | None) -> bool:
    if applied_control is None:
        return False
    if isinstance(applied_control, dict):
        if "applied_ok" in applied_control:
            return bool(applied_control["applied_ok"])
        return any(key in applied_control for key in ("throttle", "brake", "steer"))
    applied_ok = getattr(applied_control, "applied_ok", None)
    if applied_ok is not None:
        return bool(applied_ok)
    return any(hasattr(applied_control, key) for key in ("throttle", "brake", "steer"))
