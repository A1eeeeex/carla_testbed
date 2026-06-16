from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ActorKinematics2D:
    x: float
    y: float
    yaw_rad: float
    speed_mps: float | None = None
    length_m: float | None = None
    width_m: float | None = None


def bumper_to_bumper_gap(
    ego: ActorKinematics2D,
    target: ActorKinematics2D,
    *,
    allow_center_distance_fallback: bool = True,
) -> dict[str, Any]:
    """Compute ego-front to target-rear longitudinal gap in ego heading frame."""

    dx = float(target.x) - float(ego.x)
    dy = float(target.y) - float(ego.y)
    ego_forward = (math.cos(float(ego.yaw_rad)), math.sin(float(ego.yaw_rad)))
    target_forward = (math.cos(float(target.yaw_rad)), math.sin(float(target.yaw_rad)))
    longitudinal_center_gap = dx * ego_forward[0] + dy * ego_forward[1]
    lateral_offset_m = -dx * ego_forward[1] + dy * ego_forward[0]
    relative_speed_mps = None
    if ego.speed_mps is not None and target.speed_mps is not None:
        relative_speed_mps = float(target.speed_mps) - float(ego.speed_mps)

    if ego.length_m is None or target.length_m is None:
        if not allow_center_distance_fallback:
            return {
                "status": "insufficient_data",
                "gap_m": None,
                "relative_speed_mps": relative_speed_mps,
                "gap_method": "missing_actor_length",
                "degraded": True,
                "longitudinal_center_gap_m": longitudinal_center_gap,
                "lateral_offset_m": lateral_offset_m,
                "missing_fields": [
                    name
                    for name, value in {
                        "ego.length_m": ego.length_m,
                        "target.length_m": target.length_m,
                    }.items()
                    if value is None
                ],
            }
        return {
            "status": "warn",
            "gap_m": longitudinal_center_gap,
            "relative_speed_mps": relative_speed_mps,
            "gap_method": "center_distance_fallback",
            "degraded": True,
            "longitudinal_center_gap_m": longitudinal_center_gap,
            "lateral_offset_m": lateral_offset_m,
            "missing_fields": [
                name
                for name, value in {
                    "ego.length_m": ego.length_m,
                    "target.length_m": target.length_m,
                }.items()
                if value is None
            ],
        }

    ego_front_extent = max(float(ego.length_m), 0.0) / 2.0
    target_rear_extent = _target_extent_toward_ego_frame(target, ego_forward)
    gap_m = longitudinal_center_gap - ego_front_extent - target_rear_extent
    return {
        "status": "pass",
        "gap_m": gap_m,
        "relative_speed_mps": relative_speed_mps,
        "gap_method": "bumper_to_bumper_longitudinal_projection",
        "degraded": False,
        "longitudinal_center_gap_m": longitudinal_center_gap,
        "lateral_offset_m": lateral_offset_m,
        "missing_fields": [],
        "ego_front_extent_m": ego_front_extent,
        "target_rear_extent_m": target_rear_extent,
    }


def _target_extent_toward_ego_frame(target: ActorKinematics2D, ego_forward: tuple[float, float]) -> float:
    target_forward = (math.cos(float(target.yaw_rad)), math.sin(float(target.yaw_rad)))
    target_right = (-target_forward[1], target_forward[0])
    half_length = max(float(target.length_m or 0.0), 0.0) / 2.0
    half_width = max(float(target.width_m or 0.0), 0.0) / 2.0
    return abs(_dot(target_forward, ego_forward)) * half_length + abs(_dot(target_right, ego_forward)) * half_width


def _dot(a: tuple[float, float], b: tuple[float, float]) -> float:
    return a[0] * b[0] + a[1] * b[1]
