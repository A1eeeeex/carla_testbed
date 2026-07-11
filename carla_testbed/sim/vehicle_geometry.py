from __future__ import annotations

import math
from typing import Iterable, Sequence


def wheelbase_from_world_positions(
    wheel_positions: Iterable[Sequence[float]],
    *,
    actor_location: Sequence[float],
    actor_forward: Sequence[float],
    position_scale: float = 1.0,
) -> float | None:
    """Project CARLA world-space wheel positions onto the actor forward axis."""

    forward = tuple(float(value) for value in actor_forward)
    norm = math.sqrt(sum(value * value for value in forward))
    if norm <= 1e-9:
        return None
    unit_forward = tuple(value / norm for value in forward)
    origin = tuple(float(value) for value in actor_location)
    if len(origin) != 3 or len(unit_forward) != 3:
        return None

    longitudinal: list[float] = []
    scale = float(position_scale)
    for position in wheel_positions:
        values = tuple(float(value) * scale for value in position)
        if len(values) != 3:
            continue
        offset = tuple(values[index] - origin[index] for index in range(3))
        longitudinal.append(sum(offset[index] * unit_forward[index] for index in range(3)))

    if len(longitudinal) < 2:
        return None
    longitudinal.sort()
    axle_size = max(1, len(longitudinal) // 2)
    rear = sum(longitudinal[:axle_size]) / axle_size
    front = sum(longitudinal[-axle_size:]) / axle_size
    wheelbase = front - rear
    return wheelbase if wheelbase > 1e-6 else None


def steering_wheel_angle_rad_from_tire_angle_deg(
    tire_angle_deg: float,
    *,
    steer_ratio: float,
) -> float:
    """Convert a road-wheel angle to Apollo's steering-wheel angle unit."""

    return math.radians(abs(float(tire_angle_deg))) * abs(float(steer_ratio))
