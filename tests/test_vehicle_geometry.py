from __future__ import annotations

import math

import pytest

from carla_testbed.sim.vehicle_geometry import (
    steering_wheel_angle_rad_from_tire_angle_deg,
    wheelbase_from_world_positions,
)


def test_world_wheel_positions_are_projected_onto_actor_forward() -> None:
    # CARLA 0.9.16 reports these wheel positions in world centimetres.  Sorting
    # world X would return the 1.59 m track width instead of the 2.86 m wheelbase.
    wheels = [
        (33628.64453125, 27521.515625, 65.95409393310547),
        (33469.35546875, 27521.515625, 65.95409393310547),
        (33628.64453125, 27235.46875, 65.95409393310547),
        (33469.35546875, 27235.46875, 65.95409393310547),
    ]

    wheelbase = wheelbase_from_world_positions(
        wheels,
        actor_location=(335.49, 273.78, 0.66),
        actor_forward=(0.0, 1.0, 0.0),
        position_scale=0.01,
    )

    assert wheelbase == pytest.approx(2.86046875)


def test_world_wheel_projection_is_rotation_invariant() -> None:
    wheels = [(12.0, 4.0, 0.0), (12.0, 2.0, 0.0), (9.0, 4.0, 0.0), (9.0, 2.0, 0.0)]

    wheelbase = wheelbase_from_world_positions(
        wheels,
        actor_location=(10.5, 3.0, 0.0),
        actor_forward=(1.0, 0.0, 0.0),
    )

    assert wheelbase == pytest.approx(3.0)


def test_carla_tire_degrees_are_not_apollo_steering_wheel_radians() -> None:
    converted = steering_wheel_angle_rad_from_tire_angle_deg(70.0, steer_ratio=16.0)

    assert converted == pytest.approx(math.radians(70.0) * 16.0)
    assert converted != pytest.approx(70.0)
