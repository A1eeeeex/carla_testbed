from __future__ import annotations

import math
from types import SimpleNamespace

import pytest

from carla_testbed.config import HarnessConfig
from carla_testbed.runner.harness import TestHarness as Harness


class _Ego:
    def get_acceleration(self):
        return SimpleNamespace(x=3.0, y=4.0, z=-1.0)

    def get_transform(self):
        return SimpleNamespace(
            location=SimpleNamespace(x=1.0, y=2.0, z=3.0),
            rotation=SimpleNamespace(yaw=90.0, pitch=10.0, roll=-5.0),
        )


def test_vehicle_dynamics_probe_uses_actor_heading_frame() -> None:
    harness = Harness(HarnessConfig())

    acceleration = harness._route_curve_ego_acceleration(_Ego())
    pose = harness._route_curve_ego_pose(_Ego())

    assert acceleration == pytest.approx(
        {"longitudinal": 4.0, "lateral": -3.0, "vertical": -1.0}
    )
    assert pose is not None
    assert pose["pitch"] == pytest.approx(math.radians(10.0))
    assert pose["roll"] == pytest.approx(math.radians(-5.0))
