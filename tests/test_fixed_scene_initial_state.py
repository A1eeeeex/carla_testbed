import json
import math

from carla_testbed.scenario_player.initial_state import materialize_ego_initial_speed


def test_materialize_ego_initial_speed_applies_forward_velocity_and_writes_artifact(tmp_path):
    ego = _FakeActor(_Transform(_Rotation(yaw=90.0)))
    storyboard = {"roles": {"ego": {"initial_speed_mps": 10.0}}, "params": {}}

    report = materialize_ego_initial_speed(
        ego_actor=ego,
        storyboard=storyboard,
        artifact_dir=tmp_path,
        enabled=True,
    )

    assert report["status"] == "pass"
    assert report["expected_ego_initial_speed_mps"] == 10.0
    assert math.isclose(ego.target_velocity.x, 0.0, abs_tol=1e-6)
    assert math.isclose(ego.target_velocity.y, 10.0, abs_tol=1e-6)
    assert math.isclose(report["applied_target_velocity"]["norm_mps"], 10.0, abs_tol=1e-6)
    path = tmp_path / "ego_initial_state_materialization.json"
    assert path.exists()
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["claim_boundary"].startswith("This artifact records a one-shot")


def test_materialize_ego_initial_speed_disabled_is_explicit(tmp_path):
    ego = _FakeActor(_Transform(_Rotation(yaw=0.0)))
    storyboard = {"params": {"ego_initial_speed_mps": 12.0}}

    report = materialize_ego_initial_speed(
        ego_actor=ego,
        storyboard=storyboard,
        artifact_dir=tmp_path,
        enabled=False,
    )

    assert report["status"] == "disabled"
    assert ego.target_velocity is None


def test_materialize_ego_initial_speed_missing_setter_fails(tmp_path):
    storyboard = {"roles": {"ego": {"initial_speed_mps": 8.0}}}

    report = materialize_ego_initial_speed(
        ego_actor=object(),
        storyboard=storyboard,
        artifact_dir=tmp_path,
        enabled=True,
    )

    assert report["status"] == "fail"
    assert "ego_transform_missing" in report["errors"]


class _FakeActor:
    def __init__(self, transform):
        self.transform = transform
        self.velocity = _Vector(0.0, 0.0, 0.0)
        self.target_velocity = None

    def get_transform(self):
        return self.transform

    def get_velocity(self):
        return self.velocity

    def set_target_velocity(self, velocity):
        self.target_velocity = velocity


class _Transform:
    def __init__(self, rotation):
        self.rotation = rotation


class _Rotation:
    def __init__(self, *, yaw):
        self.yaw = yaw


class _Vector:
    def __init__(self, x, y, z):
        self.x = x
        self.y = y
        self.z = z
