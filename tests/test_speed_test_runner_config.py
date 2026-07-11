from __future__ import annotations

import importlib
from pathlib import Path

import yaml


def test_speed_runner_keeps_trajectory_stitcher_disabled(tmp_path, monkeypatch) -> None:
    runner = importlib.import_module("scripts.speed_test_runner")
    monkeypatch.setattr(runner, "RUNS_DIR", tmp_path / "speed_test")

    config_path = runner.create_config(
        "60kph",
        16.67,
        "lane_keep",
        "town01_rh_spawn097_goal046",
        "lane_keep",
    )

    payload = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
    apollo = payload["algo"]["apollo"]
    assert apollo["planning"]["enable_trajectory_stitcher"] is False
    assert apollo["planning"]["default_cruise_speed_mps"] == 16.67
    assert apollo["routing"]["target_speed_mps"] == 16.67
    assert apollo["bridge"]["auto_routing"]["target_speed_mps"] == 16.67
    assert apollo["bridge"]["localization_time_source"] == "sim_time"


def test_speed_runner_applies_speed_overrides_after_capability_defaults(tmp_path, monkeypatch) -> None:
    runner = importlib.import_module("scripts.speed_test_runner")
    monkeypatch.setattr(runner, "RUNS_DIR", tmp_path / "speed_test")

    cmd = runner.build_run_command(
        tmp_path / "cfg.yaml",
        "speed_test_60kph_lane_keep",
        "town01_rh_spawn097_goal046",
        "lane_keep",
        16.67,
    )

    override_values = [cmd[index + 1] for index, item in enumerate(cmd[:-1]) if item == "--override"]
    assert "--no-enable-guard" in cmd
    assert "--enable-guard" not in cmd
    assert override_values[:8] == [
        "run.post_fail_steps=0",
        "algo.apollo.control_mapping.actuator_mapping_mode=physical",
        (
            "algo.apollo.control_mapping.physical.calibration_file="
            "artifacts/actuator_calibration_library/"
            "e969a34a5f43fd25/carla_actuator_calibration.json"
        ),
        "algo.apollo.control_mapping.physical.allow_legacy_fallback=false",
        "algo.apollo.control_mapping.physical.map_steering=true",
        "algo.apollo.control_mapping.physical.map_longitudinal=false",
        "algo.apollo.control_mapping.physical.map_throttle=false",
        "algo.apollo.control_mapping.physical.map_brake=true",
    ]
    assert override_values[-4:] == [
        "algo.apollo.routing.target_speed_mps=16.67",
        "algo.apollo.bridge.auto_routing.target_speed_mps=16.67",
        "algo.apollo.planning.default_cruise_speed_mps=16.67",
        "algo.apollo.planning.enable_trajectory_stitcher=false",
    ]


def test_claim_grade_route_health_config_uses_sim_time_localization() -> None:
    payload = yaml.safe_load(
        Path(
            "configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml"
        ).read_text(encoding="utf-8")
    )

    bridge = payload["algo"]["apollo"]["bridge"]
    assert bridge["claim_grade"]["enabled"] is True
    assert bridge["localization_time_source"] == "sim_time"


def test_speed_test_profile_opts_into_signed_deceleration_mapping() -> None:
    payload = yaml.safe_load(
        Path("configs/io/examples/speed_test_base.yaml").read_text(encoding="utf-8")
    )

    control_mapping = payload["algo"]["apollo"]["control_mapping"]
    map_speed_limit = payload["algo"]["apollo"]["map_speed_limit"]
    physical = control_mapping["physical"]
    assert map_speed_limit == {
        "enabled": True,
        "override_mps": 23.61,
        "fill_missing_lane_speed_limits": True,
        "missing_lane_default_mps": 23.61,
        "restore_original": False,
    }
    assert control_mapping["actuator_mapping_mode"] == "physical"
    assert physical["allow_legacy_fallback"] is False
    assert physical["map_steering"] is True
    assert physical["map_longitudinal"] is False
    assert physical["map_throttle"] is False
    assert physical["map_brake"] is True
