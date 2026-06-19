from __future__ import annotations

from carla_testbed.backends.apollo_cyberrt import ApolloCyberRTBackend
from carla_testbed.backends.carla_builtin import CarlaBuiltinBackend
from carla_testbed.scenario_player.compiler import compile_fixed_scene_template
from carla_testbed.scenario_player.manifest_contract import (
    fixed_scene_manifest_fields_from_storyboard,
    fixed_scene_manifest_fields_from_template_path,
)
from carla_testbed.scenario_player.schema import load_fixed_scene_template


def test_carla_builtin_contract_declares_planning_control_backend() -> None:
    contract = CarlaBuiltinBackend().contract().to_dict()

    assert contract["backend"] == "carla_builtin"
    assert contract["backend_name"] == "carla_builtin"
    assert contract["backend_type"] == "planning_control_backend"
    assert contract["input_contract"] == "scene_truth_direct"
    assert contract["output_control_mode"] == "carla_vehicle_control"
    assert contract["starts_apollo"] is False
    assert contract["starts_autoware"] is False
    assert "target_actor_state" in contract["available_truth_fields"]


def test_apollo_contract_declares_reference_backend_truth_input() -> None:
    contract = ApolloCyberRTBackend().contract().to_dict()

    assert contract["backend"] == "apollo_cyberrt"
    assert contract["backend_name"] == "apollo_cyberrt"
    assert contract["backend_type"] == "apollo_reference_backend"
    assert contract["input_contract"] == "apollo_truth_input_gt_replacement"
    assert contract["transport_mode"] == "cyberrt"
    assert contract["starts_apollo"] is True
    assert contract["needs_local_apollo"] is True
    assert "gt_localization" in contract["available_truth_fields"]


def test_fixed_scene_manifest_contract_fields_from_storyboard() -> None:
    storyboard = compile_fixed_scene_template(
        load_fixed_scene_template("configs/scenarios/baguang/follow_stop_static_300m.yaml")
    )

    fields = fixed_scene_manifest_fields_from_storyboard(storyboard)

    assert fields["artifact_contract_version"] == "phase1_scenario_run_artifacts.v1"
    assert fields["fixed_scene_case"] == "baguang_follow_stop_static_300m"
    assert fields["target_actor_contract"]["status"] == "resolved"
    assert fields["target_actor_contract"]["target_actor_role"] == "lead_vehicle"


def test_fixed_scene_manifest_contract_fields_from_template_path() -> None:
    fields = fixed_scene_manifest_fields_from_template_path(
        "configs/scenarios/baguang/cut_in_35kph_left_to_right_10m.yaml"
    )

    assert fields["artifact_contract_version"] == "phase1_scenario_run_artifacts.v1"
    assert fields["fixed_scene_case"] == "baguang_cut_in_35kph_left_to_right_10m"
    assert fields["target_actor_contract"]["status"] == "resolved"
    assert fields["target_actor_contract"]["target_actor_role"] == "lead_vehicle"
    assert fields["target_actor_contract"]["role_aliases"]["cutin_vehicle"] == "lead_vehicle"
    assert fields["target_actor_contract"]["activation"]["active_after_phase"] == "cut_in_lane_change"


def test_fixed_scene_manifest_contract_ignores_non_fixed_scene_yaml() -> None:
    fields = fixed_scene_manifest_fields_from_template_path("configs/scenarios/town01/lane_keep_097.yaml")

    assert fields["artifact_contract_version"] == "phase1_scenario_run_artifacts.v1"
    assert fields["fixed_scene_case"] is None
    assert fields["target_actor_contract"] is None
