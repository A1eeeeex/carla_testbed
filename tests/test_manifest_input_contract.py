from __future__ import annotations

from carla_testbed.backends.apollo_cyberrt import ApolloCyberRTBackend
from carla_testbed.backends.carla_builtin import CarlaBuiltinBackend


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
