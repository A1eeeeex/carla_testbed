from __future__ import annotations

from carla_testbed.scenario_player.compiler import compile_fixed_scene_template
from carla_testbed.scenario_player.schema import load_fixed_scene_template
from carla_testbed.scenario_player.target_actor import resolve_target_actor_contract


def test_follow_stop_static_resolves_lead_vehicle_target() -> None:
    template = load_fixed_scene_template("configs/scenarios/baguang/follow_stop_static_300m.yaml")
    storyboard = compile_fixed_scene_template(template)

    contract = storyboard["target_actor_contract"]

    assert contract["status"] == "resolved"
    assert contract["target_actor_role"] == "lead_vehicle"
    assert contract["source"] == "scenario_case_explicit"


def test_cut_in_aliases_cutin_vehicle_to_current_lead_role() -> None:
    template = load_fixed_scene_template("configs/scenarios/baguang/cut_in_35kph_left_to_right_10m.yaml")
    storyboard = compile_fixed_scene_template(template)

    contract = storyboard["target_actor_contract"]

    assert contract["status"] == "resolved"
    assert contract["target_actor_role"] == "lead_vehicle"
    assert contract["role_aliases"] == {"cutin_vehicle": "lead_vehicle"}
    assert contract["activation"]["active_after_phase"] == "cut_in_lane_change"
    assert contract["source"] == "scenario_case_explicit"


def test_lane_keep_target_not_required() -> None:
    contract = resolve_target_actor_contract({"scenario_class": "lane_keep", "roles": {"ego": {}}})

    assert contract["status"] == "not_required"
    assert contract["required"] is False


def test_missing_required_target_is_invalid_contract() -> None:
    contract = resolve_target_actor_contract({"scenario_class": "follow_stop_static", "roles": {"ego": {}}})

    assert contract["status"] == "missing"
    assert contract["invalid_reason"] == "missing_target_actor"
