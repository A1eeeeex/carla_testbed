from __future__ import annotations

from carla_testbed.scenario_player.compiler import compile_fixed_scene_template
from carla_testbed.scenario_player.schema import load_fixed_scene_template, validate_fixed_scene_storyboard


def test_follow_stop_compiles_to_storyboard() -> None:
    template = load_fixed_scene_template("configs/scenarios/town01/follow_stop_097.yaml")

    storyboard = compile_fixed_scene_template(template)

    validate_fixed_scene_storyboard(storyboard)
    assert storyboard["schema_version"] == "fixed_scene_storyboard.v1"
    assert storyboard["scene_id"] == "follow_stop_097"
    assert [phase["id"] for phase in storyboard["storyboard"]["phases"]] == [
        "lead_cruise",
        "lead_brake_to_stop",
        "lead_hold_stop",
    ]
    assert storyboard["runtime_boundary"]["key_actor_control_owner"] == "fixed_scene_player"


def test_baguang_static_lead_stop_compiles_without_cruise_or_brake_trigger() -> None:
    template = load_fixed_scene_template("configs/scenarios/baguang/follow_stop_static_300m.yaml")

    storyboard = compile_fixed_scene_template(template)

    phases = storyboard["storyboard"]["phases"]
    assert [phase["id"] for phase in phases] == ["lead_hold_stop"]
    action = phases[0]["actions"][0]
    assert action["type"] == "hold_stop"
    assert action["target_speed_mps"] == 0.0
    assert storyboard["roles"]["lead_vehicle"]["spawn"]["s_offset_m"] == 300.0
    assert storyboard["roles"]["lead_vehicle"]["spawn"]["feasibility"]["require_waypoint"] is True


def test_baguang_lead_accel_profile_matches_40_to_70_kph() -> None:
    template = load_fixed_scene_template("configs/scenarios/baguang/lead_accel_40_to_70_20m.yaml")

    storyboard = compile_fixed_scene_template(template)

    profile = storyboard["storyboard"]["phases"][0]["actions"][0]["profile"]
    action = storyboard["storyboard"]["phases"][0]["actions"][0]
    assert action["interpolation"] == "linear"
    assert profile == [
        {"t": 0.0, "speed_mps": 11.11},
        {"t": 5.0, "speed_mps": 11.11},
        {"t": 10.0, "speed_mps": 19.44},
        {"t": 17.6, "speed_mps": 19.44},
    ]
    assert storyboard["params"]["duration_s"] == 17.6
    assert storyboard["params"]["duration_policy"] == "lead_reaches_road_end"
    assert storyboard["params"]["ego_target_speed_mps"] == 19.44
    assert storyboard["roles"]["lead_vehicle"]["initial_speed_mps"] == 11.11


def test_baguang_lead_decel_profile_matches_70_to_40_kph() -> None:
    template = load_fixed_scene_template("configs/scenarios/baguang/lead_decel_70_to_40_20m.yaml")

    storyboard = compile_fixed_scene_template(template)

    profile = storyboard["storyboard"]["phases"][0]["actions"][0]["profile"]
    action = storyboard["storyboard"]["phases"][0]["actions"][0]
    assert action["interpolation"] == "linear"
    assert profile == [
        {"t": 0.0, "speed_mps": 19.44},
        {"t": 5.0, "speed_mps": 19.44},
        {"t": 9.0, "speed_mps": 11.11},
        {"t": 20.0, "speed_mps": 11.11},
    ]
    assert storyboard["params"]["duration_s"] == 20.0
    assert storyboard["params"]["duration_policy"] == "lead_reaches_road_end"
    assert storyboard["params"]["ego_target_speed_mps"] == 19.44
    assert storyboard["roles"]["lead_vehicle"]["initial_speed_mps"] == 19.44


def test_cut_in_compiles_lane_change_action() -> None:
    template = load_fixed_scene_template("configs/scenario_templates/cut_in.yaml")

    storyboard = compile_fixed_scene_template(template)

    action_types = [
        action["type"]
        for phase in storyboard["storyboard"]["phases"]
        for action in phase["actions"]
    ]
    assert "lane_change" in action_types
    assert storyboard["success_criteria"]["lane_change_completed"] is True


def test_baguang_cut_in_compiles_longitudinal_gap_trigger() -> None:
    template = load_fixed_scene_template("configs/scenarios/baguang/cut_in_35kph_left_to_right_10m.yaml")

    storyboard = compile_fixed_scene_template(template)

    phases = storyboard["storyboard"]["phases"]
    assert phases[1]["id"] == "cut_in_lane_change"
    assert phases[1]["start"] == {
        "type": "relative_longitudinal_distance",
        "from_role": "ego",
        "to_role": "lead_vehicle",
        "frame": "ego_body",
        "op": "<=",
        "value_m": 10.0,
    }
    action = phases[1]["actions"][0]
    assert action["type"] == "lane_change"
    assert action["direction"] == "right"
    assert action["target_speed_mps"] == 9.72
    assert action["duration_s"] == 4.0
    assert action["lateral_shift_m"] == 3.6
    assert action["trigger_frame"] == "ego_body"
    assert storyboard["params"]["duration_policy"] == "lead_reaches_road_end"
