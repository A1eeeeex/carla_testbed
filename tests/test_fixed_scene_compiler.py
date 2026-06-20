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


def test_baguang_static_lead_stop_spawn2m_variant_preserves_ego_offset() -> None:
    template = load_fixed_scene_template("configs/scenarios/baguang/follow_stop_static_300m_spawn2m.yaml")

    storyboard = compile_fixed_scene_template(template)

    assert storyboard["scene_id"] == "baguang_follow_stop_static_300m_spawn2m"
    assert storyboard["roles"]["ego"]["spawn"]["s_offset_m"] == 2.0
    assert storyboard["roles"]["lead_vehicle"]["spawn"]["s_offset_m"] == 300.0


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


def test_baguang_lead_hard_brake_profile_matches_70_to_stop() -> None:
    template = load_fixed_scene_template("configs/scenarios/baguang/lead_hard_brake_70_to_0_20m.yaml")

    storyboard = compile_fixed_scene_template(template)

    validate_fixed_scene_storyboard(storyboard)
    action = storyboard["storyboard"]["phases"][0]["actions"][0]
    assert storyboard["scene_id"] == "baguang_lead_hard_brake_70_to_0_20m"
    assert storyboard["scenario_class"] == "lead_hard_brake"
    assert storyboard["target_actor_contract"]["status"] == "resolved"
    assert storyboard["target_actor_contract"]["target_actor_role"] == "lead_vehicle"
    assert action["interpolation"] == "linear"
    assert action["profile"] == [
        {"t": 0.0, "speed_mps": 19.44},
        {"t": 3.0, "speed_mps": 19.44},
        {"t": 7.0, "speed_mps": 0.0},
        {"t": 20.0, "speed_mps": 0.0},
    ]


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

    assert storyboard["roles"]["lead_vehicle"]["spawn"]["s_offset_m"] == 10.0
    assert storyboard["params"]["initial_gap_m"] == 10.0
    assert storyboard["params"]["expected_lead_travel_m"] == 290.0
    assert storyboard["params"]["duration_s"] == 35.0
    phases = storyboard["storyboard"]["phases"]
    assert phases[1]["id"] == "cut_in_lane_change"
    assert phases[1]["start"] == {
        "type": "relative_longitudinal_distance",
        "from_role": "ego",
        "to_role": "lead_vehicle",
        "frame": "ego_body",
        "op": "<=",
        "value_m": 12.0,
    }
    action = phases[1]["actions"][0]
    assert action["type"] == "lane_change"
    assert action["direction"] == "right"
    assert action["target_speed_mps"] == 9.72
    assert action["duration_s"] == 4.0
    assert action["lateral_shift_m"] == 3.6
    assert action["trigger_frame"] == "ego_body"
    assert storyboard["params"]["duration_policy"] == "lead_reaches_road_end"
    assert storyboard["success_criteria"]["lane_change_start_gap_m"] == 10.0
    assert storyboard["success_criteria"]["lane_change_start_gap_tolerance_m"] == 2.5


def test_baguang_cut_out_compiles_lane_change_phase() -> None:
    template = load_fixed_scene_template("configs/scenarios/baguang/cut_out_35kph_right_to_left_25m.yaml")

    storyboard = compile_fixed_scene_template(template)

    validate_fixed_scene_storyboard(storyboard)
    phases = storyboard["storyboard"]["phases"]
    assert storyboard["scene_id"] == "baguang_cut_out_35kph_right_to_left_25m"
    assert storyboard["scenario_class"] == "cut_out"
    assert storyboard["target_actor_contract"]["status"] == "resolved"
    assert storyboard["target_actor_contract"]["target_actor_role"] == "lead_vehicle"
    assert phases[0]["id"] == "lead_ahead_prepare"
    assert phases[1]["id"] == "cut_out_lane_change"
    assert phases[1]["start"] == {"type": "simulation_time", "op": ">=", "value": 5.0}
    action = phases[1]["actions"][0]
    assert action["type"] == "lane_change"
    assert action["target_lane_offset"] == -1
    assert action["duration_s"] == 4.0
    assert action["lateral_shift_m"] == 3.6
    assert storyboard["params"]["duration_s"] == 35.0
    assert "duration_policy" not in storyboard["params"]
