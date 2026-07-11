from __future__ import annotations

import pytest

from carla_testbed.scenario_player.schema import load_fixed_scene_template, validate_fixed_scene_template
from carla_testbed.scenario_player.triggers import evaluate_trigger
from carla_testbed.scenario_player.actor_registry import ScenarioActorRegistry


def test_follow_stop_template_loads() -> None:
    template = load_fixed_scene_template("configs/scenario_templates/follow_stop.yaml")

    assert template["schema_version"] == "fixed_scene_template.v1"
    assert template["template"] == "follow_stop"
    assert template["roles"]["lead_vehicle"]["control_owner"] == "fixed_scene_player"


def test_static_lead_stop_template_loads() -> None:
    template = load_fixed_scene_template("configs/scenario_templates/static_lead_stop.yaml")

    assert template["template"] == "static_lead_stop"
    assert template["params"]["lead_initial_state"] == "stopped"
    assert template["roles"]["lead_vehicle"]["spawn"]["feasibility"]["require_waypoint"] is True


def test_town01_scenario_fixed_scene_block_loads() -> None:
    template = load_fixed_scene_template("configs/scenarios/town01/follow_stop_097.yaml")

    assert template["scenario_id"] == "follow_stop_097"
    assert template["map"] == "Town01"
    assert template["template"] == "follow_stop"


def test_template_requires_supported_template() -> None:
    with pytest.raises(ValueError, match="template must be one of"):
        validate_fixed_scene_template(
            {
                "schema_version": "fixed_scene_template.v1",
                "template": "hand_wavy_demo",
                "roles": {"ego": {}, "lead_vehicle": {}},
                "params": {},
            }
        )


def test_bumper_gap_spawn_requires_explicit_expected_gap() -> None:
    with pytest.raises(ValueError, match="expected_bumper_gap_m is required"):
        validate_fixed_scene_template(
            {
                "schema_version": "fixed_scene_template.v1",
                "template": "lead_vehicle_accel_decel",
                "roles": {
                    "ego": {},
                    "lead_vehicle": {"spawn": {"gap_reference": "bumper_to_bumper"}},
                },
                "params": {},
            }
        )


def test_trigger_dsl_aliases_match_proposed_yaml_shape() -> None:
    actors = ScenarioActorRegistry()
    actors.update(
        {
            "ego": {"x": 0.0, "y": 0.0, "speed_mps": 4.0, "route_s": 10.0},
            "lead_vehicle": {"x": 15.0, "y": 0.0, "speed_mps": 0.1},
        }
    )

    assert evaluate_trigger({"type": "simulation_time", "gte_s": 5.0}, sim_time_sec=5.0, world_frame=1, actors=actors)
    assert evaluate_trigger(
        {"type": "relative_distance", "from": "ego", "to": "lead_vehicle", "lte_m": 20.0},
        sim_time_sec=0.0,
        world_frame=1,
        actors=actors,
    )
    assert evaluate_trigger(
        {"type": "actor_speed", "actor": "lead_vehicle", "lte_mps": 0.2},
        sim_time_sec=0.0,
        world_frame=1,
        actors=actors,
    )
    assert evaluate_trigger(
        {"all": [{"type": "ego_route_s", "gte_m": 5.0}, {"type": "world_frame", "gte": 1}]},
        sim_time_sec=0.0,
        world_frame=1,
        actors=actors,
    )


def test_relative_distance_trigger_prefers_route_progress_gap_when_available() -> None:
    actors = ScenarioActorRegistry()
    actors.update(
        {
            "ego": {"x": 0.0, "y": 0.0, "speed_mps": 20.0, "trajectory_progress_m": 220.0},
            "lead_vehicle": {
                "x": 250.0,
                "y": 200.0,
                "speed_mps": 16.0,
                "trajectory_progress_m": 300.0,
                "route_progress_gap_m": 70.0,
                "route_progress_gap_source": "trajectory_progress_initial_center_gap",
            },
        }
    )

    assert actors.distance("ego", "lead_vehicle") > 300.0
    assert evaluate_trigger(
        {"type": "relative_distance", "from": "ego", "to": "lead_vehicle", "lte_m": 80.0},
        sim_time_sec=0.0,
        world_frame=1,
        actors=actors,
    )


def test_relative_longitudinal_trigger_uses_ego_body_frame_yaw_zero() -> None:
    actors = ScenarioActorRegistry()
    actors.update(
        {
            "ego": {"x": 0.0, "y": 0.0, "yaw_rad": 0.0},
            "lead_vehicle": {"x": 10.0, "y": 3.0},
        }
    )

    relative = actors.relative_longitudinal_lateral("ego", "lead_vehicle")

    assert relative == (10.0, 3.0)
    assert evaluate_trigger(
        {
            "type": "relative_longitudinal_distance",
            "from_role": "ego",
            "to_role": "lead_vehicle",
            "frame": "ego_body",
            "op": "<=",
            "value_m": 10.0,
        },
        sim_time_sec=0.0,
        world_frame=1,
        actors=actors,
    )


def test_relative_longitudinal_trigger_uses_ego_body_frame_yaw_ninety() -> None:
    actors = ScenarioActorRegistry()
    actors.update(
        {
            "ego": {"x": 0.0, "y": 0.0, "yaw_rad": 1.5707963267948966},
            "lead_vehicle": {"x": 3.0, "y": 10.0},
        }
    )

    relative = actors.relative_longitudinal_lateral("ego", "lead_vehicle")

    assert relative is not None
    assert relative[0] == pytest.approx(10.0)
    assert relative[1] == pytest.approx(-3.0)
    assert evaluate_trigger(
        {
            "type": "relative_longitudinal_distance",
            "from_role": "ego",
            "to_role": "lead_vehicle",
            "frame": "ego_body",
            "op": "<=",
            "value_m": 10.0,
        },
        sim_time_sec=0.0,
        world_frame=1,
        actors=actors,
    )
