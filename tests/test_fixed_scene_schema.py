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
