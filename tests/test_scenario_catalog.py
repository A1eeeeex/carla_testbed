from __future__ import annotations

from carla_testbed.scenarios.catalog import ScenarioCatalog


def test_scenario_catalog_loads_yaml_specs_without_runtime_builders() -> None:
    catalog = ScenarioCatalog(repo_root=".")
    lane = catalog.get("town01/lane_keep_097")
    traffic = catalog.get("town01_traffic_light_red_stop")

    assert lane.scenario_id == "town01_lane_keep_097"
    assert lane.scenario_class == "lane_keep"
    assert lane.requirements["planning_required"] is True
    assert traffic.requirements["traffic_light_policy"] == "carla_actual"
    assert traffic.gate_role == "hard_gate"


def test_scenario_catalog_lists_curve_and_junction_specs() -> None:
    ids = {entry.scenario_id for entry in ScenarioCatalog(repo_root=".").list()}

    assert "town01_curve217_diagnostic" in ids
    assert "town01_junction_031" in ids
