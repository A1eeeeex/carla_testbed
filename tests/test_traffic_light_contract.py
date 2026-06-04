from __future__ import annotations

from carla_testbed.analysis.traffic_light_contract import build_traffic_light_contract_report


def _town01_contract() -> dict:
    return {
        "schema_version": "town01_apollo_contract.v1",
        "map_name": "Town01",
        "apollo_map_root": "/tmp/nonexistent_apollo_map",
        "routes": [
            {
                "route_id": "traffic_light_route",
                "route_ref": "inline:traffic_light_route",
                "spawn_pose": {"x": 0.0, "y": 0.0, "z": 0.0, "heading": 0.0},
                "goal_pose": {"x": 10.0, "y": 0.0, "z": 0.0, "heading": 0.0},
                "route_points": [
                    {"x": 0.0, "y": 0.0, "heading": 0.0},
                    {"x": 10.0, "y": 0.0, "heading": 0.0},
                ],
            }
        ],
        "signals": [
            {
                "logical_id": "tl_1",
                "apollo_signal_id": "apollo_signal_1",
                "stop_line_id": "stop_line_1",
                "carla_actor_id": "carla_tl_1",
                "lane_ids": ["lane_1"],
            }
        ],
    }


def _traffic_light_mapping(**overrides: object) -> dict:
    entry = {
        "logical_id": "tl_1",
        "apollo_signal_id": "apollo_signal_1",
        "stop_line_id": "stop_line_1",
        "carla_actor_id": "carla_tl_1",
        "lane_ids": ["lane_1"],
        "default_state": "RED",
        "supported_scenarios": ["traffic_light_red_stop"],
    }
    entry.update(overrides)
    return {
        "schema_version": "apollo_traffic_light_gt.v1",
        "traffic_lights": [entry],
    }


def test_carla_actual_mapped_signal_contract_passes_schema_level() -> None:
    report = build_traffic_light_contract_report(
        _town01_contract(),
        _traffic_light_mapping(),
        scenario_class="traffic_light_red_stop",
    )

    assert report["status"] == "pass"
    assert report["claim_grade_ready"] is True
    assert report["mapping_results"][0]["stop_line_lane_overlap_evidence"] is True


def test_missing_apollo_signal_id_is_insufficient_data() -> None:
    report = build_traffic_light_contract_report(
        _town01_contract(),
        _traffic_light_mapping(apollo_signal_id=None),
        scenario_class="traffic_light_red_stop",
    )

    assert report["status"] == "insufficient_data"
    assert report["claim_grade_ready"] is False
    assert "missing_apollo_signal_id" in report["errors"]
