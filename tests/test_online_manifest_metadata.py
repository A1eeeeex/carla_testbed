from __future__ import annotations

import yaml

from carla_testbed.record.manifest_metadata import online_claim_manifest_updates


def test_legacy_online_config_exports_claim_manifest_fields() -> None:
    cfg = yaml.safe_load(
        open(
            "configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml",
            encoding="utf-8",
        )
    )
    cfg["run"]["ticks"] = 420
    cfg["run"]["profile_name"] = "town01_apollo_probe"
    cfg["run"]["capability_profile"] = "lane_keep"
    cfg["run"]["claim_profile"] = True
    cfg["run"]["materialization_probe"] = True

    updates = online_claim_manifest_updates(
        effective_config=cfg,
        scenario_metadata={"route_id": "town01_rh_spawn097_goal046", "map": "Town01"},
        config_path="configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml",
        profile_name="town01_apollo_probe",
    )

    assert updates["scenario_id"] == "lane_keep_town01_rh_spawn097_goal046"
    assert updates["scenario_class"] == "lane_keep"
    assert updates["route_id"] == "town01_rh_spawn097_goal046"
    assert updates["algorithm_variant_id"] == "apollo_10_0_carla_gt_town01_reference"
    assert (
        updates["algorithm_variant_manifest_path"]
        == "configs/algorithms/apollo_variant.carla_gt.example.yaml"
    )
    assert (
        updates["online_config_path"]
        == "configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml"
    )
    assert updates["online_config_profile_name"] == "town01_apollo_probe"
    assert updates["map"] == "Town01"
    assert updates["transport_mode"] == "ros2_gt"
    assert updates["transport_mode_source"] == "online_config.scenario.publish_ros2_gt"
    assert updates["backend"] == "apollo_cyberrt"
    assert updates["truth_input"] is True
    assert updates["claim_profile"] is True
    assert updates["materialization_probe"] is True
    assert updates["fixed_delta_seconds"] == 0.05
    assert updates["ticks"] == 420
    assert updates["duration_s"] == 21.0


def test_manifest_metadata_does_not_invent_algorithm_variant() -> None:
    updates = online_claim_manifest_updates(
        effective_config={
            "run": {"ticks": 10, "fixed_delta_seconds": 0.05, "capability_profile": "lane_keep"},
            "scenario": {"publish_ros2_gt": True},
            "algo": {"stack": "apollo"},
        },
        scenario_metadata={"route_id": "route_a", "map": "Town01"},
    )

    assert updates["backend"] == "apollo_cyberrt"
    assert updates["truth_input"] is True
    assert "algorithm_variant_id" not in updates
    assert "algorithm_variant_manifest_path" not in updates
