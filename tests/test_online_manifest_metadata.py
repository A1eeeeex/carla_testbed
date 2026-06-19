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
    assert updates["transport_mode"] == "apollo_cyberrt_gt_over_ros2_transition"
    assert updates["canonical_transport_mode"] == "apollo_cyberrt_gt_over_ros2_transition"
    assert updates["legacy_transport_name"] == "ros2_gt"
    assert updates["compat_layers"] == ["ros2_gt_transition", "legacy_route_health_transition"]
    assert updates["transport_mode_source"] == "online_config.scenario.publish_ros2_gt"
    assert updates["backend"] == "apollo_cyberrt"
    assert updates["truth_input"] is True
    assert updates["claim_profile"] is True
    assert updates["materialization_probe"] is True
    assert updates["fixed_delta_seconds"] == 0.05
    assert updates["ticks"] == 420
    assert updates["duration_s"] == 21.0
    assert updates["runtime_contract_status"] == "misconfigured"
    assert "harness_control_disable_unknown" in updates["runtime_contract"]["blockers"]


def test_online_claim_manifest_exports_aligned_runtime_contract_when_external_control_is_explicit() -> None:
    cfg = yaml.safe_load(
        open(
            "configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml",
            encoding="utf-8",
        )
    )
    cfg["run"]["claim_profile"] = True
    cfg["run"]["materialization_probe"] = True

    updates = online_claim_manifest_updates(
        effective_config=cfg,
        scenario_metadata={"route_id": "town01_rh_spawn097_goal046", "map": "Town01"},
        summary={
            "profile": {
                "harness_disable_control_effective": True,
                "external_stack": True,
            },
            "routing_success_count": 1,
            "routing_materialized": True,
            "planning_message_count": 120,
            "planning_nonempty_count": 100,
            "planning_nonempty_trajectory_ratio": 0.833,
            "planning_materialized": True,
            "control_rx_count": 200,
            "control_tx_count": 199,
            "control_handoff_status": "control_consuming_with_nonzero_planning",
        },
    )

    assert updates["runtime_contract_status"] == "aligned"
    assert updates["runtime_contract"]["status"] == "aligned"
    assert updates["runtime_contract"]["blockers"] == []
    assert updates["runtime_contract"]["transport_mode"] == "apollo_cyberrt_gt_over_ros2_transition"
    assert updates["routing_success_count"] == 1
    assert updates["routing_materialized"] is True
    assert updates["planning_message_count"] == 120
    assert updates["planning_nonempty_count"] == 100
    assert updates["planning_nonempty_trajectory_ratio"] == 0.833
    assert updates["planning_materialized"] is True
    assert updates["control_rx_count"] == 200
    assert updates["control_tx_count"] == 199
    assert updates["control_handoff_status"] == "control_consuming_with_nonzero_planning"


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


def test_explicit_run_identity_overrides_legacy_scenario_metadata() -> None:
    updates = online_claim_manifest_updates(
        effective_config={
            "run": {
                "scenario_id": "baguang_lead_decel_70_to_40_20m",
                "scenario_class": "lead_vehicle_decel",
                "route_id": "straight_road_for_baguang_mainline_lead_decel_20m",
                "capability_profile": "phase1_fixed_scene_sidecar",
            },
            "scenario": {"publish_ros2_gt": True},
            "algo": {"stack": "apollo"},
        },
        scenario_metadata={
            "scenario_id": "legacy_followstop",
            "scenario_class": "lane_keep",
            "route_id": "legacy_route",
            "map": "straight_road_for_baguang",
        },
    )

    assert updates["scenario_id"] == "baguang_lead_decel_70_to_40_20m"
    assert updates["scenario_class"] == "lead_vehicle_decel"
    assert updates["route_id"] == "straight_road_for_baguang_mainline_lead_decel_20m"


def test_normalized_legacy_run_identity_overrides_legacy_scenario_metadata() -> None:
    updates = online_claim_manifest_updates(
        effective_config={
            "backend": {
                "params": {
                    "legacy_run": {
                        "scenario_id": "baguang_lead_decel_70_to_40_20m",
                        "scenario_class": "lead_vehicle_decel",
                        "route_id": "straight_road_for_baguang_mainline_lead_decel_20m",
                        "capability_profile": "phase1_fixed_scene_sidecar",
                        "profile_name": "phase1_dynamic_sidecar",
                    }
                }
            },
            "scenario": {"publish_ros2_gt": True},
            "algo": {"stack": "apollo"},
        },
        scenario_metadata={
            "scenario_id": "legacy_followstop",
            "scenario_class": "lane_keep",
            "route_id": "legacy_route",
            "map": "straight_road_for_baguang",
        },
    )

    assert updates["scenario_id"] == "baguang_lead_decel_70_to_40_20m"
    assert updates["scenario_class"] == "lead_vehicle_decel"
    assert updates["route_id"] == "straight_road_for_baguang_mainline_lead_decel_20m"
    assert updates["capability_profile"] == "phase1_fixed_scene_sidecar"
    assert updates["online_config_profile_name"] == "phase1_dynamic_sidecar"
