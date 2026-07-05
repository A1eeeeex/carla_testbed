from __future__ import annotations

import json
from pathlib import Path

from carla_testbed.analysis.assist_ledger import (
    ASSIST_LEDGER_SCHEMA_VERSION,
    build_assist_ledger,
    build_runtime_assist_ledger,
    classify_assists,
    read_assist_ledger_from_run_dir,
)


def test_no_assist_can_claim_unassisted() -> None:
    ledger = build_assist_ledger(manifest={"run_id": "plain"})

    assert ledger["schema_version"] == ASSIST_LEDGER_SCHEMA_VERSION
    assert ledger["active_assists"] == []
    assert ledger["blocking_assists"] == []
    assert ledger["can_claim_unassisted_natural_driving"] is True
    assert ledger["assist_confidence"] == "unknown"


def test_lateral_stabilizer_blocks_unassisted_claim() -> None:
    ledger = build_assist_ledger(
        summary={
            "straight_lane_lateral_stabilizer_enabled": True,
            "straight_lane_lateral_stabilizer_apply_count": 7,
        }
    )

    assert ledger["active_assists"] == ["straight_lane_lateral_stabilizer"]
    assert ledger["blocking_assists"] == ["straight_lane_lateral_stabilizer"]
    assert ledger["can_claim_unassisted_natural_driving"] is False
    assert ledger["assist_confidence"] == "inferred"


def test_terminal_stop_hold_blocks_natural_driving_claim() -> None:
    ledger = build_assist_ledger(manifest={"active_assists": ["terminal_stop_hold"]})

    assert ledger["blocking_assists"] == ["terminal_stop_hold"]
    assert ledger["can_claim_unassisted_natural_driving"] is False
    assert ledger["assist_confidence"] == "explicit"


def test_legacy_and_manual_assists_block_unassisted_claims() -> None:
    ledger = build_assist_ledger(
        manifest={
            "active_assists": [
                "dummy_lateral",
                "legacy_followstop",
                "route_follower",
                "direct_autopilot",
                "manual_intervention",
            ]
        }
    )

    assert ledger["active_assists"] == [
        "direct_autopilot",
        "dummy_lateral",
        "legacy_followstop",
        "manual_intervention",
        "route_follower",
    ]
    assert ledger["blocking_assists"] == ledger["active_assists"]
    assert ledger["can_claim_unassisted_natural_driving"] is False


def test_steering_sign_diagnostic_override_blocks_unassisted_claims() -> None:
    ledger = build_assist_ledger(
        config={
            "assist_ledger": {
                "active_assists": ["steering_sign_diagnostic_override"],
                "assist_confidence": "explicit",
            }
        }
    )

    assert ledger["active_assists"] == ["steering_sign_diagnostic_override"]
    assert ledger["blocking_assists"] == ["steering_sign_diagnostic_override"]
    assert ledger["can_claim_unassisted_natural_driving"] is False
    assert ledger["assist_confidence"] == "explicit"


def test_runtime_count_fields_infer_blocking_assists() -> None:
    ledger = build_assist_ledger(
        bridge_stats={
            "legacy_followstop_apply_count": 1,
            "route_follower_apply_count": 2,
            "direct_autopilot_apply_count": 3,
            "manual_intervention_count": 4,
        }
    )

    assert ledger["active_assists"] == [
        "direct_autopilot",
        "legacy_followstop",
        "manual_intervention",
        "route_follower",
    ]
    assert ledger["blocking_assists"] == ledger["active_assists"]
    assert ledger["assist_confidence"] == "inferred"
    assert ledger["can_claim_unassisted_natural_driving"] is False


def test_runtime_mode_fields_infer_blocking_assists() -> None:
    route = build_assist_ledger(summary={"control_source": "route_follower"})
    auto = build_assist_ledger(summary={"controller": "carla_autopilot"})
    manual = build_assist_ledger(summary={"controller": "manual_control"})
    force_green = build_assist_ledger(summary={"traffic_light_expectation": {"stimulus_mode": "force_green"}})

    assert route["blocking_assists"] == ["route_follower"]
    assert auto["blocking_assists"] == ["direct_autopilot"]
    assert manual["blocking_assists"] == ["manual_intervention"]
    assert force_green["blocking_assists"] == ["force_green"]


def test_online_summary_lateral_mode_dummy_blocks_unassisted_claim() -> None:
    ledger = build_assist_ledger(
        summary={
            "lateral_mode": "dummy",
            "controller": "composite",
            "policy_mode": "acc",
        }
    )

    assert ledger["active_assists"] == ["dummy_lateral"]
    assert ledger["blocking_assists"] == ["dummy_lateral"]
    assert ledger["assist_sources"] == {"dummy_lateral": "summary"}
    assert ledger["assist_confidence"] == "inferred"
    assert ledger["can_claim_unassisted_natural_driving"] is False


def test_external_stack_legacy_placeholder_does_not_create_dummy_assist() -> None:
    ledger = build_runtime_assist_ledger(
        config={
            "assist_ledger": {
                "schema_version": ASSIST_LEDGER_SCHEMA_VERSION,
                "active_assists": [],
                "assist_confidence": "explicit",
                "can_claim_unassisted_natural_driving": True,
            }
        },
        summary={
            "controller": "external_stack",
            "lateral_mode": "dummy",
            "control_source": "external_stack",
            "harness_control_disabled": True,
            "legacy_controller_role": "compatibility_placeholder",
            "legacy_controller_applied": False,
        },
    )

    assert ledger["active_assists"] == []
    assert ledger["blocking_assists"] == []
    assert ledger["assist_confidence"] == "explicit"
    assert ledger["source_artifact"] == "config"
    assert ledger["can_claim_unassisted_natural_driving"] is True
    assert "assist_ledger_unknown_unassisted_claim_not_allowed" not in ledger.get("warnings", [])


def test_nested_legacy_placeholder_does_not_create_dummy_assist() -> None:
    ledger = build_runtime_assist_ledger(
        config={
            "assist_ledger": {
                "schema_version": ASSIST_LEDGER_SCHEMA_VERSION,
                "active_assists": [],
                "assist_confidence": "explicit",
                "can_claim_unassisted_natural_driving": True,
            }
        },
        summary={
            "controller": "external_stack",
            "control_source": "external_stack",
            "harness_control_disabled": True,
            "legacy_controller_applied": False,
            "legacy_controller_placeholder": {
                "controller": "composite",
                "lateral_mode": "dummy",
                "policy_mode": "acc",
                "role": "compatibility_placeholder",
                "applied": False,
            },
        },
    )

    assert ledger["active_assists"] == []
    assert ledger["blocking_assists"] == []
    assert ledger["assist_confidence"] == "explicit"
    assert ledger["can_claim_unassisted_natural_driving"] is True


def test_runtime_without_assist_declaration_cannot_claim_even_without_active_assists() -> None:
    ledger = build_runtime_assist_ledger(
        summary={
            "controller": "external_stack",
            "control_source": "external_stack",
            "harness_control_disabled": True,
            "legacy_controller_applied": False,
        }
    )

    assert ledger["active_assists"] == []
    assert ledger["assist_confidence"] == "unknown"
    assert ledger["can_claim_unassisted_natural_driving"] is False
    assert "assist_ledger_unknown_unassisted_claim_not_allowed" in ledger["warnings"]


def test_force_green_is_blocking_assist_for_claim_boundary() -> None:
    ledger = build_assist_ledger(
        manifest={
            "traffic_light": {"policy": "force_green"},
        }
    )

    assert ledger["active_assists"] == ["force_green"]
    assert ledger["blocking_assists"] == ["force_green"]
    assert ledger["can_claim_unassisted_natural_driving"] is False


def test_direct_transport_alone_is_recorded_but_non_blocking() -> None:
    ledger = build_assist_ledger(manifest={"backend": "carla_direct"})

    assert ledger["active_assists"] == ["carla_direct_transport"]
    assert ledger["blocking_assists"] == []
    assert ledger["non_blocking_assists"] == ["carla_direct_transport"]
    assert ledger["can_claim_unassisted_natural_driving"] is True


def test_autoware_overrides_block_unless_marked_diagnostic_only() -> None:
    blocking = build_assist_ledger(
        config={
            "algo": {
                "autoware": {
                    "launch_goal_planner_module": False,
                    "planning_common_max_vel_mps": 22.22,
                    "carla_control_bridge": {"longitudinal_mode": "speed_feedback"},
                }
            }
        }
    )

    assert blocking["blocking_assists"] == [
        "goal_planner_module_disabled",
        "planning_common_dynamics_override",
        "speed_feedback_bridge_profile",
    ]
    assert blocking["can_claim_unassisted_natural_driving"] is False

    diagnostic = classify_assists(
        ["goal_planner_module_disabled"],
        diagnostic_only_assists=["goal_planner_module_disabled"],
    )

    assert diagnostic["blocking_assists"] == []
    assert diagnostic["non_blocking_assists"] == ["goal_planner_module_disabled"]
    assert diagnostic["can_claim_unassisted_natural_driving"] is True


def test_read_assist_ledger_from_run_dir_prefers_explicit_file(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    payload = build_assist_ledger(manifest={"active_assists": ["straight_acc_override"]})
    (run_dir / "assist_ledger.json").write_text(json.dumps(payload), encoding="utf-8")

    ledger = read_assist_ledger_from_run_dir(run_dir)

    assert ledger["active_assists"] == ["straight_acc_override"]
    assert ledger["blocking_assists"] == ["straight_acc_override"]
