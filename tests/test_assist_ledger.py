from __future__ import annotations

import json
from pathlib import Path

from carla_testbed.analysis.assist_ledger import (
    ASSIST_LEDGER_SCHEMA_VERSION,
    build_assist_ledger,
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
