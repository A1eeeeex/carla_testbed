from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.hdmap_projection import analyze_hdmap_projection_rows
from carla_testbed.analysis.route_identity import analyze_route_identity_from_route_contract
from carla_testbed.analysis.routing_contract import analyze_routing_contract_from_route_contract


def _route_contract(**overrides):
    payload = {
        "schema_version": "apollo_route_contract.v1",
        "status": "pass",
        "run_id": "run-1",
        "scenario_id": "lane_keep_097",
        "route_id": "town01_rh_spawn097_goal046",
        "scenario_route_length_m": 120.0,
        "apollo_routing_total_length_m": 121.0,
        "routing_length_ratio": 1.008,
        "goal_xy_error_m": 0.2,
        "start_xy_error_m": 0.1,
        "scenario_start_lane": "lane_a",
        "scenario_goal_lane": "lane_a",
        "scenario_route_lane_keys": ["lane_a"],
        "apollo_routing_lane_keys": ["lane_a"],
        "scenario_start_xy_carla": {"x": 1.0, "y": 2.0},
        "scenario_goal_xy_carla": {"x": 100.0, "y": 2.0},
        "scenario_start_xy_apollo": {"x": 1.0, "y": -2.0},
        "scenario_goal_xy_apollo": {"x": 100.0, "y": -2.0},
        "latest_planning_active_route_segment": {
            "lane_keys": ["lane_a"],
            "length_m": 121.0,
        },
        "route_identity_status": "consistent",
        "route_identity": {"status": "consistent", "issues": []},
        "last_routing_request": {"kind": "long_phase_route"},
        "last_routing_response": {"status": "ok"},
        "startup_route_contract": {"status": "not_applicable"},
        "claim_route_contract": {"status": "pass", "materialized": True, "blocking_reasons": []},
        "blocking_reasons": [],
        "warnings": [],
        "missing_fields": [],
    }
    payload.update(overrides)
    return payload


def test_route_identity_from_consistent_route_contract_passes() -> None:
    report = analyze_route_identity_from_route_contract(_route_contract())

    assert report["status"] == "pass"
    assert report["expected_lane_sequence"] == ["lane_a"]
    assert report["apollo_routing_lane_sequence"] == ["lane_a"]
    assert report["planning_reference_line_lane_sequence"] == ["lane_a"]


def test_route_identity_inconsistent_blocks_claim() -> None:
    report = analyze_route_identity_from_route_contract(
        _route_contract(
            status="fail",
            route_identity_status="inconsistent",
            route_identity={"status": "inconsistent", "issues": ["lane_sequence_mismatch"]},
            blocking_reasons=["route_identity_inconsistent"],
        )
    )

    assert report["status"] == "fail"
    assert "route_identity_inconsistent" in report["blocking_reasons"]
    assert report["route_identity_issues"] == ["lane_sequence_mismatch"]


def test_routing_contract_requires_claim_route_materialized() -> None:
    report = analyze_routing_contract_from_route_contract(
        _route_contract(
            status="fail",
            claim_route_contract={
                "status": "fail",
                "materialized": False,
                "blocking_reasons": ["claim_route_not_materialized"],
            },
            blocking_reasons=["claim_route_not_materialized"],
        )
    )

    assert report["status"] == "fail"
    assert "claim_route_not_materialized" in report["blocking_reasons"]


def test_hdmap_projection_missing_is_insufficient_data() -> None:
    report = analyze_hdmap_projection_rows([])

    assert report["status"] == "insufficient_data"
    assert report["claim_grade"] is False
    assert "apollo_hdmap_projection" in report["missing_fields"]


def test_hdmap_projection_high_heading_error_fails() -> None:
    rows = [
        {
            "timestamp": float(i),
            "source": "apollo_hdmap_api",
            "status": "ok",
            "heading_error_rad": 0.2,
            "lateral_error_m": 0.05,
            "projection_s": float(i),
            "expected_route_distance_m": 60.0,
            "expected_duration_s": 60.0,
        }
        for i in range(60)
    ]

    report = analyze_hdmap_projection_rows(rows)

    assert report["status"] == "fail"
    assert "apollo_hdmap_projection_heading_error_high" in report["blocking_reasons"]


def test_route_identity_cli_writes_report(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    artifacts = run_dir / "artifacts"
    artifacts.mkdir(parents=True)
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "run-1",
                "scenario_id": "lane_keep_097",
                "route_id": "town01_rh_spawn097_goal046",
                "route": {
                    "route_id": "town01_rh_spawn097_goal046",
                    "length_m": 120.0,
                    "start_pose": {"x": 1.0, "y": 2.0},
                    "goal_pose": {"x": 100.0, "y": 2.0},
                    "lane_ids": ["lane_a"],
                },
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "summary.json").write_text(json.dumps({"run_id": "run-1"}), encoding="utf-8")
    (artifacts / "planning_topic_debug_summary.json").write_text(
        json.dumps(
            {
                "routing_response": {
                    "total_length_m": 120.0,
                    "lane_ids": ["lane_a"],
                    "start_xy": {"x": 1.0, "y": 2.0},
                    "goal_xy": {"x": 100.0, "y": 2.0},
                }
            }
        ),
        encoding="utf-8",
    )
    out = tmp_path / "out"

    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_route_identity.py",
            "--run-dir",
            str(run_dir),
            "--out",
            str(out),
        ],
        cwd=Path(__file__).resolve().parents[1],
        text=True,
        capture_output=True,
        check=False,
    )

    assert (out / "route_identity_report.json").exists()
    assert result.returncode in {0, 1}
