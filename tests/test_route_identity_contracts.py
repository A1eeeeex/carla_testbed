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


def test_route_identity_preserves_scenario_route_length_source_and_consistency() -> None:
    report = analyze_route_identity_from_route_contract(
        _route_contract(
            status="fail",
            scenario_route_length_m=229.2,
            scenario_route_length_source="declared_metadata",
            scenario_route_declared_length_m=229.2,
            scenario_route_claim_length_m=None,
            scenario_route_legacy_length_m=229.2,
            scenario_route_legacy_length_role="legacy_selection_straight_line_distance",
            scenario_route_trace_length_m=388.4,
            scenario_route_trace_length_source="route_trace_s_span",
            scenario_route_length_consistency_status="inconsistent",
            scenario_route_length_consistency_reason="declared_route_length_disagrees_with_route_trace",
            route_identity_status="inconsistent",
            route_identity={"status": "inconsistent", "issues": ["scenario_route_length_inconsistent"]},
            blocking_reasons=["scenario_route_length_inconsistent", "route_identity_inconsistent"],
        )
    )

    assert report["status"] == "fail"
    assert report["length_expected_m"] == 229.2
    assert report["length_expected_source"] == "declared_metadata"
    assert report["length_expected_declared_m"] == 229.2
    assert report["length_expected_claim_m"] is None
    assert report["length_expected_legacy_m"] == 229.2
    assert report["length_expected_legacy_role"] == "legacy_selection_straight_line_distance"
    assert report["length_expected_trace_m"] == 388.4
    assert report["length_expected_consistency_status"] == "inconsistent"
    assert "scenario_route_length_inconsistent" in report["blocking_reasons"]


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


def test_routing_contract_preserves_scenario_route_length_source_and_consistency() -> None:
    report = analyze_routing_contract_from_route_contract(
        _route_contract(
            status="fail",
            scenario_route_length_m=229.2,
            scenario_route_length_source="declared_metadata",
            scenario_route_declared_length_m=229.2,
            scenario_route_claim_length_m=None,
            scenario_route_legacy_length_m=229.2,
            scenario_route_legacy_length_role="legacy_selection_straight_line_distance",
            scenario_route_trace_length_m=388.4,
            scenario_route_trace_length_source="route_trace_s_span",
            scenario_route_length_consistency_status="inconsistent",
            scenario_route_length_consistency_reason="declared_route_length_disagrees_with_route_trace",
            claim_route_contract={
                "status": "fail",
                "materialized": False,
                "blocking_reasons": ["scenario_route_length_inconsistent"],
            },
            blocking_reasons=["scenario_route_length_inconsistent"],
        )
    )

    assert report["status"] == "fail"
    assert report["scenario_route_length_m"] == 229.2
    assert report["scenario_route_length_source"] == "declared_metadata"
    assert report["scenario_route_claim_length_m"] is None
    assert report["scenario_route_legacy_length_m"] == 229.2
    assert report["scenario_route_legacy_length_role"] == "legacy_selection_straight_line_distance"
    assert report["scenario_route_trace_length_m"] == 388.4
    assert report["scenario_route_length_consistency_status"] == "inconsistent"
    assert "scenario_route_length_inconsistent" in report["blocking_reasons"]


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
                "metadata": {
                    "scenario_metadata": {
                        "route_id": "town01_rh_spawn097_goal046",
                        "route_length_m": 120.0,
                        "spawn": {"x": 1.0, "y": 2.0},
                        "goal": {"x": 121.0, "y": 2.0},
                        "spawn_lane": {"road_id": 15, "section_id": 0, "lane_id": 1},
                        "goal_lane": {"road_id": 15, "section_id": 0, "lane_id": 1},
                        "route_trace": [
                            {"x": 1.0, "y": 2.0, "s": 0.0, "lane_id": "15:0:1"},
                            {"x": 121.0, "y": 2.0, "s": 120.0, "lane_id": "15:0:1"},
                        ],
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "summary.json").write_text(json.dumps({"run_id": "run-1"}), encoding="utf-8")
    (artifacts / "routing_response_decoded.json").write_text(
        json.dumps(
            {
                "source": "/apollo/routing_response",
                "lane_segments": [{"lane_id": "15_1_1", "start_s": 0.0, "end_s": 120.0}],
                "total_length_m": 120.0,
            }
        ),
        encoding="utf-8",
    )
    (artifacts / "routing_event_debug.jsonl").write_text(
        json.dumps({"start_raw_x": 1.0, "start_raw_y": -2.0, "goal_raw_x": 121.0, "goal_raw_y": -2.0}) + "\n",
        encoding="utf-8",
    )
    (artifacts / "planning_route_segment_debug.jsonl").write_text(
        json.dumps(
            {
                "route_segment_total_length": 120.0,
                "routing_lane_window_count": 1,
                "routing_lane_window_signature": "15_1_1@0->120",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (artifacts / "apollo_hdmap_projection.jsonl").write_text(
        json.dumps({"source": "apollo_hdmap_api", "nearest_lane_id": "15_1_1"}) + "\n",
        encoding="utf-8",
    )
    transform_path = tmp_path / "frame_transform.yaml"
    transform_path.write_text(
        json.dumps(
            {
                "map_name": "Town01",
                "source_frame": "carla_world",
                "target_frame": "apollo_map",
                "transform": {
                    "tx": 0.0,
                    "ty": 0.0,
                    "tz": 0.0,
                    "yaw_rad": 0.0,
                    "scale": 1.0,
                    "y_flip": True,
                },
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
            "--frame-transform",
            str(transform_path),
            "--out",
            str(out),
        ],
        cwd=Path(__file__).resolve().parents[1],
        text=True,
        capture_output=True,
        check=False,
    )

    assert (out / "route_identity_report.json").exists()
    report = json.loads((out / "route_identity_report.json").read_text(encoding="utf-8"))
    assert result.returncode == 0
    assert report["status"] == "pass"
    assert report["route_identity_status"] == "consistent"
