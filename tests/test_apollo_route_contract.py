from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.apollo_route_contract import (
    APOLLO_ROUTE_CONTRACT_SCHEMA_VERSION,
    ROUTE_DEFINITION_CLAIM_SCHEMA_VERSION,
    analyze_apollo_route_contract_run_dir,
    build_lane_equivalence_town01,
    build_route_definition_claim,
    write_apollo_route_contract_report,
)

FRAME_TRANSFORM = {
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


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")


def _base_run(tmp_path: Path) -> Path:
    run_dir = tmp_path / "run"
    _write_json(
        run_dir / "summary.json",
        {
            "run_id": "run",
            "scenario_id": "lane_keep_097",
            "scenario_class": "lane_keep",
            "route_id": "town01_rh_spawn097_goal046",
        },
    )
    _write_json(
        run_dir / "manifest.json",
        {
            "run_id": "run",
            "route_id": "town01_rh_spawn097_goal046",
            "metadata": {
                "scenario_metadata": {
                    "route_id": "town01_rh_spawn097_goal046",
                    "route_length_m": 230.0,
                    "spawn": {"x": 2.0, "y": 249.0},
                    "goal": {"x": 2.0, "y": 19.0},
                    "spawn_lane": {"road_id": 15, "section_id": 0, "lane_id": 1},
                    "goal_lane": {"road_id": 15, "section_id": 0, "lane_id": 1},
                    "route_trace": [
                        {"x": 2.0, "y": 249.0, "s": 0.0, "lane_id": "15:0:1"},
                        {"x": 2.0, "y": 19.0, "s": 230.0, "lane_id": "15:0:1"},
                    ],
                }
            },
        },
    )
    return run_dir


def test_route_contract_detects_apollo_routing_length_mismatch(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "artifacts/planning_topic_debug_summary.json",
        {
            "last_routing_total_length": 648.0101208441594,
            "last_routing_lane_window_count": 17,
            "last_routing_lane_window_signature": (
                "15_1_1@66.9->307.6 | 13_1_-1@0->14.0 | "
                "3_1_1@0->68.3 | 82_4_1@0->1.3"
            ),
            "last_routing_unique_lane_signature": "15_1_1 | 13_1_-1 | 3_1_1 | 82_4_1",
        },
    )

    report = analyze_apollo_route_contract_run_dir(run_dir, frame_transform=FRAME_TRANSFORM)

    assert report["schema_version"] == APOLLO_ROUTE_CONTRACT_SCHEMA_VERSION
    assert report["status"] == "fail"
    assert report["routing_length_ratio"] > 2.0
    assert "apollo_routing_length_mismatch" in report["blocking_reasons"]
    assert "apollo_routing_lane_sequence_mismatch" in report["blocking_reasons"]
    assert "route_identity_inconsistent" in report["blocking_reasons"]


def test_cross_namespace_carla_waypoint_lane_mismatch_requires_equivalence_not_fail(
    tmp_path: Path,
) -> None:
    run_dir = _base_run(tmp_path)
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    meta = manifest["metadata"]["scenario_metadata"]
    meta.update(
        {
            "route_length_m": 229.18,
            "route_length_m_role": "legacy_selection_straight_line_distance",
            "route_length_m_claim_grade": False,
            "claim_route_length_m": 388.41,
            "claim_route_length_source": "route_trace_s_span",
            "route_trace_source": "town01_forward_waypoint_trace",
            "spawn": {"x": 2.0, "y": 208.6},
            "goal": {"x": 154.0, "y": 37.1},
            "spawn_lane": {"road_id": 15, "section_id": 0, "lane_id": 1},
            "goal_lane": {"road_id": 25, "section_id": 0, "lane_id": -1},
            "route_trace": [
                {"x": 2.0, "y": 208.6, "s": 0.0, "lane_id": "15:0:1"},
                {"x": 2.0, "y": 8.6, "s": 200.0, "lane_id": "13:0:-1"},
                {"x": 80.0, "y": 1.9, "s": 280.0, "lane_id": "117:0:1"},
                {"x": 145.0, "y": 1.9, "s": 346.0, "lane_id": "83:0:1"},
                {"x": 154.0, "y": 37.1, "s": 388.41, "lane_id": "25:0:-1"},
            ],
        }
    )
    _write_json(run_dir / "manifest.json", manifest)
    _write_json(
        run_dir / "artifacts/routing_response_decoded.json",
        {
            "source": "/apollo/raw_routing_response",
            "status": "pass",
            "lane_segments": [
                {"lane_id": "15_1_1", "start_s": 107.5, "end_s": 307.6},
                {"lane_id": "13_1_-1", "start_s": 0.0, "end_s": 14.0},
                {"lane_id": "3_1_1", "start_s": 0.0, "end_s": 68.3},
                {"lane_id": "82_4_1", "start_s": 0.0, "end_s": 1.3},
                {"lane_id": "82_3_1", "start_s": 0.0, "end_s": 10.9},
                {"lane_id": "82_2_1", "start_s": 0.0, "end_s": 9.6},
                {"lane_id": "82_1_1", "start_s": 0.0, "end_s": 1.3},
                {"lane_id": "2_1_1", "start_s": 0.0, "end_s": 42.2},
                {"lane_id": "31_1_-1", "start_s": 0.0, "end_s": 15.6},
                {"lane_id": "25_1_-1", "start_s": 0.0, "end_s": 26.3},
            ],
            "total_length_m": 390.13,
        },
    )
    _write_jsonl(
        run_dir / "artifacts/routing_event_debug.jsonl",
        [
            {
                "routing_phase": "claim",
                "routing_request_kind": "claim_route",
                "goal_mode": "scenario_xy",
                "goal_source": "scenario_goal_file",
                "start_raw_x": 2.0,
                "start_raw_y": -208.6,
                "goal_raw_x": 154.0,
                "goal_raw_y": -37.1,
                "goal_projection": {
                    "applied": True,
                    "accepted": True,
                    "trusted_lane_centerline": True,
                    "distance_m": 0.01,
                    "signed_lateral_error_m": 0.01,
                },
            }
        ],
    )
    _write_jsonl(
        run_dir / "artifacts/apollo_hdmap_projection.jsonl",
        [
            {
                "timestamp": float(index),
                "nearest_lane_id": "15_1_1",
                "source": "apollo_hdmap_api",
                "status": "ok",
            }
            for index in range(60)
        ],
    )

    report = analyze_apollo_route_contract_run_dir(run_dir, frame_transform=FRAME_TRANSFORM)

    assert report["status"] == "warn"
    assert report["lane_equivalence_status"] == "cross_namespace_unverified"
    assert report["scenario_lane_namespace"] == "carla_waypoint"
    assert "scenario_apollo_lane_namespace_equivalence_unverified" in report["warnings"]
    assert "apollo_routing_lane_sequence_mismatch" not in report["blocking_reasons"]
    assert "apollo_routing_missing_scenario_lane" not in report["blocking_reasons"]
    assert report["claim_route_contract"]["materialized"] is True


def test_cross_namespace_lane_equivalence_can_be_verified_from_route_projection_rows(
    tmp_path: Path,
) -> None:
    run_dir = _base_run(tmp_path)
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    meta = manifest["metadata"]["scenario_metadata"]
    meta.update(
        {
            "route_length_m": 229.18,
            "route_length_m_role": "legacy_selection_straight_line_distance",
            "route_length_m_claim_grade": False,
            "claim_route_length_m": 388.41,
            "claim_route_length_source": "route_trace_s_span",
            "route_trace_source": "town01_forward_waypoint_trace",
            "spawn": {"x": 2.0, "y": 208.6},
            "goal": {"x": 154.0, "y": 37.1},
            "spawn_lane": {"road_id": 15, "section_id": 0, "lane_id": 1},
            "goal_lane": {"road_id": 25, "section_id": 0, "lane_id": -1},
            "route_trace": [
                {"x": 2.0, "y": 208.6, "s": 0.0, "lane_id": "15:0:1"},
                {"x": 2.0, "y": 8.6, "s": 200.0, "lane_id": "13:0:-1"},
                {"x": 15.0, "y": 2.0, "s": 216.0, "lane_id": "3:0:1"},
                {"x": 80.0, "y": 1.9, "s": 280.0, "lane_id": "117:0:1"},
                {"x": 105.0, "y": 1.9, "s": 306.0, "lane_id": "2:0:1"},
                {"x": 145.0, "y": 1.9, "s": 346.0, "lane_id": "83:0:1"},
                {"x": 154.0, "y": 37.1, "s": 388.41, "lane_id": "25:0:-1"},
            ],
        }
    )
    _write_json(run_dir / "manifest.json", manifest)
    _write_json(
        run_dir / "artifacts/routing_response_decoded.json",
        {
            "source": "/apollo/raw_routing_response",
            "status": "pass",
            "lane_segments": [
                {"lane_id": "15_1_1", "start_s": 107.5, "end_s": 307.6},
                {"lane_id": "13_1_-1", "start_s": 0.0, "end_s": 14.0},
                {"lane_id": "3_1_1", "start_s": 0.0, "end_s": 68.3},
                {"lane_id": "82_4_1", "start_s": 0.0, "end_s": 1.3},
                {"lane_id": "82_3_1", "start_s": 0.0, "end_s": 10.9},
                {"lane_id": "82_2_1", "start_s": 0.0, "end_s": 9.6},
                {"lane_id": "82_1_1", "start_s": 0.0, "end_s": 1.3},
                {"lane_id": "2_1_1", "start_s": 0.0, "end_s": 42.2},
                {"lane_id": "31_1_-1", "start_s": 0.0, "end_s": 15.6},
                {"lane_id": "25_1_-1", "start_s": 0.0, "end_s": 26.3},
            ],
            "total_length_m": 390.13,
        },
    )
    trusted_projection = {
        "applied": True,
        "accepted": True,
        "trusted_lane_centerline": True,
        "distance_m": 0.01,
        "signed_lateral_error_m": 0.01,
    }
    _write_jsonl(
        run_dir / "artifacts/routing_event_debug.jsonl",
        [
            {
                "routing_phase": "claim",
                "routing_request_kind": "claim_route",
                "start_projection": trusted_projection,
                "goal_projection": trusted_projection,
            }
        ],
    )
    _write_jsonl(
        run_dir / "artifacts/planning_route_segment_debug.jsonl",
        [
            {
                "route_segment_total_length": 390.13,
                "routing_lane_window_count": 7,
                "routing_lane_window_signature": "15_1_1 | 13_1_-1 | 3_1_1 | 82_4_1 | 2_1_1 | 31_1_-1 | 25_1_-1",
                "routing_unique_lane_signature": "15_1_1 | 13_1_-1 | 3_1_1 | 82_4_1 | 82_3_1 | 82_2_1 | 82_1_1 | 2_1_1 | 31_1_-1 | 25_1_-1",
            }
        ],
    )
    rows = _lane_equivalence_projection_rows(
        [
            ("15:1", "15_1_1"),
            ("13:-1", "13_1_-1"),
            ("3:1", "3_1_1"),
            ("117:1", "82_4_1"),
            ("2:1", "2_1_1"),
            ("83:1", "31_1_-1"),
            ("25:-1", "25_1_-1"),
        ]
    )
    _write_jsonl(run_dir / "artifacts/apollo_hdmap_projection.jsonl", rows)

    report = analyze_apollo_route_contract_run_dir(run_dir, frame_transform=FRAME_TRANSFORM)
    lane_report = build_lane_equivalence_town01(report, hdmap_projection_rows=rows)

    assert report["lane_equivalence_status"] == "verified_equivalence"
    assert "scenario_apollo_lane_namespace_equivalence_unverified" not in report["warnings"]
    assert lane_report["status"] == "pass"
    pairs = {
        row["scenario_lane_key"]: row["apollo_lane_key"]
        for row in lane_report["equivalence"]
    }
    assert pairs["117:1"] == "82:1"
    assert pairs["83:1"] == "31:-1"
    assert all(row["confidence"] == "verified" for row in lane_report["equivalence"])


def test_boundary_heading_ambiguous_lane_equivalence_is_not_hard_fail() -> None:
    report = {
        "lane_equivalence_status": "cross_namespace_unverified",
        "scenario_route_lane_sequence": ["13:-1"],
        "apollo_routing_lane_sequence": ["13:-1"],
    }
    rows = [
        {
            "source": "apollo_hdmap_api",
            "status": "ok",
            "projection_status": "ok",
            "sample_type": "route",
            "route_index": 0,
            "route_s": 200.0,
            "carla_lane_key": "13:-1",
            "nearest_lane_id": "13_1_-1",
            "projection_l": 0.001,
            "lateral_error_m": 0.001,
            "heading_error_rad": 0.09,
            "route_chord_heading_error_rad": 0.09,
            "route_trace_heading_error_rad": 0.09,
            "lane_heading_at_s": 0.0,
        },
        {
            "source": "apollo_hdmap_api",
            "status": "ok",
            "projection_status": "ok",
            "sample_type": "route",
            "route_index": 1,
            "route_s": 207.6,
            "carla_lane_key": "13:-1",
            "nearest_lane_id": "13_1_-1",
            "projection_l": 0.002,
            "lateral_error_m": 0.002,
            "heading_error_rad": 0.02,
            "route_chord_heading_error_rad": 0.02,
            "route_trace_heading_error_rad": 0.01,
            "lane_heading_at_s": 0.0,
        },
    ]

    lane_report = build_lane_equivalence_town01(report, hdmap_projection_rows=rows)

    assert lane_report["status"] == "ambiguous"
    assert "lane_equivalence_ambiguous_for_13:-1" in lane_report["missing_fields"]
    assert "13:-1:heading_error_p95_high" in lane_report["warnings"]
    assert "13:-1:heading_error_p95_high" not in lane_report["blocking_reasons"]
    assert lane_report["equivalence"][0]["status"] == "ambiguous"


def test_cross_namespace_lane_mismatch_without_hdmap_projection_is_insufficient_not_fail(
    tmp_path: Path,
) -> None:
    run_dir = _base_run(tmp_path)
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    meta = manifest["metadata"]["scenario_metadata"]
    meta.update(
        {
            "route_length_m": 229.18,
            "route_length_m_role": "legacy_selection_straight_line_distance",
            "route_length_m_claim_grade": False,
            "claim_route_length_m": 388.41,
            "claim_route_length_source": "route_trace_s_span",
            "route_trace_source": "town01_forward_waypoint_trace",
            "spawn": {"x": 2.0, "y": 208.6},
            "goal": {"x": 154.0, "y": 37.1},
            "spawn_lane": {"road_id": 15, "section_id": 0, "lane_id": 1},
            "goal_lane": {"road_id": 25, "section_id": 0, "lane_id": -1},
            "route_trace": [
                {"x": 2.0, "y": 208.6, "s": 0.0, "lane_id": "15:0:1"},
                {"x": 2.0, "y": 8.6, "s": 200.0, "lane_id": "13:0:-1"},
                {"x": 80.0, "y": 1.9, "s": 280.0, "lane_id": "117:0:1"},
                {"x": 145.0, "y": 1.9, "s": 346.0, "lane_id": "83:0:1"},
                {"x": 154.0, "y": 37.1, "s": 388.41, "lane_id": "25:0:-1"},
            ],
        }
    )
    _write_json(run_dir / "manifest.json", manifest)
    _write_json(
        run_dir / "artifacts/routing_response_decoded.json",
        {
            "source": "/apollo/raw_routing_response",
            "status": "pass",
            "lane_segments": [
                {"lane_id": "15_1_1", "start_s": 107.5, "end_s": 307.6},
                {"lane_id": "13_1_-1", "start_s": 0.0, "end_s": 14.0},
                {"lane_id": "3_1_1", "start_s": 0.0, "end_s": 68.3},
                {"lane_id": "82_4_1", "start_s": 0.0, "end_s": 1.3},
                {"lane_id": "82_3_1", "start_s": 0.0, "end_s": 10.9},
                {"lane_id": "82_2_1", "start_s": 0.0, "end_s": 9.6},
                {"lane_id": "82_1_1", "start_s": 0.0, "end_s": 1.3},
                {"lane_id": "2_1_1", "start_s": 0.0, "end_s": 42.2},
                {"lane_id": "31_1_-1", "start_s": 0.0, "end_s": 15.6},
                {"lane_id": "25_1_-1", "start_s": 0.0, "end_s": 26.3},
            ],
            "total_length_m": 390.13,
        },
    )
    _write_jsonl(
        run_dir / "artifacts/routing_event_debug.jsonl",
        [
            {
                "routing_phase": "claim",
                "routing_request_kind": "claim_route",
                "goal_mode": "scenario_xy",
                "goal_source": "scenario_goal_file",
                "start_raw_x": 2.0,
                "start_raw_y": -208.6,
                "goal_raw_x": 154.0,
                "goal_raw_y": -37.1,
                "goal_projection": {
                    "applied": True,
                    "accepted": True,
                    "trusted_lane_centerline": True,
                    "distance_m": 0.01,
                    "signed_lateral_error_m": 0.01,
                },
            }
        ],
    )

    report = analyze_apollo_route_contract_run_dir(run_dir, frame_transform=FRAME_TRANSFORM)

    assert report["status"] == "insufficient_data"
    assert report["lane_equivalence_status"] == "cross_namespace_unverified"
    assert report["scenario_lane_namespace"] == "carla_waypoint"
    assert report["lane_namespaces_directly_comparable"] is False
    assert "apollo_hdmap_projection_for_lane_equivalence" in report["missing_fields"]
    assert "scenario_apollo_lane_namespace_equivalence_unverified" in report["warnings"]
    assert "apollo_hdmap_projection_required_for_cross_namespace_lane_equivalence" in report["warnings"]
    assert "apollo_routing_lane_sequence_mismatch" not in report["blocking_reasons"]
    assert "apollo_routing_missing_scenario_lane" not in report["blocking_reasons"]


def test_route_contract_writes_lane_equivalence_artifact(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "artifacts/routing_response_decoded.json",
        {
            "source": "/apollo/routing_response",
            "status": "pass",
            "lane_segments": [{"lane_id": "15_1_1", "start_s": 0.0, "end_s": 230.0}],
            "total_length_m": 230.0,
        },
    )
    _write_jsonl(
        run_dir / "artifacts/routing_event_debug.jsonl",
        [
            {
                "routing_phase": "claim",
                "routing_request_kind": "claim_route",
                "start_raw_x": 2.0,
                "start_raw_y": -249.0,
                "goal_raw_x": 2.0,
                "goal_raw_y": -19.0,
            }
        ],
    )

    report = analyze_apollo_route_contract_run_dir(run_dir, frame_transform=FRAME_TRANSFORM)
    lane_report = build_lane_equivalence_town01(report)
    outputs = write_apollo_route_contract_report(report, run_dir / "analysis" / "apollo_route_contract")

    assert lane_report["schema_version"] == "lane_equivalence_town01.v2"
    assert "lane_equivalence_town01_artifact" in outputs
    lane_path = run_dir / "artifacts" / "lane_equivalence_town01.json"
    assert lane_path.is_file()
    payload = json.loads(lane_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "lane_equivalence_town01.v2"


def test_cross_namespace_lane_mismatch_uses_trusted_routing_projection_when_transform_missing(
    tmp_path: Path,
) -> None:
    run_dir = _base_run(tmp_path)
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    meta = manifest["metadata"]["scenario_metadata"]
    meta.update(
        {
            "route_length_m": 229.18,
            "route_length_m_role": "legacy_selection_straight_line_distance",
            "claim_route_length_m": 388.41,
            "claim_route_length_source": "route_trace_s_span",
            "route_trace_source": "town01_forward_waypoint_trace",
            "spawn": {"x": 2.0, "y": 208.6},
            "goal": {"x": 154.0, "y": 37.1},
            "spawn_lane": {"road_id": 15, "section_id": 0, "lane_id": 1},
            "goal_lane": {"road_id": 25, "section_id": 0, "lane_id": -1},
            "route_trace": [
                {"x": 2.0, "y": 208.6, "s": 0.0, "lane_id": "15:0:1"},
                {"x": 2.0, "y": 8.6, "s": 200.0, "lane_id": "13:0:-1"},
                {"x": 80.0, "y": 1.9, "s": 280.0, "lane_id": "117:0:1"},
                {"x": 145.0, "y": 1.9, "s": 346.0, "lane_id": "83:0:1"},
                {"x": 154.0, "y": 37.1, "s": 388.41, "lane_id": "25:0:-1"},
            ],
        }
    )
    _write_json(run_dir / "manifest.json", manifest)
    _write_json(
        run_dir / "artifacts/routing_response_decoded.json",
        {
            "source": "/apollo/raw_routing_response",
            "status": "pass",
            "lane_segments": [
                {"lane_id": "15_1_1", "start_s": 107.5, "end_s": 307.6},
                {"lane_id": "13_1_-1", "start_s": 0.0, "end_s": 14.0},
                {"lane_id": "3_1_1", "start_s": 0.0, "end_s": 68.3},
                {"lane_id": "82_4_1", "start_s": 0.0, "end_s": 1.3},
                {"lane_id": "82_3_1", "start_s": 0.0, "end_s": 10.9},
                {"lane_id": "2_1_1", "start_s": 0.0, "end_s": 42.2},
                {"lane_id": "31_1_-1", "start_s": 0.0, "end_s": 15.6},
                {"lane_id": "25_1_-1", "start_s": 0.0, "end_s": 26.3},
            ],
            "total_length_m": 390.13,
        },
    )
    trusted_projection = {
        "applied": True,
        "accepted": True,
        "trusted_lane_centerline": True,
        "distance_m": 0.01,
        "signed_lateral_error_m": 0.01,
    }
    _write_jsonl(
        run_dir / "artifacts/routing_event_debug.jsonl",
        [
            {
                "routing_phase": "claim",
                "routing_request_kind": "claim_route",
                "goal_mode": "scenario_xy",
                "goal_source": "scenario_goal_file",
                "start_projection": trusted_projection,
                "goal_projection": trusted_projection,
            }
        ],
    )

    report = analyze_apollo_route_contract_run_dir(run_dir)

    assert report["status"] == "insufficient_data"
    assert report["comparison_frame"] == "unavailable"
    assert report["routing_request_projection_compatible"] is True
    assert report["lane_equivalence_status"] == "cross_namespace_unverified"
    assert "routing_request_projection_used_for_endpoint_compatibility" in report["warnings"]
    assert "apollo_hdmap_projection_for_lane_equivalence" in report["missing_fields"]
    assert "apollo_routing_lane_sequence_mismatch" not in report["blocking_reasons"]
    assert "apollo_routing_missing_scenario_lane" not in report["blocking_reasons"]


def test_route_contract_blocks_inconsistent_scenario_route_length_sources(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    route_trace = manifest["metadata"]["scenario_metadata"]["route_trace"]
    route_trace[-1]["s"] = 388.0
    route_trace[-1]["x"] = 154.0
    route_trace[-1]["y"] = 37.0
    _write_json(run_dir / "manifest.json", manifest)
    _write_json(
        run_dir / "artifacts/routing_response_decoded.json",
        {
            "source": "/apollo/routing_response",
            "lane_segments": [{"lane_id": "15_1_1", "start_s": 66.9, "end_s": 296.9}],
            "total_length_m": 230.0,
        },
    )
    _write_jsonl(
        run_dir / "artifacts/routing_event_debug.jsonl",
        [
            {
                "routing_phase": "claim",
                "routing_request_kind": "claim_route",
                "start_raw_x": 2.0,
                "start_raw_y": -249.0,
                "goal_raw_x": 2.0,
                "goal_raw_y": -19.0,
            }
        ],
    )
    _write_jsonl(
        run_dir / "artifacts/planning_route_segment_debug.jsonl",
        [
            {
                "route_segment_total_length": 230.0,
                "routing_lane_window_count": 1,
                "routing_lane_window_signature": "15_1_1@66.9->296.9",
                "routing_unique_lane_signature": "15_1_1",
            }
        ],
    )
    _write_jsonl(
        run_dir / "artifacts/apollo_hdmap_projection.jsonl",
        [{"source": "apollo_hdmap_api", "nearest_lane_id": "15_1_1"}],
    )

    report = analyze_apollo_route_contract_run_dir(run_dir, frame_transform=FRAME_TRANSFORM)

    assert report["status"] == "fail"
    assert report["scenario_route_length_m"] == 230.0
    assert report["scenario_route_length_source"] == "declared_metadata"
    assert report["scenario_route_trace_length_m"] == 388.0
    assert report["scenario_route_length_consistency_status"] == "inconsistent"
    assert report["scenario_route_length_consistency_reason"] == (
        "declared_route_length_disagrees_with_route_trace"
    )
    assert "scenario_route_length_inconsistent" in report["blocking_reasons"]
    assert "scenario_route_length_inconsistent" in report["route_identity_issues"]


def test_route_contract_prefers_claim_trace_length_over_legacy_selection_length(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    scenario = manifest["metadata"]["scenario_metadata"]
    scenario["route_length_m"] = 229.2
    scenario["route_length_m_role"] = "legacy_selection_straight_line_distance"
    scenario["route_length_m_claim_grade"] = False
    scenario["claim_route_length_m"] = 388.0
    scenario["claim_route_length_source"] = "route_trace_s_span"
    scenario["route_trace_length_m"] = 388.0
    scenario["route_trace_length_source"] = "route_trace_s_span"
    scenario["goal"] = {"x": 154.0, "y": 37.0}
    route_trace = scenario["route_trace"]
    route_trace[-1]["s"] = 388.0
    route_trace[-1]["x"] = 154.0
    route_trace[-1]["y"] = 37.0
    _write_json(run_dir / "manifest.json", manifest)
    _write_json(
        run_dir / "artifacts/routing_response_decoded.json",
        {
            "source": "/apollo/routing_response",
            "lane_segments": [{"lane_id": "15_1_1", "start_s": 66.9, "end_s": 454.9}],
            "total_length_m": 388.0,
        },
    )
    _write_jsonl(
        run_dir / "artifacts/routing_event_debug.jsonl",
        [
            {
                "routing_phase": "claim",
                "routing_request_kind": "claim_route",
                "start_raw_x": 2.0,
                "start_raw_y": -249.0,
                "goal_raw_x": 154.0,
                "goal_raw_y": -37.0,
            }
        ],
    )
    _write_jsonl(
        run_dir / "artifacts/planning_route_segment_debug.jsonl",
        [
            {
                "route_segment_total_length": 388.0,
                "routing_lane_window_count": 1,
                "routing_lane_window_signature": "15_1_1@66.9->454.9",
                "routing_unique_lane_signature": "15_1_1",
            }
        ],
    )
    _write_jsonl(
        run_dir / "artifacts/apollo_hdmap_projection.jsonl",
        [{"source": "apollo_hdmap_api", "nearest_lane_id": "15_1_1"}],
    )

    report = analyze_apollo_route_contract_run_dir(run_dir, frame_transform=FRAME_TRANSFORM)

    assert report["status"] == "pass"
    assert report["scenario_route_length_m"] == 388.0
    assert report["scenario_route_length_source"] == "route_trace_s_span"
    assert report["scenario_route_claim_length_m"] == 388.0
    assert report["scenario_route_legacy_length_m"] == 229.2
    assert report["scenario_route_legacy_length_role"] == "legacy_selection_straight_line_distance"
    assert report["scenario_route_length_consistency_status"] == "consistent"
    assert report["scenario_route_length_consistency_reason"] == "claim_route_length_matches_route_trace"
    assert "scenario_route_length_inconsistent" not in report["blocking_reasons"]


def test_route_contract_uses_route_trace_length_when_declared_length_missing(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    manifest["metadata"]["scenario_metadata"].pop("route_length_m")
    _write_json(run_dir / "manifest.json", manifest)
    _write_json(
        run_dir / "artifacts/routing_response_decoded.json",
        {
            "source": "/apollo/routing_response",
            "lane_segments": [{"lane_id": "15_1_1", "start_s": 66.9, "end_s": 296.9}],
            "total_length_m": 230.0,
        },
    )
    _write_jsonl(
        run_dir / "artifacts/routing_event_debug.jsonl",
        [
            {
                "routing_phase": "claim",
                "routing_request_kind": "claim_route",
                "start_raw_x": 2.0,
                "start_raw_y": -249.0,
                "goal_raw_x": 2.0,
                "goal_raw_y": -19.0,
            }
        ],
    )
    _write_jsonl(
        run_dir / "artifacts/planning_route_segment_debug.jsonl",
        [
            {
                "route_segment_total_length": 230.0,
                "routing_lane_window_count": 1,
                "routing_lane_window_signature": "15_1_1@66.9->296.9",
                "routing_unique_lane_signature": "15_1_1",
            }
        ],
    )
    _write_jsonl(
        run_dir / "artifacts/apollo_hdmap_projection.jsonl",
        [{"source": "apollo_hdmap_api", "nearest_lane_id": "15_1_1"}],
    )

    report = analyze_apollo_route_contract_run_dir(run_dir, frame_transform=FRAME_TRANSFORM)

    assert report["status"] == "pass"
    assert report["scenario_route_length_m"] == 230.0
    assert report["scenario_route_length_source"] == "route_trace_s_span"
    assert report["scenario_route_length_consistency_status"] == "trace_only"
    assert report["scenario_route_length_consistency_reason"] == "declared_route_length_missing_trace_used"
    assert "scenario_route_length_m" not in report["missing_fields"]


def test_route_contract_passes_matching_lane_keep_route(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "artifacts/planning_topic_debug_summary.json",
        {
            "last_routing_total_length": 232.0,
            "last_routing_lane_window_count": 1,
            "last_routing_lane_window_signature": "15_1_1@66.9->298.9",
            "last_routing_unique_lane_signature": "15_1_1",
        },
    )
    _write_jsonl(
        run_dir / "artifacts/routing_event_debug.jsonl",
        [{"start_raw_x": 2.0, "start_raw_y": -249.0, "goal_raw_x": 2.0, "goal_raw_y": -19.0}],
    )

    report = analyze_apollo_route_contract_run_dir(run_dir, frame_transform=FRAME_TRANSFORM)

    assert report["status"] == "warn"
    assert report["blocking_reasons"] == []
    assert report["comparison_frame"] == "apollo_map"
    assert report["scenario_start_xy_carla"] == {"x": 2.0, "y": 249.0}
    assert report["scenario_start_xy_apollo"] == {"x": 2.0, "y": -249.0}


def test_route_contract_uses_decoded_routing_response_as_route_identity_source(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "artifacts/routing_response_decoded.json",
        {
            "source": "/apollo/routing_response",
            "lane_segments": [{"lane_id": "15_1_1", "start_s": 66.9, "end_s": 296.9}],
            "total_length_m": 230.0,
        },
    )
    _write_json(
        run_dir / "artifacts/planning_topic_debug_summary.json",
        {
            "last_routing_total_length": 648.0,
            "last_routing_lane_window_count": 17,
            "last_routing_lane_window_signature": "99_1_1@0->648",
        },
    )
    _write_jsonl(
        run_dir / "artifacts/routing_event_debug.jsonl",
        [
            {
                "routing_phase": "long",
                "routing_request_kind": "long_phase_route",
                "start_raw_x": 2.0,
                "start_raw_y": -249.0,
                "goal_raw_x": 2.0,
                "goal_raw_y": -19.0,
            }
        ],
    )
    _write_jsonl(
        run_dir / "artifacts/planning_route_segment_debug.jsonl",
        [
            {
                "route_segment_total_length": 230.0,
                "routing_lane_window_count": 1,
                "routing_lane_window_signature": "15_1_1@66.9->296.9",
                "routing_unique_lane_signature": "15_1_1",
            }
        ],
    )
    _write_jsonl(
        run_dir / "artifacts/apollo_hdmap_projection.jsonl",
        [{"source": "apollo_hdmap_api", "nearest_lane_id": "15_1_1"}],
    )

    report = analyze_apollo_route_contract_run_dir(run_dir, frame_transform=FRAME_TRANSFORM)

    assert report["status"] == "pass"
    assert report["apollo_routing_total_length_m"] == 230.0
    assert report["last_routing_response"]["source"] == "routing_response_decoded"
    assert report["routing_response_decoded"]["available"] is True
    assert report["route_identity_status"] == "consistent"


def test_route_contract_writes_route_definition_claim_artifact(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "artifacts/routing_response_decoded.json",
        {
            "source": "/apollo/routing_response",
            "lane_segments": [{"lane_id": "15_1_1", "start_s": 66.9, "end_s": 296.9}],
            "total_length_m": 230.0,
        },
    )
    _write_jsonl(
        run_dir / "artifacts/routing_event_debug.jsonl",
        [{"start_raw_x": 2.0, "start_raw_y": -249.0, "goal_raw_x": 2.0, "goal_raw_y": -19.0}],
    )
    _write_jsonl(
        run_dir / "artifacts/planning_route_segment_debug.jsonl",
        [
            {
                "route_segment_total_length": 230.0,
                "routing_lane_window_count": 1,
                "routing_lane_window_signature": "15_1_1@66.9->296.9",
                "routing_unique_lane_signature": "15_1_1",
            }
        ],
    )
    _write_jsonl(
        run_dir / "artifacts/apollo_hdmap_projection.jsonl",
        [{"source": "apollo_hdmap_api", "nearest_lane_id": "15_1_1"}],
    )

    report = analyze_apollo_route_contract_run_dir(run_dir, frame_transform=FRAME_TRANSFORM)
    outputs = write_apollo_route_contract_report(report, run_dir / "analysis/apollo_route_contract")
    claim = build_route_definition_claim(report)

    artifact_path = run_dir / "artifacts/route_definition_claim.json"
    written = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert Path(outputs["route_definition_claim"]).is_file()
    assert outputs["route_definition_claim_artifact"] == str(artifact_path)
    assert written["schema_version"] == ROUTE_DEFINITION_CLAIM_SCHEMA_VERSION
    assert written["scenario_lane_sequence"] == ["15:1"]
    assert written["apollo_lane_sequence"] == ["15:1"]
    assert written["scenario_route_samples"]
    assert written["lane_equivalence"]["status"] == "direct_match"
    assert written["namespace"] == {"scenario": "carla_waypoint", "apollo": "apollo_hdmap"}
    assert written["lane_window_signature"]["scenario"] == "15:1"
    assert written["lane_window_signature"]["apollo"] == "15_1_1"
    assert written["projection_source"] == "apollo_hdmap_api"
    assert written["equivalence_table"][0]["scenario_lane_key"] == "15:1"
    assert written["equivalence_table"][0]["apollo_lane_key"] == "15:1"
    assert claim["scenario_lane_window_signature"] == "15:1"


def test_route_contract_missing_apollo_route_is_insufficient(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)

    report = analyze_apollo_route_contract_run_dir(run_dir)

    assert report["status"] == "insufficient_data"
    assert "apollo_routing_total_length_m" in report["missing_fields"]


def test_route_contract_missing_frame_transform_does_not_hard_fail_xy(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "artifacts/planning_topic_debug_summary.json",
        {
            "last_routing_total_length": 232.0,
            "last_routing_lane_window_count": 1,
            "last_routing_lane_window_signature": "15_1_1@66.9->298.9",
            "last_routing_unique_lane_signature": "15_1_1",
        },
    )
    _write_jsonl(
        run_dir / "artifacts/routing_event_debug.jsonl",
        [{"start_raw_x": 2.0, "start_raw_y": -249.0, "goal_raw_x": 2.0, "goal_raw_y": -19.0}],
    )

    report = analyze_apollo_route_contract_run_dir(run_dir)

    assert report["status"] == "warn"
    assert "apollo_routing_goal_mismatch" not in report["blocking_reasons"]
    assert "apollo_route_xy_comparison_frame_transform_missing" in report["warnings"]
    assert report["start_xy_error_m"] is None


def test_startup_route_cannot_materialize_claim_route(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(run_dir / "artifacts/cyber_bridge_stats.json", {
        "last_routing_reason": "startup_initial_route",
        "health": {
            "routing_goal_mode": "ego_seed_ahead",
            "routing_goal_dist_m": 30.0,
            "routing_startup_phase_used": True,
        },
    })
    _write_json(
        run_dir / "artifacts/planning_topic_debug_summary.json",
        {
            "last_routing_total_length": 30.0,
            "last_routing_lane_window_count": 1,
            "last_routing_lane_window_signature": "15_1_1@66.9->96.9",
            "last_routing_unique_lane_signature": "15_1_1",
        },
    )
    _write_jsonl(
        run_dir / "artifacts/routing_event_debug.jsonl",
        [{"start_raw_x": 2.0, "start_raw_y": -249.0, "goal_raw_x": 2.0, "goal_raw_y": -219.0}],
    )

    report = analyze_apollo_route_contract_run_dir(run_dir, frame_transform=FRAME_TRANSFORM)

    assert report["routing_phase"] == "startup"
    assert report["startup_route_contract"]["status"] == "diagnostic_only"
    assert report["claim_route_contract"]["materialized"] is False
    assert "claim_route_not_materialized" in report["blocking_reasons"]


def test_long_goal_route_mismatch_is_not_clean_claim_route(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "artifacts/planning_topic_debug_summary.json",
        {
            "last_routing_total_length": 648.0,
            "last_routing_lane_window_count": 17,
            "last_routing_lane_window_signature": "15_1_1@66.9->307.6 | 13_1_-1@0->14.0 | 3_1_1@0->68.3",
            "last_routing_unique_lane_signature": "15_1_1 | 13_1_-1 | 3_1_1",
        },
    )
    _write_jsonl(
        run_dir / "artifacts/routing_event_debug.jsonl",
        [
            {
                "routing_phase": "long",
                "routing_request_index": 2,
                "routing_request_kind": "long_phase_route",
                "reroute_reason": "long_phase_transition",
                "goal_mode": "scenario_xy",
                "goal_distance_m": 251.0,
                "start_raw_x": 1.5,
                "start_raw_y": -249.0,
                "goal_raw_x": 3.3,
                "goal_raw_y": 0.5,
                "goal_projection": {
                    "available": True,
                    "distance_m": 2.9,
                    "source_trusted_lane_centerline": False,
                },
            }
        ],
    )
    _write_jsonl(
        run_dir / "artifacts/planning_route_segment_debug.jsonl",
        [
            {
                "planning_header_sequence_num": 854,
                "route_segment_total_length": 648.0,
                "routing_lane_window_count": 17,
                "routing_lane_window_signature": "15_1_1@66.9->307.6 | 13_1_-1@0->14.0 | 3_1_1@0->68.3",
                "routing_unique_lane_signature": "15_1_1 | 13_1_-1 | 3_1_1",
                "create_route_segments_status": "ready",
            }
        ],
    )

    report = analyze_apollo_route_contract_run_dir(run_dir, frame_transform=FRAME_TRANSFORM)

    assert report["status"] == "fail"
    assert report["routing_phase"] == "long_goal"
    assert report["raw_routing_phase"] == "long_goal"
    assert "long_goal_not_compatible_with_scenario_route" in report["blocking_reasons"]
    assert report["route_identity_status"] == "inconsistent"
    assert "planning_active_route_segment_length_not_scenario_route" in report["route_identity_issues"]
    assert report["last_routing_request"]["request_sequence"] == 2
    assert report["latest_planning_active_route_segment"]["route_segment_total_length_m"] == 648.0
    assert report["claim_route_contract"]["materialized"] is False


def test_unaccepted_untrusted_goal_projection_applied_blocks_claim_route(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "artifacts/planning_topic_debug_summary.json",
        {
            "last_routing_total_length": 232.0,
            "last_routing_lane_window_count": 1,
            "last_routing_lane_window_signature": "15_1_1@66.9->298.9",
            "last_routing_unique_lane_signature": "15_1_1",
        },
    )
    _write_jsonl(
        run_dir / "artifacts/routing_event_debug.jsonl",
        [
            {
                "routing_phase": "long",
                "routing_request_kind": "long_phase_route",
                "goal_mode": "scenario_xy",
                "start_raw_x": 2.0,
                "start_raw_y": -249.0,
                "goal_raw_x": 2.0,
                "goal_raw_y": -19.0,
                "goal_projection": {
                    "available": True,
                    "accepted": False,
                    "applied": True,
                    "source_trusted_lane_centerline": False,
                    "distance_m": 2.0,
                    "signed_e_y_m": 0.2,
                },
            }
        ],
    )

    report = analyze_apollo_route_contract_run_dir(run_dir, frame_transform=FRAME_TRANSFORM)

    assert report["status"] == "fail"
    assert "unaccepted_goal_projection_applied" in report["blocking_reasons"]
    assert "untrusted_goal_projection_applied" in report["blocking_reasons"]
    assert "apollo_routing_goal_projection_not_accepted" in report["blocking_reasons"]
    assert "apollo_routing_goal_projection_untrusted" in report["blocking_reasons"]


def test_goal_projection_lateral_error_blocks_claim_route(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "artifacts/planning_topic_debug_summary.json",
        {
            "last_routing_total_length": 232.0,
            "last_routing_lane_window_count": 1,
            "last_routing_lane_window_signature": "15_1_1@66.9->298.9",
            "last_routing_unique_lane_signature": "15_1_1",
        },
    )
    _write_jsonl(
        run_dir / "artifacts/routing_event_debug.jsonl",
        [
            {
                "routing_phase": "long",
                "routing_request_kind": "long_phase_route",
                "goal_mode": "scenario_xy",
                "start_raw_x": 2.0,
                "start_raw_y": -249.0,
                "goal_raw_x": 2.0,
                "goal_raw_y": -19.0,
                "goal_projection": {
                    "available": True,
                    "accepted": True,
                    "applied": True,
                    "source_trusted_lane_centerline": True,
                    "distance_m": 1.0,
                    "signed_e_y_m": 1.25,
                },
            }
        ],
    )

    report = analyze_apollo_route_contract_run_dir(run_dir, frame_transform=FRAME_TRANSFORM)

    assert report["status"] == "fail"
    assert "apollo_routing_goal_lateral_error_high" in report["blocking_reasons"]


def test_goal_near_threshold_warns_even_below_legacy_xy_threshold(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "artifacts/planning_topic_debug_summary.json",
        {
            "last_routing_total_length": 232.0,
            "last_routing_lane_window_count": 1,
            "last_routing_lane_window_signature": "15_1_1@66.9->298.9",
            "last_routing_unique_lane_signature": "15_1_1",
        },
    )
    _write_jsonl(
        run_dir / "artifacts/routing_event_debug.jsonl",
        [
            {
                "routing_phase": "long",
                "routing_request_index": 2,
                "routing_request_kind": "long_phase_route",
                "goal_raw_x": 2.0,
                "goal_raw_y": -0.5,
                "goal_projection": {
                    "available": True,
                    "distance_m": 2.9,
                    "source_trusted_lane_centerline": False,
                },
            }
        ],
    )

    report = analyze_apollo_route_contract_run_dir(run_dir, frame_transform=FRAME_TRANSFORM)

    assert report["status"] == "fail"
    assert "apollo_routing_goal_projection_untrusted" in report["blocking_reasons"]
    assert "apollo_routing_goal_near_threshold" in report["warnings"]
    assert "apollo_routing_goal_lane_compatibility_unverified" in report["warnings"]


def test_route_contract_cli_writes_report(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "artifacts/routing_response_decoded.json",
        {
            "source": "/apollo/routing_response",
            "lane_segments": [{"lane_id": "15_1_1", "start_s": 66.9, "end_s": 298.9}],
            "total_length_m": 232.0,
        },
    )
    _write_jsonl(
        run_dir / "artifacts/routing_event_debug.jsonl",
        [{"start_raw_x": 2.0, "start_raw_y": -249.0, "goal_raw_x": 2.0, "goal_raw_y": -19.0}],
    )
    _write_jsonl(
        run_dir / "artifacts/planning_route_segment_debug.jsonl",
        [
            {
                "route_segment_total_length": 232.0,
                "routing_lane_window_count": 1,
                "routing_lane_window_signature": "15_1_1@66.9->298.9",
            }
        ],
    )
    _write_jsonl(
        run_dir / "artifacts/apollo_hdmap_projection.jsonl",
        [{"source": "apollo_hdmap_api", "nearest_lane_id": "15_1_1"}],
    )
    out_dir = tmp_path / "out"
    transform_path = tmp_path / "frame_transform.yaml"
    transform_path.write_text(json.dumps(FRAME_TRANSFORM), encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_apollo_route_contract.py",
            "--run-dir",
            str(run_dir),
            "--frame-transform",
            str(transform_path),
            "--out",
            str(out_dir),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    report = json.loads((out_dir / "apollo_route_contract_report.json").read_text(encoding="utf-8"))
    assert report["status"] == "pass"
    assert report["last_routing_response"]["source"] == "routing_response_decoded"
    assert (out_dir / "apollo_route_contract_summary.md").is_file()


def test_route_contract_cli_defaults_town01_frame_transform(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "artifacts/routing_response_decoded.json",
        {
            "source": "/apollo/routing_response",
            "lane_segments": [{"lane_id": "15_1_1", "start_s": 66.9, "end_s": 298.9}],
            "total_length_m": 232.0,
        },
    )
    _write_jsonl(
        run_dir / "artifacts/routing_event_debug.jsonl",
        [{"start_raw_x": 2.0, "start_raw_y": -249.0, "goal_raw_x": 2.0, "goal_raw_y": -19.0}],
    )
    _write_jsonl(
        run_dir / "artifacts/apollo_hdmap_projection.jsonl",
        [{"source": "apollo_hdmap_api", "nearest_lane_id": "15_1_1"}],
    )
    _write_jsonl(
        run_dir / "artifacts/planning_route_segment_debug.jsonl",
        [
            {
                "route_segment_total_length": 232.0,
                "routing_lane_window_count": 1,
                "routing_lane_window_signature": "15_1_1@66.9->298.9",
            }
        ],
    )
    out_dir = tmp_path / "out_default_transform"

    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_apollo_route_contract.py",
            "--run-dir",
            str(run_dir),
            "--out",
            str(out_dir),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    report = json.loads((out_dir / "apollo_route_contract_report.json").read_text(encoding="utf-8"))
    assert report["status"] == "pass"
    assert report["source"]["frame_transform"].endswith(
        "configs/town01/apollo_frame_transform.example.yaml"
    )
    assert "apollo_route_xy_comparison_frame_transform_missing" not in report["warnings"]


def _lane_equivalence_projection_rows(pairs: list[tuple[str, str]]) -> list[dict]:
    rows: list[dict] = []
    for index, (carla_lane_key, apollo_lane_id) in enumerate(pairs):
        for sample_offset in range(3):
            rows.append(
                {
                    "source": "apollo_hdmap_api",
                    "status": "ok",
                    "projection_status": "ok",
                    "sample_type": "route",
                    "route_index": index * 10 + sample_offset,
                    "route_s": float(index * 50 + sample_offset * 5),
                    "carla_lane_key": carla_lane_key,
                    "carla_lane_id": f"{carla_lane_key.split(':', 1)[0]}:0:{carla_lane_key.split(':', 1)[1]}",
                    "nearest_lane_id": apollo_lane_id,
                    "projection_s": float(index * 10 + sample_offset),
                    "projection_l": 0.05,
                    "lateral_error_m": 0.05,
                    "heading_error_rad": 0.002,
                    "lane_heading_at_s": 0.0,
                }
            )
    return rows
