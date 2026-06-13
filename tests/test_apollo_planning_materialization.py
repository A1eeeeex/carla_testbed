from __future__ import annotations

import json
from pathlib import Path

import pytest

from carla_testbed.analysis.planning_materialization import (
    analyze_planning_materialization_run_dir,
    write_planning_materialization_report,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")


def _run_dir(tmp_path: Path) -> Path:
    run_dir = tmp_path / "run"
    _write_json(
        run_dir / "summary.json",
        {
            "run_id": "run",
            "route_id": "097",
            "scenario_class": "lane_keep",
            "fail_reason": "ROUTE_ESTABLISHMENT_LATENCY_SEC",
        },
    )
    _write_json(
        run_dir / "artifacts/cyber_bridge_stats.json",
        {
            "routing_success_count": 2,
            "routing_first_success_response_ts_sec": 12.0,
        },
    )
    return run_dir


def test_low_nonempty_ratio_hard_fails_with_route_establishment_blocker(tmp_path: Path) -> None:
    run_dir = _run_dir(tmp_path)
    rows = []
    route_rows = []
    for index in range(10):
        nonempty = index == 8
        rows.append(
            {
                "timestamp": 10.0 + index,
                "planning_header_sequence_num": index,
                "trajectory_point_count": 12 if nonempty else 0,
            }
        )
        route_rows.append(
            {
                "timestamp": 10.0 + index,
                "planning_header_sequence_num": index,
                "planning_empty_reason_guess": "ok" if nonempty else "reference_line_missing",
                "reference_line_provider_status": "ok" if nonempty else "failed",
                "reference_line_count": 1 if nonempty else 0,
                "route_segment_count": 1 if nonempty else 0,
            }
        )
    _write_jsonl(run_dir / "artifacts/planning_topic_debug.jsonl", rows)
    _write_jsonl(run_dir / "artifacts/planning_route_segment_debug.jsonl", route_rows)

    report = analyze_planning_materialization_run_dir(run_dir)

    assert report["verdict"] == "fail"
    assert report["materialization_status"] == "observed_nonempty"
    assert report["nonempty_trajectory_ratio"] == 0.1
    assert report["trajectory_point_count_max"] == 12
    assert report["reference_line_count_max"] == 1
    assert report["route_segment_count_max"] == 1
    assert "planning_reference_line" in report["suspected_layers"]
    assert "routing_materialization" in report["suspected_layers"]
    assert "planning_trajectory_materialization_low" in report["blocking_reasons"]
    assert "route_establishment_not_confirmed" in report["blocking_reasons"]
    assert report["empty_reason_histogram"]["reference_line_provider_not_ready"] == 7
    assert report["empty_reason_histogram"]["routing_not_ready"] == 2
    assert report["route_establishment"]["route_established"] is False


def test_high_nonempty_ratio_passes_materialization(tmp_path: Path) -> None:
    run_dir = _run_dir(tmp_path)
    _write_json(
        run_dir / "summary.json",
        {"run_id": "run", "route_id": "097", "scenario_class": "lane_keep"},
    )
    rows = [
        {
            "timestamp": 20.0 + index,
            "planning_header_sequence_num": index,
            "trajectory_point_count": 20 if index != 0 else 0,
        }
        for index in range(10)
    ]
    _write_jsonl(run_dir / "artifacts/planning_topic_debug.jsonl", rows)

    report = analyze_planning_materialization_run_dir(run_dir)

    assert report["verdict"] == "pass"
    assert report["materialization_status"] == "observed_nonempty"
    assert report["nonempty_trajectory_ratio"] == 0.9
    assert report["metrics"]["nonempty_trajectory_ratio"] == 0.9


def test_empty_trajectory_rows_include_asof_input_evidence(tmp_path: Path) -> None:
    run_dir = _run_dir(tmp_path)
    _write_jsonl(
        run_dir / "artifacts/planning_topic_debug.jsonl",
        [
            {"timestamp": 1.0, "planning_header_sequence_num": 1, "trajectory_point_count": 0},
            {"timestamp": 2.0, "planning_header_sequence_num": 2, "trajectory_point_count": 0},
            {"timestamp": 3.0, "planning_header_sequence_num": 3, "trajectory_point_count": 12},
        ],
    )
    _write_jsonl(
        run_dir / "artifacts/topic_publish_stats.jsonl",
        [
            {"timestamp": 1.02, "channel": "/apollo/localization/pose", "header_timestamp_sec": 1.02},
            {"timestamp": 1.03, "channel": "/apollo/canbus/chassis", "header_timestamp_sec": 1.03},
        ],
    )
    _write_jsonl(
        run_dir / "artifacts/apollo_reference_line_contract.jsonl",
        [
            {"timestamp": 1.01, "status": "ok", "reference_line_count": 1},
            {"timestamp": 2.01, "status": "failed", "reference_line_count": 0},
        ],
    )
    _write_jsonl(
        run_dir / "artifacts/apollo_hdmap_projection.jsonl",
        [
            {"timestamp": 1.01, "status": "ok"},
            {"timestamp": 2.01, "status": "no_lane"},
        ],
    )

    report = analyze_planning_materialization_run_dir(run_dir)
    asof = report["empty_asof_join"]

    assert asof["empty_row_count"] == 2
    assert asof["localization_join_coverage_ratio"] == 0.5
    assert asof["chassis_join_coverage_ratio"] == 0.5
    assert asof["reference_line_join_coverage_ratio"] == 1.0
    assert asof["hdmap_projection_join_coverage_ratio"] == 1.0
    assert asof["stale_localization_empty_count"] == 1
    assert asof["reference_line_not_ok_empty_count"] == 1
    assert asof["hdmap_non_ok_empty_count"] == 1


def test_planning_materialization_prefers_sim_time_over_wall_timestamp(tmp_path: Path) -> None:
    run_dir = _run_dir(tmp_path)
    _write_json(
        run_dir / "artifacts/cyber_bridge_stats.json",
        {
            "routing_success_count": 1,
            "routing_first_success_response_ts_sec": 12.0,
        },
    )
    _write_jsonl(
        run_dir / "artifacts/planning_topic_debug.jsonl",
        [
            {
                "timestamp": 1_780_000_000.0,
                "sim_time_sec": 11.0,
                "planning_header_sequence_num": 1,
                "trajectory_point_count": 0,
            },
            {
                "timestamp": 1_780_000_001.0,
                "sim_time_sec": 12.2,
                "planning_header_sequence_num": 2,
                "trajectory_point_count": 8,
            },
        ],
    )

    report = analyze_planning_materialization_run_dir(run_dir)

    assert report["first_planning_message_time_sec"] == 11.0
    assert report["first_nonempty_after_routing_latency_s"] == pytest.approx(0.2)
    assert report["time_domain"]["planning_time_domain"]["domain"] == "sim_time"
    assert "derived_latency_outside_reasonable_range" not in report["time_domain"]["warnings"]


def test_planning_materialization_prefers_routing_boundary_sim_time_over_wall_time(
    tmp_path: Path,
) -> None:
    run_dir = _run_dir(tmp_path)
    _write_json(
        run_dir / "summary.json",
        {"run_id": "run", "route_id": "097", "scenario_class": "lane_keep"},
    )
    _write_json(
        run_dir / "artifacts/planning_topic_debug_summary.json",
        {
            "total_messages_received": 3,
            "messages_with_nonzero_trajectory_points": 2,
            "routing_first_success_response_ts_sec": 1_781_257_358.0,
            "routing_first_success_response_after_last_routing_send_boundary_ts_sec": 60.0,
        },
    )
    _write_json(
        run_dir / "artifacts/cyber_bridge_stats.json",
        {
            "routing_success_count": 1,
            "routing_first_success_response_ts_sec": 1_781_257_358.0,
            "routing_first_success_response_after_last_routing_send_boundary_ts_sec": 60.0,
        },
    )
    _write_jsonl(
        run_dir / "artifacts/planning_topic_debug.jsonl",
        [
            {"sim_time_sec": 59.9, "planning_header_sequence_num": 1, "trajectory_point_count": 0},
            {"sim_time_sec": 60.1, "planning_header_sequence_num": 2, "trajectory_point_count": 8},
            {"sim_time_sec": 60.2, "planning_header_sequence_num": 3, "trajectory_point_count": 8},
        ],
    )
    _write_jsonl(
        run_dir / "artifacts/topic_publish_stats.jsonl",
        [
            {"sim_time_sec": 60.0, "channel": "/apollo/localization/pose"},
            {"sim_time_sec": 60.0, "channel": "/apollo/canbus/chassis"},
            {"sim_time_sec": 60.1, "channel": "/apollo/localization/pose"},
            {"sim_time_sec": 60.1, "channel": "/apollo/canbus/chassis"},
        ],
    )

    report = analyze_planning_materialization_run_dir(run_dir)

    assert report["after_routing_success_nonempty_ratio"] == 1.0
    assert report["first_nonempty_after_routing_latency_s"] == pytest.approx(0.1)
    assert report["time_domain"]["status"] == "pass"
    assert report["route_establishment"]["route_established"] is True
    assert "route_establishment_not_confirmed" not in report["blocking_reasons"]


def test_planning_materialization_nulls_invalid_mixed_time_latency(tmp_path: Path) -> None:
    run_dir = _run_dir(tmp_path)
    _write_json(
        run_dir / "artifacts/cyber_bridge_stats.json",
        {
            "routing_success_count": 1,
            "routing_first_success_response_ts_sec": 1_780_000_000.0,
        },
    )
    _write_jsonl(
        run_dir / "artifacts/planning_topic_debug.jsonl",
        [
            {
                "sim_time_sec": 12.0,
                "planning_header_sequence_num": 1,
                "trajectory_point_count": 8,
            }
        ],
    )

    report = analyze_planning_materialization_run_dir(run_dir)

    assert report["first_nonempty_after_routing_latency_s"] is None
    assert report["time_domain"]["first_nonempty_after_routing_latency_s"] is None
    assert "first_nonempty_after_routing_latency_s" in report["time_domain"]["invalid_latency_fields"]
    assert "first_nonempty_after_routing_latency_unusable" in report["warnings"]


def test_zero_freshness_join_coverage_is_unverified_not_zero_stale(tmp_path: Path) -> None:
    run_dir = _run_dir(tmp_path)
    _write_jsonl(
        run_dir / "artifacts/planning_topic_debug.jsonl",
        [
            {
                "sim_time_sec": 1.0,
                "planning_header_sequence_num": 1,
                "trajectory_point_count": 0,
            },
            {
                "sim_time_sec": 2.0,
                "planning_header_sequence_num": 2,
                "trajectory_point_count": 0,
            },
        ],
    )
    _write_jsonl(
        run_dir / "artifacts/topic_publish_stats.jsonl",
        [
            {
                "wall_time_sec": 1_780_000_000.0,
                "channel": "/apollo/localization/pose",
                "header_timestamp_sec": 1_780_000_000.0,
            },
            {
                "wall_time_sec": 1_780_000_000.1,
                "channel": "/apollo/canbus/chassis",
                "header_timestamp_sec": 1_780_000_000.1,
            },
        ],
    )

    report = analyze_planning_materialization_run_dir(run_dir)
    freshness = report["input_freshness_attribution"]

    assert report["materialization_status"] == "observed_empty"
    assert report["empty_asof_join"]["localization_join_coverage_ratio"] == 0.0
    assert freshness["status"] == "insufficient_data"
    assert freshness["localization_stale_or_gap_empty_count"] is None
    assert freshness["chassis_stale_or_gap_empty_count"] is None
    assert freshness["input_freshness_unverified_empty_count"] == 2
    assert "planning_input_freshness_unverified" in report["warnings"]


def test_missing_planning_artifacts_is_insufficient_data(tmp_path: Path) -> None:
    run_dir = _run_dir(tmp_path)

    report = analyze_planning_materialization_run_dir(run_dir)

    assert report["verdict"] == "insufficient_data"
    assert report["materialization_status"] == "missing"
    assert "planning_topic_debug_or_summary" in report["missing_fields"]
    assert "planning_materialization_insufficient_data" in report["blocking_reasons"]


def test_empty_planning_with_reference_line_missing_gets_distinct_status(tmp_path: Path) -> None:
    run_dir = _run_dir(tmp_path)
    _write_jsonl(
        run_dir / "artifacts/planning_topic_debug.jsonl",
        [
            {"timestamp": 13.0, "planning_header_sequence_num": 1, "trajectory_point_count": 0},
            {"timestamp": 14.0, "planning_header_sequence_num": 2, "trajectory_point_count": 0},
        ],
    )
    _write_jsonl(
        run_dir / "artifacts/planning_route_segment_debug.jsonl",
        [
            {
                "timestamp": 13.0,
                "planning_header_sequence_num": 1,
                "reference_line_provider_status": "failed",
                "reference_line_count": 0,
                "route_segment_count": 0,
            },
            {
                "timestamp": 14.0,
                "planning_header_sequence_num": 2,
                "lane_follow_map_status": "reference_line_missing",
                "reference_line_count": 0,
                "route_segment_count": 0,
            },
        ],
    )

    report = analyze_planning_materialization_run_dir(run_dir)

    assert report["verdict"] == "fail"
    assert report["materialization_status"] == "observed_reference_line_missing"
    assert report["empty_reason_histogram"]["reference_line_provider_not_ready"] == 2


def test_write_planning_materialization_report(tmp_path: Path) -> None:
    run_dir = _run_dir(tmp_path)
    _write_jsonl(
        run_dir / "artifacts/planning_topic_debug.jsonl",
        [
            {"timestamp": 1.0, "planning_header_sequence_num": 1, "trajectory_point_count": 0},
            {"timestamp": 2.0, "planning_header_sequence_num": 2, "trajectory_point_count": 5},
        ],
    )
    report = analyze_planning_materialization_run_dir(run_dir)

    outputs = write_planning_materialization_report(report, tmp_path / "out")

    assert Path(outputs["planning_materialization_report"]).is_file()
    assert Path(outputs["planning_materialization_summary"]).is_file()
