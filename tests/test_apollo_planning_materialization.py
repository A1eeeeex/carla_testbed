from __future__ import annotations

import json
from pathlib import Path

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
    assert report["nonempty_trajectory_ratio"] == 0.1
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


def test_missing_planning_artifacts_is_insufficient_data(tmp_path: Path) -> None:
    run_dir = _run_dir(tmp_path)

    report = analyze_planning_materialization_run_dir(run_dir)

    assert report["verdict"] == "insufficient_data"
    assert "planning_topic_debug_or_summary" in report["missing_fields"]
    assert "planning_materialization_insufficient_data" in report["blocking_reasons"]


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
