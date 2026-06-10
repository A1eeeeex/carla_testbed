from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.apollo_route_contract import (
    APOLLO_ROUTE_CONTRACT_SCHEMA_VERSION,
    analyze_apollo_route_contract_run_dir,
)


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

    report = analyze_apollo_route_contract_run_dir(run_dir)

    assert report["schema_version"] == APOLLO_ROUTE_CONTRACT_SCHEMA_VERSION
    assert report["status"] == "fail"
    assert report["routing_length_ratio"] > 2.0
    assert "apollo_routing_length_mismatch" in report["blocking_reasons"]
    assert "apollo_routing_lane_sequence_mismatch" in report["blocking_reasons"]


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
        [{"start_raw_x": 2.0, "start_raw_y": 249.0, "goal_raw_x": 2.0, "goal_raw_y": 19.0}],
    )

    report = analyze_apollo_route_contract_run_dir(run_dir)

    assert report["status"] == "pass"
    assert report["blocking_reasons"] == []


def test_route_contract_missing_apollo_route_is_insufficient(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)

    report = analyze_apollo_route_contract_run_dir(run_dir)

    assert report["status"] == "insufficient_data"
    assert "apollo_routing_total_length_m" in report["missing_fields"]


def test_route_contract_cli_writes_report(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "artifacts/planning_topic_debug_summary.json",
        {
            "last_routing_total_length": 232.0,
            "last_routing_lane_window_count": 1,
            "last_routing_lane_window_signature": "15_1_1@66.9->298.9",
        },
    )
    out_dir = tmp_path / "out"

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
    assert (out_dir / "apollo_route_contract_report.json").is_file()
    assert (out_dir / "apollo_route_contract_summary.md").is_file()
