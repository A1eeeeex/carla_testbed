from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.apollo_module_consumption import (
    APOLLO_MODULE_CONSUMPTION_SCHEMA_VERSION,
    analyze_apollo_module_consumption_run_dir,
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
            "route_id": "097",
            "scenario_id": "lane_keep_097",
            "routing_success_count": 1,
        },
    )
    _write_json(
        run_dir / "analysis/planning_materialization/planning_materialization_report.json",
        {
            "schema_version": "planning_materialization.v1",
            "first_nonempty_after_routing_latency_s": 0.2,
            "route_establishment": {"route_established": True},
            "empty_reason_histogram": {},
        },
    )
    _write_json(
        run_dir / "artifacts/planning_topic_debug_summary.json",
        {
            "total_messages_received": 100,
            "messages_with_nonzero_trajectory_points": 90,
        },
    )
    _write_jsonl(
        run_dir / "artifacts/planning_topic_debug.jsonl",
        [
            {
                "timestamp": 1.0,
                "trajectory_point_count": 10,
                "routing_header_present": True,
                "localization_age_ms": 10.0,
                "chassis_age_ms": 12.0,
                "routing_response_age_ms": 20.0,
            }
        ],
    )
    _write_jsonl(
        run_dir / "artifacts/topic_publish_stats.jsonl",
        [
            {"topic": "/apollo/localization/pose"},
            {"topic": "/apollo/canbus/chassis"},
            {"topic": "/apollo/planning"},
            {"topic": "/apollo/control"},
        ],
    )
    _write_jsonl(
        run_dir / "artifacts/control_decode_debug.jsonl",
        [
            {
                "parsed_control": {
                    "control_timestamp": 1.0,
                    "control_header_sequence_num": 1,
                    "input_trajectory_header_sequence_num": 7,
                }
            }
        ],
    )
    _write_json(
        run_dir / "analysis/prediction_evidence/prediction_evidence_report.json",
        {
            "schema_version": "prediction_evidence.v1",
            "prediction_mode": "native_observed",
            "planning_requires_prediction": True,
        },
    )
    _write_json(
        run_dir / "analysis/apollo_route_contract/apollo_route_contract_report.json",
        {
            "schema_version": "apollo_route_contract.v1",
            "status": "pass",
            "routing_phase": "claim",
            "claim_route_contract": {
                "status": "pass",
                "materialized": True,
                "blocking_reasons": [],
            },
            "blocking_reasons": [],
        },
    )
    return run_dir


def test_module_consumption_passes_when_routing_and_inputs_are_consumed(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)

    report = analyze_apollo_module_consumption_run_dir(run_dir)

    assert report["schema_version"] == APOLLO_MODULE_CONSUMPTION_SCHEMA_VERSION
    assert report["status"] == "pass"
    assert report["routing_response_consumed_by_planning"] is True
    assert report["planning_input_age"]["localization_age_ms_p95"] == 10.0
    assert report["input_topic_publish_coverage"]["has_localization"] is True
    assert report["output_topic_observation_coverage"]["has_control"] is True
    assert report["control_consumption_evidence"]["control_decode_debug_available"] is True


def test_module_consumption_fails_on_input_timeout_logs(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    (run_dir / "artifacts/apollo.log").write_text(
        "planning localization timeout\nreference line provider failed\n",
        encoding="utf-8",
    )

    report = analyze_apollo_module_consumption_run_dir(run_dir)

    assert report["status"] == "fail"
    assert report["pattern_counts"]["localization_timeout"] == 1
    assert "planning_input_timeout_logs_present" in report["blocking_reasons"]
    assert "reference_line_provider_failure_logs_present" in report["blocking_reasons"]


def test_module_consumption_flags_apollo_route_reference_line_error_patterns(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/planning_materialization/planning_materialization_report.json",
        {
            "schema_version": "planning_materialization.v1",
            "first_nonempty_after_routing_latency_s": 0.2,
            "route_establishment": {"route_established": True},
            "empty_reason_histogram": {},
            "apollo_log_error_topk": [
                {"message": "Planning failed:PLANNING_ERROR lane_follow_map.cc adc_route_index error"}
            ],
        },
    )

    report = analyze_apollo_module_consumption_run_dir(run_dir)

    assert report["status"] == "fail"
    assert report["pattern_counts"]["route_or_reference_line_failure"] == 1
    assert "route_or_reference_line_failure_logs_present" in report["blocking_reasons"]


def test_module_consumption_warns_on_transient_route_logs_after_materialization(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/planning_materialization/planning_materialization_report.json",
        {
            "schema_version": "planning_materialization.v1",
            "verdict": "pass",
            "first_nonempty_after_routing_latency_s": 0.15,
            "route_establishment": {
                "route_established": True,
                "routing_success_count": 1,
                "planning_nonempty_after_routing_success_ratio": 0.99,
            },
            "empty_reason_histogram": {},
            "apollo_log_error_topk": [
                {"message": "lane_follow_map.cc adc_route_index error, can not get distance to destination"},
                {"message": "on_lane_planning.cc Planning failed:PLANNING_ERROR planner failed"},
            ],
        },
    )

    report = analyze_apollo_module_consumption_run_dir(run_dir)

    assert report["status"] == "warn"
    assert "route_or_reference_line_failure_logs_present" not in report["blocking_reasons"]
    assert "route_or_reference_line_failure_logs_transient" in report["warnings"]


def test_module_consumption_warns_when_input_freshness_unverified(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/planning_materialization/planning_materialization_report.json",
        {
            "schema_version": "planning_materialization.v1",
            "first_nonempty_after_routing_latency_s": 0.2,
            "route_establishment": {"route_established": True},
            "empty_reason_histogram": {},
            "input_freshness_attribution": {
                "status": "insufficient_data",
                "input_freshness_unverified_empty_count": 12,
            },
        },
    )

    report = analyze_apollo_module_consumption_run_dir(run_dir)

    assert report["status"] == "warn"
    assert "planning_input_freshness_unverified" in report["warnings"]


def test_module_consumption_missing_planning_materialization_is_insufficient(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    (run_dir / "analysis/planning_materialization/planning_materialization_report.json").unlink()

    report = analyze_apollo_module_consumption_run_dir(run_dir)

    assert report["status"] == "insufficient_data"
    assert "planning_materialization_missing" in report["blocking_reasons"]


def test_module_consumption_blocks_failed_route_contract(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/apollo_route_contract/apollo_route_contract_report.json",
        {
            "schema_version": "apollo_route_contract.v1",
            "status": "fail",
            "routing_phase": "startup",
            "claim_route_contract": {
                "status": "fail",
                "materialized": False,
                "blocking_reasons": ["claim_route_not_materialized"],
            },
            "blocking_reasons": ["claim_route_not_materialized"],
        },
    )

    report = analyze_apollo_module_consumption_run_dir(run_dir)

    assert report["status"] == "fail"
    assert "route_contract_failed_before_module_consumption_claim" in report["blocking_reasons"]
    assert "claim_route_consumption_unverified" in report["blocking_reasons"]


def test_module_consumption_reports_consumed_route_identity_from_route_contract(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/apollo_route_contract/apollo_route_contract_report.json",
        {
            "schema_version": "apollo_route_contract.v1",
            "status": "fail",
            "routing_phase": "long_goal",
            "apollo_routing_total_length_m": 648.136,
            "apollo_routing_lane_signature": "15_1_1@66.8->307.6 | 13_1_-1@0.0->14.0",
            "last_routing_response": {
                "response_total_length_m": 648.136,
                "lane_window_signature": "15_1_1@66.8->307.6 | 13_1_-1@0.0->14.0",
            },
            "latest_planning_active_route_segment": {
                "route_segment_total_length_m": 648.136,
                "routing_lane_window_signature": "15_1_1@66.8->307.6 | 13_1_-1@0.0->14.0",
            },
            "claim_route_contract": {
                "status": "fail",
                "materialized": False,
                "blocking_reasons": ["route_identity_inconsistent"],
            },
            "route_identity_status": "inconsistent",
            "route_identity_issues": ["planning_active_route_segment_length_not_scenario_route"],
            "blocking_reasons": ["route_identity_inconsistent"],
        },
    )

    report = analyze_apollo_module_consumption_run_dir(run_dir)

    assert report["status"] == "fail"
    assert report["routing_response_consumed_by_planning"] is True
    assert report["consumed_route_phase"] == "long_goal"
    assert report["consumed_route_contract_status"] == "fail"
    assert report["consumed_route_total_length_m"] == 648.136
    assert report["planning_routing_header_matches_response_id"] is True
    assert report["consumed_route_identity"]["route_identity_status"] == "inconsistent"
    assert "claim_route_consumption_unverified" in report["blocking_reasons"]


def test_module_consumption_fails_when_route_not_established_and_reference_line_missing(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "summary.json",
        {
            "run_id": "run",
            "route_id": "097",
            "scenario_id": "lane_keep_097",
            "routing_success_count": 0,
        },
    )
    _write_json(
        run_dir / "artifacts/planning_topic_debug_summary.json",
        {
            "total_messages_received": 12,
            "messages_with_nonzero_trajectory_points": 0,
        },
    )
    _write_json(
        run_dir / "analysis/planning_materialization/planning_materialization_report.json",
        {
            "schema_version": "planning_materialization.v1",
            "verdict": "fail",
            "blocking_reasons": [
                "planning_trajectory_materialization_low",
                "route_establishment_not_confirmed",
            ],
            "route_establishment": {
                "route_established": False,
                "routing_success_count": 0,
                "blocking_reasons": ["routing_success_missing"],
            },
            "empty_reason_histogram": {"reference_line_provider_not_ready": 12},
        },
    )
    _write_jsonl(
        run_dir / "artifacts/planning_topic_debug.jsonl",
        [
            {
                "timestamp": 1.0,
                "trajectory_point_count": 0,
                "empty_reason": "reference_line_provider_not_ready",
            }
        ],
    )

    report = analyze_apollo_module_consumption_run_dir(run_dir)

    assert report["status"] == "fail"
    assert "routing_response_not_consumed_by_planning" in report["blocking_reasons"]
    assert "reference_line_provider_not_ready_empty_planning" in report["blocking_reasons"]


def test_module_consumption_warns_on_transient_reference_line_empty_after_route_established(
    tmp_path: Path,
) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/planning_materialization/planning_materialization_report.json",
        {
            "schema_version": "planning_materialization.v1",
            "verdict": "pass",
            "first_nonempty_after_routing_latency_s": 0.15,
            "route_establishment": {
                "route_established": True,
                "routing_success_count": 1,
                "planning_nonempty_after_routing_success_ratio": 0.99,
            },
            "empty_reason_histogram": {
                "reference_line_provider_not_ready": 3,
                "routing_not_ready": 94,
            },
            "input_freshness_attribution": {"status": "pass"},
        },
    )

    report = analyze_apollo_module_consumption_run_dir(run_dir)

    assert report["status"] == "warn"
    assert "reference_line_provider_not_ready_empty_planning" not in report["blocking_reasons"]
    assert "reference_line_provider_not_ready_empty_planning_transient" in report["warnings"]


def test_module_consumption_distinguishes_partial_routing_consumption_from_route_establishment_failure(
    tmp_path: Path,
) -> None:
    run_dir = _base_run(tmp_path)
    _write_json(
        run_dir / "analysis/planning_materialization/planning_materialization_report.json",
        {
            "schema_version": "planning_materialization.v1",
            "verdict": "fail",
            "first_nonempty_after_routing_latency_s": 2.2,
            "blocking_reasons": [
                "planning_trajectory_materialization_low",
                "route_establishment_not_confirmed",
            ],
            "route_establishment": {
                "route_established": False,
                "routing_success_count": 2,
                "planning_nonempty_after_routing_success_ratio": 0.69,
                "blocking_reasons": [
                    "planning_nonempty_after_routing_below_threshold",
                    "route_establishment_latency",
                ],
            },
            "empty_reason_histogram": {"reference_line_provider_not_ready": 528},
        },
    )

    report = analyze_apollo_module_consumption_run_dir(run_dir)

    assert report["status"] == "fail"
    assert report["routing_response_consumed_by_planning"] is True
    assert "routing_response_not_consumed_by_planning" not in report["blocking_reasons"]
    assert "reference_line_provider_not_ready_empty_planning" in report["blocking_reasons"]


def test_module_consumption_cli_writes_report(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    out_dir = tmp_path / "out"

    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_apollo_module_consumption.py",
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
    assert (out_dir / "apollo_module_consumption_report.json").is_file()
    assert (out_dir / "apollo_module_consumption_summary.md").is_file()
