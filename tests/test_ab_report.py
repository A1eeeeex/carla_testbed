from __future__ import annotations

import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.transport_ab import AB_REPORT_CSV_FIELDS, analyze_ab_manifest, write_ab_report
from carla_testbed.analysis.transport_ab import _compare_pair
from carla_testbed.analysis.transport_ab import check_ab_report_requirements


FIXTURE_BATCH = Path(__file__).resolve().parent / "fixtures" / "ab" / "simple_batch"
SCRIPT = Path("tools/analyze_ab_report.py")


def _comparison(report: dict, route_id: str) -> dict:
    for item in report["comparisons"]:
        if item["route_id"] == route_id:
            return item
    raise AssertionError(f"missing comparison for {route_id}")


def _run_result(report: dict, run_id: str) -> dict:
    for item in report["run_results"]:
        if item["run_id"] == run_id:
            return item
    raise AssertionError(f"missing run result for {run_id}")


def test_synthetic_success_pair_can_be_candidate_positive() -> None:
    report = analyze_ab_manifest(FIXTURE_BATCH / "ab_manifest.json")

    assert report["schema_version"] == "ab_report.v1"
    assert report["batch_id"] == "simple_batch"
    assert len(report["run_results"]) == 8
    assert len(report["comparisons"]) == 4

    comparison = _comparison(report, "lane097")
    assert comparison["status"] == "candidate_positive"
    assert comparison["reasons"] == ["multi_metric_gate_passed"]
    assert comparison["cadence_comparison"]["bridge_loc_hz_ratio"] == 0.9
    assert comparison["cadence_comparison"]["bridge_chassis_hz_ratio"] == 0.9
    assert comparison["cadence_comparison"]["bridge_loc_count_delta"] == -60
    assert comparison["cadence_comparison"]["bridge_chassis_count_delta"] == -60
    candidate = _run_result(report, "candidate_lane097")
    assert candidate["run_status"] == "success"
    assert candidate["return_code"] is None
    assert candidate["failure_reason"] == "success"
    assert candidate["direct_transport_contract_status"] == "aligned"
    assert candidate["direct_stale_world_frame_policy"] == "always_republish"
    assert candidate["direct_stale_world_frame_policy_source"] == "direct_bridge_stats"
    assert candidate["bridge_loc_count"] == 540
    assert candidate["bridge_chassis_count"] == 540
    assert candidate["bridge_control_rx_count"] == 1200
    assert candidate["bridge_control_tx_count"] == 1200
    assert candidate["bridge_routing_success_count"] == 1
    assert candidate["bridge_loc_hz"] == 18.0
    assert candidate["bridge_cadence_duration_s"] == 30.0
    assert candidate["bridge_cadence_duration_source"] == "manifest_duration_s"
    baseline = _run_result(report, "baseline_lane097")
    assert baseline["bridge_loc_count"] == 600
    assert baseline["bridge_loc_hz"] == 20.0
    assert candidate["steering_normalization_modes"] == ["single_percent_at_select"]
    assert candidate["steering_normalization_mode_counts"] == {"single_percent_at_select": 2}


def test_bridge_stats_timing_cadence_is_recorded_as_diagnostic_context(tmp_path: Path) -> None:
    batch = tmp_path / "batch"
    shutil.copytree(FIXTURE_BATCH, batch)
    for run_id, sim_time in (("baseline_lane097", 60.0), ("candidate_lane097", 90.0)):
        stats_path = batch / "runs" / run_id / "artifacts" / "cyber_bridge_stats.json"
        stats = json.loads(stats_path.read_text(encoding="utf-8"))
        stats["timing"] = {"sim_time_sec": sim_time}
        stats_path.write_text(json.dumps(stats), encoding="utf-8")

    report = analyze_ab_manifest(batch / "ab_manifest.json")

    baseline = _run_result(report, "baseline_lane097")
    candidate = _run_result(report, "candidate_lane097")
    comparison = _comparison(report, "lane097")
    assert baseline["bridge_loc_hz_by_stats_sim_time"] == 10.0
    assert candidate["bridge_loc_hz_by_stats_sim_time"] == 6.0
    assert candidate["direct_snapshot_hz_by_stats_sim_time"] == 600 / 90.0
    assert comparison["cadence_comparison"]["bridge_loc_hz_ratio"] == 0.9
    assert comparison["cadence_comparison"]["bridge_loc_hz_by_stats_sim_time_ratio"] == 0.6


def test_control_health_apply_diagnostics_are_recorded_in_ab_report(tmp_path: Path) -> None:
    batch = tmp_path / "batch"
    shutil.copytree(FIXTURE_BATCH, batch)
    baseline_control = batch / "runs" / "baseline_lane097" / "analysis" / "control_health"
    candidate_control = batch / "runs" / "candidate_lane097" / "analysis" / "control_health"
    baseline_control.mkdir(parents=True)
    candidate_control.mkdir(parents=True)
    (baseline_control / "control_health_report.json").write_text(
        json.dumps(
            {
                "schema_version": "control_health_report.v1",
                "status": "warn",
                "failure_reason": "control_health_warn",
                "warnings": [
                    "control_bridge_world_frame_cadence_low",
                    "control_bridge_drop_same_frame_high",
                ],
                "metrics": {
                    "control_apply_observation_delay_s": 9.1,
                    "route_s_after_first_applied_control_delta_m": 0.02,
                    "stopped_ratio_after_first_applied_control": 0.97,
                    "control_bridge_log": {
                        "available": True,
                        "apply_world_frame_hz": 3.5,
                        "same_frame_drop_ratio": 0.82,
                        "bind_to_first_apply_s": 29.4,
                        "first_watchdog_apply_wall_s": 1040.0,
                        "final_rx_count": 994,
                        "final_applied_count": 1298,
                        "final_drop_same_frame_count": 6078,
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    (candidate_control / "control_health_report.json").write_text(
        json.dumps(
            {
                "schema_version": "control_health_report.v1",
                "status": "pass",
                "failure_reason": None,
                "warnings": [],
                "metrics": {
                    "control_apply_observation_delay_s": 0.1,
                    "route_s_after_first_applied_control_delta_m": 12.0,
                    "stopped_ratio_after_first_applied_control": 0.1,
                },
            }
        ),
        encoding="utf-8",
    )

    report = analyze_ab_manifest(batch / "ab_manifest.json")

    baseline = _run_result(report, "baseline_lane097")
    candidate = _run_result(report, "candidate_lane097")
    comparison = _comparison(report, "lane097")
    assert baseline["control_health_status"] == "warn"
    assert baseline["control_bridge_apply_world_frame_hz"] == 3.5
    assert baseline["control_bridge_same_frame_drop_ratio"] == 0.82
    assert baseline["control_bridge_final_drop_same_frame_count"] == 6078
    assert candidate["control_health_status"] == "pass"
    assert candidate["control_apply_observation_delay_s"] == 0.1
    assert comparison["control_apply_comparison"]["baseline_control_bridge_apply_world_frame_hz"] == 3.5
    assert comparison["control_apply_comparison"]["candidate_control_apply_observation_delay_s"] == 0.1

    outputs = write_ab_report(tmp_path / "out", report)
    summary = Path(outputs["ab_report_summary_md"]).read_text(encoding="utf-8")
    assert "Control Apply Diagnostics" in summary
    assert "control_bridge_world_frame_cadence_low" in summary
    assert "3.5" in summary


def test_channel_health_diagnostics_are_recorded_in_ab_report(tmp_path: Path) -> None:
    batch = tmp_path / "batch"
    shutil.copytree(FIXTURE_BATCH, batch)
    channel_dir = batch / "runs" / "candidate_lane097" / "analysis" / "apollo_channel_health"
    channel_dir.mkdir(parents=True)
    (channel_dir / "apollo_channel_health_report.json").write_text(
        json.dumps(
            {
                "schema_version": "apollo_channel_health_report.v1",
                "status": "warn",
                "missing_required_channels": [],
                "low_rate_channels": ["localization"],
            }
        ),
        encoding="utf-8",
    )
    manifest_path = batch / "ab_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for run in manifest["runs"]:
        if run["run_id"] == "candidate_lane097":
            run["apollo_channel_health_path"] = (
                "runs/candidate_lane097/analysis/apollo_channel_health/apollo_channel_health_report.json"
            )
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    report = analyze_ab_manifest(manifest_path)

    candidate = _run_result(report, "candidate_lane097")
    assert candidate["apollo_channel_health_status"] == "warn"
    assert candidate["apollo_channel_health_low_rate_channels"] == ["localization"]

    outputs = write_ab_report(tmp_path / "out", report)
    with Path(outputs["ab_report_csv"]).open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    candidate_row = next(row for row in rows if row["run_id"] == "candidate_lane097")
    assert candidate_row["apollo_channel_health_status"] == "warn"
    assert candidate_row["apollo_channel_health_low_rate_channels"] == "localization"


def test_candidate_control_health_failure_blocks_positive() -> None:
    baseline = {
        "run_id": "baseline_lane097",
        "route_id": "lane097",
        "backend": "ros2_gt",
        "duration_s": 30.0,
        "run_status": "success",
        "return_code": None,
        "artifact_complete": True,
        "route_completion": 0.7,
        "lateral_error_p95_m": 0.5,
        "heading_error_p95_rad": 0.02,
        "planning_hz": 10.0,
        "carla_applied_control_hz": 20.0,
        "localization_hz": 20.0,
        "failure_reason": "success",
    }
    candidate = {
        **baseline,
        "run_id": "candidate_lane097",
        "backend": "carla_direct",
        "direct_transport_contract_status": "aligned",
        "control_health_status": "fail",
        "control_health_failure_reason": "actuation_mismatch",
    }

    comparison = _compare_pair(baseline, candidate)

    assert comparison["status"] == "insufficient_data"
    assert "candidate control_health failed: actuation_mismatch" in comparison["reasons"]


def test_steering_normalization_mode_mismatch_blocks_positive(tmp_path: Path) -> None:
    batch = tmp_path / "batch"
    shutil.copytree(FIXTURE_BATCH, batch)
    decode_path = batch / "runs" / "candidate_lane097" / "artifacts" / "bridge_control_decode.jsonl"
    decode_path.write_text(
        '{"ts_sec": 1.0, "steering_normalization_mode": "legacy_double_percent", "raw_steer": 0.001}\n',
        encoding="utf-8",
    )

    report = analyze_ab_manifest(batch / "ab_manifest.json")

    comparison = _comparison(report, "lane097")
    assert comparison["status"] == "insufficient_data"
    assert "steering_normalization_mode mismatch" in comparison["reasons"]


def test_direct_transport_contract_mismatch_blocks_positive(tmp_path: Path) -> None:
    batch = tmp_path / "batch"
    shutil.copytree(FIXTURE_BATCH, batch)
    stats_path = batch / "runs" / "candidate_lane097" / "artifacts" / "direct_bridge_stats.json"
    stats_path.write_text(
        json.dumps(
            {
                "control_apply_mode": "frame_flush_only",
                "stale_world_frame_policy": "until_control",
                "snapshot_count": 600,
            }
        ),
        encoding="utf-8",
    )

    report = analyze_ab_manifest(batch / "ab_manifest.json")

    candidate = _run_result(report, "candidate_lane097")
    assert candidate["direct_transport_contract_status"] == "mismatch"
    assert "direct_stale_world_frame_policy mismatch" in candidate["direct_transport_contract_reasons"]
    comparison = _comparison(report, "lane097")
    assert comparison["status"] == "insufficient_data"
    assert any("direct transport contract mismatch" in reason for reason in comparison["reasons"])


def test_direct_stale_policy_can_be_inferred_from_counts(tmp_path: Path) -> None:
    batch = tmp_path / "batch"
    shutil.copytree(FIXTURE_BATCH, batch)
    stats_path = batch / "runs" / "candidate_lane097" / "artifacts" / "direct_bridge_stats.json"
    direct_stats = json.loads(stats_path.read_text(encoding="utf-8"))
    direct_stats.pop("stale_world_frame_policy", None)
    stats_path.write_text(json.dumps(direct_stats), encoding="utf-8")

    report = analyze_ab_manifest(batch / "ab_manifest.json")

    candidate = _run_result(report, "candidate_lane097")
    assert candidate["direct_stale_world_frame_policy"] == "mixed_observed"
    assert candidate["direct_stale_world_frame_policy_source"] == "cyber_bridge_stats_counts"
    assert candidate["direct_transport_contract_status"] == "mismatch"
    assert "direct_stale_world_frame_policy mismatch" in candidate["direct_transport_contract_reasons"]


def test_nonzero_return_code_blocks_candidate_positive(tmp_path: Path) -> None:
    batch = tmp_path / "batch"
    shutil.copytree(FIXTURE_BATCH, batch)
    manifest_path = batch / "ab_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for row in manifest["runs"]:
        if row["run_id"] == "candidate_lane097":
            row["status"] = "failed"
            row["return_code"] = 7
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    report = analyze_ab_manifest(manifest_path)

    candidate = _run_result(report, "candidate_lane097")
    assert candidate["run_status"] == "failed"
    assert candidate["return_code"] == 7
    comparison = _comparison(report, "lane097")
    assert comparison["status"] == "insufficient_data"
    assert "candidate return_code=7" in comparison["reasons"]
    assert "candidate run_status=failed" in comparison["reasons"]


def test_direct_bridge_cadence_requirement_can_pass_with_threshold() -> None:
    report = analyze_ab_manifest(FIXTURE_BATCH / "ab_manifest.json")
    report = {
        **report,
        "run_results": [
            row
            for row in report["run_results"]
            if row["run_id"] in {"baseline_lane097", "candidate_lane097"}
        ],
        "comparisons": [_comparison(report, "lane097")],
    }

    check = check_ab_report_requirements(
        report,
        required_direct_bridge_cadence_ratio_min=0.8,
    )

    assert check["passed"] is True


def test_direct_bridge_cadence_requirement_fails_when_too_strict() -> None:
    report = analyze_ab_manifest(FIXTURE_BATCH / "ab_manifest.json")
    report = {
        **report,
        "run_results": [
            row
            for row in report["run_results"]
            if row["run_id"] in {"baseline_lane097", "candidate_lane097"}
        ],
        "comparisons": [_comparison(report, "lane097")],
    }

    check = check_ab_report_requirements(
        report,
        required_direct_bridge_cadence_ratio_min=0.95,
    )

    assert check["passed"] is False
    assert any("direct_bridge_cadence_ratio too low" in item for item in check["failures"])


def test_direct_bridge_cadence_requirement_fails_when_cadence_missing() -> None:
    report = analyze_ab_manifest(FIXTURE_BATCH / "ab_manifest.json")

    check = check_ab_report_requirements(
        report,
        required_direct_bridge_cadence_ratio_min=0.8,
    )

    assert check["passed"] is False
    assert any("direct_bridge_cadence_ratio missing" in item for item in check["failures"])


def test_candidate_vehicle_moved_only_is_not_positive() -> None:
    report = analyze_ab_manifest(FIXTURE_BATCH / "ab_manifest.json")

    candidate = _run_result(report, "candidate_lane217")
    assert candidate["artifact_complete"] is True
    assert candidate["failure_reason"] == "insufficient_data"
    assert "lateral_error_p95_m" in candidate["missing_fields"]
    assert "heading_error_p95_rad" in candidate["missing_fields"]

    comparison = _comparison(report, "lane217")
    assert comparison["status"] == "insufficient_data"
    assert any("missing required multi-metric evidence" in reason for reason in comparison["reasons"])


def test_missing_route_health_is_insufficient_data() -> None:
    report = analyze_ab_manifest(FIXTURE_BATCH / "ab_manifest.json")

    candidate = _run_result(report, "candidate_junction031")
    assert candidate["artifact_complete"] is False
    assert candidate["failure_reason"] == "artifact_missing"
    assert "route_health" in candidate["missing_fields"]

    comparison = _comparison(report, "junction031")
    assert comparison["status"] == "insufficient_data"
    assert any("artifact_complete is false" in reason for reason in comparison["reasons"])


def test_high_lateral_error_is_degraded_without_blocking_other_routes() -> None:
    report = analyze_ab_manifest(FIXTURE_BATCH / "ab_manifest.json")

    candidate = _run_result(report, "candidate_curve217")
    assert candidate["failure_reason"] == "high_lateral_error"
    assert candidate["lateral_error_p95_m"] == 2.6
    assert candidate["route_curve_artifact_gap_status"] == "insufficient_data"
    assert candidate["route_curve_artifact_gap_failure_reason"] == "per_frame_p1_missing"
    assert candidate["route_curve_missing_p1_fields"] == [
        "apollo_trajectory_heading",
        "matched_point_distance",
        "target_point_distance",
    ]

    comparison = _comparison(report, "curve217")
    assert comparison["status"] == "candidate_degraded"
    assert any("lateral_error_p95_m degraded" in reason for reason in comparison["reasons"])

    assert _comparison(report, "lane097")["status"] == "candidate_positive"
    assert report["verdict"]["status"] == "candidate_degraded"


def test_curve_candidate_positive_is_blocked_without_route_curve_p1_gap_evidence() -> None:
    baseline = {
        "run_id": "baseline_curve217",
        "route_id": "curve217",
        "backend": "ros2_gt",
        "duration_s": 30.0,
        "run_status": "success",
        "return_code": None,
        "artifact_complete": True,
        "route_completion": 0.45,
        "lateral_error_p95_m": 0.5,
        "heading_error_p95_rad": 0.02,
        "planning_hz": 10.0,
        "carla_applied_control_hz": 20.0,
        "localization_hz": 20.0,
        "failure_reason": "success",
        "route_curve_artifact_gap_status": "pass",
    }
    candidate = {
        **baseline,
        "run_id": "candidate_curve217",
        "backend": "carla_direct",
        "direct_transport_contract_status": "aligned",
        "route_curve_artifact_gap_status": "insufficient_data",
        "route_curve_missing_p1_fields": ["matched_point_distance", "target_point_distance"],
    }

    comparison = _compare_pair(baseline, candidate)

    assert comparison["status"] == "insufficient_data"
    assert any("route_curve_artifact_gap not pass" in reason for reason in comparison["reasons"])


def test_first_safety_event_context_is_extracted_from_timeseries(tmp_path: Path) -> None:
    batch = tmp_path / "batch"
    shutil.copytree(FIXTURE_BATCH, batch)
    summary_path = batch / "runs" / "candidate_lane097" / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["lane_invasion_count"] = 1
    summary_path.write_text(json.dumps(summary), encoding="utf-8")
    timeseries_path = batch / "runs" / "candidate_lane097" / "timeseries.csv"
    rows = list(csv.DictReader(timeseries_path.open(encoding="utf-8", newline="")))
    fieldnames = list(rows[0])
    for field in ("frame_id", "route_s", "lane_invasion_count", "bridge_steer_mapped"):
        if field not in fieldnames:
            fieldnames.append(field)
    for index, row in enumerate(rows):
        row["frame_id"] = str(100 + index)
        row["route_s"] = str(20.0 + index)
        row["lane_invasion_count"] = "1" if index >= 2 else "0"
        if index <= 2:
            row["apollo_steer_raw"] = "0.0"
            row["bridge_steer_mapped"] = "0.0"
            row["carla_steer_applied"] = "0.0001"
        if index == 2:
            row["cross_track_error"] = "1.05"
    with timeseries_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    report = analyze_ab_manifest(batch / "ab_manifest.json")

    candidate = _run_result(report, "candidate_lane097")
    assert candidate["failure_reason"] == "lane_invasion"
    assert candidate["first_safety_event_type"] == "lane_invasion"
    assert candidate["first_safety_event_time_s"] == 2.0
    assert candidate["first_safety_event_frame_id"] == "102"
    assert candidate["first_safety_event_route_s"] == 22.0
    assert candidate["first_safety_event_cross_track_error_m"] == 1.05
    assert candidate["first_safety_event_heading_error_rad"] == 0.034
    assert candidate["first_safety_event_ego_speed_mps"] == 2.9
    assert candidate["first_safety_event_apollo_steer_raw"] == 0.0
    assert candidate["first_safety_event_bridge_steer_mapped"] == 0.0
    assert candidate["first_safety_event_carla_steer_applied"] == 0.0001
    assert candidate["first_safety_event_control_context"] == "no_lateral_command_at_safety_event"
    assert candidate["first_safety_event_source"] == "timeseries:lane_invasion_count"
    assert candidate["safety_window_sample_count"] == 3
    assert candidate["safety_window_cross_track_error_delta_m"] == 1.05 - 0.11
    assert candidate["safety_window_apollo_steer_raw_abs_p95"] == 0.0
    assert candidate["safety_window_bridge_steer_mapped_abs_p95"] == 0.0
    assert candidate["safety_window_control_context"] == "sustained_no_lateral_command_before_safety_event"


def test_small_short_window_route_completion_delta_is_tolerated() -> None:
    baseline = {
        "run_id": "baseline_lane097",
        "route_id": "lane097",
        "backend": "ros2_gt",
        "duration_s": 30.0,
        "run_status": "success",
        "return_code": None,
        "artifact_complete": True,
        "route_completion": 0.248,
        "lateral_error_p95_m": 1.02,
        "heading_error_p95_rad": 0.017,
        "planning_hz": 10.0,
        "carla_applied_control_hz": 20.0,
        "localization_hz": 20.0,
        "failure_reason": "success",
    }
    candidate = {
        **baseline,
        "run_id": "candidate_lane097",
        "backend": "carla_direct",
        "route_completion": 0.241,
        "lateral_error_p95_m": 0.66,
        "heading_error_p95_rad": 0.007,
        "direct_transport_contract_status": "aligned",
    }

    comparison = _compare_pair(baseline, candidate)

    assert comparison["status"] == "candidate_positive"
    assert comparison["metric_tolerances"]["route_completion_abs"] == 0.02


def test_large_route_completion_delta_still_degrades_candidate() -> None:
    baseline = {
        "run_id": "baseline_lane097",
        "route_id": "lane097",
        "backend": "ros2_gt",
        "duration_s": 30.0,
        "run_status": "success",
        "return_code": None,
        "artifact_complete": True,
        "route_completion": 0.25,
        "lateral_error_p95_m": 0.5,
        "heading_error_p95_rad": 0.02,
        "planning_hz": 10.0,
        "carla_applied_control_hz": 20.0,
        "localization_hz": 20.0,
        "failure_reason": "success",
    }
    candidate = {
        **baseline,
        "run_id": "candidate_lane097",
        "backend": "carla_direct",
        "route_completion": 0.20,
        "direct_transport_contract_status": "aligned",
    }

    comparison = _compare_pair(baseline, candidate)

    assert comparison["status"] == "candidate_degraded"
    assert "route_completion regressed" in comparison["reasons"]


def test_hard_gate_summary_tracks_097_217_031_no_regression() -> None:
    report = analyze_ab_manifest(FIXTURE_BATCH / "ab_manifest.json")

    hard_gate = report["verdict"]["hard_gate_summary"]
    assert hard_gate["status"] == "hard_gate_insufficient_data"
    assert hard_gate["expected_routes"] == ["lane097", "lane217", "junction031"]
    assert hard_gate["positive_routes"] == ["lane097"]
    assert hard_gate["degraded_routes"] == []
    assert hard_gate["insufficient_routes"] == ["lane217", "junction031"]
    assert hard_gate["missing_routes"] == []
    assert hard_gate["complete"] is True
    assert hard_gate["pass"] is False


def test_report_writer_outputs_fixed_csv_fields(tmp_path: Path) -> None:
    report = analyze_ab_manifest(FIXTURE_BATCH / "ab_manifest.json")
    outputs = write_ab_report(tmp_path, report)

    assert Path(outputs["ab_report_json"]).is_file()
    assert Path(outputs["ab_report_csv"]).is_file()
    assert Path(outputs["ab_report_summary_md"]).is_file()

    with Path(outputs["ab_report_csv"]).open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames == AB_REPORT_CSV_FIELDS
        rows = list(reader)
    assert len(rows) == 8
    lane097_candidate = next(row for row in rows if row["run_id"] == "candidate_lane097")
    assert lane097_candidate["steering_normalization_modes"] == "single_percent_at_select"
    assert lane097_candidate["steering_normalization_mode_counts"] == '{"single_percent_at_select": 2}'
    assert lane097_candidate["bridge_loc_count"] == "540"
    assert lane097_candidate["bridge_loc_hz"] == "18.0"

    payload = json.loads(Path(outputs["ab_report_json"]).read_text(encoding="utf-8"))
    assert payload["verdict"]["candidate_positive_requires_multi_metric_evidence"] is True
    assert payload["verdict"]["hard_gate_summary"]["status"] == "hard_gate_insufficient_data"


def test_cli_writes_ab_report_files(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--batch-root",
            str(FIXTURE_BATCH),
            "--out",
            str(tmp_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["verdict"]["status"] == "candidate_degraded"
    assert (tmp_path / "ab_report.json").is_file()
    assert (tmp_path / "ab_report.csv").is_file()
    summary = (tmp_path / "ab_report_summary.md").read_text(encoding="utf-8")
    assert "not inferred from vehicle motion alone" in summary
    assert "hard_gate_status" in summary
    assert "Bridge Cadence Comparison" in summary
    assert "transport/materialization evidence only" in summary
    assert "do not prove curve health" in summary
    assert "Bridge Cadence Timing Context" in summary
    assert "planned manifest duration" in summary
    assert "| lane097 | 30.0 | 0.9 | 0.9 | -60 | -60 |" in summary
    assert "Steering Normalization Trace" in summary
    assert "Bridge Cadence Counters" in summary
    assert "| candidate_lane097 | carla_direct | 540 | 540 | 18.0 | 18.0 | 1200 | 1200 | 1 |" in summary
    assert "single_percent_at_select:2" in summary


def test_cli_requirement_flags_fail_when_hard_gate_not_passed(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--batch-root",
            str(FIXTURE_BATCH),
            "--out",
            str(tmp_path),
            "--require-hard-gate-pass",
            "--require-steering-normalization-mode",
            "single_percent_at_select",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["requirement_check"]["passed"] is False
    assert any("hard_gate_summary.status" in item for item in payload["requirement_check"]["failures"])
    assert any("expected [single_percent_at_select]" in item for item in payload["requirement_check"]["failures"])


def test_direct_requirement_flags_check_only_carla_direct_rows() -> None:
    report = analyze_ab_manifest(FIXTURE_BATCH / "ab_manifest.json")

    ok = check_ab_report_requirements(
        report,
        required_direct_control_apply_mode="frame_flush_only",
        required_direct_stale_world_frame_policy="always_republish",
    )

    assert ok["passed"] is True

    bad = check_ab_report_requirements(
        report,
        required_direct_stale_world_frame_policy="until_control",
    )

    assert bad["passed"] is False
    assert any("direct_stale_world_frame_policy mismatch" in item for item in bad["failures"])


def test_direct_transport_contract_alignment_requirement_passes_when_aligned() -> None:
    report = {
        "run_results": [
            {
                "run_id": "baseline",
                "backend": "ros2_gt",
                "direct_transport_contract_status": "not_applicable",
            },
            {
                "run_id": "candidate",
                "backend": "carla_direct",
                "direct_transport_contract_status": "aligned",
            },
        ],
        "verdict": {"hard_gate_summary": {"pass": False}},
    }

    result = check_ab_report_requirements(report, require_direct_transport_contract_aligned=True)

    assert result["passed"] is True


def test_requirement_check_can_require_specific_positive_route() -> None:
    report = {
        "comparisons": [
            {"route_id": "lane097", "status": "candidate_degraded"},
        ],
        "run_results": [],
        "verdict": {"hard_gate_summary": {"pass": False}},
    }

    result = check_ab_report_requirements(report, required_positive_routes=["lane097"])

    assert result["passed"] is False
    assert any("comparison_status for lane097" in item for item in result["failures"])


def test_direct_transport_contract_alignment_requirement_fails_on_planned_only() -> None:
    report = {
        "run_results": [
            {
                "run_id": "candidate_dry_run",
                "backend": "carla_direct",
                "direct_transport_contract_status": "planned_only",
            }
        ],
        "verdict": {"hard_gate_summary": {"pass": False}},
    }

    result = check_ab_report_requirements(report, require_direct_transport_contract_aligned=True)

    assert result["passed"] is False
    assert any("direct_transport_contract_status mismatch" in item for item in result["failures"])


def test_requirement_check_fails_on_nonzero_return_code() -> None:
    report = {
        "run_results": [
            {
                "run_id": "candidate_failed",
                "backend": "carla_direct",
                "run_status": "failed",
                "return_code": 7,
                "direct_transport_contract_status": "aligned",
            }
        ],
        "verdict": {"hard_gate_summary": {"pass": False}},
    }

    result = check_ab_report_requirements(report, require_direct_transport_contract_aligned=True)

    assert result["passed"] is False
    assert any("return_code nonzero" in item for item in result["failures"])
    assert any("run_status not successful" in item for item in result["failures"])


def test_requirement_check_can_require_route_curve_p1_completion() -> None:
    report = {
        "run_results": [
            {
                "run_id": "candidate_curve217",
                "route_id": "curve217",
                "backend": "carla_direct",
                "run_status": "success",
                "return_code": 0,
                "route_curve_artifact_gap_status": "insufficient_data",
                "route_curve_per_frame_p1_complete": False,
                "route_curve_missing_p1_fields": ["matched_point_distance"],
            }
        ],
        "comparisons": [],
        "verdict": {"hard_gate_summary": {"pass": False}},
    }

    result = check_ab_report_requirements(report, require_route_curve_p1_complete=True)

    assert result["passed"] is False
    assert any("route_curve_artifact_gap not pass" in item for item in result["failures"])
    assert any("matched_point_distance" in item for item in result["failures"])


def test_requirement_check_passes_when_route_curve_p1_is_complete() -> None:
    report = {
        "run_results": [
            {
                "run_id": "candidate_curve217",
                "route_id": "curve217",
                "backend": "carla_direct",
                "run_status": "success",
                "return_code": 0,
                "route_curve_artifact_gap_status": "pass",
                "route_curve_per_frame_p1_complete": True,
            }
        ],
        "comparisons": [],
        "verdict": {"hard_gate_summary": {"pass": False}},
    }

    result = check_ab_report_requirements(report, require_route_curve_p1_complete=True)

    assert result["passed"] is True
