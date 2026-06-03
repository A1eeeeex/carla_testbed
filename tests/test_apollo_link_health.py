from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from carla_testbed.analysis.apollo_link_health import (
    APOLLO_LINK_HEALTH_SCHEMA_VERSION,
    analyze_apollo_link_health_run_dir,
    write_apollo_link_health_report,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _base_run(tmp_path: Path) -> Path:
    run_dir = tmp_path / "run"
    _write_json(
        run_dir / "summary.json",
        {
            "run_id": "run",
            "scenario_id": "lane_keep_097",
            "scenario_class": "lane_keep",
            "route_id": "lane097",
            "backend": "apollo_cyberrt",
            "runtime_contract": {"status": "aligned"},
            "routing_materialized": True,
            "routing_success_count": 1,
            "planning_materialized": True,
            "control_handoff_status": "control_consuming_with_nonzero_planning",
            "metrics": {"planning_nonempty_ratio": 1.0},
        },
    )
    _write_json(
        run_dir / "manifest.json",
        {
            "run_id": "run",
            "scenario_id": "lane_keep_097",
            "scenario_class": "lane_keep",
            "route_id": "lane097",
            "backend": "apollo_cyberrt",
            "control_source": "/apollo/control",
            "transport_mode": "ros2_gt",
            "runtime_contract": {"status": "aligned"},
            "carla_world": {
                "configured_town": "Town01",
                "loaded_map_name": "Town01",
                "matches_configured_town": True,
                "spawn_point_count": 255,
            },
            "assist_ledger": {
                "schema_version": "assist_ledger.v1",
                "active_assists": [],
                "blocking_assists": [],
                "non_blocking_assists": [],
                "assist_sources": {},
                "assist_confidence": "explicit",
                "source_artifact": "manifest",
                "can_claim_unassisted_natural_driving": True,
            },
        },
    )
    _write_json(
        run_dir / "artifacts/cyber_bridge_stats.json",
        {
            "routing_success_count": 1,
            "control_rx_count": 100,
            "control_tx_count": 100,
            "apply_control_count": 100,
        },
    )
    _write_json(
        run_dir / "artifacts/bridge_health_summary.json",
        {"status": "pass", "bridge_runtime_import_ok": True},
    )
    _write_json(
        run_dir / "artifacts/bridge_transport_summary.json",
        {"status": "pass", "transport_mode": "ros2_gt"},
    )
    _write_json(
        run_dir / "artifacts/planning_topic_debug_summary.json",
        {"status": "pass", "messages_with_nonzero_trajectory_points": 100},
    )
    _write_json(
        run_dir / "analysis/apollo_channel_health/apollo_channel_health_report.json",
        {
            "schema_version": "apollo_channel_health_report.v1",
            "status": "pass",
            "missing_required_channels": [],
            "low_rate_channels": [],
            "timestamp_failures": [],
        },
    )
    _write_json(
        run_dir / "analysis/localization_contract/localization_contract_report.json",
        {
            "schema_version": "apollo_localization_contract.v1",
            "verdict": {"status": "pass", "blocking_reasons": []},
            "channel": {"status": "pass", "header_frame_id": "map"},
            "pose_consistency": {
                "heading_error_to_route_p95_rad": 0.01,
                "heading_error_to_lane_p95_rad": 0.02,
            },
            "reference_point": {
                "position_uses_vrp": True,
                "vehicle_reference_hard_gate_eligible": True,
            },
            "time": {"measurement_header_delta_ms_p95": 0.0},
            "apollo_hdmap_projection": {
                "file_present": True,
                "official_source_available": True,
                "status": "pass",
                "claim_grade": True,
                "heading_error_p95_rad": 0.01,
                "lateral_error_p95_m": 0.05,
                "blocking_reasons": [],
                "warnings": [],
            },
            "warnings": [],
        },
    )
    _write_json(
        run_dir / "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json",
        {
            "schema_version": "apollo_reference_line_contract.v1",
            "status": "pass",
            "blocking_reasons": [],
            "warnings": [],
            "evidence": {
                "planning_reference_available": True,
                "control_reference_available": True,
                "nonempty_trajectory_ratio": 1.0,
            },
            "metrics": {
                "planning_ref_heading_error_p95_rad": 0.01,
                "control_ref_heading_error_p95_rad": 0.02,
                "control_lateral_error_p95_m": 0.1,
            },
            "apollo_hdmap_projection": {
                "file_present": True,
                "official_source_available": True,
                "status": "pass",
                "claim_grade": True,
                "heading_error_p95_rad": 0.01,
                "lateral_error_p95_m": 0.05,
                "blocking_reasons": [],
                "warnings": [],
            },
        },
    )
    _write_json(
        run_dir / "analysis/apollo_control_handoff/apollo_control_handoff_report.json",
        {
            "schema_version": "apollo_control_handoff.v1",
            "verdict": "pass",
            "failure_stage": "none",
            "blocking_reasons": [],
            "warnings": [],
            "control_channel": {"message_count": 100},
            "bridge_receive": {"control_rx_count": 100},
            "mapping_and_apply": {"apply_control_count": 100},
            "vehicle_response": {"status": "pass"},
        },
    )
    _write_json(
        run_dir / "analysis/control_health/control_health_report.json",
        {
            "schema_version": "control_health_report.v1",
            "status": "pass",
            "failure_reason": None,
            "control_handoff_status": "control_consuming_with_nonzero_planning",
            "metrics": {
                "lateral_guard_apply_count": 0,
                "trajectory_contract_guard_apply_count": 0,
                "mapped_applied_steer_abs_error_p95": 0.0,
                "mapped_applied_throttle_abs_error_p95": 0.0,
                "mapped_applied_brake_abs_error_p95": 0.0,
                "route_s_after_first_applied_control_delta_m": 30.0,
            },
            "warnings": [],
        },
    )
    _write_json(
        run_dir / "analysis/obstacle_gt_contract/obstacle_gt_contract_report.json",
        {
            "schema_version": "obstacle_gt_contract.v1",
            "status": "pass",
            "object_count": 1,
            "errors": [],
            "warnings": [],
            "missing_fields": [],
        },
    )
    _write_json(
        run_dir / "analysis/natural_driving/natural_driving_report.json",
        {
            "schema_version": "natural_driving_report.v1",
            "verdict": {"status": "pass"},
            "can_claim_unassisted_natural_driving": True,
            "warnings": [],
            "blocking_reasons": [],
        },
    )
    return run_dir


def test_all_green_link_health_is_claimable(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["schema_version"] == APOLLO_LINK_HEALTH_SCHEMA_VERSION
    assert report["primary_blocker"] is None
    assert report["can_claim_unassisted_natural_driving"] is True
    assert report["layers"]["traffic_light_gt"]["status"] == "not_applicable"
    assert report["layers"]["hdmap_projection"]["status"] == "pass"


def test_link_health_uses_suite_level_natural_driving_report(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path / "suite")
    local_report = run_dir / "analysis/natural_driving/natural_driving_report.json"
    suite_report = tmp_path / "suite" / "analysis/natural_driving/natural_driving_report.json"
    suite_report.parent.mkdir(parents=True, exist_ok=True)
    local_report.unlink()
    _write_json(
        suite_report,
        {
            "schema_version": "natural_driving_report.v1",
            "verdict": {"status": "warn"},
            "can_claim_unassisted_natural_driving": False,
            "warnings": ["suite_level_warning"],
            "blocking_reasons": [],
        },
    )

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["layers"]["natural_driving_outcome"]["status"] == "warn"
    assert report["layers"]["natural_driving_outcome"]["artifact_paths"]["natural_driving_report"] == str(
        suite_report
    )


def test_reference_line_localization_lane_heading_mismatch_is_primary_blocker(
    tmp_path: Path,
) -> None:
    run_dir = _base_run(tmp_path)
    loc_path = run_dir / "analysis/localization_contract/localization_contract_report.json"
    loc = json.loads(loc_path.read_text(encoding="utf-8"))
    loc["verdict"] = {"status": "fail", "blocking_reasons": ["heading_error_to_lane_high"]}
    loc["pose_consistency"]["heading_error_to_lane_p95_rad"] = 0.3586
    _write_json(loc_path, loc)

    ref_path = run_dir / "analysis/apollo_reference_line_contract/apollo_reference_line_contract_report.json"
    ref = json.loads(ref_path.read_text(encoding="utf-8"))
    ref["status"] = "fail"
    ref["blocking_reasons"] = ["reference_line_heading_error_high"]
    ref["metrics"]["control_ref_heading_error_p95_rad"] = 0.30
    _write_json(ref_path, ref)

    control_path = run_dir / "analysis/control_health/control_health_report.json"
    control = json.loads(control_path.read_text(encoding="utf-8"))
    control["status"] = "fail"
    control["failure_reason"] = "applied_actuation_oscillation"
    _write_json(control_path, control)

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["primary_blocker"] == "reference_line/localization lane-heading mismatch"
    assert "control_mapping_apply:applied_actuation_oscillation" in report["secondary_blockers"]
    assert report["can_claim_unassisted_natural_driving"] is False
    assert report["layers"]["control_mapping_apply"]["status"] == "fail"


def test_control_oscillation_becomes_primary_only_after_localization_and_reference_pass(
    tmp_path: Path,
) -> None:
    run_dir = _base_run(tmp_path)
    control_path = run_dir / "analysis/control_health/control_health_report.json"
    control = json.loads(control_path.read_text(encoding="utf-8"))
    control["status"] = "fail"
    control["failure_reason"] = "applied_actuation_oscillation"
    _write_json(control_path, control)

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["primary_blocker"] == "control_mapping_apply:applied_actuation_oscillation"
    assert report["can_claim_unassisted_natural_driving"] is False


def test_missing_required_artifact_is_insufficient_not_pass(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    (run_dir / "analysis/apollo_channel_health/apollo_channel_health_report.json").unlink()

    report = analyze_apollo_link_health_run_dir(run_dir)

    assert report["layers"]["channel_health"]["status"] == "insufficient_data"
    assert "required_artifact_missing" in report["layers"]["channel_health"]["warnings"]
    assert report["primary_blocker"] == "channel_health:insufficient_data"
    assert report["can_claim_unassisted_natural_driving"] is False


def test_cli_writes_report(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    out = tmp_path / "out"

    result = subprocess.run(
        [
            sys.executable,
            "tools/analyze_apollo_link_health.py",
            "--run-dir",
            str(run_dir),
            "--out",
            str(out),
        ],
        check=True,
        text=True,
        capture_output=True,
    )

    payload = json.loads(result.stdout)
    assert payload["primary_blocker"] is None
    assert (out / "apollo_link_health_report.json").is_file()
    assert (out / "apollo_link_health_summary.md").is_file()


def test_write_report_outputs_json_and_markdown(tmp_path: Path) -> None:
    run_dir = _base_run(tmp_path)
    report = analyze_apollo_link_health_run_dir(run_dir)

    outputs = write_apollo_link_health_report(report, tmp_path / "written")

    assert Path(outputs["apollo_link_health_report"]).is_file()
    assert Path(outputs["apollo_link_health_summary"]).is_file()
