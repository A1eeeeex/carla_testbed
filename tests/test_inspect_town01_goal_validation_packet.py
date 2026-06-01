from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from tools.inspect_town01_goal_validation_packet import inspect_packet, write_markdown

PREPARE = Path("tools/prepare_town01_goal_validation.py")
INSPECT = Path("tools/inspect_town01_goal_validation_packet.py")


def _prepare_packet(tmp_path: Path) -> Path:
    out = tmp_path / "packet"
    result = subprocess.run(
        [
            sys.executable,
            str(PREPARE),
            "--out",
            str(out),
            "--run-root",
            str(tmp_path / "runs" / "ab"),
            "--batch-prefix",
            "packet_test",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    return Path(payload["manifest"])


def _write_lane097_positive_report(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": "ab_report.v1",
                "verdict": {
                    "status": "candidate_positive",
                    "hard_gate_summary": {
                        "status": "hard_gate_incomplete",
                        "pass": False,
                    },
                },
                "comparisons": [
                    {
                        "route_id": "lane097",
                        "status": "candidate_positive",
                        "candidate_run_id": "candidate_lane097",
                        "cadence_comparison": {
                            "bridge_loc_hz_ratio": 0.9,
                            "bridge_chassis_hz_ratio": 0.9,
                        },
                    }
                ],
                "run_results": [
                    {
                        "run_id": "candidate_lane097",
                        "route_id": "lane097",
                        "backend": "carla_direct",
                        "run_status": "success",
                        "return_code": 0,
                        "failure_reason": "success",
                        "direct_transport_contract_status": "aligned",
                        "direct_control_apply_mode": "frame_flush_only",
                        "direct_stale_world_frame_policy": "always_republish",
                        "steering_normalization_modes": ["legacy_double_percent"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def test_packet_status_starts_at_first_online_stage(tmp_path: Path) -> None:
    manifest = _prepare_packet(tmp_path)

    payload = inspect_packet(manifest)

    assert payload["status"] == "ready_to_start"
    assert payload["stage_count"] == 11
    assert payload["utility_count"] == 1
    assert payload["next_stage"]["name"] == "01_lane097_canary"
    assert payload["next_command"].endswith("01_lane097_canary.sh")
    assert payload["blocked_stage_count"] == 8
    blocked_by_name = {item["stage"]: item for item in payload["blocked_stages"]}
    assert "dependency 01_lane097_canary status=planned" in blocked_by_name["02_hard_gates"]["reasons"][0]
    assert "dependency 09_natural_driving_suite status=planned" in blocked_by_name[
        "10_natural_driving_postprocess"
    ]["reasons"][0]
    assert "dependency 08_postprocess status=planned" in blocked_by_name["11_final_goal_audit"]["reasons"][0]
    hard_stage = next(stage for stage in payload["stages"] if stage["name"] == "02_hard_gates")
    assert hard_stage["dependency_status"]["status"] == "blocked"
    assert payload["disk"]["status"] in {"ok", "warn", "critical"}
    assert payload["disk"]["free_bytes"] > 0
    assert payload["disk"]["used_percent"] >= 0
    assert payload["preflight"]["status"] == "ok"
    assert payload["preflight"]["missing_count"] == 0
    assert any(item["name"] == "tools/run_town01_direct_ab.py" for item in payload["preflight"]["checks"])
    assert any(
        item["name"] == "tools/run_town01_natural_driving_suite.py"
        for item in payload["preflight"]["checks"]
    )


def test_packet_status_advances_after_ab_report_exists(tmp_path: Path) -> None:
    manifest = _prepare_packet(tmp_path)
    packet = json.loads(manifest.read_text(encoding="utf-8"))
    lane_root = Path(packet["scripts"]["01_lane097_canary"]["run_root"])
    lane_report = lane_root / "analysis" / "ab_report.json"
    _write_lane097_positive_report(lane_report)

    payload = inspect_packet(manifest)
    hard_stage = next(stage for stage in payload["stages"] if stage["name"] == "02_hard_gates")

    assert payload["completed_count"] == 1
    assert payload["next_stage"]["name"] == "02_hard_gates"
    assert hard_stage["dependency_status"]["status"] == "satisfied"


def test_packet_status_includes_ab_stage_summary(tmp_path: Path) -> None:
    manifest = _prepare_packet(tmp_path)
    packet = json.loads(manifest.read_text(encoding="utf-8"))
    lane_root = Path(packet["scripts"]["01_lane097_canary"]["run_root"])
    lane_report = lane_root / "analysis" / "ab_report.json"
    lane_report.parent.mkdir(parents=True)
    lane_report.write_text(
        json.dumps(
            {
                "schema_version": "ab_report.v1",
                "verdict": {
                    "status": "candidate_degraded",
                    "hard_gate_summary": {
                        "status": "hard_gate_fail",
                        "pass": False,
                    },
                },
                "comparisons": [
                    {
                        "route_id": "lane097",
                        "status": "candidate_degraded",
                        "candidate_run_id": "candidate_lane097",
                        "cadence_comparison": {
                            "bridge_loc_hz_ratio": 0.3,
                            "bridge_chassis_hz_ratio": 0.3,
                        },
                    }
                ],
                "run_results": [
                    {
                        "run_id": "candidate_lane097",
                        "route_id": "lane097",
                        "backend": "carla_direct",
                        "run_status": "success",
                        "return_code": 0,
                        "failure_reason": "success",
                        "direct_transport_contract_status": "aligned",
                        "direct_control_apply_mode": "frame_flush_only",
                        "direct_stale_world_frame_policy": "mixed_observed",
                        "steering_normalization_modes": ["legacy_double_percent"],
                        "command_stdout_path": str(lane_root / "carla_direct" / "command_stdout.log"),
                        "command_stderr_path": str(lane_root / "carla_direct" / "command_stderr.log"),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    payload = inspect_packet(manifest)
    lane_stage = next(stage for stage in payload["stages"] if stage["name"] == "01_lane097_canary")

    assert payload["status"] == "attention_required"
    assert payload["failed_count"] == 1
    assert payload["blocked_stage_count"] >= 1
    assert payload["next_stage"]["name"] == "01_lane097_canary"
    assert lane_stage["status"] == "failed"
    assert lane_stage["summary"]["summary_type"] == "ab_report"
    assert lane_stage["summary"]["status"] == "failed"
    assert lane_stage["summary"]["verdict_status"] == "candidate_degraded"
    assert lane_stage["summary"]["hard_gate_status"] == "hard_gate_fail"
    assert lane_stage["summary"]["comparison_statuses"] == {"lane097": "candidate_degraded"}
    assert lane_stage["summary"]["direct_policies"] == ["mixed_observed"]
    assert lane_stage["summary"]["direct_contract_statuses"] == ["aligned"]
    assert lane_stage["summary"]["low_cadence_routes"] == ["lane097"]
    assert lane_stage["summary"]["run_logs"][0]["run_id"] == "candidate_lane097"
    assert lane_stage["summary"]["run_logs"][0]["command_stdout_path"].endswith("command_stdout.log")
    assert any(
        "direct_stale_world_frame_policy mismatch" in item
        for item in lane_stage["summary"]["requirement_failures"]
    )


def test_calibration_gate_dependency_accepts_failed_hard_gate_report(tmp_path: Path) -> None:
    manifest = _prepare_packet(tmp_path)
    packet = json.loads(manifest.read_text(encoding="utf-8"))
    hard_root = Path(packet["scripts"]["02_hard_gates"]["run_root"])
    hard_report = hard_root / "analysis" / "ab_report.json"
    hard_report.parent.mkdir(parents=True)
    hard_report.write_text(
        json.dumps(
            {
                "schema_version": "ab_report.v1",
                "verdict": {
                    "status": "candidate_degraded",
                    "hard_gate_summary": {
                        "status": "hard_gate_fail",
                        "pass": False,
                    },
                },
                "comparisons": [
                    {
                        "route_id": "lane097",
                        "status": "candidate_degraded",
                        "candidate_run_id": "candidate_lane097",
                    }
                ],
                "run_results": [
                    {
                        "run_id": "candidate_lane097",
                        "route_id": "lane097",
                        "backend": "carla_direct",
                        "run_status": "success",
                        "return_code": 0,
                        "failure_reason": "success",
                        "direct_transport_contract_status": "mismatch",
                        "direct_transport_contract_reasons": ["direct_bridge_stats missing"],
                        "direct_control_apply_mode": "frame_flush_only",
                        "direct_stale_world_frame_policy": "always_republish",
                        "steering_normalization_modes": ["legacy_double_percent"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    payload = inspect_packet(manifest)
    by_name = {stage["name"]: stage for stage in payload["stages"]}

    assert by_name["02_hard_gates"]["status"] == "failed"
    calibration_dependency = by_name["07_calibration_gates"]["dependency_status"]
    assert calibration_dependency["status"] == "satisfied"
    assert calibration_dependency["checks"][0]["allowed_statuses"] == ["completed", "failed"]


def test_packet_inspector_cli_prints_active_stage_summary(tmp_path: Path) -> None:
    manifest = _prepare_packet(tmp_path)
    packet = json.loads(manifest.read_text(encoding="utf-8"))
    lane_root = Path(packet["scripts"]["01_lane097_canary"]["run_root"])
    stdout_path = lane_root / "carla_direct" / "command_stdout.log"
    stderr_path = lane_root / "carla_direct" / "command_stderr.log"
    stdout_path.parent.mkdir(parents=True)
    stdout_path.write_text("startup ok\nrouting sent\n", encoding="utf-8")
    stderr_path.write_text("old line\nfirst divergence: direct policy mismatch\n", encoding="utf-8")
    lane_report = lane_root / "analysis" / "ab_report.json"
    lane_report.parent.mkdir(parents=True)
    lane_report.write_text(
        json.dumps(
            {
                "schema_version": "ab_report.v1",
                "verdict": {
                    "status": "candidate_degraded",
                    "hard_gate_summary": {
                        "status": "hard_gate_fail",
                        "pass": False,
                    },
                },
                "comparisons": [
                    {
                        "route_id": "lane097",
                        "status": "candidate_degraded",
                        "candidate_run_id": "candidate_lane097",
                        "cadence_comparison": {
                            "bridge_loc_hz_ratio": 0.3,
                            "bridge_chassis_hz_ratio": 0.3,
                        },
                    }
                ],
                "run_results": [
                    {
                        "run_id": "candidate_lane097",
                        "route_id": "lane097",
                        "backend": "carla_direct",
                        "run_status": "success",
                        "return_code": 0,
                        "failure_reason": "success",
                        "direct_transport_contract_status": "aligned",
                        "direct_control_apply_mode": "frame_flush_only",
                        "direct_stale_world_frame_policy": "mixed_observed",
                        "steering_normalization_modes": ["legacy_double_percent"],
                        "command_stdout_path": str(stdout_path),
                        "command_stderr_path": str(stderr_path),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [sys.executable, str(INSPECT), str(manifest)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    stdout_payload = json.loads(result.stdout)
    summary = stdout_payload["active_stage_summary"]
    assert summary["stage"] == "01_lane097_canary"
    assert summary["stage_status"] == "failed"
    assert summary["summary_status"] == "failed"
    assert summary["verdict_status"] == "candidate_degraded"
    assert summary["failure_count"] > 0
    assert summary["run_logs"][0]["run_id"] == "candidate_lane097"
    assert summary["run_logs"][0]["command_stderr_path"].endswith("command_stderr.log")
    assert summary["run_logs"][0]["command_stdout_exists"] is True
    assert summary["run_logs"][0]["command_stderr_exists"] is True
    assert "routing sent" in summary["run_logs"][0]["command_stdout_tail"]
    assert "first divergence" in summary["run_logs"][0]["command_stderr_tail"]
    assert any("direct_stale_world_frame_policy mismatch" in item for item in stdout_payload["active_stage_failures"])
    assert stdout_payload["active_stage_run_logs"][0]["run_id"] == "candidate_lane097"
    assert "first divergence" in stdout_payload["active_stage_run_logs"][0]["command_stderr_tail"]


def test_packet_status_resolves_relative_run_roots_against_repo_root(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    packet_root = tmp_path / "packet"
    run_root = repo_root / "runs" / "ab" / "relative_batch"
    report = run_root / "analysis" / "ab_report.json"
    _write_lane097_positive_report(report)
    script = packet_root / "01_lane097_canary.sh"
    script.parent.mkdir(parents=True)
    script.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    manifest = packet_root / "town01_goal_validation_packet.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": "town01_goal_validation_packet.v1",
                "repo_root": str(repo_root),
                "scripts": {
                    "01_lane097_canary": {
                        "path": str(script),
                        "run_root": "runs/ab/relative_batch",
                        "marker": None,
                        "exit_code_path": None,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    payload = inspect_packet(manifest)

    assert payload["completed_count"] == 1
    assert payload["stages"][0]["run_root"] == str(run_root)
    assert payload["stages"][0]["status"] == "completed"


def test_packet_status_reports_missing_preflight_dependencies(tmp_path: Path) -> None:
    repo_root = tmp_path / "missing_repo"
    packet_root = tmp_path / "packet"
    script = packet_root / "01_lane097_canary.sh"
    script.parent.mkdir(parents=True)
    script.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    manifest = packet_root / "town01_goal_validation_packet.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": "town01_goal_validation_packet.v1",
                "repo_root": str(repo_root),
                "python": str(tmp_path / "missing_python"),
                "scripts": {
                    "01_lane097_canary": {
                        "path": str(script),
                        "run_root": "runs/ab/missing",
                        "marker": None,
                        "exit_code_path": None,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    payload = inspect_packet(manifest)

    assert payload["preflight"]["status"] == "missing_required"
    assert payload["preflight"]["missing_count"] > 0
    missing_names = {item["name"] for item in payload["preflight"]["missing"]}
    assert "python" in missing_names
    assert "tools/run_town01_direct_ab.py" in missing_names


def test_packet_status_reports_failed_stage_exit_code(tmp_path: Path) -> None:
    manifest = _prepare_packet(tmp_path)
    packet = json.loads(manifest.read_text(encoding="utf-8"))
    exit_code_path = Path(packet["scripts"]["01_lane097_canary"]["exit_code_path"])
    exit_code_path.parent.mkdir(parents=True)
    exit_code_path.write_text("2\n", encoding="utf-8")

    payload = inspect_packet(manifest)
    by_name = {stage["name"]: stage for stage in payload["stages"]}

    assert payload["status"] == "attention_required"
    assert payload["failed_count"] == 1
    assert payload["next_stage"]["name"] == "01_lane097_canary"
    assert by_name["01_lane097_canary"]["status"] == "failed"
    assert by_name["01_lane097_canary"]["last_exit_code"] == 2
    assert by_name["01_lane097_canary"]["exit_code_exists"] is True


def test_packet_status_detects_demo_marker_and_postprocess(tmp_path: Path) -> None:
    manifest = _prepare_packet(tmp_path)
    packet = json.loads(manifest.read_text(encoding="utf-8"))
    demo_marker = Path(packet["scripts"]["05_demo_recording"]["marker"])
    demo_inspection = tmp_path / "demo" / "artifacts" / "town01_demo_recording_inspection.json"
    demo_inspection.parent.mkdir(parents=True)
    demo_inspection.write_text('{"status":"ready"}', encoding="utf-8")
    demo_marker.write_text(str(demo_inspection), encoding="utf-8")
    post_root = Path(packet["scripts"]["08_postprocess"]["run_root"])
    (post_root / "town01_postprocess.json").parent.mkdir(parents=True, exist_ok=True)
    (post_root / "town01_postprocess.json").write_text("{}", encoding="utf-8")

    payload = inspect_packet(manifest)
    by_name = {stage["name"]: stage for stage in payload["stages"]}

    assert by_name["05_demo_recording"]["status"] == "completed"
    assert by_name["08_postprocess"]["status"] == "completed"


def test_packet_status_summarizes_natural_driving_report(tmp_path: Path) -> None:
    manifest = _prepare_packet(tmp_path)
    packet = json.loads(manifest.read_text(encoding="utf-8"))
    natural_root = Path(packet["scripts"]["10_natural_driving_postprocess"]["run_root"])
    report = natural_root / "analysis" / "natural_driving" / "natural_driving_report.json"
    report.parent.mkdir(parents=True)
    local_audit = natural_root / "analysis" / "goal_audit" / "town01_goal_audit.json"
    local_audit.parent.mkdir(parents=True)
    report.write_text(
        json.dumps(
            {
                "schema_version": "town01_natural_driving_report.v1",
                "summary": {
                    "pass_count": 2,
                    "warn_count": 0,
                    "fail_count": 1,
                    "insufficient_data_count": 0,
                },
                "verdict": {
                    "status": "fail",
                    "failed_runs": ["traffic_light_red_stop"],
                    "insufficient_data_runs": [],
                    "warning_runs": [],
                },
                "run_results": [
                    {"run_id": "lane_keep_097", "scenario_class": "lane_keep", "verdict": "pass"},
                    {"run_id": "junction_031", "scenario_class": "junction_turn", "verdict": "pass"},
                    {
                        "run_id": "traffic_light_red_stop",
                        "scenario_class": "traffic_light_red_stop",
                        "verdict": "fail",
                        "failure_reason": "red_light_not_stopped",
                        "missing_artifacts": [],
                        "missing_fields": [],
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    local_audit.write_text(
        json.dumps(
            {
                "schema_version": "town01_goal_audit.v1",
                "status": "incomplete",
                "sections": {"natural_driving": {"status": "fail"}},
                "missing_evidence": ["natural_driving_report.json pass with can_claim_full_natural_driving=true"],
                "next_actions": ["inspect failed traffic-light natural-driving run artifacts"],
            }
        ),
        encoding="utf-8",
    )

    payload = inspect_packet(manifest)
    natural_stage = next(stage for stage in payload["stages"] if stage["name"] == "10_natural_driving_postprocess")
    markdown_path = tmp_path / "status.md"
    write_markdown(markdown_path, payload)
    md_text = markdown_path.read_text(encoding="utf-8")

    assert natural_stage["status"] == "failed"
    assert str(local_audit) in natural_stage["evidence_paths"]
    assert natural_stage["summary"]["summary_type"] == "natural_driving_report"
    assert natural_stage["summary"]["status"] == "failed"
    assert natural_stage["summary"]["verdict_status"] == "fail"
    assert natural_stage["summary"]["failed_runs"] == ["traffic_light_red_stop"]
    assert natural_stage["summary"]["local_goal_audit"]["summary_type"] == "goal_audit"
    assert natural_stage["summary"]["local_goal_audit"]["audit_status"] == "incomplete"
    active = next(stage for stage in payload["stages"] if stage["name"] == "10_natural_driving_postprocess")
    assert active["summary"]["fail_count"] == 1
    assert active["summary"]["problem_run_details"][0]["failure_reason"] == "red_light_not_stopped"
    assert natural_stage["missing_evidence_paths"]
    assert any(path.endswith("natural_driving_postprocess.json") for path in natural_stage["missing_evidence_paths"])
    assert "local_goal_audit_status" in md_text


def test_packet_status_marks_natural_postprocess_partial_when_local_audit_missing(
    tmp_path: Path,
) -> None:
    manifest = _prepare_packet(tmp_path)
    packet = json.loads(manifest.read_text(encoding="utf-8"))
    natural_root = Path(packet["scripts"]["10_natural_driving_postprocess"]["run_root"])
    report = natural_root / "analysis" / "natural_driving" / "natural_driving_report.json"
    postprocess = natural_root / "analysis" / "natural_driving" / "natural_driving_postprocess.json"
    report.parent.mkdir(parents=True)
    report.write_text(
        json.dumps(
            {
                "schema_version": "town01_natural_driving_report.v1",
                "summary": {
                    "pass_count": 1,
                    "warn_count": 0,
                    "fail_count": 0,
                    "insufficient_data_count": 0,
                },
                "verdict": {
                    "status": "pass",
                    "can_claim_full_natural_driving": True,
                    "failed_runs": [],
                    "insufficient_data_runs": [],
                    "warning_runs": [],
                },
                "capability_coverage": {
                    "can_claim_full_natural_driving": True,
                    "missing_required_scenario_classes": [],
                    "unproven_required_scenario_classes": [],
                },
                "run_results": [],
            }
        ),
        encoding="utf-8",
    )
    postprocess.write_text("{}", encoding="utf-8")

    payload = inspect_packet(manifest)
    natural_stage = next(stage for stage in payload["stages"] if stage["name"] == "10_natural_driving_postprocess")

    assert natural_stage["status"] == "partial"
    assert natural_stage["summary"]["status"] == "passed"
    assert "local_goal_audit" not in natural_stage["summary"]
    assert any(path.endswith("analysis/goal_audit/town01_goal_audit.json") for path in natural_stage["evidence_paths"])
    assert any(path.endswith("analysis/goal_audit/town01_goal_audit.json") for path in natural_stage["missing_evidence_paths"])
    assert all(Path(path).exists() for path in natural_stage["existing_evidence_paths"])


def test_packet_status_rejects_natural_driving_pass_without_full_claim_flag(tmp_path: Path) -> None:
    manifest = _prepare_packet(tmp_path)
    packet = json.loads(manifest.read_text(encoding="utf-8"))
    natural_root = Path(packet["scripts"]["10_natural_driving_postprocess"]["run_root"])
    report = natural_root / "analysis" / "natural_driving" / "natural_driving_report.json"
    report.parent.mkdir(parents=True)
    classes = [
        "lane_keep",
        "curve_diagnostic",
        "junction_turn",
        "traffic_light_red_stop",
        "traffic_light_green_go",
        "traffic_light_red_to_green_release",
    ]
    report.write_text(
        json.dumps(
            {
                "schema_version": "town01_natural_driving_report.v1",
                "summary": {
                    "pass_count": len(classes),
                    "warn_count": 0,
                    "fail_count": 0,
                    "insufficient_data_count": 0,
                },
                "verdict": {
                    "status": "pass",
                    "failed_runs": [],
                    "insufficient_data_runs": [],
                    "warning_runs": [],
                },
                "run_results": [
                    {
                        "run_id": f"{scenario_class}_run",
                        "scenario_class": scenario_class,
                        "verdict": "pass",
                    }
                    for scenario_class in classes
                ],
            }
        ),
        encoding="utf-8",
    )

    payload = inspect_packet(manifest)
    natural_stage = next(stage for stage in payload["stages"] if stage["name"] == "10_natural_driving_postprocess")

    assert natural_stage["status"] == "failed"
    assert natural_stage["summary"]["status"] == "failed"
    assert natural_stage["summary"]["verdict_status"] == "pass"
    assert natural_stage["summary"]["can_claim_full_natural_driving"] is False
    assert "can_claim_full_natural_driving_not_true" in natural_stage["summary"]["claim_blockers"]


def test_packet_status_rejects_natural_driving_pass_without_manifest_traffic_expectation(tmp_path: Path) -> None:
    manifest = _prepare_packet(tmp_path)
    packet = json.loads(manifest.read_text(encoding="utf-8"))
    natural_root = Path(packet["scripts"]["10_natural_driving_postprocess"]["run_root"])
    report = natural_root / "analysis" / "natural_driving" / "natural_driving_report.json"
    report.parent.mkdir(parents=True)
    classes = [
        "lane_keep",
        "curve_diagnostic",
        "junction_turn",
        "traffic_light_red_stop",
        "traffic_light_green_go",
        "traffic_light_red_to_green_release",
    ]
    report.write_text(
        json.dumps(
            {
                "schema_version": "town01_natural_driving_report.v1",
                "summary": {
                    "pass_count": len(classes),
                    "warn_count": 0,
                    "fail_count": 0,
                    "insufficient_data_count": 0,
                },
                "verdict": {
                    "status": "pass",
                    "can_claim_full_natural_driving": True,
                    "failed_runs": [],
                    "insufficient_data_runs": [],
                    "warning_runs": [],
                },
                "capability_coverage": {
                    "can_claim_full_natural_driving": True,
                    "missing_required_scenario_classes": [],
                    "unproven_required_scenario_classes": [],
                },
                "run_results": [
                    {
                        "run_id": f"{scenario_class}_run",
                        "scenario_class": scenario_class,
                        "verdict": "pass",
                    }
                    for scenario_class in classes
                ],
            }
        ),
        encoding="utf-8",
    )

    payload = inspect_packet(manifest)
    natural_stage = next(stage for stage in payload["stages"] if stage["name"] == "10_natural_driving_postprocess")
    markdown_path = tmp_path / "status.md"
    write_markdown(markdown_path, payload)
    md_text = markdown_path.read_text(encoding="utf-8")

    assert natural_stage["status"] == "failed"
    assert natural_stage["summary"]["status"] == "failed"
    assert "traffic_light_expectation_not_manifest_backed" in natural_stage["summary"]["claim_blockers"]
    assert natural_stage["summary"]["traffic_light_expectation_blockers"]
    assert "traffic_light_expectation_missing" in md_text


def test_packet_cli_surfaces_natural_driving_problem_runs(tmp_path: Path) -> None:
    manifest = _prepare_packet(tmp_path)
    packet = json.loads(manifest.read_text(encoding="utf-8"))
    natural_root = Path(packet["scripts"]["10_natural_driving_postprocess"]["run_root"])
    report = natural_root / "analysis" / "natural_driving" / "natural_driving_report.json"
    report.parent.mkdir(parents=True)
    local_audit = natural_root / "analysis" / "goal_audit" / "town01_goal_audit.json"
    local_audit.parent.mkdir(parents=True)
    report.write_text(
        json.dumps(
            {
                "schema_version": "town01_natural_driving_report.v1",
                "summary": {
                    "pass_count": 0,
                    "warn_count": 0,
                    "fail_count": 0,
                    "insufficient_data_count": 2,
                },
                "verdict": {
                    "status": "insufficient_data",
                    "failed_runs": [],
                    "insufficient_data_runs": ["lane_keep_097", "traffic_light_red_stop"],
                    "warning_runs": [],
                },
                "run_results": [
                    {
                        "run_id": "lane_keep_097",
                        "scenario_class": "lane_keep",
                        "route_id": "lane097",
                        "verdict": "insufficient_data",
                        "failure_reason": "missing_required_artifacts",
                        "missing_artifacts": ["summary.json", "timeseries.csv"],
                        "missing_fields": [],
                    },
                    {
                        "run_id": "traffic_light_red_stop",
                        "scenario_class": "traffic_light_red_stop",
                        "route_id": "traffic_light_red_stop",
                        "verdict": "insufficient_data",
                        "failure_reason": "missing_required_artifacts",
                        "missing_artifacts": ["traffic_light_contract_report.json"],
                        "missing_fields": ["stopped_at_red"],
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    local_audit.write_text(
        json.dumps(
            {
                "schema_version": "town01_goal_audit.v1",
                "status": "incomplete",
                "sections": {"natural_driving": {"status": "incomplete"}},
                "missing_evidence": ["natural-driving artifacts incomplete"],
                "next_actions": ["rerun missing natural-driving scenario artifacts"],
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [sys.executable, str(INSPECT), str(manifest)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["status"] == "attention_required"
    assert payload["active_stage_summary"]["summary_type"] == "natural_driving_report"
    assert payload["active_stage_summary"]["insufficient_data_count"] == 2
    assert payload["active_stage_summary"]["local_goal_audit"]["summary_type"] == "goal_audit"
    assert payload["active_stage_summary"]["local_goal_audit"]["audit_status"] == "incomplete"
    assert any(
        path.endswith("analysis/natural_driving/natural_driving_postprocess.json")
        for path in payload["active_stage_missing_evidence_paths"]
    )
    assert payload["active_stage_problem_runs"] == ["lane_keep_097", "traffic_light_red_stop"]
    assert payload["active_stage_problem_details"][0]["failure_reason"] == "missing_required_artifacts"
    assert "summary.json" in payload["active_stage_problem_details"][0]["missing_artifacts"]


def test_packet_cli_surfaces_final_goal_audit_missing_evidence(tmp_path: Path) -> None:
    manifest = _prepare_packet(tmp_path)
    packet = json.loads(manifest.read_text(encoding="utf-8"))
    final_root = Path(packet["scripts"]["11_final_goal_audit"]["run_root"])
    final_report = final_root / "town01_goal_audit.json"
    final_report.parent.mkdir(parents=True)
    final_report.write_text(
        json.dumps(
            {
                "schema_version": "town01_goal_audit.v1",
                "status": "incomplete",
                "sections": {
                    "ab": {
                        "status": "hard_gate_fail",
                        "hard_gate": {"status": "hard_gate_fail"},
                        "direct_cadence": {"status": "below_threshold"},
                    },
                    "natural_driving": {"status": "missing"},
                    "calibration": {"status": "missing"},
                    "random_regression": {"status": "missing"},
                    "demo_recording": {"status": "ready"},
                },
                "missing_evidence": [
                    "natural_driving_report.json pass with can_claim_full_natural_driving=true",
                    "control-actuation calibration report",
                ],
                "next_actions": [
                    "run the Town01 natural-driving suite and strict postprocess to produce natural_driving_report.json",
                    "collect calibration report only as control-actuation evidence, not automatic promotion",
                ],
                "next_action_commands": {
                    "natural_driving_online": "/home/ubuntu/miniconda3/envs/carla16/bin/python3 tools/run_town01_natural_driving_suite.py --out runs/natural/<batch_id>",
                    "strict_goal_audit": "/home/ubuntu/miniconda3/envs/carla16/bin/python3 tools/audit_town01_goal.py --fail-on-status incomplete",
                },
            }
        ),
        encoding="utf-8",
    )
    exit_code_path = Path(packet["scripts"]["11_final_goal_audit"]["exit_code_path"])
    exit_code_path.parent.mkdir(parents=True)
    exit_code_path.write_text("2\n", encoding="utf-8")
    md_out = tmp_path / "status.md"

    result = subprocess.run(
        [sys.executable, str(INSPECT), str(manifest), "--md-out", str(md_out)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    summary = payload["active_stage_summary"]
    assert payload["status"] == "attention_required"
    assert summary["stage"] == "11_final_goal_audit"
    assert summary["summary_type"] == "goal_audit"
    assert summary["summary_status"] == "incomplete"
    assert summary["audit_status"] == "incomplete"
    assert summary["missing_evidence_count"] == 2
    assert payload["active_stage_missing_evidence"][0].startswith("natural_driving_report.json")
    assert any("natural-driving suite" in item for item in payload["active_stage_next_actions"])
    assert "strict_goal_audit" in payload["active_stage_next_action_commands"]
    md_text = md_out.read_text(encoding="utf-8")
    assert "Missing evidence:" in md_text
    assert "Recommended commands:" in md_text
    assert "natural_driving_online" in md_text


def test_packet_inspector_cli_writes_outputs(tmp_path: Path) -> None:
    manifest = _prepare_packet(tmp_path)
    json_out = tmp_path / "status.json"
    md_out = tmp_path / "status.md"

    result = subprocess.run(
        [
            sys.executable,
            str(INSPECT),
            str(manifest),
            "--json-out",
            str(json_out),
            "--md-out",
            str(md_out),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    stdout_payload = json.loads(result.stdout)
    assert stdout_payload["disk_status"] in {"ok", "warn", "critical"}
    assert stdout_payload["disk_free_gib"] > 0
    assert stdout_payload["blocked_stage_count"] == 8
    assert stdout_payload["blocked_stages"]
    assert stdout_payload["preflight_status"] == "ok"
    assert stdout_payload["preflight_missing_count"] == 0
    assert stdout_payload["active_stage_summary"] is None
    assert stdout_payload["active_stage_failures"] == []
    assert stdout_payload["active_stage_run_logs"] == []
    assert stdout_payload["next_stage"] == "01_lane097_canary"
    assert stdout_payload["next_stage_run_root"].endswith("packet_test_01_lane097")
    assert stdout_payload["next_stage_script_path"].endswith("01_lane097_canary.sh")
    assert stdout_payload["next_stage_evidence_paths"][0].endswith("analysis/ab_report.json")
    assert json.loads(json_out.read_text(encoding="utf-8"))["schema_version"] == "town01_goal_validation_packet_status.v1"
    md_text = md_out.read_text(encoding="utf-8")
    assert "Town01 Goal Validation Packet Status" in md_text
    assert "failed_count" in md_text
    assert "exit_code" in md_text
    assert "summary" in md_text
    assert "## Disk" in md_text
    assert "## Preflight" in md_text
    assert "## Dependency Blocks" in md_text
