from __future__ import annotations

import json
from pathlib import Path

from carla_testbed.analysis.apollo_control_runtime_diff import (
    analyze_apollo_control_runtime_diff,
    write_apollo_control_runtime_diff,
)


def test_control_runtime_overlay_diff_correlates_with_survival(tmp_path: Path) -> None:
    baseline = _write_run(
        tmp_path,
        "baseline",
        overlay=False,
        survived=False,
        handoff_verdict="fail",
        failure_stage="process_health",
        crash_reason="tcmalloc_invalid_free",
    )
    candidate = _write_run(
        tmp_path,
        "candidate",
        overlay=True,
        survived=True,
        handoff_verdict="warn",
        failure_stage="none",
        crash_reason=None,
    )

    report = analyze_apollo_control_runtime_diff(
        baseline_run=baseline,
        candidate_run=candidate,
    )

    assert report["verdict"]["status"] == "pass"
    assert report["overlay_comparison"]["difference"] == "candidate_overlay_enabled"
    assert report["overlay_comparison"]["candidate_selected_count"] == 2
    assert report["control_survival"]["candidate_improved"] is True
    assert report["crash_signature_changed"] is True
    assert (
        report["primary_difference"]
        == "candidate_control_runtime_overlay_correlates_with_process_survival"
    )
    assert "diagnostic" in report["verdict"]["claim_boundary"].lower()


def test_control_runtime_diff_identifies_single_control_cmd_proto_added_target(tmp_path: Path) -> None:
    common_records = [
        {
            "name": "libcontrol_component.so",
            "source": "/opt/apollo/neo/.codex_backup/20260330_170640/control_runtime/libcontrol_component.so",
            "target": "/opt/apollo/neo/lib/modules/control/control_component/libcontrol_component.so",
        },
        {
            "name": "liblat_controller.so",
            "source": "/opt/apollo/neo/.codex_backup/20260330_170640/control_runtime/liblat_controller.so",
            "target": "/opt/apollo/neo/lib/modules/control/controllers/lat_based_lqr_controller/liblat_controller.so",
        },
    ]
    proto_record = {
        "name": "lib_control_cmd_proto_mcc_bin.so",
        "source": "/opt/apollo/neo/.codex_backup/20260330_170358/common_msgs/control_msgs/lib_control_cmd_proto_mcc_bin.so",
        "target": "/opt/apollo/neo/lib/modules/common_msgs/control_msgs/lib_control_cmd_proto_mcc_bin.so",
    }
    baseline = _write_run(
        tmp_path,
        "baseline",
        overlay=True,
        survived=False,
        handoff_verdict="fail",
        failure_stage="process_health",
        crash_reason="module_exited",
        overlay_records=common_records,
    )
    candidate = _write_run(
        tmp_path,
        "candidate",
        overlay=True,
        survived=True,
        handoff_verdict="warn",
        failure_stage="none",
        crash_reason=None,
        overlay_records=common_records + [proto_record],
    )

    report = analyze_apollo_control_runtime_diff(
        baseline_run=baseline,
        candidate_run=candidate,
    )

    overlay = report["overlay_comparison"]
    assert report["verdict"]["status"] == "pass"
    assert overlay["difference"] == "overlay_target_set_changed"
    assert overlay["single_added_name"] == "lib_control_cmd_proto_mcc_bin.so"
    assert overlay["single_added_target"] == proto_record["target"]
    assert overlay["control_cmd_proto_runtime_target_added"] is True
    assert report["control_survival"]["candidate_improved"] is True
    assert (
        report["primary_difference"]
        == "control_cmd_proto_runtime_target_correlates_with_process_survival"
    )
    assert "protobuf runtime differential" in report["diagnostic_conclusion"]


def test_control_runtime_diff_blocks_scenario_mismatch(tmp_path: Path) -> None:
    baseline = _write_run(tmp_path, "baseline", overlay=False, survived=False)
    candidate = _write_run(tmp_path, "candidate", overlay=True, survived=True, scenario_case="other_case")

    report = analyze_apollo_control_runtime_diff(
        baseline_run=baseline,
        candidate_run=candidate,
    )

    assert report["verdict"]["status"] == "insufficient_data"
    assert "scenario_case_mismatch" in report["blocking_reasons"]


def test_control_runtime_diff_does_not_invent_missing_survival(tmp_path: Path) -> None:
    baseline = _write_run(tmp_path, "baseline", overlay=False, survived=False)
    candidate = _write_run(tmp_path, "candidate", overlay=True, survived=True)
    (candidate / "artifacts" / "apollo_control_deferred_survival.json").unlink()

    report = analyze_apollo_control_runtime_diff(
        baseline_run=baseline,
        candidate_run=candidate,
    )

    assert report["verdict"]["status"] == "insufficient_data"
    assert "control_survival_artifact_missing" in report["blocking_reasons"]
    assert report["control_survival"]["candidate"]["available"] is False


def test_control_runtime_diff_writer_outputs_json_and_summary(tmp_path: Path) -> None:
    baseline = _write_run(tmp_path, "baseline", overlay=False, survived=False)
    candidate = _write_run(tmp_path, "candidate", overlay=True, survived=True)
    report = analyze_apollo_control_runtime_diff(baseline_run=baseline, candidate_run=candidate)

    outputs = write_apollo_control_runtime_diff(report, tmp_path / "out")

    report_path = Path(outputs["report"])
    summary_path = Path(outputs["summary"])
    assert report_path.exists()
    assert summary_path.exists()
    written = json.loads(report_path.read_text(encoding="utf-8"))
    assert written["schema_version"] == "apollo_control_runtime_diff.v1"
    assert "Apollo Control Runtime Diff" in summary_path.read_text(encoding="utf-8")


def _write_run(
    tmp_path: Path,
    name: str,
    *,
    overlay: bool,
    survived: bool,
    scenario_case: str = "baguang_follow_stop_static_300m_spawn2m",
    handoff_verdict: str = "fail",
    failure_stage: str = "process_health",
    crash_reason: str | None = "tcmalloc_invalid_free",
    overlay_records: list[dict[str, str]] | None = None,
) -> Path:
    run = tmp_path / name
    artifacts = run / "artifacts"
    handoff = run / "analysis" / "apollo_control_handoff"
    artifacts.mkdir(parents=True)
    handoff.mkdir(parents=True)
    (run / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": name,
                "scenario_id": scenario_case,
                "scenario_case": scenario_case,
                "backend": "apollo_cyberrt",
            }
        ),
        encoding="utf-8",
    )
    (run / "summary.json").write_text(
        json.dumps(
            {
                "run_id": name,
                "scenario_id": scenario_case,
                "backend": "apollo_cyberrt",
                "success": False,
                "fail_reason": "NO_CONTROL_LOG" if not survived else "LANE_INVASION_HARD",
            }
        ),
        encoding="utf-8",
    )
    (artifacts / "apollo_control_deferred_survival.json").write_text(
        json.dumps(
            {
                "control_started_pid_seen": True,
                "control_survived_5s": survived,
                "control_survived_10s": survived,
                "control_present_at_end": survived,
            }
        ),
        encoding="utf-8",
    )
    (handoff / "apollo_control_handoff_report.json").write_text(
        json.dumps(
            {
                "verdict": handoff_verdict,
                "failure_stage": failure_stage,
                "blocking_reasons": [] if handoff_verdict in {"pass", "warn"} else ["process_health_failed"],
                "process_health": {
                    "crash_detected": bool(crash_reason),
                    "crash_reason": crash_reason,
                    "crash_signature": "src/tcmalloc.cc:333] Attempt to free invalid pointer"
                    if crash_reason
                    else None,
                },
            }
        ),
        encoding="utf-8",
    )
    (artifacts / "apollo_docker_libs_manifest.json").write_text(
        json.dumps(
            [
                {
                    "alias_name": "libcyber.so",
                    "resolved_name": "libcyber.so",
                    "resolved_path": "/opt/apollo/neo/lib/cyber/libcyber.so",
                    "sync_mode": "base_sync",
                }
            ]
        ),
        encoding="utf-8",
    )
    if overlay:
        records = overlay_records or [
            {
                "name": "libcontrol_component.so",
                "source": "/opt/apollo/neo/.codex_backup/20260330_170640/control_runtime/libcontrol_component.so",
                "target": "/opt/apollo/neo/lib/modules/control/control_component/libcontrol_component.so",
            },
            {
                "name": "liblat_controller.so",
                "source": "/opt/apollo/neo/.codex_backup/20260330_170640/control_runtime/liblat_controller.so",
                "target": "/opt/apollo/neo/lib/modules/control/controllers/lat_based_lqr_controller/liblat_controller.so",
            },
        ]
        (artifacts / "apollo_control_runtime_overlay_manifest.json").write_text(
            json.dumps(
                {
                    "overlay_active": True,
                    "selected_count": len(records),
                    "records": records,
                }
            ),
            encoding="utf-8",
        )
    else:
        (artifacts / "apollo_control_runtime_overlay_restore.json").write_text(
            json.dumps({"restored": False, "reason": "no_active_state"}),
            encoding="utf-8",
        )
    return run
