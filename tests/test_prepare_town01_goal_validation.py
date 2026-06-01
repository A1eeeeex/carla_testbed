from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


SCRIPT = Path("tools/prepare_town01_goal_validation.py")


def test_prepare_town01_goal_validation_writes_staged_scripts(tmp_path: Path) -> None:
    out = tmp_path / "packet"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--out",
            str(out),
            "--run-root",
            str(tmp_path / "runs" / "ab"),
            "--batch-prefix",
            "test_goal",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["out"] == str(out)
    assert Path(payload["readme"]).is_file()
    assert Path(payload["manifest"]).is_file()
    assert payload["schema_version"] == "town01_goal_validation_packet.v1"
    manifest = json.loads(Path(payload["manifest"]).read_text(encoding="utf-8"))
    assert manifest["repo_root"] == str(Path.cwd())
    assert manifest["python"]
    assert manifest["scripts"]["01_lane097_canary"]["exit_code_path"].endswith(
        "stage_exit_codes/01_lane097_canary.exit_code"
    )
    assert manifest["scripts"]["00_status"]["exit_code_path"] is None
    assert manifest["scripts"]["01_lane097_canary"]["dependencies"] == []
    assert manifest["scripts"]["02_hard_gates"]["dependencies"] == [
        {
            "stage": "01_lane097_canary",
            "require_summary_pass": True,
            "allowed_statuses": ["completed"],
            "require_summary_present": True,
        }
    ]
    assert manifest["scripts"]["03_curve_diagnostics"]["dependencies"] == [
        {
            "stage": "02_hard_gates",
            "require_summary_pass": True,
            "allowed_statuses": ["completed"],
            "require_summary_present": True,
        }
    ]
    assert manifest["scripts"]["04_random_regression"]["dependencies"] == [
        {
            "stage": "02_hard_gates",
            "require_summary_pass": True,
            "allowed_statuses": ["completed"],
            "require_summary_present": True,
        },
        {
            "stage": "03_curve_diagnostics",
            "require_summary_pass": True,
            "allowed_statuses": ["completed"],
            "require_summary_present": True,
        },
    ]
    assert manifest["scripts"]["05_demo_recording"]["dependencies"] == [
        {
            "stage": "02_hard_gates",
            "require_summary_pass": True,
            "allowed_statuses": ["completed"],
            "require_summary_present": True,
        },
        {
            "stage": "03_curve_diagnostics",
            "require_summary_pass": True,
            "allowed_statuses": ["completed"],
            "require_summary_present": True,
        },
        {
            "stage": "04_random_regression",
            "require_summary_pass": True,
            "allowed_statuses": ["completed"],
            "require_summary_present": True,
        },
    ]
    assert manifest["scripts"]["07_calibration_gates"]["dependencies"] == [
        {
            "stage": "02_hard_gates",
            "require_summary_pass": False,
            "allowed_statuses": ["completed", "failed"],
            "require_summary_present": True,
        }
    ]
    assert manifest["scripts"]["09_natural_driving_suite"]["dependencies"] == [
        {
            "stage": "02_hard_gates",
            "require_summary_pass": True,
            "allowed_statuses": ["completed"],
            "require_summary_present": True,
        },
        {
            "stage": "03_curve_diagnostics",
            "require_summary_pass": False,
            "allowed_statuses": ["completed", "failed"],
            "require_summary_present": True,
        },
    ]
    assert manifest["scripts"]["10_natural_driving_postprocess"]["dependencies"] == [
        {
            "stage": "09_natural_driving_suite",
            "require_summary_pass": False,
            "allowed_statuses": ["completed", "failed"],
            "require_summary_present": False,
        }
    ]
    assert manifest["scripts"]["11_final_goal_audit"]["dependencies"] == [
        {
            "stage": "08_postprocess",
            "require_summary_pass": False,
            "allowed_statuses": ["completed", "failed"],
            "require_summary_present": False,
        },
        {
            "stage": "10_natural_driving_postprocess",
            "require_summary_pass": False,
            "allowed_statuses": ["completed", "failed"],
            "require_summary_present": True,
        },
    ]

    status_script = out / "00_status.sh"
    lane_script = out / "01_lane097_canary.sh"
    hard_script = out / "02_hard_gates.sh"
    curve_script = out / "03_curve_diagnostics.sh"
    random_script = out / "04_random_regression.sh"
    demo_script = out / "05_demo_recording.sh"
    audit_script = out / "06_audit.sh"
    calibration_gate_script = out / "07_calibration_gates.sh"
    postprocess_script = out / "08_postprocess.sh"
    natural_suite_script = out / "09_natural_driving_suite.sh"
    natural_postprocess_script = out / "10_natural_driving_postprocess.sh"
    final_audit_script = out / "11_final_goal_audit.sh"
    for script in (
        lane_script,
        hard_script,
        curve_script,
        random_script,
        demo_script,
        audit_script,
        calibration_gate_script,
        postprocess_script,
        natural_suite_script,
        natural_postprocess_script,
        final_audit_script,
    ):
        assert script.is_file()
        assert script.stat().st_mode & 0o111

    status = status_script.read_text(encoding="utf-8")
    assert "tools/inspect_town01_goal_validation_packet.py" in status
    assert "town01_goal_validation_packet.json" in status
    assert "trap refresh_packet_status EXIT" not in status
    assert "stage_exit_codes" not in status

    lane = lane_script.read_text(encoding="utf-8")
    assert "--routes 097" in lane
    assert "lane097_root.path" in lane
    assert "/tmp/town01_goal_validation_lane097_root.txt" not in lane
    assert "stage_exit_codes/01_lane097_canary.exit_code" in lane
    assert "packet_preflight_gate" in lane
    assert "disk critical" in lane
    assert "preflight" in lane
    assert "--require-hard-gate-pass" not in lane
    assert "--require-direct-bridge-cadence-ratio-min 0.8" in lane
    assert "trap refresh_packet_status EXIT" in lane
    assert "tools/inspect_town01_goal_validation_packet.py" in lane
    assert "town01_goal_validation_packet.json" in lane
    assert 'return "$status"' in lane
    assert "PACKET_STAGE_DEPENDENCIES" not in lane

    hard = hard_script.read_text(encoding="utf-8")
    assert "--routes 097,217,031" in hard
    assert "hard_gates_root.path" in hard
    assert "/tmp/town01_goal_validation_hard_gates_root.txt" not in hard
    assert "stage_exit_codes/02_hard_gates.exit_code" in hard
    assert "packet_preflight_gate" in hard
    assert "--require-hard-gate-pass" in hard
    assert "--require-direct-transport-contract-aligned" in hard
    assert "trap refresh_packet_status EXIT" in hard
    assert "packet_stage_dependency_gate" in hard
    assert "01_lane097_canary" in hard
    assert "require_summary_pass" in hard

    curve = curve_script.read_text(encoding="utf-8")
    assert "--include-diagnostic-curves" in curve
    assert "--routes curve217,curve213" in curve
    assert "curves_root.path" in curve
    assert "/tmp/town01_goal_validation_curves_root.txt" not in curve
    assert "stage_exit_codes/03_curve_diagnostics.exit_code" in curve
    assert "packet_preflight_gate" in curve
    assert "--require-hard-gate-pass" not in curve
    assert "--require-route-curve-p1-complete" in curve
    assert "trap refresh_packet_status EXIT" in curve
    assert "packet_stage_dependency_gate" in curve
    assert "02_hard_gates" in curve

    random = random_script.read_text(encoding="utf-8")
    assert "configs/routes/town01/random_regression_pool_20260416.yaml" in random
    assert "--include-informational-routes" in random
    assert "--routes random_lane_183_044,random_lane_213_048,random_junction_176_063,random_junction_071_063,random_curve_219_048,random_curve_177_051" in random
    assert "random_regression_root.path" in random
    assert "/tmp/town01_goal_validation_random_root.txt" not in random
    assert "stage_exit_codes/04_random_regression.exit_code" in random
    assert "packet_preflight_gate" in random
    assert "trap refresh_packet_status EXIT" in random
    assert "packet_stage_dependency_gate" in random
    assert "03_curve_diagnostics" in random

    demo = demo_script.read_text(encoding="utf-8")
    assert "tools/run_town01_demo_showcase.py" in demo
    assert "--record-dreamview" in demo
    assert "--dreamview-auto-open" in demo
    assert "--dreamview-open-wait-page" in demo
    assert "--dreamview-capture-region 1280x720+0,0" in demo
    assert "--require-recording-ready" in demo
    assert "town01_demo_recording_inspection.json" in demo
    assert "demo_recording_inspection.path" in demo
    assert "stage_exit_codes/05_demo_recording.exit_code" in demo
    assert "packet_preflight_gate" in demo
    assert "trap refresh_packet_status EXIT" in demo
    assert "packet_stage_dependency_gate" in demo
    assert "04_random_regression" in demo

    audit = audit_script.read_text(encoding="utf-8")
    assert "tools/audit_town01_goal.py" in audit
    assert "--ab-report" in audit
    assert "--fail-on-status incomplete" not in audit
    assert "test_goal_02_hard_gates/analysis/ab_report.json" in audit
    assert "test_goal_03_curves/analysis/ab_report.json" in audit
    assert "test_goal_04_random_regression/analysis/ab_report.json" in audit
    assert "DEMO_ARGS" in audit
    assert "demo_recording_inspection.path" in audit
    assert "stage_exit_codes/06_audit.exit_code" in audit
    assert "packet_preflight_gate" in audit
    assert "trap refresh_packet_status EXIT" in audit

    calibration_gates = calibration_gate_script.read_text(encoding="utf-8")
    assert "tools/build_calibration_gate_results.py" in calibration_gates
    assert "test_goal_02_hard_gates/analysis/ab_report.json" in calibration_gates
    assert "calibration_gate_results.json" in calibration_gates
    assert "stage_exit_codes/07_calibration_gates.exit_code" in calibration_gates
    assert "packet_preflight_gate" in calibration_gates
    assert "trap refresh_packet_status EXIT" in calibration_gates
    assert "packet_stage_dependency_gate" in calibration_gates
    assert "02_hard_gates" in calibration_gates

    postprocess = postprocess_script.read_text(encoding="utf-8")
    assert "tools/postprocess_town01_goal.py" in postprocess
    assert "--hard-gate-batch" in postprocess
    assert "test_goal_02_hard_gates" in postprocess
    assert "--curve-batch" in postprocess
    assert "test_goal_03_curves" in postprocess
    assert "--random-batch" in postprocess
    assert "test_goal_04_random_regression" in postprocess
    assert "DEMO_ARGS" in postprocess
    assert "demo_recording_inspection.path" in postprocess
    assert "stage_exit_codes/08_postprocess.exit_code" in postprocess
    assert "packet_preflight_gate" in postprocess
    assert "trap refresh_packet_status EXIT" in postprocess

    natural_suite = natural_suite_script.read_text(encoding="utf-8")
    assert "tools/run_town01_natural_driving_suite.py" in natural_suite
    assert "--suite configs/scenarios/town01_natural_driving_suite.yaml" in natural_suite
    assert "--continue-on-failure" in natural_suite
    assert "--carla-ignore-memory-preflight" in natural_suite
    assert "stage_exit_codes/09_natural_driving_suite.exit_code" in natural_suite
    assert "packet_preflight_gate" in natural_suite
    assert "packet_stage_dependency_gate" in natural_suite
    assert "02_hard_gates" in natural_suite
    assert "03_curve_diagnostics" in natural_suite

    natural_postprocess = natural_postprocess_script.read_text(encoding="utf-8")
    assert "tools/run_town01_natural_driving_suite.py" in natural_postprocess
    assert "--postprocess-existing" in natural_postprocess
    assert "--audit-after-postprocess" in natural_postprocess
    assert "--refresh-postprocess" in natural_postprocess
    assert "--suite configs/scenarios/town01_natural_driving_suite.yaml" in natural_postprocess
    assert "--out" in natural_postprocess
    assert "--require-full-target-coverage" in natural_postprocess
    assert "--fail-on-postprocess-status fail,warn,insufficient_data" in natural_postprocess
    assert "--ab-root" in natural_postprocess
    assert "--calibration-root" in natural_postprocess
    assert "--demo-root" in natural_postprocess
    assert "stage_exit_codes/10_natural_driving_postprocess.exit_code" in natural_postprocess
    assert "packet_preflight_gate" in natural_postprocess
    assert "packet_stage_dependency_gate" in natural_postprocess
    assert "09_natural_driving_suite" in natural_postprocess

    final_audit = final_audit_script.read_text(encoding="utf-8")
    assert "tools/audit_town01_goal.py" in final_audit
    assert "--calibration-report" in final_audit
    assert "--natural-driving-report" in final_audit
    assert "--fail-on-status incomplete" in final_audit
    assert "postprocess/calibration/calibration_report.json" in final_audit
    assert "natural_driving_report.json" in final_audit
    assert "stage_exit_codes/11_final_goal_audit.exit_code" in final_audit
    assert "packet_stage_dependency_gate" in final_audit
    assert "08_postprocess" in final_audit
    assert "10_natural_driving_postprocess" in final_audit


def test_prepare_town01_goal_validation_can_include_demo_recording(tmp_path: Path) -> None:
    out = tmp_path / "packet"
    demo = tmp_path / "runs" / "demo" / "artifacts" / "town01_demo_recording_inspection.json"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--out",
            str(out),
            "--batch-prefix",
            "demo_test",
            "--demo-recording",
            str(demo),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    postprocess = (out / "08_postprocess.sh").read_text(encoding="utf-8")
    assert "--demo-recording" in postprocess
    assert str(demo) in postprocess

    audit = (out / "06_audit.sh").read_text(encoding="utf-8")
    assert "--demo-recording" in audit
    assert str(demo) in audit


def test_prepare_town01_goal_validation_default_run_root_is_repo_absolute(tmp_path: Path) -> None:
    out = tmp_path / "packet"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--out",
            str(out),
            "--batch-prefix",
            "default_root_test",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    manifest = json.loads(Path(payload["manifest"]).read_text(encoding="utf-8"))
    lane_root = Path(manifest["scripts"]["01_lane097_canary"]["run_root"])
    assert lane_root.is_absolute()
    assert str(lane_root).startswith(str(Path.cwd() / "runs" / "ab"))


def test_prepare_town01_goal_validation_readme_keeps_guardrails(tmp_path: Path) -> None:
    out = tmp_path / "packet"
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--out", str(out), "--batch-prefix", "guardrail_test"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    readme = (out / "README.md").read_text(encoding="utf-8")
    assert "00_status.sh" in readme
    assert "carla_direct` experimental" in readme
    assert "steer_scale=0.25" in readme
    assert "physical mapping" in readme
    assert "calibration_gate_results.json" in readme
    assert "05_demo_recording.sh" in readme
    assert "08_postprocess.sh" in readme
    assert "10_natural_driving_postprocess.sh" in readme
    assert "local goal audit packet" in readme
    assert "analysis/goal_audit/town01_goal_audit.json" in readme
    assert "does not start CARLA/Apollo" in readme
    assert "Dreamview auto-open" in readme
    assert "town01_goal_validation_packet.json" in readme
    assert "dependency gates" in readme
