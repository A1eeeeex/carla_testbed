from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, Sequence

from tools.run_town01_goal_offline_audit import (
    build_audit_payload,
    compact_runbook_results,
    default_command_specs,
    exit_code_for_audit,
    inspect_runbook_dry_run_verification,
    write_json,
    write_markdown,
)


def _write_summary(root: Path, route_id: str, *, transport: str, distance_m: float) -> None:
    run_dir = root / f"{route_id}_run"
    run_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "route_id": route_id,
        "transport_mode": transport,
        "runtime_contract": {"status": "aligned"},
        "control_handoff_status": "control_consuming_with_nonzero_planning",
        "route_health_label": "route_established_but_behavior_unhealthy",
        "routing_success_count": 1,
        "route_distance_achieved_m": distance_m,
        "max_speed_mps": 6.0,
        "direct_control_apply_window_status": "observable_apply_window",
        "direct_control_apply_frame_span": 100,
        "direct_control_apply_max_speed_mps": 6.0,
        "direct_metric_consistency_status": "consistent",
    }
    (run_dir / "summary.json").write_text(json.dumps(payload), encoding="utf-8")


def _write_curve_assessment(path: Path, verdict: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "recovery_verdict": verdict,
        "summary": {
            "route_count": 2,
            "candidate_aligned_count": 2,
            "candidate_control_consuming_count": 2,
            "candidate_positive_count": 2 if verdict == "recovery_positive" else 0,
            "candidate_negative_count": 0,
            "candidate_inconclusive_count": 0,
            "verdict_counts": {"candidate_positive_distance": 2}
            if verdict == "recovery_positive"
            else {"candidate_missing": 1},
        },
        "rows": [],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_runbook_dry_run_verification(path: Path, *, status: str = "passed") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "status": status,
        "python_exec": "python",
        "checked_keys": ["followstop_lateral_enabled_canary"],
        "passed_count": 1 if status == "passed" else 0,
        "failed_count": 0 if status == "passed" else 1,
        "results": [
            {
                "key": "followstop_lateral_enabled_canary",
                "status": "passed" if status == "passed" else "failed",
                "returncode": 0 if status == "passed" else 1,
            }
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_demo_run(root: Path) -> None:
    run_dir = root / "demo_route"
    artifacts = run_dir / "artifacts"
    raw_tp = run_dir / "video" / "dual_cam" / "raw_tp"
    dreamview_video = run_dir / "video" / "dreamview" / "dreamview_capture.mp4"
    artifacts.mkdir(parents=True, exist_ok=True)
    raw_tp.mkdir(parents=True, exist_ok=True)
    dreamview_video.parent.mkdir(parents=True, exist_ok=True)
    (run_dir / "summary.json").write_text(json.dumps({"route_id": "demo"}), encoding="utf-8")
    for index in range(12):
        (raw_tp / f"{index:06d}.png").write_bytes(b"png")
    dreamview_video.write_bytes(b"mp4")
    manifest = {
        "recording_success": True,
        "recording_status": "success",
        "output_video_generated": True,
        "output_video_path": str(dreamview_video),
        "frame_count": 12,
    }
    (artifacts / "dreamview_capture_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (artifacts / "dreamview_recording_status.json").write_text(json.dumps(manifest), encoding="utf-8")


def _passing_runner(cmd: Sequence[str], cwd: Path, timeout_s: float) -> Dict[str, Any]:
    return {
        "cmd": " ".join(cmd),
        "returncode": 0,
        "elapsed_s": 0.01,
        "stdout_tail": "ok",
        "stderr_tail": "",
        "status": "passed",
    }


def _failing_runner(cmd: Sequence[str], cwd: Path, timeout_s: float) -> Dict[str, Any]:
    return {
        "cmd": " ".join(cmd),
        "returncode": 1,
        "elapsed_s": 0.01,
        "stdout_tail": "",
        "stderr_tail": "boom",
        "status": "failed",
    }


def _runbook_results() -> Dict[str, Any]:
    return {
        "status": "in_progress",
        "next_key": "followstop_lateral_enabled_canary",
        "next_command": "python -m carla_testbed run --config followstop.yaml",
        "runbook_status": "ready",
        "recommended_order": ["offline_smoke_config", "followstop_lateral_enabled_canary"],
        "results": {
            "followstop_lateral_enabled_canary": {
                "status": "dry_run_only",
                "reason": "online summary is missing",
            }
        },
    }


class RunTown01GoalOfflineAuditTests(unittest.TestCase):
    def test_default_commands_include_demo_dreamview_dry_run(self) -> None:
        specs = default_command_specs("python")
        demo = [spec for spec in specs if spec["name"] == "town01_demo_showcase_dry_run"]

        self.assertEqual(len(demo), 1)
        cmd = demo[0]["cmd"]
        self.assertIn("--record-dreamview", cmd)
        self.assertIn("--dreamview-auto-open", cmd)
        self.assertIn("--dreamview-open-wait-page", cmd)
        self.assertIn("playwright", cmd)
        self.assertIn("--dry-run", cmd)

    def test_default_commands_include_online_preflight(self) -> None:
        specs = default_command_specs("python")
        preflight = [spec for spec in specs if spec["name"] == "town01_online_preflight"]

        self.assertEqual(len(preflight), 1)
        self.assertIn("tools/inspect_town01_online_preflight.py", preflight[0]["cmd"])
        self.assertIn("artifacts/town01_online_preflight_20260522.json", preflight[0]["cmd"])

    def test_default_commands_include_goal_resume(self) -> None:
        specs = default_command_specs("python")
        resume = [spec for spec in specs if spec["name"] == "town01_goal_resume"]

        self.assertEqual(len(resume), 1)
        self.assertIn("tools/inspect_town01_goal_resume.py", resume[0]["cmd"])
        self.assertIn("artifacts/town01_goal_resume_20260522.json", resume[0]["cmd"])

    def test_default_commands_include_operator_handoff(self) -> None:
        specs = default_command_specs("python")
        handoff = [spec for spec in specs if spec["name"] == "town01_operator_handoff"]

        self.assertEqual(len(handoff), 1)
        self.assertIn("tools/prepare_town01_operator_handoff.py", handoff[0]["cmd"])
        self.assertIn("artifacts/town01_operator_handoff_20260522.json", handoff[0]["cmd"])

    def test_default_commands_include_online_runbook(self) -> None:
        specs = default_command_specs("python")
        runbook = [spec for spec in specs if spec["name"] == "town01_goal_online_runbook"]

        self.assertEqual(len(runbook), 1)
        self.assertIn("tools/prepare_town01_goal_online_runbook.py", runbook[0]["cmd"])

    def test_default_commands_include_online_runbook_results(self) -> None:
        specs = default_command_specs("python")
        results = [spec for spec in specs if spec["name"] == "town01_goal_online_runbook_results"]

        self.assertEqual(len(results), 1)
        self.assertIn("tools/inspect_town01_goal_online_runbook_results.py", results[0]["cmd"])

    def test_default_commands_include_online_runbook_executor_dry_run(self) -> None:
        specs = default_command_specs("python")
        executor = [spec for spec in specs if spec["name"] == "town01_goal_online_runbook_executor_dry_run"]

        self.assertEqual(len(executor), 1)
        self.assertIn("tools/run_town01_goal_online_runbook.py", executor[0]["cmd"])
        self.assertIn("--dry-run", executor[0]["cmd"])

    def test_default_commands_include_online_runbook_execution_inspector(self) -> None:
        specs = default_command_specs("python")
        inspector = [spec for spec in specs if spec["name"] == "town01_goal_online_runbook_execution_inspect"]

        self.assertEqual(len(inspector), 1)
        self.assertIn("tools/inspect_town01_goal_online_runbook_execution.py", inspector[0]["cmd"])
        self.assertIn("--allow-dry-run-fallback", inspector[0]["cmd"])

    def test_default_commands_include_next_online_action(self) -> None:
        specs = default_command_specs("python")
        action = [spec for spec in specs if spec["name"] == "town01_next_online_action"]

        self.assertEqual(len(action), 1)
        self.assertIn("tools/inspect_town01_next_online_action.py", action[0]["cmd"])
        self.assertIn("--python-exec", action[0]["cmd"])

    def test_default_commands_include_after_online_intake(self) -> None:
        specs = default_command_specs("python")
        intake = [spec for spec in specs if spec["name"] == "town01_after_online_intake"]

        self.assertEqual(len(intake), 1)
        self.assertIn("tools/inspect_town01_after_online.py", intake[0]["cmd"])
        self.assertIn("--allow-dry-run-fallback", intake[0]["cmd"])

    def test_default_commands_include_goal_completion_audit(self) -> None:
        specs = default_command_specs("python")
        completion = [spec for spec in specs if spec["name"] == "town01_goal_completion_audit"]

        self.assertEqual(len(completion), 1)
        self.assertIn("tools/inspect_town01_goal_completion.py", completion[0]["cmd"])
        self.assertIn("--python-exec", completion[0]["cmd"])

    def test_default_commands_include_runbook_heavy_dry_run_verification(self) -> None:
        specs = default_command_specs("python")
        dry_run = [spec for spec in specs if spec["name"] == "town01_runbook_heavy_dry_run_verification"]

        self.assertEqual(len(dry_run), 1)
        self.assertIn("tools/verify_town01_runbook_dry_runs.py", dry_run[0]["cmd"])
        self.assertIn("--require-passed", dry_run[0]["cmd"])

    def test_default_commands_include_demo_readiness_relaxed_check(self) -> None:
        specs = default_command_specs("python")
        readiness = [spec for spec in specs if spec["name"] == "town01_demo_readiness_relaxed"]

        self.assertEqual(len(readiness), 1)
        self.assertIn("tools/inspect_town01_demo_readiness.py", readiness[0]["cmd"])
        self.assertIn("--browser-cmd", readiness[0]["cmd"])
        self.assertIn("playwright", readiness[0]["cmd"])
        self.assertIn("--no-require-browser", readiness[0]["cmd"])
        self.assertIn("--no-require-video-encoder", readiness[0]["cmd"])

    def test_default_commands_include_split_curve_recovery_dry_runs(self) -> None:
        specs = default_command_specs("python")
        curve176 = [spec for spec in specs if spec["name"] == "direct_curve176_recovery_dry_run"]
        curve177 = [spec for spec in specs if spec["name"] == "direct_curve177_recovery_dry_run"]

        self.assertEqual(len(curve176), 1)
        self.assertEqual(len(curve177), 1)
        self.assertIn("tools/run_town01_direct_curve_recovery_retry.py", curve176[0]["cmd"])
        self.assertIn("town01_rh_spawn176_goal061", curve176[0]["cmd"])
        self.assertIn("town01_rh_spawn177_goal052", curve177[0]["cmd"])
        self.assertIn("--no-preflight", curve176[0]["cmd"])
        self.assertIn("--dry-run", curve176[0]["cmd"])

    def test_default_commands_include_pair_curve_recovery_dry_run(self) -> None:
        specs = default_command_specs("python")
        pair = [spec for spec in specs if spec["name"] == "direct_curve_pair_recovery_dry_run"]

        self.assertEqual(len(pair), 1)
        self.assertIn("tools/run_town01_direct_curve_pair_recovery_retry.py", pair[0]["cmd"])
        self.assertIn("--no-preflight", pair[0]["cmd"])
        self.assertIn("--dry-run", pair[0]["cmd"])

    def test_default_commands_include_goal_sequence_dry_run(self) -> None:
        specs = default_command_specs("python")
        sequence = [spec for spec in specs if spec["name"] == "town01_goal_sequence_dry_run"]

        self.assertEqual(len(sequence), 1)
        self.assertIn("tools/run_town01_goal_sequence.py", sequence[0]["cmd"])
        self.assertIn("--dry-run", sequence[0]["cmd"])

    def test_default_commands_include_post_online_triage(self) -> None:
        specs = default_command_specs("python")
        triage = [spec for spec in specs if spec["name"] == "town01_post_online_triage"]

        self.assertEqual(len(triage), 1)
        self.assertIn("tools/inspect_town01_post_online_triage.py", triage[0]["cmd"])

    def test_compact_runbook_results_extracts_broader_next(self) -> None:
        compact = compact_runbook_results(_runbook_results())

        self.assertEqual(compact["next_key"], "followstop_lateral_enabled_canary")
        self.assertEqual(compact["next_status"], "dry_run_only")
        self.assertIn("carla_testbed run", compact["next_command"])

    def test_audit_passes_offline_but_goal_remains_in_progress(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ab = root / "ab"
            _write_summary(ab / "baseline", "route_a", transport="ros2_gt", distance_m=50.0)
            _write_summary(ab / "candidate", "route_a", transport="carla_direct", distance_m=70.0)
            _write_curve_assessment(ab / "direct_curve_recovery_assessment.json", "recovery_incomplete_candidate_missing")

            payload = build_audit_payload(
                python_exec="python",
                cwd=root,
                command_specs=[{"name": "fake", "kind": "unit", "cmd": ["python", "-V"]}],
                command_runner=_passing_runner,
                transport_ab_root=ab,
                runs_root=root / "runs",
                demo_root=None,
                next_online_action_json=root / "missing_next_action.json",
                runbook_results_builder=lambda python_exec: _runbook_results(),
            )

            self.assertEqual(payload["audit_status"], "offline_pass_goal_in_progress")
            self.assertEqual(payload["runbook_results"]["next_key"], "followstop_lateral_enabled_canary")
            self.assertEqual(payload["next_online_action"]["primary"]["key"], "followstop_lateral_enabled_canary")
            self.assertEqual(payload["test_results"][0]["status"], "passed")
            self.assertEqual(exit_code_for_audit(payload, require_goal_ready=True), 2)

    def test_audit_reports_ready_when_goal_and_tests_pass(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ab = root / "ab"
            demo = root / "demo"
            for route_id in ["route_a", "route_b"]:
                _write_summary(ab / "baseline", route_id, transport="ros2_gt", distance_m=50.0)
                _write_summary(ab / "candidate", route_id, transport="carla_direct", distance_m=70.0)
            _write_curve_assessment(ab / "direct_curve_recovery_assessment.json", "recovery_positive")
            _write_demo_run(demo)

            payload = build_audit_payload(
                python_exec="python",
                cwd=root,
                command_specs=[{"name": "fake", "kind": "unit", "cmd": ["python", "-V"]}],
                command_runner=_passing_runner,
                transport_ab_root=ab,
                runs_root=root / "runs",
                demo_root=demo,
                next_online_action_json=root / "missing_next_action.json",
                runbook_results_builder=lambda python_exec: _runbook_results(),
            )

            self.assertEqual(payload["audit_status"], "offline_pass_goal_ready")
            self.assertEqual(exit_code_for_audit(payload, require_goal_ready=True), 0)

    def test_audit_reports_failed_test_even_if_goal_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ab = root / "ab"
            demo = root / "demo"
            for route_id in ["route_a", "route_b"]:
                _write_summary(ab / "baseline", route_id, transport="ros2_gt", distance_m=50.0)
                _write_summary(ab / "candidate", route_id, transport="carla_direct", distance_m=70.0)
            _write_curve_assessment(ab / "direct_curve_recovery_assessment.json", "recovery_positive")
            _write_demo_run(demo)

            payload = build_audit_payload(
                python_exec="python",
                cwd=root,
                command_specs=[{"name": "fake", "kind": "unit", "cmd": ["python", "-V"]}],
                command_runner=_failing_runner,
                transport_ab_root=ab,
                runs_root=root / "runs",
                demo_root=demo,
                next_online_action_json=root / "missing_next_action.json",
                runbook_results_builder=lambda python_exec: _runbook_results(),
            )

            self.assertEqual(payload["audit_status"], "offline_validation_failed")
            self.assertEqual(exit_code_for_audit(payload, require_goal_ready=True), 1)

    def test_runbook_dry_run_artifact_can_be_inspected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "artifacts" / "runbook.json"
            _write_runbook_dry_run_verification(path)

            result = inspect_runbook_dry_run_verification(path, required=True)

            self.assertEqual(result["status"], "passed")
            self.assertEqual(result["passed_count"], 1)
            self.assertEqual(result["failed_count"], 0)

    def test_required_runbook_dry_run_missing_blocks_audit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ab = root / "ab"
            _write_summary(ab / "baseline", "route_a", transport="ros2_gt", distance_m=50.0)
            _write_summary(ab / "candidate", "route_a", transport="carla_direct", distance_m=70.0)
            _write_curve_assessment(ab / "direct_curve_recovery_assessment.json", "recovery_incomplete_candidate_missing")

            payload = build_audit_payload(
                python_exec="python",
                cwd=root,
                command_specs=[{"name": "fake", "kind": "unit", "cmd": ["python", "-V"]}],
                command_runner=_passing_runner,
                transport_ab_root=ab,
                runs_root=root / "runs",
                runbook_dry_run_json=root / "artifacts" / "missing.json",
                require_runbook_dry_run=True,
                next_online_action_json=root / "missing_next_action.json",
                runbook_results_builder=lambda python_exec: _runbook_results(),
            )

            self.assertEqual(payload["audit_status"], "offline_validation_failed")
            self.assertEqual(payload["runbook_dry_run_verification"]["status"], "missing")

    def test_required_runbook_dry_run_failure_blocks_audit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ab = root / "ab"
            runbook_json = root / "artifacts" / "runbook.json"
            _write_summary(ab / "baseline", "route_a", transport="ros2_gt", distance_m=50.0)
            _write_summary(ab / "candidate", "route_a", transport="carla_direct", distance_m=70.0)
            _write_curve_assessment(ab / "direct_curve_recovery_assessment.json", "recovery_incomplete_candidate_missing")
            _write_runbook_dry_run_verification(runbook_json, status="failed")

            payload = build_audit_payload(
                python_exec="python",
                cwd=root,
                command_specs=[{"name": "fake", "kind": "unit", "cmd": ["python", "-V"]}],
                command_runner=_passing_runner,
                transport_ab_root=ab,
                runs_root=root / "runs",
                runbook_dry_run_json=runbook_json,
                require_runbook_dry_run=True,
                next_online_action_json=root / "missing_next_action.json",
                runbook_results_builder=lambda python_exec: _runbook_results(),
            )

            self.assertEqual(payload["audit_status"], "offline_validation_failed")
            self.assertEqual(payload["runbook_dry_run_verification"]["status"], "failed")

    def test_writes_audit_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ab = root / "ab"
            _write_summary(ab / "baseline", "route_a", transport="ros2_gt", distance_m=50.0)
            _write_summary(ab / "candidate", "route_a", transport="carla_direct", distance_m=70.0)
            _write_curve_assessment(ab / "direct_curve_recovery_assessment.json", "recovery_incomplete_candidate_missing")
            payload = build_audit_payload(
                python_exec="python",
                cwd=root,
                run_tests=False,
                transport_ab_root=ab,
                runs_root=root / "runs",
                next_online_action_json=root / "missing_next_action.json",
                runbook_results_builder=lambda python_exec: _runbook_results(),
            )
            json_out = root / "audit.json"
            md_out = root / "audit.md"

            write_json(json_out, payload)
            write_markdown(md_out, payload)

            self.assertEqual(json.loads(json_out.read_text(encoding="utf-8"))["audit_status"], "offline_pass_goal_in_progress")
            text = md_out.read_text(encoding="utf-8")
            self.assertIn("Town01 Goal Offline Audit", text)
            self.assertIn("Selected Next Online Action", text)
            self.assertIn("Broader Validation Next Command", text)
            self.assertIn("Direct Chain Next Command", text)
            self.assertIn("followstop_lateral_enabled_canary", text)


if __name__ == "__main__":
    unittest.main()
