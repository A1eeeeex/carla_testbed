from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tools import inspect_town01_goal_online_runbook_results as results


class InspectTown01GoalOnlineRunbookResultsTests(unittest.TestCase):
    def _write_transport_summary(
        self,
        path: Path,
        *,
        route_id: str,
        distance: float,
        speed: float,
        transport_mode: str,
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "route_id": route_id,
                    "runtime_contract": {"status": "aligned"},
                    "control_handoff_status": "control_consuming_with_nonzero_planning",
                    "route_health_label": "behavior_unhealthy",
                    "routing_success_count": 1,
                    "transport_mode": transport_mode,
                    "route_distance_achieved_m": distance,
                    "max_speed_mps": speed,
                }
            ),
            encoding="utf-8",
        )

    def test_summary_status_passes_smoke_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            summary = Path(tmpdir) / "summary.json"
            summary.write_text(json.dumps({"success": True, "exit_reason": "smoke_config_ok"}), encoding="utf-8")

            payload = results._summary_status(summary)

            self.assertEqual(payload["status"], "passed")

    def test_summary_status_reports_present_not_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            summary = Path(tmpdir) / "summary.json"
            summary.write_text(json.dumps({"success": False, "exit_reason": "boom"}), encoding="utf-8")

            payload = results._summary_status(summary)

            self.assertEqual(payload["status"], "present_not_success")
            self.assertIn("success=False", payload["reason"])

    def test_goal_sequence_distinguishes_dry_run_from_online(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            with mock.patch.object(results, "REPO_ROOT", root):
                artifacts = root / "artifacts"
                artifacts.mkdir(parents=True)
                (artifacts / "town01_goal_sequence_dry_run_20260522.json").write_text(
                    json.dumps({"status": "dry_run"}),
                    encoding="utf-8",
                )

                payload = results._inspect_goal_sequence()

            self.assertEqual(payload["status"], "dry_run_only")

    def test_post_online_triage_is_auxiliary_gate_when_status_is_recognized(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            artifacts = root / "artifacts"
            artifacts.mkdir(parents=True)
            (artifacts / "town01_post_online_triage_20260522.json").write_text(
                json.dumps(
                    {
                        "triage": {
                            "status": "ready_for_demo_recording",
                            "reason": "demo next",
                            "next_command": "python demo.py",
                        }
                    }
                ),
                encoding="utf-8",
            )

            with mock.patch.object(results, "REPO_ROOT", root):
                payload = results._inspect_post_online_triage()

            self.assertEqual(payload["status"], "passed")
            self.assertEqual(payload["triage_status"], "ready_for_demo_recording")
            self.assertEqual(payload["next_command"], "python demo.py")

    def test_followstop_requires_materialized_canary_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "runs" / "followstop_goal_lateral_enabled_canary"
            artifacts = run_dir / "artifacts"
            artifacts.mkdir(parents=True)
            (run_dir / "summary.json").write_text(
                json.dumps(
                    {
                        "success": False,
                        "adapter_started": True,
                        "max_speed_mps": 1.2,
                        "ros2_gt": {"counts": {"odom": 15}},
                    }
                ),
                encoding="utf-8",
            )
            (artifacts / "cyber_bridge_stats.json").write_text(
                json.dumps({"loc_count": 10, "chassis_count": 10, "control_tx_count": 7}),
                encoding="utf-8",
            )

            with mock.patch.object(results, "REPO_ROOT", root):
                payload = results._inspect_followstop()

        self.assertEqual(payload["status"], "canary_evidence_present")
        self.assertEqual(payload["checks"]["cyber_control_tx_count"], 7)

    def test_followstop_rejects_summary_without_materialization(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "runs" / "followstop_goal_lateral_enabled_canary"
            run_dir.mkdir(parents=True)
            (run_dir / "summary.json").write_text(
                json.dumps({"success": False, "adapter_started": False, "max_speed_mps": 0.0}),
                encoding="utf-8",
            )

            with mock.patch.object(results, "REPO_ROOT", root):
                payload = results._inspect_followstop()

            self.assertEqual(payload["status"], "present_not_passing")

    def test_followstop_online_attempt_without_summary_is_not_masked_by_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "runs" / "followstop_goal_lateral_enabled_canary"
            artifacts = run_dir / "artifacts"
            artifacts.mkdir(parents=True)
            (run_dir / "effective.yaml").write_text("scenario:\n  driver: carla_followstop\n", encoding="utf-8")
            (artifacts / "followstop_child.stderr.log").write_text("Traceback: boom\n", encoding="utf-8")
            dry_run = root / "runs" / "followstop_goal_lateral_enabled_canary_check"
            dry_run.mkdir(parents=True)
            (dry_run / "effective.yaml").write_text("dry_run: true\n", encoding="utf-8")

            with mock.patch.object(results, "REPO_ROOT", root):
                payload = results._inspect_followstop()

            self.assertEqual(payload["status"], "online_attempt_no_summary")
            self.assertTrue(payload["has_effective_config"])
            self.assertIn("boom", payload["stderr_tail"])
            self.assertIn(
                "followstop_goal_lateral_enabled_canary",
                {item["name"] for item in payload["recent_attempts"]},
            )

    def test_followstop_reports_dry_run_only_when_no_online_attempt_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            dry_run = root / "runs" / "followstop_goal_lateral_enabled_canary_check"
            artifacts = dry_run / "artifacts"
            artifacts.mkdir(parents=True)
            (dry_run / "effective.yaml").write_text("dry_run: true\n", encoding="utf-8")
            (artifacts / "doctor.txt").write_text(
                "\n".join(
                    [
                        "[carla_server]",
                        "host: 127.0.0.1",
                        "port: 2000",
                        "reachable: False",
                        "error: Operation not permitted",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (artifacts / "run_meta.json").write_text(
                json.dumps(
                    {
                        "carla_launch_policy": {
                            "start": True,
                            "host": "localhost",
                            "port": 2000,
                            "town": "Town01",
                            "extra_args": "-windowed",
                            "need_ros2_native": False,
                        }
                    }
                ),
                encoding="utf-8",
            )

            with mock.patch.object(results, "REPO_ROOT", root):
                payload = results._inspect_followstop()

            self.assertEqual(payload["status"], "dry_run_only")
            self.assertEqual(payload["target_run_dir"], str((root / "runs" / "followstop_goal_lateral_enabled_canary").resolve()))
            self.assertEqual(payload["recent_attempts"][0]["doctor_carla_server"]["reachable"], "False")
            self.assertEqual(payload["recent_attempts"][0]["carla_launch_policy"]["extra_args"], "-windowed")

    def test_markdown_includes_next_gate_recent_attempts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            payload = {
                "status": "in_progress",
                "next_key": "followstop_lateral_enabled_canary",
                "runbook_status": "ready",
                "executor_command": "python tools/run_town01_goal_online_runbook.py --python-exec python",
                "next_command": "python run.py",
                "recommended_order": ["followstop_lateral_enabled_canary"],
                "results": {
                    "followstop_lateral_enabled_canary": {
                        "status": "dry_run_only",
                        "reason": "needs online",
                        "path": "runs/check/effective.yaml",
                        "recent_attempts": [
                            {
                                "name": "followstop_goal_lateral_enabled_canary_check",
                                "has_summary": False,
                                "has_effective_config": True,
                                "doctor_carla_server": {
                                    "reachable": "False",
                                    "error": "Operation not permitted",
                                },
                                "carla_launch_policy": {"extra_args": "-windowed"},
                            }
                        ],
                    }
                },
            }
            out = root / "results.md"

            results.write_markdown(out, payload)

            text = out.read_text(encoding="utf-8")
            self.assertIn("Recommended Executor Command", text)
            self.assertIn("Current Gate Underlying Command", text)
            self.assertIn("Next Gate Recent Attempts", text)
            self.assertIn("followstop_goal_lateral_enabled_canary_check", text)
            self.assertIn("Operation not permitted", text)

    def test_build_payload_exposes_refreshing_executor_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "runs" / "goal_smoke_config").mkdir(parents=True)
            (root / "runs" / "goal_smoke_config" / "summary.json").write_text(
                json.dumps({"success": True}),
                encoding="utf-8",
            )

            with mock.patch.object(results, "REPO_ROOT", root):
                payload = results.build_results_payload(python_exec="/env/bin/python")

            self.assertIn("tools/run_town01_goal_online_runbook.py", payload["executor_command"])
            self.assertIn("--python-exec /env/bin/python", payload["executor_command"])

    def test_transport_ab_classifies_positive_root_from_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ab_root = root / "runs" / "town01_transport_ab_20260522_010101"
            self._write_transport_summary(
                ab_root / "baseline" / "r1" / "summary.json",
                route_id="r1",
                distance=10.0,
                speed=3.0,
                transport_mode="ros2_gt",
            )
            self._write_transport_summary(
                ab_root / "candidate" / "r1" / "summary.json",
                route_id="r1",
                distance=18.0,
                speed=3.5,
                transport_mode="carla_direct",
            )
            artifacts = root / "artifacts"
            artifacts.mkdir(parents=True)
            (artifacts / "town01_transport_ab_manifest_20260522_010101.json").write_text(
                json.dumps(
                    {
                        "ab_batch_root": str(ab_root),
                        "baseline_command": ["python", "baseline"],
                        "candidate_command": ["python", "candidate"],
                    }
                ),
                encoding="utf-8",
            )

            with mock.patch.object(results, "REPO_ROOT", root):
                payload = results._inspect_transport_ab()

            self.assertEqual(payload["status"], "candidate_positive")
            self.assertEqual(payload["summary"]["candidate_positive_count"], 1)

    def test_transport_ab_classifies_partial_root_from_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ab_root = root / "runs" / "town01_transport_ab_20260522_010101"
            self._write_transport_summary(
                ab_root / "baseline" / "r1" / "summary.json",
                route_id="r1",
                distance=10.0,
                speed=3.0,
                transport_mode="ros2_gt",
            )
            artifacts = root / "artifacts"
            artifacts.mkdir(parents=True)
            (artifacts / "town01_transport_ab_manifest_20260522_010101.json").write_text(
                json.dumps(
                    {
                        "ab_batch_root": str(ab_root),
                        "baseline_command": ["python", "baseline"],
                        "candidate_command": ["python", "candidate"],
                    }
                ),
                encoding="utf-8",
            )

            with mock.patch.object(results, "REPO_ROOT", root):
                payload = results._inspect_transport_ab()

            self.assertEqual(payload["status"], "partial_pending_rerun")
            self.assertEqual(payload["verdict_counts"]["candidate_missing"], 1)

    def test_transport_ab_dry_run_manifest_does_not_mask_real_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ab_root = root / "runs" / "town01_transport_ab_20260522_010101"
            self._write_transport_summary(
                ab_root / "baseline" / "r1" / "summary.json",
                route_id="r1",
                distance=10.0,
                speed=3.0,
                transport_mode="ros2_gt",
            )
            self._write_transport_summary(
                ab_root / "candidate" / "r1" / "summary.json",
                route_id="r1",
                distance=18.0,
                speed=3.5,
                transport_mode="carla_direct",
            )
            artifacts = root / "artifacts"
            artifacts.mkdir(parents=True)
            (artifacts / "town01_transport_ab_manifest_20260522_010101.json").write_text(
                json.dumps(
                    {
                        "ab_batch_root": str(ab_root),
                        "baseline_command": ["python", "baseline"],
                        "candidate_command": ["python", "candidate"],
                    }
                ),
                encoding="utf-8",
            )
            (artifacts / "town01_transport_ab_manifest_20260522_020202.json").write_text(
                json.dumps(
                    {
                        "ab_batch_root": str(root / "runs" / "town01_transport_ab_dry"),
                        "baseline_command": ["python", "baseline", "--dry-run"],
                        "candidate_command": ["python", "candidate", "--dry-run"],
                    }
                ),
                encoding="utf-8",
            )

            with mock.patch.object(results, "REPO_ROOT", root):
                payload = results._inspect_transport_ab()

            self.assertEqual(payload["status"], "candidate_positive")
            self.assertIn("latest_dry_run_manifest_path", payload)

    def test_demo_recording_reports_readiness_only_without_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            artifacts = root / "artifacts"
            artifacts.mkdir(parents=True)
            (artifacts / "town01_demo_readiness_20260522.json").write_text(
                json.dumps({"status": "ready"}),
                encoding="utf-8",
            )

            with mock.patch.object(results, "REPO_ROOT", root):
                payload = results._inspect_demo_recording()

            self.assertEqual(payload["status"], "readiness_only")
            self.assertEqual(payload["readiness_status"], "ready")

    def test_demo_recording_passes_when_latest_batch_has_carla_and_dreamview(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            batch = root / "runs" / "town01_capability_online_chain_20260522_010101"
            run_dir = batch / "lane097_run"
            artifacts = run_dir / "artifacts"
            raw_tp = run_dir / "video" / "dual_cam" / "raw_tp"
            dreamview_video = run_dir / "video" / "dreamview" / "dreamview_capture.mp4"
            artifacts.mkdir(parents=True)
            raw_tp.mkdir(parents=True)
            dreamview_video.parent.mkdir(parents=True)
            (batch / "artifacts").mkdir(parents=True)
            (batch / "artifacts" / "town01_demo_showcase_manifest.json").write_text(
                json.dumps({"status": "completed"}),
                encoding="utf-8",
            )
            (run_dir / "summary.json").write_text(
                json.dumps({"route_id": "town01_rh_spawn097_goal046"}),
                encoding="utf-8",
            )
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

            with mock.patch.object(results, "REPO_ROOT", root):
                payload = results._inspect_demo_recording()

            self.assertEqual(payload["status"], "passed")
            self.assertEqual(payload["inspection"]["status"], "ready")

    def test_build_results_selects_first_incomplete_key(self) -> None:
        runbook = {
            "status": "ready",
            "recommended_order": ["offline_smoke_config", "followstop_lateral_enabled_canary"],
            "commands": [
                {"key": "offline_smoke_config", "command": ["python", "smoke"]},
                {"key": "followstop_lateral_enabled_canary", "command": ["python", "followstop"]},
            ],
        }
        inspected = {
            "offline_smoke_config": {"status": "passed", "path": "summary.json", "reason": ""},
            "followstop_lateral_enabled_canary": {"status": "missing", "path": "", "reason": "missing"},
            "town01_demo_recording": {"status": "missing", "path": "", "reason": "missing"},
        }

        with (
            mock.patch.object(results, "build_runbook_payload", return_value=runbook),
            mock.patch.object(results, "inspect_results", return_value=inspected),
        ):
            payload = results.build_results_payload(python_exec="python")

        self.assertEqual(payload["status"], "in_progress")
        self.assertEqual(payload["next_key"], "followstop_lateral_enabled_canary")
        self.assertEqual(payload["next_command"], "python followstop")

    def test_build_results_accepts_followstop_canary_evidence(self) -> None:
        runbook = {
            "status": "ready",
            "recommended_order": ["offline_smoke_config", "followstop_lateral_enabled_canary"],
            "commands": [
                {"key": "offline_smoke_config", "command": ["python", "smoke"]},
                {"key": "followstop_lateral_enabled_canary", "command": ["python", "followstop"]},
                {"key": "final_strict_offline_audit", "command": ["python", "audit"]},
            ],
        }
        inspected = {
            "offline_smoke_config": {"status": "passed", "path": "summary.json", "reason": ""},
            "followstop_lateral_enabled_canary": {
                "status": "canary_evidence_present",
                "path": "summary.json",
                "reason": "materialized",
            },
            "post_online_triage": {
                "status": "passed",
                "triage_status": "ready_for_final_strict_audit",
                "path": "triage.json",
                "reason": "",
            },
            "final_strict_offline_audit": {
                "status": "offline_pass_goal_in_progress",
                "path": "audit.json",
                "reason": "",
            },
            "town01_demo_recording": {
                "status": "missing",
                "path": "",
                "reason": "missing",
            },
        }

        with (
            mock.patch.object(results, "build_runbook_payload", return_value=runbook),
            mock.patch.object(results, "inspect_results", return_value=inspected),
        ):
            payload = results.build_results_payload(python_exec="python")

        self.assertEqual(payload["status"], "needs_final_audit")
        self.assertEqual(payload["next_key"], "final_strict_offline_audit")


    def test_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            payload = {
                "status": "in_progress",
                "next_key": "followstop_lateral_enabled_canary",
                "next_command": "python followstop",
                "runbook_status": "ready",
                "recommended_order": ["followstop_lateral_enabled_canary"],
                "results": {
                    "followstop_lateral_enabled_canary": {
                        "status": "missing",
                        "reason": "missing",
                        "path": "",
                    }
                },
            }
            json_out = root / "results.json"
            md_out = root / "results.md"

            results.write_json(json_out, payload)
            results.write_markdown(md_out, payload)

            self.assertEqual(json.loads(json_out.read_text(encoding="utf-8"))["next_key"], "followstop_lateral_enabled_canary")
            self.assertIn("Town01 Goal Online Runbook Results", md_out.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
