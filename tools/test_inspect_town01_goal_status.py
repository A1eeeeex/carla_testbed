from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.inspect_town01_goal_status import (
    build_goal_status,
    build_recommended_commands,
    classify_transport_decision,
    exit_code_for_goal_status,
    find_latest_demo_root,
    inspect_retry_artifacts,
    inspect_startup_only_runs,
    retry_artifact_path,
    select_next_command,
    write_markdown,
)


def _write_summary(root: Path, route_id: str, *, transport: str, distance_m: float) -> Path:
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
        "direct_control_apply_count": 100,
        "direct_control_apply_window_status": "observable_apply_window",
        "direct_control_apply_frame_span": 100,
        "direct_control_apply_max_speed_mps": 6.0,
        "direct_metric_consistency_status": "consistent",
    }
    path = run_dir / "summary.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


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


def _write_demo_run(root: Path) -> None:
    run_dir = root / "lane097_demo"
    artifacts = run_dir / "artifacts"
    raw_tp = run_dir / "video" / "dual_cam" / "raw_tp"
    dreamview_video = run_dir / "video" / "dreamview" / "dreamview_capture.mp4"
    artifacts.mkdir(parents=True, exist_ok=True)
    raw_tp.mkdir(parents=True, exist_ok=True)
    dreamview_video.parent.mkdir(parents=True, exist_ok=True)
    (run_dir / "summary.json").write_text(json.dumps({"route_id": "town01_rh_spawn097_goal046"}), encoding="utf-8")
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


def _write_showcase_manifest(root: Path) -> None:
    artifacts = root / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)
    (artifacts / "town01_demo_showcase_manifest.json").write_text(
        json.dumps({"mode": "short", "status": "completed"}),
        encoding="utf-8",
    )


class InspectTown01GoalStatusTests(unittest.TestCase):
    def test_auto_curve_recovery_prefers_latest_candidate_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ab = root / "ab"
            runs = root / "runs"
            route_ids = ["town01_rh_spawn176_goal061", "town01_rh_spawn177_goal052"]
            for route_id in route_ids:
                _write_summary(ab / "baseline", route_id, transport="ros2_gt", distance_m=50.0)
                _write_summary(ab / "candidate", route_id, transport="carla_direct", distance_m=70.0)
                _write_summary(
                    runs / "town01_capability_online_chain_20260522_180000",
                    route_id,
                    transport="carla_direct",
                    distance_m=75.0,
                )
            _write_curve_assessment(
                ab / "direct_curve_recovery_assessment.json",
                "recovery_incomplete_candidate_missing",
            )

            payload = build_goal_status(
                transport_ab_root=ab,
                runs_root=runs,
                require_demo=False,
            )

            self.assertEqual(payload["overall_status"], "ready_for_final_review")
            self.assertEqual(payload["curve_recovery"]["source"], "auto_latest_candidates")
            self.assertEqual(payload["curve_recovery"]["recovery_verdict"], "recovery_positive")
            self.assertEqual(payload["transport_decision"]["status"], "candidate_positive")
            self.assertTrue(payload["transport_decision"]["transport_promotable"])
            self.assertIn("direct_curve_recovery_online", payload["recommended_commands"])
            self.assertIn("direct_curve_pair_recovery_online", payload["recommended_commands"])
            self.assertIn("/home/ubuntu/miniconda3/envs/carla16/bin/python3", payload["recommended_commands"]["direct_curve_recovery_online"])
            self.assertIn(
                "tools/run_town01_direct_curve_pair_recovery_retry.py",
                payload["recommended_commands"]["direct_curve_pair_recovery_online"],
            )
            self.assertIn("direct_curve176_recovery_online", payload["recommended_commands"])
            self.assertIn("direct_curve177_recovery_online", payload["recommended_commands"])
            self.assertIn(
                "tools/run_town01_direct_curve_recovery_retry.py",
                payload["recommended_commands"]["direct_curve176_recovery_online"],
            )
            self.assertIn("goal_resume", payload["recommended_commands"])
            self.assertIn("online_preflight", payload["recommended_commands"])
            self.assertIn("final_strict_offline_audit", payload["recommended_commands"])
            self.assertIn("demo_recording_readiness", payload["recommended_commands"])
            self.assertIn(
                "tools/inspect_town01_demo_readiness.py",
                payload["recommended_commands"]["demo_recording_readiness"],
            )
            self.assertIn("demo_recording_online", payload["recommended_commands"])
            self.assertIn("--require-recording-ready", payload["recommended_commands"]["demo_recording_online"])
            self.assertIn("--dreamview-browser-cmd auto", payload["recommended_commands"]["demo_recording_online"])
            self.assertIn("--dreamview-open-wait-page", payload["recommended_commands"]["demo_recording_online"])
            self.assertIn("goal_sequence_online", payload["recommended_commands"])
            self.assertIn("tools/run_town01_goal_sequence.py", payload["recommended_commands"]["goal_sequence_online"])

    def test_goal_status_stays_in_progress_when_curve_candidate_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ab = root / "ab"
            _write_summary(ab / "baseline", "town01_rh_spawn176_goal061", transport="ros2_gt", distance_m=50.0)
            _write_summary(ab / "candidate", "town01_rh_spawn176_goal061", transport="carla_direct", distance_m=70.0)
            _write_curve_assessment(ab / "direct_curve_recovery_assessment.json", "recovery_incomplete_candidate_missing")

            payload = build_goal_status(
                transport_ab_root=ab,
                runs_root=root / "runs",
                require_demo=False,
            )

            self.assertEqual(payload["overall_status"], "in_progress")
            self.assertIn("curve_recovery:pending_missing_candidate_curve", payload["blockers"])
            self.assertEqual(payload["transport_decision"]["status"], "pending_curve_recovery")
            self.assertEqual(payload["next_command"]["key"], "direct_curve176_recovery_online")
            self.assertIn("direct_curve_retry", payload)
            self.assertEqual(exit_code_for_goal_status(payload, require_ready=True), 2)

    def test_retry_artifacts_report_dry_run_separately_from_online(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            route_id = "town01_rh_spawn176_goal061"
            dry_path = retry_artifact_path(route_id, dry_run=True, artifacts_root=root)
            dry_path.parent.mkdir(parents=True, exist_ok=True)
            dry_path.write_text(
                json.dumps(
                    {
                        "status": "dry_run",
                        "route_id": route_id,
                        "final_verdict": "not_run",
                        "attempts": [{"attempt": 1, "verdict": "not_run"}],
                    }
                ),
                encoding="utf-8",
            )

            payload = inspect_retry_artifacts(route_ids=[route_id], artifacts_root=root)

            self.assertEqual(payload["status"], "dry_run_only")
            self.assertEqual(payload["routes"][0]["online"]["status"], "missing")
            self.assertEqual(payload["routes"][0]["dry_run"]["status"], "dry_run_only")

    def test_retry_artifacts_ignore_legacy_dry_run_in_online_slot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            route_id = "town01_rh_spawn176_goal061"
            online_path = retry_artifact_path(route_id, dry_run=False, artifacts_root=root)
            online_path.parent.mkdir(parents=True, exist_ok=True)
            online_path.write_text(
                json.dumps(
                    {
                        "status": "dry_run",
                        "route_id": route_id,
                        "final_verdict": "not_run",
                        "attempts": [{"attempt": 1, "verdict": "not_run"}],
                    }
                ),
                encoding="utf-8",
            )

            payload = inspect_retry_artifacts(route_ids=[route_id], artifacts_root=root)

            self.assertEqual(payload["status"], "missing")
            online = payload["routes"][0]["online"]
            self.assertEqual(online["status"], "legacy_dry_run_ignored")
            self.assertEqual(online["attempt_count"], 0)
            self.assertEqual(online["final_verdict"], "")
            self.assertEqual(
                online["last_attempt"]["candidate_invalid_reason"],
                "legacy dry-run artifact in online slot ignored",
            )

    def test_retry_artifacts_report_retry_exhausted_online_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            route_id = "town01_rh_spawn176_goal061"
            online_path = retry_artifact_path(route_id, artifacts_root=root)
            online_path.parent.mkdir(parents=True, exist_ok=True)
            online_path.write_text(
                json.dumps(
                    {
                        "status": "retryable_verdict_exhausted",
                        "route_id": route_id,
                        "final_verdict": "candidate_stale_summary",
                        "attempts": [
                            {
                                "attempt": 1,
                                "verdict": "candidate_stale_summary",
                                "candidate_summary_new_for_attempt": False,
                                "candidate_invalid_reason": "latest candidate summary predates this attempt",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            payload = inspect_retry_artifacts(route_ids=[route_id], artifacts_root=root)

            self.assertEqual(payload["status"], "has_failed_attempt")
            online = payload["routes"][0]["online"]
            self.assertEqual(online["status"], "retry_exhausted")
            self.assertEqual(online["final_verdict"], "candidate_stale_summary")
            self.assertFalse(online["last_attempt"]["candidate_summary_new_for_attempt"])

    def test_goal_status_ready_when_ab_curve_and_demo_are_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ab = root / "ab"
            demo = root / "demo"
            for route_id in ["route_a", "route_b"]:
                _write_summary(ab / "baseline", route_id, transport="ros2_gt", distance_m=50.0)
                _write_summary(ab / "candidate", route_id, transport="carla_direct", distance_m=70.0)
            _write_curve_assessment(ab / "direct_curve_recovery_assessment.json", "recovery_positive")
            _write_demo_run(demo)

            payload = build_goal_status(
                transport_ab_root=ab,
                runs_root=root / "runs",
                demo_root=demo,
                require_demo=True,
                require_dreamview=True,
            )

            self.assertEqual(payload["overall_status"], "ready_for_final_review")
            self.assertEqual(payload["blockers"], [])
            self.assertEqual(payload["demo_recording"]["status"], "ready")
            self.assertEqual(payload["transport_decision"]["status"], "candidate_positive")
            self.assertEqual(payload["next_command"]["key"], "final_strict_offline_audit")
            self.assertEqual(exit_code_for_goal_status(payload, require_ready=True), 0)

    def test_selects_curve177_after_curve176_is_recovered(self) -> None:
        payload = {
            "overall_status": "in_progress",
            "recommended_commands": build_recommended_commands(),
            "transport_ab": {"status": "partial_pending_rerun"},
            "demo_recording": {"required": True, "status": "not_checked"},
            "curve_recovery": {
                "status": "pending_missing_candidate_curve",
                "rows": [
                    {
                        "route_id": "town01_rh_spawn176_goal061",
                        "verdict": "candidate_positive_distance",
                    },
                    {
                        "route_id": "town01_rh_spawn177_goal052",
                        "verdict": "candidate_missing",
                    },
                ],
            },
        }

        command = select_next_command(payload)

        self.assertEqual(command["key"], "direct_curve177_recovery_online")
        self.assertIn("town01_rh_spawn177_goal052", command["command"])
        self.assertIn("tools/run_town01_direct_curve_recovery_retry.py", command["command"])

    def test_selects_strict_demo_recording_command_after_curve_gate(self) -> None:
        payload = {
            "overall_status": "in_progress",
            "recommended_commands": build_recommended_commands(),
            "transport_ab": {"status": "candidate_valid_mixed"},
            "demo_recording": {"required": True, "status": "not_checked"},
            "curve_recovery": {"status": "decisive", "recovery_verdict": "recovery_positive", "rows": []},
        }

        command = select_next_command(payload)

        self.assertEqual(command["key"], "demo_recording_online")
        self.assertIn("--require-recording-ready", command["command"])

    def test_transport_decision_marks_negative_curve_recovery(self) -> None:
        payload = classify_transport_decision(
            {
                "status": "candidate_valid_mixed",
                "summary": {
                    "route_count": 2,
                    "candidate_positive_count": 1,
                    "candidate_negative_count": 0,
                },
            },
            {
                "status": "decisive",
                "recovery_verdict": "recovery_negative",
                "summary": {
                    "route_count": 2,
                    "candidate_positive_count": 0,
                    "candidate_negative_count": 1,
                },
            },
        )

        self.assertEqual(payload["status"], "candidate_negative")
        self.assertFalse(payload["transport_promotable"])

    def test_auto_demo_root_uses_latest_showcase_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ab = root / "ab"
            runs = root / "runs"
            demo = runs / "town01_capability_online_chain_20260522_190000"
            for route_id in ["route_a", "route_b"]:
                _write_summary(ab / "baseline", route_id, transport="ros2_gt", distance_m=50.0)
                _write_summary(ab / "candidate", route_id, transport="carla_direct", distance_m=70.0)
            _write_curve_assessment(ab / "direct_curve_recovery_assessment.json", "recovery_positive")
            _write_demo_run(demo)
            _write_showcase_manifest(demo)

            found = find_latest_demo_root(runs)
            payload = build_goal_status(
                transport_ab_root=ab,
                runs_root=runs,
                demo_root=None,
                require_demo=True,
                require_dreamview=True,
            )

            self.assertEqual(found, demo.resolve())
            self.assertEqual(payload["overall_status"], "ready_for_final_review")
            self.assertEqual(payload["demo_recording"]["source"], "auto_latest_demo")
            self.assertEqual(payload["demo_recording"]["status"], "ready")

    def test_detects_startup_only_runs_without_capability_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            runs = Path(tmpdir) / "runs"
            startup = runs / "town01_capability_online_chain_20260522_172229"
            probe = startup / "_carla_prewarm" / "carla_boot" / "carla_startup_probe.json"
            probe.parent.mkdir(parents=True, exist_ok=True)
            probe.write_text("{}", encoding="utf-8")

            payload = inspect_startup_only_runs(runs)

            self.assertEqual(payload["status"], "found")
            self.assertEqual(payload["count"], 1)
            self.assertEqual(payload["runs"][0]["status"], "startup_only_no_route_summaries")

    def test_writes_markdown(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ab = root / "ab"
            _write_summary(ab / "baseline", "route_a", transport="ros2_gt", distance_m=50.0)
            _write_summary(ab / "candidate", "route_a", transport="carla_direct", distance_m=70.0)
            _write_curve_assessment(ab / "direct_curve_recovery_assessment.json", "recovery_positive")
            payload = build_goal_status(transport_ab_root=ab, runs_root=root / "runs", require_demo=False)
            md_out = root / "goal.md"

            write_markdown(md_out, payload)

            text = md_out.read_text(encoding="utf-8")
            self.assertIn("Town01 Goal Status", text)
            self.assertIn("Next Command", text)
            self.assertIn("Recommended Commands", text)


if __name__ == "__main__":
    unittest.main()
