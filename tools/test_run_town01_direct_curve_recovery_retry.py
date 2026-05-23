from __future__ import annotations

import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any, Dict, Sequence

from tools.run_town01_direct_curve_recovery_retry import (
    build_chain_command,
    default_output_paths,
    exit_code_for_payload,
    mark_stale_summary_if_needed,
    refresh_goal_artifacts,
    run_retry,
    should_refresh_after_run,
    should_retry_verdict,
    write_json,
    write_markdown,
)


class RunTown01DirectCurveRecoveryRetryTests(unittest.TestCase):
    def test_build_chain_command_uses_direct_candidate_flags(self) -> None:
        command = build_chain_command(
            python_exec="/env/bin/python3",
            route_id="town01_rh_spawn176_goal061",
            config_path=Path("configs/io/examples/town01_apollo_route_health_behavior_recovery_stitcher_v1_direct_candidate.yaml"),
        )

        self.assertEqual(command[0], "/env/bin/python3")
        self.assertIn("tools/run_town01_capability_online_chain.py", command)
        self.assertIn("--enable-lateral", command)
        self.assertIn("--enable-guard", command)
        self.assertIn("--startup-profile", command)
        self.assertIn("render_offscreen_no_ros2", command)
        self.assertIn("--carla-ignore-memory-preflight", command)
        self.assertIn("curve_lane_follow:town01_rh_spawn176_goal061", command)

    def test_should_retry_only_runtime_interrupted_or_missing(self) -> None:
        self.assertTrue(should_retry_verdict("candidate_missing"))
        self.assertTrue(should_retry_verdict("candidate_inconclusive_runtime_interrupted"))
        self.assertTrue(should_retry_verdict("candidate_stale_summary"))
        self.assertFalse(should_retry_verdict("candidate_positive_distance"))
        self.assertFalse(should_retry_verdict("candidate_negative_distance"))

    def test_default_outputs_are_route_specific(self) -> None:
        json_out, md_out = default_output_paths("town01_rh_spawn176_goal061")

        self.assertIn("town01_rh_spawn176_goal061", json_out.name)
        self.assertIn("town01_rh_spawn176_goal061", md_out.name)
        self.assertNotIn("dry_run", json_out.name)
        self.assertEqual(json_out.suffix, ".json")
        self.assertEqual(md_out.suffix, ".md")

    def test_default_dry_run_outputs_do_not_overlap_online_outputs(self) -> None:
        online_json, online_md = default_output_paths("town01_rh_spawn176_goal061")
        dry_json, dry_md = default_output_paths("town01_rh_spawn176_goal061", dry_run=True)

        self.assertNotEqual(online_json, dry_json)
        self.assertNotEqual(online_md, dry_md)
        self.assertIn("dry_run", dry_json.name)
        self.assertIn("dry_run", dry_md.name)

    def test_refresh_after_run_is_skipped_for_dry_run(self) -> None:
        self.assertFalse(should_refresh_after_run(dry_run=True, refresh_goal_after_run=True))
        self.assertFalse(should_refresh_after_run(dry_run=False, refresh_goal_after_run=False))
        self.assertTrue(should_refresh_after_run(dry_run=False, refresh_goal_after_run=True))

    def test_marks_summary_stale_when_it_predates_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            summary = Path(tmpdir) / "summary.json"
            summary.write_text("{}", encoding="utf-8")
            old_mtime = time.time() - 30.0
            os.utime(summary, (old_mtime, old_mtime))

            row = mark_stale_summary_if_needed(
                {
                    "route_id": "town01_rh_spawn176_goal061",
                    "verdict": "candidate_positive_distance",
                    "candidate_summary": str(summary),
                },
                attempt_started_at_s=old_mtime + 20.0,
                tolerance_s=0.0,
            )

            self.assertEqual(row["verdict"], "candidate_stale_summary")
            self.assertEqual(row["stale_previous_verdict"], "candidate_positive_distance")
            self.assertFalse(row["candidate_summary_new_for_attempt"])

    def test_dry_run_does_not_call_command_runner(self) -> None:
        called = False

        def runner(cmd: Sequence[str]) -> int:
            nonlocal called
            called = True
            return 0

        payload = run_retry(
            route_id="town01_rh_spawn176_goal061",
            retries=1,
            python_exec="/env/bin/python3",
            config_path=Path("candidate.yaml"),
            baseline_root=Path("baseline"),
            runs_root=Path("runs"),
            label_hint="label",
            ticks=420,
            post_fail_steps=120,
            preflight=False,
            dry_run=True,
            command_runner=runner,
        )

        self.assertFalse(called)
        self.assertEqual(payload["status"], "dry_run")
        self.assertEqual(payload["final_verdict"], "not_run")
        self.assertEqual(exit_code_for_payload(payload), 0)

    def test_retries_once_after_runtime_interruption_then_completes(self) -> None:
        calls: list[str] = []
        analyzer_verdicts = [
            "candidate_inconclusive_runtime_interrupted",
            "candidate_positive_distance",
        ]

        def runner(cmd: Sequence[str]) -> int:
            calls.append(" ".join(cmd))
            return 0

        def analyzer(baseline_root: Path, runs_root: Path, route_id: str, label_hint: str) -> Dict[str, Any]:
            verdict = analyzer_verdicts.pop(0)
            return {
                "route_id": route_id,
                "verdict": verdict,
                "candidate_invalid_reason": "simulator_world_tick_timeout" if "interrupted" in verdict else "",
                "candidate_summary": f"{route_id}/summary.json",
                "candidate_roots": [str(runs_root / "latest")],
            }

        payload = run_retry(
            route_id="town01_rh_spawn176_goal061",
            retries=1,
            python_exec="/env/bin/python3",
            config_path=Path("candidate.yaml"),
            baseline_root=Path("baseline"),
            runs_root=Path("runs"),
            label_hint="label",
            ticks=420,
            post_fail_steps=120,
            preflight=False,
            dry_run=False,
            retry_delay_s=0.0,
            command_runner=runner,
            route_analyzer=analyzer,
        )

        self.assertEqual(len(calls), 2)
        self.assertEqual(payload["status"], "completed")
        self.assertEqual(payload["final_verdict"], "candidate_positive_distance")
        self.assertTrue(payload["attempts"][0]["retrying"])
        self.assertFalse(payload["attempts"][1]["retrying"])

    def test_stale_positive_summary_does_not_complete_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            summary = Path(tmpdir) / "summary.json"
            summary.write_text("{}", encoding="utf-8")
            old_mtime = time.time() - 30.0
            os.utime(summary, (old_mtime, old_mtime))

            def runner(cmd: Sequence[str]) -> int:
                return 0

            def analyzer(baseline_root: Path, runs_root: Path, route_id: str, label_hint: str) -> Dict[str, Any]:
                return {
                    "route_id": route_id,
                    "verdict": "candidate_positive_distance",
                    "candidate_invalid_reason": "",
                    "candidate_summary": str(summary),
                    "candidate_roots": [str(runs_root / "old")],
                }

            payload = run_retry(
                route_id="town01_rh_spawn176_goal061",
                retries=0,
                python_exec="/env/bin/python3",
                config_path=Path("candidate.yaml"),
                baseline_root=Path("baseline"),
                runs_root=Path("runs"),
                label_hint="label",
                ticks=420,
                post_fail_steps=120,
                preflight=False,
                dry_run=False,
                retry_delay_s=0.0,
                command_runner=runner,
                route_analyzer=analyzer,
            )

            self.assertEqual(payload["final_verdict"], "candidate_stale_summary")
            self.assertEqual(payload["status"], "retryable_verdict_exhausted")
            self.assertEqual(payload["attempts"][0]["stale_previous_verdict"], "candidate_positive_distance")
            self.assertEqual(exit_code_for_payload(payload), 2)

    def test_retryable_verdict_exhausted_exits_nonzero(self) -> None:
        def runner(cmd: Sequence[str]) -> int:
            return 0

        def analyzer(baseline_root: Path, runs_root: Path, route_id: str, label_hint: str) -> Dict[str, Any]:
            return {
                "route_id": route_id,
                "verdict": "candidate_inconclusive_runtime_interrupted",
                "candidate_invalid_reason": "simulator_world_tick_timeout",
                "candidate_summary": "",
                "candidate_roots": [],
            }

        payload = run_retry(
            route_id="town01_rh_spawn176_goal061",
            retries=1,
            python_exec="/env/bin/python3",
            config_path=Path("candidate.yaml"),
            baseline_root=Path("baseline"),
            runs_root=Path("runs"),
            label_hint="label",
            ticks=420,
            post_fail_steps=120,
            preflight=False,
            dry_run=False,
            retry_delay_s=0.0,
            command_runner=runner,
            route_analyzer=analyzer,
        )

        self.assertEqual(len(payload["attempts"]), 2)
        self.assertEqual(payload["status"], "retryable_verdict_exhausted")
        self.assertEqual(exit_code_for_payload(payload), 2)

    def test_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            payload = {
                "status": "dry_run",
                "route_id": "town01_rh_spawn176_goal061",
                "final_verdict": "not_run",
                "preflight": {},
                "attempts": [
                    {
                        "attempt": 1,
                        "returncode": None,
                        "verdict": "not_run",
                        "candidate_invalid_reason": "",
                        "retrying": False,
                    }
                ],
            }
            json_out = root / "retry.json"
            md_out = root / "retry.md"

            write_json(json_out, payload)
            write_markdown(md_out, payload)

            self.assertEqual(json.loads(json_out.read_text(encoding="utf-8"))["status"], "dry_run")
            self.assertIn("Town01 Direct Curve Recovery Retry", md_out.read_text(encoding="utf-8"))

    def test_refresh_goal_artifacts_writes_goal_and_resume(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)

            def goal_builder() -> Dict[str, Any]:
                return {
                    "overall_status": "in_progress",
                    "blockers": ["curve"],
                    "transport_ab": {"status": "partial_pending_rerun"},
                    "transport_decision": {"status": "pending_curve_recovery"},
                    "curve_recovery": {
                        "status": "pending_missing_candidate_curve",
                        "recovery_verdict": "recovery_incomplete_candidate_missing",
                        "summary": {"verdict_counts": {"candidate_missing": 1}},
                    },
                    "direct_curve_retry": {"status": "missing", "routes": []},
                    "demo_recording": {"status": "not_checked", "required": True},
                    "startup_only_runs": {"status": "none", "count": 0},
                    "next_actions": [],
                    "next_command": {"key": "direct_curve176_recovery_online", "reason": "curve", "command": "python run.py"},
                    "recommended_commands": {},
                }

            def resume_builder(*, refresh_preflight: bool = True) -> Dict[str, Any]:
                return {
                    "resume_state": {"status": "awaiting_direct_curve_online", "reason": "curve"},
                    "goal_status": {
                        "overall_status": "in_progress",
                        "blockers": ["curve"],
                        "transport_decision": {},
                        "transport_ab": {},
                        "curve_recovery": {},
                        "direct_curve_retry": {},
                        "demo_recording": {},
                        "next_command": {},
                    },
                    "preflight": {"status": "ready", "failed_count": 0, "warning_count": 0},
                    "commands": {},
                    "artifacts": {},
                }

            payload = refresh_goal_artifacts(
                goal_json=root / "goal.json",
                goal_md=root / "goal.md",
                resume_json=root / "resume.json",
                resume_md=root / "resume.md",
                refresh_preflight=False,
                goal_builder=goal_builder,
                resume_builder=resume_builder,
            )

            self.assertEqual(payload["status"], "refreshed")
            self.assertEqual(payload["goal_status"], "in_progress")
            self.assertEqual(payload["resume_status"], "awaiting_direct_curve_online")
            self.assertTrue((root / "goal.json").exists())
            self.assertTrue((root / "resume.md").exists())


if __name__ == "__main__":
    unittest.main()
