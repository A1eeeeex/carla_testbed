from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.inspect_town01_goal_online_runbook_execution import (
    DEFAULT_DRY_RUN_EXECUTION_JSON,
    DEFAULT_EXECUTION_JSON,
    DEFAULT_OUTPUT_JSON,
    classify_execution,
    inspect_execution,
    write_json,
    write_markdown,
)


def _payload(
    *,
    status: str = "completed_one_step",
    initial_next_key: str = "followstop_lateral_enabled_canary",
    final_next_key: str = "town01_ros2_gt_canary",
    step_status: str = "passed",
    returncode: int | None = 0,
    dry_run: bool = False,
) -> dict:
    return {
        "execution_id": "exec-1",
        "status": status,
        "reason": "reason",
        "dry_run": dry_run,
        "started_at_s": 10.0,
        "finished_at_s": 12.5,
        "duration_s": 2.5,
        "initial_next_key": initial_next_key,
        "final_next_key": final_next_key,
        "initial_results_snapshot": {
            "status": "in_progress",
            "next_key": initial_next_key,
            "next_command": "python initial.py",
            "results": {
                initial_next_key: {
                    "status": "missing",
                    "reason": "before",
                }
            },
        },
        "final_results_snapshot": {
            "status": "in_progress",
            "next_key": final_next_key,
            "next_command": "python next.py",
            "results": {
                initial_next_key: {
                    "status": "passed",
                    "reason": "after",
                },
                final_next_key: {
                    "status": "missing",
                    "reason": "next missing",
                },
            },
        },
        "steps": [
            {
                "key": initial_next_key,
                "status": step_status,
                "returncode": returncode,
                "status_before": "missing",
                "status_after": "passed",
                "next_key_after": final_next_key,
                "evidence_progress": step_status == "evidence_progress_returncode_nonzero",
            }
        ],
    }


def _payload_with_post_run_refresh() -> dict:
    payload = _payload()
    payload["post_run_refresh"] = {
        "results": {
            "status": "in_progress",
            "next_key": "town01_ros2_gt_canary",
            "json": "artifacts/results.json",
            "md": "artifacts/results.md",
        },
        "next_action": {
            "status": "ready",
            "primary_key": "town01_ros2_gt_canary",
            "json": "artifacts/next.json",
            "md": "artifacts/next.md",
        },
        "handoff": {
            "status": "ready_to_run_next_online",
            "selected_next_key": "town01_ros2_gt_canary",
            "json": "artifacts/handoff.json",
            "md": "artifacts/handoff.md",
        },
        "offline_audit": {
            "status": "offline_pass_goal_in_progress",
            "json": "artifacts/audit.json",
            "md": "artifacts/audit.md",
        },
        "after_online_intake": {
            "status": "goal_in_progress",
            "json": "artifacts/after_online.json",
            "md": "artifacts/after_online.md",
            "next_command": "python next.py",
        },
    }
    return payload


class InspectTown01GoalOnlineRunbookExecutionTests(unittest.TestCase):
    def test_default_input_and_output_paths_are_distinct(self) -> None:
        self.assertNotEqual(DEFAULT_EXECUTION_JSON, DEFAULT_OUTPUT_JSON)
        self.assertIn("execution_20260522.json", str(DEFAULT_EXECUTION_JSON))
        self.assertIn("execution_dry_run_20260522.json", str(DEFAULT_DRY_RUN_EXECUTION_JSON))
        self.assertIn("execution_inspection_20260522.json", str(DEFAULT_OUTPUT_JSON))

    def test_classifies_advanced_to_next_gate(self) -> None:
        result = classify_execution(_payload())

        self.assertEqual(result["status"], "advanced_to_next_gate")
        self.assertEqual(result["execution_id"], "exec-1")
        self.assertEqual(result["duration_s"], 2.5)
        self.assertEqual(result["next_action"], "run_or_inspect_next_gate")
        self.assertEqual(result["result_deltas"][0]["key"], "followstop_lateral_enabled_canary")

    def test_classifies_nonzero_with_evidence_progress(self) -> None:
        result = classify_execution(
            _payload(
                step_status="evidence_progress_returncode_nonzero",
                returncode=7,
            )
        )

        self.assertEqual(result["status"], "advanced_to_next_gate")
        self.assertEqual(result["failed_steps"], [])
        self.assertEqual(result["evidence_progress_count"], 1)

    def test_classifies_failure_without_progress(self) -> None:
        result = classify_execution(
            _payload(
                status="stopped_after_failure",
                initial_next_key="followstop_lateral_enabled_canary",
                final_next_key="followstop_lateral_enabled_canary",
                step_status="failed",
                returncode=7,
            )
        )

        self.assertEqual(result["status"], "failed_without_evidence_progress")
        self.assertEqual(result["next_action"], "inspect_failed_step_artifacts_before_retry")
        self.assertEqual(len(result["failed_steps"]), 1)

    def test_classifies_dry_run_only(self) -> None:
        result = classify_execution(
            _payload(
                status="dry_run",
                final_next_key="followstop_lateral_enabled_canary",
                step_status="planned",
                returncode=None,
                dry_run=True,
            )
        )

        self.assertEqual(result["status"], "dry_run_only")
        self.assertEqual(result["next_action"], "run_online_executor_without_dry_run")

    def test_classifies_post_run_refresh_summary(self) -> None:
        result = classify_execution(_payload_with_post_run_refresh())

        refresh = result["post_run_refresh"]
        self.assertTrue(refresh["present"])
        self.assertEqual(refresh["results_next_key"], "town01_ros2_gt_canary")
        self.assertEqual(refresh["next_action_primary_key"], "town01_ros2_gt_canary")
        self.assertEqual(refresh["handoff_selected_next_key"], "town01_ros2_gt_canary")
        self.assertEqual(refresh["offline_audit_status"], "offline_pass_goal_in_progress")
        self.assertEqual(refresh["after_online_status"], "goal_in_progress")
        self.assertEqual(refresh["after_online_next_command"], "python next.py")

    def test_inspects_file_and_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            execution = root / "execution.json"
            json_out = root / "inspection.json"
            md_out = root / "inspection.md"
            execution.write_text(json.dumps(_payload_with_post_run_refresh()), encoding="utf-8")

            result = inspect_execution(execution)
            write_json(json_out, result)
            write_markdown(md_out, result)

            self.assertEqual(json.loads(json_out.read_text(encoding="utf-8"))["status"], "advanced_to_next_gate")
            text = md_out.read_text(encoding="utf-8")
            self.assertIn("Town01 Goal Online Runbook Execution Inspection", text)
            self.assertIn("execution_id", text)
            self.assertIn("exec-1", text)
            self.assertIn("Post-Run Refresh", text)
            self.assertIn("town01_ros2_gt_canary", text)
            self.assertIn("Next Command From Final Snapshot", text)


if __name__ == "__main__":
    unittest.main()
