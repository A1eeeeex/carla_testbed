from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, Sequence

from tools.run_town01_direct_curve_pair_recovery_retry import (
    build_single_route_command,
    default_output_paths,
    exit_code_for_payload,
    ordered_route_plan,
    route_id_from_next_command,
    run_pair_recovery,
    write_json,
    write_markdown,
)


def _goal(route_id: str, *, key: str = "direct_curve176_recovery_online") -> Dict[str, Any]:
    command = ""
    if route_id:
        command = f"/env/bin/python3 tools/run_town01_direct_curve_recovery_retry.py --route-id {route_id} --retries 1"
    return {
        "overall_status": "in_progress",
        "blockers": ["curve_recovery:pending_missing_candidate_curve"],
        "next_command": {
            "key": key if route_id else "final_strict_offline_audit",
            "reason": "curve",
            "command": command,
        },
        "curve_recovery": {
            "status": "pending_missing_candidate_curve" if route_id else "decisive",
            "recovery_verdict": "recovery_incomplete_candidate_missing" if route_id else "recovery_positive",
        },
    }


class RunTown01DirectCurvePairRecoveryRetryTests(unittest.TestCase):
    def test_route_id_from_next_command_handles_multiline_command(self) -> None:
        command = {
            "command": "\n".join(
                [
                    "/env/bin/python3 tools/run_town01_direct_curve_recovery_retry.py \\",
                    "  --route-id town01_rh_spawn176_goal061 \\",
                    "  --retries 1",
                ]
            )
        }

        self.assertEqual(route_id_from_next_command(command), "town01_rh_spawn176_goal061")

    def test_build_single_route_command_wraps_existing_retry_tool(self) -> None:
        command = build_single_route_command(
            python_exec="/env/bin/python3",
            route_id="town01_rh_spawn176_goal061",
            retries=2,
            dry_run=True,
            preflight=False,
            refresh_preflight_after_run=False,
        )

        self.assertEqual(command[0], "/env/bin/python3")
        self.assertIn("tools/run_town01_direct_curve_recovery_retry.py", command)
        self.assertIn("--route-id", command)
        self.assertIn("town01_rh_spawn176_goal061", command)
        self.assertIn("--no-preflight", command)
        self.assertIn("--no-refresh-preflight-after-run", command)
        self.assertIn("--dry-run", command)

    def test_default_outputs_separate_dry_run_from_online(self) -> None:
        online_json, online_md = default_output_paths(dry_run=False)
        dry_json, dry_md = default_output_paths(dry_run=True)

        self.assertNotEqual(online_json, dry_json)
        self.assertNotEqual(online_md, dry_md)
        self.assertNotIn("dry_run", online_json.name)
        self.assertIn("dry_run", dry_json.name)

    def test_ordered_route_plan_starts_with_goal_selected_route(self) -> None:
        plan = ordered_route_plan(
            _goal("town01_rh_spawn177_goal052", key="direct_curve177_recovery_online"),
            ["town01_rh_spawn176_goal061", "town01_rh_spawn177_goal052"],
        )

        self.assertEqual(plan[0], "town01_rh_spawn177_goal052")
        self.assertEqual(plan[1], "town01_rh_spawn176_goal061")

    def test_dry_run_plans_without_executing_commands(self) -> None:
        called = False

        def runner(cmd: Sequence[str]) -> int:
            nonlocal called
            called = True
            return 0

        payload = run_pair_recovery(
            route_ids=["town01_rh_spawn176_goal061", "town01_rh_spawn177_goal052"],
            python_exec="/env/bin/python3",
            dry_run=True,
            preflight=False,
            goal_builder=lambda: _goal("town01_rh_spawn176_goal061"),
            command_runner=runner,
        )

        self.assertFalse(called)
        self.assertEqual(payload["status"], "dry_run")
        self.assertEqual(payload["attempts"][0]["route_id"], "town01_rh_spawn176_goal061")
        self.assertIn("--dry-run", payload["attempts"][0]["command"])
        self.assertEqual(exit_code_for_payload(payload), 0)

    def test_executes_curve176_then_curve177_when_goal_advances(self) -> None:
        goals = [
            _goal("town01_rh_spawn176_goal061"),
            _goal("town01_rh_spawn177_goal052", key="direct_curve177_recovery_online"),
            _goal(""),
        ]
        commands: list[str] = []

        def goal_builder() -> Dict[str, Any]:
            return goals.pop(0)

        def runner(cmd: Sequence[str]) -> int:
            commands.append(" ".join(cmd))
            return 0

        payload = run_pair_recovery(
            route_ids=["town01_rh_spawn176_goal061", "town01_rh_spawn177_goal052"],
            python_exec="/env/bin/python3",
            dry_run=False,
            preflight=False,
            goal_builder=goal_builder,
            command_runner=runner,
        )

        self.assertEqual(payload["status"], "completed_gate_transition")
        self.assertEqual(len(commands), 2)
        self.assertIn("town01_rh_spawn176_goal061", commands[0])
        self.assertIn("town01_rh_spawn177_goal052", commands[1])
        self.assertEqual(payload["final_goal"]["curve_status"], "decisive")

    def test_stops_when_same_route_still_selected_after_failed_retry(self) -> None:
        goals = [
            _goal("town01_rh_spawn176_goal061"),
            _goal("town01_rh_spawn176_goal061"),
        ]

        payload = run_pair_recovery(
            route_ids=["town01_rh_spawn176_goal061", "town01_rh_spawn177_goal052"],
            python_exec="/env/bin/python3",
            dry_run=False,
            preflight=False,
            goal_builder=lambda: goals.pop(0),
            command_runner=lambda cmd: 2,
        )

        self.assertEqual(payload["status"], "stopped_after_retry_failure")
        self.assertEqual(payload["final_goal"]["next_route_id"], "town01_rh_spawn176_goal061")
        self.assertEqual(exit_code_for_payload(payload), 2)

    def test_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            payload = run_pair_recovery(
                route_ids=["town01_rh_spawn176_goal061"],
                python_exec="/env/bin/python3",
                dry_run=True,
                preflight=False,
                goal_builder=lambda: _goal("town01_rh_spawn176_goal061"),
                command_runner=lambda cmd: 0,
            )
            json_out = root / "pair.json"
            md_out = root / "pair.md"

            write_json(json_out, payload)
            write_markdown(md_out, payload)

            self.assertEqual(json.loads(json_out.read_text(encoding="utf-8"))["status"], "dry_run")
            self.assertIn("Town01 Direct Curve Pair Recovery Retry", md_out.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
