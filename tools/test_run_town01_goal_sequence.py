from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, Sequence

from tools.run_town01_goal_sequence import (
    build_demo_recording_command,
    build_offline_audit_command,
    build_pair_gate_command,
    default_output_paths,
    exit_code_for_payload,
    run_sequence,
    write_json,
    write_markdown,
)


def _goal(
    *,
    curve_status: str = "pending_missing_candidate_curve",
    demo_status: str = "not_checked",
    overall: str = "in_progress",
) -> Dict[str, Any]:
    next_key = "direct_curve176_recovery_online" if curve_status != "decisive" else "demo_recording_online"
    command = (
        "/env/bin/python3 tools/run_town01_direct_curve_recovery_retry.py --route-id town01_rh_spawn176_goal061"
        if curve_status != "decisive"
        else "/env/bin/python3 tools/run_town01_demo_showcase.py"
    )
    blockers = []
    if curve_status != "decisive":
        blockers.append(f"curve_recovery:{curve_status}")
    if demo_status != "ready":
        blockers.append(f"demo_recording:{demo_status}")
    return {
        "overall_status": overall,
        "blockers": blockers,
        "next_command": {"key": next_key, "reason": "test", "command": command},
        "curve_recovery": {
            "status": curve_status,
            "recovery_verdict": "recovery_positive" if curve_status == "decisive" else "recovery_incomplete_candidate_missing",
        },
        "transport_decision": {"status": "candidate_positive", "transport_promotable": curve_status == "decisive"},
        "demo_recording": {"status": demo_status, "required": True},
    }


def _readiness(status: str = "ready") -> Dict[str, Any]:
    return {"status": status, "failed_count": 0 if status == "ready" else 1, "warning_count": 0}


class RunTown01GoalSequenceTests(unittest.TestCase):
    def test_command_builders_use_conda_python_and_expected_tools(self) -> None:
        pair = build_pair_gate_command(python_exec="/env/bin/python3")
        demo = build_demo_recording_command(python_exec="/env/bin/python3")
        audit = build_offline_audit_command(python_exec="/env/bin/python3", require_goal_ready=True)

        self.assertEqual(pair[0], "/env/bin/python3")
        self.assertIn("tools/run_town01_direct_curve_pair_recovery_retry.py", pair)
        self.assertIn("tools/run_town01_demo_showcase.py", demo)
        self.assertIn("--record-dreamview", demo)
        self.assertIn("--dreamview-auto-open", demo)
        self.assertIn("--dreamview-open-wait-page", demo)
        self.assertIn("playwright", demo)
        self.assertIn("--require-recording-ready", demo)
        self.assertIn("tools/run_town01_goal_offline_audit.py", audit)
        self.assertIn("--require-goal-ready", audit)

    def test_default_outputs_separate_dry_run_from_online(self) -> None:
        online_json, online_md = default_output_paths(dry_run=False)
        dry_json, dry_md = default_output_paths(dry_run=True)

        self.assertNotEqual(online_json, dry_json)
        self.assertNotEqual(online_md, dry_md)
        self.assertIn("dry_run", dry_json.name)
        self.assertNotIn("dry_run", online_json.name)

    def test_dry_run_plans_curve_gate_demo_and_audit(self) -> None:
        payload = run_sequence(
            python_exec="/env/bin/python3",
            dry_run=True,
            goal_builder=lambda: _goal(curve_status="pending_missing_candidate_curve"),
            readiness_builder=lambda: _readiness("ready"),
            command_runner=lambda cmd: 0,
        )

        self.assertEqual(payload["status"], "dry_run")
        names = [step["name"] for step in payload["steps"]]
        self.assertEqual(names, ["direct_curve_pair_gate", "demo_recording", "offline_audit"])
        self.assertEqual(payload["steps"][0]["status"], "planned")
        self.assertEqual(payload["steps"][1]["status"], "skipped")
        self.assertEqual(exit_code_for_payload(payload), 0)

    def test_runs_pair_then_demo_when_goal_becomes_decisive(self) -> None:
        goals = [
            _goal(curve_status="pending_missing_candidate_curve"),
            _goal(curve_status="decisive", demo_status="not_checked"),
            _goal(curve_status="decisive", demo_status="ready", overall="ready_for_final_review"),
            _goal(curve_status="decisive", demo_status="ready", overall="ready_for_final_review"),
        ]
        commands: list[str] = []

        def goal_builder() -> Dict[str, Any]:
            return goals.pop(0)

        def runner(cmd: Sequence[str]) -> int:
            commands.append(" ".join(cmd))
            return 0

        payload = run_sequence(
            python_exec="/env/bin/python3",
            dry_run=False,
            goal_builder=goal_builder,
            readiness_builder=lambda: _readiness("ready"),
            command_runner=runner,
        )

        self.assertEqual(payload["status"], "completed_goal_ready")
        self.assertEqual(len(commands), 3)
        self.assertIn("run_town01_direct_curve_pair_recovery_retry.py", commands[0])
        self.assertIn("run_town01_demo_showcase.py", commands[1])
        self.assertIn("--dreamview-open-wait-page", commands[1])
        self.assertIn("run_town01_goal_offline_audit.py", commands[2])
        self.assertEqual(exit_code_for_payload(payload, require_goal_ready=True), 0)

    def test_stops_when_curve_gate_returns_nonzero(self) -> None:
        goals = [
            _goal(curve_status="pending_missing_candidate_curve"),
            _goal(curve_status="pending_missing_candidate_curve"),
        ]

        payload = run_sequence(
            python_exec="/env/bin/python3",
            dry_run=False,
            goal_builder=lambda: goals.pop(0),
            readiness_builder=lambda: _readiness("ready"),
            command_runner=lambda cmd: 2,
        )

        self.assertEqual(payload["status"], "stopped_after_curve_gate")
        self.assertEqual(payload["steps"][0]["status"], "failed")
        self.assertEqual(exit_code_for_payload(payload), 2)

    def test_nonzero_curve_gate_can_continue_if_goal_moved_past_curve_gate(self) -> None:
        goals = [
            _goal(curve_status="pending_missing_candidate_curve"),
            _goal(curve_status="decisive", demo_status="not_checked"),
            _goal(curve_status="decisive", demo_status="ready", overall="ready_for_final_review"),
            _goal(curve_status="decisive", demo_status="ready", overall="ready_for_final_review"),
        ]
        returncodes = [2, 0, 0]

        def goal_builder() -> Dict[str, Any]:
            return goals.pop(0)

        def runner(cmd: Sequence[str]) -> int:
            return returncodes.pop(0)

        payload = run_sequence(
            python_exec="/env/bin/python3",
            dry_run=False,
            goal_builder=goal_builder,
            readiness_builder=lambda: _readiness("ready"),
            command_runner=runner,
        )

        self.assertEqual(payload["status"], "completed_goal_ready")
        self.assertEqual(payload["steps"][0]["status"], "failed")
        self.assertEqual(payload["steps"][1]["status"], "passed")

    def test_skips_demo_when_readiness_not_ready(self) -> None:
        payload = run_sequence(
            python_exec="/env/bin/python3",
            dry_run=True,
            goal_builder=lambda: _goal(curve_status="decisive", demo_status="not_checked"),
            readiness_builder=lambda: _readiness("failed"),
            command_runner=lambda cmd: 0,
        )

        demo = [step for step in payload["steps"] if step["name"] == "demo_recording"][0]
        self.assertEqual(demo["status"], "skipped")
        self.assertIn("demo readiness is failed", demo["reason"])

    def test_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            payload = run_sequence(
                python_exec="/env/bin/python3",
                dry_run=True,
                goal_builder=lambda: _goal(curve_status="pending_missing_candidate_curve"),
                readiness_builder=lambda: _readiness("ready"),
                command_runner=lambda cmd: 0,
            )
            json_out = root / "sequence.json"
            md_out = root / "sequence.md"

            write_json(json_out, payload)
            write_markdown(md_out, payload)

            self.assertEqual(json.loads(json_out.read_text(encoding="utf-8"))["status"], "dry_run")
            self.assertIn("Town01 Goal Online Sequence", md_out.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
