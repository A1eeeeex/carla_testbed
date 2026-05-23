from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict

from tools.inspect_town01_next_online_action import (
    build_next_action_payload,
    write_json,
    write_markdown,
)


def _runbook_results(*, next_status: str = "dry_run_only", next_key: str = "followstop_lateral_enabled_canary") -> Dict[str, Any]:
    return {
        "status": "in_progress",
        "next_key": next_key,
        "next_command": "/env/bin/python3 -m carla_testbed run --config followstop.yaml",
        "results": {
            next_key: {
                "status": next_status,
                "reason": "needs online evidence",
            }
        },
    }


def _goal(*, curve_status: str = "pending_missing_candidate_curve", overall: str = "in_progress") -> Dict[str, Any]:
    return {
        "overall_status": overall,
        "blockers": [f"curve_recovery:{curve_status}"],
        "next_command": {
            "key": "direct_curve176_recovery_online",
            "command": "/env/bin/python3 tools/run_town01_direct_curve_recovery_retry.py --route-id town01_rh_spawn176_goal061",
            "reason": "curve176 needs online evidence",
        },
        "curve_recovery": {"status": curve_status},
        "demo_recording": {"status": "not_checked", "required": True},
    }


def _triage(status: str = "awaiting_direct_curve_gate") -> Dict[str, Any]:
    return {
        "triage": {
            "status": status,
            "reason": "triage reason",
            "next_command": "/env/bin/python3 tools/run_town01_goal_sequence.py",
        }
    }


class InspectTown01NextOnlineActionTests(unittest.TestCase):
    def test_auto_prefers_broader_validation_when_first_gate_incomplete(self) -> None:
        payload = build_next_action_payload(
            python_exec="/env/bin/python3",
            runbook_results_builder=lambda: _runbook_results(next_status="dry_run_only"),
            goal_builder=lambda: _goal(),
            triage_builder=lambda: _triage(),
        )

        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["primary"]["title"], "broader_validation")
        self.assertEqual(payload["primary"]["key"], "followstop_lateral_enabled_canary")
        self.assertIn("run_town01_goal_online_runbook.py", payload["primary"]["command"])
        self.assertIn("carla_testbed run", payload["primary"]["underlying_command"])

    def test_direct_focus_selects_direct_chain_command(self) -> None:
        payload = build_next_action_payload(
            python_exec="/env/bin/python3",
            focus="direct",
            runbook_results_builder=lambda: _runbook_results(next_status="dry_run_only"),
            goal_builder=lambda: _goal(),
            triage_builder=lambda: _triage(),
        )

        self.assertEqual(payload["primary"]["title"], "direct_chain")
        self.assertIn("run_town01_direct_curve_recovery_retry.py", payload["primary"]["command"])

    def test_auto_moves_to_direct_when_broader_gate_passed(self) -> None:
        payload = build_next_action_payload(
            python_exec="/env/bin/python3",
            runbook_results_builder=lambda: _runbook_results(next_status="passed"),
            goal_builder=lambda: _goal(curve_status="pending_missing_candidate_curve"),
            triage_builder=lambda: _triage(),
        )

        self.assertEqual(payload["primary"]["title"], "direct_chain")

    def test_demo_focus_selects_demo_or_sequence_command(self) -> None:
        payload = build_next_action_payload(
            python_exec="/env/bin/python3",
            focus="demo",
            runbook_results_builder=lambda: _runbook_results(next_status="passed"),
            goal_builder=lambda: _goal(curve_status="decisive"),
            triage_builder=lambda: _triage(status="ready_for_demo_recording"),
        )

        self.assertEqual(payload["primary"]["title"], "demo_recording")
        self.assertIn("run_town01_goal_sequence.py", payload["primary"]["command"])

    def test_demo_alternative_is_labeled_as_gate_until_triage_is_ready(self) -> None:
        payload = build_next_action_payload(
            python_exec="/env/bin/python3",
            runbook_results_builder=lambda: _runbook_results(next_status="dry_run_only"),
            goal_builder=lambda: _goal(),
            triage_builder=lambda: _triage(status="awaiting_direct_curve_gate"),
        )

        gate_items = [item for item in payload["alternatives"] if item["title"] == "goal_sequence_gate"]
        self.assertEqual(len(gate_items), 1)
        self.assertEqual(gate_items[0]["key"], "goal_sequence_gate")
        self.assertEqual(gate_items[0]["status"], "awaiting_direct_curve_gate")

    def test_demo_focus_reports_gate_when_demo_not_ready(self) -> None:
        payload = build_next_action_payload(
            python_exec="/env/bin/python3",
            focus="demo",
            runbook_results_builder=lambda: _runbook_results(next_status="passed"),
            goal_builder=lambda: _goal(curve_status="pending_missing_candidate_curve"),
            triage_builder=lambda: _triage(status="awaiting_direct_curve_gate"),
        )

        self.assertEqual(payload["primary"]["title"], "goal_sequence_gate")
        self.assertIn("not ready", payload["rationale"])

    def test_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            payload = build_next_action_payload(
                python_exec="/env/bin/python3",
                runbook_results_builder=lambda: _runbook_results(),
                goal_builder=lambda: _goal(),
                triage_builder=lambda: _triage(),
            )
            json_out = root / "next.json"
            md_out = root / "next.md"

            write_json(json_out, payload)
            write_markdown(md_out, payload)

            self.assertEqual(json.loads(json_out.read_text(encoding="utf-8"))["status"], "ready")
            text = md_out.read_text(encoding="utf-8")
            self.assertIn("Town01 Next Online Action", text)
            self.assertIn("Underlying Command", text)


if __name__ == "__main__":
    unittest.main()
