from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict

from tools.inspect_town01_after_online import (
    build_after_online_payload,
    classify_after_online,
    write_json,
    write_markdown,
)


def _execution(status: str = "dry_run_only") -> Dict[str, Any]:
    return {
        "status": status,
        "reason": "test execution reason",
        "next_action": "run_online_executor_without_dry_run",
        "initial_next_key": "followstop_lateral_enabled_canary",
        "final_next_key": "followstop_lateral_enabled_canary",
        "path": "/tmp/execution.json",
    }


def _goal(*, overall: str = "in_progress", blockers: list[str] | None = None) -> Dict[str, Any]:
    return {
        "overall_status": overall,
        "blockers": blockers if blockers is not None else ["curve_recovery:pending_missing_candidate_curve"],
        "transport_decision": {
            "status": "pending_curve_recovery",
            "direct_candidate_action": "rerun_curve_pair",
            "transport_promotable": False,
        },
        "transport_ab": {
            "summary": {
                "route_count": 6,
                "candidate_positive_count": 4,
                "candidate_negative_count": 0,
                "candidate_inconclusive_count": 1,
            },
            "rows": [
                {"route_id": "town01_rh_spawn176_goal061", "verdict": "candidate_inconclusive_runtime_interrupted"},
                {"route_id": "town01_rh_spawn177_goal052", "verdict": "candidate_missing"},
                {"route_id": "town01_rh_spawn183_goal044", "verdict": "candidate_positive_distance"},
            ],
        },
        "curve_recovery": {
            "status": "pending_missing_candidate_curve",
            "recovery_verdict": "recovery_incomplete_candidate_missing",
        },
    }


def _triage(status: str = "awaiting_direct_curve_gate") -> Dict[str, Any]:
    return {
        "triage": {
            "status": status,
            "reason": "triage reason",
            "next_command": "/env/bin/python3 tools/run_town01_goal_sequence.py",
        }
    }


def _next_action() -> Dict[str, Any]:
    return {
        "status": "ready",
        "primary": {
            "key": "followstop_lateral_enabled_canary",
            "command": "/env/bin/python3 tools/run_town01_goal_online_runbook.py",
        },
    }


def _handoff() -> Dict[str, Any]:
    return {
        "handoff_status": "ready_to_run_next_online",
        "commands": {
            "broader_validation_next": "/env/bin/python3 tools/run_town01_goal_online_runbook.py",
            "post_online_triage": "/env/bin/python3 tools/inspect_town01_post_online_triage.py",
            "offline_audit_optional": "/env/bin/python3 tools/run_town01_goal_offline_audit.py",
            "demo_recording_after_transport_gate": "/env/bin/python3 tools/run_town01_demo_showcase.py --dreamview-open-wait-page",
            "next_online": "/env/bin/python3 tools/run_town01_goal_online_runbook.py",
        },
    }


def _demo_readiness(status: str = "ready") -> Dict[str, Any]:
    return {"status": status, "failed_count": 0, "warning_count": 0}


class InspectTown01AfterOnlineTests(unittest.TestCase):
    def test_classifies_dry_run_as_awaiting_online_run(self) -> None:
        result = classify_after_online(
            execution=_execution("dry_run_only"),
            goal=_goal(),
            triage=_triage(),
            next_action=_next_action(),
            handoff=_handoff(),
            demo_readiness=_demo_readiness(),
        )

        self.assertEqual(result["status"], "awaiting_online_run")
        self.assertIn("run_town01_goal_online_runbook.py", result["next_command"])

    def test_classifies_failed_execution_as_needing_triage(self) -> None:
        result = classify_after_online(
            execution=_execution("failed_without_evidence_progress"),
            goal=_goal(),
            triage=_triage(),
            next_action=_next_action(),
            handoff=_handoff(),
            demo_readiness=_demo_readiness(),
        )

        self.assertEqual(result["status"], "online_run_needs_triage")
        self.assertIn("inspect_town01_post_online_triage.py", result["next_command"])

    def test_classifies_demo_ready(self) -> None:
        result = classify_after_online(
            execution=_execution("advanced_to_next_gate"),
            goal=_goal(blockers=["demo_recording:not_checked"]),
            triage=_triage("ready_for_demo_recording"),
            next_action=_next_action(),
            handoff=_handoff(),
            demo_readiness=_demo_readiness(),
        )

        self.assertEqual(result["status"], "ready_for_demo_recording")
        self.assertIn("run_town01_goal_sequence.py", result["next_command"])

    def test_classifies_final_review(self) -> None:
        result = classify_after_online(
            execution=_execution("advanced_to_next_gate"),
            goal=_goal(overall="ready_for_final_review", blockers=[]),
            triage=_triage("ready_for_final_strict_audit"),
            next_action=_next_action(),
            handoff=_handoff(),
            demo_readiness=_demo_readiness(),
        )

        self.assertEqual(result["status"], "ready_for_final_strict_audit")
        self.assertIn("run_town01_goal_offline_audit.py", result["next_command"])

    def test_builds_payload_and_writes_outputs(self) -> None:
        payload = build_after_online_payload(
            python_exec="/env/bin/python3",
            execution_builder=lambda: _execution("dry_run_only"),
            runbook_results_builder=lambda: {
                "status": "in_progress",
                "next_key": "followstop_lateral_enabled_canary",
                "executor_command": "/env/bin/python3 tools/run_town01_goal_online_runbook.py",
            },
            goal_builder=lambda: _goal(),
            triage_builder=lambda: _triage(),
            next_action_builder=lambda: _next_action(),
            handoff_builder=lambda: _handoff(),
            demo_readiness_builder=lambda: _demo_readiness(),
        )

        self.assertEqual(payload["status"], "awaiting_online_run")
        self.assertEqual(payload["runbook"]["next_key"], "followstop_lateral_enabled_canary")
        self.assertEqual(payload["demo_readiness"]["status"], "ready")
        self.assertEqual(payload["transport_gate"]["candidate_positive_count"], 4)
        self.assertEqual(
            payload["transport_gate"]["pending_rerun_routes"],
            ["town01_rh_spawn176_goal061", "town01_rh_spawn177_goal052"],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            json_out = Path(tmpdir) / "intake.json"
            md_out = Path(tmpdir) / "intake.md"
            write_json(json_out, payload)
            write_markdown(md_out, payload)

            self.assertEqual(json.loads(json_out.read_text(encoding="utf-8"))["status"], "awaiting_online_run")
            text = md_out.read_text(encoding="utf-8")
            self.assertIn("Town01 After-Online Intake", text)
            self.assertIn("Next Command", text)
            self.assertIn("pending_rerun_routes", text)


if __name__ == "__main__":
    unittest.main()
