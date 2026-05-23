from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict

from tools.inspect_town01_goal_completion import build_completion_payload, write_json, write_markdown


def _audit(status: str = "offline_pass_goal_in_progress") -> Dict[str, Any]:
    return {
        "audit_status": status,
        "test_results": [
            {"name": "unit", "status": "passed"},
            {"name": "pytest", "status": "passed"},
        ],
    }


def _runbook(status: str = "in_progress", next_key: str = "followstop_lateral_enabled_canary") -> Dict[str, Any]:
    return {
        "status": status,
        "next_key": next_key,
        "results": {
            "followstop_lateral_enabled_canary": {
                "status": "dry_run_only",
                "reason": "missing online summary",
            }
        },
    }


def _preflight(status: str = "ready") -> Dict[str, Any]:
    checks = []
    if status == "failed":
        checks.append({"name": "tcp_socket_permission", "status": "failed"})
    return {"status": status, "checks": checks}


def _goal(*, demo_status: str = "not_checked", promotable: bool = False) -> Dict[str, Any]:
    return {
        "overall_status": "in_progress",
        "blockers": [
            "transport_ab:partial_pending_rerun",
            "curve_recovery:pending_missing_candidate_curve",
            f"demo_recording:{demo_status}",
        ],
        "transport_decision": {
            "status": "candidate_positive" if promotable else "pending_curve_recovery",
            "direct_candidate_action": "promote" if promotable else "rerun_curve_pair",
            "transport_promotable": promotable,
        },
        "demo_recording": {"status": demo_status, "required": True},
        "recommended_commands": {
            "demo_recording_online": "/env/bin/python3 tools/run_town01_demo_showcase.py --dreamview-open-wait-page"
        },
        "next_command": {
            "command": "/env/bin/python3 tools/run_town01_direct_curve_recovery_retry.py --route-id town01_rh_spawn176_goal061"
        },
    }


def _after_online(*, positive: int = 4, negative: int = 0, pending: list[str] | None = None) -> Dict[str, Any]:
    return {
        "status": "awaiting_online_run",
        "next_command": "/env/bin/python3 tools/run_town01_goal_online_runbook.py",
        "transport_gate": {
            "decision_status": "pending_curve_recovery",
            "direct_candidate_action": "rerun_curve_pair",
            "transport_promotable": False,
            "candidate_positive_count": positive,
            "candidate_negative_count": negative,
            "pending_rerun_routes": pending
            if pending is not None
            else ["town01_rh_spawn176_goal061", "town01_rh_spawn177_goal052"],
        },
        "demo_readiness": {"status": "ready", "failed_count": 0, "warning_count": 0},
    }


class InspectTown01GoalCompletionTests(unittest.TestCase):
    def test_current_shape_is_incomplete_with_partial_transport(self) -> None:
        payload = build_completion_payload(
            python_exec="/env/bin/python3",
            offline_audit_builder=lambda: _audit(),
            preflight_builder=lambda: _preflight(),
            runbook_builder=lambda: _runbook(),
            goal_builder=lambda: _goal(),
            after_online_builder=lambda: _after_online(),
        )

        self.assertEqual(payload["status"], "incomplete")
        statuses = {item["key"]: item["status"] for item in payload["requirements"]}
        self.assertEqual(statuses["structural_validation"], "proven")
        self.assertEqual(statuses["critical_scene_online_validation"], "incomplete")
        self.assertEqual(statuses["transport_ab_decision"], "partial")
        self.assertEqual(statuses["demo_recording"], "partial")
        self.assertIn("run_town01_goal_online_runbook.py", payload["next_command"])

    def test_complete_when_all_requirements_are_proven(self) -> None:
        payload = build_completion_payload(
            python_exec="/env/bin/python3",
            offline_audit_builder=lambda: _audit(),
            preflight_builder=lambda: _preflight(),
            runbook_builder=lambda: _runbook(status="ready_for_goal_completion_review", next_key=""),
            goal_builder=lambda: {
                **_goal(demo_status="ready", promotable=True),
                "overall_status": "ready_for_final_review",
                "blockers": [],
            },
            after_online_builder=lambda: {
                **_after_online(positive=6, negative=0, pending=[]),
                "transport_gate": {
                    "decision_status": "candidate_positive",
                    "direct_candidate_action": "promote",
                    "transport_promotable": True,
                    "candidate_positive_count": 6,
                    "candidate_negative_count": 0,
                    "pending_rerun_routes": [],
                },
                "demo_readiness": {"status": "ready"},
            },
        )

        self.assertEqual(payload["status"], "complete")

    def test_writes_outputs(self) -> None:
        payload = build_completion_payload(
            python_exec="/env/bin/python3",
            offline_audit_builder=lambda: _audit(),
            preflight_builder=lambda: _preflight(),
            runbook_builder=lambda: _runbook(),
            goal_builder=lambda: _goal(),
            after_online_builder=lambda: _after_online(),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            json_out = Path(tmpdir) / "completion.json"
            md_out = Path(tmpdir) / "completion.md"
            write_json(json_out, payload)
            write_markdown(md_out, payload)

            self.assertEqual(json.loads(json_out.read_text(encoding="utf-8"))["status"], "incomplete")
            text = md_out.read_text(encoding="utf-8")
            self.assertIn("Town01 Goal Completion Audit", text)
            self.assertIn("transport_ab_decision", text)

    def test_failed_online_preflight_blocks_critical_scene_validation(self) -> None:
        payload = build_completion_payload(
            python_exec="/env/bin/python3",
            offline_audit_builder=lambda: _audit(),
            preflight_builder=lambda: _preflight("failed"),
            runbook_builder=lambda: _runbook(status="ready_for_goal_completion_review", next_key=""),
            goal_builder=lambda: _goal(),
            after_online_builder=lambda: _after_online(),
        )

        statuses = {item["key"]: item for item in payload["requirements"]}
        self.assertEqual(statuses["critical_scene_online_validation"]["status"], "incomplete")
        self.assertIn("tcp_socket_permission", statuses["critical_scene_online_validation"]["missing"][0])


if __name__ == "__main__":
    unittest.main()
