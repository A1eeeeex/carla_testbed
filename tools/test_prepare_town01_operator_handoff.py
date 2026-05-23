from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict

from tools.prepare_town01_operator_handoff import build_handoff_payload, write_json, write_markdown


class PrepareTown01OperatorHandoffTests(unittest.TestCase):
    def test_builds_handoff_with_next_online_command_and_expected_outputs(self) -> None:
        def goal_builder() -> Dict[str, Any]:
            return {
                "overall_status": "in_progress",
                "blockers": ["curve_recovery:pending_missing_candidate_curve"],
                "next_command": {
                    "key": "direct_curve176_recovery_online",
                    "reason": "curve176 needs fresh evidence",
                    "command": "/env/bin/python3 tools/run_town01_direct_curve_recovery_retry.py --route-id town01_rh_spawn176_goal061 --retries 1",
                },
            }

        def resume_builder() -> Dict[str, Any]:
            return {"resume_state": {"status": "awaiting_direct_curve_online"}}

        def preflight_builder() -> Dict[str, Any]:
            return {"status": "ready", "failed_count": 0, "warning_count": 0}

        def demo_builder() -> Dict[str, Any]:
            return {"status": "ready", "failed_count": 0, "warning_count": 0}

        def runbook_builder() -> Dict[str, Any]:
            return {
                "status": "in_progress",
                "next_key": "followstop_lateral_enabled_canary",
                "next_command": "/env/bin/python3 -m carla_testbed run --config followstop.yaml",
                "executor_command": "/env/bin/python3 tools/run_town01_goal_online_runbook.py --python-exec /env/bin/python3",
                "results": {
                    "followstop_lateral_enabled_canary": {
                        "status": "dry_run_only",
                        "reason": "online summary is missing",
                    }
                },
            }

        def next_action_builder() -> Dict[str, Any]:
            return {
                "status": "ready",
                "focus": "auto",
                "rationale": "broader validation is first incomplete runbook gate",
                "primary": {
                    "key": "followstop_lateral_enabled_canary",
                    "title": "broader_validation",
                    "status": "dry_run_only",
                    "reason": "online summary is missing",
                    "command": "/env/bin/python3 -m carla_testbed run --config followstop.yaml",
                },
                "alternatives": [
                    {
                        "key": "direct_curve176_recovery_online",
                        "title": "direct_chain",
                        "status": "pending_missing_candidate_curve",
                        "reason": "curve176 needs fresh evidence",
                        "command": "/env/bin/python3 tools/run_town01_direct_curve_recovery_retry.py --route-id town01_rh_spawn176_goal061 --retries 1",
                    }
                ],
            }

        payload = build_handoff_payload(
            goal_builder=goal_builder,
            resume_builder=resume_builder,
            preflight_builder=preflight_builder,
            demo_readiness_builder=demo_builder,
            runbook_results_builder=runbook_builder,
            next_action_builder=next_action_builder,
        )

        self.assertEqual(payload["handoff_status"], "ready_to_run_next_online")
        self.assertEqual(payload["selected_next_action"]["primary"]["key"], "followstop_lateral_enabled_canary")
        self.assertEqual(payload["current_route_id"], "town01_rh_spawn176_goal061")
        self.assertEqual(payload["broader_validation"]["next_key"], "followstop_lateral_enabled_canary")
        self.assertIn("run_town01_goal_online_runbook.py", payload["commands"]["broader_validation_next"])
        self.assertIn("carla_testbed run", payload["broader_validation"]["underlying_command"])
        self.assertIn("run_town01_direct_curve_recovery_retry.py", payload["commands"]["direct_chain_next"])
        self.assertIn("run_town01_goal_online_runbook.py", payload["commands"]["next_online"])
        self.assertIn("run_town01_direct_curve_pair_recovery_retry.py", payload["commands"]["curve_pair_online_optional"])
        self.assertIn("run_town01_goal_sequence.py", payload["commands"]["goal_sequence_online_optional"])
        self.assertIn("inspect_town01_post_online_triage.py", payload["commands"]["post_online_triage"])
        self.assertIn("prepare_town01_goal_online_runbook.py", payload["commands"]["online_runbook"])
        self.assertIn("inspect_town01_goal_online_runbook_results.py", payload["commands"]["online_runbook_results"])
        self.assertIn("run_town01_goal_online_runbook.py", payload["commands"]["online_runbook_execute_next"])
        self.assertIn("run_town01_goal_online_runbook.py", payload["commands"]["online_runbook_execute_until_blocked"])
        self.assertIn("--mode until-blocked", payload["commands"]["online_runbook_execute_until_blocked"])
        self.assertIn(
            "inspect_town01_goal_online_runbook_execution.py",
            payload["commands"]["online_runbook_execution_inspect"],
        )
        self.assertIn("inspect_town01_after_online.py", payload["commands"]["after_online_intake"])
        self.assertIn("--allow-dry-run-fallback", payload["commands"]["after_online_intake"])
        self.assertIn("inspect_town01_goal_completion.py", payload["commands"]["completion_audit"])
        self.assertIn("inspect_town01_next_online_action.py", payload["commands"]["next_online_action"])
        self.assertIn("--dreamview-browser-cmd auto", payload["commands"]["demo_recording_after_transport_gate"])
        self.assertIn("--dreamview-open-wait-page", payload["commands"]["demo_recording_after_transport_gate"])
        self.assertIn("town01_rh_spawn176_goal061", payload["expected_after_online"]["retry_json"])
        self.assertIn("town01_direct_curve_pair_recovery_retry_20260522.json", payload["expected_after_online"]["pair_retry_json"])
        self.assertIn("town01_goal_resume_20260522.md", payload["expected_after_online"]["resume_md"])
        self.assertEqual(payload["demo_readiness"]["status"], "ready")

    def test_missing_next_command_is_explicit(self) -> None:
        payload = build_handoff_payload(
            goal_builder=lambda: {"overall_status": "in_progress", "blockers": [], "next_command": {}},
            resume_builder=lambda: {"resume_state": {"status": "awaiting_next_command"}},
            preflight_builder=lambda: {"status": "ready", "failed_count": 0, "warning_count": 0},
            demo_readiness_builder=lambda: {"status": "ready", "failed_count": 0, "warning_count": 0},
            runbook_results_builder=lambda: {"status": "in_progress", "next_key": "", "next_command": "", "results": {}},
            next_action_builder=lambda: {"status": "missing_command", "primary": {}},
        )

        self.assertEqual(payload["handoff_status"], "missing_next_command")
        self.assertEqual(payload["current_route_id"], "")

    def test_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            payload = {
                "handoff_status": "ready_to_run_next_online",
                "goal_status": {
                    "overall_status": "in_progress",
                    "blockers": ["curve"],
                    "next_command": {"key": "direct_curve176_recovery_online", "reason": "curve"},
                },
                "resume_status": "awaiting_direct_curve_online",
                "broader_validation": {
                    "status": "in_progress",
                    "next_key": "followstop_lateral_enabled_canary",
                    "next_command": "python broader.py",
                    "underlying_command": "python raw.py",
                    "reason": "needs online summary",
                },
                "preflight": {"status": "ready", "failed_count": 0},
                "demo_readiness": {"status": "ready"},
                "current_route_id": "town01_rh_spawn176_goal061",
                "selected_next_action": {
                    "status": "ready",
                    "focus": "auto",
                    "rationale": "broader first",
                    "primary": {
                        "key": "followstop_lateral_enabled_canary",
                        "title": "broader_validation",
                        "status": "dry_run_only",
                        "reason": "needs online summary",
                    },
                    "alternatives": [],
                },
                "commands": {
                    "next_online": "python run.py",
                    "broader_validation_next": "python broader.py",
                    "direct_chain_next": "python direct.py",
                    "curve_pair_online_optional": "python pair.py",
                    "goal_sequence_online_optional": "python sequence.py",
                    "post_online_triage": "python triage.py",
                    "online_runbook": "python runbook.py",
                    "online_runbook_results": "python results.py",
                    "online_runbook_execute_next": "python execute_next.py",
                    "online_runbook_execute_until_blocked": "python execute_until_blocked.py",
                    "online_runbook_execution_inspect": "python execution_inspect.py",
                    "next_online_action": "python next_action.py",
                    "offline_audit_optional": "python audit.py",
                    "demo_recording_after_transport_gate": "python demo.py",
                },
                "expected_after_online": {"resume_md": "resume.md"},
                "operator_notes": ["note"],
            }
            json_out = root / "handoff.json"
            md_out = root / "handoff.md"

            write_json(json_out, payload)
            write_markdown(md_out, payload)

            self.assertEqual(json.loads(json_out.read_text(encoding="utf-8"))["handoff_status"], "ready_to_run_next_online")
            text = md_out.read_text(encoding="utf-8")
            self.assertIn("Town01 Operator Handoff", text)
            self.assertIn("Selected Next Online Action", text)
            self.assertIn("Broader Validation Next Command", text)
            self.assertIn("Direct Chain Next Command", text)
            self.assertIn("underlying_command", text)


if __name__ == "__main__":
    unittest.main()
