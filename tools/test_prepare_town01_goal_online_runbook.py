from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.prepare_town01_goal_online_runbook import build_runbook_payload, write_json, write_markdown


class PrepareTown01GoalOnlineRunbookTests(unittest.TestCase):
    def test_builds_key_online_validation_commands(self) -> None:
        payload = build_runbook_payload(python_exec="/env/bin/python3")
        commands = {item["key"]: item for item in payload["commands"]}

        self.assertEqual(payload["status"], "ready")
        self.assertIn("offline_smoke_config", commands)
        self.assertIn("followstop_lateral_enabled_canary", commands)
        self.assertIn("town01_ros2_gt_canary", commands)
        self.assertIn("town01_direct_goal_sequence", commands)
        self.assertIn("town01_transport_ab_canonical", commands)
        self.assertIn("town01_demo_recording", commands)
        self.assertIn("post_online_triage", commands)
        self.assertIn("inspect_online_runbook_execution", commands)
        self.assertIn("final_strict_offline_audit", commands)

    def test_commands_use_requested_python_exec(self) -> None:
        payload = build_runbook_payload(python_exec="/env/bin/python3")

        for item in payload["commands"]:
            self.assertEqual(item["command"][0], "/env/bin/python3")

    def test_online_commands_are_marked_as_runtime(self) -> None:
        payload = build_runbook_payload(python_exec="/env/bin/python3")
        commands = {item["key"]: item for item in payload["commands"]}

        self.assertTrue(commands["followstop_lateral_enabled_canary"]["starts_carla"])
        self.assertTrue(commands["town01_ros2_gt_canary"]["starts_apollo"])
        self.assertTrue(commands["town01_direct_goal_sequence"]["starts_carla"])
        self.assertTrue(commands["town01_demo_recording"]["starts_apollo"])
        self.assertFalse(commands["post_online_triage"]["starts_carla"])
        self.assertFalse(commands["final_strict_offline_audit"]["starts_apollo"])

    def test_town01_direct_sequence_command_is_goal_sequence(self) -> None:
        payload = build_runbook_payload(python_exec="/env/bin/python3")
        direct = {item["key"]: item for item in payload["commands"]}["town01_direct_goal_sequence"]

        self.assertIn("tools/run_town01_goal_sequence.py", direct["command"])
        self.assertIn("--python-exec", direct["command"])

    def test_demo_recording_command_requires_dreamview_recording_ready(self) -> None:
        payload = build_runbook_payload(python_exec="/env/bin/python3")
        demo = {item["key"]: item for item in payload["commands"]}["town01_demo_recording"]

        self.assertIn("tools/run_town01_demo_showcase.py", demo["command"])
        self.assertIn("--record-dreamview", demo["command"])
        self.assertIn("--dreamview-auto-open", demo["command"])
        self.assertIn("--dreamview-open-wait-page", demo["command"])
        self.assertIn("--dreamview-browser-cmd", demo["command"])
        self.assertIn("auto", demo["command"])
        self.assertIn("--require-recording-ready", demo["command"])

    def test_execution_inspector_is_post_online_helper_not_goal_gate(self) -> None:
        payload = build_runbook_payload(python_exec="/env/bin/python3")
        commands = {item["key"]: item for item in payload["commands"]}
        inspector = commands["inspect_online_runbook_execution"]

        self.assertFalse(inspector["starts_carla"])
        self.assertFalse(inspector["starts_apollo"])
        self.assertFalse(inspector["required_for_goal"])
        self.assertIn("tools/inspect_town01_goal_online_runbook_execution.py", inspector["command"])
        self.assertNotIn("inspect_online_runbook_execution", payload["recommended_order"])

    def test_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            payload = build_runbook_payload(python_exec="/env/bin/python3")
            json_out = root / "runbook.json"
            md_out = root / "runbook.md"

            write_json(json_out, payload)
            write_markdown(md_out, payload)

            self.assertEqual(json.loads(json_out.read_text(encoding="utf-8"))["status"], "ready")
            text = md_out.read_text(encoding="utf-8")
            self.assertIn("Town01 Goal Online Runbook", text)
            self.assertIn("followstop_lateral_enabled_canary", text)
            self.assertIn("town01_direct_goal_sequence", text)
            self.assertIn("town01_demo_recording", text)


if __name__ == "__main__":
    unittest.main()
