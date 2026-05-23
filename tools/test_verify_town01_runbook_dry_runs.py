from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, Sequence
from unittest import mock

from tools import verify_town01_runbook_dry_runs as verifier


def _runbook() -> Dict[str, Any]:
    return {
        "commands": [
            {
                "key": "followstop_lateral_enabled_canary",
                "command": ["python", "-m", "carla_testbed", "run", "--run-dir", "runs/live"],
            },
            {
                "key": "town01_direct_goal_sequence",
                "command": ["python", "tools/run_town01_goal_sequence.py", "--python-exec", "python"],
            },
        ]
    }


class VerifyTown01RunbookDryRunsTests(unittest.TestCase):
    def test_dry_run_command_rewrites_followstop_run_dir(self) -> None:
        item = _runbook()["commands"][0]

        cmd = verifier.dry_run_command_for(item, run_root=Path("runs"))

        self.assertIn("--dry-run", cmd)
        self.assertIn("runs/followstop_goal_lateral_enabled_canary_check", cmd)
        self.assertNotIn("runs/live", cmd)

    def test_dry_run_command_appends_flag_once(self) -> None:
        item = {"key": "town01_direct_goal_sequence", "command": ["python", "tool.py", "--dry-run"]}

        cmd = verifier.dry_run_command_for(item)

        self.assertEqual(cmd.count("--dry-run"), 1)

    def test_build_payload_marks_all_passed(self) -> None:
        seen: list[list[str]] = []

        def runner(cmd: Sequence[str], timeout_s: float) -> Dict[str, Any]:
            seen.append(list(cmd))
            return {"returncode": 0, "stdout_tail": "ok", "stderr_tail": ""}

        with mock.patch.object(verifier, "build_runbook_payload", return_value=_runbook()):
            payload = verifier.build_verification_payload(
                python_exec="python",
                keys=["followstop_lateral_enabled_canary", "town01_direct_goal_sequence"],
                command_runner=runner,
            )

        self.assertEqual(payload["status"], "passed")
        self.assertEqual(payload["passed_count"], 2)
        self.assertTrue(all("--dry-run" in cmd for cmd in seen))

    def test_build_payload_reports_missing_command(self) -> None:
        with mock.patch.object(verifier, "build_runbook_payload", return_value=_runbook()):
            payload = verifier.build_verification_payload(
                python_exec="python",
                keys=["missing_key"],
                command_runner=lambda cmd, timeout_s: {"returncode": 0},
            )

        self.assertEqual(payload["status"], "failed")
        self.assertEqual(payload["results"][0]["status"], "missing_command")

    def test_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            payload = {
                "status": "passed",
                "python_exec": "python",
                "passed_count": 1,
                "failed_count": 0,
                "results": [
                    {
                        "key": "town01_direct_goal_sequence",
                        "status": "passed",
                        "returncode": 0,
                        "command": ["python", "tool.py", "--dry-run"],
                    }
                ],
            }
            json_out = root / "verify.json"
            md_out = root / "verify.md"

            verifier.write_json(json_out, payload)
            verifier.write_markdown(md_out, payload)

            self.assertEqual(json.loads(json_out.read_text(encoding="utf-8"))["status"], "passed")
            self.assertIn("Town01 Runbook Dry-Run Verification", md_out.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
