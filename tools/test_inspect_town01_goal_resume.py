from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.inspect_town01_goal_resume import classify_resume_state, write_json, write_markdown


class InspectTown01GoalResumeTests(unittest.TestCase):
    def test_classifies_ready_for_final_audit(self) -> None:
        state = classify_resume_state(
            {"overall_status": "ready_for_final_review"},
            {"status": "ready"},
        )

        self.assertEqual(state["status"], "ready_for_final_strict_audit")

    def test_classifies_preflight_failure_first(self) -> None:
        state = classify_resume_state(
            {"overall_status": "in_progress", "next_command": {"key": "direct_curve176_recovery_online"}},
            {"status": "failed"},
        )

        self.assertEqual(state["status"], "fix_local_preflight_first")

    def test_classifies_direct_curve_online(self) -> None:
        state = classify_resume_state(
            {
                "overall_status": "in_progress",
                "next_command": {
                    "key": "direct_curve176_recovery_online",
                    "reason": "curve176 still needs recovery",
                },
            },
            {"status": "ready"},
        )

        self.assertEqual(state["status"], "awaiting_direct_curve_online")
        self.assertIn("curve176", state["reason"])

    def test_classifies_demo_recording(self) -> None:
        state = classify_resume_state(
            {
                "overall_status": "in_progress",
                "next_command": {
                    "key": "demo_recording_online",
                    "reason": "demo missing",
                },
            },
            {"status": "ready"},
        )

        self.assertEqual(state["status"], "awaiting_demo_recording")

    def test_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            payload = {
                "resume_state": {"status": "awaiting_direct_curve_online", "reason": "curve"},
                "goal_status": {
                    "overall_status": "in_progress",
                    "blockers": ["curve"],
                    "transport_decision": {"status": "pending_curve_recovery", "transport_promotable": False},
                    "transport_ab": {"status": "partial_pending_rerun", "verdict_counts": {"candidate_missing": 1}},
                    "curve_recovery": {
                        "status": "pending_missing_candidate_curve",
                        "recovery_verdict": "recovery_incomplete_candidate_missing",
                        "source": "existing_json",
                        "verdict_counts": {"candidate_missing": 1},
                    },
                    "direct_curve_retry": {
                        "status": "has_failed_attempt",
                        "routes": [
                            {
                                "route_id": "town01_rh_spawn176_goal061",
                                "online": {
                                    "status": "retry_exhausted",
                                    "final_verdict": "candidate_stale_summary",
                                    "attempt_count": 1,
                                    "last_attempt": {
                                        "verdict": "candidate_stale_summary",
                                        "candidate_summary_new_for_attempt": False,
                                        "candidate_invalid_reason": "latest candidate summary predates this attempt",
                                    },
                                },
                            }
                        ],
                    },
                    "demo_recording": {"status": "not_checked", "source": "none", "required": True},
                    "next_command": {"key": "direct_curve176_recovery_online", "reason": "curve", "command": "python run.py"},
                },
                "preflight": {"status": "ready", "failed_count": 0, "warning_count": 0},
                "commands": {
                    "preflight": "python preflight.py",
                    "next_online": "python run.py",
                    "post_online_audit": "python audit.py",
                    "final_strict_audit": "python audit.py --require-goal-ready",
                },
                "artifacts": {"goal_status_json": "goal.json"},
            }
            json_out = root / "resume.json"
            md_out = root / "resume.md"

            write_json(json_out, payload)
            write_markdown(md_out, payload)

            self.assertEqual(json.loads(json_out.read_text(encoding="utf-8"))["resume_state"]["status"], "awaiting_direct_curve_online")
            text = md_out.read_text(encoding="utf-8")
            self.assertIn("Town01 Goal Resume", text)
            self.assertIn("Evidence Gates", text)
            self.assertIn("curve_recovery_verdict", text)
            self.assertIn("Direct Curve Retry Attempts", text)
            self.assertIn("candidate_stale_summary", text)


if __name__ == "__main__":
    unittest.main()
