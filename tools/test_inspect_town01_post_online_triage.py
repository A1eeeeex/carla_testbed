from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict

from tools.inspect_town01_post_online_triage import (
    build_triage_payload,
    classify_triage,
    inspect_online_artifact,
    write_json,
    write_markdown,
)


def _goal(
    *,
    overall: str = "in_progress",
    curve_status: str = "pending_missing_candidate_curve",
    demo_status: str = "not_checked",
) -> Dict[str, Any]:
    next_key = "direct_curve176_recovery_online" if curve_status != "decisive" else "demo_recording_online"
    blockers = []
    if curve_status != "decisive":
        blockers.append(f"curve_recovery:{curve_status}")
    if demo_status != "ready":
        blockers.append(f"demo_recording:{demo_status}")
    return {
        "overall_status": overall,
        "blockers": blockers,
        "next_command": {"key": next_key, "reason": "test next", "command": "python next.py"},
        "curve_recovery": {"status": curve_status, "recovery_verdict": "recovery_positive"},
        "transport_decision": {"status": "candidate_positive", "transport_promotable": curve_status == "decisive"},
        "demo_recording": {"status": demo_status, "required": True},
    }


class InspectTown01PostOnlineTriageTests(unittest.TestCase):
    def test_inspect_online_artifact_ignores_dry_run_in_online_slot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "artifact.json"
            path.write_text(json.dumps({"status": "dry_run"}), encoding="utf-8")

            payload = inspect_online_artifact(path)

            self.assertEqual(payload["status"], "legacy_dry_run_ignored")
            self.assertEqual(payload["raw_status"], "dry_run")

    def test_classifies_awaiting_curve_gate(self) -> None:
        triage = classify_triage(_goal(curve_status="pending_missing_candidate_curve"), {"status": "ready"})

        self.assertEqual(triage["status"], "awaiting_direct_curve_gate")
        self.assertIn("run_town01_goal_sequence.py", triage["next_command"])

    def test_classifies_ready_for_demo_recording(self) -> None:
        triage = classify_triage(_goal(curve_status="decisive", demo_status="not_checked"), {"status": "ready"})

        self.assertEqual(triage["status"], "ready_for_demo_recording")
        self.assertIn("run_town01_demo_showcase.py", triage["next_command"])
        self.assertIn("--record-dreamview", triage["next_command"])
        self.assertIn("--dreamview-open-wait-page", triage["next_command"])
        self.assertIn("--dreamview-browser-cmd playwright", triage["next_command"])

    def test_classifies_ready_for_final_audit(self) -> None:
        triage = classify_triage(
            _goal(overall="ready_for_final_review", curve_status="decisive", demo_status="ready"),
            {"status": "ready"},
        )

        self.assertEqual(triage["status"], "ready_for_final_strict_audit")
        self.assertIn("--require-goal-ready", triage["next_command"])

    def test_builds_payload_with_artifact_slots(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            sequence = root / "sequence.json"
            pair = root / "pair.json"
            pair.write_text(json.dumps({"status": "completed_gate_transition", "reason": "ok"}), encoding="utf-8")

            payload = build_triage_payload(
                goal_builder=lambda: _goal(curve_status="decisive", demo_status="not_checked"),
                readiness_builder=lambda: {"status": "ready", "failed_count": 0, "warning_count": 0},
                sequence_json=sequence,
                pair_json=pair,
            )

            self.assertEqual(payload["triage"]["status"], "ready_for_demo_recording")
            self.assertIn("--dreamview-browser-cmd playwright", payload["triage"]["next_command"])
            self.assertEqual(payload["online_artifacts"]["goal_sequence"]["status"], "missing")
            self.assertEqual(payload["online_artifacts"]["direct_curve_pair"]["status"], "completed_gate_transition")

    def test_artifact_summary_includes_failed_steps(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "sequence.json"
            path.write_text(
                json.dumps(
                    {
                        "status": "stopped_after_curve_gate",
                        "reason": "direct curve pair gate returned non-zero",
                        "steps": [
                            {
                                "name": "direct_curve_pair_gate",
                                "status": "failed",
                                "returncode": 2,
                                "reason": "curve gate failed",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            payload = inspect_online_artifact(path)

            self.assertEqual(payload["status"], "stopped_after_curve_gate")
            self.assertEqual(payload["steps"]["count"], 1)
            self.assertEqual(payload["steps"]["last"]["name"], "direct_curve_pair_gate")
            self.assertEqual(payload["steps"]["failed"][0]["returncode"], 2)

    def test_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            payload = build_triage_payload(
                goal_builder=lambda: _goal(curve_status="pending_missing_candidate_curve"),
                readiness_builder=lambda: {"status": "ready", "failed_count": 0, "warning_count": 0},
                sequence_json=root / "sequence.json",
                pair_json=root / "pair.json",
            )
            json_out = root / "triage.json"
            md_out = root / "triage.md"

            write_json(json_out, payload)
            write_markdown(md_out, payload)

            self.assertEqual(json.loads(json_out.read_text(encoding="utf-8"))["triage"]["status"], "awaiting_direct_curve_gate")
            self.assertIn("Town01 Post-Online Triage", md_out.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
