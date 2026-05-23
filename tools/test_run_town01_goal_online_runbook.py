from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, Sequence

from tools.run_town01_goal_online_runbook import (
    exit_code_for_payload,
    persist_execution_outputs,
    refresh_operator_artifacts,
    run_online_runbook,
    write_json,
    write_markdown,
)


def _runbook() -> Dict[str, Any]:
    return {
        "commands": [
            {
                "key": "followstop_lateral_enabled_canary",
                "phase": "online_followstop",
                "purpose": "followstop",
                "command": ["python", "-m", "carla_testbed", "run"],
            },
            {
                "key": "town01_ros2_gt_canary",
                "phase": "online_town01_ros2_gt",
                "purpose": "ros2 gt",
                "command": ["python", "tools/run_town01_capability_online_chain.py"],
            },
        ]
    }


def _results(next_key: str, *, status: str = "in_progress", result_status: str = "missing") -> Dict[str, Any]:
    return {
        "status": status,
        "next_key": next_key,
        "results": {
            "followstop_lateral_enabled_canary": {"status": result_status},
            "town01_ros2_gt_canary": {"status": result_status},
        },
    }


class RunTown01GoalOnlineRunbookTests(unittest.TestCase):
    def test_dry_run_plans_current_next_key(self) -> None:
        payload = run_online_runbook(
            python_exec="python",
            dry_run=True,
            runbook_builder=lambda python_exec: _runbook(),
            results_builder=lambda python_exec: _results("followstop_lateral_enabled_canary"),
        )

        self.assertEqual(payload["status"], "dry_run")
        self.assertIn("execution_id", payload)
        self.assertIn("started_at_s", payload)
        self.assertIn("finished_at_s", payload)
        self.assertIn("duration_s", payload)
        self.assertEqual(payload["steps"][0]["key"], "followstop_lateral_enabled_canary")
        self.assertEqual(payload["steps"][0]["status"], "planned")
        self.assertIn("planned_at_s", payload["steps"][0])
        self.assertEqual(payload["initial_results_snapshot"]["next_key"], "followstop_lateral_enabled_canary")
        self.assertEqual(payload["final_results_snapshot"]["next_key"], "followstop_lateral_enabled_canary")

    def test_next_mode_executes_one_step(self) -> None:
        seen: list[list[str]] = []

        def runner(cmd: Sequence[str]) -> int:
            seen.append(list(cmd))
            return 0

        calls = iter(
            [
                _results("followstop_lateral_enabled_canary"),
                _results("town01_ros2_gt_canary", result_status="passed"),
            ]
        )

        payload = run_online_runbook(
            python_exec="python",
            mode="next",
            command_runner=runner,
            runbook_builder=lambda python_exec: _runbook(),
            results_builder=lambda python_exec: next(calls),
        )

        self.assertEqual(payload["status"], "completed_one_step")
        self.assertEqual(payload["final_next_key"], "town01_ros2_gt_canary")
        self.assertIn("execution_id", payload)
        self.assertIn("started_at_s", payload["steps"][0])
        self.assertIn("finished_at_s", payload["steps"][0])
        self.assertIn("duration_s", payload["steps"][0])
        self.assertEqual(payload["initial_results_snapshot"]["next_key"], "followstop_lateral_enabled_canary")
        self.assertEqual(payload["final_results_snapshot"]["next_key"], "town01_ros2_gt_canary")
        self.assertEqual(
            payload["final_results_snapshot"]["results"]["followstop_lateral_enabled_canary"]["status"],
            "passed",
        )
        self.assertEqual(len(seen), 1)

    def test_next_mode_stops_without_progress_even_when_returncode_zero(self) -> None:
        seen: list[list[str]] = []

        def runner(cmd: Sequence[str]) -> int:
            seen.append(list(cmd))
            return 0

        calls = iter(
            [
                _results("followstop_lateral_enabled_canary"),
                _results("followstop_lateral_enabled_canary", result_status="online_attempt_no_summary"),
            ]
        )

        payload = run_online_runbook(
            python_exec="python",
            mode="next",
            command_runner=runner,
            runbook_builder=lambda python_exec: _runbook(),
            results_builder=lambda python_exec: next(calls),
        )

        self.assertEqual(payload["status"], "stopped_without_progress")
        self.assertEqual(payload["final_next_key"], "followstop_lateral_enabled_canary")
        self.assertEqual(payload["steps"][0]["returncode"], 0)
        self.assertFalse(payload["steps"][0]["evidence_progress"])
        self.assertEqual(exit_code_for_payload(payload), 1)
        self.assertEqual(len(seen), 1)

    def test_until_blocked_advances_until_max_steps(self) -> None:
        seen: list[list[str]] = []

        def runner(cmd: Sequence[str]) -> int:
            seen.append(list(cmd))
            return 0

        calls = iter(
            [
                _results("followstop_lateral_enabled_canary"),
                _results("town01_ros2_gt_canary", result_status="passed"),
                _results("town01_ros2_gt_canary", result_status="missing"),
            ]
        )

        payload = run_online_runbook(
            python_exec="python",
            mode="until-blocked",
            max_steps=2,
            command_runner=runner,
            runbook_builder=lambda python_exec: _runbook(),
            results_builder=lambda python_exec: next(calls),
        )

        self.assertEqual(payload["status"], "stopped_without_progress")
        self.assertEqual(payload["final_next_key"], "town01_ros2_gt_canary")
        self.assertEqual(len(seen), 2)

    def test_failure_stops_without_continue_flag(self) -> None:
        payload = run_online_runbook(
            python_exec="python",
            command_runner=lambda cmd: 7,
            runbook_builder=lambda python_exec: _runbook(),
            results_builder=lambda python_exec: _results("followstop_lateral_enabled_canary"),
        )

        self.assertEqual(payload["status"], "stopped_after_failure")
        self.assertEqual(exit_code_for_payload(payload), 1)

    def test_nonzero_return_is_accepted_when_evidence_advances(self) -> None:
        calls = iter(
            [
                _results("followstop_lateral_enabled_canary"),
                _results("town01_ros2_gt_canary", result_status="passed"),
            ]
        )

        payload = run_online_runbook(
            python_exec="python",
            mode="next",
            command_runner=lambda cmd: 7,
            runbook_builder=lambda python_exec: _runbook(),
            results_builder=lambda python_exec: next(calls),
        )

        self.assertEqual(payload["status"], "completed_one_step")
        self.assertIn("non-zero return accepted", payload["reason"])
        self.assertEqual(payload["steps"][0]["status"], "evidence_progress_returncode_nonzero")
        self.assertTrue(payload["steps"][0]["evidence_progress"])
        self.assertEqual(exit_code_for_payload(payload), 0)

    def test_until_blocked_continues_after_nonzero_with_evidence_progress(self) -> None:
        seen: list[list[str]] = []

        def runner(cmd: Sequence[str]) -> int:
            seen.append(list(cmd))
            return 7 if len(seen) == 1 else 0

        calls = iter(
            [
                _results("followstop_lateral_enabled_canary"),
                _results("town01_ros2_gt_canary", result_status="passed"),
                _results("town01_ros2_gt_canary", result_status="missing"),
            ]
        )

        payload = run_online_runbook(
            python_exec="python",
            mode="until-blocked",
            max_steps=2,
            command_runner=runner,
            runbook_builder=lambda python_exec: _runbook(),
            results_builder=lambda python_exec: next(calls),
        )

        self.assertEqual(payload["status"], "stopped_without_progress")
        self.assertEqual(len(seen), 2)
        self.assertEqual(payload["steps"][0]["status"], "evidence_progress_returncode_nonzero")

    def test_ready_results_do_nothing(self) -> None:
        payload = run_online_runbook(
            python_exec="python",
            command_runner=lambda cmd: 0,
            runbook_builder=lambda python_exec: _runbook(),
            results_builder=lambda python_exec: _results(
                "final_strict_offline_audit",
                status="ready_for_goal_completion_review",
            ),
        )

        self.assertEqual(payload["status"], "nothing_to_run")
        self.assertEqual(payload["steps"], [])

    def test_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            payload = {
                "status": "dry_run",
                "reason": "planned",
                "mode": "next",
                "start_at": "next",
                "initial_next_key": "followstop_lateral_enabled_canary",
                "final_next_key": "followstop_lateral_enabled_canary",
                "steps": [
                    {
                        "key": "followstop_lateral_enabled_canary",
                        "status": "planned",
                        "returncode": None,
                        "command": ["python", "-V"],
                    }
                ],
                "initial_results_snapshot": {
                    "status": "in_progress",
                    "next_key": "followstop_lateral_enabled_canary",
                    "results": {"followstop_lateral_enabled_canary": {"status": "missing"}},
                },
                "final_results_snapshot": {
                    "status": "in_progress",
                    "next_key": "followstop_lateral_enabled_canary",
                    "results": {"followstop_lateral_enabled_canary": {"status": "missing", "reason": "missing"}},
                },
            }
            json_out = root / "run.json"
            md_out = root / "run.md"

            write_json(json_out, payload)
            write_markdown(md_out, payload)

            self.assertEqual(json.loads(json_out.read_text(encoding="utf-8"))["status"], "dry_run")
            text = md_out.read_text(encoding="utf-8")
            self.assertIn("Town01 Goal Online Runbook Execution", text)
            self.assertIn("Result Snapshot", text)

    def test_refresh_operator_artifacts_writes_lightweight_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            paths = {
                "results_json": root / "results.json",
                "results_md": root / "results.md",
                "next_json": root / "next.json",
                "next_md": root / "next.md",
                "handoff_json": root / "handoff.json",
                "handoff_md": root / "handoff.md",
            }

            payload = refresh_operator_artifacts(
                python_exec="python",
                include_offline_audit=False,
                results_builder=lambda python_exec: {
                    "status": "in_progress",
                    "next_key": "followstop_lateral_enabled_canary",
                    "next_command": "python followstop",
                    "runbook_status": "ready",
                    "recommended_order": ["followstop_lateral_enabled_canary"],
                    "results": {
                        "followstop_lateral_enabled_canary": {
                            "status": "dry_run_only",
                            "reason": "needs online evidence",
                            "path": "",
                        }
                    },
                },
                next_action_builder=lambda: {
                    "status": "ready",
                    "primary": {"key": "followstop_lateral_enabled_canary", "command": "python followstop"},
                    "alternatives": [],
                },
                handoff_builder=lambda: {
                    "handoff_status": "ready_to_run_next_online",
                    "selected_next_action": {
                        "primary": {"key": "followstop_lateral_enabled_canary"},
                    },
                    "goal_status": {"overall_status": "in_progress", "blockers": []},
                    "broader_validation": {},
                    "preflight": {},
                    "demo_readiness": {},
                    "commands": {"next_online": "python followstop"},
                    "expected_after_online": {},
                    "operator_notes": [],
                },
                results_json=paths["results_json"],
                results_md=paths["results_md"],
                next_action_json=paths["next_json"],
                next_action_md=paths["next_md"],
                handoff_json=paths["handoff_json"],
                handoff_md=paths["handoff_md"],
            )

            self.assertEqual(payload["results"]["next_key"], "followstop_lateral_enabled_canary")
            self.assertEqual(payload["next_action"]["primary_key"], "followstop_lateral_enabled_canary")
            self.assertEqual(payload["handoff"]["selected_next_key"], "followstop_lateral_enabled_canary")
            self.assertEqual(payload["offline_audit"]["status"], "skipped")
            for path in paths.values():
                self.assertTrue(path.exists(), str(path))

    def test_refresh_operator_artifacts_can_write_offline_audit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            audit_json = root / "audit.json"
            audit_md = root / "audit.md"

            payload = refresh_operator_artifacts(
                python_exec="python",
                include_offline_audit=True,
                results_builder=lambda python_exec: {
                    "status": "in_progress",
                    "next_key": "followstop_lateral_enabled_canary",
                    "next_command": "python followstop",
                    "runbook_status": "ready",
                    "recommended_order": [],
                    "results": {},
                },
                next_action_builder=lambda: {"status": "ready", "primary": {"key": "followstop_lateral_enabled_canary"}},
                handoff_builder=lambda: {
                    "handoff_status": "ready_to_run_next_online",
                    "selected_next_action": {"primary": {"key": "followstop_lateral_enabled_canary"}},
                    "goal_status": {"overall_status": "in_progress", "blockers": []},
                    "broader_validation": {},
                    "preflight": {},
                    "demo_readiness": {},
                    "commands": {},
                    "expected_after_online": {},
                    "operator_notes": [],
                },
                offline_audit_builder=lambda: {
                    "audit_status": "offline_pass_goal_in_progress",
                    "python_exec": "python",
                    "goal_status": {
                        "overall_status": "in_progress",
                        "blockers": [],
                        "transport_ab": {"status": "missing"},
                        "transport_decision": {"status": "missing"},
                        "curve_recovery": {"status": "missing"},
                        "demo_recording": {"status": "missing"},
                        "startup_only_runs": {"status": "none", "count": 0},
                        "next_actions": [],
                        "next_command": {},
                        "recommended_commands": {},
                    },
                    "test_results": [],
                    "runbook_dry_run_verification": {"status": "skipped"},
                    "runbook_results": {},
                    "next_online_action": {
                        "status": "ready",
                        "focus": "auto",
                        "rationale": "test",
                        "primary": {"key": "followstop_lateral_enabled_canary"},
                    },
                },
                results_json=root / "results.json",
                results_md=root / "results.md",
                next_action_json=root / "next.json",
                next_action_md=root / "next.md",
                handoff_json=root / "handoff.json",
                handoff_md=root / "handoff.md",
                offline_audit_json=audit_json,
                offline_audit_md=audit_md,
            )

            self.assertEqual(payload["offline_audit"]["status"], "offline_pass_goal_in_progress")
            self.assertTrue(audit_json.exists())
            self.assertTrue(audit_md.exists())

    def test_persist_execution_writes_current_artifact_before_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            json_out = root / "execution.json"
            md_out = root / "execution.md"
            payload = {
                "execution_id": "exec-current",
                "status": "completed_one_step",
                "reason": "ran",
                "mode": "next",
                "start_at": "next",
                "initial_next_key": "followstop_lateral_enabled_canary",
                "final_next_key": "town01_ros2_gt_canary",
                "steps": [],
                "initial_results_snapshot": {"status": "in_progress", "results": {}},
                "final_results_snapshot": {"status": "in_progress", "results": {}},
            }
            observed: dict[str, str] = {}

            def refresher(**kwargs: Any) -> Dict[str, Any]:
                self.assertTrue(json_out.exists())
                current = json.loads(json_out.read_text(encoding="utf-8"))
                observed["execution_id_before_refresh"] = current["execution_id"]
                self.assertNotIn("post_run_refresh", current)
                return {
                    "results": {"status": "in_progress", "next_key": "town01_ros2_gt_canary"},
                    "next_action": {"status": "ready", "primary_key": "town01_ros2_gt_canary"},
                    "handoff": {"status": "ready_to_run_next_online", "selected_next_key": "town01_ros2_gt_canary"},
                    "offline_audit": {"status": "offline_pass_goal_in_progress"},
                }

            result = persist_execution_outputs(
                payload,
                json_out=json_out,
                md_out=md_out,
                python_exec="python",
                dry_run=False,
                refresh_after_run=True,
                include_offline_audit=True,
                run_offline_tests=False,
                include_after_online_intake=False,
                refresh_runner=refresher,
            )

            final_payload = json.loads(json_out.read_text(encoding="utf-8"))
            self.assertEqual(observed["execution_id_before_refresh"], "exec-current")
            self.assertEqual(final_payload["post_run_refresh"]["results"]["next_key"], "town01_ros2_gt_canary")
            self.assertIn("post_run_refresh", result)

    def test_persist_execution_dry_run_does_not_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            called = {"refresh": False}

            def refresher(**kwargs: Any) -> Dict[str, Any]:
                called["refresh"] = True
                return {}

            payload = {
                "status": "dry_run",
                "reason": "planned",
                "mode": "next",
                "start_at": "next",
                "initial_next_key": "followstop_lateral_enabled_canary",
                "final_next_key": "followstop_lateral_enabled_canary",
                "steps": [],
                "initial_results_snapshot": {"status": "in_progress", "results": {}},
                "final_results_snapshot": {"status": "in_progress", "results": {}},
            }
            persist_execution_outputs(
                payload,
                json_out=root / "execution.json",
                md_out=root / "execution.md",
                python_exec="python",
                dry_run=True,
                refresh_after_run=True,
                include_offline_audit=True,
                run_offline_tests=False,
                refresh_runner=refresher,
            )

            self.assertFalse(called["refresh"])

    def test_persist_execution_writes_after_online_intake(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            json_out = root / "execution.json"
            md_out = root / "execution.md"
            intake_json = root / "after.json"
            intake_md = root / "after.md"
            payload = {
                "execution_id": "exec-current",
                "status": "completed_one_step",
                "reason": "ran",
                "mode": "next",
                "start_at": "next",
                "initial_next_key": "followstop_lateral_enabled_canary",
                "final_next_key": "town01_ros2_gt_canary",
                "steps": [],
                "initial_results_snapshot": {"status": "in_progress", "results": {}},
                "final_results_snapshot": {"status": "in_progress", "results": {}},
            }

            def refresher(**kwargs: Any) -> Dict[str, Any]:
                return {
                    "results": {"status": "in_progress", "next_key": "town01_ros2_gt_canary"},
                    "next_action": {"status": "ready", "primary_key": "town01_ros2_gt_canary"},
                    "handoff": {"status": "ready_to_run_next_online", "selected_next_key": "town01_ros2_gt_canary"},
                    "offline_audit": {"status": "offline_pass_goal_in_progress"},
                }

            def after_online(**kwargs: Any) -> Dict[str, Any]:
                self.assertEqual(Path(kwargs["execution_path"]), json_out)
                return {
                    "status": "goal_in_progress",
                    "reason": "test",
                    "next_command": "python next",
                    "execution": {},
                    "runbook": {},
                    "goal": {},
                    "triage": {},
                    "demo_readiness": {},
                    "handoff": {},
                }

            result = persist_execution_outputs(
                payload,
                json_out=json_out,
                md_out=md_out,
                python_exec="python",
                dry_run=False,
                refresh_after_run=True,
                include_offline_audit=True,
                run_offline_tests=False,
                include_after_online_intake=True,
                after_online_json=intake_json,
                after_online_md=intake_md,
                after_online_builder=after_online,
                refresh_runner=refresher,
            )

            self.assertTrue(intake_json.exists())
            self.assertTrue(intake_md.exists())
            self.assertEqual(
                result["post_run_refresh"]["after_online_intake"]["status"],
                "goal_in_progress",
            )
            self.assertEqual(
                json.loads(json_out.read_text(encoding="utf-8"))["post_run_refresh"]["after_online_intake"]["next_command"],
                "python next",
            )


if __name__ == "__main__":
    unittest.main()
