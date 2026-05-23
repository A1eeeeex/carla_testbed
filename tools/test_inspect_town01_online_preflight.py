from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, Sequence

from tools.inspect_town01_online_preflight import build_preflight_payload, write_json, write_markdown


def _runner_no_matches(cmd: Sequence[str], timeout_s: float) -> Dict[str, Any]:
    return {"cmd": list(cmd), "returncode": 1, "stdout": "", "stderr": ""}


def _runner_with_carla_and_port(cmd: Sequence[str], timeout_s: float) -> Dict[str, Any]:
    if list(cmd)[:2] == ["pgrep", "-af"] and "CarlaUE4" in cmd[-1]:
        return {"cmd": list(cmd), "returncode": 0, "stdout": "123 /opt/CARLA/CarlaUE4-Linux-Shipping\n", "stderr": ""}
    if list(cmd)[:2] == ["ss", "-ltnp"]:
        return {"cmd": list(cmd), "returncode": 0, "stdout": "LISTEN 0 128 127.0.0.1:2000 0.0.0.0:*\n", "stderr": ""}
    return {"cmd": list(cmd), "returncode": 1, "stdout": "", "stderr": ""}


def _goal_status() -> Dict[str, Any]:
    return {
        "overall_status": "in_progress",
        "blockers": ["curve_recovery:pending_missing_candidate_curve"],
        "next_command": {
            "key": "direct_curve176_recovery_online",
            "command": f"{sys.executable} tools/run_town01_capability_online_chain.py --step curve_lane_follow:town01_rh_spawn176_goal061",
            "reason": "curve176 still needs recovery",
        },
    }


def _runbook_results(*, python_exec: str) -> Dict[str, Any]:
    return {
        "status": "in_progress",
        "next_key": "followstop_lateral_enabled_canary",
        "next_command": f"{python_exec} -m carla_testbed run --config followstop.yaml",
    }


def _socket_ok() -> Dict[str, Any]:
    return {"status": "ok", "detail": "socket allowed"}


def _socket_failed() -> Dict[str, Any]:
    return {"status": "failed", "detail": "PermissionError: Operation not permitted"}


class InspectTown01OnlinePreflightTests(unittest.TestCase):
    def test_preflight_ready_without_runtime_matches(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cfg = root / "direct.yaml"
            cfg.write_text("x: 1\n", encoding="utf-8")
            goal = root / "goal.json"
            goal.write_text(json.dumps(_goal_status()), encoding="utf-8")

            payload = build_preflight_payload(
                config_path=cfg,
                python_exec=sys.executable,
                repo_root=root,
                goal_status_json=goal,
                refresh_goal_status=False,
                command_runner=_runner_no_matches,
                min_disk_free_gb=0.0,
                min_mem_available_gb=0.0,
                runbook_results_builder=_runbook_results,
                socket_probe=_socket_ok,
            )

            self.assertEqual(payload["status"], "ready")
            self.assertEqual(payload["failed_count"], 0)
            self.assertEqual(payload["warning_count"], 0)
            self.assertEqual(payload["goal_status"]["next_command"]["key"], "direct_curve176_recovery_online")
            self.assertEqual(payload["broader_validation"]["next_key"], "followstop_lateral_enabled_canary")

    def test_preflight_warns_on_existing_carla_and_port(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cfg = root / "direct.yaml"
            cfg.write_text("x: 1\n", encoding="utf-8")
            goal = root / "goal.json"
            goal.write_text(json.dumps(_goal_status()), encoding="utf-8")

            payload = build_preflight_payload(
                config_path=cfg,
                python_exec=sys.executable,
                repo_root=root,
                goal_status_json=goal,
                refresh_goal_status=False,
                command_runner=_runner_with_carla_and_port,
                min_disk_free_gb=0.0,
                min_mem_available_gb=0.0,
                runbook_results_builder=_runbook_results,
                socket_probe=_socket_ok,
            )

            self.assertEqual(payload["status"], "ready_with_warnings")
            self.assertGreaterEqual(payload["warning_count"], 2)

    def test_preflight_fails_on_missing_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            goal = root / "goal.json"
            goal.write_text(json.dumps(_goal_status()), encoding="utf-8")

            payload = build_preflight_payload(
                config_path=root / "missing.yaml",
                python_exec=sys.executable,
                repo_root=root,
                goal_status_json=goal,
                refresh_goal_status=False,
                command_runner=_runner_no_matches,
                min_disk_free_gb=0.0,
                min_mem_available_gb=0.0,
                runbook_results_builder=_runbook_results,
                socket_probe=_socket_ok,
            )

            self.assertEqual(payload["status"], "failed")
            self.assertGreaterEqual(payload["failed_count"], 1)

    def test_preflight_fails_when_socket_creation_is_not_permitted(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cfg = root / "direct.yaml"
            cfg.write_text("x: 1\n", encoding="utf-8")
            goal = root / "goal.json"
            goal.write_text(json.dumps(_goal_status()), encoding="utf-8")

            payload = build_preflight_payload(
                config_path=cfg,
                python_exec=sys.executable,
                repo_root=root,
                goal_status_json=goal,
                refresh_goal_status=False,
                command_runner=_runner_no_matches,
                min_disk_free_gb=0.0,
                min_mem_available_gb=0.0,
                runbook_results_builder=_runbook_results,
                socket_probe=_socket_failed,
            )

            self.assertEqual(payload["status"], "failed")
            names = {item["name"]: item for item in payload["checks"]}
            self.assertEqual(names["tcp_socket_permission"]["status"], "failed")
            self.assertIn("Operation not permitted", names["tcp_socket_permission"]["detail"])

    def test_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            payload = {
                "status": "ready",
                "failed_count": 0,
                "warning_count": 0,
                "goal_status": _goal_status(),
                "broader_validation": {
                    "status": "in_progress",
                    "next_key": "followstop_lateral_enabled_canary",
                    "next_command": "python followstop.py",
                },
                "checks": [],
            }
            json_out = root / "preflight.json"
            md_out = root / "preflight.md"

            write_json(json_out, payload)
            write_markdown(md_out, payload)

            self.assertEqual(json.loads(json_out.read_text(encoding="utf-8"))["status"], "ready")
            self.assertIn("Town01 Online Preflight", md_out.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
