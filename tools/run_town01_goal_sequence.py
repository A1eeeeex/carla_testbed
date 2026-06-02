#!/usr/bin/env python3
# LEGACY / OPERATIONAL HELPER: retained for historical Town01 goal sequence orchestration.
# Do not add new platform logic here; move reusable code into carla_testbed.experiments.
# Migration target: carla_testbed.experiments natural driving runner.
from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.inspect_town01_demo_readiness import build_readiness_payload
from tools.inspect_town01_goal_status import CONDA_CARLA16_PYTHON, build_goal_status

DEFAULT_OUTPUT_JSON = REPO_ROOT / "artifacts" / "town01_goal_sequence_20260522.json"
DEFAULT_OUTPUT_MD = REPO_ROOT / "artifacts" / "town01_goal_sequence_20260522.md"
DEFAULT_DRY_RUN_OUTPUT_JSON = REPO_ROOT / "artifacts" / "town01_goal_sequence_dry_run_20260522.json"
DEFAULT_DRY_RUN_OUTPUT_MD = REPO_ROOT / "artifacts" / "town01_goal_sequence_dry_run_20260522.md"

GoalBuilder = Callable[[], Dict[str, Any]]
ReadinessBuilder = Callable[[], Dict[str, Any]]
CommandRunner = Callable[[Sequence[str]], int]


def default_output_paths(*, dry_run: bool = False) -> tuple[Path, Path]:
    if dry_run:
        return DEFAULT_DRY_RUN_OUTPUT_JSON, DEFAULT_DRY_RUN_OUTPUT_MD
    return DEFAULT_OUTPUT_JSON, DEFAULT_OUTPUT_MD


def build_pair_gate_command(*, python_exec: str) -> List[str]:
    return [
        str(Path(python_exec).expanduser()),
        "tools/run_town01_direct_curve_pair_recovery_retry.py",
        "--python-exec",
        str(python_exec),
    ]


def build_demo_recording_command(*, python_exec: str) -> List[str]:
    return [
        str(Path(python_exec).expanduser()),
        "tools/run_town01_demo_showcase.py",
        "--mode",
        "short",
        "--record-dreamview",
        "--dreamview-auto-open",
        "--dreamview-open-wait-page",
        "--dreamview-browser-cmd",
        "playwright",
        "--dreamview-capture-mode",
        "tick_snapshot",
        "--dreamview-capture-region",
        "1280x720+0,0",
        "--dreamview-use-fixed-region",
        "--require-recording-ready",
    ]


def build_offline_audit_command(*, python_exec: str, require_goal_ready: bool = False) -> List[str]:
    cmd = [
        str(Path(python_exec).expanduser()),
        "tools/run_town01_goal_offline_audit.py",
        "--python-exec",
        str(python_exec),
    ]
    if require_goal_ready:
        cmd.append("--require-goal-ready")
    return cmd


def _subprocess_runner(cmd: Sequence[str]) -> int:
    return int(subprocess.run(list(cmd), cwd=str(REPO_ROOT), check=False).returncode)


def _brief_goal(goal: Dict[str, Any]) -> Dict[str, Any]:
    next_command = goal.get("next_command") if isinstance(goal.get("next_command"), dict) else {}
    curve = goal.get("curve_recovery") if isinstance(goal.get("curve_recovery"), dict) else {}
    demo = goal.get("demo_recording") if isinstance(goal.get("demo_recording"), dict) else {}
    decision = goal.get("transport_decision") if isinstance(goal.get("transport_decision"), dict) else {}
    return {
        "overall_status": str(goal.get("overall_status") or ""),
        "blockers": goal.get("blockers") or [],
        "next_key": str(next_command.get("key") or ""),
        "next_reason": str(next_command.get("reason") or ""),
        "curve_status": str(curve.get("status") or ""),
        "curve_recovery_verdict": str(curve.get("recovery_verdict") or ""),
        "transport_decision": str(decision.get("status") or ""),
        "transport_promotable": bool(decision.get("transport_promotable", False)),
        "demo_status": str(demo.get("status") or ""),
        "demo_required": bool(demo.get("required", False)),
    }


def _should_run_pair_gate(goal: Dict[str, Any]) -> bool:
    curve = goal.get("curve_recovery") if isinstance(goal.get("curve_recovery"), dict) else {}
    next_command = goal.get("next_command") if isinstance(goal.get("next_command"), dict) else {}
    return str(curve.get("status") or "") != "decisive" and str(next_command.get("key") or "").startswith("direct_curve")


def _should_run_demo(goal: Dict[str, Any], readiness: Dict[str, Any], *, record_demo_when_ready: bool) -> tuple[bool, str]:
    if not record_demo_when_ready:
        return False, "record_demo_when_ready disabled"
    curve = goal.get("curve_recovery") if isinstance(goal.get("curve_recovery"), dict) else {}
    demo = goal.get("demo_recording") if isinstance(goal.get("demo_recording"), dict) else {}
    if str(curve.get("status") or "") != "decisive":
        return False, "curve recovery is not decisive"
    if not bool(demo.get("required", False)):
        return False, "demo recording is not required by current goal"
    if str(demo.get("status") or "") == "ready":
        return False, "demo recording is already ready"
    if str(readiness.get("status") or "") != "ready":
        return False, f"demo readiness is {readiness.get('status', '')}"
    return True, "curve gate is decisive and demo recording readiness is ready"


def run_sequence(
    *,
    python_exec: str = CONDA_CARLA16_PYTHON,
    dry_run: bool = False,
    record_demo_when_ready: bool = True,
    final_audit: bool = True,
    goal_builder: GoalBuilder = build_goal_status,
    readiness_builder: ReadinessBuilder = build_readiness_payload,
    command_runner: CommandRunner = _subprocess_runner,
) -> Dict[str, Any]:
    steps: List[Dict[str, Any]] = []
    initial_goal = goal_builder()
    current_goal = initial_goal

    if _should_run_pair_gate(current_goal):
        command = build_pair_gate_command(python_exec=python_exec)
        step = {
            "name": "direct_curve_pair_gate",
            "reason": "curve recovery is not decisive",
            "command": command,
            "status": "planned" if dry_run else "pending",
            "returncode": None,
            "goal_before": _brief_goal(current_goal),
        }
        if not dry_run:
            step["returncode"] = command_runner(command)
            step["status"] = "passed" if step["returncode"] == 0 else "failed"
            current_goal = goal_builder()
            step["goal_after"] = _brief_goal(current_goal)
            steps.append(step)
            if step["returncode"] != 0 and _should_run_pair_gate(current_goal):
                return {
                    "status": "stopped_after_curve_gate",
                    "reason": "direct curve pair gate returned non-zero",
                    "initial_goal": _brief_goal(initial_goal),
                    "final_goal": _brief_goal(current_goal),
                    "steps": steps,
                }
        else:
            steps.append(step)

    readiness = readiness_builder()
    should_demo, demo_reason = _should_run_demo(
        current_goal,
        readiness,
        record_demo_when_ready=record_demo_when_ready,
    )
    demo_command = build_demo_recording_command(python_exec=python_exec)
    demo_step = {
        "name": "demo_recording",
        "reason": demo_reason,
        "command": demo_command,
        "status": "skipped",
        "returncode": None,
        "readiness": {
            "status": readiness.get("status", ""),
            "failed_count": readiness.get("failed_count", 0),
            "warning_count": readiness.get("warning_count", 0),
        },
    }
    if should_demo:
        demo_step["status"] = "planned" if dry_run else "pending"
        if not dry_run:
            demo_step["returncode"] = command_runner(demo_command)
            demo_step["status"] = "passed" if demo_step["returncode"] == 0 else "failed"
            current_goal = goal_builder()
            demo_step["goal_after"] = _brief_goal(current_goal)
    steps.append(demo_step)
    if not dry_run and demo_step.get("returncode") not in {None, 0}:
        return {
            "status": "stopped_after_demo_recording",
            "reason": "demo recording returned non-zero",
            "initial_goal": _brief_goal(initial_goal),
            "final_goal": _brief_goal(current_goal),
            "steps": steps,
        }

    if final_audit:
        command = build_offline_audit_command(python_exec=python_exec, require_goal_ready=False)
        audit_step = {
            "name": "offline_audit",
            "reason": "refresh goal status and artifacts after online sequence",
            "command": command,
            "status": "planned" if dry_run else "pending",
            "returncode": None,
        }
        if not dry_run:
            audit_step["returncode"] = command_runner(command)
            audit_step["status"] = "passed" if audit_step["returncode"] == 0 else "failed"
            current_goal = goal_builder()
            audit_step["goal_after"] = _brief_goal(current_goal)
        steps.append(audit_step)

    final_goal = current_goal
    if dry_run:
        status = "dry_run"
        reason = "commands were planned but not executed"
    elif _brief_goal(final_goal).get("overall_status") == "ready_for_final_review":
        status = "completed_goal_ready"
        reason = "goal status is ready_for_final_review"
    else:
        status = "completed_goal_in_progress"
        reason = "sequence finished, but goal still has blockers"
    return {
        "status": status,
        "reason": reason,
        "initial_goal": _brief_goal(initial_goal),
        "final_goal": _brief_goal(final_goal),
        "steps": steps,
    }


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_markdown(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Town01 Goal Online Sequence",
        "",
        f"- status: `{payload.get('status', '')}`",
        f"- reason: `{payload.get('reason', '')}`",
        f"- initial_goal: `{(payload.get('initial_goal') or {}).get('overall_status', '')}`",
        f"- final_goal: `{(payload.get('final_goal') or {}).get('overall_status', '')}`",
        f"- final_blockers: `{', '.join((payload.get('final_goal') or {}).get('blockers') or []) or 'none'}`",
        "",
        "| step | status | returncode | reason |",
        "|---|---|---:|---|",
    ]
    for step in payload.get("steps") or []:
        returncode = step.get("returncode")
        lines.append(
            "| `{}` | `{}` | {} | `{}` |".format(
                step.get("name", ""),
                step.get("status", ""),
                "" if returncode is None else str(returncode),
                step.get("reason", ""),
            )
        )
    lines.extend(["", "## Commands", ""])
    for step in payload.get("steps") or []:
        lines.extend(
            [
                f"### {step.get('name', '')}",
                "",
                "```bash",
                " ".join(shlex.quote(str(part)) for part in step.get("command") or []),
                "```",
                "",
            ]
        )
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def exit_code_for_payload(payload: Dict[str, Any], *, require_goal_ready: bool = False) -> int:
    if payload.get("status") in {"dry_run", "completed_goal_ready", "completed_goal_in_progress"}:
        if require_goal_ready and payload.get("status") != "completed_goal_ready":
            return 2
        return 0
    return 2


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the current Town01 online goal sequence: direct curve gate, then demo recording when the gate is ready."
    )
    parser.add_argument("--python-exec", default=CONDA_CARLA16_PYTHON)
    parser.add_argument("--record-demo-when-ready", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--final-audit", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--require-goal-ready", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json-out", type=Path, default=None)
    parser.add_argument("--md-out", type=Path, default=None)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    payload = run_sequence(
        python_exec=str(args.python_exec),
        dry_run=bool(args.dry_run),
        record_demo_when_ready=bool(args.record_demo_when_ready),
        final_audit=bool(args.final_audit),
    )
    default_json, default_md = default_output_paths(dry_run=bool(args.dry_run))
    json_out = args.json_out or default_json
    md_out = args.md_out or default_md
    write_json(json_out, payload)
    write_markdown(md_out, payload)
    print(json.dumps({"status": payload["status"], "reason": payload.get("reason", "")}, indent=2))
    raise SystemExit(exit_code_for_payload(payload, require_goal_ready=bool(args.require_goal_ready)))


if __name__ == "__main__":
    main()
