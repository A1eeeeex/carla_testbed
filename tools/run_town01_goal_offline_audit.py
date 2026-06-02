#!/usr/bin/env python3
# LEGACY / OPERATIONAL HELPER: retained for historical Town01 goal offline audit.
# Do not add new platform logic here; move reusable code into carla_testbed.analysis.
# Migration target: carla_testbed.analysis town01 goal audit modules.
from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.inspect_town01_goal_online_runbook_results import build_results_payload as build_runbook_results_payload
from tools.inspect_town01_goal_status import DEFAULT_TRANSPORT_AB_ROOT, build_goal_status, write_markdown as write_goal_markdown
from tools.inspect_town01_next_online_action import DEFAULT_OUTPUT_JSON as DEFAULT_NEXT_ONLINE_ACTION_JSON
from tools.verify_town01_runbook_dry_runs import (
    DEFAULT_OUTPUT_JSON as DEFAULT_RUNBOOK_DRY_RUN_JSON,
    DEFAULT_OUTPUT_MD as DEFAULT_RUNBOOK_DRY_RUN_MD,
)

DEFAULT_OUTPUT_JSON = REPO_ROOT / "artifacts" / "town01_goal_offline_audit_20260522.json"
DEFAULT_OUTPUT_MD = REPO_ROOT / "artifacts" / "town01_goal_offline_audit_20260522.md"

DEFAULT_TOOL_TEST_MODULES = (
    "tools.test_inspect_town01_goal_status",
    "tools.test_inspect_town01_goal_resume",
    "tools.test_inspect_town01_post_online_triage",
    "tools.test_inspect_town01_goal_online_runbook_results",
    "tools.test_inspect_town01_goal_online_runbook_execution",
    "tools.test_inspect_town01_next_online_action",
    "tools.test_inspect_town01_after_online",
    "tools.test_inspect_town01_goal_completion",
    "tools.test_inspect_town01_online_preflight",
    "tools.test_inspect_town01_demo_recording",
    "tools.test_inspect_town01_demo_readiness",
    "tools.test_town01_demo_showcase",
    "tools.test_dreamview_open_url",
    "tools.test_prepare_town01_operator_handoff",
    "tools.test_prepare_town01_goal_online_runbook",
    "tools.test_verify_town01_runbook_dry_runs",
    "tools.test_analyze_town01_direct_curve_recovery",
    "tools.test_analyze_town01_direct_divergence",
    "tools.test_analyze_town01_transport_ab",
    "tools.test_run_town01_direct_curve_recovery_retry",
    "tools.test_run_town01_direct_curve_pair_recovery_retry",
    "tools.test_run_town01_goal_sequence",
    "tools.test_run_town01_goal_online_runbook",
    "tools.test_carla_direct_transport",
    "tools.test_town01_transport_ab",
)


def _command_to_string(cmd: Sequence[str]) -> str:
    return " ".join(shlex.quote(str(item)) for item in cmd)


def _direct_curve_recovery_dry_run_cmd(python_exec: str, route_id: str) -> List[str]:
    return [
        python_exec,
        "tools/run_town01_direct_curve_recovery_retry.py",
        "--route-id",
        route_id,
        "--retries",
        "1",
        "--python-exec",
        python_exec,
        "--no-preflight",
        "--dry-run",
    ]


def _direct_curve_pair_recovery_dry_run_cmd(python_exec: str) -> List[str]:
    return [
        python_exec,
        "tools/run_town01_direct_curve_pair_recovery_retry.py",
        "--python-exec",
        python_exec,
        "--no-preflight",
        "--dry-run",
    ]


def _goal_sequence_dry_run_cmd(python_exec: str) -> List[str]:
    return [
        python_exec,
        "tools/run_town01_goal_sequence.py",
        "--python-exec",
        python_exec,
        "--dry-run",
    ]


def _post_online_triage_cmd(python_exec: str) -> List[str]:
    return [
        python_exec,
        "tools/inspect_town01_post_online_triage.py",
    ]


def _online_runbook_cmd(python_exec: str) -> List[str]:
    return [
        python_exec,
        "tools/prepare_town01_goal_online_runbook.py",
    ]


def _online_runbook_results_cmd(python_exec: str) -> List[str]:
    return [
        python_exec,
        "tools/inspect_town01_goal_online_runbook_results.py",
    ]


def _online_runbook_executor_dry_run_cmd(python_exec: str) -> List[str]:
    return [
        python_exec,
        "tools/run_town01_goal_online_runbook.py",
        "--python-exec",
        python_exec,
        "--dry-run",
    ]


def _online_runbook_execution_inspect_cmd(python_exec: str) -> List[str]:
    return [
        python_exec,
        "tools/inspect_town01_goal_online_runbook_execution.py",
        "--allow-dry-run-fallback",
    ]


def _next_online_action_cmd(python_exec: str) -> List[str]:
    return [
        python_exec,
        "tools/inspect_town01_next_online_action.py",
        "--python-exec",
        python_exec,
    ]


def _after_online_intake_cmd(python_exec: str) -> List[str]:
    return [
        python_exec,
        "tools/inspect_town01_after_online.py",
        "--python-exec",
        python_exec,
        "--allow-dry-run-fallback",
    ]


def _goal_completion_cmd(python_exec: str) -> List[str]:
    return [
        python_exec,
        "tools/inspect_town01_goal_completion.py",
        "--python-exec",
        python_exec,
    ]


def _runbook_dry_run_verification_cmd(python_exec: str) -> List[str]:
    return [
        python_exec,
        "tools/verify_town01_runbook_dry_runs.py",
        "--python-exec",
        python_exec,
        "--json-out",
        str(DEFAULT_RUNBOOK_DRY_RUN_JSON),
        "--md-out",
        str(DEFAULT_RUNBOOK_DRY_RUN_MD),
        "--require-passed",
    ]


def default_command_specs(python_exec: str) -> List[Dict[str, Any]]:
    return [
        {
            "name": "town01_tools_unittest",
            "kind": "unittest",
            "cmd": [python_exec, "-m", "unittest", *DEFAULT_TOOL_TEST_MODULES],
        },
        {
            "name": "pytest_no_runtime",
            "kind": "pytest",
            "cmd": [
                python_exec,
                "-m",
                "pytest",
                "-m",
                "not carla and not apollo and not integration",
                "-q",
            ],
        },
        {
            "name": "town01_online_preflight",
            "kind": "preflight",
            "cmd": [
                python_exec,
                "tools/inspect_town01_online_preflight.py",
                "--json-out",
                "artifacts/town01_online_preflight_20260522.json",
                "--md-out",
                "artifacts/town01_online_preflight_20260522.md",
            ],
        },
        {
            "name": "town01_goal_resume",
            "kind": "resume",
            "cmd": [
                python_exec,
                "tools/inspect_town01_goal_resume.py",
                "--json-out",
                "artifacts/town01_goal_resume_20260522.json",
                "--md-out",
                "artifacts/town01_goal_resume_20260522.md",
            ],
        },
        {
            "name": "town01_operator_handoff",
            "kind": "handoff",
            "cmd": [
                python_exec,
                "tools/prepare_town01_operator_handoff.py",
                "--json-out",
                "artifacts/town01_operator_handoff_20260522.json",
                "--md-out",
                "artifacts/town01_operator_handoff_20260522.md",
            ],
        },
        {
            "name": "town01_goal_online_runbook",
            "kind": "online_runbook",
            "cmd": _online_runbook_cmd(python_exec),
        },
        {
            "name": "town01_goal_online_runbook_results",
            "kind": "online_runbook_results",
            "cmd": _online_runbook_results_cmd(python_exec),
        },
        {
            "name": "town01_goal_online_runbook_executor_dry_run",
            "kind": "online_runbook_executor_dry_run",
            "cmd": _online_runbook_executor_dry_run_cmd(python_exec),
        },
        {
            "name": "town01_goal_online_runbook_execution_inspect",
            "kind": "online_runbook_execution_inspect",
            "cmd": _online_runbook_execution_inspect_cmd(python_exec),
        },
        {
            "name": "town01_next_online_action",
            "kind": "next_online_action",
            "cmd": _next_online_action_cmd(python_exec),
        },
        {
            "name": "town01_after_online_intake",
            "kind": "after_online_intake",
            "cmd": _after_online_intake_cmd(python_exec),
        },
        {
            "name": "town01_goal_completion_audit",
            "kind": "goal_completion_audit",
            "cmd": _goal_completion_cmd(python_exec),
        },
        {
            "name": "town01_runbook_heavy_dry_run_verification",
            "kind": "runbook_dry_run",
            "cmd": _runbook_dry_run_verification_cmd(python_exec),
        },
        {
            "name": "town01_demo_readiness_relaxed",
            "kind": "demo_readiness",
            "cmd": [
                python_exec,
                "tools/inspect_town01_demo_readiness.py",
                "--browser-cmd",
                "playwright",
                "--no-require-browser",
                "--no-require-video-encoder",
                "--capture-mode",
                "ffmpeg_realtime",
            ],
        },
        {
            "name": "direct_curve176_recovery_dry_run",
            "kind": "online_chain_dry_run",
            "cmd": _direct_curve_recovery_dry_run_cmd(python_exec, "town01_rh_spawn176_goal061"),
        },
        {
            "name": "direct_curve177_recovery_dry_run",
            "kind": "online_chain_dry_run",
            "cmd": _direct_curve_recovery_dry_run_cmd(python_exec, "town01_rh_spawn177_goal052"),
        },
        {
            "name": "direct_curve_pair_recovery_dry_run",
            "kind": "online_chain_dry_run",
            "cmd": _direct_curve_pair_recovery_dry_run_cmd(python_exec),
        },
        {
            "name": "town01_goal_sequence_dry_run",
            "kind": "online_sequence_dry_run",
            "cmd": _goal_sequence_dry_run_cmd(python_exec),
        },
        {
            "name": "town01_post_online_triage",
            "kind": "post_online_triage",
            "cmd": _post_online_triage_cmd(python_exec),
        },
        {
            "name": "town01_demo_showcase_dry_run",
            "kind": "demo_dry_run",
            "cmd": [
                python_exec,
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
                "--dry-run",
            ],
        },
    ]


def run_command(cmd: Sequence[str], *, cwd: Path, timeout_s: float) -> Dict[str, Any]:
    started = time.time()
    try:
        proc = subprocess.run(
            list(cmd),
            cwd=str(cwd),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout_s,
            check=False,
        )
        elapsed = time.time() - started
        return {
            "cmd": _command_to_string(cmd),
            "returncode": proc.returncode,
            "elapsed_s": elapsed,
            "stdout_tail": proc.stdout[-4000:],
            "stderr_tail": proc.stderr[-4000:],
            "status": "passed" if proc.returncode == 0 else "failed",
        }
    except subprocess.TimeoutExpired as exc:
        elapsed = time.time() - started
        return {
            "cmd": _command_to_string(cmd),
            "returncode": None,
            "elapsed_s": elapsed,
            "stdout_tail": (exc.stdout or "")[-4000:] if isinstance(exc.stdout, str) else "",
            "stderr_tail": (exc.stderr or "")[-4000:] if isinstance(exc.stderr, str) else "",
            "status": "timeout",
        }


CommandRunner = Callable[[Sequence[str], Path, float], Dict[str, Any]]
RunbookResultsBuilder = Callable[[str], Dict[str, Any]]


def inspect_runbook_dry_run_verification(path: Path, *, required: bool) -> Dict[str, Any]:
    if not required:
        return {
            "status": "skipped",
            "required": False,
            "path": str(path),
            "reason": "offline tests were skipped or custom command specs were supplied",
        }
    if not path.exists():
        return {
            "status": "missing",
            "required": True,
            "path": str(path),
            "reason": "runbook dry-run verification artifact is missing",
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "status": "invalid_json",
            "required": True,
            "path": str(path),
            "reason": repr(exc),
        }
    raw_status = str(payload.get("status") or "")
    status = "passed" if raw_status == "passed" else "failed"
    return {
        "status": status,
        "required": True,
        "path": str(path),
        "raw_status": raw_status,
        "python_exec": payload.get("python_exec"),
        "passed_count": payload.get("passed_count", 0),
        "failed_count": payload.get("failed_count", 0),
        "checked_keys": payload.get("checked_keys", []),
        "result_count": len(payload.get("results") or []),
    }


def compact_runbook_results(payload: Dict[str, Any]) -> Dict[str, Any]:
    next_key = str(payload.get("next_key") or "")
    results = payload.get("results") if isinstance(payload.get("results"), dict) else {}
    next_item = results.get(next_key) if isinstance(results.get(next_key), dict) else {}
    return {
        "status": str(payload.get("status") or ""),
        "next_key": next_key,
        "next_command": str(payload.get("next_command") or ""),
        "next_status": str(next_item.get("status") or ""),
        "next_reason": str(next_item.get("reason") or ""),
        "runbook_status": str(payload.get("runbook_status") or ""),
        "recommended_order": payload.get("recommended_order") or [],
    }


def compact_next_online_action(payload: Dict[str, Any]) -> Dict[str, Any]:
    primary = payload.get("primary") if isinstance(payload.get("primary"), dict) else {}
    alternatives = [item for item in payload.get("alternatives", []) if isinstance(item, dict)]
    return {
        "status": str(payload.get("status") or ""),
        "focus": str(payload.get("focus") or ""),
        "rationale": str(payload.get("rationale") or ""),
        "primary": {
            "key": str(primary.get("key") or ""),
            "title": str(primary.get("title") or ""),
            "status": str(primary.get("status") or ""),
            "reason": str(primary.get("reason") or ""),
            "command": str(primary.get("command") or ""),
        },
        "alternatives": [
            {
                "key": str(item.get("key") or ""),
                "title": str(item.get("title") or ""),
                "status": str(item.get("status") or ""),
                "reason": str(item.get("reason") or ""),
                "command": str(item.get("command") or ""),
            }
            for item in alternatives
        ],
    }


def synthesize_next_online_action(
    *,
    runbook_results: Dict[str, Any],
    goal_status: Dict[str, Any],
) -> Dict[str, Any]:
    direct_next = goal_status.get("next_command") if isinstance(goal_status.get("next_command"), dict) else {}
    primary = {
        "key": str(runbook_results.get("next_key") or ""),
        "title": "broader_validation",
        "status": str(runbook_results.get("next_status") or runbook_results.get("status") or ""),
        "reason": str(runbook_results.get("next_reason") or "next incomplete broader validation gate"),
        "command": str(runbook_results.get("next_command") or ""),
    }
    direct = {
        "key": str(direct_next.get("key") or ""),
        "title": "direct_chain",
        "status": str((goal_status.get("curve_recovery") or {}).get("status") or goal_status.get("overall_status") or ""),
        "reason": str(direct_next.get("reason") or ""),
        "command": str(direct_next.get("command") or ""),
    }
    if not primary["command"] and direct["command"]:
        primary = direct
        alternatives: List[Dict[str, str]] = []
        rationale = "broader validation command is unavailable; direct-chain gate is next"
    else:
        alternatives = [direct] if direct["command"] else []
        rationale = "broader validation remains the first selected online gate"
    return {
        "status": "ready" if primary.get("command") else "missing_command",
        "focus": "offline_audit",
        "rationale": rationale,
        "primary": primary,
        "alternatives": alternatives,
    }


def inspect_next_online_action_artifact(
    path: Path,
    *,
    runbook_results: Dict[str, Any],
    goal_status: Dict[str, Any],
) -> Dict[str, Any]:
    if not path.exists():
        return synthesize_next_online_action(runbook_results=runbook_results, goal_status=goal_status)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        fallback = synthesize_next_online_action(runbook_results=runbook_results, goal_status=goal_status)
        fallback["status"] = "artifact_invalid_json"
        fallback["artifact_error"] = repr(exc)
        fallback["artifact_path"] = str(path)
        return fallback
    compact = compact_next_online_action(payload)
    compact["artifact_path"] = str(path)
    return compact


def build_audit_payload(
    *,
    python_exec: str = sys.executable,
    cwd: Path = REPO_ROOT,
    run_tests: bool = True,
    timeout_s: float = 120.0,
    command_specs: Sequence[Dict[str, Any]] | None = None,
    command_runner: CommandRunner | None = None,
    transport_ab_root: Path = DEFAULT_TRANSPORT_AB_ROOT,
    runs_root: Path = REPO_ROOT / "runs",
    curve_recovery_json: Path | None = None,
    demo_root: Path | None = None,
    auto_demo_root: bool = True,
    runbook_dry_run_json: Path = DEFAULT_RUNBOOK_DRY_RUN_JSON,
    require_runbook_dry_run: bool | None = None,
    runbook_results_builder: RunbookResultsBuilder | None = None,
    next_online_action_json: Path = DEFAULT_NEXT_ONLINE_ACTION_JSON,
) -> Dict[str, Any]:
    using_default_command_specs = command_specs is None
    specs = list(command_specs or default_command_specs(python_exec))
    runner = command_runner or (lambda cmd, root, timeout: run_command(cmd, cwd=root, timeout_s=timeout))
    test_results: List[Dict[str, Any]] = []
    if run_tests:
        for spec in specs:
            result = runner(list(spec["cmd"]), cwd, timeout_s)
            result["name"] = spec["name"]
            result["kind"] = spec.get("kind", "")
            test_results.append(result)
    else:
        test_results = [
            {
                "name": spec["name"],
                "kind": spec.get("kind", ""),
                "cmd": _command_to_string(spec["cmd"]),
                "status": "skipped",
            }
            for spec in specs
        ]

    goal_status = build_goal_status(
        transport_ab_root=transport_ab_root,
        curve_recovery_json=curve_recovery_json,
        runs_root=runs_root,
        demo_root=demo_root,
        auto_demo_root=auto_demo_root,
    )
    runbook_results_raw = (
        runbook_results_builder(str(python_exec))
        if runbook_results_builder is not None
        else build_runbook_results_payload(python_exec=str(python_exec))
    )
    runbook_results = compact_runbook_results(runbook_results_raw)
    next_online_action = inspect_next_online_action_artifact(
        next_online_action_json,
        runbook_results=runbook_results,
        goal_status=goal_status,
    )
    runbook_dry_run_required = (
        bool(run_tests and using_default_command_specs)
        if require_runbook_dry_run is None
        else bool(require_runbook_dry_run)
    )
    runbook_dry_run_verification = inspect_runbook_dry_run_verification(
        runbook_dry_run_json,
        required=runbook_dry_run_required,
    )
    failed_tests = [result for result in test_results if result.get("status") not in {"passed", "skipped"}]
    runbook_dry_run_failed = (
        runbook_dry_run_verification.get("required") is True
        and runbook_dry_run_verification.get("status") != "passed"
    )
    if failed_tests or runbook_dry_run_failed:
        audit_status = "offline_validation_failed"
    elif goal_status.get("overall_status") == "ready_for_final_review":
        audit_status = "offline_pass_goal_ready"
    else:
        audit_status = "offline_pass_goal_in_progress"
    return {
        "audit_status": audit_status,
        "python_exec": python_exec,
        "cwd": str(cwd.expanduser().resolve()),
        "run_tests": bool(run_tests),
        "test_results": test_results,
        "runbook_dry_run_verification": runbook_dry_run_verification,
        "runbook_results": runbook_results,
        "next_online_action": next_online_action,
        "goal_status": goal_status,
    }


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_markdown(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Town01 Goal Offline Audit",
        "",
        f"- audit_status: `{payload['audit_status']}`",
        f"- python_exec: `{payload['python_exec']}`",
        f"- goal_status: `{payload['goal_status']['overall_status']}`",
        f"- next_online_action: `{((payload.get('next_online_action') or {}).get('primary') or {}).get('key', '')}`",
        f"- runbook_next_key: `{(payload.get('runbook_results') or {}).get('next_key', '')}`",
        f"- runbook_dry_run: `{payload.get('runbook_dry_run_verification', {}).get('status', '')}`",
        f"- goal_blockers: `{', '.join(payload['goal_status'].get('blockers', [])) or 'none'}`",
        "",
        "## Offline Checks",
        "",
        "| name | kind | status | returncode | elapsed_s |",
        "|---|---|---|---:|---:|",
    ]
    for result in payload.get("test_results", []):
        elapsed = result.get("elapsed_s")
        elapsed_cell = "" if elapsed is None else f"{float(elapsed):.2f}"
        returncode = result.get("returncode")
        returncode_cell = "" if returncode is None else str(returncode)
        lines.append(
            "| `{}` | `{}` | `{}` | {} | {} |".format(
                result.get("name", ""),
                result.get("kind", ""),
                result.get("status", ""),
                returncode_cell,
                elapsed_cell,
            )
        )
    runbook_dry_run = payload.get("runbook_dry_run_verification", {})
    lines.extend(
        [
            "",
            "## Runbook Heavy Dry-Run Verification",
            "",
            f"- status: `{runbook_dry_run.get('status', '')}`",
            f"- required: `{runbook_dry_run.get('required', False)}`",
            f"- path: `{runbook_dry_run.get('path', '')}`",
            f"- passed_count: `{runbook_dry_run.get('passed_count', '')}`",
            f"- failed_count: `{runbook_dry_run.get('failed_count', '')}`",
        ]
    )
    if runbook_dry_run.get("reason"):
        lines.append(f"- reason: `{runbook_dry_run.get('reason')}`")
    lines.extend(
        [
            "",
            "## Selected Next Online Action",
            "",
            f"- status: `{(payload.get('next_online_action') or {}).get('status', '')}`",
            f"- focus: `{(payload.get('next_online_action') or {}).get('focus', '')}`",
            f"- rationale: `{(payload.get('next_online_action') or {}).get('rationale', '')}`",
            f"- key: `{((payload.get('next_online_action') or {}).get('primary') or {}).get('key', '')}`",
            f"- title: `{((payload.get('next_online_action') or {}).get('primary') or {}).get('title', '')}`",
            f"- command_status: `{((payload.get('next_online_action') or {}).get('primary') or {}).get('status', '')}`",
            "",
            "```bash",
            str(((payload.get("next_online_action") or {}).get("primary") or {}).get("command", "")),
            "```",
            "",
            "## Goal Summary",
            "",
            f"- transport_ab: `{payload['goal_status']['transport_ab']['status']}`",
            f"- transport_decision: `{payload['goal_status'].get('transport_decision', {}).get('status', '')}`",
            f"- curve_recovery: `{payload['goal_status']['curve_recovery']['status']}`",
            f"- demo_recording: `{payload['goal_status']['demo_recording']['status']}`",
            "",
            "## Broader Validation Next Command",
            "",
            f"- key: `{(payload.get('runbook_results') or {}).get('next_key', '')}`",
            f"- status: `{(payload.get('runbook_results') or {}).get('next_status', '')}`",
            f"- reason: `{(payload.get('runbook_results') or {}).get('next_reason', '')}`",
            "",
            "```bash",
            str((payload.get("runbook_results") or {}).get("next_command", "")),
            "```",
            "",
            "## Direct Chain Next Command",
            "",
            f"- key: `{(payload['goal_status'].get('next_command') or {}).get('key', '')}`",
            f"- reason: `{(payload['goal_status'].get('next_command') or {}).get('reason', '')}`",
            "",
            "```bash",
            str((payload["goal_status"].get("next_command") or {}).get("command", "")),
            "```",
            "",
            "## Next Actions",
            "",
        ]
    )
    for action in payload["goal_status"].get("next_actions", []):
        lines.append(f"- {action}")
    lines.extend(["", "## Embedded Goal Runbook", ""])
    tmp_goal_md = path.with_suffix(".goal.tmp.md")
    write_goal_markdown(tmp_goal_md, payload["goal_status"])
    try:
        lines.append(tmp_goal_md.read_text(encoding="utf-8"))
    finally:
        try:
            tmp_goal_md.unlink()
        except OSError:
            pass
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def exit_code_for_audit(payload: Dict[str, Any], *, require_goal_ready: bool) -> int:
    if payload.get("audit_status") == "offline_validation_failed":
        return 1
    if require_goal_ready and payload.get("audit_status") != "offline_pass_goal_ready":
        return 2
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run offline Town01 goal validation without launching CARLA/Apollo.")
    parser.add_argument("--python-exec", default=sys.executable)
    parser.add_argument("--timeout-sec", type=float, default=120.0)
    parser.add_argument("--skip-tests", action="store_true")
    parser.add_argument(
        "--require-goal-ready",
        action="store_true",
        help="Exit with code 2 unless offline checks pass and the full goal is ready.",
    )
    parser.add_argument("--transport-ab-root", type=Path, default=DEFAULT_TRANSPORT_AB_ROOT)
    parser.add_argument("--runs-root", type=Path, default=REPO_ROOT / "runs")
    parser.add_argument("--curve-recovery-json", type=Path, default=None)
    parser.add_argument("--demo-root", type=Path, default=None)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_OUTPUT_MD)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    payload = build_audit_payload(
        python_exec=str(args.python_exec),
        cwd=REPO_ROOT,
        run_tests=not bool(args.skip_tests),
        timeout_s=float(args.timeout_sec),
        transport_ab_root=args.transport_ab_root,
        runs_root=args.runs_root,
        curve_recovery_json=args.curve_recovery_json,
        demo_root=args.demo_root,
        auto_demo_root=True,
    )
    write_json(args.json_out, payload)
    write_markdown(args.md_out, payload)
    print(json.dumps({"audit_status": payload["audit_status"], "goal_status": payload["goal_status"]["overall_status"]}, indent=2))
    raise SystemExit(exit_code_for_audit(payload, require_goal_ready=bool(args.require_goal_ready)))


if __name__ == "__main__":
    main()
