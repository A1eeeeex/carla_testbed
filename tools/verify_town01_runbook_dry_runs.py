#!/usr/bin/env python3
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

from tools.inspect_town01_goal_status import CONDA_CARLA16_PYTHON
from tools.prepare_town01_goal_online_runbook import build_runbook_payload

DEFAULT_OUTPUT_JSON = REPO_ROOT / "artifacts" / "town01_runbook_dry_run_verification_20260522.json"
DEFAULT_OUTPUT_MD = REPO_ROOT / "artifacts" / "town01_runbook_dry_run_verification_20260522.md"

CommandRunner = Callable[[Sequence[str], float], Dict[str, Any]]

DEFAULT_HEAVY_KEYS = [
    "followstop_lateral_enabled_canary",
    "town01_ros2_gt_canary",
    "town01_direct_goal_sequence",
    "town01_transport_ab_canonical",
    "town01_demo_recording",
]


def _shell(cmd: Sequence[str]) -> str:
    return " ".join(shlex.quote(str(part)) for part in cmd)


def _run_command(cmd: Sequence[str], timeout_s: float) -> Dict[str, Any]:
    try:
        completed = subprocess.run(
            list(cmd),
            cwd=str(REPO_ROOT),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=max(float(timeout_s), 1.0),
            check=False,
        )
        return {
            "returncode": int(completed.returncode),
            "stdout_tail": completed.stdout[-8000:],
            "stderr_tail": completed.stderr[-8000:],
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "returncode": None,
            "stdout_tail": str(exc.stdout or "")[-8000:],
            "stderr_tail": f"timeout after {timeout_s:.1f}s\n{str(exc.stderr or '')[-8000:]}",
        }
    except Exception as exc:
        return {
            "returncode": None,
            "stdout_tail": "",
            "stderr_tail": repr(exc),
        }


def _replace_arg_value(cmd: Sequence[str], flag: str, value: str) -> List[str]:
    updated = list(cmd)
    for index, token in enumerate(updated):
        if token == flag and index + 1 < len(updated):
            updated[index + 1] = value
            return updated
    updated.extend([flag, value])
    return updated


def _append_flag_once(cmd: Sequence[str], flag: str) -> List[str]:
    updated = list(cmd)
    if flag not in updated:
        updated.append(flag)
    return updated


def dry_run_command_for(item: Dict[str, Any], *, run_root: Path = REPO_ROOT / "runs") -> List[str]:
    key = str(item.get("key") or "")
    cmd = [str(part) for part in list(item.get("command") or [])]
    if key == "followstop_lateral_enabled_canary":
        cmd = _replace_arg_value(cmd, "--run-dir", str(run_root / "followstop_goal_lateral_enabled_canary_check"))
    return _append_flag_once(cmd, "--dry-run")


def build_verification_payload(
    *,
    python_exec: str = CONDA_CARLA16_PYTHON,
    keys: Sequence[str] = DEFAULT_HEAVY_KEYS,
    timeout_s: float = 120.0,
    command_runner: CommandRunner = _run_command,
) -> Dict[str, Any]:
    runbook = build_runbook_payload(python_exec=python_exec)
    command_by_key = {
        str(item.get("key") or ""): item
        for item in list(runbook.get("commands") or [])
        if isinstance(item, dict)
    }
    results: List[Dict[str, Any]] = []
    for key in [str(item) for item in keys if str(item).strip()]:
        item = command_by_key.get(key)
        if item is None:
            results.append(
                {
                    "key": key,
                    "status": "missing_command",
                    "command": [],
                    "returncode": None,
                    "stdout_tail": "",
                    "stderr_tail": "command key is missing from runbook",
                }
            )
            continue
        command = dry_run_command_for(item)
        outcome = command_runner(command, float(timeout_s))
        returncode = outcome.get("returncode")
        status = "passed" if returncode == 0 else "failed"
        results.append(
            {
                "key": key,
                "status": status,
                "command": command,
                "returncode": returncode,
                "stdout_tail": outcome.get("stdout_tail", ""),
                "stderr_tail": outcome.get("stderr_tail", ""),
            }
        )
    failed = [item for item in results if item.get("status") != "passed"]
    return {
        "status": "passed" if not failed else "failed",
        "python_exec": str(python_exec),
        "timeout_s": float(timeout_s),
        "checked_keys": list(keys),
        "passed_count": sum(1 for item in results if item.get("status") == "passed"),
        "failed_count": len(failed),
        "results": results,
    }


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_markdown(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Town01 Runbook Dry-Run Verification",
        "",
        f"- status: `{payload.get('status', '')}`",
        f"- python_exec: `{payload.get('python_exec', '')}`",
        f"- passed_count: `{payload.get('passed_count', 0)}`",
        f"- failed_count: `{payload.get('failed_count', 0)}`",
        "",
        "| key | status | returncode |",
        "|---|---|---:|",
    ]
    for item in payload.get("results") or []:
        returncode = item.get("returncode")
        lines.append(
            "| `{}` | `{}` | {} |".format(
                item.get("key", ""),
                item.get("status", ""),
                "" if returncode is None else str(returncode),
            )
        )
    lines.extend(["", "## Commands", ""])
    for item in payload.get("results") or []:
        lines.extend(
            [
                f"### {item.get('key', '')}",
                "",
                "```bash",
                _shell(item.get("command") or []),
                "```",
                "",
            ]
        )
        if item.get("status") != "passed":
            lines.extend(
                [
                    "stderr tail:",
                    "",
                    "```text",
                    str(item.get("stderr_tail", "")),
                    "```",
                    "",
                ]
            )
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Dry-run verify heavy online commands from the Town01 goal runbook.")
    parser.add_argument("--python-exec", default=CONDA_CARLA16_PYTHON)
    parser.add_argument("--key", action="append", default=None, help="Specific runbook key to verify; may be repeated.")
    parser.add_argument("--timeout-sec", type=float, default=120.0)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_OUTPUT_MD)
    parser.add_argument("--require-passed", action="store_true")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    keys = list(args.key if args.key is not None else DEFAULT_HEAVY_KEYS)
    payload = build_verification_payload(
        python_exec=str(args.python_exec),
        keys=keys,
        timeout_s=float(args.timeout_sec),
    )
    write_json(args.json_out, payload)
    write_markdown(args.md_out, payload)
    print(json.dumps({"status": payload["status"], "passed_count": payload["passed_count"], "failed_count": payload["failed_count"]}, indent=2))
    if args.require_passed and payload["status"] != "passed":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
