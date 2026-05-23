#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.inspect_town01_goal_online_runbook_results import (
    PASSING_STATUSES,
    build_results_payload,
)
from tools.inspect_town01_goal_status import CONDA_CARLA16_PYTHON, build_goal_status
from tools.inspect_town01_post_online_triage import build_triage_payload

DEFAULT_OUTPUT_JSON = REPO_ROOT / "artifacts" / "town01_next_online_action_20260522.json"
DEFAULT_OUTPUT_MD = REPO_ROOT / "artifacts" / "town01_next_online_action_20260522.md"

PayloadBuilder = Callable[[], Dict[str, Any]]


def _status(item: Dict[str, Any]) -> str:
    return str(item.get("status") or "")


def _result_item(results_payload: Dict[str, Any], key: str) -> Dict[str, Any]:
    results = results_payload.get("results") if isinstance(results_payload.get("results"), dict) else {}
    item = results.get(key) if isinstance(results.get(key), dict) else {}
    return item if isinstance(item, dict) else {}


def _line(
    *,
    key: str,
    title: str,
    status: str,
    command: str,
    reason: str,
    priority: str,
    underlying_command: str = "",
) -> Dict[str, str]:
    return {
        "key": key,
        "title": title,
        "status": status,
        "command": command,
        "reason": reason,
        "priority": priority,
        "underlying_command": underlying_command,
    }


def _runbook_executor_command(python_exec: str) -> str:
    return f"{python_exec} tools/run_town01_goal_online_runbook.py --python-exec {python_exec}"


def build_next_action_payload(
    *,
    python_exec: str = CONDA_CARLA16_PYTHON,
    runbook_results_builder: PayloadBuilder | None = None,
    goal_builder: PayloadBuilder = build_goal_status,
    triage_builder: PayloadBuilder = build_triage_payload,
    focus: str = "auto",
) -> Dict[str, Any]:
    runbook_results = (
        build_results_payload(python_exec=python_exec)
        if runbook_results_builder is None
        else runbook_results_builder()
    )
    goal = goal_builder()
    triage_payload = triage_builder()
    triage = triage_payload.get("triage") if isinstance(triage_payload.get("triage"), dict) else {}

    broader_key = str(runbook_results.get("next_key") or "")
    broader_item = _result_item(runbook_results, broader_key)
    broader = _line(
        key=broader_key,
        title="broader_validation",
        status=_status(broader_item) or str(runbook_results.get("status") or ""),
        command=_runbook_executor_command(python_exec),
        reason=str(broader_item.get("reason") or "next incomplete runbook gate"),
        priority="primary" if focus in {"auto", "broader"} else "secondary",
        underlying_command=str(runbook_results.get("next_command") or ""),
    )

    goal_next = goal.get("next_command") if isinstance(goal.get("next_command"), dict) else {}
    direct = _line(
        key=str(goal_next.get("key") or ""),
        title="direct_chain",
        status=str((goal.get("curve_recovery") or {}).get("status") or goal.get("overall_status") or ""),
        command=str(goal_next.get("command") or ""),
        reason=str(goal_next.get("reason") or ""),
        priority="primary" if focus == "direct" else "secondary",
    )

    triage_status = str(triage.get("status") or "")
    demo_ready = triage_status == "ready_for_demo_recording"
    demo = _line(
        key="demo_recording" if demo_ready else "goal_sequence_gate",
        title="demo_recording" if demo_ready else "goal_sequence_gate",
        status=str((goal.get("demo_recording") or {}).get("status") or "") if demo_ready else triage_status,
        command=str(triage.get("next_command") or ""),
        reason=str(triage.get("reason") or ""),
        priority="primary" if focus == "demo" else "secondary",
    )

    lines: List[Dict[str, str]] = [broader, direct, demo]
    if focus == "auto":
        if broader_key and broader["status"] not in PASSING_STATUSES:
            primary = broader
            rationale = "broader validation is first incomplete runbook gate"
        elif direct["command"] and direct["status"] != "decisive":
            primary = direct
            rationale = "broader validation is satisfied; direct curve gate is next"
        elif demo_ready:
            primary = demo
            rationale = "transport/curve gate is ready and demo recording is next"
        else:
            primary = _line(
                key="final_audit",
                title="final_audit",
                status=str(goal.get("overall_status") or ""),
                command=(
                    f"{python_exec} tools/run_town01_goal_offline_audit.py "
                    f"--python-exec {python_exec} --require-goal-ready"
                ),
                reason="no online gate selected; run final audit if evidence is ready",
                priority="primary",
            )
            rationale = "no broader/direct/demo online action is currently selected"
    elif focus == "direct":
        primary = direct
        rationale = "focus=direct selected carla_direct curve recovery"
    elif focus == "demo":
        primary = demo
        rationale = (
            "focus=demo selected demo recording path"
            if demo_ready
            else "focus=demo selected the goal sequence gate because demo recording is not ready yet"
        )
    else:
        primary = broader
        rationale = "focus=broader selected full validation runbook"

    return {
        "status": "ready" if primary.get("command") else "missing_command",
        "focus": focus,
        "primary": primary,
        "rationale": rationale,
        "alternatives": [item for item in lines if item is not primary],
        "runbook": {
            "status": str(runbook_results.get("status") or ""),
            "next_key": broader_key,
        },
        "goal": {
            "overall_status": str(goal.get("overall_status") or ""),
            "blockers": goal.get("blockers") or [],
        },
        "triage": {
            "status": str(triage.get("status") or ""),
            "reason": str(triage.get("reason") or ""),
        },
    }


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_markdown(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    primary = payload.get("primary") if isinstance(payload.get("primary"), dict) else {}
    lines = [
        "# Town01 Next Online Action",
        "",
        f"- status: `{payload.get('status', '')}`",
        f"- focus: `{payload.get('focus', '')}`",
        f"- rationale: `{payload.get('rationale', '')}`",
        f"- primary_key: `{primary.get('key', '')}`",
        f"- primary_title: `{primary.get('title', '')}`",
        f"- primary_status: `{primary.get('status', '')}`",
        "",
        "## Primary Command",
        "",
        "```bash",
        str(primary.get("command") or ""),
        "```",
        "",
    ]
    if primary.get("underlying_command") and primary.get("underlying_command") != primary.get("command"):
        lines.extend(
            [
                "## Underlying Command",
                "",
                "This is the raw command executed by the runbook wrapper.",
                "",
                "```bash",
                str(primary.get("underlying_command") or ""),
                "```",
                "",
            ]
        )
    lines.extend(
        [
            "## Alternatives",
            "",
            "| title | key | status | reason |",
            "|---|---|---|---|",
        ]
    )
    for item in payload.get("alternatives") or []:
        if not isinstance(item, dict):
            continue
        lines.append(
            "| `{}` | `{}` | `{}` | `{}` |".format(
                item.get("title", ""),
                item.get("key", ""),
                item.get("status", ""),
                str(item.get("reason", "")).replace("|", "\\|"),
            )
        )
        if item.get("command"):
            lines.extend(["", "```bash", str(item.get("command")), "```", ""])
        if item.get("underlying_command") and item.get("underlying_command") != item.get("command"):
            lines.extend(
                [
                    "Underlying:",
                    "",
                    "```bash",
                    str(item.get("underlying_command")),
                    "```",
                    "",
                ]
            )
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Choose the next Town01 online command from current evidence.")
    parser.add_argument("--python-exec", default=CONDA_CARLA16_PYTHON)
    parser.add_argument("--focus", choices=["auto", "broader", "direct", "demo"], default="auto")
    parser.add_argument("--json-out", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_OUTPUT_MD)
    parser.add_argument("--print-command", action="store_true")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    payload = build_next_action_payload(python_exec=args.python_exec, focus=args.focus)
    write_json(args.json_out, payload)
    write_markdown(args.md_out, payload)
    if args.print_command:
        print(str((payload.get("primary") or {}).get("command") or ""))
    else:
        print(
            json.dumps(
                {
                    "status": payload.get("status"),
                    "focus": payload.get("focus"),
                    "primary_key": (payload.get("primary") or {}).get("key"),
                    "primary_title": (payload.get("primary") or {}).get("title"),
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
