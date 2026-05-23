#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Callable, Dict

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.inspect_town01_demo_readiness import build_readiness_payload
from tools.inspect_town01_goal_status import CONDA_CARLA16_PYTHON, build_goal_status

DEFAULT_OUTPUT_JSON = REPO_ROOT / "artifacts" / "town01_post_online_triage_20260522.json"
DEFAULT_OUTPUT_MD = REPO_ROOT / "artifacts" / "town01_post_online_triage_20260522.md"
DEFAULT_SEQUENCE_JSON = REPO_ROOT / "artifacts" / "town01_goal_sequence_20260522.json"
DEFAULT_PAIR_JSON = REPO_ROOT / "artifacts" / "town01_direct_curve_pair_recovery_retry_20260522.json"
DEMO_BROWSER_CMD = "playwright"

GoalBuilder = Callable[[], Dict[str, Any]]
ReadinessBuilder = Callable[[], Dict[str, Any]]


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.expanduser().read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _summarize_items(items: Any, *, name_key: str) -> Dict[str, Any]:
    if not isinstance(items, list):
        return {"count": 0, "last": {}, "failed": []}
    normalized = [item for item in items if isinstance(item, dict)]
    failed = [
        {
            "name": str(item.get(name_key) or item.get("route_id") or ""),
            "status": str(item.get("status") or ""),
            "returncode": item.get("returncode"),
            "reason": str(item.get("reason") or item.get("candidate_invalid_reason") or ""),
        }
        for item in normalized
        if item.get("returncode") not in {None, 0} or str(item.get("status") or "") in {"failed", "retry_exhausted"}
    ]
    last = normalized[-1] if normalized else {}
    return {
        "count": len(normalized),
        "last": {
            "name": str(last.get(name_key) or last.get("route_id") or ""),
            "status": str(last.get("status") or ""),
            "returncode": last.get("returncode"),
            "reason": str(last.get("reason") or last.get("candidate_invalid_reason") or ""),
        },
        "failed": failed,
    }


def inspect_online_artifact(path: Path) -> Dict[str, Any]:
    resolved = path.expanduser().resolve()
    payload = _load_json(resolved)
    if not payload:
        return {
            "status": "missing",
            "path": str(resolved),
            "raw_status": "",
            "reason": "artifact is missing or unreadable",
        }
    raw_status = str(payload.get("status") or "unknown")
    if raw_status == "dry_run":
        status = "legacy_dry_run_ignored"
        reason = "dry-run payload is present in an online artifact slot"
    else:
        status = raw_status
        reason = str(payload.get("reason") or "")
    steps = _summarize_items(payload.get("steps"), name_key="name")
    attempts = _summarize_items(payload.get("attempts"), name_key="route_id")
    return {
        "status": status,
        "path": str(resolved),
        "raw_status": raw_status,
        "reason": reason,
        "steps": steps,
        "attempts": attempts,
    }


def _goal_brief(goal: Dict[str, Any]) -> Dict[str, Any]:
    next_command = goal.get("next_command") if isinstance(goal.get("next_command"), dict) else {}
    curve = goal.get("curve_recovery") if isinstance(goal.get("curve_recovery"), dict) else {}
    transport = goal.get("transport_decision") if isinstance(goal.get("transport_decision"), dict) else {}
    demo = goal.get("demo_recording") if isinstance(goal.get("demo_recording"), dict) else {}
    return {
        "overall_status": str(goal.get("overall_status") or ""),
        "blockers": goal.get("blockers") or [],
        "next_key": str(next_command.get("key") or ""),
        "next_reason": str(next_command.get("reason") or ""),
        "next_command": str(next_command.get("command") or ""),
        "curve_status": str(curve.get("status") or ""),
        "curve_recovery_verdict": str(curve.get("recovery_verdict") or ""),
        "transport_decision": str(transport.get("status") or ""),
        "transport_promotable": bool(transport.get("transport_promotable", False)),
        "demo_status": str(demo.get("status") or ""),
        "demo_required": bool(demo.get("required", False)),
    }


def _command_final_audit() -> str:
    return (
        f"{CONDA_CARLA16_PYTHON} tools/run_town01_goal_offline_audit.py "
        f"--python-exec {CONDA_CARLA16_PYTHON} --require-goal-ready"
    )


def _command_goal_sequence() -> str:
    return (
        f"{CONDA_CARLA16_PYTHON} tools/run_town01_goal_sequence.py "
        f"--python-exec {CONDA_CARLA16_PYTHON}"
    )


def _command_demo_recording() -> str:
    return (
        f"{CONDA_CARLA16_PYTHON} tools/run_town01_demo_showcase.py "
        "--mode short --record-dreamview --dreamview-auto-open --dreamview-open-wait-page "
        f"--dreamview-browser-cmd {DEMO_BROWSER_CMD} "
        "--dreamview-capture-mode tick_snapshot "
        "--dreamview-capture-region 1280x720+0,0 "
        "--dreamview-use-fixed-region --require-recording-ready"
    )


def classify_triage(goal: Dict[str, Any], readiness: Dict[str, Any]) -> Dict[str, str]:
    brief = _goal_brief(goal)
    next_key = brief["next_key"]
    if brief["overall_status"] == "ready_for_final_review":
        return {
            "status": "ready_for_final_strict_audit",
            "reason": "goal evidence is ready",
            "next_command": _command_final_audit(),
        }
    if str(brief["curve_status"]) != "decisive":
        return {
            "status": "awaiting_direct_curve_gate",
            "reason": str(brief["next_reason"] or "curve recovery still needs online evidence"),
            "next_command": _command_goal_sequence() if next_key.startswith("direct_curve") else brief["next_command"],
        }
    if brief["demo_required"] and brief["demo_status"] != "ready":
        if readiness.get("status") == "ready":
            return {
                "status": "ready_for_demo_recording",
                "reason": "curve gate is decisive and demo readiness is ready",
                "next_command": _command_demo_recording(),
            }
        return {
            "status": "fix_demo_readiness_first",
            "reason": f"demo readiness is {readiness.get('status', '')}",
            "next_command": f"{CONDA_CARLA16_PYTHON} tools/inspect_town01_demo_readiness.py",
        }
    return {
        "status": "continue_goal_audit",
        "reason": "no direct online action is selected; refresh or run the offline audit",
        "next_command": f"{CONDA_CARLA16_PYTHON} tools/run_town01_goal_offline_audit.py --python-exec {CONDA_CARLA16_PYTHON}",
    }


def build_triage_payload(
    *,
    goal_builder: GoalBuilder = build_goal_status,
    readiness_builder: ReadinessBuilder | None = None,
    sequence_json: Path = DEFAULT_SEQUENCE_JSON,
    pair_json: Path = DEFAULT_PAIR_JSON,
) -> Dict[str, Any]:
    goal = goal_builder()
    readiness = (
        build_readiness_payload(
            browser_cmd=DEMO_BROWSER_CMD,
            capture_mode="tick_snapshot",
            capture_region="1280x720+0,0",
            use_fixed_region=True,
            require_browser=True,
            require_video_encoder=True,
        )
        if readiness_builder is None
        else readiness_builder()
    )
    sequence = inspect_online_artifact(sequence_json)
    pair = inspect_online_artifact(pair_json)
    triage = classify_triage(goal, readiness)
    return {
        "triage": triage,
        "goal": _goal_brief(goal),
        "demo_readiness": {
            "status": readiness.get("status", ""),
            "failed_count": readiness.get("failed_count", 0),
            "warning_count": readiness.get("warning_count", 0),
        },
        "online_artifacts": {
            "goal_sequence": sequence,
            "direct_curve_pair": pair,
        },
    }


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_markdown(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    triage = payload.get("triage") if isinstance(payload.get("triage"), dict) else {}
    goal = payload.get("goal") if isinstance(payload.get("goal"), dict) else {}
    readiness = payload.get("demo_readiness") if isinstance(payload.get("demo_readiness"), dict) else {}
    online = payload.get("online_artifacts") if isinstance(payload.get("online_artifacts"), dict) else {}
    lines = [
        "# Town01 Post-Online Triage",
        "",
        f"- status: `{triage.get('status', '')}`",
        f"- reason: `{triage.get('reason', '')}`",
        f"- goal_status: `{goal.get('overall_status', '')}`",
        f"- goal_blockers: `{', '.join(goal.get('blockers') or []) or 'none'}`",
        f"- curve_status: `{goal.get('curve_status', '')}`",
        f"- demo_status: `{goal.get('demo_status', '')}`",
        f"- demo_readiness: `{readiness.get('status', '')}`",
        "",
        "## Online Artifact Slots",
        "",
        "| artifact | status | raw_status | last_step | failed_count | reason | path |",
        "|---|---|---|---|---:|---|---|",
    ]
    for name, item in online.items():
        if not isinstance(item, dict):
            continue
        steps = item.get("steps") if isinstance(item.get("steps"), dict) else {}
        attempts = item.get("attempts") if isinstance(item.get("attempts"), dict) else {}
        last = steps.get("last") if steps.get("count") else attempts.get("last", {})
        failed_count = len(steps.get("failed") or []) + len(attempts.get("failed") or [])
        lines.append(
            "| `{}` | `{}` | `{}` | `{}` | {} | `{}` | `{}` |".format(
                name,
                item.get("status", ""),
                item.get("raw_status", ""),
                str((last or {}).get("name") or "").replace("|", "\\|"),
                failed_count,
                str(item.get("reason", "")).replace("|", "\\|"),
                item.get("path", ""),
            )
        )
    lines.extend(["", "## Failed Online Details", ""])
    any_failed = False
    for name, item in online.items():
        if not isinstance(item, dict):
            continue
        failures = []
        steps = item.get("steps") if isinstance(item.get("steps"), dict) else {}
        attempts = item.get("attempts") if isinstance(item.get("attempts"), dict) else {}
        failures.extend(steps.get("failed") or [])
        failures.extend(attempts.get("failed") or [])
        for failure in failures:
            any_failed = True
            lines.append(
                "- `{}` `{}` status=`{}` returncode=`{}` reason=`{}`".format(
                    name,
                    failure.get("name", ""),
                    failure.get("status", ""),
                    failure.get("returncode", ""),
                    str(failure.get("reason", "")).replace("`", "'"),
                )
            )
    if not any_failed:
        lines.append("- none")
    lines.extend(
        [
            "",
            "## Next Command",
            "",
            "```bash",
            str(triage.get("next_command", "")),
            "```",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Summarize the current Town01 post-online state and choose the next operator command.")
    parser.add_argument("--sequence-json", type=Path, default=DEFAULT_SEQUENCE_JSON)
    parser.add_argument("--pair-json", type=Path, default=DEFAULT_PAIR_JSON)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_OUTPUT_MD)
    parser.add_argument("--print-next-command", action="store_true")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    payload = build_triage_payload(sequence_json=args.sequence_json, pair_json=args.pair_json)
    write_json(args.json_out, payload)
    write_markdown(args.md_out, payload)
    if args.print_next_command:
        print(str(payload.get("triage", {}).get("next_command", "")))
    else:
        print(json.dumps(payload["triage"], indent=2))


if __name__ == "__main__":
    main()
