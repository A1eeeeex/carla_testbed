#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.inspect_town01_goal_status import (  # noqa: E402
    CONDA_CARLA16_PYTHON,
    DEFAULT_OUTPUT_JSON as DEFAULT_GOAL_STATUS_JSON,
    DEFAULT_OUTPUT_MD as DEFAULT_GOAL_STATUS_MD,
    build_goal_status,
)
from tools.inspect_town01_online_preflight import (  # noqa: E402
    DEFAULT_OUTPUT_JSON as DEFAULT_PREFLIGHT_JSON,
    DEFAULT_OUTPUT_MD as DEFAULT_PREFLIGHT_MD,
    build_preflight_payload,
)

DEFAULT_OUTPUT_JSON = REPO_ROOT / "artifacts" / "town01_goal_resume_20260522.json"
DEFAULT_OUTPUT_MD = REPO_ROOT / "artifacts" / "town01_goal_resume_20260522.md"


def _offline_audit_command(*, require_ready: bool = False) -> str:
    parts = [
        CONDA_CARLA16_PYTHON,
        "tools/run_town01_goal_offline_audit.py",
        "--python-exec",
        CONDA_CARLA16_PYTHON,
    ]
    if require_ready:
        parts.append("--require-goal-ready")
    return " ".join(parts)


def _preflight_command() -> str:
    return f"{CONDA_CARLA16_PYTHON} tools/inspect_town01_online_preflight.py"


def classify_resume_state(goal_status: Dict[str, Any], preflight: Dict[str, Any]) -> Dict[str, str]:
    if goal_status.get("overall_status") == "ready_for_final_review":
        return {
            "status": "ready_for_final_strict_audit",
            "reason": "goal evidence is ready; run the strict offline audit before marking complete",
        }
    if preflight.get("status") == "failed":
        return {
            "status": "fix_local_preflight_first",
            "reason": "online preflight has failed checks, so a new online run would likely be noisy",
        }
    next_command = goal_status.get("next_command") if isinstance(goal_status.get("next_command"), dict) else {}
    key = str(next_command.get("key") or "")
    if key.startswith("direct_curve"):
        return {
            "status": "awaiting_direct_curve_online",
            "reason": str(next_command.get("reason") or "direct curve recovery still needs online evidence"),
        }
    if key == "demo_recording_online":
        return {
            "status": "awaiting_demo_recording",
            "reason": "transport evidence is ready enough to move to CARLA + Dreamview demo recording",
        }
    return {
        "status": "awaiting_next_command",
        "reason": str(next_command.get("reason") or "follow the selected next command"),
    }


def build_resume_payload(*, refresh_preflight: bool = True) -> Dict[str, Any]:
    goal_status = build_goal_status()
    preflight = build_preflight_payload(refresh_goal_status=False) if refresh_preflight else {}
    resume_state = classify_resume_state(goal_status, preflight)
    next_command = goal_status.get("next_command") if isinstance(goal_status.get("next_command"), dict) else {}
    transport_ab = goal_status.get("transport_ab") if isinstance(goal_status.get("transport_ab"), dict) else {}
    curve_recovery = goal_status.get("curve_recovery") if isinstance(goal_status.get("curve_recovery"), dict) else {}
    direct_curve_retry = (
        goal_status.get("direct_curve_retry")
        if isinstance(goal_status.get("direct_curve_retry"), dict)
        else {}
    )
    demo_recording = goal_status.get("demo_recording") if isinstance(goal_status.get("demo_recording"), dict) else {}
    return {
        "resume_state": resume_state,
        "goal_status": {
            "overall_status": goal_status.get("overall_status", ""),
            "blockers": goal_status.get("blockers", []),
            "transport_decision": goal_status.get("transport_decision", {}),
            "transport_ab": {
                "status": transport_ab.get("status", ""),
                "verdict_counts": (transport_ab.get("summary") or {}).get("verdict_counts", {}),
            },
            "curve_recovery": {
                "status": curve_recovery.get("status", ""),
                "source": curve_recovery.get("source", ""),
                "recovery_verdict": curve_recovery.get("recovery_verdict", ""),
                "verdict_counts": (curve_recovery.get("summary") or {}).get("verdict_counts", {}),
            },
            "direct_curve_retry": {
                "status": direct_curve_retry.get("status", ""),
                "routes": direct_curve_retry.get("routes", []),
            },
            "demo_recording": {
                "status": demo_recording.get("status", ""),
                "source": demo_recording.get("source", ""),
                "required": bool(demo_recording.get("required", False)),
            },
            "next_command": next_command,
        },
        "preflight": {
            "status": preflight.get("status", ""),
            "failed_count": preflight.get("failed_count", 0),
            "warning_count": preflight.get("warning_count", 0),
        },
        "commands": {
            "preflight": _preflight_command(),
            "next_online": str(next_command.get("command") or ""),
            "post_online_audit": _offline_audit_command(require_ready=False),
            "final_strict_audit": _offline_audit_command(require_ready=True),
        },
        "artifacts": {
            "goal_status_json": str(DEFAULT_GOAL_STATUS_JSON),
            "goal_status_md": str(DEFAULT_GOAL_STATUS_MD),
            "preflight_json": str(DEFAULT_PREFLIGHT_JSON),
            "preflight_md": str(DEFAULT_PREFLIGHT_MD),
        },
    }


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_markdown(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    state = payload.get("resume_state", {})
    goal = payload.get("goal_status", {})
    preflight = payload.get("preflight", {})
    commands = payload.get("commands", {})
    artifacts = payload.get("artifacts", {})
    next_command = goal.get("next_command", {})
    transport_decision = goal.get("transport_decision", {})
    transport_ab = goal.get("transport_ab", {})
    curve_recovery = goal.get("curve_recovery", {})
    direct_curve_retry = goal.get("direct_curve_retry", {})
    demo_recording = goal.get("demo_recording", {})
    lines = [
        "# Town01 Goal Resume",
        "",
        f"- resume_status: `{state.get('status', '')}`",
        f"- reason: `{state.get('reason', '')}`",
        f"- goal_status: `{goal.get('overall_status', '')}`",
        f"- goal_blockers: `{', '.join(goal.get('blockers', [])) or 'none'}`",
        f"- preflight_status: `{preflight.get('status', '')}`",
        f"- preflight_failed_count: `{preflight.get('failed_count', 0)}`",
        f"- preflight_warning_count: `{preflight.get('warning_count', 0)}`",
        "",
        "## Evidence Gates",
        "",
        f"- transport_ab: `{transport_ab.get('status', '')}`",
        f"- transport_decision: `{transport_decision.get('status', '')}`",
        f"- transport_promotable: `{transport_decision.get('transport_promotable', False)}`",
        f"- direct_candidate_action: `{transport_decision.get('direct_candidate_action', '')}`",
        f"- curve_recovery: `{curve_recovery.get('status', '')}`",
        f"- curve_recovery_verdict: `{curve_recovery.get('recovery_verdict', '')}`",
        f"- curve_recovery_source: `{curve_recovery.get('source', '')}`",
        f"- curve_verdict_counts: `{json.dumps(curve_recovery.get('verdict_counts', {}), sort_keys=True)}`",
        f"- direct_curve_retry: `{direct_curve_retry.get('status', '')}`",
        f"- demo_recording: `{demo_recording.get('status', '')}`",
        f"- demo_recording_source: `{demo_recording.get('source', '')}`",
        "",
        "## Direct Curve Retry Attempts",
        "",
        "| route_id | online_status | final_verdict | attempts | last_verdict | summary_new | invalid_reason |",
        "|---|---|---|---:|---|---|---|",
    ]
    for item in direct_curve_retry.get("routes") or []:
        online = item.get("online") if isinstance(item.get("online"), dict) else {}
        last = online.get("last_attempt") if isinstance(online.get("last_attempt"), dict) else {}
        lines.append(
            "| `{}` | `{}` | `{}` | {} | `{}` | `{}` | `{}` |".format(
                item.get("route_id", ""),
                online.get("status", ""),
                online.get("final_verdict", ""),
                online.get("attempt_count", 0),
                last.get("verdict", ""),
                last.get("candidate_summary_new_for_attempt", ""),
                last.get("candidate_invalid_reason", ""),
            )
        )
    lines.extend(
        [
            "",
            "## Current Next Online Command",
            "",
            f"- key: `{next_command.get('key', '')}`",
            f"- reason: `{next_command.get('reason', '')}`",
            "",
            "```bash",
            str(commands.get("next_online", "")),
            "```",
            "",
            "## Recommended Sequence",
            "",
            "1. Preflight without launching CARLA/Apollo:",
            "",
            "```bash",
            str(commands.get("preflight", "")),
            "```",
            "",
            "2. Run the current next online command above.",
            "",
            "3. After the online run finishes, refresh the offline audit:",
            "",
            "```bash",
            str(commands.get("post_online_audit", "")),
            "```",
            "",
            "4. Only when the audit says the goal is ready, run the strict final gate:",
            "",
            "```bash",
            str(commands.get("final_strict_audit", "")),
            "```",
            "",
            "## Related Artifacts",
            "",
        ]
    )
    for key, value in artifacts.items():
        lines.append(f"- {key}: `{value}`")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Summarize the current Town01 goal phase and next operator command.")
    parser.add_argument("--json-out", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_OUTPUT_MD)
    parser.add_argument("--refresh-preflight", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--print-next-command", action="store_true")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    payload = build_resume_payload(refresh_preflight=bool(args.refresh_preflight))
    write_json(args.json_out, payload)
    write_markdown(args.md_out, payload)
    if args.print_next_command:
        print(str(payload.get("commands", {}).get("next_online", "")))
    else:
        print(json.dumps({"resume_status": payload["resume_state"]["status"], "goal_status": payload["goal_status"]["overall_status"]}, indent=2))


if __name__ == "__main__":
    main()
