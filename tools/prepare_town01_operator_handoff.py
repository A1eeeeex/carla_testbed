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

from tools.inspect_town01_demo_readiness import build_readiness_payload as build_demo_readiness_payload
from tools.inspect_town01_goal_resume import (
    DEFAULT_OUTPUT_JSON as DEFAULT_RESUME_JSON,
    DEFAULT_OUTPUT_MD as DEFAULT_RESUME_MD,
    build_resume_payload,
)
from tools.inspect_town01_goal_status import (
    CONDA_CARLA16_PYTHON,
    DEFAULT_OUTPUT_JSON as DEFAULT_GOAL_STATUS_JSON,
    DEFAULT_OUTPUT_MD as DEFAULT_GOAL_STATUS_MD,
    build_goal_status,
)
from tools.inspect_town01_goal_online_runbook_results import build_results_payload as build_runbook_results_payload
from tools.inspect_town01_next_online_action import build_next_action_payload
from tools.inspect_town01_online_preflight import build_preflight_payload
from tools.run_town01_direct_curve_pair_recovery_retry import default_output_paths as pair_default_output_paths
from tools.run_town01_direct_curve_recovery_retry import default_output_paths

DEFAULT_OUTPUT_JSON = REPO_ROOT / "artifacts" / "town01_operator_handoff_20260522.json"
DEFAULT_OUTPUT_MD = REPO_ROOT / "artifacts" / "town01_operator_handoff_20260522.md"
DEMO_BROWSER_CMD = "auto"

PayloadBuilder = Callable[[], Dict[str, Any]]


def _offline_audit_command() -> str:
    return (
        f"{CONDA_CARLA16_PYTHON} tools/run_town01_goal_offline_audit.py "
        f"--python-exec {CONDA_CARLA16_PYTHON}"
    )


def _strict_demo_command() -> str:
    return (
        f"{CONDA_CARLA16_PYTHON} tools/run_town01_demo_showcase.py "
        "--mode short --record-dreamview --dreamview-auto-open "
        "--dreamview-open-wait-page "
        f"--dreamview-browser-cmd {DEMO_BROWSER_CMD} "
        "--dreamview-capture-mode tick_snapshot "
        "--dreamview-capture-region 1280x720+0,0 "
        "--dreamview-use-fixed-region --require-recording-ready"
    )


def _curve_pair_command() -> str:
    return (
        f"{CONDA_CARLA16_PYTHON} tools/run_town01_direct_curve_pair_recovery_retry.py "
        f"--python-exec {CONDA_CARLA16_PYTHON}"
    )


def _goal_sequence_command() -> str:
    return (
        f"{CONDA_CARLA16_PYTHON} tools/run_town01_goal_sequence.py "
        f"--python-exec {CONDA_CARLA16_PYTHON}"
    )


def _post_online_triage_command() -> str:
    return f"{CONDA_CARLA16_PYTHON} tools/inspect_town01_post_online_triage.py"


def _online_runbook_command() -> str:
    return (
        f"{CONDA_CARLA16_PYTHON} tools/prepare_town01_goal_online_runbook.py "
        f"--python-exec {CONDA_CARLA16_PYTHON}"
    )


def _online_runbook_results_command() -> str:
    return (
        f"{CONDA_CARLA16_PYTHON} tools/inspect_town01_goal_online_runbook_results.py "
        f"--python-exec {CONDA_CARLA16_PYTHON}"
    )


def _online_runbook_execute_next_command() -> str:
    return (
        f"{CONDA_CARLA16_PYTHON} tools/run_town01_goal_online_runbook.py "
        f"--python-exec {CONDA_CARLA16_PYTHON}"
    )


def _online_runbook_execute_until_blocked_command() -> str:
    return (
        f"{CONDA_CARLA16_PYTHON} tools/run_town01_goal_online_runbook.py "
        f"--python-exec {CONDA_CARLA16_PYTHON} --mode until-blocked --max-steps 3"
    )


def _online_runbook_execution_inspect_command() -> str:
    return f"{CONDA_CARLA16_PYTHON} tools/inspect_town01_goal_online_runbook_execution.py"


def _after_online_intake_command() -> str:
    return (
        f"{CONDA_CARLA16_PYTHON} tools/inspect_town01_after_online.py "
        f"--python-exec {CONDA_CARLA16_PYTHON} --allow-dry-run-fallback"
    )


def _completion_audit_command() -> str:
    return (
        f"{CONDA_CARLA16_PYTHON} tools/inspect_town01_goal_completion.py "
        f"--python-exec {CONDA_CARLA16_PYTHON}"
    )


def _next_online_action_command() -> str:
    return (
        f"{CONDA_CARLA16_PYTHON} tools/inspect_town01_next_online_action.py "
        f"--python-exec {CONDA_CARLA16_PYTHON}"
    )


def _demo_readiness_payload() -> Dict[str, Any]:
    return build_demo_readiness_payload(
        browser_cmd=DEMO_BROWSER_CMD,
        capture_mode="tick_snapshot",
        capture_region="1280x720+0,0",
        use_fixed_region=True,
        require_browser=True,
        require_video_encoder=True,
    )


def _route_id_from_next_command(next_command: Dict[str, Any]) -> str:
    command = str(next_command.get("command") or "")
    marker = "--route-id "
    if marker not in command:
        return ""
    tail = command.split(marker, 1)[1].strip()
    return tail.split()[0].strip().strip("\\")


def build_handoff_payload(
    *,
    goal_builder: PayloadBuilder = build_goal_status,
    resume_builder: PayloadBuilder = build_resume_payload,
    preflight_builder: PayloadBuilder = build_preflight_payload,
    demo_readiness_builder: PayloadBuilder = _demo_readiness_payload,
    runbook_results_builder: PayloadBuilder | None = None,
    next_action_builder: PayloadBuilder | None = None,
) -> Dict[str, Any]:
    goal = goal_builder()
    resume = resume_builder()
    preflight = preflight_builder()
    demo_readiness = demo_readiness_builder()
    if runbook_results_builder is None:
        runbook_results = build_runbook_results_payload(python_exec=CONDA_CARLA16_PYTHON)
    else:
        runbook_results = runbook_results_builder()
    next_command = goal.get("next_command") if isinstance(goal.get("next_command"), dict) else {}
    broader_next_key = str(runbook_results.get("next_key") or "")
    broader_next_underlying_command = str(runbook_results.get("next_command") or "")
    broader_next_command = str(runbook_results.get("executor_command") or _online_runbook_execute_next_command())
    broader_results = runbook_results.get("results") if isinstance(runbook_results.get("results"), dict) else {}
    broader_next_item = broader_results.get(broader_next_key) if isinstance(broader_results.get(broader_next_key), dict) else {}
    if next_action_builder is None:
        next_action = build_next_action_payload(
            python_exec=CONDA_CARLA16_PYTHON,
            runbook_results_builder=lambda: runbook_results,
            goal_builder=lambda: goal,
        )
    else:
        next_action = next_action_builder()
    next_primary = next_action.get("primary") if isinstance(next_action.get("primary"), dict) else {}
    next_online_command = str(next_primary.get("command") or "")
    if broader_next_key and str(next_primary.get("key") or "") == broader_next_key and broader_next_command:
        next_online_command = broader_next_command
        next_primary = dict(next_primary)
        next_primary.setdefault("underlying_command", broader_next_underlying_command)
        next_primary["command"] = next_online_command
    route_id = _route_id_from_next_command(next_command)
    retry_json, retry_md = default_output_paths(route_id) if route_id else (Path(""), Path(""))
    pair_json, pair_md = pair_default_output_paths()
    return {
        "handoff_status": "ready_to_run_next_online" if next_primary.get("command") else "missing_next_command",
        "selected_next_action": {
            "status": next_action.get("status", ""),
            "focus": next_action.get("focus", ""),
            "rationale": next_action.get("rationale", ""),
            "primary": next_primary,
            "alternatives": next_action.get("alternatives", []),
        },
        "goal_status": {
            "overall_status": goal.get("overall_status", ""),
            "blockers": goal.get("blockers", []),
            "next_command": next_command,
        },
        "resume_status": (resume.get("resume_state") or {}).get("status", ""),
        "broader_validation": {
            "status": runbook_results.get("status", ""),
            "next_key": broader_next_key,
            "next_command": broader_next_command,
            "underlying_command": broader_next_underlying_command,
            "reason": str(broader_next_item.get("reason") or ""),
        },
        "preflight": {
            "status": preflight.get("status", ""),
            "failed_count": preflight.get("failed_count", 0),
            "warning_count": preflight.get("warning_count", 0),
        },
        "demo_readiness": {
            "status": demo_readiness.get("status", ""),
            "failed_count": demo_readiness.get("failed_count", 0),
            "warning_count": demo_readiness.get("warning_count", 0),
        },
        "current_route_id": route_id,
        "commands": {
            "next_online": next_online_command,
            "broader_validation_next": broader_next_command,
            "direct_chain_next": str(next_command.get("command") or ""),
            "curve_pair_online_optional": _curve_pair_command(),
            "goal_sequence_online_optional": _goal_sequence_command(),
            "post_online_triage": _post_online_triage_command(),
            "online_runbook": _online_runbook_command(),
            "online_runbook_results": _online_runbook_results_command(),
            "online_runbook_execute_next": _online_runbook_execute_next_command(),
            "online_runbook_execute_until_blocked": _online_runbook_execute_until_blocked_command(),
            "online_runbook_execution_inspect": _online_runbook_execution_inspect_command(),
            "after_online_intake": _after_online_intake_command(),
            "completion_audit": _completion_audit_command(),
            "next_online_action": _next_online_action_command(),
            "offline_audit_optional": _offline_audit_command(),
            "demo_recording_after_transport_gate": _strict_demo_command(),
        },
        "expected_after_online": {
            "retry_json": str(retry_json),
            "retry_md": str(retry_md),
            "pair_retry_json": str(pair_json),
            "pair_retry_md": str(pair_md),
            "goal_status_json": str(DEFAULT_GOAL_STATUS_JSON),
            "goal_status_md": str(DEFAULT_GOAL_STATUS_MD),
            "resume_json": str(DEFAULT_RESUME_JSON),
            "resume_md": str(DEFAULT_RESUME_MD),
        },
        "recommended_operator_order": [
            "broader_validation_next",
            "direct_chain_next",
            "post_online_triage",
        ],
        "operator_notes": [
            "Run broader_validation_next first when validating that the large structural changes did not break key scenes; it is the refreshing executor, not the raw underlying gate command.",
            "Run direct_chain_next when focusing specifically on the carla_direct transport gate.",
            "Both online commands must use the conda carla16 environment.",
            "Exit code 2 can still be useful evidence; inspect retry_json and resume_md before interpreting it.",
            "The retry wrapper refreshes goal_status and goal_resume after a real online attempt.",
            "After any manual online run, post_online_triage gives the quickest one-line next step.",
            "Use online_runbook for the broader followstop/Town01/direct/demo validation order.",
            "Use online_runbook_results to see which critical-scene gates already have evidence.",
            "Use online_runbook_execute_next to execute only the current runbook next_key.",
            "Use online_runbook_execute_until_blocked to advance across runbook gates until max-steps or no-progress.",
            "Use online_runbook_execution_inspect immediately after the executor to classify whether it advanced or stalled.",
            "Use after_online_intake for the single consolidated post-online report after any online executor run.",
            "Use completion_audit to check whether the full active goal is actually proven complete.",
            "Use next_online_action for the single most concise current command recommendation.",
            "If you want one command for both direct curve routes, run curve_pair_online_optional instead of next_online.",
            "If you want curve gate plus automatic demo recording when ready, run goal_sequence_online_optional.",
            "Do not treat dry-run artifacts as online capability evidence.",
            "Only record the CARLA + Dreamview demo after the transport/curve gate moves past direct curve recovery.",
        ],
    }


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_markdown(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    goal = payload.get("goal_status", {})
    broader = payload.get("broader_validation", {})
    selected = payload.get("selected_next_action", {})
    selected_primary = selected.get("primary") if isinstance(selected.get("primary"), dict) else {}
    preflight = payload.get("preflight", {})
    demo = payload.get("demo_readiness", {})
    commands = payload.get("commands", {})
    expected = payload.get("expected_after_online", {})
    lines = [
        "# Town01 Operator Handoff",
        "",
        f"- handoff_status: `{payload.get('handoff_status', '')}`",
        f"- selected_next_key: `{selected_primary.get('key', '')}`",
        f"- selected_next_title: `{selected_primary.get('title', '')}`",
        f"- goal_status: `{goal.get('overall_status', '')}`",
        f"- goal_blockers: `{', '.join(goal.get('blockers', [])) or 'none'}`",
        f"- broader_validation_status: `{broader.get('status', '')}`",
        f"- broader_validation_next_key: `{broader.get('next_key', '')}`",
        f"- resume_status: `{payload.get('resume_status', '')}`",
        f"- preflight_status: `{preflight.get('status', '')}`",
        f"- preflight_failed_count: `{preflight.get('failed_count', 0)}`",
        f"- demo_readiness: `{demo.get('status', '')}`",
        f"- current_route_id: `{payload.get('current_route_id', '')}`",
        "",
        "## Selected Next Online Action",
        "",
        f"- status: `{selected.get('status', '')}`",
        f"- focus: `{selected.get('focus', '')}`",
        f"- rationale: `{selected.get('rationale', '')}`",
        f"- key: `{selected_primary.get('key', '')}`",
        f"- title: `{selected_primary.get('title', '')}`",
        f"- command_status: `{selected_primary.get('status', '')}`",
        f"- reason: `{selected_primary.get('reason', '')}`",
        "",
        "```bash",
        str(commands.get("next_online", "")),
        "```",
        "",
        "## Broader Validation Next Command",
        "",
        f"- key: `{broader.get('next_key', '')}`",
        f"- reason: `{broader.get('reason', '')}`",
        f"- underlying_command: `{broader.get('underlying_command', '')}`",
        "",
        "```bash",
        str(commands.get("broader_validation_next", "")),
        "```",
        "",
        "## Direct Chain Next Command",
        "",
        f"- key: `{(goal.get('next_command') or {}).get('key', '')}`",
        f"- reason: `{(goal.get('next_command') or {}).get('reason', '')}`",
        "",
        "```bash",
        str(commands.get("direct_chain_next", "") or commands.get("next_online", "")),
        "```",
        "",
        "## Expected Outputs After Online",
        "",
    ]
    for key, value in expected.items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(
        [
            "",
            "## Optional Post-Online Audit",
            "",
            "The wrapper already refreshes goal status and resume after a real online attempt. Run this only if you want the full offline test gate again.",
            "",
            "For the broader critical-scene validation order, use:",
            "",
            "```bash",
            str(commands.get("online_runbook", "")),
            "```",
            "",
            "To inspect which runbook gates already have evidence:",
            "",
            "```bash",
            str(commands.get("online_runbook_results", "")),
            "```",
            "",
            "To execute just the current broader-validation next key:",
            "",
            "```bash",
            str(commands.get("online_runbook_execute_next", "")),
            "```",
            "",
            "To execute multiple broader-validation gates until blocked or max-steps:",
            "",
            "```bash",
            str(commands.get("online_runbook_execute_until_blocked", "")),
            "```",
            "",
            "To classify the last online runbook execution artifact:",
            "",
            "```bash",
            str(commands.get("online_runbook_execution_inspect", "")),
            "```",
            "",
            "To get a consolidated post-online intake report:",
            "",
            "```bash",
            str(commands.get("after_online_intake", "")),
            "```",
            "",
            "To audit whether the full active goal is actually complete:",
            "",
            "```bash",
            str(commands.get("completion_audit", "")),
            "```",
            "",
            "For a faster next-step decision after online runs, use:",
            "",
            "```bash",
            str(commands.get("next_online_action", "")),
            "```",
            "",
            "For direct post-online triage details, use:",
            "",
            "```bash",
            str(commands.get("post_online_triage", "")),
            "```",
            "",
            "```bash",
            str(commands.get("offline_audit_optional", "")),
            "```",
            "",
            "## Optional One-Command Curve Pair Gate",
            "",
            "This wrapper runs the currently selected direct curve route and continues to the second curve route only if the goal status advances to it.",
            "",
            "```bash",
            str(commands.get("curve_pair_online_optional", "")),
            "```",
            "",
            "## Optional Goal Sequence",
            "",
            "This wrapper runs the direct curve gate, then records the CARLA + Dreamview demo automatically only after the goal status and demo readiness allow it.",
            "",
            "```bash",
            str(commands.get("goal_sequence_online_optional", "")),
            "```",
            "",
            "## Demo Command After Transport Gate",
            "",
            "```bash",
            str(commands.get("demo_recording_after_transport_gate", "")),
            "```",
            "",
            "## Notes",
            "",
        ]
    )
    for note in payload.get("operator_notes", []):
        lines.append(f"- {note}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare a single operator handoff for the current Town01 goal state.")
    parser.add_argument(
        "--python-exec",
        default=CONDA_CARLA16_PYTHON,
        help="Accepted for consistency with other goal tools; current handoff commands use conda carla16.",
    )
    parser.add_argument("--json-out", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_OUTPUT_MD)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    payload = build_handoff_payload()
    write_json(args.json_out, payload)
    write_markdown(args.md_out, payload)
    print(
        json.dumps(
            {
                "handoff_status": payload["handoff_status"],
                "goal_status": payload["goal_status"]["overall_status"],
                "broader_validation_next": payload["commands"].get("broader_validation_next", ""),
                "direct_chain_next": payload["commands"].get("direct_chain_next", ""),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
