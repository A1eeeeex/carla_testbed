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
from tools.inspect_town01_goal_online_runbook_execution import (
    DEFAULT_OUTPUT_JSON as DEFAULT_EXECUTION_INSPECTION_JSON,
    inspect_execution,
)
from tools.inspect_town01_goal_online_runbook_results import build_results_payload
from tools.inspect_town01_goal_status import CONDA_CARLA16_PYTHON, build_goal_status
from tools.inspect_town01_next_online_action import build_next_action_payload
from tools.inspect_town01_post_online_triage import build_triage_payload
from tools.prepare_town01_operator_handoff import build_handoff_payload
from tools.run_town01_goal_online_runbook import DEFAULT_OUTPUT_JSON as DEFAULT_EXECUTION_JSON
from tools.run_town01_goal_online_runbook import (
    DEFAULT_DRY_RUN_OUTPUT_JSON as DEFAULT_DRY_RUN_EXECUTION_JSON,
)

DEFAULT_OUTPUT_JSON = REPO_ROOT / "artifacts" / "town01_after_online_intake_20260522.json"
DEFAULT_OUTPUT_MD = REPO_ROOT / "artifacts" / "town01_after_online_intake_20260522.md"

PayloadBuilder = Callable[[], Dict[str, Any]]


def _dict(payload: Any) -> Dict[str, Any]:
    return payload if isinstance(payload, dict) else {}


def _goal_blockers(goal: Dict[str, Any]) -> list[str]:
    blockers = goal.get("blockers")
    return [str(item) for item in blockers] if isinstance(blockers, list) else []


def _primary_command(next_action: Dict[str, Any]) -> str:
    primary = _dict(next_action.get("primary"))
    return str(primary.get("command") or "")


def _route_ids_for_verdicts(rows: Any, verdicts: set[str]) -> list[str]:
    if not isinstance(rows, list):
        return []
    route_ids: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("verdict") or "") in verdicts:
            route_id = str(row.get("route_id") or "").strip()
            if route_id:
                route_ids.append(route_id)
    return route_ids


def _transport_gate(goal: Dict[str, Any]) -> Dict[str, Any]:
    decision = _dict(goal.get("transport_decision"))
    curve = _dict(goal.get("curve_recovery"))
    transport_ab = _dict(goal.get("transport_ab"))
    curve_summary = _dict(curve.get("summary"))
    ab_summary = _dict(transport_ab.get("summary"))
    rows = transport_ab.get("rows")
    curve_rows = curve.get("rows")
    pending = _route_ids_for_verdicts(
        rows,
        {
            "candidate_missing",
            "candidate_inconclusive_runtime_interrupted",
            "candidate_inconclusive_short_apply_metric_mismatch",
            "candidate_inconclusive_short_apply_window",
            "candidate_inconclusive_gt_ingress",
        },
    )
    if not pending:
        pending = _route_ids_for_verdicts(
            curve_rows,
            {"candidate_missing", "candidate_inconclusive_runtime_interrupted"},
        )
    return {
        "decision_status": str(decision.get("status") or ""),
        "direct_candidate_action": str(decision.get("direct_candidate_action") or ""),
        "transport_promotable": bool(decision.get("transport_promotable", False)),
        "curve_status": str(curve.get("status") or ""),
        "curve_recovery_verdict": str(curve.get("recovery_verdict") or ""),
        "route_count": int(ab_summary.get("route_count") or curve_summary.get("route_count") or 0),
        "candidate_positive_count": int(
            ab_summary.get("candidate_positive_count") or curve_summary.get("candidate_positive_count") or 0
        ),
        "candidate_negative_count": int(
            ab_summary.get("candidate_negative_count") or curve_summary.get("candidate_negative_count") or 0
        ),
        "candidate_inconclusive_count": int(
            ab_summary.get("candidate_inconclusive_count") or curve_summary.get("candidate_inconclusive_count") or 0
        ),
        "pending_rerun_routes": pending,
    }


def classify_after_online(
    *,
    execution: Dict[str, Any],
    goal: Dict[str, Any],
    triage: Dict[str, Any],
    next_action: Dict[str, Any],
    handoff: Dict[str, Any],
    demo_readiness: Dict[str, Any],
) -> Dict[str, str]:
    execution_status = str(execution.get("status") or "")
    goal_status = str(goal.get("overall_status") or "")
    triage_info = _dict(triage.get("triage"))
    triage_status = str(triage_info.get("status") or "")
    blockers = _goal_blockers(goal)
    commands = _dict(handoff.get("commands"))

    if execution_status in {"dry_run_only", "missing", ""}:
        return {
            "status": "awaiting_online_run",
            "reason": "no real online execution artifact is available",
            "next_command": str(commands.get("broader_validation_next") or _primary_command(next_action)),
        }
    if execution_status in {"failed_without_evidence_progress", "stopped_without_progress"}:
        return {
            "status": "online_run_needs_triage",
            "reason": str(execution.get("reason") or "online run did not advance evidence"),
            "next_command": str(commands.get("post_online_triage") or ""),
        }
    if goal_status == "ready_for_final_review":
        return {
            "status": "ready_for_final_strict_audit",
            "reason": "goal status is ready_for_final_review",
            "next_command": str(commands.get("offline_audit_optional") or ""),
        }
    if triage_status == "ready_for_demo_recording":
        return {
            "status": "ready_for_demo_recording",
            "reason": "transport/curve gate is ready and demo recording is next",
            "next_command": str(triage_info.get("next_command") or commands.get("demo_recording_after_transport_gate") or ""),
        }
    if blockers:
        return {
            "status": "goal_in_progress",
            "reason": ", ".join(blockers),
            "next_command": str(_primary_command(next_action) or commands.get("next_online") or ""),
        }
    if str(demo_readiness.get("status") or "") not in {"ready", "ready_with_warnings"}:
        return {
            "status": "demo_readiness_needs_attention",
            "reason": f"demo readiness is {demo_readiness.get('status', '')}",
            "next_command": str(commands.get("demo_recording_after_transport_gate") or ""),
        }
    return {
        "status": "review_required",
        "reason": "no blocker selected, but final readiness is not proven",
        "next_command": str(_primary_command(next_action) or commands.get("next_online") or ""),
    }


def build_after_online_payload(
    *,
    python_exec: str = CONDA_CARLA16_PYTHON,
    execution_path: Path = DEFAULT_EXECUTION_JSON,
    allow_dry_run_fallback: bool = False,
    execution_builder: PayloadBuilder | None = None,
    runbook_results_builder: PayloadBuilder | None = None,
    goal_builder: PayloadBuilder = build_goal_status,
    triage_builder: PayloadBuilder = build_triage_payload,
    next_action_builder: PayloadBuilder | None = None,
    handoff_builder: PayloadBuilder | None = None,
    demo_readiness_builder: PayloadBuilder | None = None,
) -> Dict[str, Any]:
    selected_execution_path = execution_path
    if allow_dry_run_fallback and not execution_path.expanduser().exists():
        selected_execution_path = DEFAULT_DRY_RUN_EXECUTION_JSON
    execution = execution_builder() if execution_builder else inspect_execution(selected_execution_path)
    runbook_results = (
        runbook_results_builder()
        if runbook_results_builder
        else build_results_payload(python_exec=python_exec)
    )
    goal = goal_builder()
    triage = triage_builder()
    if next_action_builder:
        next_action = next_action_builder()
    else:
        next_action = build_next_action_payload(
            python_exec=python_exec,
            runbook_results_builder=lambda: runbook_results,
            goal_builder=lambda: goal,
            triage_builder=lambda: triage,
        )
    if handoff_builder:
        handoff = handoff_builder()
    else:
        handoff = build_handoff_payload(
            goal_builder=lambda: goal,
            runbook_results_builder=lambda: runbook_results,
            next_action_builder=lambda: next_action,
        )
    demo_readiness = (
        demo_readiness_builder()
        if demo_readiness_builder
        else build_readiness_payload(
            browser_cmd="playwright",
            require_browser=True,
            require_video_encoder=True,
        )
    )
    intake = classify_after_online(
        execution=execution,
        goal=goal,
        triage=triage,
        next_action=next_action,
        handoff=handoff,
        demo_readiness=demo_readiness,
    )
    return {
        "status": intake["status"],
        "reason": intake["reason"],
        "next_command": intake["next_command"],
        "python_exec": str(python_exec),
        "execution": {
            "status": str(execution.get("status") or ""),
            "reason": str(execution.get("reason") or ""),
            "next_action": str(execution.get("next_action") or ""),
            "initial_next_key": str(execution.get("initial_next_key") or ""),
            "final_next_key": str(execution.get("final_next_key") or ""),
            "path": str(execution.get("path") or selected_execution_path),
        },
        "runbook": {
            "status": str(runbook_results.get("status") or ""),
            "next_key": str(runbook_results.get("next_key") or ""),
            "executor_command": str(runbook_results.get("executor_command") or ""),
        },
        "goal": {
            "overall_status": str(goal.get("overall_status") or ""),
            "blockers": _goal_blockers(goal),
        },
        "transport_gate": _transport_gate(goal),
        "triage": _dict(triage.get("triage")),
        "demo_readiness": {
            "status": str(demo_readiness.get("status") or ""),
            "failed_count": int(demo_readiness.get("failed_count") or 0),
            "warning_count": int(demo_readiness.get("warning_count") or 0),
        },
        "handoff": {
            "status": str(handoff.get("handoff_status") or ""),
            "commands": _dict(handoff.get("commands")),
        },
    }


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_markdown(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    execution = _dict(payload.get("execution"))
    runbook = _dict(payload.get("runbook"))
    goal = _dict(payload.get("goal"))
    transport = _dict(payload.get("transport_gate"))
    demo = _dict(payload.get("demo_readiness"))
    lines = [
        "# Town01 After-Online Intake",
        "",
        f"- status: `{payload.get('status', '')}`",
        f"- reason: `{payload.get('reason', '')}`",
        f"- execution_status: `{execution.get('status', '')}`",
        f"- execution_next_action: `{execution.get('next_action', '')}`",
        f"- runbook_status: `{runbook.get('status', '')}`",
        f"- runbook_next_key: `{runbook.get('next_key', '')}`",
        f"- goal_status: `{goal.get('overall_status', '')}`",
        f"- goal_blockers: `{', '.join(goal.get('blockers') or []) or 'none'}`",
        f"- transport_decision: `{transport.get('decision_status', '')}`",
        f"- transport_action: `{transport.get('direct_candidate_action', '')}`",
        f"- transport_promotable: `{transport.get('transport_promotable', '')}`",
        f"- candidate_positive_count: `{transport.get('candidate_positive_count', 0)}`",
        f"- candidate_negative_count: `{transport.get('candidate_negative_count', 0)}`",
        f"- pending_rerun_routes: `{', '.join(transport.get('pending_rerun_routes') or []) or 'none'}`",
        f"- demo_readiness: `{demo.get('status', '')}`",
        "",
        "## Next Command",
        "",
        "```bash",
        str(payload.get("next_command") or ""),
        "```",
        "",
        "## Execution Artifact",
        "",
        f"- path: `{execution.get('path', '')}`",
        f"- reason: `{execution.get('reason', '')}`",
        f"- initial_next_key: `{execution.get('initial_next_key', '')}`",
        f"- final_next_key: `{execution.get('final_next_key', '')}`",
        "",
        "## Triage",
        "",
    ]
    triage = _dict(payload.get("triage"))
    for key in ["status", "reason", "next_command"]:
        lines.append(f"- {key}: `{triage.get(key, '')}`")
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Summarize the current state immediately after a Town01 online run.")
    parser.add_argument("--python-exec", default=CONDA_CARLA16_PYTHON)
    parser.add_argument("--execution-json", type=Path, default=DEFAULT_EXECUTION_JSON)
    parser.add_argument(
        "--allow-dry-run-fallback",
        action="store_true",
        help="Use the dry-run execution artifact if the real online execution artifact is missing.",
    )
    parser.add_argument("--json-out", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_OUTPUT_MD)
    parser.add_argument("--print-command", action="store_true")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    payload = build_after_online_payload(
        python_exec=str(args.python_exec),
        execution_path=args.execution_json,
        allow_dry_run_fallback=bool(args.allow_dry_run_fallback),
    )
    write_json(args.json_out, payload)
    write_markdown(args.md_out, payload)
    if args.print_command:
        print(payload.get("next_command", ""))
    else:
        print(json.dumps({key: payload[key] for key in ["status", "reason", "next_command"]}, indent=2))


if __name__ == "__main__":
    main()
