#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.run_town01_goal_online_runbook import (
    DEFAULT_DRY_RUN_OUTPUT_JSON as DEFAULT_DRY_RUN_EXECUTION_JSON,
    DEFAULT_OUTPUT_JSON as DEFAULT_EXECUTION_JSON,
)

DEFAULT_OUTPUT_JSON = REPO_ROOT / "artifacts" / "town01_goal_online_runbook_execution_inspection_20260522.json"
DEFAULT_OUTPUT_MD = REPO_ROOT / "artifacts" / "town01_goal_online_runbook_execution_inspection_20260522.md"


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.expanduser().read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _snapshot_result_status(snapshot: Dict[str, Any], key: str) -> str:
    results = snapshot.get("results") if isinstance(snapshot.get("results"), dict) else {}
    item = results.get(key) if isinstance(results.get(key), dict) else {}
    return str(item.get("status") or "")


def _snapshot_result_reason(snapshot: Dict[str, Any], key: str) -> str:
    results = snapshot.get("results") if isinstance(snapshot.get("results"), dict) else {}
    item = results.get(key) if isinstance(results.get(key), dict) else {}
    return str(item.get("reason") or "")


def _status_counts(steps: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for step in steps:
        status = str(step.get("status") or "unknown")
        counts[status] = counts.get(status, 0) + 1
    return counts


def _failed_steps(steps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    failed: List[Dict[str, Any]] = []
    for step in steps:
        status = str(step.get("status") or "")
        returncode = step.get("returncode")
        if status == "failed" or (returncode not in {None, 0} and not bool(step.get("evidence_progress"))):
            failed.append(
                {
                    "key": str(step.get("key") or ""),
                    "status": status,
                    "returncode": returncode,
                    "status_before": str(step.get("status_before") or ""),
                    "status_after": str(step.get("status_after") or ""),
                    "next_key_after": str(step.get("next_key_after") or ""),
                }
            )
    return failed


def _result_deltas(initial: Dict[str, Any], final: Dict[str, Any]) -> List[Dict[str, str]]:
    initial_results = initial.get("results") if isinstance(initial.get("results"), dict) else {}
    final_results = final.get("results") if isinstance(final.get("results"), dict) else {}
    keys = list(dict.fromkeys([*initial_results.keys(), *final_results.keys()]))
    rows: List[Dict[str, str]] = []
    for key in keys:
        before = _snapshot_result_status(initial, str(key))
        after = _snapshot_result_status(final, str(key))
        if before != after:
            rows.append(
                {
                    "key": str(key),
                    "before": before,
                    "after": after,
                    "reason": _snapshot_result_reason(final, str(key)),
                }
            )
    return rows


def _compact_post_run_refresh(payload: Dict[str, Any]) -> Dict[str, Any]:
    refresh = payload.get("post_run_refresh") if isinstance(payload.get("post_run_refresh"), dict) else {}
    results = refresh.get("results") if isinstance(refresh.get("results"), dict) else {}
    next_action = refresh.get("next_action") if isinstance(refresh.get("next_action"), dict) else {}
    handoff = refresh.get("handoff") if isinstance(refresh.get("handoff"), dict) else {}
    audit = refresh.get("offline_audit") if isinstance(refresh.get("offline_audit"), dict) else {}
    after_online = refresh.get("after_online_intake") if isinstance(refresh.get("after_online_intake"), dict) else {}
    return {
        "present": bool(refresh),
        "results_status": str(results.get("status") or ""),
        "results_next_key": str(results.get("next_key") or ""),
        "results_json": str(results.get("json") or ""),
        "results_md": str(results.get("md") or ""),
        "next_action_status": str(next_action.get("status") or ""),
        "next_action_primary_key": str(next_action.get("primary_key") or ""),
        "next_action_json": str(next_action.get("json") or ""),
        "next_action_md": str(next_action.get("md") or ""),
        "handoff_status": str(handoff.get("status") or ""),
        "handoff_selected_next_key": str(handoff.get("selected_next_key") or ""),
        "handoff_json": str(handoff.get("json") or ""),
        "handoff_md": str(handoff.get("md") or ""),
        "offline_audit_status": str(audit.get("status") or ""),
        "offline_audit_json": str(audit.get("json") or ""),
        "offline_audit_md": str(audit.get("md") or ""),
        "after_online_status": str(after_online.get("status") or ""),
        "after_online_json": str(after_online.get("json") or ""),
        "after_online_md": str(after_online.get("md") or ""),
        "after_online_next_command": str(after_online.get("next_command") or ""),
    }


def classify_execution(payload: Dict[str, Any]) -> Dict[str, Any]:
    execution_id = str(payload.get("execution_id") or "")
    raw_status = str(payload.get("status") or "")
    dry_run = bool(payload.get("dry_run")) or raw_status == "dry_run"
    steps = [step for step in payload.get("steps") or [] if isinstance(step, dict)]
    initial_snapshot = payload.get("initial_results_snapshot") if isinstance(payload.get("initial_results_snapshot"), dict) else {}
    final_snapshot = payload.get("final_results_snapshot") if isinstance(payload.get("final_results_snapshot"), dict) else {}
    initial_next_key = str(payload.get("initial_next_key") or "")
    final_next_key = str(payload.get("final_next_key") or "")
    failed = _failed_steps(steps)
    status_counts = _status_counts(steps)
    evidence_progress_count = sum(1 for step in steps if bool(step.get("evidence_progress")))
    result_deltas = _result_deltas(initial_snapshot, final_snapshot)
    final_result_status = str(final_snapshot.get("status") or "")
    post_run_refresh = _compact_post_run_refresh(payload)

    if not payload:
        status = "missing"
        reason = "execution artifact is missing or unreadable"
    elif dry_run:
        status = "dry_run_only"
        reason = "execution artifact is a dry-run plan, not online evidence"
    elif raw_status == "completed_goal_ready" or final_result_status == "ready_for_goal_completion_review":
        status = "goal_ready"
        reason = "execution reached ready_for_goal_completion_review"
    elif failed:
        status = "failed_without_evidence_progress"
        reason = f"{len(failed)} step(s) failed without evidence progress"
    elif final_next_key and final_next_key != initial_next_key:
        status = "advanced_to_next_gate"
        reason = f"next_key advanced from {initial_next_key} to {final_next_key}"
    elif evidence_progress_count:
        status = "evidence_progress"
        reason = "at least one step reported evidence progress"
    elif raw_status == "stopped_without_progress":
        status = "stopped_without_progress"
        reason = str(payload.get("reason") or "execution stopped without gate progress")
    elif raw_status == "completed_one_step":
        status = "completed_one_step_no_gate_change"
        reason = "one command completed, but the next gate did not change"
    else:
        status = raw_status or "unknown"
        reason = str(payload.get("reason") or "")

    next_command = str(final_snapshot.get("next_command") or "")
    next_action = "inspect_latest_artifacts"
    if status in {"advanced_to_next_gate", "evidence_progress", "completed_one_step_no_gate_change"} and next_command:
        next_action = "run_or_inspect_next_gate"
    elif status == "goal_ready":
        next_action = "run_final_strict_offline_audit"
    elif status == "dry_run_only":
        next_action = "run_online_executor_without_dry_run"
    elif status == "failed_without_evidence_progress":
        next_action = "inspect_failed_step_artifacts_before_retry"

    return {
        "status": status,
        "reason": reason,
        "execution_id": execution_id,
        "raw_status": raw_status,
        "dry_run": dry_run,
        "started_at_s": payload.get("started_at_s"),
        "finished_at_s": payload.get("finished_at_s"),
        "duration_s": payload.get("duration_s"),
        "initial_next_key": initial_next_key,
        "final_next_key": final_next_key,
        "final_result_status": final_result_status,
        "step_count": len(steps),
        "step_status_counts": status_counts,
        "evidence_progress_count": evidence_progress_count,
        "failed_steps": failed,
        "result_deltas": result_deltas,
        "post_run_refresh": post_run_refresh,
        "next_action": next_action,
        "next_command": next_command,
    }


def inspect_execution(path: Path) -> Dict[str, Any]:
    resolved = path.expanduser().resolve()
    payload = _load_json(resolved)
    classified = classify_execution(payload)
    classified["path"] = str(resolved)
    return classified


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_markdown(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Town01 Goal Online Runbook Execution Inspection",
        "",
        f"- status: `{payload.get('status', '')}`",
        f"- reason: `{payload.get('reason', '')}`",
        f"- execution_id: `{payload.get('execution_id', '')}`",
        f"- raw_status: `{payload.get('raw_status', '')}`",
        f"- started_at_s: `{payload.get('started_at_s', '')}`",
        f"- finished_at_s: `{payload.get('finished_at_s', '')}`",
        f"- duration_s: `{payload.get('duration_s', '')}`",
        f"- initial_next_key: `{payload.get('initial_next_key', '')}`",
        f"- final_next_key: `{payload.get('final_next_key', '')}`",
        f"- final_result_status: `{payload.get('final_result_status', '')}`",
        f"- next_action: `{payload.get('next_action', '')}`",
        f"- path: `{payload.get('path', '')}`",
        "",
        "## Post-Run Refresh",
        "",
    ]
    refresh = payload.get("post_run_refresh") if isinstance(payload.get("post_run_refresh"), dict) else {}
    if refresh.get("present"):
        lines.extend(
            [
                f"- results_status: `{refresh.get('results_status', '')}`",
                f"- results_next_key: `{refresh.get('results_next_key', '')}`",
                f"- next_action_status: `{refresh.get('next_action_status', '')}`",
                f"- next_action_primary_key: `{refresh.get('next_action_primary_key', '')}`",
                f"- handoff_status: `{refresh.get('handoff_status', '')}`",
                f"- handoff_selected_next_key: `{refresh.get('handoff_selected_next_key', '')}`",
                f"- offline_audit_status: `{refresh.get('offline_audit_status', '')}`",
                f"- after_online_status: `{refresh.get('after_online_status', '')}`",
                f"- results_md: `{refresh.get('results_md', '')}`",
                f"- next_action_md: `{refresh.get('next_action_md', '')}`",
                f"- handoff_md: `{refresh.get('handoff_md', '')}`",
                f"- offline_audit_md: `{refresh.get('offline_audit_md', '')}`",
                f"- after_online_md: `{refresh.get('after_online_md', '')}`",
            ]
        )
    else:
        lines.append("- not present")
    lines.extend(
        [
            "",
        "## Step Counts",
        "",
        ]
    )
    counts = payload.get("step_status_counts") if isinstance(payload.get("step_status_counts"), dict) else {}
    if counts:
        for status, count in counts.items():
            lines.append(f"- `{status}`: `{count}`")
    else:
        lines.append("- none")
    lines.extend(["", "## Result Deltas", "", "| key | before | after | reason |", "|---|---|---|---|"])
    for row in payload.get("result_deltas") or []:
        lines.append(
            "| `{}` | `{}` | `{}` | `{}` |".format(
                row.get("key", ""),
                row.get("before", ""),
                row.get("after", ""),
                str(row.get("reason", "")).replace("|", "\\|"),
            )
        )
    if not payload.get("result_deltas"):
        lines.append("| none |  |  |  |")
    lines.extend(["", "## Failed Steps", "", "| key | status | returncode | status_before | status_after |", "|---|---|---:|---|---|"])
    for step in payload.get("failed_steps") or []:
        returncode = step.get("returncode")
        lines.append(
            "| `{}` | `{}` | {} | `{}` | `{}` |".format(
                step.get("key", ""),
                step.get("status", ""),
                "" if returncode is None else str(returncode),
                step.get("status_before", ""),
                step.get("status_after", ""),
            )
        )
    if not payload.get("failed_steps"):
        lines.append("| none |  |  |  |  |")
    next_command = str(payload.get("next_command") or "")
    if next_command:
        lines.extend(["", "## Next Command From Final Snapshot", "", "```bash", next_command, "```"])
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect a Town01 online runbook execution artifact.")
    parser.add_argument("--execution-json", type=Path, default=DEFAULT_EXECUTION_JSON)
    parser.add_argument(
        "--allow-dry-run-fallback",
        action="store_true",
        help="Use the dry-run execution artifact if the online artifact is missing.",
    )
    parser.add_argument("--json-out", type=Path, default=DEFAULT_OUTPUT_JSON)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_OUTPUT_MD)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    execution_json = args.execution_json
    if bool(args.allow_dry_run_fallback) and not execution_json.exists():
        execution_json = DEFAULT_DRY_RUN_EXECUTION_JSON
    payload = inspect_execution(execution_json)
    write_json(args.json_out, payload)
    write_markdown(args.md_out, payload)
    print(json.dumps({"status": payload["status"], "next_action": payload["next_action"]}, indent=2))


if __name__ == "__main__":
    main()
