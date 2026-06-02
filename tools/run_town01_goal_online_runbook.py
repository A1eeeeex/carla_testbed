#!/usr/bin/env python3
# LEGACY / OPERATIONAL HELPER: retained for historical Town01 online runbook orchestration.
# Do not add new platform logic here; move reusable code into carla_testbed.experiments.
# Migration target: carla_testbed.experiments natural driving runner.
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

from tools.inspect_town01_goal_online_runbook_results import (
    DEFAULT_OUTPUT_JSON as DEFAULT_RESULTS_JSON,
    DEFAULT_OUTPUT_MD as DEFAULT_RESULTS_MD,
    PASSING_STATUSES,
    build_results_payload,
    write_json as write_results_json,
    write_markdown as write_results_markdown,
)
from tools.inspect_town01_goal_status import CONDA_CARLA16_PYTHON
from tools.inspect_town01_next_online_action import (
    DEFAULT_OUTPUT_JSON as DEFAULT_NEXT_ACTION_JSON,
    DEFAULT_OUTPUT_MD as DEFAULT_NEXT_ACTION_MD,
    build_next_action_payload,
    write_json as write_next_action_json,
    write_markdown as write_next_action_markdown,
)
from tools.prepare_town01_operator_handoff import (
    DEFAULT_OUTPUT_JSON as DEFAULT_HANDOFF_JSON,
    DEFAULT_OUTPUT_MD as DEFAULT_HANDOFF_MD,
    build_handoff_payload,
    write_json as write_handoff_json,
    write_markdown as write_handoff_markdown,
)
from tools.prepare_town01_goal_online_runbook import build_runbook_payload

DEFAULT_OUTPUT_JSON = REPO_ROOT / "artifacts" / "town01_goal_online_runbook_execution_20260522.json"
DEFAULT_OUTPUT_MD = REPO_ROOT / "artifacts" / "town01_goal_online_runbook_execution_20260522.md"
DEFAULT_DRY_RUN_OUTPUT_JSON = REPO_ROOT / "artifacts" / "town01_goal_online_runbook_execution_dry_run_20260522.json"
DEFAULT_DRY_RUN_OUTPUT_MD = REPO_ROOT / "artifacts" / "town01_goal_online_runbook_execution_dry_run_20260522.md"

CommandRunner = Callable[[Sequence[str]], int]
RunbookBuilder = Callable[[str], Dict[str, Any]]
ResultsBuilder = Callable[[str], Dict[str, Any]]
NoArgBuilder = Callable[[], Dict[str, Any]]
RefreshRunner = Callable[..., Dict[str, Any]]
AfterOnlineBuilder = Callable[..., Dict[str, Any]]


def _shell(cmd: Sequence[str]) -> str:
    return " ".join(shlex.quote(str(part)) for part in cmd)


def _subprocess_runner(cmd: Sequence[str]) -> int:
    return int(subprocess.run(list(cmd), cwd=str(REPO_ROOT), check=False).returncode)


def _default_runbook_builder(python_exec: str) -> Dict[str, Any]:
    return build_runbook_payload(python_exec=python_exec)


def _default_results_builder(python_exec: str) -> Dict[str, Any]:
    return build_results_payload(python_exec=python_exec)


def _command_by_key(runbook: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {
        str(item.get("key") or ""): item
        for item in list(runbook.get("commands") or [])
        if isinstance(item, dict)
    }


def _result_status(results_payload: Dict[str, Any], key: str) -> str:
    results = results_payload.get("results") if isinstance(results_payload.get("results"), dict) else {}
    item = results.get(key) if isinstance(results.get(key), dict) else {}
    return str(item.get("status") or "")


def _choose_start_key(results_payload: Dict[str, Any], *, start_at: str) -> str:
    if start_at == "next":
        return str(results_payload.get("next_key") or "")
    return start_at


def _finished_status(results_payload: Dict[str, Any]) -> bool:
    return str(results_payload.get("status") or "") == "ready_for_goal_completion_review"


def _has_evidence_progress(
    *,
    current_key: str,
    next_key: str,
    status_after: str,
    results_payload: Dict[str, Any],
) -> bool:
    if _finished_status(results_payload):
        return True
    if str(status_after) in PASSING_STATUSES:
        return True
    return bool(next_key and next_key != current_key)


def _is_without_progress(
    *,
    current_key: str,
    next_key: str,
    status_after: str,
    results_payload: Dict[str, Any],
) -> bool:
    if _finished_status(results_payload):
        return False
    if str(status_after) in PASSING_STATUSES:
        return False
    return bool(next_key == current_key)


def _results_snapshot(results_payload: Dict[str, Any]) -> Dict[str, Any]:
    results = results_payload.get("results") if isinstance(results_payload.get("results"), dict) else {}
    compact_results: Dict[str, Dict[str, Any]] = {}
    for key, item in results.items():
        if not isinstance(item, dict):
            continue
        compact_results[str(key)] = {
            field: item.get(field)
            for field in (
                "status",
                "reason",
                "path",
                "root",
                "triage_status",
                "readiness_status",
            )
            if field in item
        }
        summary = item.get("summary") if isinstance(item.get("summary"), dict) else {}
        if summary:
            compact_results[str(key)]["summary"] = {
                field: summary.get(field)
                for field in (
                    "route_count",
                    "baseline_aligned_count",
                    "candidate_aligned_count",
                    "candidate_control_consuming_count",
                    "candidate_positive_count",
                    "candidate_negative_count",
                    "candidate_inconclusive_count",
                    "verdict_counts",
                )
                if field in summary
            }
    return {
        "status": str(results_payload.get("status") or ""),
        "next_key": str(results_payload.get("next_key") or ""),
        "next_command": str(results_payload.get("next_command") or ""),
        "runbook_status": str(results_payload.get("runbook_status") or ""),
        "results": compact_results,
    }


def run_online_runbook(
    *,
    python_exec: str = CONDA_CARLA16_PYTHON,
    mode: str = "next",
    start_at: str = "next",
    max_steps: int = 1,
    dry_run: bool = False,
    continue_on_failure: bool = False,
    command_runner: CommandRunner = _subprocess_runner,
    runbook_builder: RunbookBuilder = _default_runbook_builder,
    results_builder: ResultsBuilder = _default_results_builder,
) -> Dict[str, Any]:
    started_at_s = time.time()
    execution_id = f"town01_goal_online_runbook_{int(started_at_s * 1000)}"
    max_steps = max(int(max_steps), 1)
    runbook = runbook_builder(str(python_exec))
    command_map = _command_by_key(runbook)
    initial_results = results_builder(str(python_exec))
    current_results = initial_results
    initial_results_snapshot = _results_snapshot(initial_results)
    current_key = _choose_start_key(current_results, start_at=start_at)
    steps: List[Dict[str, Any]] = []

    def build_payload(
        *,
        status: str,
        reason: str,
        final_next_key: str,
        final_results: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        finished_at_s = time.time()
        snapshot_source = final_results if final_results is not None else current_results
        return {
            "execution_id": execution_id,
            "status": status,
            "reason": reason,
            "mode": mode,
            "start_at": start_at,
            "max_steps": max_steps,
            "dry_run": bool(dry_run),
            "started_at_s": started_at_s,
            "finished_at_s": finished_at_s,
            "duration_s": max(0.0, finished_at_s - started_at_s),
            "initial_next_key": str(initial_results.get("next_key") or ""),
            "final_next_key": final_next_key,
            "initial_results_snapshot": initial_results_snapshot,
            "final_results_snapshot": _results_snapshot(snapshot_source),
            "steps": steps,
        }

    if _finished_status(current_results):
        return build_payload(
            status="nothing_to_run",
            reason="runbook results already report ready_for_goal_completion_review",
            final_next_key=current_key,
        )

    for _ in range(max_steps):
        if not current_key:
            break
        before_status = _result_status(current_results, current_key)
        command_item = command_map.get(current_key)
        if command_item is None:
            return build_payload(
                status="missing_command",
                reason=f"runbook command is missing for key {current_key!r}",
                final_next_key=current_key,
            )
        command = [str(part) for part in list(command_item.get("command") or [])]
        step: Dict[str, Any] = {
            "key": current_key,
            "phase": str(command_item.get("phase") or ""),
            "purpose": str(command_item.get("purpose") or ""),
            "status_before": before_status,
            "command": command,
            "returncode": None,
            "status": "planned" if dry_run else "pending",
        }
        if dry_run:
            step["planned_at_s"] = time.time()
            steps.append(step)
            return build_payload(
                status="dry_run",
                reason="planned current runbook command but did not execute it",
                final_next_key=current_key,
            )

        step["started_at_s"] = time.time()
        returncode = command_runner(command)
        step["finished_at_s"] = time.time()
        step["duration_s"] = max(0.0, float(step["finished_at_s"]) - float(step["started_at_s"]))
        step["returncode"] = int(returncode)
        current_results = results_builder(str(python_exec))
        next_key = str(current_results.get("next_key") or "")
        step["next_key_after"] = next_key
        step["status_after"] = _result_status(current_results, current_key)
        evidence_progress = _has_evidence_progress(
            current_key=current_key,
            next_key=next_key,
            status_after=str(step.get("status_after") or ""),
            results_payload=current_results,
        )
        step["evidence_progress"] = evidence_progress
        if int(returncode) == 0:
            step["status"] = "passed"
        elif evidence_progress:
            step["status"] = "evidence_progress_returncode_nonzero"
        else:
            step["status"] = "failed"
        steps.append(step)

        if int(returncode) != 0 and not continue_on_failure and not evidence_progress:
            return build_payload(
                status="stopped_after_failure",
                reason=f"{current_key} returned {returncode}",
                final_next_key=next_key,
            )
        if _is_without_progress(
            current_key=current_key,
            next_key=next_key,
            status_after=str(step.get("status_after") or ""),
            results_payload=current_results,
        ):
            return build_payload(
                status="stopped_without_progress",
                reason=f"next_key remained {current_key}; inspect the latest artifact before retrying",
                final_next_key=next_key,
            )
        if mode == "next":
            reason = "mode=next executed one runbook command"
            if int(returncode) != 0 and evidence_progress:
                reason += "; non-zero return accepted because evidence advanced"
            return build_payload(
                status="completed_one_step",
                reason=reason,
                final_next_key=next_key,
            )
        if _finished_status(current_results):
            return build_payload(
                status="completed_goal_ready",
                reason="runbook results report ready_for_goal_completion_review",
                final_next_key=next_key,
            )
        current_key = next_key

    return build_payload(
        status="max_steps_reached",
        reason=f"executed {len(steps)} step(s); rerun after inspecting results if more work remains",
        final_next_key=str(current_results.get("next_key") or current_key),
    )


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_markdown(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Town01 Goal Online Runbook Execution",
        "",
        f"- status: `{payload.get('status', '')}`",
        f"- reason: `{payload.get('reason', '')}`",
        f"- mode: `{payload.get('mode', '')}`",
        f"- start_at: `{payload.get('start_at', '')}`",
        f"- initial_next_key: `{payload.get('initial_next_key', '')}`",
        f"- final_next_key: `{payload.get('final_next_key', '')}`",
        f"- initial_result_status: `{(payload.get('initial_results_snapshot') or {}).get('status', '')}`",
        f"- final_result_status: `{(payload.get('final_results_snapshot') or {}).get('status', '')}`",
        "",
        "| key | status | returncode | next_key_after | purpose |",
        "|---|---|---:|---|---|",
    ]
    for step in payload.get("steps") or []:
        returncode = step.get("returncode")
        lines.append(
            "| `{}` | `{}` | {} | `{}` | {} |".format(
                step.get("key", ""),
                step.get("status", ""),
                "" if returncode is None else str(returncode),
                step.get("next_key_after", ""),
                str(step.get("purpose", "")).replace("|", "\\|"),
            )
        )
    lines.extend(["", "## Commands", ""])
    for step in payload.get("steps") or []:
        lines.extend(
            [
                f"### {step.get('key', '')}",
                "",
                "```bash",
                _shell(step.get("command") or []),
                "```",
                "",
            ]
        )
    lines.extend(
        [
            "## Result Snapshot",
            "",
            "| key | initial_status | final_status | final_reason |",
            "|---|---|---|---|",
        ]
    )
    initial_results = (payload.get("initial_results_snapshot") or {}).get("results") or {}
    final_results = (payload.get("final_results_snapshot") or {}).get("results") or {}
    keys = list(dict.fromkeys([*initial_results.keys(), *final_results.keys()]))
    for key in keys:
        initial_item = initial_results.get(key) if isinstance(initial_results.get(key), dict) else {}
        final_item = final_results.get(key) if isinstance(final_results.get(key), dict) else {}
        lines.append(
            "| `{}` | `{}` | `{}` | `{}` |".format(
                key,
                initial_item.get("status", ""),
                final_item.get("status", ""),
                str(final_item.get("reason", "")).replace("|", "\\|"),
            )
        )
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def exit_code_for_payload(payload: Dict[str, Any], *, require_goal_ready: bool = False) -> int:
    status = str(payload.get("status") or "")
    if status in {"missing_command", "stopped_after_failure", "stopped_without_progress"}:
        return 1
    if require_goal_ready and status != "completed_goal_ready":
        return 2
    return 0


def default_output_paths(*, dry_run: bool) -> tuple[Path, Path]:
    if dry_run:
        return DEFAULT_DRY_RUN_OUTPUT_JSON, DEFAULT_DRY_RUN_OUTPUT_MD
    return DEFAULT_OUTPUT_JSON, DEFAULT_OUTPUT_MD


def refresh_operator_artifacts(
    *,
    python_exec: str = CONDA_CARLA16_PYTHON,
    include_offline_audit: bool = True,
    run_offline_tests: bool = True,
    results_builder: ResultsBuilder = _default_results_builder,
    next_action_builder: NoArgBuilder | None = None,
    handoff_builder: NoArgBuilder | None = None,
    offline_audit_builder: NoArgBuilder | None = None,
    results_json: Path = DEFAULT_RESULTS_JSON,
    results_md: Path = DEFAULT_RESULTS_MD,
    next_action_json: Path = DEFAULT_NEXT_ACTION_JSON,
    next_action_md: Path = DEFAULT_NEXT_ACTION_MD,
    handoff_json: Path = DEFAULT_HANDOFF_JSON,
    handoff_md: Path = DEFAULT_HANDOFF_MD,
    offline_audit_json: Path | None = None,
    offline_audit_md: Path | None = None,
) -> Dict[str, Any]:
    results_payload = results_builder(str(python_exec))
    write_results_json(results_json, results_payload)
    write_results_markdown(results_md, results_payload)

    if next_action_builder is None:
        next_action_payload = build_next_action_payload(python_exec=str(python_exec))
    else:
        next_action_payload = next_action_builder()
    write_next_action_json(next_action_json, next_action_payload)
    write_next_action_markdown(next_action_md, next_action_payload)

    if handoff_builder is None:
        handoff_payload = build_handoff_payload()
    else:
        handoff_payload = handoff_builder()
    write_handoff_json(handoff_json, handoff_payload)
    write_handoff_markdown(handoff_md, handoff_payload)

    refreshed: Dict[str, Any] = {
        "results": {
            "status": str(results_payload.get("status") or ""),
            "next_key": str(results_payload.get("next_key") or ""),
            "json": str(results_json),
            "md": str(results_md),
        },
        "next_action": {
            "status": str(next_action_payload.get("status") or ""),
            "primary_key": str((next_action_payload.get("primary") or {}).get("key") or ""),
            "json": str(next_action_json),
            "md": str(next_action_md),
        },
        "handoff": {
            "status": str(handoff_payload.get("handoff_status") or ""),
            "selected_next_key": str(((handoff_payload.get("selected_next_action") or {}).get("primary") or {}).get("key") or ""),
            "json": str(handoff_json),
            "md": str(handoff_md),
        },
        "offline_audit": {"status": "skipped", "json": "", "md": ""},
    }

    if include_offline_audit:
        from tools.run_town01_goal_offline_audit import (
            DEFAULT_OUTPUT_JSON as DEFAULT_AUDIT_JSON,
            DEFAULT_OUTPUT_MD as DEFAULT_AUDIT_MD,
            build_audit_payload,
            write_json as write_audit_json,
            write_markdown as write_audit_markdown,
        )

        audit_json = offline_audit_json or DEFAULT_AUDIT_JSON
        audit_md = offline_audit_md or DEFAULT_AUDIT_MD
        if offline_audit_builder is None:
            audit_payload = build_audit_payload(
                python_exec=str(python_exec),
                run_tests=bool(run_offline_tests),
            )
        else:
            audit_payload = offline_audit_builder()
        write_audit_json(audit_json, audit_payload)
        write_audit_markdown(audit_md, audit_payload)
        refreshed["offline_audit"] = {
            "status": str(audit_payload.get("audit_status") or ""),
            "json": str(audit_json),
            "md": str(audit_md),
        }
    return refreshed


def persist_execution_outputs(
    payload: Dict[str, Any],
    *,
    json_out: Path,
    md_out: Path,
    python_exec: str,
    dry_run: bool,
    refresh_after_run: bool,
    include_offline_audit: bool,
    run_offline_tests: bool,
    include_after_online_intake: bool = True,
    after_online_json: Path | None = None,
    after_online_md: Path | None = None,
    after_online_builder: AfterOnlineBuilder | None = None,
    refresh_runner: RefreshRunner = refresh_operator_artifacts,
) -> Dict[str, Any]:
    # Write the current execution before post-run refresh so downstream inspectors
    # cannot accidentally read a stale execution artifact during the refresh.
    write_json(json_out, payload)
    write_markdown(md_out, payload)
    if dry_run or not refresh_after_run:
        return payload
    payload["post_run_refresh"] = refresh_runner(
        python_exec=str(python_exec),
        include_offline_audit=bool(include_offline_audit),
        run_offline_tests=bool(run_offline_tests),
    )
    write_json(json_out, payload)
    write_markdown(md_out, payload)
    if include_after_online_intake:
        from tools.inspect_town01_after_online import (
            DEFAULT_OUTPUT_JSON as DEFAULT_AFTER_ONLINE_JSON,
            DEFAULT_OUTPUT_MD as DEFAULT_AFTER_ONLINE_MD,
            build_after_online_payload,
            write_json as write_after_online_json,
            write_markdown as write_after_online_markdown,
        )

        intake_json = after_online_json or DEFAULT_AFTER_ONLINE_JSON
        intake_md = after_online_md or DEFAULT_AFTER_ONLINE_MD
        if after_online_builder is None:
            intake_payload = build_after_online_payload(
                python_exec=str(python_exec),
                execution_path=json_out,
            )
        else:
            intake_payload = after_online_builder(
                python_exec=str(python_exec),
                execution_path=json_out,
            )
        write_after_online_json(intake_json, intake_payload)
        write_after_online_markdown(intake_md, intake_payload)
        payload["post_run_refresh"]["after_online_intake"] = {
            "status": str(intake_payload.get("status") or ""),
            "reason": str(intake_payload.get("reason") or ""),
            "next_command": str(intake_payload.get("next_command") or ""),
            "json": str(intake_json),
            "md": str(intake_md),
        }
        write_json(json_out, payload)
        write_markdown(md_out, payload)
    return payload


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Execute the Town01 goal online runbook from the current next_key evidence gate."
    )
    parser.add_argument("--python-exec", default=CONDA_CARLA16_PYTHON)
    parser.add_argument("--mode", choices=["next", "until-blocked"], default="next")
    parser.add_argument("--start-at", default="next", help="Runbook key to start at, or 'next'.")
    parser.add_argument("--max-steps", type=int, default=1)
    parser.add_argument("--continue-on-failure", action="store_true")
    parser.add_argument("--require-goal-ready", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--no-refresh-after-run",
        action="store_true",
        help="Do not refresh runbook-results, next-action, handoff, and offline-audit artifacts after a real run.",
    )
    parser.add_argument(
        "--no-refresh-offline-audit",
        action="store_true",
        help="After a real run, refresh lightweight operator artifacts but skip the heavier offline audit.",
    )
    parser.add_argument(
        "--skip-refresh-tests",
        action="store_true",
        help="When refreshing offline audit after a real run, skip unit/pytest checks.",
    )
    parser.add_argument(
        "--no-after-online-intake",
        action="store_true",
        help="Do not write the consolidated after-online intake report after a real run.",
    )
    parser.add_argument("--json-out", type=Path, default=None)
    parser.add_argument("--md-out", type=Path, default=None)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    payload = run_online_runbook(
        python_exec=str(args.python_exec),
        mode=str(args.mode),
        start_at=str(args.start_at),
        max_steps=int(args.max_steps),
        dry_run=bool(args.dry_run),
        continue_on_failure=bool(args.continue_on_failure),
    )
    default_json, default_md = default_output_paths(dry_run=bool(args.dry_run))
    json_out = args.json_out or default_json
    md_out = args.md_out or default_md
    payload = persist_execution_outputs(
        payload,
        json_out=json_out,
        md_out=md_out,
        python_exec=str(args.python_exec),
        dry_run=bool(args.dry_run),
        refresh_after_run=not bool(args.no_refresh_after_run),
        include_offline_audit=not bool(args.no_refresh_offline_audit),
        run_offline_tests=not bool(args.skip_refresh_tests),
        include_after_online_intake=not bool(args.no_after_online_intake),
    )
    print(json.dumps({"status": payload.get("status"), "final_next_key": payload.get("final_next_key")}, indent=2))
    raise SystemExit(exit_code_for_payload(payload, require_goal_ready=bool(args.require_goal_ready)))


if __name__ == "__main__":
    main()
