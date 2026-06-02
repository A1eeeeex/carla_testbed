#!/usr/bin/env python3
# LEGACY / OPERATIONAL HELPER: retained for historical direct curve pair retry runs.
# Do not add new platform logic here; move reusable code into carla_testbed.experiments or analysis.
# Migration target: carla_testbed.experiments AB runner and route-health analyzers.
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.analyze_town01_direct_curve_recovery import DEFAULT_ROUTE_IDS
from tools.inspect_town01_goal_status import CONDA_CARLA16_PYTHON, build_goal_status

DEFAULT_OUTPUT_JSON = REPO_ROOT / "artifacts" / "town01_direct_curve_pair_recovery_retry_20260522.json"
DEFAULT_OUTPUT_MD = REPO_ROOT / "artifacts" / "town01_direct_curve_pair_recovery_retry_20260522.md"
DEFAULT_DRY_RUN_OUTPUT_JSON = REPO_ROOT / "artifacts" / "town01_direct_curve_pair_recovery_retry_dry_run_20260522.json"
DEFAULT_DRY_RUN_OUTPUT_MD = REPO_ROOT / "artifacts" / "town01_direct_curve_pair_recovery_retry_dry_run_20260522.md"

GoalBuilder = Callable[[], Dict[str, Any]]
CommandRunner = Callable[[Sequence[str]], int]


def default_output_paths(*, dry_run: bool = False) -> tuple[Path, Path]:
    if dry_run:
        return DEFAULT_DRY_RUN_OUTPUT_JSON, DEFAULT_DRY_RUN_OUTPUT_MD
    return DEFAULT_OUTPUT_JSON, DEFAULT_OUTPUT_MD


def route_id_from_next_command(next_command: Dict[str, Any] | str | None) -> str:
    if isinstance(next_command, dict):
        command = str(next_command.get("command") or "")
    else:
        command = str(next_command or "")
    tokens = command.replace("\\\n", " ").replace("\n", " ").split()
    for index, token in enumerate(tokens):
        if token == "--route-id" and index + 1 < len(tokens):
            return tokens[index + 1].strip().strip("\\")
    return ""


def route_id_from_goal_status(goal_status: Dict[str, Any]) -> str:
    next_command = goal_status.get("next_command") if isinstance(goal_status.get("next_command"), dict) else {}
    return route_id_from_next_command(next_command)


def build_single_route_command(
    *,
    python_exec: str,
    route_id: str,
    retries: int,
    dry_run: bool = False,
    preflight: bool = True,
    refresh_goal_after_run: bool = True,
    refresh_preflight_after_run: bool = True,
) -> List[str]:
    cmd = [
        str(Path(python_exec).expanduser()),
        "tools/run_town01_direct_curve_recovery_retry.py",
        "--route-id",
        str(route_id),
        "--retries",
        str(int(retries)),
        "--python-exec",
        str(python_exec),
    ]
    if not preflight:
        cmd.append("--no-preflight")
    if not refresh_goal_after_run:
        cmd.append("--no-refresh-goal-after-run")
    if not refresh_preflight_after_run:
        cmd.append("--no-refresh-preflight-after-run")
    if dry_run:
        cmd.append("--dry-run")
    return cmd


def ordered_route_plan(goal_status: Dict[str, Any], route_ids: Sequence[str]) -> List[str]:
    allowed = [str(route_id).strip() for route_id in route_ids if str(route_id).strip()]
    selected = route_id_from_goal_status(goal_status)
    if selected and selected in allowed:
        return [selected] + [route_id for route_id in allowed if route_id != selected]
    return allowed


def _goal_brief(goal_status: Dict[str, Any]) -> Dict[str, Any]:
    next_command = goal_status.get("next_command") if isinstance(goal_status.get("next_command"), dict) else {}
    curve = goal_status.get("curve_recovery") if isinstance(goal_status.get("curve_recovery"), dict) else {}
    return {
        "overall_status": str(goal_status.get("overall_status") or ""),
        "blockers": goal_status.get("blockers") or [],
        "next_key": str(next_command.get("key") or ""),
        "next_reason": str(next_command.get("reason") or ""),
        "next_route_id": route_id_from_next_command(next_command),
        "curve_status": str(curve.get("status") or ""),
        "curve_recovery_verdict": str(curve.get("recovery_verdict") or ""),
    }


def _subprocess_runner(cmd: Sequence[str]) -> int:
    return int(subprocess.run(list(cmd), cwd=str(REPO_ROOT), check=False).returncode)


def run_pair_recovery(
    *,
    route_ids: Sequence[str] = DEFAULT_ROUTE_IDS,
    retries: int = 1,
    python_exec: str = CONDA_CARLA16_PYTHON,
    dry_run: bool = False,
    preflight: bool = True,
    refresh_goal_after_run: bool = True,
    refresh_preflight_after_run: bool = True,
    goal_builder: GoalBuilder = build_goal_status,
    command_runner: CommandRunner = _subprocess_runner,
) -> Dict[str, Any]:
    initial_goal = goal_builder()
    plan = ordered_route_plan(initial_goal, route_ids)
    attempts: List[Dict[str, Any]] = []

    if dry_run:
        for route_id in plan:
            attempts.append(
                {
                    "route_id": route_id,
                    "returncode": None,
                    "command": build_single_route_command(
                        python_exec=python_exec,
                        route_id=route_id,
                        retries=retries,
                        dry_run=True,
                        preflight=preflight,
                        refresh_goal_after_run=refresh_goal_after_run,
                        refresh_preflight_after_run=refresh_preflight_after_run,
                    ),
                    "goal_before": _goal_brief(initial_goal),
                    "goal_after": {},
                    "status": "planned",
                }
            )
        return {
            "status": "dry_run",
            "reason": "commands were planned but not executed",
            "route_ids": list(plan),
            "initial_goal": _goal_brief(initial_goal),
            "final_goal": _goal_brief(initial_goal),
            "attempts": attempts,
        }

    allowed = set(plan)
    if not plan:
        return {
            "status": "no_curve_route_selected",
            "reason": "no route ids were provided",
            "route_ids": [],
            "initial_goal": _goal_brief(initial_goal),
            "final_goal": _goal_brief(initial_goal),
            "attempts": attempts,
        }

    goal_before = initial_goal
    visited: set[str] = set()
    final_goal = initial_goal
    status = "no_curve_route_selected"
    reason = "goal status did not select a direct curve route"

    for _ in range(len(plan)):
        route_id = route_id_from_goal_status(goal_before)
        if route_id not in allowed:
            final_goal = goal_before
            status = "completed_gate_transition" if attempts else "no_curve_route_selected"
            reason = (
                "goal next command no longer points to a direct curve route"
                if attempts
                else "current goal next command does not point to a requested direct curve route"
            )
            break
        if route_id in visited:
            final_goal = goal_before
            status = "stopped_after_stalled_gate"
            reason = f"goal still selects already attempted route {route_id}"
            break

        command = build_single_route_command(
            python_exec=python_exec,
            route_id=route_id,
            retries=retries,
            dry_run=False,
            preflight=preflight,
            refresh_goal_after_run=refresh_goal_after_run,
            refresh_preflight_after_run=refresh_preflight_after_run,
        )
        returncode = command_runner(command)
        goal_after = goal_builder()
        final_goal = goal_after
        visited.add(route_id)
        next_route_id = route_id_from_goal_status(goal_after)
        attempts.append(
            {
                "route_id": route_id,
                "returncode": int(returncode),
                "command": command,
                "goal_before": _goal_brief(goal_before),
                "goal_after": _goal_brief(goal_after),
                "status": "executed",
            }
        )

        if next_route_id in allowed and next_route_id not in visited:
            goal_before = goal_after
            continue
        if next_route_id == route_id:
            status = "stopped_after_retry_failure" if int(returncode) != 0 else "stopped_after_stalled_gate"
            reason = f"goal still selects {route_id} after the retry wrapper"
        else:
            status = "completed_gate_transition"
            reason = "goal moved past the direct curve route gate"
        break
    else:
        status = "completed_pair_attempts"
        reason = "all requested direct curve routes were attempted"

    return {
        "status": status,
        "reason": reason,
        "route_ids": list(plan),
        "initial_goal": _goal_brief(initial_goal),
        "final_goal": _goal_brief(final_goal),
        "attempts": attempts,
    }


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_markdown(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Town01 Direct Curve Pair Recovery Retry",
        "",
        f"- status: `{payload.get('status', '')}`",
        f"- reason: `{payload.get('reason', '')}`",
        f"- route_ids: `{', '.join(payload.get('route_ids') or [])}`",
        f"- final_next_key: `{(payload.get('final_goal') or {}).get('next_key', '')}`",
        f"- final_next_route_id: `{(payload.get('final_goal') or {}).get('next_route_id', '')}`",
        f"- final_curve_status: `{(payload.get('final_goal') or {}).get('curve_status', '')}`",
        "",
        "| route_id | status | returncode | before_next | after_next | after_route |",
        "|---|---|---:|---|---|---|",
    ]
    for attempt in payload.get("attempts") or []:
        before = attempt.get("goal_before") if isinstance(attempt.get("goal_before"), dict) else {}
        after = attempt.get("goal_after") if isinstance(attempt.get("goal_after"), dict) else {}
        returncode = attempt.get("returncode")
        returncode_cell = "" if returncode is None else str(returncode)
        lines.append(
            "| `{}` | `{}` | {} | `{}` | `{}` | `{}` |".format(
                attempt.get("route_id", ""),
                attempt.get("status", ""),
                returncode_cell,
                before.get("next_key", ""),
                after.get("next_key", ""),
                after.get("next_route_id", ""),
            )
        )
    lines.extend(["", "## Commands", ""])
    for attempt in payload.get("attempts") or []:
        lines.extend(
            [
                f"### {attempt.get('route_id', '')}",
                "",
                "```bash",
                " ".join(str(part) for part in attempt.get("command") or []),
                "```",
                "",
            ]
        )
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def exit_code_for_payload(payload: Dict[str, Any]) -> int:
    if payload.get("status") in {"dry_run", "completed_gate_transition", "completed_pair_attempts"}:
        return 0
    return 2


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the currently needed Town01 direct curve route, then continue to the next curve route if the goal gate advances."
    )
    parser.add_argument("--route-id", action="append", default=None, choices=DEFAULT_ROUTE_IDS)
    parser.add_argument("--retries", type=int, default=1)
    parser.add_argument("--python-exec", default=CONDA_CARLA16_PYTHON)
    parser.add_argument("--preflight", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--refresh-goal-after-run", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--refresh-preflight-after-run", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--json-out",
        type=Path,
        default=None,
        help="Output JSON path. Defaults to a dry-run-specific path when --dry-run is set.",
    )
    parser.add_argument(
        "--md-out",
        type=Path,
        default=None,
        help="Output Markdown path. Defaults to a dry-run-specific path when --dry-run is set.",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    payload = run_pair_recovery(
        route_ids=list(args.route_id if args.route_id is not None else DEFAULT_ROUTE_IDS),
        retries=int(args.retries),
        python_exec=str(args.python_exec),
        dry_run=bool(args.dry_run),
        preflight=bool(args.preflight),
        refresh_goal_after_run=bool(args.refresh_goal_after_run),
        refresh_preflight_after_run=bool(args.refresh_preflight_after_run),
    )
    default_json, default_md = default_output_paths(dry_run=bool(args.dry_run))
    json_out = args.json_out or default_json
    md_out = args.md_out or default_md
    write_json(json_out, payload)
    write_markdown(md_out, payload)
    print(json.dumps({"status": payload["status"], "reason": payload.get("reason", "")}, indent=2))
    raise SystemExit(exit_code_for_payload(payload))


if __name__ == "__main__":
    main()
