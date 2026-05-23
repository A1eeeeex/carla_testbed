#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.analyze_town01_direct_curve_recovery import DEFAULT_LABEL_HINT, DEFAULT_ROUTE_IDS, find_latest_candidate_roots
from tools.analyze_town01_transport_ab import build_rows
from tools.inspect_town01_goal_resume import (
    DEFAULT_OUTPUT_JSON as DEFAULT_RESUME_JSON,
    DEFAULT_OUTPUT_MD as DEFAULT_RESUME_MD,
    build_resume_payload,
    write_markdown as write_resume_markdown,
)
from tools.inspect_town01_goal_status import (
    CONDA_CARLA16_PYTHON,
    DEFAULT_OUTPUT_JSON as DEFAULT_GOAL_STATUS_JSON,
    DEFAULT_OUTPUT_MD as DEFAULT_GOAL_STATUS_MD,
    build_goal_status,
    write_markdown as write_goal_markdown,
)
from tools.inspect_town01_online_preflight import build_preflight_payload

DEFAULT_BASELINE_ROOT = REPO_ROOT / "runs" / "town01_transport_ab_20260522_160253" / "baseline"
DEFAULT_CONFIG = (
    REPO_ROOT / "configs" / "io" / "examples" / "town01_apollo_route_health_behavior_recovery_stitcher_v1_direct_candidate.yaml"
)
RETRYABLE_VERDICTS = {
    "candidate_missing",
    "candidate_inconclusive_runtime_interrupted",
    "candidate_stale_summary",
}

CommandRunner = Callable[[Sequence[str]], int]
RouteAnalyzer = Callable[[Path, Path, str, str], Dict[str, Any]]
PayloadBuilder = Callable[[], Dict[str, Any]]
ResumeBuilder = Callable[..., Dict[str, Any]]


def _route_output_token(route_id: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in str(route_id).strip())


def default_output_paths(route_id: str, *, dry_run: bool = False) -> tuple[Path, Path]:
    token = _route_output_token(route_id) or "unknown_route"
    dry_run_suffix = "_dry_run" if dry_run else ""
    stem = f"town01_direct_curve_recovery_retry_{token}{dry_run_suffix}_20260522"
    return REPO_ROOT / "artifacts" / f"{stem}.json", REPO_ROOT / "artifacts" / f"{stem}.md"


def build_chain_command(
    *,
    python_exec: str,
    route_id: str,
    config_path: Path = DEFAULT_CONFIG,
    ticks: int = 420,
    post_fail_steps: int = 120,
    label_hint: str = DEFAULT_LABEL_HINT,
) -> List[str]:
    return [
        str(Path(python_exec).expanduser()),
        "tools/run_town01_capability_online_chain.py",
        "--enable-lateral",
        "--enable-guard",
        "--config",
        str(config_path.expanduser().resolve()),
        "--startup-profile",
        "render_offscreen_no_ros2",
        "--carla-world-ready-timeout-sec",
        "180",
        "--carla-launch-attempts",
        "1",
        "--ticks",
        str(int(ticks)),
        "--post-fail-steps",
        str(int(post_fail_steps)),
        "--progress-update-sec",
        "10",
        "--comparison-label-suffix",
        str(label_hint),
        "--continue-on-failure",
        "--carla-ignore-memory-preflight",
        "--step",
        f"curve_lane_follow:{route_id}",
    ]


def route_verdict_from_latest_candidate(
    baseline_root: Path,
    runs_root: Path,
    route_id: str,
    label_hint: str,
) -> Dict[str, Any]:
    try:
        candidate_roots = find_latest_candidate_roots(runs_root, route_ids=[route_id], label_hint=label_hint)
    except FileNotFoundError as exc:
        return {
            "route_id": route_id,
            "verdict": "candidate_missing",
            "candidate_invalid_reason": "",
            "reason": str(exc),
            "candidate_roots": [],
        }
    rows = build_rows([baseline_root.expanduser().resolve()], candidate_roots, route_ids=[route_id])
    row = rows[0] if rows else {"route_id": route_id, "verdict": "candidate_missing"}
    row["candidate_roots"] = [str(path) for path in candidate_roots]
    return row


def should_retry_verdict(verdict: str) -> bool:
    return str(verdict or "") in RETRYABLE_VERDICTS


def _summary_mtime_s(path_text: str) -> float | None:
    raw = str(path_text or "").strip()
    if not raw:
        return None
    try:
        path = Path(raw).expanduser()
        return float(path.stat().st_mtime)
    except Exception:
        return None


def mark_stale_summary_if_needed(
    row: Dict[str, Any],
    *,
    attempt_started_at_s: float,
    tolerance_s: float = 1.0,
) -> Dict[str, Any]:
    checked = dict(row)
    summary_path = str(checked.get("candidate_summary") or "")
    summary_mtime = _summary_mtime_s(summary_path)
    checked["candidate_summary_mtime_s"] = summary_mtime
    checked["candidate_summary_new_for_attempt"] = (
        summary_mtime is not None and summary_mtime >= float(attempt_started_at_s) - float(tolerance_s)
    )
    if summary_path and summary_mtime is not None and not checked["candidate_summary_new_for_attempt"]:
        previous_verdict = str(checked.get("verdict") or "")
        checked["stale_previous_verdict"] = previous_verdict
        checked["verdict"] = "candidate_stale_summary"
        checked["candidate_invalid_reason"] = "latest candidate summary predates this attempt"
    return checked


def _subprocess_runner(cmd: Sequence[str]) -> int:
    return int(subprocess.run(list(cmd), cwd=str(REPO_ROOT), check=False).returncode)


def should_refresh_after_run(*, dry_run: bool, refresh_goal_after_run: bool) -> bool:
    return bool(refresh_goal_after_run) and not bool(dry_run)


def refresh_goal_artifacts(
    *,
    goal_json: Path = DEFAULT_GOAL_STATUS_JSON,
    goal_md: Path = DEFAULT_GOAL_STATUS_MD,
    resume_json: Path = DEFAULT_RESUME_JSON,
    resume_md: Path = DEFAULT_RESUME_MD,
    refresh_preflight: bool = True,
    goal_builder: PayloadBuilder = build_goal_status,
    resume_builder: ResumeBuilder = build_resume_payload,
) -> Dict[str, Any]:
    goal_payload = goal_builder()
    write_json(goal_json, goal_payload)
    write_goal_markdown(goal_md, goal_payload)
    resume_payload = resume_builder(refresh_preflight=bool(refresh_preflight))
    write_json(resume_json, resume_payload)
    write_resume_markdown(resume_md, resume_payload)
    return {
        "status": "refreshed",
        "goal_status": str(goal_payload.get("overall_status") or ""),
        "resume_status": str((resume_payload.get("resume_state") or {}).get("status") or ""),
        "goal_json": str(goal_json),
        "goal_md": str(goal_md),
        "resume_json": str(resume_json),
        "resume_md": str(resume_md),
    }


def run_retry(
    *,
    route_id: str,
    retries: int,
    python_exec: str,
    config_path: Path,
    baseline_root: Path,
    runs_root: Path,
    label_hint: str,
    ticks: int,
    post_fail_steps: int,
    preflight: bool,
    dry_run: bool,
    retry_delay_s: float = 2.0,
    command_runner: CommandRunner = _subprocess_runner,
    route_analyzer: RouteAnalyzer = route_verdict_from_latest_candidate,
) -> Dict[str, Any]:
    attempts: List[Dict[str, Any]] = []
    preflight_payload: Dict[str, Any] = {}
    if preflight:
        preflight_payload = build_preflight_payload(config_path=config_path, python_exec=python_exec)
        if preflight_payload.get("status") == "failed":
            return {
                "status": "preflight_failed",
                "route_id": route_id,
                "preflight": preflight_payload,
                "attempts": attempts,
                "final_verdict": "",
            }

    max_attempts = max(int(retries), 0) + 1
    command = build_chain_command(
        python_exec=python_exec,
        route_id=route_id,
        config_path=config_path,
        ticks=ticks,
        post_fail_steps=post_fail_steps,
        label_hint=label_hint,
    )
    if dry_run:
        return {
            "status": "dry_run",
            "route_id": route_id,
            "preflight": preflight_payload,
            "attempts": [{"attempt": 1, "returncode": None, "verdict": "not_run", "command": command}],
            "final_verdict": "not_run",
        }

    final_verdict = ""
    for attempt_index in range(1, max_attempts + 1):
        attempt_started_at_s = time.time()
        returncode = command_runner(command)
        attempt_finished_at_s = time.time()
        row = route_analyzer(baseline_root, runs_root, route_id, label_hint)
        row = mark_stale_summary_if_needed(row, attempt_started_at_s=attempt_started_at_s)
        verdict = str(row.get("verdict") or "")
        final_verdict = verdict
        retrying = attempt_index < max_attempts and should_retry_verdict(verdict)
        attempts.append(
            {
                "attempt": attempt_index,
                "attempt_started_at_s": attempt_started_at_s,
                "attempt_finished_at_s": attempt_finished_at_s,
                "returncode": int(returncode),
                "verdict": verdict,
                "stale_previous_verdict": str(row.get("stale_previous_verdict") or ""),
                "candidate_summary_new_for_attempt": row.get("candidate_summary_new_for_attempt"),
                "candidate_summary_mtime_s": row.get("candidate_summary_mtime_s"),
                "candidate_invalid_reason": str(row.get("candidate_invalid_reason") or ""),
                "candidate_summary": str(row.get("candidate_summary") or ""),
                "candidate_roots": row.get("candidate_roots") or [],
                "retrying": bool(retrying),
                "command": command,
            }
        )
        if not retrying:
            break
        if retry_delay_s > 0.0:
            time.sleep(float(retry_delay_s))

    if should_retry_verdict(final_verdict):
        status = "retryable_verdict_exhausted"
    else:
        status = "completed"
    return {
        "status": status,
        "route_id": route_id,
        "preflight": preflight_payload,
        "attempts": attempts,
        "final_verdict": final_verdict,
    }


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_markdown(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Town01 Direct Curve Recovery Retry",
        "",
        f"- status: `{payload['status']}`",
        f"- route_id: `{payload['route_id']}`",
        f"- final_verdict: `{payload.get('final_verdict', '')}`",
        f"- preflight_status: `{(payload.get('preflight') or {}).get('status', '')}`",
        f"- post_run_refresh: `{(payload.get('post_run_refresh') or {}).get('status', '')}`",
        "",
        "| attempt | returncode | verdict | summary_new | stale_previous | invalid_reason | retrying |",
        "|---:|---:|---|---|---|---|---|",
    ]
    for attempt in payload.get("attempts", []):
        returncode = attempt.get("returncode")
        returncode_cell = "" if returncode is None else str(returncode)
        lines.append(
            "| {} | {} | `{}` | `{}` | `{}` | `{}` | `{}` |".format(
                attempt.get("attempt", ""),
                returncode_cell,
                attempt.get("verdict", ""),
                attempt.get("candidate_summary_new_for_attempt", ""),
                attempt.get("stale_previous_verdict", ""),
                attempt.get("candidate_invalid_reason", ""),
                bool(attempt.get("retrying", False)),
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def exit_code_for_payload(payload: Dict[str, Any]) -> int:
    if payload.get("status") in {"dry_run", "completed"}:
        return 0
    return 2


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one direct curve recovery route with retry on runtime-interrupted evidence.")
    parser.add_argument("--route-id", choices=DEFAULT_ROUTE_IDS, required=True)
    parser.add_argument("--retries", type=int, default=1)
    parser.add_argument("--python-exec", default=CONDA_CARLA16_PYTHON)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--baseline-root", type=Path, default=DEFAULT_BASELINE_ROOT)
    parser.add_argument("--runs-root", type=Path, default=REPO_ROOT / "runs")
    parser.add_argument("--label-hint", default=DEFAULT_LABEL_HINT)
    parser.add_argument("--ticks", type=int, default=420)
    parser.add_argument("--post-fail-steps", type=int, default=120)
    parser.add_argument("--preflight", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--retry-delay-s", type=float, default=2.0)
    parser.add_argument(
        "--refresh-goal-after-run",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="After a real online attempt, refresh goal status and resume artifacts automatically.",
    )
    parser.add_argument(
        "--refresh-preflight-after-run",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="When refreshing the goal resume after a real run, also refresh the local online preflight block.",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--json-out",
        type=Path,
        default=None,
        help="Output JSON path. Defaults to a route-specific artifact under artifacts/.",
    )
    parser.add_argument(
        "--md-out",
        type=Path,
        default=None,
        help="Output Markdown path. Defaults to a route-specific artifact under artifacts/.",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    payload = run_retry(
        route_id=str(args.route_id),
        retries=int(args.retries),
        python_exec=str(args.python_exec),
        config_path=args.config,
        baseline_root=args.baseline_root,
        runs_root=args.runs_root,
        label_hint=str(args.label_hint),
        ticks=int(args.ticks),
        post_fail_steps=int(args.post_fail_steps),
        preflight=bool(args.preflight),
        dry_run=bool(args.dry_run),
        retry_delay_s=float(args.retry_delay_s),
    )
    default_json, default_md = default_output_paths(str(args.route_id), dry_run=bool(args.dry_run))
    json_out = args.json_out or default_json
    md_out = args.md_out or default_md
    payload["post_run_refresh"] = {
        "status": "pending",
    }
    write_json(json_out, payload)
    write_markdown(md_out, payload)
    if should_refresh_after_run(dry_run=bool(args.dry_run), refresh_goal_after_run=bool(args.refresh_goal_after_run)):
        payload["post_run_refresh"] = refresh_goal_artifacts(
            refresh_preflight=bool(args.refresh_preflight_after_run),
        )
        write_json(json_out, payload)
        write_markdown(md_out, payload)
    else:
        payload["post_run_refresh"] = {
            "status": "skipped",
            "reason": "dry_run" if args.dry_run else "disabled",
        }
        write_json(json_out, payload)
        write_markdown(md_out, payload)
    print(json.dumps({"status": payload["status"], "route_id": payload["route_id"], "final_verdict": payload.get("final_verdict", "")}, indent=2))
    raise SystemExit(exit_code_for_payload(payload))


if __name__ == "__main__":
    main()
