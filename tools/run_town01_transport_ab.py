#!/usr/bin/env python3
# LEGACY / OPERATIONAL HELPER: retained for historical Town01 transport A/B orchestration.
# Do not add new platform logic here; move reusable code into carla_testbed.experiments or analysis modules.
# Migration target: carla_testbed.experiments.ab_runner and carla_testbed.analysis.transport_ab.
from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CHAIN = REPO_ROOT / "tools" / "run_town01_capability_online_chain.py"
DEFAULT_BASELINE_CONFIG = (
    REPO_ROOT / "configs" / "io" / "examples" / "town01_apollo_route_health_behavior_recovery_stitcher_v1.yaml"
)
DEFAULT_CANDIDATE_CONFIG = (
    REPO_ROOT
    / "configs"
    / "io"
    / "examples"
    / "town01_apollo_route_health_behavior_recovery_stitcher_v1_direct_candidate.yaml"
)
DEFAULT_PYTHON = Path("/home/ubuntu/miniconda3/envs/carla16/bin/python3")

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.town01_route_health import canonical_demo_steps, random_regression_steps
from tools.analyze_town01_transport_ab import build_rows, summarize_rows, write_csv, write_markdown


def _default_python_exec() -> str:
    if DEFAULT_PYTHON.exists():
        return str(DEFAULT_PYTHON)
    return sys.executable


def _selected_steps(route_set: str) -> List[str]:
    canonical = list(canonical_demo_steps(include_curve_diagnostic=False))
    random_pool = list(random_regression_steps())
    if route_set == "canonical":
        return canonical
    if route_set == "random":
        return random_pool
    if route_set == "full":
        return canonical + random_pool
    raise ValueError(f"unsupported route_set={route_set}")


def _load_runtime_scope(config_path: Path) -> dict:
    try:
        payload = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except Exception:
        payload = {}
    apollo_cfg = (((payload.get("algo") or {}).get("apollo") or {}))
    direct_cfg = dict(apollo_cfg.get("direct_bridge") or {})
    control_bridge_cfg = dict(apollo_cfg.get("carla_control_bridge") or {})
    dreamview_cfg = dict(apollo_cfg.get("dreamview") or {})
    runtime_carla_cfg = (((payload.get("runtime") or {}).get("carla") or {}))
    transport_mode = str(apollo_cfg.get("transport_mode") or "ros2_gt").strip().lower() or "ros2_gt"
    return {
        "transport_mode": transport_mode,
        "uses_ros2_gt": bool(transport_mode == "ros2_gt"),
        "uses_ros2_control_bridge": bool(
            transport_mode == "ros2_gt" and control_bridge_cfg.get("enabled", True)
        ),
        "requires_ros2_reexec": False,
        "route_command_mode": str(direct_cfg.get("route_command_mode") or "cyber_direct").strip().lower()
        or "cyber_direct",
        "require_no_ros2_runtime": bool(direct_cfg.get("require_no_ros2_runtime", False)),
        "dreamview_enabled": bool(dreamview_cfg.get("enabled", False)),
        "disable_native_ros2_arg": bool(runtime_carla_cfg.get("disable_native_ros2_arg", False)),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run a Town01 A/B batch between the current ros2_gt baseline and the "
            "carla_direct candidate using the tracked canonical/random route sets."
        )
    )
    parser.add_argument(
        "--route-set",
        choices=("canonical", "random", "full"),
        default="canonical",
        help="Tracked Town01 route-set asset to run for both baseline and candidate.",
    )
    parser.add_argument(
        "--baseline-config",
        type=Path,
        default=DEFAULT_BASELINE_CONFIG,
        help="Current mainline Town01 config.",
    )
    parser.add_argument(
        "--candidate-config",
        type=Path,
        default=DEFAULT_CANDIDATE_CONFIG,
        help="Town01 carla_direct candidate config.",
    )
    parser.add_argument(
        "--startup-profile",
        default=None,
        help=(
            "Legacy compatibility override: startup profile forwarded to both chains. "
            "For fair transport A/B, prefer the per-chain defaults or explicit "
            "--baseline-startup-profile/--candidate-startup-profile."
        ),
    )
    parser.add_argument(
        "--baseline-startup-profile",
        default="render_offscreen",
        help="Startup profile for the ros2_gt baseline chain; default keeps native CARLA ROS2 enabled.",
    )
    parser.add_argument(
        "--candidate-startup-profile",
        default="render_offscreen_no_ros2",
        help="Startup profile for the carla_direct candidate chain; default avoids native CARLA ROS2.",
    )
    parser.add_argument("--carla-world-ready-timeout-sec", type=float, default=180.0)
    parser.add_argument("--carla-launch-attempts", type=int, default=1)
    parser.add_argument("--ticks", type=int, default=420)
    parser.add_argument("--post-fail-steps", type=int, default=120)
    parser.add_argument("--python-exec", default=_default_python_exec())
    parser.add_argument("--batch-root-parent", type=Path, default=None)
    parser.add_argument("--progress-update-sec", type=float, default=3.0)
    parser.add_argument("--continue-on-failure", action="store_true")
    parser.add_argument(
        "--enable-guard",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Forward the current Town01 route-health guard flag to both baseline and candidate chains.",
    )
    parser.add_argument(
        "--stop-on-first-route-failure",
        action="store_true",
        help=(
            "Do not forward online-chain's continue-initial-step-on-failure flag. "
            "By default A/B batches keep collecting later routes when --continue-on-failure is set."
        ),
    )
    parser.add_argument("--keep-carla-alive-at-end", action="store_true")
    parser.add_argument(
        "--no-cleanup-between-chains",
        action="store_true",
        help="Skip the default CARLA process cleanup between baseline and candidate chains.",
    )
    parser.add_argument("--no-prewarm-carla", action="store_true")
    parser.add_argument(
        "--carla-ignore-memory-preflight",
        action="store_true",
        help="Forward the technical-probe memory preflight bypass to both chains.",
    )
    parser.add_argument(
        "--manifest-out",
        type=Path,
        default=None,
        help="Optional explicit output path for the A/B manifest JSON.",
    )
    parser.add_argument(
        "--override",
        action="append",
        default=[],
        help="Additional run_town01_route_health.py --override entries forwarded to both baseline and candidate chains.",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser


def _resolve_startup_profiles(args: argparse.Namespace) -> tuple[str, str]:
    if args.startup_profile:
        profile = str(args.startup_profile)
        return profile, profile
    return str(args.baseline_startup_profile), str(args.candidate_startup_profile)


def _append_steps(cmd: List[str], steps: Iterable[str]) -> None:
    for item in steps:
        cmd.extend(["--step", item])


def _build_command(
    *,
    python_exec: str,
    config_path: Path,
    steps: Sequence[str],
    startup_profile: str,
    world_ready_timeout_sec: float,
    launch_attempts: int,
    ticks: int,
    post_fail_steps: int,
    progress_update_sec: float,
    batch_root_parent: Path | None,
    no_prewarm_carla: bool,
    keep_carla_alive_at_end: bool,
    continue_on_failure: bool,
    continue_initial_step_on_failure: bool,
    comparison_label_suffix: str,
    extra_overrides: Sequence[str],
    carla_ignore_memory_preflight: bool,
    enable_guard: bool,
    dry_run: bool,
) -> List[str]:
    cmd: List[str] = [
        str(Path(python_exec).expanduser()),
        str(DEFAULT_CHAIN),
        "--enable-lateral",
        "--config",
        str(config_path.expanduser().resolve()),
        "--startup-profile",
        startup_profile,
        "--carla-world-ready-timeout-sec",
        str(world_ready_timeout_sec),
        "--carla-launch-attempts",
        str(launch_attempts),
        "--ticks",
        str(ticks),
        "--post-fail-steps",
        str(post_fail_steps),
        "--progress-update-sec",
        str(progress_update_sec),
        "--comparison-label-suffix",
        comparison_label_suffix,
    ]
    if enable_guard:
        cmd.append("--enable-guard")
    if batch_root_parent is not None:
        cmd.extend(["--batch-root-parent", str(batch_root_parent.expanduser().resolve())])
    if no_prewarm_carla:
        cmd.append("--no-prewarm-carla")
    if carla_ignore_memory_preflight:
        cmd.append("--carla-ignore-memory-preflight")
    if keep_carla_alive_at_end:
        cmd.append("--keep-carla-alive-at-end")
    if continue_on_failure:
        cmd.append("--continue-on-failure")
    if continue_initial_step_on_failure:
        cmd.append("--continue-initial-step-on-failure")
    for override in extra_overrides:
        text = str(override or "").strip()
        if text:
            cmd.extend(["--override", text])
    _append_steps(cmd, steps)
    if dry_run:
        cmd.append("--dry-run")
    return cmd


def _default_manifest_out() -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return REPO_ROOT / "artifacts" / f"town01_transport_ab_manifest_{ts}.json"


def _default_ab_batch_root() -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return REPO_ROOT / "runs" / f"town01_transport_ab_{ts}"


def _ab_batch_roots(root: Path) -> tuple[Path, Path]:
    resolved = root.expanduser().resolve()
    return resolved / "baseline", resolved / "candidate"


def _write_manifest(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _run_chain(cmd: Sequence[str], *, allow_failure: bool) -> subprocess.CompletedProcess:
    result = subprocess.run(list(cmd), check=False, cwd=str(REPO_ROOT))
    if result.returncode != 0 and not allow_failure:
        raise subprocess.CalledProcessError(result.returncode, result.args)
    return result


def _cleanup_between_chains(*, enabled: bool) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "enabled": bool(enabled),
        "status": "skipped",
        "started_at": None,
        "duration_sec": 0.0,
        "error": "",
    }
    if not enabled:
        return result
    started = time.monotonic()
    result["started_at"] = datetime.now().isoformat(timespec="seconds")
    try:
        from tools.run_town01_route_health import _cleanup_carla_processes

        _cleanup_carla_processes()
    except Exception as exc:
        result["status"] = "failed"
        result["error"] = repr(exc)
    else:
        result["status"] = "completed"
    finally:
        result["duration_sec"] = round(time.monotonic() - started, 3)
    return result


def _write_ab_report(
    *,
    baseline_root: Path,
    candidate_root: Path,
    csv_out: Path,
    md_out: Path,
) -> Dict[str, object]:
    rows = build_rows([baseline_root], [candidate_root])
    summary = summarize_rows(rows)
    write_csv(csv_out, rows)
    write_markdown(md_out, rows, summary)
    return summary


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    steps = _selected_steps(args.route_set)
    baseline_startup_profile, candidate_startup_profile = _resolve_startup_profiles(args)
    manifest_out = args.manifest_out.expanduser().resolve() if args.manifest_out else _default_manifest_out()
    ab_batch_root = (args.batch_root_parent.expanduser().resolve() if args.batch_root_parent else _default_ab_batch_root())
    baseline_batch_root, candidate_batch_root = _ab_batch_roots(ab_batch_root)
    report_csv = ab_batch_root / "transport_ab_summary.csv"
    report_md = ab_batch_root / "transport_ab_summary.md"

    baseline_cmd = _build_command(
        python_exec=args.python_exec,
        config_path=args.baseline_config,
        steps=steps,
        startup_profile=baseline_startup_profile,
        world_ready_timeout_sec=args.carla_world_ready_timeout_sec,
        launch_attempts=args.carla_launch_attempts,
        ticks=args.ticks,
        post_fail_steps=args.post_fail_steps,
        progress_update_sec=args.progress_update_sec,
        batch_root_parent=baseline_batch_root,
        no_prewarm_carla=args.no_prewarm_carla,
        keep_carla_alive_at_end=args.keep_carla_alive_at_end,
        continue_on_failure=args.continue_on_failure,
        continue_initial_step_on_failure=bool(args.continue_on_failure and not args.stop_on_first_route_failure),
        comparison_label_suffix="transport_ab_baseline",
        extra_overrides=list(args.override or []),
        carla_ignore_memory_preflight=bool(args.carla_ignore_memory_preflight),
        enable_guard=bool(args.enable_guard),
        dry_run=args.dry_run,
    )
    candidate_cmd = _build_command(
        python_exec=args.python_exec,
        config_path=args.candidate_config,
        steps=steps,
        startup_profile=candidate_startup_profile,
        world_ready_timeout_sec=args.carla_world_ready_timeout_sec,
        launch_attempts=args.carla_launch_attempts,
        ticks=args.ticks,
        post_fail_steps=args.post_fail_steps,
        progress_update_sec=args.progress_update_sec,
        batch_root_parent=candidate_batch_root,
        no_prewarm_carla=args.no_prewarm_carla,
        keep_carla_alive_at_end=args.keep_carla_alive_at_end,
        continue_on_failure=args.continue_on_failure,
        continue_initial_step_on_failure=bool(args.continue_on_failure and not args.stop_on_first_route_failure),
        comparison_label_suffix="transport_ab_direct_candidate",
        extra_overrides=list(args.override or []),
        carla_ignore_memory_preflight=bool(args.carla_ignore_memory_preflight),
        enable_guard=bool(args.enable_guard),
        dry_run=args.dry_run,
    )
    payload = {
        "route_set": args.route_set,
        "steps": list(steps),
        "baseline_config": str(args.baseline_config.expanduser().resolve()),
        "candidate_config": str(args.candidate_config.expanduser().resolve()),
        "baseline_runtime_scope": _load_runtime_scope(args.baseline_config.expanduser().resolve()),
        "candidate_runtime_scope": _load_runtime_scope(args.candidate_config.expanduser().resolve()),
        "ab_batch_root": str(ab_batch_root),
        "baseline_batch_root": str(baseline_batch_root),
        "candidate_batch_root": str(candidate_batch_root),
        "report_csv": str(report_csv),
        "report_md": str(report_md),
        "startup_profile": args.startup_profile,
        "baseline_startup_profile": baseline_startup_profile,
        "candidate_startup_profile": candidate_startup_profile,
        "ticks": int(args.ticks),
        "post_fail_steps": int(args.post_fail_steps),
        "carla_world_ready_timeout_sec": float(args.carla_world_ready_timeout_sec),
        "carla_launch_attempts": int(args.carla_launch_attempts),
        "carla_ignore_memory_preflight": bool(args.carla_ignore_memory_preflight),
        "enable_guard": bool(args.enable_guard),
        "cleanup_between_chains": not bool(args.no_cleanup_between_chains),
        "stop_on_first_route_failure": bool(args.stop_on_first_route_failure),
        "overrides": [str(item) for item in list(args.override or []) if str(item).strip()],
        "probe_only_overrides": [str(item) for item in list(args.override or []) if str(item).strip()],
        "baseline_command": baseline_cmd,
        "candidate_command": candidate_cmd,
        "results": {},
    }
    _write_manifest(manifest_out, payload)

    print("[town01-transport-ab] route_set:", args.route_set)
    print("[town01-transport-ab] steps:")
    for index, step in enumerate(steps, start=1):
        print(f"  {index}. {step}")
    print(f"[town01-transport-ab] manifest: {manifest_out}")
    print("[town01-transport-ab] baseline command:")
    print("  " + " ".join(shlex.quote(item) for item in baseline_cmd))
    print("[town01-transport-ab] candidate command:")
    print("  " + " ".join(shlex.quote(item) for item in candidate_cmd))

    if args.dry_run:
        subprocess.run(baseline_cmd, check=True, cwd=str(REPO_ROOT))
        subprocess.run(candidate_cmd, check=True, cwd=str(REPO_ROOT))
        return

    baseline_result = _run_chain(
        baseline_cmd,
        allow_failure=bool(args.continue_on_failure),
    )
    payload["results"]["baseline"] = {
        "returncode": int(baseline_result.returncode),
        "status": "completed" if baseline_result.returncode == 0 else "failed",
    }
    _write_manifest(manifest_out, payload)

    payload["results"]["between_chain_cleanup"] = _cleanup_between_chains(
        enabled=not bool(args.no_cleanup_between_chains)
    )
    _write_manifest(manifest_out, payload)

    candidate_result = _run_chain(
        candidate_cmd,
        allow_failure=bool(args.continue_on_failure),
    )
    payload["results"]["candidate"] = {
        "returncode": int(candidate_result.returncode),
        "status": "completed" if candidate_result.returncode == 0 else "failed",
    }
    payload["analysis_summary"] = _write_ab_report(
        baseline_root=baseline_batch_root,
        candidate_root=candidate_batch_root,
        csv_out=report_csv,
        md_out=report_md,
    )
    _write_manifest(manifest_out, payload)
    print(f"[town01-transport-ab] report_csv: {report_csv}")
    print(f"[town01-transport-ab] report_md: {report_md}")


if __name__ == "__main__":
    main()
