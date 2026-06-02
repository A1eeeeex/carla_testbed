#!/usr/bin/env python3
# LEGACY / OPERATIONAL HELPER: retained for historical Town01 online chain orchestration.
# Do not add new platform logic here; move reusable code into carla_testbed.experiments.
# Migration target: carla_testbed.experiments natural driving and A/B runners.
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
import re
import signal
import shlex
import subprocess
import sys
import time
from typing import Any, Dict, Iterable, List, Sequence, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.run_town01_capability_online_pack import (
    CAPABILITY_CHOICES,
    CONCRETE_STARTUP_PROFILE_CHOICES,
    DEFAULT_CARLA_LAUNCH_ATTEMPTS,
    DEFAULT_CARLA_WORLD_READY_TIMEOUT_SEC,
    DEFAULT_CARLA_RETRY_DELAY_SEC,
    DEFAULT_ESTIMATE_FIXED_DELTA_SECONDS,
    DEFAULT_MANIFEST,
    DEFAULT_STARTUP_PROFILE,
    STARTUP_PROFILE_CHOICES,
    _effective_launch_attempts,
    _default_runtime_flags_for_capability,
    build_online_command,
    estimate_online_time_budget,
    load_manifest_entry,
    resolve_startup_profile_sequence,
    startup_attempt_context,
    startup_profile_note,
    startup_profile_role,
)
from tools.run_town01_route_health import _cleanup_carla_processes, _prestart_carla


DEFAULT_CHAIN_PROFILES = (
    "curve_lane_follow",
    "junction_traverse",
    "traffic_light_actual",
)
DEFAULT_CHAIN_STARTUP_PROFILE = DEFAULT_STARTUP_PROFILE


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run multiple frozen Town01 capability online packs while reusing a single CARLA session."
    )
    parser.add_argument(
        "--capability-profile",
        dest="capability_profiles",
        action="append",
        choices=CAPABILITY_CHOICES,
        default=[],
        help="Capability profile to include. May be passed multiple times. Defaults to curve/junction/traffic.",
    )
    parser.add_argument(
        "--step",
        action="append",
        default=[],
        help=(
            "Ordered adhoc chain item in the form capability_profile:route_id. "
            "When provided, overrides --capability-profile and runs the exact routes while reusing CARLA."
        ),
    )
    parser.add_argument("--mode", choices=("seed", "full"), default="seed")
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Optional explicit Town01 route-health config path forwarded to each chain step.",
    )
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument(
        "--startup-profile",
        choices=STARTUP_PROFILE_CHOICES,
        default=DEFAULT_CHAIN_STARTUP_PROFILE,
    )
    parser.add_argument("--batch-root-parent", type=Path, default=None)
    parser.add_argument("--comparison-label-suffix", default="manual_online_chain")
    parser.add_argument("--ticks", type=int, default=None)
    parser.add_argument(
        "--post-fail-steps",
        type=int,
        default=None,
        help="Optional override for run.post_fail_steps, useful for shorter diagnostic probes.",
    )
    parser.add_argument("--carla-launch-attempts", type=int, default=DEFAULT_CARLA_LAUNCH_ATTEMPTS)
    parser.add_argument("--carla-world-ready-timeout-sec", type=float, default=DEFAULT_CARLA_WORLD_READY_TIMEOUT_SEC)
    parser.add_argument("--carla-retry-delay-sec", type=float, default=DEFAULT_CARLA_RETRY_DELAY_SEC)
    parser.add_argument(
        "--carla-ignore-memory-preflight",
        action="store_true",
        help="Forward the technical-probe memory preflight bypass to every chain step and optional prewarm.",
    )
    parser.add_argument(
        "--prewarm-carla",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Prewarm CARLA to a verified world_ready session before the first evidence step and reuse it across the chain.",
    )
    parser.add_argument(
        "--prewarm-carla-launch-attempts",
        type=int,
        default=2,
        help="Launch attempts reserved for the non-evidence CARLA prewarm bootstrap.",
    )
    parser.add_argument("--keep-carla-alive-at-end", action="store_true")
    parser.add_argument("--continue-on-failure", action="store_true")
    parser.add_argument(
        "--continue-initial-step-on-failure",
        action="store_true",
        help=(
            "With --continue-on-failure, continue collecting later steps even if the first "
            "route fails. Intended for A/B batches where invalid samples must be classified "
            "instead of aborting the whole route set."
        ),
    )
    parser.add_argument(
        "--progress",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Print live per-step progress lines with ETA while each online step is running.",
    )
    parser.add_argument(
        "--progress-update-sec",
        type=float,
        default=5.0,
        help="How often to print online-chain progress updates while waiting for a step to finish.",
    )
    parser.add_argument(
        "--auto-probe-early-stop",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Stop single-route diagnostic probes early once planning is healthy but control still has no output for a sustained window.",
    )
    parser.add_argument(
        "--early-stop-planning-no-control-sec",
        type=float,
        default=20.0,
        help="Sustained window required before auto-stopping a planning-alive / control-silent probe.",
    )
    parser.add_argument(
        "--early-stop-min-step-elapsed-sec",
        type=float,
        default=15.0,
        help="Minimum per-step runtime before the online chain is allowed to auto-stop a control-silent probe.",
    )
    parser.add_argument(
        "--early-stop-min-planning-nonempty",
        type=int,
        default=80,
        help="Minimum non-empty planning messages before a control-silent probe can auto-stop.",
    )
    parser.add_argument(
        "--early-stop-max-speed-mps",
        type=float,
        default=0.2,
        help="Maximum ego speed allowed when auto-stopping a planning-alive / control-silent probe.",
    )
    parser.add_argument(
        "--enable-lateral",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Forward the existing Town01 lateral-enabled runtime flag to each chain step.",
    )
    parser.add_argument(
        "--enable-guard",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Forward the existing guarded-lateral runtime flag to each chain step.",
    )
    parser.add_argument(
        "--override",
        action="append",
        default=[],
        help="Additional run_town01_route_health.py --override entries forwarded to every chain step.",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser


def _resolved_capability_profiles(raw_profiles: Sequence[str]) -> List[str]:
    profiles = [str(item).strip() for item in raw_profiles if str(item).strip()]
    return profiles or list(DEFAULT_CHAIN_PROFILES)


def _resolved_route_steps(raw_steps: Sequence[str], raw_profiles: Sequence[str]) -> List[Tuple[str, str | None]]:
    steps = [str(item).strip() for item in raw_steps if str(item).strip()]
    if steps:
        resolved: List[Tuple[str, str | None]] = []
        for item in steps:
            capability, sep, route_id = item.partition(":")
            capability = capability.strip()
            route_id = route_id.strip()
            if capability not in CAPABILITY_CHOICES:
                raise ValueError(f"unsupported capability_profile in --step: {capability}")
            if not sep or not route_id:
                raise ValueError(f"invalid --step (expected capability:route_id): {item}")
            resolved.append((capability, route_id))
        return resolved
    return [(capability_profile, None) for capability_profile in _resolved_capability_profiles(raw_profiles)]


def build_online_chain_plan(
    *,
    manifest_path: Path,
    route_steps: Sequence[Tuple[str, str | None]],
    capability_profiles: Sequence[str],
    mode: str,
    config_path: Path | None,
    batch_root_parent: Path,
    comparison_label_suffix: str,
    startup_profile: str,
    ticks_override: int | None,
    post_fail_steps_override: int | None,
    launch_attempts: int,
    world_ready_timeout_sec: float,
    retry_delay_sec: float,
    keep_carla_alive_at_end: bool,
    prewarmed_carla: bool = False,
    enable_lateral: bool | None = None,
    enable_guard: bool | None = None,
    extra_overrides: Sequence[str] | None = None,
    carla_ignore_memory_preflight: bool = False,
) -> List[Dict[str, Any]]:
    plan: List[Dict[str, Any]] = []
    if startup_profile not in CONCRETE_STARTUP_PROFILE_CHOICES:
        raise ValueError(f"unsupported startup_profile: {startup_profile}")
    resolved_steps = list(route_steps) or _resolved_route_steps([], capability_profiles)
    last_index = len(resolved_steps) - 1
    for index, (capability_profile, route_id_override) in enumerate(resolved_steps):
        entry = load_manifest_entry(manifest_path, capability_profile)
        stop_carla_on_exit = not keep_carla_alive_at_end and index == last_index
        # The first step defines the CARLA process shape for the whole chain.
        # Force a fresh start whenever the requested startup profile is one of
        # the curated low-memory/offscreen shapes so we do not silently reuse a
        # stale display-path server and misreport the active startup mode.
        force_fresh_start = False
        if not prewarmed_carla:
            force_fresh_start = (
                startup_profile in {"render_offscreen_no_ros2", "lowres_low_quality", "render_offscreen", "lowres_no_ros"}
                and index == 0
            )
        runtime_defaults = _default_runtime_flags_for_capability(capability_profile)
        resolved_enable_lateral = runtime_defaults["enable_lateral"] if enable_lateral is None else bool(enable_lateral)
        resolved_enable_guard = runtime_defaults["enable_guard"] if enable_guard is None else bool(enable_guard)
        argv = build_online_command(
            entry,
            mode=mode,
            batch_root_parent=batch_root_parent,
            comparison_label_suffix=comparison_label_suffix,
            startup_profile=startup_profile,
            config_path=config_path,
            ticks_override=ticks_override,
            post_fail_steps_override=post_fail_steps_override,
            launch_attempts=launch_attempts,
            world_ready_timeout_sec=world_ready_timeout_sec,
            retry_delay_sec=retry_delay_sec,
            stop_carla_on_exit=stop_carla_on_exit,
            enable_lateral=resolved_enable_lateral,
            enable_guard=resolved_enable_guard,
            force_fresh_start=force_fresh_start,
            route_id_override=route_id_override,
            extra_overrides=list(extra_overrides or []),
            carla_ignore_memory_preflight=bool(carla_ignore_memory_preflight),
        )
        budget = estimate_online_time_budget(
            entry,
            mode=mode,
            startup_profile=startup_profile,
            config_path=config_path,
            ticks_override=ticks_override,
            launch_attempts=launch_attempts,
            world_ready_timeout_sec=world_ready_timeout_sec,
            retry_delay_sec=retry_delay_sec,
            route_id_override=route_id_override,
        )
        plan.append(
            {
                "capability_profile": capability_profile,
                "route_id": route_id_override,
                "source_subset_name": str(entry.get("source_subset_name") or ""),
                "mode": mode,
                "step_index": index + 1,
                "route_count": budget["route_count"],
                "ticks": budget["ticks"],
                "startup_budget_sec": budget["startup_budget_sec"],
                "nominal_batch_runtime_sec": budget["nominal_batch_runtime_sec"],
                "total_budget_sec": budget["total_budget_sec"],
                "enable_lateral": resolved_enable_lateral,
                "enable_guard": resolved_enable_guard,
                "force_fresh_start": force_fresh_start,
                "prewarmed_carla": bool(prewarmed_carla),
                "stop_carla_on_exit": stop_carla_on_exit,
                "command_argv": argv,
                "command": " ".join(shlex.quote(item) for item in argv),
            }
        )
    return plan


def _build_prewarm_route_health_args(
    *,
    startup_profile: str,
    launch_attempts: int,
    world_ready_timeout_sec: float,
    retry_delay_sec: float,
    carla_ignore_memory_preflight: bool = False,
) -> argparse.Namespace:
    args = argparse.Namespace(
        carla_root=Path("/home/ubuntu/CARLA_0.9.16"),
        carla_port=2000,
        carla_launch_attempts=max(int(launch_attempts), 1),
        carla_world_ready_timeout_sec=float(world_ready_timeout_sec),
        carla_retry_delay_sec=float(retry_delay_sec),
        carla_no_retry_failure_families="",
        carla_force_fresh_start=True,
        carla_launcher_auto_recovery=False,
        startup_profile=startup_profile,
        carla_extra_args="",
        carla_disable_native_ros2_arg=False,
        carla_force_headless_env=False,
        carla_display_override="",
        carla_xauthority_override="",
        carla_force_sdl_x11_no_xrandr=False,
        carla_ignore_memory_preflight=bool(carla_ignore_memory_preflight),
    )
    if startup_profile == "lowres_low_quality":
        args.carla_extra_args = "-windowed -ResX=960 -ResY=540 -quality-level=Low"
    elif startup_profile == "render_offscreen_no_ros2":
        args.carla_force_headless_env = True
        args.carla_disable_native_ros2_arg = True
        args.carla_extra_args = "-RenderOffScreen -ResX=960 -ResY=540 -quality-level=Low"
    elif startup_profile == "render_offscreen":
        args.carla_force_headless_env = True
        args.carla_extra_args = "-RenderOffScreen -ResX=960 -ResY=540 -quality-level=Low"
    elif startup_profile == "lowres_no_ros":
        args.carla_disable_native_ros2_arg = True
        args.carla_extra_args = "-windowed -ResX=960 -ResY=540 -quality-level=Low"
    return args


def _prewarm_carla_session(
    *,
    startup_profile: str,
    batch_root_parent: Path,
    launch_attempts: int,
    world_ready_timeout_sec: float,
    retry_delay_sec: float,
    carla_ignore_memory_preflight: bool = False,
):
    prewarm_batch_root = batch_root_parent / "_carla_prewarm"
    prewarm_args = _build_prewarm_route_health_args(
        startup_profile=startup_profile,
        launch_attempts=launch_attempts,
        world_ready_timeout_sec=world_ready_timeout_sec,
        retry_delay_sec=retry_delay_sec,
        carla_ignore_memory_preflight=carla_ignore_memory_preflight,
    )
    print(
        "[online-chain] prewarming CARLA session "
        f"profile={startup_profile} attempts={prewarm_args.carla_launch_attempts}",
        flush=True,
    )
    launcher = _prestart_carla(prewarm_args, prewarm_batch_root)
    print(
        "[online-chain] prewarm ready "
        f"batch_root={prewarm_batch_root}",
        flush=True,
    )
    return launcher


def estimate_online_chain_time_budget(
    plan: Sequence[Dict[str, Any]],
    *,
    launch_attempts: int,
    startup_profile: str = "default",
    world_ready_timeout_sec: float,
    retry_delay_sec: float,
) -> Dict[str, Any]:
    effective_launch_attempts = _effective_launch_attempts(startup_profile, launch_attempts)
    startup_budget_sec = max(int(effective_launch_attempts), 0) * float(world_ready_timeout_sec)
    startup_budget_sec += max(int(effective_launch_attempts) - 1, 0) * float(retry_delay_sec)
    nominal_batch_runtime_sec = sum(float(item.get("nominal_batch_runtime_sec") or 0.0) for item in plan)
    total_budget_sec = startup_budget_sec + nominal_batch_runtime_sec
    total_route_count = sum(int(item.get("route_count") or 0) for item in plan)
    return {
        "capability_count": len(plan),
        "route_count": total_route_count,
        "fixed_delta_seconds_assumed": DEFAULT_ESTIMATE_FIXED_DELTA_SECONDS,
        "startup_budget_sec": round(startup_budget_sec, 1),
        "nominal_batch_runtime_sec": round(nominal_batch_runtime_sec, 1),
        "total_budget_sec": round(total_budget_sec, 1),
        "total_budget_min": round(total_budget_sec / 60.0, 1),
    }


def _load_json_if_exists(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _extract_cli_arg_value(argv: Sequence[str], flag: str) -> str:
    try:
        index = list(argv).index(flag)
    except ValueError:
        return ""
    if index + 1 >= len(argv):
        return ""
    return str(argv[index + 1]).strip()


def _format_duration_brief(total_sec: float | None) -> str:
    if total_sec is None:
        return "--:--"
    total = max(int(round(total_sec)), 0)
    minutes, seconds = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours:d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"


def _render_progress_bar(fraction: float | None, *, width: int = 16) -> str:
    if fraction is None:
        return "[" + ("?" * width) + "]"
    clipped = min(max(float(fraction), 0.0), 1.0)
    filled = int(round(clipped * width))
    filled = min(max(filled, 0), width)
    return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"


def _derive_live_phase(snapshot: Dict[str, Any]) -> str:
    if str(snapshot.get("bridge_runtime_preflight_status") or "").strip() == "bridge_runtime_import_failed":
        return "bridge_import_failed"
    startup_status = str(snapshot.get("carla_startup_status") or "").strip()
    startup_failure_family = str(snapshot.get("carla_startup_failure_family") or "").strip()
    if startup_status and startup_status != "world_ready":
        if startup_failure_family:
            return f"startup_{startup_failure_family}"
        return f"startup_{startup_status}"
    routing_request_count = int(snapshot.get("routing_request_count") or 0)
    planning_nonempty = int(snapshot.get("planning_nonempty_trajectory_count") or 0)
    control_tx_count = int(snapshot.get("control_tx_count") or 0)
    direct_control_apply_count = int(snapshot.get("direct_control_apply_count") or 0)
    if routing_request_count <= 0:
        return "routing_wait"
    if planning_nonempty <= 0:
        return "planning_wait"
    if control_tx_count > 0 or direct_control_apply_count > 0:
        return "control_output"
    if snapshot.get("deferred_control_pending"):
        return "control_pending"
    if snapshot.get("control_running"):
        return "control_running"
    route_health_label = str(snapshot.get("route_health_label") or "").strip()
    if route_health_label:
        return route_health_label
    summary_status = str(snapshot.get("summary_status") or "").strip()
    if summary_status:
        return summary_status
    return str(snapshot.get("manifest_status") or "starting")


_FOLLOWSTOP_PROGRESS_RE = re.compile(
    r"^\[progress\]\s+(?P<current>\d+)\s*/\s*(?P<total>\d+)\s+\((?P<pct>\d+)%\),\s+est remaining\s+(?P<remaining>[0-9.]+)s$"
)


def _extract_followstop_progress(batch_root_path: Path) -> Dict[str, Any]:
    candidates = sorted(batch_root_path.glob("*/artifacts/followstop_child.stdout.log"))
    if not candidates:
        return {}
    log_path = candidates[-1]
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return {}
    for line in reversed(lines):
        match = _FOLLOWSTOP_PROGRESS_RE.match(line.strip())
        if not match:
            continue
        current = int(match.group("current"))
        total = max(int(match.group("total")), 1)
        remaining_sec = float(match.group("remaining"))
        return {
            "scenario_progress_current": current,
            "scenario_progress_total": total,
            "scenario_progress_fraction": min(max(current / total, 0.0), 1.0),
            "scenario_progress_remaining_sec": max(remaining_sec, 0.0),
        }
    return {}


def _collect_step_live_snapshot(item: Dict[str, Any]) -> Dict[str, Any]:
    batch_root = _extract_cli_arg_value(item.get("command_argv") or [], "--batch-root")
    if not batch_root:
        return {}
    batch_root_path = Path(batch_root).expanduser().resolve()
    manifest = _load_json_if_exists(batch_root_path / "artifacts" / "town01_route_health_run_manifest.json")
    run_items = list(manifest.get("runs") or [])
    current_item = run_items[-1] if run_items else {}
    latest_path = batch_root_path / "LATEST.txt"
    effective_run_dir_raw = latest_path.read_text(encoding="utf-8").strip() if latest_path.exists() else ""
    effective_run_dir = Path(effective_run_dir_raw).expanduser().resolve() if effective_run_dir_raw else None

    snapshot: Dict[str, Any] = {
        "batch_root": str(batch_root_path),
        "manifest_status": str(current_item.get("status") or ""),
        "effective_run_dir": str(effective_run_dir) if effective_run_dir is not None else "",
    }
    snapshot.update(_extract_followstop_progress(batch_root_path))
    startup_probe = _load_json_if_exists(batch_root_path / "carla_boot" / "carla_startup_probe.json")
    attempts = startup_probe.get("attempts") if isinstance(startup_probe.get("attempts"), list) else []
    final_attempt = attempts[-1] if attempts and isinstance(attempts[-1], dict) else {}
    snapshot.update(
        {
            "carla_startup_status": str(
                startup_probe.get("status")
                or final_attempt.get("status")
                or ""
            ),
            "carla_startup_rpc_ready": bool(
                startup_probe.get("rpc_ready", final_attempt.get("rpc_ready", False))
            ),
            "carla_startup_world_ready": bool(
                startup_probe.get("world_ready", final_attempt.get("world_ready", False))
            ),
            "carla_startup_failure_family": str(
                startup_probe.get("failure_family")
                or final_attempt.get("failure_family")
                or ""
            ),
        }
    )
    if effective_run_dir is None or not effective_run_dir.exists():
        snapshot["phase"] = _derive_live_phase(snapshot)
        return snapshot

    summary = _load_json_if_exists(effective_run_dir / "summary.provisional.json")
    if not summary:
        summary = _load_json_if_exists(effective_run_dir / "summary.json")
    bridge_health = _load_json_if_exists(effective_run_dir / "artifacts" / "bridge_health_summary.json")
    if not bridge_health:
        bridge_health = _load_json_if_exists(effective_run_dir / "artifacts" / "bridge_health_summary.finalized.json")
    bridge_stats = _load_json_if_exists(effective_run_dir / "artifacts" / "cyber_bridge_stats.json")
    bridge_healthcheck = _load_json_if_exists(effective_run_dir / "artifacts" / "cyber_bridge_healthcheck.json")
    bridge_preflight = _load_json_if_exists(effective_run_dir / "artifacts" / "bridge_runtime_preflight.json")
    direct_bridge_stats = _load_json_if_exists(effective_run_dir / "artifacts" / "direct_bridge_stats.json")

    planning_stats = bridge_stats.get("planning") if isinstance(bridge_stats.get("planning"), dict) else {}
    last_measured_control = (
        bridge_stats.get("last_measured_control")
        if isinstance(bridge_stats.get("last_measured_control"), dict)
        else {}
    )
    command_materialization = (
        bridge_health.get("command_materialization")
        if isinstance(bridge_health.get("command_materialization"), dict)
        else {}
    )

    snapshot.update(
        {
            "summary_status": str(summary.get("summary_status") or ""),
            "route_health_label": str(summary.get("route_health_label") or ""),
            "bridge_runtime_preflight_status": str(
                bridge_preflight.get("bridge_runtime_preflight_status")
                or bridge_preflight.get("status")
                or ""
            ),
            "routing_request_count": int(
                bridge_health.get("routing_request_count")
                or bridge_stats.get("routing_request_count")
                or 0
            ),
            "planning_nonempty_trajectory_count": int(
                bridge_health.get("planning_nonempty_trajectory_count")
                or planning_stats.get("nonempty_trajectory_count")
                or 0
            ),
            "planning_message_count": int(
                bridge_health.get("planning_message_count")
                or planning_stats.get("msg_count")
                or 0
            ),
            "control_tx_count": int(bridge_stats.get("control_tx_count") or 0),
            "control_rx_count": int(bridge_stats.get("control_rx_count") or 0),
            "direct_control_apply_count": int(direct_bridge_stats.get("control_apply_count") or 0),
            "direct_control_apply_fail_count": int(direct_bridge_stats.get("control_apply_fail_count") or 0),
            "control_running": bool(bridge_healthcheck.get("control_running")),
            "deferred_control_pending": bool(bridge_healthcheck.get("deferred_control_pending")),
            "deferred_control_started": bool(bridge_healthcheck.get("deferred_control_started")),
            "speed_mps": last_measured_control.get("speed_mps") or direct_bridge_stats.get("last_speed_mps"),
            "distance_to_destination": bridge_stats.get("distance_to_destination")
            or planning_stats.get("last_distance_to_destination"),
            "command_materialization_stage": str(command_materialization.get("command_path_stage") or ""),
            "command_materialization_layer": str(command_materialization.get("first_divergence_layer") or ""),
            "command_materialization_reason": str(command_materialization.get("first_divergence_reason") or ""),
        }
    )
    snapshot["phase"] = _derive_live_phase(snapshot)
    return snapshot


def _batch_root_for_item(item: Dict[str, Any]) -> Path | None:
    batch_root = _extract_cli_arg_value(item.get("command_argv") or [], "--batch-root")
    if not batch_root:
        return None
    return Path(batch_root).expanduser().resolve()


def _probe_early_stop_condition(
    *,
    item: Dict[str, Any],
    snapshot: Dict[str, Any],
    step_elapsed_sec: float,
    min_step_elapsed_sec: float,
    min_planning_nonempty: int,
    max_speed_mps: float,
) -> bool:
    if int(item.get("route_count") or 0) != 1:
        return False
    if step_elapsed_sec < max(float(min_step_elapsed_sec), 0.0):
        return False
    if str(snapshot.get("bridge_runtime_preflight_status") or "").strip() != "bridge_runtime_ready":
        return False
    if int(snapshot.get("routing_request_count") or 0) < 1:
        return False
    if int(snapshot.get("planning_nonempty_trajectory_count") or 0) < max(int(min_planning_nonempty), 1):
        return False
    if int(snapshot.get("control_tx_count") or 0) > 0:
        return False
    speed = snapshot.get("speed_mps")
    if isinstance(speed, (int, float)) and float(speed) > float(max_speed_mps):
        return False
    phase = str(snapshot.get("phase") or "").strip()
    if phase not in {"planning_wait", "control_pending", "control_running"}:
        return False
    return True


def _persist_chain_early_stop(batch_root: Path, payload: Dict[str, Any]) -> None:
    artifacts_dir = batch_root / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    (artifacts_dir / "online_chain_early_stop.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _graceful_stop_child(child: subprocess.Popen[Any], *, interrupt_timeout_sec: float = 15.0) -> int:
    if child.poll() is not None:
        return int(child.returncode or 0)
    try:
        child.send_signal(signal.SIGINT)
    except Exception:
        try:
            child.terminate()
        except Exception:
            pass
    try:
        return int(child.wait(timeout=max(float(interrupt_timeout_sec), 1.0)))
    except subprocess.TimeoutExpired:
        try:
            child.terminate()
        except Exception:
            pass
    try:
        return int(child.wait(timeout=10.0))
    except subprocess.TimeoutExpired:
        try:
            child.kill()
        except Exception:
            pass
    return int(child.wait(timeout=10.0))


def _analyze_batch_root(batch_root: Path) -> int:
    analyze_cmd = [
        sys.executable,
        str(REPO_ROOT / "tools" / "run_town01_route_health.py"),
        "analyze",
        "--batch-root",
        str(batch_root),
    ]
    return subprocess.run(analyze_cmd, cwd=str(REPO_ROOT), check=False).returncode


def _render_progress_line(
    *,
    item: Dict[str, Any],
    step_elapsed_sec: float,
    chain_elapsed_sec: float,
    step_budget_sec: float,
    chain_budget_sec: float,
    snapshot: Dict[str, Any],
) -> str:
    step_fraction = None if step_budget_sec <= 0 else min(step_elapsed_sec / step_budget_sec, 1.0)
    chain_fraction = None if chain_budget_sec <= 0 else min(chain_elapsed_sec / chain_budget_sec, 1.0)
    parts = [
        "[online-chain][progress]",
        f"step {item['step_index']}/{item.get('chain_step_count') or item['step_index']}",
        f"step={_render_progress_bar(step_fraction)}",
        f"eta_step={_format_duration_brief(max(step_budget_sec - step_elapsed_sec, 0.0) if step_budget_sec > 0 else None)}",
        f"overall={_render_progress_bar(chain_fraction)}",
        f"eta_chain={_format_duration_brief(max(chain_budget_sec - chain_elapsed_sec, 0.0) if chain_budget_sec > 0 else None)}",
        f"phase={snapshot.get('phase') or 'starting'}",
        f"status={snapshot.get('manifest_status') or 'starting'}",
    ]
    if snapshot.get("route_health_label"):
        parts.append(f"route={snapshot['route_health_label']}")
    if snapshot.get("summary_status"):
        parts.append(f"summary={snapshot['summary_status']}")
    scenario_current = snapshot.get("scenario_progress_current")
    scenario_total = snapshot.get("scenario_progress_total")
    if isinstance(scenario_current, int) and isinstance(scenario_total, int) and scenario_total > 0:
        parts.append(f"ticks={scenario_current}/{scenario_total}")
        parts.append(f"scene={_render_progress_bar(snapshot.get('scenario_progress_fraction'))}")
    scenario_remaining_sec = snapshot.get("scenario_progress_remaining_sec")
    if isinstance(scenario_remaining_sec, (int, float)):
        parts.append(f"eta_scene={_format_duration_brief(float(scenario_remaining_sec))}")
    if snapshot.get("routing_request_count") is not None:
        parts.append(f"routing={int(snapshot.get('routing_request_count') or 0)}")
    if snapshot.get("planning_nonempty_trajectory_count") is not None:
        parts.append(f"planning={int(snapshot.get('planning_nonempty_trajectory_count') or 0)}")
    if snapshot.get("control_tx_count") is not None:
        parts.append(f"control_tx={int(snapshot.get('control_tx_count') or 0)}")
    if int(snapshot.get("control_tx_count") or 0) > 0 or int(snapshot.get("direct_control_apply_count") or 0) > 0:
        parts.append("control=output")
    elif snapshot.get("control_running"):
        parts.append("control=running")
    elif snapshot.get("deferred_control_pending"):
        parts.append("control=pending")
    if snapshot.get("direct_control_apply_count") is not None and int(snapshot.get("direct_control_apply_count") or 0) > 0:
        parts.append(f"direct_apply={int(snapshot.get('direct_control_apply_count') or 0)}")
    speed = snapshot.get("speed_mps")
    if isinstance(speed, (int, float)):
        parts.append(f"speed={float(speed):.1f}mps")
    distance = snapshot.get("distance_to_destination")
    if isinstance(distance, (int, float)):
        parts.append(f"goal_dist={float(distance):.1f}m")
    return " ".join(parts)


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    startup_profiles = resolve_startup_profile_sequence(args.startup_profile)
    route_steps = _resolved_route_steps(args.step, args.capability_profiles)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_root_parent = args.batch_root_parent or (
        REPO_ROOT / "runs" / f"town01_capability_online_chain_{timestamp}"
    )
    first_attempt_ctx = startup_attempt_context(
        batch_root_parent,
        args.comparison_label_suffix,
        startup_profiles[0],
        len(startup_profiles),
    )
    plan = build_online_chain_plan(
        manifest_path=args.manifest,
        route_steps=route_steps,
        capability_profiles=args.capability_profiles,
        mode=args.mode,
        config_path=args.config,
        batch_root_parent=Path(str(first_attempt_ctx["batch_root_parent"])),
        comparison_label_suffix=str(first_attempt_ctx["comparison_label_suffix"]),
        startup_profile=startup_profiles[0],
        ticks_override=args.ticks,
        post_fail_steps_override=args.post_fail_steps,
        launch_attempts=args.carla_launch_attempts,
        world_ready_timeout_sec=args.carla_world_ready_timeout_sec,
        retry_delay_sec=args.carla_retry_delay_sec,
        keep_carla_alive_at_end=args.keep_carla_alive_at_end,
        prewarmed_carla=bool(args.prewarm_carla),
        extra_overrides=list(args.override or []),
        carla_ignore_memory_preflight=bool(args.carla_ignore_memory_preflight),
    )
    budget = estimate_online_chain_time_budget(
        plan,
        launch_attempts=args.carla_launch_attempts,
        startup_profile=startup_profiles[-1],
        world_ready_timeout_sec=args.carla_world_ready_timeout_sec,
        retry_delay_sec=args.carla_retry_delay_sec,
    )

    print("# capability_profiles:", ",".join(item["capability_profile"] for item in plan))
    print("# startup_profile:", args.startup_profile)
    print("# startup_profile_role:", startup_profile_role(args.startup_profile))
    print("# startup_profile_note:", startup_profile_note(args.startup_profile))
    print("# startup_profile_sequence:", ",".join(startup_profiles))
    print("# startup_profile_attempt_count:", len(startup_profiles))
    print("# chain_step_count:", len(route_steps))
    print("# mode:", args.mode)
    print("# chain_capability_count:", budget["capability_count"])
    print("# chain_route_count:", budget["route_count"])
    print("# timing_budget_assumption:", f"fixed_delta_seconds={budget['fixed_delta_seconds_assumed']}")
    print("# timing_budget_startup_upper_bound_sec:", budget["startup_budget_sec"])
    print("# timing_budget_nominal_batch_runtime_sec:", budget["nominal_batch_runtime_sec"])
    print("# timing_budget_total_upper_bound_sec:", budget["total_budget_sec"])
    print("# timing_budget_total_upper_bound_min:", budget["total_budget_min"])
    chain_defaults = [
        _default_runtime_flags_for_capability(capability_profile)
        for capability_profile, _ in route_steps
    ]
    default_lateral_any = any(item["enable_lateral"] for item in chain_defaults)
    default_guard_any = any(item["enable_guard"] for item in chain_defaults)
    resolved_enable_lateral = default_lateral_any if args.enable_lateral is None else bool(args.enable_lateral)
    resolved_enable_guard = default_guard_any if args.enable_guard is None else bool(args.enable_guard)
    print("# enable_lateral:", str(bool(resolved_enable_lateral)).lower())
    print("# enable_guard:", str(bool(resolved_enable_guard)).lower())
    print("# prewarm_carla:", str(bool(args.prewarm_carla)).lower())
    print("# prewarm_carla_launch_attempts:", max(int(args.prewarm_carla_launch_attempts), 1))

    last_exit_code = 0
    chain_started_at = time.monotonic()
    try:
        for startup_attempt_index, startup_profile in enumerate(startup_profiles, start=1):
            attempt_ctx = startup_attempt_context(
                batch_root_parent,
                args.comparison_label_suffix,
                startup_profile,
                len(startup_profiles),
            )
            prewarm_launcher = None
            plan = build_online_chain_plan(
                manifest_path=args.manifest,
                route_steps=route_steps,
                capability_profiles=args.capability_profiles,
                mode=args.mode,
                config_path=args.config,
                batch_root_parent=Path(str(attempt_ctx["batch_root_parent"])),
                comparison_label_suffix=str(attempt_ctx["comparison_label_suffix"]),
                startup_profile=startup_profile,
                ticks_override=args.ticks,
                post_fail_steps_override=args.post_fail_steps,
                launch_attempts=args.carla_launch_attempts,
                world_ready_timeout_sec=args.carla_world_ready_timeout_sec,
                retry_delay_sec=args.carla_retry_delay_sec,
                keep_carla_alive_at_end=args.keep_carla_alive_at_end,
                prewarmed_carla=bool(args.prewarm_carla),
                enable_lateral=resolved_enable_lateral,
                enable_guard=resolved_enable_guard,
                extra_overrides=list(args.override or []),
                carla_ignore_memory_preflight=bool(args.carla_ignore_memory_preflight),
            )
            print(
                "# startup_attempt:",
                f"{startup_attempt_index}/{len(startup_profiles)}",
                f"profile={startup_profile}",
                f"role={startup_profile_role(startup_profile)}",
            )
            for item in plan:
                print(
                    "# step:",
                    f"{item['step_index']}/{len(plan)}",
                    f"profile={item['capability_profile']}",
                    f"route_id={item.get('route_id') or '<manifest>'}",
                    f"routes={item['route_count']}",
                    f"enable_lateral={str(bool(item['enable_lateral'])).lower()}",
                    f"enable_guard={str(bool(item['enable_guard'])).lower()}",
                    f"force_fresh_start={str(item['force_fresh_start']).lower()}",
                    f"stop_carla_on_exit={str(item['stop_carla_on_exit']).lower()}",
                )
                print(item["command"])
            print()
            if args.dry_run:
                continue

            exit_code = 0
            failed_step_index: int | None = None
            try:
                if args.prewarm_carla:
                    try:
                        prewarm_launcher = _prewarm_carla_session(
                            startup_profile=startup_profile,
                            batch_root_parent=Path(str(attempt_ctx["batch_root_parent"])),
                            launch_attempts=max(int(args.prewarm_carla_launch_attempts), 1),
                            world_ready_timeout_sec=args.carla_world_ready_timeout_sec,
                            retry_delay_sec=args.carla_retry_delay_sec,
                            carla_ignore_memory_preflight=bool(args.carla_ignore_memory_preflight),
                        )
                    except SystemExit as exc:
                        exit_code = int(exc.code) if isinstance(exc.code, int) else 1
                        failed_step_index = 1
                        print(
                            "[online-chain][WARN] CARLA prewarm failed "
                            f"startup_profile={startup_profile} returncode={exit_code}",
                            flush=True,
                        )
                    except Exception as exc:
                        exit_code = 1
                        failed_step_index = 1
                        print(
                            "[online-chain][WARN] CARLA prewarm raised unexpectedly: "
                            f"{exc!r}",
                            flush=True,
                        )

                if exit_code == 0:
                    for item in plan:
                        item["chain_step_count"] = len(plan)
                        print(
                            f"[online-chain] startup attempt {startup_attempt_index}/{len(startup_profiles)} "
                            f"step {item['step_index']}/{len(plan)} "
                            f"profile={item['capability_profile']} routes={item['route_count']}",
                            flush=True,
                        )
                        step_started_at = time.monotonic()
                        step_budget_sec = float(item.get("nominal_batch_runtime_sec") or 0.0)
                        if int(item.get("step_index") or 0) == 1 and not bool(item.get("prewarmed_carla")):
                            step_budget_sec += float(item.get("startup_budget_sec") or 0.0)
                        child = subprocess.Popen(item["command_argv"], cwd=str(REPO_ROOT))
                        last_progress_print_at = 0.0
                        no_control_condition_started_at: float | None = None
                        early_stop_applied = False
                        effective_returncode: int | None = None
                        while True:
                            returncode = child.poll()
                            now = time.monotonic()
                            snapshot: Dict[str, Any] = {}
                            if (
                                args.progress
                                and returncode is None
                                and now - last_progress_print_at >= max(float(args.progress_update_sec), 1.0)
                            ):
                                snapshot = _collect_step_live_snapshot(item)
                                print(
                                    _render_progress_line(
                                        item=item,
                                        step_elapsed_sec=now - step_started_at,
                                        chain_elapsed_sec=now - chain_started_at,
                                        step_budget_sec=step_budget_sec,
                                        chain_budget_sec=float(budget.get("total_budget_sec") or 0.0),
                                        snapshot=snapshot,
                                    ),
                                    flush=True,
                                )
                                last_progress_print_at = now
                            if returncode is None and not snapshot:
                                snapshot = _collect_step_live_snapshot(item)
                            if args.auto_probe_early_stop and returncode is None:
                                condition_active = _probe_early_stop_condition(
                                    item=item,
                                    snapshot=snapshot,
                                    step_elapsed_sec=now - step_started_at,
                                    min_step_elapsed_sec=float(args.early_stop_min_step_elapsed_sec),
                                    min_planning_nonempty=int(args.early_stop_min_planning_nonempty),
                                    max_speed_mps=float(args.early_stop_max_speed_mps),
                                )
                                if condition_active:
                                    if no_control_condition_started_at is None:
                                        no_control_condition_started_at = now
                                    sustained_for_sec = now - no_control_condition_started_at
                                    if sustained_for_sec >= max(float(args.early_stop_planning_no_control_sec), 1.0):
                                        batch_root = _batch_root_for_item(item)
                                        payload = {
                                            "reason": "planning_alive_control_no_output",
                                            "triggered_at": datetime.now().isoformat(timespec="seconds"),
                                            "step_elapsed_sec": round(now - step_started_at, 3),
                                            "sustained_for_sec": round(sustained_for_sec, 3),
                                            "snapshot": snapshot,
                                        }
                                        if batch_root is not None:
                                            _persist_chain_early_stop(batch_root, payload)
                                        print(
                                            "[online-chain][INFO] auto early-stop triggered: "
                                            "planning is healthy but control still has no output; "
                                            "stopping this single-route probe and finalizing artifacts",
                                            flush=True,
                                        )
                                        _graceful_stop_child(child)
                                        if batch_root is not None:
                                            analyze_rc = _analyze_batch_root(batch_root)
                                            effective_returncode = 0 if analyze_rc == 0 else int(analyze_rc)
                                        else:
                                            effective_returncode = 0
                                        early_stop_applied = True
                                        snapshot = _collect_step_live_snapshot(item)
                                        if args.progress:
                                            print(
                                                _render_progress_line(
                                                    item=item,
                                                    step_elapsed_sec=now - step_started_at,
                                                    chain_elapsed_sec=now - chain_started_at,
                                                    step_budget_sec=step_budget_sec,
                                                    chain_budget_sec=float(budget.get("total_budget_sec") or 0.0),
                                                    snapshot=snapshot,
                                                ),
                                                flush=True,
                                            )
                                        completed = child
                                        break
                                else:
                                    no_control_condition_started_at = None
                            if returncode is not None:
                                if args.progress:
                                    snapshot = _collect_step_live_snapshot(item)
                                    print(
                                        _render_progress_line(
                                            item=item,
                                            step_elapsed_sec=now - step_started_at,
                                            chain_elapsed_sec=now - chain_started_at,
                                            step_budget_sec=step_budget_sec,
                                            chain_budget_sec=float(budget.get("total_budget_sec") or 0.0),
                                            snapshot=snapshot,
                                        ),
                                        flush=True,
                                    )
                                completed = child
                                effective_returncode = int(completed.returncode)
                                break
                            time.sleep(1.0)
                        step_returncode = int(effective_returncode if effective_returncode is not None else completed.returncode)
                        if early_stop_applied:
                            print(
                                "[online-chain][INFO] probe finalized after early-stop "
                                f"profile={item['capability_profile']} route_id={item.get('route_id') or '<manifest>'}",
                                flush=True,
                            )
                        if step_returncode != 0:
                            exit_code = step_returncode
                            failed_step_index = int(item["step_index"])
                            print(
                                f"[online-chain][WARN] step failed profile={item['capability_profile']} "
                                f"startup_profile={startup_profile} returncode={step_returncode}",
                                flush=True,
                            )
                            initial_step_may_continue = (
                                failed_step_index == 1
                                and bool(args.continue_on_failure)
                                and bool(args.continue_initial_step_on_failure)
                            )
                            if not initial_step_may_continue and (
                                failed_step_index == 1 or not args.continue_on_failure
                            ):
                                break
            finally:
                if prewarm_launcher is not None and not args.keep_carla_alive_at_end:
                    try:
                        prewarm_launcher.stop()
                    except Exception:
                        pass
            last_exit_code = exit_code
            if exit_code == 0:
                return 0
            if (
                failed_step_index == 1
                and not bool(args.continue_initial_step_on_failure)
                and startup_attempt_index < len(startup_profiles)
            ):
                print(
                    "[online-chain][WARN] initial startup attempt failed before chain reuse became useful; "
                    "retrying the chain with the next startup profile",
                    flush=True,
                )
                continue
            return exit_code
        if args.dry_run:
            return 0
        return last_exit_code
    finally:
        if not args.dry_run and not args.keep_carla_alive_at_end:
            _cleanup_carla_processes()


if __name__ == "__main__":
    raise SystemExit(main())
