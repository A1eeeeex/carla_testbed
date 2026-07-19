#!/usr/bin/env python3
# LEGACY / OPERATIONAL HELPER: retained for historical Town01 online evidence packaging.
# Do not add new platform logic here; move reusable code into carla_testbed.record or analysis modules.
# Migration target: carla_testbed.record artifact store and carla_testbed.analysis.
from __future__ import annotations

import argparse
from datetime import datetime
from functools import lru_cache
import json
from pathlib import Path
import shlex
import subprocess
import sys
from typing import Any, Dict, List


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.config.rig_loader import load_rig_file

RUNNER = REPO_ROOT / "tools" / "run_town01_route_health.py"
DEFAULT_MANIFEST = REPO_ROOT / "artifacts" / "town01_capability_missing_history_manifest_20260327.json"
CONDA_RUN_PREFIX = [
    "/home/ubuntu/miniconda3/bin/conda",
    "run",
    "-n",
    "carla16",
    "python",
]
CAPABILITY_CHOICES = (
    "lane_keep",
    "curve_lane_follow",
    "junction_traverse",
    "traffic_light_actual",
)
CONCRETE_STARTUP_PROFILE_CHOICES = (
    "default",
    "render_offscreen_no_ros2",
    "lowres_low_quality",
    "render_offscreen",
    "lowres_no_ros",
)
STARTUP_PROFILE_CHOICES = CONCRETE_STARTUP_PROFILE_CHOICES + ("adaptive",)
DEFAULT_STARTUP_PROFILE = "render_offscreen_no_ros2"
DEFAULT_CARLA_LAUNCH_ATTEMPTS = 1
DEFAULT_CARLA_WORLD_READY_TIMEOUT_SEC = 180.0
DEFAULT_CARLA_RETRY_DELAY_SEC = 2.0
DEFAULT_ESTIMATE_FIXED_DELTA_SECONDS = 0.05


@lru_cache(maxsize=32)
def _config_run_defaults(config_path_raw: str) -> Dict[str, Any]:
    config_path_str = str(config_path_raw or "").strip()
    if not config_path_str:
        return {}
    try:
        cfg = load_rig_file(config_path_str)
    except Exception:
        return {}
    run_cfg = cfg.get("run") if isinstance(cfg, dict) else {}
    if not isinstance(run_cfg, dict):
        return {}
    defaults: Dict[str, Any] = {}
    ticks = run_cfg.get("ticks")
    post_fail_steps = run_cfg.get("post_fail_steps")
    try:
        if ticks is not None:
            defaults["ticks"] = int(ticks)
    except Exception:
        pass
    try:
        if post_fail_steps is not None:
            defaults["post_fail_steps"] = int(post_fail_steps)
    except Exception:
        pass
    return defaults


_CONFIG_PRECEDENCE_SPEED_KEYS = (
    "algo.apollo.routing.target_speed_mps",
    "algo.apollo.planning.default_cruise_speed_mps",
)


@lru_cache(maxsize=32)
def _explicit_config_speed_overrides(config_path_raw: str) -> List[str]:
    """Keep explicit profile speeds ahead of historical capability presets.

    Frozen review-pack override files may contain an older target speed.  The
    selected ``--config`` is the experiment profile, so its speed values must
    win; explicit CLI ``--override`` entries are appended afterwards and keep
    the highest precedence.
    """
    config_path = str(config_path_raw or "").strip()
    if not config_path:
        return []
    try:
        cfg = load_rig_file(config_path)
    except Exception:
        return []
    overrides: List[str] = []
    for dotted_key in _CONFIG_PRECEDENCE_SPEED_KEYS:
        value: Any = cfg
        for key in dotted_key.split("."):
            if not isinstance(value, dict) or key not in value:
                value = None
                break
            value = value[key]
        if isinstance(value, bool) or value is None or isinstance(value, (dict, list, tuple)):
            continue
        overrides.append(f"{dotted_key}={value}")
    return overrides


def _default_runtime_flags_for_capability(capability_profile: str) -> Dict[str, bool]:
    normalized = str(capability_profile or "").strip()
    if normalized in {"lane_keep", "curve_lane_follow", "junction_traverse", "traffic_light_actual"}:
        return {"enable_lateral": True, "enable_guard": True}
    return {"enable_lateral": False, "enable_guard": True}


def _effective_launch_attempts(startup_profile: str, launch_attempts: int) -> int:
    return max(int(launch_attempts), 1)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run frozen Town01 capability online packs.")
    parser.add_argument("capability_profile", choices=CAPABILITY_CHOICES)
    parser.add_argument("--mode", choices=("seed", "full"), default="seed")
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Optional explicit Town01 route-health config path forwarded to the route-health runner.",
    )
    parser.add_argument(
        "--route-id",
        default="",
        help="Optional single-route override. Reuses the capability overrides but runs only this route.",
    )
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument(
        "--startup-profile",
        choices=STARTUP_PROFILE_CHOICES,
        default=DEFAULT_STARTUP_PROFILE,
    )
    parser.add_argument("--batch-root-parent", type=Path, default=None)
    parser.add_argument("--comparison-label-suffix", default="manual_online")
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
    parser.add_argument("--no-stop-carla-on-exit", action="store_true")
    parser.add_argument(
        "--carla-ignore-memory-preflight",
        action="store_true",
        help="Forward the technical-probe memory preflight bypass to run_town01_route_health.py.",
    )
    parser.add_argument(
        "--enable-lateral",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Forward the existing Town01 lateral-enabled runtime flag to the route-health runner.",
    )
    parser.add_argument(
        "--enable-guard",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Forward the existing guarded-lateral runtime flag to the route-health runner.",
    )
    parser.add_argument(
        "--override",
        action="append",
        default=[],
        help="Additional run_town01_route_health.py --override entries to forward verbatim.",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser


def load_manifest_entry(manifest_path: Path, capability_profile: str) -> Dict[str, Any]:
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    for entry in payload.get("entries", []):
        if str(entry.get("capability_profile") or "").strip() == capability_profile:
            return entry
    raise KeyError(f"capability_profile not found in manifest: {capability_profile}")


def resolve_startup_profile_sequence(startup_profile: str) -> List[str]:
    if startup_profile == "adaptive":
        return ["render_offscreen_no_ros2", "render_offscreen", "lowres_no_ros"]
    if startup_profile in CONCRETE_STARTUP_PROFILE_CHOICES:
        return [startup_profile]
    raise ValueError(f"unsupported startup_profile: {startup_profile}")


def startup_profile_role(startup_profile: str) -> str:
    if startup_profile == "lowres_no_ros":
        return "diagnostic"
    if startup_profile == "render_offscreen_no_ros2":
        return "mainline_followstop_aligned"
    if startup_profile == "render_offscreen":
        return "rpc_differential_check"
    if startup_profile == "lowres_low_quality":
        return "display_fallback"
    if startup_profile == "adaptive":
        return "mainline_offscreen_then_fallback"
    return "mainline"


def startup_profile_note(startup_profile: str) -> str:
    if startup_profile == "lowres_no_ros":
        return "diagnostic low-res/no-ROS2 isolator; do not treat as a reusable world-ready fix on this host"
    if startup_profile == "render_offscreen_no_ros2":
        return "followstop-aligned mainline: RenderOffScreen without native CARLA ROS2 arg, explicitly capped at 960x540 Low to reduce memory pressure on this host"
    if startup_profile == "render_offscreen":
        return "RenderOffScreen with native CARLA ROS2 arg kept only as an A/B differential check against the low-res followstop-aligned default"
    if startup_profile == "lowres_low_quality":
        return "display fallback in case offscreen bring-up still stalls; keeps the 960x540 low-quality path for comparison"
    if startup_profile == "adaptive":
        return "run RenderOffScreen first, then low-res display fallback, then the no-ROS2 diagnostic isolator only if needed"
    return "baseline/mainline display path"


def startup_attempt_context(
    batch_root_parent: Path,
    comparison_label_suffix: str,
    startup_profile: str,
    attempt_count: int,
) -> Dict[str, Path | str]:
    if attempt_count <= 1:
        return {
            "batch_root_parent": batch_root_parent,
            "comparison_label_suffix": comparison_label_suffix,
        }
    profile_tag = f"startup_{startup_profile}"
    return {
        "batch_root_parent": batch_root_parent / profile_tag,
        "comparison_label_suffix": f"{comparison_label_suffix}__{profile_tag}",
    }


def build_online_command(
    entry: Dict[str, Any],
    *,
    mode: str,
    batch_root_parent: Path,
    comparison_label_suffix: str,
    startup_profile: str,
    config_path: Path | None = None,
    ticks_override: int | None,
    post_fail_steps_override: int | None = None,
    launch_attempts: int,
    world_ready_timeout_sec: float,
    retry_delay_sec: float,
    stop_carla_on_exit: bool,
    enable_lateral: bool = False,
    enable_guard: bool = False,
    force_fresh_start: bool | None = None,
    route_id_override: str | None = None,
    extra_overrides: List[str] | None = None,
    carla_ignore_memory_preflight: bool = False,
) -> List[str]:
    if mode not in {"seed", "full"}:
        raise ValueError(f"unsupported mode: {mode}")
    if startup_profile not in CONCRETE_STARTUP_PROFILE_CHOICES:
        raise ValueError(f"unsupported startup_profile: {startup_profile}")
    if force_fresh_start is None:
        force_fresh_start = startup_profile in {
            "render_offscreen_no_ros2",
            "lowres_low_quality",
            "render_offscreen",
            "lowres_no_ros",
        }
    effective_launch_attempts = _effective_launch_attempts(startup_profile, launch_attempts)

    capability_profile = str(entry["capability_profile"])
    normalized_route_id_override = str(route_id_override or "").strip()
    if normalized_route_id_override:
        route_ids_file = None
        route_tag = normalized_route_id_override.replace("/", "_")
        comparison_label = (
            f"{capability_profile}__adhoc__{route_tag}__{mode}__{comparison_label_suffix}"
        )
        batch_root = batch_root_parent / f"{capability_profile}__adhoc__{route_tag}__{mode}"
    elif mode == "seed":
        route_ids_file = Path(str(entry["seed_route_ids_file"]))
        comparison_label = f"{entry['seed_comparison_label']}__{comparison_label_suffix}"
        batch_root = batch_root_parent / f"{capability_profile}__seed"
    else:
        route_ids_file = Path(str(entry["route_ids_file"]))
        comparison_label = f"{entry['comparison_label']}__{comparison_label_suffix}"
        batch_root = batch_root_parent / f"{capability_profile}__full"
    overrides_file = Path(str(entry["overrides_file"]))
    config_defaults = _config_run_defaults(
        str(Path(config_path).expanduser().resolve()) if config_path is not None else ""
    )
    ticks = int(
        ticks_override
        if ticks_override is not None
        else config_defaults.get("ticks")
        if config_defaults.get("ticks") is not None
        else entry.get("ticks")
        or 700
    )

    argv = [
        *CONDA_RUN_PREFIX,
        str(RUNNER),
        "run",
        "--capability-profile",
        capability_profile,
        "--carla-launch-attempts",
        str(effective_launch_attempts),
        "--carla-world-ready-timeout-sec",
        str(world_ready_timeout_sec),
        "--carla-retry-delay-sec",
        str(float(retry_delay_sec)),
    ]
    if config_path is not None:
        argv.extend(["--config", str(Path(config_path).expanduser().resolve())])
    argv.extend(
        [
            "--overrides-file",
            str(overrides_file),
            "--ticks",
            str(ticks),
            "--batch-root",
            str(batch_root),
            "--comparison-label",
            comparison_label,
        ]
    )
    for override in _explicit_config_speed_overrides(
        str(Path(config_path).expanduser().resolve()) if config_path is not None else ""
    ):
        argv.extend(["--override", override])
    if post_fail_steps_override is not None:
        argv.extend(["--override", f"run.post_fail_steps={int(post_fail_steps_override)}"])
    for override in list(extra_overrides or []):
        text = str(override or "").strip()
        if text:
            argv.extend(["--override", text])
    if normalized_route_id_override:
        argv.extend(["--route-id", normalized_route_id_override])
    else:
        argv.extend(["--route-ids-file", str(route_ids_file)])
    if stop_carla_on_exit:
        argv.append("--stop-carla-on-exit")
    if carla_ignore_memory_preflight:
        argv.append("--carla-ignore-memory-preflight")
    if enable_lateral:
        argv.append("--enable-lateral")
    argv.append("--enable-guard" if enable_guard else "--no-enable-guard")
    if startup_profile == "lowres_low_quality":
        argv.extend(
            [
                "--carla-extra-args",
                "-windowed -ResX=960 -ResY=540 -quality-level=Low",
            ]
        )
        if force_fresh_start:
            argv.append("--carla-force-fresh-start")
    elif startup_profile == "render_offscreen_no_ros2":
        argv.extend(
            [
                "--carla-force-headless-env",
                "--carla-disable-native-ros2-arg",
                "--carla-extra-args",
                "-RenderOffScreen -ResX=960 -ResY=540 -quality-level=Low",
            ]
        )
        if force_fresh_start:
            argv.append("--carla-force-fresh-start")
    elif startup_profile == "render_offscreen":
        argv.extend(
            [
                "--carla-force-headless-env",
                "--carla-extra-args",
                "-RenderOffScreen -ResX=960 -ResY=540 -quality-level=Low",
            ]
        )
        if force_fresh_start:
            argv.append("--carla-force-fresh-start")
    elif startup_profile == "lowres_no_ros":
        argv.extend(
            [
                "--carla-disable-native-ros2-arg",
                "--carla-extra-args",
                "-windowed -ResX=960 -ResY=540 -quality-level=Low",
            ]
        )
        if force_fresh_start:
            argv.append("--carla-force-fresh-start")

    extra_args = [str(item) for item in list(entry.get("extra_args") or []) if str(item).strip()]
    argv.extend(extra_args)
    return argv


def _route_count_for_mode(entry: Dict[str, Any], mode: str) -> int:
    if mode == "seed":
        seed_route_id = str(entry.get("seed_route_id") or "").strip()
        return 1 if seed_route_id else 0
    route_ids = list(entry.get("missing_history_routes") or [])
    if route_ids:
        return len([route_id for route_id in route_ids if str(route_id).strip()])
    return int(entry.get("route_count") or 0)


def estimate_online_time_budget(
    entry: Dict[str, Any],
    *,
    mode: str,
    startup_profile: str = "default",
    config_path: Path | None = None,
    ticks_override: int | None,
    launch_attempts: int,
    world_ready_timeout_sec: float,
    retry_delay_sec: float,
    route_id_override: str | None = None,
    fixed_delta_seconds: float = DEFAULT_ESTIMATE_FIXED_DELTA_SECONDS,
) -> Dict[str, Any]:
    config_defaults = _config_run_defaults(
        str(Path(config_path).expanduser().resolve()) if config_path is not None else ""
    )
    ticks = int(
        ticks_override
        if ticks_override is not None
        else config_defaults.get("ticks")
        if config_defaults.get("ticks") is not None
        else entry.get("ticks")
        or 700
    )
    route_count = 1 if str(route_id_override or "").strip() else _route_count_for_mode(entry, mode)
    effective_launch_attempts = _effective_launch_attempts(startup_profile, launch_attempts)
    startup_budget_sec = max(int(effective_launch_attempts), 0) * float(world_ready_timeout_sec)
    startup_budget_sec += max(int(effective_launch_attempts) - 1, 0) * float(retry_delay_sec)
    nominal_route_runtime_sec = ticks * float(fixed_delta_seconds)
    nominal_batch_runtime_sec = route_count * nominal_route_runtime_sec
    total_budget_sec = startup_budget_sec + nominal_batch_runtime_sec
    return {
        "ticks": ticks,
        "route_count": route_count,
        "fixed_delta_seconds_assumed": float(fixed_delta_seconds),
        "startup_budget_sec": round(startup_budget_sec, 1),
        "nominal_route_runtime_sec": round(nominal_route_runtime_sec, 1),
        "nominal_batch_runtime_sec": round(nominal_batch_runtime_sec, 1),
        "total_budget_sec": round(total_budget_sec, 1),
        "total_budget_min": round(total_budget_sec / 60.0, 1),
    }


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    entry = load_manifest_entry(args.manifest, args.capability_profile)
    runtime_defaults = _default_runtime_flags_for_capability(args.capability_profile)
    resolved_enable_lateral = (
        runtime_defaults["enable_lateral"] if args.enable_lateral is None else bool(args.enable_lateral)
    )
    resolved_enable_guard = runtime_defaults["enable_guard"] if args.enable_guard is None else bool(args.enable_guard)
    startup_profiles = resolve_startup_profile_sequence(args.startup_profile)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_root_parent = args.batch_root_parent or (
        REPO_ROOT / "runs" / f"town01_capability_manual_online_{timestamp}"
    )
    budget = estimate_online_time_budget(
        entry,
        mode=args.mode,
        startup_profile=startup_profiles[-1],
        config_path=args.config,
        ticks_override=args.ticks,
        launch_attempts=args.carla_launch_attempts,
        world_ready_timeout_sec=args.carla_world_ready_timeout_sec,
        retry_delay_sec=args.carla_retry_delay_sec,
        route_id_override=args.route_id,
    )
    print("# capability_profile:", args.capability_profile)
    print("# source_subset_name:", entry.get("source_subset_name"))
    print("# startup_profile:", args.startup_profile)
    print("# startup_profile_role:", startup_profile_role(args.startup_profile))
    print("# startup_profile_note:", startup_profile_note(args.startup_profile))
    print("# startup_profile_sequence:", ",".join(startup_profiles))
    print("# startup_profile_attempt_count:", len(startup_profiles))
    print("# mode:", args.mode)
    print("# route_count:", budget["route_count"])
    print("# timing_budget_assumption:", f"fixed_delta_seconds={budget['fixed_delta_seconds_assumed']}")
    print("# timing_budget_startup_upper_bound_sec:", budget["startup_budget_sec"])
    print("# timing_budget_nominal_route_runtime_sec:", budget["nominal_route_runtime_sec"])
    print("# timing_budget_nominal_batch_runtime_sec:", budget["nominal_batch_runtime_sec"])
    print("# timing_budget_total_upper_bound_sec:", budget["total_budget_sec"])
    print("# timing_budget_total_upper_bound_min:", budget["total_budget_min"])
    print("# enable_lateral:", str(bool(resolved_enable_lateral)).lower())
    print("# enable_guard:", str(bool(resolved_enable_guard)).lower())

    last_returncode = 0
    for startup_attempt_index, startup_profile in enumerate(startup_profiles, start=1):
        attempt_ctx = startup_attempt_context(
            batch_root_parent,
            args.comparison_label_suffix,
            startup_profile,
            len(startup_profiles),
        )
        argv = build_online_command(
            entry,
            mode=args.mode,
            batch_root_parent=Path(str(attempt_ctx["batch_root_parent"])),
            comparison_label_suffix=str(attempt_ctx["comparison_label_suffix"]),
            startup_profile=startup_profile,
            config_path=args.config,
            ticks_override=args.ticks,
            post_fail_steps_override=args.post_fail_steps,
            launch_attempts=args.carla_launch_attempts,
            world_ready_timeout_sec=args.carla_world_ready_timeout_sec,
            retry_delay_sec=args.carla_retry_delay_sec,
            stop_carla_on_exit=not args.no_stop_carla_on_exit,
            enable_lateral=bool(resolved_enable_lateral),
            enable_guard=bool(resolved_enable_guard),
            force_fresh_start=None,
            route_id_override=args.route_id,
            extra_overrides=list(args.override or []),
            carla_ignore_memory_preflight=bool(args.carla_ignore_memory_preflight),
        )
        print(
            "# startup_attempt:",
            f"{startup_attempt_index}/{len(startup_profiles)}",
            f"profile={startup_profile}",
            f"role={startup_profile_role(startup_profile)}",
        )
        print(" ".join(shlex.quote(item) for item in argv))
        print()

        if args.dry_run:
            continue

        completed = subprocess.run(argv, cwd=str(REPO_ROOT))
        last_returncode = int(completed.returncode)
        if completed.returncode == 0:
            return 0
        if startup_attempt_index < len(startup_profiles):
            print(
                "[online-pack][WARN] attempt failed "
                f"profile={startup_profile} returncode={completed.returncode}; "
                "retrying with the next startup profile",
                flush=True,
            )
    if args.dry_run:
        return 0
    return last_returncode


if __name__ == "__main__":
    raise SystemExit(main())
