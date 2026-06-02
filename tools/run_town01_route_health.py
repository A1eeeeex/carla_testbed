#!/usr/bin/env python3
# LEGACY / OPERATIONAL HELPER: retained for historical Town01 online route-health orchestration.
# Do not add new platform logic here; move reusable code into carla_testbed.analysis or experiments modules.
# Migration target: carla_testbed.analysis.route_health_report and carla_testbed.experiments.
from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from carla_testbed.utils.town01_route_health import (
    ALIAS_PRESETS,
    _runtime_mode_snapshot,
    canonical_flags,
    collect_run_row,
    default_corpus_path,
    default_corpus_report_path,
    default_mainline_config_path,
    evaluate_runtime_contract,
    finalize_town01_run,
    flags_for_alias,
    load_json,
    load_jsonl,
    load_route_corpus,
    render_corpus_report,
    select_route_ids,
    write_json,
    write_jsonl,
    write_platform_comparison,
    write_platform_report,
)
from carla_testbed.config.rig_loader import apply_overrides, load_rig_file

_ROS_SOURCED_ENV: Dict[str, str] | None = None
_DOTENV_CACHE: Dict[str, str] | None = None

CAPABILITY_PROFILE_OVERRIDES: Dict[str, List[str]] = {
    "lane_keep": [
        "algo.apollo.routing.target_speed_mps=8.0",
        "algo.apollo.planning.default_cruise_speed_mps=16.67",
        "algo.apollo.planning.disable_traffic_light_rule=true",
        "algo.apollo.planning.lane_follow_only_scenario=true",
        "algo.apollo.traffic_light.policy=ignore",
        "scenario.traffic_lights.freeze=true",
    ],
    "curve_lane_follow": [
        "algo.apollo.routing.target_speed_mps=7.0",
        "algo.apollo.planning.default_cruise_speed_mps=14.0",
        "algo.apollo.planning.disable_traffic_light_rule=true",
        "algo.apollo.planning.lane_follow_only_scenario=true",
        "algo.apollo.traffic_light.policy=ignore",
        "scenario.traffic_lights.freeze=true",
    ],
    "junction_traverse": [
        "scenario.route_health.spawn_reject_junction=false",
        "scenario.route_health.spawn_junction_window_m=0.0",
        "scenario.route_health.goal_reject_junction=false",
        "algo.apollo.planning.disable_traffic_light_rule=true",
        "algo.apollo.planning.lane_follow_only_scenario=false",
        "algo.apollo.traffic_light.policy=ignore",
        "scenario.traffic_lights.freeze=true",
        "algo.apollo.routing.freeze_after_success=false",
        "algo.apollo.routing.freeze_after_long_route_success_only=false",
    ],
    "traffic_light_actual": [
        "scenario.route_health.spawn_reject_junction=false",
        "scenario.route_health.spawn_junction_window_m=0.0",
        "scenario.route_health.goal_reject_junction=false",
        "algo.apollo.planning.disable_traffic_light_rule=false",
        "algo.apollo.planning.lane_follow_only_scenario=false",
        "algo.apollo.traffic_light.policy=carla_actual",
        "scenario.traffic_lights.freeze=false",
        "scenario.traffic_lights.force_green=false",
        "algo.apollo.routing.freeze_after_success=false",
        "algo.apollo.routing.freeze_after_long_route_success_only=false",
    ],
}

CAPABILITY_PROFILE_DEFAULT_SUBSETS: Dict[str, str] = {
    "lane_keep": "lane_keep_focus_pack",
    "curve_lane_follow": "curve_lane_follow_proxy_pack",
    "junction_traverse": "junction_traverse_proxy_pack",
    "traffic_light_actual": "traffic_light_proxy_pack",
}

CAPABILITY_PROFILE_OVERRIDE_PRESET_FILES: Dict[str, Path] = {
    "lane_keep": REPO_ROOT / "configs" / "io" / "examples" / "town01_route_health_lane_keep.overrides.txt",
    "curve_lane_follow": REPO_ROOT / "configs" / "io" / "examples" / "town01_route_health_curve_lane_follow.overrides.txt",
    "junction_traverse": REPO_ROOT / "configs" / "io" / "examples" / "town01_route_health_junction_traverse.overrides.txt",
    "traffic_light_actual": REPO_ROOT / "configs" / "io" / "examples" / "town01_route_health_traffic_light_actual.overrides.txt",
}

GET_WORLD_RETRY_ATTEMPTS = 3
GET_WORLD_RETRY_DELAY_S = 5.0
LOAD_WORLD_RETRY_ATTEMPTS = 3
LOAD_WORLD_RETRY_DELAY_S = 3.0
LOAD_WORLD_DEFAULT_TIMEOUT_S = 60.0


class CarlaWorldReadyError(RuntimeError):
    def __init__(self, message: str, *, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.details = dict(details or {})


def _load_route_ids_file(path: Path) -> List[str]:
    payload_path = Path(path).expanduser().resolve()
    if payload_path.suffix.lower() == ".json":
        try:
            payload = json.loads(payload_path.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        if isinstance(payload, dict):
            raw_items = payload.get("route_ids")
        elif isinstance(payload, list):
            raw_items = payload
        else:
            raw_items = []
    else:
        raw_items = payload_path.read_text(encoding="utf-8").splitlines()
    route_ids: List[str] = []
    seen: set[str] = set()
    for item in list(raw_items or []):
        route_id = str(item or "").strip()
        if not route_id or route_id in seen:
            continue
        route_ids.append(route_id)
        seen.add(route_id)
    return route_ids


def _load_overrides_file(path: Path) -> List[str]:
    payload_path = Path(path).expanduser().resolve()
    if payload_path.suffix.lower() == ".json":
        try:
            payload = json.loads(payload_path.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
        if isinstance(payload, dict):
            raw_items = payload.get("overrides")
        elif isinstance(payload, list):
            raw_items = payload
        else:
            raw_items = []
    else:
        raw_items = payload_path.read_text(encoding="utf-8").splitlines()
    overrides: List[str] = []
    seen: set[str] = set()
    for item in list(raw_items or []):
        override = str(item or "").strip()
        if not override or override.startswith("#") or override in seen:
            continue
        overrides.append(override)
        seen.add(override)
    return overrides


def _default_capability_profile_overrides(capability_profile: str) -> List[str]:
    preset_path = CAPABILITY_PROFILE_OVERRIDE_PRESET_FILES.get(str(capability_profile or "").strip())
    if preset_path and preset_path.exists():
        return _load_overrides_file(preset_path)
    return list(CAPABILITY_PROFILE_OVERRIDES.get(str(capability_profile or "").strip(), []))


def _resolve_recommended_subset(args: argparse.Namespace) -> str:
    explicit_subset = str(getattr(args, "recommended_subset", "") or "").strip()
    if explicit_subset:
        return explicit_subset
    return CAPABILITY_PROFILE_DEFAULT_SUBSETS.get(
        str(getattr(args, "capability_profile", "") or "").strip(),
        "",
    )


def _resolve_route_ids(corpus: Dict[str, Any], args: argparse.Namespace) -> List[str]:
    explicit_route_id = str(getattr(args, "route_id", "") or "").strip()
    if explicit_route_id:
        return [explicit_route_id]
    route_ids_file = str(getattr(args, "route_ids_file", "") or "").strip()
    if route_ids_file:
        return _load_route_ids_file(Path(route_ids_file))
    recommended_subset = _resolve_recommended_subset(args)
    return select_route_ids(
        corpus,
        route_id="",
        sample_size=int(args.sample_size),
        sample_seed=int(args.sample_seed),
        recommended_subset=recommended_subset,
    )


def _load_dotenv() -> Dict[str, str]:
    global _DOTENV_CACHE
    if _DOTENV_CACHE is not None:
        return dict(_DOTENV_CACHE)
    env_path = REPO_ROOT / ".env"
    data: Dict[str, str] = {}
    if env_path.exists():
        for raw_line in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'\"")
            if key:
                data[key] = value
    _DOTENV_CACHE = data
    return dict(data)


def _ros_sourced_env(base_env: Dict[str, str]) -> Dict[str, str]:
    global _ROS_SOURCED_ENV
    if _ROS_SOURCED_ENV is not None:
        env = base_env.copy()
        env.update(_ROS_SOURCED_ENV)
        return env
    ros_setup = Path("/opt/ros/humble/setup.bash")
    if not ros_setup.exists():
        return base_env.copy()
    try:
        probe = subprocess.run(
            ["bash", "-lc", f"source {shlex.quote(str(ros_setup))} >/dev/null 2>&1 && env -0"],
            cwd=str(REPO_ROOT),
            env=base_env.copy(),
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10.0,
        )
    except subprocess.TimeoutExpired:
        return base_env.copy()
    if probe.returncode != 0:
        return base_env.copy()
    sourced: Dict[str, str] = {}
    for item in probe.stdout.split(b"\0"):
        if not item or b"=" not in item:
            continue
        key, value = item.split(b"=", 1)
        try:
            sourced[key.decode("utf-8")] = value.decode("utf-8")
        except UnicodeDecodeError:
            continue
    _ROS_SOURCED_ENV = sourced
    env = base_env.copy()
    env.update(sourced)
    return env


def _cleanup_runtime_processes() -> None:
    patterns = [
        "dreamview_plus",
        "dreamview.sh start",
        "bootstrap.sh start_plus",
        "tools/apollo10_cyber_bridge/bridge.py",
        "tb_apollo10_gt_bridge",
        "carla_control_bridge",
        "python -m carla_testbed run",
        "examples/run_followstop.py",
        "modules/routing/dag/routing.dag",
        "modules/prediction/dag/prediction.dag",
        "modules/external_command/process_component/dag/external_command_process.dag",
        "modules/planning/planning_component/dag/planning.dag",
        "modules/control/control_component/dag/control.dag",
    ]
    env = os.environ.copy()
    for pattern in patterns:
        subprocess.run(
            ["bash", "-lc", f"pkill -TERM -f {shlex.quote(pattern)} || true"],
            cwd=str(REPO_ROOT),
            env=env,
            check=False,
            timeout=10.0,
        )
    time.sleep(2.0)
    for pattern in patterns:
        probe = subprocess.run(
            ["bash", "-lc", f"pgrep -f {shlex.quote(pattern)} >/dev/null && echo alive || true"],
            cwd=str(REPO_ROOT),
            env=env,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10.0,
        )
        if "alive" in (probe.stdout or ""):
            subprocess.run(
                ["bash", "-lc", f"pkill -KILL -f {shlex.quote(pattern)} || true"],
                cwd=str(REPO_ROOT),
                env=env,
                check=False,
                timeout=10.0,
            )
    time.sleep(1.0)


def _read_meminfo_mb() -> Dict[str, float]:
    values: Dict[str, float] = {}
    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8", errors="replace").splitlines():
            if ":" not in line:
                continue
            key, raw_value = line.split(":", 1)
            parts = raw_value.strip().split()
            if not parts:
                continue
            try:
                kb = float(parts[0])
            except Exception:
                continue
            values[key.strip()] = round(kb / 1024.0, 3)
    except Exception:
        return {}
    return values


def _host_swap_enabled() -> bool:
    swaps_path = Path("/proc/swaps")
    if not swaps_path.exists():
        return False
    try:
        lines = [line.strip() for line in swaps_path.read_text(encoding="utf-8", errors="replace").splitlines() if line.strip()]
    except Exception:
        return False
    return len(lines) > 1


def _memory_preflight_snapshot(min_available_mb: float = 8192.0) -> Dict[str, Any]:
    meminfo = _read_meminfo_mb()
    available_mb = float(meminfo.get("MemAvailable", 0.0) or 0.0)
    total_mb = float(meminfo.get("MemTotal", 0.0) or 0.0)
    swap_enabled = _host_swap_enabled()
    if available_mb < float(min_available_mb):
        status = "blocked_available_memory_below_min"
        blocked = True
    elif not swap_enabled:
        status = "warn_no_swap"
        blocked = False
    else:
        status = "ok"
        blocked = False
    return {
        "status": status,
        "blocked": blocked,
        "host_swap_enabled": swap_enabled,
        "available_memory_mb_before_start": round(available_mb, 3),
        "total_memory_mb": round(total_mb, 3),
        "min_available_memory_mb_required": float(min_available_mb),
    }


def _probe_existing_carla_world(
    host: str,
    port: int,
    *,
    timeout_s: float = 5.0,
) -> Dict[str, Any]:
    from carla_testbed.sim.carla_client import CarlaClientManager

    result: Dict[str, Any] = {
        "ready": False,
        "town": "",
        "error": "",
    }
    try:
        manager = CarlaClientManager(host=host, port=port, timeout=max(1.0, float(timeout_s)))
        client = manager.create_client()
        world = client.get_world()
        town_name = str(world.get_map().name or "").split("/")[-1]
        result["town"] = town_name
        result["ready"] = town_name == "Town01"
    except Exception as exc:
        result["error"] = repr(exc)
    return result


def _cleanup_carla_processes() -> None:
    env = os.environ.copy()
    patterns = [
        "CarlaUE4-Linux-Shipping",
        "CarlaUE4.sh",
    ]
    for pattern in patterns:
        subprocess.run(
            ["bash", "-lc", f"pkill -TERM -f {shlex.quote(pattern)} || true"],
            cwd=str(REPO_ROOT),
            env=env,
            check=False,
            timeout=10.0,
        )
    time.sleep(2.0)
    for pattern in patterns:
        subprocess.run(
            ["bash", "-lc", f"pkill -KILL -f {shlex.quote(pattern)} || true"],
            cwd=str(REPO_ROOT),
            env=env,
            check=False,
            timeout=10.0,
        )
    time.sleep(1.0)


def _build_base_overrides(args: argparse.Namespace) -> List[str]:
    overrides: List[str] = []
    dotenv = _load_dotenv()
    apollo_root = (args.apollo_root or os.environ.get("APOLLO_ROOT") or dotenv.get("APOLLO_ROOT") or "").strip()
    docker_container = (
        args.docker_container or os.environ.get("APOLLO_DOCKER_CONTAINER") or dotenv.get("APOLLO_DOCKER_CONTAINER") or ""
    ).strip()
    carla_root = (args.carla_root or os.environ.get("CARLA_ROOT") or dotenv.get("CARLA_ROOT") or "").strip()
    if apollo_root:
        overrides.extend([f"algo.apollo.apollo_root={apollo_root}", f"paths.apollo_root={apollo_root}"])
    if docker_container:
        overrides.append(f"algo.apollo.docker.container={docker_container}")
    if carla_root:
        overrides.extend(
            [
                f"paths.carla_root={carla_root}",
                f"carla.root={carla_root}",
                f"runtime.carla.root={carla_root}",
                "runtime.carla.start=false",
            ]
        )
    if bool(getattr(args, "carla_disable_native_ros2_arg", False)):
        overrides.extend(
            [
                'runtime.carla.disable_native_ros2_arg=true',
                'carla.disable_native_ros2_arg=true',
                'runtime.carla.extra_args=""',
                'carla.extra_args=""',
            ]
        )
    capability_profile = str(getattr(args, "capability_profile", "") or "").strip()
    overrides_file = str(getattr(args, "overrides_file", "") or "").strip()
    if capability_profile and not overrides_file:
        overrides.extend(_default_capability_profile_overrides(capability_profile))
    if overrides_file:
        overrides.extend(_load_overrides_file(Path(overrides_file)))
    overrides.extend(args.override)
    return overrides


def _compose_prestart_carla_extra_args(args: argparse.Namespace) -> str:
    auto_ros2 = not bool(getattr(args, "carla_disable_native_ros2_arg", False))
    parts: List[str] = []
    if auto_ros2:
        parts.append("--ros2")
    extra_args_raw = str(getattr(args, "carla_extra_args", "") or "").strip()
    if extra_args_raw:
        parts.append(extra_args_raw)
    return " ".join(part for part in parts if part).strip()


def _compose_prestart_carla_env_overrides(args: argparse.Namespace) -> Dict[str, str]:
    env_overrides: Dict[str, str] = {"MALLOC_ARENA_MAX": "2"}
    if bool(getattr(args, "carla_force_headless_env", False)):
        # Keep the mainline offscreen profile truly headless so we do not
        # silently inherit a stale desktop/X11 session during overnight runs.
        env_overrides["DISPLAY"] = ""
        env_overrides["WAYLAND_DISPLAY"] = ""
        env_overrides["XAUTHORITY"] = ""
        env_overrides["SDL_AUDIODRIVER"] = "dummy"
    display_override = str(getattr(args, "carla_display_override", "") or "").strip()
    if display_override:
        env_overrides["DISPLAY"] = display_override
    xauthority_override = str(getattr(args, "carla_xauthority_override", "") or "").strip()
    if xauthority_override:
        env_overrides["XAUTHORITY"] = xauthority_override
    if bool(getattr(args, "carla_force_sdl_x11_no_xrandr", False)) and not bool(
        getattr(args, "carla_force_headless_env", False)
    ):
        env_overrides["SDL_VIDEODRIVER"] = "x11"
        env_overrides["SDL_VIDEO_X11_REQUIRE_XRANDR"] = "0"
    return env_overrides


def _display_probe_monitor_count(display_probe: Dict[str, Any]) -> Optional[int]:
    raw_count = display_probe.get("monitor_count")
    if isinstance(raw_count, int):
        return raw_count
    output_head = "\n".join(str(item) for item in (display_probe.get("xrandr_output_head") or []))
    match = re.search(r"Monitors:\s*(\d+)", output_head)
    if match:
        return int(match.group(1))
    return None


def _startup_probe_failure_family(attempt_row: Dict[str, Any]) -> str:
    status = str(attempt_row.get("status") or "")
    if status == "world_ready":
        return "world_ready"
    diagnostics = attempt_row.get("launcher_diagnostics") or {}
    tail = "\n".join(str(item) for item in (diagnostics.get("latest_server_log_tail") or []))
    lowered_tail = tail.lower()
    has_eof = "error retrieving stream id" in lowered_tail or "failed to read header" in lowered_tail
    has_sig11 = "signal 11" in lowered_tail
    has_request_exit = "requestexitwithstatus" in lowered_tail or "request exit" in lowered_tail
    process_alive = diagnostics.get("process_alive")
    ports = diagnostics.get("target_port_snapshot") or []
    any_port_open = any(bool(item.get("open")) for item in ports if isinstance(item, dict))
    if status == "world_not_ready":
        if has_eof and has_sig11 and process_alive is False:
            return "rpc_ready_world_not_ready_eof_sig11_exit"
        if has_eof and process_alive is False:
            return "rpc_ready_world_not_ready_eof_exit"
        if has_eof and process_alive:
            return "rpc_ready_world_not_ready_eof_alive"
        if process_alive:
            return "rpc_ready_world_not_ready_alive_no_eof"
        return "rpc_ready_world_not_ready_exit_no_eof"
    if status == "rpc_ready":
        if has_eof and has_sig11 and process_alive is False:
            return "rpc_ready_followup_missing_eof_sig11_exit"
        if has_eof and process_alive is False:
            return "rpc_ready_followup_missing_eof_exit"
        if has_eof and process_alive:
            return "rpc_ready_followup_missing_eof_alive"
        if process_alive:
            return "rpc_ready_followup_missing_alive_no_eof"
        return "rpc_ready_followup_missing"
    if status == "rpc_not_ready":
        if has_eof and any_port_open and process_alive:
            return "rpc_not_ready_ports_open_eof_alive"
        if has_request_exit and process_alive:
            return "rpc_not_ready_request_exit_alive"
        if has_request_exit and process_alive is False:
            return "rpc_not_ready_request_exit_exit"
        if has_eof and process_alive is False:
            return "rpc_not_ready_eof_exit"
        if process_alive is False:
            return "rpc_not_ready_early_exit"
        if any_port_open:
            return "rpc_not_ready_ports_open_no_eof"
        return "rpc_not_ready_no_listener"
    if status == "launcher_error":
        if has_eof:
            return "launcher_error_with_eof"
        return "launcher_error"
    if status == "starting":
        return "starting"
    return status or "unknown"


def _normalize_startup_probe_attempt_row(attempt_row: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(attempt_row)
    diagnostics = dict(normalized.get("launcher_diagnostics") or {})
    display_probe = diagnostics.get("display_probe")
    if isinstance(display_probe, dict):
        display_probe = dict(display_probe)
        display_probe.setdefault("monitor_count", _display_probe_monitor_count(display_probe))
        diagnostics["display_probe"] = display_probe
    normalized["launcher_diagnostics"] = diagnostics
    normalized.setdefault("failure_family", _startup_probe_failure_family(normalized))
    return normalized


def _startup_probe_attempt_rows(probe_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    attempts = probe_payload.get("attempts")
    if isinstance(attempts, list) and attempts:
        raw_rows = [row for row in attempts if isinstance(row, dict)]
    else:
        raw_rows = [probe_payload]
    normalized_rows: List[Dict[str, Any]] = []
    for index, raw_row in enumerate(raw_rows, start=1):
        row = _normalize_startup_probe_attempt_row(raw_row)
        row.setdefault("attempt", index)
        normalized_rows.append(row)
    return normalized_rows


def _startup_probe_payload(
    *,
    attempt_rows: Sequence[Dict[str, Any]],
    memory_preflight: Dict[str, Any],
    bootstrap_policy: Dict[str, Any],
    retry_policy: Dict[str, Any],
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "memory_preflight": dict(memory_preflight or {}),
        "bootstrap_policy": dict(bootstrap_policy or {}),
        "attempts": [dict(row) for row in attempt_rows if isinstance(row, dict)],
        "retry_policy": dict(retry_policy or {}),
    }
    final_rows = _startup_probe_attempt_rows(payload)
    final_row = final_rows[-1] if final_rows else {}
    if isinstance(final_row, dict) and final_row:
        payload["final_attempt"] = int(final_row.get("attempt") or len(final_rows) or 0)
        payload["status"] = str(final_row.get("status") or "").strip() or None
        payload["rpc_ready"] = bool(final_row.get("rpc_ready"))
        payload["world_ready"] = bool(final_row.get("world_ready"))
        payload["failure_family"] = str(final_row.get("failure_family") or "").strip() or None
    return payload


def _csv_tokens(raw: Any) -> List[str]:
    return [item.strip() for item in str(raw or "").split(",") if item.strip()]


def _startup_retry_policy_payload(args: argparse.Namespace) -> Dict[str, Any]:
    return {
        "max_attempts": max(1, int(getattr(args, "carla_launch_attempts", 1) or 1)),
        "retry_delay_sec": float(getattr(args, "carla_retry_delay_sec", 2.0) or 2.0),
        "no_retry_failure_families": _csv_tokens(getattr(args, "carla_no_retry_failure_families", "")),
    }


def _startup_retry_decision(
    attempt_row: Dict[str, Any],
    *,
    attempt: int,
    max_attempts: int,
    args: argparse.Namespace,
) -> tuple[bool, str]:
    if attempt >= max_attempts:
        return False, "max_attempts_reached"
    failure_family = str(
        attempt_row.get("failure_family") or _startup_probe_failure_family(attempt_row) or ""
    ).strip()
    no_retry_families = set(_csv_tokens(getattr(args, "carla_no_retry_failure_families", "")))
    if failure_family and failure_family in no_retry_families:
        return False, f"nonretry_family:{failure_family}"
    return True, "retry_allowed"


def _extract_connect_world_error_details(exc: BaseException) -> Dict[str, Any]:
    details = dict(getattr(exc, "details", {}) or {})
    if not details:
        return {}
    extracted: Dict[str, Any] = {}
    for key in (
        "connect_world_elapsed_sec",
        "connect_world_cycle_count",
        "connect_world_get_world_attempt_count",
        "connect_world_load_world_attempt_count",
        "connect_world_last_error",
        "connect_world_fail_fast_reason",
        "launcher_alive_after_failure",
        "launcher_log_contains_end_of_file",
        "launcher_log_contains_request_exit",
        "launcher_log_contains_sig11",
    ):
        if key in details:
            extracted[key] = details[key]
    return extracted


def _best_effort_stop_launcher(launcher: Any) -> None:
    try:
        launcher.stop()
    except Exception:
        pass


def _prestart_carla(args: argparse.Namespace, batch_root: Path) -> CarlaLauncher:
    from tbio.carla.launcher import CarlaLauncher

    dotenv = _load_dotenv()
    carla_root = Path(args.carla_root or os.environ.get("CARLA_ROOT") or dotenv.get("CARLA_ROOT") or "").expanduser().resolve()
    if not carla_root.exists():
        raise SystemExit("CARLA_ROOT not set or invalid")
    run_dir = batch_root / "carla_boot"
    startup_probe_path = run_dir / "carla_startup_probe.json"
    attempt_rows: List[Dict[str, Any]] = []
    memory_preflight = _memory_preflight_snapshot()
    max_attempts = max(1, int(getattr(args, "carla_launch_attempts", 1) or 1))
    world_ready_timeout_s = float(getattr(args, "carla_world_ready_timeout_sec", 180.0) or 180.0)
    world_ready_poll_s = float(getattr(args, "carla_world_ready_poll_sec", 2.0) or 2.0)
    retry_delay_s = float(getattr(args, "carla_retry_delay_sec", 2.0) or 2.0)
    retry_policy = _startup_retry_policy_payload(args)
    extra_args = _compose_prestart_carla_extra_args(args)
    env_overrides = _compose_prestart_carla_env_overrides(args)
    post_wait_ready_settle_s = 3.0
    bootstrap_stability_window_s = 60.0
    bootstrap_policy = {
        "startup_profile": str(getattr(args, "startup_profile", "") or "render_offscreen_no_ros2"),
        "bootstrap_stability_window_s": bootstrap_stability_window_s,
        "memory_guard_enabled": True,
        "memory_high": "9G",
        "memory_max": "10G",
        "oom_policy": "kill",
    }
    if bool(getattr(args, "carla_ignore_memory_preflight", False)):
        memory_preflight["ignored"] = True
        memory_preflight["ignored_for"] = "technical_probe"
        if bool(memory_preflight.get("blocked")):
            memory_preflight["original_status"] = str(memory_preflight.get("status") or "")
            memory_preflight["status"] = "warn_memory_preflight_ignored"
            memory_preflight["blocked"] = False

    def _write_probe() -> None:
        write_json(
            startup_probe_path,
            _startup_probe_payload(
                attempt_rows=attempt_rows,
                memory_preflight=memory_preflight,
                bootstrap_policy=bootstrap_policy,
                retry_policy=retry_policy,
            ),
        )

    last_launcher: CarlaLauncher | None = None
    _write_probe()
    if bool(memory_preflight.get("blocked")) and not bool(getattr(args, "carla_force_fresh_start", False)):
        reuse_probe = _probe_existing_carla_world("127.0.0.1", int(args.carla_port), timeout_s=5.0)
        memory_preflight["reuse_existing_session_ready"] = bool(reuse_probe.get("ready"))
        memory_preflight["reuse_existing_session_town"] = str(reuse_probe.get("town") or "")
        memory_preflight["reuse_existing_session_error"] = str(reuse_probe.get("error") or "")
        if bool(reuse_probe.get("ready")):
            memory_preflight["status"] = "warn_low_available_memory_reuse_existing_session"
            memory_preflight["blocked"] = False
        _write_probe()
    if bool(memory_preflight.get("blocked")):
        raise SystemExit(
            "[town01-route-health] CARLA memory preflight blocked: "
            f"available={memory_preflight['available_memory_mb_before_start']}MB "
            f"required={memory_preflight['min_available_memory_mb_required']}MB"
        )
    for attempt in range(1, max_attempts + 1):
        launcher = CarlaLauncher(
            carla_root=carla_root,
            host="127.0.0.1",
            port=args.carla_port,
            town="",
            extra_args=extra_args,
            foreground=False,
            run_dir=run_dir,
            stop_reused_on_exit=False,
            env_overrides=env_overrides,
            force_fresh_start=bool(getattr(args, "carla_force_fresh_start", False)),
            enable_auto_recovery=bool(getattr(args, "carla_launcher_auto_recovery", False)),
            bootstrap_stability_window_s=bootstrap_stability_window_s,
            use_systemd_scope_memory_guard=True,
            memory_high=bootstrap_policy["memory_high"],
            memory_max=bootstrap_policy["memory_max"],
            oom_policy=bootstrap_policy["oom_policy"],
        )
        attempt_row: Dict[str, Any] = {
            "attempt": attempt,
            "status": "starting",
            "rpc_ready": False,
            "world_ready": False,
            "reused": False,
        }
        last_launcher = launcher
        try:
            attempt_rows.append(attempt_row)
            _write_probe()
            launcher.start()
            attempt_row["reused"] = bool(launcher.reused)
            if not launcher.wait_ready(timeout_s=world_ready_timeout_s, poll_s=min(world_ready_poll_s, 1.0)):
                attempt_row["status"] = "rpc_not_ready"
                attempt_row["rpc_error"] = "rpc_not_ready"
                attempt_row["launcher_diagnostics"] = launcher.diagnostics_snapshot()
                attempt_row["failure_family"] = _startup_probe_failure_family(attempt_row)
                retry_allowed, retry_reason = _startup_retry_decision(
                    attempt_row, attempt=attempt, max_attempts=max_attempts, args=args
                )
                attempt_row["retry_eligible"] = retry_allowed
                attempt_row["retry_decision_reason"] = retry_reason
                _write_probe()
                if retry_allowed:
                    launcher.stop()
                    _cleanup_carla_processes()
                    time.sleep(3.0)
                    continue
                _best_effort_stop_launcher(launcher)
                raise SystemExit("CARLA failed to become ready")
            attempt_row["status"] = "rpc_ready"
            attempt_row["rpc_ready"] = True
            attempt_row["launcher_diagnostics"] = launcher.diagnostics_snapshot()
            attempt_row["failure_family"] = _startup_probe_failure_family(attempt_row)
            if post_wait_ready_settle_s > 0.0:
                time.sleep(post_wait_ready_settle_s)
            _write_probe()
            try:
                client, world = _connect_world(
                    "127.0.0.1",
                    int(args.carla_port),
                    timeout_s=world_ready_timeout_s,
                    poll_s=world_ready_poll_s,
                    startup_diagnostics=lambda: launcher.diagnostics_snapshot(probe_rpc=False),
                    fail_fast_on_bad_world_after_s=(
                        min(20.0, max(8.0, world_ready_timeout_s * 0.25))
                        if bool(getattr(args, "carla_launcher_auto_recovery", False))
                        else 0.0
                    ),
                )
                attempt_row["status"] = "world_ready"
                attempt_row["world_ready"] = True
                attempt_row["world_map"] = str(world.get_map().name)
                attempt_row["launcher_diagnostics"] = launcher.diagnostics_snapshot()
                attempt_row["failure_family"] = _startup_probe_failure_family(attempt_row)
                attempt_row["retry_eligible"] = False
                attempt_row["retry_decision_reason"] = "world_ready"
                _write_probe()
                del client
                return launcher
            except Exception as exc:
                attempt_row["status"] = "world_not_ready"
                attempt_row["world_error"] = repr(exc)
                attempt_row.update(_extract_connect_world_error_details(exc))
                attempt_row["launcher_diagnostics"] = launcher.diagnostics_snapshot()
                attempt_row["failure_family"] = _startup_probe_failure_family(attempt_row)
                retry_allowed, retry_reason = _startup_retry_decision(
                    attempt_row, attempt=attempt, max_attempts=max_attempts, args=args
                )
                attempt_row["retry_eligible"] = retry_allowed
                attempt_row["retry_decision_reason"] = retry_reason
                _write_probe()
                fail_fast_reason = str(attempt_row.get("connect_world_fail_fast_reason") or "").strip()
                if fail_fast_reason:
                    print(
                        "[town01-route-health][WARN] CARLA RPC came up but world readiness "
                        f"hit fail-fast family {fail_fast_reason} on attempt {attempt}/{max_attempts}: {exc}",
                        flush=True,
                    )
                else:
                    print(
                        "[town01-route-health][WARN] CARLA RPC came up but world readiness "
                        f"failed on attempt {attempt}/{max_attempts}: {exc}",
                        flush=True,
                    )
                if retry_allowed:
                    launcher.stop()
                    _cleanup_carla_processes()
                    time.sleep(retry_delay_s)
                    continue
                _best_effort_stop_launcher(launcher)
                raise SystemExit("CARLA failed to become world-ready")
        except SystemExit:
            raise
        except Exception as exc:
            attempt_row["status"] = "launcher_error"
            attempt_row["launcher_error"] = repr(exc)
            attempt_row["launcher_diagnostics"] = launcher.diagnostics_snapshot()
            attempt_row["failure_family"] = _startup_probe_failure_family(attempt_row)
            retry_allowed, retry_reason = _startup_retry_decision(
                attempt_row, attempt=attempt, max_attempts=max_attempts, args=args
            )
            attempt_row["retry_eligible"] = retry_allowed
            attempt_row["retry_decision_reason"] = retry_reason
            _write_probe()
            if retry_allowed:
                try:
                    launcher.stop()
                except Exception:
                    pass
                _cleanup_carla_processes()
                time.sleep(retry_delay_s)
                continue
            _best_effort_stop_launcher(launcher)
            raise
    if last_launcher is not None:
        _best_effort_stop_launcher(last_launcher)
        raise SystemExit("CARLA failed to become world-ready")
    raise SystemExit("CARLA failed to launch")


def _connect_world(
    host: str,
    port: int,
    *,
    timeout_s: float = 180.0,
    poll_s: float = 2.0,
    startup_diagnostics: Optional[Callable[[], Dict[str, Any]]] = None,
    fail_fast_on_bad_world_after_s: float = 0.0,
):
    from carla_testbed.sim.carla_client import CarlaClientManager

    connect_started_at = time.time()
    manager_cycle_count = 0
    get_world_attempt_count = 0
    load_world_attempt_count = 0
    last_err: Exception | None = None

    def _world_ready_fail_fast_details() -> Dict[str, Any]:
        diagnostics = {}
        if callable(startup_diagnostics):
            try:
                diagnostics = dict(startup_diagnostics() or {})
            except Exception:
                diagnostics = {}
        ports = diagnostics.get("target_port_snapshot") or []
        any_port_open = any(bool(item.get("open")) for item in ports if isinstance(item, dict))
        tail = "\n".join(str(item) for item in (diagnostics.get("latest_server_log_tail") or []))
        lowered_tail = tail.lower()
        has_eof = "error retrieving stream id" in lowered_tail or "failed to read header" in lowered_tail
        has_request_exit = "requestexitwithstatus" in lowered_tail or "request exit" in lowered_tail
        has_sig11 = "signal 11" in lowered_tail
        details = {
            "launcher_alive_after_failure": diagnostics.get("process_alive"),
            "launcher_log_contains_end_of_file": has_eof,
            "launcher_log_contains_request_exit": has_request_exit,
            "launcher_log_contains_sig11": has_sig11,
            "connect_world_elapsed_sec": round(time.time() - connect_started_at, 3),
            "connect_world_cycle_count": manager_cycle_count,
            "connect_world_get_world_attempt_count": get_world_attempt_count,
            "connect_world_load_world_attempt_count": load_world_attempt_count,
            "connect_world_last_error": repr(last_err) if last_err is not None else "",
        }
        fail_fast_reason = ""
        if diagnostics.get("process_alive") and any_port_open and has_eof and has_request_exit:
            fail_fast_reason = "rpc_ready_world_not_ready_eof_request_exit_alive"
        elif diagnostics.get("process_alive") and any_port_open and has_eof:
            fail_fast_reason = "rpc_ready_world_not_ready_eof_alive"
        elif diagnostics.get("process_alive") and any_port_open and has_request_exit:
            fail_fast_reason = "rpc_ready_world_not_ready_request_exit_alive"
        if fail_fast_reason:
            details["connect_world_fail_fast_reason"] = fail_fast_reason
        return details

    def _maybe_fail_fast() -> None:
        if fail_fast_on_bad_world_after_s <= 0.0:
            return
        if (time.time() - connect_started_at) < float(fail_fast_on_bad_world_after_s):
            return
        details = _world_ready_fail_fast_details()
        fail_fast_reason = str(details.get("connect_world_fail_fast_reason") or "").strip()
        if not fail_fast_reason:
            return
        raise CarlaWorldReadyError(
            f"World readiness fail-fast after {details['connect_world_elapsed_sec']:.1f}s: {fail_fast_reason}",
            details=details,
        )

    def _remaining_call_timeout(deadline_s: float, default_timeout_s: float) -> float:
        remaining = max(0.0, float(deadline_s) - time.time())
        if remaining <= 0.0:
            raise TimeoutError(
                f"Timed out waiting for Town01 world readiness at {host}:{port} after {float(timeout_s):.0f}s"
            )
        return min(float(default_timeout_s), remaining)

    def _bringup_attempt_callback(phase: str, payload: Dict[str, Any]) -> None:
        nonlocal get_world_attempt_count, load_world_attempt_count, last_err
        if phase == "get_world_attempt_start":
            get_world_attempt_count = max(get_world_attempt_count, int(payload.get("attempt") or 0))
            return
        if phase == "get_world_attempt_failed":
            error = payload.get("error")
            last_err = RuntimeError(str(error)) if error else last_err
            print(
                f"[town01-route-health][WARN] get_world attempt {payload.get('attempt')}/{payload.get('attempts')} "
                f"failed: {error}",
                flush=True,
            )
            _maybe_fail_fast()
            return
        if phase == "load_world_attempt_start":
            load_world_attempt_count = max(load_world_attempt_count, int(payload.get("attempt") or 0))
            return
        if phase == "load_world_attempt_failed":
            error = payload.get("error")
            last_err = RuntimeError(str(error)) if error else last_err
            print(
                f"[town01-route-health][WARN] load_world attempt {payload.get('attempt')}/{payload.get('attempts')} "
                f"failed: {error}",
                flush=True,
            )
            _maybe_fail_fast()

    def _load_town01_world(client, *, previous_timeout: float, deadline_s: float):
        nonlocal last_err
        try:
            from carla_testbed.sim.bringup import load_world_with_retry

            return load_world_with_retry(
                client,
                "Town01",
                attempts=LOAD_WORLD_RETRY_ATTEMPTS,
                delay_s=LOAD_WORLD_RETRY_DELAY_S,
                timeout_s=_remaining_call_timeout(deadline_s, LOAD_WORLD_DEFAULT_TIMEOUT_S),
                restore_timeout_s=previous_timeout,
                attempt_callback=_bringup_attempt_callback,
            )
        except Exception as exc:
            last_err = exc
            raise CarlaWorldReadyError(
                "load_world Town01 failed",
                details=_world_ready_fail_fast_details(),
            ) from exc

    deadline = time.time() + float(timeout_s)
    manager_timeout_s = min(max(1.0, float(timeout_s)), 30.0)
    while time.time() < deadline:
        try:
            manager_cycle_count += 1
            manager = CarlaClientManager(host=host, port=port, timeout=manager_timeout_s)
            client = manager.create_client()
            break
        except Exception as exc:
            last_err = exc
            print(f"[town01-route-health][WARN] CARLA client not ready yet: {exc}", flush=True)
            _maybe_fail_fast()
            time.sleep(float(poll_s))
    else:
        raise CarlaWorldReadyError(
            f"Timed out waiting for Town01 world readiness at {host}:{port} after {float(timeout_s):.0f}s",
            details=_world_ready_fail_fast_details(),
        ) from last_err

    world = None
    try:
        from carla_testbed.sim.bringup import get_world_with_retry

        world = get_world_with_retry(
            client,
            attempts=GET_WORLD_RETRY_ATTEMPTS,
            delay_s=GET_WORLD_RETRY_DELAY_S,
            timeout_s=_remaining_call_timeout(deadline, manager.timeout),
            attempt_callback=_bringup_attempt_callback,
        )
    except Exception as exc:
        last_err = exc
        print(
            "[town01-route-health][WARN] get_world never stabilized after RPC came up; "
            "trying direct load_world('Town01') recovery",
            flush=True,
        )
        world = _load_town01_world(
            client,
            previous_timeout=manager.timeout,
            deadline_s=deadline,
        )

    current_town = world.get_map().name.split("/")[-1]
    if current_town != "Town01":
        world = _load_town01_world(
            client,
            previous_timeout=manager.timeout,
            deadline_s=deadline,
        )
    return client, world


def _default_corpus_cfg() -> Town01RouteHealthConfig:
    from carla_testbed.scenarios.town01_route_health import Town01RouteHealthConfig

    return Town01RouteHealthConfig(
        random_seed=1,
        strict_spawn=False,
        route_step_m=5.0,
        spawn_min_forward_length_m=160.0,
        spawn_min_backward_length_m=25.0,
        spawn_reject_junction=True,
        spawn_junction_window_m=12.0,
        spawn_min_successor_count=1,
        goal_min_route_length_m=220.0,
        goal_max_route_length_m=360.0,
        goal_min_end_margin_m=25.0,
        goal_reject_junction=True,
        max_spawn_candidates=9999,
        max_goal_attempts=6,
    )


def _write_static_docs(repo_artifacts: Path) -> None:
    docs = {
        "town01_route_health_state_machine.md": "\n".join(
            [
                "# Town01 Route Health State Machine",
                "",
                "- `CARLA_READY`: CARLA world ready and town verified as `Town01`.",
                "- `MAP_READY`: route corpus route resolved and scenario metadata written.",
                "- `ROUTING_READY`: `routing_event_debug.jsonl` contains the first `routing_request_sent=true` event.",
                "- `ROUTE_ESTABLISHED`: `routing_success_count >= 1`.",
                "- `PLANNING_READY`: `planning_topic_debug.jsonl` first reaches `trajectory_point_count > 0`.",
                "- `CONTROL_READY`: control consume event stream shows planning trajectory consumption.",
                "- `CRUISE_ACTIVE`: control is active and vehicle has moved at least `5m` with `speed >= 1.0m/s`.",
                "- `ROUTE_COMPLETED`: `route_completion_ratio >= 0.95` or `final_goal_distance_m <= 10m`.",
                "- `ROUTE_FAILED`: run ended without meeting completion and is attributed to the latest unreached state edge.",
            ]
        )
        + "\n",
        "town01_route_health_acceptance_policy.md": "\n".join(
            [
                "# Town01 Route Health Acceptance Policy",
                "",
                "- Core policy is single-source and route-health centric.",
                "- Core thresholds:",
                "  - `routing_request_count >= 1`",
                "  - `route_establishment_latency_sec <= 45`",
                "  - `planning_nonzero_ratio >= 0.80`",
                "  - `control_used_planning_ratio >= 0.80`",
                "  - `invalid_goal_count == 0`",
                "  - `long_phase_invalid_goal_skip == 0`",
                "  - `unstable_reference_line_skip == 0`",
                "  - `control_no_trajectory_count == 0`",
                "  - `route_distance_achieved_m >= 140`",
                "  - `route_completion_ratio >= 0.55`",
                "  - `max_speed_mps >= 5.0`",
                "  - `whether_vehicle_moved == true`",
                "  - `low_speed_creep_duration_sec <= 2.0`",
                "  - `summary_status == finalized`",
                "  - `finalized_from_event_stream == true`",
                "  - `summary_written_successfully == true`",
                "  - `manifest_completeness == 1.0`",
                "- Labels:",
                "  - `route_not_established`",
                "  - `route_established_but_no_control_progress`",
                "  - `route_established_but_behavior_unhealthy`",
                "  - `route_health_candidate`",
                "  - `route_health_pass`",
            ]
        )
        + "\n",
        "town01_route_health_run_guide.md": "\n".join(
            [
                "# Town01 Route Health Run Guide",
                "",
                "## Canonical Usage",
                "",
                "```bash",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py build-corpus",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --sample-size 12",
                "```",
                "",
                "## Single Route Rerun",
                "",
                "```bash",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --route-id town01_rh_spawn068_goal068 --ticks 700",
                "```",
                "",
                "## Frozen Route-ID Replay",
                "",
                "```bash",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile curve_lane_follow --route-ids-file artifacts/town01_capability_review_runbook_YYYYMMDD_route_ids/curve_lane_follow__curve_lane_follow_proxy_pack.route_ids.json --overrides-file artifacts/town01_capability_review_runbook_YYYYMMDD_overrides/curve_lane_follow__curve_lane_follow_proxy_pack.overrides.txt --ticks 700",
                "```",
                "",
                "## Capability Review Runbook",
                "",
                "```bash",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_capability_review_packs.py",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/analyze_town01_capability_review_manifest.py --manifest artifacts/town01_capability_review_manifest_YYYYMMDD.json",
                "```",
                "",
                "运行 `tools/run_town01_capability_review_packs.py` 后，会同步产出：",
                "",
                "- `artifacts/town01_capability_missing_history_runbook_YYYYMMDD.md`",
                "- `artifacts/town01_capability_missing_history_manifest_YYYYMMDD.json`",
                "- `artifacts/town01_capability_missing_history_summary_YYYYMMDD.csv`",
                "",
                "## Capability Missing-History Rerun",
                "",
                "```bash",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_capability_missing_history_packs.py --manifest artifacts/town01_capability_review_manifest_YYYYMMDD.json",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile curve_lane_follow --route-ids-file artifacts/town01_capability_missing_history_runbook_YYYYMMDD_route_ids/curve_lane_follow__curve_lane_follow_proxy_pack__missing_history__seed.route_ids.json --overrides-file artifacts/town01_capability_missing_history_runbook_YYYYMMDD_overrides/curve_lane_follow__curve_lane_follow_proxy_pack__missing_history.overrides.txt --ticks 700",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile curve_lane_follow --route-ids-file artifacts/town01_capability_missing_history_runbook_YYYYMMDD_route_ids/curve_lane_follow__curve_lane_follow_proxy_pack__missing_history.route_ids.json --overrides-file artifacts/town01_capability_missing_history_runbook_YYYYMMDD_overrides/curve_lane_follow__curve_lane_follow_proxy_pack__missing_history.overrides.txt --ticks 700",
                "```",
                "",
                "## Capability Profiles",
                "",
                "```bash",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile lane_keep",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile curve_lane_follow",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile junction_traverse",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile traffic_light_actual",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile lane_keep --recommended-subset lane_keep_focus_pack --sample-size 4",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile curve_lane_follow --recommended-subset curve_lane_follow_proxy_pack --sample-size 4",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile junction_traverse --recommended-subset junction_traverse_proxy_pack --sample-size 4",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile traffic_light_actual --recommended-subset traffic_light_proxy_pack --sample-size 4",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile curve_lane_follow --recommended-subset curve_lane_follow_seed_pack --sample-size 4",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile junction_traverse --recommended-subset junction_traverse_seed_pack --sample-size 4",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile traffic_light_actual --recommended-subset traffic_light_seed_pack --sample-size 4",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile curve_lane_follow --recommended-subset curve_lane_follow_history_gap_queue --sample-size 4",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile junction_traverse --recommended-subset junction_traverse_history_gap_queue --sample-size 4",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile traffic_light_actual --recommended-subset traffic_light_history_gap_queue --sample-size 4",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile curve_lane_follow --recommended-subset curve_lane_follow_focus_pack --sample-size 4",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile junction_traverse --recommended-subset junction_traverse_focus_pack --sample-size 4",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile traffic_light_actual --recommended-subset traffic_light_focus_pack --sample-size 4",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile lane_keep --recommended-subset lane_keep_review_priority_queue --sample-size 4",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile curve_lane_follow --recommended-subset curve_lane_follow_review_priority_queue --sample-size 4",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile junction_traverse --recommended-subset junction_traverse_review_priority_queue --sample-size 4",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile traffic_light_actual --recommended-subset traffic_light_review_priority_queue --sample-size 4",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile lane_keep --recommended-subset lane_keep_first_wave_smoke --sample-size 4",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile curve_lane_follow --recommended-subset curve_lane_follow_first_wave_smoke --sample-size 4",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile junction_traverse --recommended-subset junction_traverse_first_wave_smoke --sample-size 4",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --capability-profile traffic_light_actual --recommended-subset traffic_light_first_wave_smoke --sample-size 4",
                "```",
                "",
                "手工切换配置时，也可以直接传：",
                "",
                "```bash",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --config configs/io/examples/town01_apollo_route_health.yaml --recommended-subset lane_keep_candidate --sample-size 4",
                "```",
                "",
                "## Lateral Validation",
                "",
                "```bash",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --recommended-subset lateral_smoke_candidate --sample-size 4 --enable-lateral --enable-guard",
                "```",
                "",
                "## Startup Isolator",
                "",
                "```bash",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py build-corpus --start-carla --carla-disable-native-ros2-arg --carla-extra-args '-windowed -ResX=960 -ResY=540 -quality-level=Low'",
                "```",
                "",
                "## X11 No-XRandR Probe",
                "",
                "```bash",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py build-corpus --start-carla --carla-force-fresh-start --carla-force-sdl-x11-no-xrandr --carla-extra-args '-windowed -ResX=1280 -ResY=720'",
                "```",
                "",
                "## Display Override Replay",
                "",
                "```bash",
                "DISPLAY=:0 Xephyr :99 -screen 1280x720 -noreset &",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py build-corpus --start-carla --carla-force-fresh-start --carla-force-sdl-x11-no-xrandr --carla-display-override :99 --carla-extra-args '-windowed -ResX=960 -ResY=540'",
                "```",
                "",
                "## Recording Validation",
                "",
                "```bash",
                "/home/ubuntu/miniconda3/bin/conda run -n carla16 python tools/run_town01_route_health.py run --route-id <route_id> --enable-recording",
                "```",
                "",
                "## Deprecated Aliases",
                "",
                "- `--alias relaxed`",
                "- `--alias strict`",
                "- `--alias lateral_enabled`",
                "- `--profiles relaxed,strict` remains supported but deprecated.",
            ]
        )
        + "\n",
        "town01_route_health_extension_points.md": "\n".join(
            [
                "# Town01 Route Health Extension Points",
                "",
                "- `enable_lateral`: releases final steer output for true lateral validation in the same scene.",
                "- `enable_guard`: keeps transparent lateral guards enabled and reports guard trigger ratio.",
                "- `enable_lateral_debug`: preserves heavier lateral debug evidence without forking the scene.",
                "- `enable_recording`: enables Dreamview recording and requires explicit manifest/status outputs.",
                "- `enable_strict_observation`: raises observability depth and recording/debug evidence retention.",
                "- `enable_stage6_reference_line`: enables Apollo internal stage6 reference-line debug/feature flags.",
                "- `stage6_clear_lane_follow_cache_on_new_command`: clears lane-follow cache on new command landing frames.",
                "- `stage6_reference_line_generation_guard`: enables the stage6 reference-line generation guard.",
            ]
        )
        + "\n",
    }
    for name, content in docs.items():
        path = repo_artifacts / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")


def _build_corpus(output_path: Path, report_path: Path, host: str, port: int) -> Dict[str, Any]:
    from carla_testbed.scenarios.town01_route_health import Town01RouteHealthScenario

    _client, world = _connect_world(host, port)
    carla_map = world.get_map()
    bp_lib = world.get_blueprint_library()
    scenario = Town01RouteHealthScenario(_default_corpus_cfg())
    corpus = scenario.build_route_corpus(carla_map)
    write_json(output_path, corpus)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(render_corpus_report(corpus), encoding="utf-8")
    _ = bp_lib  # keep import/use explicit for runtime sanity
    return corpus


def _legacy_aliases(args: argparse.Namespace) -> List[str]:
    if args.profiles:
        aliases = [item.strip() for item in str(args.profiles).split(",") if item.strip()]
        invalid = [item for item in aliases if item not in ALIAS_PRESETS]
        if invalid:
            raise SystemExit(f"Unknown deprecated profiles: {invalid}")
        print("[town01-route-health][WARN] --profiles is deprecated; use canonical flags or --alias")
        return aliases
    if args.alias:
        return [str(args.alias)]
    return []


def _flags_for_run(args: argparse.Namespace, alias: str = "") -> Dict[str, Any]:
    if alias:
        print(f"[town01-route-health][WARN] alias '{alias}' is deprecated; mapped to canonical flags")
        flags = flags_for_alias(alias)
    else:
        flags = canonical_flags(
            {
                "enable_lateral": bool(args.enable_lateral),
                "enable_guard": bool(args.enable_guard),
                "enable_lateral_debug": bool(args.enable_lateral_debug),
                "enable_recording": bool(args.enable_recording),
                "enable_strict_observation": bool(args.enable_strict_observation),
                "enable_stage6_reference_line": bool(args.enable_stage6_reference_line),
                "stage6_clear_lane_follow_cache_on_new_command": bool(
                    args.stage6_clear_lane_follow_cache_on_new_command
                ),
                "stage6_reference_line_generation_guard": bool(args.stage6_reference_line_generation_guard),
            }
        )
    return flags


def _overrides_for_flags(
    flags: Dict[str, Any],
    *,
    corpus_path: Path,
    route_id: str,
    ticks: int,
    comparison_label: str,
    capability_profile: str = "",
) -> List[str]:
    overrides = [
        f"run.ticks={int(ticks)}",
        f"scenario.route_health.route_corpus_path={str(corpus_path)}",
        f"scenario.route_health.route_id={route_id}",
        f"run.comparison_label={comparison_label}",
        "runtime.carla.start=false",
        "record.probe.enable=true",
        "record.control_log.enable=true",
    ]
    if capability_profile:
        overrides.append(f"run.capability_profile={capability_profile}")
    if flags["enable_lateral"]:
        overrides.extend(
            [
                "algo.apollo.control_mapping.force_zero_steer_output=false",
                "algo.apollo.control_mapping.straight_lane_zero_steer_enabled=false",
                "algo.apollo.planning.acc_only_mode=false",
                "algo.apollo.planning.longitudinal_only_pipeline=false",
                "algo.apollo.planning.longitudinal_only_keep_lane_follow_path=false",
            ]
        )
    else:
        overrides.extend(
            [
                "algo.apollo.control_mapping.force_zero_steer_output=true",
                "algo.apollo.control_mapping.straight_lane_zero_steer_enabled=false",
                "algo.apollo.planning.acc_only_mode=true",
                "algo.apollo.planning.longitudinal_only_pipeline=true",
                "algo.apollo.planning.longitudinal_only_keep_lane_follow_path=true",
            ]
        )
    guard_enabled = bool(flags["enable_guard"])
    overrides.extend(
        [
            f"algo.apollo.control_mapping.low_speed_steer_guard_enabled={str(guard_enabled).lower()}",
            f"algo.apollo.control_mapping.low_speed_sustained_saturation_guard_enabled={str(guard_enabled).lower()}",
            f"algo.apollo.control_mapping.sustained_lateral_guard_enabled={str(guard_enabled).lower()}",
            f"algo.apollo.control_mapping.trajectory_contract_lateral_guard_enabled={str(guard_enabled).lower()}",
            f"algo.apollo.dreamview.record.enabled={str(bool(flags['enable_recording'])).lower()}",
        ]
    )
    if flags["enable_recording"]:
        overrides.extend(
            [
                "algo.apollo.dreamview.record.capture_mode=tick_snapshot",
                "algo.apollo.dreamview.record.mode=screen",
            ]
        )
    if flags["enable_strict_observation"]:
        overrides.extend(
            [
                "algo.apollo.map_contract.fail_fast_on_high_risk_mismatch=true",
                "record.probe.max_msgs=8",
            ]
        )
    if flags["enable_lateral_debug"]:
        overrides.extend(
            [
                "algo.apollo.bridge.debug_dump_control_raw=true",
                "record.probe.max_msgs=8",
            ]
        )
    overrides.extend(
        [
            f"algo.apollo.stage6_reference_line.enabled={str(bool(flags['enable_stage6_reference_line'])).lower()}",
            "algo.apollo.stage6_reference_line.clear_lane_follow_cache_on_new_command="
            + str(bool(flags["stage6_clear_lane_follow_cache_on_new_command"])).lower(),
            "algo.apollo.stage6_reference_line.reference_line_generation_guard="
            + str(bool(flags["stage6_reference_line_generation_guard"])).lower(),
        ]
    )
    return overrides


def _preview_runtime_contract(
    config_path: Path,
    *,
    base_overrides: List[str],
    flags: Dict[str, Any],
    corpus_path: Path,
    route_id: str,
    ticks: int,
    comparison_label: str,
    capability_profile: str = "",
    final_overrides: Optional[List[str]] = None,
) -> Dict[str, Any]:
    preview_cfg = apply_overrides(
        load_rig_file(str(config_path)),
        list(base_overrides)
        + _overrides_for_flags(
            flags,
            corpus_path=corpus_path,
            route_id=route_id,
            ticks=ticks,
            comparison_label=comparison_label,
            capability_profile=capability_profile,
        )
        + [f"run.profile_name=town01_route_health_{comparison_label}_{route_id}"]
        + list(final_overrides or []),
    )
    runtime_mode = _runtime_mode_snapshot(flags, preview_cfg)
    runtime_contract = evaluate_runtime_contract(capability_profile, runtime_mode)
    return {
        "effective_cfg": preview_cfg,
        "runtime_mode": runtime_mode,
        "runtime_contract": runtime_contract,
    }


def _invoke_run(
    *,
    run_dir: Path,
    route_id: str,
    corpus_path: Path,
    config_path: Path,
    flags: Dict[str, Any],
    comparison_label: str,
    ticks: int,
    base_overrides: List[str],
    capability_profile: str = "",
    run_overrides: Optional[List[str]] = None,
    final_overrides: Optional[List[str]] = None,
) -> int:
    _cleanup_runtime_processes()
    cmd = [
        sys.executable,
        "-m",
        "carla_testbed",
        "run",
        "--config",
        str(config_path),
        "--run-dir",
        str(run_dir),
    ]
    resolved_run_overrides = list(run_overrides or [])
    if not resolved_run_overrides:
        resolved_run_overrides = _overrides_for_flags(
            flags,
            corpus_path=corpus_path,
            route_id=route_id,
            ticks=ticks,
            comparison_label=comparison_label,
            capability_profile=capability_profile,
        )
    profile_name = f"town01_route_health_{comparison_label}_{route_id}"
    resolved_run_overrides.append(f"run.profile_name={profile_name}")
    # CLI --override is user intent and must win over capability/runtime defaults.
    for item in [*base_overrides, *resolved_run_overrides, *list(final_overrides or [])]:
        cmd.extend(["--override", item])
    env = _ros_sourced_env(os.environ.copy())
    print(f"[town01-route-health] run route_id={route_id} label={comparison_label} -> {run_dir}")
    try:
        return subprocess.run(cmd, cwd=str(REPO_ROOT), env=env, check=False).returncode
    finally:
        _cleanup_runtime_processes()


def _update_manifest(batch_root: Path, manifest: Dict[str, Any]) -> None:
    write_json(batch_root / "artifacts" / "town01_route_health_run_manifest.json", manifest)


def _resolve_run_dir(run_dir: Path) -> Path:
    run_dir = Path(run_dir).expanduser().resolve()
    redirect_path = run_dir / "RUN_DIR_REDIRECT.txt"
    if redirect_path.exists():
        target_raw = redirect_path.read_text(encoding="utf-8", errors="replace").strip()
        if target_raw:
            target = Path(target_raw).expanduser()
            if not target.is_absolute():
                target = (run_dir.parent / target).resolve()
            if target.exists():
                return target
    return run_dir


def _discover_batch_runs(batch_root: Path) -> List[Dict[str, Any]]:
    discovered: List[Dict[str, Any]] = []
    names = {child.name for child in batch_root.iterdir() if child.is_dir()}
    for child in sorted(batch_root.iterdir()):
        if not child.is_dir():
            continue
        if child.name in {"artifacts", "carla_boot"}:
            continue
        base_name = child.name
        if "__" not in base_name:
            continue
        if re.search(r"__\d+$", base_name):
            prefix = re.sub(r"__\d+$", "", base_name)
            if prefix in names:
                continue
        comparison_label, route_id = base_name.split("__", 1)
        effective_dir = _resolve_run_dir(child)
        if not effective_dir.exists():
            continue
        discovered.append(
            {
                "run_index": len(discovered) + 1,
                "alias": "",
                "comparison_label": comparison_label,
                "route_id": route_id,
                "flags": canonical_flags({}),
                "run_dir": str(child),
                "effective_run_dir": str(effective_dir),
                "status": "discovered",
                "returncode": None,
            }
        )
    return discovered


def _reconcile_manifest_item_from_analysis(
    item: Dict[str, Any],
    *,
    requested_run_dir: Path,
    finalized: Dict[str, Any],
    row: Dict[str, Any],
) -> bool:
    changed = False
    effective_run_dir = _resolve_run_dir(requested_run_dir)
    effective_run_dir_s = str(effective_run_dir)
    if str(item.get("effective_run_dir") or "") != effective_run_dir_s:
        item["effective_run_dir"] = effective_run_dir_s
        changed = True

    summary_path = effective_run_dir / "summary.json"
    if summary_path.exists():
        summary_path_s = str(summary_path.resolve())
        if str(item.get("summary_path") or "") != summary_path_s:
            item["summary_path"] = summary_path_s
            changed = True

    summary_status = finalized.get("summary_status") or row.get("summary_status")
    if summary_status and str(item.get("summary_status") or "") != str(summary_status):
        item["summary_status"] = summary_status
        changed = True

    for key in ("finalized_from_event_stream", "summary_written_successfully"):
        value = row.get(key)
        if value is None:
            value = finalized.get(key)
        if value is None:
            continue
        if item.get(key) != value:
            item[key] = value
            changed = True

    if not item.get("route_id") and row.get("route_id"):
        item["route_id"] = row["route_id"]
        changed = True

    status = str(item.get("status") or "").strip()
    if summary_status == "finalized" and status in {"", "running"}:
        item["status"] = "completed_from_analysis"
        changed = True

    if changed:
        item["analyzed_at"] = datetime.now().isoformat(timespec="seconds")
    return changed


def _publish_outputs(batch_root: Path, rows: List[Dict[str, Any]], *, publish_repo: bool = False) -> None:
    batch_artifacts = batch_root / "artifacts"
    repo_artifacts = REPO_ROOT / "artifacts"
    comparison_name = "town01_route_health_platform_comparison.csv"
    report_name = "town01_route_health_platform_mainline_report.md"
    write_platform_comparison(rows, batch_artifacts / comparison_name)
    write_platform_report(rows, batch_artifacts / report_name)
    if publish_repo:
        write_platform_comparison(rows, repo_artifacts / comparison_name)
        write_platform_report(rows, repo_artifacts / report_name)

    aggregate_timeline: List[Dict[str, Any]] = []
    for row in rows:
        run_dir = _resolve_run_dir(Path(str(row["run_dir"])))
        route_id = str(row.get("route_id") or "")
        for item in load_jsonl(run_dir / "artifacts" / "town01_route_health_state_timeline.jsonl"):
            aggregate_timeline.append(
                {
                    "run_dir": str(run_dir),
                    "route_id": route_id,
                    **item,
                }
            )
    write_jsonl(batch_artifacts / "town01_route_health_state_timeline.jsonl", aggregate_timeline)
    if publish_repo:
        write_jsonl(repo_artifacts / "town01_route_health_state_timeline.jsonl", aggregate_timeline)


def _analyze_batch(batch_root: Path, *, publish_repo: bool = False) -> List[Dict[str, Any]]:
    manifest_path = batch_root / "artifacts" / "town01_route_health_run_manifest.json"
    manifest = load_json(manifest_path)
    runs = list(manifest.get("runs") or [])
    if not runs:
        runs = _discover_batch_runs(batch_root)
        if runs:
            manifest["runs"] = runs
            _update_manifest(batch_root, manifest)
    rows: List[Dict[str, Any]] = []
    for item in runs:
        run_dir = _resolve_run_dir(Path(str(item.get("run_dir") or "")))
        if not run_dir.exists():
            continue
        raw_flags = item.get("flags") if isinstance(item.get("flags"), dict) else None
        finalized = finalize_town01_run(run_dir, flags=raw_flags)
        row = collect_run_row(run_dir)
        row["comparison_label"] = str(item.get("comparison_label") or row.get("comparison_label") or "")
        if _reconcile_manifest_item_from_analysis(item, requested_run_dir=run_dir, finalized=finalized, row=row):
            manifest["runs"] = runs
        row["summary_status"] = finalized.get("summary_status")
        rows.append(row)
    if manifest.get("runs") != runs:
        manifest["runs"] = runs
    if runs:
        _update_manifest(batch_root, manifest)
    _publish_outputs(batch_root, rows, publish_repo=publish_repo)
    return rows


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Town01 route health unified mainline entry")
    sub = parser.add_subparsers(dest="cmd", required=True)

    corpus_p = sub.add_parser("build-corpus", help="Build Town01 route corpus")
    corpus_p.add_argument("--output", type=Path, default=default_corpus_path(REPO_ROOT))
    corpus_p.add_argument("--report", type=Path, default=default_corpus_report_path(REPO_ROOT))
    corpus_p.add_argument("--carla-root", default="")
    corpus_p.add_argument("--carla-port", type=int, default=2000)
    corpus_p.add_argument("--start-carla", action="store_true")
    corpus_p.add_argument("--carla-launch-attempts", type=int, default=1)
    corpus_p.add_argument("--carla-world-ready-timeout-sec", type=float, default=180.0)
    corpus_p.add_argument("--carla-world-ready-poll-sec", type=float, default=2.0)
    corpus_p.add_argument("--carla-retry-delay-sec", type=float, default=2.0)
    corpus_p.add_argument("--carla-no-retry-failure-families", default="")
    corpus_p.add_argument("--carla-extra-args", default="")
    corpus_p.add_argument("--carla-disable-native-ros2-arg", action="store_true")
    corpus_p.add_argument("--carla-force-fresh-start", action="store_true")
    corpus_p.add_argument("--carla-launcher-auto-recovery", action=argparse.BooleanOptionalAction, default=False)
    corpus_p.add_argument("--carla-force-headless-env", action="store_true")
    corpus_p.add_argument("--carla-force-sdl-x11-no-xrandr", action="store_true")
    corpus_p.add_argument("--carla-display-override", default="")
    corpus_p.add_argument("--carla-xauthority-override", default="")
    corpus_p.add_argument(
        "--carla-ignore-memory-preflight",
        action="store_true",
        help="Technical-probe escape hatch: ignore the CARLA host memory preflight instead of blocking startup.",
    )

    run_p = sub.add_parser("run", help="Run Town01 mainline batch")
    run_p.add_argument("--config", type=Path, default=default_mainline_config_path(REPO_ROOT))
    run_p.add_argument("--corpus", type=Path, default=default_corpus_path(REPO_ROOT))
    run_p.add_argument(
        "--capability-profile",
        choices=sorted(CAPABILITY_PROFILE_OVERRIDES.keys()),
        default="",
        help="Apply a geometry-oriented Apollo capability profile on top of the canonical mainline config.",
    )
    run_p.add_argument("--route-id", default="")
    run_p.add_argument("--route-ids-file", default="")
    run_p.add_argument("--overrides-file", default="")
    run_p.add_argument("--recommended-subset", default="")
    run_p.add_argument("--sample-size", type=int, default=12)
    run_p.add_argument("--sample-seed", type=int, default=1)
    run_p.add_argument("--batch-root", default="")
    run_p.add_argument("--ticks", type=int, default=700)
    run_p.add_argument("--enable-lateral", action="store_true")
    run_p.add_argument("--enable-guard", action=argparse.BooleanOptionalAction, default=True)
    run_p.add_argument("--enable-lateral-debug", action="store_true")
    run_p.add_argument("--enable-recording", action="store_true")
    run_p.add_argument("--enable-strict-observation", action="store_true")
    run_p.add_argument("--enable-stage6-reference-line", action=argparse.BooleanOptionalAction, default=False)
    run_p.add_argument(
        "--clear-lane-follow-cache-on-new-command",
        action=argparse.BooleanOptionalAction,
        default=False,
        dest="stage6_clear_lane_follow_cache_on_new_command",
    )
    run_p.add_argument(
        "--stage6-reference-line-generation-guard",
        action=argparse.BooleanOptionalAction,
        default=False,
        dest="stage6_reference_line_generation_guard",
    )
    run_p.add_argument("--continue-on-failure", action="store_true")
    run_p.add_argument("--comparison-label", default="")
    run_p.add_argument("--alias", choices=sorted(ALIAS_PRESETS.keys()), default="")
    run_p.add_argument("--profiles", default="", help="Deprecated compatibility aliases list")
    run_p.add_argument("--apollo-root", default="")
    run_p.add_argument("--docker-container", default="")
    run_p.add_argument("--carla-root", default="")
    run_p.add_argument("--carla-port", type=int, default=2000)
    run_p.add_argument("--stop-carla-on-exit", action="store_true")
    run_p.add_argument("--carla-launch-attempts", type=int, default=1)
    run_p.add_argument("--carla-world-ready-timeout-sec", type=float, default=180.0)
    run_p.add_argument("--carla-world-ready-poll-sec", type=float, default=2.0)
    run_p.add_argument("--carla-retry-delay-sec", type=float, default=2.0)
    run_p.add_argument("--carla-no-retry-failure-families", default="")
    run_p.add_argument("--carla-extra-args", default="")
    run_p.add_argument("--carla-disable-native-ros2-arg", action="store_true")
    run_p.add_argument("--carla-force-fresh-start", action="store_true")
    run_p.add_argument("--carla-launcher-auto-recovery", action=argparse.BooleanOptionalAction, default=False)
    run_p.add_argument("--carla-force-headless-env", action="store_true")
    run_p.add_argument("--carla-force-sdl-x11-no-xrandr", action="store_true")
    run_p.add_argument("--carla-display-override", default="")
    run_p.add_argument("--carla-xauthority-override", default="")
    run_p.add_argument(
        "--carla-ignore-memory-preflight",
        action="store_true",
        help="Technical-probe escape hatch: ignore the CARLA host memory preflight instead of blocking startup.",
    )
    run_p.add_argument("--override", action="append", default=[])

    analyze_p = sub.add_parser("analyze", help="Finalize and analyze a batch")
    analyze_p.add_argument("--batch-root", required=True)

    compare_p = sub.add_parser("compare", help="Compare one or more analyzed batches")
    compare_p.add_argument("--batch-root", action="append", required=True)
    compare_p.add_argument("--comparison", type=Path, default=None)
    compare_p.add_argument("--report", type=Path, default=None)
    return parser


def _run_build_corpus(args: argparse.Namespace) -> None:
    report_path = Path(args.report).expanduser().resolve()
    _write_static_docs(report_path.parent)
    launcher = None
    try:
        if args.start_carla:
            launcher = _prestart_carla(args, REPO_ROOT / "runs" / "town01_route_health_corpus")
        corpus = _build_corpus(
            output_path=Path(args.output).expanduser().resolve(),
            report_path=report_path,
            host="127.0.0.1",
            port=int(args.carla_port),
        )
        print(
            "[town01-route-health] corpus built "
            f"routes={len(corpus.get('routes') or [])} output={Path(args.output).expanduser().resolve()}"
        )
    finally:
        if launcher is not None:
            launcher.stop()


def _run_mainline(args: argparse.Namespace) -> None:
    batch_root = Path(
        args.batch_root
        or (REPO_ROOT / "runs" / f"town01_route_health_mainline_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    ).expanduser().resolve()
    batch_root.mkdir(parents=True, exist_ok=True)
    _write_static_docs(batch_root / "artifacts")
    config_path = Path(args.config).expanduser().resolve()
    corpus_path = Path(args.corpus).expanduser().resolve()
    launcher = None
    try:
        launcher = _prestart_carla(args, batch_root)
        if not corpus_path.exists():
            _build_corpus(
                output_path=corpus_path,
                report_path=default_corpus_report_path(REPO_ROOT),
                host="127.0.0.1",
                port=int(args.carla_port),
            )
        corpus = load_route_corpus(corpus_path)
        recommended_subset = _resolve_recommended_subset(args)
        route_ids = _resolve_route_ids(corpus, args)
        aliases = _legacy_aliases(args)
        run_specs: List[Dict[str, Any]] = []
        if aliases:
            for alias in aliases:
                flags = _flags_for_run(args, alias=alias)
                label = str(args.comparison_label or alias).strip() or alias
                for route_id in route_ids:
                    run_specs.append({"comparison_label": label, "flags": flags, "alias": alias, "route_id": route_id})
        else:
            flags = _flags_for_run(args)
            label = str(args.comparison_label or args.capability_profile or "mainline").strip() or "mainline"
            for route_id in route_ids:
                run_specs.append({"comparison_label": label, "flags": flags, "alias": "", "route_id": route_id})

        base_overrides = _build_base_overrides(args)
        manifest: Dict[str, Any] = {
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "batch_root": str(batch_root),
            "config_path": str(config_path),
            "corpus_path": str(corpus_path),
            "capability_profile": str(args.capability_profile or ""),
            "sample_seed": int(args.sample_seed),
            "route_ids_file": str(args.route_ids_file or ""),
            "recommended_subset": recommended_subset,
            "route_ids": route_ids,
            "runs": [],
        }
        _update_manifest(batch_root, manifest)
        rows: List[Dict[str, Any]] = []
        for index, spec in enumerate(run_specs, start=1):
            route_id = str(spec["route_id"])
            comparison_label = str(spec["comparison_label"])
            run_dir = batch_root / f"{comparison_label}__{route_id}"
            run_overrides = _overrides_for_flags(
                spec["flags"],
                corpus_path=corpus_path,
                route_id=route_id,
                ticks=int(args.ticks),
                comparison_label=comparison_label,
                capability_profile=str(args.capability_profile or ""),
            )
            contract_preview = _preview_runtime_contract(
                config_path,
                base_overrides=base_overrides,
                flags=spec["flags"],
                corpus_path=corpus_path,
                route_id=route_id,
                ticks=int(args.ticks),
                comparison_label=comparison_label,
                capability_profile=str(args.capability_profile or ""),
                final_overrides=list(args.override or []),
            )
            runtime_contract = contract_preview["runtime_contract"]
            item = {
                "run_index": index,
                "alias": spec["alias"],
                "comparison_label": comparison_label,
                "route_id": route_id,
                "flags": dict(spec["flags"]),
                "run_dir": str(run_dir),
                "runtime_contract_status": runtime_contract.get("status"),
                "runtime_contract_requires_true_lateral": runtime_contract.get("requires_true_lateral"),
                "runtime_contract_blockers": list(runtime_contract.get("blockers") or []),
                "status": "running",
                "returncode": None,
            }
            manifest["runs"].append(item)
            _update_manifest(batch_root, manifest)
            if runtime_contract.get("status") == "misconfigured":
                item["status"] = "blocked_preflight"
                item["returncode"] = None
                _update_manifest(batch_root, manifest)
                blockers = ", ".join(str(part) for part in (runtime_contract.get("blockers") or [])) or "unknown"
                raise SystemExit(
                    "[town01-route-health] runtime contract mismatch before launch: "
                    f"capability_profile={args.capability_profile or '<none>'} route_id={route_id} blockers={blockers}"
                )
            rc = _invoke_run(
                run_dir=run_dir,
                route_id=route_id,
                corpus_path=corpus_path,
                config_path=config_path,
                flags=spec["flags"],
                comparison_label=comparison_label,
                ticks=int(args.ticks),
                base_overrides=base_overrides,
                capability_profile=str(args.capability_profile or ""),
                run_overrides=run_overrides,
                final_overrides=list(args.override or []),
            )
            item["effective_run_dir"] = str(_resolve_run_dir(run_dir))
            item["returncode"] = rc
            item["status"] = "completed" if rc == 0 else "failed"
            _update_manifest(batch_root, manifest)
            effective_run_dir = _resolve_run_dir(run_dir)
            if effective_run_dir.exists():
                try:
                    finalize_town01_run(effective_run_dir, flags=spec["flags"])
                    row = collect_run_row(effective_run_dir)
                    row["comparison_label"] = comparison_label
                    rows.append(row)
                except Exception as exc:
                    item["finalize_error"] = str(exc)
                    _update_manifest(batch_root, manifest)
                    if not args.continue_on_failure:
                        raise
            if rc != 0 and not args.continue_on_failure:
                raise SystemExit(rc)

        _publish_outputs(batch_root, rows, publish_repo=False)
        _update_manifest(batch_root, manifest)
        print(f"[town01-route-health] batch completed -> {batch_root}")
    finally:
        if launcher is not None and args.stop_carla_on_exit:
            launcher.stop()
            _cleanup_carla_processes()


def _run_analyze(args: argparse.Namespace) -> None:
    batch_root = Path(args.batch_root).expanduser().resolve()
    _write_static_docs(batch_root / "artifacts")
    rows = _analyze_batch(batch_root, publish_repo=False)
    print(f"[town01-route-health] analyzed runs={len(rows)}")


def _run_compare(args: argparse.Namespace) -> None:
    rows: List[Dict[str, Any]] = []
    batch_roots: List[Path] = []
    for item in args.batch_root:
        batch_root = Path(item).expanduser().resolve()
        batch_roots.append(batch_root)
        rows.extend(_analyze_batch(batch_root, publish_repo=False))
    default_output_root = batch_roots[0] / "artifacts" if len(batch_roots) == 1 else REPO_ROOT / "artifacts"
    _write_static_docs(default_output_root)
    comparison_path = (
        Path(args.comparison).expanduser().resolve()
        if args.comparison is not None
        else default_output_root / "town01_route_health_platform_comparison.csv"
    )
    report_path = (
        Path(args.report).expanduser().resolve()
        if args.report is not None
        else default_output_root / "town01_route_health_platform_mainline_report.md"
    )
    comparison_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    write_platform_comparison(rows, comparison_path)
    write_platform_report(rows, report_path)
    print(f"[town01-route-health] compare rows={len(rows)}")


def main(argv: Optional[List[str]] = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.cmd == "build-corpus":
        _run_build_corpus(args)
        return
    if args.cmd == "run":
        _run_mainline(args)
        return
    if args.cmd == "analyze":
        _run_analyze(args)
        return
    if args.cmd == "compare":
        _run_compare(args)
        return
    raise SystemExit(f"Unknown command: {args.cmd}")


if __name__ == "__main__":
    main()
