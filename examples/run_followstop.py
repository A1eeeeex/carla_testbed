#!/usr/bin/env python3
"""LEGACY demo entry: follow-stop harness.

Kept for compatibility, demos, and regression checks. Do not add new platform
logic here. New platform logic should go to:

- `carla_testbed/runner`
- `carla_testbed/scenarios`
- `carla_testbed/adapters`
- `carla_testbed/record`
- `carla_testbed/evaluation`

Mode-2 (Autoware container direct CARLA) follow-stop harness.
需提前准备 Town01 地图（点云+lanelet2），放在宿主机路径如 /home/ubuntu/autoware-contents/maps/Town01，并通过 AUTOWARE_MAP_PATH 挂载进 Autoware 容器（默认已指向该路径）。
一条命令跑通：
python examples/run_followstop.py --config configs/io/examples/followstop_autoware.yaml --start-carla --carla-root <CARLA_ROOT> --ticks 500
"""
from __future__ import annotations

import argparse
import atexit
import csv
import json
import os
import math
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import carla
import yaml

from algo.registry import get_adapter
from carla_testbed.config import HarnessConfig
from carla_testbed.config.rig_loader import apply_overrides, load_rig_file, load_rig_preset, rig_to_specs
from carla_testbed.control import LegacyControllerConfig
from carla_testbed.evaluation import (
    applied_control_health_from_timeseries,
    bridge_target_speed_failures_from_check,
    bridge_target_speed_health_from_log,
    control_health_failures_from_check,
    safety_failures_from_summary,
)
from carla_testbed.analysis.autoware_followstop import (
    analyze_followstop_run,
    write_followstop_report,
)
from carla_testbed.analysis.autoware_control_diagnostics import (
    analyze_autoware_control_run,
    write_autoware_control_diagnostics,
)
from carla_testbed.record import RecordManager, RecordOptions
from carla_testbed.record.manifest_metadata import online_claim_manifest_updates
from carla_testbed.record.monitor import SignalMonitor
from carla_testbed.record.rviz.launcher import RvizLauncher
from carla_testbed.runner import TestHarness
from carla_testbed.scenarios import (
    ApolloSemanticSuiteConfig,
    ApolloSemanticSuiteScenario,
    CalibrationOnlyConfig,
    CalibrationOnlyScenario,
    FollowStopConfig,
    FollowStopScenario,
    Town01RouteHealthConfig,
    Town01RouteHealthScenario,
)
from carla_testbed.scenarios.apollo_semantic_suite import SemanticLeadProfile
from carla_testbed.sim import configure_synchronous_mode, connect_world_with_retry, restore_settings
from carla_testbed.utils.env import resolve_carla_root, resolve_repo_root
from carla_testbed.utils.run_naming import build_run_name, next_available_run_dir, update_latest_pointer
from tbio.carla.launcher import CarlaLauncher
from tbio.contract.generate_artifacts import generate_all
from tbio.ros2.goal_engage import ComposeRos2Runner, LocalRos2Runner, send_goal_and_engage
from tbio.ros2.observability import (
    assess_probe_results,
    default_probe_topics,
    infer_topic_types,
    start_control_logger,
    start_topic_probe,
    stop_process,
)
from tbio.scripts.healthcheck_ros2 import healthcheck


TESTBED_ROOT = resolve_repo_root()
try:
    DEFAULT_CARLA_ROOT = resolve_carla_root(os.environ.get("CARLA_ROOT"), strict=False)
except Exception:
    # Keep CLI importable even when local CARLA path is not configured yet.
    DEFAULT_CARLA_ROOT = Path("/path/to/CARLA_0.9.16")
_ACTIVE_CLEANUPS: Dict[str, threading.Thread] = {}
_ROS2_REEXEC_FLAG = "TESTBED_ROS2_ENV_BOOTSTRAPPED"
_ROS_HUMBLE_PREFIX = Path("/opt/ros/humble")


def _config_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def _followstop_carla_env_overrides(runtime_carla_cfg: Dict[str, Any]) -> Dict[str, str]:
    env_overrides: Dict[str, str] = {}
    if _config_bool(runtime_carla_cfg.get("force_headless_env"), default=False):
        # Match the route-health prewarm discipline: keep offscreen CARLA
        # independent of stale desktop/X11 state on overnight validation hosts.
        env_overrides.update(
            {
                "DISPLAY": "",
                "WAYLAND_DISPLAY": "",
                "XAUTHORITY": "",
                "SDL_AUDIODRIVER": "dummy",
                "MALLOC_ARENA_MAX": "2",
            }
        )
    display_override = str(runtime_carla_cfg.get("display_override") or "").strip()
    if display_override:
        env_overrides["DISPLAY"] = display_override
    xauthority_override = str(runtime_carla_cfg.get("xauthority_override") or "").strip()
    if xauthority_override:
        env_overrides["XAUTHORITY"] = xauthority_override
    if _config_bool(runtime_carla_cfg.get("force_sdl_x11_no_xrandr"), default=False):
        env_overrides["SDL_VIDEODRIVER"] = "x11"
        env_overrides["SDL_VIDEO_X11_REQUIRE_XRANDR"] = "0"
    env_overrides.setdefault("MALLOC_ARENA_MAX", "2")
    return env_overrides


def _prepend_env_paths(env: Dict[str, str], key: str, values: List[Path | str]) -> None:
    current = [item for item in str(env.get(key, "")).split(":") if item]
    for value in reversed([str(item) for item in values if item]):
        if value not in current:
            current.insert(0, value)
    if current:
        env[key] = ":".join(current)


def _maybe_reexec_with_ros2_runtime(args: argparse.Namespace) -> None:
    if not bool(getattr(args, "enable_ros2_gt", False)):
        return
    if os.environ.get(_ROS2_REEXEC_FLAG) == "1":
        return
    if not _ROS_HUMBLE_PREFIX.exists():
        return

    env = os.environ.copy()
    env[_ROS2_REEXEC_FLAG] = "1"
    pyver = f"python{sys.version_info.major}.{sys.version_info.minor}"
    _prepend_env_paths(env, "AMENT_PREFIX_PATH", [_ROS_HUMBLE_PREFIX])
    _prepend_env_paths(env, "COLCON_PREFIX_PATH", [_ROS_HUMBLE_PREFIX])
    _prepend_env_paths(env, "CMAKE_PREFIX_PATH", [_ROS_HUMBLE_PREFIX])
    _prepend_env_paths(
        env,
        "LD_LIBRARY_PATH",
        [
            _ROS_HUMBLE_PREFIX / "lib",
            _ROS_HUMBLE_PREFIX / "local" / "lib",
            _ROS_HUMBLE_PREFIX / "lib" / "x86_64-linux-gnu",
        ],
    )
    _prepend_env_paths(
        env,
        "PYTHONPATH",
        [
            _ROS_HUMBLE_PREFIX / "local" / "lib" / pyver / "dist-packages",
            _ROS_HUMBLE_PREFIX / "lib" / pyver / "site-packages",
        ],
    )
    env.setdefault("RMW_IMPLEMENTATION", "rmw_fastrtps_cpp")
    print("[ros2-env] re-exec with /opt/ros/humble runtime for in-process GT publisher")
    os.execvpe(sys.executable, [sys.executable, "-m", "examples.run_followstop", *sys.argv[1:]], env)


def _cleanup_stale_apollo_runtime_processes() -> None:
    patterns = [
        "tools/apollo10_cyber_bridge/bridge.py",
        "algo/nodes/control/carla_control_bridge/ros2_autoware_to_carla.py",
        "modules/routing/dag/routing.dag",
        "modules/prediction/dag/prediction.dag",
        "modules/external_command/process_component/dag/external_command_process.dag",
        "modules/planning/planning_component/dag/planning.dag",
        "modules/control/control_component/dag/control.dag",
    ]
    for pattern in patterns:
        try:
            subprocess.run(
                ["bash", "-lc", f"pkill -f {pattern!r} >/dev/null 2>&1 || true"],
                cwd=str(TESTBED_ROOT),
                check=False,
            )
        except Exception:
            pass


def _is_port_open(host: str, port: int) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(1.0)
            return sock.connect_ex((host, port)) == 0
    except PermissionError:
        print("[WARN] socket permission denied; skipping port probe")
        return True


def _wait_for_carla(host: str, port: int, timeout: float = 30.0) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        if _is_port_open(host, port):
            try:
                client = carla.Client(host, port)
                client.set_timeout(1.0)
                client.get_server_version()
                return True
            except Exception:
                pass
        time.sleep(1.0)
    return False


def _dedup(seq):
    seen = set()
    out = []
    for x in seq:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _load_json_if_exists(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return None
        return out
    except Exception:
        return None


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except Exception:
        return None


def _safe_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "on"}:
        return True
    if text in {"false", "0", "no", "n", "off"}:
        return False
    return None


def _safe_int_list(value: Any) -> List[int]:
    if value is None or value == "":
        return []
    items = value if isinstance(value, (list, tuple, set)) else [value]
    out: List[int] = []
    seen = set()
    for item in items:
        parsed = _safe_int(item)
        if parsed is None or parsed in seen:
            continue
        seen.add(parsed)
        out.append(parsed)
    return out


def _compute_low_speed_creep_metrics(
    debug_timeseries_path: Path,
    *,
    start_after_sec: float,
    speed_below_mps: float,
    front_gap_above_m: float,
    require_routing_established: bool,
    ignore_when_terminal_stop_hold_active: bool,
    require_reached_speed_mps: float = 0.0,
) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {
        "available": False,
        "path": str(debug_timeseries_path),
        "start_after_sec": float(start_after_sec),
        "speed_below_mps": float(speed_below_mps),
        "front_gap_above_m": float(front_gap_above_m),
        "require_routing_established": bool(require_routing_established),
        "ignore_when_terminal_stop_hold_active": bool(ignore_when_terminal_stop_hold_active),
        "require_reached_speed_mps": float(require_reached_speed_mps),
        "sample_count": 0,
        "matched_sample_count": 0,
        "longest_streak_frames": 0,
        "longest_streak_duration_sec": 0.0,
        "first_match_ts_sec": None,
        "longest_streak_start_ts_sec": None,
        "longest_streak_end_ts_sec": None,
    }
    if not debug_timeseries_path.exists():
        return metrics
    try:
        with debug_timeseries_path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
    except Exception:
        return metrics
    if not rows:
        metrics["available"] = True
        return metrics

    metrics["available"] = True
    metrics["sample_count"] = len(rows)
    first_ts = _safe_float(rows[0].get("ts_sec"))
    if first_ts is None:
        return metrics

    current_frames = 0
    current_start_ts: Optional[float] = None
    prev_ts: Optional[float] = None
    best_end_ts: Optional[float] = None
    best_duration = 0.0
    reached_speed_gate = float(require_reached_speed_mps) <= 0.0

    def _close_streak(end_ts: Optional[float]) -> None:
        nonlocal current_frames, current_start_ts, best_duration, best_end_ts
        if current_frames <= 0 or current_start_ts is None or end_ts is None:
            current_frames = 0
            current_start_ts = None
            return
        duration = max(0.0, float(end_ts) - float(current_start_ts))
        if current_frames > int(metrics["longest_streak_frames"]) or (
            current_frames == int(metrics["longest_streak_frames"]) and duration > best_duration
        ):
            metrics["longest_streak_frames"] = int(current_frames)
            metrics["longest_streak_start_ts_sec"] = float(current_start_ts)
            metrics["longest_streak_end_ts_sec"] = float(end_ts)
            best_duration = float(duration)
            best_end_ts = float(end_ts)
            metrics["longest_streak_duration_sec"] = float(duration)
        current_frames = 0
        current_start_ts = None

    for row in rows:
        ts_sec = _safe_float(row.get("ts_sec"))
        speed_mps = _safe_float(row.get("speed_mps"))
        front_gap_lon_m = _safe_float(row.get("front_obstacle_gap_lon_m"))
        routing_established = _safe_bool(row.get("routing_established"))
        terminal_stop_hold_active = _safe_bool(row.get("terminal_stop_hold_active"))
        prev_ts = ts_sec if ts_sec is not None else prev_ts
        if ts_sec is None or speed_mps is None or front_gap_lon_m is None:
            _close_streak(prev_ts)
            continue
        if (
            not reached_speed_gate
            and float(speed_mps) >= float(require_reached_speed_mps)
        ):
            reached_speed_gate = True
        cond = ts_sec >= (first_ts + float(start_after_sec))
        cond = cond and reached_speed_gate
        cond = cond and speed_mps < float(speed_below_mps)
        cond = cond and front_gap_lon_m > float(front_gap_above_m)
        if require_routing_established:
            cond = cond and bool(routing_established)
        if ignore_when_terminal_stop_hold_active:
            cond = cond and not bool(terminal_stop_hold_active)
        if cond:
            metrics["matched_sample_count"] = int(metrics["matched_sample_count"]) + 1
            if metrics["first_match_ts_sec"] is None:
                metrics["first_match_ts_sec"] = float(ts_sec)
            if current_frames == 0:
                current_start_ts = float(ts_sec)
            current_frames += 1
        else:
            _close_streak(ts_sec)
        prev_ts = ts_sec
    _close_streak(prev_ts)
    if best_end_ts is not None:
        metrics["longest_streak_end_ts_sec"] = best_end_ts
    return metrics


def _compute_basic_lateral_metrics(control_decode_path: Path) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {
        "available": False,
        "path": str(control_decode_path),
        "sample_count": 0,
        "raw_steer_nonzero_count": 0,
        "raw_steer_nonzero_ratio": 0.0,
        "raw_steer_saturated_count": 0,
        "raw_steer_saturated_ratio": 0.0,
        "longest_continuous_saturation_frames": 0,
        "longest_continuous_saturation_sec": 0.0,
        "commanded_steer_nonzero_count": 0,
        "commanded_steer_nonzero_ratio": 0.0,
        "force_zero_steer_applied_count": 0,
        "guard_trigger_count": 0,
        "guard_trigger_reason_top1": None,
    }
    if not control_decode_path.exists():
        return metrics
    try:
        lines = control_decode_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return metrics
    rows: List[Dict[str, Any]] = []
    for line in lines:
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    if not rows:
        return metrics
    metrics["available"] = True
    metrics["sample_count"] = len(rows)
    current_sat_frames = 0
    current_sat_start_ts: Optional[float] = None
    best_sat_duration = 0.0
    guard_reasons: Dict[str, int] = {}

    def _close_sat(ts_sec: Optional[float]) -> None:
        nonlocal current_sat_frames, current_sat_start_ts, best_sat_duration
        if current_sat_frames <= 0 or current_sat_start_ts is None or ts_sec is None:
            current_sat_frames = 0
            current_sat_start_ts = None
            return
        duration = max(0.0, float(ts_sec) - float(current_sat_start_ts))
        if current_sat_frames > int(metrics["longest_continuous_saturation_frames"]) or (
            current_sat_frames == int(metrics["longest_continuous_saturation_frames"]) and duration > best_sat_duration
        ):
            metrics["longest_continuous_saturation_frames"] = int(current_sat_frames)
            metrics["longest_continuous_saturation_sec"] = float(duration)
            best_sat_duration = float(duration)
        current_sat_frames = 0
        current_sat_start_ts = None

    prev_ts: Optional[float] = None
    for row in rows:
        ts_sec = _safe_float(row.get("ts_sec"))
        raw_steer = _safe_float(row.get("raw_steer"))
        commanded_steer = _safe_float(row.get("commanded_steer"))
        if raw_steer is not None and abs(raw_steer) > 1e-6:
            metrics["raw_steer_nonzero_count"] = int(metrics["raw_steer_nonzero_count"]) + 1
        if raw_steer is not None and abs(raw_steer) >= 0.99:
            metrics["raw_steer_saturated_count"] = int(metrics["raw_steer_saturated_count"]) + 1
            if current_sat_frames == 0:
                current_sat_start_ts = ts_sec if ts_sec is not None else prev_ts
            current_sat_frames += 1
        else:
            _close_sat(ts_sec if ts_sec is not None else prev_ts)
        if commanded_steer is not None and abs(commanded_steer) > 1e-6:
            metrics["commanded_steer_nonzero_count"] = int(metrics["commanded_steer_nonzero_count"]) + 1
        if bool(row.get("force_zero_steer_applied")):
            metrics["force_zero_steer_applied_count"] = int(metrics["force_zero_steer_applied_count"]) + 1
        guard_reasons_in_row = []
        if bool(row.get("low_speed_steer_guard_applied")):
            guard_reasons_in_row.append("low_speed_steer_guard")
        if bool(row.get("low_speed_sustained_guard_applied")):
            guard_reasons_in_row.append("low_speed_sustained_guard")
        if bool(row.get("sustained_saturation_guard_applied")):
            guard_reasons_in_row.append("sustained_saturation_guard")
        if guard_reasons_in_row:
            metrics["guard_trigger_count"] = int(metrics["guard_trigger_count"]) + 1
            for reason in guard_reasons_in_row:
                guard_reasons[reason] = int(guard_reasons.get(reason, 0)) + 1
        prev_ts = ts_sec if ts_sec is not None else prev_ts
    _close_sat(prev_ts)
    sample_count = int(metrics["sample_count"]) or 1
    metrics["raw_steer_nonzero_ratio"] = float(metrics["raw_steer_nonzero_count"]) / float(sample_count)
    metrics["raw_steer_saturated_ratio"] = float(metrics["raw_steer_saturated_count"]) / float(sample_count)
    metrics["commanded_steer_nonzero_ratio"] = float(metrics["commanded_steer_nonzero_count"]) / float(sample_count)
    if guard_reasons:
        metrics["guard_trigger_reason_top1"] = max(guard_reasons.items(), key=lambda item: item[1])[0]
    return metrics


def _compute_route_health_metrics(
    scenario_meta: Dict[str, Any],
    ego: Optional[carla.Actor],
    *,
    routing_success_count: Optional[int],
) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {
        "available": False,
        "route_established": bool((routing_success_count or 0) > 0),
        "route_length_target_m": _safe_float(scenario_meta.get("route_length_m")),
        "start_goal_distance_m": None,
        "final_goal_distance_m": None,
        "route_distance_achieved_m": None,
        "route_completion_percentage": None,
        "spawn_lane": scenario_meta.get("spawn_lane"),
        "goal_lane": scenario_meta.get("goal_lane"),
        "goal_selection_attempt_count": len((scenario_meta.get("goal_selection_attempts") or [])),
    }
    spawn = scenario_meta.get("spawn") or {}
    goal = scenario_meta.get("goal") or {}
    sx = _safe_float(spawn.get("x"))
    sy = _safe_float(spawn.get("y"))
    gx = _safe_float(goal.get("x"))
    gy = _safe_float(goal.get("y"))
    if sx is None or sy is None or gx is None or gy is None:
        return metrics
    start_goal_distance = math.sqrt(((gx - sx) ** 2) + ((gy - sy) ** 2))
    metrics["start_goal_distance_m"] = float(start_goal_distance)
    try:
        if ego is not None:
            ego_loc = ego.get_location()
            final_goal_distance = math.sqrt(((gx - float(ego_loc.x)) ** 2) + ((gy - float(ego_loc.y)) ** 2))
            metrics["final_goal_distance_m"] = float(final_goal_distance)
            if start_goal_distance > 1e-3:
                completion = max(0.0, min(1.0, 1.0 - (final_goal_distance / start_goal_distance)))
                metrics["route_completion_percentage"] = float(completion)
                metrics["route_distance_achieved_m"] = float(start_goal_distance - final_goal_distance)
            metrics["available"] = True
    except Exception:
        pass
    return metrics


def _quaternion_from_yaw(yaw_deg: float):
    yaw_rad = math.radians(yaw_deg)
    half = yaw_rad / 2.0
    return 0.0, 0.0, math.sin(half), math.cos(half)


def _autoware_pose_from_carla_transform(
    tr: carla.Transform,
    *,
    ahead_m: float = 0.0,
    invert_y: bool = True,
    invert_yaw: bool = True,
) -> Dict[str, float]:
    """Convert a CARLA actor transform into the ROS map frame used by GT bridge."""
    yaw_carla = float(tr.rotation.yaw)
    loc = tr.location
    target_x = float(loc.x) + (float(ahead_m) * math.cos(math.radians(yaw_carla)))
    target_y = float(loc.y) + (float(ahead_m) * math.sin(math.radians(yaw_carla)))
    yaw_ros = -yaw_carla if invert_yaw else yaw_carla
    qx, qy, qz, qw = _quaternion_from_yaw(yaw_ros)
    return {
        "x": target_x,
        "y": -target_y if invert_y else target_y,
        "z": float(loc.z),
        "yaw_deg": float(yaw_ros),
        "qx": qx,
        "qy": qy,
        "qz": qz,
        "qw": qw,
        "source_carla_x": float(target_x),
        "source_carla_y": float(target_y),
        "source_carla_yaw_deg": float(yaw_carla),
    }


def _transform_xy_with_cfg(x: float, y: float, z: float, tf_cfg: Dict[str, Any]) -> tuple[float, float, float]:
    yaw_rad = math.radians(float(tf_cfg.get("yaw_deg", 0.0) or 0.0))
    c = math.cos(yaw_rad)
    s = math.sin(yaw_rad)
    tx = float(tf_cfg.get("tx", 0.0) or 0.0)
    ty = float(tf_cfg.get("ty", 0.0) or 0.0)
    tz = float(tf_cfg.get("tz", 0.0) or 0.0)
    return (
        (c * x) - (s * y) + tx,
        (s * x) + (c * y) + ty,
        z + tz,
    )


def _stable_actor_transform(
    actor: carla.Actor,
    *,
    max_attempts: int = 12,
    fallback_transform: Optional[carla.Transform] = None,
) -> carla.Transform:
    """Wait briefly for spawned actor pose to settle (avoid transient 0,0,0)."""
    best = actor.get_transform()
    world = None
    sync_mode = False
    try:
        world = actor.get_world()
        sync_mode = bool(world.get_settings().synchronous_mode)
    except Exception:
        world = None
        sync_mode = False
    for _ in range(max(1, int(max_attempts))):
        tr = actor.get_transform()
        loc = tr.location
        if (abs(float(loc.x)) + abs(float(loc.y))) > 1e-3:
            return tr
        best = tr
        try:
            if sync_mode and world is not None:
                world.tick()
            else:
                time.sleep(0.03)
        except Exception:
            time.sleep(0.03)
    if fallback_transform is not None:
        loc = fallback_transform.location
        if (abs(float(loc.x)) + abs(float(loc.y))) > 1e-3:
            return fallback_transform
    return best


def _wait_for_external_actor_visibility(
    client: carla.Client,
    world: carla.World,
    *,
    expected: Dict[str, int],
    max_ticks: int,
    tick_timeout_s: float = 5.0,
    sleep_s: float = 0.0,
) -> Dict[str, Any]:
    """Tick CARLA until spawned actors are visible to a fresh client world.

    CARLA 0.9.x can return actor handles from spawn_actor before a separate
    client can list them. External stack bridges run in their own process, so
    this gate proves the actor registry is committed before the bridge starts.
    """

    expected = {str(k): int(v) for k, v in expected.items() if int(v or 0) > 0}
    report: Dict[str, Any] = {
        "visible": False,
        "attempts": 0,
        "frames": [],
        "expected": dict(expected),
        "seen": {},
        "actor_count": None,
        "vehicle_count": None,
        "last_error": None,
    }
    if not expected:
        report["visible"] = True
        return report

    max_ticks = max(0, int(max_ticks))
    for attempt in range(max_ticks + 1):
        report["attempts"] = attempt + 1
        try:
            fresh_world = client.get_world()
            snapshot = fresh_world.get_snapshot()
            frame = int(getattr(snapshot, "frame", 0) or 0)
            actors = fresh_world.get_actors()
            vehicles = list(actors.filter("vehicle.*"))
            seen: Dict[str, Optional[int]] = {}
            for label, actor_id in expected.items():
                found = None
                for actor in vehicles:
                    role_name = str(actor.attributes.get("role_name", "") or "")
                    ros_name = str(actor.attributes.get("ros_name", "") or "")
                    if int(getattr(actor, "id", 0) or 0) == actor_id or role_name == label or ros_name == label:
                        found = int(getattr(actor, "id", 0) or 0)
                        break
                seen[label] = found
            report.update(
                {
                    "frames": [*report.get("frames", []), frame],
                    "seen": seen,
                    "actor_count": len(actors),
                    "vehicle_count": len(vehicles),
                }
            )
            if all(value is not None for value in seen.values()):
                report["visible"] = True
                return report
        except Exception as exc:
            report["last_error"] = repr(exc)

        if attempt >= max_ticks:
            break

        try:
            # Use a fresh client world first; this matches what external
            # bridges observe better than a possibly stale pre-load handle.
            client.get_world().tick(float(tick_timeout_s))
        except Exception as exc:
            report["last_error"] = repr(exc)
            try:
                world.tick(float(tick_timeout_s))
            except Exception as fallback_exc:
                report["last_error"] = f"{repr(exc)}; fallback={repr(fallback_exc)}"
        if sleep_s > 0.0:
            time.sleep(float(sleep_s))

    return report


def _write_apollo_scenario_goal(
    effective_cfg: Dict[str, Any],
    out_run_dir: Path,
    ego: carla.Actor,
    front: Optional[carla.Actor],
    args: argparse.Namespace,
    scenario: Optional[Any] = None,
) -> Optional[Path]:
    stack = ((effective_cfg.get("algo", {}) or {}).get("stack") or "").strip().lower()
    if stack != "apollo":
        return None

    apollo_cfg = (effective_cfg.get("algo", {}) or {}).get("apollo", {}) or {}
    routing_cfg = apollo_cfg.get("routing", {}) or {}
    tf_cfg = apollo_cfg.get("carla_to_apollo", {}) or {}
    artifacts_dir = out_run_dir / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    out_path = artifacts_dir / "scenario_goal.json"

    world = None
    try:
        world = ego.get_world()
    except Exception:
        world = None

    def _tf_from_meta(payload: Any) -> Optional[carla.Transform]:
        if not isinstance(payload, dict):
            return None
        if "x" not in payload or "y" not in payload:
            return None
        return carla.Transform(
            carla.Location(
                x=float(payload.get("x", 0.0)),
                y=float(payload.get("y", 0.0)),
                z=float(payload.get("z", 0.0)),
            ),
            carla.Rotation(yaw=float(payload.get("yaw_deg", 0.0))),
        )

    def _apollo_map_xyz_from_carla_raw(x: float, y: float, z: float) -> tuple[float, float, float]:
        yy = -float(y) if bool(getattr(args, "ros_invert_tf", False)) else float(y)
        return _transform_xy_with_cfg(float(x), yy, float(z), tf_cfg)

    scenario_meta = scenario.metadata() if scenario is not None and hasattr(scenario, "metadata") else {}
    if not isinstance(scenario_meta, dict):
        scenario_meta = {}

    ego_fallback = _tf_from_meta(scenario_meta.get("spawn"))
    front_fallback = _tf_from_meta(scenario_meta.get("front_spawn"))
    if world is not None:
        try:
            spawn_points = world.get_map().get_spawn_points()
            ego_idx = int(getattr(args, "ego_idx", -1))
            if ego_fallback is None and 0 <= ego_idx < len(spawn_points):
                ego_fallback = spawn_points[ego_idx]
            if front is not None:
                front_idx = int(getattr(args, "front_idx", -1))
                if front_fallback is None and 0 <= front_idx < len(spawn_points):
                    front_fallback = spawn_points[front_idx]
        except Exception:
            pass

    try:
        ego_live_tr = ego.get_transform()
    except Exception:
        ego_live_tr = _stable_actor_transform(ego, max_attempts=1, fallback_transform=ego_fallback)
    ego_tr = _stable_actor_transform(ego, fallback_transform=ego_fallback)
    ego_used_fallback = bool(
        ego_fallback is not None
        and (abs(float(ego_live_tr.location.x)) + abs(float(ego_live_tr.location.y))) <= 1e-3
        and (abs(float(ego_tr.location.x)) + abs(float(ego_tr.location.y))) > 1e-3
    )
    ego_loc = ego_tr.location
    front_gap: Optional[Dict[str, float]] = None
    front_used_fallback = False
    if front is not None:
        try:
            front_live_tr = front.get_transform()
            front_tr = _stable_actor_transform(front, fallback_transform=front_fallback)
            front_used_fallback = bool(
                front_fallback is not None
                and (abs(float(front_live_tr.location.x)) + abs(float(front_live_tr.location.y))) <= 1e-3
                and (abs(float(front_tr.location.x)) + abs(float(front_tr.location.y))) > 1e-3
            )
            dx = float(front_tr.location.x - ego_loc.x)
            dy = float(front_tr.location.y - ego_loc.y)
            dz = float(front_tr.location.z - ego_loc.z)
            yaw_rad = math.radians(float(ego_tr.rotation.yaw))
            heading_x = math.cos(yaw_rad)
            heading_y = math.sin(yaw_rad)
            front_gap = {
                "distance_m": float(math.sqrt((dx * dx) + (dy * dy) + (dz * dz))),
                "lon_m": float((dx * heading_x) + (dy * heading_y)),
                "lat_m": float((-dx * heading_y) + (dy * heading_x)),
                "dx_m": dx,
                "dy_m": dy,
                "dz_m": dz,
            }
        except Exception:
            front_gap = None
    goal_source = "ego_heading_ahead"
    goal_raw: Dict[str, float]
    goal_raw_carla: Optional[Dict[str, float]] = None
    payload: Dict[str, Any]
    scenario_driver = str(
        (((effective_cfg.get("scenario", {}) or {}).get("driver")) or scenario_meta.get("scenario_driver") or "")
    ).strip().lower()

    def _route_health_metadata_payload() -> Dict[str, Any]:
        return {
            "route_length_m": scenario_meta.get("route_length_m"),
            "route_length_m_role": scenario_meta.get("route_length_m_role"),
            "route_length_m_claim_grade": scenario_meta.get("route_length_m_claim_grade"),
            "claim_route_length_m": scenario_meta.get("claim_route_length_m"),
            "claim_route_length_source": scenario_meta.get("claim_route_length_source"),
            "route_trace_length_m": scenario_meta.get("route_trace_length_m"),
            "route_trace_length_source": scenario_meta.get("route_trace_length_source"),
            "route_trace_point_count": scenario_meta.get("route_trace_point_count"),
            "remain_length_after_goal_m": scenario_meta.get("remain_length_after_goal_m"),
            "spawn_lane": scenario_meta.get("spawn_lane"),
            "goal_lane": scenario_meta.get("goal_lane"),
        }

    scenario_goal_raw = scenario_meta.get("scenario_goal_raw_carla") if isinstance(scenario_meta, dict) else None
    if isinstance(scenario_goal_raw, dict) and "x" in scenario_goal_raw and "y" in scenario_goal_raw:
        goal_raw_carla = {
            "x": float(scenario_goal_raw["x"]),
            "y": float(scenario_goal_raw["y"]),
            "z": float(scenario_goal_raw.get("z", ego_loc.z)),
        }
        gx, gy, gz = _apollo_map_xyz_from_carla_raw(
            goal_raw_carla["x"], goal_raw_carla["y"], goal_raw_carla["z"]
        )
        goal_source = "scenario_metadata_apollo_map_xy"
        goal_raw = {"x": float(gx), "y": float(gy), "z": float(gz)}
        payload = {
            "frame": "apollo_map",
            "source": goal_source,
            "source_raw_frame": "carla_raw",
            "scenario_driver": scenario_driver,
            "goal": goal_raw,
            "goal_raw_carla": dict(goal_raw_carla),
            "route_health_metadata": _route_health_metadata_payload(),
        }
    elif args.scenario_goal_x is not None and args.scenario_goal_y is not None:
        goal_source = "cli_xy"
        goal_raw = {
            "x": float(args.scenario_goal_x),
            "y": float(args.scenario_goal_y),
            "z": float(args.scenario_goal_z if args.scenario_goal_z is not None else ego_loc.z),
        }
        payload = {
            "frame": "carla_raw",
            "source": goal_source,
            "goal": goal_raw,
        }
    else:
        cfg_goal_xy = routing_cfg.get("scenario_goal_xy")
        if isinstance(cfg_goal_xy, dict) and "x" in cfg_goal_xy and "y" in cfg_goal_xy:
            goal_source = "config_xy"
            goal_raw = {
                "x": float(cfg_goal_xy["x"]),
                "y": float(cfg_goal_xy["y"]),
                "z": float(cfg_goal_xy.get("z", ego_loc.z)),
            }
            payload = {
                "frame": "carla_raw",
                "source": goal_source,
                "goal": goal_raw,
            }
        else:
            ahead_requested = args.scenario_goal_ahead_m
            if ahead_requested is None:
                ahead_requested = float(routing_cfg.get("scenario_goal_ahead_m", 300.0) or 300.0)
            ahead_m = float(ahead_requested)
            front_margin_m = float(routing_cfg.get("scenario_goal_min_front_margin_m", 40.0) or 40.0)
            force_beyond_front = bool(routing_cfg.get("scenario_goal_force_beyond_front", True))
            front_gap_rule_applied = False
            if (
                force_beyond_front
                and front_gap is not None
                and float(front_gap.get("lon_m", -1.0)) > 0.0
                and abs(float(front_gap.get("lat_m", 0.0))) <= 6.0
            ):
                ahead_m = max(ahead_m, float(front_gap["lon_m"]) + front_margin_m)
                front_gap_rule_applied = True
                goal_source = "ego_heading_ahead_front_gap_guarded"
            yaw_rad = math.radians(float(ego_tr.rotation.yaw))
            goal_raw = {
                "x": float(ego_loc.x + float(ahead_m) * math.cos(yaw_rad)),
                "y": float(ego_loc.y + float(ahead_m) * math.sin(yaw_rad)),
                "z": float(ego_loc.z),
            }
            payload = {
                "frame": "relative",
                "source": goal_source,
                "relative_to": "ego_heading",
                "goal_ahead_m_requested": float(ahead_requested),
                "goal_ahead_m": float(ahead_m),
                "front_gap_rule_applied": front_gap_rule_applied,
                "front_gap_min_margin_m": front_margin_m,
                "goal": goal_raw,
            }

    start_x, start_y, start_z = _apollo_map_xyz_from_carla_raw(
        float(ego_loc.x), float(ego_loc.y), float(ego_loc.z)
    )
    payload.update(
        {
            "generated_at_unix_sec": time.time(),
            "requested_goal_mode": str(routing_cfg.get("goal_mode", "scenario_xy") or "scenario_xy"),
            "goal_raw_carla": goal_raw_carla or goal_raw,
            "front_gap": front_gap,
            "fallback_spawn_used": {
                "ego": ego_used_fallback,
                "front": front_used_fallback,
            },
            "start_at_write_time": {
                "x": start_x,
                "y": start_y,
                "z": start_z,
            },
        }
    )
    out_path.write_text(json.dumps(payload, indent=2))
    return out_path


def _normalize_ros_namespace(namespace: str) -> str:
    ns = (namespace or "/carla").strip()
    if not ns.startswith("/"):
        ns = "/" + ns
    ns = ns.rstrip("/")
    return ns or "/carla"


def _run_cleanup_with_timeout(label: str, fn, timeout_s: float = 8.0) -> None:
    prev = _ACTIVE_CLEANUPS.get(label)
    if prev is not None and prev.is_alive():
        print(
            f"[WARN] cleanup '{label}' still has a timed-out worker from a previous run; "
            "skip spawning another cleanup thread"
        )
        return
    err_holder: Dict[str, Exception] = {}

    def _worker():
        try:
            fn()
        except Exception as exc:
            err_holder["exc"] = exc

    t = threading.Thread(target=_worker, name=f"cleanup_{label}", daemon=True)
    _ACTIVE_CLEANUPS[label] = t
    t.start()
    t.join(timeout=timeout_s)
    if t.is_alive():
        print(f"[WARN] cleanup '{label}' timed out after {timeout_s:.1f}s, continuing")
        return
    if _ACTIVE_CLEANUPS.get(label) is t:
        _ACTIVE_CLEANUPS.pop(label, None)
    exc = err_holder.get("exc")
    if exc is not None:
        print(f"[WARN] cleanup '{label}' failed: {exc}")


def _start_carla_tick_pump(
    world,
    *,
    label: str,
    tick_timeout_s: float = 5.0,
    sleep_s: float = 0.0,
) -> tuple[threading.Event, threading.Thread, Dict[str, Any]]:
    """Temporarily keep a synchronous CARLA world moving during external waits.

    Autoware route setup waits on ROS topics produced by a CARLA GT bridge.
    If the runner stops ticking while waiting, the bridge cannot observe new
    frames and those topics never appear. This pump is scoped to that wait only;
    the main harness remains the normal tick owner.
    """
    stop_event = threading.Event()
    stats: Dict[str, Any] = {"ticks": 0, "errors": 0, "last_error": None}

    def _worker() -> None:
        while not stop_event.is_set():
            try:
                world.tick(float(tick_timeout_s))
                stats["ticks"] = int(stats.get("ticks", 0)) + 1
            except Exception as exc:
                stats["errors"] = int(stats.get("errors", 0)) + 1
                stats["last_error"] = str(exc)
                time.sleep(0.2)
            if sleep_s > 0.0:
                time.sleep(float(sleep_s))

    thread = threading.Thread(target=_worker, name=f"{label}_carla_tick_pump", daemon=True)
    thread.start()
    return stop_event, thread, stats


def _stop_carla_tick_pump(
    stop_event: threading.Event,
    thread: threading.Thread,
    stats: Dict[str, Any],
    *,
    label: str,
    join_timeout_s: float = 5.0,
) -> None:
    stop_event.set()
    thread.join(timeout=float(join_timeout_s))
    if thread.is_alive():
        print(f"[WARN] {label} CARLA tick pump did not stop within {join_timeout_s:.1f}s")
    print(
        f"[goal] {label} CARLA tick pump stopped "
        f"ticks={stats.get('ticks', 0)} errors={stats.get('errors', 0)}"
        + (f" last_error={stats.get('last_error')}" if stats.get("last_error") else "")
    )


def _build_ros2_runner(effective_cfg: Dict[str, Any], adapter_started: bool):
    stack = effective_cfg.get("algo", {}).get("stack")
    if stack == "autoware" and adapter_started:
        aw_cfg = effective_cfg.get("algo", {}).get("autoware", {}) or {}
        compose_file = Path(aw_cfg.get("compose", "algo/baselines/autoware/docker/compose.yaml"))
        service = aw_cfg.get("container_name", "autoware")
        return ComposeRos2Runner(compose_file=compose_file, service=service, repo_root=TESTBED_ROOT)
    return LocalRos2Runner()


def _default_followstop_run_dir(
    repo_root: Path,
    ts: int,
    cfg: Dict[str, Any],
    *,
    town: str,
) -> Path:
    run_name = build_run_name(
        str(ts),
        [
            "followstop",
            (cfg.get("algo", {}) or {}).get("stack", "algo"),
            (cfg.get("io", {}) or {}).get("mode", "io"),
            (cfg.get("scenario", {}) or {}).get("driver", "scenario"),
            town,
        ],
    )
    return next_available_run_dir(repo_root / "runs", run_name)


def build_ros2_bag_topics(
    rig_final: dict,
    ego_id: str,
    namespace: str = "/carla",
    camera_suffix: str = "image",
    lidar_suffix: str = "point_cloud",
    radar_suffix: str = "point_cloud",
    include_tf: bool = True,
    include_clock: bool = True,
    auto_topics: bool = True,
    explicit_topics: Optional[List[str]] = None,
    extra_topics: Optional[List[str]] = None,
) -> List[str]:
    ns = _normalize_ros_namespace(namespace)
    topics = []
    if auto_topics and rig_final:
        for sensor in rig_final.get("sensors", []) or []:
            if not sensor.get("enabled", True):
                continue
            sid = sensor.get("id")
            bp = sensor.get("blueprint", "")
            prefix = f"{ns}/{ego_id}/{sid}"
            if bp.startswith("sensor.camera"):
                topics.append(f"{prefix}/{camera_suffix}")
                topics.append(f"{prefix}/camera_info")
            elif bp.startswith("sensor.lidar"):
                topics.append(f"{prefix}/{lidar_suffix}")
            elif "sensor.other.imu" in bp:
                topics.append(f"{prefix}/imu")
            elif "sensor.other.gnss" in bp:
                topics.append(f"{prefix}/gnss")
            elif "sensor.other.radar" in bp:
                topics.append(f"{prefix}/{radar_suffix}")
        # 安全兜底：即便 rig 未声明，也默认尝试录 imu/gnss 话题（原生接口常见命名）
        topics.append(f"{ns}/{ego_id}/imu")
        topics.append(f"{ns}/{ego_id}/gnss")
        topics.append(f"{ns}/{ego_id}/odom")
        topics.append(f"{ns}/{ego_id}/objects3d")
        topics.append(f"{ns}/{ego_id}/objects_markers")
    if explicit_topics:
        topics.extend([t.strip() for t in explicit_topics if t.strip()])
    if extra_topics:
        topics.extend([t.strip() for t in extra_topics if t.strip()])
    # include_tf/clock handled by recorder flags; no need to insert here unless user specified explicitly
    return _dedup(topics)


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Follow-stop demo（支持配置驱动）")
    ap.add_argument("--config", type=Path, required=False, help="推荐：configs/io/examples/followstop_autoware.yaml")
    ap.add_argument("--override", action="append", default=[], help="key=value 覆盖配置，可多次")
    ap.add_argument("--run-dir", type=Path, default=None, help="输出目录；不填则自动 runs/followstop_<ts>")
    ap.add_argument("--town", default="Town01")
    ap.add_argument("--ticks", type=int, default=None)
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=2000)
    ap.add_argument("--carla-root", type=Path, default=DEFAULT_CARLA_ROOT, help="CARLA根目录（含 code/followstop/controllers.py）")
    ap.add_argument("--front-idx", type=int, default=210)
    ap.add_argument("--ego-idx", type=int, default=120)
    ap.add_argument("--controller", default="composite")
    ap.add_argument("--lateral-mode", default="dummy")
    ap.add_argument("--policy-mode", default="acc")
    ap.add_argument("--agent-type", default="basic")
    ap.add_argument("--takeover-dist", type=float, default=200.0)
    ap.add_argument("--blend-time", type=float, default=1.5)
    ap.add_argument("--rig", default="minimal", choices=["minimal", "apollo_like", "perception_lidar", "perception_camera", "fullstack"])
    ap.add_argument("--rig-file", type=str, default=None, help="自定义 rig yaml/json 路径")
    ap.add_argument("--rig-override", action="append", default=[], help="rig 覆盖，格式 key=value，支持点路径")
    ap.add_argument("--enable-fail-capture", action="store_true", help="失败时抓取失败窗口 HUD 视频")
    ap.add_argument("--ego-id", default="hero", help="ego role_name/ros_name（默认 hero）")
    # Recording modes (new)
    ap.add_argument("--record", action="append", choices=["dual_cam", "hud", "sensor_demo"], help="录制/渲染模式（可多次传递）")
    ap.add_argument("--record-output", type=Path, default=None, help="录制输出目录（默认 run_dir/video）")
    ap.add_argument("--record-fps", type=float, default=None, help="录制输出 fps（默认取仿真 1/dt）")
    ap.add_argument("--record-resolution", type=str, default="1920x1080", help="录制分辨率，如 1920x1080")
    ap.add_argument("--record-chase-distance", type=float, default=8.0)
    ap.add_argument("--record-chase-height", type=float, default=3.0)
    ap.add_argument("--record-chase-pitch", type=float, default=-15.0)
    ap.add_argument("--record-max-lidar-points", type=int, default=10000)
    ap.add_argument("--record-keep-frames", action="store_true", help="保留中间帧 png")
    ap.add_argument("--record-no-lidar", action="store_true", help="sensor_demo 渲染时跳过 lidar overlay")
    ap.add_argument("--record-no-radar", action="store_true", help="sensor_demo 渲染时跳过 radar overlay")
    ap.add_argument("--record-no-hud", action="store_true", help="跳过 HUD 叠加（sensor_demo/hud 模式）")
    ap.add_argument("--record-hud-mode", choices=["driving", "debug"], default="driving", help="HUD 模式：driving 简洁 / debug 全量")
    ap.add_argument("--record-hud-col-width", type=int, default=360, help="HUD 左侧列宽（320~380）")
    ap.add_argument("--record-dual-cam-third-person-only", action="store_true", help="dual_cam 仅导出第三人称视频")
    # Deprecated
    ap.add_argument("--record-demo", action="store_true", help="(deprecated) 录制双相机 demo")
    ap.add_argument("--make-hud", action="store_true", help="(deprecated) 生成 HUD overlay")
    # Spectator follow
    ap.add_argument("--follow-spectator", action="store_true", help="让 CARLA spectator 视角跟随 ego（本地渲染时便于观察）")
    ap.add_argument("--follow-spectator-distance", type=float, default=None, help="跟车视角距离，默认沿用 record_chase_distance")
    ap.add_argument("--follow-spectator-height", type=float, default=None, help="跟车视角高度，默认沿用 record_chase_height")
    ap.add_argument("--follow-spectator-pitch", type=float, default=None, help="跟车视角俯仰角，默认沿用 record_chase_pitch")
    ap.add_argument("--scenario-goal-ahead-m", type=float, default=None, help="Apollo 场景终点前向距离（默认取配置或 300m）")
    ap.add_argument("--scenario-goal-x", type=float, default=None, help="Apollo 场景终点 X（CARLA 坐标）")
    ap.add_argument("--scenario-goal-y", type=float, default=None, help="Apollo 场景终点 Y（CARLA 坐标）")
    ap.add_argument("--scenario-goal-z", type=float, default=None, help="Apollo 场景终点 Z（CARLA 坐标，可选）")
    # CARLA server auto-start
    ap.add_argument("--start-carla", action="store_true", help="若未手动启动 CARLA，自动按给定参数拉起服务器")
    ap.add_argument("--carla-binary", type=Path, default=None, help="CarlaUE4.sh 路径（默认 carla_root/CarlaUE4.sh）")
    ap.add_argument("--carla-extra-args", type=str, default="", help="透传给 CarlaUE4 的其他参数，例如 \"-quality-level=Epic\"")
    ap.add_argument("--carla-foreground", action="store_true", help="前台输出 CARLA 日志（同时写入 log 文件）")
    return ap


def main():
    ap = build_arg_parser()
    args = ap.parse_args()
    print("[INFO] 建议使用 --config 简化参数；其他参数保留为兼容覆盖。")

    # defaults for ROS2/rviz/rosbag even if CLI flags未声明
    _defaults = {
        "enable_ros2_native": False,
        "ros2_namespace": "/carla",
        "ros_invert_tf": True,
        "enable_ros2_gt": False,
        "ros2_gt_publish_tf": True,
        "ros2_gt_publish_odom": True,
        "ros2_gt_publish_objects3d": True,
        "ros2_gt_publish_markers": True,
        "ros2_gt_objects_radius_m": 120.0,
        "ros2_gt_max_objects": 64,
        "ros2_gt_odom_hz": 20.0,
        "ros2_gt_tf_hz": 20.0,
        "ros2_gt_objects_hz": 10.0,
        "ros2_gt_markers_hz": 10.0,
        "ros2_gt_qos_reliability": "best_effort",
        "ros2_gt_qos_depth": 10,
        "enable_rviz": False,
        "rviz_mode": "docker",
        "rviz_domain": 0,
        "rviz_ego": None,
        "rviz_camera_image_suffix": "image",
        "rviz_lidar_cloud_suffix": "point_cloud",
        "rviz_docker_image": "carla_testbed_rviz:humble",
        "enable_ros2_bag": False,
        "ros2_bag_out": None,
        "ros2_bag_storage": "sqlite3",
        "ros2_bag_compress": "none",
        "ros2_bag_max_size_mb": None,
        "ros2_bag_max_duration_s": None,
        "ros2_bag_include_tf": True,
        "ros2_bag_include_clock": True,
        "ros2_bag_topics": None,
        "ros2_bag_extra_topics": None,
        "ros2_bag_auto_topics": True,
        "ros2_bag_camera_image_suffix": "image",
        "ros2_bag_lidar_cloud_suffix": "point_cloud",
        "ros2_bag_radar_cloud_suffix": "point_cloud",
        "auto_align_front_spawn": True,
        "require_aligned_front_spawn": False,
        "front_min_ahead_m": 20.0,
        "front_max_ahead_m": 80.0,
        "front_target_ahead_m": None,
        "front_placement_mode": "spawn_index",
        "front_waypoint_ahead_m": None,
        "front_max_lateral_m": 3.0,
        "front_max_heading_diff_deg": 25.0,
        "force_green_traffic_lights": False,
        "freeze_traffic_lights": True,
        "vehicle_blueprint_id": "",
        "vehicle_blueprint_patterns": [
            "vehicle.lincoln.mkz_2020",
            "vehicle.lincoln.mkz_2017",
            "vehicle.lincoln.mkz*",
            "vehicle.tesla.model3",
        ],
        "ego_offset_x_m": 0.0,
        "ego_offset_y_m": 0.0,
        "ego_offset_z_m": 0.0,
        "ego_yaw_offset_deg": 0.0,
        "front_offset_x_m": 0.0,
        "front_offset_y_m": 0.0,
        "front_offset_z_m": 0.0,
        "front_yaw_offset_deg": 0.0,
        "ego_initial_brake": 0.0,
        "ego_initial_hand_brake": False,
        "semantic_scene_type": "lateral_straight_track",
        "semantic_waypoint_spacing_m": 3.0,
        "semantic_preview_distance_m": 40.0,
        "semantic_curve_min_yaw_delta_deg": 12.0,
        "semantic_straight_max_yaw_delta_deg": 4.0,
        "semantic_ego_front_gap_m": 38.0,
        "semantic_far_front_gap_m": 120.0,
        "semantic_scene_length_ahead_m": 120.0,
        "semantic_allow_junction": False,
        "semantic_preferred_road_ids": [],
        "semantic_preferred_section_ids": [],
        "semantic_preferred_lane_ids": [],
        "semantic_min_start_y": None,
        "semantic_max_start_y": None,
        "semantic_candidate_sorted_index": None,
        "semantic_lead_profile_mode": "static_stop",
        "semantic_lead_static_brake": 1.0,
        "semantic_lead_hand_brake": True,
        "semantic_lead_cruise_speed_mps": 6.0,
        "semantic_lead_hold_before_move_sec": 2.0,
        "semantic_lead_cruise_hold_sec": 3.0,
        "semantic_lead_stop_hold_sec": 2.0,
        "semantic_lead_throttle_cap": 0.55,
        "semantic_lead_brake_cap": 0.65,
    }
    for k, v in _defaults.items():
        if not hasattr(args, k):
            setattr(args, k, v)

    def deep_update(base: Dict[str, Any], patch: Dict[str, Any]):
        for k, v in patch.items():
            if isinstance(v, dict) and isinstance(base.get(k), dict):
                deep_update(base[k], v)
            else:
                base[k] = v
        return base

    def parse_overrides(pairs):
        out: Dict[str, Any] = {}
        for item in pairs or []:
            if "=" not in item:
                continue
            k, v = item.split("=", 1)
            cursor = out
            parts = k.split(".")
            for p in parts[:-1]:
                cursor = cursor.setdefault(p, {})
            cursor[parts[-1]] = yaml.safe_load(v)
        return out

    # 若提供 config，则将其字段映射到现有参数，其他保持默认/CLI 值
    if args.config:
        cfg = yaml.safe_load(args.config.read_text()) or {}
        cfg = deep_update(cfg, parse_overrides(args.override))
        # 保存 effective 到 run_dir 后面再写
        run_cfg = cfg.get("run", {})
        args.town = run_cfg.get("map", args.town)
        if args.ticks is None:
            args.ticks = run_cfg.get("ticks", 10)
        args.ego_id = run_cfg.get("ego_id", args.ego_id)
        scenario_cfg = cfg.get("scenario", {})
        stack_cfg = (cfg.get("algo", {}) or {}).get("stack")
        args.front_idx = scenario_cfg.get("front_idx", args.front_idx)
        args.ego_idx = scenario_cfg.get("ego_idx", args.ego_idx)
        args.auto_align_front_spawn = bool(
            scenario_cfg.get("auto_align_front_spawn", args.auto_align_front_spawn)
        )
        args.require_aligned_front_spawn = bool(
            scenario_cfg.get("require_aligned_front_spawn", args.require_aligned_front_spawn)
        )
        args.front_min_ahead_m = float(scenario_cfg.get("front_min_ahead_m", args.front_min_ahead_m))
        args.front_max_ahead_m = float(scenario_cfg.get("front_max_ahead_m", args.front_max_ahead_m))
        target_ahead = scenario_cfg.get("front_target_ahead_m", args.front_target_ahead_m)
        args.front_target_ahead_m = None if target_ahead is None else float(target_ahead)
        args.front_placement_mode = str(
            scenario_cfg.get("front_placement_mode", args.front_placement_mode) or "spawn_index"
        )
        waypoint_ahead = scenario_cfg.get(
            "front_waypoint_ahead_m", args.front_waypoint_ahead_m
        )
        args.front_waypoint_ahead_m = None if waypoint_ahead is None else float(waypoint_ahead)
        args.front_max_lateral_m = float(
            scenario_cfg.get("front_max_lateral_m", args.front_max_lateral_m)
        )
        args.front_max_heading_diff_deg = float(
            scenario_cfg.get("front_max_heading_diff_deg", args.front_max_heading_diff_deg)
        )
        args.vehicle_blueprint_id = str(
            scenario_cfg.get("vehicle_blueprint_id", args.vehicle_blueprint_id) or ""
        )
        patterns = scenario_cfg.get("vehicle_blueprint_patterns", args.vehicle_blueprint_patterns)
        if isinstance(patterns, list) and patterns:
            args.vehicle_blueprint_patterns = [str(item) for item in patterns]
        ego_pose_offset = scenario_cfg.get("ego_pose_offset", {}) or {}
        front_pose_offset = scenario_cfg.get("front_pose_offset", {}) or {}
        args.ego_offset_x_m = float(
            ego_pose_offset.get("x_m", scenario_cfg.get("ego_offset_x_m", args.ego_offset_x_m))
        )
        args.ego_offset_y_m = float(
            ego_pose_offset.get("y_m", scenario_cfg.get("ego_offset_y_m", args.ego_offset_y_m))
        )
        args.ego_offset_z_m = float(
            ego_pose_offset.get("z_m", scenario_cfg.get("ego_offset_z_m", args.ego_offset_z_m))
        )
        args.ego_yaw_offset_deg = float(
            ego_pose_offset.get(
                "yaw_deg", scenario_cfg.get("ego_yaw_offset_deg", args.ego_yaw_offset_deg)
            )
        )
        args.front_offset_x_m = float(
            front_pose_offset.get("x_m", scenario_cfg.get("front_offset_x_m", args.front_offset_x_m))
        )
        args.front_offset_y_m = float(
            front_pose_offset.get("y_m", scenario_cfg.get("front_offset_y_m", args.front_offset_y_m))
        )
        args.front_offset_z_m = float(
            front_pose_offset.get("z_m", scenario_cfg.get("front_offset_z_m", args.front_offset_z_m))
        )
        args.front_yaw_offset_deg = float(
            front_pose_offset.get(
                "yaw_deg", scenario_cfg.get("front_yaw_offset_deg", args.front_yaw_offset_deg)
            )
        )
        ego_initial_hold_cfg = scenario_cfg.get("ego_initial_hold", {}) or {}
        args.ego_initial_brake = float(
            ego_initial_hold_cfg.get("brake", scenario_cfg.get("ego_initial_brake", args.ego_initial_brake))
        )
        args.ego_initial_hand_brake = bool(
            ego_initial_hold_cfg.get(
                "hand_brake",
                scenario_cfg.get("ego_initial_hand_brake", args.ego_initial_hand_brake),
            )
        )
        semantic_cfg = scenario_cfg.get("semantic_suite", {}) or {}
        args.semantic_scene_type = str(
            semantic_cfg.get("scene_type", scenario_cfg.get("scene_type", args.semantic_scene_type)) or ""
        )
        args.semantic_waypoint_spacing_m = float(
            semantic_cfg.get("waypoint_spacing_m", args.semantic_waypoint_spacing_m)
        )
        args.semantic_preview_distance_m = float(
            semantic_cfg.get("preview_distance_m", args.semantic_preview_distance_m)
        )
        args.semantic_curve_min_yaw_delta_deg = float(
            semantic_cfg.get("curve_min_yaw_delta_deg", args.semantic_curve_min_yaw_delta_deg)
        )
        args.semantic_straight_max_yaw_delta_deg = float(
            semantic_cfg.get("straight_max_yaw_delta_deg", args.semantic_straight_max_yaw_delta_deg)
        )
        args.semantic_ego_front_gap_m = float(
            semantic_cfg.get("ego_front_gap_m", args.semantic_ego_front_gap_m)
        )
        args.semantic_far_front_gap_m = float(
            semantic_cfg.get("far_front_gap_m", args.semantic_far_front_gap_m)
        )
        args.semantic_scene_length_ahead_m = float(
            semantic_cfg.get("scene_length_ahead_m", args.semantic_scene_length_ahead_m)
        )
        args.semantic_allow_junction = bool(
            semantic_cfg.get("allow_junction", args.semantic_allow_junction)
        )
        args.semantic_preferred_road_ids = _safe_int_list(
            semantic_cfg.get("preferred_road_ids", args.semantic_preferred_road_ids)
        )
        args.semantic_preferred_section_ids = _safe_int_list(
            semantic_cfg.get("preferred_section_ids", args.semantic_preferred_section_ids)
        )
        args.semantic_preferred_lane_ids = _safe_int_list(
            semantic_cfg.get("preferred_lane_ids", args.semantic_preferred_lane_ids)
        )
        args.semantic_min_start_y = _safe_float(
            semantic_cfg.get("min_start_y", args.semantic_min_start_y)
        )
        args.semantic_max_start_y = _safe_float(
            semantic_cfg.get("max_start_y", args.semantic_max_start_y)
        )
        args.semantic_candidate_sorted_index = _safe_int(
            semantic_cfg.get("candidate_sorted_index", args.semantic_candidate_sorted_index)
        )
        lead_profile_cfg = semantic_cfg.get("lead_profile", {}) or {}
        args.semantic_lead_profile_mode = str(
            lead_profile_cfg.get("mode", args.semantic_lead_profile_mode) or ""
        )
        args.semantic_lead_static_brake = float(
            lead_profile_cfg.get("static_brake", args.semantic_lead_static_brake)
        )
        args.semantic_lead_hand_brake = bool(
            lead_profile_cfg.get("hand_brake", args.semantic_lead_hand_brake)
        )
        args.semantic_lead_cruise_speed_mps = float(
            lead_profile_cfg.get("cruise_speed_mps", args.semantic_lead_cruise_speed_mps)
        )
        args.semantic_lead_hold_before_move_sec = float(
            lead_profile_cfg.get("hold_before_move_sec", args.semantic_lead_hold_before_move_sec)
        )
        args.semantic_lead_cruise_hold_sec = float(
            lead_profile_cfg.get("cruise_hold_sec", args.semantic_lead_cruise_hold_sec)
        )
        args.semantic_lead_stop_hold_sec = float(
            lead_profile_cfg.get("stop_hold_sec", args.semantic_lead_stop_hold_sec)
        )
        args.semantic_lead_throttle_cap = float(
            lead_profile_cfg.get("throttle_cap", args.semantic_lead_throttle_cap)
        )
        args.semantic_lead_brake_cap = float(
            lead_profile_cfg.get("brake_cap", args.semantic_lead_brake_cap)
        )
        tl_cfg = scenario_cfg.get("traffic_lights", {}) or {}
        args.force_green_traffic_lights = bool(
            tl_cfg.get("force_green", args.force_green_traffic_lights)
        )
        args.freeze_traffic_lights = bool(
            tl_cfg.get("freeze", args.freeze_traffic_lights)
        )
        io_ros_cfg = ((cfg.get("io", {}) or {}).get("ros", {}) or {})
        args.ros2_namespace = io_ros_cfg.get("namespace", args.ros2_namespace)
        args.enable_ros2_native = scenario_cfg.get("publish_ros2_native", args.enable_ros2_native)
        default_gt = True if stack_cfg == "apollo" else args.enable_ros2_native
        args.enable_ros2_gt = scenario_cfg.get("publish_ros2_gt", default_gt)
        gt_cfg = scenario_cfg.get("gt", {}) or {}
        args.ros2_gt_publish_tf = gt_cfg.get("publish_tf", args.ros2_gt_publish_tf)
        args.ros2_gt_publish_odom = gt_cfg.get("publish_odom", args.ros2_gt_publish_odom)
        args.ros2_gt_publish_objects3d = gt_cfg.get("publish_objects3d", args.ros2_gt_publish_objects3d)
        args.ros2_gt_publish_markers = gt_cfg.get("publish_markers", args.ros2_gt_publish_markers)
        args.ros2_gt_objects_radius_m = gt_cfg.get("objects_radius_m", args.ros2_gt_objects_radius_m)
        args.ros2_gt_max_objects = gt_cfg.get("max_objects", args.ros2_gt_max_objects)
        args.ros2_gt_odom_hz = gt_cfg.get("odom_hz", args.ros2_gt_odom_hz)
        args.ros2_gt_tf_hz = gt_cfg.get("tf_hz", args.ros2_gt_tf_hz)
        args.ros2_gt_objects_hz = gt_cfg.get("objects_hz", args.ros2_gt_objects_hz)
        args.ros2_gt_markers_hz = gt_cfg.get("markers_hz", args.ros2_gt_markers_hz)
        args.ros2_gt_qos_reliability = gt_cfg.get("qos_reliability", args.ros2_gt_qos_reliability)
        args.ros2_gt_qos_depth = gt_cfg.get("qos_depth", args.ros2_gt_qos_depth)
        apollo_tf_cfg = (
            ((cfg.get("algo", {}) or {}).get("apollo", {}) or {}).get("carla_to_apollo", {}) or {}
        )
        if "invert_tf" in apollo_tf_cfg:
            args.ros_invert_tf = bool(apollo_tf_cfg.get("invert_tf"))
        elif "invert_tf" in io_ros_cfg:
            args.ros_invert_tf = bool(io_ros_cfg.get("invert_tf"))
        # rig
        io_contract = cfg.get("io", {}).get("contract", {}) if cfg.get("io") else {}
        rig_path = io_contract.get("sensor_minimal")
        if rig_path:
            rig_path = Path(rig_path)
            if rig_path.exists() and rig_path.suffix in [".yaml", ".yml", ".json"]:
                args.rig_file = rig_path
            else:
                args.rig = str(rig_path)
        # control/record 映射
        record_cfg = cfg.get("record", {})
        rb = record_cfg.get("rosbag", {}) if record_cfg else {}
        if rb.get("enable"):
            args.enable_ros2_bag = True
            args.ros2_bag_out = Path(rb.get("out")) if rb.get("out") else args.ros2_bag_out
            args.ros2_bag_storage = rb.get("storage", args.ros2_bag_storage)
            args.ros2_bag_compress = rb.get("compress", args.ros2_bag_compress)
            args.ros2_bag_include_tf = rb.get("include_tf", args.ros2_bag_include_tf)
            args.ros2_bag_include_clock = rb.get("include_clock", args.ros2_bag_include_clock)
            args.ros2_bag_max_size_mb = rb.get("max_size_mb", args.ros2_bag_max_size_mb)
            args.ros2_bag_max_duration_s = rb.get("max_duration_s", args.ros2_bag_max_duration_s)
            args.ros2_bag_auto_topics = rb.get("auto_topics", args.ros2_bag_auto_topics)
            if rb.get("camera_suffix"):
                args.ros2_bag_camera_image_suffix = rb["camera_suffix"]
            if rb.get("lidar_suffix"):
                args.ros2_bag_lidar_cloud_suffix = rb["lidar_suffix"]
            if rb.get("radar_suffix"):
                args.ros2_bag_radar_cloud_suffix = rb["radar_suffix"]
            if rb.get("topics"):
                args.ros2_bag_topics = ",".join(rb.get("topics", []))
            if rb.get("extra_topics"):
                args.ros2_bag_extra_topics = ",".join(rb.get("extra_topics", []))
        # visual/record
        vis = record_cfg.get("visual", {})
        modes = vis.get("modes") or []
        if modes:
            args.record = modes
        if vis.get("output"):
            args.record_output = Path(vis["output"])
        if vis.get("fps") is not None:
            args.record_fps = vis.get("fps")
        if vis.get("resolution"):
            args.record_resolution = str(vis["resolution"])
        for key, attr in [
            ("chase_distance", "record_chase_distance"),
            ("chase_height", "record_chase_height"),
            ("chase_pitch", "record_chase_pitch"),
            ("max_lidar_points", "record_max_lidar_points"),
            ("keep_frames", "record_keep_frames"),
            ("no_lidar", "record_no_lidar"),
            ("no_radar", "record_no_radar"),
            ("no_hud", "record_no_hud"),
            ("hud_mode", "record_hud_mode"),
            ("hud_col_w", "record_hud_col_width"),
            ("dual_cam_third_person_only", "record_dual_cam_third_person_only"),
            ("follow_spectator", "follow_spectator"),
            ("follow_distance", "follow_spectator_distance"),
            ("follow_height", "follow_spectator_height"),
            ("follow_pitch", "follow_spectator_pitch"),
        ]:
            if key in vis:
                setattr(args, attr, vis[key])
        # rviz
        rviz_cfg = cfg.get("rviz", {}) or {}
        if rviz_cfg:
            args.enable_rviz = rviz_cfg.get("enable", args.enable_rviz)
            args.rviz_mode = rviz_cfg.get("mode", args.rviz_mode)
            args.rviz_domain = rviz_cfg.get("domain", args.rviz_domain)
            args.rviz_ego = rviz_cfg.get("ego", args.rviz_ego)
            args.rviz_camera_image_suffix = rviz_cfg.get("camera_suffix", args.rviz_camera_image_suffix)
            args.rviz_lidar_cloud_suffix = rviz_cfg.get("lidar_suffix", args.rviz_lidar_cloud_suffix)
            args.rviz_docker_image = rviz_cfg.get("docker_image", args.rviz_docker_image)
        # log
        log_level = cfg.get("logging", {}).get("level")
        if log_level:
            os.environ["LOGLEVEL"] = str(log_level)
        apollo_routing_cfg = (((cfg.get("algo", {}) or {}).get("apollo", {}) or {}).get("routing", {}) or {})
        if args.scenario_goal_ahead_m is None:
            args.scenario_goal_ahead_m = apollo_routing_cfg.get("scenario_goal_ahead_m", args.scenario_goal_ahead_m)
        cfg_goal_xy = apollo_routing_cfg.get("scenario_goal_xy")
        if (
            args.scenario_goal_x is None
            and args.scenario_goal_y is None
            and isinstance(cfg_goal_xy, dict)
            and "x" in cfg_goal_xy
            and "y" in cfg_goal_xy
        ):
            args.scenario_goal_x = float(cfg_goal_xy["x"])
            args.scenario_goal_y = float(cfg_goal_xy["y"])
            if "z" in cfg_goal_xy:
                args.scenario_goal_z = float(cfg_goal_xy["z"])
    else:
        cfg = {}
        if args.ticks is None:
            args.ticks = 10

    _maybe_reexec_with_ros2_runtime(args)

    # run_dir and effective config
    ts = int(time.time())
    repo_root = Path(__file__).resolve().parents[1]
    effective_cfg = deep_update(cfg.copy(), parse_overrides(args.override))
    out_run_dir = args.run_dir or _default_followstop_run_dir(repo_root, ts, effective_cfg, town=args.town)
    if args.run_dir and not out_run_dir.resolve().is_relative_to(repo_root):
        # redirect to repo runs
        redirected = repo_root / "runs" / args.run_dir.name
        redirected.mkdir(parents=True, exist_ok=True)
        out_run_dir = redirected
        try:
            args.run_dir.mkdir(parents=True, exist_ok=True)
            (args.run_dir / "RUN_DIR_REDIRECT.txt").write_text(str(out_run_dir))
        except Exception:
            pass
    if args.run_dir and out_run_dir.exists():
        # Avoid mixing artifacts from different attempts in the same run dir.
        try:
            if any(out_run_dir.iterdir()):
                alt = next_available_run_dir(out_run_dir.parent, out_run_dir.name)
                print(f"[run] requested run dir is non-empty, redirecting to: {alt}")
                out_run_dir = alt
                try:
                    args.run_dir.mkdir(parents=True, exist_ok=True)
                    (args.run_dir / "RUN_DIR_REDIRECT.txt").write_text(str(out_run_dir))
                except Exception:
                    pass
        except Exception as exc:
            print(f"[WARN] failed to check run dir reuse for {out_run_dir}: {exc}")
    out_run_dir.mkdir(parents=True, exist_ok=True)
    update_latest_pointer(out_run_dir)
    log_dir = out_run_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    startup_stage_path = out_run_dir / "artifacts" / "startup_stage.json"
    startup_timeline_path = out_run_dir / "artifacts" / "startup_stage_timeline.jsonl"
    carla_world_ready_trace_path = out_run_dir / "artifacts" / "carla_world_ready_trace.jsonl"
    carla_world_ready_summary_path = out_run_dir / "artifacts" / "carla_world_ready_summary.json"
    startup_stage_path.parent.mkdir(parents=True, exist_ok=True)
    carla_world_ready_summary: Dict[str, Any] = {
        "target_town": str(args.town),
        "start_carla": bool(args.start_carla),
        "status": "initialized",
        "client_connect_attempts": [],
        "wait_ready_attempts": [],
        "get_world_attempts": [],
        "load_world_attempts": [],
        "current_town_before_load": None,
        "final_town": None,
    }

    def _write_startup_stage(stage: str, **payload: Any) -> None:
        try:
            data: Dict[str, Any] = {"stage": stage, "ts_sec": time.time()}
            data.update(payload)
            startup_stage_path.write_text(json.dumps(data, indent=2, ensure_ascii=False))
            with startup_timeline_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(data, ensure_ascii=False) + "\n")
        except Exception:
            pass

    def _write_carla_world_ready_summary(**updates: Any) -> None:
        try:
            carla_world_ready_summary.update(updates)
            payload = dict(carla_world_ready_summary)
            payload["last_updated_ts_sec"] = time.time()
            carla_world_ready_summary_path.write_text(
                json.dumps(payload, indent=2, ensure_ascii=False)
            )
        except Exception:
            pass

    def _write_carla_world_ready_event(event: str, **payload: Any) -> None:
        try:
            data: Dict[str, Any] = {"event": event, "ts_sec": time.time()}
            data.update(payload)
            with carla_world_ready_trace_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(data, ensure_ascii=False) + "\n")
        except Exception:
            pass

    _write_startup_stage("run_initialized")
    _write_carla_world_ready_summary(status="run_initialized")
    _write_carla_world_ready_event("run_initialized")
    cleanup_state: Dict[str, Any] = {
        "done": False,
        "adapter": None,
        "adapter_profile": None,
        "adapter_start_attempted": False,
        "out_run_dir": out_run_dir,
        "control_logger_proc": None,
        "control_logger_extra_procs": [],
        "sensor_probe_proc": None,
        "world": None,
        "original_settings": None,
        "actors": None,
        "scenario": None,
        "carla_launcher": None,
        "carla_stop_hook": None,
    }
    cleanup_lock = threading.Lock()
    signal_state = {"handling": False}

    def _cleanup_once(reason: str) -> None:
        with cleanup_lock:
            if cleanup_state["done"]:
                return
            cleanup_state["done"] = True
        print(f"[cleanup] begin ({reason})")
        adapter_obj = cleanup_state.get("adapter")
        adapter_prof = cleanup_state.get("adapter_profile")
        if adapter_obj and adapter_prof and cleanup_state.get("adapter_start_attempted"):
            try:
                _run_cleanup_with_timeout(
                    "adapter_stop",
                    lambda: adapter_obj.stop(adapter_prof, cleanup_state["out_run_dir"]),
                    timeout_s=40.0,
                )
            except Exception as exc:
                print(f"[WARN] adapter stop failed: {exc}")
        _run_cleanup_with_timeout(
            "apollo_runtime_process_cleanup",
            _cleanup_stale_apollo_runtime_processes,
            timeout_s=6.0,
        )
        stop_process(cleanup_state.get("control_logger_proc"))
        for proc in cleanup_state.get("control_logger_extra_procs", []) or []:
            stop_process(proc)
        stop_process(cleanup_state.get("sensor_probe_proc"))
        world_obj = cleanup_state.get("world")
        original = cleanup_state.get("original_settings")
        if world_obj is not None and original is not None:
            _run_cleanup_with_timeout(
                "restore_settings",
                lambda: restore_settings(world_obj, original),
                timeout_s=6.0,
            )
        scenario_obj = cleanup_state.get("scenario")
        actors_obj = cleanup_state.get("actors")
        if scenario_obj is not None and actors_obj is not None:
            _run_cleanup_with_timeout("scenario_destroy", scenario_obj.destroy, timeout_s=8.0)
        launcher = cleanup_state.get("carla_launcher")
        if launcher is not None:
            _run_cleanup_with_timeout("carla_stop", launcher.stop, timeout_s=8.0)
        hook = cleanup_state.get("carla_stop_hook")
        if hook is not None:
            try:
                atexit.unregister(hook)
            except Exception:
                pass
            cleanup_state["carla_stop_hook"] = None
        print(f"[cleanup] end ({reason})")

    prev_sigint = signal.getsignal(signal.SIGINT)
    prev_sigterm = signal.getsignal(signal.SIGTERM)

    def _handle_interrupt(signum, _frame):
        if signal_state["handling"]:
            print(f"[WARN] received signal {signum} again during cleanup, forcing exit")
            os._exit(128 + signum)
        signal_state["handling"] = True
        try:
            print(f"[WARN] received signal {signum}, cleaning up before exit")
            _cleanup_once(f"signal_{signum}")
        finally:
            signal_state["handling"] = False
        if signum == signal.SIGINT:
            raise KeyboardInterrupt
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGINT, _handle_interrupt)
    signal.signal(signal.SIGTERM, _handle_interrupt)
    eff_path = out_run_dir / "effective.yaml"
    if args.config:
        effective_cfg["_profile_config_path"] = str(args.config.resolve())
    effective_cfg.setdefault("run", {})["ticks"] = args.ticks
    effective_cfg.setdefault("run", {})["map"] = args.town
    effective_cfg.setdefault("run", {})["ego_id"] = args.ego_id
    effective_cfg.setdefault("io", {}).setdefault("ros", {})["namespace"] = _normalize_ros_namespace(args.ros2_namespace)
    effective_cfg.setdefault("scenario", {})["front_idx"] = args.front_idx
    effective_cfg.setdefault("scenario", {})["ego_idx"] = args.ego_idx
    effective_cfg.setdefault("scenario", {})["vehicle_blueprint_id"] = args.vehicle_blueprint_id
    effective_cfg.setdefault("scenario", {})["vehicle_blueprint_patterns"] = list(
        args.vehicle_blueprint_patterns
    )
    effective_cfg.setdefault("scenario", {})["ego_pose_offset"] = {
        "x_m": float(args.ego_offset_x_m),
        "y_m": float(args.ego_offset_y_m),
        "z_m": float(args.ego_offset_z_m),
        "yaw_deg": float(args.ego_yaw_offset_deg),
    }
    effective_cfg.setdefault("scenario", {})["front_pose_offset"] = {
        "x_m": float(args.front_offset_x_m),
        "y_m": float(args.front_offset_y_m),
        "z_m": float(args.front_offset_z_m),
        "yaw_deg": float(args.front_yaw_offset_deg),
    }
    effective_cfg.setdefault("scenario", {})["semantic_suite"] = {
        "scene_type": str(args.semantic_scene_type),
        "waypoint_spacing_m": float(args.semantic_waypoint_spacing_m),
        "preview_distance_m": float(args.semantic_preview_distance_m),
        "curve_min_yaw_delta_deg": float(args.semantic_curve_min_yaw_delta_deg),
        "straight_max_yaw_delta_deg": float(args.semantic_straight_max_yaw_delta_deg),
        "ego_front_gap_m": float(args.semantic_ego_front_gap_m),
        "far_front_gap_m": float(args.semantic_far_front_gap_m),
        "scene_length_ahead_m": float(args.semantic_scene_length_ahead_m),
        "allow_junction": bool(args.semantic_allow_junction),
        "preferred_road_ids": [int(item) for item in args.semantic_preferred_road_ids],
        "preferred_section_ids": [int(item) for item in args.semantic_preferred_section_ids],
        "preferred_lane_ids": [int(item) for item in args.semantic_preferred_lane_ids],
        "min_start_y": _safe_float(args.semantic_min_start_y),
        "max_start_y": _safe_float(args.semantic_max_start_y),
        "candidate_sorted_index": _safe_int(args.semantic_candidate_sorted_index),
        "lead_profile": {
            "mode": str(args.semantic_lead_profile_mode),
            "static_brake": float(args.semantic_lead_static_brake),
            "hand_brake": bool(args.semantic_lead_hand_brake),
            "cruise_speed_mps": float(args.semantic_lead_cruise_speed_mps),
            "hold_before_move_sec": float(args.semantic_lead_hold_before_move_sec),
            "cruise_hold_sec": float(args.semantic_lead_cruise_hold_sec),
            "stop_hold_sec": float(args.semantic_lead_stop_hold_sec),
            "throttle_cap": float(args.semantic_lead_throttle_cap),
            "brake_cap": float(args.semantic_lead_brake_cap),
        },
    }
    eff_path.write_text(yaml.safe_dump(effective_cfg, sort_keys=False))
    acceptance_cfg = effective_cfg.get("acceptance", {}) if effective_cfg else {}
    acceptance_speed_threshold = float(acceptance_cfg.get("min_speed_mps", 5.0) or 5.0)
    acceptance_require_apollo_health_artifacts = bool(
        acceptance_cfg.get("require_apollo_health_artifacts", False)
    )
    acceptance_min_planning_nonempty = _safe_int(
        acceptance_cfg.get("min_planning_nonempty_trajectory_count")
    )
    acceptance_min_planning_nonzero_ratio = _safe_float(
        acceptance_cfg.get("min_planning_nonzero_ratio")
    )
    acceptance_max_invalid_goal_count = _safe_int(acceptance_cfg.get("max_invalid_goal_count"))
    acceptance_max_routing_invalid_goal_skip_count = _safe_int(
        acceptance_cfg.get("max_routing_skipped_due_to_invalid_goal_count")
    )
    low_speed_creep_cfg = acceptance_cfg.get("low_speed_creep", {}) or {}
    acceptance_low_speed_creep_enabled = bool(low_speed_creep_cfg.get("enabled", False))
    acceptance_low_speed_creep_start_after_sec = float(
        low_speed_creep_cfg.get("start_after_sec", 15.0) or 15.0
    )
    acceptance_low_speed_creep_speed_below_mps = float(
        low_speed_creep_cfg.get("speed_below_mps", 1.0) or 1.0
    )
    acceptance_low_speed_creep_front_gap_above_m = float(
        low_speed_creep_cfg.get("front_gap_above_m", 20.0) or 20.0
    )
    acceptance_low_speed_creep_require_routing_established = bool(
        low_speed_creep_cfg.get("require_routing_established", True)
    )
    acceptance_low_speed_creep_ignore_terminal_hold = bool(
        low_speed_creep_cfg.get("ignore_when_terminal_stop_hold_active", True)
    )
    acceptance_low_speed_creep_require_reached_speed_mps = _safe_float(
        low_speed_creep_cfg.get("require_reached_speed_mps")
    )
    if acceptance_low_speed_creep_require_reached_speed_mps is None:
        acceptance_low_speed_creep_require_reached_speed_mps = 0.0
    acceptance_low_speed_creep_max_duration_sec = _safe_float(
        low_speed_creep_cfg.get("max_duration_sec")
    )
    control_health_acceptance_cfg = acceptance_cfg.get("control_health", {}) or {}
    acceptance_control_health_enabled = bool(control_health_acceptance_cfg.get("enabled", False))
    acceptance_control_health_fail_on_missing = bool(
        control_health_acceptance_cfg.get("fail_on_missing", False)
    )
    acceptance_control_health_max_switch_count = _safe_int(
        control_health_acceptance_cfg.get("max_applied_throttle_brake_switch_count")
    )
    if acceptance_control_health_max_switch_count is None:
        acceptance_control_health_max_switch_count = 10
    target_speed_tracking_cfg = control_health_acceptance_cfg.get("target_speed_tracking", {}) or {}
    acceptance_target_speed_tracking_enabled = bool(
        target_speed_tracking_cfg.get("enabled", False)
    )
    acceptance_target_speed_tracking_fail_on_missing = bool(
        target_speed_tracking_cfg.get("fail_on_missing", False)
    )
    acceptance_target_speed_tracking_overspeed_threshold_mps = _safe_float(
        target_speed_tracking_cfg.get("overspeed_threshold_mps")
    )
    if acceptance_target_speed_tracking_overspeed_threshold_mps is None:
        acceptance_target_speed_tracking_overspeed_threshold_mps = 2.0
    acceptance_target_speed_tracking_max_throttle_rows = _safe_int(
        target_speed_tracking_cfg.get("max_throttle_while_overspeed_rows")
    )
    if acceptance_target_speed_tracking_max_throttle_rows is None:
        acceptance_target_speed_tracking_max_throttle_rows = 9
    route_health_acceptance_cfg = acceptance_cfg.get("route_health", {}) or {}
    route_health_min_completion_ratio = _safe_float(
        route_health_acceptance_cfg.get("min_completion_ratio")
    )
    route_health_min_distance_achieved_m = _safe_float(
        route_health_acceptance_cfg.get("min_distance_achieved_m")
    )
    route_health_max_raw_steer_saturated_ratio = _safe_float(
        route_health_acceptance_cfg.get("max_raw_steer_saturated_ratio")
    )
    route_health_max_longest_saturation_sec = _safe_float(
        route_health_acceptance_cfg.get("max_longest_saturation_sec")
    )
    followstop_acceptance_cfg = acceptance_cfg.get("followstop", {}) or {}
    acceptance_followstop_enabled = bool(followstop_acceptance_cfg.get("enabled", False))
    acceptance_followstop_fail_on_warn = bool(
        followstop_acceptance_cfg.get("fail_on_warn", True)
    )
    acceptance_followstop_stop_zone_m = _safe_float(
        followstop_acceptance_cfg.get("stop_zone_m")
    )
    if acceptance_followstop_stop_zone_m is None:
        acceptance_followstop_stop_zone_m = 15.0
    acceptance_followstop_stopped_speed_mps = _safe_float(
        followstop_acceptance_cfg.get("stopped_speed_mps")
    )
    if acceptance_followstop_stopped_speed_mps is None:
        acceptance_followstop_stopped_speed_mps = 1.0

    if args.enable_rviz and not args.enable_ros2_native:
        print("[ERROR] --enable-rviz 仅在 --enable-ros2-native 模式下可用")
        sys.exit(1)
    if args.enable_ros2_bag and not args.enable_ros2_native and stack_cfg != "autoware":
        print("[ERROR] --enable-ros2-bag 仅支持原生 ROS2 发布模式，请先加 --enable-ros2-native")
        sys.exit(1)
    args.ros2_namespace = _normalize_ros_namespace(args.ros2_namespace)
    rviz_ego = args.rviz_ego or args.ego_id

    default_out = out_run_dir
    # Parse resolution
    def _parse_res(text: str):
        if isinstance(text, (list, tuple)) and len(text) == 2:
            return int(text[0]), int(text[1])
        if "x" in text:
            w, h = text.lower().split("x", 1)
            return int(w), int(h)
        return 1920, 1080

    record_modes: List[str] = []
    if args.record:
        record_modes.extend(args.record)
    if args.record_demo:
        print("[WARN] --record-demo 已弃用，请改用 --record dual_cam")
        record_modes.append("dual_cam")
    if args.make_hud:
        print("[WARN] --make-hud 已弃用，请改用 --record hud")
        record_modes.append("hud")
    # deduplicate
    record_modes = list(dict.fromkeys(record_modes))
    record_resolution = _parse_res(args.record_resolution)
    # spectator follow defaults reuse chase params unless用户覆盖
    follow_spectator = args.follow_spectator
    follow_distance = args.follow_spectator_distance or args.record_chase_distance
    follow_height = args.follow_spectator_height or args.record_chase_height
    follow_pitch = args.follow_spectator_pitch or args.record_chase_pitch

    cfg = HarnessConfig(
        town=args.town,
        max_steps=args.ticks,
        out_dir=default_out,
        enable_ros2_native=args.enable_ros2_native,
        ros2_namespace=args.ros2_namespace,
        ros_invert_tf=args.ros_invert_tf,
        enable_ros2_gt=args.enable_ros2_gt,
        ros2_gt_publish_tf=args.ros2_gt_publish_tf,
        ros2_gt_publish_odom=args.ros2_gt_publish_odom,
        ros2_gt_publish_objects3d=args.ros2_gt_publish_objects3d,
        ros2_gt_publish_markers=args.ros2_gt_publish_markers,
        ros2_gt_objects_radius_m=args.ros2_gt_objects_radius_m,
        ros2_gt_max_objects=args.ros2_gt_max_objects,
        ros2_gt_odom_hz=args.ros2_gt_odom_hz,
        ros2_gt_tf_hz=args.ros2_gt_tf_hz,
        ros2_gt_objects_hz=args.ros2_gt_objects_hz,
        ros2_gt_markers_hz=args.ros2_gt_markers_hz,
        ros2_gt_qos_reliability=args.ros2_gt_qos_reliability,
        ros2_gt_qos_depth=args.ros2_gt_qos_depth,
        ego_id=args.ego_id,
        record_modes=record_modes,
        record_output=args.record_output,
        record_fps=args.record_fps,
        record_resolution=record_resolution,
        record_chase_distance=args.record_chase_distance,
        record_chase_height=args.record_chase_height,
        record_chase_pitch=args.record_chase_pitch,
        record_max_lidar_points=args.record_max_lidar_points,
        record_keep_frames=args.record_keep_frames,
        record_no_lidar=args.record_no_lidar,
        record_no_radar=args.record_no_radar,
        record_no_hud=args.record_no_hud,
        follow_spectator=follow_spectator,
        spectator_distance=follow_distance,
        spectator_height=follow_height,
        spectator_pitch=follow_pitch,
        enable_ros2_bag=args.enable_ros2_bag,
        ros2_bag_out=args.ros2_bag_out,
        ros2_bag_storage=args.ros2_bag_storage,
        ros2_bag_compress=args.ros2_bag_compress,
        ros2_bag_max_size_mb=args.ros2_bag_max_size_mb,
        ros2_bag_max_duration_s=args.ros2_bag_max_duration_s,
        ros2_bag_include_tf=args.ros2_bag_include_tf,
        ros2_bag_include_clock=args.ros2_bag_include_clock,
        ros2_bag_topics=args.ros2_bag_topics.split(",") if args.ros2_bag_topics else None,
        ros2_bag_extra_topics=args.ros2_bag_extra_topics.split(",") if args.ros2_bag_extra_topics else None,
        ros2_bag_auto_topics=args.ros2_bag_auto_topics,
        ros2_bag_camera_image_suffix=args.ros2_bag_camera_image_suffix,
        ros2_bag_lidar_cloud_suffix=args.ros2_bag_lidar_cloud_suffix,
        ros2_bag_radar_cloud_suffix=args.ros2_bag_radar_cloud_suffix,
    )
    if effective_cfg is not None:
        # Keep adapter/backend timing aligned with the actual simulator step.
        effective_cfg.setdefault("run", {})["dt"] = float(cfg.dt)
    run_cfg = effective_cfg.get("run", {}) if effective_cfg else {}
    pacing_cfg = run_cfg.get("wall_time_pacing", {}) if isinstance(run_cfg, dict) else {}
    if isinstance(pacing_cfg, dict):
        cfg.wall_time_pacing_enabled = bool(pacing_cfg.get("enabled", cfg.wall_time_pacing_enabled))
        target_interval = pacing_cfg.get("target_interval_s", pacing_cfg.get("target_dt_s"))
        if target_interval is None and cfg.wall_time_pacing_enabled:
            target_interval = run_cfg.get("fixed_delta_seconds", cfg.dt)
        if target_interval is not None:
            try:
                cfg.wall_time_pacing_target_interval_s = max(0.0, float(target_interval))
            except Exception:
                print(f"[WARN] invalid run.wall_time_pacing.target_interval_s={target_interval!r}; pacing target ignored")
        max_sleep = pacing_cfg.get("max_sleep_s")
        if max_sleep is not None:
            try:
                cfg.wall_time_pacing_max_sleep_s = max(0.0, float(max_sleep))
            except Exception:
                print(f"[WARN] invalid run.wall_time_pacing.max_sleep_s={max_sleep!r}; pacing sleep cap ignored")
    fail_strategy = str(run_cfg.get("fail_strategy", "")).strip().lower()
    if fail_strategy in {"fail_fast", "log_and_continue"}:
        cfg.fail_strategy = fail_strategy
    post_fail_steps = run_cfg.get("post_fail_steps")
    if post_fail_steps is not None:
        try:
            cfg.post_fail_steps = max(1, int(post_fail_steps))
        except Exception:
            pass
    # Prepare Autoware/dummy via adapter when stack 指定
    adapter = None
    adapter_profile = None
    adapter_start_attempted = False
    cleanup_state["adapter"] = adapter
    cleanup_state["adapter_profile"] = adapter_profile
    cleanup_state["adapter_start_attempted"] = adapter_start_attempted
    stack = effective_cfg.get("algo", {}).get("stack") if effective_cfg else None
    if stack:
        try:
            adapter = get_adapter(stack)
            adapter_profile = effective_cfg
            # generate artifacts if requested
            gen_cfg = adapter_profile.get("io", {}).get("generate", {}) if adapter_profile.get("io") else {}
            contract_paths = adapter_profile.get("io", {}).get("contract", {}) if adapter_profile.get("io") else {}
            artifacts_dir = Path(adapter_profile.get("artifacts", {}).get("dir", out_run_dir / "artifacts"))
            if any(gen_cfg.get(k, False) for k in ["sensor_mapping", "sensor_kit_calibration", "qos_overrides", "frames"]):
                generate_all(
                    rig_path=Path(contract_paths.get("sensor_minimal", "configs/rigs/minimal.yaml")),
                    contract_path=Path(contract_paths.get("canon_ros2", "io/contract/canon_ros2.yaml")),
                    frames_path=Path("io/contract/frames.yaml"),
                    out_dir=artifacts_dir,
                )
            adapter_profile.setdefault("artifacts", {})["dir"] = str(artifacts_dir)
            adapter_profile.setdefault("runtime", {})["compose_clean"] = adapter_profile.get("runtime", {}).get("compose_clean", False)
        except Exception as exc:
            print(f"[WARN] adapter init failed: {exc}")
            adapter = None
            adapter_profile = None
    cleanup_state["adapter"] = adapter
    cleanup_state["adapter_profile"] = adapter_profile

    harness = TestHarness(cfg)
    monitor = SignalMonitor(snapshot_interval=20)

    carla_launcher = None
    adapter_started = False
    adapter_fail_reason = None
    carla_stop_hook = None
    autoware_stack_cfg = ((effective_cfg.get("algo", {}) or {}).get("autoware", {}) or {}) if effective_cfg else {}
    defer_adapter_start_until_scenario_spawn = (
        str(stack or "").strip().lower() == "autoware"
        and _config_bool(autoware_stack_cfg.get("start_after_scenario_spawn"), default=True)
    )

    def _start_stack_adapter(start_reason: str) -> None:
        nonlocal adapter_start_attempted, adapter_started, adapter_fail_reason
        if not adapter or not adapter_profile or adapter_started:
            return
        try:
            _write_startup_stage(
                "adapter_prepare_start",
                stack=str(stack or ""),
                start_reason=str(start_reason),
            )
            adapter_start_attempted = True
            cleanup_state["adapter_start_attempted"] = True
            adapter.prepare(adapter_profile, out_run_dir)
            _write_startup_stage(
                "adapter_prepare_done",
                stack=str(stack or ""),
                start_reason=str(start_reason),
            )
            started_result = adapter.start(adapter_profile, out_run_dir)
            adapter_started = True if started_result is None else bool(started_result)
            _write_startup_stage(
                "adapter_start_done",
                stack=str(stack or ""),
                adapter_started=adapter_started,
                start_reason=str(start_reason),
            )
            if stack in {"autoware", "apollo"} and not adapter_started:
                adapter_fail_reason = (
                    "AUTOWARE_CONTAINER_EXIT" if stack == "autoware" else "APOLLO_BRIDGE_EXIT"
                )
                return
            if stack == "apollo":
                ok = adapter.healthcheck(adapter_profile, out_run_dir)
                if not ok:
                    print("[WARN] Apollo backend healthcheck failed (non-fatal); see artifacts")
            else:
                ok = healthcheck(eff_path, timeout=5.0)
                if not ok:
                    print("[WARN] ROS2 healthcheck failed (non-fatal); see messages above")
        except Exception as exc:
            if stack == "autoware":
                adapter_fail_reason = "AUTOWARE_START_FAIL"
            elif stack == "apollo":
                adapter_fail_reason = "APOLLO_START_FAIL"
            else:
                adapter_fail_reason = "ADAPTER_START_FAIL"
            print(f"[WARN] adapter start failed: {exc}")
            _write_startup_stage(
                "adapter_start_exception",
                stack=str(stack or ""),
                start_reason=str(start_reason),
                error=str(exc),
            )

    runtime_carla_cfg = ((effective_cfg.get("runtime", {}) or {}).get("carla", {}) or {}) if effective_cfg else {}
    stop_reused_on_exit = bool(runtime_carla_cfg.get("stop_reused_on_exit", True))
    carla_ready_timeout_sec = float(runtime_carla_cfg.get("ready_timeout_sec", 180.0) or 180.0)
    carla_ready_poll_sec = float(runtime_carla_cfg.get("ready_poll_sec", 1.0) or 1.0)
    carla_post_ready_settle_sec = float(runtime_carla_cfg.get("post_ready_settle_sec", 0.0) or 0.0)
    carla_get_world_attempts = int(runtime_carla_cfg.get("get_world_attempts", 3) or 3)
    carla_get_world_delay_sec = float(runtime_carla_cfg.get("get_world_delay_sec", 3.0) or 3.0)
    carla_load_world_attempts = int(runtime_carla_cfg.get("load_world_attempts", 3) or 3)
    carla_load_world_delay_sec = float(runtime_carla_cfg.get("load_world_delay_sec", 3.0) or 3.0)
    carla_load_world_timeout_sec = float(runtime_carla_cfg.get("load_world_timeout_sec", 60.0) or 60.0)
    carla_launch_with_map = _config_bool(runtime_carla_cfg.get("launch_with_map"), default=True)
    carla_env_overrides = _followstop_carla_env_overrides(runtime_carla_cfg)
    carla_bootstrap_stability_window_s = float(
        runtime_carla_cfg.get("bootstrap_stability_window_s", 0.0) or 0.0
    )
    carla_use_memory_guard = _config_bool(
        runtime_carla_cfg.get("use_systemd_scope_memory_guard"),
        default=False,
    )
    carla_enable_auto_recovery = _config_bool(
        runtime_carla_cfg.get("launcher_auto_recovery"),
        default=False,
    )
    carla_world_ready_summary.update(
        {
            "launch_with_map": bool(carla_launch_with_map),
            "launch_town": str(args.town if carla_launch_with_map else ""),
            "force_headless_env": bool(_config_bool(runtime_carla_cfg.get("force_headless_env"), default=False)),
            "use_systemd_scope_memory_guard": bool(carla_use_memory_guard),
            "bootstrap_stability_window_s": carla_bootstrap_stability_window_s,
        }
    )
    if args.start_carla:
        carla_launcher = CarlaLauncher(
            carla_root=args.carla_root,
            host=args.host,
            port=args.port,
            town=args.town if carla_launch_with_map else "",
            extra_args=args.carla_extra_args,
            foreground=args.carla_foreground,
            run_dir=out_run_dir,
            stop_reused_on_exit=stop_reused_on_exit,
            env_overrides=carla_env_overrides,
            enable_auto_recovery=carla_enable_auto_recovery,
            bootstrap_stability_window_s=carla_bootstrap_stability_window_s,
            use_systemd_scope_memory_guard=carla_use_memory_guard,
            memory_high=str(runtime_carla_cfg.get("memory_high") or "9G"),
            memory_max=str(runtime_carla_cfg.get("memory_max") or "10G"),
            oom_policy=str(runtime_carla_cfg.get("oom_policy") or "kill"),
        )
        cleanup_state["carla_launcher"] = carla_launcher
        carla_stop_hook = lambda: carla_launcher.stop()
        cleanup_state["carla_stop_hook"] = carla_stop_hook
        atexit.register(carla_stop_hook)
        try:
            _write_startup_stage("carla_launch_start")
            _write_carla_world_ready_event("carla_launch_start")
            carla_launcher.start()
            wait_ready_attempt = 1
            wait_ready_started = time.time()
            _write_carla_world_ready_event(
                "carla_wait_ready_attempt_start",
                attempt=wait_ready_attempt,
                timeout_sec=carla_ready_timeout_sec,
                poll_sec=carla_ready_poll_sec,
            )
            wait_ready_ok = carla_launcher.wait_ready(
                timeout_s=carla_ready_timeout_sec,
                poll_s=carla_ready_poll_sec,
            )
            wait_ready_row = {
                "attempt": wait_ready_attempt,
                "elapsed_sec": time.time() - wait_ready_started,
                "reused": bool(carla_launcher.reused),
                "result": "ok" if wait_ready_ok else "failed",
            }
            carla_world_ready_summary["wait_ready_attempts"].append(wait_ready_row)
            _write_carla_world_ready_summary(status="waiting_carla_ready")
            _write_carla_world_ready_event("carla_wait_ready_attempt_done", **wait_ready_row)
            if not wait_ready_ok:
                if not carla_launcher.reused:
                    print("[WARN] CARLA 首次启动未就绪，尝试自动重启一次")
                    _write_startup_stage("carla_wait_ready_retry")
                    _write_carla_world_ready_event("carla_wait_ready_retry")
                    carla_launcher.stop()
                    time.sleep(2.0)
                    carla_launcher.start()
                wait_ready_attempt = 2
                wait_ready_started = time.time()
                _write_carla_world_ready_event(
                    "carla_wait_ready_attempt_start",
                    attempt=wait_ready_attempt,
                    timeout_sec=carla_ready_timeout_sec,
                    poll_sec=carla_ready_poll_sec,
                )
                wait_ready_ok = carla_launcher.wait_ready(
                    timeout_s=carla_ready_timeout_sec,
                    poll_s=carla_ready_poll_sec,
                )
                wait_ready_row = {
                    "attempt": wait_ready_attempt,
                    "elapsed_sec": time.time() - wait_ready_started,
                    "reused": bool(carla_launcher.reused),
                    "result": "ok" if wait_ready_ok else "failed",
                }
                carla_world_ready_summary["wait_ready_attempts"].append(wait_ready_row)
                _write_carla_world_ready_summary(status="waiting_carla_ready")
                _write_carla_world_ready_event("carla_wait_ready_attempt_done", **wait_ready_row)
                if not wait_ready_ok:
                    print("[ERROR] CARLA 未在超时时间内就绪")
                    print(carla_launcher.diagnose_tail())
                    _write_startup_stage("carla_wait_ready_failed")
                    _write_carla_world_ready_summary(status="carla_wait_ready_failed")
                    _write_carla_world_ready_event("carla_wait_ready_failed")
                    sys.exit(1)
            _write_startup_stage("carla_wait_ready_ok", reused=bool(carla_launcher.reused))
            _write_carla_world_ready_summary(status="carla_wait_ready_ok")
            _write_carla_world_ready_event(
                "carla_wait_ready_ok",
                reused=bool(carla_launcher.reused),
            )
            if carla_post_ready_settle_sec > 0.0:
                print(f"[carla] ready, settling for {carla_post_ready_settle_sec:.1f}s")
                time.sleep(carla_post_ready_settle_sec)
        except Exception as exc:
            print(f"[ERROR] CARLA 启动失败: {exc}")
            if carla_launcher:
                print(carla_launcher.diagnose_tail())
            _write_startup_stage("carla_launch_exception", error=str(exc))
            _write_carla_world_ready_summary(status="carla_launch_exception", error=repr(exc))
            _write_carla_world_ready_event("carla_launch_exception", error=repr(exc))
            sys.exit(1)
    else:
        if not _wait_for_carla(args.host, args.port, timeout=10.0):
            _write_carla_world_ready_summary(status="external_carla_missing")
            _write_carla_world_ready_event("external_carla_missing")
            print("[ERROR] 未检测到运行中的 CARLA，请先启动或使用 --start-carla")
            sys.exit(1)

    def _record_bringup_phase(phase: str, payload: Dict[str, Any]) -> None:
        row = {k: v for k, v in dict(payload).items() if k not in {"attempts", "delay_sec", "timeout_sec"}}
        if phase == "client_connect_attempt_start":
            _write_carla_world_ready_event(
                "carla_client_connect_attempt_start",
                **payload,
            )
            return
        if phase == "client_connect_attempt_ok":
            row["success"] = True
            carla_world_ready_summary["client_connect_attempts"].append(row)
            _write_carla_world_ready_summary(status="carla_client_connect_ok")
            _write_carla_world_ready_event("carla_client_connect_attempt_ok", **row)
            return
        if phase == "client_connect_attempt_failed":
            row["success"] = False
            carla_world_ready_summary["client_connect_attempts"].append(row)
            _write_carla_world_ready_summary(status="carla_client_connect_retrying")
            _write_carla_world_ready_event("carla_client_connect_attempt_failed", **row)
            print(
                f"[carla][WARN] client connect attempt {payload.get('attempt')}/{payload.get('attempts')} "
                f"failed: {payload.get('error')}",
                flush=True,
            )
            return
        if phase == "get_world_attempt_start":
            _write_carla_world_ready_event("carla_get_world_attempt_start", **payload)
            return
        if phase == "get_world_attempt_ok":
            row["success"] = True
            carla_world_ready_summary["get_world_attempts"].append(row)
            _write_carla_world_ready_summary(
                status="carla_get_world_ok",
                current_town_before_load=row.get("current_town"),
            )
            _write_carla_world_ready_event("carla_get_world_attempt_ok", **row)
            return
        if phase == "get_world_attempt_failed":
            row["success"] = False
            carla_world_ready_summary["get_world_attempts"].append(row)
            _write_carla_world_ready_summary(status="carla_get_world_retrying")
            _write_carla_world_ready_event("carla_get_world_attempt_failed", **row)
            print(
                f"[carla][WARN] get_world attempt {payload.get('attempt')}/{payload.get('attempts')} "
                f"failed: {payload.get('error')}",
                flush=True,
            )
            return
        if phase == "get_world_failed":
            _write_carla_world_ready_summary(status="carla_get_world_failed", error=payload.get("error"))
            _write_carla_world_ready_event("carla_get_world_failed", **payload)
            return
        if phase == "load_world_attempt_start":
            if int(payload.get("attempt") or 0) == 1:
                current_town_before_load = str(carla_world_ready_summary.get("current_town_before_load") or "")
                _write_startup_stage(
                    "carla_load_world_start",
                    current_town=current_town_before_load,
                    target_town=args.town,
                )
                _write_carla_world_ready_summary(status="carla_load_world_start")
                _write_carla_world_ready_event(
                    "carla_load_world_start",
                    current_town=current_town_before_load,
                    target_town=args.town,
                )
            _write_carla_world_ready_event("carla_load_world_attempt_start", **payload)
            return
        if phase == "load_world_attempt_ok":
            row["success"] = True
            carla_world_ready_summary["load_world_attempts"].append(row)
            _write_carla_world_ready_summary(
                status="carla_load_world_ok",
                final_town=row.get("loaded_town") or payload.get("target_town"),
            )
            _write_carla_world_ready_event("carla_load_world_attempt_ok", **row)
            return
        if phase == "load_world_attempt_failed":
            row["success"] = False
            carla_world_ready_summary["load_world_attempts"].append(row)
            _write_carla_world_ready_summary(status="carla_load_world_retrying")
            _write_carla_world_ready_event("carla_load_world_attempt_failed", **row)
            print(
                f"[carla][WARN] load_world attempt {payload.get('attempt')}/{payload.get('attempts')} "
                f"failed: {payload.get('error')}",
                flush=True,
            )
            return
        if phase == "load_world_failed":
            _write_carla_world_ready_summary(status="carla_load_world_failed", error=payload.get("error"))
            _write_carla_world_ready_event("carla_load_world_failed", **payload)

    _write_startup_stage("carla_get_world_start")
    bringup_result = connect_world_with_retry(
        host=args.host,
        port=int(args.port),
        target_town=str(args.town),
        client_timeout_s=30.0,
        client_attempts=3,
        client_delay_s=2.0,
        get_world_attempts=carla_get_world_attempts,
        get_world_delay_s=carla_get_world_delay_sec,
        load_world_attempts=carla_load_world_attempts,
        load_world_delay_s=carla_load_world_delay_sec,
        load_world_timeout_s=carla_load_world_timeout_sec,
        root=args.carla_root,
        attempt_callback=_record_bringup_phase,
    )
    client = bringup_result.client
    world = bringup_result.world
    current_town = str(bringup_result.current_town_before_load or "")
    _write_startup_stage("carla_get_world_ok", current_town=current_town)
    _write_carla_world_ready_summary(
        status="carla_get_world_ok",
        current_town_before_load=current_town,
    )
    _write_carla_world_ready_event("carla_get_world_ok", current_town=current_town)
    if current_town != args.town:
        loaded_town = str(bringup_result.final_town or "")
        _write_startup_stage("carla_load_world_ok", current_town=loaded_town)
        _write_carla_world_ready_summary(status="world_ready", final_town=loaded_town)
        _write_carla_world_ready_event("carla_load_world_ok", current_town=loaded_town)
    else:
        _write_carla_world_ready_summary(status="world_ready", final_town=current_town)
        _write_carla_world_ready_event("carla_load_world_skipped", current_town=current_town)

    # Start external stacks only after CARLA has settled on the target town.
    # Autoware GT bridge additionally needs the ego actor to exist before route
    # submission, otherwise localization/tf waits can expire before the bridge
    # ever binds the CARLA actor.
    if adapter and adapter_profile and defer_adapter_start_until_scenario_spawn:
        _write_startup_stage(
            "adapter_start_deferred_until_scenario_spawn",
            stack=str(stack or ""),
        )
    else:
        _start_stack_adapter("post_world_ready")

    original_settings = world.get_settings()
    cleanup_state["world"] = world
    cleanup_state["original_settings"] = original_settings
    if cfg.synchronous_mode:
        original_settings = configure_synchronous_mode(world, cfg.dt)
        cleanup_state["original_settings"] = original_settings
    bp_lib = world.get_blueprint_library()

    scenario_driver = ((effective_cfg.get("scenario", {}) or {}).get("driver") or "").strip().lower()
    _write_startup_stage("scenario_prepare", scenario_driver=scenario_driver)
    if scenario_driver == "carla_apollo_semantic_suite":
        scenario = ApolloSemanticSuiteScenario(
            ApolloSemanticSuiteConfig(
                scene_type=str(args.semantic_scene_type or "lateral_straight_track"),
                role_ego=args.ego_id,
                role_front="front",
                vehicle_blueprint_id=str(args.vehicle_blueprint_id or ""),
                vehicle_blueprint_patterns=tuple(str(item) for item in args.vehicle_blueprint_patterns),
                waypoint_spacing_m=float(args.semantic_waypoint_spacing_m),
                preview_distance_m=float(args.semantic_preview_distance_m),
                curve_min_yaw_delta_deg=float(args.semantic_curve_min_yaw_delta_deg),
                straight_max_yaw_delta_deg=float(args.semantic_straight_max_yaw_delta_deg),
                ego_front_gap_m=float(args.semantic_ego_front_gap_m),
                far_front_gap_m=float(args.semantic_far_front_gap_m),
                scene_length_ahead_m=float(args.semantic_scene_length_ahead_m),
                allow_junction=bool(args.semantic_allow_junction),
                preferred_road_ids=tuple(int(item) for item in args.semantic_preferred_road_ids),
                preferred_section_ids=tuple(int(item) for item in args.semantic_preferred_section_ids),
                preferred_lane_ids=tuple(int(item) for item in args.semantic_preferred_lane_ids),
                min_start_y=_safe_float(args.semantic_min_start_y),
                max_start_y=_safe_float(args.semantic_max_start_y),
                candidate_sorted_index=_safe_int(args.semantic_candidate_sorted_index),
                ego_offset_x_m=float(args.ego_offset_x_m),
                ego_offset_y_m=float(args.ego_offset_y_m),
                ego_offset_z_m=float(args.ego_offset_z_m),
                ego_yaw_offset_deg=float(args.ego_yaw_offset_deg),
                front_offset_x_m=float(args.front_offset_x_m),
                front_offset_y_m=float(args.front_offset_y_m),
                front_offset_z_m=float(args.front_offset_z_m),
                front_yaw_offset_deg=float(args.front_yaw_offset_deg),
                lead_profile=SemanticLeadProfile(
                    mode=str(args.semantic_lead_profile_mode or "static_stop"),
                    static_brake=float(args.semantic_lead_static_brake),
                    hand_brake=bool(args.semantic_lead_hand_brake),
                    cruise_speed_mps=float(args.semantic_lead_cruise_speed_mps),
                    hold_before_move_sec=float(args.semantic_lead_hold_before_move_sec),
                    cruise_hold_sec=float(args.semantic_lead_cruise_hold_sec),
                    stop_hold_sec=float(args.semantic_lead_stop_hold_sec),
                    throttle_cap=float(args.semantic_lead_throttle_cap),
                    brake_cap=float(args.semantic_lead_brake_cap),
                ),
            )
        )
    elif scenario_driver == "carla_actuator_calibration":
        calibration_cfg = ((effective_cfg.get("scenario", {}) or {}).get("calibration_only", {}) or {})
        scenario = CalibrationOnlyScenario(
            CalibrationOnlyConfig(
                spawn_idx=int(calibration_cfg.get("spawn_idx", args.ego_idx)),
                strict_spawn=bool(calibration_cfg.get("strict_spawn", True)),
                ego_id=args.ego_id,
                vehicle_blueprint_id=str(
                    calibration_cfg.get("vehicle_blueprint_id", args.vehicle_blueprint_id) or ""
                ),
                vehicle_blueprint_patterns=tuple(
                    str(item)
                    for item in calibration_cfg.get(
                        "vehicle_blueprint_patterns",
                        args.vehicle_blueprint_patterns,
                    )
                ),
                force_green_traffic_lights=bool(
                    calibration_cfg.get("force_green_traffic_lights", args.force_green_traffic_lights)
                ),
                freeze_traffic_lights=bool(
                    calibration_cfg.get("freeze_traffic_lights", args.freeze_traffic_lights)
                ),
                ego_offset_x_m=float(calibration_cfg.get("ego_offset_x_m", args.ego_offset_x_m)),
                ego_offset_y_m=float(calibration_cfg.get("ego_offset_y_m", args.ego_offset_y_m)),
                ego_offset_z_m=float(calibration_cfg.get("ego_offset_z_m", args.ego_offset_z_m)),
                ego_yaw_offset_deg=float(
                    calibration_cfg.get("ego_yaw_offset_deg", args.ego_yaw_offset_deg)
                ),
            )
        )
    elif scenario_driver == "carla_town01_route_health":
        route_health_cfg = ((effective_cfg.get("scenario", {}) or {}).get("route_health", {}) or {})
        traffic_lights_cfg = ((effective_cfg.get("scenario", {}) or {}).get("traffic_lights", {}) or {})
        scenario = Town01RouteHealthScenario(
            Town01RouteHealthConfig(
                scenario_class=str(
                    route_health_cfg.get(
                        "scenario_class",
                        ((effective_cfg.get("run", {}) or {}).get("scenario_class", "lane_keep")),
                    )
                    or "lane_keep"
                ),
                capability_profile=str(
                    route_health_cfg.get(
                        "capability_profile",
                        ((effective_cfg.get("run", {}) or {}).get("capability_profile", "lane_keep")),
                    )
                    or "lane_keep"
                ),
                random_seed=int(
                    route_health_cfg.get("random_seed", ((effective_cfg.get("run", {}) or {}).get("seed", 1)))
                ),
                ego_id=args.ego_id,
                vehicle_blueprint_id=str(route_health_cfg.get("vehicle_blueprint_id", args.vehicle_blueprint_id) or ""),
                vehicle_blueprint_patterns=tuple(
                    str(item)
                    for item in route_health_cfg.get(
                        "vehicle_blueprint_patterns",
                        args.vehicle_blueprint_patterns,
                    )
                ),
                strict_spawn=bool(route_health_cfg.get("strict_spawn", False)),
                preferred_spawn_idx=int(route_health_cfg.get("preferred_spawn_idx", -1) or -1),
                force_green_traffic_lights=bool(
                    route_health_cfg.get("force_green_traffic_lights", args.force_green_traffic_lights)
                ),
                freeze_traffic_lights=bool(
                    route_health_cfg.get("freeze_traffic_lights", args.freeze_traffic_lights)
                ),
                traffic_light_control_mode=str(
                    route_health_cfg.get(
                        "traffic_light_control_mode",
                        traffic_lights_cfg.get("control_mode", ""),
                    )
                    or ""
                ),
                traffic_light_initial_state=str(
                    route_health_cfg.get(
                        "traffic_light_initial_state",
                        traffic_lights_cfg.get("initial_state", ""),
                    )
                    or ""
                ),
                traffic_light_release_state=str(
                    route_health_cfg.get(
                        "traffic_light_release_state",
                        traffic_lights_cfg.get("release_state", ""),
                    )
                    or ""
                ),
                traffic_light_release_after_s=float(
                    route_health_cfg.get(
                        "traffic_light_release_after_s",
                        traffic_lights_cfg.get("release_after_s", 0.0),
                    )
                    or 0.0
                ),
                traffic_light_target_actor_ids=tuple(
                    _safe_int_list(
                        route_health_cfg.get(
                            "traffic_light_target_actor_ids",
                            traffic_lights_cfg.get("target_actor_ids", []),
                        )
                    )
                ),
                route_step_m=float(route_health_cfg.get("route_step_m", 5.0) or 5.0),
                spawn_min_forward_length_m=float(
                    route_health_cfg.get("spawn_min_forward_length_m", 120.0) or 120.0
                ),
                spawn_min_backward_length_m=float(
                    route_health_cfg.get("spawn_min_backward_length_m", 25.0) or 25.0
                ),
                spawn_reject_junction=bool(route_health_cfg.get("spawn_reject_junction", True)),
                spawn_junction_window_m=float(route_health_cfg.get("spawn_junction_window_m", 12.0) or 12.0),
                spawn_min_successor_count=int(route_health_cfg.get("spawn_min_successor_count", 1) or 1),
                goal_min_route_length_m=float(route_health_cfg.get("goal_min_route_length_m", 180.0) or 180.0),
                goal_max_route_length_m=float(route_health_cfg.get("goal_max_route_length_m", 360.0) or 360.0),
                goal_min_end_margin_m=float(route_health_cfg.get("goal_min_end_margin_m", 20.0) or 20.0),
                goal_reject_junction=bool(route_health_cfg.get("goal_reject_junction", True)),
                max_spawn_candidates=int(route_health_cfg.get("max_spawn_candidates", 80) or 80),
                max_goal_attempts=int(route_health_cfg.get("max_goal_attempts", 5) or 5),
                route_id=str(route_health_cfg.get("route_id", "") or ""),
                route_corpus_path=str(route_health_cfg.get("route_corpus_path", "") or ""),
                ego_offset_x_m=float(route_health_cfg.get("ego_offset_x_m", args.ego_offset_x_m)),
                ego_offset_y_m=float(route_health_cfg.get("ego_offset_y_m", args.ego_offset_y_m)),
                ego_offset_z_m=float(route_health_cfg.get("ego_offset_z_m", args.ego_offset_z_m)),
                ego_yaw_offset_deg=float(route_health_cfg.get("ego_yaw_offset_deg", args.ego_yaw_offset_deg)),
            )
        )
    else:
        scenario = FollowStopScenario(
            FollowStopConfig(
                front_idx=args.front_idx,
                ego_idx=args.ego_idx,
                ego_id=args.ego_id,
                vehicle_blueprint_id=str(args.vehicle_blueprint_id or ""),
                vehicle_blueprint_patterns=tuple(str(item) for item in args.vehicle_blueprint_patterns),
                auto_align_front_spawn=bool(args.auto_align_front_spawn),
                require_aligned_front_spawn=bool(args.require_aligned_front_spawn),
                front_min_ahead_m=float(args.front_min_ahead_m),
                front_max_ahead_m=float(args.front_max_ahead_m),
                front_target_ahead_m=args.front_target_ahead_m,
                front_placement_mode=str(args.front_placement_mode or "spawn_index"),
                front_waypoint_ahead_m=args.front_waypoint_ahead_m,
                front_max_lateral_m=float(args.front_max_lateral_m),
                front_max_heading_diff_deg=float(args.front_max_heading_diff_deg),
                force_green_traffic_lights=bool(args.force_green_traffic_lights),
                freeze_traffic_lights=bool(args.freeze_traffic_lights),
                ego_offset_x_m=float(args.ego_offset_x_m),
                ego_offset_y_m=float(args.ego_offset_y_m),
                ego_offset_z_m=float(args.ego_offset_z_m),
                ego_yaw_offset_deg=float(args.ego_yaw_offset_deg),
                front_offset_x_m=float(args.front_offset_x_m),
                front_offset_y_m=float(args.front_offset_y_m),
                front_offset_z_m=float(args.front_offset_z_m),
                front_yaw_offset_deg=float(args.front_yaw_offset_deg),
                ego_initial_brake=float(args.ego_initial_brake),
                ego_initial_hand_brake=bool(args.ego_initial_hand_brake),
            )
        )
    cleanup_state["scenario"] = scenario
    actors = None

    try:
        _write_startup_stage("scenario_build_start", scenario_type=type(scenario).__name__)
        actors = scenario.build(world, world.get_map(), bp_lib)
        _write_startup_stage("scenario_build_done", scenario_type=type(scenario).__name__)
        cleanup_state["actors"] = actors
        ego = actors.ego
        front = actors.front
        run_mode_cfg_for_spawn = effective_cfg.get("run", {}) if effective_cfg else {}
        post_spawn_settle_ticks = int(run_mode_cfg_for_spawn.get("post_spawn_settle_ticks", 2) or 0)
        if post_spawn_settle_ticks > 0:
            for _ in range(post_spawn_settle_ticks):
                try:
                    # Before external-stack route submission this process is
                    # the only reliable CARLA tick owner. Prefer an active
                    # tick even when cfg.synchronous_mode is stale, otherwise
                    # spawned actors can remain invisible to bridge clients.
                    world.tick()
                except Exception as exc:
                    if not cfg.synchronous_mode:
                        try:
                            world.wait_for_tick(1.0)
                            continue
                        except Exception:
                            pass
                    print(f"[WARN] post-spawn settle tick failed: {exc}")
                    break
        post_spawn_visibility_ticks = int(
            run_mode_cfg_for_spawn.get(
                "post_spawn_visibility_ticks",
                30 if defer_adapter_start_until_scenario_spawn else 0,
            )
            or 0
        )
        if post_spawn_visibility_ticks > 0:
            _write_startup_stage(
                "scenario_actor_visibility_start",
                ego_actor_id=int(getattr(ego, "id", 0) or 0),
                front_actor_id=int(getattr(front, "id", 0) or 0) if front is not None else 0,
                max_ticks=post_spawn_visibility_ticks,
            )
            visibility_report = _wait_for_external_actor_visibility(
                client,
                world,
                expected={
                    str(args.ego_id or "ego"): int(getattr(ego, "id", 0) or 0),
                    "front": int(getattr(front, "id", 0) or 0) if front is not None else 0,
                },
                max_ticks=post_spawn_visibility_ticks,
                tick_timeout_s=float(run_mode_cfg_for_spawn.get("post_spawn_visibility_tick_timeout_s", 5.0) or 5.0),
                sleep_s=float(run_mode_cfg_for_spawn.get("post_spawn_visibility_sleep_s", 0.0) or 0.0),
            )
            try:
                (out_run_dir / "artifacts" / "scenario_actor_visibility.json").write_text(
                    json.dumps(visibility_report, indent=2, ensure_ascii=False)
                )
            except Exception:
                pass
            stage = (
                "scenario_actor_visibility_ok"
                if visibility_report.get("visible")
                else "scenario_actor_visibility_failed"
            )
            _write_startup_stage(stage, **visibility_report)
            if not visibility_report.get("visible"):
                print(
                    "[WARN] spawned actors are not visible to a fresh CARLA client before adapter start; "
                    f"report={out_run_dir / 'artifacts' / 'scenario_actor_visibility.json'}"
                )
                if _config_bool(run_mode_cfg_for_spawn.get("require_post_spawn_actor_visibility"), default=False):
                    raise RuntimeError("spawned actors not visible to external CARLA clients")
        scenario_meta_path = out_run_dir / "artifacts" / "scenario_metadata.json"
        try:
            scenario_meta = scenario.metadata() if hasattr(scenario, "metadata") else {}
            if not isinstance(scenario_meta, dict):
                scenario_meta = {}
            scenario_meta.update(
                {
                    "scenario_driver": scenario_driver,
                    "ego_role": args.ego_id,
                    "front_role": "front",
                    "ego_actor_id": int(getattr(ego, "id", 0) or 0),
                    "front_actor_id": int(getattr(front, "id", 0) or 0) if front is not None else 0,
                }
            )
            scenario_meta_path.parent.mkdir(parents=True, exist_ok=True)
            scenario_meta_path.write_text(json.dumps(scenario_meta, indent=2))
        except Exception as exc:
            print(f"[WARN] failed to write scenario metadata: {exc}")
        if scenario_driver == "carla_apollo_semantic_suite":
            print(f"Spawned semantic suite scene: {json.dumps(scenario.metadata(), ensure_ascii=True)}")
        elif scenario_driver == "carla_actuator_calibration":
            print(f"Spawned calibration-only scene: {json.dumps(scenario.metadata(), ensure_ascii=True)}")
        elif scenario_driver == "carla_town01_route_health":
            print(f"Spawned town01 route health scene: {json.dumps(scenario.metadata(), ensure_ascii=True)}")
        else:
            print(f"Spawned ego at idx={scenario.cfg.ego_idx}, front at idx={scenario.cfg.front_idx}")
        try:
            scenario_goal_path = _write_apollo_scenario_goal(
                effective_cfg, out_run_dir, ego, front, args, scenario=scenario
            )
            if scenario_goal_path is not None:
                print(f"[apollo] scenario goal written: {scenario_goal_path}")
        except Exception as exc:
            print(f"[WARN] failed to write Apollo scenario goal: {exc}")

        if defer_adapter_start_until_scenario_spawn and adapter and adapter_profile and not adapter_started:
            _write_startup_stage(
                "adapter_start_after_scenario_spawn",
                stack=str(stack or ""),
                ego_actor_id=int(getattr(ego, "id", 0) or 0),
                front_actor_id=int(getattr(front, "id", 0) or 0) if front is not None else 0,
            )
            _start_stack_adapter("post_scenario_spawn")

        ros2_runner = _build_ros2_runner(effective_cfg, adapter_started)

        # optional: send goal + engage (backend-agnostic for compose/local ROS2)
        aw_cfg = effective_cfg.get("algo", {}).get("autoware", {}) if effective_cfg else {}
        goal_cfg = aw_cfg.get("goal", {}) if aw_cfg else {}
        goal_log_path = out_run_dir / "artifacts" / "autoware_goal_and_engage.log"
        if goal_cfg.get("enable", False) and (args.enable_ros2_native or (stack == "autoware" and adapter_started)):
            ahead_m = float(goal_cfg.get("ahead_m", 50.0) or 0.0)
            frame_id = goal_cfg.get("frame_id", "map")
            target_actor = front or ego
            fallback_tf = None
            ego_fallback_tf = None
            try:
                spawns = world.get_map().get_spawn_points()
                spawn_idx = getattr(getattr(scenario, "cfg", None), "front_idx" if front is not None else "ego_idx", None)
                if isinstance(spawn_idx, int) and 0 <= spawn_idx < len(spawns):
                    fallback_tf = spawns[spawn_idx]
                ego_idx = getattr(getattr(scenario, "cfg", None), "ego_idx", None)
                if isinstance(ego_idx, int) and 0 <= ego_idx < len(spawns):
                    ego_fallback_tf = spawns[ego_idx]
            except Exception:
                fallback_tf = None
                ego_fallback_tf = None
            explicit_goal = goal_cfg.get("carla_goal_pose") or goal_cfg.get("goal_pose")
            if isinstance(explicit_goal, dict):
                tr = carla.Transform(
                    carla.Location(
                        x=float(explicit_goal.get("x", 0.0) or 0.0),
                        y=float(explicit_goal.get("y", 0.0) or 0.0),
                        z=float(explicit_goal.get("z", 0.0) or 0.0),
                    ),
                    carla.Rotation(yaw=float(explicit_goal.get("yaw_deg", 0.0) or 0.0)),
                )
                target_actor_id = "config"
            else:
                tr = _stable_actor_transform(target_actor, fallback_transform=fallback_tf)
                target_actor_id = getattr(target_actor, "id", "<unknown>")
            ego_tr_for_aw = _stable_actor_transform(ego, fallback_transform=ego_fallback_tf)
            loc = tr.location
            yaw = tr.rotation.yaw
            aw_tf_cfg = aw_cfg.get("carla_to_autoware", {}) or {}
            invert_y = bool(aw_tf_cfg.get("invert_y", True))
            invert_yaw = bool(aw_tf_cfg.get("invert_yaw", True))
            goal_pose = _autoware_pose_from_carla_transform(
                tr,
                ahead_m=ahead_m,
                invert_y=invert_y,
                invert_yaw=invert_yaw,
            )
            initial_pose = _autoware_pose_from_carla_transform(
                ego_tr_for_aw,
                ahead_m=0.0,
                invert_y=invert_y,
                invert_yaw=invert_yaw,
            )
            if (abs(float(goal_pose["x"])) + abs(float(goal_pose["y"]))) <= 1e-3:
                print(
                    "[WARN] Autoware goal is near map origin after spawn-settle; "
                    "routing may be invalid. Check scenario spawn metadata."
                )
            print(
                "[goal] Autoware target from actor_id=%s loc=(%.3f, %.3f, %.3f) "
                "yaw=%.3f ahead_m=%.3f -> carla_goal=(%.3f, %.3f, %.3f) "
                "autoware_goal=(%.3f, %.3f, %.3f yaw=%.3f)"
                % (
                    target_actor_id,
                    float(loc.x),
                    float(loc.y),
                    float(loc.z),
                    float(yaw),
                    float(ahead_m),
                    float(goal_pose["source_carla_x"]),
                    float(goal_pose["source_carla_y"]),
                    float(goal_pose["z"]),
                    float(goal_pose["x"]),
                    float(goal_pose["y"]),
                    float(goal_pose["z"]),
                    float(goal_pose["yaw_deg"]),
                )
            )
            print(
                "[localization] Autoware initial pose from ego loc=(%.3f, %.3f, %.3f) "
                "yaw=%.3f -> autoware_initial=(%.3f, %.3f, %.3f yaw=%.3f)"
                % (
                    float(ego_tr_for_aw.location.x),
                    float(ego_tr_for_aw.location.y),
                    float(ego_tr_for_aw.location.z),
                    float(ego_tr_for_aw.rotation.yaw),
                    float(initial_pose["x"]),
                    float(initial_pose["y"]),
                    float(initial_pose["z"]),
                    float(initial_pose["yaw_deg"]),
                )
            )
            pre_goal_warmup_ticks = int(goal_cfg.get("pre_goal_warmup_ticks", 0) or 0)
            pre_goal_warmup_sleep_s = float(goal_cfg.get("pre_goal_warmup_sleep_s", 0.0) or 0.0)
            if pre_goal_warmup_ticks > 0:
                print(f"[goal] pre-goal CARLA warmup ticks={pre_goal_warmup_ticks}")
                for _ in range(pre_goal_warmup_ticks):
                    try:
                        # The external bridge needs committed CARLA frames
                        # before localization/tf waits start. An active tick
                        # is safer than wait_for_tick here because there may be
                        # no other tick owner before harness.run().
                        world.tick()
                    except Exception as exc:
                        if not cfg.synchronous_mode:
                            try:
                                world.wait_for_tick()
                                if pre_goal_warmup_sleep_s > 0.0:
                                    time.sleep(pre_goal_warmup_sleep_s)
                                continue
                            except Exception:
                                pass
                        print(f"[WARN] pre-goal warmup tick failed: {exc}")
                        break
                    if pre_goal_warmup_sleep_s > 0.0:
                        time.sleep(pre_goal_warmup_sleep_s)
            tick_pump = None
            tick_while_waiting = bool(goal_cfg.get("tick_while_waiting_for_route", True))
            if tick_while_waiting and cfg.synchronous_mode:
                tick_pump = _start_carla_tick_pump(
                    world,
                    label="autoware_route_wait",
                    tick_timeout_s=float(goal_cfg.get("route_wait_tick_timeout_s", 5.0) or 5.0),
                    sleep_s=float(goal_cfg.get("route_wait_tick_sleep_s", 0.0) or 0.0),
                )
                print("[goal] started CARLA tick pump while waiting for Autoware localization/tf/route")
            try:
                    result = send_goal_and_engage(
                        ros2_runner,
                        goal_pose,
                        frame_id=frame_id,
                        publish_goal_topic_after_route_service=bool(
                            goal_cfg.get("publish_goal_topic_after_route_service", True)
                        ),
                        initial_pose=initial_pose,
                    wait_timeout_s=float(goal_cfg.get("wait_timeout_s", 30.0) or 30.0),
                    localization_wait_s=float(goal_cfg.get("localization_wait_s", 12.0) or 12.0),
                    tf_wait_s=float(goal_cfg.get("tf_wait_s", 8.0) or 8.0),
                    localization_retry_wait_s=float(
                        goal_cfg.get("localization_retry_wait_s", 15.0) or 15.0
                    ),
                    tf_retry_wait_s=float(goal_cfg.get("tf_retry_wait_s", 15.0) or 15.0),
                    route_ready_wait_s=(
                        float(goal_cfg["route_ready_wait_s"])
                        if goal_cfg.get("route_ready_wait_s") is not None
                        else None
                    ),
                    engage_retry_timeout_s=float(
                        goal_cfg.get("engage_retry_timeout_s", 0.0) or 0.0
                    ),
                    engage_retry_period_s=float(
                        goal_cfg.get("engage_retry_period_s", 2.0) or 2.0
                    ),
                    engage_ready_topics=list(goal_cfg.get("engage_ready_topics", []) or []),
                    engage_ready_wait_s=(
                        float(goal_cfg["engage_ready_wait_s"])
                        if goal_cfg.get("engage_ready_wait_s") is not None
                        else None
                    ),
                    engage_ready_probe_timeout_s=float(
                        goal_cfg.get("engage_ready_probe_timeout_s", 5.0) or 5.0
                    ),
                    require_engage_ready_topics=bool(
                        goal_cfg.get("require_engage_ready_topics", False)
                    ),
                    require_localization_before_route=bool(
                        goal_cfg.get("require_localization_before_route", False)
                    ),
                    log_path=goal_log_path,
                )
            finally:
                if tick_pump is not None:
                    stop_event, thread, stats = tick_pump
                    _stop_carla_tick_pump(stop_event, thread, stats, label="autoware_route_wait")
            print(
                "[goal] sent=%s route_set=%s localization_initialized=%s "
                "subscriber_ready=%s subscriber_count=%d engage=%s"
                % (
                    result.goal_sent,
                    result.route_set,
                    result.localization_initialized,
                    result.goal_subscriber_ready,
                    result.goal_subscriber_count,
                    result.engage_succeeded,
                )
            )
            if bool(goal_cfg.get("require_engage_ready_topics", False)) and not result.engage_succeeded:
                fail_reason = "AUTOWARE_PRE_ENGAGE_READINESS_FAILED"
                goal_status = {
                    "schema_version": "autoware_goal_status.v1",
                    "success": False,
                    "fail_reason": fail_reason,
                    "goal_sent": bool(result.goal_sent),
                    "route_set": bool(result.route_set),
                    "localization_initialized": bool(result.localization_initialized),
                    "goal_subscriber_ready": bool(result.goal_subscriber_ready),
                    "goal_subscriber_count": int(result.goal_subscriber_count),
                    "engage_succeeded": bool(result.engage_succeeded),
                    "pre_engage_ready": result.pre_engage_ready,
                    "required_engage_ready_topics": list(goal_cfg.get("engage_ready_topics", []) or []),
                    "missing_engage_ready_topics": list(result.missing_engage_ready_topics or []),
                    "log_path": str(goal_log_path),
                }
                artifacts_dir.mkdir(parents=True, exist_ok=True)
                (artifacts_dir / "autoware_goal_status.json").write_text(
                    json.dumps(goal_status, indent=2)
                )
                summary = {
                    "success": False,
                    "exit_reason": fail_reason,
                    "fail_reason": fail_reason,
                    "frames": 0,
                    "scenario_driver": scenario_driver,
                    "adapter_started": adapter_started,
                    "adapter_fail_reason": adapter_fail_reason,
                    "autoware_goal_status": goal_status,
                    "metrics": {
                        "frames": 0,
                        "exit_reason": fail_reason,
                        "collision_count": 0,
                        "lane_invasion_count": 0,
                    },
                }
                (out_run_dir / "summary.json").write_text(json.dumps(summary, indent=2))
                print(f"[goal][fail-fast] {fail_reason}; wrote {out_run_dir / 'summary.json'}")
                return

        run_mode_cfg = effective_cfg.get("run", {}) if effective_cfg else {}
        external_probe_hold = bool(run_mode_cfg.get("external_probe_hold", False))
        external_probe_timeout_sec = float(run_mode_cfg.get("external_probe_timeout_sec", 1800.0) or 1800.0)
        external_probe_heartbeat_sec = float(run_mode_cfg.get("external_probe_heartbeat_sec", 15.0) or 15.0)
        external_probe_warmup_ticks = int(run_mode_cfg.get("external_probe_warmup_ticks", 5) or 5)
        if external_probe_hold:
            if cfg.synchronous_mode:
                for _ in range(max(1, external_probe_warmup_ticks)):
                    try:
                        world.tick()
                    except Exception as exc:
                        print(f"[WARN] external probe warmup tick failed: {exc}")
                        break
            summary_path = out_run_dir / "summary.json"
            hold_summary = {
                "success": True,
                "fail_reason": None,
                "mode": "external_probe_hold",
                "timeout_sec": external_probe_timeout_sec,
                "scenario_driver": scenario_driver,
                "ego_actor_id": int(getattr(ego, "id", 0) or 0),
                "front_actor_id": int(getattr(front, "id", 0) or 0) if front is not None else 0,
            }
            summary_path.write_text(json.dumps(hold_summary, indent=2))
            print(
                "[external_probe_hold] ready "
                f"ego_actor_id={hold_summary['ego_actor_id']} timeout_sec={external_probe_timeout_sec:.1f}"
            )
            deadline = time.time() + max(1.0, external_probe_timeout_sec)
            next_heartbeat = time.time() + max(5.0, external_probe_heartbeat_sec)
            while time.time() < deadline:
                if time.time() >= next_heartbeat:
                    remain = max(0.0, deadline - time.time())
                    print(
                        "[external_probe_hold] waiting "
                        f"remain={remain:.0f}s run_dir={out_run_dir}"
                    )
                    next_heartbeat = time.time() + max(5.0, external_probe_heartbeat_sec)
                if cfg.synchronous_mode:
                    try:
                        world.tick()
                    except Exception as exc:
                        print(f"[WARN] external probe hold tick failed: {exc}")
                        time.sleep(1.0)
                else:
                    time.sleep(1.0)
            print("[external_probe_hold] timeout reached, exiting")
            return

        ctrl_cfg = LegacyControllerConfig(
            lateral_mode=args.lateral_mode,
            policy_mode=args.policy_mode,
            controller_mode=args.controller,
            agent_type=args.agent_type,
            takeover_dist_m=args.takeover_dist,
            blend_time_s=args.blend_time,
        )
        # rig loading
        preset_dir = Path(__file__).resolve().parents[1] / "configs" / "rigs"
        rig_raw = load_rig_preset(args.rig, preset_dir)
        if args.rig_file:
            rig_raw = load_rig_file(args.rig_file)
        rig_final = apply_overrides(rig_raw, args.rig_override)
        rig_label = args.rig if not args.rig_file else Path(args.rig_file).stem
        sensor_specs, events_cfg = rig_to_specs(rig_final)
        if effective_cfg.get("record", {}).get("sensors", {}).get("enable") is False:
            sensor_specs = None
            rig_final = {**(rig_final or {}), "sensors": []}
        rviz_launcher = None
        if args.enable_ros2_native and args.enable_rviz:
            rviz_rig_path = out_run_dir / "config" / f"{rig_label}_rviz.yaml"
            rviz_rig_path.parent.mkdir(parents=True, exist_ok=True)
            rviz_rig_path.write_text(yaml.safe_dump(rig_final))
            rviz_launcher = RvizLauncher(
                rig_path=rviz_rig_path,
                ego_id=rviz_ego,
                domain_id=args.rviz_domain,
                mode=args.rviz_mode,
                docker_image=args.rviz_docker_image,
                camera_suffix=args.rviz_camera_image_suffix,
                lidar_suffix=args.rviz_lidar_cloud_suffix,
            )
        bag_cfg = None
        if args.enable_ros2_native and args.enable_ros2_bag:
            bag_out = args.ros2_bag_out or (out_run_dir / "ros2_bag" / "bag")
            topics = []
            explicit = args.ros2_bag_topics.split(",") if args.ros2_bag_topics else None
            extras = args.ros2_bag_extra_topics.split(",") if args.ros2_bag_extra_topics else None
            topics = build_ros2_bag_topics(
                rig_final=rig_final,
                ego_id=args.ego_id,
                namespace=args.ros2_namespace,
                camera_suffix=args.ros2_bag_camera_image_suffix,
                lidar_suffix=args.ros2_bag_lidar_cloud_suffix,
                radar_suffix=args.ros2_bag_radar_cloud_suffix,
                include_tf=args.ros2_bag_include_tf,
                include_clock=args.ros2_bag_include_clock,
                auto_topics=args.ros2_bag_auto_topics,
                explicit_topics=explicit,
                extra_topics=extras,
            )
            print(f"[ROS2 bag] topics: {topics}")
            bag_cfg = {
                "out_path": bag_out,
                "topics": topics,
                "storage": args.ros2_bag_storage,
                "compress": args.ros2_bag_compress,
                "include_tf": args.ros2_bag_include_tf,
                "include_clock": args.ros2_bag_include_clock,
                "max_size_mb": args.ros2_bag_max_size_mb,
                "max_duration_s": args.ros2_bag_max_duration_s,
                "env": None,
                "log_path": out_run_dir / "logs" / "ros2_bag.log",
            }
        # Prepare record manager (run_dir not created until run)
        record_opts = RecordOptions(
            modes=record_modes,
            output_dir=args.record_output or None,
            fps=args.record_fps,
            resolution=record_resolution,
            chase_distance=args.record_chase_distance,
            chase_height=args.record_chase_height,
            chase_pitch=args.record_chase_pitch,
            max_lidar_points=args.record_max_lidar_points,
            keep_frames=args.record_keep_frames,
            skip_lidar=args.record_no_lidar,
            skip_radar=args.record_no_radar,
            skip_hud=args.record_no_hud,
            hud_mode=args.record_hud_mode,
            hud_col_width=args.record_hud_col_width,
            dual_cam_third_person_only=bool(args.record_dual_cam_third_person_only),
        )
        record_mgr = RecordManager(
            run_dir=out_run_dir,
            rig_resolved=rig_final,
            config_paths={},  # filled inside harness via cfg outputs
            opts=record_opts,
        ) if record_modes else None

        sensor_capture_enabled = True
        if effective_cfg.get("record", {}).get("sensors", {}).get("enable") is False:
            sensor_capture_enabled = False
        external_stack = stack in {"autoware", "apollo"}
        apollo_cfg_effective = ((effective_cfg.get("algo", {}) or {}).get("apollo", {}) or {})
        apollo_direct_no_ros2 = bool(
            stack == "apollo"
            and str(apollo_cfg_effective.get("transport_mode") or "").strip() == "carla_direct"
            and bool((apollo_cfg_effective.get("direct_bridge", {}) or {}).get("require_no_ros2_runtime", False))
        )
        # External stacks (Apollo/Autoware) should usually own control output.
        # Keep this behavior explicit and configurable for ablation experiments.
        disable_control_cfg = ((effective_cfg.get("algo", {}) or {}).get(
            "disable_legacy_harness_control_for_external_stack"
        ))
        disable_control_cfg_effective = bool(external_stack) if disable_control_cfg is None else bool(disable_control_cfg)
        disable_control = disable_control_cfg_effective
        ros2_mode_active = bool(
            args.enable_ros2_native
            or args.enable_ros2_gt
            or (external_stack and adapter_started and not apollo_direct_no_ros2)
        )
        tick_callbacks = []
        scenario_tick_hook = getattr(scenario, "on_sim_tick", None)
        if callable(scenario_tick_hook):
            tick_callbacks.append(scenario_tick_hook)
            print(f"[scenario] tick hook enabled for driver={scenario_driver}")
        if stack == "apollo" and adapter_started and adapter is not None:
            backend_obj = getattr(adapter, "backend", None)
            tick_hook = getattr(backend_obj, "on_sim_tick", None) if backend_obj is not None else None
            if callable(tick_hook):
                tick_callbacks.append(tick_hook)
                print("[apollo] dreamview capture hooked to simulation ticks")

        # optional: start control logger via shared ROS2 runner
        control_log_cfg = effective_cfg.get("record", {}).get("control_log", {}) if effective_cfg else {}
        control_logger_proc = None
        default_control_topic = "/control/command/control_cmd" if stack == "autoware" else "/tb/ego/control_cmd"
        control_log_enable_default = bool(stack in {"autoware", "apollo"})
        if ros2_mode_active and control_log_cfg.get("enable", control_log_enable_default):
            topic = control_log_cfg.get("topic", default_control_topic)
            max_msgs = control_log_cfg.get("max_msgs")
            reliability = control_log_cfg.get("reliability", "best_effort")
            topic_type = control_log_cfg.get("topic_type")
            if not topic_type and stack == "apollo":
                control_out_type = (
                    ((effective_cfg.get("algo", {}) or {}).get("apollo", {}) or {}).get("control_out_type", "direct")
                )
                if control_out_type == "direct":
                    topic_type = "std_msgs/msg/Float32MultiArray"
                elif control_out_type == "twist":
                    topic_type = "geometry_msgs/msg/Twist"
                else:
                    topic_type = "ackermann_msgs/msg/AckermannDriveStamped"
            if reliability not in ["best_effort", "reliable"]:
                reliability = "best_effort"
            force_anymsg = control_log_cfg.get("force_anymsg", True)
            out_log = out_run_dir / "artifacts" / "autoware_control.jsonl"
            out_err = out_run_dir / "artifacts" / "autoware_control.log"
            try:
                control_logger_proc = start_control_logger(
                    ros2_runner,
                    TESTBED_ROOT,
                    topic=topic,
                    topic_type=topic_type,
                    out_jsonl=out_log,
                    out_log=out_err,
                    max_msgs=max_msgs,
                    force_anymsg=force_anymsg,
                    reliability=reliability,
                )
                cleanup_state["control_logger_proc"] = control_logger_proc
                print(f"[monitor] control logger started for {topic}, writing to {out_log}")
            except Exception as exc:
                print(f"[WARN] failed to start control logger: {exc}")
            extra_control_topics = [
                str(t).strip()
                for t in (control_log_cfg.get("extra_topics") or [])
                if str(t).strip() and str(t).strip() != str(topic)
            ]
            extra_topic_types = control_log_cfg.get("extra_topic_types") or {}
            for extra_topic in extra_control_topics:
                safe_topic = extra_topic.strip("/").replace("/", "__").replace("~", "_")
                out_extra = out_run_dir / "artifacts" / f"ros2_topic__{safe_topic}.jsonl"
                out_extra_log = out_run_dir / "artifacts" / f"ros2_topic__{safe_topic}.log"
                try:
                    proc = start_control_logger(
                        ros2_runner,
                        TESTBED_ROOT,
                        topic=extra_topic,
                        topic_type=extra_topic_types.get(extra_topic),
                        out_jsonl=out_extra,
                        out_log=out_extra_log,
                        max_msgs=max_msgs,
                        force_anymsg=force_anymsg,
                        reliability=reliability,
                    )
                    cleanup_state.setdefault("control_logger_extra_procs", []).append(proc)
                    print(f"[monitor] extra topic logger started for {extra_topic}, writing to {out_extra}")
                except Exception as exc:
                    print(f"[WARN] failed to start extra topic logger for {extra_topic}: {exc}")

        # optional: topic probe via shared ROS2 runner
        probe_cfg = None
        if effective_cfg:
            rec = effective_cfg.get("record", {}) or {}
            probe_cfg = rec.get("probe") or rec.get("sensor_probe") or {}
        else:
            probe_cfg = {}
        sensor_probe_proc = None
        probe_topics_effective: List[str] = []
        if probe_cfg.get("enable", True) and ros2_mode_active:
            control_topic_default = control_log_cfg.get("topic", default_control_topic)
            probe_topics = [t for t in (probe_cfg.get("topics") or []) if t] or default_probe_topics(
                stack=stack,
                ego_id=args.ego_id,
                rig_final=rig_final,
                control_topic=control_topic_default,
                namespace=args.ros2_namespace,
            )
            probe_topics_effective = probe_topics
            if probe_topics:
                max_msgs = probe_cfg.get("max_msgs", 5)
                out_probe = out_run_dir / "artifacts" / "sensor_probe.json"
                out_probe_log = out_run_dir / "artifacts" / "sensor_probe.log"
                try:
                    typed_topics = infer_topic_types(probe_topics)
                    sensor_probe_proc = start_topic_probe(
                        ros2_runner,
                        TESTBED_ROOT,
                        topics=probe_topics,
                        out_json=out_probe,
                        out_log=out_probe_log,
                        max_msgs=max_msgs,
                        discover_prefixes=[args.ros2_namespace] if args.enable_ros2_native else None,
                        typed_topics=typed_topics,
                    )
                    cleanup_state["sensor_probe_proc"] = sensor_probe_proc
                    print(f"[monitor] sensor probe started for {len(probe_topics)} topics, writing to {out_probe}")
                except Exception as exc:
                    print(f"[WARN] failed to start sensor probe: {exc}")

        state, summary = harness.run(
            world=world,
            carla_map=world.get_map(),
            ego=ego,
            front=front,
            controller_cfg=ctrl_cfg,
            out_dir=out_run_dir,
            sensor_specs=sensor_specs,
            rig_raw=rig_raw,
            rig_final=rig_final,
            rig_name=args.rig if not args.rig_file else Path(args.rig_file).name,
            events_cfg=events_cfg,
            enable_fail_capture=args.enable_fail_capture,
            record_manager=record_mgr,
            client=client,
            rviz_launcher=rviz_launcher,
            bag_recorder_cfg=bag_cfg,
            monitor=monitor,
            disable_control=disable_control,
            sensor_capture_enabled=sensor_capture_enabled,
            tick_callbacks=tick_callbacks or None,
            scenario_metadata=scenario_meta,
        )
        print(
            f"Run core finished: harness_success={summary['success']} "
            f"harness_fail_reason={summary['fail_reason']} collisions={summary['collision_count']}"
        )
        try:
            latest_scenario_meta = scenario.metadata() if hasattr(scenario, "metadata") else {}
            if not isinstance(latest_scenario_meta, dict):
                latest_scenario_meta = {}
            latest_scenario_meta.update(
                {
                    "scenario_driver": scenario_driver,
                    "ego_role": args.ego_id,
                    "front_role": "front",
                    "ego_actor_id": int(getattr(ego, "id", 0) or 0),
                    "front_actor_id": int(getattr(front, "id", 0) or 0) if front is not None else 0,
                }
            )
            scenario_meta = json.loads(json.dumps(latest_scenario_meta, default=str))
            scenario_meta_path.parent.mkdir(parents=True, exist_ok=True)
            scenario_meta_path.write_text(json.dumps(scenario_meta, indent=2))
            manifest_path = out_run_dir / "manifest.json"
            if manifest_path.exists():
                manifest_data = _load_json_if_exists(manifest_path)
                manifest_metadata = manifest_data.get("metadata")
                if not isinstance(manifest_metadata, dict):
                    manifest_metadata = {}
                manifest_metadata["scenario_metadata"] = scenario_meta
                manifest_data["metadata"] = manifest_metadata
                if isinstance(scenario_meta.get("traffic_light_control"), dict):
                    manifest_data["traffic_light_control"] = scenario_meta["traffic_light_control"]
                manifest_path.write_text(json.dumps(manifest_data, indent=2))
        except Exception as exc:
            print(f"[WARN] failed to refresh scenario metadata after run: {exc}")

        summary_path = out_run_dir / "summary.json"
        provisional_summary_path = out_run_dir / "summary.provisional.json"
        summary_data = summary
        try:
            if summary_path.exists():
                summary_data = json.loads(summary_path.read_text())
        except Exception as exc:
            print(f"[WARN] failed to read summary.json for augmentation: {exc}")

        control_log_path = out_run_dir / "artifacts" / "autoware_control.jsonl"
        control_log_ok = False
        control_log_size = control_log_path.stat().st_size if control_log_path.exists() else 0
        if control_log_path.exists() and control_log_size > 0:
            try:
                for line in control_log_path.read_text().splitlines():
                    try:
                        rec = json.loads(line)
                    except Exception:
                        continue
                    raw_len = rec.get("raw_len", 0)
                    # control_logger may dump parsed ROS2 messages without raw_len.
                    if isinstance(rec, dict) and rec and "raw_len" not in rec:
                        control_log_ok = True
                        break
                    if isinstance(raw_len, (int, float)) and raw_len > 0:
                        control_log_ok = True
                        break
            except Exception as exc:
                print(f"[WARN] failed to parse control log: {exc}")

        sensor_probe_path = out_run_dir / "artifacts" / "sensor_probe.json"
        sensor_probe_ok = False
        clock_count = 0
        tf_count = None
        ros2_sensor_topic_count = 0
        ros2_sensor_msgs = 0
        ros2_sensor_ok = False
        ros2_invalid_topics: List[str] = []
        if sensor_probe_path.exists():
            try:
                probe_assess = assess_probe_results(
                    sensor_probe_path,
                    probe_topics_effective,
                    args.ego_id,
                    namespace=args.ros2_namespace,
                )
                sensor_probe_ok = probe_assess.sensor_probe_ok
                clock_count = probe_assess.clock_count
                tf_count = probe_assess.tf_count
                ros2_sensor_topic_count = probe_assess.ros2_sensor_topic_count
                ros2_sensor_msgs = probe_assess.ros2_sensor_msgs
                ros2_sensor_ok = probe_assess.ros2_sensor_ok
                raw_probe = json.loads(sensor_probe_path.read_text())
                ros2_invalid_topics = ((raw_probe.get("_meta") or {}).get("invalid_discovered_topics") or [])
            except Exception as exc:
                print(f"[WARN] failed to parse sensor probe: {exc}")
        motion_ok = state.max_speed_mps > acceptance_speed_threshold
        require_control_log = bool(stack == "autoware" or (stack == "apollo" and not apollo_direct_no_ros2))
        require_sensor_probe = bool(stack == "autoware" or (stack == "apollo" and not apollo_direct_no_ros2))
        ros2_enable_api_available = bool(
            hasattr(carla.Actor, "enable_for_ros")
            or hasattr(carla.Sensor, "enable_for_ros")
            or hasattr(carla.Vehicle, "enable_for_ros")
        )

        failures = []
        failure_details: List[Dict[str, Any]] = []
        for safety_failure in safety_failures_from_summary(summary_data):
            failures.append(safety_failure.code)
            failure_details.append(safety_failure.detail)
        control_health_check = applied_control_health_from_timeseries(
            out_run_dir / "timeseries.csv",
            max_throttle_brake_switch_count=acceptance_control_health_max_switch_count,
        )
        for control_health_failure in control_health_failures_from_check(
            control_health_check,
            enabled=acceptance_control_health_enabled,
            fail_on_missing=acceptance_control_health_fail_on_missing,
        ):
            failures.append(control_health_failure.code)
            failure_details.append(control_health_failure.detail)
        bridge_target_speed_check = bridge_target_speed_health_from_log(
            out_run_dir / "artifacts" / "autoware_carla_control_bridge.log",
            overspeed_threshold_mps=acceptance_target_speed_tracking_overspeed_threshold_mps,
            max_throttle_while_overspeed_rows=acceptance_target_speed_tracking_max_throttle_rows,
        )
        for bridge_target_speed_failure in bridge_target_speed_failures_from_check(
            bridge_target_speed_check,
            enabled=acceptance_target_speed_tracking_enabled,
            fail_on_missing=acceptance_target_speed_tracking_fail_on_missing,
        ):
            failures.append(bridge_target_speed_failure.code)
            failure_details.append(bridge_target_speed_failure.detail)
        followstop_acceptance_check: Dict[str, Any] = {
            "enabled": acceptance_followstop_enabled,
            "available": False,
        }
        if acceptance_followstop_enabled:
            try:
                followstop_report = analyze_followstop_run(
                    out_run_dir,
                    min_ahead_m=float(args.front_min_ahead_m),
                    max_ahead_m=float(args.front_max_ahead_m),
                    max_lateral_m=float(args.front_max_lateral_m),
                    max_heading_diff_deg=float(args.front_max_heading_diff_deg),
                    stop_zone_m=float(acceptance_followstop_stop_zone_m),
                    stopped_speed_mps=float(acceptance_followstop_stopped_speed_mps),
                )
                followstop_paths = write_followstop_report(
                    followstop_report,
                    out_run_dir / "analysis" / "autoware_followstop",
                )
                followstop_status = str(followstop_report.get("status") or "insufficient_data")
                followstop_reasons = list(followstop_report.get("failure_reasons") or [])
                followstop_acceptance_check = {
                    "enabled": True,
                    "available": True,
                    "ok": followstop_status == "pass",
                    "status": followstop_status,
                    "failure_reasons": followstop_reasons,
                    "json_path": followstop_paths.get("json"),
                    "stop_zone_m": float(acceptance_followstop_stop_zone_m),
                    "stopped_speed_mps": float(acceptance_followstop_stopped_speed_mps),
                    "fail_on_warn": acceptance_followstop_fail_on_warn,
                }
                followstop_should_fail = followstop_status in {"fail", "insufficient_data"} or (
                    acceptance_followstop_fail_on_warn and followstop_status == "warn"
                )
                if followstop_should_fail:
                    failures.append("FOLLOWSTOP_NOT_VALID")
                    failure_details.append(
                        {
                            "code": "FOLLOWSTOP_NOT_VALID",
                            "scope": "scenario_behavior",
                            "status": followstop_status,
                            "failure_reasons": followstop_reasons,
                            "path": followstop_paths.get("json"),
                        }
                    )
            except Exception as exc:
                followstop_acceptance_check = {
                    "enabled": True,
                    "available": False,
                    "ok": False,
                    "error": str(exc),
                }
                failures.append("FOLLOWSTOP_DIAGNOSTICS_FAILED")
                failure_details.append(
                    {
                        "code": "FOLLOWSTOP_DIAGNOSTICS_FAILED",
                        "scope": "scenario_behavior",
                        "error": str(exc),
                    }
                )
        autoware_control_diagnostics_check: Dict[str, Any] = {
            "enabled": stack == "autoware",
            "available": False,
        }
        if stack == "autoware":
            try:
                autoware_control_report = analyze_autoware_control_run(out_run_dir)
                autoware_control_out = out_run_dir / "analysis" / "autoware_control"
                write_autoware_control_diagnostics(autoware_control_report, autoware_control_out)
                autoware_control_verdict = autoware_control_report.get("verdict") or {}
                autoware_control_diagnostics_check = {
                    "enabled": True,
                    "available": True,
                    "status": autoware_control_verdict.get("status"),
                    "failure_reasons": autoware_control_verdict.get("failure_reasons") or [],
                    "json_path": str(autoware_control_out / "autoware_control_diagnostics.json"),
                }
            except Exception as exc:
                autoware_control_diagnostics_check = {
                    "enabled": True,
                    "available": False,
                    "error": str(exc),
                }
                print(f"[WARN] failed to write Autoware control diagnostics: {exc}")
        if adapter_fail_reason and not adapter_started:
            failures.append(adapter_fail_reason)
            failure_details.append({"code": adapter_fail_reason, "scope": "adapter"})
        if require_control_log and not control_log_ok:
            failures.append("NO_CONTROL_LOG")
            failure_details.append({"code": "NO_CONTROL_LOG", "scope": "transport"})
        if require_sensor_probe and not sensor_probe_ok:
            failures.append("NO_SENSOR_DATA")
            failure_details.append({"code": "NO_SENSOR_DATA", "scope": "transport"})
        if ros2_mode_active and not ros2_sensor_ok:
            if args.enable_ros2_native and not ros2_enable_api_available:
                failures.append("ROS2_NATIVE_PY_API_UNAVAILABLE")
                failure_details.append(
                    {"code": "ROS2_NATIVE_PY_API_UNAVAILABLE", "scope": "transport"}
                )
            else:
                failures.append("ROS2_PUBLISH_NO_SENSOR_MSG")
                failure_details.append({"code": "ROS2_PUBLISH_NO_SENSOR_MSG", "scope": "transport"})
        if not motion_ok:
            failures.append("EGO_NOT_MOVING")
            failure_details.append({"code": "EGO_NOT_MOVING", "scope": "motion"})

        apollo_bridge_health_path = out_run_dir / "artifacts" / "bridge_health_summary.json"
        apollo_planning_summary_path = out_run_dir / "artifacts" / "planning_topic_debug_summary.json"
        apollo_cyber_bridge_stats_path = out_run_dir / "artifacts" / "cyber_bridge_stats.json"
        apollo_debug_timeseries_path = out_run_dir / "artifacts" / "debug_timeseries.csv"

        apollo_bridge_health = _load_json_if_exists(apollo_bridge_health_path)
        apollo_planning_summary = _load_json_if_exists(apollo_planning_summary_path)
        apollo_cyber_bridge_stats = _load_json_if_exists(apollo_cyber_bridge_stats_path)

        apollo_health_artifacts_ok = all(
            path.exists()
            for path in (
                apollo_bridge_health_path,
                apollo_planning_summary_path,
                apollo_cyber_bridge_stats_path,
            )
        )
        planning_nonempty_count = _safe_int(
            apollo_bridge_health.get("planning_nonempty_trajectory_count")
        )
        if planning_nonempty_count is None:
            planning_nonempty_count = _safe_int(
                apollo_planning_summary.get("messages_with_nonzero_trajectory_points")
            )
        planning_total_messages = _safe_int(apollo_planning_summary.get("total_messages_received")) or 0
        planning_nonzero_ratio: Optional[float] = None
        if planning_total_messages > 0 and planning_nonempty_count is not None:
            planning_nonzero_ratio = float(planning_nonempty_count) / float(planning_total_messages)
        invalid_goal_count = _safe_int(apollo_bridge_health.get("invalid_goal_count"))
        if invalid_goal_count is None:
            invalid_goal_count = _safe_int(apollo_cyber_bridge_stats.get("invalid_goal_count"))
        routing_invalid_goal_skip_count = _safe_int(
            apollo_cyber_bridge_stats.get("routing_skipped_due_to_invalid_goal_count")
        )
        routing_success_count = _safe_int(apollo_cyber_bridge_stats.get("routing_success_count"))
        last_goal_validity = apollo_bridge_health.get("goal_validity_last") or {}
        low_speed_creep_metrics = (
            _compute_low_speed_creep_metrics(
                apollo_debug_timeseries_path,
                start_after_sec=acceptance_low_speed_creep_start_after_sec,
                speed_below_mps=acceptance_low_speed_creep_speed_below_mps,
                front_gap_above_m=acceptance_low_speed_creep_front_gap_above_m,
                require_routing_established=acceptance_low_speed_creep_require_routing_established,
                ignore_when_terminal_stop_hold_active=acceptance_low_speed_creep_ignore_terminal_hold,
                require_reached_speed_mps=acceptance_low_speed_creep_require_reached_speed_mps,
            )
            if acceptance_low_speed_creep_enabled
            else {
                "available": apollo_debug_timeseries_path.exists(),
                "path": str(apollo_debug_timeseries_path),
                "enabled": False,
            }
        )
        lateral_metrics = _compute_basic_lateral_metrics(
            out_run_dir / "artifacts" / "bridge_control_decode.jsonl"
        )
        scenario_meta = {}
        try:
            scenario_meta_path = out_run_dir / "artifacts" / "scenario_metadata.json"
            if scenario_meta_path.exists():
                scenario_meta = json.loads(scenario_meta_path.read_text(encoding="utf-8"))
                if not isinstance(scenario_meta, dict):
                    scenario_meta = {}
        except Exception:
            scenario_meta = {}
        route_health_metrics = (
            _compute_route_health_metrics(
                scenario_meta,
                ego,
                routing_success_count=routing_success_count,
            )
            if scenario_driver == "carla_town01_route_health"
            else {"available": False}
        )

        if stack == "apollo":
            if acceptance_require_apollo_health_artifacts and not apollo_health_artifacts_ok:
                failures.append("MISSING_APOLLO_HEALTH_ARTIFACTS")
                failure_details.append(
                    {
                        "code": "MISSING_APOLLO_HEALTH_ARTIFACTS",
                        "scope": "apollo_health",
                        "path": str(apollo_bridge_health_path.parent),
                    }
                )
            if (
                acceptance_min_planning_nonempty is not None
                and planning_nonempty_count is not None
                and planning_nonempty_count < acceptance_min_planning_nonempty
            ):
                failures.append("APOLLO_PLANNING_NONEMPTY_TOO_LOW")
                failure_details.append(
                    {
                        "code": "APOLLO_PLANNING_NONEMPTY_TOO_LOW",
                        "scope": "apollo_health",
                        "actual": planning_nonempty_count,
                        "threshold": acceptance_min_planning_nonempty,
                    }
                )
            if (
                acceptance_min_planning_nonzero_ratio is not None
                and planning_nonzero_ratio is not None
                and planning_nonzero_ratio < acceptance_min_planning_nonzero_ratio
            ):
                failures.append("APOLLO_PLANNING_NONZERO_RATIO_TOO_LOW")
                failure_details.append(
                    {
                        "code": "APOLLO_PLANNING_NONZERO_RATIO_TOO_LOW",
                        "scope": "apollo_health",
                        "actual": planning_nonzero_ratio,
                        "threshold": acceptance_min_planning_nonzero_ratio,
                    }
                )
            if (
                acceptance_max_invalid_goal_count is not None
                and invalid_goal_count is not None
                and invalid_goal_count > acceptance_max_invalid_goal_count
            ):
                failures.append("APOLLO_INVALID_GOAL_EXCESSIVE")
                failure_details.append(
                    {
                        "code": "APOLLO_INVALID_GOAL_EXCESSIVE",
                        "scope": "apollo_routing",
                        "actual": invalid_goal_count,
                        "threshold": acceptance_max_invalid_goal_count,
                        "last_invalid_goal_reason": last_goal_validity.get("invalid_goal_reason"),
                    }
                )
            if (
                acceptance_max_routing_invalid_goal_skip_count is not None
                and routing_invalid_goal_skip_count is not None
                and routing_invalid_goal_skip_count > acceptance_max_routing_invalid_goal_skip_count
            ):
                failures.append("APOLLO_INVALID_LONG_ROUTE_SKIP_EXCESSIVE")
                failure_details.append(
                    {
                        "code": "APOLLO_INVALID_LONG_ROUTE_SKIP_EXCESSIVE",
                        "scope": "apollo_routing",
                        "actual": routing_invalid_goal_skip_count,
                        "threshold": acceptance_max_routing_invalid_goal_skip_count,
                    }
                )
            if (
                acceptance_low_speed_creep_enabled
                and acceptance_low_speed_creep_max_duration_sec is not None
                and float(low_speed_creep_metrics.get("longest_streak_duration_sec", 0.0) or 0.0)
                > acceptance_low_speed_creep_max_duration_sec
            ):
                failures.append("SCENARIO_LOW_SPEED_CREEP")
                failure_details.append(
                    {
                        "code": "SCENARIO_LOW_SPEED_CREEP",
                        "scope": "scenario_behavior",
                        "actual_sec": float(
                            low_speed_creep_metrics.get("longest_streak_duration_sec", 0.0) or 0.0
                        ),
                        "threshold_sec": acceptance_low_speed_creep_max_duration_sec,
                        "first_match_ts_sec": low_speed_creep_metrics.get("first_match_ts_sec"),
                    }
                )
            if scenario_driver == "carla_town01_route_health":
                route_established = bool(route_health_metrics.get("route_established"))
                if not route_established:
                    failures.append("ROUTE_NOT_ESTABLISHED")
                    failure_details.append(
                        {
                            "code": "ROUTE_NOT_ESTABLISHED",
                            "scope": "route_health",
                            "routing_success_count": routing_success_count,
                        }
                    )
                route_completion_ratio = _safe_float(route_health_metrics.get("route_completion_percentage"))
                if (
                    route_health_min_completion_ratio is not None
                    and route_completion_ratio is not None
                    and route_completion_ratio < route_health_min_completion_ratio
                ):
                    failures.append("ROUTE_COMPLETION_TOO_LOW")
                    failure_details.append(
                        {
                            "code": "ROUTE_COMPLETION_TOO_LOW",
                            "scope": "route_health",
                            "actual": route_completion_ratio,
                            "threshold": route_health_min_completion_ratio,
                        }
                    )
                route_distance_achieved_m = _safe_float(route_health_metrics.get("route_distance_achieved_m"))
                if (
                    route_health_min_distance_achieved_m is not None
                    and route_distance_achieved_m is not None
                    and route_distance_achieved_m < route_health_min_distance_achieved_m
                ):
                    failures.append("ROUTE_DISTANCE_ACHIEVED_TOO_LOW")
                    failure_details.append(
                        {
                            "code": "ROUTE_DISTANCE_ACHIEVED_TOO_LOW",
                            "scope": "route_health",
                            "actual": route_distance_achieved_m,
                            "threshold": route_health_min_distance_achieved_m,
                        }
                    )
                raw_steer_saturated_ratio = _safe_float(lateral_metrics.get("raw_steer_saturated_ratio"))
                if (
                    route_health_max_raw_steer_saturated_ratio is not None
                    and raw_steer_saturated_ratio is not None
                    and raw_steer_saturated_ratio > route_health_max_raw_steer_saturated_ratio
                ):
                    failures.append("ROUTE_HEALTH_LATERAL_SATURATION_EXCESSIVE")
                    failure_details.append(
                        {
                            "code": "ROUTE_HEALTH_LATERAL_SATURATION_EXCESSIVE",
                            "scope": "route_health",
                            "actual": raw_steer_saturated_ratio,
                            "threshold": route_health_max_raw_steer_saturated_ratio,
                        }
                    )
                longest_sat_sec = _safe_float(lateral_metrics.get("longest_continuous_saturation_sec"))
                if (
                    route_health_max_longest_saturation_sec is not None
                    and longest_sat_sec is not None
                    and longest_sat_sec > route_health_max_longest_saturation_sec
                ):
                    failures.append("ROUTE_HEALTH_LATERAL_SATURATION_TOO_LONG")
                    failure_details.append(
                        {
                            "code": "ROUTE_HEALTH_LATERAL_SATURATION_TOO_LONG",
                            "scope": "route_health",
                            "actual_sec": longest_sat_sec,
                            "threshold_sec": route_health_max_longest_saturation_sec,
                        }
                    )

        route_health_label = None
        if scenario_driver == "carla_town01_route_health":
            route_established = bool(route_health_metrics.get("route_established"))
            if any(code in failures for code in ["NO_CONTROL_LOG", "NO_SENSOR_DATA", "ROS2_PUBLISH_NO_SENSOR_MSG"]):
                route_health_label = "chain_not_alive"
            elif not route_established:
                route_health_label = "route_not_established"
            elif failures:
                route_health_label = "route_established_but_behavior_unhealthy"
            else:
                completion = _safe_float(route_health_metrics.get("route_completion_percentage")) or 0.0
                if completion >= 0.75:
                    route_health_label = "route_health_pass"
                else:
                    route_health_label = "route_health_candidate"

        acceptance = {
            "success": len(failures) == 0,
            "fail_reason": failures[0] if failures else None,
            "failure_codes": failures,
            "failure_details": failure_details,
            "checks": {
                "control_log": {
                    "ok": control_log_ok,
                    "required": require_control_log,
                    "path": str(control_log_path),
                    "size_bytes": control_log_size,
                },
                "sensor_probe": {
                    "ok": sensor_probe_ok,
                    "required": require_sensor_probe,
                    "path": str(sensor_probe_path),
                    "clock_count": clock_count,
                    "tf_count": tf_count,
                },
                "ros2_publish": {
                    "ok": ros2_sensor_ok,
                    "sensor_topic_count": ros2_sensor_topic_count,
                    "sensor_msg_count": ros2_sensor_msgs,
                    "invalid_discovered_topics": ros2_invalid_topics,
                    "native_enable_api_available": ros2_enable_api_available,
                    "topics": probe_topics_effective,
                },
                "ego_motion": {
                    "ok": motion_ok,
                    "max_speed_mps": state.max_speed_mps,
                    "threshold": acceptance_speed_threshold,
                },
                "safety": {
                    "ok": not any(detail.get("scope") == "safety" for detail in failure_details),
                    "exit_reason": summary_data.get("exit_reason"),
                    "collision_count": summary_data.get("collision_count"),
                    "lane_invasion_count": summary_data.get("lane_invasion_count"),
                },
                "control_health": {
                    "enabled": acceptance_control_health_enabled,
                    "fail_on_missing": acceptance_control_health_fail_on_missing,
                    **control_health_check,
                    "target_speed_tracking": {
                        "enabled": acceptance_target_speed_tracking_enabled,
                        "fail_on_missing": acceptance_target_speed_tracking_fail_on_missing,
                        **bridge_target_speed_check,
                    },
                },
                "followstop": followstop_acceptance_check,
                "autoware_control_diagnostics": autoware_control_diagnostics_check,
                "apollo_health_artifacts": {
                    "ok": apollo_health_artifacts_ok,
                    "required": acceptance_require_apollo_health_artifacts,
                    "bridge_health_summary_path": str(apollo_bridge_health_path),
                    "planning_topic_debug_summary_path": str(apollo_planning_summary_path),
                    "cyber_bridge_stats_path": str(apollo_cyber_bridge_stats_path),
                },
                "apollo_planning_nonempty": {
                    "ok": (
                        acceptance_min_planning_nonempty is None
                        or planning_nonempty_count is None
                        or planning_nonempty_count >= acceptance_min_planning_nonempty
                    ),
                    "actual": planning_nonempty_count,
                    "threshold": acceptance_min_planning_nonempty,
                    "planning_total_messages": planning_total_messages,
                },
                "apollo_planning_nonzero_ratio": {
                    "ok": (
                        acceptance_min_planning_nonzero_ratio is None
                        or planning_nonzero_ratio is None
                        or planning_nonzero_ratio >= acceptance_min_planning_nonzero_ratio
                    ),
                    "actual": planning_nonzero_ratio,
                    "threshold": acceptance_min_planning_nonzero_ratio,
                },
                "apollo_invalid_goal": {
                    "ok": (
                        acceptance_max_invalid_goal_count is None
                        or invalid_goal_count is None
                        or invalid_goal_count <= acceptance_max_invalid_goal_count
                    ),
                    "actual": invalid_goal_count,
                    "threshold": acceptance_max_invalid_goal_count,
                    "last_invalid_goal_reason": last_goal_validity.get("invalid_goal_reason"),
                },
                "apollo_invalid_long_route_skip": {
                    "ok": (
                        acceptance_max_routing_invalid_goal_skip_count is None
                        or routing_invalid_goal_skip_count is None
                        or routing_invalid_goal_skip_count <= acceptance_max_routing_invalid_goal_skip_count
                    ),
                    "actual": routing_invalid_goal_skip_count,
                    "threshold": acceptance_max_routing_invalid_goal_skip_count,
                    "routing_success_count": routing_success_count,
                },
                "scenario_low_speed_creep": {
                    "enabled": acceptance_low_speed_creep_enabled,
                    "ok": (
                        (not acceptance_low_speed_creep_enabled)
                        or acceptance_low_speed_creep_max_duration_sec is None
                        or float(low_speed_creep_metrics.get("longest_streak_duration_sec", 0.0) or 0.0)
                        <= acceptance_low_speed_creep_max_duration_sec
                    ),
                    "threshold_sec": acceptance_low_speed_creep_max_duration_sec,
                    **low_speed_creep_metrics,
                },
                "lateral_metrics": lateral_metrics,
                "route_health": {
                    "label": route_health_label,
                    "min_completion_ratio": route_health_min_completion_ratio,
                    "min_distance_achieved_m": route_health_min_distance_achieved_m,
                    "max_raw_steer_saturated_ratio": route_health_max_raw_steer_saturated_ratio,
                    "max_longest_saturation_sec": route_health_max_longest_saturation_sec,
                    **route_health_metrics,
                },
            },
        }
        profile_config_path = str(args.config.resolve()) if args.config else ""
        profile_name = str(
            ((effective_cfg.get("run", {}) or {}).get("profile_name") or "").strip()
            or (Path(profile_config_path).stem if profile_config_path else "unknown")
        )
        profile_info = {
            "profile_name": profile_name,
            "profile_config_path": profile_config_path,
            "effective_yaml_path": str(eff_path.resolve()),
            "disable_legacy_harness_control_for_external_stack": disable_control_cfg_effective,
            "disable_legacy_harness_control_for_external_stack_raw": disable_control_cfg,
            "harness_disable_control_effective": bool(disable_control),
            "external_stack": bool(external_stack),
        }
        summary_data["profile_name"] = profile_name
        summary_data["profile_config_path"] = profile_config_path
        summary_data["profile"] = profile_info
        summary_data["claim_profile"] = bool((effective_cfg.get("run", {}) or {}).get("claim_profile", False))
        summary_data["materialization_probe"] = bool(
            (effective_cfg.get("run", {}) or {}).get("materialization_probe", False)
        )
        summary_data["comparison_label"] = str((run_mode_cfg.get("comparison_label") or "")).strip()
        summary_data["adapter_started"] = adapter_started
        if adapter_fail_reason:
            summary_data["adapter_fail_reason"] = adapter_fail_reason
        control_rx_count = _safe_int(apollo_cyber_bridge_stats.get("control_rx_count"))
        control_tx_count = _safe_int(apollo_cyber_bridge_stats.get("control_tx_count"))
        summary_data["routing_success_count"] = routing_success_count
        summary_data["routing_materialized"] = bool((routing_success_count or 0) > 0)
        summary_data["planning_message_count"] = planning_total_messages
        summary_data["planning_nonempty_count"] = planning_nonempty_count
        summary_data["planning_nonempty_trajectory_ratio"] = planning_nonzero_ratio
        summary_data["planning_materialized"] = bool((planning_nonempty_count or 0) > 0)
        summary_data["control_rx_count"] = control_rx_count
        summary_data["control_tx_count"] = control_tx_count
        if (planning_nonempty_count or 0) > 0 and (control_rx_count or 0) > 0:
            summary_data["control_handoff_status"] = "control_consuming_with_nonzero_planning"
        elif (planning_nonempty_count or 0) > 0:
            summary_data["control_handoff_status"] = "control_missing"
        else:
            summary_data["control_handoff_status"] = "planning_not_materialized"
        summary_data["acceptance"] = acceptance
        summary_data["success"] = acceptance["success"]
        summary_data["fail_reason"] = acceptance["fail_reason"]
        summary_data["summary_status"] = "provisional"
        summary_data["finalized_from_event_stream"] = False
        if scenario_driver == "carla_town01_route_health":
            summary_data["route_health"] = acceptance["checks"].get("route_health", {})
            summary_data["route_health_label"] = acceptance["checks"].get("route_health", {}).get("label")
            if isinstance(scenario_meta.get("traffic_light_control"), dict):
                summary_data["traffic_light_control"] = scenario_meta["traffic_light_control"]
        standard_claim_updates = online_claim_manifest_updates(
            effective_config=effective_cfg,
            scenario_metadata=scenario_meta,
            summary=summary_data,
            config_path=profile_config_path,
            profile_name=profile_name,
        )
        summary_data.update(standard_claim_updates)
        try:
            manifest_path = out_run_dir / "manifest.json"
            if manifest_path.exists():
                manifest_data = _load_json_if_exists(manifest_path)
                manifest_data.update(standard_claim_updates)
                manifest_path.write_text(json.dumps(manifest_data, indent=2))
        except Exception as exc:
            print(f"[WARN] failed to update standard run manifest metadata: {exc}")
        provisional_summary_path.write_text(json.dumps(summary_data, indent=2))
        summary_path.write_text(json.dumps(summary_data, indent=2))
        (out_run_dir / "artifacts" / "profile_info.json").write_text(json.dumps(profile_info, indent=2))
        print(f"[acceptance] success={acceptance['success']} fail_reason={acceptance['fail_reason']}")
    finally:
        _cleanup_once("finally")
        signal.signal(signal.SIGINT, prev_sigint)
        signal.signal(signal.SIGTERM, prev_sigterm)
        print("Settings restored, exiting.")


if __name__ == "__main__":
    main()
