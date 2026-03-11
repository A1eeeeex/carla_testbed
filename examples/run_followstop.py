#!/usr/bin/env python3
"""
Mode-2 (Autoware container direct CARLA) follow-stop harness.
需提前准备 Town01 地图（点云+lanelet2），放在宿主机路径如 /home/ubuntu/autoware-contents/maps/Town01，并通过 AUTOWARE_MAP_PATH 挂载进 Autoware 容器（默认已指向该路径）。
一条命令跑通：
python examples/run_followstop.py --config configs/io/examples/followstop_autoware.yaml --start-carla --carla-root <CARLA_ROOT> --ticks 500
"""
from __future__ import annotations

import argparse
import atexit
import json
import os
import math
import signal
import socket
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
from carla_testbed.record import RecordManager, RecordOptions
from carla_testbed.record.monitor import SignalMonitor
from carla_testbed.record.rviz.launcher import RvizLauncher
from carla_testbed.runner import TestHarness
from carla_testbed.scenarios import FollowStopConfig, FollowStopScenario
from carla_testbed.sim import CarlaClientManager, configure_synchronous_mode, restore_settings
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


def _quaternion_from_yaw(yaw_deg: float):
    yaw_rad = math.radians(yaw_deg)
    half = yaw_rad / 2.0
    return 0.0, 0.0, math.sin(half), math.cos(half)


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


def _write_apollo_scenario_goal(
    effective_cfg: Dict[str, Any],
    out_run_dir: Path,
    ego: carla.Actor,
    front: Optional[carla.Actor],
    args: argparse.Namespace,
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

    ego_fallback = None
    front_fallback = None
    if world is not None:
        try:
            spawn_points = world.get_map().get_spawn_points()
            ego_idx = int(getattr(args, "ego_idx", -1))
            if 0 <= ego_idx < len(spawn_points):
                ego_fallback = spawn_points[ego_idx]
            if front is not None:
                front_idx = int(getattr(args, "front_idx", -1))
                if 0 <= front_idx < len(spawn_points):
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
    payload: Dict[str, Any]
    if args.scenario_goal_x is not None and args.scenario_goal_y is not None:
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

    start_x, start_y, start_z = _transform_xy_with_cfg(float(ego_loc.x), float(ego_loc.y), float(ego_loc.z), tf_cfg)
    payload.update(
        {
            "generated_at_unix_sec": time.time(),
            "requested_goal_mode": str(routing_cfg.get("goal_mode", "scenario_xy") or "scenario_xy"),
            "goal_raw_carla": goal_raw,
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
    ap.add_argument("--ticks", type=int, default=10)
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
        "front_min_ahead_m": 20.0,
        "front_max_ahead_m": 80.0,
        "front_max_lateral_m": 3.0,
        "front_max_heading_diff_deg": 25.0,
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
        args.ticks = run_cfg.get("ticks", args.ticks)
        args.ego_id = run_cfg.get("ego_id", args.ego_id)
        scenario_cfg = cfg.get("scenario", {})
        stack_cfg = (cfg.get("algo", {}) or {}).get("stack")
        args.front_idx = scenario_cfg.get("front_idx", args.front_idx)
        args.ego_idx = scenario_cfg.get("ego_idx", args.ego_idx)
        args.auto_align_front_spawn = bool(
            scenario_cfg.get("auto_align_front_spawn", args.auto_align_front_spawn)
        )
        args.front_min_ahead_m = float(scenario_cfg.get("front_min_ahead_m", args.front_min_ahead_m))
        args.front_max_ahead_m = float(scenario_cfg.get("front_max_ahead_m", args.front_max_ahead_m))
        args.front_max_lateral_m = float(
            scenario_cfg.get("front_max_lateral_m", args.front_max_lateral_m)
        )
        args.front_max_heading_diff_deg = float(
            scenario_cfg.get("front_max_heading_diff_deg", args.front_max_heading_diff_deg)
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
        except Exception as exc:
            print(f"[WARN] failed to check run dir reuse for {out_run_dir}: {exc}")
    out_run_dir.mkdir(parents=True, exist_ok=True)
    update_latest_pointer(out_run_dir)
    log_dir = out_run_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    cleanup_state: Dict[str, Any] = {
        "done": False,
        "adapter": None,
        "adapter_profile": None,
        "adapter_start_attempted": False,
        "out_run_dir": out_run_dir,
        "control_logger_proc": None,
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
        stop_process(cleanup_state.get("control_logger_proc"))
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
    effective_cfg.setdefault("run", {})["ticks"] = args.ticks
    effective_cfg.setdefault("run", {})["map"] = args.town
    effective_cfg.setdefault("run", {})["ego_id"] = args.ego_id
    effective_cfg.setdefault("io", {}).setdefault("ros", {})["namespace"] = _normalize_ros_namespace(args.ros2_namespace)
    effective_cfg.setdefault("scenario", {})["front_idx"] = args.front_idx
    effective_cfg.setdefault("scenario", {})["ego_idx"] = args.ego_idx
    eff_path.write_text(yaml.safe_dump(effective_cfg, sort_keys=False))
    acceptance_cfg = effective_cfg.get("acceptance", {}) if effective_cfg else {}
    acceptance_speed_threshold = acceptance_cfg.get("min_speed_mps", 5.0)

    if args.enable_rviz and not args.enable_ros2_native:
        print("[ERROR] --enable-rviz 仅在 --enable-ros2-native 模式下可用")
        sys.exit(1)
    if args.enable_ros2_bag and not args.enable_ros2_native:
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
    runtime_carla_cfg = ((effective_cfg.get("runtime", {}) or {}).get("carla", {}) or {}) if effective_cfg else {}
    stop_reused_on_exit = bool(runtime_carla_cfg.get("stop_reused_on_exit", True))
    if args.start_carla:
        carla_launcher = CarlaLauncher(
            carla_root=args.carla_root,
            host=args.host,
            port=args.port,
            town=args.town,
            extra_args=args.carla_extra_args,
            foreground=args.carla_foreground,
            run_dir=out_run_dir,
            stop_reused_on_exit=stop_reused_on_exit,
        )
        cleanup_state["carla_launcher"] = carla_launcher
        carla_stop_hook = lambda: carla_launcher.stop()
        cleanup_state["carla_stop_hook"] = carla_stop_hook
        atexit.register(carla_stop_hook)
        try:
            carla_launcher.start()
            if not carla_launcher.wait_ready(timeout_s=180.0, poll_s=1.0):
                if not carla_launcher.reused:
                    print("[WARN] CARLA 首次启动未就绪，尝试自动重启一次")
                    carla_launcher.stop()
                    time.sleep(2.0)
                    carla_launcher.start()
                if not carla_launcher.wait_ready(timeout_s=180.0, poll_s=1.0):
                    print("[ERROR] CARLA 未在超时时间内就绪")
                    print(carla_launcher.diagnose_tail())
                    sys.exit(1)
        except Exception as exc:
            print(f"[ERROR] CARLA 启动失败: {exc}")
            if carla_launcher:
                print(carla_launcher.diagnose_tail())
            sys.exit(1)
    else:
        if not _wait_for_carla(args.host, args.port, timeout=10.0):
            print("[ERROR] 未检测到运行中的 CARLA，请先启动或使用 --start-carla")
            sys.exit(1)

    if adapter and adapter_profile:
        try:
            adapter_start_attempted = True
            cleanup_state["adapter_start_attempted"] = True
            adapter.prepare(adapter_profile, out_run_dir)
            started_result = adapter.start(adapter_profile, out_run_dir)
            adapter_started = True if started_result is None else bool(started_result)
            if stack in {"autoware", "apollo"} and not adapter_started:
                adapter_fail_reason = (
                    "AUTOWARE_CONTAINER_EXIT" if stack == "autoware" else "APOLLO_BRIDGE_EXIT"
                )
            else:
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

    client_mgr = CarlaClientManager(host=args.host, port=args.port, timeout=30.0, root=args.carla_root)
    client = client_mgr.create_client()
    # 有些情况下 CARLA 已启动但地图加载较慢，增加 get_world 重试以避免 30s 超时
    def _get_world_retry(cli, attempts=3, delay=3.0):
        last_exc = None
        for i in range(1, attempts + 1):
            try:
                return cli.get_world()
            except Exception as exc:
                last_exc = exc
                print(f"[carla][WARN] get_world attempt {i} failed: {exc}")
                time.sleep(delay)
        raise last_exc or RuntimeError("get_world failed")

    world = _get_world_retry(client, attempts=3, delay=3.0)
    try:
        current_town = world.get_map().name.split("/")[-1]
    except Exception:
        current_town = ""
    if current_town != args.town:
        def _load_world_retry(cli, town: str, attempts: int = 3, delay: float = 3.0, timeout: float = 60.0):
            last_exc = None
            for i in range(1, attempts + 1):
                try:
                    # temporarily extend timeout for loading heavy maps
                    cli.set_timeout(timeout)
                    w = cli.load_world(town)
                    return w
                except Exception as exc:
                    last_exc = exc
                    print(f"[carla][WARN] load_world attempt {i} failed: {exc}")
                    time.sleep(delay)
            raise last_exc or RuntimeError("load_world failed")

        world = _load_world_retry(client, args.town, attempts=3, delay=3.0, timeout=60.0)
        client.set_timeout(client_mgr.timeout)
        time.sleep(1.0)

    original_settings = world.get_settings()
    cleanup_state["world"] = world
    cleanup_state["original_settings"] = original_settings
    if cfg.synchronous_mode:
        original_settings = configure_synchronous_mode(world, cfg.dt)
        cleanup_state["original_settings"] = original_settings
    bp_lib = world.get_blueprint_library()

    scenario = FollowStopScenario(
        FollowStopConfig(
            front_idx=args.front_idx,
            ego_idx=args.ego_idx,
            ego_id=args.ego_id,
            auto_align_front_spawn=bool(args.auto_align_front_spawn),
            front_min_ahead_m=float(args.front_min_ahead_m),
            front_max_ahead_m=float(args.front_max_ahead_m),
            front_max_lateral_m=float(args.front_max_lateral_m),
            front_max_heading_diff_deg=float(args.front_max_heading_diff_deg),
        )
    )
    cleanup_state["scenario"] = scenario
    actors = None

    try:
        actors = scenario.build(world, world.get_map(), bp_lib)
        cleanup_state["actors"] = actors
        ego = actors.ego
        front = actors.front
        print(f"Spawned ego at idx={scenario.cfg.ego_idx}, front at idx={scenario.cfg.front_idx}")
        try:
            scenario_goal_path = _write_apollo_scenario_goal(effective_cfg, out_run_dir, ego, front, args)
            if scenario_goal_path is not None:
                print(f"[apollo] scenario goal written: {scenario_goal_path}")
        except Exception as exc:
            print(f"[WARN] failed to write Apollo scenario goal: {exc}")

        ros2_runner = _build_ros2_runner(effective_cfg, adapter_started)

        # optional: send goal + engage (backend-agnostic for compose/local ROS2)
        goal_cfg = effective_cfg.get("algo", {}).get("autoware", {}).get("goal", {}) if effective_cfg else {}
        goal_log_path = out_run_dir / "artifacts" / "autoware_goal_and_engage.log"
        if goal_cfg.get("enable", False) and (args.enable_ros2_native or (stack == "autoware" and adapter_started)):
            ahead_m = float(goal_cfg.get("ahead_m", 50.0) or 0.0)
            frame_id = goal_cfg.get("frame_id", "map")
            target_actor = front or ego
            tr = target_actor.get_transform()
            loc = tr.location
            yaw = tr.rotation.yaw
            dx = ahead_m * math.cos(math.radians(yaw))
            dy = ahead_m * math.sin(math.radians(yaw))
            qx, qy, qz, qw = _quaternion_from_yaw(yaw)
            goal_pose = {
                "x": loc.x + dx,
                "y": loc.y + dy,
                "z": loc.z,
                "qx": qx,
                "qy": qy,
                "qz": qz,
                "qw": qw,
            }
            result = send_goal_and_engage(
                ros2_runner,
                goal_pose,
                frame_id=frame_id,
                log_path=goal_log_path,
            )
            print(
                "[goal] sent=%s subscriber_ready=%s subscriber_count=%d engage=%s"
                % (
                    result.goal_sent,
                    result.goal_subscriber_ready,
                    result.goal_subscriber_count,
                    result.engage_succeeded,
                )
            )

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
        disable_control = bool(external_stack)
        ros2_mode_active = bool(args.enable_ros2_native or args.enable_ros2_gt or (external_stack and adapter_started))
        tick_callbacks = []
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
        )
        print(
            f"Run core finished: harness_success={summary['success']} "
            f"harness_fail_reason={summary['fail_reason']} collisions={summary['collision_count']}"
        )

        summary_path = out_run_dir / "summary.json"
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
        require_control_log = bool(stack in {"autoware", "apollo"})
        require_sensor_probe = bool(stack in {"autoware", "apollo"})
        ros2_enable_api_available = bool(
            hasattr(carla.Actor, "enable_for_ros")
            or hasattr(carla.Sensor, "enable_for_ros")
            or hasattr(carla.Vehicle, "enable_for_ros")
        )

        failures = []
        if adapter_fail_reason and not adapter_started:
            failures.append(adapter_fail_reason)
        if require_control_log and not control_log_ok:
            failures.append("NO_CONTROL_LOG")
        if require_sensor_probe and not sensor_probe_ok:
            failures.append("NO_SENSOR_DATA")
        if ros2_mode_active and not ros2_sensor_ok:
            if args.enable_ros2_native and not ros2_enable_api_available:
                failures.append("ROS2_NATIVE_PY_API_UNAVAILABLE")
            else:
                failures.append("ROS2_PUBLISH_NO_SENSOR_MSG")
        if not motion_ok:
            failures.append("EGO_NOT_MOVING")

        acceptance = {
            "success": len(failures) == 0,
            "fail_reason": failures[0] if failures else None,
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
            },
        }
        summary_data["adapter_started"] = adapter_started
        if adapter_fail_reason:
            summary_data["adapter_fail_reason"] = adapter_fail_reason
        summary_data["acceptance"] = acceptance
        summary_data["success"] = acceptance["success"]
        summary_data["fail_reason"] = acceptance["fail_reason"]
        summary_path.write_text(json.dumps(summary_data, indent=2))
        print(f"[acceptance] success={acceptance['success']} fail_reason={acceptance['fail_reason']}")
    finally:
        _cleanup_once("finally")
        signal.signal(signal.SIGINT, prev_sigint)
        signal.signal(signal.SIGTERM, prev_sigterm)
        print("Settings restored, exiting.")


if __name__ == "__main__":
    main()
