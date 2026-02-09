#!/usr/bin/env python3
"""
Mode-2 (Autoware container direct CARLA) follow-stop harness.
需提前准备 Town01 地图（点云+lanelet2），放在宿主机路径如 /home/ubuntu/autoware-contents/maps/Town01，并通过 AUTOWARE_MAP_PATH 挂载进 Autoware 容器（默认已指向该路径）。
一条命令跑通：
python examples/run_followstop.py --config configs/io/examples/followstop_autoware.yaml --start-carla --carla-root <CARLA_ROOT> --ticks 500
"""
from __future__ import annotations

import argparse
import json
import os
import time
import subprocess
import socket
from pathlib import Path
from typing import List, Optional, Dict, Any
import yaml
import math

from carla_testbed.utils.env import resolve_repo_root, resolve_carla_root
from tbio.contract.generate_artifacts import generate_all
from tbio.scripts.healthcheck_ros2 import healthcheck
from algo.registry import get_adapter
from tbio.carla.launcher import CarlaLauncher


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


TESTBED_ROOT = resolve_repo_root()
DEFAULT_CARLA_ROOT = resolve_carla_root(os.environ.get("CARLA_ROOT"))

import carla

from carla_testbed.config import HarnessConfig
from carla_testbed.control import LegacyControllerConfig
from carla_testbed.config.rig_loader import apply_overrides, load_rig_file, load_rig_preset, rig_to_specs
from carla_testbed.runner import TestHarness
from carla_testbed.scenarios import FollowStopConfig, FollowStopScenario
from carla_testbed.sim import CarlaClientManager, configure_synchronous_mode, restore_settings
from carla_testbed.record import RecordManager, RecordOptions
from carla_testbed.record.monitor import SignalMonitor
from carla_testbed.record.rviz.launcher import RvizLauncher
from carla_testbed.record.ros2_bag import Ros2BagRecorder
from tbio.contract.generate_artifacts import generate_all
from tbio.scripts.healthcheck_ros2 import healthcheck
from algo.registry import get_adapter
from tbio.carla.launcher import CarlaLauncher


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


def _send_goal_to_autoware(
    compose_file: Path,
    pose: Dict[str, float],
    frame_id: str = "map",
    container: str = "autoware",
):
    """
    Push a PoseStamped goal into Autoware mission planner using ros2 topic pub.
    pose keys: x,y,z,qx,qy,qz,qw
    """
    compose_file = Path(compose_file).resolve()
    def _exec(cmd, timeout=30):
        return subprocess.check_output(cmd, text=True, timeout=timeout)

    def _compose_exec(bash_cmd: str, timeout=20) -> str:
        full = ["docker", "compose", "-f", str(compose_file), "exec", "-T", container, "bash", "-lc", bash_cmd]
        return _exec(full, timeout=timeout)

    log_lines = []
    def log(msg):
        print(msg)
        log_lines.append(msg)

    # wait container Up
    try:
        for _ in range(30):
            ps = _exec(["docker", "compose", "-f", str(compose_file), "ps", "--status", "running"], timeout=10)
            if "autoware" in ps and "running" in ps:
                break
            time.sleep(1)
        else:
            log("[goal] autoware not running within 30s")
            return False, log_lines
    except Exception as exc:
        log(f"[goal] compose ps failed: {exc}")
        return False, log_lines

    env_setup = (
        "source /opt/ros/humble/setup.bash; "
        "if [ -f /opt/Autoware/install/setup.bash ]; then source /opt/Autoware/install/setup.bash; "
        "elif [ -f /autoware/install/setup.bash ]; then source /autoware/install/setup.bash; fi; "
    )
    pose_yaml = (
        f"{{header:{{frame_id:'{frame_id}'}}, "
        f"pose:{{position:{{x:{pose['x']:.3f},y:{pose['y']:.3f},z:{pose['z']:.3f}}}, "
        f"orientation:{{x:{pose['qx']:.6f},y:{pose['qy']:.6f},z:{pose['qz']:.6f},w:{pose['qw']:.6f}}}}}}}"
    )

    # wait for subscriber
    sub_ready = False
    for _ in range(30):
        try:
            info = _compose_exec(f"{env_setup} ros2 topic info -v /planning/mission_planning/goal || true", timeout=5)
            log(info)
            if "Subscription count" in info:
                for line in info.splitlines():
                    if "Subscription count" in line:
                        try:
                            cnt = int(line.strip().split()[-1])
                            if cnt >= 1:
                                sub_ready = True
                                break
                        except Exception:
                            pass
        except Exception as exc:
            log(f"[goal] topic info error: {exc}")
        if sub_ready:
            break
        time.sleep(1)
    if not sub_ready:
        log("[goal] no subscriber detected within 30s; abort goal publish")
        return False, log_lines

    cmd_str = f"{env_setup} ros2 topic pub --once --qos-reliability reliable /planning/mission_planning/goal geometry_msgs/msg/PoseStamped \"{pose_yaml}\""
    try:
        log(f"[autoware] sending goal via compose: {cmd_str}")
        out = _compose_exec(cmd_str, timeout=20)
        log(out)
    except Exception as exc:
        log(f"[WARN] failed to send goal: {exc}")
        return False, log_lines

    # Engage / change to autonomous (best effort)
    try:
        svc_list = _compose_exec(f"{env_setup} ros2 service list", timeout=10)
        topic_list = _compose_exec(f"{env_setup} ros2 topic list", timeout=10)
        log("[engage] services:\n" + svc_list)
        log("[engage] topics:\n" + topic_list)
    except Exception as exc:
        log(f"[engage] list failed: {exc}")
        svc_list = topic_list = ""

    def try_service(name, payload):
        if name not in svc_list:
            return False
        try:
            typ = _compose_exec(f"{env_setup} ros2 service type {name}", timeout=5).strip()
            log(f"[engage] calling {name} type={typ} payload={payload}")
            call_cmd = f"{env_setup} ros2 service call {name} {typ} '{payload}'"
            log(_compose_exec(call_cmd, timeout=10))
            return True
        except Exception as exc:
            log(f"[engage] {name} failed: {exc}")
            return False

    def try_topic(name, payload):
        if name not in topic_list:
            return False
        try:
            typ = _compose_exec(f"{env_setup} ros2 topic type {name}", timeout=5).strip()
            log(f"[engage] pub {name} type={typ} payload={payload}")
            pub_cmd = f"{env_setup} ros2 topic pub --once {name} {typ} '{payload}'"
            log(_compose_exec(pub_cmd, timeout=10))
            return True
        except Exception as exc:
            log(f"[engage] {name} failed: {exc}")
            return False

    engaged = False
    engaged = engaged or try_service("/api/operation_mode/change_to_autonomous", "{}")
    engaged = engaged or try_service("/api/autoware/set/engage", "{data: true}")
    engaged = engaged or try_topic("/autoware/engage", "{data: true}")
    engaged = engaged or try_topic("/vehicle/engage", "{data: true}")
    if not engaged:
        log("[engage] no engage interface succeeded")

    return True, log_lines


def build_ros2_bag_topics(
    rig_final: dict,
    ego_id: str,
    camera_suffix: str = "image",
    lidar_suffix: str = "point_cloud",
    radar_suffix: str = "point_cloud",
    include_tf: bool = True,
    include_clock: bool = True,
    auto_topics: bool = True,
    explicit_topics: Optional[List[str]] = None,
    extra_topics: Optional[List[str]] = None,
) -> List[str]:
    topics = []
    if auto_topics and rig_final:
        for sensor in rig_final.get("sensors", []) or []:
            if not sensor.get("enabled", True):
                continue
            sid = sensor.get("id")
            bp = sensor.get("blueprint", "")
            prefix = f"/carla/{ego_id}/{sid}"
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
        topics.append(f"/carla/{ego_id}/imu")
        topics.append(f"/carla/{ego_id}/gnss")
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
    # Deprecated
    ap.add_argument("--record-demo", action="store_true", help="(deprecated) 录制双相机 demo")
    ap.add_argument("--make-hud", action="store_true", help="(deprecated) 生成 HUD overlay")
    # Spectator follow
    ap.add_argument("--follow-spectator", action="store_true", help="让 CARLA spectator 视角跟随 ego（本地渲染时便于观察）")
    ap.add_argument("--follow-spectator-distance", type=float, default=None, help="跟车视角距离，默认沿用 record_chase_distance")
    ap.add_argument("--follow-spectator-height", type=float, default=None, help="跟车视角高度，默认沿用 record_chase_height")
    ap.add_argument("--follow-spectator-pitch", type=float, default=None, help="跟车视角俯仰角，默认沿用 record_chase_pitch")
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
        "ros_invert_tf": True,
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
        args.front_idx = scenario_cfg.get("front_idx", args.front_idx)
        args.ego_idx = scenario_cfg.get("ego_idx", args.ego_idx)
        args.enable_ros2_native = scenario_cfg.get("publish_ros2_native", args.enable_ros2_native)
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
    else:
        cfg = {}

    # run_dir and effective config
    ts = int(time.time())
    repo_root = Path(__file__).resolve().parents[1]
    out_run_dir = args.run_dir or (repo_root / "runs" / f"followstop_{ts}")
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
    out_run_dir.mkdir(parents=True, exist_ok=True)
    log_dir = out_run_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    eff_path = out_run_dir / "effective.yaml"
    effective_cfg = deep_update(cfg.copy(), parse_overrides(args.override))
    effective_cfg.setdefault("run", {})["ticks"] = args.ticks
    effective_cfg.setdefault("run", {})["map"] = args.town
    effective_cfg.setdefault("run", {})["ego_id"] = args.ego_id
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
        ros_invert_tf=args.ros_invert_tf,
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
    # Prepare Autoware/dummy via adapter when stack 指定
    adapter = None
    adapter_profile = None
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

    harness = TestHarness(cfg)
    monitor = SignalMonitor(snapshot_interval=20)

    carla_launcher = None
    adapter_started = False
    adapter_fail_reason = None
    if args.start_carla:
        carla_launcher = CarlaLauncher(
            carla_root=args.carla_root,
            host=args.host,
            port=args.port,
            town=args.town,
            extra_args=args.carla_extra_args,
            foreground=args.carla_foreground,
            run_dir=out_run_dir,
        )
        try:
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
            adapter.prepare(adapter_profile, out_run_dir)
            adapter_started = adapter.start(adapter_profile, out_run_dir)
            if not adapter_started:
                adapter_fail_reason = "AUTOWARE_CONTAINER_EXIT"
            else:
                ok = healthcheck(eff_path, timeout=5.0)
                if not ok:
                    print("[WARN] ROS2 healthcheck failed (non-fatal); see messages above")
        except Exception as exc:
            adapter_fail_reason = "AUTOWARE_START_FAIL"
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
    if cfg.synchronous_mode:
        original_settings = configure_synchronous_mode(world, cfg.dt)
    bp_lib = world.get_blueprint_library()

    scenario = FollowStopScenario(FollowStopConfig(front_idx=args.front_idx, ego_idx=args.ego_idx, ego_id=args.ego_id))
    actors = None

    try:
        actors = scenario.build(world, world.get_map(), bp_lib)
        ego = actors.ego
        front = actors.front
        print(f"Spawned ego at idx={scenario.cfg.ego_idx}, front at idx={scenario.cfg.front_idx}")

        # optional: send goal to Autoware mission planner (front车前方一定距离)
        if stack == "autoware":
            goal_cfg = effective_cfg.get("algo", {}).get("autoware", {}).get("goal", {}) if effective_cfg else {}
            if goal_cfg.get("enable", False):
                ahead_m = float(goal_cfg.get("ahead_m", 50.0) or 0.0)
                frame_id = goal_cfg.get("frame_id", "map")
                compose_file = Path(effective_cfg.get("algo", {}).get("autoware", {}).get("compose", "algo/baselines/autoware/docker/compose.yaml"))
                container_name = effective_cfg.get("algo", {}).get("autoware", {}).get("container_name", "autoware")
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
                _send_goal_to_autoware(compose_file, goal_pose, frame_id=frame_id, container=container_name)

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
        disable_control = bool(effective_cfg.get("algo", {}).get("stack") == "autoware")
        # optional: start control logger inside Autoware container
        control_log_cfg = effective_cfg.get("record", {}).get("control_log", {}) if effective_cfg else {}
        control_logger_proc = None
        if disable_control and control_log_cfg.get("enable", True) and adapter_started:
            topic = control_log_cfg.get("topic", "/control/command/control_cmd")
            max_msgs = control_log_cfg.get("max_msgs")
            reliability = control_log_cfg.get("reliability", "best_effort")
            if reliability not in ["best_effort", "reliable"]:
                reliability = "best_effort"
            force_anymsg = control_log_cfg.get("force_anymsg", True)
            compose_file = effective_cfg.get("algo", {}).get("autoware", {}).get("compose", "algo/baselines/autoware/docker/compose.yaml")
            out_log = out_run_dir / "artifacts" / "autoware_control.jsonl"
            out_err = out_run_dir / "artifacts" / "autoware_control.log"
            cmd_str = (
                "source /opt/ros/humble/setup.bash; "
                "if [ -f /opt/Autoware/install/setup.bash ]; then source /opt/Autoware/install/setup.bash; "
                "elif [ -f /autoware/install/setup.bash ]; then source /autoware/install/setup.bash; fi; "
                f"python /work/io/ros2/tools/control_logger.py --topic {topic} --out {out_log}"
            )
            if max_msgs:
                cmd_str += f" --max-msgs {max_msgs}"
            if force_anymsg:
                cmd_str += " --force-anymsg"
            if reliability:
                cmd_str += f" --reliability {reliability}"
            cmd = [
                "docker", "compose", "-f", str(Path(compose_file).resolve()), "exec", "-T", "autoware",
                "bash", "-lc", cmd_str,
            ]
            try:
                control_logger_proc = subprocess.Popen(cmd, stdout=open(out_err, "w"), stderr=subprocess.STDOUT)
                print(f"[monitor] control logger started for {topic}, writing to {out_log}")
            except Exception as exc:
                print(f"[WARN] failed to start control logger: {exc}")

        # sensor probe inside autoware container
        probe_cfg = None
        if effective_cfg:
            rec = effective_cfg.get("record", {}) or {}
            probe_cfg = rec.get("probe") or rec.get("sensor_probe") or {}
        else:
            probe_cfg = {}
        sensor_probe_proc = None
        if probe_cfg.get("enable", True) and stack == "autoware" and adapter_started:
            control_topic_default = control_log_cfg.get("topic", "/control/command/control_cmd")
            probe_topics = [t for t in (probe_cfg.get("topics") or []) if t] or ["/clock", "/tf", control_topic_default]
            if probe_topics:
                max_msgs = probe_cfg.get("max_msgs", 5)
                compose_file = effective_cfg.get("algo", {}).get("autoware", {}).get("compose", "algo/baselines/autoware/docker/compose.yaml")
                out_probe = out_run_dir / "artifacts" / "sensor_probe.json"
                topics_arg = " ".join(probe_topics)
                cmd_str = (
                    "source /opt/ros/humble/setup.bash; "
                    "if [ -f /opt/Autoware/install/setup.bash ]; then source /opt/Autoware/install/setup.bash; "
                    "elif [ -f /autoware/install/setup.bash ]; then source /autoware/install/setup.bash; fi; "
                    f"python /work/io/ros2/tools/topic_probe.py --topics {topics_arg} --max-msgs {max_msgs} --out {out_probe}"
                )
                cmd = [
                    "docker", "compose", "-f", str(Path(compose_file).resolve()), "exec", "-T", "autoware",
                    "bash", "-lc", cmd_str,
                ]
                try:
                    sensor_probe_proc = subprocess.Popen(cmd)
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
        )
        print(f"Run finished: success={summary['success']} fail_reason={summary['fail_reason']} collisions={summary['collision_count']}")

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
                    if isinstance(raw_len, (int, float)) and raw_len > 0:
                        control_log_ok = True
                        break
            except Exception as exc:
                print(f"[WARN] failed to parse control log: {exc}")

        sensor_probe_path = out_run_dir / "artifacts" / "sensor_probe.json"
        sensor_probe_ok = False
        clock_count = 0
        tf_count = None
        if sensor_probe_path.exists():
            try:
                probe_data = json.loads(sensor_probe_path.read_text())
                clock_count = int((probe_data.get("/clock") or {}).get("count", 0))
                tf_entry = probe_data.get("/tf")
                tf_count = None if tf_entry is None else int(tf_entry.get("count", 0))
                sensor_probe_ok = clock_count >= 2 and (tf_count is None or tf_count >= 1)
            except Exception as exc:
                print(f"[WARN] failed to parse sensor probe: {exc}")
        motion_ok = state.max_speed_mps > acceptance_speed_threshold

        failures = []
        if adapter_fail_reason and not adapter_started:
            failures.append(adapter_fail_reason)
        if not control_log_ok:
            failures.append("NO_CONTROL_LOG")
        if not sensor_probe_ok:
            failures.append("NO_SENSOR_DATA")
        if not motion_ok:
            failures.append("EGO_NOT_MOVING")

        acceptance = {
            "success": len(failures) == 0,
            "fail_reason": failures[0] if failures else None,
            "checks": {
                "control_log": {
                    "ok": control_log_ok,
                    "path": str(control_log_path),
                    "size_bytes": control_log_size,
                },
                "sensor_probe": {
                    "ok": sensor_probe_ok,
                    "path": str(sensor_probe_path),
                    "clock_count": clock_count,
                    "tf_count": tf_count,
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
        if failures:
            summary_data["success"] = False
            summary_data.setdefault("fail_reason", failures[0])
        summary_path.write_text(json.dumps(summary_data, indent=2))
        print(f"[acceptance] success={acceptance['success']} fail_reason={acceptance['fail_reason']}")
    finally:
        if 'control_logger_proc' in locals() and control_logger_proc and control_logger_proc.poll() is None:
            control_logger_proc.terminate()
            try:
                control_logger_proc.wait(timeout=5)
            except Exception:
                control_logger_proc.kill()
        if 'sensor_probe_proc' in locals() and sensor_probe_proc and sensor_probe_proc.poll() is None:
            sensor_probe_proc.terminate()
            try:
                sensor_probe_proc.wait(timeout=5)
            except Exception:
                sensor_probe_proc.kill()
        if 'carla_launcher' in locals() and carla_launcher:
            carla_launcher.stop()
        restore_settings(world, original_settings)
        if actors is not None:
            scenario.destroy()
        print("Settings restored, exiting.")


if __name__ == "__main__":
    main()
